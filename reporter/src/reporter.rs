// SPDX-License-Identifier: AGPL-3.0-only

//! Thread-safe, clonable NDJSON v3 event emitter over private Unix domain sockets.

use std::{
    env,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use kat0_pipeline_protocol::{
    model::{Definition, Snapshot},
    v3::{Event, RunOutcome, RunState, SCHEMA_VERSION, TerminalStatus, encode_line},
};
use tokio::{
    io::AsyncWriteExt as _,
    net::UnixStream,
    sync::{Mutex, watch},
    task::JoinHandle,
    time::{self, MissedTickBehavior, timeout},
};

use crate::error::{BoxError, RunResult, classify_error, wrap_failure};

/// Environment variable set by the pipeline orchestrator pointing to the NDJSON v3 Unix socket.
pub const EVENTS_SOCK_ENV: &str = "KAT0_PIPELINE_EVENTS_SOCK";
const SOCKET_IO_TIMEOUT: Duration = Duration::from_secs(2);

struct ReporterInner {
    started: Instant,
    state: RunState,
    stream: Option<UnixStream>,
    warned_ipc: bool,
}

impl ReporterInner {
    fn now_ms(&self) -> u64 {
        let elapsed = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        elapsed.max(self.state.snapshot().elapsed_ms)
    }

    async fn emit(&mut self, event: Event) -> RunResult {
        self.state.apply(event.clone())?;
        self.send_wire(&event).await;
        Ok(())
    }

    async fn send_wire(&mut self, event: &Event) {
        let Some(stream) = self.stream.as_mut() else {
            return;
        };
        let result = async {
            let mut line = encode_line(event)?;
            line.push('\n');
            timeout(SOCKET_IO_TIMEOUT, stream.write_all(line.as_bytes()))
                .await
                .map_err(|_| "timed out writing to pipeline IPC socket")??;
            Ok::<(), BoxError>(())
        }
        .await;
        if let Err(error) = result {
            self.stream = None;
            if !self.warned_ipc {
                eprintln!(
                    "[pipeline-reporter] Event channel closed ({error}); tests and cleanup will continue."
                );
                self.warned_ipc = true;
            }
        }
    }
}

struct ReporterShared {
    orchestrated: bool,
    inner: Mutex<ReporterInner>,
}

/// Clonable, thread-safe NDJSON v3 event emitter backed by [`RunState`].
///
/// Streams events over [`EVENTS_SOCK_ENV`] when orchestrated by the pipeline CLI, and always
/// maintains a validated local [`Snapshot`] for standalone `snapshot.json` persistence.
///
/// Cloning a [`Reporter`] shares the underlying IPC connection and reducer state, making it safe
/// to pass across concurrent worker tasks (`tokio::spawn` / `JoinSet`).
#[derive(Clone)]
pub struct Reporter {
    shared: Arc<ReporterShared>,
}

/// RAII guard for a background [`Event::Tick`] task started via [`Reporter::start_ticking`].
///
/// Dropping the guard stops the background ticker cleanly.
pub struct TickGuard {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl TickGuard {
    /// Stops the background ticker and waits for its task to exit.
    pub async fn stop(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for TickGuard {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Reporter {
    /// Starts a new run and connects to [`EVENTS_SOCK_ENV`] when set.
    pub async fn start(definition: Definition, workers: u16) -> RunResult<Self> {
        let sock = env::var_os(EVENTS_SOCK_ENV).map(PathBuf::from);
        Self::start_with_socket(definition, workers, sock.as_deref()).await
    }

    /// Starts a new run using an explicit optional Unix socket path.
    pub async fn start_with_socket(
        definition: Definition,
        workers: u16,
        socket_path: Option<&Path>,
    ) -> RunResult<Self> {
        let start_event = Event::RunStarted {
            schema_version: SCHEMA_VERSION,
            definition,
            workers: Some(workers.max(1)),
        };
        let state = RunState::start(start_event.clone())?;

        let mut warned_ipc = false;
        let stream = match socket_path {
            Some(path) => match timeout(SOCKET_IO_TIMEOUT, UnixStream::connect(path)).await {
                Ok(Ok(stream)) => Some(stream),
                Ok(Err(error)) => {
                    eprintln!(
                        "[pipeline-reporter] Failed to connect to {}: {error}; continuing standalone.",
                        path.display()
                    );
                    warned_ipc = true;
                    None
                }
                Err(_) => {
                    eprintln!(
                        "[pipeline-reporter] Timed out connecting to {}; continuing standalone.",
                        path.display()
                    );
                    warned_ipc = true;
                    None
                }
            },
            None => None,
        };

        let orchestrated = stream.is_some();
        let mut inner = ReporterInner {
            started: Instant::now(),
            state,
            stream,
            warned_ipc,
        };
        inner.send_wire(&start_event).await;
        Ok(Self {
            shared: Arc::new(ReporterShared {
                orchestrated,
                inner: Mutex::new(inner),
            }),
        })
    }

    /// Returns `true` when this reporter connected to a parent pipeline IPC socket.
    pub fn is_orchestrated(&self) -> bool {
        self.shared.orchestrated
    }

    /// Returns a cloned point-in-time [`Snapshot`] of the current run state.
    pub async fn snapshot(&self) -> Snapshot {
        self.shared.inner.lock().await.state.snapshot().clone()
    }

    /// Emits a single [`Event::Tick`] to advance the live clock if the run is still active.
    pub async fn tick(&self) -> RunResult {
        let mut guard = self.shared.inner.lock().await;
        if guard.state.is_finished() {
            return Ok(());
        }
        let elapsed_ms = guard.now_ms();
        guard.emit(Event::Tick { elapsed_ms }).await
    }

    /// Spawns a lightweight background ticker emitting [`Event::Tick`] every `interval` until
    /// dropped or until the run reaches a terminal state.
    pub fn start_ticking(&self, interval: Duration) -> TickGuard {
        let reporter = self.clone();
        let (stop, mut stop_rx) = watch::channel(false);
        let period = interval.max(Duration::from_millis(50));
        let task = tokio::spawn(async move {
            let mut ticker = time::interval(period);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            // Skip the immediate first tick so elapsed time advances after the first interval.
            ticker.tick().await;
            loop {
                tokio::select! {
                    biased;
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let mut guard = reporter.shared.inner.lock().await;
                        if guard.state.is_finished() {
                            break;
                        }
                        let elapsed_ms = guard.now_ms();
                        let _ = guard.emit(Event::Tick { elapsed_ms }).await;
                    }
                }
            }
        });
        TickGuard {
            stop,
            task: Some(task),
        }
    }

    /// Executes `action` within a `job_started` / `job_finished` envelope.
    pub async fn job<T, E: Into<BoxError>>(
        &self,
        job_id: &str,
        action: impl Future<Output = Result<T, E>>,
    ) -> RunResult<T> {
        {
            let mut guard = self.shared.inner.lock().await;
            let at_ms = guard.now_ms();
            guard
                .emit(Event::JobStarted {
                    job_id: job_id.to_owned(),
                    at_ms,
                })
                .await?;
        }
        let outcome = action.await.map_err(Into::into);
        let status = if outcome.is_ok() {
            TerminalStatus::Passed
        } else {
            TerminalStatus::Failed
        };
        {
            let mut guard = self.shared.inner.lock().await;
            let at_ms = guard.now_ms();
            guard
                .emit(Event::JobFinished {
                    job_id: job_id.to_owned(),
                    status,
                    at_ms,
                })
                .await?;
        }
        outcome.map_err(|source| wrap_failure(job_id, None, None, source))
    }

    /// Executes `action` within a `case_started` / `case_finished` envelope.
    pub async fn case<T, E: Into<BoxError>>(
        &self,
        job_id: &str,
        case_id: &str,
        title: Option<&str>,
        action: impl Future<Output = Result<T, E>>,
    ) -> RunResult<T> {
        {
            let mut guard = self.shared.inner.lock().await;
            let at_ms = guard.now_ms();
            guard
                .emit(Event::CaseStarted {
                    job_id: job_id.to_owned(),
                    case_id: case_id.to_owned(),
                    title: title.map(str::to_owned),
                    at_ms,
                })
                .await?;
        }
        let outcome = action.await.map_err(Into::into);
        let (status, kind) = match &outcome {
            Ok(_) => (TerminalStatus::Passed, None),
            Err(error) => (TerminalStatus::Failed, Some(classify_error(error.as_ref()))),
        };
        {
            let mut guard = self.shared.inner.lock().await;
            let at_ms = guard.now_ms();
            guard
                .emit(Event::CaseFinished {
                    job_id: job_id.to_owned(),
                    case_id: case_id.to_owned(),
                    status,
                    at_ms,
                    kind,
                })
                .await?;
        }
        outcome.map_err(|source| wrap_failure(job_id, Some(case_id), None, source))
    }

    /// Executes `action` within a `step_started` / `step_finished` envelope.
    pub async fn step<T, E: Into<BoxError>>(
        &self,
        job_id: &str,
        case_id: Option<&str>,
        step_id: &str,
        title: Option<&str>,
        action: impl Future<Output = Result<T, E>>,
    ) -> RunResult<T> {
        {
            let mut guard = self.shared.inner.lock().await;
            let at_ms = guard.now_ms();
            guard
                .emit(Event::StepStarted {
                    job_id: job_id.to_owned(),
                    case_id: case_id.map(str::to_owned),
                    step_id: step_id.to_owned(),
                    title: title.map(str::to_owned),
                    at_ms,
                })
                .await?;
        }
        let outcome = action.await.map_err(Into::into);
        let (status, kind) = match &outcome {
            Ok(_) => (TerminalStatus::Passed, None),
            Err(error) => (TerminalStatus::Failed, Some(classify_error(error.as_ref()))),
        };
        {
            let mut guard = self.shared.inner.lock().await;
            let at_ms = guard.now_ms();
            guard
                .emit(Event::StepFinished {
                    job_id: job_id.to_owned(),
                    case_id: case_id.map(str::to_owned),
                    step_id: step_id.to_owned(),
                    status,
                    at_ms,
                    kind,
                })
                .await?;
        }
        outcome.map_err(|source| wrap_failure(job_id, case_id, Some(step_id), source))
    }

    /// Marks `job_id` as skipped.
    pub async fn skip_job(&self, job_id: &str) -> RunResult {
        let mut guard = self.shared.inner.lock().await;
        let at_ms = guard.now_ms();
        guard
            .emit(Event::JobFinished {
                job_id: job_id.to_owned(),
                status: TerminalStatus::Skipped,
                at_ms,
            })
            .await
    }

    /// Marks `case_id` inside `job_id` as skipped.
    pub async fn skip_case(&self, job_id: &str, case_id: &str) -> RunResult {
        let mut guard = self.shared.inner.lock().await;
        let at_ms = guard.now_ms();
        guard
            .emit(Event::CaseFinished {
                job_id: job_id.to_owned(),
                case_id: case_id.to_owned(),
                status: TerminalStatus::Skipped,
                at_ms,
                kind: None,
            })
            .await
    }

    /// Finalizes the run, flushes and closes the IPC stream, and returns the terminal [`Snapshot`].
    pub async fn finish(&self, passed: bool) -> RunResult<Snapshot> {
        let mut guard = self.shared.inner.lock().await;
        if !guard.state.is_finished() {
            let elapsed_ms = guard.now_ms();
            let status = if passed {
                RunOutcome::Passed
            } else {
                RunOutcome::Failed
            };
            guard
                .emit(Event::RunFinished { status, elapsed_ms })
                .await?;
        }
        if let Some(mut stream) = guard.stream.take() {
            let _ = timeout(SOCKET_IO_TIMEOUT, stream.shutdown()).await;
        }
        Ok(guard.state.snapshot().clone())
    }
}

#[cfg(test)]
mod tests {
    use kat0_pipeline_protocol::{
        builder::PipelineBuilder,
        model::{FailureKind, Status},
        v2::validate_snapshot,
        v3::decode_line,
    };
    use tokio::{
        io::{AsyncBufReadExt as _, BufReader},
        net::UnixListener,
        task::JoinSet,
    };

    use crate::error::TypedFailure;

    use super::*;

    fn assert_send_sync<T: Send + Sync + Clone + 'static>() {}

    fn sample_definition() -> Definition {
        PipelineBuilder::new("Reporter test", "Streams NDJSON over unix socket")
            .stage("Verify", |stage| {
                stage.test_job("func", "Functionality", |job| {
                    job.case("login", "Sign in", |_| {});
                });
            })
            .build()
            .unwrap()
    }

    #[test]
    fn reporter_is_clone_send_sync() {
        assert_send_sync::<Reporter>();
    }

    #[tokio::test]
    async fn streams_ndjson_v3_events_over_unix_socket_and_tracks_failure_kinds() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("events.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();

        let reader = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut lines = BufReader::new(stream).lines();
            let mut events = Vec::new();
            while let Some(line) = lines.next_line().await.unwrap() {
                events.push(decode_line(&line).unwrap());
            }
            events
        });

        let reporter = Reporter::start_with_socket(sample_definition(), 1, Some(&sock_path))
            .await
            .unwrap();
        assert!(reporter.is_orchestrated());
        let result = reporter
            .job("func", async {
                reporter
                    .case("func", "login", None, async {
                        reporter
                            .step(
                                "func",
                                Some("login"),
                                "check-cookie",
                                Some("Verify HttpOnly cookie"),
                                async {
                                    Err::<(), _>(TypedFailure::new(
                                        FailureKind::Assertion,
                                        "private assertion detail",
                                    ))
                                },
                            )
                            .await
                    })
                    .await
            })
            .await;
        assert!(result.is_err());

        let snapshot = reporter.finish(false).await.unwrap();
        assert_eq!(snapshot.status, Status::Failed);
        assert_eq!(
            snapshot
                .case_row("func", "login", Some("check-cookie"))
                .map(|row| row.status),
            Some(Status::Failed)
        );

        let events = reader.await.unwrap();
        assert!(matches!(events.first(), Some(Event::RunStarted { .. })));
        assert!(matches!(
            events.last(),
            Some(Event::RunFinished {
                status: RunOutcome::Failed,
                ..
            })
        ));
        assert!(events.iter().any(|event| matches!(
            event,
            Event::StepFinished {
                step_id,
                status: TerminalStatus::Failed,
                kind: Some(FailureKind::Assertion),
                ..
            } if step_id == "check-cookie"
        )));
    }

    #[tokio::test]
    async fn degrades_gracefully_when_ipc_socket_is_missing_or_closed() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.sock");
        let reporter = Reporter::start_with_socket(sample_definition(), 1, Some(&missing))
            .await
            .unwrap();
        assert!(!reporter.is_orchestrated());
        reporter
            .job("func", async { reporter.skip_case("func", "login").await })
            .await
            .unwrap();
        let snapshot = reporter.finish(true).await.unwrap();
        assert_eq!(snapshot.status, Status::Passed);
        assert_eq!(
            snapshot
                .case_row("func", "login", None)
                .map(|row| row.status),
            Some(Status::Skipped)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_worker_concurrent_tasks_stream_valid_events_and_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("concurrent.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();

        let reader = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut lines = BufReader::new(stream).lines();
            let mut events = Vec::new();
            while let Some(line) = lines.next_line().await.unwrap() {
                events.push(decode_line(&line).unwrap());
            }
            events
        });

        let definition = PipelineBuilder::new("Concurrent suite", "4 parallel workers")
            .stage("Parallel", |stage| {
                for worker in 1..=4 {
                    stage.test_job(format!("job-{worker}"), format!("Job {worker}"), |job| {
                        job.case(format!("case-{worker}"), format!("Case {worker}"), |_| {});
                    });
                }
            })
            .build()
            .unwrap();

        let reporter = Reporter::start_with_socket(definition, 4, Some(&sock_path))
            .await
            .unwrap();
        let ticker = reporter.start_ticking(Duration::from_millis(50));

        let mut workers = JoinSet::new();
        for worker in 1..=4 {
            let reporter = reporter.clone();
            workers.spawn(async move {
                let job_id = format!("job-{worker}");
                let case_id = format!("case-{worker}");
                reporter
                    .job(&job_id, async {
                        reporter
                            .case(&job_id, &case_id, None, async {
                                reporter
                                    .step(
                                        &job_id,
                                        Some(&case_id),
                                        "check",
                                        Some("Verify parallel step"),
                                        async {
                                            time::sleep(Duration::from_millis(20)).await;
                                            Ok::<(), std::io::Error>(())
                                        },
                                    )
                                    .await
                            })
                            .await
                    })
                    .await
            });
        }

        while let Some(result) = workers.join_next().await {
            result.unwrap().unwrap();
        }
        ticker.stop().await;

        let snapshot = reporter.finish(true).await.unwrap();
        validate_snapshot(&snapshot).unwrap();
        assert_eq!(snapshot.status, Status::Passed);
        for worker in 1..=4 {
            assert_eq!(
                snapshot.job_status(&format!("job-{worker}")),
                Some(Status::Passed)
            );
        }

        // Replay all events received on the socket to verify protocol integrity.
        let events = reader.await.unwrap();
        let mut iter = events.into_iter();
        let mut replayed = RunState::start(iter.next().unwrap()).unwrap();
        assert_eq!(replayed.workers(), 4);
        for event in iter {
            replayed.apply(event).unwrap();
        }
        assert!(replayed.is_finished());
        assert_eq!(replayed.snapshot().status, Status::Passed);
    }

    #[tokio::test]
    async fn tick_guard_emits_monotonic_ticks_and_stops_on_drop() {
        let reporter = Reporter::start_with_socket(sample_definition(), 1, None)
            .await
            .unwrap();
        let ticker = reporter.start_ticking(Duration::from_millis(50));
        time::sleep(Duration::from_millis(120)).await;
        ticker.stop().await;
        assert!(reporter.snapshot().await.elapsed_ms >= 40);
        reporter.tick().await.unwrap();
        let final_snapshot = reporter.finish(true).await.unwrap();
        assert_eq!(final_snapshot.status, Status::Passed);
    }
}
