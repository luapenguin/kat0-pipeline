// SPDX-License-Identifier: AGPL-3.0-only

//! Thread-safe, clonable NDJSON v3 event emitter over private local IPC endpoints.
//!
//! Connects over Unix domain sockets on Unix platforms and local-only Named Pipes on Windows,
//! decoupling socket I/O from the in-memory reducer lock via a dedicated background writer task.

use std::{
    env,
    future::Future,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use kat0_pipeline_protocol::{
    model::{Definition, FailureKind, Snapshot},
    v3::{Event, RunOutcome, RunState, SCHEMA_VERSION, TerminalStatus, encode_line},
};
use tokio::{
    io::AsyncWriteExt as _,
    sync::{
        mpsc::{self, error::TrySendError},
        watch,
    },
    task::JoinHandle,
    time::{self, MissedTickBehavior, timeout},
};

use crate::error::{BoxError, RunResult, classify_error, wrap_failure};

/// Environment variable set by the pipeline orchestrator pointing to the NDJSON v3 IPC endpoint.
pub const EVENTS_SOCK_ENV: &str = "KAT0_PIPELINE_EVENTS_SOCK";
const SOCKET_IO_TIMEOUT: Duration = Duration::from_secs(2);
const WRITER_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const WIRE_CHANNEL_CAPACITY: usize = 1_024;

struct ReporterInner {
    started: Instant,
    state: RunState,
    wire_tx: Option<mpsc::Sender<String>>,
    writer_task: Option<JoinHandle<()>>,
    warned_ipc: bool,
}

impl ReporterInner {
    fn now_ms(&self) -> u64 {
        let elapsed = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        elapsed.max(self.state.snapshot().elapsed_ms)
    }

    fn emit(&mut self, event: Event) -> RunResult {
        self.state.apply(event.clone())?;
        self.enqueue_wire(&event, false);
        Ok(())
    }

    fn emit_tick(&mut self, elapsed_ms: u64) -> RunResult {
        let event = Event::Tick { elapsed_ms };
        self.state.apply(event.clone())?;
        self.enqueue_wire(&event, true);
        Ok(())
    }

    fn enqueue_wire(&mut self, event: &Event, droppable: bool) {
        let Some(tx) = self.wire_tx.as_ref() else {
            return;
        };
        let mut line = match encode_line(event) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.close_wire(&error.to_string());
                return;
            }
        };
        line.push('\n');
        match tx.try_send(line) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) if droppable => {}
            Err(TrySendError::Full(_)) => {
                self.close_wire("IPC event queue saturated");
            }
            Err(TrySendError::Closed(_)) => {
                self.close_wire("IPC writer channel closed");
            }
        }
    }

    fn close_wire(&mut self, reason: &str) {
        self.wire_tx = None;
        if !self.warned_ipc {
            eprintln!(
                "[pipeline-reporter] Event channel closed ({reason}); tests and cleanup will continue."
            );
            self.warned_ipc = true;
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
/// to pass across concurrent worker tasks (`tokio::spawn` / `JoinSet`). Socket writes run on a
/// dedicated background task so slow consumers never block concurrent workers.
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

enum ActiveScope<'a> {
    Job {
        job_id: &'a str,
    },
    Case {
        job_id: &'a str,
        case_id: &'a str,
    },
    Step {
        job_id: &'a str,
        case_id: Option<&'a str>,
        step_id: &'a str,
    },
}

/// Cancellation-safe RAII guard ensuring an active job, case, or step emits a terminal
/// `Failed` transition if its future is cancelled (dropped) before completion.
struct ScopeGuard<'a> {
    reporter: &'a Reporter,
    scope: Option<ActiveScope<'a>>,
}

impl<'a> ScopeGuard<'a> {
    fn new(reporter: &'a Reporter, scope: ActiveScope<'a>) -> Self {
        Self {
            reporter,
            scope: Some(scope),
        }
    }

    fn disarm(&mut self) {
        self.scope = None;
    }
}

impl Drop for ScopeGuard<'_> {
    fn drop(&mut self) {
        let Some(scope) = self.scope.take() else {
            return;
        };
        let mut inner = self.reporter.lock_inner();
        if inner.state.is_finished() {
            return;
        }
        let at_ms = inner.now_ms();
        let event = match scope {
            ActiveScope::Job { job_id } => Event::JobFinished {
                job_id: job_id.to_owned(),
                status: TerminalStatus::Failed,
                at_ms,
            },
            ActiveScope::Case { job_id, case_id } => Event::CaseFinished {
                job_id: job_id.to_owned(),
                case_id: case_id.to_owned(),
                status: TerminalStatus::Failed,
                at_ms,
                kind: Some(FailureKind::Execution),
            },
            ActiveScope::Step {
                job_id,
                case_id,
                step_id,
            } => Event::StepFinished {
                job_id: job_id.to_owned(),
                case_id: case_id.map(str::to_owned),
                step_id: step_id.to_owned(),
                status: TerminalStatus::Failed,
                at_ms,
                kind: Some(FailureKind::Execution),
            },
        };
        let _ = inner.emit(event);
    }
}

impl Reporter {
    /// Starts a new run and connects to [`EVENTS_SOCK_ENV`] when set.
    pub async fn start(definition: Definition, workers: u16) -> RunResult<Self> {
        let sock = env::var_os(EVENTS_SOCK_ENV).map(PathBuf::from);
        Self::start_with_socket(definition, workers, sock.as_deref()).await
    }

    /// Starts a new run using an explicit optional local IPC endpoint path.
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
        let (wire_tx, writer_task) = match socket_path {
            Some(path) => match transport::connect(path).await {
                Ok(stream) => {
                    let (tx, rx) = mpsc::channel(WIRE_CHANNEL_CAPACITY);
                    let task = tokio::spawn(run_wire_writer(stream, rx));
                    (Some(tx), Some(task))
                }
                Err(error) => {
                    eprintln!(
                        "[pipeline-reporter] Failed to connect to {}: {error}; continuing standalone.",
                        path.display()
                    );
                    warned_ipc = true;
                    (None, None)
                }
            },
            None => (None, None),
        };

        let orchestrated = wire_tx.is_some();
        let mut inner = ReporterInner {
            started: Instant::now(),
            state,
            wire_tx,
            writer_task,
            warned_ipc,
        };
        inner.enqueue_wire(&start_event, false);
        Ok(Self {
            shared: Arc::new(ReporterShared {
                orchestrated,
                inner: Mutex::new(inner),
            }),
        })
    }

    fn lock_inner(&self) -> MutexGuard<'_, ReporterInner> {
        self.shared
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Returns `true` when this reporter connected to a parent pipeline IPC endpoint.
    pub fn is_orchestrated(&self) -> bool {
        self.shared.orchestrated
    }

    /// Returns a cloned point-in-time [`Snapshot`] of the current run state.
    pub async fn snapshot(&self) -> Snapshot {
        self.lock_inner().state.snapshot().clone()
    }

    /// Emits a single [`Event::Tick`] to advance the live clock if the run is still active.
    pub async fn tick(&self) -> RunResult {
        let mut inner = self.lock_inner();
        if inner.state.is_finished() {
            return Ok(());
        }
        let elapsed_ms = inner.now_ms();
        inner.emit_tick(elapsed_ms)
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
                        let mut inner = reporter.lock_inner();
                        if inner.state.is_finished() {
                            break;
                        }
                        let elapsed_ms = inner.now_ms();
                        let _ = inner.emit_tick(elapsed_ms);
                    }
                }
            }
        });
        TickGuard {
            stop,
            task: Some(task),
        }
    }

    /// Executes `action` within a cancellation-safe `job_started` / `job_finished` envelope.
    pub async fn job<T, E: Into<BoxError>>(
        &self,
        job_id: &str,
        action: impl Future<Output = Result<T, E>>,
    ) -> RunResult<T> {
        {
            let mut inner = self.lock_inner();
            let at_ms = inner.now_ms();
            inner.emit(Event::JobStarted {
                job_id: job_id.to_owned(),
                at_ms,
            })?;
        }
        let mut guard = ScopeGuard::new(self, ActiveScope::Job { job_id });
        let outcome = action.await.map_err(Into::into);
        guard.disarm();
        let status = if outcome.is_ok() {
            TerminalStatus::Passed
        } else {
            TerminalStatus::Failed
        };
        {
            let mut inner = self.lock_inner();
            let at_ms = inner.now_ms();
            inner.emit(Event::JobFinished {
                job_id: job_id.to_owned(),
                status,
                at_ms,
            })?;
        }
        outcome.map_err(|source| wrap_failure(job_id, None, None, source))
    }

    /// Executes `action` within a cancellation-safe `case_started` / `case_finished` envelope.
    pub async fn case<T, E: Into<BoxError>>(
        &self,
        job_id: &str,
        case_id: &str,
        title: Option<&str>,
        action: impl Future<Output = Result<T, E>>,
    ) -> RunResult<T> {
        {
            let mut inner = self.lock_inner();
            let at_ms = inner.now_ms();
            inner.emit(Event::CaseStarted {
                job_id: job_id.to_owned(),
                case_id: case_id.to_owned(),
                title: title.map(str::to_owned),
                at_ms,
            })?;
        }
        let mut guard = ScopeGuard::new(self, ActiveScope::Case { job_id, case_id });
        let outcome = action.await.map_err(Into::into);
        guard.disarm();
        let (status, kind) = match &outcome {
            Ok(_) => (TerminalStatus::Passed, None),
            Err(error) => (TerminalStatus::Failed, Some(classify_error(error.as_ref()))),
        };
        {
            let mut inner = self.lock_inner();
            let at_ms = inner.now_ms();
            inner.emit(Event::CaseFinished {
                job_id: job_id.to_owned(),
                case_id: case_id.to_owned(),
                status,
                at_ms,
                kind,
            })?;
        }
        outcome.map_err(|source| wrap_failure(job_id, Some(case_id), None, source))
    }

    /// Executes `action` within a cancellation-safe `step_started` / `step_finished` envelope.
    pub async fn step<T, E: Into<BoxError>>(
        &self,
        job_id: &str,
        case_id: Option<&str>,
        step_id: &str,
        title: Option<&str>,
        action: impl Future<Output = Result<T, E>>,
    ) -> RunResult<T> {
        {
            let mut inner = self.lock_inner();
            let at_ms = inner.now_ms();
            inner.emit(Event::StepStarted {
                job_id: job_id.to_owned(),
                case_id: case_id.map(str::to_owned),
                step_id: step_id.to_owned(),
                title: title.map(str::to_owned),
                at_ms,
            })?;
        }
        let mut guard = ScopeGuard::new(
            self,
            ActiveScope::Step {
                job_id,
                case_id,
                step_id,
            },
        );
        let outcome = action.await.map_err(Into::into);
        guard.disarm();
        let (status, kind) = match &outcome {
            Ok(_) => (TerminalStatus::Passed, None),
            Err(error) => (TerminalStatus::Failed, Some(classify_error(error.as_ref()))),
        };
        {
            let mut inner = self.lock_inner();
            let at_ms = inner.now_ms();
            inner.emit(Event::StepFinished {
                job_id: job_id.to_owned(),
                case_id: case_id.map(str::to_owned),
                step_id: step_id.to_owned(),
                status,
                at_ms,
                kind,
            })?;
        }
        outcome.map_err(|source| wrap_failure(job_id, case_id, Some(step_id), source))
    }

    /// Marks `job_id` as skipped.
    pub async fn skip_job(&self, job_id: &str) -> RunResult {
        let mut inner = self.lock_inner();
        let at_ms = inner.now_ms();
        inner.emit(Event::JobFinished {
            job_id: job_id.to_owned(),
            status: TerminalStatus::Skipped,
            at_ms,
        })
    }

    /// Marks `case_id` inside `job_id` as skipped.
    pub async fn skip_case(&self, job_id: &str, case_id: &str) -> RunResult {
        let mut inner = self.lock_inner();
        let at_ms = inner.now_ms();
        inner.emit(Event::CaseFinished {
            job_id: job_id.to_owned(),
            case_id: case_id.to_owned(),
            status: TerminalStatus::Skipped,
            at_ms,
            kind: None,
        })
    }

    /// Finalizes the run, flushes and closes the IPC writer task, and returns the terminal [`Snapshot`].
    pub async fn finish(&self, passed: bool) -> RunResult<Snapshot> {
        let (wire_tx, writer_task, snapshot) = {
            let mut inner = self.lock_inner();
            if !inner.state.is_finished() {
                let elapsed_ms = inner.now_ms();
                let status = if passed {
                    RunOutcome::Passed
                } else {
                    RunOutcome::Failed
                };
                inner.emit(Event::RunFinished { status, elapsed_ms })?;
            }
            (
                inner.wire_tx.take(),
                inner.writer_task.take(),
                inner.state.snapshot().clone(),
            )
        };
        drop(wire_tx);
        if let Some(task) = writer_task {
            let _ = timeout(WRITER_DRAIN_TIMEOUT, task).await;
        }
        Ok(snapshot)
    }
}

async fn run_wire_writer(mut stream: transport::Stream, mut rx: mpsc::Receiver<String>) {
    while let Some(line) = rx.recv().await {
        match timeout(SOCKET_IO_TIMEOUT, stream.write_all(line.as_bytes())).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                eprintln!(
                    "[pipeline-reporter] Event channel closed ({error}); tests and cleanup will continue."
                );
                return;
            }
            Err(_) => {
                eprintln!(
                    "[pipeline-reporter] Event channel closed (timed out writing to pipeline IPC endpoint); tests and cleanup will continue."
                );
                return;
            }
        }
    }
    let _ = timeout(SOCKET_IO_TIMEOUT, stream.flush()).await;
    let _ = timeout(SOCKET_IO_TIMEOUT, stream.shutdown()).await;
}

#[cfg(unix)]
mod transport {
    use std::path::Path;

    use tokio::{net::UnixStream, time::timeout};

    use super::SOCKET_IO_TIMEOUT;
    use crate::error::BoxError;

    pub(super) type Stream = UnixStream;

    pub(super) async fn connect(path: &Path) -> Result<Stream, BoxError> {
        timeout(SOCKET_IO_TIMEOUT, UnixStream::connect(path))
            .await
            .map_err(|_| "timed out connecting to pipeline IPC socket")?
            .map_err(Into::into)
    }
}

#[cfg(windows)]
mod transport {
    use std::{
        path::Path,
        time::{Duration, Instant},
    };

    use tokio::{
        net::windows::named_pipe::{ClientOptions, NamedPipeClient},
        time,
    };

    use super::SOCKET_IO_TIMEOUT;
    use crate::error::BoxError;

    const ERROR_PIPE_BUSY: i32 = 231;

    pub(super) type Stream = NamedPipeClient;

    pub(super) async fn connect(path: &Path) -> Result<Stream, BoxError> {
        let deadline = Instant::now() + SOCKET_IO_TIMEOUT;
        loop {
            match ClientOptions::new().open(path) {
                Ok(client) => return Ok(client),
                Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                    if Instant::now() >= deadline {
                        return Err("timed out connecting to pipeline IPC named pipe".into());
                    }
                    time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

#[cfg(not(any(unix, windows)))]
mod transport {
    use std::{io, path::Path};

    use crate::error::BoxError;

    pub(super) type Stream = tokio::io::Sink;

    pub(super) async fn connect(_path: &Path) -> Result<Stream, BoxError> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IPC event transport is only supported on Unix and Windows platforms",
        )
        .into())
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
        task::JoinSet,
    };

    use crate::error::TypedFailure;

    use super::*;

    fn assert_send_sync<T: Send + Sync + Clone + 'static>() {}

    fn sample_definition() -> Definition {
        PipelineBuilder::new("Reporter test", "Streams NDJSON over local IPC")
            .stage("Verify", |stage| {
                stage.test_job("func", "Functionality", |job| {
                    job.case("login", "Sign in", |_| {});
                });
            })
            .build()
            .unwrap()
    }

    #[cfg(unix)]
    fn spawn_test_server(
        dir: &Path,
        name: &str,
        initial_read_delay: Duration,
    ) -> (PathBuf, JoinHandle<Vec<Event>>) {
        let sock_path = dir.join(name);
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();
        let reader = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            if !initial_read_delay.is_zero() {
                time::sleep(initial_read_delay).await;
            }
            let mut lines = BufReader::new(stream).lines();
            let mut events = Vec::new();
            while let Some(line) = lines.next_line().await.unwrap() {
                events.push(decode_line(&line).unwrap());
            }
            events
        });
        (sock_path, reader)
    }

    #[cfg(windows)]
    fn spawn_test_server(
        _dir: &Path,
        name: &str,
        initial_read_delay: Duration,
    ) -> (PathBuf, JoinHandle<Vec<Event>>) {
        use std::sync::atomic::{AtomicU64, Ordering};
        use tokio::net::windows::named_pipe::ServerOptions;

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pipe_name = format!(
            r"\\.\pipe\kat0-reporter-test-{}-{seq}-{name}",
            std::process::id()
        );
        let server = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .create(&pipe_name)
            .unwrap();
        let reader = tokio::spawn(async move {
            server.connect().await.unwrap();
            if !initial_read_delay.is_zero() {
                time::sleep(initial_read_delay).await;
            }
            let mut lines = BufReader::new(server).lines();
            let mut events = Vec::new();
            while let Some(line) = lines.next_line().await.unwrap() {
                events.push(decode_line(&line).unwrap());
            }
            events
        });
        (PathBuf::from(pipe_name), reader)
    }

    #[test]
    fn reporter_is_clone_send_sync() {
        assert_send_sync::<Reporter>();
    }

    #[tokio::test]
    async fn streams_ndjson_v3_events_over_local_ipc_and_tracks_failure_kinds() {
        let dir = tempfile::tempdir().unwrap();
        let (sock_path, reader) = spawn_test_server(dir.path(), "events.sock", Duration::ZERO);

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
    async fn multi_worker_concurrent_tasks_stream_valid_events_even_with_slow_reader() {
        let dir = tempfile::tempdir().unwrap();
        // Delay reading from the IPC server to prove workers never block on socket I/O.
        let (sock_path, reader) =
            spawn_test_server(dir.path(), "concurrent.sock", Duration::from_millis(80));

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

        // Replay all events received on the IPC endpoint to verify strict ordering and integrity.
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
    async fn cancelling_nested_step_case_or_job_emits_failed_terminal_transitions() {
        let dir = tempfile::tempdir().unwrap();
        let (sock_path, reader) = spawn_test_server(dir.path(), "cancel.sock", Duration::ZERO);

        let reporter = Reporter::start_with_socket(sample_definition(), 1, Some(&sock_path))
            .await
            .unwrap();

        // Cancel a step inside an active case/job via an external timeout, then run a cleanup step.
        reporter
            .job("func", async {
                let case_result = reporter
                    .case("func", "login", None, async {
                        let timed_out = timeout(
                            Duration::from_millis(20),
                            reporter.step(
                                "func",
                                Some("login"),
                                "stalled",
                                Some("Stalled step"),
                                async {
                                    std::future::pending::<Result<(), std::io::Error>>().await
                                },
                            ),
                        )
                        .await;
                        assert!(timed_out.is_err());

                        // Subsequent cleanup step in the same case must still succeed because
                        // ScopeGuard closed "stalled" cleanly on drop.
                        reporter
                            .step(
                                "func",
                                Some("login"),
                                "cleanup",
                                Some("Cleanup after cancel"),
                                async { Ok::<(), std::io::Error>(()) },
                            )
                            .await?;

                        Err::<(), BoxError>(
                            std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "outer timeout cancelled step",
                            )
                            .into(),
                        )
                    })
                    .await;
                assert!(case_result.is_err());
                case_result
            })
            .await
            .expect_err("job should fail after cancelled step");

        let snapshot = reporter.finish(false).await.unwrap();
        validate_snapshot(&snapshot).unwrap();
        assert_eq!(snapshot.status, Status::Failed);
        assert_eq!(
            snapshot
                .case_row("func", "login", Some("stalled"))
                .map(|row| row.status),
            Some(Status::Failed)
        );
        assert_eq!(
            snapshot
                .case_row("func", "login", Some("cleanup"))
                .map(|row| row.status),
            Some(Status::Passed)
        );

        // Verify the wire event stream replays cleanly with zero protocol errors.
        let events = reader.await.unwrap();
        let mut iter = events.into_iter();
        let mut replayed = RunState::start(iter.next().unwrap()).unwrap();
        for event in iter {
            replayed.apply(event).unwrap();
        }
        assert!(replayed.is_finished());
        validate_snapshot(replayed.snapshot()).unwrap();

        // Also verify full 3-level nested cancellation (job -> case -> step) unwinds in order.
        let (nested_sock, nested_reader) =
            spawn_test_server(dir.path(), "cancel-nested.sock", Duration::ZERO);
        let nested_reporter =
            Reporter::start_with_socket(sample_definition(), 1, Some(&nested_sock))
                .await
                .unwrap();
        let cancelled = timeout(
            Duration::from_millis(20),
            nested_reporter.job("func", async {
                nested_reporter
                    .case("func", "login", None, async {
                        nested_reporter
                            .step(
                                "func",
                                Some("login"),
                                "deep-hang",
                                Some("Deep hang"),
                                async {
                                    std::future::pending::<Result<(), std::io::Error>>().await
                                },
                            )
                            .await
                    })
                    .await
            }),
        )
        .await;
        assert!(cancelled.is_err());

        let nested_snapshot = nested_reporter.finish(false).await.unwrap();
        validate_snapshot(&nested_snapshot).unwrap();
        assert_eq!(nested_snapshot.status, Status::Failed);
        assert_eq!(nested_snapshot.job_status("func"), Some(Status::Failed));
        assert_eq!(
            nested_snapshot
                .case_row("func", "login", Some("deep-hang"))
                .map(|row| row.status),
            Some(Status::Failed)
        );

        let nested_events = nested_reader.await.unwrap();
        let mut nested_iter = nested_events.into_iter();
        let mut nested_replayed = RunState::start(nested_iter.next().unwrap()).unwrap();
        for event in nested_iter {
            nested_replayed.apply(event).unwrap();
        }
        assert!(nested_replayed.is_finished());
        validate_snapshot(nested_replayed.snapshot()).unwrap();
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
