// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    ffi::OsString,
    io::{self, Write as _},
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use kat0_pipeline_protocol::{
    model::{FailureKind as ProtocolFailureKind, Snapshot, Status},
    v2,
    v3::{RecordedFailure, RunState},
};
use tokio::{process::Command, sync::mpsc};

use crate::{
    RunResult,
    bundle::Bundle,
    ipc::{EVENTS_SOCK_ENV, EventSocket, IncomingMessage},
    prepare::reserve_report,
    ui::{
        UiMode,
        browser::{BrowserSession, DEFAULT_GRACE_PERIOD},
        terminal::TerminalPresenter,
    },
    validate::{self, Failure, FailureKind, FailureSite},
};

pub struct RunOptions {
    pub command: OsString,
    pub args: Vec<OsString>,
    pub ui: UiMode,
    pub write_html: bool,
    pub report_root: PathBuf,
    pub open_browser: bool,
    pub browser_grace: Duration,
    pub bundle: Option<Arc<Bundle>>,
}

impl RunOptions {
    pub fn new(command: OsString, args: Vec<OsString>, report_root: PathBuf) -> Self {
        let open_browser = std::env::var_os("PIPELINE_NO_OPEN").as_deref() != Some("1".as_ref());
        Self {
            command,
            args,
            ui: UiMode::Terminal,
            write_html: true,
            report_root,
            open_browser,
            browser_grace: DEFAULT_GRACE_PERIOD,
            bundle: None,
        }
    }
}

pub struct RunArtifacts {
    pub snapshot_path: PathBuf,
    pub html_path: Option<PathBuf>,
}

/// Orchestrates an external test command connected over a private `0700` Unix domain socket,
/// streaming NDJSON v3 events into the selected UI presenter(s) and publishing final artifacts.
pub async fn execute(options: RunOptions) -> RunResult<RunArtifacts> {
    let mut bundle = match options.bundle {
        Some(prebuilt) => Some(prebuilt),
        None if options.ui.uses_browser() => Some(Arc::new(Bundle::embedded()?)),
        None => None,
    };

    let (socket, mut rx) = EventSocket::bind()?;
    let started_at = Instant::now();

    let mut child = Command::new(&options.command)
        .args(&options.args)
        .env(EVENTS_SOCK_ENV, socket.path())
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            format!(
                "No se pudo iniciar {}: {error}",
                Path::new(&options.command).display()
            )
        })?;

    let mut session = RunSession {
        state: None,
        terminal: options.ui.uses_terminal().then(TerminalPresenter::stdout),
        browser: None,
        bundle: bundle.as_ref().map(Arc::clone),
        uses_browser: options.ui.uses_browser(),
        open_browser: options.open_browser,
    };
    let mut interrupted = false;

    let child_status: Option<ExitStatus> = loop {
        tokio::select! {
            biased;
            ctrl_c = tokio::signal::ctrl_c() => {
                if ctrl_c.is_ok() {
                    interrupted = true;
                    let _ = child.kill().await;
                    let status = child.wait().await.ok();
                    break status;
                }
            }
            message = rx.recv() => {
                if let Some(message) = message {
                    session.handle(message);
                }
            }
            waited = child.wait() => {
                break waited.ok();
            }
        }
    };

    // Stop accepting new IPC clients, drain in-flight writers, and process remaining buffered events.
    socket.close().await;
    session.drain(&mut rx);

    let RunSession {
        state,
        mut terminal,
        browser,
        ..
    } = session;

    let Some(mut state) = state else {
        if let Some(browser) = browser {
            let _ = browser.close(Duration::ZERO).await;
        }
        let detail = child_status.map_or_else(
            || "sin estado de salida".to_owned(),
            |status| status.to_string(),
        );
        return Err(format!(
            "El productor terminó ({detail}) sin emitir run_started por {EVENTS_SOCK_ENV}"
        )
        .into());
    };

    let elapsed_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    if !state.is_finished() {
        let terminal_snapshot = Arc::new(state.finalize_aborted(elapsed_ms).clone());
        if let Some(browser) = &browser {
            browser.publish(terminal_snapshot);
        }
    }

    let mut failures = collect_failures(&state, child_status, interrupted);

    let html_target = match reserve_report(&options.report_root).await {
        Ok(path) => path,
        Err(error) => {
            if let Some(terminal) = &mut terminal {
                terminal.on_finish(&state, None);
            }
            if let Some(browser) = browser {
                let _ = browser.close(Duration::ZERO).await;
            }
            return Err(error.into());
        }
    };
    let snapshot_path = match html_target.parent() {
        Some(parent) => parent.join("snapshot.json"),
        None => {
            if let Some(terminal) = &mut terminal {
                terminal.on_finish(&state, None);
            }
            if let Some(browser) = browser {
                let _ = browser.close(Duration::ZERO).await;
            }
            return Err("Reserved report path is missing a parent directory".into());
        }
    };
    if let Err(error) = write_snapshot_atomic(&snapshot_path, state.snapshot()).await {
        failures.push(Failure {
            kind: FailureKind::ReportWrite,
            site: FailureSite::Report(snapshot_path.clone()),
            cause: error,
        });
    }

    let mut published_html: Option<PathBuf> = None;

    let report_validation = if options.write_html {
        let saved = async {
            let bundle = match bundle.take() {
                Some(existing) => existing,
                None => Arc::new(Bundle::embedded()?),
            };
            bundle.save(state.snapshot(), &html_target).await
        }
        .await;
        let validation = validate::run(failures, &html_target, saved).await;
        if let Some(path) = validation.report_path {
            published_html = Some(path.to_owned());
        }
        validation
            .result
            .map_err(Box::<dyn std::error::Error>::from)
    } else if failures.is_empty() {
        Ok(())
    } else {
        let combined = failures
            .drain(..)
            .map(|failure| failure.to_string())
            .collect::<Vec<_>>()
            .join("\nAdemás: ");
        Err(combined.into())
    };

    if let Some(terminal) = &mut terminal {
        terminal.on_finish(&state, published_html.as_deref());
    } else if let Some(path) = &published_html {
        println!("Report: {}", path.display());
    }

    if let Some(browser) = browser {
        let _ = browser.close(options.browser_grace).await;
    }

    report_validation?;
    Ok(RunArtifacts {
        snapshot_path,
        html_path: published_html,
    })
}

struct RunSession {
    state: Option<RunState>,
    terminal: Option<TerminalPresenter>,
    browser: Option<BrowserSession>,
    bundle: Option<Arc<Bundle>>,
    uses_browser: bool,
    open_browser: bool,
}

impl RunSession {
    fn drain(&mut self, rx: &mut mpsc::Receiver<IncomingMessage>) {
        while let Ok(message) = rx.try_recv() {
            self.handle(message);
        }
    }

    fn handle(&mut self, message: IncomingMessage) {
        match message {
            IncomingMessage::ProtocolError(error) => {
                eprintln!("[kat0-pipeline] Ignored event: {error}");
            }
            IncomingMessage::Event(event) => {
                if self.state.is_none() {
                    match RunState::start(event) {
                        Ok(run) => {
                            if let Some(terminal) = self.terminal.as_mut() {
                                terminal.on_start(&run);
                            }
                            if self.uses_browser
                                && let Some(bundle) = self.bundle.as_ref()
                            {
                                let initial = Arc::new(run.snapshot().clone());
                                match BrowserSession::start(
                                    initial,
                                    Arc::clone(bundle),
                                    self.open_browser,
                                ) {
                                    Ok(session) => self.browser = Some(session),
                                    Err(error) => {
                                        eprintln!(
                                            "[kat0-pipeline] Live browser viewer unavailable: {error}"
                                        );
                                    }
                                }
                            }
                            self.state = Some(run);
                        }
                        Err(error) => {
                            eprintln!("[kat0-pipeline] Invalid initial event: {error}");
                        }
                    }
                    return;
                }
                if let Some(run) = self.state.as_mut() {
                    match run.apply(event.clone()) {
                        Ok(updated) => {
                            if let Some(browser) = self.browser.as_ref() {
                                browser.publish(Arc::new(updated.clone()));
                            }
                            if let Some(terminal) = self.terminal.as_mut() {
                                terminal.on_event(&event, run);
                            }
                        }
                        Err(error) => {
                            eprintln!("[kat0-pipeline] Ignored invalid transition: {error}");
                        }
                    }
                }
            }
        }
    }
}

fn collect_failures(
    state: &RunState,
    child_status: Option<ExitStatus>,
    interrupted: bool,
) -> Vec<Failure> {
    let mut failures: Vec<Failure> = state.failures().iter().map(map_recorded_failure).collect();

    if interrupted {
        failures.push(Failure {
            kind: FailureKind::Execution,
            site: FailureSite::Run,
            cause: "Ejecución interrumpida por señal del usuario".into(),
        });
    } else if state.snapshot().status == Status::Failed && failures.is_empty() {
        failures.push(Failure {
            kind: FailureKind::Execution,
            site: FailureSite::Run,
            cause: "La ejecución terminó con fallos".into(),
        });
    }

    if let Some(status) = child_status
        && !status.success()
        && failures.is_empty()
    {
        failures.push(Failure {
            kind: FailureKind::Execution,
            site: FailureSite::Run,
            cause: format!("El productor devolvió código de salida no exitoso ({status})").into(),
        });
    }
    failures
}

fn map_recorded_failure(entry: &RecordedFailure) -> Failure {
    let kind = match entry.kind {
        ProtocolFailureKind::Startup => FailureKind::Startup,
        ProtocolFailureKind::Assertion => FailureKind::Assertion,
        ProtocolFailureKind::Timeout => FailureKind::Timeout,
        ProtocolFailureKind::Execution | _ => FailureKind::Execution,
    };
    let step = match (&entry.case_id, &entry.step_id) {
        (Some(case_id), Some(step_id)) => Some(format!("{case_id}/{step_id}")),
        (Some(case_id), None) => Some(case_id.clone()),
        (None, Some(step_id)) => Some(step_id.clone()),
        (None, None) => None,
    };
    Failure {
        kind,
        site: FailureSite::Task {
            job: entry.job_id.clone(),
            step,
        },
        cause: "El productor registró este fallo".into(),
    }
}

async fn write_snapshot_atomic(path: &Path, snapshot: &Snapshot) -> RunResult {
    let bytes = v2::encode(snapshot)?;
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || -> io::Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("El snapshot requiere un directorio"))?;
        std::fs::create_dir_all(parent)?;
        let mut staged = tempfile::Builder::new()
            .prefix(".snapshot-")
            .tempfile_in(parent)?;
        staged.write_all(&bytes)?;
        staged.as_file().sync_all()?;
        staged
            .persist_noclobber(&path)
            .map_err(|error| error.error)?;
        Ok(())
    })
    .await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use kat0_pipeline_protocol::v2::decode;
    use tokio::fs;

    use super::*;

    const FIXTURE_V3: &str = include_str!("../fixtures/web_regression_v3.ndjson");

    #[cfg(windows)]
    const PYTHON_BIN: &str = "python";
    #[cfg(not(windows))]
    const PYTHON_BIN: &str = "python3";

    fn python_sender_script(lines: &[&str], exit_code: i32, emit_stdout_noise: bool) -> String {
        let payload = serde_json::to_string(lines).unwrap();
        let noise = if emit_stdout_noise {
            "print('arbitrary stdout noise from cargo/tests', flush=True)\n\
             print('stderr diagnostic line', file=sys.stderr, flush=True)\n"
        } else {
            ""
        };
        format!(
            "import json, os, socket, sys, time\n\
             {noise}\
             endpoint = os.environ['{EVENTS_SOCK_ENV}']\n\
             lines = json.loads({payload:?})\n\
             if os.name == 'nt':\n    \
                 deadline = time.monotonic() + 5.0\n    \
                 while True:\n        \
                     try:\n            \
                         pipe = open(endpoint, 'wb', buffering=0)\n            \
                         break\n        \
                     except OSError:\n            \
                         if time.monotonic() >= deadline:\n                \
                             raise\n            \
                         time.sleep(0.01)\n    \
                 with pipe:\n        \
                     for line in lines:\n            \
                         pipe.write((line + '\\n').encode('utf-8'))\n\
             else:\n    \
                 sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)\n    \
                 sock.connect(endpoint)\n    \
                 for line in lines:\n        \
                     sock.sendall((line + '\\n').encode('utf-8'))\n    \
                 sock.close()\n\
             sys.exit({exit_code})\n"
        )
    }

    #[tokio::test]
    async fn terminal_mode_runs_with_no_html_and_ignores_child_stdout_noise() {
        let root = tempfile::tempdir().unwrap();
        let lines = [
            r#"{"type":"run_started","schema_version":3,"workers":1,"definition":{"title":"Pass","description":"Happy path","columns":[{"title":"Verify","jobs":[{"id":"job-1","title":"Check","is_test":true,"dependencies":[],"steps":[]}]}]}}"#,
            r#"{"type":"job_started","job_id":"job-1","at_ms":0}"#,
            r#"{"type":"step_started","job_id":"job-1","step_id":"s1","title":"Run check","at_ms":10}"#,
            r#"{"type":"step_finished","job_id":"job-1","step_id":"s1","status":"passed","at_ms":60}"#,
            r#"{"type":"job_finished","job_id":"job-1","status":"passed","at_ms":70}"#,
            r#"{"type":"run_finished","status":"passed","elapsed_ms":80}"#,
        ];
        let script = python_sender_script(&lines, 0, true);
        let mut options = RunOptions::new(
            PYTHON_BIN.into(),
            vec!["-c".into(), script.into()],
            root.path().to_owned(),
        );
        options.ui = UiMode::Terminal;
        options.write_html = false;
        options.open_browser = false;

        let artifacts = execute(options).await.unwrap();
        assert!(artifacts.html_path.is_none());
        let snapshot = decode(&fs::read(&artifacts.snapshot_path).await.unwrap()).unwrap();
        assert_eq!(snapshot.status, Status::Passed);
        assert_eq!(snapshot.elapsed_ms, 80);
    }

    #[tokio::test]
    async fn browser_mode_publishes_html_and_preserves_typed_failure_on_failed_run() {
        let root = tempfile::tempdir().unwrap();
        let mut lines: Vec<&str> = FIXTURE_V3
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect();
        // Inject a malformed line mid-stream to verify protocol error resilience.
        lines.insert(3, "{not-valid-json");
        let script = python_sender_script(&lines, 1, true);
        let mut options = RunOptions::new(
            PYTHON_BIN.into(),
            vec!["-c".into(), script.into()],
            root.path().to_owned(),
        );
        options.ui = UiMode::Browser;
        options.write_html = true;
        options.open_browser = false;
        options.browser_grace = Duration::from_millis(20);
        options.bundle = Some(Arc::new(Bundle::for_test()));

        let error = execute(options)
            .await
            .err()
            .expect("failed fixture must return error");
        let display = error.to_string();
        assert!(display.contains("Aserción fallida"));
        assert!(display.contains("errors-recovery"));
        assert!(display.contains("dns-races/retry-write"));

        // Even on failure, both snapshot.json and standalone index.html must be published.
        let mut entries = fs::read_dir(root.path()).await.unwrap();
        let run_dir = entries.next_entry().await.unwrap().unwrap().path();
        assert!(run_dir.join("index.html").is_file());
        let snapshot = decode(&fs::read(run_dir.join("snapshot.json")).await.unwrap()).unwrap();
        assert_eq!(snapshot.status, Status::Failed);
    }

    #[tokio::test]
    async fn abrupt_child_exit_finalizes_aborted_snapshot_and_saves_html() {
        let root = tempfile::tempdir().unwrap();
        let lines = [
            r#"{"type":"run_started","schema_version":3,"definition":{"title":"Crash","description":"Abrupt exit","columns":[{"title":"Verify","jobs":[{"id":"job-1","title":"Check","is_test":true,"dependencies":[],"steps":[{"id":"s1","title":"Step 1"},{"id":"s2","title":"Step 2"}]}]}]}}"#,
            r#"{"type":"job_started","job_id":"job-1","at_ms":0}"#,
            r#"{"type":"step_started","job_id":"job-1","step_id":"s1","at_ms":5}"#,
        ];
        let script = python_sender_script(&lines, 2, false);
        let mut options = RunOptions::new(
            PYTHON_BIN.into(),
            vec!["-c".into(), script.into()],
            root.path().to_owned(),
        );
        options.ui = UiMode::Terminal;
        options.write_html = true;
        options.open_browser = false;
        options.bundle = Some(Arc::new(Bundle::for_test()));

        assert!(execute(options).await.is_err());

        let mut entries = fs::read_dir(root.path()).await.unwrap();
        let run_dir = entries.next_entry().await.unwrap().unwrap().path();
        assert!(run_dir.join("index.html").is_file());
        let snapshot = decode(&fs::read(run_dir.join("snapshot.json")).await.unwrap()).unwrap();
        assert_eq!(snapshot.status, Status::Failed);
        assert_eq!(
            snapshot.row("job-1", Some("s1")).map(|row| row.status),
            Some(Status::Failed)
        );
        assert_eq!(
            snapshot.row("job-1", Some("s2")).map(|row| row.status),
            Some(Status::Skipped)
        );
    }

    #[tokio::test]
    async fn closes_browser_cleanly_when_report_root_cannot_be_reserved() {
        let scratch = tempfile::tempdir().unwrap();
        let blocked_root = scratch.path().join("not-a-dir");
        fs::write(&blocked_root, b"existing file").await.unwrap();
        let lines = [
            r#"{"type":"run_started","schema_version":3,"workers":1,"definition":{"title":"Pass","description":"Blocked report root","columns":[{"title":"Verify","jobs":[{"id":"job-1","title":"Check","is_test":true,"dependencies":[],"steps":[]}]}]}}"#,
            r#"{"type":"job_started","job_id":"job-1","at_ms":0}"#,
            r#"{"type":"job_finished","job_id":"job-1","status":"passed","at_ms":10}"#,
            r#"{"type":"run_finished","status":"passed","elapsed_ms":10}"#,
        ];
        let script = python_sender_script(&lines, 0, false);
        let mut options = RunOptions::new(
            PYTHON_BIN.into(),
            vec!["-c".into(), script.into()],
            blocked_root.clone(),
        );
        options.ui = UiMode::Browser;
        options.write_html = true;
        options.open_browser = false;
        options.browser_grace = Duration::from_secs(5);
        options.bundle = Some(Arc::new(Bundle::for_test()));

        // Should fail promptly (closing browser with Duration::ZERO instead of waiting 5s grace).
        let result = tokio::time::timeout(Duration::from_secs(3), execute(options))
            .await
            .expect("must close browser promptly on reserve failure");
        assert!(result.is_err());
        assert_eq!(fs::read(&blocked_root).await.unwrap(), b"existing file");
    }
}
