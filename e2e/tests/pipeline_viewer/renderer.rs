// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    env,
    path::{Path, PathBuf, absolute},
    process::Stdio,
    time::Duration,
};

use tokio::{
    fs,
    io::{self, AsyncBufReadExt as _, BufReader},
    process::Command,
};

use super::{BoxError, CLOSE, TestResult, within, workspace_root};

const BUILD: &str = "cargo build --locked -p kat0-pipeline --bin kat0-pipeline";
const CLI_TIMEOUT: Duration = Duration::from_secs(180);
const LOOPBACK: &str = "http://127.0.0.1:";
const LIVE_URL_PREFIX: &str = "[kat0-pipeline] ";
const RUN_REPORT_PREFIX: &str = "Report: ";

pub(super) struct Renderer {
    bin: PathBuf,
    root: PathBuf,
}

impl Renderer {
    pub(super) fn new(root: &Path) -> TestResult<Self> {
        let bin = absolute(env::var_os("PIPELINE_REPORT_BIN").map_or_else(
            || workspace_root().join("target/debug/kat0-pipeline"),
            PathBuf::from,
        ))?;
        if !bin.is_file() {
            return Err(format!(
                "Missing prebuilt binary {}; run `{BUILD}` from the workspace root or set PIPELINE_REPORT_BIN",
                bin.display(),
            )
            .into());
        }
        Ok(Self {
            bin,
            root: root.canonicalize()?,
        })
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    pub(super) async fn sample(&self) -> TestResult<PathBuf> {
        self.report("sample", None, "Sample report (no tests executed): ", false)
            .await
    }

    pub(super) async fn import_failed(&self, input: &Path) -> TestResult<PathBuf> {
        self.report("import", Some(input), "Imported report: ", true)
            .await
    }

    async fn report(
        &self,
        flag: &str,
        input: Option<&Path>,
        prefix: &str,
        expect_failure: bool,
    ) -> TestResult<PathBuf> {
        let output = within(
            CLI_TIMEOUT,
            flag,
            self.command()
                .arg(flag)
                .args(input)
                .stdin(Stdio::null())
                .output(),
        )
        .await?;
        let expected_exit = if expect_failure {
            output.status.code().is_some_and(|code| code != 0)
        } else {
            output.status.success()
        };
        if !expected_exit {
            return Err(format!("{flag}: unexpected exit status ({})", output.status).into());
        }

        let stdout = std::str::from_utf8(&output.stdout)?;
        let path = stdout
            .lines()
            .find_map(|line| line.strip_prefix(prefix))
            .map(Path::new)
            .ok_or_else(|| format!("{flag}: did not announce report path on stdout"))?;
        if !path.is_absolute() {
            return Err(format!("{flag}: announced report path is not absolute").into());
        }
        let path = fs::canonicalize(path).await?;
        if !path.starts_with(&self.root) || !path.is_file() {
            return Err(
                format!("{flag}: did not save report file inside PIPELINE_REPORT_ROOT").into(),
            );
        }
        Ok(path)
    }

    pub(super) async fn with_live(
        &self,
        input: &Path,
        check: impl AsyncFnOnce(&str) -> TestResult,
    ) -> TestResult {
        let mut child = self
            .command()
            .arg("serve")
            .arg(input)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let mut stdout = BufReader::new(child.stdout.take().ok_or("Missing serve stdout")?);
        let checked = async {
            let mut line = String::new();
            within(CLI_TIMEOUT, "Starting serve", stdout.read_line(&mut line)).await?;
            let origin = parse_loopback_origin(line.trim_end_matches('\n'))
                .ok_or("serve did not announce a valid loopback origin on stdout")?;
            check(&origin).await
        }
        .await;

        // Keep both pipes alive through the browser check; stdin EOF owns shutdown.
        drop(child.stdin.take());
        let closed = within(CLOSE, "Closing serve on stdin EOF", async {
            let mut sink = io::sink();
            let (exit, extra_bytes) =
                tokio::try_join!(child.wait(), io::copy(&mut stdout, &mut sink))?;
            if !exit.success() {
                return Err(format!("serve failed to exit cleanly on stdin EOF ({exit})").into());
            }
            if extra_bytes != 0 {
                return Err(
                    "serve printed unexpected output beyond the origin announcement".into(),
                );
            }
            Ok::<(), BoxError>(())
        })
        .await;
        checked.and(closed)
    }

    pub(super) async fn with_orchestrated_run(
        &self,
        program: &str,
        args: &[&str],
        check: impl AsyncFnOnce(&str) -> TestResult,
    ) -> TestResult<PathBuf> {
        let mut child = self
            .command()
            .env("PIPELINE_NO_OPEN", "1")
            .args(["run", "--ui", "browser", "--report-dir"])
            .arg(&self.root)
            .arg("--")
            .arg(program)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .spawn()?;
        let mut stdout = BufReader::new(child.stdout.take().ok_or("Missing run stdout")?);

        let origin = within(CLI_TIMEOUT, "Starting run --ui browser", async {
            let mut line = String::new();
            loop {
                line.clear();
                let read = stdout.read_line(&mut line).await?;
                if read == 0 {
                    return Err::<String, BoxError>(
                        "kat0-pipeline run exited before announcing its loopback origin".into(),
                    );
                }
                if let Some(url) = line.trim_end().strip_prefix(LIVE_URL_PREFIX)
                    && let Some(origin) = parse_loopback_origin(url)
                {
                    return Ok(origin);
                }
            }
        })
        .await?;

        let checked = check(&origin).await;
        let finished = within(CLI_TIMEOUT, "Finishing run --ui browser", async {
            let mut remaining = String::new();
            let (exit, _) = tokio::try_join!(child.wait(), async {
                let mut line = String::new();
                while stdout.read_line(&mut line).await? != 0 {
                    remaining.push_str(&line);
                    line.clear();
                }
                Ok::<(), io::Error>(())
            })?;
            if !exit.success() {
                return Err::<PathBuf, BoxError>(
                    format!("kat0-pipeline run returned unexpected status ({exit})").into(),
                );
            }
            let report = remaining
                .lines()
                .find_map(|line| line.strip_prefix(RUN_REPORT_PREFIX))
                .map(Path::new)
                .ok_or("kat0-pipeline run did not announce final HTML report")?;
            let canonical = fs::canonicalize(report).await?;
            if !canonical.starts_with(&self.root) || !canonical.is_file() {
                return Err("kat0-pipeline run saved report outside --report-dir".into());
            }
            Ok(canonical)
        })
        .await;

        checked?;
        finished
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.bin);
        command
            .env("PIPELINE_REPORT_ROOT", &self.root)
            .stderr(Stdio::null())
            .kill_on_drop(true);
        command
    }
}

pub(super) fn parse_loopback_origin(raw: &str) -> Option<String> {
    let trimmed = raw.trim_end_matches('/');
    let port = trimmed
        .strip_prefix(LOOPBACK)?
        .parse::<u16>()
        .ok()
        .filter(|&port| port != 0)?;
    Some(format!("{LOOPBACK}{port}"))
}
