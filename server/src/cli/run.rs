// SPDX-License-Identifier: AGPL-3.0-only

use std::{ffi::OsString, path::PathBuf, time::Duration};

use kat0_pipeline::{
    RunResult,
    runner::{self, RunOptions},
    ui::{UiMode, browser::DEFAULT_GRACE_PERIOD},
};

use super::publication::report_root;

const MAX_GRACE_MS: u64 = 60_000;

pub(super) const HELP: &str = "\
USAGE:
    kat0-pipeline run [OPTIONS] -- <COMMAND> [ARGS...]

OPTIONS:
    --ui <MODE>         UI presenter: terminal (default), browser, or both
    --no-html           Skip standalone HTML report generation (still writes snapshot.json)
    --no-open           Do not open a browser automatically in browser/both mode
    --report-dir <DIR>  Root directory where run-*/ artifacts are created
    --grace-ms <MS>     Loopback server grace period after completion (0..=60000, default 2000)
    -h, --help          Print help information";

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ParsedRun {
    Help,
    Command(RunCommand),
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct RunCommand {
    pub(super) ui: UiMode,
    pub(super) write_html: bool,
    pub(super) no_open: bool,
    pub(super) grace: Duration,
    pub(super) report_root: PathBuf,
    pub(super) command: OsString,
    pub(super) args: Vec<OsString>,
}

pub(super) fn parse(args: &[OsString]) -> Result<ParsedRun, String> {
    let mut ui = UiMode::Terminal;
    let mut write_html = true;
    let mut no_open = false;
    let mut grace = DEFAULT_GRACE_PERIOD;
    let mut root = None;
    let mut index = 0;

    while index < args.len() {
        let flag = &args[index];
        if flag == "--" {
            index += 1;
            break;
        }
        match flag.to_str() {
            Some("-h" | "--help") => return Ok(ParsedRun::Help),
            Some("--ui") => {
                let value = args
                    .get(index + 1)
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| {
                        "Missing value for --ui (expected terminal, browser, or both)".to_owned()
                    })?;
                ui = UiMode::parse(value)?;
                index += 2;
            }
            Some("--no-html") => {
                write_html = false;
                index += 1;
            }
            Some("--no-open") => {
                no_open = true;
                index += 1;
            }
            Some("--report-dir") => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "Missing path for --report-dir".to_owned())?;
                root = Some(PathBuf::from(value));
                index += 2;
            }
            Some("--grace-ms") => {
                let raw = args
                    .get(index + 1)
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| "Missing integer value for --grace-ms".to_owned())?;
                let ms: u64 = raw
                    .parse()
                    .map_err(|_| format!("Invalid --grace-ms value: {raw:?}"))?;
                if ms > MAX_GRACE_MS {
                    return Err(format!(
                        "--grace-ms must be at most {MAX_GRACE_MS} ms (got {ms})"
                    ));
                }
                grace = Duration::from_millis(ms);
                index += 2;
            }
            Some(other) if other.starts_with('-') => {
                return Err(format!("Unrecognized option for `run`: {other}"));
            }
            _ => break,
        }
    }

    let Some(command) = args.get(index).cloned() else {
        return Err(
            "Missing command to execute. Usage: kat0-pipeline run [--ui terminal|browser|both] [--no-html] [--no-open] [--report-dir DIR] [--grace-ms MS] -- <cmd> [args...]"
                .to_owned(),
        );
    };
    let child_args = args[index + 1..].to_vec();
    Ok(ParsedRun::Command(RunCommand {
        ui,
        write_html,
        no_open,
        grace,
        report_root: root.unwrap_or_else(report_root),
        command,
        args: child_args,
    }))
}

pub(super) async fn execute(parsed: RunCommand) -> RunResult {
    let mut options = RunOptions::new(parsed.command, parsed.args, parsed.report_root);
    options.ui = parsed.ui;
    options.write_html = parsed.write_html;
    if parsed.no_open {
        options.open_browser = false;
    }
    options.browser_grace = parsed.grace;
    let _artifacts = runner::execute(options).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os(items: &[&str]) -> Vec<OsString> {
        items.iter().map(OsString::from).collect()
    }

    #[test]
    fn parses_run_flags_and_passthrough_command() {
        let ParsedRun::Command(parsed) = parse(&os(&[
            "--ui",
            "browser",
            "--no-html",
            "--no-open",
            "--grace-ms",
            "500",
            "--report-dir",
            "/tmp/out",
            "--",
            "cargo",
            "run",
            "--",
            "--filter",
            "orgs",
        ]))
        .unwrap() else {
            panic!("expected command");
        };
        assert_eq!(parsed.ui, UiMode::Browser);
        assert!(!parsed.write_html);
        assert!(parsed.no_open);
        assert_eq!(parsed.grace, Duration::from_millis(500));
        assert_eq!(parsed.report_root, PathBuf::from("/tmp/out"));
        assert_eq!(parsed.command, OsString::from("cargo"));
        assert_eq!(parsed.args, os(&["run", "--", "--filter", "orgs"]));

        let ParsedRun::Command(default) = parse(&os(&["--", "echo", "ok"])).unwrap() else {
            panic!("expected default command");
        };
        assert_eq!(default.ui, UiMode::Terminal);
        assert!(default.write_html);
        assert!(!default.no_open);
        assert_eq!(default.grace, DEFAULT_GRACE_PERIOD);
    }

    #[test]
    fn rejects_invalid_ui_grace_or_missing_command() {
        assert!(parse(&os(&["--ui", "invalid", "--", "echo"])).is_err());
        assert!(parse(&os(&["--ui", "terminal", "--"])).is_err());
        assert!(parse(&os(&["--grace-ms", "999999", "--", "echo"])).is_err());
        assert!(parse(&os(&["--grace-ms", "abc", "--", "echo"])).is_err());
        assert!(parse(&os(&["--unknown", "--", "echo"])).is_err());
    }
}
