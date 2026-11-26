// SPDX-License-Identifier: AGPL-3.0-only

use std::{ffi::OsString, path::Path};

use kat0_pipeline::RunResult;

mod live;
mod preview;
mod publication;
mod run;

const VERSION_LINE: &str = concat!(
    "kat0-pipeline ",
    env!("CARGO_PKG_VERSION"),
    " (protocol v1/v2/v3)"
);

const MAIN_HELP: &str = "\
kat0-pipeline — live terminal/browser pipeline runner and standalone HTML report generator

USAGE:
    kat0-pipeline <COMMAND> [OPTIONS]
    kat0-pipeline --version | --help

COMMANDS:
    run       Orchestrate a command over NDJSON v3 IPC and render live/offline progress
    sample    Generate a standalone offline sample HTML report (no tests executed)
    import    Import a finished v1 or v2 JSON result into a standalone HTML report
    serve     Serve a v2 snapshot JSON file on loopback with SSE live updates

OPTIONS:
    -h, --help       Print help information (also works after any subcommand)
    -V, --version    Print version and supported protocol versions";

const SAMPLE_HELP: &str = "\
USAGE:
    kat0-pipeline sample

Generates a standalone offline HTML report from the bundled sample fixture without
executing any jobs or making network requests.";

const IMPORT_HELP: &str = "\
USAGE:
    kat0-pipeline import <FILE>

Validates a finished v1 or v2 JSON result file and writes a standalone offline HTML report.";

const SERVE_HELP: &str = "\
USAGE:
    kat0-pipeline serve <FILE>

Validates a v2 snapshot JSON file, serves the interactive panel over loopback HTTP/SSE,
prints the origin URL to stdout, and shuts down cleanly on stdin EOF.";

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Print(&'static str),
    Run(run::RunCommand),
    Sample,
    Import(OsString),
    Serve(OsString),
}

pub(crate) async fn dispatch() -> RunResult {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    match parse(&args)? {
        Action::Print(text) => {
            println!("{text}");
            Ok(())
        }
        Action::Run(command) => run::execute(command).await,
        Action::Sample => preview::report().await,
        Action::Import(path) => publication::import_report(Path::new(&path)).await,
        Action::Serve(path) => live::serve(Path::new(&path)).await,
    }
}

fn is_help(flag: &OsString) -> bool {
    matches!(flag.to_str(), Some("-h" | "--help"))
}

fn parse(args: &[OsString]) -> Result<Action, String> {
    match args {
        [] => Err(MAIN_HELP.to_owned()),
        [flag] if is_help(flag) || flag == "help" => Ok(Action::Print(MAIN_HELP)),
        [flag] if matches!(flag.to_str(), Some("-V" | "--version")) => {
            Ok(Action::Print(VERSION_LINE))
        }
        [cmd, rest @ ..] if cmd == "help" => match rest {
            [sub] if sub == "run" => Ok(Action::Print(run::HELP)),
            [sub] if sub == "sample" => Ok(Action::Print(SAMPLE_HELP)),
            [sub] if sub == "import" => Ok(Action::Print(IMPORT_HELP)),
            [sub] if sub == "serve" => Ok(Action::Print(SERVE_HELP)),
            _ => Err(MAIN_HELP.to_owned()),
        },
        [cmd, rest @ ..] if cmd == "run" => match run::parse(rest)? {
            run::ParsedRun::Help => Ok(Action::Print(run::HELP)),
            run::ParsedRun::Command(parsed) => Ok(Action::Run(parsed)),
        },
        [cmd] if cmd == "sample" => Ok(Action::Sample),
        [cmd, flag] if cmd == "sample" && is_help(flag) => Ok(Action::Print(SAMPLE_HELP)),
        [cmd, flag] if cmd == "import" && is_help(flag) => Ok(Action::Print(IMPORT_HELP)),
        [cmd, path] if cmd == "import" => Ok(Action::Import(path.clone())),
        [cmd, flag] if cmd == "serve" && is_help(flag) => Ok(Action::Print(SERVE_HELP)),
        [cmd, path] if cmd == "serve" => Ok(Action::Serve(path.clone())),
        _ => Err(format!("Invalid arguments.\n\n{MAIN_HELP}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os(items: &[&str]) -> Vec<OsString> {
        items.iter().map(OsString::from).collect()
    }

    #[test]
    fn parses_version_help_and_subcommands() {
        assert_eq!(parse(&os(&["--version"])), Ok(Action::Print(VERSION_LINE)));
        assert_eq!(parse(&os(&["-V"])), Ok(Action::Print(VERSION_LINE)));
        assert_eq!(parse(&os(&["--help"])), Ok(Action::Print(MAIN_HELP)));
        assert_eq!(parse(&os(&["-h"])), Ok(Action::Print(MAIN_HELP)));
        assert_eq!(parse(&os(&["help", "run"])), Ok(Action::Print(run::HELP)));
        assert_eq!(parse(&os(&["run", "--help"])), Ok(Action::Print(run::HELP)));
        assert_eq!(
            parse(&os(&["sample", "-h"])),
            Ok(Action::Print(SAMPLE_HELP))
        );
        assert_eq!(
            parse(&os(&["import", "--help"])),
            Ok(Action::Print(IMPORT_HELP))
        );
        assert_eq!(
            parse(&os(&["serve", "--help"])),
            Ok(Action::Print(SERVE_HELP))
        );
        assert_eq!(parse(&os(&["sample"])), Ok(Action::Sample));
        assert_eq!(
            parse(&os(&["import", "result.json"])),
            Ok(Action::Import(OsString::from("result.json")))
        );
        assert_eq!(
            parse(&os(&["serve", "snapshot.json"])),
            Ok(Action::Serve(OsString::from("snapshot.json")))
        );
    }

    #[test]
    fn rejects_removed_legacy_flags_and_missing_args() {
        assert!(parse(&os(&[])).is_err());
        assert!(parse(&os(&["--sample-report"])).is_err());
        assert!(parse(&os(&["--import-e2e", "a.json"])).is_err());
        assert!(parse(&os(&["--serve", "a.json"])).is_err());
        assert!(parse(&os(&["unknown"])).is_err());
    }
}
