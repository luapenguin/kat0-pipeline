// SPDX-License-Identifier: AGPL-3.0-only

#![forbid(unsafe_code)]

mod ci;
mod panel;
mod sha256;

use std::process::ExitCode;

const USAGE: &str = "\
Usage:
    cargo run --locked -p xtask -- build-panel
    cargo run --locked -p xtask -- verify-panel [--manifest-only]
    cargo run --locked -p xtask -- ci [--list | --only <check>[,<check>...]]";

#[derive(Debug, PartialEq, Eq)]
enum Task {
    Help,
    BuildPanel,
    VerifyPanel { rebuild: bool },
    Ci(ci::CiAction),
}

fn main() -> ExitCode {
    match dispatch() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn dispatch() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    match parse(&refs)? {
        Task::Help => {
            println!("{USAGE}");
            Ok(())
        }
        Task::BuildPanel => panel::build_and_stage(),
        Task::VerifyPanel { rebuild } => panel::verify(rebuild),
        Task::Ci(action) => ci::execute(action),
    }
}

fn parse(args: &[&str]) -> Result<Task, Box<dyn std::error::Error>> {
    match args {
        ["-h" | "--help" | "help"] => Ok(Task::Help),
        ["build-panel"] => Ok(Task::BuildPanel),
        ["verify-panel"] => Ok(Task::VerifyPanel { rebuild: true }),
        ["verify-panel", "--manifest-only"] => Ok(Task::VerifyPanel { rebuild: false }),
        ["ci", rest @ ..] => Ok(Task::Ci(ci::parse_args(rest)?)),
        _ => Err(USAGE.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_xtask_commands() {
        assert_eq!(parse(&["--help"]).unwrap(), Task::Help);
        assert_eq!(parse(&["build-panel"]).unwrap(), Task::BuildPanel);
        assert_eq!(
            parse(&["verify-panel"]).unwrap(),
            Task::VerifyPanel { rebuild: true }
        );
        assert_eq!(
            parse(&["verify-panel", "--manifest-only"]).unwrap(),
            Task::VerifyPanel { rebuild: false }
        );
        assert_eq!(
            parse(&["ci"]).unwrap(),
            Task::Ci(ci::CiAction::Run(ci::Check::ALL.to_vec()))
        );
        assert_eq!(
            parse(&["ci", "--list"]).unwrap(),
            Task::Ci(ci::CiAction::List)
        );
        assert!(parse(&[]).is_err());
        assert!(parse(&["unknown"]).is_err());
    }
}
