// SPDX-License-Identifier: AGPL-3.0-only

#![forbid(unsafe_code)]

mod panel;
mod sha256;

use std::process::ExitCode;

const USAGE: &str = "Usage: cargo run -p xtask -- <build-panel | verify-panel [--manifest-only]>";

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
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["build-panel"] => panel::build_and_stage(),
        ["verify-panel"] => panel::verify(true),
        ["verify-panel", "--manifest-only"] => panel::verify(false),
        _ => Err(USAGE.into()),
    }
}
