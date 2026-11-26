// SPDX-License-Identifier: AGPL-3.0-only

#![forbid(unsafe_code)]

use std::process::ExitCode;

mod cli;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match cli::dispatch().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error}");
            ExitCode::FAILURE
        }
    }
}
