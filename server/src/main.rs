// SPDX-License-Identifier: AGPL-3.0-only

#![forbid(unsafe_code)]

use std::process::ExitCode;

mod bundle;
mod cli;
mod http;
mod import;
mod ipc;
mod prepare;
mod runner;
mod sse;
mod ui;
mod validate;
mod wire;

pub(crate) type RunResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

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
