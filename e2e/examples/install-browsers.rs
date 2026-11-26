// SPDX-License-Identifier: AGPL-3.0-only

#![forbid(unsafe_code)]

use std::{env, process::ExitCode};

use kat0_pipeline_playwright::playwright_rs::{install_browsers, install_browsers_with_deps};

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let mut with_deps = false;
    let mut browsers: Vec<String> = Vec::new();
    for arg in env::args().skip(1) {
        if arg == "--with-deps" {
            with_deps = true;
        } else {
            browsers.push(arg);
        }
    }

    let browser_refs: Vec<&str> = browsers.iter().map(String::as_str).collect();
    let selection = (!browser_refs.is_empty()).then_some(browser_refs.as_slice());
    let result = if with_deps {
        install_browsers_with_deps(selection).await
    } else {
        install_browsers(selection).await
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Failed to install Playwright browsers: {error}");
            ExitCode::FAILURE
        }
    }
}
