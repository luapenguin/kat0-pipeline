// SPDX-License-Identifier: AGPL-3.0-only

#![forbid(unsafe_code)]

//! Minimal end-to-end Playwright demo orchestrated by `kat0-pipeline`.
//!
//! Run with:
//! ```sh
//! kat0-pipeline run --ui browser -- cargo run --locked -p kat0-pipeline-playwright --example playwright_demo
//! ```

use std::{process::ExitCode, time::Duration};

use kat0_pipeline_playwright::{
    FirefoxSession, classify_error,
    playwright_rs::{LaunchOptions, expect},
    reporter::{
        Reporter, RunResult,
        execute::{combine, within},
        protocol::builder::PipelineBuilder,
    },
    with_page,
};

const LAUNCH_TIMEOUT: Duration = Duration::from_secs(30);
const ACTION_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run_demo().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Demo failed: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run_demo() -> RunResult {
    let definition = PipelineBuilder::new(
        "Playwright demo",
        "Self-contained Firefox verification using kat0-pipeline-playwright.",
    )
    .stage("Prepare", |stage| {
        stage.job("prepare", "Browser", |job| {
            job.step("launch-firefox", "Launch Playwright Firefox");
        });
    })
    .stage("Verify", |stage| {
        stage.test_job("smoke", "Smoke", |job| {
            job.depends_on("prepare")
                .case("heading", "Render heading", |_| {});
        });
    })
    .stage("Cleanup", |stage| {
        stage.job("cleanup", "Clean up", |job| {
            job.depends_on("smoke")
                .step("close-firefox", "Close Firefox and driver");
        });
    })
    .build()?;

    let reporter = Reporter::start(definition, 1).await?;
    let ticks = reporter.start_ticking(Duration::from_millis(250));

    let session = reporter
        .job("prepare", async {
            reporter
                .step("prepare", None, "launch-firefox", None, async {
                    FirefoxSession::launch(LaunchOptions::new().headless(true), LAUNCH_TIMEOUT)
                        .await
                })
                .await
        })
        .await;

    let outcome = match session {
        Ok(session) => {
            let checks = reporter
                .job("smoke", async {
                    reporter
                        .case("smoke", "heading", None, async {
                            with_page(
                                session.browser(),
                                None,
                                ACTION_TIMEOUT,
                                ACTION_TIMEOUT,
                                async |page| {
                                    reporter
                                        .step(
                                            "smoke",
                                            Some("heading"),
                                            "set-content",
                                            Some("Load local HTML document"),
                                            async {
                                                within(
                                                    ACTION_TIMEOUT,
                                                    page.set_content(
                                                        "<main><h1>kat0-pipeline ready</h1></main>",
                                                        None,
                                                    ),
                                                )
                                                .await
                                            },
                                        )
                                        .await?;
                                    reporter
                                        .step(
                                            "smoke",
                                            Some("heading"),
                                            "assert-heading",
                                            Some("Verify rendered heading"),
                                            async {
                                                expect(page.locator("h1"))
                                                    .to_have_text("kat0-pipeline ready")
                                                    .await
                                                    .map_err(classify_error)
                                            },
                                        )
                                        .await
                                },
                            )
                            .await
                        })
                        .await
                })
                .await;

            let cleanup = reporter
                .job("cleanup", async {
                    reporter
                        .step("cleanup", None, "close-firefox", None, async {
                            session.close(ACTION_TIMEOUT).await
                        })
                        .await
                })
                .await;

            combine(checks, cleanup)
        }
        Err(error) => Err(error),
    };

    ticks.stop().await;
    let _snapshot = reporter.finish(outcome.is_ok()).await?;
    outcome
}
