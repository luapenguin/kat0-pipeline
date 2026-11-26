// SPDX-License-Identifier: AGPL-3.0-only

//! Async NDJSON v3 event reporter, atomic snapshot persistence, and execution combinators.
//!
//! This crate connects test runners (such as `playwright-rs` suites) to a parent pipeline
//! orchestrator over a private local IPC endpoint (Unix domain socket on Unix, local-only
//! Named Pipe on Windows) while preserving full standalone operation when run directly.
//!
//! # Quick Example
//!
//! ```no_run
//! use kat0_pipeline_reporter::{
//!     Reporter,
//!     protocol::builder::PipelineBuilder,
//! };
//!
//! # async fn example() -> kat0_pipeline_reporter::RunResult {
//! let definition = PipelineBuilder::new("Web regression", "Smoke checks")
//!     .stage("Verify", |stage| {
//!         stage.test_job("smoke", "Smoke", |job| {
//!             job.step("open", "Open home page");
//!         });
//!     })
//!     .build()?;
//!
//! let reporter = Reporter::start(definition, 1).await?;
//! let outcome = reporter
//!     .job("smoke", async {
//!         reporter
//!             .step("smoke", None, "open", None, async { Ok::<_, std::io::Error>(()) })
//!             .await
//!     })
//!     .await;
//! let _snapshot = reporter.finish(outcome.is_ok()).await?;
//! outcome
//! # }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod error;
pub mod execute;
pub mod reporter;
pub mod snapshot;

pub use kat0_pipeline_protocol as protocol;

pub use error::{BoxError, CombinedError, RunResult, TaskFailure, TypedFailure};
pub use reporter::{EVENTS_SOCK_ENV, Reporter, TickGuard};
pub use snapshot::SnapshotFile;
