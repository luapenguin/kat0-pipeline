// SPDX-License-Identifier: AGPL-3.0-only

//! Pure, I/O-free pipeline protocol types, snapshot codecs, and NDJSON v3 event reducer.
//!
//! This crate defines the hierarchical `Job -> Case -> Step` execution model shared between
//! pipeline orchestrators, live browser panels (WASM), and test reporters.
//!
//! # Quick Example
//!
//! ```
//! use kat0_pipeline_protocol::{
//!     builder::PipelineBuilder,
//!     model::Status,
//!     v3::{Event, RunOutcome, RunState, SCHEMA_VERSION, TerminalStatus},
//! };
//!
//! let definition = PipelineBuilder::new("Web regression", "Smoke suite")
//!     .stage("Verify", |stage| {
//!         stage.test_job("auth", "Authentication", |job| {
//!             job.step("login", "Sign in");
//!         });
//!     })
//!     .build()
//!     .expect("valid pipeline definition");
//!
//! let mut state = RunState::start(Event::RunStarted {
//!     schema_version: SCHEMA_VERSION,
//!     definition,
//!     workers: Some(1),
//! })
//! .expect("valid run start");
//!
//! state.apply(Event::JobStarted { job_id: "auth".into(), at_ms: 0 }).unwrap();
//! state.apply(Event::StepStarted {
//!     job_id: "auth".into(),
//!     case_id: None,
//!     step_id: "login".into(),
//!     title: None,
//!     at_ms: 5,
//! }).unwrap();
//! state.apply(Event::StepFinished {
//!     job_id: "auth".into(),
//!     case_id: None,
//!     step_id: "login".into(),
//!     status: TerminalStatus::Passed,
//!     at_ms: 50,
//!     kind: None,
//! }).unwrap();
//! state.apply(Event::JobFinished {
//!     job_id: "auth".into(),
//!     status: TerminalStatus::Passed,
//!     at_ms: 55,
//! }).unwrap();
//! let snapshot = state
//!     .apply(Event::RunFinished { status: RunOutcome::Passed, elapsed_ms: 60 })
//!     .unwrap();
//! assert_eq!(snapshot.status, Status::Passed);
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod builder;
pub mod limits;
pub mod model;
pub mod v2;
pub mod v3;
