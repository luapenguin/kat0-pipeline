// SPDX-License-Identifier: AGPL-3.0-only

#![forbid(unsafe_code)]

pub mod bundle;
pub mod http;
pub mod import;
pub mod ipc;
pub mod prepare;
pub mod runner;
mod sse;
pub mod ui;
pub mod validate;
pub mod wire;

pub use kat0_pipeline_protocol as protocol;

pub type RunResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
