// SPDX-License-Identifier: AGPL-3.0-only

//! NDJSON v3 streaming event protocol and deterministic state reducer.

mod event;
mod reducer;

pub use event::{Event, RunOutcome, SCHEMA_VERSION, TerminalStatus, decode_line, encode_line};
pub use reducer::{ProtocolError, RecordedFailure, RunState};
