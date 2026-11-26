// SPDX-License-Identifier: AGPL-3.0-only

use serde::{Deserialize, Serialize};

use crate::{
    limits::{MAX_EVENT_LINE_BYTES, MAX_RUN_STARTED_LINE_BYTES},
    model::{Definition, FailureKind, Status},
};

use super::ProtocolError;

/// Current NDJSON streaming protocol schema version.
pub const SCHEMA_VERSION: u32 = 3;

/// Terminal status for a job, case, or step event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TerminalStatus {
    /// Item succeeded.
    Passed,
    /// Item failed.
    Failed,
    /// Item was skipped.
    Skipped,
}

impl From<TerminalStatus> for Status {
    fn from(status: TerminalStatus) -> Self {
        match status {
            TerminalStatus::Passed => Self::Passed,
            TerminalStatus::Failed => Self::Failed,
            TerminalStatus::Skipped => Self::Skipped,
        }
    }
}

/// Terminal outcome for an entire pipeline run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunOutcome {
    /// Every executed item passed.
    Passed,
    /// At least one item failed or the run ended unsuccessfully.
    Failed,
}

impl From<RunOutcome> for Status {
    fn from(outcome: RunOutcome) -> Self {
        match outcome {
            RunOutcome::Passed => Self::Passed,
            RunOutcome::Failed => Self::Failed,
        }
    }
}

/// A single NDJSON v3 transition event emitted by a test producer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum Event {
    /// Declares the pipeline topology and starts the run. Must be the first event.
    RunStarted {
        /// Protocol version; must equal [`SCHEMA_VERSION`].
        schema_version: u32,
        /// Full pipeline stage/job topology.
        definition: Definition,
        /// Optional concurrency hint (`>= 1`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workers: Option<u16>,
    },
    /// Marks a job as running.
    JobStarted {
        /// Job identifier from the pipeline definition.
        job_id: String,
        /// Monotonic start timestamp in milliseconds since run start.
        at_ms: u64,
    },
    /// Marks a case as running, dynamically registering it when `title` is provided.
    CaseStarted {
        /// Parent job identifier.
        job_id: String,
        /// Case identifier within the job.
        case_id: String,
        /// Optional title used to dynamically register an undeclared case.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// Monotonic start timestamp in milliseconds.
        at_ms: u64,
    },
    /// Marks a step as running, dynamically registering it when `title` is provided.
    StepStarted {
        /// Parent job identifier.
        job_id: String,
        /// Optional parent case identifier when the step belongs to a case.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        case_id: Option<String>,
        /// Step identifier within its job or case.
        step_id: String,
        /// Optional title used to dynamically register an undeclared step.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// Monotonic start timestamp in milliseconds.
        at_ms: u64,
    },
    /// Marks a step as passed, failed, or skipped.
    StepFinished {
        /// Parent job identifier.
        job_id: String,
        /// Optional parent case identifier when the step belongs to a case.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        case_id: Option<String>,
        /// Step identifier within its job or case.
        step_id: String,
        /// Terminal outcome for the step.
        status: TerminalStatus,
        /// Monotonic completion timestamp in milliseconds.
        at_ms: u64,
        /// Optional typed failure classification when `status` is [`TerminalStatus::Failed`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kind: Option<FailureKind>,
    },
    /// Marks a case as passed, failed, or skipped.
    CaseFinished {
        /// Parent job identifier.
        job_id: String,
        /// Case identifier within the job.
        case_id: String,
        /// Terminal outcome for the case.
        status: TerminalStatus,
        /// Monotonic completion timestamp in milliseconds.
        at_ms: u64,
        /// Optional typed failure classification when `status` is [`TerminalStatus::Failed`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        kind: Option<FailureKind>,
    },
    /// Marks a job as passed, failed, or skipped.
    JobFinished {
        /// Job identifier.
        job_id: String,
        /// Terminal outcome for the job.
        status: TerminalStatus,
        /// Monotonic completion timestamp in milliseconds.
        at_ms: u64,
    },
    /// Advances the live elapsed clock without mutating task statuses.
    Tick {
        /// Monotonic elapsed time in milliseconds.
        elapsed_ms: u64,
    },
    /// Finalizes the entire run. No further events are accepted afterwards.
    RunFinished {
        /// Final run outcome.
        status: RunOutcome,
        /// Final monotonic elapsed time in milliseconds.
        elapsed_ms: u64,
    },
}

impl Event {
    fn max_line_bytes(&self) -> usize {
        match self {
            Self::RunStarted { .. } => MAX_RUN_STARTED_LINE_BYTES,
            _ => MAX_EVENT_LINE_BYTES,
        }
    }
}

/// Encodes a single NDJSON v3 event line and verifies its byte size limit.
pub fn encode_line(event: &Event) -> Result<String, ProtocolError> {
    let encoded = serde_json::to_string(event)?;
    let limit = event.max_line_bytes();
    if encoded.len() > limit {
        return Err(ProtocolError::new(format!(
            "Event exceeds the {} KiB wire limit",
            limit / 1024
        )));
    }
    Ok(encoded)
}

/// Decodes a single NDJSON v3 event line while enforcing line framing and size bounds.
pub fn decode_line(line: &str) -> Result<Event, ProtocolError> {
    let trimmed = line.trim_end_matches(['\r', '\n']);
    if trimmed.is_empty() {
        return Err(ProtocolError::new("NDJSON event line is empty"));
    }
    if trimmed.contains(['\n', '\r']) {
        return Err(ProtocolError::new(
            "Each NDJSON event must occupy a single line",
        ));
    }
    if trimmed.len() > MAX_RUN_STARTED_LINE_BYTES {
        return Err(ProtocolError::new(format!(
            "NDJSON line exceeds the maximum of {} KiB",
            MAX_RUN_STARTED_LINE_BYTES / 1024
        )));
    }
    let event: Event = serde_json::from_str(trimmed)?;
    let limit = event.max_line_bytes();
    if trimmed.len() > limit {
        return Err(ProtocolError::new(format!(
            "Event exceeds the {} KiB wire limit",
            limit / 1024
        )));
    }
    Ok(event)
}
