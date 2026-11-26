// SPDX-License-Identifier: AGPL-3.0-only

//! Version 2 complete-snapshot JSON wire format and validation.

use std::{collections::BTreeMap, error::Error, fmt};

use serde::{Deserialize, Serialize};

use crate::{
    limits::{MAX_DESCRIPTION_BYTES, MAX_ID_BYTES, MAX_SNAPSHOT_BYTES, MAX_TITLE_BYTES},
    model::{DefinitionError, Row, Snapshot, Status, Step},
};

/// Schema version number for complete v2 snapshot JSON documents.
pub const SCHEMA_VERSION: u32 = 2;

/// Error returned when encoding, decoding, or validating a v2 snapshot document fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError(String);

impl WireError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for WireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for WireError {}

impl From<DefinitionError> for WireError {
    fn from(error: DefinitionError) -> Self {
        Self(error.to_string())
    }
}

impl From<serde_json::Error> for WireError {
    fn from(error: serde_json::Error) -> Self {
        Self(error.to_string())
    }
}

#[derive(Deserialize)]
struct Version {
    schema_version: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedDocument {
    schema_version: u32,
    snapshot: Snapshot,
}

#[derive(Serialize)]
struct BorrowedDocument<'a> {
    schema_version: u32,
    snapshot: &'a Snapshot,
}

/// Extracts `schema_version` from a raw JSON document before full deserialization.
pub fn schema_version(bytes: &[u8]) -> Result<u32, WireError> {
    Ok(serde_json::from_slice::<Version>(bytes)?.schema_version)
}

/// Validates and serializes a [`Snapshot`] as a versioned v2 JSON document.
pub fn encode(snapshot: &Snapshot) -> Result<Vec<u8>, WireError> {
    validate_snapshot(snapshot)?;
    let bytes = serde_json::to_vec(&BorrowedDocument {
        schema_version: SCHEMA_VERSION,
        snapshot,
    })?;
    check_size(bytes.len() as u64, MAX_SNAPSHOT_BYTES)?;
    Ok(bytes)
}

/// Decodes and validates a versioned v2 snapshot JSON document.
pub fn decode(bytes: &[u8]) -> Result<Snapshot, WireError> {
    check_size(bytes.len() as u64, MAX_SNAPSHOT_BYTES)?;
    let OwnedDocument {
        schema_version,
        snapshot,
    } = serde_json::from_slice(bytes)?;
    if schema_version != SCHEMA_VERSION {
        return Err(WireError::new(format!(
            "Expected snapshot schema v{SCHEMA_VERSION}, got v{schema_version}"
        )));
    }
    validate_snapshot(&snapshot)?;
    Ok(snapshot)
}

/// Verifies that `len` does not exceed `limit` bytes.
pub fn check_size(len: u64, limit: usize) -> Result<(), WireError> {
    if len > limit as u64 {
        return Err(WireError::new(format!(
            "Payload exceeds {} KiB limit",
            limit / 1024
        )));
    }
    Ok(())
}

/// Validates structural consistency, text bounds, statuses, and monotonic spans of a [`Snapshot`].
pub fn validate_snapshot(snapshot: &Snapshot) -> Result<(), WireError> {
    let definition = snapshot.definition();
    definition.validate()?;
    validate_title(&definition.title)?;
    if definition.description.len() > MAX_DESCRIPTION_BYTES
        || definition.description.chars().any(char::is_control)
    {
        return Err(WireError::new(format!(
            "Description must be at most {MAX_DESCRIPTION_BYTES} bytes without control characters"
        )));
    }
    if !matches!(
        snapshot.status,
        Status::Running | Status::Passed | Status::Failed
    ) {
        return Err(WireError::new("Invalid overall snapshot status"));
    }
    if snapshot.jobs().len() != definition.jobs().count() {
        return Err(WireError::new("Snapshot jobs do not match definition"));
    }
    for column in &definition.columns {
        validate_title(&column.title)?;
        for job in &column.jobs {
            validate_label(&job.id, &job.title)?;
            let state = snapshot
                .jobs()
                .get(&job.id)
                .ok_or_else(|| WireError::new("Missing defined job state"))?;
            if state.steps.len() != job.steps.len() {
                return Err(WireError::new("Snapshot steps do not match definition"));
            }
            if state.cases.len() != job.cases.len() {
                return Err(WireError::new("Snapshot cases do not match definition"));
            }
            let status = state.row.status;
            if (snapshot.status != Status::Running && active(status))
                || (snapshot.status == Status::Passed && status == Status::Failed)
            {
                return Err(WireError::new(
                    "Job status is incompatible with overall run status",
                ));
            }
            let job_span = span(&state.row, snapshot.elapsed_ms)?;
            validate_steps(
                &job.steps,
                &state.steps,
                status,
                job_span,
                snapshot.elapsed_ms,
            )?;
            for case in &job.cases {
                validate_label(&case.id, &case.title)?;
                let case_state = state
                    .cases
                    .get(&case.id)
                    .ok_or_else(|| WireError::new("Missing defined case state"))?;
                if case_state.steps.len() != case.steps.len() {
                    return Err(WireError::new("Case steps do not match definition"));
                }
                let case_status = case_state.row.status;
                if incompatible_child_status(status, case_status) {
                    return Err(WireError::new(
                        "Case status is incompatible with its parent job",
                    ));
                }
                let case_span = span(&case_state.row, snapshot.elapsed_ms)?;
                if let Some((start, end)) = case_span
                    && !job_span.is_some_and(|(from, to)| from <= start && end <= to)
                {
                    return Err(WireError::new("Case timing falls outside its parent job"));
                }
                validate_steps(
                    &case.steps,
                    &case_state.steps,
                    case_status,
                    case_span,
                    snapshot.elapsed_ms,
                )?;
            }
        }
    }
    Ok(())
}

fn validate_steps(
    defined: &[Step],
    recorded: &BTreeMap<String, Row>,
    parent_status: Status,
    parent_span: Option<(u64, u64)>,
    elapsed_ms: u64,
) -> Result<(), WireError> {
    let mut previous_end = 0;
    for step in defined {
        validate_label(&step.id, &step.title)?;
        let row = recorded
            .get(&step.id)
            .ok_or_else(|| WireError::new("Missing defined step state"))?;
        if incompatible_child_status(parent_status, row.status) {
            return Err(WireError::new(
                "Step status is incompatible with its container",
            ));
        }
        if let Some((start, end)) = span(row, elapsed_ms)? {
            if !parent_span.is_some_and(|(from, to)| from <= start && end <= to)
                || start < previous_end
            {
                return Err(WireError::new(
                    "Step timings overlap or fall outside their container",
                ));
            }
            previous_end = end;
        }
    }
    Ok(())
}

fn incompatible_child_status(parent: Status, child: Status) -> bool {
    (matches!(parent, Status::Pending | Status::Skipped) && child != parent)
        || (!active(parent) && active(child))
        || (parent == Status::Passed && child == Status::Failed)
}

fn active(status: Status) -> bool {
    matches!(status, Status::Pending | Status::Running)
}

fn span(row: &Row, elapsed: u64) -> Result<Option<(u64, u64)>, WireError> {
    match (row.status, row.started_ms, row.duration_ms) {
        (Status::Pending | Status::Skipped, None, None) => Ok(None),
        (Status::Running, Some(start), None) if start <= elapsed => Ok(Some((start, elapsed))),
        (Status::Passed | Status::Failed, Some(start), Some(duration)) => start
            .checked_add(duration)
            .filter(|&end| end <= elapsed)
            .map(|end| Some((start, end)))
            .ok_or_else(|| WireError::new("Timing exceeds total run duration")),
        _ => Err(WireError::new(
            "Row timings are incompatible with its status",
        )),
    }
}

/// Validates an identifier length and its associated display title.
pub fn validate_label(id: &str, text: &str) -> Result<(), WireError> {
    if id.len() > MAX_ID_BYTES {
        return Err(WireError::new(format!(
            "Identifiers must be at most {MAX_ID_BYTES} bytes"
        )));
    }
    validate_title(text)
}

/// Validates that a display title is non-empty, within [`MAX_TITLE_BYTES`], and free of control characters.
pub fn validate_title(text: &str) -> Result<(), WireError> {
    if text.trim().is_empty() || text.len() > MAX_TITLE_BYTES || text.chars().any(char::is_control)
    {
        return Err(WireError::new(format!(
            "Titles must be non-empty, free of control characters, and at most {MAX_TITLE_BYTES} bytes"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::model::{Case, Column, Definition, Job, Step};

    fn sample_document() -> Value {
        json!({
            "schema_version": 2,
            "snapshot": {
                "definition": {
                    "title": "Web regression",
                    "description": "Simulated snapshot.",
                    "columns": [
                        {
                            "title": "Prepare",
                            "jobs": [
                                {
                                    "id": "prepare-env",
                                    "title": "Test env",
                                    "is_test": false,
                                    "dependencies": [],
                                    "steps": [
                                        {"id": "boot", "title": "Boot"}
                                    ]
                                }
                            ]
                        },
                        {
                            "title": "Regression",
                            "jobs": [
                                {
                                    "id": "change-role",
                                    "title": "Edit role",
                                    "is_test": true,
                                    "dependencies": ["prepare-env"],
                                    "steps": [
                                        {"id": "save", "title": "Save role"}
                                    ]
                                }
                            ]
                        }
                    ]
                },
                "status": "running",
                "elapsed_ms": 48_300,
                "jobs": {
                    "prepare-env": {
                        "row": {"status": "passed", "started_ms": 0, "duration_ms": 10_000},
                        "steps": {
                            "boot": {"status": "passed", "started_ms": 0, "duration_ms": 10_000}
                        }
                    },
                    "change-role": {
                        "row": {"status": "failed", "started_ms": 10_000, "duration_ms": 21_300},
                        "steps": {
                            "save": {"status": "failed", "started_ms": 10_000, "duration_ms": 21_300}
                        }
                    }
                }
            }
        })
    }

    #[test]
    fn encodes_and_decodes_valid_v2_snapshots_with_nested_cases_and_long_titles() {
        let long_title = "a".repeat(200);
        let definition = Definition {
            title: "Sample".into(),
            description: "Round trip".into(),
            columns: vec![Column {
                title: "Regression".into(),
                jobs: vec![Job {
                    id: "errors-recovery".into(),
                    title: "Errors & recovery".into(),
                    is_test: true,
                    dependencies: vec![],
                    steps: vec![],
                    cases: vec![Case {
                        id: "dns-races".into(),
                        title: long_title,
                        steps: vec![Step {
                            id: "retry".into(),
                            title: "Retry write explicitly".into(),
                        }],
                    }],
                }],
            }],
        };
        let mut snapshot = Snapshot::new(definition).unwrap();
        snapshot.elapsed_ms = 2_000;
        snapshot.status = Status::Failed;
        let job = snapshot.jobs_mut().get_mut("errors-recovery").unwrap();
        job.row = Row {
            status: Status::Failed,
            started_ms: Some(100),
            duration_ms: Some(1_500),
        };
        let case = job.cases.get_mut("dns-races").unwrap();
        case.row = Row {
            status: Status::Failed,
            started_ms: Some(150),
            duration_ms: Some(1_200),
        };
        case.steps.insert(
            "retry".into(),
            Row {
                status: Status::Failed,
                started_ms: Some(200),
                duration_ms: Some(900),
            },
        );

        let encoded = encode(&snapshot).unwrap();
        assert_eq!(schema_version(&encoded).unwrap(), SCHEMA_VERSION);
        assert_eq!(decode(&encoded).unwrap(), snapshot);

        // Step timing outside its case span must be rejected.
        let mut invalid_step = snapshot.clone();
        invalid_step
            .jobs_mut()
            .get_mut("errors-recovery")
            .unwrap()
            .cases
            .get_mut("dns-races")
            .unwrap()
            .steps
            .get_mut("retry")
            .unwrap()
            .started_ms = Some(100);
        assert!(encode(&invalid_step).is_err());

        // Case timing outside its job span must be rejected.
        let mut invalid_case = snapshot;
        invalid_case
            .jobs_mut()
            .get_mut("errors-recovery")
            .unwrap()
            .cases
            .get_mut("dns-races")
            .unwrap()
            .row
            .started_ms = Some(50);
        assert!(encode(&invalid_case).is_err());
    }

    #[test]
    fn rejects_malformed_or_oversized_documents() {
        let base = sample_document();
        for (path, value) in [
            ("/schema_version", json!(99)),
            ("/snapshot/status", json!("passed")),
            ("/snapshot/elapsed_ms", json!(42)),
            ("/snapshot/jobs/prepare-env/steps", json!({})),
            (
                "/snapshot/definition/columns/1/jobs/0/dependencies",
                json!(["missing"]),
            ),
            (
                "/snapshot/jobs/surplus",
                json!({"row": {"status": "skipped"}, "steps": {}}),
            ),
            (
                "/snapshot/jobs/change-role/row",
                json!({"status": "pending"}),
            ),
        ] {
            let mut sample = base.clone();
            let (container, key) = path.rsplit_once('/').unwrap();
            sample.pointer_mut(container).unwrap()[key] = value;
            assert!(
                decode(&serde_json::to_vec(&sample).unwrap()).is_err(),
                "{path}"
            );
        }
        assert!(validate_title(&"x".repeat(MAX_TITLE_BYTES + 1)).is_err());
        assert!(decode(&vec![b' '; MAX_SNAPSHOT_BYTES + 1]).is_err());
    }
}
