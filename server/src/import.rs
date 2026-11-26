// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    path::{Path, PathBuf},
};

use kat0_pipeline_protocol::{
    limits::{MAX_SNAPSHOT_BYTES, MAX_V1_BYTES},
    model::{Column, Definition, Job, JobState, Row, Snapshot, Status, Step},
    v2::{self, check_size, validate_label},
};
use serde::{Deserialize, de::IgnoredAny};

use crate::{
    RunResult,
    prepare::reserve_report,
    validate::{Failure, FailureKind, FailureSite},
    wire,
};

const SCHEMA_VERSION: u32 = 1;
const MAX_STEPS: usize = 64;

pub struct ImportedRun {
    pub snapshot: Snapshot,
    pub report_path: PathBuf,
    pub failures: Vec<Failure>,
}

/// Validates a completed external result before reserving a report path. No jobs are run.
pub async fn prepare(input: &Path, output_root: &Path) -> RunResult<ImportedRun> {
    prepare_run(input, output_root).await.map_err(|source| {
        ImportError {
            path: input.to_owned(),
            source,
        }
        .into()
    })
}

#[derive(Debug)]
struct ImportError {
    path: PathBuf,
    source: Box<dyn Error>,
}

impl fmt::Display for ImportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "No se pudo importar {}: {}",
            self.path.display(),
            self.source
        )
    }
}

impl Error for ImportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedRun {
    // The dispatcher checks the version before this strict deserialization.
    #[serde(rename = "schema_version")]
    _schema_version: IgnoredAny,
    job_id: String,
    job_title: String,
    elapsed_ms: u64,
    steps: Vec<RecordedStep>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedStep {
    id: String,
    title: String,
    result: RecordedResult,
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum RecordedResult {
    Passed {
        started_ms: u64,
        duration_ms: u64,
    },
    Failed {
        started_ms: u64,
        duration_ms: u64,
        kind: RecordedFailure,
    },
    Skipped {},
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum RecordedFailure {
    Startup,
    Assertion,
    Execution,
}

async fn prepare_run(input: &Path, output_root: &Path) -> RunResult<ImportedRun> {
    let bytes = wire::read_bytes(input, MAX_SNAPSHOT_BYTES).await?;
    let (snapshot, failures) = match wire::version(&bytes)? {
        SCHEMA_VERSION => {
            check_size(bytes.len() as u64, MAX_V1_BYTES)?;
            decode_v1(serde_json::from_slice(&bytes)?)?
        }
        v2::SCHEMA_VERSION => {
            let snapshot = wire::decode(&bytes)?;
            if snapshot.status == Status::Running {
                return Err("Para generar el informe se requiere una ejecución terminada".into());
            }
            let failures = snapshot_failures(&snapshot);
            (snapshot, failures)
        }
        version => return Err(format!("Versión de resultados no admitida: {version}").into()),
    };
    Ok(ImportedRun {
        snapshot,
        report_path: reserve_report(output_root).await?,
        failures,
    })
}

fn decode_v1(record: RecordedRun) -> RunResult<(Snapshot, Vec<Failure>)> {
    let RecordedRun {
        job_id,
        job_title,
        elapsed_ms,
        steps,
        ..
    } = record;
    if !(1..=MAX_STEPS).contains(&steps.len()) {
        return Err("El resultado requiere de 1 a 64 pasos".into());
    }
    validate_label(&job_id, &job_title)?;

    let mut definition_steps = Vec::with_capacity(steps.len());
    let mut recorded_steps = BTreeMap::new();
    let mut failures = Vec::new();
    let mut skipped = false;
    let mut previous_end = 0;
    for RecordedStep { id, title, result } in steps {
        validate_label(&id, &title)?;
        let row = match result {
            RecordedResult::Passed {
                started_ms,
                duration_ms,
            } => timed_row(
                Status::Passed,
                started_ms,
                duration_ms,
                elapsed_ms,
                &mut previous_end,
            )?,
            RecordedResult::Failed {
                started_ms,
                duration_ms,
                kind,
            } => {
                let row = timed_row(
                    Status::Failed,
                    started_ms,
                    duration_ms,
                    elapsed_ms,
                    &mut previous_end,
                )?;
                let kind = match kind {
                    RecordedFailure::Startup => FailureKind::Startup,
                    RecordedFailure::Assertion => FailureKind::Assertion,
                    RecordedFailure::Execution => FailureKind::Execution,
                };
                failures.push(external_failure(&job_id, Some(&id), kind));
                row
            }
            RecordedResult::Skipped {} => {
                skipped = true;
                Row {
                    status: Status::Skipped,
                    started_ms: None,
                    duration_ms: None,
                }
            }
        };
        recorded_steps.insert(id.clone(), row);
        definition_steps.push(Step { id, title });
    }
    if skipped && failures.is_empty() {
        return Err("Un resultado sin fallos no puede tener pasos omitidos".into());
    }

    let definition = Definition {
        title: "Imported E2E result".into(),
        description: "Recorded results from an external run. Import does not execute tests.".into(),
        columns: vec![Column {
            title: "External run".into(),
            jobs: vec![Job {
                id: job_id.clone(),
                title: job_title,
                is_test: true,
                dependencies: vec![],
                steps: definition_steps,
                cases: vec![],
            }],
        }],
    };
    let status = Status::completed(failures.is_empty());
    let snapshot = Snapshot::from_parts(
        definition,
        status,
        elapsed_ms,
        BTreeMap::from([(
            job_id,
            JobState {
                row: Row {
                    status,
                    started_ms: Some(0),
                    duration_ms: Some(elapsed_ms),
                },
                steps: recorded_steps,
                cases: BTreeMap::new(),
            },
        )]),
    )?;
    Ok((snapshot, failures))
}

fn timed_row(
    status: Status,
    started_ms: u64,
    duration_ms: u64,
    elapsed_ms: u64,
    previous_end: &mut u64,
) -> RunResult<Row> {
    let Some(end) = started_ms
        .checked_add(duration_ms)
        .filter(|&end| started_ms >= *previous_end && end <= elapsed_ms)
    else {
        return Err(
            "Los tiempos de los pasos están solapados, desordenados o fuera de la ejecución".into(),
        );
    };
    *previous_end = end;
    Ok(Row {
        status,
        started_ms: Some(started_ms),
        duration_ms: Some(duration_ms),
    })
}

fn external_failure(job: &str, step: Option<&str>, kind: FailureKind) -> Failure {
    Failure {
        kind,
        site: FailureSite::Task {
            job: job.into(),
            step: step.map(str::to_owned),
        },
        cause: "El productor externo registró este fallo".into(),
    }
}

fn snapshot_failures(snapshot: &Snapshot) -> Vec<Failure> {
    let mut failures = Vec::new();
    for job in snapshot
        .definition()
        .jobs()
        .filter(|job| snapshot.job_status(&job.id) == Some(Status::Failed))
    {
        let before = failures.len();
        for step in &job.steps {
            if snapshot
                .row(&job.id, Some(&step.id))
                .is_some_and(|row| row.status == Status::Failed)
            {
                failures.push(external_failure(
                    &job.id,
                    Some(&step.id),
                    FailureKind::Execution,
                ));
            }
        }
        for case in &job.cases {
            if snapshot
                .case_row(&job.id, &case.id, None)
                .is_some_and(|row| row.status == Status::Failed)
            {
                let before_case = failures.len();
                for step in &case.steps {
                    if snapshot
                        .case_row(&job.id, &case.id, Some(&step.id))
                        .is_some_and(|row| row.status == Status::Failed)
                    {
                        let target = format!("{}/{}", case.id, step.id);
                        failures.push(external_failure(
                            &job.id,
                            Some(&target),
                            FailureKind::Execution,
                        ));
                    }
                }
                if failures.len() == before_case {
                    failures.push(external_failure(
                        &job.id,
                        Some(&case.id),
                        FailureKind::Execution,
                    ));
                }
            }
        }
        if failures.len() == before {
            failures.push(external_failure(&job.id, None, FailureKind::Execution));
        }
    }
    if snapshot.status == Status::Failed && failures.is_empty() {
        failures.push(Failure {
            kind: FailureKind::Execution,
            site: FailureSite::Run,
            cause: "La ejecución externa terminó con fallos".into(),
        });
    }
    failures
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use tokio::fs;

    use super::*;
    use crate::validate;

    fn completed_run() -> Value {
        json!({
            "schema_version": 1,
            "job_id": "smoke-example",
            "job_title": "Example smoke",
            "elapsed_ms": 430,
            "steps": [
                {"id": "start", "title": "Start", "result": {"status": "passed", "started_ms": 0, "duration_ms": 160}},
                {"id": "check", "title": "Check", "result": {"status": "passed", "started_ms": 200, "duration_ms": 150}}
            ]
        })
    }

    async fn write_result(root: &Path, result: &Value) -> PathBuf {
        let input = root.join("result.json");
        fs::write(&input, serde_json::to_vec(result).unwrap())
            .await
            .unwrap();
        input
    }

    #[tokio::test]
    async fn imports_completed_rows_and_original_timings() {
        let dir = tempfile::tempdir().unwrap();
        let input = write_result(dir.path(), &completed_run()).await;
        let root = dir.path().join("reports");
        let first = prepare(&input, &root).await.unwrap();
        let second = prepare(&input, &root).await.unwrap();
        assert_ne!(first.report_path, second.report_path);
        assert!(!first.report_path.exists());
        assert!(first.failures.is_empty());
        let snapshot = first.snapshot;
        assert_eq!(snapshot.status, Status::Passed);
        assert_eq!(snapshot.elapsed_ms, 430);
        assert_eq!(
            snapshot.definition().jobs().next().unwrap().title,
            "Example smoke"
        );
        assert_eq!(
            snapshot.row("smoke-example", None).unwrap().duration_ms,
            Some(430)
        );
        assert_eq!(
            snapshot.row("smoke-example", Some("check")),
            Some(&Row {
                status: Status::Passed,
                started_ms: Some(200),
                duration_ms: Some(150),
            })
        );
    }

    #[tokio::test]
    async fn imports_failed_steps_and_cleanup_evidence_without_executing_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let mut run = completed_run();
        run["steps"][0]["result"] =
            json!({"status": "failed", "kind": "startup", "started_ms": 0, "duration_ms": 160});
        run["steps"][1]["result"] = json!({"status": "skipped"});
        run["steps"].as_array_mut().unwrap().push(json!({
            "id": "release", "title": "Release", "result": {"status": "failed", "kind": "execution", "started_ms": 200, "duration_ms": 150}
        }));
        let input = write_result(dir.path(), &run).await;
        let imported = prepare(&input, &dir.path().join("reports")).await.unwrap();
        assert_eq!(imported.snapshot.status, Status::Failed);
        assert_eq!(
            imported.snapshot.row("smoke-example", Some("check")),
            Some(&Row {
                status: Status::Skipped,
                started_ms: None,
                duration_ms: None,
            })
        );
        fs::write(&imported.report_path, b"<!doctype html>")
            .await
            .unwrap();
        let checked = validate::run(imported.failures, &imported.report_path, Ok(())).await;
        assert_eq!(checked.report_path, Some(imported.report_path.as_path()));
        let error = checked.result.unwrap_err();
        assert_eq!(error.failures().len(), 2);
        for (failure, (step, kind)) in error.failures().iter().zip([
            ("start", FailureKind::Startup),
            ("release", FailureKind::Execution),
        ]) {
            assert_eq!(failure.kind, kind);
            assert_eq!(
                failure.site,
                FailureSite::Task {
                    job: "smoke-example".into(),
                    step: Some(step.into())
                }
            );
        }
    }

    #[tokio::test]
    async fn refuses_to_render_a_live_document_as_a_finished_result() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("reports");
        let input = dir.path().join("snapshot.json");
        fs::write(&input, include_bytes!("../fixtures/web_regression.json"))
            .await
            .unwrap();
        assert!(prepare(&input, &root).await.is_err());
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn rejects_incompatible_or_inconsistent_data_before_creating_reports() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("reports");
        for (pointer, invalid) in [
            ("/schema_version", json!(2)),
            ("/schema_version", json!(99)),
            ("/job_id", json!("invalid/job")),
            ("/steps", json!([])),
            ("/steps/1/id", json!("start")),
            ("/steps/0/result", json!({"status": "running"})),
            ("/steps/0/result", json!({"status": "skipped"})),
            (
                "/steps/0/result",
                json!({"status": "failed", "kind": "unknown", "started_ms": 0, "duration_ms": 160}),
            ),
            (
                "/steps/0/result",
                json!({"status": "passed", "started_ms": 0, "duration_ms": 160, "details": "ineligible data"}),
            ),
            (
                "/steps/0/result",
                json!({"status": "passed", "started_ms": 1, "duration_ms": u64::MAX}),
            ),
            ("/steps/1/result/started_ms", json!(159)),
            ("/steps/1/result/duration_ms", json!(500)),
        ] {
            let mut run = completed_run();
            *run.pointer_mut(pointer).unwrap() = invalid;
            let input = write_result(dir.path(), &run).await;
            assert!(prepare(&input, &root).await.is_err(), "accepted {pointer}");
            assert!(!root.exists(), "created report for invalid {pointer}");
        }
    }

    #[tokio::test]
    async fn rejects_skipped_rows_with_timings_even_in_a_failed_run() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("reports");
        let mut run = completed_run();
        run["steps"][0]["result"] =
            json!({"status": "failed", "kind": "startup", "started_ms": 0, "duration_ms": 160});
        run["steps"][1]["result"] = json!({"status": "skipped", "duration_ms": 3});
        let input = write_result(dir.path(), &run).await;
        assert!(prepare(&input, &root).await.is_err());
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn rejects_missing_incomplete_nonregular_or_oversize_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("reports");
        let input = dir.path().join("result.json");
        assert!(prepare(&input, &root).await.is_err());
        assert!(prepare(dir.path(), &root).await.is_err());
        fs::write(&input, b"{\"schema_version\":1,").await.unwrap();
        assert!(prepare(&input, &root).await.is_err());

        // Oversized v1 payload (> 64 KiB) is rejected even though v2 accepts up to 512 KiB.
        let mut oversized_v1 = serde_json::to_vec(&completed_run()).unwrap();
        oversized_v1.resize(MAX_V1_BYTES + 1, b' ');
        fs::write(&input, &oversized_v1).await.unwrap();
        assert!(prepare(&input, &root).await.is_err());

        // Oversized file beyond v2 limit (> 512 KiB) is rejected.
        fs::write(&input, vec![b' '; MAX_SNAPSHOT_BYTES + 1])
            .await
            .unwrap();
        assert!(prepare(&input, &root).await.is_err());
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn imports_v2_nested_case_failures_with_case_and_step_location() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("reports");
        let doc = json!({
            "schema_version": 2,
            "snapshot": {
                "definition": {
                    "title": "Web regression",
                    "description": "Nested cases",
                    "columns": [{
                        "title": "Regression",
                        "jobs": [{
                            "id": "errors-recovery",
                            "title": "Errors & recovery",
                            "is_test": true,
                            "dependencies": [],
                            "steps": [],
                            "cases": [{
                                "id": "dns-races",
                                "title": "Controlled DNS write races",
                                "steps": [{
                                    "id": "retry-save",
                                    "title": "Retry save explicitly"
                                }]
                            }]
                        }]
                    }]
                },
                "status": "failed",
                "elapsed_ms": 900,
                "jobs": {
                    "errors-recovery": {
                        "row": {"status": "failed", "started_ms": 0, "duration_ms": 900},
                        "steps": {},
                        "cases": {
                            "dns-races": {
                                "row": {"status": "failed", "started_ms": 100, "duration_ms": 700},
                                "steps": {
                                    "retry-save": {"status": "failed", "started_ms": 200, "duration_ms": 400}
                                }
                            }
                        }
                    }
                }
            }
        });
        let input = write_result(dir.path(), &doc).await;
        let imported = prepare(&input, &root).await.unwrap();
        assert_eq!(imported.snapshot.status, Status::Failed);
        assert_eq!(imported.failures.len(), 1);
        assert_eq!(
            imported.failures[0].site,
            FailureSite::Task {
                job: "errors-recovery".into(),
                step: Some("dns-races/retry-save".into()),
            }
        );
    }
}
