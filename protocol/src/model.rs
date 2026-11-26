// SPDX-License-Identifier: AGPL-3.0-only

//! Core domain model for pipeline definitions, execution states, and snapshots.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde::{Deserialize, Serialize};

/// Execution status for a pipeline run, job, case, or step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Status {
    /// Waiting to start.
    Pending,
    /// Currently executing.
    Running,
    /// Completed successfully.
    Passed,
    /// Completed with failure.
    Failed,
    /// Skipped (not executed).
    Skipped,
}

/// Typed classification for a recorded failure without leaking raw diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FailureKind {
    /// Failed during environment setup, driver launch, or startup.
    Startup,
    /// Failed a test expectation or assertion.
    Assertion,
    /// Exceeded an execution or wait deadline.
    Timeout,
    /// Failed due to an operational or runtime error.
    Execution,
}

impl Status {
    /// Returns [`Status::Passed`] when `passed` is `true`, otherwise [`Status::Failed`].
    pub fn completed(passed: bool) -> Self {
        if passed { Self::Passed } else { Self::Failed }
    }

    /// Returns `true` when the status represents a finished run ([`Status::Passed`] or [`Status::Failed`]).
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Passed | Self::Failed)
    }

    /// Transitions [`Status::Pending`] to [`Status::Skipped`], leaving other statuses unchanged.
    pub fn skip_if_pending(&mut self) {
        if *self == Self::Pending {
            *self = Self::Skipped;
        }
    }
}

/// The flow's presentation order and dependencies, supplied by its caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    /// Display title for the pipeline run.
    pub title: String,
    /// Short human-readable description shown below the title.
    pub description: String,
    /// Ordered stage columns rendered left-to-right.
    pub columns: Vec<Column>,
}

/// A stage column grouping one or more [`Job`] nodes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    /// Stage display heading.
    pub title: String,
    /// Jobs belonging to this stage.
    pub jobs: Vec<Job>,
}

/// A single node within a pipeline stage column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    /// Unique ASCII identifier (`[A-Za-z0-9_-]`).
    pub id: String,
    /// Display title rendered on the job card.
    pub title: String,
    /// Whether the job belongs in the Jobs, Failed Jobs and Tests views.
    pub is_test: bool,
    /// Job IDs from earlier stages that this job depends on.
    pub dependencies: Vec<String>,
    /// Optional direct steps belonging to this job.
    #[serde(default)]
    pub steps: Vec<Step>,
    /// Optional nested test cases belonging to this job.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cases: Vec<Case>,
}

/// A nested test case belonging to a [`Job`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    /// Unique ASCII identifier within its parent job.
    pub id: String,
    /// Display title for the test case.
    pub title: String,
    /// Steps recorded inside this test case.
    #[serde(default)]
    pub steps: Vec<Step>,
}

/// An atomic execution step within a [`Job`] or [`Case`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    /// Unique ASCII identifier within its parent job or case.
    pub id: String,
    /// Display title for the step.
    pub title: String,
}

impl Definition {
    /// Iterates over every [`Job`] in column-then-job definition order.
    pub fn jobs(&self) -> impl Iterator<Item = &Job> {
        self.columns.iter().flat_map(|column| &column.jobs)
    }

    /// Number of test items shown in the Tests tab: counts individual cases when present,
    /// or one entry per test job without nested cases.
    pub fn test_case_count(&self) -> usize {
        self.jobs()
            .filter(|job| job.is_test)
            .map(|job| job.cases.len().max(1))
            .sum()
    }

    /// Validates stage structure, unique ASCII identifiers, and acyclic stage-ordered dependencies.
    pub fn validate(&self) -> Result<(), DefinitionError> {
        if self.columns.is_empty() || self.columns.iter().any(|column| column.jobs.is_empty()) {
            return Err(DefinitionError(
                "Pipeline definition requires at least one column with jobs".into(),
            ));
        }
        let mut job_columns = BTreeMap::new();
        for (index, column) in self.columns.iter().enumerate() {
            for job in &column.jobs {
                if !valid_id(&job.id) || job_columns.insert(job.id.as_str(), index).is_some() {
                    return Err(DefinitionError(format!(
                        "Invalid or duplicate job ID: {:?}",
                        job.id
                    )));
                }
                validate_step_ids(&job.id, &job.steps)?;
                let mut case_ids = BTreeSet::new();
                for case in &job.cases {
                    if !valid_id(&case.id) || !case_ids.insert(case.id.as_str()) {
                        return Err(DefinitionError(format!(
                            "Invalid or duplicate case ID in {:?}: {:?}",
                            job.id, case.id
                        )));
                    }
                    validate_step_ids(&case.id, &case.steps)?;
                }
            }
        }
        for (index, column) in self.columns.iter().enumerate() {
            for job in &column.jobs {
                let mut seen = BTreeSet::new();
                for dependency in &job.dependencies {
                    if !seen.insert(dependency)
                        || !job_columns
                            .get(dependency.as_str())
                            .is_some_and(|&source| source < index)
                    {
                        return Err(DefinitionError(format!(
                            "Dependency {:?} of {:?} must refer uniquely to a job in an earlier stage",
                            dependency, job.id
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

fn validate_step_ids(owner: &str, steps: &[Step]) -> Result<(), DefinitionError> {
    let mut step_ids = BTreeSet::new();
    for step in steps {
        if !valid_id(&step.id) || !step_ids.insert(step.id.as_str()) {
            return Err(DefinitionError(format!(
                "Invalid or duplicate step ID in {:?}: {:?}",
                owner, step.id
            )));
        }
    }
    Ok(())
}

pub(crate) fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// Error returned when a [`Definition`] violates structural, ID, or dependency invariants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionError(String);

impl fmt::Display for DefinitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for DefinitionError {}

/// Status and monotonic timing record for a single job, case, or step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Row {
    /// Current status of the item.
    pub status: Status,
    /// Start timestamp in milliseconds since run start, when started.
    pub started_ms: Option<u64>,
    /// Final duration in milliseconds, when finished.
    pub duration_ms: Option<u64>,
}

impl Row {
    /// Constructs a pending row with no timestamps.
    pub fn pending() -> Self {
        Self {
            status: Status::Pending,
            started_ms: None,
            duration_ms: None,
        }
    }

    /// Computes the live duration at `elapsed_ms` while running, or returns `duration_ms` once completed.
    pub fn duration_at(&self, elapsed_ms: u64) -> Option<u64> {
        if self.status == Status::Running {
            self.started_ms
                .map(|start| elapsed_ms.saturating_sub(start))
        } else {
            self.duration_ms
        }
    }
}

/// Recorded execution state for a single [`Case`] and its steps.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseState {
    /// Status and timing for the case itself.
    pub row: Row,
    /// Status and timing rows keyed by step ID.
    #[serde(default)]
    pub steps: BTreeMap<String, Row>,
}

/// Recorded execution state for a single [`Job`], its direct steps, and nested cases.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobState {
    /// Status and timing for the job itself.
    pub row: Row,
    /// Status and timing rows keyed by direct step ID.
    #[serde(default)]
    pub steps: BTreeMap<String, Row>,
    /// Nested case states keyed by case ID.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub cases: BTreeMap<String, CaseState>,
}

/// Complete point-in-time state of a pipeline execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    definition: Definition,
    /// Overall status of the run.
    pub status: Status,
    /// Monotonic elapsed duration of the run in milliseconds.
    pub elapsed_ms: u64,
    jobs: BTreeMap<String, JobState>,
}

impl Snapshot {
    /// Creates an initial running snapshot with all declared jobs, cases, and steps set to [`Status::Pending`].
    pub fn new(definition: Definition) -> Result<Self, DefinitionError> {
        definition.validate()?;
        let jobs = definition
            .jobs()
            .map(|job| {
                let steps = job
                    .steps
                    .iter()
                    .map(|step| (step.id.clone(), Row::pending()))
                    .collect();
                let cases = job
                    .cases
                    .iter()
                    .map(|case| {
                        let case_steps = case
                            .steps
                            .iter()
                            .map(|step| (step.id.clone(), Row::pending()))
                            .collect();
                        (
                            case.id.clone(),
                            CaseState {
                                row: Row::pending(),
                                steps: case_steps,
                            },
                        )
                    })
                    .collect();
                (
                    job.id.clone(),
                    JobState {
                        row: Row::pending(),
                        steps,
                        cases,
                    },
                )
            })
            .collect();
        Ok(Self {
            definition,
            status: Status::Running,
            elapsed_ms: 0,
            jobs,
        })
    }

    /// Returns the immutable pipeline [`Definition`] embedded in this snapshot.
    pub fn definition(&self) -> &Definition {
        &self.definition
    }

    pub(crate) fn definition_mut(&mut self) -> &mut Definition {
        &mut self.definition
    }

    /// Returns the recorded state map keyed by job ID.
    pub fn jobs(&self) -> &BTreeMap<String, JobState> {
        &self.jobs
    }

    /// Returns a mutable reference to the recorded state map keyed by job ID.
    pub fn jobs_mut(&mut self) -> &mut BTreeMap<String, JobState> {
        &mut self.jobs
    }

    /// Looks up a job row (`step = None`) or a direct step row (`step = Some(step_id)`).
    pub fn row(&self, job: &str, step: Option<&str>) -> Option<&Row> {
        let job = self.jobs.get(job)?;
        match step {
            Some(step) => job.steps.get(step),
            None => Some(&job.row),
        }
    }

    /// Looks up a case row (`step = None`) or a step row inside a case (`step = Some(step_id)`).
    pub fn case_row(&self, job: &str, case: &str, step: Option<&str>) -> Option<&Row> {
        let case = self.jobs.get(job)?.cases.get(case)?;
        match step {
            Some(step) => case.steps.get(step),
            None => Some(&case.row),
        }
    }

    /// Returns the current [`Status`] of `job`, if present.
    pub fn job_status(&self, job: &str) -> Option<Status> {
        self.row(job, None).map(|row| row.status)
    }

    /// Returns the most specific running job/case/step label for live status summaries.
    pub fn active_label(&self) -> Option<String> {
        if self.status != Status::Running {
            return None;
        }
        for job in self.definition.jobs() {
            let Some(state) = self.jobs.get(&job.id) else {
                continue;
            };
            if state.row.status != Status::Running {
                continue;
            }
            for case in &job.cases {
                let Some(case_state) = state.cases.get(&case.id) else {
                    continue;
                };
                if case_state.row.status != Status::Running {
                    continue;
                }
                if let Some(step) = case.steps.iter().find(|step| {
                    case_state
                        .steps
                        .get(&step.id)
                        .is_some_and(|row| row.status == Status::Running)
                }) {
                    return Some(format!("{} › {}", case.title, step.title));
                }
                return Some(case.title.clone());
            }
            if let Some(step) = job.steps.iter().find(|step| {
                state
                    .steps
                    .get(&step.id)
                    .is_some_and(|row| row.status == Status::Running)
            }) {
                return Some(format!("{} › {}", job.title, step.title));
            }
            return Some(job.title.clone());
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition() -> Definition {
        let job = |id: &str, dependencies: Vec<String>| Job {
            id: id.into(),
            title: id.into(),
            is_test: true,
            dependencies,
            steps: vec![Step {
                id: "check".into(),
                title: "Check".into(),
            }],
            cases: vec![],
        };
        Definition {
            title: "Sample flow".into(),
            description: "A caller-defined flow".into(),
            columns: vec![
                Column {
                    title: "Build".into(),
                    jobs: vec![job("compile", vec![])],
                },
                Column {
                    title: "Verify".into(),
                    jobs: vec![job("unit-tests", vec!["compile".into()])],
                },
            ],
        }
    }

    #[test]
    fn active_durations_advance_and_completed_durations_do_not() {
        let mut row = Row::pending();
        assert_eq!(row.duration_at(120), None);
        row.started_ms = Some(120);
        row.status = Status::Running;
        assert_eq!(row.duration_at(100), Some(0));
        assert_eq!(row.duration_at(241), Some(121));
        row.status = Status::Passed;
        row.duration_ms = Some(150);
        assert_eq!(row.duration_at(500), Some(150));
    }

    #[test]
    fn caller_defined_snapshot_round_trips_with_stable_ids() {
        let definition = definition();
        let snapshot = Snapshot::new(definition.clone()).expect("valid definition");
        assert_eq!(snapshot.definition(), &definition);
        assert_eq!(snapshot.job_status("unit-tests"), Some(Status::Pending));
        assert!(snapshot.row("unit-tests", Some("check")).is_some());
        assert!(snapshot.row("unit-tests", Some("missing")).is_none());
        let json = serde_json::to_string(&snapshot).expect("encode snapshot");
        assert!(!json.contains("\"cases\""));
        assert_eq!(
            serde_json::from_str::<Snapshot>(&json).expect("decode snapshot"),
            snapshot
        );
    }

    #[test]
    fn nested_cases_initialize_count_and_report_active_label() {
        let definition = Definition {
            title: "Web regression".into(),
            description: "Grouped by aspect and case".into(),
            columns: vec![Column {
                title: "Regression".into(),
                jobs: vec![Job {
                    id: "errors-recovery".into(),
                    title: "Errors & recovery".into(),
                    is_test: true,
                    dependencies: vec![],
                    steps: vec![],
                    cases: vec![
                        Case {
                            id: "menu-races".into(),
                            title: "Controlled Menu Builder writes".into(),
                            steps: vec![Step {
                                id: "retry-create".into(),
                                title: "Menu Create retry".into(),
                            }],
                        },
                        Case {
                            id: "dns-races".into(),
                            title: "DNS reload races".into(),
                            steps: vec![],
                        },
                    ],
                }],
            }],
        };
        assert_eq!(definition.test_case_count(), 2);

        let mut snapshot = Snapshot::new(definition).expect("valid nested definition");
        assert_eq!(snapshot.active_label(), None);

        let job = snapshot.jobs_mut().get_mut("errors-recovery").unwrap();
        job.row = Row {
            status: Status::Running,
            started_ms: Some(0),
            duration_ms: None,
        };
        assert_eq!(
            snapshot.active_label().as_deref(),
            Some("Errors & recovery")
        );

        snapshot
            .jobs_mut()
            .get_mut("errors-recovery")
            .unwrap()
            .cases
            .get_mut("menu-races")
            .unwrap()
            .row = Row {
            status: Status::Running,
            started_ms: Some(10),
            duration_ms: None,
        };
        assert_eq!(
            snapshot.active_label().as_deref(),
            Some("Controlled Menu Builder writes")
        );

        snapshot
            .jobs_mut()
            .get_mut("errors-recovery")
            .unwrap()
            .cases
            .get_mut("menu-races")
            .unwrap()
            .steps
            .insert(
                "retry-create".into(),
                Row {
                    status: Status::Running,
                    started_ms: Some(20),
                    duration_ms: None,
                },
            );
        assert_eq!(
            snapshot.active_label().as_deref(),
            Some("Controlled Menu Builder writes › Menu Create retry")
        );
        assert_eq!(
            snapshot
                .case_row("errors-recovery", "menu-races", Some("retry-create"))
                .map(|row| row.status),
            Some(Status::Running)
        );
    }

    #[test]
    fn rejects_invalid_or_duplicate_ids_and_dependencies() {
        let mut malformed = definition();
        malformed.columns[1].jobs[0].id = "invalid id".into();
        assert!(Snapshot::new(malformed).is_err());

        let mut duplicate = definition();
        duplicate.columns[1].jobs[0].id = "compile".into();
        assert!(Snapshot::new(duplicate).is_err());

        let mut duplicate_step = definition();
        let steps = &mut duplicate_step.columns[1].jobs[0].steps;
        steps.push(steps[0].clone());
        assert!(Snapshot::new(duplicate_step).is_err());

        let mut duplicate_case = definition();
        duplicate_case.columns[1].jobs[0].cases = vec![
            Case {
                id: "case-a".into(),
                title: "Case A".into(),
                steps: vec![],
            },
            Case {
                id: "case-a".into(),
                title: "Case A duplicate".into(),
                steps: vec![],
            },
        ];
        assert!(Snapshot::new(duplicate_case).is_err());

        let mut duplicate_case_step = definition();
        duplicate_case_step.columns[1].jobs[0].cases = vec![Case {
            id: "case-a".into(),
            title: "Case A".into(),
            steps: vec![
                Step {
                    id: "s1".into(),
                    title: "Step 1".into(),
                },
                Step {
                    id: "s1".into(),
                    title: "Step 1 dup".into(),
                },
            ],
        }];
        assert!(Snapshot::new(duplicate_case_step).is_err());

        let mut missing = definition();
        missing.columns[1].jobs[0].dependencies = vec!["missing".into()];
        assert!(Snapshot::new(missing).is_err());

        let mut backward = definition();
        backward.columns[0].jobs[0].dependencies = vec!["unit-tests".into()];
        assert!(Snapshot::new(backward).is_err());
    }
}
