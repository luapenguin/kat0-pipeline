// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    fmt,
};

use crate::{
    limits::{
        MAX_CASES_PER_JOB, MAX_ID_BYTES, MAX_RECORDED_FAILURES, MAX_STEPS_PER_CONTAINER,
        MAX_TOTAL_CASES, MAX_TOTAL_STEPS,
    },
    model::{Case, CaseState, DefinitionError, FailureKind, Row, Snapshot, Status, Step, valid_id},
    v2::{WireError, validate_label, validate_snapshot},
};

use super::event::{Event, RunOutcome, SCHEMA_VERSION, TerminalStatus};

/// Error returned when decoding or applying an invalid NDJSON v3 event transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError(String);

impl ProtocolError {
    pub(super) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ProtocolError {}

impl From<DefinitionError> for ProtocolError {
    fn from(error: DefinitionError) -> Self {
        Self(error.to_string())
    }
}

impl From<WireError> for ProtocolError {
    fn from(error: WireError) -> Self {
        Self(error.to_string())
    }
}

impl From<serde_json::Error> for ProtocolError {
    fn from(error: serde_json::Error) -> Self {
        Self(error.to_string())
    }
}

/// Typed failure entry recorded by [`RunState`] as failed transitions arrive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedFailure {
    /// Failing job identifier.
    pub job_id: String,
    /// Optional failing case identifier.
    pub case_id: Option<String>,
    /// Optional failing step identifier.
    pub step_id: Option<String>,
    /// Typed failure category.
    pub kind: FailureKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct StepScope {
    job_id: String,
    case_id: Option<String>,
}

/// Pure in-memory reducer that validates and applies NDJSON v3 events into a [`Snapshot`].
pub struct RunState {
    snapshot: Snapshot,
    workers: u16,
    failures: Vec<RecordedFailure>,
    failed_jobs: HashSet<String>,
    failed_cases: HashSet<(String, String)>,
    job_locations: HashMap<String, (usize, usize)>,
    case_locations: HashMap<(String, String), usize>,
    step_locations: HashMap<(StepScope, String), usize>,
    running_steps: HashSet<StepScope>,
    last_step_index: HashMap<StepScope, usize>,
    total_cases: usize,
    total_steps: usize,
}

impl RunState {
    /// Starts a new state machine from the required initial [`Event::RunStarted`] event.
    pub fn start(event: Event) -> Result<Self, ProtocolError> {
        let Event::RunStarted {
            schema_version,
            definition,
            workers,
        } = event
        else {
            return Err(ProtocolError::new(
                "NDJSON v3 stream must start with a run_started event",
            ));
        };
        if schema_version != SCHEMA_VERSION {
            return Err(ProtocolError::new(format!(
                "Expected NDJSON protocol v{SCHEMA_VERSION}, got v{schema_version}"
            )));
        }
        let workers = match workers {
            None => 1,
            Some(0) => {
                return Err(ProtocolError::new("Worker count must be at least 1"));
            }
            Some(count) => count,
        };
        let snapshot = Snapshot::new(definition)?;
        validate_snapshot(&snapshot)?;

        let mut job_locations = HashMap::new();
        let mut case_locations = HashMap::new();
        let mut step_locations = HashMap::new();
        let mut total_cases = 0usize;
        let mut total_steps = 0usize;
        for (col_idx, column) in snapshot.definition().columns.iter().enumerate() {
            for (job_idx, job) in column.jobs.iter().enumerate() {
                job_locations.insert(job.id.clone(), (col_idx, job_idx));
                let job_scope = StepScope {
                    job_id: job.id.clone(),
                    case_id: None,
                };
                for (step_idx, step) in job.steps.iter().enumerate() {
                    step_locations.insert((job_scope.clone(), step.id.clone()), step_idx);
                }
                total_steps += job.steps.len();
                total_cases += job.cases.len();
                for (case_idx, case) in job.cases.iter().enumerate() {
                    case_locations.insert((job.id.clone(), case.id.clone()), case_idx);
                    let case_scope = StepScope {
                        job_id: job.id.clone(),
                        case_id: Some(case.id.clone()),
                    };
                    for (step_idx, step) in case.steps.iter().enumerate() {
                        step_locations.insert((case_scope.clone(), step.id.clone()), step_idx);
                    }
                    total_steps += case.steps.len();
                }
            }
        }

        Ok(Self {
            snapshot,
            workers,
            failures: Vec::new(),
            failed_jobs: HashSet::new(),
            failed_cases: HashSet::new(),
            job_locations,
            case_locations,
            step_locations,
            running_steps: HashSet::new(),
            last_step_index: HashMap::new(),
            total_cases,
            total_steps,
        })
    }

    /// Returns a reference to the current [`Snapshot`].
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Consumes this state machine and returns the final [`Snapshot`].
    pub fn into_snapshot(self) -> Snapshot {
        self.snapshot
    }

    /// Returns the concurrency hint reported by `run_started` (`>= 1`).
    pub fn workers(&self) -> u16 {
        self.workers
    }

    /// Returns all typed failures recorded so far in arrival order.
    pub fn failures(&self) -> &[RecordedFailure] {
        &self.failures
    }

    /// Returns `true` once the run has reached a terminal status.
    pub fn is_finished(&self) -> bool {
        self.snapshot.status.is_terminal()
    }

    /// Validates and applies a single NDJSON v3 [`Event`], returning the updated [`Snapshot`].
    pub fn apply(&mut self, event: Event) -> Result<&Snapshot, ProtocolError> {
        if self.is_finished() {
            return Err(ProtocolError::new(
                "Events are not accepted after the run has finished",
            ));
        }
        match event {
            Event::RunStarted { .. } => {
                return Err(ProtocolError::new(
                    "run_started may only appear once at the beginning of the stream",
                ));
            }
            Event::JobStarted { job_id, at_ms } => self.start_job(&job_id, at_ms)?,
            Event::CaseStarted {
                job_id,
                case_id,
                title,
                at_ms,
            } => self.start_case(&job_id, &case_id, title, at_ms)?,
            Event::StepStarted {
                job_id,
                case_id,
                step_id,
                title,
                at_ms,
            } => self.start_step(&job_id, case_id.as_deref(), &step_id, title, at_ms)?,
            Event::StepFinished {
                job_id,
                case_id,
                step_id,
                status,
                at_ms,
                kind,
            } => self.finish_step(&job_id, case_id.as_deref(), &step_id, status, at_ms, kind)?,
            Event::CaseFinished {
                job_id,
                case_id,
                status,
                at_ms,
                kind,
            } => self.finish_case(&job_id, &case_id, status, at_ms, kind)?,
            Event::JobFinished {
                job_id,
                status,
                at_ms,
            } => self.finish_job(&job_id, status, at_ms)?,
            Event::Tick { elapsed_ms } => {
                self.advance_clock(elapsed_ms)?;
            }
            Event::RunFinished { status, elapsed_ms } => self.finish_run(status, elapsed_ms)?,
        }
        Ok(&self.snapshot)
    }

    /// Marks any remaining active rows as failed and pending rows as skipped when the
    /// producer exits without sending `run_finished`. Idempotent once terminal.
    pub fn finalize_aborted(&mut self, elapsed_ms: u64) -> &Snapshot {
        if self.is_finished() {
            return &self.snapshot;
        }
        let elapsed = elapsed_ms.max(self.snapshot.elapsed_ms);
        self.snapshot.elapsed_ms = elapsed;
        self.snapshot.status = Status::Failed;
        self.running_steps.clear();

        let job_ids: Vec<String> = self
            .snapshot
            .definition()
            .jobs()
            .map(|job| job.id.clone())
            .collect();
        let mut aborted_failures = Vec::new();
        for job_id in job_ids {
            let Some(job) = self.snapshot.jobs_mut().get_mut(&job_id) else {
                continue;
            };
            let mut recorded_inside_job = false;
            for (step_id, row) in &mut job.steps {
                if abort_row(row, elapsed) {
                    recorded_inside_job = true;
                    aborted_failures.push(RecordedFailure {
                        job_id: job_id.clone(),
                        case_id: None,
                        step_id: Some(step_id.clone()),
                        kind: FailureKind::Execution,
                    });
                }
            }
            for (case_id, case_state) in &mut job.cases {
                let mut recorded_inside_case = false;
                for (step_id, row) in &mut case_state.steps {
                    if abort_row(row, elapsed) {
                        recorded_inside_case = true;
                        recorded_inside_job = true;
                        aborted_failures.push(RecordedFailure {
                            job_id: job_id.clone(),
                            case_id: Some(case_id.clone()),
                            step_id: Some(step_id.clone()),
                            kind: FailureKind::Execution,
                        });
                    }
                }
                if abort_row(&mut case_state.row, elapsed) && !recorded_inside_case {
                    recorded_inside_job = true;
                    aborted_failures.push(RecordedFailure {
                        job_id: job_id.clone(),
                        case_id: Some(case_id.clone()),
                        step_id: None,
                        kind: FailureKind::Execution,
                    });
                }
            }
            if abort_row(&mut job.row, elapsed) && !recorded_inside_job {
                aborted_failures.push(RecordedFailure {
                    job_id,
                    case_id: None,
                    step_id: None,
                    kind: FailureKind::Execution,
                });
            }
        }
        for failure in aborted_failures {
            self.record_failure(failure);
        }
        &self.snapshot
    }

    fn record_failure(&mut self, failure: RecordedFailure) {
        self.failed_jobs.insert(failure.job_id.clone());
        if let Some(case_id) = &failure.case_id {
            self.failed_cases
                .insert((failure.job_id.clone(), case_id.clone()));
        }
        if self.failures.len() < MAX_RECORDED_FAILURES {
            self.failures.push(failure);
        }
    }

    fn advance_clock(&mut self, next_ms: u64) -> Result<(), ProtocolError> {
        if next_ms < self.snapshot.elapsed_ms {
            return Err(ProtocolError::new(format!(
                "Clock moved backwards from {} ms to {next_ms} ms",
                self.snapshot.elapsed_ms
            )));
        }
        self.snapshot.elapsed_ms = next_ms;
        Ok(())
    }

    fn start_job(&mut self, job_id: &str, at_ms: u64) -> Result<(), ProtocolError> {
        self.ensure_clock_non_decreasing(at_ms)?;
        let &(col_idx, job_idx) = self
            .job_locations
            .get(job_id)
            .ok_or_else(|| ProtocolError::new(format!("Unknown job: {job_id:?}")))?;
        for dependency in &self.snapshot.definition().columns[col_idx].jobs[job_idx].dependencies {
            let status = self
                .snapshot
                .job_status(dependency)
                .ok_or_else(|| ProtocolError::new(format!("Unknown dependency: {dependency:?}")))?;
            if matches!(status, Status::Pending | Status::Running) {
                return Err(ProtocolError::new(format!(
                    "Job {job_id:?} cannot start before dependency {dependency:?} finishes"
                )));
            }
        }
        let job = self
            .snapshot
            .jobs_mut()
            .get_mut(job_id)
            .ok_or_else(|| ProtocolError::new(format!("Unknown job: {job_id:?}")))?;
        if job.row.status != Status::Pending {
            return Err(ProtocolError::new(format!(
                "Job {job_id:?} has already been started or finished"
            )));
        }
        job.row = Row {
            status: Status::Running,
            started_ms: Some(at_ms),
            duration_ms: None,
        };
        self.snapshot.elapsed_ms = at_ms;
        Ok(())
    }

    fn start_case(
        &mut self,
        job_id: &str,
        case_id: &str,
        title: Option<String>,
        at_ms: u64,
    ) -> Result<(), ProtocolError> {
        self.ensure_clock_non_decreasing(at_ms)?;
        validate_identifier(case_id, "case")?;
        let &(col_idx, job_idx) = self
            .job_locations
            .get(job_id)
            .ok_or_else(|| ProtocolError::new(format!("Unknown job: {job_id:?}")))?;
        let job_state = self
            .snapshot
            .jobs()
            .get(job_id)
            .ok_or_else(|| ProtocolError::new(format!("Unknown job: {job_id:?}")))?;
        if job_state.row.status != Status::Running {
            return Err(ProtocolError::new(format!(
                "Cannot start case {case_id:?}: job {job_id:?} is not running"
            )));
        }

        let case_key = (job_id.to_owned(), case_id.to_owned());
        if let Some(&case_idx) = self.case_locations.get(&case_key) {
            let existing_case = job_state.cases.get(case_id).ok_or_else(|| {
                ProtocolError::new(format!("Unknown case {case_id:?} in {job_id:?}"))
            })?;
            if existing_case.row.status != Status::Pending {
                return Err(ProtocolError::new(format!(
                    "Case {case_id:?} in {job_id:?} has already been started or finished"
                )));
            }
            if let Some(provided_title) = title.as_deref() {
                let expected_title = self.snapshot.definition().columns[col_idx].jobs[job_idx]
                    .cases[case_idx]
                    .title
                    .as_str();
                if expected_title != provided_title {
                    return Err(ProtocolError::new(format!(
                        "Title for case {case_id:?} does not match definition"
                    )));
                }
            }
            let case_state = self
                .snapshot
                .jobs_mut()
                .get_mut(job_id)
                .and_then(|job| job.cases.get_mut(case_id))
                .ok_or_else(|| {
                    ProtocolError::new(format!("Unknown case {case_id:?} in {job_id:?}"))
                })?;
            case_state.row = Row {
                status: Status::Running,
                started_ms: Some(at_ms),
                duration_ms: None,
            };
        } else {
            let title = title.ok_or_else(|| {
                ProtocolError::new(format!(
                    "Dynamic case {case_id:?} in {job_id:?} requires a title"
                ))
            })?;
            validate_label(case_id, &title)?;
            let job_case_count = self.snapshot.definition().columns[col_idx].jobs[job_idx]
                .cases
                .len();
            if job_case_count >= MAX_CASES_PER_JOB {
                return Err(ProtocolError::new(format!(
                    "Job {job_id:?} exceeds maximum of {MAX_CASES_PER_JOB} cases"
                )));
            }
            if self.total_cases >= MAX_TOTAL_CASES {
                return Err(ProtocolError::new(format!(
                    "Pipeline run exceeds maximum of {MAX_TOTAL_CASES} total cases"
                )));
            }
            let def_job = &mut self.snapshot.definition_mut().columns[col_idx].jobs[job_idx];
            let new_case_idx = def_job.cases.len();
            def_job.cases.push(Case {
                id: case_id.to_owned(),
                title,
                steps: Vec::new(),
            });
            let job_state = self
                .snapshot
                .jobs_mut()
                .get_mut(job_id)
                .ok_or_else(|| ProtocolError::new(format!("Unknown job: {job_id:?}")))?;
            job_state.cases.insert(
                case_id.to_owned(),
                CaseState {
                    row: Row {
                        status: Status::Running,
                        started_ms: Some(at_ms),
                        duration_ms: None,
                    },
                    steps: BTreeMap::new(),
                },
            );
            self.case_locations.insert(case_key, new_case_idx);
            self.total_cases += 1;
        }
        self.snapshot.elapsed_ms = at_ms;
        Ok(())
    }

    fn start_step(
        &mut self,
        job_id: &str,
        case_id: Option<&str>,
        step_id: &str,
        title: Option<String>,
        at_ms: u64,
    ) -> Result<(), ProtocolError> {
        self.ensure_clock_non_decreasing(at_ms)?;
        validate_identifier(step_id, "step")?;
        self.ensure_container_running(job_id, case_id)?;

        let scope = StepScope {
            job_id: job_id.to_owned(),
            case_id: case_id.map(str::to_owned),
        };
        // Steps inside the same job or case must not overlap.
        if self.running_steps.contains(&scope) {
            return Err(ProtocolError::new(format!(
                "Another step is already running inside {job_id:?}"
            )));
        }

        let existing_index = self.find_defined_step_index(&scope, step_id);

        if let Some((index, expected_title)) = existing_index {
            if let Some(provided) = title.as_deref()
                && provided != expected_title
            {
                return Err(ProtocolError::new(format!(
                    "Title for step {step_id:?} does not match definition"
                )));
            }
            if self
                .last_step_index
                .get(&scope)
                .is_some_and(|&previous| index <= previous)
            {
                return Err(ProtocolError::new(format!(
                    "Step {step_id:?} is out of defined order"
                )));
            }
            let row = self.step_row_mut(job_id, case_id, step_id)?;
            if row.status != Status::Pending {
                return Err(ProtocolError::new(format!(
                    "Step {step_id:?} has already been started or finished"
                )));
            }
            *row = Row {
                status: Status::Running,
                started_ms: Some(at_ms),
                duration_ms: None,
            };
            self.last_step_index.insert(scope.clone(), index);
            self.running_steps.insert(scope);
        } else {
            let title = title.ok_or_else(|| {
                ProtocolError::new(format!("Dynamic step {step_id:?} requires a title"))
            })?;
            validate_label(step_id, &title)?;
            let new_index = self.append_dynamic_step(&scope, step_id, title)?;
            let steps_map = self.steps_map_mut(job_id, case_id)?;
            steps_map.insert(
                step_id.to_owned(),
                Row {
                    status: Status::Running,
                    started_ms: Some(at_ms),
                    duration_ms: None,
                },
            );
            self.step_locations
                .insert((scope.clone(), step_id.to_owned()), new_index);
            self.last_step_index.insert(scope.clone(), new_index);
            self.running_steps.insert(scope);
            self.total_steps += 1;
        }

        self.snapshot.elapsed_ms = at_ms;
        Ok(())
    }

    fn finish_step(
        &mut self,
        job_id: &str,
        case_id: Option<&str>,
        step_id: &str,
        status: TerminalStatus,
        at_ms: u64,
        kind: Option<FailureKind>,
    ) -> Result<(), ProtocolError> {
        self.ensure_clock_non_decreasing(at_ms)?;
        validate_failure_kind(status, kind)?;
        self.ensure_container_running(job_id, case_id)?;

        let row = self.step_row_mut(job_id, case_id, step_id)?;
        complete_row(row, status, at_ms, step_id)?;
        if status != TerminalStatus::Skipped {
            self.running_steps.remove(&StepScope {
                job_id: job_id.to_owned(),
                case_id: case_id.map(str::to_owned),
            });
        }
        if status == TerminalStatus::Failed {
            self.record_failure(RecordedFailure {
                job_id: job_id.to_owned(),
                case_id: case_id.map(str::to_owned),
                step_id: Some(step_id.to_owned()),
                kind: kind.unwrap_or(FailureKind::Execution),
            });
        }
        self.snapshot.elapsed_ms = at_ms;
        Ok(())
    }

    fn finish_case(
        &mut self,
        job_id: &str,
        case_id: &str,
        status: TerminalStatus,
        at_ms: u64,
        kind: Option<FailureKind>,
    ) -> Result<(), ProtocolError> {
        self.ensure_clock_non_decreasing(at_ms)?;
        validate_failure_kind(status, kind)?;
        let case_scope = StepScope {
            job_id: job_id.to_owned(),
            case_id: Some(case_id.to_owned()),
        };
        if self.running_steps.contains(&case_scope) {
            return Err(ProtocolError::new(format!(
                "Case {case_id:?} still has a running step"
            )));
        }
        let job = self
            .snapshot
            .jobs_mut()
            .get_mut(job_id)
            .ok_or_else(|| ProtocolError::new(format!("Unknown job: {job_id:?}")))?;
        if job.row.status != Status::Running {
            return Err(ProtocolError::new(format!(
                "Cannot finish case {case_id:?}: job {job_id:?} is not running"
            )));
        }
        let case = job
            .cases
            .get_mut(case_id)
            .ok_or_else(|| ProtocolError::new(format!("Unknown case {case_id:?} in {job_id:?}")))?;
        if status == TerminalStatus::Passed
            && case.steps.values().any(|row| row.status == Status::Failed)
        {
            return Err(ProtocolError::new(format!(
                "Case {case_id:?} cannot pass when it has failed steps"
            )));
        }
        complete_row(&mut case.row, status, at_ms, case_id)?;
        for step_row in case.steps.values_mut() {
            step_row.status.skip_if_pending();
        }
        if status == TerminalStatus::Failed {
            let key = (job_id.to_owned(), case_id.to_owned());
            if !self.failed_cases.contains(&key) {
                self.record_failure(RecordedFailure {
                    job_id: job_id.to_owned(),
                    case_id: Some(case_id.to_owned()),
                    step_id: None,
                    kind: kind.unwrap_or(FailureKind::Execution),
                });
            }
        }
        self.snapshot.elapsed_ms = at_ms;
        Ok(())
    }

    fn finish_job(
        &mut self,
        job_id: &str,
        status: TerminalStatus,
        at_ms: u64,
    ) -> Result<(), ProtocolError> {
        self.ensure_clock_non_decreasing(at_ms)?;
        let job_scope = StepScope {
            job_id: job_id.to_owned(),
            case_id: None,
        };
        let has_running_direct_step = self.running_steps.contains(&job_scope);
        let job = self
            .snapshot
            .jobs_mut()
            .get_mut(job_id)
            .ok_or_else(|| ProtocolError::new(format!("Unknown job: {job_id:?}")))?;
        let has_running_child = has_running_direct_step
            || job
                .cases
                .values()
                .any(|case| case.row.status == Status::Running);
        if has_running_child {
            return Err(ProtocolError::new(format!(
                "Job {job_id:?} still has running steps or cases"
            )));
        }
        let has_failed_child = job.steps.values().any(|row| row.status == Status::Failed)
            || job
                .cases
                .values()
                .any(|case| case.row.status == Status::Failed);
        if status == TerminalStatus::Passed && has_failed_child {
            return Err(ProtocolError::new(format!(
                "Job {job_id:?} cannot pass when it has failed steps or cases"
            )));
        }
        complete_row(&mut job.row, status, at_ms, job_id)?;
        for step_row in job.steps.values_mut() {
            step_row.status.skip_if_pending();
        }
        for case in job.cases.values_mut() {
            case.row.status.skip_if_pending();
            for step_row in case.steps.values_mut() {
                step_row.status.skip_if_pending();
            }
        }
        if status == TerminalStatus::Failed && !self.failed_jobs.contains(job_id) {
            self.record_failure(RecordedFailure {
                job_id: job_id.to_owned(),
                case_id: None,
                step_id: None,
                kind: FailureKind::Execution,
            });
        }
        self.snapshot.elapsed_ms = at_ms;
        Ok(())
    }

    fn finish_run(&mut self, outcome: RunOutcome, elapsed_ms: u64) -> Result<(), ProtocolError> {
        self.ensure_clock_non_decreasing(elapsed_ms)?;
        if self
            .snapshot
            .jobs()
            .values()
            .any(|job| job.row.status == Status::Running)
        {
            return Err(ProtocolError::new(
                "Cannot finish run while jobs are still running",
            ));
        }
        if outcome == RunOutcome::Passed
            && self
                .snapshot
                .jobs()
                .values()
                .any(|job| job.row.status == Status::Failed)
        {
            return Err(ProtocolError::new(
                "Run cannot pass when any job has failed",
            ));
        }
        for job in self.snapshot.jobs_mut().values_mut() {
            job.row.status.skip_if_pending();
            for step_row in job.steps.values_mut() {
                step_row.status.skip_if_pending();
            }
            for case in job.cases.values_mut() {
                case.row.status.skip_if_pending();
                for step_row in case.steps.values_mut() {
                    step_row.status.skip_if_pending();
                }
            }
        }
        self.snapshot.elapsed_ms = elapsed_ms;
        self.snapshot.status = outcome.into();
        Ok(())
    }

    fn ensure_clock_non_decreasing(&self, at_ms: u64) -> Result<(), ProtocolError> {
        if at_ms < self.snapshot.elapsed_ms {
            return Err(ProtocolError::new(format!(
                "Clock moved backwards from {} ms to {at_ms} ms",
                self.snapshot.elapsed_ms
            )));
        }
        Ok(())
    }

    fn ensure_container_running(
        &self,
        job_id: &str,
        case_id: Option<&str>,
    ) -> Result<(), ProtocolError> {
        let job = self
            .snapshot
            .jobs()
            .get(job_id)
            .ok_or_else(|| ProtocolError::new(format!("Unknown job: {job_id:?}")))?;
        if job.row.status != Status::Running {
            return Err(ProtocolError::new(format!("Job {job_id:?} is not running")));
        }
        if let Some(case_id) = case_id {
            let case = job.cases.get(case_id).ok_or_else(|| {
                ProtocolError::new(format!("Unknown case {case_id:?} in {job_id:?}"))
            })?;
            if case.row.status != Status::Running {
                return Err(ProtocolError::new(format!(
                    "Case {case_id:?} in {job_id:?} is not running"
                )));
            }
        }
        Ok(())
    }

    fn find_defined_step_index(&self, scope: &StepScope, step_id: &str) -> Option<(usize, &str)> {
        let &step_idx = self
            .step_locations
            .get(&(scope.clone(), step_id.to_owned()))?;
        let &(col_idx, job_idx) = self.job_locations.get(&scope.job_id)?;
        let job = &self.snapshot.definition().columns[col_idx].jobs[job_idx];
        let step = match scope.case_id.as_deref() {
            Some(case_id) => {
                let &case_idx = self
                    .case_locations
                    .get(&(scope.job_id.clone(), case_id.to_owned()))?;
                job.cases.get(case_idx)?.steps.get(step_idx)?
            }
            None => job.steps.get(step_idx)?,
        };
        Some((step_idx, step.title.as_str()))
    }

    fn append_dynamic_step(
        &mut self,
        scope: &StepScope,
        step_id: &str,
        title: String,
    ) -> Result<usize, ProtocolError> {
        if self.total_steps >= MAX_TOTAL_STEPS {
            return Err(ProtocolError::new(format!(
                "Pipeline run exceeds maximum of {MAX_TOTAL_STEPS} total steps"
            )));
        }
        let &(col_idx, job_idx) = self
            .job_locations
            .get(&scope.job_id)
            .ok_or_else(|| ProtocolError::new(format!("Unknown job: {:?}", scope.job_id)))?;
        let case_idx = match scope.case_id.as_deref() {
            Some(case_id) => Some(
                *self
                    .case_locations
                    .get(&(scope.job_id.clone(), case_id.to_owned()))
                    .ok_or_else(|| {
                        ProtocolError::new(format!(
                            "Unknown case {case_id:?} in {:?}",
                            scope.job_id
                        ))
                    })?,
            ),
            None => None,
        };
        let job = &mut self.snapshot.definition_mut().columns[col_idx].jobs[job_idx];
        let steps = match case_idx {
            Some(idx) => &mut job.cases[idx].steps,
            None => &mut job.steps,
        };
        if steps.len() >= MAX_STEPS_PER_CONTAINER {
            let container = scope.case_id.as_deref().unwrap_or(scope.job_id.as_str());
            return Err(ProtocolError::new(format!(
                "Container {container:?} exceeds maximum of {MAX_STEPS_PER_CONTAINER} steps"
            )));
        }
        let index = steps.len();
        steps.push(Step {
            id: step_id.to_owned(),
            title,
        });
        Ok(index)
    }

    fn steps_map_mut(
        &mut self,
        job_id: &str,
        case_id: Option<&str>,
    ) -> Result<&mut BTreeMap<String, Row>, ProtocolError> {
        let job = self
            .snapshot
            .jobs_mut()
            .get_mut(job_id)
            .ok_or_else(|| ProtocolError::new(format!("Unknown job: {job_id:?}")))?;
        match case_id {
            Some(case_id) => Ok(&mut job
                .cases
                .get_mut(case_id)
                .ok_or_else(|| {
                    ProtocolError::new(format!("Unknown case {case_id:?} in {job_id:?}"))
                })?
                .steps),
            None => Ok(&mut job.steps),
        }
    }

    fn step_row_mut(
        &mut self,
        job_id: &str,
        case_id: Option<&str>,
        step_id: &str,
    ) -> Result<&mut Row, ProtocolError> {
        self.steps_map_mut(job_id, case_id)?
            .get_mut(step_id)
            .ok_or_else(|| ProtocolError::new(format!("Unknown step: {step_id:?}")))
    }
}

fn validate_identifier(id: &str, kind: &str) -> Result<(), ProtocolError> {
    if !valid_id(id) || id.len() > MAX_ID_BYTES {
        return Err(ProtocolError::new(format!("Invalid {kind} ID: {id:?}")));
    }
    Ok(())
}

fn validate_failure_kind(
    status: TerminalStatus,
    kind: Option<FailureKind>,
) -> Result<(), ProtocolError> {
    if status != TerminalStatus::Failed && kind.is_some() {
        return Err(ProtocolError::new(
            "FailureKind is only valid when status is failed",
        ));
    }
    Ok(())
}

fn complete_row(
    row: &mut Row,
    status: TerminalStatus,
    at_ms: u64,
    id: &str,
) -> Result<(), ProtocolError> {
    match status {
        TerminalStatus::Skipped => {
            if row.status != Status::Pending {
                return Err(ProtocolError::new(format!(
                    "Only a pending row can be marked skipped ({id:?})"
                )));
            }
            row.status = Status::Skipped;
            Ok(())
        }
        TerminalStatus::Passed | TerminalStatus::Failed => {
            if row.status != Status::Running {
                return Err(ProtocolError::new(format!(
                    "Cannot finish {id:?} because it is not running"
                )));
            }
            let started = row
                .started_ms
                .ok_or_else(|| ProtocolError::new(format!("Missing started_ms on {id:?}")))?;
            if at_ms < started {
                return Err(ProtocolError::new(format!(
                    "Finish timestamp precedes start timestamp on {id:?}"
                )));
            }
            row.status = status.into();
            row.duration_ms = Some(at_ms - started);
            Ok(())
        }
    }
}

/// Returns true when a running row transitioned to failed.
fn abort_row(row: &mut Row, elapsed_ms: u64) -> bool {
    match row.status {
        Status::Running => {
            row.status = Status::Failed;
            row.duration_ms = row.started_ms.map(|start| elapsed_ms.saturating_sub(start));
            true
        }
        Status::Pending => {
            row.status = Status::Skipped;
            false
        }
        Status::Passed | Status::Failed | Status::Skipped => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        limits::{MAX_EVENT_LINE_BYTES, MAX_RUN_STARTED_LINE_BYTES},
        model::{Column, Definition, Job},
        v2::validate_snapshot,
        v3::{decode_line, encode_line},
    };

    const FIXTURE_V3: &str = include_str!("../../fixtures/web_regression_v3.ndjson");

    fn replay(ndjson: &str) -> RunState {
        let mut lines = ndjson.lines().filter(|line| !line.trim().is_empty());
        let first = decode_line(lines.next().expect("run_started line")).unwrap();
        let mut state = RunState::start(first).unwrap();
        validate_snapshot(state.snapshot()).unwrap();
        for line in lines {
            let event = decode_line(line).unwrap();
            let round_tripped = decode_line(&encode_line(&event).unwrap()).unwrap();
            assert_eq!(event, round_tripped);
            let snapshot = state.apply(event).unwrap();
            validate_snapshot(snapshot).unwrap();
        }
        state
    }

    #[test]
    fn replays_canonical_v3_fixture_with_nested_cases_dynamic_steps_and_cleanup() {
        let state = replay(FIXTURE_V3);
        assert!(state.is_finished());
        assert_eq!(state.workers(), 1);
        let snapshot = state.snapshot();
        assert_eq!(snapshot.status, Status::Failed);
        assert_eq!(snapshot.elapsed_ms, 6_400);
        assert_eq!(snapshot.definition().test_case_count(), 3);

        // Prepare and functionality passed; errors-recovery failed with cleanup step passed.
        assert_eq!(snapshot.job_status("prepare-env"), Some(Status::Passed));
        assert_eq!(snapshot.job_status("functionality"), Some(Status::Passed));
        assert_eq!(snapshot.job_status("errors-recovery"), Some(Status::Failed));
        assert_eq!(snapshot.job_status("cleanup-env"), Some(Status::Passed));
        assert_eq!(snapshot.job_status("publish-report"), Some(Status::Passed));

        // Dynamic step inside case + cleanup step after assertion failure.
        assert_eq!(
            snapshot
                .case_row("errors-recovery", "dns-races", Some("retry-write"))
                .map(|row| (row.status, row.duration_ms)),
            Some((Status::Failed, Some(600)))
        );
        assert_eq!(
            snapshot
                .case_row("errors-recovery", "dns-races", Some("close-context"))
                .map(|row| (row.status, row.duration_ms)),
            Some((Status::Passed, Some(100)))
        );
        assert_eq!(
            snapshot
                .case_row("errors-recovery", "menu-races", None)
                .map(|row| row.status),
            Some(Status::Skipped)
        );
        assert_eq!(
            state.failures(),
            &[RecordedFailure {
                job_id: "errors-recovery".into(),
                case_id: Some("dns-races".into()),
                step_id: Some("retry-write".into()),
                kind: FailureKind::Assertion,
            }]
        );
    }

    fn minimal_run_started() -> Event {
        Event::RunStarted {
            schema_version: SCHEMA_VERSION,
            workers: Some(2),
            definition: Definition {
                title: "Minimal".into(),
                description: "Reducer unit test".into(),
                columns: vec![
                    Column {
                        title: "Prepare".into(),
                        jobs: vec![Job {
                            id: "prep".into(),
                            title: "Prepare".into(),
                            is_test: false,
                            dependencies: vec![],
                            steps: vec![
                                Step {
                                    id: "s1".into(),
                                    title: "Step 1".into(),
                                },
                                Step {
                                    id: "s2".into(),
                                    title: "Step 2".into(),
                                },
                            ],
                            cases: vec![],
                        }],
                    },
                    Column {
                        title: "Verify".into(),
                        jobs: vec![Job {
                            id: "verify".into(),
                            title: "Verify".into(),
                            is_test: true,
                            dependencies: vec!["prep".into()],
                            steps: vec![],
                            cases: vec![Case {
                                id: "c1".into(),
                                title: "Case 1".into(),
                                steps: vec![],
                            }],
                        }],
                    },
                ],
            },
        }
    }

    #[test]
    fn finalize_aborted_marks_running_as_failed_and_pending_as_skipped() {
        let mut state = RunState::start(minimal_run_started()).unwrap();
        state
            .apply(Event::JobStarted {
                job_id: "prep".into(),
                at_ms: 100,
            })
            .unwrap();
        state
            .apply(Event::StepStarted {
                job_id: "prep".into(),
                case_id: None,
                step_id: "s1".into(),
                title: None,
                at_ms: 150,
            })
            .unwrap();

        let terminal = state.finalize_aborted(500).clone();
        validate_snapshot(&terminal).unwrap();
        assert_eq!(terminal.status, Status::Failed);
        assert_eq!(terminal.elapsed_ms, 500);
        assert_eq!(
            terminal.row("prep", Some("s1")),
            Some(&Row {
                status: Status::Failed,
                started_ms: Some(150),
                duration_ms: Some(350),
            })
        );
        assert_eq!(
            terminal.row("prep", Some("s2")).map(|row| row.status),
            Some(Status::Skipped)
        );
        assert_eq!(terminal.job_status("verify"), Some(Status::Skipped));
        assert_eq!(
            terminal
                .case_row("verify", "c1", None)
                .map(|row| row.status),
            Some(Status::Skipped)
        );
        assert_eq!(
            state.failures(),
            &[RecordedFailure {
                job_id: "prep".into(),
                case_id: None,
                step_id: Some("s1".into()),
                kind: FailureKind::Execution,
            }]
        );

        // Idempotent and rejects further events once terminal.
        assert_eq!(state.finalize_aborted(900), &terminal);
        assert!(state.apply(Event::Tick { elapsed_ms: 600 }).is_err());
    }

    #[test]
    fn rejects_invalid_transitions_clock_regression_overlaps_and_oversized_lines() {
        // Non-run_started first event, wrong version, zero workers.
        assert!(RunState::start(Event::Tick { elapsed_ms: 0 }).is_err());
        assert!(
            RunState::start(Event::RunStarted {
                schema_version: 2,
                definition: match minimal_run_started() {
                    Event::RunStarted { definition, .. } => definition,
                    _ => unreachable!(),
                },
                workers: None,
            })
            .is_err()
        );
        assert!(
            RunState::start(Event::RunStarted {
                schema_version: 3,
                definition: match minimal_run_started() {
                    Event::RunStarted { definition, .. } => definition,
                    _ => unreachable!(),
                },
                workers: Some(0),
            })
            .is_err()
        );

        let mut state = RunState::start(minimal_run_started()).unwrap();
        // Dependency not finished yet.
        assert!(
            state
                .apply(Event::JobStarted {
                    job_id: "verify".into(),
                    at_ms: 10,
                })
                .is_err()
        );
        // Second run_started rejected.
        assert!(state.apply(minimal_run_started()).is_err());

        state
            .apply(Event::JobStarted {
                job_id: "prep".into(),
                at_ms: 100,
            })
            .unwrap();
        // Clock regression rejected.
        assert!(state.apply(Event::Tick { elapsed_ms: 99 }).is_err());

        // Start s2 first (allowed, skips s1), then trying to start s1 out of order must fail.
        state
            .apply(Event::StepStarted {
                job_id: "prep".into(),
                case_id: None,
                step_id: "s2".into(),
                title: None,
                at_ms: 110,
            })
            .unwrap();
        // Overlapping step inside same job rejected.
        assert!(
            state
                .apply(Event::StepStarted {
                    job_id: "prep".into(),
                    case_id: None,
                    step_id: "s1".into(),
                    title: None,
                    at_ms: 120,
                })
                .is_err()
        );
        // FailureKind on Passed step rejected.
        assert!(
            state
                .apply(Event::StepFinished {
                    job_id: "prep".into(),
                    case_id: None,
                    step_id: "s2".into(),
                    status: TerminalStatus::Passed,
                    at_ms: 130,
                    kind: Some(FailureKind::Timeout),
                })
                .is_err()
        );
        state
            .apply(Event::StepFinished {
                job_id: "prep".into(),
                case_id: None,
                step_id: "s2".into(),
                status: TerminalStatus::Failed,
                at_ms: 130,
                kind: Some(FailureKind::Timeout),
            })
            .unwrap();
        // Out-of-order s1 after s2 rejected.
        assert!(
            state
                .apply(Event::StepStarted {
                    job_id: "prep".into(),
                    case_id: None,
                    step_id: "s1".into(),
                    title: None,
                    at_ms: 140,
                })
                .is_err()
        );
        // Passing a job with a failed step rejected.
        assert!(
            state
                .apply(Event::JobFinished {
                    job_id: "prep".into(),
                    status: TerminalStatus::Passed,
                    at_ms: 150,
                })
                .is_err()
        );
        state
            .apply(Event::JobFinished {
                job_id: "prep".into(),
                status: TerminalStatus::Failed,
                at_ms: 150,
            })
            .unwrap();
        // Passing the run when a job failed rejected.
        assert!(
            state
                .apply(Event::RunFinished {
                    status: RunOutcome::Passed,
                    elapsed_ms: 160,
                })
                .is_err()
        );

        // Decode guards: empty, multiline, unknown field, oversized event.
        assert!(decode_line("").is_err());
        assert!(
            decode_line(
                "{\"type\":\"tick\",\"elapsed_ms\":1}\n{\"type\":\"tick\",\"elapsed_ms\":2}"
            )
            .is_err()
        );
        assert!(decode_line("{\"type\":\"tick\",\"elapsed_ms\":1,\"extra\":true}").is_err());
        let oversized_tick = format!(
            "{{\"type\":\"tick\",\"elapsed_ms\":1{}}}",
            " ".repeat(MAX_EVENT_LINE_BYTES)
        );
        assert!(decode_line(&oversized_tick).is_err());
        assert!(decode_line(&" ".repeat(MAX_RUN_STARTED_LINE_BYTES + 1)).is_err());
    }

    #[test]
    fn supports_parallel_jobs_dynamic_cases_and_case_level_failures() {
        let mut state = RunState::start(minimal_run_started()).unwrap();
        assert_eq!(state.workers(), 2);

        // Complete prep.
        state
            .apply(Event::JobStarted {
                job_id: "prep".into(),
                at_ms: 0,
            })
            .unwrap();
        state
            .apply(Event::JobFinished {
                job_id: "prep".into(),
                status: TerminalStatus::Passed,
                at_ms: 10,
            })
            .unwrap();

        // Start verify job and run a pre-declared case + a dynamically registered case concurrently.
        state
            .apply(Event::JobStarted {
                job_id: "verify".into(),
                at_ms: 20,
            })
            .unwrap();
        state
            .apply(Event::CaseStarted {
                job_id: "verify".into(),
                case_id: "c1".into(),
                title: None,
                at_ms: 25,
            })
            .unwrap();
        // Dynamic case without title must fail.
        assert!(
            state
                .apply(Event::CaseStarted {
                    job_id: "verify".into(),
                    case_id: "c2".into(),
                    title: None,
                    at_ms: 30,
                })
                .is_err()
        );
        state
            .apply(Event::CaseStarted {
                job_id: "verify".into(),
                case_id: "c2".into(),
                title: Some("Dynamic case 2".into()),
                at_ms: 30,
            })
            .unwrap();
        state
            .apply(Event::StepStarted {
                job_id: "verify".into(),
                case_id: Some("c2".into()),
                step_id: "dyn-step".into(),
                title: Some("Dynamic step".into()),
                at_ms: 35,
            })
            .unwrap();
        state
            .apply(Event::StepFinished {
                job_id: "verify".into(),
                case_id: Some("c2".into()),
                step_id: "dyn-step".into(),
                status: TerminalStatus::Passed,
                at_ms: 55,
                kind: None,
            })
            .unwrap();
        state
            .apply(Event::CaseFinished {
                job_id: "verify".into(),
                case_id: "c2".into(),
                status: TerminalStatus::Passed,
                at_ms: 60,
                kind: None,
            })
            .unwrap();
        state
            .apply(Event::CaseFinished {
                job_id: "verify".into(),
                case_id: "c1".into(),
                status: TerminalStatus::Failed,
                at_ms: 80,
                kind: Some(FailureKind::Timeout),
            })
            .unwrap();
        state
            .apply(Event::JobFinished {
                job_id: "verify".into(),
                status: TerminalStatus::Failed,
                at_ms: 90,
            })
            .unwrap();
        state
            .apply(Event::RunFinished {
                status: RunOutcome::Failed,
                elapsed_ms: 100,
            })
            .unwrap();

        assert_eq!(
            state.failures(),
            &[RecordedFailure {
                job_id: "verify".into(),
                case_id: Some("c1".into()),
                step_id: None,
                kind: FailureKind::Timeout,
            }]
        );
        let terminal = state.into_snapshot();
        validate_snapshot(&terminal).unwrap();
        assert_eq!(terminal.definition().test_case_count(), 2);
        assert_eq!(
            terminal
                .case_row("verify", "c2", Some("dyn-step"))
                .map(|row| (row.status, row.duration_ms)),
            Some((Status::Passed, Some(20)))
        );
    }

    #[test]
    fn enforces_dynamic_case_step_and_failure_quotas() {
        // 1) Dynamic cases per job and total cases quotas.
        let jobs_for_total_cases = (MAX_TOTAL_CASES / MAX_CASES_PER_JOB) + 1;
        let case_def = Definition {
            title: "Case quota".into(),
            description: "Tests dynamic case limits".into(),
            columns: vec![Column {
                title: "Verify".into(),
                jobs: (0..jobs_for_total_cases)
                    .map(|j| Job {
                        id: format!("j-{j}"),
                        title: format!("Job {j}"),
                        is_test: true,
                        dependencies: vec![],
                        steps: vec![],
                        cases: vec![],
                    })
                    .collect(),
            }],
        };
        let mut state = RunState::start(Event::RunStarted {
            schema_version: SCHEMA_VERSION,
            definition: case_def,
            workers: Some(1),
        })
        .unwrap();

        let mut clock = 0u64;
        for j in 0..jobs_for_total_cases {
            let job_id = format!("j-{j}");
            state
                .apply(Event::JobStarted {
                    job_id: job_id.clone(),
                    at_ms: clock,
                })
                .unwrap();
            let is_last_job = j + 1 == jobs_for_total_cases;
            if !is_last_job {
                for c in 0..MAX_CASES_PER_JOB {
                    let case_id = format!("c-{c}");
                    state
                        .apply(Event::CaseStarted {
                            job_id: job_id.clone(),
                            case_id: case_id.clone(),
                            title: Some(format!("Case {c}")),
                            at_ms: clock,
                        })
                        .unwrap();
                    clock += 1;
                    state
                        .apply(Event::CaseFinished {
                            job_id: job_id.clone(),
                            case_id,
                            status: TerminalStatus::Failed,
                            at_ms: clock,
                            kind: Some(FailureKind::Assertion),
                        })
                        .unwrap();
                }
                // Exceeding MAX_CASES_PER_JOB on job 0 must fail.
                if j == 0 {
                    assert!(
                        state
                            .apply(Event::CaseStarted {
                                job_id: job_id.clone(),
                                case_id: "overflow-case".into(),
                                title: Some("Overflow".into()),
                                at_ms: clock,
                            })
                            .is_err()
                    );
                }
                state
                    .apply(Event::JobFinished {
                        job_id,
                        status: TerminalStatus::Failed,
                        at_ms: clock,
                    })
                    .unwrap();
            } else {
                // Total cases across all prior jobs already equals MAX_TOTAL_CASES.
                assert!(
                    state
                        .apply(Event::CaseStarted {
                            job_id: job_id.clone(),
                            case_id: "total-overflow".into(),
                            title: Some("Total overflow".into()),
                            at_ms: clock,
                        })
                        .is_err()
                );
                state
                    .apply(Event::JobFinished {
                        job_id,
                        status: TerminalStatus::Passed,
                        at_ms: clock,
                    })
                    .unwrap();
            }
        }
        // Recorded failures are capped at MAX_RECORDED_FAILURES while run still fails.
        assert_eq!(state.failures().len(), MAX_RECORDED_FAILURES);
        state
            .apply(Event::RunFinished {
                status: RunOutcome::Failed,
                elapsed_ms: clock,
            })
            .unwrap();
        assert_eq!(state.snapshot().status, Status::Failed);

        // 2) Dynamic steps per container and total steps quotas.
        let jobs_for_total_steps = (MAX_TOTAL_STEPS / MAX_STEPS_PER_CONTAINER) + 1;
        let step_def = Definition {
            title: "Step quota".into(),
            description: "Tests dynamic step limits".into(),
            columns: vec![Column {
                title: "Verify".into(),
                jobs: (0..jobs_for_total_steps)
                    .map(|j| Job {
                        id: format!("j-{j}"),
                        title: format!("Job {j}"),
                        is_test: true,
                        dependencies: vec![],
                        steps: vec![],
                        cases: vec![],
                    })
                    .collect(),
            }],
        };
        let mut step_state = RunState::start(Event::RunStarted {
            schema_version: SCHEMA_VERSION,
            definition: step_def,
            workers: Some(1),
        })
        .unwrap();
        let mut step_clock = 0u64;
        for j in 0..jobs_for_total_steps {
            let job_id = format!("j-{j}");
            step_state
                .apply(Event::JobStarted {
                    job_id: job_id.clone(),
                    at_ms: step_clock,
                })
                .unwrap();
            let is_last_job = j + 1 == jobs_for_total_steps;
            if !is_last_job {
                for s in 0..MAX_STEPS_PER_CONTAINER {
                    let step_id = format!("s-{s}");
                    step_state
                        .apply(Event::StepStarted {
                            job_id: job_id.clone(),
                            case_id: None,
                            step_id: step_id.clone(),
                            title: Some(format!("Step {s}")),
                            at_ms: step_clock,
                        })
                        .unwrap();
                    step_clock += 1;
                    step_state
                        .apply(Event::StepFinished {
                            job_id: job_id.clone(),
                            case_id: None,
                            step_id,
                            status: TerminalStatus::Passed,
                            at_ms: step_clock,
                            kind: None,
                        })
                        .unwrap();
                }
                if j == 0 {
                    assert!(
                        step_state
                            .apply(Event::StepStarted {
                                job_id: job_id.clone(),
                                case_id: None,
                                step_id: "overflow-step".into(),
                                title: Some("Overflow".into()),
                                at_ms: step_clock,
                            })
                            .is_err()
                    );
                }
                step_state
                    .apply(Event::JobFinished {
                        job_id,
                        status: TerminalStatus::Passed,
                        at_ms: step_clock,
                    })
                    .unwrap();
            } else {
                assert!(
                    step_state
                        .apply(Event::StepStarted {
                            job_id: job_id.clone(),
                            case_id: None,
                            step_id: "total-overflow-step".into(),
                            title: Some("Total overflow".into()),
                            at_ms: step_clock,
                        })
                        .is_err()
                );
                step_state
                    .apply(Event::JobFinished {
                        job_id,
                        status: TerminalStatus::Passed,
                        at_ms: step_clock,
                    })
                    .unwrap();
            }
        }
        step_state
            .apply(Event::RunFinished {
                status: RunOutcome::Passed,
                elapsed_ms: step_clock,
            })
            .unwrap();
        assert_eq!(step_state.snapshot().status, Status::Passed);
    }
}
