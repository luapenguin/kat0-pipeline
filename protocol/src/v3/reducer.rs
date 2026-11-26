// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt,
};

use crate::{
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
    last_step_index: HashMap<StepScope, usize>,
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
        Ok(Self {
            snapshot,
            workers,
            failures: Vec::new(),
            last_step_index: HashMap::new(),
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

        let job_ids: Vec<String> = self
            .snapshot
            .definition()
            .jobs()
            .map(|job| job.id.clone())
            .collect();
        for job_id in job_ids {
            let Some(job) = self.snapshot.jobs_mut().get_mut(&job_id) else {
                continue;
            };
            let mut recorded_inside_job = false;
            for (step_id, row) in &mut job.steps {
                if abort_row(row, elapsed) {
                    recorded_inside_job = true;
                    self.failures.push(RecordedFailure {
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
                        self.failures.push(RecordedFailure {
                            job_id: job_id.clone(),
                            case_id: Some(case_id.clone()),
                            step_id: Some(step_id.clone()),
                            kind: FailureKind::Execution,
                        });
                    }
                }
                if abort_row(&mut case_state.row, elapsed) && !recorded_inside_case {
                    recorded_inside_job = true;
                    self.failures.push(RecordedFailure {
                        job_id: job_id.clone(),
                        case_id: Some(case_id.clone()),
                        step_id: None,
                        kind: FailureKind::Execution,
                    });
                }
            }
            if abort_row(&mut job.row, elapsed) && !recorded_inside_job {
                self.failures.push(RecordedFailure {
                    job_id,
                    case_id: None,
                    step_id: None,
                    kind: FailureKind::Execution,
                });
            }
        }
        &self.snapshot
    }

    fn advance_clock(&mut self, next_ms: u64) -> Result<(), ProtocolError> {
        if next_ms < self.snapshot.elapsed_ms {
            return Err(ProtocolError::new(format!(
                "El reloj retrocedió de {} ms a {next_ms} ms",
                self.snapshot.elapsed_ms
            )));
        }
        self.snapshot.elapsed_ms = next_ms;
        Ok(())
    }

    fn start_job(&mut self, job_id: &str, at_ms: u64) -> Result<(), ProtocolError> {
        self.ensure_clock_non_decreasing(at_ms)?;
        let dependencies: Vec<String> = self
            .snapshot
            .definition()
            .jobs()
            .find(|job| job.id == job_id)
            .ok_or_else(|| ProtocolError::new(format!("Job desconocido: {job_id:?}")))?
            .dependencies
            .clone();
        for dependency in &dependencies {
            let status = self.snapshot.job_status(dependency).ok_or_else(|| {
                ProtocolError::new(format!("Dependencia desconocida: {dependency:?}"))
            })?;
            if matches!(status, Status::Pending | Status::Running) {
                return Err(ProtocolError::new(format!(
                    "El job {job_id:?} no puede iniciar antes de que termine {dependency:?}"
                )));
            }
        }
        let job = self
            .snapshot
            .jobs_mut()
            .get_mut(job_id)
            .ok_or_else(|| ProtocolError::new(format!("Job desconocido: {job_id:?}")))?;
        if job.row.status != Status::Pending {
            return Err(ProtocolError::new(format!(
                "El job {job_id:?} ya fue iniciado o finalizado"
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
        validate_identifier(case_id, "caso")?;
        let job_state = self
            .snapshot
            .jobs()
            .get(job_id)
            .ok_or_else(|| ProtocolError::new(format!("Job desconocido: {job_id:?}")))?;
        if job_state.row.status != Status::Running {
            return Err(ProtocolError::new(format!(
                "No se puede iniciar el caso {case_id:?}: el job {job_id:?} no está en ejecución"
            )));
        }

        if let Some(existing_case) = job_state.cases.get(case_id) {
            if existing_case.row.status != Status::Pending {
                return Err(ProtocolError::new(format!(
                    "El caso {case_id:?} en {job_id:?} ya fue iniciado o finalizado"
                )));
            }
            if let Some(provided_title) = title.as_deref() {
                let expected_title = self
                    .snapshot
                    .definition()
                    .jobs()
                    .find(|job| job.id == job_id)
                    .and_then(|job| job.cases.iter().find(|case| case.id == case_id))
                    .map(|case| case.title.as_str());
                if expected_title != Some(provided_title) {
                    return Err(ProtocolError::new(format!(
                        "El título del caso {case_id:?} no coincide con la definición"
                    )));
                }
            }
            let case_state = self
                .snapshot
                .jobs_mut()
                .get_mut(job_id)
                .expect("checked job")
                .cases
                .get_mut(case_id)
                .expect("checked case");
            case_state.row = Row {
                status: Status::Running,
                started_ms: Some(at_ms),
                duration_ms: None,
            };
        } else {
            let title = title.ok_or_else(|| {
                ProtocolError::new(format!(
                    "El caso dinámico {case_id:?} en {job_id:?} requiere title"
                ))
            })?;
            validate_label(case_id, &title)?;
            let def_job = self
                .snapshot
                .definition_mut()
                .columns
                .iter_mut()
                .flat_map(|column| &mut column.jobs)
                .find(|job| job.id == job_id)
                .expect("checked job definition");
            def_job.cases.push(Case {
                id: case_id.to_owned(),
                title,
                steps: Vec::new(),
            });
            self.snapshot
                .jobs_mut()
                .get_mut(job_id)
                .expect("checked job")
                .cases
                .insert(
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
        validate_identifier(step_id, "paso")?;
        self.ensure_container_running(job_id, case_id)?;

        // Steps inside the same job or case must not overlap.
        let has_running_step = match case_id {
            Some(case_id) => self
                .snapshot
                .jobs()
                .get(job_id)
                .and_then(|job| job.cases.get(case_id))
                .is_some_and(|case| case.steps.values().any(|row| row.status == Status::Running)),
            None => self
                .snapshot
                .jobs()
                .get(job_id)
                .is_some_and(|job| job.steps.values().any(|row| row.status == Status::Running)),
        };
        if has_running_step {
            return Err(ProtocolError::new(format!(
                "Ya hay un paso en ejecución dentro de {job_id:?}"
            )));
        }

        let scope = StepScope {
            job_id: job_id.to_owned(),
            case_id: case_id.map(str::to_owned),
        };
        let existing_index = self.find_defined_step_index(job_id, case_id, step_id);

        if let Some((index, expected_title)) = existing_index {
            if let Some(provided) = title.as_deref()
                && provided != expected_title
            {
                return Err(ProtocolError::new(format!(
                    "El título del paso {step_id:?} no coincide con la definición"
                )));
            }
            if self
                .last_step_index
                .get(&scope)
                .is_some_and(|&previous| index <= previous)
            {
                return Err(ProtocolError::new(format!(
                    "El paso {step_id:?} está fuera del orden definido"
                )));
            }
            let row = self.step_row_mut(job_id, case_id, step_id)?;
            if row.status != Status::Pending {
                return Err(ProtocolError::new(format!(
                    "El paso {step_id:?} ya fue iniciado o finalizado"
                )));
            }
            *row = Row {
                status: Status::Running,
                started_ms: Some(at_ms),
                duration_ms: None,
            };
            self.last_step_index.insert(scope, index);
        } else {
            let title = title.ok_or_else(|| {
                ProtocolError::new(format!("El paso dinámico {step_id:?} requiere title"))
            })?;
            validate_label(step_id, &title)?;
            let new_index = self.append_dynamic_step(job_id, case_id, step_id, title)?;
            let steps_map = self.steps_map_mut(job_id, case_id)?;
            steps_map.insert(
                step_id.to_owned(),
                Row {
                    status: Status::Running,
                    started_ms: Some(at_ms),
                    duration_ms: None,
                },
            );
            self.last_step_index.insert(scope, new_index);
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
        if status == TerminalStatus::Failed {
            self.failures.push(RecordedFailure {
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
        let job = self
            .snapshot
            .jobs_mut()
            .get_mut(job_id)
            .ok_or_else(|| ProtocolError::new(format!("Job desconocido: {job_id:?}")))?;
        if job.row.status != Status::Running {
            return Err(ProtocolError::new(format!(
                "No se puede finalizar el caso {case_id:?}: el job {job_id:?} no está en ejecución"
            )));
        }
        let case = job.cases.get_mut(case_id).ok_or_else(|| {
            ProtocolError::new(format!("Caso desconocido {case_id:?} en {job_id:?}"))
        })?;
        if case.steps.values().any(|row| row.status == Status::Running) {
            return Err(ProtocolError::new(format!(
                "El caso {case_id:?} aún tiene un paso en ejecución"
            )));
        }
        if status == TerminalStatus::Passed
            && case.steps.values().any(|row| row.status == Status::Failed)
        {
            return Err(ProtocolError::new(format!(
                "El caso {case_id:?} no puede aprobarse si tiene pasos fallidos"
            )));
        }
        complete_row(&mut case.row, status, at_ms, case_id)?;
        for step_row in case.steps.values_mut() {
            step_row.status.skip_if_pending();
        }
        if status == TerminalStatus::Failed {
            let has_step_failure = self
                .failures
                .iter()
                .any(|entry| entry.job_id == job_id && entry.case_id.as_deref() == Some(case_id));
            if !has_step_failure {
                self.failures.push(RecordedFailure {
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
        let job = self
            .snapshot
            .jobs_mut()
            .get_mut(job_id)
            .ok_or_else(|| ProtocolError::new(format!("Job desconocido: {job_id:?}")))?;
        let has_running_child = job.steps.values().any(|row| row.status == Status::Running)
            || job
                .cases
                .values()
                .any(|case| case.row.status == Status::Running);
        if has_running_child {
            return Err(ProtocolError::new(format!(
                "El job {job_id:?} aún tiene pasos o casos en ejecución"
            )));
        }
        let has_failed_child = job.steps.values().any(|row| row.status == Status::Failed)
            || job
                .cases
                .values()
                .any(|case| case.row.status == Status::Failed);
        if status == TerminalStatus::Passed && has_failed_child {
            return Err(ProtocolError::new(format!(
                "El job {job_id:?} no puede aprobarse con pasos o casos fallidos"
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
        if status == TerminalStatus::Failed {
            let has_child_failure = self.failures.iter().any(|entry| entry.job_id == job_id);
            if !has_child_failure {
                self.failures.push(RecordedFailure {
                    job_id: job_id.to_owned(),
                    case_id: None,
                    step_id: None,
                    kind: FailureKind::Execution,
                });
            }
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
                "No se puede finalizar la ejecución mientras haya jobs en curso",
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
                "La ejecución no puede aprobarse si algún job falló",
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
                "El reloj retrocedió de {} ms a {at_ms} ms",
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
            .ok_or_else(|| ProtocolError::new(format!("Job desconocido: {job_id:?}")))?;
        if job.row.status != Status::Running {
            return Err(ProtocolError::new(format!(
                "El job {job_id:?} no está en ejecución"
            )));
        }
        if let Some(case_id) = case_id {
            let case = job.cases.get(case_id).ok_or_else(|| {
                ProtocolError::new(format!("Caso desconocido {case_id:?} en {job_id:?}"))
            })?;
            if case.row.status != Status::Running {
                return Err(ProtocolError::new(format!(
                    "El caso {case_id:?} en {job_id:?} no está en ejecución"
                )));
            }
        }
        Ok(())
    }

    fn find_defined_step_index(
        &self,
        job_id: &str,
        case_id: Option<&str>,
        step_id: &str,
    ) -> Option<(usize, &str)> {
        let job = self
            .snapshot
            .definition()
            .jobs()
            .find(|job| job.id == job_id)?;
        let steps = match case_id {
            Some(case_id) => &job.cases.iter().find(|case| case.id == case_id)?.steps,
            None => &job.steps,
        };
        steps
            .iter()
            .enumerate()
            .find(|(_, step)| step.id == step_id)
            .map(|(index, step)| (index, step.title.as_str()))
    }

    fn append_dynamic_step(
        &mut self,
        job_id: &str,
        case_id: Option<&str>,
        step_id: &str,
        title: String,
    ) -> Result<usize, ProtocolError> {
        let job = self
            .snapshot
            .definition_mut()
            .columns
            .iter_mut()
            .flat_map(|column| &mut column.jobs)
            .find(|job| job.id == job_id)
            .ok_or_else(|| ProtocolError::new(format!("Job desconocido: {job_id:?}")))?;
        let steps = match case_id {
            Some(case_id) => {
                &mut job
                    .cases
                    .iter_mut()
                    .find(|case| case.id == case_id)
                    .ok_or_else(|| {
                        ProtocolError::new(format!("Caso desconocido {case_id:?} en {job_id:?}"))
                    })?
                    .steps
            }
            None => &mut job.steps,
        };
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
            .ok_or_else(|| ProtocolError::new(format!("Job desconocido: {job_id:?}")))?;
        match case_id {
            Some(case_id) => Ok(&mut job
                .cases
                .get_mut(case_id)
                .ok_or_else(|| {
                    ProtocolError::new(format!("Caso desconocido {case_id:?} en {job_id:?}"))
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
            .ok_or_else(|| ProtocolError::new(format!("Paso desconocido: {step_id:?}")))
    }
}

fn validate_identifier(id: &str, kind: &str) -> Result<(), ProtocolError> {
    if !valid_id(id) || id.len() > crate::limits::MAX_ID_BYTES {
        return Err(ProtocolError::new(format!(
            "ID de {kind} no válido: {id:?}"
        )));
    }
    Ok(())
}

fn validate_failure_kind(
    status: TerminalStatus,
    kind: Option<FailureKind>,
) -> Result<(), ProtocolError> {
    if status != TerminalStatus::Failed && kind.is_some() {
        return Err(ProtocolError::new(
            "FailureKind solo es válido cuando el estado es failed",
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
                    "Solo una fila pendiente puede marcarse como skipped ({id:?})"
                )));
            }
            row.status = Status::Skipped;
            Ok(())
        }
        TerminalStatus::Passed | TerminalStatus::Failed => {
            if row.status != Status::Running {
                return Err(ProtocolError::new(format!(
                    "No se puede finalizar {id:?} porque no está en ejecución"
                )));
            }
            let started = row
                .started_ms
                .ok_or_else(|| ProtocolError::new(format!("Falta started_ms en {id:?}")))?;
            if at_ms < started {
                return Err(ProtocolError::new(format!(
                    "Fin anterior al inicio en {id:?}"
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
}
