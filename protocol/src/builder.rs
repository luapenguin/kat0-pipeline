// SPDX-License-Identifier: AGPL-3.0-only

//! Fluent builders for constructing and validating [`Definition`] topologies.

use crate::model::{Case, Column, Definition, DefinitionError, Job, Step};

/// Builder for a validated [`Definition`].
///
/// # Example
///
/// ```
/// use kat0_pipeline_protocol::builder::PipelineBuilder;
///
/// let definition = PipelineBuilder::new("Web regression", "End-to-end verification")
///     .stage("Prepare", |stage| {
///         stage.job("env", "Test env", |job| {
///             job.step("boot", "Start server");
///         });
///     })
///     .stage("Verify", |stage| {
///         stage.test_job("auth", "Authentication", |job| {
///             job.depends_on("env")
///                 .case("login", "Sign in", |case| {
///                     case.step("submit", "Submit form");
///                 });
///         });
///     })
///     .build()
///     .expect("valid topology");
///
/// assert_eq!(definition.columns.len(), 2);
/// assert_eq!(definition.test_case_count(), 1);
/// ```
#[derive(Clone, Debug)]
pub struct PipelineBuilder {
    title: String,
    description: String,
    columns: Vec<Column>,
}

impl PipelineBuilder {
    /// Starts a new pipeline definition with the given title and description.
    pub fn new(title: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            description: description.into(),
            columns: Vec::new(),
        }
    }

    /// Appends a stage column configured by `configure`.
    pub fn stage(
        mut self,
        title: impl Into<String>,
        configure: impl FnOnce(&mut StageBuilder),
    ) -> Self {
        let mut stage = StageBuilder {
            title: title.into(),
            jobs: Vec::new(),
        };
        configure(&mut stage);
        self.columns.push(Column {
            title: stage.title,
            jobs: stage.jobs,
        });
        self
    }

    /// Validates and returns the completed [`Definition`].
    pub fn build(self) -> Result<Definition, DefinitionError> {
        let definition = Definition {
            title: self.title,
            description: self.description,
            columns: self.columns,
        };
        definition.validate()?;
        Ok(definition)
    }
}

/// Builder for jobs belonging to a single stage [`Column`].
#[derive(Clone, Debug)]
pub struct StageBuilder {
    title: String,
    jobs: Vec<Job>,
}

impl StageBuilder {
    /// Adds a non-test lifecycle job (for example setup, cleanup, or reporting).
    pub fn job(
        &mut self,
        id: impl Into<String>,
        title: impl Into<String>,
        configure: impl FnOnce(&mut JobBuilder),
    ) -> &mut Self {
        self.push_job(id, title, false, configure)
    }

    /// Adds a test job included in test counts and failure summaries.
    pub fn test_job(
        &mut self,
        id: impl Into<String>,
        title: impl Into<String>,
        configure: impl FnOnce(&mut JobBuilder),
    ) -> &mut Self {
        self.push_job(id, title, true, configure)
    }

    fn push_job(
        &mut self,
        id: impl Into<String>,
        title: impl Into<String>,
        is_test: bool,
        configure: impl FnOnce(&mut JobBuilder),
    ) -> &mut Self {
        let mut job = JobBuilder {
            id: id.into(),
            title: title.into(),
            is_test,
            dependencies: Vec::new(),
            steps: Vec::new(),
            cases: Vec::new(),
        };
        configure(&mut job);
        self.jobs.push(Job {
            id: job.id,
            title: job.title,
            is_test: job.is_test,
            dependencies: job.dependencies,
            steps: job.steps,
            cases: job.cases,
        });
        self
    }
}

/// Builder for a single [`Job`] node.
#[derive(Clone, Debug)]
pub struct JobBuilder {
    id: String,
    title: String,
    is_test: bool,
    dependencies: Vec<String>,
    steps: Vec<Step>,
    cases: Vec<Case>,
}

impl JobBuilder {
    /// Declares a dependency on a job ID from an earlier stage.
    pub fn depends_on(&mut self, job_id: impl Into<String>) -> &mut Self {
        self.dependencies.push(job_id.into());
        self
    }

    /// Declares multiple dependencies on job IDs from earlier stages.
    pub fn depends_on_all<I, S>(&mut self, job_ids: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.dependencies
            .extend(job_ids.into_iter().map(Into::into));
        self
    }

    /// Appends a direct step to the job.
    pub fn step(&mut self, id: impl Into<String>, title: impl Into<String>) -> &mut Self {
        self.steps.push(Step {
            id: id.into(),
            title: title.into(),
        });
        self
    }

    /// Appends a nested [`Case`] to the job.
    pub fn case(
        &mut self,
        id: impl Into<String>,
        title: impl Into<String>,
        configure: impl FnOnce(&mut CaseBuilder),
    ) -> &mut Self {
        let mut case = CaseBuilder {
            id: id.into(),
            title: title.into(),
            steps: Vec::new(),
        };
        configure(&mut case);
        self.cases.push(Case {
            id: case.id,
            title: case.title,
            steps: case.steps,
        });
        self
    }
}

/// Builder for steps belonging to a nested [`Case`].
#[derive(Clone, Debug)]
pub struct CaseBuilder {
    id: String,
    title: String,
    steps: Vec<Step>,
}

impl CaseBuilder {
    /// Appends a step to this case.
    pub fn step(&mut self, id: impl Into<String>, title: impl Into<String>) -> &mut Self {
        self.steps.push(Step {
            id: id.into(),
            title: title.into(),
        });
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_valid_multi_stage_topology_and_rejects_invalid_dependencies() {
        let definition = PipelineBuilder::new("CI", "End-to-end checks")
            .stage("Setup", |stage| {
                stage.job("prepare", "Prepare", |job| {
                    job.step("boot", "Boot server");
                });
            })
            .stage("Tests", |stage| {
                stage.test_job("ui", "UI Suite", |job| {
                    job.depends_on("prepare")
                        .case("login", "Sign in", |case| {
                            case.step("fill", "Fill form").step("submit", "Submit");
                        })
                        .case("logout", "Sign out", |_| {});
                });
            })
            .stage("Teardown", |stage| {
                stage.job("cleanup", "Clean up", |job| {
                    job.depends_on_all(["ui"]).step("stop", "Stop server");
                });
            })
            .build()
            .expect("valid definition");

        assert_eq!(definition.columns.len(), 3);
        assert_eq!(definition.jobs().count(), 3);
        assert_eq!(definition.test_case_count(), 2);

        let invalid = PipelineBuilder::new("Broken", "Missing dependency")
            .stage("Tests", |stage| {
                stage.test_job("ui", "UI", |job| {
                    job.depends_on("nonexistent");
                });
            })
            .build();
        assert!(invalid.is_err());
    }
}
