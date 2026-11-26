// SPDX-License-Identifier: AGPL-3.0-only

//! Send+Sync error types and failure classification helpers for pipeline reporters.

use std::{error::Error, fmt, io};

use kat0_pipeline_protocol::model::FailureKind;

/// Thread-safe boxed error type used across async reporter APIs.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// Convenience result alias returning [`BoxError`].
pub type RunResult<T = ()> = Result<T, BoxError>;

/// Typed failure wrapper allowing callers to classify errors into protocol [`FailureKind`]
/// without leaking sensitive details into the NDJSON stream or HTML report.
#[derive(Debug)]
pub struct TypedFailure {
    /// Protocol failure classification.
    pub kind: FailureKind,
    source: BoxError,
}

impl TypedFailure {
    /// Wraps `source` with an explicit [`FailureKind`].
    pub fn new(kind: FailureKind, source: impl Into<BoxError>) -> Self {
        Self {
            kind,
            source: source.into(),
        }
    }
}

impl fmt::Display for TypedFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self.kind {
            FailureKind::Startup => "Startup failure",
            FailureKind::Assertion => "Assertion failed",
            FailureKind::Timeout => "Operation timed out",
            FailureKind::Execution | _ => "Execution failure",
        };
        write!(formatter, "{label}: {}", self.source)
    }
}

impl Error for TypedFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Error enriched with the pipeline job, case, and step location where it occurred.
#[derive(Debug)]
pub struct TaskFailure {
    /// Job identifier where the failure occurred.
    pub job: String,
    /// Optional case identifier where the failure occurred.
    pub case: Option<String>,
    /// Optional step identifier where the failure occurred.
    pub step: Option<String>,
    /// Classified protocol failure kind.
    pub kind: FailureKind,
    source: BoxError,
}

impl fmt::Display for TaskFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Job {:?}", self.job)?;
        if let Some(case) = &self.case {
            write!(formatter, ", case {case:?}")?;
        }
        if let Some(step) = &self.step {
            write!(formatter, ", step {step:?}")?;
        }
        write!(formatter, ": {}", self.source)
    }
}

impl Error for TaskFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Error that preserves two independent failures (such as a failed test and a failed cleanup).
#[derive(Debug)]
pub struct CombinedError {
    first: BoxError,
    second: BoxError,
}

impl CombinedError {
    /// Combines two independent errors without discarding either cause.
    pub fn new(first: BoxError, second: BoxError) -> Self {
        Self { first, second }
    }

    /// Returns the primary (first) error.
    pub fn first(&self) -> &(dyn Error + Send + Sync + 'static) {
        self.first.as_ref()
    }

    /// Returns the secondary error.
    pub fn second(&self) -> &(dyn Error + Send + Sync + 'static) {
        self.second.as_ref()
    }
}

impl fmt::Display for CombinedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}\nAlso: {}", self.first, self.second)
    }
}

impl Error for CombinedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.first.as_ref())
    }
}

/// Infers the protocol [`FailureKind`] from a boxed or typed error.
pub fn classify_error(error: &(dyn Error + 'static)) -> FailureKind {
    if let Some(typed) = error.downcast_ref::<TypedFailure>() {
        return typed.kind;
    }
    if let Some(task) = error.downcast_ref::<TaskFailure>() {
        return task.kind;
    }
    if let Some(io_err) = error.downcast_ref::<io::Error>()
        && io_err.kind() == io::ErrorKind::TimedOut
    {
        return FailureKind::Timeout;
    }
    FailureKind::Execution
}

pub(crate) fn wrap_failure(
    job: &str,
    case: Option<&str>,
    step: Option<&str>,
    source: BoxError,
) -> BoxError {
    if let Some(existing) = source.downcast_ref::<TaskFailure>()
        && existing.job == job
        && (step.is_none() && (case.is_none() || existing.case.as_deref() == case))
    {
        return source;
    }
    let kind = classify_error(source.as_ref());
    TaskFailure {
        job: job.to_owned(),
        case: case.map(str::to_owned),
        step: step.map(str::to_owned),
        kind,
        source,
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_sync<T: Send + Sync + 'static>() {}

    #[test]
    fn error_types_are_send_sync_and_classify_timeouts() {
        assert_send_sync::<TypedFailure>();
        assert_send_sync::<TaskFailure>();
        assert_send_sync::<CombinedError>();

        let timeout_err = io::Error::new(io::ErrorKind::TimedOut, "timed out");
        assert_eq!(classify_error(&timeout_err), FailureKind::Timeout);

        let startup = TypedFailure::new(FailureKind::Startup, timeout_err);
        assert_eq!(classify_error(&startup), FailureKind::Startup);
    }
}
