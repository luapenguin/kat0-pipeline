// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    error::Error,
    fmt, io,
    path::{Path, PathBuf},
};

use tokio::fs;

use crate::RunResult;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureKind {
    Startup,
    Assertion,
    Timeout,
    Execution,
    ReportWrite,
    ReportMissing,
    ReportCheck,
}

impl FailureKind {
    fn label(self) -> &'static str {
        match self {
            Self::Startup => "Startup failure",
            Self::Assertion => "Assertion failed",
            Self::Timeout => "Operation timed out",
            Self::Execution => "Execution failure",
            Self::ReportWrite => "Failed to save report",
            Self::ReportMissing => "Expected report is missing",
            Self::ReportCheck => "Failed to verify report",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum FailureSite {
    Run,
    Task { job: String, step: Option<String> },
    Report(PathBuf),
}

#[derive(Debug)]
pub struct Failure {
    pub kind: FailureKind,
    pub site: FailureSite,
    pub cause: Box<dyn Error + Send + Sync>,
}

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: ", self.kind.label())?;
        match &self.site {
            FailureSite::Run => (),
            FailureSite::Task { job, step } => {
                write!(formatter, "Job {job:?}")?;
                if let Some(step) = step {
                    write!(formatter, ", step {step:?}")?;
                }
                formatter.write_str(": ")?;
            }
            FailureSite::Report(path) => write!(formatter, "{}: ", path.display())?,
        }
        fmt::Display::fmt(&self.cause, formatter)
    }
}

impl Error for Failure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

#[derive(Debug)]
pub struct ValidationError(Vec<Failure>);

impl ValidationError {
    /// Imported failures in definition order, followed by any report failure.
    pub fn failures(&self) -> &[Failure] {
        &self.0
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, failure) in self.failures().iter().enumerate() {
            if index != 0 {
                formatter.write_str("\nAlso: ")?;
            }
            write!(formatter, "{failure}")?;
        }
        Ok(())
    }
}

impl Error for ValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.failures().first().map(|failure| failure as &dyn Error)
    }
}

#[must_use = "check the validation result before reporting success"]
#[derive(Debug)]
pub struct Validation<'a> {
    pub report_path: Option<&'a Path>,
    pub result: Result<(), ValidationError>,
}

/// Checks the output without discarding independently recorded failures.
pub async fn run<'a>(
    mut failures: Vec<Failure>,
    path: &'a Path,
    saved: RunResult,
) -> Validation<'a> {
    let report_path = match check_report(path, saved).await {
        Ok(()) => Some(path),
        Err(failure) => {
            failures.push(failure);
            None
        }
    };
    let result = if failures.is_empty() {
        Ok(())
    } else {
        Err(ValidationError(failures))
    };
    Validation {
        report_path,
        result,
    }
}

async fn check_report(path: &Path, saved: RunResult) -> Result<(), Failure> {
    let (kind, cause) = match saved {
        Err(cause) => (FailureKind::ReportWrite, cause),
        Ok(()) => match fs::metadata(path).await {
            Ok(metadata) if metadata.is_file() && metadata.len() > 0 => return Ok(()),
            Ok(_) => (
                FailureKind::ReportCheck,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Expected a non-empty regular file",
                )
                .into(),
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                (FailureKind::ReportMissing, error.into())
            }
            Err(error) => (FailureKind::ReportCheck, error.into()),
        },
    };
    Err(Failure {
        kind,
        site: FailureSite::Report(path.to_owned()),
        cause,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refuses_missing_empty_and_nonregular_reports() {
        let scratch = tempfile::tempdir().unwrap();
        let empty = scratch.path().join("empty.html");
        fs::write(&empty, b"").await.unwrap();
        for (path, kind) in [
            (
                scratch.path().join("missing.html"),
                FailureKind::ReportMissing,
            ),
            (empty, FailureKind::ReportCheck),
            (scratch.path().to_owned(), FailureKind::ReportCheck),
        ] {
            let validation = run(Vec::new(), &path, Ok(())).await;
            assert!(validation.report_path.is_none());
            assert_eq!(validation.result.unwrap_err().failures()[0].kind, kind);
        }
    }
}
