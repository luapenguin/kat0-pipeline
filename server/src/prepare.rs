// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf, absolute},
};

#[derive(Debug)]
pub struct ReserveError {
    root: PathBuf,
    source: io::Error,
}

impl fmt::Display for ReserveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Failed to reserve run directory under {}: {}",
            self.root.display(),
            self.source
        )
    }
}

impl Error for ReserveError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Reserves a unique output path after the caller has validated the input.
/// The directory is retained, but the report file does not yet exist.
pub async fn reserve_report(output_root: &Path) -> Result<PathBuf, ReserveError> {
    let root = output_root.to_owned();
    tokio::task::spawn_blocking(move || {
        let root = absolute(root)?;
        fs::create_dir_all(&root)?;
        let directory = tempfile::Builder::new().prefix("run-").tempdir_in(root)?;
        Ok(directory.keep().join("index.html"))
    })
    .await
    .map_err(io::Error::other)
    .and_then(|result| result)
    .map_err(|source| ReserveError {
        root: output_root.to_owned(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn concurrent_runs_reserve_separate_report_paths() {
        let scratch = tempfile::tempdir().unwrap();
        let root = scratch.path().join("reports");
        let (first, second) = tokio::join!(reserve_report(&root), reserve_report(&root));
        let first = first.unwrap();
        let second = second.unwrap();
        assert_ne!(first.parent(), second.parent());
        for path in [&first, &second] {
            let directory = path.parent().unwrap();
            assert!(directory.is_dir());
            assert_eq!(directory.parent(), Some(root.as_path()));
            assert!(!path.exists());
        }
    }

    #[tokio::test]
    async fn output_error_preserves_an_existing_file() {
        let scratch = tempfile::tempdir().unwrap();
        let root = scratch.path().join("not-a-directory");
        tokio::fs::write(&root, b"existing file").await.unwrap();
        assert!(reserve_report(&root).await.is_err());
        assert_eq!(tokio::fs::read(&root).await.unwrap(), b"existing file");
    }
}
