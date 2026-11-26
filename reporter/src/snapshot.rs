// SPDX-License-Identifier: AGPL-3.0-only

//! Atomic v2 `snapshot.json` persistence for standalone runs.

use std::{
    fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
};

use kat0_pipeline_protocol::{model::Snapshot, v2};

use crate::error::RunResult;

/// Reserved unique `run-*/snapshot.json` file handle with atomic writes.
pub struct SnapshotFile(PathBuf);

impl SnapshotFile {
    /// Reserves a unique `run-*/snapshot.json` output path under `root`.
    pub fn reserve(root: &Path) -> RunResult<Self> {
        let result = fs::create_dir_all(root)
            .and_then(|()| tempfile::Builder::new().prefix("run-").tempdir_in(root));
        let directory = result.map_err(|error| {
            format!(
                "Failed to reserve run directory under {}: {error}",
                root.display()
            )
        })?;
        Ok(Self(directory.keep().join("snapshot.json")))
    }

    /// Returns the reserved `snapshot.json` path.
    pub fn path(&self) -> &Path {
        &self.0
    }

    /// Atomically encodes and writes `snapshot` as a v2 JSON document.
    pub async fn write(&self, snapshot: Snapshot) -> RunResult {
        let path = self.0.clone();
        let result = tokio::task::spawn_blocking(move || publish(&path, &snapshot))
            .await
            .map_err(io::Error::other)
            .and_then(|result| result);
        result.map_err(|error| {
            format!(
                "Failed to publish snapshot {}: {error}",
                self.path().display()
            )
            .into()
        })
    }
}

fn publish(path: &Path, snapshot: &Snapshot) -> io::Result<()> {
    let bytes = v2::encode(snapshot).map_err(io::Error::other)?;
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("Snapshot path requires a parent directory"))?;
    let mut staged = tempfile::NamedTempFile::new_in(parent)?;
    staged.write_all(&bytes)?;
    if snapshot.status.is_terminal() {
        staged.as_file().sync_all()?;
    }
    staged.persist(path).map_err(|error| error.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use kat0_pipeline_protocol::{builder::PipelineBuilder, model::Status};

    use super::*;

    #[tokio::test]
    async fn reserves_unique_directories_and_writes_valid_v2_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let first = SnapshotFile::reserve(root.path()).unwrap();
        let second = SnapshotFile::reserve(root.path()).unwrap();
        assert_ne!(first.path(), second.path());

        let definition = PipelineBuilder::new("Standalone", "Snapshot persistence test")
            .stage("Verify", |stage| {
                stage.test_job("check", "Check", |_| {});
            })
            .build()
            .unwrap();
        let snapshot = Snapshot::new(definition).unwrap();
        first.write(snapshot.clone()).await.unwrap();

        let decoded = v2::decode(&tokio::fs::read(first.path()).await.unwrap()).unwrap();
        assert_eq!(decoded.status, Status::Running);
        assert_eq!(decoded, snapshot);
    }
}
