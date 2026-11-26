// SPDX-License-Identifier: AGPL-3.0-only

use std::path::Path;

use kat0_pipeline_protocol::{
    limits::MAX_SNAPSHOT_BYTES,
    model::Snapshot,
    v2::{self, check_size},
};
use tokio::{fs, io::AsyncReadExt as _};

use crate::RunResult;

pub(crate) async fn read_bytes(path: &Path, max_bytes: usize) -> RunResult<Vec<u8>> {
    let metadata = fs::metadata(path).await?;
    if !metadata.is_file() {
        return Err("El resultado debe ser un archivo regular".into());
    }
    check_size(metadata.len(), max_bytes)?;
    let mut bytes = Vec::new();
    fs::File::open(path)
        .await?
        .take((max_bytes + 1) as u64)
        .read_to_end(&mut bytes)
        .await?;
    check_size(bytes.len() as u64, max_bytes)?;
    Ok(bytes)
}

pub async fn read(path: &Path) -> RunResult<Snapshot> {
    decode(&read_bytes(path, MAX_SNAPSHOT_BYTES).await?)
}

pub fn decode(bytes: &[u8]) -> RunResult<Snapshot> {
    Ok(v2::decode(bytes)?)
}

pub(crate) fn version(bytes: &[u8]) -> RunResult<u32> {
    Ok(v2::schema_version(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kat0_pipeline_protocol::model::Status;

    const SAMPLE: &[u8] = include_bytes!("../fixtures/web_regression.json");

    #[test]
    fn preserves_definition_states_and_clock() {
        let sample = decode(SAMPLE).unwrap();
        assert_eq!(sample.definition().columns.len(), 5);
        assert_eq!(sample.status, Status::Running);
        assert_eq!(sample.elapsed_ms, 48_300);
        assert_eq!(sample.job_status("change-role"), Some(Status::Failed));
    }
}
