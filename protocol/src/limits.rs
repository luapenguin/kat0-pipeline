// SPDX-License-Identifier: AGPL-3.0-only

//! Size limits enforced across protocol versions to keep wire payloads bounded.

/// Maximum UTF-8 byte length for any job, case, or step identifier.
pub const MAX_ID_BYTES: usize = 64;
/// Maximum UTF-8 byte length for any pipeline, column, job, case, or step title.
pub const MAX_TITLE_BYTES: usize = 256;
/// Maximum UTF-8 byte length for a pipeline description.
pub const MAX_DESCRIPTION_BYTES: usize = 512;
/// Maximum byte size accepted for a v1 external smoke result document.
pub const MAX_V1_BYTES: usize = 64 * 1024;
/// Maximum byte size accepted for an encoded v2 snapshot document.
pub const MAX_SNAPSHOT_BYTES: usize = 512 * 1024;
/// Maximum byte size accepted for a standard NDJSON v3 event line.
pub const MAX_EVENT_LINE_BYTES: usize = 16 * 1024;
/// Maximum byte size accepted for the initial `run_started` NDJSON v3 event line.
pub const MAX_RUN_STARTED_LINE_BYTES: usize = 128 * 1024;
