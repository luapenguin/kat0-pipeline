# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Workspace-wide `[workspace.lints]` policy (`unsafe_code = "forbid"`, `clippy::{dbg_macro, todo, unimplemented} = "deny"`) inherited across all 7 workspace crates.
- Structural DoS quotas in `kat0-pipeline-protocol` (`MAX_COLUMNS`, `MAX_JOBS`, `MAX_CASES_PER_JOB`, `MAX_STEPS_PER_CONTAINER`, `MAX_TOTAL_CASES`, `MAX_TOTAL_STEPS`, `MAX_RECORDED_FAILURES`) enforced in `Definition::validate()` and `RunState::apply()`.
- `Snapshot::from_parts(...)` validated constructor preserving structural and timing invariants across crate boundaries.
- Full Windows IPC support using native Tokio local-only Named Pipes (`first_pipe_instance(true)`, `reject_remote_clients(true)`) in both `kat0-pipeline` (`server`) and `kat0-pipeline-reporter`, verified by a dedicated `windows-unit` CI job.
- Supply-chain and license audit configuration (`deny.toml`) and automatic mixed-cost GitHub CI (`push`/`pull_request` on `main` and `development`).

### Changed

- **Security hardening:**
  - `kat0-pipeline` loopback HTTP/SSE server now enforces strict `Host` header validation (DNS rebinding protection), blocks `Sec-Fetch-Site: cross-site` reads on `/events` and `/snapshot`, and attaches `Content-Security-Policy`, `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, and `Cache-Control: no-store` on every response.
  - `EventSocket` enforces `MAX_IPC_CONNECTIONS = 32`, reaps finished reader tasks continuously, drains queued backlog connections on shutdown, verifies peer UID via `peer_cred()` on Unix, and closes misbehaving/oversized streams immediately.
  - `open_in_default_browser` validates strict `http://127.0.0.1:<nonzero-port>/` URLs before invoking OS openers.
- **Scalability & concurrency:**
  - `RunState` uses `O(1)` internal lookup indices instead of repeated linear scans.
  - `Reporter` decouples IPC socket writes onto a dedicated background writer task fed by a bounded channel (zero I/O under `Mutex`) and adds RAII `ScopeGuard` cancellation safety for `job`, `case`, and `step`.
  - Live HTTP/SSE pre-serializes each `Snapshot` once (`PublishedSnapshot`) across all clients, and `serve <file>` shares a single background file poller across all SSE subscribers.
  - WASM client `job_details` tracks zero-allocation structural counts with `O(1)` job lookup instead of cloning `Job` on every `Tick`.
- **API & codebase hygiene:**
  - Converted `kat0-pipeline` (`server`) to a binary-only crate (removed accidental `server/src/lib.rs` public surface) and restricted `Snapshot::jobs_mut` to `pub(crate)`.
  - Standardized all error and diagnostic messages across the entire workspace to English.
  - Added automated parity verification for shared NDJSON v3 fixtures in `xtask` and CI, flattened single-file component directories in `client/`, and aligned `base64` to `0.23` across host crates.

## [0.1.0] - 2026-11-26

### Added

- Initial standalone workspace release of `kat0-pipeline` under `AGPL-3.0-only`:
  - `kat0-pipeline-protocol`: I/O-free pipeline model (`Job -> Case -> Step`), `PipelineBuilder`, v2 snapshot codec, and NDJSON v3 event reducer.
  - `kat0-pipeline-reporter`: async clonable `Send + Sync` NDJSON v3 reporter over Unix domain sockets with atomic snapshot persistence and execution combinators.
  - `kat0-pipeline-playwright`: optional `playwright-rs` error classification and Firefox/context/page lifecycle adapter.
  - `kat0-pipeline`: standalone CLI binary (`run`, `sample`, `import`, `serve`) with live terminal/browser SSE presenters and embedded offline WASM report panel.
  - `xtask`: reproducible embedded panel asset build and SHA-256 verification.
- Automated multi-platform GitHub Releases (Linux `x86_64`/`aarch64`, macOS `aarch64`/`x86_64`, Windows `x86_64`) with `LICENSE`, `SHA256SUMS.txt`, and `cargo-binstall` metadata.
