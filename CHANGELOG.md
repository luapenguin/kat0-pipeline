# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-26

### Added

- Initial standalone workspace release of `kat0-pipeline` under `AGPL-3.0-only`:
  - `kat0-pipeline-protocol`: I/O-free pipeline model (`Job -> Case -> Step`), `PipelineBuilder`, v2 snapshot codec with validated `Snapshot::from_parts(...)`, `O(1)` NDJSON v3 event reducer, and structural DoS quotas (`MAX_COLUMNS`, `MAX_JOBS`, `MAX_CASES_PER_JOB`, `MAX_STEPS_PER_CONTAINER`, `MAX_TOTAL_CASES`, `MAX_TOTAL_STEPS`, `MAX_RECORDED_FAILURES`).
  - `kat0-pipeline-reporter`: async clonable `Send + Sync` NDJSON v3 reporter over local IPC (Unix domain sockets and local-only Windows Named Pipes) with a decoupled background wire writer, RAII cancellation safety for `job`/`case`/`step`, atomic snapshot persistence, and execution combinators.
  - `kat0-pipeline-playwright`: optional `playwright-rs` error classification and Firefox/context/page lifecycle adapter.
  - `kat0-pipeline`: standalone binary-only CLI (`run`, `sample`, `import`, `serve`) with live terminal and browser SSE presenters (pre-serialized `PublishedSnapshot` broadcast and shared file poller), hardened loopback HTTP/IPC security (strict `Host` validation, `Sec-Fetch-Site` check, CSP/security headers, Unix `peer_cred()` UID checks, `MAX_IPC_CONNECTIONS = 32`, strict loopback URL validation for browser launch), and embedded offline WASM report panel.
  - `xtask`: reproducible embedded panel asset build, SHA-256 verification, shared NDJSON v3 fixture parity checks, and local pre-push CI runner.
- Workspace-wide `[workspace.lints]` policy (`unsafe_code = "forbid"`, `clippy::{dbg_macro, todo, unimplemented} = "deny"`), `deny.toml` supply-chain and license auditing, and automatic GitHub Actions CI (Linux, Windows Named Pipes, MSRV `1.88`/`1.95`, panel reproducibility, Playwright Firefox E2E).
- Automated multi-platform GitHub Releases (Linux `x86_64`/`aarch64`, macOS `aarch64`/`x86_64`, Windows `x86_64`) with `LICENSE`, `SHA256SUMS.txt`, and `cargo-binstall` metadata.
