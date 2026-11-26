# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-26

### Added

- Initial standalone workspace release of `kat0-pipeline` under `AGPL-3.0-only`:
  - `kat0-pipeline-protocol`: I/O-free pipeline model (`Job -> Case -> Step`), `PipelineBuilder`, v2 snapshot codec, and NDJSON v3 event reducer.
  - `kat0-pipeline-reporter`: async clonable `Send + Sync` NDJSON v3 reporter over Unix domain sockets with atomic snapshot persistence and execution combinators.
  - `kat0-pipeline-playwright`: optional `playwright-rs` error classification and Firefox/context/page lifecycle adapter.
  - `kat0-pipeline`: standalone CLI binary (`run`, `sample`, `import`, `serve`) with live terminal/browser SSE presenters and embedded offline WASM report panel.
  - `xtask`: reproducible embedded panel asset build and SHA-256 verification.
- Automated multi-platform GitHub Releases (Linux `x86_64`/`aarch64`, macOS `aarch64`/`x86_64`, Windows `x86_64`) with `LICENSE`, `SHA256SUMS.txt`, and `cargo-binstall` metadata.
