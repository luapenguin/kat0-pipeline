# `kat0-pipeline`

Live terminal and browser pipeline runner, NDJSON v3 event protocol, and standalone offline HTML report generator in Rust.

## Workspace layout

This workspace contains seven crates sharing a single `Cargo.lock` and producing a single CLI binary (`kat0-pipeline`):

- [`protocol`](protocol/src/lib.rs) (`kat0-pipeline-protocol`): pure `serde` library with zero I/O or async runtime dependencies. Defines the hierarchical `Job -> Case -> Step` model (`Definition`, `Snapshot`, `Status`, `FailureKind`), `PipelineBuilder`, size limits, v2 snapshot codec, and NDJSON v3 event/reducer state machine.
- [`reporter`](reporter/src/lib.rs) (`kat0-pipeline-reporter`): async producer library (Tokio + Unix socket IPC) with zero `axum`, `leptos`, or `playwright-rs` dependencies. Provides clonable `Send + Sync` `Reporter`, `TickGuard`, `TypedFailure`, `TaskFailure`, `SnapshotFile`, and execution combinators (`execute`).
- [`playwright`](playwright/src/lib.rs) (`kat0-pipeline-playwright`): optional adapter crate isolating `playwright-rs` error classification (`classify_error`, `assertion_error`, `startup_error`) and safe browser/context lifecycle helpers (`FirefoxSession`, `with_context`, `with_page`).
- [`server`](server/src/lib.rs) (`kat0-pipeline`): host-only CLI binary with embedded WASM/JS/CSS panel assets (no `leptos` or `playwright-rs` dependency, no runtime Cargo/WASM build required).
- [`client`](client/src/lib.rs) (`kat0-pipeline-client`, `publish = false`): Leptos CSR WASM panel rendering live SSE (`/events`, `/snapshot` resync) and standalone offline reports.
- [`xtask`](xtask/src/main.rs) (`publish = false`): reproducible build and SHA-256 verification of the embedded panel assets under [`server/assets/`](server/assets/).
- [`e2e`](e2e/README.md) (`kat0-pipeline-e2e`, `publish = false`): Firefox browser verification suite for the offline report, live SSE viewer, and orchestrated `run` command.

See also: [Wire Protocol Specification](docs/PROTOCOL.md), [Changelog](CHANGELOG.md), and [Security Policy](SECURITY.md).

## Installing the CLI

Prebuilt binaries for Linux (`x86_64`, `aarch64`), macOS (`aarch64`, `x86_64`), and Windows (`x86_64`) are published on [GitHub Releases](https://github.com/luapenguin/kat0-pipeline/releases) alongside `SHA256SUMS.txt` and `LICENSE`.

From source:

```sh
cargo install --path server --locked
kat0-pipeline --version
kat0-pipeline --help
```

*(Once published to `crates.io`: `cargo install kat0-pipeline --locked` or `cargo binstall kat0-pipeline`.)*

## Usage

### Orchestrate a live run (`run`)

```sh
# Live terminal presenter + standalone HTML report
kat0-pipeline run --ui terminal -- <command> [args...]

# Live browser viewer (loopback Axum + SSE) + standalone HTML report
kat0-pipeline run --ui browser -- <command> [args...]

# Both presenters simultaneously without auto-opening a browser
kat0-pipeline run --ui both --no-open -- <command> [args...]
```

Options for `run`:

- `--ui terminal|browser|both` (default: `terminal`)
- `--no-html`: writes `snapshot.json` and skips generating `index.html`
- `--no-open`: suppresses automatic OS browser launch (`xdg-open` / `open` / `cmd /C start`; also honored via `PIPELINE_NO_OPEN=1`)
- `--report-dir <DIR>`: root output directory for `run-*/` (default: `PIPELINE_REPORT_ROOT`, `./target/pipeline`, or `<temp>/kat0-pipeline`)
- `--grace-ms <MS>`: loopback server grace period after completion (`0..=60000`, default `2000`)

The orchestrator binds a private `0700` Unix domain socket (on Unix platforms) and passes its path to the child process via `KAT0_PIPELINE_EVENTS_SOCK`. Child `stdout`/`stderr` stay untouched for human diagnostics.

### Generate a standalone sample report (`sample`)

```sh
kat0-pipeline sample
```

### Import a finished v1 or v2 JSON file (`import`)

```sh
kat0-pipeline import path/to/snapshot.json
```

### Serve a v2 snapshot file with live SSE (`serve`)

```sh
kat0-pipeline serve path/to/snapshot.json
```

### Quickstart with `playwright-rs`

Run the bundled self-contained Firefox demo (requires Playwright Firefox installed via `cargo run --locked -p kat0-pipeline-e2e --example install-browsers -- firefox`):

```sh
kat0-pipeline run --ui browser -- cargo run --locked -p kat0-pipeline-playwright --example playwright_demo
```

## Rebuilding and verifying embedded panel assets (`xtask`)

Whenever [`client/`](client/) or [`client/report.css`](client/report.css) changes, rebuild and verify the embedded assets using `cargo-leptos 0.3.12`:

```sh
cargo run --locked -p xtask -- build-panel
cargo run --locked -p xtask -- verify-panel
```

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --locked -p kat0-pipeline-protocol -p kat0-pipeline-reporter -p kat0-pipeline-playwright -p kat0-pipeline -p xtask -p kat0-pipeline-e2e --all-targets -- -D warnings
cargo clippy --locked -p kat0-pipeline-client --lib --target wasm32-unknown-unknown -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked -p kat0-pipeline-protocol -p kat0-pipeline-reporter -p kat0-pipeline-playwright --no-deps
cargo test --locked -p kat0-pipeline-protocol -p kat0-pipeline-reporter -p kat0-pipeline-playwright -p kat0-pipeline -p xtask -p kat0-pipeline-e2e
cargo run --locked -p xtask -- verify-panel
```

For the full Playwright Firefox E2E suite, see [`e2e/README.md`](e2e/README.md).

## License

Copyright (C) 2026 Rios

This project is licensed under the GNU Affero General Public License v3.0 only (`AGPL-3.0-only`). See [LICENSE](LICENSE) for the full license text.
