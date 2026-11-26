# `kat0-pipeline-e2e`

End-to-end browser verification suite for the `kat0-pipeline` CLI, loopback Axum/SSE server, and embedded WASM report panel using `playwright-rs` (Firefox).

## Prerequisites

1. Build the `kat0-pipeline` binary:
   ```sh
   cargo build --locked -p kat0-pipeline --bin kat0-pipeline
   ```
   *(Or point `PIPELINE_REPORT_BIN` to an installed binary.)*
2. Install the Playwright Firefox browser matching `Cargo.lock`:
   ```sh
   cargo run --locked -p kat0-pipeline-e2e --example install-browsers -- firefox
   ```
   On Linux without system browser libraries, pass `--with-deps`:
   ```sh
   cargo run --locked -p kat0-pipeline-e2e --example install-browsers -- firefox --with-deps
   ```

## Running the suite

```sh
# Fast unit checks (no browser required)
cargo test --locked -p kat0-pipeline-e2e

# Full Firefox E2E suite (offline report, live SSE, orchestrated run)
cargo test --locked -p kat0-pipeline-e2e --test pipeline_viewer -- --ignored
```
