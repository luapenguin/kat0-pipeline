# Security Policy

## Supported Versions

Security fixes are provided for the latest release on the active minor series:

| Version | Supported |
| --- | --- |
| `0.1.x` | Yes |

## Reporting a Vulnerability

Please **do not** report security vulnerabilities through public GitHub issues.

Instead, open a private report via **GitHub Security Advisories** in this repository (`Security` → `Report a vulnerability` at <https://github.com/luapenguin/kat0-pipeline/security/advisories/new>).

When reporting an issue, please include:
- The affected crate(s) and version (`kat0-pipeline`, `kat0-pipeline-protocol`, `kat0-pipeline-reporter`, or `kat0-pipeline-playwright`).
- Minimal steps to reproduce (or a sample NDJSON v3 / JSON v2 input).
- Expected vs. observed behavior and estimated impact.

You can expect an initial acknowledgement within 5 business days. Once confirmed, a fix and release notes will be coordinated before public disclosure.

## Security Model & Hardening Invariants

`kat0-pipeline` is designed for local development and CI environments and enforces the following invariants across its crates:

- **Memory safety:** Every workspace crate enforces `#![forbid(unsafe_code)]`.
- **Private IPC channel:** `kat0-pipeline run` binds a Unix domain socket inside a temporary directory with `0700` permissions (`KAT0_PIPELINE_EVENTS_SOCK`) and cleans it up on shutdown.
- **Loopback-only HTTP/SSE:** The live browser viewer binds exclusively to `127.0.0.1` on an ephemeral port, sets `Cache-Control: no-store`, caps concurrent SSE subscribers (`MAX_CLIENTS`), and terminates streams cleanly on completion or shutdown (`204 No Content`).
- **Bounded wire payloads:** All v1, v2, and NDJSON v3 inputs enforce strict size limits (`protocol/src/limits.rs`) and bounded line reads to prevent unbounded allocations.
- **Safe HTML embedding:** Snapshot JSON embedded inside `<script id="run-data" type="application/json">` escapes `<` to `\u003c` so untrusted strings cannot break out of the script element.
- **Atomic report publication:** Offline HTML reports and `snapshot.json` files are staged in the destination directory, flushed, and published with `persist_noclobber` so existing files are never overwritten or corrupted.
- **Offline self-contained reports:** Generated HTML reports embed all CSS, JS, and WASM as `data:` URLs and make zero external network requests.
