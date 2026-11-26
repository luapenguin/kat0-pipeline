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

- **Memory safety:** Every workspace crate enforces `#![forbid(unsafe_code)]` (also enforced workspace-wide via `[workspace.lints]`).
- **Private local IPC channel:** `kat0-pipeline run` binds a private `0700` Unix domain socket with peer UID verification (`peer_cred()`) on Unix, or a local-only Named Pipe (`first_pipe_instance(true)`, `reject_remote_clients(true)`) on Windows, passes the endpoint via `KAT0_PIPELINE_EVENTS_SOCK`, caps concurrent IPC clients (`MAX_IPC_CONNECTIONS = 32`), closes misbehaving/oversized streams immediately, and cleans up on shutdown.
- **Hardened loopback-only HTTP/SSE:** The live browser viewer binds exclusively to `127.0.0.1` on an ephemeral port, enforces strict `Host` header validation against the bound address (mitigating DNS rebinding), rejects `Sec-Fetch-Site: cross-site` reads on `/events` and `/snapshot`, attaches security headers (`Content-Security-Policy`, `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, `Cache-Control: no-store`), caps concurrent SSE subscribers (`MAX_CLIENTS`), and terminates streams cleanly on completion or shutdown (`204 No Content`).
- **Bounded wire payloads and structural quotas:** All v1, v2, and NDJSON v3 inputs enforce strict payload size limits, structural quotas (`MAX_COLUMNS`, `MAX_JOBS`, `MAX_CASES_PER_JOB`, `MAX_STEPS_PER_CONTAINER`, `MAX_TOTAL_CASES`, `MAX_TOTAL_STEPS`, `MAX_RECORDED_FAILURES` in `protocol/src/limits.rs`), and bounded line reads to prevent unbounded allocations.
- **Safe HTML embedding & browser launch:** Snapshot JSON embedded inside `<script id="run-data" type="application/json">` escapes `<` to `\u003c` so untrusted strings cannot break out of the script element, and automatic browser launching validates a strict `http://127.0.0.1:<port>/` format before invoking OS openers.
- **Atomic report publication:** Offline HTML reports and `snapshot.json` files are staged in the destination directory, flushed, and published with `persist_noclobber` so existing files are never overwritten or corrupted.
- **Offline self-contained reports:** Generated HTML reports embed all CSS, JS, and WASM as `data:` URLs and make zero external network requests.
