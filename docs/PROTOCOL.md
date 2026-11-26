# `kat0-pipeline` Wire Protocol Specification

This document specifies the wire formats accepted and emitted by `kat0-pipeline` (`kat0-pipeline-protocol`):

- **NDJSON v3** (`schema_version = 3`): live streaming event protocol over IPC (`kat0-pipeline run`).
- **Snapshot JSON v2** (`schema_version = 2`): complete validated pipeline snapshot (`snapshot.json`, `kat0-pipeline import`, `kat0-pipeline serve`, and `GET /snapshot`).
- **External Smoke JSON v1** (`schema_version = 1`): legacy single-job finished report accepted by `kat0-pipeline import`.

Any producer — in Rust (via `kat0-pipeline-reporter`) or any other language — can integrate with `kat0-pipeline` by implementing the NDJSON v3 stream below.

---

## 1. Live IPC Transport (`KAT0_PIPELINE_EVENTS_SOCK`)

When `kat0-pipeline run` launches a child command on Unix platforms:

1. The orchestrator creates a private temporary directory with mode `0700` and binds a Unix domain stream socket (`events.sock`).
2. The socket path is passed to the child process in the environment variable:
   ```text
   KAT0_PIPELINE_EVENTS_SOCK
   ```
3. Child `stdout` and `stderr` are left untouched for human/CI diagnostics.
4. The producer connects to the Unix socket and writes **UTF-8, `\n`-delimited JSON objects** (one event per line; no embedded `\n` or `\r` inside a record).
5. Blank lines are ignored. Multiple concurrent connections are accepted on the same socket, though a single serialized connection (as used by `kat0-pipeline-reporter`) guarantees ordering.

---

## 2. Size & Validation Limits

All wire limits are enforced by `kat0-pipeline-protocol` (`protocol/src/limits.rs`):

| Constant | Limit | Applies to |
| --- | --- | --- |
| `MAX_ID_BYTES` | `64` bytes | `job_id`, `case_id`, `step_id` (ASCII `[A-Za-z0-9_-]`) |
| `MAX_TITLE_BYTES` | `256` bytes | Pipeline, column, job, case, and step titles (non-empty, no control characters) |
| `MAX_DESCRIPTION_BYTES` | `512` bytes | Pipeline description (no control characters) |
| `MAX_V1_BYTES` | `64 KiB` | v1 finished smoke JSON document |
| `MAX_SNAPSHOT_BYTES` | `512 KiB` | Complete v2 snapshot JSON document |
| `MAX_EVENT_LINE_BYTES` | `16 KiB` | Any NDJSON v3 event line except `run_started` |
| `MAX_RUN_STARTED_LINE_BYTES` | `128 KiB` | Initial `run_started` NDJSON v3 event line |
| `MAX_COLUMNS` | `64` | Stage columns per pipeline definition |
| `MAX_JOBS` | `256` | Total jobs across all columns |
| `MAX_CASES_PER_JOB` | `512` | Nested cases per job (static + dynamic) |
| `MAX_STEPS_PER_CONTAINER` | `256` | Direct steps per job or steps per case (static + dynamic) |
| `MAX_TOTAL_CASES` | `2,048` | Total nested cases across an entire run |
| `MAX_TOTAL_STEPS` | `8,192` | Total steps across an entire run |
| `MAX_RECORDED_FAILURES` | `1,024` | Typed failures retained in memory by `RunState` |

---

## 3. Domain Model

A pipeline `Definition` is a directed acyclic graph organized into ordered stage columns (`columns`), rendered left-to-right:

```text
Definition
 └── Column[] (stages, left-to-right)
      └── Job[] (id, title, is_test, dependencies[], steps[], cases[])
           ├── Step[] (direct steps on the job)
           └── Case[] (nested test cases: id, title, steps[])
                └── Step[] (steps inside a case)
```

- Every `Job.id` is unique across the entire `Definition`.
- Every `Job.dependencies` entry must reference a `Job.id` declared in a **strictly earlier** column (guaranteeing acyclicity).
- `is_test: true` includes the job in the **Jobs**, **Failed Jobs**, and **Tests** tabs.
- Unknown JSON fields are rejected (`deny_unknown_fields`).

### Statuses (`Status`)

| JSON value | Meaning |
| --- | --- |
| `"pending"` | Waiting to start |
| `"running"` | Currently executing |
| `"passed"` | Completed successfully |
| `"failed"` | Completed with failure |
| `"skipped"` | Not executed |

### Failure Classification (`FailureKind`)

Optional typed failure reason on failed cases or steps:

| JSON value | Meaning |
| --- | --- |
| `"startup"` | Environment setup, driver, or service launch failure |
| `"assertion"` | Test expectation or assertion failure |
| `"timeout"` | Deadline or wait timeout exceeded |
| `"execution"` | Other runtime or operational error |

---

## 4. NDJSON v3 Events (`schema_version = 3`)

Every line is a JSON object tagged by `"type"` (snake_case):

### 4.1 `run_started` (must be first)

```json
{
  "type": "run_started",
  "schema_version": 3,
  "workers": 1,
  "definition": {
    "title": "Web regression",
    "description": "End-to-end verification pipeline",
    "columns": [
      {
        "title": "Prepare",
        "jobs": [
          {
            "id": "setup",
            "title": "Test env",
            "is_test": false,
            "dependencies": [],
            "steps": [
              { "id": "boot", "title": "Start server" }
            ]
          }
        ]
      },
      {
        "title": "Test",
        "jobs": [
          {
            "id": "smoke",
            "title": "Smoke",
            "is_test": true,
            "dependencies": ["setup"],
            "steps": [],
            "cases": [
              { "id": "login", "title": "User login", "steps": [] }
            ]
          }
        ]
      }
    ]
  }
}
```

- `workers` is optional (`>= 1`).
- Must be received before any other event.

### 4.2 `job_started` / `job_finished`

```json
{"type":"job_started","job_id":"setup","at_ms":0}
{"type":"job_finished","job_id":"setup","status":"passed","at_ms":120}
```

- `status` in `job_finished`: `"passed" | "failed" | "skipped"`.

### 4.3 `case_started` / `case_finished`

```json
{"type":"case_started","job_id":"smoke","case_id":"login","title":"User login","at_ms":120}
{"type":"case_finished","job_id":"smoke","case_id":"login","status":"failed","at_ms":450,"kind":"assertion"}
```

- `title` is optional: if `case_id` was not predeclared in `definition`, providing `title` registers the case dynamically under `job_id`.
- `kind` is optional and only valid when `status` is `"failed"`.

### 4.4 `step_started` / `step_finished`

```json
{"type":"step_started","job_id":"smoke","case_id":"login","step_id":"submit","title":"Submit form","at_ms":150}
{"type":"step_finished","job_id":"smoke","case_id":"login","step_id":"submit","status":"passed","at_ms":300}
```

- Omit `case_id` for direct job steps; include `case_id` for steps belonging to a nested case.
- Providing `title` dynamically registers an undeclared step.
- `kind` is optional in `step_finished` and only valid when `status` is `"failed"`.

### 4.5 `tick`

```json
{"type":"tick","elapsed_ms":1000}
```

Advances the live elapsed clock without modifying task statuses (useful during long-running steps).

### 4.6 `run_finished` (must be last)

```json
{"type":"run_finished","status":"passed","elapsed_ms":450}
```

- `status`: `"passed" | "failed"`.
- No further events are accepted after `run_finished`.

### Ordering, Clock & Abort Invariants

- Timestamps (`at_ms`, `elapsed_ms`) are monotonic non-decreasing milliseconds since run start.
- Child spans (cases and steps) must fall within their parent's active time span and steps within the same container may not overlap.
- If the child process exits before sending `run_finished`, the orchestrator finalizes the snapshot as aborted: any `running` items become `failed`, any `pending` items become `skipped`, and both `snapshot.json` and `index.html` are still published.

---

## 5. Snapshot JSON v2 (`schema_version = 2`)

Persisted as `snapshot.json`, served at `GET /snapshot`, and accepted by `kat0-pipeline import` (finished runs only) and `kat0-pipeline serve`:

```json
{
  "schema_version": 2,
  "snapshot": {
    "definition": { "...": "..." },
    "status": "passed",
    "elapsed_ms": 450,
    "jobs": {
      "setup": {
        "row": { "status": "passed", "started_ms": 0, "duration_ms": 120 },
        "steps": {
          "boot": { "status": "passed", "started_ms": 0, "duration_ms": 120 }
        }
      }
    }
  }
}
```

Each `Row` obeys strict timing invariants:
- `pending` / `skipped`: `started_ms` and `duration_ms` must be omitted (`None`).
- `running`: `started_ms <= elapsed_ms`, `duration_ms` omitted.
- `passed` / `failed`: `started_ms + duration_ms <= elapsed_ms`.

---

## 6. Legacy External Smoke JSON v1 (`schema_version = 1`)

Accepted by `kat0-pipeline import` (up to `64 KiB` and `64` steps) for single-job finished results:

```json
{
  "schema_version": 1,
  "job_id": "smoke",
  "job_title": "Browser smoke",
  "elapsed_ms": 250,
  "steps": [
    {
      "id": "open",
      "title": "Open page",
      "result": { "status": "passed", "started_ms": 0, "duration_ms": 250 }
    }
  ]
}
```

Step `result.status` accepts `"passed"`, `"failed"` (with `"kind": "startup" | "assertion" | "execution"`), or `"skipped"`.
