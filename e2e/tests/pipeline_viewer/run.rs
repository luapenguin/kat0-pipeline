// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kat0_pipeline_playwright::playwright_rs::{Browser, Page, expect};
use tokio::fs;

use super::{
    Renderer, ReportLocation, TestResult, check_heading, check_metrics, check_panel, check_run,
};

const TITLE: &str = "Orchestrated SSE run";
const DESCRIPTION: &str = "Live NDJSON v3 events streamed over a private Unix socket.";
const ACCESS_JOB: &str = "#panel-pipeline-job-access";

const PRODUCER_SCRIPT: &str = r#"
import json, os, socket, sys, time

gate_path = sys.argv[1]
sock_path = os.environ["KAT0_PIPELINE_EVENTS_SOCK"]

def send(sock, payload):
    sock.sendall((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))

with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
    s.connect(sock_path)
    send(s, {
        "type": "run_started",
        "schema_version": 3,
        "workers": 1,
        "definition": {
            "title": "Orchestrated SSE run",
            "description": "Live NDJSON v3 events streamed over a private Unix socket.",
            "columns": [
                {
                    "title": "Prepare",
                    "jobs": [
                        {
                            "id": "prepare",
                            "title": "Test env",
                            "is_test": False,
                            "dependencies": [],
                            "steps": [{"id": "boot", "title": "Start isolated fixture"}]
                        }
                    ]
                },
                {
                    "title": "Regression",
                    "jobs": [
                        {
                            "id": "access",
                            "title": "Access",
                            "is_test": True,
                            "dependencies": ["prepare"],
                            "steps": [],
                            "cases": [
                                {"id": "login-flow", "title": "Login flow", "steps": []}
                            ]
                        }
                    ]
                }
            ]
        }
    })
    send(s, {"type": "job_started", "job_id": "prepare", "at_ms": 0})
    send(s, {"type": "step_started", "job_id": "prepare", "step_id": "boot", "at_ms": 0})
    send(s, {"type": "step_finished", "job_id": "prepare", "step_id": "boot", "status": "passed", "at_ms": 40})
    send(s, {"type": "job_finished", "job_id": "prepare", "status": "passed", "at_ms": 40})
    send(s, {"type": "job_started", "job_id": "access", "at_ms": 40})
    send(s, {"type": "case_started", "job_id": "access", "case_id": "login-flow", "at_ms": 40})
    send(s, {
        "type": "step_started",
        "job_id": "access",
        "case_id": "login-flow",
        "step_id": "submit-credentials",
        "title": "Submit credentials",
        "at_ms": 50
    })

    deadline = time.monotonic() + 30.0
    while not os.path.exists(gate_path) and time.monotonic() < deadline:
        time.sleep(0.05)

    send(s, {
        "type": "step_finished",
        "job_id": "access",
        "case_id": "login-flow",
        "step_id": "submit-credentials",
        "status": "passed",
        "at_ms": 180
    })
    send(s, {"type": "case_finished", "job_id": "access", "case_id": "login-flow", "status": "passed", "at_ms": 190})
    send(s, {"type": "job_finished", "job_id": "access", "status": "passed", "at_ms": 200})
    send(s, {"type": "run_finished", "status": "passed", "elapsed_ms": 200})
"#;

pub(super) async fn check(browser: &Browser, renderer: &Renderer) -> TestResult {
    let gate = renderer.root().join("continue.flag");
    let gate_arg = gate
        .to_str()
        .ok_or("Temporary test path is not valid UTF-8")?;

    let report = renderer
        .with_orchestrated_run(
            "python3",
            &["-u", "-c", PRODUCER_SCRIPT, gate_arg],
            async |origin| {
                check_panel(browser, ReportLocation::Live(origin), async |page| {
                    check_live_sse(page, &gate).await
                })
                .await
            },
        )
        .await?;

    check_panel(browser, ReportLocation::Saved(&report), check_saved_report).await
}

async fn check_live_sse(page: &Page, gate: &Path) -> TestResult {
    let sse_requests = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&sse_requests);
    page.on_request(move |request| {
        if request.url().ends_with("/events") {
            observed.fetch_add(1, Ordering::Relaxed);
        }
        async { Ok(()) }
    })
    .await?;

    check_heading(page, TITLE, DESCRIPTION).await?;
    expect(page.locator("#status:visible"))
        .to_have_text("Running")
        .await?;
    expect(page.locator("#active-step:visible"))
        .to_have_text("Now running: Login flow › Submit credentials")
        .await?;

    // Open job details while running to confirm live rendering of dynamic case steps.
    page.locator(ACCESS_JOB).click(None).await?;
    expect(page.locator("#timeline .detail-title:visible"))
        .to_have_text("Access")
        .await?;
    expect(page.locator("#timeline .case-title:visible"))
        .to_have_text("Login flow")
        .await?;
    expect(page.locator("#timeline .cases .steps .step-title:visible"))
        .to_have_text("Submit credentials")
        .await?;

    // Release the producer so it emits terminal events over the Unix socket -> SSE.
    fs::write(gate, b"go").await?;

    check_run(page, "Passed", "200 ms").await?;
    expect(page.locator("#active-step")).to_be_hidden().await?;
    check_metrics(
        page.locator("#timeline .cases .steps .metrics"),
        "Passed",
        "130 ms",
    )
    .await?;
    expect(page.locator("#tab-tests .count"))
        .to_have_text("1")
        .await?;

    // Wait briefly after terminal state to ensure EventSource stays closed without reconnecting.
    let after_terminal = sse_requests.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(600)).await;
    if sse_requests.load(Ordering::Relaxed) != after_terminal {
        return Err("EventSource attempted to reconnect to /events after terminal state".into());
    }
    Ok(())
}

async fn check_saved_report(page: &Page) -> TestResult {
    check_heading(page, TITLE, DESCRIPTION).await?;
    check_run(page, "Passed", "200 ms").await?;
    expect(page.locator("#active-step")).to_be_hidden().await?;
    page.locator(ACCESS_JOB).click(None).await?;
    expect(page.locator("#timeline .case-title:visible"))
        .to_have_text("Login flow")
        .await?;
    expect(page.locator("#timeline .cases .steps .step-title:visible"))
        .to_have_text("Submit credentials")
        .await?;
    check_metrics(
        page.locator("#timeline .cases .steps .metrics"),
        "Passed",
        "130 ms",
    )
    .await?;
    Ok(())
}
