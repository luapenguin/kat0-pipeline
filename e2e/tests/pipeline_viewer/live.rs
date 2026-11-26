// SPDX-License-Identifier: AGPL-3.0-only

use std::path::Path;

use kat0_pipeline_playwright::playwright_rs::{self, Browser, Page, expect};
use serde_json::{Value, json};

use super::{
    Renderer, ReportLocation, TestResult, check_heading, check_metrics, check_panel, check_run,
    workspace_root,
};

const TITLE: &str = "Live web checks";
const DESCRIPTION: &str = "Prepare the environment before checking organization pages.";
const BROWSE: &str = "#panel-pipeline-job-browse-orgs";
const BROWSE_DETAIL: &str = "#timeline .row:has(.detail-title) .metrics";
const PAGINATION: &str = "#timeline .steps .row:has-text('Verify pagination') .metrics";
const HISTORY: &str = "#timeline .steps .row:has-text('Check back navigation') .metrics";

pub(super) async fn check(browser: &Browser, renderer: &Renderer) -> TestResult {
    let path = renderer.root().join("live.json");
    let mut document = alternate_flow().await?;
    replace_snapshot(&path, &document)?;
    renderer
        .with_live(&path, async |origin| {
            check_panel(browser, ReportLocation::Live(origin), async |page| {
                check_updates(page, &path, &mut document).await
            })
            .await
        })
        .await?;
    let report = renderer.import_failed(&path).await?;
    check_panel(browser, ReportLocation::Saved(&report), check_imported).await
}

async fn alternate_flow() -> TestResult<Value> {
    let fixture = workspace_root().join("server/fixtures/web_regression.json");
    let bytes = tokio::fs::read(fixture).await?;
    let mut document: Value = serde_json::from_slice(&bytes)?;
    if document["schema_version"] != 2 {
        return Err("Canonical fixture must be a v2 snapshot".into());
    }

    // Exercise caller-defined topology without maintaining a second sample fixture.
    let snapshot = document
        .get_mut("snapshot")
        .ok_or("Missing snapshot in canonical fixture")?;
    let columns = snapshot
        .pointer_mut("/definition/columns")
        .and_then(Value::as_array_mut)
        .ok_or("Missing columns in canonical fixture")?;
    columns.truncate(2);
    let regression = columns
        .get_mut(1)
        .and_then(|column| column.get_mut("jobs"))
        .and_then(Value::as_array_mut)
        .ok_or("Missing Regression stage jobs")?;
    regression.retain(|job| job["id"] == "browse-orgs");
    let jobs = snapshot
        .get_mut("jobs")
        .and_then(Value::as_object_mut)
        .ok_or("Missing jobs map in snapshot")?;
    jobs.retain(|id, _| matches!(id.as_str(), "prepare-env" | "browse-orgs"));
    snapshot["definition"]["title"] = json!(TITLE);
    snapshot["definition"]["description"] = json!(DESCRIPTION);
    Ok(document)
}

fn replace_snapshot(path: &Path, document: &Value) -> TestResult {
    // Stage beside the input so the live reader never sees a partially written snapshot.
    let mut staged =
        tempfile::NamedTempFile::new_in(path.parent().ok_or("Snapshot path missing parent dir")?)?;
    serde_json::to_writer(staged.as_file_mut(), document)?;
    staged.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn finish(document: &mut Value, elapsed_ms: u64) -> TestResult {
    let snapshot = &mut document["snapshot"];
    snapshot["status"] = json!("failed");
    snapshot["elapsed_ms"] = json!(elapsed_ms);
    for pointer in [
        "/jobs/browse-orgs/row",
        "/jobs/browse-orgs/steps/check-pagination",
    ] {
        let row = snapshot
            .pointer_mut(pointer)
            .ok_or_else(|| format!("Missing {pointer}"))?;
        let duration = row["started_ms"]
            .as_u64()
            .and_then(|start| elapsed_ms.checked_sub(start))
            .ok_or_else(|| format!("Invalid start timestamp at {pointer}"))?;
        row["status"] = json!("failed");
        row["duration_ms"] = json!(duration);
    }
    *snapshot
        .pointer_mut("/jobs/browse-orgs/steps/check-history/status")
        .ok_or("Missing status for Check back navigation")? = json!("skipped");
    Ok(())
}

async fn check_updates(page: &Page, path: &Path, document: &mut Value) -> TestResult {
    check_topology(page).await?;
    check_run(page, "Running", "48.3 s").await?;
    check_served_snapshot(page, document).await?;
    expect(page.locator("#tab-failed .count"))
        .to_have_text("0")
        .await?;
    let job = page.locator(BROWSE);
    check_metrics(job.locator(".job-metrics"), "Running", "8.3 s").await?;
    job.click(None).await?;
    expect(page.locator("#timeline .detail-title"))
        .to_have_text("Org pages")
        .await?;
    check_metrics(page.locator(PAGINATION), "Running", "6.5 s").await?;

    document["snapshot"]["elapsed_ms"] = json!(49_300);
    replace_snapshot(path, document)?;
    check_run(page, "Running", "49.3 s").await?;
    check_metrics(page.locator(BROWSE_DETAIL), "Running", "9.3 s").await?;

    finish(document, 50_000)?;
    replace_snapshot(path, document)?;
    check_run(page, "Failed", "50.0 s").await?;
    check_served_snapshot(page, document).await?;
    check_terminal_detail(page).await?;
    check_metrics(job.locator(".job-metrics"), "Failed", "10.0 s").await?;
    let mark = job.locator("svg.job-icon.failed .job-icon-mark");
    expect(mark).to_have_css("animation-name", "none").await?;

    expect(page.locator("#tab-failed .count"))
        .to_have_text("1")
        .await?;
    let failed = page.locator("#tab-failed");
    expect(failed.clone())
        .to_contain_text("Failed Jobs")
        .await?;
    failed.click(None).await?;
    expect(failed)
        .to_have_attribute("aria-selected", "true")
        .await?;
    let names = page.locator("#panel-failed .job-name:visible");
    expect(names.clone()).to_have_count(1).await?;
    expect(names).to_have_text("Org pages").await?;
    Ok(())
}

async fn check_imported(page: &Page) -> TestResult {
    check_topology(page).await?;
    check_run(page, "Failed", "50.0 s").await?;
    check_metrics(
        page.locator(BROWSE).locator(".job-metrics"),
        "Failed",
        "10.0 s",
    )
    .await?;
    expect(page.locator("#tab-jobs .count"))
        .to_have_text("1")
        .await?;
    page.locator("#tab-jobs").click(None).await?;
    expect(page.locator("#panel-jobs .job:visible"))
        .to_have_count(1)
        .await?;
    page.locator("#panel-jobs-job-browse-orgs")
        .click(None)
        .await?;
    check_terminal_detail(page).await?;
    Ok(())
}

async fn check_topology(page: &Page) -> playwright_rs::Result<()> {
    check_heading(page, TITLE, DESCRIPTION).await?;
    expect(page.locator("#panel-pipeline .stage-column:visible"))
        .to_have_count(2)
        .await?;
    expect(page.locator("#panel-pipeline-column-0:visible"))
        .to_have_text("Prepare")
        .await?;
    expect(page.locator("#panel-pipeline-column-1:visible"))
        .to_have_text("Regression")
        .await?;
    expect(page.locator("#panel-pipeline .job:visible"))
        .to_have_count(2)
        .await?;
    expect(page.locator(".connections path[data-connection][d]"))
        .to_have_count(1)
        .await
}

async fn check_terminal_detail(page: &Page) -> playwright_rs::Result<()> {
    expect(page.locator("#timeline .detail-title:visible"))
        .to_have_text("Org pages")
        .await?;
    check_metrics(page.locator(BROWSE_DETAIL), "Failed", "10.0 s").await?;
    check_metrics(page.locator(PAGINATION), "Failed", "8.2 s").await?;
    check_metrics(page.locator(HISTORY), "Not run", "—").await
}

async fn check_served_snapshot(page: &Page, document: &Value) -> TestResult {
    let served = page
        .evaluate::<(), Value>(
            r#"async () => {
            const response = await fetch('/snapshot');
            if (!response.ok) throw new Error('GET /snapshot failed');
            return response.json();
        }"#,
            None,
        )
        .await?;
    if served != document["snapshot"] {
        return Err("GET /snapshot did not return the active file snapshot".into());
    }
    Ok(())
}
