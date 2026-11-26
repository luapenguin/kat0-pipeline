// SPDX-License-Identifier: AGPL-3.0-only

#![forbid(unsafe_code)]

use std::{
    fmt::Write as _,
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kat0_pipeline_playwright::{
    FirefoxSession,
    playwright_rs::{
        self, Browser, BrowserContextOptions, LaunchOptions, Locator, Page, Viewport, expect,
    },
    reporter::{BoxError, RunResult as TestResult, execute::combine},
};
use tokio::time::timeout;

#[path = "pipeline_viewer/live.rs"]
mod live;
#[path = "pipeline_viewer/renderer.rs"]
mod renderer;
#[path = "pipeline_viewer/run.rs"]
mod run;
#[path = "pipeline_viewer/sample.rs"]
mod sample;

use renderer::Renderer;

const BROWSER_START: Duration = Duration::from_secs(30);
const PAGE_CHECK: Duration = Duration::from_secs(60);
const CLOSE: Duration = Duration::from_secs(10);
const DESKTOP: Viewport = Viewport {
    width: 1272,
    height: 420,
};

#[tokio::test]
#[ignore = "requires a prebuilt kat0-pipeline and Playwright Firefox; see e2e/README.md"]
async fn pipeline_viewer_renders_offline_and_observes_live_snapshots() -> TestResult {
    let directory = tempfile::Builder::new()
        .prefix("pipeline viewer ")
        .tempdir()?;
    let renderer = Renderer::new(directory.path())?;
    let report = renderer.sample().await?;

    // No executable override: use the Firefox revision pinned by the Playwright driver.
    let session = FirefoxSession::launch(
        LaunchOptions::new().headless(true).timeout(15_000.0),
        BROWSER_START,
    )
    .await?;
    let checked = async {
        check_panel(
            session.browser(),
            ReportLocation::Saved(&report),
            sample::check,
        )
        .await?;
        live::check(session.browser(), &renderer).await?;
        run::check(session.browser(), &renderer).await
    }
    .await;
    let closed = session.close(CLOSE).await;
    combine(checked, closed)
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("e2e lives directly under the kat0-pipeline workspace root")
        .to_owned()
}

async fn within<T, E: Into<BoxError>>(
    limit: Duration,
    phase: &str,
    action: impl Future<Output = Result<T, E>>,
) -> TestResult<T> {
    timeout(limit, action)
        .await
        .map_err(|_| format!("{phase}: timed out after {} s", limit.as_secs()))?
        .map_err(Into::into)
}

enum ReportLocation<'a> {
    Saved(&'a Path),
    Live(&'a str),
}

async fn check_panel(
    browser: &Browser,
    report: ReportLocation<'_>,
    check: impl AsyncFnOnce(&Page) -> TestResult,
) -> TestResult {
    let (url, offline, phase) = match report {
        ReportLocation::Saved(path) => (file_url(path)?, true, "Offline report"),
        ReportLocation::Live(origin) => (origin.to_owned(), false, "Live report"),
    };
    let options = BrowserContextOptions::builder()
        .offline(offline)
        .viewport(DESKTOP)
        .build();
    let context = within(
        CLOSE,
        "Opening browser context",
        browser.new_context_with_options(options),
    )
    .await?;
    let http_requests = Arc::new(AtomicUsize::new(0));
    let checked = within(PAGE_CHECK, phase, async {
        let page = context.new_page().await?;
        page.set_default_timeout(5_000.0).await;
        page.set_default_navigation_timeout(15_000.0).await;
        if offline {
            let observed = Arc::clone(&http_requests);
            page.on_request(move |request| {
                let url = request.url();
                if url.starts_with("http://") || url.starts_with("https://") {
                    observed.fetch_add(1, Ordering::Relaxed);
                }
                async { Ok(()) }
            })
            .await?;
        }
        let response = page.goto(&url, None).await?;
        if !offline && !response.is_some_and(|response| response.ok()) {
            return Err("Local server failed to serve the panel".into());
        }
        expect(page.locator("#panel-ready.snapshot-view"))
            .to_be_visible()
            .await?;
        check(&page).await
    })
    .await;
    let closed = within(CLOSE, "Closing browser context", context.close()).await;
    checked.and(closed).map_err(|error| {
        format!(
            "{phase}: {}",
            error.to_string().replace(&url, "[temporary URL]")
        )
    })?;
    if http_requests.load(Ordering::Relaxed) != 0 {
        return Err("Standalone offline report attempted HTTP requests".into());
    }
    Ok(())
}

fn file_url(path: &Path) -> TestResult<String> {
    let mut url = String::from("file://");
    for &byte in path.as_os_str().as_encoded_bytes() {
        if byte.is_ascii_alphanumeric() || b"/-._~".contains(&byte) {
            url.push(char::from(byte));
        } else {
            write!(url, "%{byte:02X}")?;
        }
    }
    Ok(url)
}

async fn check_heading(page: &Page, title: &str, description: &str) -> playwright_rs::Result<()> {
    let heading = page.locator(".snapshot-heading");
    expect(heading.clone())
        .to_have_css("clip-path", "none")
        .await?;
    expect(heading.locator("h1:visible"))
        .to_have_text(title)
        .await?;
    expect(heading.locator("#pipeline-flow:visible"))
        .to_contain_text(description)
        .await
}

async fn check_run(page: &Page, status: &str, elapsed: &str) -> playwright_rs::Result<()> {
    expect(page.locator("#status:visible"))
        .to_have_text(status)
        .await?;
    expect(page.locator("#elapsed:visible"))
        .to_have_text(elapsed)
        .await
}

async fn check_metrics(
    metrics: Locator,
    status: &str,
    duration: &str,
) -> playwright_rs::Result<()> {
    // A clipped box can still match Playwright's :visible.
    expect(metrics.clone())
        .to_have_css("clip-path", "none")
        .await?;
    expect(metrics.locator(".status:visible"))
        .to_have_text(status)
        .await?;
    expect(metrics.locator(".duration:visible"))
        .to_have_text(duration)
        .await
}

#[test]
fn parses_loopback_origins_and_encodes_file_urls() {
    assert_eq!(
        renderer::parse_loopback_origin("http://127.0.0.1:41234/"),
        Some("http://127.0.0.1:41234".into())
    );
    assert_eq!(renderer::parse_loopback_origin("http://127.0.0.1:0"), None);
    assert_eq!(
        renderer::parse_loopback_origin("https://example.test:443"),
        None
    );
    assert_eq!(
        file_url(Path::new("/tmp/pipeline viewer/index.html")).unwrap(),
        "file:///tmp/pipeline%20viewer/index.html"
    );
}
