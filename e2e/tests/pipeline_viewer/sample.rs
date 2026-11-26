// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashSet;

use kat0_pipeline_playwright::playwright_rs::{
    self, ColorScheme, EmulateMediaOptions, Page, ReducedMotion, Viewport, expect,
};

use super::{DESKTOP, TestResult, check_heading, check_metrics, check_run};

pub(super) async fn check(page: &Page) -> TestResult {
    check_heading(
        page,
        "Kat0 Web Regression",
        "Simulated snapshot. No jobs ran.",
    )
    .await?;
    expect(page.locator("#pipeline-flow"))
        .to_contain_text("The report includes test and cleanup outcomes.")
        .await?;
    check_run(page, "Running", "48.3 s").await?;
    check_graph(page).await?;
    check_layout(page).await?;
    check_motion(page).await?;
    check_tabs(page).await?;
    check_edit_role(page).await?;
    Ok(())
}

async fn check_graph(page: &Page) -> TestResult {
    let stages = [
        "Prepare",
        "Regression",
        "Checks & evidence",
        "Cleanup",
        "Report",
    ];
    expect(page.locator("#panel-pipeline .stage-column"))
        .to_have_count(stages.len())
        .await?;
    for (index, title) in stages.into_iter().enumerate() {
        expect(page.locator(format!("#panel-pipeline-column-{index}:visible")))
            .to_have_text(title)
            .await?;
    }

    let jobs = [
        ("prepare-env", "Test env", "passed", "Passed", "10.0 s"),
        ("change-role", "Edit role", "failed", "Failed", "21.3 s"),
        ("sign-in", "Sign in", "passed", "Passed", "14.2 s"),
        ("browse-orgs", "Org pages", "running", "Running", "8.3 s"),
        ("role-persistence", "Verify role", "skipped", "Not run", "—"),
        ("save-evidence", "Archive", "pending", "Waiting", "—"),
        ("cleanup-env", "Clean up", "pending", "Waiting", "—"),
        ("collect-results", "Publish", "pending", "Waiting", "—"),
    ];
    expect(page.locator("#panel-pipeline .job:visible"))
        .to_have_count(jobs.len())
        .await?;
    let mut marks = HashSet::new();
    for (id, title, state, label, duration) in jobs {
        let job = page.locator(format!("#panel-pipeline-job-{id}"));
        expect(job.locator(".job-name:visible"))
            .to_have_text(title)
            .await?;
        let icon = job.locator(format!("svg.job-icon.status.{state}"));
        expect(icon.clone()).to_be_visible().await?;
        let mark = icon
            .locator(".job-icon-mark")
            .get_attribute("d")
            .await?
            .filter(|path| !path.is_empty())
            .ok_or_else(|| format!("El icono del job {id} no tiene trazado"))?;
        marks.insert(mark);
        check_metrics(job.locator(".job-metrics"), label, duration).await?;
    }
    if marks.len() != 5 {
        return Err("Los cinco estados deben distinguirse por el icono".into());
    }
    Ok(())
}

async fn check_layout(page: &Page) -> TestResult {
    let paths = page.locator(".connections path[data-connection][d]");
    let branches = page.locator(".connections path[data-connection][d*=' A '][d*=' V ']");
    let straight = page.locator(".connections path[data-connection][d]:not([d*=' A '])");
    expect(paths.clone()).to_have_count(9).await?;
    expect(branches.clone()).to_have_count(4).await?;
    expect(straight.clone()).to_have_count(5).await?;
    check_labels_fit(page).await?;
    let branch = branches.first();
    let desktop_path = branch
        .get_attribute("d")
        .await?
        .ok_or("La rama no tiene trazado en escritorio")?;

    page.set_viewport_size(Viewport {
        width: 390,
        height: DESKTOP.height,
    })
    .await?;
    expect(branch.clone())
        .not()
        .to_have_attribute("d", &desktop_path)
        .await?;
    expect(paths).to_have_count(9).await?;
    expect(branches).to_have_count(4).await?;
    expect(straight).to_have_count(5).await?;
    check_labels_fit(page).await?;
    let only_graph_scrolls = page
        .evaluate::<(), bool>(
            r#"() => {
            const graph = document.querySelector('.pipeline-scroll');
            const tabs = document.querySelector('.tabs');
            const root = document.documentElement;
            return graph.scrollWidth > graph.clientWidth &&
                getComputedStyle(graph).overflowX === 'auto' &&
                tabs.scrollWidth === tabs.clientWidth && root.scrollWidth === root.clientWidth;
        }"#,
            None,
        )
        .await?;
    if !only_graph_scrolls {
        return Err("Solo el grafo debe desbordar horizontalmente a 390 px".into());
    }
    page.set_viewport_size(DESKTOP).await?;
    expect(branch).to_have_attribute("d", &desktop_path).await?;
    Ok(())
}

async fn check_labels_fit(page: &Page) -> TestResult {
    let clipped = page
        .evaluate::<(), Vec<String>>(
            r#"() => [...document.querySelectorAll('#panel-pipeline .job')].filter(job => {
            const title = job.querySelector('.job-name');
            const metrics = job.querySelector('.job-metrics');
            const card = job.getBoundingClientRect();
            const box = metrics.getBoundingClientRect();
            return title.scrollWidth > title.clientWidth ||
                metrics.scrollWidth > metrics.clientWidth ||
                box.top < card.top || box.bottom > card.bottom;
        }).map(job => job.id)"#,
            None,
        )
        .await?;
    if !clipped.is_empty() {
        return Err(format!("Tarjetas con texto recortado: {clipped:?}").into());
    }
    Ok(())
}

async fn check_motion(page: &Page) -> playwright_rs::Result<()> {
    let light = EmulateMediaOptions::builder()
        .color_scheme(ColorScheme::Light)
        .reduced_motion(ReducedMotion::NoPreference)
        .build();
    page.emulate_media(light.clone()).await?;
    let running = page.locator("#panel-pipeline-job-browse-orgs .job-icon-mark");
    expect(running.clone())
        .to_have_css("fill", "rgb(115, 115, 115)")
        .await?;
    expect(running.clone())
        .to_have_css("animation-duration", "1.55s")
        .await?;
    expect(running.clone())
        .to_have_css("animation-iteration-count", "infinite")
        .await?;
    expect(running.clone())
        .not()
        .to_have_css("transform", "none")
        .await?;
    expect(page.locator("#panel-pipeline-job-prepare-env .job-icon-mark"))
        .to_have_css("stroke", "rgb(22, 163, 74)")
        .await?;
    page.emulate_media(
        EmulateMediaOptions::builder()
            .color_scheme(ColorScheme::Dark)
            .reduced_motion(ReducedMotion::Reduce)
            .build(),
    )
    .await?;
    expect(running.clone())
        .to_have_css("fill", "rgb(169, 169, 175)")
        .await?;
    expect(running.clone())
        .to_have_css("animation-name", "none")
        .await?;
    page.emulate_media(light).await?;
    expect(running).not().to_have_css("transform", "none").await
}

async fn check_tabs(page: &Page) -> playwright_rs::Result<()> {
    for (from, key, to) in [
        ("pipeline", "ArrowRight", "jobs"),
        ("jobs", "End", "tests"),
        ("tests", "ArrowLeft", "failed"),
        ("failed", "Home", "pipeline"),
    ] {
        page.locator(format!("#tab-{from}"))
            .press(key, None)
            .await?;
        let tab = page.locator(format!("#tab-{to}"));
        expect(tab.clone())
            .to_have_attribute("aria-selected", "true")
            .await?;
        expect(tab).to_be_focused().await?;
        expect(page.locator(format!("#panel-{to}")))
            .to_be_visible()
            .await?;
    }
    Ok(())
}

async fn check_edit_role(page: &Page) -> playwright_rs::Result<()> {
    let edit_role = page.locator("#panel-pipeline-job-change-role");
    let timeline = page.locator("#timeline");
    expect(timeline.clone()).to_be_hidden().await?;
    edit_role.press("Enter", None).await?;
    expect(timeline.clone()).to_be_visible().await?;
    expect(edit_role.clone())
        .to_have_attribute("aria-expanded", "true")
        .await?;
    expect(timeline.locator(".detail-title"))
        .to_have_text("Edit role")
        .await?;
    check_metrics(
        timeline.locator(".row:has(.detail-title) .metrics"),
        "Failed",
        "21.3 s",
    )
    .await?;
    for (state, name, label, duration) in [
        ("failed", "Save role change", "Failed", "14.9 s"),
        ("skipped", "Check displayed role", "Not run", "—"),
    ] {
        let step = timeline.locator(format!(".steps .row:has(.status.{state})"));
        expect(step.locator(".step-title:visible"))
            .to_have_text(name)
            .await?;
        check_metrics(step.locator(".metrics"), label, duration).await?;
    }
    timeline
        .locator(".close-timeline")
        .press("Escape", None)
        .await?;
    expect(timeline).to_be_hidden().await?;
    expect(edit_role.clone())
        .to_have_attribute("aria-expanded", "false")
        .await?;
    expect(edit_role).to_be_focused().await
}
