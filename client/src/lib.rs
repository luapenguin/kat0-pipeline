// SPDX-License-Identifier: AGPL-3.0-only

#![forbid(unsafe_code)]

use kat0_pipeline_protocol::model::{Definition, Job, Snapshot, Status};
use leptos::{mount::mount_to, prelude::*};
use wasm_bindgen::{JsCast, JsValue, prelude::wasm_bindgen};

mod components;
mod graph;
mod stream;

use components::{
    JobButton, Metrics, Tab, TabBar, button_id, duration_text, focus, status_class, status_label,
};
use graph::pipeline;

#[wasm_bindgen(js_name = hydrate)]
pub fn mount_report() -> Result<(), JsValue> {
    let document = document();
    let root = document
        .get_element_by_id("panel")
        .ok_or_else(|| JsValue::from_str("Missing report mount point"))?
        .dyn_into::<web_sys::HtmlElement>()?;
    let live = root.get_attribute("data-live").as_deref() == Some("true");
    let json = document
        .get_element_by_id("run-data")
        .and_then(|element| element.text_content())
        .ok_or_else(|| JsValue::from_str("Missing run snapshot"))?;
    let initial: Snapshot =
        serde_json::from_str(&json).map_err(|error| JsValue::from_str(&error.to_string()))?;
    initial
        .definition()
        .validate()
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    document.set_title(&initial.definition().title);
    mount_to(root, move || view! { <Report initial live/> }).forget();
    if let Some(loading) = document.get_element_by_id("panel-loading") {
        loading.remove();
    }
    Ok(())
}

#[component]
fn Report(initial: Snapshot, live: bool) -> impl IntoView {
    let definition = StoredValue::new(initial.definition().clone());
    let (snapshot, write_snapshot) = {
        let snapshot = RwSignal::new(initial);
        (snapshot.read_only(), snapshot)
    };
    let disconnected = RwSignal::new(false);
    let _live_stream = live
        .then(|| stream::connect(write_snapshot, disconnected))
        .flatten()
        .map(StoredValue::new_local);
    let active = RwSignal::new(Tab::Pipeline);
    let selected = RwSignal::<Option<String>>::new(None);
    let close_details = move || {
        if let Some(job_id) = selected.get_untracked() {
            selected.set(None);
            focus(&button_id(active.get_untracked(), &job_id));
        }
    };
    let run_status = Memo::new(move |_| snapshot.with(|run| run.status));
    let active_work = Memo::new(move |_| snapshot.with(Snapshot::active_label));
    let job_count = definition.with_value(|definition| test_jobs(definition).count());
    let test_count = Memo::new(move |_| snapshot.with(|run| run.definition().test_case_count()));
    let failed = Memo::new(move |_| {
        snapshot.with(|run| {
            definition.with_value(|definition| {
                test_jobs(definition)
                    .filter(|job| run.job_status(&job.id) == Some(Status::Failed))
                    .count()
            })
        })
    });

    view! {
        <main id="panel-ready" class="snapshot-view" on:keydown=move |event: web_sys::KeyboardEvent| {
            if event.key() == "Escape" {
                close_details();
            }
        }>
            <header class="snapshot-heading">
                <h1>{definition.with_value(|definition| definition.title.clone())}</h1>
                <p id="pipeline-flow">
                    {definition.with_value(|definition| definition.description.clone())}
                </p>
                <p id="active-step" class="active-step" role="status" hidden=move || active_work.with(Option::is_none)>
                    {move || active_work.get().map(|label| format!("Now running: {label}"))}
                </p>
            </header>
            <div class="toolbar">
                <TabBar active selected failed job_count test_count/>
                <div class="run-summary">
                    <span id="status" class=move || status_class(run_status.get()) role="status">
                        {move || status_label(run_status.get())}
                    </span>
                    <span>"Total: "<span id="elapsed" class="duration">
                        {move || snapshot.with(|run| duration_text(Some(run.elapsed_ms)))}
                    </span></span>
                    <span role="status" hidden=move || !disconnected.get()>"Live updates unavailable"</span>
                </div>
            </div>
            <section id=Tab::Pipeline.panel_id() role="tabpanel" aria-labelledby=Tab::Pipeline.id()
                aria-describedby="pipeline-flow" hidden=move || active.get() != Tab::Pipeline>
                {definition.with_value(|definition| pipeline(definition, snapshot, selected))}
            </section>
            <section class="list-panel" id=Tab::Jobs.panel_id() role="tabpanel" aria-labelledby=Tab::Jobs.id()
                hidden=move || active.get() != Tab::Jobs>
                <JobList definition snapshot selected tab=Tab::Jobs/>
            </section>
            <section class="list-panel" id=Tab::Failed.panel_id() role="tabpanel" aria-labelledby=Tab::Failed.id() tabindex="0"
                hidden=move || active.get() != Tab::Failed>
                <JobList definition snapshot selected tab=Tab::Failed/>
                <p class="empty-jobs" hidden=move || failed.get() != 0>"No failed jobs."</p>
            </section>
            <section class="list-panel" id=Tab::Tests.panel_id() role="tabpanel" aria-labelledby=Tab::Tests.id() tabindex="0"
                hidden=move || active.get() != Tab::Tests>
                {definition.with_value(|definition| test_jobs(definition).map(|job| view! {
                    <section class="progress" aria-label=job.title.clone()>
                        {job_details(snapshot, job.id.clone())}
                    </section>
                }).collect_view())}
            </section>
            <section class="timeline" id="timeline" aria-labelledby="timeline-title" hidden=move || selected.with(Option::is_none)>
                <div class="progress">
                    <div class="progress-header">
                        <h2 id="timeline-title">"Job details"</h2>
                        <button class="close-timeline" type="button" on:click=move |_| close_details()>"Close"</button>
                    </div>
                    {move || selected.get().map(|id| job_details(snapshot, id))}
                </div>
            </section>
        </main>
    }
}

fn test_jobs(definition: &Definition) -> impl Iterator<Item = &Job> {
    definition.jobs().filter(|job| job.is_test)
}

#[component]
fn JobList(
    definition: StoredValue<Definition>,
    snapshot: ReadSignal<Snapshot>,
    selected: RwSignal<Option<String>>,
    tab: Tab,
) -> impl IntoView {
    view! {
        <ul class="job-list" role="list">
            {definition.with_value(|definition| test_jobs(definition).map(|job| {
                let job_id = StoredValue::new(job.id.clone());
                view! {
                    <li hidden=move || tab == Tab::Failed && snapshot.with(|run| {
                        job_id.with_value(|id| run.job_status(id) != Some(Status::Failed))
                    })>
                        <JobButton snapshot selected tab job_id title=job.title.clone()/>
                    </li>
                }
            }).collect_view())}
        </ul>
    }
}

fn job_details(snapshot: ReadSignal<Snapshot>, job_id: String) -> impl IntoView {
    let location = snapshot.with_untracked(|run| find_job_location(run.definition(), &job_id));
    let job_id = StoredValue::new(job_id);
    // Track only lightweight structural counts (Copy, zero allocations) so ticks and status
    // transitions never clone the Job definition or rebuild the DOM.
    let structure = Memo::new(move |_| {
        snapshot.with(|run| {
            job_id.with_value(|id| {
                lookup_job(run.definition(), location, id).map(|job| {
                    let case_steps: usize = job.cases.iter().map(|case| case.steps.len()).sum();
                    (job.steps.len(), job.cases.len(), case_steps)
                })
            })
        })
    });
    move || {
        structure.track();
        let job = snapshot.with_untracked(|run| {
            job_id.with_value(|id| lookup_job(run.definition(), location, id).cloned())
        })?;
        let has_steps = !job.steps.is_empty();
        let has_cases = !job.cases.is_empty();
        let has_entries = has_steps || has_cases;
        Some(view! {
            <div class="row">
                <h2 class="detail-title">{job.title}</h2>
                <span class="metrics"><Metrics snapshot job_id/></span>
            </div>
            <ol class="steps" role="list" hidden=!has_steps>
                {job.steps.into_iter().map(|step| view! {
                    <li><div class="row">
                        <span class="step-title">{step.title}</span>
                        <span class="metrics"><Metrics snapshot job_id step=step.id/></span>
                    </div></li>
                }).collect_view()}
            </ol>
            <ol class="cases" role="list" hidden=!has_cases>
                {job.cases.into_iter().map(|case| {
                    let case_id = case.id;
                    let has_case_steps = !case.steps.is_empty();
                    view! {
                        <li class="case">
                            <div class="row">
                                <span class="case-title">{case.title}</span>
                                <span class="metrics"><Metrics snapshot job_id case=case_id.clone()/></span>
                            </div>
                            <ol class="steps" role="list" hidden=!has_case_steps>
                                {case.steps.into_iter().map(|step| view! {
                                    <li><div class="row">
                                        <span class="step-title">{step.title}</span>
                                        <span class="metrics"><Metrics snapshot job_id case=case_id.clone() step=step.id/></span>
                                    </div></li>
                                }).collect_view()}
                            </ol>
                        </li>
                    }
                }).collect_view()}
            </ol>
            <p class="empty-steps" hidden=has_entries>"No steps recorded."</p>
        })
    }
}

fn find_job_location(definition: &Definition, job_id: &str) -> Option<(usize, usize)> {
    definition
        .columns
        .iter()
        .enumerate()
        .find_map(|(col_idx, column)| {
            column
                .jobs
                .iter()
                .position(|job| job.id == job_id)
                .map(|job_idx| (col_idx, job_idx))
        })
}

fn lookup_job<'a>(
    definition: &'a Definition,
    location: Option<(usize, usize)>,
    job_id: &str,
) -> Option<&'a Job> {
    if let Some((col_idx, job_idx)) = location
        && let Some(job) = definition
            .columns
            .get(col_idx)
            .and_then(|column| column.jobs.get(job_idx))
        && job.id == job_id
    {
        return Some(job);
    }
    definition.jobs().find(|job| job.id == job_id)
}
