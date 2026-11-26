// SPDX-License-Identifier: AGPL-3.0-only

use kat0_pipeline_protocol::model::{Snapshot, Status};
use leptos::prelude::*;

mod buttons;
mod icons;
mod tabbar;

pub(crate) use buttons::{JobButton, button_id};
pub(crate) use tabbar::{Tab, TabBar, focus};

#[component]
pub(crate) fn Metrics(
    snapshot: ReadSignal<Snapshot>,
    job_id: StoredValue<String>,
    #[prop(optional)] case: Option<String>,
    #[prop(optional)] step: Option<String>,
) -> impl IntoView {
    let metrics = Memo::new(move |_| {
        snapshot.with(|run| {
            job_id.with_value(|id| {
                let row = match case.as_deref() {
                    Some(case_id) => run.case_row(id, case_id, step.as_deref()),
                    None => run.row(id, step.as_deref()),
                };
                row.map(|row| (row.status, row.duration_at(run.elapsed_ms)))
            })
        })
    });
    let status = Memo::new(move |_| metrics.get().map(|(status, _)| status));
    view! {
        <span class=move || status.get().map(status_class)>{move || status.get().map(status_label)}</span>
        " "
        <span class="duration">{move || duration_text(metrics.get().and_then(|(_, duration)| duration))}</span>
    }
}

pub(crate) fn status_label(status: Status) -> &'static str {
    match status {
        Status::Pending => "Waiting",
        Status::Running => "Running",
        Status::Passed => "Passed",
        Status::Failed => "Failed",
        Status::Skipped => "Not run",
        _ => "Unknown",
    }
}

pub(crate) fn status_class(status: Status) -> &'static str {
    match status {
        Status::Pending => "status pending",
        Status::Running => "status running",
        Status::Passed => "status passed",
        Status::Failed => "status failed",
        Status::Skipped => "status skipped",
        _ => "status pending",
    }
}

pub(crate) fn duration_text(ms: Option<u64>) -> String {
    match ms {
        None => "—".into(),
        Some(ms) if ms < 1000 => format!("{ms} ms"),
        Some(ms) => format!("{:.1} s", ms as f64 / 1000.0),
    }
}

fn aria(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}
