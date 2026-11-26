// SPDX-License-Identifier: AGPL-3.0-only

use kat0_pipeline_protocol::model::{Snapshot, Status};
use leptos::{html, prelude::*};

use super::{
    Metrics, aria,
    icons::{ChevronDownIcon, StatusIcon},
    tabbar::Tab,
};

#[component]
pub(crate) fn JobButton(
    snapshot: ReadSignal<Snapshot>,
    selected: RwSignal<Option<String>>,
    tab: Tab,
    job_id: StoredValue<String>,
    title: String,
    #[prop(optional)] node: NodeRef<html::Button>,
) -> impl IntoView {
    let (id, title_id, metrics_id) = job_id.with_value(|id| {
        let panel = tab.panel_id();
        (
            button_id(tab, id),
            format!("{panel}-name-{id}"),
            format!("{panel}-metrics-{id}"),
        )
    });
    let status = Memo::new(move |_| {
        snapshot
            .with(|run| job_id.with_value(|id| run.job_status(id)))
            .unwrap_or(Status::Pending)
    });
    view! {
        <button class="job" type="button" id=id node_ref=node
            aria-labelledby=title_id aria-describedby=metrics_id
            aria-controls="timeline" aria-expanded=move || {
                selected.with(|open| job_id.with_value(|id| aria(open.as_ref() == Some(id))))
            }
            on:click=move |_| selected.update(|open| {
                job_id.with_value(|id| *open = (open.as_ref() != Some(id)).then(|| id.clone()));
            })>
            <StatusIcon status/>
            <span>
                <span class="job-name" id=title_id.clone() title=title>{title.clone()}</span>
                <span class="job-metrics" id=metrics_id.clone()><Metrics snapshot job_id/></span>
            </span>
            <span class="job-expander" aria-hidden="true">
                <ChevronDownIcon/>
            </span>
        </button>
    }
}

pub(crate) fn button_id(tab: Tab, job_id: &str) -> String {
    format!("{}-job-{job_id}", tab.panel_id())
}
