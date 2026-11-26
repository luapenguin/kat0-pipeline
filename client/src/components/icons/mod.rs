// SPDX-License-Identifier: AGPL-3.0-only

use kat0_pipeline_protocol::model::Status;
use leptos::prelude::*;

use super::status_class;

#[component]
pub(super) fn StatusIcon(#[prop(into)] status: Signal<Status>) -> impl IntoView {
    view! {
        <svg class=move || format!("job-icon {}", status_class(status.get())) viewBox="0 0 32 32"
            fill="none" stroke="currentColor" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round"
            aria-hidden="true" focusable="false">
            <circle class="job-icon-ring" cx="16" cy="16" r="14.5"/>
            <path class="job-icon-mark" d=move || status_path(status.get())/>
        </svg>
    }
}

#[component]
pub(super) fn ChevronDownIcon() -> impl IntoView {
    view! {
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
            stroke-linecap="round" stroke-linejoin="round" focusable="false">
            <path d="m6.5 9 5.5 5.5L17.5 9"/>
        </svg>
    }
}

fn status_path(status: Status) -> &'static str {
    match status {
        Status::Pending => "M16 9.5v7h5",
        Status::Running => concat!(
            "M6 14a10 10 0 0 1 17.5-4.6L26 7v8h-8l3.3-3.3A7 7 0 0 0 9.2 14H6Z ",
            "M26 18a10 10 0 0 1-17.5 4.6L6 25v-8h8l-3.3 3.3A7 7 0 0 0 22.8 18H26Z",
        ),
        Status::Passed => "m10.5 16 3.7 3.7 7.3-7.3",
        Status::Failed => "M16 9.5v7m0 4.5v1",
        Status::Skipped => "m11 11 10 10",
        _ => "M16 9.5v7h5",
    }
}
