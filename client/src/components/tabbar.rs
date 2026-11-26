// SPDX-License-Identifier: AGPL-3.0-only

use leptos::prelude::*;
use wasm_bindgen::JsCast;

use super::aria;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tab {
    Pipeline,
    Jobs,
    Failed,
    Tests,
}

impl Tab {
    const ALL: [Self; 4] = [Self::Pipeline, Self::Jobs, Self::Failed, Self::Tests];

    pub(crate) fn id(self) -> &'static str {
        match self {
            Self::Pipeline => "tab-pipeline",
            Self::Jobs => "tab-jobs",
            Self::Failed => "tab-failed",
            Self::Tests => "tab-tests",
        }
    }

    pub(crate) fn panel_id(self) -> &'static str {
        match self {
            Self::Pipeline => "panel-pipeline",
            Self::Jobs => "panel-jobs",
            Self::Failed => "panel-failed",
            Self::Tests => "panel-tests",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Pipeline => "Pipeline",
            Self::Jobs => "Jobs",
            Self::Failed => "Failed Jobs",
            Self::Tests => "Tests",
        }
    }
}

#[component]
pub(crate) fn TabBar(
    active: RwSignal<Tab>,
    selected: RwSignal<Option<String>>,
    failed: Memo<usize>,
    job_count: usize,
    test_count: Memo<usize>,
) -> impl IntoView {
    let select = move |tab| {
        if active.get_untracked() != tab {
            selected.set(None);
            active.set(tab);
        }
    };
    view! {
        <div class="tabs" role="tablist" aria-label="Pipeline report">
            {Tab::ALL.into_iter().enumerate().map(|(index, tab)| view! {
                <button class="tab" type="button" id=tab.id() role="tab" aria-controls=tab.panel_id()
                    aria-selected=move || aria(active.get() == tab)
                    tabindex=move || if active.get() == tab { "0" } else { "-1" }
                    on:click=move |_| select(tab)
                    on:keydown=move |event: web_sys::KeyboardEvent| {
                        let count = Tab::ALL.len();
                        let target = match event.key().as_str() {
                            "ArrowLeft" => (index + count - 1) % count,
                            "ArrowRight" => (index + 1) % count,
                            "Home" => 0,
                            "End" => count - 1,
                            _ => return,
                        };
                        event.prevent_default();
                        select(Tab::ALL[target]);
                        focus(Tab::ALL[target].id());
                    }>
                    {tab.label()}
                    {(tab != Tab::Pipeline).then(|| view! {
                        <span class="count">{move || match tab {
                            Tab::Jobs => job_count,
                            Tab::Failed => failed.get(),
                            Tab::Tests => test_count.get(),
                            Tab::Pipeline => 0,
                        }}</span>
                    })}
                </button>
            }).collect_view()}
        </div>
    }
}

pub(crate) fn focus(id: &str) {
    if let Some(button) = document()
        .get_element_by_id(id)
        .and_then(|element| element.dyn_into::<web_sys::HtmlElement>().ok())
    {
        let _ = button.focus();
    }
}
