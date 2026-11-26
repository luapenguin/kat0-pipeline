// SPDX-License-Identifier: AGPL-3.0-only

use gloo_net::http::Request;
use kat0_pipeline_protocol::model::Snapshot;
use leptos::{prelude::*, task::spawn_local};
use wasm_bindgen::{JsCast, closure::Closure};
use web_sys::{Event, EventSource, MessageEvent};

/// RAII handle owning the browser `EventSource` and its JS callbacks.
/// Dropping the handle detaches handlers and closes the stream without leaking closures.
pub(crate) struct LiveStream {
    source: EventSource,
    _on_message: Closure<dyn FnMut(MessageEvent)>,
    _on_error: Closure<dyn FnMut(Event)>,
}

impl Drop for LiveStream {
    fn drop(&mut self) {
        self.source.set_onmessage(None);
        self.source.set_onerror(None);
        self.source.close();
    }
}

pub(crate) fn connect(
    snapshot: RwSignal<Snapshot>,
    disconnected: RwSignal<bool>,
) -> Option<LiveStream> {
    let Ok(source) = EventSource::new("/events") else {
        disconnected.set(true);
        spawn_local(resync_snapshot(snapshot, disconnected, None));
        return None;
    };

    let source_for_message = source.clone();
    let on_message = Closure::wrap(Box::new(move |event: MessageEvent| {
        let Some(text) = event.data().as_string() else {
            return;
        };
        let Ok(next) = serde_json::from_str::<Snapshot>(&text) else {
            return;
        };
        let finished = next.status.is_terminal();
        snapshot.set(next);
        disconnected.set(false);
        if finished {
            source_for_message.close();
        }
    }) as Box<dyn FnMut(MessageEvent)>);

    let source_for_error = source.clone();
    let on_error = Closure::wrap(Box::new(move |_event: Event| {
        if snapshot.with_untracked(|run| run.status.is_terminal()) {
            source_for_error.close();
            return;
        }
        // Distinguish permanent close (e.g. HTTP 204 during shutdown) from transient reconnects,
        // and attempt one /snapshot resync to avoid missing a terminal state.
        let closed = source_for_error.ready_state() == EventSource::CLOSED;
        if !closed {
            disconnected.set(true);
        }
        spawn_local(resync_snapshot(
            snapshot,
            disconnected,
            Some(source_for_error.clone()),
        ));
    }) as Box<dyn FnMut(Event)>);

    source.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    source.set_onerror(Some(on_error.as_ref().unchecked_ref()));

    Some(LiveStream {
        source,
        _on_message: on_message,
        _on_error: on_error,
    })
}

async fn resync_snapshot(
    snapshot: RwSignal<Snapshot>,
    disconnected: RwSignal<bool>,
    source: Option<EventSource>,
) {
    let recovered = async {
        let response = Request::get("/snapshot").send().await.ok()?;
        if !response.ok() {
            return None;
        }
        response.json::<Snapshot>().await.ok()
    }
    .await;

    if let Some(next) = recovered {
        let finished = next.status.is_terminal();
        snapshot.set(next);
        if finished {
            disconnected.set(false);
            if let Some(source) = source {
                source.close();
            }
        } else if source
            .as_ref()
            .is_some_and(|source| source.ready_state() == EventSource::CLOSED)
        {
            disconnected.set(true);
        }
    } else {
        disconnected.set(true);
    }
}
