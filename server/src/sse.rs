// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    convert::Infallible,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    http::{StatusCode, header},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures_util::stream;
use kat0_pipeline_protocol::model::Snapshot;
use tokio::{
    sync::watch,
    time::{self, MissedTickBehavior},
};

use crate::{http::ShutdownSignal, wire};

pub(crate) const MAX_CLIENTS: usize = 8;
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);
const FILE_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Pre-serialized snapshot payload shared across live HTTP and SSE consumers so each
/// snapshot transition is encoded to JSON once instead of once per connected client.
#[derive(Clone)]
pub struct PublishedSnapshot {
    snapshot: Arc<Snapshot>,
    json: Arc<str>,
}

impl PublishedSnapshot {
    pub fn new(snapshot: Arc<Snapshot>) -> Self {
        let json = serde_json::to_string(snapshot.as_ref())
            .map(Arc::<str>::from)
            .unwrap_or_else(|_| Arc::from("{}"));
        Self { snapshot, json }
    }

    pub fn json(&self) -> &str {
        self.json.as_ref()
    }

    pub fn is_terminal(&self) -> bool {
        self.snapshot.status.is_terminal()
    }
}

#[derive(Clone, Default)]
pub(crate) struct ClientPool(Arc<AtomicUsize>);

impl ClientPool {
    pub(crate) fn try_acquire(&self) -> Option<ClientGuard> {
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            if current >= MAX_CLIENTS {
                return None;
            }
            match self.0.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(ClientGuard {
                        active: Arc::clone(&self.0),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }
}

pub(crate) struct ClientGuard {
    active: Arc<AtomicUsize>,
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

fn event_for(published: &PublishedSnapshot) -> Event {
    Event::default().data(published.json())
}

fn no_store_sse<S>(stream: S) -> Response
where
    S: futures_util::Stream<Item = Result<Event, Infallible>> + Send + 'static,
{
    let sse = Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(KEEP_ALIVE_INTERVAL)
            .text("keep-alive"),
    );
    ([(header::CACHE_CONTROL, "no-store")], sse).into_response()
}

/// Spawns a single shared file-polling task backing all SSE subscribers of `serve <file>`.
///
/// Reads and validates the snapshot file at most once per `FILE_POLL_INTERVAL` regardless of
/// subscriber count, publishing pre-serialized [`PublishedSnapshot`] updates over `watch`,
/// and exits cleanly when `shutdown` triggers.
pub(crate) fn spawn_file_poller(
    path: Arc<Path>,
    initial: Arc<Snapshot>,
    mut shutdown: ShutdownSignal,
) -> watch::Receiver<PublishedSnapshot> {
    let (tx, rx) = watch::channel(PublishedSnapshot::new(Arc::clone(&initial)));
    tokio::spawn(async move {
        let mut last: Option<Snapshot> = None;
        let mut ticker = time::interval(FILE_POLL_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = shutdown.triggered() => break,
                _ = ticker.tick() => {
                    if let Ok(next) = wire::read(&path).await
                        && next.definition() == initial.definition()
                        && last.as_ref() != Some(&next)
                    {
                        last = Some(next.clone());
                        if tx.send(PublishedSnapshot::new(Arc::new(next))).is_err() {
                            break;
                        }
                    }
                }
            }
        }
    });
    rx
}

/// Streams pre-serialized snapshots from a shared `watch` channel.
///
/// - Returns `204 No Content` if shutdown has already started (stops EventSource reconnects).
/// - Returns `503 Service Unavailable` if `MAX_CLIENTS` are already connected.
/// - Emits the current snapshot immediately; closes right after delivering a terminal snapshot
///   or when `shutdown` triggers.
pub(crate) fn watch_events(
    mut updates: watch::Receiver<PublishedSnapshot>,
    shutdown: ShutdownSignal,
    clients: &ClientPool,
) -> Response {
    if shutdown.is_triggered() {
        return (
            [(header::CACHE_CONTROL, "no-store")],
            StatusCode::NO_CONTENT,
        )
            .into_response();
    }
    let Some(guard) = clients.try_acquire() else {
        return (
            [(header::CACHE_CONTROL, "no-store")],
            StatusCode::SERVICE_UNAVAILABLE,
        )
            .into_response();
    };

    // Mark current value seen and emit it as the first frame.
    let initial = updates.borrow_and_update().clone();
    let stream = stream::unfold(
        (Some(initial), updates, shutdown, guard, false),
        |(pending, mut updates, mut shutdown, guard, done)| async move {
            if done {
                return None;
            }
            if let Some(published) = pending {
                let terminal = published.is_terminal();
                let event = event_for(&published);
                return Some((Ok(event), (None, updates, shutdown, guard, terminal)));
            }
            tokio::select! {
                biased;
                () = shutdown.triggered() => None,
                changed = updates.changed() => {
                    if changed.is_err() {
                        return None;
                    }
                    let published = updates.borrow_and_update().clone();
                    let terminal = published.is_terminal();
                    let event = event_for(&published);
                    Some((Ok(event), (None, updates, shutdown, guard, terminal)))
                }
            }
        },
    );
    no_store_sse(stream)
}
