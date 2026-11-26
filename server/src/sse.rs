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

fn event_for(snapshot: &Snapshot) -> Option<Event> {
    Event::default().json_data(snapshot).ok()
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

/// Streams snapshots from an in-memory `watch` channel.
///
/// - Returns `204 No Content` if shutdown has already started (stops EventSource reconnects).
/// - Returns `503 Service Unavailable` if `MAX_CLIENTS` are already connected.
/// - Emits the current snapshot immediately; closes right after delivering a terminal snapshot
///   or when `shutdown` triggers.
pub(crate) fn watch_events(
    mut updates: watch::Receiver<Arc<Snapshot>>,
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
            if let Some(snapshot) = pending {
                let terminal = snapshot.status.is_terminal();
                let event = event_for(snapshot.as_ref())?;
                return Some((Ok(event), (None, updates, shutdown, guard, terminal)));
            }
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.triggered() => return None,
                    changed = updates.changed() => {
                        if changed.is_err() {
                            return None;
                        }
                        let snapshot = updates.borrow_and_update().clone();
                        let terminal = snapshot.status.is_terminal();
                        if let Some(event) = event_for(snapshot.as_ref()) {
                            return Some((Ok(event), (None, updates, shutdown, guard, terminal)));
                        }
                    }
                }
            }
        },
    );
    no_store_sse(stream)
}

/// Streams snapshots from a v2 snapshot file (`serve <file>`) while preserving the same
/// SSE lifecycle contract (immediate first frame, terminal close, `204` on shutdown, client cap).
pub(crate) fn file_events(
    path: Arc<Path>,
    initial: Arc<Snapshot>,
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

    let mut ticker = time::interval(FILE_POLL_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let stream = stream::unfold(
        (
            path,
            initial,
            None::<Snapshot>,
            ticker,
            shutdown,
            guard,
            false,
        ),
        |(path, initial, mut last, mut ticker, mut shutdown, guard, done)| async move {
            if done {
                return None;
            }
            if last.is_none() {
                let first = match wire::read(&path).await {
                    Ok(snapshot) if snapshot.definition() == initial.definition() => snapshot,
                    _ => initial.as_ref().clone(),
                };
                let terminal = first.status.is_terminal();
                let event = event_for(&first)?;
                last = Some(first);
                return Some((
                    Ok(event),
                    (path, initial, last, ticker, shutdown, guard, terminal),
                ));
            }
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.triggered() => return None,
                    _ = ticker.tick() => {
                        if let Ok(next) = wire::read(&path).await
                            && next.definition() == initial.definition()
                            && last.as_ref() != Some(&next)
                        {
                            let terminal = next.status.is_terminal();
                            if let Some(event) = event_for(&next) {
                                last = Some(next);
                                return Some((
                                    Ok(event),
                                    (path, initial, last, ticker, shutdown, guard, terminal),
                                ));
                            }
                        }
                    }
                }
            }
        },
    );
    no_store_sse(stream)
}
