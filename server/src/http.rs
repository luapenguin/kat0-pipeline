// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    io,
    net::{Ipv4Addr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Request, State},
    http::{HeaderName, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use kat0_pipeline_protocol::model::Snapshot;
use tokio::{net::TcpListener, runtime::Handle, sync::watch, task::JoinHandle, time::timeout};

use crate::{
    bundle::Bundle,
    sse::{ClientPool, file_events, watch_events},
    wire,
};

const CONTENT_SECURITY_POLICY: &str = "default-src 'none'; script-src 'unsafe-inline' data: 'wasm-unsafe-eval'; style-src data: 'unsafe-inline'; connect-src 'self' data:; img-src data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'; object-src 'none'";
const SEC_FETCH_SITE: HeaderName = HeaderName::from_static("sec-fetch-site");

/// Cloneable signal triggered before Axum waits for open connections to drain.
#[derive(Clone)]
pub struct ShutdownSignal(watch::Receiver<bool>);

impl ShutdownSignal {
    pub fn is_triggered(&self) -> bool {
        *self.0.borrow()
    }

    pub async fn triggered(&mut self) {
        if *self.0.borrow_and_update() {
            return;
        }
        while self.0.changed().await.is_ok() {
            if *self.0.borrow() {
                return;
            }
        }
    }
}

pub struct LocalServer {
    origin: String,
    // Notifying or dropping the owner triggers ShutdownSignal and lets Axum drain.
    shutdown: watch::Sender<bool>,
    task: JoinHandle<io::Result<()>>,
}

impl LocalServer {
    pub fn start(build_router: impl FnOnce(ShutdownSignal) -> Router) -> io::Result<Self> {
        let runtime = Handle::try_current().map_err(io::Error::other)?;
        let listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let bound_host = listener.local_addr()?.to_string();
        let origin = format!("http://{bound_host}");
        let listener = TcpListener::from_std(listener)?;
        let (shutdown, receiver) = watch::channel(false);
        let expected_host: Arc<str> = Arc::from(bound_host);
        let app = build_router(ShutdownSignal(receiver.clone())).layer(
            middleware::from_fn_with_state(expected_host, security_guard),
        );
        let mut stopped = ShutdownSignal(receiver);
        let task = runtime.spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    stopped.triggered().await;
                })
                .await
        });
        Ok(Self {
            origin,
            shutdown,
            task,
        })
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    pub async fn close(self) -> io::Result<()> {
        let _ = self.shutdown.send(true);
        drop(self.shutdown);
        let mut task = self.task;
        match timeout(Duration::from_secs(5), &mut task).await {
            Ok(result) => result.map_err(io::Error::other)?,
            Err(_) => {
                task.abort();
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "El servidor local no se cerró a tiempo",
                ))
            }
        }
    }
}

/// Serves an in-memory live run backed by a `watch` channel with SSE (`/events`) and
/// fallback `/snapshot`.
pub fn live_routes(
    updates: watch::Receiver<Arc<Snapshot>>,
    bundle: Arc<Bundle>,
    shutdown: ShutdownSignal,
) -> Router {
    Router::new()
        .route("/", get(live_panel_page))
        .route("/snapshot", get(live_snapshot))
        .route("/events", get(live_sse))
        .with_state(LiveState {
            updates,
            bundle,
            shutdown,
            clients: ClientPool::default(),
        })
}

#[derive(Clone)]
struct LiveState {
    updates: watch::Receiver<Arc<Snapshot>>,
    bundle: Arc<Bundle>,
    shutdown: ShutdownSignal,
    clients: ClientPool,
}

async fn live_panel_page(State(state): State<LiveState>) -> impl IntoResponse {
    let snapshot = state.updates.borrow().clone();
    render_panel(&state.bundle, &snapshot)
}

async fn live_snapshot(State(state): State<LiveState>) -> impl IntoResponse {
    let snapshot = state.updates.borrow().clone();
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(snapshot.as_ref().clone()),
    )
}

async fn live_sse(State(state): State<LiveState>) -> Response {
    watch_events(state.updates, state.shutdown, &state.clients)
}

/// The client keeps the initial definition; a live publisher may change only the rows and clock.
pub fn file_routes(
    path: PathBuf,
    initial: Snapshot,
    bundle: Arc<Bundle>,
    shutdown: ShutdownSignal,
) -> Router {
    Router::new()
        .route("/", get(file_panel_page))
        .route("/snapshot", get(file_snapshot))
        .route("/events", get(file_sse))
        .with_state(FileState {
            path: path.into(),
            initial: Arc::new(initial),
            bundle,
            shutdown,
            clients: ClientPool::default(),
        })
}

#[derive(Clone)]
struct FileState {
    path: Arc<Path>,
    initial: Arc<Snapshot>,
    bundle: Arc<Bundle>,
    shutdown: ShutdownSignal,
    clients: ClientPool,
}

async fn read_matching(state: &FileState) -> Option<Snapshot> {
    match wire::read(&state.path).await {
        Ok(snapshot) if snapshot.definition() == state.initial.definition() => Some(snapshot),
        _ => None,
    }
}

async fn file_panel_page(State(state): State<FileState>) -> impl IntoResponse {
    let current = read_matching(&state).await;
    let snapshot = current.as_ref().unwrap_or(state.initial.as_ref());
    render_panel(&state.bundle, snapshot)
}

async fn file_snapshot(State(state): State<FileState>) -> impl IntoResponse {
    let current = match read_matching(&state).await {
        Some(snapshot) => Ok(Json(snapshot)),
        None => Err(StatusCode::SERVICE_UNAVAILABLE),
    };
    ([(header::CACHE_CONTROL, "no-store")], current)
}

async fn file_sse(State(state): State<FileState>) -> Response {
    file_events(state.path, state.initial, state.shutdown, &state.clients)
}

fn render_panel(bundle: &Bundle, snapshot: &Snapshot) -> Response {
    let live = !snapshot.status.is_terminal();
    let response = match bundle.render(snapshot, live) {
        Ok(html) => Ok(Html(html)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    };
    ([(header::CACHE_CONTROL, "no-store")], response).into_response()
}

/// Enforces strict loopback `Host` matching (DNS rebinding protection), blocks cross-site
/// browser reads on live endpoints, and attaches baseline security headers to every response.
async fn security_guard(
    State(expected_host): State<Arc<str>>,
    request: Request,
    next: Next,
) -> Response {
    let host_ok = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|host| host.eq_ignore_ascii_case(&expected_host));
    if !host_ok {
        return apply_security_headers(StatusCode::MISDIRECTED_REQUEST.into_response());
    }

    let path = request.uri().path();
    if matches!(path, "/events" | "/snapshot")
        && let Some(fetch_site) = request
            .headers()
            .get(&SEC_FETCH_SITE)
            .and_then(|value| value.to_str().ok())
        && !matches!(fetch_site, "same-origin" | "none")
    {
        return apply_security_headers(StatusCode::FORBIDDEN.into_response());
    }

    apply_security_headers(next.run(request).await)
}

fn apply_security_headers(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    response
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use kat0_pipeline_protocol::model::{Column, Definition, Job, JobState, Row, Status};
    use serde_json::{Value, json};
    use tokio::{
        fs,
        io::{AsyncBufReadExt as _, AsyncReadExt, AsyncWriteExt, BufReader},
        net::TcpStream,
        sync::Notify,
    };

    use super::*;
    use crate::sse::MAX_CLIENTS;

    async fn request(server: &LocalServer, path: &str) -> String {
        let address = server.origin().strip_prefix("http://").unwrap();
        let message =
            format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
        timeout(Duration::from_secs(5), async {
            let mut client = TcpStream::connect(address).await?;
            client.write_all(message.as_bytes()).await?;
            let mut response = String::new();
            client.read_to_string(&mut response).await?;
            Ok::<_, io::Error>(response)
        })
        .await
        .unwrap()
        .unwrap()
    }

    async fn open_sse(server: &LocalServer) -> (String, BufReader<TcpStream>) {
        let address = server.origin().strip_prefix("http://").unwrap();
        let message = format!(
            "GET /events HTTP/1.1\r\nHost: {address}\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n"
        );
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(message.as_bytes()).await.unwrap();
        let mut reader = BufReader::new(client);
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            timeout(Duration::from_secs(5), reader.read_line(&mut line))
                .await
                .unwrap()
                .unwrap();
            headers.push_str(&line);
            if line == "\r\n" || line.is_empty() {
                break;
            }
        }
        (headers, reader)
    }

    async fn next_sse_json(reader: &mut BufReader<TcpStream>) -> Option<Value> {
        loop {
            let mut line = String::new();
            let read = timeout(Duration::from_secs(5), reader.read_line(&mut line))
                .await
                .unwrap()
                .unwrap();
            if read == 0 {
                return None;
            }
            if let Some(payload) = line.strip_prefix("data: ") {
                return Some(serde_json::from_str(payload.trim()).unwrap());
            }
        }
    }

    async fn replace(path: &Path, doc: &Value) {
        let staged = path.with_extension("tmp");
        fs::write(&staged, serde_json::to_vec(doc).unwrap())
            .await
            .unwrap();
        fs::rename(staged, path).await.unwrap();
    }

    fn minimal_snapshot(status: Status, elapsed_ms: u64) -> Snapshot {
        let definition = Definition {
            title: "Live run".into(),
            description: "SSE test".into(),
            columns: vec![Column {
                title: "Verify".into(),
                jobs: vec![Job {
                    id: "check".into(),
                    title: "Check".into(),
                    is_test: true,
                    dependencies: vec![],
                    steps: vec![],
                    cases: vec![],
                }],
            }],
        };
        let row = match status {
            Status::Running => Row {
                status: Status::Running,
                started_ms: Some(0),
                duration_ms: None,
            },
            Status::Passed | Status::Failed => Row {
                status,
                started_ms: Some(0),
                duration_ms: Some(elapsed_ms),
            },
            _ => Row::pending(),
        };
        Snapshot::from_parts(
            definition,
            status,
            elapsed_ms,
            BTreeMap::from([(
                "check".into(),
                JobState {
                    row,
                    steps: BTreeMap::new(),
                    cases: BTreeMap::new(),
                },
            )]),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn live_sse_streams_watch_updates_closes_on_terminal_and_serves_late_clients() {
        let (tx, rx) = watch::channel(Arc::new(minimal_snapshot(Status::Running, 100)));
        let bundle = Arc::new(Bundle::for_test());
        let server = LocalServer::start(move |shutdown| live_routes(rx, bundle, shutdown)).unwrap();

        let (headers, mut stream) = open_sse(&server).await;
        assert!(headers.starts_with("HTTP/1.1 200"));
        assert!(headers.lines().any(|line| {
            line.to_ascii_lowercase()
                .starts_with("content-type: text/event-stream")
        }));
        assert!(
            headers
                .lines()
                .any(|line| line.eq_ignore_ascii_case("cache-control: no-store"))
        );

        let first = next_sse_json(&mut stream).await.unwrap();
        assert_eq!(first["status"], "running");
        assert_eq!(first["elapsed_ms"], 100);

        tx.send(Arc::new(minimal_snapshot(Status::Running, 250)))
            .unwrap();
        let second = next_sse_json(&mut stream).await.unwrap();
        assert_eq!(second["elapsed_ms"], 250);

        tx.send(Arc::new(minimal_snapshot(Status::Passed, 400)))
            .unwrap();
        let terminal = next_sse_json(&mut stream).await.unwrap();
        assert_eq!(terminal["status"], "passed");
        assert_eq!(terminal["elapsed_ms"], 400);
        // Stream must close automatically after delivering the terminal frame.
        assert!(next_sse_json(&mut stream).await.is_none());

        // Late-connecting SSE client still receives the terminal frame once and closes.
        let (late_headers, mut late_stream) = open_sse(&server).await;
        assert!(late_headers.starts_with("HTTP/1.1 200"));
        let late_frame = next_sse_json(&mut late_stream).await.unwrap();
        assert_eq!(late_frame["status"], "passed");
        assert!(next_sse_json(&mut late_stream).await.is_none());

        // GET / after terminal renders data-live="false".
        let page = request(&server, "/").await;
        assert!(page.contains(r#"id="panel" data-live="false""#));

        timeout(Duration::from_secs(1), server.close())
            .await
            .expect("server closes promptly")
            .unwrap();
    }

    #[tokio::test]
    async fn live_sse_enforces_client_cap_and_closes_promptly_on_server_shutdown() {
        let (_tx, rx) = watch::channel(Arc::new(minimal_snapshot(Status::Running, 100)));
        let bundle = Arc::new(Bundle::for_test());
        let server = LocalServer::start(move |shutdown| live_routes(rx, bundle, shutdown)).unwrap();

        let mut streams = Vec::with_capacity(MAX_CLIENTS);
        for _ in 0..MAX_CLIENTS {
            let (headers, mut stream) = open_sse(&server).await;
            assert!(headers.starts_with("HTTP/1.1 200"));
            assert_eq!(
                next_sse_json(&mut stream).await.unwrap()["status"],
                "running"
            );
            streams.push(stream);
        }

        // Next SSE client exceeds MAX_CLIENTS and gets 503.
        let overflow = request(&server, "/events").await;
        assert!(overflow.starts_with("HTTP/1.1 503"));

        // Dropping one client frees its slot via RAII guard.
        streams.pop();
        let mut replacement = None;
        for _ in 0..20 {
            let (headers, stream) = open_sse(&server).await;
            if headers.starts_with("HTTP/1.1 200") {
                replacement = Some(stream);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut replacement = replacement.expect("dropping a client should free an SSE slot");
        assert_eq!(
            next_sse_json(&mut replacement).await.unwrap()["status"],
            "running"
        );
        streams.push(replacement);

        // LocalServer::close() must terminate all active SSE streams well under 1 second.
        timeout(Duration::from_secs(1), server.close())
            .await
            .expect("close must terminate open SSE streams promptly")
            .unwrap();
        for mut stream in streams {
            assert!(next_sse_json(&mut stream).await.is_none());
        }
    }

    #[tokio::test]
    async fn watch_events_returns_204_once_shutdown_is_triggered() {
        let (_tx, rx) = watch::channel(Arc::new(minimal_snapshot(Status::Running, 100)));
        let (stop_tx, stop_rx) = watch::channel(true);
        let _keep = stop_tx;
        let response = watch_events(rx, ShutdownSignal(stop_rx), &ClientPool::default());
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn serves_dynamic_panel_and_file_updates_only_with_initial_definition() {
        let scratch = tempfile::tempdir().unwrap();
        let path = scratch.path().join("state.json");
        let bytes = include_bytes!("../fixtures/web_regression.json");
        let mut doc: Value = serde_json::from_slice(bytes).unwrap();
        fs::write(&path, bytes).await.unwrap();
        let initial = wire::decode(bytes).unwrap();
        let bundle = Arc::new(Bundle::for_test());
        let server = LocalServer::start(|shutdown| {
            file_routes(path.clone(), initial.clone(), Arc::clone(&bundle), shutdown)
        })
        .unwrap();
        assert!(server.origin().starts_with("http://127.0.0.1:"));

        let response = request(&server, "/").await;
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(
            response
                .lines()
                .any(|line| line.eq_ignore_ascii_case("cache-control: no-store"))
        );
        let body = response.split_once("\r\n\r\n").unwrap().1;
        assert!(body.contains(r#"id="panel" data-live="true""#));
        assert!(body.contains(r#""elapsed_ms":48300"#));

        assert!(
            request(&server, "/demo/counter")
                .await
                .starts_with("HTTP/1.1 404")
        );

        let read = async || {
            let response = request(&server, "/snapshot").await;
            assert!(response.starts_with("HTTP/1.1 200"));
            assert!(
                response
                    .lines()
                    .any(|line| line.eq_ignore_ascii_case("cache-control: no-store"))
            );
            serde_json::from_str::<Value>(response.split_once("\r\n\r\n").unwrap().1).unwrap()
        };
        assert_eq!(read().await, doc["snapshot"]);

        let (sse_headers, mut sse_stream) = open_sse(&server).await;
        assert!(sse_headers.starts_with("HTTP/1.1 200"));
        assert_eq!(
            next_sse_json(&mut sse_stream).await.unwrap()["elapsed_ms"],
            48_300
        );

        doc["snapshot"]["elapsed_ms"] = json!(48_500);
        replace(&path, &doc).await;
        assert_eq!(read().await, doc["snapshot"]);
        assert_eq!(
            next_sse_json(&mut sse_stream).await.unwrap()["elapsed_ms"],
            48_500
        );
        let updated_page = request(&server, "/").await;
        assert!(updated_page.contains(r#""elapsed_ms":48500"#));
        assert!(updated_page.contains(r#"id="panel" data-live="true""#));

        // Transition to a terminal snapshot and verify GET / switches data-live to false
        // and SSE delivers the final frame and closes.
        let mut terminal = doc.clone();
        terminal["snapshot"]["status"] = json!("failed");
        terminal["snapshot"]["elapsed_ms"] = json!(50_000);
        for (job_id, job) in terminal["snapshot"]["jobs"].as_object_mut().unwrap() {
            let row = job["row"].as_object_mut().unwrap();
            match row["status"].as_str().unwrap() {
                "pending" => {
                    row["status"] = json!("skipped");
                    for step in job["steps"].as_object_mut().unwrap().values_mut() {
                        step["status"] = json!("skipped");
                    }
                }
                "running" => {
                    let start = row["started_ms"].as_u64().unwrap();
                    row["status"] = json!("failed");
                    row["duration_ms"] = json!(50_000 - start);
                    for step in job["steps"].as_object_mut().unwrap().values_mut() {
                        match step["status"].as_str().unwrap() {
                            "running" => {
                                let step_start = step["started_ms"].as_u64().unwrap();
                                step["status"] = json!("failed");
                                step["duration_ms"] = json!(50_000 - step_start);
                            }
                            "pending" => {
                                step["status"] = json!("skipped");
                            }
                            _ => {}
                        }
                    }
                }
                _ => {
                    let _ = job_id;
                }
            }
        }
        replace(&path, &terminal).await;
        let sse_terminal = next_sse_json(&mut sse_stream).await.unwrap();
        assert_eq!(sse_terminal["status"], "failed");
        assert_eq!(sse_terminal["elapsed_ms"], 50_000);
        assert!(next_sse_json(&mut sse_stream).await.is_none());

        let terminal_page = request(&server, "/").await;
        assert!(terminal_page.starts_with("HTTP/1.1 200"));
        assert!(terminal_page.contains(r#"id="panel" data-live="false""#));
        assert!(terminal_page.contains(r#""elapsed_ms":50000"#));

        let mut wrong = doc.clone();
        wrong["snapshot"]["definition"]["title"] = json!("Different execution");
        replace(&path, &wrong).await;
        assert!(
            request(&server, "/snapshot")
                .await
                .starts_with("HTTP/1.1 503")
        );
        fs::write(&path, b"{}").await.unwrap();
        assert!(
            request(&server, "/snapshot")
                .await
                .starts_with("HTTP/1.1 503")
        );
        replace(&path, &doc).await;
        assert_eq!(read().await, doc["snapshot"]);
        let address = server.origin().strip_prefix("http://").unwrap().to_owned();
        server.close().await.unwrap();
        assert!(TcpStream::connect(address).await.is_err());
    }

    #[tokio::test]
    async fn close_notifies_long_lived_handlers_before_timeout() {
        let entered = Arc::new(Notify::new());
        let entered_handler = Arc::clone(&entered);
        let server = LocalServer::start(move |shutdown| {
            Router::new().route(
                "/hold",
                get(move || {
                    let entered = Arc::clone(&entered_handler);
                    let mut shutdown = shutdown.clone();
                    async move {
                        assert!(!shutdown.is_triggered());
                        entered.notify_one();
                        shutdown.triggered().await;
                        assert!(shutdown.is_triggered());
                        StatusCode::NO_CONTENT
                    }
                }),
            )
        })
        .unwrap();

        let address = server.origin().strip_prefix("http://").unwrap().to_owned();
        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(&address).await.unwrap();
            let message =
                format!("GET /hold HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
            stream.write_all(message.as_bytes()).await.unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            response
        });

        timeout(Duration::from_secs(2), entered.notified())
            .await
            .expect("handler should receive request");
        timeout(Duration::from_secs(2), server.close())
            .await
            .expect("close should finish promptly via ShutdownSignal")
            .unwrap();
        let response = client.await.unwrap();
        assert!(response.starts_with("HTTP/1.1 204"));
    }

    async fn raw_request(server: &LocalServer, raw_http: &str) -> String {
        let address = server.origin().strip_prefix("http://").unwrap();
        timeout(Duration::from_secs(5), async {
            let mut client = TcpStream::connect(address).await?;
            client.write_all(raw_http.as_bytes()).await?;
            let mut response = String::new();
            client.read_to_string(&mut response).await?;
            Ok::<_, io::Error>(response)
        })
        .await
        .unwrap()
        .unwrap()
    }

    #[tokio::test]
    async fn enforces_security_headers_host_validation_and_fetch_metadata() {
        let (_tx, rx) = watch::channel(Arc::new(minimal_snapshot(Status::Running, 100)));
        let bundle = Arc::new(Bundle::for_test());
        let server = LocalServer::start(move |shutdown| live_routes(rx, bundle, shutdown)).unwrap();
        let host = server.origin().strip_prefix("http://").unwrap();

        // Valid request carries full security header baseline.
        let ok = request(&server, "/").await;
        assert!(ok.starts_with("HTTP/1.1 200"));
        let lower = ok.to_ascii_lowercase();
        assert!(lower.contains("cache-control: no-store"));
        assert!(lower.contains("x-content-type-options: nosniff"));
        assert!(lower.contains("x-frame-options: deny"));
        assert!(lower.contains("referrer-policy: no-referrer"));
        assert!(lower.contains("content-security-policy: default-src 'none'"));

        // Mismatched Host header (DNS rebinding) is rejected with 421.
        let rebind = raw_request(
            &server,
            "GET / HTTP/1.1\r\nHost: evil.example:1234\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(rebind.starts_with("HTTP/1.1 421"));
        assert!(
            rebind
                .to_ascii_lowercase()
                .contains("x-frame-options: deny")
        );

        // Cross-site browser fetch to /snapshot or /events is rejected with 403.
        for path in ["/snapshot", "/events"] {
            let cross = raw_request(
                &server,
                &format!(
                    "GET {path} HTTP/1.1\r\nHost: {host}\r\nSec-Fetch-Site: cross-site\r\nConnection: close\r\n\r\n"
                ),
            )
            .await;
            assert!(
                cross.starts_with("HTTP/1.1 403"),
                "expected 403 for cross-site {path}, got: {cross}"
            );
        }

        // Same-origin fetch to /snapshot succeeds.
        let same_origin = raw_request(
            &server,
            &format!(
                "GET /snapshot HTTP/1.1\r\nHost: {host}\r\nSec-Fetch-Site: same-origin\r\nConnection: close\r\n\r\n"
            ),
        )
        .await;
        assert!(same_origin.starts_with("HTTP/1.1 200"));

        server.close().await.unwrap();
    }
}
