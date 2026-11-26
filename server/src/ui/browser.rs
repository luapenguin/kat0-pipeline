// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    io,
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};

use kat0_pipeline_protocol::model::Snapshot;
use tokio::sync::watch;

use crate::{
    bundle::Bundle,
    http::{LocalServer, live_routes},
    ipc::EVENTS_SOCK_ENV,
};

pub const DEFAULT_GRACE_PERIOD: Duration = Duration::from_secs(2);

/// Owns the loopback Axum/SSE server for `--ui browser` and `--ui both`.
pub struct BrowserSession {
    updates: watch::Sender<Arc<Snapshot>>,
    server: LocalServer,
}

impl BrowserSession {
    pub fn start(
        initial: Arc<Snapshot>,
        bundle: Arc<Bundle>,
        open_browser: bool,
    ) -> io::Result<Self> {
        let (updates, rx) = watch::channel(initial);
        let server = LocalServer::start(move |shutdown| live_routes(rx, bundle, shutdown))?;
        let url = format!("{}/", server.origin());
        println!("[kat0-pipeline] {url}");
        if open_browser {
            open_in_default_browser(&url);
        }
        Ok(Self { updates, server })
    }

    pub fn origin(&self) -> &str {
        self.server.origin()
    }

    pub fn publish(&self, snapshot: Arc<Snapshot>) {
        let _ = self.updates.send(snapshot);
    }

    pub async fn close(self, grace: Duration) -> io::Result<()> {
        if !grace.is_zero() {
            tokio::time::sleep(grace).await;
        }
        self.server.close().await
    }
}

fn open_in_default_browser(url: &str) {
    let mut command = browser_command(url);
    let spawned = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .env_remove(EVENTS_SOCK_ENV)
        .spawn();
    if spawned.is_err() {
        eprintln!("[kat0-pipeline] Open the URL above in your browser.");
    }
}

#[cfg(target_os = "macos")]
fn browser_command(url: &str) -> Command {
    let mut cmd = Command::new("open");
    cmd.arg(url);
    cmd
}

#[cfg(target_os = "windows")]
fn browser_command(url: &str) -> Command {
    let mut cmd = Command::new("cmd");
    cmd.args(["/C", "start", "", url]);
    cmd
}

#[cfg(not(any(target_os = "macos", target_os = "windows")))]
fn browser_command(url: &str) -> Command {
    let mut cmd = Command::new("xdg-open");
    cmd.arg(url);
    cmd
}
