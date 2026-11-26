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
    sse::PublishedSnapshot,
};

pub const DEFAULT_GRACE_PERIOD: Duration = Duration::from_secs(2);

/// Owns the loopback Axum/SSE server for `--ui browser` and `--ui both`.
pub struct BrowserSession {
    updates: watch::Sender<PublishedSnapshot>,
    server: LocalServer,
}

impl BrowserSession {
    pub fn start(
        initial: Arc<Snapshot>,
        bundle: Arc<Bundle>,
        open_browser: bool,
    ) -> io::Result<Self> {
        let (updates, rx) = watch::channel(PublishedSnapshot::new(initial));
        let server = LocalServer::start(move |shutdown| live_routes(rx, bundle, shutdown))?;
        let url = format!("{}/", server.origin());
        println!("[kat0-pipeline] {url}");
        if open_browser {
            open_in_default_browser(&url);
        }
        Ok(Self { updates, server })
    }

    pub fn publish(&self, snapshot: Arc<Snapshot>) {
        let _ = self.updates.send(PublishedSnapshot::new(snapshot));
    }

    pub async fn close(self, grace: Duration) -> io::Result<()> {
        if !grace.is_zero() {
            tokio::time::sleep(grace).await;
        }
        self.server.close().await
    }
}

fn open_in_default_browser(url: &str) {
    if !is_safe_loopback_url(url) {
        eprintln!("[kat0-pipeline] Refusing to open unexpected browser URL.");
        return;
    }
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

/// Validates that `url` is strictly `http://127.0.0.1:<nonzero-port>/` so OS browser launchers
/// (including `cmd /C start` on Windows) can never receive shell metacharacters or remote URLs.
fn is_safe_loopback_url(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("http://127.0.0.1:") else {
        return false;
    };
    let Some(port_str) = rest.strip_suffix('/') else {
        return false;
    };
    !port_str.is_empty()
        && port_str.bytes().all(|byte| byte.is_ascii_digit())
        && port_str.parse::<u16>().is_ok_and(|port| port != 0)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_only_strict_loopback_urls_for_browser_launch() {
        assert!(is_safe_loopback_url("http://127.0.0.1:41234/"));
        assert!(!is_safe_loopback_url("http://127.0.0.1:0/"));
        assert!(!is_safe_loopback_url("http://127.0.0.1:41234"));
        assert!(!is_safe_loopback_url("http://127.0.0.1:41234/&calc.exe/"));
        assert!(!is_safe_loopback_url("https://127.0.0.1:41234/"));
        assert!(!is_safe_loopback_url("http://example.com:80/"));
    }
}
