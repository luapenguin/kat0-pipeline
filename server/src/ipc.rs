// SPDX-License-Identifier: AGPL-3.0-only

use std::{io, path::Path};

use kat0_pipeline_protocol::v3::Event;
use tokio::sync::mpsc;

/// Environment variable passed to the orchestrated child process pointing to the NDJSON v3 IPC endpoint.
pub const EVENTS_SOCK_ENV: &str = "KAT0_PIPELINE_EVENTS_SOCK";

pub enum IncomingMessage {
    Event(Event),
    ProtocolError(String),
}

/// Platform-isolated IPC event receiver owned by the orchestrator.
///
/// On Unix platforms this binds a private `0700` Unix domain socket; dropping or closing
/// the handle removes the socket and temporary directory automatically.
pub struct EventSocket {
    inner: transport::Endpoint,
}

impl EventSocket {
    pub fn bind() -> io::Result<(Self, mpsc::Receiver<IncomingMessage>)> {
        let (inner, rx) = transport::Endpoint::bind()?;
        Ok((Self { inner }, rx))
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// Stops accepting new connections, drains active readers, and cleans up transport state.
    pub async fn close(self) {
        self.inner.close().await;
    }
}

#[cfg(unix)]
mod transport {
    use std::{
        fs::{self, Permissions},
        io,
        os::unix::fs::PermissionsExt as _,
        path::{Path, PathBuf},
        time::Duration,
    };

    use kat0_pipeline_protocol::{limits::MAX_RUN_STARTED_LINE_BYTES, v3::decode_line};
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncBufRead, AsyncBufReadExt as _, BufReader},
        net::{UnixListener, UnixStream},
        sync::{mpsc, watch},
        task::{JoinHandle, JoinSet},
        time::timeout,
    };

    use super::IncomingMessage;

    const MAX_WIRE_LINE_BYTES: usize = MAX_RUN_STARTED_LINE_BYTES + 2;
    const CHANNEL_CAPACITY: usize = 256;
    const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

    pub(super) struct Endpoint {
        _dir: TempDir,
        path: PathBuf,
        stop_accept: watch::Sender<bool>,
        accept_task: JoinHandle<()>,
    }

    impl Endpoint {
        pub(super) fn bind() -> io::Result<(Self, mpsc::Receiver<IncomingMessage>)> {
            let dir = tempfile::Builder::new().prefix("pipeline-ipc-").tempdir()?;
            fs::set_permissions(dir.path(), Permissions::from_mode(0o700))?;
            let path = dir.path().join("events.sock");
            let listener = UnixListener::bind(&path)?;
            let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
            let (stop_accept, mut stop_rx) = watch::channel(false);

            let accept_task = tokio::spawn(async move {
                let mut readers = JoinSet::new();
                loop {
                    tokio::select! {
                        biased;
                        changed = stop_rx.changed() => {
                            if changed.is_err() || *stop_rx.borrow() {
                                break;
                            }
                        }
                        accepted = listener.accept() => match accepted {
                            Ok((stream, _addr)) => {
                                let tx = tx.clone();
                                readers.spawn(read_connection(stream, tx));
                            }
                            Err(error) => {
                                let _ = tx.send(IncomingMessage::ProtocolError(format!(
                                    "Failed to accept IPC connection: {error}"
                                ))).await;
                                break;
                            }
                        }
                    }
                }
                // Drain active connections briefly so in-flight terminal events are not dropped.
                let _ = timeout(DRAIN_TIMEOUT, async {
                    while readers.join_next().await.is_some() {}
                })
                .await;
                readers.abort_all();
            });

            Ok((
                Self {
                    _dir: dir,
                    path,
                    stop_accept,
                    accept_task,
                },
                rx,
            ))
        }

        pub(super) fn path(&self) -> &Path {
            &self.path
        }

        pub(super) async fn close(self) {
            let _ = self.stop_accept.send(true);
            let _ = self.accept_task.await;
            drop(self._dir);
        }
    }

    async fn read_connection(stream: UnixStream, tx: mpsc::Sender<IncomingMessage>) {
        let mut reader = BufReader::new(stream);
        let mut buffer = Vec::new();
        loop {
            match read_bounded_line(&mut reader, &mut buffer, MAX_WIRE_LINE_BYTES).await {
                Ok(LineRead::Eof) => break,
                Ok(LineRead::TooLong) => {
                    let _ = tx
                        .send(IncomingMessage::ProtocolError(format!(
                            "NDJSON line exceeds the maximum of {} KiB",
                            MAX_RUN_STARTED_LINE_BYTES / 1024
                        )))
                        .await;
                }
                Ok(LineRead::Line) => {
                    let message = match std::str::from_utf8(&buffer) {
                        Ok(text) if text.trim().is_empty() => continue,
                        Ok(text) => match decode_line(text) {
                            Ok(event) => IncomingMessage::Event(event),
                            Err(error) => IncomingMessage::ProtocolError(error.to_string()),
                        },
                        Err(_) => {
                            IncomingMessage::ProtocolError("NDJSON line is not valid UTF-8".into())
                        }
                    };
                    if tx.send(message).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = tx
                        .send(IncomingMessage::ProtocolError(format!(
                            "Error reading NDJSON events from IPC socket: {error}"
                        )))
                        .await;
                    break;
                }
            }
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum LineRead {
        Eof,
        Line,
        TooLong,
    }

    /// Reads a single `\n`-delimited line without ever allocating more than `max_bytes`.
    async fn read_bounded_line<R: AsyncBufRead + Unpin>(
        reader: &mut R,
        out: &mut Vec<u8>,
        max_bytes: usize,
    ) -> io::Result<LineRead> {
        out.clear();
        let mut exceeded = false;
        loop {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                return if exceeded {
                    Ok(LineRead::TooLong)
                } else if out.is_empty() {
                    Ok(LineRead::Eof)
                } else {
                    Ok(LineRead::Line)
                };
            }
            let newline_pos = available.iter().position(|&byte| byte == b'\n');
            let take = newline_pos.map_or(available.len(), |index| index + 1);
            if !exceeded {
                if out.len() + take > max_bytes {
                    exceeded = true;
                    out.clear();
                } else {
                    out.extend_from_slice(&available[..take]);
                }
            }
            reader.consume(take);
            if newline_pos.is_some() {
                return Ok(if exceeded {
                    LineRead::TooLong
                } else {
                    LineRead::Line
                });
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use kat0_pipeline_protocol::v3::{Event, encode_line};
        use tokio::io::AsyncWriteExt as _;

        use super::*;
        use crate::ipc::EventSocket;

        #[tokio::test]
        async fn binds_private_0700_socket_receives_events_and_rejects_oversized_lines() {
            let (socket, mut rx) = EventSocket::bind().unwrap();
            let sock_path = socket.path().to_owned();
            let parent = sock_path.parent().unwrap().to_owned();
            let mode = fs::metadata(&parent).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700);

            let mut client = UnixStream::connect(&sock_path).await.unwrap();
            let first = encode_line(&Event::Tick { elapsed_ms: 42 }).unwrap();
            client.write_all(first.as_bytes()).await.unwrap();
            client.write_all(b"\n").await.unwrap();

            // Send an oversized line without OOM, followed by a valid line on the same stream.
            let oversized = vec![b'a'; MAX_WIRE_LINE_BYTES + 64];
            client.write_all(&oversized).await.unwrap();
            client.write_all(b"\n").await.unwrap();

            let second = encode_line(&Event::Tick { elapsed_ms: 84 }).unwrap();
            client.write_all(second.as_bytes()).await.unwrap();
            client.write_all(b"\n").await.unwrap();
            drop(client);

            assert!(matches!(
                rx.recv().await,
                Some(IncomingMessage::Event(Event::Tick { elapsed_ms: 42 }))
            ));
            assert!(matches!(
                rx.recv().await,
                Some(IncomingMessage::ProtocolError(_))
            ));
            assert!(matches!(
                rx.recv().await,
                Some(IncomingMessage::Event(Event::Tick { elapsed_ms: 84 }))
            ));

            socket.close().await;
            assert!(!parent.exists());
        }
    }
}

#[cfg(not(unix))]
mod transport {
    use std::{io, path::Path};

    use tokio::sync::mpsc;

    use super::IncomingMessage;

    pub(super) struct Endpoint;

    impl Endpoint {
        pub(super) fn bind() -> io::Result<(Self, mpsc::Receiver<IncomingMessage>)> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unix domain socket IPC transport is currently supported on Unix platforms only",
            ))
        }

        pub(super) fn path(&self) -> &Path {
            Path::new("")
        }

        pub(super) async fn close(self) {}
    }
}
