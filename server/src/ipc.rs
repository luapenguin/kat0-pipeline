// SPDX-License-Identifier: AGPL-3.0-only

use std::{io, path::Path, time::Duration};

use kat0_pipeline_protocol::{
    limits::MAX_RUN_STARTED_LINE_BYTES,
    v3::{Event, decode_line},
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt as _, AsyncRead, BufReader},
    sync::{OwnedSemaphorePermit, mpsc},
};

/// Environment variable passed to the orchestrated child process pointing to the NDJSON v3 IPC endpoint.
pub const EVENTS_SOCK_ENV: &str = "KAT0_PIPELINE_EVENTS_SOCK";

#[cfg(any(unix, windows))]
const CHANNEL_CAPACITY: usize = 256;
#[cfg(any(unix, windows))]
const MAX_IPC_CONNECTIONS: usize = 32;
#[cfg(any(unix, windows))]
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

const MAX_WIRE_LINE_BYTES: usize = MAX_RUN_STARTED_LINE_BYTES + 2;
const MAX_CONSECUTIVE_PROTOCOL_ERRORS: usize = 16;

pub enum IncomingMessage {
    Event(Event),
    ProtocolError(String),
}

/// Platform-isolated IPC event receiver owned by the orchestrator.
///
/// - On Unix platforms this binds a private `0700` Unix domain socket and verifies peer UID.
/// - On Windows this hosts a local-only Named Pipe (`reject_remote_clients(true)`).
/// - Dropping or closing the handle stops accepting new clients and cleans up transport state.
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

#[cfg(any(unix, windows))]
async fn read_connection<R: AsyncRead + Unpin>(
    stream: R,
    tx: mpsc::Sender<IncomingMessage>,
    _permit: OwnedSemaphorePermit,
) {
    let mut reader = BufReader::new(stream);
    let mut buffer = Vec::new();
    let mut consecutive_errors = 0usize;
    loop {
        match read_bounded_line(&mut reader, &mut buffer, MAX_WIRE_LINE_BYTES).await {
            Ok(LineRead::Eof) => break,
            Ok(LineRead::TooLong) => {
                let _ = tx
                    .send(IncomingMessage::ProtocolError(format!(
                        "NDJSON line exceeds the maximum of {} KiB; closing IPC connection",
                        MAX_RUN_STARTED_LINE_BYTES / 1024
                    )))
                    .await;
                break;
            }
            Ok(LineRead::Line) => {
                let message = match std::str::from_utf8(&buffer) {
                    Ok(text) if text.trim().is_empty() => continue,
                    Ok(text) => match decode_line(text) {
                        Ok(event) => {
                            consecutive_errors = 0;
                            IncomingMessage::Event(event)
                        }
                        Err(error) => {
                            consecutive_errors += 1;
                            IncomingMessage::ProtocolError(error.to_string())
                        }
                    },
                    Err(_) => {
                        consecutive_errors += 1;
                        IncomingMessage::ProtocolError("NDJSON line is not valid UTF-8".into())
                    }
                };
                if tx.send(message).await.is_err() {
                    break;
                }
                if consecutive_errors >= MAX_CONSECUTIVE_PROTOCOL_ERRORS {
                    let _ = tx
                        .send(IncomingMessage::ProtocolError(format!(
                            "Closing IPC connection after {MAX_CONSECUTIVE_PROTOCOL_ERRORS} consecutive protocol errors"
                        )))
                        .await;
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
#[cfg(any(unix, windows))]
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

#[cfg(unix)]
mod transport {
    use std::{
        fs::{self, Permissions},
        io,
        os::unix::fs::{MetadataExt as _, PermissionsExt as _},
        path::{Path, PathBuf},
        sync::Arc,
    };

    use tempfile::TempDir;
    use tokio::{
        net::{UnixListener, UnixStream},
        sync::{Semaphore, mpsc, watch},
        task::{JoinHandle, JoinSet},
        time::timeout,
    };

    use super::{
        CHANNEL_CAPACITY, DRAIN_TIMEOUT, IncomingMessage, MAX_IPC_CONNECTIONS, read_connection,
    };

    pub(super) struct Endpoint {
        _dir: TempDir,
        path: PathBuf,
        stop_accept: watch::Sender<bool>,
        accept_task: JoinHandle<()>,
    }

    async fn accept_unix_client(
        stream: UnixStream,
        expected_uid: u32,
        permits: &Arc<Semaphore>,
        tx: &mpsc::Sender<IncomingMessage>,
        readers: &mut JoinSet<()>,
    ) {
        let same_uid = stream
            .peer_cred()
            .is_ok_and(|cred| cred.uid() == expected_uid);
        if !same_uid {
            let _ = tx
                .send(IncomingMessage::ProtocolError(
                    "Rejected IPC connection from unexpected peer UID".into(),
                ))
                .await;
            return;
        }
        let Ok(permit) = Arc::clone(permits).try_acquire_owned() else {
            let _ = tx
                .send(IncomingMessage::ProtocolError(format!(
                    "Rejected IPC connection: exceeded maximum of {MAX_IPC_CONNECTIONS} concurrent clients"
                )))
                .await;
            return;
        };
        readers.spawn(read_connection(stream, tx.clone(), permit));
    }

    impl Endpoint {
        pub(super) fn bind() -> io::Result<(Self, mpsc::Receiver<IncomingMessage>)> {
            let dir = tempfile::Builder::new().prefix("pipeline-ipc-").tempdir()?;
            fs::set_permissions(dir.path(), Permissions::from_mode(0o700))?;
            let expected_uid = fs::metadata(dir.path())?.uid();
            let path = dir.path().join("events.sock");
            let listener = UnixListener::bind(&path)?;
            let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
            let (stop_accept, mut stop_rx) = watch::channel(false);
            let permits = Arc::new(Semaphore::new(MAX_IPC_CONNECTIONS));

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
                        Some(_) = readers.join_next(), if !readers.is_empty() => {}
                        accepted = listener.accept() => match accepted {
                            Ok((stream, _addr)) => {
                                accept_unix_client(stream, expected_uid, &permits, &tx, &mut readers).await;
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
                // Drain any connections already queued in the kernel accept backlog before shutdown
                // so fast-exiting producers never lose their events.
                if let Ok(std_listener) = listener.into_std() {
                    let _ = std_listener.set_nonblocking(true);
                    while let Ok((std_stream, _)) = std_listener.accept() {
                        if std_stream.set_nonblocking(true).is_ok()
                            && let Ok(stream) = UnixStream::from_std(std_stream)
                        {
                            accept_unix_client(stream, expected_uid, &permits, &tx, &mut readers)
                                .await;
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

    #[cfg(test)]
    mod tests {
        use kat0_pipeline_protocol::v3::{Event, encode_line};
        use tokio::{io::AsyncWriteExt as _, net::UnixStream};

        use super::*;
        use crate::ipc::{EventSocket, MAX_WIRE_LINE_BYTES};

        #[tokio::test]
        async fn binds_private_0700_socket_and_closes_stream_after_oversized_line() {
            let (socket, mut rx) = EventSocket::bind().unwrap();
            let sock_path = socket.path().to_owned();
            let parent = sock_path.parent().unwrap().to_owned();
            let mode = fs::metadata(&parent).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700);

            let mut client = UnixStream::connect(&sock_path).await.unwrap();
            let first = encode_line(&Event::Tick { elapsed_ms: 42 }).unwrap();
            client.write_all(first.as_bytes()).await.unwrap();
            client.write_all(b"\n").await.unwrap();

            // Send an oversized line without OOM; server must reject and close this stream.
            let oversized = vec![b'a'; MAX_WIRE_LINE_BYTES + 64];
            client.write_all(&oversized).await.unwrap();
            client.write_all(b"\n").await.unwrap();

            assert!(matches!(
                rx.recv().await,
                Some(IncomingMessage::Event(Event::Tick { elapsed_ms: 42 }))
            ));
            assert!(matches!(
                rx.recv().await,
                Some(IncomingMessage::ProtocolError(_))
            ));
            drop(client);

            // A fresh client connection on the same socket continues working cleanly.
            let mut second_client = UnixStream::connect(&sock_path).await.unwrap();
            let second = encode_line(&Event::Tick { elapsed_ms: 84 }).unwrap();
            second_client.write_all(second.as_bytes()).await.unwrap();
            second_client.write_all(b"\n").await.unwrap();
            drop(second_client);

            assert!(matches!(
                rx.recv().await,
                Some(IncomingMessage::Event(Event::Tick { elapsed_ms: 84 }))
            ));

            socket.close().await;
            assert!(!parent.exists());
        }

        #[tokio::test]
        async fn enforces_max_ipc_connections_and_reaps_closed_clients() {
            let (socket, mut rx) = EventSocket::bind().unwrap();
            let sock_path = socket.path().to_owned();

            // Open MAX_IPC_CONNECTIONS active clients and confirm each is accepted.
            let mut active = Vec::with_capacity(MAX_IPC_CONNECTIONS);
            for i in 0..MAX_IPC_CONNECTIONS {
                let mut stream = UnixStream::connect(&sock_path).await.unwrap();
                let line = encode_line(&Event::Tick {
                    elapsed_ms: i as u64,
                })
                .unwrap();
                stream.write_all(line.as_bytes()).await.unwrap();
                stream.write_all(b"\n").await.unwrap();
                assert!(matches!(
                    rx.recv().await,
                    Some(IncomingMessage::Event(Event::Tick { elapsed_ms })) if elapsed_ms == i as u64
                ));
                active.push(stream);
            }

            // One more concurrent client must be rejected with a protocol error.
            let _overflow = UnixStream::connect(&sock_path).await.unwrap();
            assert!(matches!(
                rx.recv().await,
                Some(IncomingMessage::ProtocolError(msg)) if msg.contains("concurrent clients")
            ));
            drop(_overflow);

            // Dropping one active client frees its slot cleanly.
            active.pop();
            let mut recovered = false;
            for _ in 0..20 {
                let mut client = UnixStream::connect(&sock_path).await.unwrap();
                let line = encode_line(&Event::Tick { elapsed_ms: 999 }).unwrap();
                if client.write_all(line.as_bytes()).await.is_ok()
                    && client.write_all(b"\n").await.is_ok()
                {
                    drop(client);
                    if matches!(
                        rx.recv().await,
                        Some(IncomingMessage::Event(Event::Tick { elapsed_ms: 999 }))
                    ) {
                        recovered = true;
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(recovered, "dropping an IPC client should free a slot");

            drop(active);
            socket.close().await;
        }
    }
}

#[cfg(windows)]
mod transport {
    use std::{
        collections::hash_map::RandomState,
        hash::BuildHasher as _,
        io,
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, SystemTime},
    };

    use tokio::{
        net::windows::named_pipe::ServerOptions,
        sync::{Semaphore, mpsc, watch},
        task::{JoinHandle, JoinSet},
        time::timeout,
    };

    use super::{
        CHANNEL_CAPACITY, DRAIN_TIMEOUT, IncomingMessage, MAX_IPC_CONNECTIONS, read_connection,
    };

    static PIPE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    pub(super) struct Endpoint {
        path: PathBuf,
        stop_accept: watch::Sender<bool>,
        accept_task: JoinHandle<()>,
    }

    impl Endpoint {
        pub(super) fn bind() -> io::Result<(Self, mpsc::Receiver<IncomingMessage>)> {
            let pipe_name = unique_pipe_name();
            let mut server = ServerOptions::new()
                .first_pipe_instance(true)
                .reject_remote_clients(true)
                .create(&pipe_name)?;
            let path = PathBuf::from(&pipe_name);
            let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
            let (stop_accept, mut stop_rx) = watch::channel(false);
            let permits = Arc::new(Semaphore::new(MAX_IPC_CONNECTIONS));

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
                        Some(_) = readers.join_next(), if !readers.is_empty() => {}
                        connected = server.connect() => match connected {
                            Ok(()) => {
                                let next = match ServerOptions::new()
                                    .reject_remote_clients(true)
                                    .create(&pipe_name)
                                {
                                    Ok(next) => next,
                                    Err(error) => {
                                        let _ = tx.send(IncomingMessage::ProtocolError(format!(
                                            "Failed to create next IPC named pipe instance: {error}"
                                        ))).await;
                                        break;
                                    }
                                };
                                let client = std::mem::replace(&mut server, next);
                                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                                    let _ = tx.send(IncomingMessage::ProtocolError(format!(
                                        "Rejected IPC connection: exceeded maximum of {MAX_IPC_CONNECTIONS} concurrent clients"
                                    ))).await;
                                    continue;
                                };
                                let tx = tx.clone();
                                readers.spawn(read_connection(client, tx, permit));
                            }
                            Err(error) => {
                                let _ = tx.send(IncomingMessage::ProtocolError(format!(
                                    "Failed to accept IPC named pipe connection: {error}"
                                ))).await;
                                break;
                            }
                        }
                    }
                }
                // Briefly accept a fast-exiting producer that already connected before shutdown.
                if let Ok(Ok(())) = timeout(Duration::from_millis(20), server.connect()).await
                    && let Ok(permit) = Arc::clone(&permits).try_acquire_owned()
                {
                    readers.spawn(read_connection(server, tx.clone(), permit));
                }
                let _ = timeout(DRAIN_TIMEOUT, async {
                    while readers.join_next().await.is_some() {}
                })
                .await;
                readers.abort_all();
            });

            Ok((
                Self {
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
        }
    }

    fn unique_pipe_name() -> String {
        let pid = std::process::id();
        let seq = PIPE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        let hi = RandomState::new().hash_one((pid, seq, nanos));
        let lo = RandomState::new().hash_one((nanos, seq, pid));
        format!(r"\\.\pipe\kat0-pipeline-{pid}-{hi:016x}{lo:016x}")
    }

    #[cfg(test)]
    mod tests {
        use kat0_pipeline_protocol::v3::{Event, encode_line};
        use tokio::{io::AsyncWriteExt as _, net::windows::named_pipe::ClientOptions};

        use super::*;
        use crate::ipc::{EventSocket, MAX_WIRE_LINE_BYTES};

        #[tokio::test]
        async fn hosts_local_named_pipe_and_closes_stream_after_oversized_line() {
            let (socket, mut rx) = EventSocket::bind().unwrap();
            let pipe_path = socket.path().to_owned();
            assert!(
                pipe_path
                    .to_string_lossy()
                    .starts_with(r"\\.\pipe\kat0-pipeline-")
            );

            let mut client = ClientOptions::new().open(&pipe_path).unwrap();
            let first = encode_line(&Event::Tick { elapsed_ms: 42 }).unwrap();
            client.write_all(first.as_bytes()).await.unwrap();
            client.write_all(b"\n").await.unwrap();

            let oversized = vec![b'a'; MAX_WIRE_LINE_BYTES + 64];
            let _ = client.write_all(&oversized).await;
            let _ = client.write_all(b"\n").await;

            assert!(matches!(
                rx.recv().await,
                Some(IncomingMessage::Event(Event::Tick { elapsed_ms: 42 }))
            ));
            assert!(matches!(
                rx.recv().await,
                Some(IncomingMessage::ProtocolError(_))
            ));
            drop(client);

            let mut second_client = ClientOptions::new().open(&pipe_path).unwrap();
            let second = encode_line(&Event::Tick { elapsed_ms: 84 }).unwrap();
            second_client.write_all(second.as_bytes()).await.unwrap();
            second_client.write_all(b"\n").await.unwrap();
            drop(second_client);

            assert!(matches!(
                rx.recv().await,
                Some(IncomingMessage::Event(Event::Tick { elapsed_ms: 84 }))
            ));

            socket.close().await;
        }
    }
}

#[cfg(not(any(unix, windows)))]
mod transport {
    use std::{io, path::Path};

    use tokio::sync::mpsc;

    use super::IncomingMessage;

    pub(super) struct Endpoint;

    impl Endpoint {
        pub(super) fn bind() -> io::Result<(Self, mpsc::Receiver<IncomingMessage>)> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC event transport is only supported on Unix and Windows platforms",
            ))
        }

        pub(super) fn path(&self) -> &Path {
            Path::new("")
        }

        pub(super) async fn close(self) {}
    }
}
