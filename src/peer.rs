//! Peer mode: bind a fixed local port (so other peers can reach us later),
//! handshake with the master, then poll its manifest and keep a local copy.
//! See SPEC.md §5.3–5.4.

use std::fs;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::path::Path;
use std::time::{Duration, Instant};

use crate::log;
use crate::protocol::{self, Message};

pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
pub const MANIFEST_TIMEOUT: Duration = Duration::from_secs(3);
pub const MANIFEST_POLL_INTERVAL: Duration = Duration::from_secs(3);

/// Run the peer: handshake once, then poll the manifest forever.
pub fn run(local_port: u16, remote: SocketAddr, name: &str, data_dir: &str) -> io::Result<()> {
    let socket = UdpSocket::bind(("0.0.0.0", local_port))?;
    log::info(&format!(
        "peer listening on 0.0.0.0:{local_port} (name: {name}, data dir: {data_dir})"
    ));
    fs::create_dir_all(data_dir)?;

    // 1. Handshake (SPEC §5.3)
    let server_name = match transact(
        &socket,
        remote,
        &Message::HandshakeRequest {
            name: name.to_string(),
        },
        |m| match m {
            Message::HandshakeResponse { name } => Some(name.clone().into_bytes()),
            _ => None,
        },
        HANDSHAKE_TIMEOUT,
    )? {
        Some(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        None => {
            log::error(&format!("handshake timed out — no reply from {remote}"));
            std::process::exit(1);
        }
    };
    log::info(&format!("handshake OK — master {remote} (name: {server_name})"));

    // 2. Manifest sync loop (SPEC §5.4)
    log::info(&format!(
        "polling manifest every {}s",
        MANIFEST_POLL_INTERVAL.as_secs()
    ));
    let mut last_manifest: Option<Vec<u8>> = None;
    loop {
        match transact(
            &socket,
            remote,
            &Message::ManifestRequest,
            |m| match m {
                Message::ManifestResponse { data } => Some(data.clone()),
                _ => None,
            },
            MANIFEST_TIMEOUT,
        ) {
            Ok(Some(data)) if !data.is_empty() => {
                if last_manifest.as_deref() != Some(data.as_slice()) {
                    let segment_count = data
                        .split(|b| *b == b'\n')
                        .filter(|line| !line.is_empty() && line[0] != b'#')
                        .count();
                    write_manifest(data_dir, &data)?;
                    log::info(&format!(
                        "manifest updated ({segment_count} segments, {} bytes)",
                        data.len()
                    ));
                    log::debug(&format!(
                        "manifest contents:\n{}",
                        String::from_utf8_lossy(&data)
                    ));
                    last_manifest = Some(data);
                } else {
                    log::debug("manifest unchanged");
                }
            }
            Ok(Some(_)) => {
                log::warn("master returned an empty manifest — keeping previous copy");
            }
            Ok(None) => {
                log::warn(&format!(
                    "manifest request timed out — will retry in {}s",
                    MANIFEST_POLL_INTERVAL.as_secs()
                ));
            }
            Err(e) => {
                log::error(&format!("manifest request failed: {e}"));
            }
        }
        std::thread::sleep(MANIFEST_POLL_INTERVAL);
    }
}

/// Atomically write the manifest into `data_dir/live.m3u8` (tmp + rename).
fn write_manifest(data_dir: &str, data: &[u8]) -> io::Result<()> {
    let real = Path::new(data_dir).join("live.m3u8");
    let tmp = Path::new(data_dir).join("live.m3u8.tmp");
    fs::write(&tmp, data)?;
    fs::rename(&tmp, &real)?;
    Ok(())
}

/// Send `request`, then wait up to `timeout` for a datagram that `extract`
/// recognizes. Returns the extracted payload, or `None` on timeout.
/// Datagrams that don't match are logged and skipped.
fn transact(
    socket: &UdpSocket,
    remote: SocketAddr,
    request: &Message,
    extract: impl Fn(&Message) -> Option<Vec<u8>>,
    timeout: Duration,
) -> io::Result<Option<Vec<u8>>> {
    socket.send_to(&protocol::encode(request), remote)?;

    let deadline = Instant::now() + timeout;
    let mut buf = [0u8; 65536];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(None);
        }
        socket.set_read_timeout(Some(remaining))?;

        match socket.recv_from(&mut buf) {
            Ok((n, src)) => match protocol::decode(&buf[..n]) {
                Ok(msg) => {
                    if let Some(payload) = extract(&msg) {
                        return Ok(Some(payload));
                    }
                    log::debug(&format!("ignoring unexpected message from {src}: {msg:?}"));
                }
                Err(e) => {
                    log::warn(&format!("dropping malformed datagram from {src}: {e}"));
                }
            },
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut =>
            {
                return Ok(None);
            }
            Err(e) => return Err(e),
        }
    }
}
