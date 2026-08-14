//! Peer mode (SPEC.md §6): handshake with a master, poll its manifest, and
//! pull missing segments into the data dir. Also serves what it has to
//! other nodes (via the shared Node dispatch).

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::log;
use crate::node::{Event, Node};
use crate::protocol::{self, Message};
use crate::transfer;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
const MANIFEST_POLL_INTERVAL: Duration = Duration::from_secs(3);
const MANIFEST_RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);
/// How long to wait before retrying a segment whose pull failed.
const FAIL_RETRY_COOLDOWN: Duration = Duration::from_secs(5);

pub fn run(local_port: u16, remote: SocketAddr, name: &str, data_dir: &str) -> io::Result<()> {
    let data_dir = PathBuf::from(data_dir);
    fs::create_dir_all(&data_dir)?;

    let socket = UdpSocket::bind(("0.0.0.0", local_port))?;
    log::info(&format!(
        "peer listening on 0.0.0.0:{local_port} (name: {name}, data dir: {})",
        data_dir.display()
    ));

    let mut node = Node::new(
        socket,
        name.to_string(),
        data_dir.join("live.m3u8"),
        data_dir.clone(),
    );

    // --- peer state ---
    let mut handshake_done = false;
    let handshake_deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    let mut next_poll = Instant::now() + MANIFEST_POLL_INTERVAL;
    let mut poll_timeout: Option<Instant> = None;
    let mut pull: Option<u16> = None; // active receiver transfer id
    let mut pull_queue: VecDeque<String> = VecDeque::new();
    let mut queued: HashSet<String> = HashSet::new(); // queued or in-flight
    let mut failed_at: HashMap<String, Instant> = HashMap::new();

    // Initial handshake.
    let hs = Message::HandshakeRequest {
        name: name.to_string(),
    };
    node.socket.send_to(&protocol::encode(&hs), remote)?;
    log::info(&format!("sent HANDSHAKE_REQUEST to {remote}"));

    let mut buf = [0u8; 65536];
    loop {
        // --- compute the socket timeout as the earliest deadline ---
        let mut deadlines: Vec<Instant> = Vec::new();
        if let Some(d) = node.next_deadline() {
            deadlines.push(d);
        }
        if handshake_done {
            deadlines.push(next_poll);
            if let Some(t) = poll_timeout {
                deadlines.push(t);
            }
        } else {
            deadlines.push(handshake_deadline);
        }
        // Clamp zero (deadline already passed) to 1ns: set_read_timeout
        // rejects a 0-duration timeout on Linux; we want to tick immediately.
        let timeout = deadlines.iter().min().map(|d| {
            let rem = d.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                Duration::from_nanos(1)
            } else {
                rem
            }
        });
        node.socket.set_read_timeout(timeout)?;

        // --- receive & dispatch ---
        match node.socket.recv_from(&mut buf) {
            Ok((n, src)) => match node.handle(&buf[..n], src) {
                Event::HandshakeResponse { name: server_name } => {
                    log::info(&format!("handshake OK — master {src} (name: {server_name})"));
                    handshake_done = true;
                    next_poll = Instant::now(); // poll immediately
                }
                Event::ManifestResponse { data } => {
                    if !data.is_empty() {
                        if write_manifest(&data_dir, &data)? {
                            sync_queue(&data_dir, &data, &mut pull_queue, &mut queued, &failed_at);
                            log::info(&format!(
                                "manifest updated ({} segments)",
                                segment_count(&data)
                            ));
                        }
                    } else {
                        log::warn("master returned an empty manifest — keeping previous copy");
                    }
                    poll_timeout = None;
                }
                Event::None => {}
            },
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut => {}
            Err(e) => return Err(e),
        }

        // --- timers & pull scheduling ---
        let now = Instant::now();
        node.tick(now);

        if !handshake_done {
            if now >= handshake_deadline {
                log::error(&format!("handshake timed out — no reply from {remote}"));
                std::process::exit(1);
            }
            continue;
        }

        if now >= next_poll {
            let req = Message::ManifestRequest;
            node.socket.send_to(&protocol::encode(&req), remote)?;
            log::trace("sent MANIFEST_REQUEST");
            next_poll = now + MANIFEST_POLL_INTERVAL;
            poll_timeout = Some(now + MANIFEST_RESPONSE_TIMEOUT);
        }
        if let Some(t) = poll_timeout {
            if now >= t {
                log::warn("manifest request timed out — will retry on next poll");
                poll_timeout = None;
            }
        }

        // Sequential segment pull: one receiver at a time.
        if let Some(id) = pull {
            if let Some(outcome) = node.registry.receiver_outcome(id) {
                let filename = node.registry.receiver_filename(id);
                match outcome {
                    Ok(()) => {
                        log::info(&format!("segment {filename} saved"));
                        queued.remove(&filename);
                    }
                    Err(e) => {
                        log::warn(&format!("segment {filename} pull failed: {e}"));
                        queued.remove(&filename);
                        failed_at.insert(filename, now);
                    }
                }
                node.registry.remove_receiver(id);
                pull = None;
            }
        }
        if pull.is_none() {
            if let Some(filename) = pull_queue.pop_front() {
                if !transfer::valid_filename(&filename) {
                    log::warn(&format!("skipping invalid filename from manifest: {filename:?}"));
                    queued.remove(&filename);
                } else if let Some(id) =
                    node.registry
                        .start_receiver(&node.socket, remote, &data_dir, &filename)
                {
                    log::info(&format!("pulling {filename} (transfer {id:#06x})"));
                    pull = Some(id);
                } else {
                    log::warn("could not start download — will retry");
                    pull_queue.push_back(filename);
                }
            }
        }
    }
}

/// Atomically write the manifest; returns true if the content changed.
fn write_manifest(data_dir: &Path, data: &[u8]) -> io::Result<bool> {
    let real = data_dir.join("live.m3u8");
    let tmp = data_dir.join("live.m3u8.tmp");
    if fs::read(&real).map(|old| old == data).unwrap_or(false) {
        return Ok(false);
    }
    fs::write(&tmp, data)?;
    fs::rename(&tmp, &real)?;
    Ok(true)
}

/// Parse segment filenames out of an m3u8 playlist.
fn parse_manifest(data: &[u8]) -> Vec<String> {
    std::str::from_utf8(data)
        .map(|s| {
            s.lines()
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .map(|l| l.trim().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn segment_count(data: &[u8]) -> usize {
    parse_manifest(data).len()
}

/// Enqueue manifest segments that are missing locally and not already
/// queued/in-flight and not in the failure cooldown.
fn sync_queue(
    data_dir: &Path,
    manifest: &[u8],
    queue: &mut VecDeque<String>,
    queued: &mut HashSet<String>,
    failed_at: &HashMap<String, Instant>,
) {
    for filename in parse_manifest(manifest) {
        if !transfer::valid_filename(&filename) {
            continue;
        }
        if data_dir.join(&filename).exists() {
            continue;
        }
        if queued.contains(&filename) {
            continue;
        }
        if failed_at
            .get(&filename)
            .map(|t| t.elapsed() < FAIL_RETRY_COOLDOWN)
            .unwrap_or(false)
        {
            continue;
        }
        queued.insert(filename.clone());
        queue.push_back(filename);
    }
}
