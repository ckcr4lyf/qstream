//! Peer mode (SPEC.md §6): handshake with a bootstrap node, poll its
//! manifest, discover other peers via peerlists, and pull missing segments
//! from whichever peers have them — several in parallel. Also serves what
//! it has to other nodes (via the shared Node dispatch).

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::http;
use crate::log;
use crate::node::{Event, Node};
use crate::protocol::{self, Message};
use crate::transfer;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
const MANIFEST_POLL_INTERVAL: Duration = Duration::from_secs(3);
const MANIFEST_RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);
const PEERLIST_POLL_INTERVAL: Duration = Duration::from_secs(5);
const PEER_TTL: Duration = Duration::from_secs(600);
/// How long to wait before retrying a segment whose pull failed.
const FAIL_RETRY_COOLDOWN: Duration = Duration::from_secs(5);
/// Concurrent segment downloads.
const MAX_PARALLEL_DOWNLOADS: usize = 4;
/// Don't start more than this many concurrent pulls from one peer.
const MAX_INFLIGHT_PER_PEER: usize = 2;

/// An in-flight download: which file from which peer.
struct ActiveJob {
    filename: String,
    peer: SocketAddr,
}

pub fn run(
    local_port: u16,
    remote: SocketAddr,
    name: &str,
    data_dir: &str,
    http_port: Option<u16>,
) -> io::Result<()> {
    let data_dir = PathBuf::from(data_dir);
    fs::create_dir_all(&data_dir)?;

    let socket = UdpSocket::bind(("0.0.0.0", local_port))?;
    let local_addr = socket.local_addr()?;
    log::info(&format!(
        "peer listening on 0.0.0.0:{local_port} (name: {name}, data dir: {})",
        data_dir.display()
    ));

    if let Some(hp) = http_port {
        let root = data_dir.clone();
        thread::spawn(move || {
            if let Err(e) = http::serve(root, hp) {
                log::error(&format!("http server failed: {e}"));
                std::process::exit(1);
            }
        });
    }

    let mut node = Node::new(
        socket,
        name.to_string(),
        data_dir.join("live.m3u8"),
        data_dir.clone(),
    );

    // --- protocol state ---
    let mut handshake_done = false;
    let handshake_deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    let mut next_poll = Instant::now() + MANIFEST_POLL_INTERVAL;
    let mut poll_timeout: Option<Instant> = None;
    let mut next_peerlist = Instant::now() + PEERLIST_POLL_INTERVAL;
    let mut pending_handshakes: HashMap<SocketAddr, Instant> = HashMap::new();

    // --- job scheduler state ---
    let mut active: HashMap<u16, ActiveJob> = HashMap::new();
    let mut pull_queue: VecDeque<String> = VecDeque::new();
    let mut queued: HashSet<String> = HashSet::new(); // queued or in-flight
    let mut tried: HashMap<String, HashSet<SocketAddr>> = HashMap::new();
    let mut failed_at: HashMap<String, Instant> = HashMap::new();

    // Initial handshake with the bootstrap node.
    let hs = Message::HandshakeRequest {
        name: name.to_string(),
    };
    node.socket.send_to(&protocol::encode(&hs), remote)?;
    log::info(&format!("sent HANDSHAKE_REQUEST to {remote}"));

    let mut buf = [0u8; 65536];
    loop {
        // --- earliest deadline becomes the socket timeout ---
        let mut deadlines: Vec<Instant> = Vec::new();
        if let Some(d) = node.next_deadline() {
            deadlines.push(d);
        }
        deadlines.extend(pending_handshakes.values().copied());
        if handshake_done {
            deadlines.push(next_poll);
            deadlines.push(next_peerlist);
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
            Ok((n, src)) => {
                let now = Instant::now();
                match node.handle(&buf[..n], src) {
                    Event::HandshakeResponse { src, name: peer_name } => {
                        node.register_peer(src, peer_name.clone());
                        if src == remote && !handshake_done {
                            log::info(&format!("handshake OK — bootstrap {src} (name: {peer_name})"));
                            handshake_done = true;
                            next_poll = Instant::now();
                            next_peerlist = Instant::now();
                        }
                        if pending_handshakes.remove(&src).is_some() {
                            log::info(&format!("discovered peer {peer_name} at {src}"));
                        }
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
                            log::warn("bootstrap returned an empty manifest — keeping previous copy");
                        }
                        poll_timeout = None;
                    }
                    Event::PeerlistResponse { peers } => {
                        for peer in peers {
                            if peer == local_addr || peer.port() == 0 {
                                continue;
                            }
                            if node.peers.contains_key(&peer)
                                || pending_handshakes.contains_key(&peer)
                            {
                                continue;
                            }
                            pending_handshakes.insert(peer, now + HANDSHAKE_TIMEOUT);
                            let req = Message::HandshakeRequest {
                                name: name.to_string(),
                            };
                            let _ = node.socket.send_to(&protocol::encode(&req), peer);
                            log::debug(&format!("handshaking with discovered peer {peer}"));
                        }
                    }
                    Event::None => {}
                }
            }
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut => {}
            Err(e) => return Err(e),
        }

        // --- timers & scheduling ---
        let now = Instant::now();
        node.tick(now);
        node.prune_peers(PEER_TTL, now);

        if !handshake_done {
            if now >= handshake_deadline {
                log::error(&format!("handshake timed out — no reply from {remote}"));
                std::process::exit(1);
            }
            continue;
        }

        // Discovered peers that didn't answer: drop, retried on next list.
        pending_handshakes.retain(|peer, deadline| {
            if now >= *deadline {
                log::warn(&format!("no handshake reply from {peer} — skipping"));
                false
            } else {
                true
            }
        });

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

        if now >= next_peerlist {
            let req = Message::PeerlistRequest;
            node.socket.send_to(&protocol::encode(&req), remote)?;
            log::trace("sent PEERLIST_REQUEST");
            next_peerlist = now + PEERLIST_POLL_INTERVAL;
        }

        schedule_jobs(
            &mut node,
            &mut active,
            &mut pull_queue,
            &mut queued,
            &mut tried,
            &mut failed_at,
            &data_dir,
            local_addr,
            now,
        );
    }
}

/// Collect finished downloads, then fill up to MAX_PARALLEL_DOWNLOADS slots.
fn schedule_jobs(
    node: &mut Node,
    active: &mut HashMap<u16, ActiveJob>,
    queue: &mut VecDeque<String>,
    queued: &mut HashSet<String>,
    tried: &mut HashMap<String, HashSet<SocketAddr>>,
    failed_at: &mut HashMap<String, Instant>,
    data_dir: &Path,
    local_addr: SocketAddr,
    now: Instant,
) {
    // 1. Reap finished receivers.
    let finished: Vec<(u16, ActiveJob, Result<(), String>, bool)> = {
        let mut v = Vec::new();
        for (id, job) in active.iter() {
            if let Some(outcome) = node.registry.receiver_outcome(*id) {
                v.push((
                    *id,
                    ActiveJob {
                        filename: job.filename.clone(),
                        peer: job.peer,
                    },
                    outcome,
                    node.registry.receiver_unresponsive(*id),
                ));
            }
        }
        v
    };
    for (id, job, outcome, unresponsive) in finished {
        active.remove(&id);
        node.registry.remove_receiver(id);
        match outcome {
            Ok(()) => {
                log::info(&format!("segment {} saved", job.filename));
                queued.remove(&job.filename);
                tried.remove(&job.filename);
            }
            Err(e) => {
                log::warn(&format!("segment {} pull failed: {e}", job.filename));
                if unresponsive {
                    // Peer never answered — likely dead; stop sending it work.
                    log::warn(&format!("evicting unresponsive peer {}", job.peer));
                    node.peers.remove(&job.peer);
                }
                let untried = node.peers.iter().any(|(p, _)| {
                    *p != local_addr
                        && tried
                            .get(&job.filename)
                            .map(|s| !s.contains(p))
                            .unwrap_or(true)
                });
                if untried {
                    // Try another peer for this segment.
                    queue.push_front(job.filename);
                } else {
                    queued.remove(&job.filename);
                    tried.remove(&job.filename);
                    failed_at.insert(job.filename, now);
                }
            }
        }
    }

    // 2. Fill free slots.
    while active.len() < MAX_PARALLEL_DOWNLOADS {
        let Some(filename) = queue.pop_front() else { break };
        let Some(peer) = pick_peer(node, active, tried, &filename, local_addr) else {
            queue.push_front(filename);
            break;
        };
        match node.registry.start_receiver(&node.socket, peer, data_dir, &filename) {
            Some(id) => {
                log::info(&format!("pulling {filename} from {peer} (transfer {id:#06x})"));
                active.insert(
                    id,
                    ActiveJob {
                        filename: filename.clone(),
                        peer,
                    },
                );
                tried.entry(filename).or_default().insert(peer);
            }
            None => {
                queue.push_front(filename);
                break;
            }
        }
    }
}

/// Choose the least-loaded peer that hasn't failed this job yet.
fn pick_peer(
    node: &Node,
    active: &HashMap<u16, ActiveJob>,
    tried: &HashMap<String, HashSet<SocketAddr>>,
    filename: &str,
    local_addr: SocketAddr,
) -> Option<SocketAddr> {
    let tried_for = tried.get(filename);
    let inflight = |p: &SocketAddr| active.values().filter(|j| &j.peer == p).count();
    let candidates: Vec<SocketAddr> = node
        .peers
        .keys()
        .filter(|p| **p != local_addr)
        .filter(|p| tried_for.map(|s| !s.contains(p)).unwrap_or(true))
        .filter(|p| inflight(p) < MAX_INFLIGHT_PER_PEER)
        .cloned()
        .collect();
    if candidates.is_empty() {
        return None;
    }
    let min = candidates.iter().map(inflight).min().unwrap();
    let best: Vec<SocketAddr> = candidates.into_iter().filter(|p| inflight(p) == min).collect();
    // Pseudo-random tiebreak so load spreads across peers.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    Some(best[(nanos as usize) % best.len()])
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
