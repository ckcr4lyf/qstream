//! Peer mode (SPEC.md §6): handshake with a bootstrap node, poll its
//! manifest, discover other peers via peerlists, and pull missing segments
//! from whichever peers have them — several in parallel. Also serves what
//! it has to other nodes (via the shared Node dispatch).
//!
//! M5: source selection is score-weighted (peer ranking), peers preferred
//! over the bootstrap once comparable, and eviction requires repeated
//! unresponsiveness so a slow link isn't mistaken for a dead node.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::fault::Rng;
use crate::http;
use crate::log;
use crate::node::{Event, Node, PullResult, StatsSnapshot};
use crate::protocol::{self, Message, PEER_PARENT, PEER_SAME_IP};
use crate::transfer;
use crate::upnp;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
/// Manifest poll base interval; each poll is staggered by 0..JITTER so
/// peers don't see new segments in lockstep (M5 — synchronized polls make
/// peer-to-peer pulls useless: everyone asks everyone for the newest
/// segment before anyone has it).
const MANIFEST_POLL_INTERVAL: Duration = Duration::from_secs(2);
const POLL_JITTER_MS: u64 = 1000;
const MANIFEST_RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);
const PEERLIST_POLL_INTERVAL: Duration = Duration::from_secs(5);
const PEER_TTL: Duration = Duration::from_secs(600);
/// How long to wait before retrying a segment whose pull failed.
const FAIL_RETRY_COOLDOWN: Duration = Duration::from_secs(5);
/// Concurrent segment downloads.
const MAX_PARALLEL_DOWNLOADS: usize = 4;
/// Don't start more than this many concurrent pulls from one peer.
const MAX_INFLIGHT_PER_PEER: usize = 2;
/// Unresponsive pulls before a peer is evicted (M5: one timeout can just
/// be a bad moment; three is a pattern).
const EVICT_AFTER_UNRESPONSIVE: u32 = 3;
/// Give assigned parents a short opportunity to receive and announce a new
/// segment before using the origin. This turns parent assignments into a
/// real replication path without making a stalled parent permanent.
const PARENT_WAIT: Duration = Duration::from_secs(5);
/// Keep this many complete segments beyond the player-facing edge. With the
/// default 2-second HLS segments, three entries give playback about 6 seconds
/// to absorb replication jitter without delaying swarm synchronization.
const DEFAULT_PLAYBACK_HOLDBACK_SEGMENTS: usize = 3;

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
    // `playback.m3u8` is derived from local files, never protocol state. A
    // prior run's version could name segments that no longer exist.
    let _ = fs::remove_file(data_dir.join("playback.m3u8"));
    let playback_holdback = playback_holdback_segments();

    let socket = UdpSocket::bind(("0.0.0.0", local_port))?;
    let _ = socket.set_broadcast(true); // LAN beacon (N3)
    let local_addr = socket.local_addr()?;
    log::info(&format!(
        "peer listening on 0.0.0.0:{local_port} (name: {name}, data dir: {})",
        data_dir.display()
    ));

    let stats_sink: Arc<Mutex<StatsSnapshot>> = Arc::new(Mutex::new(StatsSnapshot::default()));
    if let Some(hp) = http_port {
        let root = data_dir.clone();
        let stats = stats_sink.clone();
        thread::spawn(move || {
            if let Err(e) = http::serve(root, hp, Some(stats)) {
                log::error(&format!("http server failed: {e}"));
                std::process::exit(1);
            }
        });
    }

    let mut node = Node::new(
        socket,
        name.to_string(),
        "peer",
        data_dir.join("live.m3u8"),
        data_dir.clone(),
        crate::fault::FaultInjector::from_env(),
        Some(stats_sink),
    );

    // Opportunistic UPnP mapping (N4): promotes us to directly-reachable.
    // Disable with QSTREAM_NO_UPNP=1 (fault/lab tests).
    if std::env::var("QSTREAM_NO_UPNP").is_err() {
        match upnp::try_map(local_port) {
            Some(addr) => node.set_claimed(addr),
            None => log::info("UPnP mapping unavailable — continuing without one"),
        }
    }

    // --- protocol state ---
    let mut handshake_done = false;
    let mut next_handshake_retry = Instant::now() + HANDSHAKE_TIMEOUT;
    let mut poll_timeout: Option<Instant> = None;
    let mut pending_handshakes: HashMap<SocketAddr, Instant> = HashMap::new();

    // --- job scheduler state ---
    let mut active: HashMap<u16, ActiveJob> = HashMap::new();
    let mut pull_queue: VecDeque<String> = VecDeque::new();
    let mut queued: HashSet<String> = HashSet::new(); // queued or in-flight
    let mut tried: HashMap<String, HashSet<SocketAddr>> = HashMap::new();
    let mut failed_at: HashMap<String, Instant> = HashMap::new();
    let mut retry_after: HashMap<(String, SocketAddr), Instant> = HashMap::new();
    let mut parents: HashSet<SocketAddr> = HashSet::new();
    let mut parent_wait: HashMap<String, Instant> = HashMap::new();
    let mut unresponsive_hits: HashMap<SocketAddr, u32> = HashMap::new();
    let mut rng = Rng::new(0);

    // Stagger the very first polls too, so peers don't start in lockstep.
    let stagger = Duration::from_millis(rng.next() % POLL_JITTER_MS);
    let mut next_poll = Instant::now() + stagger;
    let stagger = Duration::from_millis(rng.next() % (2 * POLL_JITTER_MS));
    let mut next_peerlist = Instant::now() + stagger;

    // Initial handshake with the bootstrap node.
    let hs = Message::HandshakeRequest {
        claimed: node.claimed.unwrap_or(SocketAddr::from(([0, 0, 0, 0], 0))),
        name: name.to_string(),
    };
    node.send(&protocol::encode(&hs), remote);
    log::info(&format!("sent HANDSHAKE_REQUEST to {remote}"));

    let mut buf = [0u8; 65536];
    loop {
        // --- earliest deadline becomes the socket timeout ---
        let mut deadlines: Vec<Instant> = Vec::new();
        if let Some(d) = node.next_deadline() {
            deadlines.push(d);
        }
        deadlines.extend(pending_handshakes.values().copied());
        if !handshake_done {
            deadlines.push(next_handshake_retry);
        }
        deadlines.push(next_poll);
        deadlines.push(next_peerlist);
        if let Some(t) = poll_timeout {
            deadlines.push(t);
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
                    Event::HandshakeResponse {
                        src,
                        name: peer_name,
                    } => {
                        node.register_peer(src, peer_name.clone());
                        if src == remote && !handshake_done {
                            log::info(&format!(
                                "handshake OK — bootstrap {src} (name: {peer_name})"
                            ));
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
                                sync_queue(
                                    &data_dir,
                                    &data,
                                    &mut pull_queue,
                                    &mut queued,
                                    &failed_at,
                                );
                                refresh_playback_manifest(&data_dir, playback_holdback)?;
                                log::info(&format!(
                                    "manifest updated ({} segments)",
                                    segment_count(&data)
                                ));
                            }
                        } else {
                            log::warn(
                                "bootstrap returned an empty manifest — keeping previous copy",
                            );
                        }
                        poll_timeout = None;
                    }
                    Event::PeerlistResponse { peers } => {
                        parents.clear();
                        // We're "remote" if our observed public endpoint is
                        // globally reachable; then the master's loopback/
                        // private peers (its local swarm) are unreachable
                        // from here — skip them instead of handshaking into
                        // the void (DEVLOG: home peer vs 127.0.0.1 entries).
                        let remote_me = node
                            .my_public
                            .map(|a| crate::node::remote_public(a))
                            .unwrap_or(false);
                        for (peer, flags) in peers {
                            if peer == local_addr || peer.port() == 0 {
                                continue;
                            }
                            if remote_me && !crate::node::remote_public(peer) {
                                continue;
                            }
                            // Same public IP as us: same NAT/LAN — the LAN
                            // beacon (broadcast PING) will find it directly;
                            // handshaking the public endpoint would hit the
                            // NAT's hairpin behavior (N3).
                            if flags & PEER_SAME_IP != 0 {
                                continue;
                            }
                            if flags & PEER_PARENT != 0 {
                                parents.insert(peer);
                            }
                            if node.peers.contains_key(&peer)
                                || pending_handshakes.contains_key(&peer)
                            {
                                continue;
                            }
                            pending_handshakes.insert(peer, now + HANDSHAKE_TIMEOUT);
                            let req = Message::HandshakeRequest {
                                claimed: node
                                    .claimed
                                    .unwrap_or(SocketAddr::from(([0, 0, 0, 0], 0))),
                                name: name.to_string(),
                            };
                            node.send(&protocol::encode(&req), peer);
                            log::debug(&format!("handshaking with discovered peer {peer}"));
                        }
                    }
                    Event::None => {}
                }
            }
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
            }
            Err(e) => return Err(e),
        }

        // --- timers & scheduling ---
        let now = Instant::now();
        node.tick(now);
        node.prune_peers(PEER_TTL, now);

        // Keep retrying the bootstrap handshake until it lands (M5): a
        // dropped first packet must not kill the peer. Manifest/peerlist
        // polling is not gated on it — the bootstrap answers anyway.
        if !handshake_done && now >= next_handshake_retry {
            let req = Message::HandshakeRequest {
                claimed: node.claimed.unwrap_or(SocketAddr::from(([0, 0, 0, 0], 0))),
                name: name.to_string(),
            };
            node.send(&protocol::encode(&req), remote);
            log::warn(&format!(
                "bootstrap handshake unanswered — retrying {remote}"
            ));
            next_handshake_retry = now + HANDSHAKE_TIMEOUT;
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
            node.send(&protocol::encode(&req), remote);
            log::trace("sent MANIFEST_REQUEST");
            let jitter = Duration::from_millis(rng.next() % POLL_JITTER_MS);
            next_poll = now + MANIFEST_POLL_INTERVAL + jitter;
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
            node.send(&protocol::encode(&req), remote);
            log::trace("sent PEERLIST_REQUEST");
            let jitter = Duration::from_millis(rng.next() % (2 * POLL_JITTER_MS));
            next_peerlist = now + PEERLIST_POLL_INTERVAL + jitter;
        }

        schedule_jobs(
            &mut node,
            &mut active,
            &mut pull_queue,
            &mut queued,
            &mut tried,
            &parents,
            &mut parent_wait,
            &mut failed_at,
            &mut retry_after,
            &mut unresponsive_hits,
            &data_dir,
            playback_holdback,
            local_addr,
            remote,
            &mut rng,
            now,
        );
        node.queue_depth = pull_queue.len() as u64;
        node.inflight = active.len() as u64;
    }
}

/// Collect finished downloads, then fill up to MAX_PARALLEL_DOWNLOADS slots.
fn schedule_jobs(
    node: &mut Node,
    active: &mut HashMap<u16, ActiveJob>,
    queue: &mut VecDeque<String>,
    queued: &mut HashSet<String>,
    tried: &mut HashMap<String, HashSet<SocketAddr>>,
    parents: &HashSet<SocketAddr>,
    parent_wait: &mut HashMap<String, Instant>,
    failed_at: &mut HashMap<String, Instant>,
    retry_after: &mut HashMap<(String, SocketAddr), Instant>,
    unresponsive_hits: &mut HashMap<SocketAddr, u32>,
    data_dir: &Path,
    playback_holdback: usize,
    local_addr: SocketAddr,
    bootstrap: SocketAddr,
    rng: &mut Rng,
    now: Instant,
) {
    // 1. Reap finished receivers.
    let finished: Vec<(
        u16,
        ActiveJob,
        Result<(), String>,
        bool,
        bool,
        bool,
        Option<u64>,
        Option<u64>,
    )> = {
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
                    node.registry.receiver_not_found(*id),
                    node.registry.receiver_retryable_not_found(*id),
                    node.registry.receiver_first_packet_ms(*id),
                    node.registry.receiver_saved_bytes(*id),
                ));
            }
        }
        v
    };
    for (
        id,
        job,
        outcome,
        unresponsive,
        not_found,
        retryable_not_found,
        latency,
        bytes,
    ) in finished {
        active.remove(&id);
        if outcome.is_ok() {
            // Let the registry keep the receiver in grace (COMPLETE_GRACE)
            // to re-ACK a lost final ACK; it removes the receiver itself.
        } else {
            node.registry.remove_receiver(id);
        }
        let result = if outcome.is_ok() {
            PullResult::Ok
        } else if unresponsive {
            PullResult::Timeout
        } else if retryable_not_found {
            PullResult::RetryableNotFound
        } else if not_found {
            PullResult::NotFound
        } else {
            PullResult::Other
        };
        node.record_pull(job.peer, result, latency, bytes.unwrap_or(0));
        match result {
            PullResult::Ok => {
                refresh_playback_manifest(data_dir, playback_holdback).unwrap_or_else(|e| {
                    log::warn(&format!("playback manifest update failed: {e}"))
                });
                node.announce_availability();
                log::info(&format!("segment {} saved", job.filename));
                queued.remove(&job.filename);
                tried.remove(&job.filename);
                parent_wait.remove(&job.filename);
                retry_after.retain(|(filename, _), _| filename != &job.filename);
                unresponsive_hits.remove(&job.peer);
                // Keep the receiver in the registry for its grace period so
                // it can re-ACK COMPLETE to a sender whose final ACK was
                // lost (M5); the registry removes it after COMPLETE_GRACE.
            }
            PullResult::RetryableNotFound => {
                log::debug(&format!(
                    "segment {} temporarily unavailable from {}",
                    job.filename, job.peer
                ));
                retry_after.insert(
                    (job.filename.clone(), job.peer),
                    now + FAIL_RETRY_COOLDOWN,
                );
                if let Some(tried_for) = tried.get_mut(&job.filename) {
                    tried_for.remove(&job.peer);
                }
                queue.push_back(job.filename);
            }
            _ => {
                log::warn(&format!(
                    "segment {} pull failed: {}",
                    job.filename,
                    outcome.unwrap_err()
                ));
                if unresponsive {
                    // Peer never answered — count it; evict only after a
                    // pattern (M5), so a burst or slow link isn't fatal.
                    let hits = unresponsive_hits.entry(job.peer).or_insert(0);
                    *hits += 1;
                    if *hits >= EVICT_AFTER_UNRESPONSIVE {
                        log::warn(&format!("evicting unresponsive peer {}", job.peer));
                        node.peers.remove(&job.peer);
                        unresponsive_hits.remove(&job.peer);
                    }
                } else {
                    // Peer answered — it's alive; a bad moment is forgiven.
                    unresponsive_hits.remove(&job.peer);
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
        let Some(filename) = queue.pop_front() else {
            break;
        };
        let Some(peer) = pick_peer(
            node,
            active,
            tried,
            parents,
            parent_wait,
            &filename,
            local_addr,
            bootstrap,
            retry_after,
            rng,
        )
        else {
            queue.push_front(filename);
            break;
        };
        match node
            .registry
            .start_receiver(&node.socket, &mut node.fault, peer, data_dir, &filename)
        {
            Some(id) => {
                log::info(&format!(
                    "pulling {filename} from {peer} (transfer {id:#06x})"
                ));
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

/// Choose a peer for the next pull: score-weighted (peer ranking, M5),
/// least-loaded, not already tried for this job, peers slightly preferred
/// over the bootstrap once their scores are comparable.
fn pick_peer(
    node: &Node,
    active: &HashMap<u16, ActiveJob>,
    tried: &HashMap<String, HashSet<SocketAddr>>,
    parents: &HashSet<SocketAddr>,
    parent_wait: &mut HashMap<String, Instant>,
    filename: &str,
    local_addr: SocketAddr,
    bootstrap: SocketAddr,
    retry_after: &HashMap<(String, SocketAddr), Instant>,
    rng: &mut Rng,
) -> Option<SocketAddr> {
    let tried_for = tried.get(filename);
    let inflight = |p: &SocketAddr| active.values().filter(|j| &j.peer == p).count();
    // Candidates are resolved to their best reachable address (LAN path
    // preferred, N3). Fresh positive inventory is preferred for every
    // segment, including the live edge; the master remains the fallback.
    let now = Instant::now();
    let mut candidates: Vec<SocketAddr> = node
        .peers
        .keys()
        .map(|p| node.effective_addr(*p))
        .filter(|p| *p != local_addr)
        .filter(|p| node.peer_may_have(*p, filename, now))
        .filter(|p| {
            let cooling = retry_after
                .get(&(filename.to_string(), *p))
                .map(|until| now < *until)
                .unwrap_or(false);
            !cooling && tried_for.map(|s| !s.contains(p)).unwrap_or(true)
        })
        .filter(|p| inflight(p) < MAX_INFLIGHT_PER_PEER)
        .collect();
    let parent_sources: Vec<SocketAddr> = candidates
        .iter()
        .copied()
        .filter(|peer| parents.contains(peer))
        .filter(|peer| node.peer_availability(*peer, filename, now) == Some(true))
        .collect();
    if !parent_sources.is_empty() {
        parent_wait.remove(filename);
        candidates = parent_sources;
    } else {
        let has_known_parent = parents.iter().any(|parent| {
            *parent != local_addr && node.peers.contains_key(parent)
        });
        if has_known_parent {
            let since = parent_wait.entry(filename.to_string()).or_insert(now);
            if now.duration_since(*since) < PARENT_WAIT {
                return None;
            }
        }
    }
    candidates = prefer_advertised_peers(candidates, bootstrap, |peer| {
        node.peer_availability(peer, filename, now)
    });
    if candidates.is_empty() {
        return None;
    }
    let weight = |p: &SocketAddr| -> f64 {
        let inflight = inflight(p) as f64;
        let score = node
            .peer_stats
            .get(p)
            .map(|s| s.score as f64)
            .unwrap_or(50.0);
        let fresh = if node.path_fresh(*p, now) { 1.25 } else { 0.7 };
        let availability = match node.peer_availability(*p, filename, now) {
            Some(true) => 3.0,
            Some(false) => 0.0,
            None => 0.35,
        };
        let mut w = (score + 1.0) / (inflight + 1.0) * fresh * availability;
        if *p == bootstrap {
            w *= 0.9; // retain the swarm's origin fallback role
        }
        w.max(0.001)
    };
    let total: f64 = candidates.iter().map(weight).sum();
    let mut roll = (rng.next() as f64 / u64::MAX as f64) * total;
    for p in candidates {
        roll -= weight(&p);
        if roll <= 0.0 {
            return Some(p);
        }
    }
    None
}

fn prefer_advertised_peers<F>(
    candidates: Vec<SocketAddr>,
    bootstrap: SocketAddr,
    availability: F,
) -> Vec<SocketAddr>
where
    F: Fn(SocketAddr) -> Option<bool>,
{
    let peer_sources: Vec<SocketAddr> = candidates
        .iter()
        .copied()
        .filter(|p| *p != bootstrap)
        .filter(|p| availability(*p) == Some(true))
        .collect();
    if !peer_sources.is_empty() {
        // Once any non-origin peer proves it has the piece, keep the origin
        // out of this request entirely. The master is a fallback, not a
        // competing source for every replicated segment.
        return peer_sources;
    }

    // If the origin has a fresh positive answer, unknown peers are not worth
    // a NOT_FOUND trial. Unknown peers remain useful for segments outside the
    // origin's inventory window, or while the origin's inventory is stale.
    if availability(bootstrap) == Some(true) {
        candidates
            .into_iter()
            .filter(|p| *p == bootstrap)
            .collect()
    } else {
        candidates
    }
}

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

fn segment_count(data: &[u8]) -> usize {
    transfer::parse_manifest(data).len()
}

fn playback_holdback_segments() -> usize {
    std::env::var("QSTREAM_PLAYBACK_HOLDBACK_SEGMENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_PLAYBACK_HOLDBACK_SEGMENTS)
}

/// Atomically update the player-only playlist. `live.m3u8` remains the raw
/// master manifest used for synchronization and UDP manifest responses.
fn refresh_playback_manifest(data_dir: &Path, holdback_segments: usize) -> io::Result<()> {
    let manifest = fs::read(data_dir.join("live.m3u8"))?;
    let playback = data_dir.join("playback.m3u8");
    let Some(data) = http::playback_playlist(data_dir, &manifest, holdback_segments) else {
        let _ = fs::remove_file(playback);
        return Ok(());
    };
    if fs::read(&playback).map(|old| old == data).unwrap_or(false) {
        return Ok(());
    }
    let tmp = data_dir.join("playback.m3u8.tmp");
    fs::write(&tmp, data)?;
    fs::rename(tmp, playback)
}

/// Enqueue manifest segments that are missing locally and not already
/// queued/in-flight and not in the failure cooldown.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertised_peer_replaces_master_as_source() {
        let master = "127.0.0.1:3333".parse().unwrap();
        let peer = "127.0.0.1:4444".parse().unwrap();
        let candidates = vec![master, peer];
        let selected = prefer_advertised_peers(candidates, master, |addr| {
            (addr == peer).then_some(true)
        });
        assert_eq!(selected, vec![peer]);
    }

    #[test]
    fn master_remains_fallback_without_advertised_peer() {
        let master = "127.0.0.1:3333".parse().unwrap();
        let peer = "127.0.0.1:4444".parse().unwrap();
        let candidates = vec![master, peer];
        let selected = prefer_advertised_peers(candidates.clone(), master, |_| None);
        assert_eq!(selected, candidates);
    }

    #[test]
    fn unknown_peer_does_not_compete_with_positive_master() {
        let master = "127.0.0.1:3333".parse().unwrap();
        let peer = "127.0.0.1:4444".parse().unwrap();
        let selected = prefer_advertised_peers(vec![master, peer], master, |addr| {
            (addr == master).then_some(true)
        });
        assert_eq!(selected, vec![master]);
    }
}

fn sync_queue(
    data_dir: &Path,
    manifest: &[u8],
    queue: &mut VecDeque<String>,
    queued: &mut HashSet<String>,
    failed_at: &HashMap<String, Instant>,
) {
    for filename in transfer::parse_manifest(manifest) {
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
        // Newest first: live players want the edge, and older playlist
        // segments roll off anyway (DEVLOG: oldest-first left the peer far
        // behind the edge, 404 storm for the player).
        queue.push_front(filename);
    }
}
