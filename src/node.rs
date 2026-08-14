//! Shared node core (SPEC.md §7.5): one socket, message dispatch, transfer
//! registry, peer registry, fault injection, peer stats/ranking (M5).
//! Used by both master and peer.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use crate::fault::FaultInjector;
use crate::log;
use crate::protocol::{self, Message};
use crate::transfer::{self, RegEvent, TransferRegistry};

/// Events a caller (the peer) may want to react to.
#[derive(Debug)]
pub enum Event {
    /// A HANDSHAKE_RESPONSE arrived from `src` (we sent a handshake request).
    HandshakeResponse { src: SocketAddr, name: String },
    /// A MANIFEST_RESPONSE arrived with the raw m3u8 bytes.
    ManifestResponse { data: Vec<u8> },
    /// A PEERLIST_RESPONSE arrived with discovered peers.
    PeerlistResponse { peers: Vec<SocketAddr> },
    /// Nothing the caller needs to act on.
    None,
}

/// A known peer and when we last heard from it.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub name: String,
    pub last_seen: Instant,
}

/// How a pull from a peer ended (requester-side view).
#[derive(Clone, Copy, PartialEq)]
pub enum PullResult {
    Ok,
    NotFound,
    Timeout,
    Other,
}

/// How a serve to a peer ended (server-side view).
#[derive(Clone, Copy, PartialEq)]
pub enum ServeResult {
    Served,
    NotFound,
    SenderFailed,
}

/// Per-peer quality bookkeeping (M5): both sides of every interaction feed
/// a single score per peer, exposed via logs and GET /peers.
#[derive(Debug, Clone)]
pub struct PeerStat {
    pub name: String,
    pub pulls: u32,
    pub nf_pulls: u32,
    pub timeouts: u32,
    pub other_fails: u32,
    pub served: u32,
    pub nf_served: u32,
    pub sender_fails: u32,
    pub latency_ms: u32, // EWMA of first-packet latency
    pub score: u32,
    pub last_seen: Instant,
}

impl PeerStat {
    fn new(name: String) -> Self {
        PeerStat {
            name,
            pulls: 0,
            nf_pulls: 0,
            timeouts: 0,
            other_fails: 0,
            served: 0,
            nf_served: 0,
            sender_fails: 0,
            latency_ms: 0,
            score: 50,
            last_seen: Instant::now(),
        }
    }

    fn adjust(&mut self, delta: i32) {
        self.score = (self.score as i32 + delta).clamp(0, 100) as u32;
    }
}

pub struct Node {
    pub socket: UdpSocket,
    pub name: String,
    pub manifest_path: PathBuf,
    pub registry: TransferRegistry,
    /// Peers we know about (handshaked or discovered), keyed by address.
    pub peers: HashMap<SocketAddr, PeerInfo>,
    /// Quality stats per peer, keyed by address (M5).
    pub peer_stats: HashMap<SocketAddr, PeerStat>,
    /// Outgoing-datagram fault injection (M5).
    pub fault: FaultInjector,
    started: Instant,
    downloaded_total: u64,
    retention_secs: u64,
    next_snapshot: Instant,
    next_rank_log: Instant,
    next_prune: Instant,
    stats_sink: Option<Arc<Mutex<Vec<String>>>>,
}

impl Node {
    pub fn new(
        socket: UdpSocket,
        name: String,
        manifest_path: PathBuf,
        segment_root: PathBuf,
        fault: FaultInjector,
        stats_sink: Option<Arc<Mutex<Vec<String>>>>,
    ) -> Node {
        let retention_secs = std::env::var("QSTREAM_RETENTION_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        if fault.enabled() {
            log::info(&fault.summary());
        }
        Node {
            socket,
            name,
            manifest_path,
            registry: TransferRegistry::new(segment_root),
            peers: HashMap::new(),
            peer_stats: HashMap::new(),
            fault,
            started: Instant::now(),
            downloaded_total: 0,
            retention_secs,
            next_snapshot: Instant::now() + Duration::from_secs(5),
            next_rank_log: Instant::now() + Duration::from_secs(60),
            next_prune: Instant::now() + Duration::from_secs(30),
            stats_sink,
        }
    }

    /// Send one datagram through the fault injector.
    pub fn send(&mut self, bytes: &[u8], dst: SocketAddr) {
        let now = Instant::now();
        self.fault.send(&self.socket, bytes.to_vec(), dst, now);
    }

    pub fn register_peer(&mut self, addr: SocketAddr, name: String) {
        let entry = self.peers.entry(addr).or_insert(PeerInfo {
            name: name.clone(),
            last_seen: Instant::now(),
        });
        entry.last_seen = Instant::now();
        if entry.name != name {
            log::debug(&format!("peer {addr} renamed to {name}"));
            entry.name = name.clone();
        }
        self.peer_stats
            .entry(addr)
            .or_insert_with(|| PeerStat::new(name));
    }

    fn touch_peer(&mut self, addr: SocketAddr) {
        if let Some(info) = self.peers.get_mut(&addr) {
            info.last_seen = Instant::now();
        }
        if let Some(stat) = self.peer_stats.get_mut(&addr) {
            stat.last_seen = Instant::now();
        }
    }

    /// Drop peers we haven't heard from in `ttl`.
    pub fn prune_peers(&mut self, ttl: Duration, now: Instant) {
        let stale: Vec<SocketAddr> = self
            .peers
            .iter()
            .filter(|(_, info)| now.duration_since(info.last_seen) >= ttl)
            .map(|(addr, _)| *addr)
            .collect();
        for addr in stale {
            log::debug(&format!("evicting idle peer {addr}"));
            self.peers.remove(&addr);
        }
    }

    /// Record how a pull from `peer` ended (requester-side ranking input).
    pub fn record_pull(&mut self, peer: SocketAddr, result: PullResult, latency_ms: Option<u64>) {
        let name = self.peers.get(&peer).map(|p| p.name.clone()).unwrap_or_else(|| peer.to_string());
        let stat = self.peer_stats.entry(peer).or_insert_with(|| PeerStat::new(name));
        stat.last_seen = Instant::now();
        match result {
            PullResult::Ok => {
                stat.pulls += 1;
                stat.adjust(2);
                if let Some(ms) = latency_ms {
                    if stat.latency_ms == 0 {
                        stat.latency_ms = ms as u32;
                    } else {
                        stat.latency_ms = (stat.latency_ms * 3 + ms as u32) / 4;
                    }
                }
                self.downloaded_total += 1;
            }
            PullResult::NotFound => {
                stat.nf_pulls += 1;
                stat.adjust(-1);
            }
            PullResult::Timeout => {
                stat.timeouts += 1;
                stat.adjust(-10);
            }
            PullResult::Other => {
                stat.other_fails += 1;
                stat.adjust(-3);
            }
        }
    }

    /// Record how a serve to `peer` ended (server-side ranking input).
    pub fn record_serve(&mut self, peer: SocketAddr, result: ServeResult) {
        let name = self.peers.get(&peer).map(|p| p.name.clone()).unwrap_or_else(|| peer.to_string());
        let stat = self.peer_stats.entry(peer).or_insert_with(|| PeerStat::new(name));
        match result {
            ServeResult::Served => {
                stat.served += 1;
                stat.adjust(1);
            }
            ServeResult::NotFound => {
                stat.nf_served += 1;
                stat.adjust(-1);
            }
            ServeResult::SenderFailed => {
                stat.sender_fails += 1;
                stat.adjust(-5);
            }
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        let now = Instant::now();
        match (self.registry.next_deadline(), self.fault.next_deadline(now)) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    pub fn tick(&mut self, now: Instant) {
        self.registry.tick(&self.socket, &mut self.fault, now);
        self.fault.drain(&self.socket, now);

        // Turn serve outcomes into peer stats.
        for event in self.registry.drain_events() {
            match event {
                RegEvent::Served { src } => self.record_serve(src, ServeResult::Served),
                RegEvent::NotFound { src } => self.record_serve(src, ServeResult::NotFound),
                RegEvent::SenderFailed { src } => self.record_serve(src, ServeResult::SenderFailed),
            }
        }

        self.prune_segments(now);
        self.publish_snapshot(now);
        self.log_ranking(now);
    }

    /// Delete segments that rolled out of the playlist and are older than
    /// the retention window (M5): viewers may be 3-4 s behind the edge, so
    /// old pieces must stay servable for a while. `QSTREAM_RETENTION_SECS`.
    fn prune_segments(&mut self, now: Instant) {
        if self.retention_secs == 0 || now < self.next_prune {
            return;
        }
        self.next_prune = now + Duration::from_secs(30);
        let manifest = fs::read(&self.manifest_path).unwrap_or_default();
        let in_playlist: HashSet<String> = transfer::parse_manifest(&manifest).into_iter().collect();
        let retention = Duration::from_secs(self.retention_secs);
        let root = self.registry.segment_root().to_path_buf();
        let Ok(entries) = fs::read_dir(&root) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            let is_segment = name.starts_with("seg_") && name.ends_with(".ts");
            let is_tmp = name.ends_with(".tmp");
            if !is_segment && !is_tmp {
                continue;
            }
            if in_playlist.contains(&name) {
                continue;
            }
            let Ok(meta) = entry.metadata() else { continue };
            let Ok(modified) = meta.modified() else { continue };
            let Ok(age) = SystemTime::now().duration_since(modified) else { continue };
            if age >= retention {
                match fs::remove_file(entry.path()) {
                    Ok(()) => log::debug(&format!("pruned old segment {name}")),
                    Err(e) => log::debug(&format!("prune {name} failed: {e}")),
                }
            }
        }
    }

    /// Refresh the shared stats snapshot the HTTP server serves at /peers.
    fn publish_snapshot(&mut self, now: Instant) {
        if now < self.next_snapshot {
            return;
        }
        self.next_snapshot = now + Duration::from_secs(5);
        if let Some(sink) = &self.stats_sink {
            if let Ok(mut guard) = sink.lock() {
                *guard = self.stats_lines(now);
            }
        }
    }

    fn stats_lines(&self, now: Instant) -> Vec<String> {
        let uptime = now.duration_since(self.started).as_secs();
        let mut lines = vec![format!(
            "node {} uptime={} downloaded={}",
            self.name, uptime, self.downloaded_total
        )];
        let mut ranked: Vec<(&SocketAddr, &PeerStat)> = self.peer_stats.iter().collect();
        ranked.sort_by(|a, b| b.1.score.cmp(&a.1.score).then(a.0.cmp(b.0)));
        for (addr, s) in ranked {
            lines.push(format!(
                "peer {} {} score={} pulls={} nf_pulls={} timeouts={} other_fails={} served={} nf_served={} sender_fails={} latency={}ms",
                s.name, addr, s.score, s.pulls, s.nf_pulls, s.timeouts, s.other_fails,
                s.served, s.nf_served, s.sender_fails, s.latency_ms
            ));
        }
        if self.fault.enabled() {
            lines.push(format!(
                "fault dropped={} emitted={}",
                self.fault.stats.dropped, self.fault.stats.emitted
            ));
        }
        lines
    }

    fn log_ranking(&mut self, now: Instant) {
        if now < self.next_rank_log {
            return;
        }
        self.next_rank_log = now + Duration::from_secs(60);
        if self.peer_stats.is_empty() {
            return;
        }
        let mut ranked: Vec<(&SocketAddr, &PeerStat)> = self.peer_stats.iter().collect();
        ranked.sort_by(|a, b| b.1.score.cmp(&a.1.score));
        let parts: Vec<String> = ranked
            .iter()
            .take(8)
            .map(|(_, s)| {
                format!(
                    "{} {} (pull {}/{}/{}/{} served {}/{}/{})",
                    s.name, s.score, s.pulls, s.nf_pulls, s.timeouts, s.other_fails,
                    s.served, s.nf_served, s.sender_fails
                )
            })
            .collect();
        log::info(&format!("peer ranking: {}", parts.join(", ")));
    }

    /// Handle one incoming datagram. Returns an event for the caller.
    pub fn handle(&mut self, datagram: &[u8], src: SocketAddr) -> Event {
        let message = match protocol::decode(datagram) {
            Ok(m) => m,
            Err(e) => {
                log::warn(&format!("dropping malformed datagram from {src}: {e}"));
                return Event::None;
            }
        };

        // Any traffic from a known peer refreshes its liveness.
        self.touch_peer(src);

        match message {
            Message::HandshakeRequest { name } => {
                log::info(&format!("handshake from {src} (name: {name})"));
                self.register_peer(src, name);
                let reply = Message::HandshakeResponse {
                    name: self.name.clone(),
                };
                self.send(&protocol::encode(&reply), src);
                Event::None
            }
            Message::HandshakeResponse { name } => {
                log::debug(&format!("handshake response from {src} (name: {name})"));
                Event::HandshakeResponse { src, name }
            }
            Message::ManifestRequest => {
                // Re-read from disk every time — the live playlist rolls.
                let data = fs::read(&self.manifest_path).unwrap_or_default();
                let reply = Message::ManifestResponse { data };
                self.send(&protocol::encode(&reply), src);
                Event::None
            }
            Message::ManifestResponse { data } => {
                log::debug(&format!("manifest response ({} bytes) from {src}", data.len()));
                Event::ManifestResponse { data }
            }
            Message::PeerlistRequest => {
                // Reply with our view, excluding the requester.
                let peers: Vec<SocketAddr> = self
                    .peers
                    .keys()
                    .filter(|p| **p != src)
                    .cloned()
                    .collect();
                let n = peers.len();
                let reply = Message::PeerlistResponse { peers };
                self.send(&protocol::encode(&reply), src);
                log::trace(&format!("replied PEERLIST_RESPONSE ({n} peers) to {src}"));
                Event::None
            }
            Message::PeerlistResponse { peers } => {
                log::debug(&format!("peerlist response ({} peers) from {src}", peers.len()));
                Event::PeerlistResponse { peers }
            }
            Message::SegmentRequest {
                transfer_id,
                filename,
            } => {
                self.registry
                    .serve(&self.socket, &mut self.fault, transfer_id, &filename, src);
                Event::None
            }
            Message::SegmentContents {
                transfer_id,
                packet_number,
                total_packets,
                data,
            } => {
                self.registry.on_content(
                    &self.socket,
                    &mut self.fault,
                    transfer_id,
                    packet_number,
                    total_packets,
                    data,
                    src,
                );
                Event::None
            }
            Message::SegmentNotFound { transfer_id } => {
                self.registry.on_not_found(&self.socket, transfer_id);
                Event::None
            }
            Message::Ack {
                transfer_id,
                ack_type,
                next_start,
                next_count,
            } => {
                self.registry.on_ack(transfer_id, ack_type, next_start, next_count, src);
                Event::None
            }
        }
    }
}
