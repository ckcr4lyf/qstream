//! Shared node core (SPEC.md §7.5): one socket, message dispatch, transfer
//! registry, peer registry, fault injection, peer stats/ranking (M5).
//! Used by both master and peer.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use crate::fault::FaultInjector;
use crate::log;
use crate::protocol::{self, Message, PEER_PARENT, PEER_SAME_IP, PEER_UPNP_MAPPED};
use crate::transfer::{self, RegEvent, TransferRegistry};

/// PING every peer on this cadence (N2): each PING is a keep-alive that
/// keeps both NATs' mappings alive and doubles as a punch (simultaneous
/// open happens naturally when both sides ping).
pub const PING_INTERVAL: Duration = Duration::from_secs(10);
/// LAN beacon cadence (N3): a broadcast PING announces the node on the LAN.
pub const BEACON_INTERVAL: Duration = Duration::from_secs(5);
/// A path counts as fresh while a PONG was received within this window.
pub const PATH_FRESH: Duration = Duration::from_secs(30);

/// Events a caller (the peer) may want to react to.
#[derive(Debug)]
pub enum Event {
    /// A HANDSHAKE_RESPONSE arrived from `src` (we sent a handshake request).
    HandshakeResponse { src: SocketAddr, name: String },
    /// A MANIFEST_RESPONSE arrived with the raw m3u8 bytes.
    ManifestResponse { data: Vec<u8> },
    /// A PEERLIST_RESPONSE arrived with discovered peers (addr + flags).
    PeerlistResponse { peers: Vec<(SocketAddr, u8)> },
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
    RetryableNotFound,
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
    pub downloaded_bytes: u64,
    pub nf_pulls: u32,
    pub timeouts: u32,
    pub other_fails: u32,
    pub served: u32,
    pub nf_served: u32,
    pub sender_fails: u32,
    pub uploaded_bytes: u64,
    pub latency_ms: u32, // EWMA of first-packet latency
    pub score: u32,
    pub last_seen: Instant,
}

impl PeerStat {
    fn new(name: String) -> Self {
        PeerStat {
            name,
            pulls: 0,
            downloaded_bytes: 0,
            nf_pulls: 0,
            timeouts: 0,
            other_fails: 0,
            served: 0,
            nf_served: 0,
            sender_fails: 0,
            uploaded_bytes: 0,
            latency_ms: 0,
            score: 50,
            last_seen: Instant::now(),
        }
    }

    fn adjust(&mut self, delta: i32) {
        self.score = (self.score as i32 + delta).clamp(0, 100) as u32;
    }
}

/// Shared stats snapshot served at /peers (text), /stats (JSON) and
/// /metrics (Prometheus exposition).
#[derive(Default)]
pub struct StatsSnapshot {
    pub lines: Vec<String>,
    pub json: String,
    pub metrics: String,
}

/// Reachability state for one peer address (N2): when did we last ping it
/// and get a PONG, and was the path discovered on the LAN (beacon)?
#[derive(Debug, Clone)]
pub struct PathState {
    pub last_ping: Instant,
    pub last_pong: Option<Instant>,
    pub lan: bool,
}

impl PathState {
    fn fresh(&self, now: Instant) -> bool {
        self.last_pong
            .map(|t| now.duration_since(t) < PATH_FRESH)
            .unwrap_or(false)
    }
}

pub struct Node {
    pub socket: UdpSocket,
    pub name: String,
    pub role: &'static str,
    pub manifest_path: PathBuf,
    pub registry: TransferRegistry,
    /// Peers we know about (handshaked or discovered), keyed by address.
    pub peers: HashMap<SocketAddr, PeerInfo>,
    /// Quality stats per peer, keyed by address (M5).
    pub peer_stats: HashMap<SocketAddr, PeerStat>,
    /// Outgoing-datagram fault injection (M5).
    pub fault: FaultInjector,
    /// Our observed public endpoint (from handshake responses, N1).
    pub my_public: Option<SocketAddr>,
    /// Our claimed public endpoint (UPnP mapping, N4), if any.
    pub claimed: Option<SocketAddr>,
    /// Random per-node nonce stamped in PINGs; own broadcast echoes carry it
    /// back and are ignored (DEVLOG: beacon self-discovery bug).
    beacon_nonce: u32,
    /// Claimed endpoints reported by peers (N1): peer addr -> claimed.
    claims: HashMap<SocketAddr, SocketAddr>,
    /// Ping/pong reachability per peer address (N2).
    paths: HashMap<SocketAddr, PathState>,
    /// Recent segment inventories learned from upgraded peers. Inventories are
    /// advisory and expire quickly because a live peer's store changes fast.
    availability: HashMap<SocketAddr, (protocol::SegmentAvailability, Instant)>,
    /// Exact negative answers from a peer. These suppress retries while a
    /// positive bitmap catches up with retention or filesystem changes.
    missing_segments: HashMap<(SocketAddr, u64), Instant>,
    /// Per-segment origin seed assignments. The master serves a segment to a
    /// bounded set of peers; those peers fan it out to the rest of the swarm.
    origin_seeders: HashMap<String, HashSet<SocketAddr>>,
    origin_seed_assignments: u64,
    origin_seed_denials: u64,
    local_addr: SocketAddr,
    next_ping: Instant,
    next_beacon: Instant,
    started: Instant,
    downloaded_total: u64,
    downloaded_bytes: u64,
    served_total: u64,
    served_bytes: u64,
    /// Pending pull queue depth + in-flight jobs (peer mode; set by peer.rs).
    pub queue_depth: u64,
    pub inflight: u64,
    retention_secs: u64,
    next_snapshot: Instant,
    next_rank_log: Instant,
    next_prune: Instant,
    stats_sink: Option<Arc<Mutex<StatsSnapshot>>>,
}

impl Node {
    pub fn new(
        socket: UdpSocket,
        name: String,
        role: &'static str,
        manifest_path: PathBuf,
        segment_root: PathBuf,
        fault: FaultInjector,
        stats_sink: Option<Arc<Mutex<StatsSnapshot>>>,
    ) -> Node {
        let retention_secs = std::env::var("QSTREAM_RETENTION_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        if fault.enabled() {
            log::info(&fault.summary());
        }
        let local_addr = socket
            .local_addr()
            .unwrap_or(SocketAddr::from(([127, 0, 0, 1], 0)));
        let beacon_nonce = crate::fault::Rng::new(0).next() as u32;
        Node {
            socket,
            name,
            role,
            manifest_path,
            registry: TransferRegistry::new(segment_root),
            peers: HashMap::new(),
            peer_stats: HashMap::new(),
            fault,
            my_public: None,
            claimed: None,
            beacon_nonce,
            claims: HashMap::new(),
            paths: HashMap::new(),
            availability: HashMap::new(),
            missing_segments: HashMap::new(),
            origin_seeders: HashMap::new(),
            origin_seed_assignments: 0,
            origin_seed_denials: 0,
            local_addr,
            next_ping: Instant::now() + PING_INTERVAL,
            next_beacon: Instant::now() + BEACON_INTERVAL,
            started: Instant::now(),
            downloaded_total: 0,
            downloaded_bytes: 0,
            served_total: 0,
            served_bytes: 0,
            queue_depth: 0,
            inflight: 0,
            retention_secs,
            next_snapshot: Instant::now() + Duration::from_secs(5),
            next_rank_log: Instant::now() + Duration::from_secs(60),
            next_prune: Instant::now() + Duration::from_secs(30),
            stats_sink,
        }
    }

    /// Set our claimed public endpoint (UPnP mapping, N4).
    pub fn set_claimed(&mut self, addr: SocketAddr) {
        self.claimed = Some(addr);
        log::info(&format!("UPnP mapping claimed: {addr}"));
    }

    /// Is `addr` a LAN path (discovered via beacon)?
    pub fn is_lan_path(&self, addr: SocketAddr) -> bool {
        self.paths.get(&addr).map(|p| p.lan).unwrap_or(false)
    }

    /// Is the direct path to `addr` fresh (PONG within PATH_FRESH)?
    pub fn path_fresh(&self, addr: SocketAddr, now: Instant) -> bool {
        self.paths.get(&addr).map(|p| p.fresh(now)).unwrap_or(false)
    }

    /// Record a peer's compact recent segment inventory.
    pub fn record_availability(
        &mut self,
        peer: SocketAddr,
        availability: protocol::SegmentAvailability,
    ) {
        let now = Instant::now();
        for number in 0..protocol::AVAILABILITY_MASK_BITS {
            if availability.mask & (1 << number) != 0 {
                self.missing_segments
                    .remove(&(peer, availability.newest.saturating_sub(number as u64)));
            }
        }
        self.availability.insert(peer, (availability, now));
    }

    /// Record an exact negative response for a requested segment.
    pub fn record_missing(&mut self, peer: SocketAddr, filename: &str) {
        const MISSING_TTL: Duration = Duration::from_secs(15);
        if let Some(number) = segment_number(filename) {
            self.missing_segments
                .insert((peer, number), Instant::now() + MISSING_TTL);
        }
    }

    /// Decide whether the master may seed `filename` to `peer`. A peer that
    /// already has a lease remains allowed; new leases stop at the budget.
    fn origin_seed_allowed(&mut self, filename: &str, peer: SocketAddr) -> bool {
        if self.role != "master" || !self.registry.has_segment(filename) {
            return true;
        }
        const DEFAULT_ORIGIN_SEEDERS: usize = 2;
        let limit = std::env::var("QSTREAM_ORIGIN_SEEDERS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_ORIGIN_SEEDERS);
        let now = Instant::now();
        let viable: HashSet<SocketAddr> = self
            .origin_seeders
            .get(filename)
            .into_iter()
            .flat_map(|seeders| seeders.iter().copied())
            .filter(|candidate| {
                self.peers.contains_key(candidate)
                    && self.peer_availability(*candidate, filename, now) != Some(false)
            })
            .collect();
        let seeders = self
            .origin_seeders
            .entry(filename.to_string())
            .or_default();
        seeders.retain(|candidate| viable.contains(candidate));
        let requester_is_remote = remote_public(peer);
        let reachable_seeders: HashSet<SocketAddr> = seeders
            .iter()
            .copied()
            .filter(|candidate| !requester_is_remote || remote_public(*candidate))
            .collect();
        if seed_lease_allowed(&reachable_seeders, peer, limit) {
            if !seeders.contains(&peer) {
                seeders.insert(peer);
                self.origin_seed_assignments += 1;
                log::debug(&format!(
                    "origin seed lease {} -> {} ({}/{})",
                    filename,
                    peer,
                    seeders.len(),
                    limit
                ));
            }
            return true;
        }

        // Never deny the only recovery path. A lease is only effective after
        // at least one other known peer has a fresh positive inventory bit.
        // This keeps the origin bounded during normal fanout but admits a new
        // seed when the current lease holders failed or are still catching up.
        let now = Instant::now();
        let peer_source_exists = self.peers.keys().any(|candidate| {
            *candidate != peer
                && (!requester_is_remote || remote_public(*candidate))
                && self.peer_availability(*candidate, filename, now) == Some(true)
        });
        if !peer_source_exists {
            let seeders = self
                .origin_seeders
                .entry(filename.to_string())
                .or_default();
            seeders.insert(peer);
            self.origin_seed_assignments += 1;
            return true;
        }
        self.origin_seed_denials += 1;
        false
    }

    /// Remove seed leases for peers that have left the master registry.
    fn prune_origin_seeders(&mut self) {
        let known: HashSet<SocketAddr> = self.peers.keys().copied().collect();
        self.origin_seeders.retain(|_, seeders| {
            seeders.retain(|peer| known.contains(peer));
            !seeders.is_empty()
        });
    }

    /// Push the current store inventory to known peers after a segment lands.
    /// This is an unsolicited PONG by design: it remains decodable by legacy
    /// peers and avoids waiting for the next 10-second keep-alive cycle.
    pub fn announce_availability(&mut self) {
        let pong = Message::Pong {
            availability: self.registry.segment_availability(),
        };
        let bytes = protocol::encode(&pong);
        for peer in self.peers.keys().copied().collect::<Vec<_>>() {
            self.send(&bytes, peer);
        }
    }

    /// Return a fresh inventory answer for `filename` when one is known.
    /// `None` means the peer has no usable answer, so callers should retain
    /// it as a fallback candidate rather than treating it as absent.
    pub fn peer_availability(
        &self,
        peer: SocketAddr,
        filename: &str,
        now: Instant,
    ) -> Option<bool> {
        const AVAILABILITY_TTL: Duration = Duration::from_secs(15);
        let number = segment_number(filename)?;
        if let Some(expires_at) = self.missing_segments.get(&(peer, number)) {
            if now < *expires_at {
                return Some(false);
            }
        }
        let (availability, observed_at) = self.availability.get(&peer)?;
        if now.duration_since(*observed_at) > AVAILABILITY_TTL {
            return None;
        }
        availability.contains(number)
    }

    /// Return false only when a recent inventory explicitly says the peer
    /// lacks `filename`; unknown or stale inventories remain eligible.
    pub fn peer_may_have(&self, peer: SocketAddr, filename: &str, now: Instant) -> bool {
        self.peer_availability(peer, filename, now) != Some(false)
    }

    /// Resolve a peer address to the best address to reach it: if the same
    /// peer name is known via a LAN path, use that (N3, connectivity
    /// ladder tier 1).
    pub fn effective_addr(&self, addr: SocketAddr) -> SocketAddr {
        let name = self.peers.get(&addr).map(|p| p.name.clone());
        if let Some(name) = name {
            for (a, info) in &self.peers {
                if info.name == name && a != &addr && self.is_lan_path(*a) {
                    return *a;
                }
            }
        }
        addr
    }

    /// Record a PONG for `src` (fresh direct path).
    fn record_pong(&mut self, src: SocketAddr, now: Instant) {
        let entry = self.paths.entry(src).or_insert(PathState {
            last_ping: now,
            last_pong: None,
            lan: false,
        });
        entry.last_pong = Some(now);
        entry.last_ping = now;
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
        self.paths.entry(addr).or_insert(PathState {
            last_ping: Instant::now(),
            last_pong: None,
            lan: false,
        });
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
    pub fn record_pull(
        &mut self,
        peer: SocketAddr,
        result: PullResult,
        latency_ms: Option<u64>,
        bytes: u64,
    ) {
        let name = self
            .peers
            .get(&peer)
            .map(|p| p.name.clone())
            .unwrap_or_else(|| peer.to_string());
        let stat = self
            .peer_stats
            .entry(peer)
            .or_insert_with(|| PeerStat::new(name));
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
                stat.downloaded_bytes += bytes;
                self.downloaded_total += 1;
                self.downloaded_bytes += bytes;
            }
            PullResult::NotFound => {
                // A definitive absence is availability churn, not bad
                // service. Keep it visible without punishing the score.
                stat.nf_pulls += 1;
            }
            PullResult::RetryableNotFound => {
                // Temporary origin admission denial is not evidence that a
                // peer lacks the segment and should not pollute NF counts.
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
    pub fn record_serve(&mut self, peer: SocketAddr, result: ServeResult, bytes: u64) {
        let name = self
            .peers
            .get(&peer)
            .map(|p| p.name.clone())
            .unwrap_or_else(|| peer.to_string());
        let stat = self
            .peer_stats
            .entry(peer)
            .or_insert_with(|| PeerStat::new(name));
        match result {
            ServeResult::Served => {
                stat.served += 1;
                stat.adjust(2);
                stat.uploaded_bytes += bytes;
                self.served_total += 1;
                self.served_bytes += bytes;
            }
            ServeResult::NotFound => {
                stat.nf_served += 1;
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
                RegEvent::Served { src, bytes } => {
                    self.record_serve(src, ServeResult::Served, bytes)
                }
                RegEvent::NotFound { src } => self.record_serve(src, ServeResult::NotFound, 0),
                RegEvent::SenderFailed { src } => {
                    self.record_serve(src, ServeResult::SenderFailed, 0)
                }
            }
        }

        self.ping_cycle(now);
        self.prune_origin_seeders();
        if self.prune_segments(now) {
            self.announce_availability();
        }
        self.publish_snapshot(now);
        self.log_ranking(now);
    }

    /// PING every known peer (keep-alive + punch, N2) and, for peers, a
    /// LAN beacon (broadcast PING, N3).
    fn ping_cycle(&mut self, now: Instant) {
        if now >= self.next_ping {
            self.next_ping = now + PING_INTERVAL;
            let ping = Message::Ping {
                nonce: self.beacon_nonce,
                name: self.name.clone(),
            };
            for addr in self.peers.keys().copied().collect::<Vec<_>>() {
                self.send(&protocol::encode(&ping), addr);
                self.paths.entry(addr).or_insert(PathState {
                    last_ping: now,
                    last_pong: None,
                    lan: false,
                });
            }
        }
        if self.role == "peer" && now >= self.next_beacon {
            self.next_beacon = now + BEACON_INTERVAL;
            let ping = Message::Ping {
                nonce: self.beacon_nonce,
                name: self.name.clone(),
            };
            let broadcast = SocketAddr::from(([255, 255, 255, 255], self.local_addr.port()));
            self.send(&protocol::encode(&ping), broadcast);
        }
    }

    /// Delete segments that rolled out of the playlist and are older than
    /// the retention window (M5): viewers may be 3-4 s behind the edge, so
    /// old pieces must stay servable for a while. `QSTREAM_RETENTION_SECS`.
    /// Returns true when the advertised inventory changed.
    fn prune_segments(&mut self, now: Instant) -> bool {
        if self.retention_secs == 0 || now < self.next_prune {
            return false;
        }
        self.next_prune = now + Duration::from_secs(30);
        let manifest = fs::read(&self.manifest_path).unwrap_or_default();
        let in_playlist: HashSet<String> =
            transfer::parse_manifest(&manifest).into_iter().collect();
        let retention = Duration::from_secs(self.retention_secs);
        let root = self.registry.segment_root().to_path_buf();
        let Ok(entries) = fs::read_dir(&root) else {
            return false;
        };
        let mut removed = false;
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
            let Ok(modified) = meta.modified() else {
                continue;
            };
            let Ok(age) = SystemTime::now().duration_since(modified) else {
                continue;
            };
            if age >= retention {
                match fs::remove_file(entry.path()) {
                    Ok(()) => {
                        removed = true;
                        log::debug(&format!("pruned old segment {name}"));
                    }
                    Err(e) => log::debug(&format!("prune {name} failed: {e}")),
                }
            }
        }
        removed
    }

    /// Refresh the shared stats snapshot the HTTP server serves at /peers
    /// and /stats.
    fn publish_snapshot(&mut self, now: Instant) {
        if now < self.next_snapshot {
            return;
        }
        self.next_snapshot = now + Duration::from_secs(5);
        if let Some(sink) = &self.stats_sink {
            if let Ok(mut guard) = sink.lock() {
                guard.lines = self.stats_lines(now);
                guard.json = self.stats_json(now);
                guard.metrics = self.metrics_text(now);
            }
        }
    }

    fn store_bytes(&self) -> u64 {
        let root = self.registry.segment_root();
        std::fs::read_dir(root)
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|e| {
                        let n = e.file_name().to_string_lossy().to_string();
                        n.starts_with("seg_") && n.ends_with(".ts")
                    })
                    .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
                    .sum()
            })
            .unwrap_or(0)
    }

    /// Newest segment number in the local manifest copy (the live edge the
    /// node knows about).
    fn manifest_edge(&self) -> u64 {
        std::fs::read(&self.manifest_path)
            .ok()
            .and_then(|d| transfer::parse_manifest(&d).into_iter().next_back())
            .and_then(|name| {
                name.strip_prefix("seg_")
                    .and_then(|n| n.strip_suffix(".ts"))
                    .and_then(|n| n.parse::<u64>().ok())
            })
            .unwrap_or(0)
    }

    /// (count, newest segment number) in the local store.
    fn store_segments(&self) -> (u64, u64) {
        let root = self.registry.segment_root();
        let mut count = 0u64;
        let mut newest = 0u64;
        if let Ok(entries) = std::fs::read_dir(root) {
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                if let Some(rest) = name
                    .strip_prefix("seg_")
                    .and_then(|n| n.strip_suffix(".ts"))
                {
                    if let Ok(num) = rest.parse::<u64>() {
                        count += 1;
                        newest = newest.max(num);
                    }
                }
            }
        }
        (count, newest)
    }

    fn rss_bytes() -> u64 {
        // /proc/self/statm: size resident shared text lib data dt — RSS is
        // the second field, in pages.
        std::fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| {
                s.split_whitespace()
                    .nth(1)
                    .map(|p| p.parse::<u64>().unwrap_or(0))
            })
            .map(|pages| pages * 4096)
            .unwrap_or(0)
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
            let path = match self.paths.get(addr) {
                Some(p) if p.lan => "lan".to_string(),
                Some(p) if p.fresh(now) => "fresh".to_string(),
                _ => "stale".to_string(),
            };
            let inventory = self
                .availability
                .get(addr)
                .filter(|(_, observed)| now.duration_since(*observed) <= Duration::from_secs(15))
                .map(|(availability, _)| {
                    format!(
                        " newest={} mask={:04x}",
                        availability.newest, availability.mask
                    )
                })
                .unwrap_or_default();
            lines.push(format!(
                "peer {} {} score={} pulls={} downloaded_bytes={} nf_pulls={} timeouts={} other_fails={} served={} uploaded_bytes={} nf_served={} sender_fails={} latency={}ms path={}{}",
                s.name, addr, s.score, s.pulls, s.downloaded_bytes, s.nf_pulls, s.timeouts, s.other_fails,
                s.served, s.uploaded_bytes, s.nf_served, s.sender_fails, s.latency_ms, path, inventory
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

    /// JSON stats document for GET /stats (M5). Hand-rolled JSON — std has
    /// no serializer; all values are numeric or escaped strings.
    fn stats_json(&self, now: Instant) -> String {
        let uptime = now.duration_since(self.started).as_secs();
        let mut ranked: Vec<(&SocketAddr, &PeerStat)> = self.peer_stats.iter().collect();
        ranked.sort_by(|a, b| b.1.score.cmp(&a.1.score).then(a.0.cmp(b.0)));
        let peers_json: Vec<String> = ranked
            .iter()
            .map(|(addr, s)| {
                format!(
                    "{{\"name\":\"{}\",\"addr\":\"{}\",\"score\":{},\"pulls\":{},\"downloaded_bytes\":{},\"nf_pulls\":{},\"timeouts\":{},\"served\":{},\"uploaded_bytes\":{},\"nf_served\":{},\"latency_ms\":{}}}",
                    json_escape(&s.name),
                    addr,
                    s.score,
                    s.pulls,
                    s.downloaded_bytes,
                    s.nf_pulls,
                    s.timeouts,
                    s.served,
                    s.uploaded_bytes,
                    s.nf_served,
                    s.latency_ms
                )
            })
            .collect();
        let fault_json = if self.fault.enabled() {
            format!(
                "\"fault\":{{\"dropped\":{},\"emitted\":{}}}",
                self.fault.stats.dropped, self.fault.stats.emitted
            )
        } else {
            "\"fault\":null".to_string()
        };
        let (store_count, local_newest) = self.store_segments();
        let edge = self.manifest_edge();
        let public = self
            .my_public
            .map(|a| format!("\"{}\"", a))
            .unwrap_or_else(|| "null".to_string());
        format!(
            "{{\"version\":\"{}\",\"node\":\"{}\",\"role\":\"{}\",\"uptime_secs\":{},\"peers_in_swarm\":{},\"public_endpoint\":{},\"peers\":[{}],\"downloaded\":{{\"segments\":{},\"bytes\":{}}},\"uploaded\":{{\"segments\":{},\"bytes\":{}}},\"store_bytes\":{},\"store_segments\":{},\"edge_segment\":{},\"local_newest\":{},\"catch_up\":{},\"active_transfers\":{},\"queue_depth\":{},\"inflight\":{},{},\"rss_bytes\":{}}}",
            env!("CARGO_PKG_VERSION"),
            json_escape(&self.name),
            self.role,
            uptime,
            self.peers.len(),
            public,
            peers_json.join(","),
            self.downloaded_total,
            self.downloaded_bytes,
            self.served_total,
            self.served_bytes,
            self.store_bytes(),
            store_count,
            edge,
            local_newest,
            edge.saturating_sub(local_newest),
            self.registry.active_count(),
            self.queue_depth,
            self.inflight,
            fault_json,
            Self::rss_bytes()
        )
    }

    /// Prometheus text exposition for GET /metrics (M5). Flat series with
    /// a `node` label; per-peer series carry a `peer` label.
    fn metrics_text(&self, now: Instant) -> String {
        const DEFS: &[(&str, &str, &str)] = &[
            (
                "qstream_uptime_seconds",
                "Seconds since node start.",
                "gauge",
            ),
            (
                "qstream_peers_in_swarm",
                "Number of peers currently known to this node.",
                "gauge",
            ),
            (
                "qstream_downloaded_segments_total",
                "Segments fully downloaded by this node.",
                "counter",
            ),
            (
                "qstream_downloaded_bytes_total",
                "Bytes fully downloaded by this node.",
                "counter",
            ),
            (
                "qstream_uploaded_segments_total",
                "Segments served to other nodes.",
                "counter",
            ),
            (
                "qstream_uploaded_bytes_total",
                "Bytes served to other nodes.",
                "counter",
            ),
            (
                "qstream_store_segments",
                "Segments currently in the local store.",
                "gauge",
            ),
            (
                "qstream_store_bytes",
                "Bytes currently in the local store.",
                "gauge",
            ),
            (
                "qstream_edge_segment",
                "Newest segment number in the synced manifest.",
                "gauge",
            ),
            (
                "qstream_local_newest",
                "Newest segment number in the local store.",
                "gauge",
            ),
            (
                "qstream_catch_up",
                "Segments behind the live edge (0 = caught up).",
                "gauge",
            ),
            (
                "qstream_active_transfers",
                "In-flight senders + receivers.",
                "gauge",
            ),
            (
                "qstream_queue_depth",
                "Pending segment pulls in the queue.",
                "gauge",
            ),
            ("qstream_inflight", "Active pull jobs.", "gauge"),
            (
                "qstream_rss_bytes",
                "Resident memory of this process.",
                "gauge",
            ),
            (
                "qstream_fault_dropped_total",
                "Datagrams dropped by fault injection.",
                "counter",
            ),
            (
                "qstream_fault_emitted_total",
                "Datagrams emitted by fault injection.",
                "counter",
            ),
            (
                "qstream_origin_seed_assignments_total",
                "Origin seed leases assigned to peers.",
                "counter",
            ),
            (
                "qstream_origin_seed_denials_total",
                "Origin seed requests denied after the per-segment budget.",
                "counter",
            ),
            (
                "qstream_peer_score",
                "Quality score 0-100 for a known peer.",
                "gauge",
            ),
            (
                "qstream_peer_pulls_total",
                "Successful pulls from a peer.",
                "counter",
            ),
            (
                "qstream_peer_downloaded_bytes_total",
                "Payload bytes downloaded from a peer.",
                "counter",
            ),
            (
                "qstream_peer_nf_pulls_total",
                "NOT_FOUND responses from a peer.",
                "counter",
            ),
            (
                "qstream_peer_timeouts_total",
                "No-response failures from a peer.",
                "counter",
            ),
            (
                "qstream_peer_served_total",
                "Segments served to a peer.",
                "counter",
            ),
            (
                "qstream_peer_uploaded_bytes_total",
                "Payload bytes uploaded to a peer.",
                "counter",
            ),
            (
                "qstream_peer_latency_ms",
                "EWMA first-packet latency to a peer.",
                "gauge",
            ),
        ];

        let mut out = String::with_capacity(4096);
        for (name, help, ty) in DEFS {
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {ty}\n"));
        }
        let mut s = |name: &str, labels: &str, value: u64| {
            out.push_str(&format!("{name}{labels} {value}\n"));
        };

        let nl = format!("{{node=\"{}\"}}", json_escape(&self.name));
        let uptime = now.duration_since(self.started).as_secs();
        let (store_count, local_newest) = self.store_segments();
        let edge = self.manifest_edge();

        s("qstream_uptime_seconds", &nl, uptime);
        s("qstream_peers_in_swarm", &nl, self.peers.len() as u64);
        s(
            "qstream_downloaded_segments_total",
            &nl,
            self.downloaded_total,
        );
        s("qstream_downloaded_bytes_total", &nl, self.downloaded_bytes);
        s("qstream_uploaded_segments_total", &nl, self.served_total);
        s("qstream_uploaded_bytes_total", &nl, self.served_bytes);
        s("qstream_store_segments", &nl, store_count);
        s("qstream_store_bytes", &nl, self.store_bytes());
        s("qstream_edge_segment", &nl, edge);
        s("qstream_local_newest", &nl, local_newest);
        s("qstream_catch_up", &nl, edge.saturating_sub(local_newest));
        s(
            "qstream_active_transfers",
            &nl,
            self.registry.active_count() as u64,
        );
        s("qstream_queue_depth", &nl, self.queue_depth);
        s("qstream_inflight", &nl, self.inflight);
        s(
            "qstream_origin_seed_assignments_total",
            &nl,
            self.origin_seed_assignments,
        );
        s(
            "qstream_origin_seed_denials_total",
            &nl,
            self.origin_seed_denials,
        );
        s("qstream_rss_bytes", &nl, Self::rss_bytes());
        if self.fault.enabled() {
            s("qstream_fault_dropped_total", &nl, self.fault.stats.dropped);
            s("qstream_fault_emitted_total", &nl, self.fault.stats.emitted);
        }

        let mut ranked: Vec<(&SocketAddr, &PeerStat)> = self.peer_stats.iter().collect();
        ranked.sort_by(|a, b| b.1.score.cmp(&a.1.score).then(a.0.cmp(b.0)));
        for (_, stat) in ranked {
            let pl = format!(
                "{{node=\"{}\",peer=\"{}\"}}",
                json_escape(&self.name),
                json_escape(&stat.name)
            );
            s("qstream_peer_score", &pl, stat.score as u64);
            s("qstream_peer_pulls_total", &pl, stat.pulls as u64);
            s(
                "qstream_peer_downloaded_bytes_total",
                &pl,
                stat.downloaded_bytes,
            );
            s("qstream_peer_nf_pulls_total", &pl, stat.nf_pulls as u64);
            s("qstream_peer_timeouts_total", &pl, stat.timeouts as u64);
            s("qstream_peer_served_total", &pl, stat.served as u64);
            s(
                "qstream_peer_uploaded_bytes_total",
                &pl,
                stat.uploaded_bytes,
            );
            s("qstream_peer_latency_ms", &pl, stat.latency_ms as u64);
        }
        out
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
                    s.name,
                    s.score,
                    s.pulls,
                    s.nf_pulls,
                    s.timeouts,
                    s.other_fails,
                    s.served,
                    s.nf_served,
                    s.sender_fails
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
            Message::HandshakeRequest { claimed, name } => {
                log::info(&format!("handshake from {src} (name: {name})"));
                self.register_peer(src, name.clone());
                self.claims.insert(src, claimed);
                let reply = Message::HandshakeResponse {
                    observed: src,
                    name: self.name.clone(),
                };
                self.send(&protocol::encode(&reply), src);
                Event::None
            }
            Message::HandshakeResponse { observed, name } => {
                log::debug(&format!("handshake response from {src} (name: {name})"));
                if self.my_public != Some(observed) {
                    log::info(&format!(
                        "my public endpoint (as seen by {src}): {observed}"
                    ));
                }
                self.my_public = Some(observed);
                Event::HandshakeResponse { src, name }
            }
            Message::Ping { nonce, name } => {
                if nonce == self.beacon_nonce {
                    // Our own broadcast echo — the kernel delivers a
                    // broadcast back to the sender's socket (DEVLOG).
                    return Event::None;
                }
                if src == self.local_addr {
                    return Event::None;
                }
                let known = self.peers.contains_key(&src);
                if !known && !name.is_empty() {
                    // First contact via PING: a LAN beacon or a punch probe.
                    // Names are display labels, not stable identities: many
                    // peers legitimately use the default name "peer". Keep
                    // simultaneous public endpoints instead of re-keying one
                    // over another based on a colliding name.
                    let display = name.clone();
                    log::info(&format!("discovered peer {display} at {src} (ping)"));
                    self.register_peer(src, display);
                    // A first PING from a private or loopback source is a LAN
                    // beacon. Public PINGs are punch probes and must not be
                    // mislabeled as LAN paths in /peers.
                    let is_lan_source = match src.ip() {
                        IpAddr::V4(ip) => ip.is_private() || ip.is_loopback(),
                        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unicast_link_local(),
                    };
                    if is_lan_source {
                        self.paths.get_mut(&src).map(|p| p.lan = true);
                    }
                }
                let pong = Message::Pong {
                    availability: self.registry.segment_availability(),
                };
                self.send(&protocol::encode(&pong), src);
                Event::None
            }
            Message::Pong { availability } => {
                log::trace(&format!("pong from {src} — direct path fresh"));
                self.record_pong(src, Instant::now());
                if let Some(availability) = availability {
                    self.record_availability(src, availability);
                }
                Event::None
            }
            Message::ManifestRequest => {
                // Re-read from disk every time — the live playlist rolls.
                let data = fs::read(&self.manifest_path).unwrap_or_default();
                let reply = Message::ManifestResponse { data };
                self.send(&protocol::encode(&reply), src);
                Event::None
            }
            Message::ManifestResponse { data } => {
                log::debug(&format!(
                    "manifest response ({} bytes) from {src}",
                    data.len()
                ));
                Event::ManifestResponse { data }
            }
            Message::PeerlistRequest => {
                // Reply with our view, excluding the requester. Each entry
                // carries flags: UPNP_MAPPED if the peer's claimed mapping
                // is what we observe, SAME_IP if it shares the requester's
                // public IP (likely the same NAT/LAN, N1/N3). Loopback and
                // private endpoints are only meaningful to peers on the
                // same host/LAN — don't advertise them to remote peers
                // (DEVLOG: home peer wasted handshakes on 127.0.0.1).
                const MAX_ADVERTISED_PEERS: usize = 16;
                const PARENT_COUNT: usize = 2;
                let requester_remote = remote_public(src);
                let mut candidates: Vec<SocketAddr> = self
                    .peers
                    .keys()
                    .copied()
                    .filter(|p| *p != src)
                    .filter(|p| !(requester_remote && !remote_public(*p)))
                    .collect();
                // Prefer peers that have served less so parent assignments
                // spread origin and upload work across the swarm.
                candidates.sort_by(|a, b| {
                    let a_served = self.peer_stats.get(a).map(|s| s.served).unwrap_or(0);
                    let b_served = self.peer_stats.get(b).map(|s| s.served).unwrap_or(0);
                    a_served.cmp(&b_served).then(a.cmp(b))
                });
                let parents: HashSet<SocketAddr> = candidates
                    .iter()
                    .copied()
                    .take(PARENT_COUNT)
                    .collect();
                let peers: Vec<(SocketAddr, u8)> = candidates
                    .into_iter()
                    .take(MAX_ADVERTISED_PEERS)
                    .map(|p| {
                        let mut flags = 0u8;
                        if self.claims.get(&p) == Some(&p) {
                            flags |= PEER_UPNP_MAPPED;
                        }
                        if same_ip4(p, src) {
                            flags |= PEER_SAME_IP;
                        }
                        if parents.contains(&p) {
                            flags |= PEER_PARENT;
                        }
                        (p, flags)
                    })
                    .collect();
                let n = peers.len();
                let reply = Message::PeerlistResponse { peers };
                self.send(&protocol::encode(&reply), src);
                log::trace(&format!("replied PEERLIST_RESPONSE ({n} peers) to {src}"));
                Event::None
            }
            Message::PeerlistResponse { peers } => {
                log::debug(&format!(
                    "peerlist response ({} peers) from {src}",
                    peers.len()
                ));
                Event::PeerlistResponse { peers }
            }
            Message::SegmentRequest {
                transfer_id,
                filename,
            } => {
                if self.origin_seed_allowed(&filename, src) {
                    self.registry.serve(
                        &self.socket,
                        &mut self.fault,
                        transfer_id,
                        &filename,
                        src,
                    );
                } else {
                    self.registry.reject_not_found(
                        &self.socket,
                        &mut self.fault,
                        transfer_id,
                        src,
                        true,
                    );
                }
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
            Message::SegmentNotFound {
                transfer_id,
                availability,
                retryable,
            } => {
                if let Some(availability) = availability {
                    self.record_availability(src, availability);
                }
                if let Some((filename, retryable)) =
                    self.registry.on_not_found(&self.socket, transfer_id, retryable)
                {
                    if !retryable {
                        self.record_missing(src, &filename);
                    }
                }
                Event::None
            }
            Message::Ack {
                transfer_id,
                ack_type,
                next_start,
                next_count,
            } => {
                self.registry
                    .on_ack(transfer_id, ack_type, next_start, next_count, src);
                Event::None
            }
        }
    }
}

/// Minimal JSON string escaping for the hand-rolled stats document.
pub fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_escape_handles_specials() {
        assert_eq!(json_escape("plain"), "plain");
        assert_eq!(json_escape("a\"b\\c\nd\te"), "a\\\"b\\\\c\\nd\\te");
        assert_eq!(json_escape("\u{1}"), "\\u0001");
    }

    #[test]
    fn peer_stats_keep_directional_payload_bytes() {
        let mut stat = PeerStat::new("peer".to_string());
        stat.downloaded_bytes += 1234;
        stat.uploaded_bytes += 5678;
        assert_eq!(stat.downloaded_bytes, 1234);
        assert_eq!(stat.uploaded_bytes, 5678);
    }

    #[test]
    fn origin_seed_lease_is_bounded_but_reentrant() {
        let first = "127.0.0.1:1001".parse().unwrap();
        let second = "127.0.0.1:1002".parse().unwrap();
        let third = "127.0.0.1:1003".parse().unwrap();
        let seeders = HashSet::from([first, second]);
        assert!(seed_lease_allowed(&seeders, first, 2));
        assert!(!seed_lease_allowed(&seeders, third, 2));
        assert!(seed_lease_allowed(&HashSet::new(), third, 2));
    }
}

/// Do two addresses share an IPv4 address? (Peerlist SAME_IP flag, N1.)
fn same_ip4(a: SocketAddr, b: SocketAddr) -> bool {
    match (a, b) {
        (SocketAddr::V4(x), SocketAddr::V4(y)) => x.ip() == y.ip(),
        _ => false,
    }
}

/// Is `addr` a globally reachable endpoint (not loopback, not RFC1918)?
/// Loopback/private peers of the master are meaningless to a remote peer;
/// they must not be advertised (or handshaken) across the internet.
fn seed_lease_allowed(seeders: &HashSet<SocketAddr>, peer: SocketAddr, limit: usize) -> bool {
    seeders.contains(&peer) || seeders.len() < limit
}

fn segment_number(name: &str) -> Option<u64> {
    name.strip_prefix("seg_")?.strip_suffix(".ts")?.parse().ok()
}

pub fn remote_public(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(v4) => !v4.is_loopback() && !v4.is_private(),
        IpAddr::V6(v6) => !v6.is_loopback() && !v6.is_unicast_link_local(),
    }
}
