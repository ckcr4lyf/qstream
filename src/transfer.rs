//! Segment transfer state machines (SPEC.md §5.5, §7; PROTOCOL.pdf §6).
//!
//! Receiver-driven windows: the receiver names the exact next packet range
//! in every ACK, so sender and receiver never need to keep synchronized
//! window sizes. The sender just executes ranges and retransmits its last
//! range on ack timeout; the receiver deduplicates, so blind re-sends are
//! safe (SPEC.md §7.3 convergence).

use std::collections::HashMap;
use std::fs;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::fault::FaultInjector;
use crate::log;
use crate::protocol::{self, AckType, Message, SegmentAvailability, AVAILABILITY_MASK_BITS};

pub const SEGMENT_PACKET_SIZE: usize = 1400;
pub const INITIAL_WINDOW: u16 = 5;
pub const MAX_WINDOW: u16 = 64;
/// Receiver re-request retries (with backoff, ~25 s worst case).
pub const RETRY_LIMIT: u32 = 8;
/// Sender ack-timeout retries (with backoff, outlives the receiver's budget
/// so a burst-stalled window can recover).
pub const SENDER_RETRY_LIMIT: u32 = 30;
pub const COMPLETE_GRACE: Duration = Duration::from_millis(4000);
pub const MAX_CONCURRENT_TRANSFERS: usize = 32;

/// Tunables overridable via env (SPEC.md §7.4).
#[derive(Clone, Copy)]
pub struct Settings {
    pub pace_ms: u64,
    pub quiet_ms: u64,
    pub first_timeout_ms: u64,
}

static SETTINGS: OnceLock<Settings> = OnceLock::new();

pub fn settings() -> &'static Settings {
    SETTINGS.get_or_init(|| {
        let env = |name: &str, default: u64| -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        Settings {
            pace_ms: env("QSTREAM_PACING_MS", 1),
            quiet_ms: env("QSTREAM_QUIET_MS", 150),
            first_timeout_ms: env("QSTREAM_FIRST_TIMEOUT_MS", 4000),
        }
    })
}

fn quiet_period() -> Duration {
    Duration::from_millis(settings().quiet_ms)
}

/// Receiver quiet period adapted to the observed inter-packet gap (M5): a
/// delayed link spaces packets out, and a fixed quiet period would treat the
/// gaps as loss and burn retries. `backoff` grows on repeated re-requests so
/// a burst (all packets lost for a moment) spreads its retries instead of
/// exhausting the budget in the middle of the outage.
fn adaptive_quiet(gap_est: Duration, backoff: u32) -> Duration {
    let base = quiet_period().max(gap_est.saturating_mul(3) + Duration::from_millis(50));
    let mult = 1u32 << backoff.min(4);
    base.saturating_mul(mult).min(Duration::from_secs(8))
}

/// Sender-side ack timeout: paced window delivery plus receiver quiet with
/// slack, and at least 2× the measured RTT so delayed links don't cause
/// blind retransmits. Min 300 ms.
fn ack_timeout(count: u16, rtt_est: Duration) -> Duration {
    let base = (count as u64 * settings().pace_ms + settings().quiet_ms + 100).max(300);
    Duration::from_millis(base).max(rtt_est.saturating_mul(2) + Duration::from_millis(150))
}

/// Ack timeout for retry number `retries` — exponential backoff, capped.
fn retry_interval(count: u16, rtt_est: Duration, retries: u32) -> Duration {
    let base = ack_timeout(count, rtt_est);
    let mult = 1u32 << retries.min(4);
    base.saturating_mul(mult).min(Duration::from_secs(8))
}

/// Parse segment filenames out of an m3u8 playlist.
pub fn parse_manifest(data: &[u8]) -> Vec<String> {
    std::str::from_utf8(data)
        .map(|s| {
            s.lines()
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .map(|l| l.trim().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// Number of packets for a file of `size` bytes (SPEC.md §5.5).
pub fn packet_count(size: usize) -> u16 {
    (size.div_ceil(SEGMENT_PACKET_SIZE).max(1)).min(u16::MAX as usize) as u16
}

/// Reject path traversal and weird names (SPEC.md §7.5).
pub fn valid_filename(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && !name.contains('/')
        && !name.contains('\\')
        && !name.starts_with('.')
        && name.chars().all(|c| c.is_ascii_graphic())
}

// ---------------------------------------------------------------------------
// Sender (serving a file)

/// Packets sent per tick — pacing is deadline-driven, never blocking.
const SEND_CHUNK: u16 = 8;

pub struct SenderTransfer {
    pub transfer_id: u16,
    pub remote: SocketAddr,
    pub total_packets: u16,
    file: Vec<u8>,
    range: (u16, u16), // (start, count) of the current window
    range_sent: u16,   // packets of the current window already sent
    retry_count: u32,
    ack_deadline: Option<Instant>, // armed once the window is fully sent
    send_deadline: Instant,        // when to send the next chunk
    rtt_est: Duration,             // EWMA of measured round-trip (M5)
    window_sent_at: Option<Instant>,
}

impl SenderTransfer {
    pub fn new(transfer_id: u16, remote: SocketAddr, file: Vec<u8>) -> Self {
        let n = packet_count(file.len());
        let count = INITIAL_WINDOW.min(n);
        SenderTransfer {
            transfer_id,
            remote,
            total_packets: n,
            file,
            range: (1, count),
            range_sent: 0,
            retry_count: 0,
            ack_deadline: None,
            send_deadline: Instant::now(),
            rtt_est: Duration::from_millis(250),
            window_sent_at: None,
        }
    }

    pub fn payload_bytes(&self) -> u64 {
        self.file.len() as u64
    }

    pub fn deadline(&self) -> Instant {
        if self.range_sent >= self.range.1 {
            // Window fully sent — next event is the ack timer.
            self.ack_deadline.unwrap_or(self.send_deadline)
        } else {
            self.send_deadline
        }
    }

    /// Advance the state machine; called once per loop tick.
    pub fn tick(
        &mut self,
        socket: &UdpSocket,
        fault: &mut FaultInjector,
        now: Instant,
    ) -> Result<(), String> {
        let (start, count) = self.range;

        // Send the next chunk of the current window (paced, non-blocking).
        if self.range_sent < count && now >= self.send_deadline {
            let to_send = (count - self.range_sent).min(SEND_CHUNK);
            let first = (start as u32 + self.range_sent as u32) as u16;
            self.send_packets(socket, fault, first, to_send);
            self.range_sent += to_send;
            let pace = Duration::from_millis(settings().pace_ms);
            if pace > Duration::ZERO && to_send > 0 {
                self.send_deadline = now + pace.saturating_mul(to_send as u32);
            } else {
                self.send_deadline = now;
            }
            if self.range_sent >= count {
                self.window_sent_at = Some(now);
                self.ack_deadline =
                    Some(now + retry_interval(count, self.rtt_est, self.retry_count));
            }
        }

        // Ack timer: the window was fully sent but no ACK arrived.
        if self.range_sent >= count {
            if let Some(ack) = self.ack_deadline {
                if now >= ack {
                    self.retry_count += 1;
                    if self.retry_count > SENDER_RETRY_LIMIT {
                        return Err(format!(
                            "sender {:#06x}: no ACK for range {:?} after {SENDER_RETRY_LIMIT} retries",
                            self.transfer_id, self.range
                        ));
                    }
                    log::debug(&format!(
                        "sender {:#06x}: ack timeout, resending range {:?} (retry {}/{SENDER_RETRY_LIMIT})",
                        self.transfer_id, self.range, self.retry_count
                    ));
                    self.range_sent = 0;
                    self.ack_deadline = None;
                    self.send_deadline = now;
                }
            }
        }
        Ok(())
    }

    fn send_packets(
        &self,
        socket: &UdpSocket,
        fault: &mut FaultInjector,
        first_packet: u16,
        count: u16,
    ) {
        for i in 0..count {
            let packet_number = (first_packet as u32 + i as u32) as u16;
            let seek = (packet_number as usize - 1) * SEGMENT_PACKET_SIZE;
            let end = (seek + SEGMENT_PACKET_SIZE).min(self.file.len());
            let msg = Message::SegmentContents {
                transfer_id: self.transfer_id,
                packet_number,
                total_packets: self.total_packets,
                data: self.file[seek..end].to_vec(),
            };
            fault.send(socket, protocol::encode(&msg), self.remote, Instant::now());
        }
    }

    /// Handle an ACK. Returns `true` when the transfer is complete.
    pub fn on_ack(&mut self, ack_type: AckType, next_start: u16, next_count: u16) -> bool {
        match ack_type {
            AckType::Complete => true,
            AckType::Progress => {
                let n = self.total_packets as u32;
                let start = next_start as u32;
                let count = next_count as u32;
                if start >= 1 && start <= n {
                    // New range: sample the round-trip from the previous
                    // window's send to this advance (M5, adaptive timers).
                    if let Some(t) = self.window_sent_at.take() {
                        self.rtt_est = (self.rtt_est * 3 + t.elapsed()) / 4;
                    }
                    let count = count.min(n - start + 1);
                    self.range = (start as u16, count as u16);
                    self.range_sent = 0;
                    self.ack_deadline = None;
                    self.retry_count = 0;
                    self.send_deadline = Instant::now();
                } else {
                    // Bogus or already-covered range: nothing to send; wait
                    // for the receiver's COMPLETE.
                    self.range = (1, 0);
                    self.range_sent = 0;
                    self.ack_deadline = Some(Instant::now() + retry_interval(1, self.rtt_est, 1));
                }
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Receiver (downloading a file)

pub struct ReceiverTransfer {
    pub transfer_id: u16,
    pub remote: SocketAddr,
    pub filename: String,
    data_dir: PathBuf,
    packets: HashMap<u16, Vec<u8>>, // packet_number -> payload
    total: Option<u16>,
    range: Option<(u16, u16)>, // current requested range
    retry_count: u32,
    first_response_deadline: Instant,
    quiet_deadline: Instant,
    deadline: Instant,
    outcome: Option<Result<(), String>>,
    unresponsive: bool,
    not_found: bool,
    started_at: Instant,
    gap_est: Duration, // EWMA of inter-packet gaps (M5)
    backoff: u32,      // quiet-period backoff exponent
    last_arrival: Option<Instant>,
    first_packet_latency: Option<u64>,
    request_resent: bool,
    saved_bytes: Option<u64>,
}

impl ReceiverTransfer {
    pub fn new(transfer_id: u16, remote: SocketAddr, filename: String, data_dir: PathBuf) -> Self {
        let now = Instant::now();
        ReceiverTransfer {
            transfer_id,
            remote,
            filename,
            data_dir,
            packets: HashMap::new(),
            total: None,
            range: None,
            retry_count: 0,
            first_response_deadline: now + Duration::from_millis(settings().first_timeout_ms),
            quiet_deadline: now + quiet_period(),
            deadline: now + quiet_period(),
            outcome: None,
            unresponsive: false,
            not_found: false,
            started_at: now,
            gap_est: quiet_period(),
            backoff: 0,
            last_arrival: None,
            first_packet_latency: None,
            request_resent: false,
            saved_bytes: None,
        }
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn outcome(&self) -> Option<Result<(), String>> {
        self.outcome.clone()
    }

    pub fn fail(&mut self, reason: String) {
        log::error(&format!(
            "transfer {:#06x} ({}) failed: {reason}",
            self.transfer_id, self.filename
        ));
        self.outcome = Some(Err(reason));
        self.deadline = Instant::now() + COMPLETE_GRACE;
    }

    /// Fail because the peer never answered — the peer may be dead.
    pub fn fail_unresponsive(&mut self, reason: String) {
        self.unresponsive = true;
        self.fail(reason);
    }

    pub fn unresponsive(&self) -> bool {
        self.unresponsive
    }

    /// Fail because the peer replied it doesn't have the segment.
    pub fn mark_not_found(&mut self) {
        self.not_found = true;
    }

    pub fn not_found(&self) -> bool {
        self.not_found
    }

    /// Milliseconds from request to first content packet.
    pub fn first_packet_latency_ms(&self) -> Option<u64> {
        self.first_packet_latency
    }

    /// Bytes written to disk once complete.
    pub fn saved_bytes(&self) -> Option<u64> {
        self.saved_bytes
    }

    fn recompute_deadline(&mut self) {
        self.deadline = self.first_response_deadline.min(self.quiet_deadline);
    }

    fn send_ack(&self, socket: &UdpSocket, fault: &mut FaultInjector, (start, count): (u16, u16)) {
        let msg = Message::Ack {
            transfer_id: self.transfer_id,
            ack_type: AckType::Progress,
            next_start: start,
            next_count: count,
        };
        fault.send(socket, protocol::encode(&msg), self.remote, Instant::now());
    }

    fn send_complete(&self, socket: &UdpSocket, fault: &mut FaultInjector) {
        let msg = Message::Ack {
            transfer_id: self.transfer_id,
            ack_type: AckType::Complete,
            next_start: 0,
            next_count: 0,
        };
        fault.send(socket, protocol::encode(&msg), self.remote, Instant::now());
    }

    fn range_complete(&self, start: u16, count: u16) -> bool {
        let end = start as u32 + count as u32;
        (start as u32..end).all(|pn| self.packets.contains_key(&(pn as u16)))
    }

    pub fn on_content(
        &mut self,
        socket: &UdpSocket,
        fault: &mut FaultInjector,
        packet_number: u16,
        total_packets: u16,
        data: Vec<u8>,
    ) {
        if self.outcome.is_some() {
            // Stray packet after completion: re-ACK COMPLETE so a lost final
            // ACK converges (SPEC.md §7.3).
            if self.outcome.as_ref().map(|r| r.is_ok()).unwrap_or(false) {
                self.send_complete(socket, fault);
            }
            return;
        }

        if self.total.is_none() {
            self.total = Some(total_packets);
            self.first_packet_latency = Some(self.started_at.elapsed().as_millis() as u64);
            let count = INITIAL_WINDOW.min(total_packets);
            self.range = Some((1, count));
            // Got data — first-response no longer applies.
            self.first_response_deadline = Instant::now() + Duration::from_secs(3600);
            self.quiet_deadline = Instant::now() + adaptive_quiet(self.gap_est, 0);
        }

        let Some(total) = self.total else { return };
        if packet_number == 0 || packet_number > total {
            return;
        }

        let Some((start, count)) = self.range else {
            return;
        };

        if (packet_number as u32) < start as u32
            || (packet_number as u32) >= start as u32 + count as u32
        {
            // Stray/dup (sender resending an old range after our ACK was
            // lost): re-state our current request (nudge).
            log::trace(&format!(
                "receiver {:#06x}: stray packet {packet_number} (current range ({start},{count})), nudging",
                self.transfer_id
            ));
            self.send_ack(socket, fault, (start, count));
            return;
        }

        if self.packets.contains_key(&packet_number) {
            return; // duplicate within range
        }
        if let Some(prev) = self.last_arrival {
            let gap = prev.elapsed();
            self.gap_est = (self.gap_est * 3 + gap) / 4;
        }
        self.last_arrival = Some(Instant::now());
        self.backoff = 0;
        self.packets.insert(packet_number, data);
        self.quiet_deadline = Instant::now() + adaptive_quiet(self.gap_est, self.backoff);

        if (self.packets.len() as u32) >= total as u32 {
            self.complete(socket, fault);
            return;
        }

        if self.range_complete(start, count) {
            self.advance(socket, fault, start, count, total);
        }
    }

    fn advance(
        &mut self,
        socket: &UdpSocket,
        fault: &mut FaultInjector,
        start: u16,
        count: u16,
        total: u16,
    ) {
        let next_start = start as u32 + count as u32;
        let remaining = total as u32 - next_start + 1;
        if remaining <= 0 {
            self.complete(socket, fault);
            return;
        }
        let next_count = (count as u32 * 2).min(MAX_WINDOW as u32).min(remaining);
        self.range = Some((next_start as u16, next_count as u16));
        self.retry_count = 0;
        self.quiet_deadline = Instant::now() + adaptive_quiet(self.gap_est, 0);
        self.send_ack(socket, fault, (next_start as u16, next_count as u16));
        log::trace(&format!(
            "receiver {:#06x}: window {start}+{count} done, requesting ({next_start}, {next_count})",
            self.transfer_id
        ));
    }

    fn complete(&mut self, socket: &UdpSocket, fault: &mut FaultInjector) {
        let total = self.total.unwrap_or(0) as usize;
        let mut buffer = vec![0u8; total.saturating_mul(SEGMENT_PACKET_SIZE)];
        let mut final_size = 0usize;
        for (pn, payload) in &self.packets {
            let pos = (*pn as usize - 1).saturating_mul(SEGMENT_PACKET_SIZE);
            if pos + payload.len() <= buffer.len() {
                buffer[pos..pos + payload.len()].copy_from_slice(payload);
            }
            if *pn as usize == total {
                final_size = pos + payload.len();
            }
        }
        if final_size == 0 || final_size > buffer.len() {
            final_size = buffer.len();
        }

        // Atomic write (tmp + rename) so readers never see a partial file.
        let real = self.data_dir.join(&self.filename);
        let tmp = self.data_dir.join(format!("{}.tmp", self.filename));
        let written = (|| -> io::Result<()> {
            fs::write(&tmp, &buffer[..final_size])?;
            fs::rename(&tmp, &real)?;
            Ok(())
        })();
        if let Err(e) = written {
            self.fail(format!("failed to write {}: {e}", real.display()));
            return;
        }

        let dt = self.started_at.elapsed().as_millis();
        let kbps = if dt > 0 {
            (final_size as u128 * 1000 / dt) / 1024
        } else {
            0
        };
        self.saved_bytes = Some(final_size as u64);
        log::info(&format!(
            "downloaded {} ({} bytes, {} packets, {}ms, {} KB/s)",
            self.filename,
            final_size,
            self.packets.len(),
            dt,
            kbps
        ));

        self.send_complete(socket, fault);
        self.outcome = Some(Ok(()));
        self.deadline = Instant::now() + COMPLETE_GRACE;
    }

    pub fn on_tick(&mut self, socket: &UdpSocket, fault: &mut FaultInjector, now: Instant) {
        if self.outcome.is_some() {
            return;
        }

        // No response at all yet: resend the request at half the first-
        // response timeout so a single dropped request doesn't cost the
        // whole budget (M5). Fresh budget after the resend.
        if self.packets.is_empty() && !self.request_resent {
            let timeout = Duration::from_millis(settings().first_timeout_ms);
            if now >= self.started_at + timeout / 2 {
                self.request_resent = true;
                self.first_response_deadline = now + timeout;
                let req = Message::SegmentRequest {
                    transfer_id: self.transfer_id,
                    filename: self.filename.clone(),
                };
                fault.send(socket, protocol::encode(&req), self.remote, now);
                log::debug(&format!(
                    "receiver {:#06x}: no response yet, resending request",
                    self.transfer_id
                ));
            }
        }

        if now >= self.first_response_deadline && self.packets.is_empty() {
            self.fail_unresponsive(format!(
                "no response from {} for {}",
                self.remote, self.filename
            ));
            return;
        }

        let mut acted = false;
        if now >= self.quiet_deadline {
            // Back off the quiet period so a burst spreads its retries.
            self.backoff = (self.backoff + 1).min(4);
            self.quiet_deadline = now + adaptive_quiet(self.gap_est, self.backoff);
            acted = true;
            if let Some((start, count)) = self.range {
                if !self.range_complete(start, count) {
                    self.retry_count += 1;
                    if self.retry_count > RETRY_LIMIT {
                        self.fail(format!(
                            "range ({start},{count}) of {} incomplete after {RETRY_LIMIT} retries",
                            self.filename
                        ));
                        return;
                    }
                    log::debug(&format!(
                        "receiver {:#06x}: re-requesting range ({start},{count}) (retry {}/{RETRY_LIMIT})",
                        self.transfer_id, self.retry_count
                    ));
                    self.send_ack(socket, fault, (start, count));
                }
            }
        }
        if acted {
            self.recompute_deadline();
        }
    }
}

// ---------------------------------------------------------------------------
// Registry

pub struct TransferRegistry {
    segment_root: PathBuf,
    senders: HashMap<u16, SenderTransfer>,
    receivers: HashMap<u16, ReceiverTransfer>,
    events: Vec<RegEvent>,
}

/// Outcomes the node layer turns into peer stats (M5).
pub enum RegEvent {
    Served { src: SocketAddr, bytes: u64 },
    NotFound { src: SocketAddr },
    SenderFailed { src: SocketAddr },
}

fn random_id() -> u16 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    ((nanos & 0xFFFF) as u16) ^ (((nanos >> 16) & 0xFFFF) as u16)
}

impl TransferRegistry {
    pub fn new(segment_root: PathBuf) -> Self {
        TransferRegistry {
            segment_root,
            senders: HashMap::new(),
            receivers: HashMap::new(),
            events: Vec::new(),
        }
    }

    pub fn segment_root(&self) -> &Path {
        &self.segment_root
    }

    pub fn has_segment(&self, filename: &str) -> bool {
        self.segment_root.join(filename).is_file()
    }

    /// Take recorded serve outcomes (drained by the node layer).
    pub fn drain_events(&mut self) -> Vec<RegEvent> {
        std::mem::take(&mut self.events)
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.senders
            .values()
            .map(SenderTransfer::deadline)
            .chain(self.receivers.values().map(ReceiverTransfer::deadline))
            .min()
    }

    pub fn active_count(&self) -> usize {
        self.senders.len() + self.receivers.len()
    }

    fn fresh_id(&self) -> u16 {
        loop {
            let id = random_id();
            if id != 0 && !self.senders.contains_key(&id) && !self.receivers.contains_key(&id) {
                return id;
            }
        }
    }

    fn send_not_found(
        &self,
        socket: &UdpSocket,
        fault: &mut FaultInjector,
        transfer_id: u16,
        src: SocketAddr,
    ) {
        let msg = Message::SegmentNotFound {
            transfer_id,
            availability: self.segment_availability(),
        };
        fault.send(socket, protocol::encode(&msg), src, Instant::now());
    }

    /// Serve a SEGMENT_REQUEST from `src`.
    pub fn serve(
        &mut self,
        socket: &UdpSocket,
        fault: &mut FaultInjector,
        transfer_id: u16,
        filename: &str,
        src: SocketAddr,
    ) {
        if !valid_filename(filename) {
            log::warn(&format!(
                "rejecting invalid filename {filename:?} from {src}"
            ));
            self.events.push(RegEvent::NotFound { src });
            self.send_not_found(socket, fault, transfer_id, src);
            return;
        }
        if self.active_count() >= MAX_CONCURRENT_TRANSFERS {
            // Never silently drop: a dropped request looks like a dead peer
            // to the requester and can cause false evictions.
            log::warn("transfer registry full — answering NOT_FOUND");
            self.events.push(RegEvent::NotFound { src });
            self.send_not_found(socket, fault, transfer_id, src);
            return;
        }

        let filepath = self.segment_root.join(filename);
        match fs::read(&filepath) {
            Ok(file) => {
                let sender = SenderTransfer::new(transfer_id, src, file);
                log::info(&format!(
                    "serving {filename} to {src} (transfer {transfer_id:#06x}, {} packets)",
                    sender.total_packets
                ));
                self.senders.insert(transfer_id, sender);
            }
            Err(_) => {
                log::debug(&format!(
                    "{filename} not in {} — SEGMENT_NOT_FOUND",
                    self.segment_root.display()
                ));
                self.events.push(RegEvent::NotFound { src });
                self.send_not_found(socket, fault, transfer_id, src);
            }
        }
    }

    /// Reject a request while preserving the normal NOT_FOUND wire response
    /// and requester-side availability update.
    pub fn reject_not_found(
        &mut self,
        socket: &UdpSocket,
        fault: &mut FaultInjector,
        transfer_id: u16,
        src: SocketAddr,
    ) {
        self.events.push(RegEvent::NotFound { src });
        self.send_not_found(socket, fault, transfer_id, src);
    }

    /// Start a download from `remote`; returns the transfer id.
    pub fn start_receiver(
        &mut self,
        socket: &UdpSocket,
        fault: &mut FaultInjector,
        remote: SocketAddr,
        data_dir: &Path,
        filename: &str,
    ) -> Option<u16> {
        if self.active_count() >= MAX_CONCURRENT_TRANSFERS {
            log::warn("transfer registry full — cannot start download");
            return None;
        }
        let id = self.fresh_id();
        let req = Message::SegmentRequest {
            transfer_id: id,
            filename: filename.to_string(),
        };
        fault.send(socket, protocol::encode(&req), remote, Instant::now());
        let receiver =
            ReceiverTransfer::new(id, remote, filename.to_string(), data_dir.to_path_buf());
        self.receivers.insert(id, receiver);
        Some(id)
    }

    pub fn on_content(
        &mut self,
        socket: &UdpSocket,
        fault: &mut FaultInjector,
        transfer_id: u16,
        packet_number: u16,
        total_packets: u16,
        data: Vec<u8>,
        src: SocketAddr,
    ) {
        if let Some(r) = self.receivers.get_mut(&transfer_id) {
            r.on_content(socket, fault, packet_number, total_packets, data);
        } else {
            log::trace(&format!(
                "SEGMENT_CONTENTS for unknown transfer {transfer_id:#06x} from {src}"
            ));
        }
    }

    pub fn on_ack(
        &mut self,
        transfer_id: u16,
        ack_type: AckType,
        next_start: u16,
        next_count: u16,
        src: SocketAddr,
    ) {
        if let Some(sender) = self.senders.get_mut(&transfer_id) {
            if sender.on_ack(ack_type, next_start, next_count) {
                let bytes = sender.payload_bytes();
                let remote = sender.remote;
                log::info(&format!(
                    "transfer {transfer_id:#06x} complete — sender freed"
                ));
                self.events.push(RegEvent::Served {
                    src: remote,
                    bytes,
                });
                self.senders.remove(&transfer_id);
            }
        } else {
            log::trace(&format!(
                "ACK for unknown transfer {transfer_id:#06x} from {src}"
            ));
        }
    }

    /// Summarize locally stored recent segments in a compact 16-bit mask.
    pub fn segment_availability(&self) -> Option<SegmentAvailability> {
        let entries = fs::read_dir(&self.segment_root).ok()?;
        let numbers: Vec<u64> = entries
            .flatten()
            .filter_map(|entry| segment_number(&entry.file_name().to_string_lossy()))
            .collect();
        let newest = *numbers.iter().max()?;
        let mut mask = 0u16;
        for number in numbers {
            let distance = newest.saturating_sub(number);
            if distance < AVAILABILITY_MASK_BITS as u64 {
                mask |= 1 << distance;
            }
        }
        Some(SegmentAvailability { newest, mask })
    }

    pub fn on_not_found(&mut self, _socket: &UdpSocket, transfer_id: u16) -> Option<String> {
        if let Some(r) = self.receivers.get_mut(&transfer_id) {
            let filename = r.filename.clone();
            r.mark_not_found();
            r.fail(format!("peer does not have {filename}"));
            Some(filename)
        } else {
            log::trace(&format!(
                "NOT_FOUND for unknown transfer {transfer_id:#06x}"
            ));
            None
        }
    }

    pub fn receiver_outcome(&self, id: u16) -> Option<Result<(), String>> {
        self.receivers.get(&id).and_then(ReceiverTransfer::outcome)
    }

    /// Whether the receiver failed because its peer never answered.
    pub fn receiver_unresponsive(&self, id: u16) -> bool {
        self.receivers
            .get(&id)
            .map(ReceiverTransfer::unresponsive)
            .unwrap_or(false)
    }

    /// Whether the receiver failed because the peer lacks the segment.
    pub fn receiver_not_found(&self, id: u16) -> bool {
        self.receivers
            .get(&id)
            .map(ReceiverTransfer::not_found)
            .unwrap_or(false)
    }

    /// Milliseconds from request to first content packet, if it got that far.
    pub fn receiver_first_packet_ms(&self, id: u16) -> Option<u64> {
        self.receivers
            .get(&id)
            .and_then(ReceiverTransfer::first_packet_latency_ms)
    }

    /// Bytes written to disk for a completed receiver.
    pub fn receiver_saved_bytes(&self, id: u16) -> Option<u64> {
        self.receivers
            .get(&id)
            .and_then(ReceiverTransfer::saved_bytes)
    }

    pub fn remove_receiver(&mut self, id: u16) {
        self.receivers.remove(&id);
    }

    /// Advance all transfers whose timers have expired.
    pub fn tick(&mut self, socket: &UdpSocket, fault: &mut FaultInjector, now: Instant) {
        // Senders: chunk sends + ack timeouts (paced, non-blocking).
        let mut expired: Vec<u16> = Vec::new();
        for (id, sender) in self.senders.iter_mut() {
            match sender.tick(socket, fault, now) {
                Ok(()) => {}
                Err(e) => {
                    log::warn(&e);
                    let src = sender.remote;
                    self.events.push(RegEvent::SenderFailed { src });
                    expired.push(*id);
                }
            }
        }
        for id in expired {
            self.senders.remove(&id);
        }

        // Receivers: quiet / first-response / grace expiry.
        let mut done: Vec<u16> = Vec::new();
        for (id, receiver) in self.receivers.iter_mut() {
            if receiver.outcome().is_some() {
                if now >= receiver.deadline() {
                    done.push(*id);
                }
            } else {
                receiver.on_tick(socket, fault, now);
            }
        }
        for id in done {
            self.receivers.remove(&id);
        }
    }
}

fn segment_number(name: &str) -> Option<u64> {
    name.strip_prefix("seg_")?.strip_suffix(".ts")?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_count_bounds() {
        assert_eq!(packet_count(0), 1);
        assert_eq!(packet_count(1), 1);
        assert_eq!(packet_count(SEGMENT_PACKET_SIZE), 1);
        assert_eq!(packet_count(SEGMENT_PACKET_SIZE + 1), 2);
        assert_eq!(packet_count(SEGMENT_PACKET_SIZE * 60), 60);
    }

    #[test]
    fn filename_validation() {
        assert!(valid_filename("seg_0042.ts"));
        assert!(valid_filename("a.b-c_d"));
        assert!(!valid_filename(""));
        assert!(!valid_filename("../secret"));
        assert!(!valid_filename("dir/seg.ts"));
        assert!(!valid_filename("dir\\seg.ts"));
        assert!(!valid_filename(".hidden"));
        assert!(!valid_filename("has space.ts"));
    }
}
