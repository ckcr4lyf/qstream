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

use crate::log;
use crate::protocol::{self, AckType, Message};

pub const SEGMENT_PACKET_SIZE: usize = 1400;
pub const INITIAL_WINDOW: u16 = 5;
pub const MAX_WINDOW: u16 = 64;
pub const FIRST_RESPONSE_TIMEOUT: Duration = Duration::from_millis(2000);
pub const RETRY_LIMIT: u32 = 8;
pub const COMPLETE_GRACE: Duration = Duration::from_millis(2000);
pub const MAX_CONCURRENT_TRANSFERS: usize = 16;

/// Tunables overridable via env (SPEC.md §7.4).
#[derive(Clone, Copy)]
pub struct Settings {
    pub pace_ms: u64,
    pub quiet_ms: u64,
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
        }
    })
}

fn quiet_period() -> Duration {
    Duration::from_millis(settings().quiet_ms)
}

/// Sender-side ack timeout: enough time for the paced window to arrive plus
/// the receiver's quiet period, with slack. Min 300 ms.
fn ack_timeout(count: u16) -> Duration {
    let ms = (count as u64 * settings().pace_ms + settings().quiet_ms + 100).max(300);
    Duration::from_millis(ms)
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

pub struct SenderTransfer {
    pub transfer_id: u16,
    pub remote: SocketAddr,
    pub total_packets: u16,
    file: Vec<u8>,
    range: (u16, u16), // (start, count) currently being sent
    retry_count: u32,
    deadline: Instant, // ack timer
}

impl SenderTransfer {
    pub fn new(transfer_id: u16, remote: SocketAddr, file: Vec<u8>) -> Self {
        let n = packet_count(file.len());
        SenderTransfer {
            transfer_id,
            remote,
            total_packets: n,
            file,
            range: (1, INITIAL_WINDOW.min(n)),
            retry_count: 0,
            deadline: Instant::now() + ack_timeout(INITIAL_WINDOW.min(n)),
        }
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn send_range(&mut self, socket: &UdpSocket) {
        let (start, count) = self.range;
        if count > 0 {
            self.send_packets(socket, start, count);
        }
        self.deadline = Instant::now() + ack_timeout(count.max(1));
    }

    fn send_packets(&self, socket: &UdpSocket, start: u16, count: u16) {
        let pace = Duration::from_millis(settings().pace_ms);
        for i in 0..count {
            let packet_number = (start as u32 + i as u32) as u16;
            let seek = (packet_number as usize - 1) * SEGMENT_PACKET_SIZE;
            let end = (seek + SEGMENT_PACKET_SIZE).min(self.file.len());
            let msg = Message::SegmentContents {
                transfer_id: self.transfer_id,
                packet_number,
                total_packets: self.total_packets,
                data: self.file[seek..end].to_vec(),
            };
            if let Err(e) = socket.send_to(&protocol::encode(&msg), self.remote) {
                log::error(&format!(
                    "sender {:#06x}: send failed: {e}",
                    self.transfer_id
                ));
            }
            if pace > Duration::ZERO && i + 1 < count {
                std::thread::sleep(pace);
            }
        }
    }

    /// Handle an ACK. Returns `true` when the transfer is complete.
    pub fn on_ack(
        &mut self,
        socket: &UdpSocket,
        ack_type: AckType,
        next_start: u16,
        next_count: u16,
    ) -> bool {
        match ack_type {
            AckType::Complete => true,
            AckType::Progress => {
                let n = self.total_packets as u32;
                let start = next_start as u32;
                let count = next_count as u32;
                if start >= 1 && start <= n {
                    let count = count.min(n - start + 1);
                    self.range = (start as u16, count as u16);
                    self.retry_count = 0;
                    self.send_range(socket);
                } else {
                    // Bogus or already-covered range: nothing to send; wait
                    // for the receiver's COMPLETE.
                    self.deadline = Instant::now() + ack_timeout(1);
                }
                false
            }
        }
    }

    /// Ack timer fired: retransmit the current range, or fail.
    pub fn on_timeout(&mut self, socket: &UdpSocket) -> Result<(), String> {
        self.retry_count += 1;
        if self.retry_count > RETRY_LIMIT {
            return Err(format!(
                "sender {:#06x}: no ACK for range {:?} after {RETRY_LIMIT} retries",
                self.transfer_id, self.range
            ));
        }
        log::debug(&format!(
            "sender {:#06x}: ack timeout, resending range {:?} (retry {}/{RETRY_LIMIT})",
            self.transfer_id, self.range, self.retry_count
        ));
        self.send_range(socket);
        Ok(())
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
    started_at: Instant,
}

impl ReceiverTransfer {
    pub fn new(
        transfer_id: u16,
        remote: SocketAddr,
        filename: String,
        data_dir: PathBuf,
    ) -> Self {
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
            first_response_deadline: now + FIRST_RESPONSE_TIMEOUT,
            quiet_deadline: now + quiet_period(),
            deadline: now + quiet_period(),
            outcome: None,
            started_at: now,
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

    fn recompute_deadline(&mut self) {
        self.deadline = self.first_response_deadline.min(self.quiet_deadline);
    }

    fn send_ack(&self, socket: &UdpSocket, (start, count): (u16, u16)) {
        let msg = Message::Ack {
            transfer_id: self.transfer_id,
            ack_type: AckType::Progress,
            next_start: start,
            next_count: count,
        };
        if let Err(e) = socket.send_to(&protocol::encode(&msg), self.remote) {
            log::error(&format!("receiver {:#06x}: ack send failed: {e}", self.transfer_id));
        }
    }

    fn send_complete(&self, socket: &UdpSocket) {
        let msg = Message::Ack {
            transfer_id: self.transfer_id,
            ack_type: AckType::Complete,
            next_start: 0,
            next_count: 0,
        };
        let _ = socket.send_to(&protocol::encode(&msg), self.remote);
    }

    fn range_complete(&self, start: u16, count: u16) -> bool {
        let end = start as u32 + count as u32;
        (start as u32..end).all(|pn| self.packets.contains_key(&(pn as u16)))
    }

    pub fn on_content(
        &mut self,
        socket: &UdpSocket,
        packet_number: u16,
        total_packets: u16,
        data: Vec<u8>,
    ) {
        if self.outcome.is_some() {
            // Stray packet after completion: re-ACK COMPLETE so a lost final
            // ACK converges (SPEC.md §7.3).
            if self.outcome.as_ref().map(|r| r.is_ok()).unwrap_or(false) {
                self.send_complete(socket);
            }
            return;
        }

        if self.total.is_none() {
            self.total = Some(total_packets);
            let count = INITIAL_WINDOW.min(total_packets);
            self.range = Some((1, count));
            // Got data — first-response no longer applies.
            self.first_response_deadline = Instant::now() + Duration::from_secs(3600);
            self.quiet_deadline = Instant::now() + quiet_period();
        }

        let Some(total) = self.total else { return };
        if packet_number == 0 || packet_number > total {
            return;
        }

        let Some((start, count)) = self.range else { return };

        if (packet_number as u32) < start as u32
            || (packet_number as u32) >= start as u32 + count as u32
        {
            // Stray/dup (sender resending an old range after our ACK was
            // lost): re-state our current request (nudge).
            log::trace(&format!(
                "receiver {:#06x}: stray packet {packet_number} (current range ({start},{count})), nudging",
                self.transfer_id
            ));
            self.send_ack(socket, (start, count));
            return;
        }

        if self.packets.contains_key(&packet_number) {
            return; // duplicate within range
        }
        self.packets.insert(packet_number, data);
        self.quiet_deadline = Instant::now() + quiet_period();

        if (self.packets.len() as u32) >= total as u32 {
            self.complete(socket);
            return;
        }

        if self.range_complete(start, count) {
            self.advance(socket, start, count, total);
        }
    }

    fn advance(&mut self, socket: &UdpSocket, start: u16, count: u16, total: u16) {
        let next_start = start as u32 + count as u32;
        let remaining = total as u32 - next_start + 1;
        if remaining <= 0 {
            self.complete(socket);
            return;
        }
        let next_count = (count as u32 * 2)
            .min(MAX_WINDOW as u32)
            .min(remaining);
        self.range = Some((next_start as u16, next_count as u16));
        self.retry_count = 0;
        self.quiet_deadline = Instant::now() + quiet_period();
        self.send_ack(socket, (next_start as u16, next_count as u16));
        log::trace(&format!(
            "receiver {:#06x}: window {start}+{count} done, requesting ({next_start}, {next_count})",
            self.transfer_id
        ));
    }

    fn complete(&mut self, socket: &UdpSocket) {
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
        log::info(&format!(
            "downloaded {} ({} bytes, {} packets, {}ms, {} KB/s)",
            self.filename,
            final_size,
            self.packets.len(),
            dt,
            kbps
        ));

        self.send_complete(socket);
        self.outcome = Some(Ok(()));
        self.deadline = Instant::now() + COMPLETE_GRACE;
    }

    pub fn on_tick(&mut self, socket: &UdpSocket, now: Instant) {
        if self.outcome.is_some() {
            return;
        }

        if now >= self.first_response_deadline && self.packets.is_empty() {
            self.fail(format!("no response from {} for {}", self.remote, self.filename));
            return;
        }

        let mut acted = false;
        if now >= self.quiet_deadline {
            self.quiet_deadline = now + quiet_period();
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
                    self.send_ack(socket, (start, count));
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
        }
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

    fn send_not_found(&self, socket: &UdpSocket, transfer_id: u16, src: SocketAddr) {
        let msg = Message::SegmentNotFound { transfer_id };
        let _ = socket.send_to(&protocol::encode(&msg), src);
    }

    /// Serve a SEGMENT_REQUEST from `src`.
    pub fn serve(
        &mut self,
        socket: &UdpSocket,
        transfer_id: u16,
        filename: &str,
        src: SocketAddr,
    ) {
        if !valid_filename(filename) {
            log::warn(&format!("rejecting invalid filename {filename:?} from {src}"));
            self.send_not_found(socket, transfer_id, src);
            return;
        }
        if self.active_count() >= MAX_CONCURRENT_TRANSFERS {
            log::warn("transfer registry full — dropping segment request");
            return;
        }

        let filepath = self.segment_root.join(filename);
        match fs::read(&filepath) {
            Ok(file) => {
                let mut sender = SenderTransfer::new(transfer_id, src, file);
                sender.send_range(socket);
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
                self.send_not_found(socket, transfer_id, src);
            }
        }
    }

    /// Start a download from `remote`; returns the transfer id.
    pub fn start_receiver(
        &mut self,
        socket: &UdpSocket,
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
        if let Err(e) = socket.send_to(&protocol::encode(&req), remote) {
            log::error(&format!("failed to send SEGMENT_REQUEST: {e}"));
            return None;
        }
        let receiver = ReceiverTransfer::new(id, remote, filename.to_string(), data_dir.to_path_buf());
        self.receivers.insert(id, receiver);
        Some(id)
    }

    pub fn on_content(
        &mut self,
        socket: &UdpSocket,
        transfer_id: u16,
        packet_number: u16,
        total_packets: u16,
        data: Vec<u8>,
        src: SocketAddr,
    ) {
        if let Some(r) = self.receivers.get_mut(&transfer_id) {
            r.on_content(socket, packet_number, total_packets, data);
        } else {
            log::trace(&format!(
                "SEGMENT_CONTENTS for unknown transfer {transfer_id:#06x} from {src}"
            ));
        }
    }

    pub fn on_ack(
        &mut self,
        socket: &UdpSocket,
        transfer_id: u16,
        ack_type: AckType,
        next_start: u16,
        next_count: u16,
        src: SocketAddr,
    ) {
        if let Some(sender) = self.senders.get_mut(&transfer_id) {
            if sender.on_ack(socket, ack_type, next_start, next_count) {
                log::info(&format!("transfer {transfer_id:#06x} complete — sender freed"));
                self.senders.remove(&transfer_id);
            }
        } else {
            log::trace(&format!(
                "ACK for unknown transfer {transfer_id:#06x} from {src}"
            ));
        }
    }

    pub fn on_not_found(&mut self, _socket: &UdpSocket, transfer_id: u16) {
        if let Some(r) = self.receivers.get_mut(&transfer_id) {
            let filename = r.filename.clone();
            r.fail(format!("peer does not have {filename}"));
        } else {
            log::trace(&format!("NOT_FOUND for unknown transfer {transfer_id:#06x}"));
        }
    }

    pub fn receiver_outcome(&self, id: u16) -> Option<Result<(), String>> {
        self.receivers.get(&id).and_then(ReceiverTransfer::outcome)
    }

    pub fn receiver_filename(&self, id: u16) -> String {
        self.receivers
            .get(&id)
            .map(|r| r.filename.clone())
            .unwrap_or_default()
    }

    pub fn remove_receiver(&mut self, id: u16) {
        self.receivers.remove(&id);
    }

    /// Advance all transfers whose timers have expired.
    pub fn tick(&mut self, socket: &UdpSocket, now: Instant) {
        // Senders: ack timeouts.
        let mut expired: Vec<u16> = Vec::new();
        for (id, sender) in self.senders.iter_mut() {
            if now >= sender.deadline() {
                match sender.on_timeout(socket) {
                    Ok(()) => {}
                    Err(e) => {
                        log::warn(&e);
                        expired.push(*id);
                    }
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
                receiver.on_tick(socket, now);
            }
        }
        for id in done {
            self.receivers.remove(&id);
        }
    }
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
