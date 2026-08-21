//! Deterministic fault injection for the outgoing datagram path (M5).
//!
//! Env (parsed once at startup):
//!   QSTREAM_FAULT_DROP_PCT     0-100  % of outgoing datagrams dropped
//!   QSTREAM_FAULT_DUP_PCT      0-100  % sent twice
//!   QSTREAM_FAULT_DELAY_MS            fixed one-way latency added to outgoing
//!   QSTREAM_FAULT_REORDER_PCT  0-100  % of sends swapped with the next one
//!   QSTREAM_FAULT_BURST_EVERY_MS      period of full-drop bursts (0 = off)
//!   QSTREAM_FAULT_BURST_DUR_MS        duration of each drop burst
//!   QSTREAM_FAULT_SEED                RNG seed (0 = time-based)
//!
//! Rolls are independent per datagram (Bernoulli), so effective loss is
//! binomially distributed like a real link. Applied to ALL outgoing
//! datagrams (control + data), as a bad link would.

use std::collections::VecDeque;
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

/// Tiny deterministic PRNG (SplitMix64) — std has no RNG.
pub struct Rng(u64);

impl Rng {
    pub fn new(mut seed: u64) -> Rng {
        if seed == 0 {
            seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(1);
        }
        Rng(seed)
    }

    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// True with probability `pct`/100.
    pub fn roll(&mut self, pct: u32) -> bool {
        if pct >= 100 {
            return true;
        }
        if pct == 0 {
            return false;
        }
        self.next() % 100 < pct as u64
    }
}

#[derive(Default, Clone, Copy)]
pub struct FaultStats {
    pub dropped: u64,
    pub emitted: u64,
}

pub struct FaultInjector {
    rng: Rng,
    started: Instant,
    drop_pct: u32,
    dup_pct: u32,
    reorder_pct: u32,
    delay_ms: u64,
    burst_every_ms: u64,
    burst_dur_ms: u64,
    /// Datagram held back for reordering (sent after the next one).
    held: Option<(Instant, Vec<u8>, SocketAddr)>,
    /// Datagrams waiting out their artificial latency.
    delayed: VecDeque<(Instant, Vec<u8>, SocketAddr)>,
    pub stats: FaultStats,
}

const HOLD_MAX: Duration = Duration::from_millis(50);

impl FaultInjector {
    pub fn from_env() -> Self {
        let env = |name: &str, default: u64| -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        let seed = env("QSTREAM_FAULT_SEED", 0);
        FaultInjector {
            rng: Rng::new(seed),
            started: Instant::now(),
            drop_pct: env("QSTREAM_FAULT_DROP_PCT", 0) as u32,
            dup_pct: env("QSTREAM_FAULT_DUP_PCT", 0) as u32,
            reorder_pct: env("QSTREAM_FAULT_REORDER_PCT", 0) as u32,
            delay_ms: env("QSTREAM_FAULT_DELAY_MS", 0),
            burst_every_ms: env("QSTREAM_FAULT_BURST_EVERY_MS", 0),
            burst_dur_ms: env("QSTREAM_FAULT_BURST_DUR_MS", 0),
            held: None,
            delayed: VecDeque::new(),
            stats: FaultStats::default(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.drop_pct > 0
            || self.dup_pct > 0
            || self.reorder_pct > 0
            || self.delay_ms > 0
            || self.burst_every_ms > 0
    }

    pub fn summary(&self) -> String {
        format!(
            "fault injection: drop {}% dup {}% delay {}ms reorder {}% burst {}/{}ms seed rng",
            self.drop_pct,
            self.dup_pct,
            self.delay_ms,
            self.reorder_pct,
            self.burst_dur_ms,
            self.burst_every_ms
        )
    }

    fn in_burst(&self, now: Instant) -> bool {
        if self.burst_every_ms == 0 {
            return false;
        }
        let el = now.duration_since(self.started).as_millis() as u64;
        el % self.burst_every_ms < self.burst_dur_ms
    }

    fn pipeline(&mut self, socket: &UdpSocket, bytes: Vec<u8>, dst: SocketAddr, now: Instant) {
        let copies = if self.dup_pct > 0 && self.rng.roll(self.dup_pct) {
            2
        } else {
            1
        };
        for _ in 0..copies {
            if self.drop_pct > 0 && self.rng.roll(self.drop_pct) {
                self.stats.dropped += 1;
                continue;
            }
            if self.delay_ms > 0 {
                self.delayed.push_back((
                    now + Duration::from_millis(self.delay_ms),
                    bytes.clone(),
                    dst,
                ));
            } else {
                if socket.send_to(&bytes, dst).is_ok() {
                    self.stats.emitted += 1;
                }
            }
        }
    }

    /// Inject faults into one outgoing datagram, then send (or schedule) it.
    pub fn send(&mut self, socket: &UdpSocket, bytes: Vec<u8>, dst: SocketAddr, now: Instant) {
        if self.in_burst(now) {
            self.stats.dropped += 1;
            return;
        }
        if self.reorder_pct > 0 {
            if let Some((_, held_bytes, held_dst)) = self.held.take() {
                // A datagram was held back: release the NEW one first, then
                // the held one — the receiver sees them swapped.
                self.pipeline(socket, bytes, dst, now);
                self.pipeline(socket, held_bytes, held_dst, now);
                return;
            }
            if self.rng.roll(self.reorder_pct) {
                self.held = Some((now, bytes, dst));
                return;
            }
        }
        self.pipeline(socket, bytes, dst, now);
    }

    /// Release delayed/held datagrams whose time has come. Called per tick.
    pub fn drain(&mut self, socket: &UdpSocket, now: Instant) {
        while let Some(front) = self.delayed.front() {
            if front.0 > now {
                break;
            }
            let (_, bytes, dst) = self.delayed.pop_front().unwrap();
            if socket.send_to(&bytes, dst).is_ok() {
                self.stats.emitted += 1;
            }
        }
        if let Some((held_at, bytes, dst)) = self.held.take() {
            if now.duration_since(held_at) >= HOLD_MAX {
                if socket.send_to(&bytes, dst).is_ok() {
                    self.stats.emitted += 1;
                }
            } else {
                self.held = Some((held_at, bytes, dst));
            }
        }
    }

    /// Earliest deadline the event loop must wake up for (delay queue, held).
    pub fn next_deadline(&self, now: Instant) -> Option<Instant> {
        let d = self
            .delayed
            .front()
            .map(|(t, _, _)| *t)
            .or_else(|| self.held.as_ref().map(|(t, _, _)| *t + HOLD_MAX));
        d.map(|t| {
            if t > now {
                t
            } else {
                now + Duration::from_nanos(1)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rng_deterministic() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next(), b.next());
        }
    }

    #[test]
    fn roll_bounds() {
        let mut r = Rng::new(7);
        for _ in 0..1000 {
            let v = r.next() % 100;
            assert!(v < 100);
        }
        assert!(!r.roll(0));
        assert!(r.roll(100));
    }

    #[test]
    fn burst_window() {
        let mut f = FaultInjector::from_env();
        f.burst_every_ms = 1000;
        f.burst_dur_ms = 100;
        let now = Instant::now();
        let start = now - Duration::from_millis(50); // started 50ms ago
        f.started = start;
        // 50ms into the cycle -> in burst (first 100ms of 1000ms cycle)
        let mid = now + Duration::from_millis(100); // 150ms in -> out of burst
        assert!(f.in_burst(now));
        assert!(!f.in_burst(mid));
    }
}
