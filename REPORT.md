# qstream — Resiliency Report (M5)

Date: 2025-08-14 · Binary: `target/release/qstream` (commit `M5`, +2 follow-up fixes)
Topology: 1 master (UDP 3333, HTTP 18080) + 5 peers (UDP 4444-4448; HTTP
3333/18081-18084), all loopback, ffmpeg HLS source (2 s segments, ~1400-byte
UDP packets). Data/logs per scenario: `/tmp/lab/<scenario>/`.
Harness: `scripts/run_all.sh`, `scripts/run_scenario.sh <name> [secs]`,
`scripts/lab.sh {start <scenario>|stop|status|peers|attach}` (tmux),
`scripts/metrics.py <dir>`. Faults per node via `QSTREAM_FAULT_*` env vars
(drop/dup/delay/reorder/burst, seeded RNG) applied to ALL outgoing datagrams.

Legend: `saved` = segments written · `pulls` = attempts · `nf/to/inc/other` =
failure kinds (NOT_FOUND / no-response / incomplete / other) · `evicts` =
unresponsive evictions · `KB/s`, `xfer_ms` = per-transfer medians · `lag_ms` =
creation→saved replication delay (median/p90/max) · `end-cov` = master's final
playlist present locally · `ok-by-src` = successful deliveries by source
(3333 = master) — the sharing measure.

---

## Scenario matrix (all: 0 integrity mismatches)

### S0 baseline (no faults) — 120 s
All peers: 69-70 saved (100%), 0 failures, 0 evictions, ~1.09 MB/s,
lag 2.1-2.7 s med / 3.2-4.2 s p90, end-cov 9-10/10.
`ok-by-src`: master ~70%, peers ~30%. Retention (60 s) keeps dirs at ~30 files.

### S1 loss 5% on master — 150 s
83-84 saved (100%), 0 timeouts/incomplete, 0 evictions. **Cost: ~5× slower**
(223-226 KB/s vs 1090; xfer 1.3 s), lag 3.3-4.2 s. Whole-window retransmission
is the culprit (see Ideas).

### S2 loss 10% on all 6 nodes — 150 s (hardest)
77-82 saved (**~93%**), 3-8 timeouts/peer, 0 evictions, 127-135 KB/s,
lag 4.5-7.7 s med (p90 to 11.7 s), end-cov 6-8/10. Survives but exceeds the
3-4 s delay budget — the case for selective retransmission + FEC.
`registry-full` answers: **0** (a follow-up fix eliminated 213 pre-fix).

### S3 loss 20% on peer-2 only — 120 s
Swarm unaffected: others 68-70 saved, 0-2 timeouts, 1.1 MB/s, lag 2.3-3.5 s.
Sick peer itself: 68 saved but 432 KB/s and 3 timeouts. Ranking identifies it:
**peer-2 score 44 vs healthy peers 72-92** (master 100).

### S4 burst: master drops 100% for 1 s every 6 s (~17% bursty loss) — 150 s
80-84 saved, **0 failures, 0 evictions, full 1.1 MB/s**, lag 2.8-3.1 s,
end-cov 9-10/10. Exponential backoff + adaptive quiet absorb bursts almost
for free (better than uniform loss).

### S5 +100 ms one-way delay on all nodes — 150 s
82-83 saved (**98%**), **0 timeouts/incomplete** — adaptive timers hold.
But throughput collapses to 139-145 KB/s: windowed flow control is RTT-bound
(one window in flight); pipelining is the fix. lag 5.0-5.2 s.

### S6 +300 ms delay + 5% loss on peer-1 only — 120 s
Sick peer: 67 saved (95%), 110 KB/s, lag 5.6 s. Healthy peers: 69 saved,
1.11 MB/s, 0 timeouts — they route around peer-1. Caveat: latency-only
sickness doesn't lower the score (no timeouts occur), so peer-1 is still
pulled occasionally; a latency term in the score would help.

### S7 10% duplicates + 10% reordering on master — 150 s
84 saved (100%), 0 failures, **full 1.09 MB/s**, lag 2.4-3.0 s. Duplicates
and reordering are free (dedup + order-free reassembly).

### S8 SIGKILL peer-3 at 60 s (of 180 s)
Survivors: 99 saved (100%), 3-4 timeouts (jobs against the corpse), then
**peer-3 evicted** (1 eviction per peer after 3 unresponsive hits), full
speed throughout, end-cov 9/10. Peer-3 froze at 39 (killed).

### S9 SIGKILL master at 60 s (of 150 s)
Peers freeze at ~40 saved: manifests and segments only exist on the master,
so **no new content anywhere** (documented limitation). 1-2 stuck jobs/peer,
master *not* evicted (below the 3-hit threshold). HTTP keeps serving cached
segments — a viewer sees the tail freeze. Master failover is the fix (Ideas).

---

## How loss is dealt with (mechanisms)

- **Receiver-driven windows** (SPEC §7): receiver names the exact next
  `(start,count)` in every ACK; sender executes. No window-state desync.
- **Quiet-period re-request**: after 150 ms (adaptive, §7.7) without new
  packets the receiver re-requests the range; retries with exponential
  backoff, then fails the job → retried from another peer → re-queued by the
  next manifest sync if still in the playlist.
- **Sender ack-timeout retransmit** with RTT-EWMA + backoff; 30-retry budget
  deliberately outlives the receiver's.
- **Request resend** at half the first-response timeout (dropped request
  costs ~2 s instead of 4 s).
- **Dedup + order-free reassembly** → duplicates/reordering are free (S7).
- **Trial-based availability**: `SEGMENT_NOT_FOUND` → next peer; a request
  when the registry is full gets NOT_FOUND (never a silent drop).
- **Peer ranking**: score (start 50; +2 ok, −10 no-response, −3 other;
  NOT_FOUND counted but unpenalized — it's availability churn, not bad
  service) steers work to healthy peers (S3); eviction after **3 consecutive**
  unresponsive pulls. Ranking visible via `GET /peers` + the 60 s log line.
- **Poll jitter (0-1 s)** so peers don't see new segments in lockstep —
  without it everyone asks everyone for the newest segment before anyone has
  it, and peers never share in steady state (master served 100%).
- **Retention (60 s)**: old segments stay servable past the playlist edge,
  so a viewer 3-4 s behind (or recovering) still finds its pieces.

## Bugs found by fault testing (all fixed)

1. **Blocking paced sends → congestion collapse** under concurrent
   transfers (kernel-buffer overflow → drops → retries → more drops).
   Fixed: deadline-driven chunked sends (`6ca185b`).
2. **A dropped first packet killed the peer**: single handshake attempt,
   hard exit. Fixed: handshake retried in background; manifest polling not
   gated on it.
3. **Synchronized polls killed peer-to-peer sharing** (master served 100% of
   deliveries in steady state). Fixed: poll jitter → peers serve ~30%.
4. **Registry-full requests silently dropped** → looked like a dead peer →
   false evictions. Fixed: answer NOT_FOUND.
5. **Eviction after one timeout** was too aggressive under loss. Fixed: 3
   consecutive.
6. **Fixed quiet/ack timers break under latency**. Fixed: adaptive timers
   (gap-EWMA + backoff on the receiver, RTT-EWMA on the sender).
7. **Lost final ACK left senders retrying for minutes** → sender slots
   clogged (213 registry-full events under 10% loss). Fixed: completed
   receivers stay in grace re-ACKing COMPLETE; registry-full → 0.

## Ideas to try next (M6 backlog)

- **Selective retransmission**: ACK carries a bitmap of the packets missing
  from the current range; the sender resends only those. Whole-range resend
  is the main cost under loss (S1: 5% loss → 5× slower).
- **Forward error correction** (XOR/Reed-Solomon parity packets): recover
  loss with no round trip — pairs well with the 3-4 s latency budget.
- **Pipelined windows**: multiple outstanding ranges (TCP-style) so
  throughput isn't RTT-bound (S5: +100 ms delay → 145 KB/s).
- **Latency term in the score** so delay-sick peers (S6) are avoided too.
- **Master failover**: new segments exist only on the master's disk. A
  shadow master (peer that pre-fetches everything) or elected-seed rotation
  is required for real HA.
- **Score exchange / capabilities in PEERLIST** (1-byte protocol bump):
  swarm-wide ranking; the master ranks all peers in /peers.
- **Player-side jitter buffer (3-4 s)**: makes short bursts invisible; the
  retention window already keeps old segments servable.
- **Stats dashboard**: /stats JSON time series + lag/score graphs in the lab
  monitor pane.
