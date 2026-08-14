# qstream — SPEC

> Modern Rust rewrite of the `udp-file-transfer` P2P video streaming design.
> Single static binary, no runtime dependencies. `std`-only for now.
>
> Status: **v0.2 — M0 (handshake) + M1 (manifest) implemented; M2 (segment transfer) spec'd.**

---

## 1. Overview

qstream distributes a live HLS video stream over plain UDP in a peer-to-peer
fashion:

- One **master** (seed) node ingests a live HLS stream (manifest `live.m3u8`
  + `.ts` segments) and serves it over UDP.
- **Peers** discover each other, request segments over UDP, and re-serve the
  segments they have to other peers.
- Every node exposes its local copy over HTTP so a player (`ffplay`, `mpv`,
  browser HLS player) can consume it.

UDP is lossy, unordered and unacknowledged. Reliability, ordering and flow
control are implemented **in the protocol layer** (see §7).

Inherited design DNA from `udp-file-transfer` (TS): binary header, window-based
flow control with ACKs, manifest polling, job queue with peer selection.
We modernize: Rust, explicit state machines, tests, fault injection — and we
fix the original's window-desync flaw (see §7) with receiver-driven windows.

## 2. Goals / Non-goals

### Goals
- Single binary; start as `server` (master/seed) or `peer` via subcommand.
- Streaming over UDP with flow control tuned for real-time HLS delivery.
- Peers share load: a peer can serve segments it has downloaded.
- Correct-by-construction flow control: no sender/receiver window desync
  under packet loss (see §7).
- Deterministic, testable protocol codec (property/round-trip tests).

### Non-goals (for now)
- NAT traversal / hole punching / DHT-style discovery (fixed IPs on a LAN).
- Encryption/auth (plain UDP, trusting the network).
- Multicast.
- Non-HLS sources (one live m3u8 stream per master).

## 3. Terminology

| Term      | Meaning                                                     |
|-----------|-------------------------------------------------------------|
| master    | Seed node that owns the source HLS stream and serves it     |
| peer      | Node that downloads from master/peers and re-serves         |
| manifest  | `live.m3u8` playlist listing current `.ts` segment files    |
| segment   | One `.ts` chunk of the stream, transferred as a single file |
| node      | Any running qstream instance (master or peer)               |
| peer list | Set of known nodes; exchanged via PEERLIST messages         |

## 4. Network model

```
                  +----------+
   ffmpeg/HLS --> | master   |  UDP: manifest + segments
                  +----------+
                     ^  |  ^
        handshake /  |  |  |  handshake /
        peerlist     |  |  |  segments (peers serve each other)
                     |  v  |
               +-----+  +--+-----+
               | peer1 | | peer2  |  ...
               +-----+  +--+-----+
                  |         |
             HTTP 1337   HTTP 1338
                  |         |
                ffplay    ffplay
```

- One master. Peers connect to master (or any known peer) to bootstrap.
- Every node has **one UDP socket** it both listens and sends on.
- Every node optionally exposes its folder over HTTP for playback.

## 5. Wire protocol (v2)

All integers are **big-endian** (network byte order). Every datagram is one
message: fixed header + optional payload.

### 5.1 Header (14 bytes)

| Offset | Size | Field         | Notes                             |
|--------|------|---------------|-----------------------------------|
| 0      | 3    | magic         | ASCII `QST` (`0x51 0x53 0x54`)   |
| 3      | 1    | version       | protocol version, currently `0x02`|
| 4      | 1    | message type  | see §5.2                           |
| 5      | 1    | flags         | ACK type for ACK messages, else `0x00` |
| 6      | 2    | data length   | payload length in bytes (0..=65535) |
| 8      | 2    | transfer id   | transfer correlation; `0x0000` if unused |
| 10     | 2    | packet number | 1-based packet index within a transfer |
| 12     | 2    | total packets | total packets in a transfer        |

Datagrams whose magic/version don't match are dropped and logged.

Header v2 (14 B) extends v1 (8 B) with `flags`, `transfer id`, `packet
number` and `total packets` — needed to multiplex concurrent segments on one
socket and to route packets of a transfer. (v2 was trimmed from 16 B during
spec: 3-byte magic instead of 4, no reserved byte.)

### 5.2 Message catalog

| Message              | Code | Direction        | Payload                        | Status     |
|----------------------|------|------------------|--------------------------------|------------|
| HANDSHAKE_REQUEST    | 0x01 | peer → master    | node name (UTF-8)              | ✅ done M0 |
| HANDSHAKE_RESPONSE   | 0x02 | master → peer    | node name (UTF-8)              | ✅ done M0 |
| PING                 | 0x10 | any → any        | —                              | ⏳ planned  |
| PONG                 | 0x11 | any → any        | —                              | ⏳ planned  |
| MANIFEST_REQUEST     | 0x20 | peer → master    | —                              | ✅ done M1 |
| MANIFEST_RESPONSE    | 0x21 | master → peer    | m3u8 contents (raw bytes)      | ✅ done M1 |
| SEGMENT_REQUEST      | 0x30 | any → any        | filename (UTF-8)                  | 📝 spec'd M2 |
| SEGMENT_CONTENTS     | 0x31 | any → any        | file chunk (≤1400 bytes)          | 📝 spec'd M2 |
| SEGMENT_NOT_FOUND    | 0x32 | any → any        | —                                 | 📝 spec'd M2 |
| ACK                  | 0x40 | any → any        | next range (u16 start, u16 count) or empty + `COMPLETE` flag | 📝 spec'd M2 |
| PEERLIST_REQUEST     | 0x50 | peer → master    | —                              | ⏳ planned  |
| PEERLIST_RESPONSE    | 0x51 | master → peer    | packed (ip:port) entries       | ⏳ planned  |

### 5.3 Handshake flow (M0)

1. `peer` binds its UDP socket (fixed local port) and sends
   `HANDSHAKE_REQUEST` with its node name to the master.
2. `master` validates the header, records the sender `ip:port` + name in its
   peer list, and replies `HANDSHAKE_RESPONSE` with its own name.
3. `peer` awaits the response with a timeout (`HANDSHAKE_TIMEOUT_MS = 3000`).
   Success ⇒ connected (exit 0 in CLI); timeout ⇒ error (exit 1).

A node that receives `HANDSHAKE_RESPONSE` for a request it never made is
ignored/logged. Duplicate handshakes are idempotent (peer list is keyed by
`SocketAddr`; name updates are allowed).

### 5.4 Manifest exchange (M1)

1. After a successful handshake, the peer sends `MANIFEST_REQUEST` to the
   master every `MANIFEST_POLL_INTERVAL_MS = 3000`.
2. The master re-reads its manifest file from disk on every request (the live
   playlist rolls) and replies `MANIFEST_RESPONSE` with the raw m3u8 bytes.
3. The peer writes the response atomically (tmp + rename) to
   `<data-dir>/live.m3u8`, keeping its local copy in sync.
4. Empty response (master read failure) → peer keeps its previous copy and
   logs a warning. Request timeout → warn and retry on the next poll.

There is no per-request retry inside a poll; the next poll (3s later) is the
retry. A peer tracks the last manifest it wrote and only rewrites on change.

### 5.5 Segment transfer (M2)

A segment is transferred as one **transfer**, identified by a random
`transfer id` chosen by the requester and echoed by the responder in every
related datagram (content packets *and* ACKs). All datagrams for a transfer
go to/from the node's fixed listening socket — there are no ephemeral
sender sockets — so routing is purely by transfer id.

Messages:

- `SEGMENT_REQUEST` — payload: filename (UTF-8); `transfer id` = fresh random.
- `SEGMENT_CONTENTS` — payload: a chunk of the file, ≤ `SEGMENT_PACKET_SIZE`
  bytes; `packet number` = 1-based index; `total packets` = N.
- `SEGMENT_NOT_FOUND` — responder lacks the file; transfer fails.
- `ACK` — payload: next range `(start, count)` (see §7), or empty with
  `flags = COMPLETE`.

Packetization: a file of size S yields `N = max(1, ceil(S / 1400))` packets;
all but the last are exactly 1400 bytes. An empty file yields one packet with
a zero-length payload. Datagram size ≤ 14 + 1400 = 1414 bytes (MTU-safe).

## 6. Node states

```
master:  Listening ──► Serving (handshake, manifest + segment requests until Ctrl-C)

peer:    Idle ──► Handshaking ──► Synced ──► ManifestSync ──► SegmentSync
         (poll manifest,          (download missing segments, serve own copy)
         write local copy)
```

## 7. Reliability & flow control (M2)

Reliability is **receiver-driven**. The receiver owns the window: it
explicitly names the next packet range it wants. This eliminates the
window-size desync that plagued the original design, where both sides
guessed the window size independently and diverged whenever an ACK was lost.

### 7.1 Receiver (per transfer)

State: `have` (set of received packet numbers), `current = (start, count)`
(the range it is asking for), `retry_count`, timers.

1. Send `SEGMENT_REQUEST`.
2. On the first `SEGMENT_CONTENTS`: learn `N = total packets`; request
   `current = (1, min(INITIAL_WINDOW, N))` via `ACK`.
3. On content packets: dedup into `have`.
   - Range complete (all `count` packets of `current` present):
     - all N packets present → assemble file, write atomically
       (tmp + rename), send `ACK(COMPLETE)`, keep the transfer in a short
       COMPLETE grace so a late duplicate re-triggers `ACK(COMPLETE)`.
     - else → `current = (start+count, min(2*count, MAX_WINDOW, N))`;
       send `ACK` with the new range; reset `retry_count`.
4. Quiet period (`WINDOW_QUIET_MS` with no new packets):
   - range incomplete → re-send the same `ACK` (retransmit request);
     `retry_count++`; fail after `WINDOW_RETRY_LIMIT`.
   - range complete → our previous ACK was lost → re-send it (nudge).
5. No packets at all within `FIRST_RESPONSE_TIMEOUT_MS` → fail.
6. `SEGMENT_NOT_FOUND` → fail.

Window growth `count → min(2*count, MAX_WINDOW)` is slow-start: the window
doubles per successfully completed range up to the cap.

### 7.2 Sender (per transfer)

State: `file`, `N`, `last_range = (start, count)`, `retry_count`.

1. On `SEGMENT_REQUEST`: read the file; if missing → `SEGMENT_NOT_FOUND`.
2. On `ACK(range)`: clamp `count` to remaining packets; `last_range = range`;
   send it (paced); reset `retry_count`; arm the ack timer.
3. On `ACK(COMPLETE)`: free transfer state.
4. Ack timer (`ACK_TIMEOUT_MS = count * PACE_INTERVAL_MS + WINDOW_QUIET_MS
   + 100`, min 300 ms): resend `last_range` (receiver dedups);
   `retry_count++`; fail after `ACK_RETRY_LIMIT`.

The sender never guesses window sizes — it executes the receiver's ranges —
and never needs to know which packets were lost: retransmitting the last
range is safe (receiver dedups), and if the receiver has moved on it re-sends
its current range (nudge), which the sender adopts.

### 7.3 Convergence

Every loss case converges because the receiver re-states its intent on every
datagram and every quiet period, while the sender re-sends its last range on
ack timeout:

- **content loss** → quiet period → same range re-requested → filled → advance
- **ACK loss** → sender ack timeout → resend → receiver nudge → new range
- **COMPLETE loss** → sender ack timeout → resend → receiver re-sends
  COMPLETE (grace period) → sender frees

### 7.4 Pacing & settings

| Setting                     | Value | Meaning                                |
|-----------------------------|-------|----------------------------------------|
| `SEGMENT_PACKET_SIZE`       | 1400  | bytes per content packet               |
| `INITIAL_WINDOW`            | 5     | first range size                       |
| `MAX_WINDOW`                | 64    | largest range size                     |
| `PACE_INTERVAL_MS`          | 1     | sleep between packets (rate limiter)   |
| `WINDOW_QUIET_MS`           | 150   | receiver quiet period before ACKing    |
| `FIRST_RESPONSE_TIMEOUT_MS` | 2000  | give up if nothing arrives             |
| `WINDOW_RETRY_LIMIT`        | 8     | receiver re-request limit              |
| `ACK_RETRY_LIMIT`           | 8     | sender resend limit                    |
| `MAX_CONCURRENT_TRANSFERS`  | 16    | per-node transfer registry bound       |
| `COMPLETE_GRACE_MS`         | 2000  | keep done transfers to re-ACK COMPLETE |

Pacing is the throughput limiter: 1 packet/ms ≈ 11 Mbps ceiling, ample for a
1 Mbps HLS stream. Env overrides: `QSTREAM_PACING_MS`, `QSTREAM_QUIET_MS`.

### 7.5 Node internals

Each node runs one recv loop on its single socket. Incoming datagrams are
routed by transfer id to a per-transfer state machine; transfers expose their
next deadline and the loop's read timeout is the minimum of all deadlines, so
a single thread drives every transfer. Non-transfer messages (handshake,
manifest) are handled inline.

The transfer registry is bounded (`MAX_CONCURRENT_TRANSFERS`); failed,
complete and timed-out transfers are evicted. Filenames are validated to
reject path traversal (no `/`, `\`, leading `.`, control characters).

## 8. CLI

```
qstream server <port> <manifest-path>                        # master/seed mode
qstream peer <local-port> <remote-ip> <remote-port> [data-dir]       # peer mode
qstream --help
```

- `server <port> <manifest-path>` binds `0.0.0.0:<port>` and serves the
  m3u8 playlist at `<manifest-path>` (validated at startup). Segments are
  served from the manifest's directory (`dirname(manifest-path)`).
- `peer <local-port> ...` binds `0.0.0.0:<local-port>` so peers can later be
  reached by other peers on a known port. `[data-dir]` defaults to `./data`;
  the synced manifest is written to `<data-dir>/live.m3u8`.
- `QSTREAM_LOG=error|warn|info|debug|trace` controls verbosity (default info).

## 9. Milestones

| #  | Milestone                                        | Status |
|----|--------------------------------------------------|--------|
| M0 | Scaffold + UDP handshake (this spec, §5.3)       | ✅     |
| M1 | Manifest exchange (poll + serve)                 | ✅     |
| M2 | Segment transfer: receiver-driven windows, ACKs, reassembly | 📝 spec'd §5.5/§7 |
| M3 | Peer discovery (PEERLIST) + job queue            | ⏳     |
| M4 | Live HLS integration (ffmpeg → segments → HTTP)  | ⏳     |
| M5 | Robustness: retransmission, timeouts, fault tests| ⏳     |

## 10. Testing strategy

- **Unit:** codec round-trips (encode/decode every message), malformed
  header rejection (bad magic/version/truncation), length edge cases;
  packetize/assemble round-trips (out of order, duplicates, partial last
  packet).
- **Integration:** master + peer on loopback; assert handshake outcome,
  peer list contents, and that the peer's synced manifest tracks the
  master's rolling playlist (sequence numbers advance in lockstep); a
  requested segment arrives byte-identical to the master's file.
- **Fault injection (M2+):** test harness that drops/duplicates/reorders
  packets via an env var, to validate window/retry logic.
- **E2E (M4+):** ffmpeg-generated HLS → master → 2 peers → `ffplay` playback
  of all three nodes.
