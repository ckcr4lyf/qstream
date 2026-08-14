# qstream — SPEC

> Modern Rust rewrite of the `udp-file-transfer` P2P video streaming design.
> Single static binary, no runtime dependencies. `std`-only for now.
>
> Status: **v0 — Milestone M0 (project scaffold + handshake) implemented.**

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

Inherited design DNA from `udp-file-transfer` (TS):
10-byte binary header, window-based flow control with
`DOUBLE / STAY / HALF / RETRY` ACKs, manifest polling, job queue with peer
selection. We modernize: Rust, explicit state machines, tests, fault injection.

## 2. Goals / Non-goals

### Goals
- Single binary; start as `server` (master/seed) or `peer` via subcommand.
- Streaming over UDP with flow control tuned for real-time HLS delivery.
- Peers share load: a peer can serve segments it has downloaded.
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

## 5. Wire protocol (v0)

All integers are **big-endian** (network byte order). Every datagram is one
message: fixed header + optional payload.

### 5.1 Header (8 bytes)

| Offset | Size | Field          | Notes                                   |
|--------|------|----------------|-----------------------------------------|
| 0      | 4    | magic          | ASCII `QSTR` (`0x51 0x53 0x54 0x52`)   |
| 4      | 1    | version        | protocol version, currently `0x01`      |
| 5      | 1    | message type   | see §5.2                                |
| 6      | 2    | data length    | payload length in bytes (0..=65535)     |

Messages whose magic/version don't match are dropped and logged.

### 5.2 Message catalog

| Message              | Code | Direction        | Payload                        | Status     |
|----------------------|------|------------------|--------------------------------|------------|
| HANDSHAKE_REQUEST    | 0x01 | peer → master    | node name (UTF-8)              | ✅ done M0 |
| HANDSHAKE_RESPONSE   | 0x02 | master → peer    | node name (UTF-8)              | ✅ done M0 |
| PING                 | 0x10 | any → any        | —                              | ⏳ planned  |
| PONG                 | 0x11 | any → any        | —                              | ⏳ planned  |
| MANIFEST_REQUEST     | 0x20 | peer → master    | —                              | ⏳ planned  |
| MANIFEST_RESPONSE    | 0x21 | master → peer    | m3u8 contents                  | ⏳ planned  |
| SEGMENT_REQUEST      | 0x30 | any → any        | filename (UTF-8)               | ⏳ planned  |
| SEGMENT_CONTENTS     | 0x31 | any → any        | file data (packetized)         | ⏳ planned  |
| SEGMENT_NOT_FOUND    | 0x32 | any → any        | —                              | ⏳ planned  |
| ACK                  | 0x40 | any → any        | flow-control flags (see §7)    | ⏳ planned  |
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

## 6. Node states

```
master:  Listening ──► Serving (handles requests until Ctrl-C)

peer:    Idle ──► Handshaking ──► Synced ──► Streaming (poll manifest,
         queue segments, serve peers)        ──► (planned M1+)
```

## 7. Reliability & flow control (planned, inherited from v1 design)

- **Windows:** sender transmits windows of `window_size` packets; receiver
  ACKs each window. Initial window 5.
- **ACK flags:** `DOUBLE` (all received — double window), `STAY` (received
  after a timeout — keep size), `HALF` (lossy window — halve),
  `RETRY` (retransmit current window), `COMPLETE` (transfer done).
- **Pacing:** `SEND_INTERVAL_COUNT` packets, then sleep `SEND_INTERVAL_TIME` ms
  (configurable; default 1 packet / 1 ms).
- **Receiver:** dedup by packet number, out-of-order reassembly, window
  timeout → RETRY (up to `WINDOW_RETRY_LIMIT`), then fail the transfer.
- **Segment size:** `1400` bytes (safe for typical MTU 1500 minus headers).

## 8. CLI

```
qstream server <port>                          # master/seed mode
qstream peer <local-port> <remote-ip> <remote-port>   # peer mode
qstream --help
```

- `server <port>` binds `0.0.0.0:<port>`.
- `peer <local-port> ...` binds `0.0.0.0:<local-port>` so peers can later be
  reached by other peers on a known port.

## 9. Milestones

| #  | Milestone                                        | Status |
|----|--------------------------------------------------|--------|
| M0 | Scaffold + UDP handshake (this spec, §5.3)       | ✅     |
| M1 | Manifest exchange (poll + serve)                 | ⏳     |
| M2 | Segment transfer: windows, ACKs, reassembly      | ⏳     |
| M3 | Peer discovery (PEERLIST) + job queue            | ⏳     |
| M4 | Live HLS integration (ffmpeg → segments → HTTP)  | ⏳     |
| M5 | Robustness: retransmission, timeouts, fault tests| ⏳     |

## 10. Testing strategy

- **Unit:** codec round-trips (encode/decode every message), malformed
  header rejection (bad magic/version/truncation), length edge cases.
- **Integration:** master + peer on loopback; assert handshake outcome and
  peer list contents.
- **Fault injection (M2+):** test harness that drops/duplicates/reorders
  packets via an env var, to validate window/retry logic.
- **E2E (M4+):** ffmpeg-generated HLS → master → 2 peers → `ffplay` playback
  of all three nodes.
