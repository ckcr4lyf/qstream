# qstream — SPEC

> Modern Rust rewrite of the `udp-file-transfer` P2P video streaming design.
> Single binary, no runtime dependencies, `std`-only for now.
>
> Current status: **protocol v3; M0–M5 implemented; NAT traversal N1–N4
> implemented and lab-verified.** This document describes the current design,
> while [DEVLOG.md](DEVLOG.md) records implementation history and
> [NAT.md](NAT.md) covers the NAT-specific design and remaining relay work.

## 1. Overview

qstream distributes a live HLS video stream over plain UDP in a peer-to-peer
fashion:

- One **master** (seed) node ingests a live HLS stream (manifest `live.m3u8`
  + `.ts` segments) and serves it over UDP.
- **Peers** discover each other, request segments over UDP, and re-serve the
  segments they have to other peers.
- Every node can expose its local copy over HTTP so a player (`ffplay`, `mpv`,
  browser HLS player) can consume it.

UDP is lossy, unordered and unacknowledged. Reliability, ordering, flow
control, endpoint discovery, and NAT path maintenance are implemented above
UDP. The protocol is deliberately plain and unauthenticated.

Inherited design DNA from `udp-file-transfer` (TS): binary header, window-based
flow control with ACKs, manifest polling, and a job queue with peer selection.
We modernize it with Rust, explicit state machines, tests, fault injection,
NAT traversal, observability, and a fix for the original window-desync flaw:
windows are receiver-driven.

## 2. Goals / non-goals

### Goals

- Single binary; start as `server` (master/seed) or `peer` via subcommand.
- Streaming over UDP with flow control tuned for real-time HLS delivery.
- Peers share load: a peer can serve segments it has downloaded.
- Correct-by-construction flow control under packet loss.
- Deterministic, testable protocol codec.
- Direct operation across common cone/restricted NATs and same-LAN peers,
  using endpoint observation, PING/PONG hole punching, LAN beacons, and
  opportunistic UPnP-IGD mapping.
- Operational visibility through peer stats, JSON stats, and Prometheus
  metrics.

### Non-goals (current)

- Encryption, authentication, or authorization: the network is trusted by
  assumption.
- Relay fallback for symmetric-NAT pairs.
- Master failover or high availability.
- DHT-style discovery or an unbounded/full-mesh peer list.
- Multicast.
- Non-HLS sources (one live m3u8 stream per master).
- General-purpose HTTP serving (the embedded server is a minimal playback and
  inspection endpoint).

## 3. Terminology

| Term | Meaning |
|---|---|
| master | Seed node that owns the source HLS stream and serves it |
| peer | Node that downloads from master/peers and re-serves segments |
| manifest | `live.m3u8` playlist listing current `.ts` segment files |
| segment | One `.ts` chunk of the stream, transferred as a single file |
| node | Any running qstream instance, master or peer |
| peer list | Bounded set of known nodes exchanged via PEERLIST messages |
| inventory | Compact newest-first bitmap of a node's recent local segments |
| parent | A low-load peer temporarily preferred by the master for replication |
| origin seeder | A peer granted a per-segment lease to receive a normal origin copy |

## 4. Network model

```text
                  +----------+
   ffmpeg/HLS --> | master   |  UDP: manifest + segments
                  +----------+
                     ^  |  ^
        handshake /  |  |  |  handshake /
        peerlist     |  |  |  segments (peers serve each other)
                     |  v  |
               +-----+  +--+-----+
               | peer1 | | peer2  |  ...
               +-----+ +---------+
                  |         |
             HTTP 1337   HTTP 1338
                  |         |
                ffplay    ffplay
```

- One master. Peers connect to the configured bootstrap node (normally the
  master, though a known peer can be used as the rendezvous source).
- Every node has one UDP socket it both listens and sends on.
- A node may expose its folder and operational endpoints over HTTP.
- The master advertises at most 16 reachable peers in a peerlist response and
  marks up to two as preferred parents. This bounds each response but does not
  make discovery globally scalable.

## 5. Wire protocol (v3)

All integers are **big-endian** (network byte order). Every datagram is one
message: fixed header plus optional payload. Old protocol versions are
rejected; v3 is not wire-compatible with v2.

### 5.1 Header (14 bytes)

| Offset | Size | Field | Notes |
|---:|---:|---|---|
| 0 | 3 | magic | ASCII `QST` (`0x51 0x53 0x54`) |
| 3 | 1 | version | protocol version, currently `0x03` |
| 4 | 1 | message type | see §5.2 |
| 5 | 1 | flags | ACK subtype or `SEGMENT_NOT_READY`; otherwise `0x00` |
| 6 | 2 | data length | Payload length in bytes (`0..=65535`) |
| 8 | 2 | transfer id | Transfer correlation; `0x0000` if unused |
| 10 | 2 | packet number | 1-based packet index within a transfer |
| 12 | 2 | total packets | Total packets in a transfer |

Datagrams whose magic/version do not match are dropped and logged. Header v3
retains the 14-byte multiplexing layout introduced by the earlier transfer
protocol.

### 5.2 Message catalog

| Message | Code | Direction | Payload | Status |
|---|---:|---|---|---|
| HANDSHAKE_REQUEST | `0x01` | peer → node | 6-byte claimed endpoint + UTF-8 display name | ✅ |
| HANDSHAKE_RESPONSE | `0x02` | node → peer | 6-byte observed endpoint + UTF-8 display name | ✅ |
| MANIFEST_REQUEST | `0x20` | peer → bootstrap | — | ✅ |
| MANIFEST_RESPONSE | `0x21` | bootstrap → peer | raw m3u8 bytes | ✅ |
| SEGMENT_REQUEST | `0x30` | any → any | filename (UTF-8) | ✅ |
| SEGMENT_CONTENTS | `0x31` | any → any | file chunk (≤1400 bytes) | ✅ |
| SEGMENT_NOT_FOUND | `0x32` | any → any | optional 10-byte inventory | ✅ |
| ACK | `0x40` | any → any | next range, or empty COMPLETE | ✅ |
| PEERLIST_REQUEST | `0x50` | peer → any | — | ✅ |
| PEERLIST_RESPONSE | `0x51` | any → peer | packed 7-byte entries | ✅ |
| PING | `0x60` | any → any | 4-byte nonce + UTF-8 display name | ✅ |
| PONG | `0x61` | any → any | optional 10-byte inventory | ✅ |

Peerlist entries contain IPv4 address, port, and flags:

- `PEER_UPNP_MAPPED = 0x01`: the peer's claimed endpoint matched the endpoint
  observed by the master.
- `PEER_SAME_IP = 0x02`: the endpoint shares the requester's observed IPv4
  address and is likely behind the same NAT.
- `PEER_PARENT = 0x04`: the master assigned the peer as a preferred parent.

`SEGMENT_NOT_READY = 0x01` is an additive `SEGMENT_NOT_FOUND` flag. It means
that the origin's bounded seed admission temporarily denied the request; it is
not a definitive absence and must be retried without ordinary NOT_FOUND
accounting. An unflagged response means the responder lacks the file.

The optional inventory is `u64 newest segment number` followed by a `u16`
newest-first mask. Bit 0 represents `newest`, bit 1 represents `newest - 1`,
and so on through 16 recent positions. Inventory answers expire after 15
seconds; unknown or out-of-window pieces remain trial candidates.

### 5.3 Handshake and endpoint observation

1. A node binds its UDP socket and sends `HANDSHAKE_REQUEST` with its claimed
   UPnP endpoint, or `0.0.0.0:0` when it has no mapping, plus its display name.
2. The responder records the source `ip:port`, replies with
   `HANDSHAKE_RESPONSE`, and includes that observed source endpoint.
3. The requester records the observed endpoint as its public endpoint. It
   continues retrying the bootstrap handshake in the background if a response
   is lost.

Names are display labels, not identities. Peer registries are keyed by public
socket endpoint, so multiple nodes using the default name `peer` remain
independent.

### 5.4 Manifest exchange

1. A peer sends `MANIFEST_REQUEST` to its bootstrap node on a staggered,
   roughly two-second polling cadence.
2. The responder re-reads its manifest file and replies with raw m3u8 bytes.
3. The peer writes the response atomically to `<data-dir>/live.m3u8` and
   reconciles its newest-first missing-segment queue with the current playlist.
4. An empty response leaves the previous manifest in place. A request timeout
   is retried on the next poll.

The peer derives `<data-dir>/playback.m3u8` from locally complete files. It
filters out missing segments, advances media sequence appropriately, and holds
back three complete local segments by default; this prevents player 404 storms
at a lagging live edge.

### 5.5 Segment transfer

A segment is transferred as one transfer, identified by a fresh requester
transfer ID echoed by the responder. All datagrams use the node's fixed UDP
socket; routing is by transfer ID plus the participating socket.

- `SEGMENT_REQUEST`: filename payload and fresh transfer ID.
- `SEGMENT_CONTENTS`: file chunk, packet number 1-based, total packet count.
- `SEGMENT_NOT_FOUND`: absence or temporary origin admission denial.
- `ACK`: next packet range `(u16 start, u16 count)`, or empty with
  `flags = 0x04` (`COMPLETE`).

Packetization uses `N = max(1, ceil(size / 1400))` packets, with datagrams no
larger than 1414 bytes. Empty files are represented by one zero-length packet.
Filenames must be flat, printable ASCII names without `/`, `\\`, leading `.`,
control characters, or spaces.

### 5.6 Discovery and NAT path maintenance

Peers poll their bootstrap for a bounded peer list and handshake with newly
seen endpoints. Handshakes are mutual, and serving a segment requires no
handshake after the endpoint is known.

Every node PINGs known peers every 10 seconds. PING/PONG provides keep-alive,
connectivity/liveness evidence, and a direct hole-punch packet. Peers also
broadcast a PING beacon every five seconds to the local broadcast address on
their UDP port; a private source discovered this way is marked as a LAN path
and preferred over a public hairpin path. The beacon nonce prevents a node
from registering its own broadcast echo.

A peer opportunistically requests a UDP UPnP-IGD mapping at startup. The
master verifies a claimed mapping when it matches the observed endpoint. No
relay is implemented, so true symmetric-NAT pairs are not guaranteed to have
a direct path; see [NAT.md](NAT.md).

## 6. Node states

```text
master: Listening -> Serving
        (handshake, manifest, segment requests, HTTP until Ctrl-C)

peer:   boot -> bootstrap handshake (with background retry)
        -> manifest/discovery sync -> parallel segment scheduling
        -> UDP serving + optional HTTP playback/metrics
```

## 7. Reliability, scheduling, and flow control

### 7.1 Receiver-driven windows

The receiver owns the window and explicitly names the next packet range.
Initially it requests `(1, min(5, N))`; after a complete range it doubles the
count up to 64. It deduplicates packets, assembles them out of order, writes
atomically, sends COMPLETE, and remains in a four-second grace period to
re-ACK a lost final acknowledgement.

A quiet receiver re-sends the current range. The sender retransmits its last
range when its adaptive ACK timer expires. This converges under content loss,
ACK loss, duplicates, reordering, and burst loss, but retransmission is of the
whole current range rather than individual missing packets.

### 7.2 Adaptive timers and pacing

The base settings are:

| Setting | Value |
|---|---:|
| `SEGMENT_PACKET_SIZE` | 1400 bytes |
| `INITIAL_WINDOW` | 5 |
| `MAX_WINDOW` | 64 |
| `QSTREAM_PACING_MS` | 1 |
| `QSTREAM_QUIET_MS` | 150 |
| `QSTREAM_FIRST_TIMEOUT_MS` | 4000 |
| receiver retry limit | 8 |
| sender retry limit | 30 |
| complete grace | 4000 ms |
| transfer registry bound | 32 active transfers |
| peer list response bound | 16 peers |
| parent assignments | up to 2 peers |
| parallel downloads | 4 |
| in-flight pulls per peer | 2 |

Receiver quiet periods use the observed inter-packet-gap EWMA and exponential
backoff, capped at eight seconds. Sender ACK timers use a measured RTT EWMA,
exponential backoff, and the longer 30-retry budget. If no first packet arrives
by half the first-response timeout, the request is resent once.

### 7.3 Fault injection

The `FaultInjector` applies seeded drop, duplicate, delay, reorder, and periodic
burst faults to all outgoing datagrams, including control messages. This is
used by the lab scripts to test real protocol behavior rather than only data
packets.

### 7.4 Inventory-aware scheduling and origin seeding

The peer scheduler reconciles queued work against the current rolling
manifest, prioritizes the newest entries, and keeps a per-segment tried set.
Source selection is tiered:

1. peers whose fresh inventory positively confirms the requested segment;
2. the bootstrap/master when its fresh inventory confirms it;
3. unknown candidates only when no authoritative fresh source exists.

Fresh negative inventory and exact `SEGMENT_NOT_FOUND` answers suppress a peer
for 15 seconds. Temporary `SEGMENT_NOT_READY` responses remove the source
from the tried set and retry after a five-second source cooldown. The master
retains the origin as an authoritative recovery path.

For each segment the master normally admits at most two viable origin seeders
(`QSTREAM_ORIGIN_SEEDERS=2`). Existing leases remain valid, stale/absent
seeders can be replaced, and a safety check admits a recovery seed when no
other reachable peer has a fresh positive copy. The bounded origin policy is
intended to fan new segments through the overlay rather than make every peer
pull every live-edge segment from the master.

### 7.5 Peer ranking and accounting

Each peer starts at score 50. Successful pulls add 2, no-response timeouts
subtract 10, and other failures subtract 3, clamped to 0–100. Definitive
NOT_FOUND responses are tracked but do not penalize the score because they
represent availability churn, not bad service. A peer is evicted after three
consecutive unresponsive pulls. `/peers` exposes ranking, path freshness,
inventory, transfer counts, latency, and directional payload bytes.

The node also exposes `/stats` JSON and `/metrics` Prometheus text, including
origin seed assignments/denials and per-peer downloaded/uploaded byte totals.

## 8. CLI

```text
qstream server <port> <manifest-path> [http-port]
qstream peer <local-port> <remote-ip> <remote-port> [data-dir] [http-port]
qstream --help
```

`server` binds `0.0.0.0:<port>`, validates the manifest path, and serves
segments from its directory. `peer` binds `0.0.0.0:<local-port>`, writes its
synchronized state below `[data-dir]` (default `./data`), and can start the
embedded HTTP server. `QSTREAM_NAME` controls the display name.

## 9. Milestones

| Milestone | Status |
|---|---|
| M0 scaffold + UDP handshake | ✅ |
| M1 manifest exchange | ✅ |
| M2 reliable receiver-driven segment transfer | ✅ |
| M3 peer discovery + parallel job queue | ✅ |
| M4 live HLS playback over embedded HTTP | ✅ |
| M5 fault injection, adaptive timers, ranking, retention, inventory-aware scheduling, bounded parents/origin seeding | ✅ |
| N1 endpoint observation and protocol v3 handshake/peerlist | ✅ |
| N2 PING/PONG keep-alive and direct hole punching | ✅ |
| N3 same-LAN beacon paths | ✅ |
| N4 opportunistic UPnP-IGD mapping | ✅ |
| M6 selective retransmission, pipelined windows, FEC, master failover, relay, and broader swarm scaling | ⏳ |

## 10. Playback and HTTP

Playback is out-of-band and uses plain HTTP, not the UDP protocol. Each node
embeds a minimal `std`-only HTTP/1.1 server with GET/HEAD and
`Connection: close`. It serves:

- `GET /live.m3u8` — the raw/filtered manifest;
- `GET /playback.m3u8` — the peer's holdback playlist when present;
- `GET /seg_NNNN.ts` — a complete segment;
- `GET /health` — plain `ok`;
- `GET /peers` — human-readable ranking;
- `GET /stats` — JSON statistics;
- `GET /metrics` — Prometheus exposition.

Only flat validated filenames are served; there is no directory listing, path
traversal, range support, authentication, or encryption. Point `ffplay` at
`playback.m3u8` for peers and `live.m3u8` for a master.

## 11. Testing strategy and known limits

- **Unit:** codec round trips, malformed headers/payloads, availability masks,
  playlist filtering, filename validation, fault-injector behavior, and
  scheduler/lease helpers (`cargo test`).
- **Deterministic integration:** `python3 scripts/test_suite.py` builds a
  synthetic HLS origin, starts real release binaries with temporary directories
  and dynamically reserved localhost ports, then verifies HTTP health/playback,
  traversal rejection, byte-for-byte replication, peer-to-peer sharing, and
  seeded 5% loss recovery. It does not require tmux, ffmpeg, the checked-in
  live playlist, or sudo.
- **Loopback/lab integration:** the longer scripts exercise rolling manifests,
  peer discovery, restart recovery, and peer/master kill scenarios.
- **Fault testing:** loss, delay, duplication, reordering, and burst scenarios
  are recorded in [REPORT.md](REPORT.md).
- **NAT testing:** `scripts/natlab.sh` exercises NATed master pulls,
  same-LAN paths, cross-NAT paths, and fake UPnP verification. Its iptables
  SNAT is port-preserving rather than a full true-symmetric NAT model.
- **Playback:** embedded HTTP status/MIME/filtering behavior is covered by unit
  tests and the lab's curl/player checks.

Known architectural limits are deliberate: whole-window retransmission is
expensive under uniform loss, one-window-at-a-time flow control is RTT-bound,
master failure stops new content, direct symmetric-NAT pairs need a relay, and
bounded peerlists are not a DHT. These are the primary M6 directions.
