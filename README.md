# qstream

Modern Rust rewrite of the `udp-file-transfer` P2P live-video streaming
design. One master (seed) serves an HLS stream over UDP; peers download
segments and re-serve them to one another. The project is a single Rust
binary with no runtime dependencies and a `std`-only implementation.

**Current status:** protocol v3, milestones M0–M5, and NAT traversal milestones
N1–N4 are implemented and lab-verified. This includes receiver-driven reliable
segment transfer, adaptive timers, fault injection, peer ranking, inventory-
aware scheduling, bounded parent assignments, bounded origin seeding, LAN
beacons, UDP hole punching, and opportunistic UPnP-IGD mapping. See
[SPEC.md](SPEC.md), [NAT.md](NAT.md), and the running [DEVLOG.md](DEVLOG.md).

This is an experimentally validated prototype, not a secure public swarm:
the UDP protocol and HTTP endpoint have no authentication or encryption, and
there is no relay or master failover yet.

## Build

```sh
cargo build --release
```

## Run

Terminal 1 — master, pointed at a live HLS playlist:

```sh
./target/release/qstream server 3333 live/live.m3u8 8080
```

Terminal 2 — peer, bootstrapped from the master:

```sh
./target/release/qstream peer 4444 127.0.0.1 3333 ./data 8081
```

Terminal 3 — watch the peer's replicated stream:

```sh
ffplay http://127.0.0.1:8081/playback.m3u8 -live_start_index 0
```

The peer keeps the raw synchronized manifest in `./data/live.m3u8` and
creates `./data/playback.m3u8` for local players. The playback manifest lists
only complete local segments and holds back three complete segments by
default to absorb replication jitter. The raw manifest is the swarm's
synchronization state; the playback manifest is derived local state.

Peers poll the bootstrap node for the rolling manifest and bounded peer list,
then pull missing segments in parallel. Source selection prefers fresh peer
inventories and retains the master as an authoritative recovery fallback.
Peers also serve completed local segments to other nodes.

## Operations and environment

| Variable | Meaning | Default |
|---|---|---:|
| `QSTREAM_NAME` | Display name sent in handshakes and PINGs; names are not identities | `master` / `peer` |
| `QSTREAM_LOG` | `error`, `warn`, `info`, `debug`, or `trace` | `info` |
| `QSTREAM_PACING_MS` | Minimum spacing between outgoing content packets | `1` |
| `QSTREAM_QUIET_MS` | Base receiver quiet period; timers adapt to observed gaps | `150` |
| `QSTREAM_FIRST_TIMEOUT_MS` | First-content timeout, including one request resend | `4000` |
| `QSTREAM_RETENTION_SECS` | Keep segments past the playlist edge for serving/playback; `0` disables pruning | `0` |
| `QSTREAM_PLAYBACK_HOLDBACK_SEGMENTS` | Complete local segments withheld from `playback.m3u8` | `3` |
| `QSTREAM_ORIGIN_SEEDERS` | Maximum normal origin seed leases per segment | `2` |
| `QSTREAM_NO_UPNP` | Disable opportunistic UPnP-IGD port mapping when set | unset |
| `QSTREAM_FAULT_DROP_PCT` | Percentage of outgoing datagrams dropped | `0` |
| `QSTREAM_FAULT_DUP_PCT` | Percentage sent twice | `0` |
| `QSTREAM_FAULT_DELAY_MS` | Fixed one-way outgoing delay | `0` |
| `QSTREAM_FAULT_REORDER_PCT` | Percentage of sends reordered | `0` |
| `QSTREAM_FAULT_BURST_EVERY_MS` / `QSTREAM_FAULT_BURST_DUR_MS` | Periodic full-drop bursts | `0` / `0` |
| `QSTREAM_FAULT_SEED` | Fault-injector RNG seed; `0` is time-based | `0` |

With HTTP enabled, nodes expose `/health`, `/peers`, `/stats`, and
`/metrics`. The embedded server is intentionally minimal (GET/HEAD,
connection-close, flat validated filenames, no ranges) and is intended for
local playback and operational inspection.

## Tests and validation

```sh
cargo test
cargo fmt -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release
python3 scripts/test_suite.py
```

Unit tests cover codecs, malformed datagrams, playlist filtering, scheduling
helpers, fault injection, and transfer helpers. The deterministic integration
suite in `scripts/test_suite.py` starts real release binaries against a
synthetic HLS origin and verifies HTTP health/playback, traversal rejection,
byte-for-byte replication, peer-to-peer sharing, and recovery under a seeded
5% loss link. It uses temporary directories and dynamically reserved localhost
ports, so it does not depend on tmux, ffmpeg, the checked-in live playlist, or
sudo. The longer `scripts/` labs additionally exercise loopback swarms, packet
loss/delay/reordering, peer and master failure, restart recovery, and a
netns/iptables NAT matrix. The NAT lab uses port-preserving SNAT rather than a
full true-symmetric NAT emulator; symmetric NAT pairs without a stable
reachable endpoint still require a relay, which is not implemented.

## Protocol document

The current wire protocol is documented in [PROTOCOL.pdf](PROTOCOL.pdf), with
source in [PROTOCOL.typ](PROTOCOL.typ). Regenerate it with:

```sh
typst compile PROTOCOL.typ PROTOCOL.pdf
```
