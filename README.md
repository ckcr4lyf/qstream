# qstream

Modern Rust rewrite of the `udp-file-transfer` P2P live video streaming
design: one master (seed) serves an HLS stream over plain UDP, peers download
segments and re-serve them to each other. Single static binary, no runtime
dependencies.

**Status: Milestones M0 (handshake), M1 (manifest exchange) and M2 (segment
transfer) implemented.** See [SPEC.md](SPEC.md) for the full design and
milestone plan.

## Build

```
cargo build --release
```

## Run

Terminal 1 — master (point it at a live HLS playlist):

```
./target/release/qstream server 3333 live/live.m3u8
```

Terminal 2 — peer (handshakes, then polls the manifest every 3s and keeps a
local copy in the data dir):

```
./target/release/qstream peer 4444 127.0.0.1 3333
```

The peer logs `handshake OK`, then `manifest updated (N segments, M bytes)`
as the live playlist rolls; its copy lands in `./data/live.m3u8` (use a
custom dir: `qstream peer 4444 127.0.0.1 3333 /path/to/dir`). It then
**pulls every missing segment** from the master into the data dir
(`downloaded seg_XXXX.ts (N bytes, ...ms, ... KB/s)`).

## Environment

| Var          | Meaning                                        | Default |
|--------------|------------------------------------------------|---------|
| `QSTREAM_NAME` | node name sent in handshake                  | `master` / `peer` |
| `QSTREAM_LOG`  | log level: `error` `warn` `info` `debug` `trace` | `info` |

## Tests

```
cargo test
```

Unit tests cover the wire-protocol codec (round-trips, malformed datagrams).

## Protocol document

The wire protocol is documented in [PROTOCOL.pdf](PROTOCOL.pdf) (source: `PROTOCOL.typ`, regenerate with `typst compile PROTOCOL.typ PROTOCOL.pdf`).
