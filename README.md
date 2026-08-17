# qstream

Modern Rust rewrite of the `udp-file-transfer` P2P live video streaming
design: one master (seed) serves an HLS stream over plain UDP, peers download
segments and re-serve them to each other. Single static binary, no runtime
dependencies.

**Status: Milestones M0–M4 implemented** (handshake, manifest, segment transfer, peer discovery + parallel job queue, HTTP playback). See [SPEC.md](SPEC.md) for the full design and
milestone plan.

## Build

```
cargo build --release
```

## Run

Terminal 1 — master (point it at a live HLS playlist, serve it over HTTP):

```
./target/release/qstream server 3333 live/live.m3u8 8080
```

Terminal 2 — peer (handshakes, syncs the manifest, pulls segments in
parallel from the master and any discovered peers, serves over HTTP):

```
./target/release/qstream peer 4444 127.0.0.1 3333 ./data 8081
```

Terminal 3 — watch the stream (from the peer — it's replicated over UDP):

```
ffplay http://127.0.0.1:8081/playback.m3u8 -live_start_index 0
```

The peer logs `handshake OK`, then `manifest updated (N segments, M bytes)`
as the live playlist rolls; its raw synced manifest lands in `./data/live.m3u8`
(use a custom dir: `qstream peer 4444 127.0.0.1 3333 /path/to/dir`). It also
creates `./data/playback.m3u8` for local players: this lists only complete
local segments and holds back three segments by default to absorb replication
jitter. `live.m3u8` remains the manifest shared with other qstream peers. It
then **pulls every missing segment** — several in parallel, from the master
*and* any peers it discovers via peerlists (`pulling seg_X from <addr>`,
`downloaded seg_XXXX.ts (N bytes, ...ms, ... KB/s)`).

## Environment

| Var          | Meaning                                        | Default |
|--------------|------------------------------------------------|---------|
| `QSTREAM_NAME` | node name sent in handshake                  | `master` / `peer` |
| `QSTREAM_LOG`  | log level: `error` `warn` `info` `debug` `trace` | `info` |
| `QSTREAM_PLAYBACK_HOLDBACK_SEGMENTS` | complete local segments withheld from `playback.m3u8` | `3` |

## Tests

```
cargo test
```

Unit tests cover the wire-protocol codec (round-trips, malformed datagrams).

## Protocol document

The wire protocol is documented in [PROTOCOL.pdf](PROTOCOL.pdf) (source: `PROTOCOL.typ`, regenerate with `typst compile PROTOCOL.typ PROTOCOL.pdf`).
