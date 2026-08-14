# qstream

Modern Rust rewrite of the `udp-file-transfer` P2P live video streaming
design: one master (seed) serves an HLS stream over plain UDP, peers download
segments and re-serve them to each other. Single static binary, no runtime
dependencies.

**Status: Milestone M0 — scaffold + UDP handshake.** See [SPEC.md](SPEC.md)
for the full design and milestone plan.

## Build

```
cargo build --release
```

## Run

Terminal 1 — master:

```
./target/release/qstream server 3333
```

Terminal 2 — peer:

```
./target/release/qstream peer 4444 127.0.0.1 3333
```

The peer should log `handshake OK`, and the master should log
`peer connected: 127.0.0.1:4444`.

Set a custom node name (sent in the handshake payload) with:

```
QSTREAM_NAME=peer-alpha ./target/release/qstream peer 4444 127.0.0.1 3333
```

## Tests

```
cargo test
```

Unit tests cover the wire-protocol codec (round-trips, malformed datagrams).
