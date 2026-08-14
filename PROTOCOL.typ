#import "@preview/cetz:0.3.4": canvas, draw

#set page(paper: "a4", margin: (x: 2.2cm, y: 2cm))
#set text(size: 10.5pt, lang: "en")
#set par(justify: true)
#set heading(numbering: "1.1")

#show heading: set block(above: 1.1em, below: 0.5em)
#show heading.where(level: 1): set text(size: 16pt)
#show heading.where(level: 2): set text(size: 12.5pt)
#show heading.where(level: 3): set text(size: 11pt, style: "italic")

#let mono(body) = text(font: "DejaVu Sans Mono", size: 8.5pt, body)
#let hexcode(v) = mono[#v]

// table.header in this typst version doesn't accept fill — build header
// rows as filled cells instead.
#let thead(fill, ..cells) = cells.pos().map(c => table.cell(fill: fill, c))

// ---------------------------------------------------------------- diagrams
// Sequence diagram helper: participants (name, x-cm), then messages
// (from, to, label, style) where style ∈ ("req", "resp", "ack", "data")
#let seqdiagram(participants, messages, height: auto) = {
  let n = participants.len()
  let x(i) = 0.8 + i * 3.2
  let y(k) = -1.35 - k * 1.5
  let bottom = if height == auto {
    y(messages.len() - 1) - 0.8
  } else { height }
  let style-color(style) = if style == "req" {
    rgb("#2563eb")
  } else if style == "resp" {
    rgb("#16a34a")
  } else if style == "ack" {
    rgb("#7c3aed")
  } else {
    rgb("#0f766e")
  }
  let style-dash(style) = if style == "ack" { (dash: "dashed") } else { (:) }
  canvas(length: 1cm, {
    import draw: *
    for (i, p) in participants.enumerate() {
      line((x(i), 0.35), (x(i), bottom), stroke: rgb("#94a3b8") + 0.8pt)
      content(
        (x(i), 0.12),
        text(p.first(), size: 9pt),
        fill: rgb("#e0e7ff"),
        stroke: 0.6pt + rgb("#6366f1"),
        frame: "rect",
        padding: 0.2cm,
      )
    }
    for (k, m) in messages.enumerate() {
      let (a, b, label, style) = m
      let (x1, x2) = (x(a), x(b))
      let y = y(k)
      let dir = if x1 < x2 { 1 } else { -1 }
      let color = style-color(style)
      let dash = if style == "ack" { "dashed" } else { "solid" }
      line(
        (x1 + 0.25 * dir, y),
        (x2 - 0.25 * dir, y),
        stroke: (paint: color, thickness: 1.1pt, dash: dash),
        mark: (end: ">"),
      )
      content(
        ((x1 + x2) / 2, y + 0.22),
        text(label, size: 8pt, fill: rgb("#334155")),
      )
    }
  })
}

// ---------------------------------------------------------------- document

#align(center)[
  #text(size: 23pt, weight: "bold")[qstream]
  #v(6pt)
  #text(size: 13.5pt)[Wire Protocol Specification]
  #v(4pt)
  #text(size: 10.5pt, fill: gray)[Version 0.2 (Draft) — header, manifest request, piece request]
]

#v(10pt)
#line(length: 100%, stroke: 0.7pt + rgb("#cbd5e1"))

#align(center)[
  #text(size: 9pt, fill: gray)[
    Companion to `SPEC.md` (design & milestones). This document defines the
    on-the-wire format. Status: #text(fill: rgb("#15803d"))[Implemented — M0, M1, M2].
  ]
]

#v(8pt)

= Introduction

qstream is a peer-to-peer live video streaming protocol carried over plain
UDP. One #strong[master] node serves an HLS playlist (manifest + `.ts`
segments); #strong[peers] download segments and re-serve them to each other.
Every node has a single UDP socket on which it both listens and sends.

UDP gives no ordering, no deduplication and no reliability — everything
beyond "send a datagram" is defined here. This document specifies:

- the fixed #emph[datagram header] (section 3),
- the #emph[message catalog] (section 4),
- the #emph[manifest request] happy path (section 5),
- the #emph[piece (segment) request] happy path (section 6).

The flow-control and retransmission rules are described in `SPEC.md` §7;
only the wire formats are fixed here.

= Conventions

- All integers are #strong[big-endian] (network byte order).
- Every datagram is exactly one message: fixed header + optional payload.
- Byte offsets are 0-based from the start of the datagram.
- Reserved or unused fields are sent as `0x00` and must be ignored on read.
- Example byte strings are written as space-separated hex, e.g.
  #hexcode[51 53 54].

= Datagram header

The header is fixed at #strong[14 bytes] and present in every message.

#v(6pt)
#table(
  columns: 14,
  align: center,
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 3pt,
  ..thead(
    rgb("#e2e8f0"),
    ..range(14).map(i => text(size: 8pt)[#i]),
  ),
  table.cell(colspan: 3, fill: rgb("#fef3c7"), stroke: 0.5pt + rgb("#cbd5e1"))[#text(size: 8pt)[magic]],
  table.cell(colspan: 1, fill: rgb("#fef3c7"))[#text(size: 8pt)[ver]],
  table.cell(colspan: 1, fill: rgb("#fef3c7"))[#text(size: 8pt)[type]],
  table.cell(colspan: 1, fill: rgb("#fef3c7"))[#text(size: 8pt)[flags]],
  table.cell(colspan: 2, fill: rgb("#dcfce7"), stroke: 0.5pt + rgb("#cbd5e1"))[#text(size: 8pt)[data length]],
  table.cell(colspan: 2, fill: rgb("#dcfce7"))[#text(size: 8pt)[transfer id]],
  table.cell(colspan: 2, fill: rgb("#dcfce7"))[#text(size: 8pt)[packet \#]],
  table.cell(colspan: 2, fill: rgb("#dcfce7"))[#text(size: 8pt)[total]],
)
#v(4pt)

#table(
  columns: (auto, auto, auto, 1fr),
  align: (left, left, left, left),
  stroke: 0.4pt + rgb("#cbd5e1"),
  inset: 4pt,
  ..thead(
    rgb("#f1f5f9"),
    strong([Offset]), strong([Size]), strong([Field]), strong([Description]),
  ),
  [0], [3], [magic], [#mono["QST"] (0x51 0x53 0x54). Rejects stray/garbage datagrams.],
  [3], [1], [version], [Protocol version, currently #mono[0x02].],
  [4], [1], [message type], [One of the codes in section 4.],
  [5], [1], [flags], [ACK type for #mono[ACK] messages (#mono[0x00] progress, #mono[0x04] complete); else #mono[0x00].],
  [6], [2], [data length], [Payload length in bytes (0..=65535). Datagram = 14 + this.],
  [8], [2], [transfer id], [Correlates all datagrams of one piece transfer; #mono[0x0000] when unused.],
  [10], [2], [packet number], [1-based packet index within a transfer; #mono[0x0000] when unused.],
  [12], [2], [total packets], [Total packets #mono[N] of the transfer; #mono[0x0000] when unused.],
)

#v(6pt)
#emph[Validation.] A receiver drops a datagram if the magic or version do not
match, or if `data length` exceeds the remaining bytes of the datagram.

== Field usage per message

Fields are only meaningful for certain message types; the others must be zero.
A #emph[transfer] is identified by its `transfer id` — chosen at random by the
requester and echoed by the responder in every related datagram.

#table(
  columns: (auto, auto, auto, auto, auto, auto),
  align: (left, center, center, center, center, left),
  stroke: 0.4pt + rgb("#cbd5e1"),
  inset: 4pt,
  ..thead(
    rgb("#f1f5f9"),
    strong([Message]), strong([flags]), strong([transfer id]), strong([packet \#]), strong([total]), strong([payload]),
  ),
  [HANDSHAKE_REQUEST], [—], [—], [—], [—], [node name (UTF-8)],
  [HANDSHAKE_RESPONSE], [—], [—], [—], [—], [node name (UTF-8)],
  [MANIFEST_REQUEST], [—], [—], [—], [—], [—],
  [MANIFEST_RESPONSE], [—], [—], [—], [—], [m3u8 contents],
  [SEGMENT_REQUEST], [—], [#strong[✓] transfer], [—], [—], [filename (UTF-8)],
  [SEGMENT_CONTENTS], [—], [#strong[✓] transfer], [#strong[✓] index], [#strong[✓] N], [file chunk ≤ 1400 B],
  [SEGMENT_NOT_FOUND], [—], [#strong[✓] transfer], [—], [—], [—],
  [ACK], [#strong[✓] type], [#strong[✓] transfer], [—], [—], [next range (start, count) — see §6],
)

= Message catalog

#table(
  columns: (auto, auto, auto, auto, 1fr),
  align: (left, center, center, left, left),
  stroke: 0.4pt + rgb("#cbd5e1"),
  inset: 4pt,
  ..thead(
    rgb("#f1f5f9"),
    strong([Code]), strong([Message]), strong([Direction]), strong([Status]), strong([Payload]),
  ),
  [#hexcode[0x01]], [HANDSHAKE_REQUEST], [peer → master], [done (M0)], [node name (UTF-8)],
  [#hexcode[0x02]], [HANDSHAKE_RESPONSE], [master → peer], [done (M0)], [node name (UTF-8)],
  [#hexcode[0x10]], [PING], [any → any], [planned], [—],
  [#hexcode[0x11]], [PONG], [any → any], [planned], [—],
  [#hexcode[0x20]], [MANIFEST_REQUEST], [peer → master], [done (M1)], [—],
  [#hexcode[0x21]], [MANIFEST_RESPONSE], [master → peer], [done (M1)], [m3u8 contents],
  [#hexcode[0x30]], [SEGMENT_REQUEST], [any → any], [spec'd (M2)], [filename (UTF-8)],
  [#hexcode[0x31]], [SEGMENT_CONTENTS], [any → any], [spec'd (M2)], [file chunk ≤ 1400 B],
  [#hexcode[0x32]], [SEGMENT_NOT_FOUND], [any → any], [spec'd (M2)], [—],
  [#hexcode[0x40]], [ACK], [any → any], [spec'd (M2)], [next range (start, count)],
  [#hexcode[0x50]], [PEERLIST_REQUEST], [peer → master], [planned], [—],
  [#hexcode[0x51]], [PEERLIST_RESPONSE], [master → peer], [planned], [packed ip:port entries],
)

= Manifest request (happy path)

The peer polls the master for the current HLS playlist. Polling is
#emph[stateless]: every request is answered with the latest manifest, there
is no session.

== Messages

#table(
  columns: (auto, 1fr),
  align: (left, left),
  stroke: 0.4pt + rgb("#cbd5e1"),
  inset: 4pt,
  ..thead(rgb("#f1f5f9"), strong([Message]), strong([Format])),
  [MANIFEST_REQUEST], [
    #mono[51 53 54] (magic) · #mono[02] (version) · #mono[20] (type) ·
    #mono[00] (flags) · #mono[00 00] (data length) ·
    #mono[00 00] (transfer id) · #mono[00 00] (packet \#) · #mono[00 00] (total)
    — 14 bytes, no payload.
  ],
  [MANIFEST_RESPONSE], [
    Same header with type #mono[21] and `data length` = manifest size;
    payload = raw m3u8 bytes. Example for the playlist
    #mono[#text("#EXTM3U\\n")] (8 bytes):
    #mono[51 53 54 02 21 00 00 08 00 00 00 00 00 00] then
    #mono[23 45 58 54 4D 33 55 0A].
  ],
)

== Exchange

#seqdiagram(
  (("peer", 0), ("master", 1)),
  (
    (0, 1, [MANIFEST_REQUEST], "req"),
    (1, 0, [MANIFEST_RESPONSE  ·  payload = m3u8], "resp"),
  ),
)

#v(6pt)
1. The peer sends #mono[MANIFEST_REQUEST] (no payload, no transfer id).
2. The master re-reads its playlist file from disk and replies
   #mono[MANIFEST_RESPONSE] with the raw contents.
3. The peer writes the response atomically (tmp + rename) to
   #mono[\<data-dir\>/live.m3u8].

#emph[Failure handling.] If the master cannot read the manifest it replies
with an empty payload (the peer keeps its previous copy). A timeout is
retried on the next poll — the peer polls every 3 s by default.

= Piece (segment) request (happy path)

A piece is one `.ts` segment file, transferred as a sequence of packets.
The transfer is #strong[receiver-driven]: the peer explicitly requests the
next packet range with every ACK, so there is no sender/receiver window
synchronization to lose (see `SPEC.md` §7). `INITIAL_WINDOW = 5`,
`MAX_WINDOW = 64`.

== Messages

#table(
  columns: (auto, 1fr),
  align: (left, left),
  stroke: 0.4pt + rgb("#cbd5e1"),
  inset: 4pt,
  ..thead(rgb("#f1f5f9"), strong([Message]), strong([Format])),
  [SEGMENT_REQUEST], [
    Type #mono[30]; `transfer id` = fresh random value; payload = filename.
    Example requesting #mono[seg_0042.ts] with transfer id #mono[0x01A7]:
    #mono[51 53 54 02 30 00 00 0B 01 A7 00 00 00 00]
    then #mono[73 65 67 5F 30 30 34 32 2E 74 73].
  ],
  [SEGMENT_CONTENTS], [
    Type #mono[31]; echoes `transfer id`; `packet number` = 1-based index;
    `total packets` = #mono[N]; payload = chunk of the file, ≤ 1400 bytes.
    First packet of a 60-packet file: #mono[51 53 54 02 31 00 05 78 01 A7 00 01 00 3C]
    followed by 1400 bytes of file data.
  ],
  [SEGMENT_NOT_FOUND], [
    Type #mono[32]; echoes `transfer id`; no payload. The master does not
    have the requested file.
  ],
  [ACK], [
    Type #mono[40]; echoes `transfer id`; payload = next range
    #mono[(start, count)] as two big-endian u16 (4 bytes). #mono[start] is
    the first packet wanted, #mono[count] how many. Example: "send me
    packets 6..10" → #mono[00 06 00 05].
    With #mono[flags = 0x04] and an empty payload the transfer is complete.
  ],
)

== Exchange (happy path, window growth 5 → 10 → 20)

#seqdiagram(
  (("peer", 0), ("master", 1)),
  (
    (0, 1, [SEGMENT_REQUEST  ·  seg_0042.ts, id = T], "req"),
    (1, 0, [SEGMENT_CONTENTS  ·  packets 1–5, total = N], "data"),
    (0, 1, [ACK  ·  next range (6, 10)], "ack"),
    (1, 0, [SEGMENT_CONTENTS  ·  packets 6–10], "data"),
    (0, 1, [ACK  ·  next range (11, 20)], "ack"),
    (1, 0, [SEGMENT_CONTENTS  ·  packets 11–20], "data"),
    (0, 1, [ACK  ·  next range (21, 40)], "ack"),
    (1, 0, [SEGMENT_CONTENTS  ·  …], "data"),
    (0, 1, [ACK  ·  COMPLETE  ·  file assembled], "ack"),
  ),
)

#v(6pt)
1. The peer sends #mono[SEGMENT_REQUEST] for #mono[seg_0042.ts] with a fresh
   #emph[transfer id] #mono[T].
2. The master reads the file, computes #mono[N = max(1, ceil(size/1400))]
   packets, and sends the first window: packets #mono[1..5].
3. The peer learns #mono[N] from the first content packet and replies
   #mono[ACK (6, 10)]: the next window, doubled.
4. Windows double on every fully-received range (#mono[5 → 10 → 20 → 40 → …])
   up to #mono[MAX_WINDOW = 64].
5. As soon as the peer has all #mono[N] packets it reassembles the file
   (out-of-order safe: packets are placed by their `packet number`), writes
   it atomically, and sends #mono[ACK] with the #strong[COMPLETE] flag.
6. The master frees the transfer state.

#emph[Loss handling] (short version; full rules in `SPEC.md` §7): if the
peer's current range does not fill within the quiet period, it re-sends the
same #mono[ACK] (retransmit request). If the master's ack timer fires it
re-sends its last range — the peer deduplicates, so blind re-sends are safe.
Both sides give up after #mono[8] retries.

= Appendix: transfer settings

#table(
  columns: (auto, auto, 1fr),
  align: (left, right, left),
  stroke: 0.4pt + rgb("#cbd5e1"),
  inset: 4pt,
  ..thead(rgb("#f1f5f9"), strong([Setting]), strong([Value]), strong([Meaning])),
  [#mono[SEGMENT_PACKET_SIZE]], [1400], [bytes per content packet],
  [#mono[INITIAL_WINDOW]], [5], [first requested range size],
  [#mono[MAX_WINDOW]], [64], [largest range size],
  [#mono[PACE_INTERVAL_MS]], [1], [sleep between packets (rate limiter)],
  [#mono[WINDOW_QUIET_MS]], [150], [receiver quiet period before ACKing],
  [#mono[FIRST_RESPONSE_TIMEOUT_MS]], [2000], [give up if nothing arrives],
  [#mono[WINDOW_RETRY_LIMIT]], [8], [receiver re-request limit],
  [#mono[ACK_RETRY_LIMIT]], [8], [sender resend limit],
  [#mono[MAX_CONCURRENT_TRANSFERS]], [16], [per-node transfer registry bound],
)
