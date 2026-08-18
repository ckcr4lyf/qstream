# qstream — DEVLOG (milestone N: NAT traversal)

Running log of problems observed and how they were fixed, milestone N
(hole punching, UPnP, same-LAN). Newest at the bottom.

---

## N1-N4 implementation (2025-08-15)

**What was built (protocol v3):**

- Handshake payloads now carry endpoints: request = claimed UPnP mapping
  (6 B) + name; response = observed public endpoint (6 B) + name
  (in-band STUN — the peer learns its public endpoint from the responder).
- Peerlist entries grew from 6 to 7 bytes: ip + port + flags
  (`PEER_UPNP_MAPPED` when a peer's claimed mapping matches what we
  observe; `PEER_SAME_IP` when it shares the requester's public IP).
- New messages `PING 0x60` (payload = name) and `PONG 0x61` (empty).
- Version bumped 0x02 → 0x03; old binaries reject cleanly.

**N2 — PING/PONG is one mechanism, three jobs:**

- keep-alive (every peer PINGs every peer every 10 s; keeps NAT mappings
  alive between bursty segment transfers),
- punch (the PING is the simultaneous-open packet; when both sides ping,
  both NATs' mappings are open to each other, so SEGMENT_REQUESTs get
  through — this is what makes cone↔cone and symmetric-sender cases work),
- liveness/connectivity check (a fresh PONG = direct path works;
  `pick_peer` weights fresh paths 1.25× vs 0.7× stale).

**N3 — same-LAN:**

- Peers broadcast a PING to 255.255.255.255:<port> every 5 s (LAN beacon);
  receiving peers register the sender under its *private* address (marked
  `lan`), so transfers stay on the LAN instead of hairpinning the NAT.
- `effective_addr()` resolves a peer to its LAN address when one is known;
  `PEER_SAME_IP` peerlist entries skip the pointless hairpin handshake.

**N4 — UPnP-IGD:**

- `upnp.rs`: SSDP M-SEARCH → device description → WANIPConnection control
  URL → GetExternalIPAddress + AddPortMapping. std-only (SOAP over a plain
  TcpStream, XML extracted with tag scans). Opportunistic: ~1 s to fail
  when no IGD exists; `QSTREAM_NO_UPNP=1` disables.
- A peer with a mapping claims it in handshakes; the master verifies
  claimed == observed and sets `PEER_UPNP_MAPPED`, so other peers know it
  is directly reachable.

**Problems seen during implementation:**

1. Wire-format test vectors (segment/ack) still carried version 0x02 after
   the bump — 4 tests failed. Fixed the vectors to 0x03.
2. My handshake wire-format vector declared data length 0x0E; actual is
   0x0C (6 B endpoint + 6 B name). Fixed.
3. `upnp.rs` initially missed `use crate::log` and passed `String` where
   `&str` was needed (two compile errors). Fixed.
4. The `Event::PeerlistResponse` type changed to `Vec<(SocketAddr, u8)>`
   but node.rs's Event enum wasn't updated — type error caught at compile.

## LAN beacon self-discovery bug (2025-08-15) — FIXED

**Symptom:** the loopback lab showed `discovered peer peer-1 at
45.87.251.232:4444 (ping)` — a peer registered ITSELF as a peer at the
machine's public IP.

**Cause:** the kernel delivers a node's own UDP broadcast back to its own
socket (loopback of broadcast on the host's interface). The self-guard
compared `src == local_addr` (0.0.0.0:4444) which never matches the echo's
source (machine's external IP).

**Fix:** PING payload now carries a random per-node nonce (4 B) before the
name; a node ignores any PING whose nonce equals its own — own echoes are
recognized regardless of which IP they arrive from. Also avoids
self-registration polluting the peers map and `pick_peer`.

**Verified:** no more self-discovery in the lab; 0 failures; path=fresh on
/peers.

## NAT lab bring-up (2025-08-15) — four real bugs, all fixed

The lab emulates home NATs with netns + iptables: each "home" is a netns
with its own SNAT (symmetric-style, port-preserving) or DNAT (full cone).
Master sits on the host at a public-ish address (10.99.0.1).

1. **User-defined iptables chains fail on nf_tables** ("Invalid argument"
   on the jump rule) — switched to the built-in chains.
2. **teardown can't delete a netns with running processes** — devices
   linger and the next setup breaks ("File exists" cascades). Fixed:
   teardown kills qstream first.
3. **SNAT must live inside the netns, not on the host**: packets destined
   to a LOCAL process (the master) skip the host's POSTROUTING, so host
   SNAT never applied to master-bound traffic. Moved NAT into each home
   netns (more realistic anyway).
4. **`multiple -d flags not allowed`** in iptables v1.8.7 — the SNAT
   exclusion for broadcasts failed silently, so (a) the master saw private
   addresses and (b) beacons got NATed. Fixed with a RETURN rule for
   255.255.255.255 before the SNAT rule.
5. **ARP blackholes replies to NAT pseudo-addresses**: routes
   `10.99.0.x/32 dev veth` are scope-link and need ARP; no one answers
   ARP for a NAT virtual IP, so every reply from the master vanished.
   Fixed with `ip neigh ... nud permanent` (static neighbor to the home's
   veth MAC) + pinned route src 10.99.0.1 (conntrack reverse-translation
   requires replies to come from the address the peer sent to).
6. **The host's pre-existing MASQUERADE (WAN-only, oifname ens3) is
   harmless** — left untouched.

With those fixed, the whole §3 matrix passes in the lab:

- NATed peer pulls from master: works (receiver-driven; request opens the
  path, sender replies to src).
- Same-LAN peers (home1↔home2, one L2 domain): LAN beacon discovers the
  private address (10.0.0.x) and transfers stay direct — no NAT hop.
- Cross-NAT peers (home1↔cone1, different L2): transfers flow via public
  endpoints through both NATs; per-destination SNAT ports are re-keyed on
  first PING contact (the re-key fix), e.g. cone1 served home1 from
  10.99.0.2:43358 — a port the master never saw.
- 0 timeouts / 0 incomplete across all three homes; only trial NOT_FOUND
  churn. Handshakes complete, all paths fresh.

## N4 verification — fake IGD (2025-08-15)

`scripts/fake_igd.py`: an SSDP responder + device-description HTTP server +
SOAP control endpoint (GetExternalIPAddress / AddPortMapping), logging
every action. Verified end-to-end against `upnp::try_map`:

- SSDP M-SEARCH → LOCATION → description → WANIPConnection controlURL
  extraction → SOAP calls: all work, std-only.
- A peer with the fake mapping logs `UPnP mapping claimed: <external>:<port>`
  and sends it in handshakes.
- Master verification: when claimed == observed (fake IGD pointed at
  127.0.0.1), the peerlist entry carries `flags=0x03`
  (UPNP_MAPPED | SAME_IP) — decoded live with a raw peerlist request.
- The fake IGD is a reusable lab tool for CI.

## Notes / limitations found

- The LAN beacon only reaches peers bound to the SAME port on the same
  broadcast domain (one qstream per host, same port across hosts — the
  normal deployment). Same-host peers with different ports don't hear each
  other's beacons; the master's peerlist covers discovery there.
- home1's SNAT (kernel) is port-preserving; per-destination port
  allocation (true symmetric NAT) is NOT emulated by iptables — the lab
  proves cone-ish behavior. True symmetric x symmetric remains relay-only
  (documented in NAT.md; relay intentionally not built).
- The host's pre-existing MASQUERADE (VPS) was WAN-only and harmless, but
  teaching the lab this lesson took a while: check for ambient NAT rules
  before blaming your own setup.

## Remote peer vs master's loopback swarm (2025-08-15) — FIXED

**Symptom:** the home peer (real public IP, UPnP-mapped) logged endless
`no handshake reply from 127.0.0.1:4445-4448 — skipping`.

**Cause:** the master advertises each peer at the endpoint IT observes. In
the lab that's 127.0.0.1 — meaningless to a remote peer (its own loopback).
The requester wasted handshakes into the void.

**Fix (both sides):** `remote_public()` — an endpoint is globally reachable
iff not loopback and not RFC1918/link-local. The master's peerlist reply
drops loopback/private entries for remote (public) requesters; the requester
skips such entries when its own observed endpoint is public. The loopback
lab and same-LAN cases are unaffected (loopback/private requester = no
filtering).

**Unexpected find:** despite the noise, the remote peer still got served —
the loopback peers initiated handshakes toward it (via its UPnP endpoint),
and it registered them at the host's PUBLIC IP:port (45.87.251.232:4448),
which is locally delivered — so cross-topology transfers worked anyway.

## Remote peer playback: 404 storm + catch-up lag (2025-08-15) — FIXED

**Symptom (home peer):** mpv fetched the playlist and immediately requested
the NEWEST segments, which the peer hadn't downloaded yet -> 404 per
segment; the peer itself was ~19 segments behind the live edge.

**Root causes:**
1. `sync_queue` enqueued playlist segments OLDEST first — the peer raced
   toward the edge only after fetching history, and it never quite got
   there because...
2. Segment transfers are RTT-bound: the receiver-driven windows take
   ~log2(packets) round trips (~7 for a 250-packet segment), and at the
   home peer's ~200-500ms RTT that's ~1.6s per segment — barely above the
   stream rate, so the gap never closed. (Pipelined windows are the real
   fix; backlog.)
3. The peer's HTTP served the manifest unfiltered — segments listed but
   not yet on disk -> player 404s.

**Fixes:**
- `http.rs`: serve playlists filtered to segments that exist on disk,
  EXT-X-MEDIA-SEQUENCE advanced to the first kept segment (3 unit tests).
  Players can never request a missing segment; they track the available
  tail instead. Master unaffected (has everything).
- `peer.rs`: `sync_queue` now enqueues NEWEST first — the peer races to
  the edge; old segments (which roll off anyway) are fetched opportunistically.

**Also observed:** the home peer's peerlist showed `192.168.128.1:4447` —
a LAN-beacon discovery of another qstream instance on the user's home
network; benign (trial NOT_FOUND churn).

## Live-edge scheduling and availability hints (2026-08-17) — VERIFIED

**Problem:** a remote peer can discover a home peer that is several pieces
behind the master. Trial-based selection then wastes requests on the home
peer for fresh playlist entries that only the master can reliably serve.

**Changes (protocol v3 compatible):**

- The newest three manifest entries are master-only. Peers are still selected
  for older backfill, where sharing has a chance to help.
- `PONG` and `SEGMENT_NOT_FOUND` may now carry an optional 10-byte inventory:
  `u64 newest segment number + u16 newest-first availability mask`. It covers
  the most recent 16 pieces; old nodes send/accept the empty legacy payload.
- An inventory expires after 15 seconds. A peer is skipped only when its fresh
  inventory explicitly says it lacks a requested older piece; unknown and
  out-of-window pieces remain trial candidates.

**Local baseline:** after restart, peer-1 made 13 live pulls in 25 seconds,
all from the master, with 0 `SEGMENT_NOT_FOUND` retries and 0 protocol decode
errors.

**Cross-VPS verification:** VPS-2 (`140.238.230.56`, peer UDP 4445) joined the
master swarm while the home peer (`183.178.210.60:4450`) was lagging. Initial
backfill used the home peer for 4 older pieces. In the following 103 seconds,
VPS-2 completed 57 master pulls and 0 failures; it made 0 NOT_FOUND pulls to
the home peer. HTTP `GET /playback.m3u8` returned 200. The master recorded 54
successful segment serves to VPS-2 over the same interval.

**Remaining limitation:** segment downloads over the cross-VPS path are about
250-315 KB/s and roughly 0.9-1.1 s per segment. The edge policy prevents bad
peer trials, but pipelined windows remain the main throughput improvement.

**Follow-up:** inventory is now announced immediately after a completed peer
download, rather than only piggybacked on the 10-second PING/PONG cadence.
`/peers` displays a fresh inventory as `newest=<n> mask=<hex>` for inspection.
In a second cross-VPS run, VPS-2 saw the master's live inventory
`newest=134505 mask=07ff`, completed 33 pulls with 0 failures in 66 seconds,
and served `playback.m3u8` with HTTP 200. The legacy home peer exposes no
inventory, as expected; it remains compatible but is not selected for the
three-segment live edge.

## Origin load correction (2026-08-17) — SUPERSEDES EDGE RULE

The fixed master-only rule for the newest three segments was not scalable: it
made every peer fetch at least those three pieces from the origin, even when
another peer already had them. With 10,000 peers this creates 30,000 origin
segment transfers per rolling edge window.

The scheduler now uses availability-first selection for every manifest entry:

- a non-master peer with a fresh inventory bit confirming the requested piece
  is selected without competing with the master;
- the master remains an authoritative fallback when no non-master peer can
  prove possession;
- unknown inventories remain eligible as low-confidence fallback candidates;
- fresh inventories explicitly denying a piece are still filtered out.

This preserves recovery when the swarm has no known copy while allowing the
swarm to fan out new segments from peers instead of forcing all live-edge
traffic through the master. A future 10,000-peer deployment still needs
bounded peer discovery and neighbor fanout; source selection alone does not
make a full-mesh peer list scalable.

## Stale availability correction (2026-08-18) — VERIFIED IN PROGRESS

After adding a second upgraded peer, VPS-2 successfully began pulling pieces
from `183.178.210.60:4447`, but a few requests still raced against stale
positive inventory while that peer's retained files changed. The requester
then retried the same piece against the master, creating avoidable duplicate
work.

The scheduler now records exact `SEGMENT_NOT_FOUND` answers per peer and piece
for 15 seconds. A later positive inventory bit clears that negative entry.
Source selection is also tiered: known non-master copies first, a fresh
positive master copy second, and unknown candidates only when the master has
no fresh answer. This removes routine unknown-peer `NOT_FOUND` trials from the
live window while preserving origin recovery.

Retention pruning now immediately announces a fresh inventory when files are
removed. This closes the stale-positive window without increasing the normal
announcement cadence.

## Per-peer transfer accounting (2026-08-18)

`PeerStat` now tracks directional payload bytes in addition to segment counts:
`downloaded_bytes` is completed payload received from that peer, and
`uploaded_bytes` is completed payload sent to that peer. Upload bytes are
recorded only after `ACK_COMPLETE`, so failed or abandoned sends are excluded.
The fields are exposed in `/peers`, `/stats`, and Prometheus metrics as
`qstream_peer_downloaded_bytes_total` and
`qstream_peer_uploaded_bytes_total`.

## Bounded parent assignments (2026-08-18) — STAGE 1

The previous scheduler knew which peers had pieces but did not coordinate
replication. The master now returns at most 16 reachable peers in each
peerlist response and marks up to two low-load peers with the additive
`PEER_PARENT` flag. Peers retain only the current assignment and prefer a
parent when its fresh inventory confirms the requested segment; the master
remains the fallback when parents are not ready. The existing 7-byte peerlist
format and protocol version remain unchanged, so legacy peers can ignore the
new flag.

A five-second parent wait is also applied for known assigned parents. If none
has a fresh positive inventory for a queued segment, the peer waits briefly
for parent replication before using the master recovery path. This is bounded
so a stalled parent cannot hold playback indefinitely.

The first live run also exposed a peer identity bug: two independent peers
using the default display name `peer` were re-keyed over each other when their
PINGs arrived. Peer names are not identities, so public socket endpoints are
now kept independently. This is required for stable parent assignments and
inventory tracking when users leave the default name unchanged.
