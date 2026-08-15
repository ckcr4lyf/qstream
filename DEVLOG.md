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
