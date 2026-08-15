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
