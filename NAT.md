# qstream — NAT traversal (milestone N)

Date: 2025-08-15 · Status: N1-N4 implemented and lab-verified (see DEVLOG); N6 remains
Scope: keep the swarm working when most peers are ordinary home clients
behind NAT. At least the master is publicly reachable (VPS).

This document covers the implemented connectivity ladder and the remaining
relay design. The current binary implements endpoint observation, PING/PONG
keep-alives and punching, same-LAN beacons, and opportunistic UPnP-IGD
mapping. It does **not** implement relay forwarding.

---

## 1. The problem

A home client is behind a NAT box: inbound UDP is blocked unless the client
opened the path first. The implemented path maintenance handles common cone
and restricted NATs, but not every NAT type; a relay is still needed for
symmetric-NAT pairs with no stable endpoint.

The good news: **our transfers are receiver-driven** — the receiver sends
`SEGMENT_REQUEST` and the sender only replies. That means the *receiver's*
NAT is never a problem: its outbound request creates the mapping its data
rides back on (works even for symmetric NAT, because the sender replies to
the request's source port). The hard part is only: **can the sender receive
the request?**

## 2. NAT background (short)

| Type | Behavior | Home-relevant? |
|---|---|---|
| Full cone | one mapping, anyone can send in | rare (legacy) |
| Address-restricted cone | inbound only from IPs the client has sent to | common |
| Port-restricted cone | inbound only from exact IP:port the client sent to | most common |
| Symmetric | fresh mapping per destination | mobile/cellular, some ISPs, CGNAT |

- NAT mappings expire — RFC 4787 says ≥ 2 min is the *recommendation*; real
  NATs commonly drop UDP mappings in 30 s-5 min. Keep-alives required.
- **CGNAT** (carrier-grade NAT, mobile/data links): often symmetric, never
  has UPnP, can't be punched reliably. **Relay is the only guarantee.**
- References: RFC 4787 (NAT UDP behavior), RFC 5128 (P2P across NATs),
  RFC 6887 (PCP), UPnP IGD v1.0/v2.0 specs, libp2p hole-punching docs.

## 3. Connectivity ladder

Every peer establishes the best path it can, in order of preference:

```
1. same-LAN direct      (peer behind the same NAT / same broadcast domain)
2. direct punched       (both peers' NATs allow a direct path)
3. relay                (master or any publicly-reachable peer forwards)
```

UPnP is not a path — it's a promotion: a peer with a working port mapping
behaves like a public peer, so it lands on tier 2 (and can act as a tier-3
relay for others).

### Scenario matrix (who is where → how it works)

| Receiver | Sender | Mechanism |
|---|---|---|
| public | public | direct (today) |
| NATed (any) | public/UPnP | **receiver-driven request just works** — receiver's outbound request opens its NAT; sender replies to request src |
| NATed | NATed, both cone | sender punches: sends PING to receiver's public endpoint so its NAT accepts the receiver's request; receiver-driven pull then works |
| NATed | NATed, restricted on both sides | simultaneous punch (both sides PING each other's public endpoints around the same time); retries for a few seconds |
| NATed | symmetric | **relay** (one-way punch may work if receiver is full cone; not reliable) |
| both symmetric | | **relay** |
| same NAT | same NAT | LAN direct (private endpoints, §5.4) |

Key consequence of the design: **the receiver never needs punching or a
mapping of its own.** Only the sender side must be reachable-or-punched.
The manifest/peerlist polls to the master keep the master-mapping alive for
free (every 2-5 s).

## 4. Components

### 4.1 Endpoint discovery & reachability (in-band, no STUN)

- The master sees every peer's **observed public endpoint** (the source
  address of its packets) and returns it in the handshake response. This is
  an in-band STUN-like mapped address.
- The handshake request carries a 6-byte claimed endpoint from UPnP, or
  `0.0.0.0:0` when there is no mapping. The master cross-checks the claim
  against the observed source and marks matching entries with
  `PEER_UPNP_MAPPED`.
- Peerlist entries carry a 1-byte flags field: `PEER_UPNP_MAPPED`,
  `PEER_SAME_IP`, and additive `PEER_PARENT`. Reachability freshness is
  tracked locally from PONGs rather than encoded as a separate peerlist
  capability.
- Peers learn their observed endpoint from the handshake response.

### 4.2 PING / PONG — one mechanism, three jobs

New tiny messages (0x60/0x61). A peer sends PING to another peer's public
endpoint; PONG replies. This serves as:

1. **Connectivity check / punch** — the PING is the "simultaneous open"
   packet; a PONG proves the direct path works.
2. **Keep-alive** — PING every ~10-15 s keeps both NATs' mappings alive
   between segment transfers (which are bursty: ~1.5 s of traffic every
   2 s, then silence).
3. **Liveness** — replaces the unresponsive-timeout heuristic for peers we
   haven't transferred with recently.

The implementation sends PINGs every 10 seconds to known peers. This keeps
mappings alive and provides the simultaneous-open packet used for direct
punching; a received PONG marks the path fresh for 30 seconds. Both sides
maintain this behavior independently, so no extra master coordination message
is required.

### 4.3 Receiver-driven pulls (already our design — keep it)

The request opens the receiver's NAT; the sender replies to the request's
source address. Two consequences to preserve:

- Senders must reply to the *request's src*, not to a stored peer address
  (they already do — `SEGMENT_REQUEST` handling uses `src`).
- The sender should PING the receiver before/while serving a first request
  from it, to make sure *its own* NAT accepts the request stream.

### 4.4 Same-LAN shortcut

- The master marks peerlist entries with `PEER_SAME_IP` when observed IPv4
  endpoints share an address.
- Peers also broadcast a PING with a per-node nonce to
  `255.255.255.255:<local-port>` every five seconds. A peer receiving a
  private-source beacon registers that endpoint as a LAN path and prefers it
  over a public hairpin path. The nonce prevents self-discovery.

### 4.5 UPnP-IGD port mapping (std-only)

A peer with UPnP enabled on its router can request a stable public mapping
and promote itself to tier 2 (and to relay-capable). std-only sequence:

1. **SSDP**: UDP `M-SEARCH` to `239.255.255.250:1900`,
   `ST: urn:schemas-upnp-org:device:InternetGatewayDevice:1` → response has
   `LOCATION` (device description URL).
2. **Description**: plain HTTP GET, extract `WANIPConnection` (fall back to
   `WANPPPConnection`) service's `controlURL`.
3. **SOAP**: HTTP POST `AddPortMapping` (`NewExternalPort`, `NewProtocol=UDP`,
   `NewInternalPort`, `NewInternalClient`, `NewEnabled=1`,
   `NewLeaseDuration=0`).

XML is extracted with tiny substring scans over the fixed response shapes —
no XML parser, std-only. All of this is ~200 lines. Realities: many routers
ship with UPnP **disabled** now (it's a known attack surface), and CGNAT has
no UPnP. So: try at startup + on demand, log success/failure, never depend
on it. (PCP RFC 6887 / NAT-PMP are simpler UDP protocols — bonus if trivial,
but router support is poor; skip unless needed.)

### 4.6 Relay (the guarantee)

New `RELAY_DATA` envelope (0x63): `{target ip:port (6 B), inner datagram}`.
Any node with a stable public endpoint (master, or a promoted peer) can
relay. Mechanics (master as example):

- NATed receiver R wants a segment from sender S (symmetric S or deadlock):
  R sends `RELAY{target=S, inner=SEGMENT_REQUEST}` to the master.
- Master forwards the inner request to S; the request's src is the master,
  so S's sender is created with `remote = master`. S replies normally.
- Master tracks `transfer_id → R` (learned from R's relayed requests) and
  forwards S's packets carrying that transfer id back to R. ACKs from R
  flow back the same way (`RELAY{target=S, inner=ACK}`).
- R sees everything from the master's address — its receiver accepts
  content by transfer id regardless of src (already true today).

Cost: one relayed segment = ~2× its bytes on the relay's link. For HLS
(~300 KB/2 s per peer) one VPS relay comfortably serves many peers. Peers
with mappings can relay for others, spreading the load.

HTTP playback: remote viewers always use the master's HTTP (public). Local
viewers hit peers directly on the LAN. A NATed peer's HTTP server stays
LAN-only unless the user maps it (out of scope; TCP mappings via UPnP are
a later nicety).

## 5. Protocol v3 changes

Implemented protocol v3 changes:

- Version byte `0x02` → `0x03`; old versions are rejected cleanly.
- Handshake request/response payloads are 6-byte endpoint + UTF-8 display
  name. Requests carry the claimed endpoint; responses carry the observed
  endpoint.
- Peerlist entries are 7 bytes: IPv4 address, port, and flags
  (`PEER_UPNP_MAPPED`, `PEER_SAME_IP`, or `PEER_PARENT`).
- `PING 0x60` carries a 4-byte nonce plus display name; `PONG 0x61` is empty
  or carries the optional 10-byte recent inventory.
- `SEGMENT_NOT_FOUND` can carry the same inventory and the additive
  `SEGMENT_NOT_READY` flag for temporary origin admission denials.

The proposed `RELAY_DATA 0x63` envelope remains unimplemented and is not part
of the current message catalog.

## 6. Milestones

| # | What | Status |
|---|---|---|
| N1 | Endpoint observation + handshake/peerlist v3 (public endpoint, flags) | ✅ lab-verified |
| N2 | PING/PONG: keep-alives, connectivity checks, punch; direct path cone↔cone | ✅ lab-verified (cross-NAT transfers) |
| N3 | Same-LAN: LAN beacon (broadcast PING) + private-address paths | ✅ lab-verified (home1↔home2 direct) |
| N4 | UPnP-IGD mapping (std-only SSDP/SOAP/mini-XML), verified via fake IGD | ✅ lab-verified (flags=0x03) |
| N5 | ~~same_public_ip private-port tries~~ → replaced by the LAN beacon | ✅ |
| N6 | VPS live test: master on one VPS, peers on others + a NATed home client | ⏳ next |

## 7. Testing without real routers: `scripts/natlab.sh` (implemented)

A small userspace NAT emulator is the lab tool: bind one UDP socket per
"home", translate (private ip:port → nat ip:port) and enforce per-type
rules. Type is a flag:

- `cone` — single mapping per private endpoint; inbound allowed per cone
  rules (full/restricted/port-restricted selectable).
- `symmetric` — per-destination mapping; inbound only on exact mappings.

Layout in the lab: `nat_sim.py` runs one instance per "home", peers bind
inside a network namespace or simply talk to the simulator as their
"gateway". Each sim instance exposes the two faces on real ports:
private-side listener (peers' sockets) and public-side listener (the
"internet", where master + other NATs live). This emulates the full NAT
taxonomy deterministically, so every row of the §3 matrix is testable in
CI. (Real routers/VPSes come later; the user has VPSes for N6.)

## 8. Risks & open questions

- **Symmetric NAT without any stable endpoint → relay only.** Accepted:
  relay is the guarantee; CGNAT users *will* relay (or consume from the
  master's HTTP instead).
- **Relay bandwidth**: master relays 2× per segment. Mitigations: peers
  with mappings relay too; viewers can consume via master HTTP (no UDP).
- **Private-IP guessing** for same-LAN: unreliable on multi-NIC hosts; the
  LAN beacon (broadcast) is the robust answer — likely promoted to N5.
- **transfer-id routing on relays** (16-bit): collision risk when many
  relayed transfers share a sender; low probability, and keying on
  (sender, transfer id) bounds it. Revisit if relays get busy.
- **UPnP on modern routers**: frequently disabled; treat as opportunistic.
- **Hairpin NAT** (peer reaches own public IP): avoided by same-LAN path.

## 9. What stays the same

- Single socket per node, receiver-driven windows, adaptive timers, fault
  injection, ranking — all untouched.
- Master stays rendezvous + authoritative manifest source; HTTP for remote
  playback.
- std-only, one binary.
