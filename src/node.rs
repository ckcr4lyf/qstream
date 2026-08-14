//! Shared node core (SPEC.md §7.5): one socket, message dispatch, transfer
//! registry, peer registry. Used by both master and peer.

use std::collections::HashMap;
use std::fs;
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::log;
use crate::protocol::{self, Message};
use crate::transfer::TransferRegistry;

/// Events a caller (the peer) may want to react to.
#[derive(Debug)]
pub enum Event {
    /// A HANDSHAKE_RESPONSE arrived from `src` (we sent a handshake request).
    HandshakeResponse { src: SocketAddr, name: String },
    /// A MANIFEST_RESPONSE arrived with the raw m3u8 bytes.
    ManifestResponse { data: Vec<u8> },
    /// A PEERLIST_RESPONSE arrived with discovered peers.
    PeerlistResponse { peers: Vec<SocketAddr> },
    /// Nothing the caller needs to act on.
    None,
}

/// A known peer and when we last heard from it.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub name: String,
    pub last_seen: Instant,
}

pub struct Node {
    pub socket: UdpSocket,
    pub name: String,
    pub manifest_path: PathBuf,
    pub registry: TransferRegistry,
    /// Peers we know about (handshaked or discovered), keyed by address.
    pub peers: HashMap<SocketAddr, PeerInfo>,
}

impl Node {
    pub fn new(socket: UdpSocket, name: String, manifest_path: PathBuf, segment_root: PathBuf) -> Node {
        Node {
            socket,
            name,
            manifest_path,
            registry: TransferRegistry::new(segment_root),
            peers: HashMap::new(),
        }
    }

    pub fn register_peer(&mut self, addr: SocketAddr, name: String) {
        let entry = self.peers.entry(addr).or_insert(PeerInfo {
            name: name.clone(),
            last_seen: Instant::now(),
        });
        entry.last_seen = Instant::now();
        if entry.name != name {
            log::debug(&format!("peer {addr} renamed to {name}"));
            entry.name = name;
        }
    }

    fn touch_peer(&mut self, addr: SocketAddr) {
        if let Some(info) = self.peers.get_mut(&addr) {
            info.last_seen = Instant::now();
        }
    }

    /// Drop peers we haven't heard from in `ttl`.
    pub fn prune_peers(&mut self, ttl: Duration, now: Instant) {
        let stale: Vec<SocketAddr> = self
            .peers
            .iter()
            .filter(|(_, info)| now.duration_since(info.last_seen) >= ttl)
            .map(|(addr, _)| *addr)
            .collect();
        for addr in stale {
            log::debug(&format!("evicting idle peer {addr}"));
            self.peers.remove(&addr);
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.registry.next_deadline()
    }

    pub fn tick(&mut self, now: Instant) {
        self.registry.tick(&self.socket, now);
    }

    /// Handle one incoming datagram. Returns an event for the caller.
    pub fn handle(&mut self, datagram: &[u8], src: SocketAddr) -> Event {
        let message = match protocol::decode(datagram) {
            Ok(m) => m,
            Err(e) => {
                log::warn(&format!("dropping malformed datagram from {src}: {e}"));
                return Event::None;
            }
        };

        // Any traffic from a known peer refreshes its liveness.
        self.touch_peer(src);

        match message {
            Message::HandshakeRequest { name } => {
                log::info(&format!("handshake from {src} (name: {name})"));
                self.register_peer(src, name);
                let reply = Message::HandshakeResponse {
                    name: self.name.clone(),
                };
                let _ = self.socket.send_to(&protocol::encode(&reply), src);
                Event::None
            }
            Message::HandshakeResponse { name } => {
                log::debug(&format!("handshake response from {src} (name: {name})"));
                Event::HandshakeResponse { src, name }
            }
            Message::ManifestRequest => {
                // Re-read from disk every time — the live playlist rolls.
                let data = fs::read(&self.manifest_path).unwrap_or_default();
                let reply = Message::ManifestResponse { data };
                let _ = self.socket.send_to(&protocol::encode(&reply), src);
                Event::None
            }
            Message::ManifestResponse { data } => {
                log::debug(&format!("manifest response ({} bytes) from {src}", data.len()));
                Event::ManifestResponse { data }
            }
            Message::PeerlistRequest => {
                // Reply with our view, excluding the requester.
                let peers: Vec<SocketAddr> = self
                    .peers
                    .keys()
                    .filter(|p| **p != src)
                    .cloned()
                    .collect();
                let n = peers.len();
                let reply = Message::PeerlistResponse { peers };
                let _ = self.socket.send_to(&protocol::encode(&reply), src);
                log::trace(&format!("replied PEERLIST_RESPONSE ({n} peers) to {src}"));
                Event::None
            }
            Message::PeerlistResponse { peers } => {
                log::debug(&format!("peerlist response ({} peers) from {src}", peers.len()));
                Event::PeerlistResponse { peers }
            }
            Message::SegmentRequest {
                transfer_id,
                filename,
            } => {
                self.registry.serve(&self.socket, transfer_id, &filename, src);
                Event::None
            }
            Message::SegmentContents {
                transfer_id,
                packet_number,
                total_packets,
                data,
            } => {
                self.registry.on_content(
                    &self.socket,
                    transfer_id,
                    packet_number,
                    total_packets,
                    data,
                    src,
                );
                Event::None
            }
            Message::SegmentNotFound { transfer_id } => {
                self.registry.on_not_found(&self.socket, transfer_id);
                Event::None
            }
            Message::Ack {
                transfer_id,
                ack_type,
                next_start,
                next_count,
            } => {
                self.registry.on_ack(
                    &self.socket,
                    transfer_id,
                    ack_type,
                    next_start,
                    next_count,
                    src,
                );
                Event::None
            }
        }
    }
}
