//! Shared node core (SPEC.md §7.5): one socket, message dispatch, transfer
//! registry. Used by both master and peer.

use std::fs;
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::time::Instant;

use crate::log;
use crate::protocol::{self, Message};
use crate::transfer::TransferRegistry;

/// Events a caller (the peer) may want to react to.
#[derive(Debug)]
pub enum Event {
    /// A HANDSHAKE_RESPONSE arrived (we sent a handshake request).
    HandshakeResponse { name: String },
    /// A MANIFEST_RESPONSE arrived with the raw m3u8 bytes.
    ManifestResponse { data: Vec<u8> },
    /// Nothing the caller needs to act on.
    None,
}

pub struct Node {
    pub socket: UdpSocket,
    pub name: String,
    pub manifest_path: PathBuf,
    pub registry: TransferRegistry,
}

impl Node {
    pub fn new(socket: UdpSocket, name: String, manifest_path: PathBuf, segment_root: PathBuf) -> Node {
        Node {
            socket,
            name,
            manifest_path,
            registry: TransferRegistry::new(segment_root),
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
        match protocol::decode(datagram) {
            Ok(Message::HandshakeRequest { name }) => {
                log::info(&format!("handshake from {src} (name: {name})"));
                let reply = Message::HandshakeResponse {
                    name: self.name.clone(),
                };
                let _ = self.socket.send_to(&protocol::encode(&reply), src);
                Event::None
            }
            Ok(Message::HandshakeResponse { name }) => {
                log::debug(&format!("handshake response from {src} (name: {name})"));
                Event::HandshakeResponse { name }
            }
            Ok(Message::ManifestRequest) => {
                // Re-read from disk every time — the live playlist rolls.
                let data = fs::read(&self.manifest_path).unwrap_or_default();
                let reply = Message::ManifestResponse { data };
                let _ = self.socket.send_to(&protocol::encode(&reply), src);
                Event::None
            }
            Ok(Message::ManifestResponse { data }) => {
                log::debug(&format!("manifest response ({} bytes) from {src}", data.len()));
                Event::ManifestResponse { data }
            }
            Ok(Message::SegmentRequest {
                transfer_id,
                filename,
            }) => {
                self.registry.serve(&self.socket, transfer_id, &filename, src);
                Event::None
            }
            Ok(Message::SegmentContents {
                transfer_id,
                packet_number,
                total_packets,
                data,
            }) => {
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
            Ok(Message::SegmentNotFound { transfer_id }) => {
                self.registry.on_not_found(&self.socket, transfer_id);
                Event::None
            }
            Ok(Message::Ack {
                transfer_id,
                ack_type,
                next_start,
                next_count,
            }) => {
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
            Err(e) => {
                log::warn(&format!("dropping malformed datagram from {src}: {e}"));
                Event::None
            }
        }
    }
}
