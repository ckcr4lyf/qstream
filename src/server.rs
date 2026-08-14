//! Master (seed) mode: bind a listening UDP socket, answer handshakes and
//! manifest requests, maintain the peer list. See SPEC.md §6.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::path::Path;

use crate::log;
use crate::protocol::{self, Message};

/// Run the master node until the process is interrupted.
pub fn run(port: u16, manifest_path: &str, name: &str) -> io::Result<()> {
    if !Path::new(manifest_path).is_file() {
        log::error(&format!("manifest file not found: {manifest_path}"));
        std::process::exit(1);
    }

    let socket = UdpSocket::bind(("0.0.0.0", port))?;
    log::info(&format!("master listening on 0.0.0.0:{port} (name: {name})"));
    log::info(&format!("serving manifest from {manifest_path}"));

    // Peer list keyed by socket address; value is the peer's self-reported name.
    let mut peers: HashMap<SocketAddr, String> = HashMap::new();

    let mut buf = [0u8; 65536];
    loop {
        let (n, src) = socket.recv_from(&mut buf)?;

        match protocol::decode(&buf[..n]) {
            Ok(Message::HandshakeRequest { name: peer_name }) => {
                match peers.get(&src) {
                    None => log::info(&format!("peer connected: {src} (name: {peer_name})")),
                    Some(existing) if *existing != peer_name => {
                        log::info(&format!("peer {src} re-handshaked, new name: {peer_name}"));
                    }
                    _ => {}
                }
                peers.insert(src, peer_name);

                let reply = Message::HandshakeResponse {
                    name: name.to_string(),
                };
                socket.send_to(&protocol::encode(&reply), src)?;
                log::trace(&format!("replied HANDSHAKE_RESPONSE to {src}"));
            }
            Ok(Message::ManifestRequest) => {
                // Re-read from disk every time — the live playlist rolls.
                match fs::read(manifest_path) {
                    Ok(data) => {
                        let len = data.len();
                        let reply = Message::ManifestResponse { data };
                        socket.send_to(&protocol::encode(&reply), src)?;
                        log::trace(&format!(
                            "replied MANIFEST_RESPONSE ({len} bytes) to {src}"
                        ));
                    }
                    Err(e) => {
                        log::error(&format!("failed to read manifest {manifest_path}: {e}"));
                        // Empty response tells the peer we have nothing right now.
                        let reply = Message::ManifestResponse { data: Vec::new() };
                        socket.send_to(&protocol::encode(&reply), src)?;
                    }
                }
            }
            other => {
                log::warn(&format!("ignoring unexpected message from {src}: {other:?}"));
            }
        }
    }
}
