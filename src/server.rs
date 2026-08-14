//! Master (seed) mode: bind a listening UDP socket, answer handshakes,
//! maintain the peer list. See SPEC.md §6.

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};

use crate::log;
use crate::protocol::{self, Message};

/// Run the master node until the process is interrupted.
pub fn run(port: u16, name: &str) -> io::Result<()> {
    let socket = UdpSocket::bind(("0.0.0.0", port))?;
    log::info(&format!("master listening on 0.0.0.0:{port} (name: {name})"));

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
            Ok(Message::HandshakeResponse { name: peer_name }) => {
                log::warn(&format!(
                    "ignoring HANDSHAKE_RESPONSE from {src} (name: {peer_name}) — master never requests handshakes"
                ));
            }
            Err(e) => {
                log::warn(&format!("dropping malformed datagram from {src}: {e}"));
            }
        }
    }
}
