//! Peer mode: bind a fixed local port (so other peers can reach us later),
//! handshake with the master, and report the result. See SPEC.md §5.3.

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use crate::log;
use crate::protocol::{self, Message};

/// How long to wait for the master's HANDSHAKE_RESPONSE (SPEC.md §5.3).
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);

/// Run the peer: send one handshake request, wait for the reply.
pub fn run(local_port: u16, remote: SocketAddr, name: &str) -> io::Result<()> {
    let socket = UdpSocket::bind(("0.0.0.0", local_port))?;
    log::info(&format!("peer listening on 0.0.0.0:{local_port} (name: {name})"));

    let request = Message::HandshakeRequest {
        name: name.to_string(),
    };
    socket.send_to(&protocol::encode(&request), remote)?;
    log::info(&format!("sent HANDSHAKE_REQUEST to {remote}"));

    socket.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
    let mut buf = [0u8; 65536];

    loop {
        match socket.recv_from(&mut buf) {
            Ok((n, src)) => match protocol::decode(&buf[..n]) {
                Ok(Message::HandshakeResponse { name: server_name }) => {
                    log::info(&format!("handshake OK — master {src} (name: {server_name})"));
                    return Ok(());
                }
                Ok(other) => {
                    log::warn(&format!("unexpected message from {src}: {other:?}"));
                }
                Err(e) => {
                    log::warn(&format!("dropping malformed datagram from {src}: {e}"));
                }
            },
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut =>
            {
                log::error(&format!(
                    "handshake timed out after {}s — no reply from {remote}",
                    HANDSHAKE_TIMEOUT.as_secs()
                ));
                std::process::exit(1);
            }
            Err(e) => return Err(e),
        }
    }
}
