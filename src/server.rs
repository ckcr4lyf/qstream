//! Master (seed) mode (SPEC.md §6): serve manifest + segments over one UDP
//! socket until interrupted.

use std::io;
use std::net::UdpSocket;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::log;
use crate::node::Node;

pub fn run(port: u16, manifest_path: &str, name: &str) -> io::Result<()> {
    let manifest_path = PathBuf::from(manifest_path);
    if !manifest_path.is_file() {
        log::error(&format!(
            "manifest file not found: {}",
            manifest_path.display()
        ));
        std::process::exit(1);
    }
    let segment_root = manifest_path
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();

    let socket = UdpSocket::bind(("0.0.0.0", port))?;
    log::info(&format!("master listening on 0.0.0.0:{port} (name: {name})"));
    log::info(&format!("serving manifest from {}", manifest_path.display()));
    log::info(&format!("serving segments from {}", segment_root.display()));

    let mut node = Node::new(socket, name.to_string(), manifest_path, segment_root);
    let mut buf = [0u8; 65536];

    loop {
        // Clamp zero (deadline already passed) to 1ns: set_read_timeout
        // rejects a 0-duration timeout on Linux; we want to tick immediately.
        let timeout = node.next_deadline().map(|d| {
            let rem = d.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                Duration::from_nanos(1)
            } else {
                rem
            }
        });
        node.socket.set_read_timeout(timeout)?;

        match node.socket.recv_from(&mut buf) {
            Ok((n, src)) => {
                node.handle(&buf[..n], src);
            }
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut => {}
            Err(e) => return Err(e),
        }

        node.tick(Instant::now());
    }
}
