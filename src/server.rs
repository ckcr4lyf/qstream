//! Master (seed) mode (SPEC.md §6): serve manifest + segments over one UDP
//! socket until interrupted. Optionally also serves the stream over HTTP
//! for playback (M4).

use std::io;
use std::net::UdpSocket;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::fault::FaultInjector;
use crate::http;
use crate::log;
use crate::node::Node;

pub fn run(
    port: u16,
    manifest_path: &str,
    name: &str,
    http_port: Option<u16>,
) -> io::Result<()> {
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

    if let Some(hp) = http_port {
        let root = segment_root.clone();
        let stats: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let stats_http = stats.clone();
        thread::spawn(move || {
            if let Err(e) = http::serve(root, hp, Some(stats_http)) {
                log::error(&format!("http server failed: {e}"));
                std::process::exit(1);
            }
        });
        let mut node = Node::new(
            socket,
            name.to_string(),
            manifest_path,
            segment_root,
            FaultInjector::from_env(),
            Some(stats),
        );
        let mut buf = [0u8; 65536];
        run_loop(&mut node, &mut buf)
    } else {
        let mut node = Node::new(
            socket,
            name.to_string(),
            manifest_path,
            segment_root,
            FaultInjector::from_env(),
            None,
        );
        let mut buf = [0u8; 65536];
        run_loop(&mut node, &mut buf)
    }
}

fn run_loop(node: &mut Node, buf: &mut [u8; 65536]) -> io::Result<()> {
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

        match node.socket.recv_from(buf) {
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
