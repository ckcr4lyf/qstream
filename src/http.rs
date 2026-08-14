//! Minimal std-only HTTP/1.1 static file server (SPEC §11, M4).
//!
//! Serves a directory (manifest + segments) so HLS players (ffplay,
//! browsers) can watch the stream over HTTP. One thread per connection,
//! `Connection: close`, GET/HEAD only, no range requests. Not a
//! general-purpose server — just enough to serve a live playlist.

use std::fs;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::log;
use crate::transfer::valid_filename;

const MAX_REQUEST_BYTES: usize = 8192;

/// Serve `root` over HTTP on 0.0.0.0:port. Blocks forever. When `stats` is
/// given, GET /peers and /stats return the node's live stats snapshot (M5).
pub fn serve(root: PathBuf, port: u16, stats: Option<Arc<Mutex<Vec<String>>>>) -> io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port))?;
    log::info(&format!("http: serving {} on 0.0.0.0:{port}", root.display()));
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let root = root.clone();
                let stats = stats.clone();
                thread::spawn(move || handle_connection(root, stats, stream));
            }
            Err(e) => log::warn(&format!("http: accept failed: {e}")),
        }
    }
    Ok(())
}

fn handle_connection(root: PathBuf, stats: Option<Arc<Mutex<Vec<String>>>>, mut stream: TcpStream) {
    // Read until the end of headers (or a sane cap).
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() >= MAX_REQUEST_BYTES {
                    break;
                }
            }
        }
    }

    let request = String::from_utf8_lossy(&buf);
    let request_line = request.lines().next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();

    let head_only = method == "HEAD";
    if method != "GET" && method != "HEAD" {
        respond(
            &mut stream,
            405,
            "Method Not Allowed",
            "text/plain",
            b"method not allowed\n",
            false,
        );
        return;
    }

    // Strip query string and leading slash; validate the name.
    let name = target.split('?').next().unwrap_or_default().trim_start_matches('/');

    // M5: live stats routes (peer ranking, counters, fault totals).
    if name == "peers" || name == "stats" {
        if let Some(stats) = stats {
            let lines = stats.lock().map(|g| g.join("\n")).unwrap_or_default();
            let body = if lines.is_empty() {
                "node stats not ready\n".to_string()
            } else {
                format!("{lines}\n")
            };
            respond(&mut stream, 200, "OK", "text/plain", body.as_bytes(), head_only);
            return;
        }
    }
    if name == "health" {
        respond(&mut stream, 200, "OK", "text/plain", b"ok\n", head_only);
        return;
    }

    if !valid_filename(name) {
        respond(&mut stream, 404, "Not Found", "text/plain", b"not found\n", head_only);
        return;
    }

    match fs::read(root.join(name)) {
        Ok(data) => {
            log::debug(&format!("http: GET /{name} -> 200 ({} bytes)", data.len()));
            respond(&mut stream, 200, "OK", mime_of(name), &data, head_only);
        }
        Err(_) => {
            log::debug(&format!("http: GET /{name} -> 404"));
            respond(&mut stream, 404, "Not Found", "text/plain", b"not found\n", head_only);
        }
    }
}

fn mime_of(name: &str) -> &'static str {
    if name.ends_with(".m3u8") {
        "application/vnd.apple.mpegurl"
    } else if name.ends_with(".ts") {
        "video/mp2t"
    } else if name.ends_with(".mp4") {
        "video/mp4"
    } else {
        "application/octet-stream"
    }
}

fn respond(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    mime: &str,
    body: &[u8],
    head_only: bool,
) {
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: {mime}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    if !head_only {
        let _ = stream.write_all(body);
    }
    let _ = stream.flush();
}
