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
use crate::node::StatsSnapshot;
use crate::transfer::valid_filename;

const MAX_REQUEST_BYTES: usize = 8192;

/// Serve `root` over HTTP on 0.0.0.0:port. Blocks forever. When `stats` is
/// given, GET /peers returns the ranking text and /stats returns the JSON
/// stats document (M5).
pub fn serve(root: PathBuf, port: u16, stats: Option<Arc<Mutex<StatsSnapshot>>>) -> io::Result<()> {
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

fn handle_connection(root: PathBuf, stats: Option<Arc<Mutex<StatsSnapshot>>>, mut stream: TcpStream) {
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

    // M5: live stats routes.
    if name == "peers" {
        if let Some(stats) = stats {
            let body = stats.lock().map(|g| g.lines.join("\n")).unwrap_or_default();
            let body = if body.is_empty() {
                "node stats not ready\n".to_string()
            } else {
                format!("{body}\n")
            };
            respond(&mut stream, 200, "OK", "text/plain", body.as_bytes(), head_only);
            return;
        }
    }
    if name == "stats" {
        if let Some(stats) = stats {
            let body = stats.lock().map(|g| g.json.clone()).unwrap_or_default();
            let body = if body.is_empty() {
                "{}".to_string()
            } else {
                format!("{body}\n")
            };
            respond(&mut stream, 200, "OK", "application/json", body.as_bytes(), head_only);
            return;
        }
    }
    if name == "metrics" {
        if let Some(stats) = stats {
            let body = stats.lock().map(|g| g.metrics.clone()).unwrap_or_default();
            let body = if body.is_empty() {
                String::new()
            } else {
                format!("{body}\n")
            };
            respond(
                &mut stream,
                200,
                "OK",
                "text/plain; version=0.0.4",
                body.as_bytes(),
                head_only,
            );
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
            // Serve a playlist filtered to segments that actually exist, so
            // players never 404 on a live edge the node hasn't replicated
            // yet (DEVLOG: remote peer lag -> mpv 404 storm).
            let body = if name.ends_with(".m3u8") {
                filter_playlist(&root, &data)
            } else {
                data
            };
            respond(&mut stream, 200, "OK", mime_of(name), &body, head_only);
        }
        Err(_) => {
            log::debug(&format!("http: GET /{name} -> 404"));
            respond(&mut stream, 404, "Not Found", "text/plain", b"not found\n", head_only);
        }
    }
}

/// Rewrite an m3u8 playlist to list only segments present in `root`.
/// EXT-X-MEDIA-SEQUENCE is advanced to the first kept segment so the
/// playlist stays coherent; the master (which has everything) is unchanged.
fn filter_playlist(root: &std::path::Path, data: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(data);
    let mut seq: u64 = 0;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
            seq = rest.trim().parse().unwrap_or(0);
        }
    }
    let mut out: Vec<u8> = Vec::with_capacity(data.len());
    let mut pending: Vec<&str> = Vec::new(); // # lines queued before a segment
    let mut first_kept: Option<u64> = None;
    let mut saw_m3u = false;
    for line in text.lines() {
        if line.starts_with('#') {
            if line.starts_with("#EXT-X-MEDIA-SEQUENCE:") {
                continue; // re-emitted after we know the first kept segment
            }
            pending.push(line);
            if line.starts_with("#EXTM3U") {
                saw_m3u = true;
            }
            continue;
        }
        let name = line.trim();
        if name.is_empty() {
            continue;
        }
        if root.join(name).is_file() {
            if first_kept.is_none() {
                first_kept = Some(seq);
            }
            for p in pending.drain(..) {
                out.extend_from_slice(p.as_bytes());
                out.push(b'\n');
            }
            out.extend_from_slice(name.as_bytes());
            out.push(b'\n');
        } else {
            pending.clear(); // drop EXTINF etc. for missing segments
        }
        seq += 1;
    }
    // Empty result (nothing available yet): serve the original so players
    // at least see the edge and retry.
    if !saw_m3u || out.is_empty() {
        return data.to_vec();
    }
    // Insert the advanced MEDIA-SEQUENCE right after #EXTM3U/#EXT-X-VERSION.
    let insert = format!("#EXT-X-MEDIA-SEQUENCE:{}\n", first_kept.unwrap_or(0));
    let mut final_out: Vec<u8> = Vec::with_capacity(out.len() + 32);
    let text_out = String::from_utf8_lossy(&out);
    let mut inserted = false;
    for line in text_out.lines() {
        final_out.extend_from_slice(line.as_bytes());
        final_out.push(b'\n');
        if !inserted && (line.starts_with("#EXTM3U") || line.starts_with("#EXT-X-VERSION:")) {
            final_out.extend_from_slice(insert.as_bytes());
            inserted = true;
        }
    }
    if !inserted {
        final_out.extend_from_slice(insert.as_bytes());
    }
    final_out
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
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmpdir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("qstream_http_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn playlist_filter_keeps_only_present_segments() {
        let dir = tmpdir("filter1");
        fs::write(dir.join("seg_0100.ts"), b"x").unwrap();
        fs::write(dir.join("seg_0102.ts"), b"x").unwrap();
        let playlist = "\
#EXTM3U\n\
#EXT-X-VERSION:3\n\
#EXT-X-TARGETDURATION:2\n\
#EXT-X-MEDIA-SEQUENCE:100\n\
#EXTINF:2.0,\n\
seg_0100.ts\n\
#EXTINF:2.0,\n\
seg_0101.ts\n\
#EXTINF:2.0,\n\
seg_0102.ts\n";
        let out = filter_playlist(&dir, playlist.as_bytes());
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("seg_0100.ts"));
        assert!(!text.contains("seg_0101.ts"));
        assert!(text.contains("seg_0102.ts"));
        // MEDIA-SEQUENCE must stay 100 (first kept is the first segment).
        assert!(text.contains("#EXT-X-MEDIA-SEQUENCE:100"));
        // EXTINF for the dropped segment must not linger.
        assert_eq!(text.matches("#EXTINF").count(), 2);
    }

    #[test]
    fn playlist_filter_advances_sequence_after_dropped_leader() {
        let dir = tmpdir("filter2");
        fs::write(dir.join("seg_0102.ts"), b"x").unwrap();
        let playlist = "\
#EXTM3U\n\
#EXT-X-MEDIA-SEQUENCE:100\n\
#EXTINF:2.0,\n\
seg_0100.ts\n\
#EXTINF:2.0,\n\
seg_0101.ts\n\
#EXTINF:2.0,\n\
seg_0102.ts\n";
        let out = filter_playlist(&dir, playlist.as_bytes());
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("#EXT-X-MEDIA-SEQUENCE:102"));
        assert!(text.contains("seg_0102.ts"));
        assert!(!text.contains("seg_0100.ts"));
    }

    #[test]
    fn playlist_filter_empty_falls_back_to_original() {
        let dir = tmpdir("filter3");
        let playlist = "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:5\n#EXTINF:2.0,\nseg_0005.ts\n";
        assert_eq!(
            filter_playlist(&dir, playlist.as_bytes()),
            playlist.as_bytes()
        );
    }
}
