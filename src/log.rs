//! Minimal timestamped logger. Kept dependency-free for now.

use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

pub fn log(level: &str, msg: &str) {
    eprintln!("[{:>13}] {level:<5} {msg}", now_ms());
}

pub fn trace(msg: &str) {
    log("TRACE", msg);
}

pub fn info(msg: &str) {
    log("INFO", msg);
}

pub fn warn(msg: &str) {
    log("WARN", msg);
}

pub fn error(msg: &str) {
    log("ERROR", msg);
}
