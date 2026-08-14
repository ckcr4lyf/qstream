//! Minimal timestamped logger with env-configurable verbosity.
//! Levels: 0=error, 1=warn, 2=info, 3=debug, 4=trace. Default: info (2).

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static LEVEL: AtomicU8 = AtomicU8::new(2);

/// Set the verbosity from a string (`error|warn|info|debug|trace`).
/// Unknown values are ignored.
pub fn set_level(level: &str) {
    let value = match level {
        "error" => 0,
        "warn" => 1,
        "info" => 2,
        "debug" => 3,
        "trace" => 4,
        _ => return,
    };
    LEVEL.store(value, Ordering::Relaxed);
}

fn enabled(level: u8) -> bool {
    level <= LEVEL.load(Ordering::Relaxed)
}

fn log(level_name: &str, level: u8, msg: &str) {
    if !enabled(level) {
        return;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    eprintln!("[{now:>13}] {level_name:<5} {msg}");
}

pub fn trace(msg: &str) {
    log("TRACE", 4, msg);
}

pub fn debug(msg: &str) {
    log("DEBUG", 3, msg);
}

pub fn info(msg: &str) {
    log("INFO", 2, msg);
}

pub fn warn(msg: &str) {
    log("WARN", 1, msg);
}

pub fn error(msg: &str) {
    log("ERROR", 0, msg);
}
