//! Minimal UTC logger with env-configurable verbosity.
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
    if enabled(level) {
        eprintln!(
            "[{}] {level_name:<5} {msg}",
            utc_timestamp(SystemTime::now())
        );
    }
}

/// Format UTC wall-clock time as `YYYY-MM-DDTHH:MM:SS.sss`.
fn utc_timestamp(now: SystemTime) -> String {
    let elapsed = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let seconds = elapsed.as_secs() as i64;
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = day_seconds / 3_600;
    let minute = (day_seconds % 3_600) / 60;
    let second = day_seconds % 60;
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{:03}",
        elapsed.subsec_millis()
    )
}

/// Convert days since 1970-01-01 to Gregorian year/month/day.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 {
        z / 146_097
    } else {
        (z - 146_096) / 146_097
    };
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_part = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_part + 2) / 5 + 1;
    let month = month_part + if month_part < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };
    (year, month, day)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_date_epoch() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn civil_date_known_value() {
        assert_eq!(civil_from_days(20_000), (2024, 10, 4));
    }
}
