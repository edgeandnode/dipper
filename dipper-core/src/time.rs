//! Wall-clock helpers shared across dipper crates.

use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock seconds since the Unix epoch, with `0` as the fallback
/// when the system clock is set before 1970 (a near-impossible state
/// where any sane behaviour is fine).
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
