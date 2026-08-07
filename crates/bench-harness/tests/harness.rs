//! Syscall-touching smokes for the bench harness — every test here is
//! #[ignore] (edge-harness convention) and runs via `scripts/bench.sh --smoke`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::time::Duration;

fn scratch(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("bench-smoke-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("scratch dir {dir:?} creates: {e}"));
    dir.to_string_lossy().into_owned()
}

// A durable sync above this rate is not durable (T4 review, M-4) — this is
// the upper-bound half of the oracle: `> 0.0` alone cannot fail for any
// interesting reason (any wrong-but-positive number passes it, including a
// tmpfs-derived ceiling of 10^6+/s, which is exactly I-1's bug). No real
// F_FULLFSYNC/fsync-backed device gets anywhere close to this on any
// hardware charpente targets.
const IMPLAUSIBLY_DURABLE_PER_SEC: f64 = 1_000_000.0;

#[test]
#[ignore]
fn device_layer_produces_nonzero_rates() {
    let dir = scratch("device");
    let d = bench_harness::device::run(&dir, Duration::from_millis(200), Duration::from_secs(1))
        .expect("device layer runs");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(d.syncs_per_sec > 0.0, "syncs_per_sec: {}", d.syncs_per_sec);
    assert!(
        d.appends_per_sec > 0.0,
        "appends_per_sec: {}",
        d.appends_per_sec
    );
    assert!(
        d.syncs_per_sec < IMPLAUSIBLY_DURABLE_PER_SEC,
        "syncs_per_sec implausibly high for a durable sync: {} (measured on a \
         RAM-backed filesystem?)",
        d.syncs_per_sec
    );
    assert!(
        d.appends_per_sec < IMPLAUSIBLY_DURABLE_PER_SEC,
        "appends_per_sec implausibly high for a durable sync: {}",
        d.appends_per_sec
    );
}
