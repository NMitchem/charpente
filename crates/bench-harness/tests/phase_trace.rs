//! The trace must attribute a REAL run, not just synthetic events.
//!
//! These tests deliberately assert NOTHING about which phase is largest. That
//! is the measurement this instrument exists to take, and a test that encodes
//! the expected answer stops being a check and becomes a way to confirm a
//! prior belief.
//!
//! Integration test binaries are their own crate — `bench-harness`'s
//! `lib.rs`-level `#![allow(...)]` does not reach here, so this needs its
//! own, exactly as `tests/harness.rs` and `tests/app.rs` already carry.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[test]
fn the_core_arm_attributes_a_real_run() {
    // bench-harness has NO `tempfile` dependency and does not need one — this
    // is the idiom `tests/harness.rs:21` already uses. Do not add a crate.
    let dir = std::env::temp_dir().join(format!("bench-phase-core-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let (report, invalid) = bench_harness::core_bench::run(
        dir.to_str().expect("utf-8 path"),
        std::time::Duration::from_millis(200),
        std::time::Duration::from_millis(800),
    )
    .expect("core arm runs");

    let phases = report
        .phases
        .expect("a recording run carries phases; refusals were: {invalid:?}");
    assert!(
        invalid.is_empty(),
        "attribution must not refuse a single-threaded arm: {invalid:?}"
    );
    assert_eq!(
        phases.total(),
        report.window_nanos,
        "phases must close to the measured window exactly"
    );
    assert!(
        phases.wal_sync_io > 0,
        "a run that commits must have paid at least one WAL fsync"
    );
}
