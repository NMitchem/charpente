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

    // Task 3 Ruling C (2026-08-19 phase-attribution plan): `invalid.is_empty()`
    // conflated EVERY reason `core_bench::run` can push — including the
    // pre-existing, out-of-scope `core_accounting::write_accounting`
    // device-bracket-stability gate (`measure::unstable_device_reason`),
    // which this crate's own ignored smoke tests already document as
    // expected to fire occasionally on a loaded box — with a
    // phase-attribution refusal specifically, which is what this test's own
    // comment claims to check. A phase test must assert what IT means: no
    // PHASE-ATTRIBUTION refusal fired. Every such refusal — from
    // `phase_accounting::attribute` itself, or from `core_bench::run`'s own
    // capacity-growth/window-dilution guards added alongside it — is
    // prefixed "contract:"; every other reason this vec can carry
    // (`core_accounting`'s heartbeat/crosscheck/ratio/stability reasons) is
    // prefixed "INVALID:". That split is load-bearing here, not a
    // coincidence of two unrelated authors' conventions.
    let phase_refusals: Vec<&String> = invalid
        .iter()
        .filter(|r| r.starts_with("contract:"))
        .collect();
    assert!(
        phase_refusals.is_empty(),
        "phase attribution must not refuse a single-threaded arm: {phase_refusals:?}"
    );

    let phases = report
        .phases
        .expect("no phase-attribution refusal fired above, so phases must be Some");
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
