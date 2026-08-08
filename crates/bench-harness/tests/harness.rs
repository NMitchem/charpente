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
    // T4 re-review, M-F: each append frame is stamped with its own offset, so
    // no two blocks are byte-identical. High entropy inside one frame defeats
    // zero-elision; only distinct blocks also defeat an LZ-family compressor
    // (ZFS lz4, btrfs zstd) making this payload's append cheaper than a real
    // WAL frame's. Read back before the cleanup below.
    let appended = std::fs::read(std::path::Path::new(&dir).join("bench-append.dat"))
        .expect("the append loop's file exists");
    assert!(
        appended.len() >= 512,
        "expected at least two 256-byte frames, got {} bytes",
        appended.len()
    );
    assert_ne!(
        &appended[..256],
        &appended[256..512],
        "consecutive frames must not be byte-identical, or the file is one block repeated"
    );
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

#[test]
#[ignore]
fn core_layer_commits_and_reports_a_ratio() {
    // T5 review, C-2 / M-6: a fabricated 1000.0 ceiling let the ratio
    // assertion degenerate into checking arithmetic against a made-up
    // number, and meant the layer's ratio math was never grounded in a real
    // device measurement. Run a REAL (short) device probe first, on its own
    // subdirectory of the same scratch tree — mirroring bin/bench.rs's own
    // shape (device runs in its own dir, core in its own, same base
    // filesystem) without needing the full binary (that's the separate
    // subprocess test below, which closes the rest of M-6: the binary's OWN
    // `report.device.as_ref().expect(...)` handoff line).
    let base = scratch("core");
    let device_dir = format!("{base}/device");
    let core_dir = format!("{base}/core");
    std::fs::create_dir_all(&device_dir).expect("device subdir");
    std::fs::create_dir_all(&core_dir).expect("core subdir");
    let device = bench_harness::device::run(
        &device_dir,
        Duration::from_millis(200),
        Duration::from_secs(1),
    )
    .expect("device layer runs");
    let ceiling = device.syncs_per_sec;
    let (core, invalid) = bench_harness::core_bench::run(
        &core_dir,
        Duration::from_millis(200),
        Duration::from_secs(1),
        ceiling,
    )
    .expect("core layer runs");
    let _ = std::fs::remove_dir_all(&base);
    // NOT `invalid.is_empty()` (M-2 kept that assertion for the pre-existing
    // heartbeat arms, which need the machine to be genuinely broken to fire —
    // this is a DIFFERENT source). C-1's ratio gate is designed to fire on
    // short/noisy windows — the review measured it 2-in-7 even on a "quiet"
    // machine with fully separate runs, and this smoke's 1s windows plus
    // real parallel-test contention make it MORE likely, not less. A gate
    // that never fires on a short window would be the bug (that is C-1
    // itself). Tolerate only that one, named, reason; any OTHER invalid
    // reason on a healthy short run means something really regressed.
    for reason in &invalid {
        assert!(
            reason.contains("device_ratio") && reason.contains("exceeds 1.0"),
            "C-1's ratio gate legitimately fires on some short/noisy windows (that's the \
             fix working) — but any OTHER invalid reason here means something regressed: \
             {reason}"
        );
    }
    assert!(core.acked_writes_per_sec > 0.0);
    // T5 review, C-2 (recommended fix 2): the implausibility upper bound the
    // device smoke already has (IMPLAUSIBLY_DURABLE_PER_SEC's own rationale
    // above applies verbatim — no fsync-backed commit path reaches this).
    assert!(
        core.acked_writes_per_sec < IMPLAUSIBLY_DURABLE_PER_SEC,
        "acked_writes_per_sec implausibly high for an fsync-bound commit path: {} \
         (measured on a RAM-backed filesystem?)",
        core.acked_writes_per_sec
    );
    assert!(
        core.reads_per_sec > core.acked_writes_per_sec,
        "reads never fsync — this is a weak sanity bound (large margin); the real \
         protection against a read loop touching nothing is core_bench::run's own \
         hits == reads invariant (T5 review, C-2 recommended fix 3), not this assertion"
    );
    // T5 review, C-2 (recommended fix 1): `device_ratio > 0.0` was strictly
    // implied by the `acked_writes_per_sec > 0.0` assertion above — no input
    // to `run` could make one fail and not the other. Assert the field's
    // DERIVATION instead, which can actually fail (e.g. a wrong multiplier
    // or a swapped operand, both proven by mutation in the fix-round report).
    assert!(
        (core.device_ratio - core.acked_writes_per_sec / ceiling).abs() < 1e-9,
        "device_ratio must equal acked_writes_per_sec / device.syncs_per_sec: {} vs {}",
        core.device_ratio,
        core.acked_writes_per_sec / ceiling
    );
}

// T5 review, M-6: the smoke above measures a real device ceiling, but never
// drives `bin/bench.rs`'s OWN device->core handoff
// (`report.device.as_ref().expect("device ran first").syncs_per_sec`) — only
// manual CLI runs did. Spawn the real binary end to end.
#[test]
#[ignore]
fn the_binarys_device_to_core_handoff_is_exercised() {
    let dir = scratch("handoff");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_bench"))
        .args([
            "core",
            "--duration",
            "1",
            "--warmup",
            "0.2",
            "--dir",
            &dir,
            "--json",
        ])
        .output()
        .expect("bench binary runs");
    let _ = std::fs::remove_dir_all(&dir);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let code = out.status.code();
    // Not necessarily 0: C-1's ratio gate can legitimately mark a short run
    // INVALID (exit 1) on a noisy machine — this test proves the handoff
    // itself works, not that every run is valid.
    assert!(
        code == Some(0) || code == Some(1),
        "bench core must exit 0 (valid) or 1 (INVALID) through the real device->core \
         handoff, got {code:?}\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("\"core\":{"),
        "the real device->core handoff must produce a core report: {stdout}"
    );
    assert!(stdout.contains("\"device_ratio\""), "got: {stdout}");
}

// T5 review, C-3 / Noah ruling #2: a genuinely degenerate CLI window (the
// review's own repro, `--duration 0.004`) must be refused, not published as
// a self-contradicting "duration_secs: 0.00" artifact with four rates
// derived from it.
//
// T5 re-review, N-7 (Minor): at this duration the DEVICE layer's own floor
// (device::timed_loop, which runs first per bin/bench.rs's handoff) is what
// actually refuses — `core_bench::run` is never reached, so this test proves
// ruling #2's end-to-end regression through the CLI, not the core loops'
// OWN wiring to the shared floor. `a_below_floor_core_write_window_is_refused`
// below is what proves that.
#[test]
#[ignore]
fn a_degenerate_cli_duration_is_refused_not_published() {
    let dir = scratch("degenerate");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_bench"))
        .args([
            "core",
            "--duration",
            "0.004",
            "--warmup",
            "0",
            "--dir",
            &dir,
            "--json",
        ])
        .output()
        .expect("bench binary runs");
    let _ = std::fs::remove_dir_all(&dir);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(
        out.status.code(),
        Some(1),
        "a degenerate window must be refused (layer failure, exit 1), not published at \
         exit 0\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("layer failed") && stderr.contains("INVALID"),
        "got: {stderr}"
    );
}

// T5 re-review, N-1 (Critical): WAL seqs are 0-based (log.rs's next_seq
// starts at 0), so on a fresh Db with warmup == 0, `last_applied()` returns
// None before ANY record is applied and Some(0) after the first. The
// original I-3 fix's `unwrap_or(0)` read "no records applied" as "record 0
// applied", so the applied-seq delta came out one low on every such run —
// publishing a wrong log_records_per_sec AND raising a named INVALID
// accusing charpente's commit path of breaking the one-record-per-transact
// contract it did not break. `--warmup 0` is exactly what C-3's own repro
// used, so this is a flag value the sub-project has already exercised.
#[test]
#[ignore]
fn a_warmup_zero_core_write_window_reports_the_correct_log_record_count() {
    let dir = scratch("warmup-zero");
    let (core, invalid) =
        bench_harness::core_bench::run(&dir, Duration::ZERO, Duration::from_secs(1), 250.0)
            .expect("core layer runs");
    let _ = std::fs::remove_dir_all(&dir);
    for reason in &invalid {
        assert!(
            reason.contains("device_ratio") && reason.contains("exceeds 1.0"),
            "a warmup==0 run must not raise a FALSE log_records_per_sec mismatch (N-1) — \
             any invalid reason here besides the ratio gate is a regression: {reason}"
        );
    }
    assert!(
        (core.log_records_per_sec - core.acked_writes_per_sec).abs() < 1e-6,
        "core_bench's canonical transaction commits exactly one effects record per \
         transact (no dispatcher runs at this layer to double it with a job Done-mark), \
         so log_records_per_sec must equal acked_writes_per_sec even when warmup == 0: \
         {} vs {}",
        core.log_records_per_sec,
        core.acked_writes_per_sec
    );
}

// T5 review, C-3 / Noah ruling #2: proves the WIRING inside core_bench::run
// itself, not just measure::degenerate_window_reason in isolation (unit-
// tested in measure.rs's own #[cfg(test)] module) or device.rs's parallel
// wiring. `canonical` performs a real fsync per op (~3-5ms on this class of
// hardware per the review's own measurements), so a tiny nonzero duration
// reliably lands a SMALL NONZERO op count — the discriminating case a naive
// `ops == 0` check (what shipped before this round) would have let through.
#[test]
#[ignore]
fn a_below_floor_core_write_window_is_refused() {
    let dir = scratch("core-below-floor");
    let err =
        bench_harness::core_bench::run(&dir, Duration::ZERO, Duration::from_millis(10), 250.0)
            .expect_err("a handful of ops in 10ms must be refused, not published");
    let _ = std::fs::remove_dir_all(&dir);
    let msg = err.to_string();
    assert!(msg.contains("INVALID"), "got: {msg}");
    assert!(msg.contains("core write"), "must name which loop: {msg}");
    assert!(msg.contains("--duration"), "must name the fix: {msg}");
}
