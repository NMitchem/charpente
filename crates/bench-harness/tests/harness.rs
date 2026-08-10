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
    // T5 review, C-2 / M-6 required a REAL device probe here, because a
    // fabricated 1000.0 ceiling let the ratio assertion degenerate into
    // checking arithmetic against a made-up number. The 2026-08-10 interlude
    // (§4.2) satisfies that requirement a better way and the probe is gone:
    // this layer brackets its OWN write window and divides by that, so its
    // ratio math is grounded in a real device measurement taken either side of
    // the very window being measured — strictly closer than a separate probe
    // in a separate directory could be. Exactly the move T7 rev. 3 made for
    // the wire smoke (see its comment below). M-6's other half — the binary's
    // own device->core handoff — is still covered by the subprocess test.
    let base = scratch("core");
    let core_dir = format!("{base}/core");
    std::fs::create_dir_all(&core_dir).expect("core subdir");
    let (core, invalid) = bench_harness::core_bench::run(
        &core_dir,
        Duration::from_millis(200),
        Duration::from_secs(1),
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
    // 2026-08-10 interlude §4.2: the denominator is this run's OWN bracket
    // max, not the one-shot `device.syncs_per_sec` measured in a separate
    // window above. Asserting against the published bracket fields keeps this
    // pin self-contained in the artifact — the reader can check it by hand
    // from the JSON, which the old form (referencing a local binding) could
    // not be.
    let core_ceiling = core
        .device_appends_per_sec_before
        .max(core.device_appends_per_sec_after);
    assert!(
        (core.device_ratio - core.acked_writes_per_sec / core_ceiling).abs() < 1e-9,
        "device_ratio must equal acked_writes_per_sec / max(bracket before, after): {} vs {}",
        core.device_ratio,
        core.acked_writes_per_sec / core_ceiling
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
        bench_harness::core_bench::run(&dir, Duration::ZERO, Duration::from_secs(1))
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
    let err = bench_harness::core_bench::run(&dir, Duration::ZERO, Duration::from_millis(10))
        .expect_err("a handful of ops in 10ms must be refused, not published");
    let _ = std::fs::remove_dir_all(&dir);
    let msg = err.to_string();
    assert!(msg.contains("INVALID"), "got: {msg}");
    assert!(msg.contains("core write"), "must name which loop: {msg}");
    assert!(msg.contains("--duration"), "must name the fix: {msg}");
}

#[test]
#[ignore]
fn kernel_layer_drains_the_batch_with_every_job_done() {
    const REQUESTS: u64 = 500;
    let base = scratch("kernel");
    let kernel_dir = format!("{base}/kernel");
    std::fs::create_dir_all(&kernel_dir).expect("kernel subdir");
    // T6 review, M-5 / Noah ruling gave this layer a real device ceiling
    // rather than an invented number. The 2026-08-10 interlude (§4.2) keeps
    // that and improves it: the ceiling is now bracketed either side of this
    // batch's own window, so the separate probe that used to stand in for it
    // is gone.
    let (k, invalid) = bench_harness::kernel_bench::run(&kernel_dir, REQUESTS).expect(
        "kernel layer runs — a refusal here naming the minimum measured window means this \
         batch drained in under a second, i.e. the store is not on a device doing real \
         durable syncs (RAM-backed filesystem?), which is the C-1 floor working",
    );
    let _ = std::fs::remove_dir_all(&base);
    // Same shape as the core smoke above: the ratio gate is DESIGNED to fire
    // on short/noisy windows, so tolerate that one named reason and nothing
    // else.
    for reason in &invalid {
        assert!(
            reason.contains("device_ratio") && reason.contains("exceeds 1.0"),
            "the ratio gate legitimately fires on some short/noisy windows — but any OTHER \
             invalid reason here means something regressed: {reason}"
        );
    }
    // Genuinely measured: a kernel counter, not an echo of run()'s argument.
    // This is also the only thing covering responses_recorded, via the sink
    // crosscheck that would have pushed a reason above.
    assert_eq!(
        k.jobs_done, REQUESTS,
        "every /hit/:id enqueues exactly one Bump, and the batch must drain them all"
    );
    // T6 review, I-5: the two assertions this replaces could not fail.
    // `requests_total == 500` compares run()'s own argument with itself
    // through two struct fields with no measurement in between, and
    // `acked_writes_per_sec > 0.0` is the exact assertion T5's C-2 ruled out
    // (it holds for every finite positive window, and for infinity too).
    // Assert the upper implausibility bound and the DERIVATIONS instead —
    // each of which a wrong operand, a swapped operand or a wrong multiplier
    // would break.
    assert!(
        k.acked_writes_per_sec < IMPLAUSIBLY_DURABLE_PER_SEC,
        "acked_writes_per_sec implausibly high for a path spending two durable syncs per \
         request: {} (measured on a RAM-backed filesystem?)",
        k.acked_writes_per_sec
    );
    assert!(
        (k.acked_writes_per_sec - k.requests_total as f64 / k.window_secs).abs() < 1e-9,
        "acked_writes_per_sec must be requests_total over the published window: {} vs {}",
        k.acked_writes_per_sec,
        k.requests_total as f64 / k.window_secs
    );
    assert!(
        (k.log_records_per_sec - (k.requests_total + k.jobs_done) as f64 / k.window_secs).abs()
            < 1e-9,
        "log_records_per_sec must be (requests + jobs_done) over the SAME window — a wrong \
         multiplier or a swapped operand breaks this: {} vs {}",
        k.log_records_per_sec,
        (k.requests_total + k.jobs_done) as f64 / k.window_secs
    );
    // 2026-08-10 interlude §4.2: this layer now brackets its own batch window,
    // so the denominator is its own bracket max rather than the one-shot
    // sample measured above.
    let k_ceiling = k
        .device_appends_per_sec_before
        .max(k.device_appends_per_sec_after);
    assert!(
        (k.device_ratio - k.log_records_per_sec / k_ceiling).abs() < 1e-9,
        "device_ratio must divide the LOG rate (two syncs per request), not the ack rate: \
         {} vs {}",
        k.device_ratio,
        k.log_records_per_sec / k_ceiling
    );
    assert!(
        k.device_appends_per_sec_before > 0.0 && k.device_appends_per_sec_after > 0.0,
        "this layer must take its own bracket samples — the interlude's whole §4.2: {}/{}",
        k.device_appends_per_sec_before,
        k.device_appends_per_sec_after
    );
}

// T7 rev. 3: no separate device-ceiling probe here any more — this layer
// brackets its own ceiling per point (device::device_bracket_sample, before
// spawning the child and after killing it), rather than the one-shot
// cross-window sample rev. 2 divided by.
//
// T7 rev. 4 is what makes `invalid.is_empty()` a real assertion rather than
// a coin flip, and rev. 3 alone did not: with a bracket-stable ceiling this
// smoke still failed 3 of 3, because the quantity being flagged was the
// DERIVED record rate (records_per_request * throughput, which assumes a
// steady-state job drain a 2s window has not reached). The gate now reads
// the COUNTED acked-request rate, which cannot exceed the ceiling by
// construction (ack-after-durable). See the plan's Task 7 rev. 4 block for
// the readings. If this flakes, that is a finding to report, not a
// threshold to widen.
#[test]
#[ignore]
fn wire_layer_sweeps_the_bench_app() {
    let bin = bench_harness::wire::build_bin("bench-harness", "bench-app").expect("bin builds");
    let (wire, invalid) = bench_harness::wire::sweep(
        &bin,
        bench_harness::wire::Target::BenchApp,
        &[1, 4],
        Duration::from_millis(300),
        Duration::from_secs(2),
    )
    .expect("sweep runs");
    assert!(
        invalid.is_empty(),
        "short healthy run must be valid: {invalid:?}"
    );
    assert_eq!(wire.points.len(), 2);
    for p in &wire.points {
        assert!(p.throughput_per_sec > 0.0);
        // The derivation, asserted rather than assumed (T6 review, I-5: a
        // `> 0.0` on a number the caller supplied is not an assertion).
        assert_eq!(p.log_records_per_sec, 2.0 * p.throughput_per_sec);
        // T7 rev. 3: the published ratio divides by the bracket MAX, not
        // either sample alone. Published, not gated — that is rev. 4.
        assert_eq!(
            p.device_ratio,
            p.log_records_per_sec
                / p.device_appends_per_sec_before
                    .max(p.device_appends_per_sec_after)
        );
        assert!(
            p.p50_us <= p.p99_us && p.p99_us <= p.p999_us,
            "percentile ordering"
        );
    }
}

// T8: the same sweep against a REAL app rather than the purpose-built
// vehicle. What this smoke adds over its bench-app twin is the parts
// bench-app has none of — a login round-trip whose token every subsequent
// request must carry as a cookie, and a handler that reads before it writes.
// A broken `session_setup` shows up here as a 401 storm rather than as a
// plausible-looking number, because the driver counts non-200s as errors.
//
// `--duration` is 2s, not the plan's 1s. The plan's Task 8 Step 2 block
// specifies 1s, but Task 7's own rev. 2 ruling rejected exactly that value
// for exactly this shape: `MIN_MEASURED_WINDOW` is 1s, and "a smoke that
// sits exactly on a refusal boundary is a flake waiting for a slow CI box."
// The two instructions contradict; this follows the ruling, not the
// transcribed literal.
//
// **Why this smoke does NOT assert `invalid.is_empty()` while its bench-app
// twin above does (Noah ruling, 2026-08-10).** invoicing commits ONE log
// record per request, so its acked-request rate and the device ceiling are
// the same physical quantity, and T7 rev. 4's gate sits at exactly 1.0x.
// Measured on an Apple M4 Pro / apfs: the raw fsync bracket reads 219-246/s,
// a `Wal::append`+`Wal::sync` loop 224-231/s, a bare in-process `Db::transact`
// loop 246/s (1.00x the ceiling, with no HTTP at all), and this target
// 237-257/s. The commit path is entirely fsync-bound, so every one of those
// measures the same thing to within its own +/-5% noise, and the gate fired on
// roughly one healthy run in three. bench-app never shows it because its 2.0
// records-per-request buys a 2x margin — rev. 4's "no false-alarm path" was
// established only where the gate had headroom.
//
// Measuring the ceiling on the WAL path instead was tried first and is
// refuted by the numbers above: there is no path meaningfully faster than the
// thing being bounded. So the gate stays exactly as it is — unweakened, and
// still catching a genuinely impossible number — and this smoke tolerates
// that ONE named reason, which is precisely what
// `core_layer_commits_and_reports_a_ratio` already does one layer down for
// the same reason. Any OTHER reason still reddens: a 401 storm, a lost point,
// a disagreeing crosscheck and a starved heartbeat all carry different
// wording, and a LOST point additionally fails the `points.len()` assertion
// below, since a lost point publishes nothing.
#[test]
#[ignore]
fn wire_layer_drives_invoicing_through_login() {
    let bin = bench_harness::wire::build_bin("invoicing", "node").expect("node builds");
    let (wire, invalid) = bench_harness::wire::sweep(
        &bin,
        bench_harness::wire::Target::Invoicing,
        &[2],
        Duration::from_millis(300),
        Duration::from_secs(2),
    )
    .expect("sweep runs");
    for reason in &invalid {
        assert!(
            reason.contains("acked requests/s") && reason.contains("the measured device ceiling"),
            "a 1-record-per-request target legitimately sits at 1.0x the ceiling, so the \
             ack-rate gate fires on measurement noise here (that is the gate working, and \
             the ruling is to tolerate it rather than weaken it) — but any OTHER invalid \
             reason means something regressed: {reason}"
        );
    }
    assert_eq!(wire.points.len(), 1);
    assert!(wire.points[0].throughput_per_sec > 0.0);
    // invoicing's create_customer is get + put with no enqueue, so its record
    // rate is 1.0x throughput, not bench-app's 2.0x. Asserting the
    // per-target multiple here is what makes `records_per_request` a
    // measured contract rather than a constant nobody reads (the T7 smoke
    // asserts the same relation at 2.0).
    assert_eq!(
        wire.points[0].log_records_per_sec,
        wire.points[0].throughput_per_sec
    );
}
