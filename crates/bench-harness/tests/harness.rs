//! Syscall-touching smokes for the bench harness — #[ignore] by the
//! edge-harness convention, run via `scripts/bench.sh --smoke`.
//!
//! T9 exception, stated because the blanket claim used to be true and is not:
//! the two forced-INVALID-arm tests at the bottom of this file are NOT
//! ignored. They spawn no process, bind no port and touch no data directory —
//! one holds a mutex, the other builds a struct and renders it — so the reason
//! the convention exists does not apply to them, and they run in every
//! `cargo test --workspace`. The criterion is "does it need a process, a port
//! or a disk", not a count.
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
    // this is a DIFFERENT source). C-1's ratio gate fires on short/noisy
    // windows — the review measured it 2-in-7 even on a "quiet" machine with
    // fully separate runs, and this smoke's 1s windows plus real
    // parallel-test contention made it MORE likely, not less.
    //
    // 2026-08-10 interlude: that frequency is now historical. This layer used
    // to divide by layer 1's one-shot `device.syncs_per_sec` and went red 4 of
    // 4 `bench core` runs; dividing by its own brackets, at its own measured
    // error, it went 6 of 6 GREEN with ratios 0.91-1.01. The tolerance is kept
    // because a gate that CANNOT fire would be vacuous — not because firing is
    // still expected. Tolerate only that one, named, reason; any OTHER invalid
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
    // I-3 fix (2026-08-10 group-commit interlude, T1 fix wave): divide the
    // PUBLISHED syncs rate, not `acked_writes_per_sec` — T1 moved the gate's
    // numerator to `syncs_per_sec`, and this pin asserted the superseded
    // one, passing only because the two coincide before group commit
    // (`core_accounting`'s own `while_one_txn_costs_one_fsync_the_gate_
    // input_is_unchanged` is where that coincidence is asserted directly).
    assert!(
        (core.device_ratio - core.syncs_per_sec / core_ceiling).abs() < 1e-9,
        "device_ratio must equal syncs_per_sec / max(bracket before, after): {} vs {}",
        core.device_ratio,
        core.syncs_per_sec / core_ceiling
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
    // Raised from 500 (group-commit T7): the job arm now batches too, same
    // as the request arm did from T6, so 500 requests no longer clear
    // MIN_MEASURED_WINDOW's 1s floor (they drained in ~140ms on the box
    // this was found on) — the identical shape as kernel_bench.rs's own
    // sentinel test, which needed the same bump for the same reason.
    //
    // Fix round M-8: 500 -> 5_000 measured only ~1.4s against the 1s floor
    // (~40% margin) on the box that found it, and this sub-project exists to
    // make that margin SMALLER over time, not stay thin — see
    // `kernel_bench.rs`'s identical sentinel test for the full accounting.
    // 20_000 (~4x) trades a few extra seconds of `--ignored` test time for
    // real headroom.
    const REQUESTS: u64 = 20_000;
    let base = scratch("kernel");
    let kernel_dir = format!("{base}/kernel");
    std::fs::create_dir_all(&kernel_dir).expect("kernel subdir");
    // T6 review, M-5 / Noah ruling gave this layer a real device ceiling
    // rather than an invented number. The 2026-08-10 interlude (§4.2) keeps
    // that and improves it: the ceiling is now bracketed either side of this
    // batch's own window, so the separate probe that used to stand in for it
    // is gone.
    let (k, invalid) = bench_harness::kernel_bench::run(&kernel_dir, REQUESTS).expect(
        // Fix round M-10 (same defect class as kernel_bench.rs's identical
        // site, extended here since it sits right beside this same edit):
        // under batching, an ordinary device is now the LIKELY reason a
        // refusal names the floor, not a RAM-backed filesystem.
        "kernel layer runs — a refusal here naming the minimum measured window most likely \
         means this device is simply fast enough to drain this batch in under a second now \
         that both arms batch (which is the C-1 floor working); less likely, it means the \
         store is not on a device doing real durable syncs at all (RAM-backed filesystem?)",
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
    // I-3 fix (2026-08-10 group-commit interlude, T1 fix wave): divide the
    // PUBLISHED syncs rate, not `log_records_per_sec` — T1 moved the gate's
    // numerator to `syncs_per_sec`, and this pin asserted the superseded
    // one, passing only because the two coincide before group commit (one
    // sync per commit; `kernel_accounting`'s own `while_one_commit_costs_
    // one_fsync_the_gate_input_is_unchanged` is where that coincidence is
    // asserted directly).
    assert!(
        (k.device_ratio - k.syncs_per_sec / k_ceiling).abs() < 1e-9,
        "device_ratio must divide SYNCS, not the log record rate: {} vs {}",
        k.device_ratio,
        k.syncs_per_sec / k_ceiling
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
// steady-state job drain a 2s window has not reached). See the plan's Task 7
// rev. 4 block for the readings. If this flakes, that is a finding to report,
// not a threshold to widen.
//
// **Two corrections, 2026-08-14 (the wire sync counter).** The sentence that
// used to sit here — "the gate now reads the COUNTED acked-request rate" —
// described rev. 4's design, which the 2026-08-10 device-ratio interlude
// deliberately REVERSED back to a derived `syncs_per_request * throughput`
// (see `wire_accounting`'s module doc). So this comment had been wrong about
// its own gate since that interlude.
//
// And the assertion below was RED for two days without anyone seeing it.
// Group commit's T6/T7 (2026-08-12/13) made N commits share one fsync, at
// which point the derived sync rate ran away from the ceiling and the gate
// refused every contended point. Proven, not inferred: at the pre-fix commit
// this test fails with "954 durable syncs/s from 477 acked requests/s is
// 4.08x the measured device ceiling" — 954 being exactly 2.0 * 477, the
// derivation. It stayed invisible because `#[ignore]` keeps it out of
// `cargo test --workspace`, and the last recorded `scripts/bench.sh --smoke`
// run (12/12 green, 2026-08-10) predates the batching. **The `#[ignore]` that
// keeps expensive tests out of the default suite also hides their failures
// until someone runs the matrix by hand** — same class as the kernel pin
// having had no CI enforcement (T9 census, C4).
//
// The gate now reads a real server-reported sync count for bench-app
// (`/__syncs`, lib.rs), which is what `wire_accounting`'s "owed work"
// paragraph asked for.
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
        // bench-app reports a real count; a silent fall back to the derivation
        // would leave the gate on an unsound quantity with this test still
        // green, which is precisely how the pre-fix state survived two days.
        assert!(
            p.syncs_counted,
            "[{} conns] bench-app must report a COUNTED sync rate — `syncs_counted: false` means \
             /__syncs was unreachable or unparseable and the unsound derivation is back in the \
             gate",
            p.connections
        );
        // The discriminating assertion, and it is unsatisfiable under the old
        // derivation by construction: that produced `syncs_per_request *
        // throughput` = 2.0 * throughput for bench-app, which can never be
        // below throughput. Only amortization can put it there. Asserted at
        // CONTENDED points only — at 1 connection nothing batches, so ~2 syncs
        // per request is the correct answer and this must not be demanded of
        // it.
        if p.connections > 1 {
            // T13: `syncs_per_sec` is an `Option` — `None` is the WITHDRAWN
            // derived rate, which for bench-app would mean `/__syncs` went
            // unanswered. `expect` rather than a silent skip: the assertion
            // above already demands `syncs_counted`, and a `None` reaching here
            // after that would be the two fields disagreeing.
            let syncs = p
                .syncs_per_sec
                .expect("a counted point publishes its rate (syncs_counted is asserted above)");
            assert!(
                syncs < p.throughput_per_sec,
                "[{} conns] group commit must amortize: {} syncs/s against {} req/s is {:.2} \
                 syncs per request — at or above 1.0 means batches of one, i.e. either the \
                 request arm stopped draining or the derivation is back",
                p.connections,
                syncs,
                p.throughput_per_sec,
                syncs / p.throughput_per_sec
            );
        }
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
// the same physical quantity, and the gate sits at exactly 1.0x. bench-app
// never shows it because its 2.0 records-per-request buys a 2x margin — T7
// rev. 4's "no false-alarm path" was established only where the gate had
// headroom.
//
// **UPDATED by the 2026-08-10 device-ratio-gate interlude, which disproved
// part of what this comment used to say.** The shipped text claimed
// "measuring the ceiling on the WAL path was tried first and is refuted …
// there is no path meaningfully faster than the thing being bounded". That
// refutation rested on n=2. Re-run at n=15 paired, with the loop-order
// confound controlled at n=14, the append shape is ~4.2% FASTER than the
// one-byte-rewrite shape the bracket used (mean paired ratio 1.0421, faster
// in 12 of 15, sign test p~0.035) — so the bracket was ~4% BELOW the path it
// was bounding, and was never an upper bound at all. The interlude's §4.1
// reinstates that idea; §4.3 then gates against each point's own measurement
// error rather than `1.0 + 1e-6`.
//
// The tolerance below therefore STAYS, but its expected frequency inverts:
// before the interlude this gate fired on roughly one healthy run in three,
// and after it, six consecutive `bench wire --target invoicing` runs produced
// ratios of 0.858-0.989 with the gate never firing. It is kept because a gate
// that CANNOT fire on a short window would be vacuous, not because firing is
// still routine — if it starts firing again on healthy runs, that is a
// finding, not the weather. `core_layer_commits_and_reports_a_ratio` carries
// the same tolerance one layer down for the same reason. Any OTHER reason
// still reddens: a 401 storm, a lost point, a disagreeing crosscheck and a
// starved heartbeat all carry different wording, and a LOST point
// additionally fails the `points.len()` assertion below, since a lost point
// publishes nothing.
//
// **T13 (Noah ruling (b)) — the tolerated reason is no longer reachable here,
// and the tolerance is kept on purpose.** invoicing reports no `/__syncs`, so
// its sync rate is derived; a derived rate is now withdrawn rather than gated,
// which means this target has no sync ratio to trip. Everything above is the
// history of why it once did. The loop stays because it is now a REGRESSION
// guard rather than weather-tolerance: if this reason appears again, a derived
// rate reached the gate, which is precisely what ruling (b) removed.
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

// ---------------------------------------------------------------------------
// T9: the forced INVALID arms (spec §7 — each arm proven by construction, not
// by waiting for a real run to happen to produce it).
//
// These two are deliberately NOT #[ignore]: neither spawns a process, binds a
// port, or writes a data directory. See this file's module doc.
// ---------------------------------------------------------------------------

#[test]
fn a_starved_heartbeat_flags_invalid_through_its_real_path() {
    // Threads only. Holding the gate starves the tick thread through the exact
    // mutex it takes every tick — the real path, not a mocked verdict.
    let hb = bench_harness::measure::Heartbeat::start();
    let gate = hb.gate();
    let held = gate.lock().expect("test holds the heartbeat gate");
    std::thread::sleep(Duration::from_millis(300));
    drop(held);
    let verdict = hb.finish().expect("a starved heartbeat must flag the run");
    assert!(verdict.contains("INVALID"), "got: {verdict}");
    // AMENDED at Task 4's re-review (2026-08-07, finding I-A): this rig starves
    // the heartbeat thread while the process stays AWAKE, which is the
    // STARVATION arm, not the suspension arm. T4's I-5 fix split the two and
    // reworded this reason accordingly, so asserting "suspended" here now
    // fails. Assert the arm this test actually exercises.
    assert!(verdict.contains("starved"), "got: {verdict}");
}

#[test]
fn a_saturated_client_is_flagged_on_the_point() {
    // The saturation rule is arithmetic on measured CPU (spec §4); force it by
    // construction — busy_fraction 1.0 against the 0.9 threshold — and assert
    // the RENDERING. `a_saturated_client_is_flagged_through_the_real_sweep`
    // below is the sibling that proves the CLASSIFICATION through wire::sweep's
    // own CPU arithmetic; this one would pass even if that formula were wrong,
    // which is exactly why both exist (plan-review I-4).
    let p = bench_harness::report::SweepPoint {
        connections: 1,
        throughput_per_sec: 1.0,
        log_records_per_sec: 2.0,
        // T13: a derived point publishes no rate. This test renders a
        // saturation flag and reads nothing from the sync cell, so the
        // withdrawn form is the right fixture rather than a concession.
        syncs_per_sec: None,
        syncs_counted: false,
        device_ratio: 0.5,
        device_appends_per_sec_before: 2.0,
        device_appends_per_sec_after: 2.0,
        p50_us: 1,
        p99_us: 1,
        p999_us: 1,
        client_busy_fraction: 1.0,
        saturated: 1.0 > bench_harness::wire_accounting::SATURATION_FRACTION,
    };
    assert!(
        p.saturated,
        "a fully-busy client must mark its point saturated"
    );
    let human = bench_harness::report::render_human(&bench_harness::report::Report {
        hardware: "h".into(),
        os: "o".into(),
        filesystem: "f".into(),
        cores: 1,
        sync_semantics: "s",
        build_profile: "dev",
        warmup_secs: 0.0,
        duration_secs: 0.0,
        device: Some(bench_harness::report::DeviceReport {
            syncs_per_sec: 1.0,
            appends_per_sec: 1.0,
        }),
        core: None,
        kernel: None,
        wire: vec![bench_harness::report::WireReport {
            target: "t".into(),
            points: vec![p],
        }],
        invalid: vec![],
    });
    assert!(human.contains("CLIENT-SATURATED"), "got: {human}");
}

// ---------------------------------------------------------------------------
// T9: the three that drive the `bench` BINARY. #[ignore] — they spawn
// processes and build crates.
//
// The first two close spec §7 bullet 1's real gap: every other smoke in this
// file calls library functions, so nothing else exercises the binary's own
// argv-parse / dispatch / exit-code contract, and a regression there would be
// invisible (plan-review C-2).
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn the_bench_binary_smokes_end_to_end() {
    let bin = bench_harness::wire::build_bin("bench-harness", "bench").expect("bench builds");
    let out = std::process::Command::new(&bin)
        .args(["smoke", "--json"])
        .output()
        .expect("bench smoke runs");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    // T13 (Noah ruling (d)): this used to assert `exit 0`, which was then a
    // condition this harness's own published documentation said could not hold
    // — `docs/BENCH.md` recorded that `invalid: []` was unreachable on the full
    // matrix, because invoicing mounts no `/__syncs`, its sync numerator fell
    // back to `syncs_per_request * throughput`, and group commit makes that
    // multiplier false once requests queue: the gate read ~2x the ceiling and
    // refused, correctly.
    //
    // **A gate that is permanently red for an expected reason is not a gate.**
    // A real regression in this smoke would have looked identical to the
    // known-expected failure, which is the whole cost. So this takes the shape
    // `wire_layer_drives_invoicing_through_login` already uses one layer down
    // (and `kernel_bench::syncs_are_measured_not_derived_under_batching` one
    // layer below that): tolerate the ONE named reason, redden on any other.
    //
    // **Ruling (b) then removed the reason itself, and this tolerance is kept
    // deliberately rather than left by accident.** A derived sync rate is now
    // withdrawn instead of gated, so on today's harness the invoicing refusal
    // this was written for cannot fire, and the `refusals.is_empty()` arm below
    // is the one a green run takes — the stronger assertion, back in force.
    // What the loop still buys is that it reddens on ANY reason, including the
    // one it names: if that refusal ever appears again it means a derived rate
    // reached the gate, which is the regression ruling (b) exists to prevent.
    //
    // **If a DIFFERENT reason appears here, do not read it as flakiness.** It
    // means one of device/core/kernel, or the COUNTED bench-app wire arm, has
    // stopped producing a measurement, and the reason text names which layer
    // and why. Note what this text match cannot see: the refusal below names
    // neither the target nor whether its sync rate was counted or derived, so
    // a bench-app point tripping the same gate would be tolerated here. That
    // case is covered, and deliberately not by this test —
    // `wire_layer_sweeps_the_bench_app` asserts `invalid.is_empty()` for that
    // target at the same 1/4 connection counts in this same `--ignored` batch.
    //
    // Reasons are read off `render_human`'s own `!! ` marker (report.rs:554),
    // the same contract this file's `a_starved_heartbeat_flags_invalid_through_its_real_path`
    // asserts against — this crate has no JSON parser, by design.
    let refusals: Vec<&str> = stderr
        .lines()
        .filter_map(|l| l.trim_start().strip_prefix("!! "))
        .collect();
    for reason in &refusals {
        assert!(
            reason.contains("durable syncs/s from")
                && reason.contains("the measured device ceiling"),
            "the only refusal this smoke tolerates is the sync-ratio one, and since T13's \
             ruling (b) even that should no longer be reachable — a derived sync rate is \
             withdrawn rather than gated, so seeing it here means a derived rate reached the \
             gate again. Any OTHER reason means a layer stopped measuring: \
             {reason}\nstderr:\n{stderr}\nstdout:\n{stdout}"
        );
    }
    // The exit contract, tied to the refusals rather than asserted blind: no
    // reasons must mean exit 0, and reasons must mean exit 1. A smoke that
    // refuses a point and still exits 0 is the failure `exit_code_for` exists
    // to prevent, and tolerating the reason above must not tolerate that too.
    if refusals.is_empty() {
        assert!(
            out.status.success(),
            "bench smoke published no refusal, so it must exit 0\nstderr:\n{stderr}\nstdout:\n{stdout}"
        );
    } else {
        assert_eq!(
            out.status.code(),
            Some(1),
            "bench smoke published {} refusal(s), so it must exit 1\nstderr:\n{stderr}\nstdout:\n{stdout}",
            refusals.len()
        );
    }
    // All four layers assembled into ONE report — this is what `all`/`smoke`
    // add over the single-layer subcommands, and the only place it is checked.
    for key in [
        "\"bench\":{",
        "\"device\":{",
        "\"core\":{",
        "\"kernel\":{",
        "\"wire\":[",
    ] {
        assert!(stdout.contains(key), "smoke JSON missing {key}: {stdout}");
    }
    // Both wire targets ran, not just the default one.
    assert!(
        stdout.contains("\"target\":\"bench-app\"") && stdout.contains("\"target\":\"invoicing\""),
        "smoke must sweep BOTH wire targets: {stdout}"
    );
    // The smoke override reached the artifact. Without this the run could
    // silently use the 5 s + 30 s defaults and still pass everything above —
    // which is the whole failure mode Step 3's "select before the device run"
    // ordering exists to prevent.
    assert!(
        stdout.contains("\"duration_secs\":1.00"),
        "smoke's forced 1 s duration must reach the report: {stdout}"
    );
    assert!(
        stdout.contains("\"warmup_secs\":0.20"),
        "smoke's forced 0.2 s warmup must reach the report: {stdout}"
    );
}

#[test]
#[ignore]
fn a_bogus_subcommand_is_a_usage_error() {
    let bin = bench_harness::wire::build_bin("bench-harness", "bench").expect("bench builds");
    let out = std::process::Command::new(&bin)
        .arg("bogus")
        .output()
        .expect("bench bogus runs");
    assert_eq!(out.status.code(), Some(2), "usage errors exit 2");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("usage: bench"), "got: {stderr}");
}

#[test]
#[ignore]
fn a_saturated_client_is_flagged_through_the_real_sweep() {
    // Forces client_busy_fraction through wire::sweep's REAL formula
    // (cpu / (n * elapsed)) rather than a hand-built literal: two spinner
    // threads burn this process's CPU during a 1-conn sweep, driving the
    // measured fraction well above 0.9. `a_saturated_client_is_flagged_on_the_point`
    // proves the RENDERING; this proves the CLASSIFICATION (plan-review I-4).
    let bin = bench_harness::wire::build_bin("bench-harness", "bench-app").expect("bin builds");
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let spinners: Vec<_> = (0..2)
        .map(|_| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut x = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    x = x
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                }
                std::hint::black_box(x);
            })
        })
        .collect();
    let result = bench_harness::wire::sweep(
        &bin,
        bench_harness::wire::Target::BenchApp,
        &[1],
        Duration::from_millis(200),
        Duration::from_secs(1),
    );
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for s in spinners {
        let _ = s.join();
    }
    let (wire, _invalid) = result.expect("sweep runs");
    assert!(
        wire.points[0].saturated,
        "2 spinner threads over a 1-conn sweep must saturate the client: busy fraction {}",
        wire.points[0].client_busy_fraction
    );
}
