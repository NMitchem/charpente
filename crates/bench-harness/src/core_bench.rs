//! Layer 2 (spec §2): Db::transact in a closed loop against the prod Disk —
//! the commit path alone (WAL append + sync + apply + store materialization).
//! The spec's Context prediction is about this module's number.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::device::timed_loop;
use crate::measure::{Heartbeat, crosscheck, degenerate_window_reason};
use crate::report::CoreReport;
use crate::{Bump, SLOTS, Slot, SlotBumped, schema};
use charpente::core::io::Disk;
use charpente::prod::ProdDisk;
use charpente::{Db, DbConfig, Id, TxError, View};
use std::io;
use std::time::{Duration, Instant};

fn canonical(db: &mut Db, i: u64) -> io::Result<u64> {
    db.transact(|tx| -> Result<u64, io::Error> {
        let slot_id = Id::<Slot>::from_raw(i % SLOTS);
        let hits = tx.get(slot_id)?.map_or(0, |s| s.hits) + 1;
        tx.put(&Slot { id: slot_id, hits });
        // T5 review, M-8: this job row is never drained — no Kernel runs at
        // this layer, so nothing services `charpente.jobs`, and the table
        // accumulates one unserviced row per measured op (≈10k+ over a
        // default 30s run). Investigated (2026-08-08, two throwaway
        // experiments, deleted before commit): 8 reopen rounds and one
        // continuous 40×1s-subwindow run showed NO monotonic throughput
        // decline as the backlog grew to ~10,330 rows — at this workload's
        // F_FULLFSYNC-bound rate (~200-300/s), each transact's cost is
        // dominated by the durable sync, and redb's insert cost is swamped
        // by comparison at the durations tested. NOT claimed to generalize
        // to a much larger backlog, longer runs, or faster storage, where
        // the backlog's relative cost would grow — untested, not ruled out.
        tx.enqueue(Bump { slot: i % SLOTS });
        tx.emit(SlotBumped { slot: i % SLOTS });
        Ok(hits)
    })
    .map_err(|e| match e {
        TxError::Io(io) => io,
        TxError::App(io) => io,
        other => io::Error::other(format!("unexpected transact refusal: {other:?}")),
    })
}

/// How much play `device_ratio` may have above 1.0 before it is flagged (T5
/// review, C-1 / Noah ruling #1). `Engine::submit` performs exactly one
/// `wal.sync()` per transaction with no group commit, so the commit path
/// cannot durably ack faster than the device's own sync rate in fact — a
/// ratio above 1.0 is IMPOSSIBLE, not merely unlikely, so anything above it
/// is a measurement artifact worth flagging, not a real durability finding.
///
/// T5 re-review, N-4 / Noah ruling #4 (fixed here): the original 1e-2 was
/// justified as covering "floating-point/timer-resolution noise" — that
/// justification was wrong by roughly seven orders of magnitude. Over a
/// 1s `Instant`-measured window, timer resolution contributes ~1e-9 relative
/// error and f64 arithmetic ~1e-16; 1e-2 instead absorbed a band of
/// cross-window SAMPLING noise (single-sample dispersion of 220-310 syncs/s
/// was measured across otherwise-identical runs), which is precisely the
/// phenomenon this gate exists to detect. A ratio anywhere in `(1.0, 1.01]`
/// published unflagged at exit 0 was C-1's exact defect, one order of
/// magnitude smaller, still shipped. Tightened to 1e-6 — still generous for
/// genuine float/timer slop, the only cause this constant is meant to
/// excuse. The accepted cost: more short/noisy runs now exit 1; those runs
/// ARE genuinely invalid measurements, and the bracket samples
/// (device_syncs_per_sec_before/_after) are what tell the reader why, not a
/// wider slack that hides the same fact.
const DEVICE_RATIO_SLACK: f64 = 1e-6;

/// `None` = plausible; `Some(reason)` = INVALID, naming BOTH candidate
/// causes rather than asserting one (the review measured 220-310 syncs/s
/// spread across otherwise-identical runs — noise comparable to the whole
/// signal the ratio is supposed to carry).
fn device_ratio_reason(ratio: f64, denominator: f64, before: f64, after: f64) -> Option<String> {
    if ratio > 1.0 + DEVICE_RATIO_SLACK {
        Some(format!(
            "INVALID: device_ratio {ratio:.3} exceeds 1.0 + {DEVICE_RATIO_SLACK} — \
             Engine::submit performs exactly one wal.sync() per transaction with no group \
             commit, so the commit path cannot durably ack faster than the device's own sync \
             rate; a ratio above 1.0 is a measurement artifact rather than a real durability \
             finding. Two candidate causes, either or both: (1) cross-window variance — \
             device.syncs_per_sec ({denominator:.2}) is a single sample from a separate \
             invocation, and this run's own bracket samples ({before:.2} before / {after:.2} \
             after the write window) show the spread that single sample cannot; (2) the \
             ratio's denominator (device.syncs_per_sec, the one-byte-overwrite loop) is not \
             the same op shape as the canonical commit's WAL append — device.appends_per_sec \
             is closer and may be exceeded less, or not at all"
        ))
    } else {
        None
    }
}

/// A short, independent device-sync sample on the SAME directory the core
/// write window measures (T5 review, C-1 / Noah ruling #1) — reuses
/// `device::timed_loop` rather than hand-rolling a second sync loop. Fixed
/// (not user-configurable) duration: this is a bracket around the write
/// window, not a second `bench device` invocation, and a full spec-default
/// device run (5s warmup + 30s duration) on both sides would roughly triple
/// `bench core`'s wall time for a supporting measurement.
const BRACKET_WARMUP: Duration = Duration::from_millis(100);
const BRACKET_DURATION: Duration = Duration::from_secs(1);

fn device_bracket_sample(dir: &str) -> io::Result<f64> {
    let disk = ProdDisk::new(dir)?;
    let file = disk.open("bench-core-bracket.dat")?;
    // T5 re-review, N-11 (Minor, fixed here): `timed_loop` formats its
    // degenerate-window message as "device {name}", so passing "core
    // bracket" here rendered "the device core bracket loop" — a doubled
    // "device" reading. "bracket" alone reads as "the device bracket loop".
    timed_loop("bracket", BRACKET_WARMUP, BRACKET_DURATION, || {
        file.write_at(0, &[1u8])?;
        file.sync()
    })
}

pub fn run(
    dir: &str,
    warmup: Duration,
    duration: Duration,
    device_syncs_per_sec: f64,
) -> io::Result<(CoreReport, Vec<String>)> {
    let mut db = Db::open(Box::new(ProdDisk::new(dir)?), DbConfig::default(), schema())?;
    let mut invalid = Vec::new();
    let mut i = 0u64;

    let warm_end = Instant::now() + warmup;
    while Instant::now() < warm_end {
        canonical(&mut db, i)?;
        i += 1;
    }

    // T5 review, C-1 / Noah ruling #1: bracket the measured write window
    // with an independent device-sync sample on either side.
    //
    // T5 re-review, N-2 (Important, fixed here): the bracket samples are
    // PUBLISHED artifact fields (device_syncs_per_sec_before/_after) — I-1's
    // whole point was that a measured, published window needs a suspension/
    // starvation guard, and the original fix covered only the write loop
    // itself, leaving these two new sibling windows uncovered in the same
    // commit that added them. `hb` now starts before the first bracket
    // sample and finishes after the second, so a suspend or starvation
    // episode during either bracket is caught exactly like one during the
    // write loop.
    let hb = Heartbeat::start();
    let device_before = device_bracket_sample(dir)?;

    let start = Instant::now();
    let end = start + duration;
    let measured_from = i;
    let mut grand = 0u64;
    // T5 review, I-3: log_records_per_sec is a MEASUREMENT of
    // Db::last_applied() sampled either side of the window, not a copy of
    // acked_writes_per_sec — see the crosscheck below.
    let applied_before = db.last_applied();
    while Instant::now() < end {
        canonical(&mut db, i)?;
        i += 1;
        grand += 1;
    }
    let elapsed = start.elapsed();
    let applied_after = db.last_applied();
    let device_after = device_bracket_sample(dir)?;
    if let Some(reason) = hb.finish() {
        invalid.push(reason);
    }
    // T5 review, C-3 / Noah ruling #2: refuse a degenerate write window by
    // name — parity with device::timed_loop — before anything downstream
    // treats `grand` as a real measurement. The heartbeat is stopped above
    // regardless, so an early return here does not leak its tick thread.
    if let Some(reason) = degenerate_window_reason("core write", grand, duration) {
        return Err(io::Error::other(reason));
    }
    // Totals two ways (spec §5): the loop counter delta vs the grand counter.
    // Structurally vacuous today — both increment in the same statement of
    // the same single-threaded loop — kept so every layer's throughput has a
    // uniform cross-check shape; it becomes a real check only if this loop
    // ever gains concurrency (the wire/kernel layers' cross-checks are the
    // genuinely independent ones).
    match crosscheck(&[i - measured_from], grand) {
        Ok(_) => {}
        Err(reason) => invalid.push(reason),
    }
    let acked = grand as f64 / elapsed.as_secs_f64();

    // I-3's cross-check: the canonical transaction is documented (below) to
    // commit exactly one effects record per transact, so the applied-seq
    // delta must equal the acked-write count.
    //
    // T5 re-review, N-1 (Critical, fixed here): WAL seqs are 0-based
    // (log.rs's next_seq starts at 0), so `Db::last_applied()` returning
    // `None` means "zero records applied" while `Some(0)` means "one record
    // applied" (seq 0 itself). The prior `unwrap_or(0)` treated both the
    // same — reading "no records applied yet" (a fresh Db, warmup == 0) as
    // "record 0 already applied" — which undercounted the delta by exactly
    // one on every such run and published a wrong log_records_per_sec plus a
    // false "contract broke" INVALID below. Matched explicitly instead: a
    // `None` before must become `Some(a) + 1`, not `unwrap_or(0)`'s implicit
    // "start counting from the record that doesn't exist".
    let applied_delta = match (applied_before, applied_after) {
        (Some(b), Some(a)) => a - b,
        (None, Some(a)) => a + 1,
        (None, None) => 0,
        (Some(_), None) => unreachable!("last_applied cannot go backwards to None"),
    };
    if applied_delta != grand {
        invalid.push(format!(
            "INVALID: log_records_per_sec measured {applied_delta} applied records against \
             {grand} acked writes — core_bench's canonical transaction is documented to \
             commit exactly one effects record per transact (no dispatcher runs at this \
             layer, so nothing doubles it with a job Done-mark), so these must agree; a \
             mismatch means that contract broke"
        ));
    }
    let log_records_per_sec = applied_delta as f64 / elapsed.as_secs_f64();

    let device_ratio = acked / device_syncs_per_sec;
    if let Some(reason) = device_ratio_reason(
        device_ratio,
        device_syncs_per_sec,
        device_before,
        device_after,
    ) {
        invalid.push(reason);
    }

    // Reads: one snapshot, typed gets. No log involvement — this number shows
    // where the write ceiling does NOT apply (spec §3). Measured under the
    // SAME warmup/duration the caller passed: the Report's warmup_secs/
    // duration_secs fields are global, and a silently different read window
    // would belie them (spec §5 — fields, not caveats buried in code).
    //
    // T5 review, I-2: spec §3 says "typed gets against a POPULATED store" —
    // population was previously an accident of how many writes the window
    // above happened to complete (up to +73% inflated reads/sec on a short
    // run, since a miss is cheaper than a hit over a smaller tree). Populate
    // every key in 0..SLOTS here, OUTSIDE any measured window and outside
    // `grand`/`acked` — this is setup, not a measurement.
    //
    // T5 re-review, N-2 (Important, fixed here): `read_hb` now starts BEFORE
    // this population loop rather than after it. Population can run for
    // several seconds of real fsyncs (I-2's own cost account), and a sleep
    // or starvation episode there would disturb the machine for the read
    // window that follows with nothing able to see it — the same gap I-1
    // was reopened to close, one loop earlier.
    let read_hb = Heartbeat::start();
    for slot in 0..SLOTS {
        db.transact(|tx| -> Result<(), io::Error> {
            let slot_id = Id::<Slot>::from_raw(slot);
            if tx.get(slot_id)?.is_none() {
                tx.put(&Slot {
                    id: slot_id,
                    hits: 0,
                });
            }
            Ok(())
        })
        .map_err(|e| match e {
            TxError::Io(io) => io,
            TxError::App(io) => io,
            other => io::Error::other(format!("unexpected transact refusal: {other:?}")),
        })?;
    }

    let snap = db.snapshot()?;
    // T5 review, I-1: `read_hb` (started above, before the population loop)
    // covers the read warmup and the read window too — symmetric with the
    // write window's own `hb`, and now spanning everything measured or
    // published in this half of the layer, per N-2 above.
    let read_warm_end = Instant::now() + warmup;
    let mut r = 0u64;
    while Instant::now() < read_warm_end {
        let _ = snap.get(Id::<Slot>::from_raw(r % SLOTS))?;
        r += 1;
    }
    let read_start = Instant::now();
    let read_end = read_start + duration;
    let mut reads = 0u64;
    // T5 review, C-2: count HITS, not just read attempts — every key in
    // 0..SLOTS is populated above, so under a working read loop every read
    // is a hit. The mutation the review demonstrated (a loop body reduced to
    // `reads += 1;` with no `get` call, publishing 42M "reads/sec" from a
    // loop touching the store zero times) leaves `hits` at 0 while `reads`
    // climbs into the millions — caught below, from inside `run`, not by a
    // test-side bound that a portable rate check could not substitute for.
    let mut hits = 0u64;
    while Instant::now() < read_end {
        if snap.get(Id::<Slot>::from_raw(reads % SLOTS))?.is_some() {
            hits += 1;
        }
        reads += 1;
    }
    let read_elapsed = read_start.elapsed();
    if let Some(reason) = read_hb.finish() {
        invalid.push(reason);
    }
    // T5 re-review, N-6 (Minor): this arm is UNREACHABLE through `run`'s
    // public signature, not merely unproven by real timing — `run` drives
    // both windows from the same `duration`, reads run on the order of
    // 1.1M/s (needing a window under ~4.5us to trip this floor) while
    // writes run on the order of 250/s (tripping the write arm above under
    // ~20ms), three to four orders of magnitude earlier. Any duration small
    // enough to reach here would already have returned Err from the write
    // arm. Kept for parity with `device::timed_loop`'s shared floor (Noah
    // ruling #2 asked for BOTH core loops to carry it) and because it is
    // harmless — but it is the fifth assertion this sub-project has found
    // that cannot fail through the path that reaches it; it would only
    // become live if `run` grew separate read/write durations.
    if let Some(reason) = degenerate_window_reason("core read", reads, duration) {
        return Err(io::Error::other(reason));
    }
    if hits != reads {
        return Err(io::Error::other(format!(
            "INVALID: the core read loop completed {reads} reads but only {hits} were hits \
             against a fully populated store (every key in 0..{SLOTS} is written before the \
             read window, spec §3) — either population failed or this loop stopped \
             exercising the store"
        )));
    }
    let reads_per_sec = reads as f64 / read_elapsed.as_secs_f64();

    Ok((
        CoreReport {
            acked_writes_per_sec: acked,
            log_records_per_sec,
            reads_per_sec,
            device_ratio,
            device_ratio_denominator: "device.syncs_per_sec",
            device_syncs_per_sec_before: device_before,
            device_syncs_per_sec_after: device_after,
        },
        invalid,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // T5 review, C-1 / Noah ruling #1: a physically-impossible ratio must be
    // named, not silently published. Pure function — no hardware timing
    // dependency — so this asserts the gate itself, independent of whether
    // a real run happens to reproduce a >1.0 ratio (the review saw it 2 of
    // 7 runs on a quiet machine, not reliably reproducible on demand).
    #[test]
    fn a_ratio_above_one_plus_slack_is_named_invalid() {
        let r = device_ratio_reason(1.05, 250.0, 245.0, 255.0).expect("must flag");
        assert!(r.contains("INVALID"), "got: {r}");
        assert!(r.contains("1.050"), "must name the offending value: {r}");
        assert!(
            r.contains("cross-window variance"),
            "must name candidate cause 1: {r}"
        );
        assert!(
            r.contains("not the same op shape"),
            "must name candidate cause 2: {r}"
        );
        assert!(
            r.contains("device.syncs_per_sec"),
            "must name which operand the ratio divides by: {r}"
        );
        assert!(r.contains("245.00"), "must show the before sample: {r}");
        assert!(r.contains("255.00"), "must show the after sample: {r}");
    }

    #[test]
    fn a_ratio_at_or_below_the_slack_boundary_is_not_flagged() {
        assert!(device_ratio_reason(1.0, 250.0, 245.0, 255.0).is_none());
        assert!(
            device_ratio_reason(1.0 + DEVICE_RATIO_SLACK, 250.0, 245.0, 255.0).is_none(),
            "the boundary itself must not flag — only strictly above it"
        );
        assert!(device_ratio_reason(0.79, 250.0, 245.0, 255.0).is_none());
    }

    #[test]
    fn a_ratio_just_over_the_slack_boundary_is_flagged() {
        assert!(
            device_ratio_reason(1.0 + DEVICE_RATIO_SLACK + 0.001, 250.0, 245.0, 255.0).is_some()
        );
    }
}
