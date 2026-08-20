//! Layer 2 (spec §2): Db::transact in a closed loop against the prod Disk —
//! the commit path alone (WAL append + sync + apply + store materialization).
//! The spec's Context prediction is about this module's number.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::core_accounting::{self, CoreObservations, WriteObs};
use crate::device::device_bracket_sample;
use crate::measure::Heartbeat;
use crate::phase_accounting::{attribute, window_brackets_measured_section};
use crate::report::CoreReport;
use crate::tracing_disk::{MAX_TRACE_CAPACITY, Trace, TracingDisk};
use crate::{Bump, SLOTS, Slot, SlotBumped, schema};
use charpente::prod::ProdDisk;
use charpente::{Db, DbConfig, Id, TxError, View};
use std::io;
use std::time::{Duration, Instant};

/// Floor on the trace's preallocated event vec (Task 1: `size_of::<Event>()`
/// is 24 bytes, so this is ~25 MB). Covers this layer's fixed per-run cost —
/// `Db::open` and the store materialization at entry — which does not scale
/// with `--duration`, and keeps every run at or below the measured default
/// allocating exactly what it allocated before this constant became a floor.
const MIN_TRACE_CAPACITY: usize = 1 << 20;

/// Events per second of wall clock, with margin (Task 3 review, M-1).
///
/// The Ruling B doc this replaces claimed this arm "has no workload input to
/// derive a capacity FROM." That was false: `--duration` is one, `--warmup` is
/// another, and both are already parameters of `run`. The consequence of the
/// false claim was a fixed ceiling a long run silently walks past —
/// `bench core --warmup 30 --duration 600` produces ~2M events against a
/// 1,048,576 reservation and loses its phase block to a correct, loud refusal
/// it never needed to trigger.
///
/// The rate is measured, not guessed: the capacity finding recorded 94,562
/// events over a 30 s window (see `tracing_disk::Trace::capacity_grew_reason`),
/// ~3,150/s, and Task 3's review independently put it near 2,900/s. 6,000
/// carries ~90% headroom over the higher of the two — the same posture as
/// `kernel_bench::EVENTS_PER_REQUEST_ESTIMATE`, and for the same reason: a
/// different `SLOTS`/schema shape could move the real rate without this
/// crate's own fixtures changing at all.
const EVENTS_PER_SEC_ESTIMATE: usize = 6_000;

/// Sized from the run's own duration, floored, and capped at
/// `MAX_TRACE_CAPACITY` (Task 3 review, M-1 and M-3).
///
/// `+ 2` because both `as_secs()` truncate, so a `--warmup 0.9 --duration 1.9`
/// run must not be sized as if it were 0 s + 1 s. Saturating throughout: an
/// absurd duration must not panic this arithmetic before the run starts, and
/// the clamp is what keeps an absurd one from requesting an unservable
/// allocation — past the cap the reservation stops holding and
/// `Trace::capacity_grew_reason` withholds the phase block loudly, which is a
/// far better outcome than aborting the process on allocation failure.
fn trace_capacity(warmup: Duration, duration: Duration) -> usize {
    let secs = warmup
        .as_secs()
        .saturating_add(duration.as_secs())
        .saturating_add(2) as usize;
    secs.saturating_mul(EVENTS_PER_SEC_ESTIMATE)
        .clamp(MIN_TRACE_CAPACITY, MAX_TRACE_CAPACITY)
}

// `clamp` panics if the bounds invert. Both are consts, so prove it at compile
// time rather than discovering it from a bench run.
const _: () = assert!(MIN_TRACE_CAPACITY <= MAX_TRACE_CAPACITY);

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

/// Interlude spec §4.2 (Noah ruling, 2026-08-10): this no longer takes layer
/// 1's `device.syncs_per_sec`. The gate divides by the brackets taken either
/// side of this function's own write window, so a one-shot sample from a
/// separate invocation is no longer an input to any decision here.
pub fn run(
    dir: &str,
    warmup: Duration,
    duration: Duration,
) -> io::Result<(CoreReport, Vec<String>)> {
    let trace = Trace::recording(trace_capacity(warmup, duration));
    let mut db = Db::open(
        Box::new(TracingDisk::new(
            Box::new(ProdDisk::new(dir)?),
            trace.clone(),
        )),
        DbConfig::default(),
        schema(),
    )?;
    let mut invalid = Vec::new();
    let mut i = 0u64;
    // The observations seam (interlude spec §1). Every count, duration and
    // verdict this function takes is recorded here, and every decision about
    // them is made in `core_accounting` — this function makes no arithmetic
    // decision of its own. Filled in two stages so the refusal points do not
    // move (spec §2): the write half is accounted BEFORE the population loop,
    // which can cost seconds of real fsyncs on a run about to be discarded.
    let mut obs = CoreObservations {
        duration,
        ..Default::default()
    };
    let warm_end = Instant::now() + warmup;
    while Instant::now() < warm_end {
        canonical(&mut db, i)?;
        i += 1;
    }

    // T5 review, C-1 / Noah ruling #1: bracket the measured write window
    // with an independent device-sync sample on either side.
    //
    // T5 re-review, N-2 (Important): the bracket samples are PUBLISHED
    // artifact fields (device_appends_per_sec_before/_after) — I-1's whole
    // point was that a measured, published window needs a suspension/
    // starvation guard, and the original fix covered only the write loop
    // itself, leaving these two new sibling windows uncovered in the same
    // commit that added them. `hb` starts before the first bracket sample and
    // finishes after the second, so a suspend or starvation episode during
    // either bracket is caught exactly like one during the write loop.
    let hb = Heartbeat::start();
    obs.write.device_before = device_bracket_sample(dir)?;

    // Task 3 (2026-08-19 phase-attribution plan): stamped from the trace's
    // OWN clock (`Trace::now_nanos`), not a second `Instant`, so
    // `phase_accounting::attribute` can compare these against every
    // recorded `Event`'s timestamps directly. Taken at the true edge of the
    // measured section — no traced op is in flight at this instant on a
    // single-threaded arm.
    let window_start_nanos = trace.now_nanos();
    let start = Instant::now();
    let end = start + duration;
    let measured_from = i;
    let mut grand = 0u64;
    // T5 review, I-3: log_records_per_sec is a MEASUREMENT of
    // Db::last_applied() sampled either side of the window, not a copy of
    // acked_writes_per_sec — see `core_accounting::write_accounting`.
    obs.write.applied_before = db.last_applied();
    while Instant::now() < end {
        canonical(&mut db, i)?;
        i += 1;
        grand += 1;
    }
    let elapsed = start.elapsed();
    let window_end_nanos = trace.now_nanos();

    // Task 3 review, M-2: the OTHER half of the window-integrity gate. The
    // `events_ignored_after_window` check below catches a window stamped too
    // narrow; nothing caught one stamped too wide, and a `window_start_nanos`
    // hoisted above the warmup loop folds every warmup fsync into the
    // published shares while every count stays put and the closure invariant
    // still holds exactly. `elapsed` is the second, independent clock that
    // makes that visible.
    if let Some(reason) = window_brackets_measured_section(
        window_start_nanos,
        window_end_nanos,
        elapsed.as_nanos() as u64,
    ) {
        invalid.push(reason);
    }
    // Ruling A (2026-08-19 phase-attribution plan): checked BEFORE attribute()
    // is even attempted. A grown buffer does not corrupt any recorded event's
    // own timestamps — attribute() would still return `Ok` and its closure
    // invariant would still hold — the contamination is invisible to that
    // function by construction (see `Trace::capacity_grew_reason`'s doc), so
    // there is nothing for attribute() to usefully check here; the refusal
    // has to happen at this layer instead.
    let phases = if let Some(reason) = trace.capacity_grew_reason() {
        invalid.push(reason);
        None
    } else {
        let events = trace.events();
        match attribute(&events, window_start_nanos, window_end_nanos) {
            Ok(attribution) => {
                // The payoff of fix round 3's R-1: a window stamped from the
                // wrong place no longer fails loudly, it dilutes quietly. Events
                // before the window are warmup and `Db::open` — expected. Events
                // AFTER it are not, on an arm whose window closes at the end of
                // its run, so they mean the window and the trace disagree about
                // what was measured.
                if attribution.events_ignored_after_window > 0 {
                    invalid.push(format!(
                        "contract: {} traced op(s) fall after the measured window closed. On \
                         this arm the window closes at the end of the run, so this means \
                         `window_end_nanos` was stamped before the last IO completed and the \
                         published phase shares would be diluted by the ops left out. Fix: \
                         stamp `window_end_nanos` with `Trace::now_nanos()` after the measured \
                         section finishes, not before.",
                        attribution.events_ignored_after_window
                    ));
                }
                Some(attribution.totals)
            }
            Err(refusals) => {
                invalid.extend(refusals);
                None
            }
        }
    };

    // C-2 fix (2026-08-10 group-commit interlude, T1 fix wave): a FULL
    // struct literal for the fields settled at the end of the write window,
    // not `obs.write.field = ...` trailing mutations — the shape that let
    // `syncs` silently stay at `WriteObs::default()`'s 0 with the compiler
    // saying nothing (review C-2, "a trailing statement 40 lines below a
    // `..Default::default()` is the least obvious form available"). Every
    // field is named here, so a future field added to `WriteObs`, or a
    // future edit that drops one of these, breaks the build instead of
    // silently zeroing a gate input. `applied_before`/`device_before` were
    // already set above, before the loop.
    obs.write = WriteObs {
        applied_before: obs.write.applied_before,
        device_before: obs.write.device_before,
        elapsed,
        applied_after: db.last_applied(),
        device_after: device_bracket_sample(dir)?,
        heartbeat: hb.finish(),
        ops: grand,
        loop_delta: i - measured_from,
        // `canonical` above is one `Db::transact` per op with no batching, so
        // one acked write costs exactly one fsync today. A future
        // group-commit primary path is where this stops being a copy of
        // `ops`.
        syncs: grand,
    };

    // May refuse — and refuses HERE, before the population loop below, which
    // is the ordering spec §2 requires be preserved.
    let write =
        core_accounting::write_accounting(obs.duration, &obs.write).map_err(io::Error::other)?;
    invalid.extend(write.invalid);

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
    // T5 re-review, N-2 (Important): `read_hb` starts BEFORE this population
    // loop rather than after it. Population can run for several seconds of
    // real fsyncs (I-2's own cost account), and a sleep or starvation episode
    // there would disturb the machine for the read window that follows with
    // nothing able to see it — the same gap I-1 was reopened to close, one
    // loop earlier.
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
    // T5 review, C-2: count HITS, not just read attempts — the invariant that
    // consumes them lives in `core_accounting::read_accounting`.
    let mut hits = 0u64;
    while Instant::now() < read_end {
        if snap.get(Id::<Slot>::from_raw(reads % SLOTS))?.is_some() {
            hits += 1;
        }
        reads += 1;
    }
    obs.read.elapsed = read_start.elapsed();
    obs.read.heartbeat = read_hb.finish();
    obs.read.reads = reads;
    obs.read.hits = hits;

    let read =
        core_accounting::read_accounting(obs.duration, &obs.read).map_err(io::Error::other)?;
    invalid.extend(read.invalid);

    Ok((
        CoreReport {
            acked_writes_per_sec: write.acked_writes_per_sec,
            log_records_per_sec: write.log_records_per_sec,
            reads_per_sec: read.reads_per_sec,
            syncs_per_sec: write.syncs_per_sec,
            device_ratio: write.device_ratio,
            device_ratio_denominator: "the device bracket max",
            device_appends_per_sec_before: obs.write.device_before,
            device_appends_per_sec_after: obs.write.device_after,
            phases,
            window_nanos: window_end_nanos - window_start_nanos,
        },
        invalid,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Task 3 review, M-1: the fixed ceiling this formula replaces was
    // defensible only for the two run lengths that had been measured. These
    // pin the three things the replacement has to get right.

    #[test]
    fn the_measured_default_run_is_unchanged_by_the_formula() {
        // `bin/bench.rs` defaults: 5 s warmup + 30 s duration. The floor
        // governs there, so every run at or below the default allocates
        // exactly what it allocated before this became a formula — the
        // capacity finding's 94,562 measured events against 1,048,576.
        let capacity = trace_capacity(Duration::from_secs(5), Duration::from_secs(30));
        assert_eq!(capacity, MIN_TRACE_CAPACITY);
        assert!(
            capacity > 94_562 * 2,
            "the default run's measured event count must clear the floor with margin"
        );
    }

    #[test]
    fn a_long_run_is_sized_from_its_duration_rather_than_refused() {
        // The case the old fixed ceiling got wrong: at ~3,150 events/s a
        // 630 s run produces ~2M events, nearly double `MIN_TRACE_CAPACITY`.
        let capacity = trace_capacity(Duration::from_secs(30), Duration::from_secs(600));
        assert!(
            capacity > MIN_TRACE_CAPACITY,
            "a 630 s run must not be sized by the floor"
        );
        assert!(
            capacity as f64 > 630.0 * 3_150.0,
            "capacity {capacity} must clear the measured ~3,150 events/s rate"
        );
    }

    #[test]
    fn sub_second_durations_are_rounded_up_not_truncated() {
        // Both `as_secs()` truncate, so a run must never be sized shorter than
        // it is. Tested above the floor, which would otherwise mask the
        // difference by collapsing both sides onto itself.
        let fractional = trace_capacity(Duration::from_millis(199_900), Duration::from_secs(200));
        assert!(
            fractional >= 400 * EVENTS_PER_SEC_ESTIMATE,
            "199.9 s + 200 s must size as at least 400 s, not the 399 s that \
             truncating both operands would give: {fractional}"
        );
    }

    #[test]
    fn an_absurd_duration_is_clamped_rather_than_aborting_the_process() {
        // Same M-3 reasoning as the kernel arm: saturating arithmetic that
        // does not panic still requests an unservable allocation.
        assert_eq!(
            trace_capacity(Duration::from_secs(u64::MAX), Duration::from_secs(u64::MAX)),
            MAX_TRACE_CAPACITY
        );
    }
}
