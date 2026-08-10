//! Layer 2 (spec §2): Db::transact in a closed loop against the prod Disk —
//! the commit path alone (WAL append + sync + apply + store materialization).
//! The spec's Context prediction is about this module's number.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::core_accounting::{self, CoreObservations};
use crate::device::device_bracket_sample;
use crate::measure::Heartbeat;
use crate::report::CoreReport;
use crate::{Bump, SLOTS, Slot, SlotBumped, schema};
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

/// Interlude spec §4.2 (Noah ruling, 2026-08-10): this no longer takes layer
/// 1's `device.syncs_per_sec`. The gate divides by the brackets taken either
/// side of this function's own write window, so a one-shot sample from a
/// separate invocation is no longer an input to any decision here.
pub fn run(
    dir: &str,
    warmup: Duration,
    duration: Duration,
) -> io::Result<(CoreReport, Vec<String>)> {
    let mut db = Db::open(Box::new(ProdDisk::new(dir)?), DbConfig::default(), schema())?;
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
    obs.write.elapsed = start.elapsed();
    obs.write.applied_after = db.last_applied();
    obs.write.device_after = device_bracket_sample(dir)?;
    obs.write.heartbeat = hb.finish();
    obs.write.ops = grand;
    obs.write.loop_delta = i - measured_from;

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
            device_ratio: write.device_ratio,
            device_ratio_denominator: "the device bracket max",
            device_appends_per_sec_before: obs.write.device_before,
            device_appends_per_sec_after: obs.write.device_after,
        },
        invalid,
    ))
}
