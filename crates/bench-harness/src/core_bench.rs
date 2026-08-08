//! Layer 2 (spec §2): Db::transact in a closed loop against the prod Disk —
//! the commit path alone (WAL append + sync + apply + store materialization).
//! The spec's Context prediction is about this module's number.
//! Edge tooling outside the deterministic core (DESIGN §1).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::measure::{Heartbeat, crosscheck};
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

    let hb = Heartbeat::start();
    let start = Instant::now();
    let end = start + duration;
    let measured_from = i;
    let mut grand = 0u64;
    while Instant::now() < end {
        canonical(&mut db, i)?;
        i += 1;
        grand += 1;
    }
    let elapsed = start.elapsed();
    if let Some(reason) = hb.finish() {
        invalid.push(reason);
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

    // Reads: one snapshot, typed gets. No log involvement — this number shows
    // where the write ceiling does NOT apply (spec §3). Measured under the
    // SAME warmup/duration the caller passed: the Report's warmup_secs/
    // duration_secs fields are global, and a silently different read window
    // would belie them (spec §5 — fields, not caveats buried in code).
    let snap = db.snapshot()?;
    let read_warm_end = Instant::now() + warmup;
    let mut r = 0u64;
    while Instant::now() < read_warm_end {
        let _ = snap.get(Id::<Slot>::from_raw(r % SLOTS))?;
        r += 1;
    }
    let read_start = Instant::now();
    let read_end = read_start + duration;
    let mut reads = 0u64;
    while Instant::now() < read_end {
        let _ = snap.get(Id::<Slot>::from_raw(reads % SLOTS))?;
        reads += 1;
    }
    let reads_per_sec = reads as f64 / read_start.elapsed().as_secs_f64();

    Ok((
        CoreReport {
            acked_writes_per_sec: acked,
            // No dispatcher runs at this layer: exactly one effects record
            // commits per transact, so records/sec == acked/sec here. The
            // kernel layer is where the job Done-marks double it (spec §3).
            log_records_per_sec: acked,
            reads_per_sec,
            device_ratio: acked / device_syncs_per_sec,
        },
        invalid,
    ))
}
