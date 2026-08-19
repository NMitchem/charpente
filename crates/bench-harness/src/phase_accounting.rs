//! Every arithmetic decision about a trace lives here. `tracing_disk` records
//! and decides nothing — the observations seam, as in `core_accounting`.
//!
//! **What a gap is, and the assumption that makes it mean anything.** Between
//! two recorded ops the process is doing something the `Disk` boundary cannot
//! see: CPU, or waiting. Attributing that time requires that ops do not
//! overlap — i.e. that the arm is single-threaded. The `core` and `kernel`
//! arms are; the `wire` arm is not, which is why `attribute` REFUSES an
//! overlapping trace rather than publishing a number nobody can defend.
//!
//! **Measurement and inference are kept apart on purpose.** This module reports
//! where the time went (`gap_within_store`, `gap_wal_to_store`, …). It does not
//! decide that `gap_within_store` "is redb's CPU" — that reading is stated in
//! `docs/BENCH.md` where it can be argued with, not buried in a field name.

use crate::tracing_disk::{Event, OpKind, PathClass};

/// Nanoseconds, every field. The fields partition the measured window: `total`
/// equals the window exactly, by construction, and `attribute` asserts it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PhaseTotals {
    /// Time inside WAL ops that are not `sync` — `submit_all`'s append loop.
    pub wal_append_io: u64,
    /// Time inside WAL `sync` — the one durability barrier per batch.
    pub wal_sync_io: u64,
    /// Time inside ops on `tables.redb` — redb's own IO.
    pub store_io: u64,
    /// Time inside ops on anything else (the `.horizon` breadcrumb).
    pub other_io: u64,
    /// Gap between two consecutive WAL ops.
    pub gap_within_wal: u64,
    /// Gap from a WAL op to a store op — the sync→apply transition.
    pub gap_wal_to_store: u64,
    /// Gap between two consecutive store ops — work redb does between its own
    /// IOs.
    pub gap_within_store: u64,
    /// Gap from a store op back to a WAL op — end of one batch to the start of
    /// the next, so: everything OUTSIDE the commit path.
    pub gap_store_to_wal: u64,
    /// Any gap whose neighbours do not fall in the pairs above.
    pub gap_other: u64,
    /// Window start to the first recorded op.
    pub lead_in: u64,
    /// Last recorded op to window end.
    pub tail: u64,
}

impl PhaseTotals {
    pub fn total(&self) -> u64 {
        self.wal_append_io
            + self.wal_sync_io
            + self.store_io
            + self.other_io
            + self.gap_within_wal
            + self.gap_wal_to_store
            + self.gap_within_store
            + self.gap_store_to_wal
            + self.gap_other
            + self.lead_in
            + self.tail
    }
}

/// Partition `window_nanos` across the phases the trace reveals.
///
/// Refuses (rather than returning a number) when the trace cannot support the
/// arithmetic: overlapping ops, or an op outside the window.
pub fn attribute(events: &[Event], window_nanos: u64) -> Result<PhaseTotals, Vec<String>> {
    let mut invalid = Vec::new();

    for pair in events.windows(2) {
        if pair[1].start_nanos < pair[0].end_nanos {
            invalid.push(format!(
                "contract: phase attribution requires a single-threaded arm, but a traced op \
                 ending at {} ns overlaps the next starting at {} ns. On a concurrent arm the \
                 gap between ops is not idle time and every phase share below would be \
                 meaningless. Fix: run this on the core or kernel arm (both in-process, \
                 single-threaded), not the wire arm.",
                pair[0].end_nanos, pair[1].start_nanos
            ));
            break;
        }
    }

    if let Some(last) = events.last()
        && last.end_nanos > window_nanos
    {
        invalid.push(format!(
            "contract: a traced op ends at {} ns, past the measured window's {} ns. The trace \
             and the window disagree about what was measured. Fix: start the trace after the \
             window opens and read it before the window closes (see kernel_bench's \
             syncs_before/syncs_after pair for the shape).",
            last.end_nanos, window_nanos
        ));
    }

    if !invalid.is_empty() {
        return Err(invalid);
    }

    let mut totals = PhaseTotals::default();

    let Some(first) = events.first() else {
        totals.lead_in = window_nanos;
        return Ok(totals);
    };

    totals.lead_in = first.start_nanos;
    totals.tail = window_nanos - events[events.len() - 1].end_nanos;

    for event in events {
        let duration = event.end_nanos - event.start_nanos;
        match (event.path, event.op) {
            (PathClass::Wal, OpKind::Sync) => totals.wal_sync_io += duration,
            (PathClass::Wal, _) => totals.wal_append_io += duration,
            (PathClass::Store, _) => totals.store_io += duration,
            (PathClass::Other, _) => totals.other_io += duration,
        }
    }

    for pair in events.windows(2) {
        let gap = pair[1].start_nanos - pair[0].end_nanos;
        match (pair[0].path, pair[1].path) {
            (PathClass::Wal, PathClass::Wal) => totals.gap_within_wal += gap,
            (PathClass::Wal, PathClass::Store) => totals.gap_wal_to_store += gap,
            (PathClass::Store, PathClass::Store) => totals.gap_within_store += gap,
            (PathClass::Store, PathClass::Wal) => totals.gap_store_to_wal += gap,
            _ => totals.gap_other += gap,
        }
    }

    // Negative space: the fields partition the window. A residual means a gap
    // or an op was counted twice or not at all, which would show up as a phase
    // share that looks plausible and is wrong.
    assert_eq!(
        totals.total(),
        window_nanos,
        "phase totals must close to the measured window exactly"
    );

    Ok(totals)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracing_disk::{Event, OpKind, PathClass};

    fn ev(path: PathClass, op: OpKind, start: u64, end: u64) -> Event {
        Event {
            path,
            op,
            start_nanos: start,
            end_nanos: end,
        }
    }

    #[test]
    fn a_batch_attributes_append_sync_and_apply_separately() {
        // Two WAL appends, one WAL sync, two store ops — `submit_all`'s shape.
        let events = vec![
            ev(PathClass::Wal, OpKind::WriteAt, 0, 10),
            ev(PathClass::Wal, OpKind::WriteAt, 10, 20),
            ev(PathClass::Wal, OpKind::Sync, 20, 1020),
            ev(PathClass::Store, OpKind::WriteAt, 1030, 1050),
            ev(PathClass::Store, OpKind::WriteAt, 1060, 1080),
        ];
        let t = attribute(&events, 1100).expect("single-threaded trace attributes");
        assert_eq!(t.wal_append_io, 20);
        assert_eq!(t.wal_sync_io, 1000);
        assert_eq!(t.store_io, 40);
        assert_eq!(t.gap_wal_to_store, 10, "the sync→apply transition");
        assert_eq!(
            t.gap_within_store, 10,
            "redb-internal CPU between its own IOs"
        );
        assert_eq!(t.tail, 20);
    }

    #[test]
    fn every_nanosecond_of_the_window_is_attributed() {
        let events = vec![
            ev(PathClass::Wal, OpKind::WriteAt, 5, 15),
            ev(PathClass::Store, OpKind::Sync, 25, 30),
        ];
        let t = attribute(&events, 100).expect("attributes");
        assert_eq!(t.total(), 100, "totals must close to the window exactly");
    }

    #[test]
    fn overlapping_ops_are_refused_not_averaged() {
        let events = vec![
            ev(PathClass::Wal, OpKind::WriteAt, 0, 100),
            ev(PathClass::Store, OpKind::WriteAt, 50, 150),
        ];
        let invalid = attribute(&events, 200).expect_err("overlap must refuse");
        assert!(
            invalid[0].contains("single-threaded"),
            "the refusal must name the contract it enforces, got: {}",
            invalid[0]
        );
    }

    #[test]
    fn an_op_past_the_window_end_is_refused() {
        let events = vec![ev(PathClass::Wal, OpKind::Sync, 0, 500)];
        let invalid = attribute(&events, 100).expect_err("op outside the window must refuse");
        assert!(invalid[0].contains("window"), "got: {}", invalid[0]);
    }

    #[test]
    fn an_empty_trace_is_all_lead_in() {
        let t = attribute(&[], 100).expect("an empty trace is legal");
        assert_eq!(t.lead_in, 100);
        assert_eq!(t.total(), 100);
    }
}
