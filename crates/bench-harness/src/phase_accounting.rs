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
//! decide whose CPU runs during a gap — that reading belongs in `docs/BENCH.md`
//! where it can be argued with, not buried in a field name or a test message.
//!
//! **The window and the trace share one clock, and the boundary is exact.**
//! `window_start_nanos`/`window_end_nanos` must be stamped with
//! `Trace::now_nanos()` (`tracing_disk.rs`) — the same origin every recorded
//! `Event` uses — taken at the true edges of the measured section, with no
//! traced op in flight at either edge. Events lying wholly outside
//! `[window_start_nanos, window_end_nanos]` are ignored: that is warmup (the
//! `Db::open` and population work that necessarily precedes a measured
//! window), and ignoring it is the point — folding it in would silently
//! dilute every published share by an undisclosed factor. An event that
//! straddles a boundary instead of falling entirely inside or outside it means
//! the window was stamped from the wrong place, and `attribute` refuses rather
//! than clipping it, silently or otherwise.

use crate::tracing_disk::{Event, OpKind, PathClass};

/// Nanoseconds, every field. The fields partition the measured window: `total`
/// equals `window_end_nanos - window_start_nanos` exactly, by construction,
/// and `attribute` asserts it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PhaseTotals {
    /// Time inside WAL `write_at`/`set_len` — `submit_all`'s append loop.
    pub wal_append_io: u64,
    /// Time inside WAL `read_at`/`len`. Distinct from `wal_append_io` because
    /// a read is a different kind of cost from a write, not because the
    /// steady-state commit path issues any today (it does not — see
    /// `log.rs:278,287`; this field exists for the recovery/rollover/follower
    /// paths that do).
    pub wal_read_io: u64,
    /// Time inside WAL `sync` — the one durability barrier per batch.
    pub wal_sync_io: u64,
    /// Time inside `tables.redb` `write_at`/`set_len` — redb's own writes.
    pub store_write_io: u64,
    /// Time inside `tables.redb` `read_at`/`len`.
    pub store_read_io: u64,
    /// Time inside `tables.redb` `sync`. `Store::checkpoint` uses redb's
    /// default `Durability::Immediate`, unlike `apply`/`import`/`gc_events`
    /// (which set `Durability::None`), so a real fsync on `tables.redb` lands
    /// here on a checkpoint — a device-durability cost, not throughput, which
    /// is exactly why `wal_sync_io` above is its own field rather than folded
    /// into `wal_append_io`. Kept separate from `store_write_io` so the two
    /// kinds of cost cannot silently average into one number.
    pub store_sync_io: u64,
    /// Time inside ops on anything else (the `.horizon` breadcrumb) — any op
    /// kind.
    pub other_io: u64,
    /// Gap between two consecutive WAL ops.
    pub gap_within_wal: u64,
    /// Gap from a WAL op to a store op — the sync→apply transition.
    pub gap_wal_to_store: u64,
    /// Gap between two consecutive store ops. What runs in this gap is not
    /// decided by this module — see `docs/BENCH.md` for that reading, argued
    /// with the field's actual contents rather than assumed from its name.
    pub gap_within_store: u64,
    /// Gap from a store op back to a WAL op — end of one batch to the start of
    /// the next, so: everything OUTSIDE the commit path.
    pub gap_store_to_wal: u64,
    /// Any gap whose neighbours do not fall in the pairs above — e.g. either
    /// side of the `.horizon` breadcrumb at a checkpoint boundary
    /// (`(Store,Other)`, `(Other,Wal)`, and the `Other`-internal pairs).
    pub gap_other: u64,
    /// `window_start_nanos` to the first in-window op.
    pub lead_in: u64,
    /// Last in-window op to `window_end_nanos`.
    pub tail: u64,
}

impl PhaseTotals {
    pub fn total(&self) -> u64 {
        self.wal_append_io
            + self.wal_read_io
            + self.wal_sync_io
            + self.store_write_io
            + self.store_read_io
            + self.store_sync_io
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

/// Partition `[window_start_nanos, window_end_nanos]` across the phases the
/// trace reveals. `events` may include ops recorded before or after the
/// window (warmup, teardown) — those are ignored, not attributed.
///
/// Refuses (rather than returning a number) when the trace cannot support the
/// arithmetic: an inverted window, a malformed event (`end < start`),
/// overlapping ops, or an event that straddles a window boundary instead of
/// falling entirely inside or outside it. Every applicable check runs and its
/// reasons are collected before returning, so a trace violating more than one
/// contract reports all of them.
pub fn attribute(
    events: &[Event],
    window_start_nanos: u64,
    window_end_nanos: u64,
) -> Result<PhaseTotals, Vec<String>> {
    let mut invalid = Vec::new();

    if window_end_nanos < window_start_nanos {
        invalid.push(format!(
            "contract: window_end_nanos must not precede window_start_nanos, but the measured \
             window's end ({window_end_nanos} ns) precedes its start ({window_start_nanos} ns). \
             Fix: stamp both boundaries with `Trace::now_nanos()`, start before end, in the \
             order the measured window actually opened and closed."
        ));
    }

    // Negative space (hard rule 4): `Instant` is monotonic and `Trace::record`
    // derives both timestamps from one `Instant` sample either side of the op,
    // so a real recording can never produce `end < start`. But `Event`'s
    // fields are all `pub`, and this function's whole value is that its
    // output can be trusted — including against a hand-built or fuzzed
    // `Event`, which is exactly what a test fixture is. Unguarded, `:end -
    // :start` wraps in release (no `overflow-checks` override in this
    // workspace's `Cargo.toml`), and because the wrap is congruent mod 2^64
    // the closure identity below still holds — the assert that is supposed to
    // catch a broken invariant passes on garbage instead.
    for event in events {
        if event.end_nanos < event.start_nanos {
            invalid.push(format!(
                "contract: a traced op must end no earlier than it starts, but a {:?} {:?} op \
                 ends at {} ns having started at {} ns. `Trace::record` derives both timestamps \
                 from one monotonic `Instant`, so an event violating this was synthesised rather \
                 than recorded — check whatever constructed this `Event` directly instead of \
                 through `TracingDisk`.",
                event.path, event.op, event.end_nanos, event.start_nanos
            ));
        }
    }

    for pair in events.windows(2) {
        if pair[1].start_nanos < pair[0].end_nanos {
            invalid.push(format!(
                "contract: phase attribution requires a single-threaded arm, but a traced {:?} \
                 {:?} op ending at {} ns overlaps the next, a {:?} {:?} op starting at {} ns. On \
                 a concurrent arm the gap between ops is not idle time and every phase share \
                 below would be meaningless. Fix: run this on the core or kernel arm (both \
                 in-process, single-threaded), not the wire arm.",
                pair[0].path,
                pair[0].op,
                pair[0].end_nanos,
                pair[1].path,
                pair[1].op,
                pair[1].start_nanos
            ));
            // Only the first overlap is named. A concurrent arm can produce
            // thousands of overlapping pairs; naming the first is enough to
            // diagnose "this trace did not come from a single-threaded arm"
            // without flooding the caller with a report a healthy arm will
            // never need to read past the first line of.
            break;
        }
    }

    // I-2: an event must fall entirely inside the window or entirely outside
    // it. "Entirely outside" is warmup/teardown and is ignored below, not an
    // error. Anything else — starts before `window_start_nanos` and ends
    // inside it, or starts inside and ends after `window_end_nanos` — means
    // no IO was in flight at the instant the window boundary was stamped
    // (true on a single-threaded arm), so a straddle means the boundary was
    // stamped from the wrong place. Only checked once the window itself is
    // known to be ordered; against an inverted window "inside"/"outside" is
    // not a meaningful question and would just add a confusing second error.
    if window_end_nanos >= window_start_nanos {
        for event in events {
            let fully_outside =
                event.end_nanos <= window_start_nanos || event.start_nanos >= window_end_nanos;
            let fully_inside =
                event.start_nanos >= window_start_nanos && event.end_nanos <= window_end_nanos;
            if !fully_outside && !fully_inside {
                invalid.push(format!(
                    "contract: a traced {:?} {:?} op spans {} ns to {} ns, which straddles the \
                     measured window's boundary ({window_start_nanos} ns to {window_end_nanos} \
                     ns) instead of falling entirely inside or entirely outside it. On a \
                     single-threaded arm no IO is in flight at the instant a window boundary is \
                     stamped, so a straddle means the boundary was stamped from the wrong place. \
                     Fix: stamp `window_start_nanos`/`window_end_nanos` with `Trace::now_nanos()` \
                     at the true edges of the measured section, with no traced op still running \
                     across either edge.",
                    event.path, event.op, event.start_nanos, event.end_nanos
                ));
            }
        }
    }

    if !invalid.is_empty() {
        return Err(invalid);
    }

    let window_nanos = window_end_nanos - window_start_nanos;
    let mut totals = PhaseTotals::default();

    let in_window: Vec<Event> = events
        .iter()
        .copied()
        .filter(|e| e.start_nanos >= window_start_nanos && e.end_nanos <= window_end_nanos)
        .collect();

    let Some(first) = in_window.first() else {
        totals.lead_in = window_nanos;
        return Ok(totals);
    };

    totals.lead_in = first.start_nanos - window_start_nanos;
    totals.tail = window_end_nanos - in_window[in_window.len() - 1].end_nanos;

    for event in &in_window {
        let duration = event.end_nanos - event.start_nanos;
        match (event.path, event.op) {
            (PathClass::Wal, OpKind::Sync) => totals.wal_sync_io += duration,
            (PathClass::Wal, OpKind::ReadAt | OpKind::Len) => totals.wal_read_io += duration,
            (PathClass::Wal, OpKind::WriteAt | OpKind::SetLen) => totals.wal_append_io += duration,
            (PathClass::Store, OpKind::Sync) => totals.store_sync_io += duration,
            (PathClass::Store, OpKind::ReadAt | OpKind::Len) => totals.store_read_io += duration,
            (PathClass::Store, OpKind::WriteAt | OpKind::SetLen) => {
                totals.store_write_io += duration
            }
            (PathClass::Other, _) => totals.other_io += duration,
        }
    }

    for pair in in_window.windows(2) {
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
        // Two WAL appends, one WAL sync, two store writes — `submit_all`'s shape.
        let events = vec![
            ev(PathClass::Wal, OpKind::WriteAt, 0, 10),
            ev(PathClass::Wal, OpKind::WriteAt, 10, 20),
            ev(PathClass::Wal, OpKind::Sync, 20, 1020),
            ev(PathClass::Store, OpKind::WriteAt, 1030, 1050),
            ev(PathClass::Store, OpKind::WriteAt, 1060, 1080),
        ];
        let t = attribute(&events, 0, 1100).expect("single-threaded trace attributes");
        assert_eq!(t.wal_append_io, 20);
        assert_eq!(t.wal_sync_io, 1000);
        assert_eq!(t.store_write_io, 40);
        assert_eq!(t.gap_wal_to_store, 10, "the sync→apply transition");
        assert_eq!(
            t.gap_within_store, 10,
            "a gap between two consecutive tables.redb ops"
        );
        assert_eq!(t.tail, 20);
    }

    /// Every field non-zero on purpose, and every field's value distinct from
    /// every other field's. A closure test whose fixture leaves a field at
    /// zero cannot detect that field being dropped from `total` (which is how
    /// the very first version of this test passed while `gap_within_store`
    /// was deleted); a fixture whose fields collide in value cannot detect two
    /// fields' targets being swapped, which the closure invariant cannot see
    /// either way (`total()` is the same sum regardless of which field a
    /// nanosecond lands in — see the deletion/reroute proof in the task
    /// report). This fixture also covers the two `gap_other` members
    /// (`(Store,Other)`, `(Other,Wal)`) a checkpoint boundary actually
    /// produces (`Store::checkpoint` then `write_horizon_breadcrumb` then the
    /// next batch's WAL append), which the original fixture did not reach.
    #[test]
    fn every_nanosecond_of_the_window_is_attributed() {
        let events = vec![
            ev(PathClass::Wal, OpKind::WriteAt, 10, 16), // wal_append_io part 1 (dur 6)
            ev(PathClass::Wal, OpKind::ReadAt, 20, 29),  // wal_read_io (dur 9)
            ev(PathClass::Wal, OpKind::Sync, 39, 61),    // wal_sync_io (dur 22)
            ev(PathClass::Store, OpKind::WriteAt, 73, 84), // store_write_io (dur 11)
            ev(PathClass::Store, OpKind::ReadAt, 99, 112), // store_read_io (dur 13)
            ev(PathClass::Wal, OpKind::WriteAt, 130, 137), // wal_append_io part 2 (dur 7)
            ev(PathClass::Store, OpKind::Sync, 154, 178), // store_sync_io (dur 24)
            ev(PathClass::Other, OpKind::Len, 198, 203), // other_io (dur 5)
            ev(PathClass::Wal, OpKind::WriteAt, 226, 234), // wal_append_io part 3 (dur 8)
        ];
        let t = attribute(&events, 0, 253).expect("attributes");

        assert_eq!(t.wal_append_io, 21, "6 + 7 + 8, three separate WAL writes");
        assert_eq!(t.wal_read_io, 9);
        assert_eq!(t.wal_sync_io, 22);
        assert_eq!(t.store_write_io, 11);
        assert_eq!(t.store_read_io, 13);
        assert_eq!(t.store_sync_io, 24);
        assert_eq!(t.other_io, 5);
        assert_eq!(t.gap_within_wal, 14, "(20-16) + (39-29)");
        assert_eq!(t.gap_wal_to_store, 29, "(73-61) + (154-137)");
        assert_eq!(t.gap_within_store, 15, "99-84");
        assert_eq!(t.gap_store_to_wal, 18, "130-112");
        assert_eq!(
            t.gap_other, 43,
            "(198-178) Store→Other + (226-203) Other→Wal — the two reachable \
             gap_other members the original fixture never exercised"
        );
        assert_eq!(t.lead_in, 10);
        assert_eq!(t.tail, 19, "253-234");

        // The fixture's own contract: if a later edit collides two fields'
        // values, this test silently stops distinguishing a swap between
        // them; if it zeroes a field, this test silently stops guarding that
        // field's term in `total` at all.
        let named = [
            ("wal_append_io", t.wal_append_io),
            ("wal_read_io", t.wal_read_io),
            ("wal_sync_io", t.wal_sync_io),
            ("store_write_io", t.store_write_io),
            ("store_read_io", t.store_read_io),
            ("store_sync_io", t.store_sync_io),
            ("other_io", t.other_io),
            ("gap_within_wal", t.gap_within_wal),
            ("gap_wal_to_store", t.gap_wal_to_store),
            ("gap_within_store", t.gap_within_store),
            ("gap_store_to_wal", t.gap_store_to_wal),
            ("gap_other", t.gap_other),
            ("lead_in", t.lead_in),
            ("tail", t.tail),
        ];
        for (name, value) in named {
            assert_ne!(
                value, 0,
                "{name} is zero — this fixture no longer guards its term"
            );
        }
        for i in 0..named.len() {
            for j in (i + 1)..named.len() {
                assert_ne!(
                    named[i].1, named[j].1,
                    "{} and {} share a value ({}) — this fixture can no longer distinguish a \
                     swap between them",
                    named[i].0, named[j].0, named[i].1
                );
            }
        }

        assert_eq!(t.total(), 253, "totals must close to the window exactly");
    }

    #[test]
    fn overlapping_ops_are_refused_not_averaged() {
        let events = vec![
            ev(PathClass::Wal, OpKind::WriteAt, 0, 100),
            ev(PathClass::Store, OpKind::WriteAt, 50, 150),
        ];
        let invalid = attribute(&events, 0, 200).expect_err("overlap must refuse");
        assert!(
            invalid[0].contains("single-threaded"),
            "the refusal must name the contract it enforces, got: {}",
            invalid[0]
        );
        // N-3: two bare offsets are not a locator against a million-event
        // trace. Both ops' path and kind are sitting right on the pair that
        // failed — name them.
        assert!(
            invalid[0].contains("Wal") && invalid[0].contains("WriteAt"),
            "must name the first op's path and kind: {}",
            invalid[0]
        );
        assert!(
            invalid[0].contains("Store"),
            "must name the second op's path: {}",
            invalid[0]
        );
    }

    // N-2: the overlap loop's `break` means only the first overlapping pair
    // is ever named. Proven here rather than left as an unexplained line: a
    // trace with two independent overlaps still produces exactly one message.
    #[test]
    fn only_the_first_of_several_overlaps_is_reported() {
        let events = vec![
            ev(PathClass::Wal, OpKind::WriteAt, 0, 100),
            ev(PathClass::Store, OpKind::WriteAt, 50, 150), // overlaps event 0
            ev(PathClass::Other, OpKind::WriteAt, 120, 200), // overlaps event 1 too
        ];
        let invalid = attribute(&events, 0, 300).expect_err("overlap must refuse");
        assert_eq!(
            invalid.len(),
            1,
            "a concurrent arm can produce thousands of overlapping pairs; naming only the \
             first is deliberate so the caller is not flooded: {invalid:?}"
        );
    }

    #[test]
    fn an_inverted_event_is_refused_not_silently_wrapped() {
        // I-1: end 1 ns before start. In release this subtracts and wraps
        // rather than panicking, and because the wrap is congruent mod 2^64
        // the closure identity still holds — the assert alone cannot catch
        // it, which is why this is a named refusal instead.
        let events = vec![ev(PathClass::Wal, OpKind::WriteAt, 19, 18)];
        let invalid = attribute(&events, 0, 20).expect_err("an inverted event must refuse");
        assert!(
            invalid[0].contains("Wal") && invalid[0].contains("WriteAt"),
            "must name the offending op: {}",
            invalid[0]
        );
        assert!(
            invalid[0].contains("19") && invalid[0].contains("18"),
            "must name both timestamps: {}",
            invalid[0]
        );
    }

    #[test]
    fn an_inverted_window_is_refused() {
        let invalid = attribute(&[], 100, 50).expect_err("window end before start must refuse");
        assert!(
            invalid[0].contains("precede"),
            "must name the contract: {}",
            invalid[0]
        );
        assert!(
            invalid[0].contains("100") && invalid[0].contains("50"),
            "must name both boundaries: {}",
            invalid[0]
        );
    }

    #[test]
    fn a_straddling_start_boundary_event_is_refused() {
        // Starts before window_start (10), ends inside it — no IO can be in
        // flight at the instant a single-threaded arm's window opens, so this
        // means the boundary was stamped from the wrong place.
        let events = vec![ev(PathClass::Wal, OpKind::WriteAt, 5, 15)];
        let invalid = attribute(&events, 10, 100).expect_err("a straddling event must refuse");
        assert!(
            invalid[0].contains("straddle") && invalid[0].contains("window"),
            "got: {}",
            invalid[0]
        );
    }

    #[test]
    fn a_straddling_end_boundary_event_is_refused() {
        // Starts inside the window, ends after window_end (100).
        let events = vec![ev(PathClass::Wal, OpKind::WriteAt, 90, 150)];
        let invalid = attribute(&events, 0, 100).expect_err("a straddling event must refuse");
        assert!(
            invalid[0].contains("straddle") && invalid[0].contains("window"),
            "got: {}",
            invalid[0]
        );
    }

    #[test]
    fn events_wholly_before_the_window_are_ignored_as_warmup() {
        // I-2's concrete failure scenario: `Db::open` and warmup run before
        // the measured window opens. Folding that time in (by widening the
        // window instead of ignoring the events) would silently dilute every
        // published share; this proves it is ignored instead.
        let events = vec![
            ev(PathClass::Wal, OpKind::WriteAt, 0, 5), // warmup, wholly before window_start=10
            ev(PathClass::Wal, OpKind::WriteAt, 20, 30), // inside the window
        ];
        let t =
            attribute(&events, 10, 100).expect("warmup before the window is ignored, not refused");
        assert_eq!(
            t.lead_in, 10,
            "lead_in measures from window_start, not from the warmup event"
        );
        assert_eq!(t.wal_append_io, 10, "only the in-window op is counted");
        assert_eq!(t.tail, 70);
        assert_eq!(t.total(), 90);
    }

    #[test]
    fn a_trace_with_nothing_inside_the_window_is_entirely_lead_in() {
        // Every event is warmup; the in-window set is empty, which is legal
        // (same shape as an empty trace) rather than an error.
        let events = vec![ev(PathClass::Wal, OpKind::WriteAt, 0, 5)];
        let t = attribute(&events, 10, 100)
            .expect("a warmup-only trace has an empty in-window set, which is legal");
        assert_eq!(t.lead_in, 90);
        assert_eq!(t.total(), 90);
    }

    #[test]
    fn an_empty_trace_is_all_lead_in() {
        let t = attribute(&[], 50, 150).expect("an empty trace is legal");
        assert_eq!(t.lead_in, 100);
        assert_eq!(t.total(), 100);
    }
}
