//! The deterministic kernel (DESIGN §1, spec 2026-07-02-queue-cron-design.md):
//! the event loop multiplexing HTTP requests, due jobs, and cron ticks — all
//! inside the simulation boundary.

pub(crate) mod cron;
mod dispatch;
mod tail;

pub use cron::{CronRow, CronScheduler};
pub use dispatch::{Dispatcher, JobDef, JobEntry, JobError, JobOutcome};
pub use tail::{EventFrame, EventSink, SimSink, SimSinkHandle, SinkStatus, TAIL_BATCH_EVENTS};

use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Mutex};

use charpente_core::db::{Db, MAX_BATCH};
use charpente_core::event::EventRow;
use charpente_core::io::{Clock, Instant, ObjectStore};
use charpente_core::table::View;

use crate::archive::Archive;
use crate::http::{App, Cursor, Handled, Request, Response, io_failure_response};
use crate::kernel::tail::Tail;
use crate::repl::{REPL_TICK_NANOS, Repl};

/// Delivers a response back to whoever is waiting on a request — a channel
/// reply in production, a recorded `(status, body)` in the sim. `stream` is
/// the SSE upgrade: instead of ending the exchange, it hands back a sink the
/// kernel pushes frames into for the rest of the connection (T5's Tail arm
/// is the first caller; today nothing calls it — `App::handle`'s plain path
/// never produces a subscription, only `handle_or_subscribe` does).
pub trait Respond: Send {
    /// One-shot: this response ends the exchange.
    fn send(self: Box<Self>, response: Response);
    /// The SSE upgrade: deliver the stream-opening response, return the sink
    /// the kernel pushes frames into for the rest of the connection.
    fn stream(self: Box<Self>, response: Response) -> Box<dyn EventSink>;
}

pub type Reply = Box<dyn Respond>;

/// What woke `Kernel::run` out of a park.
pub enum Wake {
    /// A request arrived — the next `poll()` returns it.
    Ready,
    /// The requested deadline passed with nothing new.
    Deadline,
    /// The source is exhausted; the loop must return.
    Shutdown,
}

/// The result of one `Kernel::step`. `Worked` serviced exactly one ready
/// source; `Idle` found nothing ready and reports the earliest deadline to
/// park on — `step` itself never blocks, so a driver (`run`, or a custom loop)
/// parks on `Idle`. Extracting the loop body this way lets replication drive
/// the same decision code without owning the park (T8).
pub enum Step {
    Worked,
    Idle { deadline: Option<Instant> },
}

/// Where the kernel's requests come from: hyper's channel in production, a
/// scripted timeline in the sim. `RequestSource` is the one seam DESIGN §1's
/// same-shape rule allows — `Kernel::run` is otherwise identical code.
pub trait RequestSource {
    /// Non-blocking. A returned request must eventually get its `Reply`
    /// called.
    fn poll(&mut self) -> Option<(Request, Reply)>;

    /// Parks until a request arrives (`Ready`), the deadline passes
    /// (`Deadline`), or the source is exhausted (`Shutdown`). Must not drop
    /// requests: one observed during the park surfaces from the next
    /// `poll()`.
    fn wait_until(&mut self, deadline: Option<Instant>) -> Wake;
}

/// Counters the kernel loop maintains for observability and tests — a single
/// snapshot, cheap to copy out at any point.
#[derive(Default, Clone, Copy, Debug)]
pub struct KernelStats {
    pub requests: u64,
    pub jobs_done: u64,
    pub jobs_retried: u64,
    pub jobs_dead: u64,
    pub cron_ticks: u64,
    pub cron_catchups: u64,
    /// `Archive::step` calls the arm has made — including the ones that
    /// returned `Err`, since a retry is still a step the loop spent.
    pub archive_steps: u64,
}

#[derive(Clone, Copy)]
enum Ready {
    Request,
    Job,
    Cron,
    Repl,
    Tail,
    Archive,
}

/// The deterministic kernel: one `App`, one due-heap, one cron schedule, one
/// clock, run by `run` against whatever `RequestSource` is handed in — hyper
/// in production, `SimSource` under the simulator.
pub struct Kernel {
    app: App,
    dispatcher: Dispatcher,
    cron: CronScheduler,
    clock: Box<dyn Clock>,
    pending: Option<(Request, Reply)>,
    stats: KernelStats,
    /// The replication engine when this kernel runs replicated (`None` for a
    /// plain single node — the ready set then never gates and never ticks).
    repl: Option<Repl>,
    /// Whether the last iteration saw this node accepting writes. Every way
    /// a node becomes writable mid-run — a promote, a switchover hand-off
    /// (both inside `Repl::step`), or the witness boot gate resolving —
    /// rebuilds the due-heap and cron from the replicated store and runs the
    /// schema gate, because those are exactly the pieces of startup a
    /// non-writable node had to skip.
    ///
    /// Keyed on the write gate rather than the ROLE (M2 sub-project 0b): a
    /// witness-gated node is a `Role::Primary` the whole time, so a
    /// role-keyed edge never fires for it. `writes_allowed()` is a strict
    /// refinement of `Repl::is_primary()` — it agrees in every state that
    /// existed before, and additionally covers `Verifying`.
    was_writable: bool,
    /// Live SSE subscriptions (realtime T5). A tail joins the ready set only
    /// while it is behind (`next_id < next_event_id`); dead tails (below
    /// retention, or a sink that reported `Full`/`Closed`) are removed right
    /// after the arm services them.
    tails: Vec<Tail>,
    /// Round-robin start index into `tails` for the next `Ready::Tail`
    /// service — bounds one slow tail from starving the others.
    tail_cursor: usize,
    /// The archive and the store it ships to, together or not at all
    /// (`None` for a node that does not archive — the ready set then never
    /// gates on one).
    ///
    /// ONE `Option` over the PAIR rather than two independent `Option`s.
    /// `Archive::step(&mut self, db, store)` takes the store on every call
    /// and the `Archive` never holds it (ruling 32), so the kernel has to
    /// keep it; and an archive without a store — or a store without an
    /// archive — is not a state this arm could act on. Pairing them makes
    /// that unrepresentable instead of merely unhandled, which is hard rule
    /// 4's negative space applied to a type.
    archive: Option<(Archive, Box<dyn ObjectStore>)>,
    /// What the archive knew last time it found nothing to do:
    /// `Some(last_applied)` if the previous `Archive::step` left it idle,
    /// `None` if it left work pending. The ready predicate's watermark —
    /// see the ready-set build for why `idle()` alone cannot be it.
    archive_idle_at: Option<Option<u64>>,
    /// Set when the last `Archive::step` returned `Err`, cleared when the
    /// loop parks. Paces a retry at one per PARK rather than one per
    /// iteration; see the ready-set build.
    archive_retry_parked: bool,
    /// `gapped()`/`stopped()` as of the last archive step, so the arm can
    /// raise spec §7's two operator notices on the EDGE rather than once per
    /// retry.
    archive_gapped: bool,
    archive_stopped: bool,
    /// The last notice the archive raised, left for a driver that would
    /// rather route it than read stderr (`Repl::take_witness_notice`'s
    /// precedent).
    archive_notice: Option<String>,
}

impl Kernel {
    /// Consumes the mounted App: upserts cron rows, rebuilds the due-heap
    /// from Pending rows. `now` for the cron phase comes from `clock`. The
    /// single-node constructor — no replication.
    pub fn new(app: App, clock: Box<dyn Clock>) -> io::Result<Kernel> {
        Kernel::build(app, clock, None, None)
    }

    /// The replicated constructor: same setup, plus a `Repl` whose role gates
    /// the ready set (a follower serves reads and ships, never runs jobs/cron)
    /// and whose park tick (`REPL_TICK_NANOS`) bounds every idle wait so
    /// in-flight replication IO is polled promptly.
    pub fn with_replication(app: App, clock: Box<dyn Clock>, repl: Repl) -> io::Result<Kernel> {
        Kernel::build(app, clock, Some(repl), None)
    }

    /// The archiving constructor (archive spec §4): same setup, plus the
    /// `Archive` and the `ObjectStore` it ships to. FOUR arguments, not
    /// three, because `Archive::step` takes the store on every call and the
    /// `Archive` never holds it — see the `archive` field.
    ///
    /// `Archive::open` registers the retention pin, so it must already have
    /// run against this `App`'s `Db` before the `App` is consumed here.
    pub fn with_archive(
        app: App,
        clock: Box<dyn Clock>,
        archive: Archive,
        store: Box<dyn ObjectStore>,
    ) -> io::Result<Kernel> {
        Kernel::build(app, clock, None, Some((archive, store)))
    }

    fn build(
        mut app: App,
        clock: Box<dyn Clock>,
        repl: Option<Repl>,
        archive: Option<(Archive, Box<dyn ObjectStore>)>,
    ) -> io::Result<Kernel> {
        let mut dispatcher = Dispatcher::new(app.take_job_defs());
        let mut cron = CronScheduler::new(app.take_cron_entries());
        let now = clock.now().as_nanos();
        let was_writable = app.db_mut().writes_allowed();
        // `cron.upsert` writes the schedule rows — skip it whenever the write
        // gate is shut (its transaction would be refused, and on a follower
        // the rows arrive by replication anyway); `after_repl_step` re-runs it
        // the moment the gate opens. `dispatcher.rebuild` is a read, always
        // safe.
        //
        // The guard is the WRITE GATE, not the role: a witness-verifying node
        // is a `Role::Primary`, so a role guard here would let the upsert
        // transact, hit `TxError::Verifying`, and `?`-propagate out of the
        // CONSTRUCTOR — a node that never enters `run` and cannot serve one
        // read, silently implementing strict-refuse instead of spec §2's
        // ruled "serve reads, withhold writes".
        if was_writable {
            cron.upsert(app.db_mut(), now)?;
        }
        dispatcher.rebuild(&app.db_mut().snapshot()?)?;
        // The rebuild scan above is the authoritative snapshot of everything
        // enqueued before construction; ids still sitting in the drain buffer
        // from pre-construction transacts are already in the heap — draining
        // them later would resurrect already-serviced jobs as duplicate heap
        // entries (found by the 120-ticks test, seed 0x650).
        let _ = app.db_mut().drain_enqueued();
        Ok(Kernel {
            app,
            dispatcher,
            cron,
            clock,
            pending: None,
            stats: KernelStats::default(),
            repl,
            was_writable,
            tails: Vec::new(),
            tail_cursor: 0,
            archive,
            // `None`, not `Some(db.last_applied())`: the archive has never
            // been stepped, so it has never had a chance to find nothing to
            // do, and its opening manifest GET is real work.
            archive_idle_at: None,
            archive_retry_parked: false,
            archive_gapped: false,
            archive_stopped: false,
            archive_notice: None,
        })
    }

    /// The event loop (DESIGN §1 same-shape rule): identical code under
    /// `serve()` and under the sim; only the `RequestSource` impl differs.
    /// Returns `Ok(())` on source `Shutdown`; `Err` on platform IO failure
    /// (poisoned engine — restart to recover). Thin wrapper over `step`: it
    /// owns the park, `step` owns the decision.
    pub fn run(&mut self, source: &mut dyn RequestSource) -> io::Result<()> {
        // The role-aware schema gate (spec §2): single-node and primary
        // kernels migrate before serving anything; followers skip (their
        // migration arrives by shipping) and run it at promote instead.
        if self.app.db_mut().writes_allowed() {
            self.app.db_mut().migrate_schema()?;
        }
        loop {
            match self.step(source)? {
                Step::Worked => {}
                Step::Idle { deadline } => {
                    if let Wake::Shutdown = source.wait_until(deadline) {
                        // Clean shutdown (spec §2's other half): checkpoint
                        // so the next open finds a compacted store rather
                        // than replaying the WAL tail — skipped when
                        // poisoned, since checkpointing a poisoned engine
                        // would mask the poison.
                        if !self.app.db_mut().poisoned() {
                            self.app.db_mut().checkpoint()?;
                        }
                        return Ok(());
                    }
                }
            }
        }
    }

    /// One iteration of the loop: build the ready set, service exactly one
    /// ready source (the same-shape decision, DESIGN §1), and return `Worked`;
    /// or, when nothing is ready, return `Idle { deadline }` without parking.
    /// The replication kernel arm and the park tick land here in T8.
    pub fn step(&mut self, source: &mut dyn RequestSource) -> io::Result<Step> {
        if self.pending.is_none() {
            self.pending = source.poll();
        }
        let now = self.clock.now().as_nanos();
        // Non-destructive IO movement, the peer of `source.poll()` (DESIGN §1):
        // accept conns, drain readable messages, note timers — no `Db` access.
        if let Some(repl) = &mut self.repl {
            repl.pump_io(now);
        }
        // A follower (or a draining primary, or a primary still verifying its
        // witnesses) serves reads and ships, but never runs jobs or cron:
        // those transactions would hit `TxError::NotPrimary`/`Verifying`
        // (DESIGN §5). The guard is the write gate, not the role — see
        // `was_writable`.
        let writable = self.app.db().writes_allowed();
        let mut ready = Vec::with_capacity(6);
        if self.pending.is_some() {
            ready.push(Ready::Request);
        }
        if writable && self.dispatcher.has_due(now) {
            ready.push(Ready::Job);
        }
        if writable && self.cron.has_due(now) {
            ready.push(Ready::Cron);
        }
        if self
            .repl
            .as_ref()
            .is_some_and(|r| r.has_work(self.app.db(), now))
        {
            ready.push(Ready::Repl);
        }
        // THE ARCHIVE ARM'S READY PREDICATE (archive spec §4; ruling 57).
        // Four clauses, and none of them is optional — each rules out a
        // different way of getting this wrong, and three of the four were
        // reached for first and are WRONG:
        //
        // The store cannot be asked. `ObjectStore` is exactly `start` and
        // `poll`, and `poll` CONSUMES the completion ("the two are
        // deliberately indistinguishable", io.rs) — there is no
        // non-destructive readiness peek anywhere on it or on `Archive`.
        //
        // `archive.is_some()` alone spins the core. `step` would return
        // `Worked` every iteration, `Step::Idle` would never be reached, and
        // `wait_until` would never be called: 100% CPU on a caught-up
        // archive in production, and virtual time that never advances under
        // the sim. A `has_work`-shaped predicate is a CORRECTNESS
        // requirement here, exactly as it is for `Repl`.
        //
        // `!archive.idle()` parks a live archive forever. `idle` answers
        // true in FIVE states (see `Archive::idle`) and four of them still
        // have un-archived data behind the `true`; worse, `idle` is a
        // SNAPSHOT that only updates when you step, so "skip while idle"
        // is self-sealing — a write-gated archive "resumes by itself when
        // the gate opens" only if something keeps stepping it, and the step
        // is what the predicate is refusing.
        //
        // What works is `idle` PAIRED WITH THE WATERMARK IT WAS TAKEN AT.
        // `archive_idle_at` is `Some(L)` when the last step found nothing to
        // do against a `Db` whose `last_applied` was `L`, and comparing it
        // to a freshly read `last_applied` asks "has the world moved since
        // the archive last looked", which is never stale. That is sound
        // because every source of new archive work requires `last_applied`
        // to advance: the snapshot cadence (`cadence_due`) and
        // `open_snapshot` are both functions of it, the "waiting" arm clears
        // only when it reaches `layout_end`, and a new segment seals only
        // during a WAL append, which advances it in the same call.
        // `Wal::gc` only ever removes segments, which never creates work.
        //
        // `last_applied()` is a plain field read (engine.rs), which is what
        // makes it admissible here. `sealed_segments()` is a `Disk::list`
        // and must never appear in a per-iteration predicate (F2).
        //
        // `stopped()` is the ONE state that must be skipped outright: a
        // stopped archive answers `idle() == true` and `Ok(())` forever, so
        // without this clause the kernel drives a dead archive for the life
        // of the process and reports it as the healthy steady state.
        //
        // `verifying()` is re-read every iteration rather than remembered,
        // which is what makes the gate OPENING re-arm the arm with no step
        // in between (spec §4 rule 1).
        //
        // `archive_retry_parked` paces a FAILED store op at one retry per
        // kernel PARK instead of one per iteration. Without it an object
        // store outage pegs the loop: a failed `start` leaves the archive in
        // a non-`Idle` state permanently, so `archive_idle_at` stays `None`
        // and the arm is ready forever — and `Archive::step`'s
        // `yield_or_bound` does a `Disk::list` on every one of those
        // iterations. Not a timer, deliberately: spec §8 states the archive
        // arm never reads wall-clock time, directly or transitively, which
        // is also why `snapshot_every` counts records rather than seconds.
        if let Some((archive, _)) = &self.archive
            && !archive.stopped()
            && !self.app.db().verifying()
            && !self.archive_retry_parked
            && self.archive_idle_at != Some(self.app.db().last_applied())
        {
            ready.push(Ready::Archive);
        }
        // The Tail source only ever joins via data state, never a timer: a
        // tail becomes eligible the instant some commit moves
        // `next_event_id` past it, and the guard here skips the read
        // entirely when there are no tails at all, so a kernel that never
        // sees a subscribe does zero extra IO and existing pinned seeds
        // replay unchanged (kernel.rs's `loop_replays_identically_...`).
        if !self.tails.is_empty() {
            let next_event_id = self.app.db().next_event_id()?;
            if self.tails.iter().any(|t| t.next_id < next_event_id) {
                ready.push(Ready::Tail);
            }
        }
        if ready.is_empty() {
            // The loop is about to park, which is the archive's retry
            // pacing: whatever `wait_until` does with the deadline below is
            // what separates one failed store attempt from the next.
            self.archive_retry_parked = false;
            // Gate the job/cron deadlines on `writable` too: a node with a
            // due-but-gated job must not busy-spin on a deadline already in the
            // past — the park tick (below) is what wakes it to re-check.
            let mut deadline = [
                writable.then(|| self.dispatcher.next_deadline()).flatten(),
                writable.then(|| self.cron.next_deadline()).flatten(),
                self.repl.as_ref().and_then(|r| r.next_deadline(now)),
            ]
            .into_iter()
            .flatten()
            .min()
            .map(Instant::from_nanos);
            // The replication park tick (spec §3's accepted wart): a
            // repl-configured node never parks past one tick, so in-flight sim
            // messages and prod TCP are both polled within `REPL_TICK_NANOS`.
            if self.repl.is_some() {
                let tick = Instant::from_nanos(now + REPL_TICK_NANOS);
                deadline = Some(deadline.map_or(tick, |d| d.min(tick)));
            }
            return Ok(Step::Idle { deadline });
        }
        // The same-shape decision site (DESIGN §1): one entropy draw
        // picks among ready sources, only when contended — an idle
        // iteration (exactly one ready source) must not consume entropy.
        let choice = if ready.len() == 1 {
            ready[0]
        } else {
            ready[(self.app.entropy().next_u64() % ready.len() as u64) as usize]
        };
        match choice {
            Ready::Request => {
                // Drain into ONE group, commit once, then reply (spec §1).
                //
                // Bounded by requests POLLED, not by records staged: a
                // handler that returns `Err` or writes nothing stages no
                // record, so a record-bounded loop would keep draining while
                // `poll()` returns `Some` — holding one `Request` (up to the
                // 1 MiB body cap) and one `Reply` per item. Counting
                // requests bounds both. Do not "simplify" this to a
                // staged-record check; that restores the unbounded case.
                // Every request in this batch is handed the BATCH's `now`,
                // not its own arrival time, so sessions minted in one batch
                // share an `expires_at` and lazy expiry is evaluated at one
                // instant for all of them. Harmless at the TTL scale this
                // runs at (seconds to days), but it is a real change from
                // one-clock-read-per-request and is deliberate, not an
                // oversight.
                // A snapshot failure here is ONE request's 500, not the
                // kernel's death (T6 review I-3). Before this task the same
                // failure reached `handle_or_subscribe`, which absorbed it
                // into the named 500 and let the poison check below decide
                // whether to stop — a non-poisoning `Store::snapshot` error
                // was survivable and the node kept serving. A bare `?` here
                // would kill the node before `self.pending` is even taken,
                // leaving that request unanswered and escaping `run` with a
                // raw `io::Error` that names neither contract nor fix.
                let (mut group, staged) = match self.app.begin_group() {
                    Ok(pair) => pair,
                    Err(e) => {
                        let (_, reply) = self.pending.take().expect("checked above");
                        reply.send(io_failure_response(&e));
                        self.stats.requests += 1;
                        return Ok(Step::Worked);
                    }
                };
                let mut handled = Vec::with_capacity(4);
                let mut replies = Vec::with_capacity(4);
                // What each request SAW and whether it wrote: `(records the
                // group already held, staged)`. NOT the request's position —
                // `CommitFailure::applied` counts RECORDS and the two are
                // not 1:1, since a read-only request or one whose handler
                // returned `Err` stages none.
                //
                // A read's `records_before` is load-bearing, not bookkeeping:
                // `Group::stage` points every `tx` at the group overlay, so a
                // read running after a sibling's write returns that sibling's
                // UNCOMMITTED effects and is invalidated with it (T6 review
                // C-1). Only a request that ran ahead of every staged record
                // saw purely committed state.
                let mut saw = Vec::with_capacity(4);
                let mut next = Some(self.pending.take().expect("checked above"));
                while let Some((request, reply)) = next.take() {
                    let before = group.len();
                    let outcome = staged.stage_or_subscribe(&mut group, request, now);
                    // A subscribe ENDS the batch. Its cursor is resolved
                    // after the commit (`validate_subscribe` needs `&Db`,
                    // which the open group holds), so any sibling staged
                    // BEHIND it would have its events committed before that
                    // resolution — and the tail would start past them,
                    // silently missing events it must receive. That is a
                    // gap, exactly what realtime's membership oracle
                    // forbids. Ending the batch here reproduces the
                    // unbatched order the cursor's meaning assumes: whatever
                    // committed before this request is behind the cursor,
                    // whatever comes after is delivered.
                    let ends_batch = matches!(outcome, Handled::Subscribe(_));
                    handled.push(outcome);
                    saw.push((before, group.len() > before));
                    replies.push(reply);
                    if !ends_batch && handled.len() < MAX_BATCH {
                        next = source.poll();
                    }
                }
                let drained = handled.len();
                // Nothing has been acked yet — the replies are still held.
                // `commit` is the only point after which any of them may be
                // sent, which is ack-after-durable stated structurally.
                // `applied` inside is COUNTED by the engine, never re-derived
                // from `last_applied()` before-vs-after.
                let failure = group.commit().err();
                for ((outcome, reply), (before, staged)) in
                    handled.into_iter().zip(replies).zip(saw)
                {
                    // A request keeps its response unless a record it saw —
                    // or its own — is the one left uncertain. The same rule
                    // `Db::transact` and `App::handle_or_subscribe` apply to
                    // their batch of one, so no path can drift. The 500 means
                    // "unknown", never "rejected": per spec §4 the record may
                    // still be durable and apply on restart.
                    if let Some(failure) = &failure
                        && failure.invalidates(before, staged)
                    {
                        reply.send(io_failure_response(&failure.error));
                        continue;
                    }
                    match outcome {
                        Handled::Response(response) => reply.send(response),
                        Handled::Subscribe(sub) => {
                            match validate_subscribe(self.app.db(), sub.cursor) {
                                Ok(next_id) => {
                                    let sink = reply.stream(Response::sse_headers());
                                    self.tails.push(Tail { sub, sink, next_id });
                                }
                                Err(response) => reply.send(response),
                            }
                        }
                    }
                }
                self.stats.requests += drained as u64;
                // Spec 2026-07-05 (request-arm ruling): a request-path Io
                // is absorbed into a 500 for the client, but a poisoned
                // engine serves no one — stop; restart-and-recover is the
                // only exit from poison. Non-poisoning faults continue.
                if self.app.db_mut().poisoned() {
                    // Check the store cause FIRST: a latched store can also
                    // poison the engine (its apply fails too), and the store
                    // latch is the root cause — reporting apply-poison there
                    // would tell the operator durability is unknowable when
                    // the WAL is in fact intact.
                    let store = self.app.db_mut().store_failed();
                    // The parenthetical MUST keep redb's own remedy prose
                    // ("close and re-open the database") verbatim: that is the
                    // needle `checks::classify` matches to call a latch fault
                    // fallout rather than an unexplained bug (checks.rs:72),
                    // and spec §8 makes touching that matcher a non-goal. A
                    // paraphrase here reads fine and silently fails every sim
                    // universe whose injected fault latches the store.
                    return Err(io::Error::other(if store {
                        "kernel stopped: the store handle is dead after a disk IO failure \
                         (redb refuses every further operation until you close and re-open \
                         the database) — durable state is intact and the WAL is \
                         authoritative; restart the process to recover"
                    } else {
                        "kernel stopped: the engine is poisoned (a request-path IO failure \
                         made durability unknowable) — the request received its 500; \
                         restart the process to recover from the WAL"
                    }));
                }
            }
            Ready::Job => {
                // Drain into ONE group, commit once (spec §1) — the same
                // shape as the request arm, adapted for the one thing that
                // differs: `run_due_into` is fallible PER ITEM (a platform
                // read fault mid-handler, which `ReadFaults` in
                // tests/kernel.rs drives under the fault matrix), where the
                // request arm's `stage_or_subscribe` never returns a bare
                // `Result`. A `?` straight through this loop would return
                // from `step` with `group` still alive and non-empty —
                // `Group::Drop` panics on exactly that, and the panic names
                // none of `checks::classify`'s needles, so every injected
                // fault in a multi-job batch would report as a real bug
                // (group-commit T7, C-1). Fixed by commit-then-bubble: drain
                // in an inner closure, commit UNCONDITIONALLY once it
                // returns — whether or not it errored — and combine the two
                // results, the commit's own failure winning (it is the one
                // that says whether anything reached the store). The Cron
                // arm below has the identical shape for the identical
                // reason.
                //
                // Bounded by JOBS RUN, not by records staged: a job
                // contributes at most one record but calls `stage` up to
                // TWICE (its own attempt, then a separate failure-record
                // transaction on an App rejection — see
                // `Dispatcher::run_due_into`'s doc). Counting jobs run still
                // bounds records at MAX_BATCH (each job nets ≤ 1 record),
                // the same guarantee the request arm's poll-count bound
                // gives by a different argument — this is NOT a repeat of
                // that reasoning and is written down so the next reader
                // does not assume it is.
                let mut group = self.app.db_mut().begin_group()?;
                let mut drained = 0usize;
                let drain = (|| -> io::Result<()> {
                    while self.dispatcher.has_due(now) && drained < MAX_BATCH {
                        match self.dispatcher.run_due_into(&mut group, now)? {
                            // Stats increment as jobs are STAGED, before the
                            // batch commits (accepted, T7 pre-flight §3): if
                            // the commit below then fails, these counters
                            // claim N done against 0 (or an uncertain number)
                            // durable — and the DUE-HEAP is left stale the
                            // same way, and it is the more consequential of
                            // the two (fix round M-3): a job this loop's
                            // `run_due_into` removed as Done never comes
                            // back, and a `Retried` job's speculative NEW
                            // `due_at` stays in the heap too. Both are safe
                            // only because this arm returns `Err` on ANY
                            // commit failure below (deliberately NOT the
                            // wrapper's finer `invalidates` policy — see the
                            // comment on that `match` for why), so the
                            // kernel stops right after, and a restart rebuilds
                            // the heap from the store (the only truth) at
                            // `Kernel::build`. `KernelStats`/`Kernel::stats()`
                            // and `Kernel::step()` are both `pub`, so nothing
                            // in the type system stops an external caller
                            // from reading these counters after a `step()`
                            // error (unlike, say, a private field) — so this
                            // is not "nothing reads it", it is "nothing reads
                            // it before a restart resets it", which is what
                            // stopping the kernel guarantees.
                            JobOutcome::Done { .. } => self.stats.jobs_done += 1,
                            JobOutcome::Retried { .. } => self.stats.jobs_retried += 1,
                            JobOutcome::Dead { .. } => self.stats.jobs_dead += 1,
                        }
                        drained += 1;
                    }
                    Ok(())
                })();
                match (drain, group.commit()) {
                    (Ok(()), Ok(())) => {}
                    // Fix round I-2: deliberately does NOT apply
                    // `CommitFailure::invalidates` the way `Db::transact` and
                    // the one-job/one-schedule wrappers now do (dispatch.rs's
                    // `run_due`, cron.rs's `tick`). ANY commit failure here
                    // stops the kernel outright — consistent with M2
                    // sub-project 0a's "stop on the latch" ruling, and it
                    // strictly reduces the window the wrappers' finer answer
                    // exists to narrow (a checkpoint-arm failure here would
                    // otherwise leave `self.stats`/the due-heap stale, per
                    // the comment above, until the very next commit failed
                    // anyway — pre-T7 behaviour). Reviewed and accepted (T7
                    // fix round I-2), not an oversight; the Cron arm below
                    // answers the identical way for the identical reason.
                    (Ok(()), Err(failure)) => return Err(failure.error),
                    (Err(e), Ok(())) => return Err(e),
                    (Err(_), Err(failure)) => return Err(failure.error),
                }
            }
            Ready::Cron => {
                // Identical shape to the Job arm above, for the identical
                // reason (`CronScheduler::tick_into`'s `io::Result` is the
                // same fallible-per-item hazard C-1 names). Ruled (Noah,
                // 2026-08-12): drain it for uniformity, not throughput — the
                // EXPECTED WIN HERE IS ZERO. `tick_into` services one entry
                // per call and pushes its `next_due` past `now`, so the max
                // cron batch is `entries.len()`, ~2 in practice (the app's
                // own schedule plus the platform's built-in daily session
                // sweep), and the two phases drift apart at nanosecond
                // resolution and essentially never co-occur. Leaving one arm
                // on `db.transact` while the other two stage is the
                // asymmetry someone "cleans up" later not knowing it was
                // deliberate — do not measure this arm and report a
                // disappointing number as a regression.
                let mut group = self.app.db_mut().begin_group()?;
                let mut drained = 0usize;
                let drain = (|| -> io::Result<()> {
                    while self.cron.has_due(now) && drained < MAX_BATCH {
                        let (_, catchup) = self.cron.tick_into(&mut group, now)?;
                        self.stats.cron_ticks += 1;
                        if catchup {
                            self.stats.cron_catchups += 1;
                        }
                        drained += 1;
                    }
                    Ok(())
                })();
                match (drain, group.commit()) {
                    (Ok(()), Ok(())) => {}
                    // Fix round I-2: same deliberate divergence from
                    // `CommitFailure::invalidates` as the Job arm above, for
                    // the identical reason — see that arm's comment. ANY
                    // commit failure here stops the kernel.
                    (Ok(()), Err(failure)) => return Err(failure.error),
                    (Err(e), Ok(())) => return Err(e),
                    (Err(_), Err(failure)) => return Err(failure.error),
                }
            }
            Ready::Repl => {
                let repl = self.repl.as_mut().expect("Repl arm only pushed when Some");
                // A `Refuse`-diverged or fault error bubbles like the job arm's
                // — restart-and-recover is the only exit (DESIGN §5).
                repl.step(self.app.db_mut(), &*self.clock)?;
                if repl.db_needs_reseed() {
                    // `run_seed` consumes and returns the `Db` (it cannot
                    // replace one it only borrows); the swap is adjacent so
                    // `db()`/`db_mut()` never observe the empty slot.
                    let db = self.app.take_db();
                    let db = repl.run_seed(db, &*self.clock)?;
                    self.app.put_db(db);
                }
                self.after_repl_step()?;
            }
            Ready::Tail => {
                let next_event_id = self.app.db().next_event_id()?;
                let n = self.tails.len();
                let idx = (0..n)
                    .map(|i| (self.tail_cursor + i) % n)
                    .find(|&i| self.tails[i].next_id < next_event_id)
                    .expect("Ready::Tail only pushed when some tail is behind");
                let survives = tail::deliver_unit(&mut self.tails[idx], self.app.db())?;
                self.tail_cursor = (idx + 1) % n;
                if !survives {
                    self.tails.swap_remove(idx);
                }
            }
            Ready::Archive => {
                let (archive, store) = self
                    .archive
                    .as_mut()
                    .expect("Archive arm only pushed when Some");
                // ONE `step` per iteration, never a loop to completion.
                // `Archive::step`'s own `read_budget_bytes` exists precisely
                // so no single call reads a whole 64 MiB segment, and that
                // bound is worth nothing if the arm drains the archive here.
                let outcome = archive.step(self.app.db(), &**store);
                let (idle, gapped, stopped) = (archive.idle(), archive.gapped(), archive.stopped());
                self.stats.archive_steps += 1;
                // The watermark the ready predicate reads. Taken AFTER the
                // step, against the `Db` the step actually saw.
                self.archive_idle_at = idle.then(|| self.app.db().last_applied());
                // Spec §7's last row and §8's first invariant: an archive
                // failure is NOT a database failure. A `?` here would kill a
                // serving node over a transient PUT, which is precisely the
                // power spec §5 says a derived artifact must not have over
                // the thing it derives from. So the arm absorbs every error
                // — and the archive itself retries the same key with the
                // same bytes, so absorbing loses nothing.
                //
                // The two conditions spec §7 says DO produce an operator
                // notice — a gap, and a permanent ownership refusal — are
                // raised on the false->true EDGE of `gapped()`/`stopped()`,
                // which is exact: both flags are set by the very step that
                // returns the notice as its `Err`. Edge-triggering is what
                // makes it "one notice" in the two cases that matter: the
                // gap churn described in `yield_or_bound` (measured at 750
                // notices over 3,000 steps) collapses to one, and a
                // within-bound retry storm produces none at all, which is
                // what §7's first row asks for.
                if let Err(e) = outcome {
                    if (gapped && !self.archive_gapped) || (stopped && !self.archive_stopped) {
                        // Written from inside the loop for the reason
                        // `Repl::emit_witness_notice` gives for the same
                        // choice: it is produced mid-run and no driver polls
                        // the kernel while it is looping, so an operator
                        // would otherwise have nothing to read. The string
                        // is also left on `archive_notice` for a driver that
                        // would rather route it.
                        eprintln!("charpente archive: {e}");
                        self.archive_notice = Some(e.to_string());
                    }
                    self.archive_retry_parked = true;
                }
                self.archive_gapped = gapped;
                self.archive_stopped = stopped;
            }
        }
        // Any serviced source may have enqueued (handlers can enqueue too), so
        // this runs after every branch above. A follower's shipped applies also
        // fill the buffer, but a follower never runs jobs — drain it either way
        // to keep it bounded and feed the heap only while writable (becoming
        // writable rebuilds the heap from the store in `after_repl_step`).
        let enqueued = self.app.db_mut().drain_enqueued();
        if writable {
            self.dispatcher.note_enqueued(enqueued);
        }
        Ok(Step::Worked)
    }

    /// After a `Repl::step`, catch this node BECOMING WRITABLE — a promote, a
    /// switchover hand-off, or the witness boot gate resolving — and run the
    /// three pieces of startup a non-writable node had to skip:
    ///
    /// - the schema gate (`Kernel::run` guards it on `writes_allowed()` and
    ///   never retries it),
    /// - `cron.upsert` (`Kernel::build` guards it the same way, and a first
    ///   `Ready::Cron` without it panics on the missing row),
    /// - the due-heap rebuild from the replicated store — the store's Pending
    ///   rows are the only truth, and `rebuild` only enqueues Pending ones, so
    ///   a job the old primary already marked Done is never resurrected.
    ///
    /// Keyed on the write gate, not the role: a witness-gated node's role is
    /// `Primary` from open, so the role edge this used to watch never fires
    /// for it and all three would be skipped forever — silently, in the
    /// migration's case. `do_promote` also runs the schema gate itself before
    /// acking the control verb; running it a second time here is a no-op walk
    /// over an already-current schema, and keeping both means neither call
    /// site has to know about the other.
    fn after_repl_step(&mut self) -> io::Result<()> {
        let writable = self.app.db().writes_allowed();
        if writable && !self.was_writable {
            self.app.db_mut().migrate_schema()?;
            let now = self.clock.now().as_nanos();
            self.cron.upsert(self.app.db_mut(), now)?;
            let snap = self.app.db_mut().snapshot()?;
            self.dispatcher.rebuild(&snap)?;
        }
        self.was_writable = writable;
        Ok(())
    }

    pub fn stats(&self) -> KernelStats {
        self.stats
    }

    /// The last notice the archive arm raised (a gap, or a permanent
    /// ownership refusal), taken. `None` forever on a kernel with no
    /// archive, and `None` between notices. Mirrors
    /// `Repl::take_witness_notice`.
    pub fn take_archive_notice(&mut self) -> Option<String> {
        self.archive_notice.take()
    }

    /// Borrows the mounted App — tests read `db().last_applied()`/snapshots
    /// without consuming the kernel (`into_app` is the consuming form).
    #[doc(hidden)]
    pub fn app(&self) -> &App {
        &self.app
    }

    /// Tests that drive the write gate or write records mid-run need the
    /// `Db` mutably; `app()` is the read-only form.
    #[doc(hidden)]
    pub fn app_mut_for_tests(&mut self) -> &mut App {
        &mut self.app
    }

    /// Lets a test ask the archive itself (`gapped()`, `stopped()`,
    /// `pin_floor()`) without consuming the kernel.
    #[doc(hidden)]
    pub fn archive_for_tests(&self) -> Option<&Archive> {
        self.archive.as_ref().map(|(a, _)| a)
    }

    /// True for a live primary — `false` on a follower or a draining primary.
    /// Mirrors `Repl::is_primary`; a non-replicated kernel is always primary.
    pub fn is_primary(&self) -> bool {
        self.repl.as_ref().is_none_or(Repl::is_primary)
    }

    /// Hands the mounted App back for post-run inspection in tests.
    pub fn into_app(self) -> App {
        self.app
    }
}

/// Registration-time validation for an SSE subscribe (T5, DESIGN §6), reading
/// off the `Db` snapshot: a present cursor must sit at or above this node's
/// retention floor (checked first — a cursor that old is simply gone, no
/// finer diagnosis is possible), and if it clears that bar, the retained row
/// at its id must carry its exact seq (checked second — a cursor ahead of
/// everything this node has ever minted, and a cursor whose id landed on a
/// different event than the one the client saw, are both a diverged
/// timeline, distinguished only by the message's `{detail}`). No cursor at
/// all always succeeds, positioned to tail from now. Returns the tail's
/// starting `next_id` on success.
fn validate_subscribe(db: &Db, cursor: Option<Cursor>) -> Result<u64, Response> {
    let next_event_id = db.next_event_id().map_err(io_500)?;
    let Some(cursor) = cursor else {
        return Ok(next_event_id);
    };
    let floor = db.event_floor().map_err(io_500)?;
    if cursor.id < floor {
        return Err(Response::error(
            409,
            &format!(
                "cursor {} is below this node's retention floor {floor} — events that old are \
                 gone; re-sync current state through reads, then reconnect without a cursor",
                cursor.wire()
            ),
        ));
    }
    let snap = db.snapshot().map_err(io_500)?;
    let rows: Vec<EventRow> = snap.scan_from(cursor.id, 1).map_err(io_500)?;
    match rows.first() {
        Some(row) if row.id.raw() == cursor.id => {
            if row.seq != cursor.seq {
                return Err(Response::error(
                    409,
                    &format!(
                        "cursor {} diverged from this node's timeline (event {} here has seq \
                         {}, not {}) — your history came from another primary; re-sync current \
                         state through reads, then reconnect without a cursor",
                        cursor.wire(),
                        cursor.id,
                        row.seq,
                        cursor.seq
                    ),
                ));
            }
            Ok(cursor.id + 1)
        }
        _ => Err(Response::error(
            409,
            &format!(
                "cursor {} diverged from this node's timeline (next event id is {next_event_id}) \
                 — your history came from another primary; re-sync current state through reads, \
                 then reconnect without a cursor",
                cursor.wire()
            ),
        )),
    }
}

fn io_500(e: io::Error) -> Response {
    Response::error(
        500,
        &format!("internal storage failure validating the subscribe cursor — retry: {e}"),
    )
}

/// `(status, body)` pairs `SimSource` records — named so its `Arc<Mutex<_>>`
/// doesn't trip clippy's type-complexity lint at every use site.
pub type RecordedResponses = Arc<Mutex<Vec<(u16, Vec<u8>)>>>;

/// `SimSinkHandle`s `SimSource` hands back, one per SSE upgrade it has
/// produced, in upgrade order — the realtime counterpart to
/// `RecordedResponses`.
pub type SinkHandles = Arc<Mutex<Vec<SimSinkHandle>>>;

/// One workload-supplied `SimSink` stall closure, queued for the next SSE
/// upgrade `SimSource` produces.
type StallQueue = Arc<Mutex<VecDeque<Box<dyn FnMut() -> bool + Send>>>>;

/// Deterministic scripted source for sim tests: requests arrive at virtual
/// times; replies record `(status, body)`; the horizon bounds the universe.
pub struct SimSource {
    clock: Box<dyn Clock>,
    script: VecDeque<(u64, Request)>,
    responses: RecordedResponses,
    horizon: u64,
    stalls: StallQueue,
    sinks: SinkHandles,
}

impl SimSource {
    pub fn new(clock: Box<dyn Clock>, horizon_nanos: u64) -> SimSource {
        SimSource {
            clock,
            script: VecDeque::new(),
            responses: Arc::new(Mutex::new(Vec::new())),
            horizon: horizon_nanos,
            stalls: Arc::new(Mutex::new(VecDeque::new())),
            sinks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Requests must be pushed in arrival order (asserted).
    pub fn push_at(&mut self, at_nanos: u64, request: Request) {
        assert!(
            self.script.back().is_none_or(|&(t, _)| t <= at_nanos),
            "SimSource script must be pushed in arrival order"
        );
        self.script.push_back((at_nanos, request));
    }

    pub fn responses(&self) -> RecordedResponses {
        self.responses.clone()
    }

    /// Queues a `SimSink` stall behavior for this source's next SSE upgrade
    /// (FIFO — one entry per `Respond::stream` call, in the order those
    /// upgrades happen; a request that never reaches an SSE route leaves the
    /// queue untouched). An upgrade with nothing queued gets a sink that
    /// never stalls. This is the seam onto a subscribe's delivery: a
    /// workload queues a stall, scripts the SSE request that will consume
    /// it, then reads the matching `SimSinkHandle` back from `sinks()` once
    /// the kernel has run.
    pub fn push_stall(&mut self, stall: Box<dyn FnMut() -> bool + Send>) {
        self.stalls
            .lock()
            .expect("stall queue lock poisoned by an earlier panic")
            .push_back(stall);
    }

    /// `SimSinkHandle`s for every SSE upgrade this source has produced so
    /// far, in upgrade order.
    pub fn sinks(&self) -> SinkHandles {
        self.sinks.clone()
    }
}

impl RequestSource for SimSource {
    fn poll(&mut self) -> Option<(Request, Reply)> {
        let now = self.clock.now().as_nanos();
        if self.script.front().is_some_and(|&(at, _)| at <= now) {
            let (_, request) = self.script.pop_front().expect("checked");
            let reply: Reply = Box::new(SimReply {
                sink: self.responses.clone(),
                stalls: self.stalls.clone(),
                sinks: self.sinks.clone(),
            });
            return Some((request, reply));
        }
        None
    }

    fn wait_until(&mut self, deadline: Option<Instant>) -> Wake {
        let now = self.clock.now().as_nanos();
        let arrival = self.script.front().map(|&(at, _)| at);
        let target = [arrival, deadline.map(Instant::as_nanos)]
            .into_iter()
            .flatten()
            .min();
        match target {
            Some(t) if t <= self.horizon => {
                if t > now {
                    self.clock.sleep(std::time::Duration::from_nanos(t - now));
                }
                if arrival == Some(t) {
                    Wake::Ready
                } else {
                    Wake::Deadline
                }
            }
            _ => Wake::Shutdown, // nothing left inside this universe's horizon
        }
    }
}

/// `SimSource`'s `Respond`: `send` is the existing recorded-response
/// behavior; `stream` records the SSE upgrade's opening response the same
/// way, then hands back a real `SimSink` — stallable per a workload-queued
/// closure (`SimSource::push_stall`), defaulting to one that never stalls —
/// and stashes the matching `SimSinkHandle` for `SimSource::sinks()`.
struct SimReply {
    sink: RecordedResponses,
    stalls: StallQueue,
    sinks: SinkHandles,
}

impl Respond for SimReply {
    fn send(self: Box<Self>, response: Response) {
        self.sink
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .push((response.status, response.body));
    }

    fn stream(self: Box<Self>, response: Response) -> Box<dyn EventSink> {
        self.sink
            .lock()
            .expect("responses lock poisoned by an earlier panic")
            .push((response.status, response.body));
        let stall = self
            .stalls
            .lock()
            .expect("stall queue lock poisoned by an earlier panic")
            .pop_front()
            .unwrap_or_else(|| Box::new(|| false) as Box<dyn FnMut() -> bool + Send>);
        let (sink, handle) = SimSink::new(stall);
        self.sinks
            .lock()
            .expect("sink handle list lock poisoned by an earlier panic")
            .push(handle);
        Box::new(sink)
    }
}
