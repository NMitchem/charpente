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
use charpente_core::io::{Clock, Instant};
use charpente_core::table::View;

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
}

#[derive(Clone, Copy)]
enum Ready {
    Request,
    Job,
    Cron,
    Repl,
    Tail,
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
}

impl Kernel {
    /// Consumes the mounted App: upserts cron rows, rebuilds the due-heap
    /// from Pending rows. `now` for the cron phase comes from `clock`. The
    /// single-node constructor — no replication.
    pub fn new(app: App, clock: Box<dyn Clock>) -> io::Result<Kernel> {
        Kernel::build(app, clock, None)
    }

    /// The replicated constructor: same setup, plus a `Repl` whose role gates
    /// the ready set (a follower serves reads and ships, never runs jobs/cron)
    /// and whose park tick (`REPL_TICK_NANOS`) bounds every idle wait so
    /// in-flight replication IO is polled promptly.
    pub fn with_replication(app: App, clock: Box<dyn Clock>, repl: Repl) -> io::Result<Kernel> {
        Kernel::build(app, clock, Some(repl))
    }

    fn build(mut app: App, clock: Box<dyn Clock>, repl: Option<Repl>) -> io::Result<Kernel> {
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
        let mut ready = Vec::with_capacity(4);
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
            Ready::Job => match self.dispatcher.run_due(self.app.db_mut(), now)? {
                JobOutcome::Done { .. } => self.stats.jobs_done += 1,
                JobOutcome::Retried { .. } => self.stats.jobs_retried += 1,
                JobOutcome::Dead { .. } => self.stats.jobs_dead += 1,
            },
            Ready::Cron => {
                let (_, catchup) = self.cron.tick(self.app.db_mut(), now)?;
                self.stats.cron_ticks += 1;
                if catchup {
                    self.stats.cron_catchups += 1;
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

    /// Borrows the mounted App — tests read `db().last_applied()`/snapshots
    /// without consuming the kernel (`into_app` is the consuming form).
    #[doc(hidden)]
    pub fn app(&self) -> &App {
        &self.app
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
