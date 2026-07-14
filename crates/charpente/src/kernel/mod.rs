//! The deterministic kernel (DESIGN §1, spec 2026-07-02-queue-cron-design.md):
//! the event loop multiplexing HTTP requests, due jobs, and cron ticks — all
//! inside the simulation boundary.

pub(crate) mod cron;
mod dispatch;

pub use cron::{CronRow, CronScheduler};
pub use dispatch::{Dispatcher, JobDef, JobEntry, JobError, JobOutcome};

use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Mutex};

use charpente_core::io::{Clock, Instant};

use crate::http::{App, Request, Response};
use crate::repl::{REPL_TICK_NANOS, Repl};

/// Delivers a `Response` back to whoever is waiting on a request — a channel
/// reply in production, a recorded `(status, body)` in the sim.
pub type Reply = Box<dyn FnOnce(Response) + Send>;

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
    /// Whether the last iteration saw this node as a live primary. A
    /// follower→primary flip (promote or a switchover hand-off, both inside
    /// `Repl::step`) rebuilds the due-heap and cron from the replicated store
    /// — the only truth for a promoted node.
    was_primary: bool,
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
        let was_primary = repl.as_ref().is_none_or(Repl::is_primary);
        // `cron.upsert` writes the schedule rows — skip it on a follower (its
        // `Db` is write-stopped, and the rows arrive by replication anyway); a
        // promotion re-runs it in `after_repl_step`. `dispatcher.rebuild` is a
        // read, always safe.
        if was_primary {
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
            was_primary,
        })
    }

    /// The event loop (DESIGN §1 same-shape rule): identical code under
    /// `serve()` and under the sim; only the `RequestSource` impl differs.
    /// Returns `Ok(())` on source `Shutdown`; `Err` on platform IO failure
    /// (poisoned engine — restart to recover). Thin wrapper over `step`: it
    /// owns the park, `step` owns the decision.
    pub fn run(&mut self, source: &mut dyn RequestSource) -> io::Result<()> {
        loop {
            match self.step(source)? {
                Step::Worked => {}
                Step::Idle { deadline } => {
                    if let Wake::Shutdown = source.wait_until(deadline) {
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
        // A follower (or a draining primary — `is_primary` is false while
        // handing off) serves reads and ships, but never runs jobs or cron:
        // those transactions would hit `TxError::NotPrimary` (DESIGN §5).
        let primary = self.repl.as_ref().is_none_or(Repl::is_primary);
        let mut ready = Vec::with_capacity(4);
        if self.pending.is_some() {
            ready.push(Ready::Request);
        }
        if primary && self.dispatcher.has_due(now) {
            ready.push(Ready::Job);
        }
        if primary && self.cron.has_due(now) {
            ready.push(Ready::Cron);
        }
        if self
            .repl
            .as_ref()
            .is_some_and(|r| r.has_work(self.app.db(), now))
        {
            ready.push(Ready::Repl);
        }
        if ready.is_empty() {
            // Gate the job/cron deadlines on `primary` too: a follower with a
            // due-but-gated job must not busy-spin on a deadline already in the
            // past — the park tick (below) is what wakes it to re-check.
            let mut deadline = [
                primary.then(|| self.dispatcher.next_deadline()).flatten(),
                primary.then(|| self.cron.next_deadline()).flatten(),
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
                let (request, reply) = self.pending.take().expect("checked above");
                let response = self.app.handle(request);
                reply(response);
                self.stats.requests += 1;
                // Spec 2026-07-05 (request-arm ruling): a request-path Io
                // is absorbed into a 500 for the client, but a poisoned
                // engine serves no one — stop; restart-and-recover is the
                // only exit from poison. Non-poisoning faults continue.
                if self.app.db_mut().poisoned() {
                    return Err(io::Error::other(
                        "kernel stopped: the engine is poisoned (a request-path IO failure \
                         made durability unknowable) — the request received its 500; \
                         restart the process to recover from the WAL",
                    ));
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
        }
        // Any serviced source may have enqueued (handlers can enqueue too), so
        // this runs after every branch above. A follower's shipped applies also
        // fill the buffer, but a follower never runs jobs — drain it either way
        // to keep it bounded and feed the heap only while primary (a promotion
        // rebuilds the heap from the store in `after_repl_step`).
        let enqueued = self.app.db_mut().drain_enqueued();
        if primary {
            self.dispatcher.note_enqueued(enqueued);
        }
        Ok(Step::Worked)
    }

    /// After a `Repl::step`, catch a follower→primary flip (promote, or a
    /// switchover hand-off): rebuild the due-heap and cron from the replicated
    /// store — the store's Pending rows are the only truth, and `rebuild` only
    /// enqueues Pending ones, so a job the old primary already marked Done is
    /// never resurrected onto the heap.
    fn after_repl_step(&mut self) -> io::Result<()> {
        let primary = self.repl.as_ref().is_none_or(Repl::is_primary);
        if primary && !self.was_primary {
            let now = self.clock.now().as_nanos();
            self.cron.upsert(self.app.db_mut(), now)?;
            let snap = self.app.db_mut().snapshot()?;
            self.dispatcher.rebuild(&snap)?;
        }
        self.was_primary = primary;
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

/// `(status, body)` pairs `SimSource` records — named so its `Arc<Mutex<_>>`
/// doesn't trip clippy's type-complexity lint at every use site.
pub type RecordedResponses = Arc<Mutex<Vec<(u16, Vec<u8>)>>>;

/// Deterministic scripted source for sim tests: requests arrive at virtual
/// times; replies record `(status, body)`; the horizon bounds the universe.
pub struct SimSource {
    clock: Box<dyn Clock>,
    script: VecDeque<(u64, Request)>,
    responses: RecordedResponses,
    horizon: u64,
}

impl SimSource {
    pub fn new(clock: Box<dyn Clock>, horizon_nanos: u64) -> SimSource {
        SimSource {
            clock,
            script: VecDeque::new(),
            responses: Arc::new(Mutex::new(Vec::new())),
            horizon: horizon_nanos,
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
}

impl RequestSource for SimSource {
    fn poll(&mut self) -> Option<(Request, Reply)> {
        let now = self.clock.now().as_nanos();
        if self.script.front().is_some_and(|&(at, _)| at <= now) {
            let (_, request) = self.script.pop_front().expect("checked");
            let sink = self.responses.clone();
            let reply: Reply = Box::new(move |response: Response| {
                sink.lock()
                    .expect("responses lock poisoned by an earlier panic")
                    .push((response.status, response.body));
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
