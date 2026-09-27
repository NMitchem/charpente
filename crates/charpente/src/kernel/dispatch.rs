//! Job dispatch types. The Dispatcher itself lands with the due-heap task.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};

use charpente_core::db::{CrossTenant, Db, Group, JobShapeRefusal, Tx, TxError};
use charpente_core::job::{JobRow, JobStatus, RetryPolicy};
use charpente_core::table::{Id, Snapshot, View};

/// Why a job run did not complete. The App/Io split mirrors `TxError` and is
/// load-bearing: `App` is the handler's own rejection (or an absorbed panic,
/// a payload that does not decode, or the handler's transaction refused for
/// enqueueing another shape of a mounted job) — a real attempt that feeds
/// retry budget and lands in the row's `last_error`. `Io` is a platform fault
/// surfaced through the handler's reads — NOT an attempt: the job stays
/// Pending, nothing is recorded, the error bubbles (restart-and-recover).
/// Folding Io into the retry path would let injected faults Dead-letter
/// healthy jobs.
pub enum JobError {
    App(String),
    Io(io::Error),
}

impl JobError {
    pub fn app(message: impl Into<String>) -> JobError {
        JobError::App(message.into())
    }
}

impl From<io::Error> for JobError {
    fn from(e: io::Error) -> JobError {
        JobError::Io(e)
    }
}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobError::App(message) => f.write_str(message),
            JobError::Io(e) => {
                write!(
                    f,
                    "platform IO failure during the handler (not an attempt): {e}"
                )
            }
        }
    }
}

impl fmt::Debug for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JobError({self})")
    }
}

/// One registered job type: the descriptor `jobs![…]` collects (explicit
/// registration — distributed registration stays rejected, like routes).
pub struct JobDef {
    pub name: &'static str,
    pub retry: RetryPolicy,
    /// `#[job(cross_tenant)]` — the same declared capability a route carries
    /// (spec §3), for the background rollups that need it. `describe` lists
    /// it beside the route flag, so the cross-tenant inventory covers jobs
    /// too.
    pub cross_tenant: bool,
    /// `Job::FINGERPRINT` — the payload shape this binary compiled. Dispatch
    /// asserts a row's recorded shape equals it before the glue decodes
    /// (payload-shapes spec §4).
    pub fingerprint: u64,
    /// The token is `Some` exactly when `cross_tenant` is set: the glue
    /// `#[job]` emits passes it on to the payload's `run` only for a
    /// declared job, and a job that never declared it is handed `None` it
    /// has no way to use.
    pub glue: JobGlue,
}

/// The glue signature `#[job]` generates: decode the payload, call the
/// payload's `run`, hand back its verdict. Named, like `http::Glue`, so the
/// field above stays under clippy's type-complexity threshold rather than
/// spelling a four-part fn pointer inline.
pub type JobGlue = fn(&mut Tx, &[u8], Option<&CrossTenant>) -> Result<(), JobError>;

/// Implemented by the marker impl `#[job]` emits, so `jobs![a, b]` collects
/// descriptors explicitly.
pub trait JobEntry {
    fn def() -> JobDef;
}

#[macro_export]
macro_rules! jobs {
    ($($job:path),* $(,)?) => {
        ::std::vec![$(<$job as $crate::kernel::JobEntry>::def()),*]
    };
}

/// The result of running the earliest-due job. `Done`/`Retried`/`Dead` are
/// all committed records — a run only fails to produce one of these when the
/// *platform* faults (see `Dispatcher::run_due`).
#[derive(Debug)]
pub enum JobOutcome {
    Done { id: u64 },
    Retried { id: u64, attempts: u32, due_at: u64 },
    Dead { id: u64, attempts: u32 },
}

/// The due-heap: which jobs to run and when. The store (`charpente.jobs`) is
/// the only truth; this is a cache of it, rebuilt at kernel start and kept in
/// sync incrementally by `note_enqueued` and `run_due`.
pub struct Dispatcher {
    defs: BTreeMap<&'static str, JobDef>,
    /// (due_at, id) — BTreeSet keeps the earliest-due first; ids break ties.
    due: BTreeSet<(u64, u64)>,
}

impl Dispatcher {
    pub fn new(defs: Vec<JobDef>) -> Dispatcher {
        let mut map = BTreeMap::new();
        for def in defs {
            let clash = map.insert(def.name, def);
            assert!(
                clash.is_none(),
                "job registered twice — each #[job] type is mounted once in jobs![…]"
            );
        }
        Dispatcher {
            defs: map,
            due: BTreeSet::new(),
        }
    }

    /// Rebuilds the due-heap from Pending rows (called at kernel start — the
    /// store is the only truth; the heap is a cache of it).
    pub fn rebuild(&mut self, snap: &Snapshot) -> io::Result<()> {
        self.due.clear();
        for job in snap.scan::<JobRow>()? {
            if job.status == JobStatus::Pending {
                self.due.insert((job.due_at, job.id.raw()));
            }
        }
        Ok(())
    }

    /// Feeds ids drained from `Db::drain_enqueued` (fresh enqueues are due
    /// immediately: due_at 0).
    pub fn note_enqueued(&mut self, ids: Vec<Id<JobRow>>) {
        for id in ids {
            self.due.insert((0, id.raw()));
        }
    }

    pub fn next_deadline(&self) -> Option<u64> {
        self.due.first().map(|&(due_at, _)| due_at)
    }

    pub fn has_due(&self, now: u64) -> bool {
        self.next_deadline().is_some_and(|d| d <= now)
    }

    /// Runs the earliest due job by STAGING it into `group` instead of
    /// committing on its own (group-commit T7, C-2) — the shared half of
    /// `run_due`'s one-job wrapper and the kernel's batched job-arm drain.
    /// `Ok(outcome)` covers handler success and handler failure (retry/dead
    /// — both are now STAGED records, not yet durable), and a handler whose
    /// own transaction `Group::stage` refused with a `JobShapeRefusal` is
    /// that kind of failure: an attempt, like a panic. `Err` is a PLATFORM
    /// failure (any other `TxError::Io`, or an `Io` the handler's own reads
    /// surfaced): the heap entry is restored to its ORIGINAL `(due_at, id)`
    /// before returning, and the job stays Pending in the store either way,
    /// since nothing this call staged has been committed.
    ///
    /// **Contract for a caller sharing `group` across several jobs** (the
    /// kernel's batched drain): an `Err` return here does NOT mean `group`
    /// is empty — an earlier job in the same batch may already have staged
    /// a record. The caller owns `group.commit()` and must run it whether
    /// or not this returns `Err` — `Group`'s only non-panicking exit is
    /// `commit()`, and dropping a non-empty one panics by contract (C-1).
    ///
    /// **One job calls `group.stage` up to TWICE, but nets at most ONE
    /// record** (fix round M-4, cross-referenced from `kernel/mod.rs`'s
    /// batched job arm, which bounds `MAX_BATCH` on jobs run rather than
    /// records staged and needs this argument, not the request arm's
    /// poll-count one, to justify it). On an App-level rejection the FIRST
    /// stage (the handler attempt) nets ZERO records — `f`'s `Err` propagates
    /// out of `Group::stage` via `?` before it ever touches `self.records` —
    /// and only the SECOND stage (the failure-record write below) nets one.
    /// On success there is only the one stage. Either way `stage`'s own
    /// assert (`records.len() < MAX_BATCH`, checked BEFORE `f` runs) sees at
    /// most `MAX_BATCH - 1` when a job's second stage runs, so a batch of
    /// `MAX_BATCH` jobs can never push `group.len()` past `MAX_BATCH`.
    pub fn run_due_into(&mut self, group: &mut Group, now: u64) -> io::Result<JobOutcome> {
        let &(due_at, id) = self.due.first().expect("run_due called with an empty heap");
        assert!(
            due_at <= now,
            "dispatcher bug: run_due at {now} but the earliest job is due at {due_at}"
        );
        self.due.remove(&(due_at, id));

        let defs = &self.defs;
        // Minted before the group is borrowed mutably, exactly as the
        // request path mints it — see `CrossTenant`.
        let cap = group.cross_tenant_cap();
        let attempt = group.stage(|tx| {
            let mut row = tx
                .get(Id::<JobRow>::from_raw(id))?
                .unwrap_or_else(|| panic!("dispatcher invariant: due job {id} has no row"));
            assert_eq!(
                row.status,
                JobStatus::Pending,
                "dispatcher invariant: job {id} in the heap is not Pending"
            );
            let Some(def) = defs.get(row.name.as_str()) else {
                return Err(JobError::App(format!(
                    "job \"{}\" is not registered — add it to the jobs![…] list passed to \
                     App::mount_jobs (or this log outlived a renamed job type)",
                    row.name
                )));
            };
            // Dispatch runs only while writable (the `Ready::Job` push is
            // gated on it), and every way to become writable passes a
            // refusal over a Pending row of a foreign shape first:
            // `Kernel::build` on a node writable at start, `after_repl_step`
            // at every write-gate opening. While writable, new rows come only
            // from this binary's own enqueues, and `Group::stage` refuses one
            // under another shape of a mounted name; shipped frames apply only
            // on a follower. So a foreign-shape payload never reaches decode
            // — asserted, not handled (hard rule 4).
            assert_eq!(
                row.shape,
                Some(def.fingerprint),
                "dispatcher invariant: job {id} (\"{}\") has recorded shape {} but this binary \
                 compiled {:#x} — a foreign-shape payload never reaches decode",
                row.name,
                row.shape
                    .map_or_else(|| "none".to_string(), |shape| format!("{shape:#x}")),
                def.fingerprint
            );
            match catch_unwind(AssertUnwindSafe(|| {
                (def.glue)(tx, &row.payload, def.cross_tenant.then_some(&cap))
            })) {
                Ok(result) => result?,
                Err(payload) => {
                    // A panic is the handler's own deterministic bug: an
                    // attempt (poison-pill absorption), never a platform fault.
                    return Err(JobError::App(format!(
                        "handler panicked: {}",
                        panic_text(&*payload)
                    )));
                }
            }
            row.status = JobStatus::Done;
            row.last_error = None;
            tx.put(&row);
            Ok(())
        });

        // `Group::stage`'s only `TxError::Io` exit is its enqueue refusal: this
        // handler's transaction enqueued another shape of a mounted job name.
        // That is the handler's own deterministic bug, like a panic — an
        // attempt, never a platform fault, since no restart could clear it.
        // Asserted, not handled (hard rule 4): a second `Io` exit in `stage`
        // must be classified here on purpose, not dead-lettered by default.
        let attempt = match attempt {
            Err(TxError::Io(e)) => {
                assert!(
                    JobShapeRefusal::of(&e).is_some(),
                    "dispatcher invariant: Group::stage returned an io error that is not its \
                     job-shape refusal — classify it in dispatch before it can reach a job: {e}"
                );
                Err(TxError::App(JobError::App(e.to_string())))
            }
            other => other,
        };
        match attempt {
            Ok(()) => Ok(JobOutcome::Done { id }),
            // Platform faults — the transaction machinery's own, or one the
            // handler's reads surfaced — are NOT attempts: the row is
            // untouched and Pending, the heap entry comes back, no ledger
            // write, and the error bubbles (restart-and-recover).
            Err(TxError::Io(e)) | Err(TxError::App(JobError::Io(e))) => {
                self.due.insert((due_at, id)); // still Pending in the store
                Err(e)
            }
            Err(TxError::NotPrimary { .. }) => {
                // The T8 kernel gate makes this unreachable — job dispatch
                // never runs on a follower. If it ever fires, that gate
                // broke: fail loudly, don't silently retry.
                self.due.insert((due_at, id));
                Err(io::Error::other(
                    "BUG: a job/cron transaction hit NotPrimary — the kernel must not run \
                     dispatch or cron on a follower (DESIGN §5)",
                ))
            }
            Err(TxError::Verifying { .. }) => {
                // Same shape as the NotPrimary arm above: unreachable because
                // the Ready::Job push is gated on writes_allowed(), which a
                // verifying node fails.
                self.due.insert((due_at, id));
                Err(io::Error::other(
                    "BUG: a job/cron transaction hit Verifying — the kernel must not run \
                     dispatch or cron on a node whose witness gate is still shut (M2 \
                     sub-project 0b). The guard that makes this unreachable is \
                     writes_allowed(): Kernel::build's cron.upsert, the \
                     Ready::Job/Ready::Cron pushes, and Kernel::run's migrate_schema all \
                     check it, and after_repl_step re-runs the skipped work when the gate \
                     opens. Reaching here means one of those checks was weakened back to a \
                     role test — a verifying node is a Role::Primary, so is_primary() does \
                     not cover it",
                ))
            }
            Err(TxError::App(JobError::App(err))) => {
                // Every fallible step below (the policy read, the
                // failure-record stage) is run through this closure so that
                // ANY Err exit — not just the sibling Io arm's — still
                // restores the heap entry: run_due_into's contract is that a
                // Pending job is always in the heap on return.
                //
                // The retry-policy lookup is folded into the SAME stage as
                // the failure record (group-commit T7, C-2), rather than a
                // separate `db.snapshot()` re-read the way this used to
                // work: `Group` deliberately exposes no snapshot accessor
                // (spec §1 has no group-snapshot escape hatch), and reading
                // `row.name` through `tx` here is strictly MORE correct
                // anyway — `tx` sees this batch's own earlier writes through
                // the group overlay, where a bare snapshot would not. This
                // also deletes one of the two `expect("row existed a moment
                // ago")` sites the old shape needed.
                let defs = &self.defs;
                let staged = group.stage(|tx| {
                    let mut row = tx
                        .get(Id::<JobRow>::from_raw(id))?
                        .expect("row existed a moment ago");
                    let retry = defs
                        .get(row.name.as_str())
                        .map_or(RetryPolicy::DEFAULT, |d| d.retry);
                    row.attempts += 1;
                    row.last_error = Some(err.clone());
                    let outcome = if row.attempts >= retry.max {
                        row.status = JobStatus::Dead;
                        JobOutcome::Dead {
                            id,
                            attempts: row.attempts,
                        }
                    } else {
                        row.due_at = now.saturating_add(retry.backoff_nanos(row.attempts));
                        JobOutcome::Retried {
                            id,
                            attempts: row.attempts,
                            due_at: row.due_at,
                        }
                    };
                    tx.put(&row);
                    Ok::<_, io::Error>(outcome)
                });
                match staged.map_err(|e| match e {
                    TxError::Io(e) | TxError::App(e) => e,
                    TxError::NotPrimary { .. } => io::Error::other(
                        "BUG: a job/cron transaction hit NotPrimary — the kernel must not \
                         run dispatch or cron on a follower (DESIGN §5)",
                    ),
                    TxError::Verifying { .. } => io::Error::other(
                        "BUG: a job/cron transaction hit Verifying — the kernel must not \
                         run dispatch or cron on a node whose witness gate is still shut \
                         (M2 sub-project 0b). The guard that makes this unreachable is \
                         writes_allowed(): Kernel::build's cron.upsert, the \
                         Ready::Job/Ready::Cron pushes, and Kernel::run's migrate_schema \
                         all check it, and after_repl_step re-runs the skipped work when \
                         the gate opens. Reaching here means one of those checks was \
                         weakened back to a role test — a verifying node is a \
                         Role::Primary, so is_primary() does not cover it",
                    ),
                }) {
                    Ok(outcome) => {
                        if let JobOutcome::Retried { due_at, .. } = outcome {
                            self.due.insert((due_at, id));
                        }
                        Ok(outcome)
                    }
                    Err(e) => {
                        self.due.insert((due_at, id));
                        Err(e)
                    }
                }
            }
        }
    }

    /// The one-job wrapper (group-commit T7, C-2; fix round I-2):
    /// `begin_group`, one `run_due_into`, then `commit` — the shape every
    /// existing direct caller of `run_due` (ten call sites across
    /// `tests/kernel.rs`) keeps compiling against unchanged. The kernel's
    /// own batched job arm calls `run_due_into` directly against a group it
    /// drains many jobs into instead.
    ///
    /// **On a commit failure, this now applies `Db::transact`'s ruled policy
    /// exactly** (`CommitFailure::invalidates`, db.rs:1438-1445/2256-2299 —
    /// the ONE rule, so the single-transaction path, this wrapper and
    /// `CronScheduler::tick`'s wrapper cannot answer the same question
    /// differently). A fresh group used by no one else means
    /// `records_before` is always `0`, so `invalidates(0, staged)` is the
    /// whole question. Under the checkpoint arm (`applied == n`, already
    /// reported through `poisoned()`/`store_failed()`) the job's outcome IS
    /// durable: this returns `Ok(outcome)` and leaves the heap alone —
    /// re-inserting `(due_at, id)` would put the heap at Pending while the
    /// store says `Done`, exactly the state `run_due_into`'s own invariant
    /// assert exists to catch. On every OTHER commit failure (an
    /// append/sync failure, or a checkpoint failure nothing else reports),
    /// nothing this call staged is trustworthy: the heap bookkeeping
    /// `run_due_into` already did on the assumption commit would succeed is
    /// undone — a `Retried` outcome's speculative NEW `due_at` entry is
    /// removed first — and the heap is restored to the ORIGINAL `(due_at,
    /// id)`, mirroring the `Io` arm above, one level up.
    pub fn run_due(&mut self, db: &mut Db, now: u64) -> io::Result<JobOutcome> {
        let &(due_at, id) = self.due.first().expect("run_due called with an empty heap");
        let mut group = db.begin_group()?;
        let result = self.run_due_into(&mut group, now);
        // Sampled BEFORE commit (which `mem::take`s the group's records):
        // `run_due_into`'s Ok arm always stages exactly one record (the
        // Done/Retried/Dead write), so `staged` is true whenever `result` is
        // `Ok` — but read from the group itself rather than from `result`,
        // so this stays correct if that ever changes.
        let staged = !group.is_empty();
        // Nothing is durable until commit runs — and it runs regardless of
        // `result` (C-1's commit-then-bubble rule): `Group`'s only
        // non-panicking exit is `commit()`, and by the time `run_due_into`
        // has returned, `group` holds at most the one record it staged for
        // THIS job (a fresh group, used by no one else).
        match (result, group.commit()) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Ok(outcome), Err(failure)) => {
                let reported_elsewhere = db.poisoned() || db.store_failed();
                if failure.invalidates(0, staged) || !reported_elsewhere {
                    if let JobOutcome::Retried {
                        due_at: new_due_at, ..
                    } = outcome
                    {
                        self.due.remove(&(new_due_at, id));
                    }
                    self.due.insert((due_at, id));
                    Err(failure.error)
                } else {
                    Ok(outcome)
                }
            }
            (Err(e), Ok(())) => Err(e),
            // The commit's own failure is the more informative fact: it says
            // whether anything reached the store, and `run_due_into`'s own
            // Err arms already restore (due_at, id) — nothing further to
            // undo on this path.
            (Err(_), Err(failure)) => Err(failure.error),
        }
    }
}

fn panic_text(payload: &(dyn std::any::Any + Send)) -> &str {
    // &*payload, not &payload: a &Box would itself coerce into the trait
    // object and every downcast would miss (the swarm-runner lesson).
    if let Some(s) = payload.downcast_ref::<&str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s
    } else {
        "<non-string panic payload>"
    }
}
