//! Job dispatch types. The Dispatcher itself lands with the due-heap task.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};

use charpente_core::db::{Db, Tx, TxError};
use charpente_core::job::{JobRow, JobStatus, RetryPolicy};
use charpente_core::table::{Id, Snapshot, View};

/// Why a job run did not complete. The App/Io split mirrors `TxError` and is
/// load-bearing: `App` is the handler's own rejection (or an absorbed panic,
/// or a corrupt payload) — a real attempt that feeds retry budget and lands
/// in the row's `last_error`. `Io` is a platform fault surfaced through the
/// handler's reads — NOT an attempt: the job stays Pending, nothing is
/// recorded, the error bubbles (restart-and-recover). Folding Io into the
/// retry path would let injected faults Dead-letter healthy jobs.
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
    pub glue: fn(&mut Tx, &[u8]) -> Result<(), JobError>,
}

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

    /// Runs the earliest due job. `Ok(outcome)` covers handler success and
    /// handler failure (retry/dead — both are committed records); `Err` is a
    /// PLATFORM failure (`TxError::Io`, or an `Io` the handler's own reads
    /// surfaced): the job stays Pending in the store, the heap entry is
    /// restored, and the caller decides (restart).
    pub fn run_due(&mut self, db: &mut Db, now: u64) -> io::Result<JobOutcome> {
        let &(due_at, id) = self.due.first().expect("run_due called with an empty heap");
        assert!(
            due_at <= now,
            "dispatcher bug: run_due at {now} but the earliest job is due at {due_at}"
        );
        self.due.remove(&(due_at, id));

        let defs = &self.defs;
        let attempt = db.transact(|tx| {
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
            match catch_unwind(AssertUnwindSafe(|| (def.glue)(tx, &row.payload))) {
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
                // Every fallible step below (the policy re-read, the
                // failure-record transact) is run through this closure so
                // that ANY Err exit — not just the sibling Io arm's — still
                // restores the heap entry: run_due's contract is that a
                // Pending job is always in the heap on return.
                let mut record = || -> io::Result<JobOutcome> {
                    let retry = self
                        .defs
                        .get(
                            db.snapshot()?
                                .get(Id::<JobRow>::from_raw(id))?
                                .expect("row existed a moment ago")
                                .name
                                .as_str(),
                        )
                        .map_or(RetryPolicy::DEFAULT, |d| d.retry);
                    db.transact(|tx| {
                        let mut row = tx
                            .get(Id::<JobRow>::from_raw(id))?
                            .expect("row existed a moment ago");
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
                    })
                    .map_err(|e| match e {
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
                    })
                };
                match record() {
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
