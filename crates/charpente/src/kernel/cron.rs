//! Cron (DESIGN §6): schedules are rows, ticks are transactions, and a tick
//! is just a recurring enqueue — jobs inherit retry/status/observability.
//! Catch-up fires once (next_due jumps past now); backfill is a non-goal.

use std::io;
use std::time::Duration;

use charpente_core::db::{Db, Tx, TxError};
use charpente_core::table::{Id, View};
use charpente_macros::table;

#[table]
pub struct CronRow {
    pub id: Id<CronRow>,
    #[index]
    pub name: String,
    pub interval_nanos: u64,
    pub next_due: u64,
}

/// Named (like `kernel::RecordedResponses`/`SinkHandles`) so the closure type
/// doesn't trip clippy's type-complexity lint at every use site. Widened
/// (auth T6) to carry the tick's own `now`: the platform's built-in
/// session-expiry sweep enqueues a `#[job]` that needs a clock reading, and a
/// job handler runs as a transaction with no clock of its own (DESIGN §1) —
/// the cron tick is the only place `now` is available, so it rides along
/// into the enqueue closure instead.
pub type CronEnqueue = Box<dyn Fn(&mut Tx, u64) + Send>;

pub struct CronEntry {
    pub name: &'static str,
    pub interval: Duration,
    pub enqueue: CronEnqueue,
}

pub struct CronScheduler {
    /// (entry, cached next_due) — the cache mirrors the row; the store is
    /// the truth and upsert/tick keep them identical.
    entries: Vec<(CronEntry, u64)>,
}

impl CronScheduler {
    pub fn new(entries: Vec<CronEntry>) -> CronScheduler {
        CronScheduler {
            entries: entries.into_iter().map(|e| (e, 0)).collect(),
        }
    }

    pub fn upsert(&mut self, db: &mut Db, now: u64) -> io::Result<()> {
        for (entry, cached) in &mut self.entries {
            let interval = nanos(entry.interval, entry.name);
            let existing = {
                let snap = db.snapshot()?;
                CronRow::by_name(&snap, &entry.name.to_string())?.pop()
            };
            let row = match existing {
                Some(mut row) => {
                    row.interval_nanos = interval;
                    row
                }
                None => {
                    let snap = db.snapshot()?;
                    let next_id = snap.scan::<CronRow>()?.last().map_or(0, |r| r.id.raw() + 1);
                    CronRow {
                        id: Id::from_raw(next_id),
                        name: entry.name.to_string(),
                        interval_nanos: interval,
                        next_due: now + interval,
                    }
                }
            };
            // Cache updates only after the commit succeeds (matching `tick`):
            // a failed write must not leave the cache pointing at a row that
            // was never durably written.
            db.transact(|tx| {
                tx.put(&row);
                Ok::<(), io::Error>(())
            })
            .map_err(flatten)?;
            *cached = row.next_due;
        }
        Ok(())
    }

    pub fn next_deadline(&self) -> Option<u64> {
        self.entries.iter().map(|&(_, due)| due).min()
    }

    pub fn has_due(&self, now: u64) -> bool {
        self.next_deadline().is_some_and(|d| d <= now)
    }

    pub fn tick(&mut self, db: &mut Db, now: u64) -> io::Result<(&'static str, bool)> {
        let (entry, cached) = self
            .entries
            .iter_mut()
            .filter(|(_, due)| *due <= now)
            .min_by_key(|(_, due)| *due)
            .expect("tick called with nothing due");
        let interval = nanos(entry.interval, entry.name);
        let catchup = now >= (*cached).saturating_add(interval); // a whole interval missed
        let next = now + interval; // catch-up fires once: jump past now
        let name = entry.name;
        let enqueue = &entry.enqueue;
        db.transact(|tx| {
            let mut row = CronRow::by_name(tx, &name.to_string())?
                .pop()
                .unwrap_or_else(|| panic!("cron invariant: schedule \"{name}\" has no row"));
            row.next_due = next;
            tx.put(&row);
            enqueue(tx, now);
            Ok::<(), io::Error>(())
        })
        .map_err(flatten)?;
        *cached = next;
        Ok((name, catchup))
    }
}

fn nanos(d: Duration, name: &'static str) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or_else(|_| {
        panic!(
            "cron schedule \"{name}\": interval {d:?} exceeds u64 nanos (~584 years) — \
             pick a smaller interval"
        )
    })
}

fn flatten(e: TxError<io::Error>) -> io::Error {
    match e {
        TxError::App(e) | TxError::Io(e) => e,
        TxError::NotPrimary { .. } => io::Error::other(
            "BUG: a job/cron transaction hit NotPrimary — the kernel must not run dispatch or \
             cron on a follower (DESIGN §5)",
        ),
    }
}
