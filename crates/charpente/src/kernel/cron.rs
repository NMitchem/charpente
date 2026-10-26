//! Cron (DESIGN §6): schedules are rows, ticks are transactions, and a tick
//! is just a recurring enqueue — jobs inherit retry/status/observability.
//! Catch-up fires once (next_due jumps past now); backfill is a non-goal.

use std::io;
use std::time::Duration;

use charpente_core::db::{Db, Group, Tx};
use charpente_core::io::WallTime;
use charpente_core::table::{Id, View};
use charpente_macros::table;

use super::dispatch::flatten;

#[table(global)]
pub struct CronRow {
    pub id: Id<CronRow>,
    #[index]
    pub name: String,
    pub interval_nanos: u64,
    pub next_due: WallTime,
}

/// Named (like `kernel::RecordedResponses`/`SinkHandles`) so the closure type
/// doesn't trip clippy's type-complexity lint at every use site. Widened
/// (auth T6) to carry the tick's own `now`: the platform's built-in
/// session-expiry sweep enqueues a `#[job]` that needs a clock reading, and a
/// job handler runs as a transaction with no clock of its own (DESIGN §1) —
/// the cron tick is the only place `now` is available, so it rides along
/// into the enqueue closure instead.
pub type CronEnqueue = Box<dyn Fn(&mut Tx, WallTime) + Send>;

pub struct CronEntry {
    pub name: &'static str,
    pub interval: Duration,
    pub enqueue: CronEnqueue,
}

pub struct CronScheduler {
    /// (entry, cached next_due) — the cache mirrors the row; the store is
    /// the truth and upsert/tick keep them identical.
    entries: Vec<(CronEntry, WallTime)>,
}

impl CronScheduler {
    pub fn new(entries: Vec<CronEntry>) -> CronScheduler {
        CronScheduler {
            entries: entries
                .into_iter()
                .map(|e| (e, WallTime::UNIX_EPOCH))
                .collect(),
        }
    }

    pub fn upsert(&mut self, db: &mut Db, now: WallTime) -> io::Result<()> {
        for (entry, cached) in &mut self.entries {
            let interval = nanos(entry.interval, entry.name);
            let existing = {
                let snap = db.snapshot()?;
                CronRow::by_name(&snap, &entry.name.to_string())?.pop()
            };
            let row = match existing {
                // The row keeps the interval its `next_due` was set with until
                // the next tick sets both, so `next_due - interval_nanos` stays
                // a time this store saw (the wall floor, kernel/clock.rs) when
                // a deploy shortens the interval. It is still written: every
                // open has always logged this put.
                Some(row) => row,
                None => {
                    let snap = db.snapshot()?;
                    let next_id = snap.scan::<CronRow>()?.last().map_or(0, |r| r.id.raw() + 1);
                    CronRow {
                        id: Id::from_raw(next_id),
                        name: entry.name.to_string(),
                        interval_nanos: interval,
                        next_due: now + entry.interval,
                    }
                }
            };
            // `tick`'s one rule: the cache follows the store, not the answer.
            // The answer is `Db::transact`'s (`Err` unless the failure is
            // reported elsewhere), and the cache takes the row's `next_due`
            // whenever the row reached the store, so a row that is durable
            // never leaves its schedule due at `UNIX_EPOCH`.
            let mut group = db.begin_group()?;
            let result = group
                .stage(|tx| {
                    tx.put(&row);
                    Ok::<(), io::Error>(())
                })
                .map_err(flatten);
            let staged = !group.is_empty();
            match (result, group.commit()) {
                (Ok(()), Ok(())) => *cached = row.next_due,
                (Ok(()), Err(failure)) => {
                    let invalidated = failure.invalidates(0, staged);
                    if !invalidated {
                        *cached = row.next_due;
                    }
                    if invalidated || !(db.poisoned() || db.store_failed()) {
                        return Err(failure.error);
                    }
                }
                (Err(e), Ok(())) => return Err(e),
                (Err(_), Err(failure)) => return Err(failure.error),
            }
        }
        Ok(())
    }

    pub fn next_deadline(&self) -> Option<WallTime> {
        self.entries.iter().map(|&(_, due)| due).min()
    }

    pub fn has_due(&self, now: WallTime) -> bool {
        self.next_deadline().is_some_and(|d| d <= now)
    }

    /// Ticks the earliest due schedule by STAGING it into `group` instead of
    /// committing on its own (group-commit T7, C-2) — the shared half of
    /// `tick`'s one-schedule wrapper and the kernel's batched cron-arm
    /// drain. The in-memory cache (`*cached`) advances the moment staging
    /// succeeds, on the assumption the eventual commit will too — required
    /// so a batched drain never re-ticks the SAME entry twice in one loop
    /// (`has_due` reads the cache).
    ///
    /// **Who undoes a wrong advance, and who doesn't:** `tick`'s one-schedule
    /// wrapper undoes it on a commit failure that invalidates this tick
    /// (`CommitFailure::invalidates`) — see its doc. The kernel's own
    /// batched cron arm (`kernel/mod.rs`'s `Ready::Cron`) never undoes it,
    /// on ANY commit failure, because that arm stops the kernel outright
    /// either way (see the comment at that call site for why): a stale
    /// cache does not survive to be read again before a restart rebuilds it
    /// from the store.
    pub fn tick_into(
        &mut self,
        group: &mut Group,
        now: WallTime,
    ) -> io::Result<(&'static str, bool)> {
        let (entry, cached) = self
            .entries
            .iter_mut()
            .filter(|(_, due)| *due <= now)
            .min_by_key(|(_, due)| *due)
            .expect("tick called with nothing due");
        // A whole interval missed.
        let catchup = now.saturating_duration_since(*cached) >= entry.interval;
        let interval = nanos(entry.interval, entry.name);
        // Catch-up fires once, then the next slot on the grid, never now + interval.
        let next = next_slot(*cached, now, interval, entry.name);
        let name = entry.name;
        let enqueue = &entry.enqueue;
        group
            .stage(|tx| {
                let mut row = CronRow::by_name(tx, &name.to_string())?
                    .pop()
                    .unwrap_or_else(|| panic!("cron invariant: schedule \"{name}\" has no row"));
                row.next_due = next;
                row.interval_nanos = interval;
                tx.put(&row);
                enqueue(tx, now);
                Ok::<(), io::Error>(())
            })
            .map_err(flatten)?;
        *cached = next;
        Ok((name, catchup))
    }

    /// The one-schedule wrapper (group-commit T7, C-2):
    /// `begin_group` + one `tick_into` + `commit`. Its callers are all tests
    /// (`tests/kernel.rs`, and `kernel/clock.rs`'s floor test). The kernel's
    /// own batched cron arm calls `tick_into` directly against a group it
    /// drains several ticks into instead.
    ///
    /// **On a commit failure, this answers exactly as `Db::transact`'s ruled
    /// policy does** (`CommitFailure::invalidates`, the ONE rule, so the
    /// single-transaction path, `Dispatcher::run_due`'s wrapper and this one
    /// cannot answer the same question differently — see `run_due`'s doc). A
    /// fresh group used by no one else means `records_before` is always `0`,
    /// so `invalidates(0, staged)` says whether the tick's record reached the
    /// store. It answers `Err` when the record did not reach it, and also when
    /// it did but nothing else reports the failure (a horizon-breadcrumb sync,
    /// which poisons nothing and latches nothing), so the failure is never
    /// swallowed. Under the checkpoint arm already reported through
    /// `poisoned()`/`store_failed()` it answers `Ok((name, catchup))`.
    ///
    /// **The cache follows the store, not the answer** — the same rule
    /// `run_due`'s heap and `upsert`'s cache follow. `tick_into` advanced the
    /// ticked entry's cache on the assumption the commit would succeed. Only
    /// a record that never reached the store (`applied == 0`, an append, sync
    /// or apply failure) rolls it back to `previous`, so the same interval
    /// ticks again. When the record did reach it (`applied == n`, either
    /// checkpoint arm) the advance and its enqueue are durable and the cache
    /// stays where `tick_into` put it: rolled back, it would re-tick and
    /// re-enqueue the SAME interval the next time `has_due` is checked,
    /// against a store that already holds both — and the unreported arm
    /// leaves the `Db` writable, so that second enqueue would commit
    /// (`CommitFailure`: records `0..applied` must not be retried).
    /// `(name, previous)` is peeked with the SAME selection `tick_into` makes
    /// (deterministic over the same `self.entries`/`now`, and nothing mutates
    /// `self.entries` in between), so it names the entry `tick_into` actually
    /// picked without `tick_into` itself having to hand it back.
    pub fn tick(&mut self, db: &mut Db, now: WallTime) -> io::Result<(&'static str, bool)> {
        let (name, previous) = self
            .entries
            .iter()
            .filter(|(_, due)| *due <= now)
            .min_by_key(|(_, due)| *due)
            .map(|(entry, due)| (entry.name, *due))
            .expect("tick called with nothing due");
        let mut group = db.begin_group()?;
        let result = self.tick_into(&mut group, now);
        // Sampled BEFORE commit (which `mem::take`s the group's records) —
        // see `Dispatcher::run_due`'s identical comment.
        let staged = !group.is_empty();
        match (result, group.commit()) {
            (Ok(out), Ok(())) => Ok(out),
            (Ok(out), Err(failure)) => {
                let reported_elsewhere = db.poisoned() || db.store_failed();
                let invalidated = failure.invalidates(0, staged);
                if invalidated {
                    // Entry names are unique by construction — `App::schedule`
                    // asserts it at mount time (fix round M-5, hard rule 4:
                    // assert the negative space instead of silently doing
                    // nothing on an unreachable miss) — so the entry
                    // `tick_into` just picked by this exact same selection
                    // must still be here.
                    let (_, cached) = self
                        .entries
                        .iter_mut()
                        .find(|(e, _)| e.name == name)
                        .unwrap_or_else(|| {
                            panic!(
                                "cron invariant: schedule \"{name}\" vanished between \
                                 tick_into and its wrapper's restore — entry names are unique \
                                 and App::schedule asserts it at mount time, so this entry \
                                 must still be at the same index tick_into just selected it \
                                 from"
                            )
                        });
                    *cached = previous;
                }
                if invalidated || !reported_elsewhere {
                    Err(failure.error)
                } else {
                    Ok(out)
                }
            }
            (Err(e), Ok(())) => Err(e),
            (Err(_), Err(failure)) => Err(failure.error),
        }
    }
}

/// `interval` must be the value the same put writes to `interval_nanos`: the
/// open-time floor is `next_due - interval_nanos`, which must stay <= `now`.
fn next_slot(next_due: WallTime, now: WallTime, interval: u64, name: &'static str) -> WallTime {
    let (due, now) = (next_due.as_nanos(), now.as_nanos());
    assert!(
        interval > 0 && now >= due,
        "cron schedule \"{name}\": tick before due"
    );
    let next = (now - due)
        .div_euclid(interval)
        .checked_add(1)
        .and_then(|k| k.checked_mul(interval))
        .and_then(|d| d.checked_add(due))
        .unwrap_or_else(|| {
            panic!("cron schedule \"{name}\": next slot after {due}ns overflows u64 nanos")
        });
    assert!(
        next > now && next - interval <= now,
        "cron schedule \"{name}\": slot off grid"
    );
    WallTime::from_nanos(next)
}

fn nanos(d: Duration, name: &'static str) -> u64 {
    assert!(
        !d.is_zero(),
        "cron schedule \"{name}\": interval is zero — a schedule needs an interval of at least 1ns; pick the period it should run at"
    );
    u64::try_from(d.as_nanos()).unwrap_or_else(|_| {
        panic!(
            "cron schedule \"{name}\": interval {d:?} exceeds u64 nanos (~584 years) — \
             pick a smaller interval"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(
        expected = "cron schedule \"z\": interval is zero — a schedule needs an interval of at least 1ns; pick the period it should run at"
    )]
    fn a_zero_interval_is_refused_by_name() {
        nanos(Duration::ZERO, "z");
    }

    #[test]
    fn next_slot_stays_on_grid_and_floor_holds_after_an_interval_change() {
        const S: u64 = 1_000_000_000;
        let w = |s: u64| WallTime::from_nanos(s * S);
        assert_eq!(next_slot(w(100), w(125), 10 * S, "t"), w(130));
        assert_eq!(next_slot(w(100), w(100), 10 * S, "t"), w(110));
        assert_eq!(next_slot(w(100), w(1000), 10 * S, "t"), w(1010));
        // Row stored 10s, entry now 2s: the put writes 2s, so the floor
        // next - 2s is <= the tick's now.
        let next = next_slot(w(100), w(125), 2 * S, "t");
        assert_eq!(next, w(126));
        assert!(next.as_nanos() - 2 * S <= w(125).as_nanos());
    }

    use crate::{DbConfig, Schema};
    use charpente_core::io::{Disk, File};
    use charpente_sim::{FaultConfig, Sim};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    /// Counts WAL syncs only (mirrors `tests/group_commit.rs`'s
    /// `CountingDisk`/`OpCounts`, which this crate cannot import — that file
    /// is a separate `tests/` binary — so this is a local, minimal rebuild
    /// for this one assertion rather than a shared import).
    struct SyncCountingDisk {
        inner: charpente_sim::SimDisk,
        syncs: Arc<AtomicU64>,
    }

    impl Disk for SyncCountingDisk {
        fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
            let file = self.inner.open(path)?;
            Ok(Box::new(SyncCountingFile {
                inner: file,
                syncs: self.syncs.clone(),
            }))
        }
        fn remove(&self, path: &str) -> io::Result<()> {
            self.inner.remove(path)
        }
        fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
            self.inner.list(prefix)
        }
        fn sync_dir(&self, prefix: &str) -> io::Result<()> {
            self.inner.sync_dir(prefix)
        }
    }

    struct SyncCountingFile {
        inner: Box<dyn File>,
        syncs: Arc<AtomicU64>,
    }

    impl File for SyncCountingFile {
        fn len(&self) -> io::Result<u64> {
            self.inner.len()
        }
        fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
            self.inner.read_at(offset, out)
        }
        fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            self.inner.write_at(offset, data)
        }
        fn set_len(&self, len: u64) -> io::Result<()> {
            self.inner.set_len(len)
        }
        fn sync(&self) -> io::Result<()> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            self.inner.sync()
        }
    }

    /// Fix round I-3 (ruled, Noah 2026-08-13): nothing in the repo drove
    /// `tick_into`'s SECOND drain-loop iteration before this, and the cron
    /// arm is the only place a batch performs an `#[index]`-backed read
    /// through the group overlay across siblings (`CronRow::by_name` ->
    /// `Tx::scan_by`, `db.rs:606-649`). Two entries with DIFFERENT intervals
    /// (so assertion (b) below can discriminate: index cross-talk — one
    /// schedule's tick adopting the OTHER schedule's row through the group
    /// overlay — would leave a row's `next_due` advanced by the WRONG
    /// entry's interval, or the same entry ticked twice) are both due at
    /// construction (`CronScheduler::new`'s cache starts every entry at 0),
    /// so a single `Group` draining both via `tick_into` twice, committed
    /// once, is exactly the untested shape.
    ///
    /// The reviewer traced `Tx::scan_by`'s overlay layering as structurally
    /// correct (group-commit T7 review, CLEARED). This test's own report
    /// records the mutation that proves it: perturbing the group-overlay
    /// key check in `Tx::scan_by` turns this red for exactly the reason
    /// named above.
    #[test]
    fn a_group_drains_two_due_schedules_with_one_sync_and_no_index_cross_talk() {
        let sim = Sim::with_faults(0x60C0_0011, FaultConfig::none());
        let syncs = Arc::new(AtomicU64::new(0));
        let disk = SyncCountingDisk {
            inner: sim.disk(),
            syncs: syncs.clone(),
        };
        let mut db = Db::open(
            Box::new(disk),
            DbConfig::default(),
            Schema::new().table::<CronRow>(),
        )
        .expect("Db::open on a fresh sim disk");

        let now = WallTime::from_nanos(1_000_000_000);
        let alpha_interval = Duration::from_secs(5);
        let beta_interval = Duration::from_secs(9);
        let alpha_enqueued = Arc::new(AtomicBool::new(false));
        let beta_enqueued = Arc::new(AtomicBool::new(false));
        let (a_flag, b_flag) = (alpha_enqueued.clone(), beta_enqueued.clone());
        let mut scheduler = CronScheduler::new(vec![
            CronEntry {
                name: "alpha",
                interval: alpha_interval,
                enqueue: Box::new(move |_tx, _now| {
                    a_flag.store(true, Ordering::SeqCst);
                }),
            },
            CronEntry {
                name: "beta",
                interval: beta_interval,
                enqueue: Box::new(move |_tx, _now| {
                    b_flag.store(true, Ordering::SeqCst);
                }),
            },
        ]);
        // Pre-create both rows directly, NOT through `upsert` (which would
        // give them DIFFERENT next_dues from one shared `now`, since the
        // intervals differ). "Both due at once" is already guaranteed by
        // `CronScheduler::new`'s cache (0 for every entry) — the rows' own
        // `next_due` fields don't have to agree for that, but are set equal
        // anyway for a coherent starting point.
        //
        // IDs deliberately INVERT tick order (alpha ticks FIRST via
        // `min_by_key`'s tie-break-picks-first rule, since both start cached
        // at 0 and alpha is entry 0, but alpha's ROW ID is the HIGHER one):
        // `Tx::scan_by`'s merge (`db.rs:606-649`) is a `BTreeMap<id, R>`, and
        // `CronRow::by_name(..).pop()` takes the LAST (highest-id) match. An
        // index-cross-talk bug — the group overlay's key check dropped, so
        // EVERY staged row on this table leaks into every by-name query —
        // would then have alpha's already-staged row (id 1) sort AFTER
        // beta's own snapshot row (id 0) in the second tick's merge, and
        // `.pop()` would silently hand `tick_into` alpha's row while it
        // means to touch beta's. With ids the other way around, `.pop()`
        // would happen to pick the correct row anyway and this test would
        // not discriminate the bug at all (found by mutation while proving
        // this test's teeth — see the fix report).
        db.transact(|tx| {
            tx.put(&CronRow {
                id: Id::from_raw(1),
                name: "alpha".to_string(),
                interval_nanos: alpha_interval.as_nanos() as u64,
                next_due: now,
            });
            tx.put(&CronRow {
                id: Id::from_raw(0),
                name: "beta".to_string(),
                interval_nanos: beta_interval.as_nanos() as u64,
                next_due: now,
            });
            Ok::<(), io::Error>(())
        })
        .expect("seed both cron rows");

        let before = syncs.load(Ordering::SeqCst);
        let mut group = db.begin_group().expect("group");
        assert!(scheduler.has_due(now), "both entries start due (cache 0)");
        let (first, _) = scheduler.tick_into(&mut group, now).expect("first tick");
        assert!(
            scheduler.has_due(now),
            "the SECOND entry must still be due after the first tick_into — this is the \
             untested second-iteration drain"
        );
        let (second, _) = scheduler.tick_into(&mut group, now).expect("second tick");
        assert_ne!(
            first, second,
            "two different schedules must have ticked, not the same one twice"
        );
        assert!(
            !scheduler.has_due(now),
            "both entries ticked past now, so neither is due any more"
        );
        group.commit().expect("commit");

        // (a) one sync for the whole batch.
        assert_eq!(
            syncs.load(Ordering::SeqCst) - before,
            1,
            "two staged ticks must share ONE fsync"
        );
        // (b) each row's next_due is its OWN interval past now — the
        // discriminating conjunct (index cross-talk would leave a row
        // carrying the WRONG entry's interval, or the same row ticked
        // twice).
        let snap = db.snapshot().unwrap();
        let alpha = CronRow::by_name(&snap, &"alpha".to_string())
            .unwrap()
            .pop()
            .unwrap();
        let beta = CronRow::by_name(&snap, &"beta".to_string())
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            alpha.next_due,
            WallTime::from_nanos(5_000_000_000),
            "alpha's row must advance to its OWN next grid slot (cache 0, 5s grid), not beta's"
        );
        assert_eq!(
            beta.next_due,
            WallTime::from_nanos(9_000_000_000),
            "beta's row must advance to its OWN next grid slot (cache 0, 9s grid), not alpha's"
        );
        // (c) both enqueues landed.
        assert!(
            alpha_enqueued.load(Ordering::SeqCst),
            "alpha's enqueue closure ran"
        );
        assert!(
            beta_enqueued.load(Ordering::SeqCst),
            "beta's enqueue closure ran"
        );
    }

    /// A 0.2 data directory meets the stale-directory refusal at `CronRow`, the
    /// platform table every directory records (the session-sweep cron). The stored
    /// fingerprint is the real 0.2 one, so this fails if the arm that treats the
    /// un-prefixed `CronRow` as a platform table goes.
    #[test]
    fn a_0_2_data_directory_is_refused_at_cron_row_by_name() {
        use charpente_core::table::{Codec, IndexDef, Row, Unscoped};

        struct CronRow02 {
            id: Id<CronRow02>,
        }
        impl Row for CronRow02 {
            type Scope = Unscoped;
            const TABLE: &'static str = "CronRow";
            const INDEXES: &'static [IndexDef<CronRow02>] = &[];
            const VERSION: u32 = 1;
            const FINGERPRINT: Option<u64> = Some(0xbed2ea12c6f56d9a);
            fn id(&self) -> Id<CronRow02> {
                self.id
            }
            fn encode_row(&self, out: &mut Vec<u8>) {
                self.id.encode(out);
            }
            fn decode_row(bytes: &[u8]) -> std::io::Result<CronRow02> {
                Ok(CronRow02 {
                    id: Id::decode(&mut &bytes[..])?,
                })
            }
        }
        let sim = Sim::with_faults(0x2EB1_0007, FaultConfig::none());
        let cfg = || DbConfig {
            store_path: "t.redb".into(),
            ..DbConfig::default()
        };
        let mut db = Db::open(
            Box::new(sim.disk()),
            cfg(),
            Schema::new().table::<CronRow02>(),
        )
        .unwrap();
        db.transact(|tx| -> Result<(), ()> {
            tx.put(&CronRow02 {
                id: Id::from_raw(1),
            });
            Ok(())
        })
        .unwrap();
        db.checkpoint().unwrap();
        drop(db);
        let err = Db::open(
            Box::new(sim.disk()),
            cfg(),
            Schema::new().table::<CronRow>(),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "this data directory was written by another charpente release: the platform table \
                 \"CronRow\" has a different shape (0xbed2ea12c6f56d9a stored, {:#x} compiled). \
                 charpente does not migrate its own formats between releases (DECISIONS §15). Start \
                 this node from an empty data directory, or restore an archive written by this \
                 release.",
                CronRow::FINGERPRINT.unwrap()
            )
        );
    }
}
