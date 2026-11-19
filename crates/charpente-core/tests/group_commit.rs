//! `Db::begin_group`/`Group::stage`/`Group::commit` (group-commit T4, spec
//! §1–§2): the two-level overlay a staged transaction reads through (its own
//! writes, then the group's accumulated writes, then the store snapshot),
//! and the isolation that makes a rejected or panicking handler structurally
//! unable to corrupt the batch.
//!
//! An integration test, not `db.rs`'s `mod tests`: that module is pure
//! `TxEffects` encode/decode and cannot build a `Db`, since building one
//! needs a `Disk` and the only impl is `charpente_sim`'s — a `--lib` test
//! target is a distinct compilation unit from the one `charpente_sim` links
//! against, and the trait impls do not unify (`tests/engine.rs`'s header
//! documents this same constraint). `begin_group`/`stage`/`commit` are
//! `pub`, so they are reachable from here like every other `Db`-constructing
//! test in this crate.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use charpente_core::db::{Db, DbConfig, Job, Schema, TxError};
use charpente_core::io::{Disk, Entropy, File, WallTime};
use charpente_core::job::{DeadRecord, JobRow, JobStatus};
use charpente_core::table::{Codec, Id, IndexDef, Row, Unscoped, View};
use charpente_sim::{FaultConfig, Sim};

#[derive(Debug, PartialEq)]
struct Widget {
    id: Id<Widget>,
    count: u32,
}

impl Row for Widget {
    type Scope = Unscoped;
    const TABLE: &'static str = "Widget";
    const INDEXES: &'static [IndexDef<Widget>] = &[];

    fn id(&self) -> Id<Widget> {
        self.id
    }

    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
    }

    fn decode_row(bytes: &[u8]) -> io::Result<Widget> {
        let input = &mut &bytes[..];
        Ok(Widget {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
        })
    }
}

fn cfg() -> DbConfig {
    DbConfig {
        store_path: "t.redb".into(),
        ..DbConfig::default()
    }
}

fn fresh_db(seed: u64) -> Db {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    Db::open(Box::new(sim.disk()), cfg(), Schema::new().table::<Widget>()).unwrap()
}

/// The property the whole two-level overlay exists for.
#[test]
fn a_staged_transaction_sees_an_earlier_one_in_the_same_group() {
    let mut db = fresh_db(0x60C0_0005);
    let mut g = db.begin_group().expect("group");
    g.stage(|tx| {
        tx.put(&Widget {
            id: Id::from_raw(1),
            count: 7,
        });
        Ok::<_, ()>(())
    })
    .expect("first stages");
    let seen = g
        .stage(|tx| Ok::<_, ()>(tx.get::<Widget>(Id::from_raw(1)).unwrap()))
        .expect("second stages");
    assert_eq!(
        seen.map(|w| w.count),
        Some(7),
        "B must see A's write, or a batch silently loses updates"
    );
    g.commit().expect("commit");
}

/// The assertion deferred from Task 3 (Noah ruling, 2026-08-11): isolation is
/// what makes panic-safety structural per spec §2, and `stage` is where it
/// first becomes publicly observable.
#[test]
fn a_staged_transaction_that_fails_leaves_the_group_untouched() {
    let mut db = fresh_db(0x60C0_0006);
    let mut g = db.begin_group().expect("group");
    let r = g.stage(|tx| {
        tx.put(&Widget {
            id: Id::from_raw(1),
            count: 1,
        });
        Err::<(), _>("rejected")
    });
    assert!(
        matches!(r, Err(TxError::App("rejected"))),
        "the app error surfaces as its own"
    );
    assert_eq!(g.len(), 0, "a rejected transaction queues no record");
    let seen = g
        .stage(|tx| Ok::<_, ()>(tx.get::<Widget>(Id::from_raw(1)).unwrap()))
        .expect("stages");
    assert!(
        seen.is_none(),
        "a rejected transaction's writes must never reach the group overlay — this is what \
         makes a panicking handler structurally unable to corrupt the batch"
    );
}

#[test]
fn a_panicking_stage_leaves_the_group_clean() {
    let mut db = fresh_db(0x60C0_0004);
    let mut g = db.begin_group().expect("group");
    g.stage(|tx| {
        tx.put(&Widget {
            id: Id::from_raw(1),
            count: 1,
        });
        Ok::<_, ()>(())
    })
    .expect("first stages");
    let boom = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        g.stage(|tx| -> Result<(), ()> {
            tx.put(&Widget {
                id: Id::from_raw(2),
                count: 2,
            });
            panic!("handler exploded");
        })
    }));
    assert!(boom.is_err(), "the panic propagates out of stage");
    let seen = g
        .stage(|tx| Ok::<_, ()>(tx.get::<Widget>(Id::from_raw(2)).unwrap()))
        .expect("third stages");
    assert_eq!(
        seen, None,
        "a panicking handler's writes must never reach the group"
    );
    g.commit().expect("commit");
}

/// Task 4 fix round, Important 2: `Group` is reachable from any app crate
/// via `charpente::core::db` (no `pub(crate)` boundary stops it), so a
/// dropped-without-commit group is the same silent-write-loss bug class
/// `Tx::over`'s own rationale comment says app code cannot express — one
/// level up. The `Drop` guard makes it loud instead.
#[test]
#[should_panic(expected = "contract:")]
fn dropping_a_group_with_staged_records_panics() {
    let mut db = fresh_db(0x60C0_0007);
    let mut g = db.begin_group().expect("group");
    g.stage(|tx| {
        tx.put(&Widget {
            id: Id::from_raw(1),
            count: 1,
        });
        Ok::<_, ()>(())
    })
    .expect("stages");
    drop(g); // never committed — must panic, not silently lose the write
}

/// Task 4 fix round, Important 3: `MAX_BATCH` must actually bound the
/// group, not merely be reported by `is_full()`. Stages one past the cap
/// (65 single-record transactions against `MAX_BATCH = 64`) and expects the
/// 65th `stage` call itself to refuse before queuing a 65th record.
#[test]
#[should_panic(expected = "contract: a group holds at most 64 staged records")]
fn stage_refuses_past_max_batch() {
    let mut db = fresh_db(0x60C0_0008);
    let mut g = db.begin_group().expect("group");
    for i in 0..65u64 {
        g.stage(|tx| {
            tx.put(&Widget {
                id: Id::from_raw(i),
                count: i as u32,
            });
            Ok::<_, ()>(())
        })
        .expect("stages");
    }
}

/// Task 4 fix round, Important 2's `panicking()` guard specifically — not
/// merely the ordinary arm `dropping_a_group_with_staged_records_panics`
/// exercises. Spec §4 row 2 is a handler panicking with records already
/// staged: the group (owned by the caller's stack frame, not the panicking
/// closure) drops mid-unwind, non-empty. Without the guard this is a
/// double panic during unwind, which Rust turns into `abort()` — killing
/// the whole test process rather than failing one test — so `catch_unwind`
/// successfully returning `Err` here IS the proof nothing doubled.
#[test]
fn a_panic_elsewhere_does_not_double_panic_through_a_staged_groups_drop() {
    let mut db = fresh_db(0x60C0_0009);
    let boom = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut g = db.begin_group().expect("group");
        g.stage(|tx| {
            tx.put(&Widget {
                id: Id::from_raw(1),
                count: 1,
            });
            Ok::<_, ()>(())
        })
        .expect("stages");
        panic!("unrelated failure after staging, before commit");
        // `g` drops here during unwind, 1 record staged: the guard must
        // stay silent (std::thread::panicking() is true), or this aborts
        // the process instead of unwinding to catch_unwind below.
    }));
    assert!(
        boom.is_err(),
        "the original panic must propagate to catch_unwind, not abort the process"
    );
}

// -------------------------------------------------- t5 fix r2, m1

/// A fixed-width row: two `u64` fields, nothing schema-dependent, so it
/// seals in a small, easy-to-reason-about number of bytes per record.
struct Tiny {
    id: Id<Tiny>,
    value: u64,
}

impl Row for Tiny {
    type Scope = Unscoped;
    const TABLE: &'static str = "Tiny";
    const INDEXES: &'static [IndexDef<Tiny>] = &[];

    fn id(&self) -> Id<Tiny> {
        self.id
    }

    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.value.encode(out);
    }

    fn decode_row(bytes: &[u8]) -> io::Result<Tiny> {
        let input = &mut &bytes[..];
        Ok(Tiny {
            id: Id::decode(input)?,
            value: u64::decode(input)?,
        })
    }
}

/// Wraps a real `SimDisk`, adding one test-only switch: once armed, `list`
/// swaps the SECOND and THIRD names in whatever the inner disk actually
/// returned, leaving the first (base_seq 0) exactly where it was. Everything
/// else (`open`/`remove`/`sync_dir`) delegates straight through — same idiom
/// as `FaultyTracingDisk` in `charpente/tests/group_commit.rs`, an
/// `Arc`-shared flag rather than an owned one because `Db::open` takes
/// ownership of the `Box<dyn Disk>` and the test needs to flip the switch
/// afterward.
///
/// A swap, not a full reversal: reversing a real segment listing always
/// moves base_seq 0 to the END, which trips the OLD `checked_sub` panic (the
/// specific `[…, 0]` shape it already half-covers per m1) rather than
/// proving the NEW assert. Swapping an interior pair produces the general
/// `[…, 8, 4, …]` shape the review's `[5, 3]` example is about — unsorted,
/// but not ending in 0 — which the old panic does not catch at all.
struct ScrambledListDisk {
    inner: charpente_sim::SimDisk,
    scramble: Arc<AtomicBool>,
}

impl Disk for ScrambledListDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        self.inner.open(path)
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut names = self.inner.list(prefix)?;
        if self.scramble.load(Ordering::SeqCst) && names.len() >= 3 {
            names.swap(1, 2);
        }
        Ok(names)
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

/// m1 (task-5-review.md): `Db::sealed_segments`'s `checked_sub` panic only
/// covers the single unsorted shape `[…, 0]` — any other unsorted listing
/// (e.g. `[5, 3]`) silently produced an inverted `layout_end < base_seq`
/// with no assert to catch it at the source. This proves the new
/// `bases.windows(2)` assert (db.rs) actually fires: seal several real
/// segments through a normal, unscrambled disk (so WAL recovery/rotation
/// never sees a bad listing), then arm the switch and call
/// `sealed_segments()` again — now fed the same real segment names with an
/// interior pair swapped (an unsorted, non-zero-terminated shape, the same
/// violation class as `[5, 3]`).
#[test]
#[should_panic(
    expected = "Disk::list(\"wal/\") returned a segment listing that is not strictly \
                            increasing"
)]
fn sealed_segments_asserts_disk_list_is_sorted() {
    let sim = Sim::with_faults(0x5EA1_5E67_1157, FaultConfig::none());
    let scramble = Arc::new(AtomicBool::new(false));
    let disk = ScrambledListDisk {
        inner: sim.disk(),
        scramble: scramble.clone(),
    };
    let mut db_cfg = cfg();
    db_cfg.wal.segment_bytes = 200; // tiny: several records seal a segment
    let mut db = Db::open(Box::new(disk), db_cfg, Schema::new().table::<Tiny>()).unwrap();
    for i in 0..50u64 {
        db.transact(|tx| -> Result<(), std::convert::Infallible> {
            tx.put(&Tiny {
                id: Id::from_raw(i),
                value: i,
            });
            Ok(())
        })
        .unwrap();
    }
    let sealed = db.sealed_segments().unwrap();
    assert!(
        sealed.len() >= 3,
        "fixture sealed too few segments to exercise the assert meaningfully: {}",
        sealed.len()
    );

    scramble.store(true, Ordering::SeqCst);
    let _ = db.sealed_segments();
}

/// A job with no payload, for the two tests below, which read the job queue
/// inside a batch the way the dispatcher does.
struct Ping;

impl Codec for Ping {
    fn encode(&self, _out: &mut Vec<u8>) {}
    fn decode(_input: &mut &[u8]) -> io::Result<Ping> {
        Ok(Ping)
    }
}

impl Job for Ping {
    const NAME: &'static str = "ping";
    const FINGERPRINT: u64 = 0x7C00_0900;
    type Scope = Unscoped;
}

/// DESIGN §2: the group overlay carries rows only. An enqueue is a log effect
/// that becomes a `JobRow` at APPLY, with the id the `next_job_id` counter
/// assigns there, so a transaction staged behind it in the same batch cannot
/// see it — and no transaction can look, except the dispatcher's
/// `Tx::get_job`. This was pinned from two handlers that scanned the queue
/// and the event log (`charpente/tests/group_commit.rs`, seed `0x60C1_0007`)
/// until no transaction could read either (tenancy-completion spec §3).
#[test]
fn an_enqueue_is_invisible_to_a_later_sibling_until_the_batch_commits() {
    let mut db = fresh_db(0x7C00_0901);
    let mut g = db.begin_group().expect("group");
    let admission = g.tenant_admission();
    g.stage(|tx| {
        tx.enqueue(Ping);
        Ok::<_, ()>(())
    })
    .expect("the enqueue stages");
    let seen = g
        .stage(|tx| {
            tx.get_job(Id::from_raw(0), &admission)
                .map_err(|e| e.to_string())
        })
        .expect("the read stages");
    assert!(
        seen.is_none(),
        "an enqueue staged earlier in this batch must be invisible to a later sibling: job rows \
         materialize at apply, so no staged transaction can see one — got {:?}",
        seen.map(|row| (row.id.raw(), row.name))
    );
    assert_eq!(
        g.len(),
        1,
        "the enqueue is the batch's one record; the read stages none"
    );
    g.commit().expect("commit");
    let jobs = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert_eq!(
        jobs.iter()
            .map(|row| (row.id.raw(), row.name.as_str(), row.status))
            .collect::<Vec<_>>(),
        vec![(0, "ping", JobStatus::Pending)],
        "the job DID materialize at apply, once the batch committed"
    );
}

/// The dispatcher's failure stage reads a job row again after an earlier
/// stage of the same batch may have settled it (dispatch.rs, the
/// retry-policy read), so `Tx::get_job` reads what `View::get` reads: this
/// transaction's writes, then the group's, then the snapshot.
#[test]
fn the_dispatchers_job_read_sees_a_settle_staged_earlier_in_its_batch() {
    let mut db = fresh_db(0x7C00_0902);
    db.transact(|tx| {
        tx.enqueue(Ping);
        Ok::<_, ()>(())
    })
    .expect("the enqueue commits");
    let mut g = db.begin_group().expect("group");
    let admission = g.tenant_admission();
    g.stage(|tx| {
        let mut row = tx
            .get_job(Id::from_raw(0), &admission)
            .map_err(|e| e.to_string())?
            .expect("the committed enqueue materialized job 0");
        row.attempts = 1;
        row.last_error = Some("the relay timed out".to_string());
        tx.settle_job(&row, &admission);
        Ok::<_, String>(())
    })
    .expect("the settle stages");
    let seen = g
        .stage(|tx| {
            tx.get_job(Id::from_raw(0), &admission)
                .map_err(|e| e.to_string())
        })
        .expect("the read stages")
        .expect("job 0 exists");
    assert_eq!(
        (seen.attempts, seen.last_error.as_deref()),
        (1, Some("the relay timed out")),
        "a job read inside a batch must see the settle an earlier stage of that batch made"
    );
    g.commit().expect("commit");
}

// 0.5.0 Task 1: `tx.now()`. These live here rather than in db.rs's `mod tests`,
// which cannot build a `Db` (see the header).

#[test]
fn tx_now_reads_the_groups_time() {
    let mut db = fresh_db(0x7A10_0001);
    let mut g = db.begin_group().expect("group");
    g.set_now(WallTime::from_nanos(7));
    for _ in 0..2 {
        let seen = g.stage(|tx| Ok::<_, ()>(tx.now())).expect("stage");
        assert_eq!(seen, WallTime::from_nanos(7));
    }
    g.commit().expect("commit");
}

#[test]
fn transact_at_hands_its_time_to_the_closure() {
    let mut db = fresh_db(0x7A10_0002);
    let seen = db
        .transact_at(WallTime::from_nanos(41), |tx| Ok::<_, ()>(tx.now()))
        .expect("transact_at");
    assert_eq!(seen, WallTime::from_nanos(41));
}

#[test]
#[should_panic(
    expected = "contract: tx.now() reads the transaction's time, and this transaction has none"
)]
fn tx_now_without_a_time_source_is_refused_by_name() {
    let mut db = fresh_db(0x7A10_0003);
    let _ = db.transact(|tx| {
        tx.now();
        Ok::<_, ()>(())
    });
}

#[test]
#[should_panic(
    expected = "contract: Group::set_now is called once, before the group's first stage"
)]
fn set_now_after_a_stage_is_refused() {
    let mut db = fresh_db(0x7A10_0004);
    let mut g = db.begin_group().expect("group");
    g.stage(|tx| {
        tx.put(&Widget {
            id: Id::from_raw(1),
            count: 1,
        });
        Ok::<_, ()>(())
    })
    .expect("stage");
    g.set_now(WallTime::from_nanos(1));
}

// 0.5.0 Task 3: the timed enqueue (tag 0x0e).

#[test]
fn a_timed_enqueue_materializes_its_due_time() {
    let mut db = fresh_db(0x7A30_0001);
    db.transact(|tx| {
        tx.enqueue(Ping);
        tx.enqueue_at(WallTime::from_nanos(5_000), Ping);
        Ok::<_, ()>(())
    })
    .expect("transact");
    let drained = db.drain_enqueued();
    assert_eq!(
        drained.iter().map(|e| e.due_at).collect::<Vec<_>>(),
        vec![WallTime::UNIX_EPOCH, WallTime::from_nanos(5_000)],
        "an untimed enqueue is due at the epoch, a timed one at its time"
    );
    let rows = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert_eq!(
        rows.iter().map(|r| r.due_at).collect::<Vec<_>>(),
        vec![WallTime::UNIX_EPOCH, WallTime::from_nanos(5_000)]
    );
}

#[test]
fn enqueue_after_adds_the_delay_to_the_transactions_time() {
    let mut db = fresh_db(0x7A30_0002);
    db.transact_at(WallTime::from_nanos(1_000), |tx| {
        tx.enqueue_after(std::time::Duration::from_nanos(250), Ping);
        Ok::<_, ()>(())
    })
    .expect("transact_at");
    assert_eq!(db.drain_enqueued()[0].due_at, WallTime::from_nanos(1_250));
}

/// Review Focus 5: a delay centuries out saturates; it neither panics nor wraps
/// to a time in the past, and the job stays Pending.
#[test]
fn enqueue_after_saturates_instead_of_overflowing() {
    let mut db = fresh_db(0x7A30_0003);
    db.transact_at(WallTime::from_nanos(u64::MAX - 1), |tx| {
        tx.enqueue_after(std::time::Duration::from_nanos(10), Ping);
        tx.enqueue_after(std::time::Duration::MAX, Ping);
        Ok::<_, ()>(())
    })
    .expect("transact_at");
    let drained = db.drain_enqueued();
    assert_eq!(drained[0].due_at, WallTime::from_nanos(u64::MAX));
    assert_eq!(drained[1].due_at, WallTime::from_nanos(u64::MAX));
    let rows = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert!(rows.iter().all(|r| r.status == JobStatus::Pending));
    db.transact_at(WallTime::from_nanos(0), |tx| {
        tx.enqueue_after(std::time::Duration::MAX, Ping);
        Ok::<_, ()>(())
    })
    .expect("transact_at");
    assert_eq!(
        db.drain_enqueued()[0].due_at,
        WallTime::from_nanos(u64::MAX)
    );
}

#[test]
#[should_panic(
    expected = "contract: tx.now() reads the transaction's time, and this transaction has none"
)]
fn enqueue_after_needs_a_time_source() {
    let mut db = fresh_db(0x7A30_0004);
    let _ = db.transact(|tx| {
        tx.enqueue_after(std::time::Duration::from_secs(1), Ping);
        Ok::<_, ()>(())
    });
}

// 0.5.0 Task 9: a re-issue resends the dead job's own request under its own key.

/// An outbound job whose payload is one byte, so a copy of the request is told
/// from a fresh one.
struct Charge(u8);

impl Codec for Charge {
    fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.0);
    }
    fn decode(input: &mut &[u8]) -> io::Result<Charge> {
        Ok(Charge(u8::decode(input)?))
    }
}

impl Job for Charge {
    const NAME: &'static str = "charge";
    const FINGERPRINT: u64 = charpente_core::db::outbound_fingerprint(0x7E57_0C0C);
    type Scope = Unscoped;
    const OUTBOUND: bool = true;
}

/// Counts what a minting group draws.
struct CountingEntropy(std::sync::atomic::AtomicU32);

impl Entropy for CountingEntropy {
    fn fill(&self, buf: &mut [u8]) {
        self.0.fetch_add(1, Ordering::SeqCst);
        buf.fill(0xEE);
    }
}

/// The row the dispatcher hands `DeadRecord::from_row` for a failed `Charge`.
fn failed_charge(payload: u8, key: Option<[u8; 16]>, scope: Option<u64>) -> JobRow {
    JobRow {
        id: Id::from_raw(9),
        name: Charge::NAME.into(),
        payload: vec![payload],
        key: None,
        status: JobStatus::Pending,
        attempts: 3,
        due_at: WallTime::UNIX_EPOCH,
        last_error: Some("the remote refused".into()),
        shape: Some(Charge::FINGERPRINT),
        scope,
        outbound_key: key,
    }
}

/// The record is the timed enqueue with the dead job's own name, payload
/// bytes, shape and key, due when asked, and nothing was drawn.
#[test]
fn a_reissue_stages_the_dead_jobs_own_request_under_its_key() {
    let mut db = fresh_db(0x7A30_0009);
    let entropy = CountingEntropy(Default::default());
    let mut g = db.begin_group_minting(&entropy).expect("group");
    let dead = DeadRecord::from_row(
        &failed_charge(0x5A, Some([7; 16]), None),
        &g.tenant_admission(),
    )
    .into_typed::<Charge>();
    g.stage(|tx| {
        tx.reissue_at(WallTime::from_nanos(77), dead);
        Ok::<_, ()>(())
    })
    .expect("stage");
    g.commit().map_err(|f| f.error).expect("commit");
    assert_eq!(
        entropy.0.load(Ordering::SeqCst),
        0,
        "a supplied key draws no Entropy"
    );
    let rows = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(
        (
            &*row.name,
            &row.payload[..],
            row.outbound_key,
            row.due_at,
            row.shape,
            row.scope
        ),
        (
            "charge",
            &[0x5A][..],
            Some([7; 16]),
            WallTime::from_nanos(77),
            Some(Charge::FINGERPRINT),
            None
        )
    );
    assert_eq!(row.status, JobStatus::Pending);
}

/// A supplied key needs no mint, so a group that cannot mint stages a
/// re-issue (spec §5 D: the loop skips a supplied key). The same group
/// refuses a fresh outbound enqueue by name, next test: the loop skips only a
/// key that is already there.
#[test]
fn a_reissue_stages_in_a_group_that_cannot_mint() {
    let mut db = fresh_db(0x7A30_000A);
    let mut g = db.begin_group().expect("group");
    g.set_now(WallTime::from_nanos(1_000));
    let dead = DeadRecord::from_row(
        &failed_charge(1, Some([3; 16]), None),
        &g.tenant_admission(),
    )
    .into_typed::<Charge>();
    g.stage(|tx| {
        tx.reissue_after(std::time::Duration::from_secs(60), dead);
        Ok::<_, ()>(())
    })
    .expect("a supplied key needs no mint");
    g.commit().map_err(|f| f.error).expect("commit");
    let rows = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert_eq!(
        (rows[0].outbound_key, rows[0].due_at),
        (Some([3; 16]), WallTime::from_nanos(1_000 + 60_000_000_000))
    );
}

#[test]
#[should_panic(
    expected = "contract: job \"charge\" is outbound, so its enqueue needs an idempotency key"
)]
fn a_fresh_outbound_enqueue_in_a_group_that_cannot_mint_still_panics() {
    let mut db = fresh_db(0x7A30_000B);
    let mut g = db.begin_group().expect("group");
    let _ = g.stage(|tx| {
        tx.enqueue(Charge(1));
        Ok::<_, ()>(())
    });
}

/// A pure job's re-issue carries no key, since it never had one: a delayed
/// re-enqueue of its payload.
#[test]
fn a_pure_jobs_reissue_carries_no_key() {
    let mut db = fresh_db(0x7A30_000C);
    let mut g = db.begin_group().expect("group");
    let mut row = failed_charge(0, None, None);
    row.name = Ping::NAME.into();
    row.shape = Some(Ping::FINGERPRINT);
    let dead = DeadRecord::from_row(&row, &g.tenant_admission()).into_typed::<Ping>();
    g.stage(|tx| {
        tx.reissue_at(WallTime::from_nanos(5), dead);
        Ok::<_, ()>(())
    })
    .expect("stage");
    g.commit().map_err(|f| f.error).expect("commit");
    let rows = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert_eq!(
        (rows[0].outbound_key, rows[0].due_at),
        (None, WallTime::from_nanos(5))
    );
}

/// A scoped record re-issues only inside the tenant it names: this `Tx`
/// opened none.
#[test]
#[should_panic(
    expected = "contract: a re-issue of scoped job \"ping\" must stay in tenant 4, the one its \
                Dead record names \u{2014} re-issue it from its own on_dead, whose ScopedTx is \
                that tenant"
)]
fn a_scoped_record_reissues_only_inside_its_tenant() {
    let mut db = fresh_db(0x7A30_000D);
    let mut g = db.begin_group().expect("group");
    let mut row = failed_charge(0, None, Some(4));
    row.name = Ping::NAME.into();
    row.shape = Some(Ping::FINGERPRINT);
    let dead = DeadRecord::from_row(&row, &g.tenant_admission()).into_typed::<Ping>();
    let _ = g.stage(|tx| {
        tx.reissue_at(WallTime::from_nanos(5), dead);
        Ok::<_, ()>(())
    });
}
