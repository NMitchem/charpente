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

use charpente_core::db::{Db, DbConfig, Schema, TxError};
use charpente_core::io::{Disk, File};
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
