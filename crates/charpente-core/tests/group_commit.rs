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

use charpente_core::db::{Db, DbConfig, Schema, TxError};
use charpente_core::table::{Codec, Id, IndexDef, Row, View};
use charpente_sim::{FaultConfig, Sim};

#[derive(Debug, PartialEq)]
struct Widget {
    id: Id<Widget>,
    count: u32,
}

impl Row for Widget {
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
