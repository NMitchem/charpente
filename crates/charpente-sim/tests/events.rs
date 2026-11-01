//! `checks::events` and `events_scoped`: the durable event log read as the
//! app's own types, every page of it.

use std::io;

use charpente::auth::Principal;
use charpente::core::event::EventRow;
use charpente::core::table::Codec;
use charpente::http::ApiError;
use charpente::wire::WireU64;
use charpente::{Db, DbConfig, Id, Schema, Tenant, Tx, View, event};
use charpente_sim::checks::{EVENTS_PAGE, events, events_scoped};
use charpente_sim::{FaultConfig, Sim};

#[event(global)]
struct Opened {
    n: WireU64,
}

#[event(global)]
struct Closed {
    n: WireU64,
}

struct Org;

impl Tenant for Org {
    const NAME: &'static str = "Org";
    type Who = Principal;
    type Error = ApiError;
    fn authorize(_tx: &Tx, _who: &Principal, _scope: Id<Org>) -> Result<(), ApiError> {
        Ok(())
    }
}

#[event(scoped_by = Org)]
struct Paid {
    n: WireU64,
}

fn open(seed: u64) -> Db {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap()
}

fn nums<E>(rows: &[E], n: impl Fn(&E) -> u64) -> Vec<u64> {
    rows.iter().map(n).collect()
}

#[test]
fn events_reads_only_its_type_in_log_order() {
    let mut db = open(0xE7E0_0001);
    for i in 0..4u64 {
        db.transact(|tx| {
            tx.emit(Opened { n: WireU64(i) });
            if i % 2 == 0 {
                tx.emit(Closed {
                    n: WireU64(100 + i),
                });
            }
            Ok::<(), io::Error>(())
        })
        .unwrap();
    }
    let snap = db.snapshot().unwrap();
    assert_eq!(
        nums(&events::<Opened>(&snap).unwrap(), |e| e.n.0),
        [0, 1, 2, 3]
    );
    assert_eq!(
        nums(&events::<Closed>(&snap).unwrap(), |e| e.n.0),
        [100, 102]
    );
}

#[test]
fn events_scoped_reads_one_tenant() {
    let mut db = open(0xE7E0_0002);
    for (tenant, n) in [(1u64, 10u64), (2, 20), (1, 11), (2, 21)] {
        let mut group = db.begin_group().unwrap();
        let admission = group.tenant_admission();
        group
            .stage(|tx| {
                tx.admit_scope(Id::<Org>::from_raw(tenant), &admission)
                    .emit(Paid { n: WireU64(n) });
                Ok::<(), io::Error>(())
            })
            .unwrap();
        group.commit().unwrap();
    }
    let snap = db.snapshot().unwrap();
    assert_eq!(
        nums(&events_scoped::<Paid>(&snap, 1).unwrap(), |e| e.n.0),
        [10, 11]
    );
    assert_eq!(
        nums(&events_scoped::<Paid>(&snap, 2).unwrap(), |e| e.n.0),
        [20, 21]
    );
    assert!(events_scoped::<Paid>(&snap, 3).unwrap().is_empty());
}

#[test]
fn events_reads_past_one_page() {
    let mut db = open(0xE7E0_0003);
    let total = EVENTS_PAGE as u64 + 1;
    for i in 0..total {
        db.transact(|tx| {
            tx.emit(Opened { n: WireU64(i) });
            Ok::<(), io::Error>(())
        })
        .unwrap();
    }
    let snap = db.snapshot().unwrap();
    let got = nums(&events::<Opened>(&snap).unwrap(), |e| e.n.0);
    assert_eq!(got, (0..total).collect::<Vec<_>>());
}

/// The fix clause every "does not decode" error ends on (hard rule 5).
const UNDECODABLE: &str = " — it was written by a different shape of the event, through a change inside a type one of its fields holds, which the fingerprint cannot see (GUIDE §12), so read it with the type that wrote it; if no such type changed, the stored bytes are corrupt";

/// A hand-written event type reading `Opened`'s rows: same name, so the
/// reader selects them; `fingerprint` and decode are the knobs.
macro_rules! reads_opened_as {
    ($name:ident, $fingerprint:expr, $decode:expr) => {
        #[derive(Debug)]
        struct $name;
        impl Codec for $name {
            fn encode(&self, _out: &mut Vec<u8>) {}
            fn decode(input: &mut &[u8]) -> io::Result<$name> {
                $decode(input)
            }
        }
        impl charpente::Event for $name {
            const NAME: &'static str = <Opened as charpente::Event>::NAME;
            const FINGERPRINT: u64 = $fingerprint;
            type Scope = charpente::core::table::Unscoped;
        }
    };
}

fn one_opened_row() -> (charpente::Snapshot, u64) {
    let mut db = open(0xE7E0_0004);
    db.transact(|tx| {
        tx.emit(Opened { n: WireU64(1) });
        Ok::<(), io::Error>(())
    })
    .unwrap();
    let snap = db.snapshot().unwrap();
    let id = snap.scan::<EventRow>().unwrap()[0].id.raw();
    (snap, id)
}

#[test]
fn a_payload_that_does_not_decode_names_the_event_and_row() {
    reads_opened_as!(
        TooWide,
        <Opened as charpente::Event>::FINGERPRINT,
        |input: &mut &[u8]| {
            WireU64::decode(input)?;
            WireU64::decode(input)?;
            Ok(TooWide)
        }
    );
    let (snap, id) = one_opened_row();
    let err = events::<TooWide>(&snap).unwrap_err();
    let name = <Opened as charpente::Event>::NAME;
    assert_eq!(
        err,
        format!(
            "events: event \"{name}\" row {id} does not decode: row decode: needed 8 bytes for u64, 0 left — schema drift or corruption{UNDECODABLE}"
        ),
        "{err}"
    );
}

#[test]
fn bytes_left_over_after_the_payload_are_an_err() {
    reads_opened_as!(
        TooNarrow,
        <Opened as charpente::Event>::FINGERPRINT,
        |input: &mut &[u8]| {
            u8::decode(input)?;
            Ok(TooNarrow)
        }
    );
    let (snap, id) = one_opened_row();
    let err = events::<TooNarrow>(&snap).unwrap_err();
    let name = <Opened as charpente::Event>::NAME;
    assert_eq!(
        err,
        format!(
            "events: event \"{name}\" row {id} does not decode: 7 bytes left over after the payload{UNDECODABLE}"
        ),
        "{err}"
    );
}

#[test]
fn a_row_written_under_another_shape_is_an_err_naming_both_fingerprints() {
    reads_opened_as!(Reshaped, 0x1234, |_input: &mut &[u8]| Ok(Reshaped));
    let (snap, id) = one_opened_row();
    let written = <Opened as charpente::Event>::FINGERPRINT;
    let err = events::<Reshaped>(&snap).unwrap_err();
    let name = <Opened as charpente::Event>::NAME;
    assert_eq!(
        err,
        format!(
            "events: event \"{name}\" row {id} was written under fingerprint {written:#018x}, \
             but this type's is 0x0000000000001234; the log holds another shape of the event — \
             read it with the type that wrote it (a changed event is a new #[event] type under a \
             new name, GUIDE §11)"
        ),
        "{err}"
    );
}

/// The first page ends on a row `events` skips (a `Closed`), so it holds one
/// match fewer than it has rows: a reader that ended on a page short of
/// matches, not of rows, would stop here and drop the two rows after it. A
/// cursor that advanced only past matched rows passes this test: page 2
/// starts on the skipped row, skips it again and reads on, with the same
/// result. `events_reads_on_past_a_whole_page_of_skipped_rows` is the test
/// for that cursor.
#[test]
fn events_reads_on_when_a_full_page_ends_on_a_skipped_row() {
    let mut db = open(0xE7E0_0005);
    let mut n = 0u64;
    for i in 0..(EVENTS_PAGE as u64 + 2) {
        db.transact(|tx| {
            if i == EVENTS_PAGE as u64 - 1 {
                tx.emit(Closed { n: WireU64(i) });
            } else {
                tx.emit(Opened { n: WireU64(i) });
            }
            Ok::<(), io::Error>(())
        })
        .unwrap();
        n += 1;
    }
    let snap = db.snapshot().unwrap();
    let want: Vec<u64> = (0..n).filter(|&i| i != EVENTS_PAGE as u64 - 1).collect();
    assert_eq!(nums(&events::<Opened>(&snap).unwrap(), |e| e.n.0), want);
}

/// `events`' page-end test through the tenant filter: the first page ends on
/// tenant 2's row, so tenant 1's read of it holds one match fewer than rows,
/// and a reader that ended on a page short of matches would drop the two rows
/// after it. Like that test, it cannot see a cursor that advanced only past
/// matched rows: page 2 rereads the one skipped row and reads on.
#[test]
fn events_scoped_reads_on_when_a_full_page_ends_on_another_tenant() {
    let mut db = open(0xE7E0_0006);
    let last = EVENTS_PAGE as u64 - 1;
    for i in 0..(EVENTS_PAGE as u64 + 2) {
        let tenant = if i == last { 2 } else { 1 };
        let mut group = db.begin_group().unwrap();
        let admission = group.tenant_admission();
        group
            .stage(|tx| {
                tx.admit_scope(Id::<Org>::from_raw(tenant), &admission)
                    .emit(Paid { n: WireU64(i) });
                Ok::<(), io::Error>(())
            })
            .unwrap();
        group.commit().unwrap();
    }
    let snap = db.snapshot().unwrap();
    let want: Vec<u64> = (0..EVENTS_PAGE as u64 + 2).filter(|&i| i != last).collect();
    assert_eq!(
        nums(&events_scoped::<Paid>(&snap, 1).unwrap(), |e| e.n.0),
        want
    );
    assert_eq!(
        nums(&events_scoped::<Paid>(&snap, 2).unwrap(), |e| e.n.0),
        [last]
    );
}

/// A whole page of rows `events` skips, then one it reads. The cursor must
/// advance past every row read, matched or not. A cursor that advanced only
/// past matched rows would stay at 0 after this page and read the same full
/// page again, forever: under that mutant this test HANGS rather than fails,
/// so a run that never finishes is its red.
#[test]
fn events_reads_on_past_a_whole_page_of_skipped_rows() {
    let mut db = open(0xE7E0_0007);
    for i in 0..EVENTS_PAGE as u64 {
        db.transact(|tx| {
            tx.emit(Closed { n: WireU64(i) });
            Ok::<(), io::Error>(())
        })
        .unwrap();
    }
    db.transact(|tx| {
        tx.emit(Opened { n: WireU64(7) });
        Ok::<(), io::Error>(())
    })
    .unwrap();
    let snap = db.snapshot().unwrap();
    assert_eq!(nums(&events::<Opened>(&snap).unwrap(), |e| e.n.0), [7]);
    assert_eq!(events::<Closed>(&snap).unwrap().len(), EVENTS_PAGE);
}
