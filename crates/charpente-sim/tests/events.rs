//! `checks::events` and `events_scoped`: the durable event log read as the
//! app's own types, every page of it.

use std::io;

use charpente::auth::Principal;
use charpente::core::event::EventRow;
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

#[test]
fn a_payload_that_does_not_decode_names_the_event_and_row() {
    let mut db = open(0xE7E0_0004);
    db.transact(|tx| {
        tx.emit(Opened { n: WireU64(1) });
        Ok::<(), io::Error>(())
    })
    .unwrap();
    // `Closed` and `Opened` share a payload shape, so give one event a name
    // whose payload is too short for its type: read `Opened`'s row as a type
    // of the same name that wants more bytes.
    #[derive(Debug)]
    struct Wide {
        a: WireU64,
        b: WireU64,
    }
    impl charpente::core::table::Codec for Wide {
        fn encode(&self, out: &mut Vec<u8>) {
            self.a.encode(out);
            self.b.encode(out);
        }
        fn decode(input: &mut &[u8]) -> io::Result<Wide> {
            Ok(Wide {
                a: WireU64::decode(input)?,
                b: WireU64::decode(input)?,
            })
        }
    }
    impl charpente::Event for Wide {
        const NAME: &'static str = <Opened as charpente::Event>::NAME;
        const FINGERPRINT: u64 = 0;
        type Scope = charpente::core::table::Unscoped;
    }
    let snap = db.snapshot().unwrap();
    let id = snap.scan::<EventRow>().unwrap()[0].id.raw();
    let err = events::<Wide>(&snap).unwrap_err();
    assert!(err.contains(<Opened as charpente::Event>::NAME), "{err}");
    assert!(err.contains(&format!("row {id}")), "{err}");
}
