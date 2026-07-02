//! Typed transactions (PLAN.md M0 step 4): read-your-own-writes, atomic
//! effects records, abort semantics, recovery replay, and the contract
//! violations — unit tests first; the crash-property suite follows in the
//! same file (added by the next plan task).

use std::io;

use charpente::{Db, DbConfig, Event, Job, Schema, TxError, View, table};
use charpente_core::table::{Codec, Id};
use charpente_sim::{FaultConfig, Sim};

#[table]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table]
struct Invoice {
    id: Id<Invoice>,
    #[index]
    org: Id<Org>,
    amount_cents: i64,
    paid: bool,
}

struct SendReceipt {
    invoice: u64,
}

impl Codec for SendReceipt {
    fn encode(&self, out: &mut Vec<u8>) {
        self.invoice.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<SendReceipt> {
        Ok(SendReceipt {
            invoice: u64::decode(input)?,
        })
    }
}

impl Job for SendReceipt {
    const NAME: &'static str = "send_receipt";
}

struct InvoiceFinalized {
    invoice: u64,
}

impl Codec for InvoiceFinalized {
    fn encode(&self, out: &mut Vec<u8>) {
        self.invoice.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<InvoiceFinalized> {
        Ok(InvoiceFinalized {
            invoice: u64::decode(input)?,
        })
    }
}

impl Event for InvoiceFinalized {
    const NAME: &'static str = "invoice_finalized";
}

fn invoice(id: u64, org: u64, amount_cents: i64) -> Invoice {
    Invoice {
        id: Id::from_raw(id),
        org: Id::from_raw(org),
        amount_cents,
        paid: false,
    }
}

fn ids(rows: &[Invoice]) -> Vec<u64> {
    rows.iter().map(|r| r.id.raw()).collect()
}

fn app_schema() -> Schema {
    Schema::new().table::<Org>().table::<Invoice>()
}

fn quiet_db(seed: u64) -> (Sim, Db) {
    let sim = Sim::with_faults(seed, FaultConfig::none());
    let db = Db::open(Box::new(sim.disk()), DbConfig::default(), app_schema()).unwrap();
    (sim, db)
}

#[test]
fn reads_see_own_writes_and_commit_applies() {
    let (_sim, mut db) = quiet_db(0x51E4);
    let total = db
        .transact(|tx| {
            tx.put(&Org {
                id: Id::from_raw(1),
                name: "acme".into(),
            });
            tx.put(&invoice(10, 1, 5000));
            tx.put(&invoice(11, 1, 200));
            tx.delete::<Invoice>(Id::from_raw(11));
            let mine = tx.scan::<Invoice>()?;
            assert_eq!(ids(&mine), vec![10], "own put and own delete both visible");
            assert_eq!(
                tx.get::<Invoice>(Id::from_raw(10))?
                    .expect("own put")
                    .amount_cents,
                5000
            );
            assert!(
                tx.get::<Invoice>(Id::from_raw(11))?.is_none(),
                "own tombstone"
            );
            Ok::<i64, io::Error>(mine.iter().map(|i| i.amount_cents).sum())
        })
        .unwrap();
    assert_eq!(total, 5000);

    let snap = db.snapshot().unwrap();
    assert_eq!(snap.scan::<Invoice>().unwrap().len(), 1);
    assert_eq!(snap.scan::<Org>().unwrap().len(), 1);
    assert_eq!(db.last_applied(), Some(0), "one transaction, one record");
}

#[test]
fn index_reads_see_own_writes() {
    let (_sim, mut db) = quiet_db(0x51E5);
    let (acme, globex) = (Id::<Org>::from_raw(1), Id::<Org>::from_raw(2));
    db.transact(|tx| {
        tx.put(&invoice(10, 1, 100));
        tx.put(&invoice(11, 2, 200));
        Ok::<_, io::Error>(())
    })
    .unwrap();

    db.transact(|tx| {
        // Move 11 to acme, add 12 at globex, drop 10 — all inside the txn.
        let mut moved = tx.get::<Invoice>(Id::from_raw(11))?.expect("committed");
        moved.org = acme;
        tx.put(&moved);
        tx.put(&invoice(12, 2, 300));
        tx.delete::<Invoice>(Id::from_raw(10));

        assert_eq!(ids(&Invoice::by_org(tx, &acme)?), vec![11]);
        assert_eq!(ids(&Invoice::by_org(tx, &globex)?), vec![12]);
        Ok::<_, io::Error>(())
    })
    .unwrap();

    // And the committed state agrees.
    let snap = db.snapshot().unwrap();
    assert_eq!(ids(&Invoice::by_org(&snap, &acme).unwrap()), vec![11]);
    assert_eq!(ids(&Invoice::by_org(&snap, &globex).unwrap()), vec![12]);
}

#[test]
fn abort_discards_everything_and_consumes_no_seq() {
    let (_sim, mut db) = quiet_db(0xAB07);
    db.transact(|tx| {
        tx.put(&invoice(1, 1, 1));
        Ok::<_, io::Error>(())
    })
    .unwrap();

    let err = db
        .transact(|tx| -> Result<(), &str> {
            tx.put(&invoice(2, 1, 2));
            tx.enqueue(SendReceipt { invoice: 2 });
            Err("changed my mind")
        })
        .unwrap_err();
    assert!(matches!(err, TxError::App("changed my mind")));

    assert_eq!(db.last_applied(), Some(0), "abort consumed no seq");
    let snap = db.snapshot().unwrap();
    assert_eq!(snap.scan::<Invoice>().unwrap().len(), 1);
    assert!(
        snap.jobs().unwrap().is_empty(),
        "aborted enqueue left no trace"
    );
}

#[test]
fn read_only_transactions_write_nothing() {
    let (_sim, mut db) = quiet_db(0x2EAD);
    db.transact(|tx| {
        tx.put(&invoice(1, 1, 7));
        Ok::<_, io::Error>(())
    })
    .unwrap();

    let amount = db
        .transact(|tx| {
            Ok::<_, io::Error>(
                tx.get::<Invoice>(Id::from_raw(1))?
                    .expect("row")
                    .amount_cents,
            )
        })
        .unwrap();
    assert_eq!(amount, 7);
    assert_eq!(db.last_applied(), Some(0), "read-only txn appended nothing");
}

#[test]
fn effects_commit_as_one_record_and_recovery_replays_them() {
    let sim = Sim::with_faults(0x0DE1, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), app_schema()).unwrap();
    db.transact(|tx| {
        tx.put(&invoice(10, 1, 5000));
        tx.enqueue(SendReceipt { invoice: 10 });
        tx.emit(InvoiceFinalized { invoice: 10 });
        Ok::<_, io::Error>(())
    })
    .unwrap();

    // No checkpoint: crash with everything open. The store reverts to
    // nothing; the WAL record must rebuild row AND job together.
    sim.restart();
    drop(db);

    let db = Db::open(Box::new(sim.disk()), DbConfig::default(), app_schema()).unwrap();
    let snap = db.snapshot().unwrap();
    assert_eq!(
        snap.scan::<Invoice>().unwrap().len(),
        1,
        "replay rebuilt the row"
    );
    let jobs = snap.jobs().unwrap();
    assert_eq!(jobs.len(), 1, "replay rebuilt the job with the row");
    let (seq, idx, name, payload) = &jobs[0];
    assert_eq!((*seq, *idx, name.as_str()), (0, 0, "send_receipt"));
    assert_eq!(
        SendReceipt::decode(&mut payload.as_slice())
            .unwrap()
            .invoice,
        10
    );
    assert_eq!(db.last_applied(), Some(0));
}

#[test]
fn recovery_names_unregistered_tables() {
    let sim = Sim::with_faults(0x5C4E, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), app_schema()).unwrap();
    db.transact(|tx| {
        tx.put(&invoice(1, 1, 1));
        Ok::<_, io::Error>(())
    })
    .unwrap();
    sim.restart();
    drop(db);

    let err = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Org>(), // Invoice missing
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("table \"Invoice\"") && msg.contains("register it in the Schema"),
        "{msg}"
    );
}

#[test]
#[should_panic(expected = "registered twice")]
fn duplicate_schema_registration_is_a_contract_violation() {
    let _ = Schema::new().table::<Invoice>().table::<Invoice>();
}

#[table]
struct Blob {
    id: Id<Blob>,
    data: Vec<u8>,
}

#[test]
#[should_panic(expected = "one log record holds at most")]
fn oversized_transactions_are_a_contract_violation() {
    let sim = Sim::with_faults(0xB16, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Blob>(),
    )
    .unwrap();
    let _ = db.transact(|tx| {
        tx.put(&Blob {
            id: Id::from_raw(1),
            data: vec![0; 17 << 20],
        });
        Ok::<_, io::Error>(())
    });
}
