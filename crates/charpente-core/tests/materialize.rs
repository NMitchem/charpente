//! Enqueue-effect materialization (M0 step 6, task 2): `Db::transact`
//! enqueues become `JobRow`s at apply, ids from the monotonic meta counter.
//!
//! This lives as an integration test, not inline in `db.rs`'s `#[cfg(test)]`
//! module, because `charpente-sim` is a dev-dependency that itself depends on
//! `charpente-core` (a permitted cycle per CLAUDE.md). That cycle only
//! resolves for separate test-crate binaries: a unit test compiles
//! `charpente-core` with `--cfg test`, a distinct compilation unit from the
//! plain build `charpente-sim` links against, so trait impls (e.g. `Disk`
//! for `SimDisk`) fail to unify across the two ("multiple different versions
//! of crate charpente_core in the dependency graph"). Integration tests
//! don't hit this: both this crate and `charpente-sim` depend on the one
//! normal (non-`--test`) build.

use std::io;

use charpente_core::db::{Db, DbConfig, Event, Job, Schema, Tx};
use charpente_core::event::EventRow;
use charpente_core::job::{JobRow, JobStatus};
use charpente_core::table::{Codec, View};
use charpente_sim::{FaultConfig, Sim};

struct Ping;
impl Codec for Ping {
    fn encode(&self, _out: &mut Vec<u8>) {}
    fn decode(_input: &mut &[u8]) -> io::Result<Ping> {
        Ok(Ping)
    }
}
impl Job for Ping {
    const NAME: &'static str = "ping";
}

struct TestEvent {
    n: u32,
}
impl Codec for TestEvent {
    fn encode(&self, out: &mut Vec<u8>) {
        self.n.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<TestEvent> {
        Ok(TestEvent {
            n: u32::decode(input)?,
        })
    }
}
impl Event for TestEvent {
    const NAME: &'static str = "test_event";
}

#[test]
fn enqueues_materialize_job_rows_with_counter_ids() {
    let sim = Sim::with_faults(0x600, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    db.transact(|tx: &mut Tx| {
        tx.enqueue(Ping);
        tx.enqueue_keyed("k:1", Ping);
        Ok::<(), io::Error>(())
    })
    .unwrap();
    let drained = db.drain_enqueued();
    assert_eq!(
        drained.iter().map(|id| id.raw()).collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(db.drain_enqueued().is_empty(), "drain drains");

    let jobs = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].name, "ping");
    assert_eq!(jobs[0].key, None);
    assert_eq!(jobs[1].key.as_deref(), Some("k:1"));
    assert!(
        jobs.iter()
            .all(|j| j.status == JobStatus::Pending && j.attempts == 0 && j.due_at == 0)
    );

    // Ids keep counting across transactions.
    db.transact(|tx: &mut Tx| {
        tx.enqueue(Ping);
        Ok::<(), io::Error>(())
    })
    .unwrap();
    assert_eq!(db.drain_enqueued()[0].raw(), 2);
}

#[test]
fn emits_materialize_event_rows_in_call_order_with_monotonic_ids() {
    let sim = Sim::with_faults(0x601, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    db.transact(|tx: &mut Tx| {
        tx.emit(TestEvent { n: 1 });
        tx.emit_keyed("org-7", TestEvent { n: 2 });
        tx.emit(TestEvent { n: 3 });
        Ok::<_, ()>(())
    })
    .unwrap();
    let snap = db.snapshot().unwrap();
    let rows: Vec<EventRow> = snap.scan_from(0, 16).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].id.raw(), 0);
    assert_eq!(rows[1].id.raw(), 1);
    assert_eq!(rows[2].id.raw(), 2);
    assert_eq!(rows[1].key.as_deref(), Some("org-7"));
    assert!(rows.iter().all(|r| r.name == "test_event"));
    assert_eq!(db.next_event_id().unwrap(), 3);
    // seq stamps the committing record: all three share it.
    assert_eq!(rows[0].seq, rows[2].seq);
}

#[test]
fn scan_from_honors_from_id_and_limit() {
    let sim = Sim::with_faults(0x602, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    db.transact(|tx: &mut Tx| {
        tx.emit(TestEvent { n: 1 });
        tx.emit_keyed("org-7", TestEvent { n: 2 });
        tx.emit(TestEvent { n: 3 });
        Ok::<_, ()>(())
    })
    .unwrap();
    let snap = db.snapshot().unwrap();
    let rows: Vec<EventRow> = snap.scan_from(1, 1).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id.raw(), 1);
}
