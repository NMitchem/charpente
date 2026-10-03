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

use charpente_core::db::{Db, DbConfig, Event, Job, Schema, Tenant, Tx, scoped_fingerprint};
use charpente_core::event::EventRow;
use charpente_core::job::{JobRow, JobStatus};
use charpente_core::table::{Codec, Id, Unscoped, View};
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
    const FINGERPRINT: u64 = 0x7E57_0009;
    type Scope = Unscoped;
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
    const FINGERPRINT: u64 = 0x7E57_000A;
    // A global event: it belongs to no tenant.
    type Scope = Unscoped;
}

/// The tenant the scoped-emit test opens; it admits everyone.
struct TestOrg;
impl Tenant for TestOrg {
    const NAME: &'static str = "TestOrg";
    type Who = str;
    type Error = io::Error;
    fn authorize(_tx: &Tx, _who: &str, _scope: Id<TestOrg>) -> Result<(), io::Error> {
        Ok(())
    }
}

/// The event emitted inside `TestOrg`'s scope: its own type beside
/// `TestEvent`, scoped by `TestOrg` (tenancy-completion spec §6), so
/// `ScopedTx::emit` takes it and a bare `Tx` does not. Its hand-written
/// payload fold is a literal, as a hand-written `Row`'s shape is, and its
/// `FINGERPRINT` mixes `TestOrg`'s `NAME` into it, as `#[event]` would:
/// `ScopedTx::emit` checks exactly that equation.
struct OrgEvent {
    n: u32,
}
impl Codec for OrgEvent {
    fn encode(&self, out: &mut Vec<u8>) {
        self.n.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<OrgEvent> {
        Ok(OrgEvent {
            n: u32::decode(input)?,
        })
    }
}
impl Event for OrgEvent {
    const NAME: &'static str = "org_event";
    const PAYLOAD_FINGERPRINT: u64 = 0x7E57_000B;
    const FINGERPRINT: u64 = scoped_fingerprint(Self::PAYLOAD_FINGERPRINT, TestOrg::NAME);
    type Scope = TestOrg;
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
fn enqueues_and_emits_record_their_types_shape() {
    let sim = Sim::with_faults(0x605, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    db.transact(|tx: &mut Tx| {
        tx.enqueue(Ping);
        tx.emit(TestEvent { n: 1 });
        Ok::<(), io::Error>(())
    })
    .unwrap();
    let snap = db.snapshot().unwrap();
    assert_eq!(
        snap.scan::<JobRow>().unwrap()[0].shape,
        Some(<Ping as Job>::FINGERPRINT)
    );
    let events: Vec<EventRow> = snap.scan_from(0, 1).unwrap();
    assert_eq!(events[0].shape, Some(<TestEvent as Event>::FINGERPRINT));
}

/// Spec §6.6: a record a pre-shape binary wrote (`0x02` enqueue, `0x03`
/// emit), applied the way a follower applies shipped frames, materializes
/// with no shape — which every later check treats as foreign.
#[test]
fn a_pre_shape_record_materializes_with_no_shape() {
    let sim = Sim::with_faults(0x606, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    let mut record = Vec::new();
    2u32.encode(&mut record);
    record.push(2);
    "ping".to_string().encode(&mut record);
    Vec::<u8>::new().encode(&mut record);
    None::<String>.encode(&mut record);
    record.push(3);
    "test_event".to_string().encode(&mut record);
    vec![1u8, 0, 0, 0].encode(&mut record);
    None::<String>.encode(&mut record);
    let seq = db.last_applied().map_or(0, |s| s + 1);
    db.apply_shipped(&[(seq, record)]).unwrap();
    let snap = db.snapshot().unwrap();
    assert_eq!(snap.scan::<JobRow>().unwrap()[0].shape, None);
    let events: Vec<EventRow> = snap.scan_from(0, 1).unwrap();
    assert_eq!(events[0].shape, None);
}

/// Job-tenancy spec §6, Review Focus 2 and 4. A shipped record carrying the
/// scoped enqueue tag `0x0a`, applied the way a follower, a restore and a
/// replay all apply one, materializes a `JobRow` whose `scope` is the
/// record's. Tenant 0 comes through as `Some(0)`, and the `0x08` enqueue
/// beside it as `None`.
#[test]
fn a_shipped_scoped_enqueue_materializes_with_its_scope_tenant_zero_included() {
    let sim = Sim::with_faults(0x607, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    let mut record = Vec::new();
    3u32.encode(&mut record);
    for (payload, scope) in [(1u8, Some(0u64)), (2, Some(7)), (3, None)] {
        record.push(if scope.is_some() { 10 } else { 8 });
        "ping".to_string().encode(&mut record);
        vec![payload].encode(&mut record);
        None::<String>.encode(&mut record);
        0x5E4Du64.encode(&mut record);
        if let Some(scope) = scope {
            scope.encode(&mut record);
        }
    }
    let seq = db.last_applied().map_or(0, |s| s + 1);
    db.apply_shipped(&[(seq, record)]).unwrap();
    let rows = db.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (r.payload.clone(), r.shape, r.scope))
            .collect::<Vec<_>>(),
        vec![
            (vec![1], Some(0x5E4D), Some(0)),
            (vec![2], Some(0x5E4D), Some(7)),
            (vec![3], Some(0x5E4D), None),
        ],
        "each row carries its record's tenant; tenant 0 is Some(0), not None"
    );
}

/// Tenancy-completion spec §6, the `0x0a` precedent. A shipped record
/// carrying the scoped emit tag `0x0b`, applied the way a follower, a restore
/// and a replay all apply one, materializes an `EventRow` whose `scope` is
/// the record's and whose key is untouched. Tenant 0 comes through as
/// `Some(0)`, and the `0x09` emits beside it as `None`.
#[test]
fn a_shipped_scoped_emit_materializes_with_its_scope_tenant_zero_included() {
    let sim = Sim::with_faults(0x608, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    let effects: [(u8, Option<&str>, Option<u64>); 4] = [
        (1, None, Some(0)),
        (2, Some("room-9"), Some(7)),
        (3, None, None),
        (4, Some("room-9"), None),
    ];
    let mut record = Vec::new();
    4u32.encode(&mut record);
    for (payload, key, scope) in effects {
        record.push(if scope.is_some() { 11 } else { 9 });
        "test_event".to_string().encode(&mut record);
        vec![payload].encode(&mut record);
        key.map(str::to_string).encode(&mut record);
        0x7E57_000Au64.encode(&mut record);
        if let Some(scope) = scope {
            scope.encode(&mut record);
        }
    }
    let seq = db.last_applied().map_or(0, |s| s + 1);
    db.apply_shipped(&[(seq, record)]).unwrap();
    let rows: Vec<EventRow> = db.snapshot().unwrap().scan_from(0, 16).unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (r.payload.clone(), r.key.clone(), r.shape, r.scope))
            .collect::<Vec<_>>(),
        vec![
            (vec![1], None, Some(0x7E57_000A), Some(0)),
            (
                vec![2],
                Some("room-9".to_string()),
                Some(0x7E57_000A),
                Some(7)
            ),
            (vec![3], None, Some(0x7E57_000A), None),
            (vec![4], Some("room-9".to_string()), Some(0x7E57_000A), None),
        ],
        "each row carries its record's tenant beside its key; tenant 0 is Some(0), not None"
    );
}

/// Tenancy-completion spec §6 and §2.3: an emit on a `ScopedTx` records the
/// tenant in the row's own field and leaves the key the app's — `emit` with
/// no key, `emit_keyed` with the app's — and the explicit deref emits a
/// global event, with no tenant, although the transaction holds one. Tenant
/// 0 comes through as `Some(0)`.
#[test]
fn a_scoped_emit_records_its_tenant_apart_from_its_key() {
    let sim = Sim::with_faults(0x609, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    db.transact(|tx: &mut Tx| -> Result<(), io::Error> {
        let mut scoped = tx.open_scope(Id::<TestOrg>::from_raw(0), "member")?;
        scoped.emit(OrgEvent { n: 1 });
        scoped.emit_keyed("room-9", OrgEvent { n: 2 });
        (*scoped).emit(TestEvent { n: 3 });
        (*scoped).emit_keyed("room-9", TestEvent { n: 4 });
        Ok(())
    })
    .unwrap();
    let rows: Vec<EventRow> = db.snapshot().unwrap().scan_from(0, 16).unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (r.name.as_str(), r.key.as_deref(), r.scope))
            .collect::<Vec<_>>(),
        vec![
            ("org_event", None, Some(0)),
            ("org_event", Some("room-9"), Some(0)),
            ("test_event", None, None),
            ("test_event", Some("room-9"), None),
        ],
        "the tenant rides its own field, tenant 0 included; the key is the app's alone"
    );
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
fn checkpoint_gc_retains_exactly_event_retention_events() {
    let sim = Sim::with_faults(0x603, FaultConfig::none());
    let cfg = DbConfig {
        event_retention: 4,
        checkpoint_every: 0,
        ..DbConfig::default()
    };
    let mut db = Db::open(Box::new(sim.disk()), cfg, Schema::new()).unwrap();
    for i in 0..10u32 {
        db.transact(|tx: &mut Tx| {
            tx.emit(TestEvent { n: i });
            Ok::<_, ()>(())
        })
        .unwrap();
    }
    db.checkpoint().unwrap();
    let snap = db.snapshot().unwrap();
    let rows: Vec<EventRow> = snap.scan_from(0, 64).unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].id.raw(), 6); // ids 6..=9 survive
    assert_eq!(db.event_floor().unwrap(), 6);
}

#[test]
fn event_floor_is_next_event_id_when_empty() {
    let sim = Sim::with_faults(0x604, FaultConfig::none());
    let cfg = DbConfig {
        event_retention: 0,
        checkpoint_every: 0,
        ..DbConfig::default()
    };
    let mut db = Db::open(Box::new(sim.disk()), cfg, Schema::new()).unwrap();
    // Fresh Db, no emits:
    assert_eq!(db.event_floor().unwrap(), 0);

    // After 3 emits and a checkpoint with event_retention: 0, GC deletes
    // everything up to the tail — the floor falls back to next_event_id.
    for i in 0..3u32 {
        db.transact(|tx: &mut Tx| {
            tx.emit(TestEvent { n: i });
            Ok::<_, ()>(())
        })
        .unwrap();
    }
    db.checkpoint().unwrap();
    assert_eq!(db.event_floor().unwrap(), db.next_event_id().unwrap());
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
