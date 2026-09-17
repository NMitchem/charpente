//! Typed tables (PLAN.md M0 step 3): the `#[table]` macro, the store's
//! durability model (Durability::None + checkpoint, WAL replay in between),
//! MVCC snapshots, and secondary indexes — first as surgical unit tests, then
//! as a full log+tables integration property under the fault matrix. This is
//! also redb's audition: its own crash recovery runs under our torn-write
//! injection here.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use charpente::table;
use charpente_core::engine::{Engine, StateMachine};
use charpente_core::io::{Disk, Entropy, File};
use charpente_core::job::JobRow;
use charpente_core::log::WalConfig;
use charpente_core::table::{Codec, Id, Snapshot, Store, UNSCOPED, View};
use charpente_sim::checks::is_fault_fallout;
use charpente_sim::{FaultConfig, Sim, run_universes_swarm};

#[table(global)]
struct Org {
    id: Id<Org>,
    name: String,
}

#[table(global)]
struct Invoice {
    id: Id<Invoice>,
    #[index]
    org: Id<Org>,
    amount_cents: i64,
    memo: Option<String>,
    paid: bool,
}

fn invoice(id: u64, org: u64, amount_cents: i64) -> Invoice {
    Invoice {
        id: Id::from_raw(id),
        org: Id::from_raw(org),
        amount_cents,
        memo: if id.is_multiple_of(2) {
            Some(format!("memo-{id}"))
        } else {
            None
        },
        paid: id.is_multiple_of(3),
    }
}

fn quiet_store() -> (Sim, Store) {
    let sim = Sim::with_faults(0x7AB1E, FaultConfig::none());
    let store = Store::open(&sim.disk(), "tables.redb").unwrap();
    (sim, store)
}

// -------------------------------------------------------- op-counting disk
//
// Modelled on `OpCounts`/`CountingDisk` in
// `crates/charpente/tests/group_commit.rs:84-155` — same shape (write-shaped
// ops and syncs as two counters), local here too rather than shared, since no
// crate exports this pattern.

#[derive(Clone, Default)]
struct OpCounts {
    writes: Arc<AtomicU64>,
    syncs: Arc<AtomicU64>,
}

impl OpCounts {
    /// Total disk-touching calls (write-shaped ops plus syncs) — what
    /// distinguishes "no transaction was opened" from "a transaction was
    /// opened and committed but changed nothing user-visible".
    fn total(&self) -> u64 {
        self.writes.load(Ordering::SeqCst) + self.syncs.load(Ordering::SeqCst)
    }
}

struct CountingDisk {
    inner: charpente_sim::SimDisk,
    counts: OpCounts,
}

impl Disk for CountingDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let file = self.inner.open(path)?;
        Ok(Box::new(CountingFile {
            inner: file,
            counts: self.counts.clone(),
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

struct CountingFile {
    inner: Box<dyn File>,
    counts: OpCounts,
}

impl File for CountingFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read_at(offset, out)
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.counts.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.write_at(offset, data)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.counts.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.set_len(len)
    }

    fn sync(&self) -> io::Result<()> {
        self.counts.syncs.fetch_add(1, Ordering::SeqCst);
        self.inner.sync()
    }
}

fn counting_store() -> (Sim, Store, OpCounts) {
    let sim = Sim::with_faults(0x7AB1E, FaultConfig::none());
    let counts = OpCounts::default();
    let disk = CountingDisk {
        inner: sim.disk(),
        counts: counts.clone(),
    };
    let store = Store::open(&disk, "tables.redb").unwrap();
    (sim, store, counts)
}

#[test]
fn typed_roundtrip_and_scan() {
    let (_sim, store) = quiet_store();
    store
        .apply(0, |w| {
            w.put(
                UNSCOPED,
                &Org {
                    id: Id::from_raw(1),
                    name: "acme".into(),
                },
            )?;
            w.put(UNSCOPED, &invoice(10, 1, 5000))?;
            w.put(UNSCOPED, &invoice(11, 1, 200))
        })
        .unwrap();

    let snap = store.snapshot().unwrap();
    let got = snap
        .get::<Invoice>(Id::from_raw(10))
        .unwrap()
        .expect("row exists");
    assert_eq!(got.amount_cents, 5000);
    assert_eq!(got.memo.as_deref(), Some("memo-10"));
    assert!(snap.get::<Invoice>(Id::from_raw(99)).unwrap().is_none());

    let all = snap.scan::<Invoice>().unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].id, Id::from_raw(10), "scan is in id order");
    assert_eq!(snap.scan::<Org>().unwrap().len(), 1);
}

#[test]
fn secondary_index_follows_updates_and_deletes() {
    let (_sim, store) = quiet_store();
    let (acme, globex) = (Id::<Org>::from_raw(1), Id::<Org>::from_raw(2));
    store
        .apply(0, |w| {
            w.put(UNSCOPED, &invoice(10, 1, 100))?;
            w.put(UNSCOPED, &invoice(11, 1, 200))?;
            w.put(UNSCOPED, &invoice(12, 2, 300))
        })
        .unwrap();

    let snap = store.snapshot().unwrap();
    let acme_invoices = Invoice::by_org(&snap, &acme).unwrap();
    assert_eq!(
        acme_invoices.iter().map(|i| i.id.raw()).collect::<Vec<_>>(),
        vec![10, 11]
    );
    assert_eq!(Invoice::by_org(&snap, &globex).unwrap().len(), 1);

    // Move invoice 11 to globex: the acme entry must disappear.
    store
        .apply(1, |w| w.put(UNSCOPED, &invoice(11, 2, 200)))
        .unwrap();
    // Delete invoice 12: its globex entry must disappear.
    store
        .apply(2, |w| w.delete::<Invoice>(UNSCOPED, Id::from_raw(12)))
        .unwrap();

    let snap = store.snapshot().unwrap();
    assert_eq!(
        Invoice::by_org(&snap, &acme)
            .unwrap()
            .iter()
            .map(|i| i.id.raw())
            .collect::<Vec<_>>(),
        vec![10]
    );
    assert_eq!(
        Invoice::by_org(&snap, &globex)
            .unwrap()
            .iter()
            .map(|i| i.id.raw())
            .collect::<Vec<_>>(),
        vec![11]
    );
}

#[test]
fn snapshots_are_mvcc() {
    let (_sim, store) = quiet_store();
    store
        .apply(0, |w| w.put(UNSCOPED, &invoice(1, 1, 100)))
        .unwrap();

    let before = store.snapshot().unwrap();
    store
        .apply(1, |w| w.put(UNSCOPED, &invoice(2, 1, 200)))
        .unwrap();

    assert_eq!(
        before.scan::<Invoice>().unwrap().len(),
        1,
        "old snapshot must not see the new row"
    );
    assert_eq!(
        store.snapshot().unwrap().scan::<Invoice>().unwrap().len(),
        2
    );
}

#[test]
fn applied_seq_tracks_and_must_be_gapless() {
    let (_sim, store) = quiet_store();
    assert_eq!(store.applied_seq().unwrap(), None);
    store
        .apply(0, |w| w.put(UNSCOPED, &invoice(1, 1, 1)))
        .unwrap();
    store
        .apply(1, |w| w.put(UNSCOPED, &invoice(2, 1, 2)))
        .unwrap();
    assert_eq!(store.applied_seq().unwrap(), Some(1));
}

#[test]
#[should_panic(expected = "must be gapless")]
fn applying_out_of_order_is_a_contract_violation() {
    let (_sim, store) = quiet_store();
    store
        .apply(0, |w| w.put(UNSCOPED, &invoice(1, 1, 1)))
        .unwrap();
    let _ = store.apply(5, |w| w.put(UNSCOPED, &invoice(2, 1, 2)));
}

/// The durability model itself: un-checkpointed applies vanish in a crash
/// (by design — the WAL replays them); checkpointed state survives. The
/// crash must land while the database is OPEN: redb's `Database::drop` is a
/// graceful close that durably flushes non-durable commits (discovered when
/// this test originally dropped the store first and seq 2 survived).
#[test]
fn crash_reverts_to_the_last_checkpoint() {
    let sim = Sim::with_faults(0xC4A5, FaultConfig::none());
    let store = Store::open(&sim.disk(), "tables.redb").unwrap();
    store
        .apply(0, |w| w.put(UNSCOPED, &invoice(1, 1, 100)))
        .unwrap();
    store
        .apply(1, |w| w.put(UNSCOPED, &invoice(2, 1, 200)))
        .unwrap();
    store.checkpoint().unwrap();
    store
        .apply(2, |w| w.put(UNSCOPED, &invoice(3, 1, 300)))
        .unwrap(); // never checkpointed

    sim.restart(); // crash with the database open — no graceful close
    drop(store); // its close attempts hit dead handles, harmlessly

    let store = Store::open(&sim.disk(), "tables.redb").unwrap();
    assert_eq!(
        store.applied_seq().unwrap(),
        Some(1),
        "state reverts to the checkpoint"
    );
    let rows = store.snapshot().unwrap().scan::<Invoice>().unwrap();
    assert_eq!(
        rows.len(),
        2,
        "the un-checkpointed apply is gone — the WAL's job to replay"
    );
}

// ---------------------------------------------------- integration property

/// Log records for the property test: a put or delete of a `Kv` row.
#[table(global)]
struct Kv {
    id: Id<Kv>,
    #[index]
    bucket: u8,
    data: Vec<u8>,
}

enum Op {
    Put(Kv),
    Delete(u64),
}

impl Op {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Op::Put(row) => {
                out.push(0);
                charpente_core::table::Row::encode_row(row, &mut out);
            }
            Op::Delete(id) => {
                out.push(1);
                id.encode(&mut out);
            }
        }
        out
    }

    fn decode(payload: &[u8]) -> Op {
        match payload[0] {
            0 => Op::Put(
                <Kv as charpente_core::table::Row>::decode_row(&payload[1..]).expect("valid op"),
            ),
            1 => Op::Delete(u64::decode(&mut &payload[1..]).expect("valid op")),
            other => panic!("unknown op tag {other}"),
        }
    }

    fn apply_to_model(&self, model: &mut BTreeMap<u64, (u8, Vec<u8>)>) {
        match self {
            Op::Put(row) => {
                model.insert(row.id.raw(), (row.bucket, row.data.clone()));
            }
            Op::Delete(id) => {
                model.remove(id);
            }
        }
    }
}

/// Replays every recorded op into an in-memory model — the oracle the store
/// is compared against.
#[derive(Default)]
struct OpLog {
    ops: Vec<Vec<u8>>,
}

impl StateMachine for OpLog {
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()> {
        assert_eq!(seq, self.ops.len() as u64);
        self.ops.push(payload.to_vec());
        Ok(())
    }
}

fn model_of(ops: &[Vec<u8>]) -> BTreeMap<u64, (u8, Vec<u8>)> {
    let mut model = BTreeMap::new();
    for payload in ops {
        Op::decode(payload).apply_to_model(&mut model);
    }
    model
}

fn store_matches_model(
    snap: &Snapshot,
    model: &BTreeMap<u64, (u8, Vec<u8>)>,
) -> Result<(), String> {
    let rows = snap.scan::<Kv>().map_err(|e| format!("scan failed: {e}"))?;
    if rows.len() != model.len() {
        return Err(format!(
            "store has {} rows, model has {}",
            rows.len(),
            model.len()
        ));
    }
    for row in &rows {
        match model.get(&row.id.raw()) {
            Some((bucket, data)) if *bucket == row.bucket && *data == row.data => {}
            Some(_) => return Err(format!("row {} differs from the model", row.id.raw())),
            None => return Err(format!("store invented row {}", row.id.raw())),
        }
    }
    // Index consistency: every bucket lookup agrees with the model.
    for bucket in 0..4u8 {
        let by_index =
            Kv::by_bucket(snap, &bucket).map_err(|e| format!("index scan failed: {e}"))?;
        let expected: Vec<u64> = model
            .iter()
            .filter(|(_, (b, _))| *b == bucket)
            .map(|(id, _)| *id)
            .collect();
        let got: Vec<u64> = by_index.iter().map(|r| r.id.raw()).collect();
        if got != expected {
            return Err(format!(
                "index bucket {bucket}: {got:?} != model {expected:?}"
            ));
        }
    }
    Ok(())
}

/// The full step-4-preview loop: WAL is the truth, the store materializes it,
/// crash anywhere, recovery replays the gap. redb runs over the fault-
/// injecting disk throughout.
fn tables_workload(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    let wal_cfg = WalConfig {
        dir: "wal".to_string(),
        segment_bytes: 4096,
    };
    let mut submitted: Vec<Vec<u8>> = Vec::new();
    let mut acked = 0usize;

    for _round in 0..3 {
        sim.crash_disk_after(64 + entropy.next_u64() % 512);

        // The store's own `applied_seq` is the durable horizon (segment-mgmt
        // T4/T5, mirroring `Db::open`'s production use): opened before
        // recovery so a corrupt frame at-or-below it is a named hard error
        // rather than a silent, torn-tail-style cut. Without this, once
        // `bit_flip`/`misdirected_write` can corrupt already-durable WAL
        // bytes, `Engine::recover(..., None)` could quietly hand back fewer
        // than `acked` ops — indistinguishable from a real durability bug.
        let (engine, store) = match (|| {
            let store = Store::open(&sim.disk(), "tables.redb")?;
            let horizon = store.applied_seq()?;
            let engine = Engine::recover(
                Box::new(sim.disk()),
                wal_cfg.clone(),
                OpLog::default(),
                horizon,
            )?;
            io::Result::Ok((engine, store))
        })() {
            Ok(pair) => pair,
            Err(_) => {
                sim.restart();
                continue;
            }
        };

        // WAL invariants, as ever.
        let mut engine = engine;
        let recovered = engine.machine().ops.clone();
        if recovered.len() < acked {
            // Excused ONLY when the harness actually injected durable
            // corruption in this universe: bit_flip/misdirected_write on a
            // WAL file above the checkpoint horizon is silently cut at
            // recovery, by the WAL convention (see
            // `checks::durable_corruption_fired`); the strong oracle stands
            // in every universe where corruption did not fire.
            if charpente_sim::checks::durable_corruption_fired(sim) {
                return Ok(());
            }
            return Err(format!(
                "durability violated: acked {acked}, recovered {}",
                recovered.len()
            ));
        }
        if recovered[..] != submitted[..recovered.len().min(submitted.len())]
            || recovered.len() > submitted.len()
        {
            return Err("recovered ops are not a prefix of submitted ops".to_string());
        }
        submitted = recovered.clone();
        acked = submitted.len();

        // The store must sit at some prefix of the recovered log…
        let applied = match store.applied_seq() {
            Ok(applied) => applied,
            Err(_) => {
                sim.restart();
                continue;
            }
        };
        let next_to_apply = applied.map_or(0, |s| s + 1) as usize;
        if next_to_apply > recovered.len() {
            return Err(format!(
                "store applied_seq {applied:?} is ahead of the recovered log ({} records) — it must never outrun durable truth",
                recovered.len()
            ));
        }

        // …and replaying the gap must reproduce the model exactly.
        let mut alive = true;
        for (i, payload) in recovered[next_to_apply..].iter().enumerate() {
            let seq = (next_to_apply + i) as u64;
            let op = Op::decode(payload);
            if store
                .apply(seq, |w| match &op {
                    Op::Put(row) => w.put(UNSCOPED, row),
                    Op::Delete(id) => w.delete::<Kv>(UNSCOPED, Id::from_raw(*id)),
                })
                .is_err()
            {
                alive = false;
                break;
            }
        }
        if !alive {
            sim.restart();
            continue;
        }
        match store.snapshot() {
            Ok(snap) => {
                // The scan/index calls inside `store_matches_model` are disk
                // reads too — an injected `read_error` can trip one even
                // though the snapshot itself opened fine, so its own result
                // needs the same classify-and-reopen treatment as every
                // other fallible step here (not the bare `?` this used to
                // be), or a transient read fault reads as a real mismatch.
                if let Err(msg) = store_matches_model(&snap, &model_of(&submitted[..acked])) {
                    if !is_fault_fallout(&msg) {
                        return Err(msg);
                    }
                    sim.restart();
                    continue;
                }
            }
            Err(_) => {
                sim.restart();
                continue;
            }
        }

        // New traffic: submit to the log, then apply to the store.
        for _ in 0..1 + entropy.next_u64() % 8 {
            let op = if entropy.next_u64().is_multiple_of(4) && !model_of(&submitted).is_empty() {
                let keys: Vec<u64> = model_of(&submitted).keys().copied().collect();
                Op::Delete(keys[(entropy.next_u64() as usize) % keys.len()])
            } else {
                let mut data = vec![0u8; 32 + (entropy.next_u64() % 400) as usize];
                entropy.fill(&mut data);
                Op::Put(Kv {
                    id: Id::from_raw(entropy.next_u64() % 64),
                    bucket: (entropy.next_u64() % 4) as u8,
                    data,
                })
            };
            let payload = op.encode();
            submitted.push(payload.clone());
            let seq = match engine.submit(&payload) {
                Ok(seq) => seq,
                Err(_) => break,
            };
            acked += 1;
            if store
                .apply(seq, |w| match &op {
                    Op::Put(row) => w.put(UNSCOPED, row),
                    Op::Delete(id) => w.delete::<Kv>(UNSCOPED, Id::from_raw(*id)),
                })
                .is_err()
            {
                break;
            }
            if entropy.next_u64().is_multiple_of(5) && store.checkpoint().is_err() {
                break;
            }
        }
        sim.restart();
    }
    Ok(())
}

#[test]
fn tables_and_log_survive_the_fault_matrix() {
    run_universes_swarm!(
        256,
        tables_workload,
        require: [
            "fault:write_error",
            "fault:sync_error",
            "fault:crash",
            "crash:applied",
            "crash:torn",
            "crash:dropped",
            "restart",
        ]
    );
}

#[test]
fn tables_replay_identically_for_the_same_seed() {
    let trace = |seed: u64| {
        let mut sim = Sim::new(seed);
        let result = tables_workload(&mut sim);
        assert_eq!(result, Ok(()), "seed {seed:#x}");
        (sim.durable_fingerprint(), sim.now())
    };
    for seed in [0x7, 0xBEEF_CAFE, 0x0F1E_2D3C_4B5A_6978] {
        assert_eq!(trace(seed), trace(seed));
    }
}

#[test]
fn jobs_materialize_with_counter_ids_in_order() {
    let (_sim, store) = quiet_store();
    store
        .apply(0, |w| {
            w.materialize_job("send_receipt", b"invoice-10", None)?;
            w.materialize_job("audit", b"", Some("k:1"))?;
            Ok(())
        })
        .unwrap();
    store
        .apply(1, |w| w.materialize_job("audit", b"x", None).map(|_| ()))
        .unwrap();

    let jobs = store.snapshot().unwrap().scan::<JobRow>().unwrap();
    assert_eq!(
        jobs.iter()
            .map(|j| (
                j.id.raw(),
                j.name.as_str(),
                j.payload.as_slice(),
                j.key.as_deref()
            ))
            .collect::<Vec<_>>(),
        vec![
            (0, "send_receipt", b"invoice-10".as_slice(), None),
            (1, "audit", b"".as_slice(), Some("k:1")),
            (2, "audit", b"x".as_slice(), None),
        ]
    );
}

// -------------------------------------------------------- version + fingerprint

#[charpente::row_shape(version = 1)]
struct GizmoV1 {
    id: u64,
    count: u32,
}

#[charpente::table(version = 2)]
struct Gizmo {
    id: charpente::core::table::Id<Gizmo>,
    count: u32,
    label: String,
}

impl charpente::core::migration::TableMigration for Gizmo {
    type Prev = GizmoV1;
    fn migrate(prev: GizmoV1) -> Result<Gizmo, charpente::core::migration::MigrationError> {
        Ok(Gizmo {
            id: charpente::core::table::Id::from_raw(prev.id),
            count: prev.count,
            label: String::new(),
        })
    }
}

#[test]
fn versioned_table_carries_version_and_fingerprint() {
    use charpente::core::table::Row;
    assert_eq!(Gizmo::VERSION, 2);
    assert!(Gizmo::FINGERPRINT.is_some());
}

#[test]
fn bare_table_stays_version_one_and_now_fingerprinted() {
    use charpente::core::table::Row;
    assert_eq!(Org::VERSION, 1);
    assert!(Org::FINGERPRINT.is_some());
}

// -------------------------------------------------------- scoped_by (Task 3, tenant scoping)

#[charpente::table(scoped_by = Org)]
struct ScopedWidget {
    id: charpente::core::table::Id<ScopedWidget>,
    org: charpente::core::table::Id<Org>,
}

/// `#[table(scoped_by = Org)]` requires this — the app's own membership
/// check (crates/charpente/src/tenant.rs). Trivially `Ok(())`: this file
/// exercises the type-level wiring only; the refusing case lives in
/// `charpente/tests/tenant.rs`.
impl charpente::Tenant for Org {
    type Who = charpente::auth::Principal;
    type Error = charpente::http::ApiError;

    fn authorize(
        _tx: &charpente::Tx,
        _who: Option<&charpente::auth::Principal>,
        _scope: charpente::core::table::Id<Org>,
    ) -> Result<(), charpente::http::ApiError> {
        Ok(())
    }
}

/// Oracle for the macro's `Scope` emission (Task 2 review finding): Task 2's
/// only coverage was a hand-written `Row` impl (`JobRow`), so the macro
/// could emit `type Scope = ();` today and every test would still pass.
/// This pins BOTH branches against real `#[table]`-generated types — an
/// ordinary table stays `Unscoped`, a `scoped_by = Org` one gets `Org` —
/// and `scope_is`'s bound only type-checks if `Scope` is EXACTLY `S`, so it
/// fails to compile, not silently passes, if the macro emits anything else.
#[test]
fn table_macro_emits_the_declared_scope_type() {
    use charpente::core::table::{Row, Unscoped};
    fn scope_is<R: Row<Scope = S>, S>() {}
    scope_is::<Org, Unscoped>();
    scope_is::<Invoice, Unscoped>();
    scope_is::<ScopedWidget, Org>();
}

/// Task 4 (tenant scoping): the composite-key wiring through the REAL
/// `#[table(scoped_by = …)]` macro output, not just a hand-written stand-in
/// (`charpente-core/tests/engine.rs` has one of those too, since `core`
/// cannot depend on `charpente` to use the macro itself). Same shape as that
/// test: a scoped put/delete must not error, and the resulting physical
/// table diverges from the bare-`u64`-keyed shape `scan_raw` tries FIRST —
/// through Task 6, that meant a `[u8;16]` key-type mismatch; Task 6c teaches
/// `scan_raw` the same composite-key fallback `scan_raw_from` already had,
/// so this now proves the divergence by the SUCCESSFUL read carrying the
/// real `(scope, id)` rather than by the mismatch, and that the row is gone
/// (not merely unscanned) once deleted.
#[test]
fn scoped_by_macro_output_stores_under_the_composite_key() {
    use charpente::core::table::Row;

    let (_sim, store) = quiet_store();
    store
        .apply(0, |w| {
            w.put(
                7,
                &ScopedWidget {
                    id: Id::from_raw(1),
                    org: Id::from_raw(9),
                },
            )
        })
        .expect("a scoped put through the macro-generated Row impl must not error");

    let snap = store.snapshot().unwrap();
    let rows = snap
        .scan_raw(ScopedWidget::TABLE, true)
        .expect("scan_raw must learn the [u8;16] key shape rather than erroring");
    let scopes_and_ids: Vec<(u64, u64)> = rows.iter().map(|(s, id, _)| (*s, *id)).collect();
    assert_eq!(
        scopes_and_ids,
        vec![(7, 1)],
        "scan_raw must recover the real (scope, id) from the composite key the macro-generated \
         Row produces, not UNSCOPED"
    );

    store
        .apply(1, |w| w.delete::<ScopedWidget>(7, Id::from_raw(1)))
        .expect("a scoped delete through the macro-generated Row impl must not error");
    let snap = store.snapshot().unwrap();
    let rows = snap.scan_raw(ScopedWidget::TABLE, true).unwrap();
    assert!(
        rows.is_empty(),
        "the deleted row must not survive the scan: {rows:?}"
    );
}

#[test]
fn row_shape_roundtrips_under_its_own_codec() {
    use charpente::core::table::RowShape;
    let mut bytes = Vec::new();
    GizmoV1 { id: 4, count: 9 }.encode_row(&mut bytes);
    let back = GizmoV1::decode_row(&bytes).unwrap();
    assert_eq!((back.id, back.count), (4, 9));
}

/// A batch is one commit point (MANIFESTO §5): a failure anywhere in it must
/// leave the store exactly where it started, not partway through.
#[test]
fn apply_batch_rolls_back_every_record_when_one_fails() {
    let (_sim, store) = quiet_store();
    store
        .apply(0, |w| w.put(UNSCOPED, &invoice(1, 1, 100)))
        .unwrap();

    let records: Vec<(u64, &[u8])> = vec![(1, b"a"), (2, b"b"), (3, b"c")];
    let err = store
        .apply_batch(&records, |w, seq, _payload| {
            if seq == 3 {
                return Err(io::Error::other("the third record refuses"));
            }
            w.put(UNSCOPED, &invoice(seq + 10, 1, 500))
        })
        .expect_err("the third record's failure must fail the whole batch");
    assert_eq!(err.to_string(), "the third record refuses");

    assert_eq!(
        store.applied_seq().unwrap(),
        Some(0),
        "a failed batch must not move applied_seq — the store is a materialization \
         of the log, and a partly-applied batch corresponds to no single commit point"
    );
    let snap = store.snapshot().unwrap();
    assert_eq!(
        snap.scan::<Invoice>().unwrap().len(),
        1,
        "records 1 and 2 succeeded inside the txn but must roll back with record 3"
    );
}

/// The happy path: every record lands and applied_seq ends at the last seq.
#[test]
fn apply_batch_commits_every_record_and_ends_at_the_last_seq() {
    let (_sim, store) = quiet_store();

    let records: Vec<(u64, &[u8])> = vec![(0, b"a"), (1, b"b"), (2, b"c")];
    store
        .apply_batch(&records, |w, seq, _payload| {
            w.put(UNSCOPED, &invoice(seq + 1, 1, 100))
        })
        .expect("all three records apply");

    assert_eq!(store.applied_seq().unwrap(), Some(2));
    assert_eq!(
        store.snapshot().unwrap().scan::<Invoice>().unwrap().len(),
        3
    );
}

/// Gapless is preserved, not traded away: it used to cost a read txn per
/// record, and now costs one read txn plus a running counter. Both shapes of
/// violation must still fire — a wrong first seq, and a gap mid-batch.
#[test]
#[should_panic(expected = "apply must be gapless")]
fn apply_batch_rejects_a_wrong_first_seq() {
    let (_sim, store) = quiet_store();
    let records: Vec<(u64, &[u8])> = vec![(7, b"a")];
    let _ = store.apply_batch(&records, |w, _seq, _p| w.put(UNSCOPED, &invoice(1, 1, 1)));
}

#[test]
#[should_panic(expected = "apply must be gapless")]
fn apply_batch_rejects_a_gap_in_the_middle_of_a_batch() {
    let (_sim, store) = quiet_store();
    let records: Vec<(u64, &[u8])> = vec![(0, b"a"), (1, b"b"), (3, b"c")];
    let _ = store.apply_batch(&records, |w, seq, _p| {
        w.put(UNSCOPED, &invoice(seq + 1, 1, 1))
    });
}

/// Spec §3's new invariant, from the reader's side: a read taken through the
/// Writer inside a batch sees the batch's OWN writes, because it reads through
/// the batch's transaction. This is why in-closure reads are correct by
/// construction and need no assertion — and this test is what makes that
/// claim falsifiable rather than asserted in a comment.
#[test]
fn a_read_through_the_writer_inside_a_batch_sees_the_batch_so_far() {
    let (_sim, store) = quiet_store();
    let records: Vec<(u64, &[u8])> = vec![(0, b"a"), (1, b"b")];
    let mut seen = Vec::new();
    store
        .apply_batch(&records, |w, seq, _payload| {
            seen.push(w.applied_seq()?);
            w.put(UNSCOPED, &invoice(seq + 1, 1, 100))
        })
        .expect("both records apply");

    assert_eq!(
        seen,
        vec![None, Some(0)],
        "the second record must observe the first record's applied_seq bump — a \
         fresh read txn opened mid-batch would have shown None twice"
    );
}

/// An empty batch must cost nothing: no read txn, no write txn, no commit.
/// `applied_seq() == None` alone would be vacuous here — a write transaction
/// opened and committed with an empty loop body also leaves `applied_seq` at
/// `None`, so that assertion cannot tell "never touched disk" from "touched
/// disk and did nothing". Counting disk ops is what makes the "cost nothing"
/// claim in this comment falsifiable rather than merely stated.
#[test]
fn apply_batch_of_nothing_is_a_no_op() {
    let (_sim, store, counts) = counting_store();
    let before = counts.total();
    store
        .apply_batch(&[], |w, _s, _p| w.put(UNSCOPED, &invoice(1, 1, 1)))
        .expect("an empty batch is a no-op");
    let after = counts.total();
    assert_eq!(store.applied_seq().unwrap(), None);
    assert_eq!(
        after, before,
        "an empty batch must not touch disk at all — no read txn to check applied_seq, \
         no write txn, no commit"
    );
}
