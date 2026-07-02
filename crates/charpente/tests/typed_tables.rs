//! Typed tables (PLAN.md M0 step 3): the `#[table]` macro, the store's
//! durability model (Durability::None + checkpoint, WAL replay in between),
//! MVCC snapshots, and secondary indexes — first as surgical unit tests, then
//! as a full log+tables integration property under the fault matrix. This is
//! also redb's audition: its own crash recovery runs under our torn-write
//! injection here.

use std::collections::BTreeMap;
use std::io;

use charpente::table;
use charpente_core::engine::{Engine, StateMachine};
use charpente_core::io::Entropy;
use charpente_core::log::WalConfig;
use charpente_core::table::{Codec, Id, Snapshot, Store, View};
use charpente_sim::{FaultConfig, Sim, run_universes};

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

#[test]
fn typed_roundtrip_and_scan() {
    let (_sim, store) = quiet_store();
    store
        .apply(0, |w| {
            w.put(&Org {
                id: Id::from_raw(1),
                name: "acme".into(),
            })?;
            w.put(&invoice(10, 1, 5000))?;
            w.put(&invoice(11, 1, 200))
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
            w.put(&invoice(10, 1, 100))?;
            w.put(&invoice(11, 1, 200))?;
            w.put(&invoice(12, 2, 300))
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
    store.apply(1, |w| w.put(&invoice(11, 2, 200))).unwrap();
    // Delete invoice 12: its globex entry must disappear.
    store
        .apply(2, |w| w.delete::<Invoice>(Id::from_raw(12)))
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
    store.apply(0, |w| w.put(&invoice(1, 1, 100))).unwrap();

    let before = store.snapshot().unwrap();
    store.apply(1, |w| w.put(&invoice(2, 1, 200))).unwrap();

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
    store.apply(0, |w| w.put(&invoice(1, 1, 1))).unwrap();
    store.apply(1, |w| w.put(&invoice(2, 1, 2))).unwrap();
    assert_eq!(store.applied_seq().unwrap(), Some(1));
}

#[test]
#[should_panic(expected = "must be gapless")]
fn applying_out_of_order_is_a_contract_violation() {
    let (_sim, store) = quiet_store();
    store.apply(0, |w| w.put(&invoice(1, 1, 1))).unwrap();
    let _ = store.apply(5, |w| w.put(&invoice(2, 1, 2)));
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
    store.apply(0, |w| w.put(&invoice(1, 1, 100))).unwrap();
    store.apply(1, |w| w.put(&invoice(2, 1, 200))).unwrap();
    store.checkpoint().unwrap();
    store.apply(2, |w| w.put(&invoice(3, 1, 300))).unwrap(); // never checkpointed

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
#[table]
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

        let (engine, store) = match (|| {
            let engine = Engine::recover(Box::new(sim.disk()), wal_cfg.clone(), OpLog::default())?;
            let store = Store::open(&sim.disk(), "tables.redb")?;
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
                    Op::Put(row) => w.put(row),
                    Op::Delete(id) => w.delete::<Kv>(Id::from_raw(*id)),
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
            Ok(snap) => store_matches_model(&snap, &model_of(&submitted[..acked]))?,
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
                    Op::Put(row) => w.put(row),
                    Op::Delete(id) => w.delete::<Kv>(Id::from_raw(*id)),
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
    run_universes!(256, tables_workload);
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
fn jobs_table_records_in_order() {
    let (_sim, store) = quiet_store();
    store
        .apply(0, |w| {
            w.enqueue_raw(0, 0, "send_receipt", b"invoice-10")?;
            w.enqueue_raw(0, 1, "audit", b"")
        })
        .unwrap();
    store
        .apply(1, |w| w.enqueue_raw(1, 0, "audit", b"x"))
        .unwrap();

    let jobs = store.snapshot().unwrap().jobs().unwrap();
    assert_eq!(
        jobs,
        vec![
            (0, 0, "send_receipt".to_string(), b"invoice-10".to_vec()),
            (0, 1, "audit".to_string(), Vec::new()),
            (1, 0, "audit".to_string(), b"x".to_vec()),
        ]
    );
}
