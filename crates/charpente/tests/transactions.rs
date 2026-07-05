//! Typed transactions (PLAN.md M0 step 4): read-your-own-writes, atomic
//! effects records, abort semantics, recovery replay, and the contract
//! violations — unit tests first; the crash-property suite follows in the
//! same file (added by the next plan task).

use std::collections::BTreeMap;
use std::io;

use charpente::{Db, DbConfig, Event, Job, Schema, TxError, View, table};
use charpente_core::io::Entropy;
use charpente_core::job::JobRow;
use charpente_core::log::{Wal, WalConfig};
use charpente_core::table::{Codec, Id, Snapshot, Store};
use charpente_sim::{FaultConfig, Sim, run_universes_swarm};

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
        snap.scan::<JobRow>().unwrap().is_empty(),
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
    let jobs = snap.scan::<JobRow>().unwrap();
    assert_eq!(jobs.len(), 1, "replay rebuilt the job with the row");
    let job = &jobs[0];
    assert_eq!((job.id.raw(), job.name.as_str()), (0, "send_receipt"));
    assert_eq!(
        SendReceipt::decode(&mut job.payload.as_slice())
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

struct Ping {
    i: u64,
}

impl Codec for Ping {
    fn encode(&self, out: &mut Vec<u8>) {
        self.i.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<Ping> {
        Ok(Ping {
            i: u64::decode(input)?,
        })
    }
}

impl Job for Ping {
    const NAME: &'static str = "ping";
}

#[test]
#[should_panic(expected = "cannot be empty")]
fn empty_idempotency_key_is_a_contract_violation() {
    let sim = Sim::with_faults(0x601, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    let _ = db.transact(|tx| {
        tx.enqueue_keyed("", Ping { i: 0 });
        Ok::<(), std::io::Error>(())
    });
}

// ---------------------------------------------------- crash property

#[table]
struct Account {
    id: Id<Account>,
    #[index]
    bucket: u8,
    balance: i64,
}

#[table]
struct EntryRow {
    id: Id<EntryRow>,
    account: Id<Account>,
    amount: i64,
}

struct Audit {
    txn: u64,
}

impl Codec for Audit {
    fn encode(&self, out: &mut Vec<u8>) {
        self.txn.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<Audit> {
        Ok(Audit {
            txn: u64::decode(input)?,
        })
    }
}

impl Job for Audit {
    const NAME: &'static str = "audit";
}

fn property_schema() -> Schema {
    Schema::new().table::<Account>().table::<EntryRow>()
}

/// Transaction number `i`, derived from `i` alone so the oracle can
/// reconstruct any committed prefix: an account balance bump (read-modify-
/// write), a ledger entry keyed by `i`, sometimes a delete of an older
/// entry, and always exactly one audit job carrying `i`.
fn txn_shape(i: u64) -> (u64, i64, Option<u64>) {
    let account = i % 8;
    let amount = ((i as i64) * 7) % 1000;
    let delete_entry = if i > 0 && i.is_multiple_of(3) {
        Some(i / 2)
    } else {
        None
    };
    (account, amount, delete_entry)
}

fn apply_txn(tx: &mut charpente::Tx, i: u64) -> io::Result<()> {
    let (account, amount, delete_entry) = txn_shape(i);
    let balance = tx
        .get::<Account>(Id::from_raw(account))?
        .map_or(0, |a| a.balance);
    tx.put(&Account {
        id: Id::from_raw(account),
        bucket: (account % 4) as u8,
        balance: balance + amount,
    });
    tx.put(&EntryRow {
        id: Id::from_raw(i),
        account: Id::from_raw(account),
        amount,
    });
    if let Some(e) = delete_entry {
        tx.delete::<EntryRow>(Id::from_raw(e));
    }
    tx.enqueue(Audit { txn: i });
    Ok(())
}

struct Model {
    accounts: BTreeMap<u64, i64>,
    entries: BTreeMap<u64, u64>, // entry id -> account id
}

fn model_after(n: u64) -> Model {
    let mut model = Model {
        accounts: BTreeMap::new(),
        entries: BTreeMap::new(),
    };
    for i in 0..n {
        let (account, amount, delete_entry) = txn_shape(i);
        *model.accounts.entry(account).or_insert(0) += amount;
        model.entries.insert(i, account);
        if let Some(e) = delete_entry {
            model.entries.remove(&e);
        }
    }
    model
}

fn db_matches_model(snap: &Snapshot, n: u64) -> Result<(), String> {
    let model = model_after(n);

    let accounts = snap.scan::<Account>().map_err(|e| format!("scan: {e}"))?;
    let got: BTreeMap<u64, i64> = accounts.iter().map(|a| (a.id.raw(), a.balance)).collect();
    if got != model.accounts {
        return Err(format!(
            "accounts diverge from the model after {n} txns: {got:?} != {:?}",
            model.accounts
        ));
    }

    let entries = snap.scan::<EntryRow>().map_err(|e| format!("scan: {e}"))?;
    let got: BTreeMap<u64, u64> = entries
        .iter()
        .map(|e| (e.id.raw(), e.account.raw()))
        .collect();
    if got != model.entries {
        return Err(format!(
            "entries diverge from the model after {n} txns: {got:?} != {:?}",
            model.entries
        ));
    }

    for bucket in 0..4u8 {
        let by_index = Account::by_bucket(snap, &bucket).map_err(|e| format!("index: {e}"))?;
        let got: Vec<u64> = by_index.iter().map(|a| a.id.raw()).collect();
        let expected: Vec<u64> = model
            .accounts
            .keys()
            .copied()
            .filter(|a| (a % 4) as u8 == bucket)
            .collect();
        if got != expected {
            return Err(format!(
                "index bucket {bucket}: {got:?} != model {expected:?}"
            ));
        }
    }

    // The dual-write invariant: exactly one audit job per committed txn,
    // never a state change without its job or a job without its state.
    let jobs = snap.scan::<JobRow>().map_err(|e| format!("jobs: {e}"))?;
    if jobs.len() as u64 != n {
        return Err(format!(
            "dual-write torn: {n} committed txns but {} jobs",
            jobs.len()
        ));
    }
    for (i, job) in jobs.iter().enumerate() {
        let audit =
            Audit::decode(&mut job.payload.as_slice()).map_err(|e| format!("job decode: {e}"))?;
        if (job.id.raw(), job.name.as_str(), audit.txn) != (i as u64, "audit", i as u64) {
            return Err(format!(
                "job {i} diverges: id {} {} txn {}",
                job.id.raw(),
                job.name,
                audit.txn
            ));
        }
    }
    Ok(())
}

fn is_injected(text: &str) -> bool {
    text.contains("simulated") || text.contains("injected")
}

/// One universe: recover → verify against the oracle → mixed commits and
/// aborts → random checkpoint → crash, everything seeded.
fn tx_workload(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    let cfg = DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 4096,
        },
        store_path: "tables.redb".to_string(),
    };
    let mut committed = 0u64; // acknowledged transactions

    for _round in 0..3 {
        sim.crash_disk_after(64 + entropy.next_u64() % 512);
        let mut db = match Db::open(Box::new(sim.disk()), cfg.clone(), property_schema()) {
            Ok(db) => db,
            Err(_) => {
                sim.restart();
                continue;
            }
        };

        let recovered = db.last_applied().map_or(0, |s| s + 1);
        if recovered < committed {
            return Err(format!(
                "durability violated: {committed} txns acknowledged, {recovered} recovered"
            ));
        }
        committed = recovered; // unacked survivors are history now
        match db.snapshot() {
            Ok(snap) => db_matches_model(&snap, committed)?,
            Err(_) => {
                sim.restart();
                continue;
            }
        }

        for _ in 0..1 + entropy.next_u64() % 8 {
            let i = committed;
            if entropy.next_u64().is_multiple_of(5) {
                // An abort must leave no trace and consume no seq.
                let before = db.last_applied();
                match db.transact(|tx| -> Result<(), &str> {
                    let _ = apply_txn(tx, i);
                    Err("aborted on purpose")
                }) {
                    Err(TxError::App(_)) => {}
                    Err(TxError::Io(_)) => break, // snapshot failed: died here
                    Ok(()) => unreachable!("the closure always aborts"),
                }
                if db.last_applied() != before {
                    return Err("aborted transaction consumed a seq".to_string());
                }
            }
            match db.transact(|tx| apply_txn(tx, i)) {
                Ok(()) => committed += 1,
                // a tx read hit an injected fault; nothing was written
                // (found by seed 0x5e713efe41f78d1a)
                Err(TxError::App(_)) => break,
                Err(TxError::Io(_)) => break, // died mid-commit; durable or not
            }
            if entropy.next_u64().is_multiple_of(4)
                && let Err(e) = db.checkpoint()
            {
                if !is_injected(&e.to_string()) {
                    return Err(format!("non-injected checkpoint failure: {e}"));
                }
                break;
            }
        }
        sim.restart();
    }

    // Final verification: the last round's commits get checked too. Db::open
    // replays the whole un-checkpointed gap through the store, so unlike the
    // WAL's read-only final recovery it needs a long fault-free streak of
    // writes; under persistent injected faults it may legitimately never
    // succeed (seed 0x4cea828a65f0f1eb). Injected-fault failures skip the
    // final check — the oracle still runs at every successful open across
    // the universe set — but any other open error is a real bug.
    for _attempt in 0..8 {
        let db = match Db::open(Box::new(sim.disk()), cfg.clone(), property_schema()) {
            Ok(db) => db,
            Err(e) => {
                if is_injected(&e.to_string()) {
                    sim.restart();
                    continue;
                }
                return Err(format!(
                    "final recovery failed with a non-injected error: {e}"
                ));
            }
        };
        let recovered = db.last_applied().map_or(0, |s| s + 1);
        if recovered < committed {
            return Err(format!(
                "durability violated: {committed} txns acknowledged, {recovered} recovered"
            ));
        }
        let snap = match db.snapshot() {
            Ok(snap) => snap,
            Err(_) => {
                sim.restart();
                continue;
            }
        };
        return db_matches_model(&snap, recovered);
    }
    Ok(()) // every attempt died to injected faults; earlier rounds verified every reachable prefix
}

#[test]
fn transactions_survive_the_fault_matrix() {
    run_universes_swarm!(
        256,
        tx_workload,
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
fn transactions_replay_identically_for_the_same_seed() {
    let trace = |seed: u64| {
        let mut sim = Sim::new(seed);
        let result = tx_workload(&mut sim);
        assert_eq!(result, Ok(()), "seed {seed:#x}");
        (sim.durable_fingerprint(), sim.now())
    };
    for seed in [0x42, 0xDEAD_BEEF, 0x0BAD_5EED_0BAD_5EED] {
        assert_eq!(trace(seed), trace(seed));
    }
}

// ---------------------------------------------------- negative space

/// The classic dual-write bug, reconstructed on purpose: apply to the store
/// and checkpoint BEFORE the WAL record is synced. The harness must catch
/// the store outrunning durable truth — if it ever stops catching it, the
/// crash injection has regressed (same contract as crash_recovery.rs's
/// harness_catches_naive_recovery).
fn broken_ordering_workload(sim: &mut Sim) -> Result<(), String> {
    let entropy = sim.entropy();
    let cfg = WalConfig {
        dir: "wal".to_string(),
        segment_bytes: 4096,
    };

    for _round in 0..3 {
        sim.crash_disk_after(32 + entropy.next_u64() % 256);
        let opened = (|| {
            let mut durable = 0u64;
            let wal = Wal::open(Box::new(sim.disk()), cfg.clone(), |_, _| {
                durable += 1;
                Ok(())
            })?;
            let store = Store::open(&sim.disk(), "tables.redb")?;
            io::Result::Ok((wal, store, durable))
        })();
        let (mut wal, store, durable) = match opened {
            Ok(t) => t,
            Err(_) => {
                sim.restart();
                continue;
            }
        };
        let applied = match store.applied_seq() {
            Ok(a) => a,
            Err(_) => {
                sim.restart();
                continue;
            }
        };

        // THE invariant this workload exists to violate:
        let next = applied.map_or(0, |s| s + 1);
        if next > durable {
            return Err(format!(
                "store applied_seq {applied:?} outran the durable log ({durable} records) — \
                 phantom state a conventional dual-write stack would serve as truth"
            ));
        }

        // Catch the store up to durable truth (content-free: only the seq
        // bump matters to this workload).
        let mut alive = true;
        for s in next..durable {
            if store.apply(s, |_| Ok(())).is_err() {
                alive = false;
                break;
            }
        }
        if !alive {
            sim.restart();
            continue;
        }

        // New traffic, in the BROKEN order: append → apply → checkpoint →
        // (only now) sync. A crash in the window makes the store a liar.
        let rounds = 1 + entropy.next_u64() % 6;
        for seq in durable..durable + rounds {
            if wal.append(&[0xAB; 64]).is_err() {
                break;
            }
            if store.apply(seq, |_| Ok(())).is_err() {
                break;
            }
            if store.checkpoint().is_err() {
                break;
            }
            if wal.sync().is_err() {
                break;
            }
        }
        sim.restart();
    }
    Ok(())
}

/// Fixed base so CI is deterministic (the positive property uses fresh
/// seeds every run).
const META_TEST_BASE: u64 = 0x7C4A_2026_0004;

#[test]
fn harness_catches_apply_before_sync() {
    let mut state = META_TEST_BASE;
    let mut caught = None;
    for _ in 0..400 {
        let seed = charpente_sim::rng::splitmix64(&mut state);
        let mut sim = Sim::new(seed);
        if let Err(msg) = broken_ordering_workload(&mut sim) {
            caught = Some((seed, msg));
            break;
        }
    }
    let (seed, msg) = caught.expect(
        "the harness no longer catches store-apply-before-WAL-sync — \
         the crash injection itself has regressed",
    );
    eprintln!("caught broken ordering: seed {seed:#018x}: {msg}");

    // The seed IS the reproduction.
    let mut replay = Sim::new(seed);
    assert_eq!(
        broken_ordering_workload(&mut replay),
        Err(msg),
        "replaying the seed must reproduce the identical failure"
    );
}

/// Hard rule 3: a crash during initial store creation left durable garbage
/// redb rejects on every open (bad magic, InvalidData) — without the
/// rebuild-from-WAL path in Db::open this universe bricks permanently.
#[test]
fn regression_store_bricked_by_creation_crash() {
    let mut sim = Sim::new(0x1f733f8d9eebf822);
    assert_eq!(
        tx_workload(&mut sim),
        Ok(()),
        "seed 0x1f733f8d9eebf822 no longer recovers a store bricked at creation"
    );
}
