//! Typed transactions (PLAN.md M0 step 4): read-your-own-writes, atomic
//! effects records, abort semantics, recovery replay, and the contract
//! violations — unit tests first; the crash-property suite follows in the
//! same file (added by the next plan task).

use std::collections::BTreeMap;
use std::io;

use charpente::{Db, DbConfig, Event, Job, Schema, Seeder, TxError, View, table};
use charpente_core::event::EventRow;
use charpente_core::io::{Disk, Entropy};
use charpente_core::job::JobRow;
use charpente_core::log::{Wal, WalConfig};
use charpente_core::table::{Codec, Id, Row, Snapshot, Store};
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

/// Mirrors what `Db::open` registers internally (it always adds `JobRow`
/// and `EventRow` itself) — the export side needs its own independent
/// `Schema` instance to walk `table_names()` over, since a live `Db` never
/// hands its schema back.
fn export_schema() -> Schema {
    app_schema().table::<JobRow>().table::<EventRow>()
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
    // Only the sim's own fault wordings — any Io error (from transact's
    // commit, or from checkpoint's own gc, which can now poison the WAL too)
    // breaks and restarts the universe immediately, before a second op could
    // ever reach an already-poisoned/latched handle, so this workload never
    // sees the WAL/engine's "poisoned" guard text or redb's "close and
    // re-open the database" latch — unlike kernel.rs's SUT, which absorbs
    // request Io into 500s and keeps running. The WAL's durable-horizon hard
    // error (segment-mgmt T4/T5) is recognized too: bit_flip/
    // misdirected_write corrupting an already-acknowledged frame below the
    // checkpoint horizon is a legitimate fault-matrix consequence surfaced
    // loudly by design, not a bug (and it is permanent — a universe that
    // hits it on recovery retries into the every-attempt-died fallthrough).
    text.contains("simulated") || text.contains("injected") || text.contains("durable horizon")
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
        ..DbConfig::default()
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
            // Excused ONLY when the harness actually injected durable
            // corruption in this universe: bit_flip/misdirected_write on a
            // WAL file above the checkpoint horizon is silently cut at
            // recovery, by the WAL convention (see
            // `checks::durable_corruption_fired`); ending the universe here
            // keeps the strong oracle standing everywhere corruption did
            // not fire.
            if charpente_sim::checks::durable_corruption_fired(sim) {
                return Ok(());
            }
            return Err(format!(
                "durability violated: {committed} txns acknowledged, {recovered} recovered"
            ));
        }
        committed = recovered; // unacked survivors are history now
        match db.snapshot() {
            Ok(snap) => {
                // `db_matches_model`'s own scan/index calls are disk reads
                // too — an injected `read_error` can trip one even though
                // the snapshot itself opened fine, so its result needs the
                // same classify-and-reopen treatment as everywhere else
                // (not the bare `?` this used to be).
                if let Err(msg) = db_matches_model(&snap, committed) {
                    if !is_injected(&msg) {
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
                    Err(TxError::NotPrimary { .. }) => {
                        unreachable!("this workload never calls forbid_writes")
                    }
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
                Err(TxError::NotPrimary { .. }) => {
                    unreachable!("this workload never calls forbid_writes")
                }
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
            // Same excuse-only-if-injected routing as the round loop above.
            if charpente_sim::checks::durable_corruption_fired(sim) {
                return Ok(());
            }
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
        return match db_matches_model(&snap, recovered) {
            Ok(()) => Ok(()),
            Err(msg) if is_injected(&msg) => {
                sim.restart();
                continue;
            }
            Err(msg) => Err(msg),
        };
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

/// Step 7 (spec §Ruled 3): the kernel needs to ask "is this engine dead?"
/// after a request-path Io 500. Poison is already tracked (fsyncgate);
/// this pins the read-only surface that exposes it.
///
/// Deviation from the task brief: the brief's literal test set
/// `sync_error: 1.0` on the `Sim` up front, but `FaultConfig` is fixed for a
/// `Sim`'s whole lifetime, and redb's store creation inside `Db::open`
/// itself syncs — so that config fails `Db::open` before the test ever
/// reaches `transact`. `crash_disk_now` (used elsewhere in this file, e.g.
/// `broken_ordering_workload`) lets the open succeed and then guarantees
/// the very next disk op fails, which is what actually exercises the
/// poison path.
#[test]
fn poisoned_reports_wal_poison_after_io_failure() {
    let sim = Sim::with_faults(0x9015_0DED, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), app_schema()).unwrap();
    assert!(!db.poisoned(), "a fresh db must not be poisoned");
    sim.crash_disk_now();
    let err = db
        .transact(|tx| {
            tx.put(&invoice(1, 1, 1));
            Ok::<_, io::Error>(())
        })
        .unwrap_err();
    assert!(
        matches!(err, TxError::Io(_)),
        "a crashed disk must fail the commit"
    );
    assert!(
        db.poisoned(),
        "an Io commit failure must leave the db observably poisoned"
    );
}

/// T1 followup: the WAL-poison test above never reaches the engine's OWN
/// poison bit (engine.rs's "a durable record failed to apply" guard) — a
/// crashed disk fails the WAL's own append/sync first. This test opens with
/// Invoice missing from the schema on purpose: the WAL append and sync for
/// an Invoice put both succeed (no disk fault at all), but
/// `TableMachine::apply`'s schema lookup fails afterward — a post-sync
/// apply failure, which is the engine's own bit, not the WAL's.
#[test]
fn poisoned_reports_the_engines_own_bit_after_a_post_sync_apply_failure() {
    let sim = Sim::with_faults(0x9015_0DEE, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Org>(), // Invoice missing on purpose
    )
    .unwrap();
    assert!(!db.poisoned());

    let err = db
        .transact(|tx| {
            tx.put(&invoice(1, 1, 1));
            Ok::<_, io::Error>(())
        })
        .unwrap_err();
    assert!(matches!(err, TxError::Io(_)));
    assert!(
        db.poisoned(),
        "a post-sync apply failure must poison the db"
    );

    // A second transaction never touches disk: `Engine::submit`'s own
    // poisoned-flag check fires first. Its distinct wording proves this run
    // hit the engine's OWN bit, not the WAL's fsyncgate poison.
    let err = db
        .transact(|tx| {
            tx.put(&Org {
                id: Id::from_raw(2),
                name: "x".into(),
            });
            Ok::<_, io::Error>(())
        })
        .unwrap_err();
    assert!(
        matches!(&err, TxError::Io(e) if e.to_string().contains("a durable record failed to apply")),
        "{err:?}"
    );
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

/// Builds a committed, gracefully-closed store, then durably resizes its file
/// to `resize(old_len)` — the crash-collapse states where the store file's
/// length disagrees with its durable header's layout (an unsynced `set_len`
/// drops while the header god-page write survives, or a torn tail write
/// extends the file past the layout). redb 4.1.0 panics at open on both
/// (page_manager.rs:231 / :257) instead of returning Err; Store::open must
/// convert that into an open failure so Db::open discards and rebuilds from
/// the WAL. Found by the kernel flagship swarm, seed 0x64622c74f8660a09.
fn mangled_store_len_recovers(resize: impl Fn(u64) -> u64) {
    let (sim, mut db) = quiet_db(0x0DD5_12E5);
    db.transact(|tx| {
        tx.put(&Org {
            id: Id::from_raw(1),
            name: "acme".into(),
        });
        tx.put(&invoice(10, 1, 5000));
        Ok::<_, io::Error>(())
    })
    .unwrap();
    // Graceful close: redb durably flushes, so header layout == file length.
    drop(db);

    let file = sim.disk().open("tables.redb").unwrap();
    let len = file.len().unwrap();
    file.set_len(resize(len)).unwrap();
    file.sync().unwrap();
    drop(file);

    let db = Db::open(Box::new(sim.disk()), DbConfig::default(), app_schema()).unwrap();
    let snap = db.snapshot().unwrap();
    assert_eq!(
        ids(&snap.scan::<Invoice>().unwrap()),
        vec![10],
        "rebuilt store must replay the full WAL"
    );
    assert_eq!(
        snap.scan::<Org>().unwrap().len(),
        1,
        "rebuilt store must replay the full WAL"
    );
}

/// File shorter than the header's layout: redb 4.1.0 asserts at
/// page_manager.rs:231 before its own repair path can run.
#[test]
fn store_shorter_than_header_layout_is_discarded_and_rebuilt() {
    mangled_store_len_recovers(|len| len - 4096);
}

/// File longer than the header's layout but not page-aligned: redb 4.1.0
/// recalculates the layout, then asserts exact equality at page_manager.rs:257.
#[test]
fn store_longer_than_header_layout_is_discarded_and_rebuilt() {
    mangled_store_len_recovers(|len| len + 100);
}

// ---------------------------------------------------- segment-mgmt T3: GC

/// Segment names are `<dir>/<base_seq:020>.log` (log.rs module docs); parsed
/// here rather than via the crate's private `parse_segment_name` (which
/// integration tests cannot see) to check GC's effect from the outside.
fn segment_base(name: &str) -> u64 {
    name.strip_prefix("wal/")
        .and_then(|s| s.strip_suffix(".log"))
        .expect("segment name shape")
        .parse()
        .expect("segment base is numeric")
}

fn tiny_segments_cfg() -> DbConfig {
    DbConfig {
        wal: WalConfig {
            dir: "wal".to_string(),
            segment_bytes: 64,
        },
        ..DbConfig::default()
    }
}

#[test]
fn checkpoint_gcs_segments_and_recovery_still_replays_the_tail() {
    let cfg = tiny_segments_cfg();
    let sim = Sim::with_faults(0xC0FF_EE01, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
    db.transact(|tx| {
        tx.put(&Org {
            id: Id::from_raw(1),
            name: "acme".into(),
        });
        Ok::<_, io::Error>(())
    })
    .unwrap();
    for i in 0..50u64 {
        db.transact(|tx| {
            tx.put(&invoice(i, 1, i as i64));
            Ok::<_, io::Error>(())
        })
        .unwrap();
    }

    let before = sim.disk().list("wal/").unwrap().len();
    assert!(
        before > 2,
        "tiny segments should have rotated many times by now: {before}"
    );

    db.checkpoint().unwrap();

    let names = sim.disk().list("wal/").unwrap();
    assert!(
        names.len() < before,
        "checkpoint should have GC'd finished segments: {before} -> {}",
        names.len()
    );
    let earliest_base = names.iter().map(|n| segment_base(n)).min().unwrap();
    assert!(
        earliest_base > 0,
        "checkpoint should have advanced the log floor past 0"
    );

    drop(db);
    let db = Db::open(Box::new(sim.disk()), cfg, app_schema()).unwrap();
    let snap = db.snapshot().unwrap();
    assert_eq!(snap.scan::<Org>().unwrap().len(), 1);
    assert_eq!(snap.scan::<Invoice>().unwrap().len(), 50);
    for i in 0..50u64 {
        assert!(
            snap.get::<Invoice>(Id::from_raw(i)).unwrap().is_some(),
            "invoice {i} must survive the retained tail + checkpointed store"
        );
    }
}

#[test]
fn retention_pin_holds_segments_gc_would_otherwise_delete() {
    let cfg = tiny_segments_cfg();
    let sim = Sim::with_faults(0xC0FF_EE02, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), cfg, app_schema()).unwrap();
    for i in 0..50u64 {
        db.transact(|tx| {
            tx.put(&invoice(i, 1, i as i64));
            Ok::<_, io::Error>(())
        })
        .unwrap();
    }
    let before = sim.disk().list("wal/").unwrap().len();

    db.pin_retention(Box::new(|| 0));
    db.checkpoint().unwrap();

    let after = sim.disk().list("wal/").unwrap().len();
    assert_eq!(
        after, before,
        "a pin at seq 0 must block every deletion: {before} -> {after}"
    );
}

#[test]
fn auto_checkpoint_fires_on_the_record_cadence() {
    let cfg = DbConfig {
        checkpoint_every: 8,
        ..tiny_segments_cfg()
    };
    let sim = Sim::with_faults(0xC0FF_EE03, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), cfg, app_schema()).unwrap();
    for i in 0..9u64 {
        db.transact(|tx| {
            tx.put(&invoice(i, 1, i as i64));
            Ok::<_, io::Error>(())
        })
        .unwrap();
    }

    let names = sim.disk().list("wal/").unwrap();
    let earliest_base = names.iter().map(|n| segment_base(n)).min().unwrap();
    assert!(
        earliest_base > 0,
        "the cadence should have auto-checkpointed by the 9th commit, GC'ing early segments"
    );
}

#[test]
fn unopenable_store_with_compacted_wal_is_a_named_hard_error() {
    let cfg = tiny_segments_cfg();
    let sim = Sim::with_faults(0x0DD5_C0DE, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
    db.transact(|tx| {
        tx.put(&Org {
            id: Id::from_raw(1),
            name: "acme".into(),
        });
        tx.put(&invoice(10, 1, 5000));
        Ok::<_, io::Error>(())
    })
    .unwrap();
    for i in 0..20u64 {
        db.transact(|tx| {
            tx.put(&invoice(100 + i, 1, i as i64));
            Ok::<_, io::Error>(())
        })
        .unwrap();
    }
    db.checkpoint().unwrap();
    assert!(
        sim.disk()
            .list("wal/")
            .unwrap()
            .iter()
            .map(|n| segment_base(n))
            .min()
            .unwrap()
            > 0,
        "setup must actually compact the log before mangling the store"
    );
    // Graceful close: redb durably flushes, so header layout == file length.
    drop(db);

    let file = sim.disk().open("tables.redb").unwrap();
    let len = file.len().unwrap();
    file.set_len(len - 4096).unwrap();
    file.sync().unwrap();
    drop(file);

    let err = Db::open(Box::new(sim.disk()), cfg, app_schema()).unwrap_err();
    let msg = err.to_string();
    for needle in ["cannot open", "compacted", "restore", "retain_segments"] {
        assert!(
            msg.contains(needle),
            "error must name the state and the fix: {msg}"
        );
    }
}

// -------------------------------------------------- segment-mgmt T4: horizon

#[test]
fn db_open_routes_the_store_horizon_into_recovery() {
    let sim = Sim::with_faults(0xC0FF_EE06, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), app_schema()).unwrap();
    for i in 0..10u64 {
        db.transact(|tx| {
            tx.put(&invoice(i, 1, i as i64));
            Ok::<_, io::Error>(())
        })
        .unwrap();
    }
    db.checkpoint().unwrap(); // durable horizon (store applied_seq) is now 9
    drop(db);

    // Corrupt the CRC byte of the very first frame's header (offset 12) —
    // guaranteed to sit inside an early frame regardless of the record's own
    // payload encoding, and definitely at-or-below the horizon of 9.
    let segments = sim.disk().list("wal/").unwrap();
    let file = sim.disk().open(&segments[0]).unwrap();
    let mut byte = [0u8; 1];
    file.read_at(12, &mut byte).unwrap();
    file.write_at(12, &[byte[0] ^ 0xFF]).unwrap();
    file.sync().unwrap();

    let err = Db::open(Box::new(sim.disk()), DbConfig::default(), app_schema()).unwrap_err();
    let msg = err.to_string();
    for needle in ["invalid", "durable horizon", "acknowledged", "restore"] {
        assert!(
            msg.contains(needle),
            "Db::open must route the store's horizon into recovery, not open with fewer rows: {msg}"
        );
    }
}

/// The other half of the recovery-horizon rule (T2/T3 review adjudication):
/// `base_seq > applied_seq + 1` means records between them are gone from
/// BOTH the store and the log — corruption or entry loss, never a shape
/// `Db::checkpoint`'s own gc can produce (gc always keeps the log's floor at
/// or below its checkpoint's `applied_seq + 1`). Constructed directly at the
/// raw `Store`/`Wal` layer — the store's durable `applied_seq` is pinned to
/// 5 by hand, then the log is gc'd past seq 6 with no store involved at all
/// — because reaching this state through `Db`'s own API would mean fighting
/// (or duplicating) the very invariant this test proves `Db::open` enforces.
#[test]
fn db_open_refuses_a_vanished_prefix() {
    let cfg = tiny_segments_cfg();
    let sim = Sim::with_faults(0xC0FF_EE07, FaultConfig::none());

    let store = Store::open(&sim.disk(), &cfg.store_path).unwrap();
    for seq in 0..=5u64 {
        store.apply(seq, |_w| Ok::<(), io::Error>(())).unwrap();
    }
    store.checkpoint().unwrap(); // durably pins applied_seq at 5
    drop(store);

    let mut wal = Wal::open(Box::new(sim.disk()), cfg.wal.clone(), |_, _| Ok(())).unwrap();
    let records: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 8]).collect();
    for record in &records {
        wal.append(record).unwrap();
    }
    wal.sync().unwrap();
    wal.gc(20, 1).unwrap();
    assert!(
        wal.base_seq() > 6,
        "setup must push the log's base past the store's horizon (5 + 1 = 6): {}",
        wal.base_seq()
    );
    drop(wal);

    let err = Db::open(Box::new(sim.disk()), cfg, app_schema()).unwrap_err();
    let msg = err.to_string();
    for needle in ["base", "horizon", "neither", "restore"] {
        assert!(
            msg.contains(needle),
            "vanished-prefix error must name the base, the horizon, and the fix: {msg}"
        );
    }
}

#[test]
fn unopenable_store_with_full_wal_still_rebuilds() {
    // Same mangling as `mangled_store_len_recovers`, but with no checkpoint
    // or GC ever run (DbConfig::default()'s huge segment_bytes means only
    // one segment ever exists, so base_seq stays 0): the pre-T3
    // discard-and-rebuild path must still work unchanged.
    mangled_store_len_recovers(|len| len - 4096);
}

/// Companion to `regression_store_bricked_by_creation_crash` below: that
/// test's pinned seed exercises the scenario through `tx_workload`'s much
/// larger fuzz surface (3 rounds of commits/checkpoints/crashes) and was
/// never itself re-verified to still land on a genuinely bricked store —
/// T1's extra `sync_dir` barriers and this task's own extra `disk.list` in
/// the rebuild-rule check both shift exactly the crash-timing that seed
/// depends on. This test instead constructs the scenario directly and
/// self-verifies it at each step: an independent `Store::open` on the
/// post-crash disk confirms the store really is bricked *before* asking
/// `Db::open` to prove it discards and rebuilds — so this test cannot
/// silently stop testing what it claims to, the way a bare pinned seed can.
#[test]
fn fresh_store_creation_crash_is_discarded_and_rebuilt() {
    let mut state = 0xB81C_2026_0003u64; // scan seed, not the regression's own
    let mut found = None;
    'scan: for _ in 0..500 {
        let seed = charpente_sim::rng::splitmix64(&mut state);
        for ops in 0..32u64 {
            let sim = Sim::new(seed);
            sim.crash_disk_after(ops);
            let cfg = DbConfig::default();
            let opened = Db::open(Box::new(sim.disk()), cfg.clone(), Schema::new());
            sim.restart();
            if opened.is_err() && Store::open(&sim.disk(), &cfg.store_path).is_err() {
                found = Some((seed, ops));
                break 'scan;
            }
        }
    }
    let (seed, ops) = found.expect(
        "no (seed, crash-op) pair in a 500x32 scan bricked a fresh store at creation — \
         the crash model regressed",
    );
    eprintln!("bricked-at-creation scan: seed {seed:#018x}, crash after {ops} disk ops");

    // The (seed, ops) pair IS the reproduction: replay it fresh.
    let sim = Sim::new(seed);
    sim.crash_disk_after(ops);
    let cfg = DbConfig::default();
    assert!(Db::open(Box::new(sim.disk()), cfg.clone(), Schema::new()).is_err());
    sim.restart();
    assert!(
        Store::open(&sim.disk(), &cfg.store_path).is_err(),
        "setup must independently confirm the store is bricked before Db::open is asked to fix it"
    );

    let db = Db::open(Box::new(sim.disk()), cfg, Schema::new())
        .expect("Db::open must discard and rebuild a store bricked at creation");
    assert_eq!(
        db.last_applied(),
        None,
        "nothing was ever durably committed to this fresh db"
    );
}

// ------------------------------------------------------------- repl T3

/// `forbid_writes` gates only effectful `transact`; reads (and effect-free
/// transactions) are untouched, and `allow_writes` restores commits.
#[test]
fn forbidden_writes_fail_with_not_primary_and_reads_still_work() {
    let (_sim, mut db) = quiet_db(0x9012_0001);
    db.transact(|tx| {
        tx.put(&invoice(1, 1, 100));
        Ok::<_, io::Error>(())
    })
    .unwrap();
    assert!(db.writes_allowed());

    db.forbid_writes("primary-1:7000".to_string());
    assert!(!db.writes_allowed());

    // A read-only transaction (no effects) still works.
    let amount = db
        .transact(|tx| {
            Ok::<_, io::Error>(tx.get::<Invoice>(Id::from_raw(1))?.map(|i| i.amount_cents))
        })
        .unwrap();
    assert_eq!(amount, Some(100));

    // An effectful transaction is rejected, naming the primary — and writes
    // nothing (last_applied is unchanged).
    let err = db
        .transact(|tx| {
            tx.put(&invoice(2, 1, 200));
            Ok::<_, io::Error>(())
        })
        .unwrap_err();
    match err {
        TxError::NotPrimary { primary } => assert_eq!(primary, "primary-1:7000"),
        other => panic!("expected NotPrimary, got {other:?}"),
    }
    assert_eq!(
        db.last_applied(),
        Some(0),
        "the rejected transaction wrote nothing"
    );

    db.allow_writes();
    assert!(db.writes_allowed());
    db.transact(|tx| {
        tx.put(&invoice(2, 1, 200));
        Ok::<_, io::Error>(())
    })
    .unwrap();
    assert_eq!(db.last_applied(), Some(1), "allow_writes restores commits");
}

/// A row-content snapshot of both tables, keyed by id — the equality check
/// two independently-built `Db`s' scans get compared against.
type TableContents = (BTreeMap<u64, String>, BTreeMap<u64, (u64, i64, bool)>);

fn table_contents(snap: &Snapshot) -> TableContents {
    let orgs = snap
        .scan::<Org>()
        .unwrap()
        .into_iter()
        .map(|o| (o.id.raw(), o.name))
        .collect();
    let invoices = snap
        .scan::<Invoice>()
        .unwrap()
        .into_iter()
        .map(|i| (i.id.raw(), (i.org.raw(), i.amount_cents, i.paid)))
        .collect();
    (orgs, invoices)
}

/// The convergence oracle in miniature: a follower fed the primary's own
/// `read_log` output via `apply_shipped`, in batches, ends up with
/// byte-identical durable state.
#[test]
fn apply_shipped_replays_records_byte_identically() {
    let sim = Sim::with_faults(0x9012_0002, FaultConfig::none());
    let host_a = sim.host("a");
    let host_b = sim.host("b");
    let mut db_a = Db::open(Box::new(host_a.disk()), DbConfig::default(), app_schema()).unwrap();
    let mut db_b = Db::open(Box::new(host_b.disk()), DbConfig::default(), app_schema()).unwrap();

    for i in 0..20u64 {
        db_a.transact(|tx| {
            if i < 3 {
                tx.put(&Org {
                    id: Id::from_raw(i),
                    name: format!("org-{i}"),
                });
            }
            tx.put(&invoice(i, i % 3, (i as i64 + 1) * 100));
            if i >= 5 {
                tx.delete::<Invoice>(Id::from_raw(i - 5));
            }
            if i % 4 == 0 {
                tx.enqueue(SendReceipt { invoice: i });
            }
            Ok::<(), io::Error>(())
        })
        .unwrap();
    }
    assert_eq!(db_a.last_applied(), Some(19), "20 transactions, 20 records");

    let frames = db_a.read_log(0, usize::MAX).unwrap();
    assert_eq!(frames.len(), 20);
    let mid = frames.len() / 2;
    db_b.apply_shipped(&frames[..mid]).unwrap();
    db_b.apply_shipped(&frames[mid..]).unwrap();

    assert_eq!(db_b.last_applied(), db_a.last_applied());
    assert_eq!(
        table_contents(&db_a.snapshot().unwrap()),
        table_contents(&db_b.snapshot().unwrap()),
        "every row must match after replay"
    );

    db_a.checkpoint().unwrap();
    db_b.checkpoint().unwrap();
    drop(db_a);
    drop(db_b);

    let db_a = Db::open(Box::new(host_a.disk()), DbConfig::default(), app_schema()).unwrap();
    let db_b = Db::open(Box::new(host_b.disk()), DbConfig::default(), app_schema()).unwrap();
    assert_eq!(db_a.last_applied(), db_b.last_applied());
    assert_eq!(
        host_a.durable_fingerprint(),
        host_b.durable_fingerprint(),
        "the convergence oracle: byte-identical durable state at equal seq"
    );
}

#[test]
fn apply_shipped_refuses_a_seq_gap_by_name() {
    let sim = Sim::with_faults(0x9012_0003, FaultConfig::none());
    let host_a = sim.host("a");
    let host_b = sim.host("b");
    let mut db_a = Db::open(Box::new(host_a.disk()), DbConfig::default(), app_schema()).unwrap();
    for i in 0..10u64 {
        db_a.transact(|tx| {
            tx.put(&invoice(i, 0, i as i64));
            Ok::<_, io::Error>(())
        })
        .unwrap();
    }
    let frames = db_a.read_log(0, usize::MAX).unwrap();
    assert_eq!(frames.len(), 10);

    let mut db_b = Db::open(Box::new(host_b.disk()), DbConfig::default(), app_schema()).unwrap();
    let err = db_b.apply_shipped(&frames[5..]).unwrap_err();
    assert!(err.to_string().contains("expects seq"), "{err}");
}

/// `apply_shipped` is the follower's own write path — `forbid_writes` never
/// gates it (only `transact` is gated).
#[test]
fn apply_shipped_still_works_while_writes_are_forbidden() {
    let sim = Sim::with_faults(0x9012_0004, FaultConfig::none());
    let host_a = sim.host("a");
    let host_b = sim.host("b");
    let mut db_a = Db::open(Box::new(host_a.disk()), DbConfig::default(), app_schema()).unwrap();
    db_a.transact(|tx| {
        tx.put(&invoice(1, 0, 500));
        Ok::<_, io::Error>(())
    })
    .unwrap();
    let frames = db_a.read_log(0, usize::MAX).unwrap();

    let mut db_b = Db::open(Box::new(host_b.disk()), DbConfig::default(), app_schema()).unwrap();
    db_b.forbid_writes("primary-1:7000".to_string());
    db_b.apply_shipped(&frames).unwrap();
    assert_eq!(db_b.last_applied(), Some(0));
    let snap = db_b.snapshot().unwrap();
    assert_eq!(snap.scan::<Invoice>().unwrap()[0].amount_cents, 500);
}

// ------------------------------------------------------------- repl T4

/// `(id, name, payload)` per job, id order — the content comparison
/// `table_contents` doesn't cover (jobs aren't one of its two tables).
fn job_contents(snap: &Snapshot) -> Vec<(u64, String, Vec<u8>)> {
    snap.scan::<JobRow>()
        .unwrap()
        .into_iter()
        .map(|j| (j.id.raw(), j.name, j.payload))
        .collect()
}

/// The full round trip: a primary's logical snapshot (every registered
/// table's raw rows, plus the meta the exporter reads off ONE snapshot
/// transaction) rebuilds a byte-for-byte-equivalent follower through
/// `Seeder` — `finish` lands the follower exactly at the horizon + 1
/// boundary `Db::open` accepts, so it opens with nothing left to replay.
#[test]
fn seed_rebuilds_a_byte_identical_store() {
    let sim = Sim::with_faults(0x5EED_0001, FaultConfig::none());
    let host_a = sim.host("a");
    let host_b = sim.host("b");
    let cfg = DbConfig::default();
    let mut db_a = Db::open(Box::new(host_a.disk()), cfg.clone(), app_schema()).unwrap();

    for i in 0..30u64 {
        db_a.transact(|tx| {
            if i < 3 {
                tx.put(&Org {
                    id: Id::from_raw(i),
                    name: format!("org-{i}"),
                });
            }
            tx.put(&invoice(i, i % 3, (i as i64 + 1) * 10));
            if i.is_multiple_of(5) {
                tx.enqueue(SendReceipt { invoice: i });
            }
            Ok::<_, io::Error>(())
        })
        .unwrap();
    }
    db_a.checkpoint().unwrap();

    // Export: one MVCC snapshot, so meta and rows are mutually consistent.
    let snap = db_a.snapshot().unwrap();
    let applied = snap.applied_seq().unwrap().expect("30 committed txns");
    let next_job_id = snap.next_job_id().unwrap();
    let next_event_id = snap.next_event_id().unwrap();
    assert!(next_job_id > 0, "setup must have enqueued at least one job");

    let mut seeder = Seeder::new(Box::new(host_b.disk()), cfg.clone(), app_schema()).unwrap();
    for table in export_schema().table_names() {
        let rows = snap.scan_raw(table).unwrap();
        seeder.rows(table, &rows).unwrap();
    }
    // Db-level seed test: no wire, no `hello_position` — the frontier CRC is
    // never read back, so `0` stands in for it (the wire path is exercised by
    // charpente/tests/replication.rs).
    let mut db_b = seeder
        .finish(applied, next_job_id, next_event_id, 0)
        .unwrap();

    assert_eq!(db_b.last_applied(), Some(applied));
    assert_eq!(
        table_contents(&db_a.snapshot().unwrap()),
        table_contents(&db_b.snapshot().unwrap()),
        "seeded state must match the export through the typed surface"
    );
    assert_eq!(
        job_contents(&db_a.snapshot().unwrap()),
        job_contents(&db_b.snapshot().unwrap()),
        "seeded jobs must match the export too — a platform table, not an app one"
    );

    // A fresh commit on B (allow_writes is the default) appends at applied + 1.
    // It also enqueues, proving next_job_id continuity: the new job's id must
    // pick up exactly where the export's counter left off, never colliding
    // with one of the seeded jobs' own ids.
    db_b.transact(|tx| {
        tx.put(&invoice(999, 0, 1));
        tx.enqueue(SendReceipt { invoice: 999 });
        Ok::<_, io::Error>(())
    })
    .unwrap();
    assert_eq!(db_b.last_applied(), Some(applied + 1));
    let new_job = db_b
        .drain_enqueued()
        .pop()
        .expect("the fresh commit enqueued exactly one job");
    assert_eq!(
        new_job.raw(),
        next_job_id,
        "job ids must continue from the exported counter, not collide with a seeded job's"
    );

    // B's WAL base == applied + 1 — exactly the horizon + 1 boundary Db::open accepts.
    let names = host_b.disk().list("wal/").unwrap();
    let base = names.iter().map(|n| segment_base(n)).min().unwrap();
    assert_eq!(
        base,
        applied + 1,
        "the seed segment must sit at applied_seq + 1"
    );

    // Reopen B: recovery replays nothing, state intact.
    let before = table_contents(&db_b.snapshot().unwrap());
    let before_applied = db_b.last_applied();
    drop(db_b);
    let db_b2 = Db::open(Box::new(host_b.disk()), cfg, app_schema()).unwrap();
    assert_eq!(db_b2.last_applied(), before_applied);
    assert_eq!(table_contents(&db_b2.snapshot().unwrap()), before);
}

/// `into_seeder` discards whatever timeline was already at the disk —
/// seeding replaces state, it never merges with it.
#[test]
fn into_seeder_discards_the_old_timeline() {
    let (sim, mut db) = quiet_db(0x5EED_0002);
    for i in 0..10u64 {
        db.transact(|tx| {
            tx.put(&invoice(i, 0, i as i64));
            Ok::<_, io::Error>(())
        })
        .unwrap();
    }
    assert_eq!(db.last_applied(), Some(9));

    let mut rows = Vec::new();
    for i in 0..5u64 {
        let mut bytes = Vec::new();
        invoice(i, 0, i as i64).encode_row(&mut bytes);
        rows.push((i, bytes));
    }

    let mut seeder = db.into_seeder().unwrap();
    seeder.rows(Invoice::TABLE, &rows).unwrap();
    let db2 = seeder.finish(4, 0, 0, 0).unwrap();

    let names = sim.disk().list("wal/").unwrap();
    assert_eq!(
        names.len(),
        1,
        "into_seeder must discard every prior segment: {names:?}"
    );
    assert_eq!(
        segment_base(&names[0]),
        5,
        "the seeded segment must sit at applied_seq + 1"
    );
    let file = sim.disk().open(&names[0]).unwrap();
    assert_eq!(
        file.len().unwrap(),
        0,
        "the seeded segment must be empty — recovery replays nothing"
    );

    assert_eq!(db2.last_applied(), Some(4));
    let snap = db2.snapshot().unwrap();
    assert_eq!(ids(&snap.scan::<Invoice>().unwrap()), vec![0, 1, 2, 3, 4]);
}

/// Proof the import path maintains secondary indexes, not just row bytes.
#[test]
fn seeded_indexes_answer_queries() {
    let (_sim_a, mut db_a) = quiet_db(0x5EED_0003);
    for i in 0..6u64 {
        db_a.transact(|tx| {
            tx.put(&invoice(i, i % 2, (i as i64 + 1) * 10));
            Ok::<_, io::Error>(())
        })
        .unwrap();
    }
    db_a.checkpoint().unwrap();

    let snap = db_a.snapshot().unwrap();
    let applied = snap.applied_seq().unwrap().unwrap();
    let next_job_id = snap.next_job_id().unwrap();
    let next_event_id = snap.next_event_id().unwrap();
    let rows = snap.scan_raw(Invoice::TABLE).unwrap();

    let sim_b = Sim::with_faults(0x5EED_0004, FaultConfig::none());
    let mut seeder =
        Seeder::new(Box::new(sim_b.disk()), DbConfig::default(), app_schema()).unwrap();
    seeder.rows(Invoice::TABLE, &rows).unwrap();
    let db_b = seeder
        .finish(applied, next_job_id, next_event_id, 0)
        .unwrap();

    let acme = Id::<Org>::from_raw(0);
    let expected = ids(&Invoice::by_org(&snap, &acme).unwrap());
    let got = ids(&Invoice::by_org(&db_b.snapshot().unwrap(), &acme).unwrap());
    assert!(
        !expected.is_empty(),
        "setup must actually populate the index bucket under test"
    );
    assert_eq!(
        got, expected,
        "a seeded index must answer the same query as the source"
    );
}
