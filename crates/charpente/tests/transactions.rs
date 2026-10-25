//! Typed transactions (PLAN.md M0 step 4): read-your-own-writes, atomic
//! effects records, abort semantics, recovery replay, and the contract
//! violations — unit tests first; the crash-property suite follows in the
//! same file (added by the next plan task).

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use charpente::{Db, DbConfig, Event, Job, Schema, Seeder, TxError, View, table};
use charpente_core::event::EventRow;
use charpente_core::io::{Disk, Entropy, File};
use charpente_core::job::JobRow;
use charpente_core::log::{Wal, WalConfig};
use charpente_core::table::{Codec, Id, Rejection, Row, Snapshot, Store, StoreRejected, Unscoped};
use charpente_sim::{FaultConfig, Sim, run_universes_swarm, run_universes_swarm_with};

/// The re-seed clause every "restore … from a replica or backup" text carries
/// (DESIGN §5's re-seed rule; S3b R3-M1), typed out so a pin goes red if it
/// drifts or goes missing.
const RESTORE_IS_A_RESEED: &str = "For a follower, or a primary once a follower is promoted in \
     its place, that is a re-seed: stop the node, delete its data dir, and restart it configured \
     as a follower of the current primary so it snapshot-seeds from it, never with a primary \
     config: the durable role file that outranks config is deleted with the data dir, so a \
     cleared node started as a primary comes up as a second writable primary";

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
    const FINGERPRINT: u64 = 0x7E57_0001;
    type Scope = Unscoped;
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
    const FINGERPRINT: u64 = 0x7E57_0002;
    // A global event: it belongs to no tenant.
    type Scope = Unscoped;
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

#[table(global)]
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
    const FINGERPRINT: u64 = 0x7E57_0003;
    type Scope = Unscoped;
}

#[test]
#[should_panic(
    expected = "contract: an `enqueue_keyed` app key names the job's external identity — it \
                cannot be empty"
)]
fn an_empty_enqueue_keyed_app_key_is_a_contract_violation() {
    let sim = Sim::with_faults(0x601, FaultConfig::none());
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), Schema::new()).unwrap();
    let _ = db.transact(|tx| {
        tx.enqueue_keyed("", Ping { i: 0 });
        Ok::<(), std::io::Error>(())
    });
}

// ---------------------------------------------------- crash property

#[table(global)]
struct Account {
    id: Id<Account>,
    #[index]
    bucket: u8,
    balance: i64,
}

#[table(global)]
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
    const FINGERPRINT: u64 = 0x7E57_0004;
    type Scope = Unscoped;
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
    // Narrower than the shared 4-family `checks::classify`: any Io error
    // (from transact's commit, from a group's stage or commit, or from
    // checkpoint's own gc, which can now poison the WAL too) breaks and
    // restarts the universe immediately, before a second op could ever
    // reach an already-poisoned/latched handle, so this workload never sees
    // Poisoned or RedbLatch — unlike kernel.rs's SUT, which absorbs request
    // Io into 500s and keeps running. Recognizing those here would only
    // ever LOOSEN this oracle: with RedbLatch accepted, a genuine bug that
    // makes every `Db::open` fail permanently passes the final-recovery arm
    // below green, because redb's latch wording is generic and is exactly
    // the wording a real store bug would wear (task-5 review C1,
    // MUTATION-B6). Injected only: the durable-horizon refusal is excused
    // where durable corruption fired and nowhere else, which its text cannot
    // say, so every open site asks `checks::durable_refusal` first (S3b
    // item 0; `boot_through_fallout` does it for the round boots).
    //
    // "breaks and restarts immediately" is load-bearing, not incidental: it
    // is what the multi-record arm's `stage_failed` break preserves. A
    // stage error leaves the store's redb handle latched, and without that
    // break the round walked on to `db.checkpoint()` — the first op to
    // touch the latched handle, surfacing redb's wording rather than the
    // original fault's (task-5 finding, seed 0x2ce7af08a308ed30; measured
    // at ~2 failures per 12,800 universes, 0 in 28,160 with the break).
    matches!(
        charpente_sim::checks::classify(text),
        charpente_sim::checks::FaultReason::Injected
    )
}

/// How many reopens the final verification gets before the universe gives
/// up. Every attempt either verifies or dies to injected fault fallout, so a
/// larger budget costs time only in universes that would otherwise have
/// concluded nothing. With the rounds aimed, 16 -> 64 took give-ups over ten
/// fresh bases from 362 to 164 for +4.4% CPU (2026-10-01, S5 review I1). Before
/// S5 a failed reopen rebuilt the table store, and 32 cost +28%.
const FINAL_ATTEMPTS: u32 = 64;

/// How many boots each round gets before it restarts and moves on
/// (`checks::boot_through_fallout`). A round whose boot fails skips its check
/// and its commits, so this budget is reach: over ten fresh bases at 256
/// universes, booting once left 3,463 rounds unbooted, and 16, 32 and 64 left
/// 563, 275 and 103, for +15.4%, +19.0% and +23.1% CPU (2026-10-01, S5 review
/// I1). More rounds mean more to recover at the end: give-ups went 73 -> 134,
/// 149 and 164, against 243 for the same bases before S5.
const ROUND_BOOT_TRIES: u32 = 64;

/// One universe: recover → verify against the oracle → mixed commits and
/// aborts → random checkpoint → crash, everything seeded.
fn tx_workload(sim: &mut Sim) -> Result<(), String> {
    tx_workload_on(sim, &|sim: &Sim| -> Box<dyn Disk> { Box::new(sim.disk()) })
}

/// `tx_workload` over the disk `disk` makes for each `Db::open`. The only other
/// maker is `DiscardWitness`, which issues no disk op of its own, so a universe
/// run through it is the same universe.
fn tx_workload_on(sim: &mut Sim, disk: &dyn Fn(&Sim) -> Box<dyn Disk>) -> Result<(), String> {
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
        // Sized to redb 4.3.0's disk-op volume per round: DESIGN §1, "countdowns".
        sim.crash_disk_after(43 + entropy.next_u64() % 346);
        // Every error a recovery meets is classified, as in the final loop
        // below. A store redb rejects never reaches here: `Db::open` discards
        // and rebuilds it from the log itself, so what comes out is either an
        // injected fault (the round restarts) or a real error, which fails
        // the universe by name rather than restarting past it.
        let Some(mut db) =
            charpente_sim::checks::boot_through_fallout(sim, ROUND_BOOT_TRIES, is_injected, || {
                Db::open(disk(sim), cfg.clone(), property_schema())
            })
            .map_err(|e| format!("round recovery failed with a non-injected error: {e}"))?
        else {
            // A durable refusal already gave the universe up, by name.
            if sim.gave_up().is_some() {
                return Ok(());
            }
            sim.restart();
            continue;
        };

        let recovered = db.last_applied().map_or(0, |s| s + 1);
        if recovered < committed {
            // Excused ONLY when the harness actually injected durable
            // corruption in this universe: bit_flip/misdirected_write on a
            // WAL file above the checkpoint horizon is silently cut at
            // recovery, by the WAL convention (see
            // `checks::durable_corruption_fired`); ending the universe here
            // keeps the strong oracle standing everywhere corruption did
            // not fire. Ending it is a give-up, never a pass: nothing past
            // the loss is adjudicated.
            if charpente_sim::checks::durable_corruption_fired(sim) {
                sim.give_up(format!(
                    "durable corruption cut acknowledged transactions at a round's recovery \
                     ({committed} acknowledged, {recovered} recovered), so durability was not \
                     adjudicated and the rounds after it never ran"
                ));
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
                // A round's recovered state was checked against the model.
                sim.cover("tx:round-verified");
            }
            Err(e) => {
                if !is_injected(&e.to_string()) {
                    return Err(format!(
                        "round snapshot failed with a non-injected error: {e}"
                    ));
                }
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
                    Err(TxError::Verifying { .. }) => {
                        unreachable!("this workload never calls verify_writes")
                    }
                    Ok(()) => unreachable!("the closure always aborts"),
                }
                if db.last_applied() != before {
                    return Err("aborted transaction consumed a seq".to_string());
                }
            }
            if entropy.next_u64().is_multiple_of(7) {
                // Multi-record batch: begin_group + several stage calls + one
                // commit is the only way through this API to reach a batch of
                // >1 record — db.transact above is exactly one stage per
                // commit (spec §7's batch-of-one path). Coverage below proves
                // this workload actually reaches ≥2-record batches under the
                // fault matrix, not just batch-of-one — a property that never
                // forms one proves nothing about batching.
                let mut g = match db.begin_group() {
                    Ok(g) => g,
                    Err(_) => break, // snapshot failed: died here
                };
                let n = 2 + entropy.next_u64() % 3; // 2..=4 records
                let mut stage_failed = false;
                for k in 0..n {
                    match g.stage(|tx| apply_txn(tx, i + k)) {
                        Ok(()) => {}
                        // a tx read hit an injected fault; Group::stage never
                        // touches its record queue before f returns Ok, so
                        // nothing staged here is lost
                        Err(TxError::App(_)) | Err(TxError::Io(_)) => {
                            stage_failed = true;
                            break;
                        }
                        Err(TxError::NotPrimary { .. }) => {
                            unreachable!("this workload never calls forbid_writes")
                        }
                        Err(TxError::Verifying { .. }) => {
                            unreachable!("this workload never calls verify_writes")
                        }
                    }
                }
                // The count the log actually carries: `Group::stage` queues a
                // record only on `Ok` WITH at least one effect, so counting
                // `Ok` returns instead would let a no-effect handler inflate
                // `committed` and fake a durability violation below.
                let glen = g.len() as u64;
                let multi = glen >= 2;
                match g.commit() {
                    Ok(()) => {
                        committed += glen;
                        if multi {
                            sim.cover("batch:multi_record");
                        }
                    }
                    Err(failure) => {
                        // `applied` is 0 (append/sync failure — nothing
                        // reached the store) or `glen` (all reached the
                        // store, checkpoint bookkeeping failed after) — the
                        // atomic-batch guarantee this task exists to fault.
                        // Unconditional break, no is_injected classification,
                        // matching db.transact's TxError::Io arm below: died
                        // mid-commit, durable or not — the outer
                        // `recovered < committed` check is what catches a
                        // genuine loss, not a text match on this error.
                        committed += failure.applied as u64;
                        if multi {
                            // Spec §5's "the test that earns §2's claim" is
                            // the FAULTED multi-record commit, not the clean
                            // one. Keyed separately from batch:multi_record
                            // because that key rides the Ok arm and stays
                            // satisfied by successful commits alone — a
                            // change that stopped the fault matrix reaching a
                            // multi-record commit boundary would decay the
                            // atomicity property to vacuous without tripping
                            // require: (task-5 review I3).
                            sim.cover("batch:multi_record_faulted");
                        }
                        break;
                    }
                }
                if stage_failed {
                    // Break the ROUND, matching db.transact's App/Io arms
                    // below for the identical errors. The break is after
                    // commit, never instead of it: `Group`'s Drop panics on a
                    // non-empty group, and `commit` is what empties the queue
                    // (on both arms) — a bare break out of the stage loop
                    // would abort the process the first time a stage failed
                    // at k > 0. See is_injected above for what this break
                    // buys.
                    break;
                }
            } else {
                match db.transact(|tx| apply_txn(tx, i)) {
                    Ok(()) => committed += 1,
                    // a tx read hit an injected fault; nothing was written
                    // (found by seed 0x5e713efe41f78d1a)
                    Err(TxError::App(_)) => break,
                    Err(TxError::Io(_)) => break, // died mid-commit; durable or not
                    Err(TxError::NotPrimary { .. }) => {
                        unreachable!("this workload never calls forbid_writes")
                    }
                    Err(TxError::Verifying { .. }) => {
                        unreachable!("this workload never calls verify_writes")
                    }
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
    // final check, and a universe in which every attempt was skipped gives
    // up; any other open error is a real bug.
    for _attempt in 0..FINAL_ATTEMPTS {
        let db = match Db::open(disk(sim), cfg.clone(), property_schema()) {
            Ok(db) => db,
            Err(e) => {
                let text = e.to_string();
                // No restart clears a durable refusal (S5 re-review R-M2), and
                // only a fired durable-corruption fault excuses one (S3b item 0).
                if charpente_sim::checks::durable_refusal(sim, &text)? {
                    return Ok(());
                }
                if is_injected(&text) {
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
            // Same excuse-only-if-injected routing, and give-up, as the round
            // loop above.
            if charpente_sim::checks::durable_corruption_fired(sim) {
                sim.give_up(format!(
                    "durable corruption cut acknowledged transactions at the final recovery \
                     ({committed} acknowledged, {recovered} recovered), so the final state was \
                     never checked"
                ));
                return Ok(());
            }
            return Err(format!(
                "durability violated: {committed} txns acknowledged, {recovered} recovered"
            ));
        }
        let snap = match db.snapshot() {
            Ok(snap) => snap,
            Err(e) => {
                if !is_injected(&e.to_string()) {
                    return Err(format!(
                        "final snapshot failed with a non-injected error: {e}"
                    ));
                }
                sim.restart();
                continue;
            }
        };
        // Per-batch apply (spec §2): the store commits a batch whole or not
        // at all, so its applied_seq is always a batch boundary — never an
        // interior seq of a batch that did not commit.
        //
        // This is the FOURTH layer over that invariant, not its oracle
        // (task-5 review I2). Three pre-existing ones fire first on a store
        // that outran the log: redb's own transaction atomicity, the gapless
        // assert in `Store::apply_batch` (table.rs), and `Db::open`'s
        // store-ahead assert (db.rs). Its exclusive window is the one those
        // structurally cannot see — a store that advanced past the log tail
        // AFTER `Engine::recover` returned, because `Db::open`'s assert
        // compares the PRE-replay store seq against the POST-replay tail.
        // Kept for that window; do not read it as the batch-atomicity
        // oracle for the swarm (the unit test that faults the batch boundary
        // is). Read off `snap`, the same transaction `db_matches_model`
        // scans below, rather than a second, later read of the store.
        let store_at = match snap.applied_seq() {
            Ok(at) => at,
            Err(e) => {
                if is_injected(&e.to_string()) {
                    sim.restart();
                    continue;
                }
                return Err(format!(
                    "store applied_seq read failed with a non-injected error: {e}"
                ));
            }
        };
        // `at >= recovered`, not `at > recovered.saturating_sub(1)`: the
        // latter masks recovered == 0 (0usize.saturating_sub(1) saturates to
        // 0, so store_at == Some(0) would silently pass when NOTHING was
        // recovered) — found by mutation-testing this exact assertion
        // (task-5 report).
        if let Some(at) = store_at
            && at >= recovered
        {
            return Err(format!(
                "store applied_seq {at} is ahead of recovered {recovered} — a batch \
                 committed to the store that the log does not cover"
            ));
        }
        return match db_matches_model(&snap, recovered) {
            Ok(()) => Ok(()),
            Err(msg) if is_injected(&msg) => {
                sim.restart();
                continue;
            }
            Err(msg) => Err(msg),
        };
    }
    sim.give_up(format!(
        "the final verification never ran: all {FINAL_ATTEMPTS} attempts hit injected faults, \
         so the last round's commits were never checked"
    ));
    Ok(())
}

/// The give-up cap. This base is ambient, so the count moves batch to batch,
/// and the cap is a margin, not a measurement: over 10 fresh batches (16 final
/// attempts) it gave up 16-31 times, mean 22.5, and the cap is twice that
/// maximum, about 7 standard deviations above the mean. It guards drift toward
/// adjudicating nothing, not the budget: with 8 attempts four fixed bases gave
/// up 35-38 times, under it. Re-measured 2026-10-01 for S5 with D1: 10 fresh
/// batches gave up 19-31 times, mean 25.4, sd 4.1, so the cap stays 62, 9.0
/// standard deviations above the mean. Re-measured 2026-10-01 with the round
/// boots aimed and 64 final attempts (S5 review I1): 10 fresh batches gave up
/// 8-24 times, mean 15.6, sd 5.0, so the cap is 48, 6.5 standard deviations
/// above the mean (23.0 at S5's parent, same bases). GUIDE §8's Poisson cap on
/// those batches (x = 156 over N = 2,560, n = 256, lambda = 17.8) is 35, under
/// 48 (2026-10-02).
const TX_GAVE_UP_CEILING: u64 = 48;

/// The reach floor: rounds whose recovered state was checked against the
/// model. A margin, as the cap is: 10 fresh batches (2026-10-01, S5 review I1)
/// checked 738-756 rounds, mean 749.4, sd 4.9, and the floor is the mean less
/// 4 standard deviations (GUIDE §8).
const TX_ROUND_VERIFIED_FLOOR: u64 = 729;

/// A durable refusal at the final recovery ends it at once, by name (S5 re-review
/// R-M2). Seed `0x14656f1b6c57a314`, at the batch's base, meets one there. No restart
/// clears it, and the loop used to spend its whole budget on it, then give
/// up as though every attempt had hit an injected fault ("the final verification never ran").
#[test]
fn a_durable_refusal_at_the_final_recovery_ends_it_at_once_by_name() {
    const SEED: u64 = 0x1465_6f1b_6c57_a314;
    let mut state = SEED;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(SEED, &mut cfg);
    let mut sim = Sim::with_faults(SEED, cfg);

    if let Err(e) = tx_workload(&mut sim) {
        panic!("seed {SEED:#x}: {e}");
    }
    assert_eq!(
        sim.coverage().get("boot:durable-refusal"),
        Some(&1),
        "seed {SEED:#x}: {:?}",
        sim.coverage()
    );
    let reason = sim
        .gave_up()
        .unwrap_or_else(|| panic!("seed {SEED:#x}: a durable refusal must give up, not pass"));
    assert!(
        reason.starts_with(
            "the boot was refused for durable corruption at or below the store's horizon"
        ),
        "seed {SEED:#x} must give up for the refusal, at once: {reason}"
    );
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
            "batch:multi_record",
            "batch:multi_record_faulted",
            "tx:round-verified",
            "boot:booted-after-retry",
        ],
        floor: [("tx:round-verified", TX_ROUND_VERIFIED_FLOOR)],
        ceiling: [("universe:gave-up", TX_GAVE_UP_CEILING)]
    );
}

/// A universe whose final verification never ran adjudicated nothing about
/// its last round, so it gives up rather than passing (LEDGER 2026-09-30,
/// follow-up 9). Every disk op faults here, so every attempt dies to injected
/// faults.
#[test]
fn a_final_verification_that_never_runs_gives_up_rather_than_passing() {
    // Control: the same seed with no faults reaches a verdict.
    let mut control = Sim::with_faults(0x7A00_0901, FaultConfig::none());
    assert_eq!(tx_workload(&mut control), Ok(()));
    assert_eq!(
        control.gave_up(),
        None,
        "a fault-free universe reaches a verdict"
    );

    let mut sim = Sim::with_faults(
        0x7A00_0901,
        FaultConfig {
            read_error: 1.0,
            write_error: 1.0,
            sync_error: 1.0,
            ..FaultConfig::none()
        },
    );
    assert_eq!(tx_workload(&mut sim), Ok(()));
    let reason = sim
        .gave_up()
        .expect("a universe whose final verification never ran must give up, not pass");
    assert!(
        reason.contains("the final verification never ran"),
        "and it must be THIS give-up: {reason}"
    );
}

/// Acknowledged transactions cut by durable corruption are the sanctioned
/// single-node loss, so the durability oracle cannot run past them: the
/// universe gives up rather than passing (LEDGER 2026-09-30, follow-up 9's
/// rule for every arm that ends a universe without a verdict). Seed
/// `0xfc1e12c7cf3f8019` meets it at a round's recovery (base `0x5E1F_0006`,
/// universe 133), seed `0x0a51f0d80437e0dc` at the final one (same base,
/// universe 82). Re-pinned 2026-10-01 for S5 with D1, which re-key every
/// universe: `0x902dd63b08e51d75` now meets the cut at the final recovery
/// instead of a round's, and `0x671991ebc8bd4f24` no longer meets durable
/// corruption. The final arm re-pinned the same day when the round boots were
/// aimed (S5 review I1): `0xa5c304ecd9f3a44e` no longer reaches it. Found by
/// scanning the same base for the first universe that gives up for the cut at
/// each recovery (one of 256 at a round's, four at the final).
#[test]
fn acked_transactions_cut_by_durable_corruption_give_up_rather_than_pass() {
    for (seed, arm) in [
        (0xfc1e_12c7_cf3f_8019_u64, "at a round's recovery"),
        (0x0a51_f0d8_0437_e0dc, "at the final recovery"),
    ] {
        let mut state = seed;
        let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
        let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
        charpente_sim::sample_extended_fields(seed, &mut cfg);
        let mut sim = Sim::with_faults(seed, cfg);

        assert_eq!(tx_workload(&mut sim), Ok(()), "seed {seed:#x}");
        assert!(
            charpente_sim::checks::durable_corruption_fired(&sim),
            "seed {seed:#x}"
        );
        let reason = sim.gave_up().unwrap_or_else(|| {
            panic!(
                "seed {seed:#x}: acked transactions lost to durable corruption must give up, \
                 not pass"
            )
        });
        assert!(
            reason.contains("durable corruption cut acknowledged transactions")
                && reason.contains(arm),
            "seed {seed:#x} must give up {arm}: {reason}"
        );
    }
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
/// crashed disk fails the WAL's own append/sync first. This test registers a
/// hand-written row whose `decode_row` cannot read what its own `encode_row`
/// wrote: `Group::stage` finds the table registered and written through its
/// registered type, the WAL append and sync succeed (no disk fault at all),
/// and `TableMachine::apply`'s decode fails afterward — a post-sync apply
/// failure, which is the engine's own bit, not the WAL's. (Until
/// `Group::stage` refused a write to an unregistered table before its record
/// existed, an `Invoice` put with `Invoice` left out of the schema was this
/// test's way to fail apply.)
#[test]
fn poisoned_reports_the_engines_own_bit_after_a_post_sync_apply_failure() {
    struct Undecodable {
        id: Id<Undecodable>,
    }
    impl Row for Undecodable {
        type Scope = Unscoped;
        const TABLE: &'static str = "test.undecodable";
        const INDEXES: &'static [charpente_core::table::IndexDef<Undecodable>] = &[];
        fn id(&self) -> Id<Undecodable> {
            self.id
        }
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.id.encode(out);
        }
        // Reads a field `encode_row` never wrote.
        fn decode_row(bytes: &[u8]) -> io::Result<Undecodable> {
            let input = &mut &bytes[..];
            let id = Id::decode(input)?;
            u32::decode(input)?;
            Ok(Undecodable { id })
        }
    }

    let sim = Sim::with_faults(0x9015_0DEE, FaultConfig::none());
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new().table::<Org>().table::<Undecodable>(),
    )
    .unwrap();
    assert!(!db.poisoned());

    let err = db
        .transact(|tx| {
            tx.put(&Undecodable {
                id: Id::from_raw(1),
            });
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
        // Sized to redb 4.3.0's disk-op volume per round: DESIGN §1, "countdowns".
        sim.crash_disk_after(27 + entropy.next_u64() % 219);
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
///
/// The seed was `0x1f733f8d9eebf822` until 2026-09-30, when redb 4.3.0 and the
/// re-sized crash countdowns left it reaching `Db::open`'s discard only through
/// an injected fsync failure, never through garbage. `0xf79c40a51bb93ae8` was
/// re-found by its reason (a temporary print of the discard's cause, 20,000
/// seeds from base `0x1F730000`): its second discard is "Not a redb database:
/// magic number mismatch", a store whose creation never became durable. With
/// the discard-and-rebuild removed, the same universe fails "final recovery
/// failed with a non-injected error: … magic number mismatch" on every later
/// open. `fresh_store_creation_crash_is_discarded_and_rebuilt` below builds the
/// same state directly and checks it at each step.
///
/// The test asserts its reason as well as its outcome: a `DiscardWitness`
/// watches `Db::open` and records why each discard happened, and at least one
/// must be creation garbage. The previous seed kept passing on `Ok(())` alone
/// after it had stopped reaching garbage, and nothing noticed.
#[test]
fn regression_store_bricked_by_creation_crash() {
    let mut sim = Sim::new(0xf79c_40a5_1bb9_3ae8);
    let seen = Arc::new(Mutex::new(StoreSeen::default()));
    let witness = seen.clone();
    let result = tx_workload_on(&mut sim, &move |sim: &Sim| -> Box<dyn Disk> {
        Box::new(DiscardWitness {
            inner: Box::new(sim.disk()),
            seen: witness.clone(),
        })
    });
    assert_eq!(
        result,
        Ok(()),
        "seed 0xf79c40a51bb93ae8 no longer recovers a store bricked at creation"
    );
    // `Ok` alone is also what a give-up returns: recovering means the rebuilt
    // store went on to be checked.
    assert_eq!(
        sim.gave_up(),
        None,
        "seed 0xf79c40a51bb93ae8 must reach a verdict on the rebuilt store, not give up"
    );
    let discards = seen.lock().expect("witness lock").discards.clone();
    assert!(
        discards.contains(&Discard::Garbage),
        "seed 0xf79c40a51bb93ae8 no longer reaches Db::open's discard through creation garbage \
         (a non-empty store without redb's magic number); its discards were {discards:?}. A \
         seed names a universe, not a scenario: re-find one that reaches this reason (hard \
         rule 3)"
    );
}

/// A disk whose first listing of the log fails with an error that names no
/// injected fault: the stand-in for a real platform error at recovery.
struct LogListingFailsOnce {
    inner: charpente_sim::SimDisk,
    failed: Arc<Mutex<bool>>,
}

impl Disk for LogListingFailsOnce {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        self.inner.open(path)
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut failed = self.failed.lock().expect("listing lock");
        if prefix.starts_with("wal") && !*failed {
            *failed = true;
            return Err(io::Error::other("the log directory could not be listed"));
        }
        self.inner.list(prefix)
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        self.inner.sync_dir(prefix)
    }
}

/// A round's recovery that fails with an error naming no injected fault fails
/// the universe by name. It used to restart past it (`Err(_) => restart`), and
/// the next round's recovery then succeeded, so the universe passed over an
/// error it never classified (sweep, 2026-10-01).
#[test]
fn a_non_injected_error_at_a_rounds_recovery_fails_the_universe() {
    let mut sim = Sim::with_faults(0x7A0F_0001, FaultConfig::none());
    let failed = Arc::new(Mutex::new(false));
    let flag = failed.clone();
    let result = tx_workload_on(&mut sim, &move |sim: &Sim| -> Box<dyn Disk> {
        Box::new(LogListingFailsOnce {
            inner: sim.disk(),
            failed: flag.clone(),
        })
    });
    assert!(
        *failed.lock().expect("listing lock"),
        "the listing never failed"
    );
    let err = result.expect_err("a non-injected recovery error must fail the universe");
    assert!(
        err.contains("round recovery failed with a non-injected error")
            && err.contains("the log directory could not be listed"),
        "it must fail at the round's recovery, by name: {err}"
    );
}

/// redb's magic number (`MAGICNUMBER`, redb 4.3.0 `header.rs`): the nine bytes
/// `Database::new` reads at offset 0 of a non-empty file before it will open it.
/// A file shorter than nine bytes is compared as zeros.
const REDB_MAGIC: [u8; 9] = [b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A];

/// Why `Db::open` discarded the store, judged from what the witness saw on the
/// store's path while it was being opened.
#[derive(Debug, Clone, PartialEq)]
enum Discard {
    /// A non-empty file without redb's magic number: a creation that never
    /// became durable ("Not a redb database: magic number mismatch").
    Garbage,
    /// The open that preceded the discard hit this IO error on the store's
    /// path: a fault a retry may not repeat, not a verdict on the file's bytes.
    AfterIoError(String),
    /// Anything else, e.g. a store redb found corrupted.
    Other,
}

#[derive(Default)]
struct StoreSeen {
    /// The store file's length and magic-number check, as redb read them
    /// since `tables.redb` was last opened.
    len: Option<u64>,
    magic: Option<bool>,
    /// True from `Disk::open("tables.redb")` until `Db::open` next touches
    /// any other path: the span in which `Store::open` runs.
    opening: bool,
    /// The first IO error on the store's path while `opening`.
    open_error: Option<String>,
    discards: Vec<Discard>,
    /// Open errors followed by a fresh open of the same file, with no discard
    /// between: a transient fault left to bubble and retried.
    retried: Vec<String>,
}

impl StoreSeen {
    fn note_error(&mut self, e: &io::Error) {
        if self.opening && self.open_error.is_none() {
            self.open_error = Some(e.to_string());
        }
    }
}

/// A pass-through `Disk` that watches `Db::open` discard `tables.redb`. It
/// records the file's length and its first nine bytes as redb reads them,
/// every IO error on the store's path while the store is being opened, and
/// classifies each removal of the file. It issues no disk op of its own. An
/// inspection that did would tick the crash countdown and draw latency, and
/// so rename the universe it is watching.
struct DiscardWitness {
    inner: Box<dyn Disk>,
    seen: Arc<Mutex<StoreSeen>>,
}

impl Disk for DiscardWitness {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let mut seen = self.seen.lock().expect("witness lock");
        if path != "tables.redb" {
            seen.opening = false;
            drop(seen);
            return self.inner.open(path);
        }
        if let Some(e) = seen.open_error.take() {
            seen.retried.push(e);
        }
        seen.len = None;
        seen.magic = None;
        seen.opening = true;
        let file = self.inner.open(path).inspect_err(|e| seen.note_error(e))?;
        Ok(Box::new(StoreWitness {
            inner: file,
            seen: self.seen.clone(),
        }))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        if path == "tables.redb" {
            let mut seen = self.seen.lock().expect("witness lock");
            let garbage = match seen.len {
                Some(len) if len > 0 => len < REDB_MAGIC.len() as u64 || seen.magic == Some(false),
                _ => false,
            };
            let discard = match seen.open_error.take() {
                Some(e) => Discard::AfterIoError(e),
                None if garbage => Discard::Garbage,
                None => Discard::Other,
            };
            seen.discards.push(discard);
        }
        self.inner.remove(path)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.seen.lock().expect("witness lock").opening = false;
        self.inner.list(prefix)
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        let r = self.inner.sync_dir(prefix);
        if prefix == "tables.redb"
            && let Err(e) = &r
        {
            self.seen.lock().expect("witness lock").note_error(e);
        }
        r
    }
}

struct StoreWitness {
    inner: Box<dyn File>,
    seen: Arc<Mutex<StoreSeen>>,
}

impl StoreWitness {
    fn noted<T>(&self, r: io::Result<T>) -> io::Result<T> {
        if let Err(e) = &r {
            self.seen.lock().expect("witness lock").note_error(e);
        }
        r
    }
}

impl File for StoreWitness {
    fn len(&self) -> io::Result<u64> {
        let len = self.noted(self.inner.len())?;
        self.seen.lock().expect("witness lock").len = Some(len);
        Ok(len)
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.noted(self.inner.read_at(offset, out))?;
        if offset == 0 && out.len() >= REDB_MAGIC.len() {
            self.seen.lock().expect("witness lock").magic =
                Some(out[..REDB_MAGIC.len()] == REDB_MAGIC);
        }
        Ok(())
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.noted(self.inner.write_at(offset, data))
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.noted(self.inner.set_len(len))
    }

    fn sync(&self) -> io::Result<()> {
        self.noted(self.inner.sync())
    }
}

/// Where `OneShotFault` fails the store's open, once.
#[derive(Debug, Clone, Copy, PartialEq)]
enum OpenSite {
    /// `Disk::open("tables.redb")`: `Store::open`'s own first call.
    Open,
    /// `Disk::sync_dir("tables.redb")`: `Store::open`'s name barrier.
    SyncDir,
    /// The first `File::read_at` on the store: redb's header read.
    Read,
    /// The first `File::sync` on the store: redb's flush of the header it
    /// marks in use (`begin_writable`).
    Sync,
}

impl OpenSite {
    const ALL: [OpenSite; 4] = [
        OpenSite::Open,
        OpenSite::SyncDir,
        OpenSite::Read,
        OpenSite::Sync,
    ];

    fn error(self) -> io::Error {
        io::Error::other(format!("one-shot {self:?} failure injected by this test"))
    }

    /// What `Db::open` returns once the fault bubbles: `Store::open`'s own
    /// two calls pass the disk's error up as it is, redb's renders it as
    /// every other redb error is rendered, and `Db::open` names the site and
    /// the fix around either, keeping the text (hard rule 5).
    fn bubbled(self) -> String {
        let disk = match self {
            OpenSite::Open | OpenSite::SyncDir => self.error().to_string(),
            OpenSite::Read | OpenSite::Sync => {
                format!("table store (redb): I/O error: {}", self.error())
            }
        };
        format!(
            "table store tables.redb failed to open on an IO error, which says nothing about \
             the file's bytes, so the store was left as it is: {disk}. Restart to retry the \
             same file; if every restart fails the same way, the disk under it is failing \
             (DESIGN §4)"
        )
    }
}

/// A `Disk` that fails one store operation at `site`, once, then passes
/// everything through: a transient IO error a restart would not see again.
struct OneShotFault {
    inner: charpente_sim::SimDisk,
    site: OpenSite,
    fired: Arc<std::sync::atomic::AtomicBool>,
}

impl OneShotFault {
    fn fire(&self, site: OpenSite) -> io::Result<()> {
        if site == self.site && !self.fired.swap(true, Ordering::SeqCst) {
            return Err(site.error());
        }
        Ok(())
    }
}

impl Disk for OneShotFault {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        if path != "tables.redb" {
            return self.inner.open(path);
        }
        self.fire(OpenSite::Open)?;
        Ok(Box::new(OneShotFile {
            inner: self.inner.open(path)?,
            site: self.site,
            fired: self.fired.clone(),
        }))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        if prefix == "tables.redb" {
            self.fire(OpenSite::SyncDir)?;
        }
        self.inner.sync_dir(prefix)
    }
}

struct OneShotFile {
    inner: Box<dyn File>,
    site: OpenSite,
    fired: Arc<std::sync::atomic::AtomicBool>,
}

impl OneShotFile {
    fn fire(&self, site: OpenSite) -> io::Result<()> {
        if site == self.site && !self.fired.swap(true, Ordering::SeqCst) {
            return Err(site.error());
        }
        Ok(())
    }
}

impl File for OneShotFile {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.fire(OpenSite::Read)?;
        self.inner.read_at(offset, out)
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write_at(offset, data)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync(&self) -> io::Result<()> {
        self.fire(OpenSite::Sync)?;
        self.inner.sync()
    }
}

/// The disk a one-shot test opens through: the fault at `site`, watched.
fn one_shot_disk(
    sim: &Sim,
    site: OpenSite,
    fired: &Arc<std::sync::atomic::AtomicBool>,
    seen: &Arc<Mutex<StoreSeen>>,
) -> Box<dyn Disk> {
    Box::new(DiscardWitness {
        inner: Box::new(OneShotFault {
            inner: sim.disk(),
            site,
            fired: fired.clone(),
        }),
        seen: seen.clone(),
    })
}

/// Builds a committed, gracefully-closed store, then durably resizes its file
/// to `resize(old_len)` — the crash-collapse states where the store file's
/// length disagrees with its durable header's layout (an unsynced `set_len`
/// drops while the header god-page write survives, or a torn tail write
/// extends the file past the layout). redb 4.1.0 panicked at open on both
/// (page_manager.rs:231 / :257) and Store::open fenced the panic; redb 4.3.0
/// returns `StorageError::Corrupted`, and Db::open discards and rebuilds from
/// the WAL either way. Found by the kernel flagship swarm, seed
/// 0x64622c74f8660a09, under 4.1.0 (4.3.0's `grow()` syncs an extension
/// before any header records it, so the crash model no longer makes these).
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
    assert_rejected_as(&sim, Rejection::Corrupted);

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

/// Probes the store the next `Db::open` will meet with an independent
/// `Store::open`, and asserts redb rejects its bytes for `reason` — the only
/// kind of open error `Db::open` discards a store for (or, on a compacted log,
/// refuses to boot over). The probe fails before redb writes anything, so the
/// store `Db::open` meets is the one probed.
fn assert_rejected_as(sim: &Sim, reason: Rejection) {
    let e = Store::open(&sim.disk(), "tables.redb")
        .err()
        .expect("the damaged store must not open");
    assert_eq!(
        StoreRejected::of(&e).map(|r| r.reason),
        Some(reason),
        "the damaged store must be redb's verdict on its bytes, not an IO error: {e}"
    );
}

/// redb's two other verdicts, written by hand into a gracefully closed store:
/// a file that does not begin with redb's magic number, and a commit slot that
/// names file format 2 (byte 64 is slot 0's version, redb 4.3.0 header.rs).
const REWRITES: [(&str, u64, &[u8], Rejection); 2] = [
    ("not a redb file", 0, b"not redb!", Rejection::NotARedbFile),
    (
        "an older file format",
        64,
        &[2],
        Rejection::UpgradeRequired { version: 2 },
    ),
];

fn rewrite_store(sim: &Sim, offset: u64, bytes: &[u8]) {
    let file = sim.disk().open("tables.redb").unwrap();
    file.write_at(offset, bytes).unwrap();
    file.sync().unwrap();
}

/// On a log that still holds seq 0, both rewrites are discarded and rebuilt,
/// like the truncations below: the format-upgrade verdict is in the rejected
/// class too, because rebuilding from the whole log loses nothing.
#[test]
fn a_store_redb_rejects_as_not_redb_or_an_older_format_is_rebuilt_from_a_full_log() {
    for (what, offset, bytes, reason) in REWRITES {
        let (sim, mut db) = quiet_db(0x5E5F_0003);
        for i in 0..10u64 {
            db.transact(|tx| {
                tx.put(&invoice(i, 1, i as i64));
                Ok::<_, io::Error>(())
            })
            .unwrap();
        }
        db.checkpoint().unwrap();
        drop(db);
        rewrite_store(&sim, offset, bytes);
        assert_rejected_as(&sim, reason);

        let seen = Arc::new(Mutex::new(StoreSeen::default()));
        let db = Db::open(
            Box::new(DiscardWitness {
                inner: Box::new(sim.disk()),
                seen: seen.clone(),
            }),
            DbConfig::default(),
            app_schema(),
        )
        .unwrap_or_else(|e| panic!("{what}: a full log must rebuild the store: {e}"));
        let discards = seen.lock().expect("witness lock").discards.clone();
        assert_eq!(discards.len(), 1, "{what}: one discard: {discards:?}");
        assert!(
            !matches!(discards[0], Discard::AfterIoError(_)),
            "{what}: {discards:?}"
        );
        assert_eq!(
            ids(&db.snapshot().unwrap().scan::<Invoice>().unwrap()),
            (0..10).collect::<Vec<u64>>(),
            "{what}: the rebuilt store must replay the full WAL"
        );
    }
}

/// On a compacted log the same two rewrites refuse the boot by name, each
/// naming its own cause and fix (hard rule 5). No charpente release wrote a
/// format below 3, so the older-format text calls the version byte damage
/// and offers the restore, naming the one in-place upgrade redb has only for
/// a format-2 file something else wrote.
#[test]
fn a_store_redb_rejects_as_not_redb_or_an_older_format_is_refused_on_a_compacted_log() {
    for (what, offset, bytes, reason) in REWRITES {
        let cfg = tiny_segments_cfg();
        let sim = Sim::with_faults(0x5E5F_0004, FaultConfig::none());
        {
            let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
            for i in 0..20u64 {
                db.transact(|tx| {
                    tx.put(&invoice(i, 1, i as i64));
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }
            db.checkpoint().unwrap();
        }
        let base = sim
            .disk()
            .list("wal/")
            .unwrap()
            .iter()
            .map(|n| segment_base(n))
            .min()
            .unwrap();
        assert!(base > 0, "{what}: setup must compact the log");
        rewrite_store(&sim, offset, bytes);
        assert_rejected_as(&sim, reason);

        let err = Db::open(Box::new(sim.disk()), cfg, app_schema()).unwrap_err();
        let expected = match reason {
            Rejection::UpgradeRequired { .. } => format!(
                "table store cannot open (table store (redb): Manual upgrade required. Expected \
                 file format version 3, but file is version 2) and the log is compacted to seq \
                 {base} — redb reads the store as file format version 2, which it opens only \
                 after a manual upgrade, and the records below its last checkpoint no longer \
                 exist locally, so it cannot be rebuilt from the log alone. No charpente release \
                 has written a format below 3, so on a store this node created the version byte \
                 is damage: restore this node from a replica or backup. {RESTORE_IS_A_RESEED}. \
                 Only a format-2 file written by something else can be upgraded in place, with \
                 redb 2.6's `Database::upgrade()`; format 1 has no in-place upgrade (DESIGN §4)"
            ),
            _ => format!(
                "table store cannot open (table store (redb): I/O error: Not a redb database: \
                 magic number mismatch) and the log is compacted to seq {base} — the records \
                 below the store's last checkpoint no longer exist locally, so the store cannot \
                 be rebuilt from the log alone; restore this node from a replica or backup, or \
                 widen DbConfig::retain_segments. {RESTORE_IS_A_RESEED} (DESIGN §4)"
            ),
        };
        assert_eq!(err.to_string(), expected, "{what}");
    }
}

/// File shorter than the header's layout: redb 4.3.0's "File truncated below
/// stored layout" (4.1.0 asserted at page_manager.rs:231).
#[test]
fn store_shorter_than_header_layout_is_discarded_and_rebuilt() {
    mangled_store_len_recovers(|len| len - 4096);
}

/// File longer than the header's layout but not page-aligned: redb 4.3.0's
/// "File length does not correspond to a valid region layout" (4.1.0
/// recalculated the layout, then asserted equality at page_manager.rs:257).
#[test]
fn store_longer_than_header_layout_is_discarded_and_rebuilt() {
    mangled_store_len_recovers(|len| len + 100);
}

fn cut_store(sim: &Sim, len: u64) {
    let file = sim.disk().open("tables.redb").unwrap();
    file.set_len(len).unwrap();
    file.sync().unwrap();
}

/// A store cut below redb's header (review M1) beside a log that still holds
/// seq 0: the text names removing the file, and the fix holds, every row back
/// from the log (S5 R-I1, the full-log case).
#[test]
fn a_store_cut_short_beside_a_full_log_names_removal_and_removal_rebuilds_every_row() {
    let sim = Sim::with_faults(0x5E5F_0901, FaultConfig::none());
    let cfg = DbConfig::default();
    {
        let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
        for i in 0..20u64 {
            db.transact(|tx| {
                tx.put(&invoice(i, 1, i as i64));
                Ok::<_, io::Error>(())
            })
            .unwrap();
        }
        db.checkpoint().unwrap();
    }
    cut_store(&sim, 100);
    for boot in 0..2 {
        let e = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema())
            .expect_err("a store cut below redb's header must not open");
        assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof, "boot {boot}: {e}");
        let text = e.to_string();
        assert!(
            text.ends_with(
                "every boot will fail the same way. The log beside it still reaches seq 0, so \
                 remove tables.redb and restart: the next boot rebuilds the store from the log \
                 (DESIGN §4)"
            ),
            "boot {boot}: {text}"
        );
    }
    sim.disk().remove(&cfg.store_path).unwrap();
    let db = Db::open(Box::new(sim.disk()), cfg, app_schema())
        .unwrap_or_else(|e| panic!("the fix the text names must boot: {e}"));
    assert_eq!(
        ids(&db.snapshot().unwrap().scan::<Invoice>().unwrap()),
        (0..20).collect::<Vec<u64>>(),
        "the store rebuilt from a full log must hold every row"
    );
}

/// The same cut beside a log compacted past seq 0 (S5 R-I1, the reviewer's
/// repro: base 18, the store cut to 100 bytes). The store's last checkpoint
/// is the only copy of the records below the base, so the text names the
/// restore, not removal. An operator who removes the file anyway, as M1's
/// first text told them to, leaves the horizon breadcrumb behind; it used to
/// lift the recovery horizon past the vanished-prefix check, and the boot
/// panicked replaying seq 18 into an empty store. Every boot after the
/// removal now refuses by name.
#[test]
fn a_store_cut_short_beside_a_compacted_log_names_a_restore_and_removing_it_is_refused_by_name() {
    let sim = Sim::with_faults(0x5E5F_0902, FaultConfig::none());
    let cfg = tiny_segments_cfg();
    {
        let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
        for i in 0..20u64 {
            db.transact(|tx| {
                tx.put(&invoice(i, 1, i as i64));
                Ok::<_, io::Error>(())
            })
            .unwrap();
        }
        db.checkpoint().unwrap();
    }
    let base = segment_base(&sim.disk().list("wal/").unwrap()[0]);
    assert_eq!(
        base, 18,
        "setup must compact the log to the reviewer's base"
    );
    assert_eq!(
        sim.disk().list("tables.redb.horizon").unwrap(),
        ["tables.redb.horizon"],
        "setup must leave the breadcrumb the removal below leaves behind"
    );
    cut_store(&sim, 100);
    let e = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema())
        .expect_err("a store cut below redb's header must not open");
    assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof, "{e}");
    let text = e.to_string();
    assert!(
        text.ends_with(&format!(
            "every boot will fail the same way. The log beside it starts at seq 18, so this \
             store holds the only copy of the records below it and cannot be rebuilt: restore \
             this node from a replica or backup. {RESTORE_IS_A_RESEED} (DESIGN §4)"
        )),
        "{text}"
    );

    // The operator's sequence under M1's first text: remove the file, restart.
    sim.disk().remove(&cfg.store_path).unwrap();
    for boot in 0..2 {
        let e = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema())
            .expect_err("a compacted log beside no store must not boot");
        assert_eq!(
            e.to_string(),
            format!(
                "table store tables.redb holds no records, but the log beside it starts at seq \
                 18: the acknowledged records from seq 0 to seq 17 exist in neither the store \
                 nor the log, so the store cannot be rebuilt from the log. Removing the store by \
                 hand leaves this state, and so does a re-seed a crash cut short; restore this \
                 node from a replica or backup. {RESTORE_IS_A_RESEED} (DESIGN §4)"
            ),
            "boot {boot}"
        );
    }
}

/// The same cut beside an empty log: the store holds the only copy of every
/// record, so the text names the restore, and the restore carries the
/// re-seed's config step (S3b R3-M1).
#[test]
fn a_store_cut_short_beside_an_empty_log_names_a_restore_with_its_reseed_step() {
    let sim = Sim::with_faults(0x5E5F_0903, FaultConfig::none());
    let cfg = DbConfig::default();
    {
        let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
        for i in 0..5u64 {
            db.transact(|tx| {
                tx.put(&invoice(i, 1, i as i64));
                Ok::<_, io::Error>(())
            })
            .unwrap();
        }
        db.checkpoint().unwrap();
    }
    for name in sim.disk().list("wal/").unwrap() {
        sim.disk().remove(&name).unwrap();
    }
    sim.disk().sync_dir("wal/").unwrap();
    cut_store(&sim, 100);
    let e = Db::open(Box::new(sim.disk()), cfg, app_schema())
        .expect_err("a store cut below redb's header must not open");
    assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof, "{e}");
    let text = e.to_string();
    assert!(
        text.ends_with(&format!(
            "every boot will fail the same way. The log beside it is empty, so this store holds \
             the only copy of its records and cannot be rebuilt: restore this node from a \
             replica or backup. {RESTORE_IS_A_RESEED} (DESIGN §4)"
        )),
        "{text}"
    );
}

/// `Db::open`'s vanished-prefix refusal for a log starting at `base`, typed
/// out (error-suite T8): `held` is the store's `applied_seq`, `None` for a
/// store holding no records.
fn vanished_prefix_refusal(held: Option<u64>, base: u64) -> String {
    let next = held.map_or(0, |applied| applied + 1);
    let (held, cause) = match held {
        None => (
            "holds no records".to_string(),
            "Removing the store by hand leaves this state, and so does a re-seed a crash cut \
             short",
        ),
        Some(applied) => (
            format!(
                "holds records only through seq {applied}, the durable horizon its own \
                 checkpoint vouches for"
            ),
            "Putting back an older copy of the store without its log leaves this state, and so \
             do corruption and entry loss",
        ),
    };
    format!(
        "table store tables.redb {held}, but the log beside it starts at seq {base}: the \
         acknowledged records from seq {next} to seq {} exist in neither the store nor the log, \
         so the store cannot be rebuilt from the log. {cause}; restore this node from a replica \
         or backup. {RESTORE_IS_A_RESEED} (DESIGN §4)",
        base - 1
    )
}

/// An older copy of the store put back behind a log compacted past it (S5
/// R2-M1, the reviewer's probe): the store checkpointed at seq 19 is copied,
/// the node runs on to a checkpoint at seq 39 that compacts the log to seq 38,
/// and the copy is put back. The breadcrumb still says 39, and the
/// vanished-prefix check used to measure against it, so the boot passed the
/// check and panicked replaying seq 38 into a store that ends at 19
/// ("Store::apply_batch reached seq 38 but expected 20"). Without the
/// breadcrumb it was refused by name. It is refused by name either way now,
/// and so is the same store beside an empty log, which used to lay its
/// segment at the breadcrumb's seq 40 and panic at the first write.
#[test]
fn an_older_store_put_back_behind_a_compacted_log_is_refused_by_name_whatever_the_breadcrumb_says()
{
    for shape in ["breadcrumb", "no breadcrumb", "empty log"] {
        let sim = Sim::with_faults(0x5E5F_1001, FaultConfig::none());
        let cfg = tiny_segments_cfg();
        {
            let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
            for i in 0..20u64 {
                db.transact(|tx| {
                    tx.put(&invoice(i, 1, i as i64));
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }
            db.checkpoint().unwrap();
        }
        let file = sim.disk().open(&cfg.store_path).unwrap();
        let mut older = vec![0u8; file.len().unwrap() as usize];
        file.read_at(0, &mut older).unwrap();
        drop(file);
        {
            let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
            for i in 20..40u64 {
                db.transact(|tx| {
                    tx.put(&invoice(i, 1, i as i64));
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }
            db.checkpoint().unwrap();
            assert_eq!(
                db.log_base(),
                38,
                "{shape}: setup must compact the log past seq 20"
            );
        }
        let file = sim.disk().open(&cfg.store_path).unwrap();
        file.set_len(older.len() as u64).unwrap();
        file.write_at(0, &older).unwrap();
        file.sync().unwrap();
        drop(file);
        let expected = match shape {
            "breadcrumb" => vanished_prefix_refusal(Some(19), 38),
            "no breadcrumb" => {
                sim.disk().remove("tables.redb.horizon").unwrap();
                sim.disk().sync_dir("tables.redb").unwrap();
                vanished_prefix_refusal(Some(19), 38)
            }
            _ => {
                for name in sim.disk().list("wal/").unwrap() {
                    sim.disk().remove(&name).unwrap();
                }
                sim.disk().sync_dir("wal/").unwrap();
                "table store tables.redb holds records only through seq 19, the durable horizon \
                 its own checkpoint vouches for, but the log beside it is empty, and the horizon \
                 breadcrumb records a checkpoint through seq 39: the acknowledged records from \
                 seq 20 to seq 39 exist in neither the store nor the log, so the store cannot be \
                 rebuilt from the log. Putting back an older copy of the store without its log \
                 leaves this state, and so do corruption and entry loss; restore this node from \
                 a replica or backup. For a follower, or a primary once a follower is promoted \
                 in its place, that is a re-seed: stop the node, delete its data dir, and restart \
                 it configured as a follower of the current primary so it snapshot-seeds from \
                 it, never with a primary config: the durable role file that outranks config is \
                 deleted with the data dir, so a cleared node started as a primary comes up as a \
                 second writable primary (DESIGN §4)"
                    .to_string()
            }
        };
        for boot in 0..2 {
            let e = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema())
                .expect_err("a store that stops short of the log's base must not boot");
            assert_eq!(e.to_string(), expected, "{shape}, boot {boot}");
        }
    }
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

    db.add_retention_pin(Box::new(|| 0));
    db.checkpoint().unwrap();

    let after = sim.disk().list("wal/").unwrap().len();
    assert_eq!(
        after, before,
        "a pin at seq 0 must block every deletion: {before} -> {after}"
    );
}

#[test]
fn retention_pins_compose_as_a_min() {
    let (_sim, mut db) = quiet_db(0x3);
    let a = Arc::new(AtomicU64::new(50));
    let b = Arc::new(AtomicU64::new(20));
    db.add_retention_pin(Box::new({
        let a = Arc::clone(&a);
        move || a.load(Ordering::Relaxed)
    }));
    db.add_retention_pin(Box::new({
        let b = Arc::clone(&b);
        move || b.load(Ordering::Relaxed)
    }));

    assert_eq!(db.retention_floor(), 20, "floor must be the min over pins");
    b.store(90, Ordering::Relaxed);
    assert_eq!(
        db.retention_floor(),
        50,
        "floor must track the new min, not the first pin"
    );
    a.store(u64::MAX, Ordering::Relaxed);
    b.store(u64::MAX, Ordering::Relaxed);
    assert_eq!(
        db.retention_floor(),
        u64::MAX,
        "no pin holding means no floor"
    );
}

#[test]
fn no_registered_pins_means_no_floor() {
    let (_sim, db) = quiet_db(0x4);
    assert_eq!(
        db.retention_floor(),
        u64::MAX,
        "an unpinned db must not hold any segment"
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
    let base = sim
        .disk()
        .list("wal/")
        .unwrap()
        .iter()
        .map(|n| segment_base(n))
        .min()
        .unwrap();
    assert!(
        base > 0,
        "setup must actually compact the log before mangling the store"
    );
    // Graceful close: redb durably flushes, so header layout == file length.
    drop(db);

    let file = sim.disk().open("tables.redb").unwrap();
    let len = file.len().unwrap();
    file.set_len(len - 4096).unwrap();
    file.sync().unwrap();
    drop(file);
    assert_rejected_as(&sim, Rejection::Corrupted);

    let err = Db::open(Box::new(sim.disk()), cfg, app_schema()).unwrap_err();
    let msg = err.to_string();
    // Needle + known-value, not byte-exact (error-suite T8): db.rs's `{e}`
    // embeds redb's own `Corrupted` text, whose lengths are redb's layout
    // arithmetic (upstream, version-tied, not ours to pin). Everything else
    // is a scenario-known format arg (the compacted base), so it's checked
    // byte-exact around that one opaque span. The redb needle is what shows
    // the open failed as an ordinary `Err` (redb 4.3.0), not a caught panic.
    assert!(
        msg.starts_with("table store cannot open ("),
        "error must open with the named state: {msg}"
    );
    assert!(
        msg.contains("(table store (redb): DB corrupted: File truncated below stored layout"),
        "the embedded cause must be redb's own corruption verdict: {msg}"
    );
    assert!(
        msg.ends_with(&format!(
            ") and the log is compacted to seq {base} — the records below the store's last \
             checkpoint no longer exist locally, so the store cannot be rebuilt from the log \
             alone; restore this node from a replica or backup, or widen \
             DbConfig::retain_segments. {RESTORE_IS_A_RESEED} (DESIGN §4)"
        )),
        "error must name the compacted base {base} and the fix: {msg}"
    );
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
    // Byte-exact (error-suite T8): the corrupted frame is always frame 0 (a
    // fixed 16-byte header, corrupted within it regardless of payload size),
    // and the checkpoint above durably pins the horizon at 9 — both format
    // args are scenario-known, so log.rs's corruption-of-acknowledged-records
    // string reconstructs in full.
    assert_eq!(
        err.to_string(),
        format!(
            "log frame 0 is invalid but at-or-below the store's durable horizon 9 — this is \
             corruption of acknowledged records, not a torn tail, and truncating it would \
             silently lose data; restore this node from a replica or backup. \
             {RESTORE_IS_A_RESEED} (DESIGN §2)"
        )
    );
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

    let base = segment_base(&sim.disk().list("wal/").unwrap()[0]);
    let err = Db::open(Box::new(sim.disk()), cfg, app_schema()).unwrap_err();
    assert_eq!(err.to_string(), vanished_prefix_refusal(Some(5), base));
}

#[test]
fn unopenable_store_with_full_wal_still_rebuilds() {
    // Same mangling as `mangled_store_len_recovers`, but with no checkpoint
    // or GC ever run (DbConfig::default()'s huge segment_bytes means only
    // one segment ever exists, so base_seq stays 0): the pre-T3
    // discard-and-rebuild path must still work unchanged.
    mangled_store_len_recovers(|len| len - 4096);
}

/// A store that `Store::open` could not open for a reason other than its bytes
/// — one transient IO error, at any of the four places a clean open can meet
/// one — is not discarded: `Db::open` returns the disk's error, naming the site
/// and the fix, and the retry opens the same store file. Before S5 (follow-up 21), any `Store::open`
/// error was read as "this file is damaged beyond use", so on a log still
/// holding seq 0 this deleted a healthy store and rebuilt it from the whole log.
#[test]
fn a_transient_store_open_fault_on_a_full_log_discards_nothing_and_the_retry_opens_it() {
    for site in OpenSite::ALL {
        let sim = Sim::with_faults(0x5E5F_0001, FaultConfig::none());
        let cfg = DbConfig::default();
        {
            let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
            for i in 0..10u64 {
                db.transact(|tx| {
                    tx.put(&invoice(i, 1, i as i64));
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }
            db.checkpoint().unwrap();
        }
        let first = sim.disk().list("wal/").unwrap();
        assert_eq!(
            first.first().map(|n| segment_base(n)),
            Some(0),
            "{site:?}: setup must keep the whole log, so the old code's discard-and-rebuild \
             was available to it"
        );

        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = Arc::new(Mutex::new(StoreSeen::default()));
        let opened = Db::open(
            one_shot_disk(&sim, site, &fired, &seen),
            cfg.clone(),
            app_schema(),
        );
        assert!(
            fired.load(Ordering::SeqCst),
            "{site:?}: the one-shot fault never fired, so this proves nothing about it"
        );
        let discards = seen.lock().expect("witness lock").discards.clone();
        match opened {
            Ok(_) => panic!(
                "{site:?}: Db::open answered Ok over a one-shot {site:?} fault, having discarded \
                 the store and rebuilt it from the log (discards: {discards:?}). A transient IO \
                 error is not a verdict on the file's bytes: the store must survive it and the \
                 error must bubble, so a restart retries (DESIGN §4)"
            ),
            Err(e) => assert_eq!(
                e.to_string(),
                site.bubbled(),
                "{site:?}: Db::open must return the disk's error, named, not a refusal"
            ),
        }
        assert_eq!(
            discards,
            Vec::<Discard>::new(),
            "{site:?}: the store must not be discarded over a transient fault"
        );

        let db = Db::open(one_shot_disk(&sim, site, &fired, &seen), cfg, app_schema())
            .unwrap_or_else(|e| panic!("{site:?}: the retry must open the store: {e}"));
        let seen = seen.lock().expect("witness lock");
        assert_eq!(
            (seen.discards.clone(), seen.retried.clone()),
            (Vec::new(), vec![site.error().to_string()]),
            "{site:?}: the retry must reopen the same store file, not a rebuilt one"
        );
        let snap = db.snapshot().unwrap();
        assert_eq!(
            ids(&snap.scan::<Invoice>().unwrap()),
            (0..10).collect::<Vec<u64>>(),
            "{site:?}: every committed record must be present after the retry"
        );
    }
}

/// The compacted-log half: before S5, one transient IO error inside
/// `Store::open` on a log compacted past seq 0 made the node refuse to boot
/// with "restore this node from a replica or backup" — the wrong fix for a
/// fault a restart retries (found by the S4 review, Inversion C': an injected
/// fsync failure inside `Store::open`, replication seed 0x8be1b31e61e52e62).
/// Now the error bubbles, named, and the retry opens with every committed
/// record present, including the ones that exist only in the checkpointed store.
#[test]
fn a_transient_store_open_fault_on_a_compacted_log_bubbles_and_the_retry_opens_it() {
    for site in OpenSite::ALL {
        let cfg = tiny_segments_cfg();
        let sim = Sim::with_faults(0x5E5F_0002, FaultConfig::none());
        {
            let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
            for i in 0..20u64 {
                db.transact(|tx| {
                    tx.put(&invoice(100 + i, 1, i as i64));
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }
            db.checkpoint().unwrap();
        }
        let base = sim
            .disk()
            .list("wal/")
            .unwrap()
            .iter()
            .map(|n| segment_base(n))
            .min()
            .unwrap();
        assert!(
            base > 0,
            "{site:?}: setup must compact the log, so the store cannot be rebuilt from it"
        );

        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = Arc::new(Mutex::new(StoreSeen::default()));
        let err = Db::open(
            one_shot_disk(&sim, site, &fired, &seen),
            cfg.clone(),
            app_schema(),
        )
        .expect_err("the faulted open must fail");
        assert!(
            fired.load(Ordering::SeqCst),
            "{site:?}: the one-shot fault never fired, so this proves nothing about it"
        );
        assert_eq!(
            err.to_string(),
            site.bubbled(),
            "{site:?}: a transient fault must bubble with the disk's own error; refusing to boot \
             with \"restore this node from a replica or backup\" sends the operator to the \
             wrong fix for an error a restart retries"
        );

        let db = Db::open(one_shot_disk(&sim, site, &fired, &seen), cfg, app_schema())
            .unwrap_or_else(|e| panic!("{site:?}: the retry must open the store: {e}"));
        assert_eq!(
            seen.lock().expect("witness lock").discards,
            Vec::<Discard>::new(),
            "{site:?}: a compacted log never discards"
        );
        let snap = db.snapshot().unwrap();
        assert_eq!(
            ids(&snap.scan::<Invoice>().unwrap()),
            (100..120).collect::<Vec<u64>>(),
            "{site:?}: every committed record must be present, including those below the \
             compacted base, which only the store holds"
        );
    }
}

/// The same fault inside a seeded swarm universe of
/// `transactions_survive_the_fault_matrix`, observed by its reason. Seed
/// `0x894c4c28968f4c96` (scan base `0x5E5FC000`, universe 16; before S5, 166 of
/// those 256 universes discarded a store after an injected IO error) first
/// meets an injected fault inside `Store::open` as a simulated fsync failure,
/// on a log that still holds seq 0. The old `Db::open` discarded that store and
/// rebuilt it; now the error bubbles and `tx_workload` restarts and reopens the
/// same file. Up to that fault the universe is the same either way, so the
/// first retried open names it.
#[test]
fn a_swarm_universe_whose_store_open_hits_an_injected_fault_retries_and_discards_nothing() {
    const SEED: u64 = 0x894c_4c28_968f_4c96;
    let mut state = SEED;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut cfg = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(SEED, &mut cfg);
    let mut sim = Sim::with_faults(SEED, cfg);
    let seen = Arc::new(Mutex::new(StoreSeen::default()));
    let witness = seen.clone();
    let result = tx_workload_on(&mut sim, &move |sim: &Sim| -> Box<dyn Disk> {
        Box::new(DiscardWitness {
            inner: Box::new(sim.disk()),
            seen: witness.clone(),
        })
    });
    assert_eq!(result, Ok(()), "seed {SEED:#x}");
    let seen = seen.lock().expect("witness lock");
    let after_io: Vec<&Discard> = seen
        .discards
        .iter()
        .filter(|d| matches!(d, Discard::AfterIoError(_)))
        .collect();
    assert!(
        after_io.is_empty(),
        "seed {SEED:#x}: Db::open discarded a store whose open had hit an IO error: \
         {after_io:?}. Only redb rejecting the file's bytes may discard it; a transient \
         fault must bubble so a restart retries (DESIGN §4)"
    );
    assert_eq!(
        seen.retried.first().map(String::as_str),
        Some(
            "simulated fsync failure — durability of writes since the last successful sync is unknown"
        ),
        "seed {SEED:#x} no longer meets an injected fsync failure inside Store::open first \
         (retried: {:?}). A seed names a universe, not a scenario: re-find one that reaches \
         this reason (hard rule 3)",
        seen.retried
    );
}

/// Companion to `regression_store_bricked_by_creation_crash` above: that
/// test's pinned seed exercises the scenario through `tx_workload`'s much
/// larger fuzz surface (3 rounds of commits/checkpoints/crashes), and any
/// change that moves crash timing renames its universe — T1's extra
/// `sync_dir` barriers, this task's own extra `disk.list` in the rebuild-rule
/// check, and redb 4.3.0's IO shape all did (it was re-found by its cause on
/// 2026-09-30; see its doc). This test instead constructs the scenario directly and
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
    let bricked = Store::open(&sim.disk(), &cfg.store_path).err().expect(
        "setup must independently confirm the store is bricked before Db::open is asked to fix it",
    );
    assert!(
        StoreRejected::of(&bricked).is_some(),
        "the bricked store must be one redb rejects by its bytes — the only open error Db::open \
         discards a store for — not an IO error a retry could clear: {bricked}"
    );

    let db = Db::open(Box::new(sim.disk()), cfg, Schema::new())
        .expect("Db::open must discard and rebuild a store bricked at creation");
    assert_eq!(
        db.last_applied(),
        None,
        "nothing was ever durably committed to this fresh db"
    );
}

// ---------------- M1 horizon-breadcrumb interlude (pre-SP6): horizon collapse
// regression + breadcrumb properties

/// Headline regression (`.superpowers/sdd/horizon-collapse-investigation.md`):
/// `Db::open`'s store discard-and-rebuild used to erase the ONLY recovery-
/// horizon witness (the store's own `applied_seq`), so a durable corruption
/// of an already-acked, already-checkpointed WAL frame was silently CUT
/// instead of raising the named corruption-of-acknowledged-records hard
/// error — the store resurrected EMPTY, with no error at all. Design A
/// (Noah-ruled, progress.md line 252): a durable horizon breadcrumb outside
/// the store survives the discard and restores the witness. The control
/// assertion (store intact) passes on both fixed and unfixed code; only the
/// discard branch is the regression this test pins.
#[test]
fn store_discard_erases_the_horizon_but_the_breadcrumb_still_refuses_the_corrupt_frame() {
    let sim = Sim::with_faults(0x486F_72C0, FaultConfig::none());
    let cfg = DbConfig::default();

    // 20 rows, one transaction each -> seq 0..19; checkpoint durably pins
    // applied_seq = 19 (the horizon) and, with the fix, the breadcrumb too.
    // One 64 MiB segment never rotates, so base_seq stays 0 -- the exact
    // reachable window (base_seq > 0 is already refused earlier).
    {
        let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
        for i in 0..20u64 {
            db.transact(|tx| {
                tx.put(&invoice(i, 1, i as i64));
                Ok::<_, io::Error>(())
            })
            .unwrap();
        }
        db.checkpoint().unwrap();
    }

    // Durably corrupt the first WAL frame's payload (offset 16, past the
    // 16-byte header) -- media bitrot of an already-acked, already-applied,
    // already-checkpointed record.
    {
        let segments = sim.disk().list("wal/").unwrap();
        let file = sim.disk().open(&segments[0]).unwrap();
        let mut byte = [0u8; 1];
        file.read_at(16, &mut byte).unwrap();
        file.write_at(16, &[byte[0] ^ 0xFF]).unwrap();
        file.sync().unwrap();
    }

    // Byte-exact (error-suite T8): the corrupted frame is always frame 0, and
    // this checkpoint durably pins the horizon at 19 (20 rows, seq 0..19) —
    // both format args are scenario-known, whether the horizon comes from the
    // intact store's applied_seq (control) or the breadcrumb (discard, below).
    let expected = format!(
        "log frame 0 is invalid but at-or-below the store's durable horizon 19 — this is \
         corruption of acknowledged records, not a torn tail, and truncating it would silently \
         lose data; restore this node from a replica or backup. {RESTORE_IS_A_RESEED} \
         (DESIGN §2)"
    );

    // Control: store INTACT. applied_seq = 19 is the durable horizon; the
    // corrupt frame 0 is at-or-below it, so recovery refuses by name. This
    // is correct today and must keep passing after the fix.
    let intact_err = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema())
        .expect_err("store intact: corruption of an acked frame must be a hard error");
    assert_eq!(
        intact_err.to_string(),
        expected,
        "control (store intact) should name the contract"
    );

    // THE BUG: discard the store (the exact proxy the landed discard/
    // rebuild regressions use elsewhere in this file -- production reaches
    // the identical applied_seq=None state via Db::open's Store::open-failed
    // discard-and-rebuild branch, e.g. a store redb refuses to open). Horizon
    // collapses to None UNLESS a breadcrumb outside the store supplies it.
    sim.disk().remove(&cfg.store_path).unwrap();
    match Db::open(Box::new(sim.disk()), cfg, app_schema()) {
        Err(e) => {
            assert_eq!(
                e.to_string(),
                expected,
                "expected the named corruption-of-acknowledged-records refusal"
            );
        }
        Ok(db) => {
            let rows = db.snapshot().unwrap().scan::<Invoice>().unwrap();
            panic!(
                "HORIZON-COLLAPSE SILENT LOSS: after discarding the store, Db::open returned \
                 Ok with {} rows (expected the corrupt acked frame to refuse by name, exactly \
                 like the intact-store control above). Discarding the store erased the only \
                 durable horizon witness, and no breadcrumb protected it.",
                rows.len()
            );
        }
    }
}

/// Crash-point sweep: the breadcrumb write must be crash-safe. Seq 0..4 are
/// durably checkpointed (and breadcrumbed) BEFORE any fault injection begins;
/// a SECOND checkpoint (seq 5..9) is then interrupted at every possible disk
/// op via `crash_disk_after`, with `crash_torn: 1.0` so every pending op that
/// survives the crash survives as a genuine partial (torn) write — including
/// a torn overwrite of the breadcrumb's target slot. Regardless of exactly
/// when the crash landed, the FIRST checkpoint's witness (seq 4) must never
/// be lost: it was durable, breadcrumbed, and barriered before the sweep's
/// fault injection even started, so the OTHER (untouched) slot always still
/// holds it even if this checkpoint's slot write was torn mid-flight.
#[test]
fn breadcrumb_write_is_crash_safe_the_old_checkpoint_stays_protected() {
    let crash_cfg = FaultConfig {
        crash_torn: 1.0,
        ..FaultConfig::none()
    };
    for crash_after in 0..40u64 {
        let sim = Sim::with_faults(0x0EA5_0002, crash_cfg.clone());
        let cfg = DbConfig::default();
        {
            let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
            for i in 0..5u64 {
                db.transact(|tx| {
                    tx.put(&invoice(i, 1, i as i64));
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }
            db.checkpoint().unwrap(); // durable: applied_seq = 4, breadcrumb = 4
            for i in 5..10u64 {
                db.transact(|tx| {
                    tx.put(&invoice(i, 1, i as i64));
                    Ok::<_, io::Error>(())
                })
                .unwrap();
            }
            sim.crash_disk_after(crash_after);
            let _ = db.checkpoint(); // may Err mid-way; only what survives matters
        }
        sim.restart();

        // Corrupt WAL frame 0 (at-or-below the OLD horizon of 4 regardless
        // of whether the second checkpoint advanced it) and force the
        // vulnerable path by discarding the store outright.
        let segments = sim.disk().list("wal/").unwrap();
        let file = sim.disk().open(&segments[0]).unwrap();
        let mut byte = [0u8; 1];
        file.read_at(16, &mut byte).unwrap();
        file.write_at(16, &[byte[0] ^ 0xFF]).unwrap();
        file.sync().unwrap();
        sim.disk().remove(&cfg.store_path).unwrap();

        let err = Db::open(Box::new(sim.disk()), cfg, app_schema()).expect_err(&format!(
            "crash_after={crash_after}: the pre-existing checkpoint at seq 4 must stay \
             protected regardless of exactly when the SECOND checkpoint's breadcrumb write \
             was interrupted"
        ));
        assert!(
            err.to_string()
                .contains("corruption of acknowledged records"),
            "crash_after={crash_after}: expected the named refusal; got: {err}"
        );
    }
}

/// Legitimate above-horizon torn tails must still cut silently — the
/// breadcrumb must never turn into a false-positive refusal machine.
/// Checkpoint pins the breadcrumb at seq 9; seq 10 is committed but never
/// checkpointed, so it sits strictly ABOVE the horizon. Corrupting it and
/// discarding the store must still open cleanly, replaying only 0..=9.
#[test]
fn torn_tail_above_the_breadcrumb_still_cuts_silently() {
    let sim = Sim::with_faults(0x0EA5_0003, FaultConfig::none());
    let cfg = DbConfig::default();
    {
        let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
        for i in 0..10u64 {
            db.transact(|tx| {
                tx.put(&invoice(i, 1, i as i64));
                Ok::<_, io::Error>(())
            })
            .unwrap();
        }
        db.checkpoint().unwrap(); // breadcrumb = 9
        db.transact(|tx| {
            tx.put(&invoice(10, 1, 10));
            Ok::<_, io::Error>(())
        })
        .unwrap(); // seq 10, never checkpointed -- strictly above the horizon
    }

    // Corrupt frame 10's payload (past its header) so it fails verification.
    let segments = sim.disk().list("wal/").unwrap();
    let file = sim.disk().open(&segments[0]).unwrap();
    // Frame layout: 16-byte header + payload, repeated. Frame 10 starts
    // after ten prior (header + payload)-sized frames of identical shape
    // (fixed-size Invoice rows), so scan forward by reading frame lengths
    // rather than hand-computing an offset.
    let mut offset = 0u64;
    for _ in 0..10 {
        let mut len_bytes = [0u8; 4];
        file.read_at(offset + 8, &mut len_bytes).unwrap();
        offset += 16 + u32::from_le_bytes(len_bytes) as u64;
    }
    let mut byte = [0u8; 1];
    file.read_at(offset + 16, &mut byte).unwrap();
    file.write_at(offset + 16, &[byte[0] ^ 0xFF]).unwrap();
    file.sync().unwrap();

    sim.disk().remove(&cfg.store_path).unwrap();
    let db = Db::open(Box::new(sim.disk()), cfg, app_schema())
        .expect("an above-horizon torn tail must still cut silently, breadcrumb or not");
    let rows = db.snapshot().unwrap().scan::<Invoice>().unwrap();
    assert_eq!(
        ids(&rows),
        (0..10).collect::<Vec<_>>(),
        "recovery must cut exactly at the corrupt above-horizon frame, keeping 0..=9"
    );
}

/// Benign rebuild with a breadcrumb present: a clean, uncorrupted log must
/// replay in FULL after a store discard, even though the breadcrumb now
/// feeds a non-`None` horizon into recovery. Proves the new mechanism adds
/// no false-positive refusal on the ordinary (nothing actually corrupted)
/// path -- the pre-existing discard/rebuild tests in this file never
/// exercise the breadcrumb at all, since none of them ever checkpoint.
#[test]
fn store_discard_with_a_breadcrumb_and_a_clean_log_replays_everything() {
    let sim = Sim::with_faults(0x0EA5_0004, FaultConfig::none());
    let cfg = DbConfig::default();
    {
        let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
        for i in 0..10u64 {
            db.transact(|tx| {
                tx.put(&invoice(i, 1, i as i64));
                Ok::<_, io::Error>(())
            })
            .unwrap();
        }
        db.checkpoint().unwrap(); // breadcrumb = 9
        for i in 10..15u64 {
            db.transact(|tx| {
                tx.put(&invoice(i, 1, i as i64));
                Ok::<_, io::Error>(())
            })
            .unwrap();
        }
    }

    sim.disk().remove(&cfg.store_path).unwrap();
    let db = Db::open(Box::new(sim.disk()), cfg, app_schema())
        .expect("a clean, uncorrupted log must still rebuild in full");
    let rows = db.snapshot().unwrap().scan::<Invoice>().unwrap();
    assert_eq!(
        ids(&rows),
        (0..15).collect::<Vec<_>>(),
        "every row, checkpointed or not, must survive a benign discard-and-rebuild"
    );
}

/// A seeded node whose store is gone (S5 R-I1). `Seeder::finish` lays the
/// log at `applied_seq + 1`, so a seeded node's log never reaches seq 0 and
/// its store holds the only copy of the seed. This test used to pin that the
/// breadcrumb `finish` writes lifted the horizon far enough for a corrupt
/// frame at it to refuse as corruption. Without the corrupt frame the same
/// state replayed seq 1 into an empty store and panicked, so the store's loss
/// is now refused first, by name, before recovery reads a frame: nothing can
/// rebuild the seed, whatever the frames hold.
#[test]
fn a_seeded_node_whose_store_is_gone_is_refused_before_recovery_reads_a_frame() {
    let sim = Sim::with_faults(0x0EA5_0005, FaultConfig::none());
    let cfg = DbConfig::default();

    let mut seeder = Seeder::new(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
    let mut bytes = Vec::new();
    invoice(1, 1, 100).encode_row(&mut bytes);
    seeder.rows(Invoice::TABLE, &[(0, 1, bytes)]).unwrap();
    let mut db = seeder.finish(0, 0, 0, 0).unwrap(); // seeded at applied_seq = 0

    // More traffic on top, then checkpoint again -- breadcrumb advances.
    db.transact(|tx| {
        tx.put(&invoice(2, 1, 200));
        Ok::<_, io::Error>(())
    })
    .unwrap();
    db.checkpoint().unwrap(); // breadcrumb = 1
    drop(db);

    // Corrupt the one frame in the seeded segment: `Seeder::finish` lays the
    // segment at base_seq = applied_seq + 1 = 1, so the post-seed
    // transaction above (seq 1) sits at offset 0 within it.
    let segments = sim.disk().list("wal/").unwrap();
    let file = sim.disk().open(&segments[0]).unwrap();
    let mut byte = [0u8; 1];
    file.read_at(16, &mut byte).unwrap();
    file.write_at(16, &[byte[0] ^ 0xFF]).unwrap();
    file.sync().unwrap();
    sim.disk().remove(&cfg.store_path).unwrap();

    let err = Db::open(Box::new(sim.disk()), cfg, app_schema())
        .expect_err("a seeded node whose store is gone must not boot");
    // Byte-exact (error-suite T8): the seed segment's base is 1.
    assert_eq!(err.to_string(), vanished_prefix_refusal(None, 1));
}

/// `Seeder::new` must clear a stale breadcrumb left behind by a PRIOR,
/// higher-numbered timeline sharing this disk (e.g. a demoted primary being
/// re-seeded in place) -- otherwise the stale (high) value would survive the
/// reseed and the read side's `max` would over-claim protection for seqs the
/// new timeline never actually acknowledged, misfiring the corruption
/// refusal on an ordinary, legitimate torn tail.
#[test]
fn seeder_new_clears_a_stale_breadcrumb_so_reseed_never_over_claims() {
    let sim = Sim::with_faults(0x0EA5_0006, FaultConfig::none());
    let cfg = DbConfig::default();

    // Old timeline: push the breadcrumb up to a HIGH value (4).
    {
        let mut db = Db::open(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
        for i in 0..5u64 {
            db.transact(|tx| {
                tx.put(&invoice(i, 1, i as i64));
                Ok::<_, io::Error>(())
            })
            .unwrap();
        }
        db.checkpoint().unwrap(); // stale breadcrumb = 4, if left behind
    }

    // Reseed the SAME disk at a much LOWER position (0): the old store,
    // wal/, and (with the fix) the stale breadcrumb are all discarded.
    let seeder = Seeder::new(Box::new(sim.disk()), cfg.clone(), app_schema()).unwrap();
    let mut db = seeder.finish(0, 0, 0, 0).unwrap(); // fresh timeline, applied_seq = 0

    // One more, UNCHECKPOINTED transaction: seq 1, strictly above the new
    // (low) horizon of 0. If the stale breadcrumb (4) had survived, seq 1
    // would be wrongly treated as at-or-below it and refused as corruption
    // instead of cut as an ordinary above-horizon torn tail.
    db.transact(|tx| {
        tx.put(&invoice(1, 1, 1));
        Ok::<_, io::Error>(())
    })
    .unwrap();
    // Power is lost before the store closes, so it comes back at the seed's
    // checkpoint, applied_seq 0: seq 1's apply was never checkpointed, and a
    // graceful close would have persisted it. (Removing the store, as this
    // test once did, now refuses by name before recovery reads a frame: the
    // log starts at seq 1, so nothing could rebuild the seed, S5 R-I1.)
    sim.crash_disk_now();
    drop(db);
    sim.restart();

    // `Seeder::finish` lays the segment at base_seq = applied_seq + 1 = 1, so
    // the post-seed transaction above (seq 1) sits at offset 0 within it.
    let segments = sim.disk().list("wal/").unwrap();
    let file = sim.disk().open(&segments[0]).unwrap();
    let mut byte = [0u8; 1];
    file.read_at(16, &mut byte).unwrap();
    file.write_at(16, &[byte[0] ^ 0xFF]).unwrap();
    file.sync().unwrap();

    let db = Db::open(Box::new(sim.disk()), cfg, app_schema()).expect(
        "the stale breadcrumb must not survive Seeder::new -- an ordinary above-horizon torn \
         tail must still cut silently, not be misfired as corruption",
    );
    let rows = db.snapshot().unwrap().scan::<Invoice>().unwrap();
    assert!(
        rows.is_empty(),
        "seq 1 was never checkpointed in the new timeline and must cut silently: {:?}",
        ids(&rows)
    );
}

/// A re-seed of a compacted node that a crash cuts short (S5 R-I1, the crash
/// model's half). `Seeder::new` removes the store, then the horizon
/// breadcrumb, then the log, barriering each removal before the next, so a
/// crash between the first barrier and the second leaves no store, a
/// breadcrumb, and a log compacted past seq 0. Nothing in that state can be
/// rebuilt: the records below the log's base lived only in the store.
///
/// Each universe crashes the re-seed at a disk op drawn from its seed and
/// power-cycles. Before the boot, it reads what the crash left: the store's
/// records (read from a copy, so the boot meets the bytes untouched), whether
/// the breadcrumb survived, and the log's base. That state names the one
/// outcome the boot may have (S5 re-review 2, R2-M2):
/// - a log based past the store's own next seq is refused with the exact
///   vanished-prefix text, whatever the breadcrumb says;
/// - otherwise the store and log boot the old timeline whole, or the new one
///   at `RESEED_AT`;
/// - with no log and no records, the node boots fresh.
///
/// A different outcome fails the universe, and so does a panic or a state
/// the crash model should not leave. The fresh outcome used to accept any
/// empty boot, so a `Db::open` that deleted the log and booted empty beside
/// a breadcrumb passed the batch, and only the seed pin caught it.
/// Measured at this base: 76 old, 40 new, 110 fresh, 30 refused (6 of them
/// beside the breadcrumb).
#[test]
fn a_reseed_cut_short_by_a_crash_boots_a_whole_timeline_or_refuses_by_name() {
    run_universes_swarm_with!(
        0x5EED_C0A1,
        256,
        |_seed: u64, cfg: &mut FaultConfig| crash_model_only(cfg),
        |sim: &mut Sim| {
            let (base, crash_at) = crash_a_reseed(sim)?;
            // The reads below are the oracle's, not the node's: no fault may
            // roll on them, and none is left to roll on the boot either.
            sim.quiesce_disk_faults();
            let left = reseed_left(sim)?;
            let expected = left.expected_boot().map_err(|e| {
                format!(
                    "a re-seed of a log compacted to seq {base}, crashed at disk op {crash_at}: {e}"
                )
            })?;
            let got = match Db::open(Box::new(sim.disk()), tiny_segments_cfg(), app_schema()) {
                Ok(db) => {
                    let rows = db
                        .snapshot()
                        .and_then(|snap| snap.scan::<Invoice>())
                        .map_err(|e| format!("read after boot: {e}"))?;
                    let rows = ids(&rows);
                    match (rows.as_slice(), db.last_applied()) {
                        (rows, Some(22)) if rows == (0..23).collect::<Vec<_>>() => {
                            ReseedBoot::OldTimeline
                        }
                        ([1000], Some(RESEED_AT)) => ReseedBoot::NewTimeline,
                        ([], None) => ReseedBoot::Fresh,
                        (rows, last) => ReseedBoot::Other(format!(
                            "booted rows {rows:?}, last applied {last:?}"
                        )),
                    }
                }
                Err(e) => ReseedBoot::Refused(e.to_string()),
            };
            if got != expected {
                return Err(format!(
                    "a re-seed of a log compacted to seq {base}, crashed at disk op {crash_at}, \
                     left {left:?}, so the boot must be {expected:?}, but it was {got:?}"
                ));
            }
            sim.cover(match got {
                ReseedBoot::OldTimeline => "reseed:old-timeline",
                ReseedBoot::NewTimeline => "reseed:new-timeline",
                ReseedBoot::Fresh => "reseed:fresh",
                ReseedBoot::Refused(_) if left.breadcrumb => "reseed:refused-beside-breadcrumb",
                ReseedBoot::Refused(_) => "reseed:refused",
                ReseedBoot::Other(_) => unreachable!("an unexpected boot never equals a prediction"),
            });
            Ok(())
        },
        require: [
            "reseed:old-timeline",
            "reseed:new-timeline",
            "reseed:fresh",
            "reseed:refused",
            "reseed:refused-beside-breadcrumb",
        ]
    );
}

/// What a crashed re-seed left on disk, read before the boot.
#[derive(Debug)]
struct ReseedLeft {
    store: StoreLeft,
    breadcrumb: bool,
    log_base: Option<u64>,
}

#[derive(Debug)]
enum StoreLeft {
    Absent,
    /// A file redb rejects: `Seeder::new`'s new store, its creation torn.
    Rejected,
    /// The store's `applied_seq`: `None` for a store holding no records.
    Held(Option<u64>),
}

#[derive(Debug, PartialEq)]
enum ReseedBoot {
    OldTimeline,
    NewTimeline,
    Fresh,
    Refused(String),
    Other(String),
}

impl ReseedLeft {
    /// The one boot this state allows, by the recovery-horizon rule: a log
    /// based past the store's own next seq is refused, whatever the
    /// breadcrumb says (DESIGN §2).
    fn expected_boot(&self) -> Result<ReseedBoot, String> {
        let held = match self.store {
            // `Db::open` discards a store redb rejects, and rebuilds only
            // from a log that still reaches seq 0; none survives beside one.
            StoreLeft::Rejected if self.log_base.is_none() => return Ok(ReseedBoot::Fresh),
            StoreLeft::Rejected => {
                return Err(format!(
                    "the crash left a state no crash should leave: {self:?}"
                ));
            }
            StoreLeft::Absent => None,
            StoreLeft::Held(applied) => applied,
        };
        let next = held.map_or(0, |applied| applied + 1);
        Ok(match (held, self.log_base) {
            (_, Some(base)) if base > next => {
                ReseedBoot::Refused(vanished_prefix_refusal(held, base))
            }
            (Some(RESEED_AT), _) => ReseedBoot::NewTimeline,
            (Some(applied), Some(_)) if applied > RESEED_AT => ReseedBoot::OldTimeline,
            (None, None) => ReseedBoot::Fresh,
            _ => {
                return Err(format!(
                    "the crash left a state no crash should leave: {self:?}"
                ));
            }
        })
    }
}

/// Reads the store's records from a copy of its bytes on a scratch disk, so
/// the boot that follows meets the file exactly as the crash left it.
fn reseed_left(sim: &Sim) -> Result<ReseedLeft, String> {
    let disk = sim.disk();
    let names = disk
        .list("tables.redb")
        .map_err(|e| format!("list the store: {e}"))?;
    let store = if names.iter().any(|name| name == "tables.redb") {
        let read = || -> io::Result<StoreLeft> {
            let file = disk.open("tables.redb")?;
            let mut bytes = vec![0u8; file.len()? as usize];
            file.read_at(0, &mut bytes)?;
            let scratch = Sim::with_faults(0, FaultConfig::none());
            let copy = scratch.disk().open("tables.redb")?;
            copy.write_at(0, &bytes)?;
            copy.sync()?;
            match Store::open(&scratch.disk(), "tables.redb") {
                Ok(store) => Ok(StoreLeft::Held(store.applied_seq()?)),
                Err(e) if StoreRejected::of(&e).is_some() => Ok(StoreLeft::Rejected),
                Err(e) => Err(e),
            }
        };
        read().map_err(|e| format!("read the store the crash left: {e}"))?
    } else {
        StoreLeft::Absent
    };
    let log_base = disk
        .list("wal/")
        .map_err(|e| format!("list the log: {e}"))?
        .first()
        .map(|name| segment_base(name));
    Ok(ReseedLeft {
        store,
        breadcrumb: names.iter().any(|name| name == "tables.redb.horizon"),
        log_base,
    })
}

/// The crash model alone: where the crash lands and what survives it. An
/// injected IO error would end the re-seed early instead.
fn crash_model_only(cfg: &mut FaultConfig) {
    cfg.write_error = 0.0;
    cfg.sync_error = 0.0;
    cfg.read_error = 0.0;
    cfg.bit_flip = 0.0;
    cfg.misdirected_write = 0.0;
}

/// Where the crashed re-seed's new timeline starts.
const RESEED_AT: u64 = 5;

/// Writes 23 rows with a checkpoint after the 20th, so the log is compacted
/// past seq 0 with records above its base, then re-seeds the node to one row
/// at `RESEED_AT` with a crash armed at a disk op drawn from the seed, and
/// power-cycles. Returns the log's base before the re-seed and the crash point.
fn crash_a_reseed(sim: &mut Sim) -> Result<(u64, u64), String> {
    let mut db = Db::open(Box::new(sim.disk()), tiny_segments_cfg(), app_schema())
        .map_err(|e| format!("setup open: {e}"))?;
    for i in 0..23u64 {
        db.transact(|tx| {
            tx.put(&invoice(i, 1, i as i64));
            Ok::<_, io::Error>(())
        })
        .map_err(|e| format!("setup write {i}: {e:?}"))?;
        if i == 19 {
            db.checkpoint()
                .map_err(|e| format!("setup checkpoint: {e}"))?;
        }
    }
    let base = db.log_base();
    assert!(base > 0, "setup must compact the log past seq 0");
    let crash_at = sim.entropy().next_u64() % 80;
    sim.crash_disk_after(crash_at);
    let reseeded = db.into_seeder().and_then(|mut seeder| {
        let mut bytes = Vec::new();
        invoice(1000, 1, 1000).encode_row(&mut bytes);
        seeder.rows(Invoice::TABLE, &[(0, 1000, bytes)])?;
        seeder.finish(RESEED_AT, 0, 0, 0)
    });
    drop(reseeded);
    sim.restart();
    Ok((base, crash_at))
}

/// Sim-found (S5 R-I1): seed `0x64a183075fd398b9`, the first universe the
/// property above failed at base `0x5EED_C0A1` before the fix. Its crash
/// lands inside `Seeder::new` after the store's removal is barriered and
/// before the breadcrumb's is, so the boot met no store, the breadcrumb, and
/// a log compacted to seq 18, and panicked replaying seq 18 into an empty
/// store ("Store::apply_batch reached seq 18 but expected 0"). It refuses by
/// name now.
#[test]
fn a_reseed_crash_that_strands_the_breadcrumb_without_its_store_is_refused_by_name() {
    const SEED: u64 = 0x64a1_8307_5fd3_98b9;
    let mut state = SEED;
    let cfg_seed = charpente_sim::rng::splitmix64(&mut state);
    let mut faults = FaultConfig::swarm(&mut charpente_sim::rng::Rng::new(cfg_seed));
    charpente_sim::sample_extended_fields(SEED, &mut faults);
    crash_model_only(&mut faults);
    let mut sim = Sim::with_faults(SEED, faults);

    let (base, _) = crash_a_reseed(&mut sim).unwrap();
    assert_eq!(base, 18, "seed {SEED:#x}");
    assert_eq!(
        sim.disk().list("tables.redb").unwrap(),
        ["tables.redb.horizon"],
        "seed {SEED:#x}: the crash must leave the breadcrumb without its store"
    );
    assert_eq!(
        segment_base(&sim.disk().list("wal/").unwrap()[0]),
        18,
        "seed {SEED:#x}: the crash must leave the compacted log"
    );
    let e = Db::open(Box::new(sim.disk()), tiny_segments_cfg(), app_schema())
        .expect_err("a compacted log beside no store must not boot");
    assert_eq!(
        e.to_string(),
        vanished_prefix_refusal(None, 18),
        "seed {SEED:#x}"
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

/// M2 sub-project 0b task 3: `verify_writes` gates only effectful `transact`,
/// same as `forbid_writes` — reads (and effect-free transactions) are
/// unaffected. This is the placement ruling (task-3 dispatch addition 1):
/// the gate check sits after `transact`'s `tx.effects.is_empty()` early
/// return, so an effect-free transaction never reaches it. Getting the
/// placement wrong (checking the gate before running `f`) would silently
/// invert the "serves reads" half of Noah's ruling while every write-side
/// test still passed — this test is what pins that placement.
#[test]
fn witness_verification_gates_writes_but_reads_still_work() {
    const SEED: u64 = 0x9012_0009;
    let (_sim, mut db) = quiet_db(SEED);
    db.transact(|tx| {
        tx.put(&invoice(1, 1, 100));
        Ok::<_, io::Error>(())
    })
    .unwrap();
    assert!(db.writes_allowed());

    db.verify_writes(vec!["127.0.0.1:7001".to_string()]);
    assert!(!db.writes_allowed());

    // A read-only transaction (no effects) still works — the gate never
    // sees it.
    let amount = db
        .transact(|tx| {
            Ok::<_, io::Error>(tx.get::<Invoice>(Id::from_raw(1))?.map(|i| i.amount_cents))
        })
        .unwrap();
    assert_eq!(amount, Some(100));

    // An effectful transaction is rejected, naming the unreached witness —
    // never a primary, since this node doesn't know whether one exists —
    // and writes nothing (last_applied is unchanged).
    let err = db
        .transact(|tx| {
            tx.put(&invoice(2, 1, 200));
            Ok::<_, io::Error>(())
        })
        .unwrap_err();
    match err {
        TxError::Verifying { unreached } => {
            assert_eq!(unreached, vec!["127.0.0.1:7001".to_string()]);
        }
        other => panic!("expected Verifying, got {other:?}"),
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

/// Negative space (hard rule 4): an empty witness list has nothing to
/// verify — the caller meant `allow_writes`, not `verify_writes`.
#[test]
#[should_panic(expected = "empty witness list")]
fn verify_writes_with_an_empty_witness_list_panics() {
    let (_sim, mut db) = quiet_db(0x9012_000A);
    db.verify_writes(vec![]);
}

/// Negative space (hard rule 4, fix round 1 M-3): `verify_writes` over an
/// honest `NotPrimary` would silently replace a "go to X" 421 with an "I
/// might still be the primary" 503 — a real regression in what the caller
/// is told. Refused by name instead.
#[test]
#[should_panic(expected = "forbid_writes was already in effect")]
fn verify_writes_over_an_honest_not_primary_panics() {
    let (_sim, mut db) = quiet_db(0x9012_000B);
    db.forbid_writes("127.0.0.1:9090".to_string());
    db.verify_writes(vec!["127.0.0.1:7001".to_string()]);
}

/// `Verifying -> Verifying` must stay legal (fix round 1 M-3's own
/// clarification): re-gating with a smaller list is supported, not a caller
/// bug. This is a CAPABILITY test, not a description of the shipped path.
/// `Repl` calls `verify_writes` exactly once, at `open`, and deliberately
/// never re-gates — `resolve_witness_gate`'s rustdoc carries the reason
/// (which answer a witness gives is peer-controlled, so a per-tick re-gate
/// would make `verify_writes`'s own assert remotely triggerable). So the 503
/// a client actually sees names the whole witness set for the life of the
/// gate; only the deadline notice tracks who is still outstanding.
#[test]
fn verify_writes_can_shrink_the_witness_list() {
    let (_sim, mut db) = quiet_db(0x9012_000C);
    db.verify_writes(vec![
        "127.0.0.1:7001".to_string(),
        "127.0.0.1:7002".to_string(),
    ]);
    assert!(!db.writes_allowed());

    // One witness answers "not a primary" — the list shrinks, still Verifying.
    db.verify_writes(vec!["127.0.0.1:7002".to_string()]);
    assert!(!db.writes_allowed());

    let err = db
        .transact(|tx| {
            tx.put(&invoice(1, 1, 100));
            Ok::<_, io::Error>(())
        })
        .unwrap_err();
    match err {
        TxError::Verifying { unreached } => {
            assert_eq!(unreached, vec!["127.0.0.1:7002".to_string()]);
        }
        other => panic!("expected Verifying, got {other:?}"),
    }
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

/// The convergence oracle in miniature, and the line it now stops at
/// (DESIGN §1, property 1). A follower fed the primary's own `read_log` output
/// via `apply_shipped` converges on content unconditionally; it converges on
/// BYTES only when it also grouped those records into store transactions the
/// way the primary did. `b` catches up in two wide batches and `c` frame by
/// frame, against a primary that committed one record at a time — so the two
/// halves of the property are asserted against each other in one run, and
/// neither half can go vacuous without the other going red.
///
/// Before per-batch apply this held for `b` too, because the store committed
/// one transaction per record no matter how frames arrived. That is the
/// incidental byte-identity DESIGN §1 now declines to claim.
#[test]
fn apply_shipped_replays_records_byte_identically() {
    let sim = Sim::with_faults(0x9012_0002, FaultConfig::none());
    let host_a = sim.host("a");
    let host_b = sim.host("b");
    let host_c = sim.host("c");
    let mut db_a = Db::open(Box::new(host_a.disk()), DbConfig::default(), app_schema()).unwrap();
    let mut db_b = Db::open(Box::new(host_b.disk()), DbConfig::default(), app_schema()).unwrap();
    let mut db_c = Db::open(Box::new(host_c.disk()), DbConfig::default(), app_schema()).unwrap();

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
    for frame in &frames {
        db_c.apply_shipped(std::slice::from_ref(frame)).unwrap();
    }

    assert_ne!(
        db_b.store_write_schedule(),
        db_a.store_write_schedule(),
        "two ten-record transactions is not twenty one-record ones — if these \
         ever agree this test has stopped exercising the batched case"
    );
    assert_eq!(
        db_c.store_write_schedule(),
        db_a.store_write_schedule(),
        "one frame per apply_shipped is one record per store transaction, \
         exactly what committing one at a time does"
    );

    for (name, db) in [("b", &db_b), ("c", &db_c)] {
        assert_eq!(db.last_applied(), db_a.last_applied(), "{name}");
        assert_eq!(
            table_contents(&db_a.snapshot().unwrap()),
            table_contents(&db.snapshot().unwrap()),
            "every row must match after replay ({name}) — content convergence \
             does not depend on how the records were batched"
        );
    }

    db_a.checkpoint().unwrap();
    db_b.checkpoint().unwrap();
    db_c.checkpoint().unwrap();
    drop(db_a);
    drop(db_b);
    drop(db_c);

    let db_a = Db::open(Box::new(host_a.disk()), DbConfig::default(), app_schema()).unwrap();
    let db_b = Db::open(Box::new(host_b.disk()), DbConfig::default(), app_schema()).unwrap();
    let db_c = Db::open(Box::new(host_c.disk()), DbConfig::default(), app_schema()).unwrap();
    assert_eq!(db_a.last_applied(), db_b.last_applied());
    assert_eq!(db_a.last_applied(), db_c.last_applied());
    assert_eq!(
        host_a.durable_fingerprint(),
        host_c.durable_fingerprint(),
        "the convergence oracle, at the scope that survives batching: equal \
         write schedule at equal seq means byte-identical durable state"
    );
    assert_ne!(
        host_a.durable_fingerprint(),
        host_b.durable_fingerprint(),
        "and the scope that does not: same log, same rows, different \
         transaction grouping, so redb's layout differs. A green assert_eq \
         here would mean grouping had stopped reaching the bytes — worth \
         knowing, but it would make DESIGN §1's caveat unnecessary rather \
         than wrong, so re-derive the caveat before deleting this line"
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

/// `TableMachine` is atomic where the trait default is per-record: one store
/// transaction per batch, so a failure anywhere rolls every record back and
/// reports nothing applied — there is no mid-batch boundary left to report.
/// Driven through `apply_shipped`, which is `submit_batch` -> `apply_batch`.
///
/// The `drain_enqueued` assertion is the load-bearing one: records 0 and 1
/// materialize jobs inside the transaction that then rolls back, and
/// enqueueing them would hand the job runner ids for rows the store never
/// took.
#[test]
fn a_failed_batch_applies_nothing_and_enqueues_nothing() {
    let sim = Sim::with_faults(0x7AB1_E001, FaultConfig::none());
    let host_a = sim.host("a");
    let host_b = sim.host("b");
    let mut db_a = Db::open(Box::new(host_a.disk()), DbConfig::default(), app_schema()).unwrap();
    for i in 0..2u64 {
        db_a.transact(|tx| {
            tx.put(&invoice(i, 0, 100));
            tx.enqueue(SendReceipt { invoice: i });
            Ok::<_, io::Error>(())
        })
        .unwrap();
    }
    let mut frames = db_a.read_log(0, usize::MAX).unwrap();
    assert_eq!(frames.len(), 2);

    // One effect claimed, tag 99: `decode_record` refuses it, and it refuses
    // INSIDE the store transaction — after the two good records have already
    // put rows and materialized jobs into that same transaction. That
    // interleaving is exactly what atomicity has to survive.
    frames.push((2, vec![1u8, 0, 0, 0, 99]));

    let mut db_b = Db::open(Box::new(host_b.disk()), DbConfig::default(), app_schema()).unwrap();
    let err = db_b.apply_shipped(&frames).unwrap_err();
    assert!(err.to_string().contains("unknown effect tag 99"), "{err}");

    assert_eq!(
        db_b.last_applied(),
        None,
        "a rolled-back batch applied nothing, so the engine's applied count is \
         0 and last_applied never moves"
    );
    let snap = db_b.snapshot().unwrap();
    assert_eq!(
        snap.applied_seq().unwrap(),
        None,
        "a failed batch must not move the store's applied_seq"
    );
    assert!(
        snap.scan::<Invoice>().unwrap().is_empty(),
        "records 0 and 1 wrote rows into the transaction that rolled back"
    );
    assert!(
        db_b.drain_enqueued().is_empty(),
        "records 0 and 1 materialized jobs inside the transaction, but the \
         transaction rolled back — enqueueing them would hand the job runner \
         ids for rows the store never took"
    );
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
        let rows = snap
            .scan_raw(table, db_a.table_scoped(table).unwrap())
            .unwrap();
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
        new_job.id.raw(),
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
        rows.push((0, i, bytes));
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
    let rows = snap.scan_raw(Invoice::TABLE, false).unwrap();

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
