//! Migration flagship properties (spec docs/superpowers/specs/
//! 2026-07-16-migrations-design.md §6, migrations T9): crash-at-any-point
//! resumability against the uninterrupted twin, the RED-provable drift
//! meta-test (hard rule 3), multi-step chain determinism, in-flight-target
//! refusal + rejection-mid-chain, index-rebuild idempotency, and replay
//! determinism. Fixtures duplicate migration_gate.rs's WidgetV1/WidgetDrifted/
//! Widget2 (integration test binaries do not share code in this repo —
//! materialize.rs vs engine.rs precedent) plus a new Widget3 step.

use std::io;

use charpente_core::db::{Db, DbConfig, Schema, TxError};
use charpente_core::migration::{MigrationError, SchemaVersion, TableMigration, schema_version_id};
use charpente_core::table::{Codec, Id, IndexDef, Row, RowShape, Unscoped, View};
use charpente_sim::{FaultConfig, Sim, run_universes_swarm_with};

struct WidgetV1 {
    id: Id<WidgetV1>,
    count: u32,
}
impl Row for WidgetV1 {
    type Scope = Unscoped;
    const TABLE: &'static str = "Widget";
    const INDEXES: &'static [IndexDef<WidgetV1>] = &[];
    const VERSION: u32 = 1;
    const FINGERPRINT: Option<u64> = Some(0x1111);
    fn id(&self) -> Id<WidgetV1> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<WidgetV1> {
        let input = &mut &bytes[..];
        Ok(WidgetV1 {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
        })
    }
}
impl RowShape for WidgetV1 {
    const VERSION: u32 = 1;
    fn encode_row(&self, out: &mut Vec<u8>) {
        Row::encode_row(self, out)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<WidgetV1> {
        <WidgetV1 as Row>::decode_row(bytes)
    }
}

struct WidgetDrifted {
    id: Id<WidgetDrifted>,
    count: u32,
    extra: bool,
}
impl Row for WidgetDrifted {
    type Scope = Unscoped;
    const TABLE: &'static str = "Widget";
    const INDEXES: &'static [IndexDef<WidgetDrifted>] = &[];
    const VERSION: u32 = 1;
    const FINGERPRINT: Option<u64> = Some(0x2222);
    fn id(&self) -> Id<WidgetDrifted> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
        self.extra.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<WidgetDrifted> {
        let input = &mut &bytes[..];
        Ok(WidgetDrifted {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
            extra: bool::decode(input)?,
        })
    }
}

struct Widget2 {
    id: Id<Widget2>,
    count: u32,
    label: String,
}
impl Row for Widget2 {
    type Scope = Unscoped;
    const TABLE: &'static str = "Widget";
    const INDEXES: &'static [IndexDef<Widget2>] = &[];
    const VERSION: u32 = 2;
    const FINGERPRINT: Option<u64> = Some(0x4444);
    fn id(&self) -> Id<Widget2> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
        self.label.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Widget2> {
        let input = &mut &bytes[..];
        Ok(Widget2 {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
            label: String::decode(input)?,
        })
    }
}
impl RowShape for Widget2 {
    const VERSION: u32 = 2;
    fn encode_row(&self, out: &mut Vec<u8>) {
        Row::encode_row(self, out)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Widget2> {
        <Widget2 as Row>::decode_row(bytes)
    }
}
impl TableMigration for Widget2 {
    type Prev = WidgetV1;
    fn migrate(prev: WidgetV1) -> Result<Widget2, MigrationError> {
        Ok(Widget2 {
            id: Id::from_raw(prev.id.raw()),
            count: prev.count,
            label: "migrated".to_string(),
        })
    }
}

struct Widget3 {
    id: Id<Widget3>,
    count: u32,
    label: String,
    tag: u8,
}
impl Row for Widget3 {
    type Scope = Unscoped;
    const TABLE: &'static str = "Widget";
    const INDEXES: &'static [IndexDef<Widget3>] = &[];
    const VERSION: u32 = 3;
    const FINGERPRINT: Option<u64> = Some(0x5555);
    fn id(&self) -> Id<Widget3> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
        self.label.encode(out);
        self.tag.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Widget3> {
        let input = &mut &bytes[..];
        Ok(Widget3 {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
            label: String::decode(input)?,
            tag: u8::decode(input)?,
        })
    }
}
impl RowShape for Widget3 {
    const VERSION: u32 = 3;
    fn encode_row(&self, out: &mut Vec<u8>) {
        Row::encode_row(self, out)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Widget3> {
        <Widget3 as Row>::decode_row(bytes)
    }
}
impl TableMigration for Widget3 {
    type Prev = Widget2;
    fn migrate(prev: Widget2) -> Result<Widget3, MigrationError> {
        Ok(Widget3 {
            id: Id::from_raw(prev.id.raw()),
            count: prev.count,
            label: prev.label,
            tag: 9,
        })
    }
}

fn cfg() -> DbConfig {
    DbConfig {
        store_path: "t.redb".into(),
        ..DbConfig::default()
    }
}

fn seed_v1(sim: &Sim, rows: u64) {
    let mut db = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<WidgetV1>(),
    )
    .unwrap();
    db.transact(|tx| -> Result<(), ()> {
        for i in 0..rows {
            tx.put(&WidgetV1 {
                id: Id::from_raw(i),
                count: i as u32,
            });
        }
        Ok(())
    })
    .unwrap();
    db.checkpoint().unwrap(); // clean shutdown: the tail is drained
}

/// `seed_v1` with faults mapped to `Err`, wrapped in the same
/// fault-classified restart loop as the main workload: a faulted universe
/// can fail the SEED too. `Ok(false)` means the round budget exhausted
/// without ever hitting a non-fallout error — every round was fault-fallout
/// — so the caller should treat the universe as covered-inconclusive rather
/// than red: the same harsh-config non-convergence class as
/// `open_and_migrate_with_remedy`'s `Ok(None)` (e.g. a config with only
/// `write_error` active and no crash axes at all can still, for an unlucky
/// seed, fail every write attempt across the whole budget).
fn seed_v1_result(sim: &Sim, rows: u64) -> Result<bool, String> {
    for _round in 0..64 {
        let attempt: io::Result<()> = (|| {
            let mut db = Db::open(
                Box::new(sim.disk()),
                cfg(),
                Schema::new().table::<WidgetV1>(),
            )?;
            db.transact(|tx| -> Result<(), ()> {
                for i in 0..rows {
                    tx.put(&WidgetV1 {
                        id: Id::from_raw(i),
                        count: i as u32,
                    });
                }
                Ok(())
            })
            .map_err(|e| match e {
                TxError::Io(e) => e,
                other => io::Error::other(format!("unexpected tx error while seeding: {other:?}")),
            })?;
            db.checkpoint()
        })();
        match attempt {
            Ok(()) => return Ok(true),
            Err(e) => {
                let msg = e.to_string();
                if !charpente_sim::checks::is_fault_fallout(&msg) {
                    return Err(format!("non-injected failure while seeding: {msg}"));
                }
                sim.restart();
            }
        }
    }
    Ok(false)
}

/// Opens under the `Widget2` schema and runs `migrate_schema`, retrying
/// through the fault matrix; on the upgrade-barrier refusal — a full
/// rebuild-from-WAL replaying a table that never started migrating, the one
/// barrier case the migrations T9 interlude fix (commit f1b711e) keeps
/// fail-closed — models the operator remedy: boot the v1 binary once (open +
/// drop; replay-then-checkpoint-at-open clears the tail), then resume
/// retrying the real open+migrate. Remedy rounds share the SAME bounded
/// round budget as ordinary retries. `Ok(None)` means the budget exhausted
/// without ever hitting a non-fallout, non-barrier error — every round was
/// fallout or a legitimately-remedied barrier, so the caller should treat
/// the universe as covered-inconclusive rather than red (harsh-config
/// non-convergence, mirroring auth's precedent), not as a failure.
fn open_and_migrate_with_remedy(sim: &Sim, db_cfg: DbConfig) -> Result<Option<Db>, String> {
    let mut remedy_pending = false;
    for _round in 0..128 {
        if remedy_pending {
            match Db::open(
                Box::new(sim.disk()),
                cfg(),
                Schema::new().table::<WidgetV1>(),
            ) {
                Ok(remedy_db) => {
                    drop(remedy_db);
                    remedy_pending = false;
                }
                Err(e) => {
                    let msg = e.to_string();
                    if !charpente_sim::checks::is_fault_fallout(&msg) {
                        return Err(format!("non-injected failure during barrier remedy: {msg}"));
                    }
                    sim.restart();
                }
            }
            continue;
        }
        let schema = Schema::new()
            .table::<Widget2>()
            .migration::<Widget2>("Widget");
        match Db::open(Box::new(sim.disk()), db_cfg.clone(), schema)
            .and_then(|mut d| d.migrate_schema().map(|()| d))
        {
            Ok(d) => return Ok(Some(d)),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("unrecovered writes from the previous binary") {
                    sim.cover("migrations:barrier-remedied");
                    remedy_pending = true;
                    continue;
                }
                if !charpente_sim::checks::is_fault_fallout(&msg) {
                    return Err(format!("non-injected failure: {msg}"));
                }
                sim.restart();
            }
        }
    }
    Ok(None)
}

// ------------------------------------------------------------ Step 1

fn migrated_twin(rows: u64) -> Vec<(u64, u32, String)> {
    // The uninterrupted twin: same seed data, faultless universe, one
    // clean migration — the oracle every faulted universe must equal.
    let sim = Sim::with_faults(0x2EB7_1000, FaultConfig::none());
    seed_v1(&sim, rows);
    let schema = Schema::new()
        .table::<Widget2>()
        .migration::<Widget2>("Widget");
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig {
            migration_batch_size: 3,
            ..cfg()
        },
        schema,
    )
    .unwrap();
    db.migrate_schema().unwrap();
    let mut rows: Vec<Widget2> = db.snapshot().unwrap().scan().unwrap();
    rows.sort_by_key(|r| r.id.raw());
    rows.into_iter()
        .map(|r| (r.id.raw(), r.count, r.label))
        .collect()
}

/// True when a durable-corruption-caused loss legitimately explains
/// `got != twin`: `got` must be a genuine subset of `twin` by FULL row
/// content (id, count, label) — every row `got` reports also exists,
/// byte-for-byte, in `twin` — AND `corruption_fired`. A row in `got` that
/// `twin` does not contain at all (wrong data, or data corruption fired but
/// something else is also wrong) always refuses, regardless of
/// `corruption_fired` — the excuse is structurally unable to mask wrong
/// data or a non-corruption loss (spec: never excuse wrong data, only
/// missing data; loss is excused ONLY in universes where corruption was
/// actually injected).
fn corruption_excuses_the_loss(
    got: &[(u64, u32, String)],
    twin: &[(u64, u32, String)],
    corruption_fired: bool,
) -> bool {
    corruption_fired && got.iter().all(|row| twin.contains(row))
}

#[cfg(test)]
mod corruption_excuse_tests {
    use super::corruption_excuses_the_loss;

    fn twin() -> Vec<(u64, u32, String)> {
        vec![
            (0, 0, "migrated".to_string()),
            (1, 1, "migrated".to_string()),
        ]
    }

    #[test]
    fn refuses_missing_data_without_corruption_having_fired() {
        assert!(!corruption_excuses_the_loss(&[], &twin(), false));
    }

    #[test]
    fn refuses_a_foreign_row_even_when_corruption_fired() {
        let got = vec![(0, 0, "migrated".to_string()), (1, 99, "wrong".to_string())];
        assert!(!corruption_excuses_the_loss(&got, &twin(), true));
    }

    #[test]
    fn excuses_a_genuine_subset_including_fully_empty_when_corruption_fired() {
        assert!(corruption_excuses_the_loss(&[], &twin(), true));
        assert!(corruption_excuses_the_loss(
            &[twin()[0].clone()],
            &twin(),
            true
        ));
        assert!(corruption_excuses_the_loss(&twin(), &twin(), true));
    }
}

// Finding 3 (migrations T9 interlude addendum — dossier
// .superpowers/sdd/migrations-t9-interlude.md): seed 0x6b436e3ea995cb6f.
// Mechanism, verified: a `Store::open` failure triggers discard-and-rebuild,
// which resets that attempt's recovery horizon to `None`/0; a real
// `fault:bit_flip` had earlier corrupted the seed record's (first) frame,
// which is now "above" the recomputed horizon and gets silently torn-tail-
// cut instead of hitting the corrupt-at-or-below-horizon hard error —
// cutting the first frame cuts everything. Pre-existing (log.rs is untouched
// by f1b711e; the decode-free replay path never runs here — the log is cut
// empty before replay reaches it). The silent-loss window itself is
// ledgered as a production follow-up outside this sub-project; here it is
// excused exactly like every other single-node durable-corruption loss
// (`corruption_excuses_the_loss` above), never masking wrong data.
#[test]
fn migration_survives_the_fault_matrix_and_equals_the_uninterrupted_twin() {
    let twin = migrated_twin(20);
    run_universes_swarm_with!(
        0x2EB7_2000,
        128,
        |_cfg: &mut FaultConfig| {},
        |sim: &mut Sim| {
            if !seed_v1_result(sim, 20)? {
                sim.cover("migrations:inconclusive");
                return Ok(());
            }
            let db_cfg = DbConfig {
                migration_batch_size: 3,
                ..cfg()
            };
            let Some(db) = open_and_migrate_with_remedy(sim, db_cfg)? else {
                // Harsh-config non-convergence (auth's inconclusive precedent):
                // every round was fault-fallout or a legitimately-remedied
                // barrier, and the budget ran out before a clean open+migrate.
                sim.cover("migrations:inconclusive");
                return Ok(());
            };
            sim.cover("migrations:resumed-to-completion");
            let mut rows: Vec<Widget2> =
                db.snapshot().map_err(|e| e.to_string())?.scan().map_err(|e| e.to_string())?;
            rows.sort_by_key(|r| r.id.raw());
            let got: Vec<(u64, u32, String)> =
                rows.into_iter().map(|r| (r.id.raw(), r.count, r.label)).collect();
            if got != twin {
                if corruption_excuses_the_loss(
                    &got,
                    &twin,
                    charpente_sim::checks::durable_corruption_fired(sim),
                ) {
                    sim.cover("migrations:corruption-excused");
                    return Ok(());
                }
                return Err(format!(
                    "faulted universe's migrated store diverged from the uninterrupted twin: \
                     got {got:?}"
                ));
            }
            Ok(())
        },
        require: ["migrations:resumed-to-completion"]
    );
}

// ------------------------------------------------------------ Step 2

#[test]
fn the_real_gate_refuses_drift_the_naive_gate_would_misdecode() {
    use charpente_core::db::{GateStatus, gate_status};

    let sim = Sim::with_faults(0x2EB7_0010, FaultConfig::none());
    seed_v1(&sim, 3);
    // Read the REAL persisted row back from the REAL store.
    let db = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<WidgetV1>(),
    )
    .unwrap();
    let persisted: SchemaVersion = db
        .snapshot()
        .unwrap()
        .get(schema_version_id("Widget"))
        .unwrap()
        .unwrap();
    let raw = db
        .snapshot()
        .unwrap()
        .scan_raw_from("Widget", false, None, 1)
        .unwrap();
    drop(db);

    // The real gate, fed the real row + WidgetDrifted's compiled meta:
    // refuses by name.
    let real = gate_status("Widget", (1, 0x2222), Some(&persisted), true);
    assert!(matches!(real, GateStatus::Refuse(_)), "{real:?}");

    // The naive gate — same signature, fingerprint check deleted (the
    // exact bug the negative-space check exists to prevent):
    fn naive_gate_status(
        _table: &str,
        compiled: (u32, u64),
        persisted: Option<&SchemaVersion>,
        _rows_exist: bool,
    ) -> GateStatus {
        match persisted {
            Some(row) if row.version == compiled.0 => GateStatus::Current,
            _ => GateStatus::Adopt,
        }
    }
    let naive = naive_gate_status("Widget", (1, 0x2222), Some(&persisted), true);
    assert_eq!(
        naive,
        GateStatus::Current,
        "the victim must wrongly proceed"
    );

    // And proceeding is not hypothetical damage: the drifted compiled
    // type MISDECODES the real stored bytes the naive gate just admitted.
    let (_, _, bytes) = &raw[0];
    assert!(
        <WidgetDrifted as Row>::decode_row(bytes).is_err(),
        "WidgetV1 bytes must not decode under WidgetDrifted — if they ever do, this \
         meta-test's damage half needs a same-length-retype fixture instead"
    );
}

// ------------------------------------------------------------ Step 3

#[test]
fn multi_step_chain_reaches_the_same_state_in_one_hop_or_two() {
    let rows = 6;

    // Path A: v1 -> v3 directly, both steps registered on one schema, one
    // migrate_schema call.
    let sim_a = Sim::with_faults(0x2EB7_0011, FaultConfig::none());
    seed_v1(&sim_a, rows);
    let schema_a = Schema::new()
        .table::<Widget3>()
        .migration::<Widget2>("Widget")
        .migration::<Widget3>("Widget");
    let mut db_a = Db::open(Box::new(sim_a.disk()), cfg(), schema_a).unwrap();
    db_a.migrate_schema().unwrap();
    let mut rows_a: Vec<Widget3> = db_a.snapshot().unwrap().scan().unwrap();
    rows_a.sort_by_key(|r| r.id.raw());
    let got_a: Vec<(u64, u32, String, u8)> = rows_a
        .into_iter()
        .map(|r| (r.id.raw(), r.count, r.label, r.tag))
        .collect();

    // Path B: v1 -> v2 (migrate, checkpoint, drop) -> v3 (reopen, migrate).
    let sim_b = Sim::with_faults(0x2EB7_0012, FaultConfig::none());
    seed_v1(&sim_b, rows);
    {
        let schema_b1 = Schema::new()
            .table::<Widget2>()
            .migration::<Widget2>("Widget");
        let mut db_b = Db::open(Box::new(sim_b.disk()), cfg(), schema_b1).unwrap();
        db_b.migrate_schema().unwrap();
        db_b.checkpoint().unwrap();
    }
    let schema_b2 = Schema::new()
        .table::<Widget3>()
        .migration::<Widget2>("Widget")
        .migration::<Widget3>("Widget");
    let mut db_b = Db::open(Box::new(sim_b.disk()), cfg(), schema_b2).unwrap();
    db_b.migrate_schema().unwrap();
    let mut rows_b: Vec<Widget3> = db_b.snapshot().unwrap().scan().unwrap();
    rows_b.sort_by_key(|r| r.id.raw());
    let got_b: Vec<(u64, u32, String, u8)> = rows_b
        .into_iter()
        .map(|r| (r.id.raw(), r.count, r.label, r.tag))
        .collect();

    assert_eq!(
        got_a, got_b,
        "v1->v3 in one hop must equal v1->v2->v3 in two"
    );
}

// ------------------------------------------------------------ Step 4

#[test]
fn in_flight_target_refusal_and_rejection_mid_chain_are_pinned_verbatim() {
    struct Widget2Rejecting {
        id: Id<Widget2Rejecting>,
        count: u32,
    }
    impl Row for Widget2Rejecting {
        type Scope = Unscoped;
        const TABLE: &'static str = "Widget";
        const INDEXES: &'static [IndexDef<Widget2Rejecting>] = &[];
        const VERSION: u32 = 2;
        const FINGERPRINT: Option<u64> = Some(0x6666);
        fn id(&self) -> Id<Widget2Rejecting> {
            self.id
        }
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.id.encode(out);
            self.count.encode(out);
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget2Rejecting> {
            let input = &mut &bytes[..];
            Ok(Widget2Rejecting {
                id: Id::decode(input)?,
                count: u32::decode(input)?,
            })
        }
    }
    impl RowShape for Widget2Rejecting {
        const VERSION: u32 = 2;
        fn encode_row(&self, out: &mut Vec<u8>) {
            Row::encode_row(self, out)
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget2Rejecting> {
            <Widget2Rejecting as Row>::decode_row(bytes)
        }
    }
    impl TableMigration for Widget2Rejecting {
        type Prev = WidgetV1;
        fn migrate(prev: WidgetV1) -> Result<Widget2Rejecting, MigrationError> {
            if prev.count == 3 {
                return Err(MigrationError::Reject(format!(
                    "row {} has count 3, which this migration deliberately refuses",
                    prev.id.raw()
                )));
            }
            Ok(Widget2Rejecting {
                id: Id::from_raw(prev.id.raw()),
                count: prev.count,
            })
        }
    }

    // A v2 fixture with the SAME version but a DIFFERENT fingerprint from
    // Widget2Rejecting — used for the third refusal below. A "same
    // fingerprint, non-rejecting" fixture cannot exist: the fingerprint
    // pins shape, and in real macro-driven code identical shape IS
    // identical fingerprint, so there is nothing distinct to construct.
    struct Widget2OtherFingerprint {
        id: Id<Widget2OtherFingerprint>,
        count: u32,
    }
    impl Row for Widget2OtherFingerprint {
        type Scope = Unscoped;
        const TABLE: &'static str = "Widget";
        const INDEXES: &'static [IndexDef<Widget2OtherFingerprint>] = &[];
        const VERSION: u32 = 2;
        const FINGERPRINT: Option<u64> = Some(0x7777);
        fn id(&self) -> Id<Widget2OtherFingerprint> {
            self.id
        }
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.id.encode(out);
            self.count.encode(out);
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget2OtherFingerprint> {
            let input = &mut &bytes[..];
            Ok(Widget2OtherFingerprint {
                id: Id::decode(input)?,
                count: u32::decode(input)?,
            })
        }
    }
    impl RowShape for Widget2OtherFingerprint {
        const VERSION: u32 = 2;
        fn encode_row(&self, out: &mut Vec<u8>) {
            Row::encode_row(self, out)
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget2OtherFingerprint> {
            <Widget2OtherFingerprint as Row>::decode_row(bytes)
        }
    }
    impl TableMigration for Widget2OtherFingerprint {
        type Prev = WidgetV1;
        fn migrate(prev: WidgetV1) -> Result<Widget2OtherFingerprint, MigrationError> {
            Ok(Widget2OtherFingerprint {
                id: Id::from_raw(prev.id.raw()),
                count: prev.count,
            })
        }
    }

    let sim = Sim::with_faults(0x2EB7_0013, FaultConfig::none());
    seed_v1(&sim, 5); // ids 0..5, counts 0..5 — row 3 has count 3
    let rejecting_schema = || {
        Schema::new()
            .table::<Widget2Rejecting>()
            .migration::<Widget2Rejecting>("Widget")
    };
    let batched_cfg = DbConfig {
        migration_batch_size: 2,
        ..cfg()
    };

    // Batch 1 (ids 0,1) commits; batch 2 (ids 2,3) hits row 3 (count 3) and
    // errors out of migrate_schema.
    let mut db = Db::open(
        Box::new(sim.disk()),
        batched_cfg.clone(),
        rejecting_schema(),
    )
    .unwrap();
    let err = db.migrate_schema().unwrap_err();
    let rejection_msg = "migrating table \"Widget\" row 3: row 3 has count 3, which this \
                          migration deliberately refuses";
    assert_eq!(err.to_string(), rejection_msg);
    drop(db);

    // Reopen with the v1 schema: the in-flight-target refusal fires
    // verbatim (migrating_to = Some(2), compiled 1).
    let err = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<WidgetV1>(),
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\" has an interrupted migration to version 2 in flight (resumable \
         watermark at scope 0 row id 1) but this binary is compiled for version 1 — rows on either \
         side of the watermark are at different versions and no single chain start covers \
         both; finish the interrupted migration with the binary that started it, or restore"
    );

    // Reopen with the SAME rejecting v2 schema and migrate again: resumption
    // is honest — the very same row rejects with the very same message.
    let mut db = Db::open(
        Box::new(sim.disk()),
        batched_cfg.clone(),
        rejecting_schema(),
    )
    .unwrap();
    let err = db.migrate_schema().unwrap_err();
    assert_eq!(err.to_string(), rejection_msg);
    drop(db);

    // Reopen with a v2 fixture of a DIFFERENT fingerprint: refuses by name —
    // the in-flight-target-fingerprint refusal (migrations T9 interlude,
    // commit f1b711e). Resuming under a different target shape would leave
    // rows below the watermark at the original target's bytes and rows
    // above at this binary's — a silently mixed-shape table — so `Db::open`
    // itself refuses; it never gets a chance to silently resume.
    let err = Db::open(
        Box::new(sim.disk()),
        batched_cfg,
        Schema::new()
            .table::<Widget2OtherFingerprint>()
            .migration::<Widget2OtherFingerprint>("Widget"),
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\" has an interrupted migration to version 2 in flight (resumable \
         watermark at scope 0 row id 1) begun by a binary whose target shape (fingerprint 0x6666) \
         differs from this binary's (fingerprint 0x7777) — resuming would mix two shapes in \
         one table; finish the interrupted migration with the binary that started it, or \
         restore"
    );
}

// ------------------------------------------------------------ Step 5

#[test]
fn migrated_index_rebuilds_and_survives_a_replayed_close_out() {
    struct WidgetIndexedV1 {
        id: Id<WidgetIndexedV1>,
        count: u32,
    }
    impl Row for WidgetIndexedV1 {
        type Scope = Unscoped;
        const TABLE: &'static str = "Widget";
        const INDEXES: &'static [IndexDef<WidgetIndexedV1>] = &[IndexDef {
            name: "by_count",
            key: |r, out| r.count.encode(out),
        }];
        const VERSION: u32 = 1;
        const FINGERPRINT: Option<u64> = Some(0x9111);
        fn id(&self) -> Id<WidgetIndexedV1> {
            self.id
        }
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.id.encode(out);
            self.count.encode(out);
        }
        fn decode_row(bytes: &[u8]) -> io::Result<WidgetIndexedV1> {
            let input = &mut &bytes[..];
            Ok(WidgetIndexedV1 {
                id: Id::decode(input)?,
                count: u32::decode(input)?,
            })
        }
    }
    impl RowShape for WidgetIndexedV1 {
        const VERSION: u32 = 1;
        fn encode_row(&self, out: &mut Vec<u8>) {
            Row::encode_row(self, out)
        }
        fn decode_row(bytes: &[u8]) -> io::Result<WidgetIndexedV1> {
            <WidgetIndexedV1 as Row>::decode_row(bytes)
        }
    }

    struct WidgetIndexedV2 {
        id: Id<WidgetIndexedV2>,
        count: u32,
        label: String,
    }
    impl Row for WidgetIndexedV2 {
        type Scope = Unscoped;
        const TABLE: &'static str = "Widget";
        const INDEXES: &'static [IndexDef<WidgetIndexedV2>] = &[IndexDef {
            name: "by_count",
            key: |r, out| r.count.encode(out),
        }];
        const VERSION: u32 = 2;
        const FINGERPRINT: Option<u64> = Some(0x9222);
        fn id(&self) -> Id<WidgetIndexedV2> {
            self.id
        }
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.id.encode(out);
            self.count.encode(out);
            self.label.encode(out);
        }
        fn decode_row(bytes: &[u8]) -> io::Result<WidgetIndexedV2> {
            let input = &mut &bytes[..];
            Ok(WidgetIndexedV2 {
                id: Id::decode(input)?,
                count: u32::decode(input)?,
                label: String::decode(input)?,
            })
        }
    }
    impl RowShape for WidgetIndexedV2 {
        const VERSION: u32 = 2;
        fn encode_row(&self, out: &mut Vec<u8>) {
            Row::encode_row(self, out)
        }
        fn decode_row(bytes: &[u8]) -> io::Result<WidgetIndexedV2> {
            <WidgetIndexedV2 as Row>::decode_row(bytes)
        }
    }
    impl TableMigration for WidgetIndexedV2 {
        type Prev = WidgetIndexedV1;
        fn migrate(prev: WidgetIndexedV1) -> Result<WidgetIndexedV2, MigrationError> {
            Ok(WidgetIndexedV2 {
                id: Id::from_raw(prev.id.raw()),
                count: prev.count,
                label: "migrated".to_string(),
            })
        }
    }

    fn by_count(db: &Db, key: u32) -> Vec<(u64, u32, String)> {
        let mut rows: Vec<WidgetIndexedV2> =
            db.snapshot().unwrap().scan_by("by_count", &key).unwrap();
        rows.sort_by_key(|r| r.id.raw());
        rows.into_iter()
            .map(|r| (r.id.raw(), r.count, r.label))
            .collect()
    }

    let sim = Sim::with_faults(0x2EB7_0014, FaultConfig::none());
    {
        let mut db = Db::open(
            Box::new(sim.disk()),
            cfg(),
            Schema::new().table::<WidgetIndexedV1>(),
        )
        .unwrap();
        db.transact(|tx| -> Result<(), ()> {
            for i in 0..8u64 {
                tx.put(&WidgetIndexedV1 {
                    id: Id::from_raw(i),
                    count: i as u32,
                });
            }
            Ok(())
        })
        .unwrap();
        db.checkpoint().unwrap();
    }

    let schema = || {
        Schema::new()
            .table::<WidgetIndexedV2>()
            .migration::<WidgetIndexedV2>("Widget")
    };
    let mut db = Db::open(Box::new(sim.disk()), cfg(), schema()).unwrap();
    db.migrate_schema().unwrap();

    // The index was rebuilt from migrated rows: exactly one row per count
    // key, carrying the migrated label.
    let before: Vec<Vec<(u64, u32, String)>> = (0..8u32).map(|k| by_count(&db, k)).collect();
    for (k, got) in before.iter().enumerate() {
        assert_eq!(got, &vec![(k as u64, k as u32, "migrated".to_string())]);
    }

    // Idempotency: crash right after migrate_schema returns — the close-out
    // record is durable in the WAL, but the store's checkpoint is behind it
    // (the canonical no-checkpoint/crash/replay idiom).
    sim.restart();
    drop(db);
    let mut db2 = Db::open(Box::new(sim.disk()), cfg(), schema()).unwrap();
    db2.migrate_schema().unwrap(); // no-op: replay already landed the close-out
    let after: Vec<Vec<(u64, u32, String)>> = (0..8u32).map(|k| by_count(&db2, k)).collect();
    assert_eq!(
        before, after,
        "the replayed close-out's index rebuild must be idempotent"
    );
}

// ------------------------------------------------------------ Step 6

#[test]
fn migration_workload_replays_identically() {
    // `replays_identically` builds its worlds via `Sim::new` — `FaultConfig::
    // default()`, not a quiet world — so the workload needs the same
    // fault-classified restart-and-remedy idiom as Step 1's flagship; the
    // seed is fixed, so both runs draw the identical fault schedule and
    // retry/remedy pattern.
    charpente_sim::checks::replays_identically(0x2EB7_0015, |sim: &mut Sim| {
        if !seed_v1_result(sim, 6)? {
            return Err("64 restart rounds without a successful seed".to_string());
        }
        let Some(db) = open_and_migrate_with_remedy(sim, cfg())? else {
            return Err("128 rounds without a successful open+migrate".to_string());
        };
        let mut rows: Vec<Widget2> = db
            .snapshot()
            .map_err(|e| e.to_string())?
            .scan()
            .map_err(|e| e.to_string())?;
        rows.sort_by_key(|r| r.id.raw());
        Ok::<Vec<(u64, u32, String)>, String>(
            rows.into_iter()
                .map(|r| (r.id.raw(), r.count, r.label))
                .collect(),
        )
    })
    .unwrap();
}

// ------------------------------------------------------- Fix round 1 (I-2)
//
// A scoped table's migration, under the SAME fault matrix as
// `migration_survives_the_fault_matrix_and_equals_the_uninterrupted_twin`
// above, so `SchemaVersion.migrating_through`'s composite `(scope, id)`
// watermark is exercised under crashes, not just a clean single-shot run.
// Seeded through `Db::apply_shipped` with hand-encoded scoped Put records
// (tag 0x05), never a raw `Store::apply` — `apply_shipped` is WAL-backed,
// so a fault mid-seed recovers by WAL replay on retry exactly like every
// other seed step in this file; a raw `Store::apply` would not have that
// recovery path (there is no WAL record to replay).

struct MigOrg;

struct ScopedWidgetV1 {
    id: Id<ScopedWidgetV1>,
    count: u32,
}
impl Row for ScopedWidgetV1 {
    type Scope = MigOrg;
    const TABLE: &'static str = "ScopedWidget";
    const INDEXES: &'static [IndexDef<ScopedWidgetV1>] = &[];
    const VERSION: u32 = 1;
    const FINGERPRINT: Option<u64> = Some(0x8181);
    fn id(&self) -> Id<ScopedWidgetV1> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<ScopedWidgetV1> {
        let input = &mut &bytes[..];
        Ok(ScopedWidgetV1 {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
        })
    }
}
impl RowShape for ScopedWidgetV1 {
    const VERSION: u32 = 1;
    fn encode_row(&self, out: &mut Vec<u8>) {
        Row::encode_row(self, out)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<ScopedWidgetV1> {
        <ScopedWidgetV1 as Row>::decode_row(bytes)
    }
}

struct ScopedWidgetV2 {
    id: Id<ScopedWidgetV2>,
    count: u32,
    label: String,
}
impl Row for ScopedWidgetV2 {
    type Scope = MigOrg;
    const TABLE: &'static str = "ScopedWidget";
    const INDEXES: &'static [IndexDef<ScopedWidgetV2>] = &[IndexDef {
        name: "by_count",
        key: |r, out| r.count.encode(out),
    }];
    const VERSION: u32 = 2;
    const FINGERPRINT: Option<u64> = Some(0x8282);
    fn id(&self) -> Id<ScopedWidgetV2> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
        self.label.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<ScopedWidgetV2> {
        let input = &mut &bytes[..];
        Ok(ScopedWidgetV2 {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
            label: String::decode(input)?,
        })
    }
}
impl RowShape for ScopedWidgetV2 {
    const VERSION: u32 = 2;
    fn encode_row(&self, out: &mut Vec<u8>) {
        Row::encode_row(self, out)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<ScopedWidgetV2> {
        <ScopedWidgetV2 as Row>::decode_row(bytes)
    }
}
impl TableMigration for ScopedWidgetV2 {
    type Prev = ScopedWidgetV1;
    fn migrate(prev: ScopedWidgetV1) -> Result<ScopedWidgetV2, MigrationError> {
        Ok(ScopedWidgetV2 {
            id: Id::from_raw(prev.id.raw()),
            count: prev.count,
            label: "migrated".to_string(),
        })
    }
}

fn scoped_len_prefixed(bytes: &[u8], out: &mut Vec<u8>) {
    (bytes.len() as u32).encode(out);
    out.extend_from_slice(bytes);
}

/// A hand-encoded one-effect `0x05` (scoped Put) record for `ScopedWidgetV1`.
fn scoped_widget_v1_put_record(scope: u64, id: u64, count: u32) -> Vec<u8> {
    let mut row_bytes = Vec::new();
    Row::encode_row(
        &ScopedWidgetV1 {
            id: Id::from_raw(id),
            count,
        },
        &mut row_bytes,
    );
    let mut record = Vec::new();
    1u32.encode(&mut record);
    record.push(5);
    scoped_len_prefixed(ScopedWidgetV1::TABLE.as_bytes(), &mut record);
    scope.encode(&mut record);
    scoped_len_prefixed(&row_bytes, &mut record);
    record
}

/// A hand-encoded unscoped (`0x00`) Put record for `SchemaVersion`, the
/// first-write adoption row `Tx::put` would normally add automatically —
/// required because a versioned table's records are refused with "no
/// adoption row on record" otherwise, and the raw `apply_shipped` records
/// below bypass `Tx`'s bookkeeping entirely.
fn scoped_widget_adoption_record(version: u32, fingerprint: u64) -> Vec<u8> {
    let row = SchemaVersion {
        id: schema_version_id("ScopedWidget"),
        table: "ScopedWidget".to_string(),
        version,
        fingerprint,
        migrating_through: None,
        migrating_to: None,
    };
    let mut row_bytes = Vec::new();
    Row::encode_row(&row, &mut row_bytes);
    let mut record = Vec::new();
    1u32.encode(&mut record);
    record.push(0);
    scoped_len_prefixed(SchemaVersion::TABLE.as_bytes(), &mut record);
    scoped_len_prefixed(&row_bytes, &mut record);
    record
}

fn scoped_seed_records(rows_per_scope: u64, scopes: &[u64]) -> Vec<(u64, Vec<u8>)> {
    let mut records = vec![(0u64, scoped_widget_adoption_record(1, 0x8181))];
    let mut seq = 1u64;
    for &scope in scopes {
        for i in 0..rows_per_scope {
            records.push((seq, scoped_widget_v1_put_record(scope, i, i as u32)));
            seq += 1;
        }
    }
    records
}

fn seed_scoped_v1(sim: &Sim, rows_per_scope: u64, scopes: &[u64]) {
    let mut db = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<ScopedWidgetV1>(),
    )
    .unwrap();
    db.apply_shipped(&scoped_seed_records(rows_per_scope, scopes))
        .unwrap();
    db.checkpoint().unwrap();
}

/// `seed_scoped_v1` retried through the fault matrix, same shape as
/// `seed_v1_result` — except `apply_shipped` streams records and can
/// commit a PREFIX of them before a fault interrupts a later one (it is
/// not one atomic unit the way `Db::transact` is), so a retry resends only
/// the records past `db.last_applied()`, never restarting the whole batch
/// at seq 0 (which the log correctly refuses as out of order).
fn seed_scoped_v1_result(sim: &Sim, rows_per_scope: u64, scopes: &[u64]) -> Result<bool, String> {
    let all_records = scoped_seed_records(rows_per_scope, scopes);
    for _round in 0..64 {
        let attempt: io::Result<()> = (|| {
            let mut db = Db::open(
                Box::new(sim.disk()),
                cfg(),
                Schema::new().table::<ScopedWidgetV1>(),
            )?;
            let next = db.last_applied().map_or(0, |l| l + 1);
            let remaining: Vec<(u64, Vec<u8>)> = all_records
                .iter()
                .filter(|(seq, _)| *seq >= next)
                .cloned()
                .collect();
            db.apply_shipped(&remaining)?;
            db.checkpoint()
        })();
        match attempt {
            Ok(()) => return Ok(true),
            Err(e) => {
                let msg = e.to_string();
                if !charpente_sim::checks::is_fault_fallout(&msg) {
                    return Err(format!("non-injected failure while seeding: {msg}"));
                }
                sim.restart();
            }
        }
    }
    Ok(false)
}

/// `open_and_migrate_with_remedy`, scoped: same barrier-remedy shape.
fn open_and_migrate_scoped_with_remedy(sim: &Sim, db_cfg: DbConfig) -> Result<Option<Db>, String> {
    let mut remedy_pending = false;
    for _round in 0..128 {
        if remedy_pending {
            match Db::open(
                Box::new(sim.disk()),
                cfg(),
                Schema::new().table::<ScopedWidgetV1>(),
            ) {
                Ok(remedy_db) => {
                    drop(remedy_db);
                    remedy_pending = false;
                }
                Err(e) => {
                    let msg = e.to_string();
                    if !charpente_sim::checks::is_fault_fallout(&msg) {
                        return Err(format!("non-injected failure during barrier remedy: {msg}"));
                    }
                    sim.restart();
                }
            }
            continue;
        }
        let schema = Schema::new()
            .table::<ScopedWidgetV2>()
            .migration::<ScopedWidgetV2>("ScopedWidget");
        match Db::open(Box::new(sim.disk()), db_cfg.clone(), schema)
            .and_then(|mut d| d.migrate_schema().map(|()| d))
        {
            Ok(d) => return Ok(Some(d)),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("unrecovered writes from the previous binary") {
                    sim.cover("migrations:scoped-barrier-remedied");
                    remedy_pending = true;
                    continue;
                }
                if !charpente_sim::checks::is_fault_fallout(&msg) {
                    return Err(format!("non-injected failure: {msg}"));
                }
                sim.restart();
            }
        }
    }
    Ok(None)
}

fn migrated_scoped_twin(rows_per_scope: u64, scopes: &[u64]) -> Vec<(u64, u64, u32, String)> {
    let sim = Sim::with_faults(0x2EB7_4000, FaultConfig::none());
    seed_scoped_v1(&sim, rows_per_scope, scopes);
    let schema = Schema::new()
        .table::<ScopedWidgetV2>()
        .migration::<ScopedWidgetV2>("ScopedWidget");
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig {
            migration_batch_size: 3,
            ..cfg()
        },
        schema,
    )
    .unwrap();
    db.migrate_schema().unwrap();
    let mut rows = db
        .snapshot()
        .unwrap()
        .scan_raw(ScopedWidgetV2::TABLE, true)
        .unwrap();
    rows.sort();
    rows.into_iter()
        .map(|(scope, id, bytes)| {
            let r = <ScopedWidgetV2 as Row>::decode_row(&bytes).unwrap();
            (scope, id, r.count, r.label)
        })
        .collect()
}

/// Fix round 1, ruling R40 (Important 2): the composite watermark, exercised
/// under the fault matrix — a scoped table with rows at THREE scopes (same
/// ids repeated per scope, so a bare-id watermark would be indistinguishable
/// from correct even if it silently collapsed tenants), a batch size smaller
/// than the row count so more than one batch and one crash point falls
/// inside the table, and the terminating property this same round's fix
/// makes true again (`db8f825`'s regression made it non-terminating for any
/// scope >= 1 row).
#[test]
fn migration_survives_the_fault_matrix_for_a_scoped_table_and_equals_the_uninterrupted_twin() {
    let scopes = [1u64, 2, 3];
    let rows_per_scope = 4u64;
    let twin = migrated_scoped_twin(rows_per_scope, &scopes);
    run_universes_swarm_with!(
        0x2EB7_5000,
        128,
        |_cfg: &mut FaultConfig| {},
        |sim: &mut Sim| {
            if !seed_scoped_v1_result(sim, rows_per_scope, &scopes)? {
                sim.cover("migrations:scoped-inconclusive");
                return Ok(());
            }
            let db_cfg = DbConfig {
                migration_batch_size: 3,
                ..cfg()
            };
            let Some(db) = open_and_migrate_scoped_with_remedy(sim, db_cfg)? else {
                sim.cover("migrations:scoped-inconclusive");
                return Ok(());
            };
            sim.cover("migrations:scoped-resumed-to-completion");
            let mut rows = db
                .snapshot()
                .map_err(|e| e.to_string())?
                .scan_raw(ScopedWidgetV2::TABLE, true)
                .map_err(|e| e.to_string())?;
            rows.sort();
            let got: Vec<(u64, u64, u32, String)> = rows
                .into_iter()
                .map(|(scope, id, bytes)| {
                    let r = <ScopedWidgetV2 as Row>::decode_row(&bytes).map_err(|e| e.to_string())?;
                    Ok::<_, String>((scope, id, r.count, r.label))
                })
                .collect::<Result<_, String>>()?;
            if got != twin {
                let corruption_fired = charpente_sim::checks::durable_corruption_fired(sim);
                if corruption_fired && got.iter().all(|row| twin.contains(row)) {
                    sim.cover("migrations:scoped-corruption-excused");
                    return Ok(());
                }
                return Err(format!(
                    "faulted universe's migrated scoped store diverged from the uninterrupted \
                     twin: got {got:?}"
                ));
            }
            Ok(())
        },
        require: ["migrations:scoped-resumed-to-completion"]
    );
}
