//! The Db::open schema gate (spec §2): refusals fire pre-serving, the
//! upgrade barrier protects a foreign WAL tail, adoption waits for first
//! write.
use std::io;

use charpente_core::db::{Db, DbConfig, Refusal, Schema};
use charpente_core::io::Disk;
use charpente_core::migration::{MigrationError, SchemaVersion, TableMigration, schema_version_id};
use charpente_core::table::{Codec, Id, IndexDef, Row, RowShape, Unscoped, View};
use charpente_sim::{FaultConfig, Sim};

/// The re-seed clause every "restore … from a replica or backup" text carries
/// (DESIGN §5's re-seed rule; S3b R3-M1), typed out so a pin goes red if it
/// drifts or goes missing.
const RESTORE_IS_A_RESEED: &str = "For a follower, or a primary once a follower is promoted in \
     its place, that is a re-seed: stop the node, delete its data dir, and restart it configured \
     as a follower of the current primary so it snapshot-seeds from it, never with a primary \
     config: the durable role file that outranks config is deleted with the data dir, so a \
     cleared node started as a primary comes up as a second writable primary";

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

/// `FINGERPRINT: None` (the `Row` trait default) — an ordinary, never-
/// versioned "Widget" table. `Db::transact`'s first-write adoption only
/// fires for `fp.is_some()` tables, so writes through this shape NEVER
/// carry a `charpente.schema_versions` adoption row: a WAL tail written by
/// this binary is indistinguishable, to a later versioned binary, from
/// corruption or a foreign log.
struct WidgetUnversioned {
    id: Id<WidgetUnversioned>,
    count: u32,
}
impl Row for WidgetUnversioned {
    type Scope = Unscoped;
    const TABLE: &'static str = "Widget";
    const INDEXES: &'static [IndexDef<WidgetUnversioned>] = &[];
    fn id(&self) -> Id<WidgetUnversioned> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.count.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<WidgetUnversioned> {
        let input = &mut &bytes[..];
        Ok(WidgetUnversioned {
            id: Id::decode(input)?,
            count: u32::decode(input)?,
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

#[test]
fn adoption_waits_for_first_write_and_lands_with_it() {
    let sim = Sim::with_faults(0x2EB7_0001, FaultConfig::none());
    let db = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<WidgetV1>(),
    )
    .unwrap();
    // Open writes nothing (spec §1: adoption-at-first-write).
    assert_eq!(db.last_applied(), None);
    let row: Option<SchemaVersion> = db
        .snapshot()
        .unwrap()
        .get(schema_version_id("Widget"))
        .unwrap();
    assert!(row.is_none());
    drop(db);
    let mut db = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<WidgetV1>(),
    )
    .unwrap();
    db.transact(|tx| -> Result<(), ()> {
        tx.put(&WidgetV1 {
            id: Id::from_raw(1),
            count: 5,
        });
        Ok(())
    })
    .unwrap();
    let row: SchemaVersion = db
        .snapshot()
        .unwrap()
        .get(schema_version_id("Widget"))
        .unwrap()
        .expect("first write must carry the adoption row");
    assert_eq!((row.version, row.fingerprint), (1, 0x1111));
    assert_eq!((row.migrating_through, row.migrating_to), (None, None));
}

#[test]
fn same_version_fingerprint_drift_refuses_to_open() {
    let sim = Sim::with_faults(0x2EB7_0002, FaultConfig::none());
    seed_v1(&sim, 1);
    let err = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<WidgetDrifted>(),
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\"'s shape changed but its #[table(version = ...)] did not — \
         persisted fingerprint 0x1111 != compiled fingerprint 0x2222 at version 1; bump the \
         version and add a TableMigration, or revert the struct change"
    );
    assert!(
        Refusal::of(&err).is_some(),
        "a fingerprint-drift barrier is a Refusal — no restart clears it, only a version bump \
         and a TableMigration do: {err}"
    );
}

#[test]
fn version_downgrade_refuses_to_open() {
    let sim = Sim::with_faults(0x2EB7_0003, FaultConfig::none());
    seed_v1(&sim, 1);
    {
        let schema = Schema::new()
            .table::<Widget2>()
            .migration::<Widget2>("Widget");
        let mut db = Db::open(Box::new(sim.disk()), cfg(), schema).unwrap();
        db.migrate_schema().unwrap();
        db.checkpoint().unwrap();
    }
    let err = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<WidgetV1>(),
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\" is at persisted version 2 but this binary is compiled for version 1 \
         — the binary is older than the data; run a binary built for at least version 2"
    );
    assert!(
        Refusal::of(&err).is_some(),
        "a version-downgrade barrier is a Refusal — no restart clears it, only a newer binary \
         does: {err}"
    );
}

#[test]
fn upgrade_barrier_refuses_a_foreign_tail_and_old_binary_recovery_clears_it() {
    let sim = Sim::with_faults(0x2EB7_0004, FaultConfig::none());
    {
        // Seed WITHOUT the clean-shutdown checkpoint, then crash-restart: the
        // store's unsynced apply reverts to nothing while the fsync'd WAL
        // record survives, so the record lives ONLY in the WAL tail — foreign
        // to a bumped binary. (A plain graceful drop would persist the apply
        // via redb's clean close, leaving no tail and no barrier; the
        // canonical no-checkpoint/crash/replay idiom is transactions.rs's
        // `sim.restart(); drop(db);`.)
        let mut db = Db::open(
            Box::new(sim.disk()),
            cfg(),
            Schema::new().table::<WidgetV1>(),
        )
        .unwrap();
        db.transact(|tx| -> Result<(), ()> {
            tx.put(&WidgetV1 {
                id: Id::from_raw(1),
                count: 5,
            });
            Ok(())
        })
        .unwrap();
        sim.restart(); // store reverts to nothing; the WAL record survives
        drop(db);
    }
    let schema = || {
        Schema::new()
            .table::<Widget2>()
            .migration::<Widget2>("Widget")
    };
    let err = Db::open(Box::new(sim.disk()), cfg(), schema()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\" has unrecovered writes from the previous binary (persisted version \
         1, this binary is compiled for version 2) — run the previous binary once to a clean \
         shutdown, then upgrade (spec §2's upgrade barrier)"
    );
    assert!(
        Refusal::of(&err).is_some(),
        "the upgrade barrier is a Refusal — no restart on this binary clears it, only running \
         the previous binary once to a clean shutdown does: {err}"
    );
    // The remedy: one old-binary open replays + checkpoints (spec §2's
    // checkpoint-at-open-after-replay)...
    drop(
        Db::open(
            Box::new(sim.disk()),
            cfg(),
            Schema::new().table::<WidgetV1>(),
        )
        .unwrap(),
    );
    // ...and the bumped binary now opens and migrates clean.
    let mut db = Db::open(Box::new(sim.disk()), cfg(), schema()).unwrap();
    db.migrate_schema().unwrap();
    let rows: Vec<Widget2> = db.snapshot().unwrap().scan().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].label, "migrated");
}

#[test]
fn migration_rewrites_every_row_and_batches_bound_the_records() {
    let sim = Sim::with_faults(0x2EB7_0005, FaultConfig::none());
    seed_v1(&sim, 5);
    let schema = Schema::new()
        .table::<Widget2>()
        .migration::<Widget2>("Widget");
    let mut db = Db::open(
        Box::new(sim.disk()),
        DbConfig {
            migration_batch_size: 2,
            ..cfg()
        },
        schema,
    )
    .unwrap();
    let before = db.last_applied();
    db.migrate_schema().unwrap();
    let rows: Vec<Widget2> = db.snapshot().unwrap().scan().unwrap();
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(row.label, "migrated");
        assert_eq!(row.count, row.id.raw() as u32);
    }
    let version: SchemaVersion = db
        .snapshot()
        .unwrap()
        .get(schema_version_id("Widget"))
        .unwrap()
        .unwrap();
    assert_eq!(version.version, 2);
    assert_eq!(
        (version.migrating_through, version.migrating_to),
        (None, None)
    );
    // batch=2 over 5 rows: 3 batch records + 1 close-out — MORE than one
    // record, proving it did not commit as a single giant transaction.
    let applied = db.last_applied().unwrap() - before.unwrap_or(0);
    assert_eq!(applied, 4);
}

#[test]
fn a_rejecting_migration_names_the_row_and_leaves_the_store_untouched() {
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
            if prev.count == 2 {
                return Err(MigrationError::Reject(format!(
                    "row {} has count 2, which this migration deliberately refuses",
                    prev.id.raw()
                )));
            }
            Ok(Widget2Rejecting {
                id: Id::from_raw(prev.id.raw()),
                count: prev.count,
            })
        }
    }

    let sim = Sim::with_faults(0x2EB7_0006, FaultConfig::none());
    seed_v1(&sim, 4); // row 2 has count 2
    let schema = Schema::new()
        .table::<Widget2Rejecting>()
        .migration::<Widget2Rejecting>("Widget");
    let mut db = Db::open(Box::new(sim.disk()), cfg(), schema).unwrap();
    let err = db.migrate_schema().unwrap_err();
    assert_eq!(
        err.to_string(),
        "migrating table \"Widget\" row 2: row 2 has count 2, which this migration \
         deliberately refuses"
    );
    assert!(
        Refusal::of(&err).is_some(),
        "a rejecting migration step is deterministic — a restart re-runs it and fails the \
         same way, so it is a Refusal, not a platform IO failure: {err}"
    );
    drop(db);
    // Nothing partially committed: the original v1 schema still opens and
    // every row is intact.
    let db = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<WidgetV1>(),
    )
    .unwrap();
    let mut rows: Vec<WidgetV1> = db.snapshot().unwrap().scan().unwrap();
    rows.sort_by_key(|r| r.id.raw());
    let counts: Vec<u32> = rows.iter().map(|r| r.count).collect();
    assert_eq!(counts, vec![0, 1, 2, 3]);
}

#[test]
fn missing_chain_link_refuses_by_name() {
    let sim = Sim::with_faults(0x2EB7_0008, FaultConfig::none());
    seed_v1(&sim, 1);
    // Compiled to version 2 with NO .migration() registered — the gap.
    let mut db = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<Widget2>(),
    )
    .unwrap();
    let err = db.migrate_schema().unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\" has no registered migration from version 1 — persisted version is \
         1, compiled version is 2; register Schema::migration::<Step>(\"Widget\") for the \
         version-1 step, or this data cannot be opened by this binary"
    );
    assert!(
        Refusal::of(&err).is_some(),
        "a missing chain link is a Refusal — no restart clears it, only registering the step \
         does: {err}"
    );
}

#[test]
fn no_adoption_row_refuses_a_foreign_tail_from_an_unversioned_binary() {
    let sim = Sim::with_faults(0x2EB7_0009, FaultConfig::none());
    {
        // Seed WITHOUT a checkpoint, then crash-restart: the store's unsynced
        // apply reverts to nothing while the fsync'd WAL record survives, so
        // the record lives ONLY in the WAL tail and a reopen must replay it
        // through TableMachine::apply (same crash idiom as the upgrade-
        // barrier test above). WidgetUnversioned's FINGERPRINT is None, so
        // this write never wrote a `charpente.schema_versions` row either.
        let mut db = Db::open(
            Box::new(sim.disk()),
            cfg(),
            Schema::new().table::<WidgetUnversioned>(),
        )
        .unwrap();
        db.transact(|tx| -> Result<(), ()> {
            tx.put(&WidgetUnversioned {
                id: Id::from_raw(1),
                count: 5,
            });
            Ok(())
        })
        .unwrap();
        sim.restart(); // store reverts to nothing; the WAL record survives
        drop(db);
    }
    // Reopen under a schema where "Widget" IS versioned: check_replay_version
    // finds no persisted `charpente.schema_versions` row for it (none was
    // ever written, in the store or the WAL tail) — the no-adoption-row
    // refusal fires before the persisted-version comparisons even run, so
    // this is provably NOT the upgrade barrier (which requires a persisted
    // row that is merely at a lower version).
    let err = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<WidgetV1>(),
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        format!(
            "replaying a record for versioned table \"Widget\" with no adoption row on record — \
             this log was not written by an adoption-aware binary, or is corrupt; restore from a \
             replica or backup. {RESTORE_IS_A_RESEED}"
        )
    );
    assert!(
        Refusal::of(&err).is_some(),
        "a missing adoption row is a Refusal — no restart clears it, only restoring from a \
         replica or backup does: {err}"
    );
}

/// The downgrade comparison inside replay itself (`check_replay_version`,
/// distinct from `check_schema_gate`'s post-replay walk that
/// `version_downgrade_refuses_to_open` above exercises): reachable only when
/// the table's own DATA record is still in the WAL tail (not checkpointed)
/// when an older binary replays it. Same crash idiom as the upgrade-barrier
/// test: seed WITHOUT a checkpoint, crash-restart so the unsynced store
/// reverts while the fsync'd tail survives, then reopen under an older
/// binary than the tail was written by.
///
/// The seed shape (`Widget2Short`) deliberately shares `WidgetV1`'s
/// FINGERPRINT (0x1111) while encoding one field short of it (`id` only, no
/// `count`) — same fingerprint, different version, an artificial combination
/// a real migration would not produce, chosen so this test can tell the
/// downgrade comparison apart from BOTH of its neighbors. A drifted
/// fingerprint would let the drift check (below this one) catch a removal of
/// this one with the same text; a byte-compatible shape (`Widget2` is a true
/// prefix-superset of `WidgetV1`) would decode cleanly either way and let
/// `check_schema_gate`'s later, unrelated walk coincidentally reproduce it
/// (both confirmed by manual mutation while writing this test). Sharing the
/// fingerprint means only this comparison can catch it; removing it here
/// falls all the way through to `WidgetV1::decode_row` running out of bytes
/// reading `count`, a distinct decode error.
#[test]
fn replaying_an_uncheckpointed_downgrade_tail_refuses_to_open() {
    struct Widget2Short {
        id: Id<Widget2Short>,
    }
    impl Row for Widget2Short {
        type Scope = Unscoped;
        const TABLE: &'static str = "Widget";
        const INDEXES: &'static [IndexDef<Widget2Short>] = &[];
        const VERSION: u32 = 2;
        const FINGERPRINT: Option<u64> = Some(0x1111);
        fn id(&self) -> Id<Widget2Short> {
            self.id
        }
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.id.encode(out);
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget2Short> {
            let input = &mut &bytes[..];
            Ok(Widget2Short {
                id: Id::decode(input)?,
            })
        }
    }

    let sim = Sim::with_faults(0x2EB7_0007, FaultConfig::none());
    {
        let mut db = Db::open(
            Box::new(sim.disk()),
            cfg(),
            Schema::new().table::<Widget2Short>(),
        )
        .unwrap();
        db.transact(|tx| -> Result<(), ()> {
            tx.put(&Widget2Short {
                id: Id::from_raw(1),
            });
            Ok(())
        })
        .unwrap();
        sim.restart(); // store reverts to nothing; the WAL record survives
        drop(db);
    }
    let err = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<WidgetV1>(),
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\" is at persisted version 2 but this binary is compiled for version 1 \
         — the binary is older than the data; run a binary built for at least version 2"
    );
    assert!(
        Refusal::of(&err).is_some(),
        "a replay-time downgrade is a Refusal — no restart on this binary clears it, only a \
         newer binary does: {err}"
    );
}

/// The fingerprint-drift comparison inside replay itself (`check_replay_version`,
/// distinct from `check_schema_gate`'s post-replay walk that
/// `same_version_fingerprint_drift_refuses_to_open` above exercises): same
/// crash idiom as the downgrade test above, with the reopening binary drifted
/// instead of behind.
#[test]
fn replaying_an_uncheckpointed_drift_tail_refuses_to_open() {
    let sim = Sim::with_faults(0x2EB7_0023, FaultConfig::none());
    {
        let mut db = Db::open(
            Box::new(sim.disk()),
            cfg(),
            Schema::new().table::<WidgetV1>(),
        )
        .unwrap();
        db.transact(|tx| -> Result<(), ()> {
            tx.put(&WidgetV1 {
                id: Id::from_raw(1),
                count: 5,
            });
            Ok(())
        })
        .unwrap();
        sim.restart(); // store reverts to nothing; the WAL record survives
        drop(db);
    }
    let err = Db::open(
        Box::new(sim.disk()),
        cfg(),
        Schema::new().table::<WidgetDrifted>(),
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\"'s shape changed but its #[table(version = ...)] did not — \
         persisted fingerprint 0x1111 != compiled fingerprint 0x2222 at version 1; bump the \
         version and add a TableMigration, or revert the struct change"
    );
    assert!(
        Refusal::of(&err).is_some(),
        "a replay-time fingerprint drift is a Refusal — no restart clears it, only a version \
         bump and a TableMigration do: {err}"
    );
}

// ------------------------------------------------------------------------
// Migrations T9 interlude regressions (spec §2, §5). All three were RED at
// HEAD 238823d and are the deterministic reproductions of the two findings the
// T9 property suite surfaced (dossier: .superpowers/sdd/migrations-t9-interlude.md).

/// FINDING 1 (RED at 238823d). A store discarded and rebuilt from the WAL after
/// a COMPLETED migration used to trip the upgrade barrier — the WAL still holds
/// the pre-bump v1 records, and replay-from-zero decoded them under the v2
/// binary. Deleting the store file lands in the exact state `Db::open`'s
/// discard-and-rebuild produces (empty store, applied_seq=None, full replay).
/// The fix decode-free-replays the below-version records and admits because the
/// WAL's own later records migrate the table to the compiled version.
#[test]
fn finding1_completed_migration_reopens_after_a_store_rebuild() {
    let sim = Sim::with_faults(0x2EB7_0020, FaultConfig::none());
    seed_v1(&sim, 5);
    let v2 = || {
        Schema::new()
            .table::<Widget2>()
            .migration::<Widget2>("Widget")
    };
    {
        let mut db = Db::open(Box::new(sim.disk()), cfg(), v2()).unwrap();
        db.migrate_schema().unwrap();
        db.checkpoint().unwrap();
    }
    // Discard the store file: the discard-and-rebuild end state.
    sim.disk().remove("t.redb").unwrap();
    // Reopen v2 — the rebuilt store must come back at v2 with the migrated rows,
    // not refuse. (At 238823d this returned the upgrade-barrier error, and the
    // "run the previous binary" remedy was a dead end: the v1 binary then
    // refused with the downgrade error — a single-node brick.)
    let db = Db::open(Box::new(sim.disk()), cfg(), v2()).unwrap();
    let mut rows: Vec<Widget2> = db.snapshot().unwrap().scan().unwrap();
    rows.sort_by_key(|r| r.id.raw());
    assert_eq!(rows.len(), 5);
    for (i, r) in rows.iter().enumerate() {
        assert_eq!(
            (r.id.raw(), r.count, r.label.as_str()),
            (i as u64, i as u32, "migrated")
        );
    }
    let sv: SchemaVersion = db
        .snapshot()
        .unwrap()
        .get(schema_version_id("Widget"))
        .unwrap()
        .unwrap();
    assert_eq!((sv.version, sv.migrating_to), (2, None));
}

/// FINDING 1, case (b): a store discarded BEFORE any migration ran. The WAL
/// holds a clean, complete v1 history but no migration records, so the rebuild
/// cannot tell it from an unrecovered foreign v1 tail (the barrier test above's
/// cousin) — and correctly BARRIERS (fail-closed retained, Noah ruling 1). The
/// barrier's remedy works here (unlike the completed-migration case): one
/// old-binary open recovers+checkpoints the v1 store, then the v2 binary
/// migrates clean.
#[test]
fn caseb_rebuild_before_migration_barriers_then_the_remedy_clears_it() {
    let sim = Sim::with_faults(0x2EB7_0021, FaultConfig::none());
    seed_v1(&sim, 5);
    let v2 = || {
        Schema::new()
            .table::<Widget2>()
            .migration::<Widget2>("Widget")
    };
    // Discard the store BEFORE migrating.
    sim.disk().remove("t.redb").unwrap();
    // Open v2 → the barrier fires (indistinguishable from a foreign tail).
    let err = Db::open(Box::new(sim.disk()), cfg(), v2()).unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\" has unrecovered writes from the previous binary (persisted version \
         1, this binary is compiled for version 2) — run the previous binary once to a clean \
         shutdown, then upgrade (spec §2's upgrade barrier)"
    );
    assert!(
        Refusal::of(&err).is_some(),
        "a full-rebuild upgrade barrier is a Refusal — no restart on this binary clears it, \
         only running the previous binary once to a clean shutdown does: {err}"
    );
    // The remedy: one old-binary open recovers+checkpoints from the WAL...
    drop(
        Db::open(
            Box::new(sim.disk()),
            cfg(),
            Schema::new().table::<WidgetV1>(),
        )
        .unwrap(),
    );
    // ...then the v2 binary opens and migrates clean.
    let mut db = Db::open(Box::new(sim.disk()), cfg(), v2()).unwrap();
    db.migrate_schema().unwrap();
    let rows: Vec<Widget2> = db.snapshot().unwrap().scan().unwrap();
    assert_eq!(rows.len(), 5);
    assert!(rows.iter().all(|r| r.label == "migrated"));
}

/// FINDING 2 (RED at 238823d). Resuming an interrupted migration under a v2
/// schema of the SAME version but a DIFFERENT fingerprint used to be admitted,
/// silently completing with the foreign transform and leaving a mixed-shape
/// table (rows below the watermark at the original target's bytes, rows above
/// at the resuming binary's). The persisted `fingerprint` field already holds
/// the original target's fingerprint mid-flight, so the gate can and now does
/// refuse by name.
#[test]
fn finding2_cross_fingerprint_resume_refuses_by_name() {
    // Two v2 shapes, same version, different fingerprint AND layout. `Widget2A`
    // begins the migration (rejecting row count 2 so batch 1 commits and the
    // watermark lands at row id 1); `Widget2B` tries to resume it.
    struct Widget2A {
        id: Id<Widget2A>,
        count: u32,
        label: String,
    }
    impl Row for Widget2A {
        type Scope = Unscoped;
        const TABLE: &'static str = "Widget";
        const INDEXES: &'static [IndexDef<Widget2A>] = &[];
        const VERSION: u32 = 2;
        const FINGERPRINT: Option<u64> = Some(0x2A2A);
        fn id(&self) -> Id<Widget2A> {
            self.id
        }
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.id.encode(out);
            self.count.encode(out);
            self.label.encode(out);
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget2A> {
            let input = &mut &bytes[..];
            Ok(Widget2A {
                id: Id::decode(input)?,
                count: u32::decode(input)?,
                label: String::decode(input)?,
            })
        }
    }
    impl RowShape for Widget2A {
        const VERSION: u32 = 2;
        fn encode_row(&self, out: &mut Vec<u8>) {
            Row::encode_row(self, out)
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget2A> {
            <Widget2A as Row>::decode_row(bytes)
        }
    }
    impl TableMigration for Widget2A {
        type Prev = WidgetV1;
        fn migrate(prev: WidgetV1) -> Result<Widget2A, MigrationError> {
            if prev.count == 2 {
                return Err(MigrationError::Reject("A refuses count 2".to_string()));
            }
            Ok(Widget2A {
                id: Id::from_raw(prev.id.raw()),
                count: prev.count,
                label: "shapeA".to_string(),
            })
        }
    }

    struct Widget2B {
        id: Id<Widget2B>,
        count: u32,
        flag: bool,
        note: String,
    }
    impl Row for Widget2B {
        type Scope = Unscoped;
        const TABLE: &'static str = "Widget";
        const INDEXES: &'static [IndexDef<Widget2B>] = &[];
        const VERSION: u32 = 2;
        const FINGERPRINT: Option<u64> = Some(0x2B2B);
        fn id(&self) -> Id<Widget2B> {
            self.id
        }
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.id.encode(out);
            self.count.encode(out);
            self.flag.encode(out);
            self.note.encode(out);
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget2B> {
            let input = &mut &bytes[..];
            Ok(Widget2B {
                id: Id::decode(input)?,
                count: u32::decode(input)?,
                flag: bool::decode(input)?,
                note: String::decode(input)?,
            })
        }
    }
    impl RowShape for Widget2B {
        const VERSION: u32 = 2;
        fn encode_row(&self, out: &mut Vec<u8>) {
            Row::encode_row(self, out)
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget2B> {
            <Widget2B as Row>::decode_row(bytes)
        }
    }
    impl TableMigration for Widget2B {
        type Prev = WidgetV1;
        fn migrate(prev: WidgetV1) -> Result<Widget2B, MigrationError> {
            Ok(Widget2B {
                id: Id::from_raw(prev.id.raw()),
                count: prev.count,
                flag: true,
                note: "shapeB".to_string(),
            })
        }
    }

    let sim = Sim::with_faults(0x2EB7_0022, FaultConfig::none());
    seed_v1(&sim, 4); // ids 0..4, counts 0..4 — row 2 has count 2
    let batched = DbConfig {
        migration_batch_size: 2,
        ..cfg()
    };
    // Begin under A: batch 1 (rows 0,1) commits, batch 2 (row 2) rejects →
    // durable in-flight watermark at row id 1, migrating_to=Some(2).
    {
        let mut db = Db::open(
            Box::new(sim.disk()),
            batched.clone(),
            Schema::new()
                .table::<Widget2A>()
                .migration::<Widget2A>("Widget"),
        )
        .unwrap();
        db.migrate_schema().unwrap_err();
    }
    // Resume under B (same version 2, fingerprint 0x2b2b ≠ 0x2a2a): refused by
    // name — no mixed-shape table.
    let err = Db::open(
        Box::new(sim.disk()),
        batched,
        Schema::new()
            .table::<Widget2B>()
            .migration::<Widget2B>("Widget"),
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "table \"Widget\" has an interrupted migration to version 2 in flight (resumable \
         watermark at scope 0 row id 1) begun by a binary whose target shape (fingerprint \
         0x2a2a) differs from this binary's (fingerprint 0x2b2b) — resuming would mix two \
         shapes in one table; finish the interrupted migration with the binary that started \
         it, or restore"
    );
}
