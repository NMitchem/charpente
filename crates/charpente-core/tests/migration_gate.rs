//! The Db::open schema gate (spec §2): refusals fire pre-serving, the
//! upgrade barrier protects a foreign WAL tail, adoption waits for first
//! write.
use std::io;

use charpente_core::db::{Db, DbConfig, Schema};
use charpente_core::migration::{MigrationError, SchemaVersion, TableMigration, schema_version_id};
use charpente_core::table::{Codec, Id, IndexDef, Row, RowShape, View};
use charpente_sim::{FaultConfig, Sim};

struct WidgetV1 {
    id: Id<WidgetV1>,
    count: u32,
}
impl Row for WidgetV1 {
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
}
