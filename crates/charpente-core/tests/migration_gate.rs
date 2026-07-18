//! The Db::open schema gate (spec §2): refusals fire pre-serving, the
//! upgrade barrier protects a foreign WAL tail, adoption waits for first
//! write.
use std::io;

use charpente_core::db::{Db, DbConfig, Schema};
use charpente_core::migration::{SchemaVersion, schema_version_id};
use charpente_core::table::{Codec, Id, IndexDef, Row, View};
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
