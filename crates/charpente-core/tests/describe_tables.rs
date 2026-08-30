//! `Db::describe_tables` — the read-only registry walk (describe T1, spec
//! docs/superpowers/specs/2026-07-23-introspection-edge-harness-design.md
//! §1.3). Lives as its own file: engine.rs works below the `Schema`/`Db`
//! layer entirely, and materialize.rs's `Db::open`'d schema has no
//! versioned, indexed `Row` fixture to extend — both `migrations.rs` and
//! `migration_gate.rs`'s hand-impl `Row` shape is mirrored here.

use std::io;

use charpente_core::db::{Db, DbConfig, Schema};
use charpente_core::migration::{MigrationError, TableMigration};
use charpente_core::table::{Codec, Id, IndexDef, Row, RowShape, Unscoped};
use charpente_sim::{FaultConfig, Sim};

struct WidgetV1 {
    id: Id<WidgetV1>,
    org: u32,
}
impl Row for WidgetV1 {
    type Scope = Unscoped;
    const TABLE: &'static str = "Widget";
    const INDEXES: &'static [IndexDef<WidgetV1>] = &[];
    const VERSION: u32 = 1;
    const FINGERPRINT: Option<u64> = Some(0xA111);
    fn id(&self) -> Id<WidgetV1> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.org.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<WidgetV1> {
        let input = &mut &bytes[..];
        Ok(WidgetV1 {
            id: Id::decode(input)?,
            org: u32::decode(input)?,
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

struct Widget {
    id: Id<Widget>,
    org: u32,
    label: String,
}
impl Row for Widget {
    type Scope = Unscoped;
    const TABLE: &'static str = "Widget";
    const INDEXES: &'static [IndexDef<Widget>] = &[IndexDef {
        name: "by_org",
        key: |r, out| r.org.encode(out),
    }];
    const VERSION: u32 = 2;
    const FINGERPRINT: Option<u64> = Some(0xA222);
    fn id(&self) -> Id<Widget> {
        self.id
    }
    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.org.encode(out);
        self.label.encode(out);
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Widget> {
        let input = &mut &bytes[..];
        Ok(Widget {
            id: Id::decode(input)?,
            org: u32::decode(input)?,
            label: String::decode(input)?,
        })
    }
}
impl RowShape for Widget {
    const VERSION: u32 = 2;
    fn encode_row(&self, out: &mut Vec<u8>) {
        Row::encode_row(self, out)
    }
    fn decode_row(bytes: &[u8]) -> io::Result<Widget> {
        <Widget as Row>::decode_row(bytes)
    }
}
impl TableMigration for Widget {
    type Prev = WidgetV1;
    fn migrate(prev: WidgetV1) -> Result<Widget, MigrationError> {
        Ok(Widget {
            id: Id::from_raw(prev.id.raw()),
            org: prev.org,
            label: "unlabeled".to_string(),
        })
    }
}

#[test]
fn describe_tables_reports_name_version_fingerprint_indexes_and_migrations() {
    let sim = Sim::with_faults(0xD35C, FaultConfig::none());
    let db = Db::open(
        Box::new(sim.disk()),
        DbConfig::default(),
        Schema::new()
            .table::<Widget>()
            .migration::<Widget>("Widget"),
    )
    .unwrap();

    let tables = db.describe_tables();

    let widget = tables.iter().find(|t| t.name == "Widget").unwrap();
    assert_eq!(widget.version, 2);
    assert!(widget.fingerprint.is_some());
    assert_eq!(widget.index_names, vec!["by_org"]);
    assert_eq!(widget.migrations, vec![(1, 2)]);

    // Platform tables ride along, in sorted order, with their real versions:
    let names: Vec<_> = tables.iter().map(|t| t.name).collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "describe_tables must be sorted-name ordered");
    assert!(names.contains(&"charpente.jobs"));
    assert!(names.contains(&"charpente.events"));

    // A platform table with no indexes and no migrations reports empty
    // Vecs, not some other absent-data encoding.
    let jobs = tables.iter().find(|t| t.name == "charpente.jobs").unwrap();
    assert_eq!(jobs.index_names, Vec::<&str>::new());
    assert_eq!(jobs.migrations, vec![]);
}
