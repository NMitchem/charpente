//! The `MigratePut` apply path and the close-out index rebuild (migrations
//! T4, spec §3). Proves at the store level what the db.rs record-roundtrip
//! test cannot: that `put_raw` overwrites a row's bytes WITHOUT touching its
//! indexes, and that applying the close-out `charpente.schema_versions` Put
//! (a `migrating_to` Some -> None transition with a version bump) rebuilds
//! that table's indexes AT APPLY.
//!
//! Route (per the task brief's fallback ladder): this is an integration
//! test, not a db.rs `#[cfg(test)]` unit test, because building a `Db`
//! needs a `Disk` and the only impl is `charpente-sim`'s — which, per
//! `tests/materialize.rs`'s header, cannot unify inside `charpente-core`'s
//! own `--cfg test` compilation. `Tx::migrate_put` is `pub(crate)` and so
//! unreachable from here, so the one migration record is hand-encoded (the
//! frozen tag-4 wire shape) and driven through the public `Db::apply_shipped`
//! — which is exactly the follower apply path, one of the three (primary,
//! replay, follower) the design requires rebuild identically at the same
//! log position.

use std::io;

use charpente_core::db::{Db, DbConfig, Schema, Tx};
use charpente_core::migration::{SchemaVersion, schema_version_id};
use charpente_core::table::{Codec, Id, IndexDef, Row, Unscoped, View};
use charpente_sim::{FaultConfig, Sim};

// A hand-written indexed table (core cannot use its own `#[table]` macro):
// one string field with a secondary index on it.
struct Widget {
    id: Id<Widget>,
    key: String,
}

fn widget_by_key(w: &Widget, out: &mut Vec<u8>) {
    w.key.encode(out);
}

impl Row for Widget {
    type Scope = Unscoped;
    const TABLE: &'static str = "Widget";
    const INDEXES: &'static [IndexDef<Widget>] = &[IndexDef {
        name: "by_key",
        key: widget_by_key,
    }];

    fn id(&self) -> Id<Widget> {
        self.id
    }

    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.key.encode(out);
    }

    fn decode_row(bytes: &[u8]) -> io::Result<Widget> {
        let input = &mut &bytes[..];
        let id = Id::decode(input)?;
        let key = String::decode(input)?;
        Ok(Widget { id, key })
    }
}

fn widget_bytes(id: u64, key: &str) -> Vec<u8> {
    let mut out = Vec::new();
    Widget {
        id: Id::from_raw(id),
        key: key.to_string(),
    }
    .encode_row(&mut out);
    out
}

// The single-effect `MigratePut` record, hand-encoded to the frozen tag-4
// wire shape (`db::encode_record`): `[n=1: u32 le][tag=4][table: u32 len +
// bytes][id: u64 le][row: u32 len + bytes]`.
fn migrate_put_record(table: &str, id: u64, row: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    1u32.encode(&mut out);
    out.push(4);
    (table.len() as u32).encode(&mut out);
    out.extend_from_slice(table.as_bytes());
    id.encode(&mut out);
    (row.len() as u32).encode(&mut out);
    out.extend_from_slice(row);
    out
}

#[test]
fn migrate_put_overwrites_without_indexing_then_close_out_rebuilds() {
    let sim = Sim::with_faults(0x2EB7_00F4, FaultConfig::none());
    let schema = Schema::new().table::<Widget>();
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), schema).unwrap();

    // Seed: one Widget keyed "old", and the in-flight migration marker for
    // its table (migrating_to Some, version still at the old value).
    db.transact(|tx: &mut Tx| {
        tx.put(&Widget {
            id: Id::from_raw(7),
            key: "old".to_string(),
        });
        tx.put(&SchemaVersion {
            id: schema_version_id("Widget"),
            table: "Widget".to_string(),
            version: 1,
            fingerprint: 0,
            migrating_through: Some(7),
            migrating_to: Some(2),
        });
        Ok::<(), ()>(())
    })
    .unwrap();

    // Apply the decode-free migration write: row 7's bytes become "new".
    let next = db.last_applied().map_or(0, |l| l + 1);
    db.apply_shipped(&[(
        next,
        migrate_put_record("Widget", 7, &widget_bytes(7, "new")),
    )])
    .unwrap();

    // After MigratePut alone: the row decodes to "new" (put_raw overwrote the
    // bytes) but the index is untouched — it still has only the "old" entry,
    // which now points at a row whose value is "new".
    {
        let snap = db.snapshot().unwrap();
        assert!(
            snap.scan_by::<Widget>("by_key", &"new".to_string())
                .unwrap()
                .is_empty(),
            "MigratePut must not create index entries — that is the close-out's job"
        );
        let stale = snap
            .scan_by::<Widget>("by_key", &"old".to_string())
            .unwrap();
        assert_eq!(stale.len(), 1, "the old index entry survives MigratePut");
        assert_eq!(
            stale[0].key, "new",
            "put_raw overwrote the row bytes (decode-free) without touching the index"
        );
    }

    // Close out the migration: a SchemaVersion Put clearing migrating_to and
    // bumping the version. This must rebuild Widget's indexes at apply.
    db.transact(|tx: &mut Tx| {
        tx.put(&SchemaVersion {
            id: schema_version_id("Widget"),
            table: "Widget".to_string(),
            version: 2,
            fingerprint: 0,
            migrating_through: None,
            migrating_to: None,
        });
        Ok::<(), ()>(())
    })
    .unwrap();

    // The rebuild fired: the index now reflects the migrated bytes.
    let snap = db.snapshot().unwrap();
    let by_new = snap
        .scan_by::<Widget>("by_key", &"new".to_string())
        .unwrap();
    assert_eq!(
        by_new.len(),
        1,
        "close-out rebuild indexed the migrated row"
    );
    assert_eq!(by_new[0].key, "new");
    assert!(
        snap.scan_by::<Widget>("by_key", &"old".to_string())
            .unwrap()
            .is_empty(),
        "the stale 'old' index entry is gone after the rebuild"
    );
}

// T11 final-review fix (Finding 3): `after_id == Some(u64::MAX)` must not
// re-include the row at u64::MAX forever. `saturating_add(1)` on u64::MAX
// stays at u64::MAX, so `from..` never advances past it — the batching
// executor would re-read and re-decode that row under the target codec
// every batch, never reaching close-out (a permanent brick for any table
// with a row at that id). The fix short-circuits: a watermark at u64::MAX
// is past every possible id, so the next batch is empty by construction.
#[test]
fn scan_raw_from_at_u64_max_watermark_returns_empty() {
    let sim = Sim::with_faults(0x2EB7_00F5, FaultConfig::none());
    let schema = Schema::new().table::<Widget>();
    let mut db = Db::open(Box::new(sim.disk()), DbConfig::default(), schema).unwrap();

    db.transact(|tx: &mut Tx| {
        tx.put(&Widget {
            id: Id::from_raw(u64::MAX),
            key: "last".to_string(),
        });
        Ok::<(), ()>(())
    })
    .unwrap();

    let snap = db.snapshot().unwrap();
    // Sanity: the row is really there and a `None` watermark sees it.
    assert_eq!(snap.scan_raw_from("Widget", None, 10).unwrap().len(), 1);
    // The watermark past every id must not re-include it.
    assert!(
        snap.scan_raw_from("Widget", Some(u64::MAX), 10)
            .unwrap()
            .is_empty(),
        "a watermark at u64::MAX must see no further rows — otherwise the row at \
         u64::MAX would re-enter every subsequent migration batch forever"
    );
}
