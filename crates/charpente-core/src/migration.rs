//! Schema versioning and migration (DESIGN §3; spec docs/superpowers/
//! specs/2026-07-16-migrations-design.md): the `TableMigration` chain,
//! the structural fingerprint, and the `charpente.schema_versions`
//! platform table the `Db::open` gate reads.

use std::io;

use crate::table::{Codec, Id, IndexDef, Row, RowShape};

/// Fixed 64-bit FNV-1a, hand-rolled: `DefaultHasher`'s algorithm is not a
/// cross-toolchain stability guarantee and a silent change would fire
/// false "shape drifted" refusals with zero code changes. The macro crate
/// carries its own copy (proc-macros cannot call this crate's code).
pub fn fnv1a_hash(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// One version step: `Prev` is the immediately-prior version's shape (a
/// `#[row_shape]` struct). For `#[table(version = N)]` with `N >= 2` the
/// macro emits a const assertion requiring this impl — a bump without it
/// does not compile (spec §1).
pub trait TableMigration: RowShape {
    type Prev: RowShape;
    fn migrate(prev: Self::Prev) -> Result<Self, MigrationError>;
}

/// Why a step failed. `Decode`: the bytes did not parse under the OLD
/// version's own codec (corruption — drift is the fingerprint gate's
/// job). `Reject`: the author's fallible conversion refused a value.
#[derive(Debug)]
pub enum MigrationError {
    Decode(io::Error),
    Reject(String),
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationError::Decode(e) => write!(f, "decode failed under the prior version: {e}"),
            MigrationError::Reject(msg) => write!(f, "{msg}"),
        }
    }
}

/// One type-erased step, registered by `Schema::migration` (T3): bytes
/// encoded under `from_version` in, bytes encoded under `to_version` out.
#[derive(Clone, Copy)]
pub struct MigrationStep {
    pub from_version: u32,
    pub to_version: u32,
    pub apply: fn(&[u8]) -> Result<Vec<u8>, MigrationError>,
}

/// The erased step fn for one `TableMigration` impl.
pub fn migration_step_fn<Step: TableMigration>(bytes: &[u8]) -> Result<Vec<u8>, MigrationError> {
    let prev = Step::Prev::decode_row(bytes).map_err(MigrationError::Decode)?;
    let cur = Step::migrate(prev)?;
    let mut out = Vec::new();
    cur.encode_row(&mut out);
    Ok(out)
}

/// Platform-owned. Hand-written `Row` impl (core cannot use `#[table]` —
/// macros depend on core; `JobRow`/`EventRow` pattern). Its own
/// `FINGERPRINT` stays the default `None` — this table is not subject to
/// the gate it implements.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaVersion {
    pub id: Id<SchemaVersion>,
    pub table: String,
    pub version: u32,
    pub fingerprint: u64,
    /// `None` idle; `Some(last_row_id_done)` mid-migration — the
    /// resumable watermark (spec §3).
    pub migrating_through: Option<u64>,
    /// `None` idle; `Some(target_version)` mid-migration. Recorded so an
    /// interrupted migration resumed by a binary compiled for a DIFFERENT
    /// target is a named refusal, not a mixed-version rewrite (spec §2).
    pub migrating_to: Option<u32>,
}

impl Row for SchemaVersion {
    const TABLE: &'static str = "charpente.schema_versions";
    const INDEXES: &'static [IndexDef<SchemaVersion>] = &[];

    fn id(&self) -> Id<SchemaVersion> {
        self.id
    }

    fn encode_row(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.table.encode(out);
        self.version.encode(out);
        self.fingerprint.encode(out);
        self.migrating_through.encode(out);
        self.migrating_to.encode(out);
    }

    fn decode_row(bytes: &[u8]) -> io::Result<SchemaVersion> {
        let input = &mut &bytes[..];
        let id = Id::decode(input)?;
        let table = String::decode(input)?;
        let version = u32::decode(input)?;
        let fingerprint = u64::decode(input)?;
        let migrating_through = Option::<u64>::decode(input)?;
        let migrating_to = Option::<u32>::decode(input)?;
        if !input.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "row decode: {} trailing bytes after a SchemaVersion row — schema drift or \
                     corruption",
                    input.len()
                ),
            ));
        }
        Ok(SchemaVersion {
            id,
            table,
            version,
            fingerprint,
            migrating_through,
            migrating_to,
        })
    }
}

/// Deterministic id for a table's row — lookup is a direct `get`, no
/// index (spec §1's deliberate simplification of the original sketch).
pub fn schema_version_id(table: &str) -> Id<SchemaVersion> {
    Id::from_raw(fnv1a_hash(table.as_bytes()))
}

/// The same-version acceptance predicate the gate calls. `None` persisted
/// means the bootstrap-baseline case — nothing on disk to compare.
pub fn fingerprint_matches(persisted: Option<u64>, compiled: u64) -> bool {
    persisted.is_none_or(|p| p == compiled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_is_deterministic_and_distinguishes_inputs() {
        assert_eq!(fnv1a_hash(b"abc"), fnv1a_hash(b"abc"));
        assert_ne!(fnv1a_hash(b"abc"), fnv1a_hash(b"abd"));
        assert_ne!(fnv1a_hash(b""), fnv1a_hash(b"\0"));
    }

    #[test]
    fn schema_version_id_is_stable_per_table_name() {
        assert_eq!(schema_version_id("Invoice"), schema_version_id("Invoice"));
        assert_ne!(schema_version_id("Invoice"), schema_version_id("Org"));
    }

    #[test]
    fn schema_version_row_roundtrips_including_in_flight_fields() {
        let row = SchemaVersion {
            id: schema_version_id("Invoice"),
            table: "Invoice".to_string(),
            version: 1,
            fingerprint: 0xdead_beef,
            migrating_through: Some(41),
            migrating_to: Some(3),
        };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        assert_eq!(SchemaVersion::decode_row(&bytes).unwrap(), row);
    }

    #[test]
    fn fingerprint_matches_refuses_only_a_real_mismatch() {
        assert!(fingerprint_matches(Some(0x1111), 0x1111));
        assert!(!fingerprint_matches(Some(0x1111), 0x2222));
        assert!(fingerprint_matches(None, 0x1111));
    }

    // Hand-written two-version toy chain — exactly what #[row_shape]/
    // #[table] generate, written by hand (core cannot use its own macros).
    struct WidgetV1 {
        count: u32,
    }
    impl RowShape for WidgetV1 {
        const VERSION: u32 = 1;
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.count.encode(out);
        }
        fn decode_row(bytes: &[u8]) -> io::Result<WidgetV1> {
            let input = &mut &bytes[..];
            Ok(WidgetV1 {
                count: u32::decode(input)?,
            })
        }
    }
    struct Widget {
        count: u32,
        label: String,
    }
    impl RowShape for Widget {
        const VERSION: u32 = 2;
        fn encode_row(&self, out: &mut Vec<u8>) {
            self.count.encode(out);
            self.label.encode(out);
        }
        fn decode_row(bytes: &[u8]) -> io::Result<Widget> {
            let input = &mut &bytes[..];
            Ok(Widget {
                count: u32::decode(input)?,
                label: String::decode(input)?,
            })
        }
    }
    impl TableMigration for Widget {
        type Prev = WidgetV1;
        fn migrate(prev: WidgetV1) -> Result<Widget, MigrationError> {
            Ok(Widget {
                count: prev.count,
                label: "unlabeled".to_string(),
            })
        }
    }

    #[test]
    fn migration_step_fn_decodes_migrates_reencodes() {
        let mut old = Vec::new();
        WidgetV1 { count: 7 }.encode_row(&mut old);
        let new = migration_step_fn::<Widget>(&old).unwrap();
        let widget = Widget::decode_row(&new).unwrap();
        assert_eq!((widget.count, widget.label.as_str()), (7, "unlabeled"));
    }

    #[test]
    fn migration_step_fn_surfaces_decode_failure() {
        let err = migration_step_fn::<Widget>(&[]).unwrap_err();
        assert!(matches!(err, MigrationError::Decode(_)), "{err:?}");
    }
}
