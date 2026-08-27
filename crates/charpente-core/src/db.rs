//! Typed transactions over the log (DESIGN.md §2, PLAN.md M0 step 4): a
//! transaction is `fn(&mut Tx) -> Result<T, E>` — reads come from a snapshot
//! plus the transaction's own writes; effects buffer and serialize as ONE
//! log record; the store applies them only after the record is durable.
//! Serializable by construction: `transact` takes `&mut Db`, so two
//! transactions cannot exist at once.
//!
//! Record payload v0: `[n: u32 le]` then n tagged effects —
//! `0x00 [table][row]` put, `0x01 [table][id: u64 le]` delete,
//! `0x02 [job][payload][key: Option<String>]` enqueue,
//! `0x03 [event][payload][key: Option<String>]` emit — strings and byte
//! fields length-prefixed per the `Codec` layout.
//! Table effects are the transaction's collapsed overlay in (table, id)
//! order: intermediate overwrites inside a transaction are invisible to
//! every observer and are not logged. Enqueues and emits keep call order.
//! The enqueue key is the job's idempotency identity to the world outside
//! the log — carried, never interpreted inside the deterministic boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;

use crate::engine::{Engine, StateMachine, SubmitFailure};
use crate::event::EventRow;
use crate::io::{Disk, File};
use crate::job::JobRow;
use crate::log::{MAX_RECORD_BYTES, WalConfig, parse_segment_name, segment_name};
use crate::table::{Codec, Id, Row, RowShape, Snapshot, Store, View, Writer};

// ---------------------------------------------------------------- config

#[derive(Clone, Debug)]
pub struct DbConfig {
    pub wal: WalConfig,
    /// The store's file name inside the `Disk` namespace.
    pub store_path: String,
    /// Segments `Db::checkpoint` always retains past the checkpoint floor,
    /// even when GC would otherwise delete more.
    pub retain_segments: usize,
    /// Auto-`checkpoint` after this many committed records; `0` disables the
    /// cadence (manual `Db::checkpoint` only).
    pub checkpoint_every: u64,
    /// How many of the most recent `EventRow`s `checkpoint`'s GC retains;
    /// `0` retains nothing beyond the current tail. Count-based rather than
    /// time-based: v0 has no transactional wall-time source (DESIGN §6).
    pub event_retention: u64,
    /// Rows per transaction while a table migrates (spec §3) — bounds one
    /// migration commit's WAL record size.
    pub migration_batch_size: u64,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        DbConfig {
            wal: WalConfig::default(),
            store_path: "tables.redb".to_string(),
            retain_segments: 2,
            checkpoint_every: 4096,
            event_retention: 65_536,
            migration_batch_size: 1000,
        }
    }
}

// ---------------------------------------------------------------- traits

/// A queued unit of work (DESIGN.md §6). `tx.enqueue` records it atomically
/// with the transaction's state changes; dispatch lives in
/// `charpente::kernel`. v0: implemented by hand; `#[job]` derives it.
pub trait Job: Codec {
    const NAME: &'static str;
    const RETRY: crate::job::RetryPolicy = crate::job::RetryPolicy::DEFAULT;
}

/// A realtime event (DESIGN.md §6). Emits materialize into `charpente.events`
/// at apply, the same way enqueues materialize `JobRow` — subscribers read
/// through the ranged scan (`View::scan_from`), not the log directly.
/// v0: implemented by hand; `#[event]` will derive it.
pub trait Event: Codec {
    const NAME: &'static str;
}

/// Why a transaction did not return `Ok`: the application's own rejection,
/// or a platform IO failure. The distinction is load-bearing — `App` means
/// nothing was written; `Io` after the WAL sync means the record is durable
/// but unacknowledged and will resurface on recovery.
#[derive(Debug)]
pub enum TxError<E> {
    App(E),
    Io(io::Error),
    /// The follower rejection (DESIGN §5): this node has been marked a
    /// read-only follower (`Db::forbid_writes`) and the transaction had at
    /// least one effect to write — nothing was attempted. `primary` names
    /// where to send it instead. Reads and effect-free transactions never
    /// see this; the gate only looks at `f`'s buffered effects.
    NotPrimary {
        primary: String,
    },
    /// The witness-verification rejection (M2 sub-project 0b, dual-primary
    /// gap): this node was a durable primary before restarting and has not
    /// yet confirmed that none of the followers it shipped to (its
    /// "witnesses") were promoted in its absence (`Db::verify_writes`).
    /// Unlike `NotPrimary`, this node does not know whether a primary exists
    /// elsewhere — it may still be the legitimate one — so there is nothing
    /// honest to name as "the" primary. `unreached` is the witness set this
    /// node is verifying, captured when the gate closed: the replication
    /// caller sets it once and never rewrites it, so it does NOT shrink as
    /// individual witnesses answer (`Repl::resolve_witness_gate` carries the
    /// reason a per-tick re-gate would be unsafe). The gate resolves
    /// automatically (to `Allowed` or a demotion) once every witness has
    /// answered. Reads and effect-free transactions never see this, same as
    /// `NotPrimary`.
    Verifying {
        unreached: Vec<String>,
    },
}

/// Collapses a `TxError` from a platform-internal transaction (the schema
/// migration executor) into a plain `io::Error`: those transactions have no
/// application error type worth preserving, and both `NotPrimary` and
/// `Verifying` are unreachable (`migrate_schema` asserts `writes_allowed()`
/// before running, and both variants are exactly the states where that's
/// false).
fn tx_err_to_io<E: fmt::Debug>(e: TxError<E>) -> io::Error {
    match e {
        TxError::Io(e) => e,
        TxError::App(e) => io::Error::other(format!("schema migration transact rejected: {e:?}")),
        TxError::NotPrimary { primary } => io::Error::other(format!(
            "schema migration ran on a non-primary node (primary: {primary}) — unreachable: \
             migrate_schema asserts writes are allowed before running"
        )),
        TxError::Verifying { unreached } => io::Error::other(format!(
            "schema migration ran on a witness-verifying node (unreached: {unreached:?}) — \
             unreachable: migrate_schema asserts writes are allowed before running"
        )),
    }
}

// ---------------------------------------------------------------- schema

#[derive(Clone)]
struct TableOps {
    apply_put: fn(&mut Writer<'_>, &[u8]) -> io::Result<()>,
    apply_delete: fn(&mut Writer<'_>, u64) -> io::Result<()>,
    // Rebuilds every one of the table's secondary indexes from its rows;
    // invoked by the migration close-out trigger in `TableMachine::apply`.
    rebuild_indexes: fn(&mut Writer<'_>) -> io::Result<()>,
    version: u32,
    fingerprint: Option<u64>,
    // `IndexDef<R>` is generic per-`Row`, so no `&'static [&'static str]` of
    // names exists to borrow — projected once at registration time instead.
    index_names: Vec<&'static str>,
}

fn apply_put<R: Row>(w: &mut Writer<'_>, row: &[u8]) -> io::Result<()> {
    w.put(&R::decode_row(row)?)
}

fn apply_delete<R: Row>(w: &mut Writer<'_>, id: u64) -> io::Result<()> {
    w.delete::<R>(Id::from_raw(id))
}

fn rebuild_indexes<R: Row>(w: &mut Writer<'_>) -> io::Result<()> {
    w.rebuild_indexes_for::<R>()
}

/// One registered table's introspectable identity — the describe walk's raw
/// material (spec §1.3). Read-only; constructed only by `Db::describe_tables`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDescription {
    pub name: &'static str,
    pub version: u32,
    pub fingerprint: Option<u64>,
    pub index_names: Vec<&'static str>,
    /// Registered migration steps as (from_version, to_version), ascending.
    pub migrations: Vec<(u32, u32)>,
}

/// One sealed WAL segment, as `Db::sealed_segments` reports it (M2
/// object-storage-archive T5, ruling 30).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedSegment {
    pub base_seq: u64,
    /// The segment's end BY LAYOUT (the next segment's base minus one),
    /// INCLUSIVE — the archive's completeness TARGET for this segment,
    /// never the value it is entitled to record in a `SegmentRef`, which
    /// must come from what `Db::read_log` actually returned (a sealed
    /// segment whose tail records are not yet applied short-reads; ruling
    /// 29).
    pub layout_end: u64,
}

/// The set of tables a `Db` can replay log records into. Every `#[table]`
/// type the application writes must be registered — replay works on raw
/// record bytes and needs the typed codecs to maintain indexes.
#[derive(Default, Clone)]
pub struct Schema {
    tables: BTreeMap<&'static str, TableOps>,
    migrations: BTreeMap<&'static str, Vec<crate::migration::MigrationStep>>,
}

impl Schema {
    pub fn new() -> Schema {
        Schema::default()
    }

    pub fn table<R: Row>(mut self) -> Schema {
        let prev = self.tables.insert(
            R::TABLE,
            TableOps {
                apply_put: apply_put::<R>,
                apply_delete: apply_delete::<R>,
                rebuild_indexes: rebuild_indexes::<R>,
                version: R::VERSION,
                fingerprint: R::FINGERPRINT,
                index_names: R::INDEXES.iter().map(|d| d.name).collect(),
            },
        );
        assert!(
            prev.is_none(),
            "Schema: table \"{}\" registered twice — register each #[table] type once",
            R::TABLE
        );
        self
    }

    /// Every registered table name, sorted (`BTreeMap` order) — the export
    /// path's walk order: a caller builds the same `Schema` on both sides
    /// and iterates this to know which tables to `Snapshot::scan_raw`.
    pub fn table_names(&self) -> Vec<&'static str> {
        self.tables.keys().copied().collect()
    }

    /// Registers one migration step for `table` (which must already be
    /// `.table::<R>()`-registered under the same name). Steps may be
    /// registered in any order; the executor looks them up by
    /// `from_version`.
    pub fn migration<Step: crate::migration::TableMigration>(
        mut self,
        table: &'static str,
    ) -> Schema {
        assert!(
            self.tables.contains_key(table),
            "Schema: migration() registered for table \"{table}\" but it was never .table()'d \
             — call .table::<R>() for \"{table}\" before registering its migrations"
        );
        self.migrations
            .entry(table)
            .or_default()
            .push(crate::migration::MigrationStep {
                from_version: Step::Prev::VERSION,
                to_version: Step::VERSION,
                apply: crate::migration::migration_step_fn::<Step>,
            });
        self
    }

    pub fn migrations_for(&self, table: &str) -> &[crate::migration::MigrationStep] {
        self.migrations.get(table).map_or(&[], Vec::as_slice)
    }

    /// A registered table's `(version, fingerprint)`.
    pub fn table_meta(&self, table: &str) -> Option<(u32, Option<u64>)> {
        self.tables
            .get(table)
            .map(|ops| (ops.version, ops.fingerprint))
    }

    /// Drops one table's ops. Used only by `Db::into_seeder` to strip the
    /// platform-owned registrations — `JobRow`, `EventRow`, `SchemaVersion`
    /// (`Db::open` always adds all three) — before handing the schema to a
    /// fresh `Seeder`, which re-adds them itself; without this, that would
    /// hit `table`'s own duplicate-registration assert.
    pub(crate) fn remove(&mut self, table: &'static str) {
        self.tables.remove(table);
    }

    fn ops(&self, table: &str) -> io::Result<&TableOps> {
        self.tables.get(table).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "log record references table \"{table}\" but the Schema does not include \
                     it — register it in the Schema passed to Db::open, or migrate the log"
                ),
            )
        })
    }
}

/// The pure fold behind `Db::schema_digest` (kept free so unit tests can
/// build a `Schema` directly without a `Db`/`Disk`): one aggregate over
/// every versioned table's `(name, version, fingerprint)`, sorted by name
/// (BTreeMap order). `FINGERPRINT: None` tables are excluded — platform
/// tables are covered by PROTO_VERSION, and a hand-written app `Row` impl
/// opts out of the digest exactly as it opts out of the gate (spec §4
/// scope note).
pub(crate) fn schema_digest_of(schema: &Schema) -> u64 {
    let mut buf = Vec::new();
    for table in schema.table_names() {
        let Some((version, Some(fingerprint))) = schema.table_meta(table) else {
            continue;
        };
        buf.extend_from_slice(table.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&version.to_le_bytes());
        buf.extend_from_slice(&fingerprint.to_le_bytes());
    }
    crate::migration::fnv1a_hash(&buf)
}

// ---------------------------------------------------------------- record

#[derive(Debug, PartialEq)]
enum Effect {
    Put {
        table: String,
        row: Vec<u8>,
    },
    Delete {
        table: String,
        id: u64,
    },
    Enqueue {
        job: String,
        payload: Vec<u8>,
        key: Option<String>,
    },
    Emit {
        event: String,
        payload: Vec<u8>,
        key: Option<String>,
    },
    MigratePut {
        table: String,
        id: u64,
        row: Vec<u8>,
    },
}

/// The shape of a collapsed table overlay: table -> id -> optional row bytes
/// (`None` = tombstone). Both a transaction's own buffered writes
/// (`TxEffects::overlay`, below) and a `Group`'s accumulated writes across a
/// batch (spec §2) are this same alias — the sameness holds by compiler,
/// not by comment, since `stage` merges one into the other.
type GroupOverlay = BTreeMap<&'static str, BTreeMap<u64, Option<Vec<u8>>>>;

/// A transaction's buffered effects: the collapsed per-table overlay
/// (`None` = tombstone) plus enqueues and emits in call order.
#[derive(Default)]
struct TxEffects {
    overlay: GroupOverlay,
    enqueues: Vec<(&'static str, Vec<u8>, Option<String>)>,
    emits: Vec<(&'static str, Vec<u8>, Option<String>)>,
    migrate_puts: Vec<(&'static str, u64, Vec<u8>)>,
}

impl TxEffects {
    fn is_empty(&self) -> bool {
        self.overlay.values().all(BTreeMap::is_empty)
            && self.enqueues.is_empty()
            && self.emits.is_empty()
            && self.migrate_puts.is_empty()
    }
}

fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    (u32::try_from(bytes.len()).expect("field exceeds u32 length")).encode(out);
    out.extend_from_slice(bytes);
}

fn encode_record(effects: &TxEffects) -> Vec<u8> {
    let n = effects.overlay.values().map(BTreeMap::len).sum::<usize>()
        + effects.enqueues.len()
        + effects.emits.len()
        + effects.migrate_puts.len();
    let mut out = Vec::new();
    (u32::try_from(n).expect("transaction exceeds u32 effects")).encode(&mut out);
    for (table, entries) in &effects.overlay {
        for (id, entry) in entries {
            match entry {
                Some(row) => {
                    out.push(0);
                    encode_bytes(table.as_bytes(), &mut out);
                    encode_bytes(row, &mut out);
                }
                None => {
                    out.push(1);
                    encode_bytes(table.as_bytes(), &mut out);
                    id.encode(&mut out);
                }
            }
        }
    }
    for (job, payload, key) in &effects.enqueues {
        out.push(2);
        encode_bytes(job.as_bytes(), &mut out);
        encode_bytes(payload, &mut out);
        key.encode(&mut out);
    }
    for (event, payload, key) in &effects.emits {
        out.push(3);
        encode_bytes(event.as_bytes(), &mut out);
        encode_bytes(payload, &mut out);
        key.encode(&mut out);
    }
    for (table, id, row) in &effects.migrate_puts {
        out.push(4);
        encode_bytes(table.as_bytes(), &mut out);
        id.encode(&mut out);
        encode_bytes(row, &mut out);
    }
    out
}

fn decode_record(payload: &[u8]) -> io::Result<Vec<Effect>> {
    let input = &mut &payload[..];
    let n = u32::decode(input)?;
    let mut effects = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let tag = u8::decode(input)?;
        effects.push(match tag {
            0 => Effect::Put {
                table: String::decode(input)?,
                row: Vec::<u8>::decode(input)?,
            },
            1 => Effect::Delete {
                table: String::decode(input)?,
                id: u64::decode(input)?,
            },
            2 => Effect::Enqueue {
                job: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
                key: Option::<String>::decode(input)?,
            },
            3 => Effect::Emit {
                event: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
                key: Option::<String>::decode(input)?,
            },
            4 => Effect::MigratePut {
                table: String::decode(input)?,
                id: u64::decode(input)?,
                row: Vec::<u8>::decode(input)?,
            },
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "record decode: unknown effect tag {other} — this log was written \
                         by a newer record format; upgrade the binary or restore the old one"
                    ),
                ));
            }
        });
    }
    if !input.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "record decode: {} trailing bytes after {n} effects — record corruption \
                 or format drift",
                input.len()
            ),
        ));
    }
    Ok(effects)
}

// ---------------------------------------------------------------- tx

/// The transaction handle: reads see this transaction's own buffered
/// writes first, then the enclosing group's accumulated writes (`group`,
/// below), then the store snapshot the group began from — a three-level
/// order, txn -> group -> snapshot (spec §2). `Db::transact` runs a group
/// of exactly one, so `group` is always empty there and this degenerates to
/// "snapshot plus this transaction's own writes". Writes, enqueues, and
/// emits buffer until commit; buffering cannot fail, so the write methods
/// return nothing.
pub struct Tx<'a> {
    snapshot: &'a Snapshot,
    /// The group's accumulated writes, `None` for a lone transaction. Reads
    /// fall through txn overlay -> group overlay -> snapshot (spec §2).
    group: Option<&'a GroupOverlay>,
    effects: TxEffects,
}

impl<'a> Tx<'a> {
    /// `pub(crate)`, deliberately, not `pub` (Noah ruling): a `pub`
    /// constructor would let an app holding a `&Db` mint a free-floating
    /// `Tx` from `Db::snapshot()`, write into it, drop it, and lose the
    /// writes silently — exactly the bug class `examples/invoicing/DEMO.md`
    /// exists to show app code cannot express. `transact` and
    /// `Group::stage` (below) are the only routes to a live `Tx`.
    pub(crate) fn over(snapshot: &'a Snapshot) -> Tx<'a> {
        Tx {
            snapshot,
            group: None,
            effects: TxEffects::default(),
        }
    }

    pub fn put<R: Row>(&mut self, row: &R) {
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        self.effects
            .overlay
            .entry(R::TABLE)
            .or_default()
            .insert(row.id().raw(), Some(bytes));
    }

    pub fn delete<R: Row>(&mut self, id: Id<R>) {
        self.effects
            .overlay
            .entry(R::TABLE)
            .or_default()
            .insert(id.raw(), None);
    }

    /// Buffers one decode-free migration write (spec §3). `row` must
    /// already be encoded under the table's TARGET version. Used only by
    /// `Db::migrate_schema`'s executor — deliberately not `pub`.
    pub(crate) fn migrate_put(&mut self, table: &'static str, id: u64, row: Vec<u8>) {
        self.effects.migrate_puts.push((table, id, row));
    }

    pub fn enqueue<J: Job>(&mut self, job: J) {
        let mut payload = Vec::new();
        job.encode(&mut payload);
        self.effects.enqueues.push((J::NAME, payload, None));
    }

    /// Enqueues with an idempotency key: the job's stable identity to the world
    /// OUTSIDE the log (consumed by M1's external-IO surface). Inside the
    /// deterministic boundary the key means nothing — exactly-once state effects
    /// are structural, so the platform neither indexes nor dedupes on it.
    pub fn enqueue_keyed<J: Job>(&mut self, key: impl Into<String>, job: J) {
        let key = key.into();
        assert!(
            !key.is_empty(),
            "contract: an idempotency key names the job's external identity — it cannot be empty"
        );
        let mut payload = Vec::new();
        job.encode(&mut payload);
        self.effects.enqueues.push((J::NAME, payload, Some(key)));
    }

    pub fn emit<Ev: Event>(&mut self, event: Ev) {
        let mut payload = Vec::new();
        event.encode(&mut payload);
        self.effects.emits.push((Ev::NAME, payload, None));
    }

    /// Emits with an opaque scoping key (e.g. an org id) — the exact-match
    /// filter `Subscription::key` matches against. Mirrors `enqueue_keyed`.
    pub fn emit_keyed<Ev: Event>(&mut self, key: impl Into<String>, event: Ev) {
        let mut payload = Vec::new();
        event.encode(&mut payload);
        self.effects
            .emits
            .push((Ev::NAME, payload, Some(key.into())));
    }
}

impl View for Tx<'_> {
    fn get<R: Row>(&self, id: Id<R>) -> io::Result<Option<R>> {
        match self
            .effects
            .overlay
            .get(R::TABLE)
            .and_then(|t| t.get(&id.raw()))
        {
            Some(Some(bytes)) => Ok(Some(R::decode_row(bytes)?)),
            Some(None) => Ok(None),
            None => match self
                .group
                .and_then(|g| g.get(R::TABLE))
                .and_then(|t| t.get(&id.raw()))
            {
                Some(Some(bytes)) => Ok(Some(R::decode_row(bytes)?)),
                Some(None) => Ok(None),
                None => self.snapshot.get(id),
            },
        }
    }

    fn scan<R: Row>(&self) -> io::Result<Vec<R>> {
        let mut merged: BTreeMap<u64, R> = BTreeMap::new();
        for row in self.snapshot.scan::<R>()? {
            merged.insert(row.id().raw(), row);
        }
        if let Some(overlay) = self.group.and_then(|g| g.get(R::TABLE)) {
            for (&id, entry) in overlay {
                match entry {
                    Some(bytes) => {
                        merged.insert(id, R::decode_row(bytes)?);
                    }
                    None => {
                        merged.remove(&id);
                    }
                }
            }
        }
        if let Some(overlay) = self.effects.overlay.get(R::TABLE) {
            for (&id, entry) in overlay {
                match entry {
                    Some(bytes) => {
                        merged.insert(id, R::decode_row(bytes)?);
                    }
                    None => {
                        merged.remove(&id);
                    }
                }
            }
        }
        Ok(merged.into_values().collect())
    }

    fn scan_by<R: Row>(&self, index_name: &str, key: &impl Codec) -> io::Result<Vec<R>> {
        let index = R::INDEXES
            .iter()
            .find(|i| i.name == index_name)
            .unwrap_or_else(|| panic!("no index named \"{index_name}\" on table {}", R::TABLE));
        let mut wanted = Vec::new();
        key.encode(&mut wanted);

        let mut merged: BTreeMap<u64, R> = BTreeMap::new();
        for row in self.snapshot.scan_by::<R>(index_name, key)? {
            merged.insert(row.id().raw(), row);
        }
        if let Some(overlay) = self.group.and_then(|g| g.get(R::TABLE)) {
            for (&id, entry) in overlay {
                // The overlay overrides the snapshot for this id either way:
                // a tombstone, or a row whose key may no longer match.
                merged.remove(&id);
                if let Some(bytes) = entry {
                    let row = R::decode_row(bytes)?;
                    let mut row_key = Vec::new();
                    (index.key)(&row, &mut row_key);
                    if row_key == wanted {
                        merged.insert(id, row);
                    }
                }
            }
        }
        if let Some(overlay) = self.effects.overlay.get(R::TABLE) {
            for (&id, entry) in overlay {
                // The overlay overrides the snapshot for this id either way:
                // a tombstone, or a row whose key may no longer match.
                merged.remove(&id);
                if let Some(bytes) = entry {
                    let row = R::decode_row(bytes)?;
                    let mut row_key = Vec::new();
                    (index.key)(&row, &mut row_key);
                    if row_key == wanted {
                        merged.insert(id, row);
                    }
                }
            }
        }
        Ok(merged.into_values().collect())
    }

    fn scan_from<R: Row>(&self, from_id: u64, limit: usize) -> io::Result<Vec<R>> {
        let mut merged: BTreeMap<u64, R> = BTreeMap::new();
        for row in self.snapshot.scan::<R>()? {
            if row.id().raw() >= from_id {
                merged.insert(row.id().raw(), row);
            }
        }
        if let Some(overlay) = self.group.and_then(|g| g.get(R::TABLE)) {
            for (&id, entry) in overlay {
                if id < from_id {
                    continue;
                }
                match entry {
                    Some(bytes) => {
                        merged.insert(id, R::decode_row(bytes)?);
                    }
                    None => {
                        merged.remove(&id);
                    }
                }
            }
        }
        if let Some(overlay) = self.effects.overlay.get(R::TABLE) {
            for (&id, entry) in overlay {
                if id < from_id {
                    continue;
                }
                match entry {
                    Some(bytes) => {
                        merged.insert(id, R::decode_row(bytes)?);
                    }
                    None => {
                        merged.remove(&id);
                    }
                }
            }
        }
        Ok(merged.into_values().take(limit).collect())
    }
}

// ---------------------------------------------------------------- machine

/// The state machine the engine drives: decodes effect records and applies
/// them to the store through the schema's vtables. Records at or below the
/// store's `applied_seq`-at-open are skipped during recovery replay.
struct TableMachine {
    store: Store,
    schema: Schema,
    skip_through: Option<u64>,
    /// Ids of jobs materialized since the last `Db::drain_enqueued` — the
    /// kernel's incremental feed for its due-heap.
    enqueued: Vec<u64>,
    /// Per-table persisted schema state as of replay progress: seeded
    /// from the checkpointed store at open, updated whenever a
    /// `charpente.schema_versions` put applies. Consulted before any row
    /// byte decodes (spec §2 — the upgrade barrier, enforced at the exact
    /// dangerous point). Keyed by table name; value is
    /// `(version, fingerprint)`.
    versions: BTreeMap<String, (u32, u64)>,
    /// Tables for which a below-compiled-version ordinary record was replayed
    /// decode-free during this recovery (spec §2 — a full rebuild-from-WAL of a
    /// store whose history crosses a version bump). Post-replay, a table in
    /// this set whose FINAL declared version reached compiled is admitted (its
    /// own history migrated forward in the WAL — a rebuilt store reopens after
    /// its migration); one still below compiled is a genuine unrecovered tail
    /// and the barrier fires (fail-closed preserved) unless it is a resumable
    /// in-flight migration to the compiled target.
    saw_below: BTreeSet<String>,
}

impl StateMachine for TableMachine {
    /// One record is a batch of one — and a batch of one does exactly the disk
    /// work this did before batching (one read txn, one write txn, one commit),
    /// which is the batch-of-1 identity the pre-batching trace fixture pins.
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()> {
        self.apply_batch(&[(seq, payload)]).map_err(|f| f.error)
    }

    /// Atomic where the trait default is per-record: the whole batch commits in
    /// one store transaction, or none of it does.
    fn apply_batch(&mut self, records: &[(u64, &[u8])]) -> Result<(), SubmitFailure> {
        // Skipped records are a contiguous prefix: `skip_through` is the
        // store's own `applied_seq` (`Db::recover`), so `seq <= skip_through`
        // is exactly what the store already holds, and the live tail starts at
        // `skip_through + 1` gapless.
        let skip_through = self.skip_through;
        let start = records
            .iter()
            .position(|(seq, _)| skip_through.is_none_or(|s| *seq > s))
            .unwrap_or(records.len());
        let live = &records[start..];
        if live.is_empty() {
            return Ok(());
        }

        let schema = &self.schema;
        // The dynamic version map is mutated in pass 1 (adoption/watermark
        // rows declare their table's version) and read in pass 2 (the gate);
        // a disjoint-field borrow alongside `schema`, both distinct from the
        // `self.store` the `apply_batch` call borrows.
        let versions = &mut self.versions;
        let saw_below = &mut self.saw_below;
        // `w.materialize_job` returns the new id; collected locally because
        // the closure can't also hold `&mut self.enqueued` while `schema`
        // (borrowed from `self`) is captured — and, load-bearing now that the
        // batch shares one transaction, because a rolled-back batch must not
        // hand the job runner ids for rows the store never took. Extended into
        // `self.enqueued` only after the batch commits.
        let mut new_ids = Vec::new();

        let result = self.store.apply_batch(live, |w, seq, payload| {
            // Decoding moved INSIDE the transaction when batching landed — it
            // used to run before the store was touched at all. That is
            // load-bearing, not incidental: a malformed payload now fails with
            // earlier records already written into this same transaction, so it
            // rolls all of them back instead of leaving a committed prefix.
            // Moving it back out would restore per-record commit semantics for
            // exactly the failure this batch is supposed to be atomic across.
            let effects = decode_record(payload)?;
            // Pass 1: schema-table effects first, so a record that both
            // adopts a table and writes its first rows has declared the
            // table's version by the time pass 2 checks it.
            for effect in &effects {
                if let Effect::Put { table, row } = effect
                    && table == crate::migration::SchemaVersion::TABLE
                {
                    // Close-out detection (spec §3): a version transition
                    // with the in-flight marker clearing rebuilds the
                    // migrated table's indexes — at apply, so primary,
                    // replay, and followers rebuild at the same log
                    // position.
                    let new = crate::migration::SchemaVersion::decode_row(row)?;
                    let old = w
                        .get_raw(table, new.id.raw())?
                        .map(|b| crate::migration::SchemaVersion::decode_row(&b))
                        .transpose()?;
                    (schema.ops(table)?.apply_put)(w, row)?;
                    let closed_out = old.as_ref().is_some_and(|o| {
                        o.migrating_to.is_some()
                            && new.migrating_to.is_none()
                            && new.version > o.version
                    });
                    if closed_out {
                        // LOAD-BEARING (T4 review): the rebuild presumes every
                        // migrated row is already durably applied. A MigratePut
                        // in this same record only lands in pass 2 (below), so a
                        // close-out sharing a record with one would rebuild over
                        // pre-migration bytes. `migrate_table` guarantees the
                        // close-out is its own transaction, strictly after every
                        // rewrite batch.
                        assert!(
                            !effects
                                .iter()
                                .any(|e| matches!(e, Effect::MigratePut { .. })),
                            "close-out version bump for \"{}\" shares a record with a \
                             MigratePut — the index rebuild would run before pass 2 applies \
                             the rewritten rows",
                            new.table
                        );
                        (schema.ops(&new.table)?.rebuild_indexes)(w)?;
                    }
                    versions.insert(new.table.clone(), (new.version, new.fingerprint));
                }
            }
            // Pass 2: everything else, version-checked before any row byte
            // decodes.
            for effect in &effects {
                match effect {
                    Effect::Put { table, row } => {
                        if table == crate::migration::SchemaVersion::TABLE {
                            continue; // pass 1 applied it
                        }
                        // A below-compiled-version ordinary record (a full
                        // rebuild-from-WAL replaying history that predates a
                        // version bump) is applied DECODE-FREE: its bytes are at
                        // the old version, and decoding them under the compiled
                        // type is the very misdecode the upgrade barrier exists
                        // to prevent (spec §2's organizing rule). The id is a
                        // version-independent prefix — every row encodes its id
                        // first, u64 LE — so put_raw needs no decode. A later
                        // MigratePut overwrites the row and the close-out
                        // rebuilds indexes; if the table never reaches compiled
                        // the post-replay barrier fires (saw_below).
                        if is_below_compiled(schema, versions, table) {
                            let id = row_id_prefix(row)?;
                            saw_below.insert(table.clone());
                            w.put_raw(table, id, row)?;
                        } else {
                            check_replay_version(schema, versions, table)?;
                            (schema.ops(table)?.apply_put)(w, row)?;
                        }
                    }
                    Effect::Delete { table, id } => {
                        if is_below_compiled(schema, versions, table) {
                            saw_below.insert(table.clone());
                            w.delete_raw(table, *id)?;
                        } else {
                            check_replay_version(schema, versions, table)?;
                            (schema.ops(table)?.apply_delete)(w, *id)?;
                        }
                    }
                    Effect::MigratePut { table, id, row } => {
                        // The rewrite itself: exempt from the version
                        // equality check (its whole point is bytes at the
                        // target version over rows at the old one), still
                        // registered-table-checked, still decode-free.
                        schema.ops(table)?;
                        w.put_raw(table, *id, row)?;
                    }
                    Effect::Enqueue { job, payload, key } => {
                        new_ids.push(w.materialize_job(job, payload, key.as_deref())?);
                    }
                    Effect::Emit {
                        event,
                        payload,
                        key,
                    } => {
                        w.materialize_event(seq, event, payload, key.as_deref())?;
                    }
                }
            }
            Ok(())
        });

        match result {
            Ok(()) => {
                self.enqueued.extend(new_ids);
                Ok(())
            }
            // A rolled-back batch applied nothing, so the count is 0 — with one
            // transaction per batch there is no longer a mid-batch boundary to
            // report. `versions` and `saw_below` are in-memory and were mutated
            // inside the closure, so they keep mutations the store discarded.
            // That divergence is tolerable ONLY because this path poisons the
            // engine: the process must restart and recover, and recovery
            // rebuilds both from the log. Do not try to roll them back.
            Err(error) => Err(SubmitFailure { applied: 0, error }),
        }
    }
}

// ---------------------------------------------------------------- gate

/// One table's gate verdict (spec §2), computed from the compiled schema
/// and the persisted `charpente.schema_versions` row. Pure — T9's
/// naive-gate meta-test runs a check-skipping variant side by side with
/// this real one against the same real store.
#[derive(Debug, Clone, PartialEq)]
pub enum GateStatus {
    /// Persisted matches compiled — nothing to do.
    Current,
    /// No persisted row: baseline (rows predate versioning) or a fresh
    /// table. Adoption happens at first write, not at open.
    Adopt,
    /// Persisted version is behind the compiled one; the executor runs
    /// (on a writable node) from `from`.
    Migrate { from: u32 },
    /// A named refusal — the message is the product surface (hard rule 5).
    Refuse(String),
}

pub fn gate_status(
    table: &str,
    compiled: (u32, u64),
    persisted: Option<&crate::migration::SchemaVersion>,
    rows_exist: bool,
) -> GateStatus {
    let (compiled_version, compiled_fingerprint) = compiled;
    let Some(row) = persisted else {
        if rows_exist && compiled_version > 1 {
            // Pre-versioning data (baseline 1) under a bumped binary:
            // migrate from 1. Post-versioning tables always have a row by
            // first-write adoption, so baseline-1 is unambiguous (spec §1).
            return GateStatus::Migrate { from: 1 };
        }
        return GateStatus::Adopt;
    };
    if let (Some(through), Some(to)) = (row.migrating_through, row.migrating_to)
        && to != compiled_version
    {
        return GateStatus::Refuse(format!(
            "table \"{table}\" has an interrupted migration to version {to} in flight \
             (resumable watermark at row id {through}) but this binary is compiled for \
             version {compiled_version} — rows on either side of the watermark are at \
             different versions and no single chain start covers both; finish the \
             interrupted migration with the binary that started it, or restore"
        ));
    }
    // In-flight-target FINGERPRINT refusal (spec §2/§5): a migration TO the
    // compiled version, but begun by a binary whose TARGET shape differs from
    // this one's. The persisted `fingerprint` field already holds the original
    // target's fingerprint (the executor writes it from the first watermark
    // on), so no extra field is needed. Resuming under a different shape would
    // leave rows below the watermark at the original target's bytes and rows
    // above at this binary's — a silently mixed-shape table, the very drift the
    // fingerprint mechanism exists to prevent. The version-equality drift arm
    // below never runs mid-resume (row.version stays at `from` until
    // close-out), so the check must live here.
    if let (Some(through), Some(to)) = (row.migrating_through, row.migrating_to)
        && to == compiled_version
        && !crate::migration::fingerprint_matches(Some(row.fingerprint), compiled_fingerprint)
    {
        return GateStatus::Refuse(format!(
            "table \"{table}\" has an interrupted migration to version {to} in flight \
             (resumable watermark at row id {through}) begun by a binary whose target shape \
             (fingerprint {:#x}) differs from this binary's (fingerprint {compiled_fingerprint:#x}) \
             — resuming would mix two shapes in one table; finish the interrupted migration \
             with the binary that started it, or restore",
            row.fingerprint
        ));
    }
    if row.version == compiled_version {
        if !crate::migration::fingerprint_matches(Some(row.fingerprint), compiled_fingerprint) {
            return GateStatus::Refuse(drift_msg(
                table,
                row.fingerprint,
                compiled_fingerprint,
                compiled_version,
            ));
        }
        return GateStatus::Current;
    }
    if row.version > compiled_version {
        return GateStatus::Refuse(downgrade_msg(table, row.version, compiled_version));
    }
    GateStatus::Migrate { from: row.version }
}

fn upgrade_barrier_msg(table: &str, persisted: u32, compiled: u32) -> String {
    format!(
        "table \"{table}\" has unrecovered writes from the previous binary (persisted version \
         {persisted}, this binary is compiled for version {compiled}) — run the previous \
         binary once to a clean shutdown, then upgrade (spec §2's upgrade barrier)"
    )
}

fn drift_msg(table: &str, persisted_fp: u64, compiled_fp: u64, version: u32) -> String {
    format!(
        "table \"{table}\"'s shape changed but its #[table(version = ...)] did not — \
         persisted fingerprint {persisted_fp:#x} != compiled fingerprint {compiled_fp:#x} at \
         version {version}; bump the version and add a TableMigration, or revert the struct \
         change"
    )
}

fn downgrade_msg(table: &str, persisted: u32, compiled: u32) -> String {
    format!(
        "table \"{table}\" is at persisted version {persisted} but this binary is compiled for \
         version {compiled} — the binary is older than the data; run a binary built for at \
         least version {persisted}"
    )
}

/// Refuses, by name, replaying an ordinary record for a table whose
/// declared version/fingerprint does not match the compiled one (spec
/// §2): the barrier (version behind), the downgrade (version ahead), and
/// drift (same version, different shape) — all BEFORE decoding row bytes.
/// A fingerprinted table with NO declared version cannot occur for
/// records written by an adoption-aware binary (first-write adoption);
/// seeing one is corruption-or-foreign-data and refuses by name.
/// True if `table` is a versioned table whose CURRENT declared version (in the
/// replay-time `versions` map) is strictly below the compiled version — i.e.
/// the record about to apply is at an older shape and must be handled
/// decode-free (spec §2).
fn is_below_compiled(
    schema: &Schema,
    versions: &BTreeMap<String, (u32, u64)>,
    table: &str,
) -> bool {
    let Some((compiled_version, Some(_))) = schema.table_meta(table) else {
        return false; // FINGERPRINT: None — not a versioned table.
    };
    versions
        .get(table)
        .is_some_and(|&(v, _)| v < compiled_version)
}

/// The row id is a version-independent prefix: every `#[table]`/`#[row_shape]`
/// struct encodes `self.id` first, as a u64 LE. So the primary key is
/// recoverable from raw bytes without decoding under any version.
fn row_id_prefix(row: &[u8]) -> io::Result<u64> {
    if row.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "row too short to carry an 8-byte id prefix",
        ));
    }
    Ok(u64::from_le_bytes(
        row[..8].try_into().expect("len checked"),
    ))
}

fn check_replay_version(
    schema: &Schema,
    versions: &BTreeMap<String, (u32, u64)>,
    table: &str,
) -> io::Result<()> {
    let Some((compiled_version, Some(compiled_fp))) = schema.table_meta(table) else {
        return Ok(()); // FINGERPRINT: None — not a versioned table.
    };
    let Some(&(persisted_version, persisted_fp)) = versions.get(table) else {
        return Err(io::Error::other(format!(
            "replaying a record for versioned table \"{table}\" with no adoption row on \
             record — this log was not written by an adoption-aware binary, or is corrupt; \
             restore from a replica or backup"
        )));
    };
    if persisted_version < compiled_version {
        return Err(io::Error::other(upgrade_barrier_msg(
            table,
            persisted_version,
            compiled_version,
        )));
    }
    if persisted_version > compiled_version {
        return Err(io::Error::other(downgrade_msg(
            table,
            persisted_version,
            compiled_version,
        )));
    }
    if persisted_fp != compiled_fp {
        return Err(io::Error::other(drift_msg(
            table,
            persisted_fp,
            compiled_fp,
            compiled_version,
        )));
    }
    Ok(())
}

// ---------------------------------------------------- horizon breadcrumb

/// Design A (`.superpowers/sdd/horizon-collapse-investigation.md`, Noah-ruled,
/// progress.md line 252): the recovery-horizon rule's ONLY witness used to be
/// the store's own `applied_seq` — a store discard-and-rebuild (`Db::open`,
/// below) erases it before `Engine::recover` ever runs, so a durable
/// corruption of an already-acked frame was silently cut instead of raising
/// the named hard error (`log.rs`'s recovery-horizon check). This sidecar
/// file, `<store_path>.horizon`, is a durable copy of the store's
/// `applied_seq` at its last checkpoint, written and barriered strictly
/// AFTER the store's own checkpoint persists — so it is always a lower bound
/// on the true durable horizon, never an over-claim. `Db::open` reads it and
/// feeds `max(store.applied_seq, breadcrumb)` into recovery instead of the
/// store's value alone.
///
/// Two fixed 12-byte slots (`u64` LE `applied_seq` + its `u32` LE CRC) at
/// offsets 0 and 12 in one file. `Disk` has no rename, so a write-new-then-
/// rename scheme isn't available; two slots are the smallest mechanism that
/// still satisfies "a torn UPDATE must not destroy the previous valid
/// witness" (each `write_at` call is its own independently-torn op in the
/// sim's crash model — module docs, `charpente-sim/src/disk.rs` — so a tear
/// of one slot's bytes can never touch the other slot's already-durable
/// bytes). Every write targets whichever slot holds the smaller (or
/// invalid/absent) value, leaving the slot holding the larger value
/// untouched; the read side takes the max of whichever slots verify. A
/// missing, torn, or CRC-invalid breadcrumb degrades to exactly `None` —
/// today's pre-breadcrumb semantics.
const HORIZON_SLOT_BYTES: u64 = 12; // u64 LE applied_seq + u32 LE crc32

fn horizon_path(store_path: &str) -> String {
    format!("{store_path}.horizon")
}

/// Reads one slot; `None` for absent, short, or CRC-invalid — never an
/// error, since a torn or missing breadcrumb must degrade silently to "no
/// witness", exactly like no breadcrumb file existing at all.
fn read_horizon_slot(file: &dyn File, len: u64, slot: u64) -> io::Result<Option<u64>> {
    let offset = slot * HORIZON_SLOT_BYTES;
    if len < offset + HORIZON_SLOT_BYTES {
        return Ok(None);
    }
    let mut buf = [0u8; HORIZON_SLOT_BYTES as usize];
    file.read_at(offset, &mut buf)?;
    let value = u64::from_le_bytes(buf[0..8].try_into().expect("8-byte slice"));
    let stored_crc = u32::from_le_bytes(buf[8..12].try_into().expect("4-byte slice"));
    Ok(if crate::crc::crc32(&buf[0..8]) == stored_crc {
        Some(value)
    } else {
        None
    })
}

/// The breadcrumb witness: the max of whichever of the two slots verify, or
/// `None` if the file is absent or neither slot verifies. Checks existence
/// via `list` first (never `Disk::open`, which creates-if-absent) so a
/// database that has never checkpointed pays no extra disk op reading a
/// breadcrumb that doesn't exist.
fn read_horizon_breadcrumb(disk: &dyn Disk, store_path: &str) -> io::Result<Option<u64>> {
    let path = horizon_path(store_path);
    if !disk.list(&path)?.iter().any(|name| name == &path) {
        return Ok(None);
    }
    let file = disk.open(&path)?;
    let len = file.len()?;
    let a = read_horizon_slot(&*file, len, 0)?;
    let b = read_horizon_slot(&*file, len, 1)?;
    Ok(match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(v), None) | (None, Some(v)) => Some(v),
        (None, None) => None,
    })
}

/// Writes `applied_seq` into whichever slot holds the smaller (or invalid)
/// existing value, then barriers the write (`File::sync` + `Disk::sync_dir`
/// — the file's directory entry must survive a crash right after this
/// returns, same fence-2 discipline as everywhere else). The OTHER slot —
/// holding the prior checkpoint's still-valid witness — is never touched, so
/// a crash mid-write leaves the breadcrumb readable at its PREVIOUS value,
/// never gone and never over-claiming.
fn write_horizon_breadcrumb(disk: &dyn Disk, store_path: &str, applied_seq: u64) -> io::Result<()> {
    let path = horizon_path(store_path);
    let file = disk.open(&path)?;
    let len = file.len()?;
    let a = read_horizon_slot(&*file, len, 0)?;
    let b = read_horizon_slot(&*file, len, 1)?;
    let slot = match (a, b) {
        (None, _) => 0,
        (_, None) => 1,
        (Some(a), Some(b)) if a <= b => 0,
        _ => 1,
    };
    let mut buf = [0u8; HORIZON_SLOT_BYTES as usize];
    buf[0..8].copy_from_slice(&applied_seq.to_le_bytes());
    let crc = crate::crc::crc32(&buf[0..8]);
    buf[8..12].copy_from_slice(&crc.to_le_bytes());
    file.write_at(slot * HORIZON_SLOT_BYTES, &buf)?;
    file.sync()?;
    disk.sync_dir(&path)?;
    Ok(())
}

/// Deletes the breadcrumb file, if any — `Seeder::new` already discards
/// `store_path` and every `wal/` segment because seeding always replaces the
/// timeline, never merges into one (its own module doc); the breadcrumb is
/// durable state ABOUT that discarded store and must go with it. Without
/// this, a stale breadcrumb from a higher-numbered prior timeline sharing
/// this disk (e.g. a demoted primary being re-seeded in place) would survive
/// into the new timeline and over-claim protection for seqs the new
/// timeline never actually acknowledged — the exact over-claim Design A's
/// lower-bound invariant forbids.
fn remove_horizon_breadcrumb(disk: &dyn Disk, store_path: &str) -> io::Result<()> {
    let path = horizon_path(store_path);
    match disk.remove(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    disk.sync_dir(&path)?;
    Ok(())
}

// ---------------------------------------------------------------- db

/// `Db`'s write gate (M2 sub-project 0b): three states, not a boolean or an
/// `Option`, because `Verifying` is neither "allowed" nor "forbidden and I
/// know where to send you" — it is "forbidden, and I don't yet know whether
/// I'm even in the wrong." Private to `Db`; `TxError::Verifying` is the
/// public surface a caller ever sees.
#[derive(Debug)]
enum WriteGate {
    Allowed,
    NotPrimary(String),
    Verifying(Vec<String>),
}

/// The composed deterministic core: WAL + store + schema. One `Db` is the
/// single writer; `transact` takes `&mut self`, so transactions are
/// serializable because two cannot exist at once.
// `Debug` is hand-written, not derived: `retention_pins` is a
// `Vec<Box<dyn Fn>>`, whose element has no `Debug` impl, and the derive
// would require one. `Db::open(..).unwrap_err()` needs SOME `Debug` bound on
// the `Ok` type, so `engine` and `cfg` are shown and `retention_pins` is
// simply omitted.
pub struct Db {
    engine: Engine<TableMachine>,
    cfg: DbConfig,
    retention_pins: Vec<Box<dyn Fn() -> u64 + Send>>,
    records_since_checkpoint: u64,
    /// `WriteGate::Allowed` (the default) permits commits; `NotPrimary`
    /// (`forbid_writes`) and `Verifying` (`verify_writes`) both refuse every
    /// effectful `transact` with their own `TxError`. Gates `transact` only
    /// — `apply_shipped` is the follower's own write path and bypasses it.
    gate: WriteGate,
    /// Tables that already carry a `charpente.schema_versions` row (T6's
    /// first-write adoption): rebuilt from the store at open and by
    /// `migrate_schema`. Omitted from `Debug` like `retention_pins`.
    adopted: BTreeSet<String>,
}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db")
            .field("engine", &self.engine)
            .field("cfg", &self.cfg)
            .field("records_since_checkpoint", &self.records_since_checkpoint)
            .field("gate", &self.gate)
            .finish()
    }
}

impl Db {
    /// Opens the store, then recovers the WAL, replaying every durable
    /// record past the store's checkpoint into it. Recovery can fail under
    /// IO faults; it is idempotent — restart and retry. A store that cannot
    /// open at all is discarded and rebuilt from the WAL — the store is a
    /// materialization of the log, never a second source of truth (DESIGN
    /// §4) — but only while the log still holds everything back to seq 0.
    /// Once `Db::checkpoint` has GC'd segments below its floor
    /// (`DbConfig::retain_segments`/`checkpoint_every`), the records the
    /// store would need no longer exist locally: rebuild is impossible and
    /// this is a named hard error instead of a silent (wrong) rebuild.
    ///
    /// The recovery-horizon rule (DESIGN §2, segment-mgmt T4): the store's
    /// `applied_seq` is the durable horizon threaded into `Engine::recover`,
    /// so a corrupt frame at-or-below it is a named hard error rather than a
    /// cut. The other half of the same rule lives here, before recovery
    /// runs at all — if the log's base_seq is already past the horizon
    /// (`applied_seq + 1`), an entire prefix of acknowledged records has
    /// vanished from both the store and the log; that is never a shape
    /// `checkpoint`'s own gc produces, so it is refused rather than silently
    /// replayed from wherever the log happens to start.
    pub fn open(disk: Box<dyn Disk>, cfg: DbConfig, schema: Schema) -> io::Result<Db> {
        // The jobs, events, and schema-versions tables are platform-owned; an
        // app registering any itself hits the existing duplicate-registration
        // assert.
        let schema = schema
            .table::<JobRow>()
            .table::<EventRow>()
            .table::<crate::migration::SchemaVersion>();
        let store = match Store::open(&*disk, &cfg.store_path) {
            Ok(store) => store,
            Err(e) => {
                // Peek the log's floor before touching the store: if it has
                // been compacted past seq 0, the records below the store's
                // last checkpoint are gone for good and rebuilding would
                // silently lose them — refuse instead (DESIGN §4).
                let prefix = format!("{}/", cfg.wal.dir);
                let names = disk.list(&prefix)?;
                if let Some(first) = names.first() {
                    let base = parse_segment_name(&cfg.wal.dir, first)?;
                    if base > 0 {
                        return Err(io::Error::other(format!(
                            "table store cannot open ({e}) and the log is compacted to seq \
                             {base} — the records below the store's last checkpoint no longer \
                             exist locally, so the store cannot be rebuilt from the log alone; \
                             restore this node from a replica or backup, or widen \
                             DbConfig::retain_segments (DESIGN §4)"
                        )));
                    }
                }
                // A store that cannot open is discarded and rebuilt from the
                // WAL — it is a materialization of the log, never a second
                // source of truth (DESIGN §4). E.g. a crash can tear the
                // store's creation, leaving durable bytes redb rejects on
                // every later open (found by seed 0x1f733f8d9eebf822).
                match disk.remove(&cfg.store_path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
                // Barrier the removal before rebuilding: an unsynced removal
                // that a later crash resurrects must not shadow the rebuilt
                // store with the bricked one it was discarded for.
                disk.sync_dir(&cfg.store_path)?;
                Store::open(&*disk, &cfg.store_path)?
            }
        };
        let skip_through = store.applied_seq()?;
        // The recovery-horizon WITNESS (horizon breadcrumb, above): a store
        // discard leaves `skip_through` at `None`, but a durable breadcrumb
        // from the store's last checkpoint can still supply the horizon —
        // `max` because the breadcrumb is only ever a lower bound (never
        // written ahead of the store's own checkpoint) and the store's own
        // value, when present, is always at least as current. Deliberately
        // NOT used for `TableMachine.skip_through` below: that field decides
        // whether a replayed record's writes are already reflected in the
        // store and must be skipped, which must stay tied to the store's
        // TRUE state — a discarded (fresh, empty) store has nothing to skip,
        // breadcrumb or not.
        let witness = match read_horizon_breadcrumb(&*disk, &cfg.store_path)? {
            Some(b) => Some(skip_through.map_or(b, |s| s.max(b))),
            None => skip_through,
        };
        // Recovery-horizon rule, vanished-prefix half (segment-mgmt T4; T2/T3
        // review adjudication). Checkpoint-driven gc can only ever leave the
        // log's floor at or below its own checkpoint's applied_seq + 1 (T2's
        // safety proof) — never above it. So base_seq > that boundary is not
        // a shape gc produces: it means the records between the horizon and
        // the base are gone from the log AND never reached the store —
        // corruption or entry loss, not something rebuild-from-WAL can
        // recover from. Peeked here, before Engine::recover runs, so the
        // hard error mutates nothing (same peek style as the unopenable-store
        // branch above; `None` reads as horizon `0`, so a fresh store paired
        // with any compacted log is rejected the same way).
        let horizon = witness.map_or(0, |applied| applied + 1);
        let prefix = format!("{}/", cfg.wal.dir);
        match disk.list(&prefix)?.first() {
            Some(first) => {
                let base = parse_segment_name(&cfg.wal.dir, first)?;
                if base > horizon {
                    return Err(io::Error::other(format!(
                        "log base_seq is {base} but the store's durable horizon is only \
                         {horizon} — acknowledged records between the horizon and the base \
                         exist in neither the store nor the log; this is corruption or entry \
                         loss, never a state Db::checkpoint's own gc can produce; restore this \
                         node from a replica or backup (DESIGN §2)"
                    )));
                }
            }
            // A checkpointed store beside an EMPTY log directory: the
            // mid-seed-finish crash shape (found by replication T6, seed
            // 0xcaef83b2feb5edf5). `Seeder::finish` checkpoints the store at
            // N and only then lays the seed segment and barriers its name —
            // a crash between the two leaves exactly this state. It proves
            // nothing was lost: every other route to a checkpointed store
            // leaves at least one name-barriered segment (a segment's
            // directory entry is barriered before any frame lands in it —
            // DESIGN §2's fence 2), so no record above the horizon can have
            // been acknowledged. Lay the segment `finish` would have — base
            // = applied_seq + 1 (this fn's `horizon` local; §2 calls
            // applied_seq itself the horizon, so in §2's words base =
            // horizon + 1), nothing to replay — instead of letting
            // `Wal::open` start a fresh log at 0 and trip the
            // outran-durable-truth assert below.
            None if skip_through.is_some() => {
                disk.open(&segment_name(&cfg.wal.dir, horizon))?;
                disk.sync_dir(&prefix)?;
            }
            None => {}
        }
        // Seed the replay-time version map (the two-pass gate) from the
        // CHECKPOINTED store; tail records update it live as their
        // adoption/watermark rows apply.
        let mut versions: BTreeMap<String, (u32, u64)> = BTreeMap::new();
        {
            let snap = store.snapshot()?;
            for table in schema.table_names() {
                if !matches!(schema.table_meta(table), Some((_, Some(_)))) {
                    continue; // FINGERPRINT: None — not a versioned table.
                }
                if let Some(row) = snap.get::<crate::migration::SchemaVersion>(
                    crate::migration::schema_version_id(table),
                )? {
                    versions.insert(row.table.clone(), (row.version, row.fingerprint));
                }
            }
        }
        let machine = TableMachine {
            store,
            schema,
            skip_through,
            enqueued: Vec::new(),
            versions,
            saw_below: BTreeSet::new(),
        };
        let mut engine = Engine::recover(disk, cfg.wal.clone(), machine, witness)?;
        // Recovery replay must not leak into the kernel's incremental feed —
        // the dispatcher rebuilds its due-heap from a Pending scan at open.
        engine.machine_mut().enqueued.clear();
        if let Some(applied) = skip_through {
            let durable = engine.last_applied();
            assert!(
                durable.is_some_and(|d| d >= applied),
                "store applied_seq {applied} is ahead of the durable log end {durable:?} — \
                 the store outran durable truth, which append→sync→apply forbids"
            );
        }
        let replayed = engine.last_applied() != skip_through;
        let mut db = Db {
            engine,
            cfg,
            retention_pins: Vec::new(),
            records_since_checkpoint: 0,
            gate: WriteGate::Allowed,
            adopted: BTreeSet::new(),
        };
        // Post-replay: the authoritative pure checks against the now-current
        // store (a drifted/downgraded table with a CLEAN tail still refuses;
        // a bump with a clean tail is fine here — the executor runs later, on
        // a writable node, via migrate_schema).
        db.check_schema_gate()?;
        db.reload_adopted()?;
        // Persist-store-at-open-after-replay (spec §2): makes "boot the
        // previous binary once" the complete barrier remedy. Conditional on
        // having replayed so no-op opens add zero disk ops (the pinned-seed
        // rule, same rationale as adoption-at-first-write). Deliberately a
        // store persist only, NOT a full `Db::checkpoint` — see
        // `persist_store_at_open`.
        if replayed {
            db.persist_store_at_open()?;
        }
        Ok(db)
    }

    /// Runs one transaction: `f` reads through `tx` and buffers effects; on
    /// `Ok` the effects commit as one durable log record and apply to the
    /// store; on `Err` nothing happened at all. A transaction with no
    /// effects writes nothing and consumes no seq.
    ///
    /// A group of one (spec §1): `transact` is exactly `begin_group` + one
    /// `stage` + `commit`, so the single-transaction path and a batched path
    /// run the identical instruction sequence — batch-of-one byte identity
    /// (spec §7) is a property of the code, not something tests must keep
    /// true by hand.
    pub fn transact<T, E>(
        &mut self,
        f: impl FnOnce(&mut Tx) -> Result<T, E>,
    ) -> Result<T, TxError<E>> {
        let mut g = self.begin_group().map_err(TxError::Io)?;
        let out = g.stage(f)?;
        // `applied` is consulted through the SAME rule the kernel's batched
        // tail uses (`CommitFailure::invalidates`), not discarded. A lone
        // transaction does have a partial-batch question after all — the
        // degenerate one — and answering it differently here is what broke
        // batch-of-one response identity (Noah ruling 2026-08-12). A record
        // that reached the store keeps its `Ok` even if the cadence
        // checkpoint then failed.
        //
        // But only swallow a failure something ELSE will report (T6 review
        // I-1, Noah ruling 2026-08-12). The original form ended "the engine
        // is poisoned either way and the kernel stops on its own check",
        // which is measurably false: `write_horizon_breadcrumb` is raw
        // `Disk` IO behind neither `Wal::guard` nor `DiskBackend`, so a sync
        // failure there latches nothing, poisons nothing, and — swallowed —
        // freezes WAL GC and the breadcrumb forever with nobody told. That
        // is the shape of the gap M2 sub-project 0a existed to close, and it
        // would have reached every `transact` caller: jobs reported `Done`,
        // cron, `migrate_schema`, app code.
        let staged = !g.is_empty();
        if let Err(failure) = g.commit() {
            let reported_elsewhere = self.poisoned() || self.store_failed();
            if failure.invalidates(0, staged) || !reported_elsewhere {
                return Err(TxError::Io(failure.error));
            }
        }
        Ok(out)
    }

    /// Opens a group over one store snapshot (spec §1–§2): `Group::stage`
    /// runs handlers against it, buffering each one's effects and merging
    /// its overlay into the group's; `Group::commit` appends every queued
    /// record, syncs ONCE for however many staged, then applies each in seq
    /// order — the append→sync→apply discipline of a lone transaction,
    /// N-wide, and apply stays strictly after sync either way.
    pub fn begin_group(&mut self) -> io::Result<Group<'_>> {
        let snapshot = self.engine.machine().store.snapshot()?;
        Ok(Group {
            snapshot,
            overlay: GroupOverlay::new(),
            records: Vec::new(),
            staged_adoptions: BTreeSet::new(),
            db: self,
        })
    }

    /// A consistent read-only view for queries outside transactions.
    pub fn snapshot(&self) -> io::Result<Snapshot> {
        self.engine.machine().store.snapshot()
    }

    /// This node's store write-transaction schedule digest
    /// (`Store::write_schedule`) — how its records were grouped into store
    /// commits, which is what redb's physical layout depends on. Two nodes at
    /// the same log position agree on rows and counters regardless; they agree
    /// on store BYTES only when this also matches (DESIGN §1, property 1).
    pub fn store_write_schedule(&self) -> u64 {
        self.engine.machine().store.write_schedule()
    }

    /// The event id counter (meta counter; `0` when unset) — same access
    /// shape as `Snapshot::next_job_id`.
    pub fn next_event_id(&self) -> io::Result<u64> {
        self.snapshot()?.next_event_id()
    }

    /// The seed frontier `(seq, frontier_crc)` this store was built at, if it
    /// was seeded (`Seeder::finish`) — `None` for a store opened from an
    /// ordinary log. The seq is pinned at seed time and never advances (the
    /// CRC is only valid for that one seq); callers compare it against
    /// `last_applied` before trusting the CRC. Used by `charpente::repl` to
    /// prove a freshly-seeded node's position (`hello_position`) and to chain
    /// the frontier through a seeded-then-promoted primary (`begin_seeding`).
    pub fn seed_frontier(&self) -> io::Result<Option<(u64, u32)>> {
        self.snapshot()?.seed_frontier()
    }

    /// Ids of jobs materialized by transactions since the last drain — the
    /// kernel's incremental feed for its due-heap. Recovery replay does not
    /// appear here; the dispatcher rebuilds from a Pending scan at open.
    pub fn drain_enqueued(&mut self) -> Vec<Id<JobRow>> {
        self.engine
            .machine_mut()
            .enqueued
            .drain(..)
            .map(Id::from_raw)
            .collect()
    }

    /// Durably persists the store, bounding recovery replay at this seq,
    /// then deletes WAL segments below the checkpoint (respecting the
    /// retention pin and `DbConfig::retain_segments`).
    pub fn checkpoint(&mut self) -> io::Result<()> {
        let store = &self.engine.machine().store;
        store.checkpoint()?;
        let applied = store.applied_seq()?;
        // Horizon breadcrumb (Design A): written strictly AFTER the store's
        // own checkpoint above persisted `applied`, so the breadcrumb is
        // always a lower bound on the true durable horizon. A fresh store
        // that has never applied anything (`applied = None`) has nothing to
        // record — the breadcrumb, like the store, simply stays absent.
        // Failure here has the SAME semantics as any other IO failure in
        // this function: it bubbles up via `?`, no new poisoning invented.
        if let Some(applied) = applied {
            write_horizon_breadcrumb(self.engine.wal().disk(), &self.cfg.store_path, applied)?;
        }
        let mut keep_from = applied.map_or(0, |s| s + 1);
        keep_from = keep_from.min(self.retention_floor());
        // Event retention GC runs ONLY here, at the cadence-driven
        // checkpoint — never inside `transact`/`Store::apply`, because GC is
        // not a log record. That is the determinism argument: a primary and
        // every follower replay the identical log and checkpoint on the
        // identical applied-records cadence (`DbConfig::checkpoint_every`),
        // so they compute the identical floor and delete the identical
        // EventRows at identical seq boundaries — fingerprint-at-equal-seq
        // equality holds (proven by `charpente/tests/replication.rs`'s
        // fingerprint oracle, which walks every registered table including
        // `charpente.events`). The has-work gate below is part of the same
        // argument (identical rows ⇒ identical gate on every node) — and it
        // keeps a checkpoint with nothing to delete from committing a write
        // transaction at all, so event-free databases checkpoint exactly as
        // they did before events existed.
        let snap = store.snapshot()?;
        let floor = snap
            .next_event_id()?
            .saturating_sub(self.cfg.event_retention);
        let has_work = floor > 0
            && snap
                .scan_from::<EventRow>(0, 1)?
                .first()
                .is_some_and(|row| row.id.raw() < floor);
        drop(snap);
        if has_work {
            store.gc_events(floor)?;
        }
        self.records_since_checkpoint = 0;
        self.engine
            .wal_mut()
            .gc(keep_from, self.cfg.retain_segments)
    }

    /// The spec §2 "the store checkpoints at open" step (persist-store-at-
    /// open-after-replay), deliberately NARROWER than `Db::checkpoint`: it
    /// advances the store's durable `applied_seq` and nothing else — no
    /// event-retention GC, no WAL segment GC, `records_since_checkpoint`
    /// untouched. Retention/segment GC must stay EXCLUSIVELY on the cadence-
    /// driven (`checkpoint_every`) schedule the realtime tail oracle is
    /// validated against; running GC at this extra, open-triggered site
    /// perturbs the deterministic GC schedule (a full checkpoint here deleted
    /// events out from under an in-flight subscriber — realtime seed
    /// 0xa2f2fa03568749d4 — and diverged primary/follower event floors at
    /// equal seq).
    fn persist_store_at_open(&self) -> io::Result<()> {
        self.engine.machine().store.checkpoint()
    }

    /// The smallest retained `EventRow` id — `next_event_id` when the table
    /// holds nothing (either no emit has ever happened, or `checkpoint`'s GC
    /// has deleted every row up to the tail). Never exceeds `next_event_id`:
    /// GC only ever deletes ids strictly below it. Subscribers compare their
    /// cursor against this to detect they have fallen off retention.
    pub fn event_floor(&self) -> io::Result<u64> {
        let snap = self.snapshot()?;
        let rows: Vec<EventRow> = snap.scan_from(0, 1)?;
        match rows.first() {
            Some(row) => {
                // Negative space: every stored event id was minted below the
                // counter, and GC never touches the counter — so the floor
                // can never reach, let alone exceed, next_event_id.
                let next = snap.next_event_id()?;
                assert!(
                    row.id.raw() < next,
                    "event_floor: smallest retained event id {} is not below next_event_id \
                     {next} — an event exists that the counter never minted",
                    row.id.raw()
                );
                Ok(row.id.raw())
            }
            None => snap.next_event_id(),
        }
    }

    /// Registers a floor `checkpoint`'s GC will never delete past — e.g. a
    /// replication cursor, or the archive's un-uploaded segment (archive
    /// spec §5). **Pins accumulate; the effective floor is the min over all
    /// of them.** A pin that is not holding returns `u64::MAX`.
    ///
    /// Renamed from `pin_retention`, which overrode: two subsystems now hold
    /// floors independently, and an override would have let whichever
    /// registered last silently delete the other's records.
    pub fn add_retention_pin(&mut self, pin: Box<dyn Fn() -> u64 + Send>) {
        self.retention_pins.push(pin);
    }

    /// The effective retention floor: the min over registered pins, or
    /// `u64::MAX` when none holds.
    pub fn retention_floor(&self) -> u64 {
        self.retention_pins
            .iter()
            .map(|p| p())
            .min()
            .unwrap_or(u64::MAX)
    }

    /// Marks this node a read-only follower: every further effectful
    /// `transact` returns `TxError::NotPrimary { primary }` naming where
    /// writes go instead (DESIGN §5). Reads and effect-free transactions are
    /// unaffected; `apply_shipped` is a separate path this never gates.
    pub fn forbid_writes(&mut self, primary: String) {
        self.gate = WriteGate::NotPrimary(primary);
    }

    /// Restores commits — the primary role, or a promoted follower's first
    /// act, or a witness-verifying node once every witness has answered.
    pub fn allow_writes(&mut self) {
        self.gate = WriteGate::Allowed;
    }

    /// Marks this node write-gated-pending-verification (M2 sub-project 0b):
    /// a durable primary that just restarted with a non-empty witness set,
    /// unsure whether one of its followers was promoted while it was down.
    /// Every further effectful `transact` returns `TxError::Verifying {
    /// unreached }` naming the witness set passed here — never a primary
    /// address, since this node may still be the legitimate one. Reads and
    /// effect-free transactions are unaffected, same as `forbid_writes`. The
    /// caller (Task 5's boot probe) resolves the gate by calling
    /// `allow_writes` once every witness answers "not a primary", or
    /// `forbid_writes` if one answers "primary"; it calls this exactly once,
    /// so the rendered list does not shrink as individual witnesses answer.
    pub fn verify_writes(&mut self, unreached: Vec<String>) {
        assert!(
            !unreached.is_empty(),
            "Db::verify_writes called with an empty witness list — call allow_writes instead; \
             an empty list has nothing left to verify"
        );
        assert!(
            !matches!(self.gate, WriteGate::NotPrimary(_)),
            "Db::verify_writes called while forbid_writes was already in effect — this would \
             replace an honest \"go to X\" 421 with an \"I might still be the primary\" 503; \
             call forbid_writes (or allow_writes) instead of layering verify_writes on top of \
             NotPrimary. Verifying -> Verifying stays legal, so re-gating with a smaller list \
             is supported — though the shipped replication caller gates exactly once."
        );
        self.gate = WriteGate::Verifying(unreached);
    }

    pub fn writes_allowed(&self) -> bool {
        matches!(self.gate, WriteGate::Allowed)
    }

    /// True while the witness boot gate is unresolved (M2 sub-project 0b):
    /// this node believes it is the primary but has not confirmed that none
    /// of the followers it shipped to was promoted while it was down.
    ///
    /// Distinct from `!writes_allowed()`, which is also true on an ordinary
    /// follower. Callers need the distinction when the question is "may I act
    /// on a timeline I have not confirmed I own" rather than "may I write" —
    /// `decide_hello` refuses to seed a newcomer in this state.
    pub fn verifying(&self) -> bool {
        matches!(self.gate, WriteGate::Verifying(_))
    }

    /// Applies frames shipped from the primary: the follower's only write
    /// path (DESIGN §5). Runs the checkpoint cadence like transact — a
    /// follower checkpoints and GCs its own log on the same schedule.
    pub fn apply_shipped(&mut self, frames: &[(u64, Vec<u8>)]) -> io::Result<()> {
        if frames.is_empty() {
            return Ok(());
        }
        self.engine.submit_batch(frames)?;
        self.records_since_checkpoint += frames.len() as u64;
        if self.cfg.checkpoint_every > 0
            && self.records_since_checkpoint >= self.cfg.checkpoint_every
        {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Verified read-back of committed records for the shipper; `up_to` is
    /// capped at `last_applied`, so the unsynced tail never travels.
    pub fn read_log(&self, from_seq: u64, max_bytes: usize) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let Some(last) = self.engine.last_applied() else {
            return Ok(Vec::new());
        };
        if from_seq > last {
            return Ok(Vec::new());
        }
        self.engine.wal().read_from(from_seq, last, max_bytes)
    }

    pub fn last_applied(&self) -> Option<u64> {
        self.engine.last_applied()
    }

    /// The lowest seq the local WAL still retains (`0` until `checkpoint`'s GC
    /// has advanced it). The primary-side session engine (`charpente::repl`)
    /// uses it to decide a `Hello`'s position: an empty (None-position)
    /// follower whose next needed seq is below this base is snapshot-seeded,
    /// while a POSITIONED follower below it is refused (diverged behind — the
    /// primary GC'd the frame at that seq and cannot prove a prefix; DESIGN §5,
    /// Design B).
    pub fn log_base(&self) -> u64 {
        self.engine.wal().base_seq()
    }

    /// The sealed WAL segments, oldest first — the archive shipper's second
    /// read-surface entry (DESIGN.md:268, M2 object-storage-archive T5,
    /// ruling 30). **The active tail is excluded**: it is still being
    /// appended to and must never be archived. A segment is sealed exactly
    /// when a later (higher base_seq) segment exists on disk — `Disk::list`
    /// is always sorted (DESIGN §1) and segment names are 20-digit
    /// zero-padded, so lexicographic order is numeric order (`Wal::gc`
    /// already depends on this), which makes the last name in the listing
    /// always the tail.
    pub fn sealed_segments(&self) -> io::Result<Vec<SealedSegment>> {
        let wal = self.engine.wal();
        let prefix = format!("{}/", self.cfg.wal.dir);
        let names = wal.disk().list(&prefix)?;
        let mut bases = Vec::with_capacity(names.len());
        for name in &names {
            bases.push(parse_segment_name(&self.cfg.wal.dir, name)?);
        }
        // `Disk::list` is contractually sorted (DESIGN §1), which is the
        // only reason the loop below can treat "next name in the listing" as
        // "next segment in the chain". The `checked_sub` panic just below
        // covers only the single unsorted shape `[…, 0]` (an unsorted
        // listing whose last name parses to base_seq 0); any other unsorted
        // shape (e.g. `[5, 3]`) would silently compute an inverted
        // `layout_end < base_seq` for that pair, caught only much later by
        // `Manifest::encode`'s own contiguity assert. Naming the contract
        // here closes that gap at its source.
        assert!(
            bases.windows(2).all(|w| w[0] < w[1]),
            "sealed_segments: Disk::list(\"{prefix}\") returned a segment listing that is not \
             strictly increasing ({bases:?}) — Disk::list is contractually sorted (DESIGN §1) \
             and segment bases are contractually strictly increasing from 0; this is a WAL \
             invariant violation, not a caller error"
        );
        let mut out = Vec::with_capacity(bases.len().saturating_sub(1));
        for i in 0..bases.len().saturating_sub(1) {
            let next_base = bases[i + 1];
            // `layout_end` is INCLUSIVE (`next_base - 1`), matching
            // `SegmentRef`'s inclusive-both-ends convention — `Wal::
            // read_from`'s internal `seg_end` is EXCLUSIVE (log.rs:334-337).
            // Checked arithmetic: two segments can never legitimately share
            // a base_seq (each holds at least one record), so `next_base`
            // is never 0 here; a 0 would mean the WAL's own segment-naming
            // invariant already broke, which is a bug worth a clear panic
            // rather than a silently wrapped `layout_end`. Since the
            // sortedness assert above, this panic is provably UNREACHABLE
            // (`bases[i + 1] == 0` would need `i + 1 == 0`) — it is kept as
            // defence in depth downstream of that assert, not as live
            // behaviour, so do not read it as the guard against unsorted
            // listings. That guard is the assert.
            let layout_end = next_base.checked_sub(1).unwrap_or_else(|| {
                panic!(
                    "sealed_segments: segment {} is immediately followed by a segment based at \
                     0 — segment bases must be strictly increasing from 0; this is a WAL \
                     invariant violation, not a caller error",
                    bases[i]
                )
            });
            out.push(SealedSegment {
                base_seq: bases[i],
                layout_end,
            });
        }
        Ok(out)
    }

    /// The registered table names, in schema-registry (sorted) order —
    /// including the platform-owned `JobRow`. The seed producer
    /// (`charpente::repl`) walks these to stream every table's rows to a
    /// fresh follower (DESIGN §5); a live `Db` never hands its `Schema` back,
    /// so this is the only reach to that walk order.
    pub fn table_names(&self) -> Vec<&'static str> {
        self.engine.machine().schema.table_names()
    }

    /// Every registered table in schema (sorted-name) order — the describe
    /// walk (spec §1). Like `table_names`, this is a read of registry state
    /// only; it never touches rows.
    pub fn describe_tables(&self) -> Vec<TableDescription> {
        let schema = &self.engine.machine().schema;
        schema
            .tables
            .iter()
            .map(|(&name, ops)| {
                let mut migrations: Vec<(u32, u32)> = schema
                    .migrations_for(name)
                    .iter()
                    .map(|s| (s.from_version, s.to_version))
                    .collect();
                // Contract determinism beats registration order: ascending
                // by from_version regardless of the order `.migration()`
                // calls registered them in.
                migrations.sort_by_key(|&(from, _)| from);
                TableDescription {
                    name,
                    version: ops.version,
                    fingerprint: ops.fingerprint,
                    index_names: ops.index_names.clone(),
                    migrations,
                }
            })
            .collect()
    }

    /// One aggregate over every versioned table's `(name, version,
    /// fingerprint)`, sorted by name (BTreeMap order). `FINGERPRINT: None`
    /// tables are excluded — platform tables are covered by PROTO_VERSION,
    /// and a hand-written app `Row` impl opts out of the digest exactly as
    /// it opts out of the gate (spec §4 scope note).
    pub fn schema_digest(&self) -> u64 {
        schema_digest_of(&self.engine.machine().schema)
    }

    /// True once the engine, its WAL, or the store handle is poisoned — every
    /// further transact fails. Callers (the kernel loop) stop and restart to
    /// recover.
    pub fn poisoned(&self) -> bool {
        self.engine.poisoned() || self.store_failed()
    }

    /// True once the store handle is dead (see `Store::failed`). Distinct from
    /// engine/WAL poison, and the distinction is load-bearing for the operator:
    /// engine poison means durability became UNKNOWABLE, while a store latch
    /// leaves the WAL intact and authoritative. Both remedies are a process
    /// restart; only one of them implies possible state loss.
    pub fn store_failed(&self) -> bool {
        self.engine.machine().store.failed()
    }

    /// The pure post-replay gate walk: returns the first named refusal.
    fn check_schema_gate(&self) -> io::Result<()> {
        let snap = self.snapshot()?;
        let schema = &self.engine.machine().schema;
        let saw_below = &self.engine.machine().saw_below;
        for table in schema.table_names() {
            let Some((version, Some(fingerprint))) = schema.table_meta(table) else {
                continue;
            };
            let persisted: Option<crate::migration::SchemaVersion> =
                snap.get(crate::migration::schema_version_id(table))?;
            let rows_exist = !snap.scan_raw_from(table, None, 1)?.is_empty();
            match gate_status(
                table,
                (version, fingerprint),
                persisted.as_ref(),
                rows_exist,
            ) {
                GateStatus::Refuse(msg) => return Err(io::Error::other(msg)),
                // The upgrade barrier fires POST-replay for a full rebuild,
                // discriminated by saw_below (spec §2). A table that needs
                // migration (Migrate) AND replayed below-version records during
                // this recovery is a genuine unrecovered foreign tail → barrier
                // (fail-closed, preserving migration_gate.rs's barrier test) —
                // UNLESS it is a resumable in-flight migration TO the compiled
                // target (its below-watermark rows were decode-free-replayed to
                // their new bytes; migrate_schema finishes the rest) → admit. A
                // table that needs migration but replayed NO below-version
                // records is the clean upgrade path (checkpointed store) →
                // migrate_schema handles it. Finding-1's completed case reached
                // compiled and never lands here (it is Current, not Migrate).
                GateStatus::Migrate { .. } if saw_below.contains(table) => {
                    let in_flight_to_compiled = persisted
                        .as_ref()
                        .and_then(|r| r.migrating_to)
                        .is_some_and(|to| to == version);
                    if !in_flight_to_compiled {
                        let persisted_version = persisted.as_ref().map_or(1, |r| r.version);
                        return Err(io::Error::other(upgrade_barrier_msg(
                            table,
                            persisted_version,
                            version,
                        )));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Rebuilds the adopted-set (T6's first-write adoption) from the store.
    /// Called at open and again by migrate_schema (a promoted follower's
    /// rows arrived by shipping after its open).
    fn reload_adopted(&mut self) -> io::Result<()> {
        let rows: Vec<crate::migration::SchemaVersion> = self.snapshot()?.scan()?;
        self.adopted = rows.into_iter().map(|r| r.table).collect();
        Ok(())
    }

    /// The writable-node half of the schema gate (spec §2/§3): verifies
    /// each behind table's chain, then runs the batched, resumable,
    /// decode-free rewrite. Call sites: the kernel's startup (single-node
    /// and primary — `Kernel::run`) and `do_promote` (a promoted follower
    /// resumes any in-flight watermark). Followers never call this — they
    /// receive migration through log shipping (spec §4).
    pub fn migrate_schema(&mut self) -> io::Result<()> {
        assert!(
            self.writes_allowed(),
            "migrate_schema on a write-forbidden node — this is the WRITE gate, not a role \
             check (M2 sub-project 0b task 5): a durable primary that restarted with recorded \
             witnesses is write-stopped until every witness answers, so a node can be \
             `Role::Primary` and still forbid this. Call it only where writes are already \
             allowed — the kernel's startup and `do_promote` both guard on \
             `writes_allowed()`; a direct caller must do the same, or wait for the gate to open"
        );
        self.reload_adopted()?;
        let tables: Vec<&'static str> = self.engine.machine().schema.table_names();
        for table in tables {
            let Some((compiled_version, Some(compiled_fingerprint))) =
                self.engine.machine().schema.table_meta(table)
            else {
                continue;
            };
            let vid = crate::migration::schema_version_id(table);
            let snap = self.snapshot()?;
            let persisted: Option<crate::migration::SchemaVersion> = snap.get(vid)?;
            let rows_exist = !snap.scan_raw_from(table, None, 1)?.is_empty();
            drop(snap);
            let from = match gate_status(
                table,
                (compiled_version, compiled_fingerprint),
                persisted.as_ref(),
                rows_exist,
            ) {
                GateStatus::Current | GateStatus::Adopt => continue,
                GateStatus::Refuse(msg) => return Err(io::Error::other(msg)),
                GateStatus::Migrate { from } => from,
            };
            // Chain existence, before touching anything.
            let steps: Vec<crate::migration::MigrationStep> =
                self.engine.machine().schema.migrations_for(table).to_vec();
            let mut cursor = from;
            while cursor != compiled_version {
                let Some(step) = steps.iter().find(|s| s.from_version == cursor) else {
                    return Err(io::Error::other(format!(
                        "table \"{table}\" has no registered migration from version {cursor} — \
                         persisted version is {from}, compiled version is {compiled_version}; \
                         register Schema::migration::<Step>(\"{table}\") for the \
                         version-{cursor} step, or this data cannot be opened by this binary"
                    )));
                };
                cursor = step.to_version;
            }
            self.migrate_table(table, from, compiled_version, compiled_fingerprint, &steps)?;
        }
        Ok(())
    }

    /// Spec §3's loop. Every batch commits its `MigratePut`s AND the
    /// watermark advance in ONE transaction — a crash between them is
    /// impossible; resumption is re-entry. The close-out transaction is the
    /// version bump whose apply rebuilds the table's indexes (T4).
    ///
    /// LOAD-BEARING ordering (T4 review): the close-out is its OWN
    /// transaction, strictly after every rewrite batch. It must never share
    /// a record with a `MigratePut`: the close-out's apply rebuilds the
    /// table's indexes in pass 1, presuming every migrated row is already
    /// durably applied — a `MigratePut` in the same record only lands in
    /// pass 2, so the rebuild would run over pre-migration bytes and corrupt
    /// the indexes. The empty-batch branch below preserves this (the
    /// close-out fires only once `scan_raw_from` past the watermark is
    /// empty), and `TableMachine::apply` debug-asserts it at the rebuild.
    fn migrate_table(
        &mut self,
        table: &'static str,
        from: u32,
        to: u32,
        fingerprint: u64,
        steps: &[crate::migration::MigrationStep],
    ) -> io::Result<()> {
        let vid = crate::migration::schema_version_id(table);
        let batch_size = self.cfg.migration_batch_size as usize;
        loop {
            let snap = self.snapshot()?;
            let watermark: Option<u64> = snap
                .get::<crate::migration::SchemaVersion>(vid)?
                .and_then(|v| v.migrating_through);
            let batch = snap.scan_raw_from(table, watermark, batch_size)?;
            drop(snap);
            if batch.is_empty() {
                self.transact(|tx| -> Result<(), io::Error> {
                    tx.put(&crate::migration::SchemaVersion {
                        id: vid,
                        table: table.to_string(),
                        version: to,
                        fingerprint,
                        migrating_through: None,
                        migrating_to: None,
                    });
                    Ok(())
                })
                .map_err(tx_err_to_io)?;
                return Ok(());
            }
            let new_watermark = batch.last().map(|(id, _)| *id);
            let mut migrated: Vec<(u64, Vec<u8>)> = Vec::with_capacity(batch.len());
            for (id, bytes) in batch {
                let mut version = from;
                let mut bytes = bytes;
                while version != to {
                    let step = steps
                        .iter()
                        .find(|s| s.from_version == version)
                        .expect("chain verified by migrate_schema");
                    bytes = (step.apply)(&bytes).map_err(|e| {
                        io::Error::other(format!("migrating table \"{table}\" row {id}: {e}"))
                    })?;
                    version = step.to_version;
                }
                migrated.push((id, bytes));
            }
            self.transact(|tx| -> Result<(), io::Error> {
                for (id, bytes) in migrated {
                    tx.migrate_put(table, id, bytes);
                }
                tx.put(&crate::migration::SchemaVersion {
                    id: vid,
                    table: table.to_string(),
                    version: from, // not done until the close-out above
                    fingerprint,
                    migrating_through: new_watermark,
                    migrating_to: Some(to),
                });
                Ok(())
            })
            .map_err(tx_err_to_io)?;
        }
    }

    /// Deconstructs a live `Db` into a `Seeder` over the same disk — the seed
    /// path's producer side (DESIGN §5): an app rebuilding a follower's state
    /// in-process from its own primary's snapshot. The recovered schema
    /// already carries `JobRow`/`EventRow`/`SchemaVersion` (`Db::open` adds
    /// all three on the way in); `Schema::remove` strips them back off so
    /// `Seeder::new` doesn't double-register any.
    pub fn into_seeder(self) -> io::Result<Seeder> {
        let schema = self.seed_schema();
        let (wal, machine) = self.engine.into_parts();
        let disk = wal.into_disk();
        drop(machine.store); // Seeder::new discards the store file itself; nothing to save
        Seeder::new(disk, self.cfg, schema)
    }

    /// This `Db`'s schema as a `Seeder` wants it: the caller's registrations,
    /// with the three the platform adds at `open` (`JobRow`, `EventRow`,
    /// `SchemaVersion`) stripped, because `Seeder::new` re-adds all three
    /// itself and `Schema::table` asserts on a duplicate.
    ///
    /// Public because a `Seeder` is not always built from the `Db` that owns
    /// the schema: `charpente restore` rebuilds a directory from an archive,
    /// and the only place the app's typed table ops exist is a `Db` the app
    /// itself built. Without this the CLI cannot name the app's tables at
    /// all, and `Seeder::rows` refuses every one of them by name.
    pub fn seed_schema(&self) -> Schema {
        let mut schema = self.engine.machine().schema.clone();
        schema.remove(JobRow::TABLE);
        schema.remove(EventRow::TABLE);
        schema.remove(crate::migration::SchemaVersion::TABLE);
        schema
    }
}

// -------------------------------------------------------------- group

/// The batch bound. A `const`, never a `DbConfig` field: spec §3 makes this
/// internal mechanism like `REPL_TICK_NANOS`, and a configurable throughput
/// knob next to `docs/BENCH.md` is the one thing not to ship. It exists
/// only to bound buffered memory — greedy drain is already self-limiting by
/// arrival rate.
///
/// SETTLED at Task 6 (the decision this doc used to defer): a kernel drain
/// loop bounds on **requests polled**, not on staged records. Records are
/// the smaller quantity — a handler returning `Err` or writing nothing
/// stages none — so a record-bounded loop is *unbounded* in exactly the
/// adversarial case the cap exists for, while the batch still holds one
/// `Request` (up to the 1 MiB body cap) and one `Reply` per drained item.
/// Bounding on requests caps both at once: `records ≤ requests ≤ MAX_BATCH`.
/// `pub` so `charpente`'s kernel shares this one constant rather than
/// duplicating the literal, which is how two copies of a limit drift.
pub const MAX_BATCH: usize = 64;

/// A batch of transactions sharing one commit (spec §1): one store
/// snapshot, one accumulated overlay, one durable sync for however many
/// `stage` calls land before `commit`. `Db::transact` is the group-of-one
/// case — `begin_group` + one `stage` + `commit` — so the single-request
/// path and a batched path execute the identical instruction sequence.
pub struct Group<'a> {
    snapshot: Snapshot,
    overlay: GroupOverlay,
    records: Vec<Vec<u8>>,
    staged_adoptions: BTreeSet<String>,
    db: &'a mut Db,
}

/// `Group` is reachable from any app crate — `charpente::core::db::Db::
/// begin_group` and `Group::stage` are both `pub`, with nothing narrower
/// than the crate's own `pub mod db` between them and app code (review
/// Important 2). Staging a write and dropping the group without `commit`
/// would silently lose it — the exact bug class the `Tx::over` rationale
/// above names, one level up. `commit` always empties `records` via
/// `mem::take` before anything fallible, so this stays silent on every real
/// commit — `Ok` or `Err` — and fires only when `commit` was never called.
/// `panicking()`-guarded: a handler panicking mid-`stage` (spec §4 row 2)
/// already leaves records staged and unwinds through this destructor —
/// without the guard, that ordinary case would double-panic and abort the
/// process instead of letting the kernel's caught unwind turn it into one
/// request's 500.
impl Drop for Group<'_> {
    fn drop(&mut self) {
        if !self.records.is_empty() && !std::thread::panicking() {
            panic!(
                "contract: Group dropped with {} staged record(s) never committed — every \
                 write staged into a group is lost unless Group::commit runs (even on an Err \
                 path, which still empties them) before the group goes out of scope; call \
                 Group::commit before dropping it",
                self.records.len()
            );
        }
    }
}

impl<'a> Group<'a> {
    /// Runs `f` against the group's snapshot plus every write staged into
    /// the group so far — read-your-own-writes across the whole batch, not
    /// just within one transaction (spec §2). On `Ok` with at least one
    /// effect, the transaction's overlay merges into the group's and its
    /// record queues for `commit`; on `Err`, or on no effects, nothing about
    /// the group changes at all — the transaction's own overlay (`tx`,
    /// local to this call) is simply dropped. That is what makes a
    /// panicking or rejected handler structurally unable to corrupt the
    /// batch: the group is never touched until `f` has already returned
    /// `Ok`.
    pub fn stage<T, E>(
        &mut self,
        f: impl FnOnce(&mut Tx) -> Result<T, E>,
    ) -> Result<T, TxError<E>> {
        // Structural memory bound (spec §3; hard rule 4 — stays on in
        // release): without this, MAX_BATCH/is_full() are advisory only and
        // spec §3's "bounded by MAX_BATCH times the 1 MiB body cap" claim is
        // not true of the code. Checked BEFORE running `f`, so a group at
        // the cap refuses a further stage before doing any work at all.
        assert!(
            self.records.len() < MAX_BATCH,
            "contract: a group holds at most {MAX_BATCH} staged records — call Group::commit \
             and begin a new group before staging another"
        );
        // Two immutable reborrows of `self` (`snapshot`, `overlay`), scoped
        // so they end here — before anything below needs `self` mutably.
        // `Tx::over` is the canonical constructor (its own doc names
        // `transact`/`Group::stage` as the only two callers); the group
        // level is patched on right after, same-module field access.
        let (out, mut effects) = {
            let mut tx = Tx::over(&self.snapshot);
            tx.group = Some(&self.overlay);
            let out = f(&mut tx).map_err(TxError::App)?;
            (out, tx.effects)
        };
        if effects.is_empty() {
            return Ok(out);
        }
        match &self.db.gate {
            WriteGate::Allowed => {}
            WriteGate::NotPrimary(primary) => {
                return Err(TxError::NotPrimary {
                    primary: primary.clone(),
                });
            }
            WriteGate::Verifying(unreached) => {
                return Err(TxError::Verifying {
                    unreached: unreached.clone(),
                });
            }
        }
        // First-write adoption, group-aware (spec §T4 note): two staged
        // transactions writing the same versioned table's first row must
        // not each emit a SchemaVersion row, so this consults
        // `self.db.adopted` (settled by a prior `commit`) UNION
        // `self.staged_adoptions` (settled by an earlier `stage` in THIS
        // group, not yet committed). `commit` folds `staged_adoptions` into
        // `db.adopted` once the whole batch is durable.
        let adoption_tables: Vec<&'static str> = effects
            .overlay
            .keys()
            .copied()
            .filter(|t| {
                *t != crate::migration::SchemaVersion::TABLE
                    && !self.db.adopted.contains(*t)
                    && !self.staged_adoptions.contains(*t)
                    && self
                        .db
                        .engine
                        .machine()
                        .schema
                        .table_meta(t)
                        .is_some_and(|(_, fp)| fp.is_some())
            })
            .collect();
        for table in &adoption_tables {
            let (version, fingerprint) = self
                .db
                .engine
                .machine()
                .schema
                .table_meta(table)
                .expect("filtered on table_meta above");
            let vid = crate::migration::schema_version_id(table);
            // Skip if this very transaction already writes the table's
            // schema row (the executor's watermark/close-out puts).
            let already = effects
                .overlay
                .get(crate::migration::SchemaVersion::TABLE)
                .is_some_and(|m| m.contains_key(&vid.raw()));
            if already {
                continue;
            }
            let row = crate::migration::SchemaVersion {
                id: vid,
                table: (*table).to_string(),
                version,
                fingerprint: fingerprint.expect("filtered on Some above"),
                migrating_through: None,
                migrating_to: None,
            };
            let mut bytes = Vec::new();
            row.encode_row(&mut bytes);
            effects
                .overlay
                .entry(crate::migration::SchemaVersion::TABLE)
                .or_default()
                .insert(vid.raw(), Some(bytes));
        }
        let record = encode_record(&effects);
        assert!(
            record.len() <= MAX_RECORD_BYTES,
            "transaction effects are {} bytes but one log record holds at most \
             {MAX_RECORD_BYTES} — split the work into smaller transactions",
            record.len()
        );
        for (table, entries) in effects.overlay {
            let group_table = self.overlay.entry(table).or_default();
            for (id, entry) in entries {
                group_table.insert(id, entry);
            }
        }
        self.records.push(record);
        for table in adoption_tables {
            self.staged_adoptions.insert(table.to_string());
        }
        Ok(out)
    }

    /// Records staged into this group and awaiting `commit`.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// True while `len() == 0` — nothing staged yet, or every staged
    /// transaction so far returned `Err`/had no effects.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Appends every staged record, syncs ONCE for the whole batch, then
    /// applies each in seq order (spec §1) — apply stays strictly after
    /// sync, so nothing here is acknowledged before it is durable. An empty
    /// group (every staged transaction rejected, or nothing staged at all)
    /// commits nothing and costs no disk op. Checkpoint cadence counts the
    /// WHOLE batch here, once, never per-record inside `stage` (spec §3).
    ///
    /// `records`/`staged_adoptions` are taken via `mem::take` rather than
    /// destructured, and the db is reached through `self.db` throughout:
    /// `Group`'s `Drop` guard (above) makes `let Group { db, records, .. } =
    /// self;` a compile error (E0509, cannot move out of a type
    /// implementing `Drop`). Taking `records` unconditionally, before
    /// anything fallible, has a bonus: by the time this returns — `Ok` or
    /// `Err` — `self.records` is already empty, so the Drop guard stays
    /// silent by construction rather than by a flag this function has to
    /// keep true.
    pub fn commit(mut self) -> Result<(), CommitFailure> {
        let records = std::mem::take(&mut self.records);
        let staged_adoptions = std::mem::take(&mut self.staged_adoptions);
        if records.is_empty() {
            return Ok(());
        }
        let n = records.len();
        // `submit_all` takes `&[&[u8]]`, not `&[Vec<u8>]` (engine.rs T2):
        // the single-commit hot path pays no extra heap copy, so the borrow
        // is reconstructed here rather than the signature loosened — this
        // collects N pointers, not N payloads.
        let refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
        self.db
            .engine
            .submit_all(&refs)
            .map_err(|f| CommitFailure {
                applied: f.applied,
                error: f.error,
            })?;
        for table in staged_adoptions {
            self.db.adopted.insert(table);
        }
        self.db.records_since_checkpoint += n as u64;
        if self.db.cfg.checkpoint_every > 0
            && self.db.records_since_checkpoint >= self.db.cfg.checkpoint_every
        {
            // An fsync failure here poisons like any other (fsyncgate). All
            // N records already reached the store at this point (the
            // `submit_all` call above already returned `Ok`), so `applied`
            // is the whole batch — this is a durability-bookkeeping failure
            // after the fact, not a partial-apply one.
            self.db
                .checkpoint()
                .map_err(|error| CommitFailure { applied: n, error })?;
        }
        Ok(())
    }
}

/// `Group::commit`'s failure (spec §4's only new contract, group-commit T4
/// fix round Important 1): `applied` is how many of the staged records
/// already reached the store — durably written and applied — before this
/// returned. Records `0..applied` already got their 200s and must not be
/// retried or re-reported; `applied` and the tail are durable but
/// unacknowledged (they will apply on restart, so their 500 is a false
/// negative, not a rejection — GUIDE names this contract, T11). `applied`
/// is `0` for an append or sync failure, since nothing in the batch reached
/// the store at all. A bare `TxError<Infallible>` could not carry this
/// boundary, which is why `commit` does not return one.
#[derive(Debug)]
pub struct CommitFailure {
    pub applied: usize,
    pub error: io::Error,
}

impl CommitFailure {
    /// Whether this failure leaves a batch participant unable to trust its
    /// own answer. `records_before` is how many records the group already
    /// held when that participant ran; `staged` is whether it then added one.
    ///
    /// The one rule, so the single-transaction path (`Db::transact`), the
    /// `App` one-request path and the kernel's batched tail cannot answer the
    /// same question differently. They did diverge (group-commit T6, Noah
    /// ruling 2026-08-12): the batched path honoured `applied` while the
    /// others discarded it, so a batch of one answered 200 where an unbatched
    /// request answered 500 — spec §7's identity holding on the disk trace
    /// and breaking on the response.
    ///
    /// **It keys on what the participant SAW, not on what it wrote** (T6
    /// review C-1, Noah ruling 2026-08-12). The earlier form asked only
    /// "which record did you stage", on the premise that a request staging
    /// nothing had nothing for a commit to make uncertain. That premise is
    /// false: `Group::stage` points every participant's `tx` at the group
    /// overlay, so a READ running after a sibling's write returns that
    /// sibling's uncommitted effects. If the commit then failed, the writer
    /// got its 500 and the reader was acked 200 carrying state no durable
    /// copy ever held. Reproduced, not theorised.
    ///
    /// So a participant is invalidated when any record it observed — or its
    /// own, if it staged one — failed to apply:
    /// - staged: its record index IS `records_before`, so `>= applied`;
    /// - read-only: it observed `0..records_before-1`, so `> applied`.
    ///
    /// Under an append/sync failure (`applied == 0`) that invalidates
    /// everything behind the first staged record, satisfying spec §4's table,
    /// while a request that ran AHEAD of every write keeps its honest answer:
    /// it only ever saw committed state. Under the checkpoint arm
    /// (`applied == n`) it invalidates nobody — those records are durable and
    /// the failure is bookkeeping after the fact. Telling such a request "may
    /// or may not have committed" is the half of M2 sub-project 0a's gap that
    /// its own fix left open.
    pub fn invalidates(&self, records_before: usize, staged: bool) -> bool {
        if staged {
            records_before >= self.applied
        } else {
            records_before > self.applied
        }
    }
}

// ---------------------------------------------------------------- seed

/// Builds a node's state from a primary's logical snapshot stream: rows in,
/// then `finish` pins the metadata, checkpoints, and lays an empty WAL
/// segment at `applied_seq + 1` — exactly the base = horizon + 1 boundary
/// `Db::open` accepts (DESIGN §5). Everything previously in this data dir
/// is discarded at construction: seeding replaces the timeline.
pub struct Seeder {
    disk: Box<dyn Disk>,
    cfg: DbConfig,
    schema: Schema,        // the caller's schema, passed to Db::open at finish
    import_schema: Schema, // schema + JobRow + EventRow + SchemaVersion, used to decode/import rows
    store: Store,
}

impl Seeder {
    /// `schema` must NOT include `JobRow`/`EventRow`/`SchemaVersion` —
    /// `Seeder` registers all three itself (mirroring `Db::open`), so a
    /// schema already carrying any (e.g. one recovered from a live `Db` via
    /// `into_seeder`) would double-register. Discards any store file,
    /// horizon breadcrumb, and `wal/` segments already at `disk`, dir-
    /// barriered — seeding always replaces a timeline, never merges into
    /// one. The breadcrumb must go too: left behind, a stale breadcrumb from
    /// a higher-numbered prior timeline sharing this disk (e.g. a demoted
    /// primary re-seeded in place) would survive into the freshly seeded
    /// timeline and over-claim protection for seqs it never acknowledged.
    pub fn new(disk: Box<dyn Disk>, cfg: DbConfig, schema: Schema) -> io::Result<Seeder> {
        match disk.remove(&cfg.store_path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        disk.sync_dir(&cfg.store_path)?;
        remove_horizon_breadcrumb(&*disk, &cfg.store_path)?;
        let wal_prefix = format!("{}/", cfg.wal.dir);
        for name in disk.list(&wal_prefix)? {
            disk.remove(&name)?;
        }
        disk.sync_dir(&wal_prefix)?;
        let store = Store::open(&*disk, &cfg.store_path)?;
        let import_schema = schema
            .clone()
            .table::<JobRow>()
            .table::<EventRow>()
            .table::<crate::migration::SchemaVersion>();
        Ok(Seeder {
            disk,
            cfg,
            schema,
            import_schema,
            store,
        })
    }

    /// One table's worth of exported rows, applied through the schema's
    /// typed ops — indexes rebuilt exactly as the live apply path maintains
    /// them. The id inside each row's own encoded bytes is what `put`
    /// re-derives the key from; the tuple's `u64` travels for
    /// debuggability and future delta use, not because `apply_put` needs it.
    pub fn rows(&mut self, table: &str, rows: &[(u64, Vec<u8>)]) -> io::Result<()> {
        self.store.import(|w| {
            for (id, bytes) in rows {
                let _ = id;
                (self.import_schema.ops(table)?.apply_put)(w, bytes)?;
            }
            Ok(())
        })
    }

    /// Pins the metadata, checkpoints, and lays the empty seed segment at
    /// `applied_seq + 1`, then hands the result through the normal
    /// `Db::open` — the same path every other startup goes through, so
    /// nothing about a seeded node's open is special. `frontier_crc` is the
    /// primary's CRC of the record at `applied_seq` (`Msg::SnapshotStart`):
    /// persisted so a follower that reboots before shipping any frame can
    /// still prove its frontier (`Db::seed_frontier`).
    pub fn finish(
        self,
        applied_seq: u64,
        next_job_id: u64,
        next_event_id: u64,
        frontier_crc: u32,
    ) -> io::Result<Db> {
        let Seeder {
            disk,
            cfg,
            schema,
            store,
            ..
        } = self;
        store.set_import_meta(applied_seq, next_job_id, next_event_id, frontier_crc)?;
        store.checkpoint()?;
        // Horizon breadcrumb (Design A), after the store checkpoint above
        // persists — same ordering rule as `Db::checkpoint`. `Seeder::new`
        // already cleared any stale breadcrumb, so this always lands in the
        // otherwise-empty slot 0; a crash before this line leaves the
        // breadcrumb absent (safe — the store's own `applied_seq`, once
        // reopened via `Db::open` below, supplies the same witness anyway).
        write_horizon_breadcrumb(&*disk, &cfg.store_path, applied_seq)?;
        drop(store); // graceful close before Db::open reopens the same file
        let name = segment_name(&cfg.wal.dir, applied_seq + 1);
        disk.open(&name)?;
        disk.sync_dir(&format!("{}/", cfg.wal.dir))?;
        Db::open(disk, cfg, schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn effects_fixture() -> TxEffects {
        let mut effects = TxEffects::default();
        let invoices = effects.overlay.entry("invoice").or_default();
        invoices.insert(10, Some(vec![1, 2, 3]));
        invoices.insert(11, None);
        effects
            .overlay
            .entry("org")
            .or_default()
            .insert(1, Some(vec![9]));
        effects.enqueues.push(("send_receipt", vec![10, 0], None));
        effects.emits.push(("invoice_finalized", vec![7], None));
        effects
    }

    #[test]
    fn record_roundtrip_is_canonical() {
        let encoded = encode_record(&effects_fixture());
        assert_eq!(
            decode_record(&encoded).unwrap(),
            vec![
                Effect::Put {
                    table: "invoice".into(),
                    row: vec![1, 2, 3]
                },
                Effect::Delete {
                    table: "invoice".into(),
                    id: 11
                },
                Effect::Put {
                    table: "org".into(),
                    row: vec![9]
                },
                Effect::Enqueue {
                    job: "send_receipt".into(),
                    payload: vec![10, 0],
                    key: None
                },
                Effect::Emit {
                    event: "invoice_finalized".into(),
                    payload: vec![7],
                    key: None
                },
            ]
        );
    }

    #[test]
    fn keyed_emit_roundtrips() {
        let mut effects = TxEffects::default();
        effects
            .emits
            .push(("invoice_finalized", vec![1], Some("org-7".to_string())));
        effects.emits.push(("ping", vec![2], None));
        let decoded = decode_record(&encode_record(&effects)).unwrap();
        assert_eq!(
            decoded,
            vec![
                Effect::Emit {
                    event: "invoice_finalized".into(),
                    payload: vec![1],
                    key: Some("org-7".into())
                },
                Effect::Emit {
                    event: "ping".into(),
                    payload: vec![2],
                    key: None
                },
            ]
        );
    }

    #[test]
    fn keyed_enqueue_roundtrips() {
        let mut effects = TxEffects::default();
        effects
            .enqueues
            .push(("send_receipt", vec![1], Some("receipt:10".to_string())));
        effects.enqueues.push(("ping", vec![2], None));
        let decoded = decode_record(&encode_record(&effects)).unwrap();
        assert_eq!(
            decoded,
            vec![
                Effect::Enqueue {
                    job: "send_receipt".into(),
                    payload: vec![1],
                    key: Some("receipt:10".into())
                },
                Effect::Enqueue {
                    job: "ping".into(),
                    payload: vec![2],
                    key: None
                },
            ]
        );
    }

    #[test]
    fn migrate_put_roundtrips_through_the_record_format() {
        let mut effects = TxEffects::default();
        effects.migrate_puts.push(("Widget", 7, vec![1, 2, 3]));
        let decoded = decode_record(&encode_record(&effects)).unwrap();
        assert_eq!(
            decoded,
            vec![Effect::MigratePut {
                table: "Widget".to_string(),
                id: 7,
                row: vec![1, 2, 3],
            }]
        );
    }

    #[test]
    fn decode_refuses_unknown_tags() {
        let mut out = Vec::new();
        1u32.encode(&mut out);
        out.push(9);
        let err = decode_record(&out).unwrap_err();
        assert!(err.to_string().contains("unknown effect tag 9"), "{err}");
    }

    #[test]
    fn decode_refuses_trailing_bytes() {
        let mut encoded = encode_record(&effects_fixture());
        encoded.push(0xAA);
        let err = decode_record(&encoded).unwrap_err();
        assert!(err.to_string().contains("trailing bytes"), "{err}");
    }

    #[test]
    fn empty_effects_know_they_are_empty() {
        assert!(TxEffects::default().is_empty());
        assert!(!effects_fixture().is_empty());
    }

    mod schema_migrations {
        use super::super::*;
        use crate::migration::{MigrationError, TableMigration};
        use crate::table::{Codec, IndexDef, RowShape};

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
            id: Id<Widget>,
            count: u32,
        }
        impl RowShape for Widget {
            const VERSION: u32 = 2;
            fn encode_row(&self, out: &mut Vec<u8>) {
                self.id.encode(out);
                self.count.encode(out);
            }
            fn decode_row(bytes: &[u8]) -> io::Result<Widget> {
                let input = &mut &bytes[..];
                Ok(Widget {
                    id: Id::decode(input)?,
                    count: u32::decode(input)?,
                })
            }
        }
        impl Row for Widget {
            const TABLE: &'static str = "Widget";
            const INDEXES: &'static [IndexDef<Widget>] = &[];
            const VERSION: u32 = 2;
            const FINGERPRINT: Option<u64> = Some(0xabc);
            fn id(&self) -> Id<Widget> {
                self.id
            }
            fn encode_row(&self, out: &mut Vec<u8>) {
                self.id.encode(out);
                self.count.encode(out);
            }
            fn decode_row(bytes: &[u8]) -> io::Result<Widget> {
                let input = &mut &bytes[..];
                Ok(Widget {
                    id: Id::decode(input)?,
                    count: u32::decode(input)?,
                })
            }
        }
        impl TableMigration for Widget {
            type Prev = WidgetV1;
            fn migrate(prev: WidgetV1) -> Result<Widget, MigrationError> {
                Ok(Widget {
                    id: Id::from_raw(0),
                    count: prev.count,
                })
            }
        }

        #[test]
        #[should_panic(expected = "never .table()'d")]
        fn migration_for_an_unregistered_table_panics_by_name() {
            let _ = Schema::new().migration::<Widget>("Widget");
        }

        #[test]
        fn registered_step_survives_schema_storage_and_applies() {
            let schema = Schema::new()
                .table::<Widget>()
                .migration::<Widget>("Widget");
            let steps = schema.migrations_for("Widget");
            assert_eq!((steps[0].from_version, steps[0].to_version), (1, 2));
            let mut old = Vec::new();
            WidgetV1 { count: 3 }.encode_row(&mut old);
            let new = (steps[0].apply)(&old).unwrap();
            assert_eq!(<Widget as Row>::decode_row(&new).unwrap().count, 3);
        }

        #[test]
        fn table_meta_reads_version_and_fingerprint() {
            let schema = Schema::new().table::<Widget>();
            assert_eq!(schema.table_meta("Widget"), Some((2, Some(0xabc))));
            assert_eq!(schema.table_meta("Nope"), None);
        }

        #[test]
        fn schema_digest_of_is_equal_for_equal_schemas() {
            let a = Schema::new().table::<Widget>();
            let b = Schema::new().table::<Widget>();
            assert_eq!(schema_digest_of(&a), schema_digest_of(&b));
        }

        #[test]
        fn schema_digest_of_differs_when_a_fingerprint_drifts() {
            struct WidgetV2Drifted {
                id: Id<WidgetV2Drifted>,
                count: u32,
            }
            impl RowShape for WidgetV2Drifted {
                const VERSION: u32 = 2;
                fn encode_row(&self, out: &mut Vec<u8>) {
                    self.id.encode(out);
                    self.count.encode(out);
                }
                fn decode_row(bytes: &[u8]) -> io::Result<WidgetV2Drifted> {
                    let input = &mut &bytes[..];
                    Ok(WidgetV2Drifted {
                        id: Id::decode(input)?,
                        count: u32::decode(input)?,
                    })
                }
            }
            impl Row for WidgetV2Drifted {
                const TABLE: &'static str = "Widget";
                const INDEXES: &'static [IndexDef<WidgetV2Drifted>] = &[];
                const VERSION: u32 = 2;
                const FINGERPRINT: Option<u64> = Some(0xdef);
                fn id(&self) -> Id<WidgetV2Drifted> {
                    self.id
                }
                fn encode_row(&self, out: &mut Vec<u8>) {
                    self.id.encode(out);
                    self.count.encode(out);
                }
                fn decode_row(bytes: &[u8]) -> io::Result<WidgetV2Drifted> {
                    let input = &mut &bytes[..];
                    Ok(WidgetV2Drifted {
                        id: Id::decode(input)?,
                        count: u32::decode(input)?,
                    })
                }
            }
            let compiled = Schema::new().table::<Widget>();
            let drifted = Schema::new().table::<WidgetV2Drifted>();
            assert_ne!(schema_digest_of(&compiled), schema_digest_of(&drifted));
        }
    }
}
