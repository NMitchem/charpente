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

use crate::engine::{Engine, StateMachine};
use crate::event::EventRow;
use crate::io::Disk;
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

/// A transaction's buffered effects: the collapsed per-table overlay
/// (`None` = tombstone) plus enqueues and emits in call order.
#[derive(Default)]
struct TxEffects {
    overlay: BTreeMap<&'static str, BTreeMap<u64, Option<Vec<u8>>>>,
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

/// The transaction handle: reads see the snapshot plus this transaction's
/// own writes; writes, enqueues, and emits buffer until commit. Buffering
/// cannot fail, so the write methods return nothing.
pub struct Tx {
    snapshot: Snapshot,
    effects: TxEffects,
}

impl Tx {
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
    // No caller until T6 wires the executor; the wire path (encode/decode/
    // apply) and its close-out trigger are exercised now (record roundtrip +
    // tests/migrate_apply.rs). The allow comes off when T6 calls this.
    #[allow(dead_code)]
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

impl View for Tx {
    fn get<R: Row>(&self, id: Id<R>) -> io::Result<Option<R>> {
        match self
            .effects
            .overlay
            .get(R::TABLE)
            .and_then(|t| t.get(&id.raw()))
        {
            Some(Some(bytes)) => Ok(Some(R::decode_row(bytes)?)),
            Some(None) => Ok(None),
            None => self.snapshot.get(id),
        }
    }

    fn scan<R: Row>(&self) -> io::Result<Vec<R>> {
        let mut merged: BTreeMap<u64, R> = BTreeMap::new();
        for row in self.snapshot.scan::<R>()? {
            merged.insert(row.id().raw(), row);
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
}

impl StateMachine for TableMachine {
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()> {
        if self.skip_through.is_some_and(|s| seq <= s) {
            return Ok(());
        }
        let effects = decode_record(payload)?;
        let schema = &self.schema;
        // The dynamic version map is mutated in pass 1 (adoption/watermark
        // rows declare their table's version) and read in pass 2 (the gate);
        // a disjoint-field borrow alongside `schema`, both distinct from the
        // `self.store` the `apply` call borrows.
        let versions = &mut self.versions;
        // `w.materialize_job` returns the new id; collected locally because
        // the closure can't also hold `&mut self.enqueued` while `schema`
        // (borrowed from `self`) is captured — extend after the store commits.
        let new_ids = self.store.apply(seq, |w| {
            let mut ids = Vec::new();
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
                        debug_assert!(
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
                        check_replay_version(schema, versions, table)?;
                        (schema.ops(table)?.apply_put)(w, row)?;
                    }
                    Effect::Delete { table, id } => {
                        check_replay_version(schema, versions, table)?;
                        (schema.ops(table)?.apply_delete)(w, *id)?;
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
                        ids.push(w.materialize_job(job, payload, key.as_deref())?);
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
            Ok(ids)
        })?;
        self.enqueued.extend(new_ids);
        Ok(())
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

// ---------------------------------------------------------------- db

/// The composed deterministic core: WAL + store + schema. One `Db` is the
/// single writer; `transact` takes `&mut self`, so transactions are
/// serializable because two cannot exist at once.
// `Debug` is hand-written, not derived: `retention_pin` is a `Box<dyn Fn>`,
// which has no `Debug` impl, and the derive would require one. `Db::open(..)
// .unwrap_err()` needs SOME `Debug` bound on the `Ok` type, so `engine` and
// `cfg` are shown and `retention_pin` is simply omitted.
pub struct Db {
    engine: Engine<TableMachine>,
    cfg: DbConfig,
    retention_pin: Option<Box<dyn Fn() -> u64 + Send>>,
    records_since_checkpoint: u64,
    /// `Some(primary)` once `forbid_writes` has marked this node a read-only
    /// follower; `None` (the default) allows commits. Gates `transact` only
    /// — `apply_shipped` is the follower's own write path and bypasses it.
    not_primary: Option<String>,
    /// Tables that already carry a `charpente.schema_versions` row (T6's
    /// first-write adoption): rebuilt from the store at open and by
    /// `migrate_schema`. Omitted from `Debug` like `retention_pin`.
    adopted: BTreeSet<String>,
}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db")
            .field("engine", &self.engine)
            .field("cfg", &self.cfg)
            .field("records_since_checkpoint", &self.records_since_checkpoint)
            .field("not_primary", &self.not_primary)
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
        let horizon = skip_through.map_or(0, |applied| applied + 1);
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
        };
        let mut engine = Engine::recover(disk, cfg.wal.clone(), machine, skip_through)?;
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
            retention_pin: None,
            records_since_checkpoint: 0,
            not_primary: None,
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
    pub fn transact<T, E>(
        &mut self,
        f: impl FnOnce(&mut Tx) -> Result<T, E>,
    ) -> Result<T, TxError<E>> {
        let snapshot = self
            .engine
            .machine()
            .store
            .snapshot()
            .map_err(TxError::Io)?;
        let mut tx = Tx {
            snapshot,
            effects: TxEffects::default(),
        };
        let out = f(&mut tx).map_err(TxError::App)?;
        if tx.effects.is_empty() {
            return Ok(out);
        }
        if let Some(primary) = &self.not_primary {
            return Err(TxError::NotPrimary {
                primary: primary.clone(),
            });
        }
        // First-write adoption (spec §1): the first write to a versioned
        // table carries that table's `charpente.schema_versions` row, so the
        // record declares its own version before the replay gate ever checks
        // it. Only versioned (Some-fingerprint) tables, only the first time
        // (`self.adopted`), and never a table this very transaction already
        // writes a schema row for (the executor's watermark/close-out puts).
        let adoption_tables: Vec<&'static str> = tx
            .effects
            .overlay
            .keys()
            .copied()
            .filter(|t| {
                *t != crate::migration::SchemaVersion::TABLE
                    && !self.adopted.contains(*t)
                    && self
                        .engine
                        .machine()
                        .schema
                        .table_meta(t)
                        .is_some_and(|(_, fp)| fp.is_some())
            })
            .collect();
        for table in &adoption_tables {
            let (version, fingerprint) = self
                .engine
                .machine()
                .schema
                .table_meta(table)
                .expect("filtered on table_meta above");
            let vid = crate::migration::schema_version_id(table);
            // Skip if this very transaction already writes the table's
            // schema row (the executor's watermark/close-out puts).
            let already = tx
                .effects
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
            tx.effects
                .overlay
                .entry(crate::migration::SchemaVersion::TABLE)
                .or_default()
                .insert(vid.raw(), Some(bytes));
        }
        let record = encode_record(&tx.effects);
        assert!(
            record.len() <= MAX_RECORD_BYTES,
            "transaction effects are {} bytes but one log record holds at most \
             {MAX_RECORD_BYTES} — split the work into smaller transactions",
            record.len()
        );
        self.engine.submit(&record).map_err(TxError::Io)?;
        for table in adoption_tables {
            self.adopted.insert(table.to_string());
        }
        self.records_since_checkpoint += 1;
        if self.cfg.checkpoint_every > 0
            && self.records_since_checkpoint >= self.cfg.checkpoint_every
        {
            // An fsync failure here poisons like any other (fsyncgate) — it
            // surfaces through the same TxError::Io as a commit failure.
            self.checkpoint().map_err(TxError::Io)?;
        }
        Ok(out)
    }

    /// A consistent read-only view for queries outside transactions.
    pub fn snapshot(&self) -> io::Result<Snapshot> {
        self.engine.machine().store.snapshot()
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
        let mut keep_from = applied.map_or(0, |s| s + 1);
        if let Some(pin) = &self.retention_pin {
            keep_from = keep_from.min(pin());
        }
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
    /// replication cursor that still needs older records than the store's
    /// own checkpoint does. Overrides any previously registered pin.
    pub fn pin_retention(&mut self, pin: Box<dyn Fn() -> u64 + Send>) {
        self.retention_pin = Some(pin);
    }

    /// Marks this node a read-only follower: every further effectful
    /// `transact` returns `TxError::NotPrimary { primary }` naming where
    /// writes go instead (DESIGN §5). Reads and effect-free transactions are
    /// unaffected; `apply_shipped` is a separate path this never gates.
    pub fn forbid_writes(&mut self, primary: String) {
        self.not_primary = Some(primary);
    }

    /// Restores commits — the primary role, or a promoted follower's first act.
    pub fn allow_writes(&mut self) {
        self.not_primary = None;
    }

    pub fn writes_allowed(&self) -> bool {
        self.not_primary.is_none()
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

    /// The registered table names, in schema-registry (sorted) order —
    /// including the platform-owned `JobRow`. The seed producer
    /// (`charpente::repl`) walks these to stream every table's rows to a
    /// fresh follower (DESIGN §5); a live `Db` never hands its `Schema` back,
    /// so this is the only reach to that walk order.
    pub fn table_names(&self) -> Vec<&'static str> {
        self.engine.machine().schema.table_names()
    }

    /// True once the engine or its WAL is poisoned — every further transact
    /// fails. Callers (the kernel loop) stop and restart to recover.
    pub fn poisoned(&self) -> bool {
        self.engine.poisoned()
    }

    /// The pure post-replay gate walk: returns the first named refusal.
    fn check_schema_gate(&self) -> io::Result<()> {
        let snap = self.snapshot()?;
        let schema = &self.engine.machine().schema;
        for table in schema.table_names() {
            let Some((version, Some(fingerprint))) = schema.table_meta(table) else {
                continue;
            };
            let persisted: Option<crate::migration::SchemaVersion> =
                snap.get(crate::migration::schema_version_id(table))?;
            let rows_exist = !snap.scan_raw_from(table, None, 1)?.is_empty();
            if let GateStatus::Refuse(msg) = gate_status(
                table,
                (version, fingerprint),
                persisted.as_ref(),
                rows_exist,
            ) {
                return Err(io::Error::other(msg));
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

    /// Deconstructs a live `Db` into a `Seeder` over the same disk — the seed
    /// path's producer side (DESIGN §5): an app rebuilding a follower's state
    /// in-process from its own primary's snapshot. The recovered schema
    /// already carries `JobRow`/`EventRow`/`SchemaVersion` (`Db::open` adds
    /// all three on the way in); `Schema::remove` strips them back off so
    /// `Seeder::new` doesn't double-register any.
    pub fn into_seeder(self) -> io::Result<Seeder> {
        let (wal, machine) = self.engine.into_parts();
        let disk = wal.into_disk();
        let TableMachine {
            store, mut schema, ..
        } = machine;
        drop(store); // Seeder::new discards the store file itself; nothing to save
        schema.remove(JobRow::TABLE);
        schema.remove(EventRow::TABLE);
        schema.remove(crate::migration::SchemaVersion::TABLE);
        Seeder::new(disk, self.cfg, schema)
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
    /// `into_seeder`) would double-register. Discards any store file and
    /// `wal/` segments already
    /// at `disk`, dir-barriered — seeding always replaces a timeline, never
    /// merges into one.
    pub fn new(disk: Box<dyn Disk>, cfg: DbConfig, schema: Schema) -> io::Result<Seeder> {
        match disk.remove(&cfg.store_path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        disk.sync_dir(&cfg.store_path)?;
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
    }
}
