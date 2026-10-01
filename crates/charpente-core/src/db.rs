//! Typed transactions over the log (DESIGN.md §2, PLAN.md M0 step 4): a
//! transaction is `fn(&mut Tx) -> Result<T, E>` — reads come from a snapshot
//! plus the transaction's own writes; effects buffer and serialize as ONE
//! log record; the store applies them only after the record is durable.
//! Serializable by construction: `transact` takes `&mut Db`, so two
//! transactions cannot exist at once.
//!
//! Record payload v0: `[n: u32 le]` then n tagged effects —
//! `0x00 [table][row]` put, `0x01 [table][id: u64 le]` delete,
//! `0x04 [table][id: u64 le][row]` migrate-put,
//! `0x05 [table][scope: u64 le][row]` scoped put,
//! `0x06 [table][scope: u64 le][id: u64 le]` scoped delete,
//! `0x07 [table][scope: u64 le][id: u64 le][row]` scoped migrate-put,
//! `0x08 [job][payload][key: Option<String>][shape: u64 le]` shaped enqueue,
//! `0x09 [event][payload][key: Option<String>][shape: u64 le]` shaped emit,
//! `0x0a [job][payload][key: Option<String>][shape: u64 le][scope: u64 le]`
//! scoped enqueue, for a `#[job(scoped_by = …)]` job (job-tenancy spec §6) —
//! strings and byte fields length-prefixed per the `Codec` layout; `0x0b` is
//! the next free tag. `0x02`/`0x03` (the same without the shape) still
//! decode, with the shape unknown, and are never written — `TxEffects` holds
//! a bare shape, so a shapeless enqueue or emit cannot be represented.
//! `0x05`/`0x06`/`0x07` and `0x0a` are the ONLY tags that carry a
//! scope. A table effect's is chosen by the table's own `Row::Scope`, not by
//! the scope value (tenant 0 is a legal tenant, so a scoped table's records
//! always use `0x05`/`0x06`/`0x07`, never `0x00`/`0x01`/`0x04`, even at
//! scope 0). A scoped enqueue is `0x0a` rather than a widened `0x08` for the
//! same reason, chosen by the job's own `Job::Scope` through the enqueue
//! family, and the same presence-not-value rule applies: an enqueue carrying
//! `Some(0)` is `0x0a`.
//! A binary that does not know a tag refuses it by name (`decode_record`'s
//! unknown-tag error) rather than misreading a wider record. `0x07` is a
//! NEW tag rather than a widened `0x04`, the same choice `0x05`/`0x06` made
//! over `0x00`/`0x01` (tenant-scoping Task 6b): a follower on an older
//! binary must refuse an unrecognized tag by name, not misread a longer
//! record as a shorter one.
//! Table effects are the transaction's collapsed overlay in (table, scope,
//! id) order: intermediate overwrites inside a transaction are invisible to
//! every observer and are not logged. Enqueues and emits keep call order.
//! The enqueue key is the job's idempotency identity to the world outside
//! the log — carried, never interpreted inside the deterministic boundary.

use std::any::TypeId;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;

use crate::engine::{Engine, StateMachine, SubmitFailure};
use crate::event::EventRow;
use crate::io::{Disk, File};
use crate::job::JobRow;
use crate::log::{MAX_RECORD_BYTES, WalConfig, parse_segment_name, segment_name};
use crate::table::{
    Codec, Id, IndexDef, Row, RowShape, Snapshot, Store, UnscopedJob, UnscopedRow, View,
    WritableRow, Writer,
};

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
    /// migration commit's WAL record size. At least 1: `migrate_schema`
    /// refuses 0 by name when a table is due to migrate.
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
    /// The payload's structural fingerprint — `#[job]` computes it from the
    /// fields (the helper `#[table]` uses). Recorded beside every enqueue. A
    /// Pending job is decoded only by a binary whose `FINGERPRINT` matches:
    /// a Pending job of a foreign shape is refused before dispatch can start
    /// (`Db::pending_job_shape_refusal`), an enqueue under another shape of a
    /// mounted name refuses its transaction (`Group::stage`), and dispatch
    /// asserts it. No default, unlike `Row::FINGERPRINT`: a hand-written impl
    /// must state one, or a hand-written job would opt out of the check
    /// silently.
    const FINGERPRINT: u64;
    /// The tenant scope type a `#[job(scoped_by = Org)]` job runs in, or
    /// `Unscoped` for a `#[job(global)]` one (job-tenancy spec §5). No
    /// default, for `FINGERPRINT`'s reason: a hand-written impl states it,
    /// or it would slip past the enqueue family's bounds unseen. `'static`
    /// because `push_enqueue` compares it by `TypeId`.
    type Scope: 'static;
    const RETRY: crate::job::RetryPolicy = crate::job::RetryPolicy::DEFAULT;
}

/// A realtime event (DESIGN.md §6). Emits materialize into `charpente.events`
/// at apply, the same way enqueues materialize `JobRow` — subscribers read
/// through the ranged scan (`View::scan_from`), not the log directly.
/// v0: implemented by hand; `#[event]` will derive it.
pub trait Event: Codec {
    const NAME: &'static str;
    /// The payload's structural fingerprint, as for `Job::FINGERPRINT`:
    /// recorded beside every emit. Delivery decodes a stored event only
    /// under the shape it was written with (payload-shapes spec §3).
    const FINGERPRINT: u64;
}

/// Why a transaction did not return `Ok`: the application's own rejection,
/// or a platform IO failure. The distinction is load-bearing — `App` means
/// nothing was written; `Io` after the WAL sync means the record is durable
/// but unacknowledged and will resurface on recovery. An `Io` carrying one of
/// `Group::stage`'s two refusals, a `JobShapeRefusal` or a
/// `TableWriteRefusal` (each type's own `of`), is the exception, not a
/// platform failure at all: `Group::stage` refused, before anything was
/// written, an enqueue of a mounted job name under another shape or the other
/// tenancy, or a write the `Schema` disagrees with about its table (not
/// registered, or registered under another scope shape or `Row` type), and a
/// retry cannot succeed. `Group::stage` also checks the write gate directly, but
/// that surfaces as `NotPrimary`/`Verifying` below, never as a `Refusal`.
/// The only ordinary `Refusal` an `Io` here can carry is
/// `resolve_effect_scope`'s or `check_replay_version`'s — a replicated
/// record decoded under a mismatched or unscoped shape — raised by apply,
/// after the WAL sync, same as any other `Io`. Every OTHER `Refusal` (a
/// table-gate barrier, a witness conflict, a strict-witness deadline) never
/// reaches `TxError` at all: those come from `Db::open`/`migrate_schema` and
/// `Repl::step`, all outside any transaction. `Refusal::of` finds `Refusal`
/// and both wrappers; each wrapper's `of` finds only itself.
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

/// A named refusal: the platform declining to proceed on state it will not
/// guess about — a Pending job of a foreign shape
/// (`Db::pending_job_shape_refusal`), an enqueue of a mounted job name under
/// another shape or a write the `Schema` disagrees with about its table
/// (`Group::stage`, as `TxError::Io`), a table-gate barrier, a
/// witness conflict, a strict-witness deadline, a replicated record decoded
/// under a mismatched or unscoped shape (`resolve_effect_scope`,
/// `check_replay_version`), the primary's refusal of a diverged or
/// unrecognized follower, or a switchover hand-off with nowhere to land.
/// Carried as an `io::Error`'s inner error — `io::Error::other` at most call
/// sites, `io::Error::new(io::ErrorKind::InvalidData, ..)` where the site
/// already has a more specific kind to keep — so every `io::Result` path is
/// untouched, and read back with `Refusal::of`. The distinction it exists to
/// carry (DESIGN §7, payload-shapes spec §5): a platform IO failure MAY clear
/// on a restart; a refusal never does on its own — its own text names what
/// has to change first — so whoever reports the error must tell the two
/// apart rather than treat every `io::Result::Err` as transient.
pub struct Refusal(pub String);

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The bare text's own `Debug`, so an `io::Error` carrying a refusal prints
/// under `main`'s `Error: …` exactly as one carrying the text alone does: the
/// marker is for `Refusal::of`, not for the operator.
impl fmt::Debug for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for Refusal {}

impl Refusal {
    /// `Some` when `e` carries a `Refusal` — a bare one, or one of
    /// `Group::stage`'s `JobShapeRefusal`/`TableWriteRefusal` special cases,
    /// so every caller that only needs "is this a refusal at all"
    /// (`kernel_stop_line`, spec §5) sees all three without knowing the
    /// wrappers exist.
    pub fn of(e: &io::Error) -> Option<&Refusal> {
        e.get_ref().and_then(|inner| {
            inner
                .downcast_ref::<Refusal>()
                .or_else(|| inner.downcast_ref::<JobShapeRefusal>().map(|j| &j.0))
                .or_else(|| inner.downcast_ref::<TableWriteRefusal>().map(|t| &t.0))
        })
    }
}

/// One of the two refusals `Group::stage` itself can produce
/// (`TableWriteRefusal` is the other): an enqueue of a
/// mounted job name under a foreign shape, or under the same shape with the
/// other tenancy (a tenant for a `global` job, none for a `scoped_by` one).
/// A distinct type, not a flag on
/// `Refusal`, because the ATTEMPT's stage (`kernel/dispatch.rs`,
/// `run_due_into`'s first `group.stage` — the handler's own transaction)
/// asserts that every `Io` error out of it is one of the two; the
/// failure-record stage that follows carries no such assert. `Group::stage`
/// also checks the write gate, but that surfaces as `NotPrimary`/`Verifying`
/// (`TxError`'s own variants), never a `Refusal`, so these two are the only
/// `Refusal`s `Group::stage` can raise at all, and each names a case that
/// IS the handler's own deterministic bug, distinct in kind from a refusal
/// naming node state (payload-shapes spec §5). A second caller relies on the
/// same distinct type: `http/app.rs`'s `tx_error_response` (~line 882)
/// answers a request's own enqueue refusal with a 500 carrying this text,
/// instead of the generic storage-failure response. Wraps a `Refusal`
/// rather than duplicating its text and `Display`, so `Refusal::of` still
/// finds it.
pub struct JobShapeRefusal(pub Refusal);

impl fmt::Display for JobShapeRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// `Refusal`'s `Debug`, for the same reason.
impl fmt::Debug for JobShapeRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for JobShapeRefusal {}

impl JobShapeRefusal {
    /// `Some` only for THIS refusal — the narrow question both callers ask:
    /// dispatch's attempt stage and the request path's `tx_error_response`.
    pub fn of(e: &io::Error) -> Option<&JobShapeRefusal> {
        e.get_ref()
            .and_then(|inner| inner.downcast_ref::<JobShapeRefusal>())
    }
}

/// `Group::stage`'s other refusal: a write the `Schema` disagrees with about
/// its table. Three cases: a table name the `Schema` does not register; a
/// write whose scope shape is not the registered `Row`'s (a scoped `Row`
/// whose `TABLE` is an unscoped table's, or the reverse); and a put or delete
/// through any Rust type other than the registered one, compared by `TypeId`.
/// Two `#[table] struct Invoice` in different modules both declare `TABLE =
/// "Invoice"`, and can share `VERSION` and `FINGERPRINT` too: a fingerprint
/// folds field names and type spellings, not `scoped_by` and not what a
/// spelling resolves to. `Schema::table` registers one type per name, so in
/// the last two cases the writer is a second, unregistered type, and the text
/// names it and the registered one. Refused before the record exists. Before
/// this check, apply refused such a write after the WAL sync (the `Schema`
/// lookup, `resolve_effect_scope`, the registered type's `decode_row`), where
/// the record is durable and every restart replays it into the same refusal;
/// or, where the bytes decoded as the registered type, or the write was a
/// delete, apply stored it as the registered type's, with no error: under
/// another `scoped_by`, in the registered scope type's tenant of the same id.
/// Its own type for `JobShapeRefusal`'s reason: the attempt stage in
/// `kernel/dispatch.rs` and `http/app.rs`'s `tx_error_response` both
/// classify it, as the handler's own deterministic bug with nothing written.
pub struct TableWriteRefusal(pub Refusal);

impl fmt::Display for TableWriteRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// `Refusal`'s `Debug`, for the same reason.
impl fmt::Debug for TableWriteRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for TableWriteRefusal {}

impl TableWriteRefusal {
    /// `Some` only for THIS refusal, as `JobShapeRefusal::of`.
    pub fn of(e: &io::Error) -> Option<&TableWriteRefusal> {
        e.get_ref()
            .and_then(|inner| inner.downcast_ref::<TableWriteRefusal>())
    }
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
    // `scope` is `table::UNSCOPED` for an unscoped table, the record's own
    // decoded scope for a scoped one — `apply_batch`'s effect dispatch
    // resolves which (`resolve_effect_scope`, below), the one place that
    // holds both the record's tag and the table's own `scoped` bit.
    apply_put: fn(&mut Writer<'_>, u64, &[u8]) -> io::Result<()>,
    apply_delete: fn(&mut Writer<'_>, u64, u64) -> io::Result<()>,
    // Rebuilds every one of the table's secondary indexes from its rows;
    // invoked by the migration close-out trigger in `TableMachine::apply`.
    rebuild_indexes: fn(&mut Writer<'_>) -> io::Result<()>,
    version: u32,
    fingerprint: Option<u64>,
    // `Row::Scope != Unscoped` — which wire tag (`0x00`/`0x01` vs
    // `0x05`/`0x06`) this table's Put/Delete effects must carry.
    scoped: bool,
    // The registered `Row` type, which `Group::stage` requires of every
    // write to this table: `TypeId::of::<R>()`, compared in memory only (it
    // is not stable across builds, so it never reaches the log, a
    // fingerprint or `describe`), and `type_name::<R>()` for refusal text.
    row_type: TypeId,
    row_name: &'static str,
    // `IndexDef<R>` is generic per-`Row`, so no `&'static [&'static str]` of
    // names exists to borrow — projected once at registration time instead.
    index_names: Vec<&'static str>,
}

fn apply_put<R: Row>(w: &mut Writer<'_>, scope: u64, row: &[u8]) -> io::Result<()> {
    w.put(scope, &R::decode_row(row)?)
}

fn apply_delete<R: Row>(w: &mut Writer<'_>, scope: u64, id: u64) -> io::Result<()> {
    w.delete::<R>(scope, Id::from_raw(id))
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
    /// Carried straight from `TableOps::scoped` (`Schema::table`'s
    /// `row_is_scoped::<R>()` call) — never recomputed here. Same value
    /// `Db::table_scoped` already hands the archive/replication callers;
    /// describe just gives it a third reader.
    pub scoped: bool,
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

/// A mounted job's declared tenancy, as `Db::register_job_shapes` takes it:
/// `None` for a `global` job, or the `scoped_by` scope's name (as
/// `JobDef.scoped_by` and `describe` spell it) with its `TypeId`
/// (`JobDef.scope_type`). The `TypeId` is compared in memory only and never
/// recorded, so it may differ between builds without touching the log.
pub type DeclaredTenancy = Option<(&'static str, TypeId)>;

/// The set of tables a `Db` can replay log records into. Every `#[table]`
/// type the application writes must be registered — replay works on raw
/// record bytes and needs the typed codecs to maintain indexes.
#[derive(Default, Clone)]
pub struct Schema {
    tables: BTreeMap<&'static str, TableOps>,
    migrations: BTreeMap<&'static str, Vec<crate::migration::MigrationStep>>,
    /// `Job::NAME` → (`Job::FINGERPRINT`, its `DeclaredTenancy`) for every
    /// job type this binary mounts (`Db::register_job_shapes`). Here rather than
    /// on `Db` because a snapshot re-seed rebuilds the `Db` from its schema,
    /// and a registry kept anywhere else would come back empty (spec
    /// amendment A4). Not part of `schema_digest_of`: job shapes are refused
    /// at promote, never compared in Hello (spec §8).
    job_shapes: BTreeMap<&'static str, (u64, DeclaredTenancy)>,
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
                scoped: crate::table::row_is_scoped::<R>(),
                row_type: TypeId::of::<R>(),
                row_name: std::any::type_name::<R>(),
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

    /// `site` names the caller for the refusal below — this lookup runs
    /// from log-record apply, `Db`/`Seeder::table_scoped` (archive export,
    /// repl seeding, archive restore) and `Seeder::rows` (seed/restore
    /// import), none of which share one honest description. This binary's own
    /// writes never reach apply's lookup unregistered: `Group::stage` refuses
    /// them before the record exists (`TableWriteRefusal`), so at apply the
    /// refusal names a log from elsewhere — another binary's, or shipped —
    /// replayed under a `Schema` that no longer has the table.
    fn ops(&self, table: &str, site: &str) -> io::Result<&TableOps> {
        self.tables.get(table).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{site} references table \"{table}\" but the Schema does not include \
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
    // `scope` is `None` for the `0x00`/`0x01` (unscoped) tag, `Some` for the
    // `0x05`/`0x06` (scoped) tag — the wire presence, not the value, is what
    // `resolve_effect_scope` checks against the table's own `TableOps::
    // scoped`: scope 0 is a legal tenant, indistinguishable by value alone
    // from "no scope field" (R35).
    Put {
        table: String,
        scope: Option<u64>,
        row: Vec<u8>,
    },
    Delete {
        table: String,
        scope: Option<u64>,
        id: u64,
    },
    // `shape`: `None` for the pre-shape tags `0x02`/`0x03`, `Some` for
    // `0x08`/`0x09`/`0x0a` — the wire presence, like `scope` above. `None` is
    // foreign to every check downstream (payload-shapes spec §2). `scope`:
    // `Some` exactly for `0x0a`, the scoped enqueue (job-tenancy spec §6).
    Enqueue {
        job: String,
        payload: Vec<u8>,
        key: Option<String>,
        shape: Option<u64>,
        scope: Option<u64>,
    },
    Emit {
        event: String,
        payload: Vec<u8>,
        key: Option<String>,
        shape: Option<u64>,
    },
    // `scope`: same presence-not-value convention as `Put`/`Delete` above —
    // `None` for tag `0x04` (unscoped), `Some` for tag `0x07` (scoped).
    MigratePut {
        table: String,
        scope: Option<u64>,
        id: u64,
        row: Vec<u8>,
    },
}

/// The shape of a collapsed table overlay: table -> (scope, id) -> optional
/// row bytes (`None` = tombstone). Both a transaction's own buffered writes
/// (`TxEffects::overlay`, below) and a `Group`'s accumulated writes across a
/// batch (spec §2) are this same alias — the sameness holds by compiler,
/// not by comment, since `stage` merges one into the other. Widened from a
/// bare `u64` key ahead of the composite-key work (design doc §2): once
/// scoped tables exist, two tenants writing the same
/// app-minted id within one batch would otherwise alias in this map, the
/// second write silently clobbering the first and read-your-writes
/// surfacing the wrong tenant's row. `scope` is `UNSCOPED` (`0`) for an
/// unscoped table and the tenant `ScopedTx` carries for a scoped one, and
/// `encode_record` reads it to build a scoped table's record: a scoped
/// table's `0` slot is tenant 0, a real tenant (R35), not an unset marker.
type GroupOverlay = BTreeMap<&'static str, BTreeMap<(u64, u64), Option<Vec<u8>>>>;

/// The overlay key's scope slot for a table that is not tenant-scoped.
/// Named rather than a bare `0` so the sites that compute a real scope for a
/// `#[table(scoped_by = …)]` table are exactly the ones that do not use this
/// constant.
const UNSCOPED: u64 = 0;

/// One buffered enqueue. `shape` is bare because a shapeless enqueue cannot
/// be represented (payload-shapes amendment A2). `scope` is the tenant a
/// `#[job(scoped_by = …)]` job was enqueued in, `None` for a `global` job:
/// presence, not value, selects tag `0x0a`, so `Some(0)` is tenant 0
/// (job-tenancy spec §6).
struct Enqueue {
    job: &'static str,
    payload: Vec<u8>,
    key: Option<String>,
    shape: u64,
    scope: Option<EnqueuedTenant>,
}

/// The tenant a scoped job was enqueued in: the id the record carries, and
/// the scope type it was opened or named as, which the record does not. A
/// tenant id names a tenant only together with its type, and the dispatcher
/// opens a row's id as the type its MOUNTED job declares, so `Group::stage`
/// refuses an enqueue whose job's scope type is not that one (job-tenancy
/// final review N1). In memory only: the record format is unchanged.
struct EnqueuedTenant {
    id: u64,
    /// `TypeId::of::<J::Scope>()`, compared with the mounted declaration's.
    scope_type: TypeId,
    /// `type_name::<J::Scope>()`, for the refusal's text only.
    scope_name: &'static str,
}

/// A transaction's buffered effects: the collapsed per-table overlay
/// (`None` = tombstone) plus enqueues and emits in call order. `scoped_tables`
/// names which of `overlay`'s tables `encode_record` must tag `0x05`/`0x06`
/// (scoped) rather than `0x00`/`0x01` — `Tx::put`/`Tx::delete` populate both
/// together, from `row_is_scoped::<R>()`, since the overlay's `(scope, id)`
/// key alone cannot tell "scope 0" apart from "no scope" (R35).
/// `migrate_puts`' own scope slot is `Option<u64>` directly (`Some` tags
/// `0x07`, `None` tags `0x04`) rather than a parallel set: `Tx::migrate_put`
/// is not generic over a `Row` type (spec §3's whole point — it is the
/// decode-free path), so it cannot compute `row_is_scoped::<R>()` itself;
/// its caller (`Db::migrate_table`, which has the `Schema`) passes the
/// table's own scoped-ness in per call, already resolved.
#[derive(Default)]
struct TxEffects {
    overlay: GroupOverlay,
    scoped_tables: BTreeSet<&'static str>,
    // Every `Row` type an app put or delete wrote, as `(TABLE,
    // type_name::<R>(), scoped, TypeId::of::<R>())`, `scoped` meaning it went
    // through `put_in`/`delete_in` rather than `Tx::put`/`Tx::delete`. Per
    // writer, not per table, so `Group::stage` sees an unscoped write to a
    // scoped table even beside a scoped write to it, which `scoped_tables`
    // cannot show, and can name the type that wrote. The encoding never
    // reads it.
    written_as: BTreeSet<(&'static str, &'static str, bool, TypeId)>,
    enqueues: Vec<Enqueue>,
    // The emit shape is a bare `u64`, so a shapeless emit cannot be
    // represented (payload-shapes amendment A2).
    emits: Vec<(&'static str, Vec<u8>, Option<String>, u64)>,
    migrate_puts: Vec<(&'static str, Option<u64>, u64, Vec<u8>)>,
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
        let scoped = effects.scoped_tables.contains(table);
        for (&(scope, id), entry) in entries {
            if !scoped {
                // `Tx::put`/`Tx::delete` are the overlay's only writers
                // (besides the SchemaVersion adoption row, always UNSCOPED)
                // and populate `scoped_tables` in lockstep with the overlay
                // — negative space, not a resolvable input.
                assert_eq!(
                    scope, UNSCOPED,
                    "record encode: table \"{table}\" id {id} carries scope {scope} but is not \
                     in TxEffects::scoped_tables — Tx::put/Tx::delete populate both together; \
                     this is an encode_record bug, not a resolvable input"
                );
            }
            match entry {
                Some(row) => {
                    if scoped {
                        out.push(5);
                        encode_bytes(table.as_bytes(), &mut out);
                        scope.encode(&mut out);
                    } else {
                        out.push(0);
                        encode_bytes(table.as_bytes(), &mut out);
                    }
                    encode_bytes(row, &mut out);
                }
                None => {
                    if scoped {
                        out.push(6);
                        encode_bytes(table.as_bytes(), &mut out);
                        scope.encode(&mut out);
                    } else {
                        out.push(1);
                        encode_bytes(table.as_bytes(), &mut out);
                    }
                    id.encode(&mut out);
                }
            }
        }
    }
    for Enqueue {
        job,
        payload,
        key,
        shape,
        scope,
    } in &effects.enqueues
    {
        out.push(if scope.is_some() { 10 } else { 8 });
        encode_bytes(job.as_bytes(), &mut out);
        encode_bytes(payload, &mut out);
        key.encode(&mut out);
        shape.encode(&mut out);
        if let Some(tenant) = scope {
            tenant.id.encode(&mut out);
        }
    }
    for (event, payload, key, shape) in &effects.emits {
        out.push(9);
        encode_bytes(event.as_bytes(), &mut out);
        encode_bytes(payload, &mut out);
        key.encode(&mut out);
        shape.encode(&mut out);
    }
    for (table, scope, id, row) in &effects.migrate_puts {
        match scope {
            None => {
                out.push(4);
                encode_bytes(table.as_bytes(), &mut out);
            }
            Some(scope) => {
                out.push(7);
                encode_bytes(table.as_bytes(), &mut out);
                scope.encode(&mut out);
            }
        }
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
                scope: None,
                row: Vec::<u8>::decode(input)?,
            },
            1 => Effect::Delete {
                table: String::decode(input)?,
                scope: None,
                id: u64::decode(input)?,
            },
            2 => Effect::Enqueue {
                job: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
                key: Option::<String>::decode(input)?,
                shape: None,
                scope: None,
            },
            3 => Effect::Emit {
                event: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
                key: Option::<String>::decode(input)?,
                shape: None,
            },
            4 => Effect::MigratePut {
                table: String::decode(input)?,
                scope: None,
                id: u64::decode(input)?,
                row: Vec::<u8>::decode(input)?,
            },
            5 => Effect::Put {
                table: String::decode(input)?,
                scope: Some(u64::decode(input)?),
                row: Vec::<u8>::decode(input)?,
            },
            6 => Effect::Delete {
                table: String::decode(input)?,
                scope: Some(u64::decode(input)?),
                id: u64::decode(input)?,
            },
            7 => Effect::MigratePut {
                table: String::decode(input)?,
                scope: Some(u64::decode(input)?),
                id: u64::decode(input)?,
                row: Vec::<u8>::decode(input)?,
            },
            8 => Effect::Enqueue {
                job: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
                key: Option::<String>::decode(input)?,
                shape: Some(u64::decode(input)?),
                scope: None,
            },
            9 => Effect::Emit {
                event: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
                key: Option::<String>::decode(input)?,
                shape: Some(u64::decode(input)?),
            },
            10 => Effect::Enqueue {
                job: String::decode(input)?,
                payload: Vec::<u8>::decode(input)?,
                key: Option::<String>::decode(input)?,
                shape: Some(u64::decode(input)?),
                scope: Some(u64::decode(input)?),
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

/// The tables only the platform writes, by name. `JobRow`'s own type is
/// refused by `Tx::put`'s bound (`table::PlatformOwned`), but apply routes a
/// record's effect by TABLE NAME, so any other `Row` whose `TABLE` borrows
/// one of these would write rows the dispatcher trusts. The app-facing
/// writes (`Tx::put`/`delete`, `ScopedTx::put`/`delete`) refuse such a type
/// in a `const` block. That refusal is a post-monomorphization error: `cargo
/// build` and `cargo test` report it at the app's call site, and `cargo
/// check`, which never monomorphizes, does not. No binary that performs the
/// write can be built. Sealing another platform table is its name here plus
/// its `type Scope = PlatformOwned`.
const PLATFORM_TABLES: [&str; 1] = [JobRow::TABLE];

const fn is_platform_table(table: &str) -> bool {
    let table = table.as_bytes();
    let mut i = 0;
    while i < PLATFORM_TABLES.len() {
        let name = PLATFORM_TABLES[i].as_bytes();
        if name.len() == table.len() {
            let mut j = 0;
            while j < name.len() && name[j] == table[j] {
                j += 1;
            }
            if j == name.len() {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// `PLATFORM_TABLES`' refusal, evaluated in a `const` block per written row
/// type, so it fails the build rather than a request.
const fn refuse_platform_table(table: &str) {
    if is_platform_table(table) {
        panic!(
            "this Row's TABLE is \"charpente.jobs\", the platform's job queue — job rows are the \
             platform's: only the enqueue family writes one, because the dispatcher runs a job \
             in the tenant its row names; give this table a name of its own, and run a job by \
             enqueueing it (`tx.enqueue`, `ScopedTx::enqueue`, or `tx.enqueue_in` from a \
             `cross_tenant` route or job)"
        );
    }
}

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
    /// The tenant scope this transaction opened, as
    /// `(TypeId::of::<S>(), type_name::<S>(), scope.raw())`. A transaction
    /// opens ONE scope (spec §8); `open_scope` asserts against this.
    opened_scope: Option<(TypeId, &'static str, u64)>,
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
            opened_scope: None,
        }
    }

    /// Stages a row on an UNSCOPED table. `R: WritableRow` is the write
    /// half of spec §1's compile-time property: a scoped row cannot be
    /// staged under `UNSCOPED` because it cannot reach this method at all,
    /// which is a stronger statement than any assertion here could make.
    /// Scoped writes go through `put_in` (or, in an app, `ScopedTx::put`).
    ///
    /// The same bound refuses a platform row (`JobRow`), and the `const`
    /// block refuses any other row type whose `TABLE` borrows a platform
    /// table's name (`PLATFORM_TABLES`).
    pub fn put<R: WritableRow>(&mut self, row: &R) {
        const { refuse_platform_table(R::TABLE) };
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        self.effects.written_as.insert((
            R::TABLE,
            std::any::type_name::<R>(),
            false,
            TypeId::of::<R>(),
        ));
        self.effects
            .overlay
            .entry(R::TABLE)
            .or_default()
            .insert((UNSCOPED, row.id().raw()), Some(bytes));
    }

    /// `put`'s tombstone, under the same two refusals.
    pub fn delete<R: WritableRow>(&mut self, id: Id<R>) {
        const { refuse_platform_table(R::TABLE) };
        self.effects.written_as.insert((
            R::TABLE,
            std::any::type_name::<R>(),
            false,
            TypeId::of::<R>(),
        ));
        self.effects
            .overlay
            .entry(R::TABLE)
            .or_default()
            .insert((UNSCOPED, id.raw()), None);
    }

    /// The dispatcher's write of a job's outcome: the Done mark, or the
    /// failure record that retries or dead-letters it. Outside the enqueue
    /// family this is the only write of a job row: `Tx::put` and `Tx::delete`
    /// refuse `JobRow` at compile time, because the dispatcher opens the
    /// tenant a row's `scope` names without asking anyone, and a row app
    /// code could write would run a job in any tenant.
    ///
    /// It takes a `TenantAdmission`, which only a `Group` mints. A handler
    /// holds `&mut Tx` and `&Ctx`, never a `Group`, and a job's glue holds a
    /// `JobScope` whose admission is private, so no app code can call this.
    /// Operator code that holds a `Group` can: it is trusted by that
    /// standing (job-tenancy spec §3, §7), since whoever holds the `Db`
    /// already holds the store.
    pub fn settle_job(&mut self, row: &JobRow, admission: &TenantAdmission) {
        let TenantAdmission(()) = admission;
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        self.effects
            .overlay
            .entry(JobRow::TABLE)
            .or_default()
            .insert((UNSCOPED, row.id.raw()), Some(bytes));
    }

    /// Stages a row on a SCOPED table under `scope` — the primitive
    /// `ScopedTx::put` delegates to once its scope was entered, either by
    /// `Tenant::authorize` passing or by a `TenantAdmission` — the
    /// operator's, or the platform's on a `public(S)` route.
    ///
    /// PRIVATE, and the whole seam is: `ScopedTx` is the only caller, and
    /// `Tx::open_scope`/`Tx::admit_scope` are `ScopedTx`'s only constructors,
    /// so naming a tenant requires either that tenant's own check or one of
    /// those admissions to have run. A `pub` version of this method is
    /// reachable from every handler holding a bare `&mut Tx`, which makes
    /// spec §7's exit criterion 1 false however the wrappers are written.
    ///
    /// `scope: Id<S>` with `R: Row<Scope = S>`, not a bare `u64`: "this scope
    /// value belongs to this table's tenant type" stays a compile error at the
    /// primitive itself, not only at `ScopedTx`'s wrapper. `S` is bounded only
    /// `'static` here rather than `Tenant`, because the migration and replay
    /// paths address a scoped table by scope value with no tenant hook in
    /// hand.
    fn put_in<S: 'static, R: Row<Scope = S>>(&mut self, scope: Id<S>, row: &R) {
        assert!(
            crate::table::row_is_scoped::<R>(),
            "Tx::put_in: table \"{}\" is not tenant-scoped (Row::Scope = Unscoped) — use Tx::put \
             for an unscoped table",
            R::TABLE
        );
        let mut bytes = Vec::new();
        row.encode_row(&mut bytes);
        self.effects.scoped_tables.insert(R::TABLE);
        self.effects.written_as.insert((
            R::TABLE,
            std::any::type_name::<R>(),
            true,
            TypeId::of::<R>(),
        ));
        self.effects
            .overlay
            .entry(R::TABLE)
            .or_default()
            .insert((scope.raw(), row.id().raw()), Some(bytes));
    }

    /// `delete`'s scope-taking counterpart; see `put_in`.
    fn delete_in<S: 'static, R: Row<Scope = S>>(&mut self, scope: Id<S>, id: Id<R>) {
        assert!(
            crate::table::row_is_scoped::<R>(),
            "Tx::delete_in: table \"{}\" is not tenant-scoped (Row::Scope = Unscoped) — use \
             Tx::delete for an unscoped table",
            R::TABLE
        );
        self.effects.scoped_tables.insert(R::TABLE);
        self.effects.written_as.insert((
            R::TABLE,
            std::any::type_name::<R>(),
            true,
            TypeId::of::<R>(),
        ));
        self.effects
            .overlay
            .entry(R::TABLE)
            .or_default()
            .insert((scope.raw(), id.raw()), None);
    }

    /// Buffers one decode-free migration write (spec §3). `row` must
    /// already be encoded under the table's TARGET version. `scope` is
    /// `None` for an unscoped table (tag `0x04`), `Some` for a scoped one
    /// (tag `0x07`) — the caller (`Db::migrate_table`) already knows the
    /// table's own scoped-ness from `Schema`, since this method is not
    /// generic over a `Row` type and cannot ask `row_is_scoped::<R>()`
    /// itself (spec §3: this is the decode-free path). Used only by
    /// `Db::migrate_schema`'s executor — deliberately not `pub`.
    pub(crate) fn migrate_put(
        &mut self,
        table: &'static str,
        scope: Option<u64>,
        id: u64,
        row: Vec<u8>,
    ) {
        self.effects.migrate_puts.push((table, scope, id, row));
    }

    /// The one body of the enqueue family (job-tenancy spec §5–§6). `scope`
    /// is the tenant a scoped job runs in, and `None` for a `global` one.
    /// The public methods' bounds decide which, and this asserts that they
    /// agree with the job's own declaration, so a scoped job is never
    /// written without its tenant and a global one never with one (hard
    /// rule 4).
    fn push_enqueue<J: Job>(&mut self, scope: Option<u64>, key: Option<String>, job: J) {
        assert_eq!(
            scope.is_some(),
            TypeId::of::<J::Scope>() != TypeId::of::<crate::table::Unscoped>(),
            "contract: job \"{}\" was enqueued with tenant {scope:?}, but a scoped job carries \
             its tenant and a global one none — the enqueue family's bounds guarantee this, so \
             reaching here is a platform bug",
            J::NAME
        );
        if let Some(key) = &key {
            assert!(
                !key.is_empty(),
                "contract: an idempotency key names the job's external identity — it cannot be \
                 empty"
            );
        }
        let mut payload = Vec::new();
        job.encode(&mut payload);
        self.effects.enqueues.push(Enqueue {
            job: J::NAME,
            payload,
            key,
            shape: J::FINGERPRINT,
            scope: scope.map(|id| EnqueuedTenant {
                id,
                scope_type: TypeId::of::<J::Scope>(),
                scope_name: std::any::type_name::<J::Scope>(),
            }),
        });
    }

    /// Enqueues a `global` job into this transaction's record. A
    /// `#[job(scoped_by = Org)]` job does not compile here: it names a
    /// tenant, and a bare `Tx` names none (`NoJobTenant`'s note says where to
    /// enqueue it instead).
    pub fn enqueue<J: UnscopedJob>(&mut self, job: J) {
        self.push_enqueue(None, None, job);
    }

    /// Enqueues with an idempotency key: the job's stable identity to the world
    /// OUTSIDE the log (consumed by M1's external-IO surface). Inside the
    /// deterministic boundary the key means nothing — exactly-once state effects
    /// are structural, so the platform neither indexes nor dedupes on it.
    pub fn enqueue_keyed<J: UnscopedJob>(&mut self, key: impl Into<String>, job: J) {
        self.push_enqueue(None, Some(key.into()), job);
    }

    /// The declared fan-out (job-tenancy spec §2 R5): enqueues a scoped job
    /// into the tenant `scope` names, from a route or job that declared
    /// `cross_tenant`. The token is the declaration, so one inventory,
    /// `describe`'s `cross_tenant=true`, answers "what reaches every tenant
    /// here", by reads and by fan-out. The job runs in `scope` exactly as if
    /// it had been enqueued from inside it.
    ///
    /// The job is taken in argument position (`impl Job`) so that `S` is the
    /// only nameable type parameter: `NoJobTenant`'s note teaches
    /// `tx.enqueue_in::<Org>(scope, job, cap)`, which a second named parameter
    /// would make E0107 (job-tenancy spec amendment A13).
    pub fn enqueue_in<S: Tenant>(
        &mut self,
        scope: Id<S>,
        job: impl Job<Scope = S>,
        cap: &CrossTenant,
    ) {
        let CrossTenant(()) = cap;
        self.push_enqueue(Some(scope.raw()), None, job);
    }

    pub fn emit<Ev: Event>(&mut self, event: Ev) {
        let mut payload = Vec::new();
        event.encode(&mut payload);
        self.effects
            .emits
            .push((Ev::NAME, payload, None, Ev::FINGERPRINT));
    }

    /// Emits with an opaque scoping key (e.g. an org id) — the exact-match
    /// filter `Subscription::key` matches against. Mirrors `enqueue_keyed`.
    pub fn emit_keyed<Ev: Event>(&mut self, key: impl Into<String>, event: Ev) {
        let mut payload = Vec::new();
        event.encode(&mut payload);
        self.effects
            .emits
            .push((Ev::NAME, payload, Some(key.into()), Ev::FINGERPRINT));
    }
}

/// One table's staged writes, as both overlay levels hold them.
type TableOverlay = BTreeMap<(u64, u64), Option<Vec<u8>>>;

/// Applies one overlay level's entries for ONE scope onto a merged read,
/// keyed by bare id. The `entry_scope != scope` skip is the whole of spec
/// §2's overlay rule on the read side: `self.group` is the BATCH-level
/// overlay, shared by every transaction in the batch, so it holds other
/// tenants' staged rows and tombstones. Merging it by bare id makes tenant
/// B's row 7 overwrite tenant A's row 7 in A's own scan, and B's tombstone
/// for row 7 delete A's row 7 out of A's own scan — both endorsed by the
/// types, since the call is `ScopedTx<A>::scan`. Filtering has to happen
/// BEFORE the merge: `(scope, id)` iterates in scope order, so the highest
/// scope would win a merge-then-hope-ordering-saves-you.
///
/// One function rather than a loop per call site because the six call sites
/// (txn overlay and group overlay, for `scan`/`scan_from`, scoped and not)
/// differ only in their inputs — and a copy of this loop that forgot the
/// filter is a silent cross-tenant read, which is exactly the drift a
/// shared function makes unrepresentable.
fn merge_overlay_rows<R: Row>(
    merged: &mut BTreeMap<u64, R>,
    overlay: Option<&TableOverlay>,
    scope: u64,
    from_id: u64,
) -> io::Result<()> {
    let Some(overlay) = overlay else {
        return Ok(());
    };
    let scoped = crate::table::row_is_scoped::<R>();
    for (&(entry_scope, id), entry) in overlay {
        if scoped {
            if entry_scope != scope {
                continue;
            }
        } else {
            // Negative space (hard rule 4): an unscoped table's overlay is
            // written only by `Tx::put`/`Tx::delete` (both `R: WritableRow`),
            // `Tx::settle_job` and the SchemaVersion adoption row, all of
            // which stage `UNSCOPED`. A skip here would turn a staging bug into a
            // silently missing row instead of a loud one.
            assert_eq!(
                entry_scope,
                UNSCOPED,
                "table \"{}\" is not tenant-scoped but its overlay holds id {id} under scope \
                 {entry_scope} — Tx::put/Tx::delete stage UNSCOPED only",
                R::TABLE
            );
        }
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
    Ok(())
}

/// `merge_overlay_rows` for an index read: the overlay overrides the
/// snapshot for an id either way — a tombstone, or a row whose indexed key
/// may no longer match — so every surviving entry is removed first and
/// re-inserted only if it still matches `wanted`. Same scope filter, same
/// reason.
fn merge_overlay_by_index<R: Row>(
    merged: &mut BTreeMap<u64, R>,
    overlay: Option<&TableOverlay>,
    scope: u64,
    index: &IndexDef<R>,
    wanted: &[u8],
) -> io::Result<()> {
    let Some(overlay) = overlay else {
        return Ok(());
    };
    let scoped = crate::table::row_is_scoped::<R>();
    for (&(entry_scope, id), entry) in overlay {
        if scoped {
            if entry_scope != scope {
                continue;
            }
        } else {
            assert_eq!(
                entry_scope,
                UNSCOPED,
                "table \"{}\" is not tenant-scoped but its overlay holds id {id} under scope \
                 {entry_scope} — Tx::put/Tx::delete stage UNSCOPED only",
                R::TABLE
            );
        }
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
    Ok(())
}

/// The named index on `R`, or a panic naming the table and the fix — the
/// lookup `scan_by`/`scan_by_in` share.
fn index_on<R: Row>(index_name: &str) -> &'static IndexDef<R> {
    R::INDEXES
        .iter()
        .find(|i| i.name == index_name)
        .unwrap_or_else(|| {
            panic!(
                "no index named \"{index_name}\" on table {} — declare it with #[index] (check \
                 the field's index name)",
                R::TABLE
            )
        })
}

impl View for Tx<'_> {
    fn get<R: UnscopedRow>(&self, id: Id<R>) -> io::Result<Option<R>> {
        match self
            .effects
            .overlay
            .get(R::TABLE)
            .and_then(|t| t.get(&(UNSCOPED, id.raw())))
        {
            Some(Some(bytes)) => Ok(Some(R::decode_row(bytes)?)),
            Some(None) => Ok(None),
            None => match self
                .group
                .and_then(|g| g.get(R::TABLE))
                .and_then(|t| t.get(&(UNSCOPED, id.raw())))
            {
                Some(Some(bytes)) => Ok(Some(R::decode_row(bytes)?)),
                Some(None) => Ok(None),
                None => self.snapshot.get(id),
            },
        }
    }

    fn scan<R: UnscopedRow>(&self) -> io::Result<Vec<R>> {
        let mut merged: BTreeMap<u64, R> = BTreeMap::new();
        for row in self.snapshot.scan::<R>()? {
            merged.insert(row.id().raw(), row);
        }
        merge_overlay_rows(
            &mut merged,
            self.group.and_then(|g| g.get(R::TABLE)),
            UNSCOPED,
            0,
        )?;
        merge_overlay_rows(&mut merged, self.effects.overlay.get(R::TABLE), UNSCOPED, 0)?;
        Ok(merged.into_values().collect())
    }

    fn scan_by<R: UnscopedRow>(&self, index_name: &str, key: &impl Codec) -> io::Result<Vec<R>> {
        let index = index_on::<R>(index_name);
        let mut wanted = Vec::new();
        key.encode(&mut wanted);

        let mut merged: BTreeMap<u64, R> = BTreeMap::new();
        for row in self.snapshot.scan_by::<R>(index_name, key)? {
            merged.insert(row.id().raw(), row);
        }
        merge_overlay_by_index(
            &mut merged,
            self.group.and_then(|g| g.get(R::TABLE)),
            UNSCOPED,
            index,
            &wanted,
        )?;
        merge_overlay_by_index(
            &mut merged,
            self.effects.overlay.get(R::TABLE),
            UNSCOPED,
            index,
            &wanted,
        )?;
        Ok(merged.into_values().collect())
    }

    fn scan_from<R: UnscopedRow>(&self, from_id: u64, limit: usize) -> io::Result<Vec<R>> {
        let mut merged: BTreeMap<u64, R> = BTreeMap::new();
        for row in self.snapshot.scan::<R>()? {
            if row.id().raw() >= from_id {
                merged.insert(row.id().raw(), row);
            }
        }
        merge_overlay_rows(
            &mut merged,
            self.group.and_then(|g| g.get(R::TABLE)),
            UNSCOPED,
            from_id,
        )?;
        merge_overlay_rows(
            &mut merged,
            self.effects.overlay.get(R::TABLE),
            UNSCOPED,
            from_id,
        )?;
        Ok(merged.into_values().take(limit).collect())
    }
}

/// The scope-taking reads: `View`'s four methods, bounded to ONE tenant of
/// ONE scoped table, over the same txn -> group -> snapshot order. PRIVATE,
/// like `put_in`/`delete_in`: `ScopedTx` below is the only caller, and
/// `Tx::open_scope`/`Tx::admit_scope` are `ScopedTx`'s only constructors, so
/// naming a tenant requires either that tenant's own `Tenant::authorize` to
/// have passed, or an admission in its place — the operator's, or the
/// platform's on a `public(S)` route. `Id<S>` with `R: Row<Scope = S>` — see
/// `put_in`.
///
/// Each asserts `row_is_scoped::<R>()` for the same reason
/// `Snapshot::get_scoped` does (hard rule 4): the type system rules out a
/// scope/table mismatch but not `S = Unscoped`, and an unscoped table read
/// through here would find no overlay entries and then open a physical
/// table the unscoped write path never creates — a wrong answer, not an
/// error.
impl Tx<'_> {
    fn get_in<S: 'static, R: Row<Scope = S>>(
        &self,
        scope: Id<S>,
        id: Id<R>,
    ) -> io::Result<Option<R>> {
        assert_scoped::<R>("get_in", "View::get");
        let key = (scope.raw(), id.raw());
        match self.effects.overlay.get(R::TABLE).and_then(|t| t.get(&key)) {
            Some(Some(bytes)) => Ok(Some(R::decode_row(bytes)?)),
            Some(None) => Ok(None),
            None => match self
                .group
                .and_then(|g| g.get(R::TABLE))
                .and_then(|t| t.get(&key))
            {
                Some(Some(bytes)) => Ok(Some(R::decode_row(bytes)?)),
                Some(None) => Ok(None),
                None => self.snapshot.get_scoped(scope.raw(), id),
            },
        }
    }

    fn scan_in<S: 'static, R: Row<Scope = S>>(&self, scope: Id<S>) -> io::Result<Vec<R>> {
        assert_scoped::<R>("scan_in", "View::scan");
        let mut merged: BTreeMap<u64, R> = BTreeMap::new();
        for row in self.snapshot.scan_scoped::<R>(scope.raw())? {
            merged.insert(row.id().raw(), row);
        }
        merge_overlay_rows(
            &mut merged,
            self.group.and_then(|g| g.get(R::TABLE)),
            scope.raw(),
            0,
        )?;
        merge_overlay_rows(
            &mut merged,
            self.effects.overlay.get(R::TABLE),
            scope.raw(),
            0,
        )?;
        Ok(merged.into_values().collect())
    }

    fn scan_by_in<S: 'static, R: Row<Scope = S>>(
        &self,
        scope: Id<S>,
        index_name: &str,
        key: &impl Codec,
    ) -> io::Result<Vec<R>> {
        assert_scoped::<R>("scan_by_in", "View::scan_by");
        let index = index_on::<R>(index_name);
        let mut wanted = Vec::new();
        key.encode(&mut wanted);

        let mut merged: BTreeMap<u64, R> = BTreeMap::new();
        for row in self
            .snapshot
            .scan_by_scoped::<R>(scope.raw(), index_name, key)?
        {
            merged.insert(row.id().raw(), row);
        }
        merge_overlay_by_index(
            &mut merged,
            self.group.and_then(|g| g.get(R::TABLE)),
            scope.raw(),
            index,
            &wanted,
        )?;
        merge_overlay_by_index(
            &mut merged,
            self.effects.overlay.get(R::TABLE),
            scope.raw(),
            index,
            &wanted,
        )?;
        Ok(merged.into_values().collect())
    }

    fn scan_from_in<S: 'static, R: Row<Scope = S>>(
        &self,
        scope: Id<S>,
        from_id: u64,
        limit: usize,
    ) -> io::Result<Vec<R>> {
        assert_scoped::<R>("scan_from_in", "View::scan_from");
        let mut merged: BTreeMap<u64, R> = BTreeMap::new();
        for row in self.snapshot.scan_scoped::<R>(scope.raw())? {
            if row.id().raw() >= from_id {
                merged.insert(row.id().raw(), row);
            }
        }
        merge_overlay_rows(
            &mut merged,
            self.group.and_then(|g| g.get(R::TABLE)),
            scope.raw(),
            from_id,
        )?;
        merge_overlay_rows(
            &mut merged,
            self.effects.overlay.get(R::TABLE),
            scope.raw(),
            from_id,
        )?;
        Ok(merged.into_values().take(limit).collect())
    }
}

/// The shared body of the four scope-taking reads' `row_is_scoped` guard.
fn assert_scoped<R: Row>(method: &str, unscoped_alternative: &str) {
    assert!(
        crate::table::row_is_scoped::<R>(),
        "Tx::{method}: table \"{}\" is not tenant-scoped (Row::Scope = Unscoped) — use \
         {unscoped_alternative} for an unscoped table",
        R::TABLE
    );
}

// ---------------------------------------------------------------- scopes

/// The app's own membership check for a tenant scope: "may this caller act
/// as this tenant?" (spec §1). `#[table(scoped_by = Org)]` requires an impl
/// for `Org` — enforced by a macro-emitted const assert, the same shape
/// `#[table(version = …)]` uses to require a `TableMigration` impl
/// (DESIGN §3).
///
/// Generic over `Who` and `Error` because the deterministic core cannot name
/// `charpente::auth::Principal` or `charpente::http::ApiError` — DESIGN §8
/// keeps auth at the platform edge and there is no reverse dependency
/// (ruling R2, amended by R45). An app writes the platform's specialization,
/// `charpente::TenantScope` (`Who = Principal`, `Error = ApiError`), which is
/// blanket-implemented for every such impl and carries the house diagnostic.
///
/// The platform calls `authorize` in exactly one place, `Tx::open_scope`
/// below, and only with a principal — and never DERIVES a scope: the tenant
/// is named by the app at the one `tx.scope::<S>(…)` line and nowhere else,
/// and no key is ever parsed back into one (the standing `emit_keyed`
/// ruling, DESIGN.md §6). The one
/// thing it does read off a scope is its decimal form, as the subscription
/// key `ScopedTx::emit` attaches (`ScopedTx::event_key`, spec §4) — rendering
/// out, never resolving in.
pub trait Tenant: Sized + 'static {
    /// Whoever is asking. `?Sized` so an app can key membership on an
    /// unsized witness (a `str` subject) as readily as on a struct.
    type Who: ?Sized;
    /// The refusal an app returns; `open_scope` returns it unchanged.
    type Error;

    /// Asked only about a real `who`: there is no `None` to pass
    /// (job-tenancy spec §3). A way into a scope with no one to ask about is
    /// declared instead: `Tx::admit_scope`, whose `TenantAdmission` only a
    /// `Group` mints.
    fn authorize(tx: &Tx<'_>, who: &Self::Who, scope: Id<Self>) -> Result<(), Self::Error>;
}

impl<'a> Tx<'a> {
    /// The one body both scope constructors share (job-tenancy spec §3). The
    /// one-scope contract is checked FIRST, because it is decidable without
    /// asking anyone: a second tenant on this transaction gets no handle
    /// whatever `admit` would answer, so the app's `authorize` — and any
    /// audit entry or side effect it performs — is never invoked for a scope
    /// no handle is issued for.
    fn enter_scope<S: Tenant, E>(
        &mut self,
        scope: Id<S>,
        admit: impl FnOnce(&Tx<'a>) -> Result<(), E>,
    ) -> Result<ScopedTx<'_, 'a, S>, E> {
        let opening = (TypeId::of::<S>(), std::any::type_name::<S>(), scope.raw());
        if let Some(prev) = self.opened_scope {
            assert!(
                (prev.0, prev.2) == (opening.0, opening.2),
                "contract: a transaction opens ONE tenant scope (spec §8) — this one already \
                 opened {}({}) and is now asked for {}({}); run the second tenant's work in its \
                 own transaction",
                prev.1,
                prev.2,
                opening.1,
                opening.2
            );
        }
        admit(self)?;
        self.opened_scope = Some(opening);
        Ok(ScopedTx { tx: self, scope })
    }

    /// Opens a tenant scope on a member's word: `S::authorize` runs, and its
    /// refusal is returned unchanged. With `admit_scope` below, the only
    /// constructors of `ScopedTx` — the only route to the six scope-taking
    /// primitives above, which are private.
    ///
    /// Apps call `tx.scope::<Org>(org, ctx)?` (`charpente::Scoped`), which
    /// delegates here with the request's principal as `who` — or to
    /// `admit_scope` on a route declared `public(S)`, or to a 401 for a
    /// caller with no credential (job-tenancy spec §4). The name differs
    /// because an inherent method shadows an extension trait's method of the
    /// same name, and the app-facing spelling is the one with the `Ctx`.
    ///
    /// `open_scope` is generic over `Who` (this trait's own `type Who`), so
    /// this rule is about the platform's own path, `Who = Principal`: `who`
    /// is whoever the app is asking about, `tx.scope` passes the request's
    /// principal and nothing else can, and a caller with no principal has no
    /// business here — it enters by a declaration (`admit_scope`), or not at
    /// all. A scope type with a different `Who` (a `Who = str` asker, say)
    /// names its own caller its own way; operator code with no principal at
    /// all is an ordinary `who` for one of those.
    ///
    /// # Panics
    ///
    /// A transaction opens ONE scope (spec §8). A second `open_scope` or
    /// `admit_scope` naming a different tenant — including one reached
    /// through `ScopedTx`'s `DerefMut` — is a contract violation, not a
    /// cross-tenant write. Re-opening the SAME scope is a no-op.
    pub fn open_scope<S: Tenant>(
        &mut self,
        scope: Id<S>,
        who: &S::Who,
    ) -> Result<ScopedTx<'_, 'a, S>, S::Error> {
        self.enter_scope(scope, |tx| S::authorize(tx, who, scope))
    }

    /// Opens a tenant scope on the operator's word, with no
    /// `Tenant::authorize` (job-tenancy spec §7): the `TenantAdmission` is
    /// the proof that the holder of a `Group` — whoever holds the `Db` —
    /// admitted this transaction, and there is no principal to ask about.
    /// The one-scope contract and the handle are exactly `open_scope`'s: the
    /// two share one body.
    ///
    /// # Panics
    ///
    /// As `open_scope`: a second, different tenant on one transaction.
    pub fn admit_scope<S: Tenant>(
        &mut self,
        scope: Id<S>,
        admission: &TenantAdmission,
    ) -> ScopedTx<'_, 'a, S> {
        let TenantAdmission(()) = admission;
        let Ok(scoped) = self.enter_scope(scope, |_| Ok::<(), std::convert::Infallible>(()));
        scoped
    }
}

/// A transaction narrowed to one tenant of one scope type — the only handle
/// an app has on a scoped table (spec §1). `Tx::open_scope` and
/// `Tx::admit_scope` are its only two constructors (job-tenancy spec §3): the
/// former runs `S::authorize` before handing one back, the latter takes a
/// `TenantAdmission` that only a `Group` mints — the operator's standing, or
/// the platform's own on a `public(S)` route, never a handler's. Either way
/// there is no second way to reach a scoped row and no way to reach one
/// without the tenant's own membership check, or one of those admissions in
/// its place, having run.
///
/// Two lifetimes, not one. `'t` is the borrow of the transaction; `'a` is
/// the transaction's own borrow of the snapshot it reads from. They cannot
/// be collapsed: `&'t mut Tx<'a>` is invariant in `'a`, so a handler holding
/// `&'t mut Tx<'a>` (with `'a` strictly longer, since the snapshot outlives
/// the transaction) can never produce the `&'t mut Tx<'t>` a single-lifetime
/// `ScopedTx<'t>` would need. Handlers write `tx.scope::<Org>(org, ctx)?`
/// and never name either.
///
/// `Deref`/`DerefMut` to `Tx` are required, not conveniences: opening a
/// scope must not lock a handler out of unscoped tables (spec §1). Method
/// resolution does NOT fall through the deref when an inherent method of the
/// same name exists but its bound fails, so an unscoped access from inside a
/// scope is written through an explicit deref:
///
/// ```ignore
/// let tx = tx.scope::<Org>(org, ctx)?;
/// tx.scan::<Invoice>()?;      // scoped:   Invoice is scoped_by = Org
/// (*tx).scan::<Region>()?;    // unscoped: Region has no tenant
/// ```
///
/// What the deref does NOT reopen is the seam: `put_in`/`delete_in`/
/// `get_in`/`scan_in`/`scan_by_in`/`scan_from_in` are private to this
/// module, so `(*tx)` reaches unscoped tables and a second `open_scope`
/// (which authorizes, and which the one-scope assert bounds) — never another
/// tenant's rows.
pub struct ScopedTx<'t, 'a, S> {
    tx: &'t mut Tx<'a>,
    scope: Id<S>,
}

impl<'a, S: 'static> ScopedTx<'_, 'a, S> {
    /// The tenant every read and write on this handle is bounded to (R18:
    /// the scope is explicit, and this field is its source). Also the reason
    /// a scope cannot be nested: this shadows the `Scoped::scope` the deref
    /// would otherwise reach, and spec §8 rules nested scopes out.
    pub fn scope(&self) -> Id<S> {
        self.scope
    }

    pub fn get<R: Row<Scope = S>>(&self, id: Id<R>) -> io::Result<Option<R>> {
        self.tx.get_in(self.scope, id)
    }

    /// Every row of `R` belonging to this tenant, in id order.
    pub fn scan<R: Row<Scope = S>>(&self) -> io::Result<Vec<R>> {
        self.tx.scan_in(self.scope)
    }

    /// Exact-match secondary index lookup within this tenant, in id order.
    pub fn scan_by<R: Row<Scope = S>>(
        &self,
        index_name: &str,
        key: &impl Codec,
    ) -> io::Result<Vec<R>> {
        self.tx.scan_by_in(self.scope, index_name, key)
    }

    /// This tenant's rows with `id >= from_id`, in id order, at most `limit`.
    pub fn scan_from<R: Row<Scope = S>>(&self, from_id: u64, limit: usize) -> io::Result<Vec<R>> {
        self.tx.scan_from_in(self.scope, from_id, limit)
    }

    /// Stages a row of this tenant. The `const` block is `Tx::put`'s: a row
    /// type whose `TABLE` borrows a platform table's name is refused here too.
    pub fn put<R: Row<Scope = S>>(&mut self, row: &R) {
        const { refuse_platform_table(R::TABLE) };
        self.tx.put_in(self.scope, row)
    }

    pub fn delete<R: Row<Scope = S>>(&mut self, id: Id<R>) {
        const { refuse_platform_table(R::TABLE) };
        self.tx.delete_in(self.scope, id)
    }

    /// The subscription key every event emitted on this handle carries: the
    /// scope's decimal id. Public because it is the SAME encoding on both
    /// sides of the exact match — an SSE handler keys its `Subscription` with
    /// it, so emitter and subscriber cannot drift into two spellings of one
    /// tenant (spec §4).
    ///
    /// Decimal rather than anything type-qualified because this key is
    /// DURABLE: it is stored in `EventRow.key`, shipped by replication, and
    /// exact-matched when a subscriber resumes. That rules out `type_name` and
    /// `TypeId`, neither of which the compiler promises to keep stable across
    /// builds — a key whose spelling moved under a binary upgrade would make
    /// every pre-upgrade backlog event silently invisible to a reconnecting
    /// subscriber. The consequence, stated rather than hidden: two DIFFERENT
    /// scope types sharing a raw id share a key, so an app with more than one
    /// scope type keys its subscriptions per scope type as well.
    pub fn event_key(&self) -> String {
        self.scope.raw().to_string()
    }

    /// `Tx::emit`, keyed by this tenant — spec §4's "one tenancy mechanism,
    /// not two". A transaction that had to pass `authorize` to reach a
    /// tenant's rows cannot then hand that tenant's event to a subscription
    /// keyed to another one, so an app that has locked its queries down does
    /// not leak through a subscription.
    ///
    /// This shadows the `Tx::emit` the `DerefMut` would otherwise reach. An
    /// emit that genuinely belongs to no tenant is written `(*tx).emit(…)` —
    /// the same explicit deref an unscoped table access takes.
    pub fn emit<Ev: Event>(&mut self, event: Ev) {
        let key = self.event_key();
        self.tx.emit_keyed(key, event);
    }

    /// Enqueues a job scoped by this tenant's type; it runs in this tenant
    /// (job-tenancy spec §5). This shadows `Tx::enqueue` the way `emit`
    /// shadows `Tx::emit`. A `global` job enqueued inside a scope is written
    /// `(*tx).enqueue(job)` in a handler that owns its `ScopedTx`, and
    /// `(**tx).enqueue(job)` in a scoped job's `run`, which holds
    /// `&mut ScopedTx`.
    pub fn enqueue<J: Job<Scope = S>>(&mut self, job: J) {
        let scope = self.scope.raw();
        self.tx.push_enqueue(Some(scope), None, job);
    }

    /// `enqueue`, with `Tx::enqueue_keyed`'s idempotency key.
    pub fn enqueue_keyed<J: Job<Scope = S>>(&mut self, key: impl Into<String>, job: J) {
        let scope = self.scope.raw();
        self.tx.push_enqueue(Some(scope), Some(key.into()), job);
    }
}

impl<'a, S> std::ops::Deref for ScopedTx<'_, 'a, S> {
    type Target = Tx<'a>;
    fn deref(&self) -> &Tx<'a> {
        self.tx
    }
}

impl<'a, S> std::ops::DerefMut for ScopedTx<'_, 'a, S> {
    fn deref_mut(&mut self) -> &mut Tx<'a> {
        self.tx
    }
}

// ------------------------------------------------------- cross-tenant reach

/// The capability a route or job declares with `cross_tenant` (spec §3,
/// job-tenancy spec §2 R5): naming a tenant from inside it is always
/// allowed; reaching tenants from outside every one is declared. That reach
/// is two doors, and both take this token: enumerating a scoped table across
/// every tenant (`Tx::across_tenants`) and fanning a scoped job out into a
/// tenant the caller names (`Tx::enqueue_in`). A handler that was not handed
/// a token can do neither — and because the declaration is on the route or
/// the job, `charpente describe` can list every endpoint that can.
///
/// Unforgeable from a handler, which is what keeps that listing an inventory
/// rather than a hint: the struct's field is private and
/// `Group::cross_tenant_cap` is the only constructor. A handler holds
/// `&mut Tx` and `&Ctx`, never a `Group` — while whoever holds the `Db` the
/// group borrows is the operator, who already reaches every tenant through
/// `Db::snapshot` (ruling R48).
///
/// `Clone` because the platform hands the same request's token to the `Ctx`
/// and to the handler argument; cloning widens nothing, since holding one at
/// all IS the capability.
#[derive(Clone)]
pub struct CrossTenant(());

impl<'a> Group<'a> {
    /// Mints the token the platform hands a route or job that declared
    /// `cross_tenant`. See `CrossTenant`: this is the only constructor, and
    /// it takes a `&Group` because that is a handle a handler never has.
    pub fn cross_tenant_cap(&self) -> CrossTenant {
        CrossTenant(())
    }
}

// ------------------------------------------------------- admission

/// Proof that the operator, or a route's `public(S)` declaration, admitted
/// this transaction into one tenant — `Tx::admit_scope`'s argument
/// (job-tenancy spec §3, §4, §7), and `Tx::settle_job`'s, the one write of
/// a job row outside the enqueue family.
///
/// Holders: the operator, through a `Group`; the platform, which mints
/// one per request for a route declared `public(S)` and keeps it private in
/// that request's `Ctx`, so `tx.scope::<S>` admits by the declaration; and
/// the dispatcher, which mints one per job run, hands a scoped job's glue
/// a `JobScope` that opens only the tenant id its row carries, and settles
/// the row with it.
///
/// Sealed exactly as `CrossTenant` is, and for the same reason: its field is
/// private and `Group::tenant_admission` is the only constructor, and a
/// handler holds `&mut Tx` and `&Ctx`, never a `Group`. Whoever holds the
/// `Db` a group borrows is the operator, who already reaches every tenant
/// through `Db::snapshot` (ruling R48); the admission makes that standing
/// explicit at a scope line instead of leaving it an exception to the
/// guarantee.
///
/// `Clone` for `CrossTenant`'s reason: holding one at all is the admission,
/// so a copy widens nothing.
#[derive(Clone)]
pub struct TenantAdmission(());

impl<'a> Group<'a> {
    /// Mints the admission `Tx::admit_scope` takes. The only constructor;
    /// see `TenantAdmission`.
    pub fn tenant_admission(&self) -> TenantAdmission {
        TenantAdmission(())
    }
}

impl<'a> Tx<'a> {
    /// The global reader (spec §3). Enumerates a scoped table across every
    /// tenant, which is why it takes the declared capability: `cap` is the
    /// proof that some route or job said `cross_tenant` out loud, and
    /// nothing else about it is read.
    ///
    /// No `Tenant::authorize` runs here, and none could: there is no single
    /// tenant to ask about. The declaration is the authorization, and it is
    /// auditable because `charpente describe` enumerates it.
    pub fn across_tenants<S: 'static>(&self, cap: &CrossTenant) -> AllTenants<'_, 'a, S> {
        let CrossTenant(()) = cap;
        AllTenants {
            tx: self,
            scope: std::marker::PhantomData,
        }
    }
}

/// A read handle over every tenant's rows of one scope type — what
/// `Tx::across_tenants` hands back. Read-only by construction: it borrows
/// the transaction immutably, so the cross-tenant surface is a reader and
/// there is no `put`/`delete` here to write one tenant's row under another's
/// scope.
pub struct AllTenants<'t, 'a, S> {
    tx: &'t Tx<'a>,
    scope: std::marker::PhantomData<S>,
}

impl<S: 'static> AllTenants<'_, '_, S> {
    /// Every tenant's rows of `R`, in `(scope, id)` order, each PAIRED WITH
    /// ITS SCOPE — `Vec<(Id<Org>, Invoice)>`, never `Vec<Invoice>` (spec
    /// §3). The tenant rides on every row, so a cross-tenant result cannot
    /// be quietly handed to code expecting one tenant's data: the types
    /// differ, and stripping the scope is a visible `.map`.
    ///
    /// Same txn -> group -> snapshot order as every other read, merged by
    /// `(scope, id)` rather than by bare id: ids are not unique across
    /// tenants, so a merge keyed by id alone would let one tenant's row
    /// overwrite another's in the answer.
    pub fn scan<R: Row<Scope = S>>(&self) -> io::Result<Vec<(Id<S>, R)>> {
        assert_scoped::<R>("across_tenants().scan", "View::scan");
        let mut merged: BTreeMap<(u64, u64), R> = BTreeMap::new();
        for (scope, id, bytes) in self.tx.snapshot.scan_raw(R::TABLE, true)? {
            merged.insert((scope, id), R::decode_row(&bytes)?);
        }
        merge_overlay_all_scopes(&mut merged, self.tx.group.and_then(|g| g.get(R::TABLE)))?;
        merge_overlay_all_scopes(&mut merged, self.tx.effects.overlay.get(R::TABLE))?;
        Ok(merged
            .into_iter()
            .map(|((scope, _), row)| (Id::from_raw(scope), row))
            .collect())
    }
}

/// `merge_overlay_rows` without the scope filter — the one read that wants
/// every tenant's staged rows, keyed by `(scope, id)` so no tenant's entry
/// can land on another's. A separate function rather than a `scope:
/// Option<u64>` parameter on `merge_overlay_rows`: that filter is the whole
/// of spec §2's overlay rule, and a nullable filter is a filter one `None`
/// away from being absent everywhere.
fn merge_overlay_all_scopes<R: Row>(
    merged: &mut BTreeMap<(u64, u64), R>,
    overlay: Option<&TableOverlay>,
) -> io::Result<()> {
    let Some(overlay) = overlay else {
        return Ok(());
    };
    // NO assertion on the scope slot here, deliberately — the mirror of the
    // `assert_eq!` on the unscoped path in `merge_overlay_rows`, which is
    // correct because there the two values genuinely differ in meaning.
    // Here they do not: on a SCOPED table `0` is tenant 0, a real tenant
    // (R35, `GroupOverlay` above), not a sentinel, and it is reachable off
    // the wire — `Id::from_raw` takes any `u64` and `FromParam for Id<T>`
    // passes straight through it. An `assert_ne!(scope, UNSCOPED)` here
    // therefore fires on LEGAL data, and since `crates/charpente/src` has no
    // `catch_unwind` and assertions stay on in release (hard rule 4), it
    // unwinds out of the kernel loop and takes the node down. Worse, this
    // function also reads the GROUP overlay, which accumulates across a
    // batch, so one request's tenant-0 write would kill a different
    // request's cross-tenant read. Pinned by
    // `tenant_zero_is_a_real_tenant_to_a_cross_tenant_read`.
    //
    // The staging bug the assertion was reaching for cannot happen anyway:
    // `Tx::put`/`Tx::delete` take `R: WritableRow`, so a scoped row "cannot
    // reach this method at all, which is a stronger statement than any
    // assertion here could make" (`Tx::put`'s own doc, above).
    for (&(scope, id), entry) in overlay {
        match entry {
            Some(bytes) => {
                merged.insert((scope, id), R::decode_row(bytes)?);
            }
            None => {
                merged.remove(&(scope, id));
            }
        }
    }
    Ok(())
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
                if let Effect::Put { table, scope, row } = effect
                    && table == crate::migration::SchemaVersion::TABLE
                {
                    // Close-out detection (spec §3): a version transition
                    // with the in-flight marker clearing rebuilds the
                    // migrated table's indexes — at apply, so primary,
                    // replay, and followers rebuild at the same log
                    // position.
                    let new = crate::migration::SchemaVersion::decode_row(row)?;
                    let ops = schema.ops(table, "a log record")?;
                    let old = w
                        // SchemaVersion is always unscoped — literal None, not a
                        // computed scoped/UNSCOPED pair that reads as if it could
                        // ever disagree.
                        .get_raw(table, None, new.id.raw())?
                        .map(|b| crate::migration::SchemaVersion::decode_row(&b))
                        .transpose()?;
                    let resolved = resolve_effect_scope(table, ops.scoped, *scope, "Put")?;
                    (ops.apply_put)(w, resolved, row)?;
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
                        (schema.ops(&new.table, "a log record")?.rebuild_indexes)(w)?;
                    }
                    versions.insert(new.table.clone(), (new.version, new.fingerprint));
                }
            }
            // Pass 2: everything else, version-checked before any row byte
            // decodes.
            for effect in &effects {
                match effect {
                    Effect::Put { table, scope, row } => {
                        if table == crate::migration::SchemaVersion::TABLE {
                            continue; // pass 1 applied it
                        }
                        let ops = schema.ops(table, "a log record")?;
                        let resolved = resolve_effect_scope(table, ops.scoped, *scope, "Put")?;
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
                            // `put_raw` carries the resolved scope, so a scoped
                            // table's pre-migration history replays under its
                            // own scope even decode-free.
                            let id = row_id_prefix(row)?;
                            saw_below.insert(table.clone());
                            w.put_raw(table, ops.scoped.then_some(resolved), id, row)?;
                        } else {
                            check_replay_version(schema, versions, table)?;
                            (ops.apply_put)(w, resolved, row)?;
                        }
                    }
                    Effect::Delete { table, scope, id } => {
                        let ops = schema.ops(table, "a log record")?;
                        let resolved = resolve_effect_scope(table, ops.scoped, *scope, "Delete")?;
                        if is_below_compiled(schema, versions, table) {
                            // `delete_raw` carries the resolved scope; see the
                            // Put arm above.
                            saw_below.insert(table.clone());
                            w.delete_raw(table, ops.scoped.then_some(resolved), *id)?;
                        } else {
                            check_replay_version(schema, versions, table)?;
                            (ops.apply_delete)(w, resolved, *id)?;
                        }
                    }
                    Effect::MigratePut {
                        table,
                        scope,
                        id,
                        row,
                    } => {
                        // The rewrite itself: exempt from the version
                        // equality check (its whole point is bytes at the
                        // target version over rows at the old one), still
                        // registered-table-checked, still decode-free.
                        let ops = schema.ops(table, "a log record")?;
                        let resolved =
                            resolve_effect_scope(table, ops.scoped, *scope, "MigratePut")?;
                        w.put_raw(table, ops.scoped.then_some(resolved), *id, row)?;
                    }
                    Effect::Enqueue {
                        job,
                        payload,
                        key,
                        shape,
                        scope,
                    } => {
                        new_ids.push(w.materialize_job(
                            job,
                            payload,
                            key.as_deref(),
                            *shape,
                            *scope,
                        )?);
                    }
                    Effect::Emit {
                        event,
                        payload,
                        key,
                        shape,
                    } => {
                        w.materialize_event(seq, event, payload, key.as_deref(), *shape)?;
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
    if let (Some((through_scope, through_id)), Some(to)) = (row.migrating_through, row.migrating_to)
        && to != compiled_version
    {
        return GateStatus::Refuse(format!(
            "table \"{table}\" has an interrupted migration to version {to} in flight \
             (resumable watermark at scope {through_scope} row id {through_id}) but this \
             binary is compiled for version {compiled_version} — rows on either side of the \
             watermark are at different versions and no single chain start covers both; \
             finish the interrupted migration with the binary that started it, or restore"
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
    if let (Some((through_scope, through_id)), Some(to)) = (row.migrating_through, row.migrating_to)
        && to == compiled_version
        && !crate::migration::fingerprint_matches(Some(row.fingerprint), compiled_fingerprint)
    {
        return GateStatus::Refuse(format!(
            "table \"{table}\" has an interrupted migration to version {to} in flight \
             (resumable watermark at scope {through_scope} row id {through_id}) begun by a \
             binary whose target shape (fingerprint {:#x}) differs from this binary's \
             (fingerprint {compiled_fingerprint:#x}) — resuming would mix two shapes in one \
             table; finish the interrupted migration with the binary that started it, or \
             restore",
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

/// Resolves a decoded Put/Delete effect's scope against the table's own
/// registered scopedness (`TableOps::scoped`, from `Row::Scope`). The wire
/// tag (`0x00`/`0x01` vs `0x05`/`0x06`, carried here as `tag_scope`'s
/// `None`/`Some`) must agree with it — an unscoped table's record must
/// never carry a scope field, and a scoped table's record must always carry
/// one, even at scope 0 (R35: tenant 0 is a legal tenant, not "no scope",
/// so the check is on the tag's PRESENCE, never the scope's value). A
/// disagreement is a schema/log mismatch: the record was written for a
/// different shape of this table than the one this binary has registered,
/// and applying it anyway would silently write under the wrong scope (or
/// none) rather than refuse (hard rule 5). This binary's own writes never
/// reach it disagreeing: `Group::stage` refuses those before the record
/// exists (`TableWriteRefusal`), so here it names a record from elsewhere,
/// shipped or written by another binary.
fn resolve_effect_scope(
    table: &str,
    table_scoped: bool,
    tag_scope: Option<u64>,
    kind: &str,
) -> io::Result<u64> {
    match (table_scoped, tag_scope) {
        (false, None) => Ok(crate::table::UNSCOPED),
        (true, Some(scope)) => Ok(scope),
        (true, None) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            Refusal(format!(
                "record decode: table \"{table}\" is tenant-scoped (Row::Scope != Unscoped) \
                 but this {kind} effect carries no scope (tag 0x00/0x01) — the record was \
                 written for an unscoped shape of this table; restore from a replica or \
                 backup, or replay under the schema that wrote it"
            )),
        )),
        (false, Some(scope)) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            Refusal(format!(
                "record decode: table \"{table}\" is not tenant-scoped (Row::Scope = \
                 Unscoped) but this {kind} effect carries scope {scope} (tag 0x05/0x06) — \
                 the record was written for a scoped shape of this table; restore from a \
                 replica or backup, or replay under the schema that wrote it"
            )),
        )),
    }
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
        return Err(io::Error::other(Refusal(format!(
            "replaying a record for versioned table \"{table}\" with no adoption row on \
             record — this log was not written by an adoption-aware binary, or is corrupt; \
             restore from a replica or backup"
        ))));
    };
    // Every caller (both effect arms above) already calls `is_below_compiled`
    // first and routes a below-compiled record through `put_raw`/`delete_raw`
    // instead of here, on the exact same `persisted_version < compiled_version`
    // comparison over the same `versions` map — so this branch never observes
    // a below-compiled record. Kept for the message text (`upgrade_barrier_msg`
    // is also the post-replay barrier's producer, db.rs `check_schema_gate`)
    // and as a fail-closed backstop if that routing ever changes.
    if persisted_version < compiled_version {
        return Err(io::Error::other(Refusal(upgrade_barrier_msg(
            table,
            persisted_version,
            compiled_version,
        ))));
    }
    if persisted_version > compiled_version {
        return Err(io::Error::other(Refusal(downgrade_msg(
            table,
            persisted_version,
            compiled_version,
        ))));
    }
    if persisted_fp != compiled_fp {
        return Err(io::Error::other(Refusal(drift_msg(
            table,
            persisted_fp,
            compiled_fp,
            compiled_version,
        ))));
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

/// A job's declared tenancy as the tenancy refusals render it: `global`, or
/// `scoped_by = Org` with the scope's name as `JobDef.scoped_by` carries it.
fn declared_tenancy(scoped_by: Option<&str>) -> String {
    scoped_by.map_or_else(|| "`global`".to_string(), |s| format!("`scoped_by = {s}`"))
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
            // outran-durable-truth assert below. Safe only because the store
            // was durable when `applied_seq` was read: a failed seed leaves it
            // in the page cache, and nothing here syncs it — redb 4.3.0's
            // writable open does (DESIGN §4, pinned by `a_seeded_store_a_-
            // restarted_node_lays_its_segment_from_survives_the_next_power_loss`).
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
    /// store; on `Err(TxError::App)` nothing was written; on
    /// `Err(TxError::Io)` the outcome is unknown — an append or sync that
    /// failed after `f` returned may still leave the record durable, and the
    /// next recovery replays it. A transaction with no effects writes nothing
    /// and consumes no seq.
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
    ///
    /// Handing one out hands out every tenant (ruling R48). A `Snapshot`
    /// carries no principal, so its scope-taking reads
    /// (`get_scoped`/`scan_scoped`/`scan_from_scoped`/`scan_by_scoped`) and
    /// its raw reads name a tenant on the caller's word, with no
    /// `Tenant::authorize` anywhere on the path. The tenancy guarantee covers
    /// the typed surface the framework hands app logic — a handler and a job
    /// receive a `Tx`, and reaching a scoped table through one requires
    /// `Tx::open_scope` and the tenant's own check. Whoever holds a `Db`
    /// holds the store, with the operator's standing.
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
        let has_work = floor > 0 && snap.first_event_id()?.is_some_and(|first| first < floor);
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
    /// cursor against this to detect they have fallen off retention. Read
    /// from the oldest row's key, never its bytes (`first_event_id`), so a
    /// corrupt row at the floor fails only a tail that reaches it.
    pub fn event_floor(&self) -> io::Result<u64> {
        let snap = self.snapshot()?;
        match snap.first_event_id()? {
            Some(first) => {
                // Negative space: every stored event id was minted below the
                // counter, and GC never touches the counter — so the floor
                // can never reach, let alone exceed, next_event_id.
                let next = snap.next_event_id()?;
                assert!(
                    first < next,
                    "event_floor: smallest retained event id {first} is not below \
                     next_event_id {next} — an event exists that the counter never minted"
                );
                Ok(first)
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

    /// Registers each mounted job type's payload shape and declared tenancy
    /// (`(name, fingerprint, scoped_by)`, where `scoped_by` is `JobDef`'s
    /// scope name paired with its `scope_type`, `None` for a `global` job)
    /// so `pending_job_shape_refusal` can name a Pending row this binary
    /// would decode under a foreign shape or dispatch into the wrong tenancy,
    /// and `Group::stage` can refuse an enqueue that disagrees with a mounted
    /// name on shape, on tenant presence, or on scope type. `App::mount_jobs`
    /// calls it with the `JobDef`s it mounts. Re-registering the same
    /// declaration is a no-op.
    pub fn register_job_shapes(&mut self, shapes: &[(&'static str, u64, DeclaredTenancy)]) {
        let registry = &mut self.engine.machine_mut().schema.job_shapes;
        for &(name, fingerprint, scoped_by) in shapes {
            let Some((prev_fingerprint, prev_scoped_by)) =
                registry.insert(name, (fingerprint, scoped_by))
            else {
                continue;
            };
            assert!(
                (prev_fingerprint, prev_scoped_by) == (fingerprint, scoped_by),
                "job \"{name}\" registered with two declarations (shape {prev_fingerprint:#x}, \
                 {}; then shape {fingerprint:#x}, {}{}) — a job name has one shape and one \
                 tenancy, and `App::mount_jobs` refuses a duplicate name, so a second \
                 `register_job_shapes` call named it again; register each job name once",
                declared_tenancy(prev_scoped_by.map(|(scope, _)| scope)),
                declared_tenancy(scoped_by.map(|(scope, _)| scope)),
                if prev_fingerprint == fingerprint
                    && prev_scoped_by.map(|(scope, _)| scope) == scoped_by.map(|(scope, _)| scope)
                {
                    ", a different scope type of the same name"
                } else {
                    ""
                }
            );
        }
    }

    /// The check that runs wherever dispatch could start (payload-shapes spec
    /// §4, amendment A1): at kernel start on a writable node, whenever the
    /// kernel's write gate opens (a witness gate opening, a completed
    /// switchover), and before a promote flips the role. `Some(refusal)`
    /// names every Pending job of a registered type whose recorded shape is
    /// not the one this binary compiled — a row with no recorded shape
    /// included — one line per job type; then every Pending job of the
    /// compiled shape whose tenant presence disagrees with the declaration
    /// this binary mounts (job-tenancy final review M4), one line per job
    /// type, since the dispatcher would open no tenant for a scoped job or
    /// one for a global job. Done and Dead rows are never decoded again and
    /// are ignored; a Pending row of an UNREGISTERED name keeps its own path
    /// (dispatch's "not registered" dead-letter).
    pub fn pending_job_shape_refusal(&self) -> io::Result<Option<String>> {
        const LISTED_IDS: usize = 5;
        let registry = &self.engine.machine().schema.job_shapes;
        if registry.is_empty() {
            return Ok(None);
        }
        let listed = |ids: &[u64]| {
            let mut listed: Vec<String> = ids.iter().take(LISTED_IDS).map(u64::to_string).collect();
            if ids.len() > LISTED_IDS {
                listed.push("…".to_string());
            }
            listed.join(", ")
        };
        let mut foreign: BTreeMap<&'static str, (Vec<u64>, BTreeSet<Option<u64>>)> =
            BTreeMap::new();
        let mut mistenanted: BTreeMap<&'static str, Vec<u64>> = BTreeMap::new();
        for job in self.snapshot()?.scan::<JobRow>()? {
            if job.status != crate::job::JobStatus::Pending {
                continue;
            }
            let Some((&name, &(compiled, scoped_by))) = registry.get_key_value(job.name.as_str())
            else {
                continue;
            };
            if job.shape != Some(compiled) {
                let (ids, recorded) = foreign.entry(name).or_default();
                ids.push(job.id.raw());
                recorded.insert(job.shape);
            } else if job.scope.is_some() != scoped_by.is_some() {
                // Presence is all a row can disagree on: it records a tenant
                // id and never its scope type, which only `Group::stage`
                // sees (job-tenancy final review N1).
                mistenanted.entry(name).or_default().push(job.id.raw());
            }
        }
        if foreign.is_empty() && mistenanted.is_empty() {
            return Ok(None);
        }
        let mut lines: Vec<String> = foreign
            .into_iter()
            .map(|(name, (ids, recorded))| {
                let recorded: Vec<String> = recorded
                    .into_iter()
                    .map(|shape| match shape {
                        Some(shape) => format!("{shape:#x}"),
                        None => "none — enqueued before job shapes were tagged".to_string(),
                    })
                    .collect();
                format!(
                    "{} Pending \"{name}\" job(s) (ids {}) were enqueued under a different shape \
                     of {name} (recorded {}; this binary {:#x}) — this binary will not decode \
                     them; run the binary that enqueued them until they are Done or Dead, then \
                     deploy this one, or restore {name}'s old fields and its \
                     `global`/`scoped_by` declaration and give the new shape its own #[job] type \
                     under a new name (GUIDE §6)",
                    ids.len(),
                    listed(&ids),
                    recorded.join(", "),
                    registry[name].0
                )
            })
            .collect();
        lines.extend(mistenanted.into_iter().map(|(name, ids)| {
            let scoped_by = registry[name].1.map(|(scope, _)| scope);
            format!(
                "{} Pending \"{name}\" job(s) (ids {}) carry {}, but this binary declares {name} \
                 {} — the enqueue family writes a tenant exactly for a `scoped_by` job, so \
                 something that contradicts {name}'s #[job] wrote these rows: a hand-written \
                 `impl Job` or `JobDef`, or a raw store write; this binary will not dispatch \
                 them; mount {name} as its #[job] declares it, or restore the store from a \
                 replica or backup taken before these rows were written (GUIDE §6)",
                ids.len(),
                listed(&ids),
                if scoped_by.is_some() {
                    "no tenant"
                } else {
                    "a tenant"
                },
                declared_tenancy(scoped_by)
            )
        }));
        Ok(Some(lines.join("\n")))
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

    /// Whether `table` is tenant-scoped (`Row::Scope != Unscoped`) — the
    /// registry's own word, for callers outside this crate that need it to
    /// pick a physical shape or a wire shape (`scan_raw`/`scan_raw_from`'s
    /// `scoped` parameter; the archive's export walk; seeding).
    pub fn table_scoped(&self, table: &str) -> io::Result<bool> {
        Ok(self
            .engine
            .machine()
            .schema
            .ops(table, "Db::table_scoped")?
            .scoped)
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
                    scoped: ops.scoped,
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
            let rows_exist = !snap
                .scan_raw_from(
                    table,
                    schema.ops(table, "Db::check_schema_gate")?.scoped,
                    None,
                    1,
                )?
                .is_empty();
            match gate_status(
                table,
                (version, fingerprint),
                persisted.as_ref(),
                rows_exist,
            ) {
                GateStatus::Refuse(msg) => return Err(io::Error::other(Refusal(msg))),
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
                        return Err(io::Error::other(Refusal(upgrade_barrier_msg(
                            table,
                            persisted_version,
                            version,
                        ))));
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
            let table_scoped = self
                .engine
                .machine()
                .schema
                .ops(table, "Db::migrate_schema")?
                .scoped;
            let snap = self.snapshot()?;
            let persisted: Option<crate::migration::SchemaVersion> = snap.get(vid)?;
            let rows_exist = !snap.scan_raw_from(table, table_scoped, None, 1)?.is_empty();
            drop(snap);
            let from = match gate_status(
                table,
                (compiled_version, compiled_fingerprint),
                persisted.as_ref(),
                rows_exist,
            ) {
                GateStatus::Current | GateStatus::Adopt => continue,
                // Unpinned: `Db::open`'s own post-replay walk (`check_schema_gate`)
                // calls `gate_status` over the same schema and the same just-replayed
                // state and already returns this same `Refuse` first, so a caller
                // never reaches a writable `Db` (and this method) with one still
                // outstanding. Kept as a fail-closed backstop, not a reachable path.
                GateStatus::Refuse(msg) => return Err(io::Error::other(Refusal(msg))),
                GateStatus::Migrate { from } => from,
            };
            // Chain existence, before touching anything.
            let steps: Vec<crate::migration::MigrationStep> =
                self.engine.machine().schema.migrations_for(table).to_vec();
            let mut cursor = from;
            while cursor != compiled_version {
                let Some(step) = steps.iter().find(|s| s.from_version == cursor) else {
                    return Err(io::Error::other(Refusal(format!(
                        "table \"{table}\" has no registered migration from version {cursor} — \
                         persisted version is {from}, compiled version is {compiled_version}; \
                         register Schema::migration::<Step>(\"{table}\") for the \
                         version-{cursor} step, or this data cannot be opened by this binary"
                    ))));
                };
                cursor = step.to_version;
            }
            // `migrate_table` closes out on its first empty batch, and a batch
            // of 0 rows is always empty, so a size of 0 would bump the version
            // over rows it never rewrote.
            if self.cfg.migration_batch_size == 0 {
                return Err(io::Error::other(Refusal(format!(
                    "table \"{table}\" is due to migrate from persisted version {from} to \
                     compiled version {compiled_version}, but DbConfig::migration_batch_size is \
                     0: a batch of 0 commits no rows, and the migration would then mark the \
                     table migrated with rows still at version {from} — set \
                     migration_batch_size to at least 1 (the default is {})",
                    DbConfig::default().migration_batch_size
                ))));
            }
            self.migrate_table(
                table,
                from,
                compiled_version,
                compiled_fingerprint,
                table_scoped,
                &steps,
            )?;
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
        scoped: bool,
        steps: &[crate::migration::MigrationStep],
    ) -> io::Result<()> {
        let vid = crate::migration::schema_version_id(table);
        let batch_size = self.cfg.migration_batch_size as usize;
        loop {
            let snap = self.snapshot()?;
            let watermark: Option<(u64, u64)> = snap
                .get::<crate::migration::SchemaVersion>(vid)?
                .and_then(|v| v.migrating_through);
            let batch = snap.scan_raw_from(table, scoped, watermark, batch_size)?;
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
            let new_watermark = batch.last().map(|(scope, id, _)| (*scope, *id));
            let mut migrated: Vec<(u64, u64, Vec<u8>)> = Vec::with_capacity(batch.len());
            for (scope, id, bytes) in batch {
                let mut version = from;
                let mut bytes = bytes;
                while version != to {
                    let step = steps
                        .iter()
                        .find(|s| s.from_version == version)
                        .expect("chain verified by migrate_schema");
                    // A step's `MigrationError` (a decode failure or the
                    // migration's own deliberate `Reject`) is deterministic
                    // over the same row's bytes — a restart re-runs the same
                    // step and fails the same way, so this is a Refusal, not
                    // a platform IO failure a retry could clear.
                    bytes = (step.apply)(&bytes).map_err(|e| {
                        io::Error::other(Refusal(format!(
                            "migrating table \"{table}\" row {id}: {e}"
                        )))
                    })?;
                    version = step.to_version;
                }
                migrated.push((scope, id, bytes));
            }
            self.transact(|tx| -> Result<(), io::Error> {
                for (scope, id, bytes) in migrated {
                    tx.migrate_put(table, scoped.then_some(scope), id, bytes);
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
    ///
    /// Before the record exists it also refuses, as `TxError::Io`, what apply
    /// would refuse only after the WAL sync: an enqueue of a mounted job name
    /// under another shape or tenancy (`JobShapeRefusal`), and a write the
    /// `Schema` disagrees with about its table (`TableWriteRefusal`): a name
    /// it does not register, the other scope shape, or a put or delete
    /// through a Rust type that is not the registered one.
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
        // Every table a transaction writes is registered, and every put and
        // delete goes through the `Row` type the `Schema` registered for its
        // name, compared by `TypeId`. Apply treats every write to a table as
        // its registered type's. One it cannot read as that (an unregistered
        // name, the other scope shape at `resolve_effect_scope`, a row that
        // does not decode) it refuses after the WAL sync, where the refusal is
        // durable and poisons every restart. One it can read lands as that,
        // with no error: a row whose bytes happen to decode, or a delete,
        // through another type (under another `scoped_by`, in the registered
        // scope type's tenant of the same id), or an unscoped write beside a
        // scoped one to a scoped table (in tenant 0, since the record carries
        // one tag per table). Refused here, before a record exists, each costs
        // only this transaction. The scope shape is checked
        // before the type because its text says more. `TypeId` sorts last in
        // `written_as`, so it orders only entries whose texts are identical:
        // which text a job's `last_error` records never depends on a value the
        // compiler may change between builds.
        let schema = &self.db.engine.machine().schema;
        for &(table, writer, scoped, row_type) in &effects.written_as {
            let refusal = match schema.tables.get(table) {
                None => format!(
                    "table \"{table}\" is not registered in the Schema, but this transaction \
                     wrote it through `{writer}` — every table a transaction writes must be \
                     registered, or apply cannot decode its rows; the transaction was refused \
                     and nothing was written, so a retry cannot succeed — register it with \
                     .table::<{writer}>() in the Schema passed to Db::open or App::open"
                ),
                Some(ops) if scoped != ops.scoped => {
                    let (registered, written) = if ops.scoped {
                        ("tenant-scoped (Row::Scope != Unscoped)", "unscoped")
                    } else {
                        ("unscoped (Row::Scope = Unscoped)", "tenant-scoped")
                    };
                    format!(
                        "table \"{table}\" is registered {registered} as `{}`, but this \
                         transaction wrote it {written} through `{writer}` — two Row types \
                         declare TABLE = \"{table}\", and a table keeps one scope shape for its \
                         whole life; the transaction was refused and nothing was written, so a \
                         retry cannot succeed — write `{}` instead, or give `{writer}` a table \
                         name of its own (a #[table] type's table name is its struct name)",
                        ops.row_name, ops.row_name
                    )
                }
                Some(ops) if row_type != ops.row_type => format!(
                    "table \"{table}\" is registered as `{}`, but this transaction wrote it \
                     through `{writer}` — two Row types declare TABLE = \"{table}\", and apply \
                     treats every write to a table as its registered type's, so this one would \
                     have been refused after the WAL sync or applied as a `{}` write; the \
                     transaction was refused and nothing was written, so a retry cannot succeed \
                     — write `{}` instead, or give `{writer}` a table name of its own (a \
                     #[table] type's table name is its struct name)",
                    ops.row_name, ops.row_name, ops.row_name
                ),
                Some(_) => continue,
            };
            return Err(TxError::Io(io::Error::other(TableWriteRefusal(Refusal(
                refusal,
            )))));
        }
        // Negative space: `Tx::settle_job`'s `JobRow` is the one overlay write
        // `written_as` does not record, and `Db::open` registers it on every
        // path, so no table reaches the record unchecked.
        assert!(
            effects
                .overlay
                .keys()
                .all(|table| schema.tables.contains_key(table)),
            "Group::stage: a transaction wrote a table the Schema does not register and \
             TxEffects::written_as did not record — every overlay writer but Tx::settle_job \
             records its Row type there"
        );
        // A mounted job name has one shape and one tenancy. `NAME` is the
        // bare type identifier, so a second `#[job]` type of the same name
        // enqueues under a shape no dispatch of this binary can decode;
        // refusing it here, before a record exists, is what makes dispatch's
        // shape assert an invariant (payload-shapes spec §4). A hand-written
        // `impl Job` can also copy a mounted job's `NAME` and `FINGERPRINT`
        // under the other tenancy, and a hand-written `JobDef` can mount one
        // under the wrong one, so tenant presence is refused here too — the
        // half of dispatch's tenancy assert that the write seal and the
        // startup refusal do not cover (job-tenancy final review M4). And the
        // scope TYPE: the record keeps a tenant id and not its type, and the
        // dispatcher opens that id as the type the mounted job declares, so
        // a stand-in enqueued in another scope type's tenant (an `impl Job`
        // copying `NAME` and `FINGERPRINT`, or a `#[job]` whose `scoped_by`
        // path spells another type alike, which the fingerprint cannot tell
        // apart) would run in the same id of the declared type, not in the
        // tenant it was enqueued in. This is the only place both types are
        // known (job-tenancy final review N1).
        let job_shapes = &self.db.engine.machine().schema.job_shapes;
        for Enqueue {
            job: name,
            shape,
            scope,
            ..
        } in &effects.enqueues
        {
            let Some(&(mounted, scoped_by)) = job_shapes.get(*name) else {
                continue;
            };
            if mounted != *shape {
                return Err(TxError::Io(io::Error::other(JobShapeRefusal(Refusal(
                    format!(
                        "job \"{name}\" was enqueued under a different shape of {name} than this \
                     binary mounts (enqueued {shape:#x}; this binary {mounted:#x}) — two #[job] \
                     types share the name {name}; the transaction was refused and nothing was \
                     written, so a retry cannot succeed — enqueue the mounted type, or give the \
                     other type its own name (GUIDE §6)"
                    ),
                )))));
            }
            if scope.is_some() != scoped_by.is_some() {
                return Err(TxError::Io(io::Error::other(JobShapeRefusal(Refusal(
                    format!(
                        "job \"{name}\" was enqueued with {}, but this binary mounts {name} as {} \
                         under the same shape ({shape:#x}) — a hand-written `impl Job` or \
                         `JobDef` contradicts {name}'s #[job]; the transaction was refused and \
                         nothing was written, so a retry cannot succeed — enqueue the mounted \
                         type, give the other type its own name, or mount {name} as its #[job] \
                         declares it (GUIDE §6)",
                        if scope.is_some() {
                            "a tenant"
                        } else {
                            "no tenant"
                        },
                        declared_tenancy(scoped_by.map(|(scope, _)| scope))
                    ),
                )))));
            }
            if let (Some(tenant), Some((declared, declared_type))) = (scope, scoped_by)
                && tenant.scope_type != declared_type
            {
                // The last path segment, as `decode_body` renders a type.
                let enqueued = tenant
                    .scope_name
                    .rsplit("::")
                    .next()
                    .unwrap_or(tenant.scope_name);
                return Err(TxError::Io(io::Error::other(JobShapeRefusal(Refusal(
                    format!(
                        "job \"{name}\" was enqueued in a `{enqueued}` tenant, but this binary \
                         mounts {name} as `scoped_by = {declared}` under the same shape \
                         ({shape:#x}), so it would run in `{declared}` {}, not in the tenant it \
                         was enqueued in — a type standing in for {name}'s #[job] was \
                         enqueued: a hand-written `impl Job`, or a #[job] whose \
                         `scoped_by` spells another scope type the same way; the transaction was \
                         refused and nothing was written, so a retry cannot succeed — enqueue the \
                         mounted {name} from a `{declared}` scope, or give the other type its \
                         own name (GUIDE §6)",
                        tenant.id
                    ),
                )))));
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
                .is_some_and(|m| m.contains_key(&(UNSCOPED, vid.raw())));
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
                .insert((UNSCOPED, vid.raw()), Some(bytes));
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
            for (key, entry) in entries {
                group_table.insert(key, entry);
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

    /// Whether `table` is tenant-scoped, per the schema this `Seeder` will
    /// import through — the archive restore path's shape discriminator for
    /// a snapshot batch object, decided the same way `Db::table_scoped`
    /// decides it for export, before any row is read.
    pub fn table_scoped(&self, table: &str) -> io::Result<bool> {
        Ok(self
            .import_schema
            .ops(table, "Seeder::table_scoped")?
            .scoped)
    }

    /// One table's worth of exported rows, applied through the schema's
    /// typed ops — indexes rebuilt exactly as the live apply path maintains
    /// them. The id inside each row's own encoded bytes is what `put`
    /// re-derives the key from; the tuple's `u64` id travels for
    /// debuggability and future delta use, not because `apply_put` needs
    /// it. The scope DOES travel because `apply_put` needs it (tenant-
    /// scoping Task 6b widened its signature): every row's own scope, as
    /// `scan_raw`/`scan_raw_from` read it back off the store, threads
    /// straight through to the typed put — `Writer::put`'s own
    /// `row_is_scoped::<R>()` assert (not this method) is the negative-space
    /// check that a mismatched scope/table pairing cannot silently apply.
    pub fn rows(&mut self, table: &str, rows: &[(u64, u64, Vec<u8>)]) -> io::Result<()> {
        self.store.import(|w| {
            for (scope, id, bytes) in rows {
                let _ = id;
                let ops = self.import_schema.ops(table, "Seeder::rows")?;
                (ops.apply_put)(w, *scope, bytes)?;
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

    /// An `io::Error` carrying a refusal prints, under `Debug`, exactly what
    /// one carrying the bare text does — `main`'s `Error: …` line for a
    /// binary that returns one (GUIDE §10's `run_backup`) shows the text, not
    /// the wrapper type the kernel's stop line tells refusals apart with.
    #[test]
    fn a_refusal_debugs_as_its_bare_text() {
        let text = "the refusal names \"its\" own fix";
        let bare = format!("{:?}", io::Error::other(text.to_string()));
        assert_eq!(
            format!("{:?}", io::Error::other(Refusal(text.to_string()))),
            bare
        );
        assert_eq!(
            format!(
                "{:?}",
                io::Error::other(JobShapeRefusal(Refusal(text.to_string())))
            ),
            bare
        );
    }

    fn effects_fixture() -> TxEffects {
        let mut effects = TxEffects::default();
        let invoices = effects.overlay.entry("invoice").or_default();
        invoices.insert((UNSCOPED, 10), Some(vec![1, 2, 3]));
        invoices.insert((UNSCOPED, 11), None);
        effects
            .overlay
            .entry("org")
            .or_default()
            .insert((UNSCOPED, 1), Some(vec![9]));
        effects.enqueues.push(Enqueue {
            job: "send_receipt",
            payload: vec![10, 0],
            key: None,
            shape: 0x5E4D,
            scope: None,
        });
        effects
            .emits
            .push(("invoice_finalized", vec![7], None, 0xE7E7));
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
                    scope: None,
                    row: vec![1, 2, 3]
                },
                Effect::Delete {
                    table: "invoice".into(),
                    scope: None,
                    id: 11
                },
                Effect::Put {
                    table: "org".into(),
                    scope: None,
                    row: vec![9]
                },
                Effect::Enqueue {
                    job: "send_receipt".into(),
                    payload: vec![10, 0],
                    key: None,
                    shape: Some(0x5E4D),
                    scope: None
                },
                Effect::Emit {
                    event: "invoice_finalized".into(),
                    payload: vec![7],
                    key: None,
                    shape: Some(0xE7E7)
                },
            ]
        );
    }

    /// Task 6b: an unscoped table's Put/Delete tags (`0x00`/`0x01`) must stay
    /// byte-identical to what this repo committed before the scoped tags
    /// (`0x05`/`0x06`) existed — every pinned simulation seed depends on it.
    /// Pinned against the literal COMMITTED bytes (captured from `HEAD`
    /// before this task touched `encode_record`), not a fresh encode of the
    /// same value on both sides of the comparison, which would pass even if
    /// both moved together.
    #[test]
    fn unscoped_put_and_delete_bytes_are_pinned_to_the_committed_v0_shape() {
        let mut put_effects = TxEffects::default();
        put_effects
            .overlay
            .entry("invoice")
            .or_default()
            .insert((UNSCOPED, 10), Some(vec![1, 2, 3]));
        assert_eq!(
            encode_record(&put_effects),
            vec![
                1, 0, 0, 0, 0, 7, 0, 0, 0, 105, 110, 118, 111, 105, 99, 101, 3, 0, 0, 0, 1, 2, 3
            ],
            "an unscoped Put's encoded bytes must not move"
        );

        let mut delete_effects = TxEffects::default();
        delete_effects
            .overlay
            .entry("invoice")
            .or_default()
            .insert((UNSCOPED, 11), None);
        assert_eq!(
            encode_record(&delete_effects),
            vec![
                1, 0, 0, 0, 1, 7, 0, 0, 0, 105, 110, 118, 111, 105, 99, 101, 11, 0, 0, 0, 0, 0, 0,
                0
            ],
            "an unscoped Delete's encoded bytes must not move"
        );
    }

    #[test]
    fn keyed_emit_roundtrips() {
        let mut effects = TxEffects::default();
        effects.emits.push((
            "invoice_finalized",
            vec![1],
            Some("org-7".to_string()),
            0xE7E7,
        ));
        effects.emits.push(("ping", vec![2], None, 0x9196));
        let decoded = decode_record(&encode_record(&effects)).unwrap();
        assert_eq!(
            decoded,
            vec![
                Effect::Emit {
                    event: "invoice_finalized".into(),
                    payload: vec![1],
                    key: Some("org-7".into()),
                    shape: Some(0xE7E7)
                },
                Effect::Emit {
                    event: "ping".into(),
                    payload: vec![2],
                    key: None,
                    shape: Some(0x9196)
                },
            ]
        );
    }

    #[test]
    fn keyed_enqueue_roundtrips() {
        let mut effects = TxEffects::default();
        effects.enqueues.push(Enqueue {
            job: "send_receipt",
            payload: vec![1],
            key: Some("receipt:10".to_string()),
            shape: 0x5E4D,
            scope: None,
        });
        effects.enqueues.push(Enqueue {
            job: "ping",
            payload: vec![2],
            key: None,
            shape: 0x9196,
            scope: None,
        });
        let decoded = decode_record(&encode_record(&effects)).unwrap();
        assert_eq!(
            decoded,
            vec![
                Effect::Enqueue {
                    job: "send_receipt".into(),
                    payload: vec![1],
                    key: Some("receipt:10".into()),
                    shape: Some(0x5E4D),
                    scope: None
                },
                Effect::Enqueue {
                    job: "ping".into(),
                    payload: vec![2],
                    key: None,
                    shape: Some(0x9196),
                    scope: None
                },
            ]
        );
    }

    /// A record written before payload shapes (tags `0x02`/`0x03`) still
    /// decodes, its shape unknown (payload-shapes spec §2). Literal bytes, not
    /// a fresh encode: no binary writes these tags any more, so encoding on
    /// both sides would prove nothing.
    #[test]
    fn a_pre_shape_enqueue_and_emit_decode_with_no_shape() {
        let bytes = vec![
            2, 0, 0, 0, // two effects
            // enqueue "ping" [7], no key
            2, 4, 0, 0, 0, b'p', b'i', b'n', b'g', 1, 0, 0, 0, 7, 0,
            // emit "pong" [9], no key
            3, 4, 0, 0, 0, b'p', b'o', b'n', b'g', 1, 0, 0, 0, 9, 0,
        ];
        assert_eq!(
            decode_record(&bytes).unwrap(),
            vec![
                Effect::Enqueue {
                    job: "ping".into(),
                    payload: vec![7],
                    key: None,
                    shape: None,
                    scope: None
                },
                Effect::Emit {
                    event: "pong".into(),
                    payload: vec![9],
                    key: None,
                    shape: None
                },
            ]
        );
    }

    /// The shaped tags, pinned as literal bytes: `0x08`/`0x09`, then the
    /// shape as eight little-endian bytes after the key.
    #[test]
    fn shaped_enqueue_and_emit_bytes_are_pinned() {
        let mut effects = TxEffects::default();
        effects.enqueues.push(Enqueue {
            job: "ping",
            payload: vec![7],
            key: None,
            shape: 0x0102_0304_0506_0708,
            scope: None,
        });
        effects
            .emits
            .push(("pong", vec![9], None, 0x1112_1314_1516_1718));
        assert_eq!(
            encode_record(&effects),
            vec![
                2, 0, 0, 0, // two effects
                8, 4, 0, 0, 0, b'p', b'i', b'n', b'g', 1, 0, 0, 0, 7, 0, // shaped enqueue
                8, 7, 6, 5, 4, 3, 2, 1, // its shape
                9, 4, 0, 0, 0, b'p', b'o', b'n', b'g', 1, 0, 0, 0, 9, 0, // shaped emit
                0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, // its shape
            ]
        );
        assert_eq!(
            decode_record(&encode_record(&effects)).unwrap(),
            vec![
                Effect::Enqueue {
                    job: "ping".into(),
                    payload: vec![7],
                    key: None,
                    shape: Some(0x0102_0304_0506_0708),
                    scope: None
                },
                Effect::Emit {
                    event: "pong".into(),
                    payload: vec![9],
                    key: None,
                    shape: Some(0x1112_1314_1516_1718)
                },
            ]
        );
    }

    /// Job-tenancy spec §6 and Review Focus 2: the scoped enqueue, pinned as
    /// literal bytes. It is the `0x08` layout under tag `0x0a`, plus the
    /// tenant as eight little-endian bytes, at tenant 0, because presence
    /// and not value selects the tag. An unscoped enqueue in the same record
    /// is still `0x08`, byte for byte, with nothing after its shape.
    #[test]
    fn scoped_enqueue_bytes_are_pinned_with_tenant_zero() {
        let mut effects = TxEffects::default();
        effects.enqueues.push(Enqueue {
            job: "ping",
            payload: vec![7],
            key: None,
            shape: 0x0102_0304_0506_0708,
            // The scope type is in memory only: the bytes below carry the
            // tenant id and nothing about its type (final review N1).
            scope: Some(EnqueuedTenant {
                id: 0,
                scope_type: TypeId::of::<u8>(),
                scope_name: "u8",
            }),
        });
        effects.enqueues.push(Enqueue {
            job: "ping",
            payload: vec![9],
            key: None,
            shape: 0x0102_0304_0506_0708,
            scope: None,
        });
        assert_eq!(
            encode_record(&effects),
            vec![
                2, 0, 0, 0, // two effects
                10, 4, 0, 0, 0, b'p', b'i', b'n', b'g', 1, 0, 0, 0, 7, 0, // scoped enqueue
                8, 7, 6, 5, 4, 3, 2, 1, // its shape
                0, 0, 0, 0, 0, 0, 0, 0, // its tenant: 0, a real tenant
                8, 4, 0, 0, 0, b'p', b'i', b'n', b'g', 1, 0, 0, 0, 9, 0, // unscoped enqueue
                8, 7, 6, 5, 4, 3, 2, 1, // its shape, and nothing after it
            ]
        );
        assert_eq!(
            decode_record(&encode_record(&effects)).unwrap(),
            vec![
                Effect::Enqueue {
                    job: "ping".into(),
                    payload: vec![7],
                    key: None,
                    shape: Some(0x0102_0304_0506_0708),
                    scope: Some(0),
                },
                Effect::Enqueue {
                    job: "ping".into(),
                    payload: vec![9],
                    key: None,
                    shape: Some(0x0102_0304_0506_0708),
                    scope: None,
                },
            ]
        );
    }

    #[test]
    fn migrate_put_roundtrips_through_the_record_format() {
        let mut effects = TxEffects::default();
        effects
            .migrate_puts
            .push(("Widget", None, 7, vec![1, 2, 3]));
        let decoded = decode_record(&encode_record(&effects)).unwrap();
        assert_eq!(
            decoded,
            vec![Effect::MigratePut {
                table: "Widget".to_string(),
                scope: None,
                id: 7,
                row: vec![1, 2, 3],
            }]
        );
    }

    /// Task 6c, brief Step 4: the unscoped `MigratePut` record must not move
    /// byte-for-byte — pinned against COMMITTED LITERAL bytes (this array),
    /// not a fresh `encode_record` output, which would agree with itself no
    /// matter what the encoder does. Same frozen shape
    /// `charpente-core/tests/migrate_apply.rs`'s `migrate_put_record` helper
    /// hand-encodes and drives through `Db::apply_shipped`: `[n=1: u32
    /// le][tag=4][table: u32 len + bytes][id: u64 le][row: u32 len +
    /// bytes]`.
    #[test]
    fn tag_4_migrate_put_wire_bytes_are_unchanged_by_task_6c() {
        let mut effects = TxEffects::default();
        effects
            .migrate_puts
            .push(("Widget", None, 7, vec![1, 2, 3]));
        let encoded = encode_record(&effects);
        let expected: &[u8] = &[
            1, 0, 0, 0, // n = 1
            4, // tag
            6, 0, 0, 0, b'W', b'i', b'd', b'g', b'e', b't', // table
            7, 0, 0, 0, 0, 0, 0, 0, // id = 7
            3, 0, 0, 0, 1, 2, 3, // row
        ];
        assert_eq!(encoded, expected);
    }

    /// Task 6c: the NEW scoped `MigratePut` tag, `0x07` — a new tag rather
    /// than a widened `0x04` (6b's precedent), so an older binary refuses it
    /// by name. Layout: `[table][scope: u64 le][id: u64 le][row]`.
    #[test]
    fn tag_7_scoped_migrate_put_roundtrips_and_has_the_documented_wire_shape() {
        let mut effects = TxEffects::default();
        effects
            .migrate_puts
            .push(("Ticket", Some(9), 7, vec![1, 2, 3]));
        let encoded = encode_record(&effects);
        let expected: &[u8] = &[
            1, 0, 0, 0, // n = 1
            7, // tag
            6, 0, 0, 0, b'T', b'i', b'c', b'k', b'e', b't', // table
            9, 0, 0, 0, 0, 0, 0, 0, // scope = 9
            7, 0, 0, 0, 0, 0, 0, 0, // id = 7
            3, 0, 0, 0, 1, 2, 3, // row
        ];
        assert_eq!(encoded, expected);
        assert_eq!(
            decode_record(&encoded).unwrap(),
            vec![Effect::MigratePut {
                table: "Ticket".to_string(),
                scope: Some(9),
                id: 7,
                row: vec![1, 2, 3],
            }]
        );
    }

    /// Task 6b: a table `Tx::put`/`Tx::delete` marked in `scoped_tables`
    /// encodes `0x05`/`0x06` and carries the overlay's own scope, including
    /// scope 0 (R35 — tenant 0 is a legal tenant, so a scoped table's
    /// records use the scoped tags even at scope 0, never the unscoped
    /// `0x00`/`0x01` shape `record_roundtrip_is_canonical` pins).
    #[test]
    fn a_scoped_tables_put_and_delete_carry_the_scope_field() {
        let mut effects = TxEffects::default();
        effects.scoped_tables.insert("ticket");
        effects
            .overlay
            .entry("ticket")
            .or_default()
            .insert((0, 5), Some(vec![9, 9]));
        effects
            .overlay
            .entry("ticket")
            .or_default()
            .insert((7, 6), None);
        let decoded = decode_record(&encode_record(&effects)).unwrap();
        assert_eq!(
            decoded,
            vec![
                Effect::Put {
                    table: "ticket".into(),
                    scope: Some(0),
                    row: vec![9, 9],
                },
                Effect::Delete {
                    table: "ticket".into(),
                    scope: Some(7),
                    id: 6,
                },
            ]
        );
    }

    #[test]
    fn resolve_effect_scope_accepts_matching_tag_and_table() {
        assert_eq!(
            resolve_effect_scope("t", false, None, "Put").unwrap(),
            crate::table::UNSCOPED
        );
        assert_eq!(resolve_effect_scope("t", true, Some(3), "Put").unwrap(), 3);
        assert_eq!(resolve_effect_scope("t", true, Some(0), "Put").unwrap(), 0);
    }

    #[test]
    fn resolve_effect_scope_refuses_a_scoped_table_with_an_unscoped_tag() {
        let err = resolve_effect_scope("ticket", true, None, "Put").unwrap_err();
        assert!(
            err.to_string()
                .contains("table \"ticket\" is tenant-scoped (Row::Scope != Unscoped)"),
            "{err}"
        );
        assert!(err.to_string().contains("carries no scope"), "{err}");
        assert!(
            err.to_string().contains(
                "restore from a replica or backup, or replay under the schema that \
                            wrote it"
            ),
            "{err}"
        );
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
        assert!(Refusal::of(&err).is_some(), "{err}");
    }

    #[test]
    fn resolve_effect_scope_refuses_an_unscoped_table_with_a_scoped_tag() {
        let err = resolve_effect_scope("widget", false, Some(3), "Delete").unwrap_err();
        assert!(
            err.to_string()
                .contains("table \"widget\" is not tenant-scoped (Row::Scope = Unscoped)"),
            "{err}"
        );
        assert!(err.to_string().contains("carries scope 3"), "{err}");
        assert!(
            err.to_string().contains(
                "restore from a replica or backup, or replay under the schema that \
                            wrote it"
            ),
            "{err}"
        );
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
        assert!(Refusal::of(&err).is_some(), "{err}");
    }

    /// `0x0b`, the next free tag: `0x0a` is the scoped enqueue.
    #[test]
    fn decode_refuses_unknown_tags() {
        let mut out = Vec::new();
        1u32.encode(&mut out);
        out.push(11);
        let err = decode_record(&out).unwrap_err();
        assert!(err.to_string().contains("unknown effect tag 11"), "{err}");
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
            type Scope = crate::table::Unscoped;
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
                type Scope = crate::table::Unscoped;
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
