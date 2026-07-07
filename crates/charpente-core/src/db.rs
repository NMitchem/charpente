//! Typed transactions over the log (DESIGN.md §2, PLAN.md M0 step 4): a
//! transaction is `fn(&mut Tx) -> Result<T, E>` — reads come from a snapshot
//! plus the transaction's own writes; effects buffer and serialize as ONE
//! log record; the store applies them only after the record is durable.
//! Serializable by construction: `transact` takes `&mut Db`, so two
//! transactions cannot exist at once.
//!
//! Record payload v0: `[n: u32 le]` then n tagged effects —
//! `0x00 [table][row]` put, `0x01 [table][id: u64 le]` delete,
//! `0x02 [job][payload][key: Option<String>]` enqueue, `0x03 [event][payload]`
//! emit — strings and byte fields length-prefixed per the `Codec` layout.
//! Table effects are the transaction's collapsed overlay in (table, id)
//! order: intermediate overwrites inside a transaction are invisible to
//! every observer and are not logged. Enqueues and emits keep call order.
//! The enqueue key is the job's idempotency identity to the world outside
//! the log — carried, never interpreted inside the deterministic boundary.

use std::collections::BTreeMap;
use std::io;

use crate::engine::{Engine, StateMachine};
use crate::io::Disk;
use crate::job::JobRow;
use crate::log::{MAX_RECORD_BYTES, WalConfig};
use crate::table::{Codec, Id, Row, Snapshot, Store, View, Writer};

// ---------------------------------------------------------------- config

#[derive(Clone, Debug)]
pub struct DbConfig {
    pub wal: WalConfig,
    /// The store's file name inside the `Disk` namespace.
    pub store_path: String,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        DbConfig {
            wal: WalConfig::default(),
            store_path: "tables.redb".to_string(),
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

/// A realtime event (DESIGN.md §6). Emits live in the log record itself —
/// subscribers tail the log; nothing is materialized in the store.
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
}

// ---------------------------------------------------------------- schema

struct TableOps {
    apply_put: fn(&mut Writer<'_>, &[u8]) -> io::Result<()>,
    apply_delete: fn(&mut Writer<'_>, u64) -> io::Result<()>,
}

fn apply_put<R: Row>(w: &mut Writer<'_>, row: &[u8]) -> io::Result<()> {
    w.put(&R::decode_row(row)?)
}

fn apply_delete<R: Row>(w: &mut Writer<'_>, id: u64) -> io::Result<()> {
    w.delete::<R>(Id::from_raw(id))
}

/// The set of tables a `Db` can replay log records into. Every `#[table]`
/// type the application writes must be registered — replay works on raw
/// record bytes and needs the typed codecs to maintain indexes.
#[derive(Default)]
pub struct Schema {
    tables: BTreeMap<&'static str, TableOps>,
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
            },
        );
        assert!(
            prev.is_none(),
            "Schema: table \"{}\" registered twice — register each #[table] type once",
            R::TABLE
        );
        self
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
    },
}

/// A transaction's buffered effects: the collapsed per-table overlay
/// (`None` = tombstone) plus enqueues and emits in call order.
#[derive(Default)]
struct TxEffects {
    overlay: BTreeMap<&'static str, BTreeMap<u64, Option<Vec<u8>>>>,
    enqueues: Vec<(&'static str, Vec<u8>, Option<String>)>,
    emits: Vec<(&'static str, Vec<u8>)>,
}

impl TxEffects {
    fn is_empty(&self) -> bool {
        self.overlay.values().all(BTreeMap::is_empty)
            && self.enqueues.is_empty()
            && self.emits.is_empty()
    }
}

fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    (u32::try_from(bytes.len()).expect("field exceeds u32 length")).encode(out);
    out.extend_from_slice(bytes);
}

fn encode_record(effects: &TxEffects) -> Vec<u8> {
    let n = effects.overlay.values().map(BTreeMap::len).sum::<usize>()
        + effects.enqueues.len()
        + effects.emits.len();
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
    for (event, payload) in &effects.emits {
        out.push(3);
        encode_bytes(event.as_bytes(), &mut out);
        encode_bytes(payload, &mut out);
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
        self.effects.emits.push((Ev::NAME, payload));
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
}

impl StateMachine for TableMachine {
    fn apply(&mut self, seq: u64, payload: &[u8]) -> io::Result<()> {
        if self.skip_through.is_some_and(|s| seq <= s) {
            return Ok(());
        }
        let effects = decode_record(payload)?;
        let schema = &self.schema;
        // `w.materialize_job` returns the new id; collected locally because
        // the closure can't also hold `&mut self.enqueued` while `schema`
        // (borrowed from `self`) is captured — extend after the store commits.
        let new_ids = self.store.apply(seq, |w| {
            let mut ids = Vec::new();
            for effect in &effects {
                match effect {
                    Effect::Put { table, row } => (schema.ops(table)?.apply_put)(w, row)?,
                    Effect::Delete { table, id } => (schema.ops(table)?.apply_delete)(w, *id)?,
                    Effect::Enqueue { job, payload, key } => {
                        ids.push(w.materialize_job(job, payload, key.as_deref())?);
                    }
                    Effect::Emit { .. } => {} // the log itself is the event feed
                }
            }
            Ok(ids)
        })?;
        self.enqueued.extend(new_ids);
        Ok(())
    }
}

// ---------------------------------------------------------------- db

/// The composed deterministic core: WAL + store + schema. One `Db` is the
/// single writer; `transact` takes `&mut self`, so transactions are
/// serializable because two cannot exist at once.
// `Debug` (not in the brief's listing, but required: `Db::open(..).unwrap_err()`
// bounds the `Ok` type on `Debug`) delegates to `Engine`'s own restricted impl.
#[derive(Debug)]
pub struct Db {
    engine: Engine<TableMachine>,
}

impl Db {
    /// Opens the store, then recovers the WAL, replaying every durable
    /// record past the store's checkpoint into it. Recovery can fail under
    /// IO faults; it is idempotent — restart and retry. A store that cannot
    /// open at all is discarded and rebuilt from the WAL — the store is a
    /// materialization of the log, never a second source of truth (DESIGN §4).
    pub fn open(disk: Box<dyn Disk>, cfg: DbConfig, schema: Schema) -> io::Result<Db> {
        // The jobs table is platform-owned; an app registering it itself
        // hits the existing duplicate-registration assert.
        let schema = schema.table::<JobRow>();
        let store = match Store::open(&*disk, &cfg.store_path) {
            Ok(store) => store,
            Err(_) => {
                // A store that cannot open is discarded and rebuilt from the
                // WAL — it is a materialization of the log, never a second
                // source of truth (DESIGN §4). E.g. a crash can tear the
                // store's creation, leaving durable bytes redb rejects on
                // every later open (found by seed 0x1f733f8d9eebf822).
                // Safe only because the WAL is never compacted and retains
                // seq 0; WAL segment GC must revisit this rebuild.
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
        let machine = TableMachine {
            store,
            schema,
            skip_through,
            enqueued: Vec::new(),
        };
        let mut engine = Engine::recover(disk, cfg.wal, machine)?;
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
        Ok(Db { engine })
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
        let record = encode_record(&tx.effects);
        assert!(
            record.len() <= MAX_RECORD_BYTES,
            "transaction effects are {} bytes but one log record holds at most \
             {MAX_RECORD_BYTES} — split the work into smaller transactions",
            record.len()
        );
        self.engine.submit(&record).map_err(TxError::Io)?;
        Ok(out)
    }

    /// A consistent read-only view for queries outside transactions.
    pub fn snapshot(&self) -> io::Result<Snapshot> {
        self.engine.machine().store.snapshot()
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

    /// Durably persists the store, bounding recovery replay at this seq.
    pub fn checkpoint(&self) -> io::Result<()> {
        self.engine.machine().store.checkpoint()
    }

    pub fn last_applied(&self) -> Option<u64> {
        self.engine.last_applied()
    }

    /// True once the engine or its WAL is poisoned — every further transact
    /// fails. Callers (the kernel loop) stop and restart to recover.
    pub fn poisoned(&self) -> bool {
        self.engine.poisoned()
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
        effects.emits.push(("invoice_finalized", vec![7]));
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
                    payload: vec![7]
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
}
