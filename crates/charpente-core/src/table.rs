//! Typed tables on redb, mounted behind the `Disk` boundary (DESIGN.md §3–§4).
//!
//! The store is a *materialization* of the log, not a second source of truth:
//! every `apply` records the log seq it corresponds to (gapless, asserted) and
//! commits with `Durability::None` — cheap, non-fsynced. A `checkpoint` makes
//! everything durable at once; after a crash the store reverts to its last
//! checkpoint and recovery replays the WAL from `applied_seq + 1`. Reads are
//! MVCC snapshots (redb read transactions): consistent, non-blocking.
//!
//! Index entries are `[key len: u32 le][key bytes][id: u64 be]` — the length
//! prefix makes exact-match lookups unambiguous (no prefix collisions).
//! Range scans over indexes arrive when a real query pattern demands an
//! order-preserving key encoding.

use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use redb::{
    Builder, Database, Durability, ReadableDatabase, ReadableTable, StorageBackend, TableDefinition,
};

use crate::io::{Disk, File};

// ---------------------------------------------------------------- Id

/// A typed row id: an `Id<Invoice>` is not an `Id<Org>`, and neither is a
/// bare u64 (DESIGN.md §3).
pub struct Id<T> {
    raw: u64,
    _table: PhantomData<fn() -> T>,
}

impl<T> Id<T> {
    pub fn from_raw(raw: u64) -> Id<T> {
        Id {
            raw,
            _table: PhantomData,
        }
    }

    pub fn raw(self) -> u64 {
        self.raw
    }
}

// Manual impls: derives would wrongly bound on `T`.
impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Id<T> {}
impl<T> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}
impl<T> Eq for Id<T> {}
impl<T> PartialOrd for Id<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for Id<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.raw.cmp(&other.raw)
    }
}
impl<T> std::hash::Hash for Id<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.raw.hash(state);
    }
}
impl<T> fmt::Debug for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let full = std::any::type_name::<T>();
        let short = full.rsplit("::").next().unwrap_or(full);
        write!(f, "Id<{short}>({})", self.raw)
    }
}

// ---------------------------------------------------------------- Codec

/// The row/field codec: deterministic, versionless v0. Fixed-width
/// little-endian integers, length-prefixed bytes. Owned here rather than a
/// serde dependency so the byte layout can never drift under the log.
pub trait Codec: Sized {
    fn encode(&self, out: &mut Vec<u8>);
    fn decode(input: &mut &[u8]) -> io::Result<Self>;
}

fn take<'a>(input: &mut &'a [u8], n: usize, what: &str) -> io::Result<&'a [u8]> {
    if input.len() < n {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "row decode: needed {n} bytes for {what}, {} left — schema drift or corruption",
                input.len()
            ),
        ));
    }
    let (head, tail) = input.split_at(n);
    *input = tail;
    Ok(head)
}

macro_rules! int_codec {
    ($($ty:ty),*) => {$(
        impl Codec for $ty {
            fn encode(&self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_le_bytes());
            }
            fn decode(input: &mut &[u8]) -> io::Result<Self> {
                let bytes = take(input, size_of::<$ty>(), stringify!($ty))?;
                Ok(<$ty>::from_le_bytes(bytes.try_into().expect("exact slice")))
            }
        }
    )*};
}
int_codec!(u8, u16, u32, u64, i8, i16, i32, i64);

impl Codec for bool {
    fn encode(&self, out: &mut Vec<u8>) {
        out.push(*self as u8);
    }
    fn decode(input: &mut &[u8]) -> io::Result<bool> {
        match take(input, 1, "bool")?[0] {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("row decode: bool byte was {other}, must be 0 or 1"),
            )),
        }
    }
}

impl Codec for Vec<u8> {
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(
            &(u32::try_from(self.len()).expect("field exceeds u32 length")).to_le_bytes(),
        );
        out.extend_from_slice(self);
    }
    fn decode(input: &mut &[u8]) -> io::Result<Vec<u8>> {
        let len = u32::decode(input)? as usize;
        Ok(take(input, len, "bytes")?.to_vec())
    }
}

impl Codec for String {
    fn encode(&self, out: &mut Vec<u8>) {
        self.as_bytes().to_vec().encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<String> {
        String::from_utf8(Vec::<u8>::decode(input)?).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("row decode: string is not UTF-8: {e}"),
            )
        })
    }
}

impl<T: Codec> Codec for Option<T> {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            None => out.push(0),
            Some(v) => {
                out.push(1);
                v.encode(out);
            }
        }
    }
    fn decode(input: &mut &[u8]) -> io::Result<Option<T>> {
        match take(input, 1, "option tag")?[0] {
            0 => Ok(None),
            1 => Ok(Some(T::decode(input)?)),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("row decode: option tag was {other}, must be 0 or 1"),
            )),
        }
    }
}

impl<T> Codec for Id<T> {
    fn encode(&self, out: &mut Vec<u8>) {
        self.raw.encode(out);
    }
    fn decode(input: &mut &[u8]) -> io::Result<Id<T>> {
        Ok(Id::from_raw(u64::decode(input)?))
    }
}

// ---------------------------------------------------------------- Row

pub struct IndexDef<R> {
    pub name: &'static str,
    pub key: fn(&R, &mut Vec<u8>),
}

/// A row's codec only — no table identity, no indexes. Implemented by
/// `#[table]` (alongside `Row`) and by `#[row_shape]` (a superseded
/// version kept in code purely so the migration chain can decode it —
/// DESIGN §3). `Row: RowShape` is deliberately NOT a supertrait bound:
/// `Row`'s codec methods predate this trait; macro-generated types
/// implement both, delegating one to the other.
pub trait RowShape: Sized {
    const VERSION: u32;
    fn encode_row(&self, out: &mut Vec<u8>);
    fn decode_row(bytes: &[u8]) -> io::Result<Self>;
}

/// A table row. Implemented by `#[table]`, not by hand.
pub trait Row: Sized + 'static {
    const TABLE: &'static str;
    const INDEXES: &'static [IndexDef<Self>];
    /// The row's schema version (`#[table(version = N)]`; `1` if omitted).
    const VERSION: u32 = 1;
    /// Structural fingerprint of the field shape, `Some` only for
    /// macro-generated (`#[table]`) types. `None` (the default) opts the
    /// table out of the versioning gate and the replication digest —
    /// hand-written platform impls are unaffected by this sub-project.
    const FINGERPRINT: Option<u64> = None;
    fn id(&self) -> Id<Self>;
    fn encode_row(&self, out: &mut Vec<u8>);
    fn decode_row(bytes: &[u8]) -> io::Result<Self>;
}

// ---------------------------------------------------------------- View

/// Read access to table state — implemented by `Snapshot` (reads outside
/// transactions) and `Tx` (reads inside one, which also see the
/// transaction's own writes). The `#[table]` accessors are generated
/// against this trait.
pub trait View {
    fn get<R: Row>(&self, id: Id<R>) -> io::Result<Option<R>>;

    /// Every row, in id order. A `Vec` for now; iterators when a real
    /// workload shows the allocation matters.
    fn scan<R: Row>(&self) -> io::Result<Vec<R>>;

    /// Exact-match secondary index lookup, in id order. Called through the
    /// typed accessors `#[table]` generates (`Invoice::by_org(...)`).
    fn scan_by<R: Row>(&self, index_name: &str, key: &impl Codec) -> io::Result<Vec<R>>;

    /// Rows with `id >= from_id`, in id order, at most `limit` — the
    /// realtime cursor read (`charpente.events`, but usable on any table). A
    /// `Vec` for now; see `scan`'s note.
    fn scan_from<R: Row>(&self, from_id: u64, limit: usize) -> io::Result<Vec<R>>;
}

// ---------------------------------------------------------------- Store

/// redb mounted behind the `Disk` trait. This is the only sanctioned way to
/// open the store; opening redb on a raw path would bypass the simulation
/// boundary (enforced by the clippy deny-list).
pub struct Store {
    db: Database,
    /// Shared with this store's `DiskBackend` — see its `latch` field.
    latch: Arc<AtomicBool>,
    /// Rolling digest of this handle's write-transaction schedule — see
    /// `write_schedule`.
    schedule: AtomicU64,
}

const META: TableDefinition<&str, u64> = TableDefinition::new("charpente.meta");

fn rows_def(table: &str) -> TableDefinition<'_, u64, &'static [u8]> {
    TableDefinition::new(table)
}

fn index_table_name<R: Row>(index: &IndexDef<R>) -> String {
    format!("{}.{}", R::TABLE, index.name)
}

fn index_entry(mut key: Vec<u8>, id: u64) -> Vec<u8> {
    let mut entry = (u32::try_from(key.len()).expect("index key exceeds u32 length"))
        .to_le_bytes()
        .to_vec();
    entry.append(&mut key);
    entry.extend_from_slice(&id.to_be_bytes());
    entry
}

fn redb_err(e: impl fmt::Display) -> io::Error {
    io::Error::other(format!("table store (redb): {e}"))
}

/// FNV-1a offset basis; `Store::schedule` starts here so an untouched handle
/// has a stable, nonzero digest.
const SCHEDULE_SEED: u64 = 0xcbf2_9ce4_8422_2325;

/// The `schedule` marker for a checkpoint, kept distinct from any batch size
/// so "applied 40 records, then checkpointed" cannot collide with a 40-record
/// batch that did not.
const SCHEDULE_CHECKPOINT: u64 = u64::MAX;

/// One record's effects inside an already-open write transaction: the
/// negative-space counter checks and the `applied_seq` bump. Free rather than
/// a method because it touches no `Store` state — which is exactly what lets a
/// batch hold one transaction across many calls (`apply_batch`).
fn apply_one_in_txn<T>(
    txn: &redb::WriteTransaction,
    seq: u64,
    f: impl FnOnce(&mut Writer<'_>) -> io::Result<T>,
) -> io::Result<T> {
    let (next_job_id_before, next_event_id_before) = {
        let meta = txn.open_table(META).map_err(redb_err)?;
        (
            meta.get("next_job_id")
                .map_err(redb_err)?
                .map_or(0, |g| g.value()),
            meta.get("next_event_id")
                .map_err(redb_err)?
                .map_or(0, |g| g.value()),
        )
    };
    let mut writer = Writer {
        txn,
        jobs_materialized: 0,
        events_materialized: 0,
    };
    let out = f(&mut writer)?;
    let mut meta = txn.open_table(META).map_err(redb_err)?;
    // Negative-space: next_job_id is a monotonic counter bumped only
    // by materialize_job. It must never move except by exactly one
    // per job materialized in this apply — never backwards, never
    // skipping, never double-counting a single effect.
    let next_job_id_after = meta
        .get("next_job_id")
        .map_err(redb_err)?
        .map_or(0, |g| g.value());
    assert_eq!(
        next_job_id_after,
        next_job_id_before + writer.jobs_materialized,
        "next_job_id moved from {next_job_id_before} to {next_job_id_after} while applying seq {seq}, \
         which materialized {} job(s) — the counter must advance by exactly one per job",
        writer.jobs_materialized
    );
    // Negative-space: same rule as next_job_id, for next_event_id and
    // materialize_event.
    let next_event_id_after = meta
        .get("next_event_id")
        .map_err(redb_err)?
        .map_or(0, |g| g.value());
    assert_eq!(
        next_event_id_after,
        next_event_id_before + writer.events_materialized,
        "next_event_id moved from {next_event_id_before} to {next_event_id_after} while \
         applying seq {seq}, which materialized {} event(s) — the counter must advance by \
         exactly one per event",
        writer.events_materialized
    );
    meta.insert("applied_seq", seq).map_err(redb_err)?;
    Ok(out)
}

impl Store {
    pub fn open(disk: &dyn Disk, path: &str) -> io::Result<Store> {
        let file = disk.open(path)?;
        // The store file's name must be durable too — File::sync (below,
        // inside redb's writes) does not persist a directory entry, so a
        // crash right after first creation could lose the file itself
        // (DESIGN §2).
        disk.sync_dir(path)?;
        // redb 4.1.0 panics (asserts) instead of returning Err on some
        // crash-mangled files — page_manager.rs:231/:257 fire when the
        // durable header's layout length disagrees with the file length,
        // a state an unsynced `set_len` dropped at crash produces while the
        // header god-page write survives. Convert an open-time panic into an
        // open failure so Db::open's discard-and-rebuild path engages; scoped
        // to open only — a redb panic during operation still propagates.
        // Reported upstream; drop the fence when redb returns Err here.
        let latch = Arc::new(AtomicBool::new(false));
        let db = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Builder::new().create_with_backend(DiskBackend {
                file,
                latch: latch.clone(),
            })
        }))
        .map_err(|panic| {
            let msg = panic
                .downcast_ref::<&str>()
                .copied()
                .map(str::to_owned)
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            io::Error::other(format!(
                "table store (redb) panicked at open — treating the store as unopenable: {msg}"
            ))
        })?
        .map_err(redb_err)?;
        Ok(Store {
            db,
            latch,
            schedule: AtomicU64::new(SCHEDULE_SEED),
        })
    }

    /// The log position this store's *committed* state corresponds to.
    pub fn applied_seq(&self) -> io::Result<Option<u64>> {
        let txn = self.db.begin_read().map_err(redb_err)?;
        match txn.open_table(META) {
            Ok(meta) => Ok(meta
                .get("applied_seq")
                .map_err(redb_err)?
                .map(|g| g.value())),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(redb_err(e)),
        }
    }

    /// Applies one log record's table effects, atomically with the seq bump.
    /// Not durable by itself — the WAL is the durability story; `checkpoint`
    /// persists. Seqs must be gapless: replay the WAL in order. `f` returns
    /// whatever the caller needs out of the transaction (e.g. materialized
    /// job ids) — generic so it can't be tempted to smuggle that state out
    /// through a captured `&mut`, which would alias the store's own borrows.
    pub fn apply<T>(
        &self,
        seq: u64,
        f: impl FnOnce(&mut Writer<'_>) -> io::Result<T>,
    ) -> io::Result<T> {
        let applied = self.applied_seq()?;
        assert_eq!(
            seq,
            applied.map_or(0, |s| s + 1),
            "Store::apply({seq}) but applied_seq is {applied:?} — apply must be gapless; replay the WAL in order"
        );
        let mut txn = self.db.begin_write().map_err(redb_err)?;
        txn.set_durability(Durability::None).map_err(redb_err)?;
        let out = apply_one_in_txn(&txn, seq, f)?;
        txn.commit().map_err(redb_err)?;
        self.mix_schedule(1);
        Ok(out)
    }

    /// Applies a contiguous run of records in ONE write transaction: all of
    /// them commit or none of them do. `apply` is this with `records.len() ==
    /// 1`, and does exactly the same disk work in that case (one read txn, one
    /// write txn, one commit) — which is what makes a never-contended universe
    /// replay byte-identically to pre-batching code.
    ///
    /// The gapless invariant is preserved, not traded: it used to cost a read
    /// transaction per record, and now costs one read plus a running counter.
    ///
    /// Unlike `apply`, `f` is `FnMut` returning `()` rather than a generic
    /// `T` per call — a batch has no single return value, so a caller that
    /// needs per-record output (e.g. materialized ids) collects it into a
    /// buffer it owns outside the closure, rather than `apply`'s discipline
    /// of forcing everything out through the return type.
    pub fn apply_batch(
        &self,
        records: &[(u64, &[u8])],
        mut f: impl FnMut(&mut Writer<'_>, u64, &[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let applied = self.applied_seq()?;
        let mut txn = self.db.begin_write().map_err(redb_err)?;
        txn.set_durability(Durability::None).map_err(redb_err)?;
        for (expected, (seq, payload)) in (applied.map_or(0, |s| s + 1)..).zip(records) {
            assert_eq!(
                *seq, expected,
                "Store::apply_batch reached seq {seq} but expected {expected} — apply must be gapless; replay the WAL in order"
            );
            apply_one_in_txn(&txn, *seq, |w| f(w, *seq, payload))?;
        }
        txn.commit().map_err(redb_err)?;
        self.mix_schedule(records.len() as u64);
        Ok(())
    }

    /// Durably persists everything applied so far. After a crash the store
    /// reverts to its last checkpoint; the WAL replays the rest.
    pub fn checkpoint(&self) -> io::Result<()> {
        let applied = self.applied_seq()?;
        let txn = self.db.begin_write().map_err(redb_err)?;
        {
            // Dirty the transaction so the commit cannot be elided.
            let mut meta = txn.open_table(META).map_err(redb_err)?;
            meta.insert("checkpoint_seq", applied.map_or(0, |s| s + 1))
                .map_err(redb_err)?;
        }
        txn.commit().map_err(redb_err)?;
        self.mix_schedule(SCHEDULE_CHECKPOINT);
        Ok(())
    }

    /// A digest of the write transactions this handle has committed, in order:
    /// one entry per `apply`/`apply_batch` (the number of records that shared
    /// the transaction) and one per `checkpoint`. NOT a hash of the data — two
    /// handles fed identical records disagree here exactly when they grouped
    /// those records into transactions differently.
    ///
    /// It exists because redb's physical layout is a function of that grouping
    /// (DESIGN §1, property 1): equal digests are the condition under which two
    /// nodes' store files can be compared byte for byte, and the sim's
    /// fingerprint oracle reads this rather than guessing from fault counts.
    /// Seeded/imported writes (`apply_rows`, `set_import_meta`) are outside it —
    /// a seeded node's file is not byte-comparable to a replayed one anyway.
    pub fn write_schedule(&self) -> u64 {
        self.schedule.load(Ordering::Relaxed)
    }

    /// Folds one committed write transaction into `schedule`. Called AFTER the
    /// commit returns `Ok`: a transaction that failed to commit left no bytes
    /// behind and must not move the digest.
    fn mix_schedule(&self, entry: u64) {
        let mut hash = self.schedule.load(Ordering::Relaxed);
        for byte in entry.to_le_bytes() {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.schedule.store(hash, Ordering::Relaxed);
    }

    /// True once this store's handle is dead: some disk operation returned
    /// `Err`, which is precisely when redb latches `CheckedBackend.io_failed`
    /// and starts refusing every operation with `PreviousIo`. Sticky, and
    /// never cleared — redb's latch is not clearable either; the only exit is
    /// closing and re-opening the database (i.e. restarting the process).
    ///
    /// Durable state is NOT implicated: the WAL is intact and committed
    /// records are durable. Only this handle is unusable.
    pub fn failed(&self) -> bool {
        self.latch.load(Ordering::SeqCst)
    }

    /// An MVCC snapshot: sees a consistent state, never blocks writers.
    pub fn snapshot(&self) -> io::Result<Snapshot> {
        Ok(Snapshot {
            txn: self.db.begin_read().map_err(redb_err)?,
        })
    }

    /// Applies raw exported rows with NO seq or `next_job_id`/`next_event_id`
    /// bookkeeping — the seed path's writer (`Seeder::rows`, db.rs). A
    /// logical snapshot has no log position of its own to record;
    /// `set_import_meta` sets all meta keys explicitly once every table has
    /// landed.
    pub fn import(&self, f: impl FnOnce(&mut Writer<'_>) -> io::Result<()>) -> io::Result<()> {
        let mut txn = self.db.begin_write().map_err(redb_err)?;
        txn.set_durability(Durability::None).map_err(redb_err)?;
        {
            let mut writer = Writer {
                txn: &txn,
                jobs_materialized: 0,
                events_materialized: 0,
            };
            f(&mut writer)?;
        }
        txn.commit().map_err(redb_err)?;
        Ok(())
    }

    /// Deletes `EventRow`s with `id < floor` in one direct store write — the
    /// retention GC's helper (`Db::checkpoint`, db.rs), never `Store::apply`:
    /// GC is not a log record, so it must not touch `applied_seq` or the
    /// `next_event_id`/`next_job_id` counters. `Durability::None`, the same
    /// pattern as `set_import_meta`/`import`: a crash before the next
    /// durable commit just loses the deletion (rows resurface, briefly
    /// over-retained — never lost data, never an under-retention bug), and
    /// the next checkpoint recomputes the same floor from `next_event_id`
    /// and deletes them again.
    pub fn gc_events(&self, floor: u64) -> io::Result<()> {
        let mut txn = self.db.begin_write().map_err(redb_err)?;
        txn.set_durability(Durability::None).map_err(redb_err)?;
        {
            let ids: Vec<u64> = match txn.open_table(rows_def(crate::event::EventRow::TABLE)) {
                Ok(rows) => {
                    let mut ids = Vec::new();
                    for entry in rows.range(0u64..floor).map_err(redb_err)? {
                        let (key, _) = entry.map_err(redb_err)?;
                        ids.push(key.value());
                    }
                    ids
                }
                Err(redb::TableError::TableDoesNotExist(_)) => Vec::new(),
                Err(e) => return Err(redb_err(e)),
            };
            let mut writer = Writer {
                txn: &txn,
                jobs_materialized: 0,
                events_materialized: 0,
            };
            for id in ids {
                writer.delete::<crate::event::EventRow>(Id::from_raw(id))?;
            }
        }
        txn.commit().map_err(redb_err)?;
        Ok(())
    }

    /// Sets all META keys directly — the seed path's counterpart to the
    /// bookkeeping `Store::apply` derives from replay. A logical import has
    /// no records to replay, only the exporter's already-final counts.
    /// `seed_frontier_crc` is the primary's `frame_crc` of the record at
    /// `applied_seq`: a follower seeded here holds none of that record's WAL
    /// bytes (its own log starts empty at `applied_seq + 1`), so this is the
    /// only proof of its frontier's identity until real frames ship —
    /// `hello_position` (charpente::repl) reads it back to Hello with a full
    /// position instead of a `None` that forces a re-seed. The seq it vouches
    /// is stored beside it (`seed_frontier_seq` = this `applied_seq`): the
    /// CRC is only ever valid for that one seq, and `applied_seq` itself
    /// advances with later applies.
    pub fn set_import_meta(
        &self,
        applied_seq: u64,
        next_job_id: u64,
        next_event_id: u64,
        seed_frontier_crc: u32,
    ) -> io::Result<()> {
        let mut txn = self.db.begin_write().map_err(redb_err)?;
        txn.set_durability(Durability::None).map_err(redb_err)?;
        {
            let mut meta = txn.open_table(META).map_err(redb_err)?;
            meta.insert("applied_seq", applied_seq).map_err(redb_err)?;
            meta.insert("next_job_id", next_job_id).map_err(redb_err)?;
            meta.insert("next_event_id", next_event_id)
                .map_err(redb_err)?;
            meta.insert("seed_frontier_seq", applied_seq)
                .map_err(redb_err)?;
            meta.insert("seed_frontier_crc", u64::from(seed_frontier_crc))
                .map_err(redb_err)?;
        }
        txn.commit().map_err(redb_err)?;
        Ok(())
    }
}

// ---------------------------------------------------------------- Writer

/// Table effects inside one `Store::apply`. Maintains secondary indexes on
/// every put and delete.
pub struct Writer<'txn> {
    txn: &'txn redb::WriteTransaction,
    /// Count of `materialize_job` calls so far this apply — `Store::apply`
    /// checks this against `next_job_id`'s actual movement.
    jobs_materialized: u64,
    /// Count of `materialize_event` calls so far this apply — `Store::apply`
    /// checks this against `next_event_id`'s actual movement.
    events_materialized: u64,
}

impl Writer<'_> {
    pub fn put<R: Row>(&mut self, row: &R) -> io::Result<()> {
        let id = row.id().raw();
        let old = self.replace_row(row, id)?;
        for index in R::INDEXES {
            let mut table = self
                .txn
                .open_table(index_def(&index_table_name(index)))
                .map_err(redb_err)?;
            if let Some(old_row) = &old {
                let mut old_key = Vec::new();
                (index.key)(old_row, &mut old_key);
                table
                    .remove(index_entry(old_key, id).as_slice())
                    .map_err(redb_err)?;
            }
            let mut new_key = Vec::new();
            (index.key)(row, &mut new_key);
            table
                .insert(index_entry(new_key, id).as_slice(), ())
                .map_err(redb_err)?;
        }
        Ok(())
    }

    pub fn delete<R: Row>(&mut self, id: Id<R>) -> io::Result<()> {
        let mut rows = self.txn.open_table(rows_def(R::TABLE)).map_err(redb_err)?;
        let old = match rows.remove(id.raw()).map_err(redb_err)? {
            Some(guard) => R::decode_row(guard.value())?,
            None => return Ok(()), // deleting an absent row is a no-op
        };
        drop(rows);
        for index in R::INDEXES {
            let mut table = self
                .txn
                .open_table(index_def(&index_table_name(index)))
                .map_err(redb_err)?;
            let mut key = Vec::new();
            (index.key)(&old, &mut key);
            table
                .remove(index_entry(key, id.raw()).as_slice())
                .map_err(redb_err)?;
        }
        Ok(())
    }

    /// Materializes one enqueue effect as a `JobRow`, id from the monotonic
    /// `next_job_id` meta counter — bumped in this same store transaction, so
    /// ids are deterministic across replay. Platform apply-path surface.
    pub fn materialize_job(
        &mut self,
        name: &str,
        payload: &[u8],
        key: Option<&str>,
    ) -> io::Result<u64> {
        let mut meta = self.txn.open_table(META).map_err(redb_err)?;
        let id = meta
            .get("next_job_id")
            .map_err(redb_err)?
            .map_or(0, |g| g.value());
        meta.insert("next_job_id", id + 1).map_err(redb_err)?;
        drop(meta);
        self.jobs_materialized += 1;
        self.put(&crate::job::JobRow {
            id: crate::table::Id::from_raw(id),
            name: name.to_string(),
            payload: payload.to_vec(),
            key: key.map(str::to_string),
            status: crate::job::JobStatus::Pending,
            attempts: 0,
            due_at: 0,
            last_error: None,
        })?;
        Ok(id)
    }

    /// Materializes one emit effect as an `EventRow`, id from the monotonic
    /// `next_event_id` meta counter — bumped in this same store transaction,
    /// so ids are deterministic across replay. Platform apply-path surface.
    pub fn materialize_event(
        &mut self,
        seq: u64,
        name: &str,
        payload: &[u8],
        key: Option<&str>,
    ) -> io::Result<u64> {
        let mut meta = self.txn.open_table(META).map_err(redb_err)?;
        let id = meta
            .get("next_event_id")
            .map_err(redb_err)?
            .map_or(0, |g| g.value());
        meta.insert("next_event_id", id + 1).map_err(redb_err)?;
        drop(meta);
        self.events_materialized += 1;
        self.put(&crate::event::EventRow {
            id: crate::table::Id::from_raw(id),
            seq,
            name: name.to_string(),
            key: key.map(str::to_string),
            payload: payload.to_vec(),
        })?;
        Ok(id)
    }

    /// Drops and rebuilds every one of `R`'s secondary index tables from
    /// the rows table. Index entries are derived, store-local data —
    /// never logged, deterministic, idempotent — so this is safe to redo
    /// after a crash at any point (spec §3: triggered by applying a
    /// migration close-out record). Memory is bounded by index keys, not
    /// rows: each pass streams the rows table once per index.
    pub fn rebuild_indexes_for<R: Row>(&mut self) -> io::Result<()> {
        for index in R::INDEXES {
            let name = index_table_name(index);
            self.txn
                .delete_table(index_def(&name))
                .map_err(|e| io::Error::other(format!("rebuild_indexes: dropping {name}: {e}")))?;
            let mut entries: Vec<Vec<u8>> = Vec::new();
            {
                let rows = self.txn.open_table(rows_def(R::TABLE)).map_err(redb_err)?;
                for item in rows.iter().map_err(redb_err)? {
                    let (k, v) = item.map_err(redb_err)?;
                    let row = R::decode_row(v.value())?;
                    let mut key = Vec::new();
                    (index.key)(&row, &mut key);
                    entries.push(index_entry(key, k.value()));
                }
            }
            let mut table = self.txn.open_table(index_def(&name)).map_err(redb_err)?;
            for entry in &entries {
                table.insert(entry.as_slice(), ()).map_err(redb_err)?;
            }
        }
        Ok(())
    }

    /// Decode-free overwrite: the `MigratePut` apply (spec §3). The bytes
    /// are already encoded under the table's target version; the old value
    /// is never interpreted (it is one version older — decoding it under
    /// the compiled type is exactly the bug this path exists to avoid),
    /// and indexes are deliberately untouched: they are rebuilt wholesale
    /// by the close-out trigger.
    pub fn put_raw(&mut self, table: &str, id: u64, row: &[u8]) -> io::Result<()> {
        let mut rows = self.txn.open_table(rows_def(table)).map_err(redb_err)?;
        rows.insert(id, row).map_err(redb_err)?;
        Ok(())
    }

    /// Decode-free primary-row removal, the delete counterpart to `put_raw`
    /// (spec §2's decode-free rebuild replay): a below-version row was written
    /// via `put_raw` and carries no index entries, so removing the primary row
    /// is sufficient — the old value is never decoded (decoding it under the
    /// compiled type is exactly the misdecode this path exists to avoid).
    pub fn delete_raw(&mut self, table: &str, id: u64) -> io::Result<()> {
        let mut rows = self.txn.open_table(rows_def(table)).map_err(redb_err)?;
        rows.remove(id).map_err(redb_err)?;
        Ok(())
    }

    /// Raw read of one row's stored bytes (apply-side helper for the
    /// close-out trigger's old-row peek).
    pub fn get_raw(&mut self, table: &str, id: u64) -> io::Result<Option<Vec<u8>>> {
        let rows = match self.txn.open_table(rows_def(table)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(redb_err(e)),
        };
        Ok(rows.get(id).map_err(redb_err)?.map(|g| g.value().to_vec()))
    }

    /// The log position visible through THIS write transaction so far — same
    /// own-transaction reasoning as `Snapshot::applied_seq`, just read off a
    /// write transaction instead of a read one. Inside `apply_batch` that
    /// includes every record before this one in the same batch, because they
    /// share `self.txn` and `apply_one_in_txn` bumps `applied_seq` per record.
    pub fn applied_seq(&self) -> io::Result<Option<u64>> {
        match self.txn.open_table(META) {
            Ok(meta) => Ok(meta
                .get("applied_seq")
                .map_err(redb_err)?
                .map(|g| g.value())),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(redb_err(e)),
        }
    }

    fn replace_row<R: Row>(&mut self, row: &R, id: u64) -> io::Result<Option<R>> {
        let mut rows = self.txn.open_table(rows_def(R::TABLE)).map_err(redb_err)?;
        let old = match rows.get(id).map_err(redb_err)? {
            Some(guard) => Some(R::decode_row(guard.value())?),
            None => None,
        };
        let mut encoded = Vec::new();
        row.encode_row(&mut encoded);
        rows.insert(id, encoded.as_slice()).map_err(redb_err)?;
        Ok(old)
    }
}

fn index_def(name: &str) -> TableDefinition<'_, &'static [u8], ()> {
    TableDefinition::new(name)
}

// ---------------------------------------------------------------- Snapshot

pub struct Snapshot {
    txn: redb::ReadTransaction,
}

impl View for Snapshot {
    fn get<R: Row>(&self, id: Id<R>) -> io::Result<Option<R>> {
        let rows = match self.txn.open_table(rows_def(R::TABLE)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(redb_err(e)),
        };
        match rows.get(id.raw()).map_err(redb_err)? {
            Some(guard) => Ok(Some(R::decode_row(guard.value())?)),
            None => Ok(None),
        }
    }

    fn scan<R: Row>(&self) -> io::Result<Vec<R>> {
        let rows = match self.txn.open_table(rows_def(R::TABLE)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(redb_err(e)),
        };
        let mut out = Vec::new();
        for entry in rows.range(0u64..=u64::MAX).map_err(redb_err)? {
            let (_, value) = entry.map_err(redb_err)?;
            out.push(R::decode_row(value.value())?);
        }
        Ok(out)
    }

    fn scan_by<R: Row>(&self, index_name: &str, key: &impl Codec) -> io::Result<Vec<R>> {
        let index = R::INDEXES
            .iter()
            .find(|i| i.name == index_name)
            .unwrap_or_else(|| panic!("no index named \"{index_name}\" on table {}", R::TABLE));
        let table = match self.txn.open_table(index_def(&index_table_name(index))) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(redb_err(e)),
        };
        let mut encoded = Vec::new();
        key.encode(&mut encoded);
        let lo = index_entry(encoded.clone(), 0);
        let hi = index_entry(encoded, u64::MAX);
        let mut out = Vec::new();
        for entry in table
            .range(lo.as_slice()..=hi.as_slice())
            .map_err(redb_err)?
        {
            let (entry_key, _) = entry.map_err(redb_err)?;
            let bytes = entry_key.value();
            let id_bytes: [u8; 8] = bytes[bytes.len() - 8..]
                .try_into()
                .expect("entry carries an id");
            let id = Id::<R>::from_raw(u64::from_be_bytes(id_bytes));
            let row = self.get(id)?.unwrap_or_else(|| {
                panic!(
                    "index \"{}\" entry points at missing {} row {id:?} — index corruption",
                    index.name,
                    R::TABLE
                )
            });
            out.push(row);
        }
        Ok(out)
    }

    /// Silent gap semantics: if `from_id` was GC'd, this starts at the next
    /// surviving row instead of erroring — callers needing gap detection
    /// must check contiguity themselves.
    fn scan_from<R: Row>(&self, from_id: u64, limit: usize) -> io::Result<Vec<R>> {
        let rows = match self.txn.open_table(rows_def(R::TABLE)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(redb_err(e)),
        };
        let mut out = Vec::new();
        for entry in rows.range(from_id..).map_err(redb_err)?.take(limit) {
            let (_, value) = entry.map_err(redb_err)?;
            out.push(R::decode_row(value.value())?);
        }
        Ok(out)
    }
}

impl Snapshot {
    /// The log position this snapshot's rows correspond to — read off THIS
    /// snapshot's own transaction, unlike `Store::applied_seq` (which opens a
    /// fresh one): consistent with the rows a caller is about to export
    /// (`scan_raw`), even as the store keeps taking new writes underneath.
    pub fn applied_seq(&self) -> io::Result<Option<u64>> {
        match self.txn.open_table(META) {
            Ok(meta) => Ok(meta
                .get("applied_seq")
                .map_err(redb_err)?
                .map(|g| g.value())),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(redb_err(e)),
        }
    }

    /// The job id counter as of this snapshot — same own-transaction
    /// consistency reasoning as `applied_seq`.
    pub fn next_job_id(&self) -> io::Result<u64> {
        match self.txn.open_table(META) {
            Ok(meta) => Ok(meta
                .get("next_job_id")
                .map_err(redb_err)?
                .map_or(0, |g| g.value())),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(0),
            Err(e) => Err(redb_err(e)),
        }
    }

    /// The event id counter as of this snapshot — same own-transaction
    /// consistency reasoning as `applied_seq`.
    pub fn next_event_id(&self) -> io::Result<u64> {
        match self.txn.open_table(META) {
            Ok(meta) => Ok(meta
                .get("next_event_id")
                .map_err(redb_err)?
                .map_or(0, |g| g.value())),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(0),
            Err(e) => Err(redb_err(e)),
        }
    }

    /// The seed frontier `(seq, crc)` (`set_import_meta`), if this store was
    /// built by the seed path — `None` for a store opened from an ordinary
    /// log (the keys are written only by an import). A stored `0` is a real
    /// CRC, not "unset", so this is an `Option`, never `map_or(0, …)`.
    pub fn seed_frontier(&self) -> io::Result<Option<(u64, u32)>> {
        match self.txn.open_table(META) {
            Ok(meta) => {
                let seq = meta.get("seed_frontier_seq").map_err(redb_err)?;
                let crc = meta.get("seed_frontier_crc").map_err(redb_err)?;
                match (seq, crc) {
                    (Some(seq), Some(crc)) => Ok(Some((seq.value(), crc.value() as u32))),
                    (None, None) => Ok(None),
                    // Negative space: set_import_meta writes both keys in one
                    // transaction — one without the other never happens.
                    (seq, crc) => panic!(
                        "seed frontier meta is torn: seq {:?}, crc {:?} — set_import_meta writes \
                         both keys atomically",
                        seq.map(|g| g.value()),
                        crc.map(|g| g.value())
                    ),
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(redb_err(e)),
        }
    }

    /// Every row's id and raw encoded bytes, in id order — the logical
    /// export primitive (`Seeder::rows` re-applies them through the typed
    /// ops on the import side). Empty, not an error, when the table has
    /// never been created.
    pub fn scan_raw(&self, table: &str) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let rows = match self.txn.open_table(rows_def(table)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(redb_err(e)),
        };
        let mut out = Vec::new();
        for entry in rows.range(0u64..=u64::MAX).map_err(redb_err)? {
            let (key, value) = entry.map_err(redb_err)?;
            out.push((key.value(), value.value().to_vec()));
        }
        Ok(out)
    }

    /// Raw rows with `id > after_id` (all rows when `None`), ascending, at
    /// most `limit` — the migration executor's bounded batch read (spec
    /// §3: batching must bound memory, not just record size).
    pub fn scan_raw_from(
        &self,
        table: &str,
        after_id: Option<u64>,
        limit: usize,
    ) -> io::Result<Vec<(u64, Vec<u8>)>> {
        // A watermark at u64::MAX is past every possible id: `saturating_add(1)`
        // below would otherwise stay at u64::MAX forever, so `from..` never
        // advances and the row at u64::MAX re-enters every batch (permanent
        // brick for a table with a row at that id).
        if after_id == Some(u64::MAX) {
            return Ok(Vec::new());
        }
        let rows = match self.txn.open_table(rows_def(table)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(redb_err(e)),
        };
        let from = after_id.map_or(0, |a| a.saturating_add(1));
        let mut out = Vec::new();
        for item in rows.range(from..).map_err(redb_err)? {
            if out.len() == limit {
                break;
            }
            let (k, v) = item.map_err(redb_err)?;
            out.push((k.value(), v.value().to_vec()));
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------- Backend

/// redb's `StorageBackend` over a `File` — the mount point that keeps all of
/// redb's IO inside the simulation boundary.
struct DiskBackend {
    file: Box<dyn File>,
    /// Set whenever an operation below returns `Err`. redb's own
    /// `CheckedBackend` latches on exactly this condition and no other
    /// (redb-4.1.0 cached_file.rs:154-197), so this mirrors redb's
    /// `io_failed` rather than inferring it from a downstream error. redb's
    /// `close()` also sets `io_failed` (:148) — deliberately NOT mirrored
    /// here: a graceful close is not a failure.
    latch: Arc<AtomicBool>,
}

impl fmt::Debug for DiskBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DiskBackend")
    }
}

impl DiskBackend {
    /// Mirrors redb's latch: set on `Err`, never cleared.
    fn note<T>(&self, r: Result<T, io::Error>) -> Result<T, io::Error> {
        if r.is_err() {
            self.latch.store(true, Ordering::SeqCst);
        }
        r
    }
}

impl StorageBackend for DiskBackend {
    fn len(&self) -> Result<u64, io::Error> {
        self.note(self.file.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        self.note(self.file.read_at(offset, out))
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.note(self.file.set_len(len))
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        self.note(self.file.sync())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        self.note(self.file.write_at(offset, data))
    }
}

#[cfg(test)]
mod latch_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A `File` whose every operation fails, so each `DiskBackend` method can
    /// be driven down its error arm independently.
    struct AlwaysErr;

    impl crate::io::File for AlwaysErr {
        fn len(&self) -> io::Result<u64> {
            Err(io::Error::other("injected len failure"))
        }
        fn read_at(&self, _offset: u64, _out: &mut [u8]) -> io::Result<()> {
            Err(io::Error::other("injected read failure"))
        }
        fn write_at(&self, _offset: u64, _data: &[u8]) -> io::Result<()> {
            Err(io::Error::other("injected write failure"))
        }
        fn set_len(&self, _len: u64) -> io::Result<()> {
            Err(io::Error::other("injected set_len failure"))
        }
        fn sync(&self) -> io::Result<()> {
            Err(io::Error::other("injected sync failure"))
        }
    }

    fn backend() -> (DiskBackend, Arc<AtomicBool>) {
        let latch = Arc::new(AtomicBool::new(false));
        let backend = DiskBackend {
            file: Box::new(AlwaysErr),
            latch: latch.clone(),
        };
        (backend, latch)
    }

    // Each of redb's five latch sites (cached_file.rs:158/167/176/185/194) has
    // exactly one DiskBackend counterpart. If redb grows a sixth, the tripwire
    // in crates/charpente/tests/kernel.rs is what catches it.

    #[test]
    fn a_failed_len_latches() {
        let (b, latch) = backend();
        assert!(!latch.load(Ordering::SeqCst), "clean before");
        assert!(b.len().is_err());
        assert!(latch.load(Ordering::SeqCst), "len must latch on Err");
    }

    #[test]
    fn a_failed_read_latches() {
        let (b, latch) = backend();
        let mut out = [0u8; 8];
        assert!(b.read(0, &mut out).is_err());
        assert!(latch.load(Ordering::SeqCst), "read must latch on Err");
    }

    #[test]
    fn a_failed_set_len_latches() {
        let (b, latch) = backend();
        assert!(b.set_len(64).is_err());
        assert!(latch.load(Ordering::SeqCst), "set_len must latch on Err");
    }

    #[test]
    fn a_failed_sync_data_latches() {
        let (b, latch) = backend();
        assert!(b.sync_data().is_err());
        assert!(latch.load(Ordering::SeqCst), "sync_data must latch on Err");
    }

    #[test]
    fn a_failed_write_latches() {
        let (b, latch) = backend();
        assert!(b.write(0, &[1, 2, 3]).is_err());
        assert!(latch.load(Ordering::SeqCst), "write must latch on Err");
    }

    /// The sixth site, and the one deliberately NOT mirrored: redb's
    /// `CheckedBackend::close` sets `io_failed` unconditionally
    /// (cached_file.rs:146-152), because it is reachable only from
    /// `Database::drop`, so the flag it sets can never be read by a live
    /// `Store`. This is the guard on that divergence — it reddens the moment a
    /// `close` override routes anything through `note`. It has to live here:
    /// the same claim is unassertable through `Store`, since `Store::open`
    /// mints a fresh flag (table.rs:307) and any re-opened store reads clean by
    /// construction.
    #[test]
    fn a_close_does_not_latch() {
        let (b, latch) = backend();
        assert!(
            b.close().is_ok(),
            "close must take redb's default Ok arm — DiskBackend does not override it"
        );
        assert!(
            !latch.load(Ordering::SeqCst),
            "close must not latch: a clean shutdown is not a failure, and mirroring redb here \
             would set a flag no live Store could ever observe"
        );
    }
}
