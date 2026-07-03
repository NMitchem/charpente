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

/// A table row. Implemented by `#[table]`, not by hand.
pub trait Row: Sized + 'static {
    const TABLE: &'static str;
    const INDEXES: &'static [IndexDef<Self>];
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
}

// ---------------------------------------------------------------- Store

/// redb mounted behind the `Disk` trait. This is the only sanctioned way to
/// open the store; opening redb on a raw path would bypass the simulation
/// boundary (enforced by the clippy deny-list).
pub struct Store {
    db: Database,
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

impl Store {
    pub fn open(disk: &dyn Disk, path: &str) -> io::Result<Store> {
        let file = disk.open(path)?;
        let db = Builder::new()
            .create_with_backend(DiskBackend { file })
            .map_err(redb_err)?;
        Ok(Store { db })
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
        let out = {
            let mut writer = Writer { txn: &txn };
            let out = f(&mut writer)?;
            let mut meta = txn.open_table(META).map_err(redb_err)?;
            meta.insert("applied_seq", seq).map_err(redb_err)?;
            out
        };
        txn.commit().map_err(redb_err)?;
        Ok(out)
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
        Ok(())
    }

    /// An MVCC snapshot: sees a consistent state, never blocks writers.
    pub fn snapshot(&self) -> io::Result<Snapshot> {
        Ok(Snapshot {
            txn: self.db.begin_read().map_err(redb_err)?,
        })
    }
}

// ---------------------------------------------------------------- Writer

/// Table effects inside one `Store::apply`. Maintains secondary indexes on
/// every put and delete.
pub struct Writer<'txn> {
    txn: &'txn redb::WriteTransaction,
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
}

// ---------------------------------------------------------------- Backend

/// redb's `StorageBackend` over a `File` — the mount point that keeps all of
/// redb's IO inside the simulation boundary.
struct DiskBackend {
    file: Box<dyn File>,
}

impl fmt::Debug for DiskBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DiskBackend")
    }
}

impl StorageBackend for DiskBackend {
    fn len(&self) -> Result<u64, io::Error> {
        self.file.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        self.file.read_at(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.file.set_len(len)
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        self.file.sync()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        self.file.write_at(offset, data)
    }
}
