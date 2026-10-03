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
//! prefix makes exact-match lookups unambiguous (no prefix collisions). A
//! scoped table's entries carry a `[scope: u64 be]` prefix ahead of that, so
//! one tenant's slice of a shared index table is a contiguous byte range —
//! the same reasoning as `scoped_key`'s row-key prefix. Range scans over
//! indexes arrive when a real query pattern demands an order-preserving key
//! encoding.

use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::ops::Bound;
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

// A row id crosses the wire as a STRING, never a JSON number: ids are u64 and
// a JSON number silently loses precision above 2^53 in JavaScript clients
// (TS-codegen spec §1). This is a wire-format decision, not a style one — do
// not "simplify" it to a derive.
impl<T> serde::Serialize for Id<T> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&self.raw)
    }
}

impl<'de, T> serde::Deserialize<'de> for Id<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V<T>(std::marker::PhantomData<T>);
        impl<T> serde::de::Visitor<'_> for V<T> {
            type Value = Id<T>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a row id as a decimal string")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Id<T>, E> {
                v.parse::<u64>().map(Id::from_raw).map_err(|_| {
                    E::custom(format!(
                        "row id must be a decimal string — got {v:?}; ids cross the wire as \
                         strings because a JSON number loses precision above 2^53"
                    ))
                })
            }
        }
        d.deserialize_str(V(std::marker::PhantomData))
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
/// little-endian integers, length-prefixed bytes. Hand-rolled, and must
/// NEVER route through serde — this crate does depend on serde now (see
/// Cargo.toml), but only for `Id<T>`'s JSON wire identity below; the byte
/// layout here must never drift under the log. The actual fence is
/// `.github/workflows/ci.yml`'s isolated `cargo check -p charpente-core`
/// step, not this comment — `id_wire_tests` below is the mechanical guard
/// on `Id<T>`'s own encoding specifically.
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

#[cfg(test)]
mod id_wire_tests {
    use super::*;

    struct Marker;

    /// Guards the `Codec` doc comment's rule for exactly ONE impl: `Id<T>`'s.
    /// Pins `Id<T>`'s `Codec` encoding at exactly 8 bytes, fixed-width
    /// little-endian, unaffected by the JSON `Serialize`/`Deserialize`
    /// impls above. Catches either an `encode`/`decode` accidentally routed
    /// through serde (the byte count or contents would no longer match a
    /// raw LE `u64`) or a width change to the disk representation.
    ///
    /// Scope, precisely: `Id<T>` only. `u64`/`String`/`Vec<u8>`/`Option<T>`'s
    /// `Codec` impls above have no byte-level pin in this module — their
    /// only coverage is the encode/decode round-trip tests elsewhere (e.g.
    /// db.rs's `record_roundtrip_is_canonical`), which stay green even
    /// under a wholesale switch to a serde binary format. If that ever
    /// needs closing, it is a new test, not a broadened claim on this one.
    #[test]
    fn ids_disk_codec_stays_8_byte_fixed_width_le_under_the_wire_serde_impls() {
        let id = Id::<Marker>::from_raw(0x0102_0304_0506_0708);
        let mut out = Vec::new();
        id.encode(&mut out);
        assert_eq!(out, 0x0102_0304_0506_0708u64.to_le_bytes().to_vec());
        assert_eq!(out.len(), 8, "Id's disk codec must stay exactly 8 bytes");

        let mut input: &[u8] = &out;
        let back = Id::<Marker>::decode(&mut input).expect("8 well-formed bytes must decode");
        assert_eq!(back.raw(), 0x0102_0304_0506_0708);
        assert!(
            input.is_empty(),
            "decode must consume exactly 8 bytes, nothing more"
        );
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

/// Marker for a table whose rows are not tenant-scoped. The default.
pub struct Unscoped;

/// The scope marker of the platform's tables: `charpente.jobs`, whose rows
/// the dispatcher trusts for a job's tenant (job-tenancy spec §6),
/// `charpente.events`, which every subscription's feed is read from, and
/// `charpente.schema_versions`, which the migration gate trusts for each
/// table's version (tenancy-completion spec §3). Keyed like `Unscoped`, by
/// bare id. What it withholds is the transaction. `Tx::put`/`Tx::delete`
/// bound `R: WritableRow`, which a row of this scope fails, by name.
/// `View`'s reads bound `R: ReadableBy<Self>`, which it meets for `Snapshot`
/// alone (`ScopeReadableBy`). `NoTenant` still holds for it, so a `Tx` read
/// gets `ScopeReadableBy`'s text and never `NoTenant`'s. Only the platform
/// writes one:
/// the enqueue family and the dispatcher's `Tx::settle_job` a job row, apply
/// an event row, first-write adoption and `Tx::put_schema_version` a
/// schema-version row.
///
/// `#[doc(hidden)]` so rustc's "the trait is implemented for" hint under
/// `NoTenant`'s diagnostic keeps naming `Unscoped` alone: this is not a
/// scope an app table can take. Sealing another platform table is its
/// `type Scope = PlatformOwned`, its name in `db.rs`'s `PLATFORM_TABLES`,
/// and its own text there and under `AppWritable`.
#[doc(hidden)]
pub struct PlatformOwned;

/// A table row. Implemented by `#[table]`, not by hand.
pub trait Row: Sized + 'static {
    /// The tenant scope this row is partitioned by, or `Unscoped`.
    /// Set by `#[table(scoped_by = …)]`. DESIGN §8's last bullet
    /// ("Multi-tenancy") records the shipped mechanism and §3's `Scope`
    /// bullet the typed surface; the design behind both is
    /// docs/superpowers/specs/2026-08-30-tenant-scoping-design.md, whose §1
    /// ("The type surface") specifies this associated type and the
    /// attribute that sets it.
    ///
    /// Undefaulted, not `= Unscoped`: `associated_type_defaults` is
    /// unstable on this toolchain (confirmed against rustc 1.95 stable —
    /// `error[E0658]: associated type defaults are unstable`), and hard
    /// rule 2 rules out reaching for a nightly feature to get one. Every
    /// `Row` impl — macro-generated and hand-written alike — must name
    /// `type Scope` explicitly.
    ///
    /// `: 'static` (Task 4): `row_is_scoped` below distinguishes a scoped
    /// table from an unscoped one by comparing `TypeId::of::<R::Scope>()`
    /// against `TypeId::of::<Unscoped>()` — a pure, deterministic function
    /// of the type itself (no ambient state, hard rule 1), and one that
    /// cannot drift out of sync with the very type `#[table(scoped_by =
    /// …)]` sets, unlike a second, independently-set bool the macro could
    /// forget to flip. `TypeId::of` requires `'static`; every real Scope
    /// marker (`Unscoped`, an app's `Org`) satisfies it trivially.
    type Scope: 'static;
    const TABLE: &'static str;
    const INDEXES: &'static [IndexDef<Self>];
    /// The row's schema version (`#[table(version = N)]`; `1` if omitted).
    const VERSION: u32 = 1;
    /// Structural fingerprint of the field shape, `Some` only for
    /// macro-generated (`#[table]`) types. `None` (the default) opts the
    /// table out of the versioning gate and the replication digest —
    /// hand-written platform impls are unaffected by this sub-project.
    const FINGERPRINT: Option<u64> = None;
    /// The `Tenant::NAME` of `Scope`, for a row scoped by a tenant type.
    /// `#[table(scoped_by = Org)]` sets `Some(Org::NAME)`. Every other row
    /// keeps `None`: `#[table(global)]` and the platform's hand-written rows
    /// alike. A constant, because this crate cannot bound `Scope: Tenant`: an
    /// unscoped row's scope is `Unscoped`. It has a default, unlike `Scope`,
    /// because associated-const defaults are stable. `Schema::table` records
    /// it, and `App::open` refuses two tenant types under one name
    /// (tenancy-completion spec §4, plan amendment A2).
    const SCOPE_NAME: Option<&'static str> = None;
    fn id(&self) -> Id<Self>;
    fn encode_row(&self, out: &mut Vec<u8>);
    fn decode_row(bytes: &[u8]) -> io::Result<Self>;
}

// ---------------------------------------------------------------- View

/// The scope marker of a table with no tenant: `Unscoped` and `PlatformOwned`
/// implement it for every row type, and no other scope type implements it at
/// all.
///
/// It exists to carry the error text below, and its shape is that text's
/// shape. `#[diagnostic::on_unimplemented]` fires for an UNIMPLEMENTED
/// TRAIT and never for an associated-type mismatch, so bounding `View`
/// directly on `R: Row<Scope = Unscoped>` reports `type mismatch resolving
/// <Invoice as Row>::Scope == Unscoped` and stops: no contract, no fix. A
/// blanket `impl<R: Row<Scope = Unscoped>> UnscopedRow for R` does no
/// better — the failure is then a nested obligation of the blanket impl and
/// rustc reports that same mismatch. Hanging the message on a trait the
/// SCOPE type either implements or does not is what makes it fire.
///
/// The otherwise-unused `R` parameter is what puts BOTH names in the
/// message: `{Self}` is the scope type and `{R}` the row, so the suggested
/// fix is the literal line to type — real scope, real table, nothing left
/// to substitute. That is the whole of its job; it is not an extension
/// point.
///
/// SEALED, by a supertrait no other crate can name — and so is `UnscopedRow`
/// below, because sealing either one alone leaves the other as the same
/// one-line opt-out. Without both seals an app writes one safe, orphan-legal
/// line (`impl NoTenant<Invoice> for Org {}`, or `impl UnscopedRow for
/// Invoice {}` one level up), `View`'s bound is satisfied, and a scoped read
/// compiles on a bare `Tx`. Spec §7's exit criterion 1 is a compile error, so
/// neither the marker that carries it nor the bound that names it can be an
/// opt-out.
///
#[diagnostic::on_unimplemented(
    message = "`{R}` is a tenant-scoped table (`#[table(scoped_by = {Self})]`) and a bare `Tx` \
               names no tenant",
    label = "`{R}` is scoped by `{Self}`",
    note = "open the tenant scope first — in a handler, `{Self}`'s own `Tenant::authorize` runs \
            there for a credentialed caller:\n    \
            let tx = tx.scope::<{Self}>(scope, ctx)?;\n    \
            tx.scan::<{R}>()?\n\
            in a job, write `scoped_by = {Self}` in place of `global` in its `#[job(…)]`, \
            keeping its other arguments, and read through the `ScopedTx` its `run` receives"
)]
pub trait NoTenant<R>: sealed::NotATenant {}

impl<R> NoTenant<R> for Unscoped {}

impl<R> NoTenant<R> for PlatformOwned {}

/// The seals. Private, so both supertrait obligations are unsatisfiable
/// outside this crate however the orphan rules fall: the only scope types that
/// will ever carry `NotATenant` are `Unscoped` and `PlatformOwned`, the only
/// one that will ever carry `AppOwned` is `Unscoped`, the only ones that will
/// ever carry `ReadScope` are those two and the app's `Tenant` types, the only
/// reader that will ever carry `Operator` is `Snapshot`, and the only rows
/// that will ever carry `NotScoped` or `Writable` are the ones whose own
/// `Row::Scope` earns it.
mod sealed {
    pub trait NotATenant: Owned {}
    pub trait NotScoped {}
    pub trait NotScopedJob {}
    pub trait AppOwned {}
    pub trait Writable {}
    /// `ScopeReadableBy`'s seal: the scope types the read seal is about.
    pub trait ReadScope {}
    /// The readers a platform table admits. `Snapshot` alone carries it.
    pub trait Operator {}
    /// Who writes a table with no tenant: `Unscoped` for an app's own,
    /// `PlatformOwned` for the platform's. Always `Self`; see `WritableRow`
    /// for why it is asked through this projection.
    pub trait Owned {
        type Owner;
    }
}

impl sealed::NotATenant for Unscoped {}
impl sealed::NotATenant for PlatformOwned {}
impl sealed::Owned for Unscoped {
    type Owner = Unscoped;
}
impl sealed::Owned for PlatformOwned {
    type Owner = PlatformOwned;
}
impl sealed::AppOwned for Unscoped {}

/// `UnscopedRow`'s seal, derived from `Row::Scope` by the same blanket
/// condition `UnscopedRow` itself uses — so the seal admits exactly the rows
/// the bound admits and adds no second source of truth.
impl<R: Row> sealed::NotScoped for R where R::Scope: NoTenant<R> {}

/// A table with no tenant scope, and so one an `Id<R>` alone can address.
/// Implemented for exactly the `Row`s whose `Scope` is `Unscoped`, derived
/// from `Row::Scope` itself rather than set a second time — the same
/// no-drift reasoning `row_is_scoped` is built on. This is `View`'s bound,
/// which is what makes a scoped read from a bare `Tx` an unsatisfied trait
/// bound rather than a runtime refusal (spec §1, §7 exit criterion 1).
///
/// SEALED for that reason, and separately from `NoTenant`: this is the trait
/// `View` actually names, so an app that implements it directly
/// (`impl UnscopedRow for Invoice {}` — orphan-legal, one line) reaches a
/// scoped table from a bare `Tx` without ever touching the marker.
pub trait UnscopedRow: Row + sealed::NotScoped {}

impl<R: Row> UnscopedRow for R where R::Scope: NoTenant<R> {}

/// The owner marker of a table app code may write: `Unscoped` implements it
/// for every row type, and `PlatformOwned` never does. It carries the refusal
/// for a write to a platform table through a bare `Tx` (`Tx::put`,
/// `Tx::delete`), and it hangs on the owner type for `NoTenant`'s reason: an
/// associated-type mismatch cannot carry a custom diagnostic.
///
/// Job rows are the platform's because the dispatcher trusts `JobRow.scope`
/// for the tenant a scoped job runs in: a row that app code could write would
/// run a job in any tenant, with no principal and no declaration (job-tenancy
/// final review C1). Event rows are the platform's because every
/// subscription's feed is read from them, so a forged one reaches another
/// tenant's subscriber. Schema-version rows are the platform's because the
/// migration gate trusts them, so a forged one skips a table's migration
/// (tenancy-completion spec §3). One bound refuses all three row types, and
/// rustc cannot pick a note by type, so the diagnostic carries one note per
/// table, each naming its own. SEALED by `AppOwned`, which only `Unscoped`
/// carries.
#[diagnostic::on_unimplemented(
    message = "`{R}` rows are the platform's, and app code cannot write them",
    label = "`{R}` is written only by the platform",
    note = "job rows are the platform's: only the enqueue family writes one, so a job runs in \
            the tenant its enqueuer opened or its fan-out named, and no app code cancels one — \
            to run a job, enqueue it:\n    \
            tx.enqueue(job);\n\
            for a `global` job; for a `scoped_by` job, inside the tenant it acts in:\n    \
            let mut tx = tx.scope::<Org>(scope, ctx)?;\n    \
            tx.enqueue(job);\n\
            or from a `cross_tenant` route or job, naming the tenant:\n    \
            tx.enqueue_in::<Org>(scope, job, cap);",
    note = "event rows are the platform's: only an emit writes one, because every \
            subscription's feed is read from them — to publish an event, emit it with \
            `tx.emit(event)`, or, inside a tenant, with `emit` on the `ScopedTx` of the tenant \
            it belongs to",
    note = "schema-version rows are the platform's: only a table's first write and \
            `Db::migrate_schema` write one, because the migration gate trusts them for each \
            table's version and a migration's resume point — to change a table's version, \
            declare it with `#[table(version = N)]` and its `TableMigration`, and the kernel \
            migrates at startup"
)]
pub trait AppWritable<R>: sealed::AppOwned {}

impl<R> AppWritable<R> for Unscoped {}

/// `WritableRow`'s seal, derived by the same condition `WritableRow` uses.
impl<R: Row> sealed::Writable for R
where
    R::Scope: NoTenant<R>,
    <R::Scope as sealed::Owned>::Owner: AppWritable<R>,
{
}

/// A row a bare `Tx` may write: an `UnscopedRow` whose table is the app's
/// own. This is `Tx::put`'s and `Tx::delete`'s bound, the write half of the
/// seal `PlatformOwned` describes; the read half is `ReadableBy`. Sealed for
/// `UnscopedRow`'s reason.
///
/// `AppWritable` is asked of the scope's `Owner`, a projection through
/// `NoTenant`'s own seal, rather than of the scope type directly. A scoped
/// row fails `NoTenant` first; the projection then has nothing to resolve,
/// so rustc prints `NoTenant`'s one diagnostic and never a second saying the
/// row is the platform's. A platform row passes `NoTenant` and fails
/// `AppWritable`, whose text is the one it gets.
pub trait WritableRow: UnscopedRow + sealed::Writable {}

impl<R: Row> WritableRow for R
where
    R::Scope: NoTenant<R>,
    <R::Scope as sealed::Owned>::Owner: AppWritable<R>,
{
}

/// The job twin of `NoTenant` (job-tenancy spec §5): `Unscoped` implements it
/// for every job type, and no other scope type implements it at all. It
/// carries the refusal for a `#[job(scoped_by = …)]` job enqueued where no
/// tenant is named — `Tx::enqueue` on a bare `Tx`, or a cron schedule — and it
/// hangs on a trait the SCOPE type implements or not for `NoTenant`'s reason:
/// an associated-type mismatch cannot carry a custom diagnostic. `{J}` puts
/// the job's name in the text, as `{R}` puts the row's.
///
/// SEALED by `NotATenant`, the same seal, and `UnscopedJob` below by its own:
/// either one alone would leave the other as a one-line, orphan-legal opt-out
/// (`impl NoJobTenant<Notify> for Org {}`, or `impl UnscopedJob for Notify {}`).
#[diagnostic::on_unimplemented(
    message = "`{J}` is a tenant-scoped job (`#[job(scoped_by = {Self})]`) and a bare `Tx` \
               names no tenant to run it in",
    label = "`{J}` is scoped by `{Self}`",
    note = "enqueue it from inside the tenant it acts in, and it runs in that tenant:\n    \
            let mut tx = tx.scope::<{Self}>(scope, ctx)?;\n    \
            tx.enqueue(job);\n\
            or fan it out from a `cross_tenant` route or job, naming the tenant:\n    \
            tx.enqueue_in::<{Self}>(scope, job, cap);\n\
            cron (`App::schedule`) enqueues on a bare `Tx`, so it schedules `global` jobs only"
)]
pub trait NoJobTenant<J>: sealed::NotATenant {}

impl<J> NoJobTenant<J> for Unscoped {}

/// `UnscopedJob`'s seal, derived from `Job::Scope` by the same condition
/// `UnscopedJob` uses, so the seal admits exactly the jobs the bound admits.
impl<J: crate::db::Job> sealed::NotScopedJob for J where J::Scope: NoJobTenant<J> {}

/// A job with no tenant: exactly the `Job`s whose `Scope` is `Unscoped`,
/// derived from `Job::Scope` as `UnscopedRow` is from `Row::Scope`. This is
/// the bound on `Tx::enqueue`, `Tx::enqueue_keyed` and `App::schedule`, so a
/// scoped job at any of them is an unsatisfied bound whose text names the fix,
/// not a job that runs in no tenant. Sealed for `UnscopedRow`'s reason.
pub trait UnscopedJob: crate::db::Job + sealed::NotScopedJob {}

impl<J: crate::db::Job> UnscopedJob for J where J::Scope: NoJobTenant<J> {}

/// Which readers `V` may read a row of scope `Self` through `View`: the read
/// half of the seal `PlatformOwned` describes (tenancy-completion spec §3).
/// An app's own table is readable by every `View`: `Unscoped` for a global
/// one, and every `S: Tenant` for a scoped one, whose rows `View`'s
/// `UnscopedRow` bound refuses instead, so that `NoTenant`'s one diagnostic
/// stays the one rustc prints for a scoped read. A platform table is readable
/// by a `Snapshot` alone: a `Tx`, and so a handler, a job and a `ScopedTx`'s
/// deref, cannot name one.
///
/// The platform impl is written for the sealed `Operator` reader marker, not
/// for `Snapshot` directly, and that is load-bearing for the diagnostic. When
/// only one impl could apply (the tenant blanket), rustc blames its nested
/// `PlatformOwned: Tenant` and never shows this text; when two could, it
/// reports this trait. The tenancy-completion plan's Task 9 quotes both.
///
/// SEALED by `ReadScope`, which only the scope types above carry.
#[diagnostic::on_unimplemented(
    message = "`{R}` is one of the platform's tables, and a transaction does not read it",
    label = "`{R}` is read through `Db::snapshot`, never through a transaction",
    note = "the platform's tables — the job queue (`JobRow`), the event log (`EventRow`) and \
            the migration record (`SchemaVersion`) — are the platform's own state, and the \
            first two hold every tenant's rows, so neither a handler nor a job reads them; \
            code that holds the `Db` reads them through a snapshot:\n    \
            db.snapshot()?.scan::<{R}>()?",
    note = "a route that shows a job's progress or an event's history reads a table of the \
            app's own, written in the same transaction as the enqueue or the emit, or \
            subscribes to the events"
)]
pub trait ScopeReadableBy<V: ?Sized, R>: sealed::ReadScope {}

impl<V: ?Sized, R> ScopeReadableBy<V, R> for Unscoped {}

impl<V: ?Sized, R, S: crate::db::Tenant> ScopeReadableBy<V, R> for S {}

impl<V: ?Sized + sealed::Operator, R> ScopeReadableBy<V, R> for PlatformOwned {}

impl sealed::ReadScope for Unscoped {}
impl sealed::ReadScope for PlatformOwned {}
impl<S: crate::db::Tenant> sealed::ReadScope for S {}
impl sealed::Operator for Snapshot {}

/// A row the reader `V` may read through `View`, derived from `Row::Scope` by
/// the one blanket impl below, as `UnscopedRow` is, so there is no second
/// source of truth. `View`'s four methods bound `R: ReadableBy<Self>`. Code
/// generic over the reader alone, like every `#[index]` accessor, needs
/// nothing more, because an app's table is readable by every `View`. Code
/// generic over the row type as well names it:
/// `fn all<R: UnscopedRow + ReadableBy<Snapshot>>(snap: &Snapshot)`.
///
/// It needs no seal of its own: an app's `impl ReadableBy<Tx<'_>> for Alias
/// {}` conflicts with the blanket impl (E0119).
pub trait ReadableBy<V: ?Sized>: Row {}

impl<R: Row, V: ?Sized> ReadableBy<V> for R where R::Scope: ScopeReadableBy<V, R> {}

/// Read access to UNSCOPED table state — implemented by `Snapshot` (reads
/// outside transactions) and `Tx` (reads inside one, which also see the
/// transaction's own writes). The `#[table]` accessors are generated
/// against this trait.
///
/// Every method bounds `R: UnscopedRow`, so this trait is the whole read
/// surface for tables that have no tenant, and none of the surface for
/// tables that do. A scoped table is read through `Snapshot`'s
/// `get_scoped`/`scan_scoped`/`scan_from_scoped`/`scan_by_scoped`, or — the
/// one an app writes — `ScopedTx`, which `Tx::open_scope` hands back once the
/// app's own `Tenant::authorize` has passed. `Tx`'s own scope-taking
/// primitives are private to `db.rs`; `ScopedTx` is their only caller.
///
/// Every method also bounds `R: ReadableBy<Self>`, which an app's own table
/// meets for every reader, and a platform table (`JobRow`, `EventRow`,
/// `SchemaVersion`) for `Snapshot` alone: a transaction does not read the
/// platform's tables (tenancy-completion spec §3). `Tx` reads its snapshot
/// through `Snapshot`'s crate-private `get_any` and its siblings, which carry
/// no `ReadableBy` bound, rather than through this trait.
pub trait View {
    fn get<R: UnscopedRow + ReadableBy<Self>>(&self, id: Id<R>) -> io::Result<Option<R>>;

    /// Every row, in id order. A `Vec` for now; iterators when a real
    /// workload shows the allocation matters.
    fn scan<R: UnscopedRow + ReadableBy<Self>>(&self) -> io::Result<Vec<R>>;

    /// Exact-match secondary index lookup, in id order. Called through the
    /// typed accessors `#[table]` generates (`Invoice::by_org(...)`).
    fn scan_by<R: UnscopedRow + ReadableBy<Self>>(
        &self,
        index_name: &str,
        key: &impl Codec,
    ) -> io::Result<Vec<R>>;

    /// Rows with `id >= from_id`, in id order, at most `limit` — the
    /// realtime cursor read (`charpente.events` through a `Snapshot`, and any
    /// app table). A `Vec` for now; see `scan`'s note.
    fn scan_from<R: UnscopedRow + ReadableBy<Self>>(
        &self,
        from_id: u64,
        limit: usize,
    ) -> io::Result<Vec<R>>;
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

/// A scoped table's row key: scope then id, both big-endian, so that
/// lexicographic byte order matches (scope, id) numeric order and one
/// tenant's rows form a contiguous range. Same reason `index_entry` encodes
/// its trailing id big-endian. `pub(crate)`, deliberately: this is an
/// internal key encoding, and `charpente-core`'s public surface is re-exported
/// wholesale through `charpente` (officially load-bearing), so anything `pub`
/// here is a compatibility promise. Nothing forces it wider — the ordering
/// property is pinned by `scoped_key_tests` below, in this crate, and that
/// test needs neither a `Store` nor a `Sim`.
///
/// Verified against redb 4.1.0's own `Key`/`Value` impls
/// (`~/.cargo/registry/.../redb-4.1.0/src/types.rs`), and re-read for 4.3.0
/// (its array `compare` is unchanged, and the new `Key::separator` returns
/// the left key whole for a fixed-width element type), before choosing this
/// over `&[u8]`: `[u8; 16]` gets `Key`/`Value` via the blanket
/// `impl<const N, T: Key> Key for [T; N]` / `impl<const N, T: Value> Value
/// for [T; N]` with `T = u8` (`u8: Key + Value` via the `le_impl!` macro,
/// `fixed_width() == Some(1)`). That blanket `Key::compare` walks the array
/// element by element and, since each element is a single byte, reduces to
/// exactly `data1.cmp(data2)` — plain lexicographic order over the 16 raw
/// bytes, which is what big-endian encoding above depends on. `[u8; 16]` is
/// also `fixed_width() == Some(16)`, which `&[u8]` (variable-width) is not —
/// a fixed-width key is the better fit for a key redb can compare without an
/// extra length probe.
pub(crate) fn scoped_key(scope: u64, id: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&scope.to_be_bytes());
    k[8..].copy_from_slice(&id.to_be_bytes());
    k
}

/// The inverse of `scoped_key`: recover `(scope, id)` from a stored
/// composite key.
fn split_scoped_key(key: [u8; 16]) -> (u64, u64) {
    let scope = u64::from_be_bytes(
        key[..8]
            .try_into()
            .expect("scoped key carries a leading 8-byte scope"),
    );
    let id = u64::from_be_bytes(
        key[8..]
            .try_into()
            .expect("scoped key carries a trailing 8-byte id"),
    );
    (scope, id)
}

fn scoped_rows_def(table: &str) -> TableDefinition<'_, [u8; 16], &'static [u8]> {
    TableDefinition::new(table)
}

/// `scan_raw`/`scan_raw_from`'s unscoped-branch fallback for whatever
/// `open_table` returns that is NOT a `TableTypeMismatch` (that specific
/// case now routes through `unscoped_open_err`, the same confident message
/// every other unscoped door gives — task 3 fix round 1). Names both known
/// shapes (composite, bare) rather than the raw redb error naming whichever
/// one it actually found, since at these two call sites the caller's
/// `scoped: bool` came from outside a compiled `R: Row` and there is
/// genuinely less certainty about which shape was expected.
fn scan_raw_shape_error(table: &str, attempt: redb::TableError) -> io::Error {
    io::Error::other(format!(
        "table \"{table}\": neither the scoped composite-key shape nor the bare-u64 shape \
         opened it ({attempt})"
    ))
}

/// `scan_raw`/`scan_raw_from`'s scoped branch found the table's rows
/// physically bare-`u64`-keyed. Scoping is opt-in per table and never
/// retrofitted onto an existing one (spec §8's non-goal, "Retrofitting
/// existing tables"): a table cannot gain a scope in place, so this is
/// always a schema/disk disagreement, never a state to tolerate — proven,
/// not assumed, by `migrate_table`'s own `MigratePut` dying on the exact
/// same physical shape with an unnamed redb type error the moment it tries
/// to write the row back under a scope.
fn scope_flip_error(table: &str) -> io::Error {
    io::Error::other(format!(
        "table \"{table}\" is declared scoped_by but its rows are keyed unscoped: a table \
         cannot gain a scope in place; create a new scoped table and copy rows through a job"
    ))
}

/// A scoped row-table open failed. `TableTypeMismatch` is the scope-flip
/// contract above: the schema asked for the composite-key shape and the disk
/// holds bare-`u64` rows. Every scoped entry point routes its open failure
/// through here, so an operator meets the named contract at whichever door
/// they knocked on. Eleven functions open a scoped row table; `scan_raw` and
/// `scan_raw_from` named the contract and the other NINE — eight of them
/// `pub` — fell through to `redb_err`, whose text is redb's own ("… is of
/// type Table<u64, &[u8]>") and names neither the contract nor the fix
/// (hard rule 5). Single-sourced rather than repeated per site: the previous
/// shape was correct in two places and silently absent in nine, and nothing
/// could notice.
fn scoped_open_err(table: &str, e: redb::TableError) -> io::Error {
    match e {
        redb::TableError::TableTypeMismatch { .. } => scope_flip_error(table),
        e => redb_err(e),
    }
}

/// An unscoped row-table open failed. `TableTypeMismatch` is the reverse
/// scope-flip contract: the schema asked for the bare-`u64` shape and the
/// disk holds composite-keyed rows. Every unscoped entry point routes its
/// open failure through here, so an operator meets the named contract at
/// whichever door they knocked on — the mirror of `scoped_open_err` above.
/// Twelve functions open an unscoped row table; `gc_events`' event-table
/// open is unreachable by this disagreement (`EventRow::Scope` is
/// `PlatformOwned`, keyed as `Unscoped` is, and its only writer, `materialize_event`,
/// always calls `put` under `UNSCOPED`, so its rows table can never be
/// created composite-keyed) and stays on `redb_err`; the other ELEVEN —
/// every one reachable through a `pub` entry point — now route through
/// here on a `TableTypeMismatch` specifically. Nine of them (this function's
/// original set) fell through to plain `redb_err` before this fix, whose
/// text is redb's own ("… is of type Table<[u8;16], &[u8]>") and names
/// neither the contract nor the fix (hard rule 5); the per-site shape is
/// exactly what let the forward gap (`14863db`) open unnoticed, hence
/// single-sourced here too. `scan_raw`/`scan_raw_from`'s unscoped branches
/// were the other two: they already special-cased `TableTypeMismatch`, but
/// through `scan_raw_shape_error`'s "neither shape opened it" — an
/// intentionally vaguer message for when the caller's `scoped: bool` (not a
/// compiled `R: Row`) leaves genuine doubt about which shape was expected —
/// which named neither the contract nor the fix either, so those two doors
/// answered the identical disagreement with a different message than the
/// other nine and nothing pinned that this was deliberate. They now give
/// the same confident answer this function gives everywhere else;
/// `scan_raw_shape_error` remains only for a non-`TableTypeMismatch` open
/// failure at those two sites, a genuinely different condition.
fn unscoped_open_err(table: &str, e: redb::TableError) -> io::Error {
    match e {
        redb::TableError::TableTypeMismatch { .. } => io::Error::other(format!(
            "table \"{table}\" is declared without scoped_by but its rows are keyed with a \
             tenant scope: a table cannot lose a scope in place; declare it scoped_by, or \
             create a new unscoped table and copy rows through a job"
        )),
        other => redb_err(other),
    }
}

/// The row-key scope slot for a table that is not tenant-scoped — every
/// table today. `pub`, not `pub(crate)`: `Writer::put`/`Writer::delete` now
/// take an explicit, required scope (RULING, tenant-scoping Task 4 dispatch
/// — deriving it from a row's own field, e.g. the spec's example `Invoice.
/// org`, was refused: the platform never interprets the scoping key, and
/// picking a field by name is the same unenforced-convention hazard
/// `row_id_prefix`'s own comment warns about), so every existing caller
/// outside this crate (`crates/charpente/tests/typed_tables.rs` drives
/// `Writer` directly) must be able to name this constant rather than reach
/// for a bare `0` whose meaning isn't visible at the call site. Deliberately
/// a DIFFERENT constant from `db.rs`'s own private `UNSCOPED` (the overlay's
/// scope slot) — same value, different layer, and conflating the two would
/// blur "the overlay's staged-write key" with "the store's on-disk row key".
/// Task 7's `tx.scope()` is the only thing that ever produces a value other
/// than this one.
pub const UNSCOPED: u64 = 0;

/// Whether `R`'s rows use the composite `(scope, id)` key rather than a bare
/// `id`. A pure function of the type `R::Scope` the (already-landed)
/// `#[table(scoped_by = …)]` macro sets — not a second, independently
/// tracked flag that could disagree with it. `TypeId::of` is deterministic
/// and ambient-state-free (hard rule 1): it depends on nothing but the
/// monomorphized type, so this same call always resolves to the same
/// compile-time-knowable answer for a given `R`, and optimizes to a constant
/// in release builds exactly as `R::VERSION`/`R::FINGERPRINT` do.
///
/// `PlatformOwned` is keyed exactly as `Unscoped` is: it seals writes, not
/// the key, so `charpente.jobs`, `charpente.events` and
/// `charpente.schema_versions` store byte-identically to before the seal.
pub(crate) fn row_is_scoped<R: Row>() -> bool {
    let scope = std::any::TypeId::of::<R::Scope>();
    scope != std::any::TypeId::of::<Unscoped>() && scope != std::any::TypeId::of::<PlatformOwned>()
}

#[cfg(test)]
mod scoped_key_tests {
    use super::*;

    /// Task 4 (tenant scoping), Step 2: `scoped_key`'s ordering property.
    ///
    /// A unit test, not an integration one: it needs no `Store` and no `Sim`,
    /// so it does not pay the price `engine.rs`'s module doc explains (a
    /// `#[cfg(test)]` build of this crate is a distinct compilation unit from
    /// the one `charpente-sim` links against, so anything touching a `Disk`
    /// has to live under `tests/`). Keeping it here is what lets `scoped_key`
    /// stay `pub(crate)`.
    #[test]
    fn a_scoped_key_orders_by_scope_then_id() {
        // Big-endian is load-bearing: lexicographic byte order must agree with
        // (scope, id) numeric order, or a prefix range is not contiguous and
        // the whole scan guarantee collapses into a filter.
        let a = scoped_key(1, u64::MAX);
        let b = scoped_key(2, 0);
        assert!(
            a < b,
            "tenant 1's last row must sort before tenant 2's first"
        );

        let c = scoped_key(1, 5);
        let d = scoped_key(1, 6);
        assert!(c < d, "within a tenant, ids must stay ordered");
    }

    /// `scoped_index_entry`'s exact byte layout: `[scope u64 be][keylen u32
    /// le][key][id u64 be]`. Every integration test that exercises a scoped
    /// index observes this only through `scan_by_scoped`, which bounds
    /// itself with the same encoder — writer and reader agree under a
    /// coordinated re-encoding (little-endian scope, or scope moved after
    /// the key), so a round trip alone proves nothing about the bytes on
    /// disk, which every pinned simulation seed depends on. A unit test, not
    /// an integration one, for the same reason `a_scoped_key_orders_by_
    /// scope_then_id` above is: it needs no `Store`.
    #[test]
    fn scoped_index_entry_layout_is_scope_be_then_keylen_le_then_key_then_id_be() {
        let entry = scoped_index_entry(
            0x0102_0304_0506_0708,
            vec![0xAA, 0xBB],
            0x1112_1314_1516_1718,
        );
        let mut expected = 0x0102_0304_0506_0708u64.to_be_bytes().to_vec();
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&[0xAA, 0xBB]);
        expected.extend_from_slice(&0x1112_1314_1516_1718u64.to_be_bytes());
        assert_eq!(entry, expected);
    }

    /// `scoped_index_entry`'s ordering claim, its own doc comment's: scope
    /// dominates key, which dominates id. Lexicographic byte order over the
    /// entry must agree with (scope, key, id) order, or `scan_by_scoped`'s
    /// tenant-bounded range is not contiguous and collapses into a filter —
    /// same reasoning as `a_scoped_key_orders_by_scope_then_id` above, one
    /// level up.
    #[test]
    fn scoped_index_entry_orders_by_scope_then_key_then_id() {
        let k = vec![9u8];
        assert!(
            scoped_index_entry(1, k.clone(), u64::MAX) < scoped_index_entry(2, k.clone(), 0),
            "scope must be the most significant field: tenant 1's last entry must sort before \
             tenant 2's first, regardless of key or id"
        );

        let k1 = vec![1u8];
        let k2 = vec![2u8];
        assert!(
            scoped_index_entry(1, k1, 9) < scoped_index_entry(1, k2, 9),
            "within one scope, entries must still order by key"
        );
    }
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

/// A scoped table's index entry: `index_entry`'s layout with `scope`
/// prefixed, big-endian, so that (as with `scoped_key`) lexicographic byte
/// order matches (scope, key, id) order and one tenant's entries for a
/// given key form a contiguous range — what `Snapshot::scan_by_scoped`'s
/// bound depends on. Unscoped tables never call this: `index_entry` above
/// keeps writing their entries byte-identically to before this prefix
/// existed (pinned by `an_unscoped_index_entry_stays_byte_identical`,
/// engine.rs).
fn scoped_index_entry(scope: u64, key: Vec<u8>, id: u64) -> Vec<u8> {
    let mut entry = scope.to_be_bytes().to_vec();
    entry.extend_from_slice(&index_entry(key, id));
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

/// redb's verdict, at [`Store::open`], that the store file's bytes are not a
/// database it will open. It is the payload of the `io::Error` `Store::open`
/// returns (kind `InvalidData`; read it back with [`StoreRejected::of`]), and
/// the only open failure `Db::open` answers by discarding the store and
/// rebuilding it from the log — or, on a log compacted past seq 0, by refusing
/// to boot. Every other `Store::open` error is an IO failure (the disk's own,
/// or redb's latch after one) that a restart retries, and `Db::open` returns it
/// with the site and the fix named, its kind and text kept. DESIGN §4 tables
/// how every error redb 4.3.0 can return at open is classified.
#[derive(Debug)]
pub struct StoreRejected {
    /// Which verdict redb gave.
    pub reason: Rejection,
    /// redb's own wording, rendered as `redb_err` renders every redb error.
    text: String,
}

/// The three ways redb rejects a store file's bytes at open (redb 4.3.0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    /// `StorageError::Corrupted`: a header field, a commit slot's or page's
    /// checksum, or a tree's structure that redb read and refused.
    Corrupted,
    /// `DatabaseError::UpgradeRequired`: the file's commit slot names redb
    /// file format `version`, which this redb opens only after a manual
    /// upgrade. Not damage in itself.
    UpgradeRequired { version: u8 },
    /// The file is not empty and does not begin with redb's magic number,
    /// e.g. a creation that never became durable.
    NotARedbFile,
}

impl fmt::Display for StoreRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl std::error::Error for StoreRejected {}

impl StoreRejected {
    /// `Some` only for redb's rejection of the file's bytes, as
    /// `TableWriteRefusal::of`.
    pub fn of(e: &io::Error) -> Option<&StoreRejected> {
        e.get_ref()
            .and_then(|inner| inner.downcast_ref::<StoreRejected>())
    }
}

/// Sorts what `create_with_backend` returned into redb's verdict on the bytes
/// and everything else, by type alone. A rejection needs positive evidence:
/// it deletes a store (or refuses a boot), so a variant not named here is an
/// IO failure and bubbles. Every variant redb 4.3.0 can return is classified
/// in DESIGN §4's table; the three rejected ones are:
/// - `Corrupted` — redb's verdict on bytes it read without error;
/// - `UpgradeRequired` — the commit slot's format version byte;
/// - `Io` that `DiskBackend` did not pass up — redb builds exactly one such
///   error from the bytes at open, "Not a redb database: magic number
///   mismatch" (`page_manager.rs:1137`), and every error the disk returns
///   reaches redb wrapped in `FromDisk`, so the wrapper is what tells the two
///   apart. The kind check keeps any other unwrapped error redb might build
///   (none is reachable at open in 4.3.0) out of the destructive arm.
///
/// One disk error is named here rather than passed up bare: a read past the
/// end of the file. redb reads its 320-byte header once the magic number
/// matches, so a file of 9 to 319 bytes with its magic intact fails every
/// open on that read. It is not a rejection, since the disk said it and redb
/// built no verdict, but no restart fixes it either, and redb makes the
/// file's length durable before it writes the magic (`page_manager.rs:1181-1200`),
/// so no crash leaves one: the file was cut outside the crash model. Its fix
/// depends on the log beside the store, which only `Db::open` sees, so
/// `Db::open` names it.
fn open_error(e: redb::DatabaseError, path: &str) -> io::Error {
    use redb::{DatabaseError, StorageError};
    if let DatabaseError::Storage(StorageError::Io(io)) = &e
        && io.kind() == io::ErrorKind::UnexpectedEof
        && io.get_ref().is_some_and(|inner| inner.is::<FromDisk>())
    {
        return io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "table store (redb): {path} ends before redb's file header does ({io}). redb \
                 found its magic number, so this is not a file it rejects; but no crash \
                 leaves a store this short, since redb makes the file's length durable before \
                 it writes the magic, so the file was cut outside the crash model and every \
                 boot will fail the same way"
            ),
        );
    }
    let reason = match &e {
        DatabaseError::Storage(StorageError::Corrupted(_)) => Rejection::Corrupted,
        DatabaseError::UpgradeRequired(version) => Rejection::UpgradeRequired { version: *version },
        DatabaseError::Storage(StorageError::Io(io))
            if io.kind() == io::ErrorKind::InvalidData
                && !io.get_ref().is_some_and(|inner| inner.is::<FromDisk>()) =>
        {
            Rejection::NotARedbFile
        }
        _ => return redb_err(e),
    };
    io::Error::new(
        io::ErrorKind::InvalidData,
        StoreRejected {
            reason,
            text: redb_err(e).to_string(),
        },
    )
}

impl Store {
    /// Opens (or creates) the store at `path` through `disk`. An `Err` whose
    /// [`StoreRejected::of`] is `Some` is redb refusing the file's bytes;
    /// any other `Err` — including this function's own `disk.open` and
    /// `disk.sync_dir` — is an IO failure, passed up as the disk or redb
    /// rendered it.
    pub fn open(disk: &dyn Disk, path: &str) -> io::Result<Store> {
        let file = disk.open(path)?;
        // The store file's name must be durable too — File::sync (below,
        // inside redb's writes) does not persist a directory entry, so a
        // crash right after first creation could lose the file itself
        // (DESIGN §2).
        disk.sync_dir(path)?;
        // A crash-mangled file whose length disagrees with its durable
        // header's layout is an ordinary `Err` here (redb 4.3.0 returns
        // `StorageError::Corrupted` where 4.1.0 asserted), rejected by
        // `open_error`, so Db::open's discard-and-rebuild path — or its
        // compacted-log refusal — takes it with no panic fence. A redb panic,
        // at open or later, is a real failure and propagates (DESIGN §4).
        let latch = Arc::new(AtomicBool::new(false));
        let db = Builder::new()
            .create_with_backend(DiskBackend {
                file,
                latch: latch.clone(),
            })
            .map_err(|e| open_error(e, path))?;
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
    ///
    /// One atomic read-modify-write, not a load/store pair. `Store` is `Sync`
    /// and public, so nothing in the type stops a caller sharing one behind an
    /// `Arc`; a load/store pair would silently drop an entry there and produce
    /// a digest that says "same grouping" about two nodes that grouped
    /// differently — a false GREEN on the byte-fingerprint oracle, which is the
    /// worst direction for this to fail. Correct by construction rather than by
    /// auditing today's callers (hard rule 4).
    fn mix_schedule(&self, entry: u64) {
        let _ = self
            .schedule
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |hash| {
                let mut hash = hash;
                for byte in entry.to_le_bytes() {
                    hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
                }
                Some(hash)
            });
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
    /// and deletes them again. Decode-free (`delete_raw`): a row GC removes is
    /// never read, so a corrupt one is collected like any other instead of
    /// failing every checkpoint from then on. Sound only because `EventRow`
    /// has no index entries to remove, which the const assert holds.
    pub fn gc_events(&self, floor: u64) -> io::Result<()> {
        const { assert!(crate::event::EventRow::INDEXES.is_empty()) };
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
                writer.delete_raw(crate::event::EventRow::TABLE, None, id)?;
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
    /// `scope` is required, not optional/defaulted (RULING, tenant-scoping
    /// Task 4 dispatch — see `UNSCOPED`'s doc comment): a forgotten scope
    /// must be a compile error at every call site, not a silently-defaulted
    /// zero. Every existing caller — inside this crate and
    /// `crates/charpente/tests/typed_tables.rs`, which drives `Writer`
    /// directly — passes `UNSCOPED` explicitly; a scoped write reaches here
    /// only through `ScopedTx`, which carries its tenant. For an unscoped `R`,
    /// `scope` is asserted rather than merely ignored (hard rule 4): a
    /// caller passing a nonzero scope to an unscoped table is a bug the
    /// bare-`u64` key path would otherwise silently swallow.
    pub fn put<R: Row>(&mut self, scope: u64, row: &R) -> io::Result<()> {
        let id = row.id().raw();
        let old = self.replace_row(scope, row, id)?;
        let scoped = row_is_scoped::<R>();
        for index in R::INDEXES {
            let mut table = self
                .txn
                .open_table(index_def(&index_table_name(index)))
                .map_err(redb_err)?;
            if let Some(old_row) = &old {
                let mut old_key = Vec::new();
                (index.key)(old_row, &mut old_key);
                let old_entry = if scoped {
                    scoped_index_entry(scope, old_key, id)
                } else {
                    index_entry(old_key, id)
                };
                table.remove(old_entry.as_slice()).map_err(redb_err)?;
            }
            let mut new_key = Vec::new();
            (index.key)(row, &mut new_key);
            let new_entry = if scoped {
                scoped_index_entry(scope, new_key, id)
            } else {
                index_entry(new_key, id)
            };
            table.insert(new_entry.as_slice(), ()).map_err(redb_err)?;
        }
        Ok(())
    }

    /// See `put`'s doc comment for why `scope` is required rather than
    /// optional. Index entries carry the same scope prefix `put` writes:
    /// removing an entry under the wrong scope would not error — redb's
    /// `remove` is a no-op on an absent key — it would just strand the real
    /// entry, so the scope must reach this removal exactly as it reaches
    /// `put`'s.
    pub fn delete<R: Row>(&mut self, scope: u64, id: Id<R>) -> io::Result<()> {
        let scoped = row_is_scoped::<R>();
        let old = if scoped {
            let mut rows = self
                .txn
                .open_table(scoped_rows_def(R::TABLE))
                .map_err(|e| scoped_open_err(R::TABLE, e))?;
            match rows.remove(scoped_key(scope, id.raw())).map_err(redb_err)? {
                Some(guard) => R::decode_row(guard.value())?,
                None => return Ok(()), // deleting an absent row is a no-op
            }
        } else {
            assert_eq!(
                scope,
                UNSCOPED,
                "Writer::delete: table \"{}\" is not tenant-scoped (Row::Scope = Unscoped) but \
                 was called with scope {scope} — pass table::UNSCOPED for an unscoped table",
                R::TABLE
            );
            let mut rows = self
                .txn
                .open_table(rows_def(R::TABLE))
                .map_err(|e| unscoped_open_err(R::TABLE, e))?;
            match rows.remove(id.raw()).map_err(redb_err)? {
                Some(guard) => R::decode_row(guard.value())?,
                None => return Ok(()), // deleting an absent row is a no-op
            }
        };
        for index in R::INDEXES {
            let mut table = self
                .txn
                .open_table(index_def(&index_table_name(index)))
                .map_err(redb_err)?;
            let mut key = Vec::new();
            (index.key)(&old, &mut key);
            let entry = if scoped {
                scoped_index_entry(scope, key, id.raw())
            } else {
                index_entry(key, id.raw())
            };
            table.remove(entry.as_slice()).map_err(redb_err)?;
        }
        Ok(())
    }

    /// Materializes one enqueue effect as a `JobRow`, id from the monotonic
    /// `next_job_id` meta counter — bumped in this same store transaction, so
    /// ids are deterministic across replay. Platform apply-path surface.
    /// `scope` is the record's tenant for a scoped job (`0x0a`), carried into
    /// the row unchanged.
    pub fn materialize_job(
        &mut self,
        name: &str,
        payload: &[u8],
        key: Option<&str>,
        shape: Option<u64>,
        scope: Option<u64>,
    ) -> io::Result<u64> {
        let mut meta = self.txn.open_table(META).map_err(redb_err)?;
        let id = meta
            .get("next_job_id")
            .map_err(redb_err)?
            .map_or(0, |g| g.value());
        meta.insert("next_job_id", id + 1).map_err(redb_err)?;
        drop(meta);
        self.jobs_materialized += 1;
        self.put(
            UNSCOPED,
            &crate::job::JobRow {
                id: crate::table::Id::from_raw(id),
                name: name.to_string(),
                payload: payload.to_vec(),
                key: key.map(str::to_string),
                status: crate::job::JobStatus::Pending,
                attempts: 0,
                due_at: 0,
                last_error: None,
                shape,
                scope,
            },
        )?;
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
        shape: Option<u64>,
    ) -> io::Result<u64> {
        let mut meta = self.txn.open_table(META).map_err(redb_err)?;
        let id = meta
            .get("next_event_id")
            .map_err(redb_err)?
            .map_or(0, |g| g.value());
        meta.insert("next_event_id", id + 1).map_err(redb_err)?;
        drop(meta);
        self.events_materialized += 1;
        self.put(
            UNSCOPED,
            &crate::event::EventRow {
                id: crate::table::Id::from_raw(id),
                seq,
                name: name.to_string(),
                key: key.map(str::to_string),
                payload: payload.to_vec(),
                shape,
            },
        )?;
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
            if row_is_scoped::<R>() {
                let rows = self
                    .txn
                    .open_table(scoped_rows_def(R::TABLE))
                    .map_err(|e| scoped_open_err(R::TABLE, e))?;
                for item in rows.iter().map_err(redb_err)? {
                    let (k, v) = item.map_err(redb_err)?;
                    let row = R::decode_row(v.value())?;
                    let key_bytes = k.value();
                    let scope = u64::from_be_bytes(
                        key_bytes[..8]
                            .try_into()
                            .expect("scoped key carries a leading 8-byte scope"),
                    );
                    let id = u64::from_be_bytes(
                        key_bytes[8..]
                            .try_into()
                            .expect("scoped key carries a trailing 8-byte id"),
                    );
                    let mut key = Vec::new();
                    (index.key)(&row, &mut key);
                    entries.push(scoped_index_entry(scope, key, id));
                }
            } else {
                let rows = self
                    .txn
                    .open_table(rows_def(R::TABLE))
                    .map_err(|e| unscoped_open_err(R::TABLE, e))?;
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
    /// by the close-out trigger. `scope`: `Some(s)` picks `scoped_rows_def`
    /// (composite key `(s, id)`); `None` picks `rows_def` (bare `id`) — the
    /// `table: &str`-keyed primitives below have no `Row` type to ask
    /// `row_is_scoped::<R>()` of, so the caller (which already looked the
    /// table up in `Schema` for other reasons at every call site) resolves
    /// it from registration. `Option<u64>` makes "scoped but called with no
    /// scope" and "unscoped but called with a scope" unrepresentable rather
    /// than caught by an assert after the fact.
    pub fn put_raw(
        &mut self,
        table: &str,
        scope: Option<u64>,
        id: u64,
        row: &[u8],
    ) -> io::Result<()> {
        match scope {
            Some(scope) => {
                let mut rows = self
                    .txn
                    .open_table(scoped_rows_def(table))
                    .map_err(|e| scoped_open_err(table, e))?;
                rows.insert(scoped_key(scope, id), row).map_err(redb_err)?;
            }
            None => {
                let mut rows = self
                    .txn
                    .open_table(rows_def(table))
                    .map_err(|e| unscoped_open_err(table, e))?;
                rows.insert(id, row).map_err(redb_err)?;
            }
        }
        Ok(())
    }

    /// Decode-free primary-row removal, the delete counterpart to `put_raw`
    /// (spec §2's decode-free rebuild replay): a below-version row was written
    /// via `put_raw` and carries no index entries, so removing the primary row
    /// is sufficient — the old value is never decoded (decoding it under the
    /// compiled type is exactly the misdecode this path exists to avoid).
    /// `scope`: see `put_raw`.
    pub fn delete_raw(&mut self, table: &str, scope: Option<u64>, id: u64) -> io::Result<()> {
        match scope {
            Some(scope) => {
                let mut rows = self
                    .txn
                    .open_table(scoped_rows_def(table))
                    .map_err(|e| scoped_open_err(table, e))?;
                rows.remove(scoped_key(scope, id)).map_err(redb_err)?;
            }
            None => {
                let mut rows = self
                    .txn
                    .open_table(rows_def(table))
                    .map_err(|e| unscoped_open_err(table, e))?;
                rows.remove(id).map_err(redb_err)?;
            }
        }
        Ok(())
    }

    /// Raw read of one row's stored bytes (apply-side helper for the
    /// close-out trigger's old-row peek). `scope`: see `put_raw`.
    pub fn get_raw(
        &mut self,
        table: &str,
        scope: Option<u64>,
        id: u64,
    ) -> io::Result<Option<Vec<u8>>> {
        match scope {
            Some(scope) => {
                let rows = match self.txn.open_table(scoped_rows_def(table)) {
                    Ok(rows) => rows,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(e) => return Err(scoped_open_err(table, e)),
                };
                Ok(rows
                    .get(scoped_key(scope, id))
                    .map_err(redb_err)?
                    .map(|g| g.value().to_vec()))
            }
            None => {
                let rows = match self.txn.open_table(rows_def(table)) {
                    Ok(rows) => rows,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(e) => return Err(unscoped_open_err(table, e)),
                };
                Ok(rows.get(id).map_err(redb_err)?.map(|g| g.value().to_vec()))
            }
        }
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

    fn replace_row<R: Row>(&mut self, scope: u64, row: &R, id: u64) -> io::Result<Option<R>> {
        let mut encoded = Vec::new();
        row.encode_row(&mut encoded);
        if row_is_scoped::<R>() {
            let mut rows = self
                .txn
                .open_table(scoped_rows_def(R::TABLE))
                .map_err(|e| scoped_open_err(R::TABLE, e))?;
            let key = scoped_key(scope, id);
            let old = match rows.get(key).map_err(redb_err)? {
                Some(guard) => Some(R::decode_row(guard.value())?),
                None => None,
            };
            rows.insert(key, encoded.as_slice()).map_err(redb_err)?;
            Ok(old)
        } else {
            assert_eq!(
                scope,
                UNSCOPED,
                "Writer::put: table \"{}\" is not tenant-scoped (Row::Scope = Unscoped) but was \
                 called with scope {scope} — pass table::UNSCOPED for an unscoped table",
                R::TABLE
            );
            let mut rows = self
                .txn
                .open_table(rows_def(R::TABLE))
                .map_err(|e| unscoped_open_err(R::TABLE, e))?;
            let old = match rows.get(id).map_err(redb_err)? {
                Some(guard) => Some(R::decode_row(guard.value())?),
                None => None,
            };
            rows.insert(id, encoded.as_slice()).map_err(redb_err)?;
            Ok(old)
        }
    }
}

fn index_def(name: &str) -> TableDefinition<'_, &'static [u8], ()> {
    TableDefinition::new(name)
}

// ---------------------------------------------------------------- Snapshot

pub struct Snapshot {
    txn: redb::ReadTransaction,
}

/// `View`'s four reads without its `ReadableBy` bound, crate-private: the
/// bodies `Snapshot`'s `View` impl delegates to, and the snapshot reads
/// `Tx`'s `View` impl and `Tx::get_job` are built on. A `Tx` read generic
/// over `R: ReadableBy<Tx>` cannot prove `R: ReadableBy<Snapshot>`, so it
/// could not call `View` on its snapshot, and `get_job` reads `JobRow`,
/// which only these reach from inside a transaction.
impl Snapshot {
    pub(crate) fn get_any<R: UnscopedRow>(&self, id: Id<R>) -> io::Result<Option<R>> {
        let rows = match self.txn.open_table(rows_def(R::TABLE)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(unscoped_open_err(R::TABLE, e)),
        };
        match rows.get(id.raw()).map_err(redb_err)? {
            Some(guard) => Ok(Some(R::decode_row(guard.value())?)),
            None => Ok(None),
        }
    }

    pub(crate) fn scan_any<R: UnscopedRow>(&self) -> io::Result<Vec<R>> {
        let rows = match self.txn.open_table(rows_def(R::TABLE)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(unscoped_open_err(R::TABLE, e)),
        };
        let mut out = Vec::new();
        for entry in rows.range(0u64..=u64::MAX).map_err(redb_err)? {
            let (_, value) = entry.map_err(redb_err)?;
            out.push(R::decode_row(value.value())?);
        }
        Ok(out)
    }

    pub(crate) fn scan_by_any<R: UnscopedRow>(
        &self,
        index_name: &str,
        key: &impl Codec,
    ) -> io::Result<Vec<R>> {
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
            let row = self.get_any(id)?.unwrap_or_else(|| {
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
    pub(crate) fn scan_from_any<R: UnscopedRow>(
        &self,
        from_id: u64,
        limit: usize,
    ) -> io::Result<Vec<R>> {
        let rows = match self.txn.open_table(rows_def(R::TABLE)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(unscoped_open_err(R::TABLE, e)),
        };
        let mut out = Vec::new();
        for entry in rows.range(from_id..).map_err(redb_err)?.take(limit) {
            let (_, value) = entry.map_err(redb_err)?;
            out.push(R::decode_row(value.value())?);
        }
        Ok(out)
    }
}

impl View for Snapshot {
    fn get<R: UnscopedRow + ReadableBy<Self>>(&self, id: Id<R>) -> io::Result<Option<R>> {
        self.get_any(id)
    }

    fn scan<R: UnscopedRow + ReadableBy<Self>>(&self) -> io::Result<Vec<R>> {
        self.scan_any()
    }

    fn scan_by<R: UnscopedRow + ReadableBy<Self>>(
        &self,
        index_name: &str,
        key: &impl Codec,
    ) -> io::Result<Vec<R>> {
        self.scan_by_any(index_name, key)
    }

    fn scan_from<R: UnscopedRow + ReadableBy<Self>>(
        &self,
        from_id: u64,
        limit: usize,
    ) -> io::Result<Vec<R>> {
        self.scan_from_any(from_id, limit)
    }
}

impl Snapshot {
    /// Task 5 (tenant scoping), ruling R28: `View`'s own `get`/`scan`/
    /// `scan_from` above keep their unscoped signatures unchanged. These
    /// four methods are the scope-taking primitives a tenant-scoped read
    /// path builds on, and the snapshot level `ScopedTx` reads through:
    /// inherent methods on `Snapshot`, not trait methods, so they sit beside
    /// `get`/`scan`/`scan_from` without touching the `View` trait.
    ///
    /// # Trust boundary (ruling R48)
    ///
    /// These four read under a scope the CALLER supplies, with no
    /// authorization: there is no `Tenant::authorize` on this path and no
    /// hook to put one on, since a `Snapshot` carries no principal. The
    /// tenancy guarantee rests on what the framework hands app logic — a
    /// handler and a job receive a `Tx`, where a scoped table is unreachable
    /// without `Tx::open_scope` (`tx.scope::<Org>(org, ctx)?`) and the
    /// tenant's own check. Whoever holds a `Db` or a `Snapshot` is trusted as
    /// the operator, the same standing as whoever holds the redb file:
    /// `scan_raw`/`scan_raw_from`/`scan_index_raw` below read every tenant's
    /// bytes for the same holder, and stay `pub` because replication seeding
    /// and the archive need them.
    ///
    /// Each asserts `row_is_scoped::<R>()` (hard rule 4, negative space):
    /// calling one of these against an UNSCOPED table would open
    /// `scoped_rows_def`, a physical redb table the unscoped write path
    /// never creates, and silently read back empty instead of erroring —
    /// the wrong-answer-not-error shape `Writer::put`/`delete`'s own scope
    /// assertions exist to convert into a loud panic on the write side.
    pub fn get_scoped<R: Row>(&self, scope: u64, id: Id<R>) -> io::Result<Option<R>> {
        assert!(
            row_is_scoped::<R>(),
            "Snapshot::get_scoped: table \"{}\" is not tenant-scoped (Row::Scope = Unscoped) — \
             use View::get for an unscoped table",
            R::TABLE
        );
        let rows = match self.txn.open_table(scoped_rows_def(R::TABLE)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(scoped_open_err(R::TABLE, e)),
        };
        match rows.get(scoped_key(scope, id.raw())).map_err(redb_err)? {
            Some(guard) => Ok(Some(R::decode_row(guard.value())?)),
            None => Ok(None),
        }
    }

    /// Every row for ONE scope, in id order — `scan`'s prefix-bounded
    /// counterpart. The bound is exactly `scoped_key(scope, 0)..=
    /// scoped_key(scope, u64::MAX)`: since `scoped_key` orders by scope then
    /// id (pinned by `scoped_key_tests`), that range is precisely one
    /// tenant's contiguous slice of the composite-keyed table, and no other
    /// scope's row can fall inside it. The scope is the caller's word and
    /// nothing authorizes it — see `get_scoped`'s trust-boundary section.
    pub fn scan_scoped<R: Row>(&self, scope: u64) -> io::Result<Vec<R>> {
        assert!(
            row_is_scoped::<R>(),
            "Snapshot::scan_scoped: table \"{}\" is not tenant-scoped (Row::Scope = Unscoped) \
             — use View::scan for an unscoped table",
            R::TABLE
        );
        let rows = match self.txn.open_table(scoped_rows_def(R::TABLE)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(scoped_open_err(R::TABLE, e)),
        };
        let mut out = Vec::new();
        for entry in rows
            .range(scoped_key(scope, 0)..=scoped_key(scope, u64::MAX))
            .map_err(redb_err)?
        {
            let (_, value) = entry.map_err(redb_err)?;
            out.push(R::decode_row(value.value())?);
        }
        Ok(out)
    }

    /// Rows with `id >= from_id` for ONE scope, in id order, at most
    /// `limit` — `scan_from`'s prefix-bounded counterpart. Same silent-gap
    /// semantics as `View::scan_from` (see its doc comment): a GC'd
    /// `from_id` starts at the next surviving row in THIS scope rather than
    /// erroring or spilling into another tenant's range. The scope is the
    /// caller's word and nothing authorizes it — see `get_scoped`'s
    /// trust-boundary section.
    pub fn scan_from_scoped<R: Row>(
        &self,
        scope: u64,
        from_id: u64,
        limit: usize,
    ) -> io::Result<Vec<R>> {
        assert!(
            row_is_scoped::<R>(),
            "Snapshot::scan_from_scoped: table \"{}\" is not tenant-scoped (Row::Scope = \
             Unscoped) — use View::scan_from for an unscoped table",
            R::TABLE
        );
        let rows = match self.txn.open_table(scoped_rows_def(R::TABLE)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(scoped_open_err(R::TABLE, e)),
        };
        let mut out = Vec::new();
        for entry in rows
            .range(scoped_key(scope, from_id)..=scoped_key(scope, u64::MAX))
            .map_err(redb_err)?
            .take(limit)
        {
            let (_, value) = entry.map_err(redb_err)?;
            out.push(R::decode_row(value.value())?);
        }
        Ok(out)
    }

    /// `scan_by`'s scope-taking counterpart — the fourth scope-taking
    /// primitive beside `get_scoped`/`scan_scoped`/`scan_from_scoped` above
    /// (see their shared doc comment on `get_scoped` for why these are
    /// inherent methods rather than `View` trait ones). Exact-match
    /// secondary index lookup bounded to one tenant's slice of the shared
    /// index table: `scoped_index_entry` prefixes every scoped row's
    /// entries with `scope` big-endian, so
    /// `scoped_index_entry(scope, k, 0)..=scoped_index_entry(scope, k,
    /// u64::MAX)` is precisely that tenant's entries for key `k` and no
    /// other tenant's — same range-is-a-tenant's-slice reasoning as
    /// `scoped_key`/`scan_scoped`. `View::scan_by` is unchanged and stays
    /// the read path for an unscoped table's index. The scope is the caller's
    /// word and nothing authorizes it — see `get_scoped`'s trust-boundary
    /// section.
    pub fn scan_by_scoped<R: Row>(
        &self,
        scope: u64,
        index_name: &str,
        key: &impl Codec,
    ) -> io::Result<Vec<R>> {
        assert!(
            row_is_scoped::<R>(),
            "Snapshot::scan_by_scoped: table \"{}\" is not tenant-scoped (Row::Scope = \
             Unscoped) — use View::scan_by for an unscoped table",
            R::TABLE
        );
        let index = R::INDEXES
            .iter()
            .find(|i| i.name == index_name)
            .unwrap_or_else(|| {
                panic!(
                    "no index named \"{index_name}\" on table {} — declare it with #[index] (check \
                 the field's index name)",
                    R::TABLE
                )
            });
        let table = match self.txn.open_table(index_def(&index_table_name(index))) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(redb_err(e)),
        };
        let mut encoded = Vec::new();
        key.encode(&mut encoded);
        let lo = scoped_index_entry(scope, encoded.clone(), 0);
        let hi = scoped_index_entry(scope, encoded, u64::MAX);
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
            let row = self.get_scoped(scope, id)?.unwrap_or_else(|| {
                panic!(
                    "index \"{}\" entry points at missing {} row {id:?} in scope {scope} — \
                     index corruption",
                    index.name,
                    R::TABLE
                )
            });
            out.push(row);
        }
        Ok(out)
    }

    /// Every entry's raw key bytes for one table's named index, in stored
    /// (byte-lexicographic) order — `scan_raw`'s counterpart for a
    /// secondary index rather than a table's primary rows, and for the
    /// identical reason (see `scan_raw`'s own doc comment): `scan_by`/
    /// `scan_by_scoped` decode a row back out through `get`/`get_scoped` and
    /// never look at the key that pointed at it, so a round trip through
    /// them proves nothing about the key's own byte layout. Takes `R` and
    /// an index name rather than a raw table-name string, so
    /// `index_table_name`'s `"{TABLE}.{index}"` format stays a private
    /// convention rather than part of the public contract. Empty, not an
    /// error, when the index table has never been created.
    ///
    /// Reads every tenant's entries, with no authorization and no scope
    /// argument to constrain it (ruling R48): the tenancy guarantee covers
    /// the typed surface the framework hands app logic — `Tx` in handlers and
    /// jobs — and a holder of `Db` or `Snapshot` is trusted as the operator.
    pub fn scan_index_raw<R: Row>(&self, index_name: &str) -> io::Result<Vec<Vec<u8>>> {
        let index = R::INDEXES
            .iter()
            .find(|i| i.name == index_name)
            .unwrap_or_else(|| {
                panic!(
                    "no index named \"{index_name}\" on table {} — declare it with #[index] (check \
                 the field's index name)",
                    R::TABLE
                )
            });
        let table = match self.txn.open_table(index_def(&index_table_name(index))) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(redb_err(e)),
        };
        let mut out = Vec::new();
        for entry in table.iter().map_err(redb_err)? {
            let (key, _) = entry.map_err(redb_err)?;
            out.push(key.value().to_vec());
        }
        Ok(out)
    }

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

    /// The smallest stored `EventRow` id, read off the key alone: the row's
    /// bytes are never decoded, so a corrupt row at the retention floor fails
    /// only the tail whose scan reaches it, never the floor read every tail
    /// and every checkpoint makes first. The same range walk `scan_from(0, 1)`
    /// makes, so the pages it reads are the same.
    pub fn first_event_id(&self) -> io::Result<Option<u64>> {
        let rows = match self.txn.open_table(rows_def(crate::event::EventRow::TABLE)) {
            Ok(rows) => rows,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(unscoped_open_err(crate::event::EventRow::TABLE, e)),
        };
        match rows.range(0u64..).map_err(redb_err)?.next() {
            Some(entry) => Ok(Some(entry.map_err(redb_err)?.0.value())),
            None => Ok(None),
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

    /// Every row's scope, id and raw encoded bytes, in stored key order —
    /// the logical export primitive (`Seeder::rows` re-applies them through
    /// the typed ops on the import side). Empty, not an error, when the
    /// table has never been created. `scope` is `table::UNSCOPED` for every
    /// row of an unscoped table.
    ///
    /// `scoped` (from `TableOps.scoped`, i.e. the compiled schema) picks
    /// which physical shape to open. Scoping is opt-in per table and never
    /// retrofitted onto an existing one (spec §8's non-goal): a table's
    /// physical shape is fixed at its first write and stays that shape for
    /// the table's whole life, so `scoped` and the physical shape must
    /// agree. A `TableTypeMismatch` (the schema says one shape, the disk
    /// holds the other) is refused by name rather than tolerated either
    /// direction — `scoped = true` names the scope-flip contract and its
    /// fix; `scoped = false` names both shapes so the operator knows what
    /// disagreed.
    ///
    /// Reads every tenant's bytes at once, with no authorization (ruling
    /// R48): the tenancy guarantee covers the typed surface the framework
    /// hands app logic — `Tx` in handlers and jobs, where a scoped table is
    /// unreachable without `Tx::open_scope` and the tenant's own check — and
    /// a holder of `Db` or `Snapshot` is trusted as the operator. This method
    /// stays `pub` because replication seeding and the archive read the whole
    /// store by definition.
    pub fn scan_raw(&self, table: &str, scoped: bool) -> io::Result<Vec<(u64, u64, Vec<u8>)>> {
        if scoped {
            match self.txn.open_table(scoped_rows_def(table)) {
                Ok(rows) => {
                    let mut out = Vec::new();
                    for entry in rows
                        .range(scoped_key(0, 0)..=scoped_key(u64::MAX, u64::MAX))
                        .map_err(redb_err)?
                    {
                        let (key, value) = entry.map_err(redb_err)?;
                        let (scope, id) = split_scoped_key(key.value());
                        out.push((scope, id, value.value().to_vec()));
                    }
                    Ok(out)
                }
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(Vec::new()),
                Err(e) => Err(scoped_open_err(table, e)),
            }
        } else {
            match self.txn.open_table(rows_def(table)) {
                Ok(rows) => {
                    let mut out = Vec::new();
                    for entry in rows.range(0u64..=u64::MAX).map_err(redb_err)? {
                        let (key, value) = entry.map_err(redb_err)?;
                        out.push((UNSCOPED, key.value(), value.value().to_vec()));
                    }
                    Ok(out)
                }
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(Vec::new()),
                Err(e @ redb::TableError::TableTypeMismatch { .. }) => {
                    Err(unscoped_open_err(table, e))
                }
                Err(e) => Err(scan_raw_shape_error(table, e)),
            }
        }
    }

    /// Raw rows strictly after `after` (all rows when `None`), ascending by
    /// stored key, at most `limit` — the migration executor's bounded batch
    /// read (spec §3: batching must bound memory, not just record size) and
    /// the archive export walk's paged read (`Archive::step_snapshotting`).
    ///
    /// `after` is `(scope, id)`: a bare id cursor cannot resume a composite-
    /// keyed table correctly, since ids are NOT globally ascending across a
    /// scope boundary (rows sort by `(scope, id)`, so tenant 2's id 0 can
    /// follow tenant 1's id `u64::MAX`) — a bare-id watermark would revisit
    /// or skip rows across scopes. Both branches SEEK (an exclusive
    /// `redb::Bound`, not a full scan filtered post-hoc).
    ///
    /// `scoped` (see `scan_raw`) picks which physical shape to open, and a
    /// `TableTypeMismatch` is refused by name exactly as `scan_raw`
    /// refuses one — never tolerated, either direction. It also walks every
    /// tenant with no authorization, on `scan_raw`'s trust boundary and for
    /// the same reason (ruling R48).
    pub fn scan_raw_from(
        &self,
        table: &str,
        scoped: bool,
        after: Option<(u64, u64)>,
        limit: usize,
    ) -> io::Result<Vec<(u64, u64, Vec<u8>)>> {
        if scoped {
            match self.txn.open_table(scoped_rows_def(table)) {
                Ok(rows) => {
                    let lo = match after {
                        None => Bound::Unbounded,
                        Some((scope, id)) => Bound::Excluded(scoped_key(scope, id)),
                    };
                    let hi = Bound::Included(scoped_key(u64::MAX, u64::MAX));
                    let mut out = Vec::new();
                    for item in rows.range((lo, hi)).map_err(redb_err)? {
                        if out.len() == limit {
                            break;
                        }
                        let (k, v) = item.map_err(redb_err)?;
                        let (scope, id) = split_scoped_key(k.value());
                        out.push((scope, id, v.value().to_vec()));
                    }
                    Ok(out)
                }
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(Vec::new()),
                Err(e) => Err(scoped_open_err(table, e)),
            }
        } else {
            match self.txn.open_table(rows_def(table)) {
                Ok(rows) => {
                    let lo = match after {
                        None => Bound::Unbounded,
                        Some((scope, id)) => {
                            assert_eq!(
                                scope, UNSCOPED,
                                "scan_raw_from: table \"{table}\" is not tenant-scoped, but the \
                                 cursor carries scope {scope} — a cursor this function returns \
                                 for an unscoped table always carries table::UNSCOPED"
                            );
                            Bound::Excluded(id)
                        }
                    };
                    let mut out = Vec::new();
                    for item in rows.range((lo, Bound::Unbounded)).map_err(redb_err)? {
                        if out.len() == limit {
                            break;
                        }
                        let (k, v) = item.map_err(redb_err)?;
                        out.push((UNSCOPED, k.value(), v.value().to_vec()));
                    }
                    Ok(out)
                }
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(Vec::new()),
                Err(e @ redb::TableError::TableTypeMismatch { .. }) => {
                    Err(unscoped_open_err(table, e))
                }
                Err(e) => Err(scan_raw_shape_error(table, e)),
            }
        }
    }
}

// ---------------------------------------------------------------- Backend

/// redb's `StorageBackend` over a `File` — the mount point that keeps all of
/// redb's IO inside the simulation boundary.
struct DiskBackend {
    file: Box<dyn File>,
    /// Set whenever an operation below returns `Err`. redb's own
    /// `CheckedBackend` latches on this condition (redb-4.3.0
    /// cached_file.rs:218-261), so this mirrors redb's `io_failed` rather
    /// than inferring it from a downstream error. Two divergences, both
    /// deliberate: redb's `close()` also sets `io_failed` (:212) — not
    /// mirrored, a graceful close is not a failure; and redb's
    /// `write_best_effort` (:266) calls this same `write` without latching —
    /// mirrored anyway, since the call is indistinguishable here, which is
    /// the conservative direction (DESIGN §4, the 4.3.0 census re-walk).
    latch: Arc<AtomicBool>,
}

impl fmt::Debug for DiskBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DiskBackend")
    }
}

impl DiskBackend {
    /// Mirrors redb's latch: set on `Err`, never cleared. The error goes up
    /// wrapped in `FromDisk`, which is how `open_error` tells it from an
    /// `io::Error` redb builds itself.
    fn note<T>(&self, r: Result<T, io::Error>) -> Result<T, io::Error> {
        r.map_err(|e| {
            self.latch.store(true, Ordering::SeqCst);
            io::Error::new(e.kind(), FromDisk(e))
        })
    }
}

/// An error `DiskBackend` passed up from the `Disk`. redb's own "Not a redb
/// database" verdict is a `StorageError::Io` too, so without this wrapper an
/// injected or real IO error could pass for a verdict on the bytes. redb moves
/// a backend's error into `StorageError::Io` without rebuilding it
/// (redb-4.3.0 cached_file.rs:218-261, `map_err(StorageError::from)`), so the
/// wrapper survives to `open_error`; `Display` and kind are the inner error's,
/// so every message that reads it is unchanged.
#[derive(Debug)]
struct FromDisk(io::Error);

impl fmt::Display for FromDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for FromDisk {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
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

    // Each of redb's five latch sites (redb-4.3.0 cached_file.rs:222/231/240/
    // 249/258) has exactly one DiskBackend counterpart. If redb grows a sixth, the tripwire
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
    /// (redb-4.3.0 cached_file.rs:210-216), because it is reachable only
    /// from the database's close, so the flag it sets can never be read by a
    /// live `Store`. This is the guard on that divergence — it reddens the moment a
    /// `close` override routes anything through `note`. It has to live here:
    /// the same claim is unassertable through `Store`, since `Store::open`
    /// mints a fresh flag and any re-opened store reads clean by construction.
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
