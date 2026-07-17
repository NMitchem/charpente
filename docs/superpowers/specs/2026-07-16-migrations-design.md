# M1 sub-project 5 — migrations: design

Date: 2026-07-16. Noah-approved scope and architecture (this session). Fifth of M1's
six sub-projects (decomposition in 2026-07-07-wal-segment-management-design.md).

## Context

Today, changing a `#[table]` struct's fields and recompiling is silently unsafe in
three independent ways, none of which anything catches:

- **Row codec is versionless and positional.** `#[table]`'s generated `encode_row`/
  `decode_row` walk struct fields in declaration order through a hand-rolled codec
  (no serde, no field names, no defaults — table.rs's own doc comment calls it
  "deterministic, versionless v0"). A length change is caught (`decode_row` errors on
  trailing/missing bytes, citing "schema drift or corruption"); a same-length field
  swap or retype is not — it silently misdecodes.
- **The store never re-validates row shape.** redb only ever sees `TableDefinition<u64,
  &[u8]>` — an opaque blob keyed by row id. All shape knowledge lives above redb in
  `Row::decode_row`. Existing on-disk bytes "open" fine under a changed struct; they
  just decode wrong (or error) on next read.
- **Replication ships raw row bytes with zero schema-identity check.** `Hello` carries
  a hardcoded `proto: u8` version, checked and refused-by-name on mismatch — but
  nothing about the *row* schema. A primary and follower compiled from divergent
  `#[table]` definitions connect today without complaint.

PLAN.md: "Migrations: schema versions as log records; compile-time incompatibility
detection." DESIGN.md §3 (sketch, not yet mechanism): "A schema change generates a
migration as a log record (schema versions live in the log like everything else).
Incompatible changes — dropping a field a query still uses — fail at compile time
because the query is code referencing the field." That sketch covers *app code*
referencing a dropped field (ordinary Rust field-access compilation — already true,
nothing to build); it does not cover the three risks above, which are this design's
actual subject.

## Scope (Noah rulings, 2026-07-16)

**In:** safe single-node evolution of `#[table]` row schemas — add a field (with a
required default), remove a field, retype a field via an explicit conversion. Eager,
upfront rewrite at `Db::open`, gated before the app serves traffic. Compile-time
enforcement of the migration chain via a dedicated trait, plus a fingerprint check at
`Db::open` catching a version bump that was forgotten. A smaller replication-side
piece: a schema-identity digest in `Hello`, refused-by-name on mismatch.

**Out (explicit non-goals):** table rename, table split/merge. `#[job]`/`#[event]`
payload schema evolution — same underlying risk (both go through the same codec and
both persist for a while: queued jobs, retained events up to `event_retention`), but
the durability exposure is smaller (bounded by queue depth / retention count, not
"forever" the way table rows are) and the mechanism below is table-shaped enough that
extending it is a follow-up, not free. Mixed-version rolling upgrades — the
replication digest check refuses a mismatch loudly; it does not enable serving reads
from a stale-schema follower while the primary is ahead. Lazy/versioned per-row
decode — rejected in favor of eager rewrite (see below); it would mean carrying
multi-version decode logic in `Store`/`Writer`/replication indefinitely, and a row
could sit un-upgraded for a table's whole lifetime.

## 1. Data model & versioning

`#[table(version = N)]` on a `#[table]` struct; omitted means `N = 1` (all existing
tables are version 1 with no code change required). Bumping to version `N` requires,
or the crate does not compile:

- The previous version's struct kept in code under an explicit name (e.g. rename the
  old struct to `UserV1`, define the new one as `User`).
- `impl TableMigration for User { type Prev = UserV1; fn migrate(old: UserV1) ->
  Result<User, MigrationError>; }` — a dedicated trait rather than `From`/`TryFrom`,
  so the macro pins exactly which version-pair an impl covers (two structs could
  otherwise have an ambiguous or accidental `From` between them) and so every
  migration has one uniform, always-fallible shape (`Result`), covering both
  infallible transforms (default-value adds, field drops) and fallible ones
  (retype conversions that can reject specific values).

The macro computes a **structural fingerprint** per version: a fixed, deterministic
64-bit hash (FNV-1a; no crypto dependency needed, no randomized-seed hashing per hard
rule 1 — this is a build-time constant computed from static field name/type
information, not runtime entropy) over the canonical `(field_name, type_name)`
sequence, emitted as `const SCHEMA_FINGERPRINT: u64` on the generated `impl Row`.

A new built-in platform table, `charpente.schema_versions` (`SchemaVersion { table:
String #[index], version: u32, fingerprint: u64, migrating_through: Option<u64> }`),
persists each table's last-known `(version, fingerprint)` — same meta-table pattern
as `next_job_id`/`next_event_id`. `migrating_through` is `None` when no migration is
in progress for that table and `Some(last_row_id_done)` mid-migration (§3).

## 2. Migration execution flow

Schema migration runs at `Db::open`, strictly **after** WAL recovery/replay (recovery
brings the store to a consistent, fully-applied state first) and strictly **before**
the app starts serving requests — no mixed-version state is ever visible to app code,
and app traffic never runs concurrently with a migration.

For each table registered in `Schema`, compare the compiled `(version, fingerprint)`
against the persisted row in `charpente.schema_versions`. No persisted row is exactly
one of two cases, disambiguated by whether the table already holds any rows: **zero
rows** (a genuinely new table, first `Db::open` ever) adopts the compiled version
directly — no migration chain required, since there is nothing to migrate; the
`schema_versions` row is written immediately as `(N, fingerprint(N))`.
**Non-zero rows with no `schema_versions` entry** (a table that predates this feature
— every table in every charpente app that exists today) is treated as `(1,
fingerprint(1))`, the honest baseline for data written before versioning existed.

- **Same version, different fingerprint** → refuse to open, named error: the struct's
  shape changed without bumping `#[table(version = ...)]`. This is the negative-space
  assertion closing the one hole compile-time trait-checking cannot close on its own
  — nothing *forces* a version bump, only a missing-migration-impl compile error when
  one *is* bumped.
- **Persisted version `V` < compiled version `N`** → run the migration chain (§3).
- **Persisted version `V` > compiled version `N`** → refuse to open, named error: this
  binary is older than the data (a downgrade after a forward migration already ran).
- **Missing chain link** (compiled code lacks a `TableMigration` impl bridging some
  `Vk -> Vk+1` between persisted `V` and compiled `N`) → refuse to open, named error
  identifying the missing version pair. (Expected once mature: after a table is fully
  migrated to `N` in production, the `TableMigration`/historical-struct code for
  versions below `N` can be deleted from source — nothing will ever need to decode
  them again. Deleting it *before* every deployed copy of the data has actually
  reached `N` is exactly what this error catches.)

## 3. Batching & resumability

The migration for one table is a sequence of ordinary `Db::transact` calls — no new
WAL record type, no new commit path. This is deliberate: it inherits crash-safety for
free from the existing "one transaction commits its effects record atomically" proof
(the same guarantee jobs and auth already lean on), rather than needing a new one.

- Loop, `MIGRATION_BATCH_SIZE` rows per transaction (`DbConfig` constant, default
  1000, same style as `retain_segments`/`checkpoint_every`): read the next batch of
  rows with `id > migrating_through` in ascending id order (redb's table is
  id-keyed; this is a stable, deterministic iteration order — it does not depend on
  ids having been *assigned* in that order, only on the row set being static for the
  duration of the migration, which it is, since app traffic is blocked). For each
  row: decode under version `V`'s decoder, apply `TableMigration::migrate` up the
  chain to `N` (propagating the first `Err` as a named migration-transform-failure,
  citing table + row id + the conversion error), re-encode under version `N`. Commit
  one transaction containing all of the batch's re-`put` rows plus the
  `migrating_through` watermark advance to the batch's last row id — **same
  transaction**, so a crash between them is impossible by construction.
- When a batch is shorter than `MIGRATION_BATCH_SIZE` (table exhausted), the closing
  transaction instead sets `migrating_through = None` and bumps `(version,
  fingerprint)` to `(N, fingerprint(N))` — this is the single moment a table is
  considered migrated.
- **Crash recovery is exactly resumption**, not a special case: on restart, `Db::open`
  re-reads `charpente.schema_versions`, finds `version` still `V` and
  `migrating_through` at wherever the last committed batch left it, and the loop
  above continues from there. No row is transformed twice, none is skipped — the
  watermark and the batch's row writes commit together or not at all.

## 4. Replication integration

`Hello` gains `schema_digest: u64` — one aggregate over every registered table's
`(name, version, fingerprint)` triple, sorted by name and folded with a fixed
deterministic combiner (same FNV-1a family as §1 — no ambient/randomized hashing),
computed locally by each side from its own compiled `Schema` at `Db::open`. A mismatch
refuses the connection by name, exactly like today's `proto` check (session.rs's
existing refusal pattern, extended).

Because the primary's eager migration is expressed as ordinary transactions (§3), its
resulting `Put` records ship to followers through the **existing** log-shipping path,
completely unchanged — no separate replication-side migration logic. What the digest
check buys is narrower and more honest: a follower whose compiled schema doesn't match
the primary's refuses the connection loudly instead of silently misdecoding replicated
bytes. It does not attempt to make a stale-schema follower safe to keep serving
reads — that would be a mixed-version rolling-upgrade guarantee, explicitly out of
scope. This is the same fail-stop direction realtime already committed to (encode
errors are whole-node-fatal, not silently dropped).

## 5. Error surface (named errors, all pinned by tests once implemented)

- Fingerprint drift under an unchanged version number (table name, both fingerprints).
- Missing migration chain link (table name, the specific `Vk -> Vk+1` gap).
- Migration transform failure (table name, row id, the `TableMigration::migrate`
  error).
- Version downgrade (table name, persisted version, compiled version).
- Replication schema-digest mismatch at `Hello` (refused by name, mirroring the
  `proto` mismatch text).

## 6. Testing strategy

- **Crash-at-any-point resumability**: inject crashes across the full fault matrix
  during a multi-batch migration; on restart, the migration completes with every row
  migrated exactly once (no loss, no duplication) — verified by comparing the final
  migrated store against a from-scratch, uninterrupted migration of the same seed
  data (a twin-universe determinism property, the established pattern in this repo).
- **Fingerprint-drift meta-test** (hard rule 3): a naive implementation that skips the
  same-version fingerprint check must fail — RED-proven against the checker the same
  way auth's raw-credential scan and realtime's skip-oracle were.
- **Multi-step chain determinism**: a table migrated v1→v2→v3 in one `Db::open` call
  produces byte-identical store contents to the same data migrated v1→v2 (restart)
  →v3 (restart) — the chain is associative regardless of where restarts land.
- **Migration-transform-failure**: a deliberately-rejecting `TableMigration::migrate`
  on a specific row produces the named error and leaves the store at its
  pre-migration state (transaction atomicity — nothing partially committed).
- **Replication digest property**: matching digests connect normally; a mismatch
  (one node's table recompiled with a drifted shape) refuses by name, zero state
  touched (mirrors the existing pin/GC/seed-counter-unmoved assertions from auth's
  peer-secret refusal tests).
- **Determinism check**: unchanged in shape from existing suites — migration draws no
  entropy (pure data transform + ordinary transactions), so no pinned seed's
  interleaving moves.

## 7. What changes where

- `charpente-macros`: `#[table]` gains the `version` attribute, `SCHEMA_FINGERPRINT`
  const generation, and the `TableMigration` trait-bound wiring (missing impl for a
  version bump becomes the compile error).
- `charpente-core`: `TableMigration` trait, the migration executor (batched
  `Db::transact` loop + watermark), `charpente.schema_versions` table, the version/
  fingerprint gate in `Db::open`.
- `charpente` (repl): `Hello.schema_digest` field (wire change, pre-release — no
  migration needed for the wire format itself) and the refusal.
- `charpente-cli`: none in this sub-project (a `charpente describe` surfacing schema
  versions is the later "error/introspection polish" sub-project per PLAN.md).
- Docs: DESIGN §3 goes from sketch to mechanism; GUIDE gains a migrations section
  with a worked add-a-field example; this design doc records the deferred `#[job]`/
  `#[event]` follow-up explicitly so it isn't silently dropped.

## 8. Evidence bar

Every task lands with its suites green over fresh seeds; sim-found bugs land with
seeds pinned, per hard rule 3. Close-out sweep: fresh batches ×2 for the new
migrations property file plus the core suites it touches (transactions, replication —
the `Hello` wire change ripples there); pinned-seed replays including both release-mode
CI seeds; invoicing's 1024-universe batch; full workspace + clippy + fmt; a recorded
live demo — add a field to an invoicing table, restart, watch existing rows come back
populated with the default, then a second demo where a follower's binary is
deliberately left un-migrated and its promote/connect attempt is refused by name.
