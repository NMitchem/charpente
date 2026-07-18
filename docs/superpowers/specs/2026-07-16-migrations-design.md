# M1 sub-project 5 — migrations: design

Date: 2026-07-16. Noah-approved scope and architecture (this session). Fifth of M1's
six sub-projects (decomposition in 2026-07-07-wal-segment-management-design.md).

**Revised 2026-07-17** after a four-lens adversarial review of the first implementation
plan (dossier: .superpowers/sdd/migrations-plan-review.md) confirmed the original §2/§3
architecture had two decode-under-the-wrong-version paths — WAL-tail replay under the
new binary, and `Writer::replace_row`'s unconditional old-value decode — plus a
replication hole (gate writes at `Db::open` made fresh followers Hello positioned,
refused by the diverged machinery). Three Noah rulings resolve them: the **upgrade
barrier** (refuse-by-name over a foreign tail, never version-aware replay), the
**role-aware gate** (followers receive migration via shipping, never migrate locally),
and **adoption-at-first-write** (schema rows piggyback on first data writes, opens write
nothing). The compile-time enforcement the original promised is restored to the macro.

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

The review added a fourth, subtler instance of the same disease *inside the original
design*: recovery replay and index maintenance both decode stored bytes under the
compiled type, so any migration mechanism that leaves old-version bytes reachable by
those paths (a WAL tail across an upgrade; a store row mid-rewrite) re-creates the
bug it exists to fix. The revised mechanism's organizing rule: **no code path ever
decodes row bytes under any version other than the one they were encoded at.** Where
that cannot be guaranteed, the node refuses by name instead of proceeding.

PLAN.md: "Migrations: schema versions as log records; compile-time incompatibility
detection." DESIGN.md §3 (sketch, not yet mechanism): "A schema change generates a
migration as a log record (schema versions live in the log like everything else).
Incompatible changes — dropping a field a query still uses — fail at compile time
because the query is code referencing the field." That sketch covers *app code*
referencing a dropped field (ordinary Rust field-access compilation — already true,
nothing to build); it does not cover the risks above, which are this design's subject.

## Scope (Noah rulings, 2026-07-16; revised rulings 2026-07-17)

**In:** safe single-node evolution of `#[table]` row schemas — add a field (with a
required default), remove a field, retype a field via an explicit conversion. Eager,
upfront rewrite gated before the node serves traffic, running only on a node that is
*writable* (single-node/primary at `Db::open`; a follower only at promote). Compile-time
enforcement of the migration chain via a dedicated trait **enforced by the macro** (a
version bump without the impl does not compile), plus a fingerprint check catching a
version bump that was forgotten. The **upgrade barrier**: a version-bumped binary
refuses by name to open data whose WAL tail contains ordinary records it cannot decode.
A replication-side piece: a schema-identity digest in `Hello`, refused-by-name on
mismatch, and followers receiving the primary's migration through ordinary log
shipping.

**Out (explicit non-goals):** table rename, table split/merge. `#[job]`/`#[event]`
payload schema evolution — same underlying risk (both go through the same codec and
both persist for a while: queued jobs, retained events up to `event_retention`), but
the durability exposure is smaller (bounded by queue depth / retention count, not
"forever" the way table rows are) and the mechanism below is table-shaped enough that
extending it is a follow-up, not free — recorded here so it isn't silently dropped.
Mixed-version rolling upgrades — the digest check refuses a mismatch loudly; it does
not enable serving reads from a stale-schema follower while the primary is ahead.
Lazy/versioned per-row decode — rejected (2026-07-16) in favor of eager rewrite, and
re-rejected (2026-07-17) in its "version-stamped replay" form: it would put
migration-chain logic inside recovery, the most-proven path in the system, and carry
multi-version decode indefinitely. The barrier costs an honest operator step in the
rare crash-then-upgrade case; version-stamped replay costs permanent complexity in the
hot path. We take the barrier.

## 1. Data model & versioning

`#[table(version = N)]` on a `#[table]` struct; omitted means `N = 1` (all existing
tables are version 1 with no code change required). Bumping to version `N >= 2`
requires, **or the crate does not compile**:

- The previous version's struct kept in code under an explicit name (e.g. rename the
  old struct to `UserV1`, define the new one as `User`), marked with the smaller
  `#[row_shape(version = N-1)]` macro — codec only, no table identity, no indexes,
  no redb registration.
- `impl TableMigration for User { type Prev = UserV1; fn migrate(old: UserV1) ->
  Result<User, MigrationError>; }` — a dedicated trait rather than `From`/`TryFrom`,
  so the macro pins exactly which version-pair an impl covers and so every migration
  has one uniform, always-fallible shape (`Result`), covering both infallible
  transforms (default-value adds, field drops) and fallible ones (retype conversions
  that can reject specific values).

The compile-time half is the macro's job: for `version >= 2`, `#[table]` emits an
assert-impl const (`const _: () = { fn assert<T: TableMigration>() {} let _ =
assert::<Self>; };`-shaped) so a bump without the impl is a missing-trait-impl error
at the struct, not a runtime surprise. Registering the step with the `Schema` builder
(`.migration::<User>("User")`) remains a runtime concern; forgetting it is caught by
the gate's missing-chain-link refusal (§5) — the layering is stated honestly: impl
existence is compile-time, chain completeness at the deployed data's version is
runtime-refused by name.

The macro computes a **structural fingerprint** per version: a fixed, deterministic
64-bit hash (FNV-1a, hand-rolled and owned by this repo — `DefaultHasher`'s algorithm
is not a cross-toolchain stability guarantee; no crypto dependency, no randomized
hashing per hard rule 1 — a build-time constant from static field name/type
information) over the canonical `(field_name, type_name)` sequence, emitted on the
generated `impl Row` as `const FINGERPRINT: Option<u64> = Some(fp)`. `Row` gains
`VERSION: u32 = 1` and `FINGERPRINT: Option<u64> = None` as defaulted consts:
`None` marks a table outside the versioning system entirely (the hand-written
platform impls — `JobRow`, `EventRow`, `SchemaVersion` itself — need zero changes and
are skipped by the gate and excluded from the digest). Hand-writing `Row` in app code
therefore silently opts out of drift protection; the GUIDE states this in one line
and points at `#[table]`.

A new built-in platform table, `charpente.schema_versions`, persists each versioned
table's last-known state: `SchemaVersion { table: String, version: u32, fingerprint:
u64, migrating_through: Option<u64>, migrating_to: Option<u32> }`. Its row id is
derived deterministically from the table name (`Id::from_raw(fnv1a(name))`), making
lookup a direct `get` — no index needed (a deliberate simplification of the original
sketch's `#[index]`). `migrating_through`/`migrating_to` are `None` when idle and
`(last_row_id_done, target_version)` mid-migration (§3); recording the *target*
closes an interruption hole — see §2's in-flight-target refusal.

**Adoption-at-first-write (2026-07-17 ruling).** A table's `SchemaVersion` row is
written lazily: the first transaction that writes data to a versioned table carries
the adoption put `(compiled_version, compiled_fingerprint)` in the *same effects
record* — same disk-op count, so the frozen fault schedules of pinned seeds are
undisturbed. `Db::open` writes nothing unless it actually migrates. This also makes
the bootstrap disambiguation sound: because adoption always lands with or before a
table's first data row, "rows exist but no `schema_versions` row" can only mean data
that predates this feature, and is treated as the honest baseline `(1,
fingerprint(1))`. (The original design's write-at-open bootstrap would have misread a
table *created* at version 3 as version 1.) A table with zero rows and no row needs
no decision at all — nothing to migrate, nothing to protect; adoption waits for the
first write.

## 2. The gate: placement, checks, and the upgrade barrier

The gate runs when a node **becomes writable** — at `Db::open` for a single node or
a primary (the durable role file from sub-project 2 is on disk and readable at open;
file wins over config, always), and at **promote** for a follower, before it accepts
its first write. Followers never run the gate (§4). All gate *checks* are read-only
against the checkpointed store; the only gate *writes* are the migration itself.

Checks run **strictly before WAL replay** — this is the load-bearing reordering. The
danger the original design missed: replay materializes records into the store by
decoding row bytes under the *compiled* type, so a tail written by an old binary is
undecodable (add/remove field) or silently misdecoded (same-length change) before any
migration could run. The gate therefore reads `charpente.schema_versions` from the
checkpointed store, compares per table against the compiled `(version, fingerprint)`,
and classifies by scanning the un-replayed tail's record **envelopes only** (effect
kind tags — schema-independent framing; row bytes are never touched):

- **Same version, same fingerprint, everywhere** → normal open; the tail was written
  by a same-schema binary and replay proceeds as today.
- **Same version, different fingerprint** → refuse, named error: the struct's shape
  changed without bumping `#[table(version = ...)]`. Fired pre-replay — a drifted
  same-version binary must not replay either. This is the negative-space assertion
  closing the one hole compile-time checking cannot close: nothing *forces* a bump.
- **Compiled version < persisted version** → refuse, named error: this binary is
  older than the data (downgrade after a forward migration ran).
- **Compiled version > persisted, tail contains any ordinary records** → refuse,
  named error — the **upgrade barrier**: "unrecovered writes from the previous
  binary; run the previous binary once to a clean shutdown, then upgrade." An acked
  write in the tail is never bricked and never misdecoded; the remedy is complete
  because recovery checkpoints (below).
- **Compiled version > persisted, tail empty or containing only migration-kind
  records** (`MigratePut` + `charpente.schema_versions` puts — an interrupted
  migration) → if the recorded `migrating_to` is `Some(X)` and `X` differs from the
  compiled target, refuse by name ("an interrupted migration to version X is in
  flight; finish it with the binary that started it, or restore") — rows on either
  side of the watermark are at *different* versions and no single chain start covers
  both. Otherwise: replay the tail decode-free, verify the chain (missing link
  between persisted `V` and compiled `N` → refuse, named error identifying the
  `Vk -> Vk+1` gap), and run or resume the migration (§3).

Two checkpoints make the barrier a crash-then-upgrade-only event, with no new
operator verbs: the store checkpoints **on clean shutdown** and **at open after a
recovery that replayed records** (an open that replayed nothing checkpoints nothing —
keeping no-op opens free of new disk ops, for the same pinned-seed reason as
adoption-at-first-write). "Boot the previous binary once" is therefore the whole
remedy — it replays its own tail, checkpoints, and leaves nothing foreign behind.

This open-time persist advances `applied_seq` only — it must not run event-retention
GC or WAL segment GC. Those stay on the cadence-driven (`checkpoint_every`) schedule
the realtime tail oracle is validated against; running them at this extra, open-
triggered site perturbs the deterministic GC schedule (realtime seed
0xa2f2fa03568749d4).

Once a table is fully migrated to `N` in production, the `TableMigration`/
`#[row_shape]` code below `N` can be deleted from source; deleting it *before* every
deployed copy of the data has reached `N` is exactly what the missing-chain-link
error catches.

## 3. Migration execution: the `MigratePut` record kind

The rewrite is a sequence of ordinary `Db::transact` calls — no new commit path, so
it inherits the existing atomic-effects-record proof — but its row writes use a new
effect kind in the payload format (DESIGN §2), **`MigratePut(table, id, bytes)`**,
with one apply semantics everywhere (live commit, crash-replay, follower apply — the
same-shape rule): **overwrite the row's stored bytes without decoding the old value,
and leave indexes untouched.** This deletes the second decode-under-the-wrong-version
path: `Writer::replace_row`'s unconditional old-value decode (needed by ordinary
`Put` for index upkeep) never sees a mid-migration row, and a crash-resume replays
`MigratePut` records over still-old rows without ever interpreting them.

- Loop, `migration_batch_size` rows per transaction (`DbConfig`, default 1000, same
  style as `retain_segments`/`checkpoint_every` — bounds one commit's WAL record
  size): read the next batch via a **bounded range read** (`id > migrating_through`,
  ascending, limit batch-size — O(batch) memory; the original whole-table rescan is
  rejected, it made batching bound nothing). For each row: decode under version `V`'s
  decoder (the executor knows `V` — this is the one place old bytes are decoded, and
  only ever under their own version's codec), apply `TableMigration::migrate` up the
  chain to `N` (first `Err` becomes the named migration-transform-failure, citing
  table + row id + the conversion error; the whole batch transaction aborts, nothing
  partial commits), re-encode under `N`. Commit the batch's `MigratePut`s plus the
  watermark advance (`migrating_through = last id`, `migrating_to = N`) in the **same
  transaction** — a crash between them is impossible by construction.
- The closing transaction (batch shorter than `migration_batch_size`) sets
  `migrating_through/migrating_to = None` and bumps `(version, fingerprint)` to
  `(N, fingerprint(N))` — the single moment a table is considered migrated.
- **Indexes are rebuilt wholesale at close-out, triggered at apply.** Applying the
  close-out `SchemaVersion` record (the `V -> N` version transition) rebuilds that
  table's secondary indexes from its now-uniform rows. Index entries are derived,
  store-local data — never logged, deterministic, idempotent, safe to redo after a
  crash — and because the trigger is the *apply* of a log record, primary and
  followers rebuild at the same log position with no separate mechanism. Tables with
  no indexes: no-op.
- **Crash recovery is exactly resumption**: on restart, the gate finds `version`
  still `V`, `migrating_to = Some(N)` matching the compiled target, and
  `migrating_through` at the last committed batch; the tail's `MigratePut` records
  replay decode-free; the loop continues from the watermark. No row is transformed
  twice, none skipped.

App traffic never runs concurrently with a migration: the gate completes before the
node serves. Reads during the mixed-version window exist only on followers mid-
shipping — §4 states the safety rule.

## 4. Replication integration

`Hello` gains `schema_digest: u64` — one aggregate over every **fingerprinted**
table's `(name, version, fingerprint)` triple (tables with `FINGERPRINT: None` —
platform tables, hand-written impls — are excluded; they are covered by
`PROTO_VERSION`, not app schema), sorted by name and folded with the same FNV-1a
family, computed by each side from its own compiled `Schema`. A mismatch refuses the
connection by name, checked after the peer-secret proof and the `proto` check
(refuse-before-any-state-change ordering preserved), exactly like today's `proto`
refusal. Wire change is pre-release, appended after `secret`, no `PROTO_VERSION`
bump — auth T7's precedent; a mixed-build peer fails loudly on codec length, never
silently.

**Followers never migrate locally (2026-07-17 ruling).** The primary's migration is
ordinary log records; followers receive it through the **existing shipping path,
unchanged**, applying `MigratePut` decode-free and rebuilding indexes at the
close-out apply like any other record. Consequences, all by construction:

- A **fresh follower** runs no gate, writes nothing at open, Hellos
  `position: None`, and is snapshot-seeded — receiving the primary's
  `schema_versions` rows in the seed. (The original write-at-open gate gave fresh
  followers a positioned Hello on a private timeline, which d5baf7e's diverged
  machinery correctly refuses — every fresh follower of every real app would have
  been unable to join.)
- A **converged follower** across an upgrade — bump or no-bump — is simply behind:
  the primary's migration records sit after its position and ship normally. **No
  re-seed is ever required by migration.** The only re-seed rule is the pre-existing
  retention-window one, unchanged.
- A follower **promoted mid-migration** finds `migrating_through/migrating_to` set
  and resumes the remaining batches itself at promote (the gate's promote-time home);
  deterministic chains make its rows byte-identical to what the dead primary would
  have written. Normal promote timeline semantics (DECISIONS §6 bounded loss) apply.
- **Read-safety rule, stated:** while a table's migration records are in flight to a
  follower, that follower's copy is mixed-version and must not be read under the
  compiled type. v0 followers serve no typed app-table reads (jobs/cron are already
  follower-gated; realtime tails read `charpente.events`, untouched by table
  migration), so this is a documented invariant, re-examined if follower reads ever
  land.

Upgrade procedure (GUIDE): stop both nodes (mixed-version serving is out of scope and
the digest refuses it anyway), install the new binary on both, start the primary (it
migrates before serving), start followers (they catch up via shipping). No re-seeds,
no ordering hazards beyond "primary first".

## 5. Error surface (named errors, all pinned by tests **verbatim** — hard rule 5;
fragment pins are not acceptance)

- Fingerprint drift under an unchanged version number (table name, both fingerprints).
- Missing migration chain link (table name, the specific `Vk -> Vk+1` gap).
- Migration transform failure (table name, row id, the `TableMigration::migrate` error).
- Version downgrade (table name, persisted version, compiled version).
- **Upgrade barrier** (table name, persisted/compiled versions, the remedy: run the
  previous binary once to a clean shutdown, then upgrade) — including its
  in-flight-target variant (interrupted migration to X, compiled targets Y), and its
  in-flight-target-**fingerprint** variant: an interrupted migration to the compiled
  version but begun by a binary whose target shape differs (both fingerprints named;
  resuming would mix two shapes in one table). The original target fingerprint is read
  from the persisted `schema_versions` row — the executor writes the target's fingerprint
  from the first watermark on, so no extra field is needed (migrations T9 interlude).
- Replication schema-digest mismatch at `Hello` (refused by name, mirroring the
  `proto` mismatch text).

## 6. Testing strategy

- **Crash-at-any-point resumability**: inject crashes across the full fault matrix
  during a multi-batch migration, using the established harness idiom
  (`sim.restart()` between attempts, injected-fault classification via the checker
  library, bounded retry rounds — never an unclassified unbounded retry loop); the
  oracle is the **twin-universe comparison**: full row-content equality (not id-sets
  — id-set oracles miss double-transform corruption) against a from-scratch,
  uninterrupted migration of the same seed data.
- **Upgrade-barrier round-trip**: seed under V, leave the tail unreplayed, open under
  N → the named barrier refusal, byte-exact; "boot the old binary" (open under V —
  replay + checkpoint-at-open) → open under N migrates green. Plus the
  in-flight-target refusal (interrupt a v1→v2 migration, open a v3 binary).
- **Fingerprint-drift meta-test** (hard rule 3): a naive gate variant that skips the
  same-version fingerprint check, wired into a real open attempt, must wrongly open
  the drifted store — RED-proven against the real gate the way auth's raw-credential
  scan was. (A predicate compared against a constant-true function is a tautology,
  not a meta-test.)
- **Multi-step chain determinism**: v1→v2→v3 in one open produces byte-identical
  store contents to v1→v2 (restart) →v3 (restart).
- **Migration-transform-failure**: a deliberately-rejecting migrate on a specific row
  produces the named error and leaves the store at its pre-migration state.
- **Replicated `#[table]` app across the gate** (the scenario the original plan never
  covered): a fresh follower seeds from a data-bearing migrated primary; a converged
  follower stopped before the upgrade catches up via shipped `MigratePut`s — content
  AND index oracles equal on both sides; a schema-mismatched peer refuses by digest,
  zero state touched (pin/GC/seed-counter-unmoved assertions, auth's pattern).
- **Promote-mid-migration**: kill the primary between batches, promote the follower,
  it resumes to the uninterrupted twin's final state.
- **Index-rebuild idempotency**: crash between the close-out apply and the next
  checkpoint; the redone rebuild is byte-identical.
- **Determinism**: a real `replays_identically` property over a migration universe —
  the claim "migration draws zero entropy" is verified, not asserted.

## 7. What changes where

- `charpente-core`: `RowShape`/`TableMigration`/`MigrationError`, `Row`'s defaulted
  `VERSION`/`FINGERPRINT`, the `SchemaVersion` platform table, the `MigratePut`
  effect kind in the payload format + its decode-free apply + the close-out index
  rebuild trigger, the pre-replay gate + barrier in `Db::open`, the executor,
  adoption-at-first-write in the commit path, checkpoint-on-clean-shutdown and
  checkpoint-at-open-after-recovery, `DbConfig::migration_batch_size`, the bounded
  range read.
- `charpente-macros`: `#[table]` gains `version`, the fingerprint const, and the
  `version >= 2` assert-impl (the compile error); the new `#[row_shape]` macro.
- `charpente` (repl/kernel): `Hello.schema_digest` + refusal; the promote-time gate
  hook; follower apply of `MigratePut` comes free via the shared apply path.
- `charpente-cli`: none (a `charpente describe` surfacing schema versions is the
  later introspection sub-project).
- Docs: DESIGN §3 sketch → mechanism (including the barrier and the organizing
  "never decode under a foreign version" rule); GUIDE gains the migrations section
  (worked add-a-field example, the upgrade procedure, the hand-written-`Row` opt-out
  warning); this doc records the deferred `#[job]`/`#[event]` follow-up.

## 8. Evidence bar

Every task lands with its suites green over fresh seeds; sim-found bugs land with
seeds pinned, per hard rule 3. **Pinned seeds are re-verified, not merely replayed**:
adoption-at-first-write is designed to leave fault schedules undisturbed (no new disk
ops), but any pinned regression whose universe's record contents change must be shown
to still exercise its historical bug (via its meta-check where one exists), or be
re-derived — "zero entropy" does not imply "unmoved interleavings", and a green
replay of a seed that no longer reproduces its bug is a silent coverage loss.
Close-out sweep: fresh batches ×2 for the migrations property file plus the touched
core suites (transactions, replication — the `Hello` change ripples there);
pinned-seed replays including both release-mode CI seeds; invoicing's 1024-universe
batch; full workspace + clippy + fmt; a recorded live demo — add a field to an
invoicing table, restart, watch existing rows come back populated with the default;
kill -9 the old binary and watch the new one print the barrier refusal, recover with
the old, upgrade clean; and a deliberately un-migrated follower's connect attempt
refused by digest, byte-exact.
