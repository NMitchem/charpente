# M2 sub-project — Tenant scoping as typed key prefixes: design

**Status:** design approved 2026-08-30 (brainstormed §1-§4 in session; this document is the
spec of record). Implementation plan: `docs/superpowers/plans/2026-08-30-tenant-scoping.md`.

## Context

`PLAN.md`'s M1 shipped auth and deferred one bullet with a one-line diagnosis that has
stood unchanged since: **"multi-tenancy is a convention today, not compiler-enforced."**
`DESIGN.md:1377-1381` carries the deferral and, in the same breath, decides most of this
design:

> Org/tenant scoping as a first-class key prefix — "every query is tenant-scoped" as a
> type-level property rather than a review checklist item — is a table-layer type-system
> feature, deferred to a design of its own; the session-subject pattern (and `emit_keyed`'s
> scoping key for realtime, §6) covers scoping until then.

Three rulings are already in force and are not reopened here:

1. **It is a table-layer type-system feature**, not an auth feature. The bullet sits in §8
   because that is where the gap was noticed; the sentence relocates the work.
2. **Enforcement is type-level.** "Rather than a review checklist item" forecloses a lint or
   a runtime guard as the primary mechanism. `DESIGN.md` §3: "every contract violation is a
   compile error."
3. **The platform never interprets the scoping key.** `DESIGN.md:1060-1062` already rules
   this for the sibling mechanism — `emit_keyed`'s key is "an opaque scoping filter (an org
   id, typically), carried but never interpreted inside the deterministic boundary." A
   platform that derived the tenant itself would contradict a ruling in force one subsystem
   over.

### What the tree actually looks like today

The census that grounds this design (all citations verified against `main` at `5bb7e99`):

- **Row keys are not byte-encoded and carry no prefix concept.** Namespacing is by physical
  redb table, named `R::TABLE`; the primary key is a bare `u64` handed to redb's native key
  type — `fn rows_def(table: &str) -> TableDefinition<'_, u64, &'static [u8]>`
  (`charpente-core/src/table.rs:346-348`).
- **Secondary index keys are hand-encoded** and already carry a byte layout built for range
  bounding: `[u32 le key len][key bytes][id: u64 be]` (`table.rs:354-361`), in tables named
  `format!("{}.{}", R::TABLE, index.name)` (`table.rs:350-352`).
- **Every read path is unbounded or id-bounded, never prefix-bounded.** `Snapshot::scan` is
  `rows.range(0u64..=u64::MAX)` (`table.rs:957-969`); `scan_from(from_id, limit)` is
  `rows.range(from_id..).take(limit)` (`table.rs:1011-1023`); `scan_by` ranges
  `index_entry(key, 0)..=index_entry(key, u64::MAX)` (`table.rs:971-1006`).
- **Staged writes are keyed `(table name, u64)`.**
  `GroupOverlay = BTreeMap<&'static str, BTreeMap<u64, Option<Vec<u8>>>>`
  (`charpente-core/src/db.rs:346-352`) — consumed by group commit and per-batch apply.
- **Ids are app-minted, not platform-minted.** There is no platform-wide `next_id`; only
  jobs and events get platform ids (`table.rs:785-838`). Apps mint by convention — e.g.
  `examples/invoicing/src/model.rs:146-156`.
- **The principal is already reachable inside a transaction.** `Ctx` is built *inside* the
  stage closure (`charpente/src/http/app.rs:701-712`), and `Ctx::principal()` is at
  `app.rs:92-94`; `Principal { subject, via }` at `charpente/src/auth/ladder.rs:26-29`.
  Nothing about auth blocks this feature — the block is entirely storage-side.

So "first-class key prefix" is not an extension of an existing prefix. There is no prefix.
This design introduces one, for opted-in tables only.

## Why this is the thesis, not a convenience

`MANIFESTO.md` §6 names the type system as "the oracle for the layer that has no torture
suite: business logic." A cross-tenant read is the highest-severity bug in that layer, and
it is the one charpente currently leaves to a reviewer noticing a missing filter. §4 and §5
describe the platform's signature move as the *structural deletion of a bug class* — the
dual-write bug is gone because there is one commit point, not because people are careful.
This applies the same move to tenancy: after this, a foreign row is not rejected, it is
**unaddressable**, because the tenant is part of the key required to name a row at all.

The honest boundary, stated here so it is never implied: **the platform guarantees
consistency, not authorization.** It cannot know that this principal belongs to org 7 — that
is an app-level `Membership` lookup, and ruling 3 above forbids the platform from
interpreting the key. What the feature buys is a collapse in audit surface: today a leak can
originate at any of N query sites; afterwards there is exactly **one** auditable line per
handler — the `scope()` call — and every query downstream of it is scoped by construction.

## Scope

**In:** the `Row::Scope` associated type and `#[table(scoped_by = …)]`; `ScopedTx`; the
`TenantScope` authorize contract; the composite row key and index prefix for scoped tables;
the overlay change; `#[route(…, cross_tenant)]` / `#[job(cross_tenant)]` and
`across_tenants`; scope-keyed event emission; the full oracle set in §5; GUIDE and DESIGN
updates in the same commits (hard rule 6).

**Out:** see §8.

## §1 The type surface

`Row` gains `type Scope`, defaulting to `Unscoped`. `#[table(scoped_by = Org)]` sets it.
`Tx`'s view methods bound on `R: Row<Scope = Unscoped>`; `ScopedTx<'_, S>`'s bound on
`R: Row<Scope = S>`. A scoped read from a bare `Tx` is therefore an unsatisfied trait bound,
not a runtime refusal, and carries a `#[diagnostic::on_unimplemented]` message naming the
contract, the site and the fix (hard rule 5).

```rust
#[table(scoped_by = Org)]
struct Invoice { id: Id<Invoice>, org: Id<Org>, total: Cents }

// Compile error without this impl — the same shape as a `version` bump with no
// `TableMigration` (DESIGN §3), enforced by a macro-emitted const assert.
impl TenantScope for Org {
    fn authorize(tx: &Tx, who: Option<&Principal>, org: Id<Org>) -> Result<(), ApiError> {
        // the app's own Membership lookup; the platform never interprets `org`
    }
}

#[route(GET "/orgs/:org/invoices", auth)]
fn list(tx: &mut Tx, org: Id<Org>, ctx: &Ctx) -> Result<Vec<Invoice>, ApiError> {
    let tx = tx.scope::<Org>(org, ctx)?;   // authorize() runs here, exactly once
    Ok(tx.scan::<Invoice>()?)               // cannot address another org
}
```

`ScopedTx<S>` derefs to `Tx`, so unscoped tables stay reachable inside a scoped handler —
opening a scope must not lock a handler out of global rows.

Requiring `TenantScope` to exist is what stops `scope()` degenerating into a cast. A handler
can still pass the wrong `org` into it; it cannot pass one that skipped the app's own
membership check, because the check is neither optional nor callable-by-forgetting.

## §2 Key encoding, the index prefix, and the overlay

**The composite key.** A scoped table's row key is the 16 bytes
`scope.to_be_bytes() ++ id.to_be_bytes()`. Big-endian is load-bearing: it makes
lexicographic byte order agree with `(scope, id)` numeric order, so a tenant's rows form one
contiguous run and a scoped scan is a single range seek rather than a full scan plus filter.
This follows the convention already in the file — `index_entry` encodes its trailing id
big-endian for exactly this reason (`table.rs:354-361`). Whether the `TableDefinition` key
type is `[u8; 16]` or `&[u8]` is pinned against redb's `Key` impls during implementation.

Every read path becomes prefix-bounded:

| path | today | scoped |
|---|---|---|
| `scan` | `range(0u64..=u64::MAX)` (`table.rs:957`) | `range((s,0)..=(s,u64::MAX))` |
| `scan_from` | `range(from_id..).take(n)` (`table.rs:1011`) | `range((s,from)..=(s,MAX)).take(n)` |
| `scan_by` | `index_entry(k,0)..=index_entry(k,MAX)` (`table.rs:971`) | the same, under a `[scope u64 be]` prefix |

**The index prefix lands in the same change as the row key.** Index keys become
`[scope be][u32 le keylen][key][id be]`. Shipping it later would leave `scan_by` as a
fully-functional cross-tenant read on a table the type system had already certified as
scoped — silent, and endorsed by the types. Index maintenance is at `table.rs:736-780` and
must be updated with it.

**The overlay.** `GroupOverlay` (`db.rs:346-352`) keys staged writes by `(table, u64)`, and
both group commit and per-batch apply consume it. The ruling:

- **Primary: widen the inner key to `(u64, u64)`**, slot 0 for unscoped tables. One code
  path. The risk is real and named: it doubles the BTreeMap key width in the hot merge path,
  changing comparison cost and node fanout — plausibly regressing the numbers per-batch
  apply just published (Disk ops 13.37 → 1.23 per log record).
- **Gate:** land it, re-run the kernel layer on the `docs/BENCH.md` protocol, and if the
  numbers move outside the established bracket, **fall back to a parallel scoped overlay**
  leaving the existing map untouched. The fallback is pre-authorized here so the choice is
  not made later under schedule pressure.

A parallel overlay does not violate one-commit-point: both maps merge into a single log
record. It is staging, not a second place things become true.

**Ruled (Task 1): three record shapes, not two, one per effect kind — neither posed world
holds uniformly.** `Effect` has five variants (`db.rs:315-339`); three carry app-table row
writes, and they do not agree with each other on whether the record carries a key. (The
other two, `Enqueue` and `Emit`, are a fourth answer worth naming rather than passing over:
their records carry no key at all — `materialize_job`/`materialize_event` (called at
`db.rs:876`, `db.rs:883`) land in `Writer::put` via `JobRow`/`EventRow` (`table.rs:785-800`,
`table.rs:815-838`), with the id generated at apply time from the `next_job_id`/
`next_event_id` meta counters rather than carried by the record at all. Neither is a scoped
table write, so out of scope for this ruling, but `Emit`'s record already carries a
length-prefixed `key: Option<String>` — encode `db.rs:401-406`, decode `db.rs:436-440` — and
that is a free win for §4: `tx.emit` auto-keying with the scope inside a `ScopedTx<Org>`
rides this existing field unchanged, World B, no format change needed there either.) The WAL
frame itself carries no record-schema field to hang anything on — `[seq][len][crc][payload]`
(`log.rs:4-5`) — so every finding below is about `db.rs`'s record schema, never `log.rs`'s
framing.

- **Put (tag 0) is World B.** The encoded record is `(table: String, row: Vec<u8>)` — no
  `id` field at all (`encode_record`'s `Some(row)` arm, `db.rs:382-386`; `decode_record`
  tag 0, `db.rs:423-426`). The row's bytes are opaque to the record schema; the key is
  recovered downstream, never carried by the record itself. The ordinary path fully decodes
  the row (`apply_put`, `db.rs:155-157`, via `R::decode_row`). A second, decode-free path
  used only for pre-migration replay (`is_below_compiled`, invoked at `db.rs:849-852`) skips
  that decode and instead reads the id as a raw 8-byte little-endian prefix of the row bytes
  (`row_id_prefix`, `db.rs:1046-1056`), trusting the convention recorded at `db.rs:1043-1045`
  that every `#[table]`/`#[row_shape]` struct encodes `id` first. That convention is not
  compiler-enforced: the `#[table]` macro requires a field named `id: Id<Self>` to exist
  (enforced by the `if !has_id` check, `charpente-macros/src/table.rs:75-82`) and checks its
  type when present (`charpente-macros/src/table.rs:41-59`) but never checks its position,
  and `encode_row` emits fields in struct declaration order with no reordering
  (`charpente-macros/src/table.rs:37-73` builds `field_idents` in that order;
  `charpente-macros/src/table.rs:138-140` emits `encode_row` from it verbatim) — an unrelated,
  pre-existing gap this project should not need to touch, but the scoping design leans on the
  same shortcut (see below), so it stops being purely unrelated.
- **Delete/tombstone (tag 1) is World A, narrowed.** The record is `(table: String, id:
  u64)` (`encode_record`'s `None` arm, `db.rs:387-391`; `decode_record` tag 1,
  `db.rs:427-430`). It has two consumers, not one, and Task 4 owes both. The ordinary path is
  `apply_delete::<R>` (`db.rs:159-161`) → `Writer::delete<R>(Id<R>)` (`table.rs:761-780`,
  which also removes that row's index entries, keyed by `id.raw()` at `table.rs:768-778` — a
  second place Task 4 must widen to the composite key, not just the record). Separately, the
  below-compiled-version branch is `db.rs:859-861` → `Writer::delete_raw(&mut self, table:
  &str, id: u64)` (`table.rs:888`), `delete_raw`'s only caller in the workspace. Both take a
  bare `u64`. This is the brief's World A shape minus the row bytes (there are none to
  delete) — `(table, id)`, with `id` a fixed 8-byte-LE primitive baked in by `int_codec!`
  (`table.rs:146-158`).
- **MigratePut (tag 4, migration-rewrite path) is World A exactly.** The record is
  `(table: String, id: u64, row: Vec<u8>)` (`encode_record`, `db.rs:407-412`;
  `decode_record` tag 4, `db.rs:441-445`), applied via `Writer::put_raw(&mut self, table:
  &str, id: u64, row: &[u8])` (`table.rs:877`).

**What this means for Task 4 and Task 12.**

- Put needs no record-schema change. A scoped table's composite key can ride through
  entirely inside the row bytes, because the record never looks at the key today.
- Delete and MigratePut *do* need a record-schema change for scoped tables, and it is a
  genuine log-format change reaching replication and archive/restore, per the brief's World
  A consequences — just scoped to these two effect kinds, not all three. Both encode `id` as
  a bit-for-bit `u64` at a fixed offset, and `decode_record` is not schema-aware: it calls
  `u64::decode` for `id` unconditionally, with no way today to know from the bytes alone that
  a given table's key is 16 bytes rather than 8. A decoder that reads the old 8-byte width
  where 16 were written does not just get the id wrong: for MigratePut it misaligns every
  subsequent byte, corrupting the `row` decode; for Delete the 8 stray bytes get consumed as
  the *next* loop iteration's tag. Either way the rest of the record's effects desynchronize,
  or the mismatch surfaces as the trailing-bytes guard firing (`db.rs:457-461`) — Delete is
  not the milder case, only the shorter one. But `decode_record` already refuses an unknown
  tag by name (`db.rs:446-454`): "this log was written by a newer record format; upgrade the
  binary or restore the old one" — the migrations idiom, already implemented, for free. That
  makes allocating new effect tags for the scoped Delete/MigratePut shapes the cheapest
  World-A encoding available: the tag itself is the version discriminator, and an old
  follower's refusal comes from code that already exists, with no `Schema` threaded into
  `decode_record` and no new machinery. **Task 4 should price that option first**, against
  the alternative of threading `Schema` into `decode_record` (it is already in scope one call
  site up, in `apply_batch` at `db.rs:784`) or making `id` self-describing length-prefixed
  bytes the way `table` and `row` already are. Task 12 owes a mixed-version replication
  oracle either way: an old follower fed a Delete or MigratePut record for a scoped table
  must refuse by name, not desync mid-record.
- Put's own decode-free replay path is the sharpest edge, and it is easy to miss precisely
  because it lives outside the record schema: `row_id_prefix` recovers only the row's own
  bare id (`Id<Invoice>`, 8 bytes) — never a scope. §1's worked example,
  `Invoice { id: Id<Invoice>, org: Id<Org>, total: Cents }`
  (`2026-08-30-tenant-scoping-design.md:97`), puts the scope in a *second* field, not folded
  into `id`. The composite storage key that a below-compiled-version scoped row needs at
  `w.put_raw` time is `(org, id)` — unrecoverable from a fixed byte prefix without decoding
  the struct, which is exactly what this path exists to avoid. Task 4 must decide how a
  below-compiled-version scoped row gets its storage key at replay time before this path can
  be trusted for any table that is both `scoped_by` and ever version-bumped; until it does,
  this is a correctness gap, not a record-format one.

**What provably does not change.** Unscoped tables keep `rows_def`'s `u64` definition and
their exact stored bytes. Every pinned seed's disk image and fault schedule is untouched, and
an app that never writes `scoped_by` compiles and runs byte-identically.

## §3 The global reader

**The rule: naming a tenant is always allowed; enumerating without naming one is a declared
capability.**

This falls out of §2 rather than being imposed on it. A point read already requires
`(scope, id)` to address a row, so a cross-tenant `get` is a `get` with the scope supplied —
explicit by construction, nothing to gate. Only a scan can enumerate without naming a
tenant.

```rust
#[route(GET "/admin/revenue", auth, cross_tenant)]
fn revenue(tx: &mut Tx, ctx: &Ctx) -> Result<Report, ApiError> {
    let rows: Vec<(Id<Org>, Invoice)> = tx.across_tenants::<Org>(ctx.cap())?.scan()?;
    …
}
```

The route attribute makes the glue pass a `CrossTenant` capability token; `across_tenants`
requires one. A handler without the attribute has no token in scope, so the call is a compile
error naming the missing declaration. `#[job(cross_tenant)]` does the same for background
rollups.

Two properties carry the weight. The result is `Vec<(Id<Org>, Invoice)>`, never
`Vec<Invoice>` — the tenant rides on every row, so a cross-tenant result cannot be quietly
handed to code expecting one tenant's data. And because the capability is declared on the
route, `charpente describe` can enumerate every endpoint in an app able to read across
tenants — turning "audit the cross-tenant reads" from a grep someone must remember into a
mechanical inventory, the same move `check-client` and `doc_inventory` already make.

Platform-internal crossers are unaffected *as capabilities* — none of them needs a new
authorization check. `migrate_schema()` rewrites rows through the raw path (`scan_raw_from`,
`table.rs:1116-1144`), below the `View` API: it must learn the new key shape, but needs no
capability. Replication and archive work at the log layer, and §2's ruling narrows that
claim rather than contradicting it: Delete and MigratePut records carry a fixed-width `id`
that must change shape for scoped tables, so replication and archive/restore inherit that
record-format change even though — the point of this paragraph — neither needs a new
capability check to carry it.

**On hard rule 2, argued rather than glossed.** `across_tenants` could be read as an escape
hatch. It is not. The guarantee this feature makes is *"a scoped query cannot silently cross
tenants."* `across_tenants` is not silent, not optional, and removes nothing from
`ScopedTx` — there is no flag that weakens the scoped path, which is what "optional
guarantees are no guarantees" targets. It is closer to `unsafe`: a named construct where the
obligation visibly moves, not a switch that disables the checker. The alternative — that
cross-tenant enumeration be inexpressible — is coherent but makes the platform unable to host
a billing rollup or an admin console, i.e. most real SaaS. The design must admit the
operation; the only question is whether it is loud enough. If a louder notch is wanted, the
capability can additionally be required at `App::mount`.

## §4 Realtime: one tenancy mechanism, not two

`emit_keyed`'s exact-match key (`db.rs:557-563`; `Subscription { entries, key, cursor }` at
`charpente/src/http/mod.rs:213-217`) is, per `DESIGN.md:1377-1381`, the stopgap this feature
replaces. Leaving the two mechanisms separate means they can disagree, and an app that has
locked down its queries can still leak through a subscription.

Therefore `tx.emit` inside a `ScopedTx<Org>` auto-keys with the scope. The oracle: a
subscription keyed to tenant A never receives an event emitted under scope B, under the full
fault matrix.

## §5 The oracles

**Compile-fail matrix (trybuild, `charpente-macros`).** Pinned `.stderr` for: a scoped `get`
on a bare `Tx`; `scoped_by = Org` with no `impl TenantScope for Org`; `across_tenants()` from
a route lacking `cross_tenant`; `ScopedTx<OrgA>` where `ScopedTx<OrgB>` is expected. Pinning
the text is what makes "a compile error, not a 500" a tested claim (DESIGN §7, hard rule 5).

**The flagship sim property.** Modeled on §8's credential property (scan every durable file
in the universe, assert no raw token appears). N tenants each write rows carrying a
tenant-unique marker; under the full fault matrix, every scoped read returns only its own
tenant's markers, including mid-recovery after a crash between any two writes. It must
exercise `get`, `scan`, `scan_from` **and** `scan_by` — `scan_by` is what catches a forgotten
index prefix, §2's worst failure mode.

**The naive-victim meta-test.** DESIGN §8 establishes the idiom: the credential property has
a twin that stores a raw token and must fail the oracle *by name*. This gets the same — an
index writer with the scope prefix omitted, and a `scan` with its range bound dropped, each
of which must turn the property red. Without it, a green property is ambiguous between "the
system is correct" and "the test cannot fail."

**Coverage `require:` keys.** Following `archive:delete_failed`. The trap here is sharper: a
single-tenant universe **cannot detect a leak at all**, because every row legitimately
belongs to the only tenant, so the property passes vacuously. `tenant:multi_tenant_write` is
therefore load-bearing. `tenant:crash_mid_batch` covers the staging/apply boundary where §2's
overlay change would go wrong.

**A wire oracle in the edge harness.** The sim proves the kernel; the edge harness covers the
wire — the two-layer pattern M1's exit criteria used. Two tenants, real TCP, stock binary:
tenant A's session never receives tenant B's row over HTTP or SSE.

Per hard rule 3 every oracle lands in the same change as the code it certifies, with seeds
pinned and the seed count stated in the commit.

## §6 What changes where

| file | change |
|---|---|
| `charpente-core/src/table.rs` | composite key for scoped tables; prefix in `index_entry`; prefix-bounded `scan`/`scan_from`/`scan_by`; index maintenance; `scan_raw_from` learns the key shape |
| `charpente-core/src/db.rs` | `Row::Scope`; `ScopedTx`; `TenantScope`; `across_tenants`; overlay key; scope-keyed `emit` |
| `charpente-macros/src/table.rs` | `scoped_by` attribute; `Scope` associated type; `TenantScope` const assert |
| `charpente-macros` (route/job) | `cross_tenant` attribute; capability token in the glue |
| `charpente/src/http/{app,router,mod}.rs` | `Ctx::cap()`; glue signatures; describe surface for `cross_tenant` |
| `charpente-sim` | the tenant property, its naive victim, the `require:` keys |
| `edge-harness` | the two-tenant wire oracle |
| `DESIGN.md` §3, §8 | the typed surface gains `Scope`; §8's deferral bullet is answered, pointing at this design |
| `PLAN.md` | the M2 tenant-scoping bullet moves to shipped |
| `docs/GUIDE.md` | the tenancy section: declaring, scoping, and the cross-tenant capability |
| `DECISIONS.md` | new entry recording the hard-rule-2 reading in §3; and §7's stale call-site figure re-counted (see below) |

## §7 Exit criteria

1. A cross-tenant read on a scoped table does not compile, and its `.stderr` is pinned.
2. `#[table(scoped_by = …)]` without a `TenantScope` impl does not compile, at the struct.
3. The flagship sim property is green over a stated seed count with `tenant:multi_tenant_write`
   as a `require:` key, and **its naive victim fails it by name**.
4. `scan`, `scan_from`, `scan_by` and `get` are each exercised by the property.
5. The edge-harness two-tenant wire oracle passes against a stock binary over real TCP.
6. A subscription keyed to tenant A never receives an event emitted under scope B.
7. `charpente describe` lists every `cross_tenant` route.
8. The kernel layer re-run on the BENCH.md protocol is inside the established bracket — or
   the pre-authorized parallel-overlay fallback is taken and the re-run then passes.
9. An app with no `scoped_by` compiles unchanged, and every pre-existing pinned seed
   reproduces exactly.
10. `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and
    the per-package test suites are green.

## §8 Non-goals, stated so they stay refused

- **Permissions / RBAC.** `DESIGN.md:1381` keeps "API-key scopes/permissions" as its own
  future design. This feature answers *which tenant*, never *what may this principal do*.
- **Nested or hierarchical scopes** (Org → Project → …). One level. YAGNI until an app needs
  two.
- **Platform-derived tenancy.** No `Principal → Org` resolver hook; ruling 3 forbids it.
- **Retrofitting existing tables.** Scoping is opt-in per table; no migration is forced on
  any app, and none of the fixtures is converted except as an oracle needs one.
  *Superseded 2026-09-16* (`2026-09-16-default-secure-surfaces-design.md`, Noah's 2026-09-13
  ruling): opt-in ended — every table now declares `scoped_by` or `global`. The
  no-forced-migration half stands: `global` is what a bare table always was.
- **Cross-tenant *writes* in one transaction.** Out of scope for v1; a transaction opens one
  scope. Revisit only with a real workload.
- **Tenant-local `Id<T>`.** Ids stay globally unique and app-minted; the key prefix, not id
  re-use, is what separates tenants.

## §9 Rulings owed to Noah

1. **§4's realtime auto-keying** widens the change. Approved in brainstorming, restated here
   because it is the cut most worth reconsidering if the sub-project runs long.
2. **§3's hard-rule-2 reading.** If Noah rejects it, cross-tenant enumeration becomes
   inexpressible and §7's criterion 7 and the `across_tenants` surface are struck.
3. **The overlay gate in §2** is pre-authorized as written; no further sign-off needed unless
   the fallback also regresses.

## §10 A drift finding noticed in passing

`DECISIONS.md` §7 describes redb coupling as "one file, ~30 call sites". The **file claim
holds exactly** — `grep -rln 'use redb::' crates/` returns only
`crates/charpente-core/src/table.rs`, and only `charpente-core/Cargo.toml` depends on the
crate. The **call-site figure is stale**, and deliberately not replaced with a number here:
two independent counts during this design disagreed (46 and 64) because they used different
pattern sets, which is itself the tell that the figure needs a defined measure before it is
quoted again. Whoever next edits that entry should define what counts as a call site and
record the count with the definition. Not a blocker for this sub-project; noted under hard
rule 6 so it is not lost.
