# Tenancy completion — design

**Date:** 2026-09-30. **Status:** approved by Noah 2026-09-30; spec of record for the plan
`docs/superpowers/plans/2026-09-30-tenancy-completion.md`.
**Predecessor:** the job-tenancy record (`docs/LEDGER.md` 2026-09-30, "Decisions for Noah" and
"Unfinished work"). This spec is sub-project S1 of the pre-publish follow-ups
(`.superpowers/sdd/2026-09-30-publish-readiness/`): ledger items **2, 3, 4, 5, 6, 7, 11 and 12**.
Noah approved the breakdown on 2026-09-30, including widening item 2 from "seal `Tx`-side reads
of platform tables" to event tenancy. Every citation was read at `89bd4e6`.

## Context

C-2 made a job say which tenant it acts in. It left four doors open. Each one lets tenant data
cross a tenant boundary without a declaration, or leaves the refusal unable to say which tenant
it means:

1. **Events are tenancy by convention.** `ScopedTx::emit` keys an event with the tenant's
   decimal id (`db.rs:1675-1677`, `:1688-1691`). But a subscription with no `.key(…)` receives
   every event of its types, keyed or not (GUIDE §11, `kernel/tail.rs:142`). `.key(…)` takes
   any string, so a handler can name another tenant's id (`http/mod.rs:266`). The tenant-scoping
   spec's §4 stated this exact problem ("an app that has locked down its queries can still leak
   through a subscription") and closed only the emit half.
2. **The platform's own tables are readable from every transaction.** `JobRow` has
   `Scope = PlatformOwned` (`job.rs:105`), which `NoTenant` admits (`table.rs:423`), so
   `tx.scan::<JobRow>()` compiles in any handler. That returns every tenant's job payloads and
   tenant ids. `EventRow` and `SchemaVersion` are `Scope = Unscoped` (`event.rs:31`,
   `migration.rs:92`), so an app can also *write* them: `tx.put(&EventRow { .. })` forges an
   event into any tenant's feed. DESIGN §8 currently documents the read as a limit ("a route
   that exposes either table must filter by its own scope"). That is a review-checklist
   obligation, which §8's own opening sentence rules out.
3. **A tenant type has no identity of its own.** A scoped job's fingerprint hashes its
   `scoped_by` path *as spelled* (`charpente-macros/src/fingerprint.rs:44-60`). So two binaries
   whose `Org` names different types agree on it (P2, P3 in
   `.superpowers/sdd/2026-09-27-job-tenancy/final-fix-rereview-2.md:120-121`). Every rendering
   cuts a `type_name` or a path at its last segment, so `a::Org` and `b::Org` print alike and a
   generic prints `` `B>` `` (FR-NB1).
4. **The operator's standing can be parked.** `CrossTenant` and `TenantAdmission` are
   `Clone + 'static` (`db.rs:1745-1746`, `:1781-1782`), and `Ctx::cap()` hands a handler
   `&CrossTenant` (`http/app.rs:115`). A handler can `.clone()` it into a `static`, and so can
   operator code. Any handler can then open any tenant with nothing declared and nothing listed
   by `describe`.

These are pre-publish because they change public API (`Tenant`, `#[event]`, `Subscription`,
`Ctx`, the capability tokens), the log record format and the replication protocol. After
0.1.0, each is a breaking release for every adopter, plus a drain-before-upgrade for deployed
nodes (job-tenancy spec §2 item 7's cost argument).

## §1 Decisions already made (not reopened here)

- **The guarantee is "a scoped query cannot silently cross tenants"**, and anything that reads
  or writes outside one tenant arrives as a declared, enumerable capability, "not as a config
  option, a `DbConfig` field, an undeclared bypass method, or an 'internal-only' accessor"
  (DECISIONS §11, "What this binds"). That settles items 2 and 5.
- **Where a declaration selects between a guarded behaviour and an open one, its absence is a
  compile error naming both** (DECISIONS §13). That settles `#[event(global | scoped_by = S)]`.
- **Whoever holds the `Db` is the operator** (DECISIONS §12), and reads through `Db::snapshot()`
  stay the operator's read path. The dispatcher, tail delivery, replication seeding, the archive
  and the sim oracles already use it (GUIDE §8's oracles read `snap.scan::<JobRow>()`).
- **A persisted identity is a declared name, never the compiler's printer** (DESIGN §3,
  Migrations: fingerprints render tokens "by the macro itself … never by the compiler's printer,
  whose spacing is not stable across toolchains"). The same reasoning applies to a tenant type's
  identity: tables have `TABLE`, events `NAME`, jobs `NAME`.
- **Refuse by name rather than guess** (DESIGN §3's organizing rule) and hard rules 4 and 5.

## §2 Rulings made in this spec

Each ruling gives its basis and what it costs if it is wrong.

1. **`Tenant` gains a required `const NAME: &'static str`.** It is a tenant type's identity, the
   way `TABLE` is a table's. Two tenant types with one `NAME` in one app are refused at mount.
   *Basis:* §1's declared-name rule; one name then fixes items 3 and 6 together. *Cost if
   wrong:* one line per `impl Tenant` (≈60 in-tree, mostly fixtures).
2. **A scoped job's and a scoped event's fingerprint mixes `S::NAME`, not the path as spelled.**
   Global fingerprints are unchanged. This closes P2 and P3 through the existing foreign-shape
   refusal, with no record-format change: the shape already rides on every row. *Residual,
   stated:* two tenant types that share a `NAME` in two different binaries are one tenant by
   declaration, exactly as two structs sharing a `TABLE` are one table. *Cost if wrong:* every
   scoped fingerprint re-pins once.
3. **App keys stay; tenancy stops being a key.** This revises the chat summary of 2026-09-30,
   which said free-form keys "go away". `examples/invoicing` keys its feed by the session subject
   (`routes.rs:254`, `:299-302`, `jobs.rs:46`), which is a per-user feed and not a tenant.
   Removing app keys would break the demo the launch run replays. So a scoped event records its
   tenant in a field of its own, and `.key(…)` remains an app-level narrowing filter, orthogonal
   to tenancy. *Cost if wrong:* none to tenancy; per-user keys stay the app's access control, as
   DESIGN §8 already says of permissions.
4. **The capability tokens are tied to the `Group` that minted them, and neither is `Clone`.**
   *Basis:* DECISIONS §12 says the handler-side seal "is exactly as strong as the boundary claim
   on the `Db` side". A token a handler or `main` can park in a `static` is the undeclared bypass
   §11 refuses. *Cost if wrong:* `Ctx` gains a lifetime parameter. Handler source still writes
   `ctx: &Ctx` and `cap: &CrossTenant` (elided lifetimes; the workspace does not enable
   `elided_lifetimes_in_paths`).
5. **`Principal` is not `Clone`** (item 5's other half). `Ctx::principal()` (`http/app.rs`) is
   the only public producer, and it lends a `&Principal` for the request's length. With no
   `Clone`, no principal outlives its request, so the `who` an app can hand `Tx::open_scope` is
   its own request's caller, and `authorize` is asked about the caller that is asking. A parked
   identity reaches no scope. *Cost if wrong:* re-adding `Clone` is additive; removing it was
   the breaking change, made before 0.1.0. *(Amended 2026-10-03, A19: this ruling first read
   "`Principal: Clone` stays", on the premise that nothing accepts a caller-supplied
   `Principal`. `Tx::open_scope` is `pub` and takes any `&S::Who`, so a parked clone opened
   every tenant its subject belongs to, from any route.)*
6. **Sessions and API keys stay app-owned global tables.** They are declared
   `#[table(global)]`, revocation is `tx.delete::<ApiKey>(id)` by design (DESIGN §8), and they
   carry no tenant data. Item 2's "unaudited" is closed by stating this, not by sealing them.

## §3 Platform-owned tables (items 2, 11)

`JobRow`, `EventRow` and `SchemaVersion` are the platform's. Each takes
`type Scope = PlatformOwned` and is listed in `PLATFORM_TABLES` (`db.rs:826`), whose refusal text
names *the matched table* and its write path. The current text is hard-coded to
`charpente.jobs` (`db.rs:848-860`).

- **App writes** (`Tx::put`/`Tx::delete`, and any `Row` whose `TABLE` borrows one of the three
  names) fail to build, exactly as a `JobRow` write does today.
- **App reads.** `Tx`/`ScopedTx` `get`/`scan`/`scan_from`/`scan_by` of a platform table become
  an unsatisfied bound. The text names the table and the operator path: `db.snapshot()` from
  code that holds the `Db`. Reads through `Snapshot` are unchanged. The trait shape is the plan's
  choice; the plan must keep `View`'s existing diagnostics for scoped tables byte-identical.
- **Platform writes keep their doors:**
  - jobs: the enqueue family plus `Tx::settle_job` (unchanged);
  - events: apply-time materialization from emit effects (unchanged);
  - schema versions: a crate-private door on `Tx` for `migrate_table`'s two writes (`db.rs:3517`,
    `:3558`), producing a record byte-identical to today's `tx.put`, which a test asserts.
  - `crates/charpente-core/tests/migrate_apply.rs:100`/`:144` forge schema-version rows through
    `tx.put`. They move to raw records, the way `migrations.rs:1064` builds them.
- **DESIGN §2's "one user-visible semantic group commit changed"** (a sibling in a batch does not
  see an enqueue or emit) stops being app-visible, because apps can no longer read the queue.
  `group_commit.rs:2034`/`:2045` read the queue from a handler to pin it. The plan re-pins the
  property at the level where it still exists, and DESIGN §2's sentence is rewritten.
- The DESIGN §8 / GUIDE §16 paragraph "the platform's own tables are not scoped … a route that
  exposes either table must filter by its own scope" is replaced by this section.

## §4 Tenant identity (items 3, 6)

```rust
pub trait Tenant: Sized + 'static {
    /// This tenant type's identity: persisted inside every scoped job's and scoped
    /// event's fingerprint, and rendered wherever a scope type is named. Renaming it is
    /// a change of identity, as renaming a table's TABLE is.
    const NAME: &'static str;
    type Who: ?Sized;
    type Error;
    fn authorize(tx: &Tx<'_>, who: &Self::Who, scope: Id<Self>) -> Result<(), Self::Error>;
}
```

- **Uniqueness.** Every scope type an app mounts, through tables, jobs, `public(S)` routes and
  events, is collected with its `TypeId`. Two different types with one `NAME`, or an empty
  `NAME`, are refused before the app serves: the refusal names both types' `type_name` (for the
  reader only; it is never persisted), the shared `NAME`, and the fix.
- **Rendering.** Every place that names a scope type renders `S::NAME`:
  - `Group::stage`'s tenancy refusal (`db.rs:3777-3796`);
  - the 401 (`http/app.rs:143-148`);
  - `describe`'s `public_scope` and `scoped_by`, and the events' new `scoped_by` (§6);
  - `JobDef.scoped_by`, which becomes the const `<S as Tenant>::NAME`, emitted by the macro in
    place of the last-segment ident.

  The last-segment helpers go away. `Tx::enter_scope`'s one-scope assert (`db.rs:1502-1511`)
  prints both the `NAME` and the full `type_name`. Result: `a::Org`/`b::Org` render as their two
  names, and a generic renders its name, never `` `B>` ``. `Group::stage`'s cause list gains
  the hand-built `JobDef` case (NB1's misplaced blame).
- **Fingerprints.** The macro emits the payload fold as a literal and calls a core `const fn`
  that mixes in `S::NAME`. `fingerprint.rs`'s `#scoped_by\0<path>\0` suffix is replaced by the
  name. A pending scoped job whose tenant type's `NAME` differs across a deploy then meets the
  existing foreign-shape refusal at startup, where the write gate opens and at promote. Its text
  gains the cause "or its tenant type's `NAME`".

## §5 Capability tokens (item 5)

```rust
pub struct CrossTenant<'g>(PhantomData<&'g ()>);     // not Clone
pub struct TenantAdmission<'g>(PhantomData<&'g ()>); // not Clone
impl<'a> Group<'a> {
    pub fn cross_tenant_cap(&self) -> CrossTenant<'_>;
    pub fn tenant_admission(&self) -> TenantAdmission<'_>;
}
```

`Ctx` becomes `Ctx<'g>`. The route glue passes the handler `ctx.cap()`'s reference rather than
a clone. `JobScope<'j>` already borrows its admission. Neither token can be stored in a `static`
or a `thread_local!`, both of which need `'static`, or outlive the group. The doc comments'
"`Clone` because … a copy widens nothing" rationale goes. Compile-fail pins: a `&CrossTenant`
cloned; each token parked in a `static`.

## §6 Event tenancy (item 2, widened)

**Declaration.** `#[event(global)]` or `#[event(scoped_by = S)]` (`S: Tenant`). A bare
`#[event]` is a compile error naming both, the same shape and text family as `#[job]`'s
(DECISIONS §13; GUIDE §9 gains the line). `Event` gains `type Scope` (`Unscoped` or `S`). A
hand-written `impl Event` is sealed the way job-tenancy sealed hand-written `impl Job`; the plan
mirrors that pattern exactly.

**Emitting.**
- `Tx::emit`/`Tx::emit_keyed` take `Ev: Event<Scope = Unscoped>` only. A scoped event there is
  an unsatisfied bound whose text names `tx.scope::<S>(…)?` and its `emit`.
- `ScopedTx<S>::emit`/`emit_keyed` take `Ev: Event<Scope = S>` and record the tenant id.
- A global event emitted from inside a scope is `(*tx).emit(ev)`, today's explicit-deref idiom.
- A scoped job's `run` already holds a `ScopedTx<S>`.
- `ScopedTx::event_key` is removed. It was the tenancy-as-a-key mechanism.

**Recording.** A scoped emit is a new record tag,
`0x0b [event][payload][key: Option<String>][shape: u64][scope: u64]`, mirroring `0x0a`. It is
chosen by the event's declared scope, never by the tenant's value, so tenant 0 is `0x0b` too.
`EventRow` gains a trailing `scope: Option<u64>`. Global emits stay `0x09`, byte-identical, so
nothing in `examples/invoicing` changes on disk. `PROTO_VERSION` goes 4 → 5 (DESIGN §5: "a
message body's shape changing is what bumps it"). `0x0c` is the next free tag.

**Subscribing.**
- `Subscription::of::<Ev>()` and `.and::<Ev>()` take global events only.
- A tenant feed is built from the scope. On a `ScopedTx<S>`, `tx.subscription::<Ev>()`
  (`Ev::Scope = S`) yields a subscription carrying that tenant, and its `.and::<Ev2>()` is
  bounded `Ev2::Scope = S`.
- Every mismatch is a compile error. The builder's type carries `S`. The handler's declared
  return type stays spelled `Subscription` (plan's choice of mechanism).
- `.key(…)` and `.cursor(…)` remain on both.

**Delivering.** A row reaches a tail iff its name matches an entry, its shape equals that
entry's fingerprint, **`row.scope == subscription.scope`**, and the key passes (`None`, or
equal). Resume validation (`kernel/mod.rs:1023`) uses the same predicate. So:
- a global subscription never receives a scoped event;
- a tenant's subscription never receives another tenant's event or a global one;
- no string names a tenant anywhere in app code.

Two scope types sharing a raw id no longer collide: the event's fingerprint mixes its tenant's
`NAME` (§4), so a row emitted under the other type is a foreign shape. This closes DESIGN §8's
first stated limit.

**Not provided:** a cross-tenant subscription. It arrives as a §11 declared capability if an app
needs one (§11 below).

## §7 Smaller items

- **Item 4, `JobScope::open` asserts its type.**
  - `JobScope` carries the mounted `JobDef`'s `scope_type` and `NAME`. `open::<S>` asserts
    `TypeId::of::<S>() == scope_type`, with a message naming the job, both scopes' `NAME`s and
    the cause (a hand-built `JobDef` whose glue or `scope_type` disagrees with its `#[job]`).
  - It runs inside the dispatcher's `catch_unwind` (`dispatch.rs:328-340`), so it is an attempt
    that retries and then dead-letters, never a kernel stop. That is the tier of the glue's
    existing `expect`.
  - This closes P4 and P5 at dispatch, before any write in the wrong tenant.
  - Stated residual: a `JobDef` whose glue *and* `scope_type` lie the same way is undetectable.
- **Item 12, the 401's fix keeps the route's other flags.** Rule 3's 401 stops restating the
  whole `#[route(…)]`. It tells the author to declare `public(NAME)` in place of the route's
  current stance *in its existing attribute, keeping its other flags*. That works for
  `cross_tenant`, for `emits(…)` and for any future flag. `#[route]`'s `undeclared_capability`
  text (`charpente-macros/src/route.rs:199-222`) gets the same treatment if it drops `emits`.
  This amends job-tenancy spec §4's mandated text.
- **Item 7, DECISIONS.md:328.** "runs in the tenant its enqueuer opened" becomes "… opened, or
  that its fan-out named". The cross-binary residual is replaced by a reference to §4's name
  mechanism, with a correction note in the §12 style ("*(Corrected <the commit's date>, tenancy-completion spec: …)*"). The
  decision is unchanged. The LEDGER tells Noah, as the C-2 record did for line 256.

## §8 `describe`

`EventInfo.keyed`, documented as "always `false`", is replaced by
`scoped_by: Option<&'static str>` (the tenant `NAME`). `public_scope` and `scoped_by` render
`NAME` (§4). The contract goldens move once: `crates/charpente/tests/describe.rs`, the CLI's,
`examples/invoicing/tests/describe_golden.rs`, GUIDE §13 via `describe_contract`. The plan
checks whether `charpente-tsgen` reads `EventInfo`.

## §9 Oracles

RED first for every behaviour. A green mutation proves nothing until the mutated code's
observable behaviour is printed.

- **Compile-fail pins** (`charpente-macros/tests/compile_fail/`, `.stderr` naming contract and
  fix), one per new compile error:
  - bare `#[event]`;
  - a scoped event through `Tx::emit`, and a global event through `ScopedTx::emit`;
  - `Subscription::of::<Scoped>()`, and `.and()` mixing scopes;
  - `tx.scan`/`tx.get` of each platform table;
  - `tx.put` of `EventRow`/`SchemaVersion`, and a borrowed `TABLE` name for each;
  - each token cloned or parked in a `static`.
- **Realtime tenancy, in sim, under the full fault matrix** (extends
  `tenant.rs`'s "a subscription keyed to tenant A never receives an event emitted under scope
  B"). N tenants emit scoped events and the app emits global ones. Three properties:
  - every global subscription receives no scoped row;
  - every tenant subscription's delivered set equals exactly that tenant's scoped rows
    (set equality, so an interior skip cannot pass);
  - a resume across the boundary is refused, not misdelivered.

  Pinned base plus fresh seeds, with anti-vacuity floors on scoped deliveries, global
  deliveries and cross-tenant attempts.
- **P2/P3 by fingerprint** (kernel.rs, reusing the re-review's `a::Org`/`b::Org` shapes): a
  Pending scoped job enqueued under one tenant `NAME` is refused at startup by a binary that
  mounts it under another, by name. Same-`NAME` types in one app are refused at mount.
- **Rendering:** the refusal and the 401 distinguish two same-last-segment types, and a generic
  tenant renders its `NAME`, all pinned byte-exact.
- **Item 4:** P4's and P5's `JobDef`s dead-letter with the named message and write nothing in the
  wrong tenant. Each assert operand is inverted separately.
- **Item 11:** the schema-version door's record is byte-equal to the old `tx.put`'s.
  `migrations.rs` and `migrate_apply.rs` stay green.
- **Item 12:** the 401 on a `cross_tenant` route and on an `emits(…)` route, pinned.
- **Mutation run** over the changed core files, with `--test-package` for every crate whose
  suites exercise them (CLAUDE.md's `--test-package` trap): 0 unresolved MISSED beyond the
  argued-equivalent list.
- **Seeds:** the new tag changes the bytes of every scoped emit, so tenant and realtime universes
  that emit scoped events re-roll. Pinned seeds are re-found by *reason*, and floors are
  re-measured identically across two consecutive batches. A pinned seed names a universe, not a
  scenario.

## §10 Docs, in the same commits (hard rule 6)

- **GUIDE:**
  - §1 (the `charpente new` template's event);
  - §5 (the 401 pinned block);
  - §6 (fingerprint mixes `NAME`);
  - §9 (`#[event(global | scoped_by)]` line);
  - §11 (events: emit, tenant feeds, keys as app filters);
  - §13 (`EventInfo.scoped_by`, `NAME` rendering);
  - §16 (`Tenant::NAME`; event tenancy; platform tables sealed; tokens not parkable; remove
    `event_key`, the multi-scope-type key limit and the "filter platform tables" obligation).
- **DESIGN:**
  - §2 (tag `0x0b`, `EventRow`, the group-commit visibility sentence);
  - §3 (Scope, Jobs and Events bullets);
  - §5 (`PROTO_VERSION` 5);
  - §6 (Realtime);
  - §7 (`describe`);
  - §8 (Multi-tenancy limits rewritten).
- **DECISIONS.md:328** (item 7).
- **README**, if it names `emit_keyed` or the tenancy limits.
- **CLAUDE.md** test lists, if a test file is added.

## §11 Migration

Nothing is published, so no adopter migrates. In-tree:
- `examples/invoicing` (two events → `#[event(global)]`; its per-subject keys unchanged);
- `crates/sim-fixture`, `crates/ts-fixture-app`, the `charpente new` templates (`lib.rs:27`);
- every `#[event]` in tests (≈95 sites) and GUIDE (26);
- every `impl Tenant` (≈60, mostly compile-fail fixtures, whose `.stderr` line numbers move).

The statuspage and helpdesk experiments live outside the tree and stay as testimony, unmigrated.

## §12 Refused, so the plan does not grow them

- A cross-tenant subscription (a §11 capability, when an app needs one).
- An app read of a tenant's own jobs or events through `ScopedTx`, for a job-status UI (the same
  trigger).
- A tenant `NAME` field on job rows (the fingerprint carries it).
- A `type_name`-derived identity (§1).
- Sealing sessions or API keys (§2.6), or removing app keys (§2.3).
- The `App::mount` notch (DECISIONS §11/§12's trigger has not fired).

## §13 Exit criteria

1. No app code can read or write another tenant's rows, jobs or events without a declaration
   `describe` lists: each door in the Context is a compile error or a named refusal, pinned.
2. P2, P3, P4, P5 and FR-NB1's renderings are closed, each by a test that was red first.
3. The realtime tenancy property holds under the full fault matrix at the pinned base and over
   fresh batches, with its floors met.
4. The workspace gate is counted green (every `test result: ok`, `--no-fail-fast`), plus the TS
   gate, the mutation run and the guide gates. CI passes on the merged tip.
5. GUIDE, DESIGN and DECISIONS say what the code does, in the same commits.

## §14 Decisions for Noah

None contradicts DECISIONS.md, so none needs a ruling (CLAUDE.md). Two things to know:
- This spec edits DECISIONS.md:328's wording (item 7) without changing the decision.
- It extends DECISIONS §13's shape to `#[event]`, which §13's "What this binds" already
  anticipates.

One revision to what was said in chat: app keys stay (§2.3).

## Amendments (implementation plan)

- **A1 — `#[event(global)]` lands first as a no-op; the bare-`#[event]` refusal lands with `scoped_by`.** Spec §6 refuses a bare `#[event]` and tells the reader to write `global` or `scoped_by = S`. The refusal lands in the same commit that makes `scoped_by` work (Task 11). Task 1 accepts `global`, sweeps it onto every event, and proves the sweep with a grep. This is the same shape as the job-tenancy plan's A1.
- **A2 — a table's tenant name reaches the uniqueness check through `Row::SCOPE_NAME`.** `Row` gains `const SCOPE_NAME: Option<&'static str> = None`, and `#[table(scoped_by = S)]` sets it to `Some(S::NAME)`. `Schema::table` records it in `TableOps` beside the scope's `TypeId`, so `App::open` can collect every table's tenant without naming `S`.
- **A3 — a tenant feed is `tx.subscription::<Ev>()` → `TenantFeed<S>` → `.into()`.** The SSE handler's declared return type stays `Result<Subscription, ApiError>`, which the route macro checks. `TenantFeed<S>` carries `S` in its type, so `.and::<Ev2>()` is bounded by `Ev2::Scope = S`. The conversion into `Subscription` is the last step. `Subscription::and` on a tenant-carrying subscription is a named assert, because the predicate could never deliver the added global event.
- **A4 — in-tree tenant `NAME`s equal the type's identifier.** FNV-1a is streaming. So "fold, then `#scoped_by\0` + NAME + `\0`" equals today's "fold, then `#scoped_by\0` + rendered path + `\0`" whenever the NAME equals the path's rendering. Every existing single-ident scoped fingerprint pin therefore stays byte-identical. Task 4 proves this at base before changing the macro. Fixtures whose two tenant types share an identifier get different NAMEs in Task 3, and their fingerprints move by design.
- **A5 — no cross-tenant emit (`emit_in`) is added.** A rollup that must notify a tenant enqueues a scoped job there with `enqueue_in`, and that job's `run` emits. This follows spec §12's refused list.
- **A6 — Task 4 executes before Task 3.** They consume nothing from each other. Task 3's refusal text says a tenant's NAME "is inside every scoped job's and event's fingerprint", and its GUIDE step extends the §6 residual that Task 4 writes. So Task 4 runs first, and the job half of that text is true on the commit that lands it. The event half becomes true at Task 11, a forward reference inside this plan that the fixed text accepts. The numbering stays, so every cross-reference keeps its meaning.
- **A7 — macro output names a tenant through `::charpente::tenant::tenant_name::<S>()`.** The contract's `<S as ::charpente::core::db::Tenant>::NAME` adds a second, unhoused E0277 ("the trait bound `Team: Tenant` is not satisfied … consider adding one") to every "scope type has no `Tenant` impl" diagnostic, measured in a scratch proc-macro crate. `<S as ::charpente::TenantScope>::NAME` is E0576. Instead, `tenant.rs` gains `#[doc(hidden)] pub const fn tenant_name<S: TenantScope>() -> &'static str { S::NAME }`. The value is `S::NAME` and it is still a const expression. Its bound is the one the three macros' `__charpente_assert_tenant_scope` const asserts carried, so those asserts are deleted, and a missing impl gets exactly one error per site, the house one. The three missing-impl fixtures keep their error counts (1, 1, 2). `#[event(scoped_by)]` (Task 11) uses the same helper.
- **A8 — a compile-time refusal quotes a scope path as written.** `#[job]`'s and `#[table]`'s "`global` and `scoped_by = …`" refusals and `#[route]`'s undeclared-capability refusal run at expansion time, where `Tenant::NAME` does not exist. With the macro's `scope_name` last-segment helper deleted (Task 2), they render the path through a new `fingerprint::path_as_written`, built from `render`'s words: `crate::tenancy::Org` and `Wrap<Desk>`, never `Org` and `Wrap`. A single-identifier path renders as before, so no existing pin moves. `#[event]`'s refusal (Task 11) uses the same renderer.
- **A9 — the capability tokens borrow what their `Group` borrows, not the `Group` itself.** Spec §5's `-> CrossTenant<'_>` / `-> TenantAdmission<'_>` ties a token to `&self`, which keeps the group shared-borrowed. Every site that mints and then stages in the same group (the dispatcher, the request path, the drift probe, every operator helper, GUIDE §16's `seed`; 22 mint sites when this plan was written, and S2's `through_firm_note` in `tenant.rs` adds two) would then be E0502. They are written `impl<'a> Group<'a> { fn cross_tenant_cap(&self) -> CrossTenant<'a>; fn tenant_admission(&self) -> TenantAdmission<'a>; }`. A token then lives no longer than the group's `&'a mut Db`, so it cannot enter a `static` or a `thread_local!` (E0521), and cloning a `&CrossTenant` only copies the reference (E0308). *Residual, true of both forms:* operator code that `Box::leak`s its `Db` can mint a `'static` token. That is the operator building a bypass on purpose, with the standing `Db::snapshot` already gives it (DECISIONS §12). Spec §2 ruling 4 reads: a handler cannot park a token, and `main` cannot without leaking the `Db`. Task 7 Step 5 re-measures the `&self` form on the real tree.
- **A10 — P5 exactly as the re-review built it is refused at mount, and the dispatch assert closes the rest.** `JobDef { scope_type: TypeId::of::<Board>(), ..File::def() }` keeps `File`'s NAME `Desk` for a type that is not `Desk`. Beside any other `Desk`-scoped mount, Task 3's uniqueness check refuses it before anything is enqueued. The P5 shape that survives mount has a `scoped_by` and `scope_type` that agree with each other but not with its glue, and `JobScope::open`'s assert (Task 5) closes it. `JobScope` carries the job's name as a third private field (`job`), because spec §7's message names the job. *Stated residual (spec §7):* a `JobDef` whose glue and `scope_type` lie the same way is undetectable. It is also not a tenancy breach: `Group::stage` held the enqueuer to that same type.
- **A11 — item 12 closes the class of refusal, not two instances.** A fix clause that restates an attribute from the author's own declaration drops the author's other arguments. Spec §7 names the 401 and `undeclared_capability`. A sweep of every `crates/*/src/**/*.rs` literal that spells an attribute found five more, and Task 6 fixes all seven:
  - `#[route]`'s neither-stance refusal, which drops `cross_tenant` and `emits(…)`;
  - `#[job]`'s no-tenancy refusal, which drops `cross_tenant` and `retry(…)`; a dropped `retry` silently resets the policy;
  - `#[table]`'s no-tenancy refusal, which drops `version = N`;
  - `emits(…)` on a non-stream route;
  - `NoTenant`'s job line in charpente-core.

  The rule: a refusal names the word to add or change and says to keep the rest. Where it names the site, it writes an elided attribute. Task 6's class sweep reports 12 restated literal lines at its base and must report 0 after. `#[event]`'s bare refusal (Task 11) is written in the same shape from the start.
- **A12 — the dispatcher reads its job row through an admission-gated door, `Tx::get_job`.** `Dispatcher::run_due_into` re-reads the job row inside `group.stage`, where the batch's own overlay must be visible and no snapshot exists (`dispatch.rs:417-424` at 57e5df5 says why; `:411-418` at e749823). Core's crate-private readers do not reach the `charpente` crate. So `Tx` gains `pub fn get_job(&self, id: Id<JobRow>, admission: &TenantAdmission<'_>) -> io::Result<Option<JobRow>>`, sealed exactly as `settle_job` is: only a `Group` mints the admission. Four operator-side test reads that already hold an admission move onto it.
- **A13 — a transaction's read of a `Row` whose `TABLE` borrows a platform table's name is refused at build, like the write.** A typed bound covers `JobRow`/`EventRow`/`SchemaVersion` themselves, but not an app's hand-written `Row` with `TABLE = "charpente.jobs"` and `Scope = Unscoped`, the door C-2's FR-C1 closed for writes. `Tx`'s four `View` methods evaluate `const { refuse_platform_read(R::TABLE) }`, the read twin of `put`'s `refuse_platform_table`. This is a post-monomorphization E0080 that `cargo build`/`cargo test` report and `cargo check` does not, FR-C2a's accepted limitation, now for reads too.
- **A14 — DESIGN §2's group-commit visibility pin moves to core.** `enqueues_and_emits_are_invisible_to_a_later_sibling_in_the_same_batch` (`crates/charpente/tests/group_commit.rs`) reads `JobRow`/`EventRow` from handlers, which Task 9 makes compile errors. The property still exists where the dispatcher reads the queue inside a batch. So Task 9 deletes that test and its routes, pins the job half in `crates/charpente-core/tests/group_commit.rs` through `Tx::get_job`, and states the emit half in DESIGN §2: no transaction can read an emit any more.
- **A15 — one quoted `View` declaration line re-pins.** Spec §3 keeps `View`'s existing diagnostics for scoped tables byte-identical. The message, the label, both `help:` blocks, every `note:` and the error count are identical. The line rustc quotes from `View::scan`'s declaration cannot be, because the bound sits on it. So `compile_fail/scoped_read_from_bare_tx.stderr`'s `fn scan<R: UnscopedRow>(…)` becomes `fn scan<R: UnscopedRow + ReadableBy<Self>>(…)`, the only `.stderr` in the tree that quotes a `View` declaration.
- **A16 — resume validation refuses, by name, a cursor on a row of another tenancy.** When `row.scope != sub.scope` (another tenant's event on a tenant feed, a global event on a tenant feed, or a tenant's event on a global subscription), the resume gets a 409 checked after the seq check and before the reshape check: `cursor <id>.<seq> sits on an event outside this feed's tenancy — a cursor resumes only the feed that delivered it, and this feed never delivers that event; reconnect without a cursor, wait for the stream to open, then read current state from the same node, holding every frame that arrives until that read is installed and then applying the held frames and every later one over it in order, idempotently`. Its remedy is D4's, copied verbatim from `validate_subscribe`'s other 409s (`kernel/mod.rs:966-1066` at 57e5df5); this plan first gave the read-first order ("re-sync current state through reads, then reconnect without a cursor"), which D4 replaced everywhere on main because it misses the changes that land between the two steps (LEDGER, D4). It names no tenant and no event. Validation still does not refuse a cursor on a same-tenancy row the feed does not admit (another event type, another app key), which payload-shapes spec §3 pins. So no global feed's behaviour, and no global baseline, moves. `Subscription::admits` stays the one predicate: delivery uses it to skip, and validation uses it to gate the reshape refusal. This makes spec §9's "a resume across the boundary is refused, not misdelivered" exact.
- **A17 — `Subscription.entries` and `SubEntry` become crate-private (Task 11).** With them `pub`, a handler admitted to `Team` 1 could push an `Org`-scoped event's `SubEntry` into its own feed. Delivery would then admit `Org` 1's rows of that event: same name, same shape, same raw id. The typed bounds and the NAME-mixing fingerprint never see a hand-pushed entry, and exit criterion 1 forbids that read. The one out-of-crate user, `realtime.rs`'s failing-encoder test, moves into a `#[cfg(test)]` module in `kernel/tail.rs`. A compile-fail fixture pins the privacy. `Subscription.key` and `.cursor` stay public: a key is an app filter (spec §2.3), and the tenant rides in the private `scope`.
- **A18 — a read goes through the `Row` type registered for its table (Task 9).** Since S2 (LEDGER 2026-10-01, "error text and schema safety"), `Group::stage` refuses a put or delete through any Rust type other than the one `Schema::table` registered under its `TABLE` (`TableWriteRefusal`). A read is not checked: `ScopedTx<Firm>::get` through a same-named `#[table(scoped_by = Firm)]` type keys on the table name and the raw scope id, so it reads the registered `Team` table's row of the same id (DESIGN §4 states this residual). A13 seals only the platform tables' names, so this is new. `Tx` holds the `Schema` its `Group` stages against, and each read of an app table (`Tx`'s four `View` reads, `ScopedTx`'s `get`/`scan`/`scan_by`/`scan_from`, and `AllTenants::scan`) compares `TypeId::of::<R>()` with the registered `row_type` before it reads. A mismatch returns `TableReadRefusal`, an `io::Error` like `TableWriteRefusal`, naming the table, the type that read, the registered type and S2's two fixes. The read returns nothing, so no row reaches the caller. In a job handler the refusal comes back through the handler's `?` as `JobError::Io`, and the dispatcher classifies it as an attempt, never a platform fault. A route's `?` answers it with a 500 carrying its text (`ApiError`'s `From<io::Error>`). `Tx`'s own `View` reads are checked too (S1 amendment review I2). An unscoped type naming a scoped table opens the other key layout and fails loudly (`unscoped_open_err`) only once the row is in the store. Before commit, `Tx`'s reads consult the transaction's and the group's overlays at `(UNSCOPED, id)` first, and `UNSCOPED` is 0, so an unregistered `#[table(global)]` twin of a scoped table reads tenant 0's row that an earlier stage of the same batch wrote: `Ok(Some("Team 0's note"))`, run on 57e5df5. Any handler batched with that stage, in any tenant or on a public route, would read it. A table the `Schema` does not register is not checked, since no registered type is there to read as. *Drafted when this plan was amended against 57e5df5, and not compiled then: Task 9 measures it.*
- **A19 — `Principal` is not `Clone` (final whole-branch review C1, 2026-10-03).** Spec §2 ruling 5 kept `Principal: Clone` on the premise that nothing accepts a caller-supplied `Principal`. The premise was false: `Tx::open_scope` is `pub` and takes `who: &S::Who`, and `Who = Principal` for every tenant type the platform's routes scope by. The final review's probe, on a `git archive 747680f` copy: an `auth` route parks `ctx.principal().cloned()` in a `static Mutex<Option<Principal>>`, and a plain `public` route then runs `tx.open_scope::<Org>(org, &parked)?` and scans a scoped table. It read org 7's rows for bob, who is not a member of org 7 (`tx.scope` refused him the same read), and for an anonymous caller, while `describe` listed the route as `public_scope=none cross_tenant=false`. A global job or a background thread could do the same. Fix: `Principal` derives `Debug` only. `Ctx::principal()` lends a `&Principal` for the request's length, so no principal outlives its request, and the `who` an app can hand `open_scope` is its own request's caller. `compile_fail/principal_cannot_be_parked.rs` pins both ways to park one (`.cloned()` is E0277, `.clone()` copies only the reference, E0308). Measured cost: `cargo check --workspace --all-targets` is clean with no call site changed, and trybuild is unchanged apart from the new fixture (`principal_is_read_only_to_an_app.rs`, whose `.clone()` now copies the reference, keeps its `.stderr` byte for byte; its comment says so). Ruling 5 and §12's refused list are amended to match.
