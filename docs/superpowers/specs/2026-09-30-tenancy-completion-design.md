# Tenancy completion — design

**Date:** 2026-09-30. **Status:** Draft, for Noah's review.
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
5. **`Principal: Clone` stays** (item 5's other half). Nothing accepts a caller-supplied
   `Principal`: `Ctx::principal()` (`http/app.rs:105`) is the only public producer, and
   `authorize` is called by the platform with the ladder's own. A parked clone reaches no scope.
   *Cost if wrong:* a later removal is itself a breaking change.
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
- Removing `Principal: Clone` (§2.5), sealing sessions or API keys (§2.6), or removing app keys
  (§2.3).
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
