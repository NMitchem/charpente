# Default-secure surfaces — design

**Date:** 2026-09-16. **Status:** approved in chat (Noah), spec committed for the plan.
**Predecessor:** the 2026-09-13 roadmap ruling (`c840b41`; docs/LEDGER.md "complete
defined") that defined M3 — Launch and named this as its second sub-project, "a DESIGN
§7/§8 change" with its own brainstorm. Sub-project 1 (publish path, `d731fde`) is done;
sub-projects 3 (third docs-only build) and 5 (the launch run) stand on this one.

## Context

On 2026-09-03 a docs-only agent building a helpdesk shipped two guarantees the platform
already had but did not enforce (`docs/experiments/2026-09-helpdesk/README.md`): an
unauthenticated `POST /agents/:id` that lets anyone join any org, and an unscoped
`tx.get(id)?.is_some()` existence check that works as a cross-tenant oracle. Both
guarantees are opt-in today — `auth` on a route, `scoped_by` on a table — and the agent
opted into neither, because nothing told it that not opting in was a decision. For a
framework whose thesis is *the framework is the reviewer*, a guarantee that is silent when
absent is the gap that matters: the reviewer's first question — "is this route open? is
this table tenant data?" — was never asked.

This sub-project makes both questions compile errors. A `#[route]` says `auth` or `public`;
a `#[table]` says `scoped_by = X` or `global`; a declaration that says neither does not
compile, and the error names both choices. Nothing about what `auth` or `scoped_by` *do*
changes. What changes is that their absence stops being a default and becomes a refusal.

The tenant-scoping spec (`2026-08-30-tenant-scoping-design.md` §8) listed "scoping is
opt-in per table; no migration is forced on any app" as a non-goal. This spec supersedes
that line — Noah's 2026-09-13 ruling — and the mechanism it chooses forces no migration:
`global` emits exactly what a bare `#[table]` emits today.

## §1 Decisions already made

- **2026-09-13 (Noah, PLAN.md M3 sub-project 2):** both opt-in guarantees flip to
  default-on as compile errors; a route without `auth` or an explicit `public` is a compile
  error naming both; a table that should be scoped and is not is a compile error, with
  "should be" left to this spec. The oracle is the helpdesk at `8913038`, rebased, failing
  to compile at both Critical sites.
- **2026-09-16 (Noah, this brainstorm):** "should be scoped" is answered by **universal
  stance** — every table declares, the platform never infers (§4). The word is `global`.
  `public` + `cross_tenant` on one route is allowed (§2). No typed-principal change, no
  field-type rule, no runtime half (§8).

## §2 Routes: `auth` | `public`, neither is the default

`#[route]` gains one flag, `public`. Exactly one of `auth` / `public` appears on every
route. The parser refuses the other three states, each at compile time, each a
`syn::Error` in the house style (name the contract, the site, the fix — hard rule 5):

```text
`create_agent` declares neither `auth` nor `public` — a route says which callers reach it: write `#[route(POST "/agents/:id", auth)]` to require a principal (`ctx.principal()` is `Some`), or `#[route(POST "/agents/:id", public)]` to serve anonymous callers. Neither is the default.
`auth` and `public` on one route — a route is one or the other; drop one
`public` given twice — write it once
```

The first error spans the path literal, so the caret sits where the flag goes. The
handler name and the route's own method and path are interpolated, so the fix is the
exact line to write. `FLAGS` — the unknown-flag message — gains `public`:

```text
a route flag is `auth`, `public`, `cross_tenant` or `emits(Ev, …)` — write `#[route(GET "/path", auth)]`, `#[route(GET "/path", public)]`, or `#[route(SSE "/path", emits(NoteAdded, NoteRemoved))]`
```

**Semantics.** `public` is today's bare route, bit for bit: no principal is required,
`ctx.principal()` may be `None`, and a `Tenant::authorize` that admits `None` has declared
anonymous access to that scope (GUIDE §16, unchanged). `auth` is unchanged. `public`
composes with `cross_tenant` and with `emits(...)` exactly as a bare route did.

**Superseded 2026-09-28** by `2026-09-27-job-tenancy-design.md` §3–§4: `authorize` takes
`&Principal`, so there is no `None` to admit. A caller with no credential at a scope line
gets a named 401, unless the route declares `public(Org)`.

**`public` + `cross_tenant` is allowed.** DECISIONS §11: a capability the platform refuses
to name does not disappear, it moves where the oracle cannot see it. An anonymous
cross-tenant read is the maximal-leak shape, and that is exactly why it must be declared
and enumerable rather than inexpressible: `charpente describe` already prints
`auth=false cross_tenant=true` for it, and that line is the audit.

**`describe` is unchanged.** `RouteInfo::auth: bool` keeps its name and wire shape; after
this it means "declared `auth`" rather than "opted in", and `false` means "declared
`public`". The §13 golden (`describe_contract`) and the generated TypeScript client do not
move — `public` is wire-identical to a bare route.

## §3 Tables: `scoped_by = X` | `global`, neither is the default

`#[table]` gains one bare word, `global`, composable with `version = N` in either order.
Exactly one of `scoped_by = X` / `global` appears on every table. The parser refuses the
other three states:

```text
`Ticket` declares no tenancy — a table says which: write `#[table(scoped_by = Org)]` if every row belongs to one tenant (the key becomes `(scope, id)` and a bare `Tx` cannot read it), or `#[table(global)]` if its rows belong to no tenant. Neither is the default.
`global` and `scoped_by = Org` on one table — a table is one or the other; drop one
`global` given twice — write it once
```

The first error is at the attribute (`Span::call_site()`, which for an attribute macro is the
`#[table(…)]` invocation itself, whatever its argument list), with the struct name interpolated. The example
scope name in the fix is `Org` when the crate has not told us better — the macro sees one
struct, not the app — and the message says "if every row belongs to one tenant", which is
the question the reviewer would have asked. The `#[table]` argument-list message becomes
"takes `version = N`, `scoped_by = <Type>` or `global`".

**Semantics.** `global` emits exactly what a bare `#[table]` emits today: `Scope =
Unscoped`, `impl UnscopedRow`, the `u64` row key, `scoped=false` in `describe`. No
type-level change, no key-format change, no migration, no `migrate_schema` interaction.
The scope type itself (`Org`) is `global` — it is the tenant registry. The membership
table (`Agent { org: Id<Org>, subject }`) is `global` — `Tenant::authorize` reads it
through a bare `&Tx`, so it cannot be scoped by the tenant it decides membership of. Both
are the honest declaration, and both are why the field-type rule was the wrong one (§4).

GUIDE §16's "unscoped tables stay reachable" through `(*tx)` reads the same with `global`
in place of `unscoped`; `ScopedTx`'s deref and the one-scope-per-transaction contract are
untouched.

**`describe` is unchanged.** `TableInfo::scoped: bool` keeps its name; `false` now means
"declared `global`".

## §4 The detection ruling: why universal stance

PLAN left "how 'should be' is detected" open. Three candidates were weighed; the first is
chosen and the other two are recorded so they stay weighed.

**Universal stance (chosen).** Every `#[table]` declares. Detection is trivial and local —
the macro looks at its own argument list — so it needs no type-level machinery, no sealed
trait, no coherence trick. It fires on the raw helpdesk, which declares no `impl Tenant`
at all (it predates §16), because it does not need one. And it is the same move as
`auth`/`public`: the platform cannot know whether `Ticket` is tenant data; it can only
refuse to guess. "The framework is the reviewer" means the framework asks — at every table,
at compile time — not that it infers.

**Scope-anchored, by field type (rejected).** Once a scope type is declared, an unscoped
table holding an `Id<Scope>` must be `scoped_by` it or say otherwise. Precise-looking and
near-zero churn for a single-tenant app. Rejected on three counts: (1) it cannot fire on
the oracle — the helpdesk declares no scope, so the oracle would degrade to "helpdesk plus
one `impl Tenant`", weaker than PLAN states; (2) its first false positive is the canonical
multi-tenant table — the membership table must be unscoped and must hold `Id<Org>` — so a
`global` override is needed anyway, at which point the rule is universal stance plus a
heuristic; (3) "is a scope" is not visible to the field's macro from a hand-written
`impl Tenant`, so it needs a sealed `IsScope` supertrait and a `#[table(scope)]` spelling,
i.e. a §16 surface change to buy a check that (2) already undercuts.

**Both (deferred).** Universal stance as the rule plus the field-type check as a second
oracle on `global` tables that hold an `Id<Scope>`. Strictly more checking, strictly more
surface. Deferred until a docs-only build shows a leak passing through a `global`
declaration; that is the reopening trigger, and it goes in PLAN's v1.x table if it ever
fires.

## §5 Oracles

**The PLAN oracle, run literally, once.** Rebase `docs-only-helpdesk` (`8913038`) onto the
finished main in a throwaway worktree and `cargo check -p helpdesk`. Under universal
stance the rebased app fails at all eight of its tables and at every route without `auth`
(`create_org`, `create_agent`, `login`). The ledger entry records the full error list and
calls out the two Critical sites by line:

- `examples/helpdesk/src/lib.rs:473` — `#[route(POST "/agents/:id")]`, neither `auth` nor
  `public`. Critical 1.
- `examples/helpdesk/src/lib.rs:134` — `#[table(version = 2)] struct Ticket`, no tenancy.
  This is the declaration Critical 2 cascades from: once `Ticket` is `scoped_by = Org`,
  the `tx.get(id)?.is_some()` at `:542` (and the same shape at `:463`, `:475`, `:644`)
  becomes §16's existing E0277 at the call line — the error the feature exists to produce.

This is a one-off, human-witnessed run: CI cannot depend on an unmerged branch, and the
branch stays unmerged (`docs/experiments/2026-09-helpdesk/README.md` says why). The
ledger is the record.

**The oracle that stays in CI: seven trybuild fixtures**, pinned stderr, in
`crates/charpente-macros/tests/compile_fail/`:

| Fixture | Pins |
|---|---|
| `helpdesk_route_neither_auth_nor_public.rs` | the helpdesk's `create_agent` lines, verbatim, header comment citing `8913038:examples/helpdesk/src/lib.rs:471-490` |
| `helpdesk_table_no_tenancy.rs` | the helpdesk's `Ticket` declaration, verbatim, citing `:134-143` |
| `route_auth_and_public.rs` | the one-or-the-other refusal |
| `route_duplicate_public.rs` | the given-twice refusal |
| `table_global_and_scoped_by.rs` | the one-or-the-other refusal |
| `table_duplicate_global.rs` | the given-twice refusal |
| `table_bare.rs` | the empty-argument-list path — a bare `#[table]` (added in review round 1) |

The two helpdesk fixtures pin the first message of each surface with real code; the five
minimal ones pin the other three states — one-or-the-other, given-twice, and the bare
attribute. trybuild's `compile_fail` is two-sided by
construction: a mutant that drops either refusal makes a fixture compile, and that is a
test failure, not a silent pass.

**Mutation instrument.** `scripts/mutants.sh --file crates/charpente-macros/src/route.rs
--file crates/charpente-macros/src/lib.rs` after the sweep, every viable mutant on the new
parse paths caught. Recorded in the ledger with the run's counts; a MISSED is a property
to write, not an exclusion to add.

**Existing gates that must not move.** `describe_contract` (the §13 golden is unchanged
because neither `describe` field changes), `ts-fixture-app`'s staleness diff (the client
is unchanged), `doc_inventory`, `hook_census`, `mutants_exclusions` (no new exclusion).

## §6 Docs, in the same commits (hard rule 6)

- **GUIDE §5** ("What `#[route]` rejects"): the rule, the first error with a full example,
  `public` in the route-shape prose. **GUIDE §2**: the same for `#[table]` and `global`.
  **GUIDE §16**: "global" for "unscoped" where it names the declaration; the `Org` and
  `Memo` examples carry `global` / `scoped_by`; the `count_memos` route stays `auth`.
- **GUIDE §9** error index: the `#[route]` and `#[table]` catalog blocks gain the six lines
  above. What `doc_truth` holds to the code are the two new pinned blocks in §2 and §5 (the
  same six lines in `<placeholder>` form); §9's catalog is unpinned because its instance
  values (`Ping`, `get_note`) cannot be placeholder-split, and the pinned count stays a
  floor.
- **Every GUIDE code block** compiles under `guide-examples`' build.rs, so every `#[route]`
  and `#[table]` in the guide is swept in §7 — a missed one is a red build.
- **DESIGN §7**: the agent-interface list says a route *declares* `auth` or `public`.
  **DESIGN §8**: the tenancy paragraph says a table *declares* `scoped_by` or `global`,
  and that absence is a compile error — the sentence that makes this a "§7/§8 change".
- **DECISIONS.md, new entry §13: "No surface has a permissive default (2026-09-16)".** The
  ruling: a declaration's absence is a compile error naming the choices, never a quiet
  default — binding future `#[route]`, `#[table]` and `#[job]` capabilities so a third
  surface does not reopen this. It cites §11 (declared, enumerable) as the shape it
  extends and records the two rejected detection rules from §4. Reopen if: a surface
  appears where the only honest default is "always on" with no second choice to name.
- **`2026-08-30-tenant-scoping-design.md` §8**: a dated one-line "superseded by this spec"
  under the opt-in non-goal. Specs are the historical record; the line says where the
  ruling moved.
- **README**: the status paragraph and any guarantee sentence that says "opt-in" for auth
  or tenancy. `docs/LEDGER.md`: the entry, with the oracle run and the mutants counts.

## §7 The sweep

Every `#[route]` in the tree that lacks `auth` gains `public`; every `#[table]` that lacks
`scoped_by` gains `global`. Counts at the time of writing: routes, ~103 in `crates/` +
`examples/`, 31 in GUIDE, 1 in `crates/charpente-cli/templates/lib.rs`; tables, ~140,
~50, 1. This includes charpente's own built-in tables (the macro is used in-crate via
`extern crate self`), the sim-fixture, ts-fixture-app, guide-examples, invoicing, the
bench and edge harnesses, and every trybuild fixture that is meant to compile past the
attribute.

**Order is the verification, and every commit is green.** The words are accepted first
(`public`, `global` — no-ops in expansion), the sweep adds them everywhere second, and the
refusal of their absence lands third: that third commit is where the compiler proves the
sweep found every site, and no commit in between leaves the workspace red. The `charpente
new` template and GUIDE §1's block are held byte-identical by test, so both change together
or the test says so.

## §8 Refused, so the plan does not grow them

- **A typed `&Principal` handler parameter on `auth` routes.** `ctx.principal()` stays
  `Option<&Principal>` and the `.expect("auth route")` idiom stays. Reopen if the third
  docs-only build trips on it.
- **The field-type rule** (§4, rejected) and **both** (§4, deferred).
- **Refusing `public` + `cross_tenant`** (§2; DECISIONS §11 forbids making it
  inexpressible).
- **RBAC / permissions.** Unchanged from the tenant-scoping spec's non-goal.
- **Any change to `Tenant::authorize`'s `Option<&Principal>`.** Anonymous scope access
  stays the app's declared policy.
  **Superseded 2026-09-28** by `2026-09-27-job-tenancy-design.md` §2 R2: the parameter is
  `&Principal`, and `Principal`'s fields are `pub(crate)`.
- **Any `describe` schema change.** The two booleans keep their names and wire
  shape; only their prose reading moves.
- **A runtime or mount-time half.** Both refusals are compile-time only; there is no
  `App::mount` check because there is nothing left to check by then.
- **A `#[job]` change.** Jobs have no caller; `cross_tenant` on a job is already a
  declaration. Nothing here touches `job.rs`.
  **Superseded 2026-09-28** by `2026-09-27-job-tenancy-design.md` §5: a job declares
  `global` or `scoped_by = Org`, and neither is the default.

## §9 Exit criteria

1. The three CI commands green, counted (`grep -c "^test result: ok"`), with the seven new
   compile-fail fixtures pinned (ten with the three added by the final review) and the
   existing sixty fixtures (forty compile-fail, twenty compile-pass) unchanged in meaning.
2. The rebased helpdesk fails to compile at `lib.rs:473` and `lib.rs:134`, the full error
   list in the ledger.
3. `scripts/mutants.sh` over the two parse files: every viable mutant caught, counts in
   the ledger, no new entry in `.cargo/mutants.toml`.
4. GUIDE §2/§5/§9/§16, DESIGN §7/§8, DECISIONS §13, README and the tenant-scoping spec's
   superseded line all in the same commits as the code they describe.
5. `charpente new` produces a crate that compiles, and its template is byte-identical to
   GUIDE §1.
