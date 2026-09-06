# Oracle density: the reviewer reviews itself — design

**Status:** draft, 2026-09-06. Authority for `docs/superpowers/plans/2026-09-06-oracle-density.md`.

## 1. Why this sub-project exists

MANIFESTO §2 states the product plainly: *"the highest-leverage artifact is not more code.
It is the oracle substrate."* §9 stakes the wager on one sentence — being able to say
*"every path through this system has been executed, under fault, thousands of times, and
here is the seed to prove it."*

Two findings from the last week say that sentence is not currently supportable, and they
are the same finding seen from two sides: **green does not yet mean what the wager needs
it to mean.**

**Finding A — the simulator gives up silently.** `finish_universe`
(`crates/charpente/tests/replication.rs:253`) is shared by five properties and
discards an `INCONCLUSIVE` universe into `sim.cover()`. One run recorded
`repl:inconclusive` at 90, 39, 45, 109 and 89 across those five, over universe sets of
256/256/256/128/128 — so if the 109 lands on a 128-universe property, that property gives
up on **85%** of its universes and still reports green. Only
`follower_converges_to_the_primary_fingerprint` is attributed and floored
(`CONVERGED_FLOOR = 167`, `FINGERPRINT_FLOOR = 17`). A sixth wrapper,
`repl:failover-inconclusive`, has four `cover(); return Ok(())` sites and **no `require:`
at all** (docs/LEDGER.md, 2026-09-06).

Two facts sharpen the work and were established by survey rather than assumed. First, the
*cause* keys already exist — `repl:inconclusive:spawn-exhausted` (:240),
`:role-file-torn` (:327), `:store-bricked` (:382) and `:recover-exhausted` (:402) are
credited at the failure sites — so attribution is a matter of running each property alone
and reading its coverage, not of inventing instrumentation. Second, of the five properties
only two carry a meaningful `require:` set, one (`failover_preserves_everything_the_
follower_acked`, 128 universes) has **no `require:` at all**, and `repl:logical-converged`
is unreachable by construction in two of them — so a floor on that key would be an
unfireable assertion in exactly the shape this sub-project exists to
eliminate — green because the failing state is unreachable, not because the system is
correct. Each property needs a floor on a key its own oracle can actually reach.

**Finding B — the reviewer missed what an outsider got wrong.** The docs-only helpdesk
experiment (`docs/experiments/2026-09-helpdesk/`) had an agent build a multi-tenant app
from the docs alone. It shipped an unauthenticated `POST /agents/:id`, an unscoped
`tx.get(id)?.is_some()` usable as a cross-tenant existence oracle, and **five sim
properties that are all one-sided — deleting the entire SLA sweep loop leaves the suite
green.** It also surfaced a platform-level oracle defect (`checks::acked_implies_durable`
building its own `Sim`, so its fault matrix was off and `require: ["restart"]` could never
fire) that reading the platform's own tests would never have found.

MANIFESTO §6 claims *"the framework is the reviewer."* Finding B is the measurement of that
claim against a real outside agent, and it came back with three misses. Finding A says the
instrument used to make such claims does not report when it fails to reach a verdict.

**This sub-project closes both.** It ships no new platform capability. That is deliberate:
per MANIFESTO §2 the feature list is not the product, and adding a third oracle while two
have known holes is the wrong order.

## 2. Scope, in dependency order

1. **The give-up rate becomes a reported number** — attribute the unattributed properties,
   port the known-good retry shape to the two helpers that lack it, set floors, and make
   "gave up without a verdict" a first-class outcome the runner prints rather than a
   coverage string that vanishes.
2. **`describe` enumerates what an app did not declare** — unscoped tables, and a
   mechanical oracle for the one describe surface that currently has none. **Half of what
   this item was scoped to build already exists** (R4).
3. **Mutation testing for app code** — a mode that neuters an app's own handlers and
   reports which sim properties stay green, so a one-sided property is caught mechanically
   instead of by a reviewer who happened to look.
4. **Compile latency, measured** — PLAN.md's risk table calls it the accepted headline
   risk and DESIGN §9.5 calls it *"the adoption risk"*; it has never been measured. A
   reading lands in docs/BENCH.md under BENCH's own method rules.

Item 1 comes first because item 3 reports through the same outcome channel item 1 builds.
Item 4 is last because it requires a quiet machine and must not contend with test runs.

## 3. Rulings taken before implementation

**R1 — enumeration first, declaration later, for both describe inventories.** The obvious
louder move for the unauthenticated-route finding is to make `#[route]` require an explicit
`public` marker, so "no auth" is declared rather than defaulted. That is refused *for now*,
on the doc set's own precedent: DECISIONS §11 and §12 rule that the pattern is to make a
capability enumerable first and to build the louder notch *"when declarations at the route
prove too quiet in practice."* Enumeration is non-breaking, ships in one task, and is the
evidence that would justify the notch. A macro change that reddens every route in
`examples/invoicing`, the fixtures and GUIDE is not justified by one experiment.
*Cost if wrong:* an app can still ship an unauthenticated route without a compile error;
the inventory only catches it if someone reads the inventory.

**R2 — a branch in the main worktree, not a second worktree.** The disk is at 96% (20 GiB
free) against a 24 GB `target/`; a second worktree duplicates that build, and the ledger
already records two OOM kills on this machine on 2026-09-06. Nothing else is in flight to
isolate from (verified: one worktree, clean tree, no stash). *Cost if wrong:* a concurrent
session editing the tree collides with this work — mitigated by re-checking `git status`
before each commit and committing named paths only.

**R4 — the unauthenticated-route half of item 2 is already built; scope moves to what is
actually missing.** The recommendation this plan came from proposed making
"routes that require no authentication" enumerable in `describe`. Survey found it shipped
long ago: `RouteInfo.auth: bool` (`crates/charpente/src/describe.rs:76`) predates
`cross_tenant`, is fed by the real `#[route(…, auth)]` flag
(`crates/charpente-macros/src/route.rs:56`), is the actual runtime gate
(`crates/charpente/src/http/app.rs:817`), and is pinned in three golden JSON files. In
`examples/invoicing`, `login` (`routes.rs:56`) carries no flag while `create_customer`
(`routes.rs:61`) does. `cross_tenant`'s own doc comment sits "beside `auth` because it is
the second thing an audit asks of a route."

**That is a more interesting result than the feature would have been.** The helpdesk agent
shipped an unauthenticated route *while the inventory that would have named it already
existed*. So the gap is not enumeration — it is that nothing makes an app author confront
the list. Under DECISIONS §11/§12 that is precisely the recorded trigger for the louder
notch: the moment an audit has to read source, the inventory has stopped being an
inventory.

The notch that would have caught it is an **app-level default declared at the mount site** —
the shape §11 already names — where an app opts into "auth required unless a route says
`public`", leaving existing apps untouched. **That is new public API and new product
surface, so this sub-project does not build it.** It is written up here, flagged for Noah's
ruling, and left out of scope. What ships instead is the genuinely missing inventory
(`TableInfo.scoped`) and a mechanical oracle for the describe surface that has none.
*Cost if wrong:* the auth notch waits for a decision that could have been made now.

**R5 — GUIDE §13's describe JSON is in scope as an oracle gap.** `docs/GUIDE.md` §13
carries a full `describe` JSON block claiming byte-for-byte parity with real output, and
nothing checks it: `guide-examples`' build.rs extracts only ` ```rust ` fences, so that
block drifts silently. A doc that claims parity with no oracle behind it is the exact
defect class this sub-project exists to close, and hard rule 6 makes it ours the moment we
add a field to that output. *Cost if wrong:* a small amount of work spent on docs
machinery rather than core.

**R3 — this sub-project does not merge itself.** Merge to `main` is Noah's call, as it was
for tenant-scoping. The branch ships complete and reviewed; it stops there.

## 4. Non-goals

- No new platform capability: no blob store, no WebSockets, no PG-wire, no LSM work.
- No change to the tenancy guarantee's boundary (DECISIONS §12 stands as ratified).
- No `#[route(public)]` macro change (R1).
- No retrofit of existing properties beyond the six wrappers named in Finding A.
