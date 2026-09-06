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

## Attribution, measured (2026-09-06)

Each of the five properties was run alone, in the foreground, one `cargo test` invocation
per property, `CHARPENTE_SEED` never set (full command list below). Coverage was read off
each run's own printed summary, so there is no interleaving ambiguity.

| Property | Universes | `repl:inconclusive` | `:spawn-exhausted` | `:role-file-torn` | `:store-bricked` | `:recover-exhausted` | Give-up rate |
|---|---:|---:|---:|---:|---:|---:|---:|
| `follower_converges_to_the_primary_fingerprint` | 256 | 89 | 0 | 0 | 0 | 89 | 34.8% |
| `late_follower_seeds_below_the_gcd_base_and_converges` | 256 | 109 | 0 | 0 | 0 | 109 | 42.6% |
| `switchover_mid_write_load_loses_nothing_and_serves_reads_throughout` | 256 | 45 | 0 | 2 | 0 | 43 | 17.6% |
| `failover_preserves_everything_the_follower_acked` | 128 | 39 | 0 | 0 | 0 | 39 | 30.5%† |
| `wal_corruption_on_the_primary_no_longer_loses_acked_records_with_a_follower` | 128 | 90 | 0 | 0 | 0 | 0 | 70.3% |

†**Correction (fix round 1, 2026-09-06):** `repl:inconclusive` and `repl:failover-inconclusive`
are **mutually exclusive per universe** for this property, not independent as first reported.
A code trace of `failover_universe` shows all four `repl:failover-inconclusive` sites
`return Ok(())` immediately, so a universe that hits one of them never reaches
`finish_universe`'s `Err(e) if e.contains(INCONCLUSIVE)` arm — and `repl:inconclusive` fires
only there. The two outcomes cannot both occur in one universe, so their union is derivable
and is this property's real give-up rate: **82/128 = 64.1%**, decomposed into two mutually
exclusive components — 30.5% (`repl:inconclusive`, all `:recover-exhausted`) plus 33.6%
(`repl:failover-inconclusive`, detailed below). **64.1% is this property's headline give-up
rate.** 30.5% is what the table column shows because it is the only component this task's
four cause keys reach; the SE comparisons later in this section use that same 30.5%
component (the one rate all five properties share a key for), not the 64.1% union, since no
equivalent second wrapper has been measured for the other four properties.

`:spawn-exhausted` and `:store-bricked` never fire in any of the five properties measured.
The fifth property's four zeros are **not** a clean bill of health: it doesn't route through
the instrumented `Db`-level helpers at all. It calls a parallel kernel-level family
(`spawn_kernel_node_with_schema`/`open_kernel`/`recover_kernel_node`,
`replication.rs:4436-4665`) that mirrors the same failure shapes (torn witness/role file,
store-bricked, spawn/recover exhaustion) but never marks any of the four cause keys — so its
90 give-ups reach only the generic `finish_universe` catch-all, with zero cause-level
attribution available from existing instrumentation. `open_kernel`'s torn-role-file arm also
never covers a cause key at all, unlike `open_node`'s equivalent arm.

**The ledger's open question, answered.** The measured counts — `{89, 109, 45, 39, 90}` —
are **the same five numbers the ledger recorded** (90, 39, 45, 109, 89), now attributed one
property at a time instead of read off an interleaved run: no discrepancy to report. The 109
lands on `late_follower_seeds_below_the_gcd_base_and_converges`, a **256**-universe property,
not a 128-universe one, so its rate is 42.6%, not the speculated ~85%. **No property gives up
at ~85% of its universes** — the highest measured rate is 70.3% (the 90, on the 128-universe
`wal_corruption...` property; `failover_preserves_everything_the_follower_acked`'s 64.1%
union, above, is the next-highest).

Comparisons below use each property's `repl:inconclusive`-only rate — the one key all five
properties share — via the **two-sample combined standard error**,
`SE = sqrt(p1(1-p1)/n1 + p2(1-p2)/n2)`, reported as the number of combined SEs the two rates
are apart (not one property's own SE applied to a raw-count difference, which understates
uncertainty). Bucketing: **indistinguishable** under ~1 combined SE, **suggestive** around 2,
**clear** past ~3. The 30.5%-vs-70.3% gap between the two 128-universe properties
(`failover`'s component rate vs `wal_corruption`) is a 39.8-percentage-point gap against a
~5.7-point combined SE: **~6.95 combined SEs** — clear, well past noise, though not the
order-of-magnitude an earlier, miscalculated version of this claim implied (that version
compared a raw *count* difference of 51 against one property's own ~5-count SE). The
42.6%-vs-34.8% gap between the two largest 256-universe properties (`late_follower_seeds` vs
`follower_converges`) is **~1.82 combined SEs** — near the noise floor, suggestive at best,
not the "probable" difference an earlier version of this claim called it. Full pairwise
combined-SE figures for all five properties, including one pair that is statistically
indistinguishable (~0.85 combined SEs), are in
`.superpowers/sdd/2026-09-06-oracle-density/task-2-report.md`'s fix-round section.

**The sixth wrapper, `repl:failover-inconclusive`** (exists only on
`failover_preserves_everything_the_follower_acked`): **43** over the same 128-universe,
unmodified run (33.6% of universes). Its four `cover(); return Ok(())` sites share one
literal string, so a one-time, fully-reverted instrumentation pass (distinct sub-keys per
site, re-run, then `git checkout --` restoring `replication.rs` to its committed content —
verified clean by `git status`/`git diff --stat`) attributed them: site 1
(`:4000-4003`, the pre-crash staging gate) fired **41/43 (95.3%)**, site 3 (`:4014-4023`, the
promote round timing out) fired **2/43 (4.7%)**, sites 2 (`:4004-4007`) and 4
(`:4033-4036`) fired **0**. The instrumented re-run reproduced byte-identical
fault/restart/`repl:inconclusive` counts to the uninstrumented run, confirming the sub-key
edit changed no behavior. This property carries no `require:`/`floor:` at all, so none of
these four sites (nor the property's one fireable `repl:inconclusive` cause,
`:recover-exhausted`) are checked for ever firing — a regression silencing any of them would
still read green.

**Also observed, out of this task's scope:** the switchover property carries two more
give-up-shaped keys not among the four cause keys or the sixth wrapper —
`repl:switchover-inconclusive` (131/256, 45.8%) and `repl:switchover-stalled` (8). Unlike
`repl:failover-inconclusive`, `repl:switchover-inconclusive` does not unconditionally
`return Ok(())` — it still runs `assert_one_writable` — so it is a reduced-oracle path, not
a full bypass. Flagged as a candidate for the same attribution treatment, not attributed
here.

Commands used (verbatim):

```bash
set -o pipefail
for p in follower_converges_to_the_primary_fingerprint \
         late_follower_seeds_below_the_gcd_base_and_converges \
         switchover_mid_write_load_loses_nothing_and_serves_reads_throughout \
         failover_preserves_everything_the_follower_acked \
         wal_corruption_on_the_primary_no_longer_loses_acked_records_with_a_follower; do
  cargo test -p charpente --test replication "$p" -- --exact --test-threads=1 --nocapture \
    2>&1 | tee "/tmp/attr-$p.log"
done
```

Runtimes (table order): 195.09s, 167.29s, 102.93s, 67.65s, 42.29s — all five well inside the
600s per-invocation ceiling; none deferred, no universe count reduced. Full detail, per-site
guard text, and the additional switchover keys are in
`.superpowers/sdd/2026-09-06-oracle-density/task-2-report.md`.
