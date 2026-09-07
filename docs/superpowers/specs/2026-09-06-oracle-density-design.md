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
| `failover_preserves_everything_the_follower_acked` | 128 | 39 → 82† | 0 | 0 | 0 | 39 | 30.5% → 64.1%† |
| `wal_corruption_on_the_primary_no_longer_loses_acked_records_with_a_follower` | 128 | 90 | 0 | 0 | 0 | 0 | 70.3% |

†**Correction (fix round 1, 2026-09-06), then superseded by task 4 (commit `a90b5dd`).**
*As measured above*, `repl:inconclusive` and `repl:failover-inconclusive` were **mutually
exclusive per universe** for this property, not independent as first reported: all four
`repl:failover-inconclusive` sites `return Ok(())` immediately, so a universe that hit one of
them never reached `finish_universe`'s `Err(e) if e.contains(INCONCLUSIVE)` arm — and
`repl:inconclusive` fires only there. The two outcomes could not both occur in one universe,
so their union was derivable and is this property's real give-up rate: **82/128 = 64.1%**,
decomposed into two then-exclusive components — 30.5% (`repl:inconclusive`, all
`:recover-exhausted`) plus 33.6% (`repl:failover-inconclusive`, detailed below). **64.1% is
this property's headline give-up rate.**

**The exclusivity ended with task 4.** All four sites now return `INCONCLUSIVE`-marked `Err`s
carrying per-site cause keys, so they route through `finish_universe` like every other give-up
in the file, and `repl:inconclusive` **measures** the 82 directly instead of it being derived:
41 `:failover-staging-gate` + 2 `:failover-promote-timeout` + 39 `:recover-exhausted`.
`repl:failover-inconclusive` (43) is now a *subset* of `repl:inconclusive`, not a disjoint
channel. The `39` / `30.5%` in the table row are the pre-conversion values, kept because the
row records what that run measured. The SE comparisons later in this section still use the
30.5% `:recover-exhausted` component (the one rate all five properties share a key for), not
the 64.1% union, since no equivalent second wrapper has been measured for the other four
properties.

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
promote round coming back inconclusive — characterised here as "timing out", which the fix
round's `drive_ctrl` keys later disproved: both firings were `:ctrl-conn-lost`) fired
**2/43 (4.7%)**, sites 2 (`:4004-4007`) and 4
(`:4033-4036`) fired **0**. The instrumented re-run reproduced byte-identical
fault/restart/`repl:inconclusive` counts to the uninstrumented run, confirming the sub-key
edit changed no behavior.

**Superseded in part (tasks 3 and 4).** At survey time this property carried no
`require:`/`floor:` at all, so nothing here was checked for ever firing. Task 3 gave it
`require: ["restart", "crash:host", "repl:failover-prefix-compared"]` and
`floor: [("repl:failover-prefix-compared", FAILOVER_PREFIX_FLOOR)]`, and task 4's mutation
proofs fired that `require:` by name (`["crash:host", "repl:failover-prefix-compared"] never
occurred over 128 universes`). **The distinction that survives is what that gate does and
does not reach.** The `require:`/`floor:` are on the *verdict* key, so a regression that
collapses this property's reach — whatever silences it — is caught. The four cause keys and
`:recover-exhausted` are **still ungated**: a regression that silences one *individual* cause
while the verdict key holds above its floor would still read green. Attribution here buys
diagnosis, not detection, for the individual causes.

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

## Floors, and the budget question, measured (2026-09-06)

Task 3 gave each of the five properties a floor on a key its own oracle can reach. A floor is
`require:`'s at-least-once generalized to an at-least-N *measured* rate, and it goes on a
**verdict** key — one that means "this universe reached a real conclusion" — never on a
give-up key, whose growth is the thing being guarded against.

| property | universes | verdict key | measured | floor | tolerated fall in reach |
|---|---:|---|---:|---:|---:|
| `follower_converges_to_the_primary_fingerprint` | 256 | `repl:logical-converged` (+ `repl:fingerprint-compared`) | 167 (17) | 167 (17) | 0% |
| `late_follower_seeds_below_the_gcd_base_and_converges` | 256 | `repl:logical-converged` | 145 | 129 | 11.0% |
| `switchover_mid_write_load_loses_nothing_and_serves_reads_throughout` | 256 | `repl:logical-converged` | 72 | 57 | 20.8% |
| `failover_preserves_everything_the_follower_acked` | 128 | `repl:failover-prefix-compared` | 46 | 35 | 23.9% |
| `wal_corruption_on_the_primary_no_longer_loses_acked_records_with_a_follower` | 128 | `repl:promised-nonempty` | 32 | 22 | 31.3% |

The four new floors carry **2 binomial SEs** of headroom below the measurement (SE_count =
sqrt(n·p·(1−p)): 7.9, 7.2, 5.4, 4.9). `CONVERGED_FLOOR`'s existing zero-headroom convention
answers a different question — "did anything drift in an exact replay" — and is deliberately
left alone; the new ones must also survive a *re-rolled* fault stream, since any change that
shifts fault draws makes the count a fresh binomial draw. The right-hand column is the price:
a 2-SE floor tolerates that much relative decay in the oracle's reach before firing, and the
tolerance is loosest where the verdict count is smallest. Every floor was mutation-proved
(raised one above its measurement → RED naming the key → restored → GREEN).

Two of the five could not reach `repl:logical-converged` at all, so flooring that key there
would have been an assertion that can never fire — the defect this sub-project exists to
remove. `failover_preserves_everything_the_follower_acked` had **no `require:` at all** and no
verdict key: its prefix loop can run zero times and return `Ok` having compared nothing. It
now keys the real and vacuous outcomes apart (`repl:failover-prefix-compared` 46,
`repl:failover-prefix-empty` 0).

**The kernel-level helper family is now instrumented.** The gap Task 2 named —
`open_kernel`/`spawn_kernel_node_with_schema`/`recover_kernel_node` and the control-conn and
restore helpers never marking a cause key — is closed, so the `wal_corruption…` property's 90
give-ups are attributed for the first time: 78 `:survivor-faulted`, 5 `:recover-exhausted`,
5 `:spawn-exhausted`, 3 `:restore-reopen`, 1 `:request-no-response`. Cause sums equal the
aggregate for the `Db`-level family; where a caller catches an inconclusive and continues
(the kernel promise scan, the switchover control conn) they are an upper bound, which is
noted at those sites.

### The budget question: narrowed, not answered

The ledger's 2026-09-05 question — whether `RECOVER_ATTEMPTS` is too small or masks a
liveness bug — was tested by raising it 500 → 5000 (temporary local edits, reverted) on all
four floored properties, and by gating the per-attempt power-cycle in two helpers still on
the shape Task 1 removed elsewhere — one of which was then deliberately reverted, for reasons
recorded below.

**What a 10× budget bought: nothing, four times over.** Give-ups moved *between* channels and
the verdict counts did not follow — `repl:logical-converged` 145 → 149 and 72 → 73,
`repl:failover-prefix-compared` 46 → 46, `repl:promised-nonempty` 32 → 32, each move well
inside one SE. On `switchover…` the give-up channel did collapse (`:recover-exhausted`
43 → 15) and the rescued universes landed in the *reduced-oracle*
`repl:switchover-inconclusive` path (131 → 158) rather than in a verdict. Runtime cost:
5.1× to 9.1×. **Raising the budget is not the fix, and the floors are set against the current
one.**

**The gated power-cycle is a RECOVERY fix and does not generalize to a spawn — measured, and
one port was reverted because of it.** `reopen_p_frames` was still restarting the host before
every one of its 500 attempts; gating it changed nothing measurable
(`failover_preserves_everything_the_follower_acked`, 128 universes: every coverage count
byte-identical, and the new `repl:inconclusive:reopen-p-exhausted` key reads 0 — the loop had
never taken a second attempt in those universes), so the fix stands as a latent-hazard
removal. Porting the same gate to `spawn_kernel_node_with_schema` made things **worse**:
`repl:inconclusive:spawn-exhausted` moved 5 → 19 (≈4 SE at n=128) while the property's give-up
total stayed at exactly 90 and its floored key at exactly 32 — the same universes dying
earlier, having exercised less. That port was reverted, the ungated state reproduces the
baseline exactly (A/B/A), and the reason is now in the loop's own comment: **both halves of
the trade invert for a spawn.** The gate's payoff is preserving a recovering node's
accumulated WAL-replay progress, and a fresh create has none; its cost is that a sim disk
whose `crash_disk_after` has fired answers every operation with `crashed_error` — whose text
contains "simulated", which `checks::classify` reads as injected fallout — so the retry loop
spins against a latched-dead disk until a restart clears `disk.crashed`, and under the gate 7
of every 8 attempts are spent that way. Task 1's `settle_boot` measurement (0 successes in
20 000 boots) is about recovery boots and does not transfer to a spawn loop. **Anyone
"fixing" that site to match its siblings should re-run that A/B first.** The remaining
every-attempt site, `recover_kernel_node`, is a genuine recovery loop where the measurement
does transfer; it is carried as an explicit follow-up and threatens no floor here (5 of 90
give-ups, and every floor is on a verdict key a fix could only raise).

**What the dominant give-up causes are.** For two properties they are rules written down in
the harness and checkable by reading it: the designated survivor is spawned
`recoverable: false` because the single-follower promise needs one undamaged copy (78 of 90
give-ups), and the failover staging gate rejects a subject that already needed a recovery
before the deliberate crash (41 of that channel's 43).

**What is still open.** `late_follower_seeds…`'s 109 give-ups are all `recover_node`
exhaustion and a 10× budget rescued only 4 of them. That is equally consistent with disks the
fault injector holds unrecoverable and with a fault-triggered wedge the retry cannot clear;
the evidence gathered does not separate them, and an earlier draft of this section's claim
that it did was retracted (task-3-report.md §10.1). One data point bears on it without
settling it: the same helper under `switchover…`'s workload *does* largely clear with a
bigger budget (65% of its exhaustions rescued), so any wedge would have to be specific to the
seeding property's configuration or fault draws, not a universal property of that loop.
Naming this as open is the point — the floors pin verdict counts that did not move under any
intervention tried, so they do not depend on the answer, and if a wedge is later found and
fixed the floors go up.

## Mutation mode, ruled (2026-09-07)

**Ruling: (a) — wire `cargo-mutants` as a `charpente-cli` verb, modelled on the edge
harness. Option (b), a job-neutering mode inside `#[job]`, is rejected.**

### Why (b) is rejected

**1. It ships an escape hatch into the platform (hard rule 2, MANIFESTO §8).** Option (b)
puts a switch inside `charpente-macros` that makes a declared job *not do its work*. Gated
or not, opt-in or not, that switch exists in the crate whose emitted code IS the compile-time
contract — and "no feature flags on correctness" is the rule this repo says it cannot absorb
a violation of. `cargo-mutants` patches a copy of the source in a scratch directory; nothing
about the mutation exists in anything we ship. That difference is categorical, not stylistic.

**2. Its cheapness is illusory.** The survey's appeal for (b) is that neutering is cheap
because `#[job]`'s glue has a uniform `Result<(), JobError>` return. But selecting the mutant
by env var does not avoid a rebuild — it *hides* the need for one. Cargo's fingerprint does
not track env vars read during proc-macro expansion, and unlike a build script a proc macro
cannot declare `rerun-if-env-changed`. So (b) needs a bespoke rebuild-forcing mechanism plus
a test proving the rebuild happened (the plan's Task 9 Step 2 already demands exactly this),
and after paying for both it rebuilds `charpente` and every dependent anyway. The saving
evaporates; the machinery remains.

**3. The asymmetry that makes (b) hard does not arise under (a).** The survey's central
technical finding is that `#[job]` never sees the handler body while `#[route]` does but has
an app-chosen `Result<T, ApiError>` with no generic safe stub. That asymmetry is a property
of *macro-level* neutering. `cargo-mutants` mutates source, so it never needs a stub the
macro can synthesise: it derives a replacement from the signature and, where it cannot,
reports the mutant **unviable** by name rather than skipping in silence. Jobs and routes get
the same treatment for free — and route handlers, which (b) could not reach at all, are
covered from the first run.

**4. Reversibility is asymmetric, and that decides it under uncertainty.** (a) costs one CLI
verb and one script; if scoped mutation proves too slow to be useful, delete both and (b) is
still on the table with a measured number in hand. (b) puts a permanent branch into
`charpente-macros` — the first `cfg`/env branch either macro has ever had (both are at zero
today) — and once apps depend on the emitted behaviour it is far harder to withdraw. When
two options differ mainly in confidence, take the one whose mistake is cheaper to undo.

### Where the code lives (hard rule 1)

`charpente-cli` may spawn processes; core may not. The precedent is already in this repo at
`crates/charpente-cli/src/sim.rs:50` and `:127`, each carrying
`#[allow(clippy::disallowed_methods)]` above it with a comment stating that process spawning
is edge tooling outside the deterministic boundary (DESIGN §1). The mutation driver follows
that idiom exactly: `cargo-mutants` runs as a child process of the CLI and never links into
`charpente-core`.

**Stated explicitly so no reviewer has to infer it:** proc-macro expansion runs at compile
time, in the proc-macro process, not in the deterministic runtime — so option (b) would
**not** have violated hard rule 1. It is rejected on hard rule 2 grounds. Conflating the two
would be a wrong reason for a right conclusion, and the distinction matters if (b) is ever
revisited.

### The biggest risk, and the mitigation

**Cost, and therefore disuse.** A full rebuild per mutant on a proc-macro-heavy workspace
means minutes each; an instrument that takes hours is an instrument nobody runs, and a
mutation report nobody runs is worth exactly nothing. The mitigation is to copy the shape
DESIGN already sanctions for the edge harness: an expensive rig that lives outside the
deterministic boundary, is not part of the default suite, runs on demand through a script and
nightly in CI, and carries an explicit honesty contract about what it can and cannot claim.
Default scope is a **named target set** — the oracle-bearing code and the fixture handlers —
never the whole workspace.

**Two anti-vacuity requirements, both non-negotiable.** First, a baseline: `cargo-mutants`
must confirm the *unmutated* suite is green before any mutant counts, because a suite that is
already red catches everything for the wrong reason. Second, **timeouts are not catches.** A
mutant whose run exceeds the budget is unresolved, and reporting it as caught would be the
same lie this sub-project exists to remove — precisely the give-up-versus-verdict distinction
Task 5 built for the simulator, arriving here in a second instrument. The report must state
caught / missed / unviable / timed-out separately, and a timeout must never be summed into
"caught".

### What it costs if this ruling is wrong

If scoped `cargo-mutants` is still too slow to be run in practice, we will have spent one CLI
verb and one script, and we will know the real per-mutant number on this workspace — which is
the fact the survey could only estimate. Nothing in `charpente`, `charpente-core` or
`charpente-macros` needs unwinding, and option (b) remains available, now with evidence. The
converse mistake is not symmetric: had (b) been built first and proved unusable, the platform
would carry a job-neutering branch that is awkward to remove and impossible to un-ship.

## The `RECOVER_ATTEMPTS` call, answered (2026-09-07)

The ledger has carried one question open since the runtime-profile work: are the
`:recover-exhausted` give-ups **a budget too small** or **a liveness bug**? Two 10×
experiments recorded in `replication.rs` disagreed — `SWITCHOVER_CONVERGED_FLOOR` rescued 65%
of its exhaustions at `RECOVER_ATTEMPTS` 5000, `SEEDED_CONVERGED_FLOOR` rescued almost none
(109 → 105) — and neither could say why, because a give-up recorded no terminal state.

`recover_exhausted_key` adds that state: the family of the error the LAST attempt saw,
classified by the existing `charpente_sim::checks::classify`.

| Property | Universes | `:recover-exhausted` | `:injected` | `:poisoned` | `:redb-latch` | `:durable-horizon` |
|---|---:|---:|---:|---:|---:|---:|
| `follower_converges_to_the_primary_fingerprint` | 256 | 92 | **92** | 0 | 0 | 0 |
| `late_follower_seeds_below_the_gcd_base_and_converges` | 256 | 110 | **110** | 0 | 0 | 0 |

**Answer: neither, and the constant should not be raised.**

*Not a wedge.* A sticky state — redb's latch, an engine poison — is a give-up that retrying
cannot clear, and it is what a liveness bug would look like here. There are **zero** of them.
Every one of the 202 exhaustions ends on a freshly injected fault, so the recovery loop is not
stuck; it is failing anew each time.

*Not a budget either.* `:injected` dominance is the easy thing to misread, and the reason this
section exists rather than a raised constant. A fresh fault per attempt is consistent with bad
luck that a larger budget would outlast — but only if per-attempt success probability is
meaningfully above zero. P1 carries **109,852 injected write errors across 256 universes, ~430
per universe**. At that rate an open essentially cannot complete, every attempt fails freshly,
and more attempts buy nothing. The seeded sibling's 10× experiment measured exactly that: ~4%
rescued where a budget-limited loop would have been largely rescued.

So the honest reading is a third thing neither option named: these give-ups are a property of
**the fault rate the recovery loop is asked to open through**, in the same family as P5's
designated-survivor rule — a workload-model consequence, not a bug and not a constant. A future
change that wants these universes back should lower what the fault matrix does to an opening
node, not raise `RECOVER_ATTEMPTS`.

**Three comments were corrected in the same commit**, all of which asserted the budget reading
as established: both `:recover-exhausted` cover sites and (before it shipped) this key's own
doc comment, which claimed an `:injected` terminal error means a larger budget would rescue it.
That claim is refuted by the very experiment cited two paragraphs above it.

**Counts differ slightly from Task 2's table** (92 vs 89, 110 vs 109) for the recorded reason:
the directory-barrier stream split re-keyed every universe, so these are the same properties
over a renamed population, not a drift in behaviour.

## `auth.rs` give-ups, attributed (2026-09-07)

Task 4 gave `replication.rs` per-cause give-up keys. `auth.rs` never got them: its eight
`INCONCLUSIVE` sites all funnelled into one `auth:inconclusive` counter, so the suite could say
that **53 of 128 universes gave up (41.4%) and not one word about why**.

Eight per-site covers, mirroring replication's naming. Measured over 128 universes, base
`0x2EB3_0001`:

| cause | count | share of the 53 |
|---|---:|---:|
| `:recover-exhausted` | 35 | 66.0% |
| `:request-no-response` | 14 | 26.4% |
| `:spawn-exhausted` | 3 | 5.7% |
| `:needs-restore` | 1 | 1.9% |
| `:witness-torn` | 0 | — |
| `:local-unrestorable` | 0 | — |
| `:scan-list-faulted` | 0 | — |
| `:scan-read-faulted` | 0 | — |

**35 + 14 + 3 + 1 = 53, which is the aggregate exactly.** That sum is the check that matters:
every give-up is attributed and none falls through, so a future cause cannot hide inside the
total. Had it come up short, the difference would have been an unattributed path.

`:recover-exhausted` is **35 of 35 `:injected`**, the same shape replication measured — so the
same conclusion applies before anyone raises `AUTH_RECOVER_ATTEMPTS`: no sticky wedge, and a
fresh fault per attempt is not evidence of a budget.

**The four zeroes are a finding, not padding.** Those give-up paths never execute in this
population, so nothing about them is under test — including whether their error text still
matches what the classifier expects. They are the sites to look at first if auth's give-up mix
ever moves.

Threading `sim` into `list_view`/`read_view` (and their two scan wrappers) is what let all eight
covers be per-site. The alternative — one central classifier matching on error prose in
`finish_auth_universe` — needed no plumbing but would have re-coupled attribution to wording,
which is the fragility `classify`'s own doc warns about.

## `realtime.rs` give-ups, attributed (2026-09-07)

The third and last unattributed suite. Eighteen `INCONCLUSIVE` sites, one aggregate counter.
All eighteen now carry a per-cause key; every site already had `sim` in scope, so unlike
`auth.rs` this needed no plumbing at all.

Measured over 128 universes per property. Two of the three properties gave up in zero
universes; the whole 60 belong to the multi-node property (base `0x2EB2_0002`):

| cause | count | share of the 60 |
|---|---:|---:|
| `:recover-exhausted` (33 of 33 `:injected`) | 33 | 55.0% |
| `:ctrl-poll-faulted` | 12 | 20.0% |
| `:no-divergent-cursor` | 11 | 18.3% |
| `:spawn-exhausted` | 3 | 5.0% |
| `:request-no-response` | 1 | 1.7% |
| the other 13 keys | 0 | — |

**33 + 12 + 11 + 3 + 1 = 60, the aggregate exactly.** I expected this one NOT to reconcile —
`realtime.rs` has five direct `sim.give_up(...)` calls that bypass the `INCONCLUSIVE` marker
convention, so the aggregate could have counted universes the per-cause keys never saw. It
does reconcile, which says those five paths did not fire in this population. That is a weaker
statement than "they cannot fire", and they remain the place to look if the sum ever stops
matching.

**Thirteen of eighteen keys are zero.** `:witness-torn`, `:role-file-torn`,
`:local-unrestorable`, the four `ctrl-*` variants other than `:ctrl-poll-faulted`,
`:target-no-kernel`, `:subscribe-no-reply` and both `foreign-cursor-*` keys never fired. Those
give-up paths are unexercised here, so nothing about them is under test — including whether
their error prose still matches what `classify` expects.

### The three suites together

| suite | universes | gave up | rate | top cause | `:recover-exhausted` injected share |
|---|---:|---:|---:|---|---:|
| `replication` (P1) | 256 | 92 | 35.9% | `:recover-exhausted` (100%) | 92/92 |
| `replication` (P2) | 256 | 111 | 43.4% | `:recover-exhausted` (99%) | 110/110 |
| `auth` | 128 | 53 | 41.4% | `:recover-exhausted` (66%) | 35/35 |
| `realtime` (multi-node) | 128 | 60 | 46.9% | `:recover-exhausted` (55%) | 33/33 |

`:recover-exhausted` is the largest single cause in every suite, and it is **100% `:injected`
in all four** — 270 of 270 across every property measured. Whatever holds these universes back
is uniform across the codebase and is not a sticky state anywhere. Combined with the 10×
experiment's ~4% rescue, the operative conclusion generalises: none of the three
`*_RECOVER_ATTEMPTS` constants should be raised.
