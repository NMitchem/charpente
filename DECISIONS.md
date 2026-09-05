# Decision log

Alternatives considered and rejected, with the reasoning. If a rejection's premise changes,
reopen the decision — that's what this file is for.

## 1. Why this project (and not the other golden opportunities)

Charpente won from a ranked field of "agent-era golden opportunity" candidates
(2026-07-01 analysis + verified research, see RESEARCH.md). The field, ranked by
*mechanical-oracle density × adoption certainty*:

1. **Fast Rust dev-loop toolchain** (tcc-class non-optimizing compiler; crater as oracle) —
   highest adoption certainty, and the runner-up. Not chosen because it's a tool, not a
   product moat: valuable, adoptable, but hard to own. **Standing relationship:** it's also
   charpente's biggest risk-reducer — if anyone builds it, charpente's dev loop improves
   for free. Track it; consider it a future sibling project.
2. **This platform** — chosen: real product moat (determinism + typed oracle are multi-year
   retrofits for competitors), and the two hardest edges compound.
3. C→Rust port factory — verified crowded with institutional money (DARPA TRACTOR,
   Microsoft); racing funded labs as an individual is bad positioning.
4. Verified parsers/codecs, superoptimizer, bespoke storage engines, GPU-kernel moat-break,
   DST framework standalone — each folded in conceptually where relevant (DST became
   charpente's spine rather than a standalone framework).

## 2. Single binary, not incremental/hybrid

Rejected: "embed into your existing stack" positioning. Verified research (RESEARCH.md,
FFI section) shows component-wise hybrids relocate risk to the boundary instead of removing
it, and every retained external service retains its ops surface and its dual-write seam.
The guarantees only hold inside the commit point; a half-adopted charpente has no story.
Greenfield-only is the honest scope (PLAN.md risk table).

## 3. Not a "Next.js/Rails of Rust"

Rejected explicitly. Human-era frameworks amortize onboarding — conventions so humans
decide less, docs so humans learn fast. That value depreciates when the developer is an
agent. Loco.rs already occupies the clone position without traction. Charpente's target is
agent preference via feedback-loop quality (MANIFESTO §3): the framework as reviewer.
Corollary: docs budget goes to error-message quality and machine-readable contracts before
tutorials.

## 4. Typed-native queries, not SQL-first

SQL strings are invisible to the compile-time oracle and exist for human REPL ergonomics.
Typed get/put/scan + typed indexes participate in the oracle. Costs accepted: no ecosystem
of SQL tooling against the primary API; ad-hoc analytics deferred to the read-only
Postgres-wire endpoint (M2). Reopen if: typed combinators prove insufficient for real app
query patterns by mid-M1.

## 5. Compiled-in Rust app code, not a WASM plugin model

SpacetimeDB chose WASM reducers (host-language flexibility, sandboxing, hot swap). Rejected
here because WASM re-introduces the serialization boundary and severs the compile-time
oracle (host types ≠ guest types — the exact seam charpente exists to delete). Costs
accepted: Rust-only app code (fine: agents write Rust happily); redeploy = binary swap
(mitigated by log-replay upgrades, maybe cranelift hot reload later). Reopen if: a
component-model toolchain ever gives *zero-copy, compile-time-checked* host/guest types.

## 6. Single-threaded core, no sharding, async log shipping (v1)

- Single-threaded apply loop: serializability by construction, DST-friendly, LMAX/Redis/
  TigerBeetle-class throughput. Parallel-apply rejected until a real workload proves the
  core loop is the bottleneck (measure first; DESIGN §9.2).
- No sharding, permanently (MANIFESTO §7). Not a v1 cut — a positioning decision.
- Async replication with possible failover loss-window in v1, rather than consensus-first:
  consensus can be added *behind the same log format* later; shipping M0 cannot wait on it.
  Reopen at M1 exit criteria.

## 7. redb first, own LSM second

Building the storage engine first is the romantic move and the wrong one: the sim harness
is the safety equipment, and it doesn't exist yet. redb behind the `Disk` trait gets M0
shipped and *still* exercises crash/torn-write recovery of our WAL + snapshot layer.
Reopen (on schedule): M2.

**Reopened on schedule 2026-08-06; re-closed: deferred behind measurement** (Noah-ruled,
premise re-examination at M2 start). The harness precondition is met, but the urgency
case is gone: redb coupling is confined to `table.rs` (one file, 170 call sites — the
measure that produces that number is recorded at the end of this entry; deferral does not
compound the swap cost); redb is healthy and its known gaps are fenced and pinned
(the open-time panic fence, both mode-skew profile overrides, the 0a latch); and the moat
NEIGHBORS defines — determinism + the compile-time oracle — already exists, while the
engine is invisible to the adopters the wager needs. Building a performance-shaped engine
with no benchmark showing redb as a bottleneck fails §6's measure-first discipline — and
no benchmark exists at all, which is also PLAN's own unfulfilled "publish honest
benchmarks early" mitigation. Build triggers, any one sufficient: (a) benchmarks show redb
limiting the headline throughput claim; (b) a redb upgrade makes the fence-maintenance
burden real (the latch's five-site census review); (c) post-launch adoption justifies
moat-deepening. The engine trait boundary is deliberately not cut in the meantime: with
one implementation it is speculative abstraction, and containment already holds by
convention; the second engine's arrival is the moment to cut it. Page checksums stay "M2
engine owns" (wal-segment spec ruling) — that IOU now hangs off the triggers rather than
a scheduled start.

**Trigger (a) read 2026-08-10; it does not fire, and the deferral stands.** The benchmark
that did not exist when this entry was re-closed now does (M2 sub-project 1,
[docs/BENCH.md](docs/BENCH.md), which is the section this trigger reads). It does not show
redb limiting the headline claim — it shows the *fsync* limiting it. The core commit path
measures 215 log records/sec against a device append ceiling of 215–258/sec bracketed
around its own window, i.e. at the ceiling within measurement error; if redb's
materialization cost anything material at this rate, core would sit well below it. Reads
agree from the other side at 2.65M/sec, four orders of magnitude above the write path,
because they never touch the log. **Scoped honestly:** the core layer bundles WAL append +
sync + apply + store materialization into one number, so this rules out redb as *the
binding constraint at this workload on this hardware* — it does not measure redb in
isolation, and says nothing about a workload whose store cost is not swamped by an fsync. A
finer attribution needs per-phase profiling the instrument does not do. The follow-up this
opens is group commit, not an engine swap.

**The call-site measure, defined 2026-09-04 (tenant-scoping spec §10).** This entry quoted
"~30 call sites" from 2026-08-06 onward and the figure was stale; two independent
recounts during the tenant-scoping design disagreed (46 and 64) because each used a
different pattern set,
which is the tell that a number was being quoted without a measure behind it. So the
measure is recorded here with the count, and a future recount that disagrees is either a
real change or a different definition — never an unresolvable difference of opinion.

> **A redb call site is one call expression in `crates/charpente-core/src/table.rs` whose
> callee is an item defined by the `redb` crate** — a redb inherent method, a redb trait
> method, or a redb associated function — counted per expression rather than per line, over
> the file with comments and doc comments stripped, and including the file's own
> `#[cfg(test)] mod latch_tests`. A *mention* of a redb type is not a call site.

**170**, as of 2026-09-04 (HEAD of `tenant-scoping`), broken down so it can be audited:
`AccessGuard::value` 43, `open_table` 41, `get` 19, `insert` 15, `range` 11, `begin_write` 6,
`commit` 6, `remove` 6, `StorageBackend` methods driven by `latch_tests` 6,
`set_durability` 5, `TableDefinition::new` 4, `iter` 3, `begin_read` 2, and one each of
`Builder::new`, `create_with_backend` and `delete_table`. 164 of the 170 are in the module
proper; the remaining 6 are the latch tests. The separate type-level surface — deliberately
*not* folded into that number, since conflating the two is what produced the disagreement —
is 29 `redb::` paths plus the file's single `use redb::{…}` importing seven names.

**The one-file claim in the paragraph above still holds exactly.** `grep -rln 'redb::'
--include='*.rs' crates/ examples/` returns only `crates/charpente-core/src/table.rs`, and
only `charpente-core/Cargo.toml` names the dependency (`redb = "4.1.0"`). What the recount
changes is the *size* of the confined surface, not its confinement — and it argues neither
way on the deferral, which rests on the build triggers, not on the call count.

## 8. Name

`charpente` (French: the load-bearing timber frame of a building). Chosen by Noah after
`socle` (first pick) proved taken on crates.io — ironically by an axum/telemetry server-
bootstrap crate, active June 2026. Availability snapshot 2026-07-01: taken = socle, massif,
menhir, chassis, armature; free = charpente, monolithe, granit, batisse, ossature,
soubassement. Reserved 2026-07-01 as empty v0.0.0 placeholders (MIT OR Apache-2.0):
`charpente`, `charpente-core`, `charpente-macros`, `charpente-sim`, `charpente-cli`.

## 9. License — MIT OR Apache-2.0 (decided 2026-07-01)

Rust-convention dual license. Rejected: FSL/BUSL-style source-available (Convex
precedent). Reasoning: adoption is the whole strategy (MANIFESTO §3 — distribution rides
agent preference), and charpente's shape punishes source-available harder than a service
would be — apps *link against* the platform and compile into one binary, so the
framework's license sits inside every adopter's shipped artifact and legal review.
Convex's mitigation (permissive clients, FSL backend) is unavailable here: the platform
*is* the app's binary. The hosted-control-plane business (PLAN M2) is protected by
keeping the control plane separate proprietary code, not by the framework license —
"the moat is the platform, the business is not-having-to-operate-it." Reopen if: a
competitor commercially hosts charpente at meaningful scale before the control plane
ships.

## 10. Sync poll-shaped IO traits, not async futures (2026-07-01)

DESIGN §1 originally sketched `IoFuture`-returning traits. Implemented synchronous
(`&self`, blocking-style, non-blocking polls for net accept/recv) after studying the
neighbors (NEIGHBORS.md): madsim/turmoil are async only because they retrofit determinism
onto tokio ecosystems; TigerBeetle's VOPR drives poll-based state machines on one thread.
Reasons: (a) the core apply loop is single-threaded and its IO is sequential by design —
async buys Send-bound and boxed-future costs with zero determinism payoff inside the
boundary; (b) redb's `StorageBackend` is sync and `&self` — the `Disk`/`File` traits
mirror it, so mounting redb (M0 step 3) is a thin adapter; (c) the bug-finding
interleaving exploration comes from crash-time write-buffer collapse, not async completion
shuffling. Costs accepted: concurrency at the edges needs its own deterministic executor
when HTTP/queue land (M0 steps 5–6); tokio-flavored async stays outside the deterministic
boundary. Reopen if: the edge executor proves the core traits must be awaitable.

## 11. Cross-tenant enumeration is a declared capability, not an escape hatch (2026-09-04)

Tenant scoping (M2, DESIGN §8, spec
`docs/superpowers/specs/2026-08-30-tenant-scoping-design.md` §3) ships
`Tx::across_tenants::<S>(cap)`, which enumerates a scoped table across every tenant.
Hard rule 2 says there are no escape hatches on guarantees, so this entry records why
`across_tenants` is not one — because the reading binds future work, not just this
sub-project.

**The guarantee is "a scoped query cannot silently cross tenants."** `across_tenants` is
not silent: it is named at the call site, its capability is declared in the route or job
attribute (`#[route(…, cross_tenant)]` / `#[job(cross_tenant)]`), the token
(`CrossTenant`) is unforgeable from a handler because its only constructor is
`Group::cross_tenant_cap` and `begin_group` lives on `Db`, and every row comes back paired
with its scope — `Vec<(Id<Org>, Invoice)>`, never `Vec<Invoice>` — so a cross-tenant result
cannot be handed to code expecting one tenant's data without a visible `.map`. It is closer
to `unsafe` than to a feature flag: a named construct where the obligation visibly moves,
not a switch that turns the checker off. Nothing about `ScopedTx` weakens when it exists;
there is no mode, no config key, and no build flag on the scoped path — which is precisely
what "optional guarantees are no guarantees" targets.

**It is also the thing that makes the audit mechanical.** Because the capability is
declared on the route rather than discovered in a function body, `charpente describe`
enumerates every endpoint and job in an app that can read across tenants. "Audit the
cross-tenant reads" stops being a grep someone has to remember and becomes an inventory,
the same move `check-client` and `doc_inventory` already make.

**The rejected alternative, stated so it stays rejected.** Making cross-tenant enumeration
*inexpressible* is coherent, and it was considered. It loses billing rollups, admin
consoles and usage reporting — i.e. most real SaaS — and an app that needs one would build
it outside the typed surface, where nothing is declared and nothing is enumerable. A
capability the platform refuses to name does not disappear; it moves somewhere the oracle
cannot see it.

**What this binds.** Any future feature that wants to read or write outside one tenant must
arrive as a *declared, enumerable capability with the tenant visible in its result type* —
not as a config option, a `DbConfig` field, an undeclared bypass method, or an
"internal-only" accessor. If a proposal cannot be declared on the route and listed by
`describe`, it is an escape hatch and hard rule 2 refuses it. Reopen if: a real app needs
a cross-tenant surface that cannot be expressed as a declaration on the entry point.
**This clause binds the surface an app's handlers and jobs can reach.** It does not reach
the operator path: a `Db` holder's scoped and raw reads sit outside the guarantee rather
than as an exception inside it, which is §12's subject. Read the two entries together —
the shapes forbidden here and the reads §12 permits look alike, and are not.
A louder notch remains available and unbuilt — the capability could additionally be
required at `App::mount` — and would be the first thing to reach for if declarations at the
route prove too quiet in practice. §12 adopts that notch as the answer to its own open
question, inheriting this trigger rather than inventing a second one.

## 12. The tenancy guarantee's boundary is the typed surface (ratified 2026-09-05)

**Status: ratified 2026-09-05.** Written up because it is now load-bearing in two places
and was ruled twice in flight (R48, R49) without being recorded here. The line is confirmed
as stated: whoever holds the `Db` is the operator. What held this entry open was the
question at the bottom, which is now answered there rather than left standing.

**The claim.** The tenancy guarantee covers the typed surface the framework hands app
logic — a `Tx`, and through it a scoped table reachable only via
`tx.scope::<Org>(org, ctx)?`, which runs the app's own `Tenant::authorize` first. It is
*not* a sandbox around the process. Whoever holds the `Db` is the operator, with the same
standing as whoever holds the data directory.

**The evidence, both directions.** Task 7's review proved the reachable path: an app's own
`main` builds the `App` it hands `launch`, so it holds `App::db()`, and
`Db::snapshot()` yields `scan_scoped`/`scan_raw`-class reads that take a scope on the
caller's word with no `authorize` anywhere on the path. Those reads are public and stay
public because replication seeding, the archive and `restore` are built on them. Task 10
made the same boundary load-bearing a second time: `CrossTenant`'s only constructor is
`Group::cross_tenant_cap`, and `begin_group` lives in `impl Db` — so a handler (which holds
`&mut Tx` and `&Ctx`, never a `Group`) cannot mint the capability, while a `Db` holder can.
The seal on the handler side is exactly as strong as the boundary claim on the `Db` side;
they are the same ruling seen twice.

**Why the alternative was not taken.** Making `Db`'s scoped and raw reads private would
require a second, privileged path for replication seeding, archive and restore — a second
place where a scope is taken on trust, with the same reachability and less visibility.
Making them `authorize`-checked is worse: there is no request, no principal and no tenant
to ask about at seed time, so the check would have to admit everything, which is a check in
name only.

**What an app is told, and where.** DESIGN §8 states the boundary in its limits paragraph;
`crates/charpente/src/tenant.rs`'s module doc states it at the code; GUIDE §16 states it to
app authors, with the remedy: code outside a handler that wants the guarantee should go
through a transaction and `tx.scope::<Org>(org, ctx)?` exactly as a handler does.

**The question that held this open, and its answer.** Should a future release put the raw
reads behind a named, `describe`-visible operator capability — the §11 shape applied one
layer down — so that even the operator path is enumerable? Read against the rest of the doc
set, that question has three readings and only one survives.

*Sandboxing the operator is already refused.* §5 rejected the WASM plugin model, so app code
compiles into the same binary as the platform: there is no trust boundary between an app's
`main` and `charpente` anywhere in this architecture, and MANIFESTO §4 and §7 both assume
one process in one trust domain. A real operator boundary would have to invent the seam §5
deliberately deleted, and would pay for it in the compile-time oracle.

*Discovering operator reads is impossible, by a ruling already on the books.* DESIGN §7
(ruling 6) settles that enumeration cannot walk call sites — `tx.emit` and
`Subscription::of::<Ev>()` run inside handler bodies, "not anywhere an app-wide walk can
observe". A `Db::snapshot()` in an app's `main` sits in that position for the same reason.

*Declaring them is the only survivor*, and a declaration needs a registration site. `main`
has exactly one: `App::mount` — the louder notch §11 already names. So the notch is not a
new proposal, it is §11's, and it inherits §11's trigger: **build it when declarations at
the route prove too quiet in practice.** The concrete sign will be an app or an audit that
had to read source to answer "what reaches every tenant here" — the moment the inventory
stops being an inventory. Until then the boundary stands as stated, documented in the three
places above rather than guarded by a capability with nothing yet to enumerate.
