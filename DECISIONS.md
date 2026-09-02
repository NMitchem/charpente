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

**Trigger read 2026-09-02; it does not fire, and the deferral stands.** (Recorded at Noah's
direction; a status-quo reading, not Noah's own hand — revisable.) `examples/invoicing` is
still the only app in the tree. `examples/invoicing/src` has three `scan::<T>()` call sites:
`list_customers` and `list_invoices` in `routes.rs` (lines 89, 222) are legitimate list-all
routes; the overdue sweep in `jobs.rs` (line 121) is a filtered full scan standing in for a
missing range index. Typed indexes ARE used where they fit: `LineItem::by_invoice` (twice —
`routes.rs:134`, `jobs.rs:28`) and `ApiKey::by_key_hash` (used by the platform's own auth
ladder, `crates/charpente/src/auth/ladder.rs:111`, which `examples/invoicing`'s `auth` routes
exercise). The chain that normalized the one gap:
`examples/invoicing/FINDINGS.md` (79-87, "`#[index]` is only demonstrated on `Id<T>` and
`String` fields") → `docs/GUIDE.md` (188-191, under the "`#[index]` works on any `Codec`
field" heading at line 131) promoting scan-then-filter to a sanctioned idiom; DESIGN.md §9
item 3 already records the same evidence informally. One gap in one small app is not a
pattern; the trigger does not fire. What reopens it: a second app, or a second forced
scan-filter in invoicing, that needs a range/comparison index — at which point `#[index]`
grows an order-preserving encoding (DESIGN §4, line 692-693: "exact-match lookups only until a
real query pattern demands order-preserving key encoding").

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

**Trigger read 2026-09-02; it does not fire, and the deferral stands.** (Recorded at Noah's
direction; a status-quo reading, not Noah's own hand — revisable.) M1's exit criterion — "a
follower promoted mid-write-load loses nothing and serves reads throughout" — was discharged
2026-07-27 with an oracle at two layers, an in-sim property plus an edge-harness wire oracle
(PLAN.md M1 exit criteria). The non-converged tail is exactly the §6 loss window, reported
separately rather than hidden: `charpente promote`'s `CtrlOk` names `last_applied`, and
"records the dead primary acked but had not shipped are the DECISIONS §6 window, lost
honestly and named" (DESIGN.md §5, line 934-935). Since then, the dual-primary boot gate
(spec 2026-07-31, DESIGN.md §5 "The boot gate", line 949) and the verifying-peer policy
(spec 2026-08-06, DESIGN.md §5, line 977-978) closed the split-brain shapes a promote could
create. Consensus-elected promotion remains addable behind the same log format, per this
entry's own text above. Async stands for M2; the loss window is bounded, named, and tested
rather than hidden. What reopens it: the hosted control plane (PLAN.md M2) needing automatic
election, or an edge-harness/production finding where the bounded window is unacceptable —
at which point consensus lands behind the log format, not as a data-plane change. Design C
(a durable timeline epoch on `Hello`) is in the M2 follow-up backlog (docs/LEDGER.md, line
1734 "M2 follow-up backlog") as the sharpening step short of consensus.

## 7. redb first, own LSM second

Building the storage engine first is the romantic move and the wrong one: the sim harness
is the safety equipment, and it doesn't exist yet. redb behind the `Disk` trait gets M0
shipped and *still* exercises crash/torn-write recovery of our WAL + snapshot layer.
Reopen (on schedule): M2.

**Reopened on schedule 2026-08-06; re-closed: deferred behind measurement** (Noah-ruled,
premise re-examination at M2 start). The harness precondition is met, but the urgency
case is gone: redb coupling is confined to `table.rs` (one file, ~30 call sites — deferral
does not compound the swap cost); redb is healthy and its known gaps are fenced and pinned
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

**Trigger (a) re-read 2026-08-20 after group commit and per-batch apply; it still does not
fire.** (Recorded at Noah's direction, on 2026-09-02; a status-quo reading, not Noah's own
hand — revisable.)
The 2026-08-10 reading above pre-dates group commit (shipped 2026-08-15) and per-batch apply
(shipped 2026-08-20), which is precisely what removes the one-fsync-per-transaction ceiling
the first reading rested on, so the trigger was owed a re-read (PLAN.md, M2 "Honest benchmark
harness" bullet). The per-phase attribution sub-project delivered it on the same instrument
and protocol with a fresh interleaved baseline: trigger (a) "does not fire under any reading"
of "largest single non-idle phase" — "the field reading leads by 71-73 points, and the two
readings that would previously have fired now decline" (PLAN.md, same bullet). Numbers and
method are in [docs/BENCH.md](docs/BENCH.md) ("Where a batched commit's time goes — per-phase
attribution (2026-08-20)"); the corresponding entry is docs/LEDGER.md's "2026-08-20 — M2
sub-project: per-batch apply (store commit point), COMPLETE" section (and the per-phase
attribution sub-project entry immediately above it, also dated 2026-08-20). This §7 remains
untouched and Noah's to sign off — what changed is that the ambiguity which made it a
judgment call is gone, so the deferral simply stands. Triggers (b) and (c) are untouched by
this re-read.

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

**Trigger read 2026-09-02; it does not fire — closed.** (Recorded at Noah's direction; a
status-quo reading, not Noah's own hand — revisable.) The edge executor landed and is
poll-shaped, the evidence this trigger asked for: `Kernel::step`
(`crates/charpente/src/kernel/mod.rs:305`) services exactly one ready source per call and
never blocks, reporting a deadline for its caller to park on (`Step::Worked`/`Step::Idle`,
lines 58-61); `Repl::pump_io`/`Repl::step`
(`crates/charpente/src/repl/mod.rs:385,503`), the `Ready::Tail` arm and the `Ready::Archive`
arm (`crates/charpente/src/kernel/mod.rs:758,771`) are all polls — DESIGN.md §1 (lines
181-232) and §5 (line 845, "Both are driven kernel-shaped") describe them as such. `hyper`/
`tokio` appear only in `crates/charpente/src/serve.rs` (verified: no other file under
`crates/charpente/src` imports either), translating wire↔struct at the edge. No core trait
became awaitable — `crates/charpente-core/src/io.rs` has no `async fn`. Keep the entry; this
is the closing paragraph.
