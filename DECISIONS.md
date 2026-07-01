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

## 8. Name

`charpente` (French: the load-bearing timber frame of a building). Chosen by Noah after
`socle` (first pick) proved taken on crates.io — ironically by an axum/telemetry server-
bootstrap crate, active June 2026. Availability snapshot 2026-07-01: taken = socle, massif,
menhir, chassis, armature; free = charpente, monolithe, granit, batisse, ossature,
soubassement. **TODO: reserve `charpente` on crates.io with an empty publish.**

## 9. License — UNDECIDED

Options on the table: MIT/Apache-2.0 dual (Rust convention, maximal adoption) vs.
FSL/BUSL-style source-available (protects a future hosted-control-plane business; Convex
precedent). Decide before anything is pushed public. Owner: Noah.
