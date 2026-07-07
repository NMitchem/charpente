# M0 step 8 — the agent-built demo app: design

Status: approved by Noah 2026-07-06 (all five protocol decisions his calls: guide-first,
invoicing, in-repo isolation, sonnet fresh-restart protocol, split-transaction bug demo).

## Goal

PLAN.md step 8, the demo that sells the thesis: an agent builds a small SaaS on
charpente against the docs alone, and `charpente test --sim` catches an injected
durability bug that would silently eat data on a conventional stack. Exit criteria
(PLAN M0): the demo app passes 10k seeded sim universes with fault injection; a crash
at any point during commit never loses an acknowledged write; the agent-authored app
required zero framework patches. The demo doubles as the launch post's raw material.

## Shape: one experiment, two phases, strict order

Phase 1 produces `docs/GUIDE.md`; phase 2 produces `examples/invoicing`, written by a
fresh agent whose entire charpente knowledge is README + GUIDE. The phases are one
experiment: the guide's table of contents is defined by what the demo agent must
accomplish, and the demo run is the guide's validation. Rejected: running the demo
against the founding docs as-is (DESIGN/PLAN are rationale documents, not an API
reference — the run would measure a predictable doc gap, burning a clean experiment);
and a docs-only stripped-checkout rig (enforcement machinery that isn't the product).

## Phase 1 — GUIDE.md

**Audience: an agent.** The guide optimizes for oracle density, not narrative. Every
section carries a complete, compilable example. Single file, `docs/GUIDE.md` — one
context load (DESIGN §7's machine-readable docs pack starts here).

**Contents** (each with a full runnable snippet, exact signatures, and the error the
reader hits when they get it wrong):

1. Project setup — Cargo.toml (one dependency: `charpente`; dev-deps `charpente-sim`;
   the `[[test]] name = "sim", harness = false` block), directory shape.
2. Tables — `#[table]`, `Id<T>`, `#[index]`, the generated `by_*` accessors, `Codec`
   field requirements.
3. The database — `Db::open`, `Schema` registration, `DbConfig`, the data directory.
4. Transactions — `Db::transact`, the `View` trait (read-your-own-writes), `tx.put`/
   `tx.delete`/`tx.get`, `tx.enqueue`/`tx.emit`, `TxError::{App, Io}`, what a
   transaction cannot do (no ambient IO — and the error you get if you try).
5. HTTP — `#[route]` argument convention (tx, path params in order, one body arg,
   optional trailing `ctx`), router mounting, sessions via `Ctx`, `ApiError`,
   ack-after-durable semantics, `charpente::serve` startup.
6. Jobs — `#[job]` + retry policy, `jobs![]` registration, handler signature
   `fn run(self, tx: &mut Tx) -> Result<(), JobError>`, exactly-once effects framing,
   `JobError::{App, Io}` semantics, dead-lettering.
7. Cron — schedule registration, tick semantics (catch-up fires once, no backfill).
8. Sim testing — `sim_tests!{}` convention, workload signature
   `fn(&mut Sim) -> Result<(), String>`, the three checkers (`is_fault_fallout`,
   `replays_identically`, `acked_implies_durable`), coverage `require` keys,
   `charpente test --sim` flags and exit codes, reading a failure report and replaying
   its seed.
9. Error index — the common contract-violation messages verbatim and what each one
   means (macro argument errors, router conflict panic, record-size assert, poisoned
   engine).

**Accuracy gate:** every code block in the guide compiles. A `guide-examples`
workspace test crate extracts the `rust` code blocks from `docs/GUIDE.md` at test
time and compiles them (blocks that are deliberate fragments are marked `rust,ignore`
and used sparingly) — extraction-based, so a guide edit cannot drift from what the
gate checks; guide rot is a build failure. Hard rule 6 applied to the guide itself.
Written by subagents from the code and design docs; reviewed like code.

## Phase 2 — the experiment

**The demo agent:** a fresh sonnet subagent, zero session context — the commodity
agent the launch post claims. Its context: the product brief below, README.md,
docs/GUIDE.md, and the prohibition. Its feedback loop: the compiler and
`charpente test --sim`. Nothing else.

**The prohibition:** it must not read `crates/*/src` (or `.superpowers/`, or the
founding design docs). Its transcript is the audit trail; any framework-source read
taints the run (recorded in findings, run restarted). Doc-lookup failures are
findings, not cheats.

**The friction ledger:** the agent records every point of confusion — missing doc,
unclear error, API surprise — to `examples/invoicing/FINDINGS.md` as it works. This
ledger is a step-8 deliverable regardless of outcome.

**The failure protocol:**
- Doc gap blocks progress → the gap becomes a GUIDE.md fix, and a FRESH agent restarts
  the build from scratch. The final artifact must be built against the docs alone, not
  docs plus live coaching. Restart count is recorded.
- Framework code change needed → the zero-framework-patches exit criterion has failed.
  Stop the experiment, fix the framework through the normal spec/plan/sim cycle, then
  restart the experiment fresh. Never patch the framework mid-run to keep a run alive.

## The product brief (requirements the demo agent receives — never implementation)

A minimal invoicing SaaS, `examples/invoicing`, a workspace member:

- **Domain:** customers; invoices with line items and a Draft → Final → Paid state
  machine. Illegal transitions are rejected as app errors.
- **The commit-point flow:** finalizing an invoice marks it Final, enqueues a
  send-receipt job, and emits an InvoiceFinalized event — one transaction. Receipt
  "sending" is a receipts-table row plus an event (v0 has no external IO; the receipt
  row is the observable effect).
- **Cron:** a daily tick enqueues an overdue-reminder job per overdue Final invoice
  (reminder = a reminders-table row; no backfill expectations beyond the platform's
  catch-up semantics).
- **HTTP:** session-authenticated routes for customer CRUD, invoice create/get/list,
  finalize, and record-payment.
- **Sim tests** (`tests/sim.rs` via `sim_tests!`), at minimum:
  - `acked_implies_durable` over the request workload (no acked write lost, crash
    anywhere);
  - `replays_identically` (determinism);
  - receipts ≡ finalized invoices, exactly (the exactly-once invariant), under the
    full fault matrix;
  - overdue-reminder behavior over virtual months (cron correctness at virtual-time
    scale).

The brief states requirements and invariants; schema, route shapes, and test structure
are the agent's own design. That freedom is part of the experiment.

## The recorded bug demo

After the app is green: a variant commit deliberately splits the finalize transaction —
mark-Final in transaction A, enqueue-receipt in transaction B. This is the
outbox-pattern dual-write bug every conventional stack carries silently. The
receipts≡finalized sim invariant catches it under crash injection. Record: the failing
seed, the failure report, the one-command repro, and the fix (recommitted as one
transaction, suite green again). This recording is the launch post's money shot; the
variant lives in git history (or a clearly-marked branch/commit), never on the shipping
tip of `examples/invoicing`.

## Evidence bar (all recorded)

- `charpente test --sim --dir examples/invoicing --universes 10000` green; master seed
  recorded. (10k universes is the PLAN exit criterion; the routine CI run may use
  fewer.)
- The crash-at-any-point claim is the in-sim deterministic crash property
  (`acked_implies_durable` under the fault matrix). A literal `kill -9` loop against
  the running prod binary is a smoke bonus if cheap, not the verified claim — the
  wire-level black-box harness is M1's Vörtex-style rig, per PLAN.
- Zero framework patches during the successful run (git history is the proof).
- FINDINGS.md friction ledger + transcript audit result + restart count.
- Workspace CI stays green with `examples/invoicing` as a member.

## Not in scope

The launch post text itself (the artifacts feed it); TS client codegen; OIDC or
multi-tenancy beyond the platform Session table; external email/webhooks (M1's
external-IO surface); any new framework feature. If the demo needs a framework
feature, that is a finding — record it, don't build it.
