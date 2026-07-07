# M0 step 7 — `charpente test --sim`: design

Status: approved (design reviewed with Noah in-session 2026-07-05; workload-source and
mechanism decisions below were presented as options and chosen by him). Also rules the
open request-arm question from step 6.

## Goal

PLAN.md step 7: the CLI that makes the simulator the app developer's (and the agent's)
inner loop — seeded parallel universes, sampled fault schedules, and failure output that
*is* a reproduction: seed + one-command repro (DESIGN §7). The step productizes the
harness the internal suite already trusts; it does not replace that suite.

## Ruled decisions

1. **Workload source: user-written sim tests + a shipped checker library** (Noah,
   in-session). App authors write property workloads with the same signature the
   internal suite uses; charpente ships reusable oracles. Auto-driving the app from its
   route/job registry (typed fuzzing) is an M1 layer on top — not step 7.
2. **Mechanism: app-linked harness, CLI orchestrates** (Noah, in-session — "A"). The
   app's sim tests compile into one cargo integration-test target with
   `harness = false`; the runner lives in charpente-sim; the `charpente` binary wraps
   `cargo test` and passes runner flags through. The self-testing-app-binary shape
   (process-per-universe, abort-proof) is deliberately deferred to M1's Vörtex-style
   edge harness.
3. **Request arm: stop only when poisoned** (Noah, in-session; closes the step-6 open
   item). After a request-path `TxError::Io` 500, the kernel checks `Db::poisoned()`
   and stops the loop if the engine is poisoned — restart-and-recover is the only exit
   from poison. Non-poisoning faults (e.g. read-path redb errors) keep step 5's
   500-and-continue contract. Job/cron arms unchanged.

## The app-side convention

`tests/sim.rs` in the app crate, `harness = false` in its `Cargo.toml`:

```rust
charpente_sim::sim_tests! {
    invoices_survive_crashes require ["fault:crash", "restart"],
    receipts_exactly_once,
}

fn invoices_survive_crashes(sim: &mut Sim) -> Result<(), String> { … }
```

- Explicit registration, same idiom as `routes![]`/`jobs![]` — no linkme/inventory
  section magic.
- Workload signature is exactly the internal suite's: `fn(&mut Sim) -> Result<(), String>`;
  `Err` and panics are both failures (per-universe `catch_unwind`, the proven pattern).
- `require [...]` lists are per-test batch-level sometimes-coverage keys with the
  existing semantics: fail by name when never hit across the batch; skipped under
  single-seed replay. The auto-covered keys (`fault:*`, `crash:*`, `restart`) come free.
- The macro generates `main()`, which hands the registry to the harness.

## The runner (charpente-sim, new `harness` module)

- **Frozen-contract reuse.** The single-universe executor is the existing
  `run_one_swarm` path — seed → FaultConfig::swarm sampling → workload → seed+config
  report. The CLI harness is a *second consumer* of that executor. `run_universes!`,
  `run_universes_swarm!`, and every pinned seed stay frozen and valid.
- **Batch model.** A run draws one master seed (OS entropy, printed at startup);
  per-universe seeds derive via the existing splitmix chain. `--master-seed 0x…`
  replays an entire batch byte-identically; `--seed 0x…` replays one universe
  (coverage requirements skipped, today's semantic).
- **Parallelism.** Universes run on a thread pool (`--jobs`, default = cores), one
  `catch_unwind` per universe. The pool lives *outside* the deterministic boundary —
  each universe inside it is a sealed deterministic world. `std::thread::spawn` is
  deny-listed; the harness module takes the same site-local
  `#[allow(clippy::disallowed_methods)]` + rationale treatment as the prod trait impls.
  Universe result order in reports is by universe index, never completion order —
  reports are deterministic under `--master-seed` replay regardless of scheduling.
- **Flags** (runner args, passed through by the CLI): `--universes N` (default 256),
  `--seed`, `--master-seed`, `--filter <substring>`, `--jobs N`, `--format human|json`.

## Failure output (product surface, tested — hard rule 5)

Human, per failure: test name, universe seed, sampled `FaultConfig`, the message, and a
copy-paste repro line:

```
sim failure in invoices_survive_crashes
  seed:   0x8bee1752811ae873
  config: FaultConfig { write_error: 0.0, sync_error: 0.118, … }
  repro:  charpente test --sim --filter invoices_survive_crashes --seed 0x8bee1752811ae873
```

JSON (`--format json`): one record per failure `{test, seed, master_seed, config,
message, repro}`, a top-level `missing_coverage` array of `{test, key}` pairs (present
when a `require`d coverage key went unmet across the batch — empty otherwise), plus a
batch summary `{master_seed, universes, failures, coverage}` — the DESIGN §7
machine-readable surface. The JSON shape is pinned by tests from day one.

Exit codes: 0 green, 1 sim failures (or unmet coverage requirements), 2 build/usage
error. Coverage failures name the key and the test that required it.

## The CLI (`charpente-cli`, first real subcommand)

`charpente test --sim [flags]`: locates the app crate (cwd), execs
`cargo test --test sim -- <runner flags>`, streams output, propagates the exit code.
Build errors surface verbatim — the compiler's output is already the oracle. The CLI
stays thin in v0: no config file, no test-target discovery beyond the `sim` target
convention. `charpente new` scaffolding generates the two-file convention later (step 8
writes them by hand and validates the docs).

## Platform-checker library (charpente-sim, v0 — deliberately three items)

1. **Fault-fallout classifier**: the three-family `is_injected` matcher (injected-fault
   wording, our poison wording, redb latch wording) the internal suite has hand-rolled
   four times — productized once, with its own tests, so app workloads and our tests
   share one definition.
2. **Determinism replay helper**: run a workload twice on one seed; compare durable
   fingerprint + a caller-supplied observation trace; failure output names the first
   divergence.
3. **Acked-implies-durable skeleton**: the request-level crash property generalized —
   app supplies a request generator and a read-back oracle as closures; the helper owns
   the crash/restart/reopen loop and the "every 200 survives" assertion.

Nothing else until the demo app (step 8) demands it.

## Request-arm work item (step-6 closure)

`Db::poisoned() -> bool` (the engine already tracks poison; this exposes it), and the
kernel request arm stops the loop after a 500 when poisoned. Property tests: (a) an
injected WAL fault that poisons mid-request stops the loop — no further requests are
served; (b) a non-poisoning fault still 500s and continues; (c) the stopped kernel's
next open recovers (restart-and-recover). DESIGN §1/§6 and CLAUDE.md updated in the
same commit.

## Testing step 7 itself

- **Fixture app crate** in the workspace (dev-only, not published) with deliberately
  failing and passing sim tests.
- **The headline meta-test**: run the harness against the failing fixture, parse the
  failure report, extract the repro line, *execute it*, and assert it reproduces the
  identical failure — the product promise as a test.
- `--master-seed` batch replay is byte-identical (report-level comparison).
- JSON shape pinned; human failure text asserted (contract, site, fix — hard rule 5).
- The frozen internal suite stays green throughout; no internal test migrates.

## Non-goals (step 7)

Typed workload auto-generation (M1), process-per-universe isolation (M1 edge harness),
`charpente new` scaffolding (step 8 decides what it generates), nightly orchestration/CI
config (a flag-invocation concern, not a platform one).
