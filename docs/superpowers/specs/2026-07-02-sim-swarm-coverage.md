# Sim harness: swarm-sampled FaultConfig + sometimes-coverage — design

Status: approved (Noah approved the sequencing 2026-07-02; design decisions flagged
below were taken conservatively and are noted for review). Lands **before** M0 step 6 so
the deterministic edge executor is born under the better harness.

## Goal

Two harness upgrades from the DST research pass (see NEIGHBORS.md technique lineage,
2026-07 additions):

1. **Swarm testing** (Groce 2012; TigerBeetle VOPR's default): each universe samples
   *which* fault types are active and at what rates from its seed, instead of every
   universe running the same fixed `FaultConfig::default()`. Fault-type competition
   within a bounded budget is what surfaces depth; fixed rates explore one point in the
   config space forever.
2. **Sometimes-coverage accounting** (Antithesis's "sometimes assertions", adapted): the
   harness counts named events across a batch of universes and can *require* that an
   event occurred at least once — closing the "256 universes ran but did any actually
   hit rotation/poison/rebuild?" gap (the Jepsen lesson: heavy fuzzing still leaves
   silent pattern gaps).

Plus one repair while we're in the runner: the swarm runner catches panics inside a
universe and re-panics with the full seed+config+repro report — the known harness gap
(a panicking universe currently bypasses seed printing, violating hard rule 5).

## Hard constraints

- **`run_universes!` / `run_universes_from!` / `run_one` are frozen.** A seed must replay
  identically forever (rng.rs module doc); the pinned regression seeds
  (0x8bee1752811ae873 in crash_recovery.rs, the noted seeds in wal.rs/typed_tables) keep
  their meaning. Swarm is a **new** runner; existing tests migrate individually.
- No new fault *types* in this task (bit-flip corruption, misdirected writes, read
  errors land with WAL segment management per DESIGN §1). This task changes how existing
  fault knobs are sampled and observed, nothing about what faults exist.
- `FaultConfig::default()` and `FaultConfig::none()` semantics unchanged.
- All work in `charpente-sim` plus test migrations; core crates untouched.

## Design

### 1. `FaultConfig::swarm`

```rust
impl FaultConfig {
    /// Samples a per-universe fault configuration (swarm testing): each fault
    /// axis is independently in or out of play, and active axes draw their
    /// rates from wide ranges — including extremes the default config never
    /// visits. Deterministic in `rng`.
    pub fn swarm(rng: &mut Rng) -> FaultConfig
}
```

Sampling (all draws through `rng`, in this exact order — the order is part of the
replay contract once shipped):

1. `write_error`: active with p=0.5 (`rng.chance(0.5)`); if active, rate =
   `rng.below(150) as f64 / 1000.0` (0.000–0.149), else 0.0.
2. `sync_error`: same scheme, independent draw.
3. Crash-collapse archetype, `rng.below(4)`:
   - 0 → all-survive: `crash_apply = 1.0, crash_torn = 0.0`
   - 1 → all-drop: `0.0, 0.0`
   - 2 → all-torn: `0.0, 1.0`
   - 3 → mixed: `crash_apply = rng.below(1001) as f64 / 1000.0`, then
     `crash_torn = (rng.below(1001) as f64 / 1000.0) * (1.0 - crash_apply)`
4. `disk_latency`: `(Duration::ZERO, hi)` where hi = one of
   `[ZERO, 100µs, 2ms, 50ms]` by `rng.below(4)`.
5. `net_latency`: `(Duration::ZERO, hi)` where hi = one of
   `[ZERO, 1ms, 20ms, 250ms]` by `rng.below(4)`.

Call `self.validate()` on the result before returning (assert, not error — a sampler
that produces an invalid config is a harness bug).

`FaultConfig` gains `#[derive(...)]` nothing new — it is already `Clone + Debug`; the
failure report prints it via `{cfg:?}`.

### 2. Coverage

The world state gains a coverage map; both the sim internals and workload code can mark
events:

```rust
// in State:
coverage: BTreeMap<&'static str, u64>,

impl Sim {
    /// Marks a named event for sometimes-coverage accounting. Workload-side
    /// marks use a "<area>:" prefix by convention (e.g. "wal:recovered").
    pub fn cover(&self, what: &'static str);
    pub fn coverage(&self) -> BTreeMap<&'static str, u64>;
}
```

Auto-covered by the sim internals (increment at the point the event happens):

| key | where |
|---|---|
| `fault:write_error` | injected write error fires (`SimFile::write_at`) |
| `fault:sync_error` | injected fsync failure fires (`SimFile::sync`) |
| `fault:crash` | injected crash fires (countdown hits zero, or ops-after-crash fail — count the transition only, in `State::disk_op`) |
| `crash:applied` / `crash:torn` / `crash:dropped` | per pending op's fate in `disk::collapse` (pass `&mut BTreeMap<&'static str, u64>` — or the enclosing `State` pieces — into `collapse`) |
| `restart` | `Sim::restart` |

### 3. The swarm runner

```rust
#[macro_export]
macro_rules! run_universes_swarm {
    ($universes:expr, $f:expr) => { ... };                                  // no requirements
    ($universes:expr, $f:expr, require: [$($key:expr),* $(,)?]) => { ... }; // batch must cover each key
}

#[doc(hidden)]
pub fn run_universes_swarm_in(
    package: &str,
    file: &str,
    universes: u64,
    require: &[&str],
    f: impl Fn(&mut Sim) -> Result<(), String>,
)
```

Per-universe seed derivation (frozen once shipped): from the universe seed `seed`
(drawn from the base exactly as today, `splitmix64(&mut state)`), derive
`let mut s = seed; let cfg_seed = splitmix64(&mut s);` then
`cfg = FaultConfig::swarm(&mut Rng::new(cfg_seed))` and build
`Sim::with_faults(seed, cfg)`. The printed/replayed seed is `seed` — `CHARPENTE_SEED`
replay re-derives the same config; the failure report prints both the seed line and a
`config: {cfg:?}` line plus the usual one-command repro (hard rule 5).

Each universe runs under `std::panic::catch_unwind(AssertUnwindSafe(...))`. On panic,
re-panic with the full failure report (seed, config, repro, and the panic payload's
`&str`/`String` message if downcastable, else a placeholder). `eprintln!` progress
header mirrors `run_universes`.

After the batch: aggregate every universe's `coverage()` into one map, print a sorted
one-line-per-key summary, then for each `require` key with count 0, panic with a message
naming the key, the universe count, and the fix ("raise the universe count, adjust the
workload, or drop the requirement — a required state that never occurs is an untested
claim"). Under `CHARPENTE_SEED` (single-universe replay) skip the requirement check and
say so on stderr.

No `run_universes_swarm_from!` yet — the meta-tests use `CHARPENTE_SEED`-free fresh
bases like the existing suite; add the fixed-base variant only if a meta-test needs it
(the requirement-failure meta-test likely does — mirror `run_universes_from!` if so).

### 4. Migrations (each in its own commit, property re-verified over fresh seeds)

Migrate the four fault-matrix property tests to the swarm runner, with batch
requirements:

- `charpente-sim/tests/wal.rs` — `wal_survives_the_fault_matrix`
- `charpente/tests/typed_tables.rs` — the fault-matrix property
- `charpente/tests/transactions.rs` — the crash property
- `charpente/tests/http.rs` — `http_requests_survive_the_fault_matrix`

Requirements for each: `["fault:write_error", "fault:sync_error", "fault:crash",
"crash:applied", "crash:torn", "crash:dropped", "restart"]` (with ~50% axis activation
over ≥256 universes, missing any is astronomically improbable; if a migrated test's
universe count is smaller, raise it or trim the list — judgment call, note it).

Determinism tests (`*_replays_identically*`, determinism.rs) and all pinned-seed
regression tests stay exactly as they are, on the frozen runner/config.

Known pre-existing bug, do not chase: post-crash `Store::open` can panic inside redb
(`page_manager.rs:231`, ~1-in-thousands universes; known seed 0x1c0189950421c6ac on the
old runner). The swarm runner's catch_unwind will now *report* it properly with a seed —
if a migrated run hits it, record the new seed in the session notes and move on; fixing
it is a separate task.

## Tests (TDD — write these first)

Meta/unit in `charpente-sim`:
1. `swarm_same_seed_same_config`: two `FaultConfig::swarm(&mut Rng::new(k))` draws are
   equal (compare `format!("{cfg:?}")`).
2. `swarm_configs_vary`: over 64 configs from `Rng::new(fixed)` chained draws, at least
   two distinct debug strings, at least one with `write_error == 0.0` and one with
   `write_error > 0.0`, and every config passes validation invariants
   (`crash_apply + crash_torn <= 1.0`, latency ranges ordered).
3. `cover_counts_marks`: `sim.cover("x")` twice → `coverage()["x"] == 2`.
4. `auto_coverage_fires`: a `FaultConfig { write_error: 1.0, .. }` sim whose write fails
   → `fault:write_error ≥ 1`; a restart after unsynced writes → `restart` and at least
   one `crash:*` key present.
5. `swarm_runner_reports_seed_on_panic` (the harness-gap fix, meta-test style like
   c3026b2): a workload that panics → `catch_unwind` around the runner → the panic
   message contains `seed:`, `config:`, and the repro command.
6. `swarm_runner_requirement_failure_names_the_key`: a no-op workload with
   `require: ["never:happens"]` → message contains the key and the guidance text.
7. `swarm_universe_replays_identically`: run one seed through the swarm derivation twice
   (workload doing entropy+disk work) → same `durable_fingerprint`, same `now()`.

Evidence bar (hard rule 3 / CLAUDE.md): full workspace green; each migrated property
test green over its full fresh-seed batch, run at least twice; state total fresh-seed
universe counts in the commit messages.

## Non-goals

New fault types; touching `run_universes!`/`run_one`; changing default/none configs;
any core-crate change; fixing the redb panic; per-universe fault *schedules* beyond
config sampling (BUGGIFY-style per-site freeze is a step-6+ idea, out of scope here).
