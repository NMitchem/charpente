# M2 sub-project 1 — honest benchmarks: design

*The instrument for DECISIONS §7's build triggers (the own-LSM engine is deferred
behind measurement, ruled 2026-08-06, commit f099aac), and PLAN's own unfulfilled
risk mitigation: "publish honest benchmarks early." Measurement only — this
sub-project changes nothing inside the deterministic boundary.*

## Context

MANIFESTO §7 and README both claim "hundreds of thousands of transactions per
second" on one big box. No benchmark code exists anywhere in the repo; the claim
has never been measured. Two facts, verified in source, frame what measurement
will find:

- `prod.rs:171-174` — the production `File::sync` is `sync_all()`, which on macOS
  is `F_FULLFSYNC` (the comment at the site says so, and why: plain fsync there
  does not survive power loss). Laptop numbers will be honestly durable, and
  durably slow — `F_FULLFSYNC` runs low-thousands per second on Apple SSDs.
- `engine.rs:110-123` — the primary's submit path is append → sync → apply → ack:
  **one sync per transaction, no group commit**. The follower's apply path
  (`engine.rs:137-162`) already amortizes one sync over a shipped batch, so the
  engine shape supports batching; the primary path does not use it.

**The prediction this spec commits to testing:** the core write path measures at
≈1× the device's durable-sync rate — sync-bound, far below the headline claim on
any hardware. If confirmed, the gap between measured tps and the headline is a
*group-commit question for a future sub-project*, and this harness is what sizes
it. If refuted, we learn something better. Either way the number becomes
DECISIONS §7 trigger (a)'s instrument: "benchmarks show redb limiting the
headline throughput claim" is answerable only when the bottleneck's layer is
measurable — a sync-bound commit path indicts the log discipline, not redb, and
no LSM would change it.

## Scope (Noah rulings, 2026-08-06)

1. **The LSM premise re-examination resolved: defer, benchmark first.** Recorded
   in DECISIONS §7 (reopened on schedule, re-closed with three named triggers).
2. **Scope: the headline claim, layered.** Charpente alone at four measurement
   points (device / core / kernel / wire). No incumbent-stack comparison in this
   sub-project.
3. **Workload: canonical txn primary, invoicing secondary.** The README-shaped
   transaction is what "tps" means in every quoted figure; `examples/invoicing`
   provides a realistic-app figure at the wire layer only.
4. **Hardware: laptop now, server at launch.** Dev-hardware numbers are honestly
   labeled as such; the numbers of record for launch come from a rented
   bare-metal Linux box when launch nears (not part of this sub-project).

## §1 Shape and placement

A new workspace crate, `crates/bench-harness`, on the `edge-harness` precedent:
outside the deterministic boundary, wall clocks and threads permitted via
site-scoped deny-list allows, never on the `cargo test --workspace` hot path.
Single dependency on `charpente` (the officially load-bearing re-export surface
reaches `Db`, `Kernel`, and the sim-source seam; a second dep is a design smell).
No new external dependencies — load generation, timing, and histograms are
hand-rolled (§4).

Contents:

- **lib** — the canonical bench app: minimal `#[table]`/`#[route]`/`#[job]`/
  `#[event]` surface plus its `build()` fn, exported so every bin below shares
  one app definition.
- **bin `bench`** — subcommands `device`, `core`, `kernel`, `wire`, `smoke`,
  plus `all` (*amended at plan fix-wave, 2026-08-07: report assembly lives in
  Rust as a sixth subcommand because JSON is hand-rolled here — merging JSON
  fragments in bash without a JSON library would be worse; `scripts/bench.sh`
  delegates to it*); human and JSON output.
- **bin `bench-app`** — the wire-layer vehicle. *Amended at plan fix-wave
  (2026-08-07): originally "`charpente::launch(addr, data_dir, build)`, exactly
  the standard app `fn main`" — impossible as stated. `launch()` re-reads the
  live process argv (`launch.rs:31`) and its `dispatch` serves only on ZERO
  argv (or `describe`); a child spawned with `--addr`/`--data-dir` flags exits
  2 as an unknown verb and never binds a port. `bench-app` therefore follows
  the committed vehicle precedent (`examples/invoicing/src/bin/node.rs`, whose
  module doc records this exact trap): parse its own flags, call
  `charpente::serve::serve`, park.*
- **`scripts/bench.sh`** — runs the full matrix and assembles one report.

## §2 The four measurement points

Each layer's delta from the previous localizes cost; no number is published
without the layer beneath it.

- **`device`** — the hardware ceiling. Two loops against a file on the target
  data directory's filesystem, through the production `Disk` impl: sync-only
  (repeated `sync` on a dirty file) and append-shaped (`write_at` + `sync` per
  iteration, the WAL's op shape). Reports durable syncs/sec. Every other number
  is published as a ratio against this one. *Amended at Task 4's review
  (2026-08-07): "the target data directory's filesystem" is an operator input,
  not a default — the `--dir` flag names it, defaulting to a throwaway temp
  dir when omitted (fine on a real disk, silently wrong on `tmpfs`; see §5's
  RAM-backed filesystem guard).*
- **`core`** — `Db::transact` in a closed loop against the prod `Disk` on a
  throwaway data dir: the commit path alone (WAL append + sync + apply + store
  materialization), no kernel, no HTTP. The prediction in Context is about this
  number.
- **`kernel`** — a `Kernel` fed by an in-process bench source implementing
  `RequestSource` (`crates/charpente/src/kernel/mod.rs:65`, the seam `SimSource`
  and `ProdSource` share), requests pre-built, no TCP.
  Adds loop scheduling, dispatch, session/auth resolution, and job execution to
  `core`'s cost.
- **`wire`** — spawn `bench-app` as a child process (edge-harness's child
  pattern), drive real HTTP/1.1 keep-alive load from in-process client threads.
  This is the number the README claim is about.

## §3 Workloads and metric definitions

**Canonical txn** (primary, all layers): get one row + mutate + put + enqueue
one job + emit one event — the README example's shape. The job handler is a
no-op state transition (its Done-mark still commits a transaction; that is the
point). Platform crons exist but are quiescent at benchmark timescales (the
session sweep is daily).

Two write metrics, always reported together:

- **acked writes/sec** — `Ok` returns from `Db::transact` (core), 200s at the
  kernel/wire layers. 200-after-durable makes this honest by construction.
- **log records/sec** — the canonical request commits one effects record and its
  job's completion commits another, so ≈2× acked writes at steady state. Stating
  both forecloses flattering double-counting in either direction.

**Snapshot reads/sec** (secondary, core layer only — *amended at plan-writing,
2026-08-06: a wire-level read sweep mostly re-measures HTTP overhead the write
sweep already shows; deferred unless the read story needs wire evidence*):
typed gets against a populated store. Reads never touch the log; this number
exists to show where the write ceiling does *not* apply.

**Invoicing** (tertiary, wire only): `examples/invoicing`'s node binary driven
on one write route (the plan pins which), labeled "realistic app" — its handler
mix measures an application, not the platform's limit.

## §4 Load generation and metrics collection

Closed-loop, hand-rolled: N client threads, one keep-alive connection each,
each thread issuing requests sequentially and recording per-request latency from
a monotonic clock. Connection sweep: 1, 4, 16, 64, 256; per sweep point,
throughput plus p50/p99/p999 from a full sorted latency vector (no lossy
histogram at these volumes). The kernel is single-threaded, so rising N mainly
measures queueing delay — that is a feature of the design, not a flaw of the
harness, and the report says so.

Two disclosures ride every wire report: **closed-loop semantics** (coordinated
omission — a slow response suppresses subsequent arrivals, flattering tail
latencies versus an open-loop arrival process) and **client saturation** (client
CPU time is measured; a client that cannot outrun the server invalidates the
point, detected by comparing client-side busy time against wall time).

## §5 The honesty contract

Every report — JSON and human — embeds, not as prose elsewhere but as fields in
the artifact: hardware model and core count, OS and filesystem, sync semantics
(`F_FULLFSYNC` named on macOS), the build profile (`dev`/`release`, from
`cfg!(debug_assertions)` — *added at Task 5's review, 2026-08-08, Noah ruling
#3: every number in that review, the implementer's report and the ledger came
from an unoptimized dev build; this barely matters for the fsync-bound write
path and matters a great deal for `reads_per_sec` and the kernel/wire layers.
`scripts/bench.sh`, not yet created as of this amendment, must force
`--release` when it lands.* *Amended at Task 5 fix round 2's re-review
(2026-08-08, N-5 / Noah ruling #5): `cfg!(debug_assertions)` names the right
mechanism but was silent about a workspace-specific fact —
`[profile.release.package.redb] debug-assertions = true` (the 2026-07-15
mode-skew fix, kept deliberately for the CI replay guarantee it holds) means
a `--release` bench run still measures a redb compiled with its OWN debug
assertions live, on exactly the path every core write lands on and all
~1.1M reads/sec come from. The field's release-mode value now states this:
`"release (redb debug-assertions on, workspace override)"`.*), warmup
discarded (default 5 s), measured
duration (default 30 s per point), and the device-ceiling ratio
(core acked-writes/sec ÷ device syncs/sec — *amended at Task 5's review,
2026-08-08: the artifact also states, as its own field, which device operand
the ratio divides by (`device.syncs_per_sec`).*).

Run-validity rules, mechanical:

- **Suspension guard** (the measurement-method rule: laptops sleep mid-run and
  wall clocks cannot tell). *Amended at plan-writing (2026-08-06): the original
  CPU-vs-wall divergence check is wrong for fsync-bound loops, where CPU is
  legitimately tiny against wall.* *Amended at Task 4's review (2026-08-07): a
  heartbeat thread alone cannot detect a suspend — its tick interval and the
  wall clock it was compared against (`Instant`) both exclude suspended time
  on Apple and Linux alike, so a genuine sleep left the original ratio
  untouched and the shipped reason string named a cause that could not have
  occurred.* The shipped mechanism is now two independent arms. A
  **suspension** arm compares a suspend-inclusive clock (`SystemTime`)
  against the monotonic `Instant`: the two diverge only across a real
  suspend (small NTP steps aside, absorbed by a slack threshold), and a
  divergence beyond it marks the run INVALID by name. *Amended at Task 4's
  re-review (2026-08-07): the reason string states why such a run is rejected
  honestly. Rates are NOT arithmetically corrupted by a suspend — every layer
  divides by the monotonic `Instant`, which excludes suspended time, so the
  measurement pauses rather than inflating its denominator. The run is
  rejected because the window was not one continuous run on a settled machine
  (cold caches, disturbed thermal and scheduler state). The same arm also
  fires on a large forward clock step (an NTP `makestep`, a resuming VM),
  which the reason names as a second cause rather than asserting a laptop
  sleep that may not have happened.* A **starvation** arm
  keeps the original heartbeat thread ticking on a fixed interval, observed
  ticks below half of expected: this never fires during a genuine suspend
  (the tick thread's own sleeps exclude suspended time exactly like
  `Instant` does), but does catch the process staying awake while starved —
  `SIGSTOP`, App Nap, severe CPU oversubscription. Either arm marks the run
  INVALID in the artifact itself, not in a footnote. Process CPU time is
  still recorded — it drives the client-saturation check.
- **RAM-backed filesystem guard** (added at Task 4's review, 2026-08-07): the
  device ceiling must be measured on the target data directory's real
  filesystem (`--dir`, defaulting to a throwaway temp dir) — a `tmpfs`/
  `ramfs` mount makes `fsync` close to free, so every ratio computed against
  it would be a plausible-looking number that means nothing. Detected by
  name and probed against the directory actually measured, not a hardcoded
  path; the run is marked INVALID in the artifact and still rendered (same
  pattern as a non-finite float, not a refusal — an invalid run's own
  artifact is where its explanation lives). *Amended at Task 4's re-review
  (2026-08-07, Noah ruling): this guard has a precondition — it can only
  classify a filesystem it could identify. When the probe degrades (a `df`
  that rejects its flags, a device name containing spaces, an unreadable
  directory) the label falls back to `unknown`, which is not RAM-backed and
  so would previously have published an unguarded run at exit 0 — the same
  failure mode in a narrower case. An undeterminable filesystem is therefore
  itself INVALID by name, stating that the guard did not run: an unverifiable
  substrate cannot back a hardware-ceiling claim.*
- **A degenerate measured window is refused, not published** (added at Task
  4's review, 2026-08-07): a window completing zero ops divides into a
  *finite* `0.0` rate, which the non-finite check cannot catch, so each
  measurement loop refuses by name instead — this one is an error rather than
  an INVALID artifact, because there is no measurement to render. Likewise a
  core-count probe reporting `0` (the one value no real system reports) marks
  the run INVALID rather than publishing a lie-shaped number silently.
  *Amended at Task 5's review (2026-08-08, C-3 / Noah ruling #2): "zero ops"
  was not a strict enough floor. A window completing exactly one op is the
  same defect one step later — a rate that is really just that one
  operation's own latency inverted (reproduced through the shipped CLI at
  `--duration 0.004`, ratios up to 1.543). The real floor a measurement loop
  must clear is that it actually ran its full requested duration AND
  completed a minimum op count. Every measurement loop in this crate is
  shaped `while Instant::now() < end { op() }`, which can only exit once
  `Instant::now() >= end` — so `elapsed >= duration` holds unconditionally
  the moment such a loop returns, and the "ran its full duration" half of the
  floor is structurally guaranteed by the loop shape itself. The floor
  therefore collapses to a minimum op count
  (`measure::MIN_MEASURED_OPS`, currently 5), applied uniformly by
  `measure::degenerate_window_reason` to `device::timed_loop` and to both of
  `core_bench::run`'s write and read loops.*
  *Amended again at Task 6's review (2026-08-09, C-1 / Noah ruling): the
  sentence "every measurement loop in this crate is shaped `while
  Instant::now() < end { op() }`" was true when it was written and Task 6
  made it false. The kernel layer is BATCH-shaped: the operator sets
  `--requests` and the window is the observation, exactly inverting the two
  roles the structural guarantee above depends on. Applying an op-count floor
  there bounds the operator's input and leaves the measured window unbounded,
  which is how `bench kernel --requests 5` published 83 acked writes/s at
  exit 0 with `invalid: []` from a ~46 ms window while `--requests 4` — a
  window of essentially the same length — refused. The floor is therefore
  TWO functions, not one: `degenerate_window_reason` for duration-bounded
  loops (the shape above, unchanged), and `measure::degenerate_batch_reason`
  for batch-shaped ones, which keeps the op floor and adds a minimum measured
  window (`measure::MIN_MEASURED_WINDOW`, currently 1s) and names
  `--requests` as the remedy — the shipped message named `--duration`, a flag
  the kernel layer never reads, so an operator who followed it got the
  byte-identical refusal. Any future layer that is not `while now < end` uses
  the batch floor or states why neither applies.*
- **The device-ceiling ratio must be physically plausible** (added at Task
  5's review, 2026-08-08, C-1 / Noah ruling #1): `Engine::submit` performs
  exactly one `wal.sync()` per transaction with no group commit, so the
  commit path cannot durably ack faster than the device's own sync rate in
  fact — a measured `device_ratio` above `1.0` (plus a small slack covering
  floating-point/timer-resolution noise) is a measurement artifact, not a
  real durability finding, and is marked INVALID by name. The reason names
  both candidate causes rather than asserting one: cross-window variance
  (the device ceiling and the core write window are measured in disjoint
  windows, and single-sample noise on the denominator alone was measured at
  220–310 syncs/s across otherwise-identical runs — comparable to the whole
  signal the ratio is about) and non-comparable op shapes (the ratio's
  denominator is the one-byte-overwrite sync loop, not the append-shaped
  loop that reproduces the WAL's own op shape). The core write window is
  additionally bracketed by an independent, short device-sync sample on
  either side (before and after), reported in the artifact as
  `device_syncs_per_sec_before`/`_after` — visible dispersion in place of a
  single distant sample from a separate `bench device` invocation.
  *Amended at Task 5 fix round 2's re-review (2026-08-08, N-4 / Noah ruling
  #4): the shipped slack's stated justification — "floating-point/timer-
  resolution noise" — was wrong by roughly seven orders of magnitude. Over a
  1 s `Instant`-measured window, timer resolution contributes ~1e-9 relative
  error; the shipped 1e-2 instead absorbed a band of cross-window SAMPLING
  noise (the same 220–310 syncs/s dispersion cited above), which is exactly
  the phenomenon this rule exists to detect — a ratio anywhere in
  `(1.0, 1.01]` published unflagged at exit 0 was this rule's own defect, one
  order of magnitude smaller, still shipped. Tightened to `1e-6`
  (`core_accounting::DEVICE_RATIO_SLACK`), still generous for genuine float/timer
  slop, the only cause the constant is meant to excuse. Accepted cost: more
  short/noisy runs now correctly exit 1; the bracket samples above are what
  tell the reader why, not a wider slack that hides the same fact.*
  *Amended at Task 6's review (2026-08-09, M-5 / Noah ruling): this rule was
  scoped to the core layer and the kernel layer shipped without it, which the
  interlude spec had already anticipated ("the ratio gate moves to
  `measure.rs` the moment T6 becomes its second consumer"). It now has two
  consumers, so the RULE — the threshold and the comparison — lives once in
  `measure::DEVICE_RATIO_SLACK` / `measure::implausible_ratio`, while the
  PROSE stays with each layer, because the physics and the available evidence
  differ. At the kernel layer each request commits one effects record and its
  Bump's Done-mark commits another, so the layer spends TWO durable syncs per
  request and the rate the ceiling bounds one-for-one is
  `log_records_per_sec`, not `acked_writes_per_sec` — equivalently
  `acked_writes_per_sec <= syncs_per_sec / 2`. The kernel reason also does
  not cite bracket samples, because that layer takes none. This is not slack:
  a steady-state 2000-request run was measured at 0.99 of its ceiling, well
  inside the band where the cross-window sampling noise cited above would
  carry it past 1.0 unnoticed.*
- **The read window's hit count must match its op count** (added at Task 5's
  review, 2026-08-08, C-2, no spec bullet at the time — added retroactively
  at Task 5 fix round 2's re-review, 2026-08-08, N-9, hard rule 6): every key
  in the read window's key space is populated before the window opens (this
  section's read-population rule below), so under a working read loop every
  `get` is a hit. `core_bench::run`'s read loop counts hits separately from
  attempts and refuses (an error, not an INVALID artifact — there is no
  measurement to render) a window whose hit count diverges from its read
  count. This is the layer's strongest run-validity check: the review's own
  mutation — a read loop reduced to counting attempts with no `get` call —
  published tens of millions of "reads/sec" from a loop touching the store
  zero times, and no portable rate bound can substitute for catching that
  from inside the loop itself.
- **The read window is measured against a populated store** (added at Task
  5's review, 2026-08-08, I-2): population was previously an accident of how
  many writes the preceding write window happened to complete — a miss is
  cheaper than a hit over a smaller tree, so `reads_per_sec` varied with
  `--duration` by as much as 73% on short runs. Every key in the read key
  space is now written OUTSIDE any measured window before the read window
  opens, making the read number duration-independent — the property a
  one-command repro needs.
- **`log_records_per_sec` is checked against acked writes** (added at Task
  5's review, 2026-08-08, I-3, no spec bullet at the time — added
  retroactively at Task 5 fix round 2's re-review, 2026-08-08, N-9, hard
  rule 6): the canonical transaction commits exactly one effects record per
  `transact` (§3 — no dispatcher runs at this layer to double it with a job
  Done-mark), so the WAL's applied-seq delta over the measured write window
  must equal the count of acked writes; a mismatch is marked INVALID by name
  rather than silently publishing a wrong `log_records_per_sec`. *Amended at
  Task 5 fix round 2's re-review (2026-08-08, N-1 / Critical): the first
  shipped version of this check computed the delta with
  `applied_after.unwrap_or(0) - applied_before.unwrap_or(0)`. WAL seqs are
  0-based, so `Db::last_applied() == None` means zero records applied while
  `Some(0)` means one — `unwrap_or(0)` conflated the two, undercounting the
  delta by exactly one on every `--warmup 0` run and firing this very rule's
  INVALID reason falsely. Fixed by matching `(applied_before, applied_after)`
  explicitly instead of collapsing `None` into the same number as `Some(0)`.*
- **Totals two ways**: every throughput number is computed both as
  requests ÷ duration and as a sum over per-thread counters; disagreement beyond
  rounding is a harness bug and fails the run.
- A number without its device ceiling alongside it is not publishable output;
  the report generator refuses to render one.

## §6 Reporting artifacts

JSON + human output on the sim harness's report discipline (DESIGN §7:
machine-readable surface, shape pinned by tests). `docs/BENCH.md` is the
committed methodology-and-numbers page: the honesty contract stated for readers,
the current numbers-of-record table with hardware labels ("Apple M-series
laptop, F_FULLFSYNC" until the Linux box run at launch replaces them), and the
one-command repro for each row. BENCH.md carries no `rust` code blocks (it is
not compile-gated and must not accidentally become so).

## §7 Testing the harness

The harness is itself code that can lie, so it gets its own checks, `#[ignore]`d
on the edge-harness model and run by `scripts/bench.sh --smoke`:

- an end-to-end smoke driving every subcommand at ~1 s durations against the
  real binaries;
- the totals-two-ways cross-check exercised with a deliberate mismatch (proves
  the failure arm exists, RED-first);
- the JSON shape pinned by test, sim-harness style;
- the client-saturation and CPU-wall INVALID arms each forced once, by
  construction rather than by luck (a sleeping thread, a starved client).

## §8 Non-goals

- No comparative incumbent-stack rig (deferred; would roughly double the work
  and demands scrupulous fairness — its own sub-project if ever).
- No CI gating, no regression tracking, no perf dashboards.
- No group-commit or any other platform change — measurement only. The harness
  sizes the group-commit opportunity; a future spec spends it.
- No sim-loop universes/sec metric (developer-loop speed is a separate concern
  with its own memory entry).
- No server rental, no launch-numbers run — this sub-project ships the
  instrument and the first honestly-labeled dev-hardware numbers.

## §9 Exit criteria

1. `scripts/bench.sh` produces one assembled report covering all four layers
   plus the invoicing wire run, on the laptop, with every honesty-contract field
   populated and both validity cross-checks live.
2. `docs/BENCH.md` committed with the first numbers of record, dev-labeled, each
   row carrying its one-command repro.
3. The Context prediction confirmed or refuted, in BENCH.md, with the measured
   core-to-device ratio — and if confirmed, the sized group-commit gap recorded
   as the follow-up's opening evidence.
4. Zero diffs inside the deterministic boundary: no `charpente-core` or
   `charpente` source change, trivially preserving every pinned seed. (Should a
   seam prove genuinely missing for the kernel layer, that is a stop-and-ask,
   not a workaround.)
5. The usual final bytes: fmt, clippy `-D warnings` (deny-list allows
   site-scoped in the new crate only), full workspace green, guide gate
   untouched.
