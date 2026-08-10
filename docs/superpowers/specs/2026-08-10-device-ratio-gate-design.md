# The device-ratio gate — design (2026-08-10)

A Noah-ruled interlude inside the benchmark-harness sub-project (spec
`2026-08-06-benchmark-harness-design.md`), opened by Task 8's §6 "What is NOT met".
It changes a gate that already carries two of Noah's rulings, so §7 records what
is being reopened and on what evidence.

## 1. The defect

`bench` refuses healthy runs. Measured on the T8 machine (Apple M4 Pro, apfs,
`F_FULLFSYNC`, dev build), by simply running the shipped CLI:

| layer | gate denominator | true ratio | observed |
|---|---|---|---|
| `core` | one-shot `device.syncs_per_sec` | 1.0 | **4 of 4 exit 1** (1.072–1.239) |
| `kernel` | one-shot `device.syncs_per_sec` | 1.0 | **2 of 3 exit 1** (1.030, 1.049) |
| `wire` invoicing | per-point bracket max | 1.0 | **3 of 6 exit 1** (up to 1.086) |
| `wire` bench-app | per-point bracket max | **0.5** | passes |

The refusals are not a measurement failure being correctly caught. They are the
gate resolving measurement noise into a verdict it cannot support.

**Why only bench-app passes.** Every layer's gate asks "did this rate exceed the
device's durable-sync rate?" For a target committing exactly one log record per
acked unit the answer is *exactly 1.0* by construction, so the comparison's sign
is decided entirely by the error in the two measurements. bench-app is the sole
configuration with margin: 2.0 records per request halves its **request**-rate
ratio to ~0.5, many multiples of the noise away from the threshold. The gate was
validated where it has headroom and shipped where it has none — the same shape of
error T7 rev. 4 diagnosed in rev. 3, one layer further out.

## 2. The premise — verified, and NOT the defect

The gate's physical claim is that one acked request pays at least one durable
sync. That is **true**, and checked at the source rather than reasoned about:
`Engine::submit` (`crates/charpente-core/src/engine.rs:115`) is unconditionally
`wal.append` → `wal.sync()` → `machine.apply`, with no group commit and no
batching. `submit_batch` (`:141`) does amortize one sync over many frames, but it
is the **follower's** apply path and is explicitly "minus the client ack".

So the numerator is sound and the inequality is sound. What is unsound is
treating a *measured estimate* of the denominator as if it were the physical
constant it estimates.

## 3. The two measured error components

### 3.1 Op-shape mismatch — ~4.2%, systematic

`device_bracket_sample` measures `write_at(0, &[1u8])` + `sync`: rewriting offset
0 of a one-byte file. The path it bounds is a **sequential WAL append**. On apfs
these do not cost the same.

15 paired `bench device --warmup 0.1 --duration 1` runs, each reporting both
shapes: mean syncs 217.70/s, mean appends 226.62/s, **mean paired ratio 1.0421**,
appends faster in 12 of 15 (sign test p ≈ 0.035).

Loop order is a confound in `device::run` (the sync loop always runs first), and
it is ruled out by an independent probe that alternates which shape runs first —
14 iterations, its own `ProdDisk`, no shared state with `device.rs`. Restricted to
iterations ≥ 4 (post process warm-up):

| order | append/sync |
|---|---|
| sync-first (append runs 2nd) | 1.0433 |
| append-first (append runs 1st) | 1.0411 |

Identical in both directions, so the advantage is the **op shape**, not the order.
(Iterations 1 and 3, with append at 176.81 and 158.69/s, are process warm-up and
are exactly what makes a naive by-order split read as order-driven.)

**Consequence: the bracket is not an upper bound.** It sits ~4% *below* the rate
of the path it is supposed to bound.

### 3.2 Estimator noise — ~10% median, up to 31.6%

The two bracket samples taken either side of a **single** wire point disagreed by,
across 12 observed points: 1.4, 4.4, 4.8, 29.4, 5.5, 10.1, 31.6, 11.9, 30.9, 11.9,
9.5, 18.4 percent.

The harness is already telling itself, per point, that it cannot pin the
denominator to better than ~10–30% — and then applies a `1e-6` tolerance to the
quotient. Run-to-run drift of the same quantity is the same size: 15 samples
spanned 197.98–234.76/s (mean 217.70, ±8.4%).

The core and kernel layers are worse still, because they divide by a **one-shot
cross-window** sample rather than a bracket: core run 2 divided by 166 while its
own bracket samples, taken moments later in the same process, read 218 and 215.
That is precisely the denominator T7 rev. 3 identified as a false-alarm generator
and replaced with per-point brackets **for the wire layer only**.

## 4. The fix

Three parts. Ruled by Noah 2026-08-10, applied uniformly to all three gating
layers.

### 4.1 The bracket measures the shape it bounds

`device_bracket_sample` becomes a sequential append + sync loop over
`frame_pattern()` — the same op shape as `device::run`'s existing `appends_per_sec`
loop and the same shape as the WAL. Each sample writes a **fresh** file, so it is
an append to a growing file rather than a rewrite of the previous sample's extent.

### 4.2 Every gate divides by a bracket around its own window

This finishes T7 rev. 3's fix at the two layers it skipped.

- **core** already takes `device_before`/`device_after` around its write window
  and merely *publishes* them (`core_bench.rs:84`, `:101`). The gate stops reading
  the threaded-in `device_ceiling` and divides by `max(before, after)`.
- **kernel** gains the same pair around its `Kernel::run` batch window, inside the
  existing `Heartbeat` so a suspend during a bracket is caught exactly as core's is.
- **wire** already does this; its denominator is unchanged.

`device_ceiling` therefore leaves `CoreWriteObs` and `KernelObs` entirely, replaced
by `device_before`/`device_after`. All three accountings become structurally
identical: observations carry the pair, the pure accounting derives ceiling,
spread, tolerance and verdict. `core_bench::run` and `kernel_bench::run` lose their
ceiling parameter, and `bench.rs` stops threading layer 1's number into layers 2
and 3. `bench device` still runs unconditionally and still publishes both rates —
it is the artifact's methodology header, no longer any layer's gate input.

### 4.3 The tolerance is derived from the measurement's own error

```rust
pub const RATIO_TOLERANCE_FLOOR: f64 = 0.10;
pub const RATIO_HARD_LIMIT: f64 = 1.5;

pub fn ratio_tolerance(before: f64, after: f64) -> f64;      // max(FLOOR, spread)
pub fn implausible_bracketed_ratio(r: f64, b: f64, a: f64) -> bool;
```

with `spread = |before - after| / max(before, after)`, and the refusal firing when
`ratio > RATIO_HARD_LIMIT || ratio > 1.0 + ratio_tolerance(before, after)`.

- **The floor** covers the case the spread cannot see: both samples agreeing with
  each other while the true rate *during* the window differed from both. It is set
  from the measured run-to-run drift above (±8.4%), rounded up. It is a
  machine-measured value, not a universal constant, and §6 says so at the site.
- **The spread term** loosens the gate exactly as much as the harness has directly
  observed it cannot measure. A point whose brackets read 147 and 215 is not a
  point at which a 5% overshoot means anything.
- **The hard limit** keeps the gate structurally non-vacuous: no bracket
  disagreement, however wild, excuses acking 50% faster than the ceiling. Without
  it a sufficiently unstable device would tolerate anything.

Calibration, stated plainly: a real violation — ack-before-sync, a dropped fsync, a
build that lies about durability — moves the ratio to 2x, 10x, unbounded. Nothing
about detecting that ever required `1e-6` resolution. What this gate gives up is
the ability to detect a violation *smaller than its own measurement error*, which
it never actually had; it only appeared to have it.

### 4.4 Naming follows the measurement

With 4.1, the bracket measures durable **appends**, so the published fields
`device_syncs_per_sec_before`/`_after` become `device_appends_per_sec_before`/
`_after`, and `CoreReport::device_ratio_denominator` names the bracket max rather
than `"device.syncs_per_sec"`. A pre-release artifact-shape change, in the same
class as realtime's `emit` format and `TxError::Verifying` — a field whose name
contradicts its contents is the kind of record inaccuracy this repo has repeatedly
paid for.

## 5. Non-goals

- **`implausible_ratio`/`DEVICE_RATIO_SLACK` are not deleted.** They remain the
  un-bracketed primitive and keep their tests; `1e-6` remains the floor's floor.
- **No threshold is widened to make a specific run pass.** The floor is derived
  from measured drift and the spread term from per-point data. §8's acceptance is
  a re-measured *distribution*, never a single green run.
- **The wire layer's denominator is unchanged** (it was already correct); only its
  shape and tolerance change.
- **Layer 1 (`bench device`) is not gated and does not change**, beyond the
  bracket helper it shares.
- **No change to `charpente-core` or `charpente`.** `git diff --stat` over both
  must be empty, as T8's was.

## 6. Test plan (RED-first, hard rule 3)

Every test below is proven to fail against current code before the fix, or — where
it pins shipped behaviour — proven by mutation. Both directions get recorded.

1. `device.rs` — **the shape is discriminating**: after a bracket sample the file
   it wrote is larger than `MIN_MEASURED_OPS * 256` bytes. The old shape leaves a
   one-byte file, so this is RED before and GREEN after, and it cannot pass for the
   wrong reason.
2. `measure.rs` — tolerance is the floor when brackets agree; grows with spread;
   the hard limit binds regardless of spread; NaN/infinite operands do not widen it
   (an infinite ratio must still refuse — today's behaviour, kept).
3. `core_accounting.rs` — the gate divides by the bracket max and not by any
   threaded-in ceiling. The discriminating case is core run 2's real numbers: 206
   acked/s with brackets 218/215 must PASS, and would be refused at 1.239 by the
   one-shot 166.
4. `kernel_accounting.rs` — the same, for the pair it newly carries.
5. All three layers — a 2.0x ratio is refused at every spread observed in §3.2,
   including the 31.6% worst case. This is the anti-vacuity test and it is what
   makes 4.3's hard limit load-bearing rather than decorative.
6. `report.rs` — the renamed fields and the new kernel bracket fields are pinned in
   both renderers.

## 7. What is reopened, and why that is allowed

**Ruling 1 (T8): "measure the ceiling on the same path."** Implemented, measured at
0.962x and 1.020x of the device bracket, and reverted as refuted — on **n = 2**.
A 4.2% effect inside 10%+ noise cannot be detected at n = 2; the refutation was
underpowered, not wrong-headed. §3.1 re-runs it at n = 15 paired plus n = 14 with
order controlled. 4.1 is Ruling 1, reinstated on evidence that can actually carry it.

**Ruling 2 (T8): "adopt the core layer's tolerant-ratio precedent."** Not reversed.
It made the invoicing *smoke test* honest without weakening the gate, which is what
it was asked to do, and it explicitly did not claim to make the CLI publish. With
4.3 in place the tolerated arm should stop being exercised on healthy runs; the
smoke keeps its tolerance (a gate that never fires on a short window would still be
the bug) and §8 records how often it actually fires.

**T7 rev. 4's "the gate is on the counted ack rate, not the derived record rate."**
Untouched and still right. This interlude changes what the rate is divided by and
how much tolerance the quotient gets, not which numerator is trustworthy.

**Ruling #4 (T5 re-review, N-4) — the one this interlude is in real tension with,
named rather than quietly overridden.** That ruling tightened `DEVICE_RATIO_SLACK`
from `1e-2` to `1e-6`, on the ground that `1e-2` "absorbed a band of cross-window
SAMPLING noise … which is precisely the phenomenon this gate exists to detect",
and that "a ratio anywhere in `(1.0, 1.01]` published unflagged at exit 0 was C-1's
exact defect". §4.3 re-introduces a tolerance an order of magnitude *larger* than
the one that ruling rejected. Both of its premises have since changed:

1. **The denominator was a single cross-window sample then.** It is now a bracket
   taken either side of the window being judged, so the estimator's error is
   directly observable per point instead of being a blanket unknown. §4.3 does not
   absorb noise with a constant; it absorbs exactly the spread the harness measured,
   and the constant floor covers only the residual the spread cannot see.
2. **No layer had been measured at a true ratio of 1.0 then.** §1 establishes that
   three of the four shipped configurations sit exactly there. That inverts the
   meaning of the evidence: a ratio in `(1.0, 1.10]` is the *expected healthy
   observation* for a one-record-per-op layer, not a signal. Ruling #4 read that
   band as under-detection because, at the time, the only validated configuration
   was the one with 2x margin.

What ruling #4 got right and this interlude keeps: a gate must not be widened to
make a run pass, and slack must be justified by the error it actually covers rather
than by a plausible-sounding story. §4.3's floor is the measured drift and §8's
acceptance is a re-measured distribution, precisely so that standard is met. What
this interlude asserts against it: a gate cannot detect a violation smaller than
its own measurement error, and pretending otherwise does not buy detection — it
buys false refusals at ~50% and a CLI that cannot publish.

## 8. Evidence required

- **The distribution, re-measured**, not one green run: ≥ 6 runs each of `bench
  core`, `bench kernel`, and `bench wire --target invoicing`, with every ratio and
  every exit code recorded, before and after. The acceptance bar is that healthy
  runs stop being refused *and* that §6.5's forced violations still are.
- The repo's standard sweep: `cargo fmt --all --check`; `cargo clippy --workspace
  --all-targets -- -D warnings`; `cargo build -p guide-examples`; `cargo test
  --workspace` ×2 with identical counts; pinned debug replays
  `0x8bee1752811ae873` and `0x348270b38f166529`; both release mode-skew tripwires
  as `ci.yml` runs them; a fresh invoicing 1024-universe batch.
- `cargo test -p bench-harness --test harness -- --ignored --test-threads=1`, the
  9-test set T8 left green.
- `git diff --stat -- crates/charpente-core crates/charpente` empty (§5).
- The edge matrix is **not** required: this touches no replication, auth or
  realtime behaviour. If any diff reaches those, it becomes required.
