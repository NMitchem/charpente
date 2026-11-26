# The fault-free twin

Status: **parked**, 2026-11-26, after the review below. PLAN's after-launch table carries
it with the trigger that reopens it. Revision 2, after the review, stays as the record of
what was tried.

## The 2026-11-26 review

Noah asked for the approach to be checked from the top, because each review round had
added an excuse, and hard rule 2 names that pattern. Three findings came out of it.

**1. Revision 2 compares against the wrong run.** It switches every fault off, outbound
errors and latency included. An app may react to those on purpose. It may release a hold
when payment is not confirmed in time, approve small orders while a fraud service is down,
or tell a customer at once that a charge failed on a 503. Each of those makes the faulted
outcome differ from the fault-free one, so revision 2 fails an app for a business
decision. An oracle stricter than the real contract also teaches an agent to hide logic
from it. Every excuse in §5.4 and every open question in §12 traces back to this.

**2. A reference that needs no excuses.** Remove only the faults the platform promises to
hide: crashes, power loss, torn writes, lost replies and re-sends, failover, and partitions
between client and server. Replay everything the app observed as the faulted run saw it:
each logical request once, in the order it committed, each transaction's clock reading,
and each remote reply, errors and outages included. The two end states must then be
byte-identical. The only exceptions are the loss windows already documented (DESIGN §2's
cut acked frame, DECISIONS §6's failover window). Remote errors, latency and deadlines are the
app's policy, and the oracle has no say over them. Matching the two runs may need job ids
and outbound keys derived from their cause instead of drawn.

**3. Its value does not cover its cost yet.** 0.7.0 makes the spike's one catch, the double
charge, impossible by construction. What is left for the twin is a fault the platform
failed to hide, and app state kept outside what the app observes. Two fresh agents with no
part in this discussion reviewed it as the user it is for, an agent building an app on
charpente, and both said to park it. A twin red is usually charpente's bug, which the
app's author can neither fix nor switch off, and a red nobody can attribute teaches the
agent to reshape the app until the red goes away. The cheaper step comes first: `Drive`
runs the platform checks itself, so an app no longer has to remember to call them.

If it is revived:
- It runs over charpente's own fixture apps, in CI and nightly, as the platform's
  regression oracle, and stays out of an app author's inner loop.
- A red names the first transaction that diverged (the request or job, and its intent
  id), what it read in each run, the fault that fired before it, and whether the app or
  the platform wrote the value that differs.
- A re-spike comes first, and any false red is a perturbation to remove at its source,
  never an excuse to add.

## Revision 2 (2026-11-22)

Status then: **paused**, 2026-11-22, revision 2. Noah chose the direction and handed the rest
over ("you have autonomy"). The choices marked *(call)* are the designer's. This is step 2
of two. It builds on step 1, retry-safe writes (`2026-11-22-retry-safe-writes-design.md`),
whose intent records tell the twin which requests committed.

**Why paused.** A builder check of this revision raised seven more questions (§12), and
they all share one cause. The two runs drift apart in ways that are not faults:
- phase lengths, and so cron tick counts;
- which call a remote model's draw lands on;
- timestamps inside event payloads and call bodies;
- `on_dead` compensations;
- the clock floor the twin's own boot is judged against.

Each round of review has found a new divergence, and each fix adds an excuse or more
machinery. Hard rule 2 reads on that pattern: if a feature seems to need escape hatches,
stop and redesign.

The twin is therefore re-spiked after step 1 lands, on the real platform, to measure two
things. First, the false-red rate of scenario mirroring on the Drive examples, the scaffold
and an app with a cron, a decision-making remote model and timestamps in events. Second,
whether it catches the §8 fixture. The result decides between three outcomes:
- build this design;
- narrow it, for example to rule 1 alone ("effects never grow"), the one rule every review
  left standing;
- drop it.

Revision 2 follows a builder check of revision 1, which found six places where the code
contradicted the design. Each was confirmed against the source:
- `tx.now()` writes no time to the log, and `Wal::gc` deletes old segments, so revision 1's
  plan to read commit times from the log could not be built.
- Job ids come from an apply-time counter, and outbound keys are drawn, so a job cannot be
  matched across the two runs, and its settle time cannot be pinned.
- The sim's clock-downtime axis jumps the clock up to 3 days on a power cycle.
- Storing every reply would put credentials on disk. Step 1 now handles that.

So revision 2 stops trying to reproduce every transaction's time. Instead, it replays the
faulted run's scenario and names the differences that time and remotes cause.

## 1. Why

MANIFESTO §2 ties agent output quality to oracle density, and §6 makes the framework the
reviewer. For business logic under faults, the reviewer today is a property the app's
author writes, and the agent that writes a bug also writes the property meant to catch it.
The twin is an oracle nobody writes. The harness also runs each passing sim universe that
went through `charpente_sim::drive` as a fault-free twin, and fails the run if faults changed
the business outcome in a way no named excuse explains. The verdict lives in
`charpente-sim`, which the app cannot edit, and no app opts in. It is the framework's answer
to "which number is the agent allowed to fail, and who owns it", for the robustness half of
correctness.

After step 1, the duplicate write can't happen. What the twin still catches:
- **App logic that turns adversity into a different outcome.** For example, a `settle` that
  treats a 503 or a throttle as a final failure and marks an order failed, where the
  fault-free run pays it.
- **Workload clients that break the contract.** A hand-built re-send with a new intent id
  double-applies.
- **Platform regressions.** Every app's workload becomes a regression oracle for the
  platform: lost acked state, duplicated effects, events out of step.
- **Anything a future fault axis or platform feature breaks**, with no new property written.

The idea is old: failure transparency (Lowell, Chandra and Chen, OSDI 2000), CrashMonkey's
crash-free oracle (OSDI 2018), metamorphic testing. charpente's contribution is cost.
Replayable universes, client-chosen row ids, intent records and a driver that knows its
re-sends make the twin cheap enough to run on every app by default.

## 2. Goals, non-goals, anti-goals

Goals:
- Every check that drives recovery through `Drive` is compared against its twin, in every
  runner, with no opt-out.
- A twin failure names the contract, the site and the fix, and prints the seed and a
  one-command repro (hard rule 5).
- Every excuse is named, detected mechanically and counted per check.

Non-goals:
- Checking intent. The twin cannot tell whether the fault-free behaviour is what was meant.
- Checks that drive recovery by hand. They get no twin, and the report says so.
- Reproducing each transaction's exact time. Time differences are classified (§5.5), not
  replayed.

Anti-goals:
- An opt-out (hard rule 2, MANIFESTO §8).
- An excuse that hides a duplicated effect. Effects never grow under any excuse but a give-up
  and durable corruption.
- Coverage from the twin satisfying a check's `require:`. The twin's coverage is discarded.

## 3. Evidence: the spike (2026-11-22)

Worktree `charpente-twin-spike`, branch `twin-spike`, uncommitted. Master seed
`0x434841524745`, 256 runs per check.

| App | Unexcused differences |
|---|---|
| double-charge, bug in, property removed | 22 runs, every one an extra charge |
| double-charge, guard restored | 0 |
| restart_under_faults, modelled_remote, drawn_inputs | 0 |
| probe, orders stamped with `tx.now()` and numbered by a counter | 192 |

What carries into this design:
- **Excuses.** A job going Dead (seed `0x2a5860b4f12a0e4c`: latency up to 2 s against a 2 s
  timeout, which is not a fault axis) and durable corruption (seed `0xaa980c770e24106d`,
  DESIGN §2's cut acked frame) are needed and sufficient for the apps above.
- **A coarse Dead excuse hid 6 double charges.** "Effects never grow" recovers them.
- **Time.** The probe's 192 differences were time values. A restart can also hand out times
  earlier than committed ones (seed `0xa34916d06e0d6d06`, 49 ms, DESIGN §1's recovered
  open-time bound), so time cannot order the twin.
- **Cost.** One app, debug build: 12.4 s to 28.7 s of user CPU. An observation, not a
  docs/BENCH.md figure.

## 4. The twin's scenario

The twin is the faulted run's scenario with every fault removed:
- **The same inputs.** The faulted run's input choices replay into the twin.
- **The same requests in the same phase.** A request committed in the faulted phase goes in
  the twin's faulted phase, at its original offset. That covers both requests answered there,
  and requests whose reply was lost but whose intent record carries a `committed_at` before
  the faulted tail began. Every other request goes in the twin's tail, at the tail's start,
  as `Drive` re-sent it. That keeps the workload's own switches in order. For example,
  double-charge's `calm.store(true)` sits between the phase and `recover`, so a tail request
  in the twin also meets a calm gateway.
- **The same process lifetime.** The twin power-cycles where `Drive::recover` did. With no
  disk faults, the restart loses nothing.
- **The same clock at each phase's start.** The twin's wall clock is offset so its faulted
  phase starts at the faulted run's phase-start wall time, and its tail at the faulted tail's
  start. A multi-day downtime jump is therefore mirrored, cron catches up once in both runs
  (GUIDE §7), and a TTL that ran out over the downtime runs out in both.
- **No faults, no latency.** Every `FaultConfig` axis is off, and outbound latency is zero
  *(call: an ideal world. Latency is adversity that can kill a job, and the Dead excuse
  covers it.)*

`Drive` records what the twin needs on the `Sim` at the end of `recover`:
- the phase-start and tail-start wall times;
- for each request index, whether it was answered in the phase;
- the end state (§5).

The faulted `Drive` knows each request's intent id, since it stamped it from the index, so
it reads `committed_at` from `charpente.intents` by that id.

## 5. The comparison

### 5.1 What is compared

- **Outbound effects.** For each host, one `(method, path, body)` per distinct idempotency
  key that reached the remote, as a multiset. The key's own text is replaced in the body
  before comparing, since apps may carry it there (GUIDE's `Idempotent::by`).
  `outbound::Delivered` gains the method, path and body.
- **Events.** `(name, key, payload)` per emitted event, as a multiset, without ids or
  sequence numbers.
- **App tables.** Every table not owned by the platform, by row id. The builder adds a
  registry flag for platform-owned tables. The spike's name test missed `CronRow`. Session,
  API key and intent tables are platform-owned and are not compared.
- **Not compared:** `charpente.jobs` (attempts and due times legitimately differ).

### 5.2 The rules

1. **Effects never grow.** Every faulted effect must appear in the twin's multiset at least
   as often. This holds under every excuse but a give-up and durable corruption.
2. **Events and effects are otherwise equal**, unless an excuse in §5.4 applies.
3. **Rows:**
   - The same row ids exist in both runs, unless an excuse applies.
   - Rows with the same id are equal, except for differences that §5.5 classifies.

### 5.3 The twin's own run

- If the twin's check returns `Err` or panics, the check fails with no faults at all. That
  is a red with its own message ("this check fails in its fault-free twin"), because the
  property or the app is wrong without any adversity.
- A twin that gives up, never reaches `recover`, or hits `TAIL_SETTLE_CAP` is a platform
  error, reported by name with the seed.
- The twin's coverage is discarded, and its own result never counts as a test of the check.

### 5.4 Excuses

Each excuse is detected mechanically and counted in the report:

1. **The faulted run gave up** (existing). No comparison.
2. **Durable corruption fired** (`checks::durable_corruption_fired`). The universe is
   excused whole. DESIGN §2 already excuses loss here, and the spike's corruption universe
   also shifted a counter.
3. **A job went Dead** in the faulted run. Row and event differences are excused. Effects
   still never grow.
4. **The faulted clock handed out an earlier time after a restart** (`committed_at` values in
   `charpente.intents` decrease across the restart). DESIGN §1's documented bound. It
   disappears when PLAN's "a stored wall reading" row ships, and the twin's count of it is
   evidence for that row.

### 5.5 Classified differences inside a row

When two rows with the same id differ, each differing value is classified:
- **A time value.** An 8-byte word that reads as a wall time inside the universe's range
  (from the earliest phase-start wall minus `CLOCK_TOLERANCE` to the last wall reading plus
  the longest downtime jump) *(call: classify by value in v1; if `describe` gains typed
  `WallTime` fields, classify by type instead)*.
- **A value a remote returned.** A byte run that appears in a reply body the faulted run's
  remote returned, at the position where the twin's row holds a run from a twin reply body.
  For example, a charge id from a gateway that numbers charges in arrival order.

A row whose every difference is classified passes, and the counts are reported. A row with
an unclassified difference fails. Classification never applies to the row-id or effect
rules.

## 6. Where it runs

The twin is one function in `charpente-sim`, called by every runner after a passing,
non-given-up run in which `Drive::recover` recorded an end state: `run_app_universe`
(`charpente test --sim`), and the `run_universes*` family. A check driven by hand records
nothing and gets no comparison. Each check's result line says how many runs the twin
compared, and how many each excuse covered: `twin: 231 of 256 compared, 18 excused (dead
14, corruption 4)`. `--format json` carries the same numbers.

## 7. The failure message

```text
check each_order_is_charged_once ... FAILED
  faults changed the outcome: this run's fault-free twin ends differently
  pay.example received POST /charge "order=4" twice under distinct keys; the twin, once
  rule: effects never grow (excuses checked: none applied)
  fix: a write was applied twice: a client re-sent it with a new Idempotency-Key, or a job
  enqueued a second effect for one request; re-send with the same key (GUIDE §5)
  replay: CHARPENTE_SEED=0x… cargo test --test sim -- --seed 0x…
```

A row failure names the table and row id, prints both rows, and marks each unclassified
difference. *(call: decoded through `describe`'s field list when it has one, else hex)*

## 8. Validation (red first)

Before the twin lands, this fixture fails under it and passes without it:
- **An app whose `settle` marks an order failed on a 503.** The faulted run has failed
  orders the twin pays, and no excuse applies, because the job settled rather than went
  Dead.
- **The spike's probe** (`tx.now()` stamps plus a counter) passes, because its time values
  are classified.

These must stay green: every Drive-based example, the scaffold's checks and double-charge's
patched copy.

## 9. What changes elsewhere

- GUIDE §8 documents the twin, its excuses and its report line. DESIGN gains the twin under
  its sim section. CHANGELOG 0.7.0 lists it.
- PLAN gains rows for: moving the hand drivers (invoicing's sim, the fixtures) onto `Drive`,
  classifying time by type, and an intent id read from a request body.
- docs/BENCH.md records the twin's cost when a run is measured there.

## 10. Left to the builder

- How the twin's clock offsets are applied: the `WallClock` drift, as the spike did, or a
  named constructor on `Sim`.
- The exact byte-run matching for remote-returned values. It must be deterministic and
  documented, and the fixture in §8 must still go red.
- How the runners share the twin function without changing the frozen seed-to-behaviour
  mapping of the faulted run.

## 11. Risks

- **Cost.** About 2.3x CPU per batch in the spike. The nightly sim and the workspace suite
  absorb it.
- **Classification hiding a real difference.** A wrong time value still looks like a time,
  so the twin cannot see a wrong timestamp. That is accepted, and stated in GUIDE §8.
- **Excuse creep.** A new excuse needs a fixture showing it hides no duplicated effect.

## 12. Open questions from the second builder check

These come from the builder's report, and the re-spike verifies each one against a running
build before the design answers it.

1. **The twin under `run_universes*`.** charpente-sim's
   `tx_now_is_seed_stable_and_never_runs_backwards_within_a_process` (`tests/determinism.rs`)
   builds a second faulted `Sim` inside its check. The twin would fail it in nearly every
   universe. Does the twin hook only `run_app_universe`, or does `Drive` ignore records made
   on a `Sim` other than the runner's?
2. **Phase placement.** A phase commit's `committed_at` (the held wall) can sit above the
   tail's start after a step back. Two alternatives decide it without comparing wall times:
   a replayed reply in the tail marks an earlier commit, or the set of intent ids present at
   the tail's boot. Also, charpente-sim needs a read path to the sealed intents table.
3. **Phase length and the twin's clock floor.** Should the twin's phase end where the
   faulted phase ended, and the tail run to the faulted `Recovered::wall`? A copied clock can
   also land behind the twin's own recovered floor and get its open refused.
4. **Classification beyond rows.** Time values and remote-returned values also travel in
   event payloads and call bodies. Should an effect an `on_dead` hook enqueues count as
   growth?
5. **Remote-model draws.** Each host's model draws from one sequential stream in delivery
   order. Should the twin key draws per call, replay the faulted run's replies per key, or
   excuse a model that makes decisions?
6. **Recording without disturbing.** Scanning the end state inside `recover` reads through
   `SimFile`, which may draw disk latency and move virtual time. Should recording move to the
   runner, after the check returns?
7. **Several Drive cycles on one `Sim`,** or `recover` given a hand-built `Phase`. What is
   recorded then?
