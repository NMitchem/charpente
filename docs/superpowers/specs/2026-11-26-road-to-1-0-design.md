# The road to 1.0

**Status:** approved by Noah, 2026-11-26. Nothing here is built.

**Where it came from:** two outside agents reviewed charpente against Noah's essay ("Nobody
Has Actually Built a Software Factory", noahmitchem.com/notes) and the "2027 bets" artifact.
Claude checked their claims against the repo, and Noah made the calls in §2, §3 and §5 in
the same conversation. The evidence they rest on is in §10.

This document does two things. It defines 1.0, with the work that leads to it in four
stages. It also says which standing decisions change and which documents change with them.
The plan written from it covers only those document edits (§8). Each stage's engineering
gets its own spec and plan when it starts.

## 1. Intent

When the roadmap closed at M3, the bet was that simulation and compile-time contracts make
an agent's backend trustworthy. The record since then shows where that bet stops.

- **Renewly (0.5.0's docs-only build).** It had seven checks, all passing, and 75 of 75
  viable mutants were caught. No check ever read the amount charged, so a $19 plan billed
  at $190 would have passed (review R4). The record filed this as "Minor, app".
- **The launch demo's rehearsal.** A conventional TypeScript and Postgres stack tied
  charpente, 0 violations to 0, on a small task. The charpente build took 7645 s against
  3940 s.

So the gap is not that agents write wrong code charpente can't catch. The agent writes both
the code and the checks that judge it, and nothing outside the agent says what was never
checked. Noah's essay names the condition for a closed loop: the agent "can read the score
without being able to change it". charpente meets that condition for the platform, through
compile errors and `charpente_sim::checks`. It does not meet it for an app's business logic.

Noah's call (2026-11-26): the frontier has moved, so charpente buys in. 1.0 becomes the
milestone that closes this gap, and decisions made for the earlier frontier change where
they stand in the way.

## 2. The 1.0 goal (decided)

> An outside team can hand one consequential workflow to agents, let them change it many
> times, and accept each change without reading the code. charpente gives them a verdict
> anyone can re-check, saying which of the team's promises were checked, how strongly, and
> what nothing checked. Their data and unfinished work survive every charpente upgrade.

In one line: accept an agent's change without reading it, and keep the business you
already have.

These are ranked. The verdict, judged against promises the building agent cannot edit, is
the point. Rehearsing a change on a copy of real data is what makes the verdict mean
something. Data surviving upgrades is the price of entry: no team trusts a workflow to a
platform that refuses last month's data. The experiment (stage 4) is the proof.

## 3. What a consequential workflow is (decided)

Any workflow with both of these properties:

1. **A mistake costs someone.** If a wrong answer costs nothing, nobody needs a verdict.
2. **Something outside the app can say whether the mistake happened.** A processor's
   settlement record, a receipt, a seat map, an access log. This is what makes a promise
   checkable at all.

Examples: billing, selling something scarce (seats, slots, inventory), deciding who may see
or do what, obligations with deadlines, orders and fulfillment. Workflows where nobody can
state what "right" means, such as ranking, recommendation or UI, are out. MANIFESTO §8's
scope still holds: greenfield backends, one box.

Money is not the scope. It is the first proving ground, because it leaves independent
records. The 1.0 gate tests two domains, so the framework can't end up shaped to payments.

## 4. Goals, non-goals, anti-goals

**Goals**
- The §2 goal, met by the exit criteria in stage 4.
- MANIFESTO, DECISIONS, PLAN, README and CLAUDE.md say what charpente now intends, in the
  same pass (§8).

**Non-goals for 1.0.** Each one either has a PLAN row with a trigger, or is refused in
DECISIONS §16. Where no row exists today, this pass adds one (my call):
- **Proving app code** (Lean, Verus, extraction). New trigger-gated row. Trigger: an escape
  class in stage 2 or 4 that sampling missed and a proof, or an exhaustive check of a
  declared state machine, would have caught. The state-machine row stays as it is.
- **An agent manager, a merge queue, a hosted control plane.** Refused in §16. Other
  projects can manage the agents, and charpente owns the verdict those managers read. The
  hosted control plane stays off the roadmap, as PLAN already says.
- **An MCP or agent tool surface generated from `describe`.** The existing row keeps its
  trigger.
- **A multi-threaded kernel.** DECISIONS §6 stands: BENCH puts the kernel loop at about
  1.3% of the commit window.
- **Replaying recorded production requests.** New trigger-gated row. Trigger: a production
  discrepancy that snapshot rehearsal can't reproduce without the requests that caused it.
- **The fault-free twin.** Parked 2026-11-26, and its row stays.
- **API stability.** APIs still change in place (§7, D2).

**Anti-goals.** These would make 1.0 a failure even if every criterion technically passed.
- Declaring 1.0 because models agree with it. The evidence is the experiment, published
  whatever it shows.
- A green verdict that leaves something unjudged off its list. Renewly's 100% mutation score
  with an unchecked price is the case to never repeat.
- Any escape hatch that makes an upgrade or the experiment pass (hard rule 2).
- Framework features that only make sense for payments.
- A README that reads as if unshipped stages had shipped.

## 5. Approaches (decided: C)

- **A. Build, then prove.** Build every 1.0 piece, then run the experiment as the gate. The
  most direct order, but if the thesis loses, we find out after a month or two of building.
- **B. Prove, then build.** Run the experiment first on 0.7.0 plus F1, and let the losses
  pick the features. The cheapest way to find where the thesis breaks, but it measures a
  charpente that has none of the 1.0 pieces, so it may only repeat the tie.
- **C. A small baseline early, the full experiment as the gate.** Build the cheap verdict
  pieces and F1, run a small baseline experiment, build the rest in an order the baseline
  can change, then run the full experiment as 1.0's exit. **Chosen by Noah, 2026-11-26.**
  It costs a second experiment run. Agent compute is cheap at Renewly's rate, and the hidden
  checks written for the baseline are reused. It keeps the buy-in honest: three models
  agreeing is not evidence, and a measured baseline is.

## 6. The four stages

The sizes are estimates from the repo's own pace: retry-safe writes took about three days
from spec to built, and 0.3.0 through 0.7.0 took about five weeks. They are not a plan.
Each stage ships as ordinary 0.x minors. Nothing waits for a 1.0 tag except the tag.

### Stage 1: the verdict (about 1–2 weeks)

1. **A verdict file.** `charpente test --sim --format json` and `charpente test --mutants`
   write one machine-readable record, bound to what will deploy. Its minimum contents:
   - a format version;
   - hashes of the source tree, the built binary, and the check sources (stage 3 turns the
     last one into the promise pack's hash);
   - the master seed;
   - per check: tested, failed and skipped counts, with reasons;
   - every excused run, with the excuse that excused it;
   - every fence exception: each `allow` of a `disallowed_methods` or `disallowed_types`
     lint in the app's source;
   - the mutation summary, including each function with no viable mutant and whether it was
     argued.

   The repro line stays the one command that replays a failure.
2. **Close the ways a builder can quietly weaken its own oracle.**
   - A function with no viable mutant fails the run unless the app argues it, with one
     rationale per entry, the way `.cargo/mutants.toml` works for charpente. This is
     Renewly's `Charge::plan`: the verb named it, and the bar still counted as met.
   - Fence exceptions are listed in the verdict (item 1) and in the human report.
   - Enforce the rule that `durable_corruption_fired` may excuse a give-up, never wrong data
     (PLAN row, trigger met three times). How the platform tells a give-up from an excused
     wrong answer is this item's open design question.
3. **F1: a `#[route]` that answers a status other than 200, and a typed conflict** (PLAN row,
   rehearsal review F1). The rehearsal's charpente arm hand-built all seven routes because
   of it, which inflated the 2x build time. It is public API and gets its own spec, as the
   row says. F2 (reading an arbitrary request header) joins this stage only if stage 2's
   brief needs it.
4. **The NEIGHBORS.md sweep.** The file says to check quarterly, and it was last checked
   2026-07-02. One outside agent claims a neighbor has gained a simulation runtime, which
   would flip a row. The positioning in §8's README edit depends on the answer.

### Stage 2: the baseline (a few weeks of wall clock, mostly Noah's time)

This stage gets its own experiment spec, with the predictions registered before the run, as
the rehearsal did (`prereg-rehearsal`). It is fixed to this shape:
- **Two arms:** charpente at its current release, and the rehearsal's conventional stack
  (TypeScript, Node, Postgres, graphile-worker). Same model, same brief.
- **One domain:** subscription billing, against a payment provider that keeps its own record.
- **About 20 increments** applied one after another to one growing app: new features,
  policy changes, a provider discrepancy.
- **Hidden checks** in the judge repo (`~/Documents/GitHub/judge`), written outside the
  builders' sessions and reviewed by Noah. The builders never see them. The judge drives
  real binaries over the wire with real faults, as in the rehearsal.
- **Measured per increment:**
  - escaped violations (a hidden check fails);
  - whether the builder's own checks would have caught each escape;
  - wall time and compute cost;
  - Noah's minutes, including the time spent writing hidden checks.

The output is a ranked list of escape classes. That list sets stage 3's order and can
promote a candidate into it.

### Stage 3: build (several releases; order set by stage 2)

Default order:
1. **An outage a sim can place** (PLAN row, trigger met, "the next oracle-density item"). It
   moves every recorded batch that draws an outage, so it gets its own spec.
2. **Promise packs.** An app's promises live in their own crate, which reaches the app only
   through its public contract (routes, `describe`, outbound effects as the sim records
   them). The verdict names the pack's hash, so a change to the pack is visible on every
   run. Who may change it is the adopter's process. charpente's part is making a change
   impossible to hide.
3. **Snapshot rehearsal.** A sim universe boots from a restored store with its pending work
   (jobs, timed enqueues, Dead rows, intents, sessions, schedules) and runs a candidate
   binary forward under faults. This needs:
   - a way to preload the sim's `Disk` and `ObjectStore` from an archive;
   - a sim clock that starts at the store's recorded time;
   - checks that judge relative to the snapshot's state, since today's app checks assume an
     empty start;
   - an answer for production data in a test rig, which is a privacy question.

   It also covers two changes developed concurrently: the verdict runs on the merged
   candidate.
4. **The upgrade promise.** DECISIONS §15 flips (§7, D1). Data and unfinished work cross
   every release from 1.0. Every release carries an upgrade path, and the sim rehearses it
   on snapshots, using item 3, before the release ships. These PLAN rows fold in: "Gating
   the hand-written platform tables" (0.3.0 R6), and "A clean stop for a launch or serve
   node, or an upgrade-barrier text" (trigger met). This item's spec decides whether a
   replicated pair upgrades one node at a time (`PROTO_VERSION` across 1.x), and weighs the
   deep-fingerprint and commit-time shape-lock rows. The promise starts at 1.0.0 (Noah,
   2026-11-26). My call: the path is rehearsed between two release candidates before 1.0.0
   ships, so the first release that makes the promise has already carried data across a
   release once. Data written by a release candidate is not promised.

Candidates the baseline can promote into this stage:
- a report of the output fields no check reads, built by perturbing values the app sends
  out or returns. This is R4's oracle, it is unproven, and it may not work, because an
  outbound `Call` body is raw bytes;
- rules enforced on every commit, through the state-machine declaration row;
- properties derived from an app's shape (PLAN row);
- mutants that delete a statement (PLAN row).

### Stage 4: the gate (1.0's exit criteria)

1.0 ships when all of these hold:
1. The verdict file carries everything stage 1 lists plus the promise pack's hash, and
   someone outside the project reproduces one verdict from the release artifacts alone.
2. A promise-pack change shows in the verdict, pinned by a test that edits a pack and
   expects the hash to move.
3. Snapshot rehearsal runs a candidate binary from a restored store with pending work, under
   faults, judged relative to the snapshot.
4. A store written by the previous release candidate, holding pending jobs, outbound jobs
   with minted keys, timed enqueues, intents, sessions and schedules, crosses to the next
   one. This is proven in the sim on snapshots and on the edge harness with `kill -9`
   during the upgrade.
5. The full experiment has run: two domains (billing, plus a scarce-allocation one such as
   appointment slots), both arms, about 50 increments, hidden checks, and, partway through,
   a policy change, a provider discrepancy and a charpente upgrade with work pending. Its
   record is published whatever it shows. **1.0 requires charpente to meet the predictions
   it registered before the run** (Noah, 2026-11-26). If it misses, the record is
   published, the work fixes what the record found, and the experiment runs once more. If
   the rerun misses too, the work stops and §2 reopens. 1.0 does not ship on a miss, and the
   experiment does not run a third time.

## 7. Decisions that change

- **D1. DECISIONS §15 (formats change in place).**
  - Until 1.0.0, stored formats keep changing in place, as now. Data written by any 0.x
    release or release candidate does not reach 1.0.0 (Noah, 2026-11-26).
  - From 1.0.0, data that release or any later one wrote crosses every later charpente
    release, along with the app's unfinished work. That covers
    every platform table (sessions, keys, cron rows, jobs, events, intents, schema versions),
    the log, and archive buckets.
  - §15's reason for refusing platform migrations was "a code path that runs once per
    adopter, under no oracle the sim exercises daily". Snapshot rehearsal answers it, because
    the upgrade path then runs in the sim before every release.
  - Its reopen condition is met by decision rather than by an adopter. Noah signed it,
    2026-11-26.
  - Downgrades stay unsupported and refused by name.
- **D2. MANIFESTO §8 ("A compatibility layer").**
  - Split it. APIs still change in place, and the break still arrives as a compile error
    that names the fix. There are still no parallel APIs.
  - Stored data is the exception from 1.0.0. An agent repairs a broken call in seconds, but
    yesterday's customers can't place their orders again.
  - The data promise doesn't depend on the version number.
  - How API breaks are numbered after 1.0 is left out of this pass on purpose. §15's rule
    ("the minor version bumps") stands through 0.x. 1.0's release decides the numbering, by
    whatever aids development without adding bloat (Noah, 2026-11-26: nobody depends on it
    yet). Neither MANIFESTO nor DECISIONS gains versioning text now.
- **D3. MANIFESTO §6 ("The framework is the reviewer").**
  - The compile-time bullet says the type system is "the oracle for … business logic". It
    is the oracle for business logic's shape: a broken contract fails to compile. It cannot
    tell a right value from a wrong one, and a price ten times too high is still a valid
    integer.
  - Add a bullet: promises the builder can't edit. The business states what it promised.
    charpente keeps those checks apart from the code the agent writes, names their version
    in every verdict, and reports what no promise judged.
- **D4. MANIFESTO §9 (the wager).** "Every path … executed, under fault, thousands of times"
  was true of Renewly's pricing path, and nothing judged it. The wager says *judged*, against
  promises the builder could not edit, and adds what nothing judged to what the seed proves.
- **D5. New DECISIONS §16: 1.0 is accepting a change without reading it (2026-11-26).** It
  records §2, §3, §5, the non-goals in §4, and what reopens each. It is the reference the
  other entries point to. The §2 condition is Noah's (2026-11-26). The other two are mine,
  and land in §16 marked "proposed, not Noah's own hand" until he rules on them, the way the
  2026-09-02 annotations were marked:
  - **§2, the goal:** the full experiment misses its registered predictions twice (stage 4,
    criterion 5).
  - **§3, the definition:** stage 2 or 4 finds a class of promise that teams need checked
    and that lacks one of the two properties.
  - **§5, order C:** stage 2 can't be run for its stated cost, for example because hidden
    checks can't be written in a sensible amount of Noah's time. Then the work falls back
    to A, and the gate stays.
- **D6. PLAN's stance after launch.** Items outside 1.0 still wait for their triggers. The
  1.0 work is scheduled by the thesis, the way M0–M3 were, because the triggers it would
  otherwise wait on all name an adopter, and charpente has none.
- **Unchanged:** DECISIONS §6 (one thread), §7, §10, §14, and every hard rule. One log, one
  commit point, no sharding, no SQL-first interface, no porting existing apps, no hosted
  control plane on the roadmap, and the twin parked.

## 8. Document edits (what the plan builds)

All of these are edits to documents, landed as one coherent pass.
- **MANIFESTO.md:** D2, D3 and D4. Keep the claims census green (below).
- **DECISIONS.md:**
  - annotate §15 with D1: dated, Noah-signed, with the evidence;
  - add §16 (D5).
- **PLAN.md:**
  - add a "1.0" milestone section after M3, with the goal, the four stages and the exit
    criteria;
  - leave every row in the trigger-gated table (my call). The milestone section names the
    rows it takes on, and each of those rows gets "**In 1.0, stage N.**" in its trigger
    cell. This is how the table already marks a row "Shipped in 0.5.0". The row history
    stays put, and GUIDE's references to rows by name (F2 around line 1501, the outage row
    around 3993 and 5154, the deep fingerprint around 8140) stay true, so GUIDE needs no
    edit. Rows taken on:
    - the durable-corruption rule and F1 (stage 1);
    - the outage row, R6's platform-table gating, and the clean stop / upgrade barrier
      (stage 3).

    F2, the deep fingerprint, the shape lock and the stage 3 candidates keep their
    triggers, each with "or stage 2 promotes it" added;
  - add the two new rows from §4 (proving app code, replaying production requests);
  - add one sentence to the table's intro, and retitle it if "After launch (v1.x)" no
    longer fits;
  - add two risks: "the thesis loses the experiment" (mitigated by stage 2, published
    either way), and "the data promise taxes every release" (mitigated by the sim
    rehearsing the upgrade path rather than people running it by hand).
- **README.md:**
  - a short "Where it's going" section: the one-line goal, that it is not yet built, and a
    pointer to PLAN's 1.0 section;
  - in Limits, say that until 1.0.0 a release can change charpente's stored formats: a node
    refuses another minor's data directory, peer or archive by name and never converts it,
    while an app's own data across its own deploys is carried. Also say that data written
    before 1.0.0 does not reach it. README says none of this today, and that is an honesty
    gap at any version;
  - repoint the F1/F2 line: F1 is 1.0 work (stage 1), and F2 stays a PLAN row.
- **CLAUDE.md:**
  - "Why this exists" gains a fifth consequence: the verdict can't change without the
    change showing. It can be broken without breaking a test, which is why it belongs on
    that list. It covers apps, and it covers charpente's own oracles, which already work
    this way: `.cargo/mutants.toml` with its pinned count and one rationale per entry, the
    claims census's `EXEMPT_COUNT`, and pinned goldens. Editing them stays allowed, as long
    as the edit is visible, counted and argued, so this pass's own `EXEMPT_COUNT` raise
    fits the rule (my call);
  - "What this is" and "Current state" stop saying the roadmap is complete, and point at
    PLAN's 1.0 section and DECISIONS §16, still carrying no task-level status.
- **LEDGER:** one entry recording the decisions and pointing here.

Out of scope here: the essay on noahmitchem.com, the launch tape's reframe (its own open
item, `docs/LEDGER.md:10007`), and any code.

## 9. Constraints

- `crates/guide-examples/tests/claims_census.rs` scans README.md and MANIFESTO.md. Every
  figure is either quoted from docs/BENCH.md or exempted, with `EXEMPT_COUNT` raised in the
  same commit. "1.0" in README is a new version figure and needs an exemption like
  "0.7.0"'s. MANIFESTO §6's "thousands of seeded universes per night" and §9's "thousands of
  times" are exempt strings: rewording them changes the exemptions.
- `doc_inventory.rs` holds CLAUDE.md's crate map and test lists. The edit doesn't touch
  them, and the gate must still pass.
- README's quoted runs are pinned by `crates/charpente-cli/tests/demo.rs`. Don't touch them.
- docs/BENCH.md is the only source of quotable performance numbers.
- Another session commits on main. Its Drive::recover work landed its ledger entry at
  `e91a9d6e`. Check `git log` before editing, and commit named paths only.
- No employer is named or alluded to. No Co-Authored-By trailers.
- The prose follows the 2026-11-21 root-doc voice. Run a humanizer pass before presenting.
- Verification is docs-only: `cargo fmt --all --check`, and `cargo test -p guide-examples
  --no-fail-fast`, counting `test result: ok` lines.

## 10. Evidence

- **R4, no check reads the charged amount:**
  `docs/experiments/2026-11-0.5.0-docs-only/README.md:150`. `Charge::plan` was named as
  having no viable mutant and was not argued (same file, bar (e)).
- **The planted double charge.** 1 of 1953 tested runs before the builder reshaped the
  provider model and the period ends, then 4 of 256 after (`attempt-2/renewly/FINDINGS.md`,
  around line 190). This is PLAN's "outage a sim can place" row.
- **The rehearsal tie:** 0 violations to 0, 7645 s against 3940 s (`docs/LEDGER.md`,
  2026-11-19). The charpente arm hand-built its routes (F1) and lifted the fence for a
  second server (F2).
- **The corruption excuse** misused in three apps (PLAN row, trigger met).
- **The kernel loop** at about 1.3% of the commit window, and the WAL sync at about 40%
  (docs/BENCH.md, the group-commit attribution).
- **Agents grading their own work:** Noah's essay. Waterloo's verification of the Alpöge
  proof, its acceptance through a fixed statement and independent kernels, and the
  76,000-line rewrite with identical statements (checked 2026-11-26) motivate the verdict
  and the promise pack. They are not evidence about charpente.

## 11. Left to the builder

- The exact prose of every edit in §8, within the content §7 fixes.
- Commit boundaries and order, and the census exemptions the prose needs.
- Whether PLAN's trigger-gated table is retitled, and to what.
- Where in README the "Where it's going" section sits.

## 12. Settled by Noah after the builder check (2026-11-26)

- **Does 1.0 require winning the experiment?** Yes. It is gated on the predictions
  registered before the run, with one rerun allowed, and §2 reopens on a second miss
  (stage 4, criterion 5).
- **Where does the data promise start?** At 1.0.0. Data written by 0.x or by a release
  candidate does not reach it (D1).
- **How are API breaks numbered after 1.0?** Left out for now (D2). Nobody depends on
  charpente yet, so this pass adds no versioning text.
