# 0.2.1 — a sim run a stranger can read

**Date:** 2026-10-17. **Status:** design agreed with Noah in session 2026-10-17; awaiting his
review of this document.

## 1. Why

The launch run (2026-10-10, `docs/experiments/2026-10-launch-run/README.md`, "Testimony" and
"What the run leaves for after launch") left four first-hour candidates, carried to 0.1.1 and
never fixed. All four are still present in 0.2.0 (checked 2026-10-17 against the binary
installed from crates.io):

1. **The sim's summary cannot be read cold.** Today a green run of the scaffolded app prints:

   ```text
   sim: 256 universe(s) per test from master seed 0x994fd0134ccf9f9e
   sim: 168 of 256 universes of "acked_note_survives_a_restart" gave up — reached no verdict
   sim: 166 of 256 universes of "acked_post_implies_the_echo_after_recovery" gave up — reached no verdict
   sim: 256 universe(s) per test, 178 verdict(s), 334 gave up, no failures (master seed: 0x994fd0134ccf9f9e)
   ```

   Both launch-run strangers read it as overstating what was tested, and no output says why a
   universe gave up: the template's `give_up` reasons never reach the user. Noah, reading it on
   2026-10-17: *"I have no idea what 334 of 512 means. This is not a comprehensible thing for
   someone."*
2. **`charpente --version` prints the usage text** (`crates/charpente-cli/src/main.rs`, the
   fallback arm).
3. **README's clippy line assumes the clippy component.** The official Rust image has none;
   one stranger could not add it offline, the other needed `rustup component add clippy`.
4. **README's "the second property goes red"** does not name the property.

Why it matters here: MANIFESTO §3 — an agent succeeds or fails on how fast and precisely the
framework says *no*, and an output nobody can read is not a precise answer; §9 — the wager is
saying "every path … has been executed, under fault, thousands of times", which needs an
output that states honestly how many runs tested something. DESIGN §7: errors are prompts.
PLAN's test for after-launch work — "does it make the reviewer catch more, or a stranger's
first hour shorter" — is met by all four.

## 2. Goals, non-goals, anti-goals

**Goals.** A person who has just run `charpente new` and never opened GUIDE reads
`charpente test --sim` output cold and can say, for each check: did it hold, how many runs
actually tested it, why the rest did not, and how to rerun exactly this batch. The other three
items are fixed. Agents get the same information from `--format json`.

**Non-goals.**
- Changing what the sim tests, its fault rates, or how often runs end early. A high skip rate
  under dense faults is expected (settled 2026-09-07: "the give-ups are the fault rate"); the
  goal is to make it visible and explained, not smaller.
- New thresholds, warnings or health checks on the skip rate. Note what that leaves: an
  app's `sim_tests!` takes `require` only, never `ceiling:` (`charpente-sim/src/lib.rs`'s
  `sim_tests!`; GUIDE §8 says so), so for an app **this output is the only place a check that
  proved nothing shows** — which is why §3's untested status exists.
- The platform's own internal suites' runner line (`charpente-sim [replication]: 38 of 128
  universes reached a verdict, 90 gave up`), `charpente test --mutants` output, and an app
  binary's own argv (`launch`'s `--version`). Stranger-facing CLI output only.
- Renaming anything: the `--universes` flag, GUIDE's term "universe", the `sim_tests!` macro,
  the `universe:gave-up` coverage key, the existing JSON fields.

**Anti-goals** — it fails even if it works when:
- any recorded seed or master-seed batch decides differently (a run that tested before is
  now skipped, or the reverse), or any universe's draws move;
- the new output hides or softens a failure, or makes a red run read green;
- an existing app stops compiling against 0.2.1, or an existing `--format json` consumer
  loses a field;
- the reasons it shows are noise: hundreds of one-off lines, or text that differs between
  two runs of the same batch.

## 3. What the stranger sees (decided with Noah)

Noah's direction: lean on the outputs of comparable CLI tools. The model is `cargo test`
(`test foo ... ok`, then `test result: ok. 2 passed; 0 failed`), with QuickCheck's and
Hypothesis's notion of a generated case that could not test the property (QuickCheck
"discarded", Hypothesis "invalid") reported beside passed and failed. Noah approved this shape
(counts per reason illustrative):

```text
running 2 sim checks, 256 runs each, under random crashes and disk faults

check acked_note_survives_a_restart ................ ok   (88 tested, 168 skipped)
check acked_post_implies_the_echo_after_recovery ... ok   (90 tested, 166 skipped)

skipped runs hit a fault before reaching the check, so they prove nothing either way:
  acked_note_survives_a_restart
    120  the POST was never acked, so durability has nothing to prove
     31  App::open hit fault fallout
     17  the kernel loop hit fault fallout
  acked_post_implies_the_echo_after_recovery
    ...

sim result: ok. 2 passed; 0 failed. 178 of 512 runs tested a check.
rerun exactly this batch: charpente test --sim --master-seed 0x994fd0134ccf9f9e
```

What is fixed by that approval:
- **Vocabulary in human output:** "run" for a universe, "tested" for a verdict, "skipped" for
  a give-up, "check" for a sim test, a rerun *command* instead of a bare master seed.
- **Layout:** a header line saying what ran; one line per check in `cargo test`'s shape with
  its tested/skipped counts; the skipped runs' reasons grouped per check with counts; a
  `cargo test`-shaped result line; the rerun command last.
- **The failure block is unchanged** (seed, config, fired faults, repro command). Both
  strangers read it correctly. It stays before the summary as today.

Filled in after the builder check (controller's calls, each following from §2's anti-goals
and the approved shape; marked so Noah can check them):
- **"Tested" means passed + failed.** A failed run tested the check. In the code `verdicts`
  excludes failures (verdicts + gave_up + failures = universes), so the human "tested" is
  verdicts + failures, and a check line shows its failed count when it has any:
  `check X ... FAILED (90 tested: 78 failed; 166 skipped)`. The JSON adds a per-check
  `failed` count beside the unchanged `verdicts` and `gave_up`.
- **A check that tested nothing is `untested`, never `ok`** — cargo's `ignored` is the
  precedent for a third status. The result line counts it (`1 passed; 0 failed; 1 untested`),
  and when every selected check is untested the result line does not begin `sim result: ok`.
  This keeps today's pinned rule that a batch that adjudicated nothing never reads as an
  unqualified success (`harness.rs` `write_human`; cli.rs
  `a_batch_where_every_universe_gave_up_is_not_an_unqualified_success`). Exit code unchanged.
- **A check with an unmet `require` key is `FAILED`**, its line naming the requirement, since
  the run exits 1 for it; it never prints `ok` beside `0 failed`.
- **The header goes to stderr at startup and carries the master seed**, replacing today's
  `sim: N universe(s) per test from master seed …` line: DESIGN §7 promises the seed is
  printed at startup so a killed batch still shows it, and `--format json` keeps stdout pure.
  The rerun command still closes the human output.
- **A single-seed replay** (`--seed`) speaks the same plain language, and for a skipped run it
  prints that run's reason with its per-run detail. It never calls a check "vacuous".
- **`--format json`** carries the same reason counts, so an agent sees what a person sees.

## 4. Constraints

- **Determinism, oracle 3's sense.** Rendering only. Nothing that draws from a seed's streams
  changes, so every pinned seed and recorded master-seed batch decides exactly as before:
  same failures (test, seed, message), same per-test tested/skipped counts, same coverage.
  The human text changes by design; the JSON keeps every existing field and value.
- **The output is deterministic and independent of `--jobs`.** Reasons are ordered by count
  descending, then by text; checks in registry order, as today.
- **Exit codes unchanged:** 0 green, 1 failures or unmet coverage, 2 usage/build/infrastructure.
- **Semver-compatible 0.2.1** for all six crates. Adding a field to an existing public struct
  with all-public fields (`harness::Report`, `harness::TestOutcomes`) or changing a public
  signature (`Sim::give_up`) is a breaking change under Rust's semver rules, so new data rides
  in new types or new methods. `cargo-semver-checks` against the published 0.2.0 is the oracle
  (a pinned version, installed for the check the way `cargo-mutants` is; not a workspace
  dependency).
- **Hard rule 5:** the new text is product surface — every new line is pinned by a test
  against its exact output, including the all-skipped case, a red run, unmet coverage, and a
  single-seed replay of a skipped run.
- **Hard rule 6:** docs land with the code — GUIDE §8's "Giving up" section (its sample batch
  lines and the all-gave-up sentence), the CLI usage text, README's "Try it", and a 0.2.1
  entry in `CHANGELOG.md`; the workspace and every install-facing version go to 0.2.1 the way
  0.2.0's bump did (`37616d8f`).
- **The scaffold requires the CLI's own full version** (controller's call, from the builder
  check). `charpente new` today writes `major.minor` (`new.rs` `dependency_version`, so
  `"0.2"`), which admits 0.2.0. Once the template uses §5 A's new method it needs 0.2.1, and
  `"0.2"` would fail to compile wherever 0.2.0 is already locked or a mirror lags — a
  first-hour failure in a release meant to remove them. So the requirement becomes the full
  version (`charpente = "0.2.1"`), GUIDE §1's manifest block follows it byte for byte (its
  test holds that), and each release's bump updates it, as minors already did.
- **Hard rule 1** applies unchanged: no clocks or ambient IO enter the harness's accounting.

## 5. The parts

**A. Stable reasons (my call).** An app's reason often embeds error text that differs on
every run (`format!("App::open hit fault fallout: {e}")` in the scaffold, and similar in
`charpente_sim::checks`). Grouped as written, a batch prints one line per distinct error.
So a give-up gets a stable reason that the output groups and counts, and an optional per-run
detail that only a single-seed replay (and the JSON's per-run data, if any) shows. The form is
additive: `Sim::give_up(reason)` keeps compiling and groups by its exact text; a new method
takes the stable reason and the detail separately. The scaffold
(`crates/charpente-cli/templates/sim.rs.tmpl`), GUIDE's examples, `charpente_sim::checks`,
`crates/sim-fixture` and `examples/invoicing` move to the new form, so the outputs a stranger
or a docs-only agent meets are grouped well from day one.

**B. Reasons on screen (my call).** Per check, the most common reasons with counts, capped at
a small number of lines, then one line naming how many other distinct reasons there were and
where to see them all (`--format json`). The all-skipped case (today's "every universe … gave
up … vacuous" line) becomes a plain sentence under that check: it was never tested, and why.

**C. JSON (my call).** Additive only: per check, the skipped reasons and their counts, in the
same order as the human output. `verdicts`, `gave_up`, `outcomes` and every other field keep
their names and values.

**D. `charpente --version` and `-V`** print `charpente <version>` to stdout and exit 0, as
`cargo --version` does. Other unknown arguments keep printing usage, exit code unchanged.

**E. README "Try it".**
- The clippy line says what to do when the component is missing: `rustup component add clippy`.
- "the second property goes red" names it, `acked_post_implies_the_echo_after_recovery`, and
  says what the stranger will see go red in the new output.
- README is held by `claims_census` (digits must be sourced or absent) and GUIDE's compile
  gate; both stay green.

## 6. How we know it worked

1. **Text pins** (hard rule 5): golden tests of the new human output for a green batch, a red
   batch, unmet coverage, the all-skipped case, more distinct reasons than the cap, and a
   single-seed replay of a skipped and of a failing run; JSON tests for the new fields and for
   every old field unchanged.
2. **Oracle 3:** every recorded master-seed batch (`sim-fixture`, `invoicing`,
   `outbound-fixture`, the scaffold) and every pinned seed decides identically — compared on
   failures, per-test counts and coverage, not on human text.
3. **Semver:** `cargo-semver-checks` against crates.io's 0.2.0 reports no breaking change for
   any of the six crates.
4. **The gate:** fmt, clippy with the deny-list, `cargo test --workspace --no-fail-fast`
   counted by `test result: ok`; CI green.
5. **A stranger reads it.** Re-run the launch run's rehearsal with the launch-run tooling
   (`docs/experiments/2026-10-launch-run/tools/`: the packaged 0.2.1 crates through
   `make-registry.sh`, a fresh box, the same `BRIEF.md` byte for byte, a headless Opus agent
   with only the README). At 0.1.0 both strangers' numbers were right, and both reported the
   output as overstating what was tested ("only 181 of 512 runs actually tested anything";
   "'256 universes' overstates how much was actually tested"); that complaint is the
   signature being removed. Written down before the run (controller's call, for Noah to check):
   - `BRIEF.md` stays byte-identical, so the old and new outputs meet the same prompt. After
     the stranger's closing report, the same session gets one neutral follow-up, recorded
     verbatim: "In two or three sentences, what did the last sim run you did tell you?"
   - **Pass** when, across the closing report and that answer: (a) for each check the
     stranger says whether it held and how many runs tested it, correctly; (b) it gives at
     least the most common reason runs were skipped, correctly; (c) nothing it says calls the
     output overstated, misleading or unclear; (d) every claim about charpente it makes is
     right, censused the way the launch run's were.
   - Recorded as a dated directory under `docs/experiments/`. A miss is a finding about the
     output: fix it and run a new attempt, recording both; it is never argued away.
6. **After the publish** (Noah's hand), the registry smoke: install from crates.io, `new`,
   clippy, sim — as 0.2.0's was on 2026-10-17.

## 7. Left to the builder

The new method's name and signature; the cap on reason lines; exact wording of the header,
result line and all-skipped sentence within §3's shape; how the per-run detail is carried;
the JSON field names; whether the coverage-requirement failure text is reworded into the same
vocabulary (it must keep naming the `--universes` flag it tells the user to raise); task
order.

## 8. Decisions and who made them

- **Noah:** the output must be readable by someone who never read GUIDE; lean on comparable
  CLI tools' outputs; the §3 layout ("looks good to me"); the scope and the stranger re-run
  as acceptance ("let's do it", after the playback that proposed it).
- **Controller (Claude), revisable:** stable reason + per-run detail as an additive API (§5 A);
  capped reason lines (§5 B); JSON additive only (§5 C); keep `--universes` and GUIDE's
  "universe"; 0.2.1 rather than 0.3.0, with semver-checks as the oracle; `--version` modelled
  on `cargo --version`; internal suites and `--mutants` out of scope. From the builder check:
  "tested" = passed + failed; the `untested` status; unmet `require` prints FAILED; the
  stderr startup header with the seed; the scaffold requiring the full version; the
  stranger re-run's written pass bar and its one follow-up question.
