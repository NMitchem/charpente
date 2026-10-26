# T22 review: the 0.3.0 docs-only build, attempt 1 (pricewatch)

Reviewer: opus subagent, 2026-10-26. I read the platform at `1106cdf0` through `git show`; the registry's `charpente-sim-0.3.0/src/harness.rs`, `charpente-cli-0.3.0/src/main.rs` and `charpente-0.3.0/GUIDE.md` are byte-identical to it. I read the app from `docs/experiments/2026-10-0.3.0-docs-only/attempt-1/pricewatch/`. All runs were made in the scratch copy `…/scratchpad/t22rev/pricewatch`, with the rehearsal registry, `RUSTUP_TOOLCHAIN=1.99.0`, `--offline --locked`, its own target dir, and a perl alarm. The logs are in `…/scratchpad/t22rev/logs/`.

## Plan

1. Read BRIEF, report.md and FINDINGS.md, then `pricewatch/src/{lib,merge,main}.rs` and `tests/sim.rs`.
2. Section 1: walk each behaviour against the code: acked durability, order-independence, dedup, tie-break, amount refusal and credential reach. Build a concrete input sequence for anything broken.
3. Section 2: copy the app, wire the registry, run the baseline `--runs 64 --master-seed 0x1`, then the five required hand mutations and the ones the code suggests. Print each one's behaviour so its effect is proven.
4. Section 3: read harness.rs (coverage/require counting, skipped runs), the mutants verb (unviable, timeout) and GUIDE §5 and §8 at 1106cdf0. Reproduce (a) in the copy.
5. Section 4: scan the app against the deny-list and the transaction rules.
6. Verdict.

Status: **complete.**

## CONTROLLER ALERT (found 2026-10-26 ~17:05, before any finding below)

Your gate script's CLI install failed, so its first `--mutants` run did NOT use the 0.3.0 verb:

- `logs/t22-install.log` reads ``error: could not find `charpente-cli` in registry `crates-io` with version `=0.3.0` ``. `logs/t22-gates.progress` reads `install exit 101`, then `charpente: /Users/noahmitchem/.cargo/bin/charpente`.
- That binary is `charpente-cli 0.1.0 (path+file:///…/charpente/crates/charpente-cli)`, installed Oct 14 (`~/.cargo/.crates2.json`), and its usage still prints `--universes`. The gate's `mutants exit 0` / `82 total — 59 caught, 0 missed, 23 unviable, 0 timed out` came from that 0.1.0 verb, so it does not settle bar condition 3. cargo-mutants itself was the same tool, so the mutant set and the unviable list are unaffected.
- `SIM REPLAY DIFFERS` in the progress log is a false red, produced by the script itself. The `running 5 sim checks…` line goes to **stderr** (harness.rs:1044, `eprintln!("{}", startup_line(…))`), while the script sent only stdout to `t22-sim-replay.log`. The awk range `/^running 5 sim checks/,/^rerun exactly/` over stdout never opens, so the diff is `1,28d0`. I checked the replay myself: the pristine copy run at `--master-seed 0x2c0cbfefeb3ad9c2` gives stdout byte-identical to `t22-builder-batch.txt` minus its first (stderr) line (`cmp` silent). It reads `1018 of 1280 runs tested a check`, as the builder's batch does.
- **Update 17:08.** You have now installed `t22gate/cli/bin/charpente`, and it prints `charpente 0.3.0`. I never ran a bare `charpente`. Before your CLI existed, I ran M1-M4 by doing what the 0.3.0 CLI does (`charpente-cli/src/sim.rs`:123-132 and 54): `cargo test --test sim --no-run`, then exec of the built binary with the flags unchanged. Run through your 0.3.0 CLI on the pristine copy, `charpente test --sim --runs 64 --master-seed 0x1` writes stdout byte-identical to my direct-binary baseline (`cmp` silent). So M1-M4 stand, and every later run used the 0.3.0 CLI by full path.

---

## Section 1: correctness against the brief. Verdict: no Critical, no Important

I walked each behaviour through `src/lib.rs` and `src/merge.rs`:

- **An acknowledged report survives crashes.** `submit_report` writes the `Report` row and the `Prices` row in one handler transaction (lib.rs:277-297), so both commit as one record. Durability after the ack is the platform's (GUIDE §5 "Ack-after-durable"). I found no path that acks before the write: the dedup early return (lib.rs:271-276) acks only a row that is already durable.
- **Order-independence.** `lowest` and `latest` are `min_by_key` over a rank key. `lowest_rank = (amount, observed, source)` (merge.rs:39-41). `latest_rank = (-observed as i128, source, amount)` (merge.rs:46-48). A report's identity is (product, source, observed, amount) (merge.rs:63-65). Within one product, two distinct reports therefore differ in at least one of the three ranked fields, so each key is a strict total order over distinct reports. The minimum of a total order is a commutative, associative and idempotent fold, which makes it independent of arrival order. `-i128::from(u64)` cannot overflow.
- **Redelivery counts once.** The `Report::by_key` index lookup happens before any write (lib.rs:270-276). A redelivery writes nothing, and even if it did, folding a duplicate into a min would be idempotent. `report_key` is injective: the four fields are numbers joined with `/`, and the amount is already positive.
- **Tie-break.** `latest_rank`'s second field is `obs.source`, ascending, so the smaller source id wins at equal time. That matches the brief. For a tie on both time and source, the smaller amount wins, which is the builder's own choice where the brief is silent (FINDINGS.md:66-70).
- **Bad amount.** `check_amount` runs `> 0` (merge.rs:52). The refusal is `amount_cents must be a whole number of cents, 1 or more — got N` (400), which names the field and what a valid amount is. It runs before any write.
- **Credential reach.** `register_product`, `register_source` and `list_source_reports` call `require_operator` (subject `== "operator"`). `submit_report` requires a `source:<u64>` subject whose `Source` row exists (lib.rs:202-214). Only two routes mint keys: `register_source`, which is operator-only, and `mint_operator_key`, which is secret-gated. So no request body can name a subject. An operator key cannot submit (`strip_prefix("source:")` fails), and a source key cannot reach an operator route.

Minor findings (app-side, brief-silent):

- **S1-m1. The operator-claim door has no hardening.** `POST /operator/key` is `public` and compares the secret with a short-circuiting `*secret == body.secret` (lib.rs:223). Nothing throttles it, so it is a timing and brute-force surface on the one route that buys root. A wrong secret also gets `ApiError::UnrecognizedCredential` (lib.rs:224), whose fixed text says `it may have been revoked; obtain a new one`, which misnames the cause. Scenario: an attacker scripts unbounded guesses against `POST /operator/key`, and every one is answered at full speed.
- **S1-m2. Listing an unknown source answers like an empty one.** `GET /sources/:id/reports` for a source that was never registered answers `200 []` (lib.rs:336-340), so it cannot be told apart from a registered source with no reports. Scenario: an operator mistypes `/sources/7/reports` and reads "no reports".
- **S1-m3. A source key can never be revoked.** No route revokes one. Re-registering a source mints another key and keeps the old one live (lib.rs:250-257), so a leaked source key keeps submitting reports forever. The brief does not ask for revocation.

No Critical or Important, so no platform-oracle attribution is needed.

## Section 2: the proof's strength. Verdict: strong; the suite is two-sided, with one Minor

Baseline on the pristine copy, `charpente test --sim --runs 64 --master-seed 0x1`: exit 0, `sim result: ok. 5 passed; 0 failed. 256 of 320 runs tested a check.` Tested runs per check: acked 43, kept 52, redelivered 51, bad_amount 54, credentials 56.

The oracles never call the app's merge. `grep -n merge tests/sim.rs` matches only the doc comment at line 9. `oracle_lowest` and `oracle_latest` (sim.rs:90-133) are independent folds over explicit comparisons. From the app, they import only its table and wire types, `build_app`, the two refusal constants and `configure_operator_secret` (sim.rs:21-24). Where the brief is silent, the oracle encodes the app's own documented choices (lowest ties, same-source same-instant latest), and M6-M8 below show those choices are pinned too.

Method. `t22rev/mutate.zsh` applies a literal substitution that must match exactly once. Then it runs `tests/probe.rs`, a reviewer-only test that calls `pricewatch::merge` directly and prints the behaviour, so every mutation is shown to have taken effect, red or green. Next it runs the batch: M1-M4 through the direct binary, M5 onward through the 0.3.0 CLI. Last, it restores the file with `cp` and `touch` and checks the result with `cmp` (merge.rs sha 49c998163d4c, lib.rs 87cb7e24122f). The sim binary's mtime is printed on every run, and it changed each time, so every mutation was rebuilt.

| # | Mutation | Effect shown (probe or reply) | Batch, 64 runs, 0x1 | Verdict |
|---|---|---|---|---|
| M1 | latest tie-break → larger source (`u64::MAX - obs.source` in `latest_rank`) | probe `latest tie t=2 … (want src1): src=2`, in both arrival orders | exit 1; `kept_values_ignore_arrival_order … FAILED (52 tested: 13 failed)`. Shrunk 43 draws → 13, to two reports at t=1 from sources 1 and 2 | CAUGHT |
| M2 | latest by arrival (`latest` returns `incoming`) | `latest kept t=3 + in t=1 (want t=3): t=1` | exit 1; kept 29/52 failed | CAUGHT |
| M3 | lowest keeps the first report seen (`lowest` returns `kept`) | `lowest kept 5 + in 3 (want 3): amt=5` | exit 1; kept 34/52 failed | CAUGHT |
| M4 | dedup keyed on (product, source) (`report_key` → `"{product}/{source}"`) | `report_key(1,2,3,4)=1/2 report_key(1,2,5,6)=1/2` | exit 1; acked 23/43, kept 18/52, redelivered 27/50 failed | CAUGHT ×3 |
| M5 | amount 0 accepted (`>= 0`) | `check_amount(0) = Ok(0)` | exit 1; acked 12/44, kept 52/52, redelivered 51/51, bad_amount 53/53 failed. The message is `Spec { … amount: 0 … } has a bad amount but was recorded` | CAUGHT ×4 |
| M6 | latest same-source same-instant → larger amount (`-obs.amount_cents`) | `latest same src+t, kept amt5 + in amt3 (want amt3): amt=5` | exit 1; kept 9/52 failed | CAUGHT |
| M7 | lowest equal-amount tie → later observation | `lowest equal amt, kept t=2 + in t=1 (want t=1): t=2` | exit 1; kept 9/52 failed | CAUGHT |
| M8 | lowest equal-amount, equal-time tie → larger source | `lowest equal amt+t … (want src1): src=2` | exit 1; kept 2/52 failed | CAUGHT, rate low but it fires at 64 runs |
| M9 | `list_source_reports` without `require_operator` (statement deleted) | reply `a source listing a source's reports answered 200 [], not 400` | exit 1; credentials 56/56 failed | CAUGHT |
| M11 | redelivery ack says `duplicate: false` | reply `answered 200 {"duplicate":false,"report":"0"}, not {"duplicate":true,…}` | exit 1; redelivered 48/51 failed | CAUGHT |
| M13 | operator claim no longer revokes (delete statement replaced) | reply `the operator's key after a rotation answered 200 [], not 401` | exit 1; credentials 56/56 failed | CAUGHT |
| M16 | `submit_report` skips the product-exists check | reply: a report for product 99 recorded (`{Spec { product: 99 … }} are recorded`) | exit 1; redelivered 51/51, credentials 56/56 failed | CAUGHT |
| M18 | `register_source` does not write the `Source` row (statement deleted) | reply `a report of -1 cents answered 400 {"error":"only a source's API key may submit a report"}` | exit 1; every check FAILED, and acked `0 tested, 64 skipped; required "pricewatch:acked" never occurred`. The require catches the vacuity | CAUGHT ×5 |

**Every hand mutation was caught.** That includes the five the dispatch asked for and three that cargo-mutants cannot generate (M9, M13, M18: statement deletions). The properties compare exact values against brief-restating oracles, so the suite is two-sided.

**S2-m1 (Minor): two properties are one-sided on their accept side. Their siblings close the gap, so the suite is not.** `a_bad_amount_is_refused_by_name` judges only bad amounts (sim.rs:857-896) and never asserts that a valid amount is accepted. `only_the_right_credentials_reach_each_route` ignores every `Redeliver` reply (sim.rs:948), so a source being refused is invisible to it. Proof, mutation M23, `check_amount` with `> 1` (probe: `check_amount(1) = Err("… got 1")`):
- `--filter a_bad_amount_is_refused_by_name` → exit 0, `check a_bad_amount_is_refused_by_name ... ok (46 tested over 46 distinct inputs, 18 skipped)`. Green.
- The full suite → exit 1, with `kept_values… FAILED (52 tested: 18 failed)` and `a_redelivered_report_counts_once … FAILED (51 tested: 20 failed)`.
Under `--property a_bad_amount_is_refused_by_name`, an over-refusal mutant would read MISSED. No cargo-mutants operator mutant of `>` produces this (each of `>=`, `<` and `==` is caught by the property itself), and the builder's run judged with every property, so this is not a bar issue. Fix: have each property assert its own accept side. For example, the bad-amount property can check that the tail's valid redeliveries answered 200.

---

## Section 3: triage of the builder's findings

### (a) Coverage in failed runs. **Platform defect, Important.** Reproduced.

Code at 1106cdf0, `crates/charpente-sim/src/harness.rs`:
- `run_batch`, lines 1085-1111. Each run's coverage is merged into `covered` only in the `Ok(end)` arm (`merge(&mut covered, &end.coverage)`, line 1091), which covers both passed runs and give-ups. The `Err(failed)` arm (line 1093) records the failure and never merges `failed.coverage`. Line 1107 then pushes `missing_coverage` when `covered.get(key) == 0`.
- The struct comment at lines 226-231 says this is deliberate: failure coverage is "Kept per-failure rather than folded into `Report::coverage` (which stays the batch statistic that `require:`/floors are measured against".
- `write_human`, line 1457. The sentence is `"{key}" never occurred in {} runs — raise --runs, adjust the workload, or drop the requirement`, with `{}` = `report.universes`, **every** run of the batch, the failed ones included.

So the count covers passed and skipped runs only, and the sentence claims all runs. When failing runs reached the key, the sentence is false, and its advice is wrong: "drop the requirement" deletes an oracle. GUIDE agrees with the sentence and not with the code. GUIDE:2908 says a key "must be hit at least once across that test's runs", and GUIDE:4504-4505 says `never occurred in <n> runs`. Neither says failed runs are excluded. One correction to the builder: it is not "only passing runs". Skipped runs count too.

Reproduction in my copy, with M5 (`check_amount` with `>= 0`; `scenario()` marks `pricewatch:judged` at sim.rs:664 before any check judges anything):

```text
check kept_values_ignore_arrival_order .............. FAILED  (52 tested over 52 distinct inputs: 52 failed; 12 skipped; required "pricewatch:judged", "pricewatch:tie" never occurred)
check a_redelivered_report_counts_once .............. FAILED  (51 tested over 51 distinct inputs: 51 failed; 13 skipped; required "pricewatch:judged", "pricewatch:duplicate-acked" never occurred)
sim coverage requirement failed in kept_values_ignore_arrival_order: "pricewatch:judged" never occurred in 64 runs — raise --runs, adjust the workload, or drop the requirement (a required state that never occurs is an untested claim)
```

The same batch's own failure block shows `fired:  crash:dropped: 1, pricewatch:judged: 1, restart: 2`. The same batch in `--format json` (`logs/m5-json.log`) shows:
- `kept_values_ignore_arrival_order: 52 failing runs, 52 of them fired pricewatch:judged`;
- `a_redelivered_report_counts_once: 51 failing runs, 51 of them fired pricewatch:judged`;
- a top-level `coverage["pricewatch:judged"] = 90`, while `missing_coverage` lists `pricewatch:judged` for those two checks.

M13 and M9 reproduce it too: `only_the_right_credentials… (56 tested: 56 failed; …; required "pricewatch:judged" never occurred)`. Fix, either of two. Count failed runs' coverage toward `require`, since the state did occur and a failure tested it. Or keep the exclusion and make the sentence true: `"pricewatch:judged" never occurred in the 12 runs that passed or were skipped (52 failing runs reached it)`, and drop the "drop the requirement" advice when failing runs hit the key. In both cases, GUIDE:2908 and GUIDE:4504 must say which runs count.

### (b) "0 missed" with a rule that had no viable mutant. **Platform defect, Important.** It is wider than `Reverse`.

- **The verb.** `crates/charpente-cli/src/mutants.rs` at 1106cdf0, `verdict()` (lines 504-578), prints one totals line, `mutants: {total} total — {caught} caught, {missed} missed, {unviable} unviable, {timeout} timed out`. It sets exit code 1 only for `missed > 0` or `timeout > 0`, and it never reads per-function outcomes. The module contract (lines 1-35) says the verb exists to turn every "absent measurement wearing the shape of a passing one" into a loud failure. A function whose every mutant is unviable is exactly that shape, at function grain, and it exits 0.
- **GUIDE §8, line 4873:** "`unviable` means the mutant did not compile — your types rejected it, which is the compile-time contract doing its job." In this run that is false. The unviable mutants are cargo-mutants failing to construct a replacement value (`Reverse::from_iter([0])`, `Default::default()` for a type with no `Default`, `Id::new()`), not a plausible bug the types rejected. GUIDE:4811 sells the instrument as answering "if this code did nothing, would anything go red?" For these functions it answered nothing.
- **The extent.** I read the unviable list (23 lines) from the controller's `t22gate/pricewatch/target/charpente-mutants/mutants.out/unviable.txt`; it matches the builder's final run. Of the functions under test, **13 have no viable body mutant at all**:
  - in merge.rs, `lowest` and `latest`, the merge folds themselves (each one mutant, `Default::default()` for `Obs`, which derives only `Clone, Copy, Debug, PartialEq, Eq`, merge.rs:13);
  - in lib.rs, `submit_report`, the handler that applies the merge (one mutant, `Ok(Default::default())` for `ReportAck`), plus `read_product`, `register_product`, `source_of`, `next_report_id`, `Prices::{lowest,latest,from_parts}`, both `From` impls, and `build_app`.
  Before the builder rewrote it, `latest_rank` had 42 such mutants (transcript: `replace latest_rank -> (Reverse<u64>, u64, i64) with (Reverse::from_iter([0]), 0, -1)` …).
- In this app the gap was closed by hand: the builder's H1-H5 and my M2/M3 cover `submit_report`, `lowest` and `latest`. A builder who trusted "0 missed" would ship a merge whose fold had never been mutated.

Fix:
- The verb lists every function in which no mutant was viable, by name: "N functions had no viable mutant, so this run says nothing about them: …". It should treat that as an unresolved result in the same family as TIMEOUT, as the sim's `untested` status does, or at least print it beside the summary.
- GUIDE:4873 is rewritten. An unviable mutant can be the tool's limitation rather than the types', and a function with only unviable mutants is untested.

### (c) Auto-timeout vs shrinking. **Platform defect (fails safe), Important.**

- **The verb.** In mutants.rs `run()`, lines 253-292, the verb derives `--timeout` only when `baseline_differs`. That is false for `--file src/*.rs --test-package pricewatch`, the app case (lines 346-353), so cargo-mutants' own auto-timeout applies: 5× its green baseline, 26 s for a 5 s baseline. When the verb does derive a timeout, `time_the_package_set` (lines 395-430) also times a green run. `grep -n shrink mutants.rs` matches nothing. Neither path accounts for the shrink a caught mutant triggers, and nothing tells the sim harness that it is running under mutation.
- **GUIDE.** §8:4675 says the shrink budget "is 500 replays, counted rather than timed", and §8:4878-4893 covers the timeout only as a package-scope matter. The two passages are never connected.
- **The shrink.** It runs on each failing check's first failure, sequentially within a check, with checks in parallel (`run_batch` → `in_parallel(&firsts, …)`). Measured on this machine at 256 runs, master seed `0x2c0cbfefeb3ad9c2`:
  - green batch: wall 19.5 s, user 53.7 s;
  - M1 (one failing check, `in 188 replays`): wall 24.5 s, user 59.1 s, so about 27 ms per replay;
  - M5 (four failing checks, 12 to 42 replays each): wall 28.8 s.
- **Worst case.** For one check: 500 serial replays × the per-run time t. The green batch is R·C·t/P, with R = 256 runs, C = 5 checks and P cores. The shrink-to-baseline ratio is therefore 500·P/(R·C) = 0.39·P, which is **4.7× on the builder's 12-core box**. A caught mutant then needs about (1 + 4.7)×, roughly 5.7× its baseline, even with no contention, against a 5× timeout. Under cargo-mutants `--jobs 4` the batch itself also slows about 3-4× (12 cores shared four ways), which gives about 8-9× the baseline, 40-45 s against 26 s. That matches the builder's `TIMEOUT … lowest_rank … (0, 0, 1) in 3s build + 26s test`, and it is why `--timeout 300` was needed.
- **Why it is not a false green.** A TIMEOUT is reported as unresolved and exits 1 (mutants.rs:565-574). But the default configuration cannot produce a clean run for a sim-tested app on a many-core machine, and that is the instrument's target use. The bar's "no hand mutation pass" then needs a hand-picked `--timeout`.

Fix, preferred: under `--mutants`, run the sim with shrinking off, since a mutant needs only red or green. For example, the verb passes a harness flag or env through `--cargo-test-arg`. Otherwise, derive the timeout as baseline + 500 × per-run time. Either way, GUIDE §8 states the interaction.

### (d) Skipped runs' seeds. **Doc gap that needs a product change to close. Minor.**

- **Human output:** reasons and counts only (`write_human`, harness.rs:1463-1505).
- **JSON:** `skipped_reasons` is `{reason, count}`, with `detail` only under `--seed` (harness.rs:1262-1274). Internally `CheckTally.reasons` is a `BTreeMap<&'static str, u64>` (harness.rs:834), so a skipped run's seed is discarded at tally time, and no output can show it.
- **GUIDE:3155-3158:** the detail is "that one run's own text, which a batch does not print and `--seed` prints under the skipped check as `detail:`". GUIDE tells the reader to use `--seed`, but no surface gives the seed. The builder had to search seeds by hand (FINDINGS.md:96-98).

Fix: `skipped_reasons[]` carries the seeds, or at least the first seed per reason. The human block prints one example per reason, `(e.g. --seed 0x…)`.

### (e) The first operator credential. **Doc gap, Minor. The builder's bootstrap is sound and keeps the platform's rules.**

- **What GUIDE §5 offers.** `ctx.create_api_key(tx, subject)` takes any subject (GUIDE:985). A comment says "The first credential comes from something the app trusts, such as an IdP or a signup route that mints a key for a subject it has just created" (GUIDE:1072-1074). OIDC with a static JWKS is the IdP path (GUIDE:1135-1188). So minting for another subject is sanctioned in prose. What is missing is a worked example, both of that and of a first operator credential for a deployment without an IdP. No CLI verb or config field mints one.
- **The builder's design.**
  - `main` reads `PRICEWATCH_OPERATOR_SECRET` into a set-once `OnceLock` (main.rs:4-6, lib.rs:29-44), outside any transaction.
  - `POST /operator/key` compares the secret, deletes every `ApiKey` whose subject is `operator`, and mints one key (lib.rs:220-232). That is one mint per request, as GUIDE:996-999 requires, and a revoke by row delete, as GUIDE:987-990 describes.
  - There is no network await. The handler's effect is its writes, which the log carries, so a follower replays them regardless of its own secret.
  - In the sim, every universe sets the same constant, so seeds replay byte-identically: the builder's final batch reproduced byte for byte on another machine, and `--jobs 1` equals `--jobs N`.
  - The nearest platform precedent is outbound hosts' credentials: an env var read once at start, never in the log (GUIDE:2066-2070).
- **Weak points.** These are app-side and listed in S1-m1: no throttle and a short-circuit compare. GUIDE also gives no way to hand deploy-time configuration to a route, so a process `static` is the only option.

Fix: a GUIDE §5 example of an operator bootstrap, through OIDC or a deploy-time secret, and of an operator minting a key for another subject.

### Triage table: every other entry

| Builder's finding | Class | Evidence at 1106cdf0 | Suggested fix |
|---|---|---|---|
| (a) `require` key "never occurred" though failing runs reached it | **platform defect** (Important) | harness.rs:1091/1093/1107 count only passed and skipped runs; :1457 prints `never occurred in {universes} runs`; :226-231 say the exclusion is deliberate; GUIDE:2908, :4504. Reproduced: 52 of 52 failing runs fired the key | Count failed runs, or say "in the N runs that passed or were skipped (M failing runs reached it)" and drop the "drop the requirement" advice; GUIDE states it |
| (b) "0 missed" hid a rule with no viable mutant | **platform defect** (Important) | mutants.rs:504-578 totals only, exit 0; GUIDE:4873 says unviable is "the compile-time contract doing its job"; 13 functions had only unviable mutants, including `lowest`, `latest`, `submit_report` | Verb names every function with no viable mutant as unresolved; GUIDE rewritten |
| (c) Auto-timeout too short because a caught mutant shrinks | **platform defect, fails safe** (Important) | mutants.rs:253-292 and 395-430 time only a green run, no shrink term; GUIDE:4675 vs 4878; worst case about 5.7× baseline at P=12 against a 5× timeout, more under `--jobs` | Disable shrinking under `--mutants`, or add 500 × per-run time; GUIDE connects them |
| (d) A batch never prints a skipped run's seed | **doc gap, needs a product change** (Minor) | harness.rs:834 keeps counts only, :1262-1274; GUIDE:3155-3158 | One seed per reason in human and JSON output |
| (e) No first credential, no key for another subject | **doc gap** (Minor) | GUIDE:985, :1072-1074, :1135-1188 | Worked bootstrap example in §5 |
| "cargo-mutants never deletes statements" (hand-deleted H1-H5) | **doc gap** (Minor) | GUIDE:4811 "if this code did nothing, would anything go red?" overstates it: body replacement of a handler returning an app type is unviable (`submit_report`'s only mutant), and statements are never deleted. Builder's H1-H5 and my M9/M13/M18 were needed | GUIDE says what cargo-mutants generates and that a handler's writes need a hand deletion or a constructible return type |
| §5 re-read: minting for another subject | doc gap, folded into (e) | GUIDE:1072-1074 prose only | (e) |
| §16 "Where the guarantee stops": mint an `ApiKey` row from operator code | not an issue | GUIDE:952-961 document the row and that only the hash is stored; a request is the documented mint path, and writing auth rows outside one is not offered, by design | none |
| §8 "Giving up" / checkers: may `durable_corruption_fired` excuse a lost *setup* step? | not an issue | GUIDE:3546-3552: "Route an acked-durability check's *shortfall* through this", a general rule the builder applied correctly | Optional: one sentence naming setup's acked writes |
| §8 "Driving the whole app": can a kernel `run` again after `Err`? | doc gap (Minor) | GUIDE:3976-3980 covers `run` again after the source is exhausted, silent after `Err`; the builder's rebuild-after-restart is the safe reading | One sentence: after `Err`, restart and rebuild |
| §14 64-bit integers as strings | not an issue | GUIDE:8021 answered it | none |
| Report identity (brief gives none; platform does not dedupe) | app-side | GUIDE:1361 "the platform does not dedupe for you"; the builder's (product, source, time, amount) key is consistent | none |
| Ties the brief leaves open | app-side (brief gap) | FINDINGS.md:66-70; pinned by M6-M8 | none |
| No 403 | not an issue | GUIDE:1257-1260 says it outright ("There is no 403 … pick one of the variants above") | none |
| App state reachable from a route (the secret in a `OnceLock`) | doc gap (Minor) | GUIDE has no mechanism or example for deploy-time config reaching a handler; outbound env credentials (GUIDE:2066-2070) are the only precedent | GUIDE §5: how a route gets deploy config, and whether a set-once static is sanctioned |
| clippy never ran (box has no clippy) | not an issue for the platform (box image: README "rustc 1.99.1, no clippy") | I ran `cargo clippy --all-targets --offline --locked -- -D warnings` in the copy on 1.99.0: exit 0, no warnings (`t22rev/clippy.log`); the controller's gate also logged `clippy exit 0` | Experiment image should carry clippy |
| Sim found a stale test and a lost-setup case; `fired:` diagnosed it | not an issue (positive testimony) | FINDINGS.md:80-91 | none |
| `time` in the batch: 16 runs in 0.12 s "looks like nothing ran" | not an issue | no timing line by design; virtual time | none |
| Give-up rate about 20%, mostly setup | app-side | 8-round setup retry under faults (sim.rs:405-504); 1018 of 1280 tested | Optional: setup in a fault-free prefix |

---

## Section 4: platform rules broken from the app side. Verdict: none

- **Deny-list.** I ran clippy with the scaffold's `clippy.toml` (the platform fence minus internals) and `-D warnings`: clean. Nothing in `src/` calls a clock, OS randomness, fs, net or spawn. `std::env::var` appears once, in `main` (main.rs:4), at the edge, before `launch`, outside any transaction.
- **Transaction rules.** There is no `.await`, and handlers are sync. Each request mints at most one credential (`mint_operator_key` and `register_source` mint exactly one). Revocation is a row delete scoped to subject `operator`. `tx.scan::<ApiKey>()` in a route is an ordinary read.
- **Compile-time.** I found nothing that should have been refused. `Id::from_raw(product.raw())` to key `Prices` by product is the sanctioned raw escape. Every route declares `auth` or `public`.
- **Observation, not counted.** The one ambient input inside a handler is the process-global `OnceLock` (lib.rs:222), and it is set once to a constant, so replay holds: there was byte-identical replay across machines and across `--jobs`. The fence cannot see a `static` read in a handler. A static with interior mutability would carry state across sim universes in one process, and nothing would refuse it. This app does not do that.

---

## Findings by severity

- **Critical: 0.** So no platform oracle missed one.
- **Important: 3, all platform defects:** (a) the false `never occurred` coverage sentence; (b) functions with no viable mutant hidden behind "0 missed"; (c) the auto-timeout ignores shrinking.
- **Minor: 10.**
  - App: S1-m1 operator-claim hardening, S1-m2 unknown-source listing, S1-m3 source keys irrevocable.
  - Proof: S2-m1, two properties one-sided on their accept side.
  - Platform docs: (d) skipped seeds; (e) first-credential example; cargo-mutants' reach vs GUIDE:4811; kernel re-`run` after `Err`; deploy config reaching a route; GUIDE:4817 invokes `scripts/mutants.sh`, a platform-repo script that does not ship in the crate (`charpente-0.3.0/` has no `scripts/`), where an app means `charpente test --mutants … --test-package <app>`.

## Bar conditions

1. **Zero framework patches: met.**
   - Cargo.toml has no `[patch]` and no `path`.
   - Every charpente crate in Cargo.lock comes from the registry.
   - Every `/opt/registry` access in the transcript is a read of GUIDE, `examples/` or the four named tests, with no `src/` read.
   - The controller's gate logged `registry in box == local registry (5985 files)`.
2. **Zero Criticals a platform oracle should have caught: met.** There are zero Criticals.
3. **`--mutants` clean over the sim properties: left to the controller.** See the alert: the gate's first run used the 0.1.0 verb. The cargo-mutants mutant set is the same either way (82 total, 23 unviable).

The three Important platform defects do not fail conditions 1-2. The README says "a platform defect is fixed before the publish", so (a), (b) and (c) are publish blockers, separate from this verdict.

**Verdict: MEETS THE BAR** on conditions 1 and 2. Condition 3 rests on the controller's 0.3.0-verb run.
