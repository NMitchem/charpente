# The 0.3.0 docs-only build

This is M3 sub-project 3's bar, re-run for 0.3.0 (spec §6.8). A fresh agent works offline and gets only what ships in the 0.3.0 crates: README, and the `charpente` crate's `GUIDE.md` and `examples/`. It builds an app in a new domain, a price watch, whose correctness rests on an order-sensitive merge. The brief gives the domain only. It has no hints about drawn inputs or shrinking.

- **Spec:** [`docs/superpowers/specs/2026-10-22-0.3.0-design.md`](../../superpowers/specs/2026-10-22-0.3.0-design.md) §6.8. **Plan:** [`docs/superpowers/plans/2026-10-22-0.3.0.md`](../../superpowers/plans/2026-10-22-0.3.0.md) Task 22.
- **Brief:** [`BRIEF.md`](BRIEF.md), committed before the run.
- **The machine:** the launch run's box ([`../2026-10-launch-run/tools/`](../2026-10-launch-run/tools/)), with no network. Its cargo is pointed at a directory registry of the six 0.3.0 crates, packaged at the pinned sha, beside their vendored dependency closure.
  - The image is `charpente-t22-box:mutants-27.1.0` (id `sha256:f7db863a76ac520bbb584d40243cbc8ac9be1be76955dccbd58ce48eef5dd145`). It is the 0.3.0 stranger's `rust@sha256:931c2c05…` image (rustc 1.99.1, no clippy) plus `cargo install cargo-mutants --version 27.1.0 --locked`, because the bar runs `charpente test --mutants`.
- **The builder:** one Opus agent, run headless from its run directory as the launch run's stranger was (Ruling P1-final). It has Bash for `./box` calls and Read confined to the run directory.

## The bar, written before the run

The bar is M3 sub-project 3's:
- zero framework patches;
- zero Criticals in review that a platform oracle should have caught;
- `charpente test --mutants` clean over its sim properties, with no hand mutation pass.

Findings are ledgered as testimony, and a platform defect is fixed before the publish.

## Attempts

| Attempt | Sha | Image | Network | Resumes | Framework patches | Criticals a platform oracle should have caught | `--mutants` | Verdict |
|---|---|---|---|---|---|---|---|---|
| 1 | `1106cdf0` (registry) | `charpente-t22-box:mutants-27.1.0` | none | 0 | 0 | 0 (0 Critical) | clean: `82 total — 59 caught, 0 missed, 23 unviable, 0 timed out` at `--timeout 300` | **MEETS THE BAR**, with three platform defects to fix before the publish |

## Attempt 1

Attempt 1 ran on the registry built at `1106cdf0`. It launched headless as session `6edbe5c6-69b8-4bc2-8bc2-5fef4aab1f1a` and ended at 2026-10-26T20:49:18Z after 133 turns and 37 minutes. Of its 128 Bash calls, 22 were refused by the session's permission check, which read Rust closures and Markdown tables as shell pipes (its report says so). No call was resumed. The builder delivered `pricewatch` in five commits ([`attempt-1/pricewatch/`](attempt-1/pricewatch/); its log is in `pricewatch.git-log.txt`). The final message is [`attempt-1/report.md`](attempt-1/report.md), and its friction ledger is `pricewatch/FINDINGS.md`.

**The bar.**
- **Zero framework patches: met.** See [`attempt-1/gates.md`](attempt-1/gates.md).
- **Zero Criticals that a platform oracle should have caught: met.** The review, [`attempt-1/review.md`](attempt-1/review.md), found no Critical. Thirteen hand mutations of its own were all caught; they included the tie-break flipped, latest by arrival, first-seen lowest, dedup on (product, source), and a zero amount accepted. The five properties are two-sided as a suite.
- **`--mutants` clean, with no hand pass: met.** The controller's run under the 0.3.0 verb gives `82 total — 59 caught, 0 missed, 23 unviable, 0 timed out`. The builder's final batch replays byte for byte from its master seed on a different OS and machine.

**What it found in the platform.** The review classified every entry in FINDINGS.md against the code at `1106cdf0`. Three are platform defects, and the controller verified each one:
- **(a) A false coverage sentence.** `required "…" never occurred in N runs` counted only passed and skipped runs. The review's mutation M5 failed 52 runs, and all 52 fired the key the sentence says never occurred. Its advice, "drop the requirement", would delete an oracle. Ruling R44: a key that a failed run fired has occurred.
- **(b) "0 missed" with untested functions.** The verb's totals hid 12 functions in which no mutant was viable, among them both merge folds and the handler that applies them. Before the builder rewrote `latest_rank`, every one of its 42 mutants was unviable, because cargo-mutants cannot construct `std::cmp::Reverse`. GUIDE called every unviable mutant "the compile-time contract doing its job". Ruling R45: the verb names those functions as untested.
- **(c) An auto-timeout that a caught mutant cannot meet.** The failing sim shrinks before it exits. Under the default invocation (no `--timeout`, `--jobs 4`, 12 cores), 20 of the 59 mutants that `--timeout 300` catches read TIMEOUT: `82 total — 39 caught, 0 missed, 23 unviable, 20 timed out` (`attempt-1/gates/t22-timeout-red.log`). Ruling R46: under `--mutants` the sim does not shrink.

The doc gaps:
- GUIDE says to `--seed` a skipped run, and no output gives one (R47: each skipped reason names its first run's seed).
- GUIDE §8 told an app to run `scripts/mutants.sh`, which does not ship, and overstated what cargo-mutants generates (R45, R48).
- GUIDE never said a kernel whose `run` returned `Err` is rebuilt (R48).
- There is no worked first-credential example and no sanctioned way for deploy-time configuration to reach a handler. The builder's `PRICEWATCH_OPERATOR_SECRET` sits in a set-once static, which the review judged sound and which the fence cannot see. This is a design question and goes to PLAN's v1.x table.

The app-side Minors (operator-claim hardening, an unknown source's listing, source keys that cannot be revoked) and the proof's Minor (two properties judge only their refuse side, and their siblings cover the other) are testimony. The fixes land in their own pinned commits after this record. Attempt 1 stands, and the bar does not call for a re-run.
