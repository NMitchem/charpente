# pricewatch — friction ledger

Built from charpente 0.3.0's shipped documentation only: `GUIDE.md`, the
three `examples/`, and the worked-example tests GUIDE names. No `src/` of any
charpente crate was read and no framework file was patched. Every entry below
is dated; all of them fall on one working day.

## Tally

- **Approaches restarted: 2.**
  1. The operator bootstrap. First design: `POST /operator/key` minted a key
     for whoever called it first on a fresh node. I dropped it before it ever
     ran: it hands the node to whoever gets there first. Second design: the
     same route behind a deploy-time secret (`PRICEWATCH_OPERATOR_SECRET`),
     refusing a second claim once an operator key existed. That one ran, and
     the sim showed it could not be retried (below), so the third design
     *rotates*: a claim with the secret revokes every earlier operator key and
     mints a new one.
  2. Sim setup. First version: one setup script, giving up when any step
     missed its ack. About 75% of runs gave up. Second version: a retry loop
     over idempotent steps, rebooting the kernel when it stopped. It had a bug
     of mine (completion was only checked when a round ended with the kernel
     still up), which the give-up `detail` exposed.

## GUIDE sections re-read, and why

- **§5 "Auth"**, three times: to find how an operator gets its *first*
  credential. §5 shows `ctx.create_api_key` minting a key for the caller's own
  subject and says "the first credential comes from something the app trusts,
  such as an IdP or a signup route". It never shows a key minted for another
  subject (an operator issuing a key to a source); I inferred that
  `create_api_key(tx, "source:<id>")` is allowed because the signature takes any
  subject.
- **§16 "Where the guarantee stops"**: to check whether operator code holding
  the `Db` could mint an `ApiKey` row directly. It can write the row, but the
  row stores a SHA-256 of a raw key, and nothing in the docs exposes the hashing
  or a mint outside a request. So I abandoned that path.
- **§8 "Giving up" and "The checkers"**, twice: to settle which losses
  `durable_corruption_fired` may excuse. The answer, an acked *setup* step cut
  by `misdirected_write`, is the same rule applied to setup rather than to the
  property's own write. GUIDE only shows it for the latter.
- **§8 "Driving the whole app in a sim"**: to learn whether a stopped
  `Kernel` can be `run` again or must be rebuilt. It says a kernel can be `run`
  again after its source is exhausted, but not what happens after `run`
  returned `Err`. I rebuild it after a process restart (`restart_process`, or
  `restart` where `disk_crashed`).
- **§14 "64-bit integers"**: to confirm `WireI64`/`WireU64` serialize as
  decimal strings, which the sim's JSON oracles compare against.

## What I could not find in the docs, and what I chose

- **A first operator credential.** No CLI verb, config field or operator-code
  helper mints an API key outside a request. *Chose:* a deploy-time secret,
  read by `main` from `PRICEWATCH_OPERATOR_SECRET` and held in a `OnceLock`.
  The secret buys an operator key at `POST /operator/key`, and each claim
  revokes the earlier keys, so it is safe to retry. Without the secret, no
  operator key can be minted.
- **App state reachable from a route.** Routes are free functions, and GUIDE
  shows no way to hand them configuration, so the secret sits in a process-wide
  `OnceLock`. The fence does not forbid that, and it is outside any table.
- **Report identity.** The brief gives a report no id, and the platform does
  not dedupe (§5: "the platform does not dedupe for you"). *Chose:* a report is
  everything it says: (product, source, observation time, amount), indexed as
  a string key. One source reporting two amounts for one product at one instant
  is two reports.
- **Ties the brief leaves open.** For the lowest price, equal amounts go to the
  earlier observation, then the smaller source id. For the latest report, a tie
  on time *and* source goes to the smaller amount. Both rules make the order
  strict over distinct reports, so the folds in `src/merge.rs` do not depend on
  arrival order.
- **No 403.** §5 says to pick a variant. Operator-only and source-only routes
  answer 400 with a named message.
- **clippy.** The box's toolchain ships without clippy, and the box is offline:
  `rustup component add clippy` failed with a DNS error. **`cargo clippy
  --all-targets -- -D warnings` was never run**, so that gate is unverified.
  `cargo build --all-targets` has no warnings.

## What the sim's output told me, and where it was unclear

- **It found a test bug in seconds.** After I switched the operator claim to
  rotating, the sim's first failure was my stale "a second operator key must
  be refused" check: `a second operator key answered 200 "ck_…", not 400`. The
  shrunk block gave the whole case on one `inputs:` line, a one-report run,
  and a `repro:` that replayed it.
- **It found a lost-setup case I had not excused.** `a report of -1 cents
  answered 401 … unrecognized credential …`. The `fired:` line showed
  `fault:misdirected_write: 1`, so media corruption had cut the source
  registration that setup had seen acked. That is the loss GUIDE §8 lets
  `durable_corruption_fired` excuse. The scenario now gives up in that case
  and fails in any other. I could not have got this from the `config:` line:
  `misdirected_write: 0.006` there only says it was possible.
- **The skipped-reasons block drove the setup rewrite.** It showed 75% of runs
  giving up in setup. The second version's `detail` lines, read with `--seed`,
  showed rounds like `[200, 200, 200, 200, 200, 200] of 6 (up: false)`: every
  step acked, then the kernel stopped. My loop never rechecked completion
  after rebooting. Reading the details took a loop of my own over `--seed`
  values: a batch never prints a skipped run's seed, so I searched seeds by
  hand until one gave up.
- **Unclear: coverage in failing runs.** The first failing batch said, of the
  check that had failed 44 times after reaching `sim.cover("pricewatch:judged")`:
  `sim coverage requirement failed …: "pricewatch:judged" never occurred in 64
  runs`. It seems only passing runs count toward a `require`, but GUIDE does
  not say so, and "never occurred" reads as a second, independent bug.
- **Unclear: `time` in the batch.** The first `--runs 16` batch took 0.12 s of
  wall time and still printed a whole report. That only made sense once the
  256-run batch took 5 s on 12 cores. Virtual time really is that cheap; it
  just looks like nothing ran.
- **Give-up rate at the default 256 runs** (master seed `0xf2ce5f0210be4524`):
  1013 of 1280 runs tested a check. Most give-ups are setup: faults that kept a
  registration from its ack in eight rounds.

## `charpente test --mutants`

Run over the two files that carry the merge: `src/merge.rs` (the folds, the
amount check, report identity) and `src/lib.rs` (the handler that applies
them, plus the routes and credentials around it). Judged by pricewatch's own
suite, the sim properties: there are no unit tests.

**First run** (`--jobs 4`, timeout auto-set to 26 s): 105 mutants.

```text
Found 105 mutants to test
ok       Unmutated baseline in 6s build + 5s test
 INFO Auto-set test timeout to 26s
TIMEOUT  src/merge.rs:40:5: replace lowest_rank -> (i64, u64, u64) with (0, 0, 1) in 3s build + 26s test
105 mutants tested in 4m: 39 caught, 65 unviable, 1 timeouts
mutants: 105 total — 39 caught, 0 missed, 65 unviable, 1 timed out
  1 TIMED OUT: unresolved, NOT caught — raise --timeout and rerun before reading this batch as clean.
```

Two problems, neither a MISSED:

1. **The timeout.** A mutant that fails the sim makes the sim *shrink* its
   first failure, up to 500 replays per check. A broken build therefore runs
   far longer than the 5 s baseline the 26 s timeout came from. GUIDE §8
   documents the shrink budget and the timeout separately and never connects
   them. Rerun with `--re lowest_rank --timeout 300`: `12 mutants tested in
   2m: 12 caught`.
2. **"0 missed" said nothing about the latest-report order.** `unviable.txt`
   held 42 mutants of `latest_rank`. Its key was `(Reverse<u64>, u64, i64)`,
   and cargo-mutants cannot construct a `std::cmp::Reverse`, so every
   replacement failed to compile. The whole tie-breaking rule for the latest
   report had no viable mutant. I rewrote the key as a plain tuple with the
   time negated through `i128`, which orders the same way, so cargo-mutants
   can mutate it.

**Final run, after that change** (`--jobs 4 --timeout 300`), output in full:

```text
Found 82 mutants to test
ok       Unmutated baseline in 6s build + 5s test
82 mutants tested in 6m: 59 caught, 23 unviable
mutants: 82 total — 59 caught, 0 missed, 23 unviable, 0 timed out
```

Every mutant is caught or unviable. The caught set includes all 19 of the new
`latest_rank` mutants (`delete -` among them), all 12 of `lowest_rank`, all
six of `check_amount`, both of `report_key`, and every guard and comparison in
`require_operator`, `mint_operator_key` and `configure_operator_secret`. The
23 unviable ones are body replacements with `Default::default()` or
`Id::new()`, which none of these types implement.

### What `--mutants` cannot see, checked by hand

cargo-mutants replaces function bodies and operators. It never deletes a
statement, and the statements that matter most in `submit_report` are writes:
`tx.put` of the report row and of the kept prices. So I deleted them by hand,
each in a throwaway copy of the crate, and ran `charpente test --sim --runs 64
--master-seed 0x1` against it:

- H1, drop the dedup lookup: caught by `a_redelivered_report_counts_once`
  (48 of 51 tested runs failed).
- H2, drop the kept-prices write: caught by `kept_values_ignore_arrival_order`
  (52 of 53).
- H3, `lowest` ignores the kept value: caught by
  `kept_values_ignore_arrival_order` (26 of 52).
- H4, `latest` ignores the kept value: caught by
  `kept_values_ignore_arrival_order` (29 of 52).
- H5, drop the report row: caught by `acked_report_survives_restarts`,
  `kept_values_ignore_arrival_order` and `a_redelivered_report_counts_once`.

Each was caught by the property written for that claim. H1's failure also
printed `required "pricewatch:duplicate-acked" never occurred`, which is the
misleading coverage line described above.

## The final batch

`charpente test --sim` at the default 256 runs per check (no flags):

```text
check acked_report_survives_restarts ................ ok  (192 tested over 190 distinct inputs, 64 skipped)
check kept_values_ignore_arrival_order .............. ok  (204 tested over 204 distinct inputs, 52 skipped)
check a_redelivered_report_counts_once .............. ok  (207 tested over 207 distinct inputs, 49 skipped)
check a_bad_amount_is_refused_by_name ............... ok  (214 tested over 214 distinct inputs, 42 skipped)
check only_the_right_credentials_reach_each_route ... ok  (201 tested over 200 distinct inputs, 55 skipped)

sim result: ok. 5 passed; 0 failed. 1018 of 1280 runs tested a check.
rerun exactly this batch: charpente test --sim --master-seed 0x2c0cbfefeb3ad9c2
```

Skipped runs, by reason: setup losing a registration's ack in all eight
rounds (41 to 52 per check); no report acked under faults (10, in the
durability check only); a durable-horizon boot refusal (2 to 4 per check);
and one run where durable corruption cut an acked setup step. `cargo test`
runs the same binary and passes too.
