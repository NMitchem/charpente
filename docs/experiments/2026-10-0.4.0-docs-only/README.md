# The 0.4.0 docs-only build

This is the docs-only bar, re-run for 0.4.0 (spec §6.8). A fresh agent works offline and gets only what ships in the 0.4.0 crates: README, and the `charpente` crate's `GUIDE.md`, `examples/`, and the `tests/` files GUIDE names as worked examples. It builds an app in a new domain, a library's hold queue. The app's correctness rests on a lifecycle of more than four states (requested, ready for pickup, picked up, expired, cancelled), an outbound effect (telling a patron their hold is ready), and inputs drawn as items of several draws. The brief gives the domain only. It has no hints about the recovery helper, drawn inputs, shrinking or mutation.

- **Spec:** [`docs/superpowers/specs/2026-10-28-0.4.0-design.md`](../../superpowers/specs/2026-10-28-0.4.0-design.md) §6.8. **Plan:** [`docs/superpowers/plans/2026-10-28-0.4.0.md`](../../superpowers/plans/2026-10-28-0.4.0.md) Task 18.
- **Brief:** [`BRIEF.md`](BRIEF.md), committed before the run (`7e496abf`).
- **The machine:** the launch run's box ([`../2026-10-launch-run/tools/`](../2026-10-launch-run/tools/)), with no network. Its cargo is pointed at a directory registry holding the six 0.4.0 crates, packaged at `4126037f`, and their vendored dependency closure.
  - The image is `charpente-t18-box:clippy-mutants-27.1.0` (id `sha256:753de9affae9d56b2df9f5adf65270b1bb2fc079ef4fee4ee95ffb1fe0e907e1`).
  - It is 0.3.0's box image (rustc 1.99.1, cargo-mutants 27.1.0) plus `rustup component add clippy`, because the brief asks for a clean `cargo clippy`.
- **The builder:** one Opus agent, run headless from its run directory as 0.3.0's was. It has Bash for `./box` calls and Read confined to the run directory.

## The bar, written before the run

The bar is spec §6.8:
- (a) zero framework patches;
- (b) its sim tests use the recovery helper and the typed event reads, with no hand-written recovery, re-send loop or `EventRow` decode;
- (c) its final batch has no untested check;
- (d) it runs `charpente test --mutants` inside its session, and every viable mutant is caught or argued equivalent by name;
- (e) no shrunk failure it reports has an item the controller can delete with the failure still replaying;
- (f) review finds no Critical that a platform oracle should have caught.

Every finding it reports gets a disposition before the publish: fixed (with the commit), a PLAN v1.x row (with its trigger), or refused (with the reason). A miss is a finding, fixed and followed by a new attempt.

## Attempts

| Attempt | Sha | Image | Network | Resumes | (a) | (b) | (c) | (d) | (e) | (f) | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `4126037f` (registry) | `charpente-t18-box:clippy-mutants-27.1.0` | none | 0 | met | met | met | met: `119 total — 82 caught, 0 missed, 37 unviable, 0 timed out` | met | met (0 Critical) | **MEETS THE BAR**, with four platform defects; three are fixed before the publish and one is a PLAN row |

## Attempt 1

Attempt 1 launched at 2026-11-01T13:44:03Z as session `2dd4443a-069f-4846-a1da-0920c8e569b6`. It ended at 14:51:47Z, after 101 turns and 68 minutes.
- **Calls.** It made 95 `./box` calls. Twelve were refused by the session's permission mode, which turns down a command containing a line that starts with `#`, as every Rust attribute does; the builder wrote files with a placeholder and restored the `#` afterwards. The other 83 ran.
- **What it delivered.** `holdqueue`, in six commits ([`attempt-1/holdqueue/`](attempt-1/holdqueue/); its log is `holdqueue.git-log.txt`).
- **Its own records.** Its final message is [`attempt-1/report.md`](attempt-1/report.md), its friction ledger is `holdqueue/FINDINGS.md`, and both of its `--mutants` runs are in `holdqueue-mutants-{1,2}.log`.
- **Redaction.** The builder set its box's `git config user.email` to the operator's account address, which it found in its harness context. The committed `agent.jsonl` and `box.jsonl` carry `<operator-email>` in its place, once each; this is the only change. The originals' sha256 are `853f7553…` and `1b047b89…`.

**The bar.** The controller's gates are in [`attempt-1/gates.md`](attempt-1/gates.md), and the review is in [`attempt-1/review.md`](attempt-1/review.md).
- **(a) met.** No `[patch]`, no `path`, and every charpente crate in `Cargo.lock` comes from the registry. The registry in the box is byte-identical to the local one.
- **(b) met.** `tests/sim.rs` drives every run through `charpente_sim::drive`: one faulted phase, then the fault-free tail. It has no hand-written recovery or re-send loop. The app emits no `#[event]`, so the typed event reads are moot. Its notification check decodes job rows by hand rather than using the checker library's outbound checks. That is a Minor app-side note (m9), not an `EventRow` decode.
- **(c) met.** The final batch, 8 checks × 256 runs under master seed `0x4a3bc8dfdc80c1da`, has every check tested. It replays line for line on a different OS and machine.
- **(d) met.** The builder's first run, over `queue.rs` and `notify.rs`, found 11 MISSED: repeat and conflict comparisons in `place`, `record_return` and `set_copies`, which no property checked. It wrote two more properties, brought `lib.rs` into scope, and its second run gave `119 total — 82 caught, 0 missed, 37 unviable, 0 timed out`. The controller re-ran that run's printed command and got the same 83 verdict lines. Each two-pass run took about 22 minutes in the box, and the controller's re-run took 1073 s on this Mac. These are observations, not BENCH figures.
- **(e) met.** The review rebuilt the exact source of each of the three shrunk failures the builder hit, and proved the rebuild by forward application to the next commit. It then replayed every single-item deletion: 42, 6 and 7 items, none of which still fails. The 69 → 64 shrink spent its whole budget, though, and a 28-draw run fails the same check. That gap is I4 below.
- **(f) met.** The review found no Critical. Every hand mutation the dispatch named went red: a later hold served first, the stock bound skipped, the notification enqueue dropped, a pickup of a requested hold accepted, and expiry at 6 logical days. Five more stayed green, and the review showed each one took effect: four operator-guard deletions, and a tick interval that expires a hold after 6 days of real time. Those are app-side proof gaps, none Critical (I6 below). cargo-mutants never deletes a `?` statement, and the verb had named those functions untested.

## Dispositions

Every finding gets one disposition (spec §4 G): **fixed** with its commit, a **PLAN** v1.x row with its trigger, or **refused** with the reason. "T" numbers are the review's triage of the builder's own findings, and "I" and "m" numbers are its Importants and Minors ([`attempt-1/review.md`](attempt-1/review.md)).

**The platform's.**

| Finding | Disposition |
|---|---|
| T1 / I1: GUIDE §5 said `Id<T>` of any `T` crosses the wire; rustc's `Row` error named no fix | **fixed** `7907e283`: GUIDE says a `#[table]` type, `Row` carries a note naming both fixes, and a trybuild case pins it |
| A bare `u64` path param's note offered `WireU64`, which is not `FromParam` (found while fixing I1) | **fixed** `ac360050` and `0f27e0f2`: the note names `Id<T>`, `WireU64` in a body, response or event field, and `String` in a path param |
| T2 / I2: the API page's `login` was public and minted a session for a subject the body names, which GUIDE forbids | **fixed** `9386f2ea`: an `auth` route minting for the caller, plus a census in `driver_census.rs` over every example, scaffold template and GUIDE block; `0f27e0f2` states what the census misses and over-flags |
| T2: no worked first credential (this app bootstraps by trust on first use, I8) | **PLAN** row "A worked first-credential example…", trigger already met; this build is the fifth app to invent one |
| T10 / I3: a `--seed` replay of a skipped run showed no faults that fired | **fixed** `009911e7`: the replay prints the failure block's `fired:` line |
| T7 / m1: `--faults-off` with an unknown name listed no valid names | **fixed** `78bf2fa4`: the refusal lists `FAULT_AXES` and GUIDE §8 names them; `0f27e0f2` pins GUIDE's list to `FaultConfig`'s fields |
| I4: the shrinker stalls on a count-prefixed loop (64 draws kept where 28 fail) | GUIDE §8 note **fixed** `20cac74f` (prefer a per-item "one more?" draw); the shrinker retry is a **PLAN** row, `48cf2f7f`, trigger "a second build whose shrunk failure keeps items a smaller count would drop" |
| T6 / m2: an answer body's type was unstated on the API page | **fixed** `e4addec7` |
| T8 / m4: whether `by_<field>` reads see the transaction's own writes | **fixed** `48cf2f7f`, one sentence in §4 |
| T13 / m5: §7 called cron's single catch-up what "most" period logic wants, which is false for a deadline | **fixed** `48cf2f7f`, qualified; a transaction's own time is the **PLAN** row "A transaction's own time", due for 0.5.0, with this build as testimony |
| T4: no time inside a transaction and no delayed job | **PLAN** row "A transaction's own time", due for 0.5.0 |
| T5 / m3: `TxError` has no `Display` | **PLAN** row P9 ("Four small API gaps…, and `TxError: Display`"), trigger now met |
| T14 / m6: GUIDE's re-send rule is silent on state transitions | **fixed** `48cf2f7f`: §8 says to key a transition by its own id |
| T16, T11 / m7: no guidance on sim credentials or on front-loading a durability claim's writes | **fixed** `48cf2f7f`: §8 says to seed fixtures in the build closure |
| T9, T11: the durability check reaches a verdict in about half its runs | **refused**: the give-ups are the fault rate (settled 2026-09-07). Of the 133 skips, 93 are faults at the first commits, and about 40 are the app's setup coming before its first hold |
| T12: pass 1 streams coverage-only catches as MISSED | **refused**: the verb says before pass 1 that its MISSED lines are judged again in pass 2, and the builder read them right |
| T3: may a handler `tx.put` an `ApiKey`? | **refused**: GUIDE §5 already says it reads and writes them like any global table |
| T15: `ApiError` has no 403 or 409 | **refused**: by design, GUIDE §5 |
| T18: the corruption excuse (commit `6da2431`) | **refused** as a platform finding: the app gives up only where `durable_corruption_fired`, never passes wrong data, and excuses no isolation check, so it is not PLAN's "routes a consistency oracle through the excuse" trigger |
| I5's platform half: Dead is unreachable in the sim and an app cannot notice it | **PLAN** row "Dead reachable in the sim…", due for 0.5.0, with this build as testimony |
| `examples/invoicing` still has two public logins that mint for a body-named subject | **PLAN** row "`examples/invoicing`'s two logins…" (`6b5df554`), trigger "the next commit that changes `examples/invoicing`'s routes, or a build or review that copies either login": the app is `publish = false`, and the change moves its recorded batches once |
| The session's permission mode refused commands with a line starting with `#` | **refused**: Claude Code's permission check, not charpente |

**The app's** (the builder's code, no charpente change): I5, the notification goes Dead after about 68 minutes of outage and the patron is never told; I6, four operator guards and the tick interval have no property; I7, a repeated pickup or cancel answers 200; I8, the operator bootstrap is trust on first use; m8, a hold id's existence leaks through the place conflict; m9, the notification check decodes job rows by hand; m10, a stale "ready" message after a cancel.

Attempt 1 stands, and the bar does not call for a re-run.
