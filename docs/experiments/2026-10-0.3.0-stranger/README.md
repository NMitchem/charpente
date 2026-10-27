# The 0.3.0 stranger

This is the launch run's rehearsal, re-run against the packaged 0.3.0 crates (spec §6.7). A stranger gets only charpente's README and the launch run's one-paragraph brief. It reproduces README's "Try it" in a fresh offline box, and every command it issues runs through a logged `./box`. After its report it is asked one neutral follow-up, and its answer is recorded verbatim. The questions are whether the 0.3.0 output (cargo-shaped batches, tested/skipped counts, shrunk inputs, a single-seed replay) tells a reader what happened, and whether a stranger can act on it.

- **Spec:** [`docs/superpowers/specs/2026-10-22-0.3.0-design.md`](../../superpowers/specs/2026-10-22-0.3.0-design.md) §6.7. **Plan:** [`docs/superpowers/plans/2026-10-22-0.3.0.md`](../../superpowers/plans/2026-10-22-0.3.0.md) Task 21.
- **Procedure and tools:** the launch run's ([`../2026-10-launch-run/`](../2026-10-launch-run/), its `tools/`, and the launch-run plan's Tasks 4–7). The stranger launches headless from its run directory, per the launch run's Ruling P1-final: `claude -p "<prompt>" --model opus --restricted --tools Bash,Read --permission-mode dontAsk --allowedTools Read "Bash(./box:*)" --strict-mcp-config --output-format stream-json --verbose`.
- **Brief:** [`BRIEF.md`](BRIEF.md), sha256 `dae25ebbfa812bb93f9d650d254e77863ab651005b82c35ebf257aafdc674dd9`, byte-identical to the launch run's.
- **Prompt:** the launch run's T6 S2 prompt, with `Make no network requests from outside the box.` appended to its machine paragraph.
- **The follow-up, sent once after the report and recorded verbatim:** `In two or three sentences, what did the last sim run you did tell you?`
- **Image:** `rust@sha256:931c2c0531a8b143e044f0f3914dfe802f15bb3d1955f0b657e9359327ac2ffc` (`rust:latest` pulled 2026-10-25: rustc 1.99.1, no clippy).

## The bar, written before the run

The launch run's mechanical bar:
- the green batch;
- the red batch after the `tx.enqueue` line is deleted;
- the stranger's replay of a failing seed, which must print the identical block;
- the controller's replay in a second fresh box, which must also print the identical block;
- sealing.

Spec §6.7, across the report and the follow-up answer:
- **(a)** For each check, it says correctly whether the check held and how many runs tested it.
- **(b)** It gives at least the most common skip reason, correctly.
- **(c)** It describes the red run's shrunk input correctly.
- **(d)** Nothing it says calls the output overstated, misleading or unclear.
- **(e)** Every claim it makes about charpente is right, censused the way the launch run's were.

A miss is a finding about the output. It is fixed as its own pinned change and the next attempt runs, with both attempts recorded and a cap of three. A miss is never argued away.

## Attempts

| Attempt | Sha / version | README sha256 | Image | Network | Resumes | Green | Red | Replay (stranger) | Replay (controller) | Sealed | (a) | (b) | (c) | (d) | (e) | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `523fc7b8` (registry; R40) | `cbaf3242…` | `rust@sha256:931c2c05…` | none | 0 | pass (call 13) | pass (call 16) | identical (call 17) | identical | yes | **miss** | met | met | met | met | **FAIL on (a)** |
| 2 | `1106cdf0` | `cbaf3242…` | `rust@sha256:931c2c05…` | none | 0 | pass (call 10) | pass (call 13) | identical (call 14) | identical | yes | **miss** | met | met | met | met | **FAIL on (a)** |
| 3 | `81b4e8be` | `cbaf3242…` | `rust@sha256:931c2c05…` | none | 0 | pass (call 8) | pass (call 10) | identical (call 11) | identical | yes | **met** | met | met | met | met | **PASS** |

`523fc7b8` is the registry the attempt ran on. The round that followed changed only `#[cfg(test)]` code, GUIDE §8 and doc-comment text, and PLAN, all of which the brief puts out of reach; README.md was unchanged (Ruling R40, progress.md).

## Attempt 1

Launched 2026-10-26T18:15:21Z, headless, session `829eb860-f230-4ddc-a5d1-ab22c0351efb`: 25 turns, 147 s, 21 box calls, all exit 0, no resume. Its report and its follow-up answer are in [`attempt-1/report.md`](attempt-1/report.md), verbatim. The box log is `attempt-1/box.jsonl`, and `call N` below is seq N in it.

**The mechanical bar.**
- **Green, call 13** (master `0x757496059e186415`): `3 passed; 0 failed. 674 of 768 runs tested a check`.
- **Red, call 16**, after the stranger deleted `tx.enqueue` (call 14; master `0x52a91d2d9766b4d0`): `acked_post_implies_the_echo_after_recovery` FAILED, 206 of its 252 tested runs failing. The shrunk run is `shrunk: 6 draws to 3, faults off: write_error, …, in 10 replays`, and its repro carries `--inputs 00 --faults-off …`.
- **The stranger's replay, call 17,** and **the controller's replay** in a second fresh box (`attempt-1/replay/`, after the stranger's own call-14 deletion was re-applied, since it restored `lib.rs` before finishing; repro exit 1) print byte-identical six-line blocks. Each equals the batch block minus its `shrunk:` and `first:` lines (`attempt-1/batch.block`, `batch.run.block`, `replay-stranger.block`, `replay-controller.block`).
- **The block comparison was ruled after the run (R41).** The two lines describe the shrink, not the run. GUIDE §8 says a `--seed` replay shrinks nothing, so a replay cannot print them. The six that remain are what the bar tests: the repro reproduces the reported run.
- **`tools/block.sh`'s seed filter matches the repro line's last word.** In 0.3.0 that word is the `--faults-off` list, so the blocks were extracted without a seed. The batch prints one block per check.

**Sealing (`tools/seal-check.sh`): sealed.**
- The box side has two hits, both environment inspection with no source read:
  - call 3 reads cargo's config and lists the cargo home's `registry/` and `bin/`;
  - call 4 runs `ls /opt/registry`, which lists names only.
- The mac side has one hit, `ls -la && ./box '…'` in the run directory. It also accounts for the count, 20 agent `./box` calls against 21 box start lines.

**Spec §6.7.**
- **(a) Missed.** The report says, correctly, whether each check held in all four batches it ran, but never how many runs tested each one. "3 checks × 256 runs each" is the run count; the tested counts at call 13 were 209, 251 and 214. The follow-up gives the aggregate, "665 of 768 runs testing a check", which is right (call 21). It adds that it read only the last 8 lines. Those lines are the tail of the skipped-runs block and the result line; the check lines sit above that block, out of a tail's reach.
- **(b) Met.** "no POST was acked, so durability has nothing to prove", the most common skip reason (42 of 47 skips at call 13).
- **(c) Met.** "6 draws → 3", `notes=1, body="alpha"`, "one restart" (`fired: restart: 1`), with the faults turned off one at a time. It omits `order=[1]`, which is trivial for a single note.
- **(d) Met.** It calls the skipped-run accounting "honest". Its one caveat is the offline clippy, an environment fact.
- **(e) Met.** Every claim about charpente is right (census below).

**The finding, and its fix.** A reader who reads the end of a batch gets the aggregate tested count but not each check's. Ruling R42: the human batch output now ends with the check lines directly above `sim result:`, after the failure blocks and the skipped-runs block, as `cargo test` ends on its per-test summary. The change is pinned in its own commit, and attempt 2 runs on a registry rebuilt at that head.

### The census, attempt 1

| # | Claim | Ruling | Evidence |
|---|---|---|---|
| 1 | aarch64 Linux, Rust 1.99.1, no network; crates come from a vendored registry at `/opt/registry` with charpente 0.3.0 and ~110 dependencies | right | calls 1–4; the registry has 117 source directories, six of them charpente's |
| 2 | `cargo install charpente-cli` installs `charpente 0.3.0` | right | calls 6–7 |
| 3 | the scaffold has one table `Note`, one route `add_note`, one job `Echo`, one event `NoteAdded`, and three effects in one commit | right | call 8 |
| 4 | the green sim is 3 checks × 256 runs under injected faults, `3 passed; 0 failed`, with skip reasons and a master-seed rerun command | right | call 13 |
| 5 | deleting `tx.enqueue` fails `acked_post_implies_the_echo_after_recovery` with "1 POST(s) were acked with 200 but only 0 Echo job(s) are durable — the enqueue did not commit with the row", and the other two checks stay green | right | call 16 |
| 6 | it shrank 6 draws to 3, turning faults off one by one, and printed a `--seed` repro | right | call 16 |
| 7 | the repro replayed identically: same seed, same shrunk input (`notes=1`, `body="alpha"`, one restart) | right | call 17; the controller's replay |
| 8 | deleting `tx.emit` fails the third check alone, with an event-durability violation and its own seed | right | call 19 (208 of 208 tested runs failed; the other two ok) |
| 9 | restoring `lib.rs` gives `3 passed; 0 failed` | right | call 21 |
| 10 | one statically-linked binary is the platform, with no Postgres or Redis to provision | right | README:6, :11 |
| 11 | one commit point binds row, job and event | right | calls 16, 19 |
| 12 | deterministic simulation with seeded fault injection gives shrunk, byte-for-byte reproducible failures | right | call 17; the controller's replay |
| 13 | README's clippy step ("hard rule 1") cannot run here: no clippy, `rustup component add` is offline, and the vendored registry has no clippy | right | calls 9–11; README:173 |
| 14 | the last run was on the restored `lib.rs`: `3 passed; 0 failed`, 665 of 768 runs tested, master `0xd3bd6d26ee164b9a` | right | call 21 |
| 15 | a pass means the faults that seed drew broke no check, not that the app is proven correct | right | GUIDE §8's own reading of a green batch |

## Attempt 2

Attempt 2 ran on a registry built at `1106cdf0`, which carries R42's fix: the batch ends on its check lines, directly above `sim result:`. It launched at 2026-10-26T20:12:18Z, headless, as session `89793b60-9d0d-40f7-86be-9fd312f849d3`. The session took 22 turns and 157 s, and made 18 box calls. Two of those calls exited non-zero, and both were environment probes: call 3 listed an absent `~/.cargo/bin`, and call 8 was an offline `rustup component add clippy`. There was no resume. The report and the follow-up are in [`attempt-2/report.md`](attempt-2/report.md).

**The mechanical bar.**
- **Green, call 10** (master `0x82dc8265b9aba391`, `EXIT=0`): 215, 254 and 201 runs tested, `670 of 768`.
- **Red, call 13,** after deleting `tx.enqueue` in call 11 (master `0xbb4bd2377a45095d`, `EXIT=1`): `acked_post_implies_the_echo_after_recovery` FAILED in 203 of 254 tested runs. The shrink went 5 draws to 3, with nine axes off, in 10 replays.
- **Both replays** print byte-identical six-line blocks. That is the stranger's (call 14) and the controller's (second fresh box, `attempt-2/replay/`, with the call-11 deletion re-applied). Each matches the batch block minus `shrunk:` and `first:`, per R41.

**Sealing: sealed.** The box side has five hits:
- call 3, which lists cargo's `bin/`;
- calls 7, 10, 13 and 17, wait loops flagged for `$(seq 1 N)`.

None reads source. The mac side has one hit: `ls -la && ./box '…'` in the run directory, which also accounts for 17 agent calls against 18 box start lines.

**Spec §6.7.**
- **(a) Missed.** The report gives "3 checks × 256 runs = 768 runs, `3 passed; 0 failed`" and "~100 runs that *skipped*" (98, right), and no check's tested count. Asked about its last sim run (call 17, `tx.emit` deleted), it gave "failed in all 219 runs that tested it" for the failing check, which is right, and only "stayed green" for the other two. Both attempts carried the result line's aggregate rather than the check lines' counts.
- **(b) Met.** It gives "no POST was acked", the most common reason in every batch, and "app never booted".
- **(c) Met.** It reports `notes=1, order=[1], body="alpha"`, 5 draws → 3.
- **(d) Met.** It calls the output "honest" about skipped runs. Its caveat is the offline clippy.
- **(e) Met.** Every claim is right: the scaffold's five files and three properties (calls 4–5), the install from a local mirror, the replay "byte-identical … from the seed alone" (call 14 and the controller's replay), the emit failure's message (call 17), and the restore by byte copy without a re-run (call 18; the follow-up says so itself).

**The finding, and its fix.** Every reader quotes the result line, and the result line gives an aggregate, `N of M runs tested a check`. That sum also lets a check tested in a few runs hide behind well-tested ones. Ruling R43 has the result line give each check's tested runs, in the order of the check lines above it. The change is pinned in its own commit, and attempt 3, the last under the cap of three, runs on a registry rebuilt at that head.

## Attempt 3

Attempt 3 is the last attempt under the cap of three. It ran on a registry built at `81b4e8be`, which carries R43: the result line gives each check's tested runs. That head also carries the docs-only build's platform fixes, R44–R49, and none of them is in the README. The session launched headless at 2026-10-27T07:21:34Z as `fc8a4ca2-2946-4c92-90dc-6a6c5b90a97c`. It took 20 turns and 123 s, made 14 box calls that all exited 0, and was never resumed. Its report and follow-up are in [`attempt-3/report.md`](attempt-3/report.md).

**The mechanical bar**
- **Green, call 8.** The three checks tested 205, 251 and 219 runs.
- **Red, call 10.** After call 9 deleted `tx.enqueue`, `acked_post_implies_the_echo_after_recovery` failed in 210 of 250 tested runs. It shrank 5 draws to 3, giving `notes=1, order=[1], body="alpha"`.
- **Both replays match.** The stranger's replay (call 11) and the controller's (a second fresh box, `attempt-3/replay/`, with call 9's deletion re-applied) print byte-identical blocks. Each equals the batch block minus `shrunk:` and `first:` (R41).

**Sealing: sealed.**
- **Box side, one hit: call 7.** It was an environment probe. It printed cargo's `config.toml` (the `/opt/registry` source replacement), listed the files named `*clippy*` (names only, eight of them under `/opt/registry`) and printed the environment. It read no source.
- **Mac side, one hit:** an `ls -la` of the run directory.
- **Calls:** 14 agent calls, 14 box start lines.

**Spec §6.7**
- **(a) Met.** Asked what its last sim run told it, the stranger answered: "all three checks passed: 205, 253 and 205 of 256 runs actually tested the property, and the rest were skipped because a fault stopped them before the check could run". That is call 14's result line exactly, `Runs tested per check, in the order above: 205, 253, 205 of 256.`. It also said what it had not kept: "I filtered the output to the result lines, so I didn't record that batch's master seed and can't replay it exactly" (call 14 greps for `check acked|sim result`).
- **(b) Met.** The report gives "no POST was acked, so durability has nothing to prove", and the follow-up says skips are "because a fault stopped them before the check could run".
- **(c) Met.** The report gives the shrunk input `notes=1, body="alpha"` and the faults turned off, and says the printed repro reproduced "the identical failure deterministically".
- **(d) Met.** The report says the skipped runs were "honestly accounted for". The follow-up says a pass is "evidence for those claims, not proof".
- **(e) Met.** Every claim is right:
  - the scaffold's files, its one table, route, job and event, and its three properties (call 4);
  - the install from the vendored registry (calls 1–3);
  - both failure messages, quoted in substance: "1 POST(s) were acked with 200 but only 0 Echo job(s) are durable — the enqueue did not commit with the row" and "its NoteAdded is in the event log 0 time(s), not once";
  - `tx.emit`'s deletion failing the third check alone (call 13);
  - the restore to green (call 14);
  - `describe`'s tables, fingerprints, route and jobs (call 14);
  - clippy's absence: no component, and `rustup component add` cannot reach `static.rust-lang.org` (calls 5–6).

**Testimony.** The stranger read README's "charpente `0.1.0` is on crates.io" (README:133) against the mirror's 0.3.0. The sentence is true, since 0.1.0 is on crates.io, but it reads stale in a 0.3.0 README. README is left as this attempt read it, so that what ships is what was tested. Updating the line is for the publish.

**Verdict: PASS** on attempt 3, after two misses on (a). Both misses were findings about the output, and each was fixed as its own pinned change: R42 moved the check lines to the end of the output, and R43 gave each check's count on the result line every reader quotes.
