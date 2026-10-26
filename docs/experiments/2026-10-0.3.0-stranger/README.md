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
