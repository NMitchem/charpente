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
