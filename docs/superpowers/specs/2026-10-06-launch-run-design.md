# The launch run — design

**Date:** 2026-10-06. **Status:** approved in chat (Noah), spec committed for review.
**Predecessor:** PLAN.md M3 sub-project 5 and exit criterion (a). Sub-projects 1–4 have
shipped; S1 (tenancy completion) merged at `befcbde`, and CI is green on `11e550a` under the
pinned 1.99.0 toolchain. The real publish (`scripts/publish.sh --for-real`) is Noah's and
sits between this spec's two phases.

## Context

M3's goal is that a stranger reproduces the wager in one sitting. README's "Try it" is the
whole pitch, in its own words: install from crates.io, scaffold an app, run the sim green,
delete the `tx.enqueue` line in `src/lib.rs`, and "the second property goes red with a
seed, and the seed replays". Nobody has run that from the registry, because nothing is on
the registry yet. The scaffold's e2e test (`crates/charpente-cli/tests/new.rs`,
`a_scaffolded_app_is_fmt_clean_clippy_clean_under_the_fence_and_green_under_the_sim`)
proves the green half against the workspace. No test runs the red half at all: the line
README calls the whole pitch is pinned by nothing.

PLAN names this run "DEMO.md re-executed against the published version". Read literally
that cannot be done: DEMO.md's outbox bug exists only because the demo patched the
framework (`Ctx::enqueue_in_a_second_transaction`), and 0.1.0 ships no such hatch. README's
"Try it" is the same demonstration in a form the published crates can run, and README's
status paragraph already describes M3's finish line in exactly its shape. This run is
"Try it".

## §1 Decisions already made (Noah, 2026-10-06)

1. **The run is README's "Try it"**, not invoicing's DEMO.md by its letter (Context).
2. **The post is both** a verbatim transcript page and Noah's essay built on it. So the
   run is captured raw and complete, and presentation is decided after the run is seen.
3. **The stranger gets the published README and one sentence.** No GUIDE, no repo, no
   hint of which line to delete. If the README alone does not get there, that is the
   finding.
4. **The agent runs on this Mac; every command it issues runs in a fresh container**
   (§4). The commands' environment provably never saw the repo; the agent's own reading
   is held docs-only by a transcript grep.
5. **Rehearse before publishing** (§5), then publish, then the real run (§6). A finding
   in the real run costs a version: fix, publish 0.1.1, run again.
6. **Defaults:** the stranger is one Opus agent. The container is the `rust` image's
   default stable with no added components; if something the README needs is missing
   from it, that is a finding, because a stranger meets it too.

## §2 The bar

An attempt passes when all six hold, each checked by the controller from the box log
(§4), never from the agent's report:

- **(a) Green first.** The untouched scaffold's `charpente test --sim` exits 0 with no
  failures.
- **(b) Red with a seed.** With `tx.enqueue` deleted, `charpente test --sim` exits 1 and
  `acked_post_implies_the_echo_after_recovery` fails, printing a seed and a repro line.
  `acked_note_survives_a_restart` does not fail.
- **(c) The agent replays it.** The agent runs that repro line and it prints the same
  failure block as the batch did, byte for byte.
- **(d) The controller replays it.** In a second fresh container, from the agent's app
  directory copied out of the first, the same repro line prints the same block again,
  byte for byte.
- **(e) Sealed.** Nothing in either transcript reads charpente's source (§4), and every
  command the agent ran on the box is in the log.
- **(f) No help.** No framework patch, and no human input. The single exception is a
  resume after a harness failure such as an API error, with a fixed message, counted in
  the record.

Friction that leaves (a)–(f) standing is testimony, recorded in the write-up as the
statuspage findings were; it never fails an attempt. Noah signs every verdict.

## §3 The brief

`BRIEF.md`, committed before the first attempt, its sha256 in the record, byte-identical
for every attempt in both phases. Its text, in full:

> You've heard of charpente. Its README is in `README.md` beside this file. Reproduce
> what it claims.
>
> You have a fresh Linux machine for this. Run every command there with
> `./box '<command>'` from this directory: it runs the command in that machine's shell,
> in a working directory that persists between calls. Use only the README; don't read
> charpente's source code, whether in cargo's registry or online.
>
> When you're done, say what you did and what you saw.

`README.md` beside it is extracted from the packaged `charpente-cli` crate: the bytes
crates.io shows on the crate page (both `charpente` and `charpente-cli` set
`readme = "../../README.md"`). For the rehearsal that is the `.crate` from
`cargo package`; for the real run, the `.crate` downloaded from crates.io. The two are
compared when both exist.

## §4 The box, the wrapper and the log

- **Image.** `rust` at the stable current when attempt 1 starts, pinned by digest, the
  digest in the record. A real run after a toolchain release uses the newer stable and
  records it; the stranger's compiler is whatever a stranger would get.
- **Container.** One per attempt, created fresh, no volumes and no bind mounts, no
  credentials. The rehearsal's box has no network at all: its registry is a local
  directory source (§5), proven by a prototype on 2026-10-06. The real run's box has the
  default bridge network, because crates.io is the point; every fetch it makes is a
  logged command, checked below.
- **`./box`.** A script in the run directory, the agent's only route to the machine. It
  runs its argument in the container's shell, keeps the working directory between calls,
  and appends two JSON lines per call to the box log: a start line when the call begins
  (sequence number, UTC time, working directory, the command) and an end line when it
  returns (sequence number, UTC time, exit code, stdout and stderr, each in full), so a
  call killed mid-run is still on record. Under §1.2 this log is the source of the
  verbatim page.
- **Run directory.** A sibling of the charpente checkout, holding exactly `BRIEF.md`,
  `README.md` and `./box`. The agent works there.
- **Sealing, checked after.** Box side: any command reading a charpente crate's sources
  under cargo's registry, or fetching the repository or docs.rs source pages. Mac side:
  any tool call naming a path outside the run directory, or any shell call that is not
  `./box`. A hit voids the attempt; what it reached for is still testimony.

## §5 Phase 1: rehearsal, before the publish

- **The red half, pinned first.** Before any box exists, `tests/new.rs` gains the test
  the e2e test lacks: scaffold, delete the `tx.enqueue` line, and require `charpente test
  --sim` to exit 1 with `acked_post_implies_the_echo_after_recovery` failing and
  `acked_note_survives_a_restart` not, then require the printed repro line to print the
  same failure block. Its inversion: skip the deletion, so the test runs the untouched
  scaffold, and it must go red (exit 0 where it requires 1). The gate runs with it in.
- **Pinned sha.** `main`, tree clean, recorded first.
- **Registry substitute.** The README's commands run verbatim, `cargo install
  charpente-cli` included. Only the source changes: inside the box, crates.io is replaced
  by a local source holding the six `.crate` files `cargo package` produces at the pinned
  sha plus their dependency closure from the real registry. Preflight proves it: the
  controller runs "Try it" itself in a fresh box and requires (a) and (b) before any agent
  is dispatched.
- **Limits, stated.** A local source holds only that closure, so anything else the
  stranger tries to install fails offline. That is a rehearsal artifact, recorded as such,
  never a finding.
- **Fail loop.** A failed bar names its first failing item. The fix lands on `main` as its
  own change with a pin (a test, a compile-fail fixture, or a `doc_truth`-pinned block),
  the gate is run, and attempt N+1 starts with a fresh agent and a fresh box at the new
  sha, `BRIEF.md` byte-identical. **Three failed rehearsals stop the loop**, and it comes
  back to Noah.
- **Pass.** The verdict goes to Noah with any fix-before-publish findings named. The
  publish follows on his call.

## §6 Phase 2: the real run, after the publish

The same protocol against crates.io itself: a fresh box with no registry substitute, a
fresh agent, the same `BRIEF.md`, and `README.md` from the published crate. This run is
exit criterion (a). Its green-first step (a) also discharges the publish-path spec's
§10.2, "the ledger records one post-publish run against the registry".

A failed bar here follows §5's fail loop, except that a fix ships only as a new version:
Noah publishes 0.1.1 and the next attempt installs that. There is no attempt cap in this
phase. Every verdict goes to Noah, and he can stop it.

## §7 Adjudication

In this order, so a fail is attributed to the first thing that failed:

1. **Frame.** Pinned sha or published version, `BRIEF.md` sha256, image digest.
2. **Sealing** (§4), both transcripts.
3. **The bar** (a)–(c), read from the box log.
4. **The controller's replay** (d), in its own fresh box, its output kept beside the
   agent's.
5. **Testimony.** The agent's closing report and every place it stalled, guessed or
   re-read, each claim about charpente checked against the code, as the statuspage census
   did.
6. **Verdict to Noah**, before the ledger entry is written.

## §8 The record

`docs/experiments/2026-10-launch-run/`:

- `BRIEF.md`, byte-identical to what every attempt received.
- `README.md`, the write-up. A table per attempt with phase, pinned sha or version, image
  digest, egress mode, resume count, (a)–(f), the master seed of each batch, the failing
  seed, the replay results, and the verdict with Noah's sign-off. Then the testimony.
- `attempt-N/box.jsonl` (the box log), `attempt-N/agent.jsonl` (the agent's transcript)
  and `attempt-N/report.md` (its closing message), unedited. If a transcript is too large
  to commit, it is compressed rather than trimmed.

In the same commits (hard rule 6): a ledger entry per attempt. On a passing real run,
PLAN.md marks sub-project 5 and M3 shipped, and README's status paragraph says so. No
crate is added, so CLAUDE.md's inventory is untouched. No DECISIONS.md entry is
contradicted by this sub-project; a fix taken from a fail is checked on its own.

## §9 Oracles on the sub-project itself

- the red-half test (§5) holds README's pitch in `cargo test --workspace`, so a regression
  is caught by the gate rather than by a stranger;
- the pinned sha, or the published version, bounds "no framework patch" to a fact;
- the box log bounds "what happened" to recorded commands, not a report;
- the controller's replay in a second box bounds "the seed replays" to a reproduction;
- the transcript greps bound "README only" to a fact about tool calls;
- `BRIEF.md`'s sha256 bounds "same brief" across attempts and phases;
- the extracted README is compared between the packaged and the published crate.

## §10 Refused, so the plan does not grow them

- **Steering the stranger.** No hint in the brief, and no README edit made to help this
  run. A README that does not get there is the finding, and fixing it costs what any
  README fix costs.
- **Curating the capture.** Nothing is trimmed or edited before it enters the record.
  The post is made from the record afterwards, as separate work.
- **A real run before a rehearsal passes.**
- **Interactive runs.** A session Noah drives makes his answers an uncontrolled input.
- **Writing the launch post here.** This sub-project ends at a signed verdict and a
  record. The post is Noah's, built on it.

## §11 Exit criteria

1. The red-half test is on `main`, mutation-proved by its inversion; a rehearsal attempt
   passes §2 and Noah has signed it.
2. Noah has published 0.1.0, or the version the fail loop reached.
3. A real-run attempt against crates.io passes §2 with its seeds recorded and the
   controller's replay byte-identical, and Noah has signed it. This is M3 exit criterion
   (a).
4. `docs/experiments/2026-10-launch-run/` carries the brief, the write-up and every
   attempt's unedited capture. PLAN.md marks sub-project 5 and M3 shipped, README's
   status paragraph is updated, and the ledger's final entry says the roadmap is complete.
