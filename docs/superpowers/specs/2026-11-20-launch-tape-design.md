# The launch tape: one double charge, caught in a second

Status: approved by Noah 2026-11-20 ("write it up"), with his answers folded in (§9). Plan:
[docs/superpowers/plans/2026-11-20-launch-tape.md](../plans/2026-11-20-launch-tape.md). Drafted after
the launch demo's rehearsal came out 0 against 0
(`.superpowers/sdd/2026-11-09-launch-demo-rehearsal/go-no-go.md`), with Noah's pointer to
retrace's demo as the model.

## 1. Intent

charpente has never been announced. The launch demo spec
([2026-11-06](2026-11-06-launch-demo-design.md)) planned to lead with a head-to-head: the same
agent builds the same backend on charpente and on a strong conventional stack, and a neutral
judge scores both under faults. The rehearsal scored both stacks 0 violations, so a reader
can't see a difference from it.

retrace's launch shows what works instead. Its README opens with a 28.6-second terminal video
of one real bug: 25 lines of C, a crash inside SQLite that every usual tool blames on SQLite,
and two commands that name the line that caused it. Every line on screen is real output from
the installed tool. A table shows what the usual tools say about the same bug. The wording is
plain: retrace commit `16149ad0` cut the slogans and closers and gave the headings plain names.

This project gives charpente the same front door, built from three parts:

1. **The tape:** a terminal video of about 30 seconds in which `charpente test --sim` finds a
   double charge in an order API, names the fix, and replays it from its seed.
2. **The output cleanup:** the sim's red output fits one screen and reads plainly. The tape
   only shows real output, so this is a product change shipped in a release, not an edit.
3. **The README rewrite** in retrace's shape: one plain sentence, the video, the example, how
   to install and use it, and its limits.

The idea the launch carries is unchanged from the launch demo spec §1. On charpente, something
can still say no to the code an agent writes. The tape shows that "no" in one second.

## 2. The bug

Measured on 2026-11-20 at `e750a520`. `crates/charpente/examples/modelled_remote.rs` is an
order API: `POST /orders/:id` stores an order and enqueues a `Charge` job, and the job calls a
modelled payment gateway, `pay.example`, that dedupes on an idempotency key. `place_order`
starts with three lines that answer an order that is already stored:

```rust
if tx.get(id)?.is_some() {
    return Ok(id);
}
```

Delete them, and a client that re-sends a POST after a lost reply places the order a second
time, with a second `Charge` job carrying its own key, so the gateway charges twice. Under
master seed `0x797d739e3d4aca7c`, `cargo run -p charpente --example modelled_remote` exits 1 in
0.9 s on a warm build (a wall-clock observation, not a BENCH.md number): 25 of 249 runs tested
failed `each_order_is_charged_once`, and 7 were skipped. The first failure shrank to one input
with five faults off, and its message names the fix: "make place_order answer an order that
is already stored".

Why this bug:
- Anyone who has written a backend knows it, and it costs real money.
- It is written in ordinary app code. M0's outbox demo needed a framework patch to show its
  bug, which makes it an argument, not a demo.
- It needs three things in order: the order commits, its reply is lost, and the client sends
  again. A test that sends each order once never meets it.
- charpente's own key handling is correct, and the bug is still caught. The sim judges what
  the app does, not only what the platform does.

What a careful reader will notice, and the README must say plainly: the bug needs `tx.put`'s
upsert followed by an unconditional enqueue. A conventional port that `INSERT`s on the client's
id would refuse the re-send with a unique violation, roll back its job, and charge once (builder
check, 2026-11-20). The bug is real on any stack that upserts, or that mints the id on the
server, but it is not what every conventional port does. The most common real-world shape is a
server-minted id with an `Idempotency-Key` header, and that waits on PLAN's F2 row (no route
reads a header). My call: the client-id version now, explained honestly, and the header
version once F2 lands.

## 3. Goals, non-goals, anti-goals

Goals:
- A reader understands the tape without sound and without reading the README.
- Everyone can reproduce it: the demo app and the exact commands are public, and the tape
  runs the released crates from crates.io.
- The sim's red output is better for every reader, agents included, not only for the camera.
- The README says what charpente is in one sentence, and every claim in it is checkable.

Non-goals:
- New platform features. The tape uses what 0.5 ships.
- Deciding the head-to-head's fate (§9, Noah's).
- A website, a launch post, or the leaderboard. Those are the launch demo spec's §8.

Anti-goals (each would make this a failure even if it worked):
- Edited or trimmed output on screen: a `| head`, a cut, or a format that exists only for the
  video.
- A bug only an escape hatch can produce.
- Saying an agent wrote the bug when none did.
- A number in the README that isn't in docs/BENCH.md and doesn't carry its master seed.
  `crates/guide-examples/tests/claims_census.rs` already refuses it.
- Slogans, closers, and claims the record doesn't support: "no bugs", "structurally
  impossible", "3am page".

## 4. Approaches

**Where the demo app lives.**
- **(chosen) A small app directory, `examples/double-charge/`,** shipped with the bug. Its
  sim goes red, and a workspace test asserts it goes red with the double-charge message, the
  same victim pattern `crates/merge-fixture/tests/sim.rs` uses. It is cheap, adds no CLI
  surface, and readers reproduce it with a clone.
- `charpente new <name> --demo` writing the buggy app. This is closest to retrace, which
  installs its demo with the tool, but it adds CLI surface and template oracles for a demo.
  It can come later if people ask for it.
- Delete the three lines from `modelled_remote` on camera. That spends seconds on an edit and
  turns a teaching example into a victim.

**How the output gets cleaner.**
- **(chosen) Change the default human format for everyone,** pinned by tests, with GUIDE §8's
  "Reading a failure" updated in the same commit (hard rule 6).
- A separate brief format. Two formats to keep honest, and a demo that shows a mode nobody
  runs by default, which is the trimmed output the anti-goals refuse.
- Trim on camera. Refused above.

**What the table compares** (retrace: what running it again, lldb and ASan each say). Settled
from MANIFESTO, 2026-11-20.
- **(chosen) What says no to this code, measured on the demo app:** `cargo build` compiles,
  `cargo clippy -- -D warnings` is clean, and a test that posts each order once passes.
  `charpente test --sim` says order N was charged twice and gives the seed and the fix. This is
  MANIFESTO §3's argument applied to one bug: the framework that wins is the one that says
  *no* fastest and most precisely, and "a reproducible simulation failure with a seed beats a
  flaky staging repro". It is §6's split too. Types are the oracle for business logic's shape,
  and the simulator is the oracle for how it behaves under faults, which is where this bug
  lives. Every row is the reader's own first tools, with no claim about another stack.
- (refused) The tests a developer would write on a conventional stack, run for real. MANIFESTO
  §8: "Porting a Postgres app here misses the point." And how the port is written decides
  whether it even has the bug (§2).

## 5. The tape

A `vhs` tape, `docs/demo/double-charge.tape`, rendered to GIF and MP4 next to it. It follows
retrace's `.superpowers/sdd/2026-11-17-hnprep/visits.tape` (in `~/Documents/GitHub/retrace`):
1600×900, Menlo 26, 35 ms typing, narration typed as `#` comments, and setup done off camera
(clone, warm build) behind `Hide`. The tape's header says every line is real output from the
released crates.

The storyboard, about 30 seconds:

```text
$ # An order API: POST /orders/:id charges a card through a payment gateway.
$ charpente test --sim
<the red block: double charge, order N charged 2 times, the fix named, seed, repro>
$ # The reply was lost, the client sent the order again, and it was placed twice.
$ charpente test --sim --seed 0x…
<the same block, byte for byte>
```

The builder may end on the explanation, as retrace does, or add the three-line fix and a green
run if it stays under about 40 seconds.

The seed (my call): one master seed is fixed before any run, in the demo's pin test, as
merge-fixture fixes `0x4D455247`. The tape types it on screen (`charpente test --sim
--master-seed 0x…`), so nothing is hidden, and it is never re-rolled for a nicer block. The
replay is the command the red output prints, typed verbatim: the shrunk run's repro line, or
the batch's rerun line, whichever is shorter on screen. README quotes the same seed.

The video is uploaded through GitHub's web UI as a user attachment, as retrace's README does
(`1ff15919`). Noah does that by hand.

## 6. The output cleanup

The acceptance test is the tape's frame. The whole red output of `charpente test --sim` on the
demo app, failure first, fits one 1600×900 frame at the tape's settings, without scrolling the
failure off. By estimate that frame holds about 23 rows of 94 columns, and the tape types two
lines first. The builder confirms the real budget by rendering a frame. Today the output is 19
lines, which wrap to 31 rows at 94 columns, and its longest line is 314 characters.

The fit is a gate for the tape's seed: its block is pinned whole. It is also measured over 32
other master seeds, with the worst row count recorded in the ledger. The cleanup's rules are
general, so the other seeds should fit too, but they aren't gated (my call).

What's in the way today, from the measured output (`human_failure_block` and the skipped-runs
summary in `crates/charpente-sim/src/harness.rs`):
- **A near-duplicate line.** `before shrinking:` repeats the whole 300-character message
  because the order number differs (2 against 4). Its purpose is to show when shrinking turned
  one failure into another. A failure whose message differs only in numbers is the same
  failure. Print the line when the failure changed kind, not when an id changed.
- **The skipped-runs detail on a red batch.** Seven lines explain why seven runs were skipped,
  and two of them say "durable corruption". A viewer reads that as charpente corrupting data;
  it is injected disk corruption. Every skip reason names its fault as injected. On a red
  batch, the skipped detail is condensed and placed after the failure. On a green batch it
  stays in full, because a green with many skips is where it matters (0.4.0: a check that
  tested nothing fails).
- **The shrink line's wording.** "1 draws to 1, faults off: …" reads as noise. Say it plainly,
  for example "shrunk to 1 input with 5 faults off, in 10 replays".
- **The app's own message** is the demo app's text, not the harness's. The demo writes it
  short and concrete.

What stays:
- The `fired:` keys, which are the same words a check's `require:` and `floor:` use.
- The seed, the repro line (one line, never hard-wrapped, so it pastes), the master seed, and
  the rerun line.
- The JSON format, and the recorded batches `oracle5.sh` diffs, which are JSON and must not
  move.

It ships as **0.6.0**, with GUIDE §8 and every pinned text in the same commits. DECISIONS §15
bumps the minor for any change to charpente's output, so this is not a patch release, and the
scaffold's `"0.5"` moves with it. Hard rule 5 makes error text product surface, so each change
is pinned by a test against the output, red first.

**The red block is printed by `charpente_sim::harness`, compiled into the demo's own test
binary, not by the CLI.** So "the tape runs the released crates" means the demo depends on
`charpente` and `charpente-sim` from crates.io. My calls:
- The demo pins `=0.6.0` and commits its `Cargo.lock`, so the tape's seed and README's quoted
  output replay for any reader, whatever ships later.
- It sits outside the workspace with its own `[workspace]` table.
- Its workspace pin test copies it and patches crates.io to the workspace's crates, as
  `crates/charpente-cli/tests/new.rs` does for the scaffold. That keeps the bug caught as the
  code moves. README's quoted output is tied to 0.6.0 and says so.

## 7. The README rewrite

retrace's shape (see its README.md as of `16149ad0`):

1. `# charpente`, then one plain sentence saying what it is. A Rust backend in one binary:
   server, database, jobs and auth. It comes with a simulator that runs the whole app through
   crashes, lost replies and outages, and every failure replays from its seed.
2. The video.
3. **Example: a double charge.** The app in a few lines, the table, the two commands with their
   real output, and what happened.
4. **Install.** `cargo install charpente-cli`, Rust 1.99 or newer.
5. **Use.** `new`, `test --sim`, `test --mutants`, keeping README's scaffold step that
   `crates/charpente-cli/tests/new.rs` pins (delete the `tx.enqueue` line, red with a seed), or
   moving that pin to the new text.
6. **How it works.** One log, one commit point, the deterministic core, in a short paragraph
   with links to DESIGN.md.
7. **Limits.** These are plain sentences: one box with no sharding, Rust only, and a
   technical preview. They also say what the sim doesn't run. The rehearsal's testimony census
   found README saying the sim drives the whole app, when the edge server never runs in it.
8. The docs table.

These go or move to their own docs: the etymology, "The three guarantees", "Why now",
"Scaling stance" (one line in Limits and a link), and "Status" (one line).

## 8. Order of work

0. The records of §9.1: PLAN's F1/F2 triggers, and the launch demo spec's status line.
1. The demo app, `examples/double-charge/`, with the bug, its sim property and the workspace
   test that pins its red block.
2. The output cleanup, red first, with GUIDE §8.
3. The table's rows, each measured on the demo app (§4): build, clippy, a test that posts each
   order once, and the sim.
4. The README rewrite, before the publish, because README is the crates.io page for
   `charpente` and `charpente-cli` (`readme = "../../README.md"`). Its quoted output is
   generated at the release commit, which is the same code crates.io will get, and the
   post-publish smoke checks it against the installed crates.
5. 0.6.0 on crates.io. The publish is Noah's.
6. The tape, recorded against the installed 0.6.0.
7. Noah uploads the video, and the link goes into README on GitHub. crates.io doesn't render
   the video and gets the link with the next release.

"An outage a sim can place" (PLAN's next oracle-density row) comes after this project, not
alongside it. Both touch `charpente-sim`, and a recorded batch that moves must have one named
cause. The demo's exact pin keeps its seed replaying when that row lands (my call).

## 9. Decided

1. **The tape replaces the head-to-head as the launch's lead** (Noah, 2026-11-20). As a
   result:
   - PLAN's F1/F2 rows lose their "before the launch demo's public attempt" trigger and go
     back to ordinary v1.x rows.
   - The launch demo spec's status line records that its head-to-head no longer leads.
   - README carries no rehearsal paragraph. Its 0-against-0 result is material for a later
     post (my call).
2. **The table compares what says no, not stacks** (§4, settled from MANIFESTO §3, §6 and §8).
3. **The look:** the tape reuses retrace's theme, so Noah's projects share one look (my call).

## 10. Left to the builder

- The demo app's exact code and message, within §2 and §3.
- The exact rule for "changed kind" in `before shrinking:`, the condensed skipped line's text,
  and the shrink line's words, within §6's acceptance test.
- Whether the tape ends on the explanation or on the fix (§5).
- The demo directory's workspace wiring, as long as `cargo test --workspace` stays green and
  the red block is pinned.

## 11. Constraints

- CLAUDE.md applies: red-first fixes with pinned seeds, commits straight to main with no
  Co-Authored-By trailer, and docs/BENCH.md as the only source of quotable performance
  numbers.
- No employer, past or present, appears anywhere, and outward contact is noah@noahmitchem.com.
- The tape runs the released crates, both the `charpente` binary and the harness compiled into
  the demo (§6). Nothing on screen comes from a local build.
