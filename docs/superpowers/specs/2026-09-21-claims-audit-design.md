# Claims audit — design

**Date:** 2026-09-21. **Status:** approved in chat (Noah), spec committed for the plan.
**Predecessor:** the 2026-09-13 roadmap ruling (docs/LEDGER.md "complete defined") that
defined M3 — Launch and named this as its fourth sub-project, and exit criterion (c):
"every number in README.md and MANIFESTO.md is in docs/BENCH.md or absent." Runs beside
sub-project 3 (the status-page build, spec `2026-09-17-third-docs-only-build-design.md`);
touches no crate that build compiles.

## Context

CLAUDE.md says docs/BENCH.md is the only source of quotable performance numbers, and
README.md says every number in it will be "measured or gone" before launch. Neither is
true yet, and nothing checks either. An evidence sweep on 2026-09-17
(`.superpowers/sdd/2026-09-17-claims-audit/sweep.md`, gitignored; its census table is
reproduced in the ledger entry this sub-project writes) found three kinds of number in the
two launch documents: charpente measurements with provenance in BENCH.md (215/sec, ~31×);
a charpente claim with none — "hundreds of thousands of transactions per second", spelled
in words so a digit grep walks past it, at README.md:85-86 and MANIFESTO.md:108-109,
which BENCH.md:281-283 quotes only to contrast against the measured 215; and figures that
are not about charpente at all — MANIFESTO §1's account of an external compiler experiment
and §7's "99.9%" — which BENCH.md can never carry. README guarantee 1 names typed telemetry
as "an M2 item that does not exist yet"; M2 closed on 2026-09-13 without it, so the label
is now wrong on top of PLAN's objection that a guarantee should not name a backlog item.

PLAN.md's sub-project 4 also owes a verification: the helpdesk experiment's F1 — a raw
redb panic trace printed on a green run, which "demonstrably destroyed a competent agent's
work" — is to be verified closed against the helpdesk repro, not assumed closed because a
panic hook now exists (`566c7df`, merged 2026-09-10; one installer, pinned by `hook_census`).

## §1 Decisions already made (Noah, 2026-09-21)

1. **Rewrite now, run later.** The README scaling paragraph and MANIFESTO §7 are
   rewritten to what BENCH.md measured. The bare-metal Linux run owed since 2026-08-15 is
   not a gate: it moves to PLAN's "After launch" table with the trigger "resizes the
   README scaling paragraph when it lands".
2. **Exit criterion (c) reads: every number that is a claim about charpente is in
   docs/BENCH.md or absent; a figure about anything else cites its source or is reworded
   to carry no figure.** The literal "every number" reading would delete MANIFESTO §1's
   evidence; the untouched reading leaves an unsourced statistic in the launch document.
3. **A standing oracle, not a one-time sweep.** `crates/guide-examples/tests/claims_census.rs`
   holds (c) the way `doc_truth.rs` holds GUIDE's pinned blocks and
   `mutants_exclusions.rs` holds the equivalent-mutant list: an explicit table, an
   exemption list with one rationale per entry and a pinned count, and an error that names
   the three ways to fix a new number.
4. **MANIFESTO §1's compiler statistic: Noah's citation, or no figures.** No source was
   supplied with the approval. The plan's rewrite task carries both branches and records
   which one ran; if a citation arrives before that task, the sentence keeps its figures
   with the source inline.

## §2 The rewrites

Exact text. The plan copies these; a reviewer diffs against them.

**README guarantee 1** (`README.md:17-20`). The clause "and typed telemetry with
change-time checks is an M2 item that does not exist yet" is deleted, leaving:

> 1. **Typed end-to-end.** Schema, queries, routes, jobs, events, and wire types are all
>    Rust types checked at compile time — a schema change that breaks a query is a compile
>    error, not a 3am page; config is a typed struct with no startup validation yet.

**README scaling paragraph** (`README.md:83-92`). The two sentences from "A modern server
executes" through "not a measurement of what charpente does today." become:

> With no network hop between app and data, one box's ceiling is its disk's durable write
> rate, not a network round trip. What charpente measures today is 215 durable records/sec
> per connection on a dev laptop under `F_FULLFSYNC`, and up to ~31× that under group commit
> where the queue is deep enough to fill a batch ([docs/BENCH.md](docs/BENCH.md)). No
> bare-metal run exists; this paragraph is resized when one lands. You will not outgrow one
> box before you can afford to build something bespoke, and pretending otherwise is how
> platforms die building a worse Spanner instead of a better SQLite.

The sentence that follows ("Measured, an uncontended commit costs one durable fsync…")
stays. In the status paragraph (`README.md:126-133`), "roughly three orders of magnitude
below the headline figure" becomes "the figure the scaling paragraph above states", and
"and the bare-metal Linux numbers that would resize it do not exist yet" becomes "; no
bare-metal Linux run exists, and PLAN.md's after-launch table carries it".

**README's sim record** (`README.md:105-106`). "(master seed `0x483e230baed4922b`)"
becomes "(master seed `0x483e230baed4922b`; [docs/LEDGER.md](docs/LEDGER.md),
2026-07-07)". A sim-batch count is testimony with a seed, not a performance number; its
home is the ledger and the sentence now says where.

**MANIFESTO §7** (`MANIFESTO.md:106-109`). "with no network hop between logic and data, a
single modern server sustains hundreds of thousands of transactions per second — more than
almost any business will ever need" becomes "with no network hop between logic and data,
one server's ceiling is its disk's durable write rate, and that is more than almost any
business will ever need (what charpente measures is in docs/BENCH.md)". Line 114:
"Designing for the 99.9%" becomes "Designing for the overwhelming majority".

**MANIFESTO §1 and §2** (`MANIFESTO.md:9-11, 17-18`). Branch A (citation supplied): the
sentence keeps its figures and gains the source in parentheses after "Linux kernel".
Branch B (none supplied):

> That constraint is gone. In early 2026, a team of parallel AI agents built a C compiler
> that passes nearly all of the GCC torture suite and boots a Linux kernel, in weeks, for
> less than a junior engineer's quarter.

and §2's "the agents hit 99%" becomes "the agents passed nearly all of it".

## §3 The oracle: `claims_census.rs`

One test in `crates/guide-examples/tests/`, beside `doc_truth.rs`, following its
root-finding and file-reading idiom. It reads `README.md` and `MANIFESTO.md` and classifies
every **figure occurrence**: a digit token (`\d[\d,]*(\.\d+)?`, with a `~`, `$`, `×`, `x`
or `%` neighbour kept in the token) and every match of `WORD_FIGURES`, a list the test
carries (`hundreds of thousands`, `tens of thousands`, `thousands of`, `hundreds of`,
`sixteen`, `two weeks`, `orders of magnitude`, `nearly all`, `overwhelming majority` are
the seeds of it — the last two so a reworded rhetorical figure is still seen and has to be exempted on
purpose, with a rationale).

Each occurrence must satisfy exactly one of:

1. **Seed-bearing.** The occurrence's line, or the line after it, contains `master seed
   0x`. Testimony with a seed lives in the ledger; the rule needs no entry.
2. **A `CLAIMS` row.** `CLAIMS: &[(&str, &str, &str)]` — `(file, figure regex, BENCH.md
   line regex)`. The test asserts both halves: the figure is covered, **and** the BENCH
   regex matches at least one line of `docs/BENCH.md`. Removing a measurement from
   BENCH.md while README still quotes it fails here, in this direction, by name.
3. **An `EXEMPT` entry.** `EXEMPT: &[(&str, &str)]` — `(regex, rationale)`. Section refs
   (`§\d+`), milestone labels (`M[0-9]`), list markers (`^\d+\. `), the version (`0\.1`),
   dates (`20\d\d-\d\d-\d\d`), the year alone, hard-rule numbers, and the cited or reworded
   external figures of MANIFESTO §1. `EXEMPT_COUNT` pins the list's length the way
   `mutants_exclusions.rs` does; adding an entry means raising it in the same diff, on
   purpose.

The failure is one message per unclassified occurrence, then a panic (hard rule 5):

```text
claims_census: README.md:86 carries a figure with no provenance: "hundreds of thousands"
  a number in README.md or MANIFESTO.md is one of three things — a charpente measurement
  (add a CLAIMS row naming the docs/BENCH.md line that carries it), a seed-bearing sim
  record (write it beside its `master seed 0x…`), or not a claim about charpente (add an
  EXEMPT entry with its rationale and raise EXEMPT_COUNT). M3 exit criterion (c).
```

Written first and shown red against the pre-rewrite documents — it must name
"hundreds of thousands" at both sites, "orders of magnitude", MANIFESTO §1's five figures,
"99%" twice and "99.9%" — then green after §2 lands. (Guarantee 1's "M2" is a milestone
label, blanked by rule; that clause is a truthfulness fix, not a number.) `CLAIMS` after the rewrite is small: `215` in README's scaling and status paragraphs →
BENCH's `device durable appends/sec` line; `~31×`/`~31x` in the same two paragraphs →
BENCH's "recovers up to ~31x" line. A `CLAIMS` row whose BENCH regex matches nothing is
its own failure with its own message naming the row.

CLAUDE.md's `guide-examples` test list gains `claims_census`; `doc_inventory` enforces
that.

## §4 F1, verified against the helpdesk repro

The repro is pinned better than the sweep found: the ledger's 2026-09-03 entry ("Docs-only helpdesk build — findings carried over") records the
helpdesk's master seed `0x12124e6ebd9cf8b9` and the count — **134** `panicked at
…page_manager.rs` lines on a fully green 10k-universe batch. The helpdesk app cannot compile
at `main` (eleven default-secure refusals and the `acked_implies_durable` signature change
of `c6e1dcc5`), so the verification runs at the fix's own merge commit, `566c7df`, where
only the one-line signature call needs patching:

- **Control** — a disposable worktree at `566c7df^1` (main before the fix), its own
  `target/`, `examples/helpdesk/` overlaid from `8913038`, added to `members`, the one
  `acked_implies_durable(sim.seed(), …)` call rewritten to the `&mut sim` form. Run
  `cargo run -p charpente-cli -- test --sim --dir examples/helpdesk --universes 10000
  --master-seed 0x12124e6ebd9cf8b9`, stderr to a file. **Predicted before running:** the
  batch is green and `grep -c 'panicked at' stderr` is greater than zero — 134 if the fault
  stream is unchanged since 2026-09-05, some other positive count if a crate between
  `14f5a94` and `566c7df^1` re-keyed it. Zero here means the repro did not reproduce and
  F1 cannot be closed by this run.
- **Fix** — the same overlay and patch at `566c7df`. **Predicted:** green, and the count
  is exactly zero.
- **Pin at HEAD** — `git diff 566c7df main -- crates/charpente-sim/src/harness.rs` shows
  the hook installer unchanged, and `hook_census` holds "one installer" at HEAD.

Any further compile fix the overlay needs is limited to what does not touch the property
under test (`acked_durable_across_crashes` or its successor in the helpdesk suite) and is
listed in the ledger. Both counts, both seeds, and the exact commands go in the ledger; F1
is closed only if the control reproduces.

## §5 Docs, in the same commits (hard rule 6)

- PLAN.md: sub-project 4 `[shipped <date>]`; exit criterion (c) reworded per §1.2; the
  "After launch" table gains a row — **Bare-metal Linux BENCH run** | trigger: a Linux box
  Noah runs docs/BENCH.md's protocol on; resizes the README scaling paragraph when it
  lands.
- CLAUDE.md: `claims_census` in the guide-examples test list.
- docs/LEDGER.md: one entry — the census table, the rewrites, which §1.4 branch ran, the
  F1 counts and seeds, the oracle's red-then-green evidence.
- DECISIONS.md: untouched. The (c) reading is a PLAN clarification, not a reversal.

## §6 Refused, so the plan does not grow them

- **Running the Linux BENCH inside this sub-project.** Noah's call: rewrite now, run later.
- **Fuzzy quantity matching in the oracle** (deciding by itself that README's `215` and
  BENCH's `215` are the same measurement). An explicit `CLAIMS` row is the claim; the test
  checks that the claim is made, not whether it is plausible.
- **Auditing GUIDE.md or DESIGN.md.** Criterion (c) names two files; GUIDE's numbers are
  held by `doc_truth` where they are pinned.
- **Re-litigating F2 and F4** while the helpdesk overlay is open. F2 is fixed
  (`c6e1dcc5`); F4 was struck. The overlay exists for one grep.

## §7 Exit criteria

1. `claims_census` red against the pre-rewrite README/MANIFESTO at the sites §3 names,
   green after §2, with `EXEMPT_COUNT` pinned and every `CLAIMS` row resolving in
   BENCH.md.
2. The §2 rewrites landed verbatim, and the ledger says whether §1.4 ran branch A or B.
3. F1's control count (> 0) and fix count (= 0) in the ledger with their commands.
4. PLAN.md carries the shipped mark, the reworded (c), and the Linux row; CLAUDE.md's
   inventory is green.
