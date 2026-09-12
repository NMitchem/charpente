# Mechanical fences: turning this repo's review-enforced invariants into machine-enforced ones

## Context

The 2026-09-09 autonomous run (`docs/superpowers/plans/2026-09-09-autonomous-run.md`) shipped
two defect fixes and, in the course of reviewing them, produced a cluster of findings that all
share one shape. Each names an invariant that currently holds because a reviewer checked it,
in a repo whose first hard rule exists specifically to forbid that arrangement:

> **Enforce mechanically (clippy `disallowed_methods`/`disallowed_types` deny-list in CI from
> the first commit), not by review.** — CLAUDE.md, hard rule 1

The findings were correctly deferred as Minors at the time: none blocked a merge, and the
final whole-corpus review triaged them explicitly. But "not blocking" and "fine to leave" are
different claims, and one of them — the panic-hook fence — was promoted by that review as
worth fixing soon, in these words: the invariant "is review-enforced in a repo whose hard rule
1 forbids that."

This sub-project closes them. It is small, and deliberately so: it adds no feature and changes
no runtime behaviour. Its output is oracles.

## §1 What this is really about

MANIFESTO §2 argues the product is the oracle substrate, because agent output quality tracks
oracle density. §6 sharpens it: "the framework is the reviewer." Every item below is a place
where this repo currently spends a *human or agent review* on something a machine could refuse
in milliseconds — which is the resource the manifesto says is scarce, spent on the thing it
says is cheap.

There is a second, less abstract argument. The panic-hook invariant was established two days
ago and is already documented in three places (a rationale comment in `charpente-core`, a
review file, and a ledger entry). None of those is checkable. The next agent to touch the
simulator will not read them.

## §2 The items, each with the finding that produced it

### Item 1 — `std::panic::set_hook` is not on the deny-list (the promoted one)

**Evidence.** `clippy.toml` contains zero entries for `set_hook` or `take_hook`. The workspace
has exactly one legitimate call site, `crates/charpente-sim/src/harness.rs:229-230`.

**Why it matters.** The F1 fix's whole safety argument is that the process-global panic hook is
installed in exactly one place, by the simulator, and that `charpente-core` never touches it —
because a second installer, or an installer inside the deterministic boundary, silently breaks
the filter and can mute a real panic. That argument is currently a paragraph in a doc comment.
`charpente-core/src/table.rs:804` even *explains* the hazard in prose while nothing prevents it.

**Shape of the fix.** Deny-list entries for `std::panic::set_hook` and `std::panic::take_hook`,
with a site-local `#[allow(clippy::disallowed_methods)]` plus rationale at the one sanctioned
site — the same pattern `clippy.toml`'s own header prescribes for production trait impls.

**The test that must exist.** A deny-list entry with no failing case is decoration. The
sub-project must show clippy actually refusing a second installer before the entry is
considered done.

### Item 2 — `install_open_fence_hook` documents itself as "Idempotent" and is not

**Evidence.** `crates/charpente-sim/src/harness.rs:212` says "Idempotent — call it from any
entry point." It is wrapped in a `std::sync::Once`, which makes it install-once-**ever**, not
idempotent. Those differ in the case that matters: if anything installs a hook afterwards, the
`Once` has already fired and the filter is silently gone.

**Why it matters.** This is the "docs the code disproves" class, which CLAUDE.md names as "the
class this repo keeps paying for" — and here the doc is describing a *safety property* of a
concurrency mechanism. An agent reading "idempotent" will reasonably conclude re-calling
restores the filter. It does not.

### Item 3 — `CHARPENTE_F1_CHILD` is set but never read

**Evidence.** One `.env(...)` call sets it; zero `var(...)` calls read it. Confirmed by count.

**Why it matters.** Dead scaffolding that looks load-bearing. It was plan-mandated — I put it
in the brief as an attribution marker and then the design moved on without it. No per-task
review caught it; the whole-corpus review did.

### Item 4 — GUIDE quotes error strings that nothing mechanically ties to the code

**Evidence.** GUIDE's copies of error text sit in ```` ```text ```` blocks, which
`guide-examples`' build.rs does not compile (it extracts ```` ```rust ````). The scope-flip
strings happen to be byte-exact today — re-verified during the run — but only because someone
looked.

**Why it matters most of the four.** "Docs the code disproves" is this repo's self-named
recurring defect, and it has now been found by reading rather than by a gate at least four
separate times: GUIDE's seed-a-brand-new-follower paragraph, three BENCH.md defects, DESIGN's
promote-path claim, and — during this very run — three independent baselines each
independently flagging GUIDE §10 as contradicting shipped behaviour. Hard rule 5 makes error
text product surface; hard rule 6 makes doc drift a bug. Neither is currently checkable.

**Shape of the fix.** A `guide-examples` test that extracts quoted error strings from the docs
and asserts each appears byte-exact in a source file or a pinning test. This is the one item
with real design content, and §4 records what it must not become.

### Item 5 — `charpente test --mutants` lets `--jobs N` silently produce wrong verdicts

**Evidence, produced by this repo's own mutation sweep and then re-verified by the
controller.** With an externally-set `CARGO_TARGET_DIR` and `--jobs 4`, cargo's per-crate
filename hash does not vary by mutation content, so concurrently-built scratch trees write to
one output path and a mutant's tests can run against a different mutant's artifact. The sweep
caught it by refusing a MISSED that contradicted the code, reading the mutant's own build log,
and finding one rlib hash shared across 21 of 36 mutants' logs including the baseline's.
Re-running that single mutant at `--jobs 1` flipped the verdict to CAUGHT with nothing else
changed.

**Why it belongs in this sub-project and not a later one.** Every other item here adds an
oracle. This one *protects the instrument that finds oracles*. A mutation tool that reports
false MISSED under a documented-and-supported flag combination makes all future sweeps
unfalsifiable — and the failure is silent: no error, no warning, just a wrong answer that
looks exactly like a real finding.

**The combination is reachable by following this repo's own advice.** CLAUDE.md's worktree
hygiene calls for a dedicated `CARGO_TARGET_DIR`; `crates/charpente-cli/src/mutants.rs:38,146`
accepts `--jobs N`; `scripts/mutants.sh` sets neither and warns about neither. Nothing tells
the operator these interact.

**Shape of the fix.** The verb refuses or neutralises the combination — either erroring when
`--jobs > 1` is passed with an externally-set `CARGO_TARGET_DIR`, or unsetting it per job so
cargo-mutants' own isolation works. Error text per hard rule 5: name the contract, the site,
and the fix. **The failing case must be observed** — a parallel run producing a MISSED that a
serial run catches — before the fix is considered done.

### Item 6 — DESIGN.md §9's continuation lines are unindented

Cosmetic, already on `main`, introduced by the previous run's own plan text. Rides along.

## §3 What this sub-project refuses to do

- **No new runtime behaviour.** If an item requires changing what the binary does, it is out of
  scope and gets its own sub-project.
- **No fixing the deferred Minors the final review said can stand** — `table.rs`'s own
  "panicked at open" text (correctly loud), `scan_by`'s silent `Ok([])` (pre-existing and
  symmetric, door now covered), `:981`'s event table (type-enforced for the class that
  matters). They were triaged deliberately; re-litigating them here would be scope creep
  dressed as tidiness.
- **No expansion of the deny-list beyond Item 1.** A deny-list audit is a reasonable
  sub-project; it is not this one, and bundling it would hide Item 1's failing-case evidence
  in a pile.

## §4 The trap Item 4 must avoid, stated before anyone builds it

A doc-truth gate is the kind of oracle that most easily becomes decoration. Three specific
failure modes, each of which this repo has already produced in some form:

1. **A gate that matches nothing and passes.** If the extractor's pattern is wrong, it finds
   zero strings, asserts nothing, and is green forever. The gate must therefore assert a
   *floor* — it found at least N strings — and that floor must be checked against a hand count
   once, at build time.
2. **A gate whose corpus silently shrinks.** If a doc is reformatted so the extractor stops
   recognising a block, coverage drops and nothing notices. Same remedy: the floor.
3. **Prose coupling that reddens for the wrong reason.** Two places in this repo deliberately
   couple prose to code (redb's remedy text in the latch stop message, and `checks::classify`),
   so a reword reddens locally by design. The gate must distinguish "the doc lies" from "the
   doc was rephrased", or it will train people to edit the test.

The RED-first rule binds: each item ships only after its failing case has been *observed*, not
argued. An item whose baseline cannot be made to fail is dropped and recorded as dropped —
that outcome is a result, not a setback.

## §5 Exit criteria

1. `clippy.toml` denies `set_hook`/`take_hook`; the one sanctioned site carries a site-local
   allow with rationale; **a second installer has been observed being refused** by
   `cargo clippy --workspace --all-targets -- -D warnings`.
2. `install_open_fence_hook`'s doc states what `Once` actually gives, naming the
   install-once-ever behaviour and what happens if a later hook is installed.
3. `CHARPENTE_F1_CHILD` is either read or removed, with the choice stated.
4. The doc-truth gate exists, carries a floor, has been observed RED against a deliberately
   falsified doc string, and green after — or is documented as dropped with its transcript.
5. `charpente test --mutants` refuses or neutralises `--jobs > 1` with an externally-set
   `CARGO_TARGET_DIR`, with the false-MISSED case observed before the fix and absent after.
6. `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and
   `cargo test --workspace --no-fail-fast` all green, with the `test result: ok` count
   reconciled exactly against the 82 measured on `62e3c0c`.
7. No runtime behaviour changed: the diff touches `clippy.toml`, doc comments, docs, and test
   files only — or names precisely what else it touched and why.
