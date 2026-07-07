# Macro-Path Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `#[table]` and `#[job]` emit `::charpente::core::` paths so app crates depend on `charpente` alone (spec: `docs/superpowers/specs/2026-07-06-macro-path-unification-design.md`).

**Architecture:** Mechanical path flip in the two proc macros, `extern crate self as charpente;` so charpente's own `#[table]` sites keep resolving, and `sim-fixture` drops to one dependency as the compiled proof. Compile-time-only; no runtime or sim-visible behavior changes.

**Tech Stack:** Rust workspace (edition 2024), proc macros (`syn`/`quote`), cargo, BSD sed (macOS).

## Global Constraints

- Hard rule 6: the macro change and ALL doc updates (macros crate header, DESIGN §3, CLAUDE.md) land in ONE commit.
- Commit directly to `main`; NO Co-Authored-By trailer (CLAUDE.md workflow).
- Verification before the commit, never after: workspace tests, clippy `-D warnings`, `fmt --check`, CLI dogfood batch, pinned regression seed.
- Do not touch `charpente`'s integration tests (`crates/charpente/tests/*.rs`) — their direct `charpente_core::` imports are legitimate (charpente really depends on core) and out of scope.
- Do not add a `crate = "…"` override attribute to any macro — explicitly a non-goal in the spec.

---

### Task 1: Single-dep contract, red → green, one commit

**Files:**
- Modify: `crates/sim-fixture/Cargo.toml` (drop `charpente-core` dep)
- Modify: `crates/sim-fixture/tests/sim.rs:6` (import path)
- Modify: `crates/charpente-macros/src/table.rs` (9 path sites)
- Modify: `crates/charpente-macros/src/job.rs` (8 path sites)
- Modify: `crates/charpente-macros/src/lib.rs:5-6` (crate-doc header)
- Modify: `crates/charpente/src/lib.rs` (add `extern crate self`)
- Modify: `DESIGN.md` §3 (single-dep sentence)
- Modify: `CLAUDE.md` (resolve the step-8 open note; update the step-5 `#[route]` sentence)

**Interfaces:**
- Consumes: `pub use charpente_core as core;` — already at `crates/charpente/src/lib.rs:5`, do not re-add.
- Produces: the stable macro-resolution contract — every `#[table]`/`#[job]`/`#[route]` expansion resolves only `::charpente::…` paths; `charpente::core` is officially load-bearing public API.

- [ ] **Step 1: RED — make sim-fixture single-dep (this is the failing test)**

In `crates/sim-fixture/Cargo.toml`, delete the line:

```toml
charpente-core.workspace = true
```

(leaving `charpente.workspace = true` as the only `[dependencies]` entry; `[dev-dependencies] charpente-sim` stays — the sim harness is a dev tool, not part of the app's runtime dep story).

In `crates/sim-fixture/tests/sim.rs`, change line 6:

```rust
// before
use charpente_core::io::Entropy;
// after
use charpente::core::io::Entropy;
```

- [ ] **Step 2: Run the build to verify it fails for the right reason**

Run: `cargo build -p sim-fixture --tests 2>&1 | head -30`

Expected: FAIL with `failed to resolve: use of unresolved module or unlinked crate 'charpente_core'` (or closely similar wording), with the error span pointing at `#[table]` on `FixtureCounter` in `crates/sim-fixture/src/lib.rs` — the expansion still references `::charpente_core::`. This failure is the recorded proof of the two-dep wart. If it fails for any *other* reason, stop and reassess before proceeding.

- [ ] **Step 3: GREEN part 1 — flip the emission paths in both macros**

Run (BSD sed, macOS):

```bash
sed -i '' 's/::charpente_core::/::charpente::core::/g' \
  crates/charpente-macros/src/table.rs \
  crates/charpente-macros/src/job.rs
```

Spot-check the result — `crates/charpente-macros/src/table.rs` around line 105 must now read:

```rust
        impl ::charpente::core::table::Row for #name {
            const TABLE: &'static str = #name_str;
            const INDEXES: &'static [::charpente::core::table::IndexDef<Self>] = &[
                #(::charpente::core::table::IndexDef {
```

and `crates/charpente-macros/src/job.rs` around line 188:

```rust
        impl ::charpente::core::db::Job for #name {
            const NAME: &'static str = #name_str;
            const RETRY: ::charpente::core::job::RetryPolicy = ::charpente::core::job::RetryPolicy {
```

(the pre-existing `::charpente::kernel::…` / `::charpente::Tx` lines in `job.rs` are already correct and must be untouched by the sed).

In `crates/charpente-macros/src/lib.rs`, replace the header lines 5–6:

```rust
// before
//! `#[table]` references `::charpente_core` paths; `#[route]` references
//! `::charpente` paths — the using crate must depend on the respective crate.
// after
//! All three macros reference `::charpente` paths (`charpente::core` for the
//! deterministic core) — the using crate depends on `charpente` alone.
```

- [ ] **Step 4: GREEN part 2 — self-resolution inside charpente**

In `crates/charpente/src/lib.rs`, insert after the crate-doc comment (before line 5's `pub use`):

```rust
// The macros emit `::charpente::` paths; this alias makes them resolve in
// charpente's own src too (Session, CronRow), not just in dependent crates.
extern crate self as charpente;
```

- [ ] **Step 5: Run the fixture build to verify green**

Run: `cargo build -p sim-fixture --tests`
Expected: PASS, no warnings.

- [ ] **Step 6: Grep gate — no stale paths in the macro sources**

Run: `grep -rn '::charpente_core::' crates/charpente-macros/src/ ; echo "exit=$?"`
Expected: no matches, `exit=1`.

- [ ] **Step 7: Docs, same commit (hard rule 6)**

`DESIGN.md` §3 ("The typed surface") — append one sentence to the section's opening paragraph, so it reads:

```markdown
Every contract in the system is a Rust item; every contract violation is a compile
error. App crates depend on `charpente` alone: all three derive macros resolve through
`::charpente::` paths (`charpente::core` re-exports the deterministic core), so the
platform is one dependency, not two.
```

`CLAUDE.md` — two edits (grep for anchor text first; wrapping may differ from what's shown):

(a) Replace the "**Open note for step 8**" bullet (anchor: `Open note for step 8`) with:

```markdown
- **Macro-path unification done (2026-07-06, spec docs/superpowers/specs/
  2026-07-06-macro-path-unification-design.md)**: `#[table]`/`#[job]` now emit
  `::charpente::core::` paths (through the public `charpente::core` re-export, which is
  officially load-bearing), matching `#[route]` — app crates depend on `charpente`
  alone. `extern crate self as charpente;` in charpente's lib.rs makes all three macros
  usable in charpente's own src. `sim-fixture` is single-dep: any expansion regression
  to `::charpente_core::` breaks the workspace build.
```

(b) In the step-5 bullet, update the `#[route]` limitation sentence (anchor: `so it works in dependent crates`):

```markdown
// before
`#[route]` emits
  `::charpente` paths, so it works in dependent crates (and integration tests), not in
  charpente's own src.
// after
`#[route]` emits
  `::charpente` paths (all three macros do, since 2026-07-06's macro-path unification,
  which also made them usable in charpente's own src via `extern crate self`).
```

- [ ] **Step 8: Full workspace verification**

Run: `cargo test --workspace`
Expected: all green — this includes charpente's own `#[table]` sites (Session, CronRow) compiling through the new paths, every integration suite, and the pinned in-suite regression seeds.

- [ ] **Step 9: Lint and format**

Run: `cargo fmt --all && cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`
Expected: fmt makes no or trivial re-wrap changes (the sed lengthened lines inside `quote!` blocks; if it re-wraps, keep the result), then both checks clean.

- [ ] **Step 10: CLI dogfood — the end-to-end proof**

Run: `cargo run -p charpente-cli -- test --sim --dir crates/sim-fixture --universes 1024 --jobs 8`
Expected: exit 0, master seed printed at startup, all universes green, coverage requirements met. Record the printed master seed for the commit message.

Run: `CHARPENTE_SEED=0x8bee1752811ae873 cargo test -p charpente-sim --test crash_recovery`
Expected: green (pinned torn-write regression seed replays clean).

- [ ] **Step 11: One commit, code + docs together**

```bash
git add crates/charpente-macros/src crates/charpente/src/lib.rs \
  crates/sim-fixture/Cargo.toml crates/sim-fixture/tests/sim.rs \
  Cargo.lock DESIGN.md CLAUDE.md
git commit -m "macros: single-dep apps — #[table]/#[job] emit ::charpente::core paths (step-8 pre-work)

Apps depend on charpente alone; charpente::core is officially load-bearing.
extern crate self as charpente makes all three macros work in charpente's own
src; sim-fixture drops charpente-core and is the compiled proof. Spec:
docs/superpowers/specs/2026-07-06-macro-path-unification-design.md.

Evidence: cargo test --workspace green; clippy/fmt clean; 1024-universe
fixture dogfood green (master seed 0x<record it>); pinned crash-recovery
seed 0x8bee1752811ae873 green."
```

(Substitute the actual master seed from Step 10. `git status` must be clean afterward except `.superpowers/` scratch, if any.)
