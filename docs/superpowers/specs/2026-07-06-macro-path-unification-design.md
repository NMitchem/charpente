# M0 step-8 pre-work — macro-path unification: design

Status: approved by Noah 2026-07-06 (direction and emit-path shape both his call).

## Goal

Close the step-7 open note before the demo app bakes the wrong shape into public docs:
apps using `#[table]` currently need BOTH `charpente` and `charpente-core` as
dependencies, because `#[table]` and `#[job]` expand to `::charpente_core::` paths while
`#[route]` expands to `::charpente::` paths (surfaced building `crates/sim-fixture`,
step-7 T6). After this change, an app crate depends on `charpente` alone, and all three
macros resolve the same way.

## Decision: emit `::charpente::core::…` through the existing public re-export

`charpente` already ships `pub use charpente_core as core;` (`lib.rs:5`). `#[table]` and
`#[job]` switch every emitted `::charpente_core::` path to `::charpente::core::` — no new
surface, and the paths agents read in expansions are real, documented, public API. This
makes `charpente::core` officially load-bearing: it is the stable resolution root for
macro expansions and cannot be renamed or hidden without a major version.

Rejected: a serde-style `#[doc(hidden)] pub mod __macro` re-export module. It keeps
`charpente::core` free to change, but adds a parallel surface that drifts, and hides
paths agents will read anyway — against the introspectability stance (DESIGN §7).

Rejected: keeping two-dep and documenting it. Requiring `charpente-core` in every app
`Cargo.toml` leaks internal layering into the public shape forever; a breaking change
later versus a small change now.

## The pieces

1. **Macro emission** (`charpente-macros/src/table.rs`, `job.rs`): every
   `::charpente_core::` path becomes `::charpente::core::` (~17 sites). `#[route]` is
   already correct and does not change. The crate-doc header (`lib.rs`) is rewritten:
   all three macros emit `::charpente::` paths; the using crate depends on `charpente`
   alone.

2. **Self-resolution inside charpente** (`charpente/src/lib.rs`): add
   `extern crate self as charpente;` so charpente's own `#[table]` sites
   (`http/app.rs` Session, `kernel/cron.rs` cron row) keep compiling — the standard
   idiom for consuming your own derive surface in-crate. Side effect, wanted: the
   documented "`#[route]` works in dependent crates, not in charpente's own src"
   limitation disappears; the CLAUDE.md sentence recording it is updated.

3. **The discriminating proof** (`crates/sim-fixture`): drop `charpente-core` from
   `[dependencies]`; change the one import (`tests/sim.rs`:
   `use charpente_core::io::Entropy` → `use charpente::core::io::Entropy`). The fixture
   is now compiled evidence of the single-dep contract — any expansion still referencing
   `::charpente_core::` breaks the workspace build.

4. **Docs in the same commit** (hard rule 6): DESIGN §3 gains one sentence — app crates
   depend on `charpente` only; macro expansions resolve through `charpente::core`.
   CLAUDE.md's step-8 open note is resolved and the current-state log updated.

## Deliberately unchanged

- `charpente`'s own integration tests keep their direct `charpente_core::` imports —
  charpente legitimately depends on core; minimal diff.
- No `crate = "…"` override attribute on the macros (the serde renamed-dep escape
  hatch). No use case until someone renames the dependency; YAGNI. Add it when a real
  consumer needs it, not before.
- `charpente-core` itself: untouched. It never uses the macros (manual impls; see
  `job.rs:80` "core cannot use the #[table] macro (macros depend on core)").
- No trybuild suite in this change — macro error-message tests remain the post-step-8
  follow-up, prioritized by what the demo agent actually trips on.

## Risk

Compile-time-only: no runtime path changes, nothing sim-visible. The one subtle piece is
`extern crate self as charpente;` — stable Rust, edition-2024-clean, affects name
resolution only inside `charpente` itself. If it ever collides with a future dependency
literally named `charpente`, that dependency cannot exist (crates.io name is ours).

## Verification

- `cargo test --workspace` green — includes the compile-time proof (sim-fixture builds
  with one dep) and every pinned regression seed.
- `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all --check`
  clean.
- CLI dogfood: `charpente test --sim` against `crates/sim-fixture`, fresh master seed,
  full batch green — proves the built artifact end-to-end, not just the build.
- Grep gate: no `::charpente_core::` remains in `charpente-macros/src/`.

This change is macro/docs surface, not core: the evidence bar is the workspace suite
plus the dogfood batch, not a fresh N-seed sim campaign (nothing here executes under the
fault matrix differently than before).
