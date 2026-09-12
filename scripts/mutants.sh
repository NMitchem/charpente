#!/usr/bin/env bash
# Runs the scoped mutation instrument: `charpente test --mutants` over the
# oracle-bearing target set (the ruling is in
# docs/superpowers/specs/2026-09-06-oracle-density-design.md, "Mutation mode,
# ruled"). Like scripts/edge-harness.sh this is an expensive rig that lives
# OUTSIDE the deterministic boundary and is not part of `cargo test
# --workspace` — it patches a scratch copy of the tree and rebuilds per mutant.
#
# Why it is scoped rather than run over everything: the whole workspace is
# 4193 mutants (measured 2026-09-07), which is days of wall clock. An
# instrument nobody runs reports nothing, so the default is a named target set
# and anything wider is an explicit --file.
#
# Scope is part of the claim. cargo-mutants runs only the MUTATED package's own
# tests unless told otherwise, and most of charpente-core's oracle coverage
# lives in charpente-sim and charpente — so the verb passes --test-package for
# you. Measured 2026-09-07: the same `||`->`&&` mutant in parse_segment_name is
# MISSED under the default package scope and CAUGHT with charpente-sim's tests
# in scope. Same mutant, two answers, decided by which suites were allowed to run.
#
# Requires the pinned tool:
#   cargo install cargo-mutants --version 27.1.0 --locked
#
# Known-equivalent mutants are hidden by name in .cargo/mutants.toml, each
# with its argument; crates/guide-examples/tests/mutants_exclusions.rs pins
# the count. A MISSED is a property to write, never an entry to add.
#
# Exit codes are the verb's: 0 every viable mutant caught, 1 a finding
# (mutants MISSED) or an unresolved run (TIMEOUTS — a timeout is not a catch),
# 2 usage, infrastructure, or a baseline that was not green.
#
# Any extra arguments are passed through, so a narrow run is:
#   scripts/mutants.sh --file crates/charpente-core/src/log.rs --re parse_segment_name
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
cargo build -p charpente-cli
# An exported CARGO_TARGET_DIR moves the build; exec from wherever cargo put it
# rather than from ./target, which under that variable holds nothing.
exec "${CARGO_TARGET_DIR:-./target}/debug/charpente" test --mutants "$@"
