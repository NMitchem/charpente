#!/usr/bin/env bash
# Runs the edge-harness rig: real built binaries, real TCP, process/
# connection-level faults (M1 sub-project 6, edge T10). Every process-,
# socket- or filesystem-touching test in the crate is #[ignore] — not part of
# `cargo test --workspace`'s default pass, since each one builds release
# binaries and drives real processes. (The crate's zero-IO parse/scan unit
# tests are NOT ignored and so are NOT run here; they run in the ordinary
# sweep. Noah's ruling, 2026-07-25.)
# `--test-threads=1`: tests bind real loopback ports and spawn real
# processes; serializing avoids port collisions between tests.
# Env knobs: EDGE_ACKED_DURABLE_ROUNDS overrides the acked-implies-durable
# oracle's round count (tests/acked_durable.rs; default 40). Must be a
# positive integer — an unparseable or zero value panics naming the bad
# value rather than silently running the default.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
exec cargo test -p edge-harness -- --ignored --test-threads=1
