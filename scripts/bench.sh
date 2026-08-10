#!/usr/bin/env bash
# The honest-benchmark instrument (M2 sub-project 1, spec
# docs/superpowers/specs/2026-08-06-benchmark-harness-design.md).
#
# Default: the full matrix — device, core, kernel, and the wire sweep against
# BOTH targets — assembled into one report. JSON goes to
# target/bench/report.json; the human render goes to the terminal.
#
# --smoke: the harness's own correctness checks, not a measurement. Runs the
# #[ignore]d tests, which spawn processes, bind loopback ports and build
# crates. `--test-threads=1` because they bind real ports and drive real
# processes, exactly as scripts/edge-harness.sh does. The crate's un-ignored
# tests (the pure accounting/rendering unit tests, plus T9's two forced-arm
# tests) are NOT run here — they run in the ordinary `cargo test --workspace`
# pass.
#
# --release matters and is not incidental: `bench` measures charpente, and a
# dev build's `reads_per_sec` and kernel/wire numbers are not the ones anyone
# should publish. Note the artifact records which profile produced it
# (`build_profile`), and that this workspace pins redb's debug-assertions ON
# even in release (the 2026-07-15 mode-skew fix), which the report's own
# methodology header states.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [[ "${1:-}" == "--smoke" ]]; then
  exec cargo test -p bench-harness -- --ignored --test-threads=1
fi
mkdir -p target/bench
cargo run --release -p bench-harness --bin bench -- all --json "$@" > target/bench/report.json
echo "JSON report: target/bench/report.json" >&2
