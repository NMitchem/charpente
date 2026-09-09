#!/usr/bin/env bash
# Takes the compile-latency reading unattended (spec
# docs/superpowers/specs/2026-09-08-compile-latency-reading-design.md).
#
# Why a runner exists at all: the instrument is finished, but the reading needs
# a quiet machine, and the quietest this box gets is while nobody is using it.
# That is also when macOS schedules Spotlight and Time Machine, so "unattended"
# is not automatically "quiet" — every attempt records its load trace so a lost
# night is diagnosable rather than mysterious.
#
# The retry rule is spec §4 and is NOT this script's judgement call: the binary
# classifies its own refusals and says so in its exit code.
#   0 accepted        -> stop, we have the reading
#   1 refused (env)   -> retry; the box was busy or slept
#   2 usage/launch    -> stop, operator error
#   3 refused (final) -> stop and REPORT; Unstable/BelowFloor/BuildFailed/
#                        ChildCpuNotAggregated describe the measurement, and
#                        rerolling them is how a reading gets manufactured
#
# caffeinate is belt and Invalid::Suspended is braces. Prevention exists so the
# night is not wasted, never so the gate is bypassed. Both stay armed.
#
#   scripts/compile-bench.sh                 # start now
#   scripts/compile-bench.sh --delay 14400   # start in 4 hours
#
# Detach it, because it runs for hours:
#   nohup scripts/compile-bench.sh --delay 14400 > /dev/null 2>&1 &
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

ATTEMPTS="${ATTEMPTS:-4}"
GAP_SECONDS="${GAP_SECONDS:-1800}"
# Outside the repo on purpose: the clean phase deletes artifacts, and sharing
# the repo's target/ would destroy the working cache and let an unrelated cargo
# perturb the measurement. Never point this at a worktree's target either — a
# shared CARGO_TARGET_DIR lets one tree's artifacts leak into another's
# verification with `git diff` clean.
BENCH_TARGET_DIR="${COMPILE_BENCH_TARGET_DIR:-${TMPDIR:-/tmp}/charpente-compile-bench-target}"
OUT_ROOT="target/compile-bench"

if [[ "${1:-}" == "--delay" ]]; then
  echo "compile-bench: sleeping ${2}s before the first attempt" >&2
  sleep "$2"
  shift 2
fi

# Pre-flight: our own cargo is not running yet, so anything found here is
# somebody else's and would show up as contention we cannot attribute.
if pgrep -x cargo >/dev/null || pgrep -x rustc >/dev/null; then
  echo "compile-bench: refusing to start — a cargo or rustc is already running" >&2
  exit 2
fi

# Build the harness BEFORE the measured window so its own compile is not part
# of the load the first observation samples.
cargo build --release -p bench-harness --bin compile-bench
BIN="target/release/compile-bench"

for attempt in $(seq 1 "$ATTEMPTS"); do
  ts="$(date +%Y%m%dT%H%M%S)"
  out="$OUT_ROOT/$ts"
  mkdir -p "$out"
  echo "compile-bench: attempt $attempt/$ATTEMPTS -> $out/run.log" >&2

  # No pipe here on purpose: $? after a pipe reads the filter, not the binary.
  set +e
  CARGO_TARGET_DIR="$BENCH_TARGET_DIR" caffeinate -i "$BIN" > "$out/run.log" 2>&1
  status=$?
  set -e
  echo "$status" > "$out/status"

  case "$status" in
    0) echo "compile-bench: attempt $attempt ACCEPTED — $out/run.log" >&2; exit 0 ;;
    1) echo "compile-bench: attempt $attempt refused (environment) — retrying" >&2 ;;
    3) echo "compile-bench: attempt $attempt refused (measurement) — NOT retrying, this is a finding" >&2; exit 3 ;;
    *) echo "compile-bench: attempt $attempt launch fault (exit $status)" >&2; exit 2 ;;
  esac

  if [[ "$attempt" -lt "$ATTEMPTS" ]]; then
    sleep "$GAP_SECONDS"
  fi
done

echo "compile-bench: $ATTEMPTS attempts, all refused on environment — no reading earned" >&2
exit 1
