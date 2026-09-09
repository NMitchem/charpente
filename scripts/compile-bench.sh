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
#   2 usage/setup     -> stop, operator error (see "Exit codes" below)
#   3 refused (final) -> stop and REPORT; Unstable/BelowFloor/BuildFailed/
#                        ChildCpuNotAggregated describe the measurement, and
#                        rerolling them is how a reading gets manufactured
#
# caffeinate is belt and Invalid::Suspended is braces. Prevention exists so the
# night is not wasted, never so the gate is bypassed. Both stay armed.
#
#   scripts/compile-bench.sh                 # start now
#   scripts/compile-bench.sh --delay 14400   # start in 4 hours
#   scripts/compile-bench.sh --self-test     # check THIS SCRIPT, no measuring
#
# Detach it, because it runs for hours:
#   nohup scripts/compile-bench.sh --delay 14400 > /dev/null 2>&1 &
#
# ── Exit codes of this script ────────────────────────────────────────────────
#   0  a reading was accepted
#   1  every attempt was refused on the ENVIRONMENT — re-arm another night
#   2  operator or setup error: a bad argument, a bad ATTEMPTS, no caffeinate,
#      a cargo already running, a failed harness build, or the binary itself
#      exiting 2 or dying on a signal
#   3  a FINAL refusal — spec §4 forbids retrying it, tonight or any night
# 1 means "this run earned nothing, the machine's fault"; a setup mistake must
# never wear that shape, because 1 is the code that authorizes another attempt.
#
# ── §4 across nights, not just across attempts ───────────────────────────────
# Exit 3 stops the loop, but a process that has exited enforces nothing. The
# spec has the operator re-arm the shot by hand after a lost night, and a fresh
# invocation the next night would otherwise have no memory of why the last one
# stopped — so a night lost to contention (re-arm: legitimate) and a night that
# hit a final refusal (re-arm: forbidden) would look identical. This script
# therefore REFUSES TO START while any recorded attempt's status file holds 3,
# and names the attempt directory so the finding can be read. Override with
#   COMPILE_BENCH_IGNORE_PRIOR_FINAL=1
# and only for one reason: a genuine instrument fix has landed since, so the
# refusal describes code that no longer exists. "It probably was not real" is
# not that reason — that is the reroll §4 exists to forbid.
#
# ── Where the artifacts go, and why not under target/ ────────────────────────
# Spec §4 requires every attempt to be recorded and the attempt count published
# with the number; §3 item 4 requires every refusal along the way, including
# refused attempts on other nights. `target/` is erased by `cargo clean` and
# ignored by git, which made the required refusal history the single most
# deletable thing in the repo — and the cross-night check above reads it. It
# lives in `bench-runs/compile-bench/<run>/attempt-N/` instead: outside
# target/, survives `cargo clean`, and is committable evidence.
#
# ── Environment ──────────────────────────────────────────────────────────────
#   ATTEMPTS                        how many attempts before giving up (4)
#   GAP_SECONDS                     wait between attempts (1800)
#   COMPILE_BENCH_TARGET_DIR        the DEDICATED target dir (see below)
#   COMPILE_BENCH_OUT_ROOT          where artifacts and the §4 history live
#   COMPILE_BENCH_BIN               the binary to drive (skips the build)
#   COMPILE_BENCH_IGNORE_PRIOR_FINAL  see "§4 across nights" above
set -euo pipefail

# Absolute, captured before the cd, because --self-test re-invokes this file.
SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Operator and setup errors. Never exit 1 from here: 1 means "the machine was
# busy, try another night", and a typo must not wear that shape.
die() {
  echo "compile-bench: $*" >&2
  exit 2
}

ATTEMPTS="${ATTEMPTS:-4}"
GAP_SECONDS="${GAP_SECONDS:-1800}"
# Outside the repo on purpose: the clean phase deletes artifacts, and sharing
# the repo's target/ would destroy the working cache and let an unrelated cargo
# perturb the measurement — a concurrent cargo would block the measured build
# on the target-dir lock, and that wait lands in wall time while child CPU and
# load stay normal, so every gate passes and an inflated figure is published.
# The binary refuses a target dir inside the repo for exactly that reason; this
# default just makes the supported setup the easy one. Never point this at a
# worktree's target either — a shared CARGO_TARGET_DIR lets one tree's
# artifacts leak into another's verification with `git diff` clean.
BENCH_TARGET_DIR="${COMPILE_BENCH_TARGET_DIR:-${TMPDIR:-/tmp}/charpente-compile-bench-target}"
OUT_ROOT="${COMPILE_BENCH_OUT_ROOT:-bench-runs/compile-bench}"
BIN="${COMPILE_BENCH_BIN:-target/release/compile-bench}"

# Test seam, set ONLY by --self-test. It disables exactly three things — the
# pre-flight cargo check, the harness build, and caffeinate — because a stub
# run acquires no quiet machine and needs no real binary. Every integrity rule
# stays live under it: §4's exit-code arms, the cross-night refusal check and
# the argument validation are precisely what the self-test exercises. Setting
# it for a real run does not make a measurement faster, it makes it fake.
SELF_TEST_CHILD="${COMPILE_BENCH_SELF_TEST_CHILD:-}"

# ── Arguments ────────────────────────────────────────────────────────────────
DELAY=0
SELF_TEST=0
ARGC=$#
while [[ $# -gt 0 ]]; do
  case "$1" in
    --delay)
      [[ $# -ge 2 ]] || die "--delay needs a number of seconds (e.g. --delay 14400)"
      [[ "$2" =~ ^[0-9]+$ ]] || die "--delay wants seconds as digits, got '$2'"
      DELAY="$2"
      shift 2
      ;;
    --self-test)
      [[ $ARGC -eq 1 ]] || die "--self-test takes no other arguments"
      SELF_TEST=1
      shift
      ;;
    *)
      # Silently ignoring this is how `--dealy 14400` starts a measurement
      # immediately, on a machine the operator is still using.
      die "unrecognised argument '$1' — usage: compile-bench.sh [--delay SECONDS] | --self-test"
      ;;
  esac
done

# ── The self-test ────────────────────────────────────────────────────────────
# Drives THIS script against a stub binary that exits with codes we choose, and
# asserts what the script does with each. The §4 arms and the cross-night
# refusal are otherwise untested: transposing the `1)` and `3)` arms below
# leaves the whole Rust suite green while converting "never retry Unstable"
# into "always retry Unstable". Run by `cargo test -p bench-harness --test
# compile_bench_script`, so it is not a command anyone has to remember.
st_fail=0
st_check() { # <what> <got> <want>
  if [[ "$2" == "$3" ]]; then
    echo "  ok    $1 ($2)"
  else
    echo "  FAIL  $1: got '$2', want '$3'" >&2
    st_fail=$((st_fail + 1))
  fi
}
st_grep() { # <what> <file> <pattern>
  if grep -qF -- "$3" "$2"; then
    echo "  ok    $1"
  else
    echo "  FAIL  $1: '$3' not in $2" >&2
    sed 's/^/        | /' "$2" >&2
    st_fail=$((st_fail + 1))
  fi
}

self_test() {
  local tmp
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT

  cat > "$tmp/stub" <<'STUB'
#!/usr/bin/env bash
# Stands in for target/release/compile-bench. Exits with the n'th code in
# $STUB_CODES on its n'th invocation, repeating the last one after that.
set -eu
n=$(( $(cat "$STUB_COUNT" 2>/dev/null || echo 0) + 1 ))
echo "$n" > "$STUB_COUNT"
set -- $STUB_CODES
if [[ $n -le $# ]]; then code="${!n}"; else code="${@: -1}"; fi
echo "stub compile-bench: invocation $n -> exit $code"
exit "$code"
STUB
  chmod +x "$tmp/stub"

  # st_run <case> <codes> <attempts> [args...]; leaves $ST_RC, $ST_RUNS, $ST_LOG
  # and $ST_OUT set. Reuses $ST_OUT_REUSE as the artifact root when set, which
  # is how the cross-night check is driven: one case leaves a status of 3
  # behind and the next one starts against it.
  st_run() {
    local case_name="$1" codes="$2" attempts="$3"
    shift 3
    local dir="$tmp/$case_name"
    mkdir -p "$dir"
    ST_OUT="${ST_OUT_REUSE:-$dir/out}"
    ST_LOG="$dir/log"
    : > "$dir/count"
    ST_RC=0
    env COMPILE_BENCH_SELF_TEST_CHILD=1 \
      COMPILE_BENCH_BIN="$tmp/stub" \
      COMPILE_BENCH_OUT_ROOT="$ST_OUT" \
      STUB_COUNT="$dir/count" \
      STUB_CODES="$codes" \
      ATTEMPTS="$attempts" \
      GAP_SECONDS=0 \
      bash "$SCRIPT" "$@" > "$ST_LOG" 2>&1 || ST_RC=$?
    ST_RUNS="$(cat "$dir/count" 2>/dev/null || echo 0)"
    ST_RUNS="${ST_RUNS:-0}"
  }

  echo "compile-bench --self-test: driving $SCRIPT against a stub binary" >&2
  # Inherited from the caller it would silently defeat the cross-night case
  # below, which is the one case here that cannot fail loudly on its own.
  unset COMPILE_BENCH_IGNORE_PRIOR_FINAL
  local ST_OUT_REUSE=""

  echo "accepted on the first attempt"
  st_run accepted "0" 3
  st_check "exit" "$ST_RC" 0
  st_check "attempts made" "$ST_RUNS" 1

  echo "an environment refusal is retried, and the retry is published"
  st_run env_then_ok "1 0" 3
  st_check "exit" "$ST_RC" 0
  st_check "attempts made" "$ST_RUNS" 2

  echo "every attempt refused on the environment earns nothing"
  st_run all_env "1 1" 2
  st_check "exit" "$ST_RC" 1
  st_check "attempts made" "$ST_RUNS" 2
  st_grep "says no reading was earned" "$ST_LOG" "no reading earned"

  # THE §4 CASE. If this one ever reads exit 1, "never retry Unstable" has
  # become "always retry Unstable" and every later reading is manufactured.
  echo "a final refusal stops the run and is never retried"
  st_run final_first "3 0 0" 3
  st_check "exit" "$ST_RC" 3
  st_check "attempts made" "$ST_RUNS" 1
  st_grep "reports it as a finding" "$ST_LOG" "NOT retrying"
  local final_out="$ST_OUT"

  # The stub's third code is 0: a script that retried past the 3 would have
  # published a reading here. The attempt count proves it did not get there.
  echo "a final refusal on a later attempt stops the run too"
  st_run final_later "1 3 0" 3
  st_check "exit" "$ST_RC" 3
  st_check "attempts made" "$ST_RUNS" 2

  echo "the binary's own usage refusal is an operator error, not a night"
  st_run usage "2" 3
  st_check "exit" "$ST_RC" 2
  st_check "attempts made" "$ST_RUNS" 1

  echo "an abnormal exit is not a refusal of any kind"
  st_run crashed "137" 3
  st_check "exit" "$ST_RC" 2
  st_check "attempts made" "$ST_RUNS" 1
  st_grep "names the abnormal exit" "$ST_LOG" "abnormal exit"

  # ── §4 across nights ──────────────────────────────────────────────────────
  echo "a recorded final refusal refuses the NEXT night's run"
  ST_OUT_REUSE="$final_out"
  st_run prior_final "0" 3
  st_check "exit" "$ST_RC" 3
  st_check "attempts made" "$ST_RUNS" 0
  st_grep "names the offending attempt" "$ST_LOG" "$final_out"
  st_grep "names the override" "$ST_LOG" "COMPILE_BENCH_IGNORE_PRIOR_FINAL"

  echo "the override lets a run start once an instrument fix has landed"
  export COMPILE_BENCH_IGNORE_PRIOR_FINAL=1
  st_run prior_final_override "0" 3
  unset COMPILE_BENCH_IGNORE_PRIOR_FINAL
  st_check "exit" "$ST_RC" 0
  st_check "attempts made" "$ST_RUNS" 1
  ST_OUT_REUSE=""

  # ── Operator errors: exit 2, and nothing is measured ──────────────────────
  echo "operator errors exit 2 and measure nothing"
  st_run attempts_word "0" abc
  st_check "ATTEMPTS=abc exit" "$ST_RC" 2
  st_check "ATTEMPTS=abc attempts made" "$ST_RUNS" 0

  st_run attempts_zero "0" 0
  st_check "ATTEMPTS=0 exit" "$ST_RC" 2
  st_check "ATTEMPTS=0 attempts made" "$ST_RUNS" 0

  st_run attempts_negative "0" -1
  st_check "ATTEMPTS=-1 exit" "$ST_RC" 2

  st_run typo_arg "0" 3 --dealy 14400
  st_check "unrecognised argument exit" "$ST_RC" 2
  st_check "unrecognised argument attempts made" "$ST_RUNS" 0

  st_run delay_bare "0" 3 --delay
  st_check "--delay with no argument exit" "$ST_RC" 2
  st_check "--delay with no argument attempts made" "$ST_RUNS" 0

  st_run delay_word "0" 3 --delay soon
  st_check "--delay soon exit" "$ST_RC" 2
  st_check "--delay soon attempts made" "$ST_RUNS" 0

  if [[ "$st_fail" -eq 0 ]]; then
    echo "compile-bench --self-test: all checks passed" >&2
    exit 0
  fi
  echo "compile-bench --self-test: $st_fail check(s) FAILED" >&2
  exit 1
}

if [[ "$SELF_TEST" -eq 1 ]]; then
  self_test
fi

# ── Setup validation ─────────────────────────────────────────────────────────
# `seq 1 abc` fails and `seq 1 0` counts DOWN on BSD, so an unvalidated
# ATTEMPTS produced either zero attempts wearing the shape of an environmental
# refusal, or two attempts when asked for none.
[[ "$ATTEMPTS" =~ ^[1-9][0-9]*$ ]] || die "ATTEMPTS must be a positive integer, got '$ATTEMPTS'"
[[ "$GAP_SECONDS" =~ ^[0-9]+$ ]] || die "GAP_SECONDS must be a whole number of seconds, got '$GAP_SECONDS'"

# ── §4 across nights: refuse to start under an outstanding final refusal ─────
shopt -s nullglob
prior_final=""
for status_file in "$OUT_ROOT"/*/*/status; do
  if [[ "$(cat "$status_file")" == "3" ]]; then
    prior_final="$(dirname "$status_file")"
    break
  fi
done
shopt -u nullglob
if [[ -n "$prior_final" && -z "${COMPILE_BENCH_IGNORE_PRIOR_FINAL:-}" ]]; then
  echo "compile-bench: refusing to start — a previous attempt ended in a FINAL refusal:" >&2
  echo "    $prior_final" >&2
  echo "    $prior_final/run.log" >&2
  echo "  Spec §4: Unstable, BelowFloor, BuildFailed and ChildCpuNotAggregated describe the" >&2
  echo "  MEASUREMENT, not the machine. Re-running after one is how 'take the reading' becomes" >&2
  echo "  'run until the numbers look good'. Report it as a finding instead." >&2
  echo "  Override with COMPILE_BENCH_IGNORE_PRIOR_FINAL=1 ONLY once a genuine instrument fix" >&2
  echo "  has landed, so that refusal describes code that no longer exists." >&2
  exit 3
fi

# The delay comes before the pre-flight on purpose: "is another cargo running"
# is only worth asking immediately before the measurement, not four hours
# earlier. The two refusals above are worth failing on straight away.
if [[ "$DELAY" -gt 0 ]]; then
  echo "compile-bench: sleeping ${DELAY}s before the first attempt" >&2
  sleep "$DELAY"
fi

# ── Pre-flight and build ─────────────────────────────────────────────────────
if [[ -z "$SELF_TEST_CHILD" ]]; then
  # Our own cargo is not running yet, so anything found here is somebody else's
  # and would show up as contention we cannot attribute.
  if pgrep -x cargo >/dev/null || pgrep -x rustc >/dev/null; then
    die "refusing to start — a cargo or rustc is already running"
  fi
  # caffeinate is belt (Invalid::Suspended is braces). The run is unattended,
  # so a box that sleeps costs the whole night.
  command -v caffeinate >/dev/null || die "caffeinate not found — this runner is macOS-only"
  CAFFEINATE=(caffeinate -i)
  if [[ -z "${COMPILE_BENCH_BIN:-}" ]]; then
    # Before the measured window, so the harness's own compile is not part of
    # the load the first observation samples. `set -e` would exit 1 here, which
    # is the retry code — a build failure is an operator error.
    cargo build --release -p bench-harness --bin compile-bench ||
      die "the harness build failed — fix that before measuring anything"
  fi
else
  # `env` rather than an empty array: macOS ships bash 3.2, where expanding an
  # empty array under `set -u` is an unbound-variable error.
  CAFFEINATE=(env)
fi
[[ -x "$BIN" ]] || die "no compile-bench binary at $BIN"

# One directory per run, one subdirectory per attempt. The pid disambiguates
# two runs started in the same second.
RUN_ID="$(date +%Y%m%dT%H%M%S)-$$"

for attempt in $(seq 1 "$ATTEMPTS"); do
  out="$OUT_ROOT/$RUN_ID/attempt-$attempt"
  mkdir -p "$out"
  echo "compile-bench: attempt $attempt/$ATTEMPTS -> $out/run.log" >&2

  # No pipe here on purpose: $? after a pipe reads the filter, not the binary.
  set +e
  CARGO_TARGET_DIR="$BENCH_TARGET_DIR" "${CAFFEINATE[@]}" "$BIN" > "$out/run.log" 2>&1
  status=$?
  set -e
  # Written before the case below acts on it: this file IS the cross-night
  # memory the check at the top of this script reads.
  echo "$status" > "$out/status"

  case "$status" in
    0) echo "compile-bench: attempt $attempt ACCEPTED — $out/run.log" >&2; exit 0 ;;
    1) echo "compile-bench: attempt $attempt refused (environment) — retrying" >&2 ;;
    2) echo "compile-bench: attempt $attempt refused (usage/setup, exit 2) — stop, operator error" >&2; exit 2 ;;
    3) echo "compile-bench: attempt $attempt refused (measurement) — NOT retrying, this is a finding" >&2; exit 3 ;;
    # A signal or a crash lands here too (128+n; 137 is SIGKILL, e.g. an OOM
    # kill), and it is not a refusal of any kind — the binary never classified
    # anything, so nothing may be concluded about the measurement.
    *) echo "compile-bench: attempt $attempt launch fault or abnormal exit (exit $status) — no verdict was reached" >&2; exit 2 ;;
  esac

  if [[ "$attempt" -lt "$ATTEMPTS" ]]; then
    sleep "$GAP_SECONDS"
  fi
done

echo "compile-bench: $ATTEMPTS attempts, all refused on environment — no reading earned" >&2
exit 1
