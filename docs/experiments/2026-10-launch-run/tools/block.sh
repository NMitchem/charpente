#!/usr/bin/env bash
# block.sh <file> <test> [<seed>]: the failure block the sim harness prints
# (crates/charpente-sim/src/harness.rs, "sim failure in …" through "  repro:  …").
set -u
[ "$#" -ge 2 ] && [ "$#" -le 3 ] || { echo "usage: block.sh <file> <test> [<seed>]" >&2; exit 2; }
awk -v head="sim failure in $2: " -v seed="${3:-}" '
  index($0, head) == 1 { on = 1; buf = "" }
  on { buf = buf $0 "\n" }
  on && index($0, "  repro:  ") == 1 {
    on = 0
    n = split($0, w, " ")
    if (seed == "" || w[n] == seed) { printf "%s", buf; found = 1; exit }
  }
  END { exit found ? 0 : 1 }' "$1"
