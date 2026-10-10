#!/usr/bin/env bash
# The box's own oracle: every case the launch-run plan's Review Focus names.
set -u
here=$(cd "$(dirname "$0")" && pwd)
t=$(mktemp -d); name="box-selftest-$$"
trap 'docker rm -f "$name" >/dev/null 2>&1; rm -rf "$t"' EXIT
pass=0; fail=0
ok()  { pass=$((pass+1)); echo "  ok   $1"; }
bad() { fail=$((fail+1)); echo "  FAIL $1"; }
bash "$here/mkbox.sh" "$name" debian:bookworm-slim "$t" "$t/log.jsonl" >/dev/null || { echo "mkbox failed"; exit 1; }
box="$t/box"
endrec() { jq -c --argjson s "$1" 'select(.event=="end" and .seq==$s)' "$t/log.jsonl"; }

# 1. quotes, $, && and a newline run and log verbatim
cmd=$'printf \'%s|\' "a b" "c\'d" \'$HOME\' && echo\necho second'
out=$("$box" "$cmd")
[ "$out" = $'a b|c\'d|$HOME|\nsecond' ] && ok "quoting runs verbatim" || bad "quoting runs verbatim: got [$out]"
logged=$(jq -r 'select(.event=="start" and .seq==1) | .command' "$t/log.jsonl")
[ "$logged" = "$cmd" ] && ok "quoting logs verbatim" || bad "quoting logs verbatim: got [$logged]"

# 2. exit code, stdout and stderr kept apart
"$box" 'echo out; echo err >&2; exit 3' >"$t/o" 2>"$t/e"; rc=$?
[ "$rc" = 3 ] && [ "$(cat "$t/o")" = out ] && [ "$(cat "$t/e")" = err ] && ok "exit and streams" || bad "exit and streams: rc=$rc"
[ "$(endrec 2 | jq -r .exit)" = 3 ] && [ "$(endrec 2 | jq -r .stdout)" = out ] && [ "$(endrec 2 | jq -r .stderr)" = err ] \
  && ok "end line carries exit and both streams" || bad "end line: $(endrec 2)"

# 3. the working directory persists; a failed cd leaves it where it was
"$box" 'mkdir -p /tmp/w && cd /tmp/w' >/dev/null
[ "$("$box" pwd)" = /tmp/w ] && ok "cwd persists" || bad "cwd persists"
"$box" 'cd /nope' >/dev/null 2>&1 && bad "failed cd exits nonzero" || ok "failed cd exits nonzero"
[ "$("$box" pwd)" = /tmp/w ] && ok "failed cd keeps cwd" || bad "failed cd keeps cwd"

# 4. a call killed mid-run is still on record: a start line, no end line
"$box" 'sleep 20' & pid=$!; sleep 2; kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
s=$(jq -r 'select(.event=="start" and .command=="sleep 20") | .seq' "$t/log.jsonl")
[ -n "$s" ] && [ -z "$(endrec "$s")" ] && ok "killed call: start without end" || bad "killed call: seq=[$s]"

# 5. parallel calls get distinct seqs and both lines
"$box" 'echo p1' >/dev/null & "$box" 'echo p2' >/dev/null & wait
n=$(jq -r 'select(.event=="start" and (.command=="echo p1" or .command=="echo p2")) | .seq' "$t/log.jsonl" | sort -u | wc -l | tr -d ' ')
e=$(for q in $(jq -r 'select(.event=="start" and (.command=="echo p1" or .command=="echo p2")) | .seq' "$t/log.jsonl"); do endrec "$q"; done | wc -l | tr -d ' ')
[ "$n" = 2 ] && [ "$e" = 2 ] && ok "parallel calls" || bad "parallel calls: seqs=$n ends=$e"

# 6. output that is not UTF-8 still makes a valid log line
"$box" "printf '\\377ok'" >/dev/null
jq -e . "$t/log.jsonl" >/dev/null && ok "every log line is JSON" || bad "every log line is JSON"

# 7. usage: anything but exactly one argument is refused and not logged
lines=$(wc -l < "$t/log.jsonl")
"$box" >/dev/null 2>&1; rc0=$?; "$box" a b >/dev/null 2>&1; rc2=$?
[ "$rc0" = 2 ] && [ "$rc2" = 2 ] && [ "$(wc -l < "$t/log.jsonl")" = "$lines" ] && ok "usage refused, unlogged" || bad "usage: rc0=$rc0 rc2=$rc2"

# 8. mkbox refuses a name in use
bash "$here/mkbox.sh" "$name" debian:bookworm-slim "$t" "$t/log2.jsonl" >/dev/null 2>&1 && bad "mkbox refuses reuse" || ok "mkbox refuses reuse"

echo "selftest-box: $pass passed, $fail failed"
[ "$fail" = 0 ]
