#!/usr/bin/env bash
set -u
here=$(cd "$(dirname "$0")" && pwd); t=$(mktemp -d); trap 'rm -rf "$t"' EXIT
pass=0; fail=0
ok()  { pass=$((pass+1)); echo "  ok   $1"; }
bad() { fail=$((fail+1)); echo "  FAIL $1"; }

# block.sh: two failing universes of one test; pick by seed
cat > "$t/batch.txt" <<'EOF'
sim: 256 universe(s) per test from master seed 0x1

sim failure in acked_post_implies_the_echo_after_recovery: ack-durability violated: one
  seed:   0x00000000000000aa
  config: FaultConfig { x: 1 }
  fired:  restart: 1
  repro:  charpente test --sim --filter acked_post_implies_the_echo_after_recovery --seed 0xaa

sim failure in acked_post_implies_the_echo_after_recovery: ack-durability violated: two
  seed:   0x00000000000000bb
  config: FaultConfig { x: 2 }
  fired:  restart: 2
  repro:  charpente test --sim --filter acked_post_implies_the_echo_after_recovery --seed 0xbb
EOF
b=$(bash "$here/block.sh" "$t/batch.txt" acked_post_implies_the_echo_after_recovery 0xbb)
[ "$(printf '%s\n' "$b" | head -1)" = "sim failure in acked_post_implies_the_echo_after_recovery: ack-durability violated: two" ] \
  && [ "$(printf '%s\n' "$b" | wc -l | tr -d ' ')" = 5 ] && ok "block by seed" || bad "block by seed: [$b]"
b1=$(bash "$here/block.sh" "$t/batch.txt" acked_post_implies_the_echo_after_recovery)
printf '%s\n' "$b1" | grep -q -- '--seed 0xaa$' && ok "first block without a seed" || bad "first block: [$b1]"
bash "$here/block.sh" "$t/batch.txt" acked_post_implies_the_echo_after_recovery 0xcc >/dev/null && bad "absent seed exits 1" || ok "absent seed exits 1"
bash "$here/block.sh" "$t/batch.txt" acked_note_survives_a_restart >/dev/null && bad "absent test exits 1" || ok "absent test exits 1"

# seal-check.sh: a clean pair, then one hit on each side
run=/Users/x/Documents/GitHub/launch-rehearsal-1
tu() { jq -nc --arg n "$1" --argjson i "$2" '{message:{role:"assistant",content:[{type:"tool_use",name:$n,input:$i}]}}'; }
{ tu Read "{\"file_path\":\"$run/README.md\"}"
  tu Bash "{\"command\":\"cd $run && ./box 'cargo install charpente-cli'\"}"
  tu Bash "{\"command\":\"./box 'charpente new myapp'\"}"; } > "$t/agent-clean.jsonl"
{ jq -nc '{seq:1,event:"start",utc:"u",cwd:"/root",command:"cargo install charpente-cli"}'
  jq -nc '{seq:2,event:"start",utc:"u",cwd:"/root",command:"charpente new myapp"}'; } > "$t/box-clean.jsonl"
bash "$here/seal-check.sh" "$t/box-clean.jsonl" "$t/agent-clean.jsonl" "$run" >/dev/null && ok "clean pair passes" || bad "clean pair passes"
cp "$t/agent-clean.jsonl" "$t/agent-dirty.jsonl"
tu Bash '{"command":"cat ~/Documents/GitHub/charpente/crates/charpente/src/lib.rs"}' >> "$t/agent-dirty.jsonl"
out=$(bash "$here/seal-check.sh" "$t/box-clean.jsonl" "$t/agent-dirty.jsonl" "$run"); rc=$?
[ "$rc" = 1 ] && printf '%s\n' "$out" | grep -q 'mac side: 1 hit' && ok "mac-side read flagged" || bad "mac-side read: rc=$rc [$out]"
cp "$t/box-clean.jsonl" "$t/box-dirty.jsonl"
jq -nc '{seq:3,event:"start",utc:"u",cwd:"/root",command:"cat /usr/local/cargo/registry/src/index.crates.io-x/charpente-0.1.0/src/lib.rs"}' >> "$t/box-dirty.jsonl"
tu Bash "{\"command\":\"./box 'cat x'\"}" >> "$t/agent-clean.jsonl"
out=$(bash "$here/seal-check.sh" "$t/box-dirty.jsonl" "$t/agent-clean.jsonl" "$run"); rc=$?
[ "$rc" = 1 ] && printf '%s\n' "$out" | grep -q 'box side: 1 hit' && ok "box-side registry read flagged" || bad "box-side: rc=$rc [$out]"
out=$(bash "$here/seal-check.sh" "$t/box-clean.jsonl" "$t/agent-clean.jsonl" "$run"); rc=$?
[ "$rc" = 1 ] && printf '%s\n' "$out" | grep -q 'agent ./box calls: 3, box start lines: 2' && ok "unlogged call flagged" || bad "count mismatch: rc=$rc [$out]"

echo "selftest-tools: $pass passed, $fail failed"
[ "$fail" = 0 ]
