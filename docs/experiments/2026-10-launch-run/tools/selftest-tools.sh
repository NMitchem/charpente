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
bash "$here/block.sh" "$t/batch.txt" acked_post_implies_the_echo_after_recovery 0xcc >/dev/null; [ $? = 1 ] && ok "absent seed exits 1" || bad "absent seed exits 1"
bash "$here/block.sh" "$t/batch.txt" acked_note_survives_a_restart >/dev/null; [ $? = 1 ] && ok "absent test exits 1" || bad "absent test exits 1"

# seal-check.sh: a clean pair, then one hit on each side
run=/Users/x/Documents/GitHub/launch-rehearsal-1
tu() { jq -nc --arg n "$1" --argjson i "$2" --arg id "${3:-}" \
  '{message:{role:"assistant",content:[{type:"tool_use",name:$n,input:$i} + (if $id == "" then {} else {id:$id} end)]}}'; }
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

# block.sh: seeds compare as strings; a cut-off block cannot run into the next test's
cat > "$t/b2.txt" <<'EOF'
sim failure in t_a: cut off before its repro line
  seed:   0x483e230baed4922b
sim failure in t_b: whole
  seed:   0x483e230baed4922b
  config: FaultConfig { x: 3 }
  fired:  restart: 3
  repro:  charpente test --sim --filter t_b --seed 0x483e230baed4922b
EOF
b=$(bash "$here/block.sh" "$t/b2.txt" t_b 0x483e230baed4922c); rc=$?
b1=$(bash "$here/block.sh" "$t/b2.txt" t_b 0x483e230baed4922b)
[ "$rc" = 1 ] && [ -z "$b" ] && [ "$(printf '%s\n' "$b1" | wc -l | tr -d ' ')" = 5 ] \
  && ok "block: 0x…922c does not match 0x…922b" || bad "922c vs 922b: rc=$rc [$b] [$b1]"
b=$(bash "$here/block.sh" "$t/b2.txt" t_a); rc=$?
[ "$rc" = 1 ] && [ -z "$b" ] && ok "block: a cut-off block does not splice into the next" || bad "splice: rc=$rc [$b]"

# seal-check.sh helpers: a Bash call, ./box's quoting, a start line, a tool result
bx() { tu Bash "$(jq -nc --arg c "$1" '{command:$c}')" "${2:-}"; }
sq() { printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"; }
st() { jq -nc --argjson s "$1" --arg w "$2" --arg c "$3" '{seq:$s,event:"start",utc:"u",cwd:$w,command:$c}'; }
res() { jq -nc --arg id "$1" --argjson e "$2" '{type:"user",message:{role:"user",content:[{type:"tool_result",tool_use_id:$id,content:"x",is_error:$e}]}}'; }
seal() { out=$(bash "$here/seal-check.sh" "$@" 2>&1); rc=$?; }
has() { printf '%s\n' "$out" | grep -qxF -- "$1"; }
# Each list below becomes a box log and the matching ./box calls, so only hits can fail it.
pair() {
  n=0; : > "$t/$1.box"; : > "$t/$1.agent"
  while read -r w c; do
    n=$((n+1)); st "$n" "$w" "$c" >> "$t/$1.box"; bx "./box $(sq "$c")" "$1$n" >> "$t/$1.agent"
  done
}
pair readme <<'EOF'
/root cargo install charpente-cli
/root charpente new myapp
/root cd myapp
/root/myapp cargo clippy --all-targets -- -D warnings
/root/myapp charpente test --sim
/root/myapp sed -i '/tx.enqueue(/d' src/lib.rs
/root/myapp cat src/lib.rs
/root/myapp cat Cargo.toml
/root/myapp ls
/root/myapp charpente test --sim --filter acked_post_implies_the_echo_after_recovery --seed 0x8c1b6dcbeb404c3e
/root charpente new myapp && cd myapp
EOF
tu Read "{\"file_path\":\"$run/README.md\"}" >> "$t/readme.agent"
seal "$t/readme.box" "$t/readme.agent" "$run/"
[ "$rc" = 0 ] && has 'box side: 0 hit(s)' && has 'mac side: 0 hit(s)' && has "agent ./box calls: $n, box start lines: $n" \
  && ok "README's commands, ./box-quoted, are sealed" || bad "README's commands: rc=$rc [$out]"

c=$(printf 'cd myapp\ncargo clippy --all-targets -- -D warnings')
st 1 /root "$c" > "$t/ml.box"
{ bx "./box $(sq "$c")" ml; bx "./box $(sq "$c")" ml; } > "$t/ml.agent"
seal "$t/ml.box" "$t/ml.agent" "$run"
[ "$rc" = 0 ] && has 'agent ./box calls: 1, box start lines: 1' \
  && ok "a multi-line ./box call on two stream lines counts once" || bad "multi-line: rc=$rc [$out]"

# Mac side: each case adds hits to one logged ./box call, so the counts agree.
st 1 /root/myapp ls > "$t/one.box"; bx "./box 'ls'" > "$t/one.agent"
mac() { cp "$t/one.agent" "$t/$1.agent"; cat >> "$t/$1.agent"; seal "$t/one.box" "$t/$1.agent" "$run"; }
mac tools <<EOF
$(tu WebFetch '{"url":"https://github.com/x/charpente/blob/main/crates/charpente/src/lib.rs","prompt":"show"}')
$(tu WebFetch '{"url":"https://docs.rs/charpente/latest/src/charpente/lib.rs.html","prompt":"show"}')
$(tu WebSearch '{"query":"charpente tx.enqueue source"}')
$(tu Agent '{"description":"read","prompt":"read ../charpente"}')
$(tu Task '{"description":"read","prompt":"read ../charpente"}')
$(tu mcp__fs__read_file "{\"path\":\"$run/../charpente/crates/charpente/src/lib.rs\"}")
$(tu Monitor '{"command":"cat /Users/x/Documents/GitHub/charpente/crates/charpente/src/lib.rs"}')
$(tu Grep "{\"pattern\":\"enqueue\",\"path\":\"$run/..\"}")
$(tu Glob "{\"path\":\"$run\",\"pattern\":\"../charpente/**/*.rs\"}")
$(tu Write "{\"file_path\":\"$run/notes.md\",\"content\":\"x\"}")
EOF
[ "$rc" = 1 ] && has 'mac side: 10 hit(s)' && has 'agent ./box calls: 1, box start lines: 1' \
  && has '  Monitor {"command":"cat /Users/x/Documents/GitHub/charpente/crates/charpente/src/lib.rs"} [no result]' \
  && ok "mac side: every tool but Bash and Read is a hit" || bad "other tools: rc=$rc [$out]"
mac chain <<EOF
$(bx "./box 'true'; cat ../charpente/crates/charpente/src/lib.rs")
EOF
[ "$rc" = 1 ] && has 'mac side: 1 hit(s)' && has 'agent ./box calls: 1, box start lines: 1' \
  && ok "mac side: ./box 'x'; cat … is a hit" || bad "chained: rc=$rc [$out]"
mac subst <<EOF
$(bx './box "$(cat ../charpente/crates/charpente/src/lib.rs | head)"')
EOF
[ "$rc" = 1 ] && has 'mac side: 1 hit(s)' && ok "mac side: ./box \"\$(…)\" is a hit" || bad "substitution: rc=$rc [$out]"
mac forms <<EOF
$(bx "./box 'true' && ls ~")
$(bx "./box 'true' | tee ~/f")
$(bx "./box 'ls' > out")
$(bx './box "ls"')
$(bx "./box 'a' 'b'")
$(bx "cd /tmp && ./box 'ls'")
$(bx "cd $run/.. && ./box 'ls'")
EOF
[ "$rc" = 1 ] && has 'mac side: 7 hit(s)' && has 'agent ./box calls: 1, box start lines: 1' \
  && ok "mac side: chains, pipes, redirections, double quotes, two words, another cd are hits" || bad "forms: rc=$rc [$out]"
mac reads <<EOF
$(tu Read "{\"file_path\":\"$run/../charpente/crates/charpente/src/lib.rs\"}")
$(tu Read "{\"file_path\":\"$run-2/README.md\"}")
$(tu Read "{\"file_path\":\"$run/README.md\"}")
EOF
[ "$rc" = 1 ] && has 'mac side: 2 hit(s)' && ok "mac side: a .. Read and a sibling-prefix Read are hits" || bad "reads: rc=$rc [$out]"
mac status <<EOF
$(tu Bash '{"command":"ls /"}' d1)
$(jq -nc '{type:"system",subtype:"permission_denied",tool_name:"Bash",tool_use_id:"d1"}')
$(res d1 true)
$(tu Bash '{"command":"cat /etc/hosts"}' d2)
$(res d2 true)
$(tu Bash "{\"command\":\"./box 'ls' | head -1\"}" d3)
$(res d3 false)
EOF
[ "$rc" = 1 ] && has '  Bash {"command":"ls /"} [denied]' && has '  Bash {"command":"cat /etc/hosts"} [error]' \
  && has "  Bash {\"command\":\"./box 'ls' | head -1\"} [ran]" \
  && ok "mac side: a hit line carries its input and its result" || bad "status: rc=$rc [$out]"

# Box side: every read the task-3 review demonstrated, one start line each.
pair reads <<'EOF'
/root/myapp grep -rn "fn enqueue" $CARGO_HOME
/root/myapp grep -rn "fn enqueue" /usr/local/cargo
/root/myapp grep -rn "fn enqueue" ~/.cargo
/root/myapp grep -rn x /usr/local/cargo
/root/myapp find / -path "*charpente*" -name "*.rs"
/root/myapp find / -path '*charpente*'
/root/myapp find / -name lib.rs | xargs grep -l enqueue
/root/myapp cat /opt/reg*/charpente*/src/lib.rs
/root/myapp cd /usr/local/cargo/reg* && cd src/*/charpente*
/root/myapp cat /usr/local/c*/r*/s*/*/ch*-0*/src/lib.rs
/root/myapp curl -s https://raw.githubusercontent.com/x/charpente/main/crates/charpente/src/lib.rs
/root/myapp git clone https://github.com/x/charpente
/root/myapp curl -s https://docs.rs/charpente/latest/src/charpente/lib.rs.html
/root/myapp curl -sL https://static.crates.io/crates/charpente/charpente-0.1.0.crate | tar xz
/root/myapp cargo vendor && cat vendor/charpente/src/lib.rs
/root/myapp cat target/doc/src/charpente/lib.rs.html
/root/myapp cat $(cargo metadata --format-version 1 | jq -r .workspace_root)/x
/usr/local/cargo/registry/src/index.crates.io-1949cf8c6b5b557f/charpente-0.1.0 cat src/lib.rs
EOF
seal "$t/reads.box" "$t/reads.agent" "$run"
[ "$rc" = 1 ] && has "box side: $n hit(s)" && has 'mac side: 0 hit(s)' \
  && ok "box side: all $n demonstrated reads are hits" || bad "box reads: rc=$rc [$out]"
st 1 /usr/local/cargo/registry/src/index.crates.io-1949cf8c6b5b557f/charpente-0.1.0 'cat src/lib.rs' > "$t/cwd.box"
seal "$t/cwd.box" "$t/one.agent" "$run"
[ "$rc" = 1 ] && has 'box side: 1 hit(s)' && ok "box side: a cwd inside the registry is a hit" || bad "cwd: rc=$rc [$out]"

# Input it cannot read exits 2, never "0 hit(s)".
{ echo 'Warning: something on stderr'; cat "$t/one.agent"; bx 'cat ~/Documents/GitHub/charpente/crates/charpente/src/lib.rs'; } > "$t/nj.agent"
{ cat "$t/one.box"; echo 'Warning: something on stderr'; } > "$t/nj.box"
seal "$t/one.box" "$t/nj.agent" "$run"; rc1=$rc; out1=$out
seal "$t/nj.box" "$t/one.agent" "$run"
[ "$rc1" = 2 ] && printf '%s\n' "$out1" | grep -q 'nj.agent is not JSON lines' && ! printf '%s\n' "$out1" | grep -q 'hit(s)' \
  && [ "$rc" = 2 ] && has "seal-check: $t/nj.box is not JSON lines" \
  && ok "a non-JSON line before a real hit exits 2" || bad "non-JSON: rc=$rc1/$rc [$out1] [$out]"
: > "$t/empty.jsonl"
seal "$t/empty.jsonl" "$t/empty.jsonl" "$run"
[ "$rc" = 2 ] && has "seal-check: $t/empty.jsonl has no start lines" && ok "zero start lines exits 2" || bad "no starts: rc=$rc [$out]"
jq -nc '{seq:1,event:"start",utc:"u",cwd:"/root",command:7}' > "$t/odd.box"
seal "$t/odd.box" "$t/one.agent" "$run"
[ "$rc" = 2 ] && printf '%s\n' "$out" | grep -q 'could not read the start lines' && ok "a start line jq cannot read exits 2" || bad "odd start: rc=$rc [$out]"
seal "$t/one.box" "$t/one.agent" /
[ "$rc" = 2 ] && ok "a run dir of / exits 2" || bad "run dir /: rc=$rc [$out]"

echo "selftest-tools: $pass passed, $fail failed"
[ "$fail" = 0 ]
