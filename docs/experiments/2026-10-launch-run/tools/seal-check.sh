#!/usr/bin/env bash
# seal-check.sh <box.jsonl> <agent.jsonl> <run-dir>: launch-run spec §4's
# sealing, checked after. Hits are printed for a human ruling; any hit, or an
# agent ./box call the log never saw, exits 1.
set -u
[ "$#" -eq 3 ] || { echo "usage: seal-check.sh <box.jsonl> <agent.jsonl> <run-dir>" >&2; exit 2; }
boxlog=$1 agent=$2 run=${3%/}
box_hits=$(jq -r 'select(.event=="start") | .command
  | select(test("registry|github\\.com|docs\\.rs|static\\.crates\\.io|crates\\.io/api|\\.crate\\b|/charpente-[a-z-]*[0-9]"))' "$boxlog")
mac_hits=$(jq -r --arg run "$run" '
  (.message // .) | select(.role? == "assistant") | .content[]?
  | select(type == "object" and .type == "tool_use")
  | if .name == "Bash" then
      (.input.command | ltrimstr("cd " + $run + " && ") | ltrimstr("cd \"" + $run + "\" && ")) as $c
      | if ($c | startswith("./box ")) and ($c | contains("GitHub/charpente") | not) then empty
        else "Bash: " + .input.command end
    elif (.name | IN("Read","Glob","Grep","LS","Edit","Write","MultiEdit","NotebookEdit")) then
      ((.input.file_path // .input.path // .input.notebook_path // "") | tostring) as $p
      | if ($p | startswith($run + "/")) or $p == $run then empty else .name + ": " + $p end
    else empty end' "$agent")
calls=$(jq -r --arg run "$run" '
  (.message // .) | select(.role? == "assistant") | .content[]?
  | select(type == "object" and .type == "tool_use" and .name == "Bash")
  | .input.command | ltrimstr("cd " + $run + " && ") | ltrimstr("cd \"" + $run + "\" && ")
  | select(startswith("./box "))' "$agent" | wc -l | tr -d ' ')
starts=$(jq -r 'select(.event=="start") | .seq' "$boxlog" | wc -l | tr -d ' ')
nb=$(printf '%s' "$box_hits" | grep -c . ); nm=$(printf '%s' "$mac_hits" | grep -c . )
echo "box side: $nb hit(s)"; [ "$nb" = 0 ] || printf '%s\n' "$box_hits" | sed 's/^/  /'
echo "mac side: $nm hit(s)"; [ "$nm" = 0 ] || printf '%s\n' "$mac_hits" | sed 's/^/  /'
echo "agent ./box calls: $calls, box start lines: $starts"
[ "$nb" = 0 ] && [ "$nm" = 0 ] && [ "$calls" = "$starts" ]
