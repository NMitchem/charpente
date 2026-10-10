#!/usr/bin/env bash
# seal-check.sh <box.jsonl> <agent.jsonl> <run-dir>: launch-run spec §4's
# sealing, checked after. <agent.jsonl> is the stranger's `claude -p
# --output-format stream-json` transcript. Hits are printed for a human
# ruling; any hit, or a count of agent ./box calls that differs from the box
# log's start lines, exits 1. Input it cannot read exits 2.
set -u -o pipefail
usage() { echo "usage: seal-check.sh <box.jsonl> <agent.jsonl> <run-dir>" >&2; exit 2; }
die() { echo "seal-check: $*" >&2; exit 2; }
[ "$#" -eq 3 ] || usage
boxlog=$1 agent=$2 run=${3%/}
case $run in /*[!/]*) ;; *) die "<run-dir> must be an absolute path below /: $3" ;; esac
jq empty "$boxlog" || die "$boxlog is not JSON lines"
jq empty "$agent" || die "$agent is not JSON lines"

# Box side: the cwd or the command names cargo's home or registry, the
# rehearsal registry, vendored or rustdoc sources, the repository or docs.rs,
# or a charpente crate directory; or it searches the filesystem, computes a
# path, or asks cargo where the sources are.
pat='registry|cargo_home|\.cargo\b|/usr\b|/opt\b|vendor/|target/doc|github\.com|githubusercontent|docs\.rs|static\.crates\.io|crates\.io/api|\.crate\b|charpente-([0-9]|(core|macros|sim|tsgen|cli)-[0-9])|charpente_|\*charpente|charpente\*|\b(find|locate|xargs|rg)\b|grep\s(.*\s)?-[a-z-]*r|cargo\s+(metadata|vendor|doc)\b|\$\(|`|(^|\s)/(\s|$)'
box=$(jq -nr --arg re "$pat" '
  [inputs | select(.event == "start")
   | if (.cwd | type) == "string" and (.command | type) == "string" then {seq, cwd, command}
     else error("start line without a cwd and a command: \(tojson)") end]
  | length, (map(select(.cwd + " " + .command | test($re; "i"))) | length, (.[] | tojson))' "$boxlog") \
  || die "could not read the start lines of $boxlog"
{ read -r starts; read -r nb; box_hits=$(cat); } <<<"$box"
[ "$starts" -gt 0 ] || die "$boxlog has no start lines"

# Mac side, default deny: a Bash call passes only as one ./box call with one
# single-quoted argument, a Read only inside the run directory; every other
# tool call is a hit. A tool_use can repeat across stream-json lines.
sq="\\A[.]/box '([^']|'\\\\'')*'\\z"
mac=$(jq -nr --arg run "$run" --arg cd1 "cd $run && " --arg cd2 "cd \"$run\" && " --arg re "$sq" '
  def strip: . as $c | ltrimstr($cd1) | if . == $c then ltrimstr($cd2) else . end;
  def boxcall: .name == "Bash" and ((.input.command? // "") | tostring | strip | test($re));
  def inrun: ((.input.file_path? // "") | tostring) as $p
    | ($p == $run or ($p | startswith($run + "/"))) and ($p | test("(^|/)[.][.](/|$)") | not);
  [inputs | .. | objects] as $o
  | [$o[] | select(.subtype? == "permission_denied") | .tool_use_id | tostring] as $denied
  | ([$o[] | select(.type? == "tool_result")
      | {key: (.tool_use_id | tostring), value: (if .is_error == true then "error" else "ran" end)}]
     | from_entries) as $res
  | [foreach ($o[] | select(.type? | strings | endswith("tool_use"))) as $u
      ({}; .[$u | {id, name, input} | tojson] += 1;
       if .[$u | {id, name, input} | tojson] == 1 then $u else empty end)] as $uses
  | ($uses | map(select(boxcall)) | length),
    ($uses | map(select((boxcall or (.name == "Read" and inrun)) | not)) | length,
      (.[] | (.id | tostring) as $i
       | "\(.name) \(.input | tojson) [\(if any($denied[]; . == $i) then "denied" else $res[$i] // "no result" end)]"))' "$agent") \
  || die "could not read the tool calls of $agent"
{ read -r calls; read -r nm; mac_hits=$(cat); } <<<"$mac"

echo "box side: $nb hit(s)"; [ "$nb" = 0 ] || printf '%s\n' "$box_hits" | sed 's/^/  /'
echo "mac side: $nm hit(s)"; [ "$nm" = 0 ] || printf '%s\n' "$mac_hits" | sed 's/^/  /'
echo "agent ./box calls: $calls, box start lines: $starts"
[ "$nb" = 0 ] && [ "$nm" = 0 ] && [ "$calls" = "$starts" ]
