#!/usr/bin/env bash
# replay.sh <box-a> <app-dir-in-a> <image> <out-dir> '<repro>' [<registry-dir>]
# Launch-run spec §2(d): the controller's replay in a second fresh box, with
# charpente installed fresh and the agent's app copied over without target/.
set -euo pipefail
[ "$#" -ge 5 ] && [ "$#" -le 6 ] || { echo "usage: replay.sh <box-a> <app-dir-in-a> <image> <out-dir> '<repro>' [<registry-dir>]" >&2; exit 2; }
a=$1 app=$2 image=$3 out=$4 repro=$5 reg=${6:-}
here=$(cd "$(dirname "$0")" && pwd)
b="$a-replay"
mkdir -p "$out"
bash "$here/mkbox.sh" "$b" "$image" "$out/run" "$out/replay.box.jsonl" ${reg:+"$reg"}
parent=$(dirname "$app"); base=$(basename "$app")
docker exec "$a" tar -C "$parent" --exclude="$base/target" -cf - "$base" | docker exec -i "$b" tar -C /root -xf -
"$out/run/box" 'cargo install charpente-cli' > "$out/install.log" 2>&1
set +e
"$out/run/box" "cd /root/$base && $repro" > "$out/replay.stdout" 2> "$out/replay.stderr"
echo "replay.sh: repro exit $?"
