#!/usr/bin/env bash
# mkbox.sh <container> <image> <run-dir> <log> [<registry-dir>]
# A fresh box for one attempt (launch-run spec §4). With <registry-dir> it is a
# rehearsal box: no network, crates.io replaced by that directory source.
set -euo pipefail
if [ "$#" -lt 4 ] || [ "$#" -gt 5 ]; then
  echo "usage: mkbox.sh <container> <image> <run-dir> <log> [<registry-dir>]" >&2; exit 2
fi
name=$1 image=$2 run=$3 log=$4 reg=${5:-}
here=$(cd "$(dirname "$0")" && pwd)
if docker container inspect "$name" >/dev/null 2>&1; then
  echo "mkbox: a container named $name exists — every attempt gets a fresh one" >&2; exit 2
fi
net=bridge; [ -n "$reg" ] && net=none
docker run -d --init --name "$name" --network "$net" "$image" sleep infinity >/dev/null
docker exec "$name" bash -c 'echo /root > /tmp/.box-cwd'
if [ -n "$reg" ]; then
  docker cp "$reg" "$name:/opt/registry"
  docker exec "$name" bash -c 'printf "[source.crates-io]\nreplace-with = \"rehearsal\"\n\n[source.rehearsal]\ndirectory = \"/opt/registry\"\n" > "${CARGO_HOME:-/root/.cargo}/config.toml"'
fi
mkdir -p "$run"
sed -e "s|@BOX@|$name|" -e "s|@LOG@|$log|" "$here/box.template" > "$run/box"
chmod +x "$run/box"
: > "$log"; echo 0 > "$log.seq"
echo "mkbox: $name (network $net) from $image; $run/box logs to $log"
