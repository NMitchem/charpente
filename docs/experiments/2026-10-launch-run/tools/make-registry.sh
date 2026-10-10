#!/usr/bin/env bash
# make-registry.sh <checkout> <out>: the rehearsal's stand-in for crates.io
# (launch-run spec §5): the six .crate files `cargo package` makes at HEAD,
# unpacked into a directory source beside the vendored dependency closure.
set -euo pipefail
[ "$#" -eq 2 ] || { echo "usage: make-registry.sh <checkout> <out>" >&2; exit 2; }
repo=$1 out=$2
if [ -n "$(git -C "$repo" status --porcelain)" ]; then
  echo "make-registry: $repo is dirty — package from a commit" >&2; exit 2
fi
version=$(grep -m1 '^version = ' "$repo/Cargo.toml" | sed 's/version = "\(.*\)"/\1/')
rm -rf "$out"; mkdir -p "$out/registry" "$out/crates"
( cd "$repo" && CARGO_TARGET_DIR="$out/target" cargo package --no-verify \
    -p charpente-core -p charpente-macros -p charpente -p charpente-sim -p charpente-tsgen -p charpente-cli )
cp "$out/target/package/"*.crate "$out/crates/"
( cd "$repo" && cargo vendor --versioned-dirs "$out/registry" >/dev/null )
for c in "$out/crates/"*.crate; do
  tar -xzf "$c" -C "$out/registry"
  sum=$(shasum -a 256 "$c" | cut -d' ' -f1)
  printf '{"files":{},"package":"%s"}' "$sum" > "$out/registry/$(basename "$c" .crate)/.cargo-checksum.json"
done
tar -xzf "$out/crates/charpente-cli-$version.crate" -O "charpente-cli-$version/README.md" > "$out/README.md"
( cd "$out/crates" && shasum -a 256 *.crate ) > "$out/crates.sha256"
rm -rf "$out/target"
echo "make-registry: $(ls "$out/crates" | wc -l | tr -d ' ') crate(s) at $(git -C "$repo" rev-parse --short HEAD), $(ls "$out/registry" | wc -l | tr -d ' ') source dir(s), README sha256 $(shasum -a 256 "$out/README.md" | cut -c1-16)"
