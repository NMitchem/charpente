#!/usr/bin/env bash
# Publish the six charpente crates to crates.io, or (default) prove that they would
# publish. Spec: docs/superpowers/specs/2026-09-13-publish-path-design.md §3.
#
# `cargo publish --workspace` (cargo >= 1.90) orders the crates itself and skips
# every `publish = false` member. The dry-run here is the FULL one — it verify-builds
# each packaged crate — which is the half CI's --no-verify step deliberately leaves
# out. Run this once before every real publish; there is no other place it runs.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -n "$(git status --porcelain)" ]]; then
  echo "publish.sh: the tree is dirty — commit or stash first; a publish must be reproducible from a commit" >&2
  git status --short >&2
  exit 2
fi

version="$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')"
head="$(git rev-parse --short HEAD)"

case "${1:-}" in
  "")
    echo "publish.sh: DRY RUN of charpente $version at $head (full verify build; pass --for-real to publish)"
    cargo publish --workspace --dry-run
    ;;
  --for-real)
    echo "publish.sh: PUBLISHING charpente $version at $head to crates.io"
    cargo publish --workspace
    ;;
  *)
    echo "usage: scripts/publish.sh [--for-real]   (default: dry run with verify)" >&2
    exit 2
    ;;
esac
