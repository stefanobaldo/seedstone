#!/usr/bin/env bash
# Renders the Homebrew formula from the template and a release's checksums.
#
#   render-formula.sh <version> <dist-dir>     > Formula/seedstone.rb
#
# <dist-dir> holds seedstone-<version>-<target>.tar.gz.sha256 for the four
# targets, as the release workflow's `build` job writes them. A missing
# checksum is a refusal, not a formula with a blank in it.
set -euo pipefail
version=${1:?version}; dist=${2:?dist dir}
here=$(cd "$(dirname "$0")/.." && pwd)
out=$(cat "$here/packaging/homebrew/seedstone.rb.tmpl")
out=${out//@VERSION@/$version}
for target in aarch64-apple-darwin x86_64-apple-darwin aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu; do
  f="$dist/seedstone-$version-$target.tar.gz.sha256"
  [ -f "$f" ] || { echo "no checksum for $target: $f" >&2; exit 1; }
  sha=$(cut -d' ' -f1 "$f")
  [ ${#sha} = 64 ] || { echo "$f does not hold a sha256: $sha" >&2; exit 1; }
  key="@SHA_$(echo "$target" | tr 'a-z-' 'A-Z_')@"
  out=${out//$key/$sha}
done
case "$out" in *@SHA_*|*@VERSION@*) echo "a placeholder survived rendering" >&2; exit 1;; esac
printf '%s\n' "$out"
