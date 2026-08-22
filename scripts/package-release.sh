#!/usr/bin/env bash
# Packages a built seedstone binary as a release archive with its checksum.
#
#   package-release.sh <binary> <version> <target-triple> <out-dir>
#
# Run by the release workflow on a tag, and by CI on every pull request so
# the packaging is never exercised for the first time on release day. The
# version is an argument rather than a reading of `Cargo.toml` because the
# two callers name the same bytes differently: a tag names a version, a pull
# request names nothing anyone will download.
set -euo pipefail
binary="${1:?binary}"; version="${2:?version}"; target="${3:?target}"; out="${4:?out dir}"
name="seedstone-${version}-${target}"
stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
mkdir -p "$stage/$name" "$out"
cp "$binary" "$stage/$name/seedstone"
cp LICENSE-APACHE LICENSE-MIT README.md CHANGELOG.md "$stage/$name/"
tar -C "$stage" -czf "$out/$name.tar.gz" "$name"
( cd "$out" && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256" )
# The archive must contain a binary that runs, and one that can say what it
# is: a release nobody can identify once it is unpacked is not a release.
# Unpacked from the archive rather than checked in place, so what is asserted
# is what someone downloading it would get.
tar -xzf "$out/$name.tar.gz" -C "$stage" "$name/seedstone"
unpacked="$stage/$name/seedstone"
usage="$("$unpacked" --help 2>&1)"
case "$usage" in
  'usage: seedstone'*) ;;
  *) echo "the packaged binary does not print its usage: $usage" >&2; exit 1;;
esac
reported="$("$unpacked" --version 2>&1)"
case "$reported" in
  'seedstone '?*) ;;
  *) echo "the packaged binary does not report its version: $reported" >&2; exit 1;;
esac
echo "packaged $out/$name.tar.gz ($reported)"
