#!/usr/bin/env bash
# The one version the workspace carries, set or checked in every place the
# release reads it. `--check X.Y.Z` exits non-zero if any of the four
# published crates reports another version: a tag that disagrees with the
# workspace would publish an archive that lies about itself, or a server
# crate that depends on a library version that does not exist.
#
#   set-version.sh --check <version>
#   set-version.sh <version>
set -euo pipefail
mode=${1:?--check <version> | <version>}
if [ "$mode" = --check ]; then
  want=${2:?version}
  status=0
  # The crates a release publishes; the simulator is not one of them. Read
  # from the manifests, which are what `cargo publish` uploads, not from
  # `cargo pkgid`, which reports the lockfile's entry and so agrees with a
  # stale lock; a lock that disagrees with the manifests is `--locked`'s to
  # refuse.
  versions="$(cargo metadata --no-deps --format-version 1 \
    | jq -r '.packages[] | "\(.name) \(.version)"')"
  for crate in seedstone seedstone-core seedstone-resp seedstone-service; do
    have="$(awk -v c="$crate" '$1 == c { print $2 }' <<<"$versions")"
    [ "$have" = "$want" ] || { echo "$crate is ${have:-absent}, the tag says $want" >&2; status=1; }
  done
  exit "$status"
fi
want=$mode
# The root manifest is the one place the version lives: the package version
# and the three exact dependency versions. `Cargo.lock` records the crates'
# own versions too, and `--locked` refuses a lock that disagrees.
sed -i.bak -E \
  -e "s/^(version = )\"[^\"]+\"/\1\"$want\"/" \
  -e "s/^(seedstone-(core|resp|service) = \{ path = \"[^\"]+\", version = )\"=[^\"]+\"/\1\"=$want\"/" \
  Cargo.toml && rm Cargo.toml.bak
cargo update -p seedstone -p seedstone-core -p seedstone-resp -p seedstone-service --precise "$want" --offline 2>/dev/null \
  || cargo build -p seedstone >/dev/null
bash "$0" --check "$want"
git --no-pager diff --stat -- Cargo.toml Cargo.lock
