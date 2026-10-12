#!/usr/bin/env bash
# The one version the release reads, checked in every place it reads it.
# `--check X.Y.Z` exits non-zero if any published crate reports another
# version: a tag that disagrees with the workspace would publish an archive
# that lies about itself.
#
#   set-version.sh --check <version>
#   set-version.sh <version>           (not implemented yet)
set -euo pipefail
mode=${1:?--check <version> | <version>}
if [ "$mode" = --check ]; then
  want=${2:?version}
  status=0
  # The crates a release publishes. Today that is the server alone; the
  # libraries carry placeholder versions and are not published.
  for crate in seedstone; do
    have="$(cargo pkgid -p "$crate" | sed 's/.*[#@]//')"
    [ "$have" = "$want" ] || { echo "$crate is $have, the tag says $want" >&2; status=1; }
  done
  exit "$status"
fi
echo "setting the version is not implemented yet" >&2; exit 2
