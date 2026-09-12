#!/usr/bin/env bash
#
# Every Rust source file in the tree stays under a size a reader can hold in
# one sitting, tests included. The number is a reading budget, not a style
# preference: a module past it gets reviewed by searching, and what is
# reviewed by searching is not reviewed. A file that needs more room is a
# file with more than one concern in it — split it by concern.
#
# Usage: module-bound.sh            (from the repository root)
set -euo pipefail

BOUND=1500
status=0

while IFS= read -r file; do
  lines=$(wc -l < "$file")
  if [ "$lines" -gt "$BOUND" ]; then
    printf '%s: %d lines, over the %d-line bound\n' "$file" "$lines" "$BOUND"
    status=1
  fi
done < <(git ls-files -- '*.rs')

exit "$status"
