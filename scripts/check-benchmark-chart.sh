#!/usr/bin/env bash
# The README's benchmark chart is the output of `bench/report.py --svg` over
# the committed raw logs, never drawn by hand. Each chart's first line names
# the inputs it was rendered from; this regenerates it from them and refuses
# a byte of difference, so the chart cannot drift from the logs or the logs
# change under the chart.
#
# Usage: check-benchmark-chart.sh            (from the repository root)
set -euo pipefail

status=0
for chart in docs/assets/benchmarks.svg docs/assets/benchmarks-dark.svg; do
  inputs=$(sed -n '1s/^<!-- inputs: \(.*\) -->$/\1/p' "$chart")
  if [ -z "$inputs" ]; then
    echo "$chart: the first line does not name its inputs" >&2
    exit 1
  fi
  tmp=$(mktemp)
  # Word-split on purpose: the values carry no spaces, by report.py's refusal.
  # shellcheck disable=SC2086
  python3 bench/report.py --svg "$tmp" $inputs
  if ! cmp -s "$tmp" "$chart"; then
    echo "$chart is not what its inputs render; regenerate it:" >&2
    echo "  python3 bench/report.py --svg $chart $inputs" >&2
    status=1
  fi
  rm -f "$tmp"
done
exit "$status"
