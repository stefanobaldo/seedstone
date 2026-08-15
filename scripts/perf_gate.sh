#!/usr/bin/env bash
#
# Self-relative performance gate: two builds of this server, one runner, one
# job — the verdict is the ratio between them. Absolute numbers here are
# runner noise; they are printed for the log and never compared across runs
# or recorded as claims. redis-benchmark is the load generator, nothing else.
#
# Usage: perf_gate.sh <base-bin> <head-bin> <ratio-floor>
set -euo pipefail

# The ratio is computed by `awk` and read back by `awk`. Under a locale that
# prints `0,96`, the second one parses that as zero and the gate fails a build
# it never measured — so the numeric locale is fixed here rather than assumed.
export LC_ALL=C

BASE_BIN=${1:?usage: perf_gate.sh <base-bin> <head-bin> <ratio-floor>}
HEAD_BIN=${2:?usage: perf_gate.sh <base-bin> <head-bin> <ratio-floor>}
FLOOR=${3:?usage: perf_gate.sh <base-bin> <head-bin> <ratio-floor>}
PORT=6395
# Spread keys (-r): load on one key measures one shard task of 1024, not the
# server. Declared here because a cell that does not say so is a cell about
# the harness.
CELL=(-t get -n 200000 -d 64 -P 64 -c 50 -r 100000)

# One measured throughput for one binary, in ops/s.
#
# Both passes are needed: the first one pays for the accept path, the dict's
# first allocations and the runner's page faults, and charging those to the
# arm that happens to run first would be a difference between the builds that
# is not one.
bench() {
    local bin=$1 pid ops
    "$bin" --bind "127.0.0.1:$PORT" &
    pid=$!
    # Poll rather than sleep: a fixed sleep is either flaky or slower than it
    # needs to be, and a benchmark that starts before the listener is up
    # measures the retry, not the server.
    for _ in $(seq 50); do
        redis-cli -p "$PORT" ping >/dev/null 2>&1 && break
        sleep 0.1
    done
    redis-benchmark -p "$PORT" "${CELL[@]}" --csv >/dev/null # warm-up pass
    ops=$(redis-benchmark -p "$PORT" "${CELL[@]}" --csv | awk -F'"' '/GET/ { print $4 }')
    kill "$pid" && wait "$pid" 2>/dev/null || true
    # An empty reading would reach `awk` below as a zero and read as a verdict.
    # It is a broken harness instead, and says so.
    if [ -z "$ops" ]; then
        echo "no GET throughput parsed out of redis-benchmark --csv for $bin" >&2
        exit 1
    fi
    echo "$ops"
}

# Alternating pairs, so slow drift on the runner lands on both arms evenly.
base_runs=()
head_runs=()
for _ in 1 2 3; do
    base_runs+=("$(bench "$BASE_BIN")")
    head_runs+=("$(bench "$HEAD_BIN")")
done

median() { printf '%s\n' "$@" | sort -n | sed -n 2p; }
base=$(median "${base_runs[@]}")
head=$(median "${head_runs[@]}")
ratio=$(awk -v h="$head" -v b="$base" 'BEGIN { printf "%.4f", h / b }')
echo "base runs: ${base_runs[*]}"
echo "head runs: ${head_runs[*]}"
echo "base=$base head=$head ratio=$ratio floor=$FLOOR"
awk -v r="$ratio" -v t="$FLOOR" 'BEGIN { exit !(r >= t) }'
