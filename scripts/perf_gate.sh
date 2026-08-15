#!/usr/bin/env bash
#
# Self-relative performance gate: two builds of this server, one runner, one
# job — the verdict is the ratio between them. Absolute numbers here are
# runner noise; they are printed for the log and never compared across runs
# or recorded as claims. redis-benchmark is the load generator, nothing else.
#
# What this gate promises, and what it does not. The load generator shares the
# runner's cores with the server it is measuring, so the throughput printed
# below is a joint measurement of both and of whoever else holds the machine.
# Two builds measured minutes apart on that same machine still compare: a
# regression that costs a third of the throughput shows up through the noise.
# One that costs three percent does not, and nothing here should be read as
# saying it did not happen.
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

# The shape both passes share. Spread keys (-r): load on one key measures one
# shard task of 1024, not the server. Declared here because a cell that does
# not say so is a cell about the harness.
SHAPE=(-d 64 -P 64 -c 50 -r 100000)
# Populating is also the warm-up, in one gesture. A million random writes over
# a hundred thousand keys leaves essentially none of them unwritten, so the
# measured GETs below find something: against an empty keyspace every GET
# misses, and a null reply exercises neither the value copy nor anything else
# a regression would plausibly land in. It also pays for the accept path, the
# dict's first allocations and the runner's page faults, which charging to
# whichever arm ran first would be a difference between the builds that is not
# one.
POPULATE=(-t set -n 1000000 "${SHAPE[@]}")
# Ten million, well past what this started at: with a smaller count the pass
# returns before the runner's scheduler has averaged out, and the median of
# three then reports which way it leaned rather than which build it was
# handed. Sustained load is what makes the median mean anything.
MEASURE=(-t get -n 10000000 "${SHAPE[@]}")
# redis-benchmark writes `key:` followed by the random integer padded to
# twelve digits. Any one of these being present says the population pass
# landed; all three missing is a harness that changed under this script, not a
# server that got slower.
PROBES=(key:000000000001 key:000000000042 key:000000099999)

# One measured throughput for one binary, in ops/s.
bench() {
    local bin=$1 pid ops probe populated=0
    "$bin" --bind "127.0.0.1:$PORT" &
    pid=$!
    # Poll rather than sleep: a fixed sleep is either flaky or slower than it
    # needs to be, and a benchmark that starts before the listener is up
    # measures the retry, not the server.
    for _ in $(seq 50); do
        redis-cli -p "$PORT" ping >/dev/null 2>&1 && break
        sleep 0.1
    done
    redis-benchmark -p "$PORT" "${POPULATE[@]}" --csv >/dev/null
    for probe in "${PROBES[@]}"; do
        if [ -n "$(redis-cli -p "$PORT" get "$probe")" ]; then
            populated=1
            break
        fi
    done
    if [ "$populated" -eq 0 ]; then
        echo "the population pass left nothing behind: this cell would measure" \
             "the miss path and call it a verdict" >&2
        exit 1
    fi
    ops=$(redis-benchmark -p "$PORT" "${MEASURE[@]}" --csv | awk -F'"' '/GET/ { print $4 }')
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
#
# Five, not three. With three, a single pass that the runner sat on could
# still move the median — one A/A comparison came back four percent from
# equality with its arms spread eight percent internally, wider than anything
# the calibration set had seen. Two more pairs cost seconds and buy a median
# that an outlier has to work much harder to reach.
base_runs=()
head_runs=()
for _ in 1 2 3 4 5; do
    base_runs+=("$(bench "$BASE_BIN")")
    head_runs+=("$(bench "$HEAD_BIN")")
done

# The middle of however many were handed in, so the count above can move
# without this quietly reporting the wrong element.
median() { printf '%s\n' "$@" | sort -n | sed -n "$(( ($# + 1) / 2 ))p"; }
base=$(median "${base_runs[@]}")
head=$(median "${head_runs[@]}")
ratio=$(awk -v h="$head" -v b="$base" 'BEGIN { printf "%.4f", h / b }')
echo "base runs: ${base_runs[*]}"
echo "head runs: ${head_runs[*]}"
echo "base=$base head=$head ratio=$ratio floor=$FLOOR"
awk -v r="$ratio" -v t="$FLOOR" 'BEGIN { exit !(r >= t) }'
