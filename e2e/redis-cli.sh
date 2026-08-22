#!/usr/bin/env bash
#
# Drives a seedstone binary with the tools that ship with Redis itself.
#
# The point is not coverage — the unit and integration suites are where the
# surface is exercised exhaustively. It is that a program nobody here wrote,
# speaking the protocol as it actually implements it, connects and gets
# answers it accepts. `redis-cli` sends `COMMAND DOCS` before it prints a
# prompt; `redis-benchmark` opens fifty connections and pipelines; neither
# asked us what we support.
#
# Usage: redis-cli.sh path/to/seedstone [port]
set -euo pipefail

BIN=${1:?usage: redis-cli.sh path/to/seedstone [port]}
PORT=${2:-6390}

# `--no-auth` rather than nothing: this is the one lane that runs open, and it
# says so. Every other lane authenticates, so both paths through the edge are
# exercised — and on an address the flag is not needed for, which keeps the
# refusal an open bind gets out of this lane's way.
"$BIN" --bind "127.0.0.1:$PORT" --no-auth &
SERVER=$!
trap 'kill "$SERVER" 2>/dev/null || true' EXIT

# The server binds before it prints, but the shell got here first. Poll rather
# than sleep: a fixed sleep is either flaky or slower than it needs to be.
for _ in $(seq 50); do
    redis-cli -p "$PORT" ping >/dev/null 2>&1 && break
    sleep 0.1
done

r() { redis-cli -p "$PORT" "$@"; }

# Fails loudly with both sides of the comparison, which `[ x = y ]` does not.
expect() {
    local what=$1 want=$2 got
    shift 2
    got=$("$@")
    if [ "$got" != "$want" ]; then
        echo "$what: expected '$want', got '$got'" >&2
        exit 1
    fi
}

expect "ping" PONG r ping
expect "echo" hi r echo hi
expect "set" OK r set k v
expect "get" v r get k
expect "set with expiry" OK r set k2 v2 EX 100

# A range rather than an equality: the second between the SET and the TTL is
# real time on a real clock, and a gate that fails when the machine is busy
# teaches people to re-run it.
ttl=$(r ttl k2)
if [ "$ttl" -le 90 ] || [ "$ttl" -gt 100 ]; then
    echo "ttl: expected 90 < ttl <= 100, got '$ttl'" >&2
    exit 1
fi

expect "ttl without deadline" -1 r ttl k
expect "ttl of a missing key" -2 r ttl missing
expect "exists counts what is there" 2 r exists k k2 missing
expect "del removes both" 2 r del k k2
expect "expire on a missing key" 0 r expire missing 10
expect "incrby from nothing" 5 r incrby n 5
expect "set nx on a fresh key" OK r set fresh a NX
# nil prints as an empty line, which is how the refusal is observed here.
expect "set nx on a taken key" "" r set fresh b NX
expect "the value the refusal left alone" a r get fresh

r info | grep -q '^# Server'
r info | grep -q '^connected_clients:'

# Exit code is the whole assertion: the benchmark fails the run if a single
# reply is malformed, and it is the only client here that pipelines.
redis-benchmark -p "$PORT" -n 1000 -c 8 -t set,get -q

echo e2e-cli-ok
