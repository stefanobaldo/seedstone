#!/usr/bin/env bash
# One measurement run: throughput and server CPU per operation for one command
# shape, against a server that is already up.
#
# What a run is, and why each part is the way it is:
#
# - n operations of `redis-benchmark`, pinned to a client cpuset disjoint from
#   the server's. The client's own CPU is reported, so "was the client the
#   bottleneck" is answered by a number rather than assumed.
# - The server's CPU is read from /proc/<pid>/stat once before and once after
#   the run: utime and stime over every thread, at the kernel's clock tick.
#   Read around a million operations the window is thousands of ticks wide;
#   read around a single operation it would be quantised to nothing. Never
#   sample below the tick.
# - Throughput is taken from the `-q` summary by the text that precedes it,
#   because the same line carries live progress ahead of the summary.
# - The key distribution is on every output line. A figure that does not say
#   whether the load was spread over the keyspace or aimed at one key is a
#   figure about the harness: one hot key exercises one shard task.
# - `evicted_keys` is read from INFO around the run, so a row measured under a
#   memory ceiling says how much eviction it did per operation. An engine that
#   does not report the field prints `-`.
#
# Shapes:
#   get                 GET of a PAYLOAD-byte value           (-t get -d PAYLOAD)
#   set                 SET of a PAYLOAD-byte value           (explicit command)
#   set-ex <seconds>    SET ... EX <seconds>                  (explicit command)
#   set-large <bytes>   SET of a <bytes>-byte value           (explicit command)
#   mget <k>            MGET of <k> keys, each drawn independently
#
# The explicit-command form ignores -d, so the payload is a literal there; it
# is the same bytes on the wire. Its -q title is the command rather than the
# shape, and the parse does not depend on the title.
#
# Usage: cell.sh <port> <pid> <arm> <kind> <shape> <depth> <clients> [shape-arg]
#   kind is whatever the driver attaches — kept, warmup, cal — echoed so that a
#   reader of the log can tell a measurement from a discarded run.
# Env: N (1000000) KEYSPACE (100000) PAYLOAD (64) CLIENT_CPUS (10-15)
#      BENCH (redis-benchmark) CLI (redis-cli)
set -uo pipefail
export LC_ALL=C

USAGE="usage: cell.sh <port> <pid> <arm> <kind> <get|set|set-ex|set-large|mget> <depth> <clients> [shape-arg]"
PORT=${1:?$USAGE}
PID=${2:?$USAGE}
ARM=${3:?$USAGE}
KIND=${4:?$USAGE}
SHAPE=${5:?$USAGE}
DEPTH=${6:?$USAGE}
CLIENTS=${7:?$USAGE}
ARG=${8:-}

N=${N:-1000000}
KEYSPACE=${KEYSPACE:-100000}
PAYLOAD=${PAYLOAD:-64}
CLIENT_CPUS=${CLIENT_CPUS:-10-15}
BENCH=${BENCH:-redis-benchmark}
CLI=${CLI:-redis-cli}
HZ=$(getconf CLK_TCK)

value_of() { head -c "$1" /dev/zero | tr '\0' x; }

case $SHAPE in
  get)
    ARG=${ARG:--}; BYTES=$PAYLOAD
    CMD=(-t get -d "$PAYLOAD");;
  set)
    ARG=${ARG:--}; BYTES=$PAYLOAD
    CMD=(SET "key:__rand_int__" "$(value_of "$PAYLOAD")");;
  set-ex)
    ARG=${ARG:-60}; BYTES=$PAYLOAD
    CMD=(SET "key:__rand_int__" "$(value_of "$PAYLOAD")" EX "$ARG");;
  set-large)
    ARG=${ARG:-10240}; BYTES=$ARG
    CMD=(SET "key:__rand_int__" "$(value_of "$ARG")");;
  mget)
    ARG=${ARG:-1}; BYTES=$PAYLOAD
    CMD=(MGET); for _ in $(seq 1 "$ARG"); do CMD+=("key:__rand_int__"); done;;
  *) echo "$USAGE" >&2; exit 2;;
esac

# utime and stime of the whole process, in seconds. The comm field can hold
# spaces and parentheses, so everything up to the last ')' is discarded first;
# what remains starts at `state`, putting utime at index 12 and stime at 13.
cpu_of() {
  awk -v hz="$HZ" '{
    s = $0; sub(/^.*\) /, "", s); split(s, f, " ")
    printf "%.4f %.4f", f[12] / hz, f[13] / hz
  }' "/proc/$1/stat"
}

evicted_of() {
  "$CLI" -p "$PORT" info stats 2>/dev/null | tr -d '\r' \
    | sed -n 's/^evicted_keys:\([0-9]*\).*/\1/p'
}

read -r U0 S0 <<< "$(cpu_of "$PID")"
E0=$(evicted_of)

# bash reports the client's own user/system time for the pinned benchmark.
TIMEFORMAT='%3U %3S'
OUT=$(mktemp)
CLIENT=$( { time taskset -c "$CLIENT_CPUS" "$BENCH" -p "$PORT" -n "$N" -c "$CLIENTS" \
    -P "$DEPTH" -r "$KEYSPACE" -q "${CMD[@]}" > "$OUT" 2>/dev/null ; } 2>&1 )

read -r U1 S1 <<< "$(cpu_of "$PID")"
E1=$(evicted_of)

OPS=$(grep 'requests per second' "$OUT" \
  | sed -E 's/.*[[:space:]]([0-9.]+) requests per second.*/\1/')
rm -f "$OUT"

read -r CU CS <<< "$CLIENT"

awk -v arm="$ARM" -v kind="$KIND" -v shape="$SHAPE" -v arg="$ARG" -v d="$DEPTH" \
    -v c="$CLIENTS" -v k="$KEYSPACE" -v b="$BYTES" -v n="$N" -v ops="${OPS:-0}" \
    -v u0="$U0" -v s0="$S0" -v u1="$U1" -v s1="$S1" -v cu="$CU" -v cs="$CS" \
    -v e0="$E0" -v e1="$E1" \
  'BEGIN {
     u = u1 - u0; s = s1 - s0; tot = u + s; cl = cu + cs
     if (e0 == "" || e1 == "") { ev = "-"; evop = "-" }
     else { ev = sprintf("%d", e1 - e0); evop = sprintf("%.3f", (e1 - e0) / n) }
     printf "cell arm=%s kind=%s shape=%s arg=%s depth=%s clients=%s keyspace=%s payload=%s n=%s ops=%.2f user_us=%.3f sys_us=%.3f total_us=%.3f cores=%.2f client_cores=%.2f evicted=%s evicted_per_op=%s\n",
       arm, kind, shape, arg, d, c, k, b, n, ops, u * 1e6 / n, s * 1e6 / n,
       tot * 1e6 / n, ops * tot / n, ops * cl / n, ev, evop
   }'
