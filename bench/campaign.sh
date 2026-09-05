#!/usr/bin/env bash
# The benchmark campaign: one stage per invocation, in the only order that
# makes the later stages readable.
#
#   canary     Redis at io-threads 1 on the reference shape, against the
#              reference machine's reference figure. A gate: outside the
#              tolerance this exits non-zero, and nothing measured afterwards
#              may be compared with the published table.
#   calibrate  twelve runs per arm on the reference shape. Its whole output is
#              one integer, W — how many runs to discard before the kept ones —
#              by a rule fixed in advance and applied by report.py. Every
#              figure it prints is discarded.
#   field      GET of 64-byte values at pipeline depths 1, 4, 16 and 64.
#   expiry     SET of 64-byte values at depth 64, without and with EX 60.
#   eviction   SET of 10 240-byte values at depth 64 under a 384 MB ceiling,
#              measured in steady-state eviction. Garnet does not take part:
#              its memory bound is a log size with tail reclamation, not a
#              ceiling with LRU eviction, and a comparable cell does not exist.
#   multikey   MGET of 1, 4 and 16 keys at depth 64.
#
# Discipline, every stage: one server up at a time, pinned to SERVER_CPUS, the
# client pinned to CLIENT_CPUS; the keyspace populated before a read cell by
# the same step every time and probed to show it landed; W discarded runs
# printed rather than hidden; three kept runs; medians taken by report.py,
# never here. No idle wait between arms — the load average is printed before
# every one instead, so a reader can see what the machine was doing.
#
# Each engine receives the configuration that matches the hardware it is
# given, where it has a knob for that, and nothing else: Redis and Valkey get
# io-threads 1 (their default) and 4; Dragonfly gets one proactor per server
# core; Garnet sizes its own threads. No allocator, hugepage or affinity
# tuning, for any arm.
#
# Usage: campaign.sh <canary|calibrate|field|expiry|eviction|multikey|all>
#        WARMUP=<W> is required by field, expiry, eviction and multikey.
#        ARMS=<comma list> restricts the arms; the default is all seven.
# Paths and cpusets are environment variables with the reference machine's
# values as defaults, so the script runs elsewhere with none of them edited.
set -uo pipefail
export LC_ALL=C

HERE=$(cd "$(dirname "$0")" && pwd)
CELL=$HERE/cell.sh
SERVER_CPUS=${SERVER_CPUS:-0-9}
export CLIENT_CPUS=${CLIENT_CPUS:-10-15}
export CLI=${CLI:-$HOME/redis-8.10.0/src/redis-cli}
export BENCH=${BENCH:-$HOME/redis-8.10.0/src/redis-benchmark}
SEEDSTONE_BIN=${SEEDSTONE_BIN:-$HOME/src/seedstone-v0.1.0/target/release/seedstone}
REDIS_SERVER=${REDIS_SERVER:-$HOME/redis-8.10.0/src/redis-server}
VALKEY_SERVER=${VALKEY_SERVER:-$HOME/valkey-9.1.1/src/valkey-server}
DRAGONFLY=${DRAGONFLY:-$HOME/dragonfly/dragonfly-aarch64}
GARNET=${GARNET:-$HOME/garnet/GarnetServer}
CEILING=${CEILING:-384mb}
ARMS=${ARMS:-seedstone,redis-iot1,redis-iot4,valkey-iot1,valkey-iot4,dragonfly,garnet}
CAL_RUNS=${CAL_RUNS:-12}
WARMUP=${WARMUP:-}

# The reference machine's reference figure: Redis at io-threads 1 on the
# reference shape. A run whose canary lands outside the tolerance was not made
# on a comparable machine, and its figures are not comparable to the published
# table.
CANARY_REFERENCE=2551021
CANARY_TOLERANCE=5

STAGE=${1:?usage: campaign.sh <canary|calibrate|field|expiry|eviction|multikey|all>}

wait_port() {
  for _ in $(seq 1 150); do
    "$CLI" -p "$1" ping 2>/dev/null | grep -q PONG && return 0
    sleep 0.2
  done
  echo "FATAL: port $1 never answered" >&2; return 1
}

populate() {
  taskset -c "$CLIENT_CPUS" "$BENCH" -p "$1" -t set -n 300000 -c 50 -P 64 -d 64 -r 100000 -q >/dev/null 2>&1
}

# Did the population land? `dbsize` answers where it is implemented; the keys
# are read back as well. redis-benchmark writes `__rand_int__` zero-padded to
# twelve digits, so the probe key is `key:%012d`.
probe_hits() {
  local port=$1 hits=0 i
  for i in $(seq 1 50); do
    [[ -n $("$CLI" -p "$port" get "$(printf 'key:%012d' $(( (i * 1997) % 100000 )))" 2>/dev/null) ]] \
      && hits=$((hits + 1))
  done
  echo "    probe: $hits/50 hits on key:%012d  dbsize=$("$CLI" -p "$port" dbsize 2>/dev/null | tr -d '\r')"
}

banner() {
  echo "### stage $1 start $(date -u +%FT%TZ)"
  echo "### load at start:$(cut -d' ' -f1-3 /proc/loadavg | sed 's/^/ /')"
  echo "### kernel $(uname -r) $(uname -m)  cpus server=$SERVER_CPUS client=$CLIENT_CPUS"
  echo "### seedstone $("$SEEDSTONE_BIN" --version 2>/dev/null)  benchmark $("$BENCH" --version 2>/dev/null | head -1)"
  echo
}

versions() {
  "$CLI" -p "$1" info server 2>/dev/null | tr -d '\r' | grep -iE '_version|^os:' | sed 's/^/    /'
}

need() { [[ -x $1 ]] || { echo "FATAL: $2 binary not executable: $1" >&2; exit 1; }; }

# start <port> <arm> <populate|clean> <workdir> -- <server cmd...>
# Leaves $PID set to the server's pid. `populate` runs the standard population
# step and probes it; `clean` starts empty, which the eviction stage declares.
start() {
  local port=$1 arm=$2 mode=$3 workdir=$4; shift 5
  ( cd "$workdir" && exec taskset -c "$SERVER_CPUS" "$@" ) >"/tmp/bench-$port.log" 2>&1 &
  PID=$!
  if ! wait_port "$port"; then
    echo "!!! $arm failed to start"; tail -5 "/tmp/bench-$port.log"; return 1
  fi
  echo "--- $arm [$mode] pid=$PID threads=$(find /proc/"$PID"/task -mindepth 1 -maxdepth 1 | wc -l) load:$(cut -d' ' -f1 /proc/loadavg)"
  echo "    cmd: $*"
  versions "$port"
  if [[ $mode == populate ]]; then
    populate "$port"
    probe_hits "$port"
  fi
}

stop() { kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; sleep 2; echo; }

# arm_start <arm> <populate|clean> <plain|ceiling>
# Every start line lives here and nowhere else, so the configuration an arm
# ran with is in one place and in the log.
arm_start() {
  local arm=$1 mode=$2 bound=$3
  case $arm in
    seedstone)
      need "$SEEDSTONE_BIN" seedstone; PORT=6390
      local extra=(); [[ $bound == ceiling ]] && extra=(--maxmemory "$CEILING" --maxmemory-policy allkeys-lru)
      start "$PORT" "$arm" "$mode" . -- "$SEEDSTONE_BIN" --bind "127.0.0.1:$PORT" --max-clients 2000 --no-auth "${extra[@]}";;
    redis-iot1|redis-iot4)
      need "$REDIS_SERVER" redis; PORT=6391
      local io=${arm#redis-iot} extra=()
      [[ $bound == ceiling ]] && extra=(--maxmemory "$CEILING" --maxmemory-policy allkeys-lru)
      start "$PORT" "$arm" "$mode" . -- "$REDIS_SERVER" --port "$PORT" --save '' --appendonly no --io-threads "$io" "${extra[@]}";;
    valkey-iot1|valkey-iot4)
      need "$VALKEY_SERVER" valkey; PORT=6393
      local io=${arm#valkey-iot} extra=()
      [[ $bound == ceiling ]] && extra=(--maxmemory "$CEILING" --maxmemory-policy allkeys-lru)
      start "$PORT" "$arm" "$mode" . -- "$VALKEY_SERVER" --port "$PORT" --save '' --appendonly no --io-threads "$io" "${extra[@]}";;
    dragonfly)
      need "$DRAGONFLY" dragonfly; PORT=6392
      local extra=(); [[ $bound == ceiling ]] && extra=("--maxmemory=$CEILING" --cache_mode=true)
      start "$PORT" "$arm" "$mode" . -- "$DRAGONFLY" --port=$PORT --proactor_threads=10 --dbfilename= --logtostderr=false "${extra[@]}";;
    garnet)
      need "$GARNET" garnet; PORT=6394
      # GarnetServer resolves its runtime assemblies from its own directory.
      start "$PORT" "$arm" "$mode" "$(dirname "$GARNET")" -- "$GARNET" --port "$PORT";;
    *) echo "FATAL: unknown arm $arm" >&2; exit 1;;
  esac
}

# runs <port> <arm> <shape> <depth> <shape-arg>: WARMUP discarded, three kept.
runs() {
  local port=$1 arm=$2 shape=$3 depth=$4 arg=$5 i
  : "${WARMUP:?this stage needs WARMUP=<W>, derived from the calibrate stage by report.py}"
  for i in $(seq 1 "$WARMUP"); do bash "$CELL" "$port" "$PID" "$arm" warmup "$shape" "$depth" 50 "$arg"; done
  for _ in 1 2 3;               do bash "$CELL" "$port" "$PID" "$arm" kept   "$shape" "$depth" 50 "$arg"; done
}

canary() {
  banner canary
  echo "### canary: redis io-threads 1, GET 64 B, depth 64, 50 clients, 100 000 spread keys, populated"
  echo "### reference $CANARY_REFERENCE ops/s, tolerance +-$CANARY_TOLERANCE %"
  arm_start redis-iot1 populate plain || exit 1
  echo "    io-threads=$("$CLI" -p "$PORT" config get io-threads | tail -1 | tr -d '\r')"
  local readings=/tmp/bench-canary.txt
  for _ in 1 2 3; do bash "$CELL" "$PORT" "$PID" redis-iot1 kept get 64 50; done | tee "$readings"
  stop
  local median
  median=$(sed -E 's/.* ops=([0-9.]+) .*/\1/' "$readings" | sort -n | sed -n 2p)
  awk -v m="$median" -v r="$CANARY_REFERENCE" -v t="$CANARY_TOLERANCE" 'BEGIN {
    if (m + 0 == 0) { print "### CANARY UNREADABLE: no ops figure parsed"; exit 1 }
    d = 100 * (m - r) / r
    printf "### CANARY median=%.2f reference=%d delta=%+.2f%% tolerance=+-%d%%\n", m, r, d, t
    if (d < 0) d = -d
    if (d > t) { print "### CANARY FAILED: this machine is not comparable to the reference; stop here"; exit 1 }
    print "### CANARY PASSED"
  }'
}

each_arm() {  # each_arm <populate|clean> <plain|ceiling> <fn> [skip-arm]
  local mode=$1 bound=$2 fn=$3 skip=${4:-} arm
  for arm in ${ARMS//,/ }; do
    [[ $arm == "$skip" ]] && { echo "### $arm skipped in this stage, by design"; echo; continue; }
    arm_start "$arm" "$mode" "$bound" || exit 1
    "$fn" "$arm"
    stop
  done
}

calibrate() {
  banner calibrate
  echo "### calibrate: how many runs until each arm settles. NOTHING HERE IS A MEASUREMENT."
  echo "### Every figure below is discarded; this stage's output is one integer, W."
  echo "### Rule, fixed in advance: per arm, the smallest i such that runs i, i+1, i+2 lie"
  echo "### within 2 % (max - min <= 2 % of their median); that arm needs i-1 discarded runs."
  echo "### W is the largest i-1 across all arms. Cap $CAL_RUNS runs; an arm that never"
  echo "### settles inside the cap is a finding, not a number to round."
  cal_arm() { local i; for i in $(seq 1 "$CAL_RUNS"); do bash "$CELL" "$PORT" "$PID" "$1" cal get 64 50; done; }
  each_arm populate plain cal_arm
}

field() {
  banner field
  echo "### field: GET 64 B at depths 1, 4, 16, 64; 50 clients; 100 000 spread keys; populated; W=$WARMUP"
  field_arm() { local d; for d in 1 4 16 64; do runs "$PORT" "$1" get "$d" -; done; }
  each_arm populate plain field_arm
}

expiry() {
  banner expiry
  echo "### expiry: SET 64 B at depth 64, without and with EX 60; 50 clients; 100 000 spread keys; W=$WARMUP"
  expiry_arm() {
    runs "$PORT" "$1" set 64 -
    runs "$PORT" "$1" set-ex 64 60
    echo "    ttl probe: $("$CLI" -p "$PORT" ttl key:000000012345 2>/dev/null | tr -d '\r') (a deadline reached the server)"
  }
  each_arm populate plain expiry_arm
}

eviction() {
  banner eviction
  echo "### eviction: SET 10 240 B at depth 64 under a $CEILING ceiling, LRU where the engine has it;"
  echo "### the keyspace is filled past the ceiling first (discarded), then W discarded runs, then"
  echo "### three kept, so every kept run is in steady-state eviction. evicted_keys per operation"
  echo "### is on every row. Garnet does not take part: its memory bound is not a ceiling with LRU."
  eviction_arm() {
    local value; value=$(head -c 10240 /dev/zero | tr '\0' x)
    taskset -c "$CLIENT_CPUS" "$BENCH" -p "$PORT" -n 60000 -c 50 -P 64 -r 100000 -q SET "key:__rand_int__" "$value" >/dev/null 2>&1
    echo "    filled past the ceiling: $("$CLI" -p "$PORT" info stats 2>/dev/null | tr -d '\r' | grep '^evicted_keys' || echo 'evicted_keys not reported')"
    runs "$PORT" "$1" set-large 64 10240
  }
  each_arm clean ceiling eviction_arm garnet
}

multikey() {
  banner multikey
  echo "### multikey: MGET of 1, 4, 16 spread keys at depth 64; 50 clients; populated; W=$WARMUP"
  echo "### ops is requests per second: the 16-key row moves sixteen times the keys of the 1-key row."
  multikey_arm() { local k; for k in 1 4 16; do runs "$PORT" "$1" mget 64 "$k"; done; }
  each_arm populate plain multikey_arm
}

case "$STAGE" in
  canary|calibrate|field|expiry|eviction|multikey) "$STAGE";;
  all)
    canary || { echo "### campaign stopped: the canary did not pass"; exit 1; }
    calibrate
    echo "### calibrate done. Derive W:  python3 bench/report.py --calibrate <this log>"
    echo "### then run, in this order:"
    echo "###   WARMUP=<W> bash bench/campaign.sh field     > 03-field.log"
    echo "###   WARMUP=<W> bash bench/campaign.sh expiry    > 04-expiry.log"
    echo "###   WARMUP=<W> bash bench/campaign.sh eviction  > 05-eviction.log"
    echo "###   WARMUP=<W> bash bench/campaign.sh multikey  > 06-multikey.log"
    ;;
  *) echo "unknown stage: $STAGE" >&2; exit 2;;
esac

echo "### stage $STAGE done $(date -u +%FT%TZ)  load:$(cut -d' ' -f1-3 /proc/loadavg)"
