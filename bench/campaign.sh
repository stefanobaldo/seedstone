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
#   keys       KEYS over a 7 000-key, 10 240-byte-value keyspace at depth 1.
#              The keyspace is loaded by keys-load.sh, not by redis-benchmark;
#              one prefix of 64 is matched per call, so every call answers the
#              same ~109 keys. Depth 1 because a page cache does not pipeline
#              KEYS; 20 000 calls per run because a call costs milliseconds.
#   durability SET of 64-byte values at depths 64 and 1 under each durability
#              setting: seedstone's --fsync never, interval and always against
#              Redis with appendonly and appendfsync no, everysec and always,
#              and each engine with no log at all; GET at depth 64 with the log
#              synced on every write, against no log. Its own arm list,
#              DURABILITY_ARMS; each arm writes into a directory of its own
#              under DATA_ROOT, emptied before it starts. Its runs can be
#              sized as a duration, DURABILITY_SECONDS, rather than a count:
#              at depth 64 the logged arms' throughput differs about fivefold,
#              so a million operations is under half a second on the fastest
#              and under two seconds on the slowest, too short a window to
#              average the background work during an arm (Redis 8.10.0's AOF
#              rewrites, this server's compactions). Each kept run is sized to
#              that many seconds at the last warmup run's throughput, so a kept
#              run faster than its warmup is shorter. The default, 10 s, is the
#              shortest window at which the logged arms' run-to-run spread
#              stopped falling on the reference machine; the stage then takes
#              about 20 minutes there.
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
# Usage: campaign.sh <canary|calibrate|field|expiry|eviction|multikey|keys|durability|all>
#        WARMUP=<W> is required by field, expiry, eviction, multikey, keys and
#        durability.
#        ARMS=<comma list> restricts the arms; the default is all seven.
#        DURABILITY_ARMS=<comma list> does the same for the durability stage.
#        DATA_ROOT=<dir> is where the durability stage's arms write (/tmp); every
#        arm writes on the same disk, so the comparison is between engines.
#        It must be on a disk: a DATA_ROOT on tmpfs is refused.
#        DURABILITY_SECONDS=<s> sizes each kept durability run to last s
#        seconds at the last warmup run's throughput, never fewer operations
#        than N; it needs WARMUP >= 1. Unset, it is 10; set
#        empty (DURABILITY_SECONDS=), every run is N, as before it existed.
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
SEEDSTONE_BIN=${SEEDSTONE_BIN:-$HOME/src/seedstone-v0.2.0/target/release/seedstone}
REDIS_SERVER=${REDIS_SERVER:-$HOME/redis-8.10.0/src/redis-server}
VALKEY_SERVER=${VALKEY_SERVER:-$HOME/valkey-9.1.1/src/valkey-server}
DRAGONFLY=${DRAGONFLY:-$HOME/dragonfly/dragonfly-aarch64}
# The Garnet release archive unpacks one directory per target framework, so the
# executable is never at the archive's root; net8.0 is the one this harness names.
GARNET=${GARNET:-$HOME/garnet/net8.0/GarnetServer}
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

DATA_ROOT=${DATA_ROOT:-/tmp}
DURABILITY_SECONDS=${DURABILITY_SECONDS-10}
DURABILITY_ARMS=${DURABILITY_ARMS:-seedstone,seedstone-never,seedstone-interval,seedstone-always,redis-iot1,redis-aof-no,redis-aof-everysec,redis-aof-always}

STAGE=${1:?usage: campaign.sh <canary|calibrate|field|expiry|eviction|multikey|keys|durability|all>}

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
  local os mem
  os=$(grep -m1 '^PRETTY_NAME=' /etc/os-release 2>/dev/null | cut -d= -f2- | tr -d '"')
  mem=$(awk '/^MemTotal/{printf "%.1f GiB", $2/1048576}' /proc/meminfo 2>/dev/null)
  echo "### stage $1 start $(date -u +%FT%TZ)"
  echo "### load at start:$(cut -d' ' -f1-3 /proc/loadavg | sed 's/^/ /')"
  echo "### kernel $(uname -r) $(uname -m)  cpus server=$SERVER_CPUS client=$CLIENT_CPUS"
  echo "### os ${os:-unknown}  memtotal ${mem:-unknown}"
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
# `loadkeys` loads the KEYS cell's keyspace through keys-load.sh and prints what
# landed.
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
  elif [[ $mode == loadkeys ]]; then
    bash "$HERE/keys-load.sh" "$port"
  fi
}

stop() { kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; sleep 2; echo; }

config_get() { "$CLI" -p "$PORT" config get "$1" | tail -1 | tr -d '\r'; }

# data_dir <arm>: the arm's own directory under DATA_ROOT, emptied, printed.
data_dir() {
  local dir=$DATA_ROOT/bench-data-$1
  if ! { rm -rf "$dir" && mkdir -p "$dir"; }; then
    echo "FATAL: cannot empty $dir" >&2; return 1
  fi
  echo "$dir"
}

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
    seedstone-never|seedstone-interval|seedstone-always)
      need "$SEEDSTONE_BIN" seedstone; PORT=6390
      local setting=${arm#seedstone-} dir
      dir=$(data_dir "$arm") || exit 1
      start "$PORT" "$arm" "$mode" . -- "$SEEDSTONE_BIN" --bind "127.0.0.1:$PORT" --max-clients 2000 --no-auth --data-dir "$dir" --fsync "$setting";;
    redis-iot1|redis-iot4)
      need "$REDIS_SERVER" redis; PORT=6391
      local io=${arm#redis-iot} extra=()
      [[ $bound == ceiling ]] && extra=(--maxmemory "$CEILING" --maxmemory-policy allkeys-lru)
      start "$PORT" "$arm" "$mode" . -- "$REDIS_SERVER" --port "$PORT" --save '' --appendonly no --io-threads "$io" "${extra[@]}";;
    redis-aof-no|redis-aof-everysec|redis-aof-always)
      need "$REDIS_SERVER" redis; PORT=6391
      local setting=${arm#redis-aof-} dir
      dir=$(data_dir "$arm") || exit 1
      # AOF rewrite is left at its defaults; what they are is printed, and
      # whether one ran is read from the log after the arm, not assumed.
      start "$PORT" "$arm" "$mode" . -- "$REDIS_SERVER" --port "$PORT" --save '' --appendonly yes --appendfsync "$setting" --dir "$dir" --io-threads 1 || return 1
      echo "    appendfsync=$(config_get appendfsync)  auto-aof-rewrite-percentage=$(config_get auto-aof-rewrite-percentage)  auto-aof-rewrite-min-size=$(config_get auto-aof-rewrite-min-size)";;
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

# durability_runs <port> <arm> <shape> <depth>: runs, for the durability stage.
# With DURABILITY_SECONDS empty it is runs, unchanged. With it set, the kept
# runs are sized to last that many seconds at the last warmup run's throughput,
# and never fewer operations than an unsized run makes; a warmup
# that read no throughput skips the row rather than run a kept run of nothing.
durability_runs() {
  local port=$1 arm=$2 shape=$3 depth=$4 i line='' ops n
  if [[ -z ${DURABILITY_SECONDS:-} ]]; then runs "$port" "$arm" "$shape" "$depth" -; return; fi
  for i in $(seq 1 "$WARMUP"); do
    line=$(bash "$CELL" "$port" "$PID" "$arm" warmup "$shape" "$depth" 50 -)
    echo "$line"
  done
  ops=$(sed -nE 's/.* ops=([0-9.]+) .*/\1/p' <<< "$line")
  n=$(awk -v o="${ops:-0}" -v s="$DURABILITY_SECONDS" -v floor="${N:-1000000}" 'BEGIN {
    if (o + 0 <= 0) exit
    x = o * s; x = (x == int(x)) ? x : int(x) + 1
    printf "%d", (x > floor) ? x : floor
  }')
  if [[ -z $n ]]; then
    echo "### ROW SKIPPED: warmup read 0 ops/s ($arm $shape depth $depth)"
    SKIPPED_ROWS=$(( ${SKIPPED_ROWS:-0} + 1 )); return 1
  fi
  for _ in 1 2 3; do N=$n bash "$CELL" "$port" "$PID" "$arm" kept "$shape" "$depth" 50 -; done
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

each_arm() {  # each_arm <populate|clean|loadkeys> <plain|ceiling> <fn> [skip-arms, comma-separated]
  local mode=$1 bound=$2 fn=$3 skip=",${4:-}," arm
  for arm in ${ARMS//,/ }; do
    [[ $skip == *",$arm,"* ]] && { echo "### $arm skipped in this stage, by design"; echo; continue; }
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
    # The probe says what it read. Asserting a deadline unconditionally would
    # print "a deadline reached the server" beside a -1, which is the sentence
    # this cell exists to check, printed over the answer that refutes it.
    local ttl; ttl=$("$CLI" -p "$PORT" ttl key:000000012345 2>/dev/null | tr -d '\r')
    if [[ $ttl =~ ^[0-9]+$ ]] && (( ttl > 0 )); then
      echo "    ttl probe: $ttl (a deadline reached the server)"
    else
      echo "    ttl probe: $ttl (NO deadline on key:000000012345 — -1 the key has none, -2 no key)"
    fi
  }
  each_arm populate plain expiry_arm
}

eviction() {
  banner eviction
  echo "### eviction: SET 10 240 B at depth 64 under a $CEILING ceiling, LRU where the engine has it;"
  echo "### the keyspace is filled past the ceiling first (discarded), then W discarded runs, then"
  echo "### three kept, so every kept run is in steady-state eviction. evicted_keys per operation"
  echo "### is on every row. Two engines do not take part, for reasons that are theirs and not"
  echo "### this cell's. Garnet: its memory bound is a log size with tail reclamation, not a"
  echo "### ceiling with LRU, so a comparable cell does not exist. Dragonfly (v1.40.2): it"
  echo "### requires 256 MiB of maxmemory per proactor thread and refuses to start below that,"
  echo "### so at the ten threads this hardware gives it the smallest ceiling it accepts is"
  echo "### 2.50 GiB - above the ~0.95 GiB this cell's keyspace can hold, which would mean no"
  echo "### eviction at all in any arm. The ceiling was declared before the run and stands."
  eviction_arm() {
    local value; value=$(head -c 10240 /dev/zero | tr '\0' x)
    taskset -c "$CLIENT_CPUS" "$BENCH" -p "$PORT" -n 60000 -c 50 -P 64 -r 100000 -q SET "key:__rand_int__" "$value" >/dev/null 2>&1
    echo "    filled past the ceiling: $("$CLI" -p "$PORT" info stats 2>/dev/null | tr -d '\r' | grep '^evicted_keys' || echo 'evicted_keys not reported')"
    runs "$PORT" "$1" set-large 64 10240
  }
  each_arm clean ceiling eviction_arm garnet,dragonfly
}

multikey() {
  banner multikey
  echo "### multikey: MGET of 1, 4, 16 spread keys at depth 64; 50 clients; populated; W=$WARMUP"
  echo "### ops is requests per second: the 16-key row moves sixteen times the keys of the 1-key row."
  multikey_arm() { local k; for k in 1 4 16; do runs "$PORT" "$1" mget 64 "$k"; done; }
  each_arm populate plain multikey_arm
}

keys() {
  banner keys
  echo "### keys: KEYS over 7 000 keys of 10 240 B, 64 prefixes, one fixed prefix matched per call"
  echo "### (~109 keys); depth 1; 50 clients; 20 000 calls per run; keyspace loaded by keys-load.sh,"
  echo "### the same keys every run; W=$WARMUP. ops is KEYS calls per second."
  # A subshell, so the three overrides reach cell.sh for this stage and no other.
  keys_arm() { ( export KEYSPACE=7000 PAYLOAD=10240 N=20000; runs "$PORT" "$1" keys 1 7 ); }
  each_arm loadkeys plain keys_arm
}

durability() {
  banner durability
  echo "### durability: SET 64 B at depths 64 and 1 under each durability setting — seedstone's"
  echo "### --fsync never/interval/always against Redis with appendonly and appendfsync"
  echo "### no/everysec/always, plus each engine with no log at all; GET 64 B at depth 64 with"
  echo "### the log synced on every write, against no log; 50 clients; 100 000 spread keys;"
  echo "### populated; one directory per arm under $DATA_ROOT, emptied before it starts; W=$WARMUP"
  # The arms' directories do not exist yet, and DATA_ROOT itself may not: the
  # filesystem is read where the path first exists. The type is taken from the
  # same df line that is printed, so what is checked is what a reader sees.
  local root=$DATA_ROOT line
  while [[ ! -e $root && $root != / ]]; do root=$(dirname "$root"); done
  if ! line=$(df -hTP "$root" 2>/dev/null | tail -1) || [[ -z $line ]]; then
    echo "### REFUSED: cannot read the filesystem under $DATA_ROOT"; return 1
  fi
  echo "### data root: $line"
  if [[ $(awk '{print $2}' <<< "$line") == tmpfs ]]; then
    echo "### REFUSED: $DATA_ROOT is on tmpfs, which syncs nothing; a durability cell there measures no sync at all"
    return 1
  fi
  if [[ -n ${DURABILITY_SECONDS:-} ]]; then
    if ! [[ $DURABILITY_SECONDS =~ ^[0-9]+$ ]] || (( DURABILITY_SECONDS < 1 )); then
      echo "### REFUSED: DURABILITY_SECONDS must be a whole number of seconds, >= 1"; return 1
    fi
    if ! [[ ${WARMUP:-0} =~ ^[0-9]+$ ]] || (( ${WARMUP:-0} < 1 )); then
      echo "### REFUSED: DURABILITY_SECONDS needs WARMUP >= 1"; return 1
    fi
    echo "### run length: each kept run sized to $DURABILITY_SECONDS s at the last warmup run's throughput, never under N=${N:-1000000}"
  else
    echo "### run length: N=${N:-1000000} per run"
  fi
  durability_arm() {
    durability_runs "$PORT" "$1" set 64
    durability_runs "$PORT" "$1" set 1
    case $1 in seedstone|seedstone-always) durability_runs "$PORT" "$1" get 64;; esac
    local dir=$DATA_ROOT/bench-data-$1
    [[ -d $dir ]] && echo "    data: $(du -sh "$dir" | cut -f1) under $dir"
    case $1 in redis-aof-*)
      echo "    aof rewrites during the arm: $(grep -c 'Background append only file rewriting' "/tmp/bench-$PORT.log")";;
    esac
  }
  ( ARMS=$DURABILITY_ARMS; each_arm populate plain durability_arm
    (( ${SKIPPED_ROWS:-0} == 0 )) || { echo "### $SKIPPED_ROWS row(s) skipped"; exit 1; } )
}

# A stage that fails must fail the script. The canary and the calibration are
# gates: a caller that reads only the exit status has to be told, and the
# trailing "stage done" echo below would otherwise make every stage exit 0.
STATUS=0
case "$STAGE" in
  canary|calibrate|field|expiry|eviction|multikey|keys|durability) "$STAGE" || STATUS=$?;;
  all)
    canary || { echo "### campaign stopped: the canary did not pass"; exit 1; }
    calibrate || STATUS=$?
    echo "### calibrate done. Derive W:  python3 bench/report.py --calibrate <this log>"
    echo "### then run, in this order:"
    echo "###   WARMUP=<W> bash bench/campaign.sh field     > 03-field.log"
    echo "###   WARMUP=<W> bash bench/campaign.sh expiry    > 04-expiry.log"
    echo "###   WARMUP=<W> bash bench/campaign.sh eviction  > 05-eviction.log"
    echo "###   WARMUP=<W> bash bench/campaign.sh multikey  > 06-multikey.log"
    echo "###   WARMUP=<W> bash bench/campaign.sh keys      > 07-keys.log"
    echo "###   WARMUP=<W> bash bench/campaign.sh durability > 08-durability.log"
    ;;
  *) echo "unknown stage: $STAGE" >&2; exit 2;;
esac

echo "### stage $STAGE done $(date -u +%FT%TZ)  load:$(cut -d' ' -f1-3 /proc/loadavg)"
exit "$STATUS"
