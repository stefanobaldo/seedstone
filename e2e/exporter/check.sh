#!/usr/bin/env bash
# The three assertions the exporter lane makes. Split from run.sh so they can
# be run against saved output, and so the lane's claim is readable in one
# screen.
set -euo pipefail
metrics="${1:?metrics file}"; log="${2:?exporter log}"; expected="${3:?expected-errors file}"

fail() { echo "exporter lane: $*" >&2; exit 1; }

expected_lines=$(grep -v '^#' "$expected" | sed '/^$/d' || true)

# 1. Up.
grep -Eq '^redis_up 1$' "$metrics" || fail "redis_up is not 1"

# 2. Named metrics, with the values the lane can predict.
#
# The value assertions are the gate's second half, and they only mean
# something once the first half — no refused command — holds: a metric the
# exporter could not build is a metric it does not print, so before then this
# block would only restate what the expectations file already says. It
# activates by itself the day that file empties; nothing has to remove a
# guard.
want() {
  local name=$1 value=$2
  grep -Eq "^${name}(\{[^}]*\})? (${value})$" "$metrics" || fail "$name $value not in the scrape"
}
if [ -z "$expected_lines" ]; then
  want redis_memory_max_bytes '6\.7108864e\+07|67108864'
  want 'redis_db_keys\{db="db0"\}' 3
  want 'redis_db_keys_expiring\{db="db0"\}' 1
  want redis_evicted_keys_total 0
  grep -Eq '^redis_memory_used_bytes [1-9]' "$metrics" || fail "redis_memory_used_bytes is missing or zero"
  grep -Eq '^redis_keyspace_hits_total ' "$metrics" || fail "redis_keyspace_hits_total is missing"
  grep -Eq '^redis_keyspace_misses_total ' "$metrics" || fail "redis_keyspace_misses_total is missing"
  grep -Eq '^redis_commands_total\{cmd="set"\} 3$' "$metrics" || fail "commandstats did not count three SETs"
fi

# 3. The error log, against what is expected today.
errors=$(grep -E 'level=(error|warn)' "$log" || true)
while IFS= read -r line; do
  [ -z "$line" ] && continue
  tolerated=false
  while IFS= read -r pattern; do
    [ -z "$pattern" ] && continue
    if [[ "$line" == *"$pattern"* ]]; then tolerated=true; break; fi
  done <<<"$expected_lines"
  $tolerated || fail "unexpected exporter error: $line"
done <<<"$errors"
while IFS= read -r pattern; do
  [ -z "$pattern" ] && continue
  grep -qF -- "$pattern" "$log" || fail "expected error no longer appears; remove its line: $pattern"
done <<<"$expected_lines"
echo "exporter lane: scrape ok, $(grep -c . "$metrics") metric lines, errors tolerated: $(printf '%s' "$expected_lines" | grep -c . || true)"
