#!/usr/bin/env bash
#
# Rotates a seedstone server's password without restarting it, and reads what
# the server said about it the way a log collector would.
#
# The three steps an operator runs: the new password joins the file and the
# server re-reads it on SIGHUP; clients move to the new password at their own
# pace — here, a redis-cli that authenticates with each in turn; the old
# password leaves the file and the server re-reads it again. At no point is a
# client holding either password refused, and the server never restarts.
#
# Every line the server writes is one JSON object. This script parses them
# with jq rather than grepping text, because a collector will parse them and
# the field names are the promise `docs/operations.md` makes.
#
# Usage: rotation.sh path/to/seedstone [port]
set -euo pipefail

BIN=${1:?usage: rotation.sh path/to/seedstone [port]}
PORT=${2:-6391}

command -v jq >/dev/null || { echo "jq is required to read the server's lines" >&2; exit 1; }
command -v redis-cli >/dev/null || { echo "redis-cli is required" >&2; exit 1; }

work=$(mktemp -d)
trap 'kill "$SERVER" 2>/dev/null || true; rm -rf "$work"' EXIT
pwfile="$work/password"
log="$work/stderr"

printf 'old-password\n' > "$pwfile"
"$BIN" --bind "127.0.0.1:$PORT" --requirepass-file "$pwfile" 2>"$log" &
SERVER=$!

# The server binds before it prints, but the shell got here first.
for _ in $(seq 50); do
    redis-cli -p "$PORT" -a old-password --no-auth-warning ping >/dev/null 2>&1 && break
    sleep 0.1
done

r() { redis-cli -p "$PORT" -a "$1" --no-auth-warning "${@:2}"; }

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

# Wait until the server has written `n` lines with the given evt.
await_lines() {
    local evt=$1 n=$2
    for _ in $(seq 50); do
        if [ "$(jq -r "select(.evt == \"$evt\") | .evt" "$log" 2>/dev/null | wc -l | tr -d ' ')" -ge "$n" ]; then
            return 0
        fi
        sleep 0.1
    done
    echo "the server did not write $n '$evt' line(s):" >&2
    cat "$log" >&2
    exit 1
}

# What a refused password looks like from a client: `redis-cli -a` reports the
# refused AUTH on *stderr* ("AUTH failed: WRONGPASS …") and then sends the
# command anyway, on a connection that never authenticated — so stdout, which
# is what these assertions read, carries `NOAUTH`. The `WRONGPASS` the server
# answered is not lost: it is one of the `error_reply` lines read at the end.
# (Measured against Homebrew's redis-cli 8.10.0, 2026-09-13.)
expect "old password authenticates" PONG r old-password ping
expect "new password is refused before the rotation" \
    "NOAUTH Authentication required." r new-password ping

# Step 1: the new password joins the file; the server re-reads it.
printf 'old-password\nnew-password\n' > "$pwfile"
kill -HUP "$SERVER"
await_lines password_reloaded 1
expect "old password still authenticates" PONG r old-password ping
expect "new password authenticates" PONG r new-password ping

# Step 2 is the clients moving over, which the line above just did.

# Step 3: the old password leaves; the server re-reads again.
printf 'new-password\n' > "$pwfile"
kill -HUP "$SERVER"
await_lines password_reloaded 2
expect "old password is refused after the rotation" \
    "NOAUTH Authentication required." r old-password ping
expect "new password authenticates" PONG r new-password ping
expect "the keyspace survived the rotation" OK r new-password set k v
expect "the keyspace survived the rotation" v r new-password get k

# A file the boot would refuse is refused by the reload, and the set stands.
printf 'a\nb\nc\n' > "$pwfile"
kill -HUP "$SERVER"
await_lines password_reload_failed 1
expect "the previous passwords stand after a failed reload" PONG r new-password ping

kill -TERM "$SERVER"
wait "$SERVER" || true
await_lines stopping 1

# What the server said, read as a collector reads it.
expect "every line is JSON with the three common fields" "" \
    jq -r 'select((.ts | type) != "number" or (.level | type) != "string" or (.evt | type) != "string") | .' "$log"
expect "listening once, at info" "info" jq -r 'select(.evt == "listening") | .level' "$log"
expect "listening names this port" "$PORT" jq -r 'select(.evt == "listening") | .port' "$log"
expect "two reloads, 2 then 1 passwords" "2 1" \
    bash -c "jq -r 'select(.evt == \"password_reloaded\") | .passwords' '$log' | paste -sd' ' -"
expect "the reloads are info" "info info" \
    bash -c "jq -r 'select(.evt == \"password_reloaded\") | .level' '$log' | paste -sd' ' -"
expect "the failed reload is error and names the rule" \
    "error one or two passwords, one per line; found 3 lines" \
    jq -r 'select(.evt == "password_reload_failed") | "\(.level) \(.error)"' "$log"
expect "stopping on SIGTERM, at info" "info SIGTERM" \
    jq -r 'select(.evt == "stopping") | "\(.level) \(.signal)"' "$log"
expect "the refused AUTHs were logged at warn" "warn warn" \
    bash -c "jq -r 'select(.evt == \"error_reply\" and .code == \"WRONGPASS\") | .level' '$log' | paste -sd' ' -"

echo e2e-rotation-ok
