#!/usr/bin/env bash
# Drives seedstone with a stock Prometheus exporter and checks the scrape.
#
# The exporter is pinned by the digest of its multi-architecture manifest
# list, so the same bytes are selected on the developer's machine and on the
# runner whatever their architectures. The tag is a comment for humans.
set -euo pipefail

server_binary="${1:?usage: run.sh <path-to-seedstone-binary>}"
port="${SEEDSTONE_PORT:-6391}"
metrics_port="${EXPORTER_PORT:-9121}"
here="$(cd "$(dirname "$0")" && pwd)"
# oliver006/redis_exporter v1.89.0
image="oliver006/redis_exporter@sha256:fed773ffd2bb2eac706e0ed9cc2fff41651193ee4977e2a4f6919658ad2313f4"
password_file="${SEEDSTONE_PASSWORD_FILE:-}"

# How the container reaches the server, and where the server has to listen for
# it to be reachable — the same split the compatibility lane makes, for the
# same reason: on Linux the container shares this machine's network namespace,
# and under Docker Desktop it does not.
if [ "$(uname -s)" = "Linux" ]; then
  bind_address="127.0.0.1"; server_host="127.0.0.1"
  network=(--network host)
else
  bind_address="0.0.0.0"; server_host="host.docker.internal"
  network=(--add-host "host.docker.internal:host-gateway" -p "${metrics_port}:${metrics_port}")
fi

# The flags this lane wants do not all exist yet. Ask the binary rather than
# assume: the lane's subject is the scrape and the exporter's log, and it has
# to run before the flags arrive as well as after. Read the usage text once,
# into a variable, so that a binary which exits non-zero on an unknown
# argument does not decide the question through the pipeline's status.
server_help="$("$server_binary" --help 2>&1 || true)"
supports() { printf '%s' "$server_help" | grep -q -- "$1"; }

server_args=(--bind "${bind_address}:${port}")
exporter_args=(--redis.addr="redis://${server_host}:${port}" --web.listen-address=":${metrics_port}")
if supports --maxmemory; then
  server_args+=(--maxmemory 64mb)
fi
if supports --requirepass-file && [ -n "$password_file" ]; then
  server_args+=(--requirepass-file "$password_file")
  exporter_args+=(--redis.password-file=/lane/password)
elif supports --no-auth; then
  server_args+=(--no-auth)
fi

"$server_binary" "${server_args[@]}" &
server=$!
trap 'kill "$server" 2>/dev/null || true; docker rm -f seedstone-exporter >/dev/null 2>&1 || true' EXIT
for _ in $(seq 100); do
  if (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then break; fi
  sleep 0.1
done

# A keyspace the checks can predict: three keys, one with a deadline.
# `${auth[@]+...}` rather than a bare `${auth[@]}`: under `set -u` bash 3.2,
# which is what a macOS developer runs, expanding an empty array is an
# unbound variable.
auth=()
if [ -n "$password_file" ] && supports --requirepass-file; then
  auth=(-a "$(cat "$password_file")" --no-auth-warning)
fi
seedstone_cli() { redis-cli -p "$port" ${auth[@]+"${auth[@]}"} "$@"; }
seedstone_cli set lane-a 1 >/dev/null
seedstone_cli set lane-b 2 >/dev/null
seedstone_cli set lane-c 3 ex 600 >/dev/null

docker rm -f seedstone-exporter >/dev/null 2>&1 || true
docker run -d --name seedstone-exporter "${network[@]}" -v "${here}:/lane:ro" \
  "$image" "${exporter_args[@]}" >/dev/null
for _ in $(seq 50); do
  curl -fs "http://127.0.0.1:${metrics_port}/metrics" >"${here}/.metrics.txt" 2>/dev/null && break
  sleep 0.2
done
sleep 1
docker logs seedstone-exporter >"${here}/.exporter.log" 2>&1

bash "${here}/check.sh" "${here}/.metrics.txt" "${here}/.exporter.log" "${here}/expected-errors.txt"
rm -f "${here}/.metrics.txt" "${here}/.exporter.log"
echo e2e-exporter-ok
