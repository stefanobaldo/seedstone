#!/usr/bin/env bash
# Drives seedstone with the django-redis suite inside a pinned container.
#
# The suite is not vendored: it is fetched from its published sdist, verified
# against a pinned digest, and extracted for the run. That keeps a third
# party's 1,200 lines of tests out of this repository while keeping the run
# reproducible.
set -euo pipefail

server_binary="${1:?usage: run.sh <path-to-seedstone-binary>}"
port="${SEEDSTONE_PORT:-6390}"
here="$(cd "$(dirname "$0")" && pwd)"
image="python:3.7-slim"

# The digest the release publishes, not the digest some download happened to
# produce. It is checked inside the container: an archive that does not match
# is a different suite, and a gate running a suite other than the one it names
# is not a gate.
export DJANGO_REDIS_SHA256=306589c7021e6468b2656edc89f62b8ba67e8d5a1c8877e2688042263daa7a63

# How the container reaches the server. On Linux — which is where this runs in
# CI — the container shares this machine's network namespace, so the server
# never leaves the loopback. Docker Desktop runs its daemon inside a virtual
# machine, where `--network host` is that machine's loopback rather than this
# one's; there the container crosses the bridge instead, and the server has to
# listen somewhere the bridge can see it. Only the plumbing differs: the
# container, its pins and the suite it runs are identical either way.
if [ "$(uname -s)" = "Linux" ]; then
  bind_address="127.0.0.1"
  server_host="127.0.0.1"
  network=(--network host)
else
  bind_address="0.0.0.0"
  server_host="host.docker.internal"
  network=(--add-host "host.docker.internal:host-gateway")
fi

"$server_binary" --bind "${bind_address}:${port}" &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT

for _ in $(seq 100); do
  if (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then break; fi
  sleep 0.1
done

docker run --rm "${network[@]}" \
  -v "${here}:/lane" -w /lane \
  -e SEEDSTONE_HOST="$server_host" \
  -e SEEDSTONE_PORT="$port" \
  -e DJANGO_REDIS_SHA256 \
  "$image" sh -euc '
    pip install --quiet --no-cache-dir --disable-pip-version-check --root-user-action=ignore -r requirements.txt
    pip download --quiet --no-deps --no-binary :all: --disable-pip-version-check --dest /tmp/sdist django-redis==4.12.1
    echo "${DJANGO_REDIS_SHA256}  /tmp/sdist/django-redis-4.12.1.tar.gz" | sha256sum -c -
    tar -xzf /tmp/sdist/django-redis-4.12.1.tar.gz -C /tmp
    cp -r /tmp/django-redis-4.12.1/tests /tmp/tests
    cp conftest.py settings.py expectations.txt /tmp/tests/
    cd /tmp/tests && python -m pytest test_backend.py -q --timeout 30
  '
