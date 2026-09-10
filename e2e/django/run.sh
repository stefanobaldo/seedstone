#!/usr/bin/env bash
# Drives seedstone with the django-redis suite inside a container pinned by
# digest to the interpreter one client pair needs. Which pair is the second
# argument: a directory beside this script holding the pins (`pair.env`),
# the requirements, the expectations file and the settings.
#
# The suite is not vendored: it is fetched from its published sdist, verified
# against a pinned digest, and extracted for the run. That keeps a third
# party's tests out of this repository while keeping the run reproducible.
set -euo pipefail

server_binary="${1:?usage: run.sh <path-to-seedstone-binary> <pair>}"
pair="${2:?usage: run.sh <path-to-seedstone-binary> <pair>   (pair: a directory beside this script, e.g. pinned or current)}"
port="${SEEDSTONE_PORT:-6390}"
here="$(cd "$(dirname "$0")" && pwd)"
pair_dir="${here}/${pair}"
[ -f "${pair_dir}/pair.env" ] || { echo "run.sh: no pair.env in ${pair_dir}" >&2; exit 2; }
# shellcheck source=/dev/null
. "${pair_dir}/pair.env"
export DJANGO_REDIS_VERSION DJANGO_REDIS_SHA256 TEST_FILES

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

# This lane always authenticates. In CI the password file is handed to it; a
# developer running the lane bare gets one written here, so the path under test
# is the same either way. The literal protects nothing — it exists so the
# authenticated path is the one this suite drives.
password_file="${SEEDSTONE_PASSWORD_FILE:-}"
if [ -z "$password_file" ]; then
  password_file="${here}/.password"
  printf 'lane-password\n' > "$password_file"
fi
password="$(cat "$password_file")"

"$server_binary" --bind "${bind_address}:${port}" --requirepass-file "$password_file" &
server=$!
trap 'kill "$server" 2>/dev/null || true; rm -f "${here}/.password"' EXIT

for _ in $(seq 100); do
  if (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then break; fi
  sleep 0.1
done

docker run --rm "${network[@]}" \
  -v "${pair_dir}:/lane" -w /lane \
  -e SEEDSTONE_HOST="$server_host" \
  -e SEEDSTONE_PORT="$port" \
  -e SEEDSTONE_PASSWORD="$password" \
  -e DJANGO_REDIS_VERSION -e DJANGO_REDIS_SHA256 -e TEST_FILES \
  "$IMAGE" sh -euc '
    pip install --quiet --no-cache-dir --disable-pip-version-check --root-user-action=ignore -r requirements.txt
    pip download --quiet --no-deps --no-binary :all: --disable-pip-version-check --dest /tmp/sdist "django-redis==${DJANGO_REDIS_VERSION}"
    archive="$(ls /tmp/sdist/django*redis-${DJANGO_REDIS_VERSION}.tar.gz)"
    echo "${DJANGO_REDIS_SHA256}  ${archive}" | sha256sum -c -
    tar -xzf "${archive}" -C /tmp
    src="$(ls -d /tmp/django*redis-${DJANGO_REDIS_VERSION})"
    cp -r "${src}/tests" /tmp/tests
    # The lane supplies the harness: its conftest, its expectations, and its
    # settings (a module or a package) replace the suite'"'"'s own.
    rm -rf /tmp/tests/conftest.py /tmp/tests/settings /tmp/tests/settings.py
    cp conftest.py expectations.txt /tmp/tests/
    if [ -d settings ]; then cp -r settings /tmp/tests/settings; else cp settings.py /tmp/tests/; fi
    cd /tmp/tests && python -m pytest ${TEST_FILES} -q --timeout 30
  '
