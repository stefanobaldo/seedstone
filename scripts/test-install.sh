#!/usr/bin/env bash
# The installer's tests, against a fake release served from a temporary
# directory: `site/install.sh` is run for real — the same download, verify
# and copy a user gets — with `SEEDSTONE_BASE_URL` pointed at a local server
# that answers `/releases/latest` with the redirect GitHub answers, and
# serves four archives whose binary is a shell script saying `seedstone
# 9.9.9`. A fifth archive, under `v9.9.8`, carries a checksum file that does
# not match it. Each case prints one `ok:`, `FAIL:` or `skip:` line.
#
# Usage: test-install.sh            (from the repository root)
#
# Nothing here reaches the network, writes outside its temporary directory
# or needs a privilege. The cases that shim `uname`, `getconf`, `ldd` or
# `sysctl` prepend a directory of one-line scripts to PATH; the case that
# needs `sudo` absent builds a PATH of the tools the installer uses and
# nothing else. (The shim bodies are written in single quotes on purpose:
# their variables expand when the shim runs.)
# shellcheck disable=SC2016
set -euo pipefail

INSTALLER=site/install.sh
TMP="$(mktemp -d)"
SERVER_PID=""
trap 'if [ -n "$SERVER_PID" ]; then kill "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; fi; rm -rf "$TMP"' EXIT

status=0
ok()   { printf 'ok: %s%s\n' "$1" "${2:+: $2}"; }
fail() { status=1; printf 'FAIL: %s: %s\n' "$1" "$2"; }
skip() { printf 'skip: %s: %s\n' "$1" "$2"; }

if [ ! -f "$INSTALLER" ]; then
  for case in installs pins missing-tag corrupted unwritable freebsd musl rosetta glibc \
      truncated piped sprite wget no-fetcher replaces path-hint; do
    fail "$case" "$INSTALLER is missing"
  done
  exit 1
fi

# --- the fake release -------------------------------------------------------

RELEASE="$TMP/release"
TARGETS="x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu x86_64-apple-darwin aarch64-apple-darwin"
if command -v sha256sum > /dev/null; then HASH="sha256sum"; else HASH="shasum -a 256"; fi

make_archive() { # <version> <target> <dir>  -> <dir>/seedstone-<version>-<target>.tar.gz and .sha256
  local v="$1" t="$2" dir="$3" name stage
  name="seedstone-$v-$t"
  stage="$(mktemp -d)"
  mkdir -p "$stage/$name" "$dir"
  printf '#!/bin/sh\necho "seedstone %s"\n' "$v" > "$stage/$name/seedstone"
  chmod 755 "$stage/$name/seedstone"
  tar -C "$stage" -czf "$dir/$name.tar.gz" "$name"
  ( cd "$dir" && $HASH "$name.tar.gz" > "$name.tar.gz.sha256" )
  rm -rf "$stage"
}

for t in $TARGETS; do
  make_archive 9.9.9 "$t" "$RELEASE/releases/download/v9.9.9"
  make_archive 9.9.8 "$t" "$RELEASE/releases/download/v9.9.8"
  # The corrupted release: the archive changes after its checksum was taken.
  printf 'trailing bytes\n' >> "$RELEASE/releases/download/v9.9.8/seedstone-9.9.8-$t.tar.gz"
done

# --- the server ---------------------------------------------------------------

# `releases/latest` answers with the 302 GitHub answers, to the tag page;
# everything under `releases/download/` is served from the directory;
# anything else is a 404, which is what a tag that does not exist gets.
python3 -I - "$RELEASE" "$TMP/port" > "$TMP/server.log" 2>&1 <<'PY' &
import http.server, os, sys
root, portfile = sys.argv[1], sys.argv[2]
class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=root, **kw)
    def do_HEAD(self):
        self.do_GET(head=True)
    def do_GET(self, head=False):
        if self.path == "/releases/latest":
            self.send_response(302)
            self.send_header("Location", f"http://{self.headers['Host']}/releases/tag/v9.9.9")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path.startswith("/releases/tag/"):
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if not self.path.startswith("/releases/download/"):
            self.send_error(404)
            return
        if head:
            super().do_HEAD()
        else:
            super().do_GET()
    def log_message(self, *a):
        pass
srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
with open(portfile, "w") as f:
    f.write(str(srv.server_address[1]))
srv.serve_forever()
PY
SERVER_PID=$!
disown "$SERVER_PID" 2>/dev/null || true
for _ in $(seq 1 50); do
  [ -s "$TMP/port" ] && break
  sleep 0.1
done
PORT="$(cat "$TMP/port")"
BASE="http://127.0.0.1:$PORT"

# --- helpers -------------------------------------------------------------------

# run <name> [VAR=value ...] -- <command ...>: runs the command with the
# variables set, into $OUT, $ERR and $RC, with a fresh install directory in
# $DIR and the fake release as the base URL.
run() {
  local name="$1"; shift
  local -a envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  DIR="$TMP/install-$name"
  mkdir -p "$DIR"
  OUT="$TMP/$name.out"; ERR="$TMP/$name.err"
  set +e
  env -u NO_COLOR SEEDSTONE_BASE_URL="$BASE" SEEDSTONE_INSTALL_DIR="$DIR" TERM=xterm COLORTERM= \
      ${envs[@]+"${envs[@]}"} "$@" > "$OUT" 2> "$ERR"
  RC=$?
  set -e
}
# A shim directory: shim <dir> <tool> <script body> writes an executable
# <tool> whose arguments are $@; the real tool is reachable as REAL_<TOOL>.
shim() {
  local dir="$1" tool="$2" body="$3" real
  mkdir -p "$dir"
  real="$(command -v "$tool" || true)"
  printf '#!/bin/sh\nREAL_%s=%s\n%s\n' "$(echo "$tool" | tr '[:lower:]' '[:upper:]')" "${real:-/bin/false}" "$body" > "$dir/$tool"
  chmod 755 "$dir/$tool"
}
# A PATH with only the tools the installer may call, no sudo among them.
tools_path() {
  local dir="$TMP/tools"
  mkdir -p "$dir"
  for t in sh env curl wget tar sha256sum shasum uname mktemp rm cp chmod mv mkdir cat sed tr head wc cut \
           getconf ldd sysctl id dirname basename grep sleep tput gzip; do
    if command -v "$t" > /dev/null 2>&1 && [ ! -e "$dir/$t" ]; then
      ln -s "$(command -v "$t")" "$dir/$t"
    fi
  done
  printf '%s' "$dir"
}
installed() { [ -x "$DIR/seedstone" ] && [ "$("$DIR/seedstone")" = "seedstone $1" ]; }
empty() { [ -z "$(ls -A "$DIR")" ]; }
has() { grep -q -- "$2" "$1"; }

# --- the cases -----------------------------------------------------------------

run installs -- sh "$INSTALLER"
if [ "$RC" -eq 0 ] && installed 9.9.9 && has "$OUT" "seedstone 9.9.9"; then
  ok installs "exit 0, $DIR/seedstone answers seedstone 9.9.9"
else
  fail installs "exit $RC; $(tail -1 "$ERR")"
fi

run pins SEEDSTONE_VERSION=v9.9.9 -- sh "$INSTALLER"
if [ "$RC" -eq 0 ] && installed 9.9.9 && has "$OUT" "v9.9.9" && ! has "$OUT" "latest"; then
  ok pins "SEEDSTONE_VERSION=v9.9.9 installs that tag and does not call it latest"
else
  fail pins "exit $RC; $(tail -1 "$ERR")"
fi

run missing-tag SEEDSTONE_VERSION=v0.0.1 -- sh "$INSTALLER"
if [ "$RC" -eq 2 ] && has "$ERR" "404" && has "$ERR" "v0.0.1" && empty; then
  ok missing-tag "exit 2 naming the 404, nothing installed"
else
  fail missing-tag "exit $RC; $(tail -1 "$ERR")"
fi

run corrupted SEEDSTONE_VERSION=v9.9.8 -- sh "$INSTALLER"
# The hashes the installer must print are the ones for the archive it chose;
# every target's pair differs, so the check reads the one for this machine.
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) t=x86_64-unknown-linux-gnu ;;
  Linux-aarch64) t=aarch64-unknown-linux-gnu ;;
  Darwin-x86_64) t=x86_64-apple-darwin ;;
  Darwin-arm64) t=aarch64-apple-darwin ;;
  *) t=x86_64-unknown-linux-gnu ;;
esac
expected="$(cut -d' ' -f1 "$RELEASE/releases/download/v9.9.8/seedstone-9.9.8-$t.tar.gz.sha256")"
actual="$($HASH "$RELEASE/releases/download/v9.9.8/seedstone-9.9.8-$t.tar.gz" | cut -d' ' -f1)"
if [ "$RC" -eq 3 ] && has "$ERR" "$expected" && has "$ERR" "$actual" && empty && [ -z "$(ls -d "${TMPDIR:-/tmp}"/seedstone-install.* 2>/dev/null)" ]; then
  ok corrupted "exit 3, both hashes printed, no file left"
else
  fail corrupted "exit $RC; $(tail -1 "$ERR")"
fi

if [ "$(id -u)" -eq 0 ]; then
  skip unwritable "running as root, every directory is writable"
else
  mkdir -p "$TMP/install-unwritable"
  chmod 555 "$TMP/install-unwritable"
  run unwritable PATH="$(tools_path)" -- sh "$INSTALLER"
  chmod 755 "$DIR"
  if [ "$RC" -eq 1 ] && has "$ERR" "$DIR" && has "$ERR" "SEEDSTONE_INSTALL_DIR" && ! has "$ERR" "sudo:" && empty; then
    ok unwritable "exit 1 naming the directory and SEEDSTONE_INSTALL_DIR, nothing copied"
  else
    fail unwritable "exit $RC; $(tail -1 "$ERR")"
  fi
fi

shim "$TMP/shim-freebsd" uname 'case "$1" in -s) echo FreeBSD ;; -m) echo amd64 ;; *) exec "$REAL_UNAME" "$@" ;; esac'
run freebsd PATH="$TMP/shim-freebsd:$PATH" -- sh "$INSTALLER"
if [ "$RC" -eq 1 ] && has "$ERR" "FreeBSD" && has "$ERR" "install.md" && empty; then
  ok freebsd "exit 1 naming FreeBSD and the install page"
else
  fail freebsd "exit $RC; $(tail -1 "$ERR")"
fi

shim "$TMP/shim-musl" uname 'case "$1" in -s) echo Linux ;; -m) echo x86_64 ;; *) exec "$REAL_UNAME" "$@" ;; esac'
shim "$TMP/shim-musl" ldd 'echo "musl libc (x86_64)"; echo "Version 1.2.4"'
shim "$TMP/shim-musl" getconf 'exit 1'
run musl PATH="$TMP/shim-musl:$PATH" -- sh "$INSTALLER"
if [ "$RC" -eq 1 ] && has "$ERR" "musl" && has "$ERR" "install.md" && empty; then
  ok musl "exit 1 naming musl and the install page"
else
  fail musl "exit $RC; $(tail -1 "$ERR")"
fi

shim "$TMP/shim-rosetta" uname 'case "$1" in -s) echo Darwin ;; -m) echo x86_64 ;; *) exec "$REAL_UNAME" "$@" ;; esac'
shim "$TMP/shim-rosetta" sysctl 'echo 1'
run rosetta PATH="$TMP/shim-rosetta:$PATH" -- sh "$INSTALLER"
if [ "$RC" -eq 0 ] && has "$OUT" "aarch64-apple-darwin" && installed 9.9.9; then
  ok rosetta "x86_64 under Rosetta installs the arm64 archive"
else
  fail rosetta "exit $RC; $(grep -c aarch64-apple-darwin "$OUT") mentions; $(tail -1 "$ERR")"
fi

if [ "$(uname -s)" != Linux ]; then
  skip glibc "the glibc floor is read on a Linux host only"
else
  shim "$TMP/shim-glibc" getconf 'case "$1" in GNU_LIBC_VERSION) echo "glibc 2.31" ;; *) exec "$REAL_GETCONF" "$@" ;; esac'
  run glibc PATH="$TMP/shim-glibc:$PATH" -- sh "$INSTALLER"
  if [ "$RC" -eq 1 ] && has "$ERR" "2.34" && has "$ERR" "2.31" && empty; then
    ok glibc "exit 1 naming 2.34 and 2.31"
  else
    fail glibc "exit $RC; $(tail -1 "$ERR")"
  fi
fi

# A download cut short: at 2000 bytes, halfway, and just before the last
# line. None may touch the install directory. The exit code depends on
# where the cut lands — between two statements the shell runs what it has
# and exits 0, inside one it cannot parse and exits 2 — so the codes are
# reported, not asserted.
size="$(wc -c < "$INSTALLER")"
truncated_ok=true; truncated_codes=""
for cut in 2000 $((size / 2)) $((size - 10)); do
  run truncated -- sh -c "head -c $cut '$INSTALLER' | sh"
  truncated_codes="$truncated_codes $RC"
  empty || truncated_ok=false
done
if $truncated_ok; then
  ok truncated "nothing created at three cuts (exit$truncated_codes)"
else
  fail truncated "a cut installed something (exit$truncated_codes)"
fi

run piped COLORTERM=truecolor -- sh -c "sh '$INSTALLER' | cat"
if [ "$RC" -eq 0 ] && ! grep -q "$(printf '\033')" "$OUT" && ! grep -q "$(printf '\033')" "$ERR"; then
  ok piped "no escape byte when stdout is a pipe"
else
  fail piped "exit $RC; $(grep -c "$(printf '\033')" "$OUT") lines with escapes"
fi

# On a pseudo-terminal advertising 24-bit colour the sprite prints: twelve
# lines carrying `38;2;` colour sequences, the half-block glyphs among them.
run sprite COLORTERM=truecolor -- python3 -I -c '
import os, pty, sys
status = pty.spawn(["sh", sys.argv[1]])
sys.exit(os.waitstatus_to_exitcode(status))' "$INSTALLER"
sprite_lines="$(grep -c "$(printf '\033')\[38;2;" "$OUT" || true)"
if [ "$RC" -eq 0 ] && [ "$sprite_lines" -eq 12 ] && grep -q "▀\|▄" "$OUT"; then
  ok sprite "twelve 24-bit lines on a pty with COLORTERM=truecolor"
else
  fail sprite "exit $RC; $sprite_lines lines with 24-bit colour"
fi

if ! command -v wget > /dev/null; then
  skip wget "no wget on this machine"
else
  run wget PATH="$(tools_path)" -- sh -c "rm -f '$TMP/tools/curl'; exec sh '$INSTALLER'"
  if [ "$RC" -eq 0 ] && installed 9.9.9; then
    ok wget "installs with wget when curl is absent"
  else
    fail wget "exit $RC; $(tail -1 "$ERR")"
  fi
  rm -rf "$TMP/tools"
fi

# wget translates its error line under a non-English locale; the tag that
# does not exist must still be reported as a 404, not as "no answer".
if ! command -v wget > /dev/null; then
  skip wget-missing-tag "no wget on this machine"
else
  run wget-missing-tag PATH="$(tools_path)" LANG=pt_BR.UTF-8 LC_ALL=pt_BR.UTF-8 SEEDSTONE_VERSION=v0.0.1 -- \
    sh -c "rm -f '$TMP/tools/curl'; exec sh '$INSTALLER'"
  if [ "$RC" -eq 2 ] && has "$ERR" "404" && empty; then
    ok wget-missing-tag "exit 2 naming the 404 under pt_BR with wget"
  else
    fail wget-missing-tag "exit $RC; $(tail -1 "$ERR")"
  fi
  rm -rf "$TMP/tools"
fi

run no-fetcher PATH="$(tools_path)" -- sh -c "rm -f '$TMP/tools/curl' '$TMP/tools/wget'; exec sh '$INSTALLER'"
if [ "$RC" -eq 1 ] && has "$ERR" "curl" && has "$ERR" "wget" && empty; then
  ok no-fetcher "exit 1 naming curl and wget"
else
  fail no-fetcher "exit $RC; $(tail -1 "$ERR")"
fi
rm -rf "$TMP/tools"

run replaces -- sh -c "printf '#!/bin/sh\necho seedstone 1.0.0\n' > '$TMP/install-replaces/seedstone'; chmod 755 '$TMP/install-replaces/seedstone'; exec sh '$INSTALLER'"
if [ "$RC" -eq 0 ] && installed 9.9.9 && has "$OUT" "1.0.0"; then
  ok replaces "an existing binary is replaced and its version printed beside the new one"
else
  fail replaces "exit $RC; $(tail -1 "$ERR")"
fi

# Ctrl-C while the archive downloads: the script stops with 130, its
# temporary directory is gone and nothing is left in the install directory.
# A background job of a script has SIGINT ignored, and a shell cannot trap a
# signal it entered with ignored, so the installer is started through a
# Python that restores the default first and then execs sh in place.
shim "$TMP/shim-slow" curl 'sleep 3; exec "$REAL_CURL" "$@"'
DIR="$TMP/install-interrupt"; mkdir -p "$DIR"
set +e
env -u NO_COLOR SEEDSTONE_BASE_URL="$BASE" SEEDSTONE_INSTALL_DIR="$DIR" TERM=xterm COLORTERM= \
  PATH="$TMP/shim-slow:$PATH" TMPDIR="$TMP/interrupt-tmp" python3 -I -c '
import os, signal, sys
signal.signal(signal.SIGINT, signal.SIG_DFL)
os.execvp("sh", ["sh", sys.argv[1]])' "$INSTALLER" > "$TMP/interrupt.out" 2> "$TMP/interrupt.err" &
pid=$!
mkdir -p "$TMP/interrupt-tmp"
sleep 1
kill -INT "$pid"
wait "$pid"; RC=$?
set -e
if [ "$RC" -eq 130 ] && empty && [ -z "$(ls -A "$TMP/interrupt-tmp")" ]; then
  ok interrupt "exit 130 on Ctrl-C, temporary directory removed, nothing installed"
else
  fail interrupt "exit $RC; install dir: $(find "$DIR" -mindepth 1 | tr '\n' ' '); temp: $(find "$TMP/interrupt-tmp" -mindepth 1 | tr '\n' ' '); $(tail -1 "$TMP/interrupt.err")"
fi

run path-hint -- sh "$INSTALLER"
if [ "$RC" -eq 0 ] && has "$OUT" "export PATH=" && has "$OUT" "$DIR"; then
  ok path-hint "a directory off PATH gets the export line"
else
  fail path-hint "exit $RC; no export line for $DIR"
fi

exit $status
