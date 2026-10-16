#!/bin/sh
# seedstone installer — https://seedstone.io/install.sh
#
# Installs the latest seedstone release on this machine: finds the release
# and the archive built for this operating system and CPU, downloads it
# with its checksum from the release's assets, verifies the SHA-256, and
# copies the one binary into place. It starts no server, writes no
# configuration, touches no shell file, and talks to no host but github.com
# and the one its release assets redirect to. Read it first.
#
#   curl -fsSL https://seedstone.io/install.sh | sh
#   wget -qO- https://seedstone.io/install.sh | sh
#
# Environment:
#   SEEDSTONE_VERSION      a tag to install, e.g. v0.3.2 (default: the latest release)
#   SEEDSTONE_INSTALL_DIR  where the binary goes (default: /usr/local/bin)
#   SEEDSTONE_BASE_URL     the repository, for a mirror or a test server
#                          (default: https://github.com/stefanobaldo/seedstone);
#                          the latest release is read from
#                          $SEEDSTONE_BASE_URL/releases/latest and an asset from
#                          $SEEDSTONE_BASE_URL/releases/download/<tag>/<file>
#
# Exit codes: 0 installed; 1 refused (unsupported machine, missing tool, no
# permission); 2 download failed; 3 checksum mismatch. Each with one sentence
# on stderr naming the cause and what to do.
#
# POSIX sh: runs under dash, bash, zsh and BusyBox ash. Everything below is a
# function, and `main` is called on the last line, so a download cut short
# defines a few functions and runs nothing.

DOCS="https://github.com/stefanobaldo/seedstone/blob/main/docs/install.md"
# The oldest glibc the Linux binaries run on: the highest GLIBC_x.y their
# symbols import, read from the release binary with `objdump -T`. The
# repository's installer check reads it again from every release it
# installs, so a toolchain that raises it turns that check red.
GLIBC_MIN=2.34

# --- output ---------------------------------------------------------------

# Colour and symbols only on a terminal, with NO_COLOR unset and TERM not
# dumb: the brand's amber where the terminal advertises 24-bit colour
# (COLORTERM), the sixteen-colour palette otherwise. Piped or plain, the
# same lines without an escape byte, so a CI log reads as a screen.
COLOR=0
setup_output() {
  if [ -t 1 ] && [ -z "${NO_COLOR+set}" ] && [ "${TERM:-dumb}" != dumb ]; then
    COLOR=1
    case "${COLORTERM:-}" in truecolor | 24bit) COLOR=2 ;; esac
  fi
}

# Styles, as SGR parameters: AMBER is the brand's amber on a 24-bit
# terminal and yellow elsewhere; the rest are the same everywhere.
style_amber() { if [ "$COLOR" -eq 2 ]; then printf '38;2;240;160;48'; else printf '33'; fi; }
style_green() { printf '32'; }
style_red() { printf '31'; }
style_bold() { printf '1'; }
style_dim() { printf '2'; }

# A span in a style, or the bare text where colour is off.
paint() { # <style parameters> <text>
  if [ "$COLOR" -ge 1 ]; then printf '\033[%sm%s\033[0m' "$1" "$2"; else printf '%s' "$2"; fi
}

# Every line of the report starts two columns in.
out() {
  printf '  %s\n' "$1"
}

# A step line: a symbol in its colour, a bold label, a value, and an
# optional note in dim.
step() { # <symbol> <style> <label> <value> [note]
  step_line="$(paint "$2" "$1")  $(paint "$(style_bold)" "$(printf '%-13s' "$3")") $4"
  if [ -n "${5:-}" ]; then step_line="$step_line   $(paint "$(style_dim)" "$5")"; fi
  out "$step_line"
}

# One sentence on stderr, then the exit code the header documents.
die() { # <exit code> <sentence>
  if [ "$COLOR" -ge 1 ]; then
    printf '  \033[31m✗\033[0m  %s\n' "$2" >&2
  else
    printf '  ✗  %s\n' "$2" >&2
  fi
  exit "$1"
}

# --- the machine ----------------------------------------------------------

# The release's target triple for this machine, in TARGET, and the words for
# the report in DETECTED. A machine no archive is built for is refused
# naming what was seen; the install page has the container image for it.
detect_target() {
  detect_os="$(uname -s)"
  detect_cpu="$(uname -m)"
  case "$detect_os" in
    Linux)
      if [ -f /etc/alpine-release ] || ldd --version 2>&1 | head -1 | grep -qi musl; then
        die 1 "this is a musl system, which the release binaries are not built for; the container image runs on it: $DOCS"
      fi
      detect_sys=unknown-linux-gnu
      detect_name=linux
      ;;
    Darwin)
      # Under Rosetta an Intel shell reports x86_64 on an Apple silicon
      # machine; the arm64 archive is the one for that machine.
      if [ "$detect_cpu" = x86_64 ] && [ "$(sysctl -n sysctl.proc_translated 2>/dev/null)" = 1 ]; then
        detect_cpu=arm64
      fi
      detect_sys=apple-darwin
      detect_name=macos
      ;;
    *)
      die 1 "no release is built for $detect_os $detect_cpu; the archives and the container image are at $DOCS"
      ;;
  esac
  case "$detect_cpu" in
    x86_64 | amd64) detect_cpu=x86_64 ;;
    aarch64 | arm64) detect_cpu=aarch64 ;;
    *) die 1 "no release is built for $detect_os $detect_cpu; the archives and the container image are at $DOCS" ;;
  esac
  TARGET="$detect_cpu-$detect_sys"
  case "$detect_name-$detect_cpu" in
    macos-aarch64) DETECTED="macos arm64" ;;
    *) DETECTED="$detect_name $detect_cpu" ;;
  esac
}

# Linux only: the binaries need GLIBC_MIN or newer, and a binary that fails
# at --version after replacing a working one is worse than a refusal. The
# version comes from getconf, or from ldd where getconf does not say.
check_glibc() {
  glibc_have="$(getconf GNU_LIBC_VERSION 2>/dev/null | sed -n 's/^glibc //p')"
  if [ -z "$glibc_have" ]; then
    glibc_have="$(ldd --version 2>/dev/null | sed -n '1s/.* \([0-9][0-9]*\.[0-9][0-9]*\)$/\1/p')"
  fi
  [ -n "$glibc_have" ] || return 0
  glibc_major="${glibc_have%%.*}"
  glibc_minor="${glibc_have#*.}"
  glibc_minor="${glibc_minor%%.*}"
  if [ "$glibc_major" -lt "${GLIBC_MIN%%.*}" ] ||
    { [ "$glibc_major" -eq "${GLIBC_MIN%%.*}" ] && [ "$glibc_minor" -lt "${GLIBC_MIN#*.}" ]; }; then
    die 1 "this system has glibc $glibc_have and the release binaries need glibc $GLIBC_MIN or newer; the container image runs on it: $DOCS"
  fi
  DETECTED="$DETECTED, glibc $glibc_have"
}

# --- the release ----------------------------------------------------------

# The latest release is the one the repository redirects releases/latest
# to; the tag is the last segment of the Location header. No API, no JSON.
latest_tag() {
  if [ "$FETCHER" = curl ]; then
    latest_location="$(curl -fsS -o /dev/null -w '%{redirect_url}' "$BASE_URL/releases/latest" 2>/dev/null)" || latest_location=""
  else
    latest_location="$(LC_ALL=C wget -S --max-redirect=0 -O /dev/null "$BASE_URL/releases/latest" 2>&1 | sed -n 's/^ *Location: *//p' | head -1 | tr -d '\r')"
  fi
  case "$latest_location" in
    */releases/tag/?*) printf '%s\n' "${latest_location##*/}" ;;
    *) return 1 ;;
  esac
}

# One URL to one file, following redirects (the assets live on another
# host). On failure the file is gone and FETCH_ERROR says what the server
# answered.
fetch() { # <url> <file>
  if [ "$FETCHER" = curl ]; then
    fetch_code="$(curl -fsSL --retry 2 -o "$2" -w '%{http_code}' "$1" 2>/dev/null)" && return 0
    case "$fetch_code" in
      000 | "") FETCH_ERROR="no answer" ;;
      *) FETCH_ERROR="HTTP $fetch_code" ;;
    esac
  else
    # wget translates its error line; the status code is read under C.
    fetch_said="$(LC_ALL=C wget -nv -O "$2" "$1" 2>&1)" && return 0
    FETCH_ERROR="$(printf '%s\n' "$fetch_said" | sed -n 's/.*ERROR \([0-9][0-9]*\).*/HTTP \1/p' | head -1)"
    [ -n "$FETCH_ERROR" ] || FETCH_ERROR="no answer"
  fi
  rm -f "$2"
  return 1
}

# sha256sum where there is one, shasum on a stock macOS; both read and write
# the two-field line the release's .sha256 holds.
checksum() {
  if [ "$HASHER" = sha256sum ]; then sha256sum "$@"; else shasum -a 256 "$@"; fi
}

# A mismatch deletes the download and prints both hashes; nothing is
# extracted from an archive that is not the release's.
verify() { # <dir> <file>
  if (cd "$1" && checksum -c "$2.sha256" > /dev/null 2>&1); then
    return 0
  fi
  verify_want="$(cut -d' ' -f1 "$1/$2.sha256")"
  verify_have="$(checksum "$1/$2" | cut -d' ' -f1)"
  rm -f "$1/$2"
  die 3 "the checksum of $2 does not match the release's: the release says $verify_want, the download is $verify_have; the download is deleted — run the installer again, and if it repeats, open an issue"
}

# --- the copy -------------------------------------------------------------

# The binary is copied beside its destination and moved over it, so a
# reader of the old one never sees a half-written file. A directory the
# user cannot write to is copied into with sudo, once, with its own prompt;
# no sudo is a refusal naming the directory and the variable that moves it.
install_binary() { # <source> <destination>
  [ -d "$INSTALL_DIR" ] || mkdir -p "$INSTALL_DIR" 2>/dev/null || true
  if [ -d "$INSTALL_DIR" ] && [ -w "$INSTALL_DIR" ]; then
    if ! { cp "$1" "$2.new" && chmod 755 "$2.new" && mv -f "$2.new" "$2"; }; then
      rm -f "$2.new"
      die 1 "could not copy the binary into $INSTALL_DIR"
    fi
    return 0
  fi
  if ! command -v sudo > /dev/null 2>&1; then
    die 1 "$INSTALL_DIR is not writable and there is no sudo; choose a directory you own with SEEDSTONE_INSTALL_DIR, e.g. SEEDSTONE_INSTALL_DIR=\$HOME/.local/bin"
  fi
  out "   $INSTALL_DIR is not writable; copying with sudo"
  sudo sh -c 'mkdir -p "$3" && cp "$1" "$2.new" && chmod 755 "$2.new" && mv -f "$2.new" "$2" || { rm -f "$2.new"; exit 1; }' sh "$1" "$2" "$INSTALL_DIR" ||
    die 1 "sudo could not copy the binary into $INSTALL_DIR; choose a directory you own with SEEDSTONE_INSTALL_DIR"
}

# The lines after the version: how to start, how to talk to it, and the
# package for this machine where apt-get, dnf or brew is on the PATH. They
# inform; nothing here is run.
closing_block() { # <version>
  out ""
  hint "Start a server" "seedstone"
  hint "Talk to it" "redis-cli ping"
  if command -v apt-get > /dev/null 2>&1; then
    case "$TARGET" in x86_64-*) closing_arch=amd64 ;; *) closing_arch=arm64 ;; esac
    hint "Run as a service" "sudo apt install ./seedstone_$1-1_$closing_arch.deb" "from the release page"
  elif command -v dnf > /dev/null 2>&1; then
    hint "Run as a service" "sudo dnf install ./seedstone-$1-1.${TARGET%%-*}.rpm" "from the release page"
  elif command -v brew > /dev/null 2>&1; then
    hint "Run as a service" "brew services start seedstone" "after brew install seedstonelabs/tap/seedstone"
  else
    hint "Run as a service" "$DOCS"
  fi
  hint "Documentation" "https://github.com/stefanobaldo/seedstone/tree/main/docs"
}

# A closing line: a dim label, the command in bold, an optional dim note.
hint() { # <label> <command> [note]
  hint_line="$(paint "$(style_dim)" "$(printf '%-18s' "$1")") $(paint "$(style_bold)" "$2")"
  if [ -n "${3:-}" ]; then hint_line="$hint_line   $(paint "$(style_dim)" "($3)")"; fi
  out "  $hint_line"
}

megabytes() { # <bytes> -> N.N
  printf '%d.%d' $(($1 / 1048576)) $((($1 % 1048576) * 10 / 1048576))
}

# --- main -----------------------------------------------------------------

main() {
  BASE_URL="${SEEDSTONE_BASE_URL:-https://github.com/stefanobaldo/seedstone}"
  INSTALL_DIR="${SEEDSTONE_INSTALL_DIR:-/usr/local/bin}"
  setup_output

  if command -v curl > /dev/null 2>&1; then
    FETCHER=curl
  elif command -v wget > /dev/null 2>&1; then
    FETCHER=wget
  else
    die 1 "neither curl nor wget is on the PATH; install one of them and run again"
  fi
  if command -v sha256sum > /dev/null 2>&1; then
    HASHER=sha256sum
  elif command -v shasum > /dev/null 2>&1; then
    HASHER=shasum
  else
    die 1 "neither sha256sum nor shasum is on the PATH, so the download could not be verified; install one and run again"
  fi
  command -v tar > /dev/null 2>&1 || die 1 "tar is not on the PATH; install it and run again"

  out "$(paint "$(style_amber);$(style_bold)" "◆ seedstone installer")"
  out ""

  detect_target
  case "$TARGET" in *-linux-gnu) check_glibc ;; esac
  step "→" "$(style_amber)" Detected "$DETECTED"

  if [ -n "${SEEDSTONE_VERSION:-}" ]; then
    TAG="$SEEDSTONE_VERSION"
    tag_how="pinned by SEEDSTONE_VERSION"
  else
    TAG="$(latest_tag)" || die 2 "could not read the latest release from $BASE_URL/releases/latest; check the connection, or pin a tag with SEEDSTONE_VERSION"
    tag_how="latest"
  fi
  VERSION="${TAG#v}"
  step "→" "$(style_amber)" Release "$TAG" "($tag_how)"

  FILE="seedstone-$VERSION-$TARGET.tar.gz"
  URL="$BASE_URL/releases/download/$TAG/$FILE"
  TMP="$(mktemp -d "${TMPDIR:-/tmp}/seedstone-install.XXXXXX")" || die 1 "could not create a temporary directory under ${TMPDIR:-/tmp}"
  # The directory goes on every exit; a signal exits, so a Ctrl-C during
  # the download does not carry on into a false diagnosis.
  trap 'rm -rf "$TMP"' EXIT
  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 143' TERM
  fetch "$URL" "$TMP/$FILE" || die 2 "could not download $FILE for $TAG ($FETCH_ERROR) from $URL; the releases are at $BASE_URL/releases"
  fetch "$URL.sha256" "$TMP/$FILE.sha256" || die 2 "could not download the checksum of $FILE for $TAG ($FETCH_ERROR) from $URL.sha256"
  step "↓" "$(style_amber)" Downloading "$FILE" "$(megabytes "$(wc -c < "$TMP/$FILE")") MB"

  verify "$TMP" "$FILE"
  step "✓" "$(style_green)" Verified "sha256 matches the release"

  tar -xzf "$TMP/$FILE" -C "$TMP" "seedstone-$VERSION-$TARGET/seedstone" ||
    die 2 "could not extract seedstone-$VERSION-$TARGET/seedstone from $FILE"
  previous=""
  if [ -x "$INSTALL_DIR/seedstone" ]; then
    previous="$("$INSTALL_DIR/seedstone" --version 2>/dev/null || true)"
  fi
  install_binary "$TMP/seedstone-$VERSION-$TARGET/seedstone" "$INSTALL_DIR/seedstone"
  if [ -n "$previous" ]; then
    step "✓" "$(style_green)" Installed "$INSTALL_DIR/seedstone" "(replaced $previous)"
  else
    step "✓" "$(style_green)" Installed "$INSTALL_DIR/seedstone"
  fi

  out ""
  out "  $(paint "$(style_amber);$(style_bold)" "$("$INSTALL_DIR/seedstone" --version)")"
  case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *)
      out ""
      out "  $(paint "$(style_dim)" "$INSTALL_DIR is not on your PATH; for this shell:")"
      out "    $(paint "$(style_bold)" "export PATH=\"$INSTALL_DIR:\$PATH\"")"
      ;;
  esac
  closing_block "$VERSION"
}


main "$@"
