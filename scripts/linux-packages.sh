#!/usr/bin/env bash
# Builds the Debian and RPM packages from target/release/seedstone, each with
# its checksum, into <out-dir>.
#
#   linux-packages.sh <out-dir>
#
# Run by the release workflow on each Linux target, and by CI on every pull
# request, so the two cannot build the packages differently. The packages
# carry the workspace version. A pre-release `-rc.N` becomes `~rc.N` in both,
# so a candidate sorts before its final: cargo-deb maps it itself, and
# cargo-generate-rpm refuses a `-` in a version, so it is given the mapped
# one. Measured with cargo-deb 3.8.0 and cargo-generate-rpm 0.21.0.
set -euo pipefail
out="${1:?out dir}"
mkdir -p "$out"
cargo install cargo-deb --version 3.8.0 --locked
cargo install cargo-generate-rpm --version 0.21.0 --locked
# From the manifest, not `cargo pkgid`, which reports the lockfile's entry.
version="$(cargo metadata --no-deps --format-version 1 \
  | jq -r '.packages[] | select(.name == "seedstone") | .version')"
cargo deb -p seedstone --no-build --no-strip --output "$out/"
# `-p` here is the crate's directory under the workspace root, and the asset
# sources in its metadata are relative to that root.
cargo generate-rpm -p crates/seedstone -s "version = \"${version/-/\~}\"" --output "$out/"
cd "$out"
# A GitHub release stores `~` in an asset's name as `.` (measured 2026-10-12),
# so a checksum naming the `~` file would not match the file downloaded. The
# file is renamed to what the release will call it; the version inside the
# package keeps its `~`, which is what the ordering reads.
for p in *.deb *.rpm; do
  [ "$p" = "${p//\~/.}" ] || mv -- "$p" "${p//\~/.}"
done
for p in *.deb *.rpm; do sha256sum "$p" > "$p.sha256"; done
ls -l
