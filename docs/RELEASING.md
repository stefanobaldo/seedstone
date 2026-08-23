# Releasing

Versions are annotated tags on `main`, named `vMAJOR.MINOR.PATCH` and
following SemVer. The project is `0.x` until it persists data.

## Candidates first, for anything but a patch

A **minor or major** version carries new surface or new behaviour, and new
behaviour is declared only after it has run somewhere that is not a test
suite. So it is cut as a sequence of release candidates — `v0.2.0-rc.1`,
`rc.2`, … — each published as a pre-release with a binary, and the final
`v0.2.0` is tagged only after a candidate has served a stable window in the
environment it was cut for. A correction to a candidate is the next
candidate, never a direct final.

A **patch** version is a correction with no new behaviour. It is tagged
directly and published as a normal release. A patch that would need a window
of its own was not a patch; cut it as a minor.

## What a tag does

Pushing a `v*` tag runs `.github/workflows/release.yml`: it builds the
release binary for x86_64 Linux with `--locked`, packages it with its
`sha256`, and publishes a GitHub Release — marked as a pre-release when the
tag contains a `-`. The packaging itself is `scripts/package-release.sh`,
which CI also runs on every pull request so that the day a tag is pushed is
not the first day the script runs.

## Cutting one

1. `CHANGELOG.md`: move the `[Unreleased]` entries under the new version and
   date; a candidate's entries stay under `[Unreleased]` until the final.
2. `crates/seedstone/Cargo.toml`: set `version` to the tag without its `v`,
   **verbatim, candidate suffix included** — `v0.1.0-rc.1` means
   `version = "0.1.0-rc.1"`, not the `0.1.0` it is being cut toward. That
   manifest and no other: the five crates carry independent versions, and this
   is the one the binary reports in `INFO` and `HELLO`, and the one
   `cargo pkgid -p seedstone` answers with — which is what the workflow
   compares the tag against, character for character.
3. `cargo build -p seedstone`, and commit the refreshed `Cargo.lock` together
   with the manifest. The lock records the crate's own version too, and the
   release job's first step is `cargo build --release -p seedstone --locked`,
   which refuses to update a lock file that disagrees with the manifest. A
   lock left behind therefore fails the release at its first step, with the
   tag already pushed and no longer movable. (`cargo update -p seedstone
   --precise X.Y.Z` does the same job if a build is not wanted.)
4. `git tag -s vX.Y.Z -m "vX.Y.Z"` on `main`, `git push origin vX.Y.Z`.
5. Watch the release workflow; the release appears with the archive and the
   checksum attached.
