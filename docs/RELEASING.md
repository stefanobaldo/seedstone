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

Pushing a `v*` tag runs `.github/workflows/release.yml`, which produces three
artifacts and not one:

- **The binary**, built for x86_64 Linux with `--locked` and packaged with its
  `sha256`. The packaging itself is `scripts/package-release.sh`, which CI also
  runs on every pull request so that the day a tag is pushed is not the first
  day the script runs.
- **The container image**, `ghcr.io/stefanobaldo/seedstone:<tag>` — built,
  smoke-tested and pushed by the same run. It is the artifact a deployment
  pins, usually by the digest the run prints rather than by the tag, so record
  the digest wherever the deployment lives.
- **The GitHub Release**, carrying the binary and its checksum, and marked as a
  pre-release when the tag contains a `-`. Its notes are the changelog's
  section for this version, cut out by name; a candidate has no section of its
  own and falls back to `[Unreleased]`.

The workflow also refuses to publish a tag whose name disagrees with
`cargo pkgid -p seedstone`, character for character. That check is the reason
step 2 below insists on the candidate suffix.

## Cutting one

1. `CHANGELOG.md`: move the `[Unreleased]` entries under the new version and
   date; a candidate's entries stay under `[Unreleased]` until the final.

   **A first release has no `Changed` and no `Fixed`.** Both are relative to
   something published, and before the first tag there is nothing to be
   relative to: a correction to code that never shipped is invisible to every
   reader of that release, and belongs to the commit that made it. Whatever
   such an entry says that a new reader still needs — a contract, a
   compatibility claim — is stated positively under `Added`, as a property of
   the thing rather than a history of it. This is worth checking at the move
   and nowhere earlier: each entry is written by the plan that earns it, when
   the destination is `[Unreleased]` and the question cannot yet be asked, so
   the assembled document is first read as a whole here.

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
4. Tag `main`, signed, **carrying the changelog section as its message** — cut
   by the same `awk` the release workflow uses for the notes, so that the tag
   and the release cannot drift apart:

   ```sh
   section() {
     awk -v want="## [$1]" 'index($0, want) == 1 { inside = 1; next }
                            inside && /^## \[/ { exit }
                            inside { print }' CHANGELOG.md
   }
   { printf 'vX.Y.Z\n'; section X.Y.Z; } | cat -s \
     | git tag -s --cleanup=whitespace vX.Y.Z -F -
   ```

   **`--cleanup=whitespace` is not optional.** `git tag -F` defaults to
   `--cleanup=strip`, which deletes every line beginning with `#` as a
   comment — so the changelog's `### Added`, `### Changed` and `### Fixed`
   headings vanish silently and the command still succeeds. `cat -s` collapses
   the blank line `awk` leaves at each end.

   Read it back before pushing, while the tag is still local and
   `git tag -d` undoes it:

   ```sh
   git tag -v vX.Y.Z          # message in full, and the signature
   git rev-parse vX.Y.Z^{}    # the commit it names
   git push origin vX.Y.Z
   ```

   `git tag -v` fails with *"gpg.ssh.allowedSignersFile needs to be configured"*
   when signing with an SSH key and no allowed-signers file exists. That is a
   local verification setting and says nothing about the tag, which is signed
   either way and is verified by the forge against the uploaded key. To verify
   locally as well:

   ```sh
   echo "$(git config --get user.email) $(cat ~/.ssh/<key>.pub)" \
     >> ~/.config/git/allowed_signers
   git config --global gpg.ssh.allowedSignersFile ~/.config/git/allowed_signers
   ```

   *(Tags through `v0.1.0-rc.2` carry only the version as their message. The
   body starts at `v0.1.0`, and the earlier tags are deliberately left alone:
   rewriting a published tag creates a new object under the same name, and
   `git fetch` does not update a tag a clone already has, so every existing
   clone would silently disagree with the repository about what that tag is.)*
5. Watch the release workflow. It ends with the archive and its checksum
   attached to the release, and the image pushed — all three artifacts of
   "What a tag does", not just the one the release page shows.
