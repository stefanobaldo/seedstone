# Releasing

Versions are annotated tags on `main`, named `vMAJOR.MINOR.PATCH` and
following SemVer. The project is `0.x` while the keyspace holds only strings.

## Candidates first, for anything but a patch

A **minor or major** version carries new surface or new behaviour, and new
behaviour is declared only after it has run somewhere that is not a test
suite. So it is cut as a sequence of release candidates — `v0.2.0-rc.1`,
`rc.2`, … — each published as a pre-release with a binary, and the final
`v0.2.0` is tagged only after a candidate has served a stable window in the
environment it was cut for. A correction to a candidate is the next
candidate, never a direct final.

A candidate is published for the people testing it and nobody else: as a
GitHub pre-release and as an image on GHCR under its tag. Docker Hub,
crates.io and the Homebrew tap receive finals only, and once the final is out its candidates'
release pages are deleted (step 6 of "Cutting one").

A **patch** version is a correction with no new behaviour. It is tagged
directly and published as a normal release. A patch that would need a window
of its own was not a patch; cut it as a minor.

## What a tag does

Pushing a `v*` tag runs `.github/workflows/release.yml`, which produces six
kinds of artifact and not one:

- **The binary**, built with `--locked` on a native runner for each of four
  targets — `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`,
  `aarch64-apple-darwin` and `x86_64-apple-darwin` (the last cross-compiled
  on Apple silicon and smoke-tested under Rosetta) — and packaged as one
  archive per target with its `sha256`. An archive is refused unless the
  binary it contains is for the target it is named after — the architecture
  is read from the binary, not taken from the runner. The
  packaging itself is `scripts/package-release.sh`, which CI also runs on
  every pull request so that the day a tag is pushed is not the first day
  the script runs.
- **The Debian and RPM packages**, one of each per Linux target, built from
  the same binary by `scripts/linux-packages.sh` (`cargo-deb` and
  `cargo-generate-rpm`, with the metadata in `crates/seedstone/Cargo.toml`),
  each with its `sha256`. They install the binary and the systemd unit in
  `packaging/systemd/`, enable and start the service on a first install,
  and restart it on an upgrade only if it is running — the `.deb`'s
  maintainer scripts are the ones cargo-deb generates from Debian's own,
  the `.rpm`'s are in the manifest. A candidate's `-rc.N` becomes `~rc.N` in both, so a candidate
  sorts before its final. CI builds both on every pull request and installs
  them in a Debian and a Fedora container.
- **The container image**, built once per architecture (`linux/amd64` and
  `linux/arm64`) on native runners, smoke-tested, pushed by digest, and
  joined into one index under the tag on `ghcr.io/stefanobaldo/seedstone`,
  and, for a final, mirrored to `docker.io/<namespace>/seedstone` — the same
  index, the same digests. `latest` moves only for a final. It is the artifact
  a deployment pins, usually by digest rather than by tag: the digest to
  record is the **index's**, printed by the `image-index` job, and a
  deployment that pins by digest keeps pinning GHCR.
- **The GitHub Release**, carrying every archive and package with its
  checksum, and marked as a pre-release when the tag contains a `-`. Its
  notes are the changelog's section for this version, cut out by name; a
  candidate has no section of its own and falls back to `[Unreleased]`.
- **The four crates on crates.io**, for a final — `seedstone-resp`, `seedstone-core`,
  `seedstone-service` and `seedstone`, all at the workspace's version —
  published by `cargo publish --workspace` in dependency order, with a
  short-lived token from crates.io's trusted publishing and no stored
  secret. After the release, because a published crate version cannot be
  withdrawn, only yanked, and because `cargo binstall seedstone` resolves
  the release's archives. CI runs the same command with `--dry-run` on
  every pull request.
- **The Homebrew formula**, for a final — rendered by
  `scripts/render-formula.sh` from `packaging/homebrew/seedstone.rb.tmpl`
  and the release's four archive checksums, checked by `brew style`, and
  pushed to `Formula/seedstone.rb` on `seedstonelabs/homebrew-tap`. The job
  then installs it from the tap on a macOS runner and runs `brew test`:
  after the push on purpose, because the tap is mutable and a bad formula
  is corrected by the next commit there. Every rehearsal runs `brew style`
  on a formula rendered from placeholder checksums.

The workflow also refuses to publish a tag that disagrees with any of the
four published crates' versions, character for character
(`scripts/set-version.sh --check`), before it builds anything. Nothing is
published unless everything was built: the release waits for every archive
and both images, and the image index — and `latest` — the crates and the
formula wait for the release. That check is the reason
step 3 below insists on the candidate suffix.

## What a release depends on outside this repository

- **Docker Hub**: the repository variable `DOCKERHUB_NAMESPACE` and the
  secrets `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` (an access token with
  read and write scope). Without them, or with an expired token, the mirror
  step fails — after the GHCR index exists and independently of the GitHub
  Release, so the release itself is complete and only the mirror is missing.
  Fix the credential and re-run the `image-index` job.
- **crates.io**: the four crates exist there, and trusted publishing is
  configured on each — repository `stefanobaldo/seedstone`, workflow
  `release.yml`, no environment. A trusted-publishing token cannot create a
  crate, and a publisher can only be configured on a crate that exists, so
  a crate new to the workspace is published once by hand, by its owner,
  before the first tag that carries it. A crate whose publisher is not
  configured fails the `crates` job after the release and the image exist;
  a tag is not re-pushed. If nothing was uploaded, configure it and re-run
  the job. If the job failed part-way, a re-run stops at the first crate
  already up, since crates.io accepts a version once: read which are up
  (`cargo info <crate>@X.Y.Z`) and publish the rest by hand from the tagged
  commit, in the order above, with `cargo publish -p <crate> --locked`.
- **The Homebrew tap**: the repository `seedstonelabs/homebrew-tap`, with a
  deploy key that has write access, and its private half in this
  repository's secret `HOMEBREW_TAP_DEPLOY_KEY`. Without it the `homebrew`
  job fails after the release, the image and the crates exist, and the tap
  stays at the previous version; restore the key and re-run the job. A
  re-run that renders the formula already on the tap pushes nothing.

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

   **Rewrite the section for someone running the server, before the first
   candidate.** Entries are written one change at a time, each by the change
   that earns it, and read together they carry mechanism a reader cannot act
   on and detail the documentation already holds. Fold what changes nothing
   for that reader into one line — a performance line, a line of edge cases
   now answering as Redis does — and keep every entry that asks something of
   them or breaks something they rely on: a format, a refusal at startup, an
   accounting change, a removed field. Do it before `rc.1` rather than at the
   final: a candidate's release notes are `[Unreleased]` as it stands, so the
   first candidate already publishes whatever the section says.

2. `CHANGELOG.md`'s link footer: point `[Unreleased]` at the new version and
   give the new version its own definition. A final gets a compare against the
   version before it; the first release of all has nothing to compare against
   and gets its release URL instead. A candidate gets **no** definition — it
   has no section of its own to link, for the same reason step 1 keeps its
   entries under `[Unreleased]`.

   ```markdown
   [Unreleased]: https://github.com/stefanobaldo/seedstone/compare/vX.Y.Z...HEAD
   [X.Y.Z]: https://github.com/stefanobaldo/seedstone/compare/vA.B.C...vX.Y.Z
   ```

   It is a step here rather than a convention because nothing checks it: the
   headings render either way, the release workflow reads sections and not
   links, and a missing definition is invisible to every gate this repository
   has.

3. `bash scripts/set-version.sh X.Y.Z`, with the tag without its `v`,
   **verbatim, candidate suffix included** — `v0.1.0-rc.1` means
   `0.1.0-rc.1`, not the `0.1.0` it is being cut toward. The version lives in
   the root `Cargo.toml`, shared by every crate; the four published crates
   carry it and point at each other with `=`, and the script refreshes
   `Cargo.lock` and checks the result. Commit both files. It is the version
   the binary reports in `INFO` and `HELLO`, and the one the workflow compares
   the tag against, character for character. The release builds with
   `--locked`, which refuses a lock file that disagrees with the manifests,
   so a manifest edited by hand without the lock fails the release at its
   first build, with the tag already pushed and no longer movable.
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
5. Watch the release workflow — every job, not the release page. It ends
   with the four archives and their checksums attached to the release, and
   the image index on GHCR and, for a final, on Docker Hub, with the four
   crates on crates.io (`cargo info seedstone@X.Y.Z`) and the formula on the
   tap (`brew info seedstonelabs/tap/seedstone`) — every artifact of
   "What a tag does", not just the ones the release page shows.
6. After a final is published, delete its candidates' release pages, which
   only served the testing that is now over — the tags stay:

   ```sh
   gh release list --limit 50 --json tagName --jq '.[].tagName | select(test("^vX\\.Y\\.Z-rc\\."))'
   gh release delete vX.Y.Z-rc.N --yes     # once per candidate, no --cleanup-tag
   ```

   The tags are kept for the reason step 4 gives for never rewriting one,
   and the candidates' images on GHCR are kept because a deployment that
   ran a candidate pinned its digest, and deleting the image deletes the
   digest.
