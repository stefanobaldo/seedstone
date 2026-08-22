# Contributing

SeedStone is in its bootstrap phase, developed by a single maintainer.
Issues and questions are welcome. Large pull requests will probably be
declined for now — open an issue first so we can talk before you write code.

## Ground rules

- Everything in this repository is written in English.
- Code follows the conventions in [docs/coding-guide.md](docs/coding-guide.md).
- Commits follow [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `docs:`, `test:`, `chore:`, `ci:`). One commit per
  coherent change.
- Branches mirror the same vocabulary: `feat/<slug>`, `fix/<slug>`,
  `docs/<slug>`, `chore/<slug>`, kebab-case.
- `main` is protected. Changes land by pull request with a linear history
  (rebase merge). Versions are annotated tags on `main`, SemVer `0.x` —
  [docs/RELEASING.md](docs/RELEASING.md) is how one is cut.

## Developer Certificate of Origin

Every commit must carry a `Signed-off-by` line (`git commit -s`), certifying
the [Developer Certificate of Origin](https://developercertificate.org/):
that you wrote the change or otherwise have the right to submit it under this
project's licenses. Pull requests with unsigned commits fail the DCO check.
