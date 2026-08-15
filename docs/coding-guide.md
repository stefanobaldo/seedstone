# Coding guide

Layout is `rustfmt`'s job and correctness is `clippy`'s; this guide records
only what no lint can express: the deliberate deviations from idiomatic Rust
this project makes, why it makes them, and the rules a reviewer needs before
judging the code.

## Deliberate deviations

Flagging these as defects in review is a false positive; each exists for a
reason the linked code documents in place.

- **Every `select!` reachable from the simulator is `biased`.** Determinism:
  the unseeded branch-choice RNG is entropy the simulator cannot replay. A
  source-grep gate (`every_select_in_simulated_code_is_biased`) enforces it.
- **The keyspace dict is hand-written, not `HashMap`.** `SCAN`'s stable
  cursor under growth and an explicitly seeded hasher are correctness
  requirements; `std`'s `RandomState` is banned workspace-wide for
  iteration-order nondeterminism.
- **Shard command handlers are plain `fn`, never `async fn`.** A handler
  that cannot `await` cannot yield mid-command, so per-key operations are
  atomic by construction. The signature is the enforcement.
- **Some gates grep source text.** Cruder than an AST and accepted: the
  failure mode of a stale grep is a false red, never a silent green.
- **Entropy enters in `main` only**, behind a site-local `allow` carrying its
  reason. The core receives seeds; it never draws them.

## Lint policy

- `clippy::pedantic` and `clippy::nursery` are on, promoted to errors in CI.
- A deviation is allowed **at the site**, as
  `#[allow(clippy::…, reason = "…")]` — the reason is mandatory.
- Workspace-level allows exist only for lints that are noise as a class;
  each would carry a comment in `Cargo.toml` and an entry here. **There are
  none.** Every deviation in the tree is site-local, and there are five:
  `slot.rs` narrowing a quotient it has just bounded, `shard.rs` taking its
  trace sink, log factory and expiry policy by value so a caller can move
  them in, `main.rs` drawing the hash seed from the OS — the composition root
  is the one place entropy may enter — `crates/seedstone-sim/src/sweep.rs`
  spawning its workers past `clippy::disallowed_methods`, since it starts
  whole simulated runs and never reaches inside one, which is what that
  prohibition protects — and one `unreachable_code` in a test whose loop can
  only end by returning. Each carries its `reason`, and the count belongs here
  because a guide that undercounts its own exceptions is how a sixth one
  arrives unremarked.
- `clippy.toml` carries the two settings a lint reads rather than a lint
  being switched off. `doc-valid-idents` lists the proper nouns
  `doc_markdown` would otherwise demand backticks around — `SeedStone`,
  `SipHash`, `FxHash` are names in prose, not Rust items, and backticking
  them would make the documentation wrong to make it tidy. The complexity
  ceilings (`cognitive-complexity-threshold`, `too-many-lines-threshold`,
  `too-many-arguments-threshold`) are written out even where they match the
  defaults, because a ceiling nobody can see is not a policy. Refactor
  before raising one.

## Conversions

No bare `as` between integer widths. A widening is `From`; a narrowing is
`try_from` with an `expect` whose message states the invariant that makes it
hold, or a checked path that propagates. This is not pedantry about a cast
operator: a pointer-width-dependent `as` shipped a real defect here, and a
silent truncation in a length field produces a record that disagrees with
its own contents — the class of bug that surfaces as data loss on one
target and never on the developer's.

Where a reinterpretation is genuinely intended, say so with `cast_unsigned`
/ `cast_signed` rather than leaving `as` to mean two different things in one
codebase.

## Review briefing

Reviews weigh, in order: correctness (including determinism), performance,
clarity of boundaries, idiomatic form. Generic style commentary is the
linter's job, not the reviewer's.
