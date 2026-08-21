# The django-redis compatibility lane

This lane points a third party's test suite at seedstone and reports what it
finds. Everything else in this repository is the project judging itself — our
tests, our simulator, our reading of the protocol. Here a cache backend nobody
involved with this project wrote connects the way it connects to Redis, and
accepts or rejects the answers on its own terms.

The suite is not vendored. `run.sh` fetches the published source archive,
verifies it against a digest pinned in the script, extracts its `tests/`
directory and runs it inside a container against a freshly started server.
That keeps a third party's 1,200 lines of tests out of this repository without
giving up a reproducible run: the archive either matches the digest or the lane
stops.

## The client pair is pinned deliberately

`requirements.txt` freezes the cache backend, the Redis client under it, the
web framework they need and the test runner. This is the client pair this gate
exercises — one specific pair, chosen for the workloads this project targets,
not a claim about any deployment anywhere.

The pin extends to the interpreter, which is why the lane runs in a container
rather than on whatever Python the CI runner offers. The pair wants an
interpreter older than the runner images will keep providing, and an
interpreter chosen by the runner is a variable this gate did not intend to
have.

**The pin is lifted once this milestone's work is complete.** At that point the
frozen pair is replaced by current releases — still pinned, never floating —
and tracked by Dependabot from then on. Lifting it is the work of whoever
closes the milestone, not of whoever adds a command; until then, treat these
versions as fixed and this section as the reason.

## The expectations file

`expectations.txt` lists every test that does not pass, with a category and a
reason. `conftest.py` turns each row into a strict `xfail`, which is what makes
the list a gate rather than a note: a listed test that starts passing turns the
lane red until its row is removed, and a row naming a test that no longer
exists fails collection outright. The list cannot quietly go stale.

Two categories, and the difference between them is the whole point:

- **`out-of-rule`** — the test needs a command this server deliberately does
  not answer. The reason must name that command. These rows are permanent
  until the command arrives, and when it does they leave on their own and the
  suite passes deeper with nobody writing a new test.
- **`not-yet`** — the surface is planned and not built. These rows are
  temporary and shrink as the work lands.

Every test also runs under a bound. The client's lock retries acquisition
forever, so a lock this server cannot grant is an infinite wait rather than a
failure — and a lane that hangs is neither red nor green, just late. The bound
turns every such wait into an ordinary failure a row can carry.

Moving a row from `not-yet` to `out-of-rule` to make the lane green is a
defect, not a fix. A review that sees a row change category without naming a
command this server refuses on purpose should stop.

## Running it

```console
$ cargo build --release -p seedstone --locked
$ bash e2e/django/run.sh target/release/seedstone
```

It starts its own server and stops it on the way out. `SEEDSTONE_PORT`
overrides the port. Docker is required; on Linux the container shares the
host's network namespace, and elsewhere it reaches the server across the
bridge — the same container and the same pins either way.
