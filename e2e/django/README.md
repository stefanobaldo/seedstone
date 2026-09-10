# The django-redis compatibility lanes

These lanes point a third party's test suite at seedstone and report what it
finds. Everything else in this repository is the project judging itself — our
tests, our simulator, our reading of the protocol. Here a cache backend nobody
involved with this project wrote connects the way it connects to Redis, and
accepts or rejects the answers on its own terms.

The suite is not vendored. `run.sh` fetches the published source archive of
the version a pair names, verifies it against the digest the pair pins,
extracts its `tests/` directory and runs it inside a container against a
freshly started server. The archive either matches the digest or the lane
stops.

## Two pairs

A pair is a directory beside `run.sh` — `pinned/` and `current/` — holding
the versions it runs (`pair.env`, `requirements.txt`), the expectations file
that makes it a gate, and the settings that point the suite at the server.
`run.sh <binary> <pair>` runs one of them; CI runs both.

**`pinned/` freezes one client pair and does not move.** `django-redis 4.12.1`
over `redis 4.1.4` and `Django 3.0.14`, on Python 3.7. This is the pair this
gate has exercised since it existed — one specific pair, chosen for the
workloads this project targets, not a claim about any deployment anywhere —
and it stays pinned so that the suite's verdict on this server keeps meaning
the same thing from one release to the next. Dependabot is told to leave it
alone (`.github/dependabot.yml`), and the advisories raised against it are
dismissed where they are raised, with the same sentence each time: these are
test-only dependencies, run inside a container, pinned by design.

**`current/` tracks current releases and moves with them.** `django-redis 7.0`
over `redis-py 8` and `Django 5.2`, on Python 3.12 — pinned exactly, never
floating, and bumped by Dependabot's pull requests, each judged by the run.
Its expectations file is longer than the pinned pair's, because the current
backend drives hashes and sets this server does not answer, and every one of
those rows names its command.

Both pairs pin the interpreter by the **manifest-list digest** of its image —
the digest of the multi-architecture index, not of one platform's image, so
that a machine developing on one architecture and a runner gating on another
resolve the same release rather than the same name. The tag is kept beside
the digest in a comment; the digest is what runs.

### What the moving pair caught, and what it needs to connect

Keeping a second pair that moves is worth exactly what it finds that a frozen
one cannot. Two operations changed the command they end at between the two
backends: `django-redis 7.0.0` deletes the keys a pattern matched through a
transactional pipeline, and increments through a server-side script, where
`4.12.1` did neither. Both tests pass on the pinned pair and are refused rows
on the current one — a client upgrade moving an operation onto a command this
server does not have is the failure mode only a pair that moves can report.

The current pair also names its protocol. **redis-py 8 speaks RESP3 by
default and opens every connection with `HELLO 3`; this server speaks RESP2
and only RESP2, and redis-py — unlike go-redis — does not downgrade when the
handshake is refused.** So `current/settings/lane.py` puts `protocol=2` in the
cache URL, and without it no test in this pair runs at all. Read on redis-py
8.1.0 against seedstone 0.1.1.

## The expectations file

`expectations.txt` lists the tests that do not pass, with a category and a
reason. `conftest.py` turns each row into a strict `xfail`, which is what makes
the list a gate rather than a note: a listed test that starts passing turns the
lane red until its row is removed, and a row that names nothing collected fails
the run outright. The list cannot quietly go stale.

The pinned pair writes one row per test. The current pair's rows may also carry
`*` as a wildcard and stand for a whole command family, because its backend
drives families this server answers none of and a row per test would repeat one
sentence twenty times; there, two rows may not claim the same test, and every
row must still match something collected.

The categories, and the difference between them is the whole point:

- **`out-of-rule`** — the test needs a command this server deliberately does
  not answer. The reason must name that command. These rows are permanent
  until the command arrives, and when it does they leave on their own and the
  suite passes deeper with nobody writing a new test.
- **`not-yet`** — the surface is planned and not built. These rows are
  temporary and shrink as the work lands.
- **`not-this-server`** (current pair only) — the test fails with this client
  pair against Redis too, so it is not a fact about this server. The reason
  carries the Redis version it was read against. One row has this category.

Every test also runs under a bound. The client's lock retries acquisition
forever, so a lock this server cannot grant is an infinite wait rather than a
failure — and a lane that hangs is neither red nor green, just late. The bound
turns every such wait into an ordinary failure a row can carry.

Moving a row from `not-yet` to `out-of-rule` to make the lane green is a
defect, not a fix. A review that sees a row change category without naming a
command this server refuses on purpose should stop.

## The lane authenticates

The server these lanes start requires a password, and the settings module
carries it in the cache URL — no username, so the client sends the
one-argument `AUTH <password>` that a Redis deployment with a `requirepass`
and no ACL user receives. **The password is a literal, `lane-password`, and it
protects nothing**: it is written into a file beside the lane when CI has not
handed one over, and it exists so that the path under test is the
authenticated one rather than the open one. Every other lane here does the
same; `redis-cli.sh` is the exception, and it runs open on purpose so that
path keeps its coverage too.

## Running it

```console
$ cargo build --release -p seedstone --locked
$ bash e2e/django/run.sh target/release/seedstone pinned
```

Replace `pinned` with `current` for the other pair.

It starts its own server and stops it on the way out. `SEEDSTONE_PORT`
overrides the port. Docker is required; on Linux the container shares the
host's network namespace, and elsewhere it reaches the server across the
bridge — the same container and the same pins either way.
