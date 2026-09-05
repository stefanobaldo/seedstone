# Changelog

Notable changes to SeedStone, in the form of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow
SemVer and are `0.x` until the server persists data;
[docs/RELEASING.md](docs/RELEASING.md) is how one is cut.

## [0.1.0] - 2026-09-05

### Added

- A server. It speaks RESP2 over TCP, so `redis-cli` and the ordinary client
  libraries reach it unmodified. `GET` and `SET`, with the whole of `SET`'s
  algebra: `EX`, `PX`, `EXAT` and `PXAT` set a deadline, `NX` and `XX` make
  the write conditional, `KEEPTTL` leaves an existing deadline alone, and
  `GET` returns the value that was replaced. `DEL`, `EXISTS` and `MGET` take
  several keys in one request. `EXPIRE`, `PEXPIRE`, `TTL` and `PERSIST` for
  deadlines, `TYPE` and `STRLEN` to ask what a key holds without reading it,
  and `INCRBY`. Keyspace inspection through `SCAN`, with `MATCH` and `COUNT`,
  `KEYS`, `DBSIZE` and `FLUSHDB`. And the connection commands a client library
  expects: `PING`, `ECHO`, `HELLO`, `COMMAND`, `CLIENT` and `QUIT`.
- A keyspace walk is at-least-once, not a snapshot. `KEYS` and `SCAN` answer
  with a set that was the keyspace at no single instant: a key created while a
  walk is in flight may be missed, and a key deleted while it is in flight may
  still appear. `KEYS` reports no key twice; `SCAN` may, exactly as in Redis.
  `SCAN` gathers up to `COUNT` keys per call across shards, so `COUNT` is the
  client's key target rather than a budget of buckets to visit — which is what
  it means in Redis — and a call may answer with more keys than were asked
  for, or with none at all and a cursor that is not `0`. The loop is the
  ordinary one: call until the cursor comes back `0`.
- Authentication. `AUTH`, and `HELLO` with an `AUTH` clause, against one
  password for the `default` user. The password arrives in
  `--requirepass-file <path>` or in `SEEDSTONE_REQUIREPASS`, never in an
  argument, because a command line is readable by every other process on the
  host. A bind outside loopback refuses to start without one of the two,
  unless `--no-auth` says so deliberately.
- A memory ceiling. `--maxmemory <size>` bounds the keyspace and
  `--maxmemory-policy allkeys-lru|noeviction` decides what happens at it:
  evict least-recently-used keys, or refuse the write. A policy with no
  ceiling to reach is refused rather than silently ignored.
- The operational surface a monitoring agent reads: `INFO` in sections —
  `server`, `clients`, `memory`, `stats`, `keyspace` and `commandstats`,
  carrying only fields this server can state truthfully, and drawing the
  default document as Redis draws it, so a bare `INFO` and `INFO default`
  leave `commandstats` out where `INFO all` carries it — `CONFIG GET` over
  the parameters that describe how it was started, selected without regard
  to case as Redis selects a parameter name — and `SLOWLOG` and `LATENCY`
  answering as the switched-off monitors they are, so that a scrape
  completes without an error line per pass. The `INFO` document ends on its
  last field, as Redis's does: sections are separated by a blank line rather
  than terminated by one, byte-for-byte against `redis:6-alpine`
  (`redis_version:6.2.24`) and `redis:8-alpine` (`redis_version:8.10.1`),
  which agree.
- A ceiling on a `KEYS` reply. Past 64 MiB of gathered keys the command is
  refused with an error naming `SCAN` as what to use instead, rather than
  returning a reply that would cost the server more than the client asked
  for.
- `INFO commandstats` reports `usec` and `usec_per_call` beside `calls`, in
  Redis's field order. Each command is timed where it is counted: at the
  executor for the commands a shard runs, and at the edge for the requests no
  shard sees whole — where the reading spans the wait for the shards the
  request reached, and so measures what the request took rather than what it
  cost. A request this server splits is counted and timed at both layers, so
  the `cmdstat_` totals are not additive; `docs/ARCHITECTURE.md` says which
  of the two figures answers which question.
- `INFO` reports `total_error_replies` in its stats section and an
  `errorstats` section with one `errorstat_<code>:count=N` row per error
  code, counted at the edge for every error reply the server writes —
  including the authentication gate's refusals.
- The server stops on `SIGTERM` the way it stops on Ctrl-C. In a container it
  is PID 1, which ignores signals it has no handler for, so this is what
  makes a pod deletion end promptly instead of at the end of its grace period.
- A container image, `ghcr.io/stefanobaldo/seedstone:<tag>`, published for
  every release beside the binary archive: a distroless image holding the
  binary alone, running as a non-root user.
- Every error reply is written to stderr as one JSON line naming the
  command behind it, its error code and its message. `INFO errorstats` counts
  errors by code; it cannot say which command produced one, which makes an
  unexplained increment unexplainable. The line closes that.
- `SETEX key seconds value` — `SET key value EX seconds` under the name Redis
  gave it before `SET` grew options, which redis-py's `setex()` still puts on
  the wire. Same write, same refusals, same error texts as `SET … EX`, and its
  own `cmdstat_setex` line in `INFO commandstats`.

## [0.0.0] - 2026-08-08

- A placeholder that reserved the name: a workspace, a pinned toolchain and
  the determinism gate, with no server behind them.
