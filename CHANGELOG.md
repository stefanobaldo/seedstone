# Changelog

Notable changes to SeedStone, in the form of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow
SemVer and are `0.x` until the server persists data;
[docs/RELEASING.md](docs/RELEASING.md) is how one is cut.

## [Unreleased]

### Added

- A server. It speaks RESP2 over TCP, so `redis-cli` and the ordinary client
  libraries reach it unmodified: `GET`, `SET`, `DEL`, `EXISTS`, `EXPIRE`,
  `TTL`, `INCRBY`, `PING`, `ECHO`, `HELLO`, `COMMAND`, `CLIENT` and `QUIT`.
- Keyspace inspection: `SCAN`, with `MATCH` and `COUNT`; `KEYS`; `DBSIZE`;
  and `FLUSHDB`. `MGET` reads several keys in one request, as `DEL` and
  `EXISTS` do.
- The rest of `SET`'s algebra: `EX`, `PX`, `EXAT` and `PXAT` set a deadline,
  `NX` and `XX` make the write conditional, `KEEPTTL` leaves an existing
  deadline alone, and `GET` returns the value that was replaced.
- `PEXPIRE` and `PERSIST` beside `EXPIRE` and `TTL`, and `TYPE` and `STRLEN`
  to ask what a key holds without reading it.
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
  completes without an error line per pass.
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

### Changed

- A keyspace walk is at-least-once, not a snapshot. `KEYS` and `SCAN` answer
  with a set that was the keyspace at no single instant: a key created while
  a walk is in flight may be missed, and a key deleted while it is in flight
  may still appear. `KEYS` reports no key twice; `SCAN` may, exactly as in
  Redis.
- `SCAN` gathers up to `COUNT` keys per call across shards rather than
  answering from one shard per call. The cursor format is unchanged and
  clients need no change. `COUNT` is now the client's key target rather than
  a budget of buckets to visit, which is what it means in Redis: a call may
  answer with more keys than `COUNT` asked for, because the target is checked
  between shards and not inside one. As before, a call may also answer with
  no keys at all and a cursor that is not `0`; the loop stays the one it
  always was, calling until the cursor comes back `0`.

### Fixed

- Accepted connections now disable Nagle's algorithm. A pipelined batch larger
  than the read ceiling leaves in more than one write, and with Nagle on every
  write after the first waited for an acknowledgement the peer had no reason to
  send promptly.
- Eviction past the memory ceiling no longer stops early when the key the
  triggering command addressed is the oldest one it sampled. The key is
  excluded from candidacy instead, so a shard with room to make still makes it.

## [0.0.0] - 2026-08-08

- A placeholder that reserved the name: a workspace, a pinned toolchain and
  the determinism gate, with no server behind them.
