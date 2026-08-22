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
  carrying only fields this server can state truthfully — `CONFIG GET` over
  the parameters that describe how it was started, and `SLOWLOG` and
  `LATENCY` answering as the switched-off monitors they are, so that a
  scrape completes without an error line per pass.
- A ceiling on a `KEYS` reply. Past 64 MiB of gathered keys the command is
  refused with an error naming `SCAN` as what to use instead, rather than
  returning a reply that would cost the server more than the client asked
  for.

### Changed

- A keyspace walk is at-least-once, not a snapshot. `KEYS` and `SCAN` answer
  with a set that was the keyspace at no single instant: a key created while
  a walk is in flight may be missed, and a key deleted while it is in flight
  may still appear. `KEYS` reports no key twice; `SCAN` may, exactly as in
  Redis.

## [0.0.0] - 2026-08-08

- A placeholder that reserved the name: a workspace, a pinned toolchain and
  the determinism gate, with no server behind them.
