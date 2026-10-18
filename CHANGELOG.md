# Changelog

Notable changes to SeedStone, in the form of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow
SemVer and are `0.x` while the keyspace holds only strings;
[docs/RELEASING.md](docs/RELEASING.md) is how one is cut.

## [Unreleased]

### Added

- **A one-line install for macOS and Linux.**
  `curl -fsSL https://seedstone.io/install.sh | sh` finds the latest
  release, downloads the archive built for the machine, verifies its
  SHA-256 and copies the binary into `/usr/local/bin` (or
  `SEEDSTONE_INSTALL_DIR`). It starts no server and writes nothing else; it
  refuses, saying what to use instead, a machine no archive is built for, a
  musl system and a glibc older than 2.34. `SEEDSTONE_VERSION` pins a
  release. [docs/install.md](docs/install.md#one-line) has the whole of it.

- **[seedstone.io](https://seedstone.io).** One page: how to install it,
  on every channel; a seed reproducing a bug, run after run; the
  benchmark cells where it is ahead of every engine measured but those
  level with it, each named with its figure, from the
  [benchmark page](docs/benchmarks.md); what running it looks like; and
  what it answers. Every section stands complete without a script.

- **The `recovery` line says where a start's time went.** Under
  `--data-dir`, it now carries `read_ms`, `images_ms` and `replay_ms`
  beside `elapsed_ms`: the time spent reading the log directory, loading
  the snapshots, and replaying the log over them. A slow start shows which
  of the three to look at.

- **A stop whose log could not be synced says so.** Under `--data-dir`, a
  stop whose last sync of the log failed, or that came while writes were
  still refused after an earlier failure, now writes `shutdown_unsynced`
  after `stopping`. It used to end as if the log had been synced; what was
  written since the last successful sync may not be on disk.

### Changed

- **A start reads its data directory at the disk's bandwidth.** Under
  `--data-dir`, a start now reads the log and the snapshots in 4 MiB reads
  and checksums them through a table. On a network-attached volume with
  milliseconds of latency per request, a start was bound by that latency
  and is now bound by the volume's bandwidth. Measured on a 744 MB
  directory with one executor, on an Axion core, on 2026-10-18:

  | | before | after |
  |---|---|---|
  | 5 ms of read latency, cold page cache | 14.4 s | 3.5 s |
  | page cache warm | 2.0 s | 0.4 s |

  The `recovery` line's `read_ms` and `images_ms` show the change on a
  given deployment.

- **The log's segment format moves to version 5.** Under `--data-dir`, each
  segment's header now lists where its process's start cut damaged shards,
  and the segment records where each sync was issued; snapshots carry the
  version too. This version reads a data directory written by 0.3.x as it
  is; a 0.3.x server refuses one this version has written to, so going back
  means starting from an empty directory.

### Fixed

- **After a crash under load, `lossy_shards` no longer reports every
  shard.** Under `--data-dir`, a crash's torn tail of the log and damage to
  records that had been synced looked the same to a start, so it charged
  the tear to every shard. The log now records where
  each sync was issued; a start charges only damage before the last such
  point, and reports the tear as `torn_bytes` on the `recovery` line,
  charged to no shard (#88).

- **`CONFIG GET slowlog-max-len` answers.** It answered an empty array, so
  a monitoring client that reads the slow log's capacity before it calls
  `SLOWLOG GET` failed its run. It now answers `128`, Redis's default.

- **Under `--fsync always`, a read whose key has expired is answered at
  once.** Removing the key writes its deletion to the log, and that made
  the read wait for the next sync, and answer with the write refusal if the
  sync failed. Only writes wait for the sync now.

- **A large damaged stretch of a log segment no longer costs the records
  after it.** Under `--data-dir`, a start steps over damage in a segment
  and checks what it finds there, up to a bounded amount of work per
  damaged stretch. A record that began within that bound but ran past it
  was given up untried, and with it the rest of the segment. Such a record
  is now checked whole; only the next one past the bound gives up the
  segment, as before.

- **A start no longer serves records discarded at an earlier start when
  damage hides where they were discarded.** Under `--data-dir`, a start
  that cuts a damaged shard writes a record marking the older records past
  the cut as discarded. If a later start could not read that record — a
  damaged stretch of the segment holding it, or a segment it could not read
  beside a snapshot it could — it replayed the discarded records,
  sometimes over that process's snapshot, into a state the node never
  held, and in the first case without reporting the shard lossy. The
  segment headers and snapshots now say where each start cut, and a start
  reads them as it reads the record.

- **A failed start no longer costs the next one its synced records.** Under
  `--data-dir`, a start that failed after reading a damaged log segment
  could leave the next start without records that had been synced, and
  nothing reported it. A start now keeps every segment it may still need
  and syncs the log it recovered from before it serves; one that cannot
  record where a damaged shard resumes now fails with `recovery_failed` and
  exits 1, instead of writing `log_fault` and serving with writes refused.
  The `recovery` line now comes before anything that can fail the start,
  and its `elapsed_ms` covers recovery alone, no longer the opening of the
  log for writing.

- **A crash during compaction no longer brings back discarded records.**
  Under `--data-dir`, once every executor has a durable snapshot the node
  removes the files of earlier processes. It removed them in whatever order
  the directory listed them, so a crash part-way through could leave
  records an earlier start had discarded at a gap in a damaged log, with
  the record that marked them discarded already gone, and the next start
  could replay them. The files now go oldest process first, and each
  process's removals are made durable before the next one's begin.

## [0.3.3] - 2026-10-12

### Added

- **More ways to install the same binary.** A release now carries archives
  for Linux x86_64 and aarch64 and for macOS on Apple silicon and Intel,
  each with its checksum, and the container image is built for `amd64` and
  `arm64` under one tag — on `ghcr.io/stefanobaldo/seedstone` as before, and
  mirrored to Docker Hub. Debian and RPM packages install the server with
  a systemd unit: the service runs as a dynamic user on loopback, with its
  log under `/var/lib/seedstone`; a password goes in `/etc/default/seedstone`
  or `/etc/sysconfig/seedstone`. `cargo install seedstone` and
  `cargo binstall seedstone` work too; the three library crates the server
  is built from are published beside it, with no API stability promise.
  On macOS and Linux, `brew install seedstonelabs/tap/seedstone` installs
  the release archive, and on macOS `brew services start seedstone` runs
  it as a service. A Helm chart, `oci://ghcr.io/stefanobaldo/charts/seedstone`,
  runs one node on Kubernetes with optional persistence and the password
  from a Secret you own.
  `docs/install.md` is the list.

### Fixed

- **A new Docker volume works for `--data-dir` as it is.** The image now
  ships `/data` owned by the user the server runs as, so a named volume
  mounted there is writable from its first start; before, the server
  stopped at startup with `recovery_failed` until the volume was handed
  over with a `chown`.

## [0.3.2] - 2026-10-11

### Fixed

- **`KEYS` is back to 0.2.0's figure.** 0.3.1 recovered two thirds of the
  regression 0.3.0 introduced; this release recovers the rest. On the
  benchmark cell `KEYS` now reads the same calls per second and CPU per call
  as 0.2.0, within run-to-run variation. The cause was two signals each
  executor checked ahead of every batch of commands, which a `KEYS` call,
  reaching every shard, paid once per shard (#102).

## [0.3.1] - 2026-10-10

### Fixed

- **`KEYS` is back to 0.2.0's calls per second and CPU per call.** Since
  0.3.0 every executor re-armed its stop signal after each batch of
  commands, and arming it took a lock the whole pool shares, twice. A `KEYS`
  call reaches every shard, so it paid that once per shard: 17 % fewer calls
  per second and 20 % more CPU per call on the benchmark cell, and nothing
  measurable on commands that reach one shard (#102).

## [0.3.0] - 2026-10-08

### Added

- **Persistence.** `--data-dir PATH` keeps a write-ahead log under
  `PATH/wal/` and recovers the keyspace on the next start from each shard's
  newest snapshot plus the log since it. A shard whose log has a gap is
  recovered up to the gap and reported. One node per directory: a second one
  started on it is refused. A clean stop (`SIGTERM`, `SIGINT`, or `SHUTDOWN`
  from a client) syncs the log before the process ends.
- **`--fsync always|interval|never`** says when the log is synced and what a
  crash can cost: `always` acknowledges a write only once it is on disk,
  `interval` (the default) syncs every 100 ms while there is anything to
  sync, busy or idle, and `never` leaves the log to the kernel and keeps the
  last snapshot. The sync runs off the request path; a read waits for one
  only when it is pipelined behind a write on its own connection.
- **One sync at a time for the whole node.** The log is written by one
  writer per node, so `--fsync always` issues one sync at a time, and each
  covers every write that was ready when it was issued (#81). Measured on a
  cloud disk with 3 240 provisioned IOPS against Redis 8.10.0 with
  `appendfsync always`, `SET` under `--fsync always` reads 1.63× Redis's
  throughput at pipeline depth 64 and level with it at depth 1.
- **Snapshots and compaction.** Past 64 MiB of log an executor images its
  shards without stopping them, and the node removes the log every image
  covers. The directory stays within a stated bound — about three times the
  snapshots plus 64 MiB per executor, plus one 64 MiB file — across restarts
  too: a previous process's files are replaced once they exceed it.
- **`SAVE`, `BGSAVE` and `LASTSAVE`.** `BGSAVE` asks for an image now; `SAVE`
  answers once an image taken after it is durable, without blocking the
  shards; `LASTSAVE` is the instant since which every shard has had one.
- **`SHUTDOWN [NOSAVE|SAVE]`**: the clean stop, asked for by a client. Under
  `--no-auth` any client that can connect can use it.
- **A failing or full disk is met with refusal**, not with acknowledgements
  the node cannot keep: writes answer `MISCONF`, reads are served, and each
  executor resumes on its own once a snapshot of its memory is durable; a
  refusing executor appends nothing to the log meanwhile, not even a lazily
  expired key's deletion (#85). A start whose first write to the log fails
  begins the same way. The reply, and how it compares with Redis's on a
  failed AOF write, is in `docs/compatibility.md` (#72).
- **A slow sync is visible.** A sync in flight for more than a second writes
  `sync_slow`, and `sync_slow_ended` when it ends (#77).
- **`INFO persistence`**: the log's size, syncs in flight and slow, images
  taken and when, executors refusing, and shards whose recovered state may
  miss writes until an image covers them — the count falls as images land,
  and each `snapshot` line says how many it cleared (#73). Redis's field
  names where the meaning matches, this server's own after them, nothing
  filled in; `docs/operations.md` lists every field.
- **Log events** for all of the above: `recovery` (with how long the start
  took, `elapsed_ms`), `recovery_truncated`, `recovery_failed`, `log_fault`,
  `snapshot_fault`, `sync_slow`, `sync_slow_ended`, `refusal_ended`,
  `snapshot`, `compaction`, `shutdown_timeout`, `fsync_ignored` — one table
  in `docs/operations.md`.
- `bench/campaign.sh durability`: `SET` at two pipeline depths under each
  `--fsync` setting, against Redis with AOF at the matching `appendfsync`,
  and `GET` with the log synced on every write against no log.

### Fixed

- A load that never lets an executor's queue run dry — clients walking the
  keyspace with `KEYS` were enough — no longer holds off its housekeeping:
  active expiry, rehashing and, under `--data-dir`, snapshots, compaction and
  the end of a refusal now run every 100 ms whatever the load (#79).

## [0.2.0] - 2026-09-22

### Added

- `PTTL`, `EXPIREAT` and `PEXPIREAT`. Their forms and how they compare with
  Redis are in [docs/compatibility.md](docs/compatibility.md).
- [docs/compatibility.md](docs/compatibility.md) — every command this server
  answers, how each differs from Redis 6.2.24 and 8.10.1, and what it refuses
  with which reply. A test keeps it in step with `COMMAND`.
- Log lines are JSON objects with `ts`, `level` and `evt`, one per line on
  stderr. The events and their fields are in
  [docs/operations.md](docs/operations.md).
- Password rotation without a restart: `--requirepass-file` may hold two
  passwords, either of which authenticates, and `SIGHUP` re-reads it. The
  procedure is in [docs/operations.md](docs/operations.md).

### Changed

- The startup and bind-failure lines are JSON. Anything matching the old
  text should select on `"evt":"listening"` or `"evt":"bind_failed"`.
- A password file with an empty or whitespace-only line, or more than two
  lines, is refused at startup, naming the rule.
- Lower CPU per request: fewer allocations and copies per command, pipelined
  `MGET`s travel with the commands around them instead of making a round trip
  of their own, and a pipeline wakes its connection once. The measurements
  are in [docs/benchmarks.md](docs/benchmarks.md).
- Each key costs 96 bytes of fixed overhead in `used_memory`, up from 80, so
  the same `--maxmemory` holds somewhat fewer keys before evicting.
- The allocator is mimalloc: resident memory grows and is released on its
  schedule rather than glibc's, and `MIMALLOC_*` environment variables reach
  it. `used_memory` is unaffected.

### Removed

- `mem_fragmentation_ratio` from `INFO memory`. It was a constant `1.00`, not
  a measurement.

### Fixed

- `redis-cli --pipe` transfers complete: an empty line between pipelined
  commands is ignored, as Redis ignores it.
- Edge cases answer as Redis 6.2.24 and 8.10.1 do: the largest expiry span a
  command accepts, and the error a `HELLO` naming an unsupported protocol
  version gets before authentication is considered.

## [0.1.1] - 2026-09-08

### Added

- `SETEX key seconds value` — `SET key value EX seconds` under the name Redis
  gave it before `SET` grew options, which redis-py's `setex()` still puts on
  the wire. Same write, same refusals, same error texts as `SET … EX`, and its
  own `cmdstat_setex` line in `INFO commandstats`.
- `SETNX key value` and `PSETEX key milliseconds value` — `SET key value NX`
  and `SET key value PX milliseconds` under the names Redis gave them before
  `SET` grew options, which redis-py's `setnx()` and `psetex()` still put on
  the wire. `SETNX` answers `1` or `0` rather than `SET … NX`'s `OK` or nil,
  which is how Redis 6.2.24 and 8.10.1 spell that reply. Each gets its own
  `cmdstat_` line in `INFO commandstats`.

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

## [0.0.0] - 2026-08-08

- A placeholder that reserved the name: a workspace, a pinned toolchain and
  the determinism gate, with no server behind them.

<!-- [0.0.0] has no definition: it was a crates.io placeholder and was never
     tagged in git, so every URL for it would 404. -->
[Unreleased]: https://github.com/stefanobaldo/seedstone/compare/v0.3.3...HEAD
[0.3.3]: https://github.com/stefanobaldo/seedstone/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/stefanobaldo/seedstone/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/stefanobaldo/seedstone/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/stefanobaldo/seedstone/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/stefanobaldo/seedstone/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/stefanobaldo/seedstone/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/stefanobaldo/seedstone/releases/tag/v0.1.0
