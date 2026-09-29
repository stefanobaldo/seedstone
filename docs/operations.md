# Operations

How this server is run and watched: its command line, what it writes, the
signals it answers, how its password is delivered and rotated, what running
without one means, what `--data-dir` promises, and what `INFO` gives a
monitor. Every claim here is measured on the binary of the version it was
written for, and a test (`crates/seedstone-service/tests/operations_page.rs`)
holds the table in *Output* to the code, so a line cannot gain or lose a field
without this page saying so.

## Command line and environment

```
seedstone [--bind ADDR:PORT] [--max-clients N] [--maxmemory SIZE]
          [--maxmemory-policy allkeys-lru|noeviction] [--requirepass-file PATH]
          [--no-auth] [--data-dir PATH]
seedstone --version | --help
```

| Flag | Default | Meaning |
|---|---|---|
| `--bind ADDR:PORT` | `127.0.0.1:6379` | The address to listen on. The default is loopback so that a server started with no arguments is not reachable from a network. |
| `--max-clients N` | `10000` | How many connections are served at once; the next one is told `ERR max number of clients reached` and closed. |
| `--maxmemory SIZE` | none | A ceiling on the keyspace, in bytes or with a suffix (`512mb`, `2gb`). |
| `--maxmemory-policy` | `noeviction` | What happens at the ceiling: `allkeys-lru` evicts, `noeviction` refuses writes. Only with `--maxmemory`. |
| `--requirepass-file PATH` | none | The password file: one password per line, one or two lines. See *Password and rotation*. |
| `--no-auth` | off | Run with no password, on purpose. See *Running without a password*. |
| `--data-dir PATH` | none | Where the node keeps its log. With it, every write is recorded and replayed on the next start; without it a restart is an empty keyspace. Snapshots keep it bounded; see *What `--data-dir` promises*. |

`--version` and `--help` answer on stdout and exit 0, in first position only.
`SEEDSTONE_REQUIREPASS` in the environment is the other way to give a
password — one, read once. A password is never an argument: a command line is
readable by every process on the host.

A bind outside loopback with no password is refused unless `--no-auth` is
given. `--requirepass-file` and `SEEDSTONE_REQUIREPASS` together are refused;
either beside `--no-auth` is refused. Exit codes: `0` on a clean stop; `1`
when the address could not be bound (after a `bind_failed` line) or the log
under `--data-dir` could not be read (after a `recovery_failed` line); `2` on a
command line the server does not understand (after the usage text, in plain
text, on stderr).

The binary's allocator is mimalloc. It reads its own tuning from the
`MIMALLOC_*` environment variables, none of which this server sets or
needs; the process's resident size grows and is released on mimalloc's
schedule, which is not glibc's — an operator sizing a ceiling against the
machine reads `used_memory` for the keyspace and the process's RSS for the
rest, as before.

## Output

Everything the server writes about itself goes to **stderr**, one JSON object
per line. Stdout carries only the answers to `--version` and `--help`. The
one exception is a refused command line — an unknown flag, a missing value,
a contradiction such as `--no-auth` beside a password — which is answered in
plain text with the usage, and exit code 2: that is the command line talking
back to whoever typed it, before a server exists.

Every line opens with the same three fields, in this order: `ts`, Unix time
in milliseconds; `level`, one of `info`, `warn` or `error`; and `evt`, the
event's name from the table below. The event's own fields follow. Two lines
as the server writes them:

```json
{"ts":1757800000000,"level":"info","evt":"listening","version":"0.1.1","bind":"0.0.0.0","port":6379}
{"ts":1757800000317,"level":"warn","evt":"error_reply","code":"ERR","cmd":"flushall","msg":"ERR unknown command 'flushall'"}
```

| Event | Level | Fields | When |
|---|---|---|---|
| `listening` | `info` | `version`, `bind`, `port` | the listener is bound; `bind` and `port` are what the kernel gave, as `CONFIG GET bind` reports them |
| `bind_failed` | `error` | `bind`, `port`, `error` | the address could not be bound; the process exits 1 after this line |
| `error_reply` | `warn` | `code`, `cmd`, `msg` | one error reply was sent to a client; `code` is the reply's first word, `cmd` the command it answered |
| `stopping` | `info` | `signal` | the server is leaving on `SIGTERM` or `SIGINT` |
| `password_reloaded` | `info` | `passwords` | `SIGHUP` re-read the password file; `passwords` is how many lines it holds now, 1 or 2 |
| `password_reload_failed` | `error` | `error` | `SIGHUP` re-read the password file and refused it; the previous passwords stay in force |
| `password_reload_skipped` | `warn` | — | `SIGHUP` arrived, but the password came from the environment or there is none, so there was nothing to re-read |
| `recovery` | `info` | `segments`, `records`, `applied`, `discarded`, `damage_bytes`, `holes`, `abandoned_segments`, `malformed`, `truncated_shards`, `lossy_shards`, `snapshots_used`, `snapshots_refused`, `files_removed` | the log under `--data-dir` was read on start-up; `applied` records were replayed, `discarded` were cut after a gap, `damage_bytes` were stepped over in `holes` damaged regions, `abandoned_segments` could not be read past a point, `malformed` records were intact but unreadable by this build; `lossy_shards` is how many shards any of that may have cost records — a hole can take a shard's last records without leaving a gap; `snapshots_used` files gave a shard its image, `snapshots_refused` were refused (no footer, counts that did not match, damage inside), `files_removed` files nothing used were removed |
| `recovery_truncated` | `warn` | `shard`, `applied`, `discarded` | one shard's log had a gap: `applied` records before it were replayed, `discarded` after it were not; the node serves what it has |
| `recovery_failed` | `error` | `error` | the log could not be read — the directory cannot be created or listed, or a segment is from a newer version; the process exits 1 after this line |
| `log_fault` | `error` | `shard`, `stage`, `error` | a shard's log could not be written (`stage` `write`) or made durable (`sync`), the executor's snapshot could not be written or made durable (`snapshot`), or a file a durable snapshot made redundant could not be removed (`remove`), on a housekeeping tick; for those two, `shard` is the executor's first shard. A failed write keeps its records and the next tick retries it. A failed sync is retried too, but a filesystem may drop what it could not write and report the retry as a success, so writes acknowledged since the last successful sync may be lost until a snapshot covers them; the next start reads what the disk kept and reports any damage it finds. A failed snapshot write keeps its buffer; a failed removal is retried at the next snapshot and at the next start |
| `snapshot` | `info` | `executor`, `cycle`, `entries`, `bytes`, `ticks`, `disk_bytes` | one executor's snapshot became durable: `entries` keys in `bytes` bytes, taken over `ticks` housekeeping ticks; `disk_bytes` is the whole of `PATH/wal/` at that moment, before the compaction that follows — the directory's peak |
| `compaction` | `info` | `executor`, `files`, `bytes` | one executor removed `files` files, `bytes` bytes, that its durable snapshot made redundant: its own older segments and snapshot, or every older process's files once every executor of this process has a durable snapshot |

`error_reply` is `warn` and not `error` on purpose: an `ERR unknown command`
is the client's mistake or the deployment's, and the server that reported it
is healthy. An alert on `level:error` therefore fires for the server's own
failures and not for a client's.

**What is promised.** The three common fields and the event names above are
stable: removing or renaming one is a breaking change, recorded under
*Changed* in the changelog and released as a minor version while the project
is `0.x`. A new field may appear on any line, and a new event may appear, in
any version, without notice — a consumer selects lines by `evt` and reads
fields by name, and does not assume either list is closed. The text of `msg`
and `error` is for people and is not an interface: it may change without
notice, and nothing should match on it.

## Signals

| Signal | Effect | Writes |
|---|---|---|
| `SIGTERM`, `SIGINT` | The server stops accepting connections and exits when the connections it is serving end. | `stopping`, with the signal's name |
| `SIGHUP` | The password file is re-read; see *Password and rotation*. | `password_reloaded`, `password_reload_failed` or `password_reload_skipped` |

In a container this binary is process 1, and process 1 ignores every signal
it has no handler for; both are handled, so a pod deletion ends in a clean
stop rather than at the end of its grace period.

## Password and rotation

One password protects the `default` user; there are no other users. It
arrives one of three ways:

- **A file**, `--requirepass-file PATH`, holding one password per line, one
  or two lines. A single trailing newline is not part of the last password;
  every other line break separates two passwords. An empty line, a
  whitespace-only line, or a third line is refused, at startup and on reload
  alike. This is the delivery that can be rotated without a restart.
- **The environment**, `SEEDSTONE_REQUIREPASS`, holding one password. It is
  read once: a process does not re-read its environment, and no
  orchestrator updates a running process's. Under this delivery a password
  change is a restart of the server — and a restart of a server started
  without `--data-dir` is the whole cache.
- **None**, with `--no-auth`. See the next section.

**Rotation without a restart.** The server accepts either of two passwords
while the file holds two, so the two sides of a rotation — the server and
its clients — no longer have to change in one moment:

1. Add the new password as the file's second line, and send `SIGHUP` to the
   server (`kill -HUP <pid>`). It writes `password_reloaded` with
   `"passwords":2`. Every client still holds the old password and still
   authenticates.
2. Restart the clients with the new password, in any order and at any pace.
   Both passwords authenticate throughout.
3. Remove the old line, and send `SIGHUP` again. `password_reloaded` with
   `"passwords":1`; the old password is refused from here on.

No server restart, so the keyspace is intact; no moment at which a client
holding either password is refused.

**What a reload does not do.** It does not touch connections that are
already authenticated: authentication is decided when it happens, and a
connection that presented a password later removed stays authenticated until
it closes — as in Redis. It does not accept a file the startup would refuse:
`password_reload_failed` names the reason and the previous passwords stay in
force. It does nothing on a node whose password came from the environment or
which has none: `password_reload_skipped`.

**What is not reported.** `CONFIG GET requirepass` answers an empty value
whether or not a password is set, as Redis does, and `INFO` never carries
one. The password is the one parameter that can change while the server
runs, and the one no command exposes; every parameter `CONFIG GET` does
report is a fact fixed at startup.

## Running without a password

`--no-auth` starts the server with no password, on any address, and says so
in the command line rather than by omission: without it, a bind outside
loopback with no password is refused, so an open server on a network is
always one somebody asked for. `AUTH` against such a server answers
`ERR AUTH <password> called without any password configured for the default
user. Are you sure your configuration is correct?`, as Redis does.

The authentication path — `AUTH`, `HELLO … AUTH`, the gate that refuses
every other command until one of them succeeds, the constant-time comparison,
the two-password set — is covered by the unit tests, the edge tests over a
real socket, and every end-to-end lane but the `redis-cli` one, which runs
open on purpose so that both paths stay exercised. It is not, at the time of
writing, exercised by a deployment the project itself runs; whoever turns
authentication on for a deployment that ran open should know that the path's
production evidence is the test suite's.

## What `--data-dir` promises

The node appends every write to a log under `PATH/wal/` and syncs it on its
housekeeping tick — every 100 ms on a node with room to spare, less often on
one kept busy, since commands are served before housekeeping. A write
acknowledged before a sync survives a crash; one acknowledged after the last
sync may not — nor, once a sync has failed (`log_fault` with `stage` `sync`),
may anything acknowledged since the last one that succeeded, until a
snapshot covers it. On start the log is read back: a shard whose records
have a gap is replayed up to the gap and reported with
`recovery_truncated`, and the node serves what it has.

The node runs one *executor* per available core, each serving a fixed
range of the shards and keeping one log for them; the `snapshot` and
`compaction` lines name it by number. Once an executor's log has grown past
64 MiB — or past the size of its last snapshot, whichever is larger — it
takes a snapshot of its shards: about 1 MiB of it is written per
housekeeping tick while the shards keep serving between ticks, so a
snapshot never holds them for longer than one tick's share takes to write,
and the `snapshot` line says when it is durable. The log it covers is then
removed, on the `compaction` line. What that bounds: **an executor's files
never exceed its last snapshot, plus the one it is writing, plus the larger
of 64 MiB and its last snapshot, plus what was written from the moment
the log crossed that size until the snapshot was durable** — three times
the last snapshot plus 64 MiB, and the writes of that span, on a keyspace
that is not growing. The crossing is noticed on the next housekeeping
tick, so the span starts up to one tick before the snapshot does. The
directory is the sum over the executors.

After a restart, the previous process's files stay until every executor of
the new one has taken a snapshot of its own; an executor that receives
little writing may take a long time to reach 64 MiB, and until it does the
directory holds the previous process's files beside the new one's. A
second restart before then adds a third process's files, and so on: each
process's share is within the bound above, and all of them are removed
together by the first process whose executors all complete a snapshot. A start
reads the newest snapshot of each shard and the log still on disk, so both
the time a start takes and the memory it needs grow with the keyspace plus
that log — which the bound above limits — not with the whole write history.

The bound holds on a disk that eventually writes. A disk that refuses every
write parks the snapshot (`log_fault` with `stage` `snapshot`, retried on
every tick), and the log grows until it accepts one.

One process at a time: the node takes an exclusive lock on `PATH/wal/LOCK`
before it reads the log, and a second node started on the same directory
writes `recovery_failed` and exits 1. The kernel releases the lock when the
process dies, so a crashed node never leaves it behind.

## What `INFO` gives a monitor

`INFO` is the operational surface a monitoring agent reads, and
[compatibility.md](compatibility.md) lists every field. Four matter to
someone watching the server rather than the keyspace:

- `run_id` (section `server`) changes on every start. A monitor computing
  a rate across two values of it is computing a rate across a restart, from
  counters that fell to zero.
- `connected_clients` and `rejected_connections` (`clients`, `stats`): how
  many connections are attached, and how many were told the client ceiling
  was reached and closed. The refusals are counted here and not logged: they
  are as frequent as the load that causes them.
- `errorstats` (a section of its own, not part of `stats`): one
  `errorstat_<CODE>` line per error code answered, with a count — the same
  population as the `error_reply` lines, in aggregate.
- `used_memory` and `maxmemory` (`memory`): the keyspace's size and the
  ceiling it is held under. There is no resident-set figure; this server does
  not read one.
