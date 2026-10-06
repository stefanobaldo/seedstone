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
          [--no-auth] [--data-dir PATH] [--fsync always|interval|never]
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
| `--fsync always\|interval\|never` | `interval` | When the log is synced: `always` before a write is acknowledged, `interval` once 100 ms have passed since the last sync, while there is anything to sync, `never` only when the log rotates to its next file and at a clean stop. Only with `--data-dir`. |

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
| `error_reply` | `warn` | `code`, `cmd`, `msg` | one error reply was sent to a client; `code` is the reply's first word, `cmd` the command it answered. A write refused because the log failed writes no line: there is one per write under load, and `log_fault` and `refusal_ended` say what happened |
| `stopping` | `info` | `signal` | the server is leaving on `SIGTERM` or `SIGINT`, or on a client's `SHUTDOWN` (`signal` is then `SHUTDOWN`) |
| `shutdown_timeout` | `warn` | — | the stop that followed `stopping` gave up waiting for the log to be synced, after one second; the process exits anyway, and what was written since the last sync may not be on disk |
| `password_reloaded` | `info` | `passwords` | `SIGHUP` re-read the password file; `passwords` is how many lines it holds now, 1 or 2 |
| `password_reload_failed` | `error` | `error` | `SIGHUP` re-read the password file and refused it; the previous passwords stay in force |
| `password_reload_skipped` | `warn` | — | `SIGHUP` arrived, but the password came from the environment or there is none, so there was nothing to re-read |
| `recovery` | `info` | `segments`, `records`, `applied`, `discarded`, `damage_bytes`, `holes`, `abandoned_segments`, `malformed`, `truncated_shards`, `lossy_shards`, `snapshots_used`, `snapshots_refused`, `files_removed` | the log under `--data-dir` was read on start-up; `applied` records were replayed, `discarded` were cut after a gap, `damage_bytes` were stepped over in `holes` damaged regions, `abandoned_segments` could not be read past a point, `malformed` records were intact but unreadable by this build; `lossy_shards` is how many shards any of that may have cost records — a hole can take a shard's last records without leaving a gap; `snapshots_used` files gave a shard its image, `snapshots_refused` were refused (no footer, counts that did not match, damage inside), `files_removed` files nothing used were removed |
| `recovery_truncated` | `warn` | `shard`, `applied`, `discarded` | one shard's log had a gap: `applied` records before it were replayed, `discarded` after it were not; the node serves what it has |
| `recovery_failed` | `error` | `error` | the log could not be read — the directory cannot be created, listed or locked (another node holds it), or a segment or snapshot was written by a build whose layout differs, newer or earlier; the process exits 1 after this line |
| `log_fault` | `error` | `stage`, `error` | the node's log could not be written (`stage` `write`), made durable (`sync`) or rotated to its next file (`rotate`), or a file a durable snapshot made redundant could not be removed (`remove`). After a failed write, sync or rotation every executor refuses writes, serves reads, and takes a snapshot of its memory; each serves writes again once its own snapshot is durable, on `refusal_ended` — see *What `--data-dir` promises*. A failure that persists is not a line per retry: it is one again when it refuses an executor that had served writes again since. A failed removal is retried at the next snapshot and at the next start. The next start reads what the disk kept and reports any damage it finds |
| `snapshot_fault` | `error` | `shard`, `error` | the snapshot of the executor whose first shard is `shard` could not be written or made durable; it starts over in a new file on a later housekeeping tick |
| `sync_slow` | `warn` | `round`, `in_flight_ms` | a sync of the log has been in flight for more than one second; under `--fsync always` every write on the node is waiting for it. One line per such sync, not per tick |
| `sync_slow_ended` | `info` | `round`, `outcome`, `duration_ms` | the sync `sync_slow` warned about ended, `ok` or `failed`, after `duration_ms`; a failure also writes `log_fault` |
| `refusal_ended` | `info` | `shard`, `refused`, `ticks` | the executor whose first shard is `shard` serves writes again: it refused them after the node's log could not be written, synced or rotated (`log_fault` came first), and a snapshot of its memory taken after the failure is durable; `refused` writes were refused over `ticks` housekeeping ticks |
| `snapshot` | `info` | `executor`, `cycle`, `entries`, `bytes`, `ticks`, `disk_bytes`, `bytes_written`, `cleared` | one executor's snapshot became durable: `entries` keys in `bytes` bytes, taken over `ticks` housekeeping ticks; `disk_bytes` is the whole of `PATH/wal/` at that moment, before the compaction that follows — the directory's peak; `bytes_written` is what the executor appended to the log while the cycle ran, the span the bound in *What `--data-dir` promises* names; `cleared` is how many of its shards had been reported lossy by `recovery` and are covered by this image, so their durable state no longer depends on the damaged region |
| `compaction` | `info` | `files`, `bytes` | the node removed `files` files, `bytes` bytes, that durable snapshots made redundant: segments every executor has covered, snapshots a newer one of the same executor superseded, or every older process's files once every executor of this process has a durable snapshot |
| `fsync_ignored` | `warn` | — | `--fsync` was given without `--data-dir`; there is no log to sync, and the setting does nothing |

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
| `SIGTERM`, `SIGINT` | The server stops accepting connections, answers what its executors had queued, syncs the log, and exits. | `stopping`, with the signal's name; then `shutdown_timeout` if the disk did not answer in time |
| `SHUTDOWN` (a client command) | The same stop as `SIGTERM`, asked for over the wire, after the authentication gate. `NOSAVE` and `SAVE` are accepted and change nothing, because the stop syncs the log anyway. Under `--no-auth` any client that can connect can stop the server, as any client can stop a 6.2.24 or 8.10.1 with no password set; with one, an unauthenticated `SHUTDOWN` is `NOAUTH` on all three. | `stopping`, with `SHUTDOWN` as the signal |
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

The node appends every write to a log under `PATH/wal/`; `--fsync` says
when the log is synced, and so what a crash can cost.

- **`always`**: a write is acknowledged only once a sync covering its record
  has completed, so every acknowledged write survives a crash. The node
  issues one sync at a time, from one writer every executor feeds, and each
  covers every write that was ready when it was issued. A read waits for one only when its
  connection pipelined it together with a write, whose reply goes out in
  the order it was sent, or when it finds its key expired and deletes it,
  which is a write; a read on any other connection is answered at once.
- **`interval`** (the default): a write is acknowledged at once, and the log
  is synced once 100 ms have passed since the last sync, whenever there is
  something to sync and no sync is in flight — on a busy node right after a
  write, on an idle one from the writer's tick. A crash costs what was
  acknowledged since the last sync that completed. A process that is
  killed, as opposed to a machine that loses power, can also cost what an
  executor had handed to the writer in the last microseconds and the writer
  had not yet written.
- **`never`**: the node issues no sync of its own. The kernel writes the log
  when it will; the writer syncs a file when it rotates to the next one. A
  crash keeps the last durable snapshot, plus whatever of the log the kernel
  had written. A process that is killed, as opposed to a machine that loses
  power, can also cost what an executor had handed to the writer in the last
  microseconds and the writer had not yet written.

Under every setting what survives of a shard is a prefix of what was
acknowledged on it, and a clean stop (`SIGTERM`, `SIGINT`) syncs everything
before the process ends, so a rollout never loses what only a crash would —
except on an executor still refusing writes after the log failed, whose
acknowledged writes only the snapshot it was taking could have covered.
`--fsync` without `--data-dir` is accepted, logged as `fsync_ignored`, and
does nothing. On start the log is read back: a shard whose records have a
gap is replayed up to the gap and reported with `recovery_truncated`, and
the node serves what it has.

**When the disk fails.** A write, a sync or a rotation of the log that
fails (`log_fault`) puts the node into refusal: every write is answered
`MISCONF Errors writing to the log: writes are refused until a snapshot is
durable` (the shape of the reply Redis 6.2.24 and 8.10.1 give a failed AOF
write under `appendfsync everysec` — [compatibility.md](compatibility.md)
has the reading), reads are served, and each executor takes a snapshot of
its shards' memory. The executors return one at a time: each serves writes
again once its own snapshot is durable, on its `refusal_ended` line, so a
node of ten executors is back to nine tenths of its writes as soon as nine
snapshots have landed, without waiting for the tenth; the first write after
the failure moves the log to a fresh file. Under `interval` and `never`,
writes acknowledged between the last good sync and the failure are in
memory, and the snapshots cover them; what a crash before they land would
lose is what the setting already allowed. Under `always`, the writes of a
batch whose sync failed are answered with the refusal, and a read in the
same batch is answered as usual. Those writes were applied without being
acknowledged, exactly as a client sees a dropped connection, and the
snapshot makes them durable: a client that retries a `SET` gets the same
value, and one that retries an `INCRBY` counts twice, as after any lost
reply. A disk with no space fails the snapshots too (`snapshot_fault`,
every tick), and the executors keep refusing until space is freed; a start
whose first write to the log fails begins refusing writes the same way. A
full disk asks for space, not for a restart.

The node runs one *executor* per available core, each serving a fixed
range of the shards; one writer appends every executor's records to the
node's log and syncs it. The `snapshot` line names the executor by number.
Once an executor has written 64 MiB to the log since its last snapshot — or
more than the size of that snapshot, whichever is larger — it takes a
snapshot of its shards: about 1 MiB of it is written per housekeeping tick
while the shards keep serving between ticks, so a snapshot never holds them
for longer than one tick's share takes to write, and the `snapshot` line
says when it is durable. The crossing is noticed on the next housekeeping
tick, so the span starts up to one tick before the snapshot does. The log
rotates into a new file every 64 MiB, and a file is removed, on the
`compaction` line, once every executor's durable snapshot covers what it
wrote there. An executor that writes little would keep such files alive
indefinitely; so the node also asks an executor for a snapshot when the log it
holds back from removal exceeds the same 64 MiB-or-its-last-snapshot bound.
What that bounds: **the node's files never exceed the sum of the last
snapshots, plus the ones being written, plus — per executor — the larger of
64 MiB and its last snapshot, plus what was written from each log crossing
its size until its snapshot was durable, plus one 64 MiB file** of
granularity. On a keyspace that is not growing, about three times the
snapshots plus 64 MiB per executor plus 64 MiB, and the writes of those
spans.

After a restart, the previous process's files stay until the node has
replaced them with snapshots of its own. They count as retained log, so the
same rule applies: once they exceed the bound on an executor's account that
executor snapshots, and when every executor of the new process has a
durable snapshot the previous process's files are removed together. A start
reads the newest snapshot of each shard and the log still on disk, so both
the time a start takes and the memory it needs grow with the keyspace plus
that log — which the bound above limits — not with the whole write history.
A directory written by an earlier build whose layout differs is refused at
start, with `recovery_failed` saying so.

The bound holds on a disk that eventually writes. A disk that refuses every
snapshot parks it (`snapshot_fault`, retried on every tick), and the log
grows until one lands.

One process at a time: the node takes an exclusive lock on `PATH/wal/LOCK`
before it reads the log, and a second node started on the same directory
writes `recovery_failed` and exits 1. The kernel releases the lock when the
process dies, so a crashed node never leaves it behind.

## What `INFO persistence` reports

`INFO persistence` is what a monitor reads about the log and the snapshots.
A node without `--data-dir` prints `loading:0` and `aof_enabled:0` and
nothing else: there is no log, and the fields that would describe one are
absent rather than zero. With `--data-dir` the section carries the fields
below, in this order. The first ones use Redis's names, where this node
measures what the name says. The ones from `fsync_policy` on are this
server's own. The section is part of an argumentless `INFO`, as it is on
Redis (read on 6.2.24 and 8.10.1).

| Field | Value | What it measures |
|---|---|---|
| `loading` | `0` | the node reads its log before it listens, so a client never sees it loading |
| `rdb_changes_since_last_save` | count | records appended to the log that no durable snapshot covers yet, summed over the executors |
| `rdb_bgsave_in_progress` | `0` or `1` | `1` while any executor's snapshot is being written; the shards keep serving while it is |
| `rdb_last_save_time` | Unix seconds | when the oldest of the shards' newest durable snapshots was taken: everything written before it is covered by a snapshot. Absent until every shard has one. A snapshot read back at start counts, so a restart does not reset it |
| `rdb_last_bgsave_status` | `ok` or `err` | `err` when the last snapshot of any executor ended in `snapshot_fault` |
| `rdb_last_bgsave_time_sec` | seconds | how long the last completed snapshot took, the longest over the executors, counted in housekeeping ticks of 100 ms and rounded down; `-1` before any |
| `rdb_saves` | count | snapshots that became durable since the process started, summed over the executors |
| `rdb_last_load_keys_loaded` | count | keys the start recovered, from snapshots and from the log |
| `aof_enabled` | `1` | the log is on |
| `aof_last_write_status` | `ok` or `err` | `err` while any executor refuses writes because the log failed (see `log_fault`) |
| `aof_current_size` | bytes | the log's files on disk, every segment |
| `aof_pending_bio_fsync` | `0` or `1` | `1` while a sync of the log is in flight; there is one writer and one sync at a time |
| `aof_delayed_fsync` | count | syncs that were in flight for more than one second, since the start: each wrote a `sync_slow` line |
| `fsync_policy` | `always`, `interval` or `never` | the `--fsync` setting |
| `log_segments` | count | the log's segment files on disk |
| `syncs_total` | count | syncs of the log completed since the start |
| `last_sync_ms` | milliseconds | how long the last completed sync took; absent until the first one |
| `refusing_executors` | count | executors refusing writes right now |
| `lossy_shards` | count | shards the start reported lossy (the `recovery` line's `lossy_shards`) that no durable snapshot has covered since; it only falls within a process, and each snapshot that covers one says so with its `cleared` field |

The rest of Redis's section is absent. There is no fork and no rewrite, so
there are no copy-on-write, fork or rewrite fields. A snapshot's duration is
reported when it ends, and the open snapshots of several executors have no
single duration, so `rdb_current_bgsave_time_sec` is absent. Nothing here
measures what `aof_base_size`, `aof_buffer_length` and `mem_aof_buffer` name.
The start does not count the keys it found expired, so
`rdb_last_load_keys_expired` is absent. A number this server does not measure
is absent here rather than zero.

## Asking for a snapshot, and stopping from a client

`BGSAVE` asks every executor for a snapshot of its shards now and answers
`Background saving started`; the shards keep serving while the images are
written, a slice per housekeeping tick, and each executor's `snapshot` line
says when its image is durable. `SAVE` asks for the same and answers `OK`
only once an image taken after it is durable on every executor — behind a
running snapshot it waits for the next one — so a `SAVE` that returned is a
keyspace that survives a crash under any `--fsync` setting. `LASTSAVE` is the
instant since which every shard has had a durable image, in Unix seconds, `0`
until then. All three answer an error naming `--data-dir` on a node started
without it.

`SHUTDOWN` is the stop `SIGTERM` asks for, asked for over the wire: the
server writes `stopping` with `SHUTDOWN` as the signal, stops accepting
connections, answers what its executors had queued — the writes pipelined
ahead of `SHUTDOWN` on its own connection among them — syncs the log, and
exits 0.
`NOSAVE` and `SAVE` are accepted and ignored, since the stop syncs the log
either way; send `SAVE` first for an image. **Under `--no-auth` any client
that can connect can stop the server** — as any client can stop a 6.2.24 or
8.10.1 with no password set; a node reachable from a network should carry a
password.

## What `INFO` gives a monitor

`INFO` is the operational surface a monitoring agent reads, and
[compatibility.md](compatibility.md) lists every field. Five matter to
someone watching the server rather than the keyspace:

- `run_id` (section `server`) changes on every start. A monitor computing
  a rate across two values of it is computing a rate across a restart, from
  counters that fell to zero.
- `connected_clients` and `rejected_connections` (`clients`, `stats`): how
  many connections are attached, and how many were told the client ceiling
  was reached and closed. The refusals are counted here and not logged: they
  are as frequent as the load that causes them.
- `errorstats` (a section of its own, not part of `stats`): one
  `errorstat_<CODE>` line per error code answered, with a count — the
  `error_reply` lines in aggregate, plus the writes refused because the log
  failed, which are counted here under `MISCONF` and not logged one by one.
- `used_memory` and `maxmemory` (`memory`): the keyspace's size and the
  ceiling it is held under. There is no resident-set figure; this server does
  not read one.
- `persistence`: the log's size, whether a sync is in flight or has been
  slow, how many executors refuse, when every shard was last imaged; the
  section above lists every field.
