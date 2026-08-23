# SeedStone

An in-memory key-value store, written from scratch in Rust, with optional
on-disk persistence — built for high performance, memory safety, and
correctness that can be demonstrated rather than argued.

## Status: early — a server you can connect to

There is a server now. It speaks RESP2 over TCP, so `redis-cli` and the
ordinary client libraries reach it without modification:

```console
$ cargo run --release -p seedstone
seedstone 0.0.0 listening on 127.0.0.1:6379

$ redis-cli
127.0.0.1:6379> set greeting hello ex 60
OK
127.0.0.1:6379> ttl greeting
(integer) 60
127.0.0.1:6379> get greeting
"hello"
```

`--bind <addr:port>`, `--max-clients <n>`, `--maxmemory <size>`,
`--maxmemory-policy allkeys-lru|noeviction`, `--requirepass-file <path>` and
`--no-auth` are the only options a server takes; `--version` and `--help`
answer and exit, in first position and nowhere else. The password may also
arrive in `SEEDSTONE_REQUIREPASS`. It is never an argument — a command line is
readable by every other process on the host. A bind outside loopback
refuses to start without a password from either source, unless `--no-auth`
says so deliberately.

**What it answers:** `GET`, `SET` (with `EX`, `PX`, `EXAT`, `PXAT`, `NX`, `XX`,
`KEEPTTL`, `GET`), `MGET`, `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `PERSIST`,
`TTL`, `TYPE`, `STRLEN`, `INCRBY`, `SCAN`, `KEYS`, `DBSIZE`, `FLUSHDB`,
`PING`, `ECHO`, `AUTH`, `HELLO`, `INFO`, `CONFIG GET`, `SLOWLOG`, `LATENCY`,
`COMMAND`, `CLIENT`, `QUIT`. `DEL`, `EXISTS` and `MGET` take several keys. Keys
with a deadline are removed when touched and by a background sweep that does
not wait to be asked. With `--maxmemory`, the keyspace is held under a ceiling
by evicting least-recently-used keys, or by refusing writes under
`noeviction`.

`INFO`, `CONFIG GET`, `SLOWLOG` and `LATENCY` are the operational surface a
monitoring agent reads: `INFO` in sections — `server`, `clients`, `memory`,
`stats`, `keyspace` and `commandstats`, carrying only fields this node can
state truthfully — `CONFIG GET` over the parameters that describe how it was
started, and `SLOWLOG` and `LATENCY` answering as the switched-off monitors
they are, so that a scrape completes rather than logging a refusal on every
pass.

**What it does not have yet:** persistence — a restart is an empty keyspace —
along with RESP3, replication, clustering, and every data type except strings.
There are no benchmarks published.

**What it deliberately does not answer:** the inline command protocol, server-
side scripting, transactions, and `CONFIG SET` — every parameter `CONFIG GET`
reports is a fact about a node that is already running, and accepting a new
ceiling at runtime would mean moving a keyspace under one. The surface is a
named list chosen for the workloads this project targets; anything outside it
is refused with an error naming the command, rather than answered
approximately. The same line is drawn outside the command set: authentication
is one password for the `default` user, with no ACL users beside it — access
control this server does not model. Nor does it terminate TLS: transport
security belongs to the deployment, in front of the node.

**Releases:** a tag publishes a GitHub Release carrying an x86_64 Linux binary
and its `sha256`. [CHANGELOG.md](CHANGELOG.md) is what changed;
[docs/RELEASING.md](docs/RELEASING.md) is how a version is cut. There is no tag
yet — for now the server is built from source, as above.

## How it is built

- `seedstone` — the edge: the process, its configuration, and the only socket,
  signal handler and entropy draw in the workspace.
- `seedstone-core` — the deterministic core: a keyspace dict with seeded
  hashing, incremental rehashing and a scan cursor stable across table growth;
  a replication-log record format that tolerates holes; and a runtime of
  message-passing shards whose command handlers cannot await. The codec is not
  among its dependencies, so a handler cannot reach for a wire frame — the
  core's independence from the protocol is a property of the build rather than
  a rule to remember.
- `seedstone-resp` — a RESP2 codec: a resumable decoder that never revisits a
  byte it has consumed, an iterative encoder, and the wire and memory limits a
  server needs at its edge.
- `seedstone-service` — the connection layer: RESP2 frames in, core commands
  out, replies back. Netless by construction — nothing in it opens a socket —
  and generic over its transport, which is what lets the binary hand it a real
  connection and the harness hand it a simulated one, with the same code in
  between.
- `seedstone-sim` — a deterministic simulation harness: the real server and
  real clients over a simulated network and clock, folding every completed
  command into a trace hash that is a function of two seeds and nothing else.

The project is built around deterministic simulation testing, so a concurrency
bug is meant to be reproducible from a seed. CI sweeps a range of simulator
seeds on every change that touches code, enforces the determinism rules that
make that reproducibility possible, and runs a self-test that plants six
genuine defects inside the server itself: a lost-update race, two broken expiry
decisions, a keyspace walk that outruns its own cursor, and two broken eviction
decisions — a node that never reclaims, and one that reclaims what it should be
keeping. Each has to be caught by the counter that owns it, and what the sweep
runs has to replay byte for byte in a second process. CI then drives the
release binary with `redis-cli`, `redis-benchmark`, redis-py and go-redis —
every one of them but `redis-cli` against a server that requires a password,
so the authenticated path is the one the gate exercises and the open one stays
exercised too — and then points a third party's cache-backend test suite at
it, run against a digest-verified archive in a container pinned by digest to
the interpreter that client pair needs. A stock Prometheus exporter scrapes
the server last, from a container pinned the same way: it must log no refused
command, and the metrics it publishes must carry the values this gate
predicts.

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) explains the decisions and why
they were made; [docs/coding-guide.md](docs/coding-guide.md) is what a reviewer
reads first.

## License

Dual-licensed under [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at
your option. Contributions are accepted under the DCO — see
[CONTRIBUTING.md](CONTRIBUTING.md). "SeedStone" is a trademark — see
[TRADEMARK.md](TRADEMARK.md).
