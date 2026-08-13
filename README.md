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

`--bind <addr:port>` and `--max-clients <n>` are the only options.

**What it answers:** `GET`, `SET` (with `EX`, `PX`, `NX`, `XX`), `DEL`,
`EXISTS`, `EXPIRE`, `TTL`, `INCRBY`, `PING`, `ECHO`, `HELLO`, `INFO`,
`COMMAND`, `CLIENT`, `QUIT`. `DEL` and `EXISTS` take several keys. Keys with a
deadline are removed when touched and by a background sweep that does not wait
to be asked.

**What it does not have yet:** persistence — a restart is an empty keyspace —
along with authentication, RESP3, the inline command protocol, replication,
clustering, and every data type except strings. There are no benchmarks
published and no compatibility claim beyond the commands listed above.

## How it is built

- `seedstone` — the edge: the process, its configuration, and the only socket,
  signal handler and entropy draw in the workspace.
- `seedstone-core` — the deterministic core: a keyspace dict with seeded
  hashing, incremental rehashing and a scan cursor stable across table growth;
  a replication-log record format that tolerates holes; a runtime of
  message-passing shards whose command handlers cannot await; and a RESP
  service layer generic over its transport.
- `seedstone-resp` — a RESP2 codec: a resumable decoder that never revisits a
  byte it has consumed, an iterative encoder, and the wire and memory limits a
  server needs at its edge.
- `seedstone-sim` — a deterministic simulation harness: the real server and
  real clients over a simulated network and clock, folding every completed
  command into a trace hash that is a function of two seeds and nothing else.

The project is built around deterministic simulation testing, so a concurrency
bug is meant to be reproducible from a seed. CI sweeps a range of simulator
seeds on every change that touches code, enforces the determinism rules that
make that reproducibility possible — including a self-test that plants a
genuine lost-update race and requires the sweep to find it and a second process
to replay it byte for byte — and finishes by driving the release binary with
`redis-cli`, `redis-benchmark`, redis-py and go-redis.

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) explains the decisions and why
they were made; [docs/coding-guide.md](docs/coding-guide.md) is what a reviewer
reads first.

## License

Dual-licensed under [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at
your option. Contributions are accepted under the DCO — see
[CONTRIBUTING.md](CONTRIBUTING.md). "SeedStone" is a trademark — see
[TRADEMARK.md](TRADEMARK.md).
