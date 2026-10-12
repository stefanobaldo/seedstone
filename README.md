<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/stefanobaldo/seedstone/main/docs/assets/wordmark-dark.svg">
    <img alt="SeedStone" src="https://raw.githubusercontent.com/stefanobaldo/seedstone/main/docs/assets/wordmark-light.svg" width="340">
  </picture>
</p>

<p align="center">
An in-memory key-value store, written from scratch in Rust, that speaks the
Redis protocol and keeps its data on disk when asked to — with correctness
that is demonstrated rather than argued.
</p>

<p align="center">
  <a href="https://github.com/stefanobaldo/seedstone/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/stefanobaldo/seedstone/actions/workflows/ci.yml/badge.svg?branch=main"></a>
  <a href="https://github.com/stefanobaldo/seedstone/releases/latest"><img alt="Release" src="https://img.shields.io/github/v/release/stefanobaldo/seedstone?display_name=tag&label=release"></a>
  <a href="https://crates.io/crates/seedstone"><img alt="crates.io" src="https://img.shields.io/crates/v/seedstone"></a>
  <a href="https://github.com/stefanobaldo/seedstone/pkgs/container/seedstone"><img alt="Container image" src="https://img.shields.io/docker/image-size/seedstone/seedstone/latest?label=container"></a>
  <a href="LICENSE-MIT"><img alt="License: MIT OR Apache-2.0" src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue"></a>
  <img alt="Rust 1.97.1" src="https://img.shields.io/badge/rust-1.97.1-orange">
</p>

## Why

- **Correctness you can replay.** The whole server runs inside a
  deterministic simulation — a virtual clock, a simulated network,
  scheduled clients — and every completed command is folded into a trace
  hash that is a function of two seeds and nothing else. A concurrency bug
  is reproduced from its seed, in a fresh process, byte for byte. CI sweeps
  seeds on every code change and proves the machinery on fourteen defects
  planted in the server itself. → [docs/testing.md](docs/testing.md)
- **Measured against the engines it stands beside, with the method next to
  the number.** Redis, Valkey, Dragonfly and Garnet on the same machine
  under the same harness, raw logs committed, and where this server is
  behind, the page says so. → [docs/benchmarks.md](docs/benchmarks.md)
- **Built to be operated.** `INFO` in sections that carry only what the
  node can state truthfully; one JSON line per event; a password that
  rotates without a restart; a disk that fails or fills met with refusal,
  not with acknowledgements the node cannot keep. A stock Prometheus
  exporter scrapes it in CI. → [docs/operations.md](docs/operations.md)

## Quick start

```sh
# Container (also on GHCR as ghcr.io/stefanobaldo/seedstone)
docker run -p 6379:6379 seedstone/seedstone --bind 0.0.0.0:6379 --no-auth

# macOS and Linux
brew install seedstonelabs/tap/seedstone

# Cargo: the release archive in seconds, or from source
cargo binstall seedstone
cargo install seedstone

# Debian, Ubuntu / Fedora, RHEL — a package with a systemd unit, from the release page
sudo apt install ./seedstone_0.3.3-1_amd64.deb
sudo dnf install ./seedstone-0.3.3-1.x86_64.rpm

# Kubernetes
helm install seedstone oci://ghcr.io/stefanobaldo/charts/seedstone --version 0.3.3 --set auth.noAuth=true
```

Every channel, with checksums, the packaged defaults and the service
units, is on [docs/install.md](docs/install.md). Then, with any client:

```console
$ redis-cli
127.0.0.1:6379> set greeting hello ex 60
OK
127.0.0.1:6379> ttl greeting
(integer) 60
127.0.0.1:6379> get greeting
"hello"
```

A server started with no arguments listens on loopback with no password.
`--bind`, `--maxmemory`, `--requirepass-file`, `--data-dir` and the rest
are on [docs/operations.md](docs/operations.md).

## Benchmarks

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/stefanobaldo/seedstone/main/docs/assets/benchmarks-dark.svg">
    <img alt="Throughput of seedstone against Redis, Valkey, Dragonfly and Garnet on four cells" src="https://raw.githubusercontent.com/stefanobaldo/seedstone/main/docs/assets/benchmarks.svg">
  </picture>
</p>

Four of the cells, throughput only, rendered from the raw logs by the
harness itself: `v0.3.2` on a GCP `c4a-standard-16`, against Redis 8.10.0,
Valkey 9.1.1, Dragonfly `df-v1.40.2` and Garnet 2.1.5. The page carries
every cell at every pipeline depth, the CPU per operation — where this
server spends more than Redis at one I/O thread on every cell measured —
the method fixed before the run, and what the numbers do not say.
→ [docs/benchmarks.md](docs/benchmarks.md)

## What it speaks

RESP2 over TCP, so `redis-cli` and the ordinary client libraries connect
unmodified. The surface is a named list, chosen for the workloads this
server targets:

| | commands |
|---|---|
| strings | `GET` `SET` `SETEX` `SETNX` `PSETEX` `MGET` `INCRBY` `STRLEN` `TYPE` |
| keys and expiry | `DEL` `EXISTS` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `PERSIST` `TTL` `PTTL` `SCAN` `KEYS` `DBSIZE` `FLUSHDB` |
| server | `PING` `ECHO` `AUTH` `HELLO` `INFO` `CONFIG` `SLOWLOG` `LATENCY` `COMMAND` `CLIENT` `QUIT` |
| persistence | `SAVE` `BGSAVE` `LASTSAVE` `SHUTDOWN` |

`SET` takes `EX`, `PX`, `EXAT`, `PXAT`, `NX`, `XX`, `KEEPTTL` and `GET`;
`DEL`, `EXISTS` and `MGET` take several keys. Anything outside the list is
refused with an error naming the command, never answered approximately.
Every option, every refusal and every deliberate difference from Redis:
[docs/compatibility.md](docs/compatibility.md).

## Persistence

`--data-dir PATH` appends every write to a log and recovers the keyspace on
the next start from each shard's newest snapshot plus the log since it.
`--fsync always|interval|never` says when the log is synced and so what a
crash can cost; snapshots are taken without stopping the shards and keep
the directory within a stated bound. A disk that fails or fills is met
with refusal, and `INFO persistence` is what a monitor reads about it.
→ [docs/operations.md](docs/operations.md#what---data-dir-promises)

## Status

`0.x`: the keyspace holds strings, and this server has been a Django
application's production cache since August 2026, without issues — 88
million commands in its first week, 147 clients at peak. Not here yet:
other data types, RESP3, replication, clustering, transactions, scripting.
Next: hashes, lists, sets and sorted sets, Pub/Sub, RESP3. Not modelled at
all: users beside the one password, and TLS — transport security belongs
to the deployment, in front of the node.
[CHANGELOG.md](CHANGELOG.md) is what changed; the current release is
`v0.3.3`.

## Documentation

- [docs/install.md](docs/install.md) — every way to install, and the packaged defaults
- [docs/operations.md](docs/operations.md) — flags, output, signals, password, persistence, `INFO`
- [docs/compatibility.md](docs/compatibility.md) — what it answers, refuses, and where it differs on purpose
- [docs/benchmarks.md](docs/benchmarks.md) — the measurements, the method, the raw logs
- [docs/testing.md](docs/testing.md) — the crates, deterministic simulation, the client lanes
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — the decisions and why
- [docs/coding-guide.md](docs/coding-guide.md) — what a reviewer reads first
- [docs/RELEASING.md](docs/RELEASING.md) — how a version is cut

## Contributing

Issues and questions are welcome; open one before a large change so we can
talk first. Commits carry a `Signed-off-by` line (DCO) and follow
Conventional Commits — [CONTRIBUTING.md](CONTRIBUTING.md) has the ground
rules.

## License

Dual-licensed under [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at
your option. "SeedStone" is a trademark — see [TRADEMARK.md](TRADEMARK.md).
