# SeedStone

An in-memory key-value store, written from scratch in Rust, with optional
on-disk persistence — built for high performance, memory safety, and
correctness that can be demonstrated rather than argued.

## Status: early — libraries, no server

This repository contains a working core and the machinery that tests it. It
does not yet contain anything you can connect to.

What exists, across four crates:

- `seedstone-resp` — a RESP2 codec: an incremental parser and an encoder,
  with the wire-level length and depth limits a server needs at its edge.
- `seedstone-core` — the deterministic core: a keyspace dict with seeded
  hashing and incremental rehashing, a scan cursor stable across table
  growth, a replication-log record format that tolerates holes, and a runtime
  of message-passing shards whose command handlers cannot await. On top of
  those, a RESP service layer generic over its transport.
- `seedstone-sim` — a deterministic simulation harness: the real server and
  real clients over a simulated network and clock, folding every completed
  command into a trace hash that is a function of two seeds and nothing else.
  It runs the core at 1024 shards.
- `seedstone` — the crate the server will live in; today an empty library.

The project is built around deterministic simulation testing, so any
concurrency bug is meant to be reproducible from a seed. CI sweeps a range of
simulator seeds on every change that touches code, and enforces the
determinism rules that make that reproducibility possible — including a
self-test that plants a genuine lost-update race and requires the sweep to
find it and a second process to replay it byte for byte.

There is no server to connect to yet, no persistence, and no benchmarks. The
README will grow as the code does.

SeedStone will speak RESP (the Redis serialization protocol) at its edge, so
existing tools and client libraries can reach it without modification. That
is how the project connects to an ecosystem — not what it is.

## License

Dual-licensed under [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at
your option. Contributions are accepted under the DCO — see
[CONTRIBUTING.md](CONTRIBUTING.md). "SeedStone" is a trademark — see
[TRADEMARK.md](TRADEMARK.md).
