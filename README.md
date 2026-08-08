# SeedStone

An in-memory key-value store, written from scratch in Rust, with optional
on-disk persistence — built for high performance, memory safety, and
correctness that can be demonstrated rather than argued.

## Status: bootstrap

This repository currently contains the project's foundation, not a server:

- a Cargo workspace with a placeholder crate,
- a CI gate that enforces determinism rules from the first commit — the
  project is built around deterministic simulation testing (DST), so any
  concurrency bug is meant to be reproducible from a seed, and the rules that
  make that possible are machine-enforced before any code exists to break
  them,
- the license, trademark, and contribution ground rules.

There is no storage engine, no network protocol, and no benchmark here yet.
The README will grow as the code does.

SeedStone will speak RESP (the Redis serialization protocol) at its edge, so
existing tools and client libraries can reach it without modification. That
is how the project connects to an ecosystem — not what it is.

## License

Dual-licensed under [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at
your option. Contributions are accepted under the DCO — see
[CONTRIBUTING.md](CONTRIBUTING.md). "SeedStone" is a trademark — see
[TRADEMARK.md](TRADEMARK.md).
