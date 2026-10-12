# seedstone-core

The deterministic core of [SeedStone](https://github.com/stefanobaldo/seedstone):
a keyspace dictionary with seeded hashing and a scan cursor stable across
table growth, a replication-log record format that tolerates holes, and a
runtime of message-passing shards whose command handlers cannot await. It
has no dependency on the wire protocol, by construction.

**Who should depend on it: nobody yet.** It is published because the server
is, and `cargo install seedstone` needs it on crates.io. Its API is the
server's internals, versioned with the server and changed whenever the
server needs it; there is no stability promise at `0.x`.

The server that uses it is the `seedstone` crate; its design is in the
repository's `docs/ARCHITECTURE.md`.
