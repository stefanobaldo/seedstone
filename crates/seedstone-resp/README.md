# seedstone-resp

The RESP2 codec of [SeedStone](https://github.com/stefanobaldo/seedstone):
a resumable decoder that never revisits a byte it has consumed, an
iterative encoder, and the wire and memory limits a server needs at its
edge.

**Who should depend on it: nobody yet.** It is published because the server
is, and `cargo install seedstone` needs it on crates.io. Its API is the
server's internals, versioned with the server and changed whenever the
server needs it; there is no stability promise at `0.x`.

The server that uses it is the `seedstone` crate; its design is in the
repository's `docs/ARCHITECTURE.md`.
