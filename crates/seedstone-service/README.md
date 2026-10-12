# seedstone-service

The connection layer of [SeedStone](https://github.com/stefanobaldo/seedstone):
RESP2 frames in, core commands out, replies back. It is netless by
construction and generic over its transport, so the same code runs over a
socket in the server and over a simulated network in tests.

**Who should depend on it: nobody yet.** It is published because the server
is, and `cargo install seedstone` needs it on crates.io. Its API is the
server's internals, versioned with the server and changed whenever the
server needs it; there is no stability promise at `0.x`.

The server that uses it is the `seedstone` crate; its design is in the
repository's `docs/ARCHITECTURE.md`.
