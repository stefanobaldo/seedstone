# How it is built, and how it is tested

The server is five crates with sharp boundaries, and a test machine that
can replay a concurrency bug from a seed. This page is the account of
both; [ARCHITECTURE.md](ARCHITECTURE.md) is the reasoning behind them.

## The crates

- `seedstone` — the edge: the process, its configuration, and the only
  socket, signal handler and entropy draw in the workspace.
- `seedstone-core` — the deterministic core: a keyspace dict with seeded
  hashing, incremental rehashing and a scan cursor stable across table
  growth; a replication-log record format that tolerates holes; and a
  runtime of message-passing shards whose command handlers cannot await.
  The codec is not among its dependencies, so a handler cannot reach for a
  wire frame — the core's independence from the protocol is a property of
  the build rather than a rule to remember.
- `seedstone-resp` — a RESP2 codec: a resumable decoder that never revisits
  a byte it has consumed, an iterative encoder, and the wire and memory
  limits a server needs at its edge.
- `seedstone-service` — the connection layer: RESP2 frames in, core
  commands out, replies back. Netless by construction — nothing in it opens
  a socket — and generic over its transport, which is what lets the binary
  hand it a real connection and the harness hand it a simulated one, with
  the same code in between.
- `seedstone-sim` — a deterministic simulation harness: the real server and
  real clients over a simulated network and clock, folding every completed
  command into a trace hash that is a function of two seeds and nothing
  else.

## Deterministic simulation

The project is built around deterministic simulation testing, so a
concurrency bug is meant to be reproducible from a seed. CI sweeps a range
of simulator seeds on every change that touches code, and enforces the
determinism rules that make that reproducibility possible.

The machinery is proved on defects planted inside the server itself —
fourteen of them, each a mistake the code could plausibly have made: a
lost-update race; two broken expiry decisions; a keyspace walk that outruns
its own cursor, and one that steps over half the shards; two broken
eviction decisions — a node that never reclaims, and one that reclaims what
it should be keeping; and seven in persistence, from a recovery that stops
at the first damaged record to a writer that calls a write durable before
any sync has covered it. Each has to be caught by the counter that owns it.
The cursor is the one exception, and says so where it is selected: a
simulated table is too small to catch a cursor between steps of its
growth, so that claim is made by a test over the dict itself.

Reproducibility is checked on its own, and on the shapes that carry the
work: the shape the sweep actually runs, the shape under a memory ceiling,
and the one that catches the race all have to replay byte for byte in a
second process, against the hash the first process computed.

A third sweep runs on a hostile disk: writes tear on a crash, writes and
syncs fail, and reads come back corrupted, with the node crashed under load
or at rest. Recovery has to bring back every write the node acknowledged as
durable, or the node has to have reported the loss.

## Real clients, last

CI then drives the release binary with `redis-cli`, `redis-benchmark`,
redis-py and go-redis — every one of them but `redis-cli` against a server
that requires a password, so the authenticated path is the one the gate
exercises and the open one stays exercised too — and then points a third
party's cache-backend test suite at it, run against a digest-verified
archive in a container pinned by digest to the interpreter that client
pair needs. A stock Prometheus exporter scrapes the server last, from a
container pinned the same way: it must log no refused command, and the
metrics it publishes must carry the values this gate predicts.

The same job builds the container image and the Linux packages and makes
each answer where it would be installed, renders the Homebrew formula,
lints the Helm chart, and installs the chart into a cluster — on every
push to `main`, and on a pull request that changes the chart or the
`Dockerfile` — so that the day a release is tagged is never the first
day any of them is built.

## Reproducing a failure

A sweep prints every seed that broke an invariant as a line beginning
`FAIL seed=<S> trace=0x…`, and closes with a summary naming its
`workload_seed` and `shape`. One seed runs again, in a fresh process, with

```sh
cargo run --release -p seedstone-sim --bin replay -- --sim-seed <S> --workload-seed <W>
```

plus `--<shape>` when the summary names one other than `standard`. The
`trace_hash` it prints must match the `trace` the sweep printed, or the
failure is not the one that was found. [coding-guide.md](coding-guide.md)
lists the determinism rules a change must keep.
