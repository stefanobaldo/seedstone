# Architecture

This document describes the decisions that shape SeedStone and the reasoning
behind them. It is about structure, not about a feature list; what the server
currently answers is in the [README](../README.md).

## Why determinism is the architecture

Concurrent correctness is not something a reader can establish by reading a
diff. A change to a sharded, message-passing datastore is correct or incorrect
depending on interleavings that the source does not display: what happens when
a rehash is halfway through and a client disconnects, when a timer fires
between a read and the write that depends on it, when two commands for the same
key arrive from different connections in the order nobody tested. A test suite
that runs those paths in whatever order the operating system chose that morning
does not verify them — it samples them, and reports the sample as a pass.

Deterministic simulation testing replaces the sample with a controlled
experiment. The whole server runs inside a simulated world: a virtual clock the
harness advances, a network the harness delivers on, clients the harness
schedules. Every completed command is folded into a trace hash. That hash is a
function of the seeds and nothing else, so a run is a reproducible object: sweep
a range of seeds to find a failure, then replay the failing seed — in a fresh
process, on another machine, six months later — and get the same failure, byte
for byte. A fix is proven by re-running the seed that exposed the bug.

That property is fragile in one direction only: anything that reads entropy,
wall-clock time, or an unspecified iteration order silently makes the trace a
function of something the harness does not control, and the bug that gets
found stops being the bug that gets reproduced. So determinism is not a testing
technique layered on top of the system. It is a constraint on every line of it,
and it cannot be retrofitted — which is why the seams below exist from the
first commit rather than from the release that needs them.

## Shard-per-core, shared-nothing

The keyspace is divided into 1024 **virtual shards**. A shard is a dictionary
nothing else can reach, its replication position, and its log. Shards are
hosted by a smaller number of **executor** tasks — one per available core — and
an executor is the only thing that ever touches the state of the shards it
owns. Work arrives as a batch of `(shard, command)` pairs on an unbounded
inbox, and an executor answers batches one at a time, in arrival order. Nothing
is shared, so nothing is locked.

Separating the two counts is deliberate. The shard count is a placement
decision: which shard a key belongs to is a property of the deployment, and a
key never moves between shards. The executor count follows the machine. A large
fixed number of virtual shards allows rebalancing across cores without
rehashing the keyspace, and it aligns the internal unit of ownership with the
unit a cluster protocol would later distribute.

**A command handler is a plain `fn`, never `async`.** It takes `&mut Dict` and
returns a reply. A handler that cannot `await` cannot yield the executor
mid-command, so a command has either not started or finished, and two commands
on one key can never interleave. Per-key atomicity is a consequence of the
signature rather than a rule reviewers have to remember — and the space of
interleavings the simulator has to explore is the space of *message orders*,
which is enumerable, rather than the space of thread schedules, which is not.

The model has a cost worth stating: a single key carrying a large share of the
traffic saturates the one executor that owns it while the others stay idle.
That is a property of shard-per-core, not a defect to be fixed later.

## The seams

A seam is a boundary where the simulator substitutes its own implementation.
The rule is to build one only where the runtime does not already provide a type
that works in both worlds — an abstraction the simulator does not need is cost
with no benefit.

- **The clock and the network come from the runtime.** The simulator
  substitutes `tokio::time` wholesale and provides TCP types implementing the
  same traits as the production ones, so the code names one API and the
  production build gets the real one. The service layer is generic over its
  transport for exactly this reason, and the same `serve_connection` runs under
  a real socket and a simulated one.
- **The filesystem seam is ours.** The simulated file type resolves its host
  through a context that does not exist outside a simulation, so production and
  simulation cannot be one type and something must dispatch. A narrow trait
  over the log's own operations is enough.
- **The trace is ours by definition.** A production server compares its
  execution against nothing; the trace exists so a simulated one can.

**Entropy enters in exactly one place.** The hash seed is drawn in `main`, the
composition root, and injected downward; nothing below it reads randomness or
wall-clock time. Deadlines are absolute monotonic instants, never wall-clock
timestamps, so a clock the harness controls is the only clock in the system.

**The replication log exists now, as a no-op.** It is the abstraction that
becomes a write-ahead log when persistence arrives and a consensus log after
that. Every mutation is recorded before it is applied, at a gapless position,
whether or not anything is listening. Adding that ordering later would mean
touching every handler; having it now costs a function call.

## The edge is an adapter

SeedStone speaks RESP2 over TCP. That is a boundary decision, not an identity:
it means existing tools and client libraries can reach the server without
modification, and it is deliberately the thinnest part of the system.

The binary is the only place a socket is opened, a signal is handled, or
entropy is drawn. Below it, the connection loop is generic over its transport;
commands that concern the connection rather than the keyspace are answered
there, and keyed commands are routed to the executor that owns their shard.
Multi-key commands fan out from the same layer.

The codec is a resumable decoder rather than a parser over a complete buffer: a
frame arriving in arbitrary chunks is decoded once, without revisiting bytes
already consumed, and the decoder bounds both the wire size of a frame and the
memory its parsed form may occupy before any of it is allocated. Expiration is
carried as an absolute deadline on each entry, enforced lazily when a key is
touched and actively by a budgeted, cursor-ordered sweep on a housekeeping
tick — deterministic in both halves, because the cursor order is fixed and the
clock belongs to the runtime.

## What CI enforces

The properties above are only worth as much as the machinery that keeps them
true.

- **The determinism prohibitions are lints**, not conventions: system time,
  OS entropy, thread spawning, unseeded random number generators and the
  standard hash containers are denied workspace-wide. Each prohibition has a
  fixture crate that violates it, and the gate fails if a fixture stops being
  rejected — a rule nobody can see failing is a rule that has already stopped
  working.
- **The simulation sweep runs on every change that touches code**, replaying a
  range of seeds. A companion self-test plants a genuine lost-update race and
  requires the sweep to find it and a second process to replay it byte for
  byte; the expiration invariants are held to the same standard, each watched
  failing against a deliberately broken server before being trusted.
- **The simulator may not reach production.** One gate proves the simulation
  crate is absent from the production dependency graph — with a positive
  control, so it cannot pass by searching an empty graph — and another compiles
  the production crates without the features a workspace-wide build would
  unify into them.
- **Real clients are the last gate.** `redis-cli`, `redis-benchmark`, redis-py
  and go-redis drive the release binary on every code change. They are the only
  judges in the pipeline that this project did not write.
