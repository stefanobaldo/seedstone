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
- **The shard policy is ours, and it is the one seam built to be broken.** It
  carries every decision a shard executor consults that production has exactly
  one answer for: today, when a deadline comes due — asked by both halves of
  expiration, the check in front of every command and the housekeeping sweep —
  how a keyspace walk's cursor advances, and whether the node must reclaim
  memory now. They travel as one value because the same executor holds all
  three for the same span. Production links exactly one
  policy, an honest zero-sized implementation; the harness supplies defective
  ones, so what an invariant catches is the defect itself rather than an
  imitation of what it would look like from outside. A cargo feature could not
  do this job: a workspace-wide build unifies features, so the harness's would
  be compiled into the shipped binary. A type parameter puts the broken
  implementations in a crate the binary does not depend on, where they are
  unlinkable rather than merely unused.

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
there, and a command that names a key is routed to the executor that owns its
shard. Two other routing shapes share that path: a command that names a *shard*
instead of a key, which is how one step of a keyspace walk reaches the shard
whose cursor it carries, and a command every shard must see, such as emptying
the keyspace or counting it. Multi-key commands fan out from this layer, and so
does the whole-keyspace walk — one cursor loop per shard, run concurrently.

Authentication is per-connection state in that same layer: one flag, and a gate
a decoded command passes before the router ever sees it — so a peer that has
not authenticated moves no key and learns nothing about the keyspace, not even
from how long a refusal took. Only the attempt itself, the handshake that can
carry it — answered whether or not it does, which is a divergence from Redis
and is stated below — and the goodbye are let through, and which side of the
gate a command falls on is decided exhaustively, so one added later does not
compile until somebody has said which. The secret is not that layer's to find: it is a
dependency the composition root reads from a file or the environment and hands
down, which is why a node configured without one starts every connection
already through the gate, and why the simulated node is handed none at all.

That layer is its own crate, and the dependency arrow is the reason. It depends
on the core and on the codec; the core depends on neither. An adapter the core
depends on is not at the edge — and with the arrow pointing this way, a second
protocol frontend is a sibling crate rather than a second adapter living inside
the deterministic core.

A connection sizes its buffers to what it is doing and gives the capacity back
when it stops. Two signals say that it has: the shape of its reads, and — for a
peer that stops producing reads at all, which the first cannot see, because
nothing wakes a task parked on one — a timer armed only while the connection
holds more than its floor, and disarmed by whichever signal empties it. A server
whose peers have all gone quiet therefore holds neither the buffers nor the
timers.

The codec is a resumable decoder rather than a parser over a complete buffer: a
frame arriving in arbitrary chunks is decoded once, without revisiting bytes
already consumed, and the decoder bounds both the wire size of a frame and the
memory its parsed form may occupy before any of it is allocated. Expiration is
carried as an absolute deadline on each entry, enforced lazily when a key is
touched and actively by a budgeted, cursor-ordered sweep on a housekeeping
tick — deterministic in both halves, because the cursor order is fixed and the
clock belongs to the runtime.

## Where we differ from Redis, and why

The differences below are behavioural and observable from a client, so they are
stated here rather than left to be discovered.

**A keyspace walk is at-least-once, not a snapshot.** `KEYS` and `SCAN` answer
with a set that was the keyspace at no single instant: a key created while a
walk is in flight may be missed, and a key deleted while it is in flight may
still appear. Redis's `KEYS` is atomic because Redis is single-threaded, not
because the guarantee was designed; here the keyspace is spread across shards
that no lock spans, and a global instant would cost a barrier every single-key
command would pay for. What a walk does guarantee is that it terminates, and
that a cursor carried across a table resize can only re-visit buckets, never
skip them. `KEYS` removes the duplicates that follow from that, so it does not
report a key twice; `SCAN` may, exactly as in Redis.

**`KEYS` does not stop the server, and is still `O(keyspace)`.** It walks in
bounded steps that yield between them, and every shard walks at once, so
nothing queues behind it — but it competes for CPU with traffic for as long as
it runs, and it accumulates the whole answer at the edge before any of it
reaches the wire. Above a per-request ceiling on the reply's size the walk is
abandoned and answered with an error that says to use `SCAN`; the ceiling is
the same per-connection figure the request side is held to. Not blocking is the
difference worth having; it is not the same thing as being cheap. Use `SCAN`.

**A `SCAN` cursor names the shard it is walking, and belongs to one process.**
The cursor packs a shard number and that shard's own cursor into the single
integer the command exchanges, which is how a client that knows nothing about
shards ends up walking all of them. Three consequences follow. The shard count
is fixed for the life of the process, so a cursor does not survive a restart. A
cursor is accepted only in the canonical decimal it was issued in, so `SCAN
007` is an error here where Redis reads cursor `7` — a cursor is not a number a
person types, but one this server issued and the client hands straight back.
And a full cycle costs at least one round trip per shard whatever the keyspace
holds, because a spent shard hands back the next shard's start rather than
continuing into it: walking a thousand keys costs about a thousand calls, where
Redis answers the same walk in about a hundred at its default `COUNT`. That is
a floor on round trips, not on work — each call still costs what a `GET` costs.

**`used_memory` is an accounting formula, not an allocator reading.** Each
shard's keyspace reports what it is accounted at — a fixed overhead per entry
plus the lengths of its key and value, plus a fixed cost per bucket of every
table it holds — and the node's figure is the sum, kept current by the shard
executors as they run commands. Nothing asks the allocator. The reason is the
one behind everything else here: an allocator's answer depends on the
allocator, the platform and the fragmentation history of the process, so a
replayed run would not reproduce it, and a memory ceiling derived from it
would be a different ceiling on every machine. What the formula costs is
exactness: it counts the bytes a value has, not the bytes the allocator
rounded them up to, and it does not count what the process spends outside the
keyspace. `mem_fragmentation_ratio` is therefore always `1.00` — the honest
answer for a figure that is not measured against an allocator at all, rather
than a ratio invented to fill the field. It also means the process's resident
size is larger than `used_memory` — by the allocator's rounding and by
everything the process holds outside the keyspace — so an operator sizing a
ceiling against the machine should leave room for the difference.

**Eviction samples from the shard that is writing, against a ceiling the whole
node shares.** `maxmemory` is compared with one figure — the sum of what every
shard holds — so the node evicts when *it* is full, as an operator reads it in
`INFO`. The victim, though, is chosen by the shard that just wrote, from a
sample of its own keys, because no shard reaches into another's table: with
many shards and a seeded hash, a sample of one shard is a fair sample of the
keyspace. The stamp the sample compares is a per-shard command counter, not a
clock, so which key is oldest replays exactly. A write that has to make room
does so before it is answered, which is latency on that write and on nothing
else — and it never frees room by undoing itself, so a value larger than the
whole ceiling is stored and leaves the node over it rather than being written
and immediately taken back. Only `allkeys-lru` and `noeviction` exist: the
`volatile-*` family reclaims nothing from a keyspace that carries no
deadlines, and offering a policy that evicts nothing would be offering a
ceiling that holds nothing.

**The ceiling is compared against what the node already holds, so one write
crosses it.** A command is refused or made room for on the figure as it stood
*before* it ran, which is how Redis compares it too: a write that arrives with
the node exactly at its ceiling lands, and it is the write after it that meets
the refusal. Under `allkeys-lru` the crossing is reclaimed immediately and the
figure is back under before the reply goes out; under `noeviction` the node
sits marginally over its ceiling until something is deleted or expires.

**The slow log and the latency monitor are disabled, by construction.** Nothing
here times a handler, so there is no threshold at which a command would be
recorded and no sample a reading could be drawn from. `SLOWLOG GET` is empty,
`LATENCY LATEST` is empty, and `CONFIG GET` reports
`slowlog-log-slower-than -1` and `latency-monitor-threshold 0` — which is what
Redis itself reports with both monitors off, so the readings and the
configuration agree. Answering rather than refusing is deliberate: a refusal
carries a different fact — that the command does not exist — and an exporter
reads the first as the node's answer and the second as a failed scrape. Only
a named list of subcommands is answered, though, and anything outside it —
`LATENCY DOCTOR` and `LATENCY GRAPH`, which Redis serves from a monitor this
server has no reading for, the `HELP` texts beside them, any other spelling a
client tries — is refused as an unknown subcommand rather than given a
sentence nobody measured.

The same absence of timing shapes `commandstats`, which carries call counts and
no `usec` fields; a zero there would be a measurement this server does not take,
printed as if it did. Per-command timing is a measurement campaign's to add,
with a cost stated, not a field to fill in. **That omission costs a metric, and
the cost is taken deliberately.** The exporter this project is gated on parses
a `cmdstat_` line by position: it drops one carrying fewer than three
comma-separated fields, and otherwise reads whatever sits in the second field
as microseconds, whatever that field is named. A line of `calls=N` alone is
therefore dropped and no per-command metric is published at all. Padding it
would publish a duration derived from a number that is not one — the same
fabrication moved from this server's output into the monitoring system, where
it is harder to see. The family is given up instead, and the lane asserts the
duration metric is absent so that a later change cannot quietly buy it back.

**`HELLO` is answered before a connection has authenticated.** Redis refuses
it on an unauthenticated connection and tells the client to use `HELLO <proto>
AUTH <user> <pass>` instead, so that a password-protected node says nothing at
all until the password has been given. Here the command is answered either
way. The inline form has to be reachable before authentication whatever else
is decided — it is how the ordinary client libraries authenticate — and a
client that cannot ask which protocol it is speaking has nothing correct to
say next. The cost is that a peer which has not authenticated can read the
node's own description: the server name, its version, the protocol version,
the deployment mode and the role. That is metadata about the process and not
about the keyspace — no key, no count and no hint of either, since every other
command is refused before the router is reached — but it is a divergence taken
with its cost, not an oversight. An operator who needs a node to say nothing
whatever before a password is given should keep the port off untrusted
networks, which is where transport security belongs here anyway.

**The surface is a named list, and anything outside it is refused.** A command
this server does not implement is answered with an error naming it, rather than
with an approximation of what it might have meant. The list is chosen for the
workloads this project targets; it grows by being extended, and a client that
needs more than it holds finds out at once rather than through a wrong answer.

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
  failing against a deliberately broken server before being trusted. So is the
  walk's cursor — against a keyspace narrow and deep enough for a cursor to be
  caught between steps, which the swept shape deliberately is not, and the
  test that plants it says so where it lives. The memory ceiling is watched
  the same way, from both sides: a node that reads its ceiling and never
  reclaims, and one that reclaims whatever it holds. Each needs a shape with a
  ceiling to be under, and that shape trades exactness for truth — where keys
  may be reclaimed at any moment, a client's model of them becomes an upper
  bound rather than an equality, and the shapes with no ceiling keep the
  exact one. That shape is swept as a step of its own rather than by widening
  the first: the first is the shape whose cost was measured, and every second
  added to it is paid for in seeds, which are detection power. Every sweep ends
  by printing how many of the simulated client's declared command forms it
  reached and naming the ones it did not. A shape with no ceiling never asks
  the node about one, so the sweep of the standard shape names `INFO memory`
  and `INFO stats` as unreached while the bounded one reaches every form. That
  line is printed rather than asserted, because what a shape does not reach is
  a property of the shape and not a defect — and printed every time, because a
  coverage claim nobody states is what it exists to end.
- **The simulator may not reach production.** One gate proves the simulation
  crate is absent from the production dependency graph — with a positive
  control, so it cannot pass by searching an empty graph — and another compiles
  the production crates without the features a workspace-wide build would
  unify into them.
- **Real clients are the last gate.** `redis-cli`, `redis-benchmark`, redis-py
  and go-redis drive the release binary on every code change, and behind them a
  third party's cache-backend test suite runs against it from an archive
  checked against a pinned digest, in a container pinned by the digest of its
  multi-architecture image, so the interpreter that client pair needs is the
  same bytes on every machine. A stock Prometheus exporter scrapes the server
  last, pinned the same way and given no flag it would not be given for Redis,
  and it is held to two things rather than one: an error log with nothing in it,
  and the metric values the lane can predict from what it wrote and how it
  started the server. One of those alone would not do. An exporter reports
  the refusals it chooses to report and silently drops the metric families
  that depended on the rest, so a quiet log is not the same as a complete
  scrape, and the predicted values are what catches a family that went
  missing without a word. They are the only judges in the pipeline that this
  project did not write, and the last two judge what they are — one pinned
  client pair, one pinned exporter — rather than the protocol in the
  abstract.
