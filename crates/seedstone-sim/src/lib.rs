//! SeedStone simulation harness: deterministic testing under turmoil.
//!
//! One simulated host runs the real server — the service layer over a real
//! [`ShardPool`] — and N simulated client hosts reach it over simulated TCP,
//! speaking RESP2 through the real codec. Nothing here is a model of the
//! system: the only code the simulator substitutes is the network and the
//! clock.
//!
//! # Two seeds, never one
//!
//! [`SimConfig::workload_seed`] drives what the clients ask for;
//! [`SimConfig::sim_seed`] drives turmoil's scheduler and network. Fed from
//! one knob, a differing trace hash could not distinguish a reordered
//! schedule from a changed workload, and the two effects would stay
//! confounded forever.
//!
//! # Why concurrency comes from client *hosts*
//!
//! Each client issues its operations in order and waits for a burst's replies
//! before issuing the next; what interleaves is which client's message the
//! server sees next, and that is a property of the simulated network. Collapse
//! the clients into one host and turmoil's seed reaches nothing — the trace
//! becomes a pure function of the workload seed, and a sweep over `sim_seed`
//! reads as a clean PASS while measuring nothing at all.
//!
//! # Why the clients pipeline
//!
//! [`SimConfig::pipeline_depth`] is what puts more than one command in a
//! server drain, and a drain with one command in it exercises no grouping and
//! no batching at all: it decodes one command, forms a chunk of one and
//! dispatches a batch of one, whatever the executor count. At depth 1 the
//! completion order is the arrival order by construction, so
//! [`SimConfig::executors`] reaches nothing the trace can see and the harness
//! reports a clean PASS over a path production never takes under load. Depth
//! is therefore not a workload flavour but a precondition for the executor
//! dimension to exist.
//!
//! # The executor dimension
//!
//! [`SimConfig::executors`] is sweepable state, not an environment reading:
//! correctness must not depend on how many executor tasks host the virtual
//! shards, while the schedule — and so the trace — legitimately does.
//! `tests/executor_mapping.rs` holds both halves.
//!
//! # Three key families, three invariants
//!
//! **Counter keys** are touched only by `INCRBY`, which is order-independent,
//! so their sum has a well-defined expected value no matter how the schedule
//! falls out. Every client adds the delta of each *acknowledged* `INCRBY` to
//! a shared expected total; a final verifier client reads every counter back
//! and sums what is actually there. The two differ **iff** an update was
//! lost. They are the one family several clients share, so they are where the
//! contention is.
//!
//! **Plain keys** take `GET`/`SET`/`DEL` and never carry a deadline. They are
//! partitioned by client, which is what lets a client hold an exact model of
//! them: what it last wrote is what a read must return, at any point and at
//! the end of the run. There is no sum invariant to have — `SET` overwrites —
//! so the model is the invariant.
//!
//! **Volatile keys** carry deadlines, and are partitioned the same way. A
//! client records the deadline it asked for, sampled from its own clock
//! *before* the request left, and holds every later read of that key against
//! it: a value returned well after the deadline is a **stale read**, an
//! absence well before it is a **spurious death**. "Well" is a band inside
//! which the client cannot tell what the server's clock said, so it declines
//! to judge — and there are two of them, asymmetric, because the two sides do
//! not owe the same thing. [`STALE_SLACK`] pays for one message's travel: the
//! deadline the server computed lands that much after the instant the client
//! recorded. [`LIVE_SLACK`] pays nothing, and is zero — that side judges from
//! the reply, which the handler produced before it, against a deadline the
//! server set no earlier than the client's own. Both counters must be zero, and both must have
//! actually decided something: [`SimOutcome`] carries the check counts beside
//! the violation counts, because an invariant that never ran is not a
//! passing one.
//!
//! # Why a client settles before it leaves
//!
//! The workload is over in a fraction of a simulated second, which is less
//! than the deadlines it hands out. So a client ends by sleeping past them —
//! [`SETTLE_CAP`] bounds how far — and reading back every key it owns: the
//! volatile ones it waited out must be gone, and the plain ones, which no
//! deadline was ever put on, must still be exactly what it wrote. That pass
//! is what puts the active sweep under test. The sweep runs on a timer
//! nothing else here waits for, and it is the one thing in this server that
//! mutates a keyspace with no command behind it.
//!
//! # The planted bugs
//!
//! [`SimConfig::planted`] serves the workload through one [`Plant`]: a lost
//! update, a server that never expires anything, or one whose sweep takes
//! everything it walks. That is the harness testing itself — a simulation that
//! has never failed has not been shown capable of failing — and each invariant
//! above owns a plant that it, and only it, is required to catch.
//!
//! Where a plant lives is where the defect it stands for would live. The two
//! expiry plants are the server’s own [`seedstone_core::shard::ExpiryPolicy`], handed to the shard
//! pool at spawn, so the invariant catches the broken decision itself; the
//! lost update is a [`PlantedRouter`] above the shard, because a handler that
//! cannot `await` cannot lose an update to itself.
//!
//! # Where each thing lives
//!
//! - `config` — what one simulation is: every knob a sweep varies.
//! - `plant` — the deliberate defects, and the server policies two of them
//!   are made of.
//! - `routers` — the two routers put in front of the real pool, one planted
//!   and one that skips a shard on a walk.
//! - `workload` — the vocabulary of the simulated clients: keys, operations,
//!   expectations, and the wire they speak over.
//! - `model` — one client's picture of the server, and the checks it holds
//!   every reply to.
//! - `verify` — reading the server's own account of itself and checking it
//!   against what the model knows.
//! - `outcome` — what a run reports, and the tallies the hosts write into
//!   while it runs.
//! - `trace` — the fold that turns every command and reply into one `u64`.
//! - `contract`, `sweep` — starting runs rather than living inside one.

use rand::rngs::ChaCha8Rng;
use rand::{RngExt, SeedableRng};
use seedstone_core::dict::DictSeed;
// The two error texts are imported, not copied. The planted router has to be
// indistinguishable from the honest one except in its atomicity, and these
// strings enter the trace hash — a private copy that drifted would make a
// planted trace differ for a reason unrelated to the race.
use seedstone_core::memory::{EvictionMode, MemoryLimit};
use seedstone_core::shard::{ShardPool, parse_i64};
use seedstone_resp::Frame;
use seedstone_service::{NodeInfo, serve_connection};
use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

// The one thing here that lives outside a simulation rather than inside one:
// it starts runs, in parallel, and reaches into none of them.
pub mod contract;

mod config;
mod model;
mod outcome;
mod plant;
mod routers;
mod sweep;
mod trace;
mod verify;
mod workload;

pub use config::SimConfig;
pub use outcome::SimOutcome;
pub use plant::Plant;
pub use routers::{PlantedRouter, SkippingRouter};
pub use sweep::{SweepReport, sweep};
pub use trace::mix;

use model::Model;
use outcome::{Shared, lock};
use plant::{EvictsBelowCeiling, IgnoresCeiling, ScanMissesRehash, ServeExpired, SweepEatsAll};
use trace::{GOLDEN, HashSink, TRACE_INIT};
use verify::{check_ceiling, evicted_keys, executor_timing, listed_keys, parse_u64};
use workload::{
    Conn, WALK_ALL, WALK_CHURN_WRITES, WALK_KEYS, WALK_PREFIX_STEPS, WALK_SCAN_COUNT, command,
    counter_key,
};

/// The port the simulated server listens on. Redis's, for familiarity; in a
/// simulation nothing else is competing for it.
const PORT: u16 = 6379;

/// The simulated host name every client connects to.
const SERVER: &str = "server";

/// How much simulated time a run may take before turmoil calls it stuck.
///
/// This is a deadlock detector, not a budget: the simulation stops as soon as
/// every client has finished, so a generous ceiling costs nothing. Overrunning
/// it means a client is blocked forever, which is a finding rather than a
/// tuning problem.
const SIM_DURATION: Duration = Duration::from_mins(10);

/// How often the verifier client re-checks whether the workload has finished.
const VERIFIER_POLL: Duration = Duration::from_millis(10);

/// The least simulated time a message spends on the wire.
///
/// turmoil's own default, spelled out here rather than inherited:
/// [`STALE_SLACK`] is derived from the pair, and a bound derived from a
/// dependency's undocumented default goes quietly wrong the day the dependency
/// changes it. Zero is also what lets [`LIVE_SLACK`] be zero — a request
/// cannot reach the server before the client sent it.
const MIN_MESSAGE_LATENCY: Duration = Duration::from_millis(0);

/// The most simulated time a message spends on the wire.
///
/// See [`MIN_MESSAGE_LATENCY`]. This one is also the width of the client's
/// ignorance about when its request was actually handled, which is what
/// [`STALE_SLACK`] pays for.
const MAX_MESSAGE_LATENCY: Duration = Duration::from_millis(100);

/// How far past a deadline a reply must land before the client will call the
/// server wrong.
///
/// The client samples its clock *before* it sends, and the server computed the
/// deadline from its own clock when the handler ran — up to one message
/// latency later, in simulated time, than the instant the client recorded. So
/// a request sent after `deadline + STALE_SLACK` met a server whose own
/// deadline had certainly passed. One [`MAX_MESSAGE_LATENCY`] covers that
/// exactly, and the housekeeping tick buys nothing here: a read meets the lazy
/// path, which does not wait for the sweep.
///
/// See [`LIVE_SLACK`] for the other half, and for why neither band carries a
/// term for the difference between two simulated hosts' clocks.
const STALE_SLACK: Duration = Duration::from_millis(100);

const _: () = assert!(
    STALE_SLACK.as_millis() >= MAX_MESSAGE_LATENCY.as_millis(),
    "the staleness band pays for one message's travel: the server's deadline \
     lands that much after the instant the client recorded"
);

/// How far inside a deadline a reply must land before the client will call a
/// missing key a spurious death.
///
/// Nothing is owed on this side, which is why it is zero. The judgement is
/// made from the instant the reply was *received*, which is after the handler
/// ran, and the server's deadline is never earlier than the one the client
/// recorded — so a reply received before the recorded deadline was produced
/// before the real one.
///
/// # Why there is no term for the hosts' clocks
///
/// Each simulated host's paused clock starts at whatever the wall clock said
/// when turmoil built its runtime, so two hosts read different absolute values
/// at the same simulated moment — by hundreds of microseconds, and it is a
/// property of the machine rather than of the run. That offset reaches neither
/// band, because neither band reads across it: every instant a client compares
/// is a reading of its own clock, and the server's deadline is compared, on the
/// server, against the server's. What links the two is elapsed simulated time,
/// and the offset cancels out of every difference taken.
///
/// What would not cancel is *drift* — one host's clock advancing by more
/// simulated time than another's over the same stretch. That is the property
/// both bands actually rest on, and it is asserted rather than assumed:
/// `tests/host_clocks.rs` holds every host in the workload's own topology to
/// the same advance.
const LIVE_SLACK: Duration = Duration::ZERO;

const _: () = assert!(
    LIVE_SLACK.is_zero(),
    "the liveness side judges from a reply the handler produced before it, \
     against a deadline the server set no earlier than the client recorded it: \
     nothing is owed. A non-zero band here means the derivation above stopped \
     being true — not that a run wanted more room"
);

/// The longest a client naps between bursts, in milliseconds.
///
/// The workload needs some simulated time to pass: every deadline it hands
/// out outlives the handful of milliseconds a burst costs, so with no pause
/// at all no read would land on the far side of one until the settle. It also
/// scatters the bursts against the server's 10 Hz housekeeping tick instead
/// of packing them into one interval.
///
/// Simulated time is *not* free, which is what keeps this small — see
/// [`SETTLE_CAP`].
const BURST_NAP_MAX_MS: u32 = 60;

/// The longest a client waits for its own deadlines before reading everything
/// back.
///
/// Simulated time costs wall clock, and not in proportion to what happens in
/// it: turmoil steps every host with running software on every one-
/// millisecond tick, and delivering messages across the topology is the
/// dominant cost of a run whether or not any were sent. A client asleep is a
/// client still being stepped — so a settle long enough to outlast every
/// deadline in [`DEADLINES`] would multiply the swept shape's wall clock for
/// coverage it does not need.
///
/// Capping it costs nothing but the keys whose deadlines outlive the wait,
/// and those are simply not decided: the band skips them, [`SimOutcome`]'s
/// check counts say so, and the workload hands out enough short deadlines
/// that the pass decides most of what it reads.
const SETTLE_CAP: Duration = Duration::from_millis(300);

/// Read buffer size for a client connection.
///
/// A burst's replies may well exceed this; the read loop reassembles across
/// reads either way, so the size is a working-set choice and not a limit.
const CLIENT_CHUNK: usize = 4096;

/// Runs one simulation and reports what it observed.
///
/// # Panics
///
/// If the simulation itself fails — a host returning an error, or
/// [`SIM_DURATION`] elapsing with a client still running. Both are harness
/// bugs or deadlocks rather than findings about the system, and neither is
/// something a sweep can carry on past.
#[must_use]
pub fn run_sim(cfg: &SimConfig) -> SimOutcome {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(SIM_DURATION)
        .min_message_latency(MIN_MESSAGE_LATENCY)
        .max_message_latency(MAX_MESSAGE_LATENCY)
        .rng_seed(cfg.sim_seed)
        .build();

    let trace = Arc::new(Mutex::new(TRACE_INIT));
    let shared = Shared::default();

    // The dict seed is derived from the simulator seed so two seeds do not
    // share a bucket layout: a hash collision that only shows up under one
    // placement then gets swept over instead of being baked into every run.
    let dict_seed = DictSeed {
        k0: mix(TRACE_INIT, cfg.sim_seed),
        k1: mix(GOLDEN, cfg.sim_seed),
    };
    let sink = HashSink(Arc::clone(&trace));
    let shards = cfg.shards;
    let executors = cfg.executors;
    let planted = cfg.planted;
    let limit = MemoryLimit {
        ceiling: cfg.maxmemory,
        mode: EvictionMode::AllKeysLru,
    };

    sim.host(SERVER, move || {
        // Cloned per invocation: turmoil may restart a host, and each start
        // needs its own future. The sink is shared on purpose — a restart
        // continues the same trace.
        let sink = sink.clone();
        server(shards, executors, dict_seed, sink, planted, limit)
    });

    for id in 0..cfg.clients {
        sim.client(
            format!("client-{id}"),
            client(id, cfg.clone(), shared.clone()),
        );
    }
    sim.client("verifier", verifier(cfg.clone(), shared.clone()));

    sim.run().expect("simulation failed");

    let tally = *lock(&shared.tally);
    SimOutcome {
        trace_hash: *lock(&trace),
        expected_sum: tally.expected,
        actual_sum: tally.actual,
        stale_reads: tally.stale_reads,
        spurious_deaths: tally.spurious_deaths,
        plain_mismatches: tally.plain_mismatches,
        dead_checks: tally.dead_checks,
        alive_checks: tally.alive_checks,
        plain_checks: tally.plain_checks,
        walk_mismatches: tally.walk_mismatches,
        walk_checks: tally.walk_checks,
        evictions_observed: tally.evictions_observed,
        evicted_keys: tally.evicted_keys,
        executor_usec: tally.executor_usec,
        executor_calls: tally.executor_calls,
        ceiling_breaches: tally.ceiling_breaches,
        ceiling_checks: tally.ceiling_checks,
        evictable: cfg.maxmemory.is_some(),
        forms_emitted: lock(&shared.forms).clone(),
    }
}

/// The server host: the real stack, on a simulated listener.
///
/// The pool is spawned *here*, inside the host, rather than around
/// [`run_sim`]: [`ShardPool::spawn`] spawns onto the ambient tokio runtime,
/// and under turmoil each host has its own. Spawned outside, the shards would
/// land on whatever runtime happened to be current — or on none at all.
async fn server(
    shards: u16,
    executors: u16,
    seed: DictSeed,
    sink: HashSink,
    planted: Option<Plant>,
    limit: MemoryLimit,
) -> turmoil::Result {
    // Every arm is spawned with the limit, the honest one included: the
    // ceiling is the shape's, not the plant's, and a run whose honest node
    // had no ceiling would be measuring a different server from the one its
    // planted twin runs.
    let pool = match planted {
        Some(Plant::ServeExpired) => {
            ShardPool::spawn_with_policy_limited(shards, executors, seed, sink, ServeExpired, limit)
        }
        Some(Plant::SweepEatsAll) => {
            ShardPool::spawn_with_policy_limited(shards, executors, seed, sink, SweepEatsAll, limit)
        }
        Some(Plant::ScanMissesRehash) => ShardPool::spawn_with_policy_limited(
            shards,
            executors,
            seed,
            sink,
            ScanMissesRehash,
            limit,
        ),
        Some(Plant::IgnoresCeiling) => ShardPool::spawn_with_policy_limited(
            shards,
            executors,
            seed,
            sink,
            IgnoresCeiling,
            limit,
        ),
        Some(Plant::EvictsBelowCeiling) => ShardPool::spawn_with_policy_limited(
            shards,
            executors,
            seed,
            sink,
            EvictsBelowCeiling,
            limit,
        ),
        // The honest pool, and the two router plants' too: both of those
        // defects live above the shard, where a real one would.
        None | Some(Plant::LostUpdate | Plant::CrossingSkipsShard) => {
            ShardPool::spawn_limited(shards, executors, seed, sink, limit)
        }
    };
    let listener = turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, PORT)).await?;
    // One per host, as it is in production: it describes the node, not the
    // connection. No workload here asks a host about itself, so nothing reads
    // it — it is here because the connection code takes one.
    let mut node = NodeInfo::for_tests();
    // The simulated node's `INFO` reads the same word its executors keep, for
    // the reason the field states: a scrape and an eviction decision must not
    // be able to disagree.
    node.memory = pool.memory();
    // And the ceiling beside it, for the same reason: a run whose `INFO`
    // reported a different `maxmemory` from the one its executors evicted by
    // would be a run whose invariants are checked against the wrong number.
    node.limit = pool.limit();
    loop {
        let (stream, _peer) = listener.accept().await?;
        // Two plants are routers: a lost update, which is a defect between two
        // messages of one command, and a crossing that skips a shard, which is
        // a defect in a loop the shards know nothing about. The rest are inside
        // the server, which is where the defects they imitate would be.
        match planted {
            Some(Plant::LostUpdate) => {
                tokio::spawn(serve_connection(
                    stream,
                    PlantedRouter::new(pool.clone()),
                    node.clone(),
                ));
            }
            Some(Plant::CrossingSkipsShard) => {
                tokio::spawn(serve_connection(
                    stream,
                    SkippingRouter::new(pool.clone()),
                    node.clone(),
                ));
            }
            _ => {
                tokio::spawn(serve_connection(stream, pool.clone(), node.clone()));
            }
        }
    }
}

/// One client host: connect, issue `ops_per_client` operations in bursts of
/// `pipeline_depth`, settle, read back everything it owns, disconnect.
///
/// Ordered on purpose — see the module documentation. A client never has two
/// *bursts* outstanding, so what interleaves is which client's burst the
/// server sees next; that there are many of these is the only source of
/// interleaving between connections, and the depth is the only source of
/// batching within one. Within a burst the order is total too, which is what
/// lets the model be updated reply by reply: a `SET` and a later `GET` of the
/// same key in one burst reach the same shard in the order they were written.
async fn client(id: u16, cfg: SimConfig, shared: Shared) -> turmoil::Result {
    // Decorrelated per client so client 1's stream is not client 0's shifted
    // by one, which a plain `workload_seed + id` would give.
    let mut rng = ChaCha8Rng::seed_from_u64(
        cfg.workload_seed ^ GOLDEN.wrapping_mul(u64::from(id).wrapping_add(1)),
    );
    let mut conn = Conn::connect().await?;
    let mut model = Model::new(id, &cfg, shared.clone());

    // A depth of zero would issue nothing forever; one is the degenerate
    // request/response client, which is a shape worth being able to ask for.
    let depth = cfg.pipeline_depth.max(1);
    let mut issued = 0u32;
    while issued < cfg.ops_per_client {
        let this_burst = depth.min(cfg.ops_per_client - issued);
        // Sampled before the burst is even composed, so every deadline it
        // hands out is one the server cannot have reached earlier than.
        let sent = Instant::now();
        let mut burst = Vec::with_capacity(this_burst as usize);
        let mut checks = Vec::with_capacity(this_burst as usize);
        // The rolls are drawn in the same order at any depth, so the workload
        // a seed describes is the same workload however it is pipelined.
        for op in 0..this_burst {
            let op = model.compose(&mut rng, sent, issued + op);
            model.record_form(op.form);
            burst.push(op.frame);
            checks.push(op.check);
        }
        let replies = conn.request_many(&burst).await?;
        model.observe(&replies, &checks, sent, Instant::now());
        // After the burst rather than inside it: the reading has to describe
        // a node with every one of those writes applied, and an `INFO`
        // pipelined among them describes whatever had run by the time it was
        // reached.
        model.probe_ceiling(&mut conn).await?;

        issued += this_burst;
        let nap = rng.random_range(0..=BURST_NAP_MAX_MS);
        tokio::time::sleep(Duration::from_millis(u64::from(nap))).await;
    }

    model.settle(&mut conn, depth as usize).await?;
    // After the settle, so the walk keys are the only thing this client has
    // written that nothing is still deciding the fate of.
    model.walk(&mut conn, &cfg, depth as usize).await?;
    lock(&shared.tally).done += 1;
    Ok(())
}

/// The last client: waits for the workload to drain, then reads every counter.
///
/// It waits rather than racing because the invariant is about what the server
/// *settled on*: a counter read while an increment is still in flight is not
/// a lost update, it is an early read.
async fn verifier(cfg: SimConfig, shared: Shared) -> turmoil::Result {
    while lock(&shared.tally).done < u32::from(cfg.clients) {
        // Simulated time: this costs ticks, not wall clock.
        tokio::time::sleep(VERIFIER_POLL).await;
    }

    let mut conn = Conn::connect().await?;
    let mut total: i64 = 0;
    // Read in bursts rather than one round trip per counter. Nothing is
    // racing any more — every client has finished — so what a serial read
    // would buy is only simulated seconds, and those are paid for in ticks
    // the whole simulation walks through.
    let depth = cfg.pipeline_depth.max(1) as usize;
    lock(&shared.forms).insert(contract::FORM_GET);
    let keys: Vec<Frame> = (0..cfg.counter_keys)
        .map(|key| command(&["GET", &counter_key(key)]))
        .collect();
    for (batch, burst) in keys.chunks(depth).enumerate() {
        for (offset, reply) in conn.request_many(burst).await?.into_iter().enumerate() {
            let key = batch * depth + offset;
            total += match reply {
                // Never incremented, or incremented back out of existence.
                Frame::Null => 0,
                Frame::Bulk(value) => {
                    parse_i64(&value).ok_or_else(|| format!("counter {key} is not an integer"))?
                }
                other => return Err(format!("counter {key} answered with {other:?}").into()),
            };
        }
    }
    lock(&shared.tally).actual = total;

    // The node's own account of the run, taken once and last: every client
    // has finished, so `evicted_keys` is final and `used_memory` describes a
    // node nothing is still writing to. The clients' own readings are the
    // ones taken under load; this is the one taken at rest, and the two
    // together are what `ceiling_breaches` is a count over.
    if let Some(ceiling) = cfg.maxmemory {
        {
            let mut forms = lock(&shared.forms);
            forms.insert(contract::FORM_INFO_STATS_COMMANDSTATS);
            forms.insert(contract::FORM_INFO_MEMORY);
        }
        // Two sections in one request rather than two requests, and the
        // difference is not tidiness: `stats` and `commandstats` are both
        // built from a broadcast the server takes *once* per `INFO`, so
        // asking for them together adds nothing to this shape's trace. A
        // second `INFO` would add a command per shard, and the recorded
        // hashes with it.
        let replies = conn
            .request_many(&[
                command(&["INFO", "stats", "commandstats"]),
                command(&["INFO", "memory"]),
            ])
            .await?;
        {
            let (usec, calls) = executor_timing(&replies[0]);
            let mut tally = lock(&shared.tally);
            tally.evicted_keys = evicted_keys(&replies[0]).unwrap_or(0);
            tally.executor_usec = usec;
            tally.executor_calls = calls;
        }
        check_ceiling(&replies[1], ceiling, &shared);
    }

    if cfg.quiescent_walk {
        walk_the_whole_family(&mut conn, &cfg, &shared).await?;
    }
    Ok(())
}

/// The quiescent walk over every walk key in the run, by both commands.
///
/// Here rather than in a client because every client has finished: nothing is
/// mutating anything, so the model knows the *whole* keyspace this pattern
/// selects and the assertion is set equality over all of it rather than over
/// one client's slice.
///
/// A full `SCAN` cycle costs at least one round trip per shard, so it is run
/// once for the run rather than once per client. N of them would buy nothing
/// the one does not: the walk is over a set nobody is touching, so a second
/// walker sees exactly what the first did.
///
/// **Under a ceiling the equality becomes containment**, in both halves and
/// for the reason [`Model::walk`] states over its own slice: a key nobody is
/// touching can still be reclaimed, so what nothing may return is a name that
/// was never written, and what `KEYS` may still not do is return one twice.
/// Completeness is exactly the claim eviction is allowed to break.
async fn walk_the_whole_family(
    conn: &mut Conn,
    cfg: &SimConfig,
    shared: &Shared,
) -> turmoil::Result<()> {
    let expected = lock(&shared.walk).clone();
    {
        let mut forms = lock(&shared.forms);
        forms.insert(contract::FORM_KEYS);
        forms.insert(contract::FORM_SCAN_MATCH);
        forms.insert(contract::FORM_SCAN_MATCH_COUNT);
    }

    let evictable = cfg.maxmemory.is_some();
    let reply = conn.request_many(&[command(&["KEYS", WALK_ALL])]).await?;
    let keys_agrees = match listed_keys(&reply[0]) {
        Some((keys, false)) if evictable => keys.is_subset(&expected),
        listed => listed == Some((expected.clone(), false)),
    };

    // Every shard costs a step even when it holds nothing, because a spent
    // shard hands back the next one's start rather than continuing into it;
    // a shard costs a second step only once its table has grown past a step's
    // bucket budget, which takes more keys than exist. So the shard count plus
    // the keyspace is a bound the walk cannot legitimately reach, and reaching
    // it is a cursor that stopped advancing.
    //
    // The walk family is the stable keys plus every churn key a client's own
    // walk wrote, which is why the churn is counted here: this runs after the
    // clients have finished, so what it walks is whatever they left behind.
    // It is stated against the prefix shape's step count on purpose — the
    // cycle-completing shape churns for as long as its walk runs, and the two
    // are never asked for together.
    let walk_family = u64::from(WALK_KEYS) + u64::from(WALK_CHURN_WRITES) * WALK_PREFIX_STEPS;
    let bound = u64::from(cfg.shards)
        + u64::from(cfg.plain_keys)
        + u64::from(cfg.volatile_keys)
        + u64::from(cfg.counter_keys)
        + u64::from(cfg.clients) * walk_family;

    let mut seen = BTreeSet::new();
    let mut cursor = 0u64;
    let mut steps = 0u64;
    let mut scan_agrees = true;
    loop {
        // Both forms, alternating, so a run exercises the option and its
        // absence and neither depends on a seed to be reached. The count is
        // the client's to choose and the server clamps it; below the clamp,
        // the number on the wire is the number that is used.
        let cursor_text = cursor.to_string();
        let count_text = WALK_SCAN_COUNT.to_string();
        let step = if steps.is_multiple_of(2) {
            command(&["SCAN", &cursor_text, "MATCH", WALK_ALL])
        } else {
            command(&[
                "SCAN",
                &cursor_text,
                "MATCH",
                WALK_ALL,
                "COUNT",
                &count_text,
            ])
        };
        let reply = conn.request_many(&[step]).await?;
        steps += 1;

        let Frame::Array(parts) = &reply[0] else {
            scan_agrees = false;
            break;
        };
        let [Frame::Bulk(next), keys] = parts.as_slice() else {
            scan_agrees = false;
            break;
        };
        let (Some(next), Some((keys, _))) = (parse_u64(next), listed_keys(keys)) else {
            scan_agrees = false;
            break;
        };
        // Repeats are allowed here and nowhere else: `SCAN` may return a key
        // twice, as Redis's does, so the union across steps is what the
        // guarantee is about.
        seen.extend(keys);
        cursor = next;
        if cursor == 0 {
            break;
        }
        if steps >= bound {
            scan_agrees = false;
            break;
        }
    }

    {
        let mut tally = lock(&shared.tally);
        tally.walk_checks += 2;
        if !keys_agrees {
            tally.walk_mismatches += 1;
        }
        let scan_set_agrees = if evictable {
            seen.is_subset(&expected)
        } else {
            seen == expected
        };
        if !scan_agrees || !scan_set_agrees {
            tally.walk_mismatches += 1;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
