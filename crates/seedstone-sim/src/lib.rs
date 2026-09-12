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
mod outcome;
mod plant;
mod routers;
mod sweep;
mod trace;
mod workload;

pub use config::SimConfig;
pub use outcome::SimOutcome;
pub use plant::Plant;
pub use routers::{PlantedRouter, SkippingRouter};

use workload::{
    COUNTER_OPS, Check, CondReply, Conn, DEADLINES, EXPIRE_SECONDS, KeyRange, Known, Op,
    PEXPIRE_MILLIS, PLAIN_END, Spelling, WALK_ALL, WALK_CHURN_DELETES, WALK_CHURN_WRITES,
    WALK_CURSOR_SHARD_SHIFT, WALK_CYCLE_STEP_BOUND, WALK_KEYS, WALK_PREFIX_STEPS, WALK_SCAN_COUNT,
    WALK_STEP_COUNT, command, counter_key, plain_key, volatile_key, walk_key, walk_pattern,
};

use plant::{EvictsBelowCeiling, IgnoresCeiling, ScanMissesRehash, ServeExpired, SweepEatsAll};
pub use sweep::{SweepReport, sweep};
pub use trace::mix;

use trace::{GOLDEN, HashSink, TRACE_INIT};

use outcome::{Shared, lock};

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

/// One client's picture of the keys it owns, and the invariants it holds the
/// server to over them.
struct Model {
    /// Which client this is. Its walk keys carry it in their names, so a glob
    /// can isolate them from every other client's.
    id: u16,
    /// How many counter keys there are. The one family this client does not
    /// own a slice of: they are shared, which is where the contention is.
    counter_keys: u32,
    plain: KeyRange,
    volatile: KeyRange,
    /// What this client last wrote to each plain key it owns.
    plain_state: Vec<Known>,
    /// The deadline it last asked for on each volatile key it owns, sampled
    /// from its own clock *before* the request left — so the deadline the
    /// server computed is this instant or later, never earlier.
    deadlines: Vec<Option<Instant>>,
    /// Whether the node this client is talking to has a ceiling.
    ///
    /// The one thing that weakens the plain family's model, and it is
    /// deliberately the whole of what it weakens: a key may be *gone* when a
    /// ceiling exists, and it may never hold a value its owner did not write.
    /// Every relaxation below is that one sentence in a different
    /// vocabulary.
    evictable: bool,
    /// The ceiling itself, for the readings taken against it.
    ceiling: Option<u64>,
    shared: Shared,
}

impl Model {
    /// The model client `id` starts with: it owns nothing yet and believes
    /// nothing.
    fn new(id: u16, cfg: &SimConfig, shared: Shared) -> Self {
        let plain = KeyRange::new(id, cfg.plain_keys, cfg.clients);
        let volatile = KeyRange::new(id, cfg.volatile_keys, cfg.clients);
        Self {
            id,
            counter_keys: cfg.counter_keys,
            plain_state: vec![Known::Nothing; plain.len as usize],
            deadlines: vec![None; volatile.len as usize],
            evictable: cfg.maxmemory.is_some(),
            ceiling: cfg.maxmemory,
            plain,
            volatile,
            shared,
        }
    }

    /// Notes that this client put `form` on the wire.
    ///
    /// Called at the point a command is composed rather than counted from the
    /// bytes afterwards, so the label the contract is checked against is the
    /// one the generator chose and not a second reading of it.
    fn record_form(&self, form: &'static str) {
        lock(&self.shared.forms).insert(form);
    }

    /// One to three plain slots, for the commands that take several keys.
    ///
    /// Repeats are not prevented: `DEL k k` and `EXISTS k k` mean different
    /// things in Redis and are separately worth getting right, so the model
    /// predicts both and lets the draw decide which one it is looking at.
    fn several(&self, rng: &mut ChaCha8Rng) -> Vec<u32> {
        (0..rng.random_range(1..=3u32))
            .map(|_| self.plain.pick(rng))
            .collect()
    }

    /// A command over several of this client's plain keys.
    fn plain_command(&self, name: &str, slots: &[u32]) -> Frame {
        let mut parts = Vec::with_capacity(slots.len() + 1);
        parts.push(name.to_owned());
        parts.extend(slots.iter().map(|slot| plain_key(self.plain.key(*slot))));
        command(&parts.iter().map(String::as_str).collect::<Vec<_>>())
    }

    /// Draws one operation, and what its reply will be worth.
    ///
    /// `sent` is the instant the burst this belongs to was composed at, which
    /// is what every deadline here is measured from; `seq` is the client's
    /// own operation counter, which goes into written values so a value that
    /// turns up under the wrong key is visible as such.
    fn compose(&self, rng: &mut ChaCha8Rng, sent: Instant, seq: u32) -> Op {
        // Drawn here rather than inside each family so the roll is one draw
        // whichever family it lands in: a helper that rolled again would make
        // the stream a function of how the arms happen to be grouped.
        let roll = rng.random_range(0..100u32);
        match roll {
            0..COUNTER_OPS => {
                let key = rng.random_range(0..self.counter_keys);
                let delta = rng.random_range(-10..=10i64);
                Op {
                    frame: command(&["INCRBY", &counter_key(key), &delta.to_string()]),
                    check: Check::Counter(delta),
                    form: contract::FORM_INCRBY,
                }
            }
            COUNTER_OPS..PLAIN_END => self.compose_plain(roll, rng, seq),
            _ => self.compose_volatile(roll, rng, sent, seq),
        }
    }

    /// An operation on a plain key: no deadline ever, and a model that knows
    /// the exact bytes.
    fn compose_plain(&self, roll: u32, rng: &mut ChaCha8Rng, seq: u32) -> Op {
        match roll {
            COUNTER_OPS..31 => self.compose_plain_set(roll, rng, seq),
            31..38 => {
                let slot = self.plain.pick(rng);
                Op {
                    frame: command(&["GET", &plain_key(self.plain.key(slot))]),
                    check: Check::PlainGet { slot },
                    form: contract::FORM_GET,
                }
            }
            38..42 => {
                let slots = self.several(rng);
                Op {
                    frame: self.plain_command("DEL", &slots),
                    check: Check::PlainDel { slots },
                    form: contract::FORM_DEL,
                }
            }
            42..46 => {
                let slots = self.several(rng);
                Op {
                    frame: self.plain_command("EXISTS", &slots),
                    check: Check::PlainExists { slots },
                    form: contract::FORM_EXISTS,
                }
            }
            46..50 => {
                let slots = self.several(rng);
                Op {
                    frame: self.plain_command("MGET", &slots),
                    check: Check::PlainMGet { slots },
                    form: contract::FORM_MGET,
                }
            }
            50..52 => {
                let slot = self.plain.pick(rng);
                Op {
                    frame: command(&["TYPE", &plain_key(self.plain.key(slot))]),
                    check: Check::PlainType { slot },
                    form: contract::FORM_TYPE,
                }
            }
            _ => {
                let slot = self.plain.pick(rng);
                Op {
                    frame: command(&["STRLEN", &plain_key(self.plain.key(slot))]),
                    check: Check::PlainStrLen { slot },
                    form: contract::FORM_STRLEN,
                }
            }
        }
    }

    /// A `SET` of a plain key, in whichever of the algebra's forms the roll
    /// landed on.
    ///
    /// Split out of [`Model::compose_plain`] rather than drawn separately, and
    /// the roll is the one already made: a helper that rolled again would make
    /// the stream a function of how the arms happen to be grouped, which is
    /// the same rule [`Model::compose`] states for the families.
    ///
    /// What is *not* here is `EXAT` and `PXAT`, and it never will be — see
    /// [`crate::contract`] for why a client with no wall clock cannot name an
    /// absolute deadline.
    fn compose_plain_set(&self, roll: u32, rng: &mut ChaCha8Rng, seq: u32) -> Op {
        match roll {
            COUNTER_OPS..24 => {
                let slot = self.plain.pick(rng);
                let value = format!("{seq}@{}", self.plain.key(slot));
                Op {
                    frame: command(&["SET", &plain_key(self.plain.key(slot)), &value]),
                    check: Check::PlainSet {
                        slot,
                        value: value.into_bytes(),
                    },
                    form: contract::FORM_SET,
                }
            }
            24..28 => {
                // The two conditions are one arm, because they are one
                // command with the sense of a single test flipped, and the
                // model predicts both from the same fact. Splitting them
                // would be two arms that had to agree about what presence
                // means.
                let only_if_present = roll >= 26;
                // One of the two `NX` rolls spells it `SETNX` instead, for
                // `SETEX`'s reason and one more. `SETNX` reaches the parser
                // through a different table entry, so a bug in that entry is
                // one no `SET … NX` can find; and it answers the decision as
                // an integer, so a server that took the right decision and
                // put it in the `SET` spelling's frame is caught here and
                // nowhere else. It takes a roll off `SET … NX` rather than
                // adding one, so the arm's share of the hundred is unchanged
                // and only what one roll puts on the wire moves.
                let old_name = roll == 25;
                let slot = self.plain.pick(rng);
                let value = format!("{seq}@{}", self.plain.key(slot));
                let key = plain_key(self.plain.key(slot));
                let frame = if old_name {
                    command(&["SETNX", &key, &value])
                } else {
                    command(&[
                        "SET",
                        &key,
                        &value,
                        if only_if_present { "XX" } else { "NX" },
                    ])
                };
                Op {
                    frame,
                    check: Check::PlainSetCond {
                        slot,
                        value: value.into_bytes(),
                        only_if_present,
                        reply: if old_name {
                            CondReply::OneOrZero
                        } else {
                            CondReply::OkOrNull
                        },
                    },
                    form: if old_name {
                        contract::FORM_SETNX
                    } else if only_if_present {
                        contract::FORM_SET_XX
                    } else {
                        contract::FORM_SET_NX
                    },
                }
            }
            28..30 => {
                let slot = self.plain.pick(rng);
                let value = format!("{seq}@{}", self.plain.key(slot));
                Op {
                    frame: command(&["SET", &plain_key(self.plain.key(slot)), &value, "GET"]),
                    check: Check::PlainSetGet {
                        slot,
                        value: value.into_bytes(),
                    },
                    form: contract::FORM_SET_GET,
                }
            }
            _ => {
                let slot = self.plain.pick(rng);
                let value = format!("{seq}@{}", self.plain.key(slot));
                Op {
                    frame: command(&["SET", &plain_key(self.plain.key(slot)), &value, "KEEPTTL"]),
                    check: Check::PlainSet {
                        slot,
                        value: value.into_bytes(),
                    },
                    form: contract::FORM_SET_KEEPTTL,
                }
            }
        }
    }

    /// An operation on a volatile key: always a deadline, and a model that
    /// knows when — not what.
    fn compose_volatile(&self, roll: u32, rng: &mut ChaCha8Rng, sent: Instant, seq: u32) -> Op {
        match roll {
            PLAIN_END..70 => {
                let slot = self.volatile.pick(rng);
                let deadline = &DEADLINES[rng.random_range(0..DEADLINES.len())];
                let key = volatile_key(self.volatile.key(slot));
                let value = format!("{seq}@{}", self.volatile.key(slot));
                let argument = deadline.argument.to_string();
                let frame = match deadline.spelling {
                    Spelling::SetOption(option) => {
                        command(&["SET", &key, &value, option, &argument])
                    }
                    Spelling::SetEx => command(&["SETEX", &key, &argument, &value]),
                    Spelling::PSetEx => command(&["PSETEX", &key, &argument, &value]),
                };
                Op {
                    frame,
                    check: Check::VolatileSet {
                        slot,
                        deadline: sent + Duration::from_millis(deadline.millis),
                    },
                    // Carried by the deadline rather than derived from its
                    // spelling here: the two would then be two places to keep
                    // in step, and the one that drifted would be the one
                    // nothing reads.
                    form: deadline.form,
                }
            }
            70..82 => {
                let slot = self.volatile.pick(rng);
                Op {
                    frame: command(&["GET", &volatile_key(self.volatile.key(slot))]),
                    check: Check::VolatileGet { slot },
                    form: contract::FORM_GET,
                }
            }
            82..90 => {
                let slot = self.volatile.pick(rng);
                Op {
                    frame: command(&[
                        "EXPIRE",
                        &volatile_key(self.volatile.key(slot)),
                        &EXPIRE_SECONDS.to_string(),
                    ]),
                    check: Check::VolatileExpire {
                        slot,
                        deadline: sent + Duration::from_secs(EXPIRE_SECONDS),
                    },
                    form: contract::FORM_EXPIRE,
                }
            }
            90..94 => {
                let slot = self.volatile.pick(rng);
                Op {
                    frame: command(&[
                        "PEXPIRE",
                        &volatile_key(self.volatile.key(slot)),
                        &PEXPIRE_MILLIS.to_string(),
                    ]),
                    check: Check::VolatileExpire {
                        slot,
                        deadline: sent + Duration::from_millis(PEXPIRE_MILLIS),
                    },
                    form: contract::FORM_PEXPIRE,
                }
            }
            94..97 => {
                let slot = self.volatile.pick(rng);
                Op {
                    frame: command(&["PERSIST", &volatile_key(self.volatile.key(slot))]),
                    check: Check::VolatilePersist { slot },
                    form: contract::FORM_PERSIST,
                }
            }
            _ => {
                let slot = self.volatile.pick(rng);
                Op {
                    frame: command(&["TTL", &volatile_key(self.volatile.key(slot))]),
                    check: Check::Ignored,
                    form: contract::FORM_TTL,
                }
            }
        }
    }

    /// Reads a burst's replies: updates the model, and reports what the
    /// invariants make of them.
    ///
    /// In order, because the burst was applied in order — a `SET` and a later
    /// `GET` of the same key inside one burst reach their shard that way
    /// round, so the model the `GET` is judged against is the one its own
    /// predecessors left.
    fn observe(&mut self, replies: &[Frame], checks: &[Check], sent: Instant, received: Instant) {
        for (reply, check) in replies.iter().zip(checks) {
            match check {
                Check::Ignored => {}
                // Only an acknowledged increment is owed to us. Anything else
                // — an error frame, a reply shape we did not expect — is not
                // a promise the server made, so counting it would manufacture
                // a violation the system never committed. Every arm below
                // reads its reply the same way.
                Check::Counter(delta) => {
                    if matches!(reply, Frame::Integer(_)) {
                        lock(&self.shared.tally).expected += delta;
                    }
                }
                Check::PlainSet { slot, value } => {
                    self.plain_state[*slot as usize] = match reply {
                        Frame::Simple(text) if text == "OK" => Known::Value(value.clone()),
                        _ => Known::Nothing,
                    };
                }
                Check::PlainSetCond {
                    slot,
                    value,
                    only_if_present,
                    reply: spelling,
                } => {
                    let held = self.plain_state[*slot as usize].clone();
                    // The two answers a condition can give, in whichever type
                    // this spelling gives them. Anything else is the server
                    // declining to run the command at all, which is no
                    // statement about the key and leaves the model with
                    // nothing to hold — and a `SETNX` answering `+OK` lands
                    // there too, which is the point of reading the frame the
                    // spelling names rather than either frame that means yes.
                    let (took, refused) = match spelling {
                        CondReply::OkOrNull => (
                            matches!(reply, Frame::Simple(text) if text == "OK"),
                            matches!(reply, Frame::Null),
                        ),
                        CondReply::OneOrZero => (
                            matches!(reply, Frame::Integer(1)),
                            matches!(reply, Frame::Integer(0)),
                        ),
                    };
                    let present = match held {
                        Known::Nothing => None,
                        Known::Absent => Some(false),
                        Known::Value(_) => Some(true),
                    };
                    if let Some(present) = present
                        && (took || refused)
                    {
                        let mut tally = lock(&self.shared.tally);
                        tally.plain_checks += 1;
                        if took != (present == *only_if_present) {
                            tally.plain_mismatches += 1;
                        }
                    }
                    self.plain_state[*slot as usize] = if took {
                        Known::Value(value.clone())
                    } else if refused {
                        // The condition did not hold, so nothing was written
                        // and the key is exactly what it was.
                        held
                    } else {
                        Known::Nothing
                    };
                }
                Check::PlainSetGet { slot, value } => {
                    // The reply is the key's *previous* value, so it answers
                    // the question a `GET` would have — held against the model
                    // by the same code, so the two cannot disagree about what
                    // agreement means.
                    self.check_plain(*slot, reply);
                    self.plain_state[*slot as usize] = match reply {
                        // A value or its absence is the command having run.
                        Frame::Bulk(_) | Frame::Null => Known::Value(value.clone()),
                        _ => Known::Nothing,
                    };
                }
                Check::PlainDel { slots } => self.check_plain_fan_out(slots, reply, true),
                Check::PlainExists { slots } => self.check_plain_fan_out(slots, reply, false),
                Check::PlainMGet { slots } => self.check_plain_mget(slots, reply),
                Check::PlainGet { slot } => self.check_plain(*slot, reply),
                Check::PlainType { slot } => self.check_plain_shape(
                    *slot,
                    reply,
                    &Frame::Simple("none".into()),
                    &Frame::Simple("string".into()),
                ),
                Check::PlainStrLen { slot } => {
                    let held = match &self.plain_state[*slot as usize] {
                        Known::Value(value) => value.len(),
                        _ => 0,
                    };
                    let held = i64::try_from(held).expect("a written value fits an i64 length");
                    self.check_plain_shape(*slot, reply, &Frame::Integer(0), &Frame::Integer(held));
                }
                Check::VolatileSet { slot, deadline } => {
                    self.deadlines[*slot as usize] = match reply {
                        Frame::Simple(text) if text == "OK" => Some(*deadline),
                        _ => None,
                    };
                }
                // A zero says the key was already gone, which is no statement
                // about when it will next die: the model gives up on it until
                // its owner writes it again.
                Check::VolatileExpire { slot, deadline } => {
                    self.deadlines[*slot as usize] = match reply {
                        Frame::Integer(1) => Some(*deadline),
                        _ => None,
                    };
                }
                // Whatever it answered, the key carries no deadline
                // afterwards: `1` removed one, and `0` says there was none to
                // remove or no key to remove it from. So the model predicts no
                // death for it until its owner writes it with one again — and
                // it asserts nothing about the key in the meantime, because
                // the volatile family's model holds deadlines and not values.
                Check::VolatilePersist { slot } => self.deadlines[*slot as usize] = None,
                Check::VolatileGet { slot } => self.check_volatile(*slot, reply, sent, received),
            }
        }
    }

    /// Holds a variadic `DEL` or `EXISTS` against the model, and — for `DEL` —
    /// applies it.
    ///
    /// The count is the whole of what the fan-out returns, and it is exactly
    /// predictable here because the keys belong to this client alone. The two
    /// commands count differently on a repeated key, which is the point of
    /// letting the draw repeat one: `DEL k k` removes it once, `EXISTS k k`
    /// finds it twice.
    ///
    /// **Weakened under a ceiling**, and this is the one check where the
    /// weakening is not merely an excused `nil`: any of the keys named may
    /// have been reclaimed, so the model's count becomes an upper bound and
    /// anything from zero up to it agrees. A count *above* it is still a
    /// mismatch — eviction can only ever remove keys, so a fan-out finding
    /// more than this client wrote is finding something nobody wrote. What is
    /// given up is the exactness, and it is given up only on the shape that
    /// has a ceiling; every other shape decides this as strictly as before.
    fn check_plain_fan_out(&mut self, slots: &[u32], reply: &Frame, removing: bool) {
        let mut counted = 0i64;
        let mut predictable = true;
        let mut seen: Vec<u32> = Vec::with_capacity(slots.len());
        for slot in slots {
            match self.plain_state[*slot as usize] {
                Known::Nothing => predictable = false,
                Known::Absent => {}
                // A removal takes the key out, so naming it twice can only
                // remove it once; a count sees it every time it is named.
                Known::Value(_) if removing && seen.contains(slot) => {}
                Known::Value(_) => counted += 1,
            }
            seen.push(*slot);
        }

        if predictable {
            let agrees = if self.evictable {
                matches!(reply, Frame::Integer(n) if (0..=counted).contains(n))
            } else {
                *reply == Frame::Integer(counted)
            };
            let mut tally = lock(&self.shared.tally);
            tally.plain_checks += 1;
            if !agrees {
                tally.plain_mismatches += 1;
            }
        }

        if removing {
            let removed = matches!(reply, Frame::Integer(_));
            for slot in slots {
                self.plain_state[*slot as usize] = if removed {
                    Known::Absent
                } else {
                    Known::Nothing
                };
            }
        }
    }

    /// Holds an `MGET` of one to three plain keys against the model.
    ///
    /// The length is checked as strictly as the contents, and that is the
    /// half worth stating: `MGET` is the only command here whose reply
    /// *shape* is a function of how many replies the fan-out gathered, so a
    /// gather that dropped one answers a shorter array rather than a wrong
    /// one. A real client pairs the array with the keys it sent — django's
    /// `get_many` zips them — and a short array quietly becomes a run of
    /// cache misses instead of an error anybody notices. Nothing else in this
    /// harness can see that, because every other fan-out folds down to a
    /// single integer.
    ///
    /// A repeated key is not special here as it is for `DEL` and `EXISTS`:
    /// each name is its own read, and reads do not consume anything.
    fn check_plain_mget(&mut self, slots: &[u32], reply: &Frame) {
        let mut evicted = 0;
        let agrees = match reply {
            Frame::Array(values) if values.len() == slots.len() => {
                let evictable = self.evictable;
                let mut agrees = true;
                for (slot, value) in slots.iter().zip(values) {
                    let slot = *slot as usize;
                    // Element by element, exactly as [`Model::check_plain`]
                    // does it for a single `GET`: an evictable model excuses
                    // a written key that is gone and follows the server, and
                    // a wrong *value* is never excused.
                    if evictable
                        && matches!(self.plain_state[slot], Known::Value(_))
                        && matches!(value, Frame::Null)
                    {
                        self.plain_state[slot] = Known::Absent;
                        evicted += 1;
                        continue;
                    }
                    agrees &= match (&self.plain_state[slot], value) {
                        // Unpredictable on its own, and the element beside it
                        // still is: one unknown key does not excuse the rest
                        // of the array.
                        (Known::Nothing, _) => true,
                        (Known::Absent, value) => matches!(value, Frame::Null),
                        (Known::Value(expected), Frame::Bulk(got)) => got == expected,
                        (Known::Value(_), _) => false,
                    };
                }
                agrees
            }
            _ => false,
        };
        let mut tally = lock(&self.shared.tally);
        tally.plain_checks += 1;
        tally.evictions_observed += evicted;
        if !agrees {
            tally.plain_mismatches += 1;
        }
    }

    /// Holds a reply about a plain key's *shape* — its type or its length —
    /// against the model.
    ///
    /// One helper for `TYPE` and `STRLEN` because they ask the same question
    /// in two vocabularies: presence, and what presence implies. Each
    /// caller supplies the answer it expects for an absent key and the one it
    /// expects for the value the model holds, which is the whole of the
    /// difference between them.
    fn check_plain_shape(&self, slot: u32, reply: &Frame, absent: &Frame, present: &Frame) {
        let agrees = match &self.plain_state[slot as usize] {
            Known::Nothing => return,
            Known::Absent => reply == absent,
            // Under a ceiling the key may have been reclaimed between the
            // write and this question, so both answers are legitimate. The
            // model is not updated from it: `TYPE` and `STRLEN` do not
            // distinguish a reclaimed key from one that was never there, and
            // a `GET` will say which soon enough.
            Known::Value(_) if self.evictable => reply == present || reply == absent,
            Known::Value(_) => reply == present,
        };
        let mut tally = lock(&self.shared.tally);
        tally.plain_checks += 1;
        if !agrees {
            tally.plain_mismatches += 1;
        }
    }

    /// Asks the node what it is holding, and holds it to its ceiling.
    ///
    /// A no-op with no ceiling, so a client on any other shape sends nothing:
    /// this is the eviction shape's frame and it does not belong in the
    /// traces every other shape produces.
    async fn probe_ceiling(&self, conn: &mut Conn) -> turmoil::Result<()> {
        let Some(ceiling) = self.ceiling else {
            return Ok(());
        };
        self.record_form(contract::FORM_INFO_MEMORY);
        let replies = conn.request_many(&[command(&["INFO", "memory"])]).await?;
        check_ceiling(&replies[0], ceiling, &self.shared);
        Ok(())
    }

    /// Holds a `GET` of a plain key against what this client last wrote.
    ///
    /// Sound because the family is partitioned: nothing else in the
    /// simulation writes this key, so "what I last wrote" is the whole truth
    /// about it and no schedule excuses a difference. Strict about the reply
    /// shape for the same reason the volatile check is lenient about it —
    /// there, an error frame is a question left unanswered; here, a `GET` of
    /// a key this client owns has no legitimate way to fail.
    fn check_plain(&mut self, slot: u32, reply: &Frame) {
        // The one thing a ceiling excuses, and it is excused before anything
        // else is judged: a key this client wrote is simply gone. The model
        // follows the server rather than keeping a value it now knows is not
        // there, so the next read of the same slot is decided against
        // `Absent` and is exact again.
        if self.evictable
            && matches!(self.plain_state[slot as usize], Known::Value(_))
            && matches!(reply, Frame::Null)
        {
            self.plain_state[slot as usize] = Known::Absent;
            {
                let mut tally = lock(&self.shared.tally);
                tally.plain_checks += 1;
                tally.evictions_observed += 1;
            }
            return;
        }
        let agrees = match (&self.plain_state[slot as usize], reply) {
            (Known::Nothing, _) => return,
            (Known::Absent, reply) => matches!(reply, Frame::Null),
            (Known::Value(value), Frame::Bulk(got)) => got == value,
            (Known::Value(_), _) => false,
        };
        let mut tally = lock(&self.shared.tally);
        tally.plain_checks += 1;
        if !agrees {
            tally.plain_mismatches += 1;
        }
    }

    /// Holds a `GET` of a volatile key against the deadline this client asked
    /// for.
    ///
    /// `sent` is a lower bound on when the server ran the read and `received`
    /// an upper bound, so each half takes the end that makes it conservative:
    /// a value is called stale only when even the *earliest* the read could
    /// have run was past the deadline, and an absence spurious only when even
    /// the *latest* it could have run was before it. Between the two the
    /// client says nothing — which is not a pass, and is why what was decided
    /// is counted beside what was violated.
    ///
    /// The two ends take different bands, [`STALE_SLACK`] and [`LIVE_SLACK`],
    /// because they are not owed the same thing; each constant carries its own
    /// derivation.
    ///
    /// **Under a ceiling only the *stale* end still decides.** Eviction
    /// removes keys and never resurrects one, so a value served past its
    /// deadline is a missed expiry whatever the node's memory is doing — that
    /// half is untouched. An *absence* inside the live band, though, is now
    /// explicable: the node may have reclaimed the key. So it is diverted to
    /// [`SimOutcome::evictions_observed`] rather than counted as a decided
    /// check the invariant happened to pass, and the model forgets the
    /// deadline so one reclaimed key is observed once and not on every read
    /// that follows. A *present* key inside the band is still a decided
    /// check, which is what keeps `alive_checks` a count of what the
    /// invariant actually settled.
    fn check_volatile(&mut self, slot: u32, reply: &Frame, sent: Instant, received: Instant) {
        let Some(deadline) = self.deadlines[slot as usize] else {
            return;
        };
        // Only a value or its absence answers the question. Anything else is
        // the server declining to, and counting it would inflate the very
        // number that says this invariant ran.
        let present = match reply {
            Frame::Bulk(_) => true,
            Frame::Null => false,
            _ => return,
        };
        if self.evictable && !present && received + LIVE_SLACK < deadline {
            self.deadlines[slot as usize] = None;
            lock(&self.shared.tally).evictions_observed += 1;
            return;
        }
        let mut tally = lock(&self.shared.tally);
        if sent > deadline + STALE_SLACK {
            tally.dead_checks += 1;
            if present {
                tally.stale_reads += 1;
            }
        } else if received + LIVE_SLACK < deadline {
            tally.alive_checks += 1;
            if !present {
                tally.spurious_deaths += 1;
            }
        }
    }

    /// The last thing a client does: wait out the deadlines it asked for, as
    /// far as [`SETTLE_CAP`] allows, then read back everything it owns.
    ///
    /// This is where the active sweep is under test. The workload is over in
    /// a fraction of a simulated second and the deadlines it handed out are
    /// longer than that, so without the wait a run would end with the
    /// keyspace full of entries nothing had reclaimed and nothing would ever
    /// have looked. After it, two things must hold at once: every volatile
    /// key whose deadline was waited out is gone, and every plain key — which
    /// no deadline was ever put on — is exactly what its owner wrote. A sweep
    /// that eats the living fails the second; a server that spares the dead
    /// fails the first.
    async fn settle(&mut self, conn: &mut Conn, depth: usize) -> turmoil::Result<()> {
        if let Some(last) = self.deadlines.iter().flatten().max() {
            // A millisecond past the staleness band — this is a wait for
            // deadlines to pass, so it is that side's band it has to clear —
            // leaving a deadline waited out decidedly behind us and the read
            // below counting as a check. Never longer than [`SETTLE_CAP`],
            // whose documentation says what the wait costs and what capping it
            // gives up.
            let until =
                (*last).min(Instant::now() + SETTLE_CAP) + STALE_SLACK + Duration::from_millis(1);
            tokio::time::sleep_until(until).await;
        }

        let mut frames = Vec::new();
        let mut checks = Vec::new();
        for slot in 0..self.volatile.len {
            frames.push(command(&["GET", &volatile_key(self.volatile.key(slot))]));
            checks.push(Check::VolatileGet { slot });
        }
        for slot in 0..self.plain.len {
            frames.push(command(&["GET", &plain_key(self.plain.key(slot))]));
            checks.push(Check::PlainGet { slot });
        }
        // The one keyspace-wide command a client here may send, and the only
        // one in the workload that reaches every shard from a single request.
        // What it answers is the whole simulation's keyspace, which no client
        // owns and none can predict, so it carries no claim — it is here
        // because the broadcast path is otherwise driven only by the service
        // layer's own tests, never by a client competing with fifteen others
        // for the same executors. Sent once per client rather than drawn into
        // the burst schedule: at one envelope per shard it would otherwise
        // decide a run's cost by itself.
        frames.push(command(&["DBSIZE"]));
        checks.push(Check::Ignored);
        self.record_form(contract::FORM_GET);
        self.record_form(contract::FORM_DBSIZE);

        for (burst, checks) in frames.chunks(depth).zip(checks.chunks(depth)) {
            let sent = Instant::now();
            let replies = conn.request_many(burst).await?;
            self.observe(&replies, checks, sent, Instant::now());
        }
        self.probe_ceiling(conn).await?;
        Ok(())
    }

    /// Writes a set of keys nothing will touch again, walks its own family
    /// while churning the rest of it, and holds the walk to its guarantee.
    ///
    /// The concurrent case, and the one the quiescent oracle cannot reach.
    /// What is quiescent here is a *set*, not the keyspace and not even this
    /// client's family: the stable keys are written before the walk starts
    /// and nothing touches them until it ends, while the same client writes
    /// and deletes other keys of the same family between the walk's steps and
    /// the other clients mutate everything else. Growth is what makes a
    /// shard's table double with the walk in flight, which is the one case a
    /// reverse-binary cursor exists to survive and the one no quiescent
    /// assertion can produce.
    ///
    /// Five claims, and they are the guarantee split into the parts a
    /// concurrent walk can still make:
    ///
    /// - **No phantom.** Every key any step returns is one this client wrote
    ///   — a stable key or a churn key. A name nothing here ever sent is a
    ///   walk answering out of another family, another client's slice, or
    ///   nowhere at all.
    /// - **Bounded.** A cycle-completing walk finishes inside
    ///   [`WALK_CYCLE_STEP_BOUND`] steps. Exceeding it is a cursor that has
    ///   stopped converging, and it is reported with the step count rather
    ///   than as a run that hung.
    /// - **At least once.** A walk that reached the end of its cycle returned
    ///   every stable key. Only a completed walk can claim this, which is why
    ///   the prefix shape does not — see
    ///   [`SimConfig::concurrent_scan_cycle`].
    /// - **`KEYS` does not repeat, and is exact.** The closing `KEYS` is one
    ///   round trip and complete by construction, taken once the churn has
    ///   stopped, so the model knows precisely what the family holds: every
    ///   stable key, plus every churn key whose write was acknowledged and
    ///   whose removal was not. `SCAN` may return a key twice and this may
    ///   not.
    /// - **Shard-monotonic.** A call crosses shards in order and hands back
    ///   wherever it stopped, so within one cycle the shard half of every
    ///   cursor returned never decreases, and every non-zero cursor names a
    ///   shard this node has. The rest of that claim — that `0` arrives only
    ///   after the *last* shard — is not readable from a cursor, because a
    ///   call may cross any number of shards before it stops and the client
    ///   sees only where it stopped. What carries it is at-least-once: a `0`
    ///   handed back before the last shard was walked is a cycle that left
    ///   keys unreturned. See `Plant::CrossingSkipsShard`, which is exactly
    ///   that defect.
    ///
    /// **What the prefix shape's steps can and cannot see, measured rather
    /// than assumed.** A step is a shard at most, so two steps cover two of a
    /// thousand and most of these walks return nothing at all: on the swept
    /// shape, two clients in a hundred and twenty-eight had a key of their own
    /// in the stretch they walked. That is thin per seed and not thin across a
    /// sweep, and it is why the result-level claim every seed rests on is the
    /// closing `KEYS` rather than the steps. What the steps carry
    /// every time is the rest of it — a well-formed reply, a cursor that
    /// moves, and nothing returned that belongs to anyone else — under a
    /// schedule, which is coverage `SCAN` had nowhere before. Widening it is
    /// not a matter of taking more steps: a spent shard hands back the next
    /// one's start rather than continuing into it, so a step is a shard
    /// whatever its bucket budget, and the fix is to let one call cross that
    /// boundary.
    ///
    /// The two `SCAN` forms are split by client id rather than alternated
    /// within a walk. A step carrying no `COUNT` takes the server's own
    /// bucket budget, which is large enough to finish a small shard's table
    /// in one call; a walk built out of those has no cursor between its steps
    /// for anything to happen underneath. Alternating would give every walk
    /// half of that and leave none of them stepping bucket by bucket, so the
    /// choice is per client: both parse paths are exercised in every run, and
    /// the odd-numbered clients are the ones whose cursor is genuinely in
    /// flight.
    async fn walk(&self, conn: &mut Conn, cfg: &SimConfig, depth: usize) -> turmoil::Result<()> {
        let names: Vec<String> = (0..WALK_KEYS).map(|slot| walk_key(self.id, slot)).collect();
        // The value is the key: nothing reads it back, and a value that names
        // its own key is what makes a mis-shelved one legible if something
        // ever does.
        let writes: Vec<Frame> = names
            .iter()
            .map(|name| command(&["SET", name, name]))
            .collect();

        self.record_form(contract::FORM_SET);
        self.record_form(contract::FORM_DEL);
        self.record_form(contract::FORM_KEYS);
        self.record_form(if self.names_a_count() {
            contract::FORM_SCAN_MATCH_COUNT
        } else {
            contract::FORM_SCAN_MATCH
        });

        let mut stable = BTreeSet::new();
        for (batch, burst) in writes.chunks(depth).enumerate() {
            for (offset, reply) in conn.request_many(burst).await?.into_iter().enumerate() {
                // Only an acknowledged write is a key we may insist on. A
                // refusal is a key that is legitimately absent, and demanding
                // it back would manufacture a violation.
                if reply == Frame::Simple("OK".into()) {
                    stable.insert(names[batch * depth + offset].clone().into_bytes());
                }
            }
        }

        let walk = self.walk_the_family(conn, cfg, &stable).await?;
        let mut present = stable;
        present.extend(walk.present.iter().cloned());
        lock(&self.shared.walk).extend(present.iter().cloned());

        let reply = conn
            .request_many(&[command(&["KEYS", &walk_pattern(self.id)])])
            .await?;
        {
            let mut tally = lock(&self.shared.tally);
            tally.walk_checks += 2;
            if !walk.holds {
                tally.walk_mismatches += 1;
            }
            // `Some((set, false))` is the only shape that can agree: anything
            // else is a malformed reply or a key returned twice, and `KEYS`
            // promises neither.
            //
            // Under a ceiling the set becomes an upper bound and the equality
            // a subset: a key this client wrote may have been reclaimed since,
            // and nothing about the walk can tell that from a key the server
            // lost. What the check keeps is the half eviction cannot excuse —
            // no name that was never written, and no name returned twice.
            let agrees = match listed_keys(&reply[0]) {
                Some((keys, false)) if self.evictable => keys.is_subset(&present),
                listed => listed == Some((present, false)),
            };
            if !agrees {
                tally.walk_mismatches += 1;
            }
        }
        Ok(())
    }

    /// Whether this client's walk names a `COUNT` on the wire.
    ///
    /// Split by client id rather than alternated within one walk. See
    /// [`Model::walk`] for why: a step with no `COUNT` takes the server's own
    /// bucket budget and can finish a small shard's table in one call, so a
    /// walk built out of them has no cursor in flight between its steps.
    const fn names_a_count(&self) -> bool {
        !self.id.is_multiple_of(2)
    }

    /// Drives the `SCAN` half of [`Model::walk`] and reports what it found.
    ///
    /// Every burst is churn first and the step last, in one write, so the
    /// step meets a family that has changed since the step before it. Which
    /// of the two the server reaches first is not this client's to decide and
    /// is not asserted on — the guarantee is stated over the stable set
    /// precisely because the rest of the family has no predictable answer.
    async fn walk_the_family(
        &self,
        conn: &mut Conn,
        cfg: &SimConfig,
        stable: &BTreeSet<Vec<u8>>,
    ) -> turmoil::Result<WalkOutcome> {
        let pattern = walk_pattern(self.id);
        let count = WALK_STEP_COUNT.to_string();

        // Churn keys whose write was acknowledged, in the order they were
        // written, and how many of them a removal has been aimed at. The
        // index is what makes the removals go oldest first and never twice at
        // the same key; `gone` is what says which of them the server
        // confirmed, since only a confirmed removal takes a key out of the
        // family the closing `KEYS` is held to.
        let mut written: Vec<String> = Vec::new();
        let mut attempted = 0usize;
        let mut gone: BTreeSet<Vec<u8>> = BTreeSet::new();
        // Every churn name this client has *sent*, acknowledged or not. The
        // no-phantom check is against this rather than against what was
        // acknowledged: a write whose reply said nothing may still have
        // landed, and a walk returning it is not the failure being looked for.
        let mut sent: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut next_slot = WALK_KEYS;

        let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut cursor = 0u64;
        // The shard the last cursor named. A walk starts at cursor 0, which is
        // shard 0's start, so a walk that has taken no step yet is already at
        // the floor the check holds every later cursor to.
        let mut last_shard = 0u64;
        let mut steps = 0u64;
        let mut holds = true;

        let completed = loop {
            let mut burst = Vec::new();
            let mut fresh = Vec::new();
            for _ in 0..WALK_CHURN_WRITES {
                let name = walk_key(self.id, next_slot);
                next_slot += 1;
                burst.push(command(&["SET", &name, &name]));
                fresh.push(name);
            }
            let deleting = (written.len() - attempted).min(WALK_CHURN_DELETES as usize);
            let targets: Vec<String> = written[attempted..attempted + deleting].to_vec();
            attempted += deleting;
            for name in &targets {
                burst.push(command(&["DEL", name]));
            }
            let cursor_text = cursor.to_string();
            burst.push(if self.names_a_count() {
                command(&["SCAN", &cursor_text, "MATCH", &pattern, "COUNT", &count])
            } else {
                command(&["SCAN", &cursor_text, "MATCH", &pattern])
            });

            let replies = conn.request_many(&burst).await?;
            steps += 1;
            for (name, reply) in fresh.iter().zip(&replies) {
                sent.insert(name.clone().into_bytes());
                if *reply == Frame::Simple("OK".into()) {
                    written.push(name.clone());
                }
            }
            for (name, reply) in targets.iter().zip(&replies[fresh.len()..]) {
                // A refusal removes nothing — the shard declines before it
                // touches the keyspace — so a removal only counts once the
                // server has said it happened.
                if matches!(reply, Frame::Integer(_)) {
                    gone.insert(name.clone().into_bytes());
                }
            }

            let Some((next, keys)) = scan_reply(replies.last().expect("the step is in the burst"))
            else {
                holds = false;
                break false;
            };
            // Repeats are `SCAN`'s to make, so the union across steps is what
            // the guarantee is about and a key returned twice is not a
            // finding here.
            if !keys
                .iter()
                .all(|key| stable.contains(key) || sent.contains(key))
            {
                holds = false;
            }
            seen.extend(keys);
            // The shard half of the cursor, read the way the edge packs it —
            // see `WALK_CURSOR_SHARD_SHIFT`. `0` is the end of the cycle and
            // names no shard, so it is the loop's business below and not this
            // check's.
            if next != 0 {
                let shard = next >> WALK_CURSOR_SHARD_SHIFT;
                if shard < last_shard || shard >= u64::from(cfg.shards) {
                    holds = false;
                }
                last_shard = shard;
            }
            cursor = next;

            if cursor == 0 {
                break true;
            }
            if cfg.concurrent_scan_cycle {
                if steps >= WALK_CYCLE_STEP_BOUND {
                    holds = false;
                    break false;
                }
            } else if steps >= WALK_PREFIX_STEPS {
                break false;
            }
        };

        // Only a walk that reached the end of its cycle saw the whole family,
        // so only that walk is held to having returned all of it — and only
        // on a node that cannot have reclaimed a stable key underneath it.
        // Under a ceiling, at-least-once is exactly the claim eviction is
        // allowed to break; what survives is the rest, and every one of those
        // claims is asserted above whatever the shape.
        if completed && !self.evictable && !stable.iter().all(|key| seen.contains(key)) {
            holds = false;
        }

        Ok(WalkOutcome {
            holds,
            present: written
                .into_iter()
                .map(String::into_bytes)
                .filter(|name| !gone.contains(name))
                .collect(),
        })
    }
}

/// What the `SCAN` half of a client's walk found.
struct WalkOutcome {
    /// Whether every claim [`Model::walk`] lists held.
    holds: bool,
    /// The churn keys the client believes it left behind: written, and not
    /// removed since.
    present: BTreeSet<Vec<u8>>,
}

/// The cursor and the keys a `SCAN` reply carries, or `None` for a reply that
/// is not one.
fn scan_reply(reply: &Frame) -> Option<(u64, BTreeSet<Vec<u8>>)> {
    let Frame::Array(parts) = reply else {
        return None;
    };
    let [Frame::Bulk(next), keys] = parts.as_slice() else {
        return None;
    };
    let next = parse_u64(next)?;
    let (keys, _) = listed_keys(keys)?;
    Some((next, keys))
}

/// The keys a reply lists, and whether any of them was listed twice.
///
/// `None` for anything that is not an array of bulk strings — an error frame
/// included, because a walk that failed returned no keys rather than an empty
/// keyspace.
fn listed_keys(reply: &Frame) -> Option<(BTreeSet<Vec<u8>>, bool)> {
    let Frame::Array(items) = reply else {
        return None;
    };
    let mut keys = BTreeSet::new();
    let mut repeated = false;
    for item in items {
        let Frame::Bulk(key) = item else {
            return None;
        };
        repeated |= !keys.insert(key.clone());
    }
    Some((keys, repeated))
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

/// What the shards charged, over the `# Commandstats` lines of an `INFO`
/// document: microseconds and the calls they were spent over.
///
/// The edge's own names are skipped, and that is the whole of the filtering:
/// a line named in [`EDGE_NAMES`] carries a figure timed across a wait for
/// shards, which is a real elapsed simulated time and not a handler's. What
/// remains is exactly what the executors measured — see
/// [`SimOutcome::executor_usec`].
///
/// A document with no such section reports `(0, 0)`, which the caller tells
/// apart from a measured zero by the call count.
fn executor_timing(info: &Frame) -> (u64, u64) {
    let Frame::Bulk(body) = info else {
        return (0, 0);
    };
    let mut usec = 0;
    let mut calls = 0;
    for line in String::from_utf8_lossy(body).lines() {
        let Some(rest) = line.strip_prefix("cmdstat_") else {
            continue;
        };
        let Some((name, fields)) = rest.split_once(':') else {
            continue;
        };
        if seedstone_service::EDGE_NAMES.contains(&name) {
            continue;
        }
        for field in fields.split(',') {
            if let Some(value) = field.strip_prefix("usec=") {
                usec += value.parse::<u64>().unwrap_or(0);
            } else if let Some(value) = field.strip_prefix("calls=") {
                calls += value.parse::<u64>().unwrap_or(0);
            }
        }
    }
    (usec, calls)
}

/// The `evicted_keys:` figure of an `INFO stats` document, if it holds one.
fn evicted_keys(info: &Frame) -> Option<u64> {
    let Frame::Bulk(body) = info else {
        return None;
    };
    String::from_utf8_lossy(body).lines().find_map(|line| {
        line.strip_prefix("evicted_keys:")?
            .trim()
            .parse::<u64>()
            .ok()
    })
}

/// Holds an `INFO memory` document to the ceiling the shape configured.
///
/// The one invariant here that is about the *node* rather than about a key:
/// whatever the schedule, whatever was written, a node told to hold its
/// keyspace under `maxmemory` is under it whenever anyone looks. A document
/// with no `used_memory:` line at all decides nothing — that is a reply the
/// caller could not read, not a node over its ceiling.
///
/// Taken after every burst as well as at each client's settle and once at
/// rest by the verifier, because a breach is transient: the node reclaims
/// inside the command that crossed the line, so a reading taken only at the
/// end of a run would meet a node that had been over its ceiling all the way
/// through and was under it by then.
///
/// A free function rather than a method because both callers need it and only
/// one of them owns a [`Model`]: the check is about the node, not about a
/// client's keys.
fn check_ceiling(info: &Frame, ceiling: u64, shared: &Shared) {
    let Frame::Bulk(body) = info else {
        return;
    };
    let Some(used) = String::from_utf8_lossy(body).lines().find_map(|line| {
        line.strip_prefix("used_memory:")?
            .trim()
            .parse::<u64>()
            .ok()
    }) else {
        return;
    };
    let mut tally = lock(&shared.tally);
    tally.ceiling_checks += 1;
    if used > ceiling {
        tally.ceiling_breaches += 1;
    }
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

/// Reads a cursor the server issued back off the wire.
///
/// The server prints one with `u64::to_string`, so this is the exact inverse
/// and nothing more: a cursor is not a number a person typed.
fn parse_u64(bytes: &[u8]) -> Option<u64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seeds_same_hash_and_no_lost_updates() {
        let a = run_sim(&SimConfig::mini(1, 42));
        let b = run_sim(&SimConfig::mini(1, 42));
        assert_eq!(a.trace_hash, b.trace_hash, "in-process determinism");
        assert!(a.invariant_holds(), "every invariant must hold: {a:?}");
        // Without this every assertion above is vacuous: a workload that
        // acknowledged no `INCRBY` satisfies `0 == 0`, and one whose reads all
        // landed inside the band satisfies "no stale reads" without having
        // looked at one.
        assert!(
            a.invariants_were_exercised(),
            "the run decided nothing: {a:?}"
        );
        assert_eq!(
            a.expected_sum, b.expected_sum,
            "a pinned workload seed must issue the same increments"
        );
        let c = run_sim(&SimConfig::mini(1, 43));
        assert_ne!(
            a.trace_hash, c.trace_hash,
            "different sim seed, different schedule"
        );
    }

    /// The exact oracle: with nothing mutating, the model knows the whole
    /// walk family, so the assertion is set equality rather than the weaker
    /// at-least-once the concurrent case forces.
    ///
    /// This is the strongest claim available for `KEYS` and `SCAN`, and it is
    /// where a wrong matcher, a shard missing from the fan-out, an inverted
    /// filter, a cursor that stops early or a broken dedup shows up with a
    /// legible message instead of as an unreadable failing seed. It is a test
    /// rather than a sweep for the reason [`SimConfig::quiescent_walk`] gives.
    #[test]
    fn a_quiescent_walk_returns_exactly_the_keys_that_are_there() {
        let mut cfg = SimConfig::mini(1, 42);
        cfg.quiescent_walk = true;
        let outcome = run_sim(&cfg);
        assert!(outcome.invariant_holds(), "{outcome:?}");
        assert!(
            outcome.invariants_were_exercised(),
            "a run that never reached the quiescent phase proves nothing: {outcome:?}"
        );
        // Named rather than left to `invariants_were_exercised`, which is
        // satisfied by the per-client walks alone: what this test is about is
        // the two assertions the cycle adds, and a run that skipped them would
        // otherwise pass here for the wrong reason.
        assert_eq!(
            outcome.walk_checks,
            2 * u64::from(cfg.clients) + 2,
            "a walk and a KEYS per client, plus the quiescent pair: {outcome:?}"
        );
    }

    /// The walk's guarantee, held under the schedule it is stated over.
    ///
    /// The quiescent oracle above knows the whole family because nothing is
    /// mutating; this is the case it cannot reach. Every client writes a
    /// stable set, then walks its own family while writing and deleting
    /// *other* keys of that family between the walk's steps — so the table
    /// grows and rehashes with the walk in flight, which is the one thing a
    /// reverse-binary cursor exists to survive, and fifteen other clients are
    /// mutating the keyspace around it the whole time.
    ///
    /// It runs the shape the gate sweeps rather than `mini`: what is being
    /// asserted is that the invariant holds where it is actually swept, and
    /// `standard` is the only shape that is.
    #[test]
    fn a_walk_under_concurrent_writers_still_returns_what_it_must() {
        let outcome = run_sim(&SimConfig::standard(7, 11));
        assert!(outcome.invariant_holds(), "{outcome:?}");
        assert!(
            outcome.invariants_were_exercised(),
            "a run that decided nothing proves nothing: {outcome:?}"
        );
        // Two checks per client, named rather than left to the line above:
        // the walk and the `KEYS` that closes it are separate claims, and a
        // run that quietly stopped making one of them would still satisfy
        // `walk_checks > 0`.
        assert_eq!(
            outcome.walk_checks,
            2 * u64::from(SimConfig::standard(7, 11).clients),
            "one walk and one KEYS per client: {outcome:?}"
        );
    }

    #[test]
    fn the_trace_hash_is_pinned_across_processes_and_builds() {
        // The harness's product. Every other assertion about the trace compares
        // two runs of the *same* build to each other, and stays green if the
        // hash moves globally — a `cargo update` that reorders tokio's ready
        // queue or changes `rand`'s sampling would silently retire every seed
        // ever filed against this project, and nothing would say so.
        //
        // Unlike the SipHash and CRC vectors, this number has no external
        // reference to be derived from: it is definitionally whatever this
        // system computes. So it pins *stability*, not correctness, and that is
        // the whole job. A mismatch here is not a bug report — it means the
        // trace's meaning changed, and the question to answer is whether that
        // was intended. When it was — a new command kind, a new folded field,
        // or a change to *when* the workload issues what it already issued —
        // update the constant in the same commit that caused it, and say so in
        // the message. Never update it to make a red suite green.
        //
        // The third of those is the easiest to mistake for the first, and
        // `expected_sum` below is what tells them apart: it is a function of
        // the commands alone, so a hash that moved while it held still means
        // the same workload met a different schedule.
        // Repinned three times so far, each time beside the workload change
        // that moved it. First when the workload grew to the rest of the
        // one-key surface — `MGET`, `PEXPIRE`, `PERSIST`, `TYPE` and `STRLEN`
        // into the burst schedule, `DBSIZE` into the settle, and the draw
        // re-sliced to make room for them. Then when it gained the keyspace
        // walk: every client now writes a walk family and holds `KEYS` to
        // returning it exactly, the verifier drives a full `SCAN` cycle, and
        // a step's cursor, count and pattern are folded where previously only
        // its outcome was. Then when that walk was put under churn: a client
        // now steps its own family with `SCAN` while writing and deleting
        // other keys of it, so `SCAN` is on the wire in every seed rather
        // than only where a test asked for a full cycle — repinned once more
        // when that prefix was cut from four steps to two, which is a change
        // to *when* the workload issues what it already issued and moves this
        // without moving `expected_sum`. And then when the
        // `SET` algebra the client could reach went in: `NX`, `XX`, `GET` and
        // `KEEPTTL` took four rolls in a hundred off the bare `SET` and the
        // plain `GET`, so this moved and `plain_checks` rose by two — the
        // conditions and the read-and-write decide one each where the rolls
        // they took decided one each anyway. And then when one `SCAN` call
        // began crossing shards: the workload is unchanged and issues the
        // same steps in the same order, but a step that used to answer from
        // one shard now answers from as many as its budget crosses, so the
        // replies it folds are different ones. That is the fourth kind of
        // repin and the one this comment did not have — a change to *what a
        // command answers*, with `expected_sum` holding still because the
        // commands did not move. The trace folds every command's kind and
        // every reply, so an added command changes it by construction. A
        // change here with no workload or reply change beside it is a
        // regression, not a repin. The fourth kind fired a second time when
        // the walk's bucket ceiling was raised: one call now crosses more
        // shards, so the same steps in the same order fold different replies,
        // and `expected_sum` and the four counts held still again.
        //
        // And then the first kind fired: `SETEX` joined the deadlines a
        // volatile write can draw, so a roll that used to spell a deadline
        // `SET key value EX 1` now sometimes spells it `SETEX key 1 value`.
        // A new command kind on the wire, folded by its tag — the case the
        // paragraph above calls a repin by construction.
        //
        // And the first kind again, for `SETNX`: one of the two rolls that
        // spelled a conditional write `SET key value NX` now spells it
        // `SETNX key value`. The arm's four rolls in a hundred are unchanged
        // and so is the number of conditional writes issued, so this moves
        // by the new tag and the new reply frame alone — `expected_sum` and
        // all four check counts hold still, which is what says the workload
        // did not move underneath it.
        //
        // And once more for `PSETEX`, which is a repin of the first kind and
        // the second at once: an eighth deadline joins the seven a volatile
        // write can draw, so both the tags on the wire and the draw itself
        // move. It is drawn short, so `dead_checks` rises where `SETEX` had
        // raised `alive_checks` — the two positional spellings now reach one
        // half of the expiration invariant each.
        const MINI_1_42: u64 = 0x959f_0105_262d_501d;

        let outcome = run_sim(&SimConfig::mini(1, 42));
        assert_eq!(
            outcome.trace_hash, MINI_1_42,
            "the recorded trace hash moved"
        );
        // The workload behind the hash, pinned separately: the two can drift
        // apart, and a changed workload with a coincidentally equal hash is the
        // one failure the assertion above cannot see. The check counts are
        // pinned for a second reason — they are what says the expiration
        // invariants ran, and a workload that quietly stopped reaching them
        // would otherwise keep passing.
        assert_eq!(outcome.expected_sum, 63, "the recorded workload moved");
        assert_eq!(
            (
                outcome.dead_checks,
                outcome.alive_checks,
                outcome.plain_checks,
                outcome.walk_checks
            ),
            // `SETEX`'s arrival moved the first two and neither of the last
            // two: the seventh deadline is a second long, so two of the seven
            // a volatile write can draw now outlive the settle where one of
            // six did, and the draw decides `alive` where it used to decide
            // `dead`. Both halves of the expiration invariant are still
            // reached, which is what these two numbers are here to say.
            //
            // `PSETEX`'s arrival moved the same two back the other way, for
            // the mirror of that reason: the eighth deadline is 300ms, so it
            // dies inside the run and two of the eight outlive the settle
            // where two of seven did. `plain_checks` and `walk_checks` held
            // still through both, which is what says a deadline was added and
            // nothing else moved.
            (51, 32, 149, 32),
            "the recorded workload decides a different number of checks"
        );
    }

    /// Every plant, asked whether the shapes `sweep` walks can catch it.
    ///
    /// Walked over [`Plant::ALL`] and matched without a wildcard, so a plant
    /// added later cannot inherit an answer nobody decided: this stops
    /// compiling until someone says where the new defect is observable.
    #[test]
    fn every_plant_answers_whether_the_swept_shapes_catch_it() {
        for plant in Plant::ALL {
            let place = plant.unobservable_on_swept_shapes();
            match plant {
                Plant::LostUpdate
                | Plant::ServeExpired
                | Plant::SweepEatsAll
                | Plant::EvictsBelowCeiling => assert_eq!(
                    place,
                    None,
                    "{} is caught where it is swept, so it has no elsewhere to name",
                    plant.name()
                ),
                Plant::ScanMissesRehash => {
                    let place =
                        place.expect("the swept shapes cannot observe an upward scan cursor");
                    assert!(
                        place.contains("dict.rs"),
                        "a reader sent somewhere must be sent to a file: {place}"
                    );
                }
                Plant::IgnoresCeiling => {
                    let place =
                        place.expect("a shape with no ceiling cannot observe one being ignored");
                    assert!(
                        place.contains("planted_eviction.rs"),
                        "a reader sent somewhere must be sent to a file: {place}"
                    );
                }
                Plant::CrossingSkipsShard => {
                    let place = place
                        .expect("a shape whose walks stop short cannot observe a skipped shard");
                    assert!(
                        place.contains("planted_crossing.rs"),
                        "a reader sent somewhere must be sent to a file: {place}"
                    );
                }
            }
        }
        // The place is a string, so nothing but this stops it outliving the
        // file it names — and a warning pointing at a path that is not there
        // is worse than no warning.
        for path in [
            concat!(env!("CARGO_MANIFEST_DIR"), "/../seedstone-core/src/dict.rs"),
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/planted_eviction.rs"),
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/planted_crossing.rs"),
        ] {
            assert!(
                std::path::Path::new(path).exists(),
                "the place a plant points at no longer exists: {path}"
            );
        }
    }

    /// Which plants a sweep's violation count is evidence about, pinned as a
    /// set rather than one by one: the interesting claim is *which* defects
    /// are outside what the swept shapes reach, and one appearing or leaving
    /// that set is a change in what those shapes measure.
    #[test]
    fn the_plants_the_swept_shapes_cannot_catch_are_the_three_that_need_a_shape() {
        let unobservable: Vec<&str> = Plant::ALL
            .into_iter()
            .filter(|plant| plant.unobservable_on_swept_shapes().is_some())
            .map(Plant::name)
            .collect();
        assert_eq!(
            unobservable,
            [
                "scan-misses-rehash",
                "ignores-ceiling",
                "crossing-skips-shard"
            ],
            "the plants a swept violation count says nothing about have changed"
        );
    }
}
