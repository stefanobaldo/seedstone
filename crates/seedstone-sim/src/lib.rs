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
//! expiry plants are the server's own [`ExpiryPolicy`], handed to the shard
//! pool at spawn, so the invariant catches the broken decision itself; the
//! lost update is a [`PlantedRouter`] above the shard, because a handler that
//! cannot `await` cannot lose an update to itself.

use rand::rngs::ChaCha8Rng;
use rand::{RngExt, SeedableRng};
use seedstone_core::dict::{DictSeed, WalkOrder};
// The two error texts are imported, not copied. The planted router has to be
// indistinguishable from the honest one except in its atomicity, and these
// strings enter the trace hash — a private copy that drifted would make a
// planted trace differ for a reason unrelated to the race.
use seedstone_core::shard::{
    Command, Deadlines, ExpiryPolicy, Reply, ReplyError, Route, Router, ShardPool, TraceSink,
    parse_i64,
};
use seedstone_resp::{Decoder, DecoderLimits, Frame, encode};
use seedstone_service::{NodeInfo, serve_connection};
use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

// The one thing here that lives outside a simulation rather than inside one:
// it starts runs, in parallel, and reaches into none of them.
pub mod contract;

mod sweep;

pub use sweep::{SweepReport, sweep};

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

/// The odd 64-bit constant from Fibonacci hashing, used both to decorrelate
/// per-client workload seeds and as the trace hash's multiplier.
const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

/// The trace hash's multiplier — the FxHash constant.
const TRACE_MULTIPLIER: u64 = 0x517c_c1b7_2722_0a95;

/// The trace hash's starting value.
///
/// Non-zero so that an empty trace is distinguishable from a trace that
/// happened to fold back to zero, and so that a `mix` of zeros still moves.
const TRACE_INIT: u64 = 0xcbf2_9ce4_8422_2325;

/// How a simulation run is shaped.
///
/// Every field is part of what a trace hash means: two runs are comparable
/// only if their configurations are identical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimConfig {
    /// How many virtual shards the server runs.
    pub shards: u16,
    /// How many executor tasks host those shards.
    ///
    /// Explicit, never read from the machine: production asks
    /// `available_parallelism`, and doing that inside a simulation would make
    /// the trace a function of the host — the definition of a determinism
    /// violation. It is a dimension the sweep varies, not a detail the
    /// environment supplies.
    pub executors: u16,
    /// How many client hosts issue the workload.
    ///
    /// This is the lever that costs wall clock — turmoil polls every host on
    /// every tick — and also the lever that buys schedule sensitivity.
    pub clients: u16,
    /// How many keys take `GET`/`SET`/`DEL` and never carry a deadline.
    ///
    /// Split evenly between the clients, so each owns a slice nothing else
    /// writes: that exclusivity is what lets a client model the family
    /// exactly and assert on every read, and it is why cross-client
    /// contention lives on the counters instead.
    ///
    /// Kept above `shards` on purpose in the swept shape: with fewer keys
    /// than shards the shard dimension is degenerate and the simulation stops
    /// exercising placement.
    pub plain_keys: u32,
    /// How many keys carry deadlines and take `SET … EX`/`PX`, `EXPIRE`,
    /// `TTL` and `GET`.
    ///
    /// Partitioned per client like [`SimConfig::plain_keys`], and for the
    /// same reason: the two expiration invariants are a statement about what
    /// *this* client asked for, which another client writing the same key
    /// would make unprovable rather than merely harder.
    ///
    /// Kept small per client so a key is written and read back several times
    /// within one run — a family large enough to be touched once each is a
    /// family whose deadlines nothing ever observes.
    pub volatile_keys: u32,
    /// How many keys take `INCRBY` and carry the sum invariant.
    ///
    /// Fewer counters means more contention on each, which is what surfaces a
    /// lost update.
    pub counter_keys: u32,
    /// How many operations each client issues before it disconnects.
    pub ops_per_client: u32,
    /// How many operations a client writes before reading any of their
    /// replies.
    ///
    /// The lever that gives a server drain something to group. At 1 every
    /// drain decodes one command and dispatches a batch of one, so neither the
    /// per-executor grouping nor the chunk bound is reached and
    /// [`SimConfig::executors`] becomes invisible to the trace — a harness
    /// measuring nothing while passing. Kept comfortably below the drain's own
    /// chunk bound: splitting a batch across chunks is a bound the service
    /// layer's own tests exercise directly, and buying it here would cost a
    /// sweep several times its wall clock for coverage that already exists.
    pub pipeline_depth: u32,
    /// Seeds the per-client operation generators.
    pub workload_seed: u64,
    /// Seeds turmoil: the network's latencies and the order hosts run in.
    pub sim_seed: u64,
    /// Whether the run ends with a complete `SCAN` cycle over the walk
    /// family.
    ///
    /// Off in both shapes below, and the reason is what it costs against what
    /// it measures. A complete cycle costs **at least one round trip per
    /// shard** — a spent shard hands back the next one's start rather than
    /// continuing into it — which is a thousand sequential round trips on the
    /// deployed shard count, measured at four and a half times the rest of a
    /// run. What it buys for that is an assertion taken after every client has
    /// stopped, so no schedule is being exercised while it runs: sweeping it
    /// over three hundred seeds proves exactly what one run proves, while
    /// costing the sweep the seeds that were finding real interleavings.
    ///
    /// So it is a test's to ask for, not a sweep's, and the sweep keeps the
    /// half that *is* schedule-sensitive: every client walks its own family
    /// with `KEYS` while the others are still writing.
    pub quiescent_walk: bool,
    /// Whether a client's own walk drives its `SCAN` cycle to the end, or
    /// stops after a short prefix.
    ///
    /// Off in both shapes below, and for the same arithmetic that keeps
    /// [`SimConfig::quiescent_walk`] off: a complete cycle costs at least one
    /// round trip per shard, and a *client's* cycle costs that once per
    /// client. On the deployed shard count that is six figures of sequential
    /// round trips for a single seed.
    ///
    /// What the prefix gives up is only the completeness half of the
    /// guarantee — at-least-once, which needs a walk that finished. The
    /// `KEYS` that closes every walk carries that half instead, complete by
    /// construction and one round trip wide, so what is lost is the claim
    /// stated over `SCAN` specifically. Everything else a walk promises is a
    /// property of each step and is asserted on every one of them.
    ///
    /// Turned on by a shape narrow enough to afford it, which is where the
    /// cursor's own liveness can be put under test: a walk that never
    /// finishes is only visible to a walk that was trying to.
    pub concurrent_scan_cycle: bool,
    /// Which deliberate defect, if any, to serve the workload through.
    pub planted: Option<Plant>,
}

impl SimConfig {
    /// The sweep configuration: the shape measured to be schedule-sensitive,
    /// which is what makes a seed sweep find anything.
    #[must_use]
    pub const fn standard(workload_seed: u64, sim_seed: u64) -> Self {
        Self {
            shards: 1024,
            executors: 10,
            clients: 128,
            plain_keys: 2048,
            volatile_keys: 1024,
            counter_keys: 64,
            ops_per_client: 25,
            pipeline_depth: 8,
            workload_seed,
            sim_seed,
            quiescent_walk: false,
            concurrent_scan_cycle: false,
            planted: None,
        }
    }

    /// A shape narrow enough for a client to walk its cycle to the end.
    ///
    /// One shard rather than a thousand, and that is the whole point: a
    /// complete `SCAN` cycle costs at least one round trip per shard *and*
    /// each shard after the first meets a table that everything written since
    /// the walk began has been growing. Every shard added is another table for
    /// the cycle to cross, and none of them shows a property the first one
    /// does not — the cursor under test belongs to one dict. What the cycle
    /// costs in this shape is stated once, beside the bound that governs it:
    /// see [`WALK_CYCLE_STEP_BOUND`].
    ///
    /// What a shape this narrow gives up is placement, and it gives it up
    /// knowingly: nothing here is about which shard a key lands in. What it
    /// buys is the only condition under which a walk's *liveness* is
    /// observable at all — a table deep enough that a step is a fraction of
    /// it, still growing while the cursor is inside it. The swept shape has
    /// neither: a thousand shards hold about four keys each, and one step
    /// finishes a table that size before anything can happen underneath it.
    ///
    /// Two clients, which is what keeps the walk affordable. The keyspace
    /// grows while the walk runs, so every extra client churning alongside it
    /// doubles the table the cursor has left to cross — the cost is
    /// exponential in how many of them there are, not linear.
    ///
    /// A shape for tests that need a finished cycle, not a second sweep.
    #[must_use]
    pub const fn narrow(workload_seed: u64, sim_seed: u64) -> Self {
        Self {
            shards: 1,
            executors: 1,
            clients: 2,
            plain_keys: 32,
            volatile_keys: 16,
            counter_keys: 4,
            ops_per_client: 16,
            pipeline_depth: 4,
            workload_seed,
            sim_seed,
            quiescent_walk: false,
            concurrent_scan_cycle: true,
            planted: None,
        }
    }

    /// A smaller shape for tests: few enough client hosts to run in a unit
    /// test, enough operations per client to keep the counters contended.
    #[must_use]
    pub const fn mini(workload_seed: u64, sim_seed: u64) -> Self {
        Self {
            shards: 1024,
            executors: 4,
            clients: 16,
            plain_keys: 256,
            volatile_keys: 128,
            counter_keys: 8,
            ops_per_client: 40,
            pipeline_depth: 8,
            workload_seed,
            sim_seed,
            quiescent_walk: false,
            concurrent_scan_cycle: false,
            planted: None,
        }
    }
}

/// A deliberate defect the harness can serve its own workload through.
///
/// Each one is the bug an invariant exists to find, and every invariant has
/// one: a guarantee nobody has watched fail is a guarantee nobody has
/// measured.
///
/// Two of the three are defects *inside* the server — a policy handed to the
/// shard pool at spawn, so what the invariant catches is the defect itself and
/// not an imitation of what it would look like. See
/// [`seedstone_core::shard::ExpiryPolicy`] for why that is expressible without
/// putting broken code in the shipped binary.
///
/// [`Plant::LostUpdate`] stays above the shard, in [`PlantedRouter`], and that
/// is not a compromise: a shard handler is a plain `fn` that cannot `await`,
/// so a lost update cannot occur inside one. Above the shard is where a real
/// one would live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plant {
    /// `INCRBY` served as a read-modify-write pair instead of one atomic
    /// message, so a concurrent increment can be overwritten. Caught by the
    /// counter sum.
    LostUpdate,
    /// A liveness check and a sweep that never find anything due: the deadline
    /// is accepted, stored, and never acted on by either half of expiration.
    /// Caught by `stale_reads`.
    ServeExpired,
    /// A sweep that takes everything it walks, undated entries included, while
    /// the read path stays honest. What an active sweep that stopped checking
    /// `expires_at` is. Caught by `spurious_deaths` and by the plain keys'
    /// model.
    SweepEatsAll,
    /// A scan cursor that counts buckets upwards instead of advancing in
    /// reverse binary order.
    ///
    /// It is right on a table that never changes size, and it is what the
    /// reverse order exists to avoid on one that does: a step moves the cursor
    /// one bucket while a doubling moves the finish line by the whole width of
    /// the table, so a keyspace growing faster than the cursor advances
    /// outruns it and the cycle never comes back to `0`. Caught by
    /// `walk_mismatches`, through the step bound — not through a lost key,
    /// which is worth being exact about: under a table that only ever grows,
    /// an upward cursor visits every bucket that is still ahead of it, and
    /// what it fails at is *arriving*, which is what the walk's step bound is
    /// stated to catch.
    ///
    /// Invisible to any walk that is not stepping bucket by bucket while its
    /// shard's table grows, which is why it has a shape of its own rather than
    /// a place in the swept one — see `tests/planted_walk.rs`.
    ScanMissesRehash,
}

impl Plant {
    /// The name this plant is selected by on a command line, and printed
    /// under in a sweep's summary.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::LostUpdate => "lost-update",
            Self::ServeExpired => "serve-expired",
            Self::SweepEatsAll => "sweep-eats-all",
            Self::ScanMissesRehash => "scan-misses-rehash",
        }
    }

    /// Every plant, so a caller listing or sweeping them cannot miss one
    /// added later.
    pub const ALL: [Self; 4] = [
        Self::LostUpdate,
        Self::ServeExpired,
        Self::SweepEatsAll,
        Self::ScanMissesRehash,
    ];

    /// The plant `name` selects, if it names one.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|plant| plant.name() == name)
    }

    /// Where this plant is observable, when the shapes a sweep walks cannot
    /// catch it — `None` when they can.
    ///
    /// A plant exists so a self-test's detection power can be measured, and a
    /// plant the swept shape cannot catch turns that measurement into a number
    /// about something else: the sweep serves the defect, finds nothing, and
    /// reports no violations. Nothing in that zero says the defect was absent,
    /// and the summary printed at the end of a run is the only place a
    /// command-line reader will ever learn the difference — so the knowledge
    /// lives here, beside the plant that has it, and the binary asks.
    ///
    /// Matched without a wildcard on purpose: a plant added later must be
    /// classified by whoever adds it, and until they do this does not compile.
    #[must_use]
    pub const fn unobservable_on_swept_shapes(self) -> Option<&'static str> {
        match self {
            // Caught where they are swept. The counter sum and both
            // expiration invariants decide on every shape this repository
            // sweeps, and `tests/standard_catches.rs` pins that for the one
            // the gate runs.
            Self::LostUpdate | Self::ServeExpired | Self::SweepEatsAll => None,
            // Needs a cursor observed *between* steps of a table that is
            // growing under it, and a swept shard holds about four keys, where
            // one step covers the whole table — see this plant's own note.
            Self::ScanMissesRehash => {
                Some("SimConfig::narrow, walked by crates/seedstone-sim/tests/planted_walk.rs")
            }
        }
    }
}

/// What one run produced.
///
/// Every violation count comes paired with the number of replies its
/// invariant actually *decided*. A zero violation count is evidence only
/// beside a non-zero check count — this is the same discipline the counter
/// sum is held to, where a workload that acknowledged no `INCRBY` satisfies
/// `0 == 0` while proving nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimOutcome {
    /// The fold of every command the server completed, in the order it
    /// completed them. A function of the two seeds and the configuration
    /// alone — stable across processes, machines and builds.
    pub trace_hash: u64,
    /// The sum of every acknowledged `INCRBY` delta.
    pub expected_sum: i64,
    /// The sum of every counter key read back at the end.
    pub actual_sum: i64,
    /// Reads that returned a value for a key certainly past its deadline.
    pub stale_reads: u64,
    /// Reads that returned nothing for a key certainly still within its
    /// deadline.
    pub spurious_deaths: u64,
    /// Reads of a plain key that disagreed with what its owner last wrote.
    pub plain_mismatches: u64,
    /// Volatile reads decided against a passed deadline — the denominator of
    /// [`SimOutcome::stale_reads`].
    pub dead_checks: u64,
    /// Volatile reads decided against a future deadline — the denominator of
    /// [`SimOutcome::spurious_deaths`].
    pub alive_checks: u64,
    /// Plain reads decided against a client's model — the denominator of
    /// [`SimOutcome::plain_mismatches`].
    pub plain_checks: u64,
    /// Keyspace walks that did not return exactly the keys that were there.
    pub walk_mismatches: u64,
    /// Keyspace walks decided against an exactly known set — the denominator
    /// of [`SimOutcome::walk_mismatches`].
    pub walk_checks: u64,
    /// Every form of every command this run's clients actually emitted, named
    /// as [`crate::contract`] names it.
    ///
    /// The numerator to the contract's denominator, and it is a *set* rather
    /// than a count on purpose: which forms were reached is the question, and
    /// how many times each was reached says nothing about coverage. Compared
    /// against the declaration at sweep level and never per seed — a rare form
    /// missing from one seed is expected; missing from a whole sweep is a
    /// claim that was never true.
    pub forms_emitted: BTreeSet<&'static str>,
}

impl SimOutcome {
    /// Whether every invariant the run measures held.
    ///
    /// The counter sum first: `INCRBY` is order-independent, so a schedule
    /// cannot legitimately move it and any difference is an acknowledged
    /// increment that did not survive. Then the keyspace invariants, each of
    /// which a schedule is equally powerless to excuse — a deadline is a
    /// deadline, a key nobody else can write is what its owner last wrote,
    /// and a walk over a set nobody is touching returns that set.
    #[must_use]
    pub const fn invariant_holds(&self) -> bool {
        self.expected_sum == self.actual_sum
            && self.stale_reads == 0
            && self.spurious_deaths == 0
            && self.plain_mismatches == 0
            && self.walk_mismatches == 0
    }

    /// Whether the run's invariants decided anything at all.
    ///
    /// Not part of [`SimOutcome::invariant_holds`] on purpose: a sweep's job
    /// is to report violations, and a run that happened to check nothing is
    /// not a violation. It is a failure of the *harness*, which is a claim
    /// for a test to make about a configuration, not for a seed to make about
    /// the system.
    #[must_use]
    pub const fn invariants_were_exercised(&self) -> bool {
        self.expected_sum != 0
            && self.dead_checks > 0
            && self.alive_checks > 0
            && self.plain_checks > 0
            && self.walk_checks > 0
    }
}

/// Folds `v` into the running hash `h`.
///
/// FxHash-style: cheap, order-dependent, and — unlike `DefaultHasher`, whose
/// output is explicitly not guaranteed stable across processes or Rust
/// versions — defined entirely by this function. Cross-process stability is
/// the product here, so the mixing function has to be ours.
#[must_use]
pub const fn mix(h: u64, v: u64) -> u64 {
    (h.rotate_left(5) ^ v).wrapping_mul(TRACE_MULTIPLIER)
}

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

    sim.host(SERVER, move || {
        // Cloned per invocation: turmoil may restart a host, and each start
        // needs its own future. The sink is shared on purpose — a restart
        // continues the same trace.
        let sink = sink.clone();
        server(shards, executors, dict_seed, sink, planted)
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
        forms_emitted: lock(&shared.forms).clone(),
    }
}

/// What the client hosts and the verifier write into, and the run reads out.
///
/// Every host in a turmoil simulation runs on the same OS thread, so this
/// mutex is never actually contended; it is here because [`TraceSink`] and
/// the futures turmoil holds must be `Send`.
#[derive(Clone, Default)]
struct Shared {
    /// The counters every host adds to.
    tally: Arc<Mutex<Tally>>,
    /// Every walk key whose write the server acknowledged, from every client.
    ///
    /// The verifier's walk asserts set equality over the whole family, and it
    /// cannot derive the family from the configuration: a write the server
    /// refused is a key that is legitimately absent, and a model that assumed
    /// otherwise would report a violation the system never committed. So the
    /// clients publish what they were told took, and the verifier holds the
    /// server to exactly that.
    walk: Arc<Mutex<BTreeSet<Vec<u8>>>>,
    /// Every form label the run's clients actually put on the wire.
    ///
    /// The observed half of the contract. A declaration alone can claim a
    /// form the generator never reaches — a branch with probability zero, or
    /// one a bug made unreachable — and the only thing that can tell the two
    /// apart is a record of what was really sent.
    forms: Arc<Mutex<BTreeSet<&'static str>>>,
}

/// Everything the hosts count between them.
#[derive(Debug, Clone, Copy, Default)]
struct Tally {
    /// The sum of every acknowledged `INCRBY` delta.
    expected: i64,
    /// The sum of every counter read back at the end.
    actual: i64,
    /// How many client hosts have finished their workload.
    done: u32,
    /// Violations, and the checks that could have found them. See
    /// [`SimOutcome`], whose fields these become.
    stale_reads: u64,
    spurious_deaths: u64,
    plain_mismatches: u64,
    dead_checks: u64,
    alive_checks: u64,
    plain_checks: u64,
    walk_mismatches: u64,
    walk_checks: u64,
}

/// Takes a lock that cannot be contended, and says so if it was poisoned.
///
/// # Panics
///
/// If another host panicked while holding the lock. That has already failed
/// the run; this only reports where.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().expect("a simulated host panicked mid-update")
}

/// A [`TraceSink`] that folds every completed command into a shared hash.
///
/// Calls arrive in each shard's own execution order, which under a
/// deterministic scheduler is a function of the seeds alone.
#[derive(Clone)]
struct HashSink(Arc<Mutex<u64>>);

impl TraceSink for HashSink {
    fn record(&self, shard: u16, seq: u64, cmd: &Command, reply: &Reply) {
        let mut h = lock(&self.0);
        let mut acc = *h;
        acc = mix(acc, u64::from(shard));
        // `seq` is the replication position where the command's effects
        // *began*, not a counter of commands and not an index into the log: a
        // read consumes no position, so the same `seq` recurs, and a write
        // that first had to evict an expired key reports the eviction's
        // position rather than its own. That is exactly what we want folded —
        // a schedule that reorders a write against a read changes which
        // position the read observed. `TraceSink::record` carries the full
        // definition.
        acc = mix(acc, seq);
        acc = mix(acc, u64::from(cmd.kind()));
        acc = match cmd.route() {
            // Byte-for-byte what this folded when a command could only name a
            // key, so no recorded trace hash moves.
            Route::Key(key) => fold_bytes(acc, key),
            // A route with no key still has to reach the hash: two commands of
            // one kind that went to different shards are different commands,
            // and `shard` above only says where the answer came from.
            //
            // Nothing produces `Route::Shard` today — the arm is here for its
            // tag, which no other route may take.
            Route::Shard(shard) => mix(mix(acc, 1), u64::from(shard)),
            Route::Every => mix(acc, 2),
            // A `ScanStep` folds a constant here, because the variant names
            // no shard of its own and the shard that ran it arrives as
            // `record`'s own argument, folded above. What decides the step is
            // its arguments, and those are folded below.
            Route::Unaddressed => mix(acc, 3),
        };
        acc = fold_inputs(acc, cmd);
        acc = fold_reply(acc, reply);
        *h = acc;
    }
}

/// Folds whatever of a command's arguments its route does not already reach.
///
/// Every command that names a key folds it through [`Route::Key`], and what
/// the rest of its arguments did is visible in the reply the shard gave. A
/// scan step is the exception on both counts: it names no key, and two steps
/// with different cursors, counts or patterns can answer alike — an empty
/// batch and a spent cursor look the same however they were asked for. So a
/// walk driven from the workload would otherwise fold its outcome and none of
/// its inputs, and a divergence in *which step was taken* would be invisible
/// until it happened to change an answer.
fn fold_inputs(h: u64, cmd: &Command) -> u64 {
    match cmd {
        Command::ScanStep {
            cursor,
            count,
            pattern,
        } => {
            let h = mix(mix(h, *cursor), *count as u64);
            pattern
                .as_ref()
                .map_or_else(|| mix(h, 0), |pattern| fold_bytes(mix(h, 1), pattern))
        }
        // Every other command's key is folded by its route, and its remaining
        // arguments are decided by the reply. A command added later whose
        // behaviour turns on an argument neither of those reaches belongs
        // here, not in a comment saying it does not matter yet.
        _ => h,
    }
}

/// Folds a byte string, length first so that concatenations cannot collide.
fn fold_bytes(mut h: u64, bytes: &[u8]) -> u64 {
    h = mix(
        h,
        u64::try_from(bytes.len()).expect("a slice length is a usize"),
    );
    for &b in bytes {
        h = mix(h, u64::from(b));
    }
    h
}

/// Folds a reply: a variant tag, then whatever distinguishes it.
///
/// The tags are part of the trace's meaning — changing one changes every
/// recorded hash, which is a deliberate cost.
fn fold_reply(h: u64, reply: &Reply) -> u64 {
    match reply {
        Reply::Bulk(None) => mix(h, 1),
        Reply::Bulk(Some(value)) => fold_bytes(mix(h, 2), value),
        Reply::Ok => mix(h, 3),
        // Tag 8 because 1 to 7 were already spoken for, and the text after it
        // because two statuses are two different answers.
        Reply::Status(text) => fold_bytes(mix(h, 8), text.as_bytes()),
        Reply::Removed(removed) => mix(mix(h, 4), u64::from(*removed)),
        Reply::Integer(n) => mix(mix(h, 5), n.cast_unsigned()),
        // Both fields, not just the cursor: two steps that resumed at the same
        // place and returned different keys are different answers, and a walk
        // that lost a key while its cursor kept advancing is exactly the
        // regression a trace hash is here to make visible.
        Reply::Scan { cursor, keys } => {
            let mut acc = mix(mix(h, 7), *cursor);
            for key in keys {
                acc = fold_bytes(acc, key);
            }
            acc
        }
        // Folds the wire text, not the variant tag: the recorded hashes
        // predate the enum and must not move for a change that renamed
        // nothing a client can see.
        Reply::Error(error) => fold_bytes(mix(h, 6), error.wire_text().as_bytes()),
    }
}

/// A server whose liveness check and sweep both stopped firing.
///
/// The deadline is accepted, stored and never acted on. `stale_reads` owns it.
#[derive(Clone, Copy)]
struct ServeExpired;

impl WalkOrder for ServeExpired {}

impl ExpiryPolicy for ServeExpired {
    fn due_on_read(&self, _expires_at: Option<Instant>, _now: Instant) -> bool {
        false
    }
    fn due_on_sweep(&self, _expires_at: Option<Instant>, _now: Instant) -> bool {
        false
    }
    fn takes_undated(&self) -> bool {
        false
    }
}

/// A sweep that stopped asking whether an entry had a deadline at all.
///
/// Everything it reaches is due, undated keys included — which is why it must
/// answer `takes_undated` yes, or the dict it is walking would never be
/// walked. The read path stays honest: this defect is the sweep's alone, and a
/// plant that broke both would not tell the two invariants apart.
#[derive(Clone, Copy)]
struct SweepEatsAll;

impl WalkOrder for SweepEatsAll {}

impl ExpiryPolicy for SweepEatsAll {
    fn due_on_read(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        Deadlines.due_on_read(expires_at, now)
    }
    fn due_on_sweep(&self, _expires_at: Option<Instant>, _now: Instant) -> bool {
        true
    }
    fn takes_undated(&self) -> bool {
        true
    }
}

/// A cursor that counts buckets upwards instead of advancing in reverse
/// binary order.
///
/// Honest about deadlines, and it has to be: what this plant is about is a
/// walk that cannot finish, and a walk whose keyspace was also disappearing
/// underneath it would leave the two indistinguishable.
#[derive(Clone, Copy)]
struct ScanMissesRehash;

impl WalkOrder for ScanMissesRehash {
    fn advance(&self, cursor: u64, mask: u64) -> u64 {
        // Wrapping rather than plain, and it costs nothing: a cursor arrives
        // from the wire and a client may send any number at all, so the
        // arithmetic has to be total. The honest order is total for the same
        // reason.
        cursor.wrapping_add(1) & mask
    }
}

impl ExpiryPolicy for ScanMissesRehash {
    fn due_on_read(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        Deadlines.due_on_read(expires_at, now)
    }
    fn due_on_sweep(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        Deadlines.due_on_sweep(expires_at, now)
    }
    fn takes_undated(&self) -> bool {
        false
    }
}

/// A router that serves the workload through one deliberate defect.
///
/// This is the harness's self-test: a simulator that never fails proves
/// nothing, so the branch ships bugs the sweep is required to find. One plant
/// lives here — [`Plant::LostUpdate`], which is a defect between two messages
/// and so has nowhere else to be; see [`Plant`] for why the other two are the
/// server's own expiry policy instead. Everything that plant does not touch
/// passes straight through, so an honest run and a planted one differ in
/// exactly one thing.
///
/// Which plant is not a parameter, and that is the point: it was one while all
/// three lived here, and leaving it would mean `new(pool, Plant::ServeExpired)`
/// compiled into a run with nothing planted in it at all. A self-test that can
/// be asked for a defect and quietly hand back an honest server is the exact
/// failure it exists to prevent.
#[derive(Clone)]
pub struct PlantedRouter {
    /// The honest pool underneath.
    pool: ShardPool,
}

impl PlantedRouter {
    /// Wraps `pool` so that a lost update is what the workload meets.
    #[must_use]
    pub const fn new(pool: ShardPool) -> Self {
        Self { pool }
    }
}

impl Router for PlantedRouter {
    // Spelled `async fn` rather than the trait's desugared `-> impl Future`:
    // with a bare `async` block as the body clippy's `manual_async_fn` fires,
    // and the gate is `-D warnings`. `ShardPool`'s own impl keeps the
    // desugared form for a reason that does not apply here — it sends on the
    // inbox before the future is awaited.
    async fn dispatch(&self, cmd: Command) -> Reply {
        self.lose_updates(cmd).await
    }

    /// The pool's own count: the plant wraps a pool, it does not resize one.
    fn shards(&self) -> u16 {
        self.pool.shards()
    }

    /// Passed straight through: the plant is a defect between two messages of
    /// one keyed command, and a shard-addressed step is neither.
    async fn dispatch_at(&self, shard: u16, cmd: Command) -> Reply {
        self.pool.dispatch_at(shard, cmd).await
    }

    /// Passed straight through: the plant is a defect between two messages of
    /// one keyed command, and a broadcast is neither.
    async fn dispatch_every(&self, cmd: Command) -> Vec<Reply> {
        self.pool.dispatch_every(cmd).await
    }
}

impl PlantedRouter {
    /// `INCRBY` as `GET`, compute, `SET`: two shard round-trips with an await
    /// between them, so another connection's increment can land in the window
    /// and be overwritten.
    async fn lose_updates(&self, cmd: Command) -> Reply {
        let Command::IncrBy { key, delta } = cmd else {
            return self.pool.dispatch(cmd).await;
        };

        let current = match self.pool.dispatch(Command::Get { key: key.clone() }).await {
            Reply::Bulk(Some(value)) => match parse_i64(&value) {
                Some(current) => current,
                None => return Reply::Error(ReplyError::NotAnInteger),
            },
            Reply::Bulk(None) => 0,
            // A shard that could not answer; pass its complaint on unchanged.
            other => return other,
        };
        let Some(updated) = current.checked_add(delta) else {
            return Reply::Error(ReplyError::WouldOverflow);
        };

        // The window, widened deliberately. `current` was read at one moment
        // and is written back at a later one, and nothing holds the key still
        // in between; this hands the scheduler an explicit chance to run
        // another connection there. Without it the window is one scheduler
        // round and the race surfaced in 2 seeds of 64 — evidence far weaker
        // than the claim the self-test makes. With it, 26 of 64.
        //
        // It is not a cheat: a genuine read-modify-write across an await is
        // exactly this shape, and the suspension point is honest rather than
        // simulated. It is also free — the honest router is untouched, and no
        // hash of a planted run is pinned anywhere, so no recorded trace moves.
        tokio::task::yield_now().await;

        match self
            .pool
            .dispatch(Command::Set {
                key,
                value: updated.to_string().into_bytes(),
                // The write the honest `INCRBY` would have made: same value,
                // same absence of options. The only thing planted here is that
                // it is a second message.
                expiry: None,
                cond: None,
                keep_ttl: false,
                get: false,
            })
            .await
        {
            Reply::Ok => Reply::Integer(updated),
            other => other,
        }
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
) -> turmoil::Result {
    let pool = match planted {
        Some(Plant::ServeExpired) => {
            ShardPool::spawn_with_policy(shards, executors, seed, sink, ServeExpired)
        }
        Some(Plant::SweepEatsAll) => {
            ShardPool::spawn_with_policy(shards, executors, seed, sink, SweepEatsAll)
        }
        Some(Plant::ScanMissesRehash) => {
            ShardPool::spawn_with_policy(shards, executors, seed, sink, ScanMissesRehash)
        }
        // The honest pool, and the lost-update plant's too: that defect lives
        // above the shard, where a real one would.
        None | Some(Plant::LostUpdate) => ShardPool::spawn(shards, executors, seed, sink),
    };
    let listener = turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, PORT)).await?;
    // One per host, as it is in production: it describes the node, not the
    // connection. No workload here asks a host about itself, so nothing reads
    // it — it is here because the connection code takes one.
    let node = NodeInfo::for_tests();
    loop {
        let (stream, _peer) = listener.accept().await?;
        // Only one plant is a router now. The other two are inside the server,
        // which is where the defects they imitate would be.
        if planted == Some(Plant::LostUpdate) {
            tokio::spawn(serve_connection(
                stream,
                PlantedRouter::new(pool.clone()),
                node.clone(),
            ));
        } else {
            tokio::spawn(serve_connection(stream, pool.clone(), node.clone()));
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

/// One client's slice of a key family.
struct KeyRange {
    /// Where this client's keys start in the family.
    first: u32,
    /// How many it owns.
    len: u32,
}

impl KeyRange {
    /// Splits `total` keys evenly between `clients` and takes `id`'s share.
    ///
    /// A remainder is left unused rather than handed to the last client: an
    /// uneven slice would give one client a differently shaped workload for a
    /// reason nobody reading a failing seed would remember.
    fn new(id: u16, total: u32, clients: u16) -> Self {
        let len = (total / u32::from(clients.max(1))).max(1);
        Self {
            first: u32::from(id) * len,
            len,
        }
    }

    /// The family-wide index of the `slot`-th key this client owns.
    const fn key(&self, slot: u32) -> u32 {
        self.first + slot
    }

    /// A slot drawn from this client's own slice.
    fn pick(&self, rng: &mut ChaCha8Rng) -> u32 {
        rng.random_range(0..self.len)
    }
}

/// What a client believes about one plain key it owns.
#[derive(Clone)]
enum Known {
    /// Never written, or written and answered with something the client could
    /// not read. Nothing is asserted about the key until it is written again.
    Nothing,
    /// Deleted by its owner, and nothing has written it since.
    Absent,
    /// Written by its owner with these bytes, and nothing has written it
    /// since.
    Value(Vec<u8>),
}

/// One operation a client is about to issue.
///
/// The form travels with the frame rather than being derived from it later:
/// the arm that composed the command is the one place that knows without
/// question which of the contract's forms it is, and a reader that had to
/// recover it from the bytes would be a second implementation of the
/// contract's spelling.
struct Op {
    frame: Frame,
    check: Check,
    /// The contract's name for what this is. See [`crate::contract`].
    form: &'static str,
}

/// What one reply is worth to the client that asked for it.
///
/// Built with the command and consumed with the reply: a reply's index in a
/// burst is its command's, so this is how a client keeps hold of what it was
/// expecting without having to read it back off the wire.
enum Check {
    /// Coverage only — the reply carries no claim. `TTL` is here: it is in
    /// the workload so its command kind is traced and its arithmetic runs,
    /// and what it could assert is a weaker form of what the `GET`
    /// invariants already assert.
    Ignored,
    /// An `INCRBY` this client owes the shared expected sum, if it is
    /// acknowledged.
    Counter(i64),
    /// A `SET` of an owned plain key: the model adopts `value` if it took.
    ///
    /// Also what a `KEEPTTL` is checked as. On a family no deadline is ever
    /// put on, the option has nothing to keep and the reply is a plain `SET`'s
    /// — which is the point: what it reaches is the option's parse path and
    /// the branch that finds no deadline to preserve. The branch that
    /// *preserves* one is not reachable from any client here, and the reason
    /// is the volatile model rather than the draw: it holds deadlines and not
    /// values, so it cannot tell a `KEEPTTL` that kept a deadline from one
    /// that met an expired key and created a new one with none. Emitting it
    /// there would mean giving the key up, which is coverage bought by losing
    /// an invariant.
    PlainSet { slot: u32, value: Vec<u8> },
    /// A `SET … NX` or `SET … XX` of an owned plain key.
    ///
    /// The strongest thing the plain model can be asked, because the answer is
    /// the model itself: `OK` where the condition held and a null where it did
    /// not. The model knows presence exactly — nothing else writes these keys
    /// — so both replies are predictions rather than observations.
    PlainSetCond {
        slot: u32,
        value: Vec<u8>,
        /// `true` for `XX`, which sets only where a value already is.
        only_if_present: bool,
    },
    /// A `SET … GET` of an owned plain key: the reply is what the key held
    /// *before* this command, and the key holds `value` after it.
    ///
    /// A `GET` and a `SET` in one round trip, and it is checked as both: the
    /// reply is held against the model exactly as a `GET`'s would be, and the
    /// model then adopts the value the command wrote.
    PlainSetGet { slot: u32, value: Vec<u8> },
    /// A `DEL` of one to three owned plain keys — the variadic form, which
    /// the service layer fans out one command per key. How many it removes is
    /// the model's to predict: the *distinct* slots it believes hold a value,
    /// since a key named twice is removed once.
    PlainDel { slots: Vec<u32> },
    /// An `EXISTS` over one to three owned plain keys, fanned out the same
    /// way. How many it counts is also the model's to predict, and by the
    /// opposite rule: a key named twice counts twice, because each name is
    /// its own command.
    PlainExists { slots: Vec<u32> },
    /// An `MGET` of one to three owned plain keys: an array, one element per
    /// name, in the order the names were written.
    ///
    /// The one command here whose reply *shape* is a function of how many
    /// replies the fan-out gathered, which is why the model checks the array's
    /// length as strictly as its contents.
    PlainMGet { slots: Vec<u32> },
    /// A `GET` of an owned plain key, held against the model.
    PlainGet { slot: u32 },
    /// A `TYPE` of an owned plain key: `string` where the model holds a
    /// value, `none` where it holds none.
    PlainType { slot: u32 },
    /// A `STRLEN` of an owned plain key: the length of what the model holds,
    /// or zero.
    PlainStrLen { slot: u32 },
    /// A `SET` of an owned volatile key: the model adopts `deadline` if it
    /// took.
    VolatileSet { slot: u32, deadline: Instant },
    /// An `EXPIRE` or a `PEXPIRE` of an owned volatile key: the model adopts
    /// `deadline` only if the server says there was a key there to take it.
    ///
    /// One check for both commands because the two differ only in the unit
    /// their argument is written in, and the model holds an instant either
    /// way.
    VolatileExpire { slot: u32, deadline: Instant },
    /// A `PERSIST` of an owned volatile key: whatever it answers, the key
    /// carries no deadline afterwards, so the model stops predicting its
    /// death.
    VolatilePersist { slot: u32 },
    /// A `GET` of an owned volatile key — the two expiration invariants.
    VolatileGet { slot: u32 },
}

/// A deadline a `SET` can ask for: the option, its argument, and what the two
/// come to in milliseconds.
struct Deadline {
    option: &'static str,
    argument: u64,
    millis: u64,
    /// Which of the contract's `SET` forms this deadline makes the command.
    /// The two options are two forms, and this is where they are told apart.
    form: &'static str,
}

/// The deadlines the workload hands out.
///
/// Spread deliberately across the run's own timescale. The short ones are
/// dead before their client has finished issuing, which is what makes a stale
/// read reachable while the server is still under load; the long ones outlive
/// the whole workload, which is what makes a spurious death reachable at all.
/// None exceeds a second, because the settle at the end waits out the longest
/// of them and every millisecond of that is paid for in ticks. Both options
/// appear because `EX` and `PX` are separate arms of the parser and separate
/// arithmetic in the handler.
const DEADLINES: [Deadline; 6] = [
    Deadline {
        option: "PX",
        argument: 1,
        millis: 1,
        form: contract::FORM_SET_PX,
    },
    Deadline {
        option: "PX",
        argument: 20,
        millis: 20,
        form: contract::FORM_SET_PX,
    },
    Deadline {
        option: "PX",
        argument: 60,
        millis: 60,
        form: contract::FORM_SET_PX,
    },
    Deadline {
        option: "PX",
        argument: 150,
        millis: 150,
        form: contract::FORM_SET_PX,
    },
    Deadline {
        option: "PX",
        argument: 300,
        millis: 300,
        form: contract::FORM_SET_PX,
    },
    // The one that outlives the settle, and the only way to reach the `EX`
    // arm of the option parser: keys given this are never decided dead, only
    // decided alive, which is the half of the invariant nothing else reaches.
    Deadline {
        option: "EX",
        argument: 1,
        millis: 1000,
        form: contract::FORM_SET_EX,
    },
];

/// The span `EXPIRE` asks for, in seconds — its argument has no finer unit,
/// so a key it touches is one whose death this run will not see. What it is
/// here for is the command itself: a deadline set by a path other than `SET`,
/// and the only source of keys certain to be *alive* late in the workload.
const EXPIRE_SECONDS: u64 = 1;

/// How many walk keys a client writes once its mutations are over.
///
/// Small, and what that gives up is worth stating. What the walk asserts is
/// that a set comes back exactly, and a systematically wrong matcher, an
/// inverted filter or a broken dedup shows up whatever the set's size. What
/// more keys would buy is *placement*: a fan-out that skipped a single shard
/// is caught only if a walk key happened to live there, and eight per client
/// covers a small fraction of the shards a deployed shape has. Covering them
/// densely enough to make that certain would put more keys in the walk family
/// than in every other family combined, which changes the shape the sweep is
/// measuring in order to catch a defect the service layer's own tests already
/// pin directly. Proving the invariant bites is a planted defect's job, not a
/// key count's.
const WALK_KEYS: u32 = 8;

/// The `COUNT` the verifier's `SCAN` asks for on the steps that carry one.
///
/// Below the ceiling the server clamps to, so the number on the wire is the
/// number the server uses — a `COUNT` above the clamp would exercise the clamp
/// instead of the option.
const WALK_SCAN_COUNT: usize = 32;

/// How many fresh keys a client writes into its own walk family between two
/// steps of its own walk.
///
/// This is the churn the concurrent invariant is stated under, and it is the
/// client's *own* family rather than a neighbour's on purpose: a key another
/// client writes cannot appear in this walk's answers at all, so it changes
/// the table and nothing else. A key of this family can appear, and whether
/// it does is exactly what the guarantee refuses to promise — a key created
/// during a walk may be returned or missed. Asserting over a family that
/// nothing added to would be asserting over the quiescent case again.
const WALK_CHURN_WRITES: u32 = 3;

/// How many churn keys the same burst removes again, oldest first.
///
/// Fewer than it writes, so the family grows: growth is what makes a table
/// double, and a doubling with a walk in flight is the case the whole cursor
/// design exists for. Removing the oldest rather than the newest is what puts
/// the removals behind the cursor, where a walk has already been.
const WALK_CHURN_DELETES: u32 = 1;

/// How many steps a client's walk takes when it is not driving its cycle to
/// the end. See [`SimConfig::concurrent_scan_cycle`].
///
/// Two, and it is a price rather than a property: every step is a round trip
/// for every client, and the walk is already the most expensive thing at the
/// tail of a run. Measured against the sweep this gate runs — 275 seeds of the
/// swept shape — two steps cost 8 % of the whole sweep's CPU and four cost
/// 20 %, against a budget with about a fifth of itself spare. What the extra
/// two steps would have bought is more of the *per-step* checks; what carries
/// the walk's result every seed is the `KEYS` that closes it, which is one
/// round trip and complete by construction, and both `SCAN` forms are on the
/// wire either way because the form is chosen per client rather than per step.
const WALK_PREFIX_STEPS: u64 = 2;

/// The `COUNT` a client's own walk asks for, on the clients that send one.
///
/// One bucket, the smallest a step can be, and that is the point: a step
/// large enough to finish a shard's table hands back cursor `0` and there is
/// no walk in flight for anything to happen underneath. Half the clients send
/// no `COUNT` at all — see [`Model::walk`] — so both parse paths are on the
/// wire in every run, and the half that names one is the half whose cursor
/// spends time between shards.
const WALK_STEP_COUNT: usize = 1;

/// How many steps a cycle-completing walk is allowed before the harness calls
/// it a walk that is not going to finish.
///
/// Not derived from the keyspace, and the churn is why: the walk grows the
/// family as it goes, so a bound stated in keys would grow with the walk and
/// could never be exceeded. What the honest cursor promises under growth is
/// not a step count but *convergence* — a doubling halves the size of every
/// later step instead of doubling the number of steps left — so the number of
/// steps it needs is bounded even while the table is not. Measured across six
/// seeds of the shape that drives a cycle: 151 to 217 steps, a tight enough
/// cluster that five times the widest of them is room a converging cursor
/// cannot use and a cursor that has stopped converging reaches on every seed.
///
/// That range is one client's cycle, and it is the half of them that names a
/// [`WALK_STEP_COUNT`] of one — a bucket at a time, which is the only rate at
/// which a cursor can be caught between steps. The half that names no `COUNT`
/// takes the server's default and finishes the same cycle in a handful of
/// steps, so it is not what the bound is sized for.
const WALK_CYCLE_STEP_BOUND: u64 = 1024;

/// Where the counter family's share of a hundred rolls ends and the plain
/// family's begins.
///
/// The two boundaries are named because they are the only numbers in the draw
/// that two functions have to agree on: [`Model::compose`] routes on them and
/// the family helpers match on the same roll rather than drawing again, so a
/// helper whose lowest arm disagreed with the boundary above it would leave a
/// band of rolls nothing composed.
const COUNTER_OPS: u32 = 18;

/// Where the plain family's share ends and the volatile family's begins. See
/// [`COUNTER_OPS`].
const PLAIN_END: u32 = 54;

/// The span `PEXPIRE` asks for, in milliseconds.
///
/// Short on purpose, and that is the whole reason both commands are in the
/// workload rather than one standing for the other. `EXPIRE`'s argument has no
/// unit finer than a second, so every key it touches outlives the run and only
/// the *alive* half of the expiration invariant ever decides one. This one
/// dies well inside a run — comfortably past [`STALE_SLACK`], so a read taken
/// afterwards is decidedly late — and is therefore the only path other than
/// `SET … PX` that can produce a stale read at all. Redis also gives the two
/// commands different ceilings, measured rather than assumed, so they are not
/// one command in two units.
const PEXPIRE_MILLIS: u64 = 150;

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
                let slot = self.plain.pick(rng);
                let value = format!("{seq}@{}", self.plain.key(slot));
                Op {
                    frame: command(&[
                        "SET",
                        &plain_key(self.plain.key(slot)),
                        &value,
                        if only_if_present { "XX" } else { "NX" },
                    ]),
                    check: Check::PlainSetCond {
                        slot,
                        value: value.into_bytes(),
                        only_if_present,
                    },
                    form: if only_if_present {
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
                Op {
                    frame: command(&[
                        "SET",
                        &volatile_key(self.volatile.key(slot)),
                        &format!("{seq}@{}", self.volatile.key(slot)),
                        deadline.option,
                        &deadline.argument.to_string(),
                    ]),
                    check: Check::VolatileSet {
                        slot,
                        deadline: sent + Duration::from_millis(deadline.millis),
                    },
                    // Carried by the deadline rather than derived from its
                    // option here: the two spellings would then be two places
                    // to keep in step, and the one that drifted would be the
                    // one nothing reads.
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
                } => {
                    let held = self.plain_state[*slot as usize].clone();
                    let took = matches!(reply, Frame::Simple(text) if text == "OK");
                    // The two answers a condition can give. Anything else is
                    // the server declining to run the command at all, which is
                    // no statement about the key and leaves the model with
                    // nothing to hold.
                    let refused = matches!(reply, Frame::Null);
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
            let mut tally = lock(&self.shared.tally);
            tally.plain_checks += 1;
            if *reply != Frame::Integer(counted) {
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
    fn check_plain_mget(&self, slots: &[u32], reply: &Frame) {
        let agrees = match reply {
            Frame::Array(values) if values.len() == slots.len() => {
                slots.iter().zip(values).all(|(slot, value)| {
                    match (&self.plain_state[*slot as usize], value) {
                        // Unpredictable on its own, and the element beside it
                        // still is: one unknown key does not excuse the rest
                        // of the array.
                        (Known::Nothing, _) => true,
                        (Known::Absent, value) => matches!(value, Frame::Null),
                        (Known::Value(expected), Frame::Bulk(got)) => got == expected,
                        (Known::Value(_), _) => false,
                    }
                })
            }
            _ => false,
        };
        let mut tally = lock(&self.shared.tally);
        tally.plain_checks += 1;
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
            Known::Value(_) => reply == present,
        };
        let mut tally = lock(&self.shared.tally);
        tally.plain_checks += 1;
        if !agrees {
            tally.plain_mismatches += 1;
        }
    }

    /// Holds a `GET` of a plain key against what this client last wrote.
    ///
    /// Sound because the family is partitioned: nothing else in the
    /// simulation writes this key, so "what I last wrote" is the whole truth
    /// about it and no schedule excuses a difference. Strict about the reply
    /// shape for the same reason the volatile check is lenient about it —
    /// there, an error frame is a question left unanswered; here, a `GET` of
    /// a key this client owns has no legitimate way to fail.
    fn check_plain(&self, slot: u32, reply: &Frame) {
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
    fn check_volatile(&self, slot: u32, reply: &Frame, sent: Instant, received: Instant) {
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
    /// Four claims, and they are the guarantee split into the parts a
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
            if listed_keys(&reply[0]) != Some((present, false)) {
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
        // so only that walk is held to having returned all of it.
        if completed && !stable.iter().all(|key| seen.contains(key)) {
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

    let reply = conn.request_many(&[command(&["KEYS", WALK_ALL])]).await?;
    let keys_agrees = listed_keys(&reply[0]) == Some((expected.clone(), false));

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
        if !scan_agrees || seen != expected {
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

/// The name of a counter key — touched only by `INCRBY`, shared by every
/// client.
fn counter_key(index: u32) -> String {
    format!("counter-{index}")
}

/// The name of a plain key — `GET`/`SET`/`DEL`, never a deadline, owned by
/// one client.
fn plain_key(index: u32) -> String {
    format!("plain-{index}")
}

/// The name of a volatile key — always written with a deadline, owned by one
/// client.
fn volatile_key(index: u32) -> String {
    format!("volatile-{index}")
}

/// The name of a walk key — written once, never expired, and named so a glob
/// isolates one client's own.
///
/// The other families are indexed by a number split between clients, which no
/// pattern can separate. A walk asserts over what one client owns, so its keys
/// carry the owner in the name.
fn walk_key(client: u16, slot: u32) -> String {
    format!("walk-{client}-{slot}")
}

/// The glob that matches exactly one client's walk keys.
fn walk_pattern(client: u16) -> String {
    format!("walk-{client}-*")
}

/// The glob that matches every walk key in the run.
///
/// `walk-*` and not `*`: the point of the assertion is set equality, and the
/// only set the harness knows exactly is this one. A walk over the whole
/// keyspace would be racing the expiry sweep for its own denominator.
const WALK_ALL: &str = "walk-*";

/// Builds a RESP2 command frame: an array of bulk strings, as a real client
/// sends.
fn command(parts: &[&str]) -> Frame {
    Frame::Array(
        parts
            .iter()
            .map(|part| Frame::Bulk(part.as_bytes().to_vec()))
            .collect(),
    )
}

/// A client's connection: the real codec over a simulated socket.
struct Conn {
    stream: turmoil::net::TcpStream,
    /// Reply bytes read from the server, resumed across reads.
    ///
    /// The same [`Decoder`] the server's own connection loop runs, for the
    /// reason its documentation gives socket readers: a one-shot `parse` over
    /// a growing buffer re-parses every already-complete element on each read
    /// that stops short of a frame. A client here reads small replies and
    /// would not feel that, but an in-tree counterexample to the codec's own
    /// advice is worth less than the twenty lines it saves.
    decoder: Decoder,
    /// Scratch for encoding, reused so a client does not allocate per request.
    out: Vec<u8>,
}

impl Conn {
    /// Opens a connection to the simulated server.
    async fn connect() -> turmoil::Result<Self> {
        Ok(Self {
            stream: turmoil::net::TcpStream::connect((SERVER, PORT)).await?,
            decoder: Decoder::new(DecoderLimits::default()),
            out: Vec::new(),
        })
    }

    /// Sends a burst of commands in one write and reads exactly that many
    /// replies, in request order.
    ///
    /// Written as one buffer rather than one write per command: a burst
    /// delivered as several messages would let the server drain each alone,
    /// which is the depth-1 shape again under a different name.
    ///
    /// `flush` after `write_all` even though turmoil's socket sends on write:
    /// a transport that buffers would otherwise hold a request the client is
    /// blocked waiting on, and that deadlock would only appear under whatever
    /// transport we ported to next.
    async fn request_many(&mut self, frames: &[Frame]) -> turmoil::Result<Vec<Frame>> {
        self.out.clear();
        for frame in frames {
            encode(frame, &mut self.out);
        }
        self.stream.write_all(&self.out).await?;
        self.stream.flush().await?;

        let mut replies = Vec::with_capacity(frames.len());
        let mut chunk = [0u8; CLIENT_CHUNK];
        while replies.len() < frames.len() {
            if let Some(reply) = self.decoder.try_next()? {
                replies.push(reply);
                continue;
            }
            let got = self.stream.read(&mut chunk).await?;
            if got == 0 {
                return Err("the server closed the connection mid-request".into());
            }
            self.decoder.feed(&chunk[..got]);
        }
        Ok(replies)
    }
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
        // they took decided one each anyway. The trace folds every command's
        // kind and every reply, so an added command changes it by
        // construction. A change here with no workload change beside it is a
        // regression, not a repin.
        const MINI_1_42: u64 = 0xa308_5f64_4e73_d96a;

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
            (54, 25, 149, 32),
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
                Plant::LostUpdate | Plant::ServeExpired | Plant::SweepEatsAll => assert_eq!(
                    place,
                    None,
                    "{} is caught where it is swept, so it has no elsewhere to name",
                    plant.name()
                ),
                Plant::ScanMissesRehash => {
                    let place =
                        place.expect("the swept shapes cannot observe an upward scan cursor");
                    assert!(
                        place.contains("planted_walk.rs"),
                        "a reader sent somewhere must be sent to a file: {place}"
                    );
                }
            }
        }
        // The place is a string, so nothing but this stops it outliving the
        // file it names — and a warning pointing at a path that is not there
        // is worse than no warning.
        assert!(
            std::path::Path::new(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/planted_walk.rs"
            ))
            .exists(),
            "the place a plant points at no longer exists"
        );
    }

    /// Which plants a sweep's violation count is evidence about, pinned as a
    /// set rather than one by one: the interesting claim is that exactly one
    /// plant is outside what the swept shapes reach, and a second one
    /// appearing is a change in what those shapes measure.
    #[test]
    fn the_scan_cursor_is_the_only_plant_the_swept_shapes_cannot_catch() {
        let unobservable: Vec<&str> = Plant::ALL
            .into_iter()
            .filter(|plant| plant.unobservable_on_swept_shapes().is_some())
            .map(Plant::name)
            .collect();
        assert_eq!(
            unobservable,
            ["scan-misses-rehash"],
            "the plants a swept violation count says nothing about have changed"
        );
    }
}
