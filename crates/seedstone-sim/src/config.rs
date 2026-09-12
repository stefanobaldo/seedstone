//! What one simulation is: seeds, shape, duration, ceiling, plant.
//!
//! Every knob a sweep varies is a field here, and [`SimConfig::mini`] is the
//! shape the pinned hash is taken on.

use crate::Plant;

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
    /// The ceiling the node's keyspace is held under, or `None` for a node
    /// with no ceiling at all.
    ///
    /// `None` on every shape but [`SimConfig::eviction`], and that is what
    /// makes the plain model's tolerance safe: a shape with no ceiling cannot
    /// legitimately lose a key, so `nil` for a written key stays a mismatch
    /// there. Only the shape that set a ceiling excuses one — see
    /// [`crate::SimOutcome::evictions_observed`].
    pub maxmemory: Option<u64>,
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
            maxmemory: None,
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
    /// see [`crate::WALK_CYCLE_STEP_BOUND`].
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
            maxmemory: None,
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
            maxmemory: None,
        }
    }

    /// A shape narrow enough to cross a ceiling many times in one run.
    ///
    /// Sixteen shards, not a thousand: the table overhead of a thousand empty
    /// dicts would be most of any small ceiling, and the shard dimension is
    /// not what this shape is about. The keys are `mini`'s; the ceiling is
    /// set so the steady-state keyspace is roughly twice what fits, which
    /// makes eviction the common case rather than an edge the run brushes
    /// once. Calibrated by `tests/planted_eviction.rs`, which requires every
    /// honest seed to evict something and to decide plain checks all the
    /// same.
    #[must_use]
    pub const fn eviction(workload_seed: u64, sim_seed: u64) -> Self {
        Self {
            shards: 16,
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
            maxmemory: Some(24 * 1024),
        }
    }

    /// A shape wide enough for one call to cross several shards, and narrow
    /// enough for a client to reach the end of its cycle.
    ///
    /// Sixteen shards, not one and not a thousand, and both bounds are the
    /// point. One shard has no crossing in it at all: a call that never leaves
    /// the shard it started on cannot step over the next one. A thousand puts
    /// the cycle out of reach — the walk keys of a client are eight, so a
    /// thousand shards hold nothing between them and the run pays for the
    /// crossing without ever completing a cycle to check it. Sixteen is enough
    /// shards that a call's bucket budget crosses most of them and the ones it
    /// does not are still ahead of the cursor.
    ///
    /// `concurrent_scan_cycle` is on, and that is what this shape buys: the
    /// walk's at-least-once claim is made only by a walk that reached the end
    /// of its cycle, and it is the only claim a call that skipped a shard
    /// breaks. Everything else a skipped shard leaves intact — the keys it did
    /// return are real, the cursor did move, and the closing `KEYS` is a
    /// broadcast that crosses nothing. See `tests/planted_crossing.rs`.
    ///
    /// `eviction`'s body with the ceiling dropped: a shape whose walks must
    /// come back with everything cannot be one where a key is allowed to
    /// vanish underneath them.
    #[must_use]
    pub const fn crossing(workload_seed: u64, sim_seed: u64) -> Self {
        Self {
            shards: 16,
            executors: 4,
            clients: 16,
            plain_keys: 64,
            volatile_keys: 32,
            counter_keys: 4,
            ops_per_client: 40,
            pipeline_depth: 8,
            workload_seed,
            sim_seed,
            quiescent_walk: false,
            concurrent_scan_cycle: true,
            planted: None,
            maxmemory: None,
        }
    }
}
