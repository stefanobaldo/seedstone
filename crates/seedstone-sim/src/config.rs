//! What one simulation is: seeds, shape, duration, ceiling, plant.
//!
//! Every knob a sweep varies is a field here, and [`SimConfig::mini`] is the
//! shape the pinned hash is taken on.

use crate::Plant;
use crate::durability::CrashSchedule;
use crate::trace::{GOLDEN, mix};
use seedstone_core::dict::{BUCKET_OVERHEAD, ENTRY_OVERHEAD};
use seedstone_core::shard::SyncPolicy;

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
    /// When the driver crashes the node.
    pub crashes: CrashPlan,
    /// What the disk does to the node's log.
    pub disk: DiskFaults,
    /// Which durability policy the node runs under.
    pub fsync: FsyncDraw,
}

/// How a run's durability policy is chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncDraw {
    /// This policy, whatever the seed.
    Fixed(SyncPolicy),
    /// One of the three, drawn from the simulator seed: the policy is a
    /// dimension of the seed, so a sweep covers all three without a seed
    /// more.
    PerSeed,
}

/// When the driver crashes the server host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPlan {
    /// Never.
    None,
    /// Once, after every client has paused and the log has had time to
    /// sync: recovery must then be exact, and the model checks it is.
    AtRest,
    /// Up to `max` times, at instants drawn from the simulator seed inside
    /// the workload window. What survives is a prefix of what was
    /// acknowledged; what was acknowledged before a shard's last sync
    /// survives outright.
    UnderLoad {
        /// The most crashes one run may draw.
        max: u8,
    },
    /// One of the other two, drawn from the simulator seed: `UnderLoad {
    /// max }` on about half the seeds, `AtRest` on the rest — and on a seed
    /// whose draw under load crashes nothing, so that every seed crashes.
    /// See [`SimConfig::crash_plan`].
    PerSeed {
        /// The most crashes a seed drawn under load may draw.
        max: u8,
    },
}

/// What the simulated disk does to the node.
///
/// Probabilities in permille so the config stays `Eq`: a trace hash is
/// comparable only between identical configurations, and a float has no
/// `Eq` to promise that with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskFaults {
    /// Writes tear at this granularity on a crash, or not at all.
    ///
    /// Smaller than any record, so a torn record is possible: at a block
    /// larger than a record every write is one block and tears degenerate
    /// to all-or-nothing.
    pub block_size: Option<u64>,
    /// Probability, in permille, that a read or write fails with `EIO`.
    pub io_error_permille: u16,
    /// Probability, in permille, that a read returns corrupted bytes.
    pub corruption_permille: u16,
    /// The range, in milliseconds and both ends included, that a deferred
    /// sync's latency is drawn from; `(0, 0)` for syncs that take no time.
    pub sync_latency_ms: (u64, u64),
}

impl DiskFaults {
    /// Whether this disk fails or corrupts, rather than only tearing what
    /// was never synced.
    #[must_use]
    pub const fn lies(&self) -> bool {
        self.io_error_permille > 0 || self.corruption_permille > 0
    }

    /// A disk that does what it is told.
    pub const NONE: Self = Self {
        block_size: None,
        io_error_permille: 0,
        corruption_permille: 0,
        sync_latency_ms: (0, 0),
    };
    /// A disk that tears pending writes on a crash and nothing else: the
    /// swept shape's disk, where the strong invariant is asserted.
    ///
    /// What it tears is the unsynced tail: the log is flushed per envelope
    /// and synced behind it, so a crash nearly always lands with bytes the
    /// sync in flight, or the next one, had not yet covered. A deferred sync
    /// takes 1–250 ms, drawn per sync: the top is past one housekeeping
    /// tick, so a crash can land inside a flight. Read on `standard` seeds
    /// 1–6 with the policy fixed, on 2026-10-02: under `always` and under
    /// `interval` every crash of those seeds (5 of 5, on four seeds) landed
    /// with a sync in flight; under `never`, which issues none, no crash did.
    pub const TORN: Self = Self {
        block_size: Some(32),
        io_error_permille: 0,
        corruption_permille: 0,
        sync_latency_ms: (1, 250),
    };
    /// A disk that also fails and lies: the `hostile` shape's, where only
    /// the weak invariant can be asserted.
    ///
    /// A write or a read on it fails at `io_error_permille`, through
    /// turmoil; a sync fails at the same rate, drawn by `SimDisk` from the
    /// run's own stream, because turmoil 0.7.2's sync checks no
    /// probability (read against its source on 2026-10-02; before this
    /// draw existed, `hostile` seeds 1–24 at `--fsync always` met 0 sync
    /// faults against 582 write faults). So a refusal here begins at a
    /// write, a sync or a rotation, and the checkpoint's footer sync can
    /// fail too.
    pub const HOSTILE: Self = Self {
        block_size: Some(32),
        io_error_permille: 20,
        corruption_permille: 20,
        sync_latency_ms: (1, 250),
    };
}

/// How many entries [`SimConfig::eviction`]'s ceiling holds: about half of
/// the shape's steady-state keyspace — 256 plain keys, 128 volatile, the
/// counters and the walk keys.
///
/// The middle of the range, 166 to 191, over which every seed
/// `tests/planted_eviction.rs` runs evicts, observes an eviction, and still
/// decides more than ten plain reads per eviction observed. The edges of
/// that range move with the schedule, so the middle is the value a small
/// change to the schedule leaves calibrated — and a change to how the
/// simulated node schedules its connections moved them, from 190 to 202.
const EVICTION_ENTRIES: u64 = 178;

/// What one of that shape's entries is accounted at: the dict's fixed
/// overhead, the bucket a load factor of one gives each entry on average,
/// and about sixteen bytes of key and value — `plain-<index>` holding
/// `<seq>@<index>`.
const EVICTION_ENTRY_BYTES: u64 = ENTRY_OVERHEAD + BUCKET_OVERHEAD + 16;

impl SimConfig {
    /// The sweep configuration: the shape measured to be schedule-sensitive,
    /// which is what makes a seed sweep find anything.
    ///
    /// Every seed of the gate now crashes the node up to twice, under load,
    /// and holds it to the strong invariant: everything acknowledged before
    /// a shard's last sync survives, and what survives is a prefix of what
    /// was acknowledged. What a crash costs here is the log's unsynced
    /// buffer, cut at a record boundary — see [`DiskFaults::TORN`] for why
    /// nothing is torn. The crash count is drawn uniformly from none, one
    /// or two, so about a third of the seeds draw no crash and keep the
    /// exact counter sum.
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
            crashes: CrashPlan::UnderLoad { max: 2 },
            disk: DiskFaults::TORN,
            fsync: FsyncDraw::PerSeed,
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
    /// see `workload::WALK_CYCLE_STEP_BOUND`.
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
            crashes: CrashPlan::None,
            disk: DiskFaults::NONE,
            fsync: FsyncDraw::Fixed(SyncPolicy::INTERVAL),
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
            crashes: CrashPlan::None,
            disk: DiskFaults::NONE,
            fsync: FsyncDraw::Fixed(SyncPolicy::INTERVAL),
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
    ///
    /// The ceiling is stated in entries and priced through the dict's own
    /// constants — see `EVICTION_ENTRIES` — so a change to what one entry
    /// is accounted at keeps the calibration instead of quietly tightening
    /// or loosening it.
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
            maxmemory: Some(EVICTION_ENTRIES * EVICTION_ENTRY_BYTES),
            crashes: CrashPlan::None,
            disk: DiskFaults::NONE,
            fsync: FsyncDraw::Fixed(SyncPolicy::INTERVAL),
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
            crashes: CrashPlan::None,
            disk: DiskFaults::NONE,
            fsync: FsyncDraw::Fixed(SyncPolicy::INTERVAL),
        }
    }

    /// `mini`'s keys and clients over sixteen shards, on a disk that tears,
    /// fails and lies, with the node crashed under load on some seeds and
    /// at rest on the others.
    ///
    /// The shape where the weak invariant is measured — no phantom value,
    /// every loss reported, the node up after every restart — and the only
    /// one where a hole can sit inside the durable region.
    ///
    /// Both crashes, because each decides what the other cannot. A crash
    /// under load tears the log's last record, which lies after every
    /// shard's highest record at the restart, so recovery charges every
    /// shard with a possible loss and every durable read is excused: such a
    /// seed decides no phantom and no lost restart, but nothing about
    /// survival. A crash at rest, after the log had time to sync, tears
    /// nothing, so recovery charges only what the disk's own faults took,
    /// and the reads that follow decide whether a write acknowledged before
    /// its shard's last sync came back. Sixteen shards, `eviction`'s count:
    /// the shard dimension is not what this shape is about, and a thousand
    /// cost a seed more for no read more.
    ///
    /// Calibrated by `tests/planted_recovery.rs`: every honest seed meets a
    /// fault, and at least `DECIDING_SEEDS` of them decide a durable read.
    #[must_use]
    pub const fn hostile(workload_seed: u64, sim_seed: u64) -> Self {
        Self {
            shards: 16,
            crashes: CrashPlan::PerSeed { max: 2 },
            disk: DiskFaults::HOSTILE,
            fsync: FsyncDraw::PerSeed,
            ..Self::mini(workload_seed, sim_seed)
        }
    }

    /// The durability policy this run's node runs under.
    ///
    /// Drawn from its own derivation of the simulator seed, so that the
    /// draw moves nothing else the seed decides.
    #[must_use]
    pub const fn policy(&self) -> SyncPolicy {
        match self.fsync {
            FsyncDraw::Fixed(policy) => policy,
            FsyncDraw::PerSeed => {
                const POLICIES: [SyncPolicy; 3] =
                    [SyncPolicy::ALWAYS, SyncPolicy::INTERVAL, SyncPolicy::NEVER];
                POLICIES[(mix(GOLDEN.rotate_left(41), self.sim_seed) % 3) as usize]
            }
        }
    }

    /// When this run's driver crashes the node, with a per-seed draw
    /// resolved to the plan it drew.
    ///
    /// Drawn from its own derivation of the simulator seed, as
    /// [`policy`](Self::policy) is, so that the draw moves nothing else the
    /// seed decides. A seed drawn under load whose schedule holds no crash
    /// crashes at rest instead: a seed that never crashes decides nothing
    /// a crash leaves behind, and on a disk whose faults are a draw too it
    /// may meet nothing hostile at all.
    #[must_use]
    pub fn crash_plan(&self) -> CrashPlan {
        match self.crashes {
            CrashPlan::PerSeed { max } => {
                let under_load = CrashPlan::UnderLoad { max };
                if mix(GOLDEN.rotate_left(53), self.sim_seed).is_multiple_of(2)
                    || CrashSchedule::draw(under_load, self.sim_seed).is_empty()
                {
                    CrashPlan::AtRest
                } else {
                    under_load
                }
            }
            plan => plan,
        }
    }
}
