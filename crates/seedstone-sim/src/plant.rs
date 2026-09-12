//! A deliberate defect the harness can serve its own workload through.
//!
//! Each one is the bug an invariant exists to find, and every invariant has
//! one: a guarantee nobody has watched fail is a guarantee nobody has
//! measured. Two of the three are defects *inside* the server — a policy
//! handed to the shard pool at spawn — and the policies themselves live here
//! beside the enum that names them.

use seedstone_core::dict::WalkOrder;
use seedstone_core::shard::{Deadlines, EvictionPolicy, ExpiryPolicy};
use tokio::time::Instant;

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
/// [`Plant::LostUpdate`] stays above the shard, in [`crate::PlantedRouter`], and that
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
    /// **Nothing this harness runs catches it any more, and that is a change
    /// to the walk rather than to this.** Observing it needs a cursor caught
    /// *between* steps of a table growing under it, and what used to put it
    /// there was a client `COUNT` of one meaning one bucket a call. A `SCAN`
    /// call now spends a bucket ceiling of the server's own, which covers a
    /// simulated shard's whole table several times over before it answers.
    /// Bounding each envelope by the key target instead was tried and does not
    /// bring it back: the *call* still loops until its target is met. The shape
    /// that would observe it is a production-sized one — a table of millions of
    /// buckets, where a call's ceiling is a rounding error against the width of
    /// the walk — which is not a shape a simulator can afford.
    ///
    /// So the plant stays selectable, stays classified, and the claim it used
    /// to carry end to end is made where one dict and no network can make it:
    /// `an_upward_cursor_is_outrun_by_a_table_growing_under_it`, in
    /// `crates/seedstone-core/src/dict.rs`. See
    /// [`Plant::unobservable_on_swept_shapes`], which says so to anyone who
    /// serves it.
    ScanMissesRehash,
    /// A node that reads its ceiling and never acts on it: the gauge climbs
    /// past `maxmemory` and nothing is ever reclaimed. Caught by
    /// `ceiling_breaches`, and only on a shape that has a ceiling — see
    /// `tests/planted_eviction.rs`.
    IgnoresCeiling,
    /// A node that reclaims whatever it is holding, ceiling or no ceiling.
    ///
    /// Caught by the plain model wherever it runs, which is what makes it the
    /// counterpart of the one above: the honest policy never evicts without a
    /// ceiling, so on a shape with none this defect is a written key that
    /// answers `nil`, and nothing excuses that.
    EvictsBelowCeiling,
    /// A `SCAN` call that steps *over* every odd shard instead of into it.
    ///
    /// The server answers a step that crossed into an odd-numbered shard as
    /// already spent — no keys, one bucket visited, cursor `0` — so the call
    /// moves straight on to the shard after it. Every cursor it hands back is
    /// still well-formed and still moves forward; every key it returns is still
    /// a real key. What it loses is half the keyspace, and the client is never
    /// told.
    ///
    /// The defect the crossing made possible and the code before it could not
    /// have had: a walk that answered from one shard per call had no shard to
    /// step over. It is caught by **at-least-once on a completed cycle**, which
    /// is the only claim it breaks, and that is why it needs a shape whose
    /// clients drive their cycle to the end over more than one shard — see
    /// [`crate::SimConfig::crossing`] and `tests/planted_crossing.rs`.
    ///
    /// Confined to steps the call *crossed into*: a shard resumed at `0` with
    /// less than the whole bucket ceiling left, which is a step no first call
    /// and no `KEYS` ever sends. Widening it to every step at cursor `0` would
    /// break `KEYS` too — a broadcast that walks each shard from `0` on a full
    /// budget — and a defect that breaks a command predating the crossing is no
    /// longer a statement about the crossing.
    CrossingSkipsShard,
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
            Self::IgnoresCeiling => "ignores-ceiling",
            Self::EvictsBelowCeiling => "evicts-below-ceiling",
            Self::CrossingSkipsShard => "crossing-skips-shard",
        }
    }

    /// Every plant, so a caller listing or sweeping them cannot miss one
    /// added later.
    pub const ALL: [Self; 7] = [
        Self::LostUpdate,
        Self::ServeExpired,
        Self::SweepEatsAll,
        Self::ScanMissesRehash,
        Self::IgnoresCeiling,
        Self::EvictsBelowCeiling,
        Self::CrossingSkipsShard,
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
            // `EvictsBelowCeiling` joins them: it evicts on every shape,
            // including the ones with no ceiling, where the plain model is
            // exact and a vanished key is a mismatch with nothing to excuse
            // it.
            Self::LostUpdate
            | Self::ServeExpired
            | Self::SweepEatsAll
            | Self::EvictsBelowCeiling => None,
            // Needs a cursor observed *between* steps of a table that is
            // growing under it, and no shape this harness can afford leaves one
            // there: a call spends the server's whole bucket ceiling, which
            // covers a simulated shard's table several times over. Not a
            // narrower shape but a *deeper* one — so the place named is not a
            // shape at all, it is the unit test at the dict where the same
            // claim costs one table and no network. See this plant's own note.
            Self::ScanMissesRehash => Some(
                "an_upward_cursor_is_outrun_by_a_table_growing_under_it, in \
                 crates/seedstone-core/src/dict.rs — no shape this harness \
                 sweeps or walks can catch it end to end",
            ),
            // A node that ignores a ceiling it does not have is a node
            // behaving honestly. The swept shapes set no `maxmemory`, so
            // nothing they do can tell this defect from the real thing.
            Self::IgnoresCeiling => {
                Some("SimConfig::eviction, swept by crates/seedstone-sim/tests/planted_eviction.rs")
            }
            // At-least-once is the only claim a skipped shard breaks, and only
            // a walk that reached the end of its cycle makes it. All three
            // swept shapes leave `concurrent_scan_cycle` off — a complete cycle
            // is a test's to ask for, not a sweep's — so no walk they run ever
            // gets to that claim, whatever their shard count.
            Self::CrossingSkipsShard => Some(
                "SimConfig::crossing, walked by crates/seedstone-sim/tests/planted_crossing.rs",
            ),
        }
    }
}

/// A server whose liveness check and sweep both stopped firing.
///
/// The deadline is accepted, stored and never acted on. `stale_reads` owns it.
#[derive(Clone, Copy)]
pub struct ServeExpired;

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

impl EvictionPolicy for ServeExpired {
    fn must_evict(&self, used: u64, ceiling: Option<u64>) -> bool {
        Deadlines.must_evict(used, ceiling)
    }
}

/// A sweep that stopped asking whether an entry had a deadline at all.
///
/// Everything it reaches is due, undated keys included — which is why it must
/// answer `takes_undated` yes, or the dict it is walking would never be
/// walked. The read path stays honest: this defect is the sweep's alone, and a
/// plant that broke both would not tell the two invariants apart.
#[derive(Clone, Copy)]
pub struct SweepEatsAll;

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

impl EvictionPolicy for SweepEatsAll {
    fn must_evict(&self, used: u64, ceiling: Option<u64>) -> bool {
        Deadlines.must_evict(used, ceiling)
    }
}

/// A cursor that counts buckets upwards instead of advancing in reverse
/// binary order.
///
/// Honest about deadlines, and it has to be: what this plant is about is a
/// walk that cannot finish, and a walk whose keyspace was also disappearing
/// underneath it would leave the two indistinguishable.
#[derive(Clone, Copy)]
pub struct ScanMissesRehash;

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

impl EvictionPolicy for ScanMissesRehash {
    fn must_evict(&self, used: u64, ceiling: Option<u64>) -> bool {
        Deadlines.must_evict(used, ceiling)
    }
}

/// A node that never reclaims: the ceiling is configured, read and ignored.
///
/// The cache that takes the host down. Everything else about it is honest —
/// deadlines fire, the sweep walks — so the only thing a run can be
/// disagreeing about is the ceiling, and the only counter that can move is
/// the one that watches it.
#[derive(Clone, Copy)]
pub struct IgnoresCeiling;

impl WalkOrder for IgnoresCeiling {}

impl ExpiryPolicy for IgnoresCeiling {
    fn due_on_read(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        Deadlines.due_on_read(expires_at, now)
    }
    fn due_on_sweep(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        Deadlines.due_on_sweep(expires_at, now)
    }
    fn takes_undated(&self) -> bool {
        Deadlines.takes_undated()
    }
}

impl EvictionPolicy for IgnoresCeiling {
    fn must_evict(&self, _used: u64, _ceiling: Option<u64>) -> bool {
        false
    }
}

/// A node that reclaims whenever it holds anything: the ceiling is
/// irrelevant.
///
/// The cache whose hit ratio is inexplicably bad. It fires with `ceiling:
/// None` too, which is the point — the honest policy never evicts without a
/// ceiling, so this defect is visible on every shape and not only on the one
/// that has a ceiling to be under.
#[derive(Clone, Copy)]
pub struct EvictsBelowCeiling;

impl WalkOrder for EvictsBelowCeiling {}

impl ExpiryPolicy for EvictsBelowCeiling {
    fn due_on_read(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        Deadlines.due_on_read(expires_at, now)
    }
    fn due_on_sweep(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        Deadlines.due_on_sweep(expires_at, now)
    }
    fn takes_undated(&self) -> bool {
        Deadlines.takes_undated()
    }
}

impl EvictionPolicy for EvictsBelowCeiling {
    fn must_evict(&self, _used: u64, _ceiling: Option<u64>) -> bool {
        true
    }
}
