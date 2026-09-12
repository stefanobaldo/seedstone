//! The shard policy is the one seam built to be broken: every decision a
//! shard executor consults that production has exactly one answer to, and
//! the simulator has several. The trace sink is the other observer a run
//! can replace.

use crate::dict::WalkOrder;
use crate::shard::{Command, Reply};
use tokio::time::Instant;

/// An observer of every command a shard completes.
///
/// The simulator folds these calls into a trace hash. Calls arrive in each
/// shard's own execution order, which under a deterministic scheduler is a
/// function of the seed alone — so the fold is reproducible.
pub trait TraceSink: Clone + Send + 'static {
    /// Called once per completed command, after the reply is computed and
    /// before it is sent.
    ///
    /// `seq` is the shard's replication position at which the command's
    /// effects *begin*: the position of the first record it appended, or —
    /// for a command that appended none — the position it observed without
    /// consuming.
    ///
    /// **Not "the record this command wrote".** A command may consume more
    /// than one position: a write that first had to remove a key whose
    /// deadline had passed appends the eviction's record here and its own
    /// after it, so the position reported is the eviction's. What the field
    /// carries is where in the shard's order the command's run started, which
    /// is what makes a schedule that reordered two commands visible; reading
    /// it as an index into the log would be wrong.
    fn record(&self, shard: u16, seq: u64, cmd: &Command, reply: &Reply);
}

/// A [`TraceSink`] that observes nothing. Production's sink.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoTrace;

impl TraceSink for NoTrace {
    fn record(&self, _shard: u16, _seq: u64, _cmd: &Command, _reply: &Reply) {}
}

/// Whether a deadline has come due.
///
/// Expiry is decided in two places — in front of every command
/// ([`crate::shard::apply::evict_if_expired`]) and on the housekeeping tick ([`crate::shard::executor::sweep_expired`]) —
/// and both ask this. Production has exactly one implementation, [`Deadlines`],
/// and the parameter exists so that the simulator can supply others: a plant
/// that answers wrongly *is* the defect an invariant claims to catch, where a
/// rewritten request only reproduces what that defect would look like from
/// outside. A cargo feature could not do this job — `cargo build --workspace`
/// unifies features, so the simulator's would be compiled into the shipped
/// binary — and a runtime flag would be worse.
pub trait ExpiryPolicy: Clone + Send + 'static {
    /// Is a key carrying `expires_at` due at `now`, for a command looking at it?
    fn due_on_read(&self, expires_at: Option<Instant>, now: Instant) -> bool;

    /// Is a key carrying `expires_at` due at `now`, for the active sweep?
    fn due_on_sweep(&self, expires_at: Option<Instant>, now: Instant) -> bool;

    /// May a key with no deadline at all be taken?
    ///
    /// `false` for any honest policy, and answering it opens both fast paths:
    /// a dict that has never held a deadline is neither walked by the sweep
    /// nor looked up in front of a command. A policy that takes undated keys
    /// has to answer `true` or it would observe nothing.
    fn takes_undated(&self) -> bool;
}

/// The honest policy: a key is due once `now` has *reached* its deadline, and
/// a key with no deadline is never due.
///
/// A zero-sized type, so the calls above monomorphise and inline into the
/// comparisons they replaced. It is the default of [`crate::shard::ShardPool::spawn`] and
/// [`crate::shard::ShardPool::spawn_with_log`], and the only implementation this crate ships.
#[derive(Debug, Clone, Copy, Default)]
pub struct Deadlines;

impl ExpiryPolicy for Deadlines {
    fn due_on_read(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        expires_at.is_some_and(|at| at <= now)
    }

    fn due_on_sweep(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        expires_at.is_some_and(|at| at <= now)
    }

    fn takes_undated(&self) -> bool {
        false
    }
}

impl WalkOrder for Deadlines {}

impl EvictionPolicy for Deadlines {
    fn must_evict(&self, used: u64, ceiling: Option<u64>) -> bool {
        // The same comparison `MemoryLimit::exceeded` makes, and the same
        // function, so the byte the two paths act at cannot drift apart.
        crate::memory::past_ceiling(used, ceiling)
    }
}

/// Whether the node must reclaim now.
///
/// The other half of the [`ExpiryPolicy`] seam, and it exists for the same
/// reason: a ceiling nobody has watched fail is a ceiling nobody has
/// measured. The two defects worth planting are on either side of the
/// comparison — a node that reads its ceiling and never acts on it, and one
/// that reclaims whatever it is holding — and neither is expressible by
/// rewriting a request from outside, because the decision is taken inside the
/// executor loop against a figure no client can see.
///
/// Asked *instead of* the comparison rather than beside it: the honest
/// implementation is that comparison, so a policy that answers differently is
/// the whole defect and not an imitation of one.
pub trait EvictionPolicy: Clone + Send + 'static {
    /// Whether the node must reclaim now, given the gauge and the ceiling.
    ///
    /// `ceiling` is `None` when the node is unbounded, and the honest answer
    /// is then `false` — a node with no ceiling has nothing to be over.
    fn must_evict(&self, used: u64, ceiling: Option<u64>) -> bool;
}

/// Every decision a shard executor consults that production has exactly one
/// answer for.
///
/// Three so far — when a deadline comes due, how a walk's cursor advances,
/// and whether the node must reclaim — and they travel together because they
/// are held by the same thing for the same span: one value, handed to
/// [`crate::shard::ShardPool::spawn_with_policy`] and kept for the life of the executor.
/// Splitting them into separate parameters would multiply a generic that
/// already reaches from the pool's constructor to the dict, and buy nothing:
/// a caller supplying one always has an answer for the others, and the honest
/// answer is a unit struct.
///
/// Blanket-implemented, so nothing implements this directly: a type that
/// answers all three questions is a shard policy, and there is no second thing
/// to remember to do.
pub trait ShardPolicy: ExpiryPolicy + WalkOrder + EvictionPolicy {}

impl<T: ExpiryPolicy + WalkOrder + EvictionPolicy> ShardPolicy for T {}
