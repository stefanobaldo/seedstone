//! The shard policy is the one seam built to be broken: every decision a
//! shard executor consults that production has exactly one answer to, and
//! the simulator has several. The trace sink is the other observer a run
//! can replace.

use crate::dict::WalkOrder;
use crate::shard::{Command, RefusalReport, Reply};
use tokio::time::Instant;

/// Which half of the tick's durability work failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFault {
    /// `flush` failed: the records stay buffered and are retried.
    Write,
    /// `sync` failed: what was flushed is on disk but not durable yet.
    Sync,
    /// A snapshot could not be opened, written or made durable: the cycle
    /// keeps its buffer and tries again on the next tick.
    Snapshot,
    /// A file a durable snapshot made redundant could not be removed: it is
    /// tried again at the next cycle, and at the next start.
    Remove,
    /// The segment could not be rotated: the next file could not be
    /// created, its header written or synced, or the directory synced.
    Rotate,
}

/// What one completed snapshot cycle amounts to, for the sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotReport {
    pub executor: u16,
    pub cycle: u32,
    /// Keys in the image.
    pub entries: u64,
    /// The snapshot file's size, header and footer included.
    pub bytes: u64,
    /// Housekeeping ticks the cycle spanned.
    pub ticks: u64,
    /// Every file under `wal/`, summed, at the instant before compaction
    /// removed what the snapshot made redundant: the directory's peak.
    pub disk_bytes: u64,
    /// Bytes of log written since the log crossed the size that opened the
    /// cycle, until the snapshot was durable — the `W` of the disk bound.
    /// The crossing falls between two ticks and the cycle opens on the
    /// next, so this counts what that tick's interval wrote past it too.
    pub bytes_written: u64,
    /// Shards the start reported lossy that this image covers: their
    /// durable state no longer depends on the damaged region.
    pub cleared: u64,
}

/// What one compaction removed, for the sink. The node's: the writer is the
/// only thing that removes files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionReport {
    pub files: u64,
    pub bytes: u64,
}

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

    /// Called once per shard when the node starts, after that shard's log
    /// has been replayed: `next_seq` is the position the shard resumes at,
    /// and `lossy` says whether damage on disk could have cost this shard
    /// records — a hole or a cut tail in a segment its executor wrote, or a
    /// segment that could not be read at all. A gap in an otherwise intact
    /// log is reported as a truncation but does not set it.
    ///
    /// A default that does nothing, so a sink that folds commands need not
    /// know a restart exists. The simulator's does: two runs that recovered
    /// different prefixes are different runs.
    fn recovered(&self, _shard: u16, _next_seq: u64, _lossy: bool) {}

    /// Called when a shard's log could not be written or synced on a tick.
    ///
    /// A default that does nothing: the tick has nowhere else to report to,
    /// and a sink that wants the line — the binary's — implements this.
    fn fault(&self, _shard: u16, _fault: LogFault, _error: &std::io::Error) {}

    /// Called when the node's log could not be written, synced or rotated,
    /// or a file it had made redundant could not be removed. One call per
    /// failure, from the writer; `fault` stays for an executor's snapshot.
    fn log_fault(&self, _fault: LogFault, _error: &std::io::Error) {}

    /// Called when the writer issues a sync: `round` numbers them from 1.
    /// What an executor's `sync_completed` later names by the same number.
    fn sync_issued(&self, _round: u64) {}

    /// Called when that sync completed or failed.
    fn sync_settled(&self, _round: u64) {}

    /// A sync in flight for longer than `SLOW_SYNC`; traced once per such
    /// sync, from the writer's tick, with how long it had waited.
    fn sync_slow(&self, _round: u64, _in_flight_ms: u64) {}

    /// That sync ended: `ok`, and how long it took.
    fn sync_slow_ended(&self, _round: u64, _ok: bool, _duration_ms: u64) {}

    /// Called when an executor answers the batches it held: `wrote` is how
    /// many of their commands appended to the log, `refused` whether they
    /// went out as the refusal rather than as their replies.
    fn held_answered(&self, _first_shard: u16, _wrote: u64, _refused: bool) {}

    /// Called once per executor when a snapshot cycle completes: the
    /// footer is synced, the directory is synced, and every record below
    /// each shard's base is durable through the image.
    ///
    /// A default that does nothing; the binary's sink writes the line, the
    /// simulator's folds it and holds the disk to its bound.
    fn snapshot(&self, _report: &SnapshotReport) {}

    /// Called once per executor after it removed the files a durable
    /// snapshot made redundant — its own older rotations and snapshot, or
    /// every older generation's files when it closed the generation's
    /// first round.
    fn compaction(&self, _report: &CompactionReport) {}

    /// Called once per executor when it serves writes again after refusing
    /// them: the snapshot of its memory that followed the failure is
    /// durable. The `fault` that began the refusal came before it.
    fn refusal_ended(&self, _report: &RefusalReport) {}
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
/// (`apply::evict_if_expired`) and on the housekeeping tick (`executor::sweep_expired`) —
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_compaction_report_names_no_executor_and_the_new_fault_has_a_name() {
        // The writer removes files for the node; a report that still named
        // an executor would name the wrong thing.
        let report = CompactionReport {
            files: 2,
            bytes: 40,
        };
        assert_eq!((report.files, report.bytes), (2, 40));
        let faults = [
            LogFault::Write,
            LogFault::Sync,
            LogFault::Snapshot,
            LogFault::Remove,
            LogFault::Rotate,
        ];
        assert_eq!(faults.len(), 5);
    }

    #[test]
    fn the_sinks_new_hooks_default_to_nothing() {
        // `NoTrace` must stay a zero-cost sink: every hook has a default.
        let sink = NoTrace;
        sink.log_fault(LogFault::Rotate, &std::io::Error::other("x"));
        sink.sync_issued(1);
        sink.sync_settled(1);
        sink.held_answered(0, 3, false);
    }
}
