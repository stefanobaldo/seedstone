//! What one simulation reports: the trace hash, the verifier's counts, and
//! the shared tallies the clients write into while it runs.

use crate::config::DiskFaults;
use crate::disk::SimDisk;
use crate::durability::{CrashRecord, DurablePoint};
use crate::trace::GOLDEN;
use rand::SeedableRng;
use rand::rngs::ChaCha8Rng;
use seedstone_core::log::checkpoint::CheckpointConfig;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    /// Reads of a plain key that answered `nil` where this client's model
    /// held a value, on a shape with a ceiling.
    ///
    /// The model side of eviction: what a client can see of it without being
    /// told. Excused rather than counted as a mismatch — but counted, because
    /// a tolerance nobody measures is a tolerance that could be swallowing
    /// the invariant whole.
    pub evictions_observed: u64,
    /// The node's own `evicted_keys`, read from the verifier's final `INFO
    /// stats`.
    ///
    /// The server side of the same story, and the two are compared rather
    /// than each trusted alone: a node that reclaimed nothing cannot have
    /// been the reason a key went missing.
    pub evicted_keys: u64,
    /// The microseconds the *shards* charged to the commands they ran, summed
    /// over every command name the edge does not count for itself, from the
    /// verifier's final `INFO`.
    ///
    /// **Always zero, and that is the claim.** A handler cannot `await`, so
    /// no simulated instant passes between the reading an envelope takes on
    /// arrival and the reading taken after each of its commands. It is what
    /// keeps `usec` out of `trace::fold_reply` and a replay byte-stable, and it is
    /// a property of the runtime rather than of the code that reads it — so
    /// `tests/command_timing.rs` asserts it, where a runtime that started
    /// advancing the clock inside a handler fails loudly.
    ///
    /// The edge's own timings are excluded because they are not the same
    /// claim: an `MGET` is timed across the wait for the shards it reached,
    /// and simulated time passes during a wait by design.
    ///
    /// Zero on every shape whose verifier takes no final `INFO` — see
    /// [`SimOutcome::executor_calls`], which is what tells that apart from a
    /// measured zero.
    pub executor_usec: u64,
    /// The calls those microseconds were spent over, from the same document.
    ///
    /// The denominator [`SimOutcome::executor_usec`]'s zero is only a
    /// measurement against: a run that read no document, or read one before
    /// any command had run, reports zero for both.
    pub executor_calls: u64,
    /// Readings of `used_memory` that were past `maxmemory`.
    pub ceiling_breaches: u64,
    /// Readings of `used_memory` taken at all — the denominator of
    /// [`SimOutcome::ceiling_breaches`].
    pub ceiling_checks: u64,
    /// Whether this run's node had a ceiling at all.
    ///
    /// What lets [`SimOutcome::invariants_were_exercised`] ask for a ceiling
    /// check on the shape that has one and not on the shapes that do not: a
    /// zero means two different things either side of this flag.
    pub evictable: bool,
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
    /// Restarts of the node this run observed: one per crash whose
    /// recovery reached the trace.
    pub recoveries: u64,
    /// Crashes the driver inflicted.
    pub crashes: u64,
    /// The least the counters may sum to after the run's crashes: every
    /// increment no crash could have taken, plus every negative one a crash
    /// may have left standing. Meaningful only where [`crashes`] is not
    /// zero.
    ///
    /// [`crashes`]: SimOutcome::crashes
    pub counter_floor: i64,
    /// The most they may sum to: the same, with the positive ones. The
    /// deltas are of either sign, so a lost increment can move the sum
    /// either way.
    pub counter_ceiling: i64,
    /// Shards that resumed at or below the sequence that was durable when
    /// the node last crashed: a durable write that did not survive.
    pub lost_durable_prefixes: u64,
    /// Of those, the ones the node's recovery did not report as lossy.
    pub unreported_losses: u64,
    /// Reads of a value a crash left known to be durable that disagreed, on
    /// a shard the recovery did not report.
    pub lost_durable_writes: u64,
    /// The same disagreements on a shard the recovery did report, and the
    /// volatile keys written before a crash that read dead inside their
    /// live band on such a shard.
    pub excused_losses: u64,
    /// Reads decided against a value a crash left known to be durable — the
    /// denominator of the two above.
    pub durable_checks: u64,
    /// Reads that returned none of the candidates a crash left open: a
    /// value nobody wrote.
    pub phantom_writes: u64,
    /// Reads decided against several candidates — the denominator of
    /// [`SimOutcome::phantom_writes`].
    pub either_checks: u64,
    /// Flush failures the node reported.
    pub write_faults: u64,
    /// Sync failures the node reported.
    pub sync_faults: u64,
    /// Server host starts that failed on the disk and were retried.
    pub start_failures: u64,
    /// Whether the run's disk could fail and lie, which is what decides
    /// whether a reported loss is excused.
    pub hostile: bool,
    /// Snapshot cycles that reached a durable footer, over every executor
    /// and every start.
    pub snapshot_cycles: u64,
    /// Compactions reported, over every executor and every start.
    pub compactions: u64,
    /// Files compaction removed at run time.
    pub files_removed: u64,
    /// The largest snapshot any executor made durable: the `S` of the bound.
    pub max_snapshot_bytes: u64,
    /// The most log any one cycle saw written during it: the `W` of the
    /// bound.
    pub max_written_during: u64,
    /// The most bytes `wal/` held at any instant the node measured it: at
    /// each snapshot's completion, before its compaction.
    pub disk_peak_bytes: u64,
    /// What [`disk_bound`] allows this run, from the node's own readings.
    pub disk_bound_bytes: u64,
    /// Snapshot writes, syncs and rotations that failed.
    pub snapshot_faults: u64,
    /// Removals that failed.
    pub remove_faults: u64,
    /// Snapshots a start refused: no footer (a crash mid-cycle), damage,
    /// or counts that did not match.
    pub snapshots_refused_at_start: u64,
    /// Files a start removed because nothing used them.
    pub files_removed_at_start: u64,
}

impl SimOutcome {
    /// Whether every invariant the run measures held.
    ///
    /// The counter sum first: `INCRBY` is order-independent, so a schedule
    /// cannot legitimately move it and any difference is an acknowledged
    /// increment that did not survive. Then the keyspace invariants, each of
    /// which a schedule is equally powerless to excuse — a deadline is a
    /// deadline, a key nobody else can write is what its owner last wrote,
    /// and a walk over a set nobody is touching returns that set. Then what
    /// a crash may not do: lose a write a sync covered (on the disk that only
    /// tears), lose one without saying so (on the disk that also fails and
    /// lies), or bring back a value nobody wrote (on any disk).
    #[must_use]
    pub const fn invariant_holds(&self) -> bool {
        // The counter sum is claimed only where nothing can reclaim a
        // counter. Under a ceiling it is not merely weaker, it is
        // unstateable: a counter key can be evicted, its accumulated value
        // goes with it, and the deltas are of either sign — so no inequality
        // survives either. Nor can a client repair the model as the plain
        // family's does: the sum is shared, no client knows when a key
        // vanished, and an increment landing on the key afterwards recreates
        // it holding a value that is right for nobody's arithmetic. The
        // shapes that sweep for lost updates are the ones with no ceiling,
        // which is where that invariant is measured — see
        // `tests/planted_race.rs`.
        //
        // Under a crash it is a range rather than an equality: an increment
        // acknowledged after its shard's last sync may be gone, and one
        // whose reply the crash took may have landed. Every increment every
        // later crash found synced is owed; each of the others may add or
        // take away its own delta. Sound — every sum a crash can leave lies
        // in the range — and weaker than equality, which stays the claim on
        // runs without one.
        let counters = if self.evictable {
            true
        } else if self.crashes == 0 {
            self.expected_sum == self.actual_sum
        } else {
            self.counter_floor <= self.actual_sum && self.actual_sum <= self.counter_ceiling
        };
        // Survival on the disk that only tears: a write a successful sync
        // covered came back, by the server's own numbers and by the
        // clients' reads. On the disk that also fails and lies, read
        // corruption can destroy a durable record and no honest server can
        // promise otherwise — what it owes there is to say so, by its own
        // numbers and by what its clients read back: a recovery that resumes
        // at the right position with the wrong records under it is a loss
        // the server's numbers cannot see.
        let durability = if self.hostile {
            self.unreported_losses == 0 && self.lost_durable_writes == 0
        } else {
            self.lost_durable_prefixes == 0 && self.lost_durable_writes == 0
        };
        counters
            && durability
            // A value no acknowledged write produced is never excused, on
            // any disk: that is a replay that invented a record.
            && self.phantom_writes == 0
            // The disk never exceeded what the formula allows it, from
            // the node's own readings of its snapshots and its directory.
            && self.disk_peak_bytes <= self.disk_bound_bytes
            && self.stale_reads == 0
            && self.spurious_deaths == 0
            && self.plain_mismatches == 0
            && self.walk_mismatches == 0
            && self.ceiling_breaches == 0
            // A key the model saw vanish that the node never reclaimed is a
            // key that vanished for some other reason, and the tolerance
            // above was wrong to excuse it. Stated as an inequality rather
            // than an equality because the two count different things: the
            // node evicts keys nobody reads back, and one client's reads are
            // a sample of what it took.
            && self.evicted_keys >= self.evictions_observed
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
            // Only where there is a ceiling to check against. On a shape with
            // none, a zero here is the honest answer and not a harness that
            // measured nothing.
            && (!self.evictable || self.ceiling_checks > 0)
            // A run that crashed must have restarted, and read something
            // back against what the crash left.
            && (self.crashes == 0
                || (self.recoveries > 0 && self.durable_checks + self.either_checks > 0))
            // A run whose node never completed a snapshot cycle measured
            // nothing about compaction or the bound.
            && self.snapshot_cycles > 0
    }
}

/// Bytes the bound allows beyond the formula: segment and snapshot
/// headers, `GENERATION`, `LOCK`.
pub const DISK_SLACK: u64 = 4096;

/// What the directory may hold, from the design's bound.
///
/// Per executor, twice the largest snapshot (the previous and the one in
/// progress), plus the live log at the trigger, plus what one cycle saw
/// written; that once per process the run started, plus a slack for
/// headers, `GENERATION` and `LOCK`. Once per process because a process's
/// files stay until a later one completes its first round of snapshots,
/// and a crash can land before that round closes: the simulator measures
/// seeds where a start completed no cycle at all between two crashes, and
/// three generations' files then share the directory.
#[must_use]
pub fn disk_bound(
    executors: u16,
    config: CheckpointConfig,
    max_snapshot: u64,
    max_written: u64,
    crashes: u64,
) -> u64 {
    let per_executor = 2 * max_snapshot
        + config.floor.max(config.ratio.saturating_mul(max_snapshot))
        + max_written;
    u64::from(executors) * per_executor * (1 + crashes) + DISK_SLACK
}

/// A run that observed nothing.
///
/// The sweep's tests ask how many runs it keeps in flight, not what any of
/// them found, and answering that takes several thousand seeds; that many
/// real simulations would answer it no better and never be run. The
/// verdict's tests start from it and set only the fields they judge.
#[cfg(test)]
pub const fn nothing_observed() -> SimOutcome {
    SimOutcome {
        trace_hash: 0,
        expected_sum: 0,
        actual_sum: 0,
        stale_reads: 0,
        spurious_deaths: 0,
        plain_mismatches: 0,
        dead_checks: 0,
        alive_checks: 0,
        plain_checks: 0,
        walk_mismatches: 0,
        walk_checks: 0,
        evictions_observed: 0,
        evicted_keys: 0,
        executor_usec: 0,
        executor_calls: 0,
        ceiling_breaches: 0,
        ceiling_checks: 0,
        evictable: false,
        forms_emitted: BTreeSet::new(),
        recoveries: 0,
        crashes: 0,
        counter_floor: 0,
        counter_ceiling: 0,
        lost_durable_prefixes: 0,
        unreported_losses: 0,
        lost_durable_writes: 0,
        excused_losses: 0,
        durable_checks: 0,
        phantom_writes: 0,
        either_checks: 0,
        write_faults: 0,
        sync_faults: 0,
        start_failures: 0,
        hostile: false,
        snapshot_cycles: 0,
        compactions: 0,
        files_removed: 0,
        max_snapshot_bytes: 0,
        max_written_during: 0,
        disk_peak_bytes: 0,
        disk_bound_bytes: DISK_SLACK,
        snapshot_faults: 0,
        remove_faults: 0,
        snapshots_refused_at_start: 0,
        files_removed_at_start: 0,
    }
}

/// What the client hosts and the verifier write into, and the run reads out.
///
/// Every host in a turmoil simulation runs on the same OS thread, so this
/// mutex is never actually contended; it is here because [`TraceSink`] and
/// the futures turmoil holds must be `Send`.
#[derive(Clone)]
pub struct Shared {
    /// The counters every host adds to.
    pub tally: Arc<Mutex<Tally>>,
    /// Every walk key whose write the server acknowledged, from every client.
    ///
    /// The verifier's walk asserts set equality over the whole family, and it
    /// cannot derive the family from the configuration: a write the server
    /// refused is a key that is legitimately absent, and a model that assumed
    /// otherwise would report a violation the system never committed. So the
    /// clients publish what they were told took, and the verifier holds the
    /// server to exactly that.
    pub walk: Arc<Mutex<BTreeSet<Vec<u8>>>>,
    /// Every form label the run's clients actually put on the wire.
    ///
    /// The observed half of the contract. A declaration alone can claim a
    /// form the generator never reaches — a branch with probability zero, or
    /// one a bug made unreachable — and the only thing that can tell the two
    /// apart is a record of what was really sent.
    pub forms: Arc<Mutex<BTreeSet<&'static str>>>,
    /// Each shard's durable point as its log last reported it: the highest
    /// sequence a successful sync covered, and when.
    ///
    /// Written by the simulated node's log — white-box on purpose. The
    /// durable point is a fact about the server, cheap to read where it is
    /// made and impossible to derive from outside: the housekeeping tick
    /// has no time quota under load, so no band of wall clock says when a
    /// sync happened.
    pub durable: Arc<Mutex<Vec<DurablePoint>>>,
    /// Every crash the driver inflicted, with the durable points as they
    /// stood at that instant.
    pub crashes: Arc<Mutex<Vec<CrashRecord>>>,
    /// Per shard, the index of the latest crash whose recovery reported it
    /// as having lost records, or `None` if none has.
    pub truncated: Arc<Mutex<Vec<Option<usize>>>>,
    /// Every acknowledged increment — what the verifier needs to say which
    /// of them a crash could not have taken.
    pub increments: Arc<Mutex<Vec<Increment>>>,
    /// The node's disk: the range its deferred syncs' latencies are drawn
    /// from, and the stream they are drawn from — the run's own, apart from
    /// the crash schedule's, so that drawing a latency moves no crash.
    ///
    /// Here and not built where the node opens its files because a
    /// restarted node continues the same stream: two processes of one run
    /// drawing from two copies of it would repeat each other's latencies.
    pub disk: SimDisk,
    /// Whether the node's disk fails or corrupts — the only disk on which a
    /// recovery's report of a possible loss excuses one.
    pub disk_lies: bool,
}

/// An acknowledged increment, and what a crash would need to have found
/// synced for it to survive.
#[derive(Debug, Clone, Copy)]
pub struct Increment {
    /// The shard its counter key hashes to.
    pub shard: u16,
    /// What it added.
    pub delta: i64,
    /// When its reply arrived, on the world clock — or `None` for one whose
    /// reply a crash took, which may or may not have been applied.
    pub acked: Option<Duration>,
    /// The index, in [`Shared::crashes`], of the first crash that came
    /// after the node that applied it started — every crash from there on
    /// could have taken it.
    ///
    /// An index and not an instant because a reply can arrive after the
    /// crash of the node that sent it.
    pub later: usize,
}

impl Shared {
    /// Shared state for a node of `shards` shards, run under `sim_seed` on
    /// a disk that does what `disk` says.
    #[must_use]
    pub fn new(shards: u16, sim_seed: u64, disk: &DiskFaults) -> Self {
        let rng = ChaCha8Rng::seed_from_u64(sim_seed ^ GOLDEN.rotate_left(29));
        Self {
            tally: Arc::default(),
            walk: Arc::default(),
            forms: Arc::default(),
            durable: Arc::new(Mutex::new(vec![None; usize::from(shards)])),
            crashes: Arc::default(),
            truncated: Arc::new(Mutex::new(vec![None; usize::from(shards)])),
            increments: Arc::default(),
            disk: SimDisk::new(disk.sync_latency_ms, Some(Arc::new(Mutex::new(rng)))),
            disk_lies: disk.lies(),
        }
    }
}

/// Everything the hosts count between them.
#[derive(Debug, Clone, Copy, Default)]
pub struct Tally {
    /// The sum of every acknowledged `INCRBY` delta.
    pub expected: i64,
    /// The sum of every counter read back at the end.
    pub actual: i64,
    /// How many client hosts have finished their workload.
    pub done: u32,
    /// Violations, and the checks that could have found them. See
    /// [`SimOutcome`], whose fields these become.
    pub stale_reads: u64,
    pub spurious_deaths: u64,
    pub plain_mismatches: u64,
    pub dead_checks: u64,
    pub alive_checks: u64,
    pub plain_checks: u64,
    pub walk_mismatches: u64,
    pub walk_checks: u64,
    pub evictions_observed: u64,
    pub evicted_keys: u64,
    pub executor_usec: u64,
    pub executor_calls: u64,
    pub ceiling_breaches: u64,
    pub ceiling_checks: u64,
    /// Restarts observed.
    pub recoveries: u64,
    /// Shards whose resumed position was at or below the durable point at
    /// the last crash.
    pub lost_durable_prefixes: u64,
    /// Of those, the ones recovery did not report as lossy.
    pub unreported_losses: u64,
    /// Flush failures reported by the node.
    pub write_faults: u64,
    /// Sync failures reported by the node.
    pub sync_faults: u64,
    /// Server host starts that failed and were retried.
    pub start_failures: u64,
    /// Client hosts that finished their bursts and are waiting, at rest,
    /// for the driver.
    pub paused: u32,
    /// Whether the driver has crashed the node at rest yet.
    pub rest_crashed: bool,
    /// The least the counters may sum to after the run's crashes: every
    /// increment no crash could have taken, plus every *negative* one a
    /// crash may have left standing.
    pub counter_floor: i64,
    /// The most they may sum to: the same, with the *positive* ones. The
    /// deltas are of either sign, so a lost increment can move the sum
    /// either way, and the range is what every survivable subset lies in.
    pub counter_ceiling: i64,
    /// Reads of a plain key a crash left exactly known, as durable, that
    /// disagreed — on a shard the recovery did not report as truncated.
    pub lost_durable_writes: u64,
    /// Those same disagreements on a shard the recovery *did* report: a
    /// loss the node owned up to. Also counts a volatile key written before
    /// a crash that read dead inside its live band on such a shard.
    pub excused_losses: u64,
    /// Reads decided against a value a crash left known to be durable —
    /// the denominator of the two above.
    pub durable_checks: u64,
    /// Reads of a key a crash left open between several candidates that
    /// returned none of them: a value nobody wrote.
    pub phantom_writes: u64,
    /// Reads decided against several candidates — the denominator of
    /// [`Tally::phantom_writes`].
    pub either_checks: u64,
    /// The checkpoint's reports and faults, and what each start refused and
    /// removed. See [`SimOutcome`], whose fields these become.
    pub snapshot_cycles: u64,
    pub compactions: u64,
    pub files_removed: u64,
    pub max_snapshot_bytes: u64,
    pub max_written_during: u64,
    pub disk_peak_bytes: u64,
    pub snapshot_faults: u64,
    pub remove_faults: u64,
    pub snapshots_refused_at_start: u64,
    pub files_removed_at_start: u64,
}

/// Takes a lock that cannot be contended, and says so if it was poisoned.
///
/// # Panics
///
/// If another host panicked while holding the lock. That has already failed
/// the run; this only reports where.
pub fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().expect("a simulated host panicked mid-update")
}
