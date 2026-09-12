//! What one simulation reports: the trace hash, the verifier's counts, and
//! the shared tallies the clients write into while it runs.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

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
    /// keeps `usec` out of [`crate::trace::fold_reply`] and a replay byte-stable, and it is
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
        (self.evictable || self.expected_sum == self.actual_sum)
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
    }
}

/// What the client hosts and the verifier write into, and the run reads out.
///
/// Every host in a turmoil simulation runs on the same OS thread, so this
/// mutex is never actually contended; it is here because [`TraceSink`] and
/// the futures turmoil holds must be `Send`.
#[derive(Clone, Default)]
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
