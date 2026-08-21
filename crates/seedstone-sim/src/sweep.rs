//! The parallel sweep runner.
//!
//! The `thread::spawn` prohibition (`clippy.toml`) exists so nothing *inside*
//! a simulated run schedules on the OS — the simulator owns scheduling there.
//! This module sits outside every simulation: it starts independent runs,
//! each deterministic from its own seeds, and never reaches into one. That is
//! the reasoning the site-local allow below carries, and
//! `docs/coding-guide.md` lists this site in the allow census.

use crate::{SimConfig, SimOutcome, run_sim};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

/// Runs `seeds` simulations starting at `first_seed` on `workers` OS
/// threads, delivering every outcome to `on_result` in ascending seed order.
///
/// Ordered delivery is what keeps the sweep's output an artifact: identical
/// at any worker count. Flushing the contiguous prefix as it completes keeps
/// the property the serial sweep had — a cancelled sweep has still reported
/// everything up to the first unfinished seed.
///
/// Workers are held to a fixed window ahead of that prefix, so one slow seed
/// costs the sweep throughput rather than memory. A sweep whose seeds finish
/// in comparable time — every sweep of a single shape — never reaches the
/// window at all, and pays only the one mutex a worker takes before each
/// seed, which is nothing measurable against the run it precedes.
///
/// Returns what the sweep found: how many seeds violated an invariant, and
/// the union of every command form its clients emitted.
///
/// The union is a sweep-level number by construction, which is the only level
/// it means anything at. A form the workload reaches on one roll in fifty is
/// absent from most seeds and present in any sweep worth the name, so a
/// per-seed comparison against the contract would fail on luck rather than on
/// coverage.
///
/// # Panics
///
/// If the range is empty, starts at seed 0 or overflows the seed space, if
/// `workers` is zero, or if a worker panicked — a run that never reported is
/// a seed nobody swept, and a sweep that hid one would claim coverage it does
/// not have.
pub fn sweep<C, F>(
    first_seed: u64,
    seeds: u64,
    workers: usize,
    config_for: C,
    on_result: F,
) -> SweepReport
where
    C: Fn(u64) -> SimConfig + Send + Sync + 'static,
    F: FnMut(u64, &SimOutcome),
{
    sweep_with(first_seed, seeds, workers, config_for, run_sim, on_result)
}

/// How many seeds a sweep may have begun and not yet reported — and so, since
/// a result reaches the collector only after its run began, how many
/// finished-but-unreported seeds the collector holds.
///
/// In-order reporting means a finished seed waits for every earlier seed, so
/// the pending set tracks the spread between the slowest seed and the rest.
/// Unbounded, that is fine at today's shard width and stops being fine the
/// moment someone widens the window — which is exactly the assumption nobody
/// re-checks. A bound makes a slow head seed apply backpressure to the
/// producers instead of to memory.
///
/// Which is why the bound is applied where runs *begin* rather than where
/// results queue. A bounded results channel would bound the channel and
/// nothing else: the collector drains it as fast as the workers fill it,
/// straight into the pending map, so no producer ever waits — and a collector
/// that stopped draining to make one wait would stall behind its own head
/// seed, whose result is in the queue it stopped reading.
///
/// Since the window is a ceiling on seeds in flight, a sweep asked for more
/// workers than this runs at this much concurrency and no more. That is
/// correct rather than merely tolerable, and no caller comes near it.
const PENDING_LIMIT: u64 = 4 * 1024;

/// What holds a worker back from beginning a seed too far ahead of the one
/// the collector is waiting for.
///
/// Seeds are handed out in ascending order, so whenever a worker is waiting
/// here, the seed at the head was drawn before that worker's — and the head's
/// own worker is never one of the waiters: `seed - head` is zero for it, and
/// the gate lets it through however long the queue behind it grows. That is
/// what makes the bound deadlock-free rather than merely small.
struct StartGate {
    state: Mutex<GateState>,
    admitted: Condvar,
}

/// What the workers wait on.
struct GateState {
    /// The lowest seed the collector has not yet reported.
    head: u64,
    /// Whether the sweep has given up on the seeds nobody has begun yet.
    ///
    /// A worker that panics owes the collector a seed it will never send. If
    /// that seed is the head, the head never moves, and without this every
    /// other worker would wait on it forever: the panic `sweep` documents and
    /// re-raises would surface as a hang instead.
    abandoned: bool,
}

impl StartGate {
    const fn new(first_seed: u64) -> Self {
        Self {
            state: Mutex::new(GateState {
                head: first_seed,
                abandoned: false,
            }),
            admitted: Condvar::new(),
        }
    }

    /// The gate's state, poisoned or not.
    ///
    /// It is two plain values that no unwind can leave half-written, and the
    /// one thread that would find the lock poisoned is a worker releasing the
    /// waiters from inside a drop guard — where refusing to read them would
    /// turn the panic this sweep reports into an abort.
    fn locked(&self) -> MutexGuard<'_, GateState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Blocks until `seed` is within [`PENDING_LIMIT`] of the collector's
    /// head, and reports whether the sweep still wants it run.
    fn admit(&self, seed: u64) -> bool {
        let mut state = self.locked();
        // The head never passes a seed nobody has reported, and `seed` is one
        // nobody has even begun, so `head <= seed` and the difference is what
        // to take. Comparing against `head + PENDING_LIMIT` would instead
        // saturate a sweep of the last seed in the space out of its own first
        // run.
        while !state.abandoned && seed - state.head >= PENDING_LIMIT {
            state = self
                .admitted
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        !state.abandoned
    }

    /// Records that every seed below `head` has been reported.
    fn head_reached(&self, head: u64) {
        self.locked().head = head;
        self.admitted.notify_all();
    }

    /// Releases every waiter, for good.
    fn abandon(&self) {
        self.locked().abandoned = true;
        self.admitted.notify_all();
    }
}

/// A worker's side of the gate, which releases the others if it unwinds.
struct WorkerGate(Arc<StartGate>);

impl WorkerGate {
    fn admit(&self, seed: u64) -> bool {
        self.0.admit(seed)
    }
}

impl Drop for WorkerGate {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.abandon();
        }
    }
}

/// The collector's side of the gate.
///
/// Dropping it — whether the sweep returns or unwinds through a caller's
/// `on_result` — is what keeps a worker from waiting on a head that nobody
/// is left to move.
struct CollectorGate(Arc<StartGate>);

impl CollectorGate {
    fn head_reached(&self, head: u64) {
        self.0.head_reached(head);
    }
}

impl Drop for CollectorGate {
    fn drop(&mut self) {
        self.0.abandon();
    }
}

/// [`sweep`], with the per-seed run supplied rather than fixed.
///
/// The seam exists for one thing the public entry point cannot offer: a test
/// of how far the sweep lets its workers run ahead needs thousands of seeds
/// in flight, and thousands of real simulations is not a test anyone runs.
fn sweep_with<C, R, F>(
    first_seed: u64,
    seeds: u64,
    workers: usize,
    config_for: C,
    run: R,
    mut on_result: F,
) -> SweepReport
where
    C: Fn(u64) -> SimConfig + Send + Sync + 'static,
    R: Fn(&SimConfig) -> SimOutcome + Send + Sync + 'static,
    F: FnMut(u64, &SimOutcome),
{
    assert!(
        first_seed >= 1,
        "seed 0 is not part of any sweep's vocabulary"
    );
    assert!(seeds >= 1, "a sweep of no seeds sweeps nothing");
    assert!(workers >= 1, "a sweep with no workers never starts a run");
    let last_seed = first_seed
        .checked_add(seeds - 1)
        .expect("the range overflows the seed space");

    // More workers than seeds is threads with nothing to draw; at large
    // counts the surplus is what aborts the process, on a spawn failure
    // rather than on anything the sweep was asked to do.
    let workers = usize::try_from(seeds).map_or(workers, |seeds| workers.min(seeds));

    let next = Arc::new(AtomicU64::new(first_seed));
    let config_for = Arc::new(config_for);
    let run = Arc::new(run);
    let gate = Arc::new(StartGate::new(first_seed));
    let (tx, rx) = mpsc::channel();

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let next = Arc::clone(&next);
        let config_for = Arc::clone(&config_for);
        let run = Arc::clone(&run);
        let gate = WorkerGate(Arc::clone(&gate));
        let tx = tx.clone();
        // Outside every simulation: starts runs, never reaches inside one.
        #[allow(
            clippy::disallowed_methods,
            reason = "the prohibition keeps OS scheduling out of a simulated \
                      run; this thread starts whole runs, each deterministic \
                      from its own seeds, and reaches into none of them"
        )]
        handles.push(std::thread::spawn(move || {
            loop {
                let seed = next.fetch_add(1, Ordering::Relaxed);
                // Below the range as well as above it: the counter wraps to
                // zero once a sweep hands out `u64::MAX`, and a worker that
                // only checked the upper bound would take the wrapped value
                // as a seed it owed the caller and never stop.
                if seed < first_seed || seed > last_seed {
                    break;
                }
                // An abandoned gate means nobody is left to report this seed:
                // a worker panicked, or the collector went away. Either way
                // the sweep is already unwinding towards what it reports.
                if !gate.admit(seed) {
                    break;
                }
                let outcome = run(&config_for(seed));
                if tx.send((seed, outcome)).is_err() {
                    break;
                }
            }
        }));
    }
    drop(tx);

    let gate = CollectorGate(gate);
    let mut report = SweepReport::default();
    let mut pending = BTreeMap::new();
    let mut expected = first_seed;
    for (seed, outcome) in rx {
        pending.insert(seed, outcome);
        let head_before = expected;
        while let Some(outcome) = pending.remove(&expected) {
            if !outcome.invariant_holds() {
                report.violations += 1;
            }
            report.forms.extend(outcome.forms_emitted.iter().copied());
            on_result(expected, &outcome);
            expected = expected.saturating_add(1);
        }
        if expected != head_before {
            gate.head_reached(expected);
        }
    }
    for handle in handles {
        handle
            .join()
            .expect("a sweep worker panicked; its seed never reported");
    }
    assert!(
        pending.is_empty(),
        "a seed was skipped without a worker panic"
    );
    report
}

/// What a whole sweep found, as against what one seed found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// How many seeds violated an invariant.
    pub violations: u64,
    /// Every command form any of the sweep's clients emitted, named as
    /// [`crate::contract`] names it.
    pub forms: BTreeSet<&'static str>,
}

#[cfg(test)]
mod tests {
    use super::{PENDING_LIMIT, sweep_with};
    use crate::{SimConfig, SimOutcome};
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    const HOLD_STEPS: u32 = 200;
    const HOLD_STEP: Duration = Duration::from_millis(1);

    /// Holds the calling seed until `at_least` seeds are in flight behind it,
    /// and no longer than [`HOLD_STEPS`] × [`HOLD_STEP`] whatever happens.
    ///
    /// The count is the condition and the stretch is only a ceiling, so a
    /// sweep that reaches the count is held for a step or two — this loop
    /// cannot notice any sooner than [`HOLD_STEP`] — and one that never can
    /// is held for a fifth of a second rather than forever. Nothing here is
    /// asserted on wall clock: the stretch decides how long a test waits,
    /// never whether it passes.
    fn hold_until(in_flight: &AtomicU64, at_least: u64) {
        for _ in 0..HOLD_STEPS {
            if in_flight.load(Ordering::Acquire) >= at_least {
                break;
            }
            std::thread::sleep(HOLD_STEP);
        }
    }

    /// A run that observed nothing.
    ///
    /// The question here is how many runs the sweep keeps in flight, not what
    /// any of them found, and answering it takes several thousand seeds. That
    /// many real simulations would answer it no better and never be run.
    fn nothing_observed() -> SimOutcome {
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
            forms_emitted: BTreeSet::new(),
        }
    }

    #[test]
    fn a_slow_head_seed_does_not_let_the_pending_map_grow_without_bound() {
        // One seed deliberately slow, many fast ones behind it. In-order
        // reporting holds the fast results until the head finishes; what must
        // be bounded is how many it holds.
        //
        // Measured as seeds begun but not yet reported, which is the set the
        // pending map is a subset of — a result reaches the map only after
        // its run began — so a bound on the wider set is a bound on the map.
        let in_flight = Arc::new(AtomicU64::new(0));
        let peak = Arc::new(AtomicU64::new(0));
        let seeds = PENDING_LIMIT + 512;
        let mut reported = 0_u64;

        sweep_with(
            1,
            seeds,
            8,
            |sim_seed| SimConfig::mini(1, sim_seed),
            {
                let in_flight = Arc::clone(&in_flight);
                let peak = Arc::clone(&peak);
                move |cfg: &SimConfig| {
                    let begun = in_flight.fetch_add(1, Ordering::AcqRel) + 1;
                    peak.fetch_max(begun, Ordering::AcqRel);
                    if cfg.sim_seed == 1 {
                        // Held until the queue behind it passes the bound —
                        // which is the failure, so the unbounded sweep is the
                        // one that finishes fast.
                        //
                        // A bounded sweep can never reach a count above the
                        // bound, which is the point of asking for one: this
                        // hold runs out its whole ceiling on every green run,
                        // and that cost is deliberate. Trimming the threshold
                        // to PENDING_LIMIT to buy the fifth of a second back
                        // would make the count reachable under the bound, so
                        // the head would stop holding while the queue behind
                        // it was still filling — and an unbounded sweep whose
                        // peak stopped at exactly PENDING_LIMIT would pass an
                        // assertion it is supposed to fail.
                        hold_until(&in_flight, PENDING_LIMIT + 1);
                    }
                    nothing_observed()
                }
            },
            |_, _| {
                in_flight.fetch_sub(1, Ordering::AcqRel);
                reported += 1;
            },
        );

        let observed_peak = peak.load(Ordering::Acquire);
        assert!(
            observed_peak <= PENDING_LIMIT,
            "the collector held {observed_peak} results, above the \
             {PENDING_LIMIT} it bounds itself to"
        );
        // A sweep that bounded itself by dropping seeds would satisfy the
        // assertion above and sweep nothing.
        assert_eq!(reported, seeds, "every seed in the range is reported");
    }

    /// Holding a worker back turns a panic into a queue of workers waiting on
    /// the seed that panicked, and if that seed is the head the queue is
    /// everyone. `sweep` documents the panic and re-raises it; re-raising it
    /// is only possible if the workers behind it are let go first, and a
    /// version that hung here instead would take a whole gate with it and
    /// report nothing at all.
    ///
    /// A regression surfaces here as a hang rather than as a failure: the
    /// workers stay queued on a head that never moves, and the test never
    /// returns. A job timing out at this test *is* the report that this
    /// guard broke. Turning that into a message would take running
    /// `sweep_with` on a second spawned thread and a `recv_timeout` in the
    /// body — a second `clippy::disallowed_methods` site in a file whose
    /// allow census is deliberately one entry and is listed as such in
    /// `docs/coding-guide.md`. A standing exception in the determinism
    /// gate's own census is the more expensive half of that trade.
    #[test]
    #[should_panic(expected = "a sweep worker panicked; its seed never reported")]
    fn a_panicking_head_seed_does_not_strand_the_workers_queued_behind_it() {
        let in_flight = Arc::new(AtomicU64::new(0));

        sweep_with(
            1,
            PENDING_LIMIT + 512,
            8,
            |sim_seed| SimConfig::mini(1, sim_seed),
            {
                let in_flight = Arc::clone(&in_flight);
                move |cfg: &SimConfig| {
                    in_flight.fetch_add(1, Ordering::AcqRel);
                    if cfg.sim_seed == 1 {
                        // Not before the workers behind it are queued at the
                        // gate: that queue is the arrangement that hangs.
                        hold_until(&in_flight, PENDING_LIMIT);
                        panic!("the head seed's worker fails");
                    }
                    nothing_observed()
                }
            },
            |_, _| {
                in_flight.fetch_sub(1, Ordering::AcqRel);
            },
        );
    }
}
