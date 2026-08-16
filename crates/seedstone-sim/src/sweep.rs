//! The parallel sweep runner.
//!
//! The `thread::spawn` prohibition (`clippy.toml`) exists so nothing *inside*
//! a simulated run schedules on the OS — the simulator owns scheduling there.
//! This module sits outside every simulation: it starts independent runs,
//! each deterministic from its own seeds, and never reaches into one. That is
//! the reasoning the site-local allow below carries, and
//! `docs/coding-guide.md` lists this site in the allow census.

use crate::{SimConfig, SimOutcome, run_sim};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;

/// Runs `seeds` simulations starting at `first_seed` on `workers` OS
/// threads, delivering every outcome to `on_result` in ascending seed order.
///
/// Ordered delivery is what keeps the sweep's output an artifact: identical
/// at any worker count. Flushing the contiguous prefix as it completes keeps
/// the property the serial sweep had — a cancelled sweep has still reported
/// everything up to the first unfinished seed.
///
/// Returns the number of seeds whose invariants did not hold.
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
    mut on_result: F,
) -> u64
where
    C: Fn(u64) -> SimConfig + Send + Sync + 'static,
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
    let (tx, rx) = mpsc::channel();

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let next = Arc::clone(&next);
        let config_for = Arc::clone(&config_for);
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
                let outcome = run_sim(&config_for(seed));
                if tx.send((seed, outcome)).is_err() {
                    break;
                }
            }
        }));
    }
    drop(tx);

    let mut violations = 0u64;
    let mut pending = BTreeMap::new();
    let mut expected = first_seed;
    for (seed, outcome) in rx {
        pending.insert(seed, outcome);
        while let Some(outcome) = pending.remove(&expected) {
            if !outcome.invariant_holds() {
                violations += 1;
            }
            on_result(expected, &outcome);
            expected = expected.saturating_add(1);
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
    violations
}
