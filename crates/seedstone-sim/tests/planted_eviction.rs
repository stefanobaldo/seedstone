//! Bounded memory, shown failing and shown holding.
//!
//! Two defects, each the server's own eviction decision handed to the pool
//! at spawn: a node that never reclaims, and one that reclaims whatever it
//! holds. The first is the cache that takes the host down; the second is the
//! cache whose hit ratio is inexplicably bad. Each is caught by the counter
//! that owns it and by no other, and the same seeds run clean without them.

use seedstone_sim::{Plant, SimConfig, SimOutcome, run_sim};

/// How many seeds each plant is given to surface on.
///
/// The same count `planted_expiry.rs` uses, and for the same reason: these
/// defects are deterministic given the workload, so the sweep is here for the
/// unplanted half — the seeds that have to come back clean — where more seeds
/// is more evidence.
const SEEDS: u64 = 4;

/// Runs `seeds` eviction-shape seeds, planted or honest.
fn sweep(plant: Option<Plant>) -> Vec<SimOutcome> {
    (1..=SEEDS)
        .map(|sim_seed| {
            let mut cfg = SimConfig::eviction(1, sim_seed);
            cfg.planted = plant;
            run_sim(&cfg)
        })
        .collect()
}

/// The honest run, and the shape's calibration: every seed must cross the
/// ceiling — or the shape is measuring an unbounded node — and must still
/// decide plain reads, or the tolerance the model grants under eviction has
/// swallowed the invariant.
#[test]
fn the_eviction_shape_evicts_on_every_seed_and_still_decides() {
    for (seed, outcome) in sweep(None).into_iter().enumerate() {
        let seed = seed + 1;
        assert!(
            outcome.invariant_holds(),
            "seed {seed} violated an invariant with an honest node: {outcome:?}"
        );
        assert!(
            outcome.invariants_were_exercised(),
            "seed {seed} decided nothing: {outcome:?}"
        );
        assert!(
            outcome.evicted_keys > 0,
            "seed {seed}: the ceiling was never reached; lower SimConfig::eviction's \
             maxmemory: {outcome:?}"
        );
        assert!(
            outcome.evictions_observed > 0,
            "seed {seed}: no client ever read an evicted key, so the tolerance was \
             never exercised: {outcome:?}"
        );
        assert!(
            outcome.plain_checks > 10 * outcome.evictions_observed,
            "seed {seed}: eviction dominates the plain reads and the model decides \
             little: {outcome:?}"
        );
    }
}

/// A node that ignores its ceiling is over it by the time anyone looks.
#[test]
fn a_node_that_never_reclaims_is_caught_by_the_ceiling_check() {
    for (seed, outcome) in sweep(Some(Plant::IgnoresCeiling)).into_iter().enumerate() {
        let seed = seed + 1;
        assert!(
            outcome.ceiling_breaches > 0,
            "seed {seed}: a node over its ceiling was not caught: {outcome:?}"
        );
        assert_eq!(
            outcome.evicted_keys, 0,
            "seed {seed}: this plant evicts nothing: {outcome:?}"
        );
        assert_eq!(
            (
                outcome.stale_reads,
                outcome.spurious_deaths,
                outcome.expected_sum == outcome.actual_sum
            ),
            (0, 0, true),
            "seed {seed}: not an expiry failure nor a lost update: {outcome:?}"
        );
    }
}

/// A node that reclaims below its ceiling is caught where the plain model is
/// exact — on a shape with no ceiling, where `nil` for a written key is a
/// mismatch and nothing excuses it.
#[test]
fn a_node_that_evicts_below_the_ceiling_is_caught_by_the_exact_model() {
    for sim_seed in 1..=SEEDS {
        let mut cfg = SimConfig::mini(1, sim_seed);
        cfg.planted = Some(Plant::EvictsBelowCeiling);
        let outcome = run_sim(&cfg);
        assert!(
            outcome.plain_mismatches > 0,
            "seed {sim_seed}: keys vanishing under no ceiling were not caught: {outcome:?}"
        );
        assert_eq!(
            outcome.stale_reads, 0,
            "seed {sim_seed}: a key evicted early is not a key served late: {outcome:?}"
        );
    }
}

/// The other plants leave the eviction counters alone: a race and a missed
/// expiry are not ceiling failures.
#[test]
fn the_eviction_counters_stay_silent_on_unrelated_plants() {
    for plant in [Plant::LostUpdate, Plant::ServeExpired] {
        for (seed, outcome) in sweep(Some(plant)).into_iter().enumerate() {
            assert_eq!(
                outcome.ceiling_breaches,
                0,
                "seed {}: {plant:?} is not a ceiling failure: {outcome:?}",
                seed + 1
            );
        }
    }
}
