//! The walk's step bound, shown catching the cursor it exists for.
//!
//! `Dict::scan` advances its cursor in reverse binary order, and the whole
//! argument for that order is a liveness one: a cursor counting buckets
//! upwards moves one bucket a step while a doubling moves the finish line by
//! the width of the table, so a keyspace growing faster than the cursor
//! advances outruns it and the cycle never comes back to zero. Nothing in this
//! repository had ever watched that happen.
//!
//! Two things have to be true at once before it can, and neither is true of
//! the shape the gate sweeps. The cursor has to be *between* steps — a step
//! whose bucket budget covers a whole table hands back zero, and a thousand
//! shards holding four keys each are all of them that size. And the table it
//! is inside has to be growing while it is inside it. So this has a shape of
//! its own: one shard, deep, and a client walking it bucket by bucket while
//! writing into it. See `SimConfig::narrow`.
//!
//! What the plant is **not** is a walk that loses a key. Under a table that
//! only ever grows, an upward cursor still visits every bucket ahead of it —
//! entries move from bucket `b` to `b` or `b + n`, never backwards — so what
//! it fails at is arriving, not covering. That is worth being exact about,
//! because "it loses keys the moment a table doubles" is the intuitive account
//! and it is wrong: the guarantee an upward cursor breaks is termination, and
//! the assertion that catches it is the step bound.

use seedstone_sim::{Plant, SimConfig, SimOutcome, run_sim};

/// How many seeds each half is given.
///
/// The defect is not a race — an upward cursor is outrun by a growing table on
/// every schedule — so one seed would prove it. The sweep is here for the
/// honest half, where the claim is that a converging cursor finishes with room
/// to spare, and more seeds is more evidence for it.
const SEEDS: u64 = 6;

fn sweep(plant: Option<Plant>) -> Vec<SimOutcome> {
    (1..=SEEDS)
        .map(|sim_seed| {
            let mut cfg = SimConfig::narrow(1, sim_seed);
            cfg.planted = plant;
            run_sim(&cfg)
        })
        .collect()
}

/// The counterpart, and the reason the test below proves anything: the same
/// seeds, the same walks, an honest cursor, and a clean bill.
///
/// A plant that also broke the unplanted run would be a plant in the wrong
/// place, and this is what says it is not. `walk_checks` is named rather than
/// left to `invariants_were_exercised`: this shape is deliberately too small
/// to reach the expiration invariants, so the only denominator that means
/// anything here is the walks'.
#[test]
fn the_same_seeds_walk_clean_with_an_honest_cursor() {
    for (seed, outcome) in sweep(None).into_iter().enumerate() {
        let seed = seed + 1;
        assert!(
            outcome.invariant_holds(),
            "seed {seed} violated an invariant with an honest cursor: {outcome:?}"
        );
        assert!(
            outcome.walk_checks > 0,
            "seed {seed} decided no walk, so its clean bill is worth nothing: {outcome:?}"
        );
    }
}

/// A cursor that counts upwards is outrun by the table it is walking.
#[test]
fn the_harness_catches_a_cursor_that_does_not_survive_a_rehash() {
    for (seed, outcome) in sweep(Some(Plant::ScanMissesRehash)).into_iter().enumerate() {
        let seed = seed + 1;
        assert!(
            outcome.walk_mismatches > 0,
            "seed {seed}: a cursor that cannot finish its cycle while the table \
             grows under it was not caught: {outcome:?}"
        );
        // The plant is the walk's and nothing else's. A defect that also moved
        // the counter sum or killed a key would be caught by whichever
        // invariant fired first, and this test would pass without the walk
        // having seen anything.
        assert_eq!(
            (
                outcome.expected_sum == outcome.actual_sum,
                outcome.stale_reads,
                outcome.spurious_deaths,
                outcome.plain_mismatches
            ),
            (true, 0, 0, 0),
            "seed {seed}: a cursor that cannot finish is not an expiry failure, \
             a lost update or a wrong value: {outcome:?}"
        );
    }
}
