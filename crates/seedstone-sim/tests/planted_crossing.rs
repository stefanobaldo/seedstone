//! A `SCAN` call crosses shards, and the shard it can now skip.
//!
//! One call continues into the next shard while its bucket budget lasts, which
//! is what turned a cycle from one round trip per shard into a handful. It also
//! made a defect possible that the previous code could not have had: a call that
//! steps *over* a shard instead of into it, answers the keys of the shards it
//! did visit, and reaches the end of the cycle having never looked at the rest.
//!
//! Catching that needs a walk that finished. At-least-once is the only claim a
//! skipped shard breaks — every key returned is still a real key, every cursor
//! still moves, and the closing `KEYS` is a broadcast that never crosses
//! anything — and at-least-once is a claim only a completed cycle makes. None of
//! the three shapes the gate sweeps completes one, so this shape exists: sixteen
//! shards, enough that a call crosses several, and a client walking its cycle to
//! the end. See `SimConfig::crossing`.

use seedstone_sim::{Plant, SimConfig, SimOutcome, run_sim};

/// How many seeds each half is given.
///
/// The defect is not a race — a call that skips a shard skips it on every
/// schedule — so one seed would prove it. The sweep is here for the honest half,
/// where the claim is that a crossing walk comes back with everything, and more
/// seeds is more evidence for it.
const SEEDS: u64 = 6;

fn sweep(plant: Option<Plant>) -> Vec<SimOutcome> {
    (1..=SEEDS)
        .map(|sim_seed| {
            let mut cfg = SimConfig::crossing(1, sim_seed);
            cfg.planted = plant;
            run_sim(&cfg)
        })
        .collect()
}

/// The counterpart, and the reason the test below proves anything: the same
/// seeds, the same walks, an honest crossing, and a clean bill.
///
/// A plant that also broke the unplanted run would be a plant in the wrong
/// place, and this is what says it is not. `walk_checks` is named rather than
/// left to `invariants_were_exercised`: what this shape is for is the walk, and
/// a run that decided none of them would pass this test while measuring
/// nothing.
#[test]
fn the_same_seeds_walk_clean_on_the_crossing_shape() {
    for (seed, outcome) in sweep(None).into_iter().enumerate() {
        let seed = seed + 1;
        assert!(
            outcome.invariant_holds(),
            "seed {seed} violated an invariant with an honest crossing: {outcome:?}"
        );
        assert!(
            outcome.walk_checks > 0,
            "seed {seed} decided no walk, so its clean bill is worth nothing: {outcome:?}"
        );
    }
}
