//! A cycle-completing walk on the shape narrow enough to afford one.
//!
//! Six seeds of `SimConfig::narrow`, walked to the end of their cycle and held
//! to every claim `Model::walk` makes. It is the only shape in this harness
//! where a client drives a whole cycle over a *deep* table — one shard, two
//! clients, and a family growing between the steps of the walk that is reading
//! it — so it is where a cursor's convergence under growth is observed end to
//! end, and where `WALK_CYCLE_STEP_BOUND` is measured.
//!
//! **It used to carry a plant, and no longer can.** `Plant::ScanMissesRehash`
//! serves the workload through a cursor that counts buckets upwards instead of
//! advancing in reverse binary order, and what that breaks is termination: a
//! cursor moving one bucket a step is outrun by a table whose doubling moves
//! the finish line by its whole width. Observing it needs the cursor caught
//! *between* steps, and what used to put it there was a client `COUNT` of one
//! meaning one bucket a call. A `SCAN` call now spends a bucket ceiling of the
//! server's own, which covers this shape's whole table several times over
//! before it answers, so there is no walk left in flight for a rehash to happen
//! underneath. Bounding each envelope by the key target instead was tried and
//! does not bring it back — the *call* still loops until its target is met.
//!
//! The shape that would observe it is a production-sized one: a table of
//! millions of buckets, where a call's ceiling is a rounding error against the
//! width of the walk. That is not a shape a simulator can afford, so the claim
//! moved down to where one dict and no network can make it —
//! `an_upward_cursor_is_outrun_by_a_table_growing_under_it`, in
//! `crates/seedstone-core/src/dict.rs`, beside the honest counterpart it is the
//! necessity argument for. `Plant::ScanMissesRehash` stays selectable and stays
//! classified: a sweep serving it prints, in as many words, that nothing this
//! harness sweeps or walks can catch it.

use seedstone_sim::{SimConfig, SimOutcome, run_sim};

/// How many seeds the shape is walked over.
///
/// The claim is that a converging cursor finishes its cycle with room to spare
/// while the table grows under it, and that is a claim about schedules: more
/// seeds is more evidence for it. Six is what the bound in
/// `WALK_CYCLE_STEP_BOUND` is measured over, so the two stay the same number on
/// purpose.
const SEEDS: u64 = 6;

fn sweep() -> Vec<SimOutcome> {
    (1..=SEEDS)
        .map(|sim_seed| run_sim(&SimConfig::narrow(1, sim_seed)))
        .collect()
}

/// Every claim `Model::walk` makes, over a cycle that finished.
///
/// `walk_checks` is named rather than left to `invariants_were_exercised`: this
/// shape is deliberately too small to reach the expiration invariants, so the
/// only denominator that means anything here is the walks'. A run that decided
/// no walk would pass the line above while proving nothing.
#[test]
fn the_same_seeds_walk_clean_with_an_honest_cursor() {
    for (seed, outcome) in sweep().into_iter().enumerate() {
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
