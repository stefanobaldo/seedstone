//! The shape CI actually sweeps, shown catching.
//!
//! `planted_race.rs` proves the harness can fail, and it does so on the mini
//! configuration, because that is what a unit-test-sized run affords. The
//! gate on every pull request sweeps `standard`, which is a different shape
//! in every dimension that matters — more clients, more keys, ten executors
//! instead of four — and a shape that has never been watched catching
//! anything is a shape nobody has evidence about.
//!
//! What this pins is the *detection power of the swept configuration*. If it
//! ever goes red, the finding is not about the plant: it is that the sweep
//! guarding this repository stopped being able to see a lost update, and the
//! answer is to find out why, never to widen the seed count until it passes.

use seedstone_sim::{Plant, SimConfig, run_sim};

/// How many seeds the plant is given.
///
/// Four, and margin rather than a measurement: the shape catches this race on
/// nearly every seed, so the count is there to absorb the one that does not
/// while still costing only a few runs. A count that had to be raised to keep
/// this green would be the finding, not the fix.
const SEEDS: u64 = 4;

#[test]
fn the_swept_shape_catches_a_planted_lost_update() {
    let caught = (1..=SEEDS).find(|sim_seed| {
        let mut cfg = SimConfig::standard(1, *sim_seed);
        cfg.planted = Some(Plant::LostUpdate);
        !run_sim(&cfg).invariant_holds()
    });
    let Some(seed) = caught else {
        panic!(
            "no seed in 1..={SEEDS} surfaced the planted race on the shape CI sweeps: \
             the gate's detection power is what has changed, so investigate it — \
             do not widen the sweep to make this pass"
        );
    };

    // The counterpart, and the reason the search above proves anything: the
    // seed that just failed is clean when the router is honest. Without it a
    // harness that reported a violation for every seed — a broken verifier, a
    // client that never connected — would pass the self-test while measuring
    // nothing. One seed rather than the whole range because a `standard` run
    // is by far the most expensive thing in this suite, and the range is
    // already swept unplanted, seventy-five seeds wide, by the gate this test
    // is about.
    let outcome = run_sim(&SimConfig::standard(1, seed));
    assert!(
        outcome.invariant_holds(),
        "seed {seed} violated an invariant with an honest server: {outcome:?}"
    );
    assert!(
        outcome.invariants_were_exercised(),
        "seed {seed} decided nothing, so its clean bill is worth nothing: {outcome:?}"
    );
}
