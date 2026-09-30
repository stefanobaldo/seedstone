//! Compaction, shown failing and shown holding.
//!
//! Two defects: a checkpoint that deletes at the rotation instead of at
//! the durable footer, and a recovery that trusts a snapshot with no
//! footer. Both are observable on the swept shape itself — a crash under
//! load is what makes them cost data — so they are served on `standard`,
//! and the honest node runs the same seeds clean.
//!
//! The claim is "some seed catches it", in the pattern `standard_catches.rs`
//! set, because whether a given seed's crash lands inside a cycle is the
//! draw of the crash schedule and not the plant's. A count that had to be
//! raised to keep this green is the finding.

use seedstone_sim::{Plant, SimConfig, SimOutcome, run_sim};

/// How many standard seeds each plant is given.
const SEEDS: u64 = 12;

/// How many the honest node is held to on its own. Fewer than the plants
/// are given, because each costs a whole `standard` run in the test build
/// and the per-PR job's budget is shared: seeds 2 to 5 already crash inside
/// a cycle, the 275-seed sweep holds every seed to the invariants, and a
/// plant's catching seed is run clean again beside it.
const HONEST_SEEDS: u64 = 6;

fn standard(sim_seed: u64, plant: Option<Plant>) -> SimOutcome {
    let mut cfg = SimConfig::standard(1, sim_seed);
    cfg.planted = plant;
    run_sim(&cfg)
}

/// The honest node: every seed cycles, holds, and decides its checks; and
/// on some seed a crash landed inside a cycle or between compaction's two
/// directory syncs, which the next start shows as a refused snapshot or a
/// file removed at start.
#[test]
fn the_honest_node_cycles_on_every_seed_and_is_crashed_inside_a_cycle_on_some() {
    let mut crashed_inside = false;
    for sim_seed in 1..=HONEST_SEEDS {
        let outcome = standard(sim_seed, None);
        assert!(
            outcome.invariant_holds(),
            "seed {sim_seed} violated an invariant with an honest node: {outcome:?}"
        );
        assert!(
            outcome.invariants_were_exercised(),
            "seed {sim_seed} decided nothing: {outcome:?}"
        );
        assert!(
            outcome.snapshot_cycles >= 2,
            "seed {sim_seed} cycled fewer than twice: {outcome:?}"
        );
        // A crash inside a cycle leaves an unfinished snapshot, which the
        // next start refuses without calling the shard lossy: nothing but a
        // crash cut it short. A shard called lossy here would excuse every
        // durable read on it for the rest of the run.
        assert_eq!(
            outcome.excused_losses, 0,
            "seed {sim_seed} excused a loss on a disk that only tears: {outcome:?}"
        );
        crashed_inside |= outcome.snapshots_refused_at_start + outcome.files_removed_at_start > 0;
    }
    assert!(
        crashed_inside,
        "no seed in 1..={HONEST_SEEDS} crashed inside a cycle: the crash window and the cycle's \
         length no longer overlap; lower SIM_CHECKPOINT.bytes_per_tick, do not widen"
    );
}

/// Deleting at the rotation loses every record between the two images to
/// a crash inside the cycle.
#[test]
fn deleting_before_the_snapshot_is_durable_is_caught() {
    let caught = (1..=SEEDS).find(|sim_seed| {
        let outcome = standard(*sim_seed, Some(Plant::DeletesBeforeDurable));
        outcome.lost_durable_writes > 0 || outcome.lost_durable_prefixes > 0
    });
    let Some(seed) = caught else {
        panic!(
            "no seed in 1..={SEEDS} surfaced the early deletion: no crash landed inside a \
             cycle — investigate, do not widen"
        );
    };
    let honest = standard(seed, None);
    assert!(
        honest.invariant_holds(),
        "seed {seed} is not clean without the plant: {honest:?}"
    );
}

/// Trusting an unfinished image restores the keys scanned before the
/// crash and loses the rest.
#[test]
fn trusting_an_unfinished_snapshot_is_caught() {
    let caught = (1..=SEEDS).find(|sim_seed| {
        let outcome = standard(*sim_seed, Some(Plant::TrustsUnfinishedSnapshot));
        outcome.lost_durable_writes > 0 || outcome.lost_durable_prefixes > 0
    });
    let Some(seed) = caught else {
        panic!(
            "no seed in 1..={SEEDS} surfaced the trusted image: no crash landed inside a \
             cycle with entries already written — investigate, do not widen"
        );
    };
    let honest = standard(seed, None);
    assert!(
        honest.invariant_holds(),
        "seed {seed} is not clean without the plant: {honest:?}"
    );
}

/// Every shape the gate sweeps cycles at least once on its first seeds,
/// and the pinned-hash shape does too: the constants in `SIM_CHECKPOINT`
/// are calibrated here.
#[test]
fn every_swept_shape_cycles() {
    for (name, outcome) in [
        ("standard", run_sim(&SimConfig::standard(1, 1))),
        ("eviction", run_sim(&SimConfig::eviction(1, 1))),
        ("mini", run_sim(&SimConfig::mini(1, 42))),
    ] {
        assert!(
            outcome.snapshot_cycles >= 1,
            "{name} did not cycle: {outcome:?}"
        );
        assert!(outcome.invariant_holds(), "{name}: {outcome:?}");
    }
    for sim_seed in 1..=4 {
        let outcome = run_sim(&SimConfig::hostile(1, sim_seed));
        assert!(
            outcome.snapshot_cycles >= 1,
            "hostile seed {sim_seed} did not complete a cycle: a write that fails is retried, so \
             the run is too short for the retries — read SETTLE_CAP before touching the faults: {outcome:?}"
        );
    }
}

/// The other plants leave the compaction counters where the honest node
/// puts them: a lost update is not a lost snapshot.
#[test]
fn unrelated_plants_do_not_move_the_compaction_verdict() {
    for plant in [Plant::LostUpdate, Plant::ServeExpired] {
        let mut cfg = SimConfig::standard(1, 1);
        cfg.planted = Some(plant);
        let outcome = run_sim(&cfg);
        assert!(
            outcome.disk_peak_bytes <= outcome.disk_bound_bytes,
            "{plant:?}: {outcome:?}"
        );
        assert!(outcome.snapshot_cycles >= 1, "{plant:?}: {outcome:?}");
    }
}

/// A seed whose crashes interrupt every executor's cycles, and whose last
/// process ends the run before its own cycle finishes: no snapshot is ever
/// reported. The bound is made of what the node reports, so there is
/// nothing to hold the directory to — and the directory is not read
/// against an empty bound.
#[test]
fn a_seed_that_never_completes_a_cycle_holds_its_invariants() {
    const NO_CYCLE: u64 = 37;
    let outcome = standard(NO_CYCLE, None);
    assert_eq!(
        outcome.snapshot_cycles, 0,
        "seed {NO_CYCLE} completes a cycle now; find another that does not: {outcome:?}"
    );
    assert!(outcome.crashes > 0, "{outcome:?}");
    assert!(outcome.invariant_holds(), "{outcome:?}");
}
