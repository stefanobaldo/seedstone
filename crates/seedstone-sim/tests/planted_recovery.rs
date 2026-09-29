//! The recovery machinery, shown failing and shown holding.
//!
//! Two defects: a reader that stops at the first damaged record, and a tick
//! that drops a buffer its write failed on. Neither is observable on the
//! swept disk — nothing there fails a write or corrupts a read, so there is
//! no damage for a reader to stop at and no failed write to drop — so both
//! are served on `hostile`, whose reads corrupt and whose writes fail, and
//! the honest node runs the same seeds clean.
//!
//! The claim is "some seed catches it", in the pattern `standard_catches.rs`
//! set, because whether a given seed puts a hole inside the durable region
//! is the disk's draw and not the plant's. A count that had to be raised to
//! keep this green is the finding.

use seedstone_sim::{Plant, SimConfig, SimOutcome, run_sim};

/// How many hostile seeds each claim is given.
const SEEDS: u64 = 12;

fn hostile(sim_seed: u64, plant: Option<Plant>) -> SimOutcome {
    let mut cfg = SimConfig::hostile(1, sim_seed);
    cfg.planted = plant;
    run_sim(&cfg)
}

/// The honest node, and the shape's calibration: every seed meets a fault
/// of some kind, recovers, and still decides its checks.
#[test]
fn the_hostile_shape_faults_on_every_seed_and_the_honest_node_holds() {
    let mut any_recovered_lossy = false;
    for sim_seed in 1..=SEEDS {
        let outcome = hostile(sim_seed, None);
        assert!(
            outcome.invariant_holds(),
            "seed {sim_seed} violated an invariant with an honest node: {outcome:?}"
        );
        assert!(
            outcome.invariants_were_exercised(),
            "seed {sim_seed} decided nothing: {outcome:?}"
        );
        assert!(
            outcome.write_faults + outcome.sync_faults + outcome.start_failures > 0
                || outcome.crashes > 0,
            "seed {sim_seed}: the hostile disk did nothing hostile; raise DiskFaults::HOSTILE: {outcome:?}"
        );
        any_recovered_lossy |= outcome.lost_durable_prefixes > 0;
    }
    assert!(
        any_recovered_lossy,
        "no seed in 1..={SEEDS} put a hole inside the durable region, so nothing here \
         distinguishes the honest reader from a prefix scan; raise corruption_permille"
    );
}

/// A reader that stops at the first damage loses records it never reports.
#[test]
fn a_prefix_scan_recovery_is_caught_as_an_unreported_loss() {
    let caught = (1..=SEEDS)
        .find(|sim_seed| hostile(*sim_seed, Some(Plant::PrefixScanRecovery)).unreported_losses > 0);
    let Some(seed) = caught else {
        panic!(
            "no seed in 1..={SEEDS} surfaced the prefix scan on the hostile shape: the \
             shape's damage no longer reaches the durable region — investigate, do not widen"
        );
    };
    let honest = hostile(seed, None);
    assert!(
        honest.invariant_holds(),
        "seed {seed} is not clean without the plant: {honest:?}"
    );
}

/// A tick that drops a failed write leaves a gap under the durable point
/// with nothing on disk to explain it.
#[test]
fn a_dropped_write_is_caught_as_an_unreported_loss() {
    let caught = (1..=SEEDS)
        .find(|sim_seed| hostile(*sim_seed, Some(Plant::DropsFailedWrite)).unreported_losses > 0);
    let Some(seed) = caught else {
        panic!(
            "no seed in 1..={SEEDS} surfaced the dropped write on the hostile shape: either \
             no write failed on any seed or the loss was excused by a truncation the \
             corruption caused — investigate, do not widen"
        );
    };
    let honest = hostile(seed, None);
    assert!(
        honest.invariant_holds(),
        "seed {seed} is not clean without the plant: {honest:?}"
    );
}

/// The other plants leave the durability counters alone.
#[test]
fn the_durability_counters_stay_silent_on_unrelated_plants() {
    for plant in [Plant::LostUpdate, Plant::ServeExpired] {
        let mut cfg = SimConfig::standard(1, 1);
        cfg.planted = Some(plant);
        let outcome = run_sim(&cfg);
        assert_eq!(
            (outcome.unreported_losses, outcome.phantom_writes),
            (0, 0),
            "{plant:?} is not a recovery failure: {outcome:?}"
        );
    }
}
