//! Compaction, shown failing and shown holding.
//!
//! Four defects: a checkpoint that reports its executor's batches covered
//! when the cycle opens instead of at the durable footer, a writer that
//! removes a rotation once any executor has covered its batches in it
//! rather than every one, a recovery that trusts a snapshot with no
//! footer, and a checkpoint that writes its footer before the log is
//! durable through what its image holds. All four are observable on the
//! swept shape itself — a crash under load is what makes them cost data or
//! serve a state nobody held — so they are served on `standard`, and the
//! honest node runs the same seeds clean.
//!
//! The claim is "some seed catches it", in the pattern `standard_catches.rs`
//! set, because whether a given seed's crash lands inside a cycle is the
//! draw of the crash schedule and not the plant's. A count that had to be
//! raised to keep this green is the finding.

use seedstone_core::shard::SyncPolicy;
use seedstone_sim::{FsyncDraw, Plant, SimConfig, SimOutcome, run_sim};

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

/// Reporting covered at the cycle's open lets the writer remove the
/// rotations holding every record between the two images, which a crash
/// inside the cycle then loses.
#[test]
fn reports_covered_at_open_is_caught() {
    let caught = (1..=SEEDS).find(|sim_seed| {
        let outcome = standard(*sim_seed, Some(Plant::ReportsCoveredAtOpen));
        outcome.lost_durable_writes > 0 || outcome.lost_durable_prefixes > 0
    });
    let Some(seed) = caught else {
        panic!(
            "no seed in 1..={SEEDS} surfaced the early cover: no crash landed inside a \
             cycle — investigate, do not widen"
        );
    };
    let honest = standard(seed, None);
    assert!(
        honest.invariant_holds(),
        "seed {seed} is not clean without the plant: {honest:?}"
    );
}

/// Removing a rotation once one executor covered its batches in it, with
/// another's records still only there: a crash before the other's snapshot
/// finds neither.
#[test]
fn removes_uncovered_is_caught() {
    let caught = (1..=SEEDS).find(|sim_seed| {
        let outcome = standard(*sim_seed, Some(Plant::RemovesUncovered));
        outcome.lost_durable_writes > 0 || outcome.lost_durable_prefixes > 0
    });
    let Some(seed) = caught else {
        panic!(
            "no seed in 1..={SEEDS} surfaced the early removal: no crash landed between one \
             executor's cover and another's — investigate, do not widen"
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

/// How many standard seeds the early footer is given, all at `--fsync
/// never`: it needs a crash that tears the log below writes a finished
/// image already holds, and the policy that syncs the log least leaves it
/// the most room. Six seeds of 1..=60 caught it there — 4, 12, 15, 39, 53
/// and 58 (read 2026-10-20) — so a window of 32 misses all of them about
/// three times in a hundred if the draws move.
const FOOTER_SEEDS: u64 = 32;

/// A footer written before the log is durable through what its image
/// holds: a crash that tears the log below those writes leaves a start
/// serving the image's later values without the log's earlier ones — a
/// state the shard never held.
#[test]
fn finishes_before_log_is_caught() {
    let never = |sim_seed: u64, plant: Option<Plant>| {
        let mut cfg = SimConfig::standard(1, sim_seed);
        cfg.fsync = FsyncDraw::Fixed(SyncPolicy::NEVER);
        cfg.planted = plant;
        run_sim(&cfg)
    };
    let caught = (1..=FOOTER_SEEDS)
        .find(|sim_seed| never(*sim_seed, Some(Plant::FinishesBeforeLog)).unheld_states > 0);
    let Some(seed) = caught else {
        panic!(
            "no seed in 1..={FOOTER_SEEDS} surfaced the early footer: no crash tore the log \
             below a finished image — investigate, do not widen"
        );
    };
    let honest = never(seed, None);
    assert!(
        honest.invariant_holds() && honest.held_checks > 0,
        "seed {seed} is not clean without the plant: {honest:?}"
    );
}
