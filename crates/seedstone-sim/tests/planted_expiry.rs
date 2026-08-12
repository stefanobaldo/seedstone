//! Every expiration invariant, shown failing.
//!
//! An invariant that has never caught anything has not been shown capable of
//! catching anything, and both of these are stated over a band the client
//! refuses to judge inside — so "no violations" is a claim about a detector
//! nobody has watched work. Each test below serves the same workload through
//! one deliberate defect and requires the counter that owns it to move, then
//! requires the same seeds to run clean once the defect is gone.
//!
//! The second half is not a formality: a counter that fired on every seed,
//! planted or not, would satisfy the first half while measuring nothing.
//!
//! What these plants do *not* prove is where a real defect would live — see
//! `Plant`, which says why they rewrite requests instead of breaking
//! handlers. What they prove is that the observation a client is written
//! against — a key alive past its deadline, a key dead before it — reaches
//! the counter it is supposed to reach, and no other.

use seedstone_sim::{Plant, SimConfig, SimOutcome, run_sim};

/// How many seeds each plant is given to surface on.
///
/// These defects are not races: every one of them is deterministic given the
/// workload, so a single seed would do. The sweep is here for the second half
/// of each test — the unplanted seeds that have to come back clean — where
/// more seeds is more evidence.
const SEEDS: u64 = 4;

/// Runs `seeds` mini seeds, planted or honest, and hands back the outcomes.
fn sweep(plant: Option<Plant>) -> Vec<SimOutcome> {
    (1..=SEEDS)
        .map(|sim_seed| {
            let mut cfg = SimConfig::mini(1, sim_seed);
            cfg.planted = plant;
            run_sim(&cfg)
        })
        .collect()
}

/// The honest run these tests are measured against, asserted once so each
/// plant's own test can say only what its plant does.
///
/// It also covers the case a planted assertion cannot: that the invariants
/// decided anything at all. A run whose reads all landed inside the band
/// would report no violations and no coverage, and every "clean" assertion
/// below would be vacuous.
#[test]
fn the_same_seeds_are_clean_and_not_vacuous_without_a_plant() {
    for (seed, outcome) in sweep(None).into_iter().enumerate() {
        assert!(
            outcome.invariant_holds(),
            "seed {} violated an invariant with an honest server: {outcome:?}",
            seed + 1
        );
        assert!(
            outcome.invariants_were_exercised(),
            "seed {} decided nothing, so it proves nothing: {outcome:?}",
            seed + 1
        );
    }
}

/// A server that quietly keeps what it promised to expire.
///
/// The reads that catch it are the ones the settle exists for: a key whose
/// deadline is comfortably past, read once nothing is racing. Nothing else
/// moves — the counters are untouched by a deadline that never fires, and the
/// plain keys never had one.
#[test]
fn a_server_that_never_expires_is_caught_by_stale_reads() {
    for (seed, outcome) in sweep(Some(Plant::ServeExpired)).into_iter().enumerate() {
        let seed = seed + 1;
        assert!(
            outcome.stale_reads > 0,
            "seed {seed}: a server holding every expired key was not caught: {outcome:?}"
        );
        assert_eq!(
            outcome.expected_sum, outcome.actual_sum,
            "seed {seed}: this plant must not disturb the counter sum: {outcome:?}"
        );
        assert_eq!(
            outcome.plain_mismatches, 0,
            "seed {seed}: this plant must not disturb keys that never had a deadline: {outcome:?}"
        );
    }
}

/// A sweep that stopped asking whether an entry had a deadline at all.
///
/// Two counters have to move, and the pair is the point: the volatile keys
/// die before their deadline, which is what a client can see about the keys
/// it gave one to, and the plain keys — which no `EXPIRE` and no `SET` option
/// ever touched — vanish underneath their owner, which is what nothing but a
/// model of them could see.
#[test]
fn a_sweep_that_eats_the_living_is_caught_twice() {
    for (seed, outcome) in sweep(Some(Plant::SweepEatsAll)).into_iter().enumerate() {
        let seed = seed + 1;
        assert!(
            outcome.spurious_deaths > 0,
            "seed {seed}: keys dying before their deadline were not caught: {outcome:?}"
        );
        assert!(
            outcome.plain_mismatches > 0,
            "seed {seed}: keys with no deadline dying were not caught: {outcome:?}"
        );
        assert_eq!(
            outcome.stale_reads, 0,
            "seed {seed}: a key that died early is not a key served late: {outcome:?}"
        );
    }
}

/// The planted race, from the expiration invariants' side.
///
/// `planted_race.rs` owns the claim that the counter sum catches it. What is
/// asserted here is the other half: a run with a real defect in it, of a kind
/// neither expiration invariant is about, must leave both of them silent.
/// Without this, a counter that fired on any planted run at all would pass
/// both tests above.
#[test]
fn the_expiration_invariants_stay_silent_on_a_lost_update() {
    for (seed, outcome) in sweep(Some(Plant::LostUpdate)).into_iter().enumerate() {
        assert_eq!(
            (
                outcome.stale_reads,
                outcome.spurious_deaths,
                outcome.plain_mismatches
            ),
            (0, 0, 0),
            "seed {}: a lost update is not an expiration failure: {outcome:?}",
            seed + 1
        );
    }
}
