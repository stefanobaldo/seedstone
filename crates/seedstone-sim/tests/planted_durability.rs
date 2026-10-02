//! The durability policy, shown failing and shown holding.

use seedstone_core::shard::SyncPolicy;
use seedstone_sim::{FsyncDraw, Plant, SimConfig, SimOutcome, run_sim};

const SEEDS: u64 = 12;
const HONEST_SEEDS: u64 = 6;
const HOSTILE_SEEDS: u64 = 24;
const POLICIES: [SyncPolicy; 3] = [SyncPolicy::ALWAYS, SyncPolicy::INTERVAL, SyncPolicy::NEVER];

fn standard(sim_seed: u64, policy: SyncPolicy, plant: Option<Plant>) -> SimOutcome {
    let mut cfg = SimConfig::standard(1, sim_seed);
    cfg.fsync = FsyncDraw::Fixed(policy);
    cfg.planted = plant;
    run_sim(&cfg)
}

fn hostile(sim_seed: u64, policy: SyncPolicy, plant: Option<Plant>) -> SimOutcome {
    let mut cfg = SimConfig::hostile(1, sim_seed);
    cfg.fsync = FsyncDraw::Fixed(policy);
    cfg.planted = plant;
    run_sim(&cfg)
}

/// The honest node holds every policy's invariant on the swept shape, and
/// on some seed a crash lands with a sync in flight.
#[test]
fn the_honest_node_holds_under_every_policy_and_is_crashed_mid_flight_on_some_seed() {
    for policy in POLICIES {
        let mut in_flight = 0;
        for sim_seed in 1..=HONEST_SEEDS {
            let outcome = standard(sim_seed, policy, None);
            eprintln!(
                "honest {} seed {sim_seed}: crashes {} in flight {}",
                policy.name(),
                outcome.crashes,
                outcome.crashes_in_flight
            );
            assert!(
                outcome.invariant_holds(),
                "{} seed {sim_seed}: {outcome:?}",
                policy.name()
            );
            assert!(
                outcome.invariants_were_exercised(),
                "{} seed {sim_seed} decided nothing: {outcome:?}",
                policy.name()
            );
            assert_eq!(
                outcome.refused,
                0,
                "{} seed {sim_seed} refused on a disk that only tears",
                policy.name()
            );
            in_flight += outcome.crashes_in_flight;
        }
        if policy != SyncPolicy::NEVER {
            assert!(
                in_flight > 0,
                "{}: no seed in 1..={HONEST_SEEDS} crashed with a sync in flight; raise the \
                 latency range's top, do not widen the seeds",
                policy.name()
            );
        }
    }
}

/// Releasing a batch at issue rather than at completion loses an
/// acknowledged write to a crash inside the flight.
#[test]
fn releasing_before_the_sync_completes_is_caught() {
    let caught = (1..=SEEDS).find(|seed| {
        let outcome = standard(*seed, SyncPolicy::ALWAYS, Some(Plant::ReleasesOnIssue));
        outcome.lost_durable_writes > 0
    });
    let Some(seed) = caught else {
        panic!("no seed in 1..={SEEDS} surfaced the early release — investigate, do not widen")
    };
    eprintln!("releases-on-issue: first caught on seed {seed}");
    assert!(
        standard(seed, SyncPolicy::ALWAYS, None).invariant_holds(),
        "seed {seed} is not clean without the plant"
    );
}

/// Raising the point to what was flushed during the flight claims as
/// durable what the sync never covered.
#[test]
fn syncing_from_the_point_of_now_rather_than_of_issue_is_caught() {
    let caught = (1..=SEEDS).find(|seed| {
        let outcome = standard(
            *seed,
            SyncPolicy::INTERVAL,
            Some(Plant::SyncsFromFlushedNow),
        );
        outcome.lost_durable_writes > 0 || outcome.lost_durable_prefixes > 0
    });
    let Some(seed) = caught else {
        panic!("no seed in 1..={SEEDS} surfaced the point of now — investigate, do not widen")
    };
    eprintln!("syncs-from-flushed-now: first caught on seed {seed}");
    assert!(
        standard(seed, SyncPolicy::INTERVAL, None).invariant_holds(),
        "seed {seed} is not clean without the plant"
    );
}

/// Acknowledging while refusing breaks the refusal's own promise: after
/// a log fault, no write is answered as done until the snapshot that ends
/// the refusal is durable. Shown at `interval`, where the answer reaches
/// the client at once; under `always` it is held for a sync on the failed
/// segment, which fails, and the client gets the refusal after all.
#[test]
fn acknowledging_while_refusing_is_caught() {
    let caught = (1..=HOSTILE_SEEDS).find(|seed| {
        let outcome = hostile(*seed, SyncPolicy::INTERVAL, Some(Plant::AcksWhileRefusing));
        outcome.acked_while_refusing > 0
    });
    let Some(seed) = caught else {
        panic!(
            "no seed in 1..={HOSTILE_SEEDS} surfaced the acknowledged refusal — investigate, do \
             not widen"
        )
    };
    eprintln!("acks-while-refusing: first caught on seed {seed}");
    assert!(
        hostile(seed, SyncPolicy::INTERVAL, None).invariant_holds(),
        "seed {seed} is not clean without the plant"
    );
}

/// On the disk that fails, the honest node refuses on some seed and ends
/// its refusal on some seed, under every policy; every refusal follows a
/// fault.
#[test]
fn the_honest_node_refuses_and_resumes_on_the_hostile_disk() {
    for policy in POLICIES {
        let (mut refused, mut ended) = (0, 0);
        for sim_seed in 1..=HOSTILE_SEEDS {
            let outcome = hostile(sim_seed, policy, None);
            assert!(
                outcome.invariant_holds(),
                "{} seed {sim_seed}: {outcome:?}",
                policy.name()
            );
            refused += u64::from(outcome.refused > 0);
            ended += u64::from(outcome.refusals_ended > 0);
        }
        eprintln!(
            "hostile {}: {refused} of {HOSTILE_SEEDS} seeds refused, {ended} resumed",
            policy.name()
        );
        assert!(
            refused > 0,
            "{}: no hostile seed in 1..={HOSTILE_SEEDS} refused a write",
            policy.name()
        );
        assert!(
            ended > 0,
            "{}: no hostile seed in 1..={HOSTILE_SEEDS} ended a refusal",
            policy.name()
        );
    }
}
