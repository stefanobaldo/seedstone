//! The harness's self-test: a known bug the sweep has to find, and find again.
//!
//! Two claims, and neither is worth much without the other. That a seed
//! surfaces the planted lost update says the simulation can fail; that the
//! `replay` binary reproduces the same seed's trace hash from a separate
//! process says a reported failure is something another machine can pick up.

use seedstone_sim::{Plant, SimConfig, run_sim};

/// How far the search for a failing seed runs before giving up.
const SEEDS: u64 = 64;

#[test]
fn planted_race_is_caught_and_replays_across_processes() {
    let mut failing = None;
    for sim_seed in 1..=SEEDS {
        let mut cfg = SimConfig::mini(1, sim_seed);
        cfg.planted = Some(Plant::LostUpdate);
        let outcome = run_sim(&cfg);
        if !outcome.invariant_holds() {
            failing = Some((sim_seed, outcome));
            break;
        }
    }
    let (seed, outcome) = failing
        .expect("no seed in 1..=64 surfaced the planted race — widen the sweep or the window");

    let run = || {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_replay"))
            .args([
                "--sim-seed",
                &seed.to_string(),
                "--mini",
                "--plant",
                Plant::LostUpdate.name(),
            ])
            .output()
            .expect("replay spawn");
        String::from_utf8(out.stdout).unwrap()
    };
    let (a, b) = (run(), run());
    assert_eq!(
        a, b,
        "the same seed must replay to the same trace hash in separate processes"
    );
    assert!(
        a.contains("invariant=violated"),
        "the replayed seed must reproduce the violation: {a}"
    );
    // Two subprocesses agreeing with each other is not enough: they agree by
    // construction, since both build their config through the same code path.
    // What a failure report promises is that *this* run reproduces, so the
    // in-process hash is what the replayed line has to carry. Without this,
    // `Args::config` could drift from the config the test built and the
    // reproduction instructions a FAIL line prints would quietly go wrong
    // while every assertion above still passed.
    assert!(
        a.contains(&format!("trace_hash=0x{:016x}", outcome.trace_hash)),
        "replay must reproduce the trace hash the in-process run computed \
         (0x{:016x}), got: {a}",
        outcome.trace_hash
    );
}

/// The counterpart, and the reason the test above proves anything: the same
/// seeds are clean when the router is honest.
///
/// Without it a harness that reported a violation for every seed — a broken
/// verifier, a client that never connected — would pass the self-test while
/// measuring nothing.
#[test]
fn the_same_seeds_are_clean_without_the_plant() {
    for sim_seed in 1..=SEEDS {
        let outcome = run_sim(&SimConfig::mini(1, sim_seed));
        assert!(
            outcome.invariant_holds(),
            "seed {sim_seed} lost an update with the honest router: \
             expected={} actual={}",
            outcome.expected_sum,
            outcome.actual_sum
        );
    }
}
