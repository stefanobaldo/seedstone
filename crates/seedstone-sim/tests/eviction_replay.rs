//! The shape with a ceiling, reproduced byte for byte from a separate process.
//!
//! `planted_race.rs` makes that claim, and makes it for the shape with no
//! ceiling. It does not carry over on its own, because the ceiling puts two
//! things on the replayed path that exist nowhere else.
//!
//! The first is a read of shared state on the write path: whether a shard
//! reclaims is decided against the node's used-memory figure, a word every
//! executor keeps current, so what a run evicts depends on what that word said
//! at the moment it was read. The second is the reply the verifier's `INFO
//! stats` produces — the widest thing the trace hash folds, twenty-two
//! counters including each shard's hits, misses and expirations — which no
//! other shape asks for.
//!
//! Either is a way for two runs of one seed to disagree, and neither is
//! covered by sweeping the shape: a sweep asserts the invariants held, never
//! that the hash came back the same. So the shape that carries the most new
//! machinery is held to the same standard as the one that carries the least —
//! two fresh processes must print the same line, and it must be the line this
//! process computed.

use seedstone_sim::{SimConfig, run_sim};

/// How many eviction-shape seeds are replayed.
///
/// `planted_eviction.rs`'s count, and for the same reason: what is being shown
/// is a property of the shape rather than of any one schedule, and each seed
/// here costs two processes on top of the run this one does.
const SEEDS: u64 = 4;

/// The workload seed every run pins — the same value [`SimConfig::eviction`]
/// is built with below and the same one passed on the command line, so a
/// differing hash means a differing schedule and nothing else.
const WORKLOAD_SEED: u64 = 1;

#[test]
fn the_eviction_shape_replays_across_processes() {
    for sim_seed in 1..=SEEDS {
        let outcome = run_sim(&SimConfig::eviction(WORKLOAD_SEED, sim_seed));
        assert!(
            outcome.invariant_holds(),
            "seed {sim_seed} violated an invariant with an honest node: {outcome:?}"
        );
        // Calibration, and the reason this file is not a slower copy of
        // `planted_race.rs`: a seed that never crossed its ceiling replays a
        // shape with no eviction in it, and would agree with itself while
        // covering none of what is written above.
        assert!(
            outcome.evicted_keys > 0,
            "seed {sim_seed}: the ceiling was never reached, so nothing about \
             eviction was replayed: {outcome:?}"
        );

        let run = || {
            let out = std::process::Command::new(env!("CARGO_BIN_EXE_replay"))
                .args([
                    "--sim-seed",
                    &sim_seed.to_string(),
                    "--workload-seed",
                    &WORKLOAD_SEED.to_string(),
                    "--eviction",
                ])
                .output()
                .expect("replay spawn");
            String::from_utf8(out.stdout).unwrap()
        };
        let (a, b) = (run(), run());
        assert_eq!(
            a, b,
            "seed {sim_seed} must replay to the same trace hash in separate processes"
        );
        // Two subprocesses agreeing prove less than they look like they do:
        // they agree by construction, both building their config through the
        // same code path. What a failure report promises is that *this* run
        // reproduces, so the in-process hash is what the replayed line has to
        // carry.
        assert!(
            a.contains(&format!("trace_hash=0x{:016x}", outcome.trace_hash)),
            "seed {sim_seed}: replay must reproduce the trace hash the in-process \
             run computed (0x{:016x}), got: {a}",
            outcome.trace_hash
        );
        // Equal hashes are not the whole claim. A shape that started violating
        // its invariants would still replay to one hash, and this file would
        // pass while `planted_eviction.rs` failed somewhere else.
        assert!(
            a.contains("invariant=ok"),
            "seed {sim_seed}: the replayed run must hold its invariants too, got: {a}"
        );
    }
}
