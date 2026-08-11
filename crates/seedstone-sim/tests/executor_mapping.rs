//! The executor dimension, held to the same standard as the rest of the
//! simulation: correctness must not depend on how many executors host the
//! shards, while the schedule — and so the trace — legitimately does.

use seedstone_sim::{SimConfig, run_sim};

#[test]
fn the_invariant_holds_at_every_executor_count() {
    let mut hashes = Vec::new();
    for executors in [1u16, 2, 4, 10] {
        let mut cfg = SimConfig::mini(7, 11);
        cfg.executors = executors;
        let outcome = run_sim(&cfg);
        assert!(
            outcome.invariant_holds(),
            "executors={executors}: lost an update (expected={} actual={})",
            outcome.expected_sum,
            outcome.actual_sum
        );
        hashes.push(outcome.trace_hash);
    }
    hashes.sort_unstable();
    hashes.dedup();
    // Different partitions schedule differently; four identical hashes would
    // mean the executor count reaches nothing the simulator can see.
    assert!(
        hashes.len() > 1,
        "the executor count is invisible to the trace"
    );
}

#[test]
fn the_same_config_reproduces_the_same_trace_in_process() {
    let cfg = SimConfig::mini(3, 17);
    assert_eq!(run_sim(&cfg).trace_hash, run_sim(&cfg).trace_hash);
}
