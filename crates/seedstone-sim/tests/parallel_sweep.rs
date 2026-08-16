//! Parallelism starts runs; it must not reach inside one. The artifact a
//! sweep produces — which seeds violated, and every trace hash — must be
//! byte-identical at any worker count, or a failure report would depend on
//! thread timing, which is the one way a parallel sweep could stop being
//! reproducible even though every run inside it is.

use seedstone_sim::{Plant, SimConfig, sweep};

fn collect(workers: usize) -> (u64, Vec<(u64, u64, bool)>) {
    let mut rows = Vec::new();
    let violations = sweep(
        1,
        8,
        workers,
        |sim_seed| {
            let mut cfg = SimConfig::mini(1, sim_seed);
            cfg.planted = Plant::from_name("lost-update");
            cfg
        },
        |seed, outcome| rows.push((seed, outcome.trace_hash, outcome.invariant_holds())),
    );
    (violations, rows)
}

#[test]
fn worker_count_changes_nothing_but_wall_clock() {
    let serial = collect(1);
    let parallel = collect(4);
    assert_eq!(serial, parallel);
    // Agreement is not completeness: two sweeps that both reported five of
    // the eight seeds would compare equal and say nothing.
    assert_eq!(serial.1.len(), 8, "every seed in the range is reported");
    assert!(
        serial.1.iter().map(|&(seed, ..)| seed).eq(1..=8),
        "reported in ascending seed order, with no seed repeated or skipped"
    );
    assert!(
        serial.1.iter().any(|(_, _, holds)| !holds),
        "the plant must be caught inside this seed range, or the assertion \
         above compares two empty successes"
    );
}

/// The last seed in the space is a seed like any other: `--seed-start
/// 18446744073709551615 --seeds 1` is a sweep a user can type. The workers
/// draw their seeds from a counter that wraps there, so a sweep reaching it
/// used to hand every remaining worker a seed below the range and keep going
/// forever, filling the reorder map with outcomes nobody was waiting for.
/// A sweep that cannot finish its own last seed reports nothing at all.
#[test]
fn the_last_seed_in_the_space_terminates() {
    let mut rows = Vec::new();
    let violations = sweep(
        u64::MAX,
        1,
        4,
        |sim_seed| SimConfig::mini(1, sim_seed),
        |seed, outcome| rows.push((seed, outcome.trace_hash)),
    );
    assert_eq!(violations, 0, "the mini shape holds unplanted");
    assert_eq!(rows.len(), 1, "one seed asked for, one seed reported");
    assert_eq!(rows[0].0, u64::MAX);
}
