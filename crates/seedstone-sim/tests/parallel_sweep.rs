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
    assert!(
        serial.1.iter().any(|(_, _, holds)| !holds),
        "the plant must be caught inside this seed range, or the assertion \
         above compares two empty successes"
    );
}
