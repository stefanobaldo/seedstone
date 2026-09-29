//! Recovery, exact: the node crashed with nothing in flight and every
//! write synced must come back with all of it.
//!
//! One run rather than a sweep, because nothing about the schedule is
//! being exercised — every client is paused — so a hundred seeds prove what
//! one does. It is the exact half of the durability claim; the sweep
//! carries the half a schedule can move.

use seedstone_sim::{CrashPlan, DiskFaults, SimConfig, run_sim};

#[test]
fn a_node_crashed_at_rest_serves_every_acknowledged_write_afterwards() {
    let mut cfg = SimConfig::standard(1, 1);
    cfg.crashes = CrashPlan::AtRest;
    cfg.disk = DiskFaults::TORN;
    let outcome = run_sim(&cfg);
    assert!(outcome.invariant_holds(), "{outcome:?}");
    assert!(outcome.invariants_were_exercised(), "{outcome:?}");
    assert_eq!(outcome.crashes, 1);
    assert_eq!(outcome.recoveries, 1);
    assert_eq!(outcome.lost_durable_prefixes, 0);
    assert_eq!(outcome.lost_durable_writes, 0);
    assert_eq!(
        outcome.either_checks, 0,
        "at rest nothing is uncertain: every slot is exact after the crash: {outcome:?}"
    );
    assert!(
        outcome.durable_checks > 100,
        "the settle read back every plain key against a durable model: {outcome:?}"
    );
    assert_eq!(
        outcome.expected_sum, outcome.actual_sum,
        "every increment was synced before the crash: {outcome:?}"
    );
    assert!(
        outcome.snapshot_cycles >= 1,
        "the settle is long enough for a cycle to complete before the crash: {outcome:?}"
    );
    assert_eq!(
        outcome.snapshots_refused_at_start, 0,
        "nothing was mid-cycle at rest: {outcome:?}"
    );
}
