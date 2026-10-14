//! A read that finds a refused write's value in memory does not make that
//! write durable.
//!
//! Under `--fsync always` a write held for its sync when the log fails is
//! applied and answered with a refusal, and a read beside it is served from
//! memory. The read narrows the key to the refused value, but nothing was
//! acknowledged: after a crash the key may be what it was before the write,
//! and a recovery that returns that state has lost nothing it promised.
//!
//! One hostile seed reached it: a segment creation failing under a
//! pipelined `SET k v; GET k`, then a crash before the next sync.

use seedstone_sim::{SimConfig, run_sim};

const WORKLOAD_SEED: u64 = 286;
const SIM_SEED: u64 = 2_851_097;

#[test]
fn a_read_of_a_refused_write_does_not_make_it_durable() {
    let outcome = run_sim(&SimConfig::hostile(WORKLOAD_SEED, SIM_SEED));
    assert_eq!(outcome.crashes, 1, "{outcome:?}");
    assert_eq!(outcome.lost_durable_writes, 0, "{outcome:?}");
    assert!(outcome.invariant_holds(), "{outcome:?}");
    assert!(outcome.invariants_were_exercised(), "{outcome:?}");
}
