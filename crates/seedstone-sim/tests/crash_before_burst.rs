//! A crash a client has not absorbed when it sends its next burst comes
//! before every write in that burst.
//!
//! The burst can only reach the node that replaced the crashed one, so
//! folding the crash in after its replies — as if the restarted node's
//! writes had gone to the old one — puts them on the wrong side of it. A
//! read then narrows the key to one of those writes, the model forgets a
//! state the key really held, and a later recovery that returns that state
//! is counted as a phantom.
//!
//! Three seeds of one standard workload reached it: `SET k 0; DEL k` on the
//! restarted node, absent read back, `SET k 20`, a second crash, and a torn
//! log tail restoring `0`.

use seedstone_sim::{SimConfig, run_sim};

const WORKLOAD_SEED: u64 = 285;
const SIM_SEEDS: [u64; 3] = [2_840_588, 2_846_321, 2_844_492];

#[test]
fn a_crash_before_a_burst_is_absorbed_before_its_writes() {
    for sim_seed in SIM_SEEDS {
        let outcome = run_sim(&SimConfig::standard(WORKLOAD_SEED, sim_seed));
        assert_eq!(outcome.crashes, 2, "seed {sim_seed}: {outcome:?}");
        assert_eq!(outcome.phantom_writes, 0, "seed {sim_seed}: {outcome:?}");
        assert!(outcome.invariant_holds(), "seed {sim_seed}: {outcome:?}");
        assert!(
            outcome.invariants_were_exercised(),
            "seed {sim_seed}: {outcome:?}"
        );
    }
}
