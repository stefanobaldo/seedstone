//! Per-command timing, held to zero under a clock that does not move.
//!
//! The server times every command at the executor and publishes the total as
//! `usec` in `INFO commandstats`. Under the simulator that figure is exactly
//! zero: a handler cannot `await`, so no simulated instant passes between the
//! reading taken when an envelope arrives and the reading taken after each of
//! its commands. That is what keeps the field out of the trace fold and a
//! replay byte-stable, and it is a property of the *runtime* rather than of
//! this code — so it is asserted here, where a runtime change that started
//! advancing the clock inside a handler fails loudly instead of quietly
//! putting a wall-clock reading somewhere a hash could reach.
//!
//! The shape is the eviction one because its verifier is the only one that
//! already reads an `INFO` at the end of a run. Asking for one on a shape that
//! does not would add a broadcast to that shape's trace, which moves recorded
//! hashes for a test that has nothing to say about them.

use seedstone_sim::{SimConfig, run_sim};

#[test]
fn per_command_timing_reads_zero_under_the_paused_clock() {
    let outcome = run_sim(&SimConfig::eviction(1, 1));
    assert_eq!(
        outcome.executor_usec, 0,
        "a simulated handler spent measurable time: {outcome:?}"
    );
    // The zero above is only a claim about timing if commands were timed at
    // all. A run that issued nothing would report the same figure and prove
    // nothing.
    assert!(
        outcome.executor_calls > 0,
        "no command was counted, so the zero above says nothing: {outcome:?}"
    );
}
