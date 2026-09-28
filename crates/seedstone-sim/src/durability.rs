//! What a crash leaves the harness knowing, and the clocks it knows it by.
//!
//! Every instant that is compared across hosts here is a reading of
//! [`world_now`] — the simulation's own elapsed time, the same on every
//! host — never a host's `Instant`: each host's paused clock starts at a
//! different base, and a comparison across two of them would be off by that
//! offset. The offsets are sub-millisecond and constant
//! (`tests/host_clocks.rs`), and that is still one millisecond too many for
//! a durability judgement.

use std::time::Duration;

use seedstone_service::FIXED_UNIX_MILLIS;

/// The simulation's elapsed time, as every host sees it.
///
/// # Panics
///
/// Outside a simulation, where there is no world to ask.
#[must_use]
pub fn world_now() -> Duration {
    turmoil::sim_elapsed().expect("world_now is read inside a simulation")
}

/// The simulated node's wall clock: a fixed epoch plus the world's elapsed
/// time, so a replayed deadline means what it meant and a second run of the
/// same seed reads the same clock.
///
/// Not `turmoil::since_epoch`, which reads `SystemTime` when the simulation
/// is built and would make the wall clock a property of the machine.
///
/// # Panics
///
/// Outside a simulation, as [`world_now`] does.
#[must_use]
pub fn sim_wall_clock() -> u64 {
    FIXED_UNIX_MILLIS + u64::try_from(world_now().as_millis()).expect("a simulation is short")
}
