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

use seedstone_core::log::disk::LogFile;
use seedstone_core::log::file::FileLog;
use seedstone_core::log::{Record, ReplicationLog};
use seedstone_service::FIXED_UNIX_MILLIS;

use crate::outcome::{Shared, lock};

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

/// A shard's durable point: the highest sequence a successful sync covered,
/// and when on the world clock — or `None` before its first.
pub type DurablePoint = Option<(u64, Duration)>;

/// A crash, and what was durable when it struck.
#[derive(Debug, Clone)]
pub struct CrashRecord {
    /// When, on the world clock.
    #[expect(
        dead_code,
        reason = "read by the clients' model once it learns of crashes; the \
                  expectation fails, and goes, the moment it is"
    )]
    pub at: Duration,
    /// Each shard's durable point at that instant — a copy, because the
    /// node overwrites its own the moment it syncs again.
    pub durable: Vec<DurablePoint>,
}

/// The simulated node's log: the real one, with every successful sync
/// reported into the run's shared state.
pub struct Observed<F: LogFile> {
    shard: u16,
    inner: FileLog<F>,
    /// The planted defect: a failed flush drops its buffer instead of
    /// keeping it.
    drops_failed_writes: bool,
    /// The run's shared state, where each durable point is reported.
    run: Shared,
}

impl<F: LogFile> Observed<F> {
    /// Wraps `inner`, the log of `shard`, reporting into `run`.
    pub const fn new(
        shard: u16,
        inner: FileLog<F>,
        drops_failed_writes: bool,
        run: Shared,
    ) -> Self {
        Self {
            shard,
            inner,
            drops_failed_writes,
            run,
        }
    }
}

impl<F: LogFile> ReplicationLog for Observed<F> {
    fn append(&mut self, rec: Record<'_>) -> std::io::Result<()> {
        self.inner.append(rec)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let result = self.inner.flush();
        if result.is_err() && self.drops_failed_writes {
            self.inner.drop_pending();
        }
        result
    }

    fn sync(&mut self) -> std::io::Result<Option<u64>> {
        let durable = self.inner.sync()?;
        if let Some(seq) = durable {
            lock(&self.run.durable)[usize::from(self.shard)] = Some((seq, world_now()));
        }
        Ok(durable)
    }
}
