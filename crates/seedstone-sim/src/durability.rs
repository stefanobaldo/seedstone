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

use rand::rngs::ChaCha8Rng;
use rand::{RngExt, SeedableRng};
use seedstone_core::log::disk::LogFile;
use seedstone_core::log::file::FileLog;
use seedstone_core::log::{Record, ReplicationLog};
use seedstone_core::shard::HOUSEKEEPING_TICK;
use seedstone_service::FIXED_UNIX_MILLIS;

use crate::config::CrashPlan;
use crate::outcome::{Shared, lock};
use crate::trace::GOLDEN;
use crate::workload::Known;

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
    /// Whether the last flush wrote everything it had.
    flushed: bool,
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
            flushed: true,
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
        self.flushed = result.is_ok();
        if result.is_err() && self.drops_failed_writes {
            self.inner.drop_pending();
        }
        result
    }

    /// Reports the durable point with the instant it was reached — but only
    /// after a flush that wrote everything: the clients judge a write
    /// durable by *when* it was acknowledged, and a sync behind a failed
    /// flush covers none of what that flush kept buffered.
    fn sync(&mut self) -> std::io::Result<Option<u64>> {
        let durable = self.inner.sync()?;
        if let Some(seq) = durable
            && self.flushed
        {
            lock(&self.run.durable)[usize::from(self.shard)] = Some((seq, world_now()));
        }
        Ok(durable)
    }
}

/// How far into a run an under-load crash may fall.
///
/// A run's workload is over inside a simulated second; a crash after every
/// client has finished is a crash nothing observes.
pub const CRASH_WINDOW: Duration = Duration::from_millis(800);

/// How long the driver lets a paused node sit before crashing it at rest:
/// three ticks, so every executor has flushed and synced with margin.
pub const REST_SETTLE: Duration = HOUSEKEEPING_TICK.saturating_mul(3);

/// The instants a run crashes at, drawn once from the simulator seed.
///
/// From `sim_seed` and not the workload seed, on the two-seed rule: the
/// workload seed says what the clients ask for; everything the environment
/// does to them belongs to the other one.
#[derive(Debug, Clone)]
pub struct CrashSchedule {
    instants: Vec<Duration>,
    next: usize,
}

impl CrashSchedule {
    /// The schedule `plan` and `sim_seed` describe.
    ///
    /// # Panics
    ///
    /// Never: [`CRASH_WINDOW`] is a constant well inside a `u64` of
    /// milliseconds.
    #[must_use]
    pub fn draw(plan: CrashPlan, sim_seed: u64) -> Self {
        let CrashPlan::UnderLoad { max } = plan else {
            return Self {
                instants: Vec::new(),
                next: 0,
            };
        };
        // Decorrelated from the network's draws, which turmoil takes from
        // the same seed: the schedule is a separate stream.
        let mut rng = ChaCha8Rng::seed_from_u64(sim_seed ^ GOLDEN.rotate_left(17));
        let count = rng.random_range(0..=max);
        let window = u64::try_from(CRASH_WINDOW.as_millis()).expect("a small window");
        let mut instants: Vec<Duration> = (0..count)
            .map(|_| Duration::from_millis(rng.random_range(0..=window)))
            .collect();
        instants.sort_unstable();
        Self { instants, next: 0 }
    }

    /// Every instant, in order. Only a test asks: the driver consumes them
    /// through [`next_due`](Self::next_due).
    #[cfg(test)]
    #[must_use]
    pub fn instants(&self) -> &[Duration] {
        &self.instants
    }

    /// Whether the next crash is due at `now`; consumes it if so.
    pub fn next_due(&mut self, now: Duration) -> bool {
        match self.instants.get(self.next) {
            Some(at) if now >= *at => {
                self.next += 1;
                true
            }
            _ => false,
        }
    }
}

/// What one plain slot's owner has written since the last write it knows
/// to be durable, with when each was acknowledged.
///
/// Pruned as it grows: a write acknowledged before its shard's durable
/// point is durable, and only the latest such needs keeping — it is the
/// state the disk holds, and everything before it is unreachable.
#[derive(Debug, Default, Clone)]
pub struct SlotHistory {
    writes: Vec<(Known, Duration)>,
}

impl SlotHistory {
    /// Notes an acknowledged write, pruning what `durable_at` — the
    /// shard's durable point as the observer last reported it — makes
    /// unreachable.
    pub fn record(&mut self, known: Known, acked: Duration, durable_at: Option<Duration>) {
        self.writes.push((known, acked));
        if let Some(durable_at) = durable_at {
            let last_durable = self.writes.iter().rposition(|(_, at)| *at < durable_at);
            if let Some(keep_from) = last_durable {
                self.writes.drain(..keep_from);
            }
        }
    }

    /// Every state the key may be in after a crash whose durable point for
    /// this slot's shard was `durable_at`: the durable value — the latest
    /// write acknowledged strictly before it, or `Absent` if none — then
    /// every write acknowledged at or after it, in order.
    #[must_use]
    pub fn candidates(&self, durable_at: Option<Duration>) -> Vec<Known> {
        let split = durable_at.map_or(0, |at| {
            self.writes
                .iter()
                .take_while(|(_, acked)| *acked < at)
                .count()
        });
        let durable = self.writes[..split]
            .last()
            .map_or(Known::Absent, |(known, _)| known.clone());
        let mut candidates = vec![durable];
        candidates.extend(self.writes[split..].iter().map(|(known, _)| known.clone()));
        candidates
    }

    /// Forgets everything but `known`, which a read just confirmed at `at`.
    pub fn keep_only(&mut self, known: Known, at: Duration) {
        self.writes.clear();
        self.writes.push((known, at));
    }
}

/// Whether an increment acknowledged at `acked` on `shard` survived every
/// crash in `later` — the crashes that came after the node that applied it
/// started: each one's durable point for the shard lay strictly after the
/// acknowledgement.
///
/// The caller says which crashes are later, by index, because the instants
/// cannot: a reply can arrive after the crash of the node that sent it, so
/// an acknowledgement later than a crash is no proof the crash came first.
#[must_use]
pub fn increment_is_durable(acked: Duration, shard: u16, later: &[CrashRecord]) -> bool {
    later.iter().all(|crash| {
        crash.durable[usize::from(shard)].is_some_and(|(_, durable_at)| acked < durable_at)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::Known;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn candidates_are_the_durable_value_plus_every_write_after_it() {
        let mut slot = SlotHistory::default();
        slot.record(Known::Value(b"1".to_vec()), ms(10), None);
        slot.record(Known::Value(b"2".to_vec()), ms(20), None);
        slot.record(Known::Absent, ms(30), None);
        // Durable at 25: "2" was acknowledged before it, the delete after.
        assert_eq!(
            slot.candidates(Some(ms(25))),
            vec![Known::Value(b"2".to_vec()), Known::Absent]
        );
        // Durable at 5: nothing acknowledged before it, so the durable
        // state is the key never having existed.
        assert_eq!(
            slot.candidates(Some(ms(5))),
            vec![
                Known::Absent,
                Known::Value(b"1".to_vec()),
                Known::Value(b"2".to_vec()),
                Known::Absent
            ]
        );
        // Nothing was ever durable: same as above.
        assert_eq!(slot.candidates(None).len(), 4);
        // Durable at 35: everything is, one candidate, exact.
        assert_eq!(slot.candidates(Some(ms(35))), vec![Known::Absent]);
        // A write acknowledged *at* the durable instant is not durable.
        assert_eq!(slot.candidates(Some(ms(30))).len(), 2);
    }

    #[test]
    fn recording_prunes_what_is_known_durable() {
        let mut slot = SlotHistory::default();
        slot.record(Known::Value(b"1".to_vec()), ms(10), None);
        slot.record(Known::Value(b"2".to_vec()), ms(20), Some(ms(15)));
        // "1" was durable when "2" was recorded, so it is the one kept
        // before "2"; nothing older survives.
        slot.record(Known::Value(b"3".to_vec()), ms(40), Some(ms(35)));
        assert_eq!(
            slot.candidates(Some(ms(35))),
            vec![Known::Value(b"2".to_vec()), Known::Value(b"3".to_vec())]
        );
        assert_eq!(slot.writes.len(), 2);
    }

    #[test]
    fn an_increment_is_durable_when_every_later_crash_found_it_synced() {
        let crash = |at: u64, durable: u64| CrashRecord {
            at: ms(at),
            durable: vec![Some((0, ms(durable)))],
        };
        assert!(increment_is_durable(ms(10), 0, &[crash(50, 20)]));
        assert!(!increment_is_durable(ms(30), 0, &[crash(50, 20)]));
        assert!(
            !increment_is_durable(ms(20), 0, &[crash(50, 20)]),
            "at the instant is not before it"
        );
        assert!(
            !increment_is_durable(ms(60), 0, &[crash(50, 20)]),
            "a reply that arrived after a crash of the node that sent it"
        );
        assert!(
            increment_is_durable(ms(60), 0, &[]),
            "applied by a node no crash has touched"
        );
        assert!(
            !increment_is_durable(ms(10), 0, &[crash(50, 20), crash(90, 5)]),
            "a later crash with an earlier durable point"
        );
        assert!(increment_is_durable(ms(10), 0, &[]));
    }
}
