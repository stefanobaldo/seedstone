//! What a crash leaves the harness knowing, and the clocks it knows it by.
//!
//! Every instant that is compared across hosts here is a reading of
//! [`world_now`] — the simulation's own elapsed time, the same on every
//! host — never a host's `Instant`: each host's paused clock starts at a
//! different base, and a comparison across two of them would be off by that
//! offset. The offsets are sub-millisecond and constant
//! (`tests/host_clocks.rs`), and that is still one millisecond too many for
//! a durability judgement.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::rngs::ChaCha8Rng;
use rand::{RngExt, SeedableRng};
use seedstone_core::log::disk::{LogFile, SyncFuture};
use seedstone_core::log::file::FileLog;
use seedstone_core::log::{Record, ReplicationLog};
use seedstone_core::shard::HOUSEKEEPING_TICK;
use seedstone_service::FIXED_UNIX_MILLIS;

use crate::SIM_CHECKPOINT;
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

/// The defects an [`Observed`] log can be made to carry.
#[derive(Debug, Clone, Copy, Default)]
pub struct ObservedPlants {
    /// A completed sync raises the durable point to what is flushed when
    /// it completes, rather than to what was flushed when it was issued.
    pub syncs_from_flushed_now: bool,
}

/// The simulated node's log: the real one, with every successful sync
/// reported into the run's shared state.
pub struct Observed<F: LogFile> {
    shard: u16,
    inner: FileLog<F>,
    /// The defects it was made to carry.
    plants: ObservedPlants,
    /// Whether the segment's sync in flight was issued through this
    /// shard's log — the one of its shards that counts the flight.
    issued_here: bool,
    /// Whether the last flush wrote everything it had.
    flushed: bool,
    /// When the segment's sync in flight was issued, on the world clock:
    /// the instant its completion makes durable. A write acknowledged after
    /// it was flushed after it too, and that sync does not cover it.
    ///
    /// Shared by every shard of the segment: the executor issues the sync
    /// through one shard's log and reports its completion to all of them.
    issued_at: Arc<Mutex<Duration>>,
    /// Every record appended since the last durable point, with when, in
    /// sequence order: what dates a point the checkpoint raises.
    appended: VecDeque<(u64, Duration)>,
    /// The run's shared state, where each durable point is reported.
    run: Shared,
}

impl<F: LogFile> Observed<F> {
    /// Wraps `inner`, the log of `shard`, reporting into `run`; `issued_at`
    /// is shared by every shard of the segment `inner` writes to.
    pub const fn new(
        shard: u16,
        inner: FileLog<F>,
        plants: ObservedPlants,
        issued_at: Arc<Mutex<Duration>>,
        run: Shared,
    ) -> Self {
        Self {
            shard,
            inner,
            plants,
            issued_here: false,
            flushed: true,
            issued_at,
            appended: VecDeque::new(),
            run,
        }
    }
}

impl<F: LogFile> ReplicationLog for Observed<F> {
    fn append(&mut self, rec: Record<'_>) -> std::io::Result<()> {
        self.appended.push_back((rec.seq, world_now()));
        self.inner.append(rec)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let result = self.inner.flush();
        self.flushed = result.is_ok();
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
            self.forget_through(seq);
        }
        Ok(durable)
    }

    fn flushed_through(&self) -> Option<u64> {
        self.inner.flushed_through()
    }

    fn begin_sync(&mut self) -> Option<SyncFuture> {
        let sync = self.inner.begin_sync();
        if sync.is_some() {
            *lock(&self.issued_at) = world_now();
            lock(&self.run.syncs).0 += 1;
            self.issued_here = true;
        }
        sync
    }

    /// Reports the durable point with the instant its sync was issued —
    /// what was flushed by then is what it covered — under the guard
    /// [`sync`](ReplicationLog::sync) keeps: only after a flush that wrote
    /// everything.
    fn sync_completed(&mut self, through: Option<u64>) -> Option<u64> {
        self.flight_over();
        let through = if self.plants.syncs_from_flushed_now {
            self.inner.flushed_through()
        } else {
            through
        };
        let durable = self.inner.sync_completed(through);
        if let Some(seq) = durable
            && self.flushed
        {
            let issued_at = *lock(&self.issued_at);
            lock(&self.run.durable)[usize::from(self.shard)] = Some((seq, issued_at));
            self.forget_through(seq);
        }
        durable
    }

    fn sync_failed(&mut self) {
        self.flight_over();
        self.inner.sync_failed();
    }

    /// The checkpoint covered this shard's records up to `through`: the
    /// durable point rises to it, dated when the first record above it was
    /// appended — or now, if none was. Not simply now: a write acknowledged
    /// between that append and the snapshot's footer sits above `through`,
    /// and no sync may have covered it yet.
    fn covered(&mut self, through: u64) {
        self.inner.covered(through);
        let at = self
            .appended
            .iter()
            .find(|(seq, _)| *seq > through)
            .map_or_else(world_now, |(_, at)| *at);
        let mut durable = lock(&self.run.durable);
        let slot = &mut durable[usize::from(self.shard)];
        if slot.is_none_or(|(seq, _)| seq < through) {
            *slot = Some((through, at));
        }
        drop(durable);
        self.forget_through(through);
    }
}

impl<F: LogFile> Observed<F> {
    /// Counts the end of the segment's flight, once: through the shard it
    /// was issued through.
    fn flight_over(&mut self) {
        if std::mem::take(&mut self.issued_here) {
            lock(&self.run.syncs).1 += 1;
        }
    }

    /// Forgets the appends at or below `seq`, now durable.
    fn forget_through(&mut self, seq: u64) {
        while self
            .appended
            .front()
            .is_some_and(|(appended, _)| *appended <= seq)
        {
            self.appended.pop_front();
        }
    }
}

/// How far into a run an under-load crash may fall.
///
/// A run's workload is over inside a simulated second; a crash after every
/// client has finished is a crash nothing observes.
pub const CRASH_WINDOW: Duration = Duration::from_millis(800);

/// How long the driver lets a paused node sit before crashing it at rest,
/// on a disk whose deferred syncs take up to `latency_ms`'s top, when the
/// largest snapshot the run has written is `snapshot_bytes`.
///
/// Three ticks, so every executor has flushed and issued its sync with
/// margin; two of the slowest syncs — the one in flight when the clients
/// paused, and the one issued behind it to cover what it did not; and one
/// whole cycle at the shape's budget, because the deletes active expiry
/// writes while the clients are paused can open one, and a crash inside it
/// is not a crash at rest.
#[must_use]
pub fn rest_settle(latency_ms: (u64, u64), snapshot_bytes: u64) -> Duration {
    let cycle_ticks = snapshot_bytes.div_ceil(SIM_CHECKPOINT.bytes_per_tick.max(1));
    HOUSEKEEPING_TICK
        .saturating_mul(3 + u32::try_from(cycle_ticks).unwrap_or(u32::MAX - 3))
        .saturating_add(Duration::from_millis(latency_ms.1.saturating_mul(2)))
}

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
    /// Never: the crash window is a constant well inside a `u64` of
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

    /// How many crashes the schedule holds: what a test that wants a seed
    /// with a crash in it filters on, without running the seed.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.instants.len()
    }

    /// Whether the schedule crashes nothing.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.instants.is_empty()
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
///
/// A write whose reply never came is kept apart from one that was
/// acknowledged: a sync after it covers it *if it landed*, and nothing says
/// it did, so it never becomes the durable state — it stays one candidate
/// beside the acknowledged write before it.
#[derive(Debug, Default, Clone)]
pub struct SlotHistory {
    /// `(state, instant, acknowledged)`, in the order the owner wrote them.
    writes: Vec<(Known, Duration, bool)>,
}

impl SlotHistory {
    /// Notes an acknowledged write, pruning what `durable_at` — the
    /// shard's durable point as the observer last reported it — makes
    /// unreachable.
    pub fn record(&mut self, known: Known, acked: Duration, durable_at: Option<Duration>) {
        self.writes.push((known, acked, true));
        if let Some(durable_at) = durable_at
            && let Some(keep_from) = self.base(durable_at)
        {
            self.writes.drain(..keep_from);
        }
    }

    /// Notes a write whose reply never came, as of `at`: it may or may not
    /// have landed.
    pub fn record_unacknowledged(&mut self, known: Known, at: Duration) {
        self.writes.push((known, at, false));
    }

    /// The last acknowledged write strictly before `durable_at`, by index.
    fn base(&self, durable_at: Duration) -> Option<usize> {
        self.writes
            .iter()
            .rposition(|(_, at, acknowledged)| *acknowledged && *at < durable_at)
    }

    /// Every state the key may be in after a crash whose durable point for
    /// this slot's shard was `durable_at`: the durable value — the latest
    /// write acknowledged strictly before it, or `Absent` if none — then
    /// every write after that one, in order: those acknowledged at or after
    /// the durable point, and those whose reply never came.
    #[must_use]
    pub fn candidates(&self, durable_at: Option<Duration>) -> Vec<Known> {
        let base = durable_at.and_then(|at| self.base(at));
        let durable = base.map_or(Known::Absent, |index| self.writes[index].0.clone());
        let after = base.map_or(0, |index| index + 1);
        let mut candidates = vec![durable];
        candidates.extend(
            self.writes[after..]
                .iter()
                .map(|(known, _, _)| known.clone()),
        );
        candidates
    }

    /// Forgets everything but `known`, which a read just confirmed at `at`.
    pub fn keep_only(&mut self, known: Known, at: Duration) {
        self.writes.clear();
        self.writes.push((known, at, true));
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
    fn a_write_that_was_never_acknowledged_stays_a_candidate_under_a_later_sync() {
        let mut slot = SlotHistory::default();
        slot.record(Known::Value(b"1".to_vec()), ms(10), None);
        // A burst whose replies never came: "2" may or may not have landed.
        slot.record_unacknowledged(Known::Value(b"2".to_vec()), ms(20));
        // A sync at 30 covers "2" if it landed, and proves nothing if it
        // did not: the key holds either.
        assert_eq!(
            slot.candidates(Some(ms(30))),
            vec![Known::Value(b"1".to_vec()), Known::Value(b"2".to_vec())]
        );
        // Pruning keeps the last acknowledged write under the durable point.
        slot.record(Known::Value(b"3".to_vec()), ms(40), Some(ms(35)));
        assert_eq!(
            slot.candidates(Some(ms(45))),
            vec![Known::Value(b"3".to_vec())]
        );
        slot.record_unacknowledged(Known::Absent, ms(50));
        slot.record(Known::Value(b"4".to_vec()), ms(60), Some(ms(55)));
        assert_eq!(
            slot.candidates(Some(ms(55))),
            vec![
                Known::Value(b"3".to_vec()),
                Known::Absent,
                Known::Value(b"4".to_vec())
            ]
        );
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
