//! The checkpoint: an executor's snapshot of its shards, taken a budget at
//! a time in the housekeeping tick.
//!
//! **The cycle.** When the bytes the executor has handed the node's writer
//! since its last cycle opened reach `max(floor, ratio × last snapshot)` —
//! or when the writer nudged it — the tick records every owned shard's
//! sequence as its *base*, notes the last batch the executor sent, and
//! opens a snapshot file. Each tick after that serialises up to
//! [`CheckpointConfig::bytes_per_tick`] of entries through
//! [`Dict::scan`](crate::dict::Dict::scan), shard by shard, into the file.
//! When every shard's cursor has come back to `0` the footer is written and
//! the file synced: the snapshot is durable, every record below a shard's
//! base is covered by its image, and the tick reports [`Completed`] — which
//! the executor hands the writer, the only thing that removes files. The
//! checkpoint rotates nothing and removes nothing but a file it abandoned.
//!
//! **Why fuzzy.** The image is not the state at any instant — a key
//! scanned early may be overwritten before the cycle ends — and it does not
//! need to be: every effect in the log is absolute, so the tail from the
//! base replayed over the image is the state, whichever tick each key was
//! taken on. What that buys is a snapshot with no stall beyond one tick's
//! budget and no copy of the keyspace, on a server that has no `fork`.
//!
//! **What the bound is.** The node's statement is the writer's
//! ([`writer`](crate::log::writer)): the last snapshots, the ones being
//! written, the retained log the nudge keeps to `max(floor, ratio × S)` per
//! executor plus what was written during the cycles, and one segment of
//! granularity.
//!
//! **Failure.** Any write or sync of the snapshot file that fails — its
//! header, its entries, its footer — abandons the *file*, never the cycle:
//! part of a failed write may have landed, and a filesystem may drop what a
//! failed sync could not write and report the next one a success, so
//! nothing more is written to that file. The scan restarts into a new file
//! with the same bases, which are still correct, and the abandoned one is
//! removed — or, if that fails too, swept by the writer when this
//! executor's next snapshot is durable. A disk that never writes again
//! parks the cycle.

use std::io;
use std::path::{Path, PathBuf};

use crate::log::ReplicationLog;
use crate::log::disk::{Disk, LogFile};
use crate::log::snapshot::{Footer, SnapshotHeader, encode_entry, snapshot_name};
use crate::shard::executor::ShardState;
use crate::shard::{LogFault, Now, SnapshotReport, TraceSink};

/// Bytes of live log that open a cycle on an executor with no snapshot
/// yet, and the least that opens one afterwards.
///
/// Sixty-four megabytes: large enough that a node whose keyspace is small
/// and whose writes are few never snapshots at all, and the floor of a
/// bound that is otherwise proportional to the keyspace.
pub const SNAPSHOT_FLOOR: u64 = 64 * 1024 * 1024;

/// A cycle opens when the live log reaches this many times the last
/// snapshot's size.
///
/// One: the log may grow to the image's size before it is folded into the
/// next one, which keeps the executor's files at three images' worth plus
/// the floor.
pub const SNAPSHOT_RATIO: u64 = 1;

/// How many bytes of entries one tick serialises, per executor.
///
/// One mebibyte: at a conservative 500 MB/s that is about two milliseconds
/// of a hundred-millisecond tick, ten mebibytes a second of snapshot
/// throughput per executor, and a gibibyte executor imaged in under two
/// minutes. A byte budget rather than a bucket count because bytes are what
/// the disk and the executor pay: a bucket budget sized for the rehash
/// would write tens of megabytes in one tick on a deployed-size executor.
/// The budget bounds the scan; the scan step that crosses it — one bucket,
/// never part of one, since a cursor names a bucket — still goes out whole,
/// so a value larger than the budget costs one tick of overshoot rather
/// than a cycle that never ends.
pub const SNAPSHOT_BYTES_PER_TICK: u64 = 1024 * 1024;

/// The three numbers a checkpoint runs by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointConfig {
    /// See [`SNAPSHOT_FLOOR`].
    pub floor: u64,
    /// See [`SNAPSHOT_RATIO`].
    pub ratio: u64,
    /// See [`SNAPSHOT_BYTES_PER_TICK`].
    pub bytes_per_tick: u64,
}

impl CheckpointConfig {
    /// The binary's values.
    pub const PRODUCTION: Self = Self {
        floor: SNAPSHOT_FLOOR,
        ratio: SNAPSHOT_RATIO,
        bytes_per_tick: SNAPSHOT_BYTES_PER_TICK,
    };
}

/// Where the executor's log stands at a tick: bytes it has handed the
/// writer since the process started, and the last batch it sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogPosition {
    pub bytes: u64,
    pub batch: Option<u64>,
}

/// A snapshot became durable: what the executor tells the writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Completed {
    pub cycle: u32,
    /// The last batch the executor sent before the cycle opened: every
    /// record below the bases is in a batch up to this one.
    pub through_batch: Option<u64>,
    /// The snapshot file's size.
    pub bytes: u64,
    /// Housekeeping ticks the cycle took.
    pub ticks: u64,
    /// Shards the start reported lossy that this image covers.
    pub cleared: u64,
}

/// What one housekeeping tick of the checkpoint did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tick {
    /// `Some` on the tick a snapshot became durable.
    pub completed: Option<Completed>,
    /// The tick's step failed: the cycle ended in a fault.
    pub faulted: bool,
}

impl Tick {
    pub const NONE: Self = Self {
        completed: None,
        faulted: false,
    };
}

/// What an executor runs in its housekeeping tick.
///
/// A trait for the reason the log is one: a node without a data directory
/// runs [`NoCheckpoint`] and pays nothing, and the simulator's node runs the
/// real one over its own disk.
pub trait Checkpoint: Send + 'static {
    /// One tick's work over the executor's shards, with `first_shard` the
    /// id of `states[0]`, `now` the tick's one clock reading and `log`
    /// where the executor's log stands.
    fn tick<L: ReplicationLog, T: TraceSink>(
        &mut self,
        first_shard: u16,
        states: &mut [ShardState<L>],
        now: Now,
        trace: &T,
        log: LogPosition,
    ) -> Tick;

    /// Open a cycle on the next tick whatever the live log — the way out of
    /// a log the disk refused: everything in memory, imaged into a fresh
    /// file. A cycle already open is abandoned, because its bases predate
    /// the refusal.
    fn force(&mut self) {}

    /// Open a cycle on the next tick whatever the live log, without
    /// abandoning one that is open: the writer's nudge.
    fn nudge(&mut self) {}

    /// Whether a cycle is open.
    fn is_open(&self) -> bool {
        false
    }
}

/// The checkpoint of a node with no data directory: nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoCheckpoint;

impl Checkpoint for NoCheckpoint {
    fn tick<L: ReplicationLog, T: TraceSink>(
        &mut self,
        _first_shard: u16,
        _states: &mut [ShardState<L>],
        _now: Now,
        _trace: &T,
        _log: LogPosition,
    ) -> Tick {
        Tick::NONE
    }
}

/// Everything a [`SegmentCheckpoint`] is built from.
pub struct CheckpointSpec<D: Disk> {
    pub disk: D,
    /// The `wal/` directory.
    pub wal: PathBuf,
    pub generation: u64,
    pub executor: u16,
    pub config: CheckpointConfig,
}

/// The real checkpoint, over the seam.
pub struct SegmentCheckpoint<D: Disk> {
    disk: D,
    wal: PathBuf,
    generation: u64,
    executor: u16,
    config: CheckpointConfig,
    /// The next cycle's number: also how many files this executor has
    /// opened, abandoned ones included.
    cycle: u32,
    /// Cycles that reached a durable footer.
    completed: u32,
    /// The last durable snapshot's size.
    last_snapshot: u64,
    /// `LogPosition::bytes` when the last cycle opened: the live log is
    /// what the executor sent since.
    opened_at: u64,
    /// The planted defect: report `Completed` at the open.
    reports_covered_at_open: bool,
    /// The next tick opens a cycle whatever the live log: see
    /// [`Checkpoint::force`].
    forced: bool,
    /// The next tick opens a cycle whatever the live log: see
    /// [`Checkpoint::nudge`].
    nudged: bool,
    open: Option<Cycle<D::File>>,
}

/// One cycle in progress.
struct Cycle<F> {
    /// `None` until the file is created — and again after a footer sync
    /// failed and the file was abandoned.
    file: Option<F>,
    /// The cycle's number, which names its file.
    number: u32,
    /// Per owned shard, by offset from `first_shard`.
    bases: Vec<u64>,
    /// Per owned shard: the scan cursor, or `None` once it came back to 0.
    cursors: Vec<Option<u64>>,
    counts: Vec<u64>,
    /// Encoded entries not yet written, drained on the tick that scanned them.
    buffer: Vec<u8>,
    scratch: Vec<u8>,
    /// Bytes written to the file so far, header included.
    bytes: u64,
    ticks: u64,
    /// The footer has been written to the file and awaits its sync.
    footer_written: bool,
    /// The file is synced and the directory sync is what remains.
    dir_pending: bool,
    /// How far past the threshold the log had grown when the cycle opened:
    /// it crosses between two ticks and the cycle opens on the next.
    overshoot: u64,
    /// The last batch the executor sent before the cycle opened.
    through_batch: Option<u64>,
    /// `LogPosition::bytes` at the open.
    opened_at: u64,
    /// The wall clock at the open, Unix milliseconds: the image describes
    /// memory as of its bases, so this is its time.
    opened_unix: u64,
}

impl<F> Cycle<F> {
    fn fresh(
        number: u32,
        bases: Vec<u64>,
        overshoot: u64,
        through_batch: Option<u64>,
        opened_at: u64,
    ) -> Self {
        let shards = bases.len();
        Self {
            file: None,
            number,
            bases,
            cursors: vec![Some(0); shards],
            counts: vec![0; shards],
            buffer: Vec::new(),
            scratch: Vec::new(),
            bytes: 0,
            ticks: 0,
            footer_written: false,
            dir_pending: false,
            overshoot,
            through_batch,
            opened_at,
            opened_unix: 0,
        }
    }

    fn scanned_all(&self) -> bool {
        self.cursors.iter().all(Option::is_none)
    }
}

impl<D: Disk + Send + 'static> SegmentCheckpoint<D> {
    #[must_use]
    pub fn new(spec: CheckpointSpec<D>) -> Self {
        Self {
            disk: spec.disk,
            wal: spec.wal,
            generation: spec.generation,
            executor: spec.executor,
            config: spec.config,
            cycle: 0,
            completed: 0,
            last_snapshot: 0,
            opened_at: 0,
            reports_covered_at_open: false,
            forced: false,
            nudged: false,
            open: None,
        }
    }

    /// The planted defect: `Completed` is reported when the cycle opens,
    /// as if the bases were the durable point. What a node that took "the
    /// old log is redundant" to mean the open rather than the synced footer
    /// would do — the writer then removes the log a crash mid-cycle needs.
    /// This server never does it; it exists so the simulator can plant
    /// exactly that and show it caught.
    pub const fn reports_covered_at_open(&mut self, plant: bool) {
        self.reports_covered_at_open = plant;
    }

    /// Cycles that reached a durable footer.
    #[must_use]
    pub const fn cycles_completed(&self) -> u32 {
        self.completed
    }

    /// Whether a cycle is open.
    #[must_use]
    pub const fn is_open(&self) -> bool {
        self.open.is_some()
    }

    /// The open cycle's bases, per owned shard.
    #[cfg(test)]
    fn open_bases(&self) -> Vec<u64> {
        self.open
            .as_ref()
            .map_or_else(Vec::new, |cycle| cycle.bases.clone())
    }

    /// Bytes of live log at which the next cycle opens.
    fn threshold(&self) -> u64 {
        self.config
            .floor
            .max(self.config.ratio.saturating_mul(self.last_snapshot))
    }

    /// Takes the bases and opens the cycle. Nothing is written to the
    /// snapshot file yet; `step` creates it.
    fn open_cycle<L: ReplicationLog>(
        &mut self,
        states: &[ShardState<L>],
        log: LogPosition,
        unix_millis: u64,
    ) {
        let overshoot = log
            .bytes
            .saturating_sub(self.opened_at)
            .saturating_sub(self.threshold());
        self.opened_at = log.bytes;
        let bases = states.iter().map(|state| state.seq).collect();
        let mut cycle = Cycle::fresh(self.cycle, bases, overshoot, log.batch, log.bytes);
        cycle.opened_unix = unix_millis;
        self.open = Some(cycle);
        self.cycle += 1;
    }

    /// Creates the cycle's file with its header, if it is not open yet.
    fn ensure_file(&self, first_shard: u16, cycle: &mut Cycle<D::File>) -> io::Result<()> {
        if cycle.file.is_some() {
            return Ok(());
        }
        let header = SnapshotHeader {
            generation: self.generation,
            executor: self.executor,
            cycle: cycle.number,
            unix_millis: cycle.opened_unix,
            bases: cycle
                .bases
                .iter()
                .enumerate()
                .map(|(offset, base)| {
                    (
                        first_shard
                            + u16::try_from(offset)
                                .expect("a shard range is shorter than u16::MAX"),
                        *base,
                    )
                })
                .collect(),
        };
        let mut file = self.disk.create_append(&self.wal.join(snapshot_name(
            self.generation,
            self.executor,
            cycle.number,
        )))?;
        let mut bytes = Vec::with_capacity(header.encoded_len());
        header.encode(&mut bytes);
        file.write_all(&bytes)?;
        // The header and the directory entry, synced once at the open: a
        // crash mid-cycle then leaves a file with a header and no footer —
        // which recovery refuses — rather than an orphan the filesystem
        // drops, whose header a start could not tell from damage. The
        // entries themselves are synced only with the footer.
        file.sync_data()?;
        self.disk.sync_dir(&self.wal)?;
        cycle.bytes = bytes.len() as u64;
        cycle.file = Some(file);
        Ok(())
    }

    /// Gives up on the cycle's file and restarts the scan into a new one,
    /// with the same bases, which are still right.
    ///
    /// The file is never written to again, because what a failed write or
    /// sync left in it is unknown: part of a write may have landed, or a
    /// filesystem may drop the pages a failed sync could not write. Writing
    /// on after it — a second header, or a retried buffer behind a partial
    /// one — would leave a finished image that recovery must refuse, once
    /// the writer had already removed the log it covers. The file is removed
    /// if it can be; one that stays is an older snapshot of this executor,
    /// which the writer removes when this executor's next one is durable.
    fn restart(&mut self, cycle: &mut Cycle<D::File>) {
        let abandoned = self
            .wal
            .join(snapshot_name(self.generation, self.executor, cycle.number));
        let mut restarted = Cycle::fresh(
            self.cycle,
            std::mem::take(&mut cycle.bases),
            cycle.overshoot,
            cycle.through_batch,
            cycle.opened_at,
        );
        restarted.ticks = cycle.ticks;
        restarted.opened_unix = cycle.opened_unix;
        self.cycle += 1;
        *cycle = restarted;
        self.remove_abandoned(&abandoned);
    }

    /// Best effort, and not reported: a failure here leaves a file the
    /// writer's next compaction removes, and reports then if it fails again.
    fn remove_abandoned(&self, path: &Path) {
        let _ = self.disk.remove_file(path);
    }

    /// Writes whatever the buffer holds.
    fn drain(cycle: &mut Cycle<D::File>) -> io::Result<()> {
        if cycle.buffer.is_empty() {
            return Ok(());
        }
        let file = cycle.file.as_mut().expect("ensure_file ran first");
        file.write_all(&cycle.buffer)?;
        cycle.bytes += cycle.buffer.len() as u64;
        cycle.buffer.clear();
        Ok(())
    }

    /// Scans up to the budget's worth of entries into the buffer.
    fn scan<L: ReplicationLog>(
        config: CheckpointConfig,
        first_shard: u16,
        states: &[ShardState<L>],
        now: Now,
        cycle: &mut Cycle<D::File>,
    ) {
        let Cycle {
            bases,
            cursors,
            counts,
            buffer,
            scratch,
            ..
        } = cycle;
        let mut spent = 0u64;
        for (offset, state) in states.iter().enumerate() {
            let Some(mut cursor) = cursors[offset] else {
                continue;
            };
            let shard = first_shard
                + u16::try_from(offset).expect("a shard range is shorter than u16::MAX");
            let base = bases[offset];
            loop {
                if spent >= config.bytes_per_tick {
                    cursors[offset] = Some(cursor);
                    return;
                }
                let before = buffer.len();
                let mut visited = 0u64;
                cursor = state.dict.scan(cursor, |key, entry| {
                    encode_entry(
                        shard,
                        base,
                        key,
                        &entry.value,
                        now.deadline_millis(entry.expires_at),
                        scratch,
                        buffer,
                    );
                    visited += 1;
                });
                counts[offset] += visited;
                spent += (buffer.len() - before) as u64;
                if cursor == 0 {
                    cursors[offset] = None;
                    break;
                }
            }
        }
    }

    /// One tick of an open cycle. `Ok(true)` once the snapshot is durable.
    fn step<L: ReplicationLog>(
        &mut self,
        first_shard: u16,
        states: &[ShardState<L>],
        now: Now,
    ) -> io::Result<bool> {
        let mut cycle = self.open.take().expect("step runs on an open cycle");
        let result = self.step_inner(first_shard, states, now, &mut cycle);
        self.open = Some(cycle);
        result
    }

    fn step_inner<L: ReplicationLog>(
        &mut self,
        first_shard: u16,
        states: &[ShardState<L>],
        now: Now,
        cycle: &mut Cycle<D::File>,
    ) -> io::Result<bool> {
        cycle.ticks += 1;
        if let Err(error) = self.ensure_file(first_shard, cycle) {
            self.restart(cycle);
            return Err(error);
        }
        if cycle.dir_pending {
            self.disk.sync_dir(&self.wal)?;
            cycle.dir_pending = false;
            return Ok(true);
        }
        if cycle.footer_written {
            return self.sync_footer(cycle);
        }
        Self::scan(self.config, first_shard, states, now, cycle);
        if cycle.scanned_all() {
            let counts = cycle
                .counts
                .iter()
                .enumerate()
                .map(|(offset, entries)| {
                    (
                        first_shard
                            + u16::try_from(offset)
                                .expect("a shard range is shorter than u16::MAX"),
                        *entries,
                    )
                })
                .collect();
            Footer { counts }.encode_record(cycle.number, &mut cycle.buffer);
            cycle.buffer.push(crate::log::END_OF_LOG);
            cycle.footer_written = true;
        }
        if let Err(error) = Self::drain(cycle) {
            self.restart(cycle);
            return Err(error);
        }
        if cycle.footer_written {
            return self.sync_footer(cycle);
        }
        Ok(false)
    }

    /// Syncs the file and then the directory. A failed file sync abandons
    /// the file and restarts the scan into the next one, with the same
    /// bases (see the module doc); a failed directory sync is retried.
    fn sync_footer(&mut self, cycle: &mut Cycle<D::File>) -> io::Result<bool> {
        let file = cycle.file.as_mut().expect("the footer was written");
        if let Err(error) = file.sync_data() {
            self.restart(cycle);
            return Err(error);
        }
        cycle.dir_pending = true;
        self.disk.sync_dir(&self.wal)?;
        cycle.dir_pending = false;
        Ok(true)
    }
}

impl<D: Disk + Send + 'static> SegmentCheckpoint<D> {
    /// The snapshot is durable: cover the shards, report, count.
    fn finish<L: ReplicationLog, T: TraceSink>(
        &mut self,
        states: &mut [ShardState<L>],
        trace: &T,
        log: LogPosition,
    ) -> Completed {
        let cycle = self.open.take().expect("finish runs on an open cycle");
        for (state, base) in states.iter_mut().zip(&cycle.bases) {
            if let Some(through) = base.checked_sub(1) {
                state.log.covered(through);
            }
        }
        self.last_snapshot = cycle.bytes;
        self.completed += 1;
        let mut cleared = 0;
        for state in states.iter_mut() {
            cleared += u64::from(std::mem::take(&mut state.lossy));
            state.image_unix_millis = Some(cycle.opened_unix);
        }
        trace.snapshot(&SnapshotReport {
            executor: self.executor,
            cycle: cycle.number,
            entries: cycle.counts.iter().sum(),
            bytes: cycle.bytes,
            ticks: cycle.ticks,
            disk_bytes: self.disk_bytes(),
            bytes_written: cycle.overshoot + log.bytes.saturating_sub(cycle.opened_at),
            cleared,
        });
        Completed {
            cycle: cycle.number,
            through_batch: cycle.through_batch,
            bytes: cycle.bytes,
            ticks: cycle.ticks,
            cleared,
        }
    }

    /// Every file under `wal/`, summed: the directory's size right now.
    fn disk_bytes(&self) -> u64 {
        self.disk
            .list(&self.wal)
            .unwrap_or_default()
            .iter()
            .filter_map(|name| self.disk.len(&self.wal.join(name)).ok())
            .sum()
    }
}

impl<D: Disk + Send + 'static> Checkpoint for SegmentCheckpoint<D> {
    fn tick<L: ReplicationLog, T: TraceSink>(
        &mut self,
        first_shard: u16,
        states: &mut [ShardState<L>],
        now: Now,
        trace: &T,
        log: LogPosition,
    ) -> Tick {
        let mut early = None;
        if self.open.is_none() {
            let live = log.bytes.saturating_sub(self.opened_at);
            if !self.forced && !self.nudged && live < self.threshold() {
                return Tick::NONE;
            }
            self.open_cycle(states, log, now.unix_millis);
            self.forced = false;
            self.nudged = false;
            if self.reports_covered_at_open {
                // The plant: the bases taken for the durable point.
                let cycle = self.open.as_ref().expect("just opened");
                early = Some(Completed {
                    cycle: cycle.number,
                    through_batch: cycle.through_batch,
                    bytes: self.last_snapshot,
                    ticks: 0,
                    cleared: 0,
                });
            }
        }
        match self.step(first_shard, states, now) {
            Ok(false) => Tick {
                completed: early,
                faulted: false,
            },
            Ok(true) => Tick {
                completed: Some(self.finish(states, trace, log)),
                faulted: false,
            },
            Err(error) => {
                trace.fault(first_shard, LogFault::Snapshot, &error);
                Tick {
                    completed: early,
                    faulted: true,
                }
            }
        }
    }

    fn force(&mut self) {
        if let Some(cycle) = self.open.take() {
            // Its file, if it has one, is never finished: whatever it holds
            // was scanned against bases the refusal made wrong.
            if cycle.file.is_some() {
                let abandoned =
                    self.wal
                        .join(snapshot_name(self.generation, self.executor, cycle.number));
                self.remove_abandoned(&abandoned);
            }
        }
        self.forced = true;
    }

    fn nudge(&mut self) {
        self.nudged = true;
    }

    fn is_open(&self) -> bool {
        self.open.is_some()
    }
}

#[cfg(test)]
mod tests;
