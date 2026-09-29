//! The checkpoint: an executor's snapshot of its shards, taken a budget at
//! a time in the housekeeping tick, and the compaction that follows it.
//!
//! **The cycle.** When the executor's live log — bytes written since the
//! last rotation — reaches `max(floor, ratio × last snapshot)`, the tick
//! rotates the segment, records every owned shard's sequence as its
//! *base*, and opens a snapshot file. Each tick after that serialises up
//! to [`CheckpointConfig::bytes_per_tick`] of entries through
//! [`Dict::scan`](crate::dict::Dict::scan), shard by shard, into the file. When every shard's
//! cursor has come back to `0` the footer is written and the file synced:
//! the snapshot is durable, every record below a shard's base is covered
//! by its image, and the executor's older rotations and its previous
//! snapshot are deleted.
//!
//! **Why fuzzy.** The image is not the state at any instant — a key
//! scanned early may be overwritten before the cycle ends — and it does not
//! need to be: every effect in the log is absolute, so the tail from the
//! base replayed over the image is the state, whichever tick each key was
//! taken on. What that buys is a snapshot with no stall beyond one tick's
//! budget and no copy of the keyspace, on a server that has no `fork`.
//!
//! **What the bound is.** Per executor, at any instant,
//! `disk ≤ S_prev + S_cur + max(floor, ratio × S_prev) + W`: the last
//! durable snapshot, the one in progress, the live log at the trigger, and
//! what was written during the cycle. The production constants below put
//! that at three times the last snapshot plus 64 MiB plus one cycle's
//! writes; a node's directory is the sum over its executors.
//!
//! **Failure.** A write that fails keeps its buffer and is retried on the
//! next tick before any scanning: the cycle is never abandoned, and a disk
//! that never writes again parks it — the bound holds on a disk that
//! eventually writes. A footer sync that fails abandons the *file*: a
//! filesystem may drop what a failed sync could not write and report the
//! next one a success, so the scan restarts into a new file with the same
//! bases, which are still correct. A removal that fails is reported and
//! tried again at the next cycle; recovery removes it at the next start.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

use crate::log::ReplicationLog;
use crate::log::disk::{Disk, LogFile};
use crate::log::file::{
    SharedSegment, live_log_bytes, lock, parse_segment_name, rotate_segment,
    segment_snapshot_covered,
};
use crate::log::snapshot::{
    Footer, SnapshotHeader, encode_entry, parse_snapshot_name, snapshot_name,
};
use crate::shard::executor::ShardState;
use crate::shard::{CompactionReport, LogFault, Now, SnapshotReport, TraceSink};

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

/// What an executor runs in its tick after the log's two passes.
///
/// A trait for the reason the log is one: a node without a data directory
/// runs [`NoCheckpoint`] and pays nothing, and the simulator's node runs the
/// real one over its own disk.
pub trait Checkpoint: Send + 'static {
    /// One tick's work over the executor's shards, with `first_shard` the
    /// id of `states[0]` and `now` the tick's one clock reading.
    fn tick<L: ReplicationLog, T: TraceSink>(
        &mut self,
        first_shard: u16,
        states: &mut [ShardState<L>],
        now: Now,
        trace: &T,
    );
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
    ) {
    }
}

/// Everything a [`SegmentCheckpoint`] is built from.
pub struct CheckpointSpec<D: Disk> {
    pub disk: D,
    /// The `wal/` directory.
    pub wal: PathBuf,
    pub generation: u64,
    pub executor: u16,
    /// How many executors this generation runs — what the round counts to.
    pub executors: u16,
    /// The executor's segment, shared with its shards' logs.
    pub segment: SharedSegment<D::File>,
    /// Shared by every executor of the process: how many have completed a
    /// cycle in this generation. The one that brings it to `executors`
    /// deletes every older generation's files.
    pub round: Arc<AtomicU16>,
    pub config: CheckpointConfig,
}

/// The real checkpoint, over the seam.
pub struct SegmentCheckpoint<D: Disk> {
    disk: D,
    wal: PathBuf,
    generation: u64,
    executor: u16,
    executors: u16,
    segment: SharedSegment<D::File>,
    round: Arc<AtomicU16>,
    config: CheckpointConfig,
    /// The next cycle's number: also how many files this executor has
    /// opened, abandoned ones included.
    cycle: u32,
    /// Cycles that reached a durable footer.
    completed: u32,
    /// The last durable snapshot's size.
    last_snapshot: u64,
    /// Whether this executor has counted toward the generation's round.
    rounded: bool,
    /// The planted defect: delete before the footer is durable.
    deletes_before_durable: bool,
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
    /// Encoded entries not yet written; kept across a failed write.
    buffer: Vec<u8>,
    scratch: Vec<u8>,
    /// Bytes written to the file so far, header included.
    bytes: u64,
    ticks: u64,
    /// The footer has been written to the file and awaits its sync.
    footer_written: bool,
    /// The file is synced and the directory sync is what remains.
    dir_pending: bool,
}

impl<F> Cycle<F> {
    fn fresh(number: u32, bases: Vec<u64>) -> Self {
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
            executors: spec.executors,
            segment: spec.segment,
            round: spec.round,
            config: spec.config,
            cycle: 0,
            completed: 0,
            last_snapshot: 0,
            rounded: false,
            deletes_before_durable: false,
            open: None,
        }
    }

    /// The planted defect: compaction runs at the rotation, when the cycle
    /// opens, instead of when its snapshot is durable. What a node that
    /// took "the old log is redundant" to mean the rotation rather than the
    /// synced footer would do — a crash anywhere in the cycle then finds
    /// neither the old segments nor an image. This server never does it;
    /// it exists so the simulator can plant exactly that and show it
    /// caught.
    pub const fn deletes_before_durable(&mut self, plant: bool) {
        self.deletes_before_durable = plant;
    }

    /// Cycles that reached a durable footer.
    #[must_use]
    pub const fn cycles_completed(&self) -> u32 {
        self.completed
    }

    /// Bytes of live log at which the next cycle opens.
    fn threshold(&self) -> u64 {
        self.config
            .floor
            .max(self.config.ratio.saturating_mul(self.last_snapshot))
    }

    /// Rotates the segment, takes the bases, and opens the cycle. Nothing
    /// is written to the snapshot file yet; `step` creates it.
    fn open_cycle<L: ReplicationLog, T: TraceSink>(
        &mut self,
        first_shard: u16,
        states: &[ShardState<L>],
        trace: &T,
    ) -> io::Result<()> {
        rotate_segment(
            &self.disk,
            &self.wal,
            self.generation,
            self.executor,
            &self.segment,
        )?;
        let bases = states.iter().map(|state| state.seq).collect();
        self.open = Some(Cycle::fresh(self.cycle, bases));
        self.cycle += 1;
        if self.deletes_before_durable {
            // The plant: the rotation taken for the durable point.
            self.compact(first_shard, self.cycle - 1, trace);
        }
        Ok(())
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

    /// Writes whatever the buffer holds; on failure the buffer is kept.
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
        self.ensure_file(first_shard, cycle)?;
        // A kept buffer goes first, and nothing is scanned behind it: the
        // budget was spent on it already.
        if !cycle.buffer.is_empty() {
            Self::drain(cycle)?;
            return Ok(false);
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
        Self::drain(cycle)?;
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
            let restarted = Cycle::fresh(self.cycle, std::mem::take(&mut cycle.bases));
            self.cycle += 1;
            *cycle = restarted;
            return Err(error);
        }
        cycle.dir_pending = true;
        self.disk.sync_dir(&self.wal)?;
        cycle.dir_pending = false;
        Ok(true)
    }
}

impl<D: Disk + Send + 'static> SegmentCheckpoint<D> {
    /// The snapshot is durable: cover the shards, report, compact, count.
    fn finish<L: ReplicationLog, T: TraceSink>(
        &mut self,
        first_shard: u16,
        states: &mut [ShardState<L>],
        trace: &T,
    ) {
        let cycle = self.open.take().expect("finish runs on an open cycle");
        for (state, base) in states.iter_mut().zip(&cycle.bases) {
            if let Some(through) = base.checked_sub(1) {
                state.log.covered(through);
            }
        }
        segment_snapshot_covered(&self.segment);
        self.last_snapshot = cycle.bytes;
        self.completed += 1;
        trace.snapshot(&SnapshotReport {
            executor: self.executor,
            cycle: cycle.number,
            entries: cycle.counts.iter().sum(),
            bytes: cycle.bytes,
            ticks: cycle.ticks,
            disk_bytes: self.disk_bytes(),
            written_during: live_log_bytes(&self.segment),
        });
        self.compact(first_shard, cycle.number, trace);
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

    /// Removes what the snapshot of `cycle` made redundant: this executor's
    /// older rotations and older snapshots of this generation; and, when
    /// this executor's first completed cycle brings the round to every
    /// executor, every older generation's files.
    fn compact<T: TraceSink>(&mut self, first_shard: u16, cycle: u32, trace: &T) {
        let rotation = lock(&self.segment).rotation;
        let older_generations = !self.rounded && {
            self.rounded = true;
            self.round.fetch_add(1, Ordering::SeqCst) + 1 == self.executors
        };
        let mut removed = CompactionReport {
            executor: self.executor,
            files: 0,
            bytes: 0,
        };
        for name in self.disk.list(&self.wal).unwrap_or_default() {
            if !self.is_redundant(&name, rotation, cycle, older_generations) {
                continue;
            }
            let path = self.wal.join(&name);
            let bytes = self.disk.len(&path).unwrap_or(0);
            match self.disk.remove_file(&path) {
                Ok(()) => {
                    removed.files += 1;
                    removed.bytes += bytes;
                }
                Err(error) => trace.fault(first_shard, LogFault::Remove, &error),
            }
        }
        if let Err(error) = self.disk.sync_dir(&self.wal) {
            trace.fault(first_shard, LogFault::Remove, &error);
        }
        trace.compaction(&removed);
    }

    /// Whether `name` is a file the snapshot of `cycle` made redundant. The
    /// older-generation arm comes first, so a file of an older generation is
    /// judged by the round and never by this executor's counters.
    fn is_redundant(&self, name: &str, rotation: u32, cycle: u32, older_generations: bool) -> bool {
        match (parse_segment_name(name), parse_snapshot_name(name)) {
            (Some((generation, _, _)), _) | (_, Some((generation, _, _)))
                if generation < self.generation =>
            {
                older_generations
            }
            (Some((generation, executor, this)), _) => {
                generation == self.generation && executor == self.executor && this < rotation
            }
            (_, Some((generation, executor, this))) => {
                generation == self.generation && executor == self.executor && this < cycle
            }
            (None, None) => false,
        }
    }
}

impl<D: Disk + Send + 'static> Checkpoint for SegmentCheckpoint<D> {
    fn tick<L: ReplicationLog, T: TraceSink>(
        &mut self,
        first_shard: u16,
        states: &mut [ShardState<L>],
        now: Now,
        trace: &T,
    ) {
        if self.open.is_none() {
            if live_log_bytes(&self.segment) < self.threshold() {
                return;
            }
            if let Err(error) = self.open_cycle(first_shard, states, trace) {
                trace.fault(first_shard, LogFault::Snapshot, &error);
                return;
            }
        }
        match self.step(first_shard, states, now) {
            Ok(false) => {}
            Ok(true) => self.finish(first_shard, states, trace),
            Err(error) => trace.fault(first_shard, LogFault::Snapshot, &error),
        }
    }
}

#[cfg(test)]
mod tests;
