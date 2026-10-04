//! The node's log writer: one task that owns the segment, the sync and the
//! directory's garbage, fed by every executor through a channel.
//!
//! **Why one.** The device charges per write request, and a sync of an
//! appended file costs a few of them whatever it carries. Ten files synced
//! in a round are at least ten data requests, one file is one: so the node
//! writes one file, and every executor hands it its batches instead of
//! writing its own. The executor's hot path loses a syscall and a lock; what
//! it gains is a message, the same passing the rest of the node is built on.
//!
//! **The round.** The writer drains everything the executors have sent and
//! writes it with one `write_all`, the shards interleaved in arrival order.
//! When a sync is due — something written, none in flight, the policy's
//! interval elapsed since the last issue — it freezes, per executor, the
//! last batch written, and issues the sync on the blocking pool. What
//! arrives while it is in flight is written and waits for the next round.
//! On completion each executor is told `Durable` through its frozen batch:
//! a sync covers what was written when it was issued, nothing after.
//!
//! **The budget.** An executor counts the bytes it has handed over and not
//! yet seen written, and stops reading its inbox above [`WRITER_BUDGET`].
//! The writer reports `Written` every [`WRITTEN_GRAIN`] of an executor's
//! bytes, so an executor's budget is refilled without a message per batch.
//!
//! **What the layout owes it.** Nothing on disk says who wrote a segment:
//! a shard's records are in sequence order in the file whichever executor
//! sent them, and recovery buckets by shard. The writer rotates at
//! [`SEGMENT_BYTES`], and it is the only thing that removes files.

use std::collections::VecDeque;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::log::checkpoint::CheckpointConfig;
use crate::log::disk::{Disk, LogFile, SyncFuture};
use crate::log::effect::Effect;
use crate::log::file::{Segment, create_segment, parse_segment_name};
use crate::log::recovery::RecoveredShard;
use crate::log::snapshot::parse_snapshot_name;
use crate::log::{Record, encode_record};
use crate::shard::{HOUSEKEEPING_TICK, LogFault, SyncPolicy, TraceSink};

#[cfg(test)]
mod tests;

/// Bytes at which the node's segment rotates.
pub const SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Bytes one executor may have handed the writer and not yet seen written
/// before it stops reading its inbox.
pub const WRITER_BUDGET: u64 = 4 * 1024 * 1024;

/// How many of one executor's bytes the writer writes before it reports
/// `Written` to it.
pub const WRITTEN_GRAIN: u64 = WRITER_BUDGET / 4;

/// What an executor sends the writer.
#[derive(Debug)]
pub enum ToWriter {
    /// A batch's bytes: every record the batch appended, in order.
    Submit {
        executor: u16,
        batch: u64,
        bytes: Vec<u8>,
    },
    /// The executor's snapshot of `cycle` is durable: every record it sent
    /// in batches up to `through_batch` is covered by the image.
    Covered {
        executor: u16,
        cycle: u32,
        through_batch: Option<u64>,
        snapshot_bytes: u64,
    },
    /// The executor has sent its last batch and waits for `Stopped`.
    Stop { executor: u16 },
}

/// What the writer sends an executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// `bytes` of this executor's submissions are in the file, cumulative.
    Written { bytes: u64 },
    /// A sync covering this executor's batches through `through_batch`
    /// completed, as round `round`; `bytes` as in `Written`.
    Durable {
        through_batch: Option<u64>,
        bytes: u64,
        round: u64,
    },
    /// The node's log failed: refuse writes until a snapshot is durable.
    Fault,
    /// Open a snapshot cycle at the next tick whatever the executor's own
    /// bytes say: the retained log on its account passed the bound.
    Nudge,
    /// The last sync is settled; the executor may end.
    Stopped,
}

/// One executor's ends of its link to the writer.
#[derive(Debug)]
pub struct WriterLink {
    pub executor: u16,
    pub to_writer: mpsc::UnboundedSender<ToWriter>,
    pub progress: mpsc::UnboundedReceiver<Progress>,
}

/// The defects the simulator plants in the writer. All `false` in
/// production.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct WriterPlants {
    /// `Durable` sent at the issue, not at the completion.
    pub durable_on_issue: bool,
    /// `Durable` names the last batch written when the sync completes,
    /// not the one frozen at its issue.
    pub durable_from_written_now: bool,
    /// A rotation is removed once *any* executor covered its batches in
    /// it, not once every one has.
    pub removes_uncovered: bool,
}

/// What [`Writer::open`] needs.
pub struct WriterSpec<D: Disk, T: TraceSink> {
    pub disk: D,
    pub wal: PathBuf,
    pub generation: u64,
    pub executors: u16,
    pub policy: SyncPolicy,
    /// Bytes at which the segment rotates: [`SEGMENT_BYTES`] in the binary.
    pub segment_bytes: u64,
    /// The floor and ratio the nudge measures the retained log against.
    pub checkpoint: CheckpointConfig,
    pub trace: T,
    pub plants: WriterPlants,
}

/// A writer, its inbox, and one link per executor.
pub struct Opened<D: Disk, T: TraceSink> {
    pub writer: Writer<D, T>,
    pub inbox: mpsc::UnboundedReceiver<ToWriter>,
    pub links: Vec<WriterLink>,
}

/// One executor, as the writer sees it.
struct Lane {
    /// The last batch received.
    received: Option<u64>,
    /// The last batch written to the file.
    written: Option<u64>,
    /// Bytes received and not yet written.
    pending_bytes: u64,
    /// Bytes written, cumulative.
    written_bytes: u64,
    /// What the last `Written` or `Durable` told the executor.
    reported_bytes: u64,
    /// What its last durable snapshot covers.
    covered: Option<u64>,
    /// Whether a snapshot has been reported this generation.
    reported: bool,
    /// That snapshot's size.
    snapshot_bytes: u64,
    /// A `Nudge` was sent and no `Covered` has answered it yet.
    nudged: bool,
    stopped: bool,
    progress: mpsc::UnboundedSender<Progress>,
}

impl Lane {
    const fn new(progress: mpsc::UnboundedSender<Progress>) -> Self {
        Self {
            received: None,
            written: None,
            pending_bytes: 0,
            written_bytes: 0,
            reported_bytes: 0,
            covered: None,
            reported: false,
            snapshot_bytes: 0,
            nudged: false,
            stopped: false,
            progress,
        }
    }
}

/// A rotation that was closed: what each executor last wrote into it, and
/// its size. The coverage table.
struct Closed {
    rotation: u32,
    last_batch: Vec<Option<u64>>,
    bytes: u64,
}

/// The node's log writer. See the module documentation.
pub struct Writer<D: Disk, T: TraceSink> {
    disk: D,
    wal: PathBuf,
    generation: u64,
    policy: SyncPolicy,
    segment_bytes: u64,
    checkpoint: CheckpointConfig,
    trace: T,
    plants: WriterPlants,
    segment: Segment<D::File>,
    lanes: Vec<Lane>,
    /// What this drain received and has not written.
    staged: Vec<u8>,
    /// The round in flight: per executor the last batch written at its
    /// issue, and the sync.
    in_flight: Option<(Vec<Option<u64>>, SyncFuture)>,
    issued_at: Instant,
    round: u64,
    closed: VecDeque<Closed>,
    /// Bytes of older generations' files at the open — retained log whose
    /// holders are the executors without a snapshot in this generation.
    older_bytes: u64,
}

impl<D: Disk + Send + 'static, T: TraceSink> Writer<D, T> {
    /// Creates the generation's first segment, header synced and the
    /// directory synced, and the links of `spec.executors` executors.
    ///
    /// # Errors
    ///
    /// Whatever the disk reports.
    pub fn open(spec: WriterSpec<D, T>) -> io::Result<Opened<D, T>> {
        let file = create_segment(&spec.disk, &spec.wal, spec.generation, 0)?;
        spec.disk.sync_dir(&spec.wal)?;
        let older_bytes = spec
            .disk
            .list(&spec.wal)?
            .iter()
            .filter(|name| {
                parse_segment_name(name)
                    .map(|(generation, _)| generation)
                    .or_else(|| parse_snapshot_name(name).map(|(generation, _, _)| generation))
                    .is_some_and(|generation| generation < spec.generation)
            })
            .map(|name| spec.disk.len(&spec.wal.join(name)).unwrap_or(0))
            .sum();
        let (to_writer, inbox) = mpsc::unbounded_channel();
        let (lanes, links) = (0..spec.executors)
            .map(|executor| {
                let (tx, rx) = mpsc::unbounded_channel();
                (
                    Lane::new(tx),
                    WriterLink {
                        executor,
                        to_writer: to_writer.clone(),
                        progress: rx,
                    },
                )
            })
            .unzip();
        let interval = spec.policy.min_interval.unwrap_or(Duration::ZERO);
        let now = Instant::now();
        let writer = Self {
            disk: spec.disk,
            wal: spec.wal,
            generation: spec.generation,
            policy: spec.policy,
            segment_bytes: spec.segment_bytes,
            checkpoint: spec.checkpoint,
            trace: spec.trace,
            plants: spec.plants,
            segment: Segment {
                file,
                rotation: 0,
                attempted: 0,
                bytes_written: 0,
                dirty: false,
                sync_failed: false,
            },
            lanes,
            staged: Vec::new(),
            in_flight: None,
            // One interval before now, so the first write is synced at once.
            issued_at: now.checked_sub(interval).unwrap_or(now),
            round: 0,
            closed: VecDeque::new(),
            older_bytes,
        };
        Ok(Opened {
            writer,
            inbox,
            links,
        })
    }

    /// The start path's: appends `bytes` now, on this thread.
    ///
    /// # Errors
    ///
    /// Whatever the disk reports.
    pub fn append_now(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.segment.file.write_all(bytes)?;
        self.segment.bytes_written += bytes.len() as u64;
        self.segment.dirty = true;
        Ok(())
    }

    /// The start path's: syncs now, on this thread. A failure marks the
    /// segment failed; the first submission rotates away from it.
    ///
    /// # Errors
    ///
    /// Whatever the disk reports.
    pub fn sync_now(&mut self) -> io::Result<()> {
        if !self.segment.dirty {
            return Ok(());
        }
        if let Err(error) = self.segment.file.sync_data() {
            self.segment.sync_failed = true;
            return Err(error);
        }
        self.segment.dirty = false;
        Ok(())
    }

    /// The writer's loop, until every executor has stopped or every link
    /// is gone.
    pub async fn run(mut self, mut inbox: mpsc::UnboundedReceiver<ToWriter>) {
        let mut tick = tokio::time::interval(HOUSEKEEPING_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        self.nudge_if_due();
        loop {
            tokio::select! {
                // `biased`, for the reason `run_executor` gives at length: the
                // runtime's RNG must not pick an arm. The completion first —
                // it releases what executors hold; then the inbox; then the
                // interval's timer.
                biased;

                result = in_flight(&mut self.in_flight), if self.in_flight.is_some() => {
                    self.sync_done(result);
                    self.maybe_issue(Instant::now());
                }
                message = inbox.recv() => {
                    // Every link dropped is the pool gone: nothing more will
                    // arrive, and nobody waits for a `Stopped`.
                    let Some(message) = message else { break };
                    self.take(message);
                    while let Ok(message) = inbox.try_recv() {
                        self.take(message);
                    }
                    self.write_staged().await;
                    self.maybe_issue(Instant::now());
                    if self.lanes.iter().all(|lane| lane.stopped) {
                        self.last_round().await;
                        break;
                    }
                }
                _ = tick.tick() => self.maybe_issue(Instant::now()),
            }
        }
    }

    fn take(&mut self, message: ToWriter) {
        match message {
            ToWriter::Submit {
                executor,
                batch,
                bytes,
            } => {
                let lane = &mut self.lanes[usize::from(executor)];
                lane.received = Some(batch);
                lane.pending_bytes += bytes.len() as u64;
                self.staged.extend_from_slice(&bytes);
            }
            ToWriter::Covered {
                executor,
                cycle,
                through_batch,
                snapshot_bytes,
            } => {
                self.covered(executor, cycle, through_batch, snapshot_bytes);
            }
            ToWriter::Stop { executor } => self.lanes[usize::from(executor)].stopped = true,
        }
    }

    /// One write for the whole drain. A segment that failed, or that is
    /// full, is rotated away from first.
    async fn write_staged(&mut self) {
        if self.staged.is_empty() {
            return;
        }
        if (self.segment.sync_failed || self.segment.bytes_written >= self.segment_bytes)
            && !self.rotate().await
        {
            return;
        }
        if let Err(error) = self.segment.file.write_all(&self.staged) {
            self.fail(LogFault::Write, &error);
            return;
        }
        let len = self.staged.len() as u64;
        self.staged.clear();
        self.segment.bytes_written += len;
        self.segment.dirty = true;
        for lane in &mut self.lanes {
            lane.written = lane.received;
            lane.written_bytes += std::mem::take(&mut lane.pending_bytes);
            if lane.written_bytes - lane.reported_bytes >= WRITTEN_GRAIN {
                lane.reported_bytes = lane.written_bytes;
                let _ = lane.progress.send(Progress::Written {
                    bytes: lane.written_bytes,
                });
            }
        }
    }

    fn maybe_issue(&mut self, now: Instant) {
        let Some(interval) = self.policy.min_interval else {
            return;
        };
        if !self.segment.dirty
            || self.segment.sync_failed
            || self.in_flight.is_some()
            || now.saturating_duration_since(self.issued_at) < interval
        {
            return;
        }
        self.segment.dirty = false;
        self.round += 1;
        let frozen: Vec<Option<u64>> = self.lanes.iter().map(|lane| lane.written).collect();
        self.trace.sync_issued(self.round);
        self.in_flight = Some((frozen.clone(), self.segment.file.sync_later()));
        self.issued_at = now;
        if self.plants.durable_on_issue {
            // The plant: answered at the issue, before anything is on disk.
            self.report_durable(&frozen);
        }
    }

    fn sync_done(&mut self, result: io::Result<()>) {
        let (frozen, _) = self
            .in_flight
            .take()
            .expect("a completion has a round in flight");
        self.trace.sync_settled(self.round);
        match result {
            Ok(()) => {
                let covered = if self.plants.durable_from_written_now {
                    // The plant: what was written during the flight, claimed.
                    self.lanes.iter().map(|lane| lane.written).collect()
                } else {
                    frozen
                };
                self.report_durable(&covered);
            }
            Err(error) => self.fail(LogFault::Sync, &error),
        }
    }

    fn report_durable(&mut self, covered: &[Option<u64>]) {
        let round = self.round;
        for (lane, through_batch) in self.lanes.iter_mut().zip(covered) {
            lane.reported_bytes = lane.written_bytes;
            let _ = lane.progress.send(Progress::Durable {
                through_batch: *through_batch,
                bytes: lane.written_bytes,
                round,
            });
        }
    }

    // Stub: the onset of the node's refusal, whole.
    fn fail(&mut self, fault: LogFault, error: &io::Error) {
        self.trace.log_fault(fault, error);
        self.segment.sync_failed = true;
        self.in_flight = None;
        for lane in &self.lanes {
            let _ = lane.progress.send(Progress::Fault);
        }
    }

    // Stub: the rotation.
    #[allow(clippy::unused_async)]
    async fn rotate(&mut self) -> bool {
        false
    }

    // Stub: the last round's sync before `Stopped`.
    #[allow(clippy::unused_async)]
    async fn last_round(&mut self) {
        for lane in &self.lanes {
            let _ = lane.progress.send(Progress::Stopped);
        }
    }

    // Stub: coverage and compaction.
    #[allow(clippy::unused_self, clippy::needless_pass_by_ref_mut)]
    const fn covered(
        &mut self,
        _executor: u16,
        _cycle: u32,
        _through_batch: Option<u64>,
        _snapshot_bytes: u64,
    ) {
    }

    // Stub: the nudge.
    #[allow(clippy::unused_self)]
    const fn nudge_if_due(&self) {}
}

/// The sync in flight, or a future that never completes when there is none
/// — so the `select!` arm can be written once.
async fn in_flight(slot: &mut Option<(Vec<Option<u64>>, SyncFuture)>) -> io::Result<()> {
    match slot {
        Some((_, sync)) => sync.as_mut().await,
        None => std::future::pending().await,
    }
}

/// Writes a `Rebase` for every cut shard at its resume position and syncs
/// them, advancing each cut shard's `seq` past its record. Before the pool
/// serves; a failure is the node's log failing at the start.
///
/// # Errors
///
/// Whatever the disk reports for the write or the sync.
pub fn write_rebases<D: Disk + Send + 'static, T: TraceSink>(
    writer: &mut Writer<D, T>,
    shards: &mut [RecoveredShard],
) -> io::Result<()> {
    let mut bytes = Vec::new();
    let mut payload = Vec::new();
    Effect::Rebase.encode(&mut payload);
    for (shard, state) in shards.iter_mut().enumerate() {
        if !state.cut {
            continue;
        }
        let shard = u16::try_from(shard).expect("a node's shard count fits a u16");
        encode_record(
            &Record {
                shard,
                seq: state.seq,
                payload: &payload,
            },
            &mut bytes,
        );
        state.seq += 1;
    }
    if bytes.is_empty() {
        return Ok(());
    }
    writer.append_now(&bytes)?;
    writer.sync_now()
}
