//! One executor's state for the shards it owns, and the housekeeping it runs
//! between batches: the sweep, the eviction, the counters `INFO` reads. The
//! tick and the per-tick budgets are declared here with the measurements that
//! set them.

use crate::dict::{Dict, Entry};
use crate::log::ReplicationLog;
use crate::log::checkpoint::{Checkpoint, LogPosition};
use crate::log::effect::{Effect, Owned};
use crate::log::writer::{Progress, ToWriter, WriterLink};
use crate::memory::{EvictionMode, MemoryGauge, MemoryLimit};
use crate::shard::apply::{append, apply};
use crate::shard::durability::{Held, Mode, Sent, SyncState, send};
use crate::shard::{
    Command, Envelope, EvictionPolicy, ExecutorPlants, ExpiryPolicy, KIND_SLOTS, PersistenceStats,
    RefusalReport, Reply, ReplyError, ReplyTo, Route, ShardPolicy, ShardStats, SyncPolicy,
    TraceSink,
};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

/// How often a shard does the work no command asked it for: advancing an
/// in-flight rehash, sweeping expired keys, and syncing its log.
///
/// Public because it is a fact about the server a test harness has to know
/// rather than assume: the granularity at which a deadline that nothing
/// touches again is actually reclaimed is this number, and the simulator's
/// expiration invariants are derived from it. Read from here, a change to
/// the cadence reaches everything that depends on it; copied, it would not.
pub const HOUSEKEEPING_TICK: Duration = Duration::from_millis(100);

/// Buckets migrated per rehash tick.
///
/// Writes already migrate as they go; the tick exists so a table that stopped
/// receiving writes mid-rehash still finishes, rather than sitting split
/// across two tables forever.
///
/// The number has to answer that "forever" and nothing else, because only
/// writes advance a rehash: [`Dict::get`] deliberately does not, and a `Del`
/// that removed nothing returns before reaching `remove`. So a shard that
/// grows and then goes read-only is draining at exactly this rate. At four
/// buckets per tick — forty a second — a table grown to 65 536 buckets stays
/// split for twenty-seven minutes, holding two tables and probing both on
/// every miss. At 1024 the same table drains in six seconds, and the tick's
/// own cost stays in the same class as one large command, which is what the
/// no-await rule actually constrains.
const REHASH_BUCKETS_PER_TICK: usize = 1024;

/// Buckets swept for expired keys per housekeeping tick.
///
/// This is the whole of the active half of expiration: a key nothing touches
/// again is reclaimed only when the cursor reaches its bucket, so the budget
/// sets how long a dead entry can hold its memory. A table of N buckets is
/// walked in `N / 256` ticks, and [`HOUSEKEEPING_TICK`] is ten a second — so
/// 2048 buckets are a full cycle in eight ticks, under a second, and a
/// million-bucket table takes 3906 ticks, six and a half minutes.
///
/// Smaller than [`REHASH_BUCKETS_PER_TICK`], and deliberately: a rehash is a
/// state the dict has to leave, paying two lookups on every miss until it
/// does, while a sweep is a standing cost every tick of the process's life. It
/// is also the more expensive walk per bucket — it reads every entry's
/// deadline rather than moving whole chains — and it runs against every owned
/// dict that could hold a deadline, where most rehashes are over.
///
/// **The budget bounds the walk, not the tick.** What this number caps is the
/// cursor steps [`Dict::expire_step`] takes; the removals that follow are
/// extra, and each costs more than a bucket of walking. Every reported key is
/// hashed a second time — the walk returns key bytes, so [`Dict::remove`]
/// hashes each one again — and while a rehash is in flight each of those
/// `remove` calls also advances it by a bucket. A tick that sweeps buckets
/// full of due deadlines therefore costs meaningfully more than this constant
/// alone suggests, and its worst case scales with how many swept entries are
/// due rather than with the budget.
const EXPIRE_BUCKETS_PER_TICK: usize = 256;

/// How many entries one eviction looks at before choosing the one it removes.
///
/// Redis's `maxmemory-samples` default, and the same trade: five is enough
/// that the key removed is nearly always among the older ones, and few enough
/// that making room costs a handful of comparisons rather than a walk of the
/// keyspace. Approximate LRU is the deliberate part — an exact one wants a
/// list threaded through every entry, which is per-key memory spent to
/// improve a hit rate that sampling already gets most of.
///
/// What five buys is measured rather than asserted:
/// `sampled_eviction_rarely_takes_a_recently_touched_key` in `dict.rs` holds
/// the sample to a bound on how often it takes a recently used key.
pub const EVICTION_SAMPLES: usize = 5;

/// The two clocks a command is served at: the monotonic instant every
/// deadline is kept in, and the wall clock a record has to carry.
///
/// One reading of each per envelope, taken by the executor, so the commands
/// of a batch agree about what time it is. The wall clock is injected rather
/// than read — the simulator's stands where the simulation says it does —
/// and reaches the shard only so that a logged deadline is absolute: a
/// record that said `EX 30` would replay to a different state than the one
/// it described.
#[derive(Debug, Clone, Copy)]
pub struct Now {
    /// The monotonic instant, from the clock the runtime controls.
    pub instant: Instant,
    /// Unix milliseconds, from the injected wall clock.
    pub unix_millis: u64,
}

/// What a logged deadline turns out to mean when it is replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replayed {
    /// The deadline is not in the future: the key is dead.
    Past,
    /// The key lives until this instant, or has no deadline the clock can
    /// represent — which the dict stores as no deadline, the same answer
    /// the command path gives a span it cannot add.
    At(Option<Instant>),
}

impl Now {
    /// A reading with the wall clock at zero, for callers that have no wall
    /// clock to offer: the core's own tests.
    #[must_use]
    pub const fn at(instant: Instant) -> Self {
        Self {
            instant,
            unix_millis: 0,
        }
    }

    /// The Unix milliseconds `at` corresponds to, for a record.
    ///
    /// Saturating in both steps: a deadline further off than `u64` can
    /// count is a deadline nothing will ever reach, and a wall clock that
    /// overflows has other problems.
    #[must_use]
    pub fn deadline_millis(self, at: Option<Instant>) -> Option<u64> {
        let left = at?.saturating_duration_since(self.instant).as_millis();
        Some(
            self.unix_millis
                .saturating_add(u64::try_from(left).unwrap_or(u64::MAX)),
        )
    }

    /// What a record's absolute deadline means now.
    #[must_use]
    pub fn replay_deadline(self, millis: u64) -> Replayed {
        match millis.checked_sub(self.unix_millis) {
            None | Some(0) => Replayed::Past,
            Some(left) => Replayed::At(self.instant.checked_add(Duration::from_millis(left))),
        }
    }
}

/// The wall clock of a node that has none to offer: it reads zero.
///
/// For the pool constructors that predate the clock and for tests. A node
/// whose records carry deadlines measured from zero replays them correctly
/// only against a clock that also reads zero — which is what makes it a
/// test's clock and not a default.
#[must_use]
pub const fn frozen_clock() -> u64 {
    0
}

/// What an executor needs to know about the node's memory: the figure, and
/// what to do when it is too large.
///
/// One argument rather than two because [`run_executor`] and
/// [`spawn_executor`] were already at the parameter count where the next one
/// is a list nobody reads. They also always travel together: a gauge with no
/// ceiling to compare against decides nothing, and a ceiling with no gauge
/// has nothing to compare.
#[derive(Clone, Debug, Default)]
pub struct Memory {
    /// The node-wide figure every executor keeps current.
    pub gauge: MemoryGauge,
    /// The ceiling that figure is held under, and how.
    pub limit: MemoryLimit,
}

/// One virtual shard's state: what used to be one task's locals.
pub struct ShardState<L> {
    pub dict: Dict,
    pub seq: u64,
    pub log: L,
    /// Where the active expiry sweep resumes on the next housekeeping tick.
    ///
    /// A [`Dict::scan`] cursor, and it belongs to the shard rather than to the
    /// dict for the same reason a `SCAN` command's does: the dict offers a
    /// position, and what keeps a position between calls is whoever is walking.
    pub expire_cursor: u64,
    /// Where the eviction sample resumes, kept for the reason the sweep keeps
    /// its own: successive samples that all started at `0` would re-read one
    /// corner of the table and offer the same few keys up every time.
    ///
    /// Separate from [`expire_cursor`](Self::expire_cursor) on purpose. The
    /// two walks have nothing to do with each other, and sharing one position
    /// would make how often a key is offered for eviction depend on how many
    /// deadlines the keyspace happens to carry.
    pub evict_cursor: u64,
    /// How many entries this shard has evicted, for `INFO`'s `evicted_keys`.
    pub evicted: u64,
    /// Keyed lookups that found a live entry, for `INFO`'s `keyspace_hits`.
    pub hits: u64,
    /// Keyed lookups that found nothing, for `INFO`'s `keyspace_misses`.
    pub misses: u64,
    /// Entries reclaimed because their deadline had passed, by either half of
    /// expiration, for `INFO`'s `expired_keys`.
    pub expired: u64,
    /// How many commands of each [`Command::kind`] this shard has run, indexed
    /// by the tag itself; slot `0` is no command and stays zero.
    ///
    /// Plain `u64`s rather than atomics because one executor task owns this
    /// shard and nothing else may touch it — the counters a scrape reads are
    /// gathered by a [`Command::Stats`] like any other command, which is what
    /// keeps `INFO` off the hot path entirely.
    pub calls: [u64; KIND_SLOTS],
    /// Microseconds those commands spent, indexed the same way — see
    /// [`ShardStats::usec`], which is what this becomes.
    pub usec: [u64; KIND_SLOTS],
    /// The start reported this shard lossy and no durable image has covered
    /// it since.
    pub lossy: bool,
    /// When this shard's newest durable image was taken, Unix milliseconds;
    /// `None` until it has one.
    pub image_unix_millis: Option<u64>,
    /// The executor owning this shard is refusing writes: what lets a read
    /// that expires its key remove it without a record for the failed log.
    pub refusing: bool,
}

impl<L> ShardState<L> {
    /// A shard's state at the moment it starts: an empty keyspace, a log at
    /// position zero, and every counter unspent.
    pub const fn new(dict: Dict, log: L) -> Self {
        Self::recovered(dict, 0, log)
    }

    /// A shard's state as recovery rebuilt it: the dict holds the image
    /// plus the tail, and `seq` is the position the shard resumes at.
    pub const fn recovered(dict: Dict, seq: u64, log: L) -> Self {
        Self {
            dict,
            seq,
            log,
            expire_cursor: 0,
            evict_cursor: 0,
            evicted: 0,
            hits: 0,
            misses: 0,
            expired: 0,
            calls: [0; KIND_SLOTS],
            usec: [0; KIND_SLOTS],
            lossy: false,
            image_unix_millis: None,
            refusing: false,
        }
    }

    /// Applies a recovered prefix to this shard's dict — see [`replay_into`].
    pub fn replay(&mut self, records: Vec<(u64, Owned)>, now: Now) {
        replay_into(&mut self.dict, &mut self.seq, records, now, Vec::new());
    }
}

/// Applies a recovered prefix to `dict`, record by record, as the node
/// applied it, and advances `seq` past the last record.
///
/// A deadline that has passed by now does not remove its key on the
/// spot: a later record in the prefix may have moved it — an `EXPIRE`
/// that reached the key before it died, say — and the node served the
/// key under that later deadline. So a passed deadline is held as due
/// *now*, and only the keys whose last word is still a passed deadline
/// are removed once the whole prefix is in. `due` seeds that list with
/// keys the caller inserted before the prefix — recovery's image entries
/// whose deadline had passed — so the same rule covers them.
pub fn replay_into(
    dict: &mut Dict,
    seq: &mut u64,
    records: Vec<(u64, Owned)>,
    now: Now,
    mut due: Vec<Bytes>,
) {
    let resolve = |millis: Option<u64>| match millis.map(|at| now.replay_deadline(at)) {
        None => (None, false),
        Some(Replayed::At(at)) => (at, false),
        Some(Replayed::Past) => (Some(now.instant), true),
    };
    for (at, effect) in records {
        debug_assert_eq!(at, *seq, "recovery hands over a gapless prefix");
        match effect {
            Owned::Put {
                key,
                value,
                deadline,
            } => {
                let (expires_at, passed) = resolve(deadline);
                if passed {
                    due.push(key.clone());
                }
                dict.insert(
                    key,
                    Entry {
                        value,
                        expires_at,
                        touched: 0,
                    },
                );
            }
            Owned::Del { key } => {
                dict.remove(&key);
            }
            Owned::Deadline { key, deadline } => {
                let (expires_at, passed) = resolve(deadline);
                if dict.set_deadline(&key, expires_at) && passed {
                    due.push(key);
                }
            }
            Owned::Flush => dict.clear(),
            // It changed no key; recovery has already used it.
            Owned::Rebase => {}
        }
        *seq = at + 1;
    }
    for key in due {
        let dead = dict
            .get(&key)
            .is_some_and(|entry| entry.expires_at.is_some_and(|at| at <= now.instant));
        if dead {
            dict.remove(&key);
        }
    }
}

/// What one executor task is built from.
///
/// A struct rather than a parameter list, for the reason [`PoolSpec`](crate::shard::PoolSpec)
/// is one: the list had reached the count where the next argument is one
/// nobody reads, and a caller that names fields cannot swap a clock for a
/// checkpoint by position.
pub struct ExecutorSpec<T, L, P, C> {
    pub first_shard: u16,
    pub states: Vec<ShardState<L>>,
    pub trace: T,
    pub policy: P,
    pub memory: Memory,
    pub clock: fn() -> u64,
    pub checkpoint: C,
    pub sync: SyncPolicy,
    pub plants: ExecutorPlants,
    /// Whether a write of the log already failed before the executor ran:
    /// it then starts refusing.
    pub log_failed: bool,
    /// The link to the node's writer; `None` on a node with no log.
    pub link: Option<WriterLink>,
    /// Turns `true` when the pool is shut down.
    pub stop: watch::Receiver<bool>,
    /// The node's persistence counters, and which cell is this executor's.
    pub stats: Arc<PersistenceStats>,
    pub executor: u16,
}

/// One executor task: own a contiguous range of shards, answer the inbox,
/// keep every owned rehash moving, and hand what it logs to the node's
/// writer.
///
/// `states` holds the range's shards in ascending order starting at
/// `first_shard`, so a command's shard id indexes it by subtraction.
///
/// Returns when the pool is shut down or the inbox closes, which happens
/// once the last [`ShardPool`] handle is dropped — after serving what was
/// queued and waiting for the writer's last sync, either way.
pub async fn run_executor<T: TraceSink, L: ReplicationLog, P: ShardPolicy, C: Checkpoint>(
    spec: ExecutorSpec<T, L, P, C>,
    mut inbox: mpsc::UnboundedReceiver<Envelope>,
) {
    let (mut this, mut stop) = Executor::new(spec);
    let mut tick = tokio::time::interval(HOUSEKEEPING_TICK);
    // A shard that fell behind resumes at its normal spacing instead of firing
    // a burst of catch-up ticks. The default is that burst, and it is the last
    // thing a shard that has just been saturated needs: it would meet the end
    // of a stall with every tick the stall cost, back to back, ahead of the
    // work that piled up behind them.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval` yields its first tick immediately; consume it so the first
    // real tick is one period away.
    tick.tick().await;

    loop {
        tokio::select! {
            // `biased` removes the runtime's RNG from this loop. Without it
            // `select!` picks among ready arms at random, seeded from OS
            // entropy — turmoil does seed tokio's runtime RNG, but only under
            // `--cfg tokio_unstable`, which this workspace does not set. That
            // is unseeded entropy inside the one crate whose whole premise is
            // that a seed reproduces a run, and `clippy.toml`'s entropy
            // prohibitions cannot see it: they match paths, not macro
            // internals.
            //
            // This used to be a precaution: the losing arm only advanced a
            // rehash, which reached no reply and no trace field, so an
            // unbiased choice was entropy nothing could observe. That stopped
            // being true with [`Command::ScanStep`]. A step's answer — both
            // its cursor and the keys it found — depends on whether a rehash
            // is in flight and how far it has run, and the simulator folds
            // both into the trace hash. So an unbiased arm choice would now
            // change a walk's answer between two runs of one seed, which is
            // the whole thing a seed is supposed to prevent. `biased` is
            // load-bearing here, not decorative; do not remove it.
            //
            // The order is the priority. The writer's word comes first: it
            // releases held replies and lowers the budget, and it is ready
            // at most a few times per round. Behind the inbox it would be
            // polled only when the inbox ran dry, so a busy executor would
            // hold its writes' replies for as long as the load lasted. The
            // stop comes next for the same reason, and is ready once. Then
            // the inbox: work the shard was asked for outranks housekeeping.
            biased;

            progress = next_progress(&mut this.sync.link), if this.sync.link.is_some() => {
                if let Some(message) = progress {
                    this.progress(message);
                } else {
                    // The writer is gone: nothing more will be proven, and
                    // the refusal this begins is never ended.
                    this.sync.link = None;
                    this.sync.writer_lost = true;
                    this.refuse();
                }
            }
            // A dropped sender is a dropped pool, which closes the inbox
            // too: stopping on either is the same stop. What is still
            // queued is served by the stop itself.
            _ = stop.changed() => break,
            // Above the budget the inbox waits for the writer's progress:
            // the stall `write` gave the executor, one step later.
            envelope = inbox.recv(), if !this.sync.over_budget() => {
                let Some(envelope) = envelope else {
                    break;
                };
                this.serve(envelope);
            }
            // One ticker per executor rather than one per shard, advancing
            // every owned dict by the same budget: the same per-dict drain
            // rate, and the same aggregate work, as independent tickers.
            _ = tick.tick() => this.housekeeping(),
        }
    }
    this.stop(&mut inbox).await;
}

/// The writer's next word, or a future that never completes when there is
/// no writer — so the `select!` arm can be written once.
async fn next_progress(link: &mut Option<WriterLink>) -> Option<Progress> {
    match link {
        Some(link) => link.progress.recv().await,
        None => std::future::pending().await,
    }
}

/// What the executor loop holds between arms: [`ExecutorSpec`]'s fields,
/// plus the link to the writer and the replies waiting on it.
struct Executor<T, L, P, C> {
    first_shard: u16,
    states: Vec<ShardState<L>>,
    trace: T,
    policy: P,
    memory: Memory,
    clock: fn() -> u64,
    checkpoint: C,
    sync: SyncState,
    stats: Arc<PersistenceStats>,
    /// This executor's index: which of `stats`'s cells is its own.
    index: u16,
    /// The cell's `changes` when the open cycle took its bases: what its
    /// image will cover. `None` while no cycle is open.
    changes_at_open: Option<u64>,
    /// A client asked for a snapshot since the last tick, and the next
    /// tick opens it: `BGSAVE` is already in progress from here.
    snapshot_asked: bool,
    /// `SAVE`s the next completed image answers: each arrived while no
    /// cycle was open, so the cycle that answers it took its bases after.
    saves: Vec<(ReplyTo, Vec<Reply>)>,
    /// `SAVE`s that arrived while a cycle was open. That cycle's bases were
    /// read before them, so a write acknowledged in between is not in its
    /// image: they wait for the cycle after it.
    saves_after: Vec<(ReplyTo, Vec<Reply>)>,
}

impl<T: TraceSink, L: ReplicationLog, P: ShardPolicy, C: Checkpoint> Executor<T, L, P, C> {
    fn new(spec: ExecutorSpec<T, L, P, C>) -> (Self, watch::Receiver<bool>) {
        let ExecutorSpec {
            first_shard,
            states,
            trace,
            policy,
            memory,
            clock,
            checkpoint,
            sync,
            plants,
            log_failed,
            link,
            stop,
            stats,
            executor,
        } = spec;
        let sync = SyncState::new(sync, plants, link);
        let mut this = Self {
            first_shard,
            states,
            trace,
            policy,
            memory,
            clock,
            checkpoint,
            sync,
            stats,
            index: executor,
            changes_at_open: None,
            snapshot_asked: false,
            saves: Vec::new(),
            saves_after: Vec::new(),
        };
        this.stats.executors[usize::from(executor)]
            .last_save_unix
            .store(newest_images_secs(&this.states), Ordering::Relaxed);
        if log_failed {
            this.refuse();
        }
        (this, stop)
    }

    /// The last thing an executor does: everything queued is served,
    /// everything appended is handed to the writer, and the writer's last
    /// sync is waited for; nothing is held. Under every policy — under
    /// `never`, that sync is the one the log ever gets.
    async fn stop(&mut self, inbox: &mut mpsc::UnboundedReceiver<Envelope>) {
        // Nothing new is accepted; what was already sent is answered.
        inbox.close();
        while let Ok(envelope) = inbox.try_recv() {
            self.serve(envelope);
        }
        let touched = self.stage_all();
        self.submit(touched);
        let Some(executor) = self.sync.link.as_ref().map(|link| link.executor) else {
            let released = self.sync.release_through(self.sync.batch);
            self.trace.held_answered(self.first_shard, released, false);
            return;
        };
        if let Some(link) = &self.sync.link {
            let _ = link.to_writer.send(ToWriter::Stop { executor });
        }
        while let Some(link) = self.sync.link.as_mut() {
            let next = link.progress.recv().await;
            match next {
                Some(Progress::Stopped) | None => break,
                Some(message) => self.progress(message),
            }
        }
        // `Durable` or `Fault` answered every held batch before `Stopped`;
        // anything still here is a batch no sync covered, and goes out as
        // the refusal rather than as a promise.
        let failed = self
            .sync
            .fail_all(&Reply::Error(ReplyError::LogWriteFailed));
        if failed > 0 {
            self.trace.held_answered(self.first_shard, failed, true);
        }
    }

    /// Hands every owned shard's buffer to the staging buffer; per shard
    /// that had anything, by offset, its flushed point.
    fn stage_all(&mut self) -> Vec<(usize, u64)> {
        let mut touched = Vec::new();
        for (offset, state) in self.states.iter_mut().enumerate() {
            let before = self.sync.staging.len();
            state.log.flush_into(&mut self.sync.staging);
            if self.sync.staging.len() > before
                && let Some(through) = state.log.flushed_through()
            {
                touched.push((offset, through));
            }
        }
        touched
    }

    /// Hands what the batch staged to the writer as one submission;
    /// whether anything was staged.
    fn submit(&mut self, shards: Vec<(usize, u64)>) -> bool {
        if self.sync.staging.is_empty() {
            return false;
        }
        let bytes = std::mem::take(&mut self.sync.staging);
        self.sync.sent_bytes += bytes.len() as u64;
        let batch = self.sync.batch;
        self.sync.batch += 1;
        match &self.sync.link {
            Some(link) => {
                let _ = link.to_writer.send(ToWriter::Submit {
                    executor: link.executor,
                    batch,
                    bytes,
                });
                self.sync.track(Sent { batch, shards });
            }
            // No writer: the bytes go nowhere, and nothing is outstanding.
            None => self.sync.acked_bytes = self.sync.sent_bytes,
        }
        true
    }

    /// The writer's word.
    ///
    /// `Durable` raises each shard's durable point to what it had flushed
    /// at the batches the sync covered — not to what it flushed after —
    /// and answers the batches it covered. A `Durable` that names batches
    /// already let go, after a `Fault`, finds nothing to raise or answer.
    fn progress(&mut self, message: Progress) {
        match message {
            Progress::Written { bytes } => {
                self.sync.acked_bytes = self.sync.acked_bytes.max(bytes);
            }
            Progress::Durable {
                through_batch,
                bytes,
                round,
            } => {
                self.sync.acked_bytes = self.sync.acked_bytes.max(bytes);
                let Some(through_batch) = through_batch else {
                    return;
                };
                while self
                    .sync
                    .sent
                    .front()
                    .is_some_and(|sent| sent.batch <= through_batch)
                {
                    let sent = self.sync.sent.pop_front().expect("checked above");
                    for (offset, seq) in sent.shards {
                        self.states[offset].log.sync_completed(Some(seq), round);
                    }
                }
                let released = self.sync.release_through(through_batch);
                if released > 0 {
                    self.trace.held_answered(self.first_shard, released, false);
                }
            }
            Progress::Fault => self.refuse(),
            Progress::Nudge => self.checkpoint.nudge(),
            Progress::Stopped => {}
        }
    }

    /// The node's log failed: from here this executor refuses writes until
    /// a snapshot of its memory is durable.
    ///
    /// What was held is answered with the refusal: applied, and not
    /// acknowledged. What was sent and not proven is let go — nothing the
    /// writer might still say about those batches can raise a point. The
    /// checkpoint is forced, abandoning an open cycle whose bases predate
    /// the failure; its next tick images memory. A failure while already
    /// refusing does all of it again, because the cycle that was running
    /// can no longer be trusted to end the refusal.
    fn refuse(&mut self) {
        let failed = self
            .sync
            .fail_all(&Reply::Error(ReplyError::LogWriteFailed));
        self.trace.held_answered(self.first_shard, failed, true);
        self.sync.sent.clear();
        self.sync.acked_bytes = self.sync.sent_bytes;
        if !self.sync.is_refusing() {
            self.stats.refusing.fetch_add(1, Ordering::Relaxed);
            self.sync.mode = Mode::Refusing {
                refused: 0,
                ticks: 0,
            };
            for state in &mut self.states {
                state.refusing = true;
            }
        }
        self.checkpoint.force();
        // The open cycle is abandoned, and the forced one takes its bases
        // after every `SAVE` waiting here.
        self.saves.append(&mut self.saves_after);
    }

    /// The inverse of the envelope's `shard - first_shard`: these states
    /// were built from a `0..shards` walk in ascending order, so a range's
    /// offsets are shard ids and fit the `u16` a shard id is.
    fn shard_at(&self, offset: usize) -> u16 {
        self.first_shard + u16::try_from(offset).expect("a shard range is shorter than u16::MAX")
    }

    /// Applies one envelope, hands what it appended to the writer, and
    /// answers it — at once, or once the sync covering it completes when
    /// the policy holds a write's replies. A read-only envelope never waits.
    fn serve(&mut self, Envelope { mut cmds, reply }: Envelope) {
        if let Some(wait) = cmds.first().and_then(|(_, cmd)| match cmd {
            Command::Snapshot { wait } => Some(*wait),
            _ => None,
        }) {
            // `Route::Every` commands travel alone: `dispatch_every` builds
            // one envelope per executor carrying that command only.
            debug_assert!(
                cmds.iter()
                    .all(|(_, cmd)| matches!(cmd, Command::Snapshot { .. }))
            );
            self.snapshot(wait, cmds.len(), reply);
            return;
        }
        // No `await` inside this loop, so a batch is applied as a unit:
        // nothing from another connection lands between its commands.
        let mut replies = Vec::with_capacity(cmds.len());
        // Which commands appended, kept only where a batch can be held: a
        // policy that answers at once never needs it, and an empty `Vec`
        // allocates nothing. Nor does a refusing executor hold anything: it
        // applies no client write, and what it may still append — a read
        // expiring its key — promises nothing that a sync would have to
        // prove, so a `Durable` must not be what answers it.
        let holds = self.sync.holds() && !self.sync.is_refusing();
        let mut appended_each = Vec::new();
        // One clock reading for the whole envelope, taken here rather
        // than inside a handler. A handler that read the clock itself
        // would still be synchronous — this is not the no-await rule —
        // but the commands of one batch would then expire keys against
        // several different instants, which is a difference nothing
        // about the batch justifies.
        //
        // It is also the first command's timing start — see
        // `ShardStats::usec`, which spends one further reading per
        // command and differences each against the one before it.
        let now = Now {
            instant: Instant::now(),
            unix_millis: (self.clock)(),
        };
        let mut last = now.instant;
        let mut wrote = false;
        let mut appended_count = 0;
        // By mutable reference, so a handler can move a command's value
        // into the dict instead of copying it — see `apply`. The trace
        // reads the command *after* the handler has had it, and reads
        // only fields no handler takes.
        for (shard, cmd) in &mut cmds {
            let (answer, appended) = self.answer(*shard, cmd, now, &mut last);
            wrote |= appended;
            appended_count += u64::from(appended);
            replies.push(answer);
            if holds {
                appended_each.push(appended);
            }
        }
        let mut touched = Vec::new();
        if wrote {
            self.stats.executors[usize::from(self.index)]
                .changes
                .fetch_add(appended_count, Ordering::Relaxed);
            // Into one buffer, in command order: one submission per batch.
            // A shard named twice hands over nothing the second time, and
            // records no second entry.
            for (shard, _) in &cmds {
                let offset = usize::from(*shard - self.first_shard);
                let before = self.sync.staging.len();
                self.states[offset].log.flush_into(&mut self.sync.staging);
                if self.sync.staging.len() > before
                    && let Some(through) = self.states[offset].log.flushed_through()
                {
                    touched.push((offset, through));
                }
            }
        }
        let batch = self.sync.batch;
        let submitted = self.submit(touched);
        if submitted && holds {
            self.sync.held.push_back(Held {
                batch,
                to: reply,
                replies,
                wrote: appended_each,
            });
        } else {
            send(reply, replies);
        }
    }

    /// A client's `SAVE` or `BGSAVE`, one reply per shard of the envelope.
    ///
    /// `BGSAVE` asks the checkpoint for a cycle on the next tick and answers
    /// at once — or answers Redis's refusal when one is open or already
    /// asked for. `SAVE` asks the same and waits: for the next image if no
    /// cycle is open, for the one after it if one is.
    fn snapshot(&mut self, wait: bool, copies: usize, reply: ReplyTo) {
        let open = self.checkpoint.is_open();
        if !wait {
            let answer = if open || self.snapshot_asked {
                Reply::Error(ReplyError::SaveInProgress)
            } else {
                self.ask_for_snapshot();
                Reply::Status("Background saving started")
            };
            send(reply, vec![answer; copies]);
        } else if open {
            self.saves_after.push((reply, vec![Reply::Ok; copies]));
        } else {
            self.ask_for_snapshot();
            self.saves.push((reply, vec![Reply::Ok; copies]));
        }
    }

    /// The checkpoint opens a cycle on the next tick, whatever the live log.
    fn ask_for_snapshot(&mut self) {
        if !self.snapshot_asked {
            self.checkpoint.nudge();
            self.snapshot_asked = true;
        }
    }

    /// The waiting `SAVE`s, after a tick: answered by an image, refused by
    /// a fault, or moved up behind the cycle that just completed.
    fn answer_saves(&mut self, completed: bool, faulted: bool) {
        if completed {
            for (to, replies) in self.saves.drain(..) {
                send(to, replies);
            }
            if !self.saves_after.is_empty() {
                self.saves.append(&mut self.saves_after);
                self.ask_for_snapshot();
            }
        } else if faulted {
            let failed = Reply::Error(ReplyError::SnapshotFailed);
            for (to, replies) in self.saves.drain(..).chain(self.saves_after.drain(..)) {
                send(to, vec![failed.clone(); replies.len()]);
            }
        }
    }

    /// One command of an envelope, answered and counted; and whether it
    /// appended to the log.
    fn answer(
        &mut self,
        shard: u16,
        cmd: &mut Command,
        now: Now,
        last: &mut Instant,
    ) -> (Reply, bool) {
        let Self {
            first_shard,
            states,
            trace,
            policy,
            memory,
            sync,
            ..
        } = self;
        let state = &mut states[usize::from(shard - *first_shard)];
        let at = state.seq;
        // Three ways a command is answered, and only the last one
        // reaches a handler.
        //
        // `Stats` is answered here rather than in `apply` because
        // what it reports — the eviction count — lives beside the
        // dict and not in it, and giving `apply` an arm that
        // could only ever answer half the fields would be an arm
        // whose answer is wrong.
        //
        // The two refusals are here for the same reason in the other
        // direction: whether the log can be kept, or a write is over
        // the ceiling, is a question about the executor or the node,
        // and `apply` is deliberately a function of one shard's own
        // state. A write refused for the log is not applied.
        let answer = if matches!(cmd, Command::Stats) {
            Reply::Stats(Box::new(stats_of(state)))
        } else if let Mode::Refusing { refused, .. } = &mut sync.mode
            && cmd.writes_the_log()
            && !sync.plants.acks_while_refusing
        {
            *refused += 1;
            Reply::Error(ReplyError::LogWriteFailed)
        } else if memory.limit.mode == EvictionMode::NoEviction
            && cmd.denied_when_full()
            && memory.limit.exceeded(memory.gauge.used())
        {
            Reply::Error(ReplyError::OutOfMemory)
        } else {
            // The gauge is the sum of what the dicts account, so
            // it is moved by the difference this command made to
            // one of them. Read either side of `apply` rather
            // than inside it: the handlers stay unaware there is
            // a gauge at all.
            let before = state.dict.used_bytes();
            let answer = apply(state, shard, cmd, now, policy);
            memory.gauge.apply(before, state.dict.used_bytes());
            // Not while refusing: an eviction is a write of its own.
            if memory.limit.mode == EvictionMode::AllKeysLru && !sync.is_refusing() {
                // The command's key survives this. `apply` takes
                // a command's value but never its key — the trace
                // reads it after — so the route still names it.
                let spared = match cmd.route() {
                    Route::Key(key) => Some(key),
                    Route::Shard(_) | Route::Every | Route::Unaddressed => None,
                };
                evict_until_fits(state, shard, memory, trace, policy, spared);
            }
            answer
        };
        // A reading per command, differenced against the one
        // before it, so a batch of `n` commands costs `n`
        // readings rather than `2n`.
        //
        // `saturating_duration_since` rather than a subtraction:
        // a monotonic clock is only promised not to go backwards,
        // and a reading that did would panic here rather than
        // report a zero microsecond nobody would miss.
        let spent = {
            let after = Instant::now();
            let spent = after.saturating_duration_since(*last).as_micros();
            *last = after;
            // A command that ran for half a million years would
            // saturate; the shard it ran on has other problems.
            u64::try_from(spent).unwrap_or(u64::MAX)
        };
        count_call(state, cmd, &answer, spent);
        trace.record(shard, at, cmd, &answer);
        let appended = state.seq != at;
        (answer, appended)
    }

    /// The work no command asked for: each shard's rehash step and sweep,
    /// the submission of what the sweep appended, and the checkpoint's
    /// budget.
    fn housekeeping(&mut self) {
        // One clock reading for the whole tick, for the reason the
        // envelope arm takes one for the whole batch: the shards of an
        // executor should not disagree about which keys this tick found
        // expired.
        let now = Instant::now();
        let refusing = self.sync.is_refusing();
        for offset in 0..self.states.len() {
            let shard = self.shard_at(offset);
            let state = &mut self.states[offset];
            // One reading either side of the whole tick's work: the
            // rehash step changes what the tables cost and the sweep
            // changes what the entries do, and the gauge only cares
            // about the sum.
            let before = state.dict.used_bytes();
            state.dict.rehash_step(REHASH_BUCKETS_PER_TICK);
            // A refusing executor writes nothing of its own accord: the
            // sweep's deletions wait, and a read still answers "no key"
            // for whatever has expired, as it does between two sweeps.
            if !refusing {
                sweep_expired(state, shard, &self.trace, now, &self.policy);
            }
            self.memory.gauge.apply(before, state.dict.used_bytes());
        }
        // What the sweep appended.
        let touched = self.stage_all();
        self.submit(touched);
        if let Mode::Refusing { ticks, .. } = &mut self.sync.mode {
            *ticks += 1;
        }
        // The checkpoint, last: its bases are read at a point where every
        // shard's buffer has been handed to the writer, and its budget is
        // the last thing the tick spends.
        let position = LogPosition {
            bytes: self.sync.sent_bytes,
            batch: self.sync.batch.checked_sub(1),
        };
        // A cycle forced away since the last tick took its count with it.
        if !self.checkpoint.is_open() {
            self.changes_at_open = None;
        }
        let tick = self.checkpoint.tick(
            self.first_shard,
            &mut self.states,
            Now {
                instant: now,
                unix_millis: (self.clock)(),
            },
            &self.trace,
            position,
        );
        // Whatever a client asked for, this tick opened.
        self.snapshot_asked = false;
        let cell = &self.stats.executors[usize::from(self.index)];
        cell.cycle_open
            .store(u64::from(self.checkpoint.is_open()), Ordering::Relaxed);
        if tick.faulted {
            cell.last_save_failed.store(1, Ordering::Relaxed);
        }
        if let Some(done) = tick.completed {
            self.stats
                .lossy_shards
                .fetch_sub(done.cleared, Ordering::Relaxed);
            // A cycle that opened on this very tick took its bases after
            // every change counted so far.
            let covered = self
                .changes_at_open
                .take()
                .unwrap_or_else(|| cell.changes.load(Ordering::Relaxed));
            cell.changes.fetch_sub(covered, Ordering::Relaxed);
            cell.saves.fetch_add(1, Ordering::Relaxed);
            cell.last_save_unix
                .store(newest_images_secs(&self.states), Ordering::Relaxed);
            cell.last_save_ticks.store(done.ticks, Ordering::Relaxed);
            cell.last_save_failed.store(0, Ordering::Relaxed);
            if let Some(link) = &self.sync.link {
                let _ = link.to_writer.send(ToWriter::Covered {
                    executor: link.executor,
                    cycle: done.cycle,
                    through_batch: done.through_batch,
                    snapshot_bytes: done.bytes,
                });
            }
            if let Mode::Refusing { refused, ticks } = self.sync.mode
                && !self.sync.writer_lost
            {
                // Forced at the failure, so the snapshot is of memory after
                // it: everything the failed log held is covered.
                self.trace.refusal_ended(&RefusalReport {
                    executor_first_shard: self.first_shard,
                    refused,
                    ticks,
                });
                self.sync.mode = Mode::Serving;
                self.stats.refusing.fetch_sub(1, Ordering::Relaxed);
                for state in &mut self.states {
                    state.refusing = false;
                }
            }
        }
        self.answer_saves(tick.completed.is_some(), tick.faulted);
        // A cycle opened on this tick: its image will cover what was
        // counted until now, and not what comes after.
        if self.checkpoint.is_open() && self.changes_at_open.is_none() {
            self.changes_at_open = Some(
                self.stats.executors[usize::from(self.index)]
                    .changes
                    .load(Ordering::Relaxed),
            );
        }
    }
}

/// The time of the oldest of these shards' newest durable images, in Unix
/// seconds — "since when is every one of them covered" — or `0` while any
/// has none. `0` is that absence, so an image taken in the epoch's first
/// second reads as the next.
fn newest_images_secs<L>(states: &[ShardState<L>]) -> u64 {
    states
        .iter()
        .map(|state| state.image_unix_millis)
        .try_fold(u64::MAX, |oldest, at| at.map(|at| oldest.min(at)))
        .filter(|_| !states.is_empty())
        .map_or(0, |millis| (millis / 1000).max(1))
}

/// Reclaims a budget's worth of expired keys from one shard, logging and
/// tracing each removal exactly as an explicit `Del` is.
///
/// The active half of expiration, and the half [`evict_if_expired`] cannot be:
/// a key nothing ever addresses again meets no command, so nothing lazy can
/// reclaim it. Together they are the guarantee — a key stops being visible at
/// its deadline, and stops costing memory shortly after.
///
/// A removal here is a keyspace mutation like any other, so it takes a
/// replication position and appends its record *before* the entry goes, and it
/// reaches the [`TraceSink`] as the `Del` it amounts to. Nothing downstream has
/// to know a sweep exists.
///
/// An append that fails abandons the rest of this tick's budget and leaves the
/// cursor where it was: the entries keep their deadlines, and the same buckets
/// are swept again on the next tick. Skipping them would mean waiting a whole
/// cycle to retry a key the log has already refused to let go.
pub fn sweep_expired<T: TraceSink, L: ReplicationLog, P: ExpiryPolicy>(
    state: &mut ShardState<L>,
    shard: u16,
    trace: &T,
    now: Instant,
    expiry: &P,
) {
    let (next, dead) =
        state
            .dict
            .expire_step(state.expire_cursor, EXPIRE_BUCKETS_PER_TICK, now, expiry);
    for key in dead {
        let at = state.seq;
        if append(
            &mut state.log,
            &mut state.seq,
            shard,
            Effect::Del { key: &key[..] },
        )
        .is_err()
        {
            return;
        }
        state.dict.remove(&key);
        // The active half of `expired_keys`; the lazy half is counted in
        // [`apply`], in front of the command that met the key.
        state.expired += 1;
        // The command an expiry is indistinguishable from. Built after the
        // removal because it takes the key, which is what keeps the sweep from
        // cloning it to say the same thing twice.
        trace.record(shard, at, &Command::Del { key }, &Reply::Removed(true));
    }
    state.expire_cursor = next;
}

/// Reclaims from this shard while the policy says the node must, one sampled
/// victim at a time, stopping when it says otherwise or the shard is empty.
///
/// The condition is [`EvictionPolicy::must_evict`] rather than the comparison
/// it stands for, so that the decision itself is the thing a plant replaces —
/// see the trait for why that is worth a seam.
///
/// Synchronous, inside the executor loop, for the reason every handler is: a
/// write that has to make room does so before its reply is sent, and nothing
/// on this executor interleaves with it. What it costs is latency on the
/// write that crossed the line, bounded by how many samples it takes to get
/// back under — the shape an operator reads in `ARCHITECTURE.md`.
///
/// Each removal is a keyspace mutation: it takes a replication position,
/// appends first, and reaches the trace as the `Del` it amounts to — the
/// discipline [`sweep_expired`] set.
///
/// **The ceiling is the node's and the victims are this shard's**, so this
/// terminates on this shard running out of keys to give rather than on the
/// figure: a value larger than the whole ceiling leaves the node over it
/// until something reclaims elsewhere. Looping instead would be a shard
/// spinning against bytes it does not own.
///
/// `spared` is the key the command that triggered this addressed, and it is
/// never the victim. Redis reaches the same place from the other side: it
/// evicts *before* running the command, so the key being written does not yet
/// exist to be chosen. Evicting here is what lets the decision be taken
/// against the figure the write actually produced, and sparing the key is
/// what keeps that from meaning a write can free room by undoing itself —
/// which on a value larger than the ceiling is the difference between storing
/// it and storing nothing.
pub fn evict_until_fits<T: TraceSink, L: ReplicationLog, P: EvictionPolicy>(
    state: &mut ShardState<L>,
    shard: u16,
    memory: &Memory,
    trace: &T,
    policy: &P,
    spared: Option<&[u8]>,
) {
    while policy.must_evict(memory.gauge.used(), memory.limit.ceiling) {
        // `spared` goes to the sampler rather than being checked against
        // what comes back: a sample that met the spared key and rejected it
        // afterwards would answer `None` — the shard saying it has nothing to
        // give — from a shard with plenty left. Excluded there, `None` keeps
        // meaning what the empty case above means, and stopping stays right.
        let Some(victim) =
            state
                .dict
                .sample_oldest(&mut state.evict_cursor, EVICTION_SAMPLES, spared)
        else {
            return;
        };
        let at = state.seq;
        if append(
            &mut state.log,
            &mut state.seq,
            shard,
            Effect::Del { key: &victim[..] },
        )
        .is_err()
        {
            return;
        }
        let before = state.dict.used_bytes();
        state.dict.remove(&victim);
        memory.gauge.apply(before, state.dict.used_bytes());
        state.evicted += 1;
        // The command an eviction is indistinguishable from, built after the
        // removal for the reason the sweep builds its own there: it takes the
        // key rather than cloning it to say the same thing twice.
        trace.record(
            shard,
            at,
            &Command::Del { key: victim },
            &Reply::Removed(true),
        );
    }
}

/// The half of [`ShardStats`] the dict alone can state: a reading of the
/// keyspace as it stands, rather than a total the shard has been running.
///
/// Above [`crate::shard::apply::apply`] with the executor's helpers rather than below it with the
/// dispatcher's, because it has two callers and the executor is the one that
/// matters: [`stats_of`] fills in the counters this cannot see, and `apply`'s
/// own `Stats` arm — which nothing routes to — answers with this alone.
///
/// # Panics
///
/// If a `usize` does not fit a `u64`, which no target this builds for has.
pub fn keyspace_stats(dict: &Dict) -> ShardStats {
    ShardStats {
        keys: u64::try_from(dict.len()).expect("a length is a usize"),
        expires: u64::try_from(dict.with_deadline()).expect("a count is a usize"),
        ..ShardStats::default()
    }
}

/// Counts one completed command against this shard's totals.
///
/// **A command that reaches every shard is not counted here**, and that is the
/// difference between a figure and a multiple of one: `DBSIZE`, `FLUSHDB` and
/// the [`Command::Stats`] an `INFO` gathers with are one request each, split
/// into one command per shard by the edge, so counting them where they land
/// would report a single `DBSIZE` as sixteen. The edge counts those, once, as
/// the requests they were. Everything else reaches exactly one shard per
/// request, so this is the only place that has to count it — and it is the
/// cheap place, because these are plain `u64`s on a state one task owns.
///
/// The refusals are counted with the successes. A command the peer sent and
/// the server answered is a call however it was answered, which is what Redis
/// counts and what an operator comparing `commandstats` to a client's own
/// tally is looking for.
///
/// `spent` is what the command cost in microseconds, and it is added here
/// rather than beside the reading that produced it so that the two figures
/// cannot disagree about which commands they describe: the one guard above
/// decides both. A command excluded from `calls` contributes no time either,
/// which is what keeps `usec_per_call` a quotient of two figures counted over
/// the same set. It is always `0` on a build with the timing compiled out.
pub fn count_call<L>(state: &mut ShardState<L>, cmd: &Command, reply: &Reply, spent: u64) {
    if !matches!(cmd.route(), Route::Every) {
        state.calls[usize::from(cmd.kind())] += 1;
        state.usec[usize::from(cmd.kind())] += spent;
    }
    match read_outcome(&state.dict, cmd, reply) {
        Some(true) => state.hits += 1,
        Some(false) => state.misses += 1,
        None => {}
    }
}

/// Whether a command that *read* a key found one, or `None` if it read none.
///
/// Redis counts `keyspace_hits` and `keyspace_misses` over the lookups that
/// read a value rather than over every command, which here is `GET`, `EXISTS`,
/// `TTL`, `TYPE` and `STRLEN` — a write is not a lookup, and neither is a
/// command that names no key.
///
/// **This is not the same set as the one that refreshes the LRU stamp**, and
/// the difference is deliberate rather than an oversight. Five commands are
/// counted here; three stamp — `GET`, `SET` and `INCRBY` — plus `STRLEN`,
/// which is the only lookup of the five that reads the value. Redis draws the
/// same two lines in the same two places, measured with `OBJECT IDLETIME`
/// against 6.2.24: `EXISTS`, `TYPE` and `TTL` are counted as lookups and
/// leave the stamp alone. A hit is about what an operator is told about the
/// keyspace; a stamp is about what eviction may take next.
///
/// Read off the reply wherever the reply says, so the classification costs
/// nothing: four of the five answer differently for a key that was there.
/// `STRLEN` is the exception — a stored empty value and an absent key both
/// measure `0` — and that one case asks the dict, which is a second hash on a
/// command nothing on the hot path issues, rather than a miss counted against
/// a key that was present.
fn read_outcome(dict: &Dict, cmd: &Command, reply: &Reply) -> Option<bool> {
    match (cmd, reply) {
        (Command::Get { .. }, Reply::Bulk(value)) => Some(value.is_some()),
        (Command::Exists { .. }, Reply::Integer(found)) => Some(*found == 1),
        // `-2` is `TTL`'s answer for a key that is not there; `-1` and every
        // span above it are answers about a key that is. `PTTL` draws the same
        // line at the same number, in the other unit.
        (Command::Ttl { .. } | Command::PTtl { .. }, Reply::Integer(remaining)) => {
            Some(*remaining != -2)
        }
        (Command::Type { .. }, Reply::Status(name)) => Some(*name != "none"),
        (Command::StrLen { key }, Reply::Integer(len)) => Some(*len > 0 || dict.get(key).is_some()),
        _ => None,
    }
}

/// What this shard has counted, as [`Command::Stats`] reports it.
///
/// The keyspace figures are read from the dict here and now; the rest are
/// running totals the shard has been maintaining. Nothing is computed across
/// shards — summing is the edge's, because only the edge has every answer.
fn stats_of<L>(state: &ShardState<L>) -> ShardStats {
    ShardStats {
        evicted: state.evicted,
        hits: state.hits,
        misses: state.misses,
        expired: state.expired,
        calls: state.calls,
        usec: state.usec,
        ..keyspace_stats(&state.dict)
    }
}
