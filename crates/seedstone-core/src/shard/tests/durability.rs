//! The executor under each durability policy: when a reply leaves, when a
//! sync is issued, and what a completed or failed sync does to the point.

use super::support::{get, set};
use crate::dict::{Dict, DictSeed, shard_seed};
use crate::log::checkpoint::{CheckpointConfig, CheckpointSpec, NoCheckpoint, SegmentCheckpoint};
use crate::log::disk::mem::MemDisk;
use crate::log::disk::{Disk, SyncFuture};
use crate::log::file::{FileLog, open_segments};
use crate::log::recovery::RecoveredShard;
use crate::log::{Record, ReplicationLog};
use crate::memory::MemoryLimit;
use crate::shard::{
    Command, Deadlines, ExecutorPlants, HOUSEKEEPING_TICK, LogFault, NoTrace, PoolSpec,
    RefusalReport, Reply, ReplyError, Router, SHUTDOWN_GRACE, ShardPool, Shutdown, SyncPolicy,
    TraceSink, frozen_clock,
};
use bytes::Bytes;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::AtomicU16;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

/// A log whose syncs complete when the test says so.
#[derive(Clone, Default)]
struct Latent {
    inner: Arc<Mutex<LatentState>>,
}

#[derive(Default)]
struct LatentState {
    flushed: Option<u64>,
    dirty: bool,
    /// One sender per issued sync, in issue order; the test completes them.
    pending: VecDeque<oneshot::Sender<std::io::Result<()>>>,
    issued: u64,
    completed: Vec<Option<u64>>,
    failed: u64,
}

impl Latent {
    fn state(&self) -> std::sync::MutexGuard<'_, LatentState> {
        self.inner.lock().expect("latent log")
    }

    fn issued(&self) -> u64 {
        self.state().issued
    }

    fn complete_next(&self, result: std::io::Result<()>) {
        let tx = self.state().pending.pop_front().expect("a sync in flight");
        let _ = tx.send(result);
    }
}

impl ReplicationLog for Latent {
    fn append(&mut self, rec: Record<'_>) -> std::io::Result<()> {
        // Flushed as appended: the double has no buffer of its own.
        self.state().flushed = Some(rec.seq);
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.state().dirty = true;
        Ok(())
    }

    fn sync(&mut self) -> std::io::Result<Option<u64>> {
        Ok(self.state().flushed)
    }

    fn flushed_through(&self) -> Option<u64> {
        self.state().flushed
    }

    fn begin_sync(&mut self) -> Option<SyncFuture> {
        let mut state = self.state();
        if !state.dirty {
            return None;
        }
        state.dirty = false;
        state.issued += 1;
        let (tx, rx) = oneshot::channel();
        state.pending.push_back(tx);
        drop(state);
        Some(Box::pin(async move {
            rx.await
                .unwrap_or_else(|_| Err(std::io::Error::other("dropped")))
        }))
    }

    fn sync_completed(&mut self, through: Option<u64>) -> Option<u64> {
        self.state().completed.push(through);
        through
    }

    fn sync_failed(&mut self) {
        self.state().failed += 1;
    }
}

fn pool(policy: SyncPolicy, log: &Latent) -> ShardPool {
    let log = log.clone();
    ShardPool::spawn_spec(PoolSpec {
        shards: 1,
        executors: 1,
        seed: DictSeed { k0: 1, k1: 2 },
        trace: NoTrace,
        make_log: move |_| log.clone(),
        policy: Deadlines,
        limit: MemoryLimit::default(),
        clock: frozen_clock,
        recovered: Vec::new(),
        make_checkpoint: |_| NoCheckpoint,
        sync: policy,
        plants: ExecutorPlants::default(),
    })
}

/// Lets every ready task run: the dispatching one, then the executor.
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

/// Under `always` a write's reply waits for the sync that covers it; a
/// read on the same shard is answered at once; the batch that arrived
/// during the flight is released by the next sync.
#[tokio::test(start_paused = true)]
async fn always_holds_a_write_until_its_sync_completes_and_never_a_read() {
    let log = Latent::default();
    let pool = pool(SyncPolicy::ALWAYS, &log);
    let first = tokio::spawn({
        let pool = pool.clone();
        async move { pool.dispatch(set(b"k", b"1")).await }
    });
    settle().await;
    assert_eq!(
        log.issued(),
        1,
        "the write was flushed and a sync issued at once"
    );
    assert!(!first.is_finished(), "held until the sync completes");
    assert_eq!(
        pool.dispatch(get(b"k")).await,
        Reply::Bulk(Some(Bytes::from_static(b"1"))),
        "a read never waits, and sees memory"
    );
    let second = tokio::spawn({
        let pool = pool.clone();
        async move { pool.dispatch(set(b"k", b"2")).await }
    });
    settle().await;
    assert_eq!(
        log.issued(),
        1,
        "one in flight; the second batch waits for the next issue"
    );
    log.complete_next(Ok(()));
    assert_eq!(first.await.unwrap(), Reply::Ok);
    settle().await;
    assert_eq!(
        log.issued(),
        2,
        "issued the moment the previous one completed"
    );
    assert!(!second.is_finished());
    log.complete_next(Ok(()));
    assert_eq!(second.await.unwrap(), Reply::Ok);
    assert_eq!(
        log.state().completed,
        vec![Some(0), Some(1)],
        "each completion carries the point at its issue"
    );
}

/// A completed sync is seen ahead of the inbox: under `always`, a write
/// whose sync has completed is answered before a backlog of reads queued
/// behind it is drained, not once the inbox happens to run dry.
#[tokio::test(start_paused = true)]
async fn a_completed_sync_releases_its_write_ahead_of_a_queued_backlog() {
    const BACKLOG: usize = 5_000;
    let log = Latent::default();
    let pool = pool(SyncPolicy::ALWAYS, &log);
    let answered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let write = tokio::spawn({
        let pool = pool.clone();
        let answered = Arc::clone(&answered);
        async move {
            let reply = pool.dispatch(set(b"k", b"v")).await;
            (reply, answered.load(std::sync::atomic::Ordering::SeqCst))
        }
    });
    settle().await;
    assert_eq!(log.issued(), 1, "the write's sync is in flight");
    let reads: Vec<_> = (0..BACKLOG)
        .map(|_| {
            let pool = pool.clone();
            let answered = Arc::clone(&answered);
            tokio::spawn(async move {
                let reply = pool.dispatch(get(b"other")).await;
                answered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                reply
            })
        })
        .collect();
    // Every read is queued before the executor next runs, and the sync
    // completes in the same instant.
    log.complete_next(Ok(()));
    let (reply, reads_before) = write.await.unwrap();
    assert_eq!(reply, Reply::Ok);
    assert!(
        reads_before < BACKLOG / 10,
        "the write waited for {reads_before} of {BACKLOG} queued reads"
    );
    for read in reads {
        assert_eq!(read.await.unwrap(), Reply::Bulk(None));
    }
}

/// Under `interval` a write is acknowledged at once, and a sync is issued
/// at most once per interval from the request path, and from the tick
/// when traffic stops.
#[tokio::test(start_paused = true)]
async fn interval_acknowledges_at_once_and_syncs_at_most_once_per_interval() {
    let log = Latent::default();
    let pool = pool(SyncPolicy::INTERVAL, &log);
    assert_eq!(pool.dispatch(set(b"k", b"1")).await, Reply::Ok);
    assert_eq!(
        log.issued(),
        1,
        "the first write issues at once: the interval since start has passed"
    );
    log.complete_next(Ok(()));
    assert_eq!(pool.dispatch(set(b"k", b"2")).await, Reply::Ok);
    assert_eq!(log.issued(), 1, "inside the interval: not yet");
    tokio::time::advance(HOUSEKEEPING_TICK + Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(log.issued(), 2, "the tick issued it when traffic stopped");
}

/// Under `never` no sync is ever issued.
#[tokio::test(start_paused = true)]
async fn never_issues_no_sync() {
    let log = Latent::default();
    let pool = pool(SyncPolicy::NEVER, &log);
    for _ in 0..5 {
        assert_eq!(pool.dispatch(set(b"k", b"v")).await, Reply::Ok);
        tokio::time::advance(HOUSEKEEPING_TICK * 2).await;
        settle().await;
    }
    assert_eq!(log.issued(), 0);
}

/// A stop drains what is queued, waits for the sync in flight, releases
/// what it held, syncs what was flushed meanwhile, and only then ends —
/// under every policy, `never` included.
#[tokio::test(start_paused = true)]
async fn shutdown_syncs_what_was_written_under_every_policy() {
    for policy in [SyncPolicy::ALWAYS, SyncPolicy::INTERVAL, SyncPolicy::NEVER] {
        let log = Latent::default();
        let pool = pool(policy, &log);
        let write = tokio::spawn({
            let pool = pool.clone();
            async move { pool.dispatch(set(b"k", b"v")).await }
        });
        settle().await;
        let stopping = tokio::spawn({
            let pool = pool.clone();
            async move { pool.shutdown().await }
        });
        settle().await;
        // Whatever was issued — the write's own sync under always or
        // interval, the stop's under never — completes now.
        while log.issued() > log.state().completed.len() as u64 {
            log.complete_next(Ok(()));
            settle().await;
        }
        assert_eq!(write.await.unwrap(), Reply::Ok, "{}", policy.name());
        assert_eq!(
            stopping.await.unwrap(),
            Shutdown::Clean,
            "{}",
            policy.name()
        );
        assert!(log.issued() >= 1, "{}: the stop synced", policy.name());
        assert_eq!(
            log.state().completed.last().copied().flatten(),
            Some(0),
            "{}: the last sync covered the write",
            policy.name()
        );
    }
}

/// A sync that never completes does not hold the process hostage: the
/// stop gives up after the grace period and says so.
#[tokio::test(start_paused = true)]
async fn shutdown_gives_up_after_the_grace_period() {
    let log = Latent::default();
    let pool = pool(SyncPolicy::INTERVAL, &log);
    assert_eq!(pool.dispatch(set(b"k", b"v")).await, Reply::Ok);
    let stopping = tokio::spawn({
        let pool = pool.clone();
        async move { pool.shutdown().await }
    });
    tokio::time::advance(SHUTDOWN_GRACE + Duration::from_millis(1)).await;
    assert_eq!(stopping.await.unwrap(), Shutdown::TimedOut);
}

/// The faults and the refusals an executor reported.
#[derive(Clone, Default)]
struct Story {
    faults: Arc<Mutex<Vec<LogFault>>>,
    ended: Arc<Mutex<Vec<RefusalReport>>>,
}

impl Story {
    fn faults_of(&self, fault: LogFault) -> usize {
        let faults = self.faults.lock().expect("story");
        faults.iter().filter(|seen| **seen == fault).count()
    }

    fn refusals_ended(&self) -> Vec<RefusalReport> {
        self.ended.lock().expect("story").clone()
    }
}

impl TraceSink for Story {
    fn record(&self, _: u16, _: u64, _: &Command, _: &Reply) {}
    fn fault(&self, _shard: u16, fault: LogFault, _error: &std::io::Error) {
        self.faults.lock().expect("story").push(fault);
    }
    fn refusal_ended(&self, report: &RefusalReport) {
        self.ended.lock().expect("story").push(*report);
    }
}

const WAL: &str = "/data/wal";

/// A pool of one executor over the in-memory disk, with the real log and
/// the real checkpoint, whose floor no write reaches: only a forced cycle
/// opens.
fn disk_pool(policy: SyncPolicy, story: &Story) -> (MemDisk, ShardPool) {
    disk_pool_from(policy, story, Vec::new(), false)
}

/// [`disk_pool`], starting from `recovered`, on a disk that fails writes
/// from the moment the segments are open if `failing`.
fn disk_pool_from(
    policy: SyncPolicy,
    story: &Story,
    recovered: Vec<RecoveredShard>,
    failing: bool,
) -> (MemDisk, ShardPool) {
    let disk = MemDisk::default();
    let wal = Path::new(WAL);
    disk.create_dir_all(wal).unwrap();
    let segments = open_segments(&disk, wal, 1, 1).unwrap();
    disk.fail_writes(failing);
    let round = Arc::new(AtomicU16::new(0));
    let (log_segment, cp_segment) = (Arc::clone(&segments[0]), Arc::clone(&segments[0]));
    let cp_disk = disk.clone();
    let pool = ShardPool::spawn_spec(PoolSpec {
        shards: 2,
        executors: 1,
        seed: DictSeed { k0: 1, k1: 2 },
        trace: story.clone(),
        make_log: move |shard| FileLog::new(shard, Arc::clone(&log_segment)),
        policy: Deadlines,
        limit: MemoryLimit::default(),
        clock: frozen_clock,
        recovered,
        make_checkpoint: move |executor| {
            SegmentCheckpoint::new(CheckpointSpec {
                disk: cp_disk.clone(),
                wal: wal.to_path_buf(),
                generation: 1,
                executor,
                executors: 1,
                segment: Arc::clone(&cp_segment),
                round: Arc::clone(&round),
                config: CheckpointConfig {
                    floor: u64::MAX,
                    ratio: 1,
                    bytes_per_tick: 1 << 20,
                },
            })
        },
        sync: policy,
        plants: ExecutorPlants::default(),
    });
    (disk, pool)
}

/// `ticks` housekeeping ticks, each let run.
async fn tick(ticks: u32) {
    for _ in 0..ticks {
        tokio::time::advance(HOUSEKEEPING_TICK).await;
        settle().await;
    }
}

fn refused() -> Reply {
    Reply::Error(ReplyError::LogWriteFailed)
}

/// A sync that fails under `always` answers the held batch with the
/// refusal, refuses every later write, serves reads — and the refused
/// write that was applied is in memory, where a reader sees it.
#[tokio::test(start_paused = true)]
async fn a_failed_sync_refuses_writes_until_a_snapshot_lands() {
    let story = Story::default();
    let (disk, pool) = disk_pool(SyncPolicy::ALWAYS, &story);
    disk.fail_syncs(true);
    assert_eq!(
        pool.dispatch(set(b"k", b"1")).await,
        refused(),
        "the held batch is answered with the refusal"
    );
    assert_eq!(
        pool.dispatch(set(b"k", b"2")).await,
        refused(),
        "and every write after it"
    );
    assert_eq!(
        pool.dispatch(get(b"k")).await,
        Reply::Bulk(Some(Bytes::from_static(b"1"))),
        "reads are served, and the applied write is what memory holds"
    );
    assert_eq!(
        pool.dispatch(Command::Del {
            key: Bytes::from_static(b"k")
        })
        .await,
        refused(),
        "a delete is a write"
    );
    assert_eq!(story.faults_of(LogFault::Sync), 1);
    disk.fail_syncs(false);
    // The way out: the forced cycle runs on the ticks, and lands.
    tick(6).await;
    assert_eq!(
        pool.dispatch(set(b"k", b"3")).await,
        Reply::Ok,
        "serving again"
    );
    let ended = story.refusals_ended();
    assert_eq!(ended.len(), 1);
    assert_eq!(
        ended[0].refused, 2,
        "the writes refused meanwhile, not the held one"
    );
    assert_eq!(ended[0].executor_first_shard, 0);
    assert!(ended[0].ticks >= 1);
    assert!(
        disk.list(Path::new(WAL))
            .unwrap()
            .iter()
            .any(|name| Path::new(name).extension().is_some_and(|ext| ext == "snap")),
        "the snapshot that ended it"
    );
}

/// A write that fails — a full disk — refuses the same way under
/// `interval`, where nothing was held: the next write is refused, and the
/// recovery snapshot covers what was acknowledged before.
/// A start whose recovery cut a shard writes a rebase before it serves; if
/// that write fails, the executor starts refusing, as it would on any
/// failed write, and serves writes again once a snapshot lands.
#[tokio::test(start_paused = true)]
async fn a_start_whose_rebase_fails_refuses_writes_until_a_snapshot_lands() {
    let story = Story::default();
    let root = DictSeed { k0: 1, k1: 2 };
    let recovered = (0..2)
        .map(|shard| RecoveredShard {
            dict: Dict::with_seed(shard_seed(root, shard)),
            seq: 0,
            lossy: true,
            cut: shard == 0,
        })
        .collect();
    let (disk, pool) = disk_pool_from(SyncPolicy::INTERVAL, &story, recovered, true);
    assert_eq!(story.faults_of(LogFault::Write), 1, "the rebase");
    disk.fail_writes(false);
    assert_eq!(
        pool.dispatch(set(b"k", b"1")).await,
        refused(),
        "a node whose log just failed takes no write"
    );
    tick(6).await;
    assert_eq!(pool.dispatch(set(b"k", b"2")).await, Reply::Ok);
    assert_eq!(story.refusals_ended().len(), 1);
}

/// A read in the same batch as a held write is served when the sync fails:
/// only the write's reply becomes the refusal.
#[tokio::test(start_paused = true)]
async fn a_read_held_beside_a_write_is_served_when_the_sync_fails() {
    let story = Story::default();
    let (disk, pool) = disk_pool(SyncPolicy::ALWAYS, &story);
    assert_eq!(pool.dispatch(set(b"r", b"0")).await, Reply::Ok);
    disk.fail_syncs(true);
    assert_eq!(
        pool.dispatch_many(vec![set(b"k", b"1"), get(b"r")]).await,
        vec![refused(), Reply::Bulk(Some(Bytes::from_static(b"0")))],
        "the write is refused, the read beside it served"
    );
}

#[tokio::test(start_paused = true)]
async fn a_full_disk_refuses_writes_and_a_snapshot_resumes_them() {
    let story = Story::default();
    let (disk, pool) = disk_pool(SyncPolicy::INTERVAL, &story);
    assert_eq!(pool.dispatch(set(b"before", b"v")).await, Reply::Ok);
    disk.fail_writes_with(std::io::ErrorKind::StorageFull);
    assert_eq!(
        pool.dispatch(set(b"k", b"1")).await,
        Reply::Ok,
        "acknowledged: the flush fails after apply, and interval holds nothing"
    );
    assert_eq!(pool.dispatch(set(b"k", b"2")).await, refused());
    assert_eq!(
        pool.dispatch(get(b"before")).await,
        Reply::Bulk(Some(Bytes::from_static(b"v")))
    );
    tick(3).await;
    assert_eq!(
        pool.dispatch(set(b"k", b"3")).await,
        refused(),
        "a full disk fails the snapshot too: still refusing"
    );
    assert!(story.faults_of(LogFault::Snapshot) >= 1);
    assert!(story.refusals_ended().is_empty());
    disk.fail_writes(false);
    tick(6).await;
    assert_eq!(pool.dispatch(set(b"k", b"3")).await, Reply::Ok);
    assert_eq!(
        pool.dispatch(get(b"k")).await,
        Reply::Bulk(Some(Bytes::from_static(b"3")))
    );
    assert_eq!(story.refusals_ended().len(), 1);
}

/// A stop whose final sync fails answers what it held with the refusal,
/// not as a success no sync stands behind.
#[tokio::test(start_paused = true)]
async fn a_stop_whose_last_sync_fails_answers_its_held_writes_with_the_refusal() {
    let log = Latent::default();
    let pool = pool(SyncPolicy::ALWAYS, &log);
    let first = tokio::spawn({
        let pool = pool.clone();
        async move { pool.dispatch(set(b"k", b"1")).await }
    });
    settle().await;
    let second = tokio::spawn({
        let pool = pool.clone();
        async move { pool.dispatch(set(b"k", b"2")).await }
    });
    settle().await;
    assert_eq!(
        log.issued(),
        1,
        "the second write waits behind the first sync"
    );
    let stopping = tokio::spawn({
        let pool = pool.clone();
        async move { pool.shutdown().await }
    });
    settle().await;
    log.complete_next(Ok(()));
    settle().await;
    assert_eq!(first.await.unwrap(), Reply::Ok);
    assert_eq!(log.issued(), 2, "the stop's own sync, for the second write");
    log.complete_next(Err(std::io::Error::other("injected")));
    assert_eq!(second.await.unwrap(), refused());
    assert_eq!(stopping.await.unwrap(), Shutdown::Clean);
    assert_eq!(log.state().failed, 1);
}
