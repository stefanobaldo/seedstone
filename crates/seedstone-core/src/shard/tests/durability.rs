//! The executor under each durability policy: when a reply leaves, what it
//! hands the node's writer, and what the writer's word does to the point.
//! Either the test plays the writer over [`fake_links`], or the real one
//! runs over the in-memory disk through [`disk_pool`].

use super::support::{SEED, WAL, disk_pool, disk_pool_cut, fake_links, get, set};
use crate::log::checkpoint::{CheckpointConfig, CheckpointSpec, NoCheckpoint, SegmentCheckpoint};
use crate::log::disk::Disk;
use crate::log::disk::mem::MemDisk;
use crate::log::file::{FileLog, segment_name};
use crate::log::writer::{Progress, ToWriter, WRITER_BUDGET, WriterLink};
use crate::log::{Decoded, decode_record};
use crate::memory::MemoryLimit;
use crate::shard::{
    Command, Deadlines, ExecutorPlants, HOUSEKEEPING_TICK, LogFault, NoTrace, PoolSpec,
    RefusalReport, Reply, ReplyError, Router, SHUTDOWN_GRACE, ShardPool, Shutdown, SyncPolicy,
    TraceSink, frozen_clock,
};
use crate::slot::{executor_of, shard_of};
use bytes::Bytes;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Two shards on one executor whose writer is the test.
fn pool_with_links(policy: SyncPolicy, links: Vec<WriterLink>) -> ShardPool {
    pool_with_links_and_trace(policy, links, NoTrace)
}

fn pool_with_links_and_trace<T: TraceSink>(
    policy: SyncPolicy,
    links: Vec<WriterLink>,
    trace: T,
) -> ShardPool {
    ShardPool::spawn_spec(PoolSpec {
        shards: 2,
        executors: 1,
        seed: SEED,
        trace,
        make_log: FileLog::new,
        policy: Deadlines,
        limit: MemoryLimit::default(),
        clock: frozen_clock,
        recovered: Vec::new(),
        make_checkpoint: |_| NoCheckpoint,
        sync: policy,
        plants: ExecutorPlants::default(),
        writer_links: links,
        log_failed: false,
    })
}

/// [`pool_with_links`] with the real checkpoint over the in-memory disk,
/// whose floor one write does not cross: only a nudge or a force opens.
fn pool_with_links_and_checkpoint(policy: SyncPolicy, links: Vec<WriterLink>) -> ShardPool {
    let disk = MemDisk::default();
    disk.create_dir_all(Path::new(WAL)).unwrap();
    ShardPool::spawn_spec(PoolSpec {
        shards: 2,
        executors: 1,
        seed: SEED,
        trace: NoTrace,
        make_log: FileLog::new,
        policy: Deadlines,
        limit: MemoryLimit::default(),
        clock: frozen_clock,
        recovered: Vec::new(),
        make_checkpoint: move |executor| {
            SegmentCheckpoint::new(CheckpointSpec {
                disk: disk.clone(),
                wal: Path::new(WAL).to_path_buf(),
                generation: 1,
                executor,
                config: CheckpointConfig {
                    floor: 1 << 20,
                    ratio: 1,
                    bytes_per_tick: 1024,
                },
            })
        },
        sync: policy,
        plants: ExecutorPlants::default(),
        writer_links: links,
        log_failed: false,
    })
}

/// Lets every ready task run: the dispatching one, then the executor.
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
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

/// How many records a submission carries.
fn count_records(mut bytes: &[u8]) -> usize {
    let mut count = 0;
    while let Decoded::Record { consumed, .. } = decode_record(bytes) {
        count += 1;
        bytes = &bytes[consumed..];
    }
    count
}

/// Two short keys that land on the same shard of `shards`.
fn two_keys_on_one_shard(shards: u16) -> (Vec<u8>, Vec<u8>) {
    let first = b"k0".to_vec();
    let target = shard_of(&first, shards);
    let second = (1..10_000)
        .map(|i| format!("k{i}").into_bytes())
        .find(|key| shard_of(key, shards) == target)
        .expect("some key shares the shard");
    (first, second)
}

/// A short key on each of the disk pools' two executors (four shards).
fn keys_on_each_executor() -> (Vec<u8>, Vec<u8>) {
    let on = |executor: u16| {
        (0..10_000)
            .map(|i| format!("k{i}").into_bytes())
            .find(|key| executor_of(shard_of(key, 4), 4, 2) == executor)
            .expect("some key lands on each executor")
    };
    (on(0), on(1))
}

/// What the executors and the writer reported.
#[derive(Clone, Default)]
struct Story {
    faults: Arc<Mutex<Vec<LogFault>>>,
    log_faults: Arc<Mutex<Vec<LogFault>>>,
    held: Arc<Mutex<Vec<(u16, u64, bool)>>>,
    ended: Arc<Mutex<Vec<RefusalReport>>>,
}

impl Story {
    fn faults_of(&self, fault: LogFault) -> usize {
        let faults = self.faults.lock().expect("story");
        faults.iter().filter(|seen| **seen == fault).count()
    }

    fn log_faults(&self) -> Vec<LogFault> {
        self.log_faults.lock().expect("story").clone()
    }

    fn held_answered(&self) -> Vec<(u16, u64, bool)> {
        self.held.lock().expect("story").clone()
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
    fn log_fault(&self, fault: LogFault, _error: &std::io::Error) {
        self.log_faults.lock().expect("story").push(fault);
    }
    fn held_answered(&self, first_shard: u16, wrote: u64, refused: bool) {
        self.held
            .lock()
            .expect("story")
            .push((first_shard, wrote, refused));
    }
    fn refusal_ended(&self, report: &RefusalReport) {
        self.ended.lock().expect("story").push(*report);
    }
}

/// Under `always` a write's reply waits for the `Durable` that names its
/// batch; a read on the same executor is answered at once.
#[tokio::test(start_paused = true)]
async fn always_holds_a_write_until_its_batch_is_durable_and_never_a_read() {
    let (links, mut to_writer, progress) = fake_links(1);
    let pool = pool_with_links(SyncPolicy::ALWAYS, links);
    let write = tokio::spawn({
        let pool = pool.clone();
        async move { pool.dispatch(set(b"k", b"v")).await }
    });
    let Some(ToWriter::Submit { batch, bytes, .. }) = to_writer.recv().await else {
        panic!("a write is submitted")
    };
    assert_eq!(batch, 0);
    assert!(!bytes.is_empty());
    settle().await;
    assert!(!write.is_finished(), "held until durable");
    assert_eq!(
        pool.dispatch(get(b"k")).await,
        Reply::Bulk(Some(Bytes::from_static(b"v"))),
        "a read is served at once, and sees the write"
    );
    progress[0]
        .send(Progress::Durable {
            through_batch: Some(0),
            bytes: bytes.len() as u64,
            round: 1,
        })
        .unwrap();
    assert_eq!(write.await.unwrap(), Reply::Ok);
}

/// The writer's word is seen ahead of the inbox: under `always`, a write
/// whose batch is durable is answered before a backlog of reads queued
/// behind it is drained, not once the inbox happens to run dry.
#[tokio::test(start_paused = true)]
async fn a_durable_releases_its_write_ahead_of_a_queued_backlog() {
    const BACKLOG: usize = 5_000;
    let (links, mut to_writer, progress) = fake_links(1);
    let pool = pool_with_links(SyncPolicy::ALWAYS, links);
    let answered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let write = tokio::spawn({
        let pool = pool.clone();
        let answered = Arc::clone(&answered);
        async move {
            let reply = pool.dispatch(set(b"k", b"v")).await;
            (reply, answered.load(std::sync::atomic::Ordering::SeqCst))
        }
    });
    let Some(ToWriter::Submit { bytes, .. }) = to_writer.recv().await else {
        panic!("a write is submitted")
    };
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
    // Every read is queued before the executor next runs, and the batch is
    // durable in the same instant.
    progress[0]
        .send(Progress::Durable {
            through_batch: Some(0),
            bytes: bytes.len() as u64,
            round: 1,
        })
        .unwrap();
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

#[tokio::test(start_paused = true)]
async fn a_shard_named_twice_in_a_batch_stages_once() {
    let (links, mut to_writer, _progress) = fake_links(1);
    let pool = pool_with_links(SyncPolicy::INTERVAL, links);
    // Two writes to keys on the same shard, pipelined in one envelope.
    let (a, b) = two_keys_on_one_shard(2);
    let replies = pool.dispatch_many(vec![set(&a, b"1"), set(&b, b"2")]).await;
    assert_eq!(replies, vec![Reply::Ok, Reply::Ok]);
    let Some(ToWriter::Submit { bytes, .. }) = to_writer.recv().await else {
        panic!("a write is submitted")
    };
    assert_eq!(count_records(&bytes), 2, "both records, one submission");
    assert!(
        to_writer.try_recv().is_err(),
        "and no second submission for the shard's second command"
    );
}

#[tokio::test(start_paused = true)]
async fn a_durable_after_a_fault_raises_nothing() {
    let (links, mut to_writer, progress) = fake_links(1);
    let story = Story::default();
    let pool = pool_with_links_and_trace(SyncPolicy::ALWAYS, links, story.clone());
    let write = tokio::spawn({
        let pool = pool.clone();
        async move { pool.dispatch(set(b"k", b"v")).await }
    });
    let Some(ToWriter::Submit { bytes, .. }) = to_writer.recv().await else {
        panic!("a write is submitted")
    };
    progress[0].send(Progress::Fault).unwrap();
    assert_eq!(
        write.await.unwrap(),
        refused(),
        "the held write is answered with the refusal"
    );
    progress[0]
        .send(Progress::Durable {
            through_batch: Some(0),
            bytes: bytes.len() as u64,
            round: 1,
        })
        .unwrap();
    settle().await;
    assert_eq!(
        story.held_answered(),
        vec![(0, 1, true)],
        "nothing was released as success after the fault"
    );
    assert_eq!(
        pool.dispatch(set(b"k2", b"v")).await,
        refused(),
        "and the executor refuses until a snapshot lands"
    );
}

#[tokio::test(start_paused = true)]
async fn above_the_budget_the_inbox_waits_for_written() {
    let (links, mut to_writer, progress) = fake_links(1);
    let pool = pool_with_links(SyncPolicy::INTERVAL, links);
    let big = vec![b'x'; usize::try_from(WRITER_BUDGET / 2).unwrap() + 1];
    assert_eq!(pool.dispatch(set(b"a", &big)).await, Reply::Ok);
    assert_eq!(
        pool.dispatch(set(b"b", &big)).await,
        Reply::Ok,
        "acknowledged at once: the budget gates the next batch, not this one"
    );
    let third = tokio::spawn({
        let pool = pool.clone();
        async move { pool.dispatch(get(b"a")).await }
    });
    tokio::time::advance(Duration::from_millis(10)).await;
    settle().await;
    assert!(
        !third.is_finished(),
        "over the budget: the inbox is not read"
    );
    let mut written = 0;
    while let Ok(ToWriter::Submit { bytes, .. }) = to_writer.try_recv() {
        written += bytes.len() as u64;
    }
    progress[0]
        .send(Progress::Written { bytes: written })
        .unwrap();
    assert!(matches!(third.await.unwrap(), Reply::Bulk(Some(_))));
}

#[tokio::test(start_paused = true)]
async fn a_nudge_opens_a_cycle_and_its_completion_is_reported_covered() {
    let (links, mut to_writer, progress) = fake_links(1);
    let pool = pool_with_links_and_checkpoint(SyncPolicy::INTERVAL, links);
    assert_eq!(pool.dispatch(set(b"k", b"v")).await, Reply::Ok);
    let Some(ToWriter::Submit { batch, .. }) = to_writer.recv().await else {
        panic!("a write is submitted")
    };
    progress[0].send(Progress::Nudge).unwrap();
    let mut covered = None;
    for _ in 0..16 {
        tick(1).await;
        if let Ok(ToWriter::Covered {
            cycle,
            through_batch,
            ..
        }) = to_writer.try_recv()
        {
            covered = Some((cycle, through_batch));
            break;
        }
    }
    assert_eq!(covered, Some((0, Some(batch))));
}

/// A stop hands what is queued to the writer and waits for its last sync
/// — under every policy, `never` included.
#[tokio::test(start_paused = true)]
async fn shutdown_syncs_what_was_written_under_every_policy() {
    for policy in [SyncPolicy::ALWAYS, SyncPolicy::INTERVAL, SyncPolicy::NEVER] {
        let (disk, pool) = disk_pool(policy, NoTrace);
        assert_eq!(pool.dispatch(set(b"k", b"v")).await, Reply::Ok);
        assert_eq!(pool.shutdown().await, Shutdown::Clean);
        let path = Path::new(WAL).join(segment_name(1, 0));
        assert_eq!(
            disk.synced_len(&path),
            disk.contents(&path).len(),
            "{}: the stop synced",
            policy.name()
        );
    }
}

/// A writer that never answers the stop does not hold the process
/// hostage: the stop gives up after the grace period and says so.
#[tokio::test(start_paused = true)]
async fn shutdown_gives_up_after_the_grace_period() {
    let (links, _to_writer, _progress) = fake_links(1);
    let pool = pool_with_links(SyncPolicy::INTERVAL, links);
    assert_eq!(pool.dispatch(set(b"k", b"v")).await, Reply::Ok);
    let stopping = tokio::spawn({
        let pool = pool.clone();
        async move { pool.shutdown().await }
    });
    tokio::time::advance(SHUTDOWN_GRACE + Duration::from_millis(1)).await;
    assert_eq!(stopping.await.unwrap(), Shutdown::TimedOut);
}

#[tokio::test(start_paused = true)]
async fn a_failed_sync_refuses_the_node_and_each_executor_resumes_on_its_own_snapshot() {
    let story = Story::default();
    let (disk, pool) = disk_pool(SyncPolicy::ALWAYS, story.clone());
    let (on_zero, on_one) = keys_on_each_executor();
    assert_eq!(pool.dispatch(set(&on_zero, b"v")).await, Reply::Ok);
    disk.fail_next_sync();
    assert_eq!(pool.dispatch(set(&on_one, b"v")).await, refused());
    assert_eq!(
        pool.dispatch(set(&on_zero, b"w")).await,
        refused(),
        "the executor that wrote nothing in that round refuses too"
    );
    assert_eq!(
        pool.dispatch(get(&on_zero)).await,
        Reply::Bulk(Some(Bytes::from_static(b"v"))),
        "reads are served"
    );
    assert_eq!(
        story.log_faults(),
        vec![LogFault::Sync],
        "one line for the node"
    );
    // Both forced cycles image a tiny keyspace and land.
    tick(3).await;
    assert_eq!(
        story.refusals_ended().len(),
        2,
        "one `refusal_ended` per executor"
    );
    assert_eq!(pool.dispatch(set(&on_zero, b"w")).await, Reply::Ok);
    assert_eq!(pool.dispatch(set(&on_one, b"w")).await, Reply::Ok);
}

/// A start whose recovery cut a shard writes a rebase before it serves; if
/// that write fails, the node starts refusing, as on any failed write of
/// the log, and serves writes again once a snapshot lands.
#[tokio::test(start_paused = true)]
async fn a_start_whose_rebase_fails_refuses_writes_until_a_snapshot_lands() {
    let story = Story::default();
    let (disk, pool) = disk_pool_cut(SyncPolicy::INTERVAL, story.clone());
    assert_eq!(pool.dispatch(set(b"k", b"v")).await, refused());
    disk.fail_writes(false);
    tick(3).await;
    assert_eq!(pool.dispatch(set(b"k", b"v")).await, Reply::Ok);
}

/// A read in the same batch as a held write is served when the sync fails:
/// only the write's reply becomes the refusal.
#[tokio::test(start_paused = true)]
async fn a_read_held_beside_a_write_is_served_when_the_sync_fails() {
    let story = Story::default();
    let (disk, pool) = disk_pool(SyncPolicy::ALWAYS, story);
    let (k, r) = two_keys_on_one_shard(4);
    assert_eq!(pool.dispatch(set(&r, b"0")).await, Reply::Ok);
    disk.fail_syncs(true);
    assert_eq!(
        pool.dispatch_many(vec![set(&k, b"1"), get(&r)]).await,
        vec![refused(), Reply::Bulk(Some(Bytes::from_static(b"0")))],
        "the write is refused, the read beside it served"
    );
}

#[tokio::test(start_paused = true)]
async fn a_full_disk_refuses_writes_and_a_snapshot_resumes_them() {
    let story = Story::default();
    let (disk, pool) = disk_pool(SyncPolicy::INTERVAL, story.clone());
    assert_eq!(pool.dispatch(set(b"before", b"v")).await, Reply::Ok);
    disk.fail_writes_with(std::io::ErrorKind::StorageFull);
    assert_eq!(
        pool.dispatch(set(b"k", b"1")).await,
        Reply::Ok,
        "acknowledged: the write fails in the writer, and interval holds nothing"
    );
    settle().await;
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
    assert_eq!(
        story.refusals_ended().len(),
        2,
        "the node refused, and each executor ended its own"
    );
}

/// A stop whose last sync fails answers what it held with the refusal,
/// not as a success no sync stands behind.
#[tokio::test(start_paused = true)]
async fn a_stop_whose_last_sync_fails_answers_its_held_writes_with_the_refusal() {
    let (disk, pool) = disk_pool(SyncPolicy::ALWAYS, NoTrace);
    // The first round stays in flight long enough for the second write to
    // be written behind it.
    disk.set_sync_latency(Duration::from_millis(10));
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
    // The round in flight already took its answer; the next sync is the
    // stop's, for the second write.
    disk.fail_next_sync();
    let stopping = tokio::spawn({
        let pool = pool.clone();
        async move { pool.shutdown().await }
    });
    assert_eq!(first.await.unwrap(), Reply::Ok);
    assert_eq!(second.await.unwrap(), refused());
    assert_eq!(stopping.await.unwrap(), Shutdown::Clean);
}
