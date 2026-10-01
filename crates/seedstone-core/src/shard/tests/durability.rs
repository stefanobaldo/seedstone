//! The executor under each durability policy: when a reply leaves, when a
//! sync is issued, and what a completed or failed sync does to the point.

use super::support::{get, set};
use crate::dict::DictSeed;
use crate::log::checkpoint::NoCheckpoint;
use crate::log::disk::SyncFuture;
use crate::log::{Record, ReplicationLog};
use crate::memory::MemoryLimit;
use crate::shard::{
    Deadlines, ExecutorPlants, HOUSEKEEPING_TICK, NoTrace, PoolSpec, Reply, Router, ShardPool,
    SyncPolicy, frozen_clock,
};
use bytes::Bytes;
use std::collections::VecDeque;
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
