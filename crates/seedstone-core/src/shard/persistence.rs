//! What the node knows about its own persistence, as counters a monitor can
//! read.
//!
//! Written by the writer and the executors where the events already
//! happen, read by `INFO`. Nothing here is on the per-command path except
//! one `fetch_add` per appended record on the executor's own thread, and
//! every store is `Relaxed`: these are gauges and totals, not a protocol.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// The node's persistence counters, one set per node: the writer's, the
/// start's, and one [`ExecutorCell`] per executor.
#[derive(Debug, Default)]
pub struct PersistenceStats {
    /// Bytes of log retained on disk, every segment.
    pub log_bytes: AtomicU64,
    /// Segments on disk.
    pub log_segments: AtomicU64,
    /// `1` while a sync is in flight.
    pub sync_in_flight: AtomicU64,
    /// Syncs completed since start.
    pub syncs_total: AtomicU64,
    /// Syncs that were in flight past the writer's slow-sync threshold.
    pub delayed_syncs: AtomicU64,
    /// How long the last completed sync took.
    pub last_sync_micros: AtomicU64,
    /// Executors refusing writes now.
    pub refusing: AtomicU64,
    /// Shards the start reported lossy that no durable image has covered
    /// since.
    pub lossy_shards: AtomicU64,
    /// Keys the start recovered, images plus replay.
    pub keys_loaded: AtomicU64,
    pub executors: Box<[ExecutorCell]>,
}

/// One executor's share of the counters.
#[derive(Debug, Default)]
pub struct ExecutorCell {
    /// `1` while a snapshot cycle is open.
    pub cycle_open: AtomicU64,
    /// Durable images completed since start.
    pub saves: AtomicU64,
    /// Seconds since the Unix epoch of this executor's newest durable image;
    /// `0` until every one of its shards has one.
    pub last_save_unix: AtomicU64,
    /// Housekeeping ticks the last completed cycle took.
    pub last_save_ticks: AtomicU64,
    /// `1` if the last cycle ended in a fault rather than an image.
    pub last_save_failed: AtomicU64,
    /// Records appended since the last durable image.
    pub changes: AtomicU64,
}

impl PersistenceStats {
    #[must_use]
    pub fn new(executors: u16) -> Arc<Self> {
        Arc::new(Self {
            executors: (0..executors).map(|_| ExecutorCell::default()).collect(),
            ..Self::default()
        })
    }

    /// Oldest `last_save_unix` over executors; `None` while any is `0`.
    #[must_use]
    pub fn last_save_unix(&self) -> Option<u64> {
        self.executors
            .iter()
            .map(|cell| cell.last_save_unix.load(Ordering::Relaxed))
            .try_fold(u64::MAX, |oldest, at| (at != 0).then(|| oldest.min(at)))
            .filter(|_| !self.executors.is_empty())
    }

    #[must_use]
    pub fn saves(&self) -> u64 {
        self.sum(|cell| &cell.saves)
    }

    #[must_use]
    pub fn changes_since_save(&self) -> u64 {
        self.sum(|cell| &cell.changes)
    }

    #[must_use]
    pub fn any_cycle_open(&self) -> bool {
        self.executors
            .iter()
            .any(|cell| cell.cycle_open.load(Ordering::Relaxed) != 0)
    }

    /// The longest of the executors' last cycles, in ticks.
    #[must_use]
    pub fn last_save_ticks(&self) -> u64 {
        self.executors
            .iter()
            .map(|cell| cell.last_save_ticks.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0)
    }

    #[must_use]
    pub fn any_save_failed(&self) -> bool {
        self.executors
            .iter()
            .any(|cell| cell.last_save_failed.load(Ordering::Relaxed) != 0)
    }

    fn sum(&self, field: impl Fn(&ExecutorCell) -> &AtomicU64) -> u64 {
        self.executors
            .iter()
            .map(|cell| field(cell).load(Ordering::Relaxed))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn last_save_is_the_oldest_executor_and_absent_until_every_executor_has_one() {
        let stats = PersistenceStats::new(3);
        assert_eq!(stats.last_save_unix(), None);
        stats.executors[0]
            .last_save_unix
            .store(1_700_000_010, Ordering::Relaxed);
        stats.executors[2]
            .last_save_unix
            .store(1_700_000_005, Ordering::Relaxed);
        assert_eq!(stats.last_save_unix(), None, "executor 1 has no image yet");
        stats.executors[1]
            .last_save_unix
            .store(1_700_000_020, Ordering::Relaxed);
        assert_eq!(stats.last_save_unix(), Some(1_700_000_005));
        stats.executors[0].saves.store(2, Ordering::Relaxed);
        stats.executors[1].saves.store(1, Ordering::Relaxed);
        assert_eq!(stats.saves(), 3);
    }
}
