//! What the node's keyspace is accounted at, across every shard.
//!
//! One number, kept by the executors and read by whoever answers `INFO` and
//! by the eviction decision. It is a *sum of formulas* — each dict reports
//! what it is accounted at and the executor applies the difference after
//! every command — and never an allocator reading, so a replay reproduces it
//! byte for byte.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// The bytes in use across every shard, as the dicts account them.
///
/// Shared between the executors and the edge. Relaxed ordering throughout:
/// nothing is published through this word, and a reader that sees a figure
/// one command stale is reading a gauge, which is what a gauge is.
#[derive(Clone, Debug, Default)]
pub struct MemoryGauge(Arc<AtomicU64>);

impl MemoryGauge {
    /// The figure right now.
    #[must_use]
    pub fn used(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Applies a dict's change in accounted size: `after - before`, in
    /// whichever direction it went. One atomic operation either way, and none
    /// at all when nothing moved — the read path's case.
    pub fn apply(&self, before: u64, after: u64) {
        if after > before {
            self.0.fetch_add(after - before, Ordering::Relaxed);
        } else if before > after {
            self.0.fetch_sub(before - after, Ordering::Relaxed);
        }
    }
}
