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

/// What a node does when the gauge passes the ceiling.
///
/// Two modes and not Redis's eight. The `volatile-*` family reclaims only
/// from keys that carry a deadline, and a keyspace that carries none — which
/// is the shape this server is built for — would answer every write over the
/// ceiling with a refusal while holding a keyspace it was told it could
/// evict. Offering a policy that evicts nothing would be offering a ceiling
/// that holds nothing. `allkeys-random` is the one that could exist and does
/// not: it is strictly worse than sampled LRU at the same cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionMode {
    /// Remove the least recently touched of a sample from the shard that is
    /// writing, until the write fits. Redis's `allkeys-lru`.
    #[default]
    AllKeysLru,
    /// Refuse the write. Redis's `noeviction`.
    NoEviction,
}

impl EvictionMode {
    /// The name Redis uses, which `CONFIG GET maxmemory-policy` reports and
    /// the command line accepts.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::AllKeysLru => "allkeys-lru",
            Self::NoEviction => "noeviction",
        }
    }

    /// The mode `name` selects, if it names one.
    ///
    /// Written over the list rather than as a `match` on strings so that the
    /// two directions cannot drift: a mode added later is spelled once, in
    /// [`name`](Self::name), and parsing follows.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        [Self::AllKeysLru, Self::NoEviction]
            .into_iter()
            .find(|mode| mode.name() == name)
    }
}

/// The ceiling and what to do at it. `None` is no ceiling, Redis's default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MemoryLimit {
    /// `maxmemory`, or `None` for unbounded.
    pub ceiling: Option<u64>,
    /// `maxmemory-policy`.
    pub mode: EvictionMode,
}

impl MemoryLimit {
    /// Whether `used` is past the ceiling. `false` with no ceiling.
    ///
    /// Strictly past, as Redis compares it: a node holding exactly
    /// `maxmemory` bytes is at its ceiling and not over it.
    #[must_use]
    pub fn exceeded(self, used: u64) -> bool {
        self.ceiling.is_some_and(|ceiling| used > ceiling)
    }
}
