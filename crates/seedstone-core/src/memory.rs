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
    #[must_use]
    pub const fn exceeded(self, used: u64) -> bool {
        past_ceiling(used, self.ceiling)
    }
}

/// Whether `used` is past `ceiling`. `false` with no ceiling.
///
/// Strictly past, as Redis compares it: a node holding exactly `maxmemory`
/// bytes is at its ceiling and not over it.
///
/// A free function because two callers ask the question from different
/// vocabularies — [`MemoryLimit::exceeded`] from the whole limit, and the
/// shard's `EvictionPolicy` from a bare `Option<u64>` it is handed — and the
/// comparison is the one place a `>` could quietly become a `>=` in one of
/// them. `noeviction` refuses a write and `allkeys-lru` reclaims for it at
/// exactly the same byte either way.
#[must_use]
pub const fn past_ceiling(used: u64, ceiling: Option<u64>) -> bool {
    match ceiling {
        Some(ceiling) => used > ceiling,
        None => false,
    }
}

/// Reads a byte size the way `redis.conf` documents one.
///
/// A bare integer is bytes. A suffix scales it, and the distinction Redis
/// draws is honoured exactly: `k` is 1000 and `kb` is 1024, `m` is a million
/// and `mb` is a mebibyte, `g` a billion and `gb` a gibibyte. The suffix is
/// case-insensitive. Anything else — a sign, a fraction, a space before the
/// suffix, a figure that overflows — is no size at all.
///
/// The two-letter suffixes are tried first, so `kb` is never read as `k`
/// followed by a stray `b`.
#[must_use]
pub fn parse_bytes(text: &str) -> Option<u64> {
    /// Longest suffix first, so `kb` wins over `k`.
    const SUFFIXES: [(&str, u64); 7] = [
        ("gb", 1 << 30),
        ("mb", 1 << 20),
        ("kb", 1 << 10),
        ("g", 1_000_000_000),
        ("m", 1_000_000),
        ("k", 1000),
        ("b", 1),
    ];
    let lowered = text.to_ascii_lowercase();
    let (digits, scale) = SUFFIXES
        .into_iter()
        .find_map(|(suffix, scale)| Some((lowered.strip_suffix(suffix)?, scale)))
        .unwrap_or((lowered.as_str(), 1));
    digits.parse::<u64>().ok()?.checked_mul(scale)
}

#[cfg(test)]
mod tests {
    use super::{EvictionMode, MemoryLimit, parse_bytes};

    #[test]
    fn byte_sizes_parse_as_redis_conf_documents_them() {
        assert_eq!(parse_bytes("1024"), Some(1024));
        assert_eq!(parse_bytes("1k"), Some(1000));
        assert_eq!(parse_bytes("1kb"), Some(1024));
        assert_eq!(parse_bytes("64MB"), Some(64 * 1024 * 1024));
        assert_eq!(parse_bytes("1gb"), Some(1 << 30));
        assert_eq!(parse_bytes("2g"), Some(2_000_000_000));
        assert_eq!(parse_bytes("0"), Some(0));
        for bad in ["", "-1", "1.5gb", "1 gb", "gb", "99999999999999999999gb"] {
            assert_eq!(parse_bytes(bad), None, "{bad:?}");
        }
    }

    /// The two directions of a mode's name agree, over the whole set rather
    /// than over the one that happens to be convenient.
    #[test]
    fn every_mode_parses_back_from_the_name_it_prints() {
        for mode in [EvictionMode::AllKeysLru, EvictionMode::NoEviction] {
            assert_eq!(EvictionMode::from_name(mode.name()), Some(mode));
        }
        assert_eq!(EvictionMode::from_name("volatile-lru"), None);
        assert_eq!(EvictionMode::from_name(""), None);
    }

    /// The ceiling is a ceiling, not a threshold: a node holding exactly
    /// `maxmemory` is at it and not over it.
    #[test]
    fn a_limit_is_exceeded_only_strictly_past_its_ceiling() {
        let limit = MemoryLimit {
            ceiling: Some(100),
            mode: EvictionMode::AllKeysLru,
        };
        assert!(!limit.exceeded(99));
        assert!(!limit.exceeded(100));
        assert!(limit.exceeded(101));
        assert!(!MemoryLimit::default().exceeded(u64::MAX));
    }
}
