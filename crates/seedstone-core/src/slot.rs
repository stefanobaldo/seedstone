//! Maps a key to one of N virtual shards.
//!
//! The mapping is a pure, deterministic function of the key bytes: the same
//! key always resolves to the same shard for a fixed shard count. That value
//! is fixed across processes, builds and platforms.

/// Computes the CRC-16/XMODEM checksum of `data`.
///
/// Polynomial `0x1021`, initial value `0x0000`, no reflection of input or
/// output, no final XOR.
#[must_use]
pub fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0x0000;
    for &byte in data {
        crc ^= u16::from(byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// Maps `key` to a shard index in `0..shards`.
///
/// # Preconditions
///
/// `shards` must be greater than zero. Violating this is a programming
/// error: it panics, with the assert's message in debug builds or with
/// Rust's remainder-by-zero panic in release builds. It is checked with a
/// `debug_assert!` rather than propagated as a `Result` or `Option`.
#[must_use]
pub fn shard_of(key: &[u8], shards: u16) -> u16 {
    debug_assert!(shards > 0, "shard_of: shards must be greater than zero");
    crc16_xmodem(key) % shards
}

/// Which executor hosts `shard`, out of `executors`, when `shards` virtual
/// shards are partitioned into contiguous ranges.
///
/// The ranges are *defined by this function*: executor `e` owns exactly the
/// shards this maps to `e`. Monotonicity in `shard` is what makes each range
/// contiguous — the property the cluster's slot-range vocabulary wants — and
/// the test holds it, together with balance within one shard.
///
/// # Preconditions
///
/// `shards` must be greater than zero and `executors` must not exceed it, so
/// that every executor owns at least one shard. Callers establish both:
/// `ShardPool::spawn` asserts them once, at construction.
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    reason = "the quotient is < executors, which is a u16"
)]
pub const fn executor_of(shard: u16, shards: u16, executors: u16) -> u16 {
    ((shard as u32 * executors as u32) / shards as u32) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc16_matches_the_xmodem_check_value() {
        assert_eq!(crc16_xmodem(b"123456789"), 0x31C3);
    }

    #[test]
    fn executors_partition_the_shards_into_contiguous_balanced_ranges() {
        for (shards, executors) in [(1024u16, 10u16), (1024, 16), (16, 16), (1024, 1), (7, 3)] {
            let mut previous = 0u16;
            let mut owned = vec![0u32; usize::from(executors)];
            for shard in 0..shards {
                let executor = executor_of(shard, shards, executors);
                assert!(
                    executor < executors,
                    "{shards}/{executors}: executor out of range"
                );
                // Monotone in the shard index is what makes every range contiguous.
                assert!(
                    executor >= previous,
                    "{shards}/{executors}: map is not monotone"
                );
                previous = executor;
                owned[usize::from(executor)] += 1;
            }
            let min = owned.iter().min().copied().unwrap();
            let max = owned.iter().max().copied().unwrap();
            assert!(min > 0, "{shards}/{executors}: an executor owns no shard");
            assert!(
                max - min <= 1,
                "{shards}/{executors}: ranges differ by more than one shard"
            );
        }
    }

    #[test]
    fn shard_of_maps_keys_to_stable_shards() {
        // Golden vectors: these must never change — a simulation replay and a
        // production node have to agree on them forever. Derived independently
        // from a from-definition reference implementation, not by printing
        // what this crate returns — do not "fix" a mismatch by pasting in
        // whatever shard_of currently returns.
        let long_key: Vec<u8> = (0..100u8).collect();
        let vectors: [(&[u8], u16); 5] = [
            (b"foo", 918),
            (b"", 0),
            (b"key:1", 513),
            (b"\xff\xfe", 302),
            (long_key.as_slice(), 670),
        ];
        for (key, shard) in vectors {
            assert_eq!(shard_of(key, 1024), shard, "key {key:?}");
        }
        assert!(shard_of(b"foo", 16) < 16);
    }
}
