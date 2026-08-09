//! Maps a key to one of N virtual shards.
//!
//! The mapping is a pure, deterministic function of the key bytes: the same
//! key always resolves to the same shard for a fixed shard count. That value
//! is fixed across processes, builds and platforms.

/// Computes the CRC-16/XMODEM checksum of `data`.
///
/// Polynomial `0x1021`, initial value `0x0000`, no reflection of input or
/// output, no final XOR.
pub fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0x0000;
    for &byte in data {
        crc ^= (byte as u16) << 8;
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
pub fn shard_of(key: &[u8], shards: u16) -> u16 {
    debug_assert!(shards > 0, "shard_of: shards must be greater than zero");
    crc16_xmodem(key) % shards
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc16_matches_the_xmodem_check_value() {
        assert_eq!(crc16_xmodem(b"123456789"), 0x31C3);
    }

    #[test]
    fn shard_of_maps_keys_to_stable_shards() {
        // Golden vectors: these must never change — a simulation replay and a
        // production node have to agree on them forever. Derived independently
        // from a from-definition reference implementation, not by printing
        // what this crate returns — do not "fix" a mismatch by pasting in
        // whatever shard_of currently returns.
        let long_key: Vec<u8> = (0..100).map(|i: u32| (i % 256) as u8).collect();
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
