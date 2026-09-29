//! The snapshot file: an executor's image of its shards, in the log's own
//! record frame.
//!
//! ```text
//! <generation:016x>-<executor:04x>-<cycle:08x>.snap
//!
//! header   SSNP · version u8 · generation u64 · executor u16 · cycle u32
//!          · shards u16 · (shard u16, base u64) × shards · crc u32
//! entries  one record per key: shard, seq = the shard's base, payload Put
//! footer   one record: shard 0xFFFF, seq = cycle,
//!          payload (shard u16, entries u64) × shards
//! end      the 0x00 marker
//! ```
//!
//! Entries are ordinary records so the hole-tolerant reader reads a
//! snapshot with no new line, and a `Put` is what an entry is: the value a
//! key holds and its absolute deadline. The **footer is the commit**: a
//! file without one is unfinished and ignored; one whose counts do not
//! match what was read is damaged and refused. The bases live in the header
//! because they are known when the cycle opens, and recovery reads them
//! from the first chunk without reading the file.
//!
//! The image is *fuzzy*: a shard's entries are taken over many ticks while
//! the shard keeps serving, so the file is not the state at any instant.
//! It does not need to be. Every effect in the log is absolute, so the tail
//! from the base replayed over any image taken after it is the state — the
//! reason the log records effects and not commands, cashed in here.

use crate::log::effect::Effect;
use crate::log::file::{FORMAT_VERSION, HeaderError, check_header_crc, parse_name};
use crate::log::{Record, crc32_iso_hdlc, encode_record};

/// The four bytes every snapshot starts with.
pub const SNAPSHOT_MAGIC: [u8; 4] = *b"SSNP";

/// The shard id the footer record carries: no shard, since a node runs at
/// most `u16::MAX - 1` of them and the deployed count is 1024. A segment
/// reader that meets it counts the record malformed.
pub const FOOTER_SHARD: u16 = u16::MAX;

/// Magic, version, generation, executor, cycle, shard count: what has to
/// be read before the header's length is known.
pub const SNAPSHOT_HEADER_FIXED_LEN: usize = 4 + 1 + 8 + 2 + 4 + 2;

/// The name of `executor`'s snapshot of `cycle` in `generation`.
#[must_use]
pub fn snapshot_name(generation: u64, executor: u16, cycle: u32) -> String {
    format!("{generation:016x}-{executor:04x}-{cycle:08x}.snap")
}

/// The generation, executor and cycle a snapshot name carries, if it is
/// one.
#[must_use]
pub fn parse_snapshot_name(name: &str) -> Option<(u64, u16, u32)> {
    parse_name(name, ".snap")
}

/// What a snapshot's header says: whose image it is, and each shard's
/// base — the sequence the tail resumes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotHeader {
    pub generation: u64,
    pub executor: u16,
    pub cycle: u32,
    /// `(shard, base)` for every shard the executor owned, in shard order.
    pub bases: Vec<(u16, u64)>,
}

impl SnapshotHeader {
    /// How many bytes [`encode`](Self::encode) appends.
    #[must_use]
    pub const fn encoded_len(&self) -> usize {
        SNAPSHOT_HEADER_FIXED_LEN + self.bases.len() * (2 + 8) + 4
    }

    /// Appends this header to `out`: the fields, then a CRC over them.
    ///
    /// # Panics
    ///
    /// If more than `u16::MAX` shards are listed, which no node runs.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let at = out.len();
        out.extend_from_slice(&SNAPSHOT_MAGIC);
        out.push(FORMAT_VERSION);
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&self.executor.to_le_bytes());
        out.extend_from_slice(&self.cycle.to_le_bytes());
        let shards = u16::try_from(self.bases.len()).expect("a node's shard count fits a u16");
        out.extend_from_slice(&shards.to_le_bytes());
        for (shard, base) in &self.bases {
            out.extend_from_slice(&shard.to_le_bytes());
            out.extend_from_slice(&base.to_le_bytes());
        }
        let crc = crc32_iso_hdlc(&out[at..]);
        out.extend_from_slice(&crc.to_le_bytes());
    }

    /// The whole header's length, from its fixed part alone: what a reader
    /// needs before it can read the rest.
    ///
    /// # Errors
    ///
    /// `Short`, `BadMagic` or `NewerVersion`; the checksum is not checked
    /// here, because the bytes it covers have not been read yet.
    pub fn header_len(fixed: &[u8]) -> Result<usize, HeaderError> {
        let Some(fixed) = fixed.get(..SNAPSHOT_HEADER_FIXED_LEN) else {
            return Err(HeaderError::Short);
        };
        if fixed[..4] != SNAPSHOT_MAGIC {
            return Err(HeaderError::BadMagic);
        }
        if fixed[4] > FORMAT_VERSION {
            return Err(HeaderError::NewerVersion(fixed[4]));
        }
        let shards = usize::from(u16::from_le_bytes([fixed[19], fixed[20]]));
        Ok(SNAPSHOT_HEADER_FIXED_LEN + shards * 10 + 4)
    }

    /// Reads a header from the start of `buf`.
    ///
    /// # Errors
    ///
    /// [`HeaderError`], as each variant says.
    pub fn decode(buf: &[u8]) -> Result<Self, HeaderError> {
        let len = Self::header_len(buf)?;
        let Some(header) = buf.get(..len) else {
            return Err(HeaderError::Short);
        };
        check_header_crc(header)?;
        let mut generation = [0; 8];
        generation.copy_from_slice(&header[5..13]);
        let executor = u16::from_le_bytes([header[13], header[14]]);
        let mut cycle = [0; 4];
        cycle.copy_from_slice(&header[15..19]);
        let shards = usize::from(u16::from_le_bytes([header[19], header[20]]));
        let mut bases = Vec::with_capacity(shards);
        let mut at = SNAPSHOT_HEADER_FIXED_LEN;
        for _ in 0..shards {
            let shard = u16::from_le_bytes([header[at], header[at + 1]]);
            let mut base = [0; 8];
            base.copy_from_slice(&header[at + 2..at + 10]);
            bases.push((shard, u64::from_le_bytes(base)));
            at += 10;
        }
        Ok(Self {
            generation: u64::from_le_bytes(generation),
            executor,
            cycle: u32::from_le_bytes(cycle),
            bases,
        })
    }
}

/// Appends one entry: a record on `shard` at `base` whose payload is the
/// `Put` of `key`. `scratch` holds the payload between the two encodings
/// and is reused across entries so a cycle allocates it once.
pub fn encode_entry(
    shard: u16,
    base: u64,
    key: &[u8],
    value: &[u8],
    deadline: Option<u64>,
    scratch: &mut Vec<u8>,
    out: &mut Vec<u8>,
) {
    scratch.clear();
    Effect::Put {
        key,
        value,
        deadline,
    }
    .encode(scratch);
    encode_record(
        &Record {
            shard,
            seq: base,
            payload: scratch,
        },
        out,
    );
}

/// The snapshot's commit: how many entries each shard has in the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Footer {
    /// `(shard, entries)` in shard order, one per shard the header lists.
    pub counts: Vec<(u16, u64)>,
}

impl Footer {
    /// Appends the footer as a record on [`FOOTER_SHARD`] at `cycle`.
    pub fn encode_record(&self, cycle: u32, out: &mut Vec<u8>) {
        let mut payload = Vec::with_capacity(self.counts.len() * 10);
        for (shard, entries) in &self.counts {
            payload.extend_from_slice(&shard.to_le_bytes());
            payload.extend_from_slice(&entries.to_le_bytes());
        }
        encode_record(
            &Record {
                shard: FOOTER_SHARD,
                seq: u64::from(cycle),
                payload: &payload,
            },
            out,
        );
    }

    /// Decodes a footer payload, or `None` if it is not one.
    #[must_use]
    pub fn decode(payload: &[u8]) -> Option<Self> {
        // Empty is not a footer: every executor owns at least one shard.
        if payload.is_empty() || !payload.len().is_multiple_of(10) {
            return None;
        }
        let counts = payload
            .chunks_exact(10)
            .map(|chunk| {
                let shard = u16::from_le_bytes([chunk[0], chunk[1]]);
                let mut entries = [0; 8];
                entries.copy_from_slice(&chunk[2..10]);
                (shard, u64::from_le_bytes(entries))
            })
            .collect();
        Some(Self { counts })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{Decoded, decode_record};

    #[test]
    fn snapshot_names_carry_the_cycle_and_sort_after_older_generations() {
        assert_eq!(
            snapshot_name(3, 1, 7),
            "0000000000000003-0001-00000007.snap"
        );
        assert_eq!(
            parse_snapshot_name("0000000000000003-0001-00000007.snap"),
            Some((3, 1, 7))
        );
        assert_eq!(
            parse_snapshot_name("0000000000000003-0001-00000007.seg"),
            None
        );
        let mut names = vec![
            snapshot_name(2, 0, 0),
            snapshot_name(1, 0, 9),
            snapshot_name(1, 0, 3),
        ];
        names.sort();
        assert_eq!(
            names,
            [
                snapshot_name(1, 0, 3),
                snapshot_name(1, 0, 9),
                snapshot_name(2, 0, 0)
            ]
        );
    }

    #[test]
    fn the_header_round_trips_with_its_bases_and_refuses_damage() {
        let header = SnapshotHeader {
            generation: 4,
            executor: 2,
            cycle: 11,
            bases: vec![(200, 15), (201, 0), (202, 7_000_000)],
        };
        let mut out = Vec::new();
        header.encode(&mut out);
        assert_eq!(out.len(), header.encoded_len());
        assert_eq!(
            SnapshotHeader::header_len(&out[..SNAPSHOT_HEADER_FIXED_LEN]),
            Ok(out.len())
        );
        assert_eq!(SnapshotHeader::decode(&out), Ok(header));
        assert_eq!(SnapshotHeader::decode(&out[..10]), Err(HeaderError::Short));
        let mut bad = out.clone();
        bad[1] = b'X';
        assert_eq!(SnapshotHeader::decode(&bad), Err(HeaderError::BadMagic));
        let mut flipped = out.clone();
        flipped[SNAPSHOT_HEADER_FIXED_LEN + 3] ^= 0x10; // inside the first base
        assert_eq!(
            SnapshotHeader::decode(&flipped),
            Err(HeaderError::BadChecksum)
        );
        let mut newer = out;
        newer[4] = FORMAT_VERSION + 1;
        assert_eq!(
            SnapshotHeader::decode(&newer),
            Err(HeaderError::NewerVersion(FORMAT_VERSION + 1))
        );
    }

    #[test]
    fn an_entry_is_a_put_record_at_the_shards_base() {
        let mut scratch = Vec::new();
        let mut out = Vec::new();
        encode_entry(
            7,
            42,
            b"key",
            b"value",
            Some(1_700_000_000_000),
            &mut scratch,
            &mut out,
        );
        let Decoded::Record {
            shard,
            seq,
            payload,
            consumed,
        } = decode_record(&out)
        else {
            panic!("an entry is one intact record")
        };
        assert_eq!((shard, seq, consumed), (7, 42, out.len()));
        assert_eq!(
            Effect::decode(payload),
            Some(Effect::Put {
                key: b"key",
                value: b"value",
                deadline: Some(1_700_000_000_000)
            })
        );
        // The scratch is reused, not regrown: a second entry starts it over.
        encode_entry(7, 42, b"k2", b"", None, &mut scratch, &mut out);
        assert_eq!(
            scratch.len(),
            Effect::Put {
                key: b"k2",
                value: b"",
                deadline: None
            }
            .encoded_len()
        );
    }

    #[test]
    fn the_footer_is_a_record_on_the_footer_shard_and_round_trips_its_counts() {
        let footer = Footer {
            counts: vec![(200, 3), (201, 0)],
        };
        let mut out = Vec::new();
        footer.encode_record(11, &mut out);
        let Decoded::Record {
            shard,
            seq,
            payload,
            ..
        } = decode_record(&out)
        else {
            panic!("a footer is one intact record")
        };
        assert_eq!((shard, seq), (FOOTER_SHARD, 11));
        assert_eq!(Footer::decode(payload), Some(footer));
        assert_eq!(
            Footer::decode(&payload[..payload.len() - 1]),
            None,
            "short is malformed"
        );
        assert_eq!(Footer::decode(&[]), None);
        let mut trailing = payload.to_vec();
        trailing.push(0);
        assert_eq!(
            Footer::decode(&trailing),
            None,
            "trailing bytes are malformed"
        );
    }
}
