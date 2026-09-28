//! Start-up: read every segment, bucket the records by shard, and hand each
//! shard the gapless prefix of its sequence.
//!
//! **The prefix rule.** A shard applies sequence `0, 1, 2, …` and stops at
//! the first number that is missing; everything of that shard with a higher
//! sequence, in any segment, is discarded and counted. Replaying past a gap
//! would produce a state the shard never held. What the server does with a
//! gap is serve the prefix and say so, per shard — a cache that refused to
//! start over damage it can absorb would be down for longer than the
//! damage costs.
//!
//! **Segments in name order** — generation, then executor — so a shard's
//! records arrive in sequence order even when it moved executors between
//! processes; they are sorted by sequence afterwards anyway, because a
//! segment abandoned mid-way or read out of order must not turn into a
//! wrong prefix.
//!
//! A segment whose header names a format version above this build's is
//! refused, and the node does not start: that is not damage, it is a
//! downgrade, and guessing at it would be worse than stopping. A header
//! that is short or carries the wrong magic is damage: the segment is
//! abandoned and counted, and every shard is marked lossy, because a loss
//! nobody can attribute to a shard is a possible loss for all of them.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use crate::log::disk::Disk;
use crate::log::effect::{Effect, Owned};
use crate::log::file::{
    HeaderError, SEGMENT_HEADER_LEN, decode_segment_header, parse_segment_name,
};
use crate::log::reader::{Item, Reader};

/// Re-exported: every caller of [`recover`] chooses one.
pub use crate::log::reader::ReaderMode;
use crate::slot::executor_of;

/// What one shard gets back: its gapless prefix, and what was cut.
#[derive(Debug, Default)]
pub struct ShardRecords {
    /// `(seq, effect)` from `0` upwards with no gap.
    pub records: Vec<(u64, Owned)>,
    /// Records of this shard discarded after the first gap.
    pub discarded: u64,
    /// Whether any of this shard's records may have been lost: a gap, damage
    /// in a segment its executor wrote, or a segment nobody could read.
    pub lossy: bool,
}

/// One shard the report names as cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardTruncation {
    pub shard: u16,
    /// Records applied before the gap.
    pub applied: u64,
    /// Records discarded after it.
    pub discarded: u64,
}

/// What recovery did, for the node's log line.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Report {
    pub segments: u64,
    pub records: u64,
    pub applied: u64,
    pub discarded: u64,
    pub malformed: u64,
    pub damage_bytes: u64,
    pub holes: u64,
    pub abandoned_segments: u64,
    pub truncated: Vec<ShardTruncation>,
}

/// Every shard's prefix, and the report.
#[derive(Debug, Default)]
pub struct Recovery {
    pub shards: Vec<ShardRecords>,
    pub report: Report,
}

/// Reads `wal` and hands each of `shards` shards its gapless prefix.
///
/// # Errors
///
/// The directory cannot be listed, or a segment names a format version
/// above this build's — the two cases where the node must not start.
/// Everything else is damage, counted and survived.
pub fn recover<D: Disk>(
    disk: &D,
    wal: &Path,
    shards: u16,
    mode: ReaderMode,
) -> io::Result<Recovery> {
    let mut names: Vec<(u64, u16, String)> = disk
        .list(wal)?
        .into_iter()
        .filter_map(|name| parse_segment_name(&name).map(|(g, e)| (g, e, name)))
        .collect();
    names.sort();
    // How many executors each generation ran: the segment names say, and a
    // shard's executor in that generation follows from it — which is what
    // lets damage in one segment be charged to the shards that wrote there.
    let mut executors_in: BTreeMap<u64, u16> = BTreeMap::new();
    for (generation, executor, _) in &names {
        let count = executors_in.entry(*generation).or_default();
        *count = (*count).max(executor.saturating_add(1));
    }

    let mut scan = Scan {
        report: Report::default(),
        buckets: (0..shards).map(|_| Vec::new()).collect(),
        damaged: vec![false; usize::from(shards)],
        unattributed_loss: false,
    };
    for (generation, executor, name) in names {
        let executors = executors_in.get(&generation).copied().unwrap_or(1);
        scan.segment(disk, wal, &name, mode, (executor, executors))?;
    }
    Ok(scan.finish())
}

/// What recovery has read so far.
struct Scan {
    report: Report,
    /// Each shard's intact records, in the order they were read.
    buckets: Vec<Vec<(u64, Owned)>>,
    /// Per shard: damage sat in a segment its executor wrote.
    damaged: Vec<bool>,
    /// A loss nobody can attribute to a shard: every shard may have paid.
    unattributed_loss: bool,
}

impl Scan {
    /// Reads one segment into the buckets. `writer` is the executor that
    /// wrote it and how many its generation ran.
    fn segment<D: Disk>(
        &mut self,
        disk: &D,
        wal: &Path,
        name: &str,
        mode: ReaderMode,
        writer: (u16, u16),
    ) -> io::Result<()> {
        self.report.segments += 1;
        let path = wal.join(name);
        let (Ok(len), Ok(mut src)) = (disk.len(&path), disk.open_read(&path)) else {
            self.abandon();
            return Ok(());
        };
        let mut header = [0u8; SEGMENT_HEADER_LEN];
        let read = read_fully(&mut src, &mut header);
        match decode_segment_header(&header[..read]) {
            Ok(_) => {}
            Err(HeaderError::NewerVersion(version)) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("{name}: format version {version} is newer than this build reads"),
                ));
            }
            Err(HeaderError::Short | HeaderError::BadMagic) => {
                self.abandon();
                return Ok(());
            }
        }
        let body_len = len.saturating_sub(SEGMENT_HEADER_LEN as u64);
        let mut reader = Reader::new(src, body_len, mode);
        loop {
            match reader.next_record() {
                Ok(Some(item)) => self.item(&item),
                Ok(None) => break,
                Err(_) => {
                    self.abandon();
                    break;
                }
            }
        }
        let damage = reader.damage();
        self.report.damage_bytes += damage.bytes + damage.truncated_tail;
        self.report.holes += damage.holes;
        if damage.abandoned {
            self.abandon();
        }
        // A hole can swallow a shard's last records, leaving no gap behind
        // to show for it; a cut tail can be a damaged length on the last
        // record rather than a crash. Either may have cost any shard this
        // segment's executor hosted.
        if damage.holes > 0 || damage.truncated_tail > 0 {
            let (executor, executors) = writer;
            let shards = u16::try_from(self.damaged.len()).unwrap_or(u16::MAX);
            for (shard, hit) in (0..shards).zip(self.damaged.iter_mut()) {
                *hit |= executor_of(shard, shards, executors) == executor;
            }
        }
        Ok(())
    }

    /// One intact record, bucketed by shard.
    fn item(&mut self, item: &Item) {
        self.report.records += 1;
        let Some(bucket) = self.buckets.get_mut(usize::from(item.shard)) else {
            self.report.malformed += 1;
            return;
        };
        match Effect::decode(&item.payload) {
            Some(effect) => bucket.push((item.seq, effect.to_owned())),
            // A well-checksummed record this build cannot read ends the
            // shard's prefix where it sits: leaving its sequence out of the
            // bucket is exactly a gap.
            None => self.report.malformed += 1,
        }
    }

    /// A segment, or the rest of one, that could not be read.
    const fn abandon(&mut self) {
        self.report.abandoned_segments += 1;
        self.unattributed_loss = true;
    }

    /// Cuts every shard's bucket to its gapless prefix.
    fn finish(mut self) -> Recovery {
        let mut shards = Vec::with_capacity(self.buckets.len());
        for (shard, (bucket, damaged)) in (0u16..).zip(self.buckets.into_iter().zip(self.damaged)) {
            let (records, discarded) = prefix(bucket);
            let gap = discarded > 0;
            self.report.applied += records.len() as u64;
            self.report.discarded += discarded;
            if gap {
                self.report.truncated.push(ShardTruncation {
                    shard,
                    applied: records.len() as u64,
                    discarded,
                });
            }
            shards.push(ShardRecords {
                records,
                discarded,
                lossy: gap || damaged || self.unattributed_loss,
            });
        }
        Recovery {
            shards,
            report: self.report,
        }
    }
}

/// A shard's records from `0` upwards up to the first missing sequence,
/// and how many came after it.
fn prefix(mut bucket: Vec<(u64, Owned)>) -> (Vec<(u64, Owned)>, u64) {
    bucket.sort_by_key(|(seq, _)| *seq);
    let mut records = Vec::with_capacity(bucket.len());
    let mut expected = 0u64;
    let mut discarded = 0u64;
    for (seq, effect) in bucket {
        if discarded > 0 || seq > expected {
            discarded += 1;
        } else if seq == expected {
            records.push((seq, effect));
            expected += 1;
        }
        // Below `expected`: the same record written twice, by a write that
        // failed part-way and was retried whole.
    }
    (records, discarded)
}

fn read_fully<R: io::Read>(src: &mut R, buf: &mut [u8]) -> usize {
    let mut got = 0;
    while got < buf.len() {
        match src.read(&mut buf[got..]) {
            Ok(0) | Err(_) => break,
            Ok(n) => got += n,
        }
    }
    got
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::disk::mem::MemDisk;
    use crate::log::effect::Effect;
    use crate::log::file::{
        FORMAT_VERSION, FileLog, SEGMENT_HEADER_LEN, encode_segment_header, open_segments,
        segment_name,
    };
    use crate::log::{Record, ReplicationLog};
    use std::sync::Arc;

    fn put(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        Effect::Put {
            key,
            value,
            deadline: None,
        }
        .encode(&mut out);
        out
    }

    /// A wal directory with one generation of `executors` segments, and the
    /// logs to write into them.
    fn wal(
        disk: &MemDisk,
        generation: u64,
        executors: u16,
    ) -> Vec<Arc<std::sync::Mutex<crate::log::file::Segment<crate::log::disk::mem::MemFile>>>> {
        let dir = Path::new("/data/wal");
        disk.create_dir_all(dir).unwrap();
        open_segments(disk, dir, generation, executors).unwrap()
    }

    fn write(log: &mut FileLog<crate::log::disk::mem::MemFile>, seq: u64, payload: &[u8]) {
        log.append(Record {
            shard: log.shard(),
            seq,
            payload,
        })
        .unwrap();
        log.flush().unwrap();
        log.sync().unwrap();
    }

    #[test]
    fn records_are_recovered_per_shard_in_sequence_order_across_segments() {
        let disk = MemDisk::default();
        // Generation 1: shard 0 on executor 0, shard 1 on executor 1.
        let segments = wal(&disk, 1, 2);
        let mut s0 = FileLog::new(0, Arc::clone(&segments[0]));
        let mut s1 = FileLog::new(1, Arc::clone(&segments[1]));
        write(&mut s0, 0, &put(b"a", b"1"));
        write(&mut s1, 0, &put(b"b", b"1"));
        write(&mut s0, 1, &put(b"a", b"2"));
        // Generation 2: one executor, both shards on it.
        let later = open_segments(&disk, Path::new("/data/wal"), 2, 1).unwrap();
        let mut s0 = FileLog::new(0, Arc::clone(&later[0]));
        let mut s1 = FileLog::new(1, Arc::clone(&later[0]));
        write(&mut s1, 1, &put(b"b", b"2"));
        write(&mut s0, 2, &put(b"a", b"3"));

        let recovery = recover(
            &disk,
            Path::new("/data/wal"),
            2,
            ReaderMode::Resynchronising,
        )
        .unwrap();
        assert_eq!(recovery.report.segments, 3);
        assert_eq!(recovery.report.records, 5);
        assert_eq!(recovery.report.applied, 5);
        let seqs = |shard: usize| -> Vec<u64> {
            recovery.shards[shard]
                .records
                .iter()
                .map(|(seq, _)| *seq)
                .collect()
        };
        assert_eq!(seqs(0), [0, 1, 2]);
        assert_eq!(seqs(1), [0, 1]);
        assert!(recovery.report.truncated.is_empty());
        assert!(!recovery.shards[0].lossy);
    }

    #[test]
    fn a_gap_truncates_that_shard_and_nothing_else() {
        let disk = MemDisk::default();
        let segments = wal(&disk, 1, 1);
        let mut s0 = FileLog::new(0, Arc::clone(&segments[0]));
        let mut s1 = FileLog::new(1, Arc::clone(&segments[0]));
        write(&mut s0, 0, &put(b"a", b"1"));
        write(&mut s0, 1, &put(b"a", b"2"));
        write(&mut s1, 0, &put(b"b", b"1"));
        write(&mut s0, 3, &put(b"a", b"4")); // seq 2 never written
        write(&mut s1, 1, &put(b"b", b"2"));

        let recovery = recover(
            &disk,
            Path::new("/data/wal"),
            2,
            ReaderMode::Resynchronising,
        )
        .unwrap();
        assert_eq!(recovery.shards[0].records.len(), 2);
        assert_eq!(recovery.shards[0].discarded, 1);
        assert!(recovery.shards[0].lossy);
        assert_eq!(recovery.shards[1].records.len(), 2);
        assert!(!recovery.shards[1].lossy);
        assert_eq!(recovery.report.truncated.len(), 1);
        assert_eq!(recovery.report.truncated[0].shard, 0);
        assert_eq!(recovery.report.truncated[0].applied, 2);
        assert_eq!(recovery.report.truncated[0].discarded, 1);
    }

    #[test]
    fn damage_in_one_segment_loses_only_the_records_in_the_hole() {
        // Two executors, one shard each: the hole is in shard 0's segment.
        let disk = MemDisk::default();
        let segments = wal(&disk, 1, 2);
        let mut s0 = FileLog::new(0, Arc::clone(&segments[0]));
        let mut s1 = FileLog::new(1, Arc::clone(&segments[1]));
        write(&mut s0, 0, &put(b"a", b"1"));
        write(&mut s1, 0, &put(b"b", b"1"));
        write(&mut s1, 1, &put(b"b", b"2"));
        let path = Path::new("/data/wal").join(segment_name(1, 0));
        let mut bytes = disk.contents(&path);
        // Flip a byte inside the first record's payload.
        bytes[SEGMENT_HEADER_LEN + 9 + 10 + 1] ^= 0xFF;
        disk.overwrite(&path, bytes);

        let recovery = recover(
            &disk,
            Path::new("/data/wal"),
            2,
            ReaderMode::Resynchronising,
        )
        .unwrap();
        assert_eq!(recovery.report.holes, 1);
        assert!(
            recovery.shards[0].records.is_empty(),
            "shard 0's only record was in the hole"
        );
        assert!(
            recovery.shards[0].lossy,
            "no gap shows the loss — nothing of shard 0 came after — so the damage in its segment must"
        );
        assert_eq!(recovery.shards[1].records.len(), 2, "shard 1 is untouched");
        assert!(
            !recovery.shards[1].lossy,
            "its executor's segment was intact"
        );
    }

    #[test]
    fn damage_is_charged_to_every_shard_its_segment_hosted() {
        // One executor hosting both shards: a hole in the segment could have
        // held either shard's records, so both are lossy though shard 1
        // recovered everything it wrote.
        let disk = MemDisk::default();
        let segments = wal(&disk, 1, 1);
        let mut s0 = FileLog::new(0, Arc::clone(&segments[0]));
        let mut s1 = FileLog::new(1, Arc::clone(&segments[0]));
        write(&mut s0, 0, &put(b"a", b"1"));
        write(&mut s1, 0, &put(b"b", b"1"));
        let path = Path::new("/data/wal").join(segment_name(1, 0));
        let mut bytes = disk.contents(&path);
        bytes[SEGMENT_HEADER_LEN + 9 + 10 + 1] ^= 0xFF;
        disk.overwrite(&path, bytes);

        let recovery = recover(
            &disk,
            Path::new("/data/wal"),
            2,
            ReaderMode::Resynchronising,
        )
        .unwrap();
        assert_eq!(recovery.shards[1].records.len(), 1);
        assert!(recovery.shards.iter().all(|shard| shard.lossy));
    }

    #[test]
    fn a_newer_format_version_refuses_to_recover() {
        let disk = MemDisk::default();
        wal(&disk, 1, 1);
        let mut header = Vec::new();
        encode_segment_header(2, 0, &mut header);
        header[4] = FORMAT_VERSION + 1;
        disk.write_file(&Path::new("/data/wal").join(segment_name(2, 0)), &header)
            .unwrap();
        let error = recover(
            &disk,
            Path::new("/data/wal"),
            1,
            ReaderMode::Resynchronising,
        )
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("version 2"), "{error}");
    }

    #[test]
    fn a_segment_with_a_bad_header_is_abandoned_and_every_shard_is_lossy() {
        let disk = MemDisk::default();
        let segments = wal(&disk, 1, 1);
        let mut s1 = FileLog::new(1, Arc::clone(&segments[0]));
        write(&mut s1, 0, &put(b"b", b"1"));
        disk.write_file(&Path::new("/data/wal").join(segment_name(2, 0)), b"junk")
            .unwrap();
        let recovery = recover(
            &disk,
            Path::new("/data/wal"),
            2,
            ReaderMode::Resynchronising,
        )
        .unwrap();
        assert_eq!(recovery.report.abandoned_segments, 1);
        assert!(recovery.shards.iter().all(|shard| shard.lossy));
        assert_eq!(
            recovery.shards[1].records.len(),
            1,
            "the good segment still counts"
        );
    }

    #[test]
    fn a_malformed_payload_ends_that_shards_prefix() {
        let disk = MemDisk::default();
        let segments = wal(&disk, 1, 1);
        let mut s0 = FileLog::new(0, Arc::clone(&segments[0]));
        write(&mut s0, 0, &put(b"a", b"1"));
        write(&mut s0, 1, &[99]); // no such tag
        write(&mut s0, 2, &put(b"a", b"3"));
        let recovery = recover(
            &disk,
            Path::new("/data/wal"),
            1,
            ReaderMode::Resynchronising,
        )
        .unwrap();
        assert_eq!(recovery.report.malformed, 1);
        assert_eq!(recovery.shards[0].records.len(), 1);
        assert_eq!(recovery.shards[0].discarded, 1);
        assert!(recovery.shards[0].lossy);
    }

    #[test]
    fn a_missing_directory_is_an_error_and_an_empty_one_is_a_fresh_node() {
        let disk = MemDisk::default();
        assert!(
            recover(
                &disk,
                Path::new("/data/wal"),
                4,
                ReaderMode::Resynchronising
            )
            .is_err()
        );
        disk.create_dir_all(Path::new("/data/wal")).unwrap();
        let recovery = recover(
            &disk,
            Path::new("/data/wal"),
            4,
            ReaderMode::Resynchronising,
        )
        .unwrap();
        assert_eq!(recovery.shards.len(), 4);
        assert!(
            recovery
                .shards
                .iter()
                .all(|shard| shard.records.is_empty() && !shard.lossy)
        );
        assert_eq!(recovery.report.segments, 0);
    }
}
