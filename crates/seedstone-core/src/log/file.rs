//! The on-disk layout, and the log that buffers for it.
//!
//! ```text
//! <data-dir>/wal/
//!   GENERATION                           the generation counter, decimal ASCII
//!   0000000000000003-00000000.seg        <generation:016x>-<rotation:08x>.seg
//!   0000000000000003-0001-00000000.snap  one executor's image: snapshot.rs
//! ```
//!
//! A **generation** is one process lifetime: read on start-up, incremented,
//! written back atomically. Segments of a newer generation sort after every
//! segment of an older one, so a shard's records are in sequence order
//! across files whichever executor owned it each time. A **segment** is a
//! fixed header followed by records in the format `log.rs` defines.
//!
//! There is one segment per node, written by the node's writer
//! ([`writer`](crate::log::writer)): every executor hands it its batches,
//! and the shards are interleaved in the order the batches arrived. A
//! rotation opens the next file; compaction deletes whole rotations.
//!
//! A shard's [`FileLog`] is a buffer: it encodes what the shard appends,
//! hands the bytes to the writer when the executor flushes, and keeps the
//! shard's points — what it handed over, what a sync made durable, and what
//! a snapshot covered.

use std::io;
use std::path::Path;

use crate::log::disk::{Disk, LogFile};
use crate::log::{Record, ReplicationLog, crc32_iso_hdlc, encode_record};

/// The four bytes every segment starts with.
pub const SEGMENT_MAGIC: [u8; 4] = *b"SSEG";

/// The layout version this build writes, and the only one it reads.
pub const FORMAT_VERSION: u8 = 2;

/// Magic, version, generation, rotation, and a CRC over the rest.
pub const SEGMENT_HEADER_LEN: usize = 4 + 1 + 8 + 4 + 4;

/// The file the generation counter lives in.
pub const GENERATION_FILE: &str = "GENERATION";

/// The name the counter is written under before it is renamed into place.
const GENERATION_TMP: &str = "GENERATION.tmp";

/// The name of the node's segment in `generation` at `rotation`.
///
/// The writer rotates at a size, so that what precedes a rotation can be
/// deleted as a whole file once every executor has covered it. Zero at
/// start.
#[must_use]
pub fn segment_name(generation: u64, rotation: u32) -> String {
    format!("{generation:016x}-{rotation:08x}.seg")
}

/// The generation and rotation a segment name carries, if it is one.
#[must_use]
pub fn parse_segment_name(name: &str) -> Option<(u64, u32)> {
    parse_name2(name, ".seg")
}

/// `<generation:016x>-<executor:04x>-<counter:08x><suffix>`: a snapshot's
/// shape, and the previous layout's segments'.
pub(crate) fn parse_name(name: &str, suffix: &str) -> Option<(u64, u16, u32)> {
    let stem = name.strip_suffix(suffix)?;
    let mut parts = stem.split('-');
    let (generation, executor, counter) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || generation.len() != 16 || executor.len() != 4 || counter.len() != 8
    {
        return None;
    }
    Some((
        u64::from_str_radix(generation, 16).ok()?,
        u16::from_str_radix(executor, 16).ok()?,
        u32::from_str_radix(counter, 16).ok()?,
    ))
}

/// `<generation:016x>-<counter:08x><suffix>`: a segment's shape.
pub(crate) fn parse_name2(name: &str, suffix: &str) -> Option<(u64, u32)> {
    let stem = name.strip_suffix(suffix)?;
    let (generation, counter) = stem.split_once('-')?;
    if generation.len() != 16
        || counter.len() != 8
        || stem.contains(|c: char| !c.is_ascii_hexdigit() && c != '-')
    {
        return None;
    }
    Some((
        u64::from_str_radix(generation, 16).ok()?,
        u32::from_str_radix(counter, 16).ok()?,
    ))
}

/// Appends a segment header to `out`: the fields, then a CRC over them.
pub fn encode_segment_header(generation: u64, rotation: u32, out: &mut Vec<u8>) {
    let at = out.len();
    out.extend_from_slice(&SEGMENT_MAGIC);
    out.push(FORMAT_VERSION);
    out.extend_from_slice(&generation.to_le_bytes());
    out.extend_from_slice(&rotation.to_le_bytes());
    let crc = crc32_iso_hdlc(&out[at..]);
    out.extend_from_slice(&crc.to_le_bytes());
}

/// Why a header could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderError {
    /// Fewer bytes than the header needs.
    Short,
    /// The magic is wrong: not this kind of file, or damaged.
    BadMagic,
    /// A version above [`FORMAT_VERSION`]: a file this build must not guess
    /// at.
    NewerVersion(u8),
    /// A version below [`FORMAT_VERSION`]: the file predates this build's
    /// layout; nothing reads it, and a start that finds one refuses.
    OlderVersion(u8),
    /// The fields do not match their CRC: damage.
    BadChecksum,
}

/// Reads a segment header: the generation and rotation it names.
///
/// The version is checked before the checksum: another version may lay its
/// header out differently, and refusing it as damage would scan a downgrade
/// as a hole, or an earlier build's directory as every shard's loss. A
/// version byte that is this layout's own once restored, with the checksum
/// then matching, was damaged rather than written by another build.
///
/// # Errors
///
/// [`HeaderError`], as each variant says.
pub fn decode_segment_header(buf: &[u8]) -> Result<(u64, u32), HeaderError> {
    let Some(header) = buf.get(..SEGMENT_HEADER_LEN) else {
        return Err(HeaderError::Short);
    };
    if header[..4] != SEGMENT_MAGIC {
        return Err(HeaderError::BadMagic);
    }
    if header[4] != FORMAT_VERSION {
        return Err(version_mismatch(header));
    }
    check_header_crc(header)?;
    let mut generation = [0; 8];
    generation.copy_from_slice(&header[5..13]);
    let mut rotation = [0; 4];
    rotation.copy_from_slice(&header[13..17]);
    Ok((u64::from_le_bytes(generation), u32::from_le_bytes(rotation)))
}

/// Why a header of this layout's length carries another version: written
/// by another build, or this layout's own header with its version byte
/// damaged — which the checksum tells apart, since it covers the byte.
pub(crate) fn version_mismatch(header: &[u8]) -> HeaderError {
    let mut restored = header.to_vec();
    restored[4] = FORMAT_VERSION;
    if check_header_crc(&restored).is_ok() {
        return HeaderError::BadChecksum;
    }
    match header[4] {
        v if v > FORMAT_VERSION => HeaderError::NewerVersion(v),
        v => HeaderError::OlderVersion(v),
    }
}

/// The last four bytes of `header` are the CRC of everything before them.
pub(crate) fn check_header_crc(header: &[u8]) -> Result<(), HeaderError> {
    let (fields, crc) = header.split_at(header.len() - 4);
    let mut expected = [0; 4];
    expected.copy_from_slice(crc);
    if crc32_iso_hdlc(fields) == u32::from_le_bytes(expected) {
        Ok(())
    } else {
        Err(HeaderError::BadChecksum)
    }
}

/// The node's open segment: the writer's alone.
pub struct Segment<F: LogFile> {
    pub(crate) file: F,
    /// Which rotation of the node's segment the file is.
    pub(crate) rotation: u32,
    /// The highest rotation a rotation has tried to create. A rotation that
    /// failed may have left a file under its name, so the next attempt takes
    /// the name after it rather than appending a second header to that one.
    pub(crate) attempted: u32,
    /// Bytes written to this rotation: what the rotation's size reads.
    pub(crate) bytes_written: u64,
    /// Whether anything was written since the last sync was issued —
    /// cleared at the issue, so that what is written during a sync in
    /// flight is the next one's.
    pub(crate) dirty: bool,
    /// Whether a write or a sync of this file has failed. A filesystem may
    /// drop the pages a failed sync could not write and report success on
    /// the next one, so from then on no sync of this file proves anything:
    /// the writer never heals it, it rotates away from it.
    pub(crate) sync_failed: bool,
}

/// One shard's log: a buffer, and the points it keeps.
pub struct FileLog {
    shard: u16,
    /// Records appended and not yet handed to the writer.
    pending: Vec<u8>,
    /// The highest sequence in `pending`.
    pending_through: Option<u64>,
    /// The highest sequence handed to the writer.
    flushed_through: Option<u64>,
    /// The highest sequence a sync or a snapshot has made durable.
    durable: Option<u64>,
}

impl FileLog {
    /// A log for `shard`.
    #[must_use]
    pub const fn new(shard: u16) -> Self {
        Self {
            shard,
            pending: Vec::new(),
            pending_through: None,
            flushed_through: None,
            durable: None,
        }
    }

    /// The shard this log belongs to.
    #[must_use]
    pub const fn shard(&self) -> u16 {
        self.shard
    }
}

impl ReplicationLog for FileLog {
    fn append(&mut self, rec: Record<'_>) -> io::Result<()> {
        debug_assert_eq!(
            rec.shard, self.shard,
            "a record reached the wrong shard's log"
        );
        encode_record(&rec, &mut self.pending);
        self.pending_through = Some(rec.seq);
        Ok(())
    }

    fn flush_into(&mut self, out: &mut Vec<u8>) {
        if self.pending.is_empty() {
            return;
        }
        out.extend_from_slice(&self.pending);
        self.pending.clear();
        self.flushed_through = self.pending_through.take().or(self.flushed_through);
    }

    fn flushed_through(&self) -> Option<u64> {
        self.flushed_through
    }

    fn sync_completed(&mut self, through: Option<u64>, _round: u64) -> Option<u64> {
        // Never lower: `covered` may have raised the point past what an
        // earlier round covered.
        self.durable = self.durable.max(through);
        self.durable
    }

    fn covered(&mut self, through: u64) {
        self.durable = self.durable.max(Some(through));
    }
}

/// Reads the generation counter, advances it, writes it back atomically,
/// and returns the generation this process now owns.
///
/// A counter that is missing, unparseable, or *behind* the segments on
/// disk is not trusted: the answer is one above the highest generation any
/// segment name carries. Behind is the case a torn write of the counter
/// produces, and trusting it would make a new generation's segments sort
/// before an old one's.
///
/// # Errors
///
/// Whatever the disk reports for listing the directory or writing the file.
pub fn next_generation<D: Disk>(disk: &D, wal: &Path) -> io::Result<u64> {
    let recorded = read_generation(disk, wal).unwrap_or(0);
    let on_disk = disk
        .list(wal)?
        .iter()
        .filter_map(|name| parse_segment_name(name))
        .map(|(generation, _)| generation)
        .max()
        .unwrap_or(0);
    let next = recorded.max(on_disk) + 1;
    disk.write_file(&wal.join(GENERATION_TMP), next.to_string().as_bytes())?;
    disk.rename(&wal.join(GENERATION_TMP), &wal.join(GENERATION_FILE))?;
    disk.sync_dir(wal)?;
    Ok(next)
}

/// The counter as written, or `None` for anything that is not a number.
fn read_generation<D: Disk>(disk: &D, wal: &Path) -> Option<u64> {
    let mut text = Vec::new();
    std::io::Read::read_to_end(
        &mut disk.open_read(&wal.join(GENERATION_FILE)).ok()?,
        &mut text,
    )
    .ok()?;
    std::str::from_utf8(&text).ok()?.trim().parse().ok()
}

/// Creates one segment file: header written and synced. The directory is
/// the caller's to sync — once per batch of files, not once per file.
///
/// # Errors
///
/// Whatever the disk reports.
pub fn create_segment<D: Disk>(
    disk: &D,
    wal: &Path,
    generation: u64,
    rotation: u32,
) -> io::Result<D::File> {
    let mut file = disk.create_append(&wal.join(segment_name(generation, rotation)))?;
    let mut header = Vec::with_capacity(SEGMENT_HEADER_LEN);
    encode_segment_header(generation, rotation, &mut header);
    file.write_all(&header)?;
    file.sync_data()?;
    Ok(file)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::log::disk::mem::MemDisk;
    use crate::log::{Decoded, decode_record};

    #[test]
    fn segment_names_carry_generation_and_rotation_and_sort_by_both() {
        assert_eq!(segment_name(3, 2), "0000000000000003-00000002.seg");
        assert_eq!(
            parse_segment_name("0000000000000003-00000002.seg"),
            Some((3, 2))
        );
        assert_eq!(
            parse_segment_name("0000000000000003-0001-00000002.seg"),
            None,
            "the previous layout's shape"
        );
        assert_eq!(parse_segment_name("0000000000000003-00000002.snap"), None);
        assert_eq!(parse_segment_name("GENERATION"), None);
        let mut names = vec![segment_name(2, 0), segment_name(1, 3), segment_name(1, 0)];
        names.sort();
        assert_eq!(
            names,
            [segment_name(1, 0), segment_name(1, 3), segment_name(2, 0)]
        );
    }

    #[test]
    fn a_segment_header_round_trips_and_refuses_other_versions_and_a_bad_checksum() {
        let mut out = Vec::new();
        encode_segment_header(9, 5, &mut out);
        assert_eq!(out.len(), SEGMENT_HEADER_LEN);
        assert_eq!(decode_segment_header(&out), Ok((9, 5)));
        assert_eq!(decode_segment_header(&out[..3]), Err(HeaderError::Short));
        let mut bad = out.clone();
        bad[0] = b'X';
        assert_eq!(decode_segment_header(&bad), Err(HeaderError::BadMagic));
        let mut flipped = out.clone();
        flipped[7] ^= 0x01; // inside the generation
        assert_eq!(
            decode_segment_header(&flipped),
            Err(HeaderError::BadChecksum)
        );
        let newer = restamped(&out, FORMAT_VERSION + 1);
        assert_eq!(
            decode_segment_header(&newer),
            Err(HeaderError::NewerVersion(FORMAT_VERSION + 1))
        );
        let older = restamped(&out, 1);
        assert_eq!(
            decode_segment_header(&older),
            Err(HeaderError::OlderVersion(1))
        );
    }

    /// `header` with its version byte set to `version` and its CRC made to
    /// match: what a build writing that version would have laid out, had it
    /// kept this layout.
    pub fn restamped(header: &[u8], version: u8) -> Vec<u8> {
        let mut out = header.to_vec();
        out[4] = version;
        let fields = out.len() - 4;
        let crc = crc32_iso_hdlc(&out[..fields]);
        out[fields..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    #[test]
    fn a_damaged_version_byte_is_a_bad_checksum_not_another_version() {
        let mut out = Vec::new();
        encode_segment_header(9, 5, &mut out);
        for version in [0, 1, FORMAT_VERSION + 1, u8::MAX] {
            let mut damaged = out.clone();
            damaged[4] = version;
            assert_eq!(
                decode_segment_header(&damaged),
                Err(HeaderError::BadChecksum),
                "version byte {version}"
            );
        }
    }

    #[test]
    fn create_segment_writes_a_synced_header_under_the_name() {
        let disk = MemDisk::default();
        disk.create_dir_all(Path::new("/w")).unwrap();
        let _file = create_segment(&disk, Path::new("/w"), 4, 1).unwrap();
        let bytes = disk.contents(&Path::new("/w").join(segment_name(4, 1)));
        assert_eq!(decode_segment_header(&bytes), Ok((4, 1)));
        assert_eq!(
            disk.synced_len(&Path::new("/w").join(segment_name(4, 1))),
            bytes.len()
        );
    }

    fn rec(shard: u16, seq: u64, payload: &[u8]) -> Record<'_> {
        Record {
            shard,
            seq,
            payload,
        }
    }

    #[test]
    fn a_log_buffers_hands_over_in_order_and_dates_its_points_by_what_it_handed_over() {
        let mut log = FileLog::new(3);
        assert_eq!(log.flushed_through(), None);
        log.append(rec(3, 0, b"a")).unwrap();
        log.append(rec(3, 1, b"b")).unwrap();
        let mut out = vec![0xEE];
        log.flush_into(&mut out);
        assert_eq!(log.flushed_through(), Some(1));
        assert_eq!(out[0], 0xEE, "appended after what the buffer held");
        let Decoded::Record {
            shard,
            seq,
            payload,
            consumed: next,
        } = decode_record(&out[1..])
        else {
            panic!()
        };
        assert_eq!((shard, seq, payload), (3, 0, &b"a"[..]));
        let Decoded::Record { seq, .. } = decode_record(&out[1 + next..]) else {
            panic!()
        };
        assert_eq!(seq, 1);
        let mut again = Vec::new();
        log.flush_into(&mut again);
        assert!(again.is_empty(), "a second flush hands over nothing");
        assert_eq!(log.flushed_through(), Some(1), "and moves no point");
        assert_eq!(log.sync_completed(Some(0), 1), Some(0));
        assert_eq!(log.sync_completed(Some(1), 2), Some(1));
        assert_eq!(log.sync_completed(Some(0), 3), Some(1), "never lowers");
        log.covered(7);
        assert_eq!(
            log.sync_completed(Some(1), 4),
            Some(7),
            "a cover is kept past a later sync"
        );
    }

    #[test]
    fn the_generation_counts_up_and_is_written_atomically() {
        let disk = MemDisk::default();
        let wal = Path::new("/data/wal");
        disk.create_dir_all(wal).unwrap();
        assert_eq!(
            next_generation(&disk, wal).unwrap(),
            1,
            "a fresh directory starts at 1"
        );
        assert_eq!(disk.contents(&wal.join(GENERATION_FILE)), b"1");
        assert_eq!(next_generation(&disk, wal).unwrap(), 2);
        assert!(
            !disk
                .list(wal)
                .unwrap()
                .iter()
                .any(|name| Path::new(name).extension().is_some_and(|ext| ext == "tmp"))
        );
    }

    #[test]
    fn a_damaged_generation_file_falls_back_to_the_segment_names() {
        let disk = MemDisk::default();
        let wal = Path::new("/data/wal");
        disk.create_dir_all(wal).unwrap();
        create_segment(&disk, wal, 7, 0).unwrap();
        disk.write_file(&wal.join(GENERATION_FILE), b"7\xff\x00")
            .unwrap();
        assert_eq!(
            next_generation(&disk, wal).unwrap(),
            8,
            "one above the highest generation any segment carries"
        );
        create_segment(&disk, wal, 8, 0).unwrap();
        disk.write_file(&wal.join(GENERATION_FILE), b"3").unwrap();
        assert_eq!(
            next_generation(&disk, wal).unwrap(),
            9,
            "a counter behind the segments is not trusted either"
        );
    }
}
