//! The on-disk layout, and the log that writes it.
//!
//! ```text
//! <data-dir>/wal/
//!   GENERATION                  the generation counter, decimal ASCII
//!   0000000000000003-0000-00000000.seg   <generation:016x>-<executor:04x>-<rotation:08x>.seg
//!   0000000000000003-0001-00000000.seg
//! ```
//!
//! A **generation** is one process lifetime: read on start-up, incremented,
//! written back atomically. Segments of a newer generation sort after every
//! segment of an older one whatever the executor counts were, so a shard's
//! records are in sequence order across files whichever executor owned it
//! each time. A **segment** is a fixed header followed by records in the
//! format `log.rs` defines, the shards interleaved in arrival order. Each
//! executor writes its own segment: one `fsync` per executor per tick.
//!
//! Who writes a segment is not part of the layout. A single writer with
//! group commit could write one segment for the whole node and nothing on
//! disk would change; compaction can delete whole segments. That is what
//! the layout was chosen for.
//!
//! The write path is two-phase, in the housekeeping tick: `flush` writes a
//! shard's buffered records to the segment, `sync` makes the segment
//! durable — once per tick, because the first shard's `sync` clears the
//! segment's dirty flag and the rest find it clean. A write that fails
//! keeps its buffer: an acknowledged record is never dropped, because a
//! dropped record followed by a later successful write is a hole inside the
//! durable region, the one damage the reader cannot repair.

use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::log::disk::{Disk, LogFile};
use crate::log::{Record, ReplicationLog, crc32_iso_hdlc, encode_record};

/// The four bytes every segment starts with.
pub const SEGMENT_MAGIC: [u8; 4] = *b"SSEG";

/// The layout version this build writes and the highest it reads.
pub const FORMAT_VERSION: u8 = 1;

/// Magic, version, generation, executor, rotation, and a CRC over the rest.
pub const SEGMENT_HEADER_LEN: usize = 4 + 1 + 8 + 2 + 4 + 4;

/// The file the generation counter lives in.
pub const GENERATION_FILE: &str = "GENERATION";

/// The name the counter is written under before it is renamed into place.
const GENERATION_TMP: &str = "GENERATION.tmp";

/// The name the segment `executor` writes in `generation` at `rotation`.
///
/// Rotation is the checkpoint's counter: every cycle opens a fresh segment
/// so that what precedes it can be deleted as a whole file. Zero at start.
#[must_use]
pub fn segment_name(generation: u64, executor: u16, rotation: u32) -> String {
    format!("{generation:016x}-{executor:04x}-{rotation:08x}.seg")
}

/// The generation, executor and rotation a segment name carries, if it is
/// one.
#[must_use]
pub fn parse_segment_name(name: &str) -> Option<(u64, u16, u32)> {
    parse_name(name, ".seg")
}

/// `<generation:016x>-<executor:04x>-<counter:08x><suffix>`, the shape both
/// file types share.
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

/// Appends a segment header to `out`: the fields, then a CRC over them.
pub fn encode_segment_header(generation: u64, executor: u16, rotation: u32, out: &mut Vec<u8>) {
    let at = out.len();
    out.extend_from_slice(&SEGMENT_MAGIC);
    out.push(FORMAT_VERSION);
    out.extend_from_slice(&generation.to_le_bytes());
    out.extend_from_slice(&executor.to_le_bytes());
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
    /// The fields do not match their CRC: damage.
    BadChecksum,
}

/// Reads a segment header: the generation, executor and rotation it names.
///
/// The version is checked before the checksum: a newer version may lay its
/// header out differently, and refusing it as damage would scan a downgrade
/// as a hole.
///
/// # Errors
///
/// [`HeaderError`], as each variant says.
pub fn decode_segment_header(buf: &[u8]) -> Result<(u64, u16, u32), HeaderError> {
    let Some(header) = buf.get(..SEGMENT_HEADER_LEN) else {
        return Err(HeaderError::Short);
    };
    if header[..4] != SEGMENT_MAGIC {
        return Err(HeaderError::BadMagic);
    }
    if header[4] > FORMAT_VERSION {
        return Err(HeaderError::NewerVersion(header[4]));
    }
    check_header_crc(header)?;
    let mut generation = [0; 8];
    generation.copy_from_slice(&header[5..13]);
    let executor = u16::from_le_bytes([header[13], header[14]]);
    let mut rotation = [0; 4];
    rotation.copy_from_slice(&header[15..19]);
    Ok((
        u64::from_le_bytes(generation),
        executor,
        u32::from_le_bytes(rotation),
    ))
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

/// One open segment, shared by the shards of the executor that writes it.
pub struct Segment<F: LogFile> {
    file: F,
    /// Whether anything was written since the last successful sync.
    ///
    /// Public to the crate so a test can see the one-sync-per-tick property
    /// it exists for.
    pub(crate) dirty: bool,
    /// Whether a sync of this segment has ever failed. A filesystem may drop
    /// the pages a failed sync could not write and report success on the
    /// next one, so from then on no sync of this segment proves anything
    /// new: every shard's durable point on it stays where it was.
    sync_failed: bool,
}

/// A segment behind the lock the shards of one executor share.
///
/// Uncontended by construction — one executor task owns every shard that
/// holds a clone — and a lock rather than a `RefCell` only because a shard's
/// state has to be `Send` to be moved onto the runtime.
pub type SharedSegment<F> = Arc<Mutex<Segment<F>>>;

/// One shard's log: a buffer, and the segment it is flushed to.
pub struct FileLog<F: LogFile> {
    shard: u16,
    /// Records appended and not yet written.
    pending: Vec<u8>,
    /// The highest sequence in `pending`.
    pending_through: Option<u64>,
    /// The highest sequence written to the segment.
    flushed_through: Option<u64>,
    /// The highest sequence a sync has made durable.
    durable: Option<u64>,
    segment: SharedSegment<F>,
}

impl<F: LogFile> FileLog<F> {
    /// A log for `shard` that flushes into `segment`.
    #[must_use]
    pub const fn new(shard: u16, segment: SharedSegment<F>) -> Self {
        Self {
            shard,
            pending: Vec::new(),
            pending_through: None,
            flushed_through: None,
            durable: None,
            segment,
        }
    }

    /// The shard this log belongs to.
    #[must_use]
    pub const fn shard(&self) -> u16 {
        self.shard
    }

    /// Discards what was appended and not yet flushed.
    ///
    /// What a log that drops a failed write would do — and this server
    /// never does; it exists so the simulator can plant exactly that
    /// defect and show it caught.
    pub fn drop_pending(&mut self) {
        self.pending.clear();
        self.pending_through = None;
    }
}

/// The segment's lock, taken through the field alone so the rest of the
/// log stays borrowable while it is held.
fn lock<F: LogFile>(segment: &SharedSegment<F>) -> std::sync::MutexGuard<'_, Segment<F>> {
    segment
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl<F: LogFile> ReplicationLog for FileLog<F> {
    fn append(&mut self, rec: Record<'_>) -> io::Result<()> {
        debug_assert_eq!(
            rec.shard, self.shard,
            "a record reached the wrong shard's log"
        );
        encode_record(&rec, &mut self.pending);
        self.pending_through = Some(rec.seq);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut segment = lock(&self.segment);
        segment.file.write_all(&self.pending)?;
        segment.dirty = true;
        drop(segment);
        self.pending.clear();
        self.flushed_through = self.pending_through.take().or(self.flushed_through);
        Ok(())
    }

    fn sync(&mut self) -> io::Result<Option<u64>> {
        let mut segment = lock(&self.segment);
        if segment.dirty {
            if let Err(error) = segment.file.sync_data() {
                segment.sync_failed = true;
                return Err(error);
            }
            segment.dirty = false;
        }
        if !segment.sync_failed {
            self.durable = self.flushed_through;
        }
        drop(segment);
        Ok(self.durable)
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
        .map(|(generation, _, _)| generation)
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

/// Creates this generation's segment for every executor, header written and
/// synced, then syncs the directory once.
///
/// The directory sync is the step that is easy to miss: a crash
/// discards a file whose directory entry was never synced, header and all.
///
/// # Errors
///
/// Whatever the disk reports.
pub fn open_segments<D: Disk>(
    disk: &D,
    wal: &Path,
    generation: u64,
    executors: u16,
) -> io::Result<Vec<SharedSegment<D::File>>> {
    let mut segments = Vec::with_capacity(usize::from(executors));
    for executor in 0..executors {
        let mut file = disk.create_append(&wal.join(segment_name(generation, executor, 0)))?;
        let mut header = Vec::with_capacity(SEGMENT_HEADER_LEN);
        encode_segment_header(generation, executor, 0, &mut header);
        file.write_all(&header)?;
        file.sync_data()?;
        segments.push(Arc::new(Mutex::new(Segment {
            file,
            dirty: false,
            sync_failed: false,
        })));
    }
    disk.sync_dir(wal)?;
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::disk::mem::MemDisk;
    use crate::log::{Decoded, decode_record};

    #[test]
    fn segment_names_sort_by_generation_then_executor_then_rotation() {
        assert_eq!(segment_name(3, 1, 0), "0000000000000003-0001-00000000.seg");
        assert_eq!(
            parse_segment_name("0000000000000003-0001-00000002.seg"),
            Some((3, 1, 2))
        );
        assert_eq!(parse_segment_name("GENERATION"), None);
        assert_eq!(
            parse_segment_name("0000000000000003-0001.seg"),
            None,
            "the old shape"
        );
        assert_eq!(
            parse_segment_name("0000000000000003-0001-00000002.snap"),
            None
        );
        let mut names = vec![
            segment_name(2, 0, 0),
            segment_name(1, 3, 0),
            segment_name(1, 0, 1),
            segment_name(1, 0, 0),
        ];
        names.sort();
        assert_eq!(
            names,
            [
                segment_name(1, 0, 0),
                segment_name(1, 0, 1),
                segment_name(1, 3, 0),
                segment_name(2, 0, 0)
            ]
        );
    }

    #[test]
    fn a_segment_header_round_trips_and_refuses_a_newer_version_or_a_bad_checksum() {
        let mut out = Vec::new();
        encode_segment_header(9, 2, 5, &mut out);
        assert_eq!(out.len(), SEGMENT_HEADER_LEN);
        assert_eq!(decode_segment_header(&out), Ok((9, 2, 5)));
        assert_eq!(decode_segment_header(&out[..3]), Err(HeaderError::Short));
        let mut bad = out.clone();
        bad[0] = b'X';
        assert_eq!(decode_segment_header(&bad), Err(HeaderError::BadMagic));
        let mut flipped = out.clone();
        flipped[7] ^= 0x01; // inside the generation
        assert_eq!(
            decode_segment_header(&flipped),
            Err(HeaderError::BadChecksum),
            "a header whose bytes moved is damage, not a different header"
        );
        let mut newer = out;
        newer[4] = FORMAT_VERSION + 1;
        // The version is read before the checksum: a newer version writes a
        // header this build cannot checksum, and refusing it as damage would
        // scan a downgrade as a hole.
        assert_eq!(
            decode_segment_header(&newer),
            Err(HeaderError::NewerVersion(FORMAT_VERSION + 1))
        );
    }

    #[test]
    fn open_segments_creates_one_per_executor_with_a_synced_header() {
        let disk = MemDisk::default();
        let wal = Path::new("/data/wal");
        disk.create_dir_all(wal).unwrap();
        let segments = open_segments(&disk, wal, 4, 2).unwrap();
        assert_eq!(segments.len(), 2);
        let mut names = disk.list(wal).unwrap();
        names.sort();
        assert_eq!(names, [segment_name(4, 0, 0), segment_name(4, 1, 0)]);
        let header = disk.contents(&wal.join(segment_name(4, 1, 0)));
        assert_eq!(decode_segment_header(&header), Ok((4, 1, 0)));
    }

    #[test]
    fn append_buffers_flush_writes_and_sync_reports_the_durable_point() {
        let disk = MemDisk::default();
        let wal = Path::new("/data/wal");
        disk.create_dir_all(wal).unwrap();
        let segments = open_segments(&disk, wal, 1, 1).unwrap();
        let path = wal.join(segment_name(1, 0, 0));
        let mut log = FileLog::new(5, Arc::clone(&segments[0]));

        log.append(Record {
            shard: 5,
            seq: 0,
            payload: b"a",
        })
        .unwrap();
        log.append(Record {
            shard: 5,
            seq: 1,
            payload: b"b",
        })
        .unwrap();
        assert_eq!(
            disk.contents(&path).len(),
            SEGMENT_HEADER_LEN,
            "append writes nothing"
        );
        assert_eq!(
            log.sync().unwrap(),
            None,
            "nothing flushed, nothing durable"
        );

        log.flush().unwrap();
        let written = disk.contents(&path);
        assert!(written.len() > SEGMENT_HEADER_LEN, "flush wrote the buffer");
        assert!(matches!(
            decode_record(&written[SEGMENT_HEADER_LEN..]),
            Decoded::Record {
                shard: 5,
                seq: 0,
                ..
            }
        ));
        assert_eq!(log.sync().unwrap(), Some(1), "both records are durable");
        log.flush().unwrap();
        assert_eq!(
            disk.contents(&path),
            written,
            "an empty buffer writes nothing"
        );
    }

    #[test]
    fn a_failed_flush_keeps_the_buffer_and_the_next_flush_writes_it() {
        let disk = MemDisk::default();
        let wal = Path::new("/data/wal");
        disk.create_dir_all(wal).unwrap();
        let segments = open_segments(&disk, wal, 1, 1).unwrap();
        let path = wal.join(segment_name(1, 0, 0));
        let mut log = FileLog::new(0, Arc::clone(&segments[0]));
        log.append(Record {
            shard: 0,
            seq: 0,
            payload: b"kept",
        })
        .unwrap();

        disk.fail_writes(true);
        assert!(log.flush().is_err());
        assert_eq!(log.sync().unwrap(), None, "nothing reached the disk");
        disk.fail_writes(false);

        log.append(Record {
            shard: 0,
            seq: 1,
            payload: b"later",
        })
        .unwrap();
        log.flush().unwrap();
        assert_eq!(log.sync().unwrap(), Some(1));
        let written = disk.contents(&path);
        let Decoded::Record { seq, consumed, .. } = decode_record(&written[SEGMENT_HEADER_LEN..])
        else {
            panic!("the first record is intact")
        };
        assert_eq!(seq, 0, "the record the failed flush kept came first");
        assert!(matches!(
            decode_record(&written[SEGMENT_HEADER_LEN + consumed..]),
            Decoded::Record { seq: 1, .. }
        ));
    }

    #[test]
    fn a_failed_sync_leaves_the_durable_point_where_it_was() {
        let disk = MemDisk::default();
        let wal = Path::new("/data/wal");
        disk.create_dir_all(wal).unwrap();
        let segments = open_segments(&disk, wal, 1, 1).unwrap();
        let mut log = FileLog::new(0, Arc::clone(&segments[0]));
        log.append(Record {
            shard: 0,
            seq: 0,
            payload: b"x",
        })
        .unwrap();
        log.flush().unwrap();
        assert_eq!(log.sync().unwrap(), Some(0));
        log.append(Record {
            shard: 0,
            seq: 1,
            payload: b"y",
        })
        .unwrap();
        log.flush().unwrap();
        disk.fail_writes(true);
        assert!(log.sync().is_err());
        disk.fail_writes(false);
        // A filesystem may drop the pages a failed sync could not write and
        // report success on the next one, so the retry proves nothing about
        // what the failed sync held: the point stays where it was, and
        // recovery on the next start finds whatever was really lost.
        assert_eq!(
            log.sync().unwrap(),
            Some(0),
            "a sync after a failed one does not advance the durable point"
        );
        log.append(Record {
            shard: 0,
            seq: 2,
            payload: b"z",
        })
        .unwrap();
        log.flush().unwrap();
        assert_eq!(log.sync().unwrap(), Some(0), "nor does any later one");
    }

    #[test]
    fn two_shards_on_one_segment_pay_one_sync_per_tick() {
        // Observable through the dirty flag: after the first shard's sync the
        // segment is clean, and the second shard's sync reports its own
        // flushed point without a second `sync_data`. The mem disk cannot
        // count syncs, so the test reads the flag.
        let disk = MemDisk::default();
        let wal = Path::new("/data/wal");
        disk.create_dir_all(wal).unwrap();
        let segments = open_segments(&disk, wal, 1, 1).unwrap();
        let mut a = FileLog::new(0, Arc::clone(&segments[0]));
        let mut b = FileLog::new(1, Arc::clone(&segments[0]));
        a.append(Record {
            shard: 0,
            seq: 0,
            payload: b"a",
        })
        .unwrap();
        b.append(Record {
            shard: 1,
            seq: 0,
            payload: b"b",
        })
        .unwrap();
        a.flush().unwrap();
        b.flush().unwrap();
        assert!(segments[0].lock().unwrap().dirty);
        assert_eq!(a.sync().unwrap(), Some(0));
        assert!(!segments[0].lock().unwrap().dirty);
        assert_eq!(b.sync().unwrap(), Some(0), "covered by a's sync");
    }

    #[test]
    fn drop_pending_discards_what_was_appended_and_not_flushed() {
        let disk = MemDisk::default();
        let wal = Path::new("/data/wal");
        disk.create_dir_all(wal).unwrap();
        let segments = open_segments(&disk, wal, 1, 1).unwrap();
        let mut log = FileLog::new(0, Arc::clone(&segments[0]));
        log.append(Record {
            shard: 0,
            seq: 0,
            payload: b"x",
        })
        .unwrap();
        log.drop_pending();
        log.flush().unwrap();
        assert_eq!(log.sync().unwrap(), None);
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
        open_segments(&disk, wal, 7, 2).unwrap();
        disk.write_file(&wal.join(GENERATION_FILE), b"7\xff\x00")
            .unwrap();
        assert_eq!(
            next_generation(&disk, wal).unwrap(),
            8,
            "one above the highest generation any segment carries"
        );
        open_segments(&disk, wal, 8, 2).unwrap();
        disk.write_file(&wal.join(GENERATION_FILE), b"3").unwrap();
        assert_eq!(
            next_generation(&disk, wal).unwrap(),
            9,
            "a counter behind the segments is not trusted either"
        );
    }
}
