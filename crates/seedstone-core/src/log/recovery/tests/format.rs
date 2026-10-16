//! Which layout versions a start reads, refuses or charges as damage, and
//! what a sync mark is in each.

use super::*;
use crate::log::file::tests::restamped;
use crate::log::file::{FORMAT_VERSION, encode_sync_mark};

#[test]
fn a_directory_from_the_previous_format_refuses_the_start() {
    let disk = MemDisk::default();
    disk.create_dir_all(Path::new("/data/wal")).unwrap();
    // The previous layout's header: magic, version 1, generation, executor,
    // rotation, crc.
    let mut header = Vec::new();
    header.extend_from_slice(b"SSEG");
    header.push(1);
    header.extend_from_slice(&1u64.to_le_bytes());
    header.extend_from_slice(&0u16.to_le_bytes());
    header.extend_from_slice(&0u32.to_le_bytes());
    let crc = crate::log::crc32_iso_hdlc(&header);
    header.extend_from_slice(&crc.to_le_bytes());
    // The name does not parse as a segment of this layout; the start must
    // still read the header rather than ignore the file.
    disk.write_file(
        Path::new("/data/wal/0000000000000001-0000-00000000.seg"),
        &header,
    )
    .unwrap();
    let error = recover(spec(&disk, 2)).expect_err("refused");
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    assert!(error.to_string().contains("predates"), "{error}");
}

#[test]
fn a_newer_format_version_refuses_to_recover() {
    let disk = MemDisk::default();
    wal(&disk, 1);
    let mut header = Vec::new();
    encode_segment_header(2, 0, &[], &mut header);
    let header = restamped(&header, FORMAT_VERSION + 1);
    disk.write_file(&Path::new("/data/wal").join(segment_name(2, 0)), &header)
        .unwrap();
    let error = recover(spec(&disk, 1)).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    assert!(
        error
            .to_string()
            .contains(&format!("version {}", FORMAT_VERSION + 1)),
        "{error}"
    );
}

#[test]
fn a_segment_with_a_bad_header_is_abandoned_and_every_shard_is_lossy() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 1, 0, &put(b"b", b"1"));
    // A header's length of bytes that are not a header.
    disk.write_file(
        &Path::new("/data/wal").join(segment_name(2, 0)),
        &[b'j'; crate::log::file::SEGMENT_HEADER_LEN],
    )
    .unwrap();
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(recovery.report.abandoned_segments, 1);
    assert!(recovery.shards.iter().all(|shard| shard.lossy));
    assert_eq!(recovery.shards[1].seq, 1, "the good segment still counts");
}

/// One flipped bit in a header's version byte is damage like any other, not
/// a directory from another build: the start goes on and charges it.
#[test]
fn a_segment_whose_version_byte_is_damaged_is_abandoned_not_refused() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 1, 0, &put(b"b", b"1"));
    let mut header = Vec::new();
    encode_segment_header(2, 0, &[], &mut header);
    header[4] = 1; // what an earlier build would have written
    disk.write_file(&Path::new("/data/wal").join(segment_name(2, 0)), &header)
        .unwrap();
    let recovery = recover(spec(&disk, 2)).expect("damage, not another version");
    assert_eq!(recovery.report.abandoned_segments, 1);
    assert!(recovery.shards.iter().all(|shard| shard.lossy));
}

#[test]
fn a_snapshot_from_a_newer_format_version_refuses_the_start() {
    let disk = MemDisk::default();
    wal(&disk, 1);
    let header = SnapshotHeader {
        generation: 1,
        executor: 0,
        cycle: 0,
        unix_millis: 0,
        bases: vec![(0, 0)],
    };
    let mut bytes = Vec::new();
    header.encode(&mut bytes);
    let bytes = restamped(&bytes, FORMAT_VERSION + 1);
    disk.write_file(&Path::new("/data/wal").join(snapshot_name(1, 0, 0)), &bytes)
        .unwrap();
    let error = recover(spec(&disk, 1)).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
}

/// A directory written by the previous layout is read: its segment headers
/// carry no cut points, and its records are replayed as ever.
#[test]
fn a_version_3_segment_is_recovered() {
    let disk = MemDisk::default();
    let dir = Path::new("/data/wal");
    disk.create_dir_all(dir).unwrap();
    let mut header = Vec::new();
    header.extend_from_slice(b"SSEG");
    header.push(3);
    header.extend_from_slice(&1u64.to_le_bytes());
    header.extend_from_slice(&0u32.to_le_bytes());
    let crc = crate::log::crc32_iso_hdlc(&header);
    header.extend_from_slice(&crc.to_le_bytes());
    let mut file = header;
    encode_record(
        &Record {
            shard: 0,
            seq: 0,
            payload: &put(b"a", b"1"),
        },
        &mut file,
    );
    disk.write_file(&dir.join(segment_name(1, 0)), &file)
        .unwrap();

    let recovery = recover(spec(&disk, 1)).unwrap();
    assert_eq!(contents(&recovery.shards[0].dict), pairs(&[(b"a", b"1")]));
    assert!(!recovery.shards[0].lossy);
}

/// A sync mark, written and synced as the writer writes one.
pub(super) fn write_mark(file: &mut MemFile) {
    let mut bytes = Vec::new();
    encode_sync_mark(&mut bytes);
    file.write_all(&bytes).unwrap();
    file.sync_data().unwrap();
}

/// Restamps generation 1's first segment to `version`, its header's CRC
/// made to match: the same records, as a build of that version wrote them.
pub(super) fn restamp_segment(disk: &MemDisk, version: u8) {
    let path = Path::new("/data/wal").join(segment_name(1, 0));
    let mut bytes = disk.contents(&path);
    let header = restamped(&bytes[..SEGMENT_HEADER_LEN], version);
    bytes[..SEGMENT_HEADER_LEN].copy_from_slice(&header);
    disk.write_file(&path, &bytes).unwrap();
}

/// `a`, a mark, `b`: what the writer leaves when a sync is issued between
/// two records of shard 0 and 1.
fn two_records_around_a_mark(disk: &MemDisk) {
    let mut seg = wal(disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    write_mark(&mut seg);
    write(&mut seg, 1, 0, &put(b"b", b"1"));
}

#[test]
fn a_sync_mark_in_a_version_5_segment_is_stepped_over_and_counted_nowhere() {
    let disk = MemDisk::default();
    two_records_around_a_mark(&disk);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(
        (
            recovery.report.records,
            recovery.report.malformed,
            recovery.report.applied
        ),
        (2, 0, 2)
    );
    assert!(
        recovery
            .shards
            .iter()
            .all(|shard| !shard.lossy && !shard.cut)
    );
}

#[test]
fn a_sync_mark_in_a_version_4_segment_is_malformed_as_before() {
    // Version 4 reserved nothing: the record is one of a shard outside the
    // node, read and counted malformed as any such record is, and charged
    // to no shard.
    let disk = MemDisk::default();
    two_records_around_a_mark(&disk);
    restamp_segment(&disk, 4);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(
        (
            recovery.report.records,
            recovery.report.malformed,
            recovery.report.applied
        ),
        (3, 1, 2)
    );
    assert!(
        recovery
            .shards
            .iter()
            .all(|shard| !shard.lossy && !shard.cut)
    );
}

#[test]
fn a_version_4_segment_is_recovered() {
    // The layout this one shares, written by the build before the marks:
    // the cut points are read and the records replay as before.
    let disk = MemDisk::default();
    let dir = Path::new("/data/wal");
    disk.create_dir_all(dir).unwrap();
    let mut seg = create_segment(&disk, dir, 1, 0, &[(1, 0)]).unwrap();
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    let mut rebase = Vec::new();
    Effect::Rebase.encode(&mut rebase);
    write(&mut seg, 1, 0, &rebase);
    let path = dir.join(segment_name(1, 0));
    let mut bytes = disk.contents(&path);
    let fixed = restamped(&bytes[..SEGMENT_HEADER_LEN], 4);
    bytes[..SEGMENT_HEADER_LEN].copy_from_slice(&fixed);
    disk.write_file(&path, &bytes).unwrap();

    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(recovery.report.malformed, 0);
    assert_eq!(contents(&recovery.shards[0].dict), pairs(&[(b"a", b"1")]));
    assert!(recovery.shards.iter().all(|shard| !shard.lossy));
}
