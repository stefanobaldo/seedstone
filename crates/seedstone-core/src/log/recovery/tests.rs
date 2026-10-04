use super::*;
use crate::dict::{Dict, DictSeed};
use crate::log::disk::LogFile;
use crate::log::disk::mem::{MemDisk, MemFile};
use crate::log::effect::Effect;
use crate::log::file::{
    FORMAT_VERSION, SEGMENT_HEADER_LEN, create_segment, encode_segment_header, segment_name,
};
use crate::log::snapshot::{Footer, SnapshotHeader, encode_entry, snapshot_name};
use crate::log::{Record, encode_record};
use crate::shard::Now;
use tokio::time::Instant;

fn now() -> Now {
    Now {
        instant: Instant::now(),
        unix_millis: 1_000_000,
    }
}

fn spec(disk: &MemDisk, shards: u16) -> RecoverSpec<'_, MemDisk> {
    RecoverSpec {
        disk,
        wal: Path::new("/data/wal"),
        shards,
        reader: ReaderMode::Resynchronising,
        trust_unfinished: false,
        seed: DictSeed { k0: 9, k1: 3 },
        now: now(),
    }
}

/// What the dict holds, as `(key, value)` pairs sorted by key.
fn contents(dict: &Dict) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut all = Vec::new();
    let mut cursor = 0;
    loop {
        cursor = dict.scan(cursor, |key, entry| {
            all.push((key.to_vec(), entry.value.to_vec()));
        });
        if cursor == 0 {
            break;
        }
    }
    all.sort();
    all
}

fn pairs(items: &[(&[u8], &[u8])]) -> Vec<(Vec<u8>, Vec<u8>)> {
    items
        .iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect()
}

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

fn del(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    Effect::Del { key }.encode(&mut out);
    out
}

/// A wal directory with `generation`'s first segment, to write into.
fn wal(disk: &MemDisk, generation: u64) -> MemFile {
    let dir = Path::new("/data/wal");
    disk.create_dir_all(dir).unwrap();
    create_segment(disk, dir, generation, 0).unwrap()
}

/// The next rotation of `generation`'s segment, as the writer opens it.
fn rotate(disk: &MemDisk, generation: u64, rotation: u32) -> MemFile {
    create_segment(disk, Path::new("/data/wal"), generation, rotation).unwrap()
}

/// One record of `shard`, written and synced.
fn write(file: &mut MemFile, shard: u16, seq: u64, payload: &[u8]) {
    let mut bytes = Vec::new();
    encode_record(
        &Record {
            shard,
            seq,
            payload,
        },
        &mut bytes,
    );
    file.write_all(&bytes).unwrap();
    file.sync_data().unwrap();
}

/// One image entry: `(shard, key, value, deadline)`.
type ImageEntry<'a> = (u16, &'a [u8], &'a [u8], Option<u64>);

/// Writes a snapshot file by hand: header with `bases`, one entry per
/// `(shard, key, value, deadline)`, and the footer with the counts if
/// `finished`.
fn snapshot(
    disk: &MemDisk,
    generation: u64,
    executor: u16,
    cycle: u32,
    bases: &[(u16, u64)],
    entries: &[ImageEntry<'_>],
    finished: bool,
) {
    let header = SnapshotHeader {
        generation,
        executor,
        cycle,
        bases: bases.to_vec(),
    };
    let mut bytes = Vec::new();
    header.encode(&mut bytes);
    let mut scratch = Vec::new();
    let mut counts: Vec<(u16, u64)> = bases.iter().map(|(shard, _)| (*shard, 0)).collect();
    for (shard, key, value, deadline) in entries {
        let base = bases
            .iter()
            .find(|(s, _)| s == shard)
            .map_or(0, |(_, b)| *b);
        encode_entry(
            *shard,
            base,
            key,
            value,
            *deadline,
            &mut scratch,
            &mut bytes,
        );
        if let Some(count) = counts.iter_mut().find(|(s, _)| s == shard) {
            count.1 += 1;
        }
    }
    if finished {
        Footer { counts }.encode_record(cycle, &mut bytes);
        bytes.push(crate::log::END_OF_LOG);
    }
    disk.write_file(
        &Path::new("/data/wal").join(snapshot_name(generation, executor, cycle)),
        &bytes,
    )
    .unwrap();
}

#[test]
fn records_are_recovered_per_shard_in_sequence_order_across_segments() {
    let disk = MemDisk::default();
    // Generation 1: both shards in the node's segment.
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    write(&mut seg, 1, 0, &put(b"b", b"1"));
    write(&mut seg, 0, 1, &put(b"a", b"2"));
    // Generation 2: the next process's segment.
    let mut later = wal(&disk, 2);
    write(&mut later, 1, 1, &put(b"b", b"2"));
    write(&mut later, 0, 2, &put(b"a", b"3"));

    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(recovery.report.segments, 2);
    assert_eq!(recovery.report.records, 5);
    assert_eq!(recovery.report.applied, 5);
    assert_eq!(recovery.shards[0].seq, 3);
    assert_eq!(contents(&recovery.shards[0].dict), pairs(&[(b"a", b"3")]));
    assert_eq!(recovery.shards[1].seq, 2);
    assert_eq!(contents(&recovery.shards[1].dict), pairs(&[(b"b", b"2")]));
    assert!(recovery.report.truncated.is_empty());
    assert!(!recovery.shards[0].lossy);
}

/// A start whose recovery cut a shard leaves the cut records on disk.
/// The next generation resumes at the cut and reuses those sequence
/// numbers, so on the start after it the old records must lose to the
/// new ones — or the node replays what it had discarded over writes it
/// acknowledged and synced.
#[test]
fn records_a_cut_left_behind_never_return_on_a_later_start() {
    let disk = MemDisk::default();
    let mut gen1 = wal(&disk, 1);
    write(&mut gen1, 0, 0, &put(b"a", b"1"));
    write(&mut gen1, 0, 1, &put(b"a", b"2"));
    write(&mut gen1, 0, 3, &put(b"c", b"stale")); // seq 2 never written
    write(&mut gen1, 0, 4, &put(b"d", b"stale"));
    let first = recover(spec(&disk, 1)).unwrap();
    assert_eq!(first.shards[0].seq, 2, "cut at the gap");

    // The next generation resumes at 2, as the node does after a cut.
    let mut gen2 = wal(&disk, 2);
    let mut rebase = Vec::new();
    Effect::Rebase.encode(&mut rebase);
    write(&mut gen2, 0, 2, &rebase);
    write(&mut gen2, 0, 3, &put(b"b", b"new"));

    let second = recover(spec(&disk, 1)).unwrap();
    let shard = &second.shards[0];
    assert_eq!(
        shard.seq, 4,
        "the new generation's prefix, and nothing past it"
    );
    assert_eq!(
        contents(&shard.dict),
        pairs(&[(b"a", b"2"), (b"b", b"new")]),
        "the acknowledged write, not the records the first start discarded"
    );
    assert!(!shard.cut);
    assert!(
        second.report.truncated.is_empty(),
        "the dead records are not a gap"
    );
}

#[test]
fn a_gap_truncates_that_shard_and_nothing_else() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    write(&mut seg, 0, 1, &put(b"a", b"2"));
    write(&mut seg, 1, 0, &put(b"b", b"1"));
    write(&mut seg, 0, 3, &put(b"a", b"4")); // seq 2 never written
    write(&mut seg, 1, 1, &put(b"b", b"2"));

    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(recovery.shards[0].seq, 2);
    assert!(recovery.shards[0].cut);
    assert!(
        !recovery.shards[0].lossy,
        "the segment is intact, so nothing on disk explains the gap: it is reported, \
         not excused"
    );
    assert_eq!(recovery.shards[1].seq, 2);
    assert!(!recovery.shards[1].lossy);
    assert_eq!(
        recovery.report.truncated,
        [ShardTruncation {
            shard: 0,
            applied: 2,
            discarded: 1
        }]
    );
}

#[test]
fn damage_loses_only_the_records_in_the_hole() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    write(&mut seg, 1, 0, &put(b"b", b"1"));
    write(&mut seg, 1, 1, &put(b"b", b"2"));
    let path = Path::new("/data/wal").join(segment_name(1, 0));
    let mut bytes = disk.contents(&path);
    // Flip a byte inside the first record's payload.
    bytes[SEGMENT_HEADER_LEN + 9 + 10 + 1] ^= 0xFF;
    disk.overwrite(&path, bytes);

    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(recovery.report.holes, 1);
    assert_eq!(recovery.shards[0].seq, 0);
    assert!(
        recovery.shards[0].dict.is_empty(),
        "shard 0's only record was in the hole"
    );
    assert_eq!(
        recovery.shards[1].seq, 2,
        "shard 1's records outside the hole replay"
    );
}

/// Flips a byte inside the payload of the record that starts `at` bytes
/// into the segment.
fn corrupt_record_at(disk: &MemDisk, path: &Path, at: usize) {
    let mut bytes = disk.contents(path);
    bytes[at + 9 + 10 + 1] ^= 0xFF;
    disk.overwrite(path, bytes);
}

#[test]
fn a_hole_is_charged_to_the_shards_with_nothing_after_it() {
    // One file holds every shard's records, so a hole could have held any
    // of them. A shard with an intact record after the hole would show
    // what it lost there as a gap; one whose last record precedes it, or
    // with none at all, would show nothing — those are the lossy ones.
    let disk = MemDisk::default();
    let path = Path::new("/data/wal").join(segment_name(1, 0));
    let mut seg = wal(&disk, 1);
    write(&mut seg, 1, 0, &put(b"b", b"1"));
    let hole = disk.contents(&path).len();
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    write(&mut seg, 2, 0, &put(b"c", b"1"));
    corrupt_record_at(&disk, &path, hole);

    let recovery = recover(spec(&disk, 3)).unwrap();
    assert_eq!(recovery.report.holes, 1);
    let lossy: Vec<bool> = recovery.shards.iter().map(|shard| shard.lossy).collect();
    assert_eq!(
        lossy,
        [true, true, false],
        "shard 0 has nothing intact, shard 1's last record precedes the hole, shard 2 \
         has a record after it and no gap"
    );
    assert_eq!(recovery.shards[2].seq, 1);
}

#[test]
fn a_gap_behind_a_hole_is_a_loss_the_shard_reports() {
    // Shard 0's second record is in the hole and its third is intact
    // after it: the gap is where it lost something, and with damage on
    // disk that is a loss, not a cut nothing explains.
    let disk = MemDisk::default();
    let path = Path::new("/data/wal").join(segment_name(1, 0));
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    let hole = disk.contents(&path).len();
    write(&mut seg, 0, 1, &put(b"a", b"2"));
    write(&mut seg, 0, 2, &put(b"a", b"3"));
    write(&mut seg, 1, 0, &put(b"b", b"1"));
    corrupt_record_at(&disk, &path, hole);

    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(recovery.shards[0].seq, 1);
    assert!(recovery.shards[0].cut);
    assert!(recovery.shards[0].lossy);
    assert!(
        !recovery.shards[1].lossy,
        "shard 1's only record is after the hole, and it starts at zero"
    );
}

#[test]
fn a_hole_across_rotations_is_judged_by_the_later_rotation() {
    // A hole in the first rotation; shard 1 continues in the second with
    // its next sequence, so it lost nothing there.
    let disk = MemDisk::default();
    let first = Path::new("/data/wal").join(segment_name(1, 0));
    let mut seg = wal(&disk, 1);
    write(&mut seg, 1, 0, &put(b"b", b"1"));
    let hole = disk.contents(&first).len();
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    let mut next = rotate(&disk, 1, 1);
    write(&mut next, 1, 1, &put(b"b", b"2"));
    corrupt_record_at(&disk, &first, hole);

    let recovery = recover(spec(&disk, 2)).unwrap();
    assert!(recovery.shards[0].lossy);
    assert!(!recovery.shards[1].lossy);
    assert_eq!(recovery.shards[1].seq, 2);
}

#[test]
fn a_cut_tail_is_charged_to_every_shard() {
    // A record the segment ends inside of: nothing intact follows, so any
    // shard's last records may have been in it.
    let disk = MemDisk::default();
    let path = Path::new("/data/wal").join(segment_name(1, 0));
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    write(&mut seg, 1, 0, &put(b"b", b"1"));
    let mut bytes = disk.contents(&path);
    bytes.truncate(bytes.len() - 3);
    disk.overwrite(&path, bytes);

    let recovery = recover(spec(&disk, 2)).unwrap();
    assert!(recovery.shards.iter().all(|shard| shard.lossy));
}

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
    encode_segment_header(2, 0, &mut header);
    header[4] = FORMAT_VERSION + 1;
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

/// A read can fail where the next one succeeds: a segment this start could
/// not read is never removed as if nothing needed it.
#[test]
fn a_segment_that_could_not_be_read_is_kept_for_the_next_start() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    let path = Path::new("/data/wal").join(segment_name(1, 0));
    let good = disk.contents(&path);
    let mut bad = good.clone();
    bad[0] ^= 0xFF;
    disk.overwrite(&path, bad);
    let recovery = recover(spec(&disk, 1)).unwrap();
    assert_eq!(recovery.report.abandoned_segments, 1);
    assert_eq!(recovery.report.files_removed, 0);
    // The header reads again: the record is still there.
    disk.overwrite(&path, good);
    let recovery = recover(spec(&disk, 1)).unwrap();
    assert_eq!(contents(&recovery.shards[0].dict), pairs(&[(b"a", b"1")]));
    assert!(!recovery.shards[0].lossy);
}

#[test]
fn a_malformed_payload_ends_that_shards_prefix() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    write(&mut seg, 0, 1, &[99]); // no such tag
    write(&mut seg, 0, 2, &put(b"a", b"3"));
    let recovery = recover(spec(&disk, 1)).unwrap();
    assert_eq!(recovery.report.malformed, 1);
    assert_eq!(recovery.shards[0].seq, 1);
    assert!(recovery.shards[0].cut);
    assert!(
        recovery.shards[0].lossy,
        "recovery read the record and could not apply it: the loss is known"
    );
}

#[test]
fn a_missing_directory_is_an_error_and_an_empty_one_is_a_fresh_node() {
    let disk = MemDisk::default();
    assert!(recover(spec(&disk, 4)).is_err());
    disk.create_dir_all(Path::new("/data/wal")).unwrap();
    let recovery = recover(spec(&disk, 4)).unwrap();
    assert_eq!(recovery.shards.len(), 4);
    assert!(
        recovery
            .shards
            .iter()
            .all(|shard| shard.seq == 0 && shard.dict.is_empty() && !shard.lossy && !shard.cut)
    );
    assert_eq!(recovery.report.segments, 0);
}

/// A snapshot with its tail: the image is inserted, the tail from the
/// base replays over it, and the segment the image covers is removed.
#[test]
fn an_image_plus_its_tail_replays_to_the_state_and_the_covered_segment_goes() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    write(&mut seg, 0, 1, &put(b"a", b"2"));
    write(&mut seg, 0, 2, &put(b"b", b"1"));
    write(&mut seg, 0, 3, &put(b"c", b"1"));
    // The cycle: rotation 1 from seq 4, the image taken fuzzily.
    snapshot(
        &disk,
        1,
        0,
        0,
        &[(0, 4)],
        &[
            (0, b"a", b"2", None),
            (0, b"b", b"1", None),
            (0, b"c", b"1", None),
        ],
        true,
    );
    seg = rotate(&disk, 1, 1);
    write(&mut seg, 0, 4, &del(b"b"));
    write(&mut seg, 0, 5, &put(b"c", b"9"));

    let recovery = recover(spec(&disk, 1)).unwrap();
    let shard = &recovery.shards[0];
    assert_eq!(contents(&shard.dict), pairs(&[(b"a", b"2"), (b"c", b"9")]));
    assert_eq!(shard.seq, 6);
    assert!(!shard.cut && !shard.lossy);
    assert_eq!(recovery.report.snapshots_used, 1);
    assert_eq!(recovery.report.applied, 2, "the tail, not the image");
    assert_eq!(
        recovery.report.files_removed, 1,
        "rotation 0 is below every base"
    );
    let names = disk.list(Path::new("/data/wal")).unwrap();
    assert!(!names.contains(&segment_name(1, 0)), "{names:?}");
    assert!(names.contains(&segment_name(1, 1)) && names.contains(&snapshot_name(1, 0, 0)));
}

#[test]
fn a_snapshot_without_a_footer_is_ignored_and_removed_and_the_log_carries_the_shard() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    write(&mut seg, 0, 1, &put(b"b", b"1"));
    snapshot(&disk, 1, 0, 0, &[(0, 2)], &[(0, b"a", b"1", None)], false);
    let recovery = recover(spec(&disk, 1)).unwrap();
    assert_eq!(
        contents(&recovery.shards[0].dict),
        pairs(&[(b"a", b"1"), (b"b", b"1")])
    );
    assert_eq!(recovery.shards[0].seq, 2);
    assert_eq!(
        (
            recovery.report.snapshots_used,
            recovery.report.snapshots_refused
        ),
        (0, 1)
    );
    assert!(
        !recovery.shards[0].lossy && !recovery.shards[0].cut,
        "a cycle a crash interrupted never covered a record: refusing it loses nothing"
    );
    assert!(
        !disk
            .list(Path::new("/data/wal"))
            .unwrap()
            .contains(&snapshot_name(1, 0, 0))
    );
}

/// The unsynced entries of an unfinished cycle can be torn by the crash
/// that interrupted it: the torn tail is still a crash, not damage.
#[test]
fn a_torn_unfinished_snapshot_is_refused_without_a_loss() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    snapshot(
        &disk,
        1,
        0,
        0,
        &[(0, 1)],
        &[(0, b"a", b"1", None), (0, b"b", b"1", None)],
        false,
    );
    let path = Path::new("/data/wal").join(snapshot_name(1, 0, 0));
    let mut bytes = disk.contents(&path);
    bytes.truncate(bytes.len() - 3);
    disk.overwrite(&path, bytes);
    let recovery = recover(spec(&disk, 1)).unwrap();
    assert_eq!(recovery.report.snapshots_refused, 1);
    assert_eq!(contents(&recovery.shards[0].dict), pairs(&[(b"a", b"1")]));
    assert!(!recovery.shards[0].lossy && !recovery.shards[0].cut);
}

/// A crash can persist a file's unsynced writes out of order: a later
/// write kept, an earlier one lost, and the gap between them zeros. An
/// unfinished snapshot then has a hole before where it stops. The log it
/// would have covered is still whole, so nothing is lost.
#[test]
fn an_unfinished_snapshot_with_a_hole_is_refused_without_a_loss_while_the_log_covers_it() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    write(&mut seg, 0, 1, &put(b"b", b"1"));
    write(&mut seg, 0, 2, &put(b"c", b"1"));
    snapshot(
        &disk,
        1,
        0,
        0,
        &[(0, 3)],
        &[
            (0, b"a", b"1", None),
            (0, b"b", b"1", None),
            (0, b"c", b"1", None),
        ],
        false,
    );
    let path = Path::new("/data/wal").join(snapshot_name(1, 0, 0));
    let mut bytes = disk.contents(&path);
    // Zero the middle of the entries: the first survives, the last too.
    let entries = bytes.len()
        - SnapshotHeader {
            generation: 1,
            executor: 0,
            cycle: 0,
            bases: vec![(0, 3)],
        }
        .encoded_len();
    let middle = bytes.len() - entries * 2 / 3;
    bytes[middle..middle + entries / 3].fill(0);
    disk.overwrite(&path, bytes);
    let recovery = recover(spec(&disk, 1)).unwrap();
    assert_eq!(recovery.report.snapshots_refused, 1);
    assert_eq!(
        contents(&recovery.shards[0].dict),
        pairs(&[(b"a", b"1"), (b"b", b"1"), (b"c", b"1")])
    );
    assert_eq!(recovery.shards[0].seq, 3);
    assert!(
        !recovery.shards[0].lossy && !recovery.shards[0].cut,
        "the log still holds every record the refused image covered"
    );
}

#[test]
fn the_planted_recovery_trusts_an_unfinished_snapshot_and_loses_the_rest() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    write(&mut seg, 0, 1, &put(b"b", b"1"));
    snapshot(&disk, 1, 0, 0, &[(0, 2)], &[(0, b"a", b"1", None)], false);
    let mut planted = spec(&disk, 1);
    planted.trust_unfinished = true;
    let recovery = recover(planted).unwrap();
    assert_eq!(
        contents(&recovery.shards[0].dict),
        pairs(&[(b"a", b"1")]),
        "b never made the image"
    );
    assert_eq!(recovery.report.snapshots_used, 1);
}

#[test]
fn a_snapshot_whose_counts_do_not_match_falls_back_to_the_older_image() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    let log: [(&[u8], &[u8]); 6] = [
        (b"a", b"1"),
        (b"b", b"1"),
        (b"c", b"1"),
        (b"d", b"1"),
        (b"a", b"2"),
        (b"e", b"1"),
    ];
    for (seq, (key, value)) in (0u64..).zip(log) {
        write(&mut seg, 0, seq, &put(key, value));
    }
    snapshot(
        &disk,
        1,
        0,
        0,
        &[(0, 2)],
        &[(0, b"a", b"1", None), (0, b"b", b"1", None)],
        true,
    );
    // Cycle 1 claims three entries and holds two: damaged in a way the
    // record checksums cannot see.
    let header = SnapshotHeader {
        generation: 1,
        executor: 0,
        cycle: 1,
        bases: vec![(0, 4)],
    };
    let mut bytes = Vec::new();
    header.encode(&mut bytes);
    let mut scratch = Vec::new();
    encode_entry(0, 4, b"a", b"1", None, &mut scratch, &mut bytes);
    encode_entry(0, 4, b"b", b"1", None, &mut scratch, &mut bytes);
    Footer {
        counts: vec![(0, 3)],
    }
    .encode_record(1, &mut bytes);
    bytes.push(crate::log::END_OF_LOG);
    disk.write_file(&Path::new("/data/wal").join(snapshot_name(1, 0, 1)), &bytes)
        .unwrap();

    let recovery = recover(spec(&disk, 1)).unwrap();
    assert_eq!(
        contents(&recovery.shards[0].dict),
        pairs(&[
            (b"a", b"2"),
            (b"b", b"1"),
            (b"c", b"1"),
            (b"d", b"1"),
            (b"e", b"1")
        ])
    );
    assert_eq!(recovery.shards[0].seq, 6);
    assert_eq!(
        (
            recovery.report.snapshots_used,
            recovery.report.snapshots_refused
        ),
        (1, 1)
    );
    assert!(
        !recovery.shards[0].lossy,
        "a refused image explains no loss while the log still reaches its base"
    );
    let names = disk.list(Path::new("/data/wal")).unwrap();
    assert!(
        names.contains(&snapshot_name(1, 0, 1)) && names.contains(&snapshot_name(1, 0, 0)),
        "a finished image refused for damage may read whole on the next start, \
         and may be the only copy of what it covers: kept, {names:?}"
    );
}

#[test]
fn a_shard_whose_only_image_is_refused_and_whose_log_was_compacted_is_cut_lossy_and_reported() {
    let disk = MemDisk::default();
    // Rotation 0 was deleted by a compaction; rotation 1 holds 4 and 5.
    wal(&disk, 1);
    let mut seg = rotate(&disk, 1, 1);
    disk.remove_file(&Path::new("/data/wal").join(segment_name(1, 0)))
        .unwrap();
    write(&mut seg, 0, 4, &put(b"x", b"4"));
    write(&mut seg, 0, 5, &put(b"y", b"5"));
    // The finished image the compaction relied on, its footer damaged.
    snapshot(&disk, 1, 0, 0, &[(0, 4)], &[(0, b"a", b"1", None)], true);
    let path = Path::new("/data/wal").join(snapshot_name(1, 0, 0));
    let mut bytes = disk.contents(&path);
    let footer_byte = bytes.len() - 3;
    bytes[footer_byte] ^= 0xFF;
    disk.overwrite(&path, bytes);
    let recovery = recover(spec(&disk, 1)).unwrap();
    let shard = &recovery.shards[0];
    assert!(
        shard.dict.is_empty(),
        "nothing from 0 survives, and 4 is past the gap"
    );
    assert_eq!(shard.seq, 0);
    assert!(shard.cut && shard.lossy);
    assert_eq!(
        recovery.report.truncated,
        [ShardTruncation {
            shard: 0,
            applied: 0,
            discarded: 2
        }]
    );
}

#[test]
fn a_snapshot_is_dead_for_a_shard_a_newer_generation_rebased_below_its_base() {
    let disk = MemDisk::default();
    // Generation 1: records 0..=1, then a snapshot with base 4 whose image
    // holds a value only the (missing) records 2..=3 could have produced.
    let mut gen1 = wal(&disk, 1);
    write(&mut gen1, 0, 0, &put(b"x", b"old"));
    write(&mut gen1, 0, 1, &put(b"y", b"old"));
    snapshot(
        &disk,
        1,
        0,
        0,
        &[(0, 4)],
        &[(0, b"x", b"stale", None), (0, b"y", b"old", None)],
        true,
    );
    // Generation 2 refused that snapshot (say its read was corrupted),
    // cut the log at the gap at 2, and rebased there.
    let mut gen2 = wal(&disk, 2);
    let mut rebase = Vec::new();
    Effect::Rebase.encode(&mut rebase);
    write(&mut gen2, 0, 2, &rebase);
    write(&mut gen2, 0, 3, &put(b"x", b"new"));

    let recovery = recover(spec(&disk, 1)).unwrap();
    let shard = &recovery.shards[0];
    assert_eq!(
        contents(&shard.dict),
        pairs(&[(b"x", b"new"), (b"y", b"old")])
    );
    assert_eq!(shard.seq, 4);
    assert_eq!(recovery.report.snapshots_used, 0, "the image was dead");
    assert!(
        !disk
            .list(Path::new("/data/wal"))
            .unwrap()
            .contains(&snapshot_name(1, 0, 0)),
        "and removed"
    );
}

#[test]
fn a_header_naming_a_shard_outside_the_node_is_refused() {
    let disk = MemDisk::default();
    wal(&disk, 1);
    snapshot(&disk, 1, 0, 0, &[(5, 0)], &[], true);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(recovery.report.snapshots_refused, 1);
    assert!(
        recovery.shards.iter().all(|shard| !shard.lossy),
        "no shard of this node was imaged by it"
    );
    assert!(
        !disk
            .list(Path::new("/data/wal"))
            .unwrap()
            .contains(&snapshot_name(1, 0, 0))
    );
}

/// The header is synced before the file's name is, so a header that does
/// not read is damage — and nobody can say which shards it imaged.
#[test]
fn a_snapshot_header_that_fails_its_checksum_makes_every_shard_lossy() {
    let disk = MemDisk::default();
    wal(&disk, 1);
    snapshot(&disk, 1, 0, 0, &[(0, 0), (1, 0)], &[], true);
    let path = Path::new("/data/wal").join(snapshot_name(1, 0, 0));
    let mut bytes = disk.contents(&path);
    bytes[6] ^= 0xFF; // inside the generation
    disk.overwrite(&path, bytes);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(recovery.report.snapshots_refused, 1);
    assert!(recovery.shards.iter().all(|shard| shard.lossy && shard.cut));
}

#[test]
fn a_crash_between_the_two_directory_syncs_leaves_both_snapshots_and_the_newer_wins() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    write(&mut seg, 0, 1, &put(b"b", b"1"));
    snapshot(
        &disk,
        1,
        0,
        0,
        &[(0, 2)],
        &[(0, b"a", b"1", None), (0, b"b", b"1", None)],
        true,
    );
    seg = rotate(&disk, 1, 1);
    write(&mut seg, 0, 2, &put(b"a", b"2"));
    write(&mut seg, 0, 3, &put(b"c", b"1"));
    snapshot(
        &disk,
        1,
        0,
        1,
        &[(0, 4)],
        &[
            (0, b"a", b"2", None),
            (0, b"b", b"1", None),
            (0, b"c", b"1", None),
        ],
        true,
    );
    seg = rotate(&disk, 1, 2);
    write(&mut seg, 0, 4, &del(b"a"));

    let recovery = recover(spec(&disk, 1)).unwrap();
    assert_eq!(
        contents(&recovery.shards[0].dict),
        pairs(&[(b"b", b"1"), (b"c", b"1")])
    );
    assert_eq!(recovery.shards[0].seq, 5);
    assert_eq!(recovery.report.snapshots_used, 1);
    assert_eq!(
        recovery.report.files_removed, 3,
        "snapshot 0, rotation 0, rotation 1"
    );
    let mut names = disk.list(Path::new("/data/wal")).unwrap();
    names.retain(|n| {
        std::path::Path::new(n)
            .extension()
            .is_some_and(|ext| ext == "seg" || ext == "snap")
    });
    names.sort();
    // Sorted by name: the generations tie, and a snapshot's executor field
    // sorts before a segment's rotation.
    assert_eq!(names, [snapshot_name(1, 0, 1), segment_name(1, 2)]);
}

#[test]
fn an_executor_count_change_finds_a_shards_image_in_an_older_generations_file() {
    let disk = MemDisk::default();
    // Generation 1, two executors: shard 1 lived on executor 1, which
    // snapshotted it.
    let mut gen1 = wal(&disk, 1);
    write(&mut gen1, 1, 0, &put(b"k", b"v"));
    snapshot(&disk, 1, 1, 0, &[(1, 1)], &[(1, b"k", b"v", None)], true);
    // Generation 2, one executor: it wrote shard 1's record 1 and never
    // completed a cycle.
    let mut gen2 = wal(&disk, 2);
    write(&mut gen2, 1, 1, &put(b"k2", b"v2"));

    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(
        contents(&recovery.shards[1].dict),
        pairs(&[(b"k", b"v"), (b"k2", b"v2")])
    );
    assert_eq!(recovery.shards[1].seq, 2);
    assert_eq!(recovery.report.snapshots_used, 1);
    let names = disk.list(Path::new("/data/wal")).unwrap();
    assert!(
        names.contains(&snapshot_name(1, 1, 0)),
        "used, so kept until the round: {names:?}"
    );
}

#[test]
fn a_flush_in_the_tail_clears_the_image() {
    let disk = MemDisk::default();
    wal(&disk, 1);
    snapshot(
        &disk,
        1,
        0,
        0,
        &[(0, 2)],
        &[(0, b"a", b"1", None), (0, b"b", b"1", None)],
        true,
    );
    let mut seg = rotate(&disk, 1, 1);
    let mut flush = Vec::new();
    Effect::Flush.encode(&mut flush);
    write(&mut seg, 0, 2, &flush);
    write(&mut seg, 0, 3, &put(b"c", b"1"));
    let recovery = recover(spec(&disk, 1)).unwrap();
    assert_eq!(contents(&recovery.shards[0].dict), pairs(&[(b"c", b"1")]));
}

#[test]
fn a_passed_deadline_in_the_image_is_removed_unless_the_tail_moved_it() {
    let disk = MemDisk::default();
    wal(&disk, 1);
    snapshot(
        &disk,
        1,
        0,
        0,
        &[(0, 2)],
        &[
            (0, b"stale", b"v", Some(999_000)),
            (0, b"moved", b"v", Some(999_000)),
            (0, b"live", b"v", Some(5_000_000)),
        ],
        true,
    );
    let mut seg = rotate(&disk, 1, 1);
    let mut moved = Vec::new();
    Effect::Deadline {
        key: b"moved",
        deadline: Some(2_000_000),
    }
    .encode(&mut moved);
    write(&mut seg, 0, 2, &moved);
    let recovery = recover(spec(&disk, 1)).unwrap();
    let dict = &recovery.shards[0].dict;
    assert!(dict.get(b"stale").is_none());
    assert!(dict.get(b"moved").is_some());
    assert!(dict.get(b"live").is_some());
}

#[test]
fn a_snapshot_from_a_newer_format_version_refuses_the_start() {
    let disk = MemDisk::default();
    wal(&disk, 1);
    let header = SnapshotHeader {
        generation: 1,
        executor: 0,
        cycle: 0,
        bases: vec![(0, 0)],
    };
    let mut bytes = Vec::new();
    header.encode(&mut bytes);
    bytes[4] = FORMAT_VERSION + 1;
    disk.write_file(&Path::new("/data/wal").join(snapshot_name(1, 0, 0)), &bytes)
        .unwrap();
    let error = recover(spec(&disk, 1)).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
}

/// A finished snapshot whose footer was lost — here cut short, so the file
/// reads as one a crash interrupted — after compaction removed the log it
/// covered. Nothing on disk says whether it was ever finished, but the log
/// stops short of its base: the shard is lossy, and the file stays, since
/// the next read of it may be whole and it is the only copy.
#[test]
fn an_unfinished_looking_snapshot_whose_log_is_gone_is_a_loss_and_is_kept() {
    let disk = MemDisk::default();
    wal(&disk, 1);
    let mut seg = rotate(&disk, 1, 1);
    disk.remove_file(&Path::new("/data/wal").join(segment_name(1, 0)))
        .unwrap();
    write(&mut seg, 0, 4, &put(b"x", b"4"));
    snapshot(&disk, 1, 0, 0, &[(0, 4)], &[(0, b"a", b"1", None)], true);
    let path = Path::new("/data/wal").join(snapshot_name(1, 0, 0));
    let mut bytes = disk.contents(&path);
    bytes.truncate(bytes.len() - 4);
    disk.overwrite(&path, bytes);
    let recovery = recover(spec(&disk, 1)).unwrap();
    assert_eq!(recovery.report.snapshots_refused, 1);
    let shard = &recovery.shards[0];
    assert_eq!(shard.seq, 0);
    assert!(
        shard.lossy,
        "the log stops short of the refused image's base"
    );
    let names = disk.list(Path::new("/data/wal")).unwrap();
    assert!(names.contains(&snapshot_name(1, 0, 0)), "kept: {names:?}");
}

/// A segment shorter than its header is a creation that failed before the
/// header was written whole: nothing is appended to a segment before its
/// header is synced, so it never held a record, and no shard lost one.
#[test]
fn a_segment_shorter_than_its_header_held_nothing_and_is_not_damage() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"1"));
    let mut partial = Vec::new();
    crate::log::file::encode_segment_header(1, 1, &mut partial);
    partial.truncate(7);
    disk.write_file(&Path::new("/data/wal").join(segment_name(1, 1)), &partial)
        .unwrap();
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(
        recovery.report.abandoned_segments, 0,
        "{:?}",
        recovery.report
    );
    assert!(recovery.shards.iter().all(|s| !s.lossy && !s.cut));
    assert_eq!(contents(&recovery.shards[0].dict), pairs(&[(b"a", b"1")]));
}
