//! A crash's tear against damage to synced records: in the log's last
//! segment, while its generation is the counter's and from version 5,
//! damage that begins at or past the last sync mark is charged to no shard
//! and still cuts the shards it would have charged; everywhere else it is
//! charged as a loss.

use super::format::{restamp_segment, write_mark};
use super::*;
use crate::log::HEADER_LEN;

fn counter(disk: &MemDisk, generation: u64) {
    disk.write_file(
        &Path::new("/data/wal").join(GENERATION_FILE),
        generation.to_string().as_bytes(),
    )
    .unwrap();
}

fn path(generation: u64, rotation: u32) -> std::path::PathBuf {
    Path::new("/data/wal").join(segment_name(generation, rotation))
}

/// Where the next write to generation 1's first segment lands.
fn end(disk: &MemDisk, generation: u64, rotation: u32) -> usize {
    disk.contents(&path(generation, rotation)).len()
}

fn lossy(recovery: &Recovery) -> Vec<bool> {
    recovery.shards.iter().map(|shard| shard.lossy).collect()
}

fn cut(recovery: &Recovery) -> Vec<bool> {
    recovery.shards.iter().map(|shard| shard.cut).collect()
}

/// `a0`, `b0`, a mark, `a1`, and `b1` cut short by the crash. Returns the
/// bytes of `b1` that are left.
fn torn_after_a_mark(disk: &MemDisk) -> u64 {
    let mut seg = wal(disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"0"));
    write(&mut seg, 1, 0, &put(b"b", b"0"));
    write_mark(&mut seg);
    write(&mut seg, 0, 1, &put(b"a", b"1"));
    let from = end(disk, 1, 0);
    write(&mut seg, 1, 1, &put(b"b", b"1"));
    let mut bytes = disk.contents(&path(1, 0));
    bytes.truncate(bytes.len() - 3);
    let left = bytes.len() - from;
    disk.overwrite(&path(1, 0), bytes);
    left as u64
}

#[test]
fn a_tear_past_the_last_mark_is_charged_to_nobody_and_reported_as_torn_bytes() {
    // The tear lies past the last sync issued: nothing in it was synced, so
    // no shard lost anything a sync was issued over. Every shard whose
    // highest record precedes it is still cut.
    let disk = MemDisk::default();
    let torn = torn_after_a_mark(&disk);
    counter(&disk, 1);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(lossy(&recovery), [false, false]);
    assert_eq!(cut(&recovery), [true, true], "both precede the tear");
    assert_eq!(recovery.report.torn_bytes, torn);
    assert_eq!(recovery.report.damage_bytes, torn);
    assert_eq!(recovery.report.holes, 0, "a tail is not a hole");
}

#[test]
fn a_last_segment_behind_the_generation_counter_gets_no_tear_class() {
    // A later start advanced the counter and left no segment of its own:
    // it synced this file and may have served from it, so the tail is in
    // records a node has served.
    let disk = MemDisk::default();
    torn_after_a_mark(&disk);
    counter(&disk, 2);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(lossy(&recovery), [true, true]);
    assert_eq!(recovery.report.torn_bytes, 0);
}

#[test]
fn a_last_segment_with_no_generation_counter_gets_no_tear_class() {
    let disk = MemDisk::default();
    torn_after_a_mark(&disk);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(lossy(&recovery), [true, true]);
    assert_eq!(recovery.report.torn_bytes, 0);
}

#[test]
fn a_shard_behind_a_tear_is_cut_and_its_hidden_record_never_returns() {
    // Shard 0's record 2 reads corrupt once: a tear, not a loss, but the
    // shard is cut at 2 and the next generation rebases there. When that
    // generation's 3 is torn and its 4 survives, the next start reads the
    // old 2 whole — and the `Rebase` is what keeps it dead.
    let disk = MemDisk::default();
    let mut gen1 = wal(&disk, 1);
    write_mark(&mut gen1);
    write(&mut gen1, 0, 0, &put(b"a", b"0"));
    write(&mut gen1, 0, 1, &put(b"a", b"1"));
    let hidden = end(&disk, 1, 0);
    write(&mut gen1, 0, 2, &put(b"a", b"old"));
    counter(&disk, 1);
    let whole = disk.contents(&path(1, 0));
    corrupt_record_at(&disk, &path(1, 0), hidden);

    let first = recover(spec(&disk, 1)).unwrap();
    let shard = &first.shards[0];
    assert!(!shard.lossy && shard.cut);
    assert_eq!(shard.seq, 2);

    // The read was transient; generation 2 resumed at 2, as the pool does.
    disk.overwrite(&path(1, 0), whole);
    let mut gen2 = create_segment(&disk, Path::new("/data/wal"), 2, 0, &[(0, 2)]).unwrap();
    write(&mut gen2, 0, 2, &rebase());
    write_mark(&mut gen2);
    let torn = end(&disk, 2, 0);
    write(&mut gen2, 0, 3, &put(b"a", b"new"));
    write(&mut gen2, 0, 4, &put(b"a", b"newer"));
    corrupt_record_at(&disk, &path(2, 0), torn);
    counter(&disk, 2);

    let second = recover(spec(&disk, 1)).unwrap();
    let shard = &second.shards[0];
    assert_eq!(
        contents(&shard.dict),
        pairs(&[(b"a", b"1")]),
        "not `a = old`, which the rebase killed, nor `a = newer` past the gap"
    );
    assert_eq!(shard.seq, 3);
    assert!(shard.cut && !shard.lossy);
    assert_eq!(second.report.discarded, 1);
}

#[test]
fn a_hole_before_the_last_mark_is_a_loss_as_today() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"0"));
    let hole = end(&disk, 1, 0);
    write(&mut seg, 1, 0, &put(b"b", b"0"));
    write(&mut seg, 0, 1, &put(b"a", b"1"));
    write_mark(&mut seg);
    corrupt_record_at(&disk, &path(1, 0), hole);
    counter(&disk, 1);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(lossy(&recovery), [false, true]);
    assert_eq!(recovery.report.torn_bytes, 0);
}

#[test]
fn a_hole_after_the_last_mark_in_a_segment_that_is_not_last_is_a_loss() {
    // The closing file was synced whole behind its rotation; only the file
    // being written can hold a tear.
    let disk = MemDisk::default();
    let mut first = wal(&disk, 1);
    write(&mut first, 0, 0, &put(b"a", b"0"));
    write_mark(&mut first);
    let hole = end(&disk, 1, 0);
    write(&mut first, 1, 0, &put(b"b", b"0"));
    let mut next = rotate(&disk, 1, 1);
    write(&mut next, 0, 1, &put(b"a", b"1"));
    write_mark(&mut next);
    corrupt_record_at(&disk, &path(1, 0), hole);
    counter(&disk, 1);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(lossy(&recovery), [false, true]);
    assert_eq!(recovery.report.torn_bytes, 0);
}

#[test]
fn a_cut_tail_in_a_segment_that_is_not_last_is_charged_to_every_shard() {
    let disk = MemDisk::default();
    let mut first = wal(&disk, 1);
    write(&mut first, 0, 0, &put(b"a", b"0"));
    write(&mut first, 1, 0, &put(b"b", b"0"));
    write_mark(&mut first);
    write(&mut first, 1, 1, &put(b"b", b"1"));
    let mut bytes = disk.contents(&path(1, 0));
    bytes.truncate(bytes.len() - 3);
    disk.overwrite(&path(1, 0), bytes);
    let mut next = rotate(&disk, 1, 1);
    write_mark(&mut next);
    counter(&disk, 1);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(lossy(&recovery), [true, true]);
    assert_eq!(recovery.report.torn_bytes, 0);
}

#[test]
fn a_cut_tail_in_a_version_4_last_segment_is_charged_to_every_shard() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"0"));
    write(&mut seg, 1, 0, &put(b"b", b"0"));
    let mut bytes = disk.contents(&path(1, 0));
    bytes.truncate(bytes.len() - 3);
    disk.overwrite(&path(1, 0), bytes);
    restamp_segment(&disk, 4);
    counter(&disk, 1);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(lossy(&recovery), [true, true]);
    assert_eq!(recovery.report.torn_bytes, 0);
}

/// `a0`, `b0` in a hole, `a1`, and no mark: what `--fsync never` leaves of
/// the segment being written.
fn a_hole_and_no_mark(disk: &MemDisk) {
    let mut seg = wal(disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"0"));
    let hole = end(disk, 1, 0);
    write(&mut seg, 1, 0, &put(b"b", b"0"));
    write(&mut seg, 0, 1, &put(b"a", b"1"));
    corrupt_record_at(disk, &path(1, 0), hole);
    counter(disk, 1);
}

#[test]
fn a_version_5_last_segment_with_no_mark_is_all_tear() {
    // No sync was issued on it, so nothing in it was synced.
    let disk = MemDisk::default();
    a_hole_and_no_mark(&disk);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(lossy(&recovery), [false, false]);
    assert_eq!(
        cut(&recovery),
        [false, true],
        "shard 1 has nothing after the hole; shard 0 has, and no gap"
    );
    assert!(recovery.report.torn_bytes > 0);
    assert_eq!(recovery.report.torn_bytes, recovery.report.damage_bytes);
}

#[test]
fn a_version_4_last_segment_gets_no_tear_class() {
    // Written by the build before the marks: an upgrade without a clean
    // stop is charged as before, once.
    let disk = MemDisk::default();
    a_hole_and_no_mark(&disk);
    restamp_segment(&disk, 4);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(lossy(&recovery), [false, true]);
    assert_eq!(recovery.report.torn_bytes, 0);
}

#[test]
fn a_hole_that_straddles_a_mark_is_a_loss() {
    // The hole begins inside `b0` and swallows the first mark; the second
    // is intact, past the hole's start, so the hole holds bytes a sync was
    // issued over. (A hole that took the *last* mark would leave the point
    // at the mark before, and what follows it rightly reads as a tear.)
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"0"));
    let hole = end(&disk, 1, 0);
    write(&mut seg, 1, 0, &put(b"b", b"0"));
    let mark = end(&disk, 1, 0);
    write_mark(&mut seg);
    write(&mut seg, 0, 1, &put(b"a", b"1"));
    write_mark(&mut seg);
    corrupt_record_at(&disk, &path(1, 0), hole);
    let mut bytes = disk.contents(&path(1, 0));
    bytes[mark + HEADER_LEN] ^= 0xFF; // the first mark's shard id
    disk.overwrite(&path(1, 0), bytes);
    counter(&disk, 1);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(recovery.report.holes, 1);
    assert_eq!(lossy(&recovery), [false, true]);
    assert_eq!(recovery.report.torn_bytes, 0);
}

#[test]
fn a_gap_inside_the_tear_is_cut_but_not_lossy() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write_mark(&mut seg);
    write(&mut seg, 0, 0, &put(b"a", b"0"));
    let hole = end(&disk, 1, 0);
    write(&mut seg, 0, 1, &put(b"a", b"1"));
    write(&mut seg, 0, 2, &put(b"a", b"2"));
    corrupt_record_at(&disk, &path(1, 0), hole);
    counter(&disk, 1);
    let recovery = recover(spec(&disk, 1)).unwrap();
    let shard = &recovery.shards[0];
    assert!(shard.cut && !shard.lossy);
    assert_eq!(shard.seq, 1);
    assert_eq!(recovery.report.discarded, 1);
    assert_eq!(
        recovery.report.truncated,
        [ShardTruncation {
            shard: 0,
            applied: 1,
            discarded: 1
        }]
    );
}

#[test]
fn an_abandonment_past_the_last_mark_still_charges_every_shard() {
    // The rest of the segment is unread, and a mark in it cannot be seen.
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"0"));
    write_mark(&mut seg);
    let claimed = 1024u32;
    let mut junk = Vec::new();
    for _ in 0..=crate::log::reader::RESYNC_CAP / u64::from(claimed) + 1 {
        junk.push(crate::log::MAGIC);
        junk.extend_from_slice(&claimed.to_le_bytes());
        junk.extend_from_slice(&[0u8; 4]);
    }
    junk.extend(vec![0x5A; 2048]);
    seg.write_all(&junk).unwrap();
    write(&mut seg, 1, 0, &put(b"b", b"0"));
    write_mark(&mut seg);
    counter(&disk, 1);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(recovery.report.abandoned_segments, 1);
    assert_eq!(lossy(&recovery), [true, true]);
}

#[test]
fn a_hole_before_an_intact_mark_is_a_loss_whatever_lies_after_it() {
    let disk = MemDisk::default();
    let mut seg = wal(&disk, 1);
    write(&mut seg, 0, 0, &put(b"a", b"0"));
    let hole = end(&disk, 1, 0);
    write(&mut seg, 1, 0, &put(b"b", b"0"));
    write_mark(&mut seg);
    write(&mut seg, 0, 1, &put(b"a", b"1"));
    write_mark(&mut seg);
    write(&mut seg, 0, 2, &put(b"a", b"2"));
    corrupt_record_at(&disk, &path(1, 0), hole);
    counter(&disk, 1);
    let recovery = recover(spec(&disk, 2)).unwrap();
    assert_eq!(lossy(&recovery), [false, true]);
    assert_eq!(recovery.report.torn_bytes, 0);
}
