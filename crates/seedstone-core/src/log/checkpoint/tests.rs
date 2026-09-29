use super::*;
use crate::dict::{Dict, DictSeed, Entry};
use crate::log::disk::mem::MemDisk;
use crate::log::effect::Effect;
use crate::log::file::{FileLog, open_segments, segment_name};
use crate::log::reader::{Item, Reader, ReaderMode};
use crate::log::snapshot::{FOOTER_SHARD, Footer, SnapshotHeader, snapshot_name};
use crate::log::{Record, ReplicationLog};
use crate::shard::executor::ShardState;
use crate::shard::{CompactionReport, NoTrace, Now, TraceSink};
use bytes::Bytes;
use std::path::Path;
use std::sync::atomic::AtomicU16;
use std::sync::{Arc, Mutex};
use tokio::time::Instant;

/// One executor's worth of shards over a fresh wal, and its checkpoint.
struct Bench {
    disk: MemDisk,
    segment: SharedSegment<crate::log::disk::mem::MemFile>,
    states: Vec<ShardState<FileLog<crate::log::disk::mem::MemFile>>>,
    checkpoint: SegmentCheckpoint<MemDisk>,
    round: Arc<AtomicU16>,
}

const WAL: &str = "/data/wal";

fn bench(shards: u16, config: CheckpointConfig) -> Bench {
    let disk = MemDisk::default();
    disk.create_dir_all(Path::new(WAL)).unwrap();
    let segments = open_segments(&disk, Path::new(WAL), 1, 1).unwrap();
    let segment = Arc::clone(&segments[0]);
    let states = (0..shards)
        .map(|shard| {
            ShardState::new(
                Dict::with_seed(DictSeed {
                    k0: u64::from(shard),
                    k1: 7,
                }),
                FileLog::new(shard, Arc::clone(&segment)),
            )
        })
        .collect();
    let round = Arc::new(AtomicU16::new(0));
    let checkpoint = SegmentCheckpoint::new(CheckpointSpec {
        disk: disk.clone(),
        wal: Path::new(WAL).to_path_buf(),
        generation: 1,
        executor: 0,
        executors: 1,
        segment: Arc::clone(&segment),
        round: Arc::clone(&round),
        config,
    });
    Bench {
        disk,
        segment,
        states,
        checkpoint,
        round,
    }
}

fn now() -> Now {
    Now {
        instant: Instant::now(),
        unix_millis: 1_000_000,
    }
}

/// A `Put` applied the way the executor applies one: logged, then stored.
fn put(state: &mut ShardState<FileLog<crate::log::disk::mem::MemFile>>, key: &[u8], value: &[u8]) {
    let mut payload = Vec::new();
    Effect::Put {
        key,
        value,
        deadline: None,
    }
    .encode(&mut payload);
    state
        .log
        .append(Record {
            shard: state.log.shard(),
            seq: state.seq,
            payload: &payload,
        })
        .unwrap();
    state.seq += 1;
    state.dict.insert(
        Bytes::copy_from_slice(key),
        Entry {
            value: Bytes::copy_from_slice(value),
            expires_at: None,
            touched: 0,
        },
    );
}

fn flush_and_sync(states: &mut [ShardState<FileLog<crate::log::disk::mem::MemFile>>]) {
    for state in states.iter_mut() {
        state.log.flush().unwrap();
    }
    for state in states.iter_mut() {
        state.log.sync().unwrap();
    }
}

/// Every record of `name`, read as recovery would.
fn records(
    disk: &MemDisk,
    name: &str,
    header_len: usize,
) -> (Vec<Item>, crate::log::reader::Damage) {
    let bytes = disk.contents(&Path::new(WAL).join(name));
    let body = bytes[header_len..].to_vec();
    let mut reader = Reader::new(
        std::io::Cursor::new(body.clone()),
        body.len() as u64,
        ReaderMode::Resynchronising,
    );
    let mut items = Vec::new();
    while let Some(item) = reader.next_record().unwrap() {
        items.push(item);
    }
    (items, reader.damage())
}

/// A sink that keeps every report and fault it was given.
#[derive(Clone, Default)]
struct Recorder {
    snapshots: Arc<Mutex<Vec<SnapshotReport>>>,
    compactions: Arc<Mutex<Vec<CompactionReport>>>,
    faults: Arc<Mutex<Vec<LogFault>>>,
}

impl TraceSink for Recorder {
    fn record(&self, _: u16, _: u64, _: &crate::shard::Command, _: &crate::shard::Reply) {}
    fn fault(&self, _shard: u16, fault: LogFault, _error: &std::io::Error) {
        self.faults.lock().unwrap().push(fault);
    }
    fn snapshot(&self, report: &SnapshotReport) {
        self.snapshots.lock().unwrap().push(*report);
    }
    fn compaction(&self, report: &CompactionReport) {
        self.compactions.lock().unwrap().push(*report);
    }
}

const SMALL: CheckpointConfig = CheckpointConfig {
    floor: 64,
    ratio: 1,
    bytes_per_tick: 1024,
};

#[test]
fn nothing_happens_below_the_floor() {
    let mut b = bench(2, SMALL);
    put(&mut b.states[0], b"a", b"1");
    flush_and_sync(&mut b.states);
    b.checkpoint.tick(0, &mut b.states, now(), &NoTrace);
    let names = b.disk.list(Path::new(WAL)).unwrap();
    assert_eq!(
        names.len(),
        1,
        "GENERATION-less test wal: the segment and nothing else: {names:?}"
    );
    assert!(names.iter().all(|n| {
        std::path::Path::new(n)
            .extension()
            .is_none_or(|ext| ext != "snap")
    }));
}

#[test]
fn crossing_the_floor_rotates_the_segment_takes_the_bases_and_opens_a_snapshot() {
    let mut b = bench(2, SMALL);
    for i in 0..8u8 {
        put(
            &mut b.states[usize::from(i % 2)],
            &[b'k', i],
            b"value-long-enough-to-cross",
        );
    }
    flush_and_sync(&mut b.states);
    assert!(live_log_bytes(&b.segment) >= SMALL.floor);
    b.checkpoint.tick(0, &mut b.states, now(), &NoTrace);
    let mut names = b.disk.list(Path::new(WAL)).unwrap();
    names.sort();
    assert!(names.contains(&segment_name(1, 0, 1)), "rotated: {names:?}");
    assert!(
        names.contains(&snapshot_name(1, 0, 0)),
        "a snapshot is open: {names:?}"
    );
    assert_eq!(
        live_log_bytes(&b.segment),
        0,
        "the counter restarted with the rotation"
    );
    let header = SnapshotHeader::decode(
        &b.disk
            .contents(&Path::new(WAL).join(snapshot_name(1, 0, 0))),
    )
    .unwrap();
    assert_eq!(
        header.bases,
        vec![(0, 4), (1, 4)],
        "each shard's sequence at the rotation"
    );
}

#[test]
fn the_image_is_complete_after_enough_ticks_and_the_footer_counts_it() {
    let mut b = bench(2, SMALL);
    for i in 0..20u8 {
        put(&mut b.states[usize::from(i % 2)], &[b'k', i], b"v");
    }
    flush_and_sync(&mut b.states);
    let recorder = Recorder::default();
    // The first tick opens; a 1 KiB budget covers twenty small entries in
    // one more tick, and the footer lands on the tick after the last scan.
    let mut ticks = 0u64;
    while recorder.snapshots.lock().unwrap().is_empty() {
        b.checkpoint.tick(0, &mut b.states, now(), &recorder);
        ticks += 1;
        assert!(ticks < 10, "a twenty-key image should not take ten ticks");
    }
    let (items, damage) = records(&b.disk, &snapshot_name(1, 0, 0), {
        let bytes = b
            .disk
            .contents(&Path::new(WAL).join(snapshot_name(1, 0, 0)));
        SnapshotHeader::header_len(&bytes).unwrap()
    });
    assert_eq!(damage, crate::log::reader::Damage::default());
    let footer = items.last().unwrap();
    assert_eq!(footer.shard, FOOTER_SHARD);
    assert_eq!(
        Footer::decode(&footer.payload).unwrap().counts,
        vec![(0, 10), (1, 10)]
    );
    let mut keys: Vec<Vec<u8>> = items[..items.len() - 1]
        .iter()
        .map(|item| match Effect::decode(&item.payload).unwrap() {
            Effect::Put { key, .. } => key.to_vec(),
            other => panic!("an entry is a Put: {other:?}"),
        })
        .collect();
    keys.sort();
    keys.dedup();
    assert_eq!(
        keys.len(),
        20,
        "every key once at least, duplicates allowed"
    );
    assert!(
        items[..items.len() - 1].iter().all(|item| item.seq == 10),
        "seq is the base"
    );
    let report = recorder.snapshots.lock().unwrap()[0];
    assert_eq!((report.executor, report.cycle, report.entries), (0, 0, 20));
    assert_eq!(report.ticks, ticks);
    assert_eq!(b.checkpoint.cycles_completed(), 1);
}

#[test]
fn the_budget_bounds_a_tick_and_a_value_larger_than_it_still_goes_out() {
    let mut b = bench(
        1,
        CheckpointConfig {
            floor: 64,
            ratio: 1,
            bytes_per_tick: 100,
        },
    );
    let big = vec![b'x'; 4096];
    put(&mut b.states[0], b"big", &big);
    for i in 0..30u8 {
        put(&mut b.states[0], &[b's', i], b"small");
    }
    flush_and_sync(&mut b.states);
    let recorder = Recorder::default();
    b.checkpoint.tick(0, &mut b.states, now(), &recorder); // opens
    let path = Path::new(WAL).join(snapshot_name(1, 0, 0));
    let mut sizes = vec![b.disk.contents(&path).len()];
    let mut ticks = 0;
    while recorder.snapshots.lock().unwrap().is_empty() {
        b.checkpoint.tick(0, &mut b.states, now(), &recorder);
        sizes.push(b.disk.contents(&path).len());
        ticks += 1;
        assert!(ticks < 200, "the cycle must finish");
    }
    // Each tick grew the file by at most one budget plus one scan step —
    // a step is a bucket, and a bucket of small entries is a few of them.
    // The big value's tick is the one that overshoots, and only that one.
    let growth: Vec<usize> = sizes.windows(2).map(|w| w[1] - w[0]).collect();
    let overshoots = growth.iter().filter(|g| **g > 2 * 100 + 64).count();
    assert!(
        overshoots <= 1,
        "one scan step may overshoot the budget: {growth:?}"
    );
    assert!(
        growth.iter().any(|g| *g > 4096),
        "and the big value did go out"
    );
    assert_eq!(recorder.snapshots.lock().unwrap()[0].entries, 31);
}

#[test]
fn a_flush_during_the_cycle_empties_the_shard_and_the_cycle_still_finishes() {
    let mut b = bench(
        1,
        CheckpointConfig {
            floor: 64,
            ratio: 1,
            bytes_per_tick: 40,
        },
    );
    for i in 0..30u8 {
        put(&mut b.states[0], &[b'k', i], b"v");
    }
    flush_and_sync(&mut b.states);
    let recorder = Recorder::default();
    b.checkpoint.tick(0, &mut b.states, now(), &recorder); // opens
    b.checkpoint.tick(0, &mut b.states, now(), &recorder); // a few entries
    // FLUSHDB lands mid-cycle: logged in the tail (seq ≥ base) and applied.
    let mut payload = Vec::new();
    Effect::Flush.encode(&mut payload);
    let seq = b.states[0].seq;
    b.states[0]
        .log
        .append(Record {
            shard: 0,
            seq,
            payload: &payload,
        })
        .unwrap();
    b.states[0].seq += 1;
    b.states[0].dict.clear();
    let mut ticks = 0;
    while recorder.snapshots.lock().unwrap().is_empty() {
        b.checkpoint.tick(0, &mut b.states, now(), &recorder);
        ticks += 1;
        assert!(ticks < 10, "an empty dict's scan returns 0 at once");
    }
    let report = recorder.snapshots.lock().unwrap()[0];
    assert!(
        report.entries < 30,
        "the image holds what was scanned before the flush: {report:?}"
    );
}

#[test]
fn a_deadline_goes_out_as_unix_milliseconds() {
    let mut b = bench(1, SMALL);
    let at = now();
    put(&mut b.states[0], b"k", b"v");
    b.states[0]
        .dict
        .set_deadline(b"k", Some(at.instant + std::time::Duration::from_secs(5)));
    for i in 0..8u8 {
        put(&mut b.states[0], &[b'p', i], b"padding-to-cross-the-floor");
    }
    flush_and_sync(&mut b.states);
    let recorder = Recorder::default();
    while recorder.snapshots.lock().unwrap().is_empty() {
        b.checkpoint.tick(0, &mut b.states, at, &recorder);
    }
    let bytes = b
        .disk
        .contents(&Path::new(WAL).join(snapshot_name(1, 0, 0)));
    let (items, _) = records(
        &b.disk,
        &snapshot_name(1, 0, 0),
        SnapshotHeader::header_len(&bytes).unwrap(),
    );
    let deadline = items
        .iter()
        .find_map(|item| match Effect::decode(&item.payload) {
            Some(Effect::Put {
                key: b"k",
                deadline,
                ..
            }) => Some(deadline),
            _ => None,
        })
        .expect("k is in the image");
    assert_eq!(deadline, Some(1_000_000 + 5_000));
}
