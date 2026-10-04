use super::*;
use crate::dict::{Dict, DictSeed, Entry};
use crate::log::disk::mem::MemDisk;
use crate::log::effect::Effect;
use crate::log::file::FileLog;
use crate::log::reader::{Item, Reader, ReaderMode};
use crate::log::snapshot::{FOOTER_SHARD, Footer, SnapshotHeader, snapshot_name};
use crate::log::{Record, ReplicationLog};
use crate::shard::executor::ShardState;
use crate::shard::{NoTrace, Now, TraceSink};
use bytes::Bytes;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::time::Instant;

/// One executor's worth of shards over a fresh wal, and its checkpoint.
/// No writer: the checkpoint images memory, not the file.
struct Bench {
    disk: MemDisk,
    states: Vec<ShardState<FileLog>>,
    checkpoint: SegmentCheckpoint<MemDisk>,
}

const WAL: &str = "/data/wal";

fn bench(shards: u16, config: CheckpointConfig) -> Bench {
    let disk = MemDisk::default();
    disk.create_dir_all(Path::new(WAL)).unwrap();
    let states = (0..shards)
        .map(|shard| {
            ShardState::new(
                Dict::with_seed(DictSeed {
                    k0: u64::from(shard),
                    k1: 7,
                }),
                FileLog::new(shard),
            )
        })
        .collect();
    let checkpoint = SegmentCheckpoint::new(CheckpointSpec {
        disk: disk.clone(),
        wal: Path::new(WAL).to_path_buf(),
        generation: 1,
        executor: 0,
        config,
    });
    Bench {
        disk,
        states,
        checkpoint,
    }
}

/// One tick at `bytes` handed over, the last batch sent `batch`.
fn tick(b: &mut Bench, bytes: u64, batch: Option<u64>) -> Option<Completed> {
    b.checkpoint.tick(
        0,
        &mut b.states,
        now(),
        &NoTrace,
        LogPosition { bytes, batch },
    )
}

/// A position at `bytes`, the last batch sent the first.
const fn at(bytes: u64) -> LogPosition {
    LogPosition {
        bytes,
        batch: Some(0),
    }
}

fn now() -> Now {
    Now {
        instant: Instant::now(),
        unix_millis: 1_000_000,
    }
}

/// A `Put` applied the way the executor applies one: logged, then stored.
fn put(state: &mut ShardState<FileLog>, key: &[u8], value: &[u8]) {
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

/// Hands every shard's buffer over, as the executor does, into a scratch
/// buffer that is dropped; returns how many bytes that was.
fn flush(states: &mut [ShardState<FileLog>]) -> u64 {
    let mut out = Vec::new();
    for state in states.iter_mut() {
        state.log.flush_into(&mut out);
    }
    out.len() as u64
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
}

/// The floor alone opens a cycle: a test that runs several cycles writes a
/// few records each time, never as much as the last image, so under a ratio
/// of one its second cycle would never open.
const FLOOR_ONLY: CheckpointConfig = CheckpointConfig {
    floor: 64,
    ratio: 0,
    bytes_per_tick: 1024,
};

const SMALL: CheckpointConfig = CheckpointConfig {
    floor: 64,
    ratio: 1,
    bytes_per_tick: 1024,
};

#[test]
fn nothing_happens_below_the_floor() {
    let mut b = bench(2, SMALL);
    put(&mut b.states[0], b"a", b"1");
    let bytes = flush(&mut b.states);
    assert!(tick(&mut b, bytes, Some(0)).is_none());
    let names = b.disk.list(Path::new(WAL)).unwrap();
    assert!(
        names.iter().all(|n| {
            std::path::Path::new(n)
                .extension()
                .is_none_or(|ext| ext != "snap")
        }),
        "no snapshot: {names:?}"
    );
    assert!(!b.checkpoint.is_open());
}

#[test]
fn crossing_the_floor_takes_the_bases_and_opens_a_snapshot() {
    let mut b = bench(2, SMALL);
    for i in 0..8u8 {
        put(
            &mut b.states[usize::from(i % 2)],
            &[b'k', i],
            b"value-long-enough-to-cross",
        );
    }
    let bytes = flush(&mut b.states);
    assert!(bytes >= SMALL.floor);
    tick(&mut b, bytes, Some(0));
    let names = b.disk.list(Path::new(WAL)).unwrap();
    assert!(
        names.contains(&snapshot_name(1, 0, 0)),
        "a snapshot is open: {names:?}"
    );
    let header = SnapshotHeader::decode(
        &b.disk
            .contents(&Path::new(WAL).join(snapshot_name(1, 0, 0))),
    )
    .unwrap();
    assert_eq!(
        header.bases,
        vec![(0, 4), (1, 4)],
        "each shard's sequence at the open"
    );
}

#[test]
fn the_image_is_complete_after_enough_ticks_and_the_footer_counts_it() {
    let mut b = bench(2, SMALL);
    for i in 0..20u8 {
        put(&mut b.states[usize::from(i % 2)], &[b'k', i], b"v");
    }
    let bytes = flush(&mut b.states);
    let recorder = Recorder::default();
    // The first tick opens; a 1 KiB budget covers twenty small entries in
    // one more tick, and the footer lands on the tick after the last scan.
    let mut ticks = 0u64;
    let mut done = None;
    while recorder.snapshots.lock().unwrap().is_empty() {
        done = b.checkpoint.tick(
            0,
            &mut b.states,
            now(),
            &recorder,
            LogPosition {
                bytes,
                batch: Some(7),
            },
        );
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
    assert_eq!(
        done,
        Some(Completed {
            cycle: 0,
            through_batch: Some(7),
            bytes: report.bytes,
        }),
        "the completing tick names the last batch sent before the open"
    );
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
    let bytes = flush(&mut b.states);
    let recorder = Recorder::default();
    b.checkpoint
        .tick(0, &mut b.states, now(), &recorder, at(bytes)); // opens
    let path = Path::new(WAL).join(snapshot_name(1, 0, 0));
    let mut sizes = vec![b.disk.contents(&path).len()];
    let mut ticks = 0;
    while recorder.snapshots.lock().unwrap().is_empty() {
        b.checkpoint
            .tick(0, &mut b.states, now(), &recorder, at(bytes));
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
    let bytes = flush(&mut b.states);
    let recorder = Recorder::default();
    b.checkpoint
        .tick(0, &mut b.states, now(), &recorder, at(bytes)); // opens
    b.checkpoint
        .tick(0, &mut b.states, now(), &recorder, at(bytes)); // a few entries
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
        b.checkpoint
            .tick(0, &mut b.states, now(), &recorder, at(bytes));
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
    let bytes = flush(&mut b.states);
    let recorder = Recorder::default();
    while recorder.snapshots.lock().unwrap().is_empty() {
        b.checkpoint.tick(
            0,
            &mut b.states,
            at,
            &recorder,
            LogPosition {
                bytes,
                batch: Some(0),
            },
        );
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

#[test]
fn a_failed_write_abandons_the_file_and_the_scan_restarts_into_a_new_one() {
    let mut b = bench(
        1,
        CheckpointConfig {
            floor: 64,
            ratio: 1,
            bytes_per_tick: 64,
        },
    );
    for i in 0..12u8 {
        put(&mut b.states[0], &[b'k', i], b"v");
    }
    let bytes = flush(&mut b.states);
    let recorder = Recorder::default();
    b.checkpoint
        .tick(0, &mut b.states, now(), &recorder, at(bytes)); // opens, writes the header
    let first = Path::new(WAL).join(snapshot_name(1, 0, 0));
    assert!(b.disk.len(&first).is_ok());
    b.disk.fail_writes(true);
    b.checkpoint
        .tick(0, &mut b.states, now(), &recorder, at(bytes));
    b.disk.fail_writes(false);
    assert_eq!(
        recorder.faults.lock().unwrap().as_slice(),
        [LogFault::Snapshot]
    );
    // What a failed write left in the file is unknown, so nothing is
    // written after it: the file goes, and the scan starts over.
    assert!(b.disk.len(&first).is_err(), "the abandoned file is removed");
    let mut ticks = 0;
    while recorder.snapshots.lock().unwrap().is_empty() {
        b.checkpoint
            .tick(0, &mut b.states, now(), &recorder, at(bytes));
        ticks += 1;
        assert!(ticks < 50);
    }
    let report = recorder.snapshots.lock().unwrap()[0];
    assert_eq!(report.cycle, 1, "the second file, not the abandoned first");
    assert_eq!(report.entries, 12, "nothing was lost or doubled");
}

#[test]
fn a_failed_footer_sync_abandons_the_file_and_restarts_with_the_same_bases() {
    // A budget of one entry or two, so the opening tick creates the file and
    // the footer lands on a later one — whose sync alone is made to fail.
    let mut b = bench(
        1,
        CheckpointConfig {
            floor: 64,
            ratio: 1,
            bytes_per_tick: 64,
        },
    );
    for i in 0..8u8 {
        put(&mut b.states[0], &[b'k', i], b"value-long-enough-to-cross");
    }
    let bytes = flush(&mut b.states);
    let recorder = Recorder::default();
    b.checkpoint
        .tick(0, &mut b.states, now(), &recorder, at(bytes)); // opens
    assert!(recorder.snapshots.lock().unwrap().is_empty());
    b.disk.fail_syncs(true);
    let mut ticks = 0;
    while recorder.faults.lock().unwrap().is_empty() {
        b.checkpoint
            .tick(0, &mut b.states, now(), &recorder, at(bytes));
        ticks += 1;
        assert!(ticks < 50, "the footer's sync is reached and fails");
    }
    b.disk.fail_syncs(false);
    assert_eq!(
        recorder.faults.lock().unwrap().as_slice(),
        [LogFault::Snapshot]
    );
    assert!(recorder.snapshots.lock().unwrap().is_empty());
    while recorder.snapshots.lock().unwrap().is_empty() {
        b.checkpoint
            .tick(0, &mut b.states, now(), &recorder, at(bytes));
        ticks += 1;
        assert!(ticks < 100, "the restarted cycle finishes");
    }
    let report = recorder.snapshots.lock().unwrap()[0];
    assert_eq!(report.cycle, 1, "the second file, not the abandoned first");
    let header = SnapshotHeader::decode(
        &b.disk
            .contents(&Path::new(WAL).join(snapshot_name(1, 0, 1))),
    )
    .unwrap();
    assert_eq!(header.bases, vec![(0, 8)], "the bases taken at the open");
    assert_eq!(report.entries, 8);
}

/// Keys in `state`'s dict and not in its log: what the image scans, with
/// nothing handed over, so a cycle spans several ticks of a small budget
/// without the live log moving.
fn pad(state: &mut ShardState<FileLog>, keys: u8) {
    for i in 0..keys {
        state.dict.insert(
            Bytes::copy_from_slice(&[b'p', i]),
            Entry {
                value: Bytes::from_static(b"v"),
                expires_at: None,
                touched: 0,
            },
        );
    }
}

/// A budget of one byte: one scan step a tick.
const STEPWISE: CheckpointConfig = CheckpointConfig {
    bytes_per_tick: 1,
    ..SMALL
};

#[test]
fn the_plant_reports_covered_at_the_open() {
    let mut b = bench(1, STEPWISE);
    pad(&mut b.states[0], 32);
    b.checkpoint.reports_covered_at_open(true);
    for i in 0..8u8 {
        put(&mut b.states[0], &[b'k', i], b"value-long-enough-to-cross");
    }
    let bytes = flush(&mut b.states);
    let done = tick(&mut b, bytes, Some(3)).expect("the plant reports at the open");
    assert_eq!((done.cycle, done.through_batch), (0, Some(3)));
    assert!(b.checkpoint.is_open(), "the cycle still runs");
}

#[test]
fn a_nudge_opens_a_cycle_below_the_floor_and_a_force_abandons_an_open_one() {
    let mut b = bench(2, STEPWISE);
    put(&mut b.states[0], b"a", b"1");
    pad(&mut b.states[1], 32);
    let bytes = flush(&mut b.states);
    assert!(bytes < SMALL.floor);
    assert!(tick(&mut b, bytes, Some(0)).is_none());
    assert!(!b.checkpoint.is_open());
    b.checkpoint.nudge();
    tick(&mut b, bytes, Some(0));
    assert!(b.checkpoint.is_open(), "nudged: open whatever the bytes");
    let open_file = snapshot_name(1, 0, 0);
    assert!(b.disk.list(Path::new(WAL)).unwrap().contains(&open_file));
    b.checkpoint.force();
    assert!(
        !b.checkpoint.is_open(),
        "forced: the open cycle is abandoned"
    );
    assert!(
        !b.disk.list(Path::new(WAL)).unwrap().contains(&open_file),
        "and its file removed"
    );
    tick(&mut b, bytes, Some(0));
    assert!(
        b.checkpoint.is_open(),
        "and the next tick opens from memory"
    );
}

#[test]
fn written_during_counts_what_the_executor_sent_from_the_crossing_to_the_footer() {
    let mut b = bench(
        1,
        CheckpointConfig {
            bytes_per_tick: 64,
            ..SMALL
        },
    );
    for i in 0..8u8 {
        put(&mut b.states[0], &[b'k', i], b"value-long-enough-to-cross");
    }
    let at_open = flush(&mut b.states);
    let recorder = Recorder::default();
    b.checkpoint.tick(
        0,
        &mut b.states,
        now(),
        &recorder,
        LogPosition {
            bytes: at_open,
            batch: Some(0),
        },
    );
    let mut sent = at_open;
    while recorder.snapshots.lock().unwrap().is_empty() {
        put(&mut b.states[0], b"x", b"y");
        sent += flush(&mut b.states);
        b.checkpoint.tick(
            0,
            &mut b.states,
            now(),
            &recorder,
            LogPosition {
                bytes: sent,
                batch: Some(1),
            },
        );
    }
    let report = recorder.snapshots.lock().unwrap()[0];
    assert_eq!(
        report.written_during,
        (at_open - SMALL.floor) + (sent - at_open)
    );
}

/// The log crosses the floor between two ticks, and the cycle opens only on
/// the next one: what was written past the floor before it opened is part
/// of what the cycle reports as written, or the disk bound misses it.
#[test]
fn what_was_written_past_the_floor_before_the_cycle_opened_is_reported_as_written() {
    let mut b = bench(2, SMALL);
    for i in 0..8u8 {
        put(
            &mut b.states[usize::from(i % 2)],
            &[b'k', i],
            b"value-long-enough-to-cross",
        );
    }
    let bytes = flush(&mut b.states);
    let past_the_floor = bytes - SMALL.floor;
    assert!(past_the_floor > 0);
    let trace = Recorder::default();
    for _ in 0..16 {
        b.checkpoint
            .tick(0, &mut b.states, now(), &trace, at(bytes));
        if !trace.snapshots.lock().unwrap().is_empty() {
            break;
        }
    }
    let snapshots = trace.snapshots.lock().unwrap().clone();
    assert_eq!(snapshots.len(), 1, "the cycle finished");
    assert_eq!(
        snapshots[0].written_during, past_the_floor,
        "nothing was sent after the open, so what is reported is the overshoot"
    );
}

/// Eight keys over two shards, handed over past the floor: the bytes.
fn eight_keys_past_the_floor(b: &mut Bench) -> u64 {
    for i in 0..8u8 {
        put(
            &mut b.states[usize::from(i % 2)],
            &[b'k', i],
            b"value-long-enough-to-cross",
        );
    }
    flush(&mut b.states)
}

/// Ticks until a snapshot is reported, sixteen ticks at most.
fn tick_until_snapshot(b: &mut Bench, trace: &Recorder, bytes: u64) {
    for _ in 0..16 {
        b.checkpoint.tick(0, &mut b.states, now(), trace, at(bytes));
        if !trace.snapshots.lock().unwrap().is_empty() {
            return;
        }
    }
}

/// What a start would make of the bench's directory.
fn recovered(b: &Bench) -> crate::log::recovery::Recovery {
    crate::log::recovery::recover(crate::log::recovery::RecoverSpec {
        disk: &b.disk,
        wal: Path::new(WAL),
        shards: u16::try_from(b.states.len()).unwrap(),
        reader: ReaderMode::Resynchronising,
        trust_unfinished: false,
        seed: DictSeed { k0: 0, k1: 7 },
        now: now(),
    })
    .unwrap()
}

/// A snapshot file whose header could not be synced is abandoned, never
/// reopened: a second header appended under the same name would leave the
/// finished image unreadable once the writer had removed the log it covers.
#[test]
fn a_failed_header_sync_abandons_the_file_and_the_image_still_reads_whole() {
    let mut b = bench(2, SMALL);
    let bytes = eight_keys_past_the_floor(&mut b);
    // The snapshot header's sync is the first the cycle makes.
    b.disk.fail_next_sync();
    let trace = Recorder::default();
    tick_until_snapshot(&mut b, &trace, bytes);
    assert_eq!(*trace.faults.lock().unwrap(), [LogFault::Snapshot]);
    assert_eq!(
        trace.snapshots.lock().unwrap().len(),
        1,
        "the cycle finished"
    );
    let recovery = recovered(&b);
    assert_eq!(recovery.report.snapshots_used, 1, "{:?}", recovery.report);
    let keys: usize = recovery.shards.iter().map(|s| s.dict.len()).sum();
    assert_eq!(keys, 8);
    assert!(recovery.shards.iter().all(|s| !s.lossy));
}

/// A forced checkpoint opens on the next tick below the threshold, and
/// `tick` says when its snapshot is durable.
#[test]
fn force_opens_a_cycle_below_the_threshold_and_tick_reports_the_completion() {
    let mut b = bench(1, FLOOR_ONLY);
    put(&mut b.states[0], b"a", b"1");
    let bytes = flush(&mut b.states);
    assert!(bytes < FLOOR_ONLY.floor);
    assert!(
        tick(&mut b, bytes, Some(0)).is_none(),
        "below the threshold: nothing"
    );
    assert!(!b.checkpoint.is_open());
    b.checkpoint.force();
    let mut completed = tick(&mut b, bytes, Some(0));
    for _ in 0..8 {
        if completed.is_some() {
            break;
        }
        completed = tick(&mut b, bytes, Some(0));
    }
    assert!(completed.is_some(), "the forced cycle completes");
    assert_eq!(b.checkpoint.cycles_completed(), 1);
    assert!(!b.checkpoint.is_open());
    assert!(
        tick(&mut b, bytes, Some(0)).is_none(),
        "forced once: below the threshold again, nothing opens"
    );
}

/// Forcing while a cycle is open abandons it: its bases predate the
/// failure that forced it, so the image plus a tail with a hole would
/// lose records. The new cycle takes the bases of now.
#[test]
fn force_while_open_abandons_the_cycle_and_takes_fresh_bases() {
    let mut b = bench(
        2,
        CheckpointConfig {
            bytes_per_tick: 1,
            ..FLOOR_ONLY
        },
    );
    let bytes = eight_keys_past_the_floor(&mut b);
    assert!(tick(&mut b, bytes, Some(0)).is_none());
    assert!(b.checkpoint.is_open());
    let first_file = snapshot_name(1, 0, 0);
    assert!(b.disk.list(Path::new(WAL)).unwrap().contains(&first_file));
    // The records a failed sync would lose.
    put(&mut b.states[0], b"late", b"1");
    put(&mut b.states[1], b"late2", b"1");
    b.checkpoint.force();
    assert!(!b.checkpoint.is_open(), "abandoned");
    assert!(
        !b.disk.list(Path::new(WAL)).unwrap().contains(&first_file),
        "its file removed"
    );
    tick(&mut b, bytes, Some(0));
    assert_eq!(
        b.checkpoint.open_bases(),
        vec![b.states[0].seq, b.states[1].seq],
        "the bases of now"
    );
}
