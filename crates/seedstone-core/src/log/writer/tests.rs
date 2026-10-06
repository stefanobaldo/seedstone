//! The writer driven by hand over `MemDisk`: the tests hold the
//! executors' ends of the links.

use super::*;
use crate::log::checkpoint::CheckpointConfig;
use crate::log::disk::mem::MemDisk;
use crate::log::file::{decode_segment_header, segment_name};
use crate::log::snapshot::snapshot_name;
use crate::log::{Decoded, decode_record};
use crate::shard::{CompactionReport, LogFault, NoTrace, SyncPolicy, TraceSink};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const WAL: &str = "/data/wal";
const SMALL: CheckpointConfig = CheckpointConfig {
    floor: 64,
    ratio: 1,
    bytes_per_tick: 1024,
};

/// A writer over a fresh wal, and the executors' ends of its links.
struct Rig {
    disk: MemDisk,
    links: Vec<WriterLink>,
    task: tokio::task::JoinHandle<()>,
    stats: Arc<PersistenceStats>,
}

fn open(
    disk: &MemDisk,
    executors: u16,
    policy: SyncPolicy,
    segment_bytes: u64,
    plants: WriterPlants,
) -> Rig {
    open_with(disk, executors, policy, segment_bytes, plants, NoTrace)
}

fn open_with<T: TraceSink>(
    disk: &MemDisk,
    executors: u16,
    policy: SyncPolicy,
    segment_bytes: u64,
    plants: WriterPlants,
    trace: T,
) -> Rig {
    disk.create_dir_all(Path::new(WAL)).unwrap();
    let stats = PersistenceStats::new(executors);
    let Opened {
        writer,
        inbox,
        links,
    } = Writer::open(WriterSpec {
        disk: disk.clone(),
        wal: Path::new(WAL).to_path_buf(),
        generation: 1,
        executors,
        policy,
        segment_bytes,
        checkpoint: SMALL,
        trace,
        plants,
        stats: stats.clone(),
    })
    .unwrap();
    Rig {
        disk: disk.clone(),
        links,
        task: tokio::spawn(writer.run(inbox)),
        stats,
    }
}

fn record(shard: u16, seq: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    crate::log::encode_record(
        &crate::log::Record {
            shard,
            seq,
            payload,
        },
        &mut out,
    );
    out
}

impl Rig {
    fn submit(&self, executor: usize, batch: u64, bytes: Vec<u8>) {
        let link = &self.links[executor];
        link.to_writer
            .send(ToWriter::Submit {
                executor: link.executor,
                batch,
                bytes,
            })
            .unwrap();
    }
    async fn next(&mut self, executor: usize) -> Progress {
        tokio::time::timeout(Duration::from_secs(5), self.links[executor].progress.recv())
            .await
            .expect("the writer answers within the timeout")
            .expect("the writer is alive")
    }
    /// Reads progress until a `Durable` arrives, returning it.
    async fn durable(&mut self, executor: usize) -> (Option<u64>, u64, u64) {
        loop {
            if let Progress::Durable {
                through_batch,
                bytes,
                round,
            } = self.next(executor).await
            {
                return (through_batch, bytes, round);
            }
        }
    }
    fn segment(&self, rotation: u32) -> Vec<u8> {
        self.disk
            .contents(&Path::new(WAL).join(segment_name(1, rotation)))
    }
}

#[tokio::test(start_paused = true)]
async fn open_creates_the_segment_with_a_synced_header_and_one_link_per_executor() {
    let disk = MemDisk::default();
    let rig = open(
        &disk,
        3,
        SyncPolicy::INTERVAL,
        1 << 20,
        WriterPlants::default(),
    );
    assert_eq!(rig.links.len(), 3);
    assert_eq!(
        rig.links.iter().map(|l| l.executor).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert_eq!(decode_segment_header(&rig.segment(0)), Ok((1, 0)));
    assert_eq!(
        disk.synced_len(&Path::new(WAL).join(segment_name(1, 0))),
        rig.segment(0).len()
    );
}

#[tokio::test(start_paused = true)]
async fn under_always_a_submission_is_written_synced_and_answered_durable_by_its_batch() {
    let disk = MemDisk::default();
    let mut rig = open(
        &disk,
        2,
        SyncPolicy::ALWAYS,
        1 << 20,
        WriterPlants::default(),
    );
    rig.submit(0, 0, record(0, 0, b"a"));
    rig.submit(1, 0, record(5, 0, b"b"));
    let (through, bytes, round) = rig.durable(0).await;
    assert_eq!(through, Some(0));
    assert_eq!(bytes, record(0, 0, b"a").len() as u64);
    assert_eq!(round, 1);
    let (through, _, round_1) = rig.durable(1).await;
    assert_eq!(
        (through, round_1),
        (Some(0), 1),
        "one round covered both executors' batches"
    );
    let body = &rig.segment(0)[crate::log::file::SEGMENT_HEADER_LEN..];
    let Decoded::Record {
        shard,
        consumed: next,
        ..
    } = decode_record(body)
    else {
        panic!()
    };
    assert_eq!(shard, 0);
    let Decoded::Record { shard, .. } = decode_record(&body[next..]) else {
        panic!()
    };
    assert_eq!(shard, 5, "the shards interleave in arrival order");
    assert_eq!(
        disk.synced_len(&Path::new(WAL).join(segment_name(1, 0))),
        rig.segment(0).len()
    );
}

#[tokio::test(start_paused = true)]
async fn what_arrives_during_a_flight_is_the_next_rounds_and_one_round_is_in_flight_at_a_time() {
    // MemDisk's deferred sync completes when the test lets time pass;
    // hold the first round in flight and submit two more batches.
    let disk = MemDisk::default();
    disk.set_sync_latency(Duration::from_millis(10));
    let mut rig = open(
        &disk,
        1,
        SyncPolicy::ALWAYS,
        1 << 20,
        WriterPlants::default(),
    );
    rig.submit(0, 0, record(0, 0, b"a"));
    tokio::task::yield_now().await;
    rig.submit(0, 1, record(0, 1, b"b"));
    rig.submit(0, 2, record(0, 2, b"c"));
    let (through, _, round) = rig.durable(0).await;
    assert_eq!(
        (through, round),
        (Some(0), 1),
        "the first round covers what was written at its issue"
    );
    let (through, _, round) = rig.durable(0).await;
    assert_eq!(
        (through, round),
        (Some(2), 2),
        "the second covers both batches that arrived meanwhile"
    );
}

#[tokio::test(start_paused = true)]
async fn under_interval_a_round_is_issued_once_per_interval_and_never_under_never() {
    let disk = MemDisk::default();
    let mut rig = open(
        &disk,
        1,
        SyncPolicy::INTERVAL,
        1 << 20,
        WriterPlants::default(),
    );
    rig.submit(0, 0, record(0, 0, b"a"));
    let (through, _, round) = rig.durable(0).await;
    assert_eq!(
        (through, round),
        (Some(0), 1),
        "the first write after a start is synced at once"
    );
    rig.submit(0, 1, record(0, 1, b"b"));
    tokio::time::advance(crate::shard::HOUSEKEEPING_TICK / 2).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(1), rig.links[0].progress.recv())
            .await
            .is_err(),
        "inside the interval nothing is issued"
    );
    tokio::time::advance(crate::shard::HOUSEKEEPING_TICK).await;
    let (through, _, round) = rig.durable(0).await;
    assert_eq!((through, round), (Some(1), 2));

    let disk = MemDisk::default();
    let mut rig = open(
        &disk,
        1,
        SyncPolicy::NEVER,
        1 << 20,
        WriterPlants::default(),
    );
    rig.submit(0, 0, record(0, 0, b"a"));
    tokio::time::advance(crate::shard::HOUSEKEEPING_TICK * 10).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(1), rig.links[0].progress.recv())
            .await
            .is_err(),
        "never issues no sync"
    );
    assert_eq!(
        disk.sync_count(&Path::new(WAL).join(segment_name(1, 0))),
        1,
        "the header's sync and nothing after it"
    );
}

#[tokio::test(start_paused = true)]
async fn written_is_reported_once_a_grain_of_an_executors_bytes_is_in_the_file() {
    let disk = MemDisk::default();
    let mut rig = open(
        &disk,
        1,
        SyncPolicy::NEVER,
        1 << 30,
        WriterPlants::default(),
    );
    let chunk = vec![0u8; (WRITTEN_GRAIN / 2) as usize];
    rig.submit(0, 0, chunk.clone());
    tokio::task::yield_now().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(1), rig.links[0].progress.recv())
            .await
            .is_err(),
        "half a grain is not reported"
    );
    rig.submit(0, 1, chunk);
    let Progress::Written { bytes } = rig.next(0).await else {
        panic!("a grain is reported")
    };
    assert_eq!(bytes, WRITTEN_GRAIN);
}

#[tokio::test(start_paused = true)]
async fn the_start_path_appends_and_syncs_now_and_a_rebase_advances_the_cut_shard() {
    let disk = MemDisk::default();
    disk.create_dir_all(Path::new(WAL)).unwrap();
    let Opened {
        mut writer,
        inbox,
        links,
    } = Writer::open(WriterSpec {
        disk: disk.clone(),
        wal: Path::new(WAL).to_path_buf(),
        generation: 1,
        executors: 1,
        policy: SyncPolicy::INTERVAL,
        segment_bytes: 1 << 20,
        checkpoint: SMALL,
        trace: NoTrace,
        plants: WriterPlants::default(),
        stats: crate::shard::PersistenceStats::new(1),
    })
    .unwrap();
    let mut shards = vec![
        crate::log::recovery::RecoveredShard {
            dict: crate::dict::Dict::with_seed(crate::dict::DictSeed { k0: 1, k1: 2 }),
            seq: 4,
            lossy: false,
            cut: true,
            image_unix_millis: None,
        },
        crate::log::recovery::RecoveredShard {
            dict: crate::dict::Dict::with_seed(crate::dict::DictSeed { k0: 1, k1: 3 }),
            seq: 9,
            lossy: false,
            cut: false,
            image_unix_millis: None,
        },
    ];
    write_rebases(&mut writer, &mut shards).unwrap();
    assert_eq!(
        (shards[0].seq, shards[1].seq),
        (5, 9),
        "only the cut shard advanced"
    );
    let body = &disk.contents(&Path::new(WAL).join(segment_name(1, 0)))
        [crate::log::file::SEGMENT_HEADER_LEN..];
    let Decoded::Record {
        shard,
        seq,
        payload,
        ..
    } = decode_record(body)
    else {
        panic!()
    };
    assert_eq!((shard, seq), (0, 4));
    assert!(matches!(
        crate::log::effect::Effect::decode(payload),
        Some(crate::log::effect::Effect::Rebase)
    ));
    assert_eq!(
        disk.synced_len(&Path::new(WAL).join(segment_name(1, 0))),
        disk.contents(&Path::new(WAL).join(segment_name(1, 0)))
            .len()
    );
    drop(links);
    drop(inbox);
    drop(writer);
}

#[tokio::test(start_paused = true)]
async fn a_failed_sync_faults_every_executor_and_the_next_submission_rotates_to_a_clean_segment() {
    let disk = MemDisk::default();
    let mut rig = open(
        &disk,
        2,
        SyncPolicy::ALWAYS,
        1 << 20,
        WriterPlants::default(),
    );
    rig.submit(0, 0, record(0, 0, b"a"));
    rig.durable(0).await;
    disk.fail_next_sync();
    rig.submit(1, 0, record(5, 0, b"b"));
    assert!(
        matches!(rig.next(0).await, Progress::Fault),
        "the executor that wrote nothing this round hears it too"
    );
    assert!(matches!(rig.next(1).await, Progress::Fault));
    // The one that resumes writes into rotation 1; rotation 0 is never written again.
    rig.submit(0, 1, record(0, 1, b"c"));
    let (through, _, _) = rig.durable(0).await;
    assert_eq!(through, Some(1));
    assert_eq!(decode_segment_header(&rig.segment(1)), Ok((1, 1)));
    let body = &rig.segment(1)[crate::log::file::SEGMENT_HEADER_LEN..];
    let Decoded::Record { seq, .. } = decode_record(body) else {
        panic!()
    };
    assert_eq!(seq, 1);
}

#[tokio::test(start_paused = true)]
async fn a_failed_write_faults_drops_what_was_staged_and_counts_it_consumed() {
    let disk = MemDisk::default();
    let mut rig = open(
        &disk,
        1,
        SyncPolicy::INTERVAL,
        1 << 20,
        WriterPlants::default(),
    );
    disk.fail_writes(true);
    rig.submit(0, 0, record(0, 0, b"a"));
    assert!(matches!(rig.next(0).await, Progress::Fault));
    disk.fail_writes(false);
    rig.submit(0, 1, record(0, 1, b"b"));
    let (through, bytes, _) = rig.durable(0).await;
    assert_eq!(through, Some(1));
    assert_eq!(
        bytes,
        (record(0, 0, b"a").len() + record(0, 1, b"b").len()) as u64,
        "the dropped batch's bytes are counted as consumed: the executor let \
         them go at its `Fault`, and a count behind its own would hold its \
         budget spent for good"
    );
}

#[tokio::test(start_paused = true)]
async fn a_retry_that_fails_faults_only_the_executors_that_sent_bytes() {
    let disk = MemDisk::default();
    let mut rig = open(
        &disk,
        2,
        SyncPolicy::INTERVAL,
        1 << 20,
        WriterPlants::default(),
    );
    disk.fail_writes(true);
    rig.submit(0, 0, record(0, 0, b"a"));
    assert!(matches!(rig.next(0).await, Progress::Fault));
    assert!(
        matches!(rig.next(1).await, Progress::Fault),
        "a fresh failure reaches everyone"
    );
    disk.fail_creates(true); // the rotation cannot create the next file
    rig.submit(0, 1, record(0, 1, b"b"));
    assert!(matches!(rig.next(0).await, Progress::Fault));
    assert!(
        tokio::time::timeout(Duration::from_millis(1), rig.links[1].progress.recv())
            .await
            .is_err(),
        "the one still refusing is not told again"
    );
}

/// Bytes an executor sent before it heard of a failure reach the writer
/// after it, and each retries the rotation; a retry that fails on a segment
/// that had already failed is the same incident, not a new line. A failure
/// after a clean rotation is a new one.
#[tokio::test(start_paused = true)]
async fn a_retry_that_fails_on_a_failed_segment_traces_no_second_line() {
    let disk = MemDisk::default();
    let trace = Recorder::default();
    let mut rig = open_with(
        &disk,
        2,
        SyncPolicy::INTERVAL,
        1 << 20,
        WriterPlants::default(),
        trace.clone(),
    );
    disk.fail_writes(true);
    rig.submit(0, 0, record(0, 0, b"a"));
    assert!(matches!(rig.next(0).await, Progress::Fault));
    disk.fail_creates(true);
    rig.submit(1, 0, record(512, 0, b"b"));
    assert!(matches!(rig.next(1).await, Progress::Fault));
    assert!(matches!(rig.next(1).await, Progress::Fault));
    assert_eq!(*trace.faults.lock().unwrap(), vec![LogFault::Write]);
    disk.fail_creates(false);
    disk.fail_writes(false);
    rig.submit(0, 1, record(0, 1, b"c"));
    tokio::task::yield_now().await;
    disk.fail_writes(true);
    rig.submit(0, 2, record(0, 2, b"d"));
    while !matches!(rig.next(0).await, Progress::Fault) {}
    assert_eq!(
        *trace.faults.lock().unwrap(),
        vec![LogFault::Write, LogFault::Write]
    );
}

/// A rotation that keeps failing is still one incident while the executors
/// it reaches are refusing from it; once one has resumed — its snapshot
/// covered — a failure that refuses it again is a new line, or the node
/// would go back to refusing with nothing in its log to say why.
#[tokio::test(start_paused = true)]
async fn a_retry_that_fails_after_an_executor_resumed_is_a_new_line() {
    let disk = MemDisk::default();
    let trace = Recorder::default();
    let mut rig = open_with(
        &disk,
        2,
        SyncPolicy::INTERVAL,
        1 << 20,
        WriterPlants::default(),
        trace.clone(),
    );
    disk.fail_writes(true);
    disk.fail_creates(true);
    rig.submit(0, 0, record(0, 0, b"a"));
    assert!(matches!(rig.next(0).await, Progress::Fault));
    assert!(matches!(rig.next(1).await, Progress::Fault));
    rig.submit(0, 1, record(0, 1, b"b")); // sent while refusing
    assert!(matches!(rig.next(0).await, Progress::Fault));
    assert_eq!(*trace.faults.lock().unwrap(), vec![LogFault::Write]);
    covered(&rig, 1, 0, None, 10); // executor 1's snapshot ends its refusal
    tokio::task::yield_now().await;
    rig.submit(1, 0, record(512, 0, b"c"));
    assert!(matches!(rig.next(1).await, Progress::Fault));
    assert_eq!(
        *trace.faults.lock().unwrap(),
        vec![LogFault::Write, LogFault::Rotate],
        "the rotation still fails, and now refuses an executor that had resumed"
    );
}

#[tokio::test(start_paused = true)]
async fn the_segment_rotates_at_its_size_and_the_old_one_is_synced_first() {
    let disk = MemDisk::default();
    let rig = open(&disk, 1, SyncPolicy::NEVER, 64, WriterPlants::default());
    rig.submit(0, 0, vec![1u8; 70]);
    tokio::task::yield_now().await;
    rig.submit(0, 1, vec![2u8; 10]);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert_eq!(
        rig.segment(0).len(),
        crate::log::file::SEGMENT_HEADER_LEN + 70
    );
    assert_eq!(
        disk.synced_len(&Path::new(WAL).join(segment_name(1, 0))),
        rig.segment(0).len(),
        "under never the rotation is the one sync the old file gets"
    );
    assert_eq!(
        rig.segment(1).len(),
        crate::log::file::SEGMENT_HEADER_LEN + 10
    );
}

#[tokio::test(start_paused = true)]
async fn a_rotation_whose_sync_fails_faults_and_nothing_is_swapped() {
    let disk = MemDisk::default();
    let mut rig = open(&disk, 1, SyncPolicy::NEVER, 64, WriterPlants::default());
    rig.submit(0, 0, vec![1u8; 70]);
    tokio::task::yield_now().await;
    disk.fail_next_sync();
    rig.submit(0, 1, vec![2u8; 10]);
    assert!(matches!(rig.next(0).await, Progress::Fault));
    assert!(
        disk.list(Path::new(WAL))
            .unwrap()
            .iter()
            .all(|n| n != &segment_name(1, 1)),
        "no new file while the old one's bytes are unproven"
    );
}

#[tokio::test(start_paused = true)]
async fn a_stop_during_a_flight_is_answered_after_both_syncs() {
    let disk = MemDisk::default();
    disk.set_sync_latency(Duration::from_millis(10));
    let mut rig = open(
        &disk,
        1,
        SyncPolicy::ALWAYS,
        1 << 20,
        WriterPlants::default(),
    );
    rig.submit(0, 0, record(0, 0, b"a"));
    tokio::task::yield_now().await;
    rig.submit(0, 1, record(0, 1, b"b"));
    rig.links[0]
        .to_writer
        .send(ToWriter::Stop { executor: 0 })
        .unwrap();
    let (through, _, round) = rig.durable(0).await;
    assert_eq!((through, round), (Some(0), 1));
    let (through, _, round) = rig.durable(0).await;
    assert_eq!(
        (through, round),
        (Some(1), 2),
        "the last submission is synced before the stop is answered"
    );
    assert!(matches!(rig.next(0).await, Progress::Stopped));
    tokio::time::timeout(Duration::from_secs(1), &mut rig.task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_stop_whose_last_sync_fails_answers_fault_then_stopped() {
    let disk = MemDisk::default();
    let mut rig = open(
        &disk,
        1,
        SyncPolicy::NEVER,
        1 << 20,
        WriterPlants::default(),
    );
    rig.submit(0, 0, record(0, 0, b"a"));
    tokio::task::yield_now().await;
    disk.fail_next_sync();
    rig.links[0]
        .to_writer
        .send(ToWriter::Stop { executor: 0 })
        .unwrap();
    assert!(matches!(rig.next(0).await, Progress::Fault));
    assert!(matches!(rig.next(0).await, Progress::Stopped));
}

/// A sink that keeps what the writer reported.
#[derive(Clone, Default)]
struct Recorder {
    compactions: Arc<Mutex<Vec<CompactionReport>>>,
    faults: Arc<Mutex<Vec<LogFault>>>,
    rounds: Arc<Mutex<Vec<u64>>>,
    slow: Arc<Mutex<Vec<(u64, u64)>>>,
    slow_ended: Arc<Mutex<Vec<(u64, bool, u64)>>>,
}

impl TraceSink for Recorder {
    fn record(&self, _: u16, _: u64, _: &crate::shard::Command, _: &crate::shard::Reply) {}
    fn compaction(&self, report: &CompactionReport) {
        self.compactions.lock().unwrap().push(*report);
    }
    fn log_fault(&self, fault: LogFault, _error: &std::io::Error) {
        self.faults.lock().unwrap().push(fault);
    }
    fn sync_issued(&self, round: u64) {
        self.rounds.lock().unwrap().push(round);
    }
    fn sync_slow(&self, round: u64, in_flight_ms: u64) {
        self.slow.lock().unwrap().push((round, in_flight_ms));
    }
    fn sync_slow_ended(&self, round: u64, ok: bool, duration_ms: u64) {
        self.slow_ended
            .lock()
            .unwrap()
            .push((round, ok, duration_ms));
    }
}

async fn yield_now_n(n: usize) {
    for _ in 0..n {
        tokio::task::yield_now().await;
    }
}

/// A sync in flight past `SLOW_SYNC` is traced once, with how long it has
/// waited, and once more when it ends — and counted.
#[tokio::test(start_paused = true)]
async fn a_slow_sync_is_traced_in_and_out_and_counted() {
    let disk = MemDisk::default();
    disk.set_sync_latency(Duration::from_millis(2_500));
    let story = Recorder::default();
    let mut rig = open_with(
        &disk,
        1,
        SyncPolicy::ALWAYS,
        1 << 20,
        WriterPlants::default(),
        story.clone(),
    );
    rig.submit(0, 0, record(0, 0, b"a")); // issues a sync
    yield_now_n(8).await;
    assert_eq!(rig.stats.sync_in_flight.load(Ordering::Relaxed), 1);
    tokio::time::advance(Duration::from_millis(1_100)).await;
    yield_now_n(8).await; // the writer's tick ran
    let slow = story.slow.lock().unwrap().clone();
    assert_eq!(slow.len(), 1, "one warning past one second");
    assert_eq!(slow[0].0, 1);
    assert!((1_000..1_200).contains(&slow[0].1), "{slow:?}");
    assert_eq!(rig.stats.delayed_syncs.load(Ordering::Relaxed), 1);
    // Slept, not advanced: the paused clock stops at every timer on the way,
    // so the writer reads the instant its sync ended.
    tokio::time::sleep(Duration::from_millis(1_450)).await;
    assert_eq!(*story.slow_ended.lock().unwrap(), vec![(1, true, 2_500)]);
    assert_eq!(
        story.slow.lock().unwrap().len(),
        1,
        "warned once, not every tick"
    );
    assert_eq!(rig.stats.sync_in_flight.load(Ordering::Relaxed), 0);
    assert_eq!(rig.stats.syncs_total.load(Ordering::Relaxed), 1);
    assert_eq!(
        rig.stats.last_sync_micros.load(Ordering::Relaxed),
        2_500_000
    );
    let _ = rig.durable(0).await;
}

/// A sync that ends inside `SLOW_SYNC` writes no line and counts no delay.
#[tokio::test(start_paused = true)]
async fn a_sync_inside_the_threshold_is_counted_and_not_traced() {
    let disk = MemDisk::default();
    disk.set_sync_latency(Duration::from_millis(900));
    let story = Recorder::default();
    let mut rig = open_with(
        &disk,
        1,
        SyncPolicy::ALWAYS,
        1 << 20,
        WriterPlants::default(),
        story.clone(),
    );
    rig.submit(0, 0, record(0, 0, b"a"));
    let _ = rig.durable(0).await;
    tokio::time::advance(Duration::from_millis(500)).await;
    yield_now_n(8).await;
    assert!(story.slow.lock().unwrap().is_empty());
    assert!(story.slow_ended.lock().unwrap().is_empty());
    assert_eq!(rig.stats.delayed_syncs.load(Ordering::Relaxed), 0);
    assert_eq!(rig.stats.syncs_total.load(Ordering::Relaxed), 1);
}

/// What `INFO` reports as the log's size follows the segments on disk.
#[tokio::test(start_paused = true)]
async fn the_log_size_counts_every_segment_on_disk() {
    let disk = MemDisk::default();
    disk.create_dir_all(Path::new(WAL)).unwrap();
    disk.write_file(&Path::new(WAL).join(segment_name(0, 0)), &[0u8; 30])
        .unwrap(); // an older generation's segment
    let rig = open(&disk, 1, SyncPolicy::NEVER, 64, WriterPlants::default());
    let header = disk
        .contents(&Path::new(WAL).join(segment_name(1, 0)))
        .len() as u64;
    assert_eq!(rig.stats.log_segments.load(Ordering::Relaxed), 2);
    assert_eq!(rig.stats.log_bytes.load(Ordering::Relaxed), 30 + header);
    rig.submit(0, 0, vec![1u8; 70]); // fills rotation 0
    yield_now_n(4).await;
    rig.submit(0, 1, vec![1u8; 10]); // opens rotation 1
    yield_now_n(4).await;
    let on_disk: u64 = names(&disk)
        .iter()
        .map(|name| disk.contents(&Path::new(WAL).join(name)).len() as u64)
        .sum();
    assert_eq!(rig.stats.log_segments.load(Ordering::Relaxed), 3);
    assert_eq!(rig.stats.log_bytes.load(Ordering::Relaxed), on_disk);
    covered(&rig, 0, 0, Some(1), 100);
    yield_now_n(4).await;
    let on_disk: u64 = names(&disk)
        .iter()
        .filter(|name| parse_segment_name(name).is_some())
        .map(|name| disk.contents(&Path::new(WAL).join(name)).len() as u64)
        .sum();
    assert_eq!(
        rig.stats.log_segments.load(Ordering::Relaxed),
        names(&disk)
            .iter()
            .filter(|name| parse_segment_name(name).is_some())
            .count() as u64
    );
    assert_eq!(rig.stats.log_bytes.load(Ordering::Relaxed), on_disk);
}

fn covered(
    rig: &Rig,
    executor: usize,
    cycle: u32,
    through_batch: Option<u64>,
    snapshot_bytes: u64,
) {
    let link = &rig.links[executor];
    link.to_writer
        .send(ToWriter::Covered {
            executor: link.executor,
            cycle,
            through_batch,
            snapshot_bytes,
        })
        .unwrap();
}

fn names(disk: &MemDisk) -> Vec<String> {
    let mut names = disk.list(Path::new(WAL)).unwrap();
    names.sort();
    names
}

#[tokio::test(start_paused = true)]
async fn a_rotation_is_removed_once_every_executor_covered_its_batches_in_it() {
    let disk = MemDisk::default();
    let rig = open(&disk, 2, SyncPolicy::NEVER, 64, WriterPlants::default());
    rig.submit(0, 0, vec![1u8; 40]);
    rig.submit(1, 0, vec![2u8; 40]); // rotation 0 fills: 80 ≥ 64
    tokio::task::yield_now().await;
    rig.submit(0, 1, vec![1u8; 10]); // opens rotation 1
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(names(&disk).contains(&segment_name(1, 1)));
    covered(&rig, 0, 0, Some(1), 100);
    tokio::task::yield_now().await;
    assert!(
        names(&disk).contains(&segment_name(1, 0)),
        "executor 1 has not covered its batch 0 in rotation 0"
    );
    covered(&rig, 1, 0, Some(0), 100);
    tokio::task::yield_now().await;
    assert!(
        !names(&disk).contains(&segment_name(1, 0)),
        "both covered: rotation 0 goes"
    );
    assert!(
        names(&disk).contains(&segment_name(1, 1)),
        "the open rotation never goes"
    );
    drop(rig);
}

#[tokio::test(start_paused = true)]
async fn an_executor_that_wrote_nothing_pins_nothing_and_still_closes_the_generation() {
    let disk = MemDisk::default();
    disk.create_dir_all(Path::new(WAL)).unwrap();
    disk.write_file(&Path::new(WAL).join(segment_name(0, 0)), &[0u8; 30])
        .unwrap(); // an older generation's file
    let rig = open(&disk, 2, SyncPolicy::NEVER, 64, WriterPlants::default());
    rig.submit(0, 0, vec![1u8; 70]);
    tokio::task::yield_now().await;
    rig.submit(0, 1, vec![1u8; 10]);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    covered(&rig, 0, 0, Some(1), 100);
    tokio::task::yield_now().await;
    assert!(
        names(&disk).contains(&segment_name(1, 0)),
        "rotation 0 holds the rebases' place while an older generation remains"
    );
    assert!(
        names(&disk).contains(&segment_name(0, 0)),
        "executor 1 has not reported: the older generation stays"
    );
    covered(&rig, 1, 0, None, 0);
    tokio::task::yield_now().await;
    assert!(
        !names(&disk).contains(&segment_name(0, 0)),
        "every executor reported: older generations go"
    );
    assert!(
        !names(&disk).contains(&segment_name(1, 0)),
        "and rotation 0 with them, covered by executor 0 and never written by 1"
    );
}

#[tokio::test(start_paused = true)]
async fn a_covered_removes_that_executors_older_snapshots_and_reports_one_compaction() {
    let disk = MemDisk::default();
    disk.create_dir_all(Path::new(WAL)).unwrap();
    disk.write_file(&Path::new(WAL).join(snapshot_name(1, 0, 0)), &[0u8; 10])
        .unwrap();
    disk.write_file(&Path::new(WAL).join(snapshot_name(1, 0, 1)), &[0u8; 10])
        .unwrap();
    disk.write_file(&Path::new(WAL).join(snapshot_name(1, 1, 0)), &[0u8; 10])
        .unwrap();
    let recorder = Recorder::default();
    let rig = open_with(
        &disk,
        2,
        SyncPolicy::NEVER,
        64,
        WriterPlants::default(),
        recorder.clone(),
    );
    covered(&rig, 0, 1, None, 10);
    tokio::task::yield_now().await;
    let names = names(&disk);
    assert!(!names.contains(&snapshot_name(1, 0, 0)));
    assert!(
        names.contains(&snapshot_name(1, 0, 1)),
        "the snapshot that was reported stays"
    );
    assert!(
        names.contains(&snapshot_name(1, 1, 0)),
        "another executor's snapshot is not this executor's to lose"
    );
    let compactions = recorder.compactions.lock().unwrap().clone();
    assert_eq!(compactions.len(), 1);
    assert_eq!((compactions[0].files, compactions[0].bytes), (1, 10));
}

#[tokio::test(start_paused = true)]
async fn the_plant_removes_a_rotation_one_executor_covered_while_another_has_not() {
    let disk = MemDisk::default();
    let rig = open(
        &disk,
        2,
        SyncPolicy::NEVER,
        64,
        WriterPlants {
            removes_uncovered: true,
            ..WriterPlants::default()
        },
    );
    rig.submit(0, 0, vec![1u8; 40]);
    rig.submit(1, 0, vec![2u8; 40]);
    tokio::task::yield_now().await;
    rig.submit(0, 1, vec![1u8; 10]);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    covered(&rig, 0, 0, Some(1), 100);
    tokio::task::yield_now().await;
    assert!(
        !names(&disk).contains(&segment_name(1, 0)),
        "the plant: gone with executor 1's records in it"
    );
}

#[tokio::test(start_paused = true)]
async fn the_oldest_holder_is_nudged_once_the_retained_log_passes_its_bound() {
    // SMALL's floor is 64: executor 1 writes 10 bytes into every rotation
    // and never snapshots on its own; executor 0 fills them.
    let disk = MemDisk::default();
    let mut rig = open(&disk, 2, SyncPolicy::NEVER, 64, WriterPlants::default());
    for batch in 0..3u64 {
        rig.submit(1, batch, vec![2u8; 10]);
        rig.submit(0, batch, vec![1u8; 60]);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        covered(&rig, 0, u32::try_from(batch).unwrap(), Some(batch), 100);
        tokio::task::yield_now().await;
    }
    // Rotations closed and retained on executor 1's account: past 64 bytes
    // from the second rotation on.
    assert!(matches!(rig.next(1).await, Progress::Nudge));
    assert!(
        tokio::time::timeout(Duration::from_millis(1), rig.links[1].progress.recv())
            .await
            .is_err(),
        "nudged once until it answers"
    );
    covered(&rig, 1, 0, Some(2), 10);
    tokio::task::yield_now().await;
    let names = names(&disk);
    assert!(
        !names.contains(&segment_name(1, 0)) && !names.contains(&segment_name(1, 1)),
        "covered by both: the closed rotations go: {names:?}"
    );
    assert!(
        names.contains(&segment_name(1, 2)),
        "the open rotation never goes: {names:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn after_a_restart_the_older_generation_is_retained_log_and_its_holders_are_nudged_at_open() {
    let disk = MemDisk::default();
    disk.create_dir_all(Path::new(WAL)).unwrap();
    disk.write_file(&Path::new(WAL).join(segment_name(0, 0)), &[0u8; 100])
        .unwrap(); // 100 > SMALL.floor
    let mut rig = open(&disk, 2, SyncPolicy::NEVER, 64, WriterPlants::default());
    assert!(matches!(rig.next(0).await, Progress::Nudge));
    assert!(matches!(rig.next(1).await, Progress::Nudge));
}

/// A rotation whose directory sync failed may have left its file behind:
/// the retry takes the name after it rather than appending a second header
/// to that one, which the next start would read as damage.
#[tokio::test(start_paused = true)]
async fn a_rotation_whose_directory_sync_fails_is_retried_under_the_next_name() {
    let disk = MemDisk::default();
    let mut rig = open(&disk, 1, SyncPolicy::NEVER, 64, WriterPlants::default());
    rig.submit(0, 0, vec![1u8; 70]);
    tokio::task::yield_now().await;
    disk.fail_one_dir_sync_after(0);
    rig.submit(0, 1, vec![2u8; 10]);
    assert!(matches!(rig.next(0).await, Progress::Fault));
    rig.submit(0, 2, vec![3u8; 10]);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert_eq!(
        decode_segment_header(&rig.segment(1)),
        Ok((1, 1)),
        "the failed attempt's file holds its header alone"
    );
    assert_eq!(rig.segment(1).len(), crate::log::file::SEGMENT_HEADER_LEN);
    assert_eq!(decode_segment_header(&rig.segment(2)), Ok((1, 2)));
    assert_eq!(
        rig.segment(2).len(),
        crate::log::file::SEGMENT_HEADER_LEN + 10,
        "the retry writes into the next name"
    );
}

#[tokio::test(start_paused = true)]
async fn a_covered_that_removes_nothing_reports_nothing_and_syncs_no_directory() {
    let disk = MemDisk::default();
    let recorder = Recorder::default();
    let rig = open_with(
        &disk,
        2,
        SyncPolicy::NEVER,
        64,
        WriterPlants::default(),
        recorder.clone(),
    );
    // A directory sync now would fail and be traced as a removal's fault.
    disk.fail_one_dir_sync_after(0);
    covered(&rig, 0, 0, None, 10);
    tokio::task::yield_now().await;
    assert!(recorder.compactions.lock().unwrap().is_empty());
    assert!(
        recorder.faults.lock().unwrap().is_empty(),
        "no directory was synced"
    );
}
