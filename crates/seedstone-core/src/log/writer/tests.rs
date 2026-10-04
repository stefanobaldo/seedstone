//! The writer driven by hand over `MemDisk`: the tests hold the
//! executors' ends of the links.

use super::*;
use crate::log::checkpoint::CheckpointConfig;
use crate::log::disk::mem::MemDisk;
use crate::log::file::{decode_segment_header, segment_name};
use crate::log::{Decoded, decode_record};
use crate::shard::{NoTrace, SyncPolicy};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;

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
}

fn open(
    disk: &MemDisk,
    executors: u16,
    policy: SyncPolicy,
    segment_bytes: u64,
    plants: WriterPlants,
) -> Rig {
    disk.create_dir_all(Path::new(WAL)).unwrap();
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
        trace: NoTrace,
        plants,
    })
    .unwrap();
    Rig {
        disk: disk.clone(),
        links,
        task: tokio::spawn(writer.run(inbox)),
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
    })
    .unwrap();
    let mut shards = vec![
        crate::log::recovery::RecoveredShard {
            dict: crate::dict::Dict::with_seed(crate::dict::DictSeed { k0: 1, k1: 2 }),
            seq: 4,
            lossy: false,
            cut: true,
        },
        crate::log::recovery::RecoveredShard {
            dict: crate::dict::Dict::with_seed(crate::dict::DictSeed { k0: 1, k1: 3 }),
            seq: 9,
            lossy: false,
            cut: false,
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
