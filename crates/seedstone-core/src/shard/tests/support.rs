//! What more than one subject file needs: one shard's state driven through
//! the interpreter directly, the command constructors, the counter sums, the
//! fake policy and trace sink the subjects share, and pools over a log on the
//! in-memory disk, with the node's writer or with the test playing it.

use crate::dict::{Dict, DictSeed, WalkOrder, shard_seed};
use crate::log::NoopLog;
use crate::log::checkpoint::{CheckpointConfig, CheckpointSpec, SegmentCheckpoint};
use crate::log::disk::Disk;
use crate::log::disk::mem::MemDisk;
use crate::log::file::{FileLog, next_generation};
use crate::log::recovery::{ReaderMode, RecoverSpec, RecoveredShard, recover};
use crate::log::writer::{
    Opened, Progress, ToWriter, Writer, WriterLink, WriterPlants, WriterSpec, write_rebases,
};
use crate::memory::MemoryLimit;
use crate::shard::apply::apply;
use crate::shard::executor::ShardState;
use crate::shard::{
    Command, Deadlines, EvictionPolicy, ExecutorPlants, Expiry, ExpiryPolicy, Now, PoolSpec, Reply,
    Router, ShardPool, ShardStats, SyncPolicy, TraceSink, frozen_clock,
};
use bytes::Bytes;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time::Instant;

/// One shard's state, driven through [`apply`] directly.
///
/// The expiry tests need two things a pool deliberately does not offer:
/// the `now` each command sees, which in production is the executor's to
/// choose, and the dict a handler left behind — an expired entry has to be
/// shown *gone*, not merely invisible to a read.
pub type Shard = ShardState<NoopLog>;

impl Shard {
    #[must_use]
    pub fn for_tests() -> Self {
        Self::new(Dict::with_seed(DictSeed { k0: 5, k1: 7 }), NoopLog)
    }

    /// By value, because [`apply`] takes what it stores: a command that has
    /// been run has had its value moved out of it, so a caller cannot
    /// usefully hold one across two runs.
    pub fn run(&mut self, mut cmd: Command, now: Instant) -> Reply {
        apply(self, 0, &mut cmd, Now::at(now), &Deadlines)
    }
}

/// `SET key value`, with no options.
pub fn set(key: &[u8], value: &[u8]) -> Command {
    Command::Set {
        key: Bytes::copy_from_slice(key),
        value: Bytes::copy_from_slice(value),
        expiry: None,
        cond: None,
        keep_ttl: false,
        get: false,
    }
}

/// `SET key value EX seconds`.
pub fn set_ex(key: &[u8], value: &[u8], seconds: u64) -> Command {
    Command::Set {
        key: Bytes::copy_from_slice(key),
        value: Bytes::copy_from_slice(value),
        expiry: Some(Expiry::Ex(seconds)),
        cond: None,
        keep_ttl: false,
        get: false,
    }
}

/// `SETEX key seconds value`.
pub fn setex(key: &[u8], seconds: u64, value: &[u8]) -> Command {
    Command::SetEx {
        key: Bytes::copy_from_slice(key),
        seconds,
        value: Bytes::copy_from_slice(value),
    }
}

pub fn get(key: &[u8]) -> Command {
    Command::Get {
        key: Bytes::copy_from_slice(key),
    }
}

/// Every shard's counters, summed the way `INFO` sums them.
pub async fn gathered(pool: &ShardPool) -> ShardStats {
    let mut total = ShardStats::default();
    for reply in pool.dispatch_every(Command::Stats).await {
        let Reply::Stats(stats) = reply else {
            panic!("{reply:?}")
        };
        total.keys += stats.keys;
        total.expires += stats.expires;
        total.evicted += stats.evicted;
        total.hits += stats.hits;
        total.misses += stats.misses;
        total.expired += stats.expired;
        for (sum, count) in total.calls.iter_mut().zip(stats.calls) {
            *sum += count;
        }
    }
    total
}

pub async fn evicted(pool: &ShardPool) -> u64 {
    pool.dispatch_every(Command::Stats)
        .await
        .into_iter()
        .map(|r| match r {
            Reply::Stats(s) => s.evicted,
            other => panic!("{other:?}"),
        })
        .sum()
}

/// One observed call: the shard, the replication position where the
/// command's effects began, the command's kind tag, and the reply.
///
/// Not "the position it ran at" and not "the record it wrote" — see
/// [`TraceSink::record`], whose doc is the definition this restates.
pub type Observed = (u16, u64, u8, Reply);

#[derive(Clone, Default)]
pub struct Recorder(pub Arc<Mutex<Vec<Observed>>>);

impl TraceSink for Recorder {
    fn record(&self, shard: u16, seq: u64, cmd: &Command, reply: &Reply) {
        self.0
            .lock()
            .expect("recorder mutex")
            .push((shard, seq, cmd.kind(), reply.clone()));
    }
}

/// Honest in front of a command, inert on the housekeeping tick.
///
/// What the two trace tests below need is a keyspace where only the lazy
/// path can remove anything, so that what they assert about positions is
/// a property of the code and not of when the tick happened to fire.
#[derive(Clone, Copy)]
pub struct NoSweep;

impl WalkOrder for NoSweep {}

impl ExpiryPolicy for NoSweep {
    fn due_on_read(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        Deadlines.due_on_read(expires_at, now)
    }
    fn due_on_sweep(&self, _expires_at: Option<Instant>, _now: Instant) -> bool {
        false
    }
    fn takes_undated(&self) -> bool {
        false
    }
}

impl EvictionPolicy for NoSweep {
    fn must_evict(&self, used: u64, ceiling: Option<u64>) -> bool {
        Deadlines.must_evict(used, ceiling)
    }
}

/// The wal of every disk-backed pool here.
pub const WAL: &str = "/data/wal";

/// The disk-backed pools' checkpoint: a floor a few writes cross.
pub const SMALL: CheckpointConfig = CheckpointConfig {
    floor: 64,
    ratio: 1,
    bytes_per_tick: 1024,
};

/// The root seed of the disk-backed pools.
pub const SEED: DictSeed = DictSeed { k0: 1, k1: 2 };

/// The channels [`Writer::open`] builds, with the writer's ends handed to
/// the test, which then plays the writer by hand.
pub fn fake_links(
    executors: u16,
) -> (
    Vec<WriterLink>,
    mpsc::UnboundedReceiver<ToWriter>,
    Vec<mpsc::UnboundedSender<Progress>>,
) {
    let (to_writer, inbox) = mpsc::unbounded_channel();
    let (links, progress) = (0..executors)
        .map(|executor| {
            let (tx, rx) = mpsc::unbounded_channel();
            (
                WriterLink {
                    executor,
                    to_writer: to_writer.clone(),
                    progress: rx,
                },
                tx,
            )
        })
        .unzip();
    (links, inbox, progress)
}

/// Four shards on two executors over the in-memory disk, with the node's
/// writer, the real logs and the real checkpoint: recovery, the writer, the
/// rebases, the pool, the writer's task — the binary's start path.
pub fn disk_pool<T: TraceSink>(policy: SyncPolicy, trace: T) -> (MemDisk, ShardPool) {
    let disk = MemDisk::default();
    let wal = Path::new(WAL);
    disk.create_dir_all(wal).unwrap();
    let recovery = recover(RecoverSpec {
        disk: &disk,
        wal,
        shards: 4,
        reader: ReaderMode::Resynchronising,
        trust_unfinished: false,
        seed: SEED,
        now: Now::at(Instant::now()),
    })
    .unwrap();
    let pool = start(&disk, policy, trace, recovery.shards, false);
    (disk, pool)
}

/// [`disk_pool`] from a recovery that cut shard 0, on a disk that fails
/// every write from the moment the writer's segment is open: the rebase
/// fails, and the node starts refusing. Writes stay failing until the test
/// says otherwise.
pub fn disk_pool_cut<T: TraceSink>(policy: SyncPolicy, trace: T) -> (MemDisk, ShardPool) {
    let disk = MemDisk::default();
    disk.create_dir_all(Path::new(WAL)).unwrap();
    let recovered = (0..4)
        .map(|shard| RecoveredShard {
            dict: Dict::with_seed(shard_seed(SEED, shard)),
            seq: 0,
            lossy: shard == 0,
            cut: shard == 0,
            image_unix_millis: None,
        })
        .collect();
    let pool = start(&disk, policy, trace, recovered, true);
    (disk, pool)
}

fn start<T: TraceSink>(
    disk: &MemDisk,
    policy: SyncPolicy,
    trace: T,
    mut recovered: Vec<RecoveredShard>,
    failing: bool,
) -> ShardPool {
    let wal = Path::new(WAL);
    let generation = next_generation(disk, wal).unwrap();
    let stats = crate::shard::PersistenceStats::new(2);
    let Opened {
        mut writer,
        inbox,
        links,
    } = Writer::open(WriterSpec {
        disk: disk.clone(),
        wal: wal.to_path_buf(),
        generation,
        executors: 2,
        policy,
        segment_bytes: 1 << 20,
        checkpoint: SMALL,
        trace: trace.clone(),
        plants: WriterPlants::default(),
        stats: stats.clone(),
    })
    .unwrap();
    disk.fail_writes(failing);
    let log_failed = write_rebases(&mut writer, &mut recovered).is_err();
    let pool = ShardPool::spawn_spec(PoolSpec {
        shards: 4,
        executors: 2,
        seed: SEED,
        trace,
        make_log: FileLog::new,
        policy: Deadlines,
        limit: MemoryLimit::default(),
        clock: frozen_clock,
        recovered,
        make_checkpoint: {
            let disk = disk.clone();
            move |executor| {
                SegmentCheckpoint::new(CheckpointSpec {
                    disk: disk.clone(),
                    wal: wal.to_path_buf(),
                    generation,
                    executor,
                    config: SMALL,
                })
            }
        },
        sync: policy,
        plants: ExecutorPlants::default(),
        writer_links: links,
        log_failed,
        stats,
    });
    tokio::spawn(writer.run(inbox));
    pool
}
