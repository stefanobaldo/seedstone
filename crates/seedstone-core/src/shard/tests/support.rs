//! What more than one subject file needs: one shard's state driven through
//! the interpreter directly, the command constructors, the counter sums, and
//! the fake policy and trace sink the subjects share.

use crate::dict::{Dict, DictSeed, WalkOrder};
use crate::log::NoopLog;
use crate::shard::apply::apply;
use crate::shard::executor::ShardState;
use crate::shard::{
    Command, Deadlines, EvictionPolicy, Expiry, ExpiryPolicy, Reply, Router, ShardPool, ShardStats,
    TraceSink,
};
use std::sync::{Arc, Mutex};
use tokio::time::Instant;

/// One shard's state, driven through [`apply`] directly.
///
/// The expiry tests need two things a pool deliberately does not offer:
/// the `now` each command sees, which in production is the executor's to
/// choose, and the dict a handler left behind — an expired entry has to be
/// shown *gone*, not merely invisible to a read.
pub type Shard = ShardState<NoopLog>;

impl Shard {
    pub fn for_tests() -> Self {
        Self::new(Dict::with_seed(DictSeed { k0: 5, k1: 7 }), NoopLog)
    }

    /// By value, because [`apply`] takes what it stores: a command that has
    /// been run has had its value moved out of it, so a caller cannot
    /// usefully hold one across two runs.
    pub fn run(&mut self, mut cmd: Command, now: Instant) -> Reply {
        apply(self, 0, &mut cmd, now, &Deadlines)
    }
}

/// `SET key value`, with no options.
pub fn set(key: &[u8], value: &[u8]) -> Command {
    Command::Set {
        key: key.to_vec(),
        value: value.to_vec(),
        expiry: None,
        cond: None,
        keep_ttl: false,
        get: false,
    }
}

/// `SET key value EX seconds`.
pub fn set_ex(key: &[u8], value: &[u8], seconds: u64) -> Command {
    Command::Set {
        key: key.to_vec(),
        value: value.to_vec(),
        expiry: Some(Expiry::Ex(seconds)),
        cond: None,
        keep_ttl: false,
        get: false,
    }
}

/// `SETEX key seconds value`.
pub fn setex(key: &[u8], seconds: u64, value: &[u8]) -> Command {
    Command::SetEx {
        key: key.to_vec(),
        seconds,
        value: value.to_vec(),
    }
}

pub fn get(key: &[u8]) -> Command {
    Command::Get { key: key.to_vec() }
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
