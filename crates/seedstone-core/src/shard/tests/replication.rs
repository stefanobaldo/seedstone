//! What reaches the log and the trace sink, at which replication position,
//! and what a log that refuses a write does to the command that needed it.

use super::support::{NoSweep, Recorder, get, set, set_ex};
use crate::dict::DictSeed;
use crate::log::{Record, ReplicationLog};
use crate::shard::{Command, HOUSEKEEPING_TICK, NoTrace, Reply, ReplyError, Router, ShardPool};
use crate::slot::shard_of;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// What the sink's `seq` means when one command consumes two positions.
///
/// A read that evicts appends one record, so it cannot tell the two
/// candidate contracts apart. A *write* over an expired key appends the
/// eviction's record and then its own, and the position reported is the
/// first — where the command's effects began, not where its own record
/// landed. Both write paths are exercised, because they append in
/// different arms.
///
/// The pool is spawned with a policy that never sweeps, so the only thing
/// that can remove these keys is the command that meets them — which is
/// the path these assertions are about. The tick is deliberately fired
/// several times in between: under the honest policy that firing would
/// reclaim them and the trace would carry the sweep's own removals instead
/// of the gaps, so the loop is what proves the dependence is gone rather
/// than merely unobserved.
///
/// The two writes still go in one batch. That is now a matter of clarity
/// alone — the policy, not the batching, is what keeps the sweep out of
/// these positions.
#[tokio::test(start_paused = true)]
async fn a_command_is_traced_where_its_effects_begin_not_where_its_record_landed() {
    let sink = Recorder::default();
    let pool = ShardPool::spawn_with_policy(1, 1, DictSeed { k0: 2, k1: 3 }, sink.clone(), NoSweep);

    pool.dispatch(set_ex(b"written", b"v", 1)).await;
    pool.dispatch(set_ex(b"counted", b"1", 1)).await;
    tokio::time::advance(Duration::from_secs(2)).await;
    for _ in 0..4 {
        tokio::time::advance(HOUSEKEEPING_TICK).await;
        tokio::task::yield_now().await;
    }
    pool.dispatch_many(vec![
        set(b"written", b"again"),
        Command::IncrBy {
            key: b"counted".to_vec(),
            delta: 7,
        },
    ])
    .await;
    pool.dispatch(get(b"written")).await;

    let seen = sink.0.lock().expect("recorder mutex").clone();
    assert_eq!(
        seen,
        vec![
            (0, 0, 2, Reply::Ok),
            (0, 1, 2, Reply::Ok),
            // Eviction at 2, the write's own record at 3, traced at 2.
            (0, 2, 2, Reply::Ok),
            // Eviction at 4, the increment's own record at 5, traced at 4.
            // A counter that had expired starts from zero.
            (0, 4, 4, Reply::Integer(7)),
            // Which leaves the next command at 6: four positions for two
            // commands is exactly what the contract says can happen.
            (0, 6, 1, Reply::Bulk(Some(b"again".to_vec()))),
        ]
    );
}

#[tokio::test]
async fn the_sink_sees_every_command_at_its_replication_position() {
    let sink = Recorder::default();
    // One shard, so every command shares a `seq` counter and the observed
    // positions are a single sequence rather than an interleaving.
    let pool = ShardPool::spawn(1, 1, DictSeed { k0: 2, k1: 3 }, sink.clone());

    pool.dispatch(Command::Get { key: b"k".to_vec() }).await;
    pool.dispatch(set(b"k", b"1")).await;
    pool.dispatch(Command::IncrBy {
        key: b"k".to_vec(),
        delta: 4,
    })
    .await;
    pool.dispatch(Command::Del {
        key: b"gone".to_vec(),
    })
    .await;

    let seen = sink.0.lock().expect("recorder mutex").clone();
    assert_eq!(
        seen,
        vec![
            // The read ran at position 0 and did not consume it.
            (0, 0, 1, Reply::Bulk(None)),
            (0, 0, 2, Reply::Ok),
            (0, 1, 4, Reply::Integer(5)),
            // The delete found nothing, so it did not consume position 2.
            (0, 2, 3, Reply::Removed(false)),
        ]
    );
}

#[tokio::test]
async fn shards_sharing_an_executor_keep_independent_replication_positions() {
    let sink = Recorder::default();
    // Four shards on one executor: every shard's state lives in one task, and
    // the positions must still be per shard, not per executor.
    let pool = ShardPool::spawn(4, 1, DictSeed { k0: 2, k1: 3 }, sink.clone());

    // Two keys on two different shards (probe until found).
    let keys: Vec<Vec<u8>> = (0..32u32).map(|i| format!("k{i}").into_bytes()).collect();
    let a = keys
        .iter()
        .find(|k| shard_of(k, 4) == 0)
        .expect("a key on shard 0")
        .clone();
    let b = keys
        .iter()
        .find(|k| shard_of(k, 4) == 1)
        .expect("a key on shard 1")
        .clone();

    for key in [&a, &b, &a, &b] {
        pool.dispatch(set(key, b"v")).await;
    }
    let seen = sink.0.lock().expect("recorder mutex").clone();
    let positions: Vec<(u16, u64)> = seen
        .iter()
        .map(|(shard, seq, _, _)| (*shard, *seq))
        .collect();
    assert_eq!(positions, vec![(0, 0), (1, 0), (0, 1), (1, 1)]);
}

/// The seam, exercised rather than asserted.
///
/// A log that is not [`NoopLog`] reaches a shard and sees every mutation at
/// its replication position. Until the pool took a log factory this test
/// could not be written at all, which is what made "the seam exists from
/// day one" a claim about intent rather than about the code.
#[tokio::test]
async fn a_supplied_log_receives_every_mutation() {
    #[derive(Clone, Default)]
    struct Recording(Arc<Mutex<Vec<(u16, u64)>>>);

    impl ReplicationLog for Recording {
        fn append(&mut self, rec: Record<'_>) -> std::io::Result<()> {
            self.0.lock().expect("log mutex").push((rec.shard, rec.seq));
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let log = Recording::default();
    let pool = ShardPool::spawn_with_log(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace, {
        let log = log.clone();
        move |_shard| log.clone()
    });

    pool.dispatch(set(b"k", b"v")).await;
    pool.dispatch(Command::Get { key: b"k".to_vec() }).await;
    pool.dispatch(Command::Del {
        key: b"absent".to_vec(),
    })
    .await;
    pool.dispatch(Command::IncrBy {
        key: b"n".to_vec(),
        delta: 1,
    })
    .await;

    // The read and the delete-that-removed-nothing append nothing, so the
    // positions are gapless — the same property `seq` is asserted to have.
    assert_eq!(*log.0.lock().expect("log mutex"), vec![(0, 0), (0, 1)]);
}

/// A mutation whose record cannot be written must not happen.
///
/// `apply`'s documentation says the record is appended *before* the dict is
/// touched, so a record can never describe a change that was not made and a
/// change can never outrun its record. With only [`NoopLog`] reachable,
/// nothing could fail an append and that ordering had no coverage at all.
#[tokio::test]
async fn a_log_that_cannot_write_refuses_the_mutation() {
    struct Failing;

    impl ReplicationLog for Failing {
        fn append(&mut self, _rec: Record<'_>) -> std::io::Result<()> {
            Err(std::io::Error::other("the disk went away"))
        }
        fn sync(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let pool =
        ShardPool::spawn_with_log(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace, |_shard| Failing);

    assert_eq!(
        pool.dispatch(set(b"k", b"v")).await,
        Reply::Error(ReplyError::LogWriteFailed)
    );
    // And the write did not land: the refusal is not cosmetic.
    assert_eq!(
        pool.dispatch(Command::Get { key: b"k".to_vec() }).await,
        Reply::Bulk(None),
        "the value was stored despite its record failing"
    );
    // An unloggable IncrBy is refused for the same reason, rather than
    // incrementing and reporting a number nothing recorded.
    assert_eq!(
        pool.dispatch(Command::IncrBy {
            key: b"n".to_vec(),
            delta: 5
        })
        .await,
        Reply::Error(ReplyError::LogWriteFailed)
    );
}
