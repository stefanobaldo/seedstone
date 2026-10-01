//! What reaches the log and the trace sink, at which replication position,
//! and what a log that refuses a write does to the command that needed it.

use super::support::{NoSweep, Recorder, get, set, set_ex};
use crate::dict::{Dict, DictSeed, Entry};
use crate::log::effect::Owned;
use crate::log::{Record, ReplicationLog};
use crate::shard::{
    Command, Expiry, HOUSEKEEPING_TICK, NoTrace, Now, Reply, ReplyError, Router, ShardPool,
    replay_into,
};
use crate::slot::shard_of;
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

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
            key: Bytes::from_static(b"counted"),
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
            (0, 6, 1, Reply::Bulk(Some(Bytes::from_static(b"again")))),
        ]
    );
}

#[tokio::test]
async fn the_sink_sees_every_command_at_its_replication_position() {
    let sink = Recorder::default();
    // One shard, so every command shares a `seq` counter and the observed
    // positions are a single sequence rather than an interleaving.
    let pool = ShardPool::spawn(1, 1, DictSeed { k0: 2, k1: 3 }, sink.clone());

    pool.dispatch(Command::Get {
        key: Bytes::from_static(b"k"),
    })
    .await;
    pool.dispatch(set(b"k", b"1")).await;
    pool.dispatch(Command::IncrBy {
        key: Bytes::from_static(b"k"),
        delta: 4,
    })
    .await;
    pool.dispatch(Command::Del {
        key: Bytes::from_static(b"gone"),
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
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<Option<u64>> {
            Ok(None)
        }
    }

    let log = Recording::default();
    let pool = ShardPool::spawn_with_log(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace, {
        let log = log.clone();
        move |_shard| log.clone()
    });

    pool.dispatch(set(b"k", b"v")).await;
    pool.dispatch(Command::Get {
        key: Bytes::from_static(b"k"),
    })
    .await;
    pool.dispatch(Command::Del {
        key: Bytes::from_static(b"absent"),
    })
    .await;
    pool.dispatch(Command::IncrBy {
        key: Bytes::from_static(b"n"),
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
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<Option<u64>> {
            Ok(None)
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
        pool.dispatch(Command::Get {
            key: Bytes::from_static(b"k")
        })
        .await,
        Reply::Bulk(None),
        "the value was stored despite its record failing"
    );
    // An unloggable IncrBy is refused for the same reason, rather than
    // incrementing and reporting a number nothing recorded.
    assert_eq!(
        pool.dispatch(Command::IncrBy {
            key: Bytes::from_static(b"n"),
            delta: 5
        })
        .await,
        Reply::Error(ReplyError::LogWriteFailed)
    );
}

/// Every mutation logs the effect it had, with the deadline made absolute.
///
/// The wall clock is injected and starts at zero here, so a `SET … EX 30`
/// logs a deadline of exactly thirty thousand milliseconds.
#[tokio::test]
async fn every_mutation_logs_its_effect_with_an_absolute_deadline() {
    use crate::log::effect::{Effect, Owned};

    #[derive(Clone, Default)]
    struct Recording(Arc<Mutex<Vec<(u64, Owned)>>>);

    impl ReplicationLog for Recording {
        fn append(&mut self, rec: Record<'_>) -> std::io::Result<()> {
            let effect = Effect::decode(rec.payload).expect("a well-formed payload");
            self.0
                .lock()
                .expect("log mutex")
                .push((rec.seq, effect.to_owned()));
            Ok(())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<Option<u64>> {
            Ok(None)
        }
    }

    let log = Recording::default();
    let pool = ShardPool::spawn_with_log(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace, {
        let log = log.clone();
        move |_shard| log.clone()
    });

    pool.dispatch(Command::Set {
        key: Bytes::from_static(b"k"),
        value: Bytes::from_static(b"v"),
        expiry: Some(Expiry::Ex(30)),
        cond: None,
        keep_ttl: false,
        get: false,
    })
    .await;
    pool.dispatch(Command::IncrBy {
        key: Bytes::from_static(b"n"),
        delta: 7,
    })
    .await;
    pool.dispatch(Command::Persist {
        key: Bytes::from_static(b"k"),
    })
    .await;
    pool.dispatch(Command::Del {
        key: Bytes::from_static(b"n"),
    })
    .await;
    // `FLUSHDB` addresses every shard, so it goes the way the edge sends it.
    pool.dispatch_every(Command::FlushDb).await;

    let recorded = log.0.lock().expect("log mutex").clone();
    assert_eq!(
        recorded,
        vec![
            (
                0,
                Owned::Put {
                    key: Bytes::from_static(b"k"),
                    value: Bytes::from_static(b"v"),
                    deadline: Some(30_000),
                }
            ),
            (
                1,
                Owned::Put {
                    key: Bytes::from_static(b"n"),
                    value: Bytes::from_static(b"7"),
                    deadline: None,
                }
            ),
            (
                2,
                Owned::Deadline {
                    key: Bytes::from_static(b"k"),
                    deadline: None,
                }
            ),
            (
                3,
                Owned::Del {
                    key: Bytes::from_static(b"n"),
                }
            ),
            (4, Owned::Flush),
        ]
    );
}

/// The tick flushes every shard's log, then syncs it, and a failure of
/// either reaches the trace sink as a fault rather than vanishing.
#[tokio::test(start_paused = true)]
async fn the_tick_flushes_then_syncs_and_reports_a_failure() {
    use crate::shard::{HOUSEKEEPING_TICK, LogFault, PoolSpec, TraceSink};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Clone, Default)]
    struct Journal(Arc<Mutex<Vec<&'static str>>>);

    impl ReplicationLog for Journal {
        fn append(&mut self, _rec: Record<'_>) -> std::io::Result<()> {
            self.0.lock().expect("journal").push("append");
            Ok(())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.0.lock().expect("journal").push("flush");
            Err(std::io::Error::other("the disk went away"))
        }
        fn sync(&mut self) -> std::io::Result<Option<u64>> {
            self.0.lock().expect("journal").push("sync");
            Ok(None)
        }
    }

    #[derive(Clone, Default)]
    struct Faults(Arc<AtomicU64>);

    impl TraceSink for Faults {
        fn record(&self, _shard: u16, _seq: u64, _cmd: &Command, _reply: &Reply) {}
        fn fault(&self, shard: u16, fault: LogFault, error: &std::io::Error) {
            assert_eq!(shard, 0);
            assert_eq!(fault, LogFault::Write);
            assert_eq!(error.to_string(), "the disk went away");
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let journal = Journal::default();
    let faults = Faults::default();
    let pool = ShardPool::spawn_spec(PoolSpec {
        shards: 1,
        executors: 1,
        seed: DictSeed { k0: 1, k1: 2 },
        trace: faults.clone(),
        make_log: {
            let journal = journal.clone();
            move |_shard| journal.clone()
        },
        policy: crate::shard::Deadlines,
        limit: crate::memory::MemoryLimit::default(),
        clock: crate::shard::frozen_clock,
        recovered: Vec::new(),
        make_checkpoint: |_executor| crate::log::checkpoint::NoCheckpoint,
        sync: crate::shard::SyncPolicy::INTERVAL,
        plants: crate::shard::ExecutorPlants::default(),
    });
    pool.dispatch(set(b"k", b"v")).await;
    tokio::time::advance(HOUSEKEEPING_TICK + Duration::from_millis(1)).await;
    tokio::task::yield_now().await;

    let seen = journal.0.lock().expect("journal").clone();
    assert!(
        seen.starts_with(&["append", "flush", "sync"]),
        "flush precedes sync on the tick, and the failed flush does not stop the sync: {seen:?}"
    );
    assert_eq!(
        faults.0.load(Ordering::SeqCst),
        1,
        "one flush failed, one fault reported"
    );
}

/// Replay from an in-memory log: the four effects, a deadline in the future
/// kept, a deadline in the past removing the key, and a flush clearing what
/// came before it.
#[tokio::test(start_paused = true)]
async fn replay_applies_effects_and_resolves_absolute_deadlines() {
    use crate::dict::Dict;
    use crate::log::NoopLog;
    use crate::shard::Now;
    use crate::shard::executor::ShardState;

    let mut state = ShardState::new(Dict::with_seed(DictSeed { k0: 1, k1: 2 }), NoopLog);
    let now = Now {
        instant: tokio::time::Instant::now(),
        unix_millis: 1_000_000,
    };
    state.replay(replayed_log(), now);
    assert_eq!(state.seq, 10, "the shard resumes after the last record");
    assert!(state.dict.get(b"stale").is_none(), "flushed");
    assert!(
        state.dict.get(b"dead").is_none(),
        "a past deadline removes the key"
    );
    assert!(
        state.dict.get(b"n").is_none(),
        "a deadline exactly now is past"
    );
    let p = state.dict.get(b"p").expect("persisted");
    assert_eq!(
        p.expires_at, None,
        "a Deadline of None removes the deadline"
    );
    let live = state.dict.get(b"live").expect("re-put after its delete");
    assert_eq!(&live.value[..], b"3");
    assert_eq!(
        live.expires_at,
        Some(now.instant + Duration::from_secs(30)),
        "a future deadline resolves against the moving wall clock"
    );
}

/// A deadline that has passed by the time of the replay is not the key's
/// fate when a later record in the prefix moved it: the node served the key
/// under the later deadline, so the replay must too. Both shapes — a `Put`
/// whose own deadline passed, then an `EXPIRE` that reached it first; and a
/// `Deadline` that passed, then another that extended it.
#[tokio::test(start_paused = true)]
async fn a_passed_deadline_that_a_later_record_extended_keeps_the_key() {
    use crate::dict::Dict;
    use crate::log::NoopLog;
    use crate::log::effect::Owned;
    use crate::shard::Now;
    use crate::shard::executor::ShardState;

    let key = |k: &'static [u8]| Bytes::from_static(k);
    let mut state = ShardState::new(Dict::with_seed(DictSeed { k0: 1, k1: 2 }), NoopLog);
    let now = Now {
        instant: tokio::time::Instant::now(),
        unix_millis: 1_000_000,
    };
    state.replay(
        vec![
            (
                0,
                Owned::Put {
                    key: key(b"put"),
                    value: key(b"v"),
                    deadline: Some(999_000),
                },
            ),
            (
                1,
                Owned::Deadline {
                    key: key(b"put"),
                    deadline: Some(1_010_000),
                },
            ),
            (
                2,
                Owned::Put {
                    key: key(b"moved"),
                    value: key(b"w"),
                    deadline: None,
                },
            ),
            (
                3,
                Owned::Deadline {
                    key: key(b"moved"),
                    deadline: Some(999_500),
                },
            ),
            (
                4,
                Owned::Deadline {
                    key: key(b"moved"),
                    deadline: Some(1_020_000),
                },
            ),
            (
                5,
                Owned::Put {
                    key: key(b"gone"),
                    value: key(b"x"),
                    deadline: Some(1_040_000),
                },
            ),
            (
                6,
                Owned::Deadline {
                    key: key(b"gone"),
                    deadline: Some(999_900),
                },
            ),
        ],
        now,
    );
    let put = state.dict.get(b"put").expect("extended before it expired");
    assert_eq!(&put.value[..], b"v");
    assert_eq!(put.expires_at, Some(now.instant + Duration::from_secs(10)));
    let moved = state
        .dict
        .get(b"moved")
        .expect("extended after a passed deadline");
    assert_eq!(
        moved.expires_at,
        Some(now.instant + Duration::from_secs(20))
    );
    assert!(
        state.dict.get(b"gone").is_none(),
        "the key's last deadline passed: it is dead"
    );
    assert_eq!(state.seq, 7);
}

/// The log `replay_applies_effects_and_resolves_absolute_deadlines` replays,
/// against a wall clock reading 1 000 000.
fn replayed_log() -> Vec<(u64, crate::log::effect::Owned)> {
    use crate::log::effect::Owned;
    let key = |k: &'static [u8]| Bytes::from_static(k);
    vec![
        (
            0,
            Owned::Put {
                key: key(b"stale"),
                value: key(b"v"),
                deadline: None,
            },
        ),
        (1, Owned::Flush),
        (
            2,
            Owned::Put {
                key: key(b"live"),
                value: key(b"1"),
                deadline: Some(1_030_000),
            },
        ),
        (
            3,
            Owned::Put {
                key: key(b"dead"),
                value: key(b"2"),
                deadline: Some(999_000),
            },
        ),
        (
            4,
            Owned::Put {
                key: key(b"n"),
                value: key(b"7"),
                deadline: None,
            },
        ),
        (
            5,
            Owned::Deadline {
                key: key(b"n"),
                deadline: Some(1_000_000),
            },
        ),
        (
            6,
            Owned::Put {
                key: key(b"p"),
                value: key(b"8"),
                deadline: Some(1_005_000),
            },
        ),
        (
            7,
            Owned::Deadline {
                key: key(b"p"),
                deadline: None,
            },
        ),
        (8, Owned::Del { key: key(b"live") }),
        (
            9,
            Owned::Put {
                key: key(b"live"),
                value: key(b"3"),
                deadline: Some(1_030_000),
            },
        ),
    ]
}

/// A pool built from a recovery serves what the log held, and reports each
/// shard's resumed position to the sink.
#[tokio::test]
async fn a_pool_spawned_from_a_recovery_serves_the_recovered_keys() {
    use crate::log::recovery::RecoveredShard;
    use crate::shard::{PoolSpec, TraceSink};

    #[derive(Clone, Default)]
    struct Resumed(Arc<Mutex<Vec<(u16, u64, bool)>>>);
    impl TraceSink for Resumed {
        fn record(&self, _shard: u16, _seq: u64, _cmd: &Command, _reply: &Reply) {}
        fn recovered(&self, shard: u16, next_seq: u64, lossy: bool) {
            self.0
                .lock()
                .expect("resumed")
                .push((shard, next_seq, lossy));
        }
    }

    let resumed = Resumed::default();
    // `k` hashes to shard 1 of 2 — asserted, so the test cannot silently
    // stop meaning what it says — and the recovery puts it there.
    assert_eq!(
        crate::slot::shard_of(b"k", 2),
        1,
        "a key that lands on shard 1"
    );
    let root = DictSeed { k0: 1, k1: 2 };
    let mut imaged = Dict::with_seed(crate::dict::shard_seed(root, 1));
    imaged.insert(
        Bytes::from_static(b"k"),
        Entry {
            value: Bytes::from_static(b"v"),
            expires_at: None,
            touched: 0,
        },
    );
    let recovered = vec![
        RecoveredShard {
            dict: Dict::with_seed(crate::dict::shard_seed(root, 0)),
            seq: 0,
            lossy: true,
            cut: true,
        },
        RecoveredShard {
            dict: imaged,
            seq: 1,
            lossy: false,
            cut: false,
        },
    ];
    let pool = ShardPool::spawn_spec(PoolSpec {
        shards: 2,
        executors: 1,
        seed: DictSeed { k0: 1, k1: 2 },
        trace: resumed.clone(),
        make_log: |_shard| crate::log::NoopLog,
        policy: crate::shard::Deadlines,
        limit: crate::memory::MemoryLimit::default(),
        clock: crate::shard::frozen_clock,
        recovered,
        make_checkpoint: |_executor| crate::log::checkpoint::NoCheckpoint,
        sync: crate::shard::SyncPolicy::INTERVAL,
        plants: crate::shard::ExecutorPlants::default(),
    });
    assert_eq!(
        pool.dispatch(Command::Get {
            key: Bytes::from_static(b"k")
        })
        .await,
        Reply::Bulk(Some(Bytes::from_static(b"v")))
    );
    let mut seen = resumed.0.lock().expect("resumed").clone();
    seen.sort_unstable();
    assert_eq!(seen, vec![(0, 0, true), (1, 1, false)]);
}

/// A shard whose recovery was cut writes a `Rebase` at its resume point
/// before anything else, so the records the cut left on disk are dead on
/// every later start; a shard recovered whole writes none.
#[tokio::test]
async fn a_shard_cut_by_its_recovery_rebases_before_its_first_write() {
    use crate::log::effect::Effect;
    use crate::log::recovery::RecoveredShard;
    use crate::shard::PoolSpec;

    /// `(shard, seq, payload)` of each append; a sync is `(u16::MAX, 0, "sync")`.
    type Appended = (u16, u64, Vec<u8>);
    #[derive(Clone, Default)]
    struct Kept(Arc<Mutex<Vec<Appended>>>);
    impl ReplicationLog for Kept {
        fn append(&mut self, rec: Record<'_>) -> std::io::Result<()> {
            self.0
                .lock()
                .expect("kept")
                .push((rec.shard, rec.seq, rec.payload.to_vec()));
            Ok(())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<Option<u64>> {
            self.0
                .lock()
                .expect("kept")
                .push((u16::MAX, 0, b"sync".to_vec()));
            Ok(None)
        }
    }

    let key = (0..64)
        .map(|i| format!("key{i}"))
        .find(|key| shard_of(key.as_bytes(), 2) == 0)
        .expect("some key lands on shard 0");
    // Each shard resumes at 1; shard 0's recovery cut records after it.
    let resumed = |shard: u16, cut: bool| RecoveredShard {
        dict: Dict::with_seed(crate::dict::shard_seed(DictSeed { k0: 1, k1: 2 }, shard)),
        seq: 1,
        lossy: false,
        cut,
    };
    let recovered = vec![resumed(0, true), resumed(1, false)];
    let kept = Kept::default();
    let pool = ShardPool::spawn_spec(PoolSpec {
        shards: 2,
        executors: 1,
        seed: DictSeed { k0: 1, k1: 2 },
        trace: super::support::Recorder::default(),
        make_log: {
            let kept = kept.clone();
            move |_shard| kept.clone()
        },
        policy: crate::shard::Deadlines,
        limit: crate::memory::MemoryLimit::default(),
        clock: crate::shard::frozen_clock,
        recovered,
        make_checkpoint: |_executor| crate::log::checkpoint::NoCheckpoint,
        sync: crate::shard::SyncPolicy::INTERVAL,
        plants: crate::shard::ExecutorPlants::default(),
    });
    let mut rebase = Vec::new();
    Effect::Rebase.encode(&mut rebase);
    let at_start = kept.0.lock().expect("kept").clone();
    assert_eq!(
        at_start,
        [(0, 1, rebase.clone()), (u16::MAX, 0, b"sync".to_vec())],
        "the rebase is synced before the pool serves anything: the state a client reads \
         after a cut must be the one the disk holds"
    );
    pool.dispatch(set(key.as_bytes(), b"v")).await;

    let seen: Vec<_> = kept
        .0
        .lock()
        .expect("kept")
        .iter()
        .filter(|(shard, _, _)| *shard != u16::MAX)
        .cloned()
        .collect();
    assert_eq!(
        seen.first(),
        Some(&(0, 1, rebase)),
        "the cut shard's first record is its rebase, at the resume point: {seen:?}"
    );
    assert_eq!(
        seen.get(1).map(|(shard, seq, _)| (*shard, *seq)),
        Some((0, 2))
    );
    assert!(
        seen.iter().all(|(shard, _, _)| *shard == 0),
        "the shard recovered whole writes no rebase: {seen:?}"
    );
}

/// A log that keeps nothing is handed no payload: the node without a log
/// encodes nothing on any write.
#[tokio::test]
async fn a_log_that_keeps_no_payloads_is_handed_none() {
    #[derive(Clone, Default)]
    struct Lengths(Arc<Mutex<Vec<usize>>>);
    impl ReplicationLog for Lengths {
        fn append(&mut self, rec: Record<'_>) -> std::io::Result<()> {
            self.0.lock().expect("lengths").push(rec.payload.len());
            Ok(())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<Option<u64>> {
            Ok(None)
        }
        fn keeps_payloads(&self) -> bool {
            false
        }
    }

    let lengths = Lengths::default();
    let pool = ShardPool::spawn_with_log(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace, {
        let lengths = lengths.clone();
        move |_shard| lengths.clone()
    });
    pool.dispatch(set(b"k", &[b'v'; 4096])).await;
    assert_eq!(*lengths.0.lock().expect("lengths"), [0]);
}

/// The checkpoint runs in the tick, after the log's two passes, once per
/// executor — it sees the executor's shards and the tick's clock.
#[tokio::test(start_paused = true)]
async fn the_checkpoint_is_ticked_once_per_executor_per_housekeeping_tick() {
    use crate::log::NoopLog;
    use crate::log::checkpoint::Checkpoint;
    use crate::shard::executor::ShardState;
    use crate::shard::{Now, PoolSpec, TraceSink};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Clone)]
    struct Counting {
        ticks: Arc<AtomicU64>,
        shards_seen: Arc<AtomicU64>,
    }
    impl Checkpoint for Counting {
        fn tick<L: ReplicationLog, T: TraceSink>(
            &mut self,
            _first_shard: u16,
            states: &mut [ShardState<L>],
            _now: Now,
            _trace: &T,
        ) {
            self.ticks.fetch_add(1, Ordering::SeqCst);
            self.shards_seen
                .fetch_max(states.len() as u64, Ordering::SeqCst);
        }
    }

    let ticks = Arc::new(AtomicU64::new(0));
    let shards_seen = Arc::new(AtomicU64::new(0));
    let counting = Counting {
        ticks: Arc::clone(&ticks),
        shards_seen: Arc::clone(&shards_seen),
    };
    let pool = ShardPool::spawn_spec(PoolSpec {
        shards: 8,
        executors: 2,
        seed: DictSeed { k0: 1, k1: 2 },
        trace: NoTrace,
        make_log: |_shard| NoopLog,
        policy: crate::shard::Deadlines,
        limit: crate::memory::MemoryLimit::default(),
        clock: crate::shard::frozen_clock,
        recovered: Vec::new(),
        make_checkpoint: move |_executor| counting.clone(),
        sync: crate::shard::SyncPolicy::INTERVAL,
        plants: crate::shard::ExecutorPlants::default(),
    });
    // Let both executors start their interval, then step the clock one
    // period at a time: a single jump of three periods would fire one tick,
    // the interval delaying rather than bursting what it missed.
    tokio::task::yield_now().await;
    for _ in 0..3 {
        tokio::time::advance(HOUSEKEEPING_TICK).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(
        ticks.load(Ordering::SeqCst),
        6,
        "three ticks on each of two executors"
    );
    assert_eq!(
        shards_seen.load(Ordering::SeqCst),
        4,
        "each sees its own four shards"
    );
    drop(pool);
}

/// Recovery seeds the due list with image keys whose deadline had passed:
/// they are removed after the tail, unless the tail moved them.
#[tokio::test(start_paused = true)]
async fn replay_into_removes_a_seeded_due_key_unless_the_tail_moved_it() {
    let now = Now {
        instant: Instant::now(),
        unix_millis: 1_000_000,
    };
    let mut dict = Dict::with_seed(DictSeed { k0: 1, k1: 2 });
    for key in [&b"stale"[..], b"moved"] {
        dict.insert(
            Bytes::copy_from_slice(key),
            Entry {
                value: Bytes::from_static(b"v"),
                expires_at: Some(now.instant),
                touched: 0,
            },
        );
    }
    let mut seq = 2;
    replay_into(
        &mut dict,
        &mut seq,
        vec![(
            2,
            Owned::Deadline {
                key: Bytes::from_static(b"moved"),
                deadline: Some(2_000_000),
            },
        )],
        now,
        vec![Bytes::from_static(b"stale"), Bytes::from_static(b"moved")],
    );
    assert_eq!(seq, 3);
    assert!(
        dict.get(b"stale").is_none(),
        "its last word was a passed deadline"
    );
    assert!(dict.get(b"moved").is_some(), "the tail moved it");
}
