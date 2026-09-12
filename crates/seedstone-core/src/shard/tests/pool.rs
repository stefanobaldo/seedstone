//! The pool as the router: where a key lands, the order a batch is answered
//! in, what a broadcast and a scan step do, and the shapes that are a
//! programming error.

use super::support::{gathered, get, set};
use crate::dict::{Dict, DictSeed, Entry};
use crate::shard::apply::scan_step;
use crate::shard::{Command, Deadlines, NoTrace, Reply, Route, Router, ShardPool};
use crate::slot::{executor_of, shard_of};
use tokio::time::Instant;

/// The counters `INFO` reports, gathered the way `INFO` gathers them.
///
/// Two things are pinned here that nothing else pins. The reads are
/// classified as Redis classifies them — `GET`, `EXISTS`, `TTL`, `TYPE`
/// and `STRLEN` count, a write does not, and `STRLEN` of a stored empty
/// value is a hit rather than the miss its `0` would suggest. And a
/// command that reaches every shard is counted **once by the edge and not
/// at all here**: a `DBSIZE` that landed in `calls` would be reported as
/// one call per shard.
#[tokio::test]
async fn a_shard_counts_the_lookups_and_the_calls_info_reports() {
    let pool = ShardPool::spawn(2, 1, DictSeed { k0: 1, k1: 2 }, NoTrace);
    assert_eq!(pool.dispatch(set(b"present", b"v")).await, Reply::Ok);
    assert_eq!(pool.dispatch(set(b"empty", b"")).await, Reply::Ok);

    // Two hits and two misses from the two commands that answer plainly.
    pool.dispatch(get(b"present")).await;
    pool.dispatch(get(b"absent")).await;
    pool.dispatch(Command::Exists {
        key: b"present".to_vec(),
    })
    .await;
    pool.dispatch(Command::Exists {
        key: b"absent".to_vec(),
    })
    .await;
    // And one of each from the three that do not.
    pool.dispatch(Command::Ttl {
        key: b"present".to_vec(),
    })
    .await;
    pool.dispatch(Command::Ttl {
        key: b"absent".to_vec(),
    })
    .await;
    pool.dispatch(Command::Type {
        key: b"present".to_vec(),
    })
    .await;
    pool.dispatch(Command::Type {
        key: b"absent".to_vec(),
    })
    .await;
    pool.dispatch(Command::StrLen {
        key: b"empty".to_vec(),
    })
    .await;
    pool.dispatch(Command::StrLen {
        key: b"absent".to_vec(),
    })
    .await;
    // A broadcast, which no shard may count.
    pool.dispatch_every(Command::DbSize).await;

    let stats = gathered(&pool).await;
    assert_eq!(stats.hits, 5, "one hit from each of the five read forms");
    assert_eq!(stats.misses, 5, "and one miss from each");
    assert_eq!(stats.keys, 2);
    assert_eq!(stats.expires, 0);
    assert_eq!(
        stats.calls[usize::from(Command::Get { key: Vec::new() }.kind())],
        2
    );
    assert_eq!(
        stats.calls[usize::from(Command::DbSize.kind())],
        0,
        "a request the edge split across every shard is the edge's to count"
    );
    assert_eq!(
        stats.calls[usize::from(Command::Stats.kind())],
        0,
        "and so is the gathering this very assertion did"
    );
}

#[tokio::test]
async fn keys_spread_over_shards_and_every_one_survives_growth() {
    // 16 shards, enough keys that several dicts outgrow their initial
    // eight buckets and rehash while the writes keep coming.
    let pool = ShardPool::spawn(16, 4, DictSeed { k0: 11, k1: 13 }, NoTrace);
    let keys: Vec<Vec<u8>> = (0..600u32)
        .map(|i| format!("key:{i}").into_bytes())
        .collect();

    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            pool.dispatch(set(key, i.to_string().as_bytes())).await,
            Reply::Ok
        );
    }
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            pool.dispatch(Command::Get { key: key.clone() }).await,
            Reply::Bulk(Some(i.to_string().into_bytes())),
            "key {key:?} lost across a rehash"
        );
    }

    // And they really did land on more than one shard, or the test above
    // proves nothing about routing.
    let mut shards: Vec<u16> = keys.iter().map(|k| shard_of(k, 16)).collect();
    shards.sort_unstable();
    shards.dedup();
    assert!(shards.len() > 1, "every key routed to one shard");
}

#[tokio::test]
async fn a_batch_is_answered_in_request_order_across_executors() {
    let pool = ShardPool::spawn(16, 4, DictSeed { k0: 9, k1: 9 }, NoTrace);
    // Keys chosen to land on more than one executor, interleaved on purpose.
    let keys: Vec<Vec<u8>> = (0..64u32)
        .map(|i| format!("key:{i}").into_bytes())
        .collect();
    let executors_hit: std::collections::BTreeSet<u16> = keys
        .iter()
        .map(|k| executor_of(shard_of(k, 16), 16, 4))
        .collect();
    assert!(
        executors_hit.len() > 1,
        "test keys all landed on one executor"
    );

    let sets: Vec<Command> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| set(key, i.to_string().as_bytes()))
        .collect();
    let replies = pool.dispatch_many(sets).await;
    assert!(replies.iter().all(|r| *r == Reply::Ok));

    let gets: Vec<Command> = keys
        .iter()
        .map(|key| Command::Get { key: key.clone() })
        .collect();
    let replies = pool.dispatch_many(gets).await;
    for (i, reply) in replies.iter().enumerate() {
        assert_eq!(
            *reply,
            Reply::Bulk(Some(i.to_string().into_bytes())),
            "reply {i} out of order or wrong"
        );
    }
}

/// `count` distinct keys that hash to `shard`, found by search — the seed
/// is fixed, so the search is deterministic and the test does not depend
/// on which keys it finds.
///
/// The search is bounded rather than open-ended: an unbounded one would
/// hang forever on the day `shard_of` stopped reaching some shard, which
/// is precisely the bug a caller is using this to rule out.
fn keys_landing_on(shard: u16, shards: u16, count: usize) -> Vec<Vec<u8>> {
    let keys: Vec<Vec<u8>> = (0u32..10_000)
        .map(|n| format!("probe-{n}").into_bytes())
        .filter(|k| shard_of(k, shards) == shard)
        .take(count)
        .collect();
    assert_eq!(
        keys.len(),
        count,
        "ten thousand probes reach every shard of a small pool {count} times"
    );
    keys
}

/// Shard `i` is given `i + 1` keys, so every shard's `DBSIZE` differs from
/// every other's and the replies can only be compared in one order.
///
/// A broadcast that answered a key each would pass any permutation: the
/// order is claimed by [`Route::Every`] and by
/// [`Router::dispatch_every`], and a claim no test can fail is not held.
#[tokio::test]
async fn a_broadcast_is_answered_once_per_shard_in_shard_order() {
    let pool = ShardPool::spawn(8, 4, DictSeed { k0: 1, k1: 1 }, NoTrace);
    for i in 0..8u16 {
        for key in keys_landing_on(i, 8, usize::from(i) + 1) {
            pool.dispatch(Command::Set {
                key,
                value: b"v".to_vec(),
                expiry: None,
                cond: None,
                keep_ttl: false,
                get: false,
            })
            .await;
        }
    }
    let replies = pool.dispatch_every(Command::DbSize).await;
    let expected: Vec<Reply> = (1..=8).map(Reply::Integer).collect();
    assert_eq!(
        replies, expected,
        "a broadcast answered out of shard order, or not once per shard"
    );
}

/// The budget the edge accounts a crossing call by: how many buckets one
/// step actually walked, which is not the count it was asked for whenever
/// the step finished the cycle first.
#[test]
fn a_scan_step_reports_how_many_buckets_it_visited() {
    let mut dict = Dict::with_seed(DictSeed { k0: 1, k1: 1 });
    // Four keys in a table of eight buckets: the load factor is one, so
    // nothing has grown and the cycle is exactly eight steps long.
    for i in 0..4u8 {
        dict.insert(
            vec![i],
            Entry {
                value: Vec::new(),
                expires_at: None,
                touched: 0,
            },
        );
    }
    let now = Instant::now();

    let Reply::Scan {
        visited, cursor, ..
    } = scan_step(&dict, 0, 3, None, now, &Deadlines)
    else {
        panic!("a scan step must answer Reply::Scan");
    };
    assert_eq!(visited, 3);
    assert_ne!(cursor, 0);

    let Reply::Scan {
        visited, cursor, ..
    } = scan_step(&dict, 0, 100, None, now, &Deadlines)
    else {
        panic!("a scan step must answer Reply::Scan");
    };
    assert_eq!(
        visited, 8,
        "a step that finishes the cycle visited the whole table, not its budget"
    );
    assert_eq!(cursor, 0);
}

#[tokio::test]
async fn a_scan_step_returns_at_most_a_countful_and_a_resumable_cursor() {
    let pool = ShardPool::spawn(1, 1, DictSeed { k0: 1, k1: 1 }, NoTrace);
    for i in 0..50u32 {
        pool.dispatch(set(format!("k{i}").as_bytes(), b"v")).await;
    }

    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut cursor = 0u64;
    let mut calls = 0;
    loop {
        let reply = pool
            .dispatch_at(
                0,
                Command::ScanStep {
                    cursor,
                    count: 10,
                    pattern: None,
                },
            )
            .await;
        let Reply::Scan {
            cursor: next, keys, ..
        } = reply
        else {
            panic!("a scan step must answer Reply::Scan, got {reply:?}");
        };
        seen.extend(keys);
        cursor = next;
        calls += 1;
        // A guard rather than an acceptance criterion — the criteria are
        // the two assertions below. Without it, a cursor that never closes
        // its cycle would wedge the suite instead of reporting itself.
        assert!(calls < 200, "a 50-key scan did not terminate");
        if cursor == 0 {
            break;
        }
    }
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 50, "every key must be seen at least once");
    assert!(
        calls > 1,
        "COUNT 10 over 50 keys must take more than one call"
    );
}

#[tokio::test]
async fn a_scan_step_filters_by_pattern_inside_the_shard() {
    let pool = ShardPool::spawn(1, 1, DictSeed { k0: 1, k1: 1 }, NoTrace);
    for name in ["alpha", "album", "beta"] {
        pool.dispatch(set(name.as_bytes(), b"v")).await;
    }
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut cursor = 0u64;
    loop {
        let Reply::Scan {
            cursor: next, keys, ..
        } = pool
            .dispatch_at(
                0,
                Command::ScanStep {
                    cursor,
                    count: 100,
                    pattern: Some(b"al*".to_vec()),
                },
            )
            .await
        else {
            panic!("expected Reply::Scan");
        };
        seen.extend(keys);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    seen.sort();
    assert_eq!(seen, vec![b"album".to_vec(), b"alpha".to_vec()]);
}

#[tokio::test]
async fn an_empty_batch_answers_immediately_with_nothing() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 1 }, NoTrace);
    assert_eq!(pool.dispatch_many(Vec::new()).await, Vec::new());
}

/// The default implementation is the compatibility contract: a router that
/// only knows `dispatch` must serve batches, one command at a time, in order.
#[tokio::test]
async fn the_default_dispatch_many_loops_dispatch_in_order() {
    #[derive(Clone)]
    struct Echo;
    impl Router for Echo {
        async fn dispatch(&self, cmd: Command) -> Reply {
            match cmd.route() {
                Route::Key(key) => Reply::Bulk(Some(key.to_vec())),
                Route::Shard(_) | Route::Every | Route::Unaddressed => Reply::Ok,
            }
        }
        /// This router hosts no shards and says so: `0..0` is a range,
        /// and it is empty, which is the honest answer for something that
        /// answers every command out of thin air.
        fn shards(&self) -> u16 {
            0
        }
        /// This router hosts no shards, so there is none to address.
        async fn dispatch_at(&self, _shard: u16, cmd: Command) -> Reply {
            self.dispatch(cmd).await
        }
        /// This router hosts no shards, so a broadcast reaches nothing.
        async fn dispatch_every(&self, _cmd: Command) -> Vec<Reply> {
            Vec::new()
        }
    }
    let replies = Echo
        .dispatch_many(vec![
            Command::Get { key: b"a".to_vec() },
            Command::Get { key: b"b".to_vec() },
        ])
        .await;
    assert_eq!(
        replies,
        vec![
            Reply::Bulk(Some(b"a".to_vec())),
            Reply::Bulk(Some(b"b".to_vec()))
        ]
    );
}

#[tokio::test]
#[should_panic(expected = "shards must be greater than zero")]
async fn a_pool_of_no_shards_is_a_programming_error() {
    ShardPool::spawn(0, 1, DictSeed { k0: 0, k1: 0 }, NoTrace);
}

#[tokio::test]
#[should_panic(expected = "executors must be in 1..=shards")]
async fn a_pool_of_no_executors_is_a_programming_error() {
    ShardPool::spawn(4, 0, DictSeed { k0: 0, k1: 0 }, NoTrace);
}

/// More executors than shards would leave one owning nothing, which the
/// partition function has no way to express and no caller has a use for.
#[tokio::test]
#[should_panic(expected = "executors must be in 1..=shards")]
async fn a_pool_with_more_executors_than_shards_is_a_programming_error() {
    ShardPool::spawn(4, 5, DictSeed { k0: 0, k1: 0 }, NoTrace);
}
