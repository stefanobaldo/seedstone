//! Deadlines: what a key with one answers before and after it passes, what
//! the lazy path removes, and what the housekeeping sweep reclaims.

use super::support::{NoSweep, Recorder, Shard, gathered, get, set, set_ex, setex};
use crate::dict::{Dict, DictSeed};
use crate::log::{Record, ReplicationLog};
use crate::shard::apply::apply;
use crate::shard::executor::ShardState;
use crate::shard::{
    Command, Deadlines, HOUSEKEEPING_TICK, NoTrace, Reply, ReplyError, Router, ShardPool,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test(start_paused = true)]
async fn set_with_ex_expires_lazily() {
    let mut shard = Shard::for_tests();
    assert_eq!(shard.run(set_ex(b"k", b"v", 30), Instant::now()), Reply::Ok);

    tokio::time::advance(Duration::from_secs(29)).await;
    assert_eq!(
        shard.run(get(b"k"), Instant::now()),
        Reply::Bulk(Some(b"v".to_vec())),
        "a key one second short of its deadline is still a key"
    );

    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(shard.run(get(b"k"), Instant::now()), Reply::Bulk(None));
    // The read is what removed it. A deadline that only hid the entry would
    // leave the keyspace growing with values nothing can ever reach again.
    assert_eq!(
        shard.dict.len(),
        0,
        "the expired entry survived the read that reported it gone"
    );
}

/// `SETEX` is `SET … EX` under its old name: the value is stored, the
/// deadline is `seconds` out, and the key dies on the read that finds it
/// due — the same lazy expiry `set_with_ex_expires_lazily` pins for `SET`.
#[tokio::test(start_paused = true)]
async fn setex_writes_the_value_and_the_deadline() {
    let mut shard = Shard::for_tests();
    assert_eq!(shard.run(setex(b"k", 30, b"v"), Instant::now()), Reply::Ok);

    tokio::time::advance(Duration::from_secs(29)).await;
    assert_eq!(
        shard.run(get(b"k"), Instant::now()),
        Reply::Bulk(Some(b"v".to_vec())),
        "a key one second short of its deadline is still a key"
    );

    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(shard.run(get(b"k"), Instant::now()), Reply::Bulk(None));
    assert_eq!(shard.dict.len(), 0, "the expired entry survived the read");
}

fn pttl(key: &[u8]) -> Command {
    Command::PTtl { key: key.to_vec() }
}

/// `PTTL` is `TTL` in milliseconds: `-2` for a key that is not there,
/// `-1` for one with no deadline, otherwise the span left — not rounded
/// to a second, which is what separates it from `TTL`. Read on 6.2.24 and
/// 8.10.1: `SET k v PX 100000` then `PTTL k` answers a value in
/// `(0, 100000]` where `TTL k` answers `100`.
#[tokio::test(start_paused = true)]
async fn pttl_answers_in_milliseconds() {
    let mut shard = Shard::for_tests();
    assert_eq!(shard.run(pttl(b"k"), Instant::now()), Reply::Integer(-2));
    assert_eq!(shard.run(set(b"k", b"v"), Instant::now()), Reply::Ok);
    assert_eq!(shard.run(pttl(b"k"), Instant::now()), Reply::Integer(-1));
    assert_eq!(
        shard.run(
            Command::PExpire {
                key: b"k".to_vec(),
                millis: 1500
            },
            Instant::now()
        ),
        Reply::Integer(1)
    );
    tokio::time::advance(Duration::from_millis(400)).await;
    assert_eq!(shard.run(pttl(b"k"), Instant::now()), Reply::Integer(1100));
    assert_eq!(
        shard.run(Command::Ttl { key: b"k".to_vec() }, Instant::now()),
        Reply::Integer(1),
        "TTL rounds the same deadline to the nearest second"
    );
}

/// `EXPIREAT`/`PEXPIREAT` reach the shard as the span left until the
/// deadline the edge resolved, so the shard's part is `PEXPIRE`'s: a
/// positive span is a deadline, a span that is not positive deletes the
/// key and is reported as an applied expiry — `1` — which is what Redis
/// answers `EXPIREAT k 1` with (6.2.24, 8.10.1), the key gone at once.
#[tokio::test(start_paused = true)]
async fn expireat_is_pexpire_on_a_span_the_edge_resolved() {
    let mut shard = Shard::for_tests();
    assert_eq!(shard.run(set(b"k", b"v"), Instant::now()), Reply::Ok);
    assert_eq!(
        shard.run(
            Command::ExpireAt {
                key: b"k".to_vec(),
                millis: 30_000
            },
            Instant::now()
        ),
        Reply::Integer(1)
    );
    assert_eq!(
        shard.run(pttl(b"k"), Instant::now()),
        Reply::Integer(30_000)
    );
    assert_eq!(
        shard.run(
            Command::PExpireAt {
                key: b"k".to_vec(),
                millis: 0
            },
            Instant::now()
        ),
        Reply::Integer(1),
        "a deadline already passed is a deletion, reported as applied"
    );
    assert_eq!(shard.run(get(b"k"), Instant::now()), Reply::Bulk(None));
    assert_eq!(shard.dict.len(), 0);
    assert_eq!(
        shard.run(
            Command::ExpireAt {
                key: b"missing".to_vec(),
                millis: 30_000
            },
            Instant::now()
        ),
        Reply::Integer(0),
        "nothing to expire"
    );
}

/// An existing key is overwritten whole, value and deadline: a key that
/// had no deadline acquires one. Measured against `redis:6-alpine`
/// (6.2.24) and `redis:8-alpine` (8.10.1): `SET pre x` then
/// `SETEX pre 50 replaced` gives `GET pre` → `replaced`, `TTL pre` → `50`.
#[tokio::test(start_paused = true)]
async fn setex_overwrites_value_and_deadline_alike() {
    let mut shard = Shard::for_tests();
    assert_eq!(shard.run(set(b"pre", b"x"), Instant::now()), Reply::Ok);
    assert_eq!(
        shard.run(setex(b"pre", 50, b"replaced"), Instant::now()),
        Reply::Ok
    );
    assert_eq!(
        shard.run(get(b"pre"), Instant::now()),
        Reply::Bulk(Some(b"replaced".to_vec()))
    );
    assert_eq!(
        shard.run(
            Command::Ttl {
                key: b"pre".to_vec()
            },
            Instant::now()
        ),
        Reply::Integer(50)
    );
}

#[tokio::test(start_paused = true)]
async fn expire_and_ttl() {
    let mut shard = Shard::for_tests();
    let ttl = |key: &[u8]| Command::Ttl { key: key.to_vec() };
    let expire = |key: &[u8], seconds: i64| Command::Expire {
        key: key.to_vec(),
        seconds,
    };

    assert_eq!(
        shard.run(ttl(b"k"), Instant::now()),
        Reply::Integer(-2),
        "TTL of a key that does not exist"
    );
    assert_eq!(
        shard.run(expire(b"k", 10), Instant::now()),
        Reply::Integer(0),
        "EXPIRE of a key that does not exist"
    );

    assert_eq!(shard.run(set(b"k", b"v"), Instant::now()), Reply::Ok);
    assert_eq!(
        shard.run(ttl(b"k"), Instant::now()),
        Reply::Integer(-1),
        "TTL of a key with no deadline"
    );

    assert_eq!(
        shard.run(expire(b"k", 100), Instant::now()),
        Reply::Integer(1)
    );
    assert_eq!(shard.run(ttl(b"k"), Instant::now()), Reply::Integer(100));
    // Rounded to nearest, byte-for-byte what Redis replies. Half a second
    // gone still reads 100 under either rounding rule, so it is the next
    // assertion and not this one that pins which rule is in force.
    tokio::time::advance(Duration::from_millis(500)).await;
    assert_eq!(shard.run(ttl(b"k"), Instant::now()), Reply::Integer(100));
    // 99.4 seconds left: Redis says 99, and rounding up would say 100.
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(
        shard.run(ttl(b"k"), Instant::now()),
        Reply::Integer(99),
        "TTL must round to nearest, as Redis does, not up"
    );
    // And the other side of the same rule: under half a second left reads
    // as 0 while the key is still very much alive.
    tokio::time::advance(Duration::from_millis(99_100)).await;
    assert_eq!(shard.run(ttl(b"k"), Instant::now()), Reply::Integer(0));
    assert_eq!(
        shard.run(get(b"k"), Instant::now()),
        Reply::Bulk(Some(b"v".to_vec())),
        "a key reading TTL 0 is still alive"
    );

    // A non-positive expiry deletes the key, and still reports that the
    // deadline was applied rather than that nothing was there.
    assert_eq!(
        shard.run(expire(b"k", 0), Instant::now()),
        Reply::Integer(1)
    );
    assert_eq!(shard.run(get(b"k"), Instant::now()), Reply::Bulk(None));
    assert_eq!(shard.dict.len(), 0);
}

#[tokio::test(start_paused = true)]
async fn expired_keys_are_dead_to_every_command() {
    let mut shard = Shard::for_tests();
    // One key per command, all past the same deadline: every arm meets an
    // entry that is still in the dict and already gone, which is the state
    // a handler that skipped the liveness check would answer from.
    for key in [
        &b"get"[..],
        b"exists",
        b"ttl",
        b"persist",
        b"pexpire",
        b"del",
        b"incrby",
        b"type",
        b"strlen",
    ] {
        assert_eq!(shard.run(set_ex(key, b"1", 10), Instant::now()), Reply::Ok);
    }
    tokio::time::advance(Duration::from_secs(11)).await;
    let now = Instant::now();

    assert_eq!(shard.run(get(b"get"), now), Reply::Bulk(None));
    assert_eq!(
        shard.run(
            Command::Exists {
                key: b"exists".to_vec()
            },
            now
        ),
        Reply::Integer(0)
    );
    assert_eq!(
        shard.run(
            Command::Ttl {
                key: b"ttl".to_vec()
            },
            now
        ),
        Reply::Integer(-2)
    );
    // Both would answer `1` against an entry that was still there: a
    // `PERSIST` would find the stale deadline to clear, and a `PEXPIRE` a
    // key to put a new one on. `0` is the answer only an eviction that has
    // already happened can produce.
    assert_eq!(
        shard.run(
            Command::Persist {
                key: b"persist".to_vec()
            },
            now
        ),
        Reply::Integer(0)
    );
    assert_eq!(
        shard.run(
            Command::PExpire {
                key: b"pexpire".to_vec(),
                millis: 10_000
            },
            now
        ),
        Reply::Integer(0)
    );
    assert_eq!(
        shard.run(
            Command::Del {
                key: b"del".to_vec()
            },
            now
        ),
        Reply::Removed(false)
    );
    assert_eq!(
        shard.run(
            Command::IncrBy {
                key: b"incrby".to_vec(),
                delta: 7
            },
            now
        ),
        Reply::Integer(7),
        "an expired counter must start from zero"
    );
    // The two that describe an entry rather than read it. Against a key
    // still in the dict they would report `string` and the length of the
    // value that outlived its deadline; `none` and `0` are the answers
    // only an eviction that has already happened can produce.
    assert_eq!(
        shard.run(
            Command::Type {
                key: b"type".to_vec()
            },
            now
        ),
        Reply::Status("none")
    );
    assert_eq!(
        shard.run(
            Command::StrLen {
                key: b"strlen".to_vec()
            },
            now
        ),
        Reply::Integer(0)
    );

    // Each was removed by the command that met it, not merely hidden from
    // it: what is left is the counter INCRBY re-created.
    assert_eq!(shard.dict.len(), 1);
}

/// An expiry is a deletion, and a deletion is a logged mutation.
///
/// The record is what a later replay reads; the replication position it
/// consumes is what the trace sink folds. So an expiration is visible to
/// both without either having to know what a deadline is.
#[tokio::test(start_paused = true)]
async fn an_expiry_is_logged_exactly_as_a_delete_is() {
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
    let mut state = ShardState::new(Dict::with_seed(DictSeed { k0: 5, k1: 7 }), log.clone());

    let mut set = set_ex(b"k", b"v", 30);
    assert_eq!(
        apply(&mut state, 3, &mut set, Instant::now(), &Deadlines),
        Reply::Ok
    );
    tokio::time::advance(Duration::from_secs(31)).await;

    // A read appends nothing of its own, so the second record below is the
    // expiry's and nothing else.
    let mut read = get(b"k");
    assert_eq!(
        apply(&mut state, 3, &mut read, Instant::now(), &Deadlines),
        Reply::Bulk(None)
    );
    assert_eq!(*log.0.lock().expect("log mutex"), vec![(3, 0), (3, 1)]);
    assert_eq!(
        state.seq, 2,
        "the expiry did not consume a replication position"
    );
    assert_eq!(state.expired, 1, "the lazy half counted the expiration");
}

/// `PEXPIRE` and `PERSIST` reach the log exactly when they change
/// something.
///
/// The replication position is what makes that observable: [`append`]
/// advances `seq` only when a record was written, so a `seq` that stood
/// still is a mutation that never happened. Both commands answer `0` for
/// the cases where there is nothing to change, and answering it *before*
/// the append is what these assertions pin — an implementation that logged
/// first would give the same `0` on the wire and leave behind a record that
/// replays to nothing.
#[tokio::test(start_paused = true)]
async fn pexpire_and_persist_reach_the_log_only_when_they_change_something() {
    let mut shard = Shard::for_tests();
    let ttl = |key: &[u8]| Command::Ttl { key: key.to_vec() };
    let persist = |key: &[u8]| Command::Persist { key: key.to_vec() };
    let pexpire = |key: &[u8], millis: i64| Command::PExpire {
        key: key.to_vec(),
        millis,
    };

    assert_eq!(
        shard.run(pexpire(b"k", 1000), Instant::now()),
        Reply::Integer(0),
        "PEXPIRE of a key that does not exist"
    );
    assert_eq!(
        shard.run(persist(b"k"), Instant::now()),
        Reply::Integer(0),
        "PERSIST of a key that does not exist"
    );
    assert_eq!(shard.seq, 0, "neither wrote a record");

    assert_eq!(shard.run(set(b"k", b"v"), Instant::now()), Reply::Ok);
    assert_eq!(shard.seq, 1, "the write that created it did");

    assert_eq!(
        shard.run(persist(b"k"), Instant::now()),
        Reply::Integer(0),
        "PERSIST of a key that is there but carries no deadline"
    );
    assert_eq!(shard.seq, 1, "and it wrote no record either");

    assert_eq!(
        shard.run(pexpire(b"k", 30_000), Instant::now()),
        Reply::Integer(1)
    );
    assert_eq!(shard.seq, 2);
    assert_eq!(
        shard.run(ttl(b"k"), Instant::now()),
        Reply::Integer(30),
        "thirty thousand milliseconds is thirty seconds"
    );

    assert_eq!(shard.run(persist(b"k"), Instant::now()), Reply::Integer(1));
    assert_eq!(shard.seq, 3);
    assert_eq!(
        shard.run(ttl(b"k"), Instant::now()),
        Reply::Integer(-1),
        "the key outlived the deadline it was carrying"
    );

    // A span that is not in the future is a deletion, answered as an
    // expiry that was applied.
    assert_eq!(
        shard.run(pexpire(b"k", 0), Instant::now()),
        Reply::Integer(1)
    );
    assert_eq!(shard.seq, 4);
    assert_eq!(
        shard.run(ttl(b"k"), Instant::now()),
        Reply::Integer(-2),
        "the key is gone, not merely left without a deadline"
    );
}

/// The sink's side of the same fact: a position disappears from the trace
/// where an expiry took one, so a run in which a key expired cannot hash
/// like a run in which it did not.
///
/// The pool is spawned with a policy that never sweeps, so the only thing
/// that can remove this key is the command that meets it — which is the
/// path these assertions are about. The tick is deliberately fired several
/// times in between: under the honest policy that firing would reclaim the
/// key and the trace would carry the sweep's own removal instead of the
/// gap, so the loop is what proves the dependence is gone rather than
/// merely unobserved.
///
/// The read and the write still go in one batch. That is now a matter of
/// clarity alone — the policy, not the batching, is what keeps the sweep
/// out of these positions.
#[tokio::test(start_paused = true)]
async fn the_sink_sees_the_position_an_expiry_consumed() {
    let sink = Recorder::default();
    let pool = ShardPool::spawn_with_policy(1, 1, DictSeed { k0: 2, k1: 3 }, sink.clone(), NoSweep);

    pool.dispatch(set_ex(b"k", b"v", 1)).await;
    tokio::time::advance(Duration::from_secs(2)).await;
    for _ in 0..4 {
        tokio::time::advance(HOUSEKEEPING_TICK).await;
        tokio::task::yield_now().await;
    }
    pool.dispatch_many(vec![get(b"k"), set(b"k", b"again")])
        .await;

    let seen = sink.0.lock().expect("recorder mutex").clone();
    assert_eq!(
        seen,
        vec![
            (0, 0, 2, Reply::Ok),
            // The read ran at position 1 and evicted the key there.
            (0, 1, 1, Reply::Bulk(None)),
            // So the next write is at 2, not at 1: the gap is the expiry.
            (0, 2, 2, Reply::Ok),
        ]
    );
}

/// The half of expiration lazy eviction cannot do.
///
/// Every key here is written once and never addressed again, so no command
/// ever meets one: under lazy expiry alone the thousand entries would sit
/// in the dict for the life of the process, and the only thing that can
/// reclaim them is the shard's own tick. The evidence is the trace — a
/// `Del` per key, at a replication position of its own — because it is
/// produced without anything touching the keyspace, which is exactly the
/// claim.
#[tokio::test(start_paused = true)]
async fn the_sweep_reclaims_untouched_expired_keys() {
    const KEYS: u64 = 1_000;
    let keys = usize::try_from(KEYS).expect("a thousand keys is a usize");

    let sink = Recorder::default();
    // One shard, so the positions below are one sequence rather than an
    // interleaving, and every key's sweep is driven by one cursor.
    let pool = ShardPool::spawn(1, 1, DictSeed { k0: 2, k1: 3 }, sink.clone());
    for i in 0..KEYS {
        assert_eq!(
            pool.dispatch(set_ex(format!("k{i}").as_bytes(), b"v", 1))
                .await,
            Reply::Ok
        );
    }

    // Past every deadline, with nothing having read a single key.
    tokio::time::advance(Duration::from_secs(2)).await;

    let removals = |sink: &Recorder| {
        sink.0
            .lock()
            .expect("recorder mutex")
            .iter()
            .filter(|(_, _, kind, reply)| *kind == 3 && *reply == Reply::Removed(true))
            .count()
    };

    // Drive the housekeeping tick until the cursor has been round the
    // table. The bound is a guard rather than an acceptance criterion: a
    // sweep that never reclaims anything must fail the test rather than
    // hang it.
    let mut ticks = 0;
    while removals(&sink) < keys {
        tokio::time::advance(HOUSEKEEPING_TICK).await;
        ticks += 1;
        assert!(
            ticks < 64,
            "after {ticks} ticks the sweep had reclaimed {} of {KEYS} keys",
            removals(&sink)
        );
    }

    // Exactly one removal per key: a sweep that visited a bucket twice in a
    // cycle, or that reported a key it had already removed, would overshoot.
    assert_eq!(removals(&sink), keys);
    let seen = sink.0.lock().expect("recorder mutex").clone();
    assert_eq!(
        seen.len(),
        2 * keys,
        "the trace holds something other than the thousand writes and their expiries"
    );
    // Each expiry consumed a replication position of its own, immediately
    // after the thousand writes: an expiry is a logged mutation, whoever
    // caused it.
    let positions: Vec<u64> = seen[keys..].iter().map(|(_, seq, _, _)| *seq).collect();
    assert_eq!(positions, (KEYS..2 * KEYS).collect::<Vec<u64>>());

    // And the keyspace really is empty: the read below runs at the position
    // the sweep left behind and consumes nothing, so it evicted nothing —
    // there was nothing left for it to evict.
    assert_eq!(
        pool.dispatch(get(b"k0")).await,
        Reply::Bulk(None),
        "a key the sweep reported gone answered a read"
    );
    let last = sink.0.lock().expect("recorder mutex").last().cloned();
    assert_eq!(last, Some((0, 2 * KEYS, 1, Reply::Bulk(None))));
}

/// A sweep's removal is a logged mutation, so a log that cannot take the
/// record does not get the removal either.
///
/// The ordering `apply` documents for a command holds for the tick as well:
/// the record is written before the entry goes, and a refused record leaves
/// the keyspace behind the log rather than ahead of it. The dead entry
/// keeps its deadline and the cursor keeps its place, so the same buckets
/// are swept again — which is what makes the failure a delay rather than a
/// leak.
#[tokio::test(start_paused = true)]
async fn a_sweep_whose_record_cannot_be_written_leaves_the_key() {
    #[derive(Clone, Default)]
    struct Breakable(Arc<Mutex<bool>>);

    impl ReplicationLog for Breakable {
        fn append(&mut self, _rec: Record<'_>) -> std::io::Result<()> {
            if *self.0.lock().expect("log mutex") {
                return Err(std::io::Error::other("the disk went away"));
            }
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let log = Breakable::default();
    let sink = Recorder::default();
    let pool = ShardPool::spawn_with_log(1, 1, DictSeed { k0: 2, k1: 3 }, sink.clone(), {
        let log = log.clone();
        move |_shard| log.clone()
    });
    assert_eq!(pool.dispatch(set_ex(b"k", b"v", 1)).await, Reply::Ok);

    *log.0.lock().expect("log mutex") = true;
    tokio::time::advance(Duration::from_secs(2)).await;
    for _ in 0..8 {
        tokio::time::advance(HOUSEKEEPING_TICK).await;
    }

    // The entry is still there. A read cannot say so directly — it would
    // report a live key and an evicted one identically — but it can say it
    // by failing: the lazy path meets the same expired entry and the same
    // broken log, and refuses for the same reason. A key the sweep had
    // removed would have answered `Bulk(None)` instead.
    assert_eq!(
        pool.dispatch(get(b"k")).await,
        Reply::Error(ReplyError::LogWriteFailed),
        "the sweep removed a key whose record could not be written"
    );

    // With the log back, the sweep reaches the same bucket again and
    // reclaims it — the buckets it abandoned were retried, not skipped.
    *log.0.lock().expect("log mutex") = false;
    let mut ticks = 0;
    while sink
        .0
        .lock()
        .expect("recorder mutex")
        .last()
        .map(|(_, _, kind, _)| *kind)
        != Some(3)
    {
        tokio::time::advance(HOUSEKEEPING_TICK).await;
        ticks += 1;
        assert!(ticks < 64, "the sweep never came back for the key");
    }
    assert_eq!(
        sink.0.lock().expect("recorder mutex").last().cloned(),
        Some((0, 1, 3, Reply::Removed(true))),
        "the expiry did not take the position after the write"
    );
}

/// Expirations are counted by both halves under the one field.
#[tokio::test(start_paused = true)]
async fn expired_keys_counts_the_lazy_half_and_the_sweep() {
    let pool = ShardPool::spawn(2, 1, DictSeed { k0: 1, k1: 2 }, NoTrace);
    assert_eq!(pool.dispatch(set_ex(b"lazy", b"v", 30)).await, Reply::Ok);
    assert_eq!(gathered(&pool).await.expires, 1, "one dated key");
    tokio::time::advance(Duration::from_secs(31)).await;
    // A read in front of the key is the lazy half.
    assert_eq!(pool.dispatch(get(b"lazy")).await, Reply::Bulk(None));
    let stats = gathered(&pool).await;
    assert_eq!(stats.expired, 1);
    assert_eq!(stats.expires, 0, "and the deadline went with the key");
}
