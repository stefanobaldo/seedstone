//! What each command answers: the `SET` algebra, the counters, and a handler
//! run on its own without a pool behind it.

use super::support::{Shard, get, set, set_ex};
use crate::dict::{Dict, DictSeed};
use crate::log::NoopLog;
use crate::shard::apply::apply;
use crate::shard::executor::ShardState;
use crate::shard::{
    Command, Cond, Deadlines, NoTrace, Reply, ReplyError, Route, Router, ShardPool,
};
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test(start_paused = true)]
async fn set_nx_xx_algebra() {
    let mut shard = Shard::for_tests();
    let conditional = |value: &[u8], cond: Cond| Command::Set {
        key: b"k".to_vec(),
        value: value.to_vec(),
        expiry: None,
        cond: Some(cond),
        keep_ttl: false,
        get: false,
    };

    // XX on an absent key stores nothing and says so.
    assert_eq!(
        shard.run(conditional(b"first", Cond::Xx), Instant::now()),
        Reply::Bulk(None)
    );
    assert_eq!(shard.run(get(b"k"), Instant::now()), Reply::Bulk(None));

    // NX on an absent key stores.
    assert_eq!(
        shard.run(conditional(b"first", Cond::Nx), Instant::now()),
        Reply::Ok
    );
    assert_eq!(
        shard.run(get(b"k"), Instant::now()),
        Reply::Bulk(Some(b"first".to_vec()))
    );

    // NX on a present key refuses, and leaves the value it found alone.
    assert_eq!(
        shard.run(conditional(b"second", Cond::Nx), Instant::now()),
        Reply::Bulk(None)
    );
    assert_eq!(
        shard.run(get(b"k"), Instant::now()),
        Reply::Bulk(Some(b"first".to_vec())),
        "a refused NX overwrote the value anyway"
    );

    // XX on a present key replaces it.
    assert_eq!(
        shard.run(conditional(b"second", Cond::Xx), Instant::now()),
        Reply::Ok
    );
    assert_eq!(
        shard.run(get(b"k"), Instant::now()),
        Reply::Bulk(Some(b"second".to_vec()))
    );

    // And a plain SET clears the deadline the key it overwrote carried —
    // Redis's semantics, and the reason a rewritten key is not silently
    // still on its predecessor's clock.
    assert_eq!(shard.run(set_ex(b"t", b"v", 30), Instant::now()), Reply::Ok);
    tokio::time::advance(Duration::from_secs(29)).await;
    assert_eq!(shard.run(set(b"t", b"w"), Instant::now()), Reply::Ok);
    tokio::time::advance(Duration::from_hours(1)).await;
    assert_eq!(
        shard.run(get(b"t"), Instant::now()),
        Reply::Bulk(Some(b"w".to_vec())),
        "a plain SET left the old deadline in place"
    );
}

#[tokio::test]
async fn commands_round_trip_through_the_pool() {
    let pool = ShardPool::spawn(16, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
    assert_eq!(pool.dispatch(set(b"k", b"v")).await, Reply::Ok);
    assert_eq!(
        pool.dispatch(Command::Get { key: b"k".to_vec() }).await,
        Reply::Bulk(Some(b"v".to_vec()))
    );
    assert_eq!(
        pool.dispatch(Command::IncrBy {
            key: b"n".to_vec(),
            delta: 5
        })
        .await,
        Reply::Integer(5)
    );
    assert_eq!(
        pool.dispatch(Command::IncrBy {
            key: b"n".to_vec(),
            delta: -2
        })
        .await,
        Reply::Integer(3)
    );
    assert_eq!(
        pool.dispatch(Command::IncrBy {
            key: b"k".to_vec(),
            delta: 1
        })
        .await,
        Reply::Error(ReplyError::NotAnInteger)
    );
    assert_eq!(
        pool.dispatch(Command::Del { key: b"k".to_vec() }).await,
        Reply::Removed(true)
    );
    assert_eq!(
        pool.dispatch(Command::Del { key: b"k".to_vec() }).await,
        Reply::Removed(false)
    );
    assert_eq!(
        pool.dispatch(Command::Get { key: b"k".to_vec() }).await,
        Reply::Bulk(None)
    );
}

/// The no-await rule, asserted structurally: this test is a plain `#[test]`
/// with no runtime under it. If `apply` ever became `async`, or grew an
/// `await`, this would stop compiling — which is the point. A comment
/// saying "do not await here" would not.
///
/// The `now` it passes is the same reading a handler would otherwise have
/// taken for itself, and taking it here is what shows a handler does not
/// need a clock — or a runtime to hold one.
#[test]
fn a_handler_runs_to_completion_without_a_runtime() {
    let mut state = ShardState::new(Dict::with_seed(DictSeed { k0: 7, k1: 9 }), NoopLog);
    let now = Instant::now();

    let stored = apply(&mut state, 0, &mut set(b"k", b"v"), now, &Deadlines);
    assert_eq!(stored, Reply::Ok);
    assert_eq!(
        apply(&mut state, 0, &mut get(b"k"), now, &Deadlines),
        Reply::Bulk(Some(b"v".to_vec()))
    );
}

/// A handler takes the command's value, and leaves everything the trace
/// reads.
///
/// `apply` moves a `Set`'s value into the dict instead of copying it, and
/// the executor hands that same command to the [`TraceSink`] afterwards.
/// So the division is load-bearing rather than incidental: a handler may
/// take what the trace does not fold, and nothing else. Taking the key
/// would move every recorded trace hash while every test that only reads
/// the keyspace back stayed green — which is the one failure this pins.
#[test]
fn a_handler_takes_the_value_and_leaves_what_the_trace_reads() {
    let mut state = ShardState::new(Dict::with_seed(DictSeed { k0: 4, k1: 6 }), NoopLog);
    let mut cmd = set(b"k", b"v");

    assert_eq!(
        apply(&mut state, 0, &mut cmd, Instant::now(), &Deadlines),
        Reply::Ok
    );
    assert_eq!(
        cmd.route(),
        Route::Key(b"k"),
        "the trace folds the key after the handler"
    );
    assert_eq!(cmd.kind(), set(b"k", b"v").kind());
    assert_eq!(
        state.dict.get(b"k").map(|entry| entry.value.clone()),
        Some(b"v".to_vec())
    );
    // The other half of the same fact: the dict holds the only copy of the
    // value, because the command no longer has one. A `SET` that copied it
    // would leave both, which is the cost this arrangement exists to avoid.
    assert!(
        matches!(&cmd, Command::Set { value, .. } if value.is_empty()),
        "the value was copied into the dict rather than moved"
    );
}

#[test]
fn seq_advances_only_for_commands_that_change_something() {
    let mut state = ShardState::new(Dict::with_seed(DictSeed { k0: 1, k1: 1 }), NoopLog);
    let now = Instant::now();
    let run = |state: &mut ShardState<NoopLog>, mut cmd: Command| {
        apply(state, 3, &mut cmd, now, &Deadlines)
    };

    // A read moves nothing.
    run(&mut state, Command::Get { key: b"a".to_vec() });
    assert_eq!(state.seq, 0);

    // A write does.
    run(&mut state, set(b"a", b"1"));
    assert_eq!(state.seq, 1);

    // A delete that removes nothing writes no record: replaying it would
    // be a no-op, so the log should not carry it.
    run(
        &mut state,
        Command::Del {
            key: b"absent".to_vec(),
        },
    );
    assert_eq!(state.seq, 1);

    // A rejected IncrBy likewise.
    run(
        &mut state,
        Command::IncrBy {
            key: b"a".to_vec(),
            delta: 1,
        },
    );
    assert_eq!(
        state.seq, 2,
        "'1' is a valid integer, so this one does count"
    );

    run(&mut state, set(b"txt", b"abc"));
    assert_eq!(state.seq, 3);
    run(
        &mut state,
        Command::IncrBy {
            key: b"txt".to_vec(),
            delta: 1,
        },
    );
    assert_eq!(
        state.seq, 3,
        "a rejected IncrBy must not consume a sequence number"
    );

    // And a delete that does remove something.
    run(&mut state, Command::Del { key: b"a".to_vec() });
    assert_eq!(state.seq, 4);
}

#[tokio::test]
async fn incr_by_reports_overflow_instead_of_panicking() {
    let pool = ShardPool::spawn(4, 4, DictSeed { k0: 3, k1: 4 }, NoTrace);
    assert_eq!(
        pool.dispatch(set(b"c", i64::MAX.to_string().as_bytes()))
            .await,
        Reply::Ok
    );
    assert_eq!(
        pool.dispatch(Command::IncrBy {
            key: b"c".to_vec(),
            delta: 1
        })
        .await,
        Reply::Error(ReplyError::WouldOverflow)
    );
    // The value is untouched.
    assert_eq!(
        pool.dispatch(Command::Get { key: b"c".to_vec() }).await,
        Reply::Bulk(Some(i64::MAX.to_string().into_bytes()))
    );
}

#[tokio::test]
async fn a_missing_counter_starts_at_zero_and_set_overwrites() {
    let pool = ShardPool::spawn(8, 4, DictSeed { k0: 5, k1: 6 }, NoTrace);
    assert_eq!(
        pool.dispatch(Command::IncrBy {
            key: b"fresh".to_vec(),
            delta: -3
        })
        .await,
        Reply::Integer(-3)
    );
    assert_eq!(pool.dispatch(set(b"fresh", b"100")).await, Reply::Ok);
    assert_eq!(
        pool.dispatch(Command::IncrBy {
            key: b"fresh".to_vec(),
            delta: 1
        })
        .await,
        Reply::Integer(101)
    );
}
