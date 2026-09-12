//! `KEYS` and `SCAN` over a connection: a walk that crosses every shard
//! without losing a key or repeating one.

use super::support::{connected, read_frames, req};
use crate::WALK_STEP_BUCKETS;
use crate::connection::serve_connection;
use crate::fan_out::{KEYS_TOO_LARGE, keys};
use crate::node::NodeInfo;
use crate::options::wrong_arity;
use crate::walk::{pack_cursor, unpack_cursor};
use seedstone_core::dict::DictSeed;
use seedstone_core::shard::{Command, NoTrace, Reply, ReplyError, Router, ShardPool};
use seedstone_resp::{Frame, encode};
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn keys_returns_every_matching_key_across_shards_without_repeating_one() {
    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    for i in 0..200u32 {
        encode(&req(&["SET", &format!("wanted-{i}"), "v"]), &mut out);
    }
    for i in 0..50u32 {
        encode(&req(&["SET", &format!("other-{i}"), "v"]), &mut out);
    }
    encode(&req(&["KEYS", "wanted-*"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, 251).await;
    let Frame::Array(items) = &frames[250] else {
        panic!("KEYS must answer an array, got {:?}", frames[250]);
    };
    let mut names: Vec<Vec<u8>> = items
        .iter()
        .map(|f| match f {
            Frame::Bulk(b) => b.clone(),
            other => panic!("KEYS must answer bulk strings, got {other:?}"),
        })
        .collect();
    let total = names.len();
    names.sort();
    names.dedup();
    assert_eq!(names.len(), 200, "every matching key must be returned");
    assert_eq!(total, names.len(), "KEYS must not repeat a key");
}

#[tokio::test]
async fn keys_on_an_empty_keyspace_answers_an_empty_array() {
    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    encode(&req(&["KEYS", "*"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 1).await;
    assert_eq!(frames[0], Frame::Array(Vec::new()));
}

/// A walk long enough to take many steps loses no key and repeats none
/// across the step boundaries, and the command behind it still answers.
///
/// **This is not the yielding proof**, and it must not be read as one: a
/// `KEYS` and a `GET` pipelined on one connection are served by one task
/// that awaits the whole walk before it looks at the next frame, so no
/// interleaving is possible here and none is asserted.
/// [`a_keys_walk_takes_many_envelopes_rather_than_one`] is the yielding
/// proof. What this holds is the seam the other one does not touch: 2000
/// keys on one shard is many times [`KEYS_STEP_BUCKETS`], and a step that
/// resumed at the wrong cursor would drop or duplicate keys across the
/// joins rather than fail outright.
#[tokio::test]
async fn a_multi_step_walk_neither_loses_a_key_nor_repeats_one() {
    let (mut r, mut w, _pool) = connected(1);
    let mut out = Vec::new();
    for i in 0..2000u32 {
        encode(&req(&["SET", &format!("k-{i}"), "v"]), &mut out);
    }
    encode(&req(&["KEYS", "k-*"]), &mut out);
    encode(&req(&["GET", "k-0"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 2002).await;
    assert!(matches!(&frames[2000], Frame::Array(a) if a.len() == 2000));
    assert_eq!(frames[2001], Frame::Bulk(b"v".to_vec()));
}

/// The claim the design rests on: a walk occupies a shard for one step,
/// not for the cycle.
///
/// Asserted by counting envelopes rather than by racing a `GET` against
/// the walk. An executor takes one envelope per pass of its loop, so a
/// walk split across many envelopes is a walk any other envelope on that
/// shard overtakes — the property — while a walk that answered in one
/// envelope would hold the shard for the whole cycle whatever a timing
/// assertion happened to observe. Counting is also deterministic, and a
/// timing assertion on a shared runner is a flake.
#[tokio::test]
async fn a_keys_walk_takes_many_envelopes_rather_than_one() {
    /// How many keys the fixture writes, sized off the budget rather than
    /// written as a number: a number tracks whatever the budget happened
    /// to be the day it was written, and this fixture was 2000 keys and
    /// stopped exercising the property the first time
    /// [`WALK_STEP_BUCKETS`] moved. Enough keys that the shard's table
    /// cannot fit inside one step whatever the constant becomes.
    const KEYS: usize = WALK_STEP_BUCKETS * 4;

    /// Counts the steps each shard is asked for, and otherwise is its pool.
    #[derive(Clone)]
    struct CountSteps {
        steps: std::sync::Arc<std::sync::Mutex<Vec<u16>>>,
        inner: ShardPool,
    }

    impl Router for CountSteps {
        async fn dispatch(&self, cmd: Command) -> Reply {
            self.inner.dispatch(cmd).await
        }

        fn shards(&self) -> u16 {
            self.inner.shards()
        }

        async fn dispatch_at(&self, shard: u16, cmd: Command) -> Reply {
            if matches!(cmd, Command::ScanStep { .. }) {
                self.steps.lock().expect("steps mutex").push(shard);
            }
            self.inner.dispatch_at(shard, cmd).await
        }

        async fn dispatch_every(&self, cmd: Command) -> Vec<Reply> {
            self.inner.dispatch_every(cmd).await
        }
    }

    let router = CountSteps {
        steps: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        inner: ShardPool::spawn(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace),
    };
    let (client, server) = tokio::io::duplex(1024 * 1024);
    tokio::spawn(serve_connection(
        server,
        router.clone(),
        NodeInfo::for_tests(),
    ));
    let (mut r, mut w) = tokio::io::split(client);

    let mut out = Vec::new();
    for i in 0..KEYS {
        encode(&req(&["SET", &format!("k-{i}"), "v"]), &mut out);
    }
    encode(&req(&["KEYS", "k-*"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, KEYS + 1).await;
    assert!(matches!(&frames[KEYS], Frame::Array(a) if a.len() == KEYS));

    let steps = router.steps.lock().expect("steps mutex").len();
    // A table this size walked WALK_STEP_BUCKETS at a time cannot be one
    // envelope. The assertion is deliberately `> 1` and not an exact
    // count: how many buckets these keys occupy is the dict's business and
    // may change, while "more than one envelope" is the property.
    assert!(
        steps > 1,
        "a {KEYS}-key walk took {steps} envelope(s); one means it held the shard for the cycle"
    );
}

/// The ceiling on a `KEYS` reply, from both sides.
///
/// Both halves are the test. A walk that abandoned unconditionally would
/// pass the first assertion on its own, and one that counted nothing
/// would pass the second; only a walk that counts what it gathers and
/// stops on the ceiling passes both. The keyspace is the same for both,
/// because a walk gathers rather than writes.
#[tokio::test]
async fn a_keys_reply_past_the_ceiling_is_refused_rather_than_gathered() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 9, k1: 4 }, NoTrace);
    // 64 names of 2 KiB each: 128 KiB of key bytes, far past the 4 KiB
    // ceiling below and nowhere near the unbounded one.
    for i in 0..64u32 {
        let mut key = format!("{i:02}-").into_bytes();
        key.resize(2 * 1024, b'k');
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

    // Matched rather than compared, here and below, so a failure reports
    // the shape it got instead of printing 128 KiB of key names.
    match keys(&pool, b"*".to_vec(), 4096).await {
        Frame::Error(text) => assert_eq!(text, KEYS_TOO_LARGE),
        Frame::Array(gathered) => panic!(
            "128 KiB of key names under a 4 KiB ceiling gathered {} keys instead of refusing",
            gathered.len()
        ),
        other => panic!("KEYS answered {other:?} rather than refusing"),
    }

    let answer = keys(&pool, b"*".to_vec(), usize::MAX).await;
    let Frame::Array(found) = answer else {
        panic!("an unbounded KEYS must answer an array, got {answer:?}");
    };
    assert_eq!(
        found.len(),
        64,
        "every key must survive a ceiling nothing can reach"
    );
}

/// A walk driven the way a client drives it: from `0`, following the
/// cursor the server hands back, until it is `0` again.
#[tokio::test]
async fn a_full_scan_returns_every_key_and_ends_at_zero() {
    // Two shards rather than four for the same 2000 keys: a shard has to
    // hold more of its own cursor space than one call's bucket budget
    // before any call can stop in the middle of one, and that is the
    // arrangement the assertions below are about.
    const SHARDS: u16 = 2;
    let (mut r, mut w, _pool) = connected(SHARDS);
    // Enough keys that a shard's cursor space is larger than one call's
    // whole bucket budget, which is what makes a call stop in the middle
    // of a shard below — the case the cursor's shard-plus-internal shape
    // exists for. Derived from the budget and the shard count rather than
    // written as a number: as a number it silently stopped exercising the
    // property the first time `WALK_STEP_BUCKETS` moved.
    let keys = WALK_STEP_BUCKETS * usize::from(SHARDS) * 4;
    let mut out = Vec::new();
    for i in 0..keys {
        encode(&req(&["SET", &format!("s-{i}"), "v"]), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let _ = read_frames(&mut r, keys).await;

    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut cursor = String::from("0");
    let mut calls = 0;
    let mut resumed_mid_table = false;
    loop {
        let mut out = Vec::new();
        encode(&req(&["SCAN", &cursor, "COUNT", "16"]), &mut out);
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();
        let frames = read_frames(&mut r, 1).await;
        let Frame::Array(pair) = &frames[0] else {
            panic!("SCAN must answer a two-element array, got {:?}", frames[0]);
        };
        assert_eq!(pair.len(), 2);
        let Frame::Bulk(next) = &pair[0] else {
            panic!("the cursor must be a bulk string");
        };
        let Frame::Array(keys) = &pair[1] else {
            panic!("the keys must be an array");
        };
        for key in keys {
            let Frame::Bulk(k) = key else {
                panic!("keys are bulk strings")
            };
            seen.push(k.clone());
        }
        cursor = String::from_utf8(next.clone()).unwrap();
        if unpack_cursor(cursor.parse().expect("this server issued this cursor")).1 != 0 {
            resumed_mid_table = true;
        }
        calls += 1;
        assert!(calls < 500, "the walk did not terminate");
        if cursor == "0" {
            break;
        }
    }
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), keys);
    // A call crosses shards now, so a cycle can cost fewer calls than the
    // node has shards and a count alone proves nothing either way. The
    // property is stated on the cursors themselves: some call stopped
    // inside a shard rather than at a boundary, which is the only thing
    // that needs the internal half of the cursor to mean something.
    assert!(
        resumed_mid_table,
        "{calls} calls over {SHARDS} shards, every one of them stopping at a shard boundary"
    );
}

/// One call crosses shards until it has the keys the client asked for.
///
/// The property the whole change exists for, stated on the wire where a
/// client can see it: `COUNT` is a number of keys, not a number of
/// buckets, and a shard too small to fill it is followed into the next
/// rather than costing a round trip of its own.
#[tokio::test]
async fn one_scan_call_crosses_shards_until_it_has_count_keys() {
    const SHARDS: u16 = 16;
    let (mut r, mut w, _pool) = connected(SHARDS);
    // Two keys per shard on average, so every shard is spent in a single
    // step and the old walk would have cost sixteen calls for thirty-two
    // keys.
    let mut out = Vec::new();
    for i in 0..32u32 {
        encode(&req(&["SET", &format!("k{i}"), "v"]), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let _ = read_frames(&mut r, 32).await;

    let scan = |cursor: String| {
        let mut out = Vec::new();
        encode(&req(&["SCAN", &cursor, "COUNT", "10"]), &mut out);
        out
    };

    w.write_all(&scan("0".to_owned())).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 1).await;
    let Frame::Array(pair) = &frames[0] else {
        panic!("SCAN must answer a two-element array, got {:?}", frames[0]);
    };
    let Frame::Array(keys) = &pair[1] else {
        panic!("the keys must be an array");
    };
    assert!(
        keys.len() >= 10,
        "a COUNT of 10 gathers at least ten keys in one call when shards are small: {}",
        keys.len()
    );

    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut cursor = String::from("0");
    let mut calls = 0;
    loop {
        w.write_all(&scan(cursor)).await.unwrap();
        w.flush().await.unwrap();
        let frames = read_frames(&mut r, 1).await;
        let Frame::Array(pair) = &frames[0] else {
            panic!("SCAN must answer a two-element array, got {:?}", frames[0]);
        };
        let (Frame::Bulk(next), Frame::Array(keys)) = (&pair[0], &pair[1]) else {
            panic!("SCAN answers a bulk cursor and an array of keys");
        };
        for key in keys {
            let Frame::Bulk(k) = key else {
                panic!("keys are bulk strings")
            };
            seen.push(k.clone());
        }
        cursor = String::from_utf8(next.clone()).unwrap();
        calls += 1;
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(seen.len(), 32, "a cycle must still return every key once");
    assert!(
        calls <= 6,
        "32 keys at COUNT 10 over {SHARDS} shards is about four calls, not sixteen: {calls}"
    );
}

/// `MATCH` filters, and it filters on the shard rather than at the edge —
/// what this asserts is only that the client sees the filtered set.
#[tokio::test]
async fn a_scan_with_match_returns_only_the_keys_that_match() {
    let (mut r, mut w, _pool) = connected(8);
    let mut out = Vec::new();
    for i in 0..40u32 {
        encode(&req(&["SET", &format!("wanted-{i}"), "v"]), &mut out);
        encode(&req(&["SET", &format!("other-{i}"), "v"]), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let _ = read_frames(&mut r, 80).await;

    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut cursor = String::from("0");
    loop {
        let mut out = Vec::new();
        encode(&req(&["SCAN", &cursor, "MATCH", "wanted-*"]), &mut out);
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();
        let frames = read_frames(&mut r, 1).await;
        let Frame::Array(pair) = &frames[0] else {
            panic!("SCAN must answer a two-element array, got {:?}", frames[0]);
        };
        let (Frame::Bulk(next), Frame::Array(keys)) = (&pair[0], &pair[1]) else {
            panic!("SCAN answers a bulk cursor and an array of keys");
        };
        for key in keys {
            let Frame::Bulk(k) = key else {
                panic!("keys are bulk strings")
            };
            assert!(
                k.starts_with(b"wanted-"),
                "MATCH let through {}",
                String::from_utf8_lossy(k)
            );
            seen.push(k.clone());
        }
        cursor = String::from_utf8(next.clone()).unwrap();
        if cursor == "0" {
            break;
        }
    }
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 40, "MATCH lost a key it should have returned");
}

#[tokio::test]
async fn scan_rejects_what_it_cannot_read_and_answers_what_it_can() {
    let (mut r, mut w, _pool) = connected(16);
    let requests: [&[&str]; 7] = [
        &["SCAN"],
        &["SCAN", "notanumber"],
        &["SCAN", "0", "COUNT", "0"],
        &["SCAN", "0", "COUNT", "-1"],
        &["SCAN", "0", "COUNT", "notanumber"],
        &["SCAN", "0", "NOSUCHOPTION", "x"],
        &["SCAN", "0", "MATCH"],
    ];
    let mut out = Vec::new();
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    assert!(matches!(&frames[0], Frame::Error(e) if e.contains("wrong number of arguments")));
    assert!(matches!(&frames[1], Frame::Error(e) if e.contains("invalid cursor")));
    assert!(matches!(&frames[2], Frame::Error(e) if e.contains("syntax error")));
    assert!(matches!(&frames[3], Frame::Error(e) if e.contains("syntax error")));
    assert!(matches!(&frames[4], Frame::Error(e) if e.contains("not an integer")));
    assert!(matches!(&frames[5], Frame::Error(e) if e.contains("syntax error")));
    assert!(matches!(&frames[6], Frame::Error(e) if e.contains("syntax error")));
}

#[tokio::test]
async fn a_cursor_naming_a_shard_that_does_not_exist_is_refused_not_ignored() {
    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    // Shard 60000 of a pool that has far fewer.
    encode(
        &req(&["SCAN", &pack_cursor(60000, 0).to_string()]),
        &mut out,
    );
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 1).await;
    assert!(matches!(&frames[0], Frame::Error(e) if e.contains("invalid cursor")));
}

/// A step that reaches [`Router::dispatch`] instead of `dispatch_at` is
/// refused, rather than answered by whichever shard its route stood in.
///
/// This is the property `SCAN` needs and `KEYS` did not. A step's shard
/// comes out of an integer a peer chose, so a route that stood in a real
/// shard would turn a mis-routed step into a walk of the wrong table that
/// answers plausibly — a fraction of the keyspace with nothing on the wire
/// to say so. `Route::Unaddressed` makes that a refusal instead.
#[tokio::test]
async fn a_scan_step_that_skips_dispatch_at_is_refused_rather_than_misrouted() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    for i in 0..64u32 {
        pool.dispatch(Command::Set {
            key: format!("k-{i}").into_bytes(),
            value: b"v".to_vec(),
            expiry: None,
            cond: None,
            keep_ttl: false,
            get: false,
        })
        .await;
    }
    let direct = pool
        .dispatch(Command::ScanStep {
            cursor: 0,
            count: usize::MAX,
            pattern: None,
        })
        .await;
    assert_eq!(
        direct,
        Reply::Error(ReplyError::ShardUnavailable),
        "an unrouted step must be refused, not answered from a stand-in shard"
    );
    // The shard the route used to stand in for still answers when it is
    // named, so the refusal is about the routing and not about the step.
    let at_zero = pool
        .dispatch_at(
            0,
            Command::ScanStep {
                cursor: 0,
                count: usize::MAX,
                pattern: None,
            },
        )
        .await;
    let Reply::Scan { cursor, keys, .. } = at_zero else {
        panic!("expected Reply::Scan");
    };
    assert_eq!(cursor, 0, "an unbounded count must finish the cycle");
    assert!(
        keys.len() < 64,
        "shard 0 of four held every key, which makes this test prove nothing"
    );
}

#[tokio::test]
async fn keys_takes_exactly_one_pattern() {
    let (mut r, mut w, _pool) = connected(4);
    let mut out = Vec::new();
    encode(&req(&["KEYS"]), &mut out);
    encode(&req(&["KEYS", "a*", "b*"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, 2).await;
    assert_eq!(frames[0], Frame::Error(wrong_arity("keys")));
    assert_eq!(frames[1], Frame::Error(wrong_arity("keys")));
}

/// Redis takes `ASYNC` and `SYNC` here; this server takes neither, and an
/// arity error is how it says so.
#[tokio::test]
async fn flushdb_takes_no_arguments() {
    let (mut r, mut w, _pool) = connected(4);
    let mut out = Vec::new();
    encode(&req(&["FLUSHDB", "ASYNC"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, 1).await;
    assert_eq!(frames[0], Frame::Error(wrong_arity("flushdb")));
}
