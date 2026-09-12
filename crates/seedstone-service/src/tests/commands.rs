//! The keyed commands over a connection: what each answers, and what each
//! refuses.

use super::support::{connected, read_frames, req};
use crate::connection::serve_connection;
use crate::node::NodeInfo;
use crate::options::wrong_arity;
use seedstone_core::dict::DictSeed;
use seedstone_core::shard::{NoTrace, ShardPool};
use seedstone_resp::{Frame, encode};
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn every_command_maps_to_its_reply_frame() {
    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    for parts in [
        &["INCRBY", "n", "5"][..],
        &["INCRBY", "n", "-2"],
        &["GET", "n"],
        &["GET", "absent"],
        &["DEL", "n"],
        &["DEL", "n"],
        &["SET", "s", "hello"],
        &["INCRBY", "s", "1"],
    ] {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, 8).await;
    assert_eq!(frames[0], Frame::Integer(5));
    assert_eq!(frames[1], Frame::Integer(3));
    assert_eq!(frames[2], Frame::Bulk(b"3".to_vec()));
    assert_eq!(frames[3], Frame::Null, "a missing key is a null bulk");
    assert_eq!(frames[4], Frame::Integer(1), "Del that removed something");
    assert_eq!(frames[5], Frame::Integer(0), "Del that removed nothing");
    assert_eq!(frames[6], Frame::Simple("OK".into()));
    assert!(
        matches!(&frames[7], Frame::Error(e) if e.contains("not an integer")),
        "{:?}",
        frames[7]
    );
}

/// Every `SET` option this server took before it took them all, every way
/// of getting them wrong, and the exact text of each refusal.
///
/// The refusals are written out as literals rather than taken from the
/// constants that produce them. A client matching on `ERR syntax error`
/// cannot see this server's constants, so a test that quoted them would go
/// on passing after a typo landed in one — which is the only failure this
/// test exists to catch.
///
/// The algebra here is Redis's own — the accepted spellings, the
/// case-insensitivity, the mutual exclusions, and every refusal's exact
/// text. `KEEPTTL`, `GET` and the absolute deadlines have a test of their
/// own below; between them the two cover the surface. An option outside
/// that surface — one Redis has grown and this server has not — is
/// answered `ERR syntax error`, which is the deliberate choice over
/// accepting an option and silently not honouring it: a client that asked
/// for something and was told `OK` by a server that dropped it has been
/// lied to, and finds out later, in production.
///
/// A row added to either test is a claim about what Redis answers, so add
/// it only with one measured.
#[tokio::test]
async fn set_options_parse_as_redis_does() {
    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    let requests: [&[&str]; 20] = [
        &["SET", "k", "v", "EX", "10"],
        &["TTL", "k"],
        &["SET", "k", "other", "PX", "500", "NX"],
        &["GET", "k"],
        &["SET", "fresh", "v", "PX", "500", "NX"],
        &["SET", "k", "v", "XX"],
        &["TTL", "k"],
        &["SET", "absent", "v", "XX"],
        &["set", "c", "v", "ex", "10", "Nx"],
        &["TTL", "c"],
        // Two *different* members of the expiry family conflict, where the
        // same one repeated does not — see the test below.
        &["SET", "k", "v", "EX", "10", "PX", "5"],
        &["SET", "k", "v", "NX", "XX"],
        &["SET", "k", "v", "EX"],
        &["SET", "k", "v", "EX", "0"],
        &["SET", "k", "v", "EX", "-1"],
        &["SET", "k", "v", "EX", "9223372036854775807"],
        &["SET", "k", "v", "PX", "0"],
        &["SET", "k", "v", "EX", "notanum"],
        // An option outside this server's surface, which is what the
        // paragraph above is about. Redis takes `PERSIST` on `GETEX` and
        // not on `SET`, so this row is one both servers refuse — and it is
        // the row that keeps the refusal itself tested now that the
        // options this server used to refuse are options it takes.
        &["SET", "k", "v", "PERSIST"],
        &["GET", "k"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let syntax = Frame::Error("ERR syntax error".into());
    let expire = Frame::Error("ERR invalid expire time in 'set' command".into());
    // Sized to the requests above, so one added without its answer is a
    // compile error rather than a `zip` that quietly stops early.
    let expected: [Frame; 20] = [
        Frame::Simple("OK".into()),
        Frame::Integer(10),
        Frame::Null,
        Frame::Bulk(b"v".to_vec()),
        Frame::Simple("OK".into()),
        Frame::Simple("OK".into()),
        // A `SET` with no expiry option clears the deadline it overwrote.
        Frame::Integer(-1),
        Frame::Null,
        Frame::Simple("OK".into()),
        Frame::Integer(10),
        syntax.clone(),
        syntax.clone(),
        syntax,
        expire.clone(),
        expire.clone(),
        expire.clone(),
        expire,
        Frame::Error("ERR value is not an integer or out of range".into()),
        Frame::Error("ERR syntax error".into()),
        // Every refusal above left the connection usable.
        Frame::Bulk(b"v".to_vec()),
    ];
    for (i, (got, want)) in frames.iter().zip(&expected).enumerate() {
        assert_eq!(got, want, "request {i}: {:?}", requests[i]);
    }
}

/// The rest of the algebra: a repeated option, `KEEPTTL`, `GET`, and the
/// deadlines `EXAT`/`PXAT` name.
///
/// Where the test above is mostly refusals, this one is mostly commands
/// that work — every option here is one this server answered `ERR syntax
/// error` until it implemented them, and each is something a real client
/// sends: `KEEPTTL` and `GET` because they are how a client updates a value
/// without losing what it knows about the key, the absolute forms because a
/// scheduler that computed a deadline once should not have to re-derive a
/// span per retry, and the repeat because a client that builds a command by
/// appending options emits one without meaning to. The three rows that are
/// still refusals are here because they are what those options conflict
/// with.
///
/// Reads the same as the test above: literal answers, and Redis's own.
#[tokio::test]
async fn set_keeps_a_ttl_answers_the_old_value_and_takes_a_deadline() {
    // The two rows no literal can answer for, and what they are worth
    // asserting: both name the same instant — one in seconds, one in
    // milliseconds — and [`NodeInfo::for_tests`] freezes the wall clock at
    // `FIXED_UNIX_MILLIS`, so the remaining span is exactly
    // 99_999_999_999_000 − 1_700_000_000_000 milliseconds and the seconds
    // `TTL` reports are arithmetic, not a measurement: the answer is the
    // upper end of the range below. The range admits the second under it
    // only because `remaining_seconds` rounds to nearest, which puts the
    // boundary a full 500 milliseconds away — the gap between a row and
    // its `TTL` on a loopback pipeline cannot reach it, and a machine
    // stalled that long is the one case worth not flaking on. A `> 0`
    // assertion would pass just as well if the deadline had been read as a
    // *relative* span, which is the defect these rows exist to catch.
    const CLOCK_ROWS: [usize; 2] = [12, 16];
    const FAR_TTL_SECONDS: std::ops::RangeInclusive<i64> = 98_299_999_998..=98_299_999_999;

    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    let requests: [&[&str]; 26] = [
        // Last occurrence wins, where this server used to answer a syntax error.
        &["SET", "k", "v", "EX", "100", "EX", "50"],
        &["TTL", "k"],
        // KEEPTTL keeps the deadline a plain SET would have cleared.
        &["SET", "k", "kept", "KEEPTTL"],
        &["TTL", "k"],
        &["GET", "k"],
        // KEEPTTL and an expiry option together are a syntax error.
        &["SET", "k", "v", "KEEPTTL", "EX", "10"],
        &["SET", "k", "v", "EX", "10", "KEEPTTL"],
        // KEEPTTL under a condition that refuses the write is not: the
        // command is well formed, it simply does not happen, and the
        // deadline it would have kept is still there afterwards.
        &["SET", "k", "other", "NX", "KEEPTTL"],
        &["TTL", "k"],
        // GET answers the previous value, and the absent case is null.
        &["SET", "k", "new", "GET"],
        &["SET", "brandnew", "first", "GET"],
        // Absolute deadlines, in both units. The two `SET`s below name the
        // same instant, so their two `TTL`s answer the same number.
        &["SET", "at", "v", "EXAT", "99999999999"],
        &["TTL", "at"],
        &["SET", "past", "v", "EXAT", "1"],
        &["EXISTS", "past"],
        &["SET", "atms", "v", "PXAT", "99999999999000"],
        &["TTL", "atms"],
        &["SET", "pastms", "v", "PXAT", "1"],
        &["EXISTS", "pastms"],
        // NX and XX still conflict, and are still last-wins-free.
        &["SET", "k", "v", "NX", "XX"],
        &["SET", "k", "v", "XX", "XX"],
        &["GET", "k"],
        // A condition that refuses the write still answers `GET` with the
        // value the write did not replace, and leaves it where it was.
        &["SET", "k", "other", "NX", "GET"],
        &["GET", "k"],
        // The same refusal over a key that does not exist: null, and the
        // refused write did not create it.
        &["SET", "missing", "v", "XX", "GET"],
        &["EXISTS", "missing"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let syntax = Frame::Error("ERR syntax error".into());
    let expected: [Frame; 26] = [
        Frame::Simple("OK".into()),
        Frame::Integer(50),
        Frame::Simple("OK".into()),
        Frame::Integer(50),
        Frame::Bulk(b"kept".to_vec()),
        syntax.clone(),
        syntax.clone(),
        Frame::Null,
        Frame::Integer(50),
        Frame::Bulk(b"kept".to_vec()),
        Frame::Null,
        Frame::Simple("OK".into()),
        // Asserted below rather than here; see CLOCK_ROWS.
        Frame::Integer(0),
        Frame::Simple("OK".into()),
        // A deadline in the past stores the key and leaves it already due,
        // so the next command to look for it does not find it.
        Frame::Integer(0),
        Frame::Simple("OK".into()),
        Frame::Integer(0),
        Frame::Simple("OK".into()),
        Frame::Integer(0),
        syntax,
        Frame::Simple("OK".into()),
        Frame::Bulk(b"v".to_vec()),
        Frame::Bulk(b"v".to_vec()),
        Frame::Bulk(b"v".to_vec()),
        Frame::Null,
        Frame::Integer(0),
    ];
    for (i, (got, want)) in frames.iter().zip(&expected).enumerate() {
        if CLOCK_ROWS.contains(&i) {
            continue;
        }
        assert_eq!(got, want, "request {i}: {:?}", requests[i]);
    }
    for row in CLOCK_ROWS {
        assert!(
            matches!(frames[row], Frame::Integer(n) if FAR_TTL_SECONDS.contains(&n)),
            "request {row}: {:?} answered {:?}",
            requests[row],
            frames[row]
        );
    }
}

/// A repeated option discards the earlier occurrence whole — its argument
/// with it, unread.
///
/// The rule is not "the last value wins" but "the earlier occurrence never
/// happened", and the two differ on exactly the rows below: an argument
/// that would have been refused is not refused if a later occurrence
/// replaces it, because nothing ever looks at it. `EX notanum EX 10` is
/// therefore a command that works and `EX 10 EX notanum` is not, and the
/// order of the two words is the entire difference.
///
/// It follows that a syntax error anywhere beats an invalid expire time
/// anywhere: the walk refuses an unknown word while reading it, and the one
/// surviving argument is validated only once the walk has finished. The
/// last row is that consequence.
///
/// Measured against a live `redis-server v=8.10.0` rather than reasoned
/// about — a parser written from the outside in would have validated
/// eagerly, and every row here would have been wrong in a way no client
/// could work around.
#[tokio::test]
async fn a_repeated_set_option_discards_the_earlier_one_argument_and_all() {
    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    let requests: [&[&str]; 7] = [
        // A zero the later occurrence discards, where `EX 0` alone is an
        // invalid expire time.
        &["SET", "k", "v", "EX", "0", "EX", "10"],
        &["TTL", "k"],
        // The same for an argument that is not a number at all. The `TTL`
        // proves the surviving occurrence took effect rather than the
        // command quietly losing its deadline.
        &["SET", "k", "v", "EX", "notanum", "EX", "10"],
        &["TTL", "k"],
        // Reversed, both are refused — and refused differently, which is
        // what says the *surviving* argument is the one being validated.
        &["SET", "k", "v", "EX", "10", "EX", "notanum"],
        &["SET", "k", "v", "EX", "10", "EX", "0"],
        // An unknown word after an argument that would have been refused:
        // the syntax error is the answer, because it happens first.
        &["SET", "k", "v", "EX", "0", "BOGUS"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let expected: [Frame; 7] = [
        Frame::Simple("OK".into()),
        Frame::Integer(10),
        Frame::Simple("OK".into()),
        Frame::Integer(10),
        Frame::Error("ERR value is not an integer or out of range".into()),
        Frame::Error("ERR invalid expire time in 'set' command".into()),
        Frame::Error("ERR syntax error".into()),
    ];
    for (i, (got, want)) in frames.iter().zip(&expected).enumerate() {
        assert_eq!(got, want, "request {i}: {:?}", requests[i]);
    }
}

/// `PEXPIRE` and `PERSIST`: a deadline named in milliseconds, and one
/// taken away.
///
/// Every row is measured against a live `redis-server v=8.10.0`, including
/// the three that are not obvious. A `PERSIST` over a key that is there but
/// carries no deadline answers `0`, not `1` — the answer is whether a
/// deadline was removed, not whether the key is now without one. A
/// `PEXPIRE` whose span is not in the future answers `1` and deletes the
/// key, which is `EXPIRE`'s applied-expiry answer in the smaller unit. And
/// a span of `i64::MAX` is refused as an invalid expire time rather than
/// accepted as a key that outlives the universe — as is every span past
/// the clock-relative boundary read on 6.2.24 and 8.10.1, whose accepted
/// side is pinned in
/// [`expiry_spans_are_bounded_by_the_clock_like_redis`].
#[tokio::test]
async fn pexpire_and_persist_move_a_deadline_and_take_it_away() {
    let (mut r, mut w, _pool) = connected(16);
    let requests: [&[&str]; 19] = [
        &["SET", "k", "v"],
        &["PERSIST", "k"], // nothing to remove
        &["PEXPIRE", "k", "100000"],
        &["TTL", "k"],
        &["PERSIST", "k"], // removes it
        &["TTL", "k"],
        &["PEXPIRE", "missing", "1000"],
        &["PERSIST", "missing"],
        &["PEXPIRE", "k", "notanumber"],
        // A deadline in the past is a deletion, reported as an expiry that
        // was applied. Zero and a negative say the same thing.
        &["SET", "z", "v"],
        &["PEXPIRE", "z", "0"],
        &["EXISTS", "z"],
        &["SET", "n", "v"],
        &["PEXPIRE", "n", "-1"],
        &["EXISTS", "n"],
        // Two spans past the ceiling, which is the clock's rather than
        // either constant in this file: the second is `MAX_EXPIRE_MILLIS`
        // itself, which the constant filter lets through and the clock
        // then refuses, because `now` plus it leaves the `i64` a deadline
        // is held in. The accepted side of that boundary is pinned to the
        // millisecond in `expiry_spans_are_bounded_by_the_clock_like_redis`,
        // which these two rows only bracket from above.
        &["PEXPIRE", "k", "9223372036854775807"],
        &["PEXPIRE", "k", "9223372036854775000"],
        // The arity, for each of the two.
        &["PEXPIRE", "k"],
        &["PERSIST", "k", "extra"],
    ];
    let mut out = Vec::new();
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    assert_eq!(frames[1], Frame::Integer(0));
    assert_eq!(frames[2], Frame::Integer(1));
    assert!(matches!(frames[3], Frame::Integer(n) if (90..=100).contains(&n)));
    assert_eq!(frames[4], Frame::Integer(1));
    assert_eq!(frames[5], Frame::Integer(-1));
    assert_eq!(frames[6], Frame::Integer(0));
    assert_eq!(frames[7], Frame::Integer(0));
    assert!(matches!(&frames[8], Frame::Error(e) if e.contains("not an integer")));
    assert_eq!(frames[10], Frame::Integer(1), "a deadline already past");
    assert_eq!(frames[11], Frame::Integer(0), "and the key it deleted");
    assert_eq!(frames[13], Frame::Integer(1), "a negative span, the same");
    assert_eq!(frames[14], Frame::Integer(0));
    assert_eq!(
        frames[15],
        Frame::Error("ERR invalid expire time in 'pexpire' command".into())
    );
    assert_eq!(
        frames[16],
        Frame::Error("ERR invalid expire time in 'pexpire' command".into()),
        "the old constant ceiling is itself past the clock's boundary"
    );
    assert_eq!(
        frames[17],
        Frame::Error("ERR wrong number of arguments for 'pexpire' command".into())
    );
    assert_eq!(
        frames[18],
        Frame::Error("ERR wrong number of arguments for 'persist' command".into())
    );
}

/// `SETEX` is `SET key value EX seconds` with the span before the value,
/// and refuses what that `SET` refuses. Every row is measured against
/// `redis:6-alpine` (`redis_version:6.2.24`) and `redis:8-alpine`
/// (`redis_version:8.10.1`), which agree on every reply but one wording:
/// 6.2 says `invalid expire time in setex`, 8.10 says `in 'setex'
/// command`. This server says the second, as it does for `set`, `expire`
/// and `pexpire`.
///
/// Two rows are about what is *not* written. A refused span — zero, or
/// not a number — leaves the key exactly as it was: the shard never
/// hears of the command, so there is nothing to roll back.
#[tokio::test]
async fn setex_is_set_with_ex_under_its_old_name() {
    let (mut r, mut w, _pool) = connected(16);
    let requests: [&[&str]; 23] = [
        &["SETEX", "good", "100", "hello"],
        &["GET", "good"],
        &["TTL", "good"],
        &["TYPE", "good"],
        &["setex", "lower", "100", "hello"],
        // Arity is exact: nothing after the value, not even a `SET`
        // option Redis would take on `SET` itself.
        &["SETEX", "k"],
        &["SETEX", "k", "1"],
        &["SETEX", "k", "1", "v", "extra"],
        &["SETEX", "k", "10", "v", "NX"],
        &["SETEX", "k", "0", "v"],
        &["SETEX", "k", "-1", "v"],
        &["SETEX", "k", "notanum", "v"],
        &["SETEX", "k", "1.5", "v"],
        &["SETEX", "k", "", "v"],
        &["SETEX", "k", "9223372036854775807", "v"],
        &["SETEX", "k", "999999999999", "v"],
        // Value and deadline are both overwritten: a key without a
        // deadline acquires one.
        &["SET", "pre", "x"],
        &["SETEX", "pre", "50", "replaced"],
        &["GET", "pre"],
        &["TTL", "pre"],
        // A refused span writes nothing.
        &["SETEX", "good", "0", "shouldnotwrite"],
        &["SETEX", "good", "notanum", "shouldnotwrite"],
        &["GET", "good"],
    ];
    let mut out = Vec::new();
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let arity = Frame::Error("ERR wrong number of arguments for 'setex' command".into());
    let expire = Frame::Error("ERR invalid expire time in 'setex' command".into());
    let not_int = Frame::Error("ERR value is not an integer or out of range".into());
    let expected: [Frame; 23] = [
        Frame::Simple("OK".into()),
        Frame::Bulk(b"hello".to_vec()),
        Frame::Integer(100),
        Frame::Simple("string".into()),
        Frame::Simple("OK".into()),
        arity.clone(),
        arity.clone(),
        arity.clone(),
        arity,
        expire.clone(),
        expire.clone(),
        not_int.clone(),
        not_int.clone(),
        not_int,
        expire.clone(),
        Frame::Simple("OK".into()),
        Frame::Simple("OK".into()),
        Frame::Simple("OK".into()),
        Frame::Bulk(b"replaced".to_vec()),
        Frame::Integer(50),
        expire,
        Frame::Error("ERR value is not an integer or out of range".into()),
        Frame::Bulk(b"hello".to_vec()),
    ];
    for (i, (got, want)) in frames.iter().zip(&expected).enumerate() {
        assert_eq!(got, want, "request {i}: {:?}", requests[i]);
    }
}

/// `SETEX` is counted under its own name, and does not move `set`'s
/// count — the reason it is a command kind rather than an alias. Redis
/// keeps the two apart the same way (`cmdstat_setex` beside
/// `cmdstat_set` on 6.2.24 and 8.10.1).
#[tokio::test]
async fn setex_is_counted_apart_from_set() {
    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    encode(&req(&["SET", "a", "1"]), &mut out);
    encode(&req(&["SETEX", "b", "10", "2"]), &mut out);
    encode(&req(&["SETEX", "c", "10", "3"]), &mut out);
    encode(&req(&["INFO", "commandstats"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 4).await;
    let Frame::Bulk(text) = &frames[3] else {
        panic!("INFO answered {:?}", frames[3])
    };
    let text = String::from_utf8(text.clone()).unwrap();
    let counted = |prefix: &str| {
        assert!(
            text.lines().any(|written| written.starts_with(prefix)),
            "no line beginning {prefix:?} in {text}"
        );
    };
    counted("cmdstat_set:calls=1,usec=");
    counted("cmdstat_setex:calls=2,usec=");
}

/// `SETNX` is `SET key value NX` under the name Redis gave it before
/// `SET` grew options, with one difference that is not cosmetic: it
/// answers an integer where `SET … NX` answers `+OK` or a nil bulk. Every
/// row is measured against `redis:6-alpine` (`redis_version:6.2.24`), and
/// the rows that could differ were read again on `redis:8-alpine`
/// (`redis_version:8.10.1`), which answers the same.
///
/// The last three rows are the ones that decide the handler. A key that
/// is already there is refused *whatever its state*, and the deadline it
/// carries is neither refreshed nor cleared — so this cannot be written
/// as a write followed by a check.
#[tokio::test]
async fn setnx_writes_only_a_key_that_is_not_there() {
    let (mut r, mut w, _pool) = connected(16);
    let requests: [&[&str]; 12] = [
        &["SETNX", "fresh", "hello"],
        &["GET", "fresh"],
        &["TTL", "fresh"],
        &["SETNX", "fresh", "other"],
        &["GET", "fresh"],
        &["setnx", "lower", "hello"],
        // Arity is exact: two arguments, no options.
        &["SETNX"],
        &["SETNX", "k"],
        &["SETNX", "k", "v", "extra"],
        // A key already there is refused with its deadline intact.
        &["SET", "withttl", "v", "EX", "100"],
        &["SETNX", "withttl", "other"],
        &["TTL", "withttl"],
    ];
    let mut out = Vec::new();
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let arity = Frame::Error("ERR wrong number of arguments for 'setnx' command".into());
    let expected: [Frame; 12] = [
        Frame::Integer(1),
        Frame::Bulk(b"hello".to_vec()),
        Frame::Integer(-1),
        Frame::Integer(0),
        Frame::Bulk(b"hello".to_vec()),
        Frame::Integer(1),
        arity.clone(),
        arity.clone(),
        arity,
        Frame::Simple("OK".into()),
        Frame::Integer(0),
        Frame::Integer(100),
    ];
    for (i, (got, want)) in frames.iter().zip(&expected).enumerate() {
        assert_eq!(got, want, "request {i}: {:?}", requests[i]);
    }
}

/// `PSETEX` is `SET key value PX milliseconds` with the span before the
/// value — `SETEX`'s millisecond spelling. Measured against
/// `redis:6-alpine` (`redis_version:6.2.24`).
#[tokio::test]
async fn psetex_writes_a_value_and_a_millisecond_deadline() {
    let (mut r, mut w, _pool) = connected(16);
    let requests: [&[&str]; 8] = [
        &["PSETEX", "good", "100000", "hello"],
        &["GET", "good"],
        &["TYPE", "good"],
        &["psetex", "lower", "100000", "hello"],
        // Value and deadline are both overwritten: a key without a
        // deadline acquires one.
        &["SET", "pre", "x"],
        &["PSETEX", "pre", "50000", "replaced"],
        &["GET", "pre"],
        // `TTL` and not `PTTL`: this server answers the first and not the
        // second. It still tells the two units apart — a span read as
        // seconds rather than milliseconds would answer `100000` here,
        // not `100`.
        &["TTL", "good"],
    ];
    let mut out = Vec::new();
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let expected: [Frame; 7] = [
        Frame::Simple("OK".into()),
        Frame::Bulk(b"hello".to_vec()),
        Frame::Simple("string".into()),
        Frame::Simple("OK".into()),
        Frame::Simple("OK".into()),
        Frame::Simple("OK".into()),
        Frame::Bulk(b"replaced".to_vec()),
    ];
    for (i, (got, want)) in frames.iter().zip(&expected).enumerate() {
        assert_eq!(got, want, "request {i}: {:?}", requests[i]);
    }
    // The deadline is a span from now, so the assertion is a range, not a
    // point: the clock advances between the write and the read, and the
    // second the reply is rounded to may be either side of it. Redis
    // decays the same way — `PTTL` after `PSETEX good 100000` read
    // `99900` on 6.2.24.
    match &frames[7] {
        Frame::Integer(secs) => assert!((99..=100).contains(secs), "TTL was {secs}"),
        other => panic!("TTL answered {other:?}"),
    }
}

/// A refused span leaves the standing key exactly as it was, value and
/// deadline both: the shard never hears of the command, so there is
/// nothing to roll back (6.2.24).
///
/// The wording of the refusal is 8.10.1's, as it is for `set`, `setex`,
/// `expire` and `pexpire` — 6.2.24 names the command bare, without quotes
/// and without the trailing `command`, and this server follows the newer
/// form for all of them.
#[tokio::test]
async fn psetex_refuses_a_span_and_writes_nothing() {
    let (mut r, mut w, _pool) = connected(16);
    let requests: [&[&str]; 8] = [
        &["SET", "guard", "original"],
        &["PSETEX", "guard", "0", "refused"],
        &["PSETEX", "guard", "-1", "refused"],
        &["PSETEX", "guard", "9223372036854775807", "refused"],
        &["PSETEX", "guard", "notanum", "refused"],
        &["PSETEX", "guard", "1.5", "refused"],
        &["GET", "guard"],
        &["TTL", "guard"],
    ];
    let mut out = Vec::new();
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let expire = Frame::Error("ERR invalid expire time in 'psetex' command".into());
    let not_int = Frame::Error("ERR value is not an integer or out of range".into());
    let expected: [Frame; 8] = [
        Frame::Simple("OK".into()),
        expire.clone(),
        expire.clone(),
        expire,
        not_int.clone(),
        not_int,
        Frame::Bulk(b"original".to_vec()),
        Frame::Integer(-1),
    ];
    for (i, (got, want)) in frames.iter().zip(&expected).enumerate() {
        assert_eq!(got, want, "request {i}: {:?}", requests[i]);
    }
}

/// Arity is exact: nothing after the value, not even a `SET` option Redis
/// would take on `SET` itself.
#[tokio::test]
async fn psetex_refuses_every_arity_but_three() {
    let (mut r, mut w, _pool) = connected(16);
    let requests: [&[&str]; 5] = [
        &["PSETEX"],
        &["PSETEX", "k"],
        &["PSETEX", "k", "1"],
        &["PSETEX", "k", "1", "v", "extra"],
        &["PSETEX", "k", "10", "v", "NX"],
    ];
    let mut out = Vec::new();
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let arity = Frame::Error("ERR wrong number of arguments for 'psetex' command".into());
    for (i, got) in frames.iter().enumerate() {
        assert_eq!(got, &arity, "request {i}: {:?}", requests[i]);
    }
}

/// `TYPE` and `STRLEN` describe an entry without handing back its value.
///
/// Both answers `TYPE` can give are here, and the shape of the first is
/// half of what is being pinned: Redis puts `+string` on the wire, so a
/// bulk reply carrying the same six bytes would be the wrong frame for the
/// right text. `STRLEN` answers `0` for a key that is not there, and — as
/// measured against Redis 8.10 — the same `0` for a key holding an empty
/// value, which is why one is stored here rather than left to a reader's
/// assumption.
#[tokio::test]
async fn type_and_strlen_describe_what_is_stored() {
    let (mut r, mut w, _pool) = connected(16);
    let requests: [&[&str]; 9] = [
        &["SET", "k", "hello"],
        &["TYPE", "k"],
        &["TYPE", "missing"],
        &["STRLEN", "k"],
        &["STRLEN", "missing"],
        &["TYPE"],
        &["SET", "empty", ""],
        &["STRLEN", "empty"],
        &["STRLEN", "k", "extra"],
    ];
    let mut out = Vec::new();
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    assert_eq!(frames[1], Frame::Simple("string".into()));
    assert_eq!(frames[2], Frame::Simple("none".into()));
    assert_eq!(frames[3], Frame::Integer(5));
    assert_eq!(frames[4], Frame::Integer(0));
    assert!(matches!(&frames[5], Frame::Error(e) if e.contains("wrong number of arguments")));
    assert_eq!(
        frames[7],
        Frame::Integer(0),
        "an empty value has a length, and it is zero"
    );
    assert_eq!(
        frames[8],
        Frame::Error("ERR wrong number of arguments for 'strlen' command".into())
    );
}

/// `FLUSHDB` reaches every shard, and the keyspace is empty afterwards.
///
/// Sixteen shards against two keys on purpose: the keys land on at most
/// two of them, so a `FLUSHDB` that emptied only the shard it happened to
/// be routed to would still answer `+OK` and would still be caught here by
/// one of the two `GET`s.
#[tokio::test]
async fn flushdb_empties_every_shard_and_answers_ok() {
    let (mut r, mut w, _pool) = connected(16);
    let requests: [&[&str]; 5] = [
        &["SET", "a", "1"],
        &["SET", "b", "2"],
        &["FLUSHDB"],
        &["GET", "a"],
        &["GET", "b"],
    ];
    let mut out = Vec::new();
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let expected: [Frame; 5] = [
        Frame::Simple("OK".into()),
        Frame::Simple("OK".into()),
        Frame::Simple("OK".into()),
        Frame::Null,
        Frame::Null,
    ];
    assert_eq!(frames, expected);
}

/// `DBSIZE` is the size of the keyspace, not of the shard the connection
/// happened to reach.
///
/// Sixteen shards again, for the reason the flush test uses them: two keys
/// cannot land on more than two of them, so a count that came from one
/// shard would read zero or one where two is the answer.
#[tokio::test]
async fn dbsize_counts_live_keys_across_shards() {
    let (mut r, mut w, _pool) = connected(16);
    let requests: [&[&str]; 6] = [
        &["DBSIZE"],
        &["SET", "a", "1"],
        &["SET", "b", "2"],
        &["DBSIZE"],
        &["DEL", "a"],
        &["DBSIZE"],
    ];
    let mut out = Vec::new();
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    assert_eq!(frames[0], Frame::Integer(0));
    assert_eq!(frames[3], Frame::Integer(2));
    assert_eq!(frames[5], Frame::Integer(1));
}

#[tokio::test]
async fn dbsize_takes_no_arguments() {
    let (mut r, mut w, _pool) = connected(4);
    let mut out = Vec::new();
    encode(&req(&["DBSIZE", "0"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, 1).await;
    assert_eq!(frames[0], Frame::Error(wrong_arity("dbsize")));
}

/// `DEL` and `EXISTS` name any number of keys, and answer with one integer
/// however many shards those keys live on.
#[tokio::test]
async fn del_and_exists_fan_out() {
    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    let requests: [&[&str]; 12] = [
        &["SET", "a", "1"],
        &["SET", "b", "2"],
        &["DEL", "a", "b", "missing"],
        &["EXISTS", "a", "b", "b"],
        &["SET", "a", "1"],
        // Duplicates count once each, as Redis does.
        &["EXISTS", "a", "a", "missing"],
        &["EXISTS", "a"],
        &["DEL", "a"],
        &["DEL", "a"],
        &["EXISTS", "missing"],
        &["DEL"],
        &["EXISTS"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    assert_eq!(frames[0], Frame::Simple("OK".into()));
    assert_eq!(frames[1], Frame::Simple("OK".into()));
    assert_eq!(frames[2], Frame::Integer(2), "two of three keys existed");
    assert_eq!(frames[3], Frame::Integer(0), "the DEL removed both");
    assert_eq!(frames[4], Frame::Simple("OK".into()));
    assert_eq!(frames[5], Frame::Integer(2), "a repeated key counts twice");
    assert_eq!(frames[6], Frame::Integer(1), "one key is still one integer");
    assert_eq!(frames[7], Frame::Integer(1));
    assert_eq!(frames[8], Frame::Integer(0));
    assert_eq!(frames[9], Frame::Integer(0));
    assert_eq!(
        frames[10],
        Frame::Error("ERR wrong number of arguments for 'del' command".into())
    );
    assert_eq!(
        frames[11],
        Frame::Error("ERR wrong number of arguments for 'exists' command".into())
    );
}

/// `MGET` answers one entry per argument, in the order the peer wrote
/// them, whatever shard each key lives on.
///
/// The one-key request is in here deliberately: its answer is a
/// one-element array, not the bare bulk a plain `GET` would give. A client
/// that counts array elements — django-redis's `get_many` sends a one-key
/// `MGET` whenever its caller passes one key — reads a bare bulk as the
/// first frame of something longer and loses the stream from there.
#[tokio::test]
async fn mget_answers_one_entry_per_argument_in_order() {
    let (mut r, mut w, _pool) = connected(16);
    let requests: [&[&str]; 5] = [
        &["SET", "a", "1"],
        &["SET", "c", "3"],
        // A key named twice is answered twice: each name is its own
        // command, so nothing here deduplicates.
        &["MGET", "a", "missing", "c", "a"],
        &["MGET", "a"],
        &["MGET"],
    ];
    let mut out = Vec::new();
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    assert_eq!(
        frames[2],
        Frame::Array(vec![
            Frame::Bulk(b"1".to_vec()),
            Frame::Null,
            Frame::Bulk(b"3".to_vec()),
            Frame::Bulk(b"1".to_vec()),
        ])
    );
    assert_eq!(
        frames[3],
        Frame::Array(vec![Frame::Bulk(b"1".to_vec())]),
        "one key is still an array"
    );
    assert!(matches!(&frames[4], Frame::Error(e) if e.contains("wrong number of arguments")));
}

/// A fan-out runs behind whatever the peer pipelined in front of it.
///
/// A keyed command decoded earlier in the same drain is sitting in the
/// chunk's batch, dispatched only when the chunk closes — so a fan-out that
/// dispatched where it was decoded would run *ahead* of commands the peer
/// wrote first. The `DEL` below would then find a key its own `SET` had not
/// written yet, and the reply stream would be in order while the keyspace
/// was not.
#[tokio::test]
async fn a_fan_out_runs_behind_the_commands_pipelined_before_it() {
    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    for parts in [&["SET", "a", "1"][..], &["DEL", "a", "b"], &["EXISTS", "a"]] {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, 3).await;
    assert_eq!(frames[0], Frame::Simple("OK".into()));
    assert_eq!(
        frames[1],
        Frame::Integer(1),
        "the fan-out ran before the SET the peer wrote in front of it"
    );
    assert_eq!(frames[2], Frame::Integer(0));
}

/// The keyspace's three questions about a key's lifetime, answered exactly
/// as Redis answers them — including the two negative `TTL`s, which clients
/// distinguish.
#[tokio::test]
async fn expire_ttl_and_exists_answer_like_redis() {
    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    let requests: [&[&str]; 12] = [
        &["SET", "k", "v"],
        &["TTL", "k"],
        &["TTL", "missing"],
        &["EXISTS", "k"],
        &["EXPIRE", "k", "100"],
        &["TTL", "k"],
        &["EXPIRE", "missing", "10"],
        // A deadline that is not in the future removes the key, and Redis
        // reports it as an applied expiry.
        &["EXPIRE", "k", "0"],
        &["EXISTS", "k"],
        &["EXPIRE", "k", "notanum"],
        &["EXPIRE", "k"],
        &["TTL"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    assert_eq!(frames[0], Frame::Simple("OK".into()));
    assert_eq!(frames[1], Frame::Integer(-1), "a key with no deadline");
    assert_eq!(frames[2], Frame::Integer(-2), "a key that is not there");
    assert_eq!(frames[3], Frame::Integer(1));
    assert_eq!(frames[4], Frame::Integer(1));
    assert_eq!(frames[5], Frame::Integer(100));
    assert_eq!(frames[6], Frame::Integer(0), "nothing to expire");
    assert_eq!(frames[7], Frame::Integer(1));
    assert_eq!(frames[8], Frame::Integer(0), "the key is gone");
    assert_eq!(
        frames[9],
        Frame::Error("ERR value is not an integer or out of range".into())
    );
    assert_eq!(
        frames[10],
        Frame::Error("ERR wrong number of arguments for 'expire' command".into())
    );
    assert_eq!(
        frames[11],
        Frame::Error("ERR wrong number of arguments for 'ttl' command".into())
    );
}

/// A span too large to be turned into a deadline is refused here, where
/// the number is still a number.
///
/// The shard resolves a span against its clock and stores nothing when the
/// arithmetic leaves the clock's range — which, for an `EXPIRE` that got
/// that far, would clear the deadline the key already had and still report
/// success: `SET k v EX 30` followed by `EXPIRE k <i64::MAX>` would make
/// the key immortal. Redis refuses the argument instead, and so does this.
#[tokio::test]
async fn an_expire_span_that_cannot_be_represented_is_refused() {
    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    let requests: [&[&str]; 5] = [
        &["SET", "k", "v", "EX", "30"],
        &["EXPIRE", "k", "9223372036854775807"],
        &["EXPIRE", "k", "-9223372036854775808"],
        &["TTL", "k"],
        &["EXISTS", "k"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let refused = Frame::Error("ERR invalid expire time in 'expire' command".into());
    assert_eq!(frames[0], Frame::Simple("OK".into()));
    assert_eq!(frames[1], refused, "a span past the ceiling");
    assert_eq!(frames[2], refused, "and past the floor");
    assert_eq!(
        frames[3],
        Frame::Integer(30),
        "the refused EXPIRE must not have touched the deadline"
    );
    assert_eq!(frames[4], Frame::Integer(1));
}

/// The millisecond read and the two absolute deadlines, answered as Redis
/// answers them (6.2.24 and 8.10.1, the readings this branch stands on).
/// `EXPIREAT` with a time already passed deletes the key and reports `1`;
/// a non-integer is `not an integer` rather than an invalid expire time,
/// the parse failing before any range check.
///
/// Where the boundaries are is a separate claim, in
/// `the_absolute_deadlines_bound_the_unit_they_multiply`.
#[tokio::test]
async fn pttl_expireat_and_pexpireat_answer_like_redis() {
    let (mut r, mut w, _pool) = connected(16);
    let now_ms = (NodeInfo::for_tests().now_unix_millis)();
    let in_100_s = ((now_ms / 1000) + 100).to_string();
    let in_100_000_ms = (now_ms + 100_000).to_string();
    let requests: Vec<Vec<&str>> = vec![
        vec!["SET", "k", "v"],
        vec!["PTTL", "k"],
        vec!["PTTL", "missing"],
        vec!["EXPIREAT", "k", &in_100_s],
        vec!["TTL", "k"],
        vec!["PEXPIREAT", "k", &in_100_000_ms],
        vec!["PTTL", "k"],
        vec!["EXPIREAT", "k", "1"],
        vec!["EXISTS", "k"],
        vec!["EXPIREAT", "missing", &in_100_s],
        vec!["SET", "k", "v"],
        vec!["EXPIREAT", "k", "notanum"],
        vec!["PEXPIREAT", "k", "notanum"],
        vec!["EXPIREAT", "k"],
        vec!["PTTL"],
        vec!["PTTL", "k", "extra"],
    ];
    let mut out = Vec::new();
    for parts in &requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, requests.len()).await;
    let not_int = Frame::Error("ERR value is not an integer or out of range".into());
    assert_eq!(frames[0], Frame::Simple("OK".into()));
    assert_eq!(frames[1], Frame::Integer(-1), "no deadline");
    assert_eq!(frames[2], Frame::Integer(-2), "no key");
    assert_eq!(frames[3], Frame::Integer(1));
    assert!(
        matches!(frames[4], Frame::Integer(99..=100)),
        "{:?}",
        frames[4]
    );
    assert_eq!(frames[5], Frame::Integer(1));
    assert!(
        matches!(frames[6], Frame::Integer(99_000..=100_000)),
        "PTTL reads back the millisecond deadline: {:?}",
        frames[6]
    );
    assert_eq!(
        frames[7],
        Frame::Integer(1),
        "a deadline in the past is applied by deleting"
    );
    assert_eq!(frames[8], Frame::Integer(0), "and the key is gone at once");
    assert_eq!(frames[9], Frame::Integer(0), "nothing to expire");
    assert_eq!(frames[10], Frame::Simple("OK".into()));
    assert_eq!(frames[11], not_int);
    assert_eq!(frames[12], not_int);
    assert_eq!(
        frames[13],
        Frame::Error("ERR wrong number of arguments for 'expireat' command".into())
    );
    let pttl_arity = Frame::Error("ERR wrong number of arguments for 'pttl' command".into());
    assert_eq!(frames[14], pttl_arity);
    assert_eq!(frames[15], pttl_arity);
}

/// The two absolute spellings refuse a moment whose multiplication by the
/// unit leaves an `i64`, and their boundaries are **not** each other's
/// mirror — which is why both ends of both are here rather than one
/// example of each.
///
/// Read on 6.2.24 and 8.10.1: `EXPIREAT` takes `±(i64::MAX / 1000)` and
/// refuses one step past either, while `PEXPIREAT` takes every `i64`
/// there is — `i64::MAX` is a live deadline and `i64::MIN` is a deletion
/// answered `1`, neither of them a refusal. A deadline at or below zero
/// is that same deletion at any magnitude: Redis performs it rather than
/// refusing the sign.
#[tokio::test]
async fn the_absolute_deadlines_bound_the_unit_they_multiply() {
    let (mut r, mut w, _pool) = connected(16);
    let requests: Vec<Vec<&str>> = vec![
        vec!["SET", "k", "v"],
        vec!["EXPIREAT", "k", "9223372036854776"],
        vec!["EXPIREAT", "k", "9223372036854775"],
        vec!["PEXPIREAT", "k", "9223372036854775807"],
        vec!["SET", "k", "v"],
        vec!["EXPIREAT", "k", "-5"],
        vec!["EXISTS", "k"],
        vec!["SET", "k", "v"],
        vec!["EXPIREAT", "k", "-9223372036854776"],
        vec!["PEXPIREAT", "k", "-9223372036854775808"],
        vec!["EXISTS", "k"],
    ];
    let mut out = Vec::new();
    for parts in &requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, requests.len()).await;
    let refused = Frame::Error("ERR invalid expire time in 'expireat' command".into());
    assert_eq!(frames[0], Frame::Simple("OK".into()));
    assert_eq!(frames[1], refused, "one second past i64::MAX / 1000");
    assert_eq!(
        frames[2],
        Frame::Integer(1),
        "i64::MAX / 1000 is the last second EXPIREAT takes"
    );
    assert_eq!(
        frames[3],
        Frame::Integer(1),
        "PEXPIREAT is not multiplied, so i64::MAX fits"
    );
    assert_eq!(frames[4], Frame::Simple("OK".into()));
    assert_eq!(
        frames[5],
        Frame::Integer(1),
        "a negative deadline is applied by deleting"
    );
    assert_eq!(frames[6], Frame::Integer(0));
    assert_eq!(frames[7], Frame::Simple("OK".into()));
    assert_eq!(frames[8], refused, "and one second past the floor");
    assert_eq!(
        frames[9],
        Frame::Integer(1),
        "while i64::MIN milliseconds is a deletion, not a refusal"
    );
    assert_eq!(frames[10], Frame::Integer(0), "so the key is gone");
}

/// The three are counted under their own names, and neither `TTL`'s
/// counter nor `PEXPIRE`'s moves — the reason each is a command kind
/// rather than an alias resolved at the edge. Redis keeps all five apart
/// the same way (`cmdstat_pttl`, `cmdstat_expireat`, `cmdstat_pexpireat`
/// beside `cmdstat_ttl` and `cmdstat_pexpire` on 6.2.24 and 8.10.1).
#[tokio::test]
async fn the_absolute_deadlines_are_counted_apart_from_the_spans() {
    let (mut r, mut w, _pool) = connected(16);
    let mut out = Vec::new();
    encode(&req(&["SET", "k", "v"]), &mut out);
    encode(&req(&["PTTL", "k"]), &mut out);
    encode(&req(&["EXPIREAT", "k", "4102444800"]), &mut out);
    encode(&req(&["PEXPIREAT", "k", "4102444800000"]), &mut out);
    encode(&req(&["INFO", "commandstats"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 5).await;
    let Frame::Bulk(text) = &frames[4] else {
        panic!("INFO answered {:?}", frames[4])
    };
    let text = String::from_utf8(text.clone()).unwrap();
    let counted = |prefix: &str| {
        assert!(
            text.lines().any(|written| written.starts_with(prefix)),
            "no line beginning {prefix:?} in {text}"
        );
    };
    counted("cmdstat_pttl:calls=1,usec=");
    counted("cmdstat_expireat:calls=1,usec=");
    counted("cmdstat_pexpireat:calls=1,usec=");
    for absent in ["cmdstat_ttl:", "cmdstat_pexpire:"] {
        assert!(
            !text.lines().any(|written| written.starts_with(absent)),
            "{absent:?} moved, so the span spellings are sharing a counter: {text}"
        );
    }
}

/// Redis bounds a span by the clock — `now + span` must fit an `i64` of
/// milliseconds — so its ceiling is `(i64::MAX - now_ms) / 1000` seconds
/// and moves by one every second. Read on 6.2.24 and 8.10.1 (issue #27):
/// one below the boundary is accepted, one above is `ERR invalid expire
/// time in '<cmd>' command`, and `i64::MAX / 1000` — this server's old
/// constant — is refused by both.
#[tokio::test]
async fn expiry_spans_are_bounded_by_the_clock_like_redis() {
    let (mut r, mut w, _pool) = connected(16);
    let now = (NodeInfo::for_tests().now_unix_millis)();
    let boundary_secs = (i64::MAX as u64 - now) / 1000;
    let boundary_millis = i64::MAX as u64 - now;
    let under_in_secs = (boundary_secs - 1).to_string();
    let over_in_secs = (boundary_secs + 1).to_string();
    let under_in_millis = (boundary_millis - 1).to_string();
    let over_in_millis = (boundary_millis + 1).to_string();
    let old = (i64::MAX / 1000).to_string();
    let requests: Vec<Vec<&str>> = vec![
        vec!["SET", "a", "v", "EX", &under_in_secs],
        vec!["SET", "a", "v", "EX", &over_in_secs],
        vec!["SET", "a", "v", "EX", &old],
        vec!["SETEX", "b", &under_in_secs, "v"],
        vec!["SETEX", "b", &over_in_secs, "v"],
        vec!["EXPIRE", "a", &under_in_secs],
        vec!["EXPIRE", "a", &over_in_secs],
        vec!["PSETEX", "c", &under_in_millis, "v"],
        vec!["PSETEX", "c", &over_in_millis, "v"],
        vec!["PEXPIRE", "a", &under_in_millis],
        vec!["PEXPIRE", "a", &over_in_millis],
        vec!["SET", "d", "v", "PX", &under_in_millis],
        vec!["SET", "d", "v", "PX", &over_in_millis],
    ];
    let mut out = Vec::new();
    for parts in &requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, requests.len()).await;
    let refused = |name: &str| Frame::Error(format!("ERR invalid expire time in '{name}' command"));
    assert_eq!(frames[0], Frame::Simple("OK".into()));
    assert_eq!(frames[1], refused("set"));
    assert_eq!(
        frames[2],
        refused("set"),
        "the old constant is past Redis's boundary"
    );
    assert_eq!(frames[3], Frame::Simple("OK".into()));
    assert_eq!(frames[4], refused("setex"));
    assert_eq!(frames[5], Frame::Integer(1));
    assert_eq!(frames[6], refused("expire"));
    assert_eq!(frames[7], Frame::Simple("OK".into()));
    assert_eq!(frames[8], refused("psetex"));
    assert_eq!(frames[9], Frame::Integer(1));
    assert_eq!(frames[10], refused("pexpire"));
    assert_eq!(frames[11], Frame::Simple("OK".into()));
    assert_eq!(frames[12], refused("set"));
}

/// An ordinary span never reads the wall clock: the constant filters run
/// first and [`CLOCK_SAFE_SPAN_MILLIS`] second, so only a probe within a
/// millennium of the boundary pays for the read.
#[tokio::test]
async fn ordinary_spans_do_not_read_the_clock() {
    static READS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    fn counting_clock() -> u64 {
        READS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        1_788_298_743_000
    }
    let pool = ShardPool::spawn(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    let mut node = NodeInfo::for_tests();
    node.now_unix_millis = counting_clock;
    tokio::spawn(serve_connection(server, pool, node));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    for parts in [
        &["SET", "k", "v", "PX", "60000"][..],
        &["SETEX", "k", "60", "v"],
        &["EXPIRE", "k", "60"],
        &["PEXPIRE", "k", "60000"],
    ] {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let _ = read_frames(&mut r, 4).await;
    assert_eq!(
        READS.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "an ordinary span read the clock"
    );
}
