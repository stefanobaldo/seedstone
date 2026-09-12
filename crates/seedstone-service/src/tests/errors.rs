//! What a connection does with a request it cannot answer: a refusal that
//! keeps the session, and a protocol error that ends it.

use super::support::{connected, read_frames};
use crate::INVALID_CURSOR;
use crate::auth::{AUTH_NOT_CONFIGURED, NOAUTH, NOAUTH_HELLO, WRONGPASS};
use crate::connection::serve_connection;
use crate::fan_out::KEYS_TOO_LARGE;
use crate::hello::NOPROTO;
use crate::node::NodeInfo;
use crate::options::SYNTAX_ERROR;
use crate::reply::{UNRENDERABLE_REPLY, reply_to_frame};
use seedstone_core::dict::DictSeed;
use seedstone_core::shard::{Command, NoTrace, Reply, Router, ShardPool};
use seedstone_resp::{Frame, MAX_ARRAY_LEN, MAX_BULK_LEN};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The texts this module puts on the wire without scrubbing are
/// frame-safe: the error constants it declares, and the status texts a
/// shard hands it.
///
/// [`every_shard_error_is_frame_safe`] holds the [`ReplyError`] set to the
/// property the frame format needs; these reach the wire the same way and
/// by the same argument — a `&'static str` nobody composed from
/// peer-supplied bytes — but nothing held them to it. The module
/// documentation names their type as the reason they may skip
/// [`safe_error`], so the type's claim is checked here rather than
/// asserted there.
///
/// The status texts are read back through a real pool rather than listed,
/// because they are the shard's to choose and a list here would be this
/// module's guess at them. `TYPE` is what produces one, and its two
/// answers are a key that is there and a key that is not.
///
/// It bites: a `\r` or `\n` added to any of these, or a lowercase error
/// code, fails this test. It cannot bite for a constant added later and
/// not listed — there is no exhaustiveness to lean on for free constants,
/// which is exactly why the list is short and lives beside the
/// declarations it names.
#[tokio::test]
async fn every_error_constant_is_frame_safe() {
    for (name, text) in [
        ("INVALID_CURSOR", INVALID_CURSOR),
        ("KEYS_TOO_LARGE", KEYS_TOO_LARGE),
        ("UNRENDERABLE_REPLY", UNRENDERABLE_REPLY),
        ("SYNTAX_ERROR", SYNTAX_ERROR),
        ("NOPROTO", NOPROTO),
        ("NOAUTH", NOAUTH),
        ("NOAUTH_HELLO", NOAUTH_HELLO),
        ("WRONGPASS", WRONGPASS),
        ("AUTH_NOT_CONFIGURED", AUTH_NOT_CONFIGURED),
    ] {
        assert!(
            !text.contains(['\r', '\n']),
            "{name} carries a frame terminator: {text:?}"
        );
        assert!(!text.is_empty(), "{name} has no text");
        assert!(
            text.split(' ').next().is_some_and(|code| {
                !code.is_empty() && code.chars().all(|c| c.is_ascii_uppercase())
            }),
            "{name} does not open with an error code: {text:?}"
        );
    }

    let pool = ShardPool::spawn(1, 1, DictSeed { k0: 3, k1: 5 }, NoTrace);
    pool.dispatch(Command::Set {
        key: b"present".to_vec(),
        value: b"v".to_vec(),
        expiry: None,
        cond: None,
        keep_ttl: false,
        get: false,
    })
    .await;
    for key in [b"present".to_vec(), b"absent".to_vec()] {
        let named = String::from_utf8_lossy(&key).into_owned();
        let reply = pool.dispatch(Command::Type { key }).await;
        assert!(
            matches!(reply, Reply::Status(_)),
            "TYPE {named} answered {reply:?} rather than a status"
        );
        let frame = reply_to_frame(reply);
        let Frame::Simple(text) = frame else {
            panic!("a status reached the wire as {frame:?} rather than a simple string");
        };
        assert!(
            !text.contains(['\r', '\n']),
            "the status for {named} carries a frame terminator: {text:?}"
        );
        assert!(!text.is_empty(), "the status for {named} has no text");
    }
}

#[tokio::test]
async fn a_protocol_error_reports_and_closes() {
    let (mut r, mut w, _pool) = connected(4);
    w.write_all(b"!nonsense\r\n").await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, 1).await;
    assert!(
        matches!(&frames[0], Frame::Error(e) if e.contains("Protocol error")),
        "{:?}",
        frames[0]
    );
    // Desynchronised: the server must not keep reading.
    let mut rest = Vec::new();
    r.read_to_end(&mut rest).await.unwrap();
    assert!(
        rest.is_empty(),
        "server kept talking after a protocol error"
    );
}

/// A declared bulk length above the codec's ceiling is refused at the
/// header, so the payload it promises is never buffered.
#[tokio::test]
async fn an_oversized_bulk_is_refused_without_being_buffered() {
    let (mut r, mut w, _pool) = connected(4);
    let over = MAX_BULK_LEN + 1;
    w.write_all(format!("*2\r\n$3\r\nGET\r\n${over}\r\n").as_bytes())
        .await
        .unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, 1).await;
    assert!(
        matches!(&frames[0], Frame::Error(e) if e.contains("Protocol error")),
        "{:?}",
        frames[0]
    );
    let mut rest = Vec::new();
    r.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty());
}

#[tokio::test]
async fn an_oversized_array_count_is_refused_too() {
    let (mut r, mut w, _pool) = connected(4);
    let over = MAX_ARRAY_LEN + 1;
    w.write_all(format!("*{over}\r\n").as_bytes())
        .await
        .unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, 1).await;
    assert!(
        matches!(&frames[0], Frame::Error(e) if e.contains("Protocol error")),
        "{:?}",
        frames[0]
    );
}

#[tokio::test]
async fn a_disconnect_ends_the_connection_task() {
    let pool = ShardPool::spawn(4, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    let task = tokio::spawn(serve_connection(server, pool, NodeInfo::for_tests()));
    drop(client);
    // Returns rather than spinning on EOF.
    task.await.expect("the connection task must end cleanly");
}
