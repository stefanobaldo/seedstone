//! A connection with a password on it: what is refused before `AUTH`, and
//! what `HELLO` may carry instead.

use super::support::{connected, node_with_password, read_frames, req};
use crate::auth::{AUTH_NOT_CONFIGURED, NOAUTH, NOAUTH_HELLO, Secret, WRONGPASS};
use crate::connection::serve_connection;
use crate::hello::NOPROTO;
use crate::node::NodeInfo;
use seedstone_core::dict::DictSeed;
use seedstone_core::shard::{NoTrace, ShardPool};
use seedstone_resp::{Frame, encode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Before `AUTH`, everything but `AUTH`, `HELLO` and `QUIT` is refused and
/// the stream stays in sync; after it, the same commands answer.
#[tokio::test]
async fn a_password_gates_every_command_until_auth() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(
        server,
        pool,
        node_with_password(b"s3cret"),
    ));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    // One per kind of command this server answers: a connection command,
    // a keyed one, and the four an exporter scrapes — which connects
    // like any other client and is refused like any other client.
    let refused: [&[&str]; 6] = [
        &["PING"],
        &["SET", "k", "v"],
        &["INFO"],
        &["CONFIG", "GET", "requirepass"],
        &["SLOWLOG", "LEN"],
        &["LATENCY", "LATEST"],
    ];
    let then: [&[&str]; 6] = [
        &["AUTH", "wrong"],
        &["AUTH", "admin", "s3cret"],
        &["AUTH", "s3cret"],
        &["SET", "k", "v"],
        &["GET", "k"],
        &["AUTH", "default", "s3cret"],
    ];
    for parts in refused.iter().chain(&then) {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, refused.len() + then.len()).await;
    for (parts, frame) in refused.iter().zip(&frames) {
        assert_eq!(
            *frame,
            Frame::Error(NOAUTH.to_owned()),
            "{parts:?} was answered before AUTH"
        );
    }
    let after = &frames[refused.len()..];
    assert_eq!(after[0], Frame::Error(WRONGPASS.to_owned()));
    assert_eq!(
        after[1],
        Frame::Error(WRONGPASS.to_owned()),
        "a username other than default is refused with the same text as a wrong password"
    );
    assert_eq!(after[2], Frame::Simple("OK".into()));
    assert_eq!(after[3], Frame::Simple("OK".into()));
    assert_eq!(after[4], Frame::Bulk(b"v".to_vec()));
    assert_eq!(
        after[5],
        Frame::Simple("OK".into()),
        "AUTH again, authenticated, is fine"
    );
}

#[tokio::test]
async fn auth_without_a_configured_password_is_the_redis_error() {
    let (mut r, mut w, _pool) = connected(4);
    let mut out = Vec::new();
    encode(&req(&["AUTH", "anything"]), &mut out);
    encode(&req(&["PING"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 2).await;
    assert_eq!(frames[0], Frame::Error(AUTH_NOT_CONFIGURED.to_owned()));
    assert_eq!(frames[1], Frame::Simple("PONG".into()));
}

/// `HELLO 2 AUTH default <pw>` authenticates in the handshake, and
/// `HELLO 2 AUTH` with a wrong password answers WRONGPASS and leaves the
/// connection unauthenticated.
#[tokio::test]
async fn hello_carries_auth() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, node_with_password(b"pw")));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    encode(&req(&["HELLO", "2", "AUTH", "default", "nope"]), &mut out);
    encode(&req(&["PING"]), &mut out);
    encode(&req(&["HELLO", "2", "AUTH", "default", "pw"]), &mut out);
    encode(&req(&["PING"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 4).await;
    assert_eq!(frames[0], Frame::Error(WRONGPASS.to_owned()));
    assert_eq!(frames[1], Frame::Error(NOAUTH.to_owned()));
    assert!(
        matches!(frames[2], Frame::Array(_)),
        "HELLO with the right password answers the map"
    );
    assert_eq!(frames[3], Frame::Simple("PONG".into()));
}

/// `HELLO` with no `AUTH` is refused on a node that has a password, as
/// Redis refuses it, and the refusal names the form that would have
/// worked. Both spellings of the credential-less handshake are checked —
/// bare, and with the version — because they take different paths through
/// `hello` and meet the gate at the same place.
///
/// The third row is the agreement, and it is here because it is the same
/// mechanism seen from the other side. `HELLO 99` names a version this
/// server refuses on its own, and that refusal travels as
/// [`Action::Refuse`], which the gate lets through by name — so the answer
/// is [`NOPROTO`] here as it is on Redis (6.2.24, 8.10.1), because a
/// request's own mistake is decided before the connection's state is. The
/// handshake that parsed is still told the form that would have worked,
/// and the one that named a version this server does not speak is told
/// which half of its request was refused.
#[tokio::test]
async fn hello_without_auth_is_refused() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, node_with_password(b"pw")));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    encode(&req(&["HELLO"]), &mut out);
    encode(&req(&["HELLO", "2"]), &mut out);
    encode(&req(&["HELLO", "99"]), &mut out);
    encode(&req(&["PING"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 4).await;
    assert_eq!(frames[0], Frame::Error(NOAUTH_HELLO.to_owned()));
    assert_eq!(frames[1], Frame::Error(NOAUTH_HELLO.to_owned()));
    assert_eq!(
        frames[2],
        Frame::Error(NOPROTO.to_owned()),
        "a HELLO the handler refused is answered with the handler's own \
         refusal, before the gate is consulted — Redis's order (6.2.24, \
         8.10.1)"
    );
    // The general refusal, not the handshake's: the two texts are
    // different on purpose and a client tells the requests apart by them.
    assert_eq!(frames[3], Frame::Error(NOAUTH.to_owned()));
}

/// The three refusals `hello` decides about the request itself pass the
/// gate unauthenticated — and only those: the same connection's `GET` is
/// still `NOAUTH`, and `AUTH_NOT_CONFIGURED`, which `hello` also decides,
/// is not one of them. Rows read against `redis:6-alpine` (6.2.24) and
/// `redis:8-alpine` (8.10.1) with `--requirepass`, no `AUTH` sent, on
/// 2026-09-10.
///
/// Two rows are this server's answer rather than Redis's, and say so
/// where they sit. `SETNAME` is **accepted** by both versions once the
/// connection has authenticated (`CLIENT GETNAME` reads the name back);
/// unauthenticated, where the rows above were read, both answer the
/// `NOAUTH HELLO` sentence instead, because the option parses and the
/// connection is then the objection. This server refuses it in either
/// state: it has no client name to set, and taking the option silently
/// would be worse than either answer. And `NOAUTH_HELLO`'s sentence is
/// 6.2.24's — 8.10.1 spells the same refusal with
/// `the HELLO <proto> AUTH <user> <pass> option`.
#[tokio::test]
async fn hello_refusals_pass_the_gate_unauthenticated() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, node_with_password(b"pw")));
    let (mut r, mut w) = tokio::io::split(client);
    let version_error = "ERR Protocol version is not an integer or out of range";
    let cases: [(&[&str], &str); 8] = [
        (&["HELLO", "99"], NOPROTO),
        (&["HELLO", "abc"], version_error),
        (&["HELLO", "abc", "BOGUS", "x"], version_error),
        (
            &["HELLO", "2", "BOGUS", "x"],
            "ERR Syntax error in HELLO option 'BOGUS'",
        ),
        // This server's own refusal, not Redis's: see the doc comment.
        (
            &["HELLO", "2", "SETNAME", "x"],
            "ERR Syntax error in HELLO option 'SETNAME'",
        ),
        // `HELLO 3` is what redis-py 8.1.0 opens every connection with
        // unless the caller names `protocol=2` — the reading behind the
        // client lane's settings, which records that and the separate
        // one-argument `AUTH` that lane's URL produces. The embedded
        // `AUTH` option is Redis's own spelling of a handshake that
        // authenticates, and both versions answer this one with a RESP3
        // map (6.2.24, 8.10.1); here the version is decided first, so the
        // client is told which half of its request was refused.
        (&["HELLO", "3", "AUTH", "default", "pw"], NOPROTO),
        (&["HELLO", "2"], NOAUTH_HELLO),
        (&["GET", "k"], NOAUTH),
    ];
    let mut out = Vec::new();
    for (args, _) in &cases {
        encode(&req(args), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, cases.len()).await;
    for (frame, (args, expected)) in frames.iter().zip(cases) {
        assert_eq!(*frame, Frame::Error(expected.to_owned()), "{args:?}");
    }
}

/// What `INFO commandstats` reports for a `HELLO` the handler refused —
/// read off the section, not inferred from the path the refusal took.
///
/// A refusal that travels as `Action::Refuse` *is* an action, and the
/// edge counts every action that is not a `Dispatch`, so a refused
/// handshake lands in `cmdstat_hello` — `calls=1,usec=1` for the one
/// below, read off the section on 2026-09-10 rather than derived from the
/// path. The same refusal spelt as the handler's `Err` never became an
/// action and printed no `cmdstat_hello` line at all, read the same way
/// the same day with the handler returning one. Which figure the section
/// therefore reports is what this test is here to keep honest; see
/// [`commandstats_section`] for what it means.
///
/// The reading is taken on the same connection *after* it authenticates,
/// because `INFO` is gated too: an unauthenticated peer cannot read the
/// section its handshake just moved. The refusal being counted happened
/// before the `AUTH`, which is the case that matters. The section is
/// node-wide rather than per connection, so the `cmdstat_auth` line beside
/// it says only that the node saw the `AUTH` — here that is the same thing
/// as this connection, because this node has exactly one.
#[tokio::test]
async fn a_refused_hello_is_counted_in_commandstats() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, node_with_password(b"pw")));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    encode(&req(&["HELLO", "99"]), &mut out);
    encode(&req(&["AUTH", "default", "pw"]), &mut out);
    encode(&req(&["INFO", "commandstats"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 3).await;
    assert_eq!(frames[0], Frame::Error(NOPROTO.to_owned()));
    let Frame::Bulk(text) = &frames[2] else {
        panic!("INFO answered {:?}", frames[2])
    };
    let text = String::from_utf8(text.clone()).unwrap();
    assert!(
        text.contains("cmdstat_hello:calls=1,"),
        "one refused handshake, one call: {text}"
    );
    assert!(text.contains("cmdstat_auth:calls=1,"), "{text}");
}

/// The same handshake on a node with **no** password is answered, because
/// there is nothing to authenticate against. This is the arm `gated`
/// never reaches, and the one that would silently disappear if the
/// refusal above were moved into `hello` itself.
#[tokio::test]
async fn hello_without_auth_is_answered_where_there_is_no_password() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, NodeInfo::for_tests()));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    encode(&req(&["HELLO", "2"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 1).await;
    assert!(matches!(frames[0], Frame::Array(_)), "{:?}", frames[0]);
}

/// The order `HELLO`'s refusals are decided in — every row measured
/// against `redis:6-alpine` (6.2.24) and asserted here so the order
/// cannot drift back.
///
/// A node with no password is only the simplest place to read them: every
/// row here is a refusal `hello` decides on its own, and those travel as
/// `Action::Refuse`, so the same answers reach a connection that has not
/// authenticated — which `hello_refusals_pass_the_gate_unauthenticated`
/// pins.
///
/// The rows that matter are the ones where two mistakes compete. `HELLO
/// abc BOGUS x` is a bad version *and* a bad option, and the version is
/// what Redis reports, so reading the options first — which this server
/// did — answers the wrong one. `SETNAME` has no row here; the divergence
/// it carries is argued where the row is, in
/// `hello_refusals_pass_the_gate_unauthenticated`.
#[tokio::test]
async fn hello_refusals_are_decided_in_redis_order() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, NodeInfo::for_tests()));
    let (mut r, mut w) = tokio::io::split(client);
    let version_error = "ERR Protocol version is not an integer or out of range";
    let cases: [(&[&str], &str); 5] = [
        (&["HELLO", "abc"], version_error),
        // The row this test exists for.
        (&["HELLO", "abc", "BOGUS", "x"], version_error),
        (&["HELLO", "99"], NOPROTO),
        (&["HELLO", "99", "AUTH", "default", "pw"], NOPROTO),
        (
            &["HELLO", "2", "BOGUS", "x"],
            "ERR Syntax error in HELLO option 'BOGUS'",
        ),
    ];
    let mut out = Vec::new();
    for (args, _) in &cases {
        encode(&req(args), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, cases.len()).await;
    for (frame, (args, expected)) in frames.iter().zip(cases) {
        assert_eq!(*frame, Frame::Error(expected.to_owned()), "{args:?}");
    }
}

/// `QUIT` is answered before authentication: a client that decides to go
/// away is not asked for a password first.
#[tokio::test]
async fn quit_is_answered_unauthenticated() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, node_with_password(b"pw")));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    encode(&req(&["QUIT"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 1).await;
    assert_eq!(frames[0], Frame::Simple("OK".into()));
    let mut rest = Vec::new();
    r.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty(), "the server kept talking after QUIT");
}

#[test]
fn the_secret_does_not_print_itself() {
    let secret = Secret::new(b"hunter2".to_vec());
    assert!(!format!("{secret:?}").contains("hunter2"));
    assert!(secret.matches(b"hunter2"));
    assert!(!secret.matches(b"hunter"));
    assert!(!secret.matches(b"hunter22"));
    assert!(!secret.matches(b""));
}
