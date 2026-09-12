//! The container commands over a connection — `COMMAND`, `CLIENT`, `CONFIG`,
//! `SLOWLOG`, `LATENCY` — and the `HELLO` reply's shape.

use super::support::{connected, node_with_password, read_frames, req};
use crate::connection::serve_connection;
use crate::containers::CONFIG_PARAMETERS;
use crate::dispatch::COMMANDS;
use crate::node::NodeInfo;
use seedstone_core::dict::DictSeed;
use seedstone_core::memory::{EvictionMode, MemoryLimit};
use seedstone_core::shard::{NoTrace, ReplyError, ShardPool};
use seedstone_resp::{Frame, encode};
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn hello_reply_shape() {
    let (mut r, mut w, _pool) = connected(4);
    let mut out = Vec::new();
    encode(&req(&["HELLO"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, 1).await;
    assert_eq!(
        frames[0],
        Frame::Array(vec![
            Frame::Bulk(b"server".to_vec()),
            Frame::Bulk(b"seedstone".to_vec()),
            Frame::Bulk(b"version".to_vec()),
            Frame::Bulk(env!("CARGO_PKG_VERSION").as_bytes().to_vec()),
            Frame::Bulk(b"proto".to_vec()),
            Frame::Integer(2),
            Frame::Bulk(b"mode".to_vec()),
            Frame::Bulk(b"standalone".to_vec()),
            Frame::Bulk(b"role".to_vec()),
            Frame::Bulk(b"master".to_vec()),
        ])
    );
}

/// `COMMAND` and its subcommands, which a client sends before it sends
/// anything the user asked for.
///
/// redis-cli opens every session with `COMMAND DOCS`. Answering it with an
/// error would not merely be unfriendly — the reply is read before the
/// prompt appears, so a session that cannot survive it never starts. Each
/// one is followed by a keyed command here for that reason: the point is
/// that the connection is still usable afterwards.
#[tokio::test]
async fn command_subcommands_answer_without_breaking_the_session() {
    let (mut r, mut w, _pool) = connected(4);
    let mut out = Vec::new();
    let requests: [&[&str]; 8] = [
        &["COMMAND"],
        &["SET", "a", "1"],
        &["COMMAND", "COUNT"],
        &["SET", "b", "2"],
        &["COMMAND", "DOCS"],
        &["GET", "b"],
        &["COMMAND", "NOSUCH"],
        &["GET", "a"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    assert!(matches!(frames[0], Frame::Array(_)), "{:?}", frames[0]);
    assert_eq!(frames[1], Frame::Simple("OK".into()));
    // Derived, not restated: the count is the command table's length, and
    // a test that wrote a number here would be a second place to keep in
    // step with the surface. What the number *means* — that every entry is
    // a command the server runs — is the test below.
    assert_eq!(
        frames[2],
        Frame::Integer(i64::try_from(COMMANDS.len()).unwrap())
    );
    assert_eq!(frames[3], Frame::Simple("OK".into()));
    assert_eq!(frames[4], Frame::Array(Vec::new()));
    assert_eq!(frames[5], Frame::Bulk(b"2".to_vec()));
    assert_eq!(
        frames[6],
        Frame::Error("ERR unknown subcommand 'NOSUCH'. Try COMMAND HELP.".into())
    );
    assert_eq!(frames[7], Frame::Bulk(b"1".to_vec()));
}

/// `COMMAND COUNT` is the table's length, so the number is only truthful
/// if every entry of the table is a command the server actually runs.
///
/// Each name is sent with no arguments and the reply is checked for one
/// thing: that it is not `unknown command`. Most answer an arity error,
/// which is exactly the point — an arity error is a command that was
/// dispatched. `QUIT` is the one name left out, because it would end the
/// connection the rest of the loop is using; it is covered by
/// `connection_commands_never_reach_the_router`.
#[tokio::test]
async fn every_name_in_the_command_table_is_a_command_the_server_runs() {
    let (mut r, mut w, _pool) = connected(4);
    let names: Vec<&[u8]> = COMMANDS
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| *name != b"QUIT")
        .collect();
    let mut out = Vec::new();
    for name in &names {
        encode(&Frame::Array(vec![Frame::Bulk(name.to_vec())]), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, names.len()).await;
    for (name, frame) in names.iter().zip(&frames) {
        let name = String::from_utf8_lossy(name);
        assert!(
            !matches!(frame, Frame::Error(e) if e.contains("unknown command")),
            "{name} is counted by COMMAND COUNT but is not dispatched: {frame:?}"
        );
    }
}

/// What a client tells the server about itself, accepted and dropped.
///
/// go-redis and redis-py both send `CLIENT SETINFO` on connect and treat a
/// failure as a connection failure, so the stub has to answer `OK` rather
/// than refuse a subcommand it does nothing with.
#[tokio::test]
async fn client_stubs_return_ok() {
    let (mut r, mut w, _pool) = connected(4);
    let mut out = Vec::new();
    let requests: [&[&str]; 6] = [
        &["CLIENT", "SETINFO", "lib-name", "x"],
        &["CLIENT", "SETNAME", "n"],
        &["client", "setname", "n"],
        &["CLIENT", "anythingelse"],
        &["CLIENT"],
        &["PING"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    assert_eq!(frames[0], Frame::Simple("OK".into()));
    assert_eq!(frames[1], Frame::Simple("OK".into()));
    assert_eq!(frames[2], Frame::Simple("OK".into()), "case-insensitive");
    assert_eq!(
        frames[3],
        Frame::Error("ERR unknown subcommand 'anythingelse'. Try CLIENT HELP.".into())
    );
    assert_eq!(
        frames[4],
        Frame::Error("ERR wrong number of arguments for 'client' command".into())
    );
    assert_eq!(
        frames[5],
        Frame::Simple("PONG".into()),
        "a refused subcommand left the connection unusable"
    );
}

/// `CONFIG GET`, which is how an exporter reads the node's configuration
/// without being told it.
///
/// Every assertion here is about the *set* of pairs a request selects,
/// because that is the whole of what the subcommand does: the globs pick
/// rows out of one fixed table, and the value beside each row is a fact
/// this node already reports elsewhere. `CONFIG GET *` is checked by
/// count against the table itself, so a parameter added there is one this
/// reply has to carry without anyone remembering to come back here.
#[tokio::test]
async fn config_get_answers_the_parameters_a_glob_selects() {
    let pool = ShardPool::spawn(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let mut node = NodeInfo::for_tests();
    node.limit = MemoryLimit {
        ceiling: Some(64 << 20),
        mode: EvictionMode::AllKeysLru,
    };
    tokio::spawn(serve_connection(server, pool, node));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    let requests: [&[&str]; 8] = [
        &["CONFIG", "GET", "maxmemory"],
        &["CONFIG", "GET", "maxmemory*"],
        &["CONFIG", "GET", "*"],
        &["CONFIG", "GET", "nosuch"],
        // Redis 7 takes several globs in one request, and a client that
        // sends two expects the union of what they select.
        &["config", "get", "port", "bind"],
        &["CONFIG", "SET", "maxmemory", "1"],
        &["CONFIG", "GET"],
        &["CONFIG"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    assert_eq!(frames[0], pairs(&[("maxmemory", "67108864")]));
    assert_eq!(
        frames[1],
        pairs(&[
            ("maxmemory", "67108864"),
            ("maxmemory-policy", "allkeys-lru")
        ])
    );
    // Held against the table's own length rather than a number written
    // out by hand: what it pins is that `*` selects every row. It does
    // not pin the values — a row with no value beside it never reaches
    // this assertion, because `config_value` has no arm for it and the
    // connection dies on the panic first.
    let Frame::Array(all) = &frames[2] else {
        panic!("CONFIG GET * answered {:?}", frames[2]);
    };
    assert_eq!(all.len(), CONFIG_PARAMETERS.len() * 2);
    assert_eq!(frames[3], Frame::Array(Vec::new()));
    assert_eq!(
        frames[4],
        pairs(&[("bind", "127.0.0.1"), ("port", "6379")]),
        "two globs select two parameters, in table order"
    );
    assert_eq!(
        frames[5],
        Frame::Error("ERR unknown subcommand 'SET'. Try CONFIG HELP.".into())
    );
    assert_eq!(
        frames[6],
        Frame::Error("ERR wrong number of arguments for 'config|get' command".into())
    );
    assert_eq!(
        frames[7],
        Frame::Error("ERR wrong number of arguments for 'config' command".into())
    );
}

/// `CONFIG GET` selects a parameter without regard to case, and `KEYS`
/// still selects a key with it.
///
/// Redis matches a configuration parameter with its glob matcher's
/// case-folding mode and a key with the same matcher's exact one, so the
/// two commands share an implementation there and differ in one argument.
/// They differ here by where the folding is done — at this call site
/// rather than inside `glob` — and the second half of this test is what
/// says the difference did not leak: a key named in one case is not found
/// in the other, which is a contract no server may break.
#[tokio::test]
async fn config_get_matches_a_parameter_without_case_and_a_key_with_it() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(64 * 1024);
    let mut node = NodeInfo::for_tests();
    node.limit = MemoryLimit {
        ceiling: Some(64 << 20),
        mode: EvictionMode::AllKeysLru,
    };
    tokio::spawn(serve_connection(server, pool, node));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    let requests: [&[&str]; 6] = [
        &["CONFIG", "GET", "MAXMEMORY"],
        &["CONFIG", "GET", "MaxMemory-Policy"],
        // A glob, folded the same way: the pattern's case is not part of
        // what it selects.
        &["CONFIG", "GET", "MAXMEMORY*"],
        &["SET", "Alpha", "1"],
        &["KEYS", "alpha"],
        &["KEYS", "Alpha"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    // The name in the reply is the table's own spelling, not the peer's.
    // Redis is not one answer here and the two versions this project is
    // read against disagree: 6.2.24 echoes its own lower-case spelling
    // for every form, while 8.10.1 echoes the peer's spelling back when
    // the request named a parameter exactly — `CONFIG GET MAXMEMORY`
    // answers `MAXMEMORY` there — and its own only when the request was
    // a glob. Measured on both, 2026-09-10. This server follows 6.2.24:
    // one spelling for every form, which is the one a client reading the
    // pairs back by name can rely on.
    assert_eq!(frames[0], pairs(&[("maxmemory", "67108864")]));
    assert_eq!(frames[1], pairs(&[("maxmemory-policy", "allkeys-lru")]));
    assert_eq!(
        frames[2],
        pairs(&[
            ("maxmemory", "67108864"),
            ("maxmemory-policy", "allkeys-lru")
        ])
    );
    assert_eq!(
        frames[4],
        Frame::Array(Vec::new()),
        "a key walk folded case, and keys are bytes"
    );
    assert_eq!(
        frames[5],
        Frame::Array(vec![Frame::Bulk(b"Alpha".to_vec())])
    );
}

/// The password is never in the reply. Redis reports `requirepass` as an
/// empty string on a node that has one, and an exporter reads the field
/// on every scrape — so the one thing this parameter must never do is
/// answer truthfully.
#[tokio::test]
async fn config_get_never_reports_the_password() {
    let pool = ShardPool::spawn(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(
        server,
        pool,
        node_with_password(b"hunter2"),
    ));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    for parts in [&["AUTH", "hunter2"][..], &["CONFIG", "GET", "requirepass"]] {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, 2).await;
    assert_eq!(frames[0], Frame::Simple("OK".into()));
    assert_eq!(frames[1], pairs(&[("requirepass", "")]));
}

/// `SLOWLOG`, on a node whose slow log recorded nothing and never will.
///
/// The distinction the assertions are about is between empty and absent.
/// An exporter scrapes `SLOWLOG LEN` on every pass and reads an error as
/// a node it could not talk to, while a zero is the node answering that
/// it has nothing slow to show — which, with no handler timed anywhere,
/// is the true reading and not a placeholder.
#[tokio::test]
async fn slowlog_answers_as_a_disabled_slow_log() {
    let (mut r, mut w, _pool) = connected(1);
    let mut out = Vec::new();
    let requests: [&[&str]; 11] = [
        &["SLOWLOG", "GET"],
        &["SLOWLOG", "GET", "128"],
        &["slowlog", "get", "-1"],
        &["SLOWLOG", "LEN"],
        &["SLOWLOG", "RESET"],
        &["SLOWLOG", "HELP"],
        &["SLOWLOG"],
        &["SLOWLOG", "GET", "128", "more"],
        &["SLOWLOG", "GET", "soon"],
        &["SLOWLOG", "LEN", "more"],
        &["SLOWLOG", "RESET", "more"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let empty = Frame::Array(Vec::new());
    assert_eq!(frames[0], empty);
    assert_eq!(
        frames[1], empty,
        "a count selects a prefix of a log with nothing in it"
    );
    assert_eq!(
        frames[2], empty,
        "and so does Redis's spelling of all of it"
    );
    assert_eq!(frames[3], Frame::Integer(0));
    assert_eq!(frames[4], Frame::Simple("OK".into()));
    assert_eq!(
        frames[5],
        Frame::Error("ERR unknown subcommand 'HELP'. Try SLOWLOG HELP.".into())
    );
    assert_eq!(
        frames[6],
        Frame::Error("ERR wrong number of arguments for 'slowlog' command".into())
    );
    assert_eq!(
        frames[7],
        Frame::Error("ERR wrong number of arguments for 'slowlog|get' command".into())
    );
    assert_eq!(
        frames[8],
        Frame::Error(ReplyError::NotAnInteger.wire_text().to_owned()),
        "the count is refused for what it is, before the log it would read"
    );
    assert_eq!(
        frames[9],
        Frame::Error("ERR wrong number of arguments for 'slowlog|len' command".into())
    );
    assert_eq!(
        frames[10],
        Frame::Error("ERR wrong number of arguments for 'slowlog|reset' command".into())
    );
}

/// `LATENCY`, on a node whose latency monitor is off.
///
/// `CONFIG GET latency-monitor-threshold` already reports `0`, which is
/// how Redis spells a monitor that is disabled, and these are the
/// readings that belong beside it: nothing sampled, so no latest reading,
/// no history for any event, and no event a reset can remove.
#[tokio::test]
async fn latency_answers_as_a_disabled_monitor() {
    let (mut r, mut w, _pool) = connected(1);
    let mut out = Vec::new();
    let requests: [&[&str]; 9] = [
        &["LATENCY", "LATEST"],
        &["latency", "history", "command"],
        &["LATENCY", "RESET"],
        &["LATENCY", "RESET", "command", "fork"],
        &["LATENCY", "DOCTOR"],
        &["LATENCY"],
        &["LATENCY", "LATEST", "more"],
        &["LATENCY", "HISTORY"],
        &["LATENCY", "HISTORY", "command", "fork"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let empty = Frame::Array(Vec::new());
    assert_eq!(frames[0], empty);
    assert_eq!(frames[1], empty, "no event has a history to answer with");
    assert_eq!(
        frames[2],
        Frame::Integer(0),
        "the reply counts the events cleared, and none were recorded"
    );
    assert_eq!(frames[3], Frame::Integer(0), "naming events clears no more");
    assert_eq!(
        frames[4],
        Frame::Error("ERR unknown subcommand 'DOCTOR'. Try LATENCY HELP.".into())
    );
    assert_eq!(
        frames[5],
        Frame::Error("ERR wrong number of arguments for 'latency' command".into())
    );
    assert_eq!(
        frames[6],
        Frame::Error("ERR wrong number of arguments for 'latency|latest' command".into())
    );
    assert_eq!(
        frames[7],
        Frame::Error("ERR wrong number of arguments for 'latency|history' command".into())
    );
    assert_eq!(
        frames[8],
        Frame::Error("ERR wrong number of arguments for 'latency|history' command".into()),
        "one event per request, as Redis takes it"
    );
}

/// `LATENCY HISTOGRAM` is answered because it is scraped, not because
/// this node has a histogram to report.
///
/// A stock Prometheus exporter asks for it beside `LATENCY LATEST` on
/// every pass, so a refusal here is an error line per scrape for a
/// command whose honest answer this server already knows. Redis answers a
/// map keyed by command name and leaves out every command it has not
/// tracked; with none tracked the map is empty, which in this protocol is
/// an empty array.
#[tokio::test]
async fn latency_histogram_answers_an_empty_map() {
    let (mut r, mut w, _pool) = connected(1);
    let mut out = Vec::new();
    let requests: [&[&str]; 2] = [
        &["LATENCY", "HISTOGRAM"],
        &["latency", "histogram", "get", "set"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    let empty = Frame::Array(Vec::new());
    assert_eq!(frames[0], empty);
    assert_eq!(
        frames[1], empty,
        "naming commands narrows a map that is already empty"
    );
}

/// A `CONFIG GET` reply written the way it reads: name and value
/// together, rather than a flat list a reader has to pair up by index.
fn pairs(entries: &[(&str, &str)]) -> Frame {
    Frame::Array(
        entries
            .iter()
            .flat_map(|(name, value)| {
                [
                    Frame::Bulk(name.as_bytes().to_vec()),
                    Frame::Bulk(value.as_bytes().to_vec()),
                ]
            })
            .collect(),
    )
}
