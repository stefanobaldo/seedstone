//! `INFO` over a connection: which sections a request selects and what each
//! one reports.

use super::support::{connected, read_frames, req};
use crate::connection::serve_connection;
use crate::info::{errorstats_section, info};
use crate::node::{NodeInfo, RUN_ID_HEX};
use crate::reply::count_error_reply;
use seedstone_core::dict::DictSeed;
use seedstone_core::memory::{EvictionMode, MemoryLimit};
use seedstone_core::shard::{NoTrace, ShardPool};
use seedstone_resp::{Frame, encode};
use std::sync::atomic::Ordering;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn info_memory_reports_what_the_pool_accounts() {
    let pool = ShardPool::spawn(16, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let gauge = pool.memory();
    let mut node = NodeInfo::for_tests();
    node.memory = gauge.clone();
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, node));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    encode(&req(&["SET", "k", "0123456789"]), &mut out);
    encode(&req(&["INFO", "memory"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 2).await;
    let Frame::Bulk(text) = &frames[1] else {
        panic!("INFO answered {:?}", frames[1])
    };
    let text = String::from_utf8(text.clone()).unwrap();
    assert!(text.starts_with("# Memory\r\n"), "{text}");
    let used: u64 = text
        .lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(used, gauge.used());
    // No RSS is measured here, so there is no ratio to report: a field
    // that is always `1.00` reads as a healthy measurement and is not one.
    assert!(!text.contains("mem_fragmentation_ratio"), "{text}");
    assert!(text.contains("used_memory_human:"), "{text}");
}

/// The ceiling and its policy are reported as Redis reports them: `0` for
/// no ceiling, and `noeviction` beside it whatever the mode nominally
/// holds — a policy with no ceiling to reach evicts nothing, so saying so
/// is the honest field.
#[tokio::test]
async fn info_memory_reports_the_ceiling_and_its_policy() {
    // The memory section reaches no shard, so any router serves; this
    // one is the smallest a pool can be.
    let router = ShardPool::spawn(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let mut node = NodeInfo::for_tests();
    node.limit = MemoryLimit {
        ceiling: Some(1 << 20),
        mode: EvictionMode::AllKeysLru,
    };
    let text = info(&router, &node, &[b"memory".to_vec()]).await;
    assert!(text.contains("\r\nmaxmemory:1048576\r\n"), "{text}");
    assert!(text.contains("\r\nmaxmemory_human:1.00M\r\n"), "{text}");
    assert!(
        text.contains("\r\nmaxmemory_policy:allkeys-lru\r\n"),
        "{text}"
    );

    let text = info(&router, &NodeInfo::for_tests(), &[b"memory".to_vec()]).await;
    assert!(text.contains("\r\nmaxmemory:0\r\n"), "{text}");
    assert!(
        text.contains("\r\nmaxmemory_policy:noeviction\r\n"),
        "{text}"
    );
}

/// `evicted_keys` is the sum of what the shards report, and it is a
/// figure this layer cannot hold itself: the count lives beside each
/// shard's dict, so the section is a broadcast rather than a field.
#[tokio::test]
async fn info_stats_reports_the_evictions_the_shards_counted() {
    // A ceiling of what the empty tables already cost, so the first write
    // crosses it and every write after that evicts.
    let pool = ShardPool::spawn_limited(
        4,
        2,
        DictSeed { k0: 1, k1: 2 },
        NoTrace,
        MemoryLimit {
            ceiling: Some(4 * 8 * 24),
            mode: EvictionMode::AllKeysLru,
        },
    );
    let mut node = NodeInfo::for_tests();
    node.memory = pool.memory();
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, node));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    for i in 0..8u8 {
        encode(&req(&["SET", &format!("k{i}"), "0123456789"]), &mut out);
    }
    encode(&req(&["INFO", "stats"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 9).await;
    let Frame::Bulk(text) = &frames[8] else {
        panic!("INFO answered {:?}", frames[8])
    };
    let text = String::from_utf8(text.clone()).unwrap();
    assert!(text.starts_with("# Stats\r\n"), "{text}");
    let evicted: u64 = text
        .lines()
        .find_map(|l| l.strip_prefix("evicted_keys:"))
        .unwrap()
        .parse()
        .unwrap();
    assert!(evicted > 0, "nothing was evicted under a ceiling: {text}");
}

/// Every section a stock Prometheus exporter reads, in one document.
///
/// Written as one test rather than one per section on purpose: the
/// exporter asks for the whole document once and builds its metric
/// families from whatever it finds, so what has to hold is that a single
/// `INFO` carries all of it at once — a section that is right on its own
/// and missing from the whole is a metric family that never appears.
///
/// `INFO all` rather than a bare `INFO`, because `# Commandstats` is
/// outside Redis's default document and an exporter that reads it against
/// a stock Redis must therefore be asking for the whole one. Sending the
/// bare form here would hold this server to a document Redis does not
/// answer, and would pass for a reason the exporter does not depend on.
#[tokio::test]
async fn info_reports_every_section_the_exporter_reads() {
    let pool = ShardPool::spawn(16, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let mut node = NodeInfo::for_tests();
    node.memory = pool.memory();
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, node));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    encode(&req(&["SET", "a", "1"]), &mut out);
    encode(&req(&["SET", "b", "2"]), &mut out);
    encode(&req(&["SET", "c", "3", "EX", "600"]), &mut out);
    encode(&req(&["GET", "a"]), &mut out);
    encode(&req(&["GET", "nosuch"]), &mut out);
    encode(&req(&["INFO", "all"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 6).await;
    let Frame::Bulk(text) = &frames[5] else {
        panic!("INFO answered {:?}", frames[5])
    };
    let text = String::from_utf8(text.clone()).unwrap();
    let has = |line: &str| {
        assert!(
            text.lines().any(|written| written == line),
            "no {line:?} in {text}"
        );
    };
    let field = |name: &str| {
        let prefix = format!("{name}:");
        text.lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap_or_else(|| panic!("no {name} in {text}"))
    };

    has("# Server");
    assert!(!field("redis_version").is_empty());
    has("redis_mode:standalone");
    let run_id = field("run_id");
    assert_eq!(run_id.len(), RUN_ID_HEX, "run_id is {run_id:?}");
    assert!(
        run_id.bytes().all(|b| b.is_ascii_hexdigit()),
        "run_id is {run_id:?}"
    );
    field("process_id").parse::<u32>().unwrap();
    assert!(!field("executable").is_empty());

    has("# Clients");
    has("# Memory");
    // `info_memory_reports_what_the_pool_accounts` makes the same claim
    // of `INFO memory`; this one makes it of the whole document, which is
    // what the exporter asks for. `contains` rather than the line-exact
    // `has` above because the claim is an absence: the name must not
    // appear anywhere, under any spelling of the line.
    assert!(
        !text.contains("mem_fragmentation_ratio"),
        "a ratio nothing measures must not be reported"
    );

    has("# Stats");
    has("keyspace_hits:1");
    has("keyspace_misses:1");
    has("expired_keys:0");
    has("evicted_keys:0");
    has("rejected_connections:0");
    has("total_error_replies:0");
    field("total_connections_received").parse::<u64>().unwrap();
    field("total_commands_processed").parse::<u64>().unwrap();
    assert!(
        field("total_net_input_bytes").parse::<u64>().unwrap() > 0,
        "the commands this connection sent were not counted"
    );
    field("total_net_output_bytes").parse::<u64>().unwrap();

    has("# Keyspace");
    has("db0:keys=3,expires=1,avg_ttl=0");

    has("# Commandstats");
    // A prefix, because what follows is a timing: the call count is the
    // node's, the microseconds are the machine's.
    let counted = |prefix: &str| {
        assert!(
            text.lines().any(|written| written.starts_with(prefix)),
            "no line beginning {prefix:?} in {text}"
        );
    };
    counted("cmdstat_set:calls=3,usec=");
    counted("cmdstat_get:calls=2,usec=");

    has("# Errorstats");
}

/// `errorstats` renders what the edge filed, one row per code, sorted.
#[tokio::test]
async fn info_errorstats_renders_one_row_per_code() {
    // The section reaches no shard, so any router serves.
    let router = ShardPool::spawn(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let node = NodeInfo::for_tests();
    count_error_reply(&node, "NOAUTH Authentication required.");
    count_error_reply(&node, "ERR unknown command 'x'");
    count_error_reply(&node, "ERR wrong number of arguments");
    let text = info(&router, &node, &[b"errorstats".to_vec()]).await;
    assert_eq!(
        text,
        "# Errorstats\r\nerrorstat_ERR:count=2\r\nerrorstat_NOAUTH:count=1\r\n"
    );
}

/// A bare header, and nothing after it. `INFO errorstats` on a fresh
/// `redis:6-alpine` (`redis_version:6.2.24`) answers the bulk
/// `# Errorstats\r\n`: Redis separates sections with a blank line rather
/// than terminating each with one, so the document ends on its last field.
/// `redis:8-alpine` (`redis_version:8.10.1`) answers the same.
#[tokio::test]
async fn info_errorstats_on_a_fresh_node_is_a_bare_header() {
    let router = ShardPool::spawn(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let text = info(&router, &NodeInfo::for_tests(), &[b"errorstats".to_vec()]).await;
    assert_eq!(text, "# Errorstats\r\n");
}

/// `# Keyspace` with no `db0` line, and no trailing blank. Measured
/// against `redis:6-alpine` (`redis_version:6.2.24`), which answers 12
/// bytes here where this server answered 14.
#[tokio::test]
async fn info_keyspace_on_an_empty_node_has_no_db_line() {
    let router = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let text = info(&router, &NodeInfo::for_tests(), &[b"keyspace".to_vec()]).await;
    assert_eq!(text, "# Keyspace\r\n");
}

/// Two sections join on exactly one blank line, and the document still
/// ends on its last field. This is the property a trailing trim can
/// break, so it is asserted separately from either section's own shape.
#[tokio::test]
async fn info_separates_sections_with_one_blank_line_and_ends_without_one() {
    let router = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let text = info(
        &router,
        &NodeInfo::for_tests(),
        &[b"keyspace".to_vec(), b"errorstats".to_vec()],
    )
    .await;
    assert_eq!(text, "# Keyspace\r\n\r\n# Errorstats\r\n");
}

/// An unrecognised command reaches the counting site, which is where the
/// line is written beside the count.
///
/// This is the end-to-end half: the drain really carries a label from
/// `frame_to_action` to `emit_chunk`, and the counter really moves for a
/// name no table holds. The line itself goes to stderr, so what it
/// contains is `error_reply_line`'s tests to assert and this one's to
/// reach.
#[tokio::test]
async fn an_unknown_command_both_counts_and_names_itself() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let node = NodeInfo::for_tests();
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, node.clone()));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    encode(&req(&["dbsizde"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 1).await;
    assert!(
        matches!(&frames[0], Frame::Error(e) if e.contains("unknown command 'dbsizde'")),
        "{frames:?}"
    );
    assert_eq!(node.error_replies.load(Ordering::Relaxed), 1);
    let text = errorstats_section(&node);
    assert!(text.contains("errorstat_ERR:count=1"), "{text}");
}

/// `commandstats` counts the request the peer made, not the commands the
/// edge split it into — and counts a broadcast once rather than once per
/// shard.
#[tokio::test]
async fn commandstats_counts_requests_rather_than_the_commands_they_became() {
    let pool = ShardPool::spawn(16, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, NodeInfo::for_tests()));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    encode(&req(&["MGET", "a", "b", "c"]), &mut out);
    encode(&req(&["DBSIZE"]), &mut out);
    encode(&req(&["PING"]), &mut out);
    encode(&req(&["INFO", "commandstats"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 4).await;
    let Frame::Bulk(text) = &frames[3] else {
        panic!("INFO answered {:?}", frames[3])
    };
    let text = String::from_utf8(text.clone()).unwrap();
    assert!(text.contains("cmdstat_mget:calls=1,"), "{text}");
    assert!(
        text.contains("cmdstat_get:calls=3,"),
        "the three GETs the fan-out ran are the shards' to count: {text}"
    );
    assert!(
        text.contains("cmdstat_dbsize:calls=1,"),
        "one DBSIZE reached sixteen shards and is still one call: {text}"
    );
    assert!(text.contains("cmdstat_ping:calls=1,"), "{text}");
    assert!(
        !text.contains("cmdstat_ttl"),
        "a command nobody sent has no line: {text}"
    );
}

/// The three fields Redis prints, in Redis's order — the exporter this
/// project is gated on reads them by position, so the order is a contract
/// and not a presentation choice.
///
/// `usec_per_call` is asserted to parse rather than to hold a value: what
/// a `SET` costs on the machine running this test is not something a test
/// may pin. What it may pin is that the field is a number, computed from
/// the two beside it.
#[tokio::test]
async fn commandstats_carries_usec_and_usec_per_call_in_redis_order() {
    let pool = ShardPool::spawn(16, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(4096);
    tokio::spawn(serve_connection(server, pool, NodeInfo::for_tests()));
    let (mut r, mut w) = tokio::io::split(client);
    let mut out = Vec::new();
    for key in ["a", "b", "c"] {
        encode(&req(&["SET", key, "v"]), &mut out);
    }
    encode(&req(&["INFO", "commandstats"]), &mut out);
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();
    let frames = read_frames(&mut r, 4).await;
    let Frame::Bulk(text) = &frames[3] else {
        panic!("INFO answered {:?}", frames[3])
    };
    let text = String::from_utf8(text.clone()).unwrap();
    let line = text
        .lines()
        .find(|line| line.starts_with("cmdstat_set:"))
        .unwrap_or_else(|| panic!("no SET line: {text}"));
    let fields: Vec<&str> = line.split(':').nth(1).unwrap().split(',').collect();
    assert_eq!(fields.len(), 3, "{line}");
    assert_eq!(fields[0], "calls=3", "{line}");
    let usec: u64 = fields[1]
        .strip_prefix("usec=")
        .unwrap_or_else(|| panic!("{line}"))
        .parse()
        .unwrap();
    let per_call: f64 = fields[2]
        .strip_prefix("usec_per_call=")
        .unwrap_or_else(|| panic!("{line}"))
        .parse()
        .unwrap();
    assert!(per_call >= 0.0, "{line}");
    // Two decimals of the quotient, which is what Redis prints and what an
    // exporter reading the field back expects to be able to divide again.
    #[allow(
        clippy::cast_precision_loss,
        reason = "the same quotient the renderer computes, over the same three calls"
    )]
    let expected = usec as f64 / 3.0;
    assert!((per_call - expected).abs() < 0.01, "{line}");
}

/// `INFO` is the first thing an operator asks a server, and the sections
/// it names are what tooling reads. Only the fields this node can answer
/// truthfully are printed; a section nobody has is an empty bulk rather
/// than an error, exactly as Redis answers one.
#[tokio::test]
async fn info_prints_the_minimal_sections() {
    let (mut r, mut w, _pool) = connected(4);
    let mut out = Vec::new();
    let requests: [&[&str]; 9] = [
        &["INFO"],
        &["INFO", "server"],
        // Section names are case-insensitive, as Redis takes them.
        &["INFO", "CLIENTS"],
        // Several at once, answered in the server's own order rather than
        // the order they were asked in — again as Redis answers them.
        &["INFO", "clients", "server"],
        &["INFO", "nosuch"],
        // The three names that ask for no section but the whole document.
        // `INFO all` is what an operator types and what a metrics exporter
        // scrapes, so answering it as an unknown section would answer the
        // command's most common form with nothing.
        &["INFO", "all"],
        &["INFO", "DEFAULT"],
        &["INFO", "everything"],
        // And one of them beats the section names beside it, in either
        // position, as measured on Redis.
        &["INFO", "nosuch", "all"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    // The field names are asserted in full, `redis_` prefix and all: that
    // prefix *is* the contract this document owes its readers — see
    // [`info`] — so a test matching the bare `version:` would go on passing
    // after a rename that broke every exporter reading it.
    let whole_document = [
        "# Server",
        "redis_version:",
        "redis_mode:standalone",
        "tcp_port:",
        "uptime_in_seconds:",
        "# Clients",
        "connected_clients:",
    ];
    let everything = bulk_text(&frames[0]);
    for field in whole_document {
        assert!(
            everything.contains(field),
            "INFO printed no {field}: {everything:?}"
        );
    }

    let server = bulk_text(&frames[1]);
    assert!(server.contains("# Server"), "{server:?}");
    assert!(
        !server.contains("# Clients"),
        "INFO server printed a section nobody asked for: {server:?}"
    );

    let clients = bulk_text(&frames[2]);
    assert!(clients.contains("# Clients"), "{clients:?}");
    assert!(
        !clients.contains("# Server"),
        "INFO CLIENTS printed a section nobody asked for: {clients:?}"
    );

    let both = bulk_text(&frames[3]);
    assert_eq!(
        both.find("# Server")
            .map(|at| at < both.find("# Clients").unwrap()),
        Some(true),
        "two named sections must come back, in the server's order: {both:?}"
    );

    assert_eq!(
        frames[4],
        Frame::Bulk(Vec::new()),
        "an unknown section is an empty bulk, not an error"
    );

    // Compared field by field rather than against `everything` verbatim:
    // `uptime_in_seconds` is free to tick between two frames of one
    // pipeline, and a test that demanded two identical documents would be
    // a flake waiting for a slow machine.
    for (i, name) in [
        (5, "all"),
        (6, "DEFAULT"),
        (7, "everything"),
        (8, "nosuch all"),
    ] {
        let text = bulk_text(&frames[i]);
        for field in whole_document {
            assert!(
                text.contains(field),
                "INFO {name} must answer the whole document, and printed no {field}: {text:?}"
            );
        }
    }
}

/// `INFO` and `INFO default` answer Redis's default document, which does
/// not carry `# Commandstats`; `all` and `everything` do.
///
/// The distinction is Redis's and it is the one an argumentless `INFO`
/// depends on: a bare `INFO` is `default` under another spelling, so the
/// section a client did not ask for is the one it must not receive. A
/// section named outright is served whatever else the list says, because
/// Redis resolves the list as a union rather than as a filter — which is
/// what makes `INFO default commandstats` a document with both halves.
#[tokio::test]
async fn info_default_leaves_out_the_sections_redis_leaves_out() {
    let (mut r, mut w, _pool) = connected(4);
    let mut out = Vec::new();
    // A command first, so the section has a line in it as well as a
    // header — the header alone would pass this test against a server
    // that had stopped counting.
    let requests: [&[&str]; 8] = [
        &["SET", "a", "1"],
        &["INFO"],
        &["INFO", "default"],
        &["INFO", "all"],
        &["INFO", "everything"],
        &["INFO", "commandstats"],
        // Union, not filter: `default` names one set and `commandstats`
        // adds itself to it.
        &["INFO", "default", "commandstats"],
        // And the default document is not empty of everything else.
        &["INFO", "DEFAULT"],
    ];
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    w.write_all(&out).await.unwrap();
    w.flush().await.unwrap();

    let frames = read_frames(&mut r, requests.len()).await;
    for (i, name) in [(1, "INFO"), (2, "INFO default")] {
        let text = bulk_text(&frames[i]);
        assert!(
            !text.contains("# Commandstats"),
            "{name} carried a section Redis omits from its default document: {text:?}"
        );
    }
    for (i, name) in [
        (3, "INFO all"),
        (4, "INFO everything"),
        (5, "INFO commandstats"),
        (6, "INFO default commandstats"),
    ] {
        let text = bulk_text(&frames[i]);
        assert!(
            text.contains("cmdstat_set:calls="),
            "{name} must carry the per-command section: {text:?}"
        );
    }
    // The default document keeps every section that *is* in Redis's
    // default set, so this test cannot pass by answering nothing.
    let default = bulk_text(&frames[7]);
    for section in ["# Server", "# Clients", "# Memory", "# Stats", "# Keyspace"] {
        assert!(
            default.contains(section),
            "INFO DEFAULT dropped {section}: {default:?}"
        );
    }
}

/// The text of a bulk reply, for the assertions that read `INFO`.
fn bulk_text(frame: &Frame) -> String {
    match frame {
        Frame::Bulk(bytes) => String::from_utf8(bytes.clone()).expect("INFO is not UTF-8"),
        other => panic!("expected a bulk reply, got {other:?}"),
    }
}
