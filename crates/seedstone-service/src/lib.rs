//! SeedStone's connection layer: RESP2 frames in, [`Command`]s out, replies back.
//!
//! Netless by construction. Nothing here opens a socket — [`serve_connection`]
//! is generic over its transport, which is what lets the production binary
//! hand it a `tokio::net::TcpStream` and the simulator hand it a simulated
//! one, with the same code in between.
//!
//! [`serve_connection`] is generic over its [`Router`] too, which is what lets
//! the simulator run the real connection code against a deliberately racy one.
//! Nothing here knows whether it is talking to a socket or to a `duplex` pipe
//! in a test.
//!
//! # What this layer is responsible for
//!
//! It is the only place where bytes a peer chose become something the rest of

mod auth;
mod connection;
mod containers;
mod dispatch;
mod expiry;
mod fan_out;
mod hello;
mod info;
mod node;
mod options;
mod reply;
mod walk;

pub use auth::{AUTH_NOT_CONFIGURED, NOAUTH, NOAUTH_HELLO, Secret, WRONGPASS};
pub use connection::{IDLE_SHED_AFTER, MAX_REQUEST_BYTES, serve_connection};
pub use dispatch::command_names;
pub use fan_out::{INVALID_CURSOR, KEYS_REPLY_BYTES, KEYS_TOO_LARGE, WALK_STEP_BUCKETS};
pub use hello::NOPROTO;
pub use node::{EDGE_NAMES, NodeInfo, RUN_ID_HEX};
pub use options::SYNTAX_ERROR;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{
        CHUNK_COMMANDS, IDLE_SHED_AFTER, MAX_REQUEST_BYTES, READ_CEILING, READ_FLOOR,
        READ_QUIET_READS, REPLY_HIGH_WATER, REPLY_SHED, append_frame, flush_replies,
        resize_connection_buffers, serve_connection, serve_connection_limited,
    };
    use crate::containers::CONFIG_PARAMETERS;
    use crate::dispatch::{COMMANDS, frame_to_action};
    use crate::fan_out::keys;
    use crate::info::{errorstats_section, info};
    use crate::options::wrong_arity;
    use crate::reply::UNRENDERABLE_REPLY;
    use crate::reply::{count_error_reply, reply_to_frame};
    use crate::walk::{pack_cursor, unpack_cursor};
    use seedstone_core::dict::DictSeed;
    use seedstone_core::memory::{EvictionMode, MemoryLimit};
    use seedstone_core::shard::{Command, Reply, ReplyError, Router};
    use seedstone_core::shard::{NoTrace, ShardPool};
    use seedstone_resp::{Decoder, DecoderLimits, Frame, encode};
    use seedstone_resp::{MAX_ARRAY_LEN, MAX_BULK_LEN, parse};
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn serves_resp_over_a_duplex_stream() {
        let pool = ShardPool::spawn(16, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(4096);
        tokio::spawn(serve_connection(server, pool, NodeInfo::for_tests()));
        let (mut r, mut w) = tokio::io::split(client);
        let mut out = Vec::new();
        encode(&req(&["SET", "k", "v"]), &mut out);
        encode(&req(&["GET", "k"]), &mut out);
        encode(&req(&["NOPE"]), &mut out);
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();
        let frames = read_frames(&mut r, 3).await;
        assert_eq!(frames[0], Frame::Simple("OK".into()));
        assert_eq!(frames[1], Frame::Bulk(b"v".to_vec()));
        assert!(matches!(&frames[2], Frame::Error(e) if e.contains("unknown command")));
    }

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

    /// The label the drain carries to the log, for each of the four shapes a
    /// frame can have: a recognised command, a recognised command its handler
    /// refuses, a name no table holds, and a frame with no command in it.
    ///
    /// The wrong-arity case is the one worth having. The label is set before
    /// the handler runs precisely so that a refusal the handler returns is
    /// attributed, and moving that assignment below the handler would leave
    /// `wrong number of arguments` anonymous while every other test still
    /// passed.
    #[test]
    fn the_label_names_the_command_behind_each_shape_of_frame() {
        let node = NodeInfo::for_tests();
        let named = |frame| {
            let (_, label) = frame_to_action(frame, &node);
            let mut out = String::new();
            label.render(&mut out);
            out
        };

        assert_eq!(named(req(&["GET", "k"])), "get");
        assert_eq!(
            named(req(&["MGET", "a", "b"])),
            "mget",
            "the request the peer made, not the GETs it becomes"
        );
        assert_eq!(
            named(req(&["SET", "k"])),
            "set",
            "a wrong arity is a refusal, and it names its command"
        );
        assert_eq!(named(req(&["dbsizde"])), "dbsizde");
        assert_eq!(
            named(Frame::Integer(1)),
            "",
            "no array of bulk strings, so no command to name"
        );
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
        let refused =
            |name: &str| Frame::Error(format!("ERR invalid expire time in '{name}' command"));
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

    /// A router that cannot be dispatched to.
    ///
    /// The whole claim of this layer is that a connection-level command is
    /// answered here and never becomes a message to a shard. Asserting the
    /// reply alone would not show that: a router *could* answer `PING`
    /// correctly and the test would not notice. This one makes the trip
    /// impossible instead.
    #[derive(Clone)]
    struct UnreachableRouter;

    impl Router for UnreachableRouter {
        async fn dispatch(&self, _cmd: Command) -> Reply {
            unreachable!("a connection command reached the router")
        }

        fn shards(&self) -> u16 {
            1
        }

        async fn dispatch_at(&self, _shard: u16, _cmd: Command) -> Reply {
            unreachable!("a connection command reached the router")
        }

        async fn dispatch_every(&self, _cmd: Command) -> Vec<Reply> {
            unreachable!("a connection command reached the router")
        }
    }

    #[tokio::test]
    async fn connection_commands_never_reach_the_router() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(serve_connection(
            server,
            UnreachableRouter,
            NodeInfo::for_tests(),
        ));
        let (mut r, mut w) = tokio::io::split(client);

        let mut out = Vec::new();
        for parts in [
            &["PING"][..],
            &["ping"],
            &["PING", "hi"],
            &["PING", "a", "b"],
            &["ECHO", "x"],
            &["ECHO"],
            &["HELLO"],
            &["HELLO", "2"],
            &["HELLO", "3"],
            &["HELLO", "notanumber"],
            &["HELLO", "2", "AUTH", "user", "pass"],
            &["HELLO", "2", "SETNAME", "client"],
            // Last: it closes the connection.
            &["QUIT"],
        ] {
            encode(&req(parts), &mut out);
        }
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        let frames = read_frames(&mut r, 13).await;
        assert_eq!(frames[0], Frame::Simple("PONG".into()));
        assert_eq!(frames[1], Frame::Simple("PONG".into()), "case-insensitive");
        assert_eq!(frames[2], Frame::Bulk(b"hi".to_vec()));
        assert!(matches!(&frames[3], Frame::Error(e) if e.contains("wrong number of arguments")));
        assert_eq!(frames[4], Frame::Bulk(b"x".to_vec()));
        assert!(matches!(&frames[5], Frame::Error(e) if e.contains("wrong number of arguments")));
        assert_eq!(frames[6], frames[7], "HELLO and HELLO 2 answer the same");
        assert_eq!(frames[8], Frame::Error(NOPROTO.into()));
        // The other two `HELLO` refusals are contracts with real clients in the
        // same way `NOPROTO` is, so they are written out rather than matched on
        // loosely.
        assert_eq!(
            frames[9],
            Frame::Error("ERR Protocol version is not an integer or out of range".into())
        );
        // `AUTH` is an option this handshake now takes, so what refuses it
        // here is the node having no password to check it against — not the
        // grammar. `SETNAME` is the option that is still not offered, and it
        // is refused rather than ignored for the reason it always was: a
        // client must not be able to believe it took effect.
        assert_eq!(frames[10], Frame::Error(AUTH_NOT_CONFIGURED.into()));
        assert_eq!(
            frames[11],
            Frame::Error("ERR Syntax error in HELLO option 'SETNAME'".into())
        );
        assert_eq!(
            frames[12],
            Frame::Simple("OK".into()),
            "QUIT is acknowledged"
        );

        // ...and then the server goes, without waiting for the peer.
        let mut rest = Vec::new();
        r.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty(), "the server kept talking after QUIT");
    }

    /// Counts how many times the connection flushes and records the largest
    /// single write, delegating everything else.
    ///
    /// The write size is what makes the accumulation bound observable from
    /// outside: the reply buffer is the connection's own, but every byte that
    /// reaches the peer passes through here, so the largest write is exactly
    /// the most one drain ever held.
    struct FlushCounting<S> {
        inner: S,
        flushes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        max_write: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl<S: AsyncRead + Unpin> AsyncRead for FlushCounting<S> {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl<S: AsyncWrite + Unpin> AsyncWrite for FlushCounting<S> {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            self.max_write
                .fetch_max(buf.len(), std::sync::atomic::Ordering::Relaxed);
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            self.flushes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    /// A router that records the size of every batch it is handed, and
    /// otherwise is a [`ShardPool`].
    ///
    /// The chunk bound is a property of the connection loop, not of anything
    /// the peer can see: two chunks and one chunk produce the same bytes in
    /// the same order. Inferring it from writes would be reading the
    /// transport's segmentation instead. This sits where the bound actually
    /// applies and writes down what it saw.
    #[derive(Clone)]
    struct BatchSizes {
        sizes: std::sync::Arc<std::sync::Mutex<Vec<usize>>>,
        inner: ShardPool,
    }

    impl Router for BatchSizes {
        async fn dispatch(&self, cmd: Command) -> Reply {
            self.inner.dispatch(cmd).await
        }

        fn shards(&self) -> u16 {
            self.inner.shards()
        }

        async fn dispatch_at(&self, shard: u16, cmd: Command) -> Reply {
            self.inner.dispatch_at(shard, cmd).await
        }

        async fn dispatch_many(&self, cmds: Vec<Command>) -> Vec<Reply> {
            self.sizes.lock().expect("sizes mutex").push(cmds.len());
            self.inner.dispatch_many(cmds).await
        }

        async fn dispatch_every(&self, cmd: Command) -> Vec<Reply> {
            self.inner.dispatch_every(cmd).await
        }
    }

    /// A drain longer than one chunk dispatches mid-drain instead of holding
    /// every command until the decoder runs dry.
    ///
    /// This is [`CHUNK_COMMANDS`]' half of the accumulation bound, the
    /// command-side twin of the byte-side one
    /// [`a_drain_writes_before_it_accumulates_without_bound`] holds. Both
    /// halves are asserted, not one: a drain of a full [`READ_CEILING`] of
    /// tiny commands earns almost no reply bytes, so the byte mark would never
    /// fire and an unbounded batch would sail past it.
    ///
    /// The two assertions are deliberately different in kind. *Every batch is
    /// within the mark* is the bound itself, and it is what would fail if the
    /// mid-drain close were deleted. *Some batch is exactly the mark* is what
    /// says the bound was reached rather than merely respected — without it a
    /// test whose reads happened to be small would pass while proving nothing.
    #[tokio::test]
    async fn a_long_pipeline_is_dispatched_in_bounded_chunks() {
        /// Enough requests that the connection's read buffer climbs from
        /// [`READ_FLOOR`] to [`READ_CEILING`] — the climb costs about a
        /// ceiling's worth of bytes on its own — and then fills it, so at
        /// least one drain carries far more than one chunk. A request below is
        /// a little over 32 bytes on the wire.
        const REQUESTS: usize = 4 * READ_CEILING / 32;

        let sizes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let router = BatchSizes {
            sizes: std::sync::Arc::clone(&sizes),
            inner: ShardPool::spawn(16, 4, DictSeed { k0: 8, k1: 8 }, NoTrace),
        };
        let (client, server) = tokio::io::duplex(4 * 1024 * 1024);
        tokio::spawn(serve_connection(server, router, NodeInfo::for_tests()));
        let (mut r, mut w) = tokio::io::split(client);

        let mut out = Vec::new();
        for i in 0..REQUESTS {
            encode(&req(&["SET", &format!("key:{i}"), "v"]), &mut out);
        }
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        let frames = read_frames(&mut r, REQUESTS).await;
        assert!(frames.iter().all(|f| *f == Frame::Simple("OK".into())));

        let sizes = sizes.lock().expect("sizes mutex").clone();
        assert_eq!(
            sizes.iter().sum::<usize>(),
            REQUESTS,
            "every command must be dispatched exactly once"
        );
        assert!(
            sizes.iter().all(|&size| size <= CHUNK_COMMANDS),
            "a batch of {} commands passed the mark of {CHUNK_COMMANDS}",
            sizes.iter().copied().max().unwrap_or(0)
        );
        assert!(
            sizes.contains(&CHUNK_COMMANDS),
            "no chunk ever closed mid-drain, so the bound was never reached"
        );
    }

    /// An `MGET` naming more keys than a chunk may hold is dispatched in
    /// slices, and answers as if it had not been.
    ///
    /// Arity here is bounded only by the protocol's array limit, so without
    /// the slicing one request could hand a single executor a quarter of a
    /// million commands — and an executor applies an envelope without yielding
    /// between them, which is the delay [`CHUNK_COMMANDS`] exists to bound. So
    /// both halves are held: the array is one entry per argument in argument
    /// order *across the slice boundaries*, including the null of a key that
    /// was never set, and no batch the router was handed exceeds the mark.
    ///
    /// The batch sizes are asserted exactly rather than as a ceiling. A
    /// ceiling alone would pass if the fold dispatched one command at a time,
    /// which is the shape this replaced and the one that gave up the pass per
    /// executor.
    #[tokio::test]
    async fn a_long_mget_is_dispatched_in_bounded_slices() {
        /// Two full slices and a remainder, so the boundary is crossed twice
        /// and the last slice is short.
        const KEYS: usize = 2 * CHUNK_COMMANDS + 3;
        /// A key inside the second slice that is never written, so a null has
        /// to hold its slot on the far side of a boundary.
        const MISSING: usize = CHUNK_COMMANDS + 2;

        let sizes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let router = BatchSizes {
            sizes: std::sync::Arc::clone(&sizes),
            inner: ShardPool::spawn(16, 4, DictSeed { k0: 8, k1: 8 }, NoTrace),
        };
        let (client, server) = tokio::io::duplex(1 << 20);
        tokio::spawn(serve_connection(server, router, NodeInfo::for_tests()));
        let (mut r, mut w) = tokio::io::split(client);

        let mut out = Vec::new();
        for i in (0..KEYS).filter(|&i| i != MISSING) {
            encode(
                &req(&["SET", &format!("key:{i}"), &i.to_string()]),
                &mut out,
            );
        }
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();
        let written = read_frames(&mut r, KEYS - 1).await;
        assert!(written.iter().all(|f| *f == Frame::Simple("OK".into())));
        // The writes had chunks of their own, and they are not what is under
        // test here.
        sizes.lock().expect("sizes mutex").clear();

        let mut parts = vec!["MGET".to_owned()];
        parts.extend((0..KEYS).map(|i| format!("key:{i}")));
        let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
        out.clear();
        encode(&req(&parts), &mut out);
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        let frames = read_frames(&mut r, 1).await;
        let expected: Vec<Frame> = (0..KEYS)
            .map(|i| {
                if i == MISSING {
                    Frame::Null
                } else {
                    Frame::Bulk(i.to_string().into_bytes())
                }
            })
            .collect();
        assert_eq!(frames[0], Frame::Array(expected));
        assert_eq!(
            *sizes.lock().expect("sizes mutex"),
            vec![CHUNK_COMMANDS, CHUNK_COMMANDS, KEYS - 2 * CHUNK_COMMANDS],
            "the fan-out must reach the router in slices of at most {CHUNK_COMMANDS}"
        );
    }

    /// A router that hands back fewer replies than commands is refused, not
    /// folded into a shorter array.
    ///
    /// Both routers in this workspace answer one reply per command, so this
    /// holds a contract rather than fixing a bug — and it is worth holding
    /// because the failure it prevents is silent. django-redis builds its
    /// `get_many` as `dict(zip(keys, values))`, so a two-key `MGET` answered
    /// with a one-entry array becomes a mapping of one key: the key that fell
    /// off the end reads as an ordinary cache miss, the client refills it, and
    /// nothing on the wire distinguishes that from a key that really was not
    /// there. An error is the only answer a peer can act on.
    #[tokio::test]
    async fn a_short_reply_vector_is_refused_rather_than_shortening_the_array() {
        /// Its pool, minus the last reply of every batch.
        #[derive(Clone)]
        struct ShortByOne {
            inner: ShardPool,
        }

        impl Router for ShortByOne {
            async fn dispatch(&self, cmd: Command) -> Reply {
                self.inner.dispatch(cmd).await
            }

            fn shards(&self) -> u16 {
                self.inner.shards()
            }

            async fn dispatch_at(&self, shard: u16, cmd: Command) -> Reply {
                self.inner.dispatch_at(shard, cmd).await
            }

            async fn dispatch_many(&self, cmds: Vec<Command>) -> Vec<Reply> {
                let mut replies = self.inner.dispatch_many(cmds).await;
                replies.pop();
                replies
            }

            async fn dispatch_every(&self, cmd: Command) -> Vec<Reply> {
                self.inner.dispatch_every(cmd).await
            }
        }

        let router = ShortByOne {
            inner: ShardPool::spawn(4, 2, DictSeed { k0: 5, k1: 7 }, NoTrace),
        };
        let (client, server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(serve_connection(server, router, NodeInfo::for_tests()));
        let (mut r, mut w) = tokio::io::split(client);

        let mut out = Vec::new();
        encode(&req(&["MGET", "a", "b"]), &mut out);
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        let frames = read_frames(&mut r, 1).await;
        assert_eq!(
            frames[0],
            Frame::Error(UNRENDERABLE_REPLY.into()),
            "a short reply vector reached the peer as an array"
        );
    }

    /// A one-key `DEL` travels in the drain's batch instead of closing the
    /// chunk in front of it.
    ///
    /// This is what [`Fold::is_identity_on_one`] buys, and the replies cannot
    /// show it: a one-key `DEL` answers `:1` whether it went with the batch or
    /// fanned out alone. What changes is how many messages the pool is handed
    /// — one batch of two here, against a batch of one and a separate dispatch
    /// behind it if the shortcut were dropped. [`BatchSizes`] is where that
    /// difference is visible, so it is where it is held.
    #[tokio::test]
    async fn a_one_key_del_travels_in_the_batch() {
        let sizes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let router = BatchSizes {
            sizes: std::sync::Arc::clone(&sizes),
            inner: ShardPool::spawn(16, 4, DictSeed { k0: 8, k1: 8 }, NoTrace),
        };
        let (client, server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(serve_connection(server, router, NodeInfo::for_tests()));
        let (mut r, mut w) = tokio::io::split(client);

        let mut out = Vec::new();
        for parts in [&["SET", "a", "1"][..], &["DEL", "a"]] {
            encode(&req(parts), &mut out);
        }
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        let frames = read_frames(&mut r, 2).await;
        assert_eq!(frames[0], Frame::Simple("OK".into()));
        assert_eq!(frames[1], Frame::Integer(1));
        assert_eq!(
            *sizes.lock().expect("sizes mutex"),
            vec![2],
            "the SET and the DEL must reach the pool as one batch"
        );
    }

    /// One drain of a pipelined batch is one flush, not one per reply.
    ///
    /// The syscall a flush becomes is per-batch work billed per command
    /// otherwise, and the invariant the simulator needs is narrower than the
    /// per-reply flush that used to provide it: never park on `read` with a
    /// reply still buffered. A drain only ends when the decoder holds no
    /// complete frame, so flushing there is exactly that invariant and nothing
    /// more.
    #[tokio::test]
    async fn a_pipelined_batch_is_flushed_once_per_drain() {
        let (mut client, server) = tokio::io::duplex(1 << 20);
        let flushes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let transport = FlushCounting {
            inner: server,
            flushes: std::sync::Arc::clone(&flushes),
            max_write: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        // PING resolves in the service layer, so the router must stay
        // unreached — a reply that took the shard round trip would let the
        // drain end early and flush more than once for reasons unrelated to
        // the placement under test.
        tokio::spawn(serve_connection(
            transport,
            UnreachableRouter,
            NodeInfo::for_tests(),
        ));

        let mut batch = Vec::new();
        for _ in 0..64 {
            encode(&req(&["PING"]), &mut batch);
        }
        client.write_all(&batch).await.unwrap();
        client.flush().await.unwrap();

        let mut got = Vec::new();
        while got.len() < 64 * b"+PONG\r\n".len() {
            let mut chunk = [0u8; 4096];
            let n = client.read(&mut chunk).await.unwrap();
            assert!(n > 0, "server hung up mid-batch");
            got.extend_from_slice(&chunk[..n]);
        }
        assert_eq!(got, b"+PONG\r\n".repeat(64));
        assert_eq!(
            flushes.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "one drain of 64 pipelined commands must flush once, not per reply"
        );
    }

    /// A drain writes before it accumulates without bound.
    ///
    /// Flushing at the drain boundary is what makes a pipelined batch cost one
    /// syscall pair instead of one per reply — but a drain ends only when the
    /// decoder holds no complete frame, and the decoder can hold a whole
    /// [`READ_CEILING`] of pipelined requests. Without a high-water mark the
    /// reply buffer grows to hold *every* reply that batch earns, where the
    /// per-reply flush it replaced held one. [`REPLY_HIGH_WATER`] bounds it,
    /// and the bound is observable from the peer's side: the largest single
    /// write is the most one drain ever held.
    #[tokio::test]
    async fn a_drain_writes_before_it_accumulates_without_bound() {
        /// Big enough that a handful of replies crosses the mark, small enough
        /// that the mark is crossed by accumulation rather than by one reply.
        const VALUE: usize = 64 * 1024;
        const READS: usize = 8;

        let pool = ShardPool::spawn(16, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(4 * 1024 * 1024);
        let flushes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_write = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let transport = FlushCounting {
            inner: server,
            flushes: std::sync::Arc::clone(&flushes),
            max_write: std::sync::Arc::clone(&max_write),
        };
        tokio::spawn(serve_connection(transport, pool, NodeInfo::for_tests()));
        let (mut r, mut w) = tokio::io::split(client);

        let value = "v".repeat(VALUE);
        let mut out = Vec::new();
        encode(&req(&["SET", "k", &value]), &mut out);
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();
        assert_eq!(read_frames(&mut r, 1).await[0], Frame::Simple("OK".into()));

        // The `SET` is its own traffic; only the pipelined batch is under test.
        flushes.store(0, std::sync::atomic::Ordering::Relaxed);
        max_write.store(0, std::sync::atomic::Ordering::Relaxed);

        out.clear();
        for _ in 0..READS {
            encode(&req(&["GET", "k"]), &mut out);
        }
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        let frames = read_frames(&mut r, READS).await;
        assert!(
            frames
                .iter()
                .all(|frame| *frame == Frame::Bulk(value.as_bytes().to_vec())),
            "every reply of the batch must arrive whole and in order"
        );

        let largest = max_write.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            largest <= REPLY_HIGH_WATER + VALUE + 64,
            "one drain accumulated {largest} bytes; the mark is {REPLY_HIGH_WATER} \
             plus at most the one reply that crossed it"
        );
        assert!(
            flushes.load(std::sync::atomic::Ordering::Relaxed) >= 2,
            "{READS} replies of {VALUE} B must cross the mark and write mid-drain"
        );
    }

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

    /// A connection command sits in the same stream as a keyed one, and the
    /// pipeline must not reorder or lose either.
    #[tokio::test]
    async fn connection_and_keyed_commands_interleave_in_one_pipeline() {
        let (mut r, mut w, _pool) = connected(8);
        let mut out = Vec::new();
        for parts in [
            &["HELLO", "2"][..],
            &["SET", "k", "v"],
            &["PING"],
            &["GET", "k"],
            &["ECHO", "done"],
        ] {
            encode(&req(parts), &mut out);
        }
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        let frames = read_frames(&mut r, 5).await;
        assert!(matches!(frames[0], Frame::Array(_)));
        assert_eq!(frames[1], Frame::Simple("OK".into()));
        assert_eq!(frames[2], Frame::Simple("PONG".into()));
        assert_eq!(frames[3], Frame::Bulk(b"v".to_vec()));
        assert_eq!(frames[4], Frame::Bulk(b"done".to_vec()));
    }

    /// Replies come back in request order even though keyed commands scatter
    /// across executors and connection commands never leave the connection —
    /// the fourth ordering constraint, the one the chunked drain adds.
    ///
    /// The three the drain already held are about *when* a write happens. This
    /// one is about *what order the bytes are in*, and it only became possible
    /// to break when a drain stopped awaiting each reply where it dispatched
    /// it: a batch answered by several executors comes back grouped by
    /// executor, and the slots are what put it back into the order the peer
    /// wrote.
    #[tokio::test]
    async fn a_pipelined_mix_is_answered_in_request_order() {
        let pool = ShardPool::spawn(16, 4, DictSeed { k0: 3, k1: 5 }, NoTrace);
        let (client, server) = tokio::io::duplex(1 << 20);
        tokio::spawn(serve_connection(server, pool, NodeInfo::for_tests()));
        let (mut r, mut w) = tokio::io::split(client);

        let mut out = Vec::new();
        for i in 0..48u32 {
            encode(
                &req(&["SET", &format!("key:{i}"), &i.to_string()]),
                &mut out,
            );
        }
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();
        let frames = read_frames(&mut r, 48).await;
        assert!(frames.iter().all(|f| *f == Frame::Simple("OK".into())));

        out.clear();
        for i in 0..48u32 {
            encode(&req(&["GET", &format!("key:{i}")]), &mut out);
            if i % 8 == 0 {
                encode(&req(&["PING"]), &mut out);
            }
        }
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        let frames = read_frames(&mut r, 48 + 6).await;
        // One frame per request, in exactly the order the requests were
        // written: each GET's bulk, with a PONG in place wherever a PING was
        // interleaved.
        let mut in_request_order = Vec::new();
        for i in 0..48u32 {
            in_request_order.push(Frame::Bulk(i.to_string().into_bytes()));
            if i % 8 == 0 {
                in_request_order.push(Frame::Simple("PONG".into()));
            }
        }
        assert_eq!(frames, in_request_order);
    }

    #[tokio::test]
    async fn command_names_are_case_insensitive() {
        let (mut r, mut w, _pool) = connected(4);
        let mut out = Vec::new();
        encode(&req(&["sEt", "k", "v"]), &mut out);
        encode(&req(&["get", "k"]), &mut out);
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();
        let frames = read_frames(&mut r, 2).await;
        assert_eq!(frames[0], Frame::Simple("OK".into()));
        assert_eq!(frames[1], Frame::Bulk(b"v".to_vec()));
    }

    #[tokio::test]
    async fn a_rejected_command_leaves_the_connection_usable() {
        let (mut r, mut w, _pool) = connected(4);
        let mut out = Vec::new();
        // Every way a well-formed frame can fail to be a command.
        encode(&req(&["GET"]), &mut out);
        encode(&req(&["GET", "a", "b"]), &mut out);
        encode(&req(&["SET", "k"]), &mut out);
        encode(&req(&["INCRBY", "k", "notanumber"]), &mut out);
        encode(&req(&["INCRBY", "k", "007"]), &mut out);
        encode(&Frame::Array(vec![]), &mut out);
        encode(&Frame::Array(vec![Frame::Integer(1)]), &mut out);
        encode(&Frame::Integer(9), &mut out);
        // ...and then a command that must still work.
        encode(&req(&["SET", "k", "v"]), &mut out);
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        let frames = read_frames(&mut r, 9).await;
        for (i, frame) in frames[..8].iter().enumerate() {
            assert!(matches!(frame, Frame::Error(_)), "frame {i}: {frame:?}");
        }
        assert!(matches!(&frames[0], Frame::Error(e) if e.contains("wrong number of arguments")));
        assert_eq!(
            frames[8],
            Frame::Simple("OK".into()),
            "the connection must survive every one of them"
        );
    }

    /// Response splitting through a command name — the first of the two
    /// defences.
    ///
    /// A bulk string carries arbitrary bytes, so a peer can name a command
    /// containing `\r\n`. Echoed into an error frame, that text would
    /// terminate the frame early and the rest would be read by the client as
    /// frames of the peer's choosing. The codec's guard against this is a
    /// `debug_assert!`, which is absent in release — so it has to be stopped
    /// here.
    ///
    /// On this path `quote` is what neutralises the bytes, before
    /// `safe_error` ever sees them. That makes this test *insufficient* on
    /// its own: it would still pass with `safe_error` removed. The test below
    /// covers the path where `safe_error` is the only thing standing there.
    #[tokio::test]
    async fn a_command_name_cannot_inject_frames_into_the_error_reply() {
        let (mut r, mut w, _pool) = connected(4);
        let mut out = Vec::new();
        encode(&req(&["EVIL\r\n+INJECTED"]), &mut out);
        encode(&req(&["SET", "k", "v"]), &mut out);
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        // Exactly two frames come back. If the name had split the first one,
        // an `+INJECTED` frame would sit between them and this would read it
        // as the second.
        let frames = read_frames(&mut r, 2).await;
        let Frame::Error(text) = &frames[0] else {
            panic!("expected an error frame, got {:?}", frames[0]);
        };
        assert!(text.contains("unknown command"), "{text}");
        assert!(
            !text.contains('\r') && !text.contains('\n'),
            "error text still carries a terminator: {text:?}"
        );
        assert_eq!(
            frames[1],
            Frame::Simple("OK".into()),
            "the frame after the error must be the reply to the next command"
        );
    }

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

    /// A peer opens a frame and keeps feeding bytes without ever terminating
    /// it — the slow memory leak with a connection attached that the module
    /// doc names — and the server must answer and close rather than buffer
    /// forever.
    ///
    /// Which frame it opens with has been wrong twice, in opposite ways, and
    /// both are worth keeping written down.
    ///
    /// It first opened with `$1000000000\r\n`, far above [`MAX_BULK_LEN`], so
    /// the codec refused the header on sight and the refusal came from the
    /// *per-frame* bulk ceiling; the accumulation ceiling was never reached.
    /// The assertion looked only for "exceeds", which both messages carry, so
    /// deleting this layer's limit outright left it green. It then opened with
    /// a bulk length the codec accepts — and that stopped working for the
    /// better reason: a declared length is now priced against `max_in_memory`
    /// at the header, so any bulk big enough to dribble past this ceiling is
    /// refused before its first payload byte.
    ///
    /// What is left, and what this now uses, is the shape that has no declared
    /// length at all. A simple string ends at its terminator and nowhere else,
    /// so a peer that never sends one can be stopped by nothing but the
    /// accumulation ceiling — which is exactly the property under test, and
    /// the assertion names that ceiling's own number.
    #[tokio::test]
    async fn a_frame_that_never_ends_is_cut_off_at_the_ceiling() {
        const CEILING: usize = 64 * 1024;

        let pool = ShardPool::spawn(4, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(8 * 1024);
        let task = tokio::spawn(serve_connection_limited(
            server,
            pool,
            NodeInfo::for_tests(),
            CEILING,
            IDLE_SHED_AFTER,
        ));
        let (mut r, mut w) = tokio::io::split(client);

        let writer = tokio::spawn(async move {
            // `+` opens a line the codec will read until it finds `\r\n`. None
            // is ever sent, and no length was promised that could bound the
            // wait, so nothing but the ceiling ever says stop.
            w.write_all(b"+").await?;
            loop {
                w.write_all(&[b'x'; 4096]).await?;
            }
            #[allow(
                unreachable_code,
                reason = "the loop above only ends by returning its write error; \
                          this line exists to give the block a Result type"
            )]
            std::io::Result::Ok(())
        });

        let frames = read_frames(&mut r, 1).await;
        let Frame::Error(text) = &frames[0] else {
            panic!("expected an error frame, got {:?}", frames[0]);
        };
        assert!(
            text.contains(&format!("exceeds the {CEILING}-byte buffering limit")),
            "unexpected refusal: {text}"
        );

        // The server closes rather than carrying on, and the writer stops
        // because the pipe it is filling went away.
        assert_eq!(
            r.read(&mut [0u8; 64]).await.unwrap(),
            0,
            "stream stayed open"
        );
        writer.abort();
        task.await.expect("the connection task must end cleanly");
    }

    /// The other half of the ceiling: what a frame costs once parsed.
    ///
    /// The wire form does not reveal it. The array header below is nine bytes
    /// and promises elements whose empty `Frame`s alone are two orders of
    /// magnitude past the budget, so a limit counting only bytes read cannot
    /// refuse this and the connection would spend the memory before
    /// discovering it could not afford it. Passing `max_request_bytes` as
    /// `max_in_memory` too is what makes the refusal land at the header; this
    /// test is what says so.
    #[tokio::test]
    async fn an_array_too_large_to_hold_is_refused_at_its_header() {
        const CEILING: usize = 64 * 1024;

        let pool = ShardPool::spawn(4, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(4096);
        let task = tokio::spawn(serve_connection_limited(
            server,
            pool,
            NodeInfo::for_tests(),
            CEILING,
            IDLE_SHED_AFTER,
        ));
        let (mut r, mut w) = tokio::io::split(client);

        // A legal count — well under `MAX_ARRAY_LEN` — that this connection's
        // budget still cannot hold.
        let count = MAX_ARRAY_LEN / 2;
        assert!(count * size_of::<Frame>() > CEILING, "the count is payable");
        w.write_all(format!("*{count}\r\n").as_bytes())
            .await
            .unwrap();
        // Nothing follows, and the peer says so. Without the shutdown a
        // decoder that accepted the header would sit waiting for elements that
        // never come, and this test would hang instead of failing.
        w.shutdown().await.unwrap();

        let frames = read_frames(&mut r, 1).await;
        let Frame::Error(text) = &frames[0] else {
            panic!("expected an error frame, got {:?}", frames[0]);
        };
        // The header refusal's own wording, not the phrase it shares with the
        // per-element charge: the setup makes `array_header` the only possible
        // source, but matching the fuller text is what checks that rather than
        // leaving it to be inferred from the setup.
        assert!(
            text.contains(&format!(
                "array of {count} elements exceeds the {CEILING}-byte in-memory limit"
            )),
            "unexpected refusal: {text}"
        );
        task.await.expect("the connection task must end cleanly");
    }

    /// The same promise for the other length-prefixed frame.
    ///
    /// This one was false until recently and is the reason the claim above is
    /// worth a test each: the bulk payload used to be priced where it was
    /// copied, which is only reachable once the whole payload has been
    /// buffered, so a peer could make a connection hold megabytes it had
    /// already been told it could not afford. The header carries the length,
    /// so the header is where it is refused — and the peer here sends nothing
    /// but the header.
    #[tokio::test]
    async fn a_bulk_too_large_to_hold_is_refused_at_its_header() {
        const CEILING: usize = 64 * 1024;

        let pool = ShardPool::spawn(4, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(4096);
        let task = tokio::spawn(serve_connection_limited(
            server,
            pool,
            NodeInfo::for_tests(),
            CEILING,
            IDLE_SHED_AFTER,
        ));
        let (mut r, mut w) = tokio::io::split(client);

        // A length the codec itself accepts — under `MAX_BULK_LEN`, so the
        // per-frame ceiling cannot be what refuses it — that this
        // connection's budget cannot hold.
        let len = MAX_BULK_LEN / 2;
        const {
            assert!(
                MAX_BULK_LEN / 2 > CEILING,
                "the budget must be the binding one"
            );
        }
        w.write_all(format!("${len}\r\n").as_bytes()).await.unwrap();
        // Not one payload byte follows. If the refusal needed the payload,
        // this would hang rather than fail — which is the point.
        w.shutdown().await.unwrap();

        let frames = read_frames(&mut r, 1).await;
        let Frame::Error(text) = &frames[0] else {
            panic!("expected an error frame, got {:?}", frames[0]);
        };
        assert!(
            text.contains(&format!(
                "decoded frame exceeds the {CEILING}-byte in-memory limit"
            )),
            "unexpected refusal: {text}"
        );
        task.await.expect("the connection task must end cleanly");
    }

    /// A request larger than one read is reassembled, not re-parsed.
    ///
    /// The adaptive read buffer starts at [`READ_FLOOR`], so a command past
    /// that size crosses several reads and several `feed`s — the case where a
    /// decoder that restarted at offset zero and one that resumes differ, and
    /// the case a buffer sized by a constant nobody tested against would have
    /// hidden.
    #[tokio::test]
    async fn a_request_spanning_many_reads_arrives_whole() {
        let (mut r, mut w, _pool) = connected(4);
        let value = vec![b'v'; 8 * READ_FLOOR];
        let mut out = Vec::new();
        encode(
            &Frame::Array(vec![
                Frame::Bulk(b"SET".to_vec()),
                Frame::Bulk(b"k".to_vec()),
                Frame::Bulk(value.clone()),
            ]),
            &mut out,
        );
        encode(&req(&["GET", "k"]), &mut out);
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        let frames = read_frames(&mut r, 2).await;
        assert_eq!(frames[0], Frame::Simple("OK".into()));
        assert_eq!(frames[1], Frame::Bulk(value));
    }

    /// A large reply must not leave its allocation attached to the connection.
    ///
    /// [`flush_replies`] clears `out` after the write rather than before the
    /// next `encode`, and that is the load-bearing half: `Vec::shrink_to`
    /// never shrinks below the length, so shedding while the reply is still in
    /// the buffer is a no-op on exactly the write that grew it. The bug that
    /// shape produces is invisible in a pipeline — the next drain's first
    /// `encode` finds an already-cleared buffer anyway — and shows up only for
    /// the client that reads one big value and then goes quiet, which is why
    /// it is asserted directly on the function rather than through a
    /// connection.
    #[tokio::test]
    async fn a_large_reply_sheds_its_buffer_before_the_next_one() {
        let mut sink: Vec<u8> = Vec::new();
        let mut out: Vec<u8> = Vec::new();

        append_frame(&mut out, &Frame::Bulk(vec![b'v'; 4 * REPLY_SHED]));
        assert!(flush_replies(&mut sink, &mut out, &AtomicU64::new(0)).await);
        assert!(sink.len() > 4 * REPLY_SHED, "the reply was truncated");
        assert!(
            out.capacity() <= REPLY_SHED,
            "capacity {} still held after the reply that grew it",
            out.capacity()
        );

        // Shedding cost nothing: the next reply is still encoded correctly
        // into the shrunken buffer.
        sink.clear();
        append_frame(&mut out, &Frame::Simple("OK".into()));
        assert!(flush_replies(&mut sink, &mut out, &AtomicU64::new(0)).await);
        assert_eq!(sink, b"+OK\r\n");
    }

    /// A drain that answered nothing must not write, and must not flush.
    ///
    /// This is what keeps the batched loop from replacing one syscall pair per
    /// reply with one per turn of the outer loop: every read that completes no
    /// frame — a dribbled request, and the first turn of every connection —
    /// reaches the flush with an empty buffer.
    #[tokio::test]
    async fn an_empty_drain_does_not_write() {
        let flushes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut sink = FlushCounting {
            inner: Vec::<u8>::new(),
            flushes: std::sync::Arc::clone(&flushes),
            max_write: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        let mut out: Vec<u8> = Vec::new();

        assert!(flush_replies(&mut sink, &mut out, &AtomicU64::new(0)).await);
        assert!(sink.inner.is_empty(), "an empty drain wrote bytes");
        assert_eq!(
            flushes.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "an empty drain spent a flush"
        );
    }

    /// A connection that went quiet gives back *all three* of its buffers.
    ///
    /// The read buffer was the only one shedding to the floor, and it is the
    /// smallest of the three: a connection that carried one large frame kept
    /// [`DecoderLimits::SHED`] plus [`REPLY_SHED`] — a quarter of a megabyte
    /// each — for the rest of its life, which is the term that dominates at
    /// the connection limit. The quiet window is one decision, so it sheds
    /// everything the connection grew, not just the buffer it is named after.
    #[tokio::test]
    async fn a_quiet_connection_sheds_every_buffer_it_grew() {
        let mut decoder = Decoder::new(DecoderLimits::default());
        let mut out: Vec<u8> = Vec::new();
        let mut read_buf = vec![0u8; READ_FLOOR];
        let mut quiet = 0u32;

        // A request and a reply, both far past the shed thresholds, so all
        // three buffers are holding the allocation a burst left behind.
        let big = vec![b'v'; 4 * DecoderLimits::SHED];
        let mut wire = Vec::new();
        encode(&req(&["ECHO"]), &mut wire);
        encode(&Frame::Bulk(big.clone()), &mut wire);
        decoder.feed(&wire);
        while matches!(decoder.try_next(), Ok(Some(_))) {}
        let mut sink: Vec<u8> = Vec::new();
        append_frame(&mut out, &Frame::Bulk(big));
        assert!(flush_replies(&mut sink, &mut out, &AtomicU64::new(0)).await);
        for _ in 0..32 {
            let got = read_buf.len();
            resize_connection_buffers(&mut read_buf, &mut decoder, &mut out, &mut quiet, got);
        }
        assert_eq!(read_buf.len(), READ_CEILING);
        assert_eq!(decoder.capacity(), DecoderLimits::SHED);
        assert_eq!(out.capacity(), REPLY_SHED);

        // The peer stops. One quiet window later, every one of them is back at
        // the floor — and the decoder's is, specifically, not still at SHED.
        for _ in 0..READ_QUIET_READS {
            resize_connection_buffers(&mut read_buf, &mut decoder, &mut out, &mut quiet, 16);
        }
        assert_eq!(read_buf.len(), READ_FLOOR);
        assert!(
            decoder.capacity() <= READ_FLOOR,
            "the decoder kept {} bytes",
            decoder.capacity()
        );
        assert!(
            out.capacity() <= READ_FLOOR,
            "the reply buffer kept {} bytes",
            out.capacity()
        );
    }

    /// Serves one request, then never speaks again, recording the size of
    /// every buffer it is offered.
    ///
    /// The record is what makes the idle shed observable from outside without
    /// a test hook: the connection's buffers are locals of its future, and the
    /// only thing it ever shows anyone is how much room it asks to read into.
    struct GoesSilent {
        request: Vec<u8>,
        offered: Arc<std::sync::Mutex<Vec<usize>>>,
    }

    impl AsyncRead for GoesSilent {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            self.offered.lock().expect("offered").push(buf.remaining());
            if self.request.is_empty() {
                // Parked, exactly as a peer that has gone quiet leaves it. No
                // waker is registered, which is the whole point: only the
                // timer can move this connection now.
                return std::task::Poll::Pending;
            }
            let take = self.request.len().min(buf.remaining());
            let chunk: Vec<u8> = self.request.drain(..take).collect();
            buf.put_slice(&chunk);
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// A sink: accept everything, remember nothing. The replies are not what
    /// [`GoesSilent`] is for.
    impl AsyncWrite for GoesSilent {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            std::task::Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// Yields until the connection has nothing left to do, and answers how
    /// many reads it has asked for by then.
    ///
    /// Every poll of the transport is recorded, so "settled" is a record whose
    /// length stops changing. The yields also keep the runtime's queue
    /// non-empty, which is what stops the paused clock auto-advancing
    /// underneath the measurement.
    async fn settle(offered: &Arc<std::sync::Mutex<Vec<usize>>>) -> usize {
        let mut len = usize::MAX;
        for _ in 0..64 {
            tokio::task::yield_now().await;
            let now = offered.lock().expect("offered").len();
            if now == len {
                return now;
            }
            len = now;
        }
        panic!("the connection never settled");
    }

    /// A peer that stops mid-conversation is re-measured by the clock.
    ///
    /// The quiet-read hysteresis cannot see this connection: its verdict is
    /// read from the shape of the reads, and this peer has stopped producing
    /// reads at all. Nothing wakes a task parked on `read`, so without a timer
    /// the buffers this connection grew are held for as long as it stays
    /// attached — which, at the connection limit's default, is the largest
    /// single amount of memory a server can be made to hold while doing
    /// nothing.
    ///
    /// After the idle interval the connection must ask for a floor-sized read
    /// again, which is the assertion the current hysteresis cannot make.
    #[tokio::test(start_paused = true)]
    async fn a_connection_that_goes_silent_gives_its_buffers_back() {
        let offered = Arc::new(std::sync::Mutex::new(Vec::new()));
        let value = vec![b'x'; 512 * 1024];
        let mut request = Vec::new();
        encode(
            &Frame::Array(vec![
                Frame::Bulk(b"SET".to_vec()),
                Frame::Bulk(b"k".to_vec()),
                Frame::Bulk(value),
            ]),
            &mut request,
        );

        let stream = GoesSilent {
            request,
            offered: Arc::clone(&offered),
        };
        let pool = ShardPool::spawn(4, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let idle = Duration::from_secs(2);
        let served = tokio::spawn(serve_connection_limited(
            stream,
            pool,
            NodeInfo::for_tests(),
            MAX_REQUEST_BYTES,
            idle,
        ));

        // Let the request be served and the connection park on a read that
        // will never complete.
        let before = settle(&offered).await;
        let grew = *offered
            .lock()
            .expect("offered")
            .iter()
            .max()
            .expect("a read");
        assert!(
            grew > READ_FLOOR,
            "the connection never grew, so this test would pass vacuously"
        );

        // Two intervals, not one, and that is the arming discipline rather
        // than slack. The timer is armed by the first growth, in the middle of
        // the burst that grew it, so its first firing finds a read counter
        // that moved since — the peer *was* talking — and re-arms instead of
        // shedding. The second firing is the one that finds nothing arrived.
        for _ in 0..2 {
            tokio::time::advance(idle + Duration::from_millis(1)).await;
            settle(&offered).await;
        }

        let offered = offered.lock().expect("offered").clone();
        assert!(
            offered.len() > before,
            "the idle timer never fired: the connection was not re-measured"
        );
        // The timer's own wake re-polls the read arm before the timer arm —
        // `biased` puts it there — so the connection is offered its grown
        // buffer one last time on the way to shedding it. What the shed has to
        // change is the read it asks for *next*, which is the last recorded.
        assert_eq!(
            offered.last().copied(),
            Some(READ_FLOOR),
            "after the idle interval the connection must be back at the floor"
        );
        served.abort();
    }

    /// Delivers a scripted run of requests and then stops for good.
    ///
    /// A chunk longer than the buffer it is offered is split, so the same
    /// script drives a connection through the growth the first chunk forces
    /// and the quiet window the rest of them make.
    struct TalksThenStops {
        chunks: std::collections::VecDeque<Vec<u8>>,
        offered: Arc<std::sync::Mutex<Vec<usize>>>,
    }

    impl AsyncRead for TalksThenStops {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            self.offered.lock().expect("offered").push(buf.remaining());
            let Some(mut chunk) = self.chunks.pop_front() else {
                return std::task::Poll::Pending;
            };
            if chunk.len() > buf.remaining() {
                let rest = chunk.split_off(buf.remaining());
                self.chunks.push_front(rest);
            }
            buf.put_slice(&chunk);
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// A sink, for the same reason [`GoesSilent`]'s is one.
    impl AsyncWrite for TalksThenStops {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            std::task::Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// A connection the reads already emptied costs no timer.
    ///
    /// This is the half of [`IDLE_SHED_AFTER`]'s claim that the silent-peer
    /// test cannot make. A peer that grows a connection and then goes *quiet*
    /// rather than silent is shed by the hysteresis, without the clock — and
    /// if arming were not undone by that route, every such connection would go
    /// on holding a timer that fires once an interval forever, to reclaim
    /// buffers that are already at the floor.
    ///
    /// A disarmed connection is one nothing can wake: the transport parks
    /// without registering a waker, so a firing timer is the only thing that
    /// could produce another read. Advancing the clock and finding no new read
    /// is therefore the assertion, and it fails if the timer is left armed.
    #[tokio::test(start_paused = true)]
    async fn a_connection_the_reads_already_emptied_holds_no_timer() {
        let offered = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut chunks = std::collections::VecDeque::new();

        // One request large enough to take the buffers to the ceiling...
        let mut big = Vec::new();
        encode(
            &Frame::Array(vec![
                Frame::Bulk(b"SET".to_vec()),
                Frame::Bulk(b"k".to_vec()),
                Frame::Bulk(vec![b'x'; 512 * 1024]),
            ]),
            &mut big,
        );
        chunks.push_back(big);
        // ...then a run of small ones, each its own read, which is exactly the
        // evidence [`READ_QUIET_READS`] accumulates. Twice the window, so the
        // shed is comfortably inside the script rather than on its last read.
        for _ in 0..2 * READ_QUIET_READS {
            let mut ping = Vec::new();
            encode(&req(&["PING"]), &mut ping);
            chunks.push_back(ping);
        }

        let stream = TalksThenStops {
            chunks,
            offered: Arc::clone(&offered),
        };
        let pool = ShardPool::spawn(4, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let idle = Duration::from_secs(2);
        let served = tokio::spawn(serve_connection_limited(
            stream,
            pool,
            NodeInfo::for_tests(),
            MAX_REQUEST_BYTES,
            idle,
        ));

        let before = settle(&offered).await;
        let script = offered.lock().expect("offered").clone();
        assert!(
            script.iter().copied().max() > Some(READ_FLOOR),
            "the connection never grew, so this test would pass vacuously"
        );
        assert_eq!(
            script.last().copied(),
            Some(READ_FLOOR),
            "the quiet window never shed, so there is no disarming to check"
        );

        // Well past the two intervals the silent-peer case needs.
        for _ in 0..3 {
            tokio::time::advance(idle + Duration::from_millis(1)).await;
            settle(&offered).await;
        }
        assert_eq!(
            offered.lock().expect("offered").len(),
            before,
            "a timer fired for a connection that had nothing left to give back"
        );
        served.abort();
    }

    /// Shedding never costs a byte of a frame still arriving.
    ///
    /// A peer can dribble a large frame slowly enough that the quiet window
    /// closes while its bytes are still in the decoder. The shed has to be a
    /// release of *spare* capacity, so the frame must still complete, and
    /// complete whole.
    #[tokio::test]
    async fn shedding_mid_frame_does_not_disturb_the_frame() {
        let mut decoder = Decoder::new(DecoderLimits::default());
        let mut out: Vec<u8> = Vec::new();
        let mut read_buf = vec![0u8; READ_FLOOR];
        let mut quiet = 0u32;

        let value = vec![b'v'; 4 * DecoderLimits::SHED];
        let mut wire = Vec::new();
        encode(&req(&["ECHO"]), &mut wire);
        encode(&Frame::Bulk(value.clone()), &mut wire);
        let (head, tail) = wire.split_at(wire.len() / 2);
        decoder.feed(head);
        assert!(matches!(decoder.try_next(), Ok(Some(_))), "the name frame");
        assert!(
            matches!(decoder.try_next(), Ok(None)),
            "the value is partial"
        );

        for _ in 0..4 * READ_QUIET_READS {
            resize_connection_buffers(&mut read_buf, &mut decoder, &mut out, &mut quiet, 16);
        }
        assert!(
            decoder.capacity() >= decoder.buffered(),
            "the shed dropped buffered bytes"
        );

        decoder.feed(tail);
        assert_eq!(decoder.try_next().unwrap(), Some(Frame::Bulk(value)));
    }

    // --- authentication ---

    fn node_with_password(pw: &[u8]) -> NodeInfo {
        let mut node = NodeInfo::for_tests();
        node.password = Some(Secret::new(pw.to_vec()));
        node
    }

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

    // --- helpers ---

    fn connected(
        shards: u16,
    ) -> (
        tokio::io::ReadHalf<tokio::io::DuplexStream>,
        tokio::io::WriteHalf<tokio::io::DuplexStream>,
        ShardPool,
    ) {
        // Four executors, or one per shard where there are fewer than four:
        // a pool may not have more executors than shards, and a test that
        // wants a single shard wants it precisely to remove the parallelism
        // that would hide what it is asserting.
        let pool = ShardPool::spawn(shards, shards.min(4), DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(serve_connection(
            server,
            pool.clone(),
            NodeInfo::for_tests(),
        ));
        let (r, w) = tokio::io::split(client);
        (r, w, pool)
    }

    /// The text of a bulk reply, for the assertions that read `INFO`.
    fn bulk_text(frame: &Frame) -> String {
        match frame {
            Frame::Bulk(bytes) => String::from_utf8(bytes.clone()).expect("INFO is not UTF-8"),
            other => panic!("expected a bulk reply, got {other:?}"),
        }
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

    fn req(parts: &[&str]) -> Frame {
        Frame::Array(
            parts
                .iter()
                .map(|p| Frame::Bulk(p.as_bytes().to_vec()))
                .collect(),
        )
    }

    async fn read_frames<R: AsyncRead + Unpin>(r: &mut R, n: usize) -> Vec<Frame> {
        let (mut buf, mut chunk, mut frames) = (Vec::new(), [0u8; 1024], Vec::new());
        while frames.len() < n {
            while let Some((f, used)) = parse(&buf).unwrap() {
                frames.push(f);
                buf.drain(..used);
                if frames.len() == n {
                    return frames;
                }
            }
            let got = r.read(&mut chunk).await.unwrap();
            assert_ne!(got, 0, "stream closed with {} of {n} frames", frames.len());
            buf.extend_from_slice(&chunk[..got]);
        }
        frames
    }
}
