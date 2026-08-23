//! The walk's exact oracle: a full cycle, on a keyspace nothing is changing,
//! returns exactly the keys that are there — each of them once.
//!
//! The kernel's own tests say what the crossing rules are; this says what a
//! client gets. It is deliberately an integration test driven over the wire:
//! the question is about the answers a `SCAN` cycle produces, not about the
//! shape of any function that produces them, and the wire is the only place
//! the two cannot be confused.
//!
//! Three shard counts, four keyspace sizes, four `COUNT`s, with and without
//! `MATCH`. The corner that costs the most is a thousand shards holding a
//! thousand keys at `COUNT 1`, and the keyspace there is cut to three hundred
//! so the whole file stays a few seconds: what the corner is for is a node
//! with far more shards than keys, and three hundred over a thousand shards
//! is that shape more sharply than a thousand would be.

use seedstone_core::dict::DictSeed;
use seedstone_core::shard::{Command, NoTrace, Router as _, ShardPool};
use seedstone_resp::{Frame, encode, parse};
use seedstone_service::{NodeInfo, serve_connection};
use std::collections::{BTreeMap, BTreeSet};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf, split};

/// The server's per-call occupancy ceiling, mirrored rather than imported —
/// `WALK_STEP_BUCKETS` is not public, and the bound below is a statement
/// about how many calls a cycle costs, which needs the number.
const BUCKET_CEILING: usize = 256;

/// The smallest table a shard can have, from the core's `INITIAL_BUCKETS`.
/// Mirrored for the same reason, and used the same way: as the floor on how
/// many buckets a cycle has to cross.
const MIN_BUCKETS_PER_SHARD: usize = 8;

struct Client {
    reader: ReadHalf<DuplexStream>,
    writer: WriteHalf<DuplexStream>,
}

impl Client {
    fn connect(pool: &ShardPool) -> Self {
        let (client, server) = tokio::io::duplex(1024 * 1024);
        tokio::spawn(serve_connection(
            server,
            pool.clone(),
            NodeInfo::for_tests(),
        ));
        let (reader, writer) = split(client);
        Self { reader, writer }
    }

    async fn scan(
        &mut self,
        cursor: &str,
        count: usize,
        pattern: Option<&str>,
    ) -> (String, Vec<Vec<u8>>) {
        let count = count.to_string();
        let mut parts = vec!["SCAN", cursor, "COUNT", &count];
        if let Some(pattern) = pattern {
            parts.push("MATCH");
            parts.push(pattern);
        }
        let request = Frame::Array(
            parts
                .iter()
                .map(|p| Frame::Bulk(p.as_bytes().to_vec()))
                .collect(),
        );
        let mut out = Vec::new();
        encode(&request, &mut out);
        self.writer
            .write_all(&out)
            .await
            .expect("the duplex accepts a request");
        self.writer.flush().await.expect("the duplex flushes");

        let mut buf = Vec::new();
        // On the heap: a cycle's future carries this across every await in
        // it, and a buffer this size on the stack makes the future itself
        // large enough for clippy to ask about.
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            if let Some((frame, _)) = parse(&buf).expect("the server speaks RESP2") {
                let Frame::Array(pair) = frame else {
                    panic!("SCAN answers a two-element array, got {frame:?}");
                };
                let (Frame::Bulk(cursor), Frame::Array(keys)) = (&pair[0], &pair[1]) else {
                    panic!("SCAN answers a bulk cursor and an array of keys");
                };
                let keys = keys
                    .iter()
                    .map(|key| match key {
                        Frame::Bulk(bytes) => bytes.clone(),
                        other => panic!("keys are bulk strings, got {other:?}"),
                    })
                    .collect();
                return (
                    String::from_utf8(cursor.clone()).expect("a cursor is decimal ASCII"),
                    keys,
                );
            }
            let got = self
                .reader
                .read(&mut chunk)
                .await
                .expect("the server answers");
            assert_ne!(got, 0, "the server closed the connection mid-cycle");
            buf.extend_from_slice(&chunk[..got]);
        }
    }
}

/// Loads `n` keys named `key:000000`… into a fresh pool.
async fn loaded(shards: u16, n: usize) -> (ShardPool, BTreeSet<Vec<u8>>) {
    let pool = ShardPool::spawn(shards, shards.min(4), DictSeed { k0: 3, k1: 7 }, NoTrace);
    let mut expected = BTreeSet::new();
    for i in 0..n {
        let key = format!("key:{i:06}").into_bytes();
        pool.dispatch(Command::Set {
            key: key.clone(),
            value: b"v".to_vec(),
            expiry: None,
            cond: None,
            keep_ttl: false,
            get: false,
        })
        .await;
        expected.insert(key);
    }
    (pool, expected)
}

/// A full cycle, driven the way a client drives one: from `0`, following the
/// cursor back, until it is `0` again.
async fn full_cycle(
    client: &mut Client,
    count: usize,
    pattern: Option<&str>,
) -> (BTreeMap<Vec<u8>, usize>, usize) {
    let mut seen: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    let mut cursor = String::from("0");
    let mut calls = 0;
    loop {
        let (next, keys) = client.scan(&cursor, count, pattern).await;
        for key in keys {
            *seen.entry(key).or_default() += 1;
        }
        calls += 1;
        cursor = next;
        if cursor == "0" {
            break;
        }
        assert!(calls < 100_000, "the cycle did not terminate");
    }
    (seen, calls)
}

/// What a cycle may cost, in calls, and why.
///
/// A call ends on one of three things, so the calls of a cycle divide into
/// three kinds and each kind is bounded on its own. A call that ended on the
/// key target returned at least `count` keys, and a quiescent cycle returns
/// each key once, so there are at most `matching / count` of those. A call
/// that ended on the bucket ceiling spent [`BUCKET_CEILING`] buckets, and a
/// cycle crosses every shard's table exactly once, so there are at most
/// `buckets / BUCKET_CEILING` of those. And exactly one call ends the cycle.
///
/// The bucket total is bounded rather than known: a table is a power of two
/// at least [`MIN_BUCKETS_PER_SHARD`] wide holding at most its own width in
/// keys, so every shard costs its floor and every key costs at most two
/// buckets beyond it.
fn call_bound(shards: u16, keys: usize, matching: usize, count: usize) -> usize {
    let buckets = usize::from(shards) * MIN_BUCKETS_PER_SHARD + 2 * keys;
    matching / count + buckets / BUCKET_CEILING + 1
}

#[tokio::test]
async fn a_full_cycle_returns_exactly_the_keyspace_once_at_every_count() {
    for shards in [1u16, 16, 1024] {
        for n in [0usize, 1, 7, 1000] {
            // The one corner cut, and it is cut for time rather than for
            // coverage: see this file's header.
            let n = if shards == 1024 && n == 1000 { 300 } else { n };
            let (pool, expected) = loaded(shards, n).await;
            let mut client = Client::connect(&pool);
            for count in [1usize, 10, 100, 1000] {
                for pattern in [None, Some("key:*7")] {
                    let wanted: BTreeSet<Vec<u8>> = expected
                        .iter()
                        .filter(|key| pattern.is_none() || key.ends_with(b"7"))
                        .cloned()
                        .collect();
                    let (seen, calls) = full_cycle(&mut client, count, pattern).await;
                    let shape = format!(
                        "shards={shards} n={n} count={count} match={}",
                        pattern.unwrap_or("-")
                    );
                    assert_eq!(
                        seen.keys().cloned().collect::<BTreeSet<_>>(),
                        wanted,
                        "{shape}: a quiescent cycle returned a different key set"
                    );
                    assert!(
                        seen.values().all(|&times| times == 1),
                        "{shape}: a quiescent cycle returned a key more than once"
                    );
                    let bound = call_bound(shards, n, wanted.len(), count);
                    assert!(
                        calls <= bound,
                        "{shape}: {calls} calls against a bound of {bound}"
                    );
                }
            }
        }
    }
}
