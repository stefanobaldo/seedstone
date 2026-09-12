//! Requests and replies over one connection: what the drain answers, in what
//! order, and in how many writes.

use super::support::{FlushCounting, connected, read_frames, req};
use crate::auth::AUTH_NOT_CONFIGURED;
use crate::connection::{CHUNK_COMMANDS, READ_CEILING, REPLY_HIGH_WATER, serve_connection};
use crate::hello::NOPROTO;
use crate::node::NodeInfo;
use crate::reply::UNRENDERABLE_REPLY;
use seedstone_core::dict::DictSeed;
use seedstone_core::shard::{Command, NoTrace, Reply, Router, ShardPool};
use seedstone_resp::{Frame, encode};
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
