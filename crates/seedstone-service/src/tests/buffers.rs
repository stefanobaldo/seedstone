//! The three buffers a connection holds: how they grow with what it is
//! doing, and when they are given back.

use super::support::{FlushCounting, connected, read_frames, req};
use crate::connection::{
    IDLE_SHED_AFTER, MAX_REQUEST_BYTES, READ_CEILING, READ_FLOOR, READ_QUIET_READS, REPLY_SHED,
    append_frame, flush_replies, resize_connection_buffers, serve_connection_limited,
};
use crate::node::NodeInfo;
use seedstone_core::dict::DictSeed;
use seedstone_core::shard::{NoTrace, ShardPool};
use seedstone_resp::{Decoder, DecoderLimits, Frame, MAX_ARRAY_LEN, MAX_BULK_LEN, encode};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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
