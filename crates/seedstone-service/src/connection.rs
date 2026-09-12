//! **Bounded buffering.** [`MAX_REQUEST_BYTES`] caps what one connection can
//! make the server hold, on top of the per-frame ceilings the codec
//! enforces ([`seedstone_resp::MAX_BULK_LEN`],
//! [`seedstone_resp::MAX_ARRAY_LEN`]). Without a cap, a peer
//! that opens a frame and never finishes it is a slow memory leak with a
//! connection attached. The cap is *set* here and *enforced* by the
//! [`Decoder`] this layer hands it to — see [`MAX_REQUEST_BYTES`] for why
//! the two are not the same place.
//!
//! **Giving the buffers back.** A connection sizes its three buffers to what
//! it is doing ([`resize_connection_buffers`]) and returns them to the floor
//! when it stops. It stops in two distinguishable ways — still talking in
//! small requests, which the reads themselves report, and gone silent, which
//! only a clock can report — so there are two signals and one shed
//! ([`IDLE_SHED_AFTER`]).

use crate::dispatch::{Action, frame_to_action, gated, settle_auth};
use crate::node::NodeInfo;
use crate::reply::{
    CommandLabel, count_error_reply, log_error_reply, protocol_error, reply_to_frame, safe_error,
};
use seedstone_core::shard::{Command, Reply, ReplyError, Router};
use seedstone_resp::{Decoder, DecoderLimits, Frame, encode};
use std::mem::take;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::Instant;

/// How many bytes one connection may hold while a frame is still incomplete.
///
/// The codec bounds a single bulk payload and a single array's element count;
/// this bounds the accumulation of them. It must stay comfortably above the
/// largest command the codec accepts — a `SET` of two
/// [`seedstone_resp::MAX_BULK_LEN`] payloads is about 32 MiB on the wire — or
/// a legitimate command would be refused as oversized.
///
/// # Where it is enforced
///
/// Not here. It is handed to the [`Decoder`] as both
/// [`DecoderLimits::max_frame_bytes`] — the wire bytes one unfinished frame
/// may occupy — and [`DecoderLimits::max_in_memory`] — what that frame costs
/// once parsed, which the wire form does not reveal. That is deliberate and
/// it is a choice, because this layer could just as well have counted the
/// bytes it read: the decoder is the thing that *holds* them, and it is the
/// only one of the two that can see the parsed size at all. A peer over the
/// ceiling is therefore answered with the codec's own `frame exceeds the
/// …-byte buffering limit` (or `decoded frame exceeds …`), not with a second
/// message this layer would have to keep in step with it. One ceiling, one
/// enforcement point, one wording.
///
/// The two halves are not enforced with the same sharpness, and the
/// difference is worth knowing. `max_in_memory` is checked *before* the
/// memory is spent — a bulk payload is priced from the length its header
/// declares, an array from its count, both ahead of the first byte either one
/// promises — so the refusal costs nothing but the header. `max_frame_bytes`
/// cannot be: a frame with no declared length, an unterminated `+` line most
/// of all, reveals its size only by ending, so the bytes have already landed
/// in the decoder's buffer when the check runs and the buffer overruns the
/// ceiling by at most one read (`READ_CEILING`). What that does *not* mean is
/// that the verdict depends on the reads: the codec applies the same ceiling
/// where a frame completes as where it runs out, so the same bytes are
/// refused whatever sizes the peer wrote them in.
///
/// # Known cost
///
/// Filling this buffer is linear in the bytes read — the decoder resumes
/// where it stopped instead of re-parsing the frame from its first byte — so
/// a dribbled frame costs what it weighs and no more. Three things about the
/// weight itself:
///
/// - **It is a *per connection* ceiling.** The product of it and the
///   connection limit is not a number the machine has. It is a bound on the
///   worst case one peer can impose, not a memory budget for all of them.
/// - **One connection at its peak holds about twice it.** The wire bytes and
///   the parsed representation are budgeted separately, at this figure each,
///   and they coexist: the decoder is still holding a frame's bytes while the
///   `Frame`s built from them accumulate. Call the per-connection peak
///   ~128 MiB, plus the reply buffer, not ~64 MiB.
/// - **The parsed side is accounted by length, and `Vec`s reserve by
///   capacity.** A partly filled array is charged for the elements in it, not
///   for the room it grew to hold them, so a `Vec` that doubled past its
///   element count is undercounted by up to a further 2×.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

/// Bytes a connection's read buffer starts at, and sheds back to.
///
/// A request is typically tens of bytes, and idle connections outnumber busy
/// ones: at the connection limit's default of 10 000, every kilobyte reserved
/// here is 10 MiB of resident memory bought before anyone has spoken.
///
/// It is the floor for all three of a connection's buffers, not just this
/// one. The decoder's and the reply buffer's own floors are a quarter of a
/// megabyte each, which is the right size to hold between requests and far
/// too much to hold for a connection that has stopped; both come down to this
/// number on the same quiet verdict, in
/// [`resize_connection_buffers`]. What that verdict needs is the hysteresis
/// this side already had — [`READ_QUIET_READS`] exists because growth and
/// shedding sit one step apart — which is why the policy lives here and the
/// codec merely exposes the lever.
///
/// **The quiet verdict is read from the reads, so it reaches only a peer that
/// is still talking** — in small requests. Nothing wakes a connection task
/// parked on a read, so a peer that goes silent mid-conversation produces no
/// evidence at all and is never re-measured by that route. The second route is
/// [`IDLE_SHED_AFTER`], a timer the connection arms while it holds more than
/// this floor; both routes end in the same place, `shed_connection_buffers`.
pub const READ_FLOOR: usize = 2 * 1024;

/// The largest single `read` a connection grows to.
///
/// Reached by doubling, one filling read at a time, so a peer that is
/// streaming gets fewer and larger syscalls while one that sends a command
/// and waits never pays for the capacity.
pub const READ_CEILING: usize = 64 * 1024;

/// Consecutive small reads before the read buffer gives its capacity back.
///
/// Hysteresis, and it is load-bearing rather than tidy. Growth and shedding
/// are one step apart — a read that fills a 4 KiB buffer doubles it, and a
/// 2 KiB read then satisfies the shed condition on the 8 KiB result — so a
/// peer alternating full and quarter reads would reallocate on every single
/// one, and pay five doublings to climb back each time. Requiring the quiet
/// to persist makes shedding a statement about a connection that stopped
/// rather than about one read that was small.
pub const READ_QUIET_READS: u32 = 4;

/// The granularity at which a connection holding more than the floor is asked
/// whether anything is still arriving.
///
/// The quiet-read hysteresis in [`resize_connection_buffers`] reads its
/// verdict from the shape of the reads, so it cannot reach a peer that has
/// stopped producing them: nothing wakes a task parked on `read`. This is the
/// only signal that can, and the interval is a compromise between holding a
/// working set across a pause in a conversation and holding it for a
/// connection that will never speak again.
///
/// **It is a granularity, not a deadline.** The timer is armed by the growth
/// that first took the connection above its floor, which is in the middle of
/// the burst that grew it, so a firing that finds reads since the arming
/// re-arms rather than sheds. A connection therefore gives its buffers back
/// somewhere between one and two intervals after its last read, and the
/// alternative — resetting the timer on every read — is the cost this
/// deliberately does not pay on the hot path.
///
/// It costs nothing while a connection is busy and nothing once it has shed:
/// the timer is armed only while the buffers are above the floor, and it
/// disarms itself after shedding. A server at its connection limit with every
/// peer silent therefore holds no timers at all.
pub const IDLE_SHED_AFTER: Duration = Duration::from_secs(2);

/// The reply buffer capacity a connection sheds back to after each write.
///
/// Same reasoning as [`DecoderLimits::SHED`] on the read side, and *the same
/// number* — taken from it rather than restated, so the two cannot drift
/// while a comment goes on claiming they agree. One large reply otherwise
/// leaves its allocation attached to the connection for the rest of that
/// connection's life.
///
/// This is the floor for a connection still working. A connection that goes
/// quiet drops below it, to [`READ_FLOOR`], along with the other two buffers
/// — see [`resize_connection_buffers`].
pub const REPLY_SHED: usize = DecoderLimits::SHED;

/// How much one drain may accumulate before it writes.
///
/// Flushing at the drain boundary is what turns a pipelined batch into one
/// syscall pair instead of one per reply. But a drain ends only when the
/// decoder holds no complete frame, and the decoder holds whatever a full
/// [`READ_CEILING`] of pipelined requests decodes to — so without a mark, one
/// drain buffers *every* reply that batch earns, where the per-reply flush it
/// replaced buffered one. This is the point at which the replies already
/// accumulated are worth a write on their own.
///
/// The number is [`REPLY_SHED`] — the capacity a working connection is already
/// allowed to keep between writes — taken from it rather than restated. The
/// mark and the shed floor agreeing is what keeps a connection at steady state
/// from both growing past it and reallocating below it: every write at the
/// mark is followed by a shed to the same size, which is a no-op.
///
/// **A single reply larger than the mark still goes out whole.** The check is
/// made after appending, never before, because a frame is not splittable: half
/// a bulk string on the wire is a protocol violation, not a partial write.
pub const REPLY_HIGH_WATER: usize = REPLY_SHED;

/// How many keyed commands one chunk of a drain may accumulate before it
/// dispatches them.
///
/// A drain hands its keyed commands to the router as a batch rather than one
/// at a time, and a drain ends only when the decoder holds no complete frame —
/// so without a mark, one batch would grow to hold every command a full
/// [`READ_CEILING`] of pipelined requests decodes to. The same shape of
/// argument as [`REPLY_HIGH_WATER`], one layer earlier: that one bounds the
/// bytes a drain holds, this one bounds the commands.
///
/// Three things at once, in fact. The pending-command vector, the reply vector
/// it is answered with, and how long one batch occupies whatever runs it —
/// [`Router::dispatch_many`] may apply a batch without yielding between
/// commands, so the batch's length is the delay it can impose on everything
/// else queued behind it.
///
/// That third reason is not the drain's alone: a multi-key request answered
/// with an array slices on this same number before it dispatches, and for
/// exactly that property — see [`fan_out`]. One constant, because it is one
/// question: how long a single request may occupy an executor.
///
/// The number sits above what a pipelining client sends in one round trip, so
/// the ordinary burst is still a single batch, while leaving a full
/// [`READ_CEILING`] of the smallest commands a dozen-odd batches rather than
/// one.
pub const CHUNK_COMMANDS: usize = 128;

/// One decoded request's place in a chunk, with the command behind it.
///
/// A request is either answered already — a connection command, or anything
/// the peer got wrong — or waiting on the router, in which case the slot
/// carries where in the chunk's batch its command went. Holding both kinds in
/// one ordered vector is what puts a batch back into request order after it
/// comes back grouped by whoever ran it.
///
/// The label rides here rather than being looked up at the drain because the
/// drain has no way back to the command: a pending slot's index points into a
/// batch that has already been moved into the router.
pub enum Slot {
    /// Answered here; this frame goes out as it stands.
    Ready(Frame, CommandLabel),
    /// Answered by the router; the index is into the chunk's batch.
    Pending(usize, CommandLabel),
}

/// Serves one connection until the peer disconnects or sends something that
/// can never be a valid frame.
///
/// Complete frames are drained from the read buffer and mapped to actions;
/// the replies accumulate and are written and **flushed together when the
/// drain ends** — the moment before this loop would park on `read`. The flush
/// placement is not optional: a transport that buffers — a simulated one
/// especially — would otherwise hold a reply the peer is blocked waiting for,
/// and the deadlock only appears once the code runs under the simulator. The
/// invariant is that this loop never waits for bytes while a reply sits
/// unflushed; batching within one drain preserves it, because a drain only
/// ends when the decoder has no complete frame left.
///
/// A drain is made of **chunks**. Within a chunk, a connection command is
/// answered on the spot and a keyed one joins a batch, both taking a [`Slot`]
/// in the order the peer wrote them; closing the chunk dispatches the batch
/// with [`Router::dispatch_many`], splices each reply into its slot, and
/// appends the lot in request order. So a keyed command is no longer awaited
/// where it is decoded — which is the point, since a batch reaches whoever
/// owns its keys in one message per owner instead of one per command — and
/// request order is restored by the slots rather than by the awaiting. A chunk
/// closes when the decoder runs dry, when the batch reaches
/// [`CHUNK_COMMANDS`], before a multi-key request fans out — see [`fan_out`],
/// which has to run *after* what the peer wrote in front of it — and at `QUIT`
/// or a protocol error.
///
/// Accumulation is bounded rather than open-ended, on both axes: a drain that
/// reaches [`REPLY_HIGH_WATER`] writes there and carries on into the same
/// buffer, and a chunk that reaches [`CHUNK_COMMANDS`] dispatches there and
/// carries on in the same drain. What one connection can hold therefore does
/// not scale with how much its peer chose to pipeline. Writing earlier can
/// never violate the invariant above — it only shortens the time a reply
/// spends buffered.
///
/// A frame that is well-formed RESP but not a command this server knows —
/// wrong arity, unknown name — is answered with an error frame and the
/// connection stays open, exactly as Redis behaves. A frame that is not
/// well-formed RESP is answered with an error frame and the connection
/// closes: the byte stream is desynchronised at that point and nothing after
/// it can be trusted.
///
/// `QUIT` is the third case: the reply is written and flushed, and then this
/// returns. Anything the peer pipelined behind it is deliberately not read —
/// it asked to leave.
pub async fn serve_connection<S, R>(stream: S, router: R, node: NodeInfo)
where
    S: AsyncRead + AsyncWrite + Unpin,
    R: Router,
{
    serve_connection_limited(stream, router, node, MAX_REQUEST_BYTES, IDLE_SHED_AFTER).await;
}

/// [`serve_connection`] with the accumulation ceiling and the idle interval as
/// parameters.
///
/// The ceiling exists so that it can be exercised. Reaching 64 MiB through a
/// pipe is linear work now rather than quadratic, but it is still 64 MiB
/// written, copied and held, for a property a 64 KiB ceiling demonstrates
/// identically — so a test on the real constant would be the slowest thing in
/// the suite by a wide margin, and this layer's stated primary defence would
/// go on having no coverage at all.
///
/// [`IDLE_SHED_AFTER`] is here for the same reason in the other direction: a
/// test that must watch the interval elapse says how long it is rather than
/// waiting out the production one.
pub async fn serve_connection_limited<S, R>(
    mut stream: S,
    router: R,
    node: NodeInfo,
    max_request_bytes: usize,
    idle_shed: Duration,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    R: Router,
{
    let mut decoder = Decoder::new(DecoderLimits {
        max_frame_bytes: max_request_bytes,
        max_in_memory: max_request_bytes,
    });
    // On the heap, not in this future. A fixed array here is capacity every
    // spawned connection task reserves whether or not its peer ever speaks,
    // and it is the whole of what made this future large enough for clippy to
    // complain about.
    let mut read_buf = vec![0u8; READ_FLOOR];
    let mut quiet_reads = 0u32;
    let mut out: Vec<u8> = Vec::new();
    // The chunk under construction. It lives outside the loop so its two
    // buffers keep their capacity across a chunk boundary instead of being
    // rebuilt per drain.
    let mut chunk = Chunk::default();

    let idle = tokio::time::sleep(idle_shed);
    tokio::pin!(idle);
    // The timer is armed only while there is something to give back, and what
    // it compares on firing is whether any read arrived since it was armed —
    // so a busy connection never registers a timer more than once per
    // interval, and never resets one on the read path.
    let mut armed = false;
    let mut reads: u64 = 0;
    let mut reads_when_armed: u64 = 0;
    // A node with no password starts every connection authenticated, so the
    // gate below is inert where there is nothing to gate.
    let mut authenticated = !node.requires_auth();

    loop {
        // Drain every complete frame the decoder already holds before asking
        // the transport for more. The replies accumulate in `out`; the write
        // happens once, at the drain's end.
        let hang_up = match drain_decoder(
            &mut stream,
            &mut out,
            &mut decoder,
            &mut chunk,
            &router,
            &node,
            &mut authenticated,
        )
        .await
        {
            Drained::Dry => false,
            Drained::HangUp => true,
            Drained::Over => return,
        };

        // The drain is over — either dry or hung up — so the open chunk closes
        // before anything is written or read. `QUIT` takes this path too: its
        // `OK` is the last slot of the last chunk, and nothing pipelined
        // behind it was ever decoded.
        if !emit_chunk(&mut stream, &mut out, &router, &mut chunk, &node).await {
            return;
        }
        if !flush_replies(&mut stream, &mut out, &node.net_out).await || hang_up {
            return;
        }

        let got = tokio::select! {
            // `biased` for the reason the shard loop states: unbiased arm
            // choice draws on the runtime's RNG, which is entropy no seed
            // replays. It is also the right priority — bytes a peer has
            // already sent outrank a decision about memory it is not using.
            biased;

            result = stream.read(&mut read_buf) => match result {
                // EOF, or a transport that failed. Either way the connection
                // is over and there is nobody left to tell.
                Ok(0) | Err(_) => return,
                Ok(got) => got,
            },

            () = &mut idle, if armed => {
                if reads == reads_when_armed {
                    shed_connection_buffers(&mut read_buf, &mut decoder, &mut out);
                    quiet_reads = 0;
                    armed = false;
                } else {
                    reads_when_armed = reads;
                    idle.as_mut().reset(Instant::now() + idle_shed);
                }
                // Nothing was read, so there is nothing to decode. The drain
                // at the top of the loop finds the decoder dry, writes
                // nothing, and comes back here.
                continue;
            }
        };
        reads += 1;
        node.net_in
            .fetch_add(got.try_into().unwrap_or(u64::MAX), Ordering::Relaxed);
        decoder.feed(&read_buf[..got]);
        resize_connection_buffers(&mut read_buf, &mut decoder, &mut out, &mut quiet_reads, got);
        // The read buffer standing above its floor is the evidence that this
        // connection grew, and it is the whole of the arming condition: the
        // timer exists to reclaim what that buffer's presence implies, so a
        // connection at the floor has nothing for it to do. Arming and
        // disarming are therefore one decision made from one comparison,
        // rather than an arm here and a disarm on the timer's own path only —
        // which would leave a timer running on every connection the
        // quiet-read hysteresis had already emptied.
        match (armed, read_buf.len() > READ_FLOOR) {
            (false, true) => {
                armed = true;
                reads_when_armed = reads;
                idle.as_mut().reset(Instant::now() + idle_shed);
            }
            (true, false) => armed = false,
            _ => {}
        }
    }
}

/// Sizes a connection's three buffers to what it is actually doing.
///
/// The read buffer is the one that grows here. A read that filled it says the
/// peer had more waiting, so the next one asks for twice as much. A read that
/// used a quarter of it or less is evidence the peer has stopped — but only
/// evidence, so `quiet` counts how much of it has accumulated and the
/// capacity goes back to the floor only once [`READ_QUIET_READS`] of them run
/// consecutively. Anything that is neither resets the count.
///
/// The counter is what keeps the two rules from fighting: without it, growth
/// and shedding sit one doubling apart and an alternating peer reallocates on
/// every read. With it, a single busy read anywhere in the window cancels the
/// shed, so the buffers only shrink for a connection that genuinely went
/// quiet — and then shrink once.
///
/// The decoder and the reply buffer shed on the same verdict rather than on
/// one of their own, and that is the whole reason this function takes them.
/// Each has a floor it manages alone — [`DecoderLimits::SHED`] and
/// [`REPLY_SHED`], a quarter of a megabyte apiece — which is right for a
/// connection between requests and much too generous for one that has
/// stopped, and neither can tell those apart from where it sits: the evidence
/// is the shape of the reads, and it arrives here. Read buffer aside, they
/// are also the larger two, so leaving them out would have shed the smallest
/// third of what a connection holds.
pub fn resize_connection_buffers(
    read_buf: &mut Vec<u8>,
    decoder: &mut Decoder,
    out: &mut Vec<u8>,
    quiet: &mut u32,
    got: usize,
) {
    if got == read_buf.len() {
        *quiet = 0;
        let grown = read_buf.len().saturating_mul(2).min(READ_CEILING);
        read_buf.resize(grown, 0);
        return;
    }
    // At the floor there is nothing to give back, so counting quiet reads
    // would be counting towards a shed that cannot happen. Nothing depends on
    // the reset — the only way out of the floor is the growth branch above,
    // which zeroes the counter itself, so no count can survive into a larger
    // buffer — but a counter left running at the floor would be state with no
    // reader, which is worse to maintain than one line that says so.
    if read_buf.len() == READ_FLOOR || got.saturating_mul(4) > read_buf.len() {
        *quiet = 0;
        return;
    }
    *quiet += 1;
    if *quiet >= READ_QUIET_READS {
        *quiet = 0;
        shed_connection_buffers(read_buf, decoder, out);
    }
}

/// Returns every buffer to the floor.
///
/// The three are shed together because they are evidence of the same thing:
/// this connection is not doing what it grew for. The decoder's and the reply
/// buffer's own floors are a quarter of a megabyte each, which is right for a
/// connection between requests and far too much for one that has stopped.
///
/// It does not touch the quiet-read counter, because the two callers reach it
/// having decided different things: [`resize_connection_buffers`] gets here by
/// filling that window and clears it as part of its own verdict, and the idle
/// timer gets here without consulting it at all. Whoever sheds resets it.
pub fn shed_connection_buffers(read_buf: &mut Vec<u8>, decoder: &mut Decoder, out: &mut Vec<u8>) {
    read_buf.truncate(READ_FLOOR);
    read_buf.shrink_to(READ_FLOOR);
    decoder.shed_to(READ_FLOOR);
    out.shrink_to(READ_FLOOR);
}

/// The chunk under construction: the replies decided so far, and the commands
/// still to be run for the ones that are waiting on a shard.
///
/// One type rather than two vectors passed side by side, because they are one
/// thing with one invariant — every [`Slot::Pending`] indexes this batch — and
/// a pair of parameters is a pair that can be handed to a call in the wrong
/// order or reset one at a time.
#[derive(Default)]
pub struct Chunk {
    /// One entry per request decoded in this chunk, in the order the peer sent
    /// them.
    slots: Vec<Slot>,
    /// The commands the pending slots are waiting on, in dispatch order.
    batch: Vec<Command>,
}

/// Why a pass over the decoder stopped.
pub enum Drained {
    /// Nothing complete is left: the caller may write what the pass earned and
    /// then ask the transport for more bytes.
    Dry,
    /// The peer asked to be disconnected. What the pass earned is still owed
    /// it, so the caller writes and then stops.
    HangUp,
    /// The connection is over and nothing further is owed — the peer is gone,
    /// or the stream desynchronised and has already been told so.
    Over,
}

/// Runs every complete frame the decoder is already holding.
///
/// Split from the connection loop so that each has one job: this one turns
/// bytes that have arrived into replies, and the loop around it decides when
/// to write and when to read. The replies accumulate in `out` rather than
/// being written here — with the one exception the error path states, which
/// has to write because it is not coming back.
pub async fn drain_decoder<S, R>(
    stream: &mut S,
    out: &mut Vec<u8>,
    decoder: &mut Decoder,
    chunk: &mut Chunk,
    router: &R,
    node: &NodeInfo,
    authenticated: &mut bool,
) -> Drained
where
    S: AsyncRead + AsyncWrite + Unpin,
    R: Router,
{
    loop {
        match decoder.try_next() {
            Ok(Some(frame)) => {
                // The gate may replace the action, and the label survives it
                // on purpose: a `GET` refused with `NOAUTH` is still a `GET`,
                // and that is what an operator reading the refusal needs.
                let (action, label) = frame_to_action(frame, node);
                match gated(action, *authenticated) {
                    Action::Dispatch(cmd) => {
                        chunk.slots.push(Slot::Pending(chunk.batch.len(), label));
                        chunk.batch.push(cmd);
                        // A batch at the mark is dispatched here rather than held
                        // until the decoder runs dry — see [`CHUNK_COMMANDS`]. The
                        // peer sees the same frames in the same order, so no
                        // ordering this loop guarantees moves; only how many
                        // commands one message carries.
                        if chunk.batch.len() >= CHUNK_COMMANDS
                            && !emit_chunk(stream, out, router, chunk, node).await
                        {
                            return Drained::Over;
                        }
                    }
                    Action::Unbatched(request) => {
                        // The chunk closes *before* the request runs, and that is
                        // an ordering requirement rather than tidiness: the
                        // commands already batched were written by the peer ahead
                        // of this one, and dispatching this while those wait would
                        // run a `DEL k` before the `SET k v` the peer pipelined in
                        // front of it, empty the keyspace in front of the writes
                        // that filled it, or answer a `KEYS` without the key a
                        // `SET` just wrote.
                        if !emit_chunk(stream, out, router, chunk, node).await {
                            return Drained::Over;
                        }
                        chunk
                            .slots
                            .push(Slot::Ready(request.answer(router, node).await, label));
                    }
                    Action::Reply(frame) | Action::Hello(frame) | Action::Refuse(frame) => {
                        chunk.slots.push(Slot::Ready(frame, label));
                    }
                    Action::Authenticate(outcome) => {
                        chunk
                            .slots
                            .push(Slot::Ready(settle_auth(outcome, authenticated), label));
                    }
                    Action::ReplyThenClose(frame) => {
                        chunk.slots.push(Slot::Ready(frame, label));
                        return Drained::HangUp;
                    }
                }
            }
            // A proper prefix of a valid frame: read more.
            Ok(None) => return Drained::Dry,
            Err(error) => {
                // Terminal, and that covers the accumulation ceiling as well
                // as malformed bytes: either way the decoder holds a half-read
                // frame with no resync point. Report it and go, without
                // draining.
                //
                // The chunk closes *first*, so the error frame is appended
                // behind whatever this drain already earned — the same order
                // the peer would have seen from a flush per reply: the
                // replies, then the refusal. A chunk that could not be emitted
                // means the peer is already gone, and there is nobody left to
                // refuse.
                if !emit_chunk(stream, out, router, chunk, node).await {
                    return Drained::Over;
                }
                append_frame(out, &safe_error(&protocol_error(&error)));
                flush_replies(stream, out, &node.net_out).await;
                return Drained::Over;
            }
        }
    }
}

/// Closes one chunk: dispatches its batch and appends every slot's frame to
/// `out`, in request order.
///
/// Returns `false` if a write failed, which means the peer is gone.
///
/// This is where the two orders meet. The batch is answered grouped by
/// whoever owns the keys — [`Router::dispatch_many`] promises only that reply
/// *i* answers command *i* — and `slots` is the record of where each of those
/// commands sat among the requests the peer actually wrote, connection
/// commands included. Walking the slots is therefore the only thing standing
/// between a batched dispatch and a reordered response stream.
///
/// A batch shorter than its slots claim is not a corruption to propagate: a
/// router that dropped commands leaves the extra slots answered with
/// [`ReplyError::ShardUnavailable`], so the peer still gets one frame per
/// request and the stream stays in step.
///
/// An empty chunk is not a write, and an empty batch is not a dispatch — the
/// drain that answered only connection commands must leave the router
/// untouched, which is a property the layer above tests directly.
pub async fn emit_chunk<S, R>(
    stream: &mut S,
    out: &mut Vec<u8>,
    router: &R,
    chunk: &mut Chunk,
    node: &NodeInfo,
) -> bool
where
    S: AsyncWrite + Unpin,
    R: Router,
{
    if chunk.slots.is_empty() {
        return true;
    }
    let mut replies: Vec<Option<Reply>> = if chunk.batch.is_empty() {
        Vec::new()
    } else {
        // By value: the batch's buffer travels on into the router rather than
        // being copied out of it. `slots` keeps its capacity across chunks;
        // this one is handed over and regrown.
        router
            .dispatch_many(take(&mut chunk.batch))
            .await
            .into_iter()
            .map(Some)
            .collect()
    };
    for slot in chunk.slots.drain(..) {
        let (frame, label) = match slot {
            Slot::Ready(frame, label) => (frame, label),
            Slot::Pending(index, label) => (
                reply_to_frame(
                    replies
                        .get_mut(index)
                        .and_then(Option::take)
                        .unwrap_or(Reply::Error(ReplyError::ShardUnavailable)),
                ),
                label,
            ),
        };
        if let Frame::Error(text) = &frame {
            count_error_reply(node, text);
            // Counted and named in the same breath, deliberately: a counter
            // that moves without a line beside it is the situation this was
            // added to end.
            log_error_reply(node, &label, text);
        }
        append_frame(out, &frame);
        // A chunk that has already earned a write's worth of replies takes it
        // here rather than waiting for the drain to end — see
        // [`REPLY_HIGH_WATER`]. The peer sees the same bytes in the same
        // order, only sooner, so none of the orderings the drain guarantees
        // moves: the write happens *between* two replies, never inside one,
        // and never while a frame is half-decoded.
        if out.len() >= REPLY_HIGH_WATER && !flush_replies(stream, out, &node.net_out).await {
            return false;
        }
    }
    true
}

/// Encodes `frame` onto the end of `out`, which may already hold earlier
/// replies from the same drain.
pub fn append_frame(out: &mut Vec<u8>, frame: &Frame) {
    encode(frame, out);
}

/// Writes everything the drain accumulated and flushes once.
///
/// Returns `false` if the write failed, which means the peer is gone.
///
/// An empty buffer is not a write: a drain that answered nothing — the first
/// turn of the loop, or a read that completed no frame — must not spend a
/// syscall pair saying so.
pub async fn flush_replies<S>(stream: &mut S, out: &mut Vec<u8>, net_out: &AtomicU64) -> bool
where
    S: AsyncWrite + Unpin,
{
    if out.is_empty() {
        return true;
    }
    let delivered = stream.write_all(out).await.is_ok() && stream.flush().await.is_ok();
    // Counted whether or not the write landed: `total_net_output_bytes` is
    // what this node produced for its peers, and a peer that vanished
    // mid-write was still served.
    net_out.fetch_add(out.len().try_into().unwrap_or(u64::MAX), Ordering::Relaxed);
    // Cleared *before* the shed, not after the next `encode`. `Vec::shrink_to`
    // never shrinks below the length, so clearing at the top of the call only
    // would make this a no-op on precisely the write that just grew the
    // buffer — the large reply would keep its allocation until a second reply
    // happened to follow it, and a client that reads one big value and then
    // goes quiet would never send that second one.
    out.clear();
    if out.capacity() > REPLY_SHED {
        out.shrink_to(REPLY_SHED);
    }
    delivered
}

#[cfg(test)]
mod tests {
    use super::*;
    use seedstone_resp::{MAX_ARRAY_LEN, MAX_BULK_LEN};

    #[test]
    fn the_request_ceiling_admits_the_largest_command_the_codec_accepts() {
        // If this ever inverts, a `SET` of two maximum-size payloads would be
        // rejected as oversized despite every frame in it being legal.
        let largest_command_on_the_wire = 2 * MAX_BULK_LEN + 1024;
        assert!(MAX_REQUEST_BYTES > largest_command_on_the_wire);

        // The same claim in the other direction, which binding this constant
        // to `max_in_memory` created a second way to break. An array of
        // `MAX_ARRAY_LEN` elements is a length the codec accepts, and its
        // empty `Frame`s alone must fit — otherwise `*1048576\r\n` starts
        // being refused at its header with nothing here to say so. The margin
        // today is 32 MiB against 64 MiB, and neither `size_of::<Frame>()` nor
        // `MAX_ARRAY_LEN` is a contract: one wider variant, or one doubling,
        // closes it.
        assert!(MAX_ARRAY_LEN.saturating_mul(size_of::<Frame>()) < MAX_REQUEST_BYTES);
    }

    #[test]
    fn the_log_ceiling_admits_the_largest_record_one_key_can_produce() {
        // The replication log debug-asserts that no record body exceeds
        // `MAX_BODY_LEN`, and a record describes exactly one key — so the
        // largest body the command layer could ever hand it is a key and a
        // value at the codec's bulk ceiling, plus framing. Today that assert
        // is unreachable because every payload is empty; this test is what
        // keeps 16 MiB-under-64 MiB a stated contract rather than a
        // coincidence of two constants, so that raising `MAX_BULK_LEN` or
        // shrinking `MAX_BODY_LEN` fails here, not in a release-build log
        // whose reader refuses the record. A payload that stops describing
        // one key re-opens this arithmetic, and inherits this test.
        let largest_one_key_payload = 2 * MAX_BULK_LEN + 1024;
        assert!(largest_one_key_payload < seedstone_core::log::MAX_BODY_LEN);
    }

    #[test]
    fn the_read_buffer_grows_while_it_fills_and_sheds_when_it_stops() {
        let mut decoder = Decoder::new(DecoderLimits::default());
        let mut out: Vec<u8> = Vec::new();
        let mut read_buf = vec![0u8; READ_FLOOR];
        let mut quiet = 0u32;

        // Filling reads double it, up to the ceiling and no further.
        for _ in 0..32 {
            let got = read_buf.len();
            resize_connection_buffers(&mut read_buf, &mut decoder, &mut out, &mut quiet, got);
        }
        assert_eq!(read_buf.len(), READ_CEILING);

        // A read that used more than a quarter holds the size: this is the
        // steady state, and reallocating through it would cost more than the
        // capacity does.
        for _ in 0..2 * READ_QUIET_READS {
            let half = READ_CEILING / 2;
            resize_connection_buffers(&mut read_buf, &mut decoder, &mut out, &mut quiet, half);
            assert_eq!(read_buf.len(), READ_CEILING);
        }

        // A quarter-or-less read is evidence, not a verdict: the capacity is
        // held until the evidence accumulates.
        let quarter = READ_CEILING / 4;
        for _ in 1..READ_QUIET_READS {
            resize_connection_buffers(&mut read_buf, &mut decoder, &mut out, &mut quiet, quarter);
            assert_eq!(read_buf.len(), READ_CEILING, "shed before the window ran");
        }
        resize_connection_buffers(&mut read_buf, &mut decoder, &mut out, &mut quiet, quarter);
        assert_eq!(read_buf.len(), READ_FLOOR);
        assert!(read_buf.capacity() <= READ_FLOOR * 2);
    }

    /// The pattern the hysteresis exists for.
    ///
    /// Growth and shedding sit one doubling apart, so a peer alternating a
    /// buffer-filling read with a quarter-sized one hits both conditions
    /// forever. Without the counter that is a reallocation on every read, and
    /// five doublings to climb back after each shed. The buffer must instead
    /// settle: a busy read anywhere in the window cancels the shed.
    #[test]
    fn an_alternating_peer_does_not_thrash_the_read_buffer() {
        let mut decoder = Decoder::new(DecoderLimits::default());
        let mut out: Vec<u8> = Vec::new();
        let mut read_buf = vec![0u8; READ_FLOOR];
        let mut quiet = 0u32;

        for _ in 0..64 {
            let full = read_buf.len();
            resize_connection_buffers(&mut read_buf, &mut decoder, &mut out, &mut quiet, full);
            let quarter = read_buf.len() / 4;
            resize_connection_buffers(&mut read_buf, &mut decoder, &mut out, &mut quiet, quarter);
        }
        assert_eq!(
            read_buf.len(),
            READ_CEILING,
            "the buffer fell back instead of settling at the ceiling"
        );
    }
}
