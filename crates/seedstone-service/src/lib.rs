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
//! the system acts on, so it owns the limits:
//!
//! - **Where a command is answered.** A command about the connection itself or
//!   about the node behind it — `PING`, `ECHO`, `QUIT`, `HELLO`, `INFO`,
//!   `COMMAND`, `CLIENT` — has no key, so there is no shard it could belong
//!   to; it is answered here and no shard hears of it. Only keyed commands
//!   become messages. [`Action`] is that decision made explicit. What those
//!   answers need to know about the process they run in arrives as
//!   [`NodeInfo`], because this layer has no clock, no port and no way to
//!   count its peers of its own.
//!
//! - **Bounded buffering.** [`MAX_REQUEST_BYTES`] caps what one connection can
//!   make the server hold, on top of the per-frame ceilings the codec
//!   enforces ([`seedstone_resp::MAX_BULK_LEN`],
//!   [`seedstone_resp::MAX_ARRAY_LEN`]). Without a cap, a peer
//!   that opens a frame and never finishes it is a slow memory leak with a
//!   connection attached. The cap is *set* here and *enforced* by the
//!   [`Decoder`] this layer hands it to — see [`MAX_REQUEST_BYTES`] for why
//!   the two are not the same place.
//!
//! - **Giving the buffers back.** A connection sizes its three buffers to what
//!   it is doing ([`resize_connection_buffers`]) and returns them to the floor
//!   when it stops. It stops in two distinguishable ways — still talking in
//!   small requests, which the reads themselves report, and gone silent, which
//!   only a clock can report — so there are two signals and one shed
//!   ([`IDLE_SHED_AFTER`]).
//!
//! - **No response splitting.** Every error frame this module emits passes
//!   through [`safe_error`] first. A `Frame::Error` is terminated by the first
//!   `\r\n` after its type byte, so text carrying either byte would let a peer
//!   dictate frames the server never meant to send — and the codec's guard
//!   against that is a debug assertion, which is not there in release. This is
//!   the enforcement point that is.

use seedstone_core::shard::{Command, Cond, Expiry, Reply, ReplyError, Router, parse_i64};
use seedstone_resp::{Decoder, DecoderLimits, Frame, ParseError, encode};
use std::future::{Future, poll_fn};
use std::mem::take;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
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
const READ_FLOOR: usize = 2 * 1024;

/// The largest single `read` a connection grows to.
///
/// Reached by doubling, one filling read at a time, so a peer that is
/// streaming gets fewer and larger syscalls while one that sends a command
/// and waits never pays for the capacity.
const READ_CEILING: usize = 64 * 1024;

/// Consecutive small reads before the read buffer gives its capacity back.
///
/// Hysteresis, and it is load-bearing rather than tidy. Growth and shedding
/// are one step apart — a read that fills a 4 KiB buffer doubles it, and a
/// 2 KiB read then satisfies the shed condition on the 8 KiB result — so a
/// peer alternating full and quarter reads would reallocate on every single
/// one, and pay five doublings to climb back each time. Requiring the quiet
/// to persist makes shedding a statement about a connection that stopped
/// rather than about one read that was small.
const READ_QUIET_READS: u32 = 4;

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
const REPLY_SHED: usize = DecoderLimits::SHED;

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
const REPLY_HIGH_WATER: usize = REPLY_SHED;

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
const CHUNK_COMMANDS: usize = 128;

/// How many buckets one step of a keyspace walk visits before answering.
///
/// It is the unit of occupancy: larger holds a shard for longer per step and
/// pays less per-envelope overhead, smaller yields sooner and pays more. Sized
/// against the core's `EXPIRE_BUCKETS_PER_TICK`, which is the other bounded
/// walk over the same table.
///
/// It serves the two walking commands differently, and deliberately with one
/// number. `KEYS` carries no `COUNT` on the wire, so this *is* its step.
/// `SCAN` takes a `COUNT` from a client, and this is the ceiling that request
/// is clamped to — because occupancy is the server's to bound whoever asked,
/// and a `COUNT` honoured literally would let one call walk an entire cycle
/// and hold the shard for it. A clamped walk still returns every key; it takes
/// the round trips this step size implies rather than the ones the client
/// asked for.
const WALK_STEP_BUCKETS: usize = 256;

/// How much of a peer-supplied byte string an error message may quote.
const QUOTE_LIMIT: usize = 32;

/// What a peer asking for a protocol this server does not speak is told.
///
/// Byte-exact to Redis, and load-bearing rather than cosmetic: go-redis v9
/// opens every connection with `HELLO 3` and downgrades to RESP2 on exactly
/// this prefix. A clearer message would break the client.
pub const NOPROTO: &str = "NOPROTO unsupported protocol version";

/// What a peer spelling a command's options wrong is told.
///
/// Byte-exact to Redis, which says no more than this whichever option was at
/// fault: the same text for an unknown option, for a repeated one, and for one
/// whose argument is missing.
pub const SYNTAX_ERROR: &str = "ERR syntax error";

/// What `SCAN` visits when the client does not say. Redis's default, and the
/// number the deployed clients this gate exercises leave unset.
const SCAN_DEFAULT_COUNT: usize = 10;

/// What a peer resuming a walk from something this server never issued is
/// told.
///
/// It covers both ways that happens: a cursor that is not the canonical
/// decimal this server prints, and one whose high bits name a shard this node
/// does not have. Redis has only the second failure and spells it
/// `ERR invalid cursor`; a client that can act on either can act on both.
pub const INVALID_CURSOR: &str = "ERR invalid cursor";

/// The largest span, in seconds, an expiry option may name.
///
/// Redis's own ceiling, and the reason it has one is arithmetic: it holds a
/// deadline in milliseconds, so a span in seconds is multiplied by a thousand
/// before anything is done with it, and a value past `i64::MAX / 1000` cannot
/// survive that. Matching the ceiling is what makes the refusal byte-exact
/// rather than merely sensible.
///
/// It also closes a hole on this side. The shard turns a span into an
/// [`Instant`](std::time::Instant) and stores *no deadline* when that
/// arithmetic leaves the clock's range — the only answer available to it, since
/// the alternative is a panic on a number a peer chose. Reached from `EXPIRE`,
/// that would clear the deadline a key already had and still report success,
/// which is a key made immortal by an argument nobody could have meant. The
/// number is refused here, where it is still a number.
const MAX_EXPIRE_SECONDS: i64 = i64::MAX / 1000;

/// What this server answers `HELLO` with, and what it calls itself.
const SERVER_NAME: &str = "seedstone";

/// The deployment shape this node is in, as `HELLO` and `INFO` both report it.
///
/// One constant for the two answers rather than a literal in each: a node that
/// told `HELLO` one thing and `INFO` another would be a node whose clients
/// disagree about what they are connected to, and that is exactly the kind of
/// drift a second literal invites.
const SERVER_MODE: &str = "standalone";

/// The wall-clock reading a node with no wall clock reports: 2023-11-14
/// 22:13:20 UTC, in milliseconds.
///
/// A round number in the recent past, chosen only to be recognisable in a
/// failure message. See [`NodeInfo::for_tests`].
const FIXED_UNIX_MILLIS: u64 = 1_700_000_000_000;

/// What a connection may say about the node it is running on.
///
/// Every field is a fact about the process, not about the connection, so this
/// is assembled once where the process is — the composition root — and cloned
/// per connection. It is a parameter rather than something this layer reads for
/// itself because none of it is knowable here: the port is the one the kernel
/// chose, the start is a reading of a clock, and the peer count is maintained
/// by the accept loop. Passing them in is what keeps the connection layer a
/// pure function of its inputs, and therefore replayable.
///
/// [`now_unix_millis`](Self::now_unix_millis) is the exception that proves the
/// rule: it is not a fact but the dependency that supplies one, because the
/// fact it supplies changes while a connection is being served. Everything
/// said above about why the others are passed in applies to it doubly.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    /// The version this node reports.
    pub version: &'static str,
    /// The port the listener actually bound, which with an ephemeral port is
    /// not the port the configuration asked for.
    pub tcp_port: u16,
    /// When the node started, on the monotonic clock.
    ///
    /// A [`tokio::time::Instant`] and never a `SystemTime`: uptime is a span,
    /// and a span measured against a clock an operator can step backwards is
    /// not a span. It is also the clock the simulator controls, so a replay
    /// reports the uptime the run had rather than the one the wall had.
    pub started: Instant,
    /// How many connections are attached right now.
    ///
    /// Shared with whoever accepts connections, which is the only party that
    /// can maintain it; this layer only ever reads it.
    pub connected: Arc<AtomicU64>,
    /// Unix time, in milliseconds — the wall clock, injected rather than read.
    ///
    /// One command family needs it: `SET`'s `EXAT`/`PXAT` name a deadline on
    /// the clock people set their watches by, and nothing else in this server
    /// does. Every other deadline is a span against the monotonic clock, which
    /// is why [`started`](Self::started) is an [`Instant`] and says there why.
    ///
    /// It arrives as a function rather than as a reading because a reading
    /// taken here would be stale by the time a connection used it, and as a
    /// *dependency* rather than a call to `SystemTime::now` because the wall
    /// clock is the one input a replay cannot reproduce: the simulator drives
    /// this layer, and it controls the monotonic clock and nothing else. A
    /// simulated node is handed a clock of its own, so a run that resolves an
    /// absolute deadline resolves it the same way every time it is replayed;
    /// the real node is handed the real one, so a client's `EXAT` means what
    /// it means everywhere else.
    pub now_unix_millis: fn() -> u64,
}

impl NodeInfo {
    /// A node description for callers that have no node to describe: the tests
    /// here, and the simulator.
    ///
    /// The port is Redis's default, so that the value is recognisable rather
    /// than arbitrary. The count starts at zero and stays there: maintaining it
    /// belongs to whoever accepts connections, and neither caller has a
    /// workload that asks.
    ///
    /// The wall clock stands still, at [`FIXED_UNIX_MILLIS`]. Neither caller
    /// has a real one to offer — there is no simulated `SystemTime`, and the
    /// only clock the simulator advances is the monotonic one — so a frozen
    /// reading is the honest answer rather than a limitation: it makes an
    /// absolute deadline resolve identically in every replay of a run, which
    /// is the whole reason this is a parameter.
    #[must_use]
    pub fn for_tests() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
            tcp_port: 6379,
            started: Instant::now(),
            connected: Arc::new(AtomicU64::new(0)),
            now_unix_millis: || FIXED_UNIX_MILLIS,
        }
    }
}

/// What to do with one request frame.
///
/// The distinction the type draws is where a command is answered.
/// [`Command`]s belong to a shard and travel; the rest — the connection's own
/// business, and everything the peer got wrong — is answered right here,
/// without a message ever leaving the connection task.
enum Action {
    /// A keyed command: route it and reply with what the shard says.
    Dispatch(Command),
    /// A request that cannot travel inside a chunk — see [`Unbatched`].
    Unbatched(Unbatched),
    /// Answer with this frame; the connection continues.
    Reply(Frame),
    /// Answer with this frame, then hang up.
    ReplyThenClose(Frame),
}

/// A request that is answered on its own, after the chunk in front of it has
/// been dispatched and before anything behind it is.
///
/// What they have in common is that each needs every command the peer
/// pipelined ahead of it to have already run. Holding them in one variant is
/// what keeps that ordering requirement in one place instead of restated at
/// each call site, where the next one would be the one that forgot.
enum Unbatched {
    /// One request's keys, split into the one-key commands the shards that
    /// own them can run, carrying how their replies become one again — see
    /// [`fan_out`], which also states what the split costs in atomicity.
    ///
    /// Usually several keys, but not only: a fold that changes the shape of a
    /// lone reply sends a one-key request through here too — see
    /// [`Fold::is_identity_on_one`].
    FanOut {
        /// The commands, in the order the peer named their keys.
        cmds: Vec<Command>,
        /// What the replies are folded into.
        fold: Fold,
    },
    /// A request naming no key at all: it reaches every shard and the answers
    /// are folded into one reply — see [`broadcast`].
    Every(Command),
    /// A request naming a pattern rather than a key. The edge walks every
    /// shard itself, a bounded step at a time, and gathers what matches — see
    /// [`keys`]. It is not an [`Every`](Unbatched::Every) because one command
    /// per shard is not one *step* per shard: a walk is a loop.
    Keys(Vec<u8>),
    /// One step of a client-driven walk: the cursor says which shard and
    /// where in it, and the answer says where to resume — see [`scan`]. It is
    /// the only one of these that reaches a single shard, and it is here
    /// rather than beside the keyed commands because the shard it reaches is
    /// unpacked at the edge instead of hashed from a key.
    Scan {
        /// The packed cursor the client sent, untrusted.
        cursor: u64,
        /// `MATCH`, filtered on the shard rather than here.
        pattern: Option<Vec<u8>>,
        /// `COUNT`, already bounded by [`WALK_STEP_BUCKETS`].
        count: usize,
    },
}

/// What a fan-out's replies are folded into.
///
/// Carried by the request rather than read off the replies, because the two
/// folds are not distinguishable from a reply: a one-key `MGET` and a `GET`
/// both come back `Reply::Bulk`, and only the request knows that one of the
/// two still owes the peer an array around it. Inferring it here would answer
/// that `MGET` with a bare bulk and desynchronise every client that counts
/// elements.
///
/// Both places that read this match the whole enum, so a third fold added
/// later is a compile error at each of them rather than a reply quietly folded
/// the wrong way — miscounted on one side, mis-shaped on the other.
#[derive(Clone, Copy)]
enum Fold {
    /// One integer: every reply is a count and the answer is their sum —
    /// `DEL`, `EXISTS`.
    Sum,
    /// One array: every reply is an entry, one per command — `MGET`.
    Array,
}

impl Fold {
    /// Whether folding a lone reply gives back exactly that reply's frame.
    ///
    /// [`Sum`](Fold::Sum) over one count is that count, so a one-key `DEL`
    /// needs no fan-out at all and can travel in the drain's batch like any
    /// other keyed command. [`Array`](Fold::Array) cannot: an array of one is
    /// a different frame from the bulk inside it, and the difference is the
    /// whole reply as far as a client parsing it is concerned.
    const fn is_identity_on_one(self) -> bool {
        match self {
            Self::Sum => true,
            Self::Array => false,
        }
    }
}

impl Unbatched {
    /// Runs the request and answers with the single frame it earns.
    async fn answer<R: Router>(self, router: &R) -> Frame {
        match self {
            Self::FanOut { cmds, fold } => fan_out(router, cmds, fold).await,
            Self::Every(cmd) => broadcast(router, cmd).await,
            Self::Keys(pattern) => keys(router, pattern).await,
            Self::Scan {
                cursor,
                pattern,
                count,
            } => scan(router, cursor, pattern, count).await,
        }
    }
}

/// One decoded request's place in a chunk.
///
/// A request is either answered already — a connection command, or anything
/// the peer got wrong — or waiting on the router, in which case the slot
/// carries where in the chunk's batch its command went. Holding both kinds in
/// one ordered vector is what puts a batch back into request order after it
/// comes back grouped by whoever ran it.
enum Slot {
    /// Answered here; this frame goes out as it stands.
    Ready(Frame),
    /// Answered by the router; the index is into the chunk's batch.
    Pending(usize),
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
async fn serve_connection_limited<S, R>(
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
    // The chunk under construction. Both live outside the loop so their
    // capacity survives a chunk boundary instead of being rebuilt per drain.
    let mut slots: Vec<Slot> = Vec::new();
    let mut batch: Vec<Command> = Vec::new();

    let idle = tokio::time::sleep(idle_shed);
    tokio::pin!(idle);
    // The timer is armed only while there is something to give back, and what
    // it compares on firing is whether any read arrived since it was armed —
    // so a busy connection never registers a timer more than once per
    // interval, and never resets one on the read path.
    let mut armed = false;
    let mut reads: u64 = 0;
    let mut reads_when_armed: u64 = 0;

    loop {
        // Drain every complete frame the decoder already holds before asking
        // the transport for more. The replies accumulate in `out`; the write
        // happens once, at the drain's end.
        let mut hang_up = false;
        let mut drained = false;
        while !drained && !hang_up {
            match decoder.try_next() {
                Ok(Some(frame)) => match frame_to_action(frame, &node) {
                    Action::Dispatch(cmd) => {
                        slots.push(Slot::Pending(batch.len()));
                        batch.push(cmd);
                        // A batch at the mark is dispatched here rather than
                        // held until the decoder runs dry — see
                        // [`CHUNK_COMMANDS`]. The peer sees the same frames in
                        // the same order, so no ordering this loop guarantees
                        // moves; only how many commands one message carries.
                        if batch.len() >= CHUNK_COMMANDS
                            && !emit_chunk(&mut stream, &mut out, &router, &mut slots, &mut batch)
                                .await
                        {
                            return;
                        }
                    }
                    Action::Unbatched(request) => {
                        // The chunk closes *before* the request runs, and that
                        // is an ordering requirement rather than tidiness: the
                        // commands already batched were written by the peer
                        // ahead of this one, and dispatching this while those
                        // wait would run a `DEL k` before the `SET k v` the
                        // peer pipelined in front of it, empty the keyspace in
                        // front of the writes that filled it, or answer a
                        // `KEYS` without the key a `SET` just wrote.
                        if !emit_chunk(&mut stream, &mut out, &router, &mut slots, &mut batch).await
                        {
                            return;
                        }
                        slots.push(Slot::Ready(request.answer(&router).await));
                    }
                    Action::Reply(frame) => slots.push(Slot::Ready(frame)),
                    Action::ReplyThenClose(frame) => {
                        slots.push(Slot::Ready(frame));
                        hang_up = true;
                    }
                },
                // A proper prefix of a valid frame: read more.
                Ok(None) => drained = true,
                Err(error) => {
                    // Terminal, and that now covers the accumulation ceiling
                    // as well as malformed bytes: either way the decoder holds
                    // a half-read frame with no resync point. Report it and
                    // go, without draining.
                    //
                    // The chunk closes *first*, so the error frame is appended
                    // behind whatever this drain already earned — the same
                    // order the peer would have seen from a flush per reply:
                    // the replies, then the refusal. A chunk that could not be
                    // emitted means the peer is already gone, and there is
                    // nobody left to refuse.
                    if !emit_chunk(&mut stream, &mut out, &router, &mut slots, &mut batch).await {
                        return;
                    }
                    append_frame(&mut out, &safe_error(&protocol_error(&error)));
                    flush_replies(&mut stream, &mut out).await;
                    return;
                }
            }
        }

        // The drain is over — either dry or hung up — so the open chunk closes
        // before anything is written or read. `QUIT` takes this path too: its
        // `OK` is the last slot of the last chunk, and nothing pipelined
        // behind it was ever decoded.
        if !emit_chunk(&mut stream, &mut out, &router, &mut slots, &mut batch).await {
            return;
        }
        if !flush_replies(&mut stream, &mut out).await || hang_up {
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
fn resize_connection_buffers(
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
fn shed_connection_buffers(read_buf: &mut Vec<u8>, decoder: &mut Decoder, out: &mut Vec<u8>) {
    read_buf.truncate(READ_FLOOR);
    read_buf.shrink_to(READ_FLOOR);
    decoder.shed_to(READ_FLOOR);
    out.shrink_to(READ_FLOOR);
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
async fn emit_chunk<S, R>(
    stream: &mut S,
    out: &mut Vec<u8>,
    router: &R,
    slots: &mut Vec<Slot>,
    batch: &mut Vec<Command>,
) -> bool
where
    S: AsyncWrite + Unpin,
    R: Router,
{
    if slots.is_empty() {
        return true;
    }
    let mut replies: Vec<Option<Reply>> = if batch.is_empty() {
        Vec::new()
    } else {
        // By value: the batch's buffer travels on into the router rather than
        // being copied out of it. `slots` keeps its capacity across chunks;
        // this one is handed over and regrown.
        router
            .dispatch_many(take(batch))
            .await
            .into_iter()
            .map(Some)
            .collect()
    };
    for slot in slots.drain(..) {
        let frame = match slot {
            Slot::Ready(frame) => frame,
            Slot::Pending(index) => reply_to_frame(
                replies
                    .get_mut(index)
                    .and_then(Option::take)
                    .unwrap_or(Reply::Error(ReplyError::ShardUnavailable)),
            ),
        };
        append_frame(out, &frame);
        // A chunk that has already earned a write's worth of replies takes it
        // here rather than waiting for the drain to end — see
        // [`REPLY_HIGH_WATER`]. The peer sees the same bytes in the same
        // order, only sooner, so none of the orderings the drain guarantees
        // moves: the write happens *between* two replies, never inside one,
        // and never while a frame is half-decoded.
        if out.len() >= REPLY_HIGH_WATER && !flush_replies(stream, out).await {
            return false;
        }
    }
    true
}

/// Encodes `frame` onto the end of `out`, which may already hold earlier
/// replies from the same drain.
fn append_frame(out: &mut Vec<u8>, frame: &Frame) {
    encode(frame, out);
}

/// Writes everything the drain accumulated and flushes once.
///
/// Returns `false` if the write failed, which means the peer is gone.
///
/// An empty buffer is not a write: a drain that answered nothing — the first
/// turn of the loop, or a read that completed no frame — must not spend a
/// syscall pair saying so.
async fn flush_replies<S>(stream: &mut S, out: &mut Vec<u8>) -> bool
where
    S: AsyncWrite + Unpin,
{
    if out.is_empty() {
        return true;
    }
    let delivered = stream.write_all(out).await.is_ok() && stream.flush().await.is_ok();
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

/// The answer to a reply this layer holds but cannot turn into the frame the
/// request is owed.
///
/// Two places can meet one, and neither is reachable from anything a peer can
/// send: [`reply_to_frame`] handed a scan step, and [`fan_out`]'s array fold
/// handed something that is not a bulk. Both would be a wiring mistake made
/// here, and both answer this rather than a frame of the wrong shape — one bad
/// reply on one connection, instead of a stream the client parses happily and
/// reads wrong.
const UNRENDERABLE_REPLY: &str = "ERR internal reply could not be rendered";

/// Translates a shard's [`Reply`] into the frame that carries it.
fn reply_to_frame(reply: Reply) -> Frame {
    match reply {
        Reply::Ok => Frame::Simple("OK".into()),
        Reply::Bulk(None) => Frame::Null,
        Reply::Bulk(Some(value)) => Frame::Bulk(value),
        Reply::Removed(removed) => Frame::Integer(i64::from(removed)),
        Reply::Integer(n) => Frame::Integer(n),
        // A scan step's reply never reaches a client under this name. It is
        // the shard-side half of a walk the edge drives itself, and whoever
        // drives one reads the cursor and the keys directly — a client that
        // is handed a cursor has to be handed the packed one, which only the
        // driver can build. Nothing routes a step here today; the arm is what
        // keeps a routing mistake made later a bad reply on one connection
        // rather than a panicked connection task.
        Reply::Scan { .. } => Frame::Error(UNRENDERABLE_REPLY.into()),
        // No `safe_error` here, and that is not an omission. A shard error is
        // a [`ReplyError`] variant, so its text is a literal in `shard.rs`
        // rather than anything a router composed — the type is what rules out
        // a terminator, and `every_shard_error_is_frame_safe` checks the whole
        // set. `safe_error` still guards the paths below, where the text is
        // built from bytes a peer chose.
        Reply::Error(error) => Frame::Error(error.wire_text().to_owned()),
    }
}

/// Runs one keyspace-wide command on every shard and folds the answers into
/// the single frame the peer sees.
///
/// **An error from any shard wins.** A keyspace-wide command that reached most
/// of the keyspace has not done what it was asked, and answering from the part
/// that worked would tell the peer something less true than the failure does.
///
/// The shards that succeeded are not rolled back, and cannot be: this layer
/// has no transaction to unwind and each shard has already applied what it
/// applied. So the error means "not everywhere", not "nowhere" — which is the
/// same thing a fan-out's error means, and for the same reason.
///
/// Short of an error, the fold is decided by the command, and it is read off
/// the command *before* it travels — the command itself is moved into the
/// router. `DBSIZE` sums, which is what makes it the size of the keyspace
/// rather than of a shard; every other keyspace-wide command asks each shard
/// to do something and is answered `+OK` once they all have.
async fn broadcast<R: Router>(router: &R, cmd: Command) -> Frame {
    let sum = matches!(cmd, Command::DbSize);
    let mut total: i64 = 0;
    for reply in router.dispatch_every(cmd).await {
        match reply {
            // Saturating for the reason [`fan_out`] saturates: a keyspace
            // larger than `i64::MAX` is not reachable, and wrapping into a
            // negative count would be a worse answer than the ceiling.
            Reply::Integer(n) => total = total.saturating_add(n),
            Reply::Ok => {}
            other => return reply_to_frame(other),
        }
    }
    if sum {
        Frame::Integer(total)
    } else {
        Frame::Simple("OK".into())
    }
}

/// Every key matching `pattern`, gathered from every shard.
///
/// One cursor loop per shard, run concurrently and joined. Each step is an
/// ordinary envelope, so this occupies a shard for the length of one step
/// rather than for the length of the walk, and the walks themselves overlap
/// rather than queueing behind each other.
///
/// The result is accumulated here before any of it reaches the wire, which is
/// the cost this shape accepts and the reason `SCAN` exists. Duplicates are
/// removed: a table that doubles mid-walk can return a key twice, which is
/// `SCAN`'s documented behaviour and would be surprising in a single answer.
///
/// The walk is `O(keyspace)` and competes for CPU with traffic while it runs.
/// What it no longer does is stop the server for its duration, which is the
/// difference worth having and not the same thing as being cheap.
async fn keys<R: Router>(router: &R, pattern: Vec<u8>) -> Frame {
    let walks: Vec<_> = (0..router.shards())
        .map(|shard| {
            let pattern = pattern.clone();
            async move {
                let mut found: Vec<Vec<u8>> = Vec::new();
                let mut cursor = 0u64;
                loop {
                    let reply = router
                        .dispatch_at(
                            shard,
                            Command::ScanStep {
                                cursor,
                                count: WALK_STEP_BUCKETS,
                                // Cloned per step, and it has to be: the
                                // command is moved into the router and the
                                // reply does not hand the pattern back, so
                                // there is nothing to carry forward. Hoisting
                                // it would need a shared, cheaply-cloned
                                // pattern in the command, which is a wider
                                // change than a keyspace walk's per-step
                                // allocation is worth beside the keys it
                                // returns in the same step.
                                pattern: Some(pattern.clone()),
                            },
                        )
                        .await;
                    match reply {
                        Reply::Scan { cursor: next, keys } => {
                            found.extend(keys);
                            cursor = next;
                            if cursor == 0 {
                                return Ok(found);
                            }
                        }
                        Reply::Error(error) => return Err(error),
                        // A router that answered something else did not run
                        // the step, and there is no partial walk to report:
                        // this is the shard failing to answer, spelled the way
                        // the dispatch path already spells that.
                        _ => return Err(ReplyError::ShardUnavailable),
                    }
                }
            }
        })
        .collect();

    let mut all: Vec<Vec<u8>> = Vec::new();
    for walk in join_all(walks).await {
        match walk {
            Ok(found) => all.extend(found),
            // One shard that could not answer makes the whole reply wrong, and
            // a short array is a wrong answer a client cannot detect. Say so
            // instead.
            Err(error) => return Frame::Error(error.wire_text().to_owned()),
        }
    }
    all.sort_unstable();
    all.dedup();
    Frame::Array(all.into_iter().map(Frame::Bulk).collect())
}

/// How many low bits of a `SCAN` cursor belong to a shard's own cursor.
///
/// The remaining 16 carry the shard. A shard count is a `u16` everywhere it
/// matters, and a dict's cursor is masked to its table size — 2^48 buckets is
/// a table this implementation cannot reach — so neither half is cramped.
const CURSOR_INTERNAL_BITS: u32 = 48;

/// The low [`CURSOR_INTERNAL_BITS`] of a cursor: the part a shard issued.
const CURSOR_INTERNAL_MASK: u64 = (1 << CURSOR_INTERNAL_BITS) - 1;

/// Packs a shard and its own cursor into the one integer `SCAN` exchanges.
///
/// `0` is both "start the walk" and "the walk is over", which is what makes a
/// multi-shard walk expressible in a client that knows nothing about shards:
/// shard 0 begins at 0, and a shard that finishes hands back the next shard's
/// start, which is non-zero for every shard but the first. Only the last
/// shard's completion produces 0 again.
fn pack_cursor(shard: u16, internal: u64) -> u64 {
    (u64::from(shard) << CURSOR_INTERNAL_BITS) | (internal & CURSOR_INTERNAL_MASK)
}

/// Splits a cursor a client handed back. Total: every `u64` is some pair.
///
/// Nothing here rejects anything. A cursor is peer-supplied, so the shard it
/// names may not exist — that is the dispatch path's refusal to make, and it
/// needs the shard count, which the packing deliberately does not know.
fn unpack_cursor(cursor: u64) -> (u16, u64) {
    let shard = u16::try_from(cursor >> CURSOR_INTERNAL_BITS)
        .expect("shifting 48 of 64 bits out leaves 16, which is a u16");
    (shard, cursor & CURSOR_INTERNAL_MASK)
}

/// One `SCAN` call: one shard, one step, and the cursor the client resumes at.
///
/// A shard that finishes hands back the next shard's start rather than 0, so
/// the client walks the whole keyspace without ever being told there is more
/// than one. Only the last shard finishing produces 0.
///
/// One call costs what a `GET` costs: no fan-out, no barrier, one envelope to
/// one shard. That is the difference from [`keys`], and the reason a client
/// with a large keyspace should be walking it with this.
///
/// The step may legitimately answer no keys with a non-zero cursor — a stretch
/// of empty buckets, or a `MATCH` that excluded everything. Redis behaves the
/// same way and clients handle it; a server that looped until it had keys
/// would be answering an unbounded call.
async fn scan<R: Router>(router: &R, cursor: u64, pattern: Option<Vec<u8>>, count: usize) -> Frame {
    let shards = router.shards();
    let (shard, internal) = unpack_cursor(cursor);
    // The shard came out of an integer the peer chose, so this is where it
    // stops being trusted. `dispatch_at` would refuse it too; refusing it here
    // is what makes the refusal say `invalid cursor` rather than name a shard
    // to a client that has no idea this server has any.
    if shard >= shards {
        return Frame::Error(INVALID_CURSOR.to_owned());
    }
    let reply = router
        .dispatch_at(
            shard,
            Command::ScanStep {
                cursor: internal,
                count,
                pattern,
            },
        )
        .await;
    let (next, keys) = match reply {
        Reply::Scan { cursor: next, keys } if next != 0 => (pack_cursor(shard, next), keys),
        // This shard is spent: hand back the next one's start, or 0 if it was
        // the last. `shard + 1` cannot overflow — `shard` is below `shards`,
        // which is a `u16`, so it is at most `u16::MAX - 1` here.
        Reply::Scan { keys, .. } => {
            let next = shard + 1;
            let cursor = if next < shards {
                pack_cursor(next, 0)
            } else {
                0
            };
            (cursor, keys)
        }
        Reply::Error(error) => return Frame::Error(error.wire_text().to_owned()),
        // A router that answered something else did not run the step, which is
        // the shard failing to answer — spelled the way [`keys`] spells it.
        _ => return Frame::Error(ReplyError::ShardUnavailable.wire_text().to_owned()),
    };
    Frame::Array(vec![
        // A bulk string, not an integer: that is what Redis sends and what
        // clients parse. A client that fed an integer back would be sending a
        // cursor this server never issued.
        Frame::Bulk(next.to_string().into_bytes()),
        Frame::Array(keys.into_iter().map(Frame::Bulk).collect()),
    ])
}

/// Drives every future to completion concurrently, and gathers the outputs in
/// the order the futures were given rather than the order they finished.
///
/// Hand-rolled because the workspace carries no futures crate and one walk
/// does not justify adding one. It also deliberately does not spawn: a spawned
/// task's completion order belongs to the runtime, so the order two shards'
/// walks finished in could differ between two runs of one seed — the
/// non-determinism [`Router::dispatch_every`] gathers by index to avoid.
/// Polling a fixed vector in index order cannot vary.
///
/// No future is polled again after it returns `Ready`: the slot that holds its
/// output is filled in the same step, and a filled slot is skipped on every
/// later pass.
async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut pending: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut done: Vec<Option<F::Output>> = Vec::new();
    done.resize_with(pending.len(), || None);
    let mut left = pending.len();

    poll_fn(move |cx| {
        for (slot, future) in done.iter_mut().zip(pending.iter_mut()) {
            if slot.is_some() {
                continue;
            }
            if let Poll::Ready(output) = future.as_mut().poll(cx) {
                *slot = Some(output);
                left -= 1;
            }
        }
        if left == 0 {
            Poll::Ready(
                done.iter_mut()
                    .map(|slot| slot.take().expect("every future has completed"))
                    .collect(),
            )
        } else {
            Poll::Pending
        }
    })
    .await
}

/// Runs one command per key and folds the replies into the one frame the
/// request is owed — see [`Fold`] for which fold, and why the request has to
/// say rather than the replies.
///
/// This is how a variadic `DEL`, `EXISTS` or `MGET` is served: the keys of one
/// request are in general owned by different shards, so there is no single
/// shard the request could be sent to, and it becomes one command per key.
///
/// **The set is not a transaction, and neither arm makes it one.** Each key's
/// command is atomic on the shard that owns it — that is the shard runtime's
/// guarantee and it is unaffected — but nothing spans the set, so a peer that
/// deletes three keys can be observed halfway through. Redis in cluster mode
/// makes the same trade, and it is the only one available here: the
/// alternative is a lock spanning shards, which would put the multi-key path's
/// cost in front of every single-key one.
///
/// What can land in the middle differs by arm, and it is the weaker of the two
/// that may be relied on. [`Fold::Sum`] dispatches one command at a time, so
/// another connection's work can land between any two of them. [`Fold::Array`]
/// hands an executor a whole slice at once and an executor applies an envelope
/// without yielding, so the keys of one slice that share an executor do in
/// fact move together — a consequence of how they are dispatched, not a
/// promise, and one that stops at the slice boundary in any case.
///
/// A shard that answers with an error ends the fan-out and that error is the
/// reply. A partial count reported as a total, or an array short by whatever
/// the failure cost, would be worse than a refusal: the peer cannot tell
/// either of them from the truth.
///
/// **The two folds do not dispatch alike, and the difference is deliberate.**
/// [`Fold::Array`] hands its commands to [`Router::dispatch_many`], which costs
/// about one envelope per executor rather than one cross-task hop per key, and
/// a multi-key read is what a client's cache layer compiles to. Not stopping
/// early costs it nothing: what was dispatched has already run by the time the
/// first reply is read, so an early return would spare no work that was still
/// avoidable. That, and not harmlessness, is the reason — a `GET` does leave
/// something behind when it finds a key expired. [`Fold::Sum`] stays
/// sequential because there an early return is real work not done: stopping at
/// the first error is what keeps a partial `DEL` from deleting further.
///
/// **A slice at a time, of [`CHUNK_COMMANDS`].** An executor applies an
/// envelope without yielding between its commands, so an envelope's length is
/// the delay one request can impose on every other connection whose keys live
/// on that executor's shards — the same property that bounds a drain's chunk,
/// bounded by the same number. It needs its own bound here because a request's
/// arity is not capped: `MGET` takes as many keys as the protocol's array
/// limit allows, as it does in Redis, so a pathological one is answered slowly
/// rather than refused while an ordinary one still travels in a single slice.
///
/// **Argument order is the array fold's contract**, and neither the dispatch
/// nor the slicing weakens it: `dispatch_many` reassembles its replies by
/// recorded position, so a slice comes back in the order its keys were named
/// whatever order the executors answered in, and the slices are appended in
/// the order they were cut. A key named twice is two commands and therefore
/// two entries — nothing here deduplicates.
async fn fan_out<R: Router>(router: &R, cmds: Vec<Command>, fold: Fold) -> Frame {
    match fold {
        Fold::Sum => {
            let mut total: i64 = 0;
            for cmd in cmds {
                total = match router.dispatch(cmd).await {
                    Reply::Removed(removed) => total.saturating_add(i64::from(removed)),
                    Reply::Integer(n) => total.saturating_add(n),
                    other => return reply_to_frame(other),
                };
            }
            Frame::Integer(total)
        }
        Fold::Array => {
            let mut entries = Vec::with_capacity(cmds.len());
            let mut pending = cmds.into_iter();
            loop {
                // The slice's length is how long one request may occupy an
                // executor, which is why it is `CHUNK_COMMANDS` rather than a
                // number of its own — see this function's doc.
                let slice: Vec<Command> = pending.by_ref().take(CHUNK_COMMANDS).collect();
                if slice.is_empty() {
                    break;
                }
                for reply in router.dispatch_many(slice).await {
                    match reply {
                        // A key that is not there is an entry all the same — the
                        // array's null, in its own slot, never a shorter array.
                        reply @ Reply::Bulk(_) => entries.push(reply_to_frame(reply)),
                        // First error in reply order wins, and it wins over
                        // everything behind it in its slice: that slice has
                        // already run, so this is a choice of which answer to
                        // give rather than a point the work stopped at. The
                        // slices behind it are the exception, and they are
                        // genuinely not dispatched.
                        error @ Reply::Error(_) => return reply_to_frame(error),
                        // Anything else is a reply of a shape this fold cannot
                        // put in an array — a command wired to [`Fold::Array`]
                        // whose shard answers with a count, say. Refused rather
                        // than rendered: a `:5` where an array of one was due
                        // is read happily and wrongly, and the peer has no way
                        // to tell.
                        _ => return Frame::Error(UNRENDERABLE_REPLY.into()),
                    }
                }
            }
            Frame::Array(entries)
        }
    }
}

/// Builds an error frame whose text cannot split the response.
///
/// Any `\r` or `\n` is replaced, and the result is truncated, so the frame
/// this produces always terminates exactly where the encoder puts its
/// terminator. Every error frame [`serve_connection`] emits comes from here.
fn safe_error(message: &str) -> Frame {
    let mut text: String = message
        .chars()
        .map(|c| if c == '\r' || c == '\n' { ' ' } else { c })
        .collect();
    // A message is a diagnostic, not a payload; an unbounded one is just
    // amplification.
    if text.len() > 512 {
        text.truncate(
            (0..=512)
                .rev()
                .find(|&i| text.is_char_boundary(i))
                .unwrap_or(0),
        );
    }
    Frame::Error(text)
}

/// Renders a codec error as the text of an error frame.
fn protocol_error(error: &ParseError) -> String {
    format!("ERR Protocol error: {error}")
}

/// Renders peer-supplied bytes for inclusion in an error message.
///
/// Printable ASCII survives; everything else becomes `\xNN`, so the result is
/// pure printable ASCII whatever the input was. [`safe_error`] is still the
/// thing that guarantees the frame is safe — this exists so the message stays
/// readable and short rather than turning a binary blob into a wall of
/// replacement characters.
fn quote(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut rendered = String::new();
    for &byte in bytes.iter().take(QUOTE_LIMIT) {
        match byte {
            b'\\' => rendered.push_str("\\\\"),
            b'\'' => rendered.push_str("\\'"),
            0x20..=0x7e => rendered.push(byte as char),
            // Written into the buffer rather than through a `format!` that
            // allocates a two-character `String` per unprintable byte.
            other => {
                let _ = write!(rendered, "\\x{other:02x}");
            }
        }
    }
    if bytes.len() > QUOTE_LIMIT {
        rendered.push_str("...");
    }
    rendered
}

/// Maps a request frame to what should happen because of it.
///
/// Every failure below is answered and survived: a frame that is well-formed
/// RESP but not a command this server can run is the peer's mistake, not a
/// reason to desynchronise the stream.
fn frame_to_action(frame: Frame, node: &NodeInfo) -> Action {
    match action_for(frame, node) {
        Ok(action) => action,
        Err(message) => Action::Reply(safe_error(&message)),
    }
}

/// What a command does with the arguments that follow its name.
///
/// One signature for every entry of [`COMMANDS`], whether or not a particular
/// command has any use for the node it is handed. The uniformity is the point:
/// it is what lets the surface be a table rather than a match, and the table is
/// what keeps `COMMAND COUNT` from drifting away from it.
type Handler = fn(&mut [Vec<u8>], &NodeInfo) -> Result<Action, String>;

/// Every command name this server accepts, and what each one does about its
/// arguments.
///
/// The single source of truth for the command surface. A name is dispatched by
/// being found here and `COMMAND COUNT` answers with how many entries there
/// are, so the number a client is told is exactly the number of commands the
/// server will run. A literal kept alongside the table could disagree with it;
/// a length cannot.
///
/// Ordered by the traffic a command carries rather than alphabetically. The
/// lookup is a scan and it is on the path every request takes, so the keyed
/// commands — which are effectively all of the traffic — come first, and the
/// ones a connection sends once or never come last.
///
/// `args` is emptied as it is matched: see [`action_for`].
const COMMANDS: &[(&[u8], Handler)] = &[
    (b"GET", |args, _| match args {
        [key] => Ok(Action::Dispatch(Command::Get { key: take(key) })),
        _ => Err(wrong_arity("get")),
    }),
    (b"SET", |args, node| match args {
        [key, value, options @ ..] => {
            // Parsed before the key and the value are taken, so a refused
            // option leaves nothing half-consumed.
            let options = set_options(options, node)?;
            Ok(Action::Dispatch(Command::Set {
                key: take(key),
                value: take(value),
                expiry: options.expiry,
                cond: options.cond,
                keep_ttl: options.keep_ttl,
                get: options.get,
            }))
        }
        _ => Err(wrong_arity("set")),
    }),
    (b"MGET", |args, _| {
        // One `Get` per argument, and no new command: what `MGET` adds to a
        // pile of `GET`s is the array around them, which is the fold's job
        // rather than a shard's.
        per_key(args, "mget", Fold::Array, |key| Command::Get { key })
    }),
    (b"DEL", |args, _| {
        per_key(args, "del", Fold::Sum, |key| Command::Del { key })
    }),
    (b"EXISTS", |args, _| {
        per_key(args, "exists", Fold::Sum, |key| Command::Exists { key })
    }),
    (b"EXPIRE", |args, _| match args {
        [key, seconds] => {
            let seconds = expire_seconds(seconds)?;
            Ok(Action::Dispatch(Command::Expire {
                key: take(key),
                seconds,
            }))
        }
        _ => Err(wrong_arity("expire")),
    }),
    (b"TTL", |args, _| match args {
        [key] => Ok(Action::Dispatch(Command::Ttl { key: take(key) })),
        _ => Err(wrong_arity("ttl")),
    }),
    (b"INCRBY", |args, _| match args {
        [key, delta] => {
            let delta =
                parse_i64(delta).ok_or_else(|| ReplyError::NotAnInteger.wire_text().to_owned())?;
            Ok(Action::Dispatch(Command::IncrBy {
                key: take(key),
                delta,
            }))
        }
        _ => Err(wrong_arity("incrby")),
    }),
    // Keyspace-wide: no key to route on, but every shard has to hear it.
    (b"DBSIZE", |args, _| match args {
        [] => Ok(Action::Unbatched(Unbatched::Every(Command::DbSize))),
        _ => Err(wrong_arity("dbsize")),
    }),
    (b"KEYS", |args, _| match args {
        [pattern] => Ok(Action::Unbatched(Unbatched::Keys(take(pattern)))),
        _ => Err(wrong_arity("keys")),
    }),
    (b"SCAN", |args, _| match args {
        [cursor, options @ ..] => {
            let cursor = parse_u64(cursor).ok_or_else(|| INVALID_CURSOR.to_owned())?;
            let (pattern, count) = scan_options(options)?;
            Ok(Action::Unbatched(Unbatched::Scan {
                cursor,
                pattern,
                count,
            }))
        }
        _ => Err(wrong_arity("scan")),
    }),
    (b"FLUSHDB", |args, _| match args {
        // Redis takes ASYNC and SYNC here. This server has one behaviour and
        // saying so plainly beats accepting a word it would then ignore.
        [] => Ok(Action::Unbatched(Unbatched::Every(Command::FlushDb))),
        _ => Err(wrong_arity("flushdb")),
    }),
    // From here down: the connection's own business, answered without a shard
    // ever hearing of it, because there is no key to route on.
    (b"PING", |args, _| match args {
        [] => Ok(Action::Reply(Frame::Simple("PONG".into()))),
        [message] => Ok(Action::Reply(Frame::Bulk(take(message)))),
        _ => Err(wrong_arity("ping")),
    }),
    (b"ECHO", |args, _| match args {
        [message] => Ok(Action::Reply(Frame::Bulk(take(message)))),
        _ => Err(wrong_arity("echo")),
    }),
    (b"HELLO", |args, node| hello(args, node)),
    (b"INFO", |args, node| {
        Ok(Action::Reply(Frame::Bulk(info(node, args).into_bytes())))
    }),
    (b"COMMAND", |args, _| match args {
        // Redis answers with a description of every command it has. This one
        // has nothing to describe, and an empty array is a client with no
        // hints rather than a client that failed to connect.
        [] => Ok(Action::Reply(Frame::Array(Vec::new()))),
        [sub, rest @ ..] => command(sub, rest),
    }),
    (b"CLIENT", |args, _| match args {
        [] => Err(wrong_arity("client")),
        [sub, rest @ ..] => client(sub, rest),
    }),
    (b"QUIT", |args, _| match args {
        [] => Ok(Action::ReplyThenClose(Frame::Simple("OK".into()))),
        _ => Err(wrong_arity("quit")),
    }),
];

/// [`frame_to_action`]'s body, with the error path expressed as `Err`.
///
/// Command names are matched case-insensitively, as Redis does.
///
/// The arguments are taken apart rather than read: a decoded frame already owns
/// its bulk payloads, so every one of them that ends up in a [`Command`] or a
/// reply is *moved* out of the array the codec built. What is left behind is an
/// empty `Vec` that dies with the array, and the alternative is a second copy
/// of every value the peer wrote, on the path every write takes.
fn action_for(frame: Frame, node: &NodeInfo) -> Result<Action, String> {
    let Frame::Array(parts) = frame else {
        return Err("ERR Protocol error: expected an array of bulk strings".into());
    };

    let mut args: Vec<Vec<u8>> = Vec::with_capacity(parts.len());
    for part in parts {
        match part {
            Frame::Bulk(bytes) => args.push(bytes),
            _ => return Err("ERR Protocol error: expected an array of bulk strings".into()),
        }
    }

    // Split rather than indexed, so the name and the arguments are two disjoint
    // borrows: the name is read to the end — an unknown command is quoted back
    // by it — while the arguments are being emptied.
    let Some((name, args)) = args.split_first_mut() else {
        return Err("ERR Protocol error: empty command".into());
    };

    // ASCII-uppercase only, which is what the command names are.
    let upper: Vec<u8> = name.to_ascii_uppercase();

    let Some((_, handler)) = COMMANDS
        .iter()
        .find(|(known, _)| *known == upper.as_slice())
    else {
        // The name is peer-supplied. It is quoted, not echoed.
        return Err(format!("ERR unknown command '{}'", quote(name)));
    };
    handler(args, node)
}

/// Answers `COMMAND` and the subcommands a client sends before it sends
/// anything a user asked for.
///
/// `COMMAND DOCS` is what redis-cli sends the moment it connects, and it reads
/// the reply before it prints a prompt — so an error there is not an
/// unfriendly message, it is a session that never starts. Answering with
/// nothing costs the user their command hints and nothing else.
fn command(sub: &[u8], rest: &[Vec<u8>]) -> Result<Action, String> {
    if sub.eq_ignore_ascii_case(b"COUNT") {
        if !rest.is_empty() {
            return Err(wrong_arity("command|count"));
        }
        // The table's length, so the count cannot disagree with what dispatch
        // accepts. See [`COMMANDS`].
        let count = i64::try_from(COMMANDS.len())
            .expect("the command surface is a handful of entries, written out in one table");
        return Ok(Action::Reply(Frame::Integer(count)));
    }
    if sub.eq_ignore_ascii_case(b"DOCS") {
        // Redis takes command names here and describes those. With nothing to
        // say about any of them, the answer is the same either way.
        return Ok(Action::Reply(Frame::Array(Vec::new())));
    }
    Err(unknown_subcommand("COMMAND", sub))
}

/// Answers the `CLIENT` subcommands a client sends about itself on connect.
///
/// Both are taken and dropped. They name a connection this server has nowhere
/// to show the name of — there is no `CLIENT LIST` here to show it in — but
/// go-redis and redis-py both send `SETINFO` as part of establishing a
/// connection and treat a refusal as a failed one, so the stub answers `OK`
/// rather than being honest about doing nothing with it.
fn client(sub: &[u8], rest: &[Vec<u8>]) -> Result<Action, String> {
    let (name, arity) = if sub.eq_ignore_ascii_case(b"SETNAME") {
        ("client|setname", 1)
    } else if sub.eq_ignore_ascii_case(b"SETINFO") {
        ("client|setinfo", 2)
    } else {
        return Err(unknown_subcommand("CLIENT", sub));
    };
    if rest.len() == arity {
        Ok(Action::Reply(Frame::Simple("OK".into())))
    } else {
        Err(wrong_arity(name))
    }
}

/// The unknown-subcommand message, byte-exact to Redis down to the full stop
/// and the pointer at the help text clients print back to their users.
///
/// `container` is the containing command's own name, a literal from
/// [`COMMANDS`]; the subcommand is peer-supplied, so it is quoted rather than
/// echoed.
fn unknown_subcommand(container: &str, sub: &[u8]) -> String {
    format!(
        "ERR unknown subcommand '{}'. Try {container} HELP.",
        quote(sub)
    )
}

/// The `INFO` arguments that name no section but the whole document.
///
/// Redis takes all three where a section name goes and answers each with a
/// whole document rather than one section, and `all` is the spelling operators
/// and metrics exporters reach for — treating it as an ordinary section name
/// would answer the most common form of the command with nothing at all.
///
/// The three are not interchangeable there: Redis's `default` omits
/// `# Commandstats` and `# Latencystats`, which `all` and `everything` both
/// carry. They collapse to one behaviour *here* only because the two sections
/// this server prints are in Redis's default set, leaving the distinction
/// nothing to select. A section added outside that set would have to honour it.
///
/// Measured against Redis 8.10.0: each of the three answers a document of the
/// same order of size as the unargumented `INFO`, and one of them anywhere in
/// the argument list wins over the section names beside it — `INFO all nosuch`
/// is the whole document while `INFO nosuch server` is the server section
/// alone.
const INFO_WHOLE_DOCUMENT: [&[u8]; 3] = [b"all", b"default", b"everything"];

/// Renders the `INFO` sections a peer asked for, or all of them if it named
/// none.
///
/// A section name nobody recognises contributes nothing rather than being an
/// error — `INFO nosuch` is an empty bulk, which is how Redis answers one and
/// leaves the client to notice that the field it wanted is absent. The three
/// names in [`INFO_WHOLE_DOCUMENT`] are the exception: they ask for no section
/// in particular and get everything.
///
/// **The field names are Redis's own, `redis_` prefix and all.** `INFO`'s
/// contract is its field names: everything that reads this output looks up
/// `redis_version` or `redis_mode` by that exact spelling, so renaming them
/// after this server would produce a document that is honest and unreadable.
/// What the server calls itself is `HELLO`'s answer, which has a field for it.
///
/// Only what this node can state truthfully is printed. A field invented to
/// fill out the section is a number some dashboard will plot.
fn info(node: &NodeInfo, wanted: &[Vec<u8>]) -> String {
    use std::fmt::Write as _;

    // One of the whole-document names anywhere in the list drops the filter
    // entirely, which is how Redis resolves them against the section names
    // beside them.
    let unfiltered = wanted.is_empty()
        || wanted.iter().any(|name| {
            INFO_WHOLE_DOCUMENT
                .iter()
                .any(|whole| name.eq_ignore_ascii_case(whole))
        });
    let asked_for =
        |section: &[u8]| unfiltered || wanted.iter().any(|name| name.eq_ignore_ascii_case(section));
    // Writing into a `String` cannot fail, so discarding the results is the
    // whole of what there is to do with them.
    let mut text = String::new();
    if asked_for(b"server") {
        let _ = write!(
            text,
            "# Server\r\n\
             redis_version:{}\r\n\
             redis_mode:{SERVER_MODE}\r\n\
             tcp_port:{}\r\n\
             uptime_in_seconds:{}\r\n\r\n",
            node.version,
            node.tcp_port,
            // Whole seconds on the monotonic clock, and saturating at zero:
            // subtracting instants this way cannot go negative however the
            // clock behaved.
            node.started.elapsed().as_secs(),
        );
    }
    if asked_for(b"clients") {
        let _ = write!(
            text,
            "# Clients\r\nconnected_clients:{}\r\n\r\n",
            node.connected.load(Ordering::Relaxed),
        );
    }
    text
}

/// Answers `HELLO`, which is how a client asks what it is talking to.
///
/// This server speaks RESP2 and only RESP2, so the version argument has
/// exactly one accepted value. The refusal for every other one is
/// [`NOPROTO`] — see that constant for why the exact text is a contract.
fn hello(args: &[Vec<u8>], node: &NodeInfo) -> Result<Action, String> {
    let version = match args {
        [] => 2,
        [version, rest @ ..] => {
            if let Some(option) = rest.first() {
                // Redis takes AUTH and SETNAME here. This server has neither
                // to offer, and saying so is better than ignoring the option
                // and letting a client believe it authenticated.
                return Err(format!(
                    "ERR Syntax error in HELLO option '{}'",
                    quote(option)
                ));
            }
            parse_i64(version).ok_or_else(|| {
                "ERR Protocol version is not an integer or out of range".to_owned()
            })?
        }
    };
    if version == 2 {
        Ok(Action::Reply(hello_frame(node)))
    } else {
        Err(NOPROTO.to_owned())
    }
}

/// The `HELLO` reply: a flat array of key-value pairs, which is how RESP2
/// carries a map.
///
/// The version and the mode are read from the node rather than written out
/// here, so that a client asking `HELLO` and a client reading `INFO` are told
/// the same two things by construction.
fn hello_frame(node: &NodeInfo) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(b"server".to_vec()),
        Frame::Bulk(SERVER_NAME.as_bytes().to_vec()),
        Frame::Bulk(b"version".to_vec()),
        Frame::Bulk(node.version.as_bytes().to_vec()),
        Frame::Bulk(b"proto".to_vec()),
        Frame::Integer(2),
        Frame::Bulk(b"mode".to_vec()),
        Frame::Bulk(SERVER_MODE.as_bytes().to_vec()),
        Frame::Bulk(b"role".to_vec()),
        Frame::Bulk(b"master".to_vec()),
    ])
}

/// Builds the action for a command that names one key or many.
///
/// Several keys become a [`fan_out`], with the atomicity that costs stated
/// there. One key stays a single dispatch — the common case, and the one that
/// travels in the drain's batch with everything else — but only where the fold
/// leaves a lone reply as it is: see [`Fold::is_identity_on_one`], which is
/// what sends a one-key `MGET` through the fan-out to be wrapped in the array
/// of one it is owed.
fn per_key(
    args: &mut [Vec<u8>],
    name: &str,
    fold: Fold,
    command: fn(Vec<u8>) -> Command,
) -> Result<Action, String> {
    match args {
        [] => Err(wrong_arity(name)),
        [key] if fold.is_identity_on_one() => Ok(Action::Dispatch(command(take(key)))),
        keys => Ok(Action::Unbatched(Unbatched::FanOut {
            cmds: keys.iter_mut().map(|key| command(take(key))).collect(),
            fold,
        })),
    }
}

/// Everything a `SET`'s options settle, in the shard's vocabulary.
///
/// A struct rather than a tuple because the last two are booleans: a caller
/// destructuring `(expiry, cond, bool, bool)` can swap the pair without the
/// compiler noticing, and `KEEPTTL` silently becoming `GET` is a defect no
/// type would catch.
struct SetOptions {
    /// How long the key should live, or `None` for no deadline.
    expiry: Option<Expiry>,
    /// The condition the write is subject to, if any.
    cond: Option<Cond>,
    /// Whether `KEEPTTL` was named.
    keep_ttl: bool,
    /// Whether `GET` was named.
    get: bool,
}

/// Parses the options a `SET` may carry after its key and value.
///
/// Walked left to right, case-insensitively, over `EX`, `PX`, `EXAT`, `PXAT`,
/// `NX`, `XX`, `KEEPTTL` and `GET`. Everything else — an option this server
/// does not know, two options that cannot both hold, an `EX` with nothing
/// after it — is [`SYNTAX_ERROR`], which is the single answer Redis gives to
/// all of them.
///
/// The one rule worth stating outright is what "cannot both hold" means, since
/// it is not "named twice". Redis takes the last occurrence of a repeated
/// option rather than refusing the command, and a client that builds a command
/// by appending options relies on it — so `EX 100 EX 50` is a 50-second
/// deadline. What it refuses is a *different* option from the same family:
/// `EX 10 PX 5` and `NX XX` are syntax errors, in either order, because there
/// is no answer to give a peer that asked for both. See [`ExpiryOption`].
///
/// That rule reaches further than which occurrence wins, and the difference is
/// visible from the wire. **A discarded occurrence is discarded whole, its
/// argument included, and that argument is never looked at** — so
/// `EX notanum EX 10` is a command Redis runs, while `EX 10 EX notanum` is the
/// one it refuses. This walk therefore performs syntax and family checks only,
/// carrying the surviving option and its *raw* argument, and the one argument
/// that survived is validated afterwards, once. A syntax error anywhere
/// consequently beats an invalid expire time anywhere, in either order:
/// `EX 0 BOGUS` is `ERR syntax error`, not an invalid expire time.
///
/// Both paragraphs are measurements of a live `redis-server v=8.10.0` rather
/// than reasoning about what a parser ought to do — they are the kind of
/// behaviour a server grows by accident and clients then depend on.
fn set_options(mut rest: &[Vec<u8>], node: &NodeInfo) -> Result<SetOptions, String> {
    // The surviving expiry option and the bytes that followed it, unread.
    let mut expiry_arg: Option<(ExpiryUnit, &[u8])> = None;
    let mut named: Option<ExpiryOption> = None;
    let mut cond: Option<Cond> = None;
    let mut keep_ttl = false;
    let mut get = false;
    while let Some((option, tail)) = rest.split_first() {
        if let Some(unit) = expiry_unit(option) {
            claim(&mut named, unit.option)?;
            let Some((value, after)) = tail.split_first() else {
                return Err(SYNTAX_ERROR.to_owned());
            };
            expiry_arg = Some((unit, value));
            rest = after;
        } else if option.eq_ignore_ascii_case(b"KEEPTTL") {
            // In the expiry family, so it conflicts with all of it: a peer
            // that asked to keep a deadline and to set one has asked for two
            // different things about the same field.
            claim(&mut named, ExpiryOption::KeepTtl)?;
            keep_ttl = true;
            rest = tail;
        } else if let Some(wanted) = condition(option) {
            if cond.is_some_and(|held| held != wanted) {
                return Err(SYNTAX_ERROR.to_owned());
            }
            cond = Some(wanted);
            rest = tail;
        } else if option.eq_ignore_ascii_case(b"GET") {
            // Conflicts with nothing: it asks about the value the write
            // replaced, which every other option leaves it free to answer.
            get = true;
            rest = tail;
        } else {
            return Err(SYNTAX_ERROR.to_owned());
        }
    }
    // The one surviving argument, validated now that nothing can discard it.
    let expiry = match expiry_arg {
        None => None,
        Some((unit, value)) => {
            let value = set_expire_value(value, unit.ceiling)?;
            Some(match unit.form {
                ExpiryForm::Span(build) => build(value),
                // The multiplication cannot overflow: `set_expire_value` has
                // just held the value to a ceiling of `i64::MAX` milliseconds
                // expressed in this unit. Saturating anyway, so that a ceiling
                // relaxed later is a deadline further off than anyone waits
                // rather than a wrapped one.
                ExpiryForm::Deadline(unit_millis) => {
                    remaining_from(value.saturating_mul(unit_millis), node)
                }
            })
        }
    };
    Ok(SetOptions {
        expiry,
        cond,
        keep_ttl,
        get,
    })
}

/// Records that `option` was named, refusing a *different* option already held.
///
/// The last occurrence of the same option wins, which is the assignment; two
/// different ones are [`SYNTAX_ERROR`]. See [`set_options`].
fn claim(held: &mut Option<ExpiryOption>, option: ExpiryOption) -> Result<(), String> {
    if held.is_some_and(|already| already != option) {
        return Err(SYNTAX_ERROR.to_owned());
    }
    *held = Some(option);
    Ok(())
}

/// `SCAN`'s options: `MATCH <glob>` and `COUNT <n>`, in either order.
///
/// A repeated option takes the last occurrence, as Redis does — and there the
/// resemblance to [`set_options`] stops. `SCAN` validates every occurrence as
/// it reads it, so an earlier one keeps a veto: `COUNT 0 COUNT 10` is a syntax
/// error here where `EX 0 EX 10` is a 10-second deadline, and
/// `COUNT notanum COUNT 10` is refused where `EX notanum EX 10` is run. The
/// asymmetry is Redis's own, measured on a live `redis-server v=8.10.0`
/// against both commands rather than inferred from either; this server copies
/// it because a client that learned one command's behaviour learned it from
/// the server it is replacing.
///
/// `COUNT` must be positive: Redis answers a syntax error for zero and for a
/// negative, which is a different failure from a `COUNT` that is not a number
/// at all, and clients distinguish them. What it asks for is then bounded by
/// [`WALK_STEP_BUCKETS`], which states why.
fn scan_options(mut rest: &[Vec<u8>]) -> Result<(Option<Vec<u8>>, usize), String> {
    let mut pattern = None;
    let mut count = SCAN_DEFAULT_COUNT;
    while let Some((option, tail)) = rest.split_first() {
        let (value, after) = tail.split_first().ok_or_else(|| SYNTAX_ERROR.to_owned())?;
        if option.eq_ignore_ascii_case(b"MATCH") {
            pattern = Some(value.clone());
        } else if option.eq_ignore_ascii_case(b"COUNT") {
            let n =
                parse_i64(value).ok_or_else(|| ReplyError::NotAnInteger.wire_text().to_owned())?;
            if n <= 0 {
                return Err(SYNTAX_ERROR.to_owned());
            }
            count = usize::try_from(n)
                .unwrap_or(WALK_STEP_BUCKETS)
                .min(WALK_STEP_BUCKETS);
        } else {
            return Err(SYNTAX_ERROR.to_owned());
        }
        rest = after;
    }
    Ok((pattern, count))
}

/// Parses the canonical decimal spelling of a `u64`.
///
/// A cursor is not a number a person typed: it is one this server issued and
/// the client handed straight back, and this server issues what
/// `u64::to_string` prints. So that spelling is the only one accepted — no
/// sign, no whitespace, no leading zeros — and anything else is a cursor this
/// server did not issue, which is what [`INVALID_CURSOR`] says.
fn parse_u64(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    if bytes[0] == b'0' && bytes.len() > 1 {
        return None;
    }
    // Verified ASCII above, so this is valid UTF-8; checked rather than
    // asserted, so a bug in the validation is a `None` and never a panic.
    std::str::from_utf8(bytes).ok()?.parse::<u64>().ok()
}

/// Which member of the expiry family an option is.
///
/// The family is `EX`, `PX`, `EXAT`, `PXAT` and `KEEPTTL` — every option that
/// has something to say about how long the key lives. They are told apart
/// because repeating one is legal and naming two is not, and the parsed
/// [`Expiry`] alone cannot say which happened: `EX 10 EX 10` and `EX 10 PX 5`
/// both leave one span behind, and only one of them is a command Redis runs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ExpiryOption {
    /// `EX`, a span in seconds.
    Ex,
    /// `PX`, a span in milliseconds.
    Px,
    /// `EXAT`, a Unix deadline in seconds.
    ExAt,
    /// `PXAT`, a Unix deadline in milliseconds.
    PxAt,
    /// `KEEPTTL`, which sets no deadline and clears none.
    KeepTtl,
}

/// What an expiry option decides about the value that follows it.
///
/// An option settles three things here and nothing else: which option it is,
/// how large the value may be, and what the value means.
struct ExpiryUnit {
    /// Which option this is, so a repeat can be told from a conflict.
    option: ExpiryOption,
    /// The largest value this unit may carry.
    ceiling: i64,
    /// What the value is measured from.
    form: ExpiryForm,
}

/// Whether an option's value is a span or a deadline.
enum ExpiryForm {
    /// A span, which stays in the unit the client chose all the way to the
    /// shard — see [`Expiry`] — and so only has to be wrapped.
    Span(fn(u64) -> Expiry),
    /// A deadline on the wall clock, in this many milliseconds per unit. It is
    /// turned into a span here; see [`remaining_from`].
    Deadline(u64),
}

/// The unit an expiry option names, if it names one.
fn expiry_unit(option: &[u8]) -> Option<ExpiryUnit> {
    if option.eq_ignore_ascii_case(b"EX") {
        Some(ExpiryUnit {
            option: ExpiryOption::Ex,
            ceiling: MAX_EXPIRE_SECONDS,
            form: ExpiryForm::Span(Expiry::Ex),
        })
    } else if option.eq_ignore_ascii_case(b"PX") {
        // Already in the unit that ceiling exists to protect, so anything
        // positive an `i64` can hold is a span Redis accepts.
        Some(ExpiryUnit {
            option: ExpiryOption::Px,
            ceiling: i64::MAX,
            form: ExpiryForm::Span(Expiry::Px),
        })
    } else if option.eq_ignore_ascii_case(b"EXAT") {
        Some(ExpiryUnit {
            option: ExpiryOption::ExAt,
            ceiling: MAX_EXPIRE_SECONDS,
            form: ExpiryForm::Deadline(1_000),
        })
    } else if option.eq_ignore_ascii_case(b"PXAT") {
        Some(ExpiryUnit {
            option: ExpiryOption::PxAt,
            ceiling: i64::MAX,
            form: ExpiryForm::Deadline(1),
        })
    } else {
        None
    }
}

/// Turns the absolute deadline `EXAT`/`PXAT` names into the span the shard
/// understands, against one reading of the node's wall clock.
///
/// This is the whole of what those two options mean here, and the conversion
/// happens at this edge rather than in the shard because the shard has no wall
/// clock and is not given one. Its `now` is a [`tokio::time::Instant`] —
/// monotonic, and *virtual* under the deterministic simulator, which advances
/// it by fiat — so an absolute Unix deadline is simply not a quantity it can
/// compare against anything it holds. Reconciling the two clocks once, here,
/// keeps the shard a pure function of what it is handed and costs a
/// subtraction on the one command family that names an absolute time. Which
/// wall clock is [`NodeInfo::now_unix_millis`]'s to say, for the reason stated
/// there.
///
/// A deadline already in the past becomes `Expiry::Px(0)`, which the shard
/// resolves to exactly `now`: the key is stored and is already due, so the
/// very next command that looks at it finds it gone. That is Redis's own
/// answer to `SET k v EXAT 1` — `+OK`, and no key — and it is a shape no other
/// path produces, since a span of zero on the wire is refused before it gets
/// this far; see [`set_expire_value`].
///
/// The span is measured here, at parse time, and added to the shard's `now`
/// when the command reaches it, so the deadline lands later than the client
/// named it by however long the dispatch took. `EX` and `PX` already behave
/// exactly this way — every span this server takes is measured against the
/// clock of the layer that reads it — so an absolute deadline is no less
/// faithful than a relative one; it is the same microseconds of drift, on a
/// command that happens to say when rather than how long.
fn remaining_from(deadline_millis: u64, node: &NodeInfo) -> Expiry {
    let now = (node.now_unix_millis)();
    // The subtraction is the live one: it saturates for every deadline already
    // past, which is the case this function exists to answer. The conversion
    // below cannot narrow — a value that got here was held to a ceiling of
    // `i64::MAX` milliseconds — and is written defensively rather than as
    // `as`, so that a ceiling relaxed later saturates instead of wrapping.
    let remaining = u128::from(deadline_millis).saturating_sub(u128::from(now));
    Expiry::Px(u64::try_from(remaining).unwrap_or(u64::MAX))
}

/// The condition a `SET` option names, if it names one.
const fn condition(option: &[u8]) -> Option<Cond> {
    if option.eq_ignore_ascii_case(b"NX") {
        Some(Cond::Nx)
    } else if option.eq_ignore_ascii_case(b"XX") {
        Some(Cond::Xx)
    } else {
        None
    }
}

/// Validates the value an expiry option names: strictly positive, and within
/// what the unit can carry.
///
/// Called once per `SET`, on the occurrence that survived the walk rather than
/// on each one — [`set_options`] says why that is observable.
///
/// Positive is a rule about the value, not about the deadline it resolves to,
/// and the two come apart for the absolute options: `EXAT 1` names a moment
/// decades gone and is a write Redis performs, while `EX 0` and `EXAT 0` are
/// both `ERR invalid expire time in 'set' command`. So a `SET` whose deadline
/// has already passed is not a write refused, but a `SET` whose *number* is
/// zero or negative is. `ceiling` is what the option's unit may carry; see
/// [`expiry_unit`].
fn set_expire_value(value: &[u8], ceiling: i64) -> Result<u64, String> {
    let value = parse_i64(value).ok_or_else(|| ReplyError::NotAnInteger.wire_text().to_owned())?;
    if value <= 0 || value > ceiling {
        return Err(invalid_expire("set"));
    }
    u64::try_from(value).map_err(|_| invalid_expire("set"))
}

/// Parses `EXPIRE`'s span, which unlike `SET`'s may be zero or negative.
///
/// A deadline in the past is a deletion the client asked for in the past tense,
/// and Redis performs it. What it refuses is a span it cannot do arithmetic on
/// — see [`MAX_EXPIRE_SECONDS`], in both directions.
fn expire_seconds(seconds: &[u8]) -> Result<i64, String> {
    let seconds =
        parse_i64(seconds).ok_or_else(|| ReplyError::NotAnInteger.wire_text().to_owned())?;
    if !(-MAX_EXPIRE_SECONDS..=MAX_EXPIRE_SECONDS).contains(&seconds) {
        return Err(invalid_expire("expire"));
    }
    Ok(seconds)
}

/// The invalid-expiry message, with the command's own lowercase name — a
/// literal from the table above, never peer-supplied text.
fn invalid_expire(name: &str) -> String {
    format!("ERR invalid expire time in '{name}' command")
}

/// The arity message, with the command's own lowercase name — a literal from
/// the table above, never peer-supplied text.
fn wrong_arity(name: &str) -> String {
    format!("ERR wrong number of arguments for '{name}' command")
}

#[cfg(test)]
mod tests {
    use super::*;
    use seedstone_core::dict::DictSeed;
    use seedstone_core::shard::{NoTrace, ShardPool};
    use seedstone_resp::{MAX_ARRAY_LEN, MAX_BULK_LEN, parse};
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
        for i in 0..2000u32 {
            encode(&req(&["SET", &format!("k-{i}"), "v"]), &mut out);
        }
        encode(&req(&["KEYS", "k-*"]), &mut out);
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();
        let frames = read_frames(&mut r, 2001).await;
        assert!(matches!(&frames[2000], Frame::Array(a) if a.len() == 2000));

        let steps = router.steps.lock().expect("steps mutex").len();
        // 2000 keys over a table walked WALK_STEP_BUCKETS at a time cannot be
        // one envelope. The assertion is deliberately `> 1` and not an exact
        // count: how many buckets 2000 keys occupy is the dict's business and
        // may change, while "more than one envelope" is the property.
        assert!(
            steps > 1,
            "a 2000-key walk took {steps} envelope(s); one means it held the shard for the cycle"
        );
    }

    /// The cursor packing's three claims, at the edges where a bit-shift is
    /// wrong if it is wrong anywhere.
    #[test]
    fn a_packed_cursor_round_trips_at_every_edge() {
        let internals = [
            0u64,
            1,
            (1 << CURSOR_INTERNAL_BITS) - 1,
            1 << (CURSOR_INTERNAL_BITS - 1),
        ];
        for shard in [0u16, 1, 255, u16::MAX] {
            for internal in internals {
                let packed = pack_cursor(shard, internal);
                assert_eq!(
                    unpack_cursor(packed),
                    (shard, internal),
                    "shard {shard} internal {internal:#x}"
                );
            }
        }
    }

    #[test]
    fn only_the_very_start_of_the_walk_is_zero() {
        // The whole scheme rests on this: 0 means "begin", and the server
        // answers 0 only when the last shard is spent. Shard 1 at internal 0
        // must therefore not be 0.
        assert_eq!(pack_cursor(0, 0), 0);
        assert_ne!(pack_cursor(1, 0), 0);
        assert_ne!(pack_cursor(u16::MAX, 0), 0);
    }

    #[test]
    fn a_client_supplied_cursor_is_untrusted_input() {
        // Anything a client sends unpacks to something; nothing panics. The
        // shard may be out of range, which the dispatch path answers rather
        // than the packing.
        for raw in [u64::MAX, 1 << 63, 0x0001_0000_0000_0000] {
            let (_shard, internal) = unpack_cursor(raw);
            assert!(internal < (1 << CURSOR_INTERNAL_BITS));
        }
    }

    /// A walk driven the way a client drives it: from `0`, following the
    /// cursor the server hands back, until it is `0` again.
    #[tokio::test]
    async fn a_full_scan_returns_every_key_and_ends_at_zero() {
        const SHARDS: u16 = 4;
        let (mut r, mut w, _pool) = connected(SHARDS);
        let mut out = Vec::new();
        for i in 0..300u32 {
            encode(&req(&["SET", &format!("s-{i}"), "v"]), &mut out);
        }
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();
        let _ = read_frames(&mut r, 300).await;

        let mut seen: Vec<Vec<u8>> = Vec::new();
        let mut cursor = String::from("0");
        let mut calls = 0;
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
            calls += 1;
            assert!(calls < 500, "the walk did not terminate");
            if cursor == "0" {
                break;
            }
        }
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), 300);
        // Every shard costs at least one call, so a walk that took exactly
        // one per shard would prove only that the fan-out ran. Anything above
        // that is a shard resumed mid-table, which is the part that needs the
        // cursor to mean something.
        assert!(
            calls > usize::from(SHARDS),
            "{calls} calls over {SHARDS} shards: no shard was ever resumed mid-table"
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

    /// The two rules `scan_options` carries that the wire tests cannot see:
    /// a repeated option is its last occurrence, and a `COUNT` from the wire
    /// is a request for work that this server bounds.
    ///
    /// The bound is the point. `COUNT` is a client's hint everywhere else and
    /// a shard's occupancy here: a step honoured literally at `u64::MAX` walks
    /// a whole cycle inside one envelope and holds the shard for it, which is
    /// the one thing the step primitive exists to prevent. A clamped walk
    /// still returns every key — it just takes the round trips the server's
    /// step size implies rather than the ones the client asked for.
    #[test]
    fn scan_options_take_the_last_occurrence_and_bound_the_step() {
        let opts = |parts: &[&str]| -> (Option<Vec<u8>>, usize) {
            let owned: Vec<Vec<u8>> = parts.iter().map(|p| p.as_bytes().to_vec()).collect();
            scan_options(&owned).expect("these options parse")
        };

        assert_eq!(opts(&[]), (None, SCAN_DEFAULT_COUNT));
        assert_eq!(opts(&["COUNT", "7"]).1, 7);
        assert_eq!(
            opts(&["MATCH", "a*", "MATCH", "b*"]).0,
            Some(b"b*".to_vec()),
            "a repeated option is its last occurrence, as SET's are"
        );

        assert_eq!(
            opts(&["COUNT", &i64::MAX.to_string()]).1,
            WALK_STEP_BUCKETS,
            "a COUNT past the step ceiling must be clamped to it"
        );
        assert_eq!(
            opts(&["COUNT", &WALK_STEP_BUCKETS.to_string()]).1,
            WALK_STEP_BUCKETS
        );
        // Past what an i64 spells is not a large COUNT, it is not a number —
        // the same answer Redis gives, and a different one from a COUNT of
        // zero.
        let owned = vec![b"COUNT".to_vec(), u64::MAX.to_string().into_bytes()];
        assert_eq!(
            scan_options(&owned),
            Err(ReplyError::NotAnInteger.wire_text().to_owned())
        );
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
        let Reply::Scan { cursor, keys } = at_zero else {
            panic!("expected Reply::Scan");
        };
        assert_eq!(cursor, 0, "an unbounded count must finish the cycle");
        assert!(
            keys.len() < 64,
            "shard 0 of four held every key, which makes this test prove nothing"
        );
    }

    /// [`join_all`]'s three claims, none of which its callers would fail
    /// loudly on: it terminates on nothing, it gathers by input order rather
    /// than completion order, and a future that parks is woken again.
    #[tokio::test]
    async fn join_all_terminates_on_an_empty_input() {
        let joined: Vec<()> = join_all(Vec::<std::future::Ready<()>>::new()).await;
        assert!(joined.is_empty());
    }

    #[tokio::test]
    async fn join_all_gathers_by_input_order_not_completion_order() {
        // Each future parks on a channel, and the channels are fired in
        // reverse. Under completion order the result would come back
        // reversed; under input order it does not.
        let mut senders = Vec::new();
        let mut futures = Vec::new();
        for i in 0..8u32 {
            let (tx, rx) = tokio::sync::oneshot::channel();
            senders.push(tx);
            futures.push(async move {
                rx.await.expect("sender held until fired");
                i
            });
        }
        let joined = tokio::spawn(join_all(futures));
        // Yield first, so every future has had a chance to park before any
        // channel fires: a future that completed on its first poll would not
        // exercise the waker path at all.
        tokio::task::yield_now().await;
        for tx in senders.into_iter().rev() {
            tx.send(()).expect("the join is still awaiting");
            tokio::task::yield_now().await;
        }
        assert_eq!(joined.await.unwrap(), (0..8u32).collect::<Vec<_>>());
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
            // Last: it closes the connection.
            &["QUIT"],
        ] {
            encode(&req(parts), &mut out);
        }
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        let frames = read_frames(&mut r, 12).await;
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
        assert_eq!(
            frames[10],
            Frame::Error("ERR Syntax error in HELLO option 'AUTH'".into())
        );
        assert_eq!(
            frames[11],
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

    /// `HELLO 3` must be refused with the `NOPROTO` prefix specifically.
    ///
    /// go-redis v9 opens every connection with `HELLO 3` and downgrades to
    /// RESP2 on exactly this reply. Any other error text — including a
    /// perfectly reasonable `ERR unsupported protocol` — makes the client give
    /// up instead of falling back, so the string is a compatibility contract
    /// and not a message.
    #[test]
    fn the_noproto_text_is_byte_exact_to_redis() {
        assert_eq!(NOPROTO, "NOPROTO unsupported protocol version");
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

    /// The other half of that truthfulness: no name appears in the table twice.
    ///
    /// Dispatch takes the *first* match while `COMMAND COUNT` reports *every*
    /// entry, so a duplicated name inflates the count above the number of
    /// commands a client can actually reach — and the test above cannot see
    /// it, because both copies of a duplicate dispatch perfectly well. Only a
    /// count of distinct names catches it.
    #[test]
    fn no_name_appears_in_the_command_table_twice() {
        let mut names: Vec<&[u8]> = COMMANDS.iter().map(|(name, _)| *name).collect();
        let total = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            names.len(),
            total,
            "COMMAND COUNT answers {total}, but the table holds only {} distinct names",
            names.len()
        );
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

    /// The same attack from the other side, made impossible rather than
    /// caught.
    ///
    /// This used to be a `SplittingRouter` returning
    /// `Reply::Error("ERR boom\r\n+INJECTED")`, proving that `safe_error`
    /// neutralised it on the way out. That router can no longer be written:
    /// [`ReplyError`] is a closed set of variants whose texts are literals in
    /// `shard.rs`, so the defence moved from a runtime scrub to the type.
    ///
    /// What replaces it is stronger than what it replaces. The old test proved
    /// one composed string was neutralised; this one holds every failure a
    /// shard can report to the property the frame format actually needs, and a
    /// new variant cannot be added without the match below forcing it into the
    /// list.
    #[test]
    fn every_shard_error_is_frame_safe() {
        let every = [
            ReplyError::NotAnInteger,
            ReplyError::WouldOverflow,
            ReplyError::ShardUnavailable,
            ReplyError::LogWriteFailed,
        ];
        for error in every {
            // Exhaustiveness: adding a variant makes this match non-exhaustive
            // and the crate stops compiling until it is named — and whoever
            // names it here sees the array above.
            match error {
                ReplyError::NotAnInteger
                | ReplyError::WouldOverflow
                | ReplyError::ShardUnavailable
                | ReplyError::LogWriteFailed => {}
            }

            let text = error.wire_text();
            assert!(
                !text.contains(['\r', '\n']),
                "{error:?} carries a frame terminator: {text:?}"
            );
            assert!(!text.is_empty(), "{error:?} has no text");
            // Redis error replies open with an uppercase code; clients match
            // on it, and a lowercase or missing one is a protocol smell.
            assert!(
                text.split(' ').next().is_some_and(|code| {
                    !code.is_empty() && code.chars().all(|c| c.is_ascii_uppercase())
                }),
                "{error:?} does not open with an error code: {text:?}"
            );
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

    #[test]
    fn safe_error_strips_every_terminator() {
        let Frame::Error(text) = safe_error("a\rb\nc\r\nd") else {
            panic!("expected an error frame");
        };
        assert_eq!(text, "a b c  d");
    }

    #[test]
    fn safe_error_truncates_on_a_character_boundary() {
        // A multi-byte character straddling the limit must not be cut in
        // half — `String::truncate` panics if it is.
        //
        // The width matters, and a two-byte character does not test this at
        // all: 512 is even, so it is always a boundary in a run of them, and
        // a bare `text.truncate(512)` with the boundary search deleted passes.
        // "€" is three bytes and 512 is not a multiple of three, so the cut
        // lands mid-character and only the search saves it. Both widths are
        // asserted so the cheap case cannot be the only cover.
        for filler in ["é", "€"] {
            let long = filler.repeat(400);
            let Frame::Error(text) = safe_error(&long) else {
                panic!("expected an error frame");
            };
            assert!(text.len() <= 512, "{filler}: {} bytes", text.len());
            assert!(long.starts_with(&text), "{filler}: not a prefix");
        }
    }

    #[test]
    fn quote_renders_arbitrary_bytes_as_printable_ascii() {
        assert_eq!(quote(b"PING"), "PING");
        assert_eq!(quote(b"a\r\nb"), "a\\x0d\\x0ab");
        assert_eq!(quote(b"\xff\x00"), "\\xff\\x00");
        assert_eq!(quote(b"it's"), "it\\'s");
        assert_eq!(quote(br"back\slash"), "back\\\\slash");
        assert!(quote(&[b'x'; 100]).ends_with("..."));
        assert!(quote(&[b'x'; 100]).len() <= QUOTE_LIMIT + 3);
        // The property the error path depends on.
        for byte in 0..=255u8 {
            let rendered = quote(&[byte]);
            assert!(
                rendered.is_ascii() && !rendered.contains(['\r', '\n']),
                "byte {byte:#04x} rendered as {rendered:?}"
            );
        }
    }

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
        assert!(flush_replies(&mut sink, &mut out).await);
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
        assert!(flush_replies(&mut sink, &mut out).await);
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

        assert!(flush_replies(&mut sink, &mut out).await);
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
        assert!(flush_replies(&mut sink, &mut out).await);
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
