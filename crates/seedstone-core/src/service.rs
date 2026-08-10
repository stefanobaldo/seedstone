//! The connection layer: RESP2 frames in, [`Command`]s out, replies back.
//!
//! [`serve_connection`] is generic over its transport and over its
//! [`Router`], which is what lets the simulator run the real connection code
//! over simulated TCP against a deliberately racy router. Nothing here knows
//! whether it is talking to a socket or to a `duplex` pipe in a test.
//!
//! # What this layer is responsible for
//!
//! It is the only place where bytes a peer chose become something the rest of
//! the system acts on, so it owns the limits:
//!
//! - **Where a command is answered.** A command about the connection itself —
//!   `PING`, `ECHO`, `QUIT`, `HELLO` — has no key, so there is no shard it
//!   could belong to; it is answered here and no shard hears of it. Only keyed
//!   commands become messages. [`Action`] is that decision made explicit.
//!
//! - **Bounded buffering.** [`MAX_REQUEST_BYTES`] caps what one connection can
//!   make the server hold, on top of the per-frame ceilings the codec
//!   enforces ([`seedstone_resp::MAX_BULK_LEN`],
//!   [`seedstone_resp::MAX_ARRAY_LEN`]). Without a cap, a peer
//!   that opens a frame and never finishes it is a slow memory leak with a
//!   connection attached. The cap is *set* here and *enforced* by the
//!   [`Decoder`] this layer hands it to — see [`MAX_REQUEST_BYTES`] for why
//!   the two are not the same place.
//! - **No response splitting.** Every error frame this module emits passes
//!   through [`safe_error`] first. A `Frame::Error` is terminated by the first
//!   `\r\n` after its type byte, so text carrying either byte would let a peer
//!   dictate frames the server never meant to send — and the codec's guard
//!   against that is a debug assertion, which is not there in release. This is
//!   the enforcement point that is.

use crate::shard::{Command, Reply, ReplyError, Router, parse_i64};
use seedstone_resp::{Decoder, DecoderLimits, Frame, ParseError, encode};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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
/// **The verdict is read from the reads, so a connection that stops entirely
/// keeps what it holds.** Nothing wakes a connection task that is parked on a
/// read, so a peer that goes silent mid-conversation is never re-measured; it
/// is a peer that goes *quiet* — still talking, in small requests — that
/// hands its capacity back. Closing that gap needs an idle signal this layer
/// does not have, which is a clock, and a clock is not this buffer's decision
/// to make.
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

/// How much of a peer-supplied byte string an error message may quote.
const QUOTE_LIMIT: usize = 32;

/// What a peer asking for a protocol this server does not speak is told.
///
/// Byte-exact to Redis, and load-bearing rather than cosmetic: go-redis v9
/// opens every connection with `HELLO 3` and downgrades to RESP2 on exactly
/// this prefix. A clearer message would break the client.
pub const NOPROTO: &str = "NOPROTO unsupported protocol version";

/// What this server answers `HELLO` with, and what it calls itself.
const SERVER_NAME: &str = "seedstone";

/// What to do with one request frame.
///
/// The distinction the type draws is where a command is answered.
/// [`Command`]s belong to a shard and travel; the rest — the connection's own
/// business, and everything the peer got wrong — is answered right here,
/// without a message ever leaving the connection task.
enum Action {
    /// A keyed command: route it and reply with what the shard says.
    Dispatch(Command),
    /// Answer with this frame; the connection continues.
    Reply(Frame),
    /// Answer with this frame, then hang up.
    ReplyThenClose(Frame),
}

/// Serves one connection until the peer disconnects or sends something that
/// can never be a valid frame.
///
/// Complete frames are drained from the read buffer, mapped to commands and
/// dispatched one at a time; the replies accumulate and are written and
/// **flushed together when the drain ends** — the moment before this loop
/// would park on `read`. The flush placement is not optional: a transport
/// that buffers — a simulated one especially — would otherwise hold a reply
/// the peer is blocked waiting for, and the deadlock only appears once the
/// code runs under the simulator. The invariant is that this loop never
/// waits for bytes while a reply sits unflushed; batching within one drain
/// preserves it, because a drain only ends when the decoder has no complete
/// frame left.
///
/// Accumulation is bounded rather than open-ended: a drain that reaches
/// [`REPLY_HIGH_WATER`] writes there and carries on into the same buffer, so
/// what one connection can hold does not scale with how much its peer chose
/// to pipeline. Writing earlier can never violate the invariant above — it
/// only shortens the time a reply spends buffered.
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
pub async fn serve_connection<S, R>(stream: S, router: R)
where
    S: AsyncRead + AsyncWrite + Unpin,
    R: Router,
{
    serve_connection_limited(stream, router, MAX_REQUEST_BYTES).await;
}

/// [`serve_connection`] with the accumulation ceiling as a parameter.
///
/// The ceiling exists so that it can be exercised. Reaching 64 MiB through a
/// pipe is linear work now rather than quadratic, but it is still 64 MiB
/// written, copied and held, for a property a 64 KiB ceiling demonstrates
/// identically — so a test on the real constant would be the slowest thing in
/// the suite by a wide margin, and this layer's stated primary defence would
/// go on having no coverage at all.
async fn serve_connection_limited<S, R>(mut stream: S, router: R, max_request_bytes: usize)
where
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

    loop {
        // Drain every complete frame the decoder already holds before asking
        // the transport for more. The replies accumulate in `out`; the write
        // happens once, at the drain's end.
        let mut hang_up = false;
        let mut drained = false;
        while !drained && !hang_up {
            match decoder.try_next() {
                Ok(Some(frame)) => {
                    match frame_to_action(frame) {
                        Action::Dispatch(cmd) => {
                            append_frame(&mut out, &reply_to_frame(router.dispatch(cmd).await));
                        }
                        Action::Reply(frame) => append_frame(&mut out, &frame),
                        Action::ReplyThenClose(frame) => {
                            append_frame(&mut out, &frame);
                            hang_up = true;
                        }
                    }
                    // A drain that has already earned a write's worth of
                    // replies takes it here rather than waiting for the
                    // decoder to run dry — see [`REPLY_HIGH_WATER`]. The peer
                    // sees the same bytes in the same order, only sooner, so
                    // none of the three orderings this loop guarantees moves:
                    // the write happens *between* two replies, never inside
                    // one, and never while a frame is half-decoded.
                    if out.len() >= REPLY_HIGH_WATER && !flush_replies(&mut stream, &mut out).await
                    {
                        return;
                    }
                }
                // A proper prefix of a valid frame: read more.
                Ok(None) => drained = true,
                Err(error) => {
                    // Terminal, and that now covers the accumulation ceiling
                    // as well as malformed bytes: either way the decoder holds
                    // a half-read frame with no resync point. Report it and
                    // go, without draining.
                    //
                    // The error frame is appended *behind* whatever this drain
                    // already answered, so the peer sees the same order it
                    // would have seen from a flush per reply: the replies it
                    // earned, then the refusal.
                    append_frame(&mut out, &safe_error(&protocol_error(&error)));
                    flush_replies(&mut stream, &mut out).await;
                    return;
                }
            }
        }

        if !flush_replies(&mut stream, &mut out).await || hang_up {
            return;
        }

        match stream.read(&mut read_buf).await {
            // EOF, or a transport that failed. Either way the connection is
            // over and there is nobody left to tell.
            Ok(0) | Err(_) => return,
            Ok(got) => {
                decoder.feed(&read_buf[..got]);
                resize_connection_buffers(
                    &mut read_buf,
                    &mut decoder,
                    &mut out,
                    &mut quiet_reads,
                    got,
                );
            }
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
        read_buf.truncate(READ_FLOOR);
        read_buf.shrink_to(READ_FLOOR);
        decoder.shed_to(READ_FLOOR);
        out.shrink_to(READ_FLOOR);
    }
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

/// Translates a shard's [`Reply`] into the frame that carries it.
fn reply_to_frame(reply: Reply) -> Frame {
    match reply {
        Reply::Ok => Frame::Simple("OK".into()),
        Reply::Bulk(None) => Frame::Null,
        Reply::Bulk(Some(value)) => Frame::Bulk(value),
        Reply::Removed(removed) => Frame::Integer(i64::from(removed)),
        Reply::Integer(n) => Frame::Integer(n),
        // No `safe_error` here, and that is not an omission. A shard error is
        // a [`ReplyError`] variant, so its text is a literal in `shard.rs`
        // rather than anything a router composed — the type is what rules out
        // a terminator, and `every_shard_error_is_frame_safe` checks the whole
        // set. `safe_error` still guards the paths below, where the text is
        // built from bytes a peer chose.
        Reply::Error(error) => Frame::Error(error.wire_text().to_owned()),
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
fn frame_to_action(frame: Frame) -> Action {
    match action_for(frame) {
        Ok(action) => action,
        Err(message) => Action::Reply(safe_error(&message)),
    }
}

/// [`frame_to_action`]'s body, with the error path expressed as `Err`.
///
/// Command names are matched case-insensitively, as Redis does.
fn action_for(frame: Frame) -> Result<Action, String> {
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

    let Some((name, args)) = args.split_first() else {
        return Err("ERR Protocol error: empty command".into());
    };

    // ASCII-uppercase only, which is what the command names are.
    let upper: Vec<u8> = name.to_ascii_uppercase();

    // The connection's own commands, answered without a shard hearing of
    // them: they are about this socket, and there is no key to route on.
    match upper.as_slice() {
        b"PING" => {
            return match args {
                [] => Ok(Action::Reply(Frame::Simple("PONG".into()))),
                [message] => Ok(Action::Reply(Frame::Bulk(message.clone()))),
                _ => Err(wrong_arity("ping")),
            };
        }
        b"ECHO" => {
            return match args {
                [message] => Ok(Action::Reply(Frame::Bulk(message.clone()))),
                _ => Err(wrong_arity("echo")),
            };
        }
        b"QUIT" => {
            return match args {
                [] => Ok(Action::ReplyThenClose(Frame::Simple("OK".into()))),
                _ => Err(wrong_arity("quit")),
            };
        }
        b"HELLO" => return hello(args),
        _ => {}
    }

    keyed_command(&upper, name, args).map(Action::Dispatch)
}

/// Answers `HELLO`, which is how a client asks what it is talking to.
///
/// This server speaks RESP2 and only RESP2, so the version argument has
/// exactly one accepted value. The refusal for every other one is
/// [`NOPROTO`] — see that constant for why the exact text is a contract.
fn hello(args: &[Vec<u8>]) -> Result<Action, String> {
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
        Ok(Action::Reply(hello_frame()))
    } else {
        Err(NOPROTO.to_owned())
    }
}

/// The `HELLO` reply: a flat array of key-value pairs, which is how RESP2
/// carries a map.
fn hello_frame() -> Frame {
    Frame::Array(vec![
        Frame::Bulk(b"server".to_vec()),
        Frame::Bulk(SERVER_NAME.as_bytes().to_vec()),
        Frame::Bulk(b"version".to_vec()),
        Frame::Bulk(env!("CARGO_PKG_VERSION").as_bytes().to_vec()),
        Frame::Bulk(b"proto".to_vec()),
        Frame::Integer(2),
        Frame::Bulk(b"mode".to_vec()),
        Frame::Bulk(b"standalone".to_vec()),
        Frame::Bulk(b"role".to_vec()),
        Frame::Bulk(b"master".to_vec()),
    ])
}

/// Maps a keyed command to the [`Command`] a shard will run.
///
/// `upper` is the uppercased name matched on; `name` is the original bytes,
/// kept only so an unknown command can be quoted back as the peer spelled it.
fn keyed_command(upper: &[u8], name: &[u8], args: &[Vec<u8>]) -> Result<Command, String> {
    match upper {
        b"GET" => match args {
            [key] => Ok(Command::Get { key: key.clone() }),
            _ => Err(wrong_arity("get")),
        },
        b"SET" => match args {
            [key, value] => Ok(Command::Set {
                key: key.clone(),
                value: value.clone(),
            }),
            _ => Err(wrong_arity("set")),
        },
        b"DEL" => match args {
            [key] => Ok(Command::Del { key: key.clone() }),
            _ => Err(wrong_arity("del")),
        },
        b"INCRBY" => match args {
            [key, delta] => parse_i64(delta)
                .map(|delta| Command::IncrBy {
                    key: key.clone(),
                    delta,
                })
                .ok_or_else(|| ReplyError::NotAnInteger.wire_text().to_owned()),
            _ => Err(wrong_arity("incrby")),
        },
        // The name is peer-supplied. It is quoted, not echoed.
        _ => Err(format!("ERR unknown command '{}'", quote(name))),
    }
}

/// The arity message, with the command's own lowercase name — a literal from
/// the table above, never peer-supplied text.
fn wrong_arity(name: &str) -> String {
    format!("ERR wrong number of arguments for '{name}' command")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dict::DictSeed;
    use crate::shard::{NoTrace, ShardPool};
    use seedstone_resp::{MAX_ARRAY_LEN, MAX_BULK_LEN, parse};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn serves_resp_over_a_duplex_stream() {
        let pool = ShardPool::spawn(16, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(4096);
        tokio::spawn(serve_connection(server, pool));
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
    }

    #[tokio::test]
    async fn connection_commands_never_reach_the_router() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(serve_connection(server, UnreachableRouter));
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
            // Last: it closes the connection.
            &["QUIT"],
        ] {
            encode(&req(parts), &mut out);
        }
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        let frames = read_frames(&mut r, 10).await;
        assert_eq!(frames[0], Frame::Simple("PONG".into()));
        assert_eq!(frames[1], Frame::Simple("PONG".into()), "case-insensitive");
        assert_eq!(frames[2], Frame::Bulk(b"hi".to_vec()));
        assert!(matches!(&frames[3], Frame::Error(e) if e.contains("wrong number of arguments")));
        assert_eq!(frames[4], Frame::Bulk(b"x".to_vec()));
        assert!(matches!(&frames[5], Frame::Error(e) if e.contains("wrong number of arguments")));
        assert_eq!(frames[6], frames[7], "HELLO and HELLO 2 answer the same");
        assert_eq!(frames[8], Frame::Error(NOPROTO.into()));
        assert_eq!(
            frames[9],
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
        tokio::spawn(serve_connection(transport, UnreachableRouter));

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

        let pool = ShardPool::spawn(16, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(4 * 1024 * 1024);
        let flushes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_write = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let transport = FlushCounting {
            inner: server,
            flushes: std::sync::Arc::clone(&flushes),
            max_write: std::sync::Arc::clone(&max_write),
        };
        tokio::spawn(serve_connection(transport, pool));
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
        let pool = ShardPool::spawn(4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(4096);
        let task = tokio::spawn(serve_connection(server, pool));
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

        let pool = ShardPool::spawn(4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(8 * 1024);
        let task = tokio::spawn(serve_connection_limited(server, pool, CEILING));
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

        let pool = ShardPool::spawn(4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(4096);
        let task = tokio::spawn(serve_connection_limited(server, pool, CEILING));
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

        let pool = ShardPool::spawn(4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(4096);
        let task = tokio::spawn(serve_connection_limited(server, pool, CEILING));
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
        let pool = ShardPool::spawn(shards, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(serve_connection(server, pool.clone()));
        let (r, w) = tokio::io::split(client);
        (r, w, pool)
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
