//! SeedStone RESP2 codec. One dependency, `bytes`, for the bulk's payload.

use bytes::Bytes;

/// A RESP2 frame — the wire protocol unit for Redis Serialization Protocol version 2.
///
/// Encodes to its RESP2 representation via [`encode`].
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// Simple string: `+OK\r\n`
    ///
    /// The text must contain no `\r` and no `\n`: the `\r\n` that follows it
    /// is the frame terminator, so either byte inside the text ends the frame
    /// early and the remainder is read as further frames. See [`encode`].
    Simple(String),
    /// Error: `-ERR boom\r\n`
    ///
    /// The text must contain no `\r` and no `\n`, for the same reason as
    /// [`Frame::Simple`]. This matters most here, because error messages are
    /// the usual place where client-supplied text is quoted back. See
    /// [`encode`].
    Error(String),
    /// Integer: `:-7\r\n`
    Integer(i64),
    /// Bulk string: `$2\r\nhi\r\n`
    ///
    /// A `Bytes` rather than a `Vec<u8>`, so that whoever stores the payload
    /// can hand it back later by reference count instead of by copy: the
    /// decoder produces it once, the keyspace keeps it, and a reply carrying
    /// the same bytes clones a pointer.
    Bulk(Bytes),
    /// Null bulk string: `$-1\r\n`
    Null,
    /// Array of frames: `*2\r\n...`
    Array(Vec<Self>),
}

/// Encodes a RESP2 frame to its wire representation, appending bytes to `out`.
///
/// # Precondition
///
/// The text of a [`Frame::Simple`] or a [`Frame::Error`] must contain no `\r`
/// and no `\n`. Those two frames are terminated by the first `\r\n` after
/// their type byte, so an embedded `\r` or `\n` ends the frame early and
/// whatever follows it in the text is read by the peer as further frames of
/// its own choosing. When such text is built from client-supplied input — an
/// error message quoting a command name, say — that is response splitting:
/// the client dictates frames the server never meant to send.
///
/// They are the only two variants with this restriction. [`Frame::Bulk`] is
/// length-prefixed, so it carries arbitrary bytes, `\r\n` and NUL included,
/// without ambiguity; prefer it for anything derived from input.
///
/// Encoding stays infallible: callers construct these frames from text they
/// control, and the precondition is checked by a debug assertion rather than
/// enforced by escaping or a `Result`.
///
/// # Depth
///
/// Encoding does not recurse. Nesting is walked with an explicit stack on the
/// heap, so no frame — however deep — can overflow the call stack here. That
/// is a stronger guarantee than `MAX_ARRAY_DEPTH` gives on the way in: the
/// depth limit says which frames [`parse`] will *produce*, and says nothing
/// about a frame a caller built itself. Encoding one of those used to be an
/// abort waiting for the first caller careless enough to construct it.
///
/// What the change buys is a ceiling the machine sets rather than one a
/// thread does, not a cost that went away. The work stack holds one pointer
/// per frame still to emit, and an array pushes all of its children the
/// moment its header goes out, so the peak follows the widest level's
/// *element count* — pointers, not payload bytes. That is a heap allocation,
/// and an allocation that cannot be served aborts too; the difference is that
/// it is bounded by the memory available instead of by the few megabytes a
/// thread's stack happens to be.
///
/// # Example
///
/// ```
/// use seedstone_resp::{Frame, encode};
///
/// let mut out = Vec::new();
/// encode(&Frame::Simple("OK".into()), &mut out);
/// assert_eq!(out, b"+OK\r\n");
/// ```
pub fn encode(frame: &Frame, out: &mut Vec<u8>) {
    // An array contributes only its header: RESP2 has no closing delimiter,
    // so its elements follow in order and nothing has to be emitted after
    // them. That is what lets one flat stack replace the recursion — pushing
    // the children in reverse is the whole of the bookkeeping.
    let mut pending = vec![frame];
    while let Some(frame) = pending.pop() {
        encode_one(frame, out, &mut pending);
    }
}

/// Emits one frame's own bytes, pushing an array's elements onto `pending`.
fn encode_one<'a>(frame: &'a Frame, out: &mut Vec<u8>, pending: &mut Vec<&'a Frame>) {
    match frame {
        Frame::Simple(s) => {
            debug_assert!(
                !s.as_bytes().contains(&b'\r') && !s.as_bytes().contains(&b'\n'),
                "simple string must not contain CR or LF: an embedded terminator \
                 splits the frame and lets the text inject frames of its own"
            );
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Frame::Error(e) => {
            debug_assert!(
                !e.as_bytes().contains(&b'\r') && !e.as_bytes().contains(&b'\n'),
                "error string must not contain CR or LF: an embedded terminator \
                 splits the frame and lets the text inject frames of its own"
            );
            out.push(b'-');
            out.extend_from_slice(e.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Frame::Integer(i) => {
            out.push(b':');
            out.extend_from_slice(i.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Frame::Bulk(b) => {
            out.push(b'$');
            out.extend_from_slice(b.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(b);
            out.extend_from_slice(b"\r\n");
        }
        Frame::Null => {
            out.extend_from_slice(b"$-1\r\n");
        }
        Frame::Array(frames) => {
            out.push(b'*');
            out.extend_from_slice(frames.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            pending.extend(frames.iter().rev());
        }
    }
}

/// An error produced by [`parse`] when the input bytes can never form a valid
/// RESP2 frame, regardless of how many more bytes arrive.
///
/// This is distinct from "not enough bytes yet", which `parse` reports as
/// `Ok(None)` instead. A `ParseError` is terminal: the caller should not
/// retry parsing on the same connection.
#[derive(Debug, PartialEq, Eq)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ParseError {}

/// The maximum number of nested [`Frame::Array`] levels `parse` accepts.
///
/// A top-level array is nesting level 1. Levels 1 through 64 are accepted;
/// an array that would be the 65th level of nesting is rejected with a
/// [`ParseError`] rather than recursed into.
const MAX_ARRAY_DEPTH: usize = 64;

/// The largest [`Frame::Bulk`] payload [`parse`] accepts, in bytes.
///
/// A declared length above this is a [`ParseError`] the moment the length
/// line is read, before a single payload byte is buffered. The ceiling does
/// three things at once:
///
/// - It bounds what a peer can make the reader hold. Without it, `$` plus a
///   huge length is a standing instruction to buffer that many bytes, and the
///   reader obeys until it dies.
/// - It makes the protocol's accept set independent of pointer width. The
///   length is parsed as `i64`, and converting it to `usize` succeeds on a
///   64-bit target and fails on a 32-bit one — so `$4294967296` used to be a
///   terminal error on one and "wait for more bytes, forever" on the other.
///   Checked against a constant, both targets reject it identically.
/// - It keeps a command within what the replication log can hold. A record
///   body is capped at 64 MiB; two payloads at this ceiling plus framing sit
///   well under that, so no accepted command can produce a record the log
///   would refuse to write.
///
///   **That last point rests on what one record can carry, which this crate
///   does not enforce.** Nothing here ties the number of bulks in an array to
///   what a command does with them, and [`DecoderLimits::max_in_memory`]
///   bounds only the total, which is the log record's ceiling and not a rule
///   about arity. What holds the arithmetic up is two properties of the layers
///   above, and a command that breaks either one breaks it:
///
///   - **A command carries at most two payloads of unbounded size** — a key
///     and a value. The options it may also carry are small literals and short
///     numbers: `SET k v EX 30` adds `EX` and `30`, which is tens of bytes on
///     top of the two payloads, not a third one at this ceiling.
///   - **A record describes one key.** A shard command addresses exactly one
///     key, because the shard that owns each key is not in general the same
///     shard, so a request naming many (`MSET`, a multi-key `DEL`) is fanned
///     out into one command — and so one record — per key before any of them
///     reaches a shard. The record never holds the whole request.
///
///   A third unbounded payload, or a record built from a whole multi-key
///   request, puts the arithmetic back in question, and whoever adds one owns
///   re-checking it against the log's ceiling.
pub const MAX_BULK_LEN: usize = 16 * 1024 * 1024;

/// The largest [`Frame::Array`] element count [`parse`] accepts.
///
/// Same reasoning as [`MAX_BULK_LEN`], for the other length-prefixed frame:
/// an unbounded count is an unbounded amount of work and buffering, and the
/// `i64`-to-`usize` conversion splits behaviour by pointer width in exactly
/// the same way.
pub const MAX_ARRAY_LEN: usize = 1024 * 1024;

/// Parses one RESP2 field holding a signed integer (a bulk/array length or an
/// `Integer` frame's value) from `field`, which must not include the
/// trailing `\r\n`.
///
/// Only an optional leading `-` followed by one or more ASCII digits is
/// accepted — no leading `+`, no whitespace, no empty field. This rejects
/// malformed input instead of relying on `str::parse`'s more permissive
/// grammar, and reports overflow as an error instead of panicking.
fn parse_i64(field: &[u8]) -> Result<i64, ParseError> {
    if field.is_empty() {
        return Err(ParseError("empty integer field".into()));
    }
    let digits = if field[0] == b'-' { &field[1..] } else { field };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Err(ParseError(format!(
            "invalid integer field: {:?}",
            String::from_utf8_lossy(field)
        )));
    }
    // `field` is either all ASCII digits or a leading '-' plus ASCII digits,
    // both verified above, so this is always valid UTF-8 — but check instead
    // of asserting it, so a bug in the validation above is a `ParseError`,
    // never a panic.
    let text = std::str::from_utf8(field)
        .map_err(|_| ParseError("integer field is not valid UTF-8".into()))?;
    text.parse::<i64>().map_err(|_| {
        ParseError(format!(
            "integer field out of range: {:?}",
            String::from_utf8_lossy(field)
        ))
    })
}

/// One of this module's length ceilings, in the width a declared length is
/// parsed at.
///
/// A declared length is compared against the ceiling *before* it is narrowed to
/// `usize`, so the comparison has to happen in `i64`. Both ceilings are small
/// compile-time constants, so widening them is exact on every target — which is
/// what makes the accept set independent of pointer width.
fn ceiling(limit: usize) -> i64 {
    i64::try_from(limit).expect("a length ceiling is a small constant")
}

/// The bounds a [`Decoder`] enforces on top of the per-frame ceilings.
///
/// [`MAX_BULK_LEN`] and [`MAX_ARRAY_LEN`] cap what any *one* length prefix may
/// declare, and they are not configurable: they are part of the protocol's
/// accept set, identical on every target. These two are the other half — what
/// a peer may make the decoder *hold* while a frame is still arriving — and a
/// caller sets them to whatever its own budget is.
///
/// # How the two relate
///
/// They are independent, and both orderings are legal, but they are not
/// independent in *effect* and a caller choosing them should know which one
/// it has made binding.
///
/// A frame's parsed form is never smaller than its wire form: the cheapest
/// element on the wire is four bytes and the cheapest one in memory is a
/// whole [`Frame`], and payload bytes cost one of each. So setting
/// `max_in_memory <= max_frame_bytes` — which is what a caller with a single
/// per-request budget naturally does — makes `max_in_memory` the binding
/// bound for every frame, and `max_frame_bytes` catches only what is still
/// accumulating: a frame with no declared length, an unterminated `+` line
/// above all, whose size is not yet known to either check.
///
/// Nothing here enforces an ordering, because neither is wrong. What does
/// depend on it is *which refusal a peer is told about*, and that is not
/// something to build on — see [`Decoder::feed`].
#[derive(Debug, Clone, Copy)]
pub struct DecoderLimits {
    /// The most undecoded wire bytes one frame may occupy.
    ///
    /// Checked both while a frame is still arriving — so it bounds the buffer
    /// a dribbled frame can grow, and a peer that opens a frame and never
    /// closes it is cut off rather than fed forever — and where the frame
    /// completes, so the verdict does not depend on where the reads fell.
    pub max_frame_bytes: usize,
    /// The most memory the *parsed* form of one frame may occupy.
    ///
    /// The wire form does not reveal this. An `Integer` element is four bytes
    /// on the wire and a whole [`Frame`] once parsed, so an array of them
    /// amplifies by roughly eight; a bound on bytes read cannot see that, and
    /// this one is what stops it.
    pub max_in_memory: usize,
}

impl DecoderLimits {
    /// The buffer capacity a drained decoder shrinks back to.
    ///
    /// A connection that once carried a large frame otherwise keeps that
    /// allocation for its whole life, and idle connections outnumber busy
    /// ones. Below this, shrinking would cost more churn than it saves.
    pub const SHED: usize = 256 * 1024;
}

impl Default for DecoderLimits {
    /// 64 MiB each, coherent with the connection layer's own request ceiling:
    /// the decoder refuses at the same point the reader above it would.
    fn default() -> Self {
        Self {
            max_frame_bytes: 64 * 1024 * 1024,
            max_in_memory: 64 * 1024 * 1024,
        }
    }
}

/// An array whose header has been read and whose elements are still arriving.
#[derive(Debug)]
struct PendingArray {
    /// Elements still to come before the array is complete.
    remaining: usize,
    /// Elements already parsed, in order.
    elements: Vec<Frame>,
}

/// One completed unit of input: a value, or the header of an array whose
/// elements are still to come.
#[derive(Debug)]
enum Element {
    Value(Frame),
    ArrayHeader(usize),
}

/// What a RESP2 type byte says the element is.
///
/// Reading the type byte into this is the first thing the reader does, and it
/// is where an unknown byte is refused — before a terminator is searched for,
/// so junk cannot buy a peer any buffering at all.
#[derive(Debug, Clone, Copy)]
enum Kind {
    Simple,
    Error,
    Integer,
    Bulk,
    Array,
}

/// How far into the current element the reader has got.
///
/// This enum is the whole point of the rework: nothing already understood is
/// understood twice. A resumed decode picks up from the recorded position
/// instead of re-reading the element from its type byte, which is what turns
/// a dribbled frame from quadratic work into linear.
#[derive(Debug, Clone, Copy)]
enum Partial {
    /// At the element's type byte; none of it has been read.
    Start,
    /// Inside a CRLF-terminated line. `scanned` is the offset of the first
    /// byte not yet tested as the start of a `\r\n`, so a resumed scan skips
    /// everything already searched.
    Line { scanned: usize },
    /// A `$` header has been read and validated. The payload starts `header`
    /// bytes past the element's first byte and runs for `len`, so waiting for
    /// it is a comparison of two numbers — the payload is never scanned.
    BulkBody { header: usize, len: usize },
}

/// The resumable half of the decoder: everything except the byte buffer.
///
/// Split out from [`Decoder`] so [`parse`] can run the same state machine
/// over a slice it does not own, and so the one-shot and streaming entry
/// points cannot drift apart.
#[derive(Debug)]
struct Decoding {
    /// Where in the buffer the frame currently being read begins.
    ///
    /// Everything before it belongs to frames already delivered and is dead
    /// weight the buffer will drop at its next compaction. Keeping it as an
    /// offset rather than compacting on the spot is what makes a pipelined
    /// read batch cost one move instead of one per frame.
    frame_start: usize,
    /// First byte not yet consumed by a *completed* element.
    scan: usize,
    /// Position inside the element at `scan`.
    partial: Partial,
    /// Arrays still being filled, outermost first. Flat, so no input can
    /// recurse this into the call stack.
    stack: Vec<PendingArray>,
    /// Accounted size of everything held in `stack`, plus the frame about to
    /// leave it.
    in_memory: usize,
    /// Bytes the state machine has looked at or copied.
    ///
    /// Only [`Decoder::bytes_examined`] reads it, and only under `cfg(test)`;
    /// it exists so the linearity property is a test assertion rather than a
    /// claim. Counting is a handful of additions per element, not one per
    /// byte, so it stays in ordinary builds rather than behind a feature that
    /// would have to be unified across the workspace.
    ///
    /// Every increment saturates. It is compiled into release builds and
    /// never reset, so on a target with a 32-bit `usize` a long-lived
    /// connection reaches the wrap in four gigabytes — and a debug build
    /// would answer that with an arithmetic panic, in the hot path of a crate
    /// whose integer parsing goes out of its way to have no panicking path at
    /// all. Resetting it per frame would be the other fix and is the wrong
    /// one: the tests read it after frames complete, so a per-frame reset
    /// would leave them asserting on a counter near zero however quadratic
    /// the decoder became.
    examined: usize,
}

impl Decoding {
    /// A decoder positioned at the start of a frame, holding nothing.
    const fn new() -> Self {
        Self {
            frame_start: 0,
            scan: 0,
            partial: Partial::Start,
            stack: Vec::new(),
            in_memory: 0,
            examined: 0,
        }
    }

    /// Records that the frame that just completed is no longer being read.
    ///
    /// Its bytes stay in the buffer — dropping them here would be a move per
    /// frame — and the next frame starts where this one ended.
    fn finish_frame(&mut self) {
        debug_assert!(self.stack.is_empty(), "a delivered frame leaves no arrays");
        self.frame_start = self.scan;
        self.partial = Partial::Start;
        self.in_memory = 0;
    }

    /// Rebases every offset after `cut` bytes were removed from the buffer's
    /// front.
    ///
    /// `cut` is always `frame_start`, so what is discarded is exactly the
    /// frames already delivered. [`Partial::BulkBody`] needs no adjustment:
    /// its two fields are measured from `scan`, not from the buffer.
    fn shift_back(&mut self, cut: usize) {
        debug_assert!(cut <= self.scan, "a cut may never pass the read cursor");
        self.frame_start -= cut;
        self.scan -= cut;
        if let Partial::Line { scanned } = &mut self.partial {
            *scanned -= cut;
        }
    }

    /// Reads elements from `buf` until one completes a top-level frame.
    ///
    /// `Ok(None)` means the input ran out mid-frame; every element completed
    /// so far is retained, and the next call resumes rather than restarts.
    fn step(&mut self, buf: &[u8], limits: &DecoderLimits) -> Result<Option<Frame>, ParseError> {
        loop {
            let Some(element) = self.read_element(buf, limits)? else {
                // Nothing can follow an unfinished frame, so every byte from
                // `frame_start` on belongs to it: this is its wire size so far.
                check_frame_bytes(buf.len() - self.frame_start, limits)?;
                return Ok(None);
            };
            if let Some(frame) = self.place(element, limits)? {
                // The same ceiling on the completion path, and it is not
                // redundant with the one above: without it a frame over the
                // limit is refused only when a chunk boundary happens to fall
                // while it is still incomplete, so the verdict would depend on
                // how the peer split its writes. Checked here as well, the
                // same bytes give the same answer at every chunking — which is
                // the property everything above this crate reasons with.
                check_frame_bytes(self.scan - self.frame_start, limits)?;
                return Ok(Some(frame));
            }
        }
    }

    /// Reads the single element at `scan`, advancing `scan` past it.
    ///
    /// `Ok(None)` means the element is incomplete; `partial` then records how
    /// far it got, so the next attempt does not start over.
    fn read_element(
        &mut self,
        buf: &[u8],
        limits: &DecoderLimits,
    ) -> Result<Option<Element>, ParseError> {
        // A bulk whose header is already parsed needs no scanning at all, and
        // no pricing either: its cost was charged when its header was read.
        if let Partial::BulkBody { header, len } = self.partial {
            return Ok(self.finish_bulk(buf, header, len)?.map(Element::Value));
        }

        // An empty line between two frames is no frame. Redis reads it as an
        // inline command with no words and does nothing — measured on 7.4.11:
        // `\r\n*1\r\n$4\r\nPING\r\n` answers `+PONG` — and `redis-cli --pipe`
        // (8.10.0) writes one after the last command of every transfer, ahead
        // of the `ECHO` it closes the transfer with. Only at the top level:
        // inside an array an element is owed, and a `\r` where a type byte is
        // due is the unknown byte it always was. The skipped bytes move
        // `frame_start` so they are compacted with the frames before them
        // and never priced against the frame that follows.
        if self.stack.is_empty() && matches!(self.partial, Partial::Start) {
            while buf.get(self.scan) == Some(&b'\r') {
                match buf.get(self.scan + 1) {
                    Some(b'\n') => {
                        self.scan += 2;
                        self.frame_start = self.scan;
                        self.examined = self.examined.saturating_add(2);
                    }
                    // A lone `\r` at the end: wait for the byte after it.
                    None => return Ok(None),
                    // `\r` followed by anything else is a type byte — the
                    // refusal below names it.
                    Some(_) => break,
                }
            }
        }

        let Some(&type_byte) = buf.get(self.scan) else {
            return Ok(None);
        };
        self.examined = self.examined.saturating_add(1);
        // The type byte is decided on before anything is spent looking for a
        // terminator. No continuation can rescue an unknown one, so waiting
        // for a `\r\n` that may never come would turn one junk byte into a
        // licence to buffer up to `max_frame_bytes` — a peer that opens with
        // garbage has to be cut off on its first byte, not its 64 millionth.
        let kind = match type_byte {
            b'+' => Kind::Simple,
            b'-' => Kind::Error,
            b':' => Kind::Integer,
            b'$' => Kind::Bulk,
            b'*' => Kind::Array,
            other => return Err(ParseError(format!("unknown RESP2 type byte: {other:#04x}"))),
        };

        let content = self.scan + 1;
        let Some((line_end, next)) = self.find_crlf(buf, content) else {
            return Ok(None);
        };
        let field = &buf[content..line_end];

        let element = match kind {
            Kind::Simple => Element::Value(Frame::Simple(
                String::from_utf8(field.to_vec())
                    .map_err(|_| ParseError("simple string is not valid UTF-8".into()))?,
            )),
            Kind::Error => Element::Value(Frame::Error(
                String::from_utf8(field.to_vec())
                    .map_err(|_| ParseError("error string is not valid UTF-8".into()))?,
            )),
            Kind::Integer => Element::Value(Frame::Integer(parse_i64(field)?)),
            Kind::Bulk => {
                let len = parse_i64(field)?;
                if len == -1 {
                    self.scan = next;
                    return Ok(Some(Element::Value(Frame::Null)));
                }
                if len < -1 {
                    return Err(ParseError(format!("negative bulk length: {len}")));
                }
                if len > ceiling(MAX_BULK_LEN) {
                    return Err(ParseError(format!(
                        "bulk length {len} exceeds the {MAX_BULK_LEN}-byte limit"
                    )));
                }
                // The ceiling above already puts this in range on any target
                // with a 32-bit-or-wider `usize`; the conversion stays so this
                // function has no panicking path at all.
                let len =
                    usize::try_from(len).map_err(|_| ParseError("bulk length too large".into()))?;
                // Priced from the declared length, at the header, before a
                // single payload byte is waited for. Charging it where the
                // payload is copied would be too late in the way that matters:
                // the copy is only reachable once the whole payload has been
                // buffered, so a peer could make the decoder hold 16 MiB it
                // had already been told it could not afford.
                self.afford(size_of::<Frame>().saturating_add(len), limits)?;
                let header = next - self.scan;
                self.partial = Partial::BulkBody { header, len };
                return Ok(self.finish_bulk(buf, header, len)?.map(Element::Value));
            }
            Kind::Array => Element::ArrayHeader(self.array_header(field, limits)?),
        };
        self.scan = next;
        Ok(Some(element))
    }

    /// Validates a `*` count line and returns the element count it declares.
    ///
    /// The depth check lives here rather than where the array is pushed, so
    /// the order the three rejections are tried in matches what a reader of
    /// the protocol expects: sign, then depth, then ceiling.
    fn array_header(&self, field: &[u8], limits: &DecoderLimits) -> Result<usize, ParseError> {
        let count = parse_i64(field)?;
        if count < 0 {
            // `*-1\r\n` (the null array) is not supported; every negative
            // count, including -1, is rejected.
            return Err(ParseError(format!(
                "negative array length is not supported: {count}"
            )));
        }
        if self.stack.len() + 1 > MAX_ARRAY_DEPTH {
            return Err(ParseError(format!(
                "array nesting exceeds the depth limit of {MAX_ARRAY_DEPTH}"
            )));
        }
        if count > ceiling(MAX_ARRAY_LEN) {
            return Err(ParseError(format!(
                "array length {count} exceeds the limit of {MAX_ARRAY_LEN}"
            )));
        }
        // In range by the ceiling above; kept for the same reason as the bulk
        // conversion.
        let count =
            usize::try_from(count).map_err(|_| ParseError("array length too large".into()))?;
        // An array that cannot fit even as empty `Frame`s is refused at its
        // header, before one element is read — the count is a promise about
        // memory, and this is the only moment the promise is cheap to refuse.
        //
        // Measured against what is *left* of the budget, not against the
        // budget entire: an inner array is read on a budget its parent has
        // already spent part of, and comparing against the whole would let it
        // through to be caught one element at a time instead.
        if count.saturating_mul(size_of::<Frame>()) > self.headroom(limits) {
            return Err(ParseError(format!(
                "array of {count} elements exceeds the {}-byte in-memory limit",
                limits.max_in_memory
            )));
        }
        Ok(count)
    }

    /// Completes a `$` element whose header has already been read.
    ///
    /// The declared length short-circuits the wait: while the payload is still
    /// arriving this compares two numbers and looks at no bytes at all, which
    /// is what keeps a 16 MiB bulk delivered in 1 KiB reads linear.
    fn finish_bulk(
        &mut self,
        buf: &[u8],
        header: usize,
        len: usize,
    ) -> Result<Option<Frame>, ParseError> {
        let overflow = || ParseError("bulk length overflows buffer offset".into());
        let end = header
            .checked_add(len)
            .and_then(|n| n.checked_add(2))
            .and_then(|n| self.scan.checked_add(n))
            .ok_or_else(overflow)?;
        if end > buf.len() {
            return Ok(None);
        }
        let payload_start = self.scan + header;
        let payload_end = payload_start + len;
        if &buf[payload_end..end] != b"\r\n" {
            return Err(ParseError("bulk payload missing CRLF terminator".into()));
        }
        self.examined = self.examined.saturating_add(len);
        let payload = Bytes::copy_from_slice(&buf[payload_start..payload_end]);
        self.scan = end;
        self.partial = Partial::Start;
        Ok(Some(Frame::Bulk(payload)))
    }

    /// Finds the first `\r\n` at or after `start`, resuming a scan an earlier
    /// call left unfinished.
    ///
    /// Returns `Some((line_end, after_crlf))` where `buf[start..line_end]` is
    /// the line's content and `after_crlf` is the offset just past the `\r\n`.
    /// `None` means "wait for more bytes" — including when `buf` ends with a
    /// lone `\r` — and records how far the search reached, so a line arriving
    /// one byte at a time is scanned once rather than once per byte.
    fn find_crlf(&mut self, buf: &[u8], start: usize) -> Option<(usize, usize)> {
        let resume = match self.partial {
            Partial::Line { scanned } if scanned > start => scanned,
            _ => start,
        };
        let mut i = resume;
        while i + 1 < buf.len() {
            if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                self.examined = self.examined.saturating_add(i + 2 - resume);
                self.partial = Partial::Start;
                return Some((i, i + 2));
            }
            i += 1;
        }
        // The loop never inspects the final byte, which may yet turn out to be
        // the `\r` of a terminator, so resuming from it is safe.
        self.examined = self.examined.saturating_add(i - resume);
        self.partial = Partial::Line { scanned: i };
        None
    }

    /// Attaches a completed element to the innermost pending array, cascading
    /// through every array the attachment fills.
    ///
    /// Returns the frame once one completes at top level, `None` while the
    /// decoder still owes elements to some array.
    fn place(
        &mut self,
        element: Element,
        limits: &DecoderLimits,
    ) -> Result<Option<Frame>, ParseError> {
        let mut value = match element {
            Element::ArrayHeader(count) => {
                // Depth is checked in `array_header`, before the count is
                // even converted; this push cannot exceed it.
                self.stack.push(PendingArray {
                    remaining: count,
                    elements: Vec::new(),
                });
                // Only a zero-element array can already be complete.
                match self.pop_filled() {
                    Some(frame) => frame,
                    None => return Ok(None),
                }
            }
            Element::Value(frame) => frame,
        };
        loop {
            self.charge(&value, limits)?;
            if self.stack.is_empty() {
                self.in_memory = 0;
                return Ok(Some(value));
            }
            let top = self.stack.last_mut().expect("the stack is not empty");
            top.elements.push(value);
            top.remaining -= 1;
            match self.pop_filled() {
                Some(frame) => value = frame,
                None => return Ok(None),
            }
        }
    }

    /// Pops the innermost pending array if it has all its elements.
    fn pop_filled(&mut self) -> Option<Frame> {
        self.stack
            .pop_if(|array| array.remaining == 0)
            .map(|filled| Frame::Array(filled.elements))
    }

    /// What is left of [`DecoderLimits::max_in_memory`] for this frame.
    ///
    /// The single place the budget is read, so every refusal below is
    /// measured against the same remaining room even though each words its
    /// error for the thing it refused.
    const fn headroom(&self, limits: &DecoderLimits) -> usize {
        limits.max_in_memory.saturating_sub(self.in_memory)
    }

    /// Refuses `cost` further bytes of parsed representation when the budget
    /// cannot cover them, without recording anything.
    ///
    /// Separate from [`Decoding::charge`] so a cost known *before* the thing
    /// that costs it exists — a bulk payload, whose length its header
    /// declares — can be refused before it is waited for.
    fn afford(&self, cost: usize, limits: &DecoderLimits) -> Result<(), ParseError> {
        if cost > self.headroom(limits) {
            return Err(ParseError(format!(
                "decoded frame exceeds the {}-byte in-memory limit",
                limits.max_in_memory
            )));
        }
        Ok(())
    }

    /// Accounts for one frame against [`DecoderLimits::max_in_memory`].
    ///
    /// Charged *before* the frame is pushed, so the limit stops the array from
    /// growing rather than discovering afterwards that it did.
    fn charge(&mut self, frame: &Frame, limits: &DecoderLimits) -> Result<(), ParseError> {
        let cost = size_of::<Frame>().saturating_add(payload_bytes(frame));
        self.afford(cost, limits)?;
        self.in_memory = self.in_memory.saturating_add(cost);
        Ok(())
    }
}

/// Refuses a frame whose wire form is past
/// [`DecoderLimits::max_frame_bytes`].
///
/// Applied at both ends of a frame's life — where it runs out of input and
/// where it completes — which is what makes the verdict the same however the
/// peer chunked its writes.
fn check_frame_bytes(wire_bytes: usize, limits: &DecoderLimits) -> Result<(), ParseError> {
    if wire_bytes > limits.max_frame_bytes {
        return Err(ParseError(format!(
            "frame exceeds the {}-byte buffering limit",
            limits.max_frame_bytes
        )));
    }
    Ok(())
}

/// The heap bytes a frame owns beyond the [`Frame`] itself.
///
/// An array's children are charged as each one is attached, so the array node
/// contributes only its own size and nothing is counted twice.
const fn payload_bytes(frame: &Frame) -> usize {
    match frame {
        Frame::Simple(text) | Frame::Error(text) => text.len(),
        Frame::Bulk(payload) => payload.len(),
        Frame::Integer(_) | Frame::Null | Frame::Array(_) => 0,
    }
}

/// A resumable RESP2 decoder: wire bytes in, frames out.
///
/// The difference from calling [`parse`] on a growing buffer is where the work
/// goes. `parse` starts at offset zero every time, so a request that arrives
/// in `n` chunks re-parses every already-complete element `n` times and
/// re-allocates its payload each time; an array of a million tiny bulks is
/// megabytes of input and order 10⁸ allocations. A `Decoder` keeps the
/// elements it has already understood, so the same input is parsed once.
///
/// It is also non-recursive. Nesting is held in a heap [`Vec`], not in the
/// call stack, so `MAX_ARRAY_DEPTH` is a policy about what the protocol
/// accepts rather than the only thing standing between a hostile frame and a
/// stack overflow.
///
/// # Example
///
/// ```
/// use seedstone_resp::{Decoder, DecoderLimits, Frame};
///
/// let mut decoder = Decoder::new(DecoderLimits::default());
/// decoder.feed(b"*1\r\n$3\r\nGET");
/// assert_eq!(decoder.try_next(), Ok(None)); // still arriving
/// decoder.feed(b"\r\n");
/// assert_eq!(
///     decoder.try_next(),
///     Ok(Some(Frame::Array(vec![Frame::Bulk("GET".into())]))),
/// );
/// assert_eq!(decoder.buffered(), 0);
/// ```
#[derive(Debug)]
pub struct Decoder {
    /// Wire bytes fed but not yet consumed by a delivered frame.
    buf: Vec<u8>,
    state: Decoding,
    limits: DecoderLimits,
}

impl Decoder {
    /// A decoder holding nothing, bounded by `limits`.
    #[must_use]
    pub const fn new(limits: DecoderLimits) -> Self {
        Self {
            buf: Vec::new(),
            state: Decoding::new(),
            limits,
        }
    }

    /// Appends freshly read wire bytes.
    ///
    /// Chunk boundaries carry no meaning: the same bytes produce the same
    /// frames, and the same verdict on whether they are a frame at all,
    /// however they are split. Nothing above this crate has to know how a
    /// peer divided its writes.
    ///
    /// **What is not promised is *which* refusal is reported.** The two
    /// [`DecoderLimits`] bounds are tested at different moments — the parsed
    /// size as elements are built, the wire size as bytes accumulate and
    /// again where a frame completes — so when they are set close together,
    /// a frame that breaks both is refused by whichever one the chunking
    /// reached first, and the message names that one. Set equal, as a caller
    /// with a single per-request budget sets them, an over-long line reports
    /// the in-memory limit when a read completes it and the buffering limit
    /// when a read stops short of doing so. Both are refusals of the same
    /// bytes for the same reason, and the text is for a human reading a log;
    /// no caller should branch on it, and a replay is unaffected either way
    /// because it feeds the same bytes in the same chunks.
    pub fn feed(&mut self, bytes: &[u8]) {
        self.compact();
        self.buf.extend_from_slice(bytes);
    }

    /// Takes the next complete frame, if the bytes fed so far hold one.
    ///
    /// `Ok(None)` means "read more and call again" and costs nothing to
    /// repeat: no element already parsed is parsed a second time. Call it in a
    /// loop after each [`Decoder::feed`] — one read can carry several frames.
    /// That loop is the intended shape, and the buffer is reclaimed on the
    /// call that ends it rather than on each frame, so draining ten frames
    /// from one read moves their bytes once.
    ///
    /// # Errors
    ///
    /// When the bytes can never form a valid frame — the same conditions
    /// [`parse`] rejects — or when the frame in flight exceeds
    /// [`DecoderLimits::max_frame_bytes`] on the wire or
    /// [`DecoderLimits::max_in_memory`] once parsed. Every one of these is
    /// terminal: the decoder holds a half-read frame it cannot resynchronise
    /// from, so the connection is the thing to drop, not the error.
    pub fn try_next(&mut self) -> Result<Option<Frame>, ParseError> {
        let Some(frame) = self.state.step(&self.buf, &self.limits)? else {
            // Out of input: the caller is about to go and read more, so this
            // is the moment to hand back what the frames just delivered were
            // occupying. Once per read batch, not once per frame.
            self.compact();
            return Ok(None);
        };
        self.state.finish_frame();
        Ok(Some(frame))
    }

    /// Drops the bytes of frames already delivered, and sheds the capacity a
    /// large frame left behind.
    ///
    /// Every removal from the front of a `Vec` moves everything after it, so
    /// doing this per frame makes a pipelined read batch quadratic in the
    /// frames it carries: `k` frames in `S` bytes move about `S·k/2` bytes
    /// instead of `S`. Deferring it to the batch boundary is the whole reason
    /// [`Decoding::frame_start`] exists.
    ///
    /// Safe at any point, not only between frames: what it discards is bounded
    /// by `frame_start`, and the offsets that survive are rebased.
    fn compact(&mut self) {
        let cut = self.state.frame_start;
        if cut == 0 {
            return;
        }
        self.buf.drain(..cut);
        self.state.shift_back(cut);
        if self.buf.capacity() > DecoderLimits::SHED {
            // A connection that carried one large frame otherwise holds that
            // buffer for the rest of its life.
            self.buf.shrink_to(DecoderLimits::SHED);
        }
    }

    /// Gives back buffer capacity down to `cap`.
    ///
    /// [`Decoder::feed`] already sheds to [`DecoderLimits::SHED`] on its own,
    /// which is the right floor for a connection between requests: below it,
    /// a decoder that is merely idle for a moment would reallocate its way
    /// back up on the next batch. It is the wrong floor for a connection that
    /// has *stopped*, and only the caller can tell those apart — the evidence
    /// is the shape of its reads, which this crate never sees. So the policy
    /// stays up there and this is the lever it pulls.
    ///
    /// Buffered bytes are never dropped: a frame still arriving survives a
    /// shed intact, and the capacity simply cannot fall below what it
    /// occupies. Calling this while one is in flight is therefore safe, and
    /// merely less effective than calling it on a drained decoder.
    pub fn shed_to(&mut self, cap: usize) {
        // Dead bytes in front of the current frame would hold the length up
        // and defeat the shrink, so they go first.
        self.compact();
        self.buf.shrink_to(cap);
    }

    /// Bytes this decoder's buffer currently reserves.
    ///
    /// The companion to [`Decoder::buffered`]: that one is what a peer has
    /// sent and this one is what holding it costs. A caller sizing a
    /// connection's footprint — or checking that a shed did what it said —
    /// needs this one.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// Bytes fed but not yet consumed by a delivered frame.
    ///
    /// Bytes of delivered frames may still be sitting in the buffer, waiting
    /// for the next compaction; they are not counted, because the caller has
    /// already been given what they were worth.
    #[must_use]
    pub const fn buffered(&self) -> usize {
        self.buf.len() - self.state.frame_start
    }

    /// Bytes the state machine has looked at or copied since construction.
    ///
    /// The linearity property this type exists for is only meaningful if it
    /// can be measured, so the counter is asserted on in this crate's tests.
    #[cfg(test)]
    #[must_use]
    pub const fn bytes_examined(&self) -> usize {
        self.state.examined
    }
}

/// Parses one RESP2 frame from the start of `buf`.
///
/// `buf` may hold more than one frame or a trailing partial one; only the
/// first complete frame is parsed. On success, returns the frame and the
/// number of bytes it occupied, so the caller can drain `consumed` bytes
/// from its buffer before calling `parse` again.
///
/// - `Ok(Some((frame, consumed)))` — one full frame was parsed.
/// - `Ok(None)` — `buf` is a proper prefix of a valid frame; read more bytes
///   from the connection and call `parse` again.
/// - `Err(ParseError)` — `buf` can never be a valid frame. This is terminal
///   for the connection; do not retry.
///
/// A caller that reads from a socket wants [`Decoder`] instead. This function
/// starts from offset zero on every call, so feeding it a growing buffer
/// re-parses — and re-allocates — every element that was already complete;
/// `Decoder` runs the same state machine and keeps what it has understood.
///
/// # Errors
///
/// When `buf` starts with bytes no continuation can rescue: an unknown type
/// byte, a malformed or out-of-range length field, a negative bulk or array
/// length, a bulk payload whose terminator is not `\r\n`, a length above
/// [`MAX_BULK_LEN`] or [`MAX_ARRAY_LEN`], or nesting past the depth limit.
///
/// And, because this function runs on [`DecoderLimits::default`], in two
/// cases that are about resources rather than about the bytes being wrong:
///
/// - **An incomplete frame already past
///   [`DecoderLimits::max_frame_bytes`]**, 64 MiB. This is an `Err` where
///   every shorter prefix of the same frame is `Ok(None)`: a peer that opens
///   a frame and never closes it is cut off rather than waited on forever.
/// - **A complete, well-formed frame whose *parsed* form exceeds
///   [`DecoderLimits::max_in_memory`]**, also 64 MiB. Nothing in such a frame
///   breaks a per-frame ceiling — every bulk is within [`MAX_BULK_LEN`] and
///   every array within [`MAX_ARRAY_LEN`] — but their sum is not something
///   the caller agreed to hold. Two nested arrays of a million integers are
///   8 MiB on the wire and 64 MiB once parsed, and that ratio is exactly what
///   a bound on bytes read cannot see.
///
/// # Example
///
/// ```
/// use seedstone_resp::{Frame, encode, parse};
///
/// let mut buf = Vec::new();
/// encode(&Frame::Integer(42), &mut buf);
/// assert_eq!(parse(&buf), Ok(Some((Frame::Integer(42), buf.len()))));
/// ```
pub fn parse(buf: &[u8]) -> Result<Option<(Frame, usize)>, ParseError> {
    let mut state = Decoding::new();
    // `scan` stops at the end of the frame that completed, which is exactly
    // the byte count this function's contract reports.
    Ok(state
        .step(buf, &DecoderLimits::default())?
        .map(|frame| (frame, state.scan)))
}

#[cfg(test)]
mod tests;
