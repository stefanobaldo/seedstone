//! SeedStone RESP2 codec: no external dependencies.

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
    Bulk(Vec<u8>),
    /// Null bulk string: `$-1\r\n`
    Null,
    /// Array of frames: `*2\r\n...`
    Array(Vec<Frame>),
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
/// Unlike [`parse`], which refuses nesting deeper than [`MAX_ARRAY_DEPTH`],
/// this function recurses on [`Frame::Array`] without a limit and will
/// overflow the stack — an abort, not a panic — on a deeply enough nested
/// frame. Nothing reachable today builds one: a parsed frame is already
/// depth-capped, and the frames this workspace constructs are flat. The
/// asymmetry is safe only while that stays true, so a caller that ever
/// encodes a frame derived from input owes a depth check first.
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
            for f in frames {
                encode(f, out);
            }
        }
    }
}

/// An error produced by [`parse`] when the input bytes can never form a valid
/// RESP2 frame, regardless of how many more bytes arrive.
///
/// This is distinct from "not enough bytes yet", which `parse` reports as
/// `Ok(None)` instead. A `ParseError` is terminal: the caller should not
/// retry parsing on the same connection.
#[derive(Debug, PartialEq)]
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
///   **That last point rests on an assumption this crate does not enforce:
///   that no command carries more than two payloads.** It is the command
///   layer's arity table that holds it up, not the codec — `parse` alone will
///   happily hand back an array of [`MAX_ARRAY_LEN`] bulks of this size. A
///   third bulk argument (`MSET`, `SET … EX`, a multi-key `DEL`) puts four
///   payloads in one record and breaks the arithmetic, so whoever adds one
///   owns re-checking it against the log's ceiling.
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

/// Finds the first `\r\n` in `buf` at or after `start`.
///
/// Returns `Some((line_end, after_crlf))` where `buf[start..line_end]` is
/// the line's content and `after_crlf` is the offset just past the `\r\n`.
/// Returns `None` when no `\r\n` is present yet — including when `buf` ends
/// with a lone `\r` — which means "wait for more bytes", not "malformed".
fn read_line(buf: &[u8], start: usize) -> Option<(usize, usize)> {
    let mut i = start;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some((i, i + 2));
        }
        i += 1;
    }
    None
}

/// Parses one RESP2 frame starting at `buf[pos..]`.
///
/// `depth` is the array-nesting level of the frame at `pos` itself (0 for a
/// frame that is not inside any array). Returns `Ok(Some((frame,
/// after_frame)))` on success, `Ok(None)` if `buf[pos..]` is a proper prefix
/// of a valid frame, or `Err` if it can never be valid.
fn parse_frame(buf: &[u8], pos: usize, depth: usize) -> Result<Option<(Frame, usize)>, ParseError> {
    let Some(&type_byte) = buf.get(pos) else {
        return Ok(None);
    };
    let content_start = pos + 1;

    match type_byte {
        b'+' => {
            let Some((line_end, next)) = read_line(buf, content_start) else {
                return Ok(None);
            };
            let s = String::from_utf8(buf[content_start..line_end].to_vec())
                .map_err(|_| ParseError("simple string is not valid UTF-8".into()))?;
            Ok(Some((Frame::Simple(s), next)))
        }
        b'-' => {
            let Some((line_end, next)) = read_line(buf, content_start) else {
                return Ok(None);
            };
            let s = String::from_utf8(buf[content_start..line_end].to_vec())
                .map_err(|_| ParseError("error string is not valid UTF-8".into()))?;
            Ok(Some((Frame::Error(s), next)))
        }
        b':' => {
            let Some((line_end, next)) = read_line(buf, content_start) else {
                return Ok(None);
            };
            let n = parse_i64(&buf[content_start..line_end])?;
            Ok(Some((Frame::Integer(n), next)))
        }
        b'$' => {
            let Some((line_end, next)) = read_line(buf, content_start) else {
                return Ok(None);
            };
            let len = parse_i64(&buf[content_start..line_end])?;
            if len == -1 {
                return Ok(Some((Frame::Null, next)));
            }
            if len < -1 {
                return Err(ParseError(format!("negative bulk length: {len}")));
            }
            if len > ceiling(MAX_BULK_LEN) {
                return Err(ParseError(format!(
                    "bulk length {len} exceeds the {MAX_BULK_LEN}-byte limit"
                )));
            }
            // The ceiling above already puts this in range on any target with
            // a 32-bit-or-wider `usize`; the conversion stays so the function
            // has no panicking path at all.
            let len =
                usize::try_from(len).map_err(|_| ParseError("bulk length too large".into()))?;
            let payload_start = next;
            let payload_end = payload_start
                .checked_add(len)
                .ok_or_else(|| ParseError("bulk length overflows buffer offset".into()))?;
            let term_end = payload_end
                .checked_add(2)
                .ok_or_else(|| ParseError("bulk length overflows buffer offset".into()))?;
            if term_end > buf.len() {
                return Ok(None);
            }
            if &buf[payload_end..term_end] != b"\r\n" {
                return Err(ParseError("bulk payload missing CRLF terminator".into()));
            }
            Ok(Some((
                Frame::Bulk(buf[payload_start..payload_end].to_vec()),
                term_end,
            )))
        }
        b'*' => {
            let Some((line_end, next)) = read_line(buf, content_start) else {
                return Ok(None);
            };
            let count = parse_i64(&buf[content_start..line_end])?;
            if count < 0 {
                // `*-1\r\n` (the null array) is not supported; every negative
                // count, including -1, is rejected.
                return Err(ParseError(format!(
                    "negative array length is not supported: {count}"
                )));
            }
            let depth = depth + 1;
            if depth > MAX_ARRAY_DEPTH {
                return Err(ParseError(format!(
                    "array nesting exceeds the depth limit of {MAX_ARRAY_DEPTH}"
                )));
            }
            if count > ceiling(MAX_ARRAY_LEN) {
                return Err(ParseError(format!(
                    "array length {count} exceeds the limit of {MAX_ARRAY_LEN}"
                )));
            }
            // In range by the ceiling above; kept for the same reason as the
            // bulk conversion.
            let count =
                usize::try_from(count).map_err(|_| ParseError("array length too large".into()))?;
            let mut frames = Vec::new();
            let mut cursor = next;
            for _ in 0..count {
                match parse_frame(buf, cursor, depth)? {
                    None => return Ok(None),
                    Some((frame, after_frame)) => {
                        frames.push(frame);
                        cursor = after_frame;
                    }
                }
            }
            Ok(Some((Frame::Array(frames), cursor)))
        }
        other => Err(ParseError(format!("unknown RESP2 type byte: {other:#04x}"))),
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
    parse_frame(buf, 0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_every_frame_type() {
        let cases: &[(Frame, &[u8])] = &[
            (Frame::Simple("OK".into()), b"+OK\r\n"),
            (Frame::Error("ERR boom".into()), b"-ERR boom\r\n"),
            (Frame::Integer(-7), b":-7\r\n"),
            (Frame::Bulk(b"hi".to_vec()), b"$2\r\nhi\r\n"),
            // The empty bulk is the one length-prefixed frame whose payload
            // and terminator are adjacent, so it is exactly where an
            // off-by-one in the encoder would hide. It is also not `Null`:
            // "a value that is zero bytes long" and "no value" are different
            // frames, and the pair is here so nobody collapses them.
            (Frame::Bulk(Vec::new()), b"$0\r\n\r\n"),
            (Frame::Null, b"$-1\r\n"),
            (
                Frame::Array(vec![
                    Frame::Bulk(b"GET".to_vec()),
                    Frame::Bulk(b"k".to_vec()),
                ]),
                b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
            ),
        ];
        for (frame, wire) in cases {
            let mut out = Vec::new();
            encode(frame, &mut out);
            assert_eq!(&out, wire);
        }
    }

    #[test]
    fn parse_round_trips_every_frame_type() {
        let cases: &[Frame] = &[
            Frame::Simple("OK".into()),
            Frame::Error("ERR boom".into()),
            Frame::Integer(-7),
            Frame::Bulk(b"hi".to_vec()),
            Frame::Null,
            Frame::Array(vec![
                Frame::Bulk(b"GET".to_vec()),
                Frame::Bulk(b"k".to_vec()),
            ]),
            Frame::Array(vec![]),
            Frame::Array(vec![Frame::Array(vec![Frame::Integer(1)])]),
        ];
        for frame in cases {
            let mut out = Vec::new();
            encode(frame, &mut out);
            let (parsed, consumed) = parse(&out).unwrap().unwrap();
            assert_eq!(&parsed, frame);
            assert_eq!(consumed, out.len());
        }
    }

    #[test]
    fn bulk_strings_carry_arbitrary_bytes() {
        // The length prefix is what makes a bulk string binary-safe: a payload
        // holding the terminator itself, a NUL and a non-UTF-8 byte must come
        // back byte for byte.
        let frame = Frame::Bulk(b"a\r\nb\x00c\xffd".to_vec());
        let mut out = Vec::new();
        encode(&frame, &mut out);
        let (parsed, consumed) = parse(&out).unwrap().unwrap();
        assert_eq!(parsed, frame);
        assert_eq!(consumed, out.len());
    }

    #[test]
    fn integers_round_trip_at_the_extremes_of_i64() {
        for value in [i64::MIN, i64::MAX] {
            let frame = Frame::Integer(value);
            let mut out = Vec::new();
            encode(&frame, &mut out);
            let (parsed, consumed) = parse(&out).unwrap().unwrap();
            assert_eq!(parsed, frame, "{value}");
            assert_eq!(consumed, out.len(), "{value}");
        }
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "must not contain CR or LF")]
    fn encoding_a_simple_string_holding_a_terminator_trips_the_debug_assertion() {
        let mut out = Vec::new();
        encode(&Frame::Simple("OK\r\n+INJECTED".into()), &mut out);
    }

    #[test]
    fn parse_returns_none_on_partial_input() {
        let mut bulk = Vec::new();
        encode(&Frame::Bulk(b"hello".to_vec()), &mut bulk);

        let mut nested_array = Vec::new();
        encode(
            &Frame::Array(vec![Frame::Array(vec![Frame::Bulk(b"x".to_vec())])]),
            &mut nested_array,
        );

        for out in [&bulk, &nested_array] {
            for cut in 0..out.len() {
                assert_eq!(parse(&out[..cut]).unwrap(), None, "cut at {cut} of {out:?}");
            }
        }
    }

    #[test]
    fn parse_returns_none_on_specific_incomplete_inputs() {
        for buf in [
            &b"$2\r\nhi\r"[..], // a lone trailing `\r`, not yet `\r\n`
            b"*3\r\n",          // an array header with no elements after it
        ] {
            assert_eq!(parse(buf).unwrap(), None, "{buf:?}");
        }
    }

    #[test]
    fn parse_rejects_malformed_input() {
        for bad in [
            &b"$abc\r\n"[..],
            b"!5\r\n",
            b"*1\r\n:x\r\n",
            b"$3\r\nabcd\r\n",
            b"*-1\r\n", // negative array length, including the null array
            b"$99999999999999999999\r\n", // bulk length out of i64 range
            b"$2\r\nhi\n\r", // terminator bytes in the wrong order
        ] {
            assert!(parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn parse_enforces_the_array_depth_limit() {
        // 64 levels of array nesting are accepted...
        let mut frame = Frame::Integer(1);
        for _ in 0..64 {
            frame = Frame::Array(vec![frame]);
        }
        let mut buf = Vec::new();
        encode(&frame, &mut buf);
        let (parsed, consumed) = parse(&buf).unwrap().unwrap();
        assert_eq!(parsed, frame);
        assert_eq!(consumed, buf.len());

        // ...but the 65th level is rejected.
        let too_deep = Frame::Array(vec![frame]);
        let mut buf = Vec::new();
        encode(&too_deep, &mut buf);
        assert!(parse(&buf).is_err());
    }

    #[test]
    fn parse_enforces_the_length_ceilings_at_the_header() {
        // Rejected the moment the length line is read — no payload byte is
        // ever buffered, which is the whole point of the ceiling.
        let over = MAX_BULK_LEN + 1;
        assert!(parse(format!("${over}\r\n").as_bytes()).is_err());
        // The boundary itself is still accepted, and still reports "need
        // more bytes" rather than an error.
        assert_eq!(parse(format!("${MAX_BULK_LEN}\r\n").as_bytes()), Ok(None));

        let over = MAX_ARRAY_LEN + 1;
        assert!(parse(format!("*{over}\r\n").as_bytes()).is_err());
        assert_eq!(parse(format!("*{MAX_ARRAY_LEN}\r\n").as_bytes()), Ok(None));
    }

    #[test]
    fn a_length_beyond_a_32_bit_usize_is_rejected_the_same_way_everywhere() {
        // The regression this pins: these lengths convert to `usize` on a
        // 64-bit target and fail to on a 32-bit one, so before the ceilings
        // the same bytes were a terminal error on one target and an
        // indefinite "wait for more" on the other. Both are errors now, on
        // every target, and the assertion holds wherever the suite runs.
        for header in [&b"$4294967296\r\n"[..], b"*4294967296\r\n"] {
            assert!(parse(header).is_err(), "{header:?}");
        }
    }

    #[test]
    fn parse_error_displays_its_message() {
        let err = parse(b"!5\r\n").unwrap_err();
        assert_eq!(err.to_string(), err.0);
        assert!(err.to_string().contains("unknown RESP2 type byte"));
        // And it is a real `std::error::Error`, so a caller can box it.
        let _boxed: Box<dyn std::error::Error> = Box::new(err);
    }

    #[test]
    fn parse_leaves_trailing_bytes_for_the_next_call() {
        let mut out = Vec::new();
        encode(&Frame::Integer(1), &mut out);
        let split = out.len();
        encode(&Frame::Integer(2), &mut out);
        let (f, used) = parse(&out).unwrap().unwrap();
        assert_eq!((f, used), (Frame::Integer(1), split));
    }
}
