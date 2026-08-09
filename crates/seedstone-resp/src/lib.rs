//! SeedStone RESP2 codec: no external dependencies.

/// A RESP2 frame — the wire protocol unit for Redis Serialization Protocol version 2.
///
/// Encodes to its RESP2 representation via [`encode`].
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// Simple string: `+OK\r\n`
    Simple(String),
    /// Error: `-ERR boom\r\n`
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
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Frame::Error(e) => {
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

/// The maximum number of nested [`Frame::Array`] levels `parse` accepts.
///
/// A top-level array is nesting level 1. Levels 1 through 64 are accepted;
/// an array that would be the 65th level of nesting is rejected with a
/// [`ParseError`] rather than recursed into.
const MAX_ARRAY_DEPTH: usize = 64;

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
    fn parse_leaves_trailing_bytes_for_the_next_call() {
        let mut out = Vec::new();
        encode(&Frame::Integer(1), &mut out);
        let split = out.len();
        encode(&Frame::Integer(2), &mut out);
        let (f, used) = parse(&out).unwrap().unwrap();
        assert_eq!((f, used), (Frame::Integer(1), split));
    }
}
