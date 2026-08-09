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
}
