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
//! - **Bounded buffering.** [`MAX_REQUEST_BYTES`] caps what one connection can
//!   make the server hold, on top of the per-frame ceilings the codec
//!   enforces ([`seedstone_resp::MAX_BULK_LEN`],
//!   [`seedstone_resp::MAX_ARRAY_LEN`]). Without a cap, a peer
//!   that opens a frame and never finishes it is a slow memory leak with a
//!   connection attached.
//! - **No response splitting.** Every error frame this module emits passes
//!   through [`safe_error`] first. A `Frame::Error` is terminated by the first
//!   `\r\n` after its type byte, so text carrying either byte would let a peer
//!   dictate frames the server never meant to send — and the codec's guard
//!   against that is a debug assertion, which is not there in release. This is
//!   the enforcement point that is.

use crate::shard::{Command, NOT_AN_INTEGER, Reply, Router, parse_i64};
use seedstone_resp::{Frame, ParseError, encode, parse};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// How many bytes one connection may hold while a frame is still incomplete.
///
/// The codec bounds a single bulk payload and a single array's element count;
/// this bounds the accumulation of them. It must stay comfortably above the
/// largest command the codec accepts — a `SET` of two
/// [`seedstone_resp::MAX_BULK_LEN`] payloads is about 32 MiB on the wire — or
/// a legitimate command would be refused as oversized.
///
/// # Known cost
///
/// The codec re-parses an incomplete frame from its start on every arriving
/// chunk, so filling this buffer is quadratic in the bytes read. The ceiling
/// bounds it — it cannot run away — but the bound is generous, and a peer
/// that dribbles a 64 MiB frame in makes the server do far more scanning than
/// reading. Closing it properly needs a resumable parser, which is a change
/// to the codec's contract, not to this constant.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

/// Bytes read from the transport per `read` call.
const READ_CHUNK: usize = 16 * 1024;

/// How much of a peer-supplied byte string an error message may quote.
const QUOTE_LIMIT: usize = 32;

/// Serves one connection until the peer disconnects or sends something that
/// can never be a valid frame.
///
/// Complete frames are drained from the read buffer, mapped to commands and
/// dispatched one at a time; each reply is written and **flushed** before the
/// next frame is handled. The flush is not optional: a transport that buffers
/// — a simulated one especially — will otherwise hold a reply the peer is
/// blocked waiting for, and the deadlock only appears once the code is run
/// under the simulator.
///
/// A frame that is well-formed RESP but not a command this server knows —
/// wrong arity, unknown name — is answered with an error frame and the
/// connection stays open, exactly as Redis behaves. A frame that is not
/// well-formed RESP is answered with an error frame and the connection
/// closes: the byte stream is desynchronised at that point and nothing after
/// it can be trusted.
pub async fn serve_connection<S, R>(stream: S, router: R)
where
    S: AsyncRead + AsyncWrite + Unpin,
    R: Router,
{
    serve_connection_limited(stream, router, MAX_REQUEST_BYTES).await
}

/// [`serve_connection`] with the accumulation ceiling as a parameter.
///
/// The ceiling exists so that it can be exercised. Reaching 64 MiB through a
/// pipe costs tens of seconds of pure scanning — the quadratic re-parse this
/// limit bounds — so a test that used the real constant would either be
/// skipped or be the slowest thing in the suite, and this layer's stated
/// primary defence would go on having no coverage at all.
async fn serve_connection_limited<S, R>(mut stream: S, router: R, max_request_bytes: usize)
where
    S: AsyncRead + AsyncWrite + Unpin,
    R: Router,
{
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; READ_CHUNK];
    let mut out: Vec<u8> = Vec::new();

    loop {
        // Drain every complete frame the buffer already holds before asking
        // the transport for more.
        let mut consumed = 0;
        loop {
            let reply = match parse(&buf[consumed..]) {
                Ok(Some((frame, used))) => {
                    consumed += used;
                    match frame_to_command(frame) {
                        Ok(cmd) => reply_to_frame(router.dispatch(cmd).await),
                        Err(message) => safe_error(&message),
                    }
                }
                // A proper prefix of a valid frame: read more.
                Ok(None) => break,
                Err(error) => {
                    // Terminal. Report it and go, without draining: the
                    // stream is desynchronised and there is no resync point.
                    let frame = safe_error(&protocol_error(&error));
                    write_frame(&mut stream, &frame, &mut out).await;
                    return;
                }
            };
            if !write_frame(&mut stream, &reply, &mut out).await {
                return;
            }
        }
        buf.drain(..consumed);

        if buf.len() > max_request_bytes {
            let frame = safe_error(&format!(
                "ERR request exceeds the {max_request_bytes}-byte limit"
            ));
            write_frame(&mut stream, &frame, &mut out).await;
            return;
        }

        match stream.read(&mut chunk).await {
            // EOF, or a transport that failed. Either way the connection is
            // over and there is nobody left to tell.
            Ok(0) | Err(_) => return,
            Ok(got) => buf.extend_from_slice(&chunk[..got]),
        }
    }
}

/// Encodes `frame` and writes it, flushing before returning.
///
/// Returns `false` if the write failed, which means the peer is gone.
async fn write_frame<S>(stream: &mut S, frame: &Frame, out: &mut Vec<u8>) -> bool
where
    S: AsyncWrite + Unpin,
{
    out.clear();
    encode(frame, out);
    stream.write_all(out).await.is_ok() && stream.flush().await.is_ok()
}

/// Translates a shard's [`Reply`] into the frame that carries it.
fn reply_to_frame(reply: Reply) -> Frame {
    match reply {
        Reply::Ok => Frame::Simple("OK".into()),
        Reply::Bulk(None) => Frame::Null,
        Reply::Bulk(Some(value)) => Frame::Bulk(value),
        Reply::Removed(removed) => Frame::Integer(i64::from(removed)),
        Reply::Integer(n) => Frame::Integer(n),
        // A router's error text is server-authored, but this layer does not
        // get to assume that about every router that will ever exist.
        Reply::Error(message) => safe_error(&message),
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
    let mut rendered = String::new();
    for &byte in bytes.iter().take(QUOTE_LIMIT) {
        match byte {
            b'\\' => rendered.push_str("\\\\"),
            b'\'' => rendered.push_str("\\'"),
            0x20..=0x7e => rendered.push(byte as char),
            other => rendered.push_str(&format!("\\x{other:02x}")),
        }
    }
    if bytes.len() > QUOTE_LIMIT {
        rendered.push_str("...");
    }
    rendered
}

/// Maps a request frame to the command it names.
///
/// `Err` carries the text of the error frame to send back; the connection
/// survives it. Command names are matched case-insensitively, as Redis does.
fn frame_to_command(frame: Frame) -> Result<Command, String> {
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

    match upper.as_slice() {
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
            [key, delta] => match parse_i64(delta) {
                Some(delta) => Ok(Command::IncrBy {
                    key: key.clone(),
                    delta,
                }),
                None => Err(NOT_AN_INTEGER.into()),
            },
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
    use seedstone_resp::{MAX_ARRAY_LEN, MAX_BULK_LEN};
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

    /// The same attack from the other side, where `safe_error` is the only
    /// defence.
    ///
    /// Nothing sanitises a `Reply::Error` on its way out of a router — no
    /// `quote` runs on this path — so if `serve_connection` passed the text
    /// straight into `Frame::Error`, the router would be dictating frames.
    /// Delete `safe_error` from `reply_to_frame` and this test fails; that is
    /// what makes it worth having.
    #[tokio::test]
    async fn a_router_error_cannot_inject_frames_either() {
        #[derive(Clone)]
        struct SplittingRouter;

        impl Router for SplittingRouter {
            async fn dispatch(&self, _cmd: Command) -> Reply {
                Reply::Error("ERR boom\r\n+INJECTED".into())
            }
        }

        let (client, server) = tokio::io::duplex(4096);
        tokio::spawn(serve_connection(server, SplittingRouter));
        let (mut r, mut w) = tokio::io::split(client);

        let mut out = Vec::new();
        encode(&req(&["GET", "a"]), &mut out);
        encode(&req(&["GET", "b"]), &mut out);
        w.write_all(&out).await.unwrap();
        w.flush().await.unwrap();

        // Two commands, so two frames. Were the reply split, the second frame
        // read here would be the injected `+INJECTED` rather than the reply
        // to the second command.
        let frames = read_frames(&mut r, 2).await;
        for frame in &frames {
            let Frame::Error(text) = frame else {
                panic!("expected an error frame, got {frame:?}");
            };
            assert!(
                !text.contains('\r') && !text.contains('\n'),
                "error text still carries a terminator: {text:?}"
            );
            assert!(
                text.contains("INJECTED"),
                "the terminator is neutralised, not the message dropped: {text:?}"
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
    }

    #[tokio::test]
    async fn a_frame_that_never_ends_is_cut_off_at_the_ceiling() {
        // The assertion above relates two constants; it never runs the code
        // that enforces either. This does: a peer opens a bulk string and
        // keeps feeding bytes without ever terminating it — the slow memory
        // leak with a connection attached that the module doc names — and the
        // server must answer and close rather than buffer forever.
        const CEILING: usize = 64 * 1024;

        let pool = ShardPool::spawn(4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let (client, server) = tokio::io::duplex(8 * 1024);
        let task = tokio::spawn(serve_connection_limited(server, pool, CEILING));
        let (mut r, mut w) = tokio::io::split(client);

        let writer = tokio::spawn(async move {
            // A well-formed, never-satisfied length prefix: every byte after
            // it is a legal continuation, so the codec can only ask for more.
            w.write_all(b"$1000000000\r\n").await?;
            loop {
                w.write_all(&[b'x'; 4096]).await?;
            }
            #[allow(unreachable_code)]
            std::io::Result::Ok(())
        });

        let frames = read_frames(&mut r, 1).await;
        let Frame::Error(text) = &frames[0] else {
            panic!("expected an error frame, got {:?}", frames[0]);
        };
        assert!(text.contains("exceeds"), "unexpected refusal: {text}");

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
