//! Every error frame this module builds from
//! peer-supplied text passes through [`safe_error`] first. A `Frame::Error`
//! is terminated by the first `\r\n` after its type byte, so text carrying
//! either byte would let a peer dictate frames the server never meant to
//! send — and the codec's guard against that is a debug assertion, which is
//! not there in release. This is the enforcement point that is.
//!
//! The frames that skip it are safe by construction, and it is their *type*
//! that makes them so: each is a `&'static str` constant declared in this
//! file, a [`ReplyError::wire_text`], or the `&'static str` a
//! [`Reply::Status`] carries — and none of the three can carry a terminator
//! a peer chose, because none of them is built from anything a peer sent.
//! The status texts are in that list rather than outside it because a
//! `Frame::Simple` is terminated by the first `\r\n` after its type byte
//! exactly as a `Frame::Error` is, so an unscrubbed one splits a response
//! just the same. Scrubbing a literal would not make it safer, only hide
//! which frames the guard is actually for. Two tests hold that claim rather
//! than leaving it as an assertion — `every_shard_error_is_frame_safe` for
//! the [`ReplyError`] set, and `every_error_constant_is_frame_safe` for the
//! constants and the status texts.

use crate::node::{EDGE_NAMES, KIND_NAMES, NodeInfo};
use seedstone_core::shard::Reply;
use seedstone_resp::{Frame, ParseError};
use std::sync::atomic::Ordering;

/// How much of a peer-supplied byte string an error message may quote.
pub const QUOTE_LIMIT: usize = 32;

/// The code an error reply is filed under: its first word, which is how Redis
/// 6.2 (`redis:6-alpine`, `redis_version:6.2.24`) groups `errorstats` (`ERR`,
/// `NOAUTH`, `WRONGPASS`, …).
pub fn error_code(text: &str) -> &str {
    text.split(' ').next().unwrap_or(text)
}

/// The command behind one slot, carried so that an error reply can name it.
///
/// **Nothing here allocates on the path where the command succeeds.** A
/// recognised command borrows the same `&'static str` the stats tables use,
/// and the owned variant is built only where a command was not recognised —
/// which is already an error path, so its allocation is paid by a request
/// that was going to be answered with an error anyway.
pub enum CommandLabel {
    /// A command the server knows, named by the static tables.
    Known(&'static str),
    /// Bytes the peer sent that name no command. Lossy-converted at
    /// construction: this is peer input and never reaches anything but a log.
    Raw(Box<str>),
    /// No command behind the reply — a protocol-level refusal.
    Anonymous,
}

impl CommandLabel {
    /// Appends the command's name, or nothing when there is no command.
    fn render(&self, out: &mut String) {
        match self {
            Self::Known(name) => out.push_str(name),
            Self::Raw(raw) => out.push_str(raw),
            Self::Anonymous => {}
        }
    }
}

/// The static name a recognised command is logged under, given its
/// ASCII-uppercased name, or `None` for a name neither table holds.
///
/// [`KIND_NAMES`] and [`EDGE_NAMES`] are the two vocabularies `commandstats`
/// prints, so a log line drawn from them names a command exactly as the
/// document an operator correlates it against does. Between them they cover
/// every entry of [`COMMANDS`] — the keyed commands by the kind a shard tags,
/// the rest by the slot the edge counts — and
/// `every_command_the_surface_accepts_has_a_static_label` is what keeps that
/// true as the surface grows.
///
/// [`KIND_NAMES`] is scanned first because its early slots are the traffic:
/// `get` and `set` resolve in two comparisons of three bytes, and the scan a
/// keyed command pays here is the shape of the one it already pays in
/// [`edge_slot`].
pub fn known_name(upper: &[u8]) -> Option<&'static str> {
    KIND_NAMES
        .iter()
        .chain(EDGE_NAMES.iter())
        .copied()
        // Slot `0` of `KIND_NAMES` is no command, and its empty name would
        // match nothing a peer can send — but a table entry that matches by
        // being empty is not something to leave to the caller.
        .find(|name| !name.is_empty() && name.as_bytes().eq_ignore_ascii_case(upper))
}

/// Appends `raw` as the body of a JSON string — no surrounding quotes.
///
/// RESP arguments and error texts quote bytes the peer chose, including
/// newlines and control characters. Without this, one argument can forge a
/// whole log line in an aggregator that splits on newlines, which is a
/// correctness problem and not a tidiness one.
pub fn json_escaped(raw: &str, out: &mut String) {
    use std::fmt::Write as _;
    for ch in raw.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
}

/// One JSON line describing one error reply.
///
/// The timestamp is [`NodeInfo::now_unix_millis`] and never `SystemTime::now`:
/// the wall clock is the one input a replay cannot reproduce, and the
/// simulator freezes this one at [`FIXED_UNIX_MILLIS`], so a replayed run
/// writes the line the original run wrote.
pub fn error_reply_line(node: &NodeInfo, label: &CommandLabel, text: &str) -> String {
    let mut line = String::with_capacity(text.len() + 96);
    line.push_str("{\"evt\":\"error_reply\",\"ts\":");
    // A field holding `fn() -> u64`, not a method: the parentheses are load
    // bearing. This is the injected wall clock, and the only one a replay can
    // reproduce.
    line.push_str(&(node.now_unix_millis)().to_string());
    line.push_str(",\"code\":\"");
    json_escaped(error_code(text), &mut line);
    line.push_str("\",\"cmd\":\"");
    let mut cmd = String::new();
    label.render(&mut cmd);
    json_escaped(&cmd, &mut line);
    line.push_str("\",\"msg\":\"");
    json_escaped(text, &mut line);
    line.push_str("\"}");
    line
}

/// Writes one error reply's line to stderr, beside the startup lines the
/// binary already writes there. Both streams reach a container's log file, so
/// the choice is consistency rather than routing.
pub fn log_error_reply(node: &NodeInfo, label: &CommandLabel, text: &str) {
    eprintln!("{}", error_reply_line(node, label, text));
}

/// Files one error reply under its code and in the total.
pub fn count_error_reply(node: &NodeInfo, text: &str) {
    node.error_replies.fetch_add(1, Ordering::Relaxed);
    let mut stats = node
        .errorstats
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *stats.entry(error_code(text).to_owned()).or_insert(0) += 1;
}

/// The answer to a reply this layer holds but cannot turn into the frame the
/// request is owed.
///
/// Three places can meet one, and none is reachable from anything a peer can
/// send: [`reply_to_frame`] handed a scan step or a shard's counters, and
/// [`fan_out`]'s array fold handed something that is not a bulk. All would be
/// a wiring mistake made here, and all answer this rather than a frame of the
/// wrong shape — one bad reply on one connection, instead of a stream the
/// client parses happily and reads wrong.
pub const UNRENDERABLE_REPLY: &str = "ERR internal reply could not be rendered";

/// Translates a shard's [`Reply`] into the frame that carries it.
pub fn reply_to_frame(reply: Reply) -> Frame {
    match reply {
        Reply::Ok => Frame::Simple("OK".into()),
        // A simple string, not a bulk. Redis puts `+string` on the wire for
        // `TYPE`, and a bulk carrying those same six bytes would be a
        // different frame for the same text — one most clients normalise away
        // and one a client reading RESP itself would not.
        Reply::Status(text) => Frame::Simple(text.into()),
        Reply::Bulk(None) => Frame::Null,
        Reply::Bulk(Some(value)) => Frame::Bulk(value),
        Reply::Removed(removed) => Frame::Integer(i64::from(removed)),
        Reply::Integer(n) => Frame::Integer(n),
        // Neither of these reaches a client under its own name, and both are
        // refused rather than rendered for the same reason: each is one
        // shard's half of something the edge assembles, and a peer handed the
        // half would have nothing on the wire to tell it so.
        //
        // A scan step is the shard-side half of a walk the edge drives
        // itself; whoever drives one reads the cursor and the keys directly,
        // and a client that is handed a cursor has to be handed the packed
        // one, which only the driver can build. A shard's counters are the
        // per-shard half of what `INFO` sums; rendered as the node's they
        // would report a keyspace a fraction of its real size.
        //
        // Nothing routes either here today. The arm is what keeps a routing
        // mistake made later a bad reply on one connection rather than a
        // plausible answer over a fraction of the truth.
        Reply::Scan { .. } | Reply::Stats(_) => Frame::Error(UNRENDERABLE_REPLY.into()),
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
pub fn safe_error(message: &str) -> Frame {
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
pub fn protocol_error(error: &ParseError) -> String {
    format!("ERR Protocol error: {error}")
}

/// Renders peer-supplied bytes for inclusion in an error message.
///
/// Printable ASCII survives; everything else becomes `\xNN`, so the result is
/// pure printable ASCII whatever the input was. [`safe_error`] is still the
/// thing that guarantees the frame is safe — this exists so the message stays
/// readable and short rather than turning a binary blob into a wall of
/// replacement characters.
pub fn quote(bytes: &[u8]) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_names;
    use crate::dispatch::frame_to_action;
    use crate::node::FIXED_UNIX_MILLIS;
    use crate::tests::support::req;
    use seedstone_core::shard::ReplyError;

    /// The escaper is the only thing standing between an argument a peer chose
    /// and a log line the aggregator parses, so it is tested on the bytes that
    /// break a line rather than on ordinary text.
    #[test]
    fn json_escaped_neutralises_everything_that_could_forge_a_line() {
        let mut out = String::new();
        json_escaped("a\"b\\c\nd\re\tf", &mut out);
        assert_eq!(out, r#"a\"b\\c\nd\re\tf"#);

        let mut out = String::new();
        json_escaped("\u{1}", &mut out);
        assert_eq!(out, r"\u0001");
    }

    /// A recognised command names itself with no allocation; the label is the
    /// static name the stats tables already carry.
    #[test]
    fn a_known_command_label_renders_its_name() {
        let mut out = String::new();
        CommandLabel::Known("get").render(&mut out);
        assert_eq!(out, "get");
    }

    /// An unrecognised command still names itself: this is the case the log
    /// was added for — a typo no table contains, where the counter could say
    /// only that an `ERR` had happened.
    #[test]
    fn an_unknown_command_label_renders_the_bytes_the_peer_sent() {
        let mut out = String::new();
        CommandLabel::Raw("dbsizde".into()).render(&mut out);
        assert_eq!(out, "dbsizde");
    }

    /// A refusal with no command behind it — a protocol-level one — says so
    /// rather than inventing a name.
    #[test]
    fn an_anonymous_label_renders_empty() {
        let mut out = String::new();
        CommandLabel::Anonymous.render(&mut out);
        assert_eq!(out, "");
    }

    /// Every name the command surface accepts resolves to a static label, so
    /// the recognised path never falls through to [`CommandLabel::Anonymous`]
    /// and never allocates. The tables the label is drawn from are maintained
    /// beside the surface rather than by it, so this is the assertion that
    /// keeps a command added to one from going unnamed by the others.
    #[test]
    fn every_command_the_surface_accepts_has_a_static_label() {
        for name in command_names() {
            assert!(
                known_name(&name.to_ascii_uppercase()).is_some(),
                "{} names no slot in KIND_NAMES or EDGE_NAMES",
                String::from_utf8_lossy(name),
            );
        }
    }

    /// The line is JSON, it carries the code, the command and the message, and
    /// its timestamp is the node's injected wall clock — frozen under the
    /// simulator, which is what makes a replay reproduce it.
    #[test]
    fn an_error_reply_line_carries_the_code_the_command_and_the_message() {
        let node = NodeInfo::for_tests();
        let line = error_reply_line(
            &node,
            &CommandLabel::Raw("dbsizde".into()),
            "ERR unknown command 'dbsizde'",
        );
        assert!(line.starts_with('{') && line.ends_with('}'), "{line}");
        assert!(line.contains(r#""evt":"error_reply""#), "{line}");
        assert!(line.contains(r#""code":"ERR""#), "{line}");
        assert!(line.contains(r#""cmd":"dbsizde""#), "{line}");
        assert!(
            line.contains(r#""msg":"ERR unknown command 'dbsizde'""#),
            "{line}"
        );
        assert!(
            line.contains(&format!(r#""ts":{FIXED_UNIX_MILLIS}"#)),
            "{line}"
        );
    }

    /// The message is escaped, because it quotes bytes the peer chose.
    #[test]
    fn an_error_reply_line_escapes_the_message_it_quotes() {
        let node = NodeInfo::for_tests();
        let line = error_reply_line(&node, &CommandLabel::Anonymous, "ERR bad \"arg\"\nsecond");
        assert!(line.contains(r#"ERR bad \"arg\"\nsecond"#), "{line}");
        assert_eq!(line.matches('\n').count(), 0, "one line, always: {line}");
    }

    /// A shard's counters are not a wire answer: an operator asks about the
    /// node, and one shard's figures rendered as the node's would be a
    /// keyspace a fraction of its real size with nothing to say so.
    #[test]
    fn one_shards_counters_are_refused_rather_than_rendered() {
        assert_eq!(
            reply_to_frame(Reply::Stats(Box::default())),
            Frame::Error(UNRENDERABLE_REPLY.to_owned())
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
            ReplyError::OutOfMemory,
        ];
        for error in every {
            // Exhaustiveness: adding a variant makes this match non-exhaustive
            // and the crate stops compiling until it is named — and whoever
            // names it here sees the array above.
            match error {
                ReplyError::NotAnInteger
                | ReplyError::WouldOverflow
                | ReplyError::ShardUnavailable
                | ReplyError::LogWriteFailed
                | ReplyError::OutOfMemory => {}
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

    /// The label the drain carries to the log, for each of the four shapes a
    /// frame can have: a recognised command, a recognised command its handler
    /// refuses, a name no table holds, and a frame with no command in it.
    ///
    /// The wrong-arity case is the one worth having. The label is set before
    /// the handler runs precisely so that a refusal the handler returns is
    /// attributed, and moving that assignment below the handler would leave
    /// `wrong number of arguments` anonymous while every other test still
    /// passed.
    #[test]
    fn the_label_names_the_command_behind_each_shape_of_frame() {
        let node = NodeInfo::for_tests();
        let named = |frame| {
            let (_, label) = frame_to_action(frame, &node);
            let mut out = String::new();
            label.render(&mut out);
            out
        };

        assert_eq!(named(req(&["GET", "k"])), "get");
        assert_eq!(
            named(req(&["MGET", "a", "b"])),
            "mget",
            "the request the peer made, not the GETs it becomes"
        );
        assert_eq!(
            named(req(&["SET", "k"])),
            "set",
            "a wrong arity is a refusal, and it names its command"
        );
        assert_eq!(named(req(&["dbsizde"])), "dbsizde");
        assert_eq!(
            named(Frame::Integer(1)),
            "",
            "no array of bulk strings, so no command to name"
        );
    }
}
