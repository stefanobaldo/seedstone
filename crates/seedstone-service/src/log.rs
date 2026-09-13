//! The one shape every runtime line has: a JSON object with `ts`, `level`
//! and `evt`, then the event's own fields.
//!
//! Severity comes from the record, not from the stream it arrived on.
//! Everything the server writes about itself goes to stderr — stdout is the
//! answer to `--version` and `--help` — and a collector that classifies by
//! stream files every line as an error, a healthy start included. `level`
//! is what lets it stop doing that without being told about this server.
//!
//! The field names are a public interface: `docs/operations.md` promises
//! them, additively — a field or an event may be added in any version,
//! none is removed or renamed without a changelog entry — and
//! `tests/operations_page.rs` holds the page's table and [`EVENTS`]
//! together. That is why a line is rendered from an [`Event`] rather than
//! from names at the call site: a name that is not in the table cannot be
//! written, so the page cannot be wrong about what the server emits.
//!
//! Rendered by hand rather than through a serialisation crate: three
//! common fields and a handful of events do not need one, and the escaping
//! in [`json_escaped`] is the one place a peer-chosen byte string is made
//! safe to put inside a line.

use std::fmt::Write as _;

/// How serious a line is: what a collector's alert selects on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    /// The server did what it was asked; nothing needs attention.
    Info,
    /// Something outside the server is wrong — a client's request, a signal
    /// with nothing to act on — and the server is healthy.
    Warn,
    /// The server could not do what it was asked.
    Error,
}

impl Level {
    /// The lower-case word the line carries.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// One kind of line: its name, its severity and the fields it carries
/// after the envelope, in order.
///
/// The statics below are the whole set; [`EVENTS`] lists them for the
/// page guard. A new event is a new static and a new row in both.
///
/// Statics and not constants: [`EVENTS`] holds a reference to each, and a
/// test asserts that the reference in the table is the same object as the
/// item it names. A `&CONST` is a fresh anonymous allocation per expression
/// site, which the compiler may or may not merge with another, so that
/// identity would hold by luck; a `&'static` to a static is the item's own
/// address, always.
#[derive(Debug)]
pub struct Event {
    /// The value of `evt`.
    pub name: &'static str,
    /// The value of `level`.
    pub level: Level,
    /// The names of the fields after the envelope, in the order rendered.
    pub fields: &'static [&'static str],
}

/// A field's value: text, escaped on the way in, or a number.
#[derive(Debug)]
pub enum Field<'a> {
    /// Text, which may quote bytes a peer chose.
    Str(&'a str),
    /// A number, rendered bare.
    Num(u64),
}

/// The listener is bound and the node is serving.
pub static LISTENING: Event = Event {
    name: "listening",
    level: Level::Info,
    fields: &["version", "bind", "port"],
};

/// The listener could not be bound; the process exits after this line.
pub static BIND_FAILED: Event = Event {
    name: "bind_failed",
    level: Level::Error,
    fields: &["bind", "port", "error"],
};

/// One error reply left for a peer. `warn`, not `error`: the mistake is the
/// request's or the configuration's, and the server is healthy.
pub static ERROR_REPLY: Event = Event {
    name: "error_reply",
    level: Level::Warn,
    fields: &["code", "cmd", "msg"],
};

/// The accept loop is leaving on a signal.
pub static STOPPING: Event = Event {
    name: "stopping",
    level: Level::Info,
    fields: &["signal"],
};

/// Every event the server can write, in the order the page lists them.
pub static EVENTS: &[&Event] = &[&LISTENING, &BIND_FAILED, &ERROR_REPLY, &STOPPING];

/// One line: the envelope, then `event`'s fields with `values` in order.
///
/// `ts` is Unix milliseconds and is the caller's to supply — the connection
/// layer hands in its injected clock so a replay writes the line the original
/// run wrote, and the binary hands in the wall clock.
///
/// # Panics
///
/// When `values.len() != event.fields.len()`: that is a call site that
/// disagrees with the table, which is a defect in this crate and not
/// something a peer can cause.
#[must_use]
pub fn line(ts: u64, event: &Event, values: &[Field<'_>]) -> String {
    assert_eq!(
        values.len(),
        event.fields.len(),
        "event {} takes {} fields, {} given",
        event.name,
        event.fields.len(),
        values.len()
    );
    let mut out = String::with_capacity(96 + values.len() * 24);
    out.push_str("{\"ts\":");
    let _ = write!(out, "{ts}");
    out.push_str(",\"level\":\"");
    out.push_str(event.level.as_str());
    out.push_str("\",\"evt\":\"");
    out.push_str(event.name);
    out.push('"');
    for (name, value) in event.fields.iter().zip(values) {
        out.push_str(",\"");
        out.push_str(name);
        out.push_str("\":");
        match value {
            Field::Str(text) => {
                out.push('"');
                json_escaped(text, &mut out);
                out.push('"');
            }
            Field::Num(n) => {
                let _ = write!(out, "{n}");
            }
        }
    }
    out.push('}');
    out
}

/// Appends `raw` as the body of a JSON string — no surrounding quotes.
///
/// RESP arguments and error texts quote bytes the peer chose, including
/// newlines and control characters. Without this, one argument can forge a
/// whole log line in an aggregator that splits on newlines, which is a
/// correctness problem and not a tidiness one.
pub fn json_escaped(raw: &str, out: &mut String) {
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

#[cfg(test)]
mod tests {
    use super::{
        BIND_FAILED, ERROR_REPLY, EVENTS, Event, Field, LISTENING, Level, STOPPING, json_escaped,
        line,
    };

    #[test]
    fn json_escaped_neutralises_everything_that_could_forge_a_line() {
        let mut out = String::new();
        json_escaped("a\"b\\c\nd\re\tf", &mut out);
        assert_eq!(out, r#"a\"b\\c\nd\re\tf"#);
        out.clear();
        json_escaped("\u{1}", &mut out);
        assert_eq!(out, r"\u0001");
    }

    /// The envelope is the three common fields in this order, then the
    /// event's own fields in the order its table gives them.
    #[test]
    fn a_line_is_the_envelope_then_the_fields_in_table_order() {
        let line = line(
            1_700_000_000_000,
            &LISTENING,
            &[Field::Str("0.1.1"), Field::Str("0.0.0.0"), Field::Num(6379)],
        );
        assert_eq!(
            line,
            r#"{"ts":1700000000000,"level":"info","evt":"listening","version":"0.1.1","bind":"0.0.0.0","port":6379}"#
        );
    }

    #[test]
    fn a_line_never_contains_a_newline() {
        let line = line(
            1,
            &BIND_FAILED,
            &[
                Field::Str("127.0.0.1\n"),
                Field::Num(1),
                Field::Str("boom\r\n\u{1}"),
            ],
        );
        assert_eq!(line.matches('\n').count(), 0, "{line}");
        assert_eq!(line.matches('\r').count(), 0, "{line}");
        assert!(line.contains(r#""error":"boom\r\n\u0001""#), "{line}");
    }

    #[test]
    fn levels_render_lower_case() {
        assert_eq!(Level::Info.as_str(), "info");
        assert_eq!(Level::Warn.as_str(), "warn");
        assert_eq!(Level::Error.as_str(), "error");
    }

    /// The table is the single source: every event in it has a distinct
    /// name, every name is a plain identifier a filter can select on, and no
    /// event reuses an envelope field.
    #[test]
    fn the_event_table_has_distinct_identifier_names() {
        let mut names: Vec<&str> = EVENTS.iter().map(|e| e.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), EVENTS.len(), "a name repeats");
        for event in EVENTS {
            assert!(
                event
                    .name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c == '_'),
                "{}",
                event.name
            );
            for field in event.fields {
                assert!(
                    !["ts", "level", "evt"].contains(field),
                    "{} reuses an envelope field",
                    event.name
                );
            }
        }
        assert!(
            EVENTS
                .iter()
                .any(|e| std::ptr::eq(*e, &raw const ERROR_REPLY))
        );
        assert!(EVENTS.iter().any(|e| std::ptr::eq(*e, &raw const STOPPING)));
    }

    /// A value count that disagrees with the table is a programming error at
    /// the call site, and is caught where it is made.
    #[test]
    #[should_panic(expected = "listening")]
    fn a_value_count_that_disagrees_with_the_table_panics() {
        let _ = line(0, &LISTENING, &[Field::Num(1)]);
    }

    /// The first row of the table is the first line a server writes.
    #[test]
    fn the_table_opens_with_listening() {
        let first: &Event = EVENTS[0];
        assert!(std::ptr::eq(first, &raw const LISTENING));
    }
}
