//! The container commands — `COMMAND`, `CLIENT`, `CONFIG`, `SLOWLOG`,
//! `LATENCY` — each a subcommand dispatcher answered at the edge. What each
//! answers, and how it differs from Redis, is on `docs/compatibility.md`.

use crate::dispatch::{Action, COMMANDS};
use crate::node::NodeInfo;
use crate::options::wrong_arity;
use crate::reply::quote;
use seedstone_core::glob;
use seedstone_core::memory::{EvictionMode, MemoryLimit};
use seedstone_core::shard::{ReplyError, parse_i64};
use seedstone_resp::Frame;

/// Answers `COMMAND` and the subcommands a client sends before it sends
/// anything a user asked for.
///
/// `COMMAND DOCS` is what redis-cli sends the moment it connects, and it reads
/// the reply before it prints a prompt — so an error there is not an
/// unfriendly message, it is a session that never starts. Answering with
/// nothing costs the user their command hints and nothing else.
pub fn command(sub: &[u8], rest: &[Vec<u8>]) -> Result<Action, String> {
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
pub fn client(sub: &[u8], rest: &[Vec<u8>]) -> Result<Action, String> {
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

/// The parameters `CONFIG GET` will answer for, in the order it reports them.
///
/// A fixed table and not a configuration store: this server has no `CONFIG
/// SET`, so every value below is read from the node that is already running
/// rather than from something an operator could have written and this could
/// have forgotten. The list is the one an exporter and an operator's `redis-cli
/// CONFIG GET maxmemory` reach for — the memory ceiling and its policy, the
/// connection ceiling, and the handful of fields a client library checks on
/// connect and would otherwise treat as a node it cannot talk to.
///
/// Four of them describe a feature this server does not have, and each is
/// answered with the value Redis reports when the feature is switched off:
/// `databases` is `1` because there is one keyspace, `requirepass` is empty
/// because Redis never reports a password, and the two latency fields are the
/// disabled readings. See [`config_value`].
pub const CONFIG_PARAMETERS: [&str; 9] = [
    "maxmemory",
    "maxmemory-policy",
    "maxclients",
    "databases",
    "requirepass",
    "slowlog-log-slower-than",
    "latency-monitor-threshold",
    "bind",
    "port",
];

/// Answers `CONFIG GET`, and refuses every other subcommand.
///
/// Redis takes any number of globs and answers the union of what they select,
/// so the table is walked once and each row offered to all of them — which is
/// also what keeps the reply in table order rather than in the order the peer
/// happened to ask.
///
/// **A parameter name is matched without regard to case**, as Redis matches
/// one: its `configGetCommand` calls the glob matcher in the case-folding mode
/// its `KEYS` does not use, so `CONFIG GET MAXMEMORY` answers there. Measured
/// against Redis 8.10.0. The folding is done to the pattern here rather than
/// inside [`glob`], which is the matcher `KEYS` shares: parameter names are
/// ASCII and this table's are lower case, so folding the pattern is the whole
/// of the difference — and keys are bytes, where a fold would be a defect.
///
/// There is no `CONFIG SET`: every parameter below is a fact about a node that
/// is already running, and accepting a new ceiling at runtime would mean
/// moving a keyspace under one. Refusing it as an unknown subcommand is the
/// honest answer, and it is the one Redis gives for a subcommand it lacks.
pub fn config(sub: &[u8], globs: &[Vec<u8>], node: &NodeInfo) -> Result<Action, String> {
    if !sub.eq_ignore_ascii_case(b"GET") {
        return Err(unknown_subcommand("CONFIG", sub));
    }
    if globs.is_empty() {
        return Err(wrong_arity("config|get"));
    }
    let mut reply = Vec::new();
    // Folded once, not once per row: the peer sends a handful of patterns and
    // the table is walked whole for each of them.
    let folded: Vec<Vec<u8>> = globs.iter().map(|glob| glob.to_ascii_lowercase()).collect();
    for name in CONFIG_PARAMETERS {
        if folded
            .iter()
            .any(|pattern| glob::matches(pattern, name.as_bytes()))
        {
            reply.push(Frame::Bulk(name.as_bytes().to_vec()));
            reply.push(Frame::Bulk(config_value(name, node).into_bytes()));
        }
    }
    Ok(Action::Reply(Frame::Array(reply)))
}

/// The memory ceiling and the eviction policy as both `INFO` and `CONFIG GET`
/// report them.
///
/// Redis reports an absent ceiling as `0`, and reports `noeviction` as the
/// policy whenever there is no ceiling to reach — whatever the configuration
/// nominally holds. Both are matched here rather than improved on: an
/// operator's tooling reads these two fields together and already knows what
/// that pair means.
///
/// One function and not the rule written twice, because the two commands are
/// read by the same tooling: a `maxmemory` from `INFO` that disagreed with the
/// one from `CONFIG GET` would be read as a node that had changed underneath.
pub const fn reported_limit(limit: MemoryLimit) -> (u64, EvictionMode) {
    match limit.ceiling {
        Some(ceiling) => (ceiling, limit.mode),
        None => (0, EvictionMode::NoEviction),
    }
}

/// What one [`CONFIG_PARAMETERS`] entry reports.
///
/// `requirepass` is empty on a node that has a password and on one that does
/// not. That is Redis's behaviour and it is the only safe one: the reply goes
/// to whoever asked, and the field would otherwise put the secret on the wire
/// on every scrape.
pub fn config_value(name: &str, node: &NodeInfo) -> String {
    let (ceiling, policy) = reported_limit(node.limit);
    match name {
        "maxmemory" => ceiling.to_string(),
        "maxmemory-policy" => policy.name().to_owned(),
        "maxclients" => node.max_clients.to_string(),
        // One keyspace, so `SELECT 1` is out of range and a client that reads
        // this field learns why before it tries.
        "databases" => "1".to_owned(),
        "requirepass" => String::new(),
        // The two readings Redis gives when the feature is off: the slow log
        // records nothing at a negative threshold, and the latency monitor is
        // disabled at zero. Neither is sampled here.
        "slowlog-log-slower-than" => "-1".to_owned(),
        "latency-monitor-threshold" => "0".to_owned(),
        "bind" => node.bind.clone(),
        "port" => node.tcp_port.to_string(),
        other => unreachable!("{other} is in CONFIG_PARAMETERS with no value beside it"),
    }
}

/// Answers `SLOWLOG` on a node whose slow log is empty and stays that way.
///
/// Nothing here times a handler, so there is no threshold at which a command
/// would be recorded — which is what `CONFIG GET slowlog-log-slower-than`
/// reports as `-1`. The alternative to answering is refusing, and a refusal
/// carries a different fact: that the command does not exist. An exporter
/// reads the first as the node's answer and the second as a failed scrape.
pub fn slowlog(sub: &[u8], rest: &[Vec<u8>]) -> Result<Action, String> {
    if sub.eq_ignore_ascii_case(b"GET") {
        match rest {
            [] => {}
            // Redis reads the count before it reads the log and refuses a
            // non-integer whatever the log holds. What the count then selects
            // is a prefix of nothing, so it is parsed and dropped.
            [count] => {
                parse_i64(count).ok_or_else(|| ReplyError::NotAnInteger.wire_text().to_owned())?;
            }
            _ => return Err(wrong_arity("slowlog|get")),
        }
        return Ok(Action::Reply(Frame::Array(Vec::new())));
    }
    if sub.eq_ignore_ascii_case(b"LEN") {
        if !rest.is_empty() {
            return Err(wrong_arity("slowlog|len"));
        }
        return Ok(Action::Reply(Frame::Integer(0)));
    }
    if sub.eq_ignore_ascii_case(b"RESET") {
        if !rest.is_empty() {
            return Err(wrong_arity("slowlog|reset"));
        }
        // Nothing to clear, and `OK` is what Redis answers to clearing a log
        // that was already empty: a runbook step that succeeds rather than
        // one with an exception written beside it.
        return Ok(Action::Reply(Frame::Simple("OK".into())));
    }
    Err(unknown_subcommand("SLOWLOG", sub))
}

/// Answers `LATENCY` on a node whose latency monitor is off.
///
/// `CONFIG GET latency-monitor-threshold` reports `0`, Redis's spelling for
/// disabled, and these are the readings that belong beside it: nothing was
/// sampled, so there is no latest reading, no history for any event, no event
/// a reset can remove, and no command with a histogram of its own. The
/// argument for answering rather than refusing is [`slowlog`]'s.
pub fn latency(sub: &[u8], rest: &[Vec<u8>]) -> Result<Action, String> {
    if sub.eq_ignore_ascii_case(b"LATEST") {
        if !rest.is_empty() {
            return Err(wrong_arity("latency|latest"));
        }
        return Ok(Action::Reply(Frame::Array(Vec::new())));
    }
    if sub.eq_ignore_ascii_case(b"HISTORY") {
        // One event name, as Redis takes it — the reply is that event's
        // samples and there is nowhere in it for a second event's.
        if rest.len() != 1 {
            return Err(wrong_arity("latency|history"));
        }
        return Ok(Action::Reply(Frame::Array(Vec::new())));
    }
    if sub.eq_ignore_ascii_case(b"RESET") {
        // Any number of event names, and the reply is how many events were
        // cleared: none, whichever were named.
        return Ok(Action::Reply(Frame::Integer(0)));
    }
    // `HISTOGRAM` is answered because it is scraped, not because there is a
    // histogram to report: a stock Prometheus exporter asks for it beside
    // `LATENCY LATEST` on every pass, and a refusal would be an error line
    // per scrape for a command whose honest answer is already known. Redis
    // answers a map keyed by command name and omits every command it has not
    // tracked; with none tracked the map is empty, which is an empty array on
    // this protocol.
    if sub.eq_ignore_ascii_case(b"HISTOGRAM") {
        return Ok(Action::Reply(Frame::Array(Vec::new())));
    }
    Err(unknown_subcommand("LATENCY", sub))
}

/// The unknown-subcommand message: Redis's shape, and not Redis's bytes.
///
/// Redis writes `Unknown subcommand or wrong number of arguments for '<sub>'.
/// Try <CMD> HELP.`; this says only the first of those two things, and says it
/// in lower case. What it keeps is the part a client acts on and prints back
/// to its user: the `ERR` prefix, the quoted subcommand, the full stop, and
/// the pointer at the help text. The exact text is pinned by the tests of
/// every command that reaches here.
///
/// `container` is the containing command's own name, a literal from
/// [`COMMANDS`]; the subcommand is peer-supplied, so it is quoted rather than
/// echoed.
pub fn unknown_subcommand(container: &str, sub: &[u8]) -> String {
    format!(
        "ERR unknown subcommand '{}'. Try {container} HELP.",
        quote(sub)
    )
}
