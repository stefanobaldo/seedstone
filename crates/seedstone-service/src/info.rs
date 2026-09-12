//! `INFO`: the sections, rendered from the node's counters and the shards'
//! gathered statistics, in Redis's field order where a field exists in both.
//! Absent families are listed on `docs/compatibility.md`.

use crate::EDGE_NAMES;
use crate::containers::reported_limit;
use crate::node::{KIND_NAMES, NodeInfo, SERVER_MODE};
use seedstone_core::shard::{Command, Reply, Router, ShardStats};
use std::sync::atomic::Ordering;

/// The `INFO` arguments that name no section but every section there is.
///
/// Redis takes both where a section name goes and answers each with a whole
/// document rather than one section, and `all` is the spelling operators and
/// metrics exporters reach for — treating it as an ordinary section name would
/// answer the most common form of the command with nothing at all.
///
/// `default` is deliberately not one of them; it names the smaller document,
/// and what it leaves out is [`INFO_OUTSIDE_DEFAULT`].
///
/// Measured against Redis 8.10.0: each answers a document of the same order of
/// size as the unargumented `INFO`, and one of them anywhere in the argument
/// list covers the section names beside it — `INFO all nosuch` is the whole
/// document while `INFO nosuch server` is the server section alone.
pub const INFO_EVERY_SECTION: [&[u8]; 2] = [b"all", b"everything"];

/// The sections this server prints that Redis's `default` document leaves out.
///
/// Redis serves `# Commandstats` and `# Latencystats` for `all` and
/// `everything` and omits both from `default` — and from an argumentless
/// `INFO`, which is `default` under another spelling. This server prints one
/// of the two, so the list has one entry.
///
/// **A section added outside Redis's default set belongs here.** That
/// obligation used to be a sentence in a comment, and the sentence was written
/// while the two sections this server printed were both inside the set,
/// leaving the distinction nothing to select. `# Commandstats` was then added
/// and the sentence did not survive it. A named list is what the next such
/// section has to be added to, rather than a claim that has to be reread.
///
/// `# Errorstats` was checked against `redis:6-alpine`
/// (`redis_version:6.2.24`) when this server started printing it: an
/// argumentless `INFO` there carries the section, so it is inside the default
/// set and does not belong in this list.
pub const INFO_OUTSIDE_DEFAULT: [&[u8]; 1] = [b"commandstats"];

/// Renders the `INFO` sections a peer asked for, or all of them if it named
/// none.
///
/// A section name nobody recognises contributes nothing rather than being an
/// error — `INFO nosuch` is an empty bulk, which is how Redis answers one and
/// leaves the client to notice that the field it wanted is absent. The names
/// in [`INFO_EVERY_SECTION`] are the exception: they ask for no section in
/// particular and get all of them.
///
/// **The argument list is a union, not a filter.** `all` and `everything` add
/// every section, `default` — and an empty list, which Redis reads as
/// `default` — adds the sections in [Redis's default
/// set](INFO_OUTSIDE_DEFAULT), and a section named outright adds itself
/// whatever else stands beside it. So `INFO default commandstats` carries both
/// halves, and a whole-document name beats the section names beside it by
/// covering them rather than by discarding them.
///
/// **The field names are Redis's own, `redis_` prefix and all.** `INFO`'s
/// contract is its field names: everything that reads this output looks up
/// `redis_version` or `redis_mode` by that exact spelling, so renaming them
/// after this server would produce a document that is honest and unreadable.
/// What the server calls itself is `HELLO`'s answer, which has a field for it.
///
/// Only what this node can state truthfully is printed. A field invented to
/// fill out the section is a number some dashboard will plot.
pub async fn info<R: Router>(router: &R, node: &NodeInfo, wanted: &[Vec<u8>]) -> String {
    use std::fmt::Write as _;

    let named = |want: &[u8]| wanted.iter().any(|name| name.eq_ignore_ascii_case(want));
    let every_section = INFO_EVERY_SECTION.iter().any(|whole| named(whole));
    // An empty argument list is Redis's `default`, not its `all`.
    let default_set = wanted.is_empty() || named(b"default");
    let asked_for = |section: &[u8]| {
        every_section || named(section) || (default_set && !INFO_OUTSIDE_DEFAULT.contains(&section))
    };
    // Writing into a `String` cannot fail, so discarding the results is the
    // whole of what there is to do with them.
    let mut text = String::new();
    if asked_for(b"server") {
        let _ = write!(
            text,
            "# Server\r\n\
             redis_version:{}\r\n\
             redis_mode:{SERVER_MODE}\r\n\
             process_id:{}\r\n\
             run_id:{}\r\n\
             tcp_port:{}\r\n\
             uptime_in_seconds:{}\r\n\
             executable:{}\r\n\r\n",
            node.version,
            node.process_id,
            node.run_id,
            node.tcp_port,
            // Whole seconds on the monotonic clock, and saturating at zero:
            // subtracting instants this way cannot go negative however the
            // clock behaved.
            node.started.elapsed().as_secs(),
            node.executable,
        );
    }
    if asked_for(b"clients") {
        let _ = write!(
            text,
            "# Clients\r\nconnected_clients:{}\r\n\r\n",
            node.connected.load(Ordering::Relaxed),
        );
    }
    if asked_for(b"memory") {
        let used = node.memory.used();
        let (ceiling, policy) = reported_limit(node.limit);
        let _ = write!(
            text,
            "# Memory\r\n\
             used_memory:{used}\r\n\
             used_memory_human:{}\r\n\
             maxmemory:{ceiling}\r\n\
             maxmemory_human:{}\r\n\
             maxmemory_policy:{}\r\n\r\n",
            human_bytes(used),
            human_bytes(ceiling),
            policy.name(),
        );
    }
    // The broadcast is paid for only by a request that asks for one of the
    // three sections built from it — `INFO memory` reaches no shard at all —
    // and a scrape that wants everything pays it once rather than three times.
    let from_shards = [&b"stats"[..], b"keyspace", b"commandstats"];
    let stats = if from_shards.iter().any(|section| asked_for(section)) {
        gather_stats(router).await
    } else {
        ShardStats::default()
    };
    if asked_for(b"stats") {
        text.push_str(&stats_section(node, &stats));
    }
    if asked_for(b"keyspace") {
        text.push_str(&keyspace_section(&stats));
    }
    if asked_for(b"commandstats") {
        text.push_str(&commandstats_section(node, &stats));
    }
    if asked_for(b"errorstats") {
        text.push_str(&errorstats_section(node));
    }
    // Redis writes the blank line *between* sections, as a prefix on each
    // section but the first — never as a suffix on all of them — so the
    // document ends on its last field. Every section here is written with its
    // own trailing blank, which is the same thing everywhere except at the
    // end, so the join is already right and only the last two bytes are
    // wrong. Trimming them is equivalent to re-shaping every section into a
    // prefix-separator and touches one place instead of seven.
    //
    // Measured against `redis:6-alpine` (`redis_version:6.2.24`) and
    // `redis:8-alpine` (`redis_version:8.10.1`), which agree: `INFO
    // keyspace` answers 12 bytes and `INFO errorstats` 14, where this server
    // answered 14 and 16.
    if text.ends_with("\r\n\r\n") {
        text.truncate(text.len() - 2);
    }
    text
}

/// One row per error code, or a bare header when nothing has failed yet.
///
/// The section is written with its own trailing blank, which is how it joins
/// the next one; [`info`] trims that blank when this section ends the
/// document, because Redis's separator is a prefix and not a suffix. On a
/// fresh node `INFO errorstats` therefore answers the bulk
/// `# Errorstats\r\n` — 14 bytes on `redis:6-alpine`
/// (`redis_version:6.2.24`), which this now matches.
pub fn errorstats_section(node: &NodeInfo) -> String {
    use std::fmt::Write as _;
    let mut text = String::from("# Errorstats\r\n");
    // The lock is released before the section is finished, rather than at the
    // end of the function: the rows are all it is held for, and the error path
    // that takes it next has no reason to wait on a `push_str`.
    {
        let stats = node
            .errorstats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (code, count) in stats.iter() {
            let _ = write!(text, "errorstat_{code}:count={count}\r\n");
        }
    }
    text.push_str("\r\n");
    text
}

/// The running totals an operator watches: what the node has done since it
/// started, half of it counted by the shards and half at the edge.
///
/// `total_error_replies` sits here beside the other edge counters. Redis 6.2
/// (`redis:6-alpine`, `redis_version:6.2.24`) prints it much later in the
/// section, near its end — the position is not the contract, the name is, and
/// everything that reads this document looks the field up by name.
pub fn stats_section(node: &NodeInfo, stats: &ShardStats) -> String {
    format!(
        "# Stats\r\n\
         total_connections_received:{}\r\n\
         total_commands_processed:{}\r\n\
         rejected_connections:{}\r\n\
         total_error_replies:{}\r\n\
         expired_keys:{}\r\n\
         evicted_keys:{}\r\n\
         keyspace_hits:{}\r\n\
         keyspace_misses:{}\r\n\
         total_net_input_bytes:{}\r\n\
         total_net_output_bytes:{}\r\n\r\n",
        node.total_connections.load(Ordering::Relaxed),
        commands_processed(node, stats),
        node.rejected_connections.load(Ordering::Relaxed),
        node.error_replies.load(Ordering::Relaxed),
        stats.expired,
        stats.evicted,
        stats.hits,
        stats.misses,
        node.net_in.load(Ordering::Relaxed),
        node.net_out.load(Ordering::Relaxed),
    )
}

/// The keyspace as it stands, in the one database this server has.
///
/// Redis prints a line per database that holds something and nothing at all
/// for one that is empty, so an empty keyspace is a section with no rows
/// rather than a row of zeros — an exporter that meets a `db0` line is being
/// told there is a keyspace to describe.
///
/// `avg_ttl` is `0`, which is what Redis reports until its own sampler has an
/// estimate. Nothing here samples deadlines, so `0` is the standing answer and
/// not a placeholder.
pub fn keyspace_section(stats: &ShardStats) -> String {
    let mut text = String::from("# Keyspace\r\n");
    if stats.keys > 0 {
        use std::fmt::Write as _;
        let _ = write!(
            text,
            "db0:keys={},expires={},avg_ttl=0\r\n",
            stats.keys, stats.expires,
        );
    }
    text.push('\r');
    text.push('\n');
    text
}

/// How many of each command this node has accepted as one.
///
/// Accepted, not run, and the difference is worth stating because Redis's
/// `calls` means the second: a command is counted where it is decoded, which
/// is before the authentication gate, so on a node with a password the `PING`,
/// `INFO`, `CONFIG` and the rest that were answered `NOAUTH` are counted here.
/// Redis puts a rejection that never executed in `rejected_calls`, which this
/// section does not carry. Which commands can be inflated that way is decided
/// by [`EDGE_NAMES`], not by whether a command names a key: an `MGET` and a
/// `KEYS` are counted here too — [`per_key`] makes the first an [`Unbatched`]
/// rather than a dispatch, and the second is one by construction — so a
/// `NOAUTH` inflates them as readily as it inflates a `PING`, without either
/// reaching a shard. What cannot be inflated is what only a shard counts: a
/// `GET` refused at the gate never reaches one, and no shard counts what it
/// never ran. A command whose own handler refused it — bad arity, an argument
/// it could not read — is not counted either way; it never became an action.
/// The exception is a refusal the handler decides *about the request*, which
/// is an [`Action::Refuse`] and so an action by construction: a `HELLO` naming
/// a protocol version this server does not speak is counted here, read off
/// this section on 2026-09-10 as
/// `cmdstat_hello:calls=1,usec=1,usec_per_call=1.00` for one such handshake.
///
/// **`usec` is measured, not estimated.** Each command is timed where it is
/// counted: at the executor, by one clock reading differenced against the
/// reading before it — the envelope's own for the first command of a batch —
/// and at the edge for the requests no shard sees whole. `usec_per_call` is
/// the quotient of the two figures beside it, taken over node totals rather
/// than averaged across shards.
///
/// The three fields are printed in Redis's order because that order is a
/// contract: the exporter this project is gated on reads a `cmdstat_` line by
/// position and takes the second field as microseconds, whatever it is named.
///
/// The figures are not comparable between the two halves, and the section
/// cannot say so in its own text. A shard's reading is what a command cost;
/// an edge reading spans the wait for every shard the request reached, so it
/// is what the request took — see [`NodeInfo::edge_usec`].
///
/// A command nobody has sent has no line, which is Redis's behaviour: the
/// section describes what has happened, not what could.
pub fn commandstats_section(node: &NodeInfo, stats: &ShardStats) -> String {
    use std::fmt::Write as _;
    let mut text = String::from("# Commandstats\r\n");
    for (name, calls, usec) in command_stats(node, stats) {
        if calls > 0 {
            // A quotient of two counts, printed to two decimals. Both sides
            // are exact in `f64` far past any figure a node produces, and
            // what is printed is rounded to a hundredth of a microsecond
            // regardless.
            #[allow(
                clippy::cast_precision_loss,
                reason = "a display figure, rounded to two decimals"
            )]
            let per_call = usec as f64 / calls as f64;
            let _ = write!(
                text,
                "cmdstat_{name}:calls={calls},usec={usec},usec_per_call={per_call:.2}\r\n"
            );
        }
    }
    text.push('\r');
    text.push('\n');
    text
}

/// Every command name this node can report on, with its call count and the
/// microseconds those calls spent.
///
/// Two halves summed under one name: what the shards counted, by
/// [`KIND_NAMES`], and what the edge counted, by [`EDGE_NAMES`]. They overlap
/// in exactly one place — `info`, which the shards leave to the edge — and
/// the sum is taken rather than assumed, so a later command that lands in both
/// prints one line and not two.
///
/// The two figures move together through this, always as a pair: a name whose
/// calls came from one half and whose microseconds came from the other would
/// render a `usec_per_call` describing neither.
pub fn command_stats(node: &NodeInfo, stats: &ShardStats) -> Vec<(&'static str, u64, u64)> {
    let mut counts: Vec<(&'static str, u64, u64)> = KIND_NAMES
        .iter()
        .enumerate()
        .skip(1)
        .map(|(kind, name)| (*name, stats.calls[kind], stats.usec[kind]))
        .collect();
    for ((name, calls), usec) in EDGE_NAMES
        .iter()
        .zip(node.edge_calls.iter())
        .zip(node.edge_usec.iter())
    {
        let (calls, usec) = (calls.load(Ordering::Relaxed), usec.load(Ordering::Relaxed));
        match counts.iter_mut().find(|(known, _, _)| known == name) {
            Some((_, total_calls, total_usec)) => {
                *total_calls += calls;
                *total_usec += usec;
            }
            None => counts.push((name, calls, usec)),
        }
    }
    counts
}

/// What `total_commands_processed` reports: every command counted anywhere.
///
/// The same two halves [`command_stats`] sums, added rather than listed.
///
/// **A request the edge splits is counted at both layers, and Redis does not
/// do this.** An `MGET` over four keys is one request here and four `GET`s at
/// the shards, so it adds five. Redis increments `stat_numcommands` once per
/// `call()`, and an `MGET` is one `call()` however many keys it names —
/// what expands into further calls there is a script or a transaction body,
/// not a multi-key read (measured against `redis:6-alpine`, 6.2.24). The
/// divergence follows from the architecture rather than from a choice: the
/// work a shard did is a figure this node can state and an operator wants,
/// and dropping it would leave the shards' own counters describing nothing.
/// So it is stated instead of corrected — here, and in the observability
/// section of `docs/ARCHITECTURE.md`, where the operator reading the document
/// meets it.
pub fn commands_processed(node: &NodeInfo, stats: &ShardStats) -> u64 {
    command_stats(node, stats)
        .into_iter()
        .map(|(_, calls, _)| calls)
        .sum()
}

/// Sums every shard's counters into the node's.
///
/// Field by field, because that is the whole of the arithmetic: each figure
/// is a count of things that happened on one shard, and what an operator asks
/// about is how many happened on the node. A shard that answers anything but
/// [`Reply::Stats`] contributes nothing rather than failing the document —
/// `INFO` is what an operator reads when something is already wrong, and a
/// scrape that fails entirely because one shard is unreachable is a scrape
/// that goes blind exactly when it is needed.
pub async fn gather_stats<R: Router>(router: &R) -> ShardStats {
    let mut total = ShardStats::default();
    for reply in router.dispatch_every(Command::Stats).await {
        if let Reply::Stats(stats) = reply {
            total.keys += stats.keys;
            total.expires += stats.expires;
            total.evicted += stats.evicted;
            total.hits += stats.hits;
            total.misses += stats.misses;
            total.expired += stats.expired;
            for (sum, count) in total.calls.iter_mut().zip(stats.calls) {
                *sum += count;
            }
            for (sum, spent) in total.usec.iter_mut().zip(stats.usec) {
                *sum += spent;
            }
        }
    }
    total
}

/// Redis's `_human` spelling: two decimals and a binary unit, `1.23K`,
/// `4.56M`, `7.89G`, and bare bytes below a kilobyte.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [(u64, &str); 3] = [(1 << 30, "G"), (1 << 20, "M"), (1 << 10, "K")];
    for (unit, suffix) in UNITS {
        if bytes >= unit {
            // Two decimals of a ratio below 2^34: f64 holds it exactly enough
            // for a display string, which is all this is.
            #[allow(
                clippy::cast_precision_loss,
                reason = "a display figure, rounded to two decimals"
            )]
            let value = bytes as f64 / unit as f64;
            return format!("{value:.2}{suffix}");
        }
    }
    format!("{bytes}B")
}
