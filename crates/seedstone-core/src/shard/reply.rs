//! What a shard answers: the reply shapes and the errors a shard can hand
//! back, each with its wire text.

use crate::shard::ShardStats;

/// Every way a shard can refuse a command.
///
/// A closed set, and that is the point. The wire text used to be a `String`
/// carried inside the reply, with two of the constants `pub` so the
/// simulator's planted router could answer exactly what the honest one
/// answers — a shared constant discourages drift but does not prevent it,
/// since any caller could still build a different string. Naming the failure
/// instead of spelling it makes the planted router agree by construction.
///
/// It also puts the frame-safety guarantee in the type: every text below is a
/// literal in this file with no `\r` and no `\n`, so a shard error can never
/// split a response frame. `every_shard_error_is_frame_safe` checks it over
/// the whole set rather than over one example.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyError {
    /// A numeric operation on a value that is not an integer.
    NotAnInteger,
    /// An `IncrBy` whose result would leave `i64`.
    WouldOverflow,
    /// The executor hosting the command's shard is gone.
    ///
    /// Unreachable while a [`crate::shard::ShardPool`] is alive — it holds every sender, and
    /// an executor task only stops when its inbox closes. It exists so the
    /// dispatch path has no `unwrap`.
    ShardUnavailable,
    /// A mutation whose log record could not be written.
    ///
    /// **A read can answer this too, and that is new.** A command meeting a
    /// key whose deadline has passed must log the eviction before removing it,
    /// like any other keyspace mutation — so `Get`, `Ttl` and `Exists` reach
    /// the log on exactly the paths where they evict, and fail here if it
    /// refuses. A client reading this as "my write did not land" would be
    /// reading it too narrowly: it means the shard could not record a change
    /// it was about to make, and so did not make it.
    LogWriteFailed,
    /// A write that would add bytes, refused because the node is over its
    /// ceiling and its policy is `noeviction`.
    ///
    /// The commands it can answer are exactly the ones
    /// [`crate::shard::Command::denied_when_full`] names. A read, a delete and an expiry
    /// are never refused this way: refusing the operations that *reclaim* is
    /// how a full node stays full.
    OutOfMemory,
}

impl ReplyError {
    /// The text this failure takes on the wire.
    ///
    /// Byte-for-byte what Redis returns where Redis has an equivalent, so
    /// existing clients that match on the string keep working.
    #[must_use]
    pub const fn wire_text(self) -> &'static str {
        match self {
            Self::NotAnInteger => "ERR value is not an integer or out of range",
            Self::WouldOverflow => "ERR increment or decrement would overflow",
            Self::ShardUnavailable => "ERR shard is unavailable",
            Self::LogWriteFailed => "ERR replication log write failed",
            // Redis's text, trailing full stop and all: clients match on it.
            Self::OutOfMemory => "OOM command not allowed when used memory > 'maxmemory'.",
        }
    }
}

/// A shard's answer to one [`crate::shard::Command`].
///
/// This is the core's own vocabulary, not RESP: the shard runtime never sees
/// a wire frame. `service` translates in both directions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// A value, or its absence.
    Bulk(Option<Vec<u8>>),
    /// The command succeeded and has nothing to return.
    Ok,
    /// A one-word answer that travels as a simple string rather than a bulk.
    ///
    /// `&'static str` for the reason [`ReplyError::wire_text`] gives: the set
    /// of texts is server-authored and closed, so no peer-supplied byte can
    /// reach it and split a frame.
    ///
    /// [`Ok`](Self::Ok) is not folded into this. It is the answer to a command
    /// that has nothing to report, which is a different statement from an
    /// answer whose content happens to be a word — and it is the answer on the
    /// write path, where a fixed variant beats carrying a string that is
    /// always the same one.
    Status(&'static str),
    /// Whether a `Del` removed anything.
    Removed(bool),
    /// An integer result.
    Integer(i64),
    /// One step of a keyspace walk: where to resume, and what this step found.
    ///
    /// A cursor of `0` ends the cycle. Any other value is where the next step
    /// resumes, and it is opaque to whoever holds it — a position in a cycle,
    /// not an offset into a table.
    Scan {
        /// Where the next step resumes, or `0` if the cycle is complete.
        cursor: u64,
        /// The keys this step visited that survived the pattern, in the order
        /// the table gave them up. Possibly empty with a non-zero cursor: a
        /// step's budget is buckets, and buckets can be empty.
        keys: Vec<Vec<u8>>,
        /// How many buckets this step visited — the edge's unit of budget
        /// when one call walks more than one shard. At least one; at most
        /// what the step was asked for; exactly the table's size when the
        /// step finished the cycle.
        visited: usize,
    },
    /// What one shard has counted. Never a wire answer — see [`ShardStats`].
    ///
    /// **Boxed, and that is not incidental.** [`ShardStats`] is 304 bytes,
    /// almost all of it the per-command call counts and the time those calls
    /// spent, and every other variant of this enum fits in forty. Stored
    /// inline it would be the size of a
    /// `Reply`, which is the type every command answers with and which an
    /// envelope holds one of per command — so a figure a scrape asks for once
    /// a minute would be paid for by every `GET` on the machine. One
    /// allocation on the one command nothing on the hot path issues is the
    /// cheaper side of that trade by a wide margin.
    Stats(Box<ShardStats>),
    /// The command failed. See [`ReplyError`] — a closed set of
    /// server-authored failures, none of whose texts can split a frame.
    Error(ReplyError),
}
