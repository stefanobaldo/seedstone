//! The shard runtime: N keyspaces, hosted by a smaller number of executor
//! tasks, each behind an unbounded inbox.
//!
//! A virtual shard is a [`Dict`] nothing else can reach, its replication
//! position, and its log. It is the unit of *keyspace ownership*, not of
//! scheduling: an executor task owns a contiguous range of shards and is the
//! only thing that touches their state. Work arrives as an [`Envelope`] — a
//! batch of `(shard, command)` pairs plus the one-shot channel its replies go
//! back on — and an executor answers envelopes one at a time, in arrival
//! order, applying each batch's commands in order. Nothing is shared, so
//! nothing is locked.
//!
//! Splitting the two lets the shard count stay a placement decision, fixed by
//! the deployment format, while the executor count follows the machine. A key
//! never moves between shards, and a shard's whole history stays inside one
//! task.
//!
//! # Why a handler is a plain `fn`
//!
//! [`apply`] takes `&mut Dict` and returns a `Reply`. It is not `async`, and
//! that is the point: a handler that cannot `await` cannot yield the executor
//! mid-command, so a command either has not started or has finished, and two
//! commands on one key can never interleave. The rule is enforced by the
//! signature rather than by review — the only `await`s in an executor task are
//! the `select!` arms of [`run_executor`]. A batch inherits the property: no
//! `await` separates its commands either, so nothing from another connection
//! can land inside one.
//!
//! That is also why the interesting concurrency bugs of this system live
//! *above* the shard, in code that sends two messages with an `await` between
//! them. The simulator plants exactly that race.

use crate::dict::{Dict, DictSeed, Entry, WalkOrder};
use crate::glob;
use crate::log::{NoopLog, Record, ReplicationLog};
use crate::memory::{EvictionMode, MemoryGauge, MemoryLimit};
use crate::slot::{executor_of, shard_of};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

/// How often a shard does the work no command asked it for: advancing an
/// in-flight rehash, sweeping expired keys, and syncing its log.
///
/// Public because it is a fact about the server a test harness has to know
/// rather than assume: the granularity at which a deadline that nothing
/// touches again is actually reclaimed is this number, and the simulator's
/// expiration invariants are derived from it. Read from here, a change to
/// the cadence reaches everything that depends on it; copied, it would not.
pub const HOUSEKEEPING_TICK: Duration = Duration::from_millis(100);

/// Buckets migrated per rehash tick.
///
/// Writes already migrate as they go; the tick exists so a table that stopped
/// receiving writes mid-rehash still finishes, rather than sitting split
/// across two tables forever.
///
/// The number has to answer that "forever" and nothing else, because only
/// writes advance a rehash: [`Dict::get`] deliberately does not, and a `Del`
/// that removed nothing returns before reaching `remove`. So a shard that
/// grows and then goes read-only is draining at exactly this rate. At four
/// buckets per tick — forty a second — a table grown to 65 536 buckets stays
/// split for twenty-seven minutes, holding two tables and probing both on
/// every miss. At 1024 the same table drains in six seconds, and the tick's
/// own cost stays in the same class as one large command, which is what the
/// no-await rule actually constrains.
const REHASH_BUCKETS_PER_TICK: usize = 1024;

/// Buckets swept for expired keys per housekeeping tick.
///
/// This is the whole of the active half of expiration: a key nothing touches
/// again is reclaimed only when the cursor reaches its bucket, so the budget
/// sets how long a dead entry can hold its memory. A table of N buckets is
/// walked in `N / 256` ticks, and [`HOUSEKEEPING_TICK`] is ten a second — so
/// 2048 buckets are a full cycle in eight ticks, under a second, and a
/// million-bucket table takes 3906 ticks, six and a half minutes.
///
/// Smaller than [`REHASH_BUCKETS_PER_TICK`], and deliberately: a rehash is a
/// state the dict has to leave, paying two lookups on every miss until it
/// does, while a sweep is a standing cost every tick of the process's life. It
/// is also the more expensive walk per bucket — it reads every entry's
/// deadline rather than moving whole chains — and it runs against every owned
/// dict that could hold a deadline, where most rehashes are over.
///
/// **The budget bounds the walk, not the tick.** What this number caps is the
/// cursor steps [`Dict::expire_step`] takes; the removals that follow are
/// extra, and each costs more than a bucket of walking. Every reported key is
/// hashed a second time — the walk returns key bytes, so [`Dict::remove`]
/// hashes each one again — and while a rehash is in flight each of those
/// `remove` calls also advances it by a bucket. A tick that sweeps buckets
/// full of due deadlines therefore costs meaningfully more than this constant
/// alone suggests, and its worst case scales with how many swept entries are
/// due rather than with the budget.
const EXPIRE_BUCKETS_PER_TICK: usize = 256;

/// How many entries one eviction looks at before choosing the one it removes.
///
/// Redis's `maxmemory-samples` default, and the same trade: five is enough
/// that the key removed is nearly always among the older ones, and few enough
/// that making room costs a handful of comparisons rather than a walk of the
/// keyspace. Approximate LRU is the deliberate part — an exact one wants a
/// list threaded through every entry, which is per-key memory spent to
/// improve a hit rate that sampling already gets most of.
///
/// What five buys is measured rather than asserted:
/// `sampled_eviction_rarely_takes_a_recently_touched_key` in `dict.rs` holds
/// the sample to a bound on how often it takes a recently used key.
pub const EVICTION_SAMPLES: usize = 5;

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
    /// Unreachable while a [`ShardPool`] is alive — it holds every sender, and
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
    /// [`Command::denied_when_full`] names. A read, a delete and an expiry
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

/// How long a `Set` asks its key to live, in the unit the client chose.
///
/// Kept in that unit rather than resolved to a [`Duration`] at the service
/// layer so the command is exactly what the peer asked for, and so the one
/// place that turns a span into a deadline is the handler that has `now`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiry {
    /// Seconds, from `EX`.
    Ex(u64),
    /// Milliseconds, from `PX`.
    Px(u64),
}

/// The condition a `Set` is subject to.
///
/// Absent, a `Set` always stores. Present, it stores only if the key's
/// existence matches — and a `Set` that stores nothing is not a failure, it is
/// an answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cond {
    /// Only if the key does not exist, from `NX`.
    Nx,
    /// Only if the key already exists, from `XX`.
    Xx,
}

/// A command addressed to the shard that owns its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Read the value stored under `key`.
    Get {
        /// The key to read.
        key: Vec<u8>,
    },
    /// Store `value` under `key`, replacing whatever was there.
    Set {
        /// The key to write.
        key: Vec<u8>,
        /// The bytes to store, kept verbatim.
        value: Vec<u8>,
        /// How long the key should live, or `None` to store it without a
        /// deadline. A `Set` with no expiry clears any deadline the key it
        /// overwrote was carrying — Redis's semantics, and the reason the
        /// absence of an option is a decision rather than a silence.
        expiry: Option<Expiry>,
        /// The condition the write is subject to, or `None` to write
        /// unconditionally.
        cond: Option<Cond>,
        /// Whether the key keeps the deadline it already had, from `KEEPTTL`.
        /// It overrides the clearing `expiry: None` would otherwise do; the
        /// two never arrive together, because the service layer refuses a
        /// `SET` that names both.
        keep_ttl: bool,
        /// Whether the reply is the value the write replaced rather than
        /// `Ok`, from `GET`. The previous value is the answer even when
        /// `cond` refuses the write — the question `GET` asks is what was
        /// there, not what the write did.
        get: bool,
    },
    /// Store `value` under `key` with a deadline `seconds` from now — `SET key
    /// value EX seconds` under the name Redis gave it before `SET` grew
    /// options. Redis lists it as deprecated since 2.6.12 (`COMMAND DOCS
    /// SETEX`, 8.10.1) and still answers it, and so do the clients: redis-py's
    /// `setex()` puts this name on the wire.
    ///
    /// Its own variant rather than a [`Set`](Self::Set) built at the edge,
    /// for one reason: a shard counts what it runs by variant, and Redis
    /// reports `cmdstat_setex` apart from `cmdstat_set` (6.2.24, 8.10.1).
    /// Folding it into `Set` would make `cmdstat_set` count commands no peer
    /// spelled that way, and leave the error-reply log's `SETEX` with no
    /// counter to correlate against. The write itself is `Set`'s, through the
    /// same handler: an unconditional store with a deadline in seconds and no
    /// other option.
    SetEx {
        /// The key to write.
        key: Vec<u8>,
        /// How many seconds from now the key dies. Strictly positive: the
        /// service layer refuses zero and negatives before dispatch, as
        /// Redis does.
        seconds: u64,
        /// The bytes to store, kept verbatim.
        value: Vec<u8>,
    },
    /// Remove `key`.
    Del {
        /// The key to remove.
        key: Vec<u8>,
    },
    /// Add `delta` to the integer stored under `key`, treating a missing key
    /// as zero.
    IncrBy {
        /// The key to update.
        key: Vec<u8>,
        /// The amount to add; may be negative.
        delta: i64,
    },
    /// Give `key` a deadline `seconds` from now, or delete it if that deadline
    /// is not in the future.
    Expire {
        /// The key to put a deadline on.
        key: Vec<u8>,
        /// How many seconds from now; zero or negative deletes the key.
        seconds: i64,
    },
    /// Give `key` a deadline `millis` from now, or delete it if that deadline
    /// is not in the future.
    ///
    /// [`Expire`](Self::Expire) in a smaller unit, and nothing else: the span
    /// is wrapped in [`Expiry::Px`] where the other wraps it in
    /// [`Expiry::Ex`], and from there the two are one handler. Where the unit
    /// does make a difference is the ceiling the service layer holds each
    /// command to, which is the one place that argument belongs.
    PExpire {
        /// The key to put a deadline on.
        key: Vec<u8>,
        /// How many milliseconds from now; zero or negative deletes the key.
        millis: i64,
    },
    /// Report how long `key` has left.
    Ttl {
        /// The key to ask about.
        key: Vec<u8>,
    },
    /// Take `key`'s deadline away, leaving the key itself where it is.
    Persist {
        /// The key to make permanent.
        key: Vec<u8>,
    },
    /// Report whether `key` exists.
    Exists {
        /// The key to ask about.
        key: Vec<u8>,
    },
    /// Report what kind of value `key` holds.
    ///
    /// The answer is looked up rather than computed: strings are the only
    /// type this server stores, so `string` for a key that is there and
    /// `none` for one that is not is the whole of it. A second answer would
    /// arrive with the command that stores a second type, not before.
    Type {
        /// The key to ask about.
        key: Vec<u8>,
    },
    /// Report how many bytes `key`'s value holds.
    ///
    /// A key that is not there is `0`, not an error and not a null — Redis's
    /// answer, and one a key holding an empty value gives too. The two are
    /// indistinguishable here because they hold the same number of bytes.
    StrLen {
        /// The key to measure.
        key: Vec<u8>,
    },
    /// Remove every key the shard holds.
    ///
    /// Keyspace-wide: one of these reaches every shard, and each empties its
    /// own dict, which is the whole of the operation because nothing is
    /// shared between them.
    FlushDb,
    /// Report how many keys the shard holds.
    ///
    /// Keyspace-wide: one of these reaches every shard, and the edge sums the
    /// answers.
    DbSize,
    /// One step of a keyspace walk on one shard.
    ///
    /// Never reaches a client under this name: `SCAN` unpacks its cursor into
    /// a shard and one of these, and `KEYS` drives one loop of these per shard
    /// concurrently. Splitting the walk into ordinary envelopes is what makes
    /// it yield — between two steps any other command on the shard runs — and
    /// it is why neither command needs a sliced loop inside the executor.
    ///
    /// The shard is the caller's to name, through
    /// [`Router::dispatch_at`]: a step carries where it is in a shard's table,
    /// not which shard's table it is.
    ScanStep {
        /// Where in this shard's cycle to resume. `0` starts one.
        cursor: u64,
        /// How many cursor steps to take before answering. Each step covers
        /// one bucket of the table, or — while a rehash is in flight — one of
        /// the smaller table and the ones of the larger it expands into, which
        /// is the same accounting [`Dict::expire_step`] uses.
        ///
        /// A bound on occupancy, not a promise about how many keys come back:
        /// a step may answer with none and a non-zero cursor.
        count: usize,
        /// Return only keys matching this glob, filtered here rather than at
        /// the edge so the channel does not carry a keyspace to discard it.
        pattern: Option<Vec<u8>>,
    },
    /// Report what this shard has counted since it started.
    ///
    /// Keyspace-wide, and never sent by a peer: `INFO` broadcasts one of
    /// these and sums the answers, which is the only way a per-shard counter
    /// becomes a node-wide figure. The alternative — one shared atomic per
    /// counter, incremented by every executor — is a contended word on the
    /// hot path to save a broadcast on a command nothing hot issues.
    ///
    /// It is answered by the executor rather than by [`apply`], because what
    /// it reports lives beside the dict rather than in it.
    Stats,
}

/// How a command reaches the shards that must run it.
///
/// Until this existed, every command named exactly one key and the hash of
/// that key was the whole routing decision. A walk step names no key and no
/// shard of its own, and `KEYS`, `DBSIZE` and `FLUSHDB` name no key at all and
/// must reach every shard. The routing decision is therefore the command's to
/// state, not something the router can derive from a key that may not exist.
///
/// **Nothing in the workspace produces [`Route::Shard`] today.**
/// [`Command::route`] answers `Key`, `Every` or `Unaddressed` and nothing
/// else. `SCAN` was the one command that would have named a shard outright;
/// its shard comes out of a cursor a peer chose, so it reaches the router
/// through [`Router::dispatch_at`] — an argument the caller has already
/// checked — rather than out of the command. The variant is kept anyway, and
/// the reason is its tag in the simulator's route fold. Those tags are
/// hand-written literals, so deleting the variant moves nothing by itself; it
/// leaves a spent number in the middle of a short list, which is an invitation
/// to close the gap, and a renumbering there changes every recorded trace
/// hash. The variant is what keeps the number visibly taken. It costs two live
/// arms — one in the shard pool's `shard_for`, one in the fold — and it is the
/// shape a genuinely shard-addressed command would arrive in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route<'a> {
    /// The hash of this key decides the shard.
    Key(&'a [u8]),
    /// This shard, named outright. No command answers with this today — see
    /// the note on the enum — so nothing is currently held to the range it
    /// would have to name; a router handed one out of range answers
    /// `ShardUnavailable` rather than panicking, which is the same refusal
    /// [`Route::Unaddressed`] gets.
    Shard(u16),
    /// Every shard, answered once each, gathered in shard order.
    Every,
    /// No shard of its own: the caller names one through
    /// [`Router::dispatch_at`].
    ///
    /// A router asked to route this on its own has no answer, and says so
    /// with [`ReplyError::ShardUnavailable`] rather than picking a shard. The
    /// alternative — standing in a plausible shard — is a command that
    /// answers from the wrong table and looks like it worked, which is the
    /// one failure shape a walk over a client-supplied cursor must not have.
    Unaddressed,
}

impl Command {
    /// How this command reaches the shards that must run it.
    ///
    /// A request naming several keys is still split into one command per key
    /// before it reaches a shard, because the shards that own them are not in
    /// general the same shard. What changed is that naming a key is no longer
    /// the only way to be routed.
    #[must_use]
    pub fn route(&self) -> Route<'_> {
        match self {
            Self::Get { key }
            | Self::Set { key, .. }
            | Self::SetEx { key, .. }
            | Self::Del { key }
            | Self::IncrBy { key, .. }
            | Self::Expire { key, .. }
            | Self::PExpire { key, .. }
            | Self::Ttl { key }
            | Self::Persist { key }
            | Self::Exists { key }
            | Self::Type { key }
            | Self::StrLen { key } => Route::Key(key),
            Self::FlushDb | Self::DbSize | Self::Stats => Route::Every,
            // The one route that is not self-sufficient. A step knows where it
            // is in *a* shard's table and not which shard's, so it names no
            // shard and the caller supplies the real one through
            // [`Router::dispatch_at`].
            //
            // This named shard `0` until a client's cursor could supply one.
            // A placeholder is a real shard, so a step that reached `dispatch`
            // by mistake walked shard 0 and answered plausibly instead of
            // failing — a partial answer over a fraction of the keyspace, with
            // nothing on the wire to distinguish it from a whole one. `SCAN`
            // unpacks its shard out of an integer a peer chose, so that stopped
            // being a hypothetical and the route stopped naming a shard.
            Self::ScanStep { .. } => Route::Unaddressed,
        }
    }

    /// The largest tag [`Command::kind`] can return.
    ///
    /// Every per-kind array is sized from this rather than from a literal
    /// beside itself: `service`'s `KIND_NAMES`, and the shard's own `calls`
    /// counter. An array sized independently is one a sixteenth variant
    /// overruns, and the overrun would happen inside a spawned executor —
    /// where it reads as a shard that died answering `ShardUnavailable`
    /// rather than as an index out of bounds.
    ///
    /// `every_kind_tag_is_contiguous_and_bounded` is what keeps it true when
    /// a variant is added: it fails, rather than the array growing silently
    /// or a panic waiting for the traffic that reaches the new command.
    pub const KIND_MAX: u8 = 16;

    /// A stable one-byte tag for this command's variant.
    ///
    /// `Get` = 1, `Set` = 2, `Del` = 3, `IncrBy` = 4, `Expire` = 5, `Ttl` = 6,
    /// `Exists` = 7, `FlushDb` = 8, `DbSize` = 9, `ScanStep` = 10,
    /// `PExpire` = 11, `Persist` = 12, `Type` = 13, `StrLen` = 14,
    /// `Stats` = 15, `SetEx` = 16. These
    /// values are folded into the simulator's trace hash, so they are part of
    /// what a replay compares: changing one changes every recorded hash. A tag
    /// is therefore never reused and never renumbered.
    #[must_use]
    pub const fn kind(&self) -> u8 {
        match self {
            Self::Get { .. } => 1,
            Self::Set { .. } => 2,
            Self::Del { .. } => 3,
            Self::IncrBy { .. } => 4,
            Self::Expire { .. } => 5,
            Self::Ttl { .. } => 6,
            Self::Exists { .. } => 7,
            Self::FlushDb => 8,
            Self::DbSize => 9,
            Self::ScanStep { .. } => 10,
            Self::PExpire { .. } => 11,
            Self::Persist { .. } => 12,
            Self::Type { .. } => 13,
            Self::StrLen { .. } => 14,
            Self::Stats => 15,
            Self::SetEx { .. } => 16,
        }
    }

    /// Whether this command is refused under `noeviction` once the gauge is
    /// past the ceiling: the ones that add bytes. Redis's `denyoom` flag.
    ///
    /// `Set`, `SetEx` and `IncrBy` and nothing else. `Expire` and `Persist` rewrite a
    /// field on an entry already there, and every other command either reads
    /// or reclaims — refusing those would leave a full node with no way back
    /// under its ceiling.
    #[must_use]
    pub const fn denied_when_full(&self) -> bool {
        matches!(
            self,
            Self::Set { .. } | Self::SetEx { .. } | Self::IncrBy { .. }
        )
    }
}

/// How many slots a per-kind array needs: one per tag, plus slot `0`, which
/// is no command.
///
/// Derived from [`Command::KIND_MAX`] rather than written beside each array,
/// which is the whole point — the two arrays that use it and `service`'s
/// `KIND_NAMES` all grow together or not at all.
pub const KIND_SLOTS: usize = Command::KIND_MAX as usize + 1;

/// A shard's answer to one [`Command`].
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

/// One shard's counters, as [`Command::Stats`] reports them.
///
/// Every field is a running total from the moment the shard started, except
/// [`keys`](Self::keys) and [`expires`](Self::expires), which are the
/// keyspace as it stands. `INFO` sums them field by field across the shards
/// and prints the sum; nothing is divided or averaged here, so summing is the
/// whole of this type's arithmetic. The one quotient `INFO` prints —
/// `usec_per_call` — is taken at render from two of these sums, so it is a
/// ratio of node totals rather than an average of shard averages.
///
/// It reaches no client under this name: the reply carrying it is refused by
/// the service layer's renderer, because a peer that could dispatch one would
/// be reading one shard's counters as if they were the node's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShardStats {
    /// Entries the dict holds, including any whose deadline has passed and
    /// which the sweep has not yet reached — the figure `DBSIZE` reports, for
    /// the reason its arm states.
    pub keys: u64,
    /// How many of those entries carry a deadline.
    pub expires: u64,
    /// Entries this shard has evicted to stay under the node's ceiling.
    /// Expiries are not evictions and are not counted here.
    pub evicted: u64,
    /// Keyed lookups that found a live entry.
    pub hits: u64,
    /// Keyed lookups that found nothing.
    pub misses: u64,
    /// Entries removed because their deadline had passed, by either half of
    /// expiration.
    pub expired: u64,
    /// How many commands of each [`Command::kind`] this shard has run, indexed
    /// by the tag itself — slot `0` is no command and stays zero. Sized from
    /// [`Command::KIND_MAX`], so a variant added to `Command` widens this
    /// rather than overrunning it.
    ///
    /// The three commands a single request sends to *every* shard are absent
    /// from this: see [`count_call`], which says why counting them here would
    /// report one request as sixteen.
    pub calls: [u64; KIND_SLOTS],
    /// How long those commands took, in microseconds, indexed the same way.
    ///
    /// One clock reading per command, differenced against the reading before
    /// it — the executor's own, taken once when the envelope arrived, for the
    /// first command of a batch. It measures the handler and what the
    /// executor does around it: the memory accounting either side of `apply`,
    /// and any eviction the write triggered, all of which are that command's
    /// cost and nobody else's.
    ///
    /// **Never folded into a trace.** Under the simulator a handler cannot
    /// `await`, so no simulated instant passes and every figure here is
    /// exactly zero; under a real runtime it is a wall-clock reading, which is
    /// the one kind of number a replay must not depend on. It is reported and
    /// not decided upon.
    pub usec: [u64; KIND_SLOTS],
}

/// One unit of work for an executor: a batch of commands and where its
/// replies go.
pub struct Envelope {
    /// `(shard, command)` pairs, applied in order.
    ///
    /// The shard id is computed once, at routing time; carrying it is what
    /// keeps the executor from hashing every key a second time. Every pair's
    /// shard must be one the receiving executor owns.
    pub cmds: Vec<(u16, Command)>,
    /// Answered once, with one reply per command, in the same order.
    pub reply: oneshot::Sender<Vec<Reply>>,
}

/// An observer of every command a shard completes.
///
/// The simulator folds these calls into a trace hash. Calls arrive in each
/// shard's own execution order, which under a deterministic scheduler is a
/// function of the seed alone — so the fold is reproducible.
pub trait TraceSink: Clone + Send + 'static {
    /// Called once per completed command, after the reply is computed and
    /// before it is sent.
    ///
    /// `seq` is the shard's replication position at which the command's
    /// effects *begin*: the position of the first record it appended, or —
    /// for a command that appended none — the position it observed without
    /// consuming.
    ///
    /// **Not "the record this command wrote".** A command may consume more
    /// than one position: a write that first had to remove a key whose
    /// deadline had passed appends the eviction's record here and its own
    /// after it, so the position reported is the eviction's. What the field
    /// carries is where in the shard's order the command's run started, which
    /// is what makes a schedule that reordered two commands visible; reading
    /// it as an index into the log would be wrong.
    fn record(&self, shard: u16, seq: u64, cmd: &Command, reply: &Reply);
}

/// A [`TraceSink`] that observes nothing. Production's sink.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoTrace;

impl TraceSink for NoTrace {
    fn record(&self, _shard: u16, _seq: u64, _cmd: &Command, _reply: &Reply) {}
}

/// Whether a deadline has come due.
///
/// Expiry is decided in two places — in front of every command
/// ([`evict_if_expired`]) and on the housekeeping tick ([`sweep_expired`]) —
/// and both ask this. Production has exactly one implementation, [`Deadlines`],
/// and the parameter exists so that the simulator can supply others: a plant
/// that answers wrongly *is* the defect an invariant claims to catch, where a
/// rewritten request only reproduces what that defect would look like from
/// outside. A cargo feature could not do this job — `cargo build --workspace`
/// unifies features, so the simulator's would be compiled into the shipped
/// binary — and a runtime flag would be worse.
pub trait ExpiryPolicy: Clone + Send + 'static {
    /// Is a key carrying `expires_at` due at `now`, for a command looking at it?
    fn due_on_read(&self, expires_at: Option<Instant>, now: Instant) -> bool;

    /// Is a key carrying `expires_at` due at `now`, for the active sweep?
    fn due_on_sweep(&self, expires_at: Option<Instant>, now: Instant) -> bool;

    /// May a key with no deadline at all be taken?
    ///
    /// `false` for any honest policy, and answering it opens both fast paths:
    /// a dict that has never held a deadline is neither walked by the sweep
    /// nor looked up in front of a command. A policy that takes undated keys
    /// has to answer `true` or it would observe nothing.
    fn takes_undated(&self) -> bool;
}

/// The honest policy: a key is due once `now` has *reached* its deadline, and
/// a key with no deadline is never due.
///
/// A zero-sized type, so the calls above monomorphise and inline into the
/// comparisons they replaced. It is the default of [`ShardPool::spawn`] and
/// [`ShardPool::spawn_with_log`], and the only implementation this crate ships.
#[derive(Debug, Clone, Copy, Default)]
pub struct Deadlines;

impl ExpiryPolicy for Deadlines {
    fn due_on_read(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        expires_at.is_some_and(|at| at <= now)
    }

    fn due_on_sweep(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        expires_at.is_some_and(|at| at <= now)
    }

    fn takes_undated(&self) -> bool {
        false
    }
}

impl WalkOrder for Deadlines {}

impl EvictionPolicy for Deadlines {
    fn must_evict(&self, used: u64, ceiling: Option<u64>) -> bool {
        // The same comparison `MemoryLimit::exceeded` makes, and the same
        // function, so the byte the two paths act at cannot drift apart.
        crate::memory::past_ceiling(used, ceiling)
    }
}

/// Whether the node must reclaim now.
///
/// The other half of the [`ExpiryPolicy`] seam, and it exists for the same
/// reason: a ceiling nobody has watched fail is a ceiling nobody has
/// measured. The two defects worth planting are on either side of the
/// comparison — a node that reads its ceiling and never acts on it, and one
/// that reclaims whatever it is holding — and neither is expressible by
/// rewriting a request from outside, because the decision is taken inside the
/// executor loop against a figure no client can see.
///
/// Asked *instead of* the comparison rather than beside it: the honest
/// implementation is that comparison, so a policy that answers differently is
/// the whole defect and not an imitation of one.
pub trait EvictionPolicy: Clone + Send + 'static {
    /// Whether the node must reclaim now, given the gauge and the ceiling.
    ///
    /// `ceiling` is `None` when the node is unbounded, and the honest answer
    /// is then `false` — a node with no ceiling has nothing to be over.
    fn must_evict(&self, used: u64, ceiling: Option<u64>) -> bool;
}

/// Every decision a shard executor consults that production has exactly one
/// answer for.
///
/// Three so far — when a deadline comes due, how a walk's cursor advances,
/// and whether the node must reclaim — and they travel together because they
/// are held by the same thing for the same span: one value, handed to
/// [`ShardPool::spawn_with_policy`] and kept for the life of the executor.
/// Splitting them into separate parameters would multiply a generic that
/// already reaches from the pool's constructor to the dict, and buy nothing:
/// a caller supplying one always has an answer for the others, and the honest
/// answer is a unit struct.
///
/// Blanket-implemented, so nothing implements this directly: a type that
/// answers all three questions is a shard policy, and there is no second thing
/// to remember to do.
pub trait ShardPolicy: ExpiryPolicy + WalkOrder + EvictionPolicy {}

impl<T: ExpiryPolicy + WalkOrder + EvictionPolicy> ShardPolicy for T {}

/// Anything that can answer a [`Command`].
///
/// The service layer is generic over this so a test, the simulator, or a
/// deliberately racy wrapper can stand in for the real pool without the
/// connection code knowing.
pub trait Router: Clone + Send + Sync + 'static {
    /// Routes `cmd` to whatever owns its key and resolves to the reply.
    ///
    /// # Cancellation
    ///
    /// **An implementation may have routed the command before the returned
    /// future is polled, and dropping that future does not un-route it.**
    /// [`ShardPool`] sends at call time; the simulator's `async fn` routers do
    /// not. So a caller that abandons a dispatch — a timeout, a `select!`, a
    /// shutdown — must treat the command as possibly applied.
    ///
    /// The per-key atomicity the shard guarantees stops at this boundary: a
    /// command either has not started or has finished, but the caller does not
    /// always get to learn which.
    fn dispatch(&self, cmd: Command) -> impl Future<Output = Reply> + Send;

    /// How many shards [`dispatch_at`](Router::dispatch_at) will accept, so
    /// that `0..shards()` is exactly the set of addressable shards.
    ///
    /// A keyspace-wide walk is driven from outside the router — one cursor
    /// loop per shard — and the count is what says how many loops that is. It
    /// lives here rather than beside the caller's other configuration because
    /// a count kept anywhere else is a second source of truth for a number the
    /// router already knows, and the failure when the two disagree is silent:
    /// a walk that visits four shards of sixteen answers with a quarter of the
    /// keyspace and no error.
    ///
    /// Deliberately without a default body, for the reason
    /// [`dispatch_every`](Router::dispatch_every) has none. A default of `1`
    /// would let a real router forget to answer and walk only shard `0`.
    fn shards(&self) -> u16;

    /// Routes `cmd` to the named shard, whatever key it does or does not
    /// carry, and resolves to its reply.
    ///
    /// This is how a command whose [`Route`] cannot name its own shard is
    /// addressed — today, [`Command::ScanStep`], which carries a position in a
    /// table and not which table. Everything else should go through
    /// [`dispatch`](Router::dispatch), which derives the shard from the key
    /// rather than trusting a caller to.
    ///
    /// An out-of-range shard answers [`ReplyError::ShardUnavailable`] rather
    /// than panicking: the shard in a resumed cursor is the client's to
    /// supply, so it is untrusted input.
    ///
    /// # Cancellation
    ///
    /// [`dispatch`](Router::dispatch)'s warning applies unchanged.
    fn dispatch_at(&self, shard: u16, cmd: Command) -> impl Future<Output = Reply> + Send;

    /// Routes a batch and resolves to one reply per command, in order.
    ///
    /// The default serves the batch one dispatch at a time — semantically
    /// identical to dispatching each command on its own, which is exactly what
    /// a test router or a deliberately racy one wants. [`ShardPool`] overrides
    /// it with the grouped path.
    ///
    /// # Cancellation
    ///
    /// [`dispatch`](Router::dispatch)'s warning applies to every command in
    /// the batch, and to an implementation that routes the whole batch before
    /// the future is polled — which [`ShardPool`] does.
    fn dispatch_many(&self, cmds: Vec<Command>) -> impl Future<Output = Vec<Reply>> + Send {
        async move {
            let mut replies = Vec::with_capacity(cmds.len());
            for cmd in cmds {
                replies.push(self.dispatch(cmd).await);
            }
            replies
        }
    }

    /// Routes `cmd` to every shard and resolves to one reply per shard, in
    /// shard order.
    ///
    /// The order is fixed rather than arrival order, for the reason
    /// [`dispatch_many`](Router::dispatch_many) gathers by index: an order
    /// that varied between two runs of one seed would be non-determinism
    /// introduced by the router itself.
    ///
    /// Deliberately without a default body — every implementor must answer
    /// it. A router that silently did not broadcast would answer a
    /// keyspace-wide command from one shard and look correct.
    ///
    /// # Cancellation
    ///
    /// [`dispatch`](Router::dispatch)'s warning applies to every shard.
    fn dispatch_every(&self, cmd: Command) -> impl Future<Output = Vec<Reply>> + Send;
}

/// A set of executor tasks, the virtual shards they host, and the inboxes
/// that reach them.
///
/// Cloning is cheap and shares the same shards: every clone is a handle to
/// one pool, not a copy of it.
#[derive(Clone)]
pub struct ShardPool {
    /// One inbox per executor, indexed by executor id.
    inboxes: Arc<Vec<mpsc::UnboundedSender<Envelope>>>,
    /// How many virtual shards the keyspace is divided into.
    ///
    /// Kept in the width it arrived in: the count is a `u16` at every point
    /// that matters — [`spawn`](ShardPool::spawn) takes one, [`shard_of`]
    /// wants one — and narrowing a `Vec`'s length back down on every dispatch
    /// would be a fallible conversion standing where an invariant already
    /// holds. Two bytes buy its absence.
    shards: u16,
    /// How many executor tasks host those shards. Redundant with
    /// `inboxes.len()`, for the reason `shards` states.
    executors: u16,
    /// The node-wide memory figure every executor of this pool keeps current.
    ///
    /// Owned here rather than by a caller because it has to be handed to the
    /// executors as they are spawned, and this is the only place that spawns
    /// them; [`memory`](ShardPool::memory) is how everything else gets a
    /// handle to the same word.
    memory: MemoryGauge,
    /// The ceiling this node's keyspace is held under, and what happens at
    /// it. Stored so that the edge can report it — `INFO` prints `maxmemory`
    /// and `maxmemory_policy` — from the same value the executors evict by.
    limit: MemoryLimit,
}

impl ShardPool {
    /// Spawns `executors` executor tasks on the current tokio runtime,
    /// hosting `shards` virtual shards between them.
    ///
    /// The shards are partitioned into contiguous ranges by
    /// [`executor_of`]. Each shard hashes with a seed derived from `seed` —
    /// `k0` xored with the shard index — so one root seed fixes the whole
    /// node's placement while no two shards share a bucket layout.
    ///
    /// Every shard logs to a [`NoopLog`]. See
    /// [`spawn_with_log`](ShardPool::spawn_with_log) to supply a real one.
    ///
    /// # Panics
    ///
    /// If `shards` is zero — there would be nowhere to route a key — or if
    /// `executors` is not in `1..=shards`.
    pub fn spawn<T: TraceSink>(shards: u16, executors: u16, seed: DictSeed, trace: T) -> Self {
        Self::spawn_full(
            shards,
            executors,
            seed,
            trace,
            |_shard| NoopLog,
            Deadlines,
            MemoryLimit::default(),
        )
    }

    /// [`spawn`](ShardPool::spawn) with a ceiling on what the node's keyspace
    /// may be accounted at.
    ///
    /// The one constructor a production node uses when `--maxmemory` is
    /// given; without it the default is Redis's, which is no ceiling at all.
    /// The limit is a property of the pool rather than of each executor
    /// because the figure it bounds is the node's: a shard evicts when the
    /// *node* is full, not when its own dict is.
    ///
    /// # Panics
    ///
    /// If `shards` is zero — there would be nowhere to route a key — or if
    /// `executors` is not in `1..=shards`.
    pub fn spawn_limited<T: TraceSink>(
        shards: u16,
        executors: u16,
        seed: DictSeed,
        trace: T,
        limit: MemoryLimit,
    ) -> Self {
        Self::spawn_full(
            shards,
            executors,
            seed,
            trace,
            |_shard| NoopLog,
            Deadlines,
            limit,
        )
    }

    /// [`spawn`](ShardPool::spawn) with the replication log supplied per shard.
    ///
    /// `make_log` is called once per shard, with that shard's index, and the
    /// log it returns is owned by that shard task for its lifetime. A single
    /// writer shared by every shard is expressible too — return clones of one
    /// handle — which is what a group-commit implementation would do.
    ///
    /// This constructor is the reason the log seam is real rather than
    /// aspirational: with it, replacing [`NoopLog`] changes an argument at one
    /// call site and nothing else. Without it, `run_executor` would have to grow
    /// a type parameter and every caller would have to be revisited on the day a
    /// log first writes bytes — which is exactly the retrofit the seam exists
    /// to avoid.
    ///
    /// # Panics
    ///
    /// If `shards` is zero — there would be nowhere to route a key — or if
    /// `executors` is not in `1..=shards`.
    pub fn spawn_with_log<T, L, F>(
        shards: u16,
        executors: u16,
        seed: DictSeed,
        trace: T,
        make_log: F,
    ) -> Self
    where
        T: TraceSink,
        L: ReplicationLog,
        F: Fn(u16) -> L,
    {
        Self::spawn_full(
            shards,
            executors,
            seed,
            trace,
            make_log,
            Deadlines,
            MemoryLimit::default(),
        )
    }

    /// [`spawn`](ShardPool::spawn) with the executor's own decisions supplied.
    ///
    /// Callers are the simulator and this crate's own tests; no production
    /// path names it. See [`ShardPolicy`] for what the value decides and
    /// [`ExpiryPolicy`] for why the seam exists at all.
    ///
    /// # Panics
    ///
    /// If `shards` is zero, or if `executors` is not in `1..=shards`.
    pub fn spawn_with_policy<T, P>(
        shards: u16,
        executors: u16,
        seed: DictSeed,
        trace: T,
        policy: P,
    ) -> Self
    where
        T: TraceSink,
        P: ShardPolicy,
    {
        Self::spawn_full(
            shards,
            executors,
            seed,
            trace,
            |_shard| NoopLog,
            policy,
            MemoryLimit::default(),
        )
    }

    /// [`spawn_with_policy`](ShardPool::spawn_with_policy) with a ceiling as
    /// well.
    ///
    /// The one shape the other constructors cannot express, and the one the
    /// simulator's eviction runs need: a ceiling to be held under *and* the
    /// decision about reaching it supplied. Either alone leaves the eviction
    /// path either unreachable or honest, and a planted defect that never
    /// runs is a defect nothing measures.
    ///
    /// # Panics
    ///
    /// If `shards` is zero, or if `executors` is not in `1..=shards`.
    pub fn spawn_with_policy_limited<T, P>(
        shards: u16,
        executors: u16,
        seed: DictSeed,
        trace: T,
        policy: P,
        limit: MemoryLimit,
    ) -> Self
    where
        T: TraceSink,
        P: ShardPolicy,
    {
        Self::spawn_full(
            shards,
            executors,
            seed,
            trace,
            |_shard| NoopLog,
            policy,
            limit,
        )
    }

    /// The one constructor with every seam exposed; the public ones above are
    /// its defaults.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "every executor gets a clone of the sink and of the policy and the \
                  originals are dropped, but taking them and the log factory by value \
                  is what lets a caller move them in rather than keep them alive \
                  alongside the pool"
    )]
    fn spawn_full<T, L, F, P>(
        shards: u16,
        executors: u16,
        seed: DictSeed,
        trace: T,
        make_log: F,
        policy: P,
        limit: MemoryLimit,
    ) -> Self
    where
        T: TraceSink,
        L: ReplicationLog,
        F: Fn(u16) -> L,
        P: ShardPolicy,
    {
        assert!(
            shards > 0,
            "ShardPool::spawn: shards must be greater than zero"
        );
        assert!(
            executors > 0 && executors <= shards,
            "ShardPool::spawn: executors must be in 1..=shards"
        );

        // Built by walking the shards once in order: `executor_of` is monotone,
        // so each executor's states arrive contiguously and in ascending shard
        // order, which is what makes `first_shard` plus an offset enough to
        // address them.
        let memory = Memory {
            gauge: MemoryGauge::default(),
            limit,
        };
        let mut inboxes = Vec::with_capacity(usize::from(executors));
        let mut pending: Option<(u16, Vec<ShardState<L>>)> = None;
        for shard in 0..shards {
            let state = ShardState::new(
                Dict::with_seed(DictSeed {
                    k0: seed.k0 ^ u64::from(shard),
                    k1: seed.k1,
                }),
                make_log(shard),
            );
            // A fresh dict already costs its table, and the gauge is the sum
            // of what the dicts account — so it starts at the sum of the
            // empty ones rather than at zero.
            memory.gauge.apply(0, state.dict.used_bytes());
            match &mut pending {
                Some((first_shard, states))
                    if executor_of(shard, shards, executors)
                        == executor_of(*first_shard, shards, executors) =>
                {
                    states.push(state);
                }
                _ => {
                    if let Some((first_shard, states)) = pending.take() {
                        inboxes.push(spawn_executor(
                            first_shard,
                            states,
                            trace.clone(),
                            policy.clone(),
                            memory.clone(),
                        ));
                    }
                    pending = Some((shard, vec![state]));
                }
            }
        }
        if let Some((first_shard, states)) = pending {
            inboxes.push(spawn_executor(
                first_shard,
                states,
                trace,
                policy,
                memory.clone(),
            ));
        }

        Self {
            inboxes: Arc::new(inboxes),
            shards,
            executors,
            memory: memory.gauge,
            limit,
        }
    }

    /// How many virtual shards this pool spans.
    #[must_use]
    pub const fn shards(&self) -> u16 {
        self.shards
    }

    /// How many executor tasks host those shards.
    #[must_use]
    pub const fn executors(&self) -> u16 {
        self.executors
    }

    /// The node-wide memory figure the executors keep current.
    #[must_use]
    pub fn memory(&self) -> MemoryGauge {
        self.memory.clone()
    }

    /// The ceiling this pool holds its keyspace under, and what it does at it.
    #[must_use]
    pub const fn limit(&self) -> MemoryLimit {
        self.limit
    }

    /// The shard a keyed or shard-addressed command belongs to, or `None`
    /// where this pool has no single answer.
    ///
    /// `None` covers three different things and answers all of them the same
    /// way, because a caller on the one-reply path can do nothing else with
    /// any of them: a shard named outside this pool's range,
    /// [`Route::Every`], which the broadcast path handles before this is
    /// asked, and [`Route::Unaddressed`], whose shard is the caller's to name
    /// through [`Router::dispatch_at`].
    fn shard_for(&self, cmd: &Command) -> Option<u16> {
        match cmd.route() {
            Route::Key(key) => Some(shard_of(key, self.shards)),
            Route::Shard(shard) if shard < self.shards => Some(shard),
            Route::Shard(_) | Route::Every | Route::Unaddressed => None,
        }
    }

    /// Sends `cmd` to the executor hosting `shard`, and hands back the channel
    /// its reply will arrive on.
    ///
    /// `None` where this pool has no such shard or its executor is gone —
    /// [`one_reply`] turns both into `ShardUnavailable`, because a caller on
    /// the one-reply path can do nothing else with either.
    ///
    /// The send happens here rather than inside the future the caller awaits.
    /// That is the behaviour [`Router::dispatch`]'s cancellation note
    /// describes, and it is why this returns a receiver rather than a future.
    fn send_one(&self, shard: u16, cmd: Command) -> Option<oneshot::Receiver<Vec<Reply>>> {
        if shard >= self.shards {
            return None;
        }
        let executor = usize::from(executor_of(shard, self.shards, self.executors));
        let (tx, rx) = oneshot::channel();
        // The inbox is unbounded, so this never blocks and the caller cannot
        // deadlock by holding the future.
        self.inboxes[executor]
            .send(Envelope {
                cmds: vec![(shard, cmd)],
                reply: tx,
            })
            .is_ok()
            .then_some(rx)
    }
}

/// Resolves the reply channel of a one-command envelope.
///
/// A shard this pool does not have, an executor that would not take the
/// envelope, and an answer that is not exactly one reply are all the same
/// thing from here — the command did not run and nothing came back — so they
/// answer alike.
async fn one_reply(pending: Option<oneshot::Receiver<Vec<Reply>>>) -> Reply {
    let Some(rx) = pending else {
        return Reply::Error(ReplyError::ShardUnavailable);
    };
    match rx.await {
        Ok(mut replies) if replies.len() == 1 => replies.pop().expect("checked non-empty"),
        _ => Reply::Error(ReplyError::ShardUnavailable),
    }
}

/// Spawns one executor task and returns the inbox that reaches it.
fn spawn_executor<T: TraceSink, L: ReplicationLog, P: ShardPolicy>(
    first_shard: u16,
    states: Vec<ShardState<L>>,
    trace: T,
    policy: P,
    memory: Memory,
) -> mpsc::UnboundedSender<Envelope> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(run_executor(first_shard, states, trace, policy, memory, rx));
    tx
}

impl Router for ShardPool {
    fn dispatch(&self, cmd: Command) -> impl Future<Output = Reply> + Send {
        // A command with no single shard to go to is refused here rather than
        // sent somewhere defensible-looking: there is no shard whose answer
        // would be the right one.
        let shard = self.shard_for(&cmd);
        one_reply(shard.and_then(|shard| self.send_one(shard, cmd)))
    }

    /// The count this pool was spawned with — see [`ShardPool::shards`], the
    /// inherent accessor this restates for callers that hold a `Router`.
    fn shards(&self) -> u16 {
        self.shards
    }

    /// The shard comes from the caller instead of from the command, and the
    /// range check that [`shard_for`](ShardPool::shard_for) would have applied
    /// is kept: this argument reaches here from a cursor a client supplied.
    fn dispatch_at(&self, shard: u16, cmd: Command) -> impl Future<Output = Reply> + Send {
        one_reply(self.send_one(shard, cmd))
    }

    fn dispatch_many(&self, cmds: Vec<Command>) -> impl Future<Output = Vec<Reply>> + Send {
        let executors = usize::from(self.executors);
        // Index-addressed buckets: iteration order is the executor order by
        // construction, which is what keeps this path free of any map
        // iteration — and so free of an iteration order that could differ
        // between two runs of the same seed.
        let mut buckets: Vec<Vec<(u16, Command)>> = Vec::new();
        buckets.resize_with(executors, Vec::new);
        // Where each command's reply will be found once the executors answer,
        // or `None` for a command this pool has no shard for — which keeps its
        // place in the batch and is answered without anything being sent.
        let mut positions: Vec<Option<(usize, usize)>> = Vec::with_capacity(cmds.len());
        for cmd in cmds {
            let Some(shard) = self.shard_for(&cmd) else {
                positions.push(None);
                continue;
            };
            let executor = usize::from(executor_of(shard, self.shards, self.executors));
            positions.push(Some((executor, buckets[executor].len())));
            buckets[executor].push((shard, cmd));
        }

        // Scattered at call time, exactly as `dispatch` sends at call time: an
        // executor starts on its bucket while the others are still being sent.
        let mut pending: Vec<Option<oneshot::Receiver<Vec<Reply>>>> =
            std::iter::repeat_with(|| None).take(executors).collect();
        for (executor, cmds) in buckets.into_iter().enumerate() {
            if cmds.is_empty() {
                continue;
            }
            let (tx, rx) = oneshot::channel();
            if self.inboxes[executor]
                .send(Envelope { cmds, reply: tx })
                .is_ok()
            {
                pending[executor] = Some(rx);
            }
        }

        async move {
            // Gathered in executor-index order — a fixed order, not the order
            // the answers happened to arrive in.
            let mut answered: Vec<Vec<Option<Reply>>> = Vec::with_capacity(pending.len());
            for rx in pending {
                answered.push(match rx {
                    Some(rx) => rx
                        .await
                        .map(|replies| replies.into_iter().map(Some).collect())
                        .unwrap_or_default(),
                    None => Vec::new(),
                });
            }
            positions
                .into_iter()
                .map(|position| {
                    position
                        .and_then(|(executor, offset)| {
                            answered[executor].get_mut(offset).and_then(Option::take)
                        })
                        .unwrap_or(Reply::Error(ReplyError::ShardUnavailable))
                })
                .collect()
        }
    }

    fn dispatch_every(&self, cmd: Command) -> impl Future<Output = Vec<Reply>> + Send {
        let shards = self.shards;
        let executors = usize::from(self.executors);
        // One bucket per executor, each carrying that executor's shards in
        // ascending order, so a bucket's replies come back in shard order and
        // the gather below is a concatenation rather than a sort.
        let mut buckets: Vec<Vec<(u16, Command)>> = Vec::new();
        buckets.resize_with(executors, Vec::new);
        for shard in 0..shards {
            let executor = usize::from(executor_of(shard, shards, self.executors));
            buckets[executor].push((shard, cmd.clone()));
        }

        // Scattered at call time, for the reason `dispatch_many` scatters at
        // call time: an executor starts on its shards while the others are
        // still being sent.
        let mut pending: Vec<Option<oneshot::Receiver<Vec<Reply>>>> =
            std::iter::repeat_with(|| None).take(executors).collect();
        for (executor, cmds) in buckets.into_iter().enumerate() {
            if cmds.is_empty() {
                continue;
            }
            let (tx, rx) = oneshot::channel();
            if self.inboxes[executor]
                .send(Envelope { cmds, reply: tx })
                .is_ok()
            {
                pending[executor] = Some(rx);
            }
        }

        async move {
            let mut replies = Vec::with_capacity(usize::from(shards));
            for rx in pending.into_iter().flatten() {
                match rx.await {
                    Ok(answers) => replies.extend(answers),
                    Err(_) => replies.push(Reply::Error(ReplyError::ShardUnavailable)),
                }
            }
            // Executors own contiguous ascending shard ranges, so concatenating
            // their answers in executor order is shard order — while every
            // executor answers. An executor whose channel errored contributes
            // one `ShardUnavailable` instead of one per shard it owned, and one
            // whose send failed leaves its slot `None` for `.flatten()` to drop
            // and contributes nothing; either way the `resize` below pads at
            // the tail, so a death mid-flight shortens one run and shifts every
            // reply behind it: the vector is still the right length and every
            // entry is still a reply this pool produced, but index `i` is no
            // longer shard `i`. Nothing relies on the correspondence today —
            // `broadcast` sums or folds and no caller indexes by shard — and a
            // live pool cannot get there, so this is stated rather than
            // defended against. A caller that does want to read replies
            // positionally has to make the padding per-executor first.
            //
            // A short answer means an executor died mid-flight; pad rather
            // than return a length the caller cannot interpret.
            replies.resize(
                usize::from(shards),
                Reply::Error(ReplyError::ShardUnavailable),
            );
            replies
        }
    }
}

/// What an executor needs to know about the node's memory: the figure, and
/// what to do when it is too large.
///
/// One argument rather than two because [`run_executor`] and
/// [`spawn_executor`] were already at the parameter count where the next one
/// is a list nobody reads. They also always travel together: a gauge with no
/// ceiling to compare against decides nothing, and a ceiling with no gauge
/// has nothing to compare.
#[derive(Clone, Debug, Default)]
struct Memory {
    /// The node-wide figure every executor keeps current.
    gauge: MemoryGauge,
    /// The ceiling that figure is held under, and how.
    limit: MemoryLimit,
}

/// One virtual shard's state: what used to be one task's locals.
struct ShardState<L> {
    dict: Dict,
    seq: u64,
    log: L,
    /// Where the active expiry sweep resumes on the next housekeeping tick.
    ///
    /// A [`Dict::scan`] cursor, and it belongs to the shard rather than to the
    /// dict for the same reason a `SCAN` command's does: the dict offers a
    /// position, and what keeps a position between calls is whoever is walking.
    expire_cursor: u64,
    /// Where the eviction sample resumes, kept for the reason the sweep keeps
    /// its own: successive samples that all started at `0` would re-read one
    /// corner of the table and offer the same few keys up every time.
    ///
    /// Separate from [`expire_cursor`](Self::expire_cursor) on purpose. The
    /// two walks have nothing to do with each other, and sharing one position
    /// would make how often a key is offered for eviction depend on how many
    /// deadlines the keyspace happens to carry.
    evict_cursor: u64,
    /// How many entries this shard has evicted, for `INFO`'s `evicted_keys`.
    evicted: u64,
    /// Keyed lookups that found a live entry, for `INFO`'s `keyspace_hits`.
    hits: u64,
    /// Keyed lookups that found nothing, for `INFO`'s `keyspace_misses`.
    misses: u64,
    /// Entries reclaimed because their deadline had passed, by either half of
    /// expiration, for `INFO`'s `expired_keys`.
    expired: u64,
    /// How many commands of each [`Command::kind`] this shard has run, indexed
    /// by the tag itself; slot `0` is no command and stays zero.
    ///
    /// Plain `u64`s rather than atomics because one executor task owns this
    /// shard and nothing else may touch it — the counters a scrape reads are
    /// gathered by a [`Command::Stats`] like any other command, which is what
    /// keeps `INFO` off the hot path entirely.
    calls: [u64; KIND_SLOTS],
    /// Microseconds those commands spent, indexed the same way — see
    /// [`ShardStats::usec`], which is what this becomes.
    usec: [u64; KIND_SLOTS],
}

impl<L> ShardState<L> {
    /// A shard's state at the moment it starts: an empty keyspace, a log at
    /// position zero, and every counter unspent.
    const fn new(dict: Dict, log: L) -> Self {
        Self {
            dict,
            seq: 0,
            log,
            expire_cursor: 0,
            evict_cursor: 0,
            evicted: 0,
            hits: 0,
            misses: 0,
            expired: 0,
            calls: [0; KIND_SLOTS],
            usec: [0; KIND_SLOTS],
        }
    }
}

/// One executor task: own a contiguous range of shards, answer the inbox,
/// keep every owned rehash moving.
///
/// `states` holds the range's shards in ascending order starting at
/// `first_shard`, so a command's shard id indexes it by subtraction.
///
/// Returns when the inbox closes, which happens once the last [`ShardPool`]
/// handle is dropped.
async fn run_executor<T: TraceSink, L: ReplicationLog, P: ShardPolicy>(
    first_shard: u16,
    mut states: Vec<ShardState<L>>,
    trace: T,
    policy: P,
    memory: Memory,
    mut inbox: mpsc::UnboundedReceiver<Envelope>,
) {
    let mut tick = tokio::time::interval(HOUSEKEEPING_TICK);
    // A shard that fell behind resumes at its normal spacing instead of firing
    // a burst of catch-up ticks. The default is that burst, and it is the last
    // thing a shard that has just been saturated needs: it would meet the end
    // of a stall with every tick the stall cost, back to back, ahead of the
    // work that piled up behind them.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval` yields its first tick immediately; consume it so the first
    // real tick is one period away.
    tick.tick().await;

    loop {
        tokio::select! {
            // `biased` removes the runtime's RNG from this loop. Without it
            // `select!` picks among ready arms at random, seeded from OS
            // entropy — turmoil does seed tokio's runtime RNG, but only under
            // `--cfg tokio_unstable`, which this workspace does not set. That
            // is unseeded entropy inside the one crate whose whole premise is
            // that a seed reproduces a run, and `clippy.toml`'s entropy
            // prohibitions cannot see it: they match paths, not macro
            // internals.
            //
            // This used to be a precaution: the losing arm only advanced a
            // rehash, which reached no reply and no trace field, so an
            // unbiased choice was entropy nothing could observe. That stopped
            // being true with [`Command::ScanStep`]. A step's answer — both
            // its cursor and the keys it found — depends on whether a rehash
            // is in flight and how far it has run, and the simulator folds
            // both into the trace hash. So an unbiased arm choice would now
            // change a walk's answer between two runs of one seed, which is
            // the whole thing a seed is supposed to prevent. `biased` is
            // load-bearing here, not decorative; do not remove it.
            //
            // Draining the inbox first is also the right priority on its own
            // merits: work the shard was asked for outranks housekeeping.
            biased;

            envelope = inbox.recv() => {
                let Some(Envelope { mut cmds, reply }) = envelope else {
                    break;
                };
                // No `await` inside this loop, so a batch is applied as a unit:
                // nothing from another connection lands between its commands.
                let mut replies = Vec::with_capacity(cmds.len());
                // One clock reading for the whole envelope, taken here rather
                // than inside a handler. A handler that read the clock itself
                // would still be synchronous — this is not the no-await rule —
                // but the commands of one batch would then expire keys against
                // several different instants, which is a difference nothing
                // about the batch justifies.
                //
                // It is also the first command's timing start — see
                // `ShardStats::usec`, which spends one further reading per
                // command and differences each against the one before it.
                let now = Instant::now();
                let mut last = now;
                // By mutable reference, so a handler can move a command's value
                // into the dict instead of copying it — see `apply`. The trace
                // reads the command *after* the handler has had it, and reads
                // only fields no handler takes.
                for (shard, cmd) in &mut cmds {
                    let state = &mut states[usize::from(*shard - first_shard)];
                    let at = state.seq;
                    // Three ways a command is answered, and only the last one
                    // reaches a handler.
                    //
                    // `Stats` is answered here rather than in `apply` because
                    // what it reports — the eviction count — lives beside the
                    // dict and not in it, and giving `apply` an arm that
                    // could only ever answer half the fields would be an arm
                    // whose answer is wrong.
                    //
                    // The refusal is here for the same reason in the other
                    // direction: whether a write is over the ceiling is a
                    // question about the node, and `apply` is deliberately a
                    // function of one shard's own state.
                    let answer = if matches!(cmd, Command::Stats) {
                        Reply::Stats(Box::new(stats_of(state)))
                    } else if memory.limit.mode == EvictionMode::NoEviction
                        && cmd.denied_when_full()
                        && memory.limit.exceeded(memory.gauge.used())
                    {
                        Reply::Error(ReplyError::OutOfMemory)
                    } else {
                        // The gauge is the sum of what the dicts account, so
                        // it is moved by the difference this command made to
                        // one of them. Read either side of `apply` rather
                        // than inside it: the handlers stay unaware there is
                        // a gauge at all.
                        let before = state.dict.used_bytes();
                        let answer = apply(state, *shard, cmd, now, &policy);
                        memory.gauge.apply(before, state.dict.used_bytes());
                        if memory.limit.mode == EvictionMode::AllKeysLru {
                            // The command's key survives this. `apply` takes
                            // a command's value but never its key — the trace
                            // reads it after — so the route still names it.
                            let spared = match cmd.route() {
                                Route::Key(key) => Some(key),
                                Route::Shard(_) | Route::Every | Route::Unaddressed => None,
                            };
                            evict_until_fits(state, *shard, &memory, &trace, &policy, spared);
                        }
                        answer
                    };
                    // A reading per command, differenced against the one
                    // before it, so a batch of `n` commands costs `n`
                    // readings rather than `2n`.
                    //
                    // `saturating_duration_since` rather than a subtraction:
                    // a monotonic clock is only promised not to go backwards,
                    // and a reading that did would panic here rather than
                    // report a zero microsecond nobody would miss.
                    let spent = {
                        let after = Instant::now();
                        let spent = after.saturating_duration_since(last).as_micros();
                        last = after;
                        // A command that ran for half a million years would
                        // saturate; the shard it ran on has other problems.
                        u64::try_from(spent).unwrap_or(u64::MAX)
                    };
                    count_call(state, cmd, &answer, spent);
                    trace.record(*shard, at, cmd, &answer);
                    replies.push(answer);
                }
                // The caller may have gone away; its replies are simply dropped.
                let _ = reply.send(replies);
            }
            // One ticker per executor rather than one per shard, advancing
            // every owned dict by the same budget: the same per-dict drain
            // rate, and the same aggregate work, as independent tickers.
            _ = tick.tick() => {
                // One clock reading for the whole tick, for the reason the
                // envelope arm takes one for the whole batch: the shards of an
                // executor should not disagree about which keys this tick found
                // expired.
                let now = Instant::now();
                for (offset, state) in states.iter_mut().enumerate() {
                    // One reading either side of the whole tick's work: the
                    // rehash step changes what the tables cost and the sweep
                    // changes what the entries do, and the gauge only cares
                    // about the sum.
                    let before = state.dict.used_bytes();
                    state.dict.rehash_step(REHASH_BUCKETS_PER_TICK);
                    // The inverse of the envelope arm's `shard - first_shard`:
                    // these states were built from a `0..shards` walk in
                    // ascending order, so a range's offsets are shard ids and
                    // fit the `u16` a shard id is.
                    let shard = first_shard
                        + u16::try_from(offset).expect("a shard range is shorter than u16::MAX");
                    sweep_expired(state, shard, &trace, now, &policy);
                    memory.gauge.apply(before, state.dict.used_bytes());
                    // The durability point, and the only place in a shard that
                    // can afford to be one: `append` runs inside a handler that
                    // cannot `await`, so it must stay cheap, while this arm is
                    // already async and may block. That split is why the trait
                    // has two methods rather than one.
                    //
                    // The cadence is the tick's, which is a starting shape
                    // rather than a policy — a real log picks its own, and may
                    // want group commit across shards instead. The error has
                    // nowhere to go until this project has somewhere to report
                    // to; a log that cannot sync is a problem for the release
                    // that gives it bytes to write, and an answer from there too.
                    let _ = state.log.sync();
                }
            }
        }
    }
}

/// Reclaims a budget's worth of expired keys from one shard, logging and
/// tracing each removal exactly as an explicit `Del` is.
///
/// The active half of expiration, and the half [`evict_if_expired`] cannot be:
/// a key nothing ever addresses again meets no command, so nothing lazy can
/// reclaim it. Together they are the guarantee — a key stops being visible at
/// its deadline, and stops costing memory shortly after.
///
/// A removal here is a keyspace mutation like any other, so it takes a
/// replication position and appends its record *before* the entry goes, and it
/// reaches the [`TraceSink`] as the `Del` it amounts to. Nothing downstream has
/// to know a sweep exists.
///
/// An append that fails abandons the rest of this tick's budget and leaves the
/// cursor where it was: the entries keep their deadlines, and the same buckets
/// are swept again on the next tick. Skipping them would mean waiting a whole
/// cycle to retry a key the log has already refused to let go.
fn sweep_expired<T: TraceSink, L: ReplicationLog, P: ExpiryPolicy>(
    state: &mut ShardState<L>,
    shard: u16,
    trace: &T,
    now: Instant,
    expiry: &P,
) {
    let (next, dead) =
        state
            .dict
            .expire_step(state.expire_cursor, EXPIRE_BUCKETS_PER_TICK, now, expiry);
    for key in dead {
        let at = state.seq;
        if append(&mut state.log, &mut state.seq, shard).is_err() {
            return;
        }
        state.dict.remove(&key);
        // The active half of `expired_keys`; the lazy half is counted in
        // [`apply`], in front of the command that met the key.
        state.expired += 1;
        // The command an expiry is indistinguishable from. Built after the
        // removal because it takes the key, which is what keeps the sweep from
        // cloning it to say the same thing twice.
        trace.record(shard, at, &Command::Del { key }, &Reply::Removed(true));
    }
    state.expire_cursor = next;
}

/// Reclaims from this shard while the policy says the node must, one sampled
/// victim at a time, stopping when it says otherwise or the shard is empty.
///
/// The condition is [`EvictionPolicy::must_evict`] rather than the comparison
/// it stands for, so that the decision itself is the thing a plant replaces —
/// see the trait for why that is worth a seam.
///
/// Synchronous, inside the executor loop, for the reason every handler is: a
/// write that has to make room does so before its reply is sent, and nothing
/// on this executor interleaves with it. What it costs is latency on the
/// write that crossed the line, bounded by how many samples it takes to get
/// back under — the shape an operator reads in `ARCHITECTURE.md`.
///
/// Each removal is a keyspace mutation: it takes a replication position,
/// appends first, and reaches the trace as the `Del` it amounts to — the
/// discipline [`sweep_expired`] set.
///
/// **The ceiling is the node's and the victims are this shard's**, so this
/// terminates on this shard running out of keys to give rather than on the
/// figure: a value larger than the whole ceiling leaves the node over it
/// until something reclaims elsewhere. Looping instead would be a shard
/// spinning against bytes it does not own.
///
/// `spared` is the key the command that triggered this addressed, and it is
/// never the victim. Redis reaches the same place from the other side: it
/// evicts *before* running the command, so the key being written does not yet
/// exist to be chosen. Evicting here is what lets the decision be taken
/// against the figure the write actually produced, and sparing the key is
/// what keeps that from meaning a write can free room by undoing itself —
/// which on a value larger than the ceiling is the difference between storing
/// it and storing nothing.
fn evict_until_fits<T: TraceSink, L: ReplicationLog, P: EvictionPolicy>(
    state: &mut ShardState<L>,
    shard: u16,
    memory: &Memory,
    trace: &T,
    policy: &P,
    spared: Option<&[u8]>,
) {
    while policy.must_evict(memory.gauge.used(), memory.limit.ceiling) {
        // `spared` goes to the sampler rather than being checked against
        // what comes back: a sample that met the spared key and rejected it
        // afterwards would answer `None` — the shard saying it has nothing to
        // give — from a shard with plenty left. Excluded there, `None` keeps
        // meaning what the empty case above means, and stopping stays right.
        let Some(victim) =
            state
                .dict
                .sample_oldest(&mut state.evict_cursor, EVICTION_SAMPLES, spared)
        else {
            return;
        };
        let at = state.seq;
        if append(&mut state.log, &mut state.seq, shard).is_err() {
            return;
        }
        let before = state.dict.used_bytes();
        state.dict.remove(&victim);
        memory.gauge.apply(before, state.dict.used_bytes());
        state.evicted += 1;
        // The command an eviction is indistinguishable from, built after the
        // removal for the reason the sweep builds its own there: it takes the
        // key rather than cloning it to say the same thing twice.
        trace.record(
            shard,
            at,
            &Command::Del { key: victim },
            &Reply::Removed(true),
        );
    }
}

/// The half of [`ShardStats`] the dict alone can state: a reading of the
/// keyspace as it stands, rather than a total the shard has been running.
///
/// Above [`apply`] with the executor's helpers rather than below it with the
/// dispatcher's, because it has two callers and the executor is the one that
/// matters: [`stats_of`] fills in the counters this cannot see, and `apply`'s
/// own `Stats` arm — which nothing routes to — answers with this alone.
///
/// # Panics
///
/// If a `usize` does not fit a `u64`, which no target this builds for has.
fn keyspace_stats(dict: &Dict) -> ShardStats {
    ShardStats {
        keys: u64::try_from(dict.len()).expect("a length is a usize"),
        expires: u64::try_from(dict.with_deadline()).expect("a count is a usize"),
        ..ShardStats::default()
    }
}

/// Counts one completed command against this shard's totals.
///
/// **A command that reaches every shard is not counted here**, and that is the
/// difference between a figure and a multiple of one: `DBSIZE`, `FLUSHDB` and
/// the [`Command::Stats`] an `INFO` gathers with are one request each, split
/// into one command per shard by the edge, so counting them where they land
/// would report a single `DBSIZE` as sixteen. The edge counts those, once, as
/// the requests they were. Everything else reaches exactly one shard per
/// request, so this is the only place that has to count it — and it is the
/// cheap place, because these are plain `u64`s on a state one task owns.
///
/// The refusals are counted with the successes. A command the peer sent and
/// the server answered is a call however it was answered, which is what Redis
/// counts and what an operator comparing `commandstats` to a client's own
/// tally is looking for.
///
/// `spent` is what the command cost in microseconds, and it is added here
/// rather than beside the reading that produced it so that the two figures
/// cannot disagree about which commands they describe: the one guard above
/// decides both. A command excluded from `calls` contributes no time either,
/// which is what keeps `usec_per_call` a quotient of two figures counted over
/// the same set. It is always `0` on a build with the timing compiled out.
fn count_call<L>(state: &mut ShardState<L>, cmd: &Command, reply: &Reply, spent: u64) {
    if !matches!(cmd.route(), Route::Every) {
        state.calls[usize::from(cmd.kind())] += 1;
        state.usec[usize::from(cmd.kind())] += spent;
    }
    match read_outcome(&state.dict, cmd, reply) {
        Some(true) => state.hits += 1,
        Some(false) => state.misses += 1,
        None => {}
    }
}

/// Whether a command that *read* a key found one, or `None` if it read none.
///
/// Redis counts `keyspace_hits` and `keyspace_misses` over the lookups that
/// read a value rather than over every command, which here is `GET`, `EXISTS`,
/// `TTL`, `TYPE` and `STRLEN` — a write is not a lookup, and neither is a
/// command that names no key.
///
/// **This is not the same set as the one that refreshes the LRU stamp**, and
/// the difference is deliberate rather than an oversight. Five commands are
/// counted here; three stamp — `GET`, `SET` and `INCRBY` — plus `STRLEN`,
/// which is the only lookup of the five that reads the value. Redis draws the
/// same two lines in the same two places, measured with `OBJECT IDLETIME`
/// against 6.2.24: `EXISTS`, `TYPE` and `TTL` are counted as lookups and
/// leave the stamp alone. A hit is about what an operator is told about the
/// keyspace; a stamp is about what eviction may take next.
///
/// Read off the reply wherever the reply says, so the classification costs
/// nothing: four of the five answer differently for a key that was there.
/// `STRLEN` is the exception — a stored empty value and an absent key both
/// measure `0` — and that one case asks the dict, which is a second hash on a
/// command nothing on the hot path issues, rather than a miss counted against
/// a key that was present.
fn read_outcome(dict: &Dict, cmd: &Command, reply: &Reply) -> Option<bool> {
    match (cmd, reply) {
        (Command::Get { .. }, Reply::Bulk(value)) => Some(value.is_some()),
        (Command::Exists { .. }, Reply::Integer(found)) => Some(*found == 1),
        // `-2` is `TTL`'s answer for a key that is not there; `-1` and every
        // span above it are answers about a key that is.
        (Command::Ttl { .. }, Reply::Integer(remaining)) => Some(*remaining != -2),
        (Command::Type { .. }, Reply::Status(name)) => Some(*name != "none"),
        (Command::StrLen { key }, Reply::Integer(len)) => Some(*len > 0 || dict.get(key).is_some()),
        _ => None,
    }
}

/// What this shard has counted, as [`Command::Stats`] reports it.
///
/// The keyspace figures are read from the dict here and now; the rest are
/// running totals the shard has been maintaining. Nothing is computed across
/// shards — summing is the edge's, because only the edge has every answer.
fn stats_of<L>(state: &ShardState<L>) -> ShardStats {
    ShardStats {
        evicted: state.evicted,
        hits: state.hits,
        misses: state.misses,
        expired: state.expired,
        calls: state.calls,
        usec: state.usec,
        ..keyspace_stats(&state.dict)
    }
}

/// Runs one command against a shard's own state.
///
/// **This is deliberately not `async`.** See the module documentation: the
/// signature is what guarantees a command cannot be suspended halfway.
///
/// A mutation appends its log record *before* touching the dict, so a record
/// can never describe a change that was not also made; a command that will
/// not change anything — a `Del` of a missing key, a rejected `IncrBy` —
/// appends nothing, which keeps the log free of records that replay to a
/// no-op. `seq` advances only when a record is appended, so the log a shard
/// produces is gapless.
///
/// **The command is taken apart, not read.** A value stored under a key is
/// *moved* out of the command and into the dict rather than copied — on the
/// write path that is the difference between one copy of a payload and two, and
/// the payload is the largest thing a peer can send. The key cannot be treated
/// that way: [`TraceSink`] observes the command after this call and folds its
/// key, so the key is cloned and the command's own copy is left intact. What a
/// handler may take is exactly what the trace does not read.
///
/// `now` is the instant the whole envelope is being served at, supplied by the
/// executor: a handler must not read a clock of its own, or two commands of
/// one batch could disagree about which keys are still alive.
///
/// **An arm whose logic is extracted gets a named free function directly
/// below, in match order.** Plenty of arms are not extracted and carry real
/// decisions anyway — `Del`, `Ttl` and `Persist` all do — because a handler
/// that says no more than the arm already says buys a name and a signature
/// for nothing. What earns an extraction is one of three things: logic two
/// arms share, as `Expire` and `PExpire` share [`set_expiry`]; an argument
/// list the dispatcher would otherwise have to assemble inline, as `Set` and
/// `SetEx` both do through [`SetArgs`]; or an arm long enough that leaving it here costs the
/// reader the dispatcher, which is the judgement `clippy::too_many_lines`
/// makes on our behalf. `IncrBy` was the nearest to that third case and is
/// now [`incr_by`], extracted on the commit that gave the write paths their
/// LRU stamp and took the dispatcher one line over the limit. `Del` is now
/// the nearest.
///
/// The ordering is the half with no judgement in it, and it is what the next
/// extraction has to respect: every extracted handler sits below in the order
/// the match dispatches, so following a command from its arm to its handler is
/// a pass rather than a search. The helpers that belong to no single arm —
/// [`evict_if_expired`], [`deadline`], [`remaining_seconds`], [`append`] —
/// follow those handlers as their own group.
fn apply<L: ReplicationLog, P: ShardPolicy>(
    state: &mut ShardState<L>,
    shard: u16,
    cmd: &mut Command,
    now: Instant,
    policy: &P,
) -> Reply {
    // The three the handlers work on, plus the one counter a handler can
    // move. Destructured rather than reached through `state` field by field so
    // that the borrows stay disjoint, and taken as a whole rather than as four
    // parameters because four more would put this function past the argument
    // ceiling this workspace sets.
    let ShardState {
        dict,
        log,
        seq,
        expired,
        ..
    } = state;
    // Lazy expiry, once, before any arm has looked at the key. Here rather
    // than in each arm on purpose: it makes "an expired key is dead to every
    // command" a property of the dispatch instead of a rule the handlers have
    // to remember, and a command added later inherits it without knowing it
    // exists.
    //
    // A command that names no key has nothing for this to stand in front of.
    // That is not a gap in the guarantee: such a command addresses the shard
    // rather than an entry, so there is no single key whose deadline it could
    // be meeting.
    if let Route::Key(key) = cmd.route() {
        match evict_if_expired(dict, log, seq, shard, key, now, policy) {
            Err(failed) => return failed,
            // The lazy half of `expired_keys`. The active half is
            // [`sweep_expired`]'s, and Redis counts both under the one field.
            Ok(reclaimed) => *expired += u64::from(reclaimed),
        }
    }

    match cmd {
        // A read is a use, so it stamps the key exactly as a write does:
        // `allkeys-lru` is about what the keyspace is *using*, and a cache
        // whose hot keys are read and never written would otherwise offer its
        // whole working set up for eviction.
        Command::Get { key } => {
            dict.touch(key);
            Reply::Bulk(dict.get(key).map(|entry| entry.value.clone()))
        }

        Command::Set {
            key,
            value,
            expiry,
            cond,
            keep_ttl,
            get,
        } => set(
            dict,
            log,
            seq,
            shard,
            now,
            SetArgs {
                key,
                value,
                expiry: *expiry,
                cond: *cond,
                keep_ttl: *keep_ttl,
                get: *get,
            },
        ),

        Command::SetEx {
            key,
            seconds,
            value,
        } => {
            let args = SetArgs::set_ex(key, value, *seconds);
            set(dict, log, seq, shard, now, args)
        }

        Command::Del { key } => {
            if dict.get(key).is_none() {
                return Reply::Removed(false);
            }
            if let Err(failed) = append(log, seq, shard) {
                return failed;
            }
            dict.remove(key);
            Reply::Removed(true)
        }

        Command::IncrBy { key, delta } => incr_by(dict, log, seq, shard, key, *delta),

        Command::Expire { key, seconds } => {
            let at = span_deadline(now, *seconds, Expiry::Ex);
            set_expiry(dict, log, seq, shard, key, at)
        }

        Command::PExpire { key, millis } => {
            let at = span_deadline(now, *millis, Expiry::Px);
            set_expiry(dict, log, seq, shard, key, at)
        }

        Command::Ttl { key } => match dict.get(key).map(|entry| entry.expires_at) {
            None => Reply::Integer(-2),
            Some(None) => Reply::Integer(-1),
            Some(Some(at)) => Reply::Integer(remaining_seconds(at, now)),
        },

        Command::Persist { key } => {
            // Two keys answer `0` here for two different reasons: one is not
            // there, and one is but carries no deadline. Neither answer is a
            // change, so neither reaches the log.
            if dict.get(key).is_none_or(|entry| entry.expires_at.is_none()) {
                return Reply::Integer(0);
            }
            if let Err(failed) = append(log, seq, shard) {
                return failed;
            }
            dict.set_deadline(key, None);
            Reply::Integer(1)
        }

        Command::Exists { key } => Reply::Integer(i64::from(dict.get(key).is_some())),

        Command::Type { key } => Reply::Status(type_name(dict, key)),

        // Stamped, like `GET` and unlike the other three lookups: measured
        // against `redis:6-alpine` (6.2.24) with `OBJECT IDLETIME`, which
        // resets on `GET` and `STRLEN` and does not on `EXISTS`, `TYPE` or
        // `TTL`. Redis draws the line at whether the command read the *value*
        // — `STRLEN` measures it, the other three only ask about the key —
        // and this follows that line rather than the one `read_outcome` draws
        // beside it. The two are deliberately different questions: what
        // counts as a keyspace lookup is about what an operator is told, and
        // what refreshes the stamp is about what eviction may take.
        Command::StrLen { key } => {
            dict.touch(key);
            Reply::Integer(value_len(dict, key))
        }

        Command::FlushDb => flush_db(dict, log, seq, shard),

        // The count includes keys whose deadline has passed but which the
        // sweep has not reached, exactly as Redis's does. Making this walk the
        // dict to exclude them would turn an `O(1)` call into an
        // `O(keyspace)` one, to report a number that is stale the instant it
        // is computed.
        Command::DbSize => Reply::Integer(i64::try_from(dict.len()).unwrap_or(i64::MAX)),

        Command::ScanStep {
            cursor,
            count,
            pattern,
        } => scan_step(dict, *cursor, *count, pattern.as_deref(), now, policy),

        // Answered by the executor, which owns the counters this reports —
        // see `run_executor`, which matches it before this is reached. The
        // arm exists because the match has no wildcard, and it answers the
        // half the dict can state and zero for the half it cannot. Routing
        // one here would be a wiring mistake, and this is what it would cost:
        // an `INFO` that reads low, never a panicked executor and never a
        // figure nothing produced.
        Command::Stats => Reply::Stats(Box::new(keyspace_stats(dict))),
    }
}

/// A [`Command::Set`]'s pieces, as [`set`] receives them.
///
/// A bundle rather than six parameters because six is more than the ceiling
/// this workspace sets on an argument list, and because they arrive together
/// or not at all: they are one command, taken apart by the match that
/// dispatched it. The key and the value stay borrowed from that command — the
/// value mutably, so the handler can take the bytes rather than copy them.
struct SetArgs<'a> {
    /// The key to write.
    key: &'a Vec<u8>,
    /// The bytes to store, taken by the write that stores them.
    value: &'a mut Vec<u8>,
    /// How long the key should live.
    expiry: Option<Expiry>,
    /// The condition the write is subject to.
    cond: Option<Cond>,
    /// Whether the key keeps the deadline it already had.
    keep_ttl: bool,
    /// Whether the reply is the value the write replaced.
    get: bool,
}

impl<'a> SetArgs<'a> {
    /// What `SETEX key seconds value` comes to: an unconditional store with a
    /// deadline in seconds and every other option off.
    ///
    /// A constructor rather than a struct literal in the dispatcher, for the
    /// second of the three reasons [`apply`] gives for moving code out of an
    /// arm: the arm's whole content was an argument list assembled inline, and
    /// six fields of it read as a decision at each one when only the deadline
    /// is a decision at all. Naming the four that are off here says once, in
    /// the place that owns the type, that `SETEX` has no options.
    const fn set_ex(key: &'a Vec<u8>, value: &'a mut Vec<u8>, seconds: u64) -> Self {
        Self {
            key,
            value,
            expiry: Some(Expiry::Ex(seconds)),
            cond: None,
            keep_ttl: false,
            get: false,
        }
    }
}

/// Stores a value, subject to everything `SET`'s options can say about it.
///
/// Its own function rather than an arm of [`apply`] because the option algebra
/// is where the command's weight is: three of the four options are decided
/// against the entry already standing there, and a dispatcher that inlined
/// them would read as one command among ten rather than as the one command
/// that has them.
fn set<L: ReplicationLog>(
    dict: &mut Dict,
    log: &mut L,
    seq: &mut u64,
    shard: u16,
    now: Instant,
    args: SetArgs<'_>,
) -> Reply {
    let SetArgs {
        key,
        value,
        expiry,
        cond,
        keep_ttl,
        get,
    } = args;

    // Everything the entry standing there decides, decided in one look:
    // whether the condition is met, what `GET` has to answer, and what
    // deadline `KEEPTTL` would keep. The lazy expiry in front of the dispatch
    // has already run, so an entry found here is a live one.
    //
    // The value is cloned only for a `GET`, since that reply is the one thing
    // that still wants the old bytes after the write has taken their place.
    let found = dict.get(key);
    let existed = found.is_some();
    let old = found.filter(|_| get).map(|entry| entry.value.clone());
    let kept = found.and_then(|entry| entry.expires_at);

    // A condition the keyspace does not meet is answered, not failed: the peer
    // asked for a write that was allowed not to happen. What it is answered
    // with is the value that survives — the one already there for a refused
    // `NX`, and nothing at all for a refused `XX`, which by definition found
    // no key. Without `GET` both are the same empty answer.
    match cond {
        Some(Cond::Nx) if existed => return Reply::Bulk(old),
        Some(Cond::Xx) if !existed => return Reply::Bulk(None),
        _ => {}
    }
    if let Err(failed) = append(log, seq, shard) {
        return failed;
    }
    // `KEEPTTL` is the one way a write leaves a deadline where it found it.
    // Without it the deadline is whatever the options name, and naming none
    // clears the one the overwritten key was carrying — see [`Command::Set`].
    let expires_at = if keep_ttl {
        kept
    } else {
        deadline(now, expiry)
    };
    dict.insert(
        key.clone(),
        Entry {
            value: std::mem::take(value),
            expires_at,
            touched: 0,
        },
    );
    // As in `IncrBy`: the entry goes in unstamped and is stamped here, so a
    // key that was just written is the last one an eviction sample would take.
    dict.touch(key);
    if get { Reply::Bulk(old) } else { Reply::Ok }
}

/// Adds `delta` to the integer under `key`, treating a missing key as zero.
///
/// Extracted for the reason [`apply`] names as the third one: the arm had
/// grown long enough that leaving it inline cost the reader the dispatcher,
/// and `clippy::too_many_lines` said so on the commit that added the touch.
///
/// Three answers and only one of them is a write: a value that is not an
/// integer and a result that would leave `i64` both refuse before anything
/// reaches the log, which is what keeps the log free of records that replay
/// to a no-op.
fn incr_by<L: ReplicationLog>(
    dict: &mut Dict,
    log: &mut L,
    seq: &mut u64,
    shard: u16,
    key: &[u8],
    delta: i64,
) -> Reply {
    let (current, expires_at) = match dict.get(key) {
        None => (0, None),
        Some(entry) => match parse_i64(&entry.value) {
            Some(n) => (n, entry.expires_at),
            None => return Reply::Error(ReplyError::NotAnInteger),
        },
    };
    let Some(next) = current.checked_add(delta) else {
        return Reply::Error(ReplyError::WouldOverflow);
    };
    if let Err(failed) = append(log, seq, shard) {
        return failed;
    }
    // The deadline rides along, as it does in Redis: an increment changes what
    // a counter holds, not how long it lives.
    dict.insert(
        key.to_vec(),
        Entry {
            value: next.to_string().into_bytes(),
            expires_at,
            touched: 0,
        },
    );
    // The stamp the insert left is the placeholder [`Entry`] documents, never
    // what a write leaves behind: the touch right after it is what makes this
    // key the newest one an eviction sample can meet.
    dict.touch(key);
    Reply::Integer(next)
}

/// What `EXPIRE`'s or `PEXPIRE`'s span turned out to mean, once its unit is
/// out of the way.
///
/// The two commands differ in exactly one thing — the unit the span is written
/// in — so they reach one handler through this rather than being two arms that
/// would have to be kept identical by hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Deadline {
    /// Put this deadline on the key. The `None` inside is a span the clock
    /// cannot represent, which [`deadline`] turns into no deadline at all.
    At(Option<Instant>),
    /// The deadline is not in the future, so the key is to be removed.
    Passed,
}

/// Reads a span in the unit its command was written in as the deadline it names.
///
/// `unit` is the [`Expiry`] constructor for that unit, so the only thing a
/// caller supplies beyond the number is what the number counts.
fn span_deadline(now: Instant, span: i64, unit: fn(u64) -> Expiry) -> Deadline {
    if span <= 0 {
        return Deadline::Passed;
    }
    let span = u64::try_from(span).expect("a positive i64 is a u64");
    Deadline::At(deadline(now, Some(unit(span))))
}

/// Applies what a span turned out to mean — a deadline, or a removal —
/// answering whether there was a key to apply it to.
fn set_expiry<L: ReplicationLog>(
    dict: &mut Dict,
    log: &mut L,
    seq: &mut u64,
    shard: u16,
    key: &[u8],
    at: Deadline,
) -> Reply {
    // Answered before the log is touched. A key that is not there receives no
    // deadline, and a record that replays to nothing is a record the log is
    // better without — the same rule a `Del` of a missing key follows.
    if dict.get(key).is_none() {
        return Reply::Integer(0);
    }
    if let Err(failed) = append(log, seq, shard) {
        return failed;
    }
    match at {
        // A deadline that is not in the future is a deletion, and Redis
        // reports it as an applied expiry rather than as a delete — the client
        // asked for the key to be gone by a time that has passed, and it is.
        Deadline::Passed => {
            dict.remove(key);
        }
        Deadline::At(at) => {
            dict.set_deadline(key, at);
        }
    }
    Reply::Integer(1)
}

/// The name of the type `key` holds, or `none` if it holds nothing.
///
/// Presence is the whole question. This shard stores strings and nothing
/// else, so every key that is there holds one and the answer is looked up
/// rather than derived from the entry. A key whose deadline has passed is
/// already gone by the time this runs — [`apply`] evicts before it
/// dispatches — so it reads as absent without this having to ask.
fn type_name(dict: &Dict, key: &[u8]) -> &'static str {
    if dict.get(key).is_some() {
        "string"
    } else {
        "none"
    }
}

/// How many bytes `key`'s value holds, or `0` if there is no such key.
///
/// A missing key and a key holding an empty value both measure `0`, which is
/// Redis's answer for each. Saturating for the reason [`Command::DbSize`]
/// saturates: a value longer than `i64::MAX` cannot be held in the first
/// place, and the ceiling is a better answer than a wrap into a negative
/// length.
fn value_len(dict: &Dict, key: &[u8]) -> i64 {
    dict.get(key).map_or(0, |entry| {
        i64::try_from(entry.value.len()).unwrap_or(i64::MAX)
    })
}

/// Empties one shard's keyspace.
fn flush_db<L: ReplicationLog>(dict: &mut Dict, log: &mut L, seq: &mut u64, shard: u16) -> Reply {
    // A shard with nothing in it has nothing to record, for the reason a `Del`
    // of a missing key appends nothing: a record that replays to a no-op is a
    // record the log is better without. So a flush of an empty keyspace costs
    // one comparison and no position.
    if dict.is_empty() {
        return Reply::Ok;
    }
    // One record for the whole removal, not one per key. The log records that
    // a mutation happened and where it sits in the shard's order — see
    // `append` — and a flush is one mutation. It is also the only spelling
    // under which a refusal can leave the keyspace alone: a record per key
    // could fail partway and there would be no flush to undo.
    if let Err(failed) = append(log, seq, shard) {
        return failed;
    }
    dict.clear();
    Reply::Ok
}

/// Visits up to `count` buckets from `cursor` and reports the live keys among
/// them that match `pattern`.
///
/// Reads the dict and nothing else: a walk observes the keyspace, so this
/// takes `&Dict`, appends no log record and consumes no replication position.
/// That is what makes a step safe to interleave with anything — the shard is
/// occupied for the length of one step and its state is unchanged by it.
fn scan_step<P: ShardPolicy>(
    dict: &Dict,
    cursor: u64,
    count: usize,
    pattern: Option<&[u8]>,
    now: Instant,
    policy: &P,
) -> Reply {
    let mut keys = Vec::new();
    let mut next = cursor;
    let mut visited = 0;
    // At least one bucket, whatever the caller asked for: a step that visited
    // none would hand back the cursor it was given, and a caller looping until
    // the cursor returns to zero would never leave.
    for _ in 0..count.max(1) {
        visited += 1;
        next = dict.scan_in_order(next, policy, |key, entry| {
            // A key whose deadline has passed is not in the keyspace, even
            // though the sweep has not reached it. Reporting it would make a
            // walk contradict the `GET` that follows it.
            if policy.due_on_read(entry.expires_at, now) {
                return;
            }
            if pattern.is_none_or(|p| glob::matches(p, key)) {
                keys.push(key.to_vec());
            }
        });
        if next == 0 {
            break;
        }
    }
    Reply::Scan {
        cursor: next,
        keys,
        visited,
    }
}

/// Removes `key` if its deadline has passed, appending the deletion to the log
/// exactly as an explicit `Del` would.
///
/// An expiration is a change to the keyspace, so it takes a replication
/// position like every other one: whatever replays the log later has to see
/// the key disappear where it disappeared here, and the [`TraceSink`] — which
/// folds the position each command ran at — sees the position the removal
/// consumed. Neither has to know what a deadline is.
///
/// Whether the deadline has come due is [`ExpiryPolicy`]'s to say, and under
/// the honest [`Deadlines`] it comes due the instant `now` reaches it, not only
/// once it is past.
///
/// Answers whether an entry was reclaimed, which is what the shard counts as
/// an expiration; the caller adds it to `expired_keys`.
///
/// Returns the reply to send instead when the record could not be written. A
/// removal that cannot be logged must not happen, for the same reason a `Del`
/// that cannot be logged does not: the alternative is a keyspace that has
/// moved past a log which does not describe it. The entry stays, and the
/// command that met it is refused rather than answered from a value that
/// should be gone.
fn evict_if_expired<L: ReplicationLog, P: ExpiryPolicy>(
    dict: &mut Dict,
    log: &mut L,
    seq: &mut u64,
    shard: u16,
    key: &[u8],
    now: Instant,
    expiry: &P,
) -> Result<bool, Reply> {
    // A keyspace with no deadlines in it — which is nearly every keyspace —
    // leaves here without hashing anything, so standing in front of every
    // command costs it a predictable branch and not a second lookup. The
    // guarantee is unaffected: the dict answers `false` only when no entry it
    // holds can be expired. A policy that takes undated keys is the one case
    // where that shortcut would hide the answer, so it says so.
    if !dict.may_hold_deadlines() && !expiry.takes_undated() {
        return Ok(false);
    }
    let Some(entry) = dict.get(key) else {
        return Ok(false);
    };
    if !expiry.due_on_read(entry.expires_at, now) {
        return Ok(false);
    }
    append(log, seq, shard)?;
    dict.remove(key);
    Ok(true)
}

/// The instant an expiry option lands on, or `None` for a key with no
/// deadline.
///
/// A deadline the clock cannot represent — a span so large that `now` plus it
/// leaves [`Instant`]'s range — is stored as no deadline at all. No instant
/// this process can reach is past it, so the two are indistinguishable to
/// every command that will ever ask, except that `TTL` reports such a key as
/// having no deadline; the alternative is an arithmetic panic on a
/// peer-supplied number.
fn deadline(now: Instant, expiry: Option<Expiry>) -> Option<Instant> {
    let span = match expiry? {
        Expiry::Ex(seconds) => Duration::from_secs(seconds),
        Expiry::Px(millis) => Duration::from_millis(millis),
    };
    now.checked_add(span)
}

/// How many seconds are left before `expires_at`, rounded to nearest.
///
/// `(milliseconds + 500) / 1000`, which is what Redis replies and therefore
/// what a client comparing two servers sees: a key with 99.4 seconds left
/// reads `99`, not `100`. That means a key with under half a second left reads
/// `0` while still being alive, which is Redis's behaviour too and not a
/// rounding accident.
///
/// Saturating rather than truncating on the way to `i64`: a remaining span
/// that does not fit is further off than any client will wait, and reporting
/// the largest number there is says that better than a wrapped one.
fn remaining_seconds(expires_at: Instant, now: Instant) -> i64 {
    let millis = expires_at.saturating_duration_since(now).as_millis();
    i64::try_from(millis.saturating_add(500) / 1000).unwrap_or(i64::MAX)
}

/// Appends one record for a mutation about to happen, advancing `seq`.
///
/// The payload is empty: today the log records that a mutation occurred and
/// where it sits in the shard's order, not what it was. Returns the `Reply` to send
/// instead when the write fails — the mutation must not proceed.
fn append<L: ReplicationLog>(log: &mut L, seq: &mut u64, shard: u16) -> Result<(), Reply> {
    let record = Record {
        shard,
        seq: *seq,
        payload: &[],
    };
    match log.append(record) {
        Ok(()) => {
            *seq += 1;
            Ok(())
        }
        Err(_) => Err(Reply::Error(ReplyError::LogWriteFailed)),
    }
}

/// Parses the canonical decimal representation of an `i64`.
///
/// Accepts an optional `-` followed by ASCII digits, and *only* the canonical
/// spelling: no leading `+`, no whitespace, no leading zeros (`"007"`), no
/// negative zero (`"-0"`). `"0"` itself is fine.
///
/// The strictness is what makes stored counters canonical. `i64::to_string`
/// emits exactly this form, so every integer has one byte representation and
/// one only — two nodes that applied the same increments hold byte-identical
/// values, which is what the simulator compares. A permissive parser would
/// let `"007"` and `"7"` both mean seven and break that.
pub fn parse_i64(bytes: &[u8]) -> Option<i64> {
    let (negative, digits) = match bytes.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, bytes),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    if digits[0] == b'0' && (digits.len() > 1 || negative) {
        return None;
    }
    // Verified ASCII above, so this is valid UTF-8; check rather than assert,
    // so a bug in the validation is a `None` and never a panic.
    std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {

    /// Every `Command` variant's tag is inside the arrays indexed by it, and
    /// the tags are `1..=KIND_MAX` with none missing and none shared.
    ///
    /// Two mechanisms, because they fail for different reasons. The `match`
    /// below has no wildcard, so **adding a variant** to `Command` stops this
    /// file compiling — which is the moment to decide the new tag rather than
    /// the moment traffic reaches it. The assertion on the tags themselves
    /// catches the rest: a tag reused, a tag skipped, or `KIND_MAX` moved
    /// without the arrays following, any of which would index past
    /// `KIND_SLOTS` inside a spawned executor and read as a shard that died.
    #[test]
    fn every_kind_tag_is_contiguous_and_bounded() {
        let every = [
            Command::Get { key: Vec::new() },
            Command::Set {
                key: Vec::new(),
                value: Vec::new(),
                expiry: None,
                cond: None,
                keep_ttl: false,
                get: false,
            },
            Command::Del { key: Vec::new() },
            Command::IncrBy {
                key: Vec::new(),
                delta: 1,
            },
            Command::Expire {
                key: Vec::new(),
                seconds: 1,
            },
            Command::Ttl { key: Vec::new() },
            Command::Exists { key: Vec::new() },
            Command::FlushDb,
            Command::DbSize,
            Command::ScanStep {
                cursor: 0,
                count: 1,
                pattern: None,
            },
            Command::PExpire {
                key: Vec::new(),
                millis: 1,
            },
            Command::Persist { key: Vec::new() },
            Command::Type { key: Vec::new() },
            Command::StrLen { key: Vec::new() },
            Command::Stats,
            Command::SetEx {
                key: Vec::new(),
                seconds: 1,
                value: Vec::new(),
            },
        ];
        for cmd in &every {
            // No wildcard: a new variant fails to compile here.
            match cmd {
                Command::Get { .. }
                | Command::Set { .. }
                | Command::Del { .. }
                | Command::IncrBy { .. }
                | Command::Expire { .. }
                | Command::Ttl { .. }
                | Command::Exists { .. }
                | Command::FlushDb
                | Command::DbSize
                | Command::ScanStep { .. }
                | Command::PExpire { .. }
                | Command::Persist { .. }
                | Command::Type { .. }
                | Command::StrLen { .. }
                | Command::Stats
                | Command::SetEx { .. } => {}
            }
        }
        let mut tags: Vec<u8> = every.iter().map(Command::kind).collect();
        tags.sort_unstable();
        assert_eq!(
            tags,
            (1..=Command::KIND_MAX).collect::<Vec<u8>>(),
            "the tags are not 1..=KIND_MAX with none missing and none shared"
        );
        assert_eq!(
            KIND_SLOTS,
            usize::from(Command::KIND_MAX) + 1,
            "a per-kind array would not hold every tag"
        );
    }
    use super::*;
    use std::sync::Mutex;
    use tokio::time::Instant;

    /// One shard's state, driven through [`apply`] directly.
    ///
    /// The expiry tests need two things a pool deliberately does not offer:
    /// the `now` each command sees, which in production is the executor's to
    /// choose, and the dict a handler left behind — an expired entry has to be
    /// shown *gone*, not merely invisible to a read.
    type Shard = ShardState<NoopLog>;

    impl Shard {
        fn for_tests() -> Self {
            Self::new(Dict::with_seed(DictSeed { k0: 5, k1: 7 }), NoopLog)
        }

        /// By value, because [`apply`] takes what it stores: a command that has
        /// been run has had its value moved out of it, so a caller cannot
        /// usefully hold one across two runs.
        fn run(&mut self, mut cmd: Command, now: Instant) -> Reply {
            apply(self, 0, &mut cmd, now, &Deadlines)
        }
    }

    /// `SET key value`, with no options.
    fn set(key: &[u8], value: &[u8]) -> Command {
        Command::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            expiry: None,
            cond: None,
            keep_ttl: false,
            get: false,
        }
    }

    /// `SET key value EX seconds`.
    fn set_ex(key: &[u8], value: &[u8], seconds: u64) -> Command {
        Command::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            expiry: Some(Expiry::Ex(seconds)),
            cond: None,
            keep_ttl: false,
            get: false,
        }
    }

    /// `SETEX key seconds value`.
    fn setex(key: &[u8], seconds: u64, value: &[u8]) -> Command {
        Command::SetEx {
            key: key.to_vec(),
            seconds,
            value: value.to_vec(),
        }
    }

    fn get(key: &[u8]) -> Command {
        Command::Get { key: key.to_vec() }
    }

    /// Every variant answers `route()`, and the keyed ones answer with the
    /// key they name.
    ///
    /// Written over a list built here rather than over the one variant that
    /// happens to be convenient: `route()` is a `match` with no wildcard, so
    /// a variant added later cannot compile without answering — but nothing
    /// makes it answer *correctly*, and a keyed command routed to the wrong
    /// key is a key served by the wrong shard.
    ///
    /// The lists are length-annotated so that adding a variant to one of them
    /// is a deliberate act rather than a line that slips in unnoticed, and
    /// every variant is in exactly one of them: the keyed ones below, and the
    /// three that name no key. `Type` and `StrLen` reached the enum without
    /// reaching this list once already, which is the failure this guard exists
    /// to catch and did not.
    #[test]
    fn every_command_declares_how_it_is_routed() {
        let keyed: [Command; 12] = [
            Command::Get { key: b"k".to_vec() },
            set(b"k", b"v"),
            Command::Del { key: b"k".to_vec() },
            Command::IncrBy {
                key: b"k".to_vec(),
                delta: 1,
            },
            Command::Expire {
                key: b"k".to_vec(),
                seconds: 1,
            },
            Command::PExpire {
                key: b"k".to_vec(),
                millis: 1,
            },
            Command::Ttl { key: b"k".to_vec() },
            Command::Persist { key: b"k".to_vec() },
            Command::Exists { key: b"k".to_vec() },
            Command::Type { key: b"k".to_vec() },
            Command::StrLen { key: b"k".to_vec() },
            setex(b"k", 1, b"v"),
        ];
        for cmd in keyed {
            assert_eq!(
                cmd.route(),
                Route::Key(b"k"),
                "{cmd:?} routes on the key it names"
            );
        }

        let keyless: [(Command, Route<'_>); 4] = [
            (Command::FlushDb, Route::Every),
            (Command::DbSize, Route::Every),
            (Command::Stats, Route::Every),
            (
                Command::ScanStep {
                    cursor: 0,
                    count: 1,
                    pattern: None,
                },
                Route::Unaddressed,
            ),
        ];
        for (cmd, route) in keyless {
            assert_eq!(cmd.route(), route, "{cmd:?} routes as it declares");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn set_with_ex_expires_lazily() {
        let mut shard = Shard::for_tests();
        assert_eq!(shard.run(set_ex(b"k", b"v", 30), Instant::now()), Reply::Ok);

        tokio::time::advance(Duration::from_secs(29)).await;
        assert_eq!(
            shard.run(get(b"k"), Instant::now()),
            Reply::Bulk(Some(b"v".to_vec())),
            "a key one second short of its deadline is still a key"
        );

        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(shard.run(get(b"k"), Instant::now()), Reply::Bulk(None));
        // The read is what removed it. A deadline that only hid the entry would
        // leave the keyspace growing with values nothing can ever reach again.
        assert_eq!(
            shard.dict.len(),
            0,
            "the expired entry survived the read that reported it gone"
        );
    }

    /// `SETEX` is `SET … EX` under its old name: the value is stored, the
    /// deadline is `seconds` out, and the key dies on the read that finds it
    /// due — the same lazy expiry `set_with_ex_expires_lazily` pins for `SET`.
    #[tokio::test(start_paused = true)]
    async fn setex_writes_the_value_and_the_deadline() {
        let mut shard = Shard::for_tests();
        assert_eq!(shard.run(setex(b"k", 30, b"v"), Instant::now()), Reply::Ok);

        tokio::time::advance(Duration::from_secs(29)).await;
        assert_eq!(
            shard.run(get(b"k"), Instant::now()),
            Reply::Bulk(Some(b"v".to_vec())),
            "a key one second short of its deadline is still a key"
        );

        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(shard.run(get(b"k"), Instant::now()), Reply::Bulk(None));
        assert_eq!(shard.dict.len(), 0, "the expired entry survived the read");
    }

    /// An existing key is overwritten whole, value and deadline: a key that
    /// had no deadline acquires one. Measured against `redis:6-alpine`
    /// (6.2.24) and `redis:8-alpine` (8.10.1): `SET pre x` then
    /// `SETEX pre 50 replaced` gives `GET pre` → `replaced`, `TTL pre` → `50`.
    #[tokio::test(start_paused = true)]
    async fn setex_overwrites_value_and_deadline_alike() {
        let mut shard = Shard::for_tests();
        assert_eq!(shard.run(set(b"pre", b"x"), Instant::now()), Reply::Ok);
        assert_eq!(
            shard.run(setex(b"pre", 50, b"replaced"), Instant::now()),
            Reply::Ok
        );
        assert_eq!(
            shard.run(get(b"pre"), Instant::now()),
            Reply::Bulk(Some(b"replaced".to_vec()))
        );
        assert_eq!(
            shard.run(
                Command::Ttl {
                    key: b"pre".to_vec()
                },
                Instant::now()
            ),
            Reply::Integer(50)
        );
    }

    /// `SETEX` carries Redis's `denyoom` flag (`COMMAND INFO SETEX` on
    /// 6.2.24 and 8.10.1), so under `noeviction` a full shard refuses it as
    /// it refuses `SET`.
    #[tokio::test]
    async fn setex_is_refused_when_full_under_noeviction() {
        let pool = limited(
            2 * 8 * crate::dict::BUCKET_OVERHEAD,
            EvictionMode::NoEviction,
        );
        assert_eq!(pool.dispatch(set(b"k0", &[0u8; 64])).await, Reply::Ok);
        assert_eq!(
            pool.dispatch(setex(b"k1", 10, &[0u8; 64])).await,
            Reply::Error(ReplyError::OutOfMemory)
        );
        assert_eq!(pool.dispatch(get(b"k1")).await, Reply::Bulk(None));
    }

    #[tokio::test(start_paused = true)]
    async fn set_nx_xx_algebra() {
        let mut shard = Shard::for_tests();
        let conditional = |value: &[u8], cond: Cond| Command::Set {
            key: b"k".to_vec(),
            value: value.to_vec(),
            expiry: None,
            cond: Some(cond),
            keep_ttl: false,
            get: false,
        };

        // XX on an absent key stores nothing and says so.
        assert_eq!(
            shard.run(conditional(b"first", Cond::Xx), Instant::now()),
            Reply::Bulk(None)
        );
        assert_eq!(shard.run(get(b"k"), Instant::now()), Reply::Bulk(None));

        // NX on an absent key stores.
        assert_eq!(
            shard.run(conditional(b"first", Cond::Nx), Instant::now()),
            Reply::Ok
        );
        assert_eq!(
            shard.run(get(b"k"), Instant::now()),
            Reply::Bulk(Some(b"first".to_vec()))
        );

        // NX on a present key refuses, and leaves the value it found alone.
        assert_eq!(
            shard.run(conditional(b"second", Cond::Nx), Instant::now()),
            Reply::Bulk(None)
        );
        assert_eq!(
            shard.run(get(b"k"), Instant::now()),
            Reply::Bulk(Some(b"first".to_vec())),
            "a refused NX overwrote the value anyway"
        );

        // XX on a present key replaces it.
        assert_eq!(
            shard.run(conditional(b"second", Cond::Xx), Instant::now()),
            Reply::Ok
        );
        assert_eq!(
            shard.run(get(b"k"), Instant::now()),
            Reply::Bulk(Some(b"second".to_vec()))
        );

        // And a plain SET clears the deadline the key it overwrote carried —
        // Redis's semantics, and the reason a rewritten key is not silently
        // still on its predecessor's clock.
        assert_eq!(shard.run(set_ex(b"t", b"v", 30), Instant::now()), Reply::Ok);
        tokio::time::advance(Duration::from_secs(29)).await;
        assert_eq!(shard.run(set(b"t", b"w"), Instant::now()), Reply::Ok);
        tokio::time::advance(Duration::from_hours(1)).await;
        assert_eq!(
            shard.run(get(b"t"), Instant::now()),
            Reply::Bulk(Some(b"w".to_vec())),
            "a plain SET left the old deadline in place"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expire_and_ttl() {
        let mut shard = Shard::for_tests();
        let ttl = |key: &[u8]| Command::Ttl { key: key.to_vec() };
        let expire = |key: &[u8], seconds: i64| Command::Expire {
            key: key.to_vec(),
            seconds,
        };

        assert_eq!(
            shard.run(ttl(b"k"), Instant::now()),
            Reply::Integer(-2),
            "TTL of a key that does not exist"
        );
        assert_eq!(
            shard.run(expire(b"k", 10), Instant::now()),
            Reply::Integer(0),
            "EXPIRE of a key that does not exist"
        );

        assert_eq!(shard.run(set(b"k", b"v"), Instant::now()), Reply::Ok);
        assert_eq!(
            shard.run(ttl(b"k"), Instant::now()),
            Reply::Integer(-1),
            "TTL of a key with no deadline"
        );

        assert_eq!(
            shard.run(expire(b"k", 100), Instant::now()),
            Reply::Integer(1)
        );
        assert_eq!(shard.run(ttl(b"k"), Instant::now()), Reply::Integer(100));
        // Rounded to nearest, byte-for-byte what Redis replies. Half a second
        // gone still reads 100 under either rounding rule, so it is the next
        // assertion and not this one that pins which rule is in force.
        tokio::time::advance(Duration::from_millis(500)).await;
        assert_eq!(shard.run(ttl(b"k"), Instant::now()), Reply::Integer(100));
        // 99.4 seconds left: Redis says 99, and rounding up would say 100.
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(
            shard.run(ttl(b"k"), Instant::now()),
            Reply::Integer(99),
            "TTL must round to nearest, as Redis does, not up"
        );
        // And the other side of the same rule: under half a second left reads
        // as 0 while the key is still very much alive.
        tokio::time::advance(Duration::from_millis(99_100)).await;
        assert_eq!(shard.run(ttl(b"k"), Instant::now()), Reply::Integer(0));
        assert_eq!(
            shard.run(get(b"k"), Instant::now()),
            Reply::Bulk(Some(b"v".to_vec())),
            "a key reading TTL 0 is still alive"
        );

        // A non-positive expiry deletes the key, and still reports that the
        // deadline was applied rather than that nothing was there.
        assert_eq!(
            shard.run(expire(b"k", 0), Instant::now()),
            Reply::Integer(1)
        );
        assert_eq!(shard.run(get(b"k"), Instant::now()), Reply::Bulk(None));
        assert_eq!(shard.dict.len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_keys_are_dead_to_every_command() {
        let mut shard = Shard::for_tests();
        // One key per command, all past the same deadline: every arm meets an
        // entry that is still in the dict and already gone, which is the state
        // a handler that skipped the liveness check would answer from.
        for key in [
            &b"get"[..],
            b"exists",
            b"ttl",
            b"persist",
            b"pexpire",
            b"del",
            b"incrby",
            b"type",
            b"strlen",
        ] {
            assert_eq!(shard.run(set_ex(key, b"1", 10), Instant::now()), Reply::Ok);
        }
        tokio::time::advance(Duration::from_secs(11)).await;
        let now = Instant::now();

        assert_eq!(shard.run(get(b"get"), now), Reply::Bulk(None));
        assert_eq!(
            shard.run(
                Command::Exists {
                    key: b"exists".to_vec()
                },
                now
            ),
            Reply::Integer(0)
        );
        assert_eq!(
            shard.run(
                Command::Ttl {
                    key: b"ttl".to_vec()
                },
                now
            ),
            Reply::Integer(-2)
        );
        // Both would answer `1` against an entry that was still there: a
        // `PERSIST` would find the stale deadline to clear, and a `PEXPIRE` a
        // key to put a new one on. `0` is the answer only an eviction that has
        // already happened can produce.
        assert_eq!(
            shard.run(
                Command::Persist {
                    key: b"persist".to_vec()
                },
                now
            ),
            Reply::Integer(0)
        );
        assert_eq!(
            shard.run(
                Command::PExpire {
                    key: b"pexpire".to_vec(),
                    millis: 10_000
                },
                now
            ),
            Reply::Integer(0)
        );
        assert_eq!(
            shard.run(
                Command::Del {
                    key: b"del".to_vec()
                },
                now
            ),
            Reply::Removed(false)
        );
        assert_eq!(
            shard.run(
                Command::IncrBy {
                    key: b"incrby".to_vec(),
                    delta: 7
                },
                now
            ),
            Reply::Integer(7),
            "an expired counter must start from zero"
        );
        // The two that describe an entry rather than read it. Against a key
        // still in the dict they would report `string` and the length of the
        // value that outlived its deadline; `none` and `0` are the answers
        // only an eviction that has already happened can produce.
        assert_eq!(
            shard.run(
                Command::Type {
                    key: b"type".to_vec()
                },
                now
            ),
            Reply::Status("none")
        );
        assert_eq!(
            shard.run(
                Command::StrLen {
                    key: b"strlen".to_vec()
                },
                now
            ),
            Reply::Integer(0)
        );

        // Each was removed by the command that met it, not merely hidden from
        // it: what is left is the counter INCRBY re-created.
        assert_eq!(shard.dict.len(), 1);
    }

    /// An expiry is a deletion, and a deletion is a logged mutation.
    ///
    /// The record is what a later replay reads; the replication position it
    /// consumes is what the trace sink folds. So an expiration is visible to
    /// both without either having to know what a deadline is.
    #[tokio::test(start_paused = true)]
    async fn an_expiry_is_logged_exactly_as_a_delete_is() {
        #[derive(Clone, Default)]
        struct Recording(Arc<Mutex<Vec<(u16, u64)>>>);

        impl ReplicationLog for Recording {
            fn append(&mut self, rec: Record<'_>) -> std::io::Result<()> {
                self.0.lock().expect("log mutex").push((rec.shard, rec.seq));
                Ok(())
            }
            fn sync(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let log = Recording::default();
        let mut state = ShardState::new(Dict::with_seed(DictSeed { k0: 5, k1: 7 }), log.clone());

        let mut set = set_ex(b"k", b"v", 30);
        assert_eq!(
            apply(&mut state, 3, &mut set, Instant::now(), &Deadlines),
            Reply::Ok
        );
        tokio::time::advance(Duration::from_secs(31)).await;

        // A read appends nothing of its own, so the second record below is the
        // expiry's and nothing else.
        let mut read = get(b"k");
        assert_eq!(
            apply(&mut state, 3, &mut read, Instant::now(), &Deadlines),
            Reply::Bulk(None)
        );
        assert_eq!(*log.0.lock().expect("log mutex"), vec![(3, 0), (3, 1)]);
        assert_eq!(
            state.seq, 2,
            "the expiry did not consume a replication position"
        );
        assert_eq!(state.expired, 1, "the lazy half counted the expiration");
    }

    /// `PEXPIRE` and `PERSIST` reach the log exactly when they change
    /// something.
    ///
    /// The replication position is what makes that observable: [`append`]
    /// advances `seq` only when a record was written, so a `seq` that stood
    /// still is a mutation that never happened. Both commands answer `0` for
    /// the cases where there is nothing to change, and answering it *before*
    /// the append is what these assertions pin — an implementation that logged
    /// first would give the same `0` on the wire and leave behind a record that
    /// replays to nothing.
    #[tokio::test(start_paused = true)]
    async fn pexpire_and_persist_reach_the_log_only_when_they_change_something() {
        let mut shard = Shard::for_tests();
        let ttl = |key: &[u8]| Command::Ttl { key: key.to_vec() };
        let persist = |key: &[u8]| Command::Persist { key: key.to_vec() };
        let pexpire = |key: &[u8], millis: i64| Command::PExpire {
            key: key.to_vec(),
            millis,
        };

        assert_eq!(
            shard.run(pexpire(b"k", 1000), Instant::now()),
            Reply::Integer(0),
            "PEXPIRE of a key that does not exist"
        );
        assert_eq!(
            shard.run(persist(b"k"), Instant::now()),
            Reply::Integer(0),
            "PERSIST of a key that does not exist"
        );
        assert_eq!(shard.seq, 0, "neither wrote a record");

        assert_eq!(shard.run(set(b"k", b"v"), Instant::now()), Reply::Ok);
        assert_eq!(shard.seq, 1, "the write that created it did");

        assert_eq!(
            shard.run(persist(b"k"), Instant::now()),
            Reply::Integer(0),
            "PERSIST of a key that is there but carries no deadline"
        );
        assert_eq!(shard.seq, 1, "and it wrote no record either");

        assert_eq!(
            shard.run(pexpire(b"k", 30_000), Instant::now()),
            Reply::Integer(1)
        );
        assert_eq!(shard.seq, 2);
        assert_eq!(
            shard.run(ttl(b"k"), Instant::now()),
            Reply::Integer(30),
            "thirty thousand milliseconds is thirty seconds"
        );

        assert_eq!(shard.run(persist(b"k"), Instant::now()), Reply::Integer(1));
        assert_eq!(shard.seq, 3);
        assert_eq!(
            shard.run(ttl(b"k"), Instant::now()),
            Reply::Integer(-1),
            "the key outlived the deadline it was carrying"
        );

        // A span that is not in the future is a deletion, answered as an
        // expiry that was applied.
        assert_eq!(
            shard.run(pexpire(b"k", 0), Instant::now()),
            Reply::Integer(1)
        );
        assert_eq!(shard.seq, 4);
        assert_eq!(
            shard.run(ttl(b"k"), Instant::now()),
            Reply::Integer(-2),
            "the key is gone, not merely left without a deadline"
        );
    }

    /// The sink's side of the same fact: a position disappears from the trace
    /// where an expiry took one, so a run in which a key expired cannot hash
    /// like a run in which it did not.
    ///
    /// The pool is spawned with a policy that never sweeps, so the only thing
    /// that can remove this key is the command that meets it — which is the
    /// path these assertions are about. The tick is deliberately fired several
    /// times in between: under the honest policy that firing would reclaim the
    /// key and the trace would carry the sweep's own removal instead of the
    /// gap, so the loop is what proves the dependence is gone rather than
    /// merely unobserved.
    ///
    /// The read and the write still go in one batch. That is now a matter of
    /// clarity alone — the policy, not the batching, is what keeps the sweep
    /// out of these positions.
    #[tokio::test(start_paused = true)]
    async fn the_sink_sees_the_position_an_expiry_consumed() {
        let sink = Recorder::default();
        let pool =
            ShardPool::spawn_with_policy(1, 1, DictSeed { k0: 2, k1: 3 }, sink.clone(), NoSweep);

        pool.dispatch(set_ex(b"k", b"v", 1)).await;
        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..4 {
            tokio::time::advance(HOUSEKEEPING_TICK).await;
            tokio::task::yield_now().await;
        }
        pool.dispatch_many(vec![get(b"k"), set(b"k", b"again")])
            .await;

        let seen = sink.0.lock().expect("recorder mutex").clone();
        assert_eq!(
            seen,
            vec![
                (0, 0, 2, Reply::Ok),
                // The read ran at position 1 and evicted the key there.
                (0, 1, 1, Reply::Bulk(None)),
                // So the next write is at 2, not at 1: the gap is the expiry.
                (0, 2, 2, Reply::Ok),
            ]
        );
    }

    /// What the sink's `seq` means when one command consumes two positions.
    ///
    /// A read that evicts appends one record, so it cannot tell the two
    /// candidate contracts apart. A *write* over an expired key appends the
    /// eviction's record and then its own, and the position reported is the
    /// first — where the command's effects began, not where its own record
    /// landed. Both write paths are exercised, because they append in
    /// different arms.
    ///
    /// The pool is spawned with a policy that never sweeps, so the only thing
    /// that can remove these keys is the command that meets them — which is
    /// the path these assertions are about. The tick is deliberately fired
    /// several times in between: under the honest policy that firing would
    /// reclaim them and the trace would carry the sweep's own removals instead
    /// of the gaps, so the loop is what proves the dependence is gone rather
    /// than merely unobserved.
    ///
    /// The two writes still go in one batch. That is now a matter of clarity
    /// alone — the policy, not the batching, is what keeps the sweep out of
    /// these positions.
    #[tokio::test(start_paused = true)]
    async fn a_command_is_traced_where_its_effects_begin_not_where_its_record_landed() {
        let sink = Recorder::default();
        let pool =
            ShardPool::spawn_with_policy(1, 1, DictSeed { k0: 2, k1: 3 }, sink.clone(), NoSweep);

        pool.dispatch(set_ex(b"written", b"v", 1)).await;
        pool.dispatch(set_ex(b"counted", b"1", 1)).await;
        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..4 {
            tokio::time::advance(HOUSEKEEPING_TICK).await;
            tokio::task::yield_now().await;
        }
        pool.dispatch_many(vec![
            set(b"written", b"again"),
            Command::IncrBy {
                key: b"counted".to_vec(),
                delta: 7,
            },
        ])
        .await;
        pool.dispatch(get(b"written")).await;

        let seen = sink.0.lock().expect("recorder mutex").clone();
        assert_eq!(
            seen,
            vec![
                (0, 0, 2, Reply::Ok),
                (0, 1, 2, Reply::Ok),
                // Eviction at 2, the write's own record at 3, traced at 2.
                (0, 2, 2, Reply::Ok),
                // Eviction at 4, the increment's own record at 5, traced at 4.
                // A counter that had expired starts from zero.
                (0, 4, 4, Reply::Integer(7)),
                // Which leaves the next command at 6: four positions for two
                // commands is exactly what the contract says can happen.
                (0, 6, 1, Reply::Bulk(Some(b"again".to_vec()))),
            ]
        );
    }

    /// The half of expiration lazy eviction cannot do.
    ///
    /// Every key here is written once and never addressed again, so no command
    /// ever meets one: under lazy expiry alone the thousand entries would sit
    /// in the dict for the life of the process, and the only thing that can
    /// reclaim them is the shard's own tick. The evidence is the trace — a
    /// `Del` per key, at a replication position of its own — because it is
    /// produced without anything touching the keyspace, which is exactly the
    /// claim.
    #[tokio::test(start_paused = true)]
    async fn the_sweep_reclaims_untouched_expired_keys() {
        const KEYS: u64 = 1_000;
        let keys = usize::try_from(KEYS).expect("a thousand keys is a usize");

        let sink = Recorder::default();
        // One shard, so the positions below are one sequence rather than an
        // interleaving, and every key's sweep is driven by one cursor.
        let pool = ShardPool::spawn(1, 1, DictSeed { k0: 2, k1: 3 }, sink.clone());
        for i in 0..KEYS {
            assert_eq!(
                pool.dispatch(set_ex(format!("k{i}").as_bytes(), b"v", 1))
                    .await,
                Reply::Ok
            );
        }

        // Past every deadline, with nothing having read a single key.
        tokio::time::advance(Duration::from_secs(2)).await;

        let removals = |sink: &Recorder| {
            sink.0
                .lock()
                .expect("recorder mutex")
                .iter()
                .filter(|(_, _, kind, reply)| *kind == 3 && *reply == Reply::Removed(true))
                .count()
        };

        // Drive the housekeeping tick until the cursor has been round the
        // table. The bound is a guard rather than an acceptance criterion: a
        // sweep that never reclaims anything must fail the test rather than
        // hang it.
        let mut ticks = 0;
        while removals(&sink) < keys {
            tokio::time::advance(HOUSEKEEPING_TICK).await;
            ticks += 1;
            assert!(
                ticks < 64,
                "after {ticks} ticks the sweep had reclaimed {} of {KEYS} keys",
                removals(&sink)
            );
        }

        // Exactly one removal per key: a sweep that visited a bucket twice in a
        // cycle, or that reported a key it had already removed, would overshoot.
        assert_eq!(removals(&sink), keys);
        let seen = sink.0.lock().expect("recorder mutex").clone();
        assert_eq!(
            seen.len(),
            2 * keys,
            "the trace holds something other than the thousand writes and their expiries"
        );
        // Each expiry consumed a replication position of its own, immediately
        // after the thousand writes: an expiry is a logged mutation, whoever
        // caused it.
        let positions: Vec<u64> = seen[keys..].iter().map(|(_, seq, _, _)| *seq).collect();
        assert_eq!(positions, (KEYS..2 * KEYS).collect::<Vec<u64>>());

        // And the keyspace really is empty: the read below runs at the position
        // the sweep left behind and consumes nothing, so it evicted nothing —
        // there was nothing left for it to evict.
        assert_eq!(
            pool.dispatch(get(b"k0")).await,
            Reply::Bulk(None),
            "a key the sweep reported gone answered a read"
        );
        let last = sink.0.lock().expect("recorder mutex").last().cloned();
        assert_eq!(last, Some((0, 2 * KEYS, 1, Reply::Bulk(None))));
    }

    /// A sweep's removal is a logged mutation, so a log that cannot take the
    /// record does not get the removal either.
    ///
    /// The ordering `apply` documents for a command holds for the tick as well:
    /// the record is written before the entry goes, and a refused record leaves
    /// the keyspace behind the log rather than ahead of it. The dead entry
    /// keeps its deadline and the cursor keeps its place, so the same buckets
    /// are swept again — which is what makes the failure a delay rather than a
    /// leak.
    #[tokio::test(start_paused = true)]
    async fn a_sweep_whose_record_cannot_be_written_leaves_the_key() {
        #[derive(Clone, Default)]
        struct Breakable(Arc<Mutex<bool>>);

        impl ReplicationLog for Breakable {
            fn append(&mut self, _rec: Record<'_>) -> std::io::Result<()> {
                if *self.0.lock().expect("log mutex") {
                    return Err(std::io::Error::other("the disk went away"));
                }
                Ok(())
            }
            fn sync(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let log = Breakable::default();
        let sink = Recorder::default();
        let pool = ShardPool::spawn_with_log(1, 1, DictSeed { k0: 2, k1: 3 }, sink.clone(), {
            let log = log.clone();
            move |_shard| log.clone()
        });
        assert_eq!(pool.dispatch(set_ex(b"k", b"v", 1)).await, Reply::Ok);

        *log.0.lock().expect("log mutex") = true;
        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..8 {
            tokio::time::advance(HOUSEKEEPING_TICK).await;
        }

        // The entry is still there. A read cannot say so directly — it would
        // report a live key and an evicted one identically — but it can say it
        // by failing: the lazy path meets the same expired entry and the same
        // broken log, and refuses for the same reason. A key the sweep had
        // removed would have answered `Bulk(None)` instead.
        assert_eq!(
            pool.dispatch(get(b"k")).await,
            Reply::Error(ReplyError::LogWriteFailed),
            "the sweep removed a key whose record could not be written"
        );

        // With the log back, the sweep reaches the same bucket again and
        // reclaims it — the buckets it abandoned were retried, not skipped.
        *log.0.lock().expect("log mutex") = false;
        let mut ticks = 0;
        while sink
            .0
            .lock()
            .expect("recorder mutex")
            .last()
            .map(|(_, _, kind, _)| *kind)
            != Some(3)
        {
            tokio::time::advance(HOUSEKEEPING_TICK).await;
            ticks += 1;
            assert!(ticks < 64, "the sweep never came back for the key");
        }
        assert_eq!(
            sink.0.lock().expect("recorder mutex").last().cloned(),
            Some((0, 1, 3, Reply::Removed(true))),
            "the expiry did not take the position after the write"
        );
    }

    /// The gauge is the sum of every dict's figure, kept current by the
    /// executors: a write moves it up by what the dict says the write cost,
    /// and a delete moves it back down by the same amount.
    #[tokio::test]
    async fn the_pool_gauge_follows_what_its_dicts_account() {
        let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
        let gauge = pool.memory();
        let empty = gauge.used();
        assert_eq!(
            empty,
            4 * 8 * crate::dict::BUCKET_OVERHEAD,
            "four empty tables"
        );
        assert_eq!(pool.dispatch(set(b"k", &[0u8; 100])).await, Reply::Ok);
        assert_eq!(
            gauge.used(),
            empty + crate::dict::entry_bytes(b"k", &[0u8; 100])
        );
        assert_eq!(
            pool.dispatch(Command::Del { key: b"k".to_vec() }).await,
            Reply::Removed(true)
        );
        assert_eq!(gauge.used(), empty);
    }

    /// The counters `INFO` reports, gathered the way `INFO` gathers them.
    ///
    /// Two things are pinned here that nothing else pins. The reads are
    /// classified as Redis classifies them — `GET`, `EXISTS`, `TTL`, `TYPE`
    /// and `STRLEN` count, a write does not, and `STRLEN` of a stored empty
    /// value is a hit rather than the miss its `0` would suggest. And a
    /// command that reaches every shard is counted **once by the edge and not
    /// at all here**: a `DBSIZE` that landed in `calls` would be reported as
    /// one call per shard.
    #[tokio::test]
    async fn a_shard_counts_the_lookups_and_the_calls_info_reports() {
        let pool = ShardPool::spawn(2, 1, DictSeed { k0: 1, k1: 2 }, NoTrace);
        assert_eq!(pool.dispatch(set(b"present", b"v")).await, Reply::Ok);
        assert_eq!(pool.dispatch(set(b"empty", b"")).await, Reply::Ok);

        // Two hits and two misses from the two commands that answer plainly.
        pool.dispatch(get(b"present")).await;
        pool.dispatch(get(b"absent")).await;
        pool.dispatch(Command::Exists {
            key: b"present".to_vec(),
        })
        .await;
        pool.dispatch(Command::Exists {
            key: b"absent".to_vec(),
        })
        .await;
        // And one of each from the three that do not.
        pool.dispatch(Command::Ttl {
            key: b"present".to_vec(),
        })
        .await;
        pool.dispatch(Command::Ttl {
            key: b"absent".to_vec(),
        })
        .await;
        pool.dispatch(Command::Type {
            key: b"present".to_vec(),
        })
        .await;
        pool.dispatch(Command::Type {
            key: b"absent".to_vec(),
        })
        .await;
        pool.dispatch(Command::StrLen {
            key: b"empty".to_vec(),
        })
        .await;
        pool.dispatch(Command::StrLen {
            key: b"absent".to_vec(),
        })
        .await;
        // A broadcast, which no shard may count.
        pool.dispatch_every(Command::DbSize).await;

        let stats = gathered(&pool).await;
        assert_eq!(stats.hits, 5, "one hit from each of the five read forms");
        assert_eq!(stats.misses, 5, "and one miss from each");
        assert_eq!(stats.keys, 2);
        assert_eq!(stats.expires, 0);
        assert_eq!(
            stats.calls[usize::from(Command::Get { key: Vec::new() }.kind())],
            2
        );
        assert_eq!(
            stats.calls[usize::from(Command::DbSize.kind())],
            0,
            "a request the edge split across every shard is the edge's to count"
        );
        assert_eq!(
            stats.calls[usize::from(Command::Stats.kind())],
            0,
            "and so is the gathering this very assertion did"
        );
    }

    /// Expirations are counted by both halves under the one field.
    #[tokio::test(start_paused = true)]
    async fn expired_keys_counts_the_lazy_half_and_the_sweep() {
        let pool = ShardPool::spawn(2, 1, DictSeed { k0: 1, k1: 2 }, NoTrace);
        assert_eq!(pool.dispatch(set_ex(b"lazy", b"v", 30)).await, Reply::Ok);
        assert_eq!(gathered(&pool).await.expires, 1, "one dated key");
        tokio::time::advance(Duration::from_secs(31)).await;
        // A read in front of the key is the lazy half.
        assert_eq!(pool.dispatch(get(b"lazy")).await, Reply::Bulk(None));
        let stats = gathered(&pool).await;
        assert_eq!(stats.expired, 1);
        assert_eq!(stats.expires, 0, "and the deadline went with the key");
    }

    /// Every shard's counters, summed the way `INFO` sums them.
    async fn gathered(pool: &ShardPool) -> ShardStats {
        let mut total = ShardStats::default();
        for reply in pool.dispatch_every(Command::Stats).await {
            let Reply::Stats(stats) = reply else {
                panic!("{reply:?}")
            };
            total.keys += stats.keys;
            total.expires += stats.expires;
            total.evicted += stats.evicted;
            total.hits += stats.hits;
            total.misses += stats.misses;
            total.expired += stats.expired;
            for (sum, count) in total.calls.iter_mut().zip(stats.calls) {
                *sum += count;
            }
        }
        total
    }

    fn limited(ceiling: u64, mode: EvictionMode) -> ShardPool {
        ShardPool::spawn_limited(
            2,
            1,
            DictSeed { k0: 1, k1: 2 },
            NoTrace,
            MemoryLimit {
                ceiling: Some(ceiling),
                mode,
            },
        )
    }

    async fn evicted(pool: &ShardPool) -> u64 {
        pool.dispatch_every(Command::Stats)
            .await
            .into_iter()
            .map(|r| match r {
                Reply::Stats(s) => s.evicted,
                other => panic!("{other:?}"),
            })
            .sum()
    }

    /// Writes past the ceiling evict until the figure is under it again, and
    /// each eviction is counted.
    #[tokio::test]
    async fn allkeys_lru_evicts_until_the_write_fits() {
        let pool = limited(
            2 * 8 * crate::dict::BUCKET_OVERHEAD + 3 * crate::dict::entry_bytes(b"k0", &[0; 64]),
            EvictionMode::AllKeysLru,
        );
        for i in 0..3u8 {
            assert_eq!(
                pool.dispatch(set(&[b'k', b'0' + i], &[0u8; 64])).await,
                Reply::Ok
            );
        }
        assert_eq!(evicted(&pool).await, 0, "three entries fit exactly");
        assert_eq!(pool.dispatch(set(b"k9", &[0u8; 64])).await, Reply::Ok);
        assert!(pool.memory().used() <= pool.limit().ceiling.unwrap());
        assert_eq!(evicted(&pool).await, 1);
        assert_eq!(
            pool.dispatch(get(b"k9")).await,
            Reply::Bulk(Some(vec![0u8; 64])),
            "the write that evicted is itself kept"
        );
    }

    /// Under `noeviction` the write is refused, byte-exact, and nothing moves.
    #[tokio::test]
    async fn noeviction_refuses_a_write_over_the_ceiling_and_keeps_reads() {
        // The ceiling is what two empty tables already cost, so the first
        // write is the one that crosses it. The comparison is against what
        // the node held *before* the command, as Redis's is: a write is never
        // refused for the bytes it is about to add, only for the ones already
        // there, so the write that crosses the line lands and the next one is
        // refused.
        let pool = limited(
            2 * 8 * crate::dict::BUCKET_OVERHEAD,
            EvictionMode::NoEviction,
        );
        assert_eq!(pool.dispatch(set(b"k0", &[0u8; 64])).await, Reply::Ok);
        assert_eq!(
            pool.dispatch(set(b"k1", &[0u8; 64])).await,
            Reply::Error(ReplyError::OutOfMemory)
        );
        assert_eq!(
            pool.dispatch(get(b"k0")).await,
            Reply::Bulk(Some(vec![0u8; 64]))
        );
        assert_eq!(
            pool.dispatch(Command::Del {
                key: b"k0".to_vec()
            })
            .await,
            Reply::Removed(true),
            "a delete is never refused"
        );
        assert_eq!(
            ReplyError::OutOfMemory.wire_text(),
            "OOM command not allowed when used memory > 'maxmemory'."
        );
        assert_eq!(evicted(&pool).await, 0);
    }

    /// A shard past the ceiling still gives up a key when the command that
    /// took it there addressed the oldest one in the sample.
    ///
    /// `TTL` is `Route::Key` and never stamps, so the key it addresses keeps
    /// whatever stamp it already had — which may be the oldest the sample
    /// meets. Sparing that key has to exclude it from candidacy rather than
    /// abandon the loop: `None` from the sampler means "this shard has nothing
    /// to give", and a shard holding two other keys is not saying that.
    ///
    /// Reached through `evict_until_fits` directly rather than through the
    /// pool, because the state it needs cannot be built out of commands: a
    /// write stamps its own key newest, so the only way the spared key is also
    /// the oldest is a shard already over a ceiling somebody else's bytes
    /// pushed it past.
    #[test]
    fn a_command_that_does_not_stamp_still_evicts_past_the_ceiling() {
        let mut state = ShardState::new(Dict::with_seed(DictSeed { k0: 3, k1: 5 }), NoopLog);
        // Stamped in this order, so `spared` is the oldest of the three and
        // the sample — which meets all of them, being smaller than
        // `EVICTION_SAMPLES` — offers it first.
        for key in [b"spared".as_slice(), b"middle", b"newest"] {
            state.dict.insert(
                key.to_vec(),
                Entry {
                    value: vec![0u8; 64],
                    expires_at: None,
                    touched: 0,
                },
            );
            state.dict.touch(key);
        }
        let used = state.dict.used_bytes();
        let memory = Memory {
            gauge: MemoryGauge::default(),
            limit: MemoryLimit {
                // One key under what the shard is holding, so one eviction is
                // enough to get back under and the loop has an end.
                ceiling: Some(used - 1),
                mode: EvictionMode::AllKeysLru,
            },
        };
        memory.gauge.apply(0, used);

        evict_until_fits(
            &mut state,
            0,
            &memory,
            &NoTrace,
            &Deadlines,
            Some(b"spared"),
        );

        assert_eq!(
            state.dict.len(),
            2,
            "the loop abandoned a shard that still had room to make"
        );
        assert_eq!(state.evicted, 1);
        assert!(
            state.dict.get(b"spared").is_some(),
            "the spared key was evicted"
        );
    }

    /// A write that cannot fit even on an empty shard stops at empty rather
    /// than looping: the write lands, the figure is over, and the next write
    /// starts evicting again.
    #[tokio::test]
    async fn a_value_larger_than_the_ceiling_empties_the_shard_and_stops() {
        let pool = limited(1, EvictionMode::AllKeysLru);
        assert_eq!(pool.dispatch(set(b"big", &[0u8; 1024])).await, Reply::Ok);
        assert_eq!(
            pool.dispatch(get(b"big")).await,
            Reply::Bulk(Some(vec![0u8; 1024]))
        );
    }

    #[tokio::test]
    async fn commands_round_trip_through_the_pool() {
        let pool = ShardPool::spawn(16, 4, DictSeed { k0: 1, k1: 2 }, NoTrace);
        assert_eq!(pool.dispatch(set(b"k", b"v")).await, Reply::Ok);
        assert_eq!(
            pool.dispatch(Command::Get { key: b"k".to_vec() }).await,
            Reply::Bulk(Some(b"v".to_vec()))
        );
        assert_eq!(
            pool.dispatch(Command::IncrBy {
                key: b"n".to_vec(),
                delta: 5
            })
            .await,
            Reply::Integer(5)
        );
        assert_eq!(
            pool.dispatch(Command::IncrBy {
                key: b"n".to_vec(),
                delta: -2
            })
            .await,
            Reply::Integer(3)
        );
        assert_eq!(
            pool.dispatch(Command::IncrBy {
                key: b"k".to_vec(),
                delta: 1
            })
            .await,
            Reply::Error(ReplyError::NotAnInteger)
        );
        assert_eq!(
            pool.dispatch(Command::Del { key: b"k".to_vec() }).await,
            Reply::Removed(true)
        );
        assert_eq!(
            pool.dispatch(Command::Del { key: b"k".to_vec() }).await,
            Reply::Removed(false)
        );
        assert_eq!(
            pool.dispatch(Command::Get { key: b"k".to_vec() }).await,
            Reply::Bulk(None)
        );
    }

    /// The no-await rule, asserted structurally: this test is a plain `#[test]`
    /// with no runtime under it. If `apply` ever became `async`, or grew an
    /// `await`, this would stop compiling — which is the point. A comment
    /// saying "do not await here" would not.
    ///
    /// The `now` it passes is the same reading a handler would otherwise have
    /// taken for itself, and taking it here is what shows a handler does not
    /// need a clock — or a runtime to hold one.
    #[test]
    fn a_handler_runs_to_completion_without_a_runtime() {
        let mut state = ShardState::new(Dict::with_seed(DictSeed { k0: 7, k1: 9 }), NoopLog);
        let now = Instant::now();

        let stored = apply(&mut state, 0, &mut set(b"k", b"v"), now, &Deadlines);
        assert_eq!(stored, Reply::Ok);
        assert_eq!(
            apply(&mut state, 0, &mut get(b"k"), now, &Deadlines),
            Reply::Bulk(Some(b"v".to_vec()))
        );
    }

    /// A handler takes the command's value, and leaves everything the trace
    /// reads.
    ///
    /// `apply` moves a `Set`'s value into the dict instead of copying it, and
    /// the executor hands that same command to the [`TraceSink`] afterwards.
    /// So the division is load-bearing rather than incidental: a handler may
    /// take what the trace does not fold, and nothing else. Taking the key
    /// would move every recorded trace hash while every test that only reads
    /// the keyspace back stayed green — which is the one failure this pins.
    #[test]
    fn a_handler_takes_the_value_and_leaves_what_the_trace_reads() {
        let mut state = ShardState::new(Dict::with_seed(DictSeed { k0: 4, k1: 6 }), NoopLog);
        let mut cmd = set(b"k", b"v");

        assert_eq!(
            apply(&mut state, 0, &mut cmd, Instant::now(), &Deadlines),
            Reply::Ok
        );
        assert_eq!(
            cmd.route(),
            Route::Key(b"k"),
            "the trace folds the key after the handler"
        );
        assert_eq!(cmd.kind(), set(b"k", b"v").kind());
        assert_eq!(
            state.dict.get(b"k").map(|entry| entry.value.clone()),
            Some(b"v".to_vec())
        );
        // The other half of the same fact: the dict holds the only copy of the
        // value, because the command no longer has one. A `SET` that copied it
        // would leave both, which is the cost this arrangement exists to avoid.
        assert!(
            matches!(&cmd, Command::Set { value, .. } if value.is_empty()),
            "the value was copied into the dict rather than moved"
        );
    }

    #[test]
    fn seq_advances_only_for_commands_that_change_something() {
        let mut state = ShardState::new(Dict::with_seed(DictSeed { k0: 1, k1: 1 }), NoopLog);
        let now = Instant::now();
        let run = |state: &mut ShardState<NoopLog>, mut cmd: Command| {
            apply(state, 3, &mut cmd, now, &Deadlines)
        };

        // A read moves nothing.
        run(&mut state, Command::Get { key: b"a".to_vec() });
        assert_eq!(state.seq, 0);

        // A write does.
        run(&mut state, set(b"a", b"1"));
        assert_eq!(state.seq, 1);

        // A delete that removes nothing writes no record: replaying it would
        // be a no-op, so the log should not carry it.
        run(
            &mut state,
            Command::Del {
                key: b"absent".to_vec(),
            },
        );
        assert_eq!(state.seq, 1);

        // A rejected IncrBy likewise.
        run(
            &mut state,
            Command::IncrBy {
                key: b"a".to_vec(),
                delta: 1,
            },
        );
        assert_eq!(
            state.seq, 2,
            "'1' is a valid integer, so this one does count"
        );

        run(&mut state, set(b"txt", b"abc"));
        assert_eq!(state.seq, 3);
        run(
            &mut state,
            Command::IncrBy {
                key: b"txt".to_vec(),
                delta: 1,
            },
        );
        assert_eq!(
            state.seq, 3,
            "a rejected IncrBy must not consume a sequence number"
        );

        // And a delete that does remove something.
        run(&mut state, Command::Del { key: b"a".to_vec() });
        assert_eq!(state.seq, 4);
    }

    #[test]
    fn parse_i64_accepts_only_the_canonical_spelling() {
        assert_eq!(parse_i64(b"0"), Some(0));
        assert_eq!(parse_i64(b"7"), Some(7));
        assert_eq!(parse_i64(b"-7"), Some(-7));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));

        for rejected in [
            &b""[..],
            b"-",
            b"007",
            b"-0",
            b"-007",
            b"+7",
            b" 7",
            b"7 ",
            b"7.0",
            b"seven",
            b"9223372036854775808",  // i64::MAX + 1
            b"-9223372036854775809", // i64::MIN - 1
            b"\xff",
        ] {
            assert_eq!(parse_i64(rejected), None, "input {rejected:?}");
        }
    }

    #[test]
    fn every_i64_round_trips_through_its_stored_form() {
        // The property `apply` relies on: what `IncrBy` writes is what
        // `parse_i64` reads back.
        for n in [
            0,
            1,
            -1,
            10,
            -10,
            99,
            -100,
            i64::MAX,
            i64::MIN,
            i64::MAX - 1,
            i64::MIN + 1,
        ] {
            assert_eq!(parse_i64(n.to_string().as_bytes()), Some(n), "value {n}");
        }
    }

    #[tokio::test]
    async fn incr_by_reports_overflow_instead_of_panicking() {
        let pool = ShardPool::spawn(4, 4, DictSeed { k0: 3, k1: 4 }, NoTrace);
        assert_eq!(
            pool.dispatch(set(b"c", i64::MAX.to_string().as_bytes()))
                .await,
            Reply::Ok
        );
        assert_eq!(
            pool.dispatch(Command::IncrBy {
                key: b"c".to_vec(),
                delta: 1
            })
            .await,
            Reply::Error(ReplyError::WouldOverflow)
        );
        // The value is untouched.
        assert_eq!(
            pool.dispatch(Command::Get { key: b"c".to_vec() }).await,
            Reply::Bulk(Some(i64::MAX.to_string().into_bytes()))
        );
    }

    #[tokio::test]
    async fn a_missing_counter_starts_at_zero_and_set_overwrites() {
        let pool = ShardPool::spawn(8, 4, DictSeed { k0: 5, k1: 6 }, NoTrace);
        assert_eq!(
            pool.dispatch(Command::IncrBy {
                key: b"fresh".to_vec(),
                delta: -3
            })
            .await,
            Reply::Integer(-3)
        );
        assert_eq!(pool.dispatch(set(b"fresh", b"100")).await, Reply::Ok);
        assert_eq!(
            pool.dispatch(Command::IncrBy {
                key: b"fresh".to_vec(),
                delta: 1
            })
            .await,
            Reply::Integer(101)
        );
    }

    #[tokio::test]
    async fn keys_spread_over_shards_and_every_one_survives_growth() {
        // 16 shards, enough keys that several dicts outgrow their initial
        // eight buckets and rehash while the writes keep coming.
        let pool = ShardPool::spawn(16, 4, DictSeed { k0: 11, k1: 13 }, NoTrace);
        let keys: Vec<Vec<u8>> = (0..600u32)
            .map(|i| format!("key:{i}").into_bytes())
            .collect();

        for (i, key) in keys.iter().enumerate() {
            assert_eq!(
                pool.dispatch(set(key, i.to_string().as_bytes())).await,
                Reply::Ok
            );
        }
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(
                pool.dispatch(Command::Get { key: key.clone() }).await,
                Reply::Bulk(Some(i.to_string().into_bytes())),
                "key {key:?} lost across a rehash"
            );
        }

        // And they really did land on more than one shard, or the test above
        // proves nothing about routing.
        let mut shards: Vec<u16> = keys.iter().map(|k| shard_of(k, 16)).collect();
        shards.sort_unstable();
        shards.dedup();
        assert!(shards.len() > 1, "every key routed to one shard");
    }

    #[tokio::test]
    async fn a_batch_is_answered_in_request_order_across_executors() {
        let pool = ShardPool::spawn(16, 4, DictSeed { k0: 9, k1: 9 }, NoTrace);
        // Keys chosen to land on more than one executor, interleaved on purpose.
        let keys: Vec<Vec<u8>> = (0..64u32)
            .map(|i| format!("key:{i}").into_bytes())
            .collect();
        let executors_hit: std::collections::BTreeSet<u16> = keys
            .iter()
            .map(|k| executor_of(shard_of(k, 16), 16, 4))
            .collect();
        assert!(
            executors_hit.len() > 1,
            "test keys all landed on one executor"
        );

        let sets: Vec<Command> = keys
            .iter()
            .enumerate()
            .map(|(i, key)| set(key, i.to_string().as_bytes()))
            .collect();
        let replies = pool.dispatch_many(sets).await;
        assert!(replies.iter().all(|r| *r == Reply::Ok));

        let gets: Vec<Command> = keys
            .iter()
            .map(|key| Command::Get { key: key.clone() })
            .collect();
        let replies = pool.dispatch_many(gets).await;
        for (i, reply) in replies.iter().enumerate() {
            assert_eq!(
                *reply,
                Reply::Bulk(Some(i.to_string().into_bytes())),
                "reply {i} out of order or wrong"
            );
        }
    }

    /// `count` distinct keys that hash to `shard`, found by search — the seed
    /// is fixed, so the search is deterministic and the test does not depend
    /// on which keys it finds.
    ///
    /// The search is bounded rather than open-ended: an unbounded one would
    /// hang forever on the day `shard_of` stopped reaching some shard, which
    /// is precisely the bug a caller is using this to rule out.
    fn keys_landing_on(shard: u16, shards: u16, count: usize) -> Vec<Vec<u8>> {
        let keys: Vec<Vec<u8>> = (0u32..10_000)
            .map(|n| format!("probe-{n}").into_bytes())
            .filter(|k| shard_of(k, shards) == shard)
            .take(count)
            .collect();
        assert_eq!(
            keys.len(),
            count,
            "ten thousand probes reach every shard of a small pool {count} times"
        );
        keys
    }

    /// Shard `i` is given `i + 1` keys, so every shard's `DBSIZE` differs from
    /// every other's and the replies can only be compared in one order.
    ///
    /// A broadcast that answered a key each would pass any permutation: the
    /// order is claimed by [`Route::Every`] and by
    /// [`Router::dispatch_every`], and a claim no test can fail is not held.
    #[tokio::test]
    async fn a_broadcast_is_answered_once_per_shard_in_shard_order() {
        let pool = ShardPool::spawn(8, 4, DictSeed { k0: 1, k1: 1 }, NoTrace);
        for i in 0..8u16 {
            for key in keys_landing_on(i, 8, usize::from(i) + 1) {
                pool.dispatch(Command::Set {
                    key,
                    value: b"v".to_vec(),
                    expiry: None,
                    cond: None,
                    keep_ttl: false,
                    get: false,
                })
                .await;
            }
        }
        let replies = pool.dispatch_every(Command::DbSize).await;
        let expected: Vec<Reply> = (1..=8).map(Reply::Integer).collect();
        assert_eq!(
            replies, expected,
            "a broadcast answered out of shard order, or not once per shard"
        );
    }

    /// The budget the edge accounts a crossing call by: how many buckets one
    /// step actually walked, which is not the count it was asked for whenever
    /// the step finished the cycle first.
    #[test]
    fn a_scan_step_reports_how_many_buckets_it_visited() {
        let mut dict = Dict::with_seed(DictSeed { k0: 1, k1: 1 });
        // Four keys in a table of eight buckets: the load factor is one, so
        // nothing has grown and the cycle is exactly eight steps long.
        for i in 0..4u8 {
            dict.insert(
                vec![i],
                Entry {
                    value: Vec::new(),
                    expires_at: None,
                    touched: 0,
                },
            );
        }
        let now = Instant::now();

        let Reply::Scan {
            visited, cursor, ..
        } = scan_step(&dict, 0, 3, None, now, &Deadlines)
        else {
            panic!("a scan step must answer Reply::Scan");
        };
        assert_eq!(visited, 3);
        assert_ne!(cursor, 0);

        let Reply::Scan {
            visited, cursor, ..
        } = scan_step(&dict, 0, 100, None, now, &Deadlines)
        else {
            panic!("a scan step must answer Reply::Scan");
        };
        assert_eq!(
            visited, 8,
            "a step that finishes the cycle visited the whole table, not its budget"
        );
        assert_eq!(cursor, 0);
    }

    #[tokio::test]
    async fn a_scan_step_returns_at_most_a_countful_and_a_resumable_cursor() {
        let pool = ShardPool::spawn(1, 1, DictSeed { k0: 1, k1: 1 }, NoTrace);
        for i in 0..50u32 {
            pool.dispatch(set(format!("k{i}").as_bytes(), b"v")).await;
        }

        let mut seen: Vec<Vec<u8>> = Vec::new();
        let mut cursor = 0u64;
        let mut calls = 0;
        loop {
            let reply = pool
                .dispatch_at(
                    0,
                    Command::ScanStep {
                        cursor,
                        count: 10,
                        pattern: None,
                    },
                )
                .await;
            let Reply::Scan {
                cursor: next, keys, ..
            } = reply
            else {
                panic!("a scan step must answer Reply::Scan, got {reply:?}");
            };
            seen.extend(keys);
            cursor = next;
            calls += 1;
            // A guard rather than an acceptance criterion — the criteria are
            // the two assertions below. Without it, a cursor that never closes
            // its cycle would wedge the suite instead of reporting itself.
            assert!(calls < 200, "a 50-key scan did not terminate");
            if cursor == 0 {
                break;
            }
        }
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), 50, "every key must be seen at least once");
        assert!(
            calls > 1,
            "COUNT 10 over 50 keys must take more than one call"
        );
    }

    #[tokio::test]
    async fn a_scan_step_filters_by_pattern_inside_the_shard() {
        let pool = ShardPool::spawn(1, 1, DictSeed { k0: 1, k1: 1 }, NoTrace);
        for name in ["alpha", "album", "beta"] {
            pool.dispatch(set(name.as_bytes(), b"v")).await;
        }
        let mut seen: Vec<Vec<u8>> = Vec::new();
        let mut cursor = 0u64;
        loop {
            let Reply::Scan {
                cursor: next, keys, ..
            } = pool
                .dispatch_at(
                    0,
                    Command::ScanStep {
                        cursor,
                        count: 100,
                        pattern: Some(b"al*".to_vec()),
                    },
                )
                .await
            else {
                panic!("expected Reply::Scan");
            };
            seen.extend(keys);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        seen.sort();
        assert_eq!(seen, vec![b"album".to_vec(), b"alpha".to_vec()]);
    }

    #[tokio::test]
    async fn an_empty_batch_answers_immediately_with_nothing() {
        let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 1 }, NoTrace);
        assert_eq!(pool.dispatch_many(Vec::new()).await, Vec::new());
    }

    /// The default implementation is the compatibility contract: a router that
    /// only knows `dispatch` must serve batches, one command at a time, in order.
    #[tokio::test]
    async fn the_default_dispatch_many_loops_dispatch_in_order() {
        #[derive(Clone)]
        struct Echo;
        impl Router for Echo {
            async fn dispatch(&self, cmd: Command) -> Reply {
                match cmd.route() {
                    Route::Key(key) => Reply::Bulk(Some(key.to_vec())),
                    Route::Shard(_) | Route::Every | Route::Unaddressed => Reply::Ok,
                }
            }
            /// This router hosts no shards and says so: `0..0` is a range,
            /// and it is empty, which is the honest answer for something that
            /// answers every command out of thin air.
            fn shards(&self) -> u16 {
                0
            }
            /// This router hosts no shards, so there is none to address.
            async fn dispatch_at(&self, _shard: u16, cmd: Command) -> Reply {
                self.dispatch(cmd).await
            }
            /// This router hosts no shards, so a broadcast reaches nothing.
            async fn dispatch_every(&self, _cmd: Command) -> Vec<Reply> {
                Vec::new()
            }
        }
        let replies = Echo
            .dispatch_many(vec![
                Command::Get { key: b"a".to_vec() },
                Command::Get { key: b"b".to_vec() },
            ])
            .await;
        assert_eq!(
            replies,
            vec![
                Reply::Bulk(Some(b"a".to_vec())),
                Reply::Bulk(Some(b"b".to_vec()))
            ]
        );
    }

    /// One observed call: the shard, the replication position where the
    /// command's effects began, the command's kind tag, and the reply.
    ///
    /// Not "the position it ran at" and not "the record it wrote" — see
    /// [`TraceSink::record`], whose doc is the definition this restates.
    type Observed = (u16, u64, u8, Reply);

    #[derive(Clone, Default)]
    struct Recorder(Arc<Mutex<Vec<Observed>>>);

    impl TraceSink for Recorder {
        fn record(&self, shard: u16, seq: u64, cmd: &Command, reply: &Reply) {
            self.0
                .lock()
                .expect("recorder mutex")
                .push((shard, seq, cmd.kind(), reply.clone()));
        }
    }

    #[tokio::test]
    async fn the_sink_sees_every_command_at_its_replication_position() {
        let sink = Recorder::default();
        // One shard, so every command shares a `seq` counter and the observed
        // positions are a single sequence rather than an interleaving.
        let pool = ShardPool::spawn(1, 1, DictSeed { k0: 2, k1: 3 }, sink.clone());

        pool.dispatch(Command::Get { key: b"k".to_vec() }).await;
        pool.dispatch(set(b"k", b"1")).await;
        pool.dispatch(Command::IncrBy {
            key: b"k".to_vec(),
            delta: 4,
        })
        .await;
        pool.dispatch(Command::Del {
            key: b"gone".to_vec(),
        })
        .await;

        let seen = sink.0.lock().expect("recorder mutex").clone();
        assert_eq!(
            seen,
            vec![
                // The read ran at position 0 and did not consume it.
                (0, 0, 1, Reply::Bulk(None)),
                (0, 0, 2, Reply::Ok),
                (0, 1, 4, Reply::Integer(5)),
                // The delete found nothing, so it did not consume position 2.
                (0, 2, 3, Reply::Removed(false)),
            ]
        );
    }

    #[tokio::test]
    async fn shards_sharing_an_executor_keep_independent_replication_positions() {
        let sink = Recorder::default();
        // Four shards on one executor: every shard's state lives in one task, and
        // the positions must still be per shard, not per executor.
        let pool = ShardPool::spawn(4, 1, DictSeed { k0: 2, k1: 3 }, sink.clone());

        // Two keys on two different shards (probe until found).
        let keys: Vec<Vec<u8>> = (0..32u32).map(|i| format!("k{i}").into_bytes()).collect();
        let a = keys
            .iter()
            .find(|k| shard_of(k, 4) == 0)
            .expect("a key on shard 0")
            .clone();
        let b = keys
            .iter()
            .find(|k| shard_of(k, 4) == 1)
            .expect("a key on shard 1")
            .clone();

        for key in [&a, &b, &a, &b] {
            pool.dispatch(set(key, b"v")).await;
        }
        let seen = sink.0.lock().expect("recorder mutex").clone();
        let positions: Vec<(u16, u64)> = seen
            .iter()
            .map(|(shard, seq, _, _)| (*shard, *seq))
            .collect();
        assert_eq!(positions, vec![(0, 0), (1, 0), (0, 1), (1, 1)]);
    }

    /// The seam, exercised rather than asserted.
    ///
    /// A log that is not [`NoopLog`] reaches a shard and sees every mutation at
    /// its replication position. Until the pool took a log factory this test
    /// could not be written at all, which is what made "the seam exists from
    /// day one" a claim about intent rather than about the code.
    #[tokio::test]
    async fn a_supplied_log_receives_every_mutation() {
        #[derive(Clone, Default)]
        struct Recording(Arc<Mutex<Vec<(u16, u64)>>>);

        impl ReplicationLog for Recording {
            fn append(&mut self, rec: Record<'_>) -> std::io::Result<()> {
                self.0.lock().expect("log mutex").push((rec.shard, rec.seq));
                Ok(())
            }
            fn sync(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let log = Recording::default();
        let pool = ShardPool::spawn_with_log(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace, {
            let log = log.clone();
            move |_shard| log.clone()
        });

        pool.dispatch(set(b"k", b"v")).await;
        pool.dispatch(Command::Get { key: b"k".to_vec() }).await;
        pool.dispatch(Command::Del {
            key: b"absent".to_vec(),
        })
        .await;
        pool.dispatch(Command::IncrBy {
            key: b"n".to_vec(),
            delta: 1,
        })
        .await;

        // The read and the delete-that-removed-nothing append nothing, so the
        // positions are gapless — the same property `seq` is asserted to have.
        assert_eq!(*log.0.lock().expect("log mutex"), vec![(0, 0), (0, 1)]);
    }

    /// A mutation whose record cannot be written must not happen.
    ///
    /// `apply`'s documentation says the record is appended *before* the dict is
    /// touched, so a record can never describe a change that was not made and a
    /// change can never outrun its record. With only [`NoopLog`] reachable,
    /// nothing could fail an append and that ordering had no coverage at all.
    #[tokio::test]
    async fn a_log_that_cannot_write_refuses_the_mutation() {
        struct Failing;

        impl ReplicationLog for Failing {
            fn append(&mut self, _rec: Record<'_>) -> std::io::Result<()> {
                Err(std::io::Error::other("the disk went away"))
            }
            fn sync(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let pool =
            ShardPool::spawn_with_log(1, 1, DictSeed { k0: 1, k1: 2 }, NoTrace, |_shard| Failing);

        assert_eq!(
            pool.dispatch(set(b"k", b"v")).await,
            Reply::Error(ReplyError::LogWriteFailed)
        );
        // And the write did not land: the refusal is not cosmetic.
        assert_eq!(
            pool.dispatch(Command::Get { key: b"k".to_vec() }).await,
            Reply::Bulk(None),
            "the value was stored despite its record failing"
        );
        // An unloggable IncrBy is refused for the same reason, rather than
        // incrementing and reporting a number nothing recorded.
        assert_eq!(
            pool.dispatch(Command::IncrBy {
                key: b"n".to_vec(),
                delta: 5
            })
            .await,
            Reply::Error(ReplyError::LogWriteFailed)
        );
    }

    #[tokio::test]
    #[should_panic(expected = "shards must be greater than zero")]
    async fn a_pool_of_no_shards_is_a_programming_error() {
        ShardPool::spawn(0, 1, DictSeed { k0: 0, k1: 0 }, NoTrace);
    }

    #[tokio::test]
    #[should_panic(expected = "executors must be in 1..=shards")]
    async fn a_pool_of_no_executors_is_a_programming_error() {
        ShardPool::spawn(4, 0, DictSeed { k0: 0, k1: 0 }, NoTrace);
    }

    /// More executors than shards would leave one owning nothing, which the
    /// partition function has no way to express and no caller has a use for.
    #[tokio::test]
    #[should_panic(expected = "executors must be in 1..=shards")]
    async fn a_pool_with_more_executors_than_shards_is_a_programming_error() {
        ShardPool::spawn(4, 5, DictSeed { k0: 0, k1: 0 }, NoTrace);
    }

    /// A policy that never finds anything due, which is what a server with no
    /// liveness check and no working sweep looks like from the outside.
    ///
    /// It is defined here, in a test module, and in `seedstone-sim` for the
    /// plants — never in a production path. That is the whole point of the
    /// parameter: the defective policies are unlinkable from the binary
    /// because they are not in a crate it depends on.
    #[derive(Clone, Copy)]
    struct NeverDue;

    impl WalkOrder for NeverDue {}

    impl ExpiryPolicy for NeverDue {
        fn due_on_read(&self, _expires_at: Option<Instant>, _now: Instant) -> bool {
            false
        }
        fn due_on_sweep(&self, _expires_at: Option<Instant>, _now: Instant) -> bool {
            false
        }
        fn takes_undated(&self) -> bool {
            false
        }
    }

    impl EvictionPolicy for NeverDue {
        fn must_evict(&self, used: u64, ceiling: Option<u64>) -> bool {
            Deadlines.must_evict(used, ceiling)
        }
    }

    /// Honest in front of a command, inert on the housekeeping tick.
    ///
    /// What the two trace tests below need is a keyspace where only the lazy
    /// path can remove anything, so that what they assert about positions is
    /// a property of the code and not of when the tick happened to fire.
    #[derive(Clone, Copy)]
    struct NoSweep;

    impl WalkOrder for NoSweep {}

    impl ExpiryPolicy for NoSweep {
        fn due_on_read(&self, expires_at: Option<Instant>, now: Instant) -> bool {
            Deadlines.due_on_read(expires_at, now)
        }
        fn due_on_sweep(&self, _expires_at: Option<Instant>, _now: Instant) -> bool {
            false
        }
        fn takes_undated(&self) -> bool {
            false
        }
    }

    impl EvictionPolicy for NoSweep {
        fn must_evict(&self, used: u64, ceiling: Option<u64>) -> bool {
            Deadlines.must_evict(used, ceiling)
        }
    }

    /// The defect this seam exists to be able to plant: the deadline is
    /// stored, the clock passes it, and the key is still there — on both paths
    /// at once, which is what makes it a missing expiry rather than a slow
    /// one.
    #[tokio::test(start_paused = true)]
    async fn a_pool_spawned_with_a_policy_expires_by_that_policy() {
        let sink = Recorder::default();
        let pool = ShardPool::spawn_with_policy(1, 1, DictSeed { k0: 2, k1: 3 }, sink, NeverDue);
        assert_eq!(pool.dispatch(set_ex(b"k", b"v", 1)).await, Reply::Ok);

        // Past the deadline, and past enough housekeeping ticks for the sweep
        // to have walked the whole table several times over.
        tokio::time::advance(Duration::from_secs(2)).await;
        for _ in 0..8 {
            tokio::time::advance(HOUSEKEEPING_TICK).await;
        }

        assert_eq!(
            pool.dispatch(get(b"k")).await,
            Reply::Bulk(Some(b"v".to_vec())),
            "the policy said nothing was due, so the key must still answer"
        );
    }

    /// A policy that reclaims whenever it is asked, whatever the gauge says
    /// and whether or not there is a ceiling at all.
    ///
    /// The eviction counterpart of [`NeverDue`], and it lives here for the
    /// same reason: the defective answers belong in a crate the binary does
    /// not depend on.
    #[derive(Clone, Copy)]
    struct AlwaysEvicts;

    impl WalkOrder for AlwaysEvicts {}

    impl ExpiryPolicy for AlwaysEvicts {
        fn due_on_read(&self, expires_at: Option<Instant>, now: Instant) -> bool {
            Deadlines.due_on_read(expires_at, now)
        }
        fn due_on_sweep(&self, expires_at: Option<Instant>, now: Instant) -> bool {
            Deadlines.due_on_sweep(expires_at, now)
        }
        fn takes_undated(&self) -> bool {
            Deadlines.takes_undated()
        }
    }

    impl EvictionPolicy for AlwaysEvicts {
        fn must_evict(&self, _used: u64, _ceiling: Option<u64>) -> bool {
            true
        }
    }

    /// Whether to reclaim is the policy's answer and not a comparison the
    /// executor makes for itself.
    ///
    /// Two keys, and the *older* one is the one read back: `evict_until_fits`
    /// never takes the key the triggering command addressed, so a one-key
    /// shard under this policy keeps its one key. With two, the second write
    /// reclaims the first and the shard drains to exactly what was written
    /// last — which is the property, stated exactly.
    ///
    /// Spawned with no ceiling on purpose. The limit's mode decides *refuse
    /// or reclaim* and [`MemoryLimit::default`]'s is `AllKeysLru`, so the
    /// loop runs and asks the policy; the policy decides *whether now*. The
    /// honest half below is what makes that a claim rather than a
    /// coincidence: [`Deadlines`] under no ceiling never evicts anything.
    #[tokio::test]
    async fn the_eviction_decision_is_the_policys() {
        let pool =
            ShardPool::spawn_with_policy(1, 1, DictSeed { k0: 2, k1: 3 }, NoTrace, AlwaysEvicts);
        assert_eq!(pool.dispatch(set(b"a", b"v")).await, Reply::Ok);
        assert_eq!(pool.dispatch(set(b"b", b"v")).await, Reply::Ok);
        assert_eq!(
            pool.dispatch(get(b"a")).await,
            Reply::Bulk(None),
            "a policy that always evicts keeps only what was written last"
        );

        let honest = ShardPool::spawn(1, 1, DictSeed { k0: 2, k1: 3 }, NoTrace);
        assert_eq!(honest.dispatch(set(b"a", b"v")).await, Reply::Ok);
        assert_eq!(honest.dispatch(set(b"b", b"v")).await, Reply::Ok);
        assert_eq!(
            honest.dispatch(get(b"a")).await,
            Reply::Bulk(Some(b"v".to_vec())),
            "the honest policy under no ceiling reclaims nothing"
        );
    }

    /// The counterpart, and the reason the test above proves anything: the
    /// honest policy is what `spawn` uses, and it does expire the key.
    #[tokio::test(start_paused = true)]
    async fn the_default_policy_is_the_honest_one() {
        let sink = Recorder::default();
        let pool = ShardPool::spawn(1, 1, DictSeed { k0: 2, k1: 3 }, sink);
        assert_eq!(pool.dispatch(set_ex(b"k", b"v", 1)).await, Reply::Ok);
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(pool.dispatch(get(b"k")).await, Reply::Bulk(None));
    }
}
