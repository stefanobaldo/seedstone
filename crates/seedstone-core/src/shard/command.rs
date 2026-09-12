//! What a shard is asked to do: the command set, how each command is routed,
//! and the kind tags the edge counts by.

/// How long a `Set` asks its key to live, in the unit the client chose.
///
/// Kept in that unit rather than resolved to a [`std::time::Duration`] at the service
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
        /// Redis 6.2.24 and 8.10.1 do.
        seconds: u64,
        /// The bytes to store, kept verbatim.
        value: Vec<u8>,
    },
    /// `SETNX key value` — `SET key value NX` under the name Redis gave it
    /// before `SET` grew options, which redis-py's `setnx()` still puts on
    /// the wire.
    ///
    /// Its own variant for [`SetEx`](Self::SetEx)'s reason and one more. A
    /// shard counts what it runs by variant and Redis reports
    /// `cmdstat_setnx` apart from `cmdstat_set` (6.2.24), so folding it into
    /// [`Set`](Self::Set) would make `cmdstat_set` count commands no peer
    /// spelled that way. The one more is the reply: `SET … NX` answers `+OK`
    /// or a nil bulk, and `SETNX` answers `:1` or `:0` (6.2.24, 8.10.1) —
    /// the same decision reported in a different type, which an alias built
    /// at the edge could not express. A client library reading that reply as
    /// a boolean, which is what redis-py's `setnx()` does, would read a
    /// truthy `OK` for a write that was refused.
    SetNx {
        /// The key to write, only if it is not already there.
        key: Vec<u8>,
        /// The bytes to store, kept verbatim.
        value: Vec<u8>,
    },
    /// `PSETEX key milliseconds value` — `SET key value PX milliseconds`
    /// under the name Redis gave it before `SET` grew options.
    ///
    /// Its own variant for [`SetEx`](Self::SetEx)'s reason: Redis reports
    /// `cmdstat_psetex` apart from `cmdstat_set` (6.2.24), and the
    /// error-reply log needs a counter to correlate its `PSETEX` against. The
    /// write itself is [`Set`](Self::Set)'s, through the same handler, and so
    /// is the reply — unlike [`SetNx`](Self::SetNx), this spelling answers
    /// exactly what the option spelling answers.
    PSetEx {
        /// The key to write.
        key: Vec<u8>,
        /// How many milliseconds from now the key dies. Strictly positive:
        /// the service layer refuses zero and negatives before dispatch, as
        /// Redis 6.2.24 and 8.10.1 do.
        millis: u64,
        /// The bytes to store, kept verbatim.
        value: Vec<u8>,
    },
    /// Report how long `key` has left, in milliseconds.
    ///
    /// [`Ttl`](Self::Ttl) in the unit the deadline is actually kept in. Its
    /// own variant for `SETEX`'s reason: a shard counts what it runs by
    /// variant, and Redis reports `cmdstat_pttl` apart from `cmdstat_ttl`
    /// (6.2.24, 8.10.1). The read is `Ttl`'s arm without the rounding.
    PTtl {
        /// The key to ask about.
        key: Vec<u8>,
    },
    /// Give `key` the deadline `EXPIREAT` named, or delete it if that deadline
    /// has passed.
    ///
    /// The shard has no wall clock, so the absolute Unix time the client sent
    /// is resolved at the edge — exactly as `SET … EXAT` is — into the span
    /// carried here, and from there this is [`PExpire`](Self::PExpire) through
    /// the same handler: a positive span is a deadline, any other span is a
    /// deletion answered `1`. Its own variant for `cmdstat_expireat` (6.2.24,
    /// 8.10.1).
    ExpireAt {
        /// The key to put a deadline on.
        key: Vec<u8>,
        /// Milliseconds left until the deadline the client named, as the edge
        /// computed them; zero or negative means the deadline has passed.
        millis: i64,
    },
    /// [`ExpireAt`](Self::ExpireAt) for `PEXPIREAT`: the same resolved span,
    /// counted under its own name (`cmdstat_pexpireat`, 6.2.24 and 8.10.1).
    PExpireAt {
        /// The key to put a deadline on.
        key: Vec<u8>,
        /// Milliseconds left until the deadline the client named.
        millis: i64,
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
    /// [`crate::shard::Router::dispatch_at`]: a step carries where it is in a shard's table,
    /// not which shard's table it is.
    ScanStep {
        /// Where in this shard's cycle to resume. `0` starts one.
        cursor: u64,
        /// How many cursor steps to take before answering. Each step covers
        /// one bucket of the table, or — while a rehash is in flight — one of
        /// the smaller table and the ones of the larger it expands into, which
        /// is the same accounting [`crate::dict::Dict::expire_step`] uses.
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
    /// It is answered by the executor rather than by [`crate::shard::apply`], because what
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
/// through [`crate::shard::Router::dispatch_at`] — an argument the caller has already
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
    /// [`crate::shard::Router::dispatch_at`].
    ///
    /// A router asked to route this on its own has no answer, and says so
    /// with [`crate::shard::ReplyError::ShardUnavailable`] rather than picking a shard. The
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
            | Self::SetNx { key, .. }
            | Self::PSetEx { key, .. }
            | Self::Del { key }
            | Self::IncrBy { key, .. }
            | Self::Expire { key, .. }
            | Self::PExpire { key, .. }
            | Self::Ttl { key }
            | Self::Persist { key }
            | Self::Exists { key }
            | Self::Type { key }
            | Self::StrLen { key }
            | Self::PTtl { key }
            | Self::ExpireAt { key, .. }
            | Self::PExpireAt { key, .. } => Route::Key(key),
            Self::FlushDb | Self::DbSize | Self::Stats => Route::Every,
            // The one route that is not self-sufficient. A step knows where it
            // is in *a* shard's table and not which shard's, so it names no
            // shard and the caller supplies the real one through
            // [`crate::shard::Router::dispatch_at`].
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
    pub const KIND_MAX: u8 = 21;

    /// A stable one-byte tag for this command's variant.
    ///
    /// `Get` = 1, `Set` = 2, `Del` = 3, `IncrBy` = 4, `Expire` = 5, `Ttl` = 6,
    /// `Exists` = 7, `FlushDb` = 8, `DbSize` = 9, `ScanStep` = 10,
    /// `PExpire` = 11, `Persist` = 12, `Type` = 13, `StrLen` = 14,
    /// `Stats` = 15, `SetEx` = 16, `SetNx` = 17, `PSetEx` = 18,
    /// `PTtl` = 19, `ExpireAt` = 20, `PExpireAt` = 21. These
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
            Self::SetNx { .. } => 17,
            Self::PSetEx { .. } => 18,
            Self::PTtl { .. } => 19,
            Self::ExpireAt { .. } => 20,
            Self::PExpireAt { .. } => 21,
        }
    }

    /// Whether this command is refused under `noeviction` once the gauge is
    /// past the ceiling: the ones that add bytes. Redis's `denyoom` flag.
    ///
    /// `Set`, `SetEx`, `SetNx`, `PSetEx` and `IncrBy` and nothing else. `Expire` and
    /// `Persist` rewrite a
    /// field on an entry already there, and every other command either reads
    /// or reclaims — refusing those would leave a full node with no way back
    /// under its ceiling.
    #[must_use]
    pub const fn denied_when_full(&self) -> bool {
        matches!(
            self,
            Self::Set { .. }
                | Self::SetEx { .. }
                | Self::SetNx { .. }
                | Self::PSetEx { .. }
                | Self::IncrBy { .. }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::tests::{set, setex};

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
            Command::SetNx {
                key: Vec::new(),
                value: Vec::new(),
            },
            Command::PSetEx {
                key: Vec::new(),
                millis: 1,
                value: Vec::new(),
            },
            Command::PTtl { key: Vec::new() },
            Command::ExpireAt {
                key: Vec::new(),
                millis: 1,
            },
            Command::PExpireAt {
                key: Vec::new(),
                millis: 1,
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
                | Command::SetEx { .. }
                | Command::SetNx { .. }
                | Command::PSetEx { .. }
                | Command::PTtl { .. }
                | Command::ExpireAt { .. }
                | Command::PExpireAt { .. } => {}
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
        let keyed: [Command; 15] = [
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
            Command::PTtl { key: b"k".to_vec() },
            Command::ExpireAt {
                key: b"k".to_vec(),
                millis: 1,
            },
            Command::PExpireAt {
                key: b"k".to_vec(),
                millis: 1,
            },
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
}
