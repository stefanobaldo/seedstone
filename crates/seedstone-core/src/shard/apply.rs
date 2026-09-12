//! The interpreter: one command against one shard's dictionary, its reply,
//! and the replication record it appends when it changed something. Expiry
//! is resolved lazily here and by the sweep in [`crate::shard::executor`].

use crate::dict::{Dict, Entry};
use crate::glob;
use crate::log::{Record, ReplicationLog};
use crate::shard::{
    Command, Cond, Expiry, ExpiryPolicy, Reply, ReplyError, Route, ShardPolicy, ShardState,
    keyspace_stats,
};
use std::time::Duration;
use tokio::time::Instant;

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
/// LRU stamp and took the dispatcher one line over the limit. `SetNx` was the
/// second to hit it, on the commit that added it, and is now [`set_nx`]. The
/// same commit took the dispatcher over a second time, and `Del` — named
/// here as the nearest for as long as this comment has existed — is now
/// [`del`]. `Expire` is the nearest of what is left.
///
/// The ordering is the half with no judgement in it, and it is what the next
/// extraction has to respect: every extracted handler sits below in the order
/// the match dispatches, so following a command from its arm to its handler is
/// a pass rather than a search. The helpers that belong to no single arm —
/// [`evict_if_expired`], [`deadline`], [`remaining_seconds`], [`append`] —
/// follow those handlers as their own group.
pub fn apply<L: ReplicationLog, P: ShardPolicy>(
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

        Command::SetNx { key, value } => set_nx(dict, log, seq, shard, now, key, value),

        Command::PSetEx { key, millis, value } => {
            let args = SetArgs::pset_ex(key, value, *millis);
            set(dict, log, seq, shard, now, args)
        }

        Command::Del { key } => del(dict, log, seq, shard, key),

        Command::IncrBy { key, delta } => incr_by(dict, log, seq, shard, key, *delta),

        Command::Expire { key, seconds } => {
            let at = span_deadline(now, *seconds, Expiry::Ex);
            set_expiry(dict, log, seq, shard, key, at)
        }

        // `EXPIREAT` and `PEXPIREAT` reach the shard as the span the edge
        // resolved out of the absolute deadline they named, which is exactly
        // `PEXPIRE`'s argument — so the three share one arm rather than three
        // copies kept identical by hand.
        Command::PExpire { key, millis }
        | Command::ExpireAt { key, millis }
        | Command::PExpireAt { key, millis } => {
            let at = span_deadline(now, *millis, Expiry::Px);
            set_expiry(dict, log, seq, shard, key, at)
        }

        Command::Ttl { key } => ttl_reply(dict, key, now, remaining_seconds),

        Command::PTtl { key } => ttl_reply(dict, key, now, remaining_millis),

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

    /// The four fields `SETNX` does not have. Named here for
    /// [`set_ex`](Self::set_ex)'s reason: the type carries six fields and
    /// only the condition is a decision this command makes.
    const fn set_nx(key: &'a Vec<u8>, value: &'a mut Vec<u8>) -> Self {
        Self {
            key,
            value,
            expiry: None,
            cond: Some(Cond::Nx),
            keep_ttl: false,
            get: false,
        }
    }

    /// [`set_ex`](Self::set_ex)'s shape with the span in milliseconds. The
    /// four fields named off here say once that `PSETEX` has no options
    /// either.
    const fn pset_ex(key: &'a Vec<u8>, value: &'a mut Vec<u8>, millis: u64) -> Self {
        Self {
            key,
            value,
            expiry: Some(Expiry::Px(millis)),
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
/// `SETNX key value`: [`set`]'s `NX` decision, reported as an integer.
///
/// Extracted for the third of the three reasons [`apply`] gives — the arm was
/// what took the dispatcher over `clippy::too_many_lines` — and it sits here
/// rather than beside its sibling handlers because the ordering rule is the
/// match's: `SetNx` dispatches after `Set` and `SetEx`, whose handler is
/// [`set`] directly above.
fn set_nx<L: ReplicationLog>(
    dict: &mut Dict,
    log: &mut L,
    seq: &mut u64,
    shard: u16,
    now: Instant,
    key: &Vec<u8>,
    value: &mut Vec<u8>,
) -> Reply {
    // `set` answers `Ok` or `Bulk(None)` for the `NX` condition; this spelling
    // reports the same decision as an integer (6.2.24, 8.10.1).
    //
    // The `other` arm is not a catch-all for convenience: `set` can answer a
    // log-write failure and the ceiling refusal, and those must travel
    // unchanged rather than become a `0` that would tell a client its key
    // already existed.
    match set(dict, log, seq, shard, now, SetArgs::set_nx(key, value)) {
        Reply::Ok => Reply::Integer(1),
        Reply::Bulk(None) => Reply::Integer(0),
        other => other,
    }
}

/// `DEL key`: remove the entry, and say whether there was one.
///
/// Extracted for [`apply`]'s third reason, on the commit its doc comment
/// predicted would come for it. The arm carries a real decision — a key that
/// is not there is not a write, so the log is never appended to for it — and
/// that decision is unchanged by the move; what changed is that the
/// dispatcher is a page again.
fn del<L: ReplicationLog>(
    dict: &mut Dict,
    log: &mut L,
    seq: &mut u64,
    shard: u16,
    key: &[u8],
) -> Reply {
    if dict.get(key).is_none() {
        return Reply::Removed(false);
    }
    if let Err(failed) = append(log, seq, shard) {
        return failed;
    }
    dict.remove(key);
    Reply::Removed(true)
}

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
pub fn scan_step<P: ShardPolicy>(
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
pub fn evict_if_expired<L: ReplicationLog, P: ExpiryPolicy>(
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

/// What `PTTL` answers for a key with a deadline: the milliseconds left, not
/// rounded — the unit the deadline is kept in, reported as it is.
fn remaining_millis(expires_at: Instant, now: Instant) -> i64 {
    i64::try_from(expires_at.saturating_duration_since(now).as_millis()).unwrap_or(i64::MAX)
}

/// What `TTL` and `PTTL` answer, which differ only in what `remaining` makes
/// of the deadline: `-2` for a key that is not there, `-1` for one carrying no
/// deadline, otherwise the span left in that function's unit.
///
/// The two sentinels are the same numbers in both units — they are not spans —
/// so they are written once here rather than in each arm.
fn ttl_reply(
    dict: &Dict,
    key: &[u8],
    now: Instant,
    remaining: fn(Instant, Instant) -> i64,
) -> Reply {
    match dict.get(key).map(|entry| entry.expires_at) {
        None => Reply::Integer(-2),
        Some(None) => Reply::Integer(-1),
        Some(Some(at)) => Reply::Integer(remaining(at, now)),
    }
}

/// Appends one record for a mutation about to happen, advancing `seq`.
///
/// The payload is empty: today the log records that a mutation occurred and
/// where it sits in the shard's order, not what it was. Returns the `Reply` to send
/// instead when the write fails — the mutation must not proceed.
pub fn append<L: ReplicationLog>(log: &mut L, seq: &mut u64, shard: u16) -> Result<(), Reply> {
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
    use super::*;

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
}
