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

use crate::dict::{Dict, DictSeed, Entry};
use crate::log::{NoopLog, Record, ReplicationLog};
use crate::slot::{executor_of, shard_of};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

/// How often an idle shard advances an in-flight rehash.
const REHASH_TICK: Duration = Duration::from_millis(100);

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
    LogWriteFailed,
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
    /// Report how long `key` has left.
    Ttl {
        /// The key to ask about.
        key: Vec<u8>,
    },
    /// Report whether `key` exists.
    Exists {
        /// The key to ask about.
        key: Vec<u8>,
    },
}

impl Command {
    /// The key this command addresses — what [`shard_of`] routes on.
    ///
    /// Exactly one key, for every command there is and every command there
    /// will be: a request naming several keys is split into one command per
    /// key before it reaches a shard, because the shards that own them are not
    /// in general the same shard.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Get { key }
            | Self::Set { key, .. }
            | Self::Del { key }
            | Self::IncrBy { key, .. }
            | Self::Expire { key, .. }
            | Self::Ttl { key }
            | Self::Exists { key } => key,
        }
    }

    /// A stable one-byte tag for this command's variant.
    ///
    /// `Get` = 1, `Set` = 2, `Del` = 3, `IncrBy` = 4, `Expire` = 5, `Ttl` = 6,
    /// `Exists` = 7. These values are folded into the simulator's trace hash,
    /// so they are part of what a replay compares: changing one changes every
    /// recorded hash.
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
        }
    }
}

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
    /// Whether a `Del` removed anything.
    Removed(bool),
    /// An integer result.
    Integer(i64),
    /// The command failed. See [`ReplyError`] — a closed set of
    /// server-authored failures, none of whose texts can split a frame.
    Error(ReplyError),
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
    /// `seq` is the shard's replication position *at which the command ran* —
    /// for a mutation, exactly the `seq` of the record it appended.
    fn record(&self, shard: u16, seq: u64, cmd: &Command, reply: &Reply);
}

/// A [`TraceSink`] that observes nothing. Production's sink.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoTrace;

impl TraceSink for NoTrace {
    fn record(&self, _shard: u16, _seq: u64, _cmd: &Command, _reply: &Reply) {}
}

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
        Self::spawn_with_log(shards, executors, seed, trace, |_shard| NoopLog)
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
    #[allow(
        clippy::needless_pass_by_value,
        reason = "every executor gets a clone of the sink and the original is dropped, \
                  but taking both it and the log factory by value is what lets a \
                  caller move them in rather than keep them alive alongside the pool"
    )]
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
        let mut inboxes = Vec::with_capacity(usize::from(executors));
        let mut pending: Option<(u16, Vec<ShardState<L>>)> = None;
        for shard in 0..shards {
            let state = ShardState {
                dict: Dict::with_seed(DictSeed {
                    k0: seed.k0 ^ u64::from(shard),
                    k1: seed.k1,
                }),
                seq: 0,
                log: make_log(shard),
            };
            match &mut pending {
                Some((first_shard, states))
                    if executor_of(shard, shards, executors)
                        == executor_of(*first_shard, shards, executors) =>
                {
                    states.push(state);
                }
                _ => {
                    if let Some((first_shard, states)) = pending.take() {
                        inboxes.push(spawn_executor(first_shard, states, trace.clone()));
                    }
                    pending = Some((shard, vec![state]));
                }
            }
        }
        if let Some((first_shard, states)) = pending {
            inboxes.push(spawn_executor(first_shard, states, trace));
        }

        Self {
            inboxes: Arc::new(inboxes),
            shards,
            executors,
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
}

/// Spawns one executor task and returns the inbox that reaches it.
fn spawn_executor<T: TraceSink, L: ReplicationLog>(
    first_shard: u16,
    states: Vec<ShardState<L>>,
    trace: T,
) -> mpsc::UnboundedSender<Envelope> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(run_executor(first_shard, states, trace, rx));
    tx
}

impl Router for ShardPool {
    fn dispatch(&self, cmd: Command) -> impl Future<Output = Reply> + Send {
        let shard = shard_of(cmd.key(), self.shards);
        let executor = usize::from(executor_of(shard, self.shards, self.executors));
        let (tx, rx) = oneshot::channel();
        // Send before the future is awaited: the inbox is unbounded, so this
        // never blocks and the caller cannot deadlock by holding the future.
        let sent = self.inboxes[executor]
            .send(Envelope {
                cmds: vec![(shard, cmd)],
                reply: tx,
            })
            .is_ok();
        async move {
            if !sent {
                return Reply::Error(ReplyError::ShardUnavailable);
            }
            match rx.await {
                // A one-command batch is answered with one reply; anything
                // else means the executor did not answer this envelope.
                Ok(mut replies) if replies.len() == 1 => replies.pop().expect("checked non-empty"),
                _ => Reply::Error(ReplyError::ShardUnavailable),
            }
        }
    }

    fn dispatch_many(&self, cmds: Vec<Command>) -> impl Future<Output = Vec<Reply>> + Send {
        let executors = usize::from(self.executors);
        // Index-addressed buckets: iteration order is the executor order by
        // construction, which is what keeps this path free of any map
        // iteration — and so free of an iteration order that could differ
        // between two runs of the same seed.
        let mut buckets: Vec<Vec<(u16, Command)>> = Vec::new();
        buckets.resize_with(executors, Vec::new);
        // Where each command's reply will be found once the executors answer.
        let mut positions = Vec::with_capacity(cmds.len());
        for cmd in cmds {
            let shard = shard_of(cmd.key(), self.shards);
            let executor = usize::from(executor_of(shard, self.shards, self.executors));
            positions.push((executor, buckets[executor].len()));
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
                .map(|(executor, offset)| {
                    answered[executor]
                        .get_mut(offset)
                        .and_then(Option::take)
                        .unwrap_or(Reply::Error(ReplyError::ShardUnavailable))
                })
                .collect()
        }
    }
}

/// One virtual shard's state: what used to be one task's locals.
struct ShardState<L> {
    dict: Dict,
    seq: u64,
    log: L,
}

/// One executor task: own a contiguous range of shards, answer the inbox,
/// keep every owned rehash moving.
///
/// `states` holds the range's shards in ascending order starting at
/// `first_shard`, so a command's shard id indexes it by subtraction.
///
/// Returns when the inbox closes, which happens once the last [`ShardPool`]
/// handle is dropped.
async fn run_executor<T: TraceSink, L: ReplicationLog>(
    first_shard: u16,
    mut states: Vec<ShardState<L>>,
    trace: T,
    mut inbox: mpsc::UnboundedReceiver<Envelope>,
) {
    let mut tick = tokio::time::interval(REHASH_TICK);
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
            // It is unobservable today — the losing arm only advances a
            // rehash, which reaches no reply and no trace field — and stops
            // being unobservable the moment anything rehash-sensitive becomes
            // visible, which a `SCAN` command would do. Draining the inbox
            // first is also the right priority on its own merits: work the
            // shard was asked for outranks housekeeping.
            biased;

            envelope = inbox.recv() => {
                let Some(Envelope { cmds, reply }) = envelope else {
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
                // about the batch justifies. Reading it once also keeps the
                // cost at one per envelope rather than one per command.
                let now = Instant::now();
                for (shard, cmd) in &cmds {
                    let state = &mut states[usize::from(shard - first_shard)];
                    let at = state.seq;
                    let answer =
                        apply(&mut state.dict, &mut state.log, &mut state.seq, *shard, cmd, now);
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
                for state in &mut states {
                    state.dict.rehash_step(REHASH_BUCKETS_PER_TICK);
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
                    // to; a log that cannot sync is a Phase 3 problem with a
                    // Phase 3 answer.
                    let _ = state.log.sync();
                }
            }
        }
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
/// The key and value are cloned into the dict because [`TraceSink`] observes
/// the command after the fact and so must outlive this call.
///
/// `now` is the instant the whole envelope is being served at, supplied by the
/// executor: a handler must not read a clock of its own, or two commands of
/// one batch could disagree about which keys are still alive.
fn apply<L: ReplicationLog>(
    dict: &mut Dict,
    log: &mut L,
    seq: &mut u64,
    shard: u16,
    cmd: &Command,
    now: Instant,
) -> Reply {
    // Lazy expiry, once, before any arm has looked at the key. Here rather
    // than in each arm on purpose: it makes "an expired key is dead to every
    // command" a property of the dispatch instead of a rule seven handlers
    // have to remember, and a command added later inherits it without knowing
    // it exists. Every command addresses exactly one key, which is what lets
    // one check stand in front of all of them.
    if let Err(failed) = evict_if_expired(dict, log, seq, shard, cmd.key(), now) {
        return failed;
    }

    match cmd {
        Command::Get { key } => Reply::Bulk(dict.get(key).map(|entry| entry.value.clone())),

        Command::Set {
            key,
            value,
            expiry,
            cond,
        } => {
            // A condition the keyspace does not meet is answered, not failed:
            // the peer asked for a write that was allowed not to happen.
            match cond {
                Some(Cond::Nx) if dict.get(key).is_some() => return Reply::Bulk(None),
                Some(Cond::Xx) if dict.get(key).is_none() => return Reply::Bulk(None),
                _ => {}
            }
            if let Err(failed) = append(log, seq, shard) {
                return failed;
            }
            dict.insert(
                key.clone(),
                Entry {
                    value: value.clone(),
                    expires_at: deadline(now, *expiry),
                },
            );
            Reply::Ok
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

        Command::IncrBy { key, delta } => {
            let (current, expires_at) = match dict.get(key) {
                None => (0, None),
                Some(entry) => match parse_i64(&entry.value) {
                    Some(n) => (n, entry.expires_at),
                    None => return Reply::Error(ReplyError::NotAnInteger),
                },
            };
            let Some(next) = current.checked_add(*delta) else {
                return Reply::Error(ReplyError::WouldOverflow);
            };
            if let Err(failed) = append(log, seq, shard) {
                return failed;
            }
            // The deadline rides along, as it does in Redis: an increment
            // changes what a counter holds, not how long it lives.
            dict.insert(
                key.clone(),
                Entry {
                    value: next.to_string().into_bytes(),
                    expires_at,
                },
            );
            Reply::Integer(next)
        }

        Command::Expire { key, seconds } => {
            if dict.get(key).is_none() {
                return Reply::Integer(0);
            }
            if let Err(failed) = append(log, seq, shard) {
                return failed;
            }
            if *seconds <= 0 {
                // A deadline that is not in the future is a deletion, and
                // Redis reports it as an applied expiry rather than as a
                // delete — the client asked for the key to be gone by a time
                // that has passed, and it is.
                dict.remove(key);
            } else if let Some(entry) = dict.get_mut(key) {
                // Present a moment ago, and a handler cannot be suspended, so
                // nothing can have removed it in between.
                let span = u64::try_from(*seconds).expect("a positive i64 is a u64");
                entry.expires_at = deadline(now, Some(Expiry::Ex(span)));
            }
            Reply::Integer(1)
        }

        Command::Ttl { key } => match dict.get(key).map(|entry| entry.expires_at) {
            None => Reply::Integer(-2),
            Some(None) => Reply::Integer(-1),
            Some(Some(at)) => Reply::Integer(remaining_seconds(at, now)),
        },

        Command::Exists { key } => Reply::Integer(i64::from(dict.get(key).is_some())),
    }
}

/// Removes `key` if its deadline has passed, appending the deletion to the log
/// exactly as an explicit `Del` would.
///
/// An expiration is a change to the keyspace, so it takes a replication
/// position like every other one: a later phase replaying the log has to see
/// the key disappear where it disappeared here, and the [`TraceSink`] — which
/// folds the position each command ran at — sees the position the removal
/// consumed. Neither has to know what a deadline is.
///
/// The deadline is the first instant the entry is gone, so a key is expired
/// when `now` has reached it and not only once it is past.
///
/// Returns the reply to send instead when the record could not be written. A
/// removal that cannot be logged must not happen, for the same reason a `Del`
/// that cannot be logged does not: the alternative is a keyspace that has
/// moved past a log which does not describe it. The entry stays, and the
/// command that met it is refused rather than answered from a value that
/// should be gone.
fn evict_if_expired<L: ReplicationLog>(
    dict: &mut Dict,
    log: &mut L,
    seq: &mut u64,
    shard: u16,
    key: &[u8],
    now: Instant,
) -> Result<(), Reply> {
    let Some(expires_at) = dict.get(key).and_then(|entry| entry.expires_at) else {
        return Ok(());
    };
    if expires_at > now {
        return Ok(());
    }
    append(log, seq, shard)?;
    dict.remove(key);
    Ok(())
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

/// How many seconds are left before `deadline`, rounded up.
///
/// Up rather than to nearest so that a key which is still alive never reads as
/// `0`, which is the value a client tests for. Saturating rather than
/// truncating on the way to `i64`: a remaining span that does not fit is
/// further off than any client will wait, and reporting the largest number
/// there is says that better than a wrapped one.
fn remaining_seconds(expires_at: Instant, now: Instant) -> i64 {
    let left = expires_at.saturating_duration_since(now);
    let seconds = left
        .as_secs()
        .saturating_add(u64::from(left.subsec_nanos() > 0));
    i64::try_from(seconds).unwrap_or(i64::MAX)
}

/// Appends one record for a mutation about to happen, advancing `seq`.
///
/// The payload is empty: Phase 1 records that a mutation occurred and where
/// it sits in the shard's order, not what it was. Returns the `Reply` to send
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
    use super::*;
    use std::sync::Mutex;
    use tokio::time::Instant;

    /// One shard's state, driven through [`apply`] directly.
    ///
    /// The expiry tests need two things a pool deliberately does not offer:
    /// the `now` each command sees, which in production is the executor's to
    /// choose, and the dict a handler left behind — an expired entry has to be
    /// shown *gone*, not merely invisible to a read.
    struct Shard {
        dict: Dict,
        log: NoopLog,
        seq: u64,
    }

    impl Shard {
        fn new() -> Self {
            Self {
                dict: Dict::with_seed(DictSeed { k0: 5, k1: 7 }),
                log: NoopLog,
                seq: 0,
            }
        }

        fn run(&mut self, cmd: &Command, now: Instant) -> Reply {
            apply(&mut self.dict, &mut self.log, &mut self.seq, 0, cmd, now)
        }
    }

    /// `SET key value`, with no options.
    fn set(key: &[u8], value: &[u8]) -> Command {
        Command::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            expiry: None,
            cond: None,
        }
    }

    /// `SET key value EX seconds`.
    fn set_ex(key: &[u8], value: &[u8], seconds: u64) -> Command {
        Command::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            expiry: Some(Expiry::Ex(seconds)),
            cond: None,
        }
    }

    fn get(key: &[u8]) -> Command {
        Command::Get { key: key.to_vec() }
    }

    #[tokio::test(start_paused = true)]
    async fn set_with_ex_expires_lazily() {
        let mut shard = Shard::new();
        assert_eq!(
            shard.run(&set_ex(b"k", b"v", 30), Instant::now()),
            Reply::Ok
        );

        tokio::time::advance(Duration::from_secs(29)).await;
        assert_eq!(
            shard.run(&get(b"k"), Instant::now()),
            Reply::Bulk(Some(b"v".to_vec())),
            "a key one second short of its deadline is still a key"
        );

        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(shard.run(&get(b"k"), Instant::now()), Reply::Bulk(None));
        // The read is what removed it. A deadline that only hid the entry would
        // leave the keyspace growing with values nothing can ever reach again.
        assert_eq!(
            shard.dict.len(),
            0,
            "the expired entry survived the read that reported it gone"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn set_nx_xx_algebra() {
        let mut shard = Shard::new();
        let conditional = |value: &[u8], cond: Cond| Command::Set {
            key: b"k".to_vec(),
            value: value.to_vec(),
            expiry: None,
            cond: Some(cond),
        };

        // XX on an absent key stores nothing and says so.
        assert_eq!(
            shard.run(&conditional(b"first", Cond::Xx), Instant::now()),
            Reply::Bulk(None)
        );
        assert_eq!(shard.run(&get(b"k"), Instant::now()), Reply::Bulk(None));

        // NX on an absent key stores.
        assert_eq!(
            shard.run(&conditional(b"first", Cond::Nx), Instant::now()),
            Reply::Ok
        );
        assert_eq!(
            shard.run(&get(b"k"), Instant::now()),
            Reply::Bulk(Some(b"first".to_vec()))
        );

        // NX on a present key refuses, and leaves the value it found alone.
        assert_eq!(
            shard.run(&conditional(b"second", Cond::Nx), Instant::now()),
            Reply::Bulk(None)
        );
        assert_eq!(
            shard.run(&get(b"k"), Instant::now()),
            Reply::Bulk(Some(b"first".to_vec())),
            "a refused NX overwrote the value anyway"
        );

        // XX on a present key replaces it.
        assert_eq!(
            shard.run(&conditional(b"second", Cond::Xx), Instant::now()),
            Reply::Ok
        );
        assert_eq!(
            shard.run(&get(b"k"), Instant::now()),
            Reply::Bulk(Some(b"second".to_vec()))
        );

        // And a plain SET clears the deadline the key it overwrote carried —
        // Redis's semantics, and the reason a rewritten key is not silently
        // still on its predecessor's clock.
        assert_eq!(
            shard.run(&set_ex(b"t", b"v", 30), Instant::now()),
            Reply::Ok
        );
        tokio::time::advance(Duration::from_secs(29)).await;
        assert_eq!(shard.run(&set(b"t", b"w"), Instant::now()), Reply::Ok);
        tokio::time::advance(Duration::from_hours(1)).await;
        assert_eq!(
            shard.run(&get(b"t"), Instant::now()),
            Reply::Bulk(Some(b"w".to_vec())),
            "a plain SET left the old deadline in place"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expire_and_ttl() {
        let mut shard = Shard::new();
        let ttl = |key: &[u8]| Command::Ttl { key: key.to_vec() };
        let expire = |key: &[u8], seconds: i64| Command::Expire {
            key: key.to_vec(),
            seconds,
        };

        assert_eq!(
            shard.run(&ttl(b"k"), Instant::now()),
            Reply::Integer(-2),
            "TTL of a key that does not exist"
        );
        assert_eq!(
            shard.run(&expire(b"k", 10), Instant::now()),
            Reply::Integer(0),
            "EXPIRE of a key that does not exist"
        );

        assert_eq!(shard.run(&set(b"k", b"v"), Instant::now()), Reply::Ok);
        assert_eq!(
            shard.run(&ttl(b"k"), Instant::now()),
            Reply::Integer(-1),
            "TTL of a key with no deadline"
        );

        assert_eq!(
            shard.run(&expire(b"k", 100), Instant::now()),
            Reply::Integer(1)
        );
        assert_eq!(shard.run(&ttl(b"k"), Instant::now()), Reply::Integer(100));
        // Rounded up, so a key with any time left never reads as zero — which
        // is a value a client tests for.
        tokio::time::advance(Duration::from_millis(500)).await;
        assert_eq!(shard.run(&ttl(b"k"), Instant::now()), Reply::Integer(100));

        // A non-positive expiry deletes the key, and still reports that the
        // deadline was applied rather than that nothing was there.
        assert_eq!(
            shard.run(&expire(b"k", 0), Instant::now()),
            Reply::Integer(1)
        );
        assert_eq!(shard.run(&get(b"k"), Instant::now()), Reply::Bulk(None));
        assert_eq!(shard.dict.len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_keys_are_dead_to_every_command() {
        let mut shard = Shard::new();
        // One key per command, all past the same deadline: every arm meets an
        // entry that is still in the dict and already gone, which is the state
        // a handler that skipped the liveness check would answer from.
        for key in [&b"get"[..], b"exists", b"ttl", b"del", b"incrby"] {
            assert_eq!(shard.run(&set_ex(key, b"1", 10), Instant::now()), Reply::Ok);
        }
        tokio::time::advance(Duration::from_secs(11)).await;
        let now = Instant::now();

        assert_eq!(shard.run(&get(b"get"), now), Reply::Bulk(None));
        assert_eq!(
            shard.run(
                &Command::Exists {
                    key: b"exists".to_vec()
                },
                now
            ),
            Reply::Integer(0)
        );
        assert_eq!(
            shard.run(
                &Command::Ttl {
                    key: b"ttl".to_vec()
                },
                now
            ),
            Reply::Integer(-2)
        );
        assert_eq!(
            shard.run(
                &Command::Del {
                    key: b"del".to_vec()
                },
                now
            ),
            Reply::Removed(false)
        );
        assert_eq!(
            shard.run(
                &Command::IncrBy {
                    key: b"incrby".to_vec(),
                    delta: 7
                },
                now
            ),
            Reply::Integer(7),
            "an expired counter must start from zero"
        );

        // Each was removed by the command that met it, not merely hidden from
        // it: what is left is the counter INCRBY re-created.
        assert_eq!(shard.dict.len(), 1);
    }

    /// An expiry is a deletion, and a deletion is a logged mutation.
    ///
    /// The record is what a later phase replays; the replication position it
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
        let mut dict = Dict::with_seed(DictSeed { k0: 5, k1: 7 });
        let mut seq = 0;
        let mut shard_log = log.clone();

        let set = set_ex(b"k", b"v", 30);
        assert_eq!(
            apply(&mut dict, &mut shard_log, &mut seq, 3, &set, Instant::now()),
            Reply::Ok
        );
        tokio::time::advance(Duration::from_secs(31)).await;

        // A read appends nothing of its own, so the second record below is the
        // expiry's and nothing else.
        let read = get(b"k");
        assert_eq!(
            apply(
                &mut dict,
                &mut shard_log,
                &mut seq,
                3,
                &read,
                Instant::now()
            ),
            Reply::Bulk(None)
        );
        assert_eq!(*log.0.lock().expect("log mutex"), vec![(3, 0), (3, 1)]);
        assert_eq!(seq, 2, "the expiry did not consume a replication position");
    }

    /// The sink's side of the same fact: a position disappears from the trace
    /// where an expiry took one, so a run in which a key expired cannot hash
    /// like a run in which it did not.
    #[tokio::test(start_paused = true)]
    async fn the_sink_sees_the_position_an_expiry_consumed() {
        let sink = Recorder::default();
        let pool = ShardPool::spawn(1, 1, DictSeed { k0: 2, k1: 3 }, sink.clone());

        pool.dispatch(set_ex(b"k", b"v", 1)).await;
        tokio::time::advance(Duration::from_secs(2)).await;
        pool.dispatch(get(b"k")).await;
        pool.dispatch(set(b"k", b"again")).await;

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
        let mut dict = Dict::with_seed(DictSeed { k0: 7, k1: 9 });
        let mut log = NoopLog;
        let mut seq = 0;
        let now = Instant::now();

        let stored = apply(&mut dict, &mut log, &mut seq, 0, &set(b"k", b"v"), now);
        assert_eq!(stored, Reply::Ok);
        assert_eq!(
            apply(&mut dict, &mut log, &mut seq, 0, &get(b"k"), now),
            Reply::Bulk(Some(b"v".to_vec()))
        );
    }

    #[test]
    fn seq_advances_only_for_commands_that_change_something() {
        let mut dict = Dict::with_seed(DictSeed { k0: 1, k1: 1 });
        let mut log = NoopLog;
        let mut seq = 0;
        let now = Instant::now();
        let mut run = |cmd: &Command, seq: &mut u64| apply(&mut dict, &mut log, seq, 3, cmd, now);

        // A read moves nothing.
        run(&Command::Get { key: b"a".to_vec() }, &mut seq);
        assert_eq!(seq, 0);

        // A write does.
        run(&set(b"a", b"1"), &mut seq);
        assert_eq!(seq, 1);

        // A delete that removes nothing writes no record: replaying it would
        // be a no-op, so the log should not carry it.
        run(
            &Command::Del {
                key: b"absent".to_vec(),
            },
            &mut seq,
        );
        assert_eq!(seq, 1);

        // A rejected IncrBy likewise.
        run(
            &Command::IncrBy {
                key: b"a".to_vec(),
                delta: 1,
            },
            &mut seq,
        );
        assert_eq!(seq, 2, "'1' is a valid integer, so this one does count");

        run(&set(b"txt", b"abc"), &mut seq);
        assert_eq!(seq, 3);
        run(
            &Command::IncrBy {
                key: b"txt".to_vec(),
                delta: 1,
            },
            &mut seq,
        );
        assert_eq!(
            seq, 3,
            "a rejected IncrBy must not consume a sequence number"
        );

        // And a delete that does remove something.
        run(&Command::Del { key: b"a".to_vec() }, &mut seq);
        assert_eq!(seq, 4);
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
                Reply::Bulk(Some(cmd.key().to_vec()))
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

    /// One observed call: the shard, the replication position it ran at, the
    /// command's kind tag, and the reply.
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
}
