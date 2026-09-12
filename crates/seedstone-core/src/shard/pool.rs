//! The pool: the executors, the shards each owns, and [`Router`], the one way
//! a command reaches a shard — by key, by shard number, or to every shard at
//! once. [`Envelope`] is what crosses the channel; [`ShardStats`] what comes
//! back for `INFO`.

use crate::dict::{Dict, DictSeed};
use crate::log::{NoopLog, ReplicationLog};
use crate::memory::{MemoryGauge, MemoryLimit};
use crate::shard::executor::{Memory, ShardState, run_executor};
use crate::shard::{
    Command, Deadlines, KIND_SLOTS, Reply, ReplyError, Route, ShardPolicy, TraceSink,
};
use crate::slot::{executor_of, shard_of};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

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
    /// from this: see [`crate::shard::executor::count_call`], which says why counting them here would
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
    /// [`crate::shard::ExpiryPolicy`] for why the seam exists at all.
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
