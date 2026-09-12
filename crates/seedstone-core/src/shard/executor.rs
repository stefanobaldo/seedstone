//! One executor's state for the shards it owns, and the housekeeping it runs
//! between batches: the sweep, the eviction, the counters `INFO` reads. The
//! tick and the per-tick budgets are declared here with the measurements that
//! set them.

use crate::dict::Dict;
use crate::log::ReplicationLog;
use crate::memory::{EvictionMode, MemoryGauge, MemoryLimit};
use crate::shard::apply::{append, apply};
use crate::shard::{
    Command, Envelope, EvictionPolicy, ExpiryPolicy, KIND_SLOTS, Reply, ReplyError, Route,
    ShardPolicy, ShardStats, TraceSink,
};
use std::time::Duration;
use tokio::sync::mpsc;
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

/// What an executor needs to know about the node's memory: the figure, and
/// what to do when it is too large.
///
/// One argument rather than two because [`run_executor`] and
/// [`spawn_executor`] were already at the parameter count where the next one
/// is a list nobody reads. They also always travel together: a gauge with no
/// ceiling to compare against decides nothing, and a ceiling with no gauge
/// has nothing to compare.
#[derive(Clone, Debug, Default)]
pub struct Memory {
    /// The node-wide figure every executor keeps current.
    pub gauge: MemoryGauge,
    /// The ceiling that figure is held under, and how.
    pub limit: MemoryLimit,
}

/// One virtual shard's state: what used to be one task's locals.
pub struct ShardState<L> {
    pub dict: Dict,
    pub seq: u64,
    pub log: L,
    /// Where the active expiry sweep resumes on the next housekeeping tick.
    ///
    /// A [`Dict::scan`] cursor, and it belongs to the shard rather than to the
    /// dict for the same reason a `SCAN` command's does: the dict offers a
    /// position, and what keeps a position between calls is whoever is walking.
    pub expire_cursor: u64,
    /// Where the eviction sample resumes, kept for the reason the sweep keeps
    /// its own: successive samples that all started at `0` would re-read one
    /// corner of the table and offer the same few keys up every time.
    ///
    /// Separate from [`expire_cursor`](Self::expire_cursor) on purpose. The
    /// two walks have nothing to do with each other, and sharing one position
    /// would make how often a key is offered for eviction depend on how many
    /// deadlines the keyspace happens to carry.
    pub evict_cursor: u64,
    /// How many entries this shard has evicted, for `INFO`'s `evicted_keys`.
    pub evicted: u64,
    /// Keyed lookups that found a live entry, for `INFO`'s `keyspace_hits`.
    pub hits: u64,
    /// Keyed lookups that found nothing, for `INFO`'s `keyspace_misses`.
    pub misses: u64,
    /// Entries reclaimed because their deadline had passed, by either half of
    /// expiration, for `INFO`'s `expired_keys`.
    pub expired: u64,
    /// How many commands of each [`Command::kind`] this shard has run, indexed
    /// by the tag itself; slot `0` is no command and stays zero.
    ///
    /// Plain `u64`s rather than atomics because one executor task owns this
    /// shard and nothing else may touch it — the counters a scrape reads are
    /// gathered by a [`Command::Stats`] like any other command, which is what
    /// keeps `INFO` off the hot path entirely.
    pub calls: [u64; KIND_SLOTS],
    /// Microseconds those commands spent, indexed the same way — see
    /// [`ShardStats::usec`], which is what this becomes.
    pub usec: [u64; KIND_SLOTS],
}

impl<L> ShardState<L> {
    /// A shard's state at the moment it starts: an empty keyspace, a log at
    /// position zero, and every counter unspent.
    pub const fn new(dict: Dict, log: L) -> Self {
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
pub async fn run_executor<T: TraceSink, L: ReplicationLog, P: ShardPolicy>(
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
pub fn sweep_expired<T: TraceSink, L: ReplicationLog, P: ExpiryPolicy>(
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
pub fn evict_until_fits<T: TraceSink, L: ReplicationLog, P: EvictionPolicy>(
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
/// Above [`crate::shard::apply::apply`] with the executor's helpers rather than below it with the
/// dispatcher's, because it has two callers and the executor is the one that
/// matters: [`stats_of`] fills in the counters this cannot see, and `apply`'s
/// own `Stats` arm — which nothing routes to — answers with this alone.
///
/// # Panics
///
/// If a `usize` does not fit a `u64`, which no target this builds for has.
pub fn keyspace_stats(dict: &Dict) -> ShardStats {
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
pub fn count_call<L>(state: &mut ShardState<L>, cmd: &Command, reply: &Reply, spent: u64) {
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
        // span above it are answers about a key that is. `PTTL` draws the same
        // line at the same number, in the other unit.
        (Command::Ttl { .. } | Command::PTtl { .. }, Reply::Integer(remaining)) => {
            Some(*remaining != -2)
        }
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
