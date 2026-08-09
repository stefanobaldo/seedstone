//! The shard runtime: N keyspaces, each owned by one task behind an inbox.
//!
//! A shard is a task, a [`Dict`] nothing else can reach, and an unbounded
//! inbox. Work arrives as an [`Envelope`] — a [`Command`] plus the one-shot
//! channel its [`Reply`] goes back on — and the shard answers messages one at
//! a time, in arrival order. Nothing is shared, so nothing is locked.
//!
//! # Why a handler is a plain `fn`
//!
//! [`apply`] takes `&mut Dict` and returns a `Reply`. It is not `async`, and
//! that is the point: a handler that cannot `await` cannot yield the shard
//! mid-command, so a command either has not started or has finished, and two
//! commands on one key can never interleave. The rule is enforced by the
//! signature rather than by review — the only `await`s in a shard task are
//! the `select!` arms of [`run_shard`].
//!
//! That is also why the interesting concurrency bugs of this system live
//! *above* the shard, in code that sends two messages with an `await` between
//! them. The simulator plants exactly that race.

use crate::dict::{Dict, DictSeed};
use crate::log::{NoopLog, Record, ReplicationLog};
use crate::slot::shard_of;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

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

/// Reply text for a numeric operation on a value that is not an integer.
///
/// Byte-for-byte the message Redis returns, so existing clients that match on
/// it keep working.
///
/// Public because it is a wire-visible contract, not an implementation
/// detail: the simulator's planted router has to answer exactly what the
/// honest one answers, or a planted trace differs for a reason other than the
/// race it exists to plant. It kept a private copy of this string, and
/// nothing linked the two.
pub const NOT_AN_INTEGER: &str = "ERR value is not an integer or out of range";

/// Reply text for an `IncrBy` whose result would leave `i64`.
///
/// Public for the same reason as [`NOT_AN_INTEGER`].
pub const WOULD_OVERFLOW: &str = "ERR increment or decrement would overflow";

/// Reply text for a command whose shard task is gone.
///
/// Unreachable while a [`ShardPool`] is alive — it holds every sender, and a
/// shard task only stops when its inbox closes. It exists so the dispatch
/// path has no `unwrap`.
const SHARD_UNAVAILABLE: &str = "ERR shard is unavailable";

/// Reply text for a mutation whose log record could not be written.
const LOG_WRITE_FAILED: &str = "ERR replication log write failed";

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
}

impl Command {
    /// The key this command addresses — what [`shard_of`] routes on.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Get { key }
            | Self::Set { key, .. }
            | Self::Del { key }
            | Self::IncrBy { key, .. } => key,
        }
    }

    /// A stable one-byte tag for this command's variant.
    ///
    /// `Get` = 1, `Set` = 2, `Del` = 3, `IncrBy` = 4. These values are folded
    /// into the simulator's trace hash, so they are part of what a replay
    /// compares: changing one changes every recorded hash.
    #[must_use]
    pub const fn kind(&self) -> u8 {
        match self {
            Self::Get { .. } => 1,
            Self::Set { .. } => 2,
            Self::Del { .. } => 3,
            Self::IncrBy { .. } => 4,
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
    /// The command failed. The text is server-authored and contains no `\r`
    /// or `\n`, so it is safe in a RESP error frame.
    Error(String),
}

/// One unit of work for a shard: a command and where its reply goes.
pub struct Envelope {
    /// The command to run.
    pub cmd: Command,
    /// The channel the shard sends the reply back on.
    pub reply: oneshot::Sender<Reply>,
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
    fn dispatch(&self, cmd: Command) -> impl Future<Output = Reply> + Send;
}

/// A set of shard tasks and the inboxes that reach them.
///
/// Cloning is cheap and shares the same shards: every clone is a handle to
/// one pool, not a copy of it.
#[derive(Clone)]
pub struct ShardPool {
    inboxes: Arc<Vec<mpsc::UnboundedSender<Envelope>>>,
    /// The inbox count, kept in the width it arrived in.
    ///
    /// Redundant with `inboxes.len()`, and deliberately so: the count is a
    /// `u16` at every point that matters — [`spawn`](ShardPool::spawn) takes
    /// one, [`shard_of`] wants one — and narrowing the `Vec`'s length back
    /// down on every dispatch would be a fallible conversion standing where an
    /// invariant already holds. Two bytes buy its absence.
    shards: u16,
}

impl ShardPool {
    /// Spawns `shards` shard tasks on the current tokio runtime.
    ///
    /// Each shard hashes with a seed derived from `seed` — `k0` xored with
    /// the shard index — so one root seed fixes the whole node's placement
    /// while no two shards share a bucket layout.
    ///
    /// # Panics
    ///
    /// If `shards` is zero: there would be nowhere to route a key.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "every shard gets a clone and the original is dropped, but taking \
                  the sink by value is what lets a caller move one in rather than \
                  keep it alive alongside the pool"
    )]
    pub fn spawn<T: TraceSink>(shards: u16, seed: DictSeed, trace: T) -> Self {
        assert!(
            shards > 0,
            "ShardPool::spawn: shards must be greater than zero"
        );
        let mut inboxes = Vec::with_capacity(usize::from(shards));
        for shard in 0..shards {
            let (tx, rx) = mpsc::unbounded_channel();
            inboxes.push(tx);
            let shard_seed = DictSeed {
                k0: seed.k0 ^ u64::from(shard),
                k1: seed.k1,
            };
            tokio::spawn(run_shard(shard, shard_seed, trace.clone(), rx));
        }
        Self {
            inboxes: Arc::new(inboxes),
            shards,
        }
    }

    /// How many shards this pool spans.
    #[must_use]
    pub const fn shards(&self) -> u16 {
        self.shards
    }
}

impl Router for ShardPool {
    fn dispatch(&self, cmd: Command) -> impl Future<Output = Reply> + Send {
        let index = usize::from(shard_of(cmd.key(), self.shards()));
        let (tx, rx) = oneshot::channel();
        // Send before the future is awaited: the inbox is unbounded, so this
        // never blocks and the caller cannot deadlock by holding the future.
        let sent = self.inboxes[index]
            .send(Envelope { cmd, reply: tx })
            .is_ok();
        async move {
            if !sent {
                return Reply::Error(SHARD_UNAVAILABLE.into());
            }
            rx.await
                .unwrap_or_else(|_| Reply::Error(SHARD_UNAVAILABLE.into()))
        }
    }
}

/// One shard task: own a dict, answer the inbox, keep any rehash moving.
///
/// Returns when the inbox closes, which happens once the last [`ShardPool`]
/// handle is dropped.
async fn run_shard<T: TraceSink>(
    shard: u16,
    seed: DictSeed,
    trace: T,
    mut inbox: mpsc::UnboundedReceiver<Envelope>,
) {
    let mut dict = Dict::with_seed(seed);
    let mut log = NoopLog;
    let mut seq: u64 = 0;

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
                let Some(Envelope { cmd, reply }) = envelope else {
                    break;
                };
                let at = seq;
                let answer = apply(&mut dict, &mut log, &mut seq, shard, &cmd);
                trace.record(shard, at, &cmd, &answer);
                // The caller may have gone away; its reply is simply dropped.
                let _ = reply.send(answer);
            }
            _ = tick.tick() => {
                dict.rehash_step(REHASH_BUCKETS_PER_TICK);
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
fn apply<L: ReplicationLog>(
    dict: &mut Dict,
    log: &mut L,
    seq: &mut u64,
    shard: u16,
    cmd: &Command,
) -> Reply {
    match cmd {
        Command::Get { key } => Reply::Bulk(dict.get(key).map(<[u8]>::to_vec)),

        Command::Set { key, value } => {
            if let Err(failed) = append(log, seq, shard) {
                return failed;
            }
            dict.insert(key.clone(), value.clone());
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
            let current = match dict.get(key) {
                None => 0,
                Some(bytes) => match parse_i64(bytes) {
                    Some(n) => n,
                    None => return Reply::Error(NOT_AN_INTEGER.into()),
                },
            };
            let Some(next) = current.checked_add(*delta) else {
                return Reply::Error(WOULD_OVERFLOW.into());
            };
            if let Err(failed) = append(log, seq, shard) {
                return failed;
            }
            dict.insert(key.clone(), next.to_string().into_bytes());
            Reply::Integer(next)
        }
    }
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
        Err(_) => Err(Reply::Error(LOG_WRITE_FAILED.into())),
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

    #[tokio::test]
    async fn commands_round_trip_through_the_pool() {
        let pool = ShardPool::spawn(16, DictSeed { k0: 1, k1: 2 }, NoTrace);
        assert_eq!(
            pool.dispatch(Command::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec()
            })
            .await,
            Reply::Ok
        );
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
            Reply::Error("ERR value is not an integer or out of range".into())
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
    #[test]
    fn a_handler_runs_to_completion_without_a_runtime() {
        let mut dict = Dict::with_seed(DictSeed { k0: 7, k1: 9 });
        let mut log = NoopLog;
        let mut seq = 0;

        let set = apply(
            &mut dict,
            &mut log,
            &mut seq,
            0,
            &Command::Set {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
        );
        assert_eq!(set, Reply::Ok);
        assert_eq!(
            apply(
                &mut dict,
                &mut log,
                &mut seq,
                0,
                &Command::Get { key: b"k".to_vec() }
            ),
            Reply::Bulk(Some(b"v".to_vec()))
        );
    }

    #[test]
    fn seq_advances_only_for_commands_that_change_something() {
        let mut dict = Dict::with_seed(DictSeed { k0: 1, k1: 1 });
        let mut log = NoopLog;
        let mut seq = 0;
        let mut run = |cmd: &Command, seq: &mut u64| apply(&mut dict, &mut log, seq, 3, cmd);

        // A read moves nothing.
        run(&Command::Get { key: b"a".to_vec() }, &mut seq);
        assert_eq!(seq, 0);

        // A write does.
        run(
            &Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            &mut seq,
        );
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

        run(
            &Command::Set {
                key: b"txt".to_vec(),
                value: b"abc".to_vec(),
            },
            &mut seq,
        );
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
        let pool = ShardPool::spawn(4, DictSeed { k0: 3, k1: 4 }, NoTrace);
        assert_eq!(
            pool.dispatch(Command::Set {
                key: b"c".to_vec(),
                value: i64::MAX.to_string().into_bytes(),
            })
            .await,
            Reply::Ok
        );
        assert_eq!(
            pool.dispatch(Command::IncrBy {
                key: b"c".to_vec(),
                delta: 1
            })
            .await,
            Reply::Error(WOULD_OVERFLOW.into())
        );
        // The value is untouched.
        assert_eq!(
            pool.dispatch(Command::Get { key: b"c".to_vec() }).await,
            Reply::Bulk(Some(i64::MAX.to_string().into_bytes()))
        );
    }

    #[tokio::test]
    async fn a_missing_counter_starts_at_zero_and_set_overwrites() {
        let pool = ShardPool::spawn(8, DictSeed { k0: 5, k1: 6 }, NoTrace);
        assert_eq!(
            pool.dispatch(Command::IncrBy {
                key: b"fresh".to_vec(),
                delta: -3
            })
            .await,
            Reply::Integer(-3)
        );
        assert_eq!(
            pool.dispatch(Command::Set {
                key: b"fresh".to_vec(),
                value: b"100".to_vec()
            })
            .await,
            Reply::Ok
        );
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
        let pool = ShardPool::spawn(16, DictSeed { k0: 11, k1: 13 }, NoTrace);
        let keys: Vec<Vec<u8>> = (0..600u32)
            .map(|i| format!("key:{i}").into_bytes())
            .collect();

        for (i, key) in keys.iter().enumerate() {
            assert_eq!(
                pool.dispatch(Command::Set {
                    key: key.clone(),
                    value: i.to_string().into_bytes(),
                })
                .await,
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
        let pool = ShardPool::spawn(1, DictSeed { k0: 2, k1: 3 }, sink.clone());

        pool.dispatch(Command::Get { key: b"k".to_vec() }).await;
        pool.dispatch(Command::Set {
            key: b"k".to_vec(),
            value: b"1".to_vec(),
        })
        .await;
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
    #[should_panic(expected = "shards must be greater than zero")]
    async fn a_pool_of_no_shards_is_a_programming_error() {
        ShardPool::spawn(0, DictSeed { k0: 0, k1: 0 }, NoTrace);
    }
}
