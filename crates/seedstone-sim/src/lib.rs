//! SeedStone simulation harness: deterministic testing under turmoil.
//!
//! One simulated host runs the real server — the service layer over a real
//! [`ShardPool`] — and N simulated client hosts reach it over simulated TCP,
//! speaking RESP2 through the real codec. Nothing here is a model of the
//! system: the only code the simulator substitutes is the network and the
//! clock.
//!
//! # Two seeds, never one
//!
//! [`SimConfig::workload_seed`] drives what the clients ask for;
//! [`SimConfig::sim_seed`] drives turmoil's scheduler and network. Fed from
//! one knob, a differing trace hash could not distinguish a reordered
//! schedule from a changed workload, and the two effects would stay
//! confounded forever.
//!
//! # Why concurrency comes from client *hosts*
//!
//! Each client issues its operations strictly sequentially; what interleaves
//! is which client's message the server sees next, and that is a property of
//! the simulated network. Collapse the clients into one host and turmoil's
//! seed reaches nothing — the trace becomes a pure function of the workload
//! seed, and a sweep over `sim_seed` reads as a clean PASS while measuring
//! nothing at all.
//!
//! # The invariant
//!
//! Counter keys are touched only by `INCRBY`, which is order-independent, so
//! their sum has a well-defined expected value no matter how the schedule
//! falls out. Every client adds the delta of each *acknowledged* `INCRBY` to
//! a shared expected total; a final verifier client reads every counter back
//! and sums what is actually there. The two differ **iff** an update was
//! lost. The remaining keys take `GET`/`SET`/`DEL` for coverage and carry no
//! sum invariant — `SET` overwrites, so it has no order-independent total.
//!
//! # The planted race
//!
//! [`SimConfig::planted`] swaps the honest router for [`PlantedRouter`], which
//! serves `INCRBY` as a read-modify-write pair instead of one atomic message.
//! That is the harness testing itself: a simulation that has never failed has
//! not been shown capable of failing, and `planted_race.rs` requires the sweep
//! to catch this one and a separate process to replay it byte for byte.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use seedstone_core::dict::DictSeed;
use seedstone_core::service::serve_connection;
// The two error texts are imported, not copied. The planted router has to be
// indistinguishable from the honest one except in its atomicity, and these
// strings enter the trace hash — a private copy that drifted would make a
// planted trace differ for a reason unrelated to the race.
use seedstone_core::shard::{Command, Reply, ReplyError, Router, ShardPool, TraceSink, parse_i64};
use seedstone_resp::{Frame, encode, parse};
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The port the simulated server listens on. Redis's, for familiarity; in a
/// simulation nothing else is competing for it.
const PORT: u16 = 6379;

/// The simulated host name every client connects to.
const SERVER: &str = "server";

/// How much simulated time a run may take before turmoil calls it stuck.
///
/// This is a deadlock detector, not a budget: the simulation stops as soon as
/// every client has finished, so a generous ceiling costs nothing. Overrunning
/// it means a client is blocked forever, which is a finding rather than a
/// tuning problem.
const SIM_DURATION: Duration = Duration::from_mins(10);

/// How often the verifier client re-checks whether the workload has finished.
const VERIFIER_POLL: Duration = Duration::from_millis(10);

/// Read buffer size for a client connection. One reply at a time, so this is
/// generous.
const CLIENT_CHUNK: usize = 4096;

/// The odd 64-bit constant from Fibonacci hashing, used both to decorrelate
/// per-client workload seeds and as the trace hash's multiplier.
const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

/// The trace hash's multiplier — the FxHash constant.
const TRACE_MULTIPLIER: u64 = 0x517c_c1b7_2722_0a95;

/// The trace hash's starting value.
///
/// Non-zero so that an empty trace is distinguishable from a trace that
/// happened to fold back to zero, and so that a `mix` of zeros still moves.
const TRACE_INIT: u64 = 0xcbf2_9ce4_8422_2325;

/// How a simulation run is shaped.
///
/// Every field is part of what a trace hash means: two runs are comparable
/// only if their configurations are identical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimConfig {
    /// How many virtual shards the server runs.
    pub shards: u16,
    /// How many executor tasks host those shards.
    ///
    /// Explicit, never read from the machine: production asks
    /// `available_parallelism`, and doing that inside a simulation would make
    /// the trace a function of the host — the definition of a determinism
    /// violation. It is a dimension the sweep varies, not a detail the
    /// environment supplies.
    pub executors: u16,
    /// How many client hosts issue the workload.
    ///
    /// This is the lever that costs wall clock — turmoil polls every host on
    /// every tick — and also the lever that buys schedule sensitivity.
    pub clients: u16,
    /// How many keys take `GET`/`SET`/`DEL`.
    ///
    /// Kept above `shards` on purpose: with fewer keys than shards the shard
    /// dimension is degenerate and the simulation stops exercising placement.
    pub string_keys: u32,
    /// How many keys take `INCRBY` and carry the sum invariant.
    ///
    /// Fewer counters means more contention on each, which is what surfaces a
    /// lost update.
    pub counter_keys: u32,
    /// How many operations each client issues before it disconnects.
    pub ops_per_client: u32,
    /// Seeds the per-client operation generators.
    pub workload_seed: u64,
    /// Seeds turmoil: the network's latencies and the order hosts run in.
    pub sim_seed: u64,
    /// Whether to route `INCRBY` through the deliberately racy router.
    pub planted: bool,
}

impl SimConfig {
    /// The sweep configuration: the shape the DST spike proved
    /// schedule-sensitive.
    #[must_use]
    pub const fn standard(workload_seed: u64, sim_seed: u64) -> Self {
        Self {
            shards: 1024,
            executors: 10,
            clients: 128,
            string_keys: 4096,
            counter_keys: 64,
            ops_per_client: 25,
            workload_seed,
            sim_seed,
            planted: false,
        }
    }

    /// A smaller shape for tests: few enough client hosts to run in a unit
    /// test, enough operations per client to keep the counters contended.
    #[must_use]
    pub const fn mini(workload_seed: u64, sim_seed: u64) -> Self {
        Self {
            shards: 1024,
            executors: 4,
            clients: 16,
            string_keys: 512,
            counter_keys: 8,
            ops_per_client: 40,
            workload_seed,
            sim_seed,
            planted: false,
        }
    }
}

/// What one run produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimOutcome {
    /// The fold of every command the server completed, in the order it
    /// completed them. A function of the two seeds and the configuration
    /// alone — stable across processes, machines and builds.
    pub trace_hash: u64,
    /// The sum of every acknowledged `INCRBY` delta.
    pub expected_sum: i64,
    /// The sum of every counter key read back at the end.
    pub actual_sum: i64,
}

impl SimOutcome {
    /// Whether the run lost an update.
    ///
    /// `INCRBY` is order-independent, so a schedule cannot legitimately move
    /// this: any difference is an acknowledged increment that did not survive.
    #[must_use]
    pub const fn invariant_holds(&self) -> bool {
        self.expected_sum == self.actual_sum
    }
}

/// Folds `v` into the running hash `h`.
///
/// FxHash-style: cheap, order-dependent, and — unlike `DefaultHasher`, whose
/// output is explicitly not guaranteed stable across processes or Rust
/// versions — defined entirely by this function. Cross-process stability is
/// the product here, so the mixing function has to be ours.
#[must_use]
pub const fn mix(h: u64, v: u64) -> u64 {
    (h.rotate_left(5) ^ v).wrapping_mul(TRACE_MULTIPLIER)
}

/// Runs one simulation and reports what it observed.
///
/// # Panics
///
/// If the simulation itself fails — a host returning an error, or
/// [`SIM_DURATION`] elapsing with a client still running. Both are harness
/// bugs or deadlocks rather than findings about the system, and neither is
/// something a sweep can carry on past.
#[must_use]
pub fn run_sim(cfg: &SimConfig) -> SimOutcome {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(SIM_DURATION)
        .rng_seed(cfg.sim_seed)
        .build();

    let trace = Arc::new(Mutex::new(TRACE_INIT));
    let shared = Shared {
        expected: Arc::new(Mutex::new(0)),
        actual: Arc::new(Mutex::new(0)),
        done: Arc::new(Mutex::new(0)),
    };

    // The dict seed is derived from the simulator seed so two seeds do not
    // share a bucket layout: a hash collision that only shows up under one
    // placement then gets swept over instead of being baked into every run.
    let dict_seed = DictSeed {
        k0: mix(TRACE_INIT, cfg.sim_seed),
        k1: mix(GOLDEN, cfg.sim_seed),
    };
    let sink = HashSink(Arc::clone(&trace));
    let shards = cfg.shards;
    let executors = cfg.executors;
    let planted = cfg.planted;

    sim.host(SERVER, move || {
        // Cloned per invocation: turmoil may restart a host, and each start
        // needs its own future. The sink is shared on purpose — a restart
        // continues the same trace.
        let sink = sink.clone();
        server(shards, executors, dict_seed, sink, planted)
    });

    for id in 0..cfg.clients {
        sim.client(
            format!("client-{id}"),
            client(id, cfg.clone(), shared.clone()),
        );
    }
    sim.client("verifier", verifier(cfg.clone(), shared.clone()));

    sim.run().expect("simulation failed");

    SimOutcome {
        trace_hash: *lock(&trace),
        expected_sum: *lock(&shared.expected),
        actual_sum: *lock(&shared.actual),
    }
}

/// State the client hosts and the verifier share.
///
/// Every host in a turmoil simulation runs on the same OS thread, so these
/// mutexes are never actually contended; they are here because [`TraceSink`]
/// and the futures turmoil holds must be `Send`.
#[derive(Clone)]
struct Shared {
    /// The sum of every acknowledged `INCRBY` delta.
    expected: Arc<Mutex<i64>>,
    /// The sum of every counter read back at the end.
    actual: Arc<Mutex<i64>>,
    /// How many client hosts have finished their workload.
    done: Arc<Mutex<u32>>,
}

/// Takes a lock that cannot be contended, and says so if it was poisoned.
///
/// # Panics
///
/// If another host panicked while holding the lock. That has already failed
/// the run; this only reports where.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().expect("a simulated host panicked mid-update")
}

/// A [`TraceSink`] that folds every completed command into a shared hash.
///
/// Calls arrive in each shard's own execution order, which under a
/// deterministic scheduler is a function of the seeds alone.
#[derive(Clone)]
struct HashSink(Arc<Mutex<u64>>);

impl TraceSink for HashSink {
    fn record(&self, shard: u16, seq: u64, cmd: &Command, reply: &Reply) {
        let mut h = lock(&self.0);
        let mut acc = *h;
        acc = mix(acc, u64::from(shard));
        // `seq` is the shard's replication position *at which the command
        // ran*, not a counter of commands: a read reports the position it saw
        // without consuming it, so the same `seq` recurs. That is exactly what
        // we want folded — a schedule that reorders a write against a read
        // changes which position the read observed.
        acc = mix(acc, seq);
        acc = mix(acc, u64::from(cmd.kind()));
        acc = fold_bytes(acc, cmd.key());
        acc = fold_reply(acc, reply);
        *h = acc;
    }
}

/// Folds a byte string, length first so that concatenations cannot collide.
fn fold_bytes(mut h: u64, bytes: &[u8]) -> u64 {
    h = mix(
        h,
        u64::try_from(bytes.len()).expect("a slice length is a usize"),
    );
    for &b in bytes {
        h = mix(h, u64::from(b));
    }
    h
}

/// Folds a reply: a variant tag, then whatever distinguishes it.
///
/// The tags are part of the trace's meaning — changing one changes every
/// recorded hash, which is a deliberate cost.
fn fold_reply(h: u64, reply: &Reply) -> u64 {
    match reply {
        Reply::Bulk(None) => mix(h, 1),
        Reply::Bulk(Some(value)) => fold_bytes(mix(h, 2), value),
        Reply::Ok => mix(h, 3),
        Reply::Removed(removed) => mix(mix(h, 4), u64::from(*removed)),
        Reply::Integer(n) => mix(mix(h, 5), n.cast_unsigned()),
        // Folds the wire text, not the variant tag: the recorded hashes
        // predate the enum and must not move for a change that renamed
        // nothing a client can see.
        Reply::Error(error) => fold_bytes(mix(h, 6), error.wire_text().as_bytes()),
    }
}

/// A router that turns the atomic `INCRBY` into a read-modify-write race.
///
/// This is the harness's self-test: a simulator that never fails proves
/// nothing, so the branch ships a bug the sweep is required to find. The bug
/// is deliberately *above* the shard, which is where a real one would live —
/// the shard's handler is a plain `fn` and cannot await, so no lost update can
/// be planted inside it. Here `INCRBY` becomes `GET`, compute, `SET`: two
/// shard round-trips with an await between them, so another connection's
/// increment can land in the window and be overwritten.
///
/// Every other command passes straight through, so the only thing that differs
/// from an honest run is the atomicity of the one command carrying the
/// invariant.
#[derive(Clone)]
pub struct PlantedRouter(pub ShardPool);

impl Router for PlantedRouter {
    // Spelled `async fn` rather than the trait's desugared `-> impl Future`:
    // with a bare `async` block as the body clippy's `manual_async_fn` fires,
    // and the gate is `-D warnings`. `ShardPool`'s own impl keeps the
    // desugared form for a reason that does not apply here — it sends on the
    // inbox before the future is awaited.
    async fn dispatch(&self, cmd: Command) -> Reply {
        let Command::IncrBy { key, delta } = cmd else {
            return self.0.dispatch(cmd).await;
        };

        let current = match self.0.dispatch(Command::Get { key: key.clone() }).await {
            Reply::Bulk(Some(value)) => match parse_i64(&value) {
                Some(current) => current,
                None => return Reply::Error(ReplyError::NotAnInteger),
            },
            Reply::Bulk(None) => 0,
            // A shard that could not answer; pass its complaint on unchanged.
            other => return other,
        };
        let Some(updated) = current.checked_add(delta) else {
            return Reply::Error(ReplyError::WouldOverflow);
        };

        // The window, widened deliberately. `current` was read at one moment
        // and is written back at a later one, and nothing holds the key still
        // in between; this hands the scheduler an explicit chance to run
        // another connection there. Without it the window is one scheduler
        // round and the race surfaced in 2 seeds of 64 — evidence far weaker
        // than the claim the self-test makes. With it, 26 of 64.
        //
        // It is not a cheat: a genuine read-modify-write across an await is
        // exactly this shape, and the suspension point is honest rather than
        // simulated. It is also free — the honest router is untouched, and no
        // hash of a planted run is pinned anywhere, so no recorded trace moves.
        tokio::task::yield_now().await;

        match self
            .0
            .dispatch(Command::Set {
                key,
                value: updated.to_string().into_bytes(),
            })
            .await
        {
            Reply::Ok => Reply::Integer(updated),
            other => other,
        }
    }
}

/// The server host: the real stack, on a simulated listener.
///
/// The pool is spawned *here*, inside the host, rather than around
/// [`run_sim`]: [`ShardPool::spawn`] spawns onto the ambient tokio runtime,
/// and under turmoil each host has its own. Spawned outside, the shards would
/// land on whatever runtime happened to be current — or on none at all.
async fn server(
    shards: u16,
    executors: u16,
    seed: DictSeed,
    sink: HashSink,
    planted: bool,
) -> turmoil::Result {
    let pool = ShardPool::spawn(shards, executors, seed, sink);
    let listener = turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, PORT)).await?;
    loop {
        let (stream, _peer) = listener.accept().await?;
        // The choice is per connection only because that is where the router
        // is handed over; it is the same choice every time.
        if planted {
            tokio::spawn(serve_connection(stream, PlantedRouter(pool.clone())));
        } else {
            tokio::spawn(serve_connection(stream, pool.clone()));
        }
    }
}

/// One client host: connect, issue `ops_per_client` operations one at a time,
/// disconnect.
///
/// Sequential on purpose — see the module documentation. The only source of
/// interleaving is that there are many of these.
async fn client(id: u16, cfg: SimConfig, shared: Shared) -> turmoil::Result {
    // Decorrelated per client so client 1's stream is not client 0's shifted
    // by one, which a plain `workload_seed + id` would give.
    let mut rng = ChaCha8Rng::seed_from_u64(
        cfg.workload_seed ^ GOLDEN.wrapping_mul(u64::from(id).wrapping_add(1)),
    );
    let mut conn = Conn::connect().await?;

    for _ in 0..cfg.ops_per_client {
        let roll = rng.random_range(0..100u32);
        if roll < 20 {
            let key = rng.random_range(0..cfg.counter_keys);
            let delta = rng.random_range(-10..=10i64);
            let reply = conn
                .request(&command(&["INCRBY", &counter_key(key), &delta.to_string()]))
                .await?;
            // Only an acknowledged increment is owed to us. Anything else —
            // an error frame, a reply shape we did not expect — is not a
            // promise the server made, so counting it would manufacture a
            // violation the system never committed.
            if let Frame::Integer(_) = reply {
                *lock(&shared.expected) += delta;
            }
        } else if roll < 60 {
            let key = rng.random_range(0..cfg.string_keys);
            conn.request(&command(&["SET", &string_key(key), &format!("v{key}")]))
                .await?;
        } else if roll < 90 {
            let key = rng.random_range(0..cfg.string_keys);
            conn.request(&command(&["GET", &string_key(key)])).await?;
        } else {
            let key = rng.random_range(0..cfg.string_keys);
            conn.request(&command(&["DEL", &string_key(key)])).await?;
        }
    }

    *lock(&shared.done) += 1;
    Ok(())
}

/// The last client: waits for the workload to drain, then reads every counter.
///
/// It waits rather than racing because the invariant is about what the server
/// *settled on*: a counter read while an increment is still in flight is not
/// a lost update, it is an early read.
async fn verifier(cfg: SimConfig, shared: Shared) -> turmoil::Result {
    while *lock(&shared.done) < u32::from(cfg.clients) {
        // Simulated time: this costs ticks, not wall clock.
        tokio::time::sleep(VERIFIER_POLL).await;
    }

    let mut conn = Conn::connect().await?;
    let mut total: i64 = 0;
    for key in 0..cfg.counter_keys {
        let reply = conn.request(&command(&["GET", &counter_key(key)])).await?;
        total += match reply {
            // Never incremented, or incremented back out of existence.
            Frame::Null => 0,
            Frame::Bulk(value) => {
                parse_i64(&value).ok_or_else(|| format!("counter {key} is not an integer"))?
            }
            other => return Err(format!("counter {key} answered with {other:?}").into()),
        };
    }
    *lock(&shared.actual) = total;
    Ok(())
}

/// The name of a counter key — touched only by `INCRBY`.
fn counter_key(index: u32) -> String {
    format!("counter-{index}")
}

/// The name of a string key — touched by `GET`, `SET` and `DEL`.
fn string_key(index: u32) -> String {
    format!("k{index}")
}

/// Builds a RESP2 command frame: an array of bulk strings, as a real client
/// sends.
fn command(parts: &[&str]) -> Frame {
    Frame::Array(
        parts
            .iter()
            .map(|part| Frame::Bulk(part.as_bytes().to_vec()))
            .collect(),
    )
}

/// A client's connection: the real codec over a simulated socket.
struct Conn {
    stream: turmoil::net::TcpStream,
    /// Bytes read from the server that do not yet form a whole frame.
    buf: Vec<u8>,
    /// Scratch for encoding, reused so a client does not allocate per request.
    out: Vec<u8>,
}

impl Conn {
    /// Opens a connection to the simulated server.
    async fn connect() -> turmoil::Result<Self> {
        Ok(Self {
            stream: turmoil::net::TcpStream::connect((SERVER, PORT)).await?,
            buf: Vec::new(),
            out: Vec::new(),
        })
    }

    /// Sends one command and reads exactly one reply.
    ///
    /// `flush` after `write_all` even though turmoil's socket sends on write:
    /// a transport that buffers would otherwise hold a request the client is
    /// blocked waiting on, and that deadlock would only appear under whatever
    /// transport we ported to next.
    async fn request(&mut self, frame: &Frame) -> turmoil::Result<Frame> {
        self.out.clear();
        encode(frame, &mut self.out);
        self.stream.write_all(&self.out).await?;
        self.stream.flush().await?;

        let mut chunk = [0u8; CLIENT_CHUNK];
        loop {
            if let Some((reply, used)) = parse(&self.buf)? {
                self.buf.drain(..used);
                return Ok(reply);
            }
            let got = self.stream.read(&mut chunk).await?;
            if got == 0 {
                return Err("the server closed the connection mid-request".into());
            }
            self.buf.extend_from_slice(&chunk[..got]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seeds_same_hash_and_no_lost_updates() {
        let a = run_sim(&SimConfig::mini(1, 42));
        let b = run_sim(&SimConfig::mini(1, 42));
        assert_eq!(a.trace_hash, b.trace_hash, "in-process determinism");
        assert!(a.invariant_holds(), "atomic INCRBY must not lose updates");
        // Without this the invariant assertion is vacuous: a workload that
        // issued no acknowledged `INCRBY` at all satisfies `0 == 0`.
        assert_ne!(a.expected_sum, 0, "the workload acknowledged no INCRBY");
        assert_eq!(
            a.expected_sum, b.expected_sum,
            "a pinned workload seed must issue the same increments"
        );
        let c = run_sim(&SimConfig::mini(1, 43));
        assert_ne!(
            a.trace_hash, c.trace_hash,
            "different sim seed, different schedule"
        );
    }

    #[test]
    fn the_trace_hash_is_pinned_across_processes_and_builds() {
        // The harness's product. Every other assertion about the trace compares
        // two runs of the *same* build to each other, and stays green if the
        // hash moves globally — a `cargo update` that reorders tokio's ready
        // queue or changes `rand`'s sampling would silently retire every seed
        // ever filed against this project, and nothing would say so.
        //
        // Unlike the SipHash and CRC vectors, this number has no external
        // reference to be derived from: it is definitionally whatever this
        // system computes. So it pins *stability*, not correctness, and that is
        // the whole job. A mismatch here is not a bug report — it means the
        // trace's meaning changed, and the question to answer is whether that
        // was intended. When it was (a new command kind, a new folded field),
        // update the constant in the same commit that caused it, and say so in
        // the message. Never update it to make a red suite green.
        const MINI_1_42: u64 = 0x2bbb_5d5f_3268_c6c6;

        let outcome = run_sim(&SimConfig::mini(1, 42));
        assert_eq!(
            outcome.trace_hash, MINI_1_42,
            "the recorded trace hash moved"
        );
        // The workload behind the hash, pinned separately: the two can drift
        // apart, and a changed workload with a coincidentally equal hash is the
        // one failure the assertion above cannot see.
        assert_eq!(outcome.expected_sum, 99, "the recorded workload moved");
    }
}
