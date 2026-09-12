//! What a connection may say about the node it is running on. This layer has
//! no clock, no port and no way to count its peers of its own, so everything
//! a connection command answers about the process arrives as [`NodeInfo`],
//! assembled once at the composition root and cloned per connection.

use crate::auth::Secret;
use seedstone_core::memory::{MemoryGauge, MemoryLimit};
use seedstone_core::shard::KIND_SLOTS;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::time::Instant;

/// What this server answers `HELLO` with, and what it calls itself.
pub const SERVER_NAME: &str = "seedstone";

/// The deployment shape this node is in, as `HELLO` and `INFO` both report it.
///
/// One constant for the two answers rather than a literal in each: a node that
/// told `HELLO` one thing and `INFO` another would be a node whose clients
/// disagree about what they are connected to, and that is exactly the kind of
/// drift a second literal invites.
pub const SERVER_MODE: &str = "standalone";

/// How many hexadecimal characters a `run_id` is. Redis's width, because a
/// monitor that stores the field stores it at Redis's size.
pub const RUN_ID_HEX: usize = 40;

/// What each [`Command::kind`] is called on the wire, indexed by the tag.
///
/// `commandstats` reports a count per command name, and the shards count per
/// command *kind* — so this is the one place the two vocabularies meet.
/// Slot `0` is no command; slot `15` is [`Command::Stats`], which is how the
/// shards answer an `INFO` and never something a peer sent, so it is counted
/// at the edge as `info` and the shards leave it alone.
///
/// `ScanStep` is `scan` because that is the command a peer sends to produce
/// one — and a `KEYS` walk runs steps too, so its steps are counted here under
/// `scan` *as well as* once at the edge under `keys`. A single `KEYS` on a
/// sixteen-shard node adds sixteen or more to `cmdstat_scan` with no peer
/// having sent a `SCAN`. Both lines are true of different things and neither
/// is the other's total; see [`commands_processed`].
///
/// Sized from [`KIND_SLOTS`] rather than from a literal, so a command added to
/// the core does not leave this table one name short of the tags it is indexed
/// by — the core's `every_kind_tag_is_contiguous_and_bounded` says which tags
/// exist, and the assertion below says this table has a name for each.
pub const KIND_NAMES: [&str; KIND_SLOTS] = [
    "",
    "get",
    "set",
    "del",
    "incrby",
    "expire",
    "ttl",
    "exists",
    "flushdb",
    "dbsize",
    "scan",
    "pexpire",
    "persist",
    "type",
    "strlen",
    "info",
    "setex",
    "setnx",
    "psetex",
    "pttl",
    "expireat",
    "pexpireat",
];

const _: () = assert!(
    KIND_NAMES.len() == KIND_SLOTS,
    "every command kind the core can tag needs a name here: commandstats \
     indexes this table by the tag itself"
);

/// The commands this layer answers or splits itself, counted here because no
/// shard can count them.
///
/// Three kinds of thing are here. The connection's own business — `PING`,
/// `AUTH`, `HELLO` and the rest — reaches no shard at all. `MGET` and `KEYS`
/// reach shards under other names: an `MGET` is a pile of `GET`s and a `KEYS`
/// is a pile of scan steps, so the shards count what they ran and this counts
/// the request the peer actually made. `DBSIZE` and `FLUSHDB` reach *every*
/// shard, so counting them where they land would report one request as one
/// per shard.
///
/// The order is this array's own — it is what `# Commandstats` prints after
/// the keyed commands, and nothing else reads it. It deliberately does not
/// track [`COMMANDS`]: a name is appended here when a command is added there,
/// and reordering the existing ones to match would move fields in a document
/// operators already read for no reader's benefit.
pub const EDGE_NAMES: [&str; 15] = [
    "mget", "keys", "dbsize", "flushdb", "ping", "echo", "auth", "hello", "info", "command",
    "client", "quit", "config", "slowlog", "latency",
];

/// The wall-clock reading a node with no wall clock reports: 2023-11-14
/// 22:13:20 UTC, in milliseconds.
///
/// A round number in the recent past, chosen only to be recognisable in a
/// failure message. See [`NodeInfo::for_tests`].
pub const FIXED_UNIX_MILLIS: u64 = 1_700_000_000_000;

/// What a connection may say about the node it is running on.
///
/// Every field is a fact about the process, not about the connection, so this
/// is assembled once where the process is — the composition root — and cloned
/// per connection. It is a parameter rather than something this layer reads for
/// itself because none of it is knowable here: the port is the one the kernel
/// chose, the start is a reading of a clock, and the peer count is maintained
/// by the accept loop. Passing them in is what keeps the connection layer a
/// pure function of its inputs, and therefore replayable.
///
/// [`now_unix_millis`](Self::now_unix_millis) is the exception that proves the
/// rule: it is not a fact but the dependency that supplies one, because the
/// fact it supplies changes while a connection is being served. Everything
/// said above about why the others are passed in applies to it doubly.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    /// The version this node reports.
    pub version: &'static str,
    /// The port the listener actually bound, which with an ephemeral port is
    /// not the port the configuration asked for.
    pub tcp_port: u16,
    /// The address the listener bound, without the port beside it.
    ///
    /// A rendered string rather than an `IpAddr` because the only thing this
    /// layer does with it is print it: `CONFIG GET bind` answers whatever
    /// the socket was actually given, which with an unspecified address is
    /// not the address the configuration named.
    pub bind: String,
    /// How many connections the node will serve at once.
    ///
    /// Maintained nowhere here — the accept loop holds the semaphore this
    /// counts — so it travels as a plain number a report can quote.
    pub max_clients: usize,
    /// When the node started, on the monotonic clock.
    ///
    /// A [`tokio::time::Instant`] and never a `SystemTime`: uptime is a span,
    /// and a span measured against a clock an operator can step backwards is
    /// not a span. It is also the clock the simulator controls, so a replay
    /// reports the uptime the run had rather than the one the wall had.
    pub started: Instant,
    /// How many connections are attached right now.
    ///
    /// Shared with whoever accepts connections, which is the only party that
    /// can maintain it; this layer only ever reads it.
    pub connected: Arc<AtomicU64>,
    /// Unix time, in milliseconds — the wall clock, injected rather than read.
    ///
    /// One command family needs it: `SET`'s `EXAT`/`PXAT` name a deadline on
    /// the clock people set their watches by, and nothing else in this server
    /// does. Every other deadline is a span against the monotonic clock, which
    /// is why [`started`](Self::started) is an [`Instant`] and says there why.
    ///
    /// It arrives as a function rather than as a reading because a reading
    /// taken here would be stale by the time a connection used it, and as a
    /// *dependency* rather than a call to `SystemTime::now` because the wall
    /// clock is the one input a replay cannot reproduce: the simulator drives
    /// this layer, and it controls the monotonic clock and nothing else. A
    /// simulated node is handed a clock of its own, so a run that resolves an
    /// absolute deadline resolves it the same way every time it is replayed;
    /// the real node is handed the real one, so a client's `EXAT` means what
    /// it means everywhere else.
    pub now_unix_millis: fn() -> u64,
    /// The node-wide memory figure, kept current by the shard executors and
    /// read here for `INFO`. Cloned from the pool at the composition root —
    /// and in the simulator from its pool — so the number a scrape reads is
    /// the number the eviction decision is taken against.
    pub memory: MemoryGauge,
    /// The ceiling that figure is held under, and what happens at it.
    ///
    /// Read from the pool at the composition root, for the reason
    /// [`memory`](Self::memory) is: what `INFO` reports and what the
    /// executors evict by must be the same value, not two configurations that
    /// happen to agree.
    pub limit: MemoryLimit,
    /// The password every connection must present, or `None` on a node that
    /// asks for none.
    ///
    /// A fact about the process like everything else here, and read from the
    /// composition root for the reason the others are — but with one more
    /// consequence: this is the only field whose *absence* changes what the
    /// connection loop will run. `None` is an open node, which is why the
    /// edge refuses to configure one on an address a network can reach.
    pub password: Option<Secret>,
    /// Forty hexadecimal characters identifying this run of the process,
    /// drawn once at the composition root beside the keyspace seed.
    ///
    /// It exists so that a monitor can tell a node that has been restarted
    /// from one that has not: every counter below resets when this changes,
    /// and a rate computed across the boundary without noticing is a rate
    /// computed from a fall to zero. Redis's own field, spelling and width
    /// included, because that is what reads it.
    pub run_id: String,
    /// The operating system's identifier for this process.
    pub process_id: u32,
    /// The path this process was started from, or `unknown` where there is
    /// none to report.
    pub executable: String,
    /// Connections accepted since the node started — a running total, unlike
    /// [`connected`](Self::connected) beside it, which is a gauge.
    pub total_connections: Arc<AtomicU64>,
    /// Connections refused because the limit was already spent.
    pub rejected_connections: Arc<AtomicU64>,
    /// Bytes read from peers, counted where they are read.
    pub net_in: Arc<AtomicU64>,
    /// Bytes written to peers, counted where they are flushed.
    pub net_out: Arc<AtomicU64>,
    /// Error replies written to peers, whatever produced them — a handler, the
    /// auth gate, the parser. `INFO stats` reports the total as
    /// `total_error_replies` and `INFO errorstats` one row per code, the way
    /// Redis 6.2 (`redis:6-alpine`, `redis_version:6.2.24`) prints them.
    /// Counted where the frame is appended to the outgoing buffer, because
    /// that is the one place every error passes through.
    pub error_replies: Arc<AtomicU64>,
    /// Per-code counts behind `errorstats`. A `BTreeMap` so the section
    /// renders in one order; a mutex because errors are rare enough that a
    /// lock on the error path costs nothing anyone will measure, and the
    /// critical section never awaits.
    pub errorstats: Arc<std::sync::Mutex<std::collections::BTreeMap<String, u64>>>,
    /// How many of each [`EDGE_NAMES`] command peers have sent, in that
    /// order.
    ///
    /// The commands this layer answers or splits itself, which for that
    /// reason no shard can count: see [`EDGE_NAMES`] for what belongs here
    /// and what is counted by the shard that runs it instead.
    pub edge_calls: Arc<[AtomicU64; EDGE_NAMES.len()]>,
    /// Microseconds those commands spent here, in the same order.
    ///
    /// Timed where they are counted, and for the same reason: an `MGET` over
    /// four keys is one request, and the four `GET`s it became are timed by
    /// the shards that ran them. Adding those four to this would report the
    /// same microseconds twice under two names.
    ///
    /// **This is not the shards' figure and does not behave like it.** A
    /// command counted here is timed across the wait for the shards it
    /// reached, so its reading includes time the executors spent on other
    /// connections' work — it is what the request took, not what it cost. The
    /// shards' [`usec`](seedstone_core::shard::ShardStats::usec) is the
    /// second, and it is the one that reads zero under a simulated clock.
    pub edge_usec: Arc<[AtomicU64; EDGE_NAMES.len()]>,
}

impl NodeInfo {
    /// A node description for callers that have no node to describe: the tests
    /// here, and the simulator.
    ///
    /// The port, the address and the client ceiling are Redis's own defaults,
    /// so that each value is recognisable rather than arbitrary. The count
    /// starts at zero and stays there: maintaining it belongs to whoever
    /// accepts connections, and neither caller has a workload that asks.
    ///
    /// The wall clock stands still, at [`FIXED_UNIX_MILLIS`]. Neither caller
    /// has a real one to offer — there is no simulated `SystemTime`, and the
    /// only clock the simulator advances is the monotonic one — so a frozen
    /// reading is the honest answer rather than a limitation: it makes an
    /// absolute deadline resolve identically in every replay of a run, which
    /// is the whole reason this is a parameter.
    #[must_use]
    pub fn for_tests() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
            tcp_port: 6379,
            bind: "127.0.0.1".to_owned(),
            max_clients: 10_000,
            started: Instant::now(),
            connected: Arc::new(AtomicU64::new(0)),
            now_unix_millis: || FIXED_UNIX_MILLIS,
            memory: MemoryGauge::default(),
            limit: MemoryLimit::default(),
            password: None,
            // A node with no process to describe says so: forty zeros is not
            // a run identifier any process would draw, so a document carrying
            // it is recognisably a test's rather than a node's.
            run_id: "0".repeat(RUN_ID_HEX),
            process_id: 0,
            executable: "unknown".to_owned(),
            total_connections: Arc::new(AtomicU64::new(0)),
            rejected_connections: Arc::new(AtomicU64::new(0)),
            net_in: Arc::new(AtomicU64::new(0)),
            net_out: Arc::new(AtomicU64::new(0)),
            error_replies: Arc::new(AtomicU64::new(0)),
            errorstats: Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new())),
            edge_calls: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
            edge_usec: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
        }
    }

    /// Whether a connection to this node must authenticate before it can run
    /// anything.
    #[must_use]
    pub const fn requires_auth(&self) -> bool {
        self.password.is_some()
    }
}

/// Which [`EDGE_NAMES`] slot a command name is counted and timed in, or
/// `None` if it is a command the shard that runs it counts instead.
///
/// The one lookup both figures go through, on purpose: a name counted in one
/// slot and timed in another would make `usec_per_call` a quotient of two
/// different commands.
pub fn edge_slot(name: &[u8]) -> Option<usize> {
    EDGE_NAMES
        .iter()
        .position(|edge| edge.as_bytes().eq_ignore_ascii_case(name))
}

/// Whole microseconds since `started`.
///
/// Saturating rather than wrapping, and the ceiling is not reachable: a single
/// command would have to run for half a million years to reach it.
pub fn micros_since(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}
