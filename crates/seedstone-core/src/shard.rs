//! The shard runtime: N keyspaces, hosted by a smaller number of executor
//! tasks, each behind an unbounded inbox.
//!
//! A virtual shard is a [`crate::dict::Dict`] nothing else can reach, its replication
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
//! [`crate::shard::apply::apply`] takes `&mut Dict` and returns a `Reply`. It is not `async`, and
//! that is the point: a handler that cannot `await` cannot yield the executor
//! mid-command, so a command either has not started or has finished, and two
//! commands on one key can never interleave. The rule is enforced by the
//! signature rather than by review — the only `await`s in an executor task are
//! the `select!` arms of [`crate::shard::executor::run_executor`]. A batch inherits the property: no
//! `await` separates its commands either, so nothing from another connection
//! can land inside one.
//!
//! That is also why the interesting concurrency bugs of this system live
//! *above* the shard, in code that sends two messages with an `await` between
//! them. The simulator plants exactly that race.

mod apply;
mod command;
mod executor;
mod policy;
mod pool;
mod reply;

pub use apply::parse_i64;
pub use command::{Command, Cond, Expiry, KIND_SLOTS, Route};
pub use executor::{EVICTION_SAMPLES, HOUSEKEEPING_TICK};
pub use policy::{Deadlines, EvictionPolicy, ExpiryPolicy, NoTrace, ShardPolicy, TraceSink};
pub use pool::{Envelope, Router, ShardPool, ShardStats};
pub use reply::{Reply, ReplyError};

#[cfg(test)]
mod tests;
