//! The trace hash: how every command and reply the shards see folds into one
//! `u64`, and the constants that make two processes fold alike.
//!
//! Changing anything here moves every pinned hash.

use seedstone_core::shard::{Command, Reply, Route, TraceSink};
use std::sync::{Arc, Mutex};

use crate::outcome::lock;

/// The odd 64-bit constant from Fibonacci hashing, used both to decorrelate
/// per-client workload seeds and as the trace hash's multiplier.
pub const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

/// The trace hash's multiplier — the FxHash constant.
const TRACE_MULTIPLIER: u64 = 0x517c_c1b7_2722_0a95;

/// The trace hash's starting value.
///
/// Non-zero so that an empty trace is distinguishable from a trace that
/// happened to fold back to zero, and so that a `mix` of zeros still moves.
pub const TRACE_INIT: u64 = 0xcbf2_9ce4_8422_2325;

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

/// A [`TraceSink`] that folds every completed command into a shared hash.
///
/// Calls arrive in each shard's own execution order, which under a
/// deterministic scheduler is a function of the seeds alone.
#[derive(Clone)]
pub struct HashSink(pub Arc<Mutex<u64>>);

impl TraceSink for HashSink {
    fn record(&self, shard: u16, seq: u64, cmd: &Command, reply: &Reply) {
        let mut h = lock(&self.0);
        let mut acc = *h;
        acc = mix(acc, u64::from(shard));
        // `seq` is the replication position where the command's effects
        // *began*, not a counter of commands and not an index into the log: a
        // read consumes no position, so the same `seq` recurs, and a write
        // that first had to evict an expired key reports the eviction's
        // position rather than its own. That is exactly what we want folded —
        // a schedule that reorders a write against a read changes which
        // position the read observed. `TraceSink::record` carries the full
        // definition.
        acc = mix(acc, seq);
        acc = mix(acc, u64::from(cmd.kind()));
        acc = match cmd.route() {
            // Byte-for-byte what this folded when a command could only name a
            // key, so no recorded trace hash moves.
            Route::Key(key) => fold_bytes(acc, key),
            // A route with no key still has to reach the hash: two commands of
            // one kind that went to different shards are different commands,
            // and `shard` above only says where the answer came from.
            //
            // Nothing produces `Route::Shard` today — the arm is here for its
            // tag, which no other route may take.
            Route::Shard(shard) => mix(mix(acc, 1), u64::from(shard)),
            Route::Every => mix(acc, 2),
            // A `ScanStep` folds a constant here, because the variant names
            // no shard of its own and the shard that ran it arrives as
            // `record`'s own argument, folded above. What decides the step is
            // its arguments, and those are folded below.
            Route::Unaddressed => mix(acc, 3),
        };
        acc = fold_inputs(acc, cmd);
        acc = fold_reply(acc, reply);
        *h = acc;
    }
}

/// Folds whatever of a command's arguments its route does not already reach.
///
/// Every command that names a key folds it through [`Route::Key`], and what
/// the rest of its arguments did is visible in the reply the shard gave. A
/// scan step is the exception on both counts: it names no key, and two steps
/// with different cursors, counts or patterns can answer alike — an empty
/// batch and a spent cursor look the same however they were asked for. So a
/// walk driven from the workload would otherwise fold its outcome and none of
/// its inputs, and a divergence in *which step was taken* would be invisible
/// until it happened to change an answer.
fn fold_inputs(h: u64, cmd: &Command) -> u64 {
    match cmd {
        Command::ScanStep {
            cursor,
            count,
            pattern,
        } => {
            let h = mix(mix(h, *cursor), *count as u64);
            pattern
                .as_ref()
                .map_or_else(|| mix(h, 0), |pattern| fold_bytes(mix(h, 1), pattern))
        }
        // Every other command's key is folded by its route, and its remaining
        // arguments are decided by the reply. A command added later whose
        // behaviour turns on an argument neither of those reaches belongs
        // here, not in a comment saying it does not matter yet.
        _ => h,
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
pub fn fold_reply(h: u64, reply: &Reply) -> u64 {
    match reply {
        Reply::Bulk(None) => mix(h, 1),
        Reply::Bulk(Some(value)) => fold_bytes(mix(h, 2), value),
        Reply::Ok => mix(h, 3),
        // Tag 8 because 1 to 7 were already spoken for, and the text after it
        // because two statuses are two different answers.
        Reply::Status(text) => fold_bytes(mix(h, 8), text.as_bytes()),
        Reply::Removed(removed) => mix(mix(h, 4), u64::from(*removed)),
        Reply::Integer(n) => mix(mix(h, 5), n.cast_unsigned()),
        // Both fields, not just the cursor: two steps that resumed at the same
        // place and returned different keys are different answers, and a walk
        // that lost a key while its cursor kept advancing is exactly the
        // regression a trace hash is here to make visible.
        // `visited` is deliberately absent: it is how many buckets the step
        // walked, an accounting figure the edge budgets by, not an answer a
        // client sees. Folding it would make the recorded hashes depend on a
        // number no observer can read.
        Reply::Scan {
            cursor,
            keys,
            visited: _,
        } => {
            let mut acc = mix(mix(h, 7), *cursor);
            for key in keys {
                acc = fold_bytes(acc, key);
            }
            acc
        }
        // Every field a replay must agree about, in declaration order. It is
        // one reply, and its content is what a divergence between two runs
        // would show in — a shard that counted a hit the replay counted as a
        // miss has diverged, whether or not any assertion happens to read
        // that field.
        //
        // `usec` is the one field deliberately left out, for the reason
        // `visited` is left out above and a stronger one besides: it is a
        // reading of a clock. Under this simulator it is exactly zero — a
        // handler cannot `await`, so no simulated instant passes inside one,
        // and `tests/command_timing.rs` holds the runtime to that — but a
        // figure whose zero depends on the runtime is not a figure a recorded
        // hash may be pinned to. The destructuring is exhaustive so that a
        // field added to `ShardStats` later stops compiling here rather than
        // being silently folded or silently skipped.
        Reply::Stats(stats) => {
            let seedstone_core::shard::ShardStats {
                keys,
                expires,
                evicted,
                hits,
                misses,
                expired,
                calls,
                usec: _,
            } = **stats;
            let mut acc = mix(mix(h, 9), keys);
            for field in [expires, evicted, hits, misses, expired] {
                acc = mix(acc, field);
            }
            for count in calls {
                acc = mix(acc, count);
            }
            acc
        }
        // Folds the wire text, not the variant tag: the recorded hashes
        // predate the enum and must not move for a change that renamed
        // nothing a client can see.
        Reply::Error(error) => fold_bytes(mix(h, 6), error.wire_text().as_bytes()),
    }
}
