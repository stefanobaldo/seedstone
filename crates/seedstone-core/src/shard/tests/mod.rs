//! The shard's tests through the pool: each spawns a [`crate::shard::ShardPool`]
//! (or a [`crate::shard::executor::ShardState`] directly) and drives commands.
//! Tests of a private item live beside it.

pub mod support;

mod commands;
mod eviction;
mod expiry;
mod policy;
mod pool;
mod replication;
