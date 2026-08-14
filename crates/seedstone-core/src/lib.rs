//! SeedStone deterministic core: shard runtime, keyspace, log abstraction.
//!
//! The shard runtime is protocol-agnostic: it speaks its own [`shard::Command`]
//! and [`shard::Reply`] vocabulary and never sees a wire frame. That is now a
//! property of the build rather than a rule to remember — the codec is not
//! among this crate's dependencies, so a handler cannot reach for a frame.
//! Translating frames into commands is `seedstone-service`'s job.

pub mod dict;
pub mod log;
pub mod shard;
pub mod slot;
