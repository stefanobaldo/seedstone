//! SeedStone deterministic core: shard runtime, keyspace, log abstraction.
//!
//! The shard runtime is protocol-agnostic: it speaks its own `Command` and
//! `Reply` vocabulary and never sees a wire frame. This crate nevertheless
//! depends on the RESP codec because the service layer that translates frames
//! into those commands lives here, at the crate's edge — the dependency
//! belongs to that translation boundary, not to the core itself.

pub mod dict;
pub mod log;
pub mod service;
pub mod shard;
pub mod slot;
