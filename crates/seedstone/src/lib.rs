//! SeedStone: an in-memory key-value store, written from scratch in Rust.
//!
//! This crate is the edge. It owns the one thing the layers below it must not:
//! contact with the operating system — a listening socket, the signal handlers
//! a clean shutdown needs and the entropy the keyspace hasher is seeded from.
//! Everything under it is
//! deterministic by construction and receives what it needs as arguments,
//! which is what lets the simulator run the same code over a simulated
//! network.
//!
//! [`server::Server`] is the accept loop; the binary next to it is a
//! composition root and nothing else.

pub mod server;
