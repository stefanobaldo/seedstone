//! SeedStone's connection layer: RESP2 frames in, [`seedstone_core::shard::Command`]s out,
//! replies back.
//!
//! Netless by construction. Nothing here opens a socket — [`serve_connection`]
//! is generic over its transport, which is what lets the production binary
//! hand it a `tokio::net::TcpStream` and the simulator hand it a simulated
//! one, with the same code in between.
//!
//! [`serve_connection`] is generic over its [`seedstone_core::shard::Router`] too, which is what lets
//! the simulator run the real connection code against a deliberately racy one.
//! Nothing here knows whether it is talking to a socket or to a `duplex` pipe
//! in a test.
//!
//! # Where each thing lives
//!
//! This layer is the only place where bytes a peer chose become something the
//! rest of the system acts on, so it owns the limits. Each module says how it
//! owns its share:
//!
//! - `connection` — the loop: reads, the three buffers and what bounds them,
//!   the chunking of a pipeline, the writes.
//! - `dispatch` — where a command is answered: the table, and `Action`, the decision
//!   made explicit.
//! - `fan_out` — the commands no single shard can answer, and the walks over
//!   the whole keyspace.
//! - `reply` — rendering a shard's answer, and the framing that keeps a
//!   peer from dictating frames the server never meant to send.
//! - `node` — what a connection may say about the process it runs in.
//! - `info`, `containers`, `hello`, `auth` — the commands answered
//!   here rather than by a shard.
//! - `options`, `expiry`, `walk` — the parsing and the arithmetic those
//!   answers are built from.

mod auth;
mod connection;
mod containers;
mod dispatch;
mod expiry;
mod fan_out;
mod hello;
mod info;
mod node;
mod options;
mod reply;
mod walk;

pub use auth::{AUTH_NOT_CONFIGURED, NOAUTH, NOAUTH_HELLO, Secret, WRONGPASS};
pub use connection::{IDLE_SHED_AFTER, MAX_REQUEST_BYTES, serve_connection};
pub use dispatch::command_names;
pub use fan_out::{INVALID_CURSOR, KEYS_REPLY_BYTES, KEYS_TOO_LARGE, WALK_STEP_BUCKETS};
pub use hello::NOPROTO;
pub use node::{EDGE_NAMES, NodeInfo, RUN_ID_HEX};
pub use options::SYNTAX_ERROR;

#[cfg(test)]
mod tests;
