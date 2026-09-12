//! SeedStone's connection layer: RESP2 frames in, [`Command`]s out, replies back.
//!
//! Netless by construction. Nothing here opens a socket — [`serve_connection`]
//! is generic over its transport, which is what lets the production binary
//! hand it a `tokio::net::TcpStream` and the simulator hand it a simulated
//! one, with the same code in between.
//!
//! [`serve_connection`] is generic over its [`Router`] too, which is what lets
//! the simulator run the real connection code against a deliberately racy one.
//! Nothing here knows whether it is talking to a socket or to a `duplex` pipe
//! in a test.
//!
//! # What this layer is responsible for
//!
//! It is the only place where bytes a peer chose become something the rest of

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
