//! The connection layer's end-to-end tests: each serves RESP over a `duplex`
//! pipe through [`crate::connection::serve_connection`] and reads the frames
//! back. A test of a private item lives beside the item, in its module.

pub mod support;

mod auth;
mod buffers;
mod commands;
mod containers;
mod errors;
mod info;
mod pipeline;
mod walk;
