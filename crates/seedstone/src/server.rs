//! The TCP edge: a command line in, a listening socket out, connections
//! served until the process is asked to stop.
//!
//! Everything here is what the deterministic core deliberately does not have —
//! a real socket, a signal handler, a bound on how many peers may be attached
//! at once. The core is reached through [`seedstone_core::service::serve_connection`],
//! which is generic over its transport, so the bytes a [`tokio::net::TcpStream`]
//! carries take exactly the path the simulator's virtual ones do.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use seedstone_core::dict::DictSeed;
use seedstone_core::service::serve_connection;
use seedstone_core::shard::{NoTrace, ShardPool};
use seedstone_resp::{Frame, encode};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

/// The address a node listens on when the command line says nothing.
///
/// Loopback, not `0.0.0.0`: a store with no authentication must not become
/// reachable from a network because someone ran it with no arguments.
const DEFAULT_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6379);

/// How many connections a node serves at once when the command line says
/// nothing. Redis's own default, so an operator's expectations transfer.
const DEFAULT_MAX_CLIENTS: usize = 10_000;

/// How many virtual shards a node runs.
///
/// Fixed rather than configurable: a key's shard is a function of the count,
/// so this number is part of the placement, and a node that could be restarted
/// with a different one would be a node whose keyspace moved. It is a
/// deployment-format decision, not a flag.
const SHARDS: u16 = 1024;

/// What a peer is told when the connection limit is already spent.
///
/// Byte-exact to Redis: clients match on this text.
pub const MAX_CLIENTS_REACHED: &str = "ERR max number of clients reached";

/// The usage text, printed on any argument this binary does not understand.
const USAGE: &str = "usage: seedstone [--bind ADDR:PORT] [--max-clients N]";

/// What the binary was asked to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// The address to listen on.
    pub bind: SocketAddr,
    /// How many connections may be served at once.
    pub max_clients: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: DEFAULT_BIND,
            max_clients: DEFAULT_MAX_CLIENTS,
        }
    }
}

impl Config {
    /// Parses the command line.
    ///
    /// Two flags do not justify an argument-parsing dependency, and the
    /// composition root is the last place to want one: every crate that
    /// reaches the binary is a crate whose transitive graph the linkage gate
    /// has to keep honest.
    ///
    /// # Errors
    ///
    /// The usage text, prefixed by what was wrong, when an argument is
    /// unknown, is missing its value, or does not parse. `--max-clients 0` is
    /// rejected too: a server that can serve nobody is a configuration
    /// mistake, not a configuration.
    pub fn from_args(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut cfg = Self::default();
        let mut args = args;
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--bind" => {
                    let value = value_for(&flag, &mut args)?;
                    cfg.bind = value
                        .parse()
                        .map_err(|_| refused(&format!("--bind: not an address: {value}")))?;
                }
                "--max-clients" => {
                    let value = value_for(&flag, &mut args)?;
                    cfg.max_clients = match value.parse::<usize>() {
                        Ok(n) if n > 0 => n,
                        _ => {
                            return Err(refused(&format!(
                                "--max-clients: not a positive integer: {value}"
                            )));
                        }
                    };
                }
                other => return Err(refused(&format!("unknown argument: {other}"))),
            }
        }
        Ok(cfg)
    }
}

/// The value that must follow `flag`.
fn value_for(flag: &str, args: &mut impl Iterator<Item = String>) -> Result<String, String> {
    args.next()
        .ok_or_else(|| refused(&format!("{flag}: missing its value")))
}

/// A complaint followed by the usage text.
fn refused(what: &str) -> String {
    format!("{what}\n{USAGE}")
}

/// A bound listener and the shards behind it.
pub struct Server {
    listener: TcpListener,
    local_addr: SocketAddr,
    max_clients: usize,
    pool: ShardPool,
}

impl Server {
    /// Binds the configured address and starts the shard tasks.
    ///
    /// `seed` fixes the keyspace hashing for the whole node. It is an argument
    /// rather than something drawn here, so a caller that wants a reproducible
    /// node — every test in this crate — gets one for free, and the single
    /// place that draws entropy stays visible in `main`.
    ///
    /// # Errors
    ///
    /// Whatever binding the address failed with: in practice the port already
    /// being in use, or an address this host does not own.
    pub async fn bind(cfg: Config, seed: DictSeed) -> std::io::Result<Self> {
        let listener = TcpListener::bind(cfg.bind).await?;
        // Asked of the socket rather than copied from the config: with port 0
        // the kernel chose, and the caller needs to learn what it chose.
        let local_addr = listener.local_addr()?;
        Ok(Self {
            listener,
            local_addr,
            max_clients: cfg.max_clients,
            pool: ShardPool::spawn(SHARDS, seed, NoTrace),
        })
    }

    /// The address actually bound.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accepts connections until the process is interrupted.
    ///
    /// Each accepted connection takes a permit from a semaphore sized by
    /// `max_clients` and runs on its own task; a connection arriving with no
    /// permit left is told so and closed rather than queued, because a peer
    /// waiting silently on a connection the server will not read is worse than
    /// a peer that knows.
    ///
    /// Shutdown is a clean exit from the accept loop: connections already
    /// running finish on their own tasks, and the process ends when the
    /// runtime does.
    pub async fn run(self) {
        let clients = Arc::new(Semaphore::new(self.max_clients));
        loop {
            tokio::select! {
                // `biased` for the reason the shard loop states: unbiased arm
                // choice draws on the runtime's RNG, which is entropy no seed
                // replays. It is also the right priority — a shutdown request
                // outranks one more connection.
                biased;

                _ = tokio::signal::ctrl_c() => break,

                accepted = self.listener.accept() => {
                    // A failed accept is per-connection — the peer vanished
                    // between the SYN and the accept, or the process is out of
                    // descriptors. Neither is a reason to stop serving the
                    // connections that did get in.
                    let Ok((stream, _peer)) = accepted else { continue };
                    match Arc::clone(&clients).try_acquire_owned() {
                        Ok(permit) => {
                            let router = self.pool.clone();
                            tokio::spawn(async move {
                                #[allow(
                                    clippy::large_futures,
                                    reason = "the 16 KiB is the connection's read chunk, and \
                                              this future is the whole body of a spawned task: \
                                              tokio already heap-allocates it, so boxing would \
                                              buy a second allocation and a pointer chase per \
                                              read without removing a byte"
                                )]
                                serve_connection(stream, router).await;
                                drop(permit);
                            });
                        }
                        Err(_) => refuse(stream).await,
                    }
                }
            }
        }
    }
}

/// Tells a peer the server is full, then drops the connection.
///
/// Both results are discarded: the peer is being disconnected either way, and
/// there is nowhere left to report a write failure to.
async fn refuse(mut stream: TcpStream) {
    let mut out = Vec::new();
    encode(&Frame::Error(MAX_CLIENTS_REACHED.to_owned()), &mut out);
    let _ = stream.write_all(&out).await;
    let _ = stream.flush().await;
}

#[cfg(test)]
mod tests {
    use super::{Config, DEFAULT_BIND, MAX_CLIENTS_REACHED, USAGE};

    #[test]
    fn args_parse_defaults_and_overrides() {
        let cfg = Config::from_args(std::iter::empty()).unwrap();
        assert_eq!(cfg.bind.to_string(), "127.0.0.1:6379");
        assert_eq!(cfg.max_clients, 10_000);

        let cfg = Config::from_args(
            ["--bind", "0.0.0.0:7000", "--max-clients", "64"]
                .iter()
                .map(ToString::to_string),
        )
        .unwrap();
        assert_eq!(cfg.bind.to_string(), "0.0.0.0:7000");
        assert_eq!(cfg.max_clients, 64);
    }

    #[test]
    fn args_reject_garbage() {
        for bad in [
            &["--bind"][..],
            &["--bind", "nonsense"],
            &["--max-clients", "-1"],
            &["--max-clients"],
            &["--max-clients", "0"],
            &["--wat"],
            // A value with no flag in front of it: silently ignoring it would
            // mean a mistyped `--bind` starts a server on the default port.
            &["6379"],
        ] {
            let refusal = Config::from_args(bad.iter().map(ToString::to_string))
                .expect_err(&format!("{bad:?} was accepted"));
            assert!(refusal.contains(USAGE), "{bad:?}: no usage text: {refusal}");
        }
    }

    /// The default is loopback, and that is a security property rather than a
    /// preference: this server has no authentication of any kind.
    #[test]
    fn the_default_bind_is_not_reachable_from_a_network() {
        assert!(DEFAULT_BIND.ip().is_loopback());
    }

    /// The refusal text is what a client matches on, so it is pinned here
    /// rather than left to whoever edits the constant next.
    #[test]
    fn the_refusal_is_byte_exact_to_redis() {
        assert_eq!(MAX_CLIENTS_REACHED, "ERR max number of clients reached");
    }
}
