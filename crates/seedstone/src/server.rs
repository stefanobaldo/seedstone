//! The TCP edge: a command line in, a listening socket out, connections
//! served until the process is asked to stop.
//!
//! Everything here is what the deterministic core deliberately does not have —
//! a real socket, a signal handler, a bound on how many peers may be attached
//! at once. The connection layer is reached through [`seedstone_service::serve_connection`],
//! which is generic over its transport, so the bytes a [`tokio::net::TcpStream`]
//! carries take exactly the path the simulator's virtual ones do.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use seedstone_core::dict::DictSeed;
use seedstone_core::memory::{EvictionMode, MemoryLimit, parse_bytes};
use seedstone_core::shard::{NoTrace, ShardPool};
use seedstone_resp::{Frame, encode};
use seedstone_service::{NodeInfo, Secret, serve_connection};
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

/// How many executor tasks host those shards.
///
/// The shard count is part of the placement and cannot move; this one is a
/// property of the machine, so it is read from it. Asking here — the
/// composition root, where the worker count is already decided — keeps the
/// two decisions side by side, and keeps the core free of any question about
/// the host it happens to be running on.
///
/// A host that will not say how much parallelism it has gets one executor:
/// the shape still works, and a node that refuses to start over this would be
/// worse than a node that runs conservatively.
fn executors() -> u16 {
    let available = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    u16::try_from(available)
        .unwrap_or(u16::MAX)
        .clamp(1, SHARDS)
}

/// What a peer is told when the connection limit is already spent.
///
/// Byte-exact to Redis: clients match on this text.
pub const MAX_CLIENTS_REACHED: &str = "ERR max number of clients reached";

/// The usage text, printed on any argument this binary does not understand.
const USAGE: &str = "usage: seedstone [--bind ADDR:PORT] [--max-clients N] [--maxmemory SIZE] \
                     [--maxmemory-policy allkeys-lru|noeviction] [--requirepass-file PATH] \
                     [--no-auth]\nenv: SEEDSTONE_REQUIREPASS";

/// The environment variable the password may arrive in instead of a file.
///
/// A path and a variable, and never an argument: a command line is readable by
/// every other process on the host, and a password that has to be typed there
/// is a password that ends up in a shell history and a process listing. The
/// file is the form an orchestrator mounts a secret as; the variable is the
/// form it injects one as.
pub const PASSWORD_ENV: &str = "SEEDSTONE_REQUIREPASS";

/// What the binary was asked to do.
///
/// Not `Copy` and not comparable, because [`password`](Self::password) is
/// neither: a secret that can be copied implicitly is a secret with more
/// copies than anyone counted, and one with an `==` is one that can be
/// compared in variable time by any caller. `Debug` is safe — the secret
/// redacts itself.
#[derive(Debug, Clone)]
pub struct Config {
    /// The address to listen on.
    pub bind: SocketAddr,
    /// How many connections may be served at once.
    pub max_clients: usize,
    /// The ceiling the keyspace is held under, and what happens at it.
    pub limit: MemoryLimit,
    /// The password every connection must present, or `None` on a node that
    /// asks for none.
    pub password: Option<Secret>,
    /// Whether running with no password was asked for in so many words.
    ///
    /// Kept beside the absent password rather than folded into it, because
    /// the two are different configurations: `None` with this unset is a
    /// default that only loopback tolerates, and `None` with it set is a
    /// decision an operator wrote down.
    pub no_auth: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: DEFAULT_BIND,
            max_clients: DEFAULT_MAX_CLIENTS,
            limit: MemoryLimit::default(),
            password: None,
            no_auth: false,
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
    /// mistake, not a configuration, and `--maxmemory 0` for the same reason
    /// in the other direction — Redis spells "no ceiling" as the flag's
    /// absence, so a zero here is a server that may store nothing rather than
    /// a server with no limit.
    ///
    /// `--maxmemory-policy` without `--maxmemory` is refused as well. A
    /// policy names what to do at a ceiling, and accepting one with no
    /// ceiling to reach would be accepting a flag that does nothing while
    /// reading as though it does.
    pub fn from_args(args: impl Iterator<Item = String>) -> Result<Self, String> {
        Self::from_args_and_env(args, |_| None)
    }

    /// Parses the command line against an environment.
    ///
    /// The environment arrives as a function rather than being read here for
    /// the reason every other input to this workspace does: a process-global
    /// read is not a parameter, and a rule about where a password may come
    /// from is exactly the rule that has to be testable without one.
    ///
    /// # The password, and the three ways of getting it wrong
    ///
    /// It comes from a file or from [`PASSWORD_ENV`], never from an argument
    /// — see that constant. Both at once is refused rather than resolved by
    /// precedence: an operator who set two has one in mind, and picking for
    /// them is picking wrong half the time. `--no-auth` beside either is
    /// refused for the same reason, being the flat contradiction of it. An
    /// empty password is refused wherever it comes from, because a file that
    /// failed to be written and a variable that expanded to nothing both look
    /// exactly like this, and neither should start a server that answers
    /// `AUTH ""`.
    ///
    /// # The bind that must be defended
    ///
    /// A bind outside loopback with no password is refused unless `--no-auth`
    /// says so deliberately. [`DEFAULT_BIND`] has always kept an unconfigured
    /// node off the network; this is the other half of the same property, for
    /// the node whose operator did configure an address.
    ///
    /// # Errors
    ///
    /// As [`from_args`](Self::from_args), plus the four refusals above.
    pub fn from_args_and_env(
        args: impl Iterator<Item = String>,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, String> {
        let mut cfg = Self::default();
        let mut args = args;
        // Held aside like `policy` below, and for the same reason: whether a
        // password is acceptable depends on flags that may come after it.
        let mut password_file: Option<String> = None;
        // Held aside rather than written straight into `cfg`, because whether
        // a policy is acceptable depends on a flag that may come after it.
        let mut policy = None;
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
                "--maxmemory" => {
                    let value = value_for(&flag, &mut args)?;
                    cfg.limit.ceiling = match parse_bytes(&value) {
                        Some(bytes) if bytes > 0 => Some(bytes),
                        _ => {
                            return Err(refused(&format!(
                                "--maxmemory: not a positive byte size: {value}"
                            )));
                        }
                    };
                }
                "--maxmemory-policy" => {
                    let value = value_for(&flag, &mut args)?;
                    policy = Some(EvictionMode::from_name(&value).ok_or_else(|| {
                        refused(&format!("--maxmemory-policy: not a policy: {value}"))
                    })?);
                }
                "--requirepass-file" => password_file = Some(value_for(&flag, &mut args)?),
                "--no-auth" => cfg.no_auth = true,
                other => return Err(refused(&format!("unknown argument: {other}"))),
            }
        }
        cfg.password = password_from(password_file.as_deref(), &env)?;
        if cfg.no_auth && cfg.password.is_some() {
            return Err(refused(
                "--no-auth: a password was configured as well; choose one",
            ));
        }
        if !cfg.bind.ip().is_loopback() && cfg.password.is_none() && !cfg.no_auth {
            return Err(refused(
                "a bind outside loopback needs a password: give --requirepass-file PATH or set \
                 SEEDSTONE_REQUIREPASS, or pass --no-auth to run open on purpose",
            ));
        }
        // After the loop, because the two flags may arrive in either order.
        match (policy, cfg.limit.ceiling) {
            (Some(_), None) => {
                return Err(refused(
                    "--maxmemory-policy: names what to do at a ceiling, but no --maxmemory was given",
                ));
            }
            (Some(mode), Some(_)) => cfg.limit.mode = mode,
            (None, _) => {}
        }
        Ok(cfg)
    }
}

/// The configured password, from the file or the environment.
///
/// Reading the file here rather than in the flag loop is what lets "both at
/// once" be one refusal instead of two half-rules: the loop records what was
/// asked for, and this decides whether the answer is a configuration.
fn password_from(
    file: Option<&str>,
    env: &impl Fn(&str) -> Option<String>,
) -> Result<Option<Secret>, String> {
    let from_env = env(PASSWORD_ENV);
    let Some(path) = file else {
        return match from_env {
            Some(value) => Ok(Some(non_empty(value.into_bytes(), PASSWORD_ENV)?)),
            None => Ok(None),
        };
    };
    if from_env.is_some() {
        return Err(refused(
            "--requirepass-file and SEEDSTONE_REQUIREPASS are both set: choose one",
        ));
    }
    let mut bytes = std::fs::read(path)
        .map_err(|error| refused(&format!("--requirepass-file: {path}: {error}")))?;
    // One trailing newline, in either spelling: every editor and every
    // `printf` that writes a secret to a file leaves one, and a password with
    // an invisible byte on the end is a support ticket nobody solves. More
    // than one is left alone — that is a file whose content was meant.
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    Ok(Some(non_empty(bytes, "--requirepass-file")?))
}

/// A password with bytes in it, or the refusal that says where the empty one
/// came from.
fn non_empty(bytes: Vec<u8>, source: &str) -> Result<Secret, String> {
    if bytes.is_empty() {
        return Err(refused(&format!("{source}: the password is empty")));
    }
    Ok(Secret::new(bytes))
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
    password: Option<Secret>,
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
            pool: ShardPool::spawn_limited(SHARDS, executors(), seed, NoTrace, cfg.limit),
            password: cfg.password,
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
        // Assembled once, here, because this is the only place that knows any
        // of it: the port the kernel actually gave out, the moment the node
        // began serving, and the count this loop is about to start
        // maintaining. Every connection gets a clone.
        let node = NodeInfo {
            version: env!("CARGO_PKG_VERSION"),
            tcp_port: self.local_addr.port(),
            started: tokio::time::Instant::now(),
            connected: Arc::new(AtomicU64::new(0)),
            now_unix_millis: wall_clock,
            memory: self.pool.memory(),
            limit: self.pool.limit(),
            password: self.password.clone(),
        };
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
                            let node = node.clone();
                            tokio::spawn(async move {
                                // Held for exactly as long as the connection
                                // is, the same way the permit beside it is.
                                let _attached = Attached::count(&node.connected);
                                serve_connection(stream, router, node).await;
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

/// Counts one attached connection for as long as it is held.
///
/// A guard rather than a decrement written after the `await`, and for the same
/// reason the semaphore permit next to it is one: a connection ends by `QUIT`,
/// by a protocol error, by the peer vanishing, by the transport failing, or by
/// the task being dropped underneath it, and a line placed after the call is
/// only reached on the subset of those that return. `connected_clients` is a
/// gauge, so an increment that is not always paired does not lose a sample —
/// it makes every later reading wrong, for the lifetime of the process.
struct Attached(Arc<AtomicU64>);

impl Attached {
    /// Counts a connection, and keeps counting it until the guard is dropped.
    fn count(connected: &Arc<AtomicU64>) -> Self {
        connected.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(connected))
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
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

/// The real wall clock, in Unix milliseconds.
///
/// One more thing this crate exists to supply and the deterministic core
/// deliberately does not have, alongside the socket, the signal handler and
/// the entropy the composition root draws. A node reached over TCP is a node
/// whose clients say `EXAT`, and an absolute deadline can only be compared to
/// the clock those clients set their own watches by.
///
/// A reading before 1970 is impossible on a machine whose clock is set at all,
/// and the alternative to answering zero for one is a panic on a command a
/// peer sent — so the epoch stands in, and every deadline a client names is
/// then in the future, which is the conservative direction.
#[allow(
    clippy::disallowed_methods,
    reason = "the prohibition keeps the wall clock out of a simulated run; \
              this is the real node's clock, handed to the service layer as a \
              dependency so a simulated one is handed its own and stays \
              replayable"
)]
fn wall_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::{
        Config, DEFAULT_BIND, EvictionMode, MAX_CLIENTS_REACHED, MemoryLimit, PASSWORD_ENV, SHARDS,
        USAGE, executors,
    };

    /// Whatever this host answers, the count has to be one the pool accepts:
    /// at least one executor, never more than there are shards to host.
    #[test]
    fn the_executor_count_is_one_the_pool_can_be_spawned_with() {
        let executors = executors();
        assert!(executors >= 1, "no executor to host a shard");
        assert!(executors <= SHARDS, "more executors than shards");
    }

    #[test]
    fn args_parse_defaults_and_overrides() {
        let cfg = Config::from_args(std::iter::empty()).unwrap();
        assert_eq!(cfg.bind.to_string(), "127.0.0.1:6379");
        assert_eq!(cfg.max_clients, 10_000);

        // `--no-auth`, because an open bind with no password no longer
        // parses: see `an_open_bind_needs_a_password_or_an_explicit_no_auth`.
        let cfg = Config::from_args(
            ["--bind", "0.0.0.0:7000", "--max-clients", "64", "--no-auth"]
                .iter()
                .map(ToString::to_string),
        )
        .unwrap();
        assert_eq!(cfg.bind.to_string(), "0.0.0.0:7000");
        assert_eq!(cfg.max_clients, 64);
        assert_eq!(cfg.limit, MemoryLimit::default());

        let cfg = Config::from_args(
            ["--maxmemory", "64mb", "--maxmemory-policy", "noeviction"]
                .iter()
                .map(ToString::to_string),
        )
        .unwrap();
        assert_eq!(
            cfg.limit,
            MemoryLimit {
                ceiling: Some(64 << 20),
                mode: EvictionMode::NoEviction,
            }
        );

        // The policy may be named before the ceiling it applies to: the rule
        // is checked after the whole line has been read, not as it is read.
        let cfg = Config::from_args(
            ["--maxmemory-policy", "allkeys-lru", "--maxmemory", "1k"]
                .iter()
                .map(ToString::to_string),
        )
        .unwrap();
        assert_eq!(
            cfg.limit,
            MemoryLimit {
                ceiling: Some(1000),
                mode: EvictionMode::AllKeysLru,
            }
        );
    }

    #[test]
    fn args_reject_garbage() {
        for bad in [
            &["--bind"][..],
            &["--bind", "nonsense"],
            &["--max-clients", "-1"],
            &["--max-clients"],
            &["--max-clients", "0"],
            &["--maxmemory", "lots"],
            &["--maxmemory"],
            // A zero ceiling is a server that may store nothing.
            &["--maxmemory", "0"],
            &["--maxmemory-policy", "volatile-lru"],
            // A policy this server does have, but with no ceiling to reach.
            &["--maxmemory-policy", "allkeys-lru"],
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
    /// preference — now with two halves. An unconfigured node stays off the
    /// network; a node whose operator configured an address off it must say
    /// what defends it, which is the refusal below.
    #[test]
    fn the_default_bind_is_not_reachable_from_a_network() {
        assert!(DEFAULT_BIND.ip().is_loopback());
    }

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| (*v).to_owned())
        }
    }

    /// A directory of this test's own, named after the test and the process,
    /// so that a suite running in parallel does not share a password file.
    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("seedstone-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn an_open_bind_needs_a_password_or_an_explicit_no_auth() {
        let open = ["--bind", "0.0.0.0:7000"].iter().map(ToString::to_string);
        let refusal = Config::from_args_and_env(open.clone(), env(&[]))
            .expect_err("open bind, no password, accepted");
        assert!(
            refusal.contains("--requirepass-file") && refusal.contains("--no-auth"),
            "{refusal}"
        );

        let cfg = Config::from_args_and_env(open.clone().chain(["--no-auth".to_owned()]), env(&[]))
            .unwrap();
        assert!(cfg.password.is_none() && cfg.no_auth);

        let cfg = Config::from_args_and_env(open, env(&[(PASSWORD_ENV, "pw")])).unwrap();
        assert!(cfg.password.as_ref().is_some_and(|s| s.matches(b"pw")));
    }

    #[test]
    fn the_password_comes_from_a_file_with_one_trailing_newline_stripped() {
        let dir = scratch("pw");
        let path = dir.join("password");
        std::fs::write(&path, "pw\n").unwrap();
        let args = ["--requirepass-file", path.to_str().unwrap()]
            .into_iter()
            .map(ToString::to_string);
        let cfg = Config::from_args_and_env(args, env(&[])).unwrap();
        assert!(cfg.password.unwrap().matches(b"pw"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn contradictory_secrets_are_refused() {
        let dir = scratch("pw2");
        let path = dir.join("password");
        std::fs::write(&path, "pw").unwrap();
        // Collected rather than borrowed: an iterator over an array built
        // inside the closure would outlive the array.
        let file_args = || {
            vec![
                "--requirepass-file".to_owned(),
                path.to_str().unwrap().to_owned(),
            ]
            .into_iter()
        };
        assert!(
            Config::from_args_and_env(file_args(), env(&[(PASSWORD_ENV, "other")])).is_err(),
            "file and env together"
        );
        assert!(
            Config::from_args_and_env(file_args().chain(["--no-auth".to_owned()]), env(&[]))
                .is_err(),
            "--no-auth with a password"
        );
        assert!(
            Config::from_args_and_env(
                ["--requirepass-file", "/nonexistent/p"]
                    .iter()
                    .map(ToString::to_string),
                env(&[])
            )
            .is_err(),
            "unreadable file"
        );
        std::fs::write(&path, "").unwrap();
        assert!(
            Config::from_args_and_env(file_args(), env(&[])).is_err(),
            "an empty password is no password"
        );
        assert!(
            Config::from_args_and_env(std::iter::empty(), env(&[(PASSWORD_ENV, "")])).is_err(),
            "an empty password is no password, from the environment either"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The refusal text is what a client matches on, so it is pinned here
    /// rather than left to whoever edits the constant next.
    #[test]
    fn the_refusal_is_byte_exact_to_redis() {
        assert_eq!(MAX_CLIENTS_REACHED, "ERR max number of clients reached");
    }
}
