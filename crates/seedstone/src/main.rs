//! The composition root: parse the command line, draw a seed, bind, serve.
//!
//! Everything this file does is a decision the layers below refuse to make for
//! themselves — where to listen, how many peers to accept, and what the
//! keyspace hasher is seeded with. Nothing here is logic; anything that looks
//! like logic belongs in [`seedstone::server`], where it can be tested without
//! a process.

use seedstone::server::{Config, Server};
use seedstone_core::dict::DictSeed;

fn main() {
    // The one place the process environment is read. Every layer below takes
    // it as a parameter, for the reason `from_args_and_env` states.
    let cfg = match Config::from_args_and_env(std::env::args().skip(1), |name| {
        std::env::var(name).ok()
    }) {
        Ok(cfg) => cfg,
        Err(usage) => {
            eprintln!("{usage}");
            std::process::exit(2);
        }
    };

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    runtime.block_on(async {
        let server = match Server::bind(cfg, entropy_seed()).await {
            Ok(server) => server,
            Err(error) => {
                eprintln!("bind failed: {error}");
                std::process::exit(1);
            }
        };
        eprintln!(
            "seedstone {} listening on {}",
            env!("CARGO_PKG_VERSION"),
            server.local_addr()
        );
        server.run().await;
    });
}

/// Draws the node's keyspace hashing seed from the operating system.
///
/// The one place in the workspace where entropy enters. A predictable SipHash
/// seed is an invitation to collide a shard's buckets on purpose, and the
/// keyspace dict is hand-written precisely so the seed is explicit — so it is
/// drawn here, once, and injected. Everything below receives it and stays
/// reproducible from it.
#[allow(
    clippy::disallowed_methods,
    reason = "the composition root is the one place entropy may enter: a \
              predictable SipHash seed is a HashDoS invitation, and the core \
              receives the seed injected, staying deterministic"
)]
fn entropy_seed() -> DictSeed {
    DictSeed {
        k0: getrandom::u64().expect("OS entropy unavailable"),
        k1: getrandom::u64().expect("OS entropy unavailable"),
    }
}
