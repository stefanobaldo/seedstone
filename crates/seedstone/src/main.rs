//! The composition root: parse the command line, draw what only the
//! operating system can supply, bind, serve.
//!
//! Everything this file does is a decision the layers below refuse to make for
//! themselves — where to listen, how many peers to accept, and what the
//! keyspace hasher is seeded with. Nothing here is logic; anything that looks
//! like logic belongs in [`seedstone::server`], where it can be tested without
//! a process.

use seedstone::server::{Config, Server, USAGE};
use seedstone_core::dict::DictSeed;
use seedstone_service::RUN_ID_HEX;

fn main() {
    let mut args = std::env::args().skip(1).peekable();
    // Answered before anything is parsed, bound, or drawn from the operating
    // system, because both questions are about the file on disk rather than
    // about a server: whoever asks is holding an unpacked archive and wants
    // to know what it is. Only in first position — anywhere else they are
    // arguments this binary does not understand, and are refused with the
    // usage text like any other, which is the honest answer to
    // `--bind :6379 --help`.
    match args.peek().map(String::as_str) {
        Some("--version") => {
            println!("seedstone {}", env!("CARGO_PKG_VERSION"));
            return;
        }
        Some("--help") => {
            println!("{USAGE}");
            return;
        }
        _ => {}
    }
    // The one place the process environment is read. Every layer below takes
    // it as a parameter, for the reason `from_args_and_env` states.
    let cfg = match Config::from_args_and_env(args, |name| std::env::var(name).ok()) {
        Ok(cfg) => cfg,
        Err(usage) => {
            eprintln!("{usage}");
            std::process::exit(2);
        }
    };

    let Entropy { seed, run_id } = entropy();
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    runtime.block_on(async {
        let server = match Server::bind(cfg, seed, run_id).await {
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

/// Everything this process asks the operating system for entropy.
///
/// One function and one exception, because there is one reason: the
/// composition root is the only place a value may be drawn that no seed can
/// reproduce. Both of these are that, and splitting them would split the
/// exception too — two sites where the guide counts one, and the second is
/// how a third arrives unremarked.
struct Entropy {
    /// What the keyspace dict hashes under. A predictable SipHash seed is an
    /// invitation to collide a shard's buckets on purpose, and the dict is
    /// hand-written precisely so the seed is explicit — so it is drawn here,
    /// once, and injected. Everything below receives it and stays
    /// reproducible from it.
    seed: DictSeed,
    /// What this run of the process calls itself in `INFO`, forty lowercase
    /// hexadecimal characters — Redis's width.
    ///
    /// Its job is to differ between two starts of the same binary: a monitor
    /// reading counters that both begin at zero cannot otherwise tell a node
    /// that restarted from one that has been up, and a rate computed across
    /// that boundary is computed from a fall to zero.
    run_id: String,
}

#[allow(
    clippy::disallowed_methods,
    reason = "the composition root is the one place entropy may enter: a \
              predictable SipHash seed is a HashDoS invitation and two starts \
              of one binary must not report the same run id, and the layers \
              below receive both injected, staying deterministic"
)]
fn entropy() -> Entropy {
    use std::fmt::Write as _;
    let seed = DictSeed {
        k0: getrandom::u64().expect("OS entropy unavailable"),
        k1: getrandom::u64().expect("OS entropy unavailable"),
    };
    // Three draws render forty-eight characters; the tail is dropped.
    let mut run_id = String::with_capacity(48);
    for _ in 0..3 {
        let word = getrandom::u64().expect("OS entropy unavailable");
        // Writing into a `String` cannot fail.
        let _ = write!(run_id, "{word:016x}");
    }
    run_id.truncate(RUN_ID_HEX);
    Entropy { seed, run_id }
}
