//! Argument parsing shared by `sweep` and `replay`.
//!
//! Hand-parsed on purpose: a CLI crate would be the only dependency these two
//! binaries have that is not the system under test, and the whole surface is
//! five flags.
//!
//! This file lives under `src/bin/shared/` rather than beside the binaries.
//! Cargo turns every `src/bin/*.rs` into a binary target, and a directory
//! becomes one only if it holds a `main.rs` — so a subdirectory without one is
//! the way to share code between binaries without accidentally shipping a
//! third.
//!
//! Both binaries parse the same union of options and each rejects the ones
//! that are not its own, so a typo like `sweep --sim-seed 3` fails loudly
//! rather than sweeping some default.

use seedstone_sim::SimConfig;

/// Every option either binary accepts.
pub struct Args {
    /// `--sim-seed S` — the single seed to replay. `replay` only.
    pub sim_seed: Option<u64>,
    /// `--seeds N` — sweep `sim_seed` over `1..=N`. `sweep` only.
    pub seeds: Option<u64>,
    /// `--workload-seed W` — pinned across a sweep so a differing trace means
    /// a differing schedule and nothing else.
    pub workload_seed: u64,
    /// `--mini` — the small configuration, for tests and quick checks.
    pub mini: bool,
    /// `--plant` — route `INCRBY` through the deliberately racy router.
    pub plant: bool,
}

/// The workload seed a sweep pins when none is given.
const DEFAULT_WORKLOAD_SEED: u64 = 1;

impl Args {
    /// Parses the process arguments.
    ///
    /// Returns the message to print on anything unrecognised, missing or
    /// unparseable — an unknown flag is an error rather than something to
    /// ignore, since silently running a different configuration than the one
    /// asked for is exactly the failure this harness exists to rule out.
    pub fn from_env() -> Result<Args, String> {
        let mut args = Args {
            sim_seed: None,
            seeds: None,
            workload_seed: DEFAULT_WORKLOAD_SEED,
            mini: false,
            plant: false,
        };

        let mut argv = std::env::args().skip(1);
        while let Some(arg) = argv.next() {
            match arg.as_str() {
                "--sim-seed" => args.sim_seed = Some(number(&arg, argv.next())?),
                "--seeds" => args.seeds = Some(number(&arg, argv.next())?),
                "--workload-seed" => args.workload_seed = number(&arg, argv.next())?,
                "--mini" => args.mini = true,
                "--plant" => args.plant = true,
                other => return Err(format!("unknown argument `{other}`")),
            }
        }

        Ok(args)
    }

    /// The configuration these arguments describe, at `sim_seed`.
    pub fn config(&self, sim_seed: u64) -> SimConfig {
        let mut cfg = if self.mini {
            SimConfig::mini(self.workload_seed, sim_seed)
        } else {
            SimConfig::standard(self.workload_seed, sim_seed)
        };
        cfg.planted = self.plant;
        cfg
    }
}

/// Reads the value that follows a flag.
fn number(flag: &str, value: Option<String>) -> Result<u64, String> {
    let value = value.ok_or_else(|| format!("{flag} needs a value"))?;
    value
        .parse()
        .map_err(|_| format!("{flag} needs a non-negative integer, got `{value}`"))
}
