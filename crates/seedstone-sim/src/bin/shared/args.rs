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

use seedstone_sim::{Plant, SimConfig};

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
    /// `--plant NAME` — serve the workload through one deliberate defect.
    ///
    /// Named rather than boolean: there are three of them now, one per
    /// invariant, and a flag that silently selects the first would be a way
    /// to run a self-test that proves nothing about the counter it was
    /// pointed at.
    pub plant: Option<Plant>,
    /// `--hashes` — print every seed's trace hash, not just the failures'.
    /// `sweep` only.
    ///
    /// Off by default because a passing sweep's output is meant to be read by
    /// a person. It is turned on when the hashes are the product: a sweep's
    /// in-process hashes only mean something once a fresh process has been
    /// asked to reproduce them, and there is nothing to compare against
    /// without this.
    pub hashes: bool,
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
    pub fn from_env() -> Result<Self, String> {
        Self::parse(std::env::args().skip(1))
    }

    /// The parser itself, over any sequence of arguments.
    ///
    /// Split from [`Self::from_env`] so the flags can be tested without a
    /// process to hang them on: a parser that is only reachable through
    /// `std::env` is a parser that gets verified by running the binary and
    /// reading its output.
    pub fn parse<I: IntoIterator<Item = String>>(argv: I) -> Result<Self, String> {
        let mut parsed = Self {
            sim_seed: None,
            seeds: None,
            workload_seed: DEFAULT_WORKLOAD_SEED,
            mini: false,
            plant: None,
            hashes: false,
        };

        let mut argv = argv.into_iter();
        while let Some(arg) = argv.next() {
            match arg.as_str() {
                "--sim-seed" => parsed.sim_seed = Some(number(&arg, argv.next())?),
                "--seeds" => parsed.seeds = Some(number(&arg, argv.next())?),
                "--workload-seed" => parsed.workload_seed = number(&arg, argv.next())?,
                "--mini" => parsed.mini = true,
                "--plant" => parsed.plant = Some(plant(argv.next())?),
                "--hashes" => parsed.hashes = true,
                other => return Err(format!("unknown argument `{other}`")),
            }
        }

        Ok(parsed)
    }

    /// The configuration these arguments describe, at `sim_seed`.
    pub const fn config(&self, sim_seed: u64) -> SimConfig {
        let mut cfg = if self.mini {
            SimConfig::mini(self.workload_seed, sim_seed)
        } else {
            SimConfig::standard(self.workload_seed, sim_seed)
        };
        cfg.planted = self.plant;
        cfg
    }
}

/// Reads the plant name that follows `--plant`.
///
/// The message lists every plant there is rather than saying the name was
/// wrong: this flag is typed by someone reproducing a failure, and the set is
/// short enough to print.
fn plant(value: Option<String>) -> Result<Plant, String> {
    let names = || {
        Plant::ALL
            .iter()
            .map(|plant| plant.name())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let value = value.ok_or_else(|| format!("--plant needs one of: {}", names()))?;
    Plant::from_name(&value).ok_or_else(|| {
        format!(
            "--plant does not know `{value}`; it takes one of: {}",
            names()
        )
    })
}

/// Reads the value that follows a flag.
fn number(flag: &str, value: Option<String>) -> Result<u64, String> {
    let value = value.ok_or_else(|| format!("{flag} needs a value"))?;
    value
        .parse()
        .map_err(|_| format!("{flag} needs a non-negative integer, got `{value}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(argv: &[&str]) -> Result<Args, String> {
        Args::parse(argv.iter().map(|arg| (*arg).to_string()))
    }

    #[test]
    fn hashes_is_off_unless_it_is_asked_for() {
        let args = parse(&["--seeds", "10"]).expect("a plain sweep parses");
        assert!(!args.hashes, "printing every hash is not the default");
    }

    #[test]
    fn hashes_parses_as_a_flag_of_its_own() {
        let args = parse(&["--seeds", "10", "--hashes"]).expect("--hashes parses");
        assert!(args.hashes);
        // The flag must not swallow the value of its neighbour, which is what
        // a hand-written parser gets wrong when a boolean is added beside the
        // options that do take one.
        assert_eq!(args.seeds, Some(10));
    }

    /// A near-miss must not be silently ignored: the whole reason this parser
    /// refuses unknown flags is that running a different configuration than the
    /// one asked for is the failure the harness exists to rule out.
    #[test]
    fn a_misspelt_hashes_is_refused_rather_than_dropped() {
        assert!(parse(&["--seeds", "10", "--hash"]).is_err());
    }

    /// `--plant` was a boolean before there were three plants, and a boolean
    /// that grows a value is how a hand-written parser starts swallowing its
    /// neighbour's argument.
    #[test]
    fn plant_takes_a_name() {
        let args = parse(&["--seeds", "10", "--plant", "serve-expired"]).expect("a name parses");
        assert_eq!(args.plant, Some(Plant::ServeExpired));
        assert_eq!(args.seeds, Some(10));
    }

    /// A plant nobody planted is worse than no plant: the run would be honest
    /// and the self-test would read as a pass.
    #[test]
    fn an_unknown_or_missing_plant_is_refused() {
        assert!(parse(&["--seeds", "10", "--plant", "nonesuch"]).is_err());
        assert!(parse(&["--seeds", "10", "--plant"]).is_err());
    }
}
