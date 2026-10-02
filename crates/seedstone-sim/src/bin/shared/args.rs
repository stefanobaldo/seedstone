//! Argument parsing shared by `sweep` and `replay`.
//!
//! Hand-parsed on purpose: a CLI crate would be the only dependency these two
//! binaries have that is not the system under test, and the whole surface is
//! a handful of flags.
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

use seedstone_core::shard::SyncPolicy;
use seedstone_sim::{FsyncDraw, Plant, SimConfig};

/// Every option either binary accepts.
#[derive(Clone)]
pub struct Args {
    /// `--sim-seed S` — the single seed to replay. `replay` only.
    pub sim_seed: Option<u64>,
    /// `--seeds N` — how many sim seeds the sweep runs, counting from
    /// `--seed-start`. `sweep` only.
    pub seeds: Option<u64>,
    /// `--seed-start S` — the first sim seed of the range. `sweep` only; the
    /// nightly window is this flag.
    pub seed_start: Option<u64>,
    /// `--workload-seed W` — pinned across a sweep so a differing trace means
    /// a differing schedule and nothing else.
    pub workload_seed: u64,
    /// Which shape the run takes: `--mini`, `--eviction`, `--hostile`, or
    /// none of them for the standard one.
    ///
    /// One value rather than a flag each, and that is the refusal below made
    /// structural: every shape but the standard one changes what a run is
    /// held to, and two asked for at once would leave two shapes claiming to
    /// be the one that ran.
    pub shape: Shape,
    /// `--plant NAME` — serve the workload through one deliberate defect.
    ///
    /// Named rather than boolean: there are three of them now, one per
    /// invariant, and a flag that silently selects the first would be a way
    /// to run a self-test that proves nothing about the counter it was
    /// pointed at.
    pub plant: Option<Plant>,
    /// `--fsync NAME` — run under this durability policy rather than the
    /// one the seed draws: how a failure found under one policy is replayed
    /// under another.
    pub fsync: Option<SyncPolicy>,
    /// `--hashes` — print every seed's trace hash, not just the failures'.
    /// `sweep` only.
    ///
    /// Off by default because a passing sweep's output is meant to be read by
    /// a person. It is turned on when the hashes are the product: a sweep's
    /// in-process hashes only mean something once a fresh process has been
    /// asked to reproduce them, and there is nothing to compare against
    /// without this.
    pub hashes: bool,
    /// `--workers K` — how many OS threads run seeds at once. `sweep` only.
    ///
    /// Absent means one thread per available core. Nothing about the sweep's
    /// output depends on it: seeds are reported in ascending order whatever
    /// the count, so this buys wall clock and nothing else.
    pub workers: Option<usize>,
}

/// A run's shape, as the command line names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// No shape flag: the swept shape.
    Standard,
    /// `--mini` — the small configuration, for tests and quick checks.
    Mini,
    /// `--eviction` — the shape with a ceiling, where the memory invariants
    /// decide: the ceiling changes what the plain model is allowed to excuse.
    Eviction,
    /// `--hostile` — the shape whose disk tears, fails and lies, with the
    /// node crashed under load: where a lost durable write is excused if the
    /// node reported it, and where the recovery plants are caught.
    Hostile,
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
            seed_start: None,
            workload_seed: DEFAULT_WORKLOAD_SEED,
            shape: Shape::Standard,
            plant: None,
            fsync: None,
            hashes: false,
            workers: None,
        };

        let mut argv = argv.into_iter();
        while let Some(arg) = argv.next() {
            match arg.as_str() {
                "--sim-seed" => parsed.sim_seed = Some(number(&arg, argv.next())?),
                "--seeds" => parsed.seeds = Some(number(&arg, argv.next())?),
                "--seed-start" => parsed.seed_start = Some(number(&arg, argv.next())?),
                "--workload-seed" => parsed.workload_seed = number(&arg, argv.next())?,
                "--mini" => parsed.choose(Shape::Mini)?,
                "--eviction" => parsed.choose(Shape::Eviction)?,
                "--hostile" => parsed.choose(Shape::Hostile)?,
                "--plant" => parsed.plant = Some(plant(argv.next())?),
                "--fsync" => parsed.fsync = Some(fsync(argv.next())?),
                "--hashes" => parsed.hashes = true,
                "--workers" => parsed.workers = Some(workers(&arg, argv.next())?),
                other => return Err(format!("unknown argument `{other}`")),
            }
        }

        Ok(parsed)
    }

    /// Takes `shape` as the run's, refusing a second one.
    fn choose(&mut self, shape: Shape) -> Result<(), String> {
        if self.shape != Shape::Standard {
            return Err(
                "one shape per sweep: --mini, --eviction and --hostile are three".to_owned(),
            );
        }
        self.shape = shape;
        Ok(())
    }

    /// The configuration these arguments describe, at `sim_seed`.
    pub const fn config(&self, sim_seed: u64) -> SimConfig {
        let mut cfg = match self.shape {
            Shape::Mini => SimConfig::mini(self.workload_seed, sim_seed),
            Shape::Eviction => SimConfig::eviction(self.workload_seed, sim_seed),
            Shape::Hostile => SimConfig::hostile(self.workload_seed, sim_seed),
            Shape::Standard => SimConfig::standard(self.workload_seed, sim_seed),
        };
        cfg.planted = self.plant;
        if let Some(policy) = self.fsync {
            cfg.fsync = FsyncDraw::Fixed(policy);
        }
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

/// Reads the policy name that follows `--fsync`: one the server's own
/// flag takes.
fn fsync(value: Option<String>) -> Result<SyncPolicy, String> {
    let value = value.ok_or("--fsync needs one of: always, interval, never")?;
    SyncPolicy::from_name(&value).ok_or_else(|| {
        format!("--fsync does not know `{value}`; it takes always, interval or never")
    })
}

/// Reads the thread count that follows `--workers`.
///
/// A count this machine cannot even index is refused rather than clamped:
/// running a different configuration than the one asked for is the failure
/// this parser exists to rule out, and that holds for the worker count as
/// much as for the seeds.
fn workers(flag: &str, value: Option<String>) -> Result<usize, String> {
    let count = number(flag, value)?;
    usize::try_from(count)
        .map_err(|_| format!("{flag} needs a count this machine can hold, got `{count}`"))
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

    /// `--fsync` fixes the policy a seed would otherwise draw, and a name
    /// the server does not take is refused.
    #[test]
    fn fsync_fixes_the_policy_by_its_name() {
        let args = parse(&["--sim-seed", "3", "--fsync", "always"]).expect("a name parses");
        assert_eq!(args.config(3).policy(), SyncPolicy::ALWAYS);
        assert_eq!(args.sim_seed, Some(3));
        assert!(parse(&["--sim-seed", "3", "--fsync", "sometimes"]).is_err());
        assert!(parse(&["--sim-seed", "3", "--fsync"]).is_err());
    }

    /// A plant nobody planted is worse than no plant: the run would be honest
    /// and the self-test would read as a pass.
    #[test]
    fn an_unknown_or_missing_plant_is_refused() {
        assert!(parse(&["--seeds", "10", "--plant", "nonesuch"]).is_err());
        assert!(parse(&["--seeds", "10", "--plant"]).is_err());
    }

    /// A shape is a choice, and two shapes asked for at once is a sweep whose
    /// summary line would name one while the other decided what the model
    /// excused.
    #[test]
    fn the_two_shape_flags_are_refused_together() {
        let args = parse(&["--seeds", "10", "--eviction"]).expect("--eviction parses");
        assert_eq!(args.shape, Shape::Eviction);
        assert_eq!(
            args.seeds,
            Some(10),
            "the new flag must not swallow its neighbour"
        );
        assert!(parse(&["--seeds", "10", "--mini", "--eviction"]).is_err());
    }

    /// The hostile disk is a shape of its own, for the reason `--eviction`
    /// is: it changes what a run is held to.
    #[test]
    fn hostile_is_a_shape_and_refused_beside_the_others() {
        let args = parse(&["--seeds", "10", "--hostile"]).expect("--hostile parses");
        assert_eq!(args.shape, Shape::Hostile);
        assert_eq!(
            args.seeds,
            Some(10),
            "the new flag must not swallow its neighbour"
        );
        assert!(parse(&["--seeds", "10", "--mini", "--hostile"]).is_err());
        assert!(parse(&["--seeds", "10", "--eviction", "--hostile"]).is_err());
    }

    #[test]
    fn workers_parses_and_defaults_to_none() {
        let args = parse(&["--seeds", "10"]).expect("a plain sweep parses");
        assert_eq!(
            args.workers, None,
            "absent means the machine decides, not one thread"
        );
        let args = parse(&["--seeds", "10", "--workers", "4"]).expect("--workers parses");
        assert_eq!(args.workers, Some(4));
        assert_eq!(
            args.seeds,
            Some(10),
            "the new flag must not swallow its neighbour"
        );
    }

    #[test]
    fn seed_start_parses_and_defaults_to_none() {
        let args = parse(&["--seeds", "10"]).expect("a plain sweep parses");
        assert_eq!(args.seed_start, None);
        let args =
            parse(&["--seeds", "10", "--seed-start", "2250001"]).expect("--seed-start parses");
        assert_eq!(args.seed_start, Some(2_250_001));
        assert_eq!(
            args.seeds,
            Some(10),
            "the new flag must not swallow its neighbour"
        );
    }
}
