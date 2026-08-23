//! `sweep` — run a contiguous range of simulator seeds against one workload.
//!
//! The workload seed is pinned and `sim_seed` varies, so a seed that fails is
//! a schedule that failed: the operations were the same in every run. Each
//! violation prints a line carrying everything needed to reproduce it, and the
//! seed on that line goes straight into `replay`.
//!
//! ```text
//! sweep --seeds N [--seed-start S] [--workload-seed W] [--mini | --eviction] [--plant NAME]
//!       [--hashes] [--workers K]
//! ```
//!
//! The range starts at seed 1 unless `--seed-start` says otherwise, which is
//! how a scheduled run sweeps a window it has not swept before rather than the
//! same opening seeds every night.
//!
//! Seeds run on `--workers` OS threads — one per available core unless asked
//! otherwise — and are reported in ascending order regardless, so the output
//! of a sweep is the same artifact at every worker count.
//!
//! `--hashes` prints every seed's trace hash, passing or not, which is what
//! turns a sweep into something a fresh process can be held against: the
//! hashes are computed in one long-lived process here, and a trace that is
//! only reproducible at the position a run happens to occupy is not
//! reproducible at all.
//!
//! Exits 1 if any seed violated any invariant.

use seedstone_sim::Plant;
use std::ops::RangeInclusive;
use std::process::ExitCode;

#[path = "shared/args.rs"]
mod args;

const USAGE: &str = "usage: sweep --seeds N [--seed-start S] [--workload-seed W] \
                     [--mini | --eviction] [--plant NAME] [--hashes] [--workers K]";

fn main() -> ExitCode {
    let args = match args::Args::from_env() {
        Ok(args) => args,
        Err(message) => return fail(&message),
    };
    // The foreign flag is diagnosed before the missing one: `sweep --sim-seed
    // 3` is someone reaching for `replay`, and saying "--seeds is required"
    // would send them the wrong way.
    if args.sim_seed.is_some() {
        return fail("--sim-seed runs one seed; sweep runs a range — see replay");
    }
    let Some(requested) = args.seeds else {
        return fail("--seeds is required");
    };
    let seed_range = match range(args.seed_start.unwrap_or(1), requested) {
        Ok(seed_range) => seed_range,
        Err(message) => return fail(&message),
    };
    let first_seed = *seed_range.start();
    let seeds = seed_range.end() - seed_range.start() + 1;
    let workers = match args.workers {
        Some(0) => return fail("--workers must be at least 1"),
        Some(k) => k,
        None => std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
    };
    let config_args = args.clone();
    let hashes = args.hashes;
    let report = seedstone_sim::sweep(
        first_seed,
        seeds,
        workers,
        move |sim_seed| config_args.config(sim_seed),
        |sim_seed, outcome| {
            if hashes {
                // One flat line per seed, printed whatever the outcome: a
                // sample of these is fed back to `replay` from a fresh
                // process, so the format is parsed by a script and must not
                // depend on whether the seed passed.
                println!("seed={} trace=0x{:016x}", sim_seed, outcome.trace_hash);
            }
            if !outcome.invariant_holds() {
                // Printed as it happens rather than collected: a sweep that is
                // cancelled — by CI, by a person — has still reported what it
                // found up to that point.
                println!(
                    "FAIL seed={} trace=0x{:016x} expected={} actual={} stale={} spurious={} \
                     plain={} walk={} breaches={} evicted={} observed={}",
                    sim_seed,
                    outcome.trace_hash,
                    outcome.expected_sum,
                    outcome.actual_sum,
                    outcome.stale_reads,
                    outcome.spurious_deaths,
                    outcome.plain_mismatches,
                    outcome.walk_mismatches,
                    outcome.ceiling_breaches,
                    outcome.evicted_keys,
                    outcome.evictions_observed
                );
            }
        },
    );

    println!(
        "swept {} seeds start={} shape={} workload_seed={} planted={} violations={}",
        seeds,
        first_seed,
        shape(&args),
        args.workload_seed,
        args.plant.map_or("none", Plant::name),
        report.violations
    );

    // What this sweep did *not* reach, named. The simulated client's contract
    // declares every form it can emit, and a shape decides which of them a
    // given sweep gets to: the quiescent walk is a test's to ask for, so a
    // gate sweeping without it does not reach the forms only that walk sends.
    // Printed rather than asserted, because the gap is a property of the
    // shape and not a defect — but printed every time, because a coverage
    // claim nobody states is the thing this contract exists to end.
    let unreached: Vec<&str> = seedstone_sim::contract::declared_forms()
        .filter(|form| !report.forms.contains(form))
        .collect();
    println!(
        "coverage {}/{} declared forms; not reached by this shape: {:?}",
        report.forms.len(),
        seedstone_sim::contract::declared_forms().count(),
        unreached
    );

    // Stderr, beside the binary's other diagnostics: stdout here is a parsed
    // artifact — `--hashes` lines are fed back into `replay` by a script, and
    // the two lines above have a fixed shape — so a warning that is not a
    // result of the sweep does not belong in it. Printed after them, because
    // what a number does not cover is only useful next to the number.
    if let Some(warning) = args
        .plant
        .and_then(|plant| unobservable_warning(plant, shape(&args)))
    {
        eprintln!("{warning}");
    }

    if report.violations == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// The seeds a sweep will walk, or the reason it cannot walk them.
///
/// A function rather than a run of `if`s in `main` because these three
/// refusals are the only thing standing between a mistyped window and a sweep
/// that quietly runs seeds nobody asked for — and a guard that can only be
/// exercised by launching the binary is a guard nothing notices the loss of.
fn range(seed_start: u64, seeds: u64) -> Result<RangeInclusive<u64>, String> {
    if seeds == 0 {
        return Err("--seeds must be at least 1".to_owned());
    }
    if seed_start == 0 {
        return Err("--seed-start must be at least 1".to_owned());
    }
    // `seeds - 1` cannot underflow: the zero case was refused above. The last
    // seed of the space is a legal range of one, so the sum that must fit is
    // the last seed, not the one past it.
    let Some(last_seed) = seed_start.checked_add(seeds - 1) else {
        return Err("--seed-start plus --seeds overflows the seed space".to_owned());
    };
    Ok(seed_start..=last_seed)
}

/// How the swept configuration is named in the summary, so a pasted line says
/// which shape produced it.
const fn shape(args: &args::Args) -> &'static str {
    if args.mini {
        "mini"
    } else if args.eviction {
        "eviction"
    } else {
        "standard"
    }
}

/// The warning a sweep owes its reader when the plant it was serving is one
/// the shape it swept cannot catch, and `None` when the shape can catch it —
/// the case that needs no words, because there the violation count *is* a
/// statement about the plant.
///
/// A function rather than a `format!` inside `main` so the three things the
/// line has to carry — the plant, the shape, the place the defect does show —
/// can be held to by a test.
fn unobservable_warning(plant: Plant, shape: &str) -> Option<String> {
    let place = plant.unobservable_on_swept_shapes()?;
    Some(format!(
        "sweep: WARNING --plant {} was served, but the {shape} shape cannot catch it: \
         the violation count above is not evidence about that defect. \
         It is observable on {place}",
        plant.name()
    ))
}

/// Reports a usage problem on stderr, leaving stdout for the sweep's own
/// output.
fn fail(message: &str) -> ExitCode {
    eprintln!("sweep: {message}");
    eprintln!("{USAGE}");
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    use super::{range, unobservable_warning};
    use seedstone_sim::Plant;

    /// Every way a caller can ask for a range that cannot be walked, and the
    /// two edges that look like one but are legal.
    #[test]
    fn a_seed_range_is_refused_when_it_cannot_be_walked() {
        assert!(range(0, 10).is_err(), "seed 0 is not a seed");
        assert!(
            range(u64::MAX, 2).is_err(),
            "a range that runs off the end of the seed space is not a range"
        );
        assert_eq!(range(1, 3).unwrap(), 1..=3);
        assert_eq!(
            range(u64::MAX, 1).unwrap(),
            u64::MAX..=u64::MAX,
            "the last seed in the space is a legal one-seed range"
        );
        assert!(range(1, 0).is_err(), "a zero-length range runs nothing");
    }

    /// The usage line is the only documentation a caller gets at the moment
    /// they need it, and a flag that exists but is not named there is a flag
    /// nobody finds.
    #[test]
    fn the_usage_line_names_every_flag_sweep_accepts() {
        for flag in [
            "--seeds",
            "--seed-start",
            "--workload-seed",
            "--mini",
            "--eviction",
            "--plant",
            "--hashes",
            "--workers",
        ] {
            assert!(super::USAGE.contains(flag), "usage does not name {flag}");
        }
    }

    /// What the warning has to carry to be worth printing: which plant is
    /// being served, that this shape cannot catch it, and where it is caught
    /// instead. A reader who gets less than all three is left with a clean
    /// bill and no way to tell it is not one.
    #[test]
    fn the_warning_names_the_plant_the_shape_and_where_the_defect_shows() {
        let warning = unobservable_warning(Plant::ScanMissesRehash, "mini")
            .expect("a plant the swept shapes cannot catch is warned about");
        assert!(warning.contains("scan-misses-rehash"), "{warning}");
        assert!(warning.contains("mini"), "{warning}");
        assert!(warning.contains("dict.rs"), "{warning}");

        let warning = unobservable_warning(Plant::IgnoresCeiling, "standard")
            .expect("a node that ignores a ceiling no swept shape sets is warned about");
        assert!(warning.contains("ignores-ceiling"), "{warning}");
        assert!(warning.contains("standard"), "{warning}");
        assert!(warning.contains("planted_eviction.rs"), "{warning}");

        let warning = unobservable_warning(Plant::CrossingSkipsShard, "standard")
            .expect("a shape whose walks stop short is warned about a skipped shard");
        assert!(warning.contains("crossing-skips-shard"), "{warning}");
        assert!(warning.contains("standard"), "{warning}");
        assert!(warning.contains("planted_crossing.rs"), "{warning}");

        assert_eq!(
            unobservable_warning(Plant::LostUpdate, "standard"),
            None,
            "a plant this shape does catch must not be warned about: its zero is evidence"
        );
        assert_eq!(
            unobservable_warning(Plant::EvictsBelowCeiling, "standard"),
            None,
            "a node that evicts under no ceiling is caught by the exact plain model, \
             so its zero is evidence too"
        );
    }
}
