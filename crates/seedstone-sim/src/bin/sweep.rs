//! `sweep` — run a contiguous range of simulator seeds against one workload.
//!
//! The workload seed is pinned and `sim_seed` varies, so a seed that fails is
//! a schedule that failed: the operations were the same in every run. Each
//! violation prints a line carrying everything needed to reproduce it, and the
//! seed on that line goes straight into `replay`.
//!
//! ```text
//! sweep --seeds N [--seed-start S] [--workload-seed W] [--mini] [--plant NAME] [--hashes]
//!       [--workers K]
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
use std::process::ExitCode;

#[path = "shared/args.rs"]
mod args;

const USAGE: &str = "usage: sweep --seeds N [--seed-start S] [--workload-seed W] [--mini] \
                     [--plant NAME] [--hashes] [--workers K]";

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
    let Some(seeds) = args.seeds else {
        return fail("--seeds is required");
    };
    if seeds == 0 {
        return fail("--seeds must be at least 1");
    }
    let first_seed = args.seed_start.unwrap_or(1);
    if first_seed == 0 {
        return fail("--seed-start must be at least 1");
    }
    if first_seed.checked_add(seeds - 1).is_none() {
        return fail("--seed-start plus --seeds overflows the seed space");
    }
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
                     plain={} walk={}",
                    sim_seed,
                    outcome.trace_hash,
                    outcome.expected_sum,
                    outcome.actual_sum,
                    outcome.stale_reads,
                    outcome.spurious_deaths,
                    outcome.plain_mismatches,
                    outcome.walk_mismatches
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

    if report.violations == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// How the swept configuration is named in the summary, so a pasted line says
/// which shape produced it.
const fn shape(args: &args::Args) -> &'static str {
    if args.mini { "mini" } else { "standard" }
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
            "--plant",
            "--hashes",
            "--workers",
        ] {
            assert!(super::USAGE.contains(flag), "usage does not name {flag}");
        }
    }
}
