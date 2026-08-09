//! `sweep` — run a contiguous range of simulator seeds against one workload.
//!
//! The workload seed is pinned and `sim_seed` varies, so a seed that fails is
//! a schedule that failed: the operations were the same in every run. Each
//! violation prints a line carrying everything needed to reproduce it, and the
//! seed on that line goes straight into `replay`.
//!
//! ```text
//! sweep --seeds N [--workload-seed W] [--mini] [--plant]
//! ```
//!
//! Exits 1 if any seed violated the lost-update invariant.

use seedstone_sim::run_sim;
use std::process::ExitCode;

#[path = "shared/args.rs"]
mod args;

const USAGE: &str = "usage: sweep --seeds N [--workload-seed W] [--mini] [--plant]";

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

    let mut violations = 0u64;
    for sim_seed in 1..=seeds {
        let outcome = run_sim(&args.config(sim_seed));
        if !outcome.invariant_holds() {
            violations += 1;
            // Printed as it happens rather than collected: a sweep that is
            // cancelled — by CI, by a person — has still reported what it
            // found up to that point.
            println!(
                "FAIL seed={} trace=0x{:016x} expected={} actual={}",
                sim_seed, outcome.trace_hash, outcome.expected_sum, outcome.actual_sum
            );
        }
    }

    println!(
        "swept {} seeds shape={} workload_seed={} planted={} violations={}",
        seeds,
        shape(&args),
        args.workload_seed,
        args.plant,
        violations
    );

    if violations == 0 {
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
