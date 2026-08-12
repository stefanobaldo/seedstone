//! `replay` — run one simulator seed and print what it produced.
//!
//! This is the "reproduces with seed 0x4f2a" artifact: given a seed a sweep
//! flagged, anyone on any machine runs this and gets a byte-identical line.
//! That is the whole claim the harness makes, so the output is deliberately
//! flat and stable — the planted-race self-test compares two subprocesses'
//! stdout directly, with no parsing in between.
//!
//! ```text
//! replay --sim-seed S [--workload-seed W] [--mini] [--plant]
//! ```
//!
//! Exits 1 if the run violated the lost-update invariant, so it also works as
//! a plain check.

use seedstone_sim::run_sim;
use std::process::ExitCode;

#[path = "shared/args.rs"]
mod args;

const USAGE: &str = "usage: replay --sim-seed S [--workload-seed W] [--mini] [--plant NAME]";

fn main() -> ExitCode {
    let args = match args::Args::from_env() {
        Ok(args) => args,
        Err(message) => return fail(&message),
    };
    // The foreign flag first: `replay --seeds 5` is someone reaching for
    // `sweep`, and "--sim-seed is required" would send them the wrong way.
    if args.seeds.is_some() {
        return fail("--seeds sweeps a range; replay runs one seed — see sweep");
    }
    if args.hashes {
        return fail(
            "--hashes selects which of a sweep's seeds print; replay always prints its own",
        );
    }
    let Some(sim_seed) = args.sim_seed else {
        return fail("--sim-seed is required");
    };

    let outcome = run_sim(&args.config(sim_seed));
    let held = outcome.invariant_holds();
    // Every counter, whatever the verdict, and each violation over the number
    // of replies its invariant decided: a zero on its own does not say whether
    // the invariant held or never ran.
    println!(
        "trace_hash=0x{:016x} expected={} actual={} stale={}/{} spurious={}/{} plain={}/{} \
         invariant={}",
        outcome.trace_hash,
        outcome.expected_sum,
        outcome.actual_sum,
        outcome.stale_reads,
        outcome.dead_checks,
        outcome.spurious_deaths,
        outcome.alive_checks,
        outcome.plain_mismatches,
        outcome.plain_checks,
        if held { "ok" } else { "violated" }
    );

    if held {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Reports a usage problem on stderr, leaving stdout clean for the one line
/// callers parse.
fn fail(message: &str) -> ExitCode {
    eprintln!("replay: {message}");
    eprintln!("{USAGE}");
    ExitCode::FAILURE
}
