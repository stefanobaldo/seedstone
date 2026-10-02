//! Every shape the gate sweeps, reproduced byte for byte from a separate
//! process.
//!
//! A sweep asserts that the invariants held. It never asserts that the run
//! *reproduces* — two runs of one seed could disagree about everything and
//! still each hold their invariants, and the sweep would pass twice. That
//! claim is this file's, and it is the one the whole project rests on, so it
//! is owed to the shapes CI actually sweeps rather than to whichever shape a
//! test was convenient to write for.
//!
//! **`standard` is here because it carries the sweep's detection power** — 275
//! seeds a run, against `eviction`'s 24 — and until this file it was the shape
//! with no cross-process replay at all. `planted_race.rs` proves the property
//! on `mini`, which CI does not sweep, and the eviction case below was added
//! when the ceiling landed. So the shape doing the most work was the one
//! proving the least about itself.
//!
//! **`eviction` is here for what only it puts on the replayed path.** The
//! first is a read of shared state on the write path: whether a shard reclaims
//! is decided against the node's used-memory figure, a word every executor
//! keeps current, so what a run evicts depends on what that word said at the
//! moment it was read. The second is the reply the verifier's `INFO stats`
//! produces — the widest thing the trace hash folds, twenty-eight counters
//! per shard including its hits, misses and expirations — which no other
//! shape asks for. Either is a way for two runs of one seed to disagree.
//!
//! Twenty-eight is `6 + KIND_SLOTS`: the six figures a `ShardStats` carries
//! outside its arrays (`keys`, `expires`, `evicted`, `hits`, `misses`,
//! `expired`), plus one `calls` counter per command kind, which is
//! `KIND_MAX + 1` = 22 with slot `0` — no command, always zero — folded with
//! the rest. `usec` is not folded, for the reason its own doc gives. The
//! arithmetic is written out because the number moves with every command kind
//! added, and the sentence before this one had been counting sixteen kinds
//! for two kinds longer than that was true.

use seedstone_core::shard::SyncPolicy;
use seedstone_sim::{CrashSchedule, FsyncDraw, SimConfig, run_sim};

/// How many eviction-shape seeds are replayed.
///
/// `planted_eviction.rs`'s count, and for the same reason: what is being shown
/// is a property of the shape rather than of any one schedule, and each seed
/// here costs two processes on top of the run this one does.
const SEEDS: u64 = 4;

/// How many standard-shape seeds are replayed.
///
/// One, and the asymmetry against the four above is paid for rather than
/// assumed. A standard seed runs a wider topology and a longer workload than
/// an eviction one, and each seed here is three runs of it: **~39 s of a debug
/// `cargo test --workspace`, measured, against ~2 s for all four eviction
/// seeds.** That is the price of the claim, and it is charged on every gate
/// run, so the number buying it should be the smallest number that buys it.
///
/// One does, because determinism is a property of the machinery rather than of
/// the schedule — a shape that reproduces on two independent schedules is not
/// one that fails on the third. What more seeds here would buy is the chance
/// of meeting a *rare* source of divergence, and that is exactly what the
/// 275-seed per-PR sweep and the nightly ten thousand are for. This file's job
/// is that the property is claimed for this shape at all, which until it
/// existed it was not.
const STANDARD_SEEDS: u64 = 1;

/// The workload seed every run pins — the same value [`SimConfig::eviction`]
/// is built with below and the same one passed on the command line, so a
/// differing hash means a differing schedule and nothing else.
const WORKLOAD_SEED: u64 = 1;

/// Runs one seed of `shape` in this process and twice more in fresh ones, and
/// holds all three to the same line.
///
/// `flags` are what the `replay` binary is given beside the seeds: none is
/// the standard shape at the policy its seed draws. `calibrate` is the shape's own proof that the
/// seed exercised what the shape exists for — a seed that replays a shape
/// whose distinguishing machinery never ran agrees with itself for no reason
/// worth having.
fn replays_across_processes(
    shape: &str,
    flags: &[&str],
    sim_seed: u64,
    config: &SimConfig,
    calibrate: impl Fn(&seedstone_sim::SimOutcome) -> Result<(), String>,
) {
    let outcome = run_sim(config);
    assert!(
        outcome.invariant_holds(),
        "{shape} seed {sim_seed} violated an invariant with an honest node: {outcome:?}"
    );
    if let Err(why) = calibrate(&outcome) {
        panic!("{shape} seed {sim_seed}: {why}: {outcome:?}");
    }

    let run = || {
        let mut args = vec![
            "--sim-seed".to_owned(),
            sim_seed.to_string(),
            "--workload-seed".to_owned(),
            WORKLOAD_SEED.to_string(),
        ];
        args.extend(flags.iter().map(|flag| (*flag).to_owned()));
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_replay"))
            .args(&args)
            .output()
            .expect("replay spawn");
        String::from_utf8(out.stdout).unwrap()
    };
    let (a, b) = (run(), run());
    assert_eq!(
        a, b,
        "{shape} seed {sim_seed} must replay to the same trace hash in separate processes"
    );
    // Two subprocesses agreeing prove less than they look like they do: they
    // agree by construction, both building their config through the same code
    // path. What a failure report promises is that *this* run reproduces, so
    // the in-process hash is what the replayed line has to carry.
    assert!(
        a.contains(&format!("trace_hash=0x{:016x}", outcome.trace_hash)),
        "{shape} seed {sim_seed}: replay must reproduce the trace hash the in-process \
         run computed (0x{:016x}), got: {a}",
        outcome.trace_hash
    );
    // Equal hashes are not the whole claim. A shape that started violating its
    // invariants would still replay to one hash, and this file would pass
    // while a planted-defect test failed somewhere else.
    assert!(
        a.contains("invariant=ok"),
        "{shape} seed {sim_seed}: the replayed run must hold its invariants too, got: {a}"
    );
}

/// The shape the per-PR gate sweeps 275 seeds of, and the nightly ten
/// thousand.
#[test]
fn the_standard_shape_replays_across_processes() {
    for sim_seed in 1..=STANDARD_SEEDS {
        replays_across_processes(
            "standard",
            &[],
            sim_seed,
            &SimConfig::standard(WORKLOAD_SEED, sim_seed),
            |outcome| {
                if outcome.snapshot_cycles == 0 {
                    return Err("the seed completed no snapshot cycle, so the checkpoint's \
                                fold is untested here"
                        .to_owned());
                }
                // The standard shape's distinguishing machinery is that it
                // decides anything at all: a seed whose workload never
                // exercised an invariant replays an empty claim.
                if outcome.invariants_were_exercised() {
                    Ok(())
                } else {
                    Err("the seed decided nothing, so nothing was replayed".to_owned())
                }
            },
        );
    }
}

/// A standard seed whose node completed snapshot cycles and was then
/// crashed inside one: the start that followed met an unfinished snapshot
/// and refused it. Seed 1 draws no crash, so it cannot say this.
#[test]
fn a_standard_seed_crashed_inside_a_cycle_replays_across_processes() {
    const CRASHED_INSIDE: u64 = 3;
    replays_across_processes(
        "standard",
        &[],
        CRASHED_INSIDE,
        &SimConfig::standard(WORKLOAD_SEED, CRASHED_INSIDE),
        |outcome| {
            if outcome.snapshot_cycles == 0 || outcome.crashes == 0 {
                return Err("the seed no longer cycles and crashes".to_owned());
            }
            if outcome.snapshots_refused_at_start == 0 {
                return Err("no crash landed inside a cycle any more: pick a seed that \
                            refuses a snapshot at its start"
                    .to_owned());
            }
            Ok(())
        },
    );
}

#[test]
fn the_eviction_shape_replays_across_processes() {
    for sim_seed in 1..=SEEDS {
        replays_across_processes(
            "eviction",
            &["--eviction"],
            sim_seed,
            &SimConfig::eviction(WORKLOAD_SEED, sim_seed),
            |outcome| {
                // Calibration, and the reason the eviction case is not a
                // slower copy of the standard one: a seed that never crossed
                // its ceiling replays a shape with no eviction in it.
                if outcome.evicted_keys > 0 {
                    Ok(())
                } else {
                    Err("the ceiling was never reached, so nothing about eviction \
                         was replayed"
                        .to_owned())
                }
            },
        );
    }
}

/// The shape whose node crashes on a disk that fails and lies: a crash, a
/// recovery through damage, and the reconnects after it are schedule like
/// anything else, and replay the same in a fresh process.
///
/// The first [`SEEDS`] seeds that *draw* a crash, rather than seeds `1..=`
/// it: a seed with none replays nothing about recovery, and skipping it
/// silently would leave the count claiming more than it shows.
#[test]
fn the_hostile_shape_replays_across_processes() {
    let crashing = (1..)
        .filter(|sim_seed| {
            !CrashSchedule::draw(
                SimConfig::hostile(WORKLOAD_SEED, *sim_seed).crashes,
                *sim_seed,
            )
            .is_empty()
        })
        .take(usize::try_from(SEEDS).expect("a small count"));
    for sim_seed in crashing {
        replays_across_processes(
            "hostile",
            &["--hostile"],
            sim_seed,
            &SimConfig::hostile(WORKLOAD_SEED, sim_seed),
            |outcome| {
                // Calibration: the draw said this seed crashes, so a run
                // with no recovery in it is a driver that did not.
                if outcome.recoveries > 0 {
                    Ok(())
                } else {
                    Err("the seed drew a crash and nothing recovered".to_owned())
                }
            },
        );
    }
}

/// One hostile seed under each durability policy, fixed rather than drawn:
/// what each policy holds back, issues and refuses is on the replayed path
/// — a drawn sync latency, a reply held for its sync, a refusal — and each
/// is a way for two runs of one seed to disagree.
#[test]
fn every_durability_policy_replays_across_processes() {
    const SIM_SEED: u64 = 3;
    for policy in [SyncPolicy::ALWAYS, SyncPolicy::INTERVAL, SyncPolicy::NEVER] {
        let mut config = SimConfig::hostile(WORKLOAD_SEED, SIM_SEED);
        config.fsync = FsyncDraw::Fixed(policy);
        replays_across_processes(
            &format!("hostile --fsync {}", policy.name()),
            &["--hostile", "--fsync", policy.name()],
            SIM_SEED,
            &config,
            |outcome| {
                if outcome.fsync == policy && outcome.recoveries > 0 {
                    Ok(())
                } else {
                    Err(
                        "the run did not take the policy it was given, or never recovered"
                            .to_owned(),
                    )
                }
            },
        );
    }
}
