use super::*;
use crate::config::{CrashPlan, DiskFaults};

#[test]
fn same_seeds_same_hash_and_no_lost_updates() {
    let a = run_sim(&SimConfig::mini(1, 42));
    let b = run_sim(&SimConfig::mini(1, 42));
    assert_eq!(a.trace_hash, b.trace_hash, "in-process determinism");
    assert!(a.invariant_holds(), "every invariant must hold: {a:?}");
    // Without this every assertion above is vacuous: a workload that
    // acknowledged no `INCRBY` satisfies `0 == 0`, and one whose reads all
    // landed inside the band satisfies "no stale reads" without having
    // looked at one.
    assert!(
        a.invariants_were_exercised(),
        "the run decided nothing: {a:?}"
    );
    assert_eq!(
        a.expected_sum, b.expected_sum,
        "a pinned workload seed must issue the same increments"
    );
    let c = run_sim(&SimConfig::mini(1, 43));
    assert_ne!(
        a.trace_hash, c.trace_hash,
        "different sim seed, different schedule"
    );
}

/// The exact oracle: with nothing mutating, the model knows the whole
/// walk family, so the assertion is set equality rather than the weaker
/// at-least-once the concurrent case forces.
///
/// This is the strongest claim available for `KEYS` and `SCAN`, and it is
/// where a wrong matcher, a shard missing from the fan-out, an inverted
/// filter, a cursor that stops early or a broken dedup shows up with a
/// legible message instead of as an unreadable failing seed. It is a test
/// rather than a sweep for the reason [`SimConfig::quiescent_walk`] gives.
#[test]
fn a_quiescent_walk_returns_exactly_the_keys_that_are_there() {
    let mut cfg = SimConfig::mini(1, 42);
    cfg.quiescent_walk = true;
    let outcome = run_sim(&cfg);
    assert!(outcome.invariant_holds(), "{outcome:?}");
    assert!(
        outcome.invariants_were_exercised(),
        "a run that never reached the quiescent phase proves nothing: {outcome:?}"
    );
    // Named rather than left to `invariants_were_exercised`, which is
    // satisfied by the per-client walks alone: what this test is about is
    // the two assertions the cycle adds, and a run that skipped them would
    // otherwise pass here for the wrong reason.
    assert_eq!(
        outcome.walk_checks,
        2 * u64::from(cfg.clients) + 2,
        "a walk and a KEYS per client, plus the quiescent pair: {outcome:?}"
    );
}

/// The walk's guarantee, held under the schedule it is stated over.
///
/// The quiescent oracle above knows the whole family because nothing is
/// mutating; this is the case it cannot reach. Every client writes a
/// stable set, then walks its own family while writing and deleting
/// *other* keys of that family between the walk's steps — so the table
/// grows and rehashes with the walk in flight, which is the one thing a
/// reverse-binary cursor exists to survive, and fifteen other clients are
/// mutating the keyspace around it the whole time.
///
/// It runs the shape the gate sweeps rather than `mini`: what is being
/// asserted is that the invariant holds where it is actually swept, and
/// `standard` is the only shape that is.
#[test]
fn a_walk_under_concurrent_writers_still_returns_what_it_must() {
    let outcome = run_sim(&SimConfig::standard(7, 11));
    assert!(outcome.invariant_holds(), "{outcome:?}");
    assert!(
        outcome.invariants_were_exercised(),
        "a run that decided nothing proves nothing: {outcome:?}"
    );
    // Two checks per client, named rather than left to the line above:
    // the walk and the `KEYS` that closes it are separate claims, and a
    // run that quietly stopped making one of them would still satisfy
    // `walk_checks > 0`.
    assert_eq!(
        outcome.walk_checks,
        2 * u64::from(SimConfig::standard(7, 11).clients),
        "one walk and one KEYS per client: {outcome:?}"
    );
}

#[test]
fn the_trace_hash_is_pinned_across_processes_and_builds() {
    // The harness's product. Every other assertion about the trace compares
    // two runs of the *same* build to each other, and stays green if the
    // hash moves globally — a `cargo update` that reorders tokio's ready
    // queue or changes `rand`'s sampling would silently retire every seed
    // ever filed against this project, and nothing would say so.
    //
    // Unlike the SipHash and CRC vectors, this number has no external
    // reference to be derived from: it is definitionally whatever this
    // system computes. So it pins *stability*, not correctness, and that is
    // the whole job. A mismatch here is not a bug report — it means the
    // trace's meaning changed, and the question to answer is whether that
    // was intended. When it was — a new command kind, a new folded field,
    // or a change to *when* the workload issues what it already issued —
    // update the constant in the same commit that caused it, and say so in
    // the message. Never update it to make a red suite green.
    //
    // The third of those is the easiest to mistake for the first, and
    // `expected_sum` below is what tells them apart: it is a function of
    // the commands alone, so a hash that moved while it held still means
    // the same workload met a different schedule.
    // Repinned three times so far, each time beside the workload change
    // that moved it. First when the workload grew to the rest of the
    // one-key surface — `MGET`, `PEXPIRE`, `PERSIST`, `TYPE` and `STRLEN`
    // into the burst schedule, `DBSIZE` into the settle, and the draw
    // re-sliced to make room for them. Then when it gained the keyspace
    // walk: every client now writes a walk family and holds `KEYS` to
    // returning it exactly, the verifier drives a full `SCAN` cycle, and
    // a step's cursor, count and pattern are folded where previously only
    // its outcome was. Then when that walk was put under churn: a client
    // now steps its own family with `SCAN` while writing and deleting
    // other keys of it, so `SCAN` is on the wire in every seed rather
    // than only where a test asked for a full cycle — repinned once more
    // when that prefix was cut from four steps to two, which is a change
    // to *when* the workload issues what it already issued and moves this
    // without moving `expected_sum`. And then when the
    // `SET` algebra the client could reach went in: `NX`, `XX`, `GET` and
    // `KEEPTTL` took four rolls in a hundred off the bare `SET` and the
    // plain `GET`, so this moved and `plain_checks` rose by two — the
    // conditions and the read-and-write decide one each where the rolls
    // they took decided one each anyway. And then when one `SCAN` call
    // began crossing shards: the workload is unchanged and issues the
    // same steps in the same order, but a step that used to answer from
    // one shard now answers from as many as its budget crosses, so the
    // replies it folds are different ones. That is the fourth kind of
    // repin and the one this comment did not have — a change to *what a
    // command answers*, with `expected_sum` holding still because the
    // commands did not move. The trace folds every command's kind and
    // every reply, so an added command changes it by construction. A
    // change here with no workload or reply change beside it is a
    // regression, not a repin. The fourth kind fired a second time when
    // the walk's bucket ceiling was raised: one call now crosses more
    // shards, so the same steps in the same order fold different replies,
    // and `expected_sum` and the four counts held still again.
    //
    // And then the first kind fired: `SETEX` joined the deadlines a
    // volatile write can draw, so a roll that used to spell a deadline
    // `SET key value EX 1` now sometimes spells it `SETEX key 1 value`.
    // A new command kind on the wire, folded by its tag — the case the
    // paragraph above calls a repin by construction.
    //
    // And the first kind again, for `SETNX`: one of the two rolls that
    // spelled a conditional write `SET key value NX` now spells it
    // `SETNX key value`. The arm's four rolls in a hundred are unchanged
    // and so is the number of conditional writes issued, so this moves
    // by the new tag and the new reply frame alone — `expected_sum` and
    // all four check counts hold still, which is what says the workload
    // did not move underneath it.
    //
    // And once more for `PSETEX`, which is a repin of the first kind and
    // the second at once: an eighth deadline joins the seven a volatile
    // write can draw, so both the tags on the wire and the draw itself
    // move. It is drawn short, so `dead_checks` rises where `SETEX` had
    // raised `alive_checks` — the two positional spellings now reach one
    // half of the expiration invariant each.
    //
    // And then a change to *when the server dispatches*, with no change to
    // the workload or to any reply: an `MGET` of a few keys stopped closing
    // the connection's batch in front of it and began travelling inside it,
    // so the shards see its `GET`s in the same envelope as the commands
    // pipelined around it rather than in one of their own. The trace
    // records the order shards saw commands in, so this moved; the workload
    // did not, and `expected_sum` held still. One check count moved with
    // it, for a reason of the schedule's own — see beside the counts.
    //
    // And then twice for the log, a folded field and a schedule change,
    // neither a change to the workload. The node's start now folds every
    // shard's recovered position — where it resumes, and whether recovery
    // called it lossy — so a run that recovered a different prefix is a
    // different run; this seed never crashes, and the fold still moves it,
    // by construction. And the simulated node's connections now run on the
    // host's local task set, so that a crash closes them in the order they
    // were opened rather than in one set by the process's history; that
    // moves when each connection's task is polled. The idle-shed threshold
    // that arrived beside them moved nothing: on this seed, and on every
    // seed of the swept shapes, no connection ever sheds — measured, not
    // assumed. `expected_sum` and the four counts held still.
    //
    // And then for the checkpoint, twice over — a folded field and a
    // schedule change, neither a change to the workload. The node's sink
    // now folds every completed snapshot cycle and every compaction, so a
    // run that snapshotted at a different tick is a different run; and the
    // tick itself does more, since the checkpoint writes a budget of
    // entries after the log's two passes, which moves when each
    // executor's tick returns. The shed probe was run again on the
    // re-cut sweeps: no connection shed on 275 `standard` seeds, 24
    // `eviction`, 24 `hostile`, nor on this seed, so the shed path is
    // still held to a clock, not to a schedule. `expected_sum` and the
    // four counts held still.
    //
    // And then a change to *when the executor looks at its sync*, with no
    // change to the workload or to any reply: a completed sync now outranks
    // the inbox in the executor's loop, where it used to be seen only once
    // the inbox ran dry — which on a busy executor could be never. That
    // moves when each sync is noticed and so when the next is issued, and
    // the trace folds both. `expected_sum` and the four counts held still.
    const MINI_1_42: u64 = 0x6d40_9fae_bec2_e3a6;

    let outcome = run_sim(&SimConfig::mini(1, 42));
    assert_eq!(
        outcome.trace_hash, MINI_1_42,
        "the recorded trace hash moved"
    );
    // The workload behind the hash, pinned separately: the two can drift
    // apart, and a changed workload with a coincidentally equal hash is the
    // one failure the assertion above cannot see. The check counts are
    // pinned for a second reason — they are what says the expiration
    // invariants ran, and a workload that quietly stopped reaching them
    // would otherwise keep passing.
    assert_eq!(outcome.expected_sum, 63, "the recorded workload moved");
    assert_eq!(
        (
            outcome.dead_checks,
            outcome.alive_checks,
            outcome.plain_checks,
            outcome.walk_checks
        ),
        // `SETEX`'s arrival moved the first two and neither of the last
        // two: the seventh deadline is a second long, so two of the seven
        // a volatile write can draw now outlive the settle where one of
        // six did, and the draw decides `alive` where it used to decide
        // `dead`. Both halves of the expiration invariant are still
        // reached, which is what these two numbers are here to say.
        //
        // `PSETEX`'s arrival moved the same two back the other way, for
        // the mirror of that reason: the eighth deadline is 300ms, so it
        // dies inside the run and two of the eight outlive the settle
        // where two of seven did. `plain_checks` and `walk_checks` held
        // still through both, which is what says a deadline was added and
        // nothing else moved.
        //
        // The batched `MGET` moved `alive_checks` alone, by one, and not
        // because anything was drawn differently: whether a volatile read
        // is decided at all depends on the simulated instants its request
        // left and its reply arrived, measured against a deadline taken
        // from an earlier write's own, and every one of those instants
        // moves when a burst no longer stops for an `MGET` of its own. One
        // read that used to fall inside the live band now falls clear of
        // it and is decided. `dead_checks`,
        // `plain_checks`, `walk_checks` and `expected_sum` held still.
        //
        // The durability policy moved `dead_checks` alone, by five, and
        // the hash not at all: `mini` fixes `interval` and its disk draws
        // no latency, so its schedule is the one it had. What moved is the
        // judgement. A volatile key's deadline is now a band as wide as
        // the write's own round trip — the server took its deadline when
        // the command ran, any time before the reply arrived — and a read
        // is decided dead only once it was sent past the band's far end.
        // Five reads sent inside it no longer decide anything.
        // `alive_checks`, `plain_checks`, `walk_checks` and `expected_sum`
        // held still.
        (46, 33, 149, 32),
        "the recorded workload decides a different number of checks"
    );
}

/// Every plant, asked whether the shapes `sweep` walks can catch it.
///
/// Walked over [`Plant::ALL`] and matched without a wildcard, so a plant
/// added later cannot inherit an answer nobody decided: this stops
/// compiling until someone says where the new defect is observable.
#[test]
fn every_plant_answers_whether_the_swept_shapes_catch_it() {
    for plant in Plant::ALL {
        let place = plant.unobservable_on_swept_shapes();
        match plant {
            Plant::LostUpdate
            | Plant::ServeExpired
            | Plant::SweepEatsAll
            | Plant::EvictsBelowCeiling
            | Plant::ReportsCoveredAtOpen
            | Plant::TrustsUnfinishedSnapshot
            | Plant::RemovesUncovered
            | Plant::DurableOnIssue
            | Plant::DurableFromWrittenNow => assert_eq!(
                place,
                None,
                "{} is caught where it is swept, so it has no elsewhere to name",
                plant.name()
            ),
            Plant::ScanMissesRehash => {
                let place = place.expect("the swept shapes cannot observe an upward scan cursor");
                assert!(
                    place.contains("dict.rs"),
                    "a reader sent somewhere must be sent to a file: {place}"
                );
            }
            Plant::IgnoresCeiling => {
                let place =
                    place.expect("a shape with no ceiling cannot observe one being ignored");
                assert!(
                    place.contains("planted_eviction.rs"),
                    "a reader sent somewhere must be sent to a file: {place}"
                );
            }
            Plant::CrossingSkipsShard => {
                let place =
                    place.expect("a shape whose walks stop short cannot observe a skipped shard");
                assert!(
                    place.contains("planted_crossing.rs"),
                    "a reader sent somewhere must be sent to a file: {place}"
                );
            }
            Plant::PrefixScanRecovery => {
                let place = place
                    .expect("a shape with no read corruption cannot observe a reader that cuts");
                assert!(
                    place.contains("planted_recovery.rs"),
                    "a reader sent somewhere must be sent to a file: {place}"
                );
            }
            Plant::AcksWhileRefusing => {
                let place =
                    place.expect("a disk that raises no error cannot observe a refusal ignored");
                assert!(
                    place.contains("planted_durability.rs"),
                    "a reader sent somewhere must be sent to a file: {place}"
                );
            }
        }
    }
    // The place is a string, so nothing but this stops it outliving the
    // file it names — and a warning pointing at a path that is not there
    // is worse than no warning.
    for path in [
        concat!(env!("CARGO_MANIFEST_DIR"), "/../seedstone-core/src/dict.rs"),
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/planted_eviction.rs"),
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/planted_crossing.rs"),
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/planted_recovery.rs"),
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/planted_durability.rs"),
    ] {
        assert!(
            std::path::Path::new(path).exists(),
            "the place a plant points at no longer exists: {path}"
        );
    }
}

/// Which plants a sweep's violation count is evidence about, pinned as a
/// set rather than one by one: the interesting claim is *which* defects
/// are outside what the swept shapes reach, and one appearing or leaving
/// that set is a change in what those shapes measure.
#[test]
fn the_plants_the_swept_shapes_cannot_catch_are_the_five_that_need_a_shape() {
    let unobservable: Vec<&str> = Plant::ALL
        .into_iter()
        .filter(|plant| plant.unobservable_on_swept_shapes().is_some())
        .map(Plant::name)
        .collect();
    assert_eq!(
        unobservable,
        [
            "scan-misses-rehash",
            "ignores-ceiling",
            "crossing-skips-shard",
            "prefix-scan-recovery",
            "acks-while-refusing"
        ],
        "the plants a swept violation count says nothing about have changed"
    );
}

/// A fresh node's every shard reports a resumed position of zero, and the
/// sink folds each into the trace: a run that started from a log and one
/// that started empty have different hashes.
#[test]
fn recovery_reaches_the_trace() {
    let outcome = run_sim(&SimConfig::mini(1, 42));
    assert_eq!(outcome.recoveries, 0, "no crash, no recovery counted");
    // The fold itself is pinned by `the_trace_hash_is_pinned…`, re-cut once
    // every input to it is in.
}

/// The driver crashes and restarts the node on the seed's schedule, and the
/// run still holds every invariant it can state.
#[test]
fn a_run_with_crashes_under_load_recovers_and_holds() {
    // Seed 2 draws two crashes, at 61 ms and 499 ms: inside the workload,
    // and more than one, so a restart is itself restarted from.
    let mut cfg = SimConfig::mini(1, 2);
    cfg.crashes = CrashPlan::UnderLoad { max: 2 };
    cfg.disk = DiskFaults::TORN;
    let outcome = run_sim(&cfg);
    assert_eq!(
        outcome.crashes, 2,
        "the seed's schedule was not driven: {outcome:?}"
    );
    assert!(outcome.invariant_holds(), "{outcome:?}");
    assert_eq!(
        outcome.recoveries, outcome.crashes,
        "every crash was followed by a recovery: {outcome:?}"
    );
    // Determinism across the crash: the same seed crashes at the same
    // instants and folds the same recoveries.
    assert_eq!(run_sim(&cfg).trace_hash, outcome.trace_hash);
}

/// At rest, every acknowledged write was synced before the crash, so the
/// model is exact after it and reads everything back.
#[test]
fn a_crash_at_rest_recovers_exactly() {
    let mut cfg = SimConfig::mini(1, 3);
    cfg.crashes = CrashPlan::AtRest;
    cfg.disk = DiskFaults::TORN;
    let outcome = run_sim(&cfg);
    assert!(outcome.invariant_holds(), "{outcome:?}");
    assert_eq!(outcome.crashes, 1);
    assert_eq!(outcome.recoveries, 1);
    assert_eq!(outcome.lost_durable_prefixes, 0);
    assert!(outcome.plain_checks > 0);
}

/// The schedule is a function of the seed and the plan, and nothing else.
#[test]
fn the_crash_schedule_is_drawn_from_the_seed() {
    use crate::durability::{CRASH_WINDOW, CrashSchedule};
    let mut a = CrashSchedule::draw(CrashPlan::UnderLoad { max: 2 }, 5);
    let mut b = CrashSchedule::draw(CrashPlan::UnderLoad { max: 2 }, 5);
    assert_eq!(a.instants(), b.instants());
    assert!(a.instants().iter().all(|at| *at <= CRASH_WINDOW));
    assert!(a.instants().len() <= 2);
    let differs = (1..20u64).any(|seed| {
        CrashSchedule::draw(CrashPlan::UnderLoad { max: 2 }, seed).instants() != a.instants()
    });
    assert!(differs, "twenty seeds drew the same schedule");
    assert!(
        CrashSchedule::draw(CrashPlan::None, 5)
            .instants()
            .is_empty()
    );
    assert!(
        CrashSchedule::draw(CrashPlan::AtRest, 5)
            .instants()
            .is_empty()
    );
    let _ = (a.next_due(Duration::ZERO), b.next_due(Duration::ZERO));
}

/// The verdict's shape under a crash: the counter sum is a range, a lost
/// durable write is a violation on the swept disk and an excused one on
/// the hostile disk only when recovery reported the shard.
#[test]
fn the_verdict_knows_what_a_crash_and_a_hostile_disk_excuse() {
    let clean = run_sim(&SimConfig::mini(1, 42));
    let mut held = clean.clone();
    held.crashes = 1;
    held.counter_floor = held.expected_sum - 5;
    held.counter_ceiling = held.expected_sum;
    held.actual_sum = held.expected_sum - 3;
    assert!(
        held.invariant_holds(),
        "a sum inside [floor, ceiling] holds under a crash"
    );
    held.actual_sum = held.expected_sum - 6;
    assert!(
        !held.invariant_holds(),
        "below the floor is a lost durable increment"
    );
    held.actual_sum = held.expected_sum + 1;
    assert!(
        !held.invariant_holds(),
        "above the ceiling is an increment nobody sent"
    );
    held.counter_ceiling = held.expected_sum + 1;
    assert!(
        held.invariant_holds(),
        "an increment whose reply a crash took may have landed"
    );

    let mut lost = clean.clone();
    lost.crashes = 1;
    lost.lost_durable_prefixes = 1;
    assert!(!lost.invariant_holds(), "the swept disk promises survival");
    lost.hostile = true;
    assert!(
        lost.invariant_holds(),
        "the hostile disk promises only that a loss is reported"
    );
    lost.unreported_losses = 1;
    assert!(!lost.invariant_holds());

    let mut read_lost = clean.clone();
    read_lost.lost_durable_writes = 1;
    assert!(
        !read_lost.invariant_holds(),
        "a durable value read back wrong"
    );

    let mut phantom = clean;
    phantom.phantom_writes = 1;
    assert!(
        !phantom.invariant_holds(),
        "a value nobody wrote is never excused"
    );
}

/// A client that first connects while the node is down — restarting, or
/// retrying a start the disk refused — waits for it instead of ending the
/// run. `hostile(1, 2)` has a connect land in that window.
#[test]
fn a_client_that_meets_a_restarting_node_waits_for_it() {
    let outcome = run_sim(&SimConfig::hostile(1, 2));
    assert!(
        outcome.crashes > 0,
        "the seed no longer crashes: {outcome:?}"
    );
    assert!(outcome.invariant_holds(), "{outcome:?}");
}

#[test]
fn the_disk_bound_is_the_formula_and_a_peak_past_it_is_a_violation() {
    let config = CheckpointConfig {
        floor: 2048,
        ratio: 1,
        bytes_per_tick: 4096,
    };
    // S = 10 000, W = 500: 2S + max(2048, S) + W = 30 500 per executor.
    assert_eq!(
        disk_bound(4, config, 10_000, 500, 0),
        4 * 30_500 + DISK_SLACK
    );
    assert_eq!(
        disk_bound(4, config, 10_000, 500, 1),
        2 * 4 * 30_500 + DISK_SLACK,
        "a restart keeps the previous process's files beside the new one's"
    );
    assert_eq!(
        disk_bound(4, config, 10_000, 500, 2),
        3 * 4 * 30_500 + DISK_SLACK,
        "a second restart before the first one's round closed keeps three"
    );
    assert_eq!(
        disk_bound(1, config, 100, 0, 0),
        2 * 100 + 2048 + DISK_SLACK,
        "the floor governs a small image"
    );
    let mut outcome = crate::outcome::nothing_observed();
    outcome.disk_bound_bytes = 100;
    outcome.disk_peak_bytes = 100;
    assert!(outcome.invariant_holds());
    outcome.disk_peak_bytes = 101;
    assert!(!outcome.invariant_holds());
}

#[test]
fn a_run_with_no_snapshot_cycle_exercised_nothing() {
    let mut outcome = crate::outcome::nothing_observed();
    // Everything else a run needs to have exercised, at one.
    outcome.expected_sum = 1;
    outcome.dead_checks = 1;
    outcome.alive_checks = 1;
    outcome.plain_checks = 1;
    outcome.walk_checks = 1;
    assert!(!outcome.invariants_were_exercised(), "no cycle ran");
    outcome.snapshot_cycles = 1;
    assert!(outcome.invariants_were_exercised());
}

/// The simulated node snapshots and compacts on every swept shape, and the
/// directory stays inside the bound.
#[test]
fn the_simulated_node_cycles_on_the_mini_shape() {
    let outcome = run_sim(&SimConfig::mini(1, 42));
    assert!(outcome.snapshot_cycles >= 1, "{outcome:?}");
    assert!(outcome.compactions >= 1, "{outcome:?}");
    assert!(
        outcome.disk_peak_bytes > 0 && outcome.disk_peak_bytes <= outcome.disk_bound_bytes,
        "{outcome:?}"
    );
    assert!(
        outcome.invariant_holds() && outcome.invariants_were_exercised(),
        "{outcome:?}"
    );
}

#[test]
fn the_three_compaction_plants_are_selectable_by_name() {
    assert_eq!(
        Plant::from_name("reports-covered-at-open"),
        Some(Plant::ReportsCoveredAtOpen)
    );
    assert_eq!(
        Plant::from_name("trusts-unfinished-snapshot"),
        Some(Plant::TrustsUnfinishedSnapshot)
    );
    assert_eq!(
        Plant::from_name("removes-uncovered"),
        Some(Plant::RemovesUncovered)
    );
    assert_eq!(Plant::ALL.len(), 14);
}

/// A sync on the simulated disk completes after a latency drawn from the
/// seed, inside the configured range; the same seed draws the same
/// latencies.
#[test]
fn a_simulated_sync_completes_after_a_drawn_latency() {
    use seedstone_core::log::disk::LogFile;

    fn draws(seed: u64) -> Vec<u64> {
        let mut sim = turmoil::Builder::new().build();
        let drawn = Arc::new(Mutex::new(Vec::new()));
        let host = Arc::clone(&drawn);
        sim.client("host", async move {
            let rng = Arc::new(Mutex::new(ChaCha8Rng::seed_from_u64(seed)));
            let disk = SimDisk::new((5, 40), Some(rng));
            disk.create_dir_all(Path::new("/d")).unwrap();
            let mut file = disk.create_append(Path::new("/d/f")).unwrap();
            for _ in 0..4 {
                file.write_all(b"x").unwrap();
                let before = turmoil::sim_elapsed().unwrap();
                file.sync_later().await.unwrap();
                let took = turmoil::sim_elapsed().unwrap().checked_sub(before).unwrap();
                host.lock()
                    .unwrap()
                    .push(u64::try_from(took.as_millis()).unwrap());
            }
            Ok(())
        });
        sim.run().unwrap();
        drawn.lock().unwrap().clone()
    }
    let a = draws(7);
    assert!(a.iter().all(|ms| (5..=40).contains(ms)), "{a:?}");
    assert_eq!(a, draws(7), "replayable");
    assert_ne!(a, draws(8), "and seeded");
}

/// The two swept shapes draw the durability policy from the seed — all
/// three reached within thirty seeds, and the draw belongs to the seed, not
/// the shape — and every other shape fixes the default.
#[test]
fn the_swept_shapes_draw_a_policy_per_seed_and_the_others_fix_interval() {
    let mut seen = BTreeSet::new();
    for seed in 1..=30 {
        seen.insert(SimConfig::standard(1, seed).policy().name());
    }
    assert_eq!(seen.len(), 3, "thirty seeds reach all three: {seen:?}");
    assert_eq!(
        SimConfig::standard(1, 5).policy(),
        SimConfig::hostile(1, 5).policy(),
        "the draw is the seed's, not the shape's"
    );
    assert_eq!(SimConfig::mini(1, 42).policy(), SyncPolicy::INTERVAL);
    assert_eq!(SimConfig::eviction(1, 3).policy(), SyncPolicy::INTERVAL);
}

/// A crash knows whether it landed with a sync in flight; on the swept disk
/// under `always`, some seed's does.
#[test]
fn a_crash_records_whether_a_sync_was_in_flight() {
    let run = |sim_seed| {
        let mut cfg = SimConfig::standard(1, sim_seed);
        cfg.fsync = FsyncDraw::Fixed(SyncPolicy::ALWAYS);
        run_sim(&cfg)
    };
    let outcome = run(2);
    assert!(outcome.crashes_in_flight <= outcome.crashes, "{outcome:?}");
    assert!(
        (1..=12).any(|seed| run(seed).crashes_in_flight > 0),
        "no seed in 1..=12 crashed with a sync in flight"
    );
}

/// No write is refused on a disk that raised no error; a refusal that
/// follows a fault is the node keeping its promise.
#[test]
fn a_refusal_on_a_disk_that_raises_no_error_is_a_violation() {
    let mut outcome = crate::outcome::nothing_observed();
    outcome.refused = 1;
    assert!(!outcome.invariant_holds(), "no fault, yet a refusal");
    outcome.hostile = true;
    assert!(
        !outcome.invariant_holds(),
        "hostile, but still no fault behind it"
    );
    outcome.write_faults = 1;
    assert!(outcome.invariant_holds(), "a refusal that follows a fault");
}

/// Under `always` an acknowledgement is the proof of durability: every
/// crashing seed decides durable reads, and holds.
#[test]
fn the_always_policy_decides_its_strong_claim_on_every_crashing_seed() {
    for sim_seed in 1..=12 {
        let mut cfg = SimConfig::standard(1, sim_seed);
        cfg.fsync = FsyncDraw::Fixed(SyncPolicy::ALWAYS);
        let outcome = run_sim(&cfg);
        assert!(outcome.invariant_holds(), "seed {sim_seed}: {outcome:?}");
        if outcome.crashes > 0 {
            assert!(
                outcome.durable_checks > 0,
                "seed {sim_seed} crashed and decided nothing durable: {outcome:?}"
            );
        }
    }
}

#[test]
fn the_three_durability_plants_are_selectable_by_name() {
    assert_eq!(
        Plant::from_name("durable-on-issue"),
        Some(Plant::DurableOnIssue)
    );
    assert_eq!(
        Plant::from_name("acks-while-refusing"),
        Some(Plant::AcksWhileRefusing)
    );
    assert_eq!(
        Plant::from_name("durable-from-written-now"),
        Some(Plant::DurableFromWrittenNow)
    );
}

/// A refused increment may have been applied, so a run that refused is
/// held to the counter range even without a crash.
#[test]
fn a_run_that_refused_is_held_to_the_counter_range() {
    let mut outcome = crate::outcome::nothing_observed();
    outcome.hostile = true;
    outcome.write_faults = 1;
    outcome.refused = 1;
    outcome.expected_sum = 5;
    outcome.actual_sum = 7;
    outcome.counter_floor = 5;
    outcome.counter_ceiling = 9;
    assert!(outcome.invariant_holds(), "inside the range");
    outcome.actual_sum = 10;
    assert!(!outcome.invariant_holds(), "outside it");
}

/// After a log fault no write is done until the refusal it began is over.
#[test]
fn a_write_acknowledged_by_a_refusing_executor_is_a_violation() {
    let mut outcome = crate::outcome::nothing_observed();
    outcome.hostile = true;
    outcome.write_faults = 1;
    assert!(outcome.invariant_holds());
    outcome.acked_while_refusing = 1;
    assert!(!outcome.invariant_holds());
}

/// A run that refused writes owes no expiration check: its volatile writes
/// may all have been refused. Everything else it still owes.
#[test]
fn a_run_that_refused_owes_no_expiration_check() {
    let mut outcome = crate::outcome::nothing_observed();
    outcome.expected_sum = 1;
    outcome.plain_checks = 1;
    outcome.walk_checks = 1;
    outcome.snapshot_cycles = 1;
    assert!(!outcome.invariants_were_exercised(), "no expiry decided");
    outcome.refused = 1;
    assert!(outcome.invariants_were_exercised());
    outcome.plain_checks = 0;
    assert!(
        !outcome.invariants_were_exercised(),
        "the plain family still owes"
    );
}

/// A run whose crashes interrupt every cycle reports no snapshot, so the
/// directory is never read: the peak is taken from what the snapshots
/// report, and the bound made of nothing holds it.
///
/// A verdict test rather than a seed: since the log is flushed per
/// envelope rather than per tick, no `standard` seed in 1..=700 completes
/// fewer than three cycles, so no seed reaches this path end to end.
#[test]
fn a_run_that_never_completes_a_cycle_is_not_read_against_an_empty_bound() {
    let mut outcome = crate::outcome::nothing_observed();
    outcome.crashes = 2;
    outcome.disk_bound_bytes = disk_bound(10, SIM_CHECKPOINT, 0, 0, 2);
    assert_eq!(outcome.snapshot_cycles, 0);
    assert_eq!(outcome.disk_peak_bytes, 0, "nothing reported, nothing read");
    assert!(outcome.invariant_holds(), "{outcome:?}");
    assert!(
        !outcome.invariants_were_exercised(),
        "and the run says it measured nothing about the bound"
    );
}

/// One pinned seed per durability policy, beside `MINI_1_42`: the schedule
/// of a hostile run under that policy, sync latencies drawn and refusals
/// possible. `hostile` is `mini`-sized, so the three cost little; a move in
/// one and not the others is a change to what that policy does.
#[test]
fn one_seed_per_durability_policy_is_pinned() {
    // `always` moved alone when a completed sync began to outrank the
    // inbox: the order changes when held replies leave, and only `always`
    // holds any.
    const HOSTILE_1_3_ALWAYS: u64 = 0x86f9_9a80_8807_2dd2;
    const HOSTILE_1_3_INTERVAL: u64 = 0x3234_f441_cdab_ad5b;
    const HOSTILE_1_3_NEVER: u64 = 0x71ba_5c7c_fbcc_a684;
    for (policy, pinned) in [
        (SyncPolicy::ALWAYS, HOSTILE_1_3_ALWAYS),
        (SyncPolicy::INTERVAL, HOSTILE_1_3_INTERVAL),
        (SyncPolicy::NEVER, HOSTILE_1_3_NEVER),
    ] {
        let mut cfg = SimConfig::hostile(1, 3);
        cfg.fsync = FsyncDraw::Fixed(policy);
        let outcome = run_sim(&cfg);
        assert_eq!(
            outcome.trace_hash,
            pinned,
            "hostile(1, 3) at --fsync {}: 0x{:016x}",
            policy.name(),
            outcome.trace_hash
        );
    }
}

/// Standard seeds the sweep once caught out, each for a reason now written
/// into the model: a reply from the crashed process that arrived after the
/// crash, read against the durable point of the process that replaced it,
/// which pruned the write the crash had left (52, 91, both drawing
/// `never`). The honest node holds on both.
#[test]
fn the_honest_node_holds_on_the_standard_seeds_that_once_caught_the_model_out() {
    for sim_seed in [52, 91] {
        let outcome = run_sim(&SimConfig::standard(1, sim_seed));
        assert!(outcome.invariant_holds(), "seed {sim_seed}: {outcome:?}");
    }
}
