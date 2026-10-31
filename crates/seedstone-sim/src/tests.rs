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
    //
    // Re-pinned: the writer task joined the schedule and a compaction report
    // names no executor. And again when a snapshot that made nothing
    // redundant stopped reporting a compaction of nothing.
    //
    // Re-pinned: the clients ask for a snapshot on one roll in a hundred
    // (`BGSAVE`), taken off TTL.
    //
    // Re-pinned: the housekeeping tick, when due, now outranks the inbox,
    // which moves when each tick runs against the envelopes around it.
    //
    // And back to the value before that: the tick is now read after each
    // envelope, against the clock reading the envelope already took, and
    // awaited only when the inbox is empty. Under the simulator's paused
    // clock time advances only when every task is idle, so the tick lands
    // where it did behind the inbox, and the hash is the one from before
    // the tick outranked it. The real-time guarantee the re-ordering bought
    // (#79) is held by the inline check and by its own test.
    //
    // Re-pinned: a snapshot's footer now waits until the log is synced
    // through what its image may hold, so a cycle can complete a tick or
    // more later than it did. `expected_sum` and the four counts held
    // still.
    const MINI_1_42: u64 = 0x630f_4d85_774f_3bc0;

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
        //
        // `BGSAVE` on roll 99 moved none of the four and not
        // `expected_sum`: it draws the slot the `TTL` it replaced would
        // have, and reads nothing the model judges.
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
            | Plant::FinishesBeforeLog
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

/// A per-seed plan names no schedule until the config resolves it: drawn
/// unresolved it would be a seed that silently never crashes.
#[test]
#[should_panic(expected = "resolve a per-seed plan")]
fn a_per_seed_plan_is_resolved_before_its_schedule_is_drawn() {
    let _ = crate::durability::CrashSchedule::draw(CrashPlan::PerSeed { max: 2 }, 5);
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
    // S = 10 000, W = 500: 2S + max(2048, S) + W = 30 500 per executor,
    // and one closed segment of 4 096 for the node.
    assert_eq!(
        disk_bound(4, config, 4096, 10_000, 500, 0),
        4 * 30_500 + 4096 + DISK_SLACK
    );
    assert_eq!(
        disk_bound(4, config, 4096, 10_000, 500, 1),
        2 * (4 * 30_500 + 4096) + DISK_SLACK,
        "a restart keeps the previous process's files beside the new one's"
    );
    assert_eq!(
        disk_bound(4, config, 4096, 10_000, 500, 2),
        3 * (4 * 30_500 + 4096) + DISK_SLACK,
        "a second restart before the first one's round closed keeps three"
    );
    assert_eq!(
        disk_bound(1, config, 4096, 100, 0, 0),
        2 * 100 + 2048 + 4096 + DISK_SLACK,
        "the floor governs a small image"
    );
    let mut outcome = crate::outcome::nothing_observed();
    outcome.disk_bound_bytes = 100;
    outcome.disk_peak_bytes = 100;
    assert!(outcome.invariant_holds());
    outcome.disk_peak_bytes = 101;
    assert!(!outcome.invariant_holds());
}

/// A closed rotation is as large as the drain that crossed its size made it,
/// not the size it rotates at: under `never` every rotation waits for the
/// old file's sync while the executors keep sending, and the next drain
/// lands whole. `standard(303, 3020111)` closes rotations of up to 36 KiB
/// at a 4 KiB segment size, and the front one, held by the slowest
/// executor's base, overran a bound that allowed one segment for it.
#[test]
fn the_disk_bound_allows_the_largest_rotation_the_node_closed() {
    let outcome = run_sim(&SimConfig::standard(303, 3_020_111));
    assert!(
        outcome.max_rotation_bytes > SIM_SEGMENT_BYTES,
        "the seed no longer closes a rotation past the segment size: {outcome:?}"
    );
    assert!(
        outcome.disk_peak_bytes <= outcome.disk_bound_bytes,
        "{outcome:?}"
    );
    assert!(outcome.invariant_holds(), "{outcome:?}");
}

#[test]
fn a_run_with_no_snapshot_cycle_exercised_nothing() {
    let mut outcome = crate::outcome::nothing_observed();
    // Everything else a run needs to have exercised, at one.
    outcome.counter_acks = 1;
    outcome.dead_checks = 1;
    outcome.alive_checks = 1;
    outcome.plain_checks = 1;
    outcome.walk_checks = 1;
    assert!(!outcome.invariants_were_exercised(), "no cycle ran");
    outcome.snapshot_cycles = 1;
    assert!(outcome.invariants_were_exercised());
}

/// The counter was exercised when some increment was acknowledged, not when
/// the acknowledged deltas happen to sum to something: they are drawn from
/// -10..=10, and hostile seed 1 once acknowledged 59 that summed to zero.
#[test]
fn acknowledged_increments_summing_to_zero_still_exercised_the_counter() {
    let mut outcome = crate::outcome::nothing_observed();
    outcome.counter_acks = 59;
    outcome.dead_checks = 1;
    outcome.alive_checks = 1;
    outcome.plain_checks = 1;
    outcome.walk_checks = 1;
    outcome.snapshot_cycles = 1;
    assert!(outcome.invariants_were_exercised(), "{outcome:?}");
    outcome.counter_acks = 0;
    outcome.expected_sum = 1;
    assert!(
        !outcome.invariants_were_exercised(),
        "a sum with no acknowledgement behind it exercised nothing"
    );
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
    assert_eq!(
        Plant::from_name("finishes-before-log"),
        Some(Plant::FinishesBeforeLog)
    );
    assert_eq!(Plant::ALL.len(), 15);
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
            let disk = SimDisk::new((5, 40), 0, Some(rng));
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
    outcome.refused_certain = 1;
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
    outcome.refused_applied = 1;
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

/// The server's two counts of its refusals add up to what the clients saw:
/// one answered at the command, the executor already refusing, and one
/// answered to a held write at a fault, applied and never acknowledged. A
/// refusal outside both is a write refused after it was applied somewhere
/// the held path does not reach.
#[test]
fn every_refused_reply_is_either_certain_or_applied_and_a_third_kind_is_a_violation() {
    let mut outcome = crate::outcome::nothing_observed();
    outcome.hostile = true;
    outcome.write_faults = 1;
    outcome.refused = 5;
    outcome.refused_certain = 3;
    outcome.refused_applied = 2;
    assert!(outcome.invariant_holds());
    outcome.refused_applied = 1;
    assert!(
        !outcome.invariant_holds(),
        "one refusal the server cannot account for"
    );
    outcome.refused_applied = 3;
    assert!(
        !outcome.invariant_holds(),
        "with no crash to take it, a refusal no client read"
    );
    outcome.crashes = 1;
    assert!(
        outcome.invariant_holds(),
        "a crash may take a refusal's reply"
    );
}

/// `acked_while_refusing` reads at the command, before a later fault; a held
/// write released as success once its executor was refusing is visible
/// only at the release.
#[test]
fn a_held_write_released_as_success_after_a_fault_is_a_violation() {
    let mut outcome = crate::outcome::nothing_observed();
    outcome.hostile = true;
    outcome.write_faults = 1;
    assert!(outcome.invariant_holds());
    outcome.released_while_refusing = 1;
    assert!(!outcome.invariant_holds());
}

/// A run that refused owes its expiration checks unless every volatile
/// write was refused: one the server took has a deadline to die by.
#[test]
fn a_run_that_refused_owes_its_expiration_checks_unless_no_volatile_write_was_taken() {
    let mut outcome = crate::outcome::nothing_observed();
    outcome.counter_acks = 1;
    outcome.plain_checks = 1;
    outcome.walk_checks = 1;
    outcome.snapshot_cycles = 1;
    assert!(!outcome.invariants_were_exercised(), "no expiry decided");
    outcome.refused = 1;
    outcome.volatile_acked = 4;
    assert!(
        !outcome.invariants_were_exercised(),
        "volatile writes were taken: the expiration checks are owed"
    );
    outcome.volatile_acked = 0;
    assert!(
        outcome.invariants_were_exercised(),
        "every volatile write refused: nothing could die"
    );
    outcome.plain_checks = 0;
    assert!(
        !outcome.invariants_were_exercised(),
        "the plain family still owes"
    );
}

/// A failure of the node's log refuses every executor at once, and each
/// returns on its own snapshot: at most one return per executor per fault.
#[test]
fn the_node_refuses_as_one_and_resumes_one_executor_at_a_time() {
    let cfg = SimConfig::hostile(1, 2);
    let outcome = run_sim(&cfg);
    let faults = outcome.write_faults + outcome.sync_faults + outcome.rotate_faults;
    assert!(faults >= 1, "seed 2 faults: {outcome:?}");
    assert!(outcome.refusals_ended >= 1, "{outcome:?}");
    assert!(
        outcome.refusals_ended <= u64::from(cfg.executors) * faults,
        "more returns than executors per fault: {outcome:?}"
    );
    assert!(outcome.invariant_holds(), "{outcome:?}");
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
    outcome.disk_bound_bytes = disk_bound(10, SIM_CHECKPOINT, SIM_SEGMENT_BYTES, 0, 0, 2);
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
    //
    // Re-pinned: the writer task joined the schedule and a compaction report
    // names no executor. And again, all three, when a snapshot that made
    // nothing redundant stopped reporting a compaction of nothing.
    //
    // Re-pinned, all three, when `hostile` went from a thousand shards to
    // sixteen, drew its crash at rest or under load per seed, and its disk
    // began to draw sync faults — one move for the three.
    //
    // Re-pinned, all three: the clients ask for a snapshot on one roll in
    // a hundred (`BGSAVE`), taken off TTL.
    //
    // Re-pinned, `interval` and `never`, when the housekeeping tick began to
    // outrank the inbox; `always` held still.
    //
    // And back, `interval` and `never`, to the values before that, when the
    // tick came to be read after each envelope and awaited only when idle:
    // under the paused clock that is the schedule the tick had behind the
    // inbox (see `MINI_1_42`). `always` held still again.
    //
    // Re-pinned, all three, when every start began to sync the files it
    // recovered from: each sync draws a fault from the disk's stream.
    //
    // Re-pinned, all three, when a segment's header began to carry its
    // generation's cut points: two bytes more in every file.
    //
    // Re-pinned, `always` alone, when a read that expires its key stopped
    // being held for the sync: its reply leaves at once.
    //
    // Re-pinned, all three, when the writer began to write a mark before
    // every sync it issues: one write more per round, and one more draw
    // from the hostile disk. `never` moved too — its rotations and its stop
    // sync, and mark. `MINI_1_42` held still.
    //
    // Re-pinned, all three, when recovery stopped charging a crash's torn
    // tail to every shard as lossy: the hash folds each recovered shard's
    // lossy flag, and fewer are set. `MINI_1_42` held still.
    //
    // Re-pinned, `always` and `never`, when a snapshot's footer began to
    // wait for the log to be synced through what its image may hold: a
    // cycle completes later, and under `never` the writer issues the sync
    // the footer asks for, a round the policy did not issue before.
    // `interval` held still on this seed.
    const HOSTILE_1_3_ALWAYS: u64 = 0x4239_76da_157a_de0b;
    const HOSTILE_1_3_INTERVAL: u64 = 0x46bc_739c_e8c0_e73a;
    const HOSTILE_1_3_NEVER: u64 = 0x2de7_22e2_4d8c_3df1;
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

/// Hostile seeds the sweep caught out, for a reason now written into the
/// model: a shard whose recovery reported its loss came back below the
/// durable point of the process before it, synced nothing before the next
/// crash, and was judged again against that point — the same loss, already
/// reported, counted as an unreported one (535, 633, 955, each crashed
/// twice). The honest node holds on all three.
#[test]
fn the_honest_node_holds_on_the_hostile_seeds_that_once_caught_the_model_out() {
    for sim_seed in [535, 633, 955] {
        let outcome = run_sim(&SimConfig::hostile(1, sim_seed));
        assert!(outcome.invariant_holds(), "seed {sim_seed}: {outcome:?}");
    }
}

/// A hostile seed that caught the node out: a start met a segment header
/// it could not read this once, cut every shard there, removed the next
/// segment — read whole, every record in it cut — and failed before it
/// wrote anything; the next start read the header and replayed a log that
/// ended early, synced records gone with nothing reported (5410, at
/// `always`, crashed once). The honest node holds on it. The draws have
/// moved since it was found and it no longer reaches that shape; the seeds
/// of the next test do.
#[test]
fn the_honest_node_holds_on_the_hostile_seed_whose_start_removed_a_cut_segment() {
    let outcome = run_sim(&SimConfig::hostile(1, 5410));
    assert!(outcome.invariant_holds(), "{outcome:?}");
}

/// Hostile seeds where a start cut shards and then failed before it wrote
/// anything — at the writer's open on 14240 (`interval`), after one crash.
/// Recovery removed the segments whose records it had cut, so the next
/// start, which read what the failed one could not, resumed shards below
/// what was durable and nothing reported it. The segments are kept now, the
/// failed start reports its recovery before it fails, and the honest node
/// holds. That report excuses nothing the next start loses — it wrote no
/// rebase, so its cut could not stand — and without the keep 14240
/// violates. Seed 1 (`never`) reaches a start that failed after it
/// recovered too; it replaced 21840 when a segment's header began to carry
/// its cut points, which moved the disk's draws.
#[test]
fn the_honest_node_holds_on_the_hostile_seeds_whose_start_failed_after_its_cut() {
    for sim_seed in [14240, 1] {
        let outcome = run_sim(&SimConfig::hostile(1, sim_seed));
        assert!(outcome.invariant_holds(), "seed {sim_seed}: {outcome:?}");
        assert!(
            outcome.start_failures > 0 && outcome.recoveries > outcome.crashes,
            "seed {sim_seed} no longer reaches a start that failed after it recovered: {outcome:?}"
        );
    }
}

/// A hostile seed where a start cut every shard and wrote their rebases,
/// and the sync of them failed (6, at `always`, crashed once; 10 until a
/// start began to sync what it recovered, 108 until the writer began to
/// mark the log before every sync, each of which moved the disk's draws).
/// The start reports its recovery and fails; the next one reads those
/// rebases, which make the older records above them dead, and resumes
/// below what was durable. That loss is the one the failed start reported,
/// and the model counts it so: the honest node holds.
#[test]
fn the_honest_node_holds_on_the_hostile_seed_whose_start_could_not_sync_its_rebase() {
    let mut cfg = SimConfig::hostile(1, 6);
    cfg.fsync = FsyncDraw::Fixed(SyncPolicy::ALWAYS);
    let outcome = run_sim(&cfg);
    assert!(outcome.invariant_holds(), "{outcome:?}");
    assert!(
        outcome.rebase_failures_at_start > 0,
        "the seed no longer reaches a failed rebase: {outcome:?}"
    );
}

/// A hostile seed where a start cut, failed writing its rebases, and the
/// node crashed before the retry, so the next start met whatever of those
/// rebases the crash left (4614 at `interval`, found by a search of
/// hostile seeds on 2026-10-20 when a snapshot's footer began to wait for
/// the log and moved the disk's draws off 10215, which a segment's header
/// carrying its cut points had moved off 17619). It either replays what the
/// failed start cut or reports its own cut: no loss goes unreported.
/// `always` had a seed too, 2023, until the writer began to mark the log
/// before every sync: of 1..=30 000 at `always`, 1 546 seeds now fail a rebase, all
/// retried before a crash, none crashed first (read 2026-10-15). The
/// reach: a rebase failed, and no start was a retry without a crash before
/// it — every start after a crash is counted once, and a retry with none
/// between would count one more.
#[test]
fn the_honest_node_holds_when_it_crashes_before_retrying_a_failed_rebase() {
    let mut cfg = SimConfig::hostile(1, 4614);
    cfg.fsync = FsyncDraw::Fixed(SyncPolicy::INTERVAL);
    let outcome = run_sim(&cfg);
    assert!(outcome.invariant_holds(), "{outcome:?}");
    assert!(
        outcome.rebase_failures_at_start > 0 && outcome.recoveries == outcome.crashes,
        "the seed no longer crashes before retrying a failed rebase: {outcome:?}"
    );
}

/// Hostile seeds where a start removed every segment of the generation
/// before it, whose images covered them, and then read the counter wrong:
/// the next generation came from the names on disk, which held only those
/// images (2986501 with workload 299 at `never`, 2919839 with workload 292
/// at `always`, found by the nightly sweep). Counted from segment names
/// alone, the generation was handed out again, the new process's snapshots
/// took the names of the old one's, and a compaction removed records no
/// image held — lost with nothing reported.
#[test]
fn the_honest_node_holds_when_the_counter_fails_to_read_beside_images_alone() {
    for (workload_seed, sim_seed) in [(299, 2_986_501), (292, 2_919_839)] {
        let outcome = run_sim(&SimConfig::hostile(workload_seed, sim_seed));
        assert!(outcome.invariant_holds(), "seed {sim_seed}: {outcome:?}");
    }
}

/// A start whose rebase sync failed leaves the rebase readable and not
/// durable, and the start after it — a retry, with no crash between —
/// reads it, resumes behind it, cuts nothing and writes no rebase of its
/// own. It must make what it read durable before it serves: a crash after
/// its first synced write would otherwise take the rebase back, and the
/// records it made dead would replay over that write, with nothing
/// reported. No hostile seed of 1..=20 000 reaches this shape under any
/// policy, so it is scripted: the failed start's aftermath planted, the
/// real start run over it, one write synced, the host crashed, and the log
/// read again.
#[test]
fn the_honest_node_holds_when_a_start_inherits_a_rebase_its_predecessor_could_not_sync() {
    let cfg = SimConfig::narrow(1, 1);
    let image: Arc<Mutex<Option<Pairs>>> = Arc::default();
    let starts = Arc::new(Mutex::new(0u32));
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(10))
        .rng_seed(1)
        .build();
    sim.host(SERVER, {
        let image = Arc::clone(&image);
        move || {
            let image = Arc::clone(&image);
            let starts = Arc::clone(&starts);
            let cfg = cfg.clone();
            async move {
                let first = {
                    let mut starts = lock(&starts);
                    *starts += 1;
                    *starts == 1
                };
                if first {
                    inherit_an_unsynced_rebase_and_write(&cfg)?;
                    // Parked until the driver crashes the host.
                    tokio::time::sleep(Duration::from_secs(100)).await;
                } else {
                    *lock(&image) = Some(read_shard_zero(&cfg)?);
                }
                Ok(())
            }
        }
    });
    for driver in ["driver", "driver-after-crash"] {
        if driver != "driver" {
            sim.crash(SERVER);
            sim.bounce(SERVER);
        }
        sim.client(driver, async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        });
        sim.run().unwrap();
    }
    let image = lock(&image).clone().expect("the second start read the log");
    let expected: Pairs = [(&b"k0"[..], &b"old"[..]), (b"k1", b"old"), (b"k3", b"new")]
        .iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect();
    assert_eq!(
        image, expected,
        "the write the start acknowledged, and not the record its rebase made dead"
    );
}

/// A shard's keys and values, sorted by key.
type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

/// One record of `shard` carrying `effect`, encoded.
fn encoded(shard: u16, seq: u64, effect: &seedstone_core::log::effect::Effect<'_>) -> Vec<u8> {
    let mut payload = Vec::new();
    effect.encode(&mut payload);
    let mut bytes = Vec::new();
    seedstone_core::log::encode_record(
        &seedstone_core::log::Record {
            shard,
            seq,
            payload: &payload,
        },
        &mut bytes,
    );
    bytes
}

/// The dict seed of the scripted starts: any one, the same for both.
const SCRIPTED_SEED: DictSeed = DictSeed { k0: 1, k1: 2 };

/// Plants what a start whose rebase sync failed leaves — generation 1's
/// records 0–3 of shard 0 synced, generation 2's rebase at 2 written and
/// not synced — runs the node's start over it, and syncs one write of the
/// generation it opened.
fn inherit_an_unsynced_rebase_and_write(cfg: &SimConfig) -> std::io::Result<()> {
    use seedstone_core::log::disk::LogFile;
    use seedstone_core::log::effect::Effect;
    use seedstone_core::log::file::{GENERATION_FILE, create_segment, segment_name};
    let put = |key, value| Effect::Put {
        key,
        value,
        deadline: None,
    };
    let mut shared = Shared::new(cfg);
    // A disk that fails nothing on its own: the failure is planted.
    shared.disk = SimDisk::default();
    let disk = shared.disk.clone();
    let wal = Path::new(DATA_DIR).join("wal");
    disk.create_dir_all(&wal)?;
    disk.sync_dir(Path::new(DATA_DIR))?;
    let mut older = create_segment(&disk, &wal, 1, 0, &[])?;
    for (seq, key) in [(0, &b"k0"[..]), (1, b"k1"), (2, b"k2"), (3, b"k3")] {
        older.write_all(&encoded(0, seq, &put(key, b"old")))?;
    }
    older.sync_data()?;
    let mut failed = create_segment(&disk, &wal, 2, 0, &[(0, 2)])?;
    failed.write_all(&encoded(0, 2, &Effect::Rebase))?;
    disk.write_file(&wal.join(GENERATION_FILE), b"2")?;
    disk.sync_dir(&wal)?;

    let sink = HashSink::new(Arc::new(Mutex::new(TRACE_INIT)), shared.clone());
    let stats = PersistenceStats::new(cfg.executors);
    let (recovery, generation, _opened) = start_log(
        cfg.shards,
        cfg.executors,
        SCRIPTED_SEED,
        None,
        &sink,
        &shared,
        &stats,
    )?;
    assert_eq!(recovery.shards[0].seq, 3, "resumed behind the rebase");
    assert!(!recovery.shards[0].cut, "and cut nothing");
    let mut current = disk.create_append(&wal.join(segment_name(generation, 0)))?;
    current.write_all(&encoded(0, 3, &put(b"k3", b"new")))?;
    current.sync_data()
}

/// Recovers the log and returns shard 0's image, asserting that recovery
/// reported no loss of it: whatever it holds is what an operator is told
/// the node kept.
fn read_shard_zero(cfg: &SimConfig) -> std::io::Result<Pairs> {
    let disk = SimDisk::default();
    let recovery = recover(RecoverSpec {
        disk: &disk,
        wal: &Path::new(DATA_DIR).join("wal"),
        shards: cfg.shards,
        reader: ReaderMode::Resynchronising,
        trust_unfinished: false,
        seed: SCRIPTED_SEED,
        now: seedstone_core::shard::Now {
            instant: Instant::now(),
            unix_millis: 0,
        },
    })?;
    let shard = &recovery.shards[0];
    assert!(!shard.lossy && !shard.cut, "{shard:?}");
    let mut pairs = Vec::new();
    let mut cursor = 0;
    loop {
        cursor = shard.dict.scan(cursor, |key, entry| {
            pairs.push((key.to_vec(), entry.value.to_vec()));
        });
        if cursor == 0 {
            break;
        }
    }
    pairs.sort();
    Ok(pairs)
}

/// A cut shard is reported one past the position recovery resumed it at,
/// because its `Rebase` was written there first. The check judges the
/// resume, not the `Rebase`: a cut shard resumed at its durable point lost
/// that record, and a clean report says nobody was told.
#[test]
fn a_cut_shard_resumed_at_its_durable_point_lost_that_record() {
    let judged = |next_seq: u64, cut: bool| {
        let shared = Shared::new(&SimConfig::mini(1, 42));
        lock(&shared.crashes).push(CrashRecord {
            at: Duration::ZERO,
            durable: vec![Some((5, Duration::ZERO)); usize::from(shared.shards)],
        });
        let sink = HashSink::new(Arc::new(Mutex::new(TRACE_INIT)), shared.clone());
        seedstone_core::shard::TraceSink::recovered(&sink, 0, next_seq, false, cut);
        let tally = *lock(&shared.tally);
        (tally.lost_durable_prefixes, tally.unreported_losses)
    };
    // Resumed at 5, its `Rebase` written there: record 5 is gone.
    assert_eq!(judged(6, true), (1, 1));
    // Resumed at 6, past the durable point: nothing durable lost.
    assert_eq!(judged(7, true), (0, 0));
    // Not cut: serving from 6 is resuming at 6.
    assert_eq!(judged(6, false), (0, 0));
    assert_eq!(judged(5, false), (1, 1));
}

/// The records a start replays past the last sync, out of a crash's tear,
/// are durable once it syncs the files it read them from: the next crash
/// owes them. A start that failed before that sync owes nothing new.
#[test]
fn a_start_that_synced_what_it_read_owes_it_to_the_next_crash() {
    let judged = |reached: Option<Reached>| {
        let shared = Shared::new(&SimConfig::mini(1, 42));
        let crash = |shared: &Shared| {
            let durable = lock(&shared.durable).clone();
            lock(&shared.crashes).push(CrashRecord {
                at: Duration::ZERO,
                durable,
            });
        };
        lock(&shared.durable)[0] = Some((5, Duration::ZERO));
        crash(&shared);
        let sink = HashSink::new(Arc::new(Mutex::new(TRACE_INIT)), shared.clone());
        // Records 6 to 9 survived the tear and were replayed; the shard was
        // cut behind it, its `Rebase` at 10.
        match reached {
            Some(reached) => sink.failed_start(0, 10, false, reached),
            None => seedstone_core::shard::TraceSink::recovered(&sink, 0, 11, false, true),
        }
        crash(&shared);
        // The next start reads a hole inside them and calls it a tear.
        seedstone_core::shard::TraceSink::recovered(&sink, 0, 9, false, true);
        let tally = *lock(&shared.tally);
        (tally.lost_durable_prefixes, tally.unreported_losses)
    };
    assert_eq!(judged(None), (1, 1), "served");
    assert_eq!(judged(Some(Reached::Rebasing)), (1, 1));
    assert_eq!(judged(Some(Reached::Synced)), (1, 1));
    assert_eq!(judged(Some(Reached::Read)), (0, 0), "never synced");
}
