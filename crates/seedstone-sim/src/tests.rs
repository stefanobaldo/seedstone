use super::*;

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
    const MINI_1_42: u64 = 0x959f_0105_262d_501d;

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
        (51, 32, 149, 32),
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
            | Plant::EvictsBelowCeiling => assert_eq!(
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
        }
    }
    // The place is a string, so nothing but this stops it outliving the
    // file it names — and a warning pointing at a path that is not there
    // is worse than no warning.
    for path in [
        concat!(env!("CARGO_MANIFEST_DIR"), "/../seedstone-core/src/dict.rs"),
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/planted_eviction.rs"),
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/planted_crossing.rs"),
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
fn the_plants_the_swept_shapes_cannot_catch_are_the_three_that_need_a_shape() {
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
            "crossing-skips-shard"
        ],
        "the plants a swept violation count says nothing about have changed"
    );
}
