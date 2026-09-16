use super::*;
// The honest policy, which is what every expiry test here is about: these
// tests are the dict's, and a defective policy is the simulator's business.
use crate::shard::Deadlines;
use std::time::Duration;

fn seed() -> DictSeed {
    DictSeed { k0: 7, k1: 11 }
}

/// An entry with no deadline — what every test in this module stores. The
/// dict never reads that field, so a deadline here would exercise nothing;
/// what a deadline means is the shard's, and is tested there.
fn entry(value: &[u8]) -> Entry {
    Entry {
        value: value.to_vec(),
        expires_at: None,
        touched: 0,
    }
}

/// The value stored under `key`, or `None` if the key is absent.
fn value<'a>(d: &'a Dict, key: &[u8]) -> Option<&'a [u8]> {
    d.get(key).map(|entry| entry.value.as_slice())
}

/// Inserts ascending numeric keys until a rehash is in flight, and returns
/// how many were inserted. Panics rather than looping forever if growth
/// never triggers.
fn fill_until_rehashing(d: &mut Dict) -> usize {
    let mut count = 0usize;
    while !d.is_rehashing() {
        d.insert(
            count.to_string().into_bytes(),
            entry(count.to_string().as_bytes()),
        );
        count += 1;
        assert!(count < 1_000, "the dict never started rehashing");
    }
    count
}

#[test]
fn hash_key_matches_siphash_1_3() {
    // Golden vectors: these must never change. A replayed simulation and a
    // production node have to agree on where a key lands forever, so a
    // `siphasher` upgrade that altered its output — or a change to what
    // bytes get fed to the hasher — would silently invalidate every seed
    // in existence. Nothing else in this file would notice: every other
    // assertion here is "the value I put in is the value I get back",
    // which any hash function satisfies, seeded or not.
    //
    // Derived independently from a from-definition SipHash written against
    // the specification, not by printing what `hash_key` returns. That
    // reference was itself validated twice: at c=2, d=4 it reproduces the
    // published SipHash-2-4 test vectors (key 000102..0f, message
    // 00..len-1) for lengths 0 through 7, and it agrees with
    // `std::hash::SipHasher` — a third, unrelated implementation — over
    // every length from 0 to 200, which covers the multi-block path the
    // short published vectors never reach. Only the round counts differ
    // between that and SipHash-1-3.
    //
    // Do not "fix" a mismatch here by pasting in whatever `hash_key`
    // currently returns.
    let reference_key = DictSeed {
        k0: 0x0706_0504_0302_0100,
        k1: 0x0f0e_0d0c_0b0a_0908,
    };
    assert_eq!(hash_key(reference_key, b""), 0xabac_0158_050f_c4dc);
    assert_eq!(hash_key(reference_key, b"a"), 0x1c26_97ab_786a_6237);
    assert_eq!(hash_key(reference_key, b"seedstone"), 0xd6ef_d9bd_3ebb_a979);

    // And under the seed this module's tests use, including a key long
    // enough to take the multi-block path.
    let ramp: Vec<u8> = (0..100u8).collect();
    assert_eq!(hash_key(seed(), b""), 0xb8f9_c5c6_7c08_d736);
    assert_eq!(hash_key(seed(), b"key:1"), 0xa3e9_6ed8_b4a0_a1f7);
    assert_eq!(hash_key(seed(), &ramp), 0xbb82_314a_8b08_5307);
}

#[test]
fn a_different_seed_hashes_and_places_keys_differently() {
    let a = seed();
    let b = DictSeed { k0: 11, k1: 7 };
    let keys: [&[u8]; 4] = [b"", b"a", b"key:1", b"seedstone"];

    // A hash that ignored its seed would make every one of these equal,
    // and the whole dict would still round-trip every key it was given.
    for key in keys {
        assert_ne!(hash_key(a, key), hash_key(b, key), "key {key:?}");
    }
    // The difference has to reach bucket placement, not just the hash
    // word: placement is what a replay actually depends on.
    assert!(
        keys.iter().any(|key| {
            bucket_index(hash_key(a, key), INITIAL_BUCKETS)
                != bucket_index(hash_key(b, key), INITIAL_BUCKETS)
        }),
        "the two seeds put every key in the same bucket"
    );
}

/// The deadline flag's accounting, over every operation that can move it.
///
/// A shard skips its whole liveness check when this reads `false`, so a
/// `false` that should have been `true` is an expiry that never happens.
/// Both ways in are covered here, and both are inside this type — a caller
/// cannot reach an `Entry` mutably, so there is no third way to check.
#[tokio::test(start_paused = true)]
async fn a_dict_knows_when_no_entry_can_be_carrying_a_deadline() {
    let mut d = Dict::with_seed(seed());
    assert!(!d.may_hold_deadlines(), "a fresh dict holds nothing");

    // Entries without deadlines leave it alone: this is the state nearly
    // every keyspace stays in, and the one the fast path is for.
    d.insert(b"plain".to_vec(), entry(b"v"));
    d.insert(b"other".to_vec(), entry(b"v"));
    assert!(!d.may_hold_deadlines());

    // Way in the first: an inserted entry that carries one.
    d.insert(
        b"dated".to_vec(),
        Entry {
            value: b"v".to_vec(),
            expires_at: Some(Instant::now()),
            touched: 0,
        },
    );
    assert!(d.may_hold_deadlines());

    // Removing the dated entry does not clear it — the flag is allowed to
    // be wrong only in the direction that costs a lookup.
    assert!(d.remove(b"dated").is_some());
    assert!(d.may_hold_deadlines());

    // Emptying the dict does clear it: nothing left to carry one.
    assert!(d.remove(b"plain").is_some());
    assert!(d.remove(b"other").is_some());
    assert!(d.is_empty());
    assert!(!d.may_hold_deadlines());

    // Way in the second: a deadline put on a key already stored.
    d.insert(b"plain".to_vec(), entry(b"v"));
    assert!(!d.may_hold_deadlines());
    assert!(d.set_deadline(b"plain", Some(Instant::now())));
    assert!(d.may_hold_deadlines());
    assert!(
        d.get(b"plain").and_then(|e| e.expires_at).is_some(),
        "set_deadline did not reach the entry"
    );

    // Clearing a deadline leaves the flag up, for the same reason removing
    // the entry did.
    assert!(d.set_deadline(b"plain", None));
    assert!(d.get(b"plain").expect("still stored").expires_at.is_none());
    assert!(d.may_hold_deadlines());

    // And a deadline aimed at a key that is not there reports the miss.
    assert!(!d.set_deadline(b"absent", Some(Instant::now())));
}

/// `set_deadline` has to reach an entry wherever the rehash left it, the
/// old table included — the same both-tables rule every other lookup
/// follows. A version that only looked in the surviving table would leave
/// half the keyspace unable to take a deadline mid-rehash.
#[tokio::test(start_paused = true)]
async fn set_deadline_finds_a_key_in_either_table() {
    let mut d = Dict::with_seed(seed());
    let inserted = fill_until_rehashing(&mut d);
    assert!(d.is_rehashing());

    // Key 0 predates the growth, so it can only be in the old table; the
    // last key inserted went straight into the new one.
    let newest = (inserted - 1).to_string().into_bytes();
    assert!(d.set_deadline(b"0", Some(Instant::now())));
    assert!(d.set_deadline(&newest, Some(Instant::now())));
    assert!(d.get(b"0").expect("key 0").expires_at.is_some());
    assert!(d.get(&newest).expect("newest key").expires_at.is_some());

    // And the deadlines survive the migration that follows.
    drain_rehash(&mut d);
    assert!(d.get(b"0").expect("key 0").expires_at.is_some());
    assert!(d.get(&newest).expect("newest key").expires_at.is_some());
}

#[test]
fn insert_get_remove_round_trip() {
    let mut d = Dict::with_seed(seed());
    d.insert(b"a".to_vec(), entry(b"1"));
    assert_eq!(value(&d, b"a"), Some(&b"1"[..]));
    d.insert(b"a".to_vec(), entry(b"2")); // overwrite, len stays 1
    assert_eq!((d.len(), value(&d, b"a")), (1, Some(&b"2"[..])));
    assert_eq!(d.remove(b"a").map(|e| e.value), Some(b"2".to_vec()));
    assert_eq!((d.len(), value(&d, b"a")), (0, None));
}

#[test]
fn survives_growth_through_many_inserts() {
    let mut d = Dict::with_seed(seed());
    for i in 0..10_000u32 {
        d.insert(i.to_string().into_bytes(), entry(i.to_string().as_bytes()));
    }
    while d.is_rehashing() {
        d.rehash_step(16);
    }
    assert_eq!(d.len(), 10_000);
    for i in (0..10_000u32).step_by(97) {
        assert_eq!(
            value(&d, i.to_string().as_bytes()),
            Some(i.to_string().as_bytes())
        );
    }
}

#[test]
fn a_fresh_dict_is_empty_and_not_rehashing() {
    let d = Dict::with_seed(seed());
    assert_eq!(d.len(), 0);
    assert!(d.is_empty());
    assert!(!d.is_rehashing());
    assert_eq!(value(&d, b"absent"), None);
}

#[test]
fn removing_an_absent_key_reports_it_and_leaves_len_alone() {
    let mut d = Dict::with_seed(seed());
    d.insert(b"present".to_vec(), entry(b"v"));
    assert!(d.remove(b"absent").is_none());
    assert_eq!(d.len(), 1);
}

#[test]
fn lookups_see_both_tables_while_rehashing() {
    let mut d = Dict::with_seed(seed());
    let inserted = fill_until_rehashing(&mut d);

    // Entries are spread across the two tables at this point: everything
    // written before the growth sits in the old one, the last insert in
    // the new one. Reads must not care which.
    assert!(d.is_rehashing());
    for i in 0..inserted {
        assert_eq!(
            value(&d, i.to_string().as_bytes()),
            Some(i.to_string().as_bytes()),
            "key {i} went missing mid-rehash"
        );
    }
    assert_eq!(d.len(), inserted);
    assert!(d.is_rehashing(), "a lookup must not advance the rehash");
}

#[test]
fn removes_reach_into_the_old_table_while_rehashing() {
    let mut d = Dict::with_seed(seed());
    let inserted = fill_until_rehashing(&mut d);

    // Key 0 predates the growth, so it can only be in the old table.
    assert_eq!(d.remove(b"0").map(|e| e.value), Some(b"0".to_vec()));
    assert_eq!(value(&d, b"0"), None);
    assert_eq!(d.len(), inserted - 1);

    // And it stays gone once the rehash completes: the migration must not
    // resurrect it from a bucket it was never removed from.
    drain_rehash(&mut d);
    assert_eq!(value(&d, b"0"), None);
    assert_eq!(d.len(), inserted - 1);
}

#[test]
fn overwriting_during_a_rehash_does_not_duplicate_the_entry() {
    let mut d = Dict::with_seed(seed());
    let inserted = fill_until_rehashing(&mut d);

    // Key 0 lives in the old table; the overwrite must update it in place
    // rather than leave a second copy in the new one.
    d.insert(b"0".to_vec(), entry(b"overwritten"));
    assert_eq!(d.len(), inserted);
    assert_eq!(value(&d, b"0"), Some(&b"overwritten"[..]));

    drain_rehash(&mut d);
    assert_eq!(d.len(), inserted);
    assert_eq!(value(&d, b"0"), Some(&b"overwritten"[..]));
    // A duplicate would survive the first removal and still answer reads.
    assert_eq!(
        d.remove(b"0").map(|e| e.value),
        Some(b"overwritten".to_vec())
    );
    assert_eq!(value(&d, b"0"), None);
}

#[test]
fn rehashing_ends_when_the_old_table_drains() {
    let mut d = Dict::with_seed(seed());
    let inserted = fill_until_rehashing(&mut d);

    let steps = drain_rehash(&mut d);
    assert!(
        steps > 0,
        "the rehash was already over before it was driven"
    );
    assert!(!d.is_rehashing());
    assert_eq!(d.len(), inserted);
    for i in 0..inserted {
        assert_eq!(
            value(&d, i.to_string().as_bytes()),
            Some(i.to_string().as_bytes()),
            "key {i} was lost by the migration"
        );
    }

    // Stepping a dict that is not rehashing is a no-op, not a panic.
    d.rehash_step(64);
    assert!(!d.is_rehashing());
    assert_eq!(d.len(), inserted);
}

/// Drives a rehash to completion one bucket at a time, returning how many
/// steps it took. Panics rather than looping forever if it never ends.
fn drain_rehash(d: &mut Dict) -> usize {
    let mut steps = 0;
    while d.is_rehashing() {
        d.rehash_step(1);
        steps += 1;
        assert!(steps < 10_000, "the rehash never finished");
    }
    steps
}

#[test]
fn a_rehash_step_larger_than_what_is_left_finishes_it_without_running_past() {
    let mut d = Dict::with_seed(seed());
    let inserted = fill_until_rehashing(&mut d);

    d.rehash_step(usize::MAX);
    assert!(!d.is_rehashing());
    assert_eq!(d.len(), inserted);
    for i in 0..inserted {
        assert_eq!(
            value(&d, i.to_string().as_bytes()),
            Some(i.to_string().as_bytes())
        );
    }
}

#[test]
fn contents_do_not_depend_on_insertion_order() {
    // Two dicts holding the same entries, reached by different operation
    // sequences — ascending versus descending, and every key overwritten
    // once — agree on every key. This says nothing about the seed: it
    // would pass just as well with two different ones. What it pins down
    // is that an overwrite is idempotent and that the order writes arrive
    // in cannot change what the dict answers.
    let mut a = Dict::with_seed(seed());
    let mut b = Dict::with_seed(seed());
    for i in 0..200u32 {
        a.insert(i.to_string().into_bytes(), entry(i.to_string().as_bytes()));
    }
    for i in (0..200u32).rev() {
        b.insert(i.to_string().into_bytes(), entry(b"stale"));
        b.insert(i.to_string().into_bytes(), entry(i.to_string().as_bytes()));
    }
    assert_eq!(a.len(), b.len());
    for i in 0..200u32 {
        assert_eq!(
            value(&a, i.to_string().as_bytes()),
            value(&b, i.to_string().as_bytes()),
            "key {i}"
        );
    }
}

#[test]
fn interleaved_inserts_and_removes_keep_len_and_contents_consistent() {
    use std::collections::BTreeSet;

    let mut d = Dict::with_seed(seed());
    let mut expected = BTreeSet::new();
    for i in 0..2_000u32 {
        let key = i.to_string().into_bytes();
        d.insert(key.clone(), entry(i.to_string().as_bytes()));
        expected.insert(key);
        // Remove an older key every third insert, so the table shrinks and
        // grows while rehashes are in flight.
        if i % 3 == 2 {
            let victim = (i / 3).to_string().into_bytes();
            assert_eq!(
                d.remove(&victim).is_some(),
                expected.remove(&victim),
                "key {} disagreed on removal",
                i / 3
            );
        }
    }
    assert_eq!(d.len(), expected.len());
    for key in &expected {
        assert_eq!(value(&d, key), Some(key.as_slice()), "key {key:?}");
    }
    for i in 0..2_000u32 {
        let key = i.to_string().into_bytes();
        if !expected.contains(&key) {
            assert_eq!(value(&d, &key), None, "key {i} should be gone");
        }
    }

    // Draining what is left by removal must bring the dict back to empty.
    // A `len` that drifted anywhere above would surface here.
    for key in &expected {
        assert!(
            d.remove(key).is_some(),
            "key {key:?} vanished before its removal"
        );
    }
    assert_eq!(d.len(), 0);
    assert!(d.is_empty());
}

#[test]
fn scan_visits_every_key_when_static() {
    let mut d = Dict::with_seed(seed());
    for i in 0..500u32 {
        d.insert(i.to_string().into_bytes(), entry(b""));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut c = 0;
    let mut steps = 0;
    loop {
        c = d.scan(c, |k, _| {
            seen.insert(k.to_vec());
        });
        if c == 0 {
            break;
        }
        // Purely a guard, not an acceptance criterion: the failure this
        // test exists to catch includes a cursor that never comes back to
        // 0, and without a bound that failure wedges the process instead of
        // reporting itself.
        steps += 1;
        assert!(steps < 10_000, "the cursor never returned to 0");
    }
    assert_eq!(seen.len(), 500);
}

#[test]
fn scan_sees_every_stable_key_across_growth() {
    // Keys inserted before the scan starts and never removed must be visited
    // at least once even though the table grows (and rehashes) mid-scan.
    let mut d = Dict::with_seed(seed());
    for i in 0..64u32 {
        d.insert(format!("stable-{i}").into_bytes(), entry(b""));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut c = 0;
    let mut extra = 0u32;
    let mut steps = 0;
    loop {
        c = d.scan(c, |k, _| {
            seen.insert(k.to_vec());
        });
        if c == 0 {
            break;
        }
        // As above, a guard rather than an assertion about the keyspace:
        // this is the test a cursor outrun by a growing table hangs in.
        steps += 1;
        assert!(steps < 10_000, "the cursor never returned to 0");

        for _ in 0..8 {
            d.insert(format!("noise-{extra}").into_bytes(), entry(b""));
            extra += 1;
        }
        d.rehash_step(1);
    }
    for i in 0..64u32 {
        assert!(
            seen.contains(format!("stable-{i}").as_bytes()),
            "lost stable-{i}"
        );
    }
}

/// The order's necessity, which the growth test above only assumes.
///
/// `scan_sees_every_stable_key_across_growth` shows that the honest cursor
/// finishes a cycle over a table doubling underneath it. That it *has to be
/// this order* is a separate claim, and this is where it is proved: the
/// same loop, the same growth, a cursor that counts buckets upwards instead
/// — and it never comes back.
///
/// The arithmetic is the whole argument. An upward cursor advances one
/// bucket a step while a doubling moves the finish line by the width of the
/// table, so a keyspace growing by more than one bucket a step outruns it
/// and the cycle has no end. Reverse binary inverts that: the bits a
/// doubling adds are the *high* ones, which the cursor has already passed,
/// so a doubling halves the size of every remaining step rather than
/// doubling how many are left.
///
/// Here rather than in the simulator, and that is a change from where it
/// used to be. The harness planted this cursor and walked a shape narrow
/// enough to catch it, back when a client's `COUNT` of one meant one bucket
/// a call. A `SCAN` call now spends a bucket ceiling of the server's own,
/// which covers a simulated shard's whole table several times over before
/// it answers — so nothing the harness can afford to run leaves a cursor in
/// flight long enough for a table to double under it. The shape that would
/// is a production-sized one: a table of millions of buckets, where a
/// call's ceiling is a rounding error against the width of the walk. What
/// the simulator cannot afford, one dict and no network can.
#[test]
fn an_upward_cursor_is_outrun_by_a_table_growing_under_it() {
    /// The defect: buckets in ascending order, masked like the honest one.
    #[derive(Clone)]
    struct Upward;
    impl WalkOrder for Upward {
        fn advance(&self, cursor: u64, mask: u64) -> u64 {
            cursor.wrapping_add(1) & mask
        }
    }

    // The growth rate of the test above, and the bound is that test's
    // guard: whatever number is generous enough to call the honest cursor
    // hung is more than generous enough to call this one outrun.
    let mut d = Dict::with_seed(seed());
    for i in 0..64u32 {
        d.insert(format!("stable-{i}").into_bytes(), entry(b""));
    }
    let mut c = 0;
    let mut extra = 0u32;
    for _ in 0..10_000 {
        c = d.scan_in_order(c, &Upward, |_, _| {});
        assert_ne!(
            c, 0,
            "an upward cursor finished a cycle over a table growing under it"
        );
        for _ in 0..8 {
            d.insert(format!("noise-{extra}").into_bytes(), entry(b""));
            extra += 1;
        }
        d.rehash_step(1);
    }
}

#[test]
fn scanning_an_empty_dict_ends_the_cycle_without_visiting_anything() {
    let d = Dict::with_seed(seed());
    // Not "returns 0 eventually": an empty keyspace must cost one call, not
    // one call per bucket, or a node full of empty shards would answer a
    // sweep with a long run of empty replies.
    assert_eq!(
        d.scan(0, |k, _| panic!("visited {k:?} in an empty dict")),
        0
    );
}

#[test]
fn a_full_cycle_at_rest_visits_every_bucket_exactly_once() {
    use std::collections::BTreeSet;

    let mut d = Dict::with_seed(seed());
    for i in 0..100u32 {
        d.insert(i.to_string().into_bytes(), entry(b""));
    }
    drain_rehash(&mut d);

    // The keys alone cannot show this: several share a bucket and some
    // buckets are empty, so a cursor that skipped or repeated a bucket
    // could still deliver every key. Read the buckets directly.
    let buckets = d.old.len();
    let mask = u64::try_from(buckets).expect("a bucket count is a usize") - 1;
    let mut visited = Vec::new();
    let mut c = 0;
    loop {
        visited.push(usize::try_from(c & mask).expect("a masked cursor fits a bucket index"));
        c = d.scan(c, |_, _| {});
        if c == 0 {
            break;
        }
        assert!(visited.len() <= buckets, "the cursor never returned to 0");
    }

    assert_eq!(visited.len(), buckets, "wrong number of steps in a cycle");
    let distinct: BTreeSet<usize> = visited.iter().copied().collect();
    assert_eq!(distinct.len(), buckets, "a bucket repeated: {visited:?}");
}

#[test]
fn a_cursor_cycle_visits_every_bucket_exactly_once_and_ends_at_zero() {
    // Every mask width small enough to enumerate, which is not the same as
    // every width a dict reaches: a table starts at `INITIAL_BUCKETS` and
    // only ever doubles, so the first few widths here are unreachable and
    // there is no ceiling at the last one. That mismatch is the point. The
    // claim is about the arithmetic, and the arithmetic does not know how
    // big the table is beyond its mask.
    //
    // What it does not claim: a cursor that counted plainly in binary order
    // satisfies every assertion below, because over a mask that never
    // changes plain counting also lands on each bucket once and wraps to 0.
    // The property the reverse increment exists for is ordering under a
    // mask that grows mid-cycle, and that is
    // `a_cycle_under_continuous_growth_converges_instead_of_chasing_the_table`.
    for power in 0..12u32 {
        let buckets = 1usize << power;
        let mask = u64::try_from(buckets).expect("a bucket count is a usize") - 1;
        let mut seen = vec![0u32; buckets];
        let mut cursor = 0u64;
        let mut steps = 0usize;
        loop {
            let index =
                usize::try_from(cursor & mask).expect("a masked cursor fits a bucket index");
            seen[index] += 1;
            cursor = reverse_increment(cursor, mask);
            steps += 1;
            // A guard rather than an acceptance criterion — the criterion
            // is the `assert_eq!` below. Without it, an increment whose
            // cycle never closes would spin here rather than report itself.
            assert!(
                steps <= buckets,
                "a cycle over {buckets} buckets took {steps} steps without closing"
            );
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(steps, buckets, "cycle length for {buckets} buckets");
        for (bucket, &visits) in seen.iter().enumerate() {
            assert_eq!(
                visits, 1,
                "bucket {bucket} of {buckets} was visited {visits} times"
            );
        }
    }
}

#[test]
fn a_cycle_spanning_the_end_of_a_rehash_still_sees_every_stable_key() {
    use std::collections::BTreeSet;

    let mut d = Dict::with_seed(seed());
    let inserted = fill_until_rehashing(&mut d);
    assert!(
        d.is_rehashing(),
        "the cycle must start with two tables live"
    );

    let mut seen = BTreeSet::new();
    let mut c = 0;
    let mut steps = 0;
    let mut finished_mid_cycle = false;
    loop {
        c = d.scan(c, |k, _| {
            seen.insert(k.to_vec());
        });
        if c == 0 {
            break;
        }
        steps += 1;
        // Collapse the two tables into one part-way through, so the rest of
        // the cycle runs against the larger table alone with a cursor that
        // was produced while both were live.
        if steps == 2 {
            drain_rehash(&mut d);
            finished_mid_cycle = true;
        }
        assert!(steps < 10_000, "the cursor never returned to 0");
    }

    assert!(finished_mid_cycle, "the cycle ended before the rehash did");
    assert!(!d.is_rehashing());
    for i in 0..inserted {
        assert!(seen.contains(i.to_string().as_bytes()), "lost key {i}");
    }
}

#[test]
fn the_growth_guarantee_holds_wherever_in_the_cycle_the_growth_starts() {
    use std::collections::BTreeSet;

    // The tests above each exercise one interleaving of growth and cursor,
    // and a cursor that advanced in plain binary order would survive some
    // of them. Sweep every position in the cycle at which the table can
    // start doubling and demand the guarantee at each one.
    let mut d = Dict::with_seed(seed());
    for i in 0..100u32 {
        d.insert(format!("stable-{i}").into_bytes(), entry(b""));
    }
    drain_rehash(&mut d);
    let cycle = d.old.len();

    for grow_at in 0..cycle {
        let mut d = Dict::with_seed(seed());
        for i in 0..100u32 {
            d.insert(format!("stable-{i}").into_bytes(), entry(b""));
        }
        drain_rehash(&mut d);
        assert_eq!(d.old.len(), cycle);

        let mut seen = BTreeSet::new();
        let mut c = 0;
        let mut step = 0usize;
        let mut noise = 0u32;
        loop {
            if step == grow_at {
                // Write until the table starts doubling, exactly here.
                while !d.is_rehashing() {
                    d.insert(format!("noise-{noise}").into_bytes(), entry(b""));
                    noise += 1;
                }
            }
            c = d.scan(c, |k, _| {
                seen.insert(k.to_vec());
            });
            if c == 0 {
                break;
            }
            // Let the migration run alongside the rest of the cycle, so the
            // cursor also crosses the moment the two tables collapse back
            // into one.
            d.rehash_step(3);
            step += 1;
            assert!(step < 10_000, "the cursor never returned to 0");
        }

        assert!(noise > 0, "the table never grew (grow_at {grow_at})");
        for i in 0..100u32 {
            assert!(
                seen.contains(format!("stable-{i}").as_bytes()),
                "lost stable-{i} when growth started at step {grow_at}"
            );
        }
    }
}

#[test]
fn a_cycle_under_continuous_growth_converges_instead_of_chasing_the_table() {
    // Coverage is not the only thing the traversal order buys, and on a
    // dict that only ever grows it is not the sharpest test of it: a cursor
    // that counted its bucket bits in plain binary order would still reach
    // every bucket, because a bucket that splits when the table doubles
    // splits into one index the cursor has passed and one still ahead of
    // it. What such a cursor loses is *termination*, and with it the whole
    // point of handing the value back to a client: it advances one bucket
    // per call, so a table that doubles faster than that outruns it and the
    // cycle never ends.
    //
    // What makes the cycle finite is that a step is a fixed *fraction* of
    // the keyspace rather than a fixed number of buckets: a step under mask
    // `m` moves the cursor forward by exactly `1 / (m + 1)` of the whole,
    // and doubling the table only makes later steps smaller. Read that
    // position off the cursor — it is the cursor's bits in reverse — and
    // demand it strictly increase. A cursor that moved through the keyspace
    // in any other order fails on the second call instead of hanging.
    let mut d = Dict::with_seed(seed());
    for i in 0..64u32 {
        d.insert(format!("stable-{i}").into_bytes(), entry(b""));
    }
    let started_over = d.new.as_ref().map_or(d.old.len(), Vec::len);

    let mut c = 0;
    let mut position = 0u64;
    let mut steps = 0usize;
    let mut noise = 0u32;
    loop {
        c = d.scan(c, |_, _| {});
        if c == 0 {
            break;
        }
        let next = c.reverse_bits();
        assert!(
            next > position,
            "the cursor moved backwards through the keyspace: \
             {position} then {next}"
        );
        position = next;

        steps += 1;
        // Monotone progress does not by itself say the cursor ever lands
        // back on 0 — a cursor whose steps shrink faster than the table
        // grows would advance forever without wrapping. Stop it here, so
        // that failure is a failure and not a hung suite. A correct cycle
        // takes a few hundred steps.
        assert!(steps < 10_000, "the cycle never came back to cursor 0");

        for _ in 0..8 {
            d.insert(format!("noise-{noise}").into_bytes(), entry(b""));
            noise += 1;
        }
        d.rehash_step(1);
    }

    // Monotone progress alone would allow a cycle that crawls. Since every
    // step is `1 / (m + 1)` of the keyspace and `m` only ever grows, a
    // whole cycle costs no more steps than the table it ends on has
    // buckets, however much the table grew along the way.
    let ended_over = d.new.as_ref().map_or(d.old.len(), Vec::len);
    assert!(ended_over > started_over, "the table never grew");
    assert!(
        steps <= ended_over,
        "a cycle over {ended_over} buckets took {steps} steps"
    );
}

/// An entry whose deadline is `expires_at`.
fn dated(value: &[u8], expires_at: Instant) -> Entry {
    Entry {
        value: value.to_vec(),
        expires_at: Some(expires_at),
        touched: 0,
    }
}

/// Sweeps from `cursor` to the end of the cycle at `budget` buckets a call
/// and returns every key reported dead, in the order the cursor reached
/// them.
fn sweep_from(d: &Dict, cursor: u64, budget: usize, now: Instant) -> Vec<Vec<u8>> {
    let mut dead = Vec::new();
    let mut cursor = cursor;
    let mut steps = 0;
    loop {
        let (next, mut batch) = d.expire_step(cursor, budget, now, &Deadlines);
        dead.append(&mut batch);
        cursor = next;
        if cursor == 0 {
            break;
        }
        steps += 1;
        assert!(steps < 10_000, "the sweep's cursor never returned to 0");
    }
    dead
}

#[test]
fn the_sweep_never_eats_the_living() {
    use std::collections::BTreeSet;

    // Built forwards from one reading rather than backwards from `now`, so
    // no arithmetic here can leave `Instant`'s range.
    let start = Instant::now();
    let (past, now, future) = (
        start,
        start + Duration::from_secs(1),
        start + Duration::from_mins(1),
    );

    // Half the keyspace past its deadline, a quarter still ahead of one,
    // a quarter carrying none at all. The two survivor kinds are different
    // failures: a sweep that ignored the deadline would take the future
    // one, and a sweep that treated "no deadline" as "expired at zero"
    // would take the other.
    let mut d = Dict::with_seed(seed());
    let mut expired = BTreeSet::new();
    let mut alive = BTreeSet::new();
    for i in 0..200u32 {
        let key = format!("k{i}").into_bytes();
        match i % 4 {
            0 | 1 => {
                d.insert(key.clone(), dated(b"v", past));
                expired.insert(key);
            }
            2 => {
                d.insert(key.clone(), entry(b"v"));
                alive.insert(key);
            }
            _ => {
                d.insert(key.clone(), dated(b"v", future));
                alive.insert(key);
            }
        }
    }

    let dead = sweep_from(&d, 0, 4, now);
    assert_eq!(
        dead.len(),
        expired.len(),
        "the sweep reported a key twice or missed one"
    );
    assert_eq!(
        dead.iter().cloned().collect::<BTreeSet<_>>(),
        expired,
        "the sweep did not report exactly the keys whose deadline had passed"
    );

    // And what it reported is what a caller can remove: the survivors are
    // all still there afterwards, and nothing else is.
    for key in &dead {
        assert!(
            d.remove(key).is_some(),
            "key {key:?} was not there to remove"
        );
    }
    assert_eq!(d.len(), alive.len());
    for key in &alive {
        assert!(d.get(key).is_some(), "the sweep ate {key:?}");
    }
}

/// The fast path the flag exists for, asserted where it is observable: a
/// dict that can hold no deadline hands the cursor straight back, so the
/// sweep costs a keyspace without expiries one branch per tick rather than
/// a walk of its table.
#[test]
fn a_dict_that_holds_no_deadline_is_not_walked_at_all() {
    let now = Instant::now();
    let mut d = Dict::with_seed(seed());
    for i in 0..100u32 {
        d.insert(format!("k{i}").into_bytes(), entry(b"v"));
    }

    // A walked cursor cannot come back where it started: a step always
    // moves it. Two starting points, because one of them could be the
    // cursor a completed cycle happens to return.
    assert_eq!(d.expire_step(0, 4, now, &Deadlines), (0, Vec::new()));
    assert_eq!(
        d.expire_step(1 << 63, 4, now, &Deadlines),
        (1 << 63, Vec::new())
    );

    // And it is skipped work, not lost work: one dated entry and the walk
    // happens again.
    d.insert(b"dated".to_vec(), dated(b"v", now));
    assert_ne!(
        d.expire_step(0, 4, now, &Deadlines).0,
        0,
        "a dict holding a deadline was not walked"
    );
}

#[test]
fn the_sweep_respects_its_budget() {
    use std::collections::BTreeSet;

    // Fifty entries over a table that has settled at sixty-four buckets, so
    // a budget of four is a small fraction of a cycle and several of the
    // buckets it covers are empty.
    let past = Instant::now();
    let now = past + Duration::from_secs(1);
    let mut d = Dict::with_seed(seed());
    for i in 0..50u32 {
        d.insert(format!("k{i}").into_bytes(), dated(b"v", past));
    }
    drain_rehash(&mut d);
    assert_eq!(d.old.len(), 64, "the table is not the size this test wants");

    // Where four ordinary scan steps end, and what they see on the way:
    // the budget is spent in exactly that traversal and no further.
    let mut expected_cursor = 0;
    let mut expected_dead = BTreeSet::new();
    for _ in 0..4 {
        expected_cursor = d.scan(expected_cursor, |key, _| {
            expected_dead.insert(key.to_vec());
        });
    }

    let (cursor, dead) = d.expire_step(0, 4, now, &Deadlines);
    assert_eq!(cursor, expected_cursor, "the sweep overran its budget");
    assert_eq!(dead.iter().cloned().collect::<BTreeSet<_>>(), expected_dead);
    assert!(!dead.is_empty(), "four buckets of fifty keys held none");
    assert!(
        dead.len() < 50,
        "a budgeted step reported the whole keyspace"
    );

    // The rest is left for later rather than skipped: resuming from the
    // cursor the budgeted call handed back reports every key it did not,
    // and between them they cover the keyspace exactly once.
    let mut covered: BTreeSet<Vec<u8>> = dead.iter().cloned().collect();
    for key in sweep_from(&d, cursor, 4, now) {
        assert!(
            covered.insert(key.clone()),
            "key {key:?} was reported twice in one cycle"
        );
    }
    assert_eq!(
        covered.len(),
        50,
        "the cycle did not reach the keys the budget left behind"
    );
}

#[test]
fn scan_hands_over_the_value_stored_under_each_key() {
    use std::collections::BTreeMap;

    let mut d = Dict::with_seed(seed());
    for i in 0..50u32 {
        d.insert(
            i.to_string().into_bytes(),
            entry(format!("v{i}").as_bytes()),
        );
    }
    drain_rehash(&mut d);

    let mut seen = BTreeMap::new();
    let mut c = 0;
    loop {
        c = d.scan(c, |k, entry| {
            seen.insert(k.to_vec(), entry.value.clone());
        });
        if c == 0 {
            break;
        }
    }
    assert_eq!(seen.len(), 50);
    for i in 0..50u32 {
        assert_eq!(
            seen.get(i.to_string().as_bytes()).map(Vec::as_slice),
            Some(format!("v{i}").as_bytes()),
            "key {i} came back with the wrong value"
        );
    }
}

/// Recomputes what `used_bytes` claims, from nothing but the entries and
/// the tables, so the running figure is held to a second derivation.
fn recount(dict: &Dict) -> u64 {
    let mut total = 0u64;
    for table in std::iter::once(&dict.old).chain(dict.new.iter()) {
        total += u64::try_from(table.len()).expect("bucket count fits") * BUCKET_OVERHEAD;
        for bucket in table {
            for (_, key, entry) in bucket {
                total += entry_bytes(key, &entry.value);
            }
        }
    }
    total
}

#[test]
fn used_bytes_tracks_every_mutation_including_growth_and_clear() {
    let mut dict = Dict::with_seed(DictSeed { k0: 1, k1: 2 });
    assert_eq!(
        dict.used_bytes(),
        recount(&dict),
        "an empty dict is only its table"
    );
    for i in 0..200u32 {
        dict.insert(
            format!("k{i}").into_bytes(),
            Entry {
                value: vec![b'v'; (i % 17) as usize],
                expires_at: None,
                touched: 0,
            },
        );
        assert_eq!(dict.used_bytes(), recount(&dict), "after insert {i}");
    }
    // Overwrite with a longer and then a shorter value.
    dict.insert(
        b"k3".to_vec(),
        Entry {
            value: vec![0; 1000],
            expires_at: None,
            touched: 0,
        },
    );
    assert_eq!(dict.used_bytes(), recount(&dict));
    dict.insert(
        b"k3".to_vec(),
        Entry {
            value: vec![0; 1],
            expires_at: None,
            touched: 0,
        },
    );
    assert_eq!(dict.used_bytes(), recount(&dict));
    for i in (0..200u32).step_by(3) {
        dict.remove(format!("k{i}").as_bytes());
        assert_eq!(dict.used_bytes(), recount(&dict), "after remove {i}");
    }
    while dict.is_rehashing() {
        dict.rehash_step(7);
        assert_eq!(dict.used_bytes(), recount(&dict), "mid-rehash");
    }
    dict.clear();
    assert_eq!(dict.used_bytes(), recount(&dict));
    assert_eq!(
        dict.used_bytes(),
        u64::try_from(INITIAL_BUCKETS).expect("a small constant") * BUCKET_OVERHEAD
    );
}

/// Recomputes what `with_deadline` claims, the way [`recount`] does for
/// the byte figure: from the entries alone.
fn recount_deadlines(dict: &Dict) -> usize {
    let mut total = 0;
    for table in std::iter::once(&dict.old).chain(dict.new.iter()) {
        for bucket in table {
            for (_, _, entry) in bucket {
                total += usize::from(entry.expires_at.is_some());
            }
        }
    }
    total
}

/// The four operations that can move the count, each in both directions:
/// a dated insert, an overwrite that drops the deadline and one that adds
/// one, `set_deadline` both ways, a removal of a dated key and of an
/// undated one, and the flush that takes the lot.
#[test]
fn with_deadline_counts_the_dated_entries_through_every_mutation() {
    let now = Instant::now();
    let later = now + Duration::from_mins(1);
    let mut dict = Dict::with_seed(DictSeed { k0: 3, k1: 4 });
    let dated = |at: Option<Instant>| Entry {
        value: b"v".to_vec(),
        expires_at: at,
        touched: 0,
    };
    assert_eq!(dict.with_deadline(), 0);

    dict.insert(b"a".to_vec(), dated(Some(later)));
    dict.insert(b"b".to_vec(), dated(None));
    assert_eq!(dict.with_deadline(), 1, "one of the two carries a deadline");
    assert_eq!(dict.with_deadline(), recount_deadlines(&dict));

    // Overwrite in both directions: the count follows the entry that is
    // there now, not the one that was.
    dict.insert(b"a".to_vec(), dated(None));
    assert_eq!(dict.with_deadline(), 0);
    dict.insert(b"b".to_vec(), dated(Some(later)));
    assert_eq!(dict.with_deadline(), 1);

    // `set_deadline` is the other way a deadline enters or leaves.
    assert!(dict.set_deadline(b"a", Some(later)));
    assert_eq!(dict.with_deadline(), 2);
    assert!(dict.set_deadline(b"a", Some(later)), "already dated");
    assert_eq!(dict.with_deadline(), 2, "re-dating moves nothing");
    assert!(dict.set_deadline(b"b", None));
    assert_eq!(dict.with_deadline(), 1);
    assert!(!dict.set_deadline(b"nosuch", Some(later)));
    assert_eq!(dict.with_deadline(), 1, "a key that is not there");

    // Removal, of a dated key and of an undated one.
    dict.remove(b"b");
    assert_eq!(dict.with_deadline(), 1);
    dict.remove(b"a");
    assert_eq!(dict.with_deadline(), 0);

    // And across a rehash, where entries live in two tables at once.
    for i in 0..200u32 {
        dict.insert(
            format!("k{i}").into_bytes(),
            dated((i % 3 == 0).then_some(later)),
        );
        assert_eq!(
            dict.with_deadline(),
            recount_deadlines(&dict),
            "after insert {i}"
        );
    }
    for i in (0..200u32).step_by(2) {
        dict.remove(format!("k{i}").as_bytes());
        assert_eq!(
            dict.with_deadline(),
            recount_deadlines(&dict),
            "after remove {i}"
        );
    }
    dict.clear();
    assert_eq!(dict.with_deadline(), 0, "a flush takes the deadlines too");
}

/// Every interleaving of four operations over two keys, so a pair that
/// only disagrees under one order is not left to a longer run to find.
#[test]
fn used_bytes_agrees_with_a_recount_over_every_short_sequence() {
    #[derive(Clone, Copy)]
    enum Op {
        Put(u8, usize),
        Del(u8),
        Clear,
    }
    let ops = [
        Op::Put(0, 3),
        Op::Put(1, 40),
        Op::Put(0, 0),
        Op::Del(0),
        Op::Del(1),
        Op::Clear,
    ];
    for a in ops {
        for b in ops {
            for c in ops {
                for d in ops {
                    let mut dict = Dict::with_seed(DictSeed { k0: 9, k1: 9 });
                    for op in [a, b, c, d] {
                        match op {
                            Op::Put(k, len) => dict.insert(
                                vec![k],
                                Entry {
                                    value: vec![1; len],
                                    expires_at: None,
                                    touched: 0,
                                },
                            ),
                            Op::Del(k) => {
                                dict.remove(&[k]);
                            }
                            Op::Clear => dict.clear(),
                        }
                        assert_eq!(dict.used_bytes(), recount(&dict));
                    }
                }
            }
        }
    }
}

/// The stamp is a per-dict counter, not a clock: a key touched later
/// carries a larger stamp, and a read touches as a write does.
#[test]
fn touching_a_key_stamps_it_later_than_every_key_touched_before() {
    let mut dict = Dict::with_seed(DictSeed { k0: 1, k1: 2 });
    for k in 0..10u8 {
        dict.insert(
            vec![k],
            Entry {
                value: vec![],
                expires_at: None,
                touched: 0,
            },
        );
    }
    dict.touch(&[3]);
    let newest = dict.get(&[3]).unwrap().touched;
    for k in (0..10u8).filter(|k| *k != 3) {
        assert!(
            dict.get(&[k]).unwrap().touched < newest,
            "key {k} is not older than the touched one"
        );
    }
}

/// Sampled LRU from a cursor walk: over a thousand keys of which the last
/// two hundred were touched again, a hundred evictions pick at most two
/// victims from the recent fifth. With five samples the chance one
/// eviction lands entirely inside the recent fifth is 0.2^5 = 0.00032, so
/// a hundred of them expect 0.03; the bound of two is wide, and the walk
/// is deterministic from the seed so this is not a coin toss.
#[test]
fn sampled_eviction_rarely_takes_a_recently_touched_key() {
    let mut dict = Dict::with_seed(DictSeed { k0: 7, k1: 11 });
    for i in 0..1000u32 {
        dict.insert(
            format!("k{i}").into_bytes(),
            Entry {
                value: vec![],
                expires_at: None,
                touched: 0,
            },
        );
        dict.touch(format!("k{i}").as_bytes());
    }
    for i in 800..1000u32 {
        dict.touch(format!("k{i}").as_bytes());
    }
    let mut cursor = 0u64;
    let mut recent_victims = 0;
    for _ in 0..100 {
        let victim = dict
            .sample_oldest(&mut cursor, 5, None)
            .expect("a non-empty dict has a victim");
        let index: u32 = std::str::from_utf8(&victim[1..]).unwrap().parse().unwrap();
        if index >= 800 {
            recent_victims += 1;
        }
        dict.remove(&victim);
    }
    assert!(
        recent_victims <= 2,
        "{recent_victims} of 100 victims were recently touched"
    );
}

/// [`ENTRY_OVERHEAD`] is a claim about what the compiler lays out, and
/// the accounting is only honest while the claim holds. Checked here
/// rather than derived in a doc comment nobody runs: the stamp widened
/// `Entry` through padding, and the next field added to it may not.
#[test]
#[cfg(target_pointer_width = "64")]
fn the_slot_layout_is_what_entry_overhead_prices() {
    assert_eq!(
        size_of::<(u64, Vec<u8>, Entry)>(),
        usize::try_from(ENTRY_OVERHEAD).expect("a small constant"),
    );
}

#[test]
fn an_empty_dict_offers_no_victim() {
    let dict = Dict::with_seed(DictSeed { k0: 1, k1: 2 });
    assert_eq!(dict.sample_oldest(&mut 0, 5, None), None);
}

/// `Bytes::from(Vec<u8>)` keeps the vector's allocation — with and without
/// spare capacity. Measured rather than assumed: the whole point of storing
/// a `Bytes` is that the bytes the codec produced go into the keyspace
/// without a second copy, and a library that copied here would move the
/// copy from `GET` to `SET` instead of removing it.
#[test]
fn a_bytes_from_a_vec_keeps_the_vecs_allocation() {
    for capacity in [64usize, 100] {
        let mut vec = Vec::with_capacity(capacity);
        vec.extend_from_slice(&[7u8; 64]);
        let ptr = vec.as_ptr();
        let bytes = bytes::Bytes::from(vec);
        assert_eq!(
            bytes.as_ptr(),
            ptr,
            "capacity {capacity}: the bytes were copied"
        );
        assert_eq!(&bytes[..], &[7u8; 64]);
    }
}

/// A `Bytes` is four words. The per-entry overhead below is derived from
/// it, so the number is held here rather than trusted.
#[test]
fn bytes_is_four_words() {
    assert_eq!(size_of::<bytes::Bytes>(), 32);
}
