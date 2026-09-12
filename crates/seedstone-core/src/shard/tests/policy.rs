//! The seam: a pool spawned with a policy asks that policy, and the default
//! one answers honestly.

use super::support::{Recorder, get, set, set_ex};
use crate::dict::{DictSeed, WalkOrder};
use crate::shard::{
    Deadlines, EvictionPolicy, ExpiryPolicy, HOUSEKEEPING_TICK, NoTrace, Reply, Router, ShardPool,
};
use std::time::Duration;
use tokio::time::Instant;

/// A policy that never finds anything due, which is what a server with no
/// liveness check and no working sweep looks like from the outside.
///
/// It is defined here, in a test module, and in `seedstone-sim` for the
/// plants — never in a production path. That is the whole point of the
/// parameter: the defective policies are unlinkable from the binary
/// because they are not in a crate it depends on.
#[derive(Clone, Copy)]
struct NeverDue;

impl WalkOrder for NeverDue {}

impl ExpiryPolicy for NeverDue {
    fn due_on_read(&self, _expires_at: Option<Instant>, _now: Instant) -> bool {
        false
    }
    fn due_on_sweep(&self, _expires_at: Option<Instant>, _now: Instant) -> bool {
        false
    }
    fn takes_undated(&self) -> bool {
        false
    }
}

impl EvictionPolicy for NeverDue {
    fn must_evict(&self, used: u64, ceiling: Option<u64>) -> bool {
        Deadlines.must_evict(used, ceiling)
    }
}

/// The defect this seam exists to be able to plant: the deadline is
/// stored, the clock passes it, and the key is still there — on both paths
/// at once, which is what makes it a missing expiry rather than a slow
/// one.
#[tokio::test(start_paused = true)]
async fn a_pool_spawned_with_a_policy_expires_by_that_policy() {
    let sink = Recorder::default();
    let pool = ShardPool::spawn_with_policy(1, 1, DictSeed { k0: 2, k1: 3 }, sink, NeverDue);
    assert_eq!(pool.dispatch(set_ex(b"k", b"v", 1)).await, Reply::Ok);

    // Past the deadline, and past enough housekeeping ticks for the sweep
    // to have walked the whole table several times over.
    tokio::time::advance(Duration::from_secs(2)).await;
    for _ in 0..8 {
        tokio::time::advance(HOUSEKEEPING_TICK).await;
    }

    assert_eq!(
        pool.dispatch(get(b"k")).await,
        Reply::Bulk(Some(b"v".to_vec())),
        "the policy said nothing was due, so the key must still answer"
    );
}

/// A policy that reclaims whenever it is asked, whatever the gauge says
/// and whether or not there is a ceiling at all.
///
/// The eviction counterpart of [`NeverDue`], and it lives here for the
/// same reason: the defective answers belong in a crate the binary does
/// not depend on.
#[derive(Clone, Copy)]
struct AlwaysEvicts;

impl WalkOrder for AlwaysEvicts {}

impl ExpiryPolicy for AlwaysEvicts {
    fn due_on_read(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        Deadlines.due_on_read(expires_at, now)
    }
    fn due_on_sweep(&self, expires_at: Option<Instant>, now: Instant) -> bool {
        Deadlines.due_on_sweep(expires_at, now)
    }
    fn takes_undated(&self) -> bool {
        Deadlines.takes_undated()
    }
}

impl EvictionPolicy for AlwaysEvicts {
    fn must_evict(&self, _used: u64, _ceiling: Option<u64>) -> bool {
        true
    }
}

/// Whether to reclaim is the policy's answer and not a comparison the
/// executor makes for itself.
///
/// Two keys, and the *older* one is the one read back: `evict_until_fits`
/// never takes the key the triggering command addressed, so a one-key
/// shard under this policy keeps its one key. With two, the second write
/// reclaims the first and the shard drains to exactly what was written
/// last — which is the property, stated exactly.
///
/// Spawned with no ceiling on purpose. The limit's mode decides *refuse
/// or reclaim* and [`MemoryLimit::default`]'s is `AllKeysLru`, so the
/// loop runs and asks the policy; the policy decides *whether now*. The
/// honest half below is what makes that a claim rather than a
/// coincidence: [`Deadlines`] under no ceiling never evicts anything.
#[tokio::test]
async fn the_eviction_decision_is_the_policys() {
    let pool = ShardPool::spawn_with_policy(1, 1, DictSeed { k0: 2, k1: 3 }, NoTrace, AlwaysEvicts);
    assert_eq!(pool.dispatch(set(b"a", b"v")).await, Reply::Ok);
    assert_eq!(pool.dispatch(set(b"b", b"v")).await, Reply::Ok);
    assert_eq!(
        pool.dispatch(get(b"a")).await,
        Reply::Bulk(None),
        "a policy that always evicts keeps only what was written last"
    );

    let honest = ShardPool::spawn(1, 1, DictSeed { k0: 2, k1: 3 }, NoTrace);
    assert_eq!(honest.dispatch(set(b"a", b"v")).await, Reply::Ok);
    assert_eq!(honest.dispatch(set(b"b", b"v")).await, Reply::Ok);
    assert_eq!(
        honest.dispatch(get(b"a")).await,
        Reply::Bulk(Some(b"v".to_vec())),
        "the honest policy under no ceiling reclaims nothing"
    );
}

/// The counterpart, and the reason the test above proves anything: the
/// honest policy is what `spawn` uses, and it does expire the key.
#[tokio::test(start_paused = true)]
async fn the_default_policy_is_the_honest_one() {
    let sink = Recorder::default();
    let pool = ShardPool::spawn(1, 1, DictSeed { k0: 2, k1: 3 }, sink);
    assert_eq!(pool.dispatch(set_ex(b"k", b"v", 1)).await, Reply::Ok);
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(pool.dispatch(get(b"k")).await, Reply::Bulk(None));
}
