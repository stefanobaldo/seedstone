//! The memory ceiling: what a shard refuses when it is full, and what it
//! gives up when its mode says to make room.

use super::support::{evicted, get, set, setex};
use crate::dict::{Dict, DictSeed, Entry};
use crate::log::NoopLog;
use crate::memory::{EvictionMode, MemoryGauge, MemoryLimit};
use crate::shard::executor::{Memory, ShardState, evict_until_fits};
use crate::shard::{Command, Deadlines, NoTrace, Reply, ReplyError, Router, ShardPool};

/// `SETEX` carries Redis's `denyoom` flag (`COMMAND INFO SETEX` on
/// 6.2.24 and 8.10.1), so under `noeviction` a full shard refuses it as
/// it refuses `SET`.
#[tokio::test]
async fn setex_is_refused_when_full_under_noeviction() {
    let pool = limited(
        2 * 8 * crate::dict::BUCKET_OVERHEAD,
        EvictionMode::NoEviction,
    );
    assert_eq!(pool.dispatch(set(b"k0", &[0u8; 64])).await, Reply::Ok);
    assert_eq!(
        pool.dispatch(setex(b"k1", 10, &[0u8; 64])).await,
        Reply::Error(ReplyError::OutOfMemory)
    );
    assert_eq!(pool.dispatch(get(b"k1")).await, Reply::Bulk(None));
}

/// The gauge is the sum of every dict's figure, kept current by the
/// executors: a write moves it up by what the dict says the write cost,
/// and a delete moves it back down by the same amount.
#[tokio::test]
async fn the_pool_gauge_follows_what_its_dicts_account() {
    let pool = ShardPool::spawn(4, 2, DictSeed { k0: 1, k1: 2 }, NoTrace);
    let gauge = pool.memory();
    let empty = gauge.used();
    assert_eq!(
        empty,
        4 * 8 * crate::dict::BUCKET_OVERHEAD,
        "four empty tables"
    );
    assert_eq!(pool.dispatch(set(b"k", &[0u8; 100])).await, Reply::Ok);
    assert_eq!(
        gauge.used(),
        empty + crate::dict::entry_bytes(b"k", &[0u8; 100])
    );
    assert_eq!(
        pool.dispatch(Command::Del { key: b"k".to_vec() }).await,
        Reply::Removed(true)
    );
    assert_eq!(gauge.used(), empty);
}

fn limited(ceiling: u64, mode: EvictionMode) -> ShardPool {
    ShardPool::spawn_limited(
        2,
        1,
        DictSeed { k0: 1, k1: 2 },
        NoTrace,
        MemoryLimit {
            ceiling: Some(ceiling),
            mode,
        },
    )
}

/// Writes past the ceiling evict until the figure is under it again, and
/// each eviction is counted.
#[tokio::test]
async fn allkeys_lru_evicts_until_the_write_fits() {
    let pool = limited(
        2 * 8 * crate::dict::BUCKET_OVERHEAD + 3 * crate::dict::entry_bytes(b"k0", &[0; 64]),
        EvictionMode::AllKeysLru,
    );
    for i in 0..3u8 {
        assert_eq!(
            pool.dispatch(set(&[b'k', b'0' + i], &[0u8; 64])).await,
            Reply::Ok
        );
    }
    assert_eq!(evicted(&pool).await, 0, "three entries fit exactly");
    assert_eq!(pool.dispatch(set(b"k9", &[0u8; 64])).await, Reply::Ok);
    assert!(pool.memory().used() <= pool.limit().ceiling.unwrap());
    assert_eq!(evicted(&pool).await, 1);
    assert_eq!(
        pool.dispatch(get(b"k9")).await,
        Reply::Bulk(Some(vec![0u8; 64])),
        "the write that evicted is itself kept"
    );
}

/// Under `noeviction` the write is refused, byte-exact, and nothing moves.
#[tokio::test]
async fn noeviction_refuses_a_write_over_the_ceiling_and_keeps_reads() {
    // The ceiling is what two empty tables already cost, so the first
    // write is the one that crosses it. The comparison is against what
    // the node held *before* the command, as Redis's is: a write is never
    // refused for the bytes it is about to add, only for the ones already
    // there, so the write that crosses the line lands and the next one is
    // refused.
    let pool = limited(
        2 * 8 * crate::dict::BUCKET_OVERHEAD,
        EvictionMode::NoEviction,
    );
    assert_eq!(pool.dispatch(set(b"k0", &[0u8; 64])).await, Reply::Ok);
    assert_eq!(
        pool.dispatch(set(b"k1", &[0u8; 64])).await,
        Reply::Error(ReplyError::OutOfMemory)
    );
    assert_eq!(
        pool.dispatch(get(b"k0")).await,
        Reply::Bulk(Some(vec![0u8; 64]))
    );
    assert_eq!(
        pool.dispatch(Command::Del {
            key: b"k0".to_vec()
        })
        .await,
        Reply::Removed(true),
        "a delete is never refused"
    );
    assert_eq!(
        ReplyError::OutOfMemory.wire_text(),
        "OOM command not allowed when used memory > 'maxmemory'."
    );
    assert_eq!(evicted(&pool).await, 0);
}

/// A shard past the ceiling still gives up a key when the command that
/// took it there addressed the oldest one in the sample.
///
/// `TTL` is `Route::Key` and never stamps, so the key it addresses keeps
/// whatever stamp it already had — which may be the oldest the sample
/// meets. Sparing that key has to exclude it from candidacy rather than
/// abandon the loop: `None` from the sampler means "this shard has nothing
/// to give", and a shard holding two other keys is not saying that.
///
/// Reached through `evict_until_fits` directly rather than through the
/// pool, because the state it needs cannot be built out of commands: a
/// write stamps its own key newest, so the only way the spared key is also
/// the oldest is a shard already over a ceiling somebody else's bytes
/// pushed it past.
#[test]
fn a_command_that_does_not_stamp_still_evicts_past_the_ceiling() {
    let mut state = ShardState::new(Dict::with_seed(DictSeed { k0: 3, k1: 5 }), NoopLog);
    // Stamped in this order, so `spared` is the oldest of the three and
    // the sample — which meets all of them, being smaller than
    // `EVICTION_SAMPLES` — offers it first.
    for key in [b"spared".as_slice(), b"middle", b"newest"] {
        state.dict.insert(
            key.to_vec(),
            Entry {
                value: vec![0u8; 64],
                expires_at: None,
                touched: 0,
            },
        );
        state.dict.touch(key);
    }
    let used = state.dict.used_bytes();
    let memory = Memory {
        gauge: MemoryGauge::default(),
        limit: MemoryLimit {
            // One key under what the shard is holding, so one eviction is
            // enough to get back under and the loop has an end.
            ceiling: Some(used - 1),
            mode: EvictionMode::AllKeysLru,
        },
    };
    memory.gauge.apply(0, used);

    evict_until_fits(
        &mut state,
        0,
        &memory,
        &NoTrace,
        &Deadlines,
        Some(b"spared"),
    );

    assert_eq!(
        state.dict.len(),
        2,
        "the loop abandoned a shard that still had room to make"
    );
    assert_eq!(state.evicted, 1);
    assert!(
        state.dict.get(b"spared").is_some(),
        "the spared key was evicted"
    );
}

/// A write that cannot fit even on an empty shard stops at empty rather
/// than looping: the write lands, the figure is over, and the next write
/// starts evicting again.
#[tokio::test]
async fn a_value_larger_than_the_ceiling_empties_the_shard_and_stops() {
    let pool = limited(1, EvictionMode::AllKeysLru);
    assert_eq!(pool.dispatch(set(b"big", &[0u8; 1024])).await, Reply::Ok);
    assert_eq!(
        pool.dispatch(get(b"big")).await,
        Reply::Bulk(Some(vec![0u8; 1024]))
    );
}
