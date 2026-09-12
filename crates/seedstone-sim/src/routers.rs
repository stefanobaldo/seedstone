//! The two routers the simulator puts in front of the real pool: one that
//! rewrites requests as a plant dictates, one that skips a shard on a
//! keyspace walk.

use seedstone_core::shard::{Command, Reply, ReplyError, Router, ShardPool, parse_i64};
use seedstone_service::WALK_STEP_BUCKETS;

/// A router that serves the workload through one deliberate defect.
///
/// This is the harness's self-test: a simulator that never fails proves
/// nothing, so the branch ships bugs the sweep is required to find. One plant
/// lives here — [`crate::Plant::LostUpdate`], which is a defect between two messages
/// and so has nowhere else to be; see [`crate::Plant`] for why the other two are the
/// server's own expiry policy instead. Everything that plant does not touch
/// passes straight through, so an honest run and a planted one differ in
/// exactly one thing.
///
/// Which plant is not a parameter, and that is the point: it was one while all
/// three lived here, and leaving it would mean `new(pool, Plant::ServeExpired)`
/// compiled into a run with nothing planted in it at all. A self-test that can
/// be asked for a defect and quietly hand back an honest server is the exact
/// failure it exists to prevent.
#[derive(Clone)]
pub struct PlantedRouter {
    /// The honest pool underneath.
    pool: ShardPool,
}

impl PlantedRouter {
    /// Wraps `pool` so that a lost update is what the workload meets.
    #[must_use]
    pub const fn new(pool: ShardPool) -> Self {
        Self { pool }
    }
}

impl Router for PlantedRouter {
    // Spelled `async fn` rather than the trait's desugared `-> impl Future`:
    // with a bare `async` block as the body clippy's `manual_async_fn` fires,
    // and the gate is `-D warnings`. `ShardPool`'s own impl keeps the
    // desugared form for a reason that does not apply here — it sends on the
    // inbox before the future is awaited.
    async fn dispatch(&self, cmd: Command) -> Reply {
        self.lose_updates(cmd).await
    }

    /// The pool's own count: the plant wraps a pool, it does not resize one.
    fn shards(&self) -> u16 {
        self.pool.shards()
    }

    /// Passed straight through: the plant is a defect between two messages of
    /// one keyed command, and a shard-addressed step is neither.
    async fn dispatch_at(&self, shard: u16, cmd: Command) -> Reply {
        self.pool.dispatch_at(shard, cmd).await
    }

    /// Passed straight through: the plant is a defect between two messages of
    /// one keyed command, and a broadcast is neither.
    async fn dispatch_every(&self, cmd: Command) -> Vec<Reply> {
        self.pool.dispatch_every(cmd).await
    }
}

impl PlantedRouter {
    /// `INCRBY` as `GET`, compute, `SET`: two shard round-trips with an await
    /// between them, so another connection's increment can land in the window
    /// and be overwritten.
    async fn lose_updates(&self, cmd: Command) -> Reply {
        let Command::IncrBy { key, delta } = cmd else {
            return self.pool.dispatch(cmd).await;
        };

        let current = match self.pool.dispatch(Command::Get { key: key.clone() }).await {
            Reply::Bulk(Some(value)) => match parse_i64(&value) {
                Some(current) => current,
                None => return Reply::Error(ReplyError::NotAnInteger),
            },
            Reply::Bulk(None) => 0,
            // A shard that could not answer; pass its complaint on unchanged.
            other => return other,
        };
        let Some(updated) = current.checked_add(delta) else {
            return Reply::Error(ReplyError::WouldOverflow);
        };

        // The window, widened deliberately. `current` was read at one moment
        // and is written back at a later one, and nothing holds the key still
        // in between; this hands the scheduler an explicit chance to run
        // another connection there. Without it the window is one scheduler
        // round and the race surfaced in 2 seeds of 64 — evidence far weaker
        // than the claim the self-test makes. With it, 26 of 64.
        //
        // It is not a cheat: a genuine read-modify-write across an await is
        // exactly this shape, and the suspension point is honest rather than
        // simulated. It is also free — the honest router is untouched, and no
        // hash of a planted run is pinned anywhere, so no recorded trace moves.
        tokio::task::yield_now().await;

        match self
            .pool
            .dispatch(Command::Set {
                key,
                value: updated.to_string().into_bytes(),
                // The write the honest `INCRBY` would have made: same value,
                // same absence of options. The only thing planted here is that
                // it is a second message.
                expiry: None,
                cond: None,
                keep_ttl: false,
                get: false,
            })
            .await
        {
            Reply::Ok => Reply::Integer(updated),
            other => other,
        }
    }
}

/// A router that steps over every odd shard a `SCAN` call crosses into.
///
/// [`crate::Plant::CrossingSkipsShard`], and a type of its own for the reason
/// [`PlantedRouter`] gives for having no plant parameter: a router that can be
/// asked for one defect and quietly serve another is the failure a self-test
/// exists to prevent. This one is above the shard because the defect is, too —
/// crossing is the edge's loop, and a shard answers one step of it knowing
/// nothing about the shards on either side.
#[derive(Clone)]
pub struct SkippingRouter {
    /// The honest pool underneath.
    pool: ShardPool,
}

impl SkippingRouter {
    /// Wraps `pool` so that a `SCAN` call skips the odd shards it crosses into.
    #[must_use]
    pub const fn new(pool: ShardPool) -> Self {
        Self { pool }
    }
}

impl Router for SkippingRouter {
    // Spelled `async fn` for the reason [`PlantedRouter`]'s impl states.
    async fn dispatch(&self, cmd: Command) -> Reply {
        self.pool.dispatch(cmd).await
    }

    /// The pool's own count: the plant wraps a pool, it does not resize one.
    fn shards(&self) -> u16 {
        self.pool.shards()
    }

    /// Where the defect is. A step at cursor `0` carrying less than the whole
    /// bucket ceiling is a shard the call *crossed into*: the shards before it
    /// spent the difference. Answering it as already spent — nothing found, one
    /// bucket charged, cycle over — sends the call straight on to the next
    /// shard with this one never opened.
    ///
    /// Every other step is passed through, and the two exclusions are what keep
    /// this a statement about the crossing. A step at cursor `0` with the full
    /// ceiling is the *first* shard of a call, which the client addressed
    /// itself; and it is every step `KEYS` sends, which walks each shard from
    /// `0` on a budget of its own and has crossed nothing.
    async fn dispatch_at(&self, shard: u16, cmd: Command) -> Reply {
        if !shard.is_multiple_of(2)
            && matches!(
                cmd,
                Command::ScanStep { cursor: 0, count, .. } if count < WALK_STEP_BUCKETS
            )
        {
            return Reply::Scan {
                cursor: 0,
                keys: Vec::new(),
                visited: 1,
            };
        }
        self.pool.dispatch_at(shard, cmd).await
    }

    /// Passed straight through: a broadcast crosses nothing.
    async fn dispatch_every(&self, cmd: Command) -> Vec<Reply> {
        self.pool.dispatch_every(cmd).await
    }
}
