//! The commands no single shard can answer: the ones every shard must see, and
//! the walks over the whole keyspace. Each sends to every shard, gathers the
//! replies in shard order rather than completion order, and folds them into one
//! frame.

use crate::connection::CHUNK_COMMANDS;
use crate::dispatch::{Fold, Gather};
use crate::reply::{UNRENDERABLE_REPLY, reply_to_frame};
use crate::walk;
use seedstone_core::shard::{Command, Reply, ReplyError, Router};
use seedstone_resp::Frame;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

/// How many bytes of key names one `KEYS` reply may accumulate at the edge.
///
/// [`keys`] gathers the whole answer here before any of it reaches the wire —
/// documented as the cost of the shape and the reason `SCAN` exists — and this
/// is the bound on it. A walk whose gathered key bytes pass this is abandoned
/// and answered with [`KEYS_TOO_LARGE`].
///
/// It is [`MAX_REQUEST_BYTES`] again, for the reason stated there: one request
/// may make one connection hold this much, on the request side or on the reply
/// side, and the two halves are held to the same figure so the pair is one
/// number rather than two to keep in step.
///
/// **It prices the key bytes and nothing else.** Not the `Vec` per key, not the
/// `$<len>\r\n` each one costs on the wire, not the capacity the gathering
/// vectors grew to hold them. So this is a bound on accumulation rather than a
/// measurement of the frame — the same undercounting [`MAX_REQUEST_BYTES`]
/// admits to on the parsed side, and acceptable for the same reason: the
/// figure is a ceiling on the worst case one peer can impose, not a memory
/// budget.
///
/// **The per-key constant is worth writing out, because "a constant" reads as
/// small and this one is not.** Every gathered key costs a `Frame` — 32 bytes,
/// the `Vec` header inline in the enum — plus its own heap allocation, which
/// no allocator serves below about 16 bytes however short the key is. Call it
/// ~48 bytes of overhead against however many bytes of key name are counted
/// here. At the short keys a cache actually holds that ratio is the whole
/// story: **one-byte keys reach this ceiling only after ~67 million of them,
/// whose headers and allocations alone are over 3 GB** — some fifty times the
/// figure this constant names, and none of it counted. The reason that is
/// tolerable is not the arithmetic but the keyspace: `maxmemory` bounds how
/// many keys can exist to be gathered, and a node that could hold 67 million
/// of them was configured to.
pub const KEYS_REPLY_BYTES: usize = 64 * 1024 * 1024;

/// What a peer whose `KEYS` reply outgrew [`KEYS_REPLY_BYTES`] is told.
///
/// Redis has no such refusal — it answers every `KEYS`, however large, and
/// blocks for as long as that takes — so there is no byte-exact text to match
/// and this one says what the client can do instead.
pub const KEYS_TOO_LARGE: &str = "ERR KEYS reply exceeds the per-request limit; use SCAN";

/// How many buckets one step of a keyspace walk visits before answering.
///
/// It is the unit of occupancy: larger holds a shard for longer per step and
/// pays less per-envelope overhead, smaller yields sooner and pays more.
///
/// **The value is chosen on measurement**, not reasoned from a sibling
/// constant. Two builds differing in this number alone were walked over a
/// keyspace shaped like the workloads this project targets, and this is the
/// one whose pattern-matched cycle cost fewer round trips and less wall for no
/// measurable cost on the cycle without a pattern, on `KEYS`, or anywhere else
/// the pair was compared. The core's `EXPIRE_BUCKETS_PER_TICK` is the other
/// bounded walk over the same table and is what an earlier value was reasoned
/// from; it is not what settled this one.
///
/// What the measurement did not price, and a reader changing this number
/// should know: a larger budget lets one call cross more shards before it
/// answers, so the call itself takes longer to return even where the cycle it
/// belongs to is faster — and a shard whose table exceeds the budget is held
/// for the whole of it in one step. Neither is visible at a keyspace where
/// tables are small and the budget is spent crossing shards rather than
/// walking one.
///
/// It serves the two walking commands differently, and deliberately with one
/// number. `KEYS` carries no `COUNT` on the wire, so this *is* its step.
/// For `SCAN` it is the **per-call occupancy ceiling across shards**: one
/// budget for the whole call, spent down by every shard the call crosses, so
/// a shard answering after eight buckets leaves the rest to the next. It is
/// not the client's number. `COUNT` is the client's, a key target, and the
/// call ends at whichever of the two is reached first — occupancy is the
/// server's to bound whoever asked, and a target honoured without a ceiling
/// would let one call walk an entire cycle and hold the node for it.
///
/// Public because the number is what distinguishes the two commands' steps
/// from outside the server: a `KEYS` step always carries the whole ceiling
/// because every shard is walked from `0` on its own budget, while every step
/// of a `SCAN` call after its first carries less, because the shards before it
/// spent some. That is the only signal a shard has that the call *crossed into*
/// it, and the simulator's crossing plant is built on it.
pub const WALK_STEP_BUCKETS: usize = 1024;

/// What `SCAN` visits when the client does not say. Redis's default, and the
/// number the clients this gate exercises leave unset.
pub const SCAN_DEFAULT_COUNT: usize = 10;

/// What a peer resuming a walk from something this server never issued is
/// told.
///
/// It covers both ways that happens: a cursor that is not the canonical
/// decimal this server prints, and one whose high bits name a shard this node
/// does not have. Redis has only the second failure and spells it
/// `ERR invalid cursor`; a client that can act on either can act on both.
pub const INVALID_CURSOR: &str = "ERR invalid cursor";

/// Runs one keyspace-wide command on every shard and folds the answers into
/// the single frame the peer sees.
///
/// **An error from any shard wins.** A keyspace-wide command that reached most
/// of the keyspace has not done what it was asked, and answering from the part
/// that worked would tell the peer something less true than the failure does.
///
/// The shards that succeeded are not rolled back, and cannot be: this layer
/// has no transaction to unwind and each shard has already applied what it
/// applied. So the error means "not everywhere", not "nowhere" — which is the
/// same thing a fan-out's error means, and for the same reason.
///
/// Short of an error, the fold is the [`Gather`] the command table chose:
/// `DBSIZE` sums, which is what makes it the size of the keyspace rather than
/// of a shard; `FLUSHDB` is answered `+OK` once every shard has.
pub async fn broadcast<R: Router>(router: &R, cmd: Command, gather: Gather) -> Frame {
    let mut total: i64 = 0;
    for reply in router.dispatch_every(cmd).await {
        match reply {
            // Saturating for the reason [`fan_out`] saturates: a keyspace
            // larger than `i64::MAX` is not reachable, and wrapping into a
            // negative count would be a worse answer than the ceiling.
            Reply::Integer(n) => total = total.saturating_add(n),
            Reply::Ok => {}
            other => return reply_to_frame(other),
        }
    }
    match gather {
        Gather::Sum => Frame::Integer(total),
        Gather::AllOk => Frame::Simple("OK".into()),
    }
}

/// Why one shard's walk in [`keys`] stopped before it had finished.
pub enum WalkStop {
    /// The shard did not run a step; the peer hears this error's own text.
    Shard(ReplyError),
    /// The gathered key bytes passed the ceiling — see [`KEYS_REPLY_BYTES`].
    ///
    /// Distinct from [`Shard`](WalkStop::Shard) because the two are different
    /// answers to different faults: one is the request asking for more than a
    /// reply may hold, the other is the node failing to serve it.
    TooLarge,
}

/// Every key matching `pattern`, gathered from every shard.
///
/// One cursor loop per shard, run concurrently and joined. Each step is an
/// ordinary envelope, so this occupies a shard for the length of one step
/// rather than for the length of the walk, and the walks themselves overlap
/// rather than queueing behind each other.
///
/// The result is accumulated here before any of it reaches the wire, which is
/// the cost this shape accepts and the reason `SCAN` exists. Duplicates are
/// removed: a table that doubles mid-walk can return a key twice, which is
/// `SCAN`'s documented behaviour and would be surprising in a single answer.
///
/// **The answer is not a snapshot, and nothing on the wire says so.** No lock
/// spans the walks and none spans a single shard's steps, so writes land
/// between them; the sharp case is `FLUSHDB`, which empties every shard while
/// the walks are in flight. `Dict::scan` short-circuits on an empty table and
/// answers `0`, so every walk still running terminates at once and `KEYS`
/// replies with whatever each had already gathered — a set that was the
/// keyspace at no single instant. That is the same trade [`fan_out`] and
/// [`broadcast`] make and the only one available at this layer, for the same
/// reason: the alternative is a lock spanning shards, paid for by every
/// single-key command.
///
/// Two things it does guarantee, and they are why the trade is acceptable.
/// The path terminates: a shrinking table can only shorten a walk, never
/// extend it, so there is no cursor loop that fails to end. And a cursor
/// carried across a resize can only re-visit buckets, never skip them — the
/// reverse-binary increment `Dict::scan` uses is correct across a shrink as
/// well as a growth — so the failure mode is a key reported twice, which the
/// dedup above removes, rather than a key that existed throughout and was
/// missed.
///
/// The walk is `O(keyspace)` and competes for CPU with traffic while it runs.
/// What it no longer does is stop the server for its duration, which is the
/// difference worth having and not the same thing as being cheap.
///
/// **It does not use [`scan`]'s crossing, and it should not — reviewed, and
/// left.** Crossing exists to spend fewer *round trips*, and a round trip is
/// what a client pays over a network. Here every step is an envelope inside
/// one process, and the shards are walked concurrently rather than one after
/// another, so a crossing would trade the concurrency for a sequence and buy
/// nothing back. Said here so the next reader sees a decision rather than an
/// oversight.
///
/// **What is accumulated is bounded.** The key bytes every walk gathers are
/// counted into one shared total as they arrive, and the first walk to see
/// that total past `ceiling` — [`KEYS_REPLY_BYTES`] on the server's own path —
/// abandons, which makes the whole reply [`KEYS_TOO_LARGE`] rather than a
/// short array nobody could tell from a small keyspace. The total is shared
/// because the ceiling is on the answer, not on any one shard's share of it.
/// It is checked once per step rather than once per key, so what is held can
/// exceed the ceiling by up to one step's keys per shard — a bound with a
/// known slack, which is what a ceiling on accumulation needs to be.
pub async fn keys<R: Router>(router: &R, pattern: Vec<u8>, ceiling: usize) -> Frame {
    // Shared by every walk, and a plain `&` reaches all of them: these are
    // futures joined inside one task, not tasks of their own. `Relaxed` is the
    // whole ordering requirement — nothing is published alongside the count,
    // and the only question asked of it is whether the total has passed the
    // ceiling, which a walk that misses by one step asks again a step later.
    let gathered = AtomicUsize::new(0);
    let walks: Vec<_> = (0..router.shards())
        .map(|shard| {
            let pattern = pattern.clone();
            let gathered = &gathered;
            async move {
                let mut found: Vec<Vec<u8>> = Vec::new();
                let mut cursor = 0u64;
                loop {
                    let reply = router
                        .dispatch_at(
                            shard,
                            Command::ScanStep {
                                cursor,
                                count: WALK_STEP_BUCKETS,
                                // Cloned per step, and it has to be: the
                                // command is moved into the router and the
                                // reply does not hand the pattern back, so
                                // there is nothing to carry forward. Hoisting
                                // it would need a shared, cheaply-cloned
                                // pattern in the command, which is a wider
                                // change than a keyspace walk's per-step
                                // allocation is worth beside the keys it
                                // returns in the same step.
                                pattern: Some(pattern.clone()),
                            },
                        )
                        .await;
                    match reply {
                        Reply::Scan {
                            cursor: next, keys, ..
                        } => {
                            // One add per step, not per key: the ceiling
                            // bounds an accumulation, and paying an atomic
                            // per key would price the bound at more than the
                            // gathering it guards.
                            let step_bytes: usize = keys.iter().map(Vec::len).sum();
                            found.extend(keys);
                            let total = gathered
                                .fetch_add(step_bytes, Ordering::Relaxed)
                                .saturating_add(step_bytes);
                            if total > ceiling {
                                return Err(WalkStop::TooLarge);
                            }
                            cursor = next;
                            if cursor == 0 {
                                return Ok(found);
                            }
                        }
                        Reply::Error(error) => return Err(WalkStop::Shard(error)),
                        // A router that answered something else did not run
                        // the step, and there is no partial walk to report:
                        // this is the shard failing to answer, spelled the way
                        // the dispatch path already spells that.
                        _ => return Err(WalkStop::Shard(ReplyError::ShardUnavailable)),
                    }
                }
            }
        })
        .collect();

    let mut all: Vec<Vec<u8>> = Vec::new();
    for walk in join_all(walks).await {
        match walk {
            Ok(found) => all.extend(found),
            // One shard that could not answer makes the whole reply wrong, and
            // a short array is a wrong answer a client cannot detect. Say so
            // instead — and the same holds for a walk that abandoned on the
            // ceiling, which is why that is an error too rather than the
            // partial answer the other walks did gather.
            Err(WalkStop::Shard(error)) => return Frame::Error(error.wire_text().to_owned()),
            Err(WalkStop::TooLarge) => return Frame::Error(KEYS_TOO_LARGE.to_owned()),
        }
    }
    all.sort_unstable();
    all.dedup();
    Frame::Array(all.into_iter().map(Frame::Bulk).collect())
}

/// One `SCAN` call: as many shards as its budget crosses, and the cursor the
/// client resumes at.
///
/// A shard that finishes is followed into the next while the call's bucket
/// budget lasts, and the cursor the client gets back is wherever the call
/// stopped — so the client walks the whole keyspace without ever being told
/// there is more than one shard. Only the last shard finishing produces 0.
///
/// **`COUNT` is the client's key target and [`WALK_STEP_BUCKETS`] is the
/// server's occupancy ceiling.** The two numbers serve two parties and the
/// call ends at whichever is reached first, which is what makes `COUNT` mean
/// on this server what Redis documents it to mean. A cycle costs about
/// `keys / COUNT` calls, plus one for each shard whose table is larger than
/// the budget a call had left when it arrived. What one call spends is up to
/// one envelope per shard it crosses, each the cost of a `GET`; each shard is
/// occupied only for its own step, because crossing is a sequence of
/// envelopes from here and never a shard reaching into its neighbour.
///
/// The completeness argument is unchanged and still stated per shard: the
/// call resumes shard `s` at the cursor that shard issued, spends it, and
/// starts shard `s + 1` at 0 — the same two cursors the client used to send
/// in two calls, concatenated inside one.
///
/// The step may legitimately answer no keys with a non-zero cursor — a
/// stretch of empty buckets, or a `MATCH` that excluded everything, in a call
/// whose budget ran out before its target. Redis behaves the same way and
/// clients handle it; a server that looped until it had keys would be
/// answering an unbounded call.
pub async fn scan<R: Router>(
    router: &R,
    cursor: u64,
    pattern: Option<Vec<u8>>,
    key_target: usize,
) -> Frame {
    // The cursor came out of an integer the peer chose, so this is where it
    // stops being trusted. `dispatch_at` would refuse a shard it does not
    // have too; refusing it here is what makes the refusal say `invalid
    // cursor` rather than name a shard to a client that has no idea this
    // server has any.
    let Ok(mut crossing) =
        walk::Crossing::begin(router.shards(), cursor, key_target, WALK_STEP_BUCKETS)
    else {
        return Frame::Error(INVALID_CURSOR.to_owned());
    };
    while let Some(step) = crossing.wants_step() {
        let reply = router
            .dispatch_at(
                step.shard,
                Command::ScanStep {
                    cursor: step.cursor,
                    count: step.count,
                    // Cloned once per shard the call crosses, which is the
                    // same per-step cost the walk always paid: the command is
                    // moved into the router and the reply does not hand the
                    // pattern back, so there is nothing to carry forward.
                    pattern: pattern.clone(),
                },
            )
            .await;
        match reply {
            Reply::Scan {
                cursor: next,
                keys,
                visited,
            } => crossing.feed(next, keys, visited),
            Reply::Error(error) => return Frame::Error(error.wire_text().to_owned()),
            // A router that answered something else did not run the step,
            // which is the shard failing to answer — spelled the way [`keys`]
            // spells it.
            _ => return Frame::Error(ReplyError::ShardUnavailable.wire_text().to_owned()),
        }
    }
    let (next, keys) = crossing.finish();
    Frame::Array(vec![
        // A bulk string, not an integer: that is what Redis sends and what
        // clients parse. A client that fed an integer back would be sending a
        // cursor this server never issued.
        Frame::Bulk(next.to_string().into_bytes()),
        Frame::Array(keys.into_iter().map(Frame::Bulk).collect()),
    ])
}

/// Drives every future to completion concurrently, and gathers the outputs in
/// the order the futures were given rather than the order they finished.
///
/// Hand-rolled because the workspace carries no futures crate and one walk
/// does not justify adding one. It also deliberately does not spawn: a spawned
/// task's completion order belongs to the runtime, so the order two shards'
/// walks finished in could differ between two runs of one seed — the
/// non-determinism [`Router::dispatch_every`] gathers by index to avoid.
/// Polling a fixed vector in index order cannot vary.
///
/// No future is polled again after it returns `Ready`: the slot that holds its
/// output is filled in the same step, and a filled slot is skipped on every
/// later pass. Every future that is *not* ready is polled again on every wake,
/// though — there is no per-future waker to tell them apart — so a wake costs
/// one poll per incomplete future, and a `KEYS` at a thousand shards does a
/// thousand cheap polls per wake. That is the price of not spawning, and it is
/// paid on a keyspace walk rather than on the request path.
pub async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut pending: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut done: Vec<Option<F::Output>> = Vec::new();
    done.resize_with(pending.len(), || None);
    let mut left = pending.len();

    poll_fn(move |cx| {
        for (slot, future) in done.iter_mut().zip(pending.iter_mut()) {
            if slot.is_some() {
                continue;
            }
            if let Poll::Ready(output) = future.as_mut().poll(cx) {
                *slot = Some(output);
                left -= 1;
            }
        }
        if left == 0 {
            Poll::Ready(
                done.iter_mut()
                    .map(|slot| slot.take().expect("every future has completed"))
                    .collect(),
            )
        } else {
            Poll::Pending
        }
    })
    .await
}

/// Runs one command per key and folds the replies into the one frame the
/// request is owed — see [`Fold`] for which fold, and why the request has to
/// say rather than the replies.
///
/// This is how a variadic `DEL`, `EXISTS` or `MGET` is served: the keys of one
/// request are in general owned by different shards, so there is no single
/// shard the request could be sent to, and it becomes one command per key.
///
/// **The set is not a transaction, and neither arm makes it one.** Each key's
/// command is atomic on the shard that owns it — that is the shard runtime's
/// guarantee and it is unaffected — but nothing spans the set, so a peer that
/// deletes three keys can be observed halfway through. Redis in cluster mode
/// makes the same trade, and it is the only one available here: the
/// alternative is a lock spanning shards, which would put the multi-key path's
/// cost in front of every single-key one.
///
/// What can land in the middle differs by arm, and it is the weaker of the two
/// that may be relied on. [`Fold::Sum`] dispatches one command at a time, so
/// another connection's work can land between any two of them. [`Fold::Array`]
/// hands an executor a whole slice at once and an executor applies an envelope
/// without yielding, so the keys of one slice that share an executor do in
/// fact move together — a consequence of how they are dispatched, not a
/// promise, and one that stops at the slice boundary in any case.
///
/// A shard that answers with an error ends the fan-out and that error is the
/// reply. A partial count reported as a total, or an array short by whatever
/// the failure cost, would be worse than a refusal: the peer cannot tell
/// either of them from the truth.
///
/// **The two folds do not dispatch alike, and the difference is deliberate.**
/// [`Fold::Array`] hands its commands to [`Router::dispatch_many`], which costs
/// about one envelope per executor rather than one cross-task hop per key, and
/// a multi-key read is what a client's cache layer compiles to. Not stopping
/// early costs it nothing: what was dispatched has already run by the time the
/// first reply is read, so an early return would spare no work that was still
/// avoidable. That, and not harmlessness, is the reason — a `GET` does leave
/// something behind when it finds a key expired. [`Fold::Sum`] stays
/// sequential because there an early return is real work not done: stopping at
/// the first error is what keeps a partial `DEL` from deleting further.
///
/// **A slice at a time, of [`CHUNK_COMMANDS`].** An executor applies an
/// envelope without yielding between its commands, so an envelope's length is
/// the delay one request can impose on every other connection whose keys live
/// on that executor's shards — the same property that bounds a drain's chunk,
/// bounded by the same number. It needs its own bound here because a request's
/// arity is not capped: `MGET` takes as many keys as the protocol's array
/// limit allows, as it does in Redis, so a pathological one is answered slowly
/// rather than refused while an ordinary one still travels in a single slice.
///
/// **Argument order is the array fold's contract**, and neither the dispatch
/// nor the slicing weakens it: `dispatch_many` reassembles its replies by
/// recorded position, so a slice comes back in the order its keys were named
/// whatever order the executors answered in, and the slices are appended in
/// the order they were cut. A key named twice is two commands and therefore
/// two entries — nothing here deduplicates.
pub async fn fan_out<R: Router>(router: &R, cmds: Vec<Command>, fold: Fold) -> Frame {
    match fold {
        Fold::Sum => {
            let mut total: i64 = 0;
            for cmd in cmds {
                total = match router.dispatch(cmd).await {
                    Reply::Removed(removed) => total.saturating_add(i64::from(removed)),
                    Reply::Integer(n) => total.saturating_add(n),
                    other => return reply_to_frame(other),
                };
            }
            Frame::Integer(total)
        }
        Fold::Array => {
            let mut entries = Vec::with_capacity(cmds.len());
            let mut pending = cmds.into_iter();
            loop {
                // The slice's length is how long one request may occupy an
                // executor, which is why it is `CHUNK_COMMANDS` rather than a
                // number of its own — see this function's doc.
                let slice: Vec<Command> = pending.by_ref().take(CHUNK_COMMANDS).collect();
                if slice.is_empty() {
                    break;
                }
                // One reply per command, or this is not an answer to the
                // request that was asked. A router that returned fewer would
                // shorten the array with nothing on the wire to say so, and a
                // client zipping its keys against the values it got back reads
                // the missing tail as cache misses it cannot tell from real
                // ones.
                let expected = slice.len();
                let replies = router.dispatch_many(slice).await;
                if replies.len() != expected {
                    return Frame::Error(UNRENDERABLE_REPLY.into());
                }
                for reply in replies {
                    match reply {
                        // A key that is not there is an entry all the same — the
                        // array's null, in its own slot, never a shorter array.
                        reply @ Reply::Bulk(_) => entries.push(reply_to_frame(reply)),
                        // First error in reply order wins, and it wins over
                        // everything behind it in its slice: that slice has
                        // already run, so this is a choice of which answer to
                        // give rather than a point the work stopped at. The
                        // slices behind it are the exception, and they are
                        // genuinely not dispatched.
                        error @ Reply::Error(_) => return reply_to_frame(error),
                        // Anything else is a reply of a shape this fold cannot
                        // put in an array — a command wired to [`Fold::Array`]
                        // whose shard answers with a count, say. Refused rather
                        // than rendered: a `:5` where an array of one was due
                        // is read happily and wrongly, and the peer has no way
                        // to tell.
                        _ => return Frame::Error(UNRENDERABLE_REPLY.into()),
                    }
                }
            }
            Frame::Array(entries)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [`join_all`]'s three claims, none of which its callers would fail
    /// loudly on: it terminates on nothing, it gathers by input order rather
    /// than completion order, and a future that parks is woken again.
    #[tokio::test]
    async fn join_all_terminates_on_an_empty_input() {
        let joined: Vec<()> = join_all(Vec::<std::future::Ready<()>>::new()).await;
        assert!(joined.is_empty());
    }

    #[tokio::test]
    async fn join_all_gathers_by_input_order_not_completion_order() {
        // Each future parks on a channel, and the channels are fired in
        // reverse. Under completion order the result would come back
        // reversed; under input order it does not.
        let mut senders = Vec::new();
        let mut futures = Vec::new();
        for i in 0..8u32 {
            let (tx, rx) = tokio::sync::oneshot::channel();
            senders.push(tx);
            futures.push(async move {
                rx.await.expect("sender held until fired");
                i
            });
        }
        let joined = tokio::spawn(join_all(futures));
        // Yield first, so every future has had a chance to park before any
        // channel fires: a future that completed on its first poll would not
        // exercise the waker path at all.
        tokio::task::yield_now().await;
        for tx in senders.into_iter().rev() {
            tx.send(()).expect("the join is still awaiting");
            tokio::task::yield_now().await;
        }
        assert_eq!(joined.await.unwrap(), (0..8u32).collect::<Vec<_>>());
    }
}
