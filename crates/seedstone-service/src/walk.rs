//! The keyspace walk's edge-side kernel: one `SCAN` call across shards.
//!
//! Pure by construction — no router, no `await`, no clock — so every rule the
//! walk rests on is stated and tested here, and the async caller in the
//! parent module only feeds it what a shard answered.
//!
//! The rules are four, and the tests below are exhaustive over them:
//!
//! 1. **The key target ends the call.** `COUNT` is what Redis documents it as,
//!    a hint for how many keys the client wants back from one call.
//! 2. **The bucket ceiling is one budget across every shard the call
//!    crosses.** It bounds this call's occupancy of the node, not of any one
//!    shard, so a shard spent after eight buckets leaves the rest to the next.
//! 3. **A spent shard is followed into the next at cursor `0`** — the same two
//!    cursors the client used to send in two calls, concatenated inside one.
//! 4. **Only the last shard being spent answers `0`.** Every other stopping
//!    point is the packed `(shard, internal)` of wherever the call stopped.

/// How many low bits of a `SCAN` cursor belong to a shard's own cursor.
///
/// The remaining 16 carry the shard. A shard count is a `u16` everywhere it
/// matters, and a dict's cursor is masked to its table size — 2^48 buckets is
/// a table this implementation cannot reach — so neither half is cramped.
pub const CURSOR_INTERNAL_BITS: u32 = 48;

/// The low [`CURSOR_INTERNAL_BITS`] of a cursor: the part a shard issued.
const CURSOR_INTERNAL_MASK: u64 = (1 << CURSOR_INTERNAL_BITS) - 1;

/// Packs a shard and its own cursor into the one integer `SCAN` exchanges.
///
/// `0` is both "start the walk" and "the walk is over", which is what makes a
/// multi-shard walk expressible in a client that knows nothing about shards:
/// shard 0 begins at 0, and a call that stops at a shard boundary hands back
/// the next shard's start, which is non-zero for every shard but the first.
/// Only the last shard's completion produces 0 again.
pub fn pack_cursor(shard: u16, internal: u64) -> u64 {
    (u64::from(shard) << CURSOR_INTERNAL_BITS) | (internal & CURSOR_INTERNAL_MASK)
}

/// Splits a cursor a client handed back. Total: every `u64` is some pair.
///
/// Nothing here rejects anything. A cursor is peer-supplied, so the shard it
/// names may not exist — that is [`Crossing::begin`]'s refusal to make, and it
/// needs the shard count, which the packing deliberately does not know.
pub fn unpack_cursor(cursor: u64) -> (u16, u64) {
    let shard = u16::try_from(cursor >> CURSOR_INTERNAL_BITS)
        .expect("shifting 48 of 64 bits out leaves 16, which is a u16");
    (shard, cursor & CURSOR_INTERNAL_MASK)
}

/// A cursor whose high bits name a shard this node does not have.
///
/// A distinct type rather than `()` so the caller cannot discard it by
/// accident, and so the one thing it means is spelled where it is returned.
#[derive(Debug, PartialEq, Eq)]
pub struct InvalidCursor;

/// One envelope the crossing wants dispatched, addressed and budgeted.
#[derive(Debug, PartialEq, Eq)]
pub struct Step {
    /// Which shard: the crossing's current position, never peer-supplied
    /// beyond the first, which [`Crossing::begin`] validated.
    pub shard: u16,
    /// Where in that shard's own table to resume — `0` for a shard the call
    /// crossed into.
    pub cursor: u64,
    /// How many buckets this step may visit: everything the call has left, so
    /// a shard never spends budget the call no longer has.
    pub count: usize,
}

/// One `SCAN` call, in progress across the shards it crosses.
///
/// Driven by a caller that alternates [`wants_step`](Self::wants_step) and
/// [`feed`](Self::feed) until the first answers `None`, then takes the answer
/// from [`finish`](Self::finish).
pub struct Crossing {
    /// How many shards this node has: the boundary `shard + 1` stops at.
    shards: u16,
    /// The shard the next step addresses.
    shard: u16,
    /// That shard's own cursor, as it issued it.
    internal: u64,
    /// How many keys the client asked for — Redis's `COUNT`, a hint.
    key_target: usize,
    /// How much of the call's bucket ceiling is unspent.
    buckets_left: usize,
    /// What has been gathered so far, in dispatch order.
    keys: Vec<Vec<u8>>,
    /// Whether the last shard is spent, which is the only way to answer `0`.
    finished: bool,
}

impl Crossing {
    /// Begins a call at the cursor the client handed back.
    ///
    /// # Errors
    ///
    /// [`InvalidCursor`] when the cursor names a shard this node does not
    /// have — the caller answers the client's `invalid cursor` for it. That
    /// refusal is here rather than at dispatch because it is what keeps the
    /// answer from naming a shard to a client that has no idea there are any.
    pub fn begin(
        shards: u16,
        cursor: u64,
        key_target: usize,
        bucket_ceiling: usize,
    ) -> Result<Self, InvalidCursor> {
        let (shard, internal) = unpack_cursor(cursor);
        if shard >= shards {
            return Err(InvalidCursor);
        }
        Ok(Self {
            shards,
            shard,
            internal,
            // Both floors are defensive rather than reachable: `scan_options`
            // refuses a `COUNT` of zero and the ceiling is a constant. A
            // budget of zero would be a call that dispatches nothing and
            // hands back the cursor it was given, which is a walk that never
            // ends — cheaper to make unrepresentable here than to reason
            // about at every caller.
            key_target: key_target.max(1),
            buckets_left: bucket_ceiling.max(1),
            keys: Vec::new(),
            finished: false,
        })
    }

    /// The next envelope to dispatch, or `None` when the call is over.
    ///
    /// Three things end a call, and the order they are checked in does not
    /// matter because they are checked before any step rather than after:
    /// the last shard is spent, the client has the keys it asked for, or the
    /// bucket budget is gone.
    pub const fn wants_step(&self) -> Option<Step> {
        if self.finished || self.keys.len() >= self.key_target || self.buckets_left == 0 {
            return None;
        }
        Some(Step {
            shard: self.shard,
            cursor: self.internal,
            count: self.buckets_left,
        })
    }

    /// Feeds one shard's answer back: its next cursor, its keys, and how many
    /// buckets it visited getting them.
    ///
    /// `visited` is charged against the call's budget with a floor of one, so
    /// a step that reported nothing still costs something and the loop cannot
    /// spin on a shard that never advances.
    pub fn feed(&mut self, next_cursor: u64, keys: Vec<Vec<u8>>, visited: usize) {
        self.keys.extend(keys);
        self.buckets_left = self.buckets_left.saturating_sub(visited.max(1));
        if next_cursor != 0 {
            self.internal = next_cursor;
            return;
        }
        // This shard is spent. `shard + 1` cannot overflow: `shard < shards`,
        // and `shards` is a `u16`, so `shard` is at most `u16::MAX - 1` here.
        let next = self.shard + 1;
        if next < self.shards {
            self.shard = next;
            self.internal = 0;
        } else {
            self.finished = true;
        }
    }

    /// The call's answer: the cursor the client resumes at, and the keys.
    ///
    /// `0` only when the last shard is spent; otherwise the packed position
    /// the call stopped at, which a later call resumes from exactly.
    pub fn finish(self) -> (u64, Vec<Vec<u8>>) {
        let cursor = if self.finished {
            0
        } else {
            pack_cursor(self.shard, self.internal)
        };
        (cursor, self.keys)
    }
}

#[cfg(test)]
mod tests {
    use super::{CURSOR_INTERNAL_BITS, Crossing, pack_cursor, unpack_cursor};

    /// Drives a whole call against a stand-in for the shards.
    ///
    /// `shard_answers` is the shard side: it takes the step's shard, cursor
    /// and budget, and answers as `scan_step` would — a next cursor, keys,
    /// and buckets visited. Every answer is checked against the contract the
    /// core's step keeps, so a test cannot accidentally assert against a
    /// shard that could not exist.
    fn drive(
        shards: u16,
        start: u64,
        target: usize,
        ceiling: usize,
        mut shard_answers: impl FnMut(u16, u64, usize) -> (u64, Vec<Vec<u8>>, usize),
    ) -> (u64, Vec<Vec<u8>>, usize) {
        let mut crossing = Crossing::begin(shards, start, target, ceiling)
            .expect("this test's start cursor names a shard that exists");
        let mut steps = 0;
        while let Some(step) = crossing.wants_step() {
            steps += 1;
            let (next, keys, visited) = shard_answers(step.shard, step.cursor, step.count);
            assert!(
                visited >= 1 && visited <= step.count,
                "a shard visits at least one bucket and never more than it was asked"
            );
            crossing.feed(next, keys, visited);
        }
        let (cursor, keys) = crossing.finish();
        (cursor, keys, steps)
    }

    /// The cursor packing's three claims, at the edges where a bit-shift is
    /// wrong if it is wrong anywhere.
    #[test]
    fn a_packed_cursor_round_trips_at_every_edge() {
        let internals = [
            0u64,
            1,
            (1 << CURSOR_INTERNAL_BITS) - 1,
            1 << (CURSOR_INTERNAL_BITS - 1),
        ];
        for shard in [0u16, 1, 255, u16::MAX] {
            for internal in internals {
                let packed = pack_cursor(shard, internal);
                assert_eq!(
                    unpack_cursor(packed),
                    (shard, internal),
                    "shard {shard} internal {internal:#x}"
                );
            }
        }
    }

    #[test]
    fn a_spent_shard_is_followed_into_the_next_while_budget_lasts() {
        // Every shard holds one key and is spent in one bucket — the
        // production shape, exaggerated: the old walk cost four round trips
        // here and this one costs a client a single call.
        let (cursor, keys, steps) = drive(4, 0, 10, 256, |shard, _, _| {
            (0, vec![vec![u8::try_from(shard).unwrap()]], 1)
        });
        assert_eq!(
            cursor, 0,
            "the last shard spent is the only way to answer 0"
        );
        assert_eq!(keys.len(), 4);
        assert_eq!(steps, 4);
    }

    #[test]
    fn the_key_target_ends_the_call_with_the_next_shards_start() {
        let (cursor, keys, steps) = drive(4, 0, 2, 256, |shard, _, _| {
            (0, vec![vec![u8::try_from(shard).unwrap()]], 1)
        });
        assert_eq!(keys.len(), 2);
        assert_eq!(steps, 2);
        assert_eq!(
            unpack_cursor(cursor),
            (2, 0),
            "stopping on the target after shard 1 is spent resumes at shard 2's start"
        );
    }

    #[test]
    fn the_bucket_ceiling_is_one_budget_across_shards() {
        // Shards that never spend: each step visits exactly what it is asked
        // and hands back a cursor of its own.
        let (cursor, _, steps) = drive(4, 0, 1000, 16, |_, cursor, count| {
            (cursor + 1, vec![], count)
        });
        assert_eq!(
            steps, 1,
            "a shard that is not spent takes the whole budget, so it is the whole call"
        );
        assert_eq!(unpack_cursor(cursor), (0, 1));

        // A shard spent after six buckets leaves ten for the next.
        let mut asked = Vec::new();
        drive(4, 0, 1000, 16, |shard, _, count| {
            asked.push((shard, count));
            (0, vec![], 6.min(count))
        });
        assert_eq!(
            asked,
            vec![(0, 16), (1, 10), (2, 4)],
            "the budget carries across shards and the call ends when it is gone"
        );
    }

    #[test]
    fn a_mid_shard_cursor_resumes_that_shard_at_that_cursor() {
        let mut asked = Vec::new();
        drive(4, pack_cursor(2, 77), 1, 256, |shard, cursor, _| {
            asked.push((shard, cursor));
            (0, vec![vec![1]], 1)
        });
        assert_eq!(asked, vec![(2, 77)]);
    }

    #[test]
    fn only_the_last_shard_spent_produces_zero() {
        let (cursor, _, _) = drive(4, pack_cursor(3, 0), 10, 256, |_, _, _| (0, vec![], 1));
        assert_eq!(cursor, 0);

        let (cursor, _, _) = drive(4, pack_cursor(2, 0), 10, 1, |_, _, _| (0, vec![], 1));
        assert_eq!(
            unpack_cursor(cursor),
            (3, 0),
            "out of budget at shard 2's end is shard 3's start, not done"
        );
    }

    #[test]
    fn keys_come_back_in_dispatch_order() {
        let (_, keys, _) = drive(2, 0, 10, 256, |shard, _, _| {
            let shard = u8::try_from(shard).unwrap();
            (0, vec![vec![shard, 0], vec![shard, 1]], 1)
        });
        assert_eq!(
            keys,
            vec![vec![0, 0], vec![0, 1], vec![1, 0], vec![1, 1]],
            "shards are dispatched in order and their keys are concatenated in it"
        );
    }

    /// The bound the old `COUNT` clamp used to carry, now stated where it is
    /// true: whatever the client asks for, one call dispatches at most one
    /// envelope per shard, so the loop terminates and its cost is bounded.
    #[test]
    fn every_budget_and_shard_count_terminates() {
        for shards in [1u16, 2, 3, 16] {
            for target in [1usize, 2, 10] {
                for ceiling in [1usize, 2, 8, 256] {
                    let (_, _, steps) = drive(shards, 0, target, ceiling, |_, _, count| {
                        (0, vec![vec![0]], count.min(3))
                    });
                    assert!(
                        steps <= usize::from(shards),
                        "shards={shards} target={target} ceiling={ceiling} steps={steps}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_cursor_naming_a_missing_shard_is_refused() {
        assert!(Crossing::begin(4, pack_cursor(4, 0), 10, 256).is_err());
        assert!(Crossing::begin(4, pack_cursor(3, 0), 10, 256).is_ok());
        assert!(
            Crossing::begin(4, pack_cursor(u16::MAX, 0), 10, 256).is_err(),
            "the widest shard a cursor can name is still refused by count, not by width"
        );
    }
}
