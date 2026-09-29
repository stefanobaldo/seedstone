//! What a crash leaves a client's model knowing.
//!
//! A write acknowledged before its shard's last sync survives a crash; one
//! acknowledged after it may or may not. So a crash turns each key the
//! client owns into a set of candidates — exact when there is one — and the
//! next read of the key says which of them the node holds, or that it holds
//! none of them.

use seedstone_core::slot::shard_of;
use seedstone_resp::Frame;
use std::time::Duration;

use super::Model;
use crate::durability::world_now;
use crate::outcome::{Increment, lock};
use crate::workload::{Check, Known, counter_key};

impl Model {
    /// Adopts `known` for `slot`, written and acknowledged at `acked`.
    pub(super) fn wrote(&mut self, slot: u32, known: Known, acked: Duration) {
        let slot = slot as usize;
        let durable_at = self.durable_at(self.plain_shard[slot]);
        self.plain_history[slot].record(known.clone(), acked, durable_at);
        self.plain_state[slot] = known;
        self.plain_durable[slot] = false;
    }

    /// The durable point of `shard` as the node last reported it.
    fn durable_at(&self, shard: u16) -> Option<Duration> {
        lock(&self.shared.durable)[usize::from(shard)].map(|(_, at)| at)
    }

    /// Folds every crash this client has not seen yet into its model.
    ///
    /// For each plain slot: the candidates a crash leaves open, from the
    /// slot's history against the crash's durable point for its shard.
    /// One candidate is exact — and marked durable, so a read that
    /// disagrees is counted as a lost durable write. More than one is
    /// `Either`. Volatile slots give up their deadline unless it was
    /// acknowledged before the durable point: the family holds deadlines,
    /// not values, and a two-candidate deadline decides nothing.
    ///
    /// Called *after* a burst's replies are observed, never before. A burst
    /// that came back was served by the node its connection was opened to,
    /// and its replies can reach the client after that node crashed —
    /// turmoil delivers what was already on the wire. Absorbed first, the
    /// crash would make those writes look like writes to the node that
    /// replaced it, which no crash has touched.
    ///
    /// Whatever the node recovered is on its disk from the moment it
    /// restarts, so the history is reset to that one entry, acknowledged at
    /// the crash. A second crash before any read then starts from the same
    /// candidates instead of from a write the first crash may have taken.
    pub fn absorb_crashes(&mut self) {
        let crashes = lock(&self.shared.crashes).clone();
        for crash in &crashes[self.crashes_seen..] {
            for slot in 0..self.plain_history.len() {
                if matches!(self.plain_state[slot], Known::Nothing) {
                    continue;
                }
                let shard = self.plain_shard[slot];
                let durable_at = crash.durable[usize::from(shard)].map(|(_, at)| at);
                let candidates = flatten(self.plain_history[slot].candidates(durable_at));
                let known = if let [only] = candidates.as_slice() {
                    self.plain_durable[slot] = true;
                    only.clone()
                } else {
                    self.plain_durable[slot] = false;
                    Known::Either(candidates)
                };
                self.plain_history[slot].keep_only(known.clone(), crash.at);
                self.plain_state[slot] = known;
            }
            for slot in 0..self.deadlines.len() {
                let shard = self.volatile_shard[slot];
                let durable_at = crash.durable[usize::from(shard)].map(|(_, at)| at);
                let kept = match (self.volatile_acked[slot], durable_at) {
                    (Some(acked), Some(durable_at)) => acked < durable_at,
                    _ => false,
                };
                if !kept {
                    self.deadlines[slot] = None;
                    self.volatile_acked[slot] = None;
                }
            }
        }
        self.crashes_seen = crashes.len();
    }

    /// A burst whose replies never came: every write in it may or may not
    /// have landed. Each plain write becomes a candidate acknowledged now
    /// — which no durable point precedes — each volatile write forgets its
    /// deadline, and each increment is recorded unacknowledged: it may or
    /// may not count.
    pub fn in_flight(&mut self, checks: &[Check]) {
        let now = world_now();
        for check in checks {
            match check {
                Check::Counter { key, delta } => {
                    let shard = shard_of(counter_key(*key).as_bytes(), self.shards);
                    lock(&self.shared.increments).push(Increment {
                        shard,
                        delta: *delta,
                        acked: None,
                        later: self.crashes_seen,
                    });
                }
                Check::PlainSet { slot, value }
                | Check::PlainSetCond { slot, value, .. }
                | Check::PlainSetGet { slot, value } => {
                    self.maybe_wrote(*slot, Known::Value(value.clone()), now);
                }
                Check::PlainDel { slots } => {
                    for slot in slots {
                        self.maybe_wrote(*slot, Known::Absent, now);
                    }
                }
                Check::VolatileSet { slot, .. }
                | Check::VolatileExpire { slot, .. }
                | Check::VolatilePersist { slot } => {
                    self.deadlines[*slot as usize] = None;
                    self.volatile_acked[*slot as usize] = None;
                }
                _ => {}
            }
        }
    }

    /// A write to `slot` that may or may not have landed.
    fn maybe_wrote(&mut self, slot: u32, known: Known, at: Duration) {
        let slot = slot as usize;
        self.plain_history[slot].record_unacknowledged(known, at);
        self.plain_state[slot] = Known::Either(flatten(self.plain_history[slot].candidates(None)));
        self.plain_durable[slot] = false;
    }

    /// Holds a read of a key a crash left open against its candidates, and
    /// narrows the model to whichever the node answered.
    pub(super) fn check_either(&mut self, slot: u32, reply: &Frame) {
        let Known::Either(candidates) = &self.plain_state[slot as usize] else {
            return;
        };
        let observed = match reply {
            Frame::Null => Known::Absent,
            Frame::Bulk(got) => Known::Value(got.to_vec()),
            _ => return,
        };
        let matched = candidates.contains(&observed);
        {
            let mut tally = lock(&self.shared.tally);
            tally.either_checks += 1;
            if !matched {
                tally.phantom_writes += 1;
            }
        }
        if matched {
            self.plain_history[slot as usize].keep_only(observed.clone(), world_now());
            self.plain_state[slot as usize] = observed;
        } else {
            // Counted once: the model holds nothing it could judge the
            // next read by until its owner writes the key again.
            self.plain_state[slot as usize] = Known::Nothing;
        }
    }

    /// Whether a volatile key read dead inside its live band is a loss the
    /// node's recovery owned up to: the deadline was written before the last
    /// crash this client absorbed, and the recovery that followed reported
    /// the key's shard as having lost records. Counted as excused, and the
    /// deadline forgotten, as [`check_durable`](Self::check_durable) does
    /// for a plain key.
    pub(super) fn excused_volatile_death(&mut self, slot: u32) -> bool {
        let Some(crash_at) = lock(&self.shared.crashes)
            .get(self.crashes_seen.wrapping_sub(1))
            .map(|crash| crash.at)
        else {
            return false;
        };
        let written_before =
            self.volatile_acked[slot as usize].is_some_and(|acked| acked < crash_at);
        let shard = usize::from(self.volatile_shard[slot as usize]);
        if !(written_before && lock(&self.shared.truncated)[shard]) {
            return false;
        }
        lock(&self.shared.tally).excused_losses += 1;
        self.deadlines[slot as usize] = None;
        self.volatile_acked[slot as usize] = None;
        true
    }

    /// Counts a read of a key a crash left exactly known, as durable.
    ///
    /// A disagreement is a durable write the node did not keep — excused
    /// only when the node's recovery reported that key's shard as having
    /// lost records.
    pub(super) fn check_durable(&mut self, slot: u32, agrees: bool) {
        let shard = usize::from(self.plain_shard[slot as usize]);
        // Read before the tally is taken: one lock order everywhere.
        let reported = lock(&self.shared.truncated)[shard];
        {
            let mut tally = lock(&self.shared.tally);
            tally.durable_checks += 1;
            if !agrees {
                if reported {
                    tally.excused_losses += 1;
                } else {
                    tally.lost_durable_writes += 1;
                }
            }
        }
        if !agrees {
            // Counted once, as above.
            self.plain_state[slot as usize] = Known::Nothing;
            self.plain_durable[slot as usize] = false;
        }
    }
}

/// `candidates` with every nested `Either` spread out and every repeat
/// dropped, in first-seen order.
fn flatten(candidates: Vec<Known>) -> Vec<Known> {
    let mut flat: Vec<Known> = Vec::with_capacity(candidates.len());
    for known in candidates {
        let spread = match known {
            Known::Either(inner) => inner,
            other => vec![other],
        };
        for known in spread {
            if !flat.contains(&known) {
                flat.push(known);
            }
        }
    }
    flat
}
