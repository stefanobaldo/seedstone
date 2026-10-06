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
        self.remember(slot, &known);
        self.plain_history[slot].record(known.clone(), acked, durable_at);
        self.plain_state[slot] = known;
        self.plain_durable[slot] = None;
        self.plain_since[slot] = None;
    }

    /// Adds `known`, if it is a value, to what the slot's owner ever wrote.
    fn remember(&mut self, slot: usize, known: &Known) {
        if let Known::Value(value) = known
            && !self.plain_ever[slot].contains(value)
        {
            self.plain_ever[slot].push(value.clone());
        }
    }

    /// The durable point of `shard` as the node last reported it — or none,
    /// while a crash this client has not absorbed stands between them.
    ///
    /// A reply can arrive after the crash of the node that sent it, and the
    /// node that replaced it may have reported a point of its own by then:
    /// a point about the new process's records, which says nothing about
    /// what the crash left of this client's writes to the old one. Pruning
    /// the history by it would forget a state the crash may have restored.
    fn durable_at(&self, shard: u16) -> Option<Duration> {
        if lock(&self.shared.crashes).len() > self.crashes_seen {
            return None;
        }
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
        for (index, crash) in crashes.iter().enumerate().skip(self.crashes_seen) {
            for slot in 0..self.plain_history.len() {
                if matches!(self.plain_state[slot], Known::Nothing) {
                    continue;
                }
                let shard = self.plain_shard[slot];
                let durable_at = crash.durable[usize::from(shard)].map(|(_, at)| at);
                let candidates = flatten(self.plain_history[slot].candidates(durable_at));
                let known = if let [only] = candidates.as_slice() {
                    // A claim already settled keeps the crash it was settled
                    // at: no write came since, and a loss the earlier
                    // recovery reported is still the one a read may find.
                    self.plain_durable[slot] = Some(self.plain_durable[slot].unwrap_or(index));
                    only.clone()
                } else {
                    self.plain_durable[slot] = None;
                    Known::Either(candidates)
                };
                self.plain_history[slot].keep_only(known.clone(), crash.at);
                self.plain_state[slot] = known;
                self.plain_since[slot] = Some(self.plain_since[slot].unwrap_or(index));
            }
            for slot in 0..self.deadlines.len() {
                let shard = self.volatile_shard[slot];
                let durable_at = crash.durable[usize::from(shard)].map(|(_, at)| at);
                let kept = match (self.volatile_acked[slot], durable_at) {
                    (Some(acked), Some(durable_at)) => acked < durable_at,
                    _ => false,
                };
                if kept {
                    self.volatile_kept[slot] = Some(self.volatile_kept[slot].unwrap_or(index));
                } else {
                    self.deadlines[slot] = None;
                    self.volatile_acked[slot] = None;
                    self.volatile_kept[slot] = None;
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
        self.remember(slot, &known);
        self.plain_history[slot].record_unacknowledged(known, at);
        self.plain_state[slot] = Known::Either(flatten(self.plain_history[slot].candidates(None)));
        self.plain_durable[slot] = None;
        // `plain_since` stays: a write with no reply settles nothing, so the
        // value under it still depends on whatever recovery it did before,
        // and a loss that recovery reported is still one a read may find.
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
            self.plain_since[slot as usize] = None;
        } else {
            // Counted once: the model holds nothing it could judge the
            // next read by until its owner writes the key again.
            self.plain_state[slot as usize] = Known::Nothing;
        }
    }

    /// Counts a read of a key a crash left exactly known, as durable.
    ///
    /// A disagreement is a durable write the node did not keep — excused
    /// only when the node's recovery reported that key's shard as having
    /// lost records.
    pub(super) fn check_durable(&mut self, slot: u32, agrees: bool) {
        let shard = usize::from(self.plain_shard[slot as usize]);
        // Read before the tally is taken: one lock order everywhere.
        // Dated from the earliest crash since the owner last wrote or read
        // the key, not from the crash that made the claim exact: that claim
        // rests on whatever the recoveries in between left on disk.
        let reported = self.reported_since(shard, self.plain_since[slot as usize]);
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
            self.plain_durable[slot as usize] = None;
        }
    }

    /// Opens every slot whose state was formed across a recovery that
    /// reported its shard as having lost records: such a recovery may have
    /// cut the shard below anything the model holds, back to any state the
    /// key ever had — its absence, or any value its owner wrote. So that is
    /// what the slot may now be. A value nobody wrote is still a phantom.
    ///
    /// Run before a burst's replies are judged: a reply comes from a node
    /// that has finished its recovery, so what it reported is known by then.
    pub fn open_reported(&mut self) {
        for slot in 0..self.plain_state.len() {
            if matches!(self.plain_state[slot], Known::Nothing)
                || !self.reported_since(usize::from(self.plain_shard[slot]), self.plain_since[slot])
            {
                continue;
            }
            let mut candidates = vec![Known::Absent];
            candidates.extend(self.plain_ever[slot].iter().cloned().map(Known::Value));
            let known = Known::Either(candidates);
            self.plain_history[slot].keep_only(known.clone(), world_now());
            self.plain_state[slot] = known;
            self.plain_durable[slot] = None;
            self.plain_since[slot] = None;
            lock(&self.shared.tally).excused_losses += 1;
        }
        // A deadline kept across such a recovery may be one it cut: the key
        // may hold an older deadline, or none. The family holds deadlines,
        // not values, so the model gives it up until its owner writes again.
        for slot in 0..self.deadlines.len() {
            if self.deadlines[slot].is_some()
                && self.reported_since(
                    usize::from(self.volatile_shard[slot]),
                    self.volatile_kept[slot],
                )
            {
                self.deadlines[slot] = None;
                self.volatile_acked[slot] = None;
                self.volatile_kept[slot] = None;
                lock(&self.shared.tally).excused_losses += 1;
            }
        }
    }

    /// Whether a recovery at or after crash `since` reported `shard` as
    /// having lost records — the only recoveries that could have lost a
    /// write known durable since that crash.
    fn reported_since(&self, shard: usize, since: Option<usize>) -> bool {
        let latest = lock(&self.shared.truncated)[shard];
        matches!((latest, since), (Some(latest), Some(since)) if latest >= since)
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::config::SimConfig;
    use crate::durability::CrashRecord;
    use crate::outcome::Shared;

    /// A key known exactly across a recovery that reported its shard lossy,
    /// then written again with no reply before a second crash, may be read
    /// back absent: the write settled nothing, so the value under it still
    /// depends on the recovery that reported the loss. Hostile seed 2784715
    /// at workload seed 279, read on the node as it stood before the
    /// clients asked for snapshots, called that read a phantom.
    #[test]
    fn a_write_with_no_reply_keeps_the_reported_recovery_its_key_depends_on() {
        let phantoms = Arc::new(Mutex::new(None));
        let out = Arc::clone(&phantoms);
        let mut sim = turmoil::Builder::new().build();
        sim.client("client", async move {
            let cfg = SimConfig::hostile(1, 1);
            let shared = Shared::new(&cfg);
            let mut model = Model::new(0, &cfg, shared.clone());
            let owner = usize::from(model.plain_shard[0]);
            let tick = Duration::from_millis(1);
            let crash = |durable: Option<u64>| CrashRecord {
                at: world_now(),
                durable: (0..usize::from(cfg.shards))
                    .map(|s| durable.filter(|_| s == owner).map(|seq| (seq, world_now())))
                    .collect(),
            };

            model.wrote(0, Known::Value(b"a".to_vec()), world_now());
            tokio::time::sleep(tick).await;
            // Synced after the write was acknowledged, so the crash keeps it
            // exactly; its recovery then reports the shard as lossy.
            lock(&shared.crashes).push(crash(Some(0)));
            model.absorb_crashes();
            lock(&shared.truncated)[owner] = Some(0);
            tokio::time::sleep(tick).await;
            model.in_flight(&[Check::PlainSet {
                slot: 0,
                value: b"b".to_vec(),
            }]);
            tokio::time::sleep(tick).await;
            // The shard synced again since, so the crash alone leaves the key
            // one of the two values, never its absence.
            lock(&shared.crashes).push(crash(Some(1)));
            model.absorb_crashes();
            model.open_reported();
            model.check_either(0, &Frame::Null);
            *out.lock().unwrap() = Some(lock(&shared.tally).phantom_writes);
            Ok(())
        });
        sim.run().unwrap();
        assert_eq!(*phantoms.lock().unwrap(), Some(0));
    }
}
