//! The durability policy: how often the log is synced, and whether a write
//! waits for it. One mechanism, three presets.

use std::collections::VecDeque;
use std::time::Duration;

use crate::log::writer::{WRITER_BUDGET, WriterLink};
use crate::shard::{HOUSEKEEPING_TICK, Reply, ReplyTo};

/// When the node's writer issues a sync of the segment, and whether an
/// executor holds a batch's replies until the sync covering them completes.
///
/// A sync is issued when the segment is dirty, none is in flight, and at
/// least `min_interval` has passed since the last was issued; `None` never
/// issues one, and only a snapshot makes anything durable. With
/// `hold_acks` the replies of a batch that wrote wait for the sync that
/// covers them; reads never wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncPolicy {
    pub min_interval: Option<Duration>,
    pub hold_acks: bool,
}

impl SyncPolicy {
    /// Every acknowledged write is on disk: the sync is issued the moment
    /// the previous one completes, and a batch's replies wait for it.
    pub const ALWAYS: Self = Self {
        min_interval: Some(Duration::ZERO),
        hold_acks: true,
    };
    /// The default: the log is synced at least every housekeeping tick's
    /// worth of time while there is anything to sync, and a write is
    /// acknowledged at once.
    pub const INTERVAL: Self = Self {
        min_interval: Some(HOUSEKEEPING_TICK),
        hold_acks: false,
    };
    /// The log is never synced; the kernel decides, and a crash keeps the
    /// last durable snapshot plus whatever the kernel had written. A clean
    /// stop is the one sync the log gets.
    pub const NEVER: Self = Self {
        min_interval: None,
        hold_acks: false,
    };

    /// The name the flag takes: `always`, `interval` or `never`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match (self.min_interval, self.hold_acks) {
            (Some(Duration::ZERO), true) => "always",
            (None, _) => "never",
            _ => "interval",
        }
    }

    /// The preset `name` selects, if it names one.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "always" => Some(Self::ALWAYS),
            "interval" => Some(Self::INTERVAL),
            "never" => Some(Self::NEVER),
            _ => None,
        }
    }
}

/// The deliberate defects the executor can carry, each the bug one of the
/// simulator's invariants exists to find. All off in the binary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutorPlants {
    /// Writes are applied and acknowledged while the executor is refusing.
    pub acks_while_refusing: bool,
}

/// An executor's refusal, at its end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefusalReport {
    /// The executor, named by its first shard, as a `fault` names it.
    pub executor_first_shard: u16,
    /// Writes refused while it lasted.
    pub refused: u64,
    /// Housekeeping ticks it lasted.
    pub ticks: u64,
}

/// Sends a batch's replies to whoever is waiting for them. The caller may
/// have gone away; its replies are then simply dropped.
pub fn send(to: ReplyTo, replies: Vec<Reply>) {
    match to {
        ReplyTo::Once(tx) => {
            let _ = tx.send(replies);
        }
        ReplyTo::Share(share) => share.deliver(replies),
    }
}

/// A batch handed to the writer and not yet durable: per shard it touched,
/// by offset, the shard's flushed point at that batch.
pub struct Sent {
    pub batch: u64,
    pub shards: Vec<(usize, u64)>,
}

/// A batch's replies, waiting for the sync that covers them.
pub struct Held {
    pub batch: u64,
    pub to: ReplyTo,
    pub replies: Vec<Reply>,
    /// Per reply, whether its command appended to the log: what the
    /// refusal replaces if the sync behind the batch fails.
    pub wrote: Vec<bool>,
}

/// Whether an executor accepts writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Serving,
    /// A write or a sync failed: writes are answered with the refusal and
    /// not applied, reads are served, and a forced snapshot of memory is
    /// the way out.
    Refusing {
        /// Writes refused so far.
        refused: u64,
        /// Housekeeping ticks spent refusing.
        ticks: u64,
    },
}

/// One executor's side of its link to the writer, and everything waiting
/// on it.
pub struct SyncState {
    pub policy: SyncPolicy,
    pub plants: ExecutorPlants,
    /// The link to the node's writer; `None` on a node with no log.
    pub link: Option<WriterLink>,
    /// What the batch being served handed over, not yet submitted.
    pub staging: Vec<u8>,
    /// Batches handed to the writer and not yet durable, in batch order.
    pub sent: VecDeque<Sent>,
    /// Bytes handed to the writer, cumulative.
    pub sent_bytes: u64,
    /// Bytes the writer reported written, cumulative.
    pub acked_bytes: u64,
    /// Replies waiting for a sync, in batch order.
    pub held: VecDeque<Held>,
    /// The number the next submission is tagged with.
    pub batch: u64,
    pub mode: Mode,
}

impl SyncState {
    /// Nothing sent, nothing held.
    #[must_use]
    pub const fn new(policy: SyncPolicy, plants: ExecutorPlants, link: Option<WriterLink>) -> Self {
        Self {
            policy,
            plants,
            link,
            staging: Vec::new(),
            sent: VecDeque::new(),
            sent_bytes: 0,
            acked_bytes: 0,
            held: VecDeque::new(),
            batch: 0,
            mode: Mode::Serving,
        }
    }

    /// Whether more than [`WRITER_BUDGET`] bytes were handed to the writer
    /// and not yet reported written: the inbox waits until they are.
    #[must_use]
    pub const fn over_budget(&self) -> bool {
        self.sent_bytes.saturating_sub(self.acked_bytes) > WRITER_BUDGET
    }

    /// Whether a batch that wrote waits for its sync before it is answered.
    #[must_use]
    pub const fn holds(&self) -> bool {
        self.policy.hold_acks && self.link.is_some()
    }

    /// Whether writes are being refused.
    #[must_use]
    pub const fn is_refusing(&self) -> bool {
        matches!(self.mode, Mode::Refusing { .. })
    }

    /// Answers every held batch's writes with `error`: they were applied,
    /// and no sync stands behind them. A read beside them is served, as it
    /// would have been in a batch of its own. Returns how many writes it
    /// answered.
    pub fn fail_all(&mut self, error: &Reply) -> u64 {
        let mut answered = 0;
        for Held {
            to,
            mut replies,
            wrote,
            ..
        } in self.held.drain(..)
        {
            for (reply, wrote) in replies.iter_mut().zip(wrote) {
                if wrote {
                    answered += 1;
                    if !matches!(reply, Reply::Error(_)) {
                        reply.clone_from(error);
                    }
                }
            }
            send(to, replies);
        }
        answered
    }

    /// Releases every held batch up to and including `batch`, in order.
    /// Returns how many of their commands had written.
    pub fn release_through(&mut self, batch: u64) -> u64 {
        let mut released = 0;
        while self.held.front().is_some_and(|held| held.batch <= batch) {
            let Held {
                to, replies, wrote, ..
            } = self.held.pop_front().expect("checked above");
            released += wrote.iter().filter(|wrote| **wrote).count() as u64;
            send(to, replies);
        }
        released
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    use crate::shard::ReplyError;

    #[test]
    fn the_budget_is_what_was_sent_and_not_yet_written() {
        let mut state = SyncState::new(SyncPolicy::INTERVAL, ExecutorPlants::default(), None);
        assert!(!state.over_budget());
        state.sent_bytes = WRITER_BUDGET + 1;
        assert!(state.over_budget());
        state.acked_bytes = 1;
        assert!(!state.over_budget(), "at the budget is not over it");
    }

    #[test]
    fn nothing_is_held_without_a_writer() {
        let state = SyncState::new(SyncPolicy::ALWAYS, ExecutorPlants::default(), None);
        assert!(!state.holds(), "a node with no log has nothing to wait for");
    }

    #[test]
    fn fail_all_and_release_count_the_writes_they_answered() {
        let mut state = SyncState::new(SyncPolicy::ALWAYS, ExecutorPlants::default(), None);
        for (batch, wrote) in [(0, vec![true, false]), (1, vec![true, true])] {
            let (tx, _rx) = oneshot::channel();
            state.held.push_back(Held {
                batch,
                to: ReplyTo::Once(tx),
                replies: vec![Reply::Ok; wrote.len()],
                wrote,
            });
        }
        assert_eq!(state.release_through(0), 1);
        assert_eq!(state.fail_all(&Reply::Error(ReplyError::LogWriteFailed)), 2);
        assert!(state.held.is_empty());
    }

    #[test]
    fn release_through_answers_the_covered_batches_in_order_and_keeps_the_rest() {
        let mut state = SyncState::new(SyncPolicy::ALWAYS, ExecutorPlants::default(), None);
        let mut receivers = Vec::new();
        for batch in [0, 0, 1, 2] {
            let (tx, rx) = oneshot::channel();
            state.held.push_back(Held {
                batch,
                to: ReplyTo::Once(tx),
                replies: vec![Reply::Ok],
                wrote: vec![true],
            });
            receivers.push(rx);
        }
        state.release_through(1);
        let answered: Vec<bool> = receivers
            .iter_mut()
            .map(|rx| rx.try_recv().is_ok())
            .collect();
        assert_eq!(answered, [true, true, true, false]);
        assert_eq!(state.held.len(), 1);
    }

    #[test]
    fn the_three_presets_round_trip_through_their_names() {
        for policy in [SyncPolicy::ALWAYS, SyncPolicy::INTERVAL, SyncPolicy::NEVER] {
            assert_eq!(SyncPolicy::from_name(policy.name()), Some(policy));
        }
        assert_eq!(SyncPolicy::from_name("everysec"), None);
        assert_eq!(SyncPolicy::INTERVAL.min_interval, Some(HOUSEKEEPING_TICK));
        let holds =
            [SyncPolicy::ALWAYS, SyncPolicy::INTERVAL, SyncPolicy::NEVER].map(|p| p.hold_acks);
        assert_eq!(
            holds,
            [true, false, false],
            "only `always` holds a write's reply"
        );
    }
}
