//! The durability policy: how often the log is synced, and whether a write
//! waits for it. One mechanism, three presets.

use std::time::Duration;

use crate::shard::HOUSEKEEPING_TICK;

/// When the executor issues a sync of its segment, and whether it holds a
/// batch's replies until the sync covering them completes.
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
    /// last durable snapshot plus whatever the kernel had written.
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
    /// Held replies go out when the sync is issued, not when it completes.
    pub releases_on_issue: bool,
    /// Writes are applied and acknowledged while the executor is refusing.
    pub acks_while_refusing: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

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
