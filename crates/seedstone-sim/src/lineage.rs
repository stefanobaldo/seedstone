//! What each shard has held: every effect the node's processes applied to
//! it, in the order they applied them.
//!
//! The durable points say how far a recovery must reach; they cannot say
//! what it may bring back. A start that replays records an earlier start
//! discarded past its cut, over an image of the writes that came after,
//! resumes at a position nobody can fault and serves a keyspace no process
//! ever held. The lineage is what says so: a recovered shard must be some
//! prefix of it.
//!
//! Some prefix, and not necessarily the one its position names. An image
//! is fuzzy — it may hold the effect of a record above its base — and a
//! start whose tail damage cut below that record resumes there, over a
//! keyspace the node held further on. That keyspace is one it held, and
//! from there the process that serves it carries the lineage on.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use seedstone_core::dict::Dict;
use seedstone_core::log::effect::{Effect, Owned};
use seedstone_core::shard::{Now, Replayed};
use tokio::time::Instant;

/// How far a deadline restored from an image may sit from the one its
/// record states.
///
/// An image stores a deadline as Unix milliseconds read off the node's
/// clocks at the snapshot, and a start turns it back into an instant: each
/// way rounds, and a key that went through several starts' images carries
/// one rounding per start. A run has at most a handful; the workload's
/// shortest deadline apart from one millisecond is twenty.
const DEADLINE_SLACK: Duration = Duration::from_millis(4);

/// A keyspace as the lineage folds it: each key's value and deadline.
type Keyspace = BTreeMap<Bytes, (Bytes, Option<Instant>)>;

/// One shard's record of what it held.
#[derive(Debug, Default)]
pub struct Lineage {
    /// The keyspace the effects apply over: empty, until a start serves one
    /// the lineage did not hold — counted then, and judged from there on.
    base: Keyspace,
    /// Every effect, in the order the node's serving processes applied
    /// them over `base`: the states it held are this list's prefixes.
    effects: Vec<Owned>,
    /// The current process's first sequence, and the effect it lands at:
    /// its record at `seq_base + n` is effect `index_base + n`.
    seq_base: u64,
    index_base: usize,
}

/// What a recovered shard was found to be against its lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Held {
    /// The keyspace the node held after this many effects.
    After(usize),
    /// A state no process of the node ever held.
    Never,
}

impl Lineage {
    /// Notes the record the current process appended at `seq`: whatever
    /// stood at or above it was this process's own, overwritten.
    ///
    /// # Panics
    ///
    /// If `payload` is not an effect, or `seq` leaves a gap: the harness
    /// lost track of the shard, which is no finding about the server.
    pub fn appended(&mut self, seq: u64, payload: &[u8]) {
        let at = seq
            .checked_sub(self.seq_base)
            .and_then(|n| usize::try_from(n).ok())
            .map(|n| self.index_base + n)
            .filter(|at| *at <= self.effects.len())
            .unwrap_or_else(|| panic!("record {seq} appended outside the lineage"));
        self.effects.truncate(at);
        self.effects.push(
            Effect::decode(payload)
                .expect("the node logs effects")
                .to_owned(),
        );
    }

    /// Whether `dict` is a keyspace this shard held, with deadlines
    /// resolved at `now`: the longest prefix it is, if any.
    #[must_use]
    pub fn judge(&self, dict: &Dict, now: Now) -> Held {
        let recovered = entries(dict);
        let mut folded = self.base.clone();
        let mut held = same(&recovered, &folded, now).then_some(0);
        for (applied, effect) in (1..).zip(&self.effects) {
            apply(&mut folded, effect, now);
            if same(&recovered, &folded, now) {
                held = Some(applied);
            }
        }
        held.map_or(Held::Never, Held::After)
    }

    /// A start serves `dict`, judged `held`, from `next_seq`: the effects
    /// above what it held are no longer held, and its records follow it.
    /// One it never held becomes the base the lineage goes on from — so a
    /// loss the disk excused, or one already counted, does not leave the
    /// shard unjudged for the rest of the run.
    pub fn served(&mut self, dict: &Dict, next_seq: u64, held: Held) {
        let applied = match held {
            Held::After(applied) => applied,
            Held::Never => {
                self.base = entries(dict);
                0
            }
        };
        self.effects.truncate(applied);
        self.seq_base = next_seq;
        self.index_base = applied;
    }
}

/// Applies one effect as replay does, but for a passed deadline: the key
/// stays, due now, because a later effect may move it — and whether it is
/// there is what [`same`] does not ask of a key that is dying.
fn apply(keyspace: &mut Keyspace, effect: &Owned, now: Now) {
    let resolve = |millis: Option<u64>| match millis.map(|at| now.replay_deadline(at)) {
        None => None,
        Some(Replayed::At(at)) => at,
        Some(Replayed::Past) => Some(now.instant),
    };
    match effect {
        Owned::Put {
            key,
            value,
            deadline,
        } => {
            keyspace.insert(key.clone(), (value.clone(), resolve(*deadline)));
        }
        Owned::Del { key } => {
            keyspace.remove(key);
        }
        Owned::Deadline { key, deadline } => {
            if let Some((_, at)) = keyspace.get_mut(key) {
                *at = resolve(*deadline);
            }
        }
        Owned::Flush => keyspace.clear(),
        Owned::Rebase => {}
    }
}

/// Whether two keyspaces are the same at `now`: the same keys, values and
/// deadlines, but for a deadline's rounding, and but for a key one of them
/// holds that dies within it.
fn same(recovered: &Keyspace, expected: &Keyspace, now: Now) -> bool {
    let dying =
        |deadline: Option<Instant>| deadline.is_some_and(|at| at <= now.instant + DEADLINE_SLACK);
    let close = |a: Option<Instant>, b: Option<Instant>| match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.max(b) - a.min(b) <= DEADLINE_SLACK,
        _ => false,
    };
    let agree = |key: &Bytes| match (recovered.get(key), expected.get(key)) {
        (Some((value, a)), Some((other, b))) => value == other && close(*a, *b),
        (Some((_, deadline)), None) | (None, Some((_, deadline))) => dying(*deadline),
        (None, None) => true,
    };
    recovered.keys().chain(expected.keys()).all(agree)
}

/// Every key of `dict`, with its value and deadline.
fn entries(dict: &Dict) -> Keyspace {
    let mut entries = Keyspace::new();
    let mut cursor = 0;
    loop {
        cursor = dict.scan(cursor, |key, entry| {
            entries.insert(key.clone(), (entry.value.clone(), entry.expires_at));
        });
        if cursor == 0 {
            return entries;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seedstone_core::dict::{DictSeed, Entry};

    fn put(key: &str, value: &str) -> Vec<u8> {
        let mut out = Vec::new();
        Effect::Put {
            key: key.as_bytes(),
            value: value.as_bytes(),
            deadline: None,
        }
        .encode(&mut out);
        out
    }

    fn now() -> Now {
        Now {
            instant: Instant::now(),
            unix_millis: 0,
        }
    }

    fn dict(pairs: &[(&str, &str)]) -> Dict {
        let mut dict = Dict::with_seed(DictSeed { k0: 1, k1: 2 });
        for (key, value) in pairs {
            dict.insert(
                Bytes::copy_from_slice(key.as_bytes()),
                Entry {
                    value: Bytes::copy_from_slice(value.as_bytes()),
                    expires_at: None,
                    touched: 0,
                },
            );
        }
        dict
    }

    #[test]
    fn every_prefix_was_held_and_nothing_else() {
        let mut lineage = Lineage::default();
        lineage.appended(0, &put("a", "1"));
        lineage.appended(1, &put("b", "1"));
        lineage.appended(2, &put("a", "2"));
        assert_eq!(lineage.judge(&dict(&[]), now()), Held::After(0));
        assert_eq!(lineage.judge(&dict(&[("a", "1")]), now()), Held::After(1));
        assert_eq!(
            lineage.judge(&dict(&[("a", "1"), ("b", "1")]), now()),
            Held::After(2)
        );
        assert_eq!(
            lineage.judge(&dict(&[("a", "2"), ("b", "1")]), now()),
            Held::After(3)
        );
        // A key's later value without the write before it.
        assert_eq!(lineage.judge(&dict(&[("a", "2")]), now()), Held::Never);
    }

    /// The case the lineage exists for: a cut's discarded records replayed
    /// over what came after it.
    #[test]
    fn records_a_cut_discarded_are_not_held_after_it() {
        let mut lineage = Lineage::default();
        lineage.appended(0, &put("a", "1"));
        lineage.appended(1, &put("a", "old"));
        // A start cuts behind record 1, writes its `Rebase` there, and the
        // shard writes on from 2.
        let held = lineage.judge(&dict(&[("a", "1")]), now());
        assert_eq!(held, Held::After(1));
        lineage.served(&dict(&[("a", "1")]), 2, held);
        lineage.appended(2, &put("b", "new"));
        assert_eq!(
            lineage.judge(&dict(&[("a", "1"), ("b", "new")]), now()),
            Held::After(2)
        );
        // A later start brings the discarded record back over it.
        assert_eq!(
            lineage.judge(&dict(&[("a", "old"), ("b", "new")]), now()),
            Held::Never
        );
    }

    /// An image that holds a record the crash tore from the log: the start
    /// resumes below it, over the keyspace the node held after it.
    #[test]
    fn a_state_held_at_a_later_position_is_held() {
        let mut lineage = Lineage::default();
        lineage.appended(0, &put("a", "1"));
        lineage.appended(1, &put("b", "1"));
        let held = lineage.judge(&dict(&[("a", "1"), ("b", "1")]), now());
        assert_eq!(held, Held::After(2));
        // It serves from 1, and its record there follows the state it
        // served, not the one the position named.
        lineage.served(&dict(&[("a", "1"), ("b", "1")]), 1, held);
        lineage.appended(1, &put("c", "1"));
        assert_eq!(
            lineage.judge(&dict(&[("a", "1"), ("b", "1"), ("c", "1")]), now()),
            Held::After(3)
        );
        assert_eq!(
            lineage.judge(&dict(&[("a", "1"), ("c", "1")]), now()),
            Held::Never
        );
    }

    #[test]
    fn a_shard_whose_start_served_what_it_never_held_is_judged_from_there() {
        let mut lineage = Lineage::default();
        lineage.appended(0, &put("a", "1"));
        let wrong = dict(&[("a", "2")]);
        let held = lineage.judge(&wrong, now());
        assert_eq!(held, Held::Never);
        lineage.served(&wrong, 1, held);
        lineage.appended(1, &put("b", "1"));
        assert_eq!(lineage.judge(&wrong, now()), Held::After(0));
        assert_eq!(
            lineage.judge(&dict(&[("a", "2"), ("b", "1")]), now()),
            Held::After(1)
        );
        assert_eq!(
            lineage.judge(&dict(&[("a", "1"), ("b", "1")]), now()),
            Held::Never
        );
    }
}
