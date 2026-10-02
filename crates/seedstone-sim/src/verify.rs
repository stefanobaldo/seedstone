//! Reading the server's own account of itself — `INFO`, `SCAN` and `KEYS`
//! replies — and checking it against what the model knows.

use seedstone_core::shard::ReplyError;
use seedstone_resp::Frame;
use std::collections::BTreeSet;

use crate::outcome::{Shared, lock};

/// What the `SCAN` half of a client's walk found.
pub struct WalkOutcome {
    /// Whether every claim [`Model::walk`] lists held.
    pub holds: bool,
    /// The churn keys the client believes it left behind: written, and not
    /// removed since.
    pub present: BTreeSet<Vec<u8>>,
    /// The churn keys whose write or removal the node answered with the
    /// refusal: either may have happened.
    pub maybe: BTreeSet<Vec<u8>>,
}

/// Whether `reply` is the node refusing a write its log cannot keep.
///
/// A claim about nothing: a write refused while the node refuses was never
/// applied, but one whose reply was held for a sync when the log failed was
/// applied and answered with the refusal all the same.
pub fn is_refusal(reply: &Frame) -> bool {
    matches!(reply, Frame::Error(text) if text == ReplyError::LogWriteFailed.wire_text())
}

/// Whether a listing of `keys` is the family `expected`, but for the names
/// in `maybe`, which may be listed or not; under a ceiling, whether it names
/// nothing outside the two.
pub fn agrees_but_for(
    keys: &BTreeSet<Vec<u8>>,
    expected: &BTreeSet<Vec<u8>>,
    maybe: &BTreeSet<Vec<u8>>,
    evictable: bool,
) -> bool {
    if evictable {
        return keys
            .iter()
            .all(|key| expected.contains(key) || maybe.contains(key));
    }
    keys.difference(maybe).eq(expected.difference(maybe))
}

/// The cursor and the keys a `SCAN` reply carries, or `None` for a reply that
/// is not one.
pub fn scan_reply(reply: &Frame) -> Option<(u64, BTreeSet<Vec<u8>>)> {
    let Frame::Array(parts) = reply else {
        return None;
    };
    let [Frame::Bulk(next), keys] = parts.as_slice() else {
        return None;
    };
    let next = parse_u64(next)?;
    let (keys, _) = listed_keys(keys)?;
    Some((next, keys))
}

/// The keys a reply lists, and whether any of them was listed twice.
///
/// `None` for anything that is not an array of bulk strings — an error frame
/// included, because a walk that failed returned no keys rather than an empty
/// keyspace.
pub fn listed_keys(reply: &Frame) -> Option<(BTreeSet<Vec<u8>>, bool)> {
    let Frame::Array(items) = reply else {
        return None;
    };
    let mut keys = BTreeSet::new();
    let mut repeated = false;
    for item in items {
        let Frame::Bulk(key) = item else {
            return None;
        };
        repeated |= !keys.insert(key.to_vec());
    }
    Some((keys, repeated))
}

/// What the shards charged, over the `# Commandstats` lines of an `INFO`
/// document: microseconds and the calls they were spent over.
///
/// The edge's own names are skipped, and that is the whole of the filtering:
/// a line named in [`EDGE_NAMES`] carries a figure timed across a wait for
/// shards, which is a real elapsed simulated time and not a handler's. What
/// remains is exactly what the executors measured — see
/// [`SimOutcome::executor_usec`].
///
/// A document with no such section reports `(0, 0)`, which the caller tells
/// apart from a measured zero by the call count.
pub fn executor_timing(info: &Frame) -> (u64, u64) {
    let Frame::Bulk(body) = info else {
        return (0, 0);
    };
    let mut usec = 0;
    let mut calls = 0;
    for line in String::from_utf8_lossy(body).lines() {
        let Some(rest) = line.strip_prefix("cmdstat_") else {
            continue;
        };
        let Some((name, fields)) = rest.split_once(':') else {
            continue;
        };
        if seedstone_service::EDGE_NAMES.contains(&name) {
            continue;
        }
        for field in fields.split(',') {
            if let Some(value) = field.strip_prefix("usec=") {
                usec += value.parse::<u64>().unwrap_or(0);
            } else if let Some(value) = field.strip_prefix("calls=") {
                calls += value.parse::<u64>().unwrap_or(0);
            }
        }
    }
    (usec, calls)
}

/// The `evicted_keys:` figure of an `INFO stats` document, if it holds one.
pub fn evicted_keys(info: &Frame) -> Option<u64> {
    let Frame::Bulk(body) = info else {
        return None;
    };
    String::from_utf8_lossy(body).lines().find_map(|line| {
        line.strip_prefix("evicted_keys:")?
            .trim()
            .parse::<u64>()
            .ok()
    })
}

/// Holds an `INFO memory` document to the ceiling the shape configured.
///
/// The one invariant here that is about the *node* rather than about a key:
/// whatever the schedule, whatever was written, a node told to hold its
/// keyspace under `maxmemory` is under it whenever anyone looks. A document
/// with no `used_memory:` line at all decides nothing — that is a reply the
/// caller could not read, not a node over its ceiling.
///
/// Taken after every burst as well as at each client's settle and once at
/// rest by the verifier, because a breach is transient: the node reclaims
/// inside the command that crossed the line, so a reading taken only at the
/// end of a run would meet a node that had been over its ceiling all the way
/// through and was under it by then.
///
/// A free function rather than a method because both callers need it and only
/// one of them owns a [`Model`]: the check is about the node, not about a
/// client's keys.
pub fn check_ceiling(info: &Frame, ceiling: u64, shared: &Shared) {
    let Frame::Bulk(body) = info else {
        return;
    };
    let Some(used) = String::from_utf8_lossy(body).lines().find_map(|line| {
        line.strip_prefix("used_memory:")?
            .trim()
            .parse::<u64>()
            .ok()
    }) else {
        return;
    };
    let mut tally = lock(&shared.tally);
    tally.ceiling_checks += 1;
    if used > ceiling {
        tally.ceiling_breaches += 1;
    }
}

/// Reads a cursor the server issued back off the wire.
///
/// The server prints one with `u64::to_string`, so this is the exact inverse
/// and nothing more: a cursor is not a number a person typed.
pub fn parse_u64(bytes: &[u8]) -> Option<u64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}
