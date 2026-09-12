//! Deadline arithmetic at the edge: spans and absolute deadlines read from a
//! command, bounded first by a constant so the hot path never reads the clock,
//! then by the clock the way Redis bounds them. The measurements behind each
//! bound are on the constants.

use crate::node::NodeInfo;
use seedstone_core::shard::{Expiry, ReplyError, parse_i64};

/// The largest span, in seconds, an expiry option may name **before the clock
/// is consulted** — the arithmetic ceiling, not Redis's.
///
/// Redis's ceiling is the clock's: it adds the span to `now` in milliseconds
/// and refuses when the sum leaves an `i64`, so its boundary is
/// `(i64::MAX - now_ms) / 1000` seconds and moves by one every second.
/// Measured on 6.2.24 and 8.10.1 on 2026-09-10, each against its own clock —
/// `now_ms` of `1789079945526` on the first and `1789079945747` on the second,
/// which put the boundary at the same second: `9223370247774828` seconds is
/// accepted and `9223370247774832` is refused by `SET … EX`, `SETEX` and
/// `EXPIRE` alike — and by `SET … PX`, `PSETEX` and `PEXPIRE` at the same
/// clock boundary, in milliseconds — while `i64::MAX / 1000` — this constant — is
/// refused by both versions. That boundary is applied by
/// [`refuse_past_the_clock`]; this constant is the cheap first check in front
/// of it, and the reason it exists is arithmetic on this side: a span in
/// seconds is multiplied by a thousand before anything is done with it, and a
/// value past `i64::MAX / 1000` cannot survive that.
///
/// It also closes a hole on this side. The shard turns a span into an
/// [`Instant`](std::time::Instant) and stores *no deadline* when that
/// arithmetic leaves the clock's range — the only answer available to it, since
/// the alternative is a panic on a number a peer chose. Reached from `EXPIRE`,
/// that would clear the deadline a key already had and still report success,
/// which is a key made immortal by an argument nobody could have meant. The
/// number is refused here, where it is still a number.
pub const MAX_EXPIRE_SECONDS: i64 = i64::MAX / 1000;

/// The largest span, in milliseconds, `PEXPIRE` may name before the clock is
/// consulted — the arithmetic ceiling, as [`MAX_EXPIRE_SECONDS`] is in its own
/// unit.
///
/// Not [`MAX_EXPIRE_SECONDS`] restated. That ceiling stands on the two reasons
/// its own documentation gives, and neither of them reaches a span already
/// written in milliseconds:
///
/// - The multiplication by a thousand the unit forces does not happen here.
/// - The immortality hole cannot be reached here either. A span so long that
///   `now` plus it leaves the clock's range is stored as *no deadline* — which
///   on the `EXPIRE` path would clear a deadline and report success — but an
///   `i64` of milliseconds is some three orders of magnitude below what
///   [`Instant`](std::time::Instant)'s seconds field holds, so every one of
///   them is a deadline the clock can still represent. Measured on this
///   platform rather than reasoned about: `Instant::checked_add` answers
///   `Some` for `Duration::from_millis(i64::MAX as u64)`.
///
/// Read beside [`MAX_EXPIRE_SECONDS`] those two paragraphs look like they
/// disagree, because the spans they talk about are the same magnitude:
/// `i64::MAX` milliseconds *is* `MAX_EXPIRE_SECONDS` seconds, to the second.
/// They do not disagree, and the reason is that neither of those spans is
/// where the immortality hole is. An [`Instant`](std::time::Instant) holds its
/// seconds in an `i64` of its own, so `checked_add` fails only where the
/// process's uptime in seconds plus the span overflows that — a sliver at the
/// very top of the range, `[i64::MAX - uptime, i64::MAX]` seconds, whose floor
/// drops by a second for every second the node has been up. Measured on this
/// platform rather than reasoned about: at an uptime of 26,674 seconds the
/// largest span `checked_add` accepted was `9_223_372_036_854_749_133`, which
/// is `i64::MAX` minus that uptime, and `EXPIRE k 9223372036854775807` is the
/// argument that lands in the sliver — no deadline stored, the one the key
/// already had cleared, `1` reported as though the expiry had been set.
///
/// `MAX_EXPIRE_SECONDS` sits some three orders of magnitude *below* that
/// sliver's floor, so it excludes the hole with room to spare rather than
/// being fitted to it: `Duration::from_secs(MAX_EXPIRE_SECONDS + 1)` is still
/// a deadline this clock represents, and so is `i64::MAX / 2`. The ceiling is
/// where it is for the reasons its own documentation gives — Redis's number,
/// and the multiplication by a thousand — and closing the hole is something it
/// does comfortably on the way past. A millisecond span is three orders below
/// the sliver for the same reason the bullet above gives, the largest one
/// there is coming to `MAX_EXPIRE_SECONDS` seconds, so no `PEXPIRE` argument
/// reaches the hole either. Both ceilings are far from it, in the same
/// direction, and neither is the edge of it.
///
/// What is left is the ceiling the two share by being one command in two units
/// — the longest deadline `EXPIRE` can name, written out in milliseconds. A
/// span `EXPIRE` refuses is not one `PEXPIRE` should accept because it was
/// spelled in a smaller unit.
///
/// Redis's ceiling here is `i64::MAX` minus its own wall-clock reading in
/// milliseconds, so it moves as the clock does, and it is the same boundary
/// in every door that takes a span: measured on 6.2.24 and 8.10.1 on
/// 2026-09-10, `PEXPIRE`, `PSETEX` and `SET … PX` each accept a span two
/// seconds below `i64::MAX - now_ms` and refuse one two seconds above, and
/// each refuses `i64::MAX`. This server reproduces that boundary rather than
/// approximating it — [`refuse_past_the_clock`] is where the comparison lives
/// — and this constant is the arithmetic filter in front of it, so a span it
/// refuses never reaches the clock at all.
pub const MAX_EXPIRE_MILLIS: i64 = MAX_EXPIRE_SECONDS * 1000;

/// A span at or below this, in milliseconds, cannot overflow the clock before
/// the year 3000, so the fast path never reads the clock for it.
///
/// `i64::MAX` minus the Unix time of 3000-01-01T00:00:00Z in milliseconds.
/// Every span a client has ever sent is some twenty orders of magnitude below
/// it; the ones above are the probes that exist to find the boundary, and
/// those pay one clock read.
pub const CLOCK_SAFE_SPAN_MILLIS: u64 = i64::MAX as u64 - 32_503_680_000_000;

/// Redis's ceiling on a span, which is the clock's: `now + span` must fit an
/// `i64` of milliseconds, so the boundary is `i64::MAX - now_ms` and moves by
/// one every millisecond. Measured on 6.2.24 and 8.10.1 (issue #27): one
/// below is accepted, one above is `invalid expire time`, and a constant
/// `i64::MAX / 1000` seconds — this server's former ceiling — is refused by
/// both, about fifty-six years of spans above where Redis stops.
///
/// The constant ceilings stay as the first check, so a span they refuse never
/// gets here, and [`CLOCK_SAFE_SPAN_MILLIS`] is the second, so a span they
/// accept reads the clock only when it is within a millennium of the boundary.
/// The hot path pays a comparison and nothing else.
pub fn refuse_past_the_clock(span_millis: u64, name: &str, node: &NodeInfo) -> Result<(), String> {
    if span_millis <= CLOCK_SAFE_SPAN_MILLIS {
        return Ok(());
    }
    let now = (node.now_unix_millis)();
    if span_millis > (i64::MAX as u64).saturating_sub(now) {
        return Err(invalid_expire(name));
    }
    Ok(())
}

/// Which member of the expiry family an option is.
///
/// The family is `EX`, `PX`, `EXAT`, `PXAT` and `KEEPTTL` — every option that
/// has something to say about how long the key lives. They are told apart
/// because repeating one is legal and naming two is not, and the parsed
/// [`Expiry`] alone cannot say which happened: `EX 10 EX 10` and `EX 10 PX 5`
/// both leave one span behind, and only one of them is a command Redis runs.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ExpiryOption {
    /// `EX`, a span in seconds.
    Ex,
    /// `PX`, a span in milliseconds.
    Px,
    /// `EXAT`, a Unix deadline in seconds.
    ExAt,
    /// `PXAT`, a Unix deadline in milliseconds.
    PxAt,
    /// `KEEPTTL`, which sets no deadline and clears none.
    KeepTtl,
}

/// What an expiry option decides about the value that follows it.
///
/// An option settles three things here and nothing else: which option it is,
/// how large the value may be, and what the value means.
pub struct ExpiryUnit {
    /// Which option this is, so a repeat can be told from a conflict.
    pub option: ExpiryOption,
    /// The largest value this unit may carry.
    pub ceiling: i64,
    /// What the value is measured from.
    pub form: ExpiryForm,
}

/// Whether an option's value is a span or a deadline.
pub enum ExpiryForm {
    /// A span, which stays in the unit the client chose all the way to the
    /// shard — see [`Expiry`] — and so only has to be wrapped. It carries
    /// this many milliseconds per unit, as `Deadline` does, for the one thing
    /// on this side that has to read the span in a common unit:
    /// [`refuse_past_the_clock`].
    Span(u64, fn(u64) -> Expiry),
    /// A deadline on the wall clock, in this many milliseconds per unit. It is
    /// turned into a span here; see [`remaining_from`].
    Deadline(u64),
}

/// The unit an expiry option names, if it names one.
pub fn expiry_unit(option: &[u8]) -> Option<ExpiryUnit> {
    if option.eq_ignore_ascii_case(b"EX") {
        Some(ExpiryUnit {
            option: ExpiryOption::Ex,
            ceiling: MAX_EXPIRE_SECONDS,
            form: ExpiryForm::Span(1_000, Expiry::Ex),
        })
    } else if option.eq_ignore_ascii_case(b"PX") {
        // The value is already in the unit a deadline is held in, so nothing
        // multiplies it and the arithmetic ceiling here is the whole `i64`.
        // The boundary that actually refuses a long span is the clock's, and
        // this door shares it with the `EX` one: measured on 2026-09-10
        // against 6.2.24 and 8.10.1, `SET k v PX 9223372036854775807` is
        // refused by both, as is a millisecond span two seconds past
        // `i64::MAX - now_ms`, while two seconds below it is accepted — see
        // [`refuse_past_the_clock`], which is where that comparison lives.
        Some(ExpiryUnit {
            option: ExpiryOption::Px,
            ceiling: i64::MAX,
            form: ExpiryForm::Span(1, Expiry::Px),
        })
    } else if option.eq_ignore_ascii_case(b"EXAT") {
        Some(ExpiryUnit {
            option: ExpiryOption::ExAt,
            ceiling: MAX_EXPIRE_SECONDS,
            form: ExpiryForm::Deadline(1_000),
        })
    } else if option.eq_ignore_ascii_case(b"PXAT") {
        Some(ExpiryUnit {
            option: ExpiryOption::PxAt,
            ceiling: i64::MAX,
            form: ExpiryForm::Deadline(1),
        })
    } else {
        None
    }
}

/// Turns the absolute deadline `EXAT`/`PXAT` names into the span the shard
/// understands, against one reading of the node's wall clock.
///
/// This is the whole of what those two options mean here, and the conversion
/// happens at this edge rather than in the shard because the shard has no wall
/// clock and is not given one. Its `now` is a [`tokio::time::Instant`] —
/// monotonic, and *virtual* under the deterministic simulator, which advances
/// it by fiat — so an absolute Unix deadline is simply not a quantity it can
/// compare against anything it holds. Reconciling the two clocks once, here,
/// keeps the shard a pure function of what it is handed and costs a
/// subtraction on the one command family that names an absolute time. Which
/// wall clock is [`NodeInfo::now_unix_millis`]'s to say, for the reason stated
/// there.
///
/// A deadline already in the past becomes `Expiry::Px(0)`, which the shard
/// resolves to exactly `now`: the key is stored and is already due, so the
/// very next command that looks at it finds it gone. That is Redis's own
/// answer to `SET k v EXAT 1` — `+OK`, and no key — and it is a shape no other
/// path produces, since a span of zero on the wire is refused before it gets
/// this far; see [`set_expire_value`].
///
/// The span is measured here, at parse time, and added to the shard's `now`
/// when the command reaches it, so the deadline lands later than the client
/// named it by however long the dispatch took. `EX` and `PX` already behave
/// exactly this way — every span this server takes is measured against the
/// clock of the layer that reads it — so an absolute deadline is no less
/// faithful than a relative one; it is the same microseconds of drift, on a
/// command that happens to say when rather than how long.
pub fn remaining_from(deadline_millis: u64, node: &NodeInfo) -> Expiry {
    let now = (node.now_unix_millis)();
    // The subtraction is the live one: it saturates for every deadline already
    // past, which is the case this function exists to answer. The conversion
    // below cannot narrow — a value that got here was held to a ceiling of
    // `i64::MAX` milliseconds — and is written defensively rather than as
    // `as`, so that a ceiling relaxed later saturates instead of wrapping.
    let remaining = u128::from(deadline_millis).saturating_sub(u128::from(now));
    Expiry::Px(u64::try_from(remaining).unwrap_or(u64::MAX))
}

/// Validates the value an expiry option names: strictly positive, and within
/// what the unit can carry.
///
/// Called once per `SET`, on the occurrence that survived the walk rather than
/// on each one — [`set_options`] says why that is observable — and once per
/// `SETEX`, on its one positional span.
///
/// Positive is a rule about the value, not about the deadline it resolves to,
/// and the two come apart for the absolute options: `EXAT 1` names a moment
/// decades gone and is a write Redis performs, while `EX 0` and `EXAT 0` are
/// both `ERR invalid expire time in 'set' command`. So a `SET` whose deadline
/// has already passed is not a write refused, but a `SET` whose *number* is
/// zero or negative is. `ceiling` is what the option's unit may carry; see
/// [`expiry_unit`]. `name` is the refusing command's own lowercase name, for
/// [`invalid_expire`] — `set` or `setex`, a literal from the table, never
/// peer-supplied.
///
/// The wording is Redis 8.10.1's. Redis 6.2.24 names the command bare — no
/// quotes, no trailing `command` — so its `SETEX` refusal reads `ERR invalid
/// expire time in setex`, and `set`, `expire` and `pexpire` read the same way
/// with their own names in that slot; the four were reworded together between
/// those versions, and this server follows the newer form for all four.
///
/// `per_unit_millis` says how to read the value as a span, for the
/// clock-relative ceiling [`refuse_past_the_clock`] applies after the
/// constant one: `Some(1000)` for `EX` and `SETEX`, `Some(1)` for `PX` and
/// `PSETEX`. `None` is for the absolute options, whose value is a deadline
/// rather than a span: no span ceiling is applied to them here, and where
/// Redis puts their boundary is outside the readings this check stands on,
/// which cover the six span commands. See [`expiry_unit`].
pub fn set_expire_value(
    value: &[u8],
    ceiling: i64,
    name: &str,
    node: &NodeInfo,
    per_unit_millis: Option<u64>,
) -> Result<u64, String> {
    let value = parse_i64(value).ok_or_else(|| ReplyError::NotAnInteger.wire_text().to_owned())?;
    if value <= 0 || value > ceiling {
        return Err(invalid_expire(name));
    }
    let value = u64::try_from(value).map_err(|_| invalid_expire(name))?;
    if let Some(per_unit_millis) = per_unit_millis {
        refuse_past_the_clock(value.saturating_mul(per_unit_millis), name, node)?;
    }
    Ok(value)
}

/// Parses `EXPIRE`'s span, which unlike `SET`'s may be zero or negative.
///
/// A deadline in the past is a deletion the client asked for in the past tense,
/// and Redis performs it. What it refuses is a span it cannot do arithmetic on
/// — see [`MAX_EXPIRE_SECONDS`], in both directions.
pub fn expire_seconds(seconds: &[u8], node: &NodeInfo) -> Result<i64, String> {
    let seconds =
        parse_i64(seconds).ok_or_else(|| ReplyError::NotAnInteger.wire_text().to_owned())?;
    if !(-MAX_EXPIRE_SECONDS..=MAX_EXPIRE_SECONDS).contains(&seconds) {
        return Err(invalid_expire("expire"));
    }
    // Only a span in the future reaches the clock, and the conversion is what
    // holds the others back: a negative span is a deletion in the past tense
    // and names no deadline the clock could overflow. The multiplication
    // cannot overflow either, the range check above having just held the
    // value to `MAX_EXPIRE_SECONDS`, which is `i64::MAX / 1000`.
    if let Ok(span) = u64::try_from(seconds) {
        refuse_past_the_clock(span * 1000, "expire", node)?;
    }
    Ok(seconds)
}

/// Parses `PEXPIRE`'s span, which like `EXPIRE`'s may be zero or negative.
///
/// Checked in one direction only, where `EXPIRE`'s span is checked in both.
/// Redis bounds a span in seconds at each end because each end is multiplied,
/// and bounds a span in milliseconds only from above — read on 6.2.24 and
/// 8.10.1, where `PEXPIRE k -9223372036854775808` deletes the key and answers
/// `1`, and where the first number below it fails the *parse* rather than the
/// range check. Every non-positive span means the same thing whatever its
/// size, so a floor here would refuse a number the command already knows what
/// to do with. [`absolute_deadline_span`] meets the same asymmetry one unit
/// out, between `EXPIREAT` and `PEXPIREAT`.
pub fn expire_millis(millis: &[u8], node: &NodeInfo) -> Result<i64, String> {
    let millis =
        parse_i64(millis).ok_or_else(|| ReplyError::NotAnInteger.wire_text().to_owned())?;
    if millis > MAX_EXPIRE_MILLIS {
        return Err(invalid_expire("pexpire"));
    }
    // As in [`expire_seconds`]: the conversion is what excludes the negatives,
    // which name a deadline already past rather than one the clock could
    // overflow.
    if let Ok(span) = u64::try_from(millis) {
        refuse_past_the_clock(span, "pexpire", node)?;
    }
    Ok(millis)
}

/// Turns the absolute Unix deadline `EXPIREAT`/`PEXPIREAT` name into the span
/// the shard understands, in milliseconds — the same reconciliation of the
/// wall clock with the shard's monotonic one that [`remaining_from`] does for
/// `SET … EXAT/PXAT`, and for the same reason.
///
/// What Redis refuses is a number it cannot do arithmetic on: a non-integer,
/// or a time whose milliseconds leave an `i64`. A time already passed — zero,
/// negative, or last year — is a deletion the client asked for in the past
/// tense, and Redis performs it (6.2.24, 8.10.1).
///
/// The range check is the multiplication overflowing rather than a pair of
/// bounds, because the two commands' boundaries are **not** each other's
/// mirror. Read on 6.2.24 and 8.10.1: `EXPIREAT` is held at both ends to
/// `±(i64::MAX / 1000)` — `-9223372036854776` is refused — while `PEXPIREAT`
/// takes every `i64` there is, `i64::MIN` included, which deletes the key and
/// answers `1`. A floor spelled `-(i64::MAX / per_unit_millis)` would refuse
/// that one millisecond value; `checked_mul` answers both commands at both
/// ends, because a millisecond count is never multiplied at all.
///
/// `per_unit_millis` is `1000` for `EXPIREAT` and `1` for `PEXPIREAT`; `name`
/// is the refusing command's own lowercase name, a literal from the table.
pub fn absolute_deadline_span(
    value: &[u8],
    per_unit_millis: i64,
    name: &str,
    node: &NodeInfo,
) -> Result<i64, String> {
    let when = parse_i64(value).ok_or_else(|| ReplyError::NotAnInteger.wire_text().to_owned())?;
    let deadline_millis = when
        .checked_mul(per_unit_millis)
        .ok_or_else(|| invalid_expire(name))?;
    if deadline_millis <= 0 {
        return Ok(0);
    }
    let deadline_millis = u64::try_from(deadline_millis).expect("checked positive above");
    let Expiry::Px(remaining) = remaining_from(deadline_millis, node) else {
        unreachable!("remaining_from answers in milliseconds")
    };
    Ok(i64::try_from(remaining).unwrap_or(i64::MAX))
}

/// The invalid-expiry message, with the command's own lowercase name — a
/// literal from the table above, never peer-supplied text.
pub fn invalid_expire(name: &str) -> String {
    format!("ERR invalid expire time in '{name}' command")
}
