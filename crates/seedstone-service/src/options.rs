//! Argument parsing for the keyed commands: `SET`'s options, `SCAN`'s, and the
//! small integer readers they share. Expiry spans are parsed here and resolved
//! in [`crate::expiry`].

use crate::node::NodeInfo;

use crate::expiry::{
    ExpiryForm, ExpiryOption, ExpiryUnit, expiry_unit, remaining_from, set_expire_value,
};
use crate::fan_out::SCAN_DEFAULT_COUNT;
use crate::{Action, Fold, Unbatched};
use seedstone_core::shard::{Command, Cond, Expiry, ReplyError, parse_i64};
use std::mem::take;

/// What a peer spelling a command's options wrong is told.
///
/// Byte-exact to Redis, which says no more than this whichever option was at
/// fault: the same text for an unknown option, for a repeated one, and for one
/// whose argument is missing.
pub const SYNTAX_ERROR: &str = "ERR syntax error";

/// Builds the action for a command that names one key or many.
///
/// Several keys become a [`fan_out`], with the atomicity that costs stated
/// there. One key stays a single dispatch — the common case, and the one that
/// travels in the drain's batch with everything else — but only where the fold
/// leaves a lone reply as it is: see [`Fold::is_identity_on_one`], which is
/// what sends a one-key `MGET` through the fan-out to be wrapped in the array
/// of one it is owed.
pub fn per_key(
    args: &mut [Vec<u8>],
    name: &'static str,
    fold: Fold,
    command: fn(Vec<u8>) -> Command,
) -> Result<Action, String> {
    match args {
        [] => Err(wrong_arity(name)),
        [key] if fold.is_identity_on_one() => Ok(Action::Dispatch(command(take(key)))),
        keys => Ok(Action::Unbatched(Unbatched::FanOut {
            cmds: keys.iter_mut().map(|key| command(take(key))).collect(),
            name,
            fold,
        })),
    }
}

/// Everything a `SET`'s options settle, in the shard's vocabulary.
///
/// A struct rather than a tuple because the last two are booleans: a caller
/// destructuring `(expiry, cond, bool, bool)` can swap the pair without the
/// compiler noticing, and `KEEPTTL` silently becoming `GET` is a defect no
/// type would catch.
pub struct SetOptions {
    /// How long the key should live, or `None` for no deadline.
    pub expiry: Option<Expiry>,
    /// The condition the write is subject to, if any.
    pub cond: Option<Cond>,
    /// Whether `KEEPTTL` was named.
    pub keep_ttl: bool,
    /// Whether `GET` was named.
    pub get: bool,
}

/// Parses the options a `SET` may carry after its key and value.
///
/// Walked left to right, case-insensitively, over `EX`, `PX`, `EXAT`, `PXAT`,
/// `NX`, `XX`, `KEEPTTL` and `GET`. Everything else — an option this server
/// does not know, two options that cannot both hold, an `EX` with nothing
/// after it — is [`SYNTAX_ERROR`], which is the single answer Redis gives to
/// all of them.
///
/// The one rule worth stating outright is what "cannot both hold" means, since
/// it is not "named twice". Redis takes the last occurrence of a repeated
/// option rather than refusing the command, and a client that builds a command
/// by appending options relies on it — so `EX 100 EX 50` is a 50-second
/// deadline. What it refuses is a *different* option from the same family:
/// `EX 10 PX 5` and `NX XX` are syntax errors, in either order, because there
/// is no answer to give a peer that asked for both. See [`ExpiryOption`].
///
/// That rule reaches further than which occurrence wins, and the difference is
/// visible from the wire. **A discarded occurrence is discarded whole, its
/// argument included, and that argument is never looked at** — so
/// `EX notanum EX 10` is a command Redis runs, while `EX 10 EX notanum` is the
/// one it refuses. This walk therefore performs syntax and family checks only,
/// carrying the surviving option and its *raw* argument, and the one argument
/// that survived is validated afterwards, once. A syntax error anywhere
/// consequently beats an invalid expire time anywhere, in either order:
/// `EX 0 BOGUS` is `ERR syntax error`, not an invalid expire time.
///
/// Both paragraphs are measurements of a live `redis-server v=8.10.0` rather
/// than reasoning about what a parser ought to do — they are the kind of
/// behaviour a server grows by accident and clients then depend on.
pub fn set_options(mut rest: &[Vec<u8>], node: &NodeInfo) -> Result<SetOptions, String> {
    // The surviving expiry option and the bytes that followed it, unread.
    let mut expiry_arg: Option<(ExpiryUnit, &[u8])> = None;
    let mut named: Option<ExpiryOption> = None;
    let mut cond: Option<Cond> = None;
    let mut keep_ttl = false;
    let mut get = false;
    while let Some((option, tail)) = rest.split_first() {
        if let Some(unit) = expiry_unit(option) {
            claim(&mut named, unit.option)?;
            let Some((value, after)) = tail.split_first() else {
                return Err(SYNTAX_ERROR.to_owned());
            };
            expiry_arg = Some((unit, value));
            rest = after;
        } else if option.eq_ignore_ascii_case(b"KEEPTTL") {
            // In the expiry family, so it conflicts with all of it: a peer
            // that asked to keep a deadline and to set one has asked for two
            // different things about the same field.
            claim(&mut named, ExpiryOption::KeepTtl)?;
            keep_ttl = true;
            rest = tail;
        } else if let Some(wanted) = condition(option) {
            if cond.is_some_and(|held| held != wanted) {
                return Err(SYNTAX_ERROR.to_owned());
            }
            cond = Some(wanted);
            rest = tail;
        } else if option.eq_ignore_ascii_case(b"GET") {
            // Conflicts with nothing: it asks about the value the write
            // replaced, which every other option leaves it free to answer.
            get = true;
            rest = tail;
        } else {
            return Err(SYNTAX_ERROR.to_owned());
        }
    }
    // The one surviving argument, validated now that nothing can discard it.
    let expiry = match expiry_arg {
        None => None,
        Some((unit, value)) => {
            // A span is held to the clock as well as to its unit's ceiling; a
            // deadline is not, its value being an absolute time rather than
            // something added to `now`.
            let per_unit_millis = match unit.form {
                ExpiryForm::Span(per_unit_millis, _) => Some(per_unit_millis),
                ExpiryForm::Deadline(_) => None,
            };
            let value = set_expire_value(value, unit.ceiling, "set", node, per_unit_millis)?;
            Some(match unit.form {
                ExpiryForm::Span(_, build) => build(value),
                // The multiplication cannot overflow: `set_expire_value` has
                // just held the value to a ceiling of `i64::MAX` milliseconds
                // expressed in this unit. Saturating anyway, so that a ceiling
                // relaxed later is a deadline further off than anyone waits
                // rather than a wrapped one.
                ExpiryForm::Deadline(unit_millis) => {
                    remaining_from(value.saturating_mul(unit_millis), node)
                }
            })
        }
    };
    Ok(SetOptions {
        expiry,
        cond,
        keep_ttl,
        get,
    })
}

/// Records that `option` was named, refusing a *different* option already held.
///
/// The last occurrence of the same option wins, which is the assignment; two
/// different ones are [`SYNTAX_ERROR`]. See [`set_options`].
pub fn claim(held: &mut Option<ExpiryOption>, option: ExpiryOption) -> Result<(), String> {
    if held.is_some_and(|already| already != option) {
        return Err(SYNTAX_ERROR.to_owned());
    }
    *held = Some(option);
    Ok(())
}

/// `SCAN`'s options: `MATCH <glob>` and `COUNT <n>`, in either order.
///
/// A repeated option takes the last occurrence, as Redis does — and there the
/// resemblance to [`set_options`] stops. `SCAN` validates every occurrence as
/// it reads it, so an earlier one keeps a veto: `COUNT 0 COUNT 10` is a syntax
/// error here where `EX 0 EX 10` is a 10-second deadline, and
/// `COUNT notanum COUNT 10` is refused where `EX notanum EX 10` is run. The
/// asymmetry is Redis's own, measured on a live `redis-server v=8.10.0`
/// against both commands rather than inferred from either; this server copies
/// it because a client that learned one command's behaviour learned it from
/// the server it is replacing.
///
/// `COUNT` must be positive: Redis answers a syntax error for zero and for a
/// negative, which is a different failure from a `COUNT` that is not a number
/// at all, and clients distinguish them. What it asks for is passed through
/// as the client's **key target** — no clamp. A huge `COUNT` is a target the
/// server's own occupancy ceiling bounds anyway: [`WALK_STEP_BUCKETS`] ends
/// the call whatever the target says, and one call dispatches at most one
/// envelope per shard.
pub fn scan_options(mut rest: &[Vec<u8>]) -> Result<(Option<Vec<u8>>, usize), String> {
    let mut pattern = None;
    let mut count = SCAN_DEFAULT_COUNT;
    while let Some((option, tail)) = rest.split_first() {
        let (value, after) = tail.split_first().ok_or_else(|| SYNTAX_ERROR.to_owned())?;
        if option.eq_ignore_ascii_case(b"MATCH") {
            pattern = Some(value.clone());
        } else if option.eq_ignore_ascii_case(b"COUNT") {
            let n =
                parse_i64(value).ok_or_else(|| ReplyError::NotAnInteger.wire_text().to_owned())?;
            if n <= 0 {
                return Err(SYNTAX_ERROR.to_owned());
            }
            // One guard, not two. `n` is positive here, so the only way the
            // conversion fails is a `usize` narrower than an `i64` meeting a
            // number too large for it — which is a number the clamp would have
            // brought down to the ceiling anyway, so the failure and the
            // success take the same branch.
            // `n` is positive here, so the only way the conversion fails
            // is a `usize` narrower than an `i64` meeting a number too large
            // for it — which is a target no keyspace could reach, and the
            // widest one this server can hold is the same answer.
            count = usize::try_from(n).unwrap_or(usize::MAX);
        } else {
            return Err(SYNTAX_ERROR.to_owned());
        }
        rest = after;
    }
    Ok((pattern, count))
}

/// Parses the canonical decimal spelling of a `u64`.
///
/// A cursor is not a number a person typed: it is one this server issued and
/// the client handed straight back, and this server issues what
/// `u64::to_string` prints. So that spelling is the only one accepted — no
/// sign, no whitespace, no leading zeros — and anything else is a cursor this
/// server did not issue, which is what [`INVALID_CURSOR`] says.
pub fn parse_u64(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    if bytes[0] == b'0' && bytes.len() > 1 {
        return None;
    }
    // Verified ASCII above, so this is valid UTF-8; checked rather than
    // asserted, so a bug in the validation is a `None` and never a panic.
    std::str::from_utf8(bytes).ok()?.parse::<u64>().ok()
}

/// The condition a `SET` option names, if it names one.
pub const fn condition(option: &[u8]) -> Option<Cond> {
    if option.eq_ignore_ascii_case(b"NX") {
        Some(Cond::Nx)
    } else if option.eq_ignore_ascii_case(b"XX") {
        Some(Cond::Xx)
    } else {
        None
    }
}

/// The arity message, with the command's own lowercase name — a literal from
/// the table above, never peer-supplied text.
pub fn wrong_arity(name: &str) -> String {
    format!("ERR wrong number of arguments for '{name}' command")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WALK_STEP_BUCKETS;

    /// The two rules `scan_options` carries that the wire tests cannot see:
    /// a repeated option is its last occurrence, and a `COUNT` from the wire
    /// is passed through as the client's key target rather than clamped.
    ///
    /// The pass-through is the point. `COUNT` is a client's hint everywhere
    /// else and this server no longer makes it anything else: the bound that
    /// used to live here is the call's occupancy ceiling, and it lives in the
    /// crossing kernel, where `every_budget_and_shard_count_terminates` pins
    /// it against every target and every shard count. A target past what any
    /// keyspace holds is answered by a cycle that ends, not by a smaller
    /// number substituted at parse time.
    #[test]
    fn scan_options_take_the_last_occurrence_and_pass_count_through() {
        let opts = |parts: &[&str]| -> (Option<Vec<u8>>, usize) {
            let owned: Vec<Vec<u8>> = parts.iter().map(|p| p.as_bytes().to_vec()).collect();
            scan_options(&owned).expect("these options parse")
        };

        assert_eq!(opts(&[]), (None, SCAN_DEFAULT_COUNT));
        assert_eq!(opts(&["COUNT", "7"]).1, 7);
        assert_eq!(
            opts(&["MATCH", "a*", "MATCH", "b*"]).0,
            Some(b"b*".to_vec()),
            "a repeated option is its last occurrence, as SET's are"
        );

        assert_eq!(
            opts(&["COUNT", &i64::MAX.to_string()]).1,
            usize::try_from(i64::MAX).expect("this gate's targets are 64-bit"),
            "a COUNT past any keyspace is still the target the client asked for"
        );
        assert_eq!(
            opts(&["COUNT", &WALK_STEP_BUCKETS.to_string()]).1,
            WALK_STEP_BUCKETS,
            "the occupancy ceiling is not a special number to a client"
        );
        // Past what an i64 spells is not a large COUNT, it is not a number —
        // the same answer Redis gives, and a different one from a COUNT of
        // zero.
        let owned = vec![b"COUNT".to_vec(), u64::MAX.to_string().into_bytes()];
        assert_eq!(
            scan_options(&owned),
            Err(ReplyError::NotAnInteger.wire_text().to_owned())
        );
    }
}
