//! What the simulated client can and cannot put on the wire.
//!
//! The sweep reports PASS over the shapes this client reaches, and for a long
//! time those shapes were undocumented — so the PASS had no denominator and
//! nobody could say what it did not cover.
//!
//! The contract is in two halves, because a declaration alone is not evidence:
//!
//! - **Declared**, below: every command the server answers, classified as
//!   emitted (with its forms) or not emitted (with a reason). A test confronts
//!   this with the server's own command table, so a command added to the
//!   server without a line here fails the build.
//! - **Observed**, in [`crate::sweep`]: what the client actually emitted over
//!   a sweep. A form declared as emitted that no seed ever produced is a false
//!   claim, and the sweep fails on it.
//!
//! Neither half is sufficient. Together they mean the coverage claim is both
//! stated and true.
//!
//! # What a reason may say
//!
//! A [`Coverage::NotEmitted`] reason states why the workload will *never*
//! emit the command — a property of the command, not of how far this harness
//! has got. "Not built yet" is a schedule, and a schedule recorded here reads
//! as a decision to whoever finds it later; it belongs in this project's own
//! planning, not here. The consequence is deliberate: a command the workload
//! merely has not reached yet leaves only one honest way to make this table
//! pass, which is to emit it.

/// Whether the simulated client emits a command, and what that claim rests on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coverage {
    /// The client emits this command, in these argument forms.
    ///
    /// Each form is a label the workload also reports, so the observed half
    /// can be compared against this list name by name.
    Emitted { forms: &'static [&'static str] },
    /// The client does not emit this command, for this reason.
    ///
    /// A reason is required: "not covered" with no cause is how a gap becomes
    /// permanent by nobody noticing it.
    NotEmitted { reason: &'static str },
}

/// The contract. One line per command the server answers.
///
/// Ordered as the server's own table is, so the two can be read side by side.
pub const DECLARED: &[(&[u8], Coverage)] = &[
    (b"GET", Coverage::Emitted { forms: &[FORM_GET] }),
    (
        b"SET",
        Coverage::Emitted {
            forms: &[
                FORM_SET,
                FORM_SET_EX,
                FORM_SET_PX,
                FORM_SET_NX,
                FORM_SET_XX,
                FORM_SET_GET,
                FORM_SET_KEEPTTL,
            ],
        },
    ),
    (
        b"MGET",
        Coverage::Emitted {
            forms: &[FORM_MGET],
        },
    ),
    (b"DEL", Coverage::Emitted { forms: &[FORM_DEL] }),
    (
        b"EXISTS",
        Coverage::Emitted {
            forms: &[FORM_EXISTS],
        },
    ),
    (
        b"EXPIRE",
        Coverage::Emitted {
            forms: &[FORM_EXPIRE],
        },
    ),
    (b"TTL", Coverage::Emitted { forms: &[FORM_TTL] }),
    (
        b"PEXPIRE",
        Coverage::Emitted {
            forms: &[FORM_PEXPIRE],
        },
    ),
    (
        b"PERSIST",
        Coverage::Emitted {
            forms: &[FORM_PERSIST],
        },
    ),
    (
        b"INCRBY",
        Coverage::Emitted {
            forms: &[FORM_INCRBY],
        },
    ),
    (
        b"TYPE",
        Coverage::Emitted {
            forms: &[FORM_TYPE],
        },
    ),
    (
        b"STRLEN",
        Coverage::Emitted {
            forms: &[FORM_STRLEN],
        },
    ),
    (
        b"DBSIZE",
        Coverage::Emitted {
            forms: &[FORM_DBSIZE],
        },
    ),
    (
        b"KEYS",
        Coverage::Emitted {
            forms: &[FORM_KEYS],
        },
    ),
    (
        b"SCAN",
        Coverage::Emitted {
            forms: &[FORM_SCAN_MATCH, FORM_SCAN_MATCH_COUNT],
        },
    ),
    (
        b"FLUSHDB",
        Coverage::NotEmitted {
            reason: "it empties the keyspace every other client in the run is \
                     asserting over, so a single emission would turn every one \
                     of their models into a false accusation. Nothing about \
                     the harness makes this temporary: the invariants are \
                     stated over keys a client owns, and a command that \
                     removes keys it does not own cannot coexist with them.",
        },
    ),
    (
        b"PING",
        Coverage::NotEmitted {
            reason: "the reply is a constant and no keyspace state is \
                     touched, so the only thing a client could fold into the \
                     trace is that constant.",
        },
    ),
    (
        b"ECHO",
        Coverage::NotEmitted {
            reason: "the reply is the argument the client just sent; there is \
                     no server state for an invariant to be about.",
        },
    ),
    (
        b"AUTH",
        Coverage::NotEmitted {
            reason: "the simulated node is configured with no password, and \
                     an AUTH against such a node is an error the client would \
                     have nothing to assert about; the connection layer's own \
                     tests hold the state machine.",
        },
    ),
    (
        b"HELLO",
        Coverage::NotEmitted {
            reason: "it negotiates a protocol version and describes the node. \
                     The harness asserts no invariant over server identity, \
                     and the one protocol it speaks is the one the codec \
                     already encodes.",
        },
    ),
    (
        b"INFO",
        Coverage::Emitted {
            forms: &[FORM_INFO_MEMORY, FORM_INFO_STATS_COMMANDSTATS],
        },
    ),
    (
        b"COMMAND",
        Coverage::NotEmitted {
            reason: "it describes the surface rather than acting on the \
                     keyspace, and the surface is confronted with this table \
                     by the test below — a stronger check than a client \
                     asking for it at runtime.",
        },
    ),
    (
        b"CLIENT",
        Coverage::NotEmitted {
            reason: "connection-local naming: nothing in the keyspace moves \
                     and no reply is predictable from a client's model.",
        },
    ),
    (
        b"CONFIG",
        Coverage::NotEmitted {
            reason: "it reports the node's configuration, which no client \
                     model is about: the values are fixed for the run, so a \
                     client that read one would be asserting a constant it \
                     was handed at startup.",
        },
    ),
    (
        b"SLOWLOG",
        Coverage::NotEmitted {
            reason: "the slow log is empty by construction: no handler here \
                     is timed, so no command can ever be recorded in it and \
                     every reply is the same constant whatever the client \
                     did before it.",
        },
    ),
    (
        b"LATENCY",
        Coverage::NotEmitted {
            reason: "the latency monitor is disabled by construction, and \
                     the readings it would report are measurements this \
                     server does not take — so there is nothing a client \
                     could do that a later reply would reflect.",
        },
    ),
    (
        b"QUIT",
        Coverage::NotEmitted {
            reason: "it closes the connection the rest of the burst is being \
                     read on, so a client that sent one would end its own run \
                     mid-workload rather than exercise anything.",
        },
    ),
];

// The form labels, named once and shared by the two halves. A literal spelled
// twice is a literal that can disagree with itself, and the disagreement would
// surface as the sweep accusing the contract of a claim it does not make.
//
// # The two `SET` options that will never be here
//
// `EXAT` and `PXAT` name an absolute deadline in seconds or milliseconds since
// the epoch, and **no client in this harness can compute one**. The edge reads
// a wall clock through an injected `now_unix_millis`, and every simulated run
// freezes it — deliberately, so that nothing in a simulation can reach a real
// clock and make a trace a function of when it ran. A client with no wall
// clock has no absolute instant to name.
//
// That is a property of the harness rather than a gap in the workload, which
// is why it is written here and not left as a line in a plan somewhere. It
// will not change by anyone getting round to it: it changes only if simulated
// runs are given a virtual wall clock the model can read, and that is a
// different decision with the determinism argument to make again. Until then
// the two options are covered by the service layer's own tests, against the
// clock they inject there.
//
// `KEEPTTL` is here and is a weaker claim than it looks; [`crate`]'s
// `Check::PlainSet` says which half of the option a client reaches and why the
// other half is the volatile model's limit rather than the draw's.
pub(crate) const FORM_GET: &str = "GET key";
pub(crate) const FORM_SET: &str = "SET key value";
pub(crate) const FORM_SET_EX: &str = "SET key value EX seconds";
pub(crate) const FORM_SET_PX: &str = "SET key value PX millis";
pub(crate) const FORM_SET_NX: &str = "SET key value NX";
pub(crate) const FORM_SET_XX: &str = "SET key value XX";
pub(crate) const FORM_SET_GET: &str = "SET key value GET";
pub(crate) const FORM_SET_KEEPTTL: &str = "SET key value KEEPTTL";
pub(crate) const FORM_MGET: &str = "MGET key [key ...]";
pub(crate) const FORM_DEL: &str = "DEL key [key ...]";
pub(crate) const FORM_EXISTS: &str = "EXISTS key [key ...]";
pub(crate) const FORM_EXPIRE: &str = "EXPIRE key seconds";
pub(crate) const FORM_TTL: &str = "TTL key";
pub(crate) const FORM_PEXPIRE: &str = "PEXPIRE key millis";
pub(crate) const FORM_PERSIST: &str = "PERSIST key";
pub(crate) const FORM_INCRBY: &str = "INCRBY key delta";
pub(crate) const FORM_TYPE: &str = "TYPE key";
pub(crate) const FORM_STRLEN: &str = "STRLEN key";
pub(crate) const FORM_DBSIZE: &str = "DBSIZE";
pub(crate) const FORM_KEYS: &str = "KEYS pattern";
pub(crate) const FORM_SCAN_MATCH: &str = "SCAN cursor MATCH pattern";
pub(crate) const FORM_SCAN_MATCH_COUNT: &str = "SCAN cursor MATCH pattern COUNT count";
// Emitted only by the eviction shape, which is the one that has an invariant
// over this document's body. Everywhere else `INFO` is what it always was —
// node identity and counters no client model can predict — and the shape that
// reads it reads exactly the three numbers it can hold the server to:
// `used_memory` against the ceiling, `evicted_keys` against what the clients
// saw go missing, and the shards' `usec` against the zero a clock that does
// not move inside a handler owes it.
pub(crate) const FORM_INFO_MEMORY: &str = "INFO memory";
// Two sections in one request, which is what the verifier sends: an `INFO`
// builds both from a single broadcast, so asking for them together costs the
// shape's trace nothing, and a second request would cost it a command per
// shard. The third figure it reads is `usec`, held to the zero a paused clock
// owes it — see `SimOutcome::executor_usec`.
pub(crate) const FORM_INFO_STATS_COMMANDSTATS: &str = "INFO stats commandstats";

/// Every form the contract claims the client emits.
///
/// The denominator the observed half is compared against. Built from
/// [`DECLARED`] rather than listed a second time, for the same reason
/// [`DECLARED`] is confronted with the server's table rather than copied from
/// it.
pub fn declared_forms() -> impl Iterator<Item = &'static str> {
    DECLARED
        .iter()
        .filter_map(|(_, coverage)| match coverage {
            Coverage::Emitted { forms } => Some(forms.iter().copied()),
            Coverage::NotEmitted { .. } => None,
        })
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn every_command_the_server_answers_is_classified() {
        let declared: BTreeSet<&[u8]> = DECLARED.iter().map(|(name, _)| *name).collect();
        let served: BTreeSet<&[u8]> = seedstone_service::command_names().collect();

        let unclassified: Vec<_> = served
            .difference(&declared)
            .map(|n| String::from_utf8_lossy(n).into_owned())
            .collect();
        assert!(
            unclassified.is_empty(),
            "the server answers commands the simulated client's contract does not \
             classify: {unclassified:?}. Add a line to DECLARED saying whether the \
             client emits it, and if not, why."
        );

        let phantom: Vec<_> = declared
            .difference(&served)
            .map(|n| String::from_utf8_lossy(n).into_owned())
            .collect();
        assert!(
            phantom.is_empty(),
            "the contract classifies commands the server does not answer: {phantom:?}"
        );
    }

    /// A name listed twice would let one line's claim hide behind another's,
    /// and the set difference above cannot see it: two entries collapse into
    /// one member.
    #[test]
    fn no_command_is_classified_twice() {
        let mut seen = BTreeSet::new();
        for (name, _) in DECLARED {
            assert!(
                seen.insert(*name),
                "{} is classified twice",
                String::from_utf8_lossy(name)
            );
        }
    }

    /// Every reason has to say something. An empty one passes the type and
    /// fails the purpose.
    #[test]
    fn every_reason_and_every_form_says_something() {
        for (name, coverage) in DECLARED {
            let name = String::from_utf8_lossy(name);
            match coverage {
                Coverage::Emitted { forms } => {
                    assert!(!forms.is_empty(), "{name} claims coverage with no form");
                    for form in *forms {
                        assert!(
                            form.starts_with(name.as_ref()),
                            "{name}'s form {form:?} does not name the command it is a form of"
                        );
                    }
                }
                Coverage::NotEmitted { reason } => {
                    assert!(!reason.is_empty(), "{name} is excluded with no reason");
                }
            }
        }
    }
}
