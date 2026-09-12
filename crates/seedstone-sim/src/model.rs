//! One simulated client's model of the server: what it wrote, what it may
//! therefore observe, and the checks it runs on every reply.
//!
//! It composes (`compose_*`), observes (`observe`, `check_*`, `settle`) and
//! walks (`walk`, `walk_the_family`) — three parts along one seam, kept in
//! one file while it fits the size bound.

use rand::RngExt;
use rand::rngs::ChaCha8Rng;
use seedstone_resp::Frame;
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::Instant;

use crate::outcome::{Shared, lock};
use crate::workload::{
    COUNTER_OPS, Check, CondReply, Conn, DEADLINES, EXPIRE_SECONDS, KeyRange, Known, Op,
    PEXPIRE_MILLIS, PLAIN_END, Spelling, WALK_CHURN_DELETES, WALK_CHURN_WRITES,
    WALK_CURSOR_SHARD_SHIFT, WALK_CYCLE_STEP_BOUND, WALK_KEYS, WALK_PREFIX_STEPS, WALK_STEP_COUNT,
    command, counter_key, plain_key, volatile_key, walk_key, walk_pattern,
};
use crate::{
    LIVE_SLACK, SETTLE_CAP, STALE_SLACK, SimConfig, WalkOutcome, check_ceiling, contract,
    listed_keys, scan_reply,
};

/// One client's picture of the keys it owns, and the invariants it holds the
/// server to over them.
pub struct Model {
    /// Which client this is. Its walk keys carry it in their names, so a glob
    /// can isolate them from every other client's.
    id: u16,
    /// How many counter keys there are. The one family this client does not
    /// own a slice of: they are shared, which is where the contention is.
    counter_keys: u32,
    plain: KeyRange,
    volatile: KeyRange,
    /// What this client last wrote to each plain key it owns.
    plain_state: Vec<Known>,
    /// The deadline it last asked for on each volatile key it owns, sampled
    /// from its own clock *before* the request left — so the deadline the
    /// server computed is this instant or later, never earlier.
    deadlines: Vec<Option<Instant>>,
    /// Whether the node this client is talking to has a ceiling.
    ///
    /// The one thing that weakens the plain family's model, and it is
    /// deliberately the whole of what it weakens: a key may be *gone* when a
    /// ceiling exists, and it may never hold a value its owner did not write.
    /// Every relaxation below is that one sentence in a different
    /// vocabulary.
    evictable: bool,
    /// The ceiling itself, for the readings taken against it.
    ceiling: Option<u64>,
    shared: Shared,
}

impl Model {
    /// The model client `id` starts with: it owns nothing yet and believes
    /// nothing.
    pub fn new(id: u16, cfg: &SimConfig, shared: Shared) -> Self {
        let plain = KeyRange::new(id, cfg.plain_keys, cfg.clients);
        let volatile = KeyRange::new(id, cfg.volatile_keys, cfg.clients);
        Self {
            id,
            counter_keys: cfg.counter_keys,
            plain_state: vec![Known::Nothing; plain.len as usize],
            deadlines: vec![None; volatile.len as usize],
            evictable: cfg.maxmemory.is_some(),
            ceiling: cfg.maxmemory,
            plain,
            volatile,
            shared,
        }
    }

    /// Notes that this client put `form` on the wire.
    ///
    /// Called at the point a command is composed rather than counted from the
    /// bytes afterwards, so the label the contract is checked against is the
    /// one the generator chose and not a second reading of it.
    pub fn record_form(&self, form: &'static str) {
        lock(&self.shared.forms).insert(form);
    }

    /// One to three plain slots, for the commands that take several keys.
    ///
    /// Repeats are not prevented: `DEL k k` and `EXISTS k k` mean different
    /// things in Redis and are separately worth getting right, so the model
    /// predicts both and lets the draw decide which one it is looking at.
    fn several(&self, rng: &mut ChaCha8Rng) -> Vec<u32> {
        (0..rng.random_range(1..=3u32))
            .map(|_| self.plain.pick(rng))
            .collect()
    }

    /// A command over several of this client's plain keys.
    fn plain_command(&self, name: &str, slots: &[u32]) -> Frame {
        let mut parts = Vec::with_capacity(slots.len() + 1);
        parts.push(name.to_owned());
        parts.extend(slots.iter().map(|slot| plain_key(self.plain.key(*slot))));
        command(&parts.iter().map(String::as_str).collect::<Vec<_>>())
    }

    /// Draws one operation, and what its reply will be worth.
    ///
    /// `sent` is the instant the burst this belongs to was composed at, which
    /// is what every deadline here is measured from; `seq` is the client's
    /// own operation counter, which goes into written values so a value that
    /// turns up under the wrong key is visible as such.
    pub fn compose(&self, rng: &mut ChaCha8Rng, sent: Instant, seq: u32) -> Op {
        // Drawn here rather than inside each family so the roll is one draw
        // whichever family it lands in: a helper that rolled again would make
        // the stream a function of how the arms happen to be grouped.
        let roll = rng.random_range(0..100u32);
        match roll {
            0..COUNTER_OPS => {
                let key = rng.random_range(0..self.counter_keys);
                let delta = rng.random_range(-10..=10i64);
                Op {
                    frame: command(&["INCRBY", &counter_key(key), &delta.to_string()]),
                    check: Check::Counter(delta),
                    form: contract::FORM_INCRBY,
                }
            }
            COUNTER_OPS..PLAIN_END => self.compose_plain(roll, rng, seq),
            _ => self.compose_volatile(roll, rng, sent, seq),
        }
    }

    /// An operation on a plain key: no deadline ever, and a model that knows
    /// the exact bytes.
    fn compose_plain(&self, roll: u32, rng: &mut ChaCha8Rng, seq: u32) -> Op {
        match roll {
            COUNTER_OPS..31 => self.compose_plain_set(roll, rng, seq),
            31..38 => {
                let slot = self.plain.pick(rng);
                Op {
                    frame: command(&["GET", &plain_key(self.plain.key(slot))]),
                    check: Check::PlainGet { slot },
                    form: contract::FORM_GET,
                }
            }
            38..42 => {
                let slots = self.several(rng);
                Op {
                    frame: self.plain_command("DEL", &slots),
                    check: Check::PlainDel { slots },
                    form: contract::FORM_DEL,
                }
            }
            42..46 => {
                let slots = self.several(rng);
                Op {
                    frame: self.plain_command("EXISTS", &slots),
                    check: Check::PlainExists { slots },
                    form: contract::FORM_EXISTS,
                }
            }
            46..50 => {
                let slots = self.several(rng);
                Op {
                    frame: self.plain_command("MGET", &slots),
                    check: Check::PlainMGet { slots },
                    form: contract::FORM_MGET,
                }
            }
            50..52 => {
                let slot = self.plain.pick(rng);
                Op {
                    frame: command(&["TYPE", &plain_key(self.plain.key(slot))]),
                    check: Check::PlainType { slot },
                    form: contract::FORM_TYPE,
                }
            }
            _ => {
                let slot = self.plain.pick(rng);
                Op {
                    frame: command(&["STRLEN", &plain_key(self.plain.key(slot))]),
                    check: Check::PlainStrLen { slot },
                    form: contract::FORM_STRLEN,
                }
            }
        }
    }

    /// A `SET` of a plain key, in whichever of the algebra's forms the roll
    /// landed on.
    ///
    /// Split out of [`Model::compose_plain`] rather than drawn separately, and
    /// the roll is the one already made: a helper that rolled again would make
    /// the stream a function of how the arms happen to be grouped, which is
    /// the same rule [`Model::compose`] states for the families.
    ///
    /// What is *not* here is `EXAT` and `PXAT`, and it never will be — see
    /// [`crate::contract`] for why a client with no wall clock cannot name an
    /// absolute deadline.
    fn compose_plain_set(&self, roll: u32, rng: &mut ChaCha8Rng, seq: u32) -> Op {
        match roll {
            COUNTER_OPS..24 => {
                let slot = self.plain.pick(rng);
                let value = format!("{seq}@{}", self.plain.key(slot));
                Op {
                    frame: command(&["SET", &plain_key(self.plain.key(slot)), &value]),
                    check: Check::PlainSet {
                        slot,
                        value: value.into_bytes(),
                    },
                    form: contract::FORM_SET,
                }
            }
            24..28 => {
                // The two conditions are one arm, because they are one
                // command with the sense of a single test flipped, and the
                // model predicts both from the same fact. Splitting them
                // would be two arms that had to agree about what presence
                // means.
                let only_if_present = roll >= 26;
                // One of the two `NX` rolls spells it `SETNX` instead, for
                // `SETEX`'s reason and one more. `SETNX` reaches the parser
                // through a different table entry, so a bug in that entry is
                // one no `SET … NX` can find; and it answers the decision as
                // an integer, so a server that took the right decision and
                // put it in the `SET` spelling's frame is caught here and
                // nowhere else. It takes a roll off `SET … NX` rather than
                // adding one, so the arm's share of the hundred is unchanged
                // and only what one roll puts on the wire moves.
                let old_name = roll == 25;
                let slot = self.plain.pick(rng);
                let value = format!("{seq}@{}", self.plain.key(slot));
                let key = plain_key(self.plain.key(slot));
                let frame = if old_name {
                    command(&["SETNX", &key, &value])
                } else {
                    command(&[
                        "SET",
                        &key,
                        &value,
                        if only_if_present { "XX" } else { "NX" },
                    ])
                };
                Op {
                    frame,
                    check: Check::PlainSetCond {
                        slot,
                        value: value.into_bytes(),
                        only_if_present,
                        reply: if old_name {
                            CondReply::OneOrZero
                        } else {
                            CondReply::OkOrNull
                        },
                    },
                    form: if old_name {
                        contract::FORM_SETNX
                    } else if only_if_present {
                        contract::FORM_SET_XX
                    } else {
                        contract::FORM_SET_NX
                    },
                }
            }
            28..30 => {
                let slot = self.plain.pick(rng);
                let value = format!("{seq}@{}", self.plain.key(slot));
                Op {
                    frame: command(&["SET", &plain_key(self.plain.key(slot)), &value, "GET"]),
                    check: Check::PlainSetGet {
                        slot,
                        value: value.into_bytes(),
                    },
                    form: contract::FORM_SET_GET,
                }
            }
            _ => {
                let slot = self.plain.pick(rng);
                let value = format!("{seq}@{}", self.plain.key(slot));
                Op {
                    frame: command(&["SET", &plain_key(self.plain.key(slot)), &value, "KEEPTTL"]),
                    check: Check::PlainSet {
                        slot,
                        value: value.into_bytes(),
                    },
                    form: contract::FORM_SET_KEEPTTL,
                }
            }
        }
    }

    /// An operation on a volatile key: always a deadline, and a model that
    /// knows when — not what.
    fn compose_volatile(&self, roll: u32, rng: &mut ChaCha8Rng, sent: Instant, seq: u32) -> Op {
        match roll {
            PLAIN_END..70 => {
                let slot = self.volatile.pick(rng);
                let deadline = &DEADLINES[rng.random_range(0..DEADLINES.len())];
                let key = volatile_key(self.volatile.key(slot));
                let value = format!("{seq}@{}", self.volatile.key(slot));
                let argument = deadline.argument.to_string();
                let frame = match deadline.spelling {
                    Spelling::SetOption(option) => {
                        command(&["SET", &key, &value, option, &argument])
                    }
                    Spelling::SetEx => command(&["SETEX", &key, &argument, &value]),
                    Spelling::PSetEx => command(&["PSETEX", &key, &argument, &value]),
                };
                Op {
                    frame,
                    check: Check::VolatileSet {
                        slot,
                        deadline: sent + Duration::from_millis(deadline.millis),
                    },
                    // Carried by the deadline rather than derived from its
                    // spelling here: the two would then be two places to keep
                    // in step, and the one that drifted would be the one
                    // nothing reads.
                    form: deadline.form,
                }
            }
            70..82 => {
                let slot = self.volatile.pick(rng);
                Op {
                    frame: command(&["GET", &volatile_key(self.volatile.key(slot))]),
                    check: Check::VolatileGet { slot },
                    form: contract::FORM_GET,
                }
            }
            82..90 => {
                let slot = self.volatile.pick(rng);
                Op {
                    frame: command(&[
                        "EXPIRE",
                        &volatile_key(self.volatile.key(slot)),
                        &EXPIRE_SECONDS.to_string(),
                    ]),
                    check: Check::VolatileExpire {
                        slot,
                        deadline: sent + Duration::from_secs(EXPIRE_SECONDS),
                    },
                    form: contract::FORM_EXPIRE,
                }
            }
            90..94 => {
                let slot = self.volatile.pick(rng);
                Op {
                    frame: command(&[
                        "PEXPIRE",
                        &volatile_key(self.volatile.key(slot)),
                        &PEXPIRE_MILLIS.to_string(),
                    ]),
                    check: Check::VolatileExpire {
                        slot,
                        deadline: sent + Duration::from_millis(PEXPIRE_MILLIS),
                    },
                    form: contract::FORM_PEXPIRE,
                }
            }
            94..97 => {
                let slot = self.volatile.pick(rng);
                Op {
                    frame: command(&["PERSIST", &volatile_key(self.volatile.key(slot))]),
                    check: Check::VolatilePersist { slot },
                    form: contract::FORM_PERSIST,
                }
            }
            _ => {
                let slot = self.volatile.pick(rng);
                Op {
                    frame: command(&["TTL", &volatile_key(self.volatile.key(slot))]),
                    check: Check::Ignored,
                    form: contract::FORM_TTL,
                }
            }
        }
    }

    /// Reads a burst's replies: updates the model, and reports what the
    /// invariants make of them.
    ///
    /// In order, because the burst was applied in order — a `SET` and a later
    /// `GET` of the same key inside one burst reach their shard that way
    /// round, so the model the `GET` is judged against is the one its own
    /// predecessors left.
    pub fn observe(
        &mut self,
        replies: &[Frame],
        checks: &[Check],
        sent: Instant,
        received: Instant,
    ) {
        for (reply, check) in replies.iter().zip(checks) {
            match check {
                Check::Ignored => {}
                // Only an acknowledged increment is owed to us. Anything else
                // — an error frame, a reply shape we did not expect — is not
                // a promise the server made, so counting it would manufacture
                // a violation the system never committed. Every arm below
                // reads its reply the same way.
                Check::Counter(delta) => {
                    if matches!(reply, Frame::Integer(_)) {
                        lock(&self.shared.tally).expected += delta;
                    }
                }
                Check::PlainSet { slot, value } => {
                    self.plain_state[*slot as usize] = match reply {
                        Frame::Simple(text) if text == "OK" => Known::Value(value.clone()),
                        _ => Known::Nothing,
                    };
                }
                Check::PlainSetCond {
                    slot,
                    value,
                    only_if_present,
                    reply: spelling,
                } => {
                    let held = self.plain_state[*slot as usize].clone();
                    // The two answers a condition can give, in whichever type
                    // this spelling gives them. Anything else is the server
                    // declining to run the command at all, which is no
                    // statement about the key and leaves the model with
                    // nothing to hold — and a `SETNX` answering `+OK` lands
                    // there too, which is the point of reading the frame the
                    // spelling names rather than either frame that means yes.
                    let (took, refused) = match spelling {
                        CondReply::OkOrNull => (
                            matches!(reply, Frame::Simple(text) if text == "OK"),
                            matches!(reply, Frame::Null),
                        ),
                        CondReply::OneOrZero => (
                            matches!(reply, Frame::Integer(1)),
                            matches!(reply, Frame::Integer(0)),
                        ),
                    };
                    let present = match held {
                        Known::Nothing => None,
                        Known::Absent => Some(false),
                        Known::Value(_) => Some(true),
                    };
                    if let Some(present) = present
                        && (took || refused)
                    {
                        let mut tally = lock(&self.shared.tally);
                        tally.plain_checks += 1;
                        if took != (present == *only_if_present) {
                            tally.plain_mismatches += 1;
                        }
                    }
                    self.plain_state[*slot as usize] = if took {
                        Known::Value(value.clone())
                    } else if refused {
                        // The condition did not hold, so nothing was written
                        // and the key is exactly what it was.
                        held
                    } else {
                        Known::Nothing
                    };
                }
                Check::PlainSetGet { slot, value } => {
                    // The reply is the key's *previous* value, so it answers
                    // the question a `GET` would have — held against the model
                    // by the same code, so the two cannot disagree about what
                    // agreement means.
                    self.check_plain(*slot, reply);
                    self.plain_state[*slot as usize] = match reply {
                        // A value or its absence is the command having run.
                        Frame::Bulk(_) | Frame::Null => Known::Value(value.clone()),
                        _ => Known::Nothing,
                    };
                }
                Check::PlainDel { slots } => self.check_plain_fan_out(slots, reply, true),
                Check::PlainExists { slots } => self.check_plain_fan_out(slots, reply, false),
                Check::PlainMGet { slots } => self.check_plain_mget(slots, reply),
                Check::PlainGet { slot } => self.check_plain(*slot, reply),
                Check::PlainType { slot } => self.check_plain_shape(
                    *slot,
                    reply,
                    &Frame::Simple("none".into()),
                    &Frame::Simple("string".into()),
                ),
                Check::PlainStrLen { slot } => {
                    let held = match &self.plain_state[*slot as usize] {
                        Known::Value(value) => value.len(),
                        _ => 0,
                    };
                    let held = i64::try_from(held).expect("a written value fits an i64 length");
                    self.check_plain_shape(*slot, reply, &Frame::Integer(0), &Frame::Integer(held));
                }
                Check::VolatileSet { slot, deadline } => {
                    self.deadlines[*slot as usize] = match reply {
                        Frame::Simple(text) if text == "OK" => Some(*deadline),
                        _ => None,
                    };
                }
                // A zero says the key was already gone, which is no statement
                // about when it will next die: the model gives up on it until
                // its owner writes it again.
                Check::VolatileExpire { slot, deadline } => {
                    self.deadlines[*slot as usize] = match reply {
                        Frame::Integer(1) => Some(*deadline),
                        _ => None,
                    };
                }
                // Whatever it answered, the key carries no deadline
                // afterwards: `1` removed one, and `0` says there was none to
                // remove or no key to remove it from. So the model predicts no
                // death for it until its owner writes it with one again — and
                // it asserts nothing about the key in the meantime, because
                // the volatile family's model holds deadlines and not values.
                Check::VolatilePersist { slot } => self.deadlines[*slot as usize] = None,
                Check::VolatileGet { slot } => self.check_volatile(*slot, reply, sent, received),
            }
        }
    }

    /// Holds a variadic `DEL` or `EXISTS` against the model, and — for `DEL` —
    /// applies it.
    ///
    /// The count is the whole of what the fan-out returns, and it is exactly
    /// predictable here because the keys belong to this client alone. The two
    /// commands count differently on a repeated key, which is the point of
    /// letting the draw repeat one: `DEL k k` removes it once, `EXISTS k k`
    /// finds it twice.
    ///
    /// **Weakened under a ceiling**, and this is the one check where the
    /// weakening is not merely an excused `nil`: any of the keys named may
    /// have been reclaimed, so the model's count becomes an upper bound and
    /// anything from zero up to it agrees. A count *above* it is still a
    /// mismatch — eviction can only ever remove keys, so a fan-out finding
    /// more than this client wrote is finding something nobody wrote. What is
    /// given up is the exactness, and it is given up only on the shape that
    /// has a ceiling; every other shape decides this as strictly as before.
    fn check_plain_fan_out(&mut self, slots: &[u32], reply: &Frame, removing: bool) {
        let mut counted = 0i64;
        let mut predictable = true;
        let mut seen: Vec<u32> = Vec::with_capacity(slots.len());
        for slot in slots {
            match self.plain_state[*slot as usize] {
                Known::Nothing => predictable = false,
                Known::Absent => {}
                // A removal takes the key out, so naming it twice can only
                // remove it once; a count sees it every time it is named.
                Known::Value(_) if removing && seen.contains(slot) => {}
                Known::Value(_) => counted += 1,
            }
            seen.push(*slot);
        }

        if predictable {
            let agrees = if self.evictable {
                matches!(reply, Frame::Integer(n) if (0..=counted).contains(n))
            } else {
                *reply == Frame::Integer(counted)
            };
            let mut tally = lock(&self.shared.tally);
            tally.plain_checks += 1;
            if !agrees {
                tally.plain_mismatches += 1;
            }
        }

        if removing {
            let removed = matches!(reply, Frame::Integer(_));
            for slot in slots {
                self.plain_state[*slot as usize] = if removed {
                    Known::Absent
                } else {
                    Known::Nothing
                };
            }
        }
    }

    /// Holds an `MGET` of one to three plain keys against the model.
    ///
    /// The length is checked as strictly as the contents, and that is the
    /// half worth stating: `MGET` is the only command here whose reply
    /// *shape* is a function of how many replies the fan-out gathered, so a
    /// gather that dropped one answers a shorter array rather than a wrong
    /// one. A real client pairs the array with the keys it sent — django's
    /// `get_many` zips them — and a short array quietly becomes a run of
    /// cache misses instead of an error anybody notices. Nothing else in this
    /// harness can see that, because every other fan-out folds down to a
    /// single integer.
    ///
    /// A repeated key is not special here as it is for `DEL` and `EXISTS`:
    /// each name is its own read, and reads do not consume anything.
    fn check_plain_mget(&mut self, slots: &[u32], reply: &Frame) {
        let mut evicted = 0;
        let agrees = match reply {
            Frame::Array(values) if values.len() == slots.len() => {
                let evictable = self.evictable;
                let mut agrees = true;
                for (slot, value) in slots.iter().zip(values) {
                    let slot = *slot as usize;
                    // Element by element, exactly as [`Model::check_plain`]
                    // does it for a single `GET`: an evictable model excuses
                    // a written key that is gone and follows the server, and
                    // a wrong *value* is never excused.
                    if evictable
                        && matches!(self.plain_state[slot], Known::Value(_))
                        && matches!(value, Frame::Null)
                    {
                        self.plain_state[slot] = Known::Absent;
                        evicted += 1;
                        continue;
                    }
                    agrees &= match (&self.plain_state[slot], value) {
                        // Unpredictable on its own, and the element beside it
                        // still is: one unknown key does not excuse the rest
                        // of the array.
                        (Known::Nothing, _) => true,
                        (Known::Absent, value) => matches!(value, Frame::Null),
                        (Known::Value(expected), Frame::Bulk(got)) => got == expected,
                        (Known::Value(_), _) => false,
                    };
                }
                agrees
            }
            _ => false,
        };
        let mut tally = lock(&self.shared.tally);
        tally.plain_checks += 1;
        tally.evictions_observed += evicted;
        if !agrees {
            tally.plain_mismatches += 1;
        }
    }

    /// Holds a reply about a plain key's *shape* — its type or its length —
    /// against the model.
    ///
    /// One helper for `TYPE` and `STRLEN` because they ask the same question
    /// in two vocabularies: presence, and what presence implies. Each
    /// caller supplies the answer it expects for an absent key and the one it
    /// expects for the value the model holds, which is the whole of the
    /// difference between them.
    fn check_plain_shape(&self, slot: u32, reply: &Frame, absent: &Frame, present: &Frame) {
        let agrees = match &self.plain_state[slot as usize] {
            Known::Nothing => return,
            Known::Absent => reply == absent,
            // Under a ceiling the key may have been reclaimed between the
            // write and this question, so both answers are legitimate. The
            // model is not updated from it: `TYPE` and `STRLEN` do not
            // distinguish a reclaimed key from one that was never there, and
            // a `GET` will say which soon enough.
            Known::Value(_) if self.evictable => reply == present || reply == absent,
            Known::Value(_) => reply == present,
        };
        let mut tally = lock(&self.shared.tally);
        tally.plain_checks += 1;
        if !agrees {
            tally.plain_mismatches += 1;
        }
    }

    /// Asks the node what it is holding, and holds it to its ceiling.
    ///
    /// A no-op with no ceiling, so a client on any other shape sends nothing:
    /// this is the eviction shape's frame and it does not belong in the
    /// traces every other shape produces.
    pub async fn probe_ceiling(&self, conn: &mut Conn) -> turmoil::Result<()> {
        let Some(ceiling) = self.ceiling else {
            return Ok(());
        };
        self.record_form(contract::FORM_INFO_MEMORY);
        let replies = conn.request_many(&[command(&["INFO", "memory"])]).await?;
        check_ceiling(&replies[0], ceiling, &self.shared);
        Ok(())
    }

    /// Holds a `GET` of a plain key against what this client last wrote.
    ///
    /// Sound because the family is partitioned: nothing else in the
    /// simulation writes this key, so "what I last wrote" is the whole truth
    /// about it and no schedule excuses a difference. Strict about the reply
    /// shape for the same reason the volatile check is lenient about it —
    /// there, an error frame is a question left unanswered; here, a `GET` of
    /// a key this client owns has no legitimate way to fail.
    fn check_plain(&mut self, slot: u32, reply: &Frame) {
        // The one thing a ceiling excuses, and it is excused before anything
        // else is judged: a key this client wrote is simply gone. The model
        // follows the server rather than keeping a value it now knows is not
        // there, so the next read of the same slot is decided against
        // `Absent` and is exact again.
        if self.evictable
            && matches!(self.plain_state[slot as usize], Known::Value(_))
            && matches!(reply, Frame::Null)
        {
            self.plain_state[slot as usize] = Known::Absent;
            {
                let mut tally = lock(&self.shared.tally);
                tally.plain_checks += 1;
                tally.evictions_observed += 1;
            }
            return;
        }
        let agrees = match (&self.plain_state[slot as usize], reply) {
            (Known::Nothing, _) => return,
            (Known::Absent, reply) => matches!(reply, Frame::Null),
            (Known::Value(value), Frame::Bulk(got)) => got == value,
            (Known::Value(_), _) => false,
        };
        let mut tally = lock(&self.shared.tally);
        tally.plain_checks += 1;
        if !agrees {
            tally.plain_mismatches += 1;
        }
    }

    /// Holds a `GET` of a volatile key against the deadline this client asked
    /// for.
    ///
    /// `sent` is a lower bound on when the server ran the read and `received`
    /// an upper bound, so each half takes the end that makes it conservative:
    /// a value is called stale only when even the *earliest* the read could
    /// have run was past the deadline, and an absence spurious only when even
    /// the *latest* it could have run was before it. Between the two the
    /// client says nothing — which is not a pass, and is why what was decided
    /// is counted beside what was violated.
    ///
    /// The two ends take different bands, [`STALE_SLACK`] and [`LIVE_SLACK`],
    /// because they are not owed the same thing; each constant carries its own
    /// derivation.
    ///
    /// **Under a ceiling only the *stale* end still decides.** Eviction
    /// removes keys and never resurrects one, so a value served past its
    /// deadline is a missed expiry whatever the node's memory is doing — that
    /// half is untouched. An *absence* inside the live band, though, is now
    /// explicable: the node may have reclaimed the key. So it is diverted to
    /// [`SimOutcome::evictions_observed`] rather than counted as a decided
    /// check the invariant happened to pass, and the model forgets the
    /// deadline so one reclaimed key is observed once and not on every read
    /// that follows. A *present* key inside the band is still a decided
    /// check, which is what keeps `alive_checks` a count of what the
    /// invariant actually settled.
    fn check_volatile(&mut self, slot: u32, reply: &Frame, sent: Instant, received: Instant) {
        let Some(deadline) = self.deadlines[slot as usize] else {
            return;
        };
        // Only a value or its absence answers the question. Anything else is
        // the server declining to, and counting it would inflate the very
        // number that says this invariant ran.
        let present = match reply {
            Frame::Bulk(_) => true,
            Frame::Null => false,
            _ => return,
        };
        if self.evictable && !present && received + LIVE_SLACK < deadline {
            self.deadlines[slot as usize] = None;
            lock(&self.shared.tally).evictions_observed += 1;
            return;
        }
        let mut tally = lock(&self.shared.tally);
        if sent > deadline + STALE_SLACK {
            tally.dead_checks += 1;
            if present {
                tally.stale_reads += 1;
            }
        } else if received + LIVE_SLACK < deadline {
            tally.alive_checks += 1;
            if !present {
                tally.spurious_deaths += 1;
            }
        }
    }

    /// The last thing a client does: wait out the deadlines it asked for, as
    /// far as [`SETTLE_CAP`] allows, then read back everything it owns.
    ///
    /// This is where the active sweep is under test. The workload is over in
    /// a fraction of a simulated second and the deadlines it handed out are
    /// longer than that, so without the wait a run would end with the
    /// keyspace full of entries nothing had reclaimed and nothing would ever
    /// have looked. After it, two things must hold at once: every volatile
    /// key whose deadline was waited out is gone, and every plain key — which
    /// no deadline was ever put on — is exactly what its owner wrote. A sweep
    /// that eats the living fails the second; a server that spares the dead
    /// fails the first.
    pub async fn settle(&mut self, conn: &mut Conn, depth: usize) -> turmoil::Result<()> {
        if let Some(last) = self.deadlines.iter().flatten().max() {
            // A millisecond past the staleness band — this is a wait for
            // deadlines to pass, so it is that side's band it has to clear —
            // leaving a deadline waited out decidedly behind us and the read
            // below counting as a check. Never longer than [`SETTLE_CAP`],
            // whose documentation says what the wait costs and what capping it
            // gives up.
            let until =
                (*last).min(Instant::now() + SETTLE_CAP) + STALE_SLACK + Duration::from_millis(1);
            tokio::time::sleep_until(until).await;
        }

        let mut frames = Vec::new();
        let mut checks = Vec::new();
        for slot in 0..self.volatile.len {
            frames.push(command(&["GET", &volatile_key(self.volatile.key(slot))]));
            checks.push(Check::VolatileGet { slot });
        }
        for slot in 0..self.plain.len {
            frames.push(command(&["GET", &plain_key(self.plain.key(slot))]));
            checks.push(Check::PlainGet { slot });
        }
        // The one keyspace-wide command a client here may send, and the only
        // one in the workload that reaches every shard from a single request.
        // What it answers is the whole simulation's keyspace, which no client
        // owns and none can predict, so it carries no claim — it is here
        // because the broadcast path is otherwise driven only by the service
        // layer's own tests, never by a client competing with fifteen others
        // for the same executors. Sent once per client rather than drawn into
        // the burst schedule: at one envelope per shard it would otherwise
        // decide a run's cost by itself.
        frames.push(command(&["DBSIZE"]));
        checks.push(Check::Ignored);
        self.record_form(contract::FORM_GET);
        self.record_form(contract::FORM_DBSIZE);

        for (burst, checks) in frames.chunks(depth).zip(checks.chunks(depth)) {
            let sent = Instant::now();
            let replies = conn.request_many(burst).await?;
            self.observe(&replies, checks, sent, Instant::now());
        }
        self.probe_ceiling(conn).await?;
        Ok(())
    }

    /// Writes a set of keys nothing will touch again, walks its own family
    /// while churning the rest of it, and holds the walk to its guarantee.
    ///
    /// The concurrent case, and the one the quiescent oracle cannot reach.
    /// What is quiescent here is a *set*, not the keyspace and not even this
    /// client's family: the stable keys are written before the walk starts
    /// and nothing touches them until it ends, while the same client writes
    /// and deletes other keys of the same family between the walk's steps and
    /// the other clients mutate everything else. Growth is what makes a
    /// shard's table double with the walk in flight, which is the one case a
    /// reverse-binary cursor exists to survive and the one no quiescent
    /// assertion can produce.
    ///
    /// Five claims, and they are the guarantee split into the parts a
    /// concurrent walk can still make:
    ///
    /// - **No phantom.** Every key any step returns is one this client wrote
    ///   — a stable key or a churn key. A name nothing here ever sent is a
    ///   walk answering out of another family, another client's slice, or
    ///   nowhere at all.
    /// - **Bounded.** A cycle-completing walk finishes inside
    ///   [`WALK_CYCLE_STEP_BOUND`] steps. Exceeding it is a cursor that has
    ///   stopped converging, and it is reported with the step count rather
    ///   than as a run that hung.
    /// - **At least once.** A walk that reached the end of its cycle returned
    ///   every stable key. Only a completed walk can claim this, which is why
    ///   the prefix shape does not — see
    ///   [`SimConfig::concurrent_scan_cycle`].
    /// - **`KEYS` does not repeat, and is exact.** The closing `KEYS` is one
    ///   round trip and complete by construction, taken once the churn has
    ///   stopped, so the model knows precisely what the family holds: every
    ///   stable key, plus every churn key whose write was acknowledged and
    ///   whose removal was not. `SCAN` may return a key twice and this may
    ///   not.
    /// - **Shard-monotonic.** A call crosses shards in order and hands back
    ///   wherever it stopped, so within one cycle the shard half of every
    ///   cursor returned never decreases, and every non-zero cursor names a
    ///   shard this node has. The rest of that claim — that `0` arrives only
    ///   after the *last* shard — is not readable from a cursor, because a
    ///   call may cross any number of shards before it stops and the client
    ///   sees only where it stopped. What carries it is at-least-once: a `0`
    ///   handed back before the last shard was walked is a cycle that left
    ///   keys unreturned. See `Plant::CrossingSkipsShard`, which is exactly
    ///   that defect.
    ///
    /// **What the prefix shape's steps can and cannot see, measured rather
    /// than assumed.** A step is a shard at most, so two steps cover two of a
    /// thousand and most of these walks return nothing at all: on the swept
    /// shape, two clients in a hundred and twenty-eight had a key of their own
    /// in the stretch they walked. That is thin per seed and not thin across a
    /// sweep, and it is why the result-level claim every seed rests on is the
    /// closing `KEYS` rather than the steps. What the steps carry
    /// every time is the rest of it — a well-formed reply, a cursor that
    /// moves, and nothing returned that belongs to anyone else — under a
    /// schedule, which is coverage `SCAN` had nowhere before. Widening it is
    /// not a matter of taking more steps: a spent shard hands back the next
    /// one's start rather than continuing into it, so a step is a shard
    /// whatever its bucket budget, and the fix is to let one call cross that
    /// boundary.
    ///
    /// The two `SCAN` forms are split by client id rather than alternated
    /// within a walk. A step carrying no `COUNT` takes the server's own
    /// bucket budget, which is large enough to finish a small shard's table
    /// in one call; a walk built out of those has no cursor between its steps
    /// for anything to happen underneath. Alternating would give every walk
    /// half of that and leave none of them stepping bucket by bucket, so the
    /// choice is per client: both parse paths are exercised in every run, and
    /// the odd-numbered clients are the ones whose cursor is genuinely in
    /// flight.
    pub async fn walk(
        &self,
        conn: &mut Conn,
        cfg: &SimConfig,
        depth: usize,
    ) -> turmoil::Result<()> {
        let names: Vec<String> = (0..WALK_KEYS).map(|slot| walk_key(self.id, slot)).collect();
        // The value is the key: nothing reads it back, and a value that names
        // its own key is what makes a mis-shelved one legible if something
        // ever does.
        let writes: Vec<Frame> = names
            .iter()
            .map(|name| command(&["SET", name, name]))
            .collect();

        self.record_form(contract::FORM_SET);
        self.record_form(contract::FORM_DEL);
        self.record_form(contract::FORM_KEYS);
        self.record_form(if self.names_a_count() {
            contract::FORM_SCAN_MATCH_COUNT
        } else {
            contract::FORM_SCAN_MATCH
        });

        let mut stable = BTreeSet::new();
        for (batch, burst) in writes.chunks(depth).enumerate() {
            for (offset, reply) in conn.request_many(burst).await?.into_iter().enumerate() {
                // Only an acknowledged write is a key we may insist on. A
                // refusal is a key that is legitimately absent, and demanding
                // it back would manufacture a violation.
                if reply == Frame::Simple("OK".into()) {
                    stable.insert(names[batch * depth + offset].clone().into_bytes());
                }
            }
        }

        let walk = self.walk_the_family(conn, cfg, &stable).await?;
        let mut present = stable;
        present.extend(walk.present.iter().cloned());
        lock(&self.shared.walk).extend(present.iter().cloned());

        let reply = conn
            .request_many(&[command(&["KEYS", &walk_pattern(self.id)])])
            .await?;
        {
            let mut tally = lock(&self.shared.tally);
            tally.walk_checks += 2;
            if !walk.holds {
                tally.walk_mismatches += 1;
            }
            // `Some((set, false))` is the only shape that can agree: anything
            // else is a malformed reply or a key returned twice, and `KEYS`
            // promises neither.
            //
            // Under a ceiling the set becomes an upper bound and the equality
            // a subset: a key this client wrote may have been reclaimed since,
            // and nothing about the walk can tell that from a key the server
            // lost. What the check keeps is the half eviction cannot excuse —
            // no name that was never written, and no name returned twice.
            let agrees = match listed_keys(&reply[0]) {
                Some((keys, false)) if self.evictable => keys.is_subset(&present),
                listed => listed == Some((present, false)),
            };
            if !agrees {
                tally.walk_mismatches += 1;
            }
        }
        Ok(())
    }

    /// Whether this client's walk names a `COUNT` on the wire.
    ///
    /// Split by client id rather than alternated within one walk. See
    /// [`Model::walk`] for why: a step with no `COUNT` takes the server's own
    /// bucket budget and can finish a small shard's table in one call, so a
    /// walk built out of them has no cursor in flight between its steps.
    const fn names_a_count(&self) -> bool {
        !self.id.is_multiple_of(2)
    }

    /// Drives the `SCAN` half of [`Model::walk`] and reports what it found.
    ///
    /// Every burst is churn first and the step last, in one write, so the
    /// step meets a family that has changed since the step before it. Which
    /// of the two the server reaches first is not this client's to decide and
    /// is not asserted on — the guarantee is stated over the stable set
    /// precisely because the rest of the family has no predictable answer.
    async fn walk_the_family(
        &self,
        conn: &mut Conn,
        cfg: &SimConfig,
        stable: &BTreeSet<Vec<u8>>,
    ) -> turmoil::Result<WalkOutcome> {
        let pattern = walk_pattern(self.id);
        let count = WALK_STEP_COUNT.to_string();

        // Churn keys whose write was acknowledged, in the order they were
        // written, and how many of them a removal has been aimed at. The
        // index is what makes the removals go oldest first and never twice at
        // the same key; `gone` is what says which of them the server
        // confirmed, since only a confirmed removal takes a key out of the
        // family the closing `KEYS` is held to.
        let mut written: Vec<String> = Vec::new();
        let mut attempted = 0usize;
        let mut gone: BTreeSet<Vec<u8>> = BTreeSet::new();
        // Every churn name this client has *sent*, acknowledged or not. The
        // no-phantom check is against this rather than against what was
        // acknowledged: a write whose reply said nothing may still have
        // landed, and a walk returning it is not the failure being looked for.
        let mut sent: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut next_slot = WALK_KEYS;

        let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut cursor = 0u64;
        // The shard the last cursor named. A walk starts at cursor 0, which is
        // shard 0's start, so a walk that has taken no step yet is already at
        // the floor the check holds every later cursor to.
        let mut last_shard = 0u64;
        let mut steps = 0u64;
        let mut holds = true;

        let completed = loop {
            let mut burst = Vec::new();
            let mut fresh = Vec::new();
            for _ in 0..WALK_CHURN_WRITES {
                let name = walk_key(self.id, next_slot);
                next_slot += 1;
                burst.push(command(&["SET", &name, &name]));
                fresh.push(name);
            }
            let deleting = (written.len() - attempted).min(WALK_CHURN_DELETES as usize);
            let targets: Vec<String> = written[attempted..attempted + deleting].to_vec();
            attempted += deleting;
            for name in &targets {
                burst.push(command(&["DEL", name]));
            }
            let cursor_text = cursor.to_string();
            burst.push(if self.names_a_count() {
                command(&["SCAN", &cursor_text, "MATCH", &pattern, "COUNT", &count])
            } else {
                command(&["SCAN", &cursor_text, "MATCH", &pattern])
            });

            let replies = conn.request_many(&burst).await?;
            steps += 1;
            for (name, reply) in fresh.iter().zip(&replies) {
                sent.insert(name.clone().into_bytes());
                if *reply == Frame::Simple("OK".into()) {
                    written.push(name.clone());
                }
            }
            for (name, reply) in targets.iter().zip(&replies[fresh.len()..]) {
                // A refusal removes nothing — the shard declines before it
                // touches the keyspace — so a removal only counts once the
                // server has said it happened.
                if matches!(reply, Frame::Integer(_)) {
                    gone.insert(name.clone().into_bytes());
                }
            }

            let Some((next, keys)) = scan_reply(replies.last().expect("the step is in the burst"))
            else {
                holds = false;
                break false;
            };
            // Repeats are `SCAN`'s to make, so the union across steps is what
            // the guarantee is about and a key returned twice is not a
            // finding here.
            if !keys
                .iter()
                .all(|key| stable.contains(key) || sent.contains(key))
            {
                holds = false;
            }
            seen.extend(keys);
            // The shard half of the cursor, read the way the edge packs it —
            // see `WALK_CURSOR_SHARD_SHIFT`. `0` is the end of the cycle and
            // names no shard, so it is the loop's business below and not this
            // check's.
            if next != 0 {
                let shard = next >> WALK_CURSOR_SHARD_SHIFT;
                if shard < last_shard || shard >= u64::from(cfg.shards) {
                    holds = false;
                }
                last_shard = shard;
            }
            cursor = next;

            if cursor == 0 {
                break true;
            }
            if cfg.concurrent_scan_cycle {
                if steps >= WALK_CYCLE_STEP_BOUND {
                    holds = false;
                    break false;
                }
            } else if steps >= WALK_PREFIX_STEPS {
                break false;
            }
        };

        // Only a walk that reached the end of its cycle saw the whole family,
        // so only that walk is held to having returned all of it — and only
        // on a node that cannot have reclaimed a stable key underneath it.
        // Under a ceiling, at-least-once is exactly the claim eviction is
        // allowed to break; what survives is the rest, and every one of those
        // claims is asserted above whatever the shape.
        if completed && !self.evictable && !stable.iter().all(|key| seen.contains(key)) {
            holds = false;
        }

        Ok(WalkOutcome {
            holds,
            present: written
                .into_iter()
                .map(String::into_bytes)
                .filter(|name| !gone.contains(name))
                .collect(),
        })
    }
}
