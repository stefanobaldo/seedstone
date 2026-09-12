//! The vocabulary of the simulated clients: which keys exist, which
//! operations a client may compose, what each expects back, and the wire the
//! client speaks over.

use rand::RngExt;
use rand::rngs::ChaCha8Rng;
use seedstone_resp::{Decoder, DecoderLimits, Frame, encode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

use crate::{CLIENT_CHUNK, PORT, SERVER, contract};

/// One client's slice of a key family.
pub struct KeyRange {
    /// Where this client's keys start in the family.
    pub first: u32,
    /// How many it owns.
    pub len: u32,
}

impl KeyRange {
    /// Splits `total` keys evenly between `clients` and takes `id`'s share.
    ///
    /// A remainder is left unused rather than handed to the last client: an
    /// uneven slice would give one client a differently shaped workload for a
    /// reason nobody reading a failing seed would remember.
    pub fn new(id: u16, total: u32, clients: u16) -> Self {
        let len = (total / u32::from(clients.max(1))).max(1);
        Self {
            first: u32::from(id) * len,
            len,
        }
    }

    /// The family-wide index of the `slot`-th key this client owns.
    pub const fn key(&self, slot: u32) -> u32 {
        self.first + slot
    }

    /// A slot drawn from this client's own slice.
    pub fn pick(&self, rng: &mut ChaCha8Rng) -> u32 {
        rng.random_range(0..self.len)
    }
}

/// What a client believes about one plain key it owns.
#[derive(Clone)]
pub enum Known {
    /// Never written, or written and answered with something the client could
    /// not read. Nothing is asserted about the key until it is written again.
    Nothing,
    /// Deleted by its owner, and nothing has written it since.
    Absent,
    /// Written by its owner with these bytes, and nothing has written it
    /// since.
    Value(Vec<u8>),
}

/// One operation a client is about to issue.
///
/// The form travels with the frame rather than being derived from it later:
/// the arm that composed the command is the one place that knows without
/// question which of the contract's forms it is, and a reader that had to
/// recover it from the bytes would be a second implementation of the
/// contract's spelling.
pub struct Op {
    pub frame: Frame,
    pub check: Check,
    /// The contract's name for what this is. See [`crate::contract`].
    pub form: &'static str,
}

/// What one reply is worth to the client that asked for it.
///
/// Built with the command and consumed with the reply: a reply's index in a
/// burst is its command's, so this is how a client keeps hold of what it was
/// expecting without having to read it back off the wire.
pub enum Check {
    /// Coverage only — the reply carries no claim. `TTL` is here: it is in
    /// the workload so its command kind is traced and its arithmetic runs,
    /// and what it could assert is a weaker form of what the `GET`
    /// invariants already assert.
    Ignored,
    /// An `INCRBY` this client owes the shared expected sum, if it is
    /// acknowledged.
    Counter(i64),
    /// A `SET` of an owned plain key: the model adopts `value` if it took.
    ///
    /// Also what a `KEEPTTL` is checked as. On a family no deadline is ever
    /// put on, the option has nothing to keep and the reply is a plain `SET`'s
    /// — which is the point: what it reaches is the option's parse path and
    /// the branch that finds no deadline to preserve. The branch that
    /// *preserves* one is not reachable from any client here, and the reason
    /// is the volatile model rather than the draw: it holds deadlines and not
    /// values, so it cannot tell a `KEEPTTL` that kept a deadline from one
    /// that met an expired key and created a new one with none. Emitting it
    /// there would mean giving the key up, which is coverage bought by losing
    /// an invariant.
    PlainSet { slot: u32, value: Vec<u8> },
    /// A `SET … NX`, a `SET … XX` or a `SETNX` of an owned plain key.
    ///
    /// The strongest thing the plain model can be asked, because the answer is
    /// the model itself: the condition held, or it did not. The model knows
    /// presence exactly — nothing else writes these keys — so both replies are
    /// predictions rather than observations.
    ///
    /// Three commands in one check rather than two, because the decision is
    /// one decision; what `reply` carries is the only thing that differs.
    PlainSetCond {
        slot: u32,
        value: Vec<u8>,
        /// `true` for `XX`, which sets only where a value already is.
        only_if_present: bool,
        /// How the two answers are spelled on the wire.
        reply: CondReply,
    },
    /// A `SET … GET` of an owned plain key: the reply is what the key held
    /// *before* this command, and the key holds `value` after it.
    ///
    /// A `GET` and a `SET` in one round trip, and it is checked as both: the
    /// reply is held against the model exactly as a `GET`'s would be, and the
    /// model then adopts the value the command wrote.
    PlainSetGet { slot: u32, value: Vec<u8> },
    /// A `DEL` of one to three owned plain keys — the variadic form, which
    /// the service layer fans out one command per key. How many it removes is
    /// the model's to predict: the *distinct* slots it believes hold a value,
    /// since a key named twice is removed once.
    PlainDel { slots: Vec<u32> },
    /// An `EXISTS` over one to three owned plain keys, fanned out the same
    /// way. How many it counts is also the model's to predict, and by the
    /// opposite rule: a key named twice counts twice, because each name is
    /// its own command.
    PlainExists { slots: Vec<u32> },
    /// An `MGET` of one to three owned plain keys: an array, one element per
    /// name, in the order the names were written.
    ///
    /// The one command here whose reply *shape* is a function of how many
    /// replies the fan-out gathered, which is why the model checks the array's
    /// length as strictly as its contents.
    PlainMGet { slots: Vec<u32> },
    /// A `GET` of an owned plain key, held against the model.
    PlainGet { slot: u32 },
    /// A `TYPE` of an owned plain key: `string` where the model holds a
    /// value, `none` where it holds none.
    PlainType { slot: u32 },
    /// A `STRLEN` of an owned plain key: the length of what the model holds,
    /// or zero.
    PlainStrLen { slot: u32 },
    /// A `SET … EX/PX` or `SETEX` of an owned volatile key: the model adopts
    /// `deadline` if it took.
    VolatileSet { slot: u32, deadline: Instant },
    /// An `EXPIRE` or a `PEXPIRE` of an owned volatile key: the model adopts
    /// `deadline` only if the server says there was a key there to take it.
    ///
    /// One check for both commands because the two differ only in the unit
    /// their argument is written in, and the model holds an instant either
    /// way.
    VolatileExpire { slot: u32, deadline: Instant },
    /// A `PERSIST` of an owned volatile key: whatever it answers, the key
    /// carries no deadline afterwards, so the model stops predicting its
    /// death.
    VolatilePersist { slot: u32 },
    /// A `GET` of an owned volatile key — the two expiration invariants.
    VolatileGet { slot: u32 },
}

/// How a conditional write's two answers are spelled on the wire.
///
/// The same decision in two types. `SETNX` is the reason this exists: it is
/// `SET key value NX` under an older name, and a client library that read its
/// reply as a boolean would read a truthy `OK` for a write that was refused if
/// the server answered the `SET` spelling's frames. So the model predicts the
/// decision once and the frame separately, and a server that got the decision
/// right in the wrong type fails here.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CondReply {
    /// `SET … NX` and `SET … XX`: `+OK` where the condition held, a null
    /// where it did not.
    OkOrNull,
    /// `SETNX`: `:1` where it held, `:0` where it did not (6.2.24, 8.10.1).
    OneOrZero,
}

/// How a deadline is spelled on the wire.
#[derive(Clone, Copy)]
pub enum Spelling {
    /// `SET key value <option> <argument>`.
    SetOption(&'static str),
    /// `SETEX key <argument> value` — the span *before* the value, which is
    /// what makes it a spelling and not an option: the parser reaches it by
    /// position, through a different table entry, and a bug in that entry is
    /// one no `SET … EX` can find.
    SetEx,
    /// `PSETEX key <argument> value` — [`SetEx`](Self::SetEx) one unit down,
    /// and a fourth table entry for the same reason.
    PSetEx,
}

/// A deadline a write can ask for: how it is spelled, its argument, and what
/// the two come to in milliseconds.
pub struct Deadline {
    pub spelling: Spelling,
    pub argument: u64,
    pub millis: u64,
    /// Which of the contract's forms this deadline makes the command. The
    /// spellings are separate forms, and this is where they are told apart.
    pub form: &'static str,
}

/// The deadlines the workload hands out.
///
/// Spread deliberately across the run's own timescale. The short ones are
/// dead before their client has finished issuing, which is what makes a stale
/// read reachable while the server is still under load; the long ones outlive
/// the whole workload, which is what makes a spurious death reachable at all.
/// None exceeds a second, because the settle at the end waits out the longest
/// of them and every millisecond of that is paid for in ticks. Both `SET`
/// options appear because `EX` and `PX` are separate arms of the parser and
/// separate arithmetic in the handler; `SETEX` and `PSETEX` appear because
/// they are a third and a fourth arm — different table entries reading the
/// span by position — that resolve to the same handler, and the sweep has to
/// see that they do.
pub const DEADLINES: [Deadline; 8] = [
    Deadline {
        spelling: Spelling::SetOption("PX"),
        argument: 1,
        millis: 1,
        form: contract::FORM_SET_PX,
    },
    Deadline {
        spelling: Spelling::SetOption("PX"),
        argument: 20,
        millis: 20,
        form: contract::FORM_SET_PX,
    },
    Deadline {
        spelling: Spelling::SetOption("PX"),
        argument: 60,
        millis: 60,
        form: contract::FORM_SET_PX,
    },
    Deadline {
        spelling: Spelling::SetOption("PX"),
        argument: 150,
        millis: 150,
        form: contract::FORM_SET_PX,
    },
    Deadline {
        spelling: Spelling::SetOption("PX"),
        argument: 300,
        millis: 300,
        form: contract::FORM_SET_PX,
    },
    // The two that outlive the settle, and the only ways to reach the
    // seconds unit: keys given these are never decided dead, only decided
    // alive, which is the half of the invariant nothing else reaches.
    Deadline {
        spelling: Spelling::SetOption("EX"),
        argument: 1,
        millis: 1000,
        form: contract::FORM_SET_EX,
    },
    Deadline {
        spelling: Spelling::SetEx,
        argument: 1,
        millis: 1000,
        form: contract::FORM_SETEX,
    },
    // `PSETEX` is drawn short rather than long, which is the half `SETEX`
    // cannot reach: its unit is milliseconds, so a key given this one dies
    // inside the run and the draw decides it *dead*. The two positional
    // spellings then cover both halves of the expiration invariant between
    // them instead of both landing on the same one.
    Deadline {
        spelling: Spelling::PSetEx,
        argument: 300,
        millis: 300,
        form: contract::FORM_PSETEX,
    },
];

/// The span `EXPIRE` asks for, in seconds — its argument has no finer unit,
/// so a key it touches is one whose death this run will not see. What it is
/// here for is the command itself: a deadline set by a path other than `SET`,
/// and the only source of keys certain to be *alive* late in the workload.
pub const EXPIRE_SECONDS: u64 = 1;

/// How many walk keys a client writes once its mutations are over.
///
/// Small, and what that gives up is worth stating. What the walk asserts is
/// that a set comes back exactly, and a systematically wrong matcher, an
/// inverted filter or a broken dedup shows up whatever the set's size. What
/// more keys would buy is *placement*: a fan-out that skipped a single shard
/// is caught only if a walk key happened to live there, and eight per client
/// covers a small fraction of the shards a deployed shape has. Covering them
/// densely enough to make that certain would put more keys in the walk family
/// than in every other family combined, which changes the shape the sweep is
/// measuring in order to catch a defect the service layer's own tests already
/// pin directly. Proving the invariant bites is a planted defect's job, not a
/// key count's.
pub const WALK_KEYS: u32 = 8;

/// The `COUNT` the verifier's `SCAN` asks for on the steps that carry one.
///
/// A key target, which is what `COUNT` is: the server gathers up to this many
/// keys per call, across as many shards as its own bucket ceiling allows. The
/// number is small enough that a verifier's cycle takes several calls on the
/// shapes swept here, which is what puts a cursor on the wire at all.
pub const WALK_SCAN_COUNT: usize = 32;

/// How many fresh keys a client writes into its own walk family between two
/// steps of its own walk.
///
/// This is the churn the concurrent invariant is stated under, and it is the
/// client's *own* family rather than a neighbour's on purpose: a key another
/// client writes cannot appear in this walk's answers at all, so it changes
/// the table and nothing else. A key of this family can appear, and whether
/// it does is exactly what the guarantee refuses to promise — a key created
/// during a walk may be returned or missed. Asserting over a family that
/// nothing added to would be asserting over the quiescent case again.
pub const WALK_CHURN_WRITES: u32 = 3;

/// How many churn keys the same burst removes again, oldest first.
///
/// Fewer than it writes, so the family grows: growth is what makes a table
/// double, and a doubling with a walk in flight is the case the whole cursor
/// design exists for. Removing the oldest rather than the newest is what puts
/// the removals behind the cursor, where a walk has already been.
pub const WALK_CHURN_DELETES: u32 = 1;

/// How many steps a client's walk takes when it is not driving its cycle to
/// the end. See [`SimConfig::concurrent_scan_cycle`].
///
/// Two, and it is a price rather than a property: every step is a round trip
/// for every client, and the walk is already the most expensive thing at the
/// tail of a run. Measured against the sweep this gate runs — 275 seeds of the
/// swept shape — two steps cost 8 % of the whole sweep's CPU and four cost
/// 20 %, against a budget with about a fifth of itself spare. What the extra
/// two steps would have bought is more of the *per-step* checks; what carries
/// the walk's result every seed is the `KEYS` that closes it, which is one
/// round trip and complete by construction, and both `SCAN` forms are on the
/// wire either way because the form is chosen per client rather than per step.
pub const WALK_PREFIX_STEPS: u64 = 2;

/// The `COUNT` a client's own walk asks for, on the clients that send one.
///
/// One key, the smallest target a call can carry, so a call ends at the first
/// key it finds rather than at the end of the budget — the shortest step this
/// client can take, and the one that leaves a cursor in flight for the most
/// of a run. It used to be one *bucket*, back when the server read `COUNT` as
/// an occupancy number; it is a key target now, and a call that finds nothing
/// matching still spends the server's whole bucket ceiling looking. Half the
/// clients send no `COUNT` at all — see [`Model::walk`] — so both parse paths
/// are on the wire in every run.
pub const WALK_STEP_COUNT: usize = 1;

/// How far to shift a `SCAN` cursor to read the shard it names.
///
/// The edge packs `(shard, internal)` into the one integer `SCAN` exchanges,
/// with the low 48 bits belonging to the shard's own cursor. Mirrored here
/// rather than imported: the packing is the service's own business and its
/// halves are private to it, so what this number is is a *claim* the harness
/// makes about the wire — the same standing the rest of the walk's checks
/// have. A cursor whose shard half stopped matching this would fail the
/// monotonic check rather than pass it quietly.
pub const WALK_CURSOR_SHARD_SHIFT: u32 = 48;

/// How many steps a cycle-completing walk is allowed before the harness calls
/// it a walk that is not going to finish.
///
/// Not derived from the keyspace, and the churn is why: the walk grows the
/// family as it goes, so a bound stated in keys would grow with the walk and
/// could never be exceeded. What the honest cursor promises under growth is
/// not a step count but *convergence* — a doubling halves the size of every
/// later step instead of doubling the number of steps left — so the number of
/// steps it needs is bounded even while the table is not.
///
/// **Measured over seeds 1 to 6 of both shapes that drive a cycle, by the step
/// count of every walk that completed one:** exactly 1 on
/// [`SimConfig::narrow`], both walk forms and every seed, and 2 to 16 on
/// [`SimConfig::crossing`] across its ninety-six walks. It used to be 151 to
/// 217, back when a client's `COUNT` of one meant one bucket a call; a call
/// now spends a bucket ceiling of the server's own and crosses shards on it,
/// so `narrow`'s whole table falls inside a single call and `crossing`'s
/// sixteen shards cost a call each at worst. The old figure's unsettled low end
/// — 151 against a re-measurement's 167 — is settled by being obsolete: both
/// walk forms cost the same now, so there is no longer a question of which
/// clients were counted.
///
/// **Re-measured when the server's bucket ceiling was raised, and unmoved** —
/// the same 1, and the same 2 to 16, over the same twelve runs. That is not a
/// coincidence and is the useful half of the measurement: on these shapes a
/// call ends at the client's key target, not at the server's ceiling, so the
/// ceiling is not what sets the step count and raising it changes nothing
/// here. A shape whose calls run out of budget before they run out of target
/// would move these figures; neither of these two does.
///
/// **What the bound is for has changed with them, and it is worth saying which
/// of the two it is.** It is not a live detector any more. It was one for a
/// cursor that advanced upwards instead of in reverse binary order — a walk
/// that cannot arrive is caught by a step count and by nothing else — and that
/// defect is no longer observable at this layer at all: the claim is proved at
/// the dict now, by `an_upward_cursor_is_outrun_by_a_table_growing_under_it`.
/// See [`Plant::ScanMissesRehash`].
///
/// So this is a loose ceiling, kept for the one thing a ceiling is still good
/// for: a walk that does not finish wedges the run instead of reporting itself,
/// and a harness that hangs says nothing at all. Sixty-four times the widest
/// cycle measured is deliberate slack rather than a number missing its target —
/// a shape with a smaller bucket ceiling or a deeper table would lengthen every
/// cycle here, and a guard that has to be re-derived before a shape can be
/// added is a guard that will be deleted instead. Five times the widest is the
/// rule when it *is* a detector, and 5 × 16 is far under this, so nothing moves.
pub const WALK_CYCLE_STEP_BOUND: u64 = 1024;

/// Where the counter family's share of a hundred rolls ends and the plain
/// family's begins.
///
/// The two boundaries are named because they are the only numbers in the draw
/// that two functions have to agree on: [`Model::compose`] routes on them and
/// the family helpers match on the same roll rather than drawing again, so a
/// helper whose lowest arm disagreed with the boundary above it would leave a
/// band of rolls nothing composed.
pub const COUNTER_OPS: u32 = 18;

/// Where the plain family's share ends and the volatile family's begins. See
/// [`COUNTER_OPS`].
pub const PLAIN_END: u32 = 54;

/// The span `PEXPIRE` asks for, in milliseconds.
///
/// Short on purpose, and that is the whole reason both commands are in the
/// workload rather than one standing for the other. `EXPIRE`'s argument has no
/// unit finer than a second, so every key it touches outlives the run and only
/// the *alive* half of the expiration invariant ever decides one. This one
/// dies well inside a run — comfortably past [`STALE_SLACK`], so a read taken
/// afterwards is decidedly late — and is therefore the only path other than
/// `SET … PX` that can produce a stale read at all. Redis also gives the two
/// commands different ceilings, measured rather than assumed, so they are not
/// one command in two units.
pub const PEXPIRE_MILLIS: u64 = 150;

/// The name of a counter key — touched only by `INCRBY`, shared by every
/// client.
pub fn counter_key(index: u32) -> String {
    format!("counter-{index}")
}

/// The name of a plain key — `GET`/`SET`/`DEL`, never a deadline, owned by
/// one client.
pub fn plain_key(index: u32) -> String {
    format!("plain-{index}")
}

/// The name of a volatile key — always written with a deadline, owned by one
/// client.
pub fn volatile_key(index: u32) -> String {
    format!("volatile-{index}")
}

/// The name of a walk key — written once, never expired, and named so a glob
/// isolates one client's own.
///
/// The other families are indexed by a number split between clients, which no
/// pattern can separate. A walk asserts over what one client owns, so its keys
/// carry the owner in the name.
pub fn walk_key(client: u16, slot: u32) -> String {
    format!("walk-{client}-{slot}")
}

/// The glob that matches exactly one client's walk keys.
pub fn walk_pattern(client: u16) -> String {
    format!("walk-{client}-*")
}

/// The glob that matches every walk key in the run.
///
/// `walk-*` and not `*`: the point of the assertion is set equality, and the
/// only set the harness knows exactly is this one. A walk over the whole
/// keyspace would be racing the expiry sweep for its own denominator.
pub const WALK_ALL: &str = "walk-*";

/// Builds a RESP2 command frame: an array of bulk strings, as a real client
/// sends.
pub fn command(parts: &[&str]) -> Frame {
    Frame::Array(
        parts
            .iter()
            .map(|part| Frame::Bulk(part.as_bytes().to_vec()))
            .collect(),
    )
}

/// A client's connection: the real codec over a simulated socket.
pub struct Conn {
    pub stream: turmoil::net::TcpStream,
    /// Reply bytes read from the server, resumed across reads.
    ///
    /// The same [`Decoder`] the server's own connection loop runs, for the
    /// reason its documentation gives socket readers: a one-shot `parse` over
    /// a growing buffer re-parses every already-complete element on each read
    /// that stops short of a frame. A client here reads small replies and
    /// would not feel that, but an in-tree counterexample to the codec's own
    /// advice is worth less than the twenty lines it saves.
    pub decoder: Decoder,
    /// Scratch for encoding, reused so a client does not allocate per request.
    pub out: Vec<u8>,
}

impl Conn {
    /// Opens a connection to the simulated server.
    pub async fn connect() -> turmoil::Result<Self> {
        Ok(Self {
            stream: turmoil::net::TcpStream::connect((SERVER, PORT)).await?,
            decoder: Decoder::new(DecoderLimits::default()),
            out: Vec::new(),
        })
    }

    /// Sends a burst of commands in one write and reads exactly that many
    /// replies, in request order.
    ///
    /// Written as one buffer rather than one write per command: a burst
    /// delivered as several messages would let the server drain each alone,
    /// which is the depth-1 shape again under a different name.
    ///
    /// `flush` after `write_all` even though turmoil's socket sends on write:
    /// a transport that buffers would otherwise hold a request the client is
    /// blocked waiting on, and that deadlock would only appear under whatever
    /// transport we ported to next.
    pub async fn request_many(&mut self, frames: &[Frame]) -> turmoil::Result<Vec<Frame>> {
        self.out.clear();
        for frame in frames {
            encode(frame, &mut self.out);
        }
        self.stream.write_all(&self.out).await?;
        self.stream.flush().await?;

        let mut replies = Vec::with_capacity(frames.len());
        let mut chunk = [0u8; CLIENT_CHUNK];
        while replies.len() < frames.len() {
            if let Some(reply) = self.decoder.try_next()? {
                replies.push(reply);
                continue;
            }
            let got = self.stream.read(&mut chunk).await?;
            if got == 0 {
                return Err("the server closed the connection mid-request".into());
            }
            self.decoder.feed(&chunk[..got]);
        }
        Ok(replies)
    }
}
