//! Where a command is answered. A command about the connection itself or
//! about the node behind it — `PING`, `ECHO`, `QUIT`, `HELLO`, `INFO`,
//! `COMMAND`, `CLIENT` — has no key, so there is no shard it could belong
//! to; it is answered here and no shard hears of it. Only keyed commands
//! become messages. [`Action`] is that decision made explicit. What those
//! answers need to know about the process they run in arrives as
//! [`NodeInfo`], because this layer has no clock, no port and no way to
//! count its peers of its own.

use crate::auth::{AUTH_NOT_CONFIGURED, NOAUTH, NOAUTH_HELLO, WRONGPASS};
use crate::containers::{client, command, config, latency, slowlog};
use crate::expiry::{
    MAX_EXPIRE_MILLIS, MAX_EXPIRE_SECONDS, absolute_deadline_span, expire_millis, expire_seconds,
    set_expire_value,
};
use crate::fan_out::{broadcast, fan_out, keys, scan};
use crate::hello::hello;
use crate::info::info;
use crate::node::{KIND_NAMES, NodeInfo, edge_slot, micros_since};
use crate::options::{parse_u64, per_key, scan_options, set_options, wrong_arity};
use crate::reply::{CommandLabel, known_name, quote, safe_error};
use crate::{INVALID_CURSOR, KEYS_REPLY_BYTES};
use seedstone_core::shard::{Command, ReplyError, Router, parse_i64};
use seedstone_resp::Frame;
use std::mem::take;
use std::sync::atomic::Ordering;
use tokio::time::Instant;

/// What to do with one request frame.
///
/// The distinction the type draws is where a command is answered.
/// [`Command`]s belong to a shard and travel; the rest — the connection's own
/// business, and everything the peer got wrong — is answered right here,
/// without a message ever leaving the connection task.
pub enum Action {
    /// A keyed command: route it and reply with what the shard says.
    Dispatch(Command),
    /// A request that cannot travel inside a chunk — see [`Unbatched`].
    Unbatched(Unbatched),
    /// Answer with this frame; the connection continues.
    Reply(Frame),
    /// The outcome of an authentication attempt: `Ok` marks the connection
    /// authenticated and replies with the frame, `Err` replies and leaves it
    /// as it was.
    ///
    /// A variant of its own rather than a `Reply` plus a flag, because it is
    /// the one action whose effect is on the connection rather than on the
    /// keyspace — and because [`gated`] must let it through, which it can only
    /// do by name.
    Authenticate(Result<Frame, Frame>),
    /// A `HELLO` that carried no `AUTH`: answered exactly as [`Reply`] is on a
    /// node with no password, and refused by [`gated`] on one that has a
    /// password and has not been given it. A variant of its own so that gate
    /// can name it, rather than a wildcard arm that would silently adopt the
    /// next action added beside it.
    ///
    /// The two spellings are not the same request, which is why only one of
    /// them is this variant. `HELLO … AUTH <user> <pass>` — how redis-py ≥ 4
    /// and go-redis authenticate — becomes [`Authenticate`] and passes the
    /// gate whatever the connection's state, because a client must be able to
    /// authenticate in the handshake. The credential-less spelling asks a node
    /// that has told it nothing to describe itself, and gets
    /// [`NOAUTH_HELLO`]: the server name, this node's version, the protocol
    /// version, the deployment mode and the role stay unread until the peer
    /// has said the password. Metadata about the process rather than about the
    /// keyspace, but a node whose whole posture is silence should not be
    /// selective about it — and Redis 6.0+ refuses the same request in the
    /// same place, so any client that reaches a password-protected Redis
    /// reaches this one.
    ///
    /// [`Reply`]: Action::Reply
    /// [`Authenticate`]: Action::Authenticate
    Hello(Frame),
    /// A request's own mistake, decided before the connection's state is.
    ///
    /// Redis answers what is wrong with the request — a protocol version it
    /// does not speak, an option it does not know — before it asks whether
    /// the connection may be answered at all, so an unauthenticated
    /// `HELLO 99` is told `NOPROTO` there. A handler's `Err` is an ordinary
    /// [`Reply`] by the time [`gated`] decides, and the gate answers every
    /// `Reply` with `NOAUTH`; this variant is how a handler says its refusal
    /// is about the request and not about the keyspace, and the gate lets it
    /// through by name. Only `hello` produces it: the three refusals it
    /// carries name nothing about the node, and no other handler has earned
    /// the right to speak before the gate. Measured on 6.2.24 and 8.10.1.
    ///
    /// [`Reply`]: Action::Reply
    Refuse(Frame),
    /// Answer with this frame, then hang up.
    ReplyThenClose(Frame),
}

/// What an action becomes on a connection that has not authenticated yet.
///
/// Two pass: the attempt itself — including the handshake that carries one —
/// and the goodbye. Everything else is refused *here*, before the router is
/// touched, so an unauthenticated peer moves no key and learns nothing about
/// the keyspace, not even from how long a refusal took. A credential-less
/// handshake is refused with its own text rather than the general one, because
/// what it needs told is the form that would have worked; see
/// [`Action::Hello`].
///
/// Exhaustive by construction: a new [`Action`] does not compile until this
/// function says which side of the gate it is on.
pub fn gated(action: Action, authenticated: bool) -> Action {
    if authenticated {
        return action;
    }
    match action {
        Action::Authenticate(outcome) => Action::Authenticate(outcome),
        // The handshake that carries no credentials is refused here and not
        // in `hello`, because this is the only place that knows whether the
        // connection has authenticated. A node with no password never reaches
        // this arm: it starts every connection authenticated, so the early
        // return above answers the handshake.
        Action::Hello(_) => Action::Reply(safe_error(NOAUTH_HELLO)),
        Action::ReplyThenClose(frame) => Action::ReplyThenClose(frame),
        Action::Refuse(frame) => Action::Refuse(frame),
        Action::Dispatch(_) | Action::Unbatched(_) | Action::Reply(_) => {
            Action::Reply(safe_error(NOAUTH))
        }
    }
}

/// A request that is answered on its own, after the chunk in front of it has
/// been dispatched and before anything behind it is.
///
/// What they have in common is that each needs every command the peer
/// pipelined ahead of it to have already run. Holding them in one variant is
/// what keeps that ordering requirement in one place instead of restated at
/// each call site, where the next one would be the one that forgot.
pub enum Unbatched {
    /// One request's keys, split into the one-key commands the shards that
    /// own them can run, carrying how their replies become one again — see
    /// [`fan_out`], which also states what the split costs in atomicity.
    ///
    /// Usually several keys, but not only: a fold that changes the shape of a
    /// lone reply sends a one-key request through here too — see
    /// [`Fold::is_identity_on_one`].
    FanOut {
        /// The commands, in the order the peer named their keys.
        cmds: Vec<Command>,
        /// What the peer called the request, lower-cased: `mget`, `del`,
        /// `exists`. Carried rather than inferred from the fold, so that
        /// [`Unbatched::edge_name`] states which request this is instead of
        /// guessing it from how its replies are put back together.
        name: &'static str,
        /// What the replies are folded into.
        fold: Fold,
    },
    /// A request naming no key at all: it reaches every shard and the answers
    /// are folded into one reply — see [`broadcast`].
    Every {
        /// The command every shard runs.
        cmd: Command,
        /// What the shards' replies are folded into.
        gather: Gather,
    },
    /// A request naming a pattern rather than a key. The edge walks every
    /// shard itself, a bounded step at a time, and gathers what matches — see
    /// [`keys`]. It is not an [`Every`](Unbatched::Every) because one command
    /// per shard is not one *step* per shard: a walk is a loop.
    Keys(Vec<u8>),
    /// One step of a client-driven walk: the cursor says which shard and
    /// where in it, and the answer says where to resume — see [`scan`]. It is
    /// the only one of these that reaches a single shard, and it is here
    /// rather than beside the keyed commands because the shard it reaches is
    /// unpacked at the edge instead of hashed from a key.
    Scan {
        /// The packed cursor the client sent, untrusted.
        cursor: u64,
        /// `MATCH`, filtered on the shard rather than here.
        pattern: Option<Vec<u8>>,
        /// `COUNT`: how many keys the client wants back, bounded when the
        /// call runs by [`WALK_STEP_BUCKETS`] rather than when it is parsed.
        count: usize,
    },
    /// The sections a peer asked `INFO` for, rendered once the chunk in front
    /// of it has run.
    ///
    /// It reaches no shard, and it is here for the ordering alone. `INFO`
    /// reports the keyspace — `used_memory` is the node's accounting of it —
    /// so a document rendered while the writes the peer pipelined ahead of it
    /// were still in flight would describe a keyspace from before them. That
    /// is the same hazard [`Keys`](Unbatched::Keys) is here for, and the
    /// answer is the same one.
    Info(Vec<Vec<u8>>),
}

/// What a fan-out's replies are folded into.
///
/// Carried by the request rather than read off the replies, because the two
/// folds are not distinguishable from a reply: a one-key `MGET` and a `GET`
/// both come back `Reply::Bulk`, and only the request knows that one of the
/// two still owes the peer an array around it. Inferring it here would answer
/// that `MGET` with a bare bulk and desynchronise every client that counts
/// elements.
///
/// Both places that read this match the whole enum, so a third fold added
/// later is a compile error at each of them rather than a reply quietly folded
/// the wrong way — miscounted on one side, mis-shaped on the other.
#[derive(Clone, Copy)]
pub enum Fold {
    /// One integer: every reply is a count and the answer is their sum —
    /// `DEL`, `EXISTS`.
    Sum,
    /// One array: every reply is an entry, one per command — `MGET`.
    Array,
}

impl Fold {
    /// Whether folding a lone reply gives back exactly that reply's frame.
    ///
    /// [`Sum`](Fold::Sum) over one count is that count, so a one-key `DEL`
    /// needs no fan-out at all and can travel in the drain's batch like any
    /// other keyed command. [`Array`](Fold::Array) cannot: an array of one is
    /// a different frame from the bulk inside it, and the difference is the
    /// whole reply as far as a client parsing it is concerned.
    pub const fn is_identity_on_one(self) -> bool {
        match self {
            Self::Sum => true,
            Self::Array => false,
        }
    }
}

/// How [`broadcast`] folds one reply per shard into the frame the peer sees.
///
/// Chosen at the command table, where the command is named, rather than
/// matched off the command inside `broadcast`. What that buys is that the
/// fold can no longer be *omitted*: a keyspace-wide command added to the
/// table has to say which of these it answers, where before it inherited
/// `+OK` from the `else` branch every command but `DbSize` fell into, without
/// anyone having chosen it. Which of the two is right remains the table's to get
/// right — both compile.
///
/// The one place that reads this matches the whole enum, so a third gather
/// added later is a compile error in [`broadcast`] rather than a reply
/// quietly folded as one of these two. The same reason [`Fold`] exists for
/// the fan-out.
#[derive(Clone, Copy)]
pub enum Gather {
    /// Every shard answers a count and the peer gets their sum — `DBSIZE`,
    /// which is how the reply is the size of the keyspace and not of a shard.
    Sum,
    /// Every shard answers `+OK` and so does the peer, once — `FLUSHDB`.
    AllOk,
}

impl Unbatched {
    /// What the peer called this request, lower-cased.
    ///
    /// The name `commandstats` reports it under, which is not always the name
    /// of the commands it becomes: an `MGET` is a pile of `GET`s and a `KEYS`
    /// is a pile of scan steps. Whether the name is one *this layer* counts is
    /// [`edge_slot`]'s question and not this one's — a multi-key `DEL` is a
    /// fan-out too, and the shards count every command it splits into.
    fn edge_name(&self) -> &'static str {
        match self {
            Self::FanOut { name, .. } => name,
            // Both of these are one command sent to every shard, and the
            // command's own name is the request's: the table that names a
            // kind for `commandstats` is the same table, so there is no
            // second spelling to keep in step.
            Self::Every { cmd, .. } => KIND_NAMES[usize::from(cmd.kind())],
            Self::Keys(_) => "keys",
            Self::Scan { .. } => "scan",
            Self::Info(_) => "info",
        }
    }

    /// Runs the request and answers with the single frame it earns.
    ///
    /// Times itself where the edge counts it, and only there — see
    /// [`NodeInfo::edge_usec`] for what that figure is and is not. A request
    /// the shards count instead takes no reading at all: a fan-out `DEL` is
    /// timed four times by the four shards that ran its four commands, and a
    /// fifth reading here would be the same microseconds under a name that
    /// already has its own.
    pub async fn answer<R: Router>(self, router: &R, node: &NodeInfo) -> Frame {
        let slot = edge_slot(self.edge_name().as_bytes());
        let started = slot.map(|_| Instant::now());
        let frame = match self {
            Self::FanOut { cmds, fold, .. } => fan_out(router, cmds, fold).await,
            Self::Every { cmd, gather } => broadcast(router, cmd, gather).await,
            Self::Keys(pattern) => keys(router, pattern, KEYS_REPLY_BYTES).await,
            Self::Scan {
                cursor,
                pattern,
                count,
            } => scan(router, cursor, pattern, count).await,
            Self::Info(wanted) => Frame::Bulk(info(router, node, &wanted).await.into_bytes()),
        };
        if let (Some(slot), Some(started)) = (slot, started) {
            node.edge_usec[slot].fetch_add(micros_since(started), Ordering::Relaxed);
        }
        frame
    }
}

/// Applies an authentication outcome to the connection and returns the frame
/// it is answered with.
///
/// The only place `authenticated` is ever set, and the reason the flag is not
/// touched anywhere else in the drain: a success is the frame *and* the state
/// change, and separating them is how one of the two gets forgotten.
pub fn settle_auth(outcome: Result<Frame, Frame>, authenticated: &mut bool) -> Frame {
    match outcome {
        Ok(frame) => {
            *authenticated = true;
            frame
        }
        Err(frame) => frame,
    }
}

/// Maps a request frame to what should happen because of it, and names the
/// command it came from.
///
/// Every failure below is answered and survived: a frame that is well-formed
/// RESP but not a command this server can run is the peer's mistake, not a
/// reason to desynchronise the stream.
///
/// The label comes back alongside the action because the frame is consumed
/// here and there is no second chance at it: the drain that eventually sees
/// the error reply has neither the frame nor the batch it went into. One
/// parse, one label — a second parse would put peer-input handling in two
/// places, which is what this module's opening paragraphs forbid.
pub fn frame_to_action(frame: Frame, node: &NodeInfo) -> (Action, CommandLabel) {
    // Anonymous until a name is read, which is what a frame that is not even
    // an array of bulk strings leaves it as.
    let mut label = CommandLabel::Anonymous;
    let action = match action_for(frame, node, &mut label) {
        Ok(action) => action,
        Err(message) => Action::Reply(safe_error(&message)),
    };
    (action, label)
}

/// What a command does with the arguments that follow its name.
///
/// One signature for every entry of [`COMMANDS`], whether or not a particular
/// command has any use for the node it is handed. The uniformity is the point:
/// it is what lets the surface be a table rather than a match, and the table is
/// what keeps `COMMAND COUNT` from drifting away from it.
pub type Handler = fn(&mut [Vec<u8>], &NodeInfo) -> Result<Action, String>;

/// Every command name this server answers.
///
/// Exported so the simulator's contract can be confronted with the surface
/// rather than with a copy of it. A hand-maintained second list is exactly the
/// thing that goes stale, and going stale here means the sweep's PASS quietly
/// stops covering something.
pub fn command_names() -> impl Iterator<Item = &'static [u8]> {
    COMMANDS.iter().map(|(name, _)| *name)
}

/// Every command name this server accepts, and what each one does about its
/// arguments.
///
/// The single source of truth for the command surface. A name is dispatched by
/// being found here and `COMMAND COUNT` answers with how many entries there
/// are, so the number a client is told is exactly the number of commands the
/// server will run. A literal kept alongside the table could disagree with it;
/// a length cannot.
///
/// Ordered by the traffic a command carries rather than alphabetically. The
/// lookup is a scan and it is on the path every request takes, so the keyed
/// commands — which are effectively all of the traffic — come first, and the
/// ones a connection sends once or never come last.
///
/// `args` is emptied as it is matched: see [`action_for`].
pub const COMMANDS: &[(&[u8], Handler)] = &[
    (b"GET", |args, _| match args {
        [key] => Ok(Action::Dispatch(Command::Get { key: take(key) })),
        _ => Err(wrong_arity("get")),
    }),
    (b"SET", |args, node| match args {
        [key, value, options @ ..] => {
            // Parsed before the key and the value are taken, so a refused
            // option leaves nothing half-consumed.
            let options = set_options(options, node)?;
            Ok(Action::Dispatch(Command::Set {
                key: take(key),
                value: take(value),
                expiry: options.expiry,
                cond: options.cond,
                keep_ttl: options.keep_ttl,
                get: options.get,
            }))
        }
        _ => Err(wrong_arity("set")),
    }),
    (b"MGET", |args, _| {
        // One `Get` per argument, and no new command: what `MGET` adds to a
        // pile of `GET`s is the array around them, which is the fold's job
        // rather than a shard's.
        per_key(args, "mget", Fold::Array, |key| Command::Get { key })
    }),
    (b"DEL", |args, _| {
        per_key(args, "del", Fold::Sum, |key| Command::Del { key })
    }),
    (b"EXISTS", |args, _| {
        per_key(args, "exists", Fold::Sum, |key| Command::Exists { key })
    }),
    (b"EXPIRE", |args, node| match args {
        [key, seconds] => {
            let seconds = expire_seconds(seconds, node)?;
            Ok(Action::Dispatch(Command::Expire {
                key: take(key),
                seconds,
            }))
        }
        _ => Err(wrong_arity("expire")),
    }),
    (b"TTL", |args, _| match args {
        [key] => Ok(Action::Dispatch(Command::Ttl { key: take(key) })),
        _ => Err(wrong_arity("ttl")),
    }),
    (b"PEXPIRE", |args, node| match args {
        [key, millis] => {
            let millis = expire_millis(millis, node)?;
            Ok(Action::Dispatch(Command::PExpire {
                key: take(key),
                millis,
            }))
        }
        _ => Err(wrong_arity("pexpire")),
    }),
    (b"PERSIST", |args, _| match args {
        [key] => Ok(Action::Dispatch(Command::Persist { key: take(key) })),
        _ => Err(wrong_arity("persist")),
    }),
    (b"PTTL", |args, _| match args {
        [key] => Ok(Action::Dispatch(Command::PTtl { key: take(key) })),
        _ => Err(wrong_arity("pttl")),
    }),
    (b"EXPIREAT", |args, node| match args {
        [key, at] => {
            let millis = absolute_deadline_span(at, 1000, "expireat", node)?;
            Ok(Action::Dispatch(Command::ExpireAt {
                key: take(key),
                millis,
            }))
        }
        _ => Err(wrong_arity("expireat")),
    }),
    (b"PEXPIREAT", |args, node| match args {
        [key, at] => {
            let millis = absolute_deadline_span(at, 1, "pexpireat", node)?;
            Ok(Action::Dispatch(Command::PExpireAt {
                key: take(key),
                millis,
            }))
        }
        _ => Err(wrong_arity("pexpireat")),
    }),
    (b"INCRBY", |args, _| match args {
        [key, delta] => {
            let delta =
                parse_i64(delta).ok_or_else(|| ReplyError::NotAnInteger.wire_text().to_owned())?;
            Ok(Action::Dispatch(Command::IncrBy {
                key: take(key),
                delta,
            }))
        }
        _ => Err(wrong_arity("incrby")),
    }),
    (b"TYPE", |args, _| match args {
        [key] => Ok(Action::Dispatch(Command::Type { key: take(key) })),
        _ => Err(wrong_arity("type")),
    }),
    (b"STRLEN", |args, _| match args {
        [key] => Ok(Action::Dispatch(Command::StrLen { key: take(key) })),
        _ => Err(wrong_arity("strlen")),
    }),
    // The last of the keyed commands, and the one with the least traffic
    // behind it: the surface's one production caller reaches it on a single
    // cache-miss path. It sits after the keyed commands that carry the load
    // so none of them pays a comparison for it.
    (b"SETEX", |args, node| match args {
        [key, seconds, value] => {
            // Parsed before the key and the value are taken, so a refused
            // span leaves nothing half-consumed — and writes nothing: the
            // shard never hears of a `SETEX` whose span was refused, which is
            // Redis's behaviour too (6.2.24, 8.10.1: a refused `SETEX` over
            // a key leaves its value and its `TTL` untouched).
            //
            // The ceiling is `SET … EX`'s, so the two spellings of one write
            // agree with each other. Whether that ceiling is Redis's is a
            // separate question, answered at `MAX_EXPIRE_SECONDS`.
            let seconds =
                set_expire_value(seconds, MAX_EXPIRE_SECONDS, "setex", node, Some(1_000))?;
            Ok(Action::Dispatch(Command::SetEx {
                key: take(key),
                seconds,
                value: take(value),
            }))
        }
        _ => Err(wrong_arity("setex")),
    }),
    // Beside `SETEX` for the same reason it sits here: a name a client
    // library still puts on the wire, behind no traffic worth a comparison
    // ahead of the keyed commands that carry the load.
    (b"SETNX", |args, _| match args {
        [key, value] => Ok(Action::Dispatch(Command::SetNx {
            key: take(key),
            value: take(value),
        })),
        _ => Err(wrong_arity("setnx")),
    }),
    (b"PSETEX", |args, node| match args {
        [key, millis, value] => {
            // Parsed before the key and the value are taken, so a refused
            // span leaves nothing half-consumed — and writes nothing: the
            // shard never hears of a `PSETEX` whose span was refused, which
            // is Redis's behaviour too (6.2.24: a refused `PSETEX` over a key
            // leaves its value and its `TTL` untouched).
            //
            // The constant ceiling is `PEXPIRE`'s rather than `SET … PX`'s,
            // and the two spellings of this write still agree about what they
            // refuse, because the number that decides a long span is neither
            // of those constants: it is the clock's boundary, which every
            // span command shares — see [`refuse_past_the_clock`]. Measured
            // on 6.2.24 and 8.10.1, `PSETEX k 9223372036854775807 v` is
            // refused, and so is `SET k v PX 9223372036854775807`.
            let millis = set_expire_value(millis, MAX_EXPIRE_MILLIS, "psetex", node, Some(1))?;
            Ok(Action::Dispatch(Command::PSetEx {
                key: take(key),
                millis,
                value: take(value),
            }))
        }
        _ => Err(wrong_arity("psetex")),
    }),
    // Keyspace-wide: no key to route on, but every shard has to hear it.
    (b"DBSIZE", |args, _| match args {
        [] => Ok(Action::Unbatched(Unbatched::Every {
            cmd: Command::DbSize,
            gather: Gather::Sum,
        })),
        _ => Err(wrong_arity("dbsize")),
    }),
    (b"KEYS", |args, _| match args {
        [pattern] => Ok(Action::Unbatched(Unbatched::Keys(take(pattern)))),
        _ => Err(wrong_arity("keys")),
    }),
    (b"SCAN", |args, _| match args {
        [cursor, options @ ..] => {
            let cursor = parse_u64(cursor).ok_or_else(|| INVALID_CURSOR.to_owned())?;
            let (pattern, count) = scan_options(options)?;
            Ok(Action::Unbatched(Unbatched::Scan {
                cursor,
                pattern,
                count,
            }))
        }
        _ => Err(wrong_arity("scan")),
    }),
    (b"FLUSHDB", |args, _| match args {
        // Redis takes ASYNC and SYNC here. This server has one behaviour and
        // saying so plainly beats accepting a word it would then ignore.
        [] => Ok(Action::Unbatched(Unbatched::Every {
            cmd: Command::FlushDb,
            gather: Gather::AllOk,
        })),
        _ => Err(wrong_arity("flushdb")),
    }),
    // From here down: the connection's own business, answered without a shard
    // ever hearing of it, because there is no key to route on.
    (b"PING", |args, _| match args {
        [] => Ok(Action::Reply(Frame::Simple("PONG".into()))),
        [message] => Ok(Action::Reply(Frame::Bulk(take(message)))),
        _ => Err(wrong_arity("ping")),
    }),
    (b"ECHO", |args, _| match args {
        [message] => Ok(Action::Reply(Frame::Bulk(take(message)))),
        _ => Err(wrong_arity("echo")),
    }),
    (b"AUTH", |args, node| {
        let (user, pass) = match &*args {
            [pass] => (None, pass),
            [user, pass] => (Some(user), pass),
            _ => return Err(wrong_arity("auth")),
        };
        let Some(secret) = &node.password else {
            return Err(AUTH_NOT_CONFIGURED.to_owned());
        };
        let user_ok = user.is_none_or(|u| u.eq_ignore_ascii_case(b"default"));
        // Both checked whatever the username said, so a wrong user and a
        // wrong password cost the same time.
        let pass_ok = secret.matches(pass);
        if user_ok && pass_ok {
            Ok(Action::Authenticate(Ok(Frame::Simple("OK".into()))))
        } else {
            Ok(Action::Authenticate(Err(Frame::Error(
                WRONGPASS.to_owned(),
            ))))
        }
    }),
    (b"HELLO", |args, node| hello(args, node)),
    (b"INFO", |args, _| {
        // Unbatched rather than answered here: see [`Unbatched::Info`] — the
        // document describes the keyspace, so it is rendered after the writes
        // the peer pipelined in front of it, not before them.
        Ok(Action::Unbatched(Unbatched::Info(
            args.iter_mut().map(take).collect(),
        )))
    }),
    (b"COMMAND", |args, _| match args {
        // Redis answers with a description of every command it has. This one
        // has nothing to describe, and an empty array is a client with no
        // hints rather than a client that failed to connect.
        [] => Ok(Action::Reply(Frame::Array(Vec::new()))),
        [sub, rest @ ..] => command(sub, rest),
    }),
    (b"CLIENT", |args, _| match args {
        [] => Err(wrong_arity("client")),
        [sub, rest @ ..] => client(sub, rest),
    }),
    (b"CONFIG", |args, node| match args {
        [] => Err(wrong_arity("config")),
        [sub, globs @ ..] => config(sub, globs, node),
    }),
    (b"SLOWLOG", |args, _| match args {
        [] => Err(wrong_arity("slowlog")),
        [sub, rest @ ..] => slowlog(sub, rest),
    }),
    (b"LATENCY", |args, _| match args {
        [] => Err(wrong_arity("latency")),
        [sub, rest @ ..] => latency(sub, rest),
    }),
    (b"QUIT", |args, _| match args {
        [] => Ok(Action::ReplyThenClose(Frame::Simple("OK".into()))),
        _ => Err(wrong_arity("quit")),
    }),
];

/// [`frame_to_action`]'s body, with the error path expressed as `Err`.
///
/// Command names are matched case-insensitively, as Redis does.
///
/// The arguments are taken apart rather than read: a decoded frame already owns
/// its bulk payloads, so every one of them that ends up in a [`Command`] or a
/// reply is *moved* out of the array the codec built. What is left behind is an
/// empty `Vec` that dies with the array, and the alternative is a second copy
/// of every value the peer wrote, on the path every write takes.
pub fn action_for(
    frame: Frame,
    node: &NodeInfo,
    label: &mut CommandLabel,
) -> Result<Action, String> {
    let Frame::Array(parts) = frame else {
        return Err("ERR Protocol error: expected an array of bulk strings".into());
    };

    let mut args: Vec<Vec<u8>> = Vec::with_capacity(parts.len());
    for part in parts {
        match part {
            Frame::Bulk(bytes) => args.push(bytes),
            _ => return Err("ERR Protocol error: expected an array of bulk strings".into()),
        }
    }

    // Split rather than indexed, so the name and the arguments are two disjoint
    // borrows: the name is read to the end — an unknown command is quoted back
    // by it — while the arguments are being emptied.
    let Some((name, args)) = args.split_first_mut() else {
        return Err("ERR Protocol error: empty command".into());
    };

    // ASCII-uppercase only, which is what the command names are.
    let upper: Vec<u8> = name.to_ascii_uppercase();

    let Some((_, handler)) = COMMANDS
        .iter()
        .find(|(known, _)| *known == upper.as_slice())
    else {
        // The one place a label allocates, and it is already an error path:
        // the reply below quotes the same bytes, so the request was going to
        // pay for them whatever happened.
        *label = CommandLabel::Raw(String::from_utf8_lossy(name).into());
        // The name is peer-supplied. It is quoted, not echoed.
        return Err(format!("ERR unknown command '{}'", quote(name)));
    };
    // Named *before* the handler runs, so a refusal the handler returns — a
    // wrong arity, an unparsable expiry — is attributed rather than anonymous.
    // `known_name` covers every entry of the table just matched, so this
    // borrows a static name and allocates nothing.
    if let Some(known) = known_name(&upper) {
        *label = CommandLabel::Known(known);
    }
    // A command that travels is counted by the shard that runs it, so this
    // search decides nothing for `GET` and `SET` beyond a miss — and a miss is
    // what keeps the clock reading below off their path entirely. A refusal
    // the handler returns as an `Err` is not counted at all: it never became
    // an action. One it returns as an [`Action::Refuse`] is counted, because
    // it did become one — `INFO commandstats` printed
    // `cmdstat_hello:calls=1,usec=1` for a single refused `HELLO 99`
    // (`a_refused_hello_is_counted_in_commandstats` is that reading, taken
    // 2026-09-10), where the same refusal spelt as an `Err` printed no
    // `cmdstat_hello` line at all.
    //
    // A refusal the *gate* returns is counted, though, because the gate runs
    // after this: on a node with a password, a `PING` answered `NOAUTH` lands
    // in the count below. See [`commandstats_section`], which states what that
    // makes the figure mean.
    let edge = edge_slot(&upper);
    let started = edge.map(|_| Instant::now());
    let action = handler(args, node)?;
    if let (Some(index), Some(started)) = (edge, started)
        && !matches!(action, Action::Dispatch(_))
    {
        node.edge_calls[index].fetch_add(1, Ordering::Relaxed);
        // What building the action cost. For a command answered from here
        // that is the whole of it; for one that still has to reach shards,
        // [`Unbatched::answer`] adds what running it costs to the same slot.
        node.edge_usec[index].fetch_add(micros_since(started), Ordering::Relaxed);
    }
    Ok(action)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The other half of that truthfulness: no name appears in the table twice.
    ///
    /// Dispatch takes the *first* match while `COMMAND COUNT` reports *every*
    /// entry, so a duplicated name inflates the count above the number of
    /// commands a client can actually reach — and the test above cannot see
    /// it, because both copies of a duplicate dispatch perfectly well. Only a
    /// count of distinct names catches it.
    #[test]
    fn no_name_appears_in_the_command_table_twice() {
        let mut names: Vec<&[u8]> = COMMANDS.iter().map(|(name, _)| *name).collect();
        let total = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            names.len(),
            total,
            "COMMAND COUNT answers {total}, but the table holds only {} distinct names",
            names.len()
        );
    }
}
