//! `HELLO`: protocol negotiation, with the authentication it may carry. Its
//! refusals are decided in Redis's order and reach the peer past the
//! authentication gate — see [`crate::gated`].

use crate::auth::{AUTH_NOT_CONFIGURED, WRONGPASS};
use crate::dispatch::Action;
use crate::node::{NodeInfo, SERVER_MODE, SERVER_NAME};
use crate::reply::{quote, safe_error};
use seedstone_core::shard::parse_i64;
use seedstone_resp::Frame;

/// What a peer asking for a protocol this server does not speak is told.
///
/// Byte-exact to Redis, and load-bearing rather than cosmetic: go-redis v9
/// opens every connection with `HELLO 3` and downgrades to RESP2 on exactly
/// this prefix. A clearer message would break the client.
pub const NOPROTO: &str = "NOPROTO unsupported protocol version";

/// Answers `HELLO`, which is how a client asks what it is talking to.
///
/// This server speaks RESP2 and only RESP2, so the version argument has
/// exactly one accepted value. The refusal for every other one is
/// [`NOPROTO`] — see that constant for why the exact text is a contract.
///
/// **The order the refusals are decided in is Redis's**, measured against
/// `redis:6-alpine` (6.2.24) rather than reasoned about: the protocol version
/// is parsed, then range-checked, then the options are read — the three this
/// handler decides about the request itself — and only then does anything
/// about the connection matter: `AUTH` if it was given, the configuration
/// mistake below if it was given to a node with no password, and otherwise
/// the gate's [`NOAUTH_HELLO`]. It is observable at every step.
/// `HELLO abc BOGUS x` names two mistakes and Redis reports the version, so
/// reading the options first would answer a different one.
///
/// The refusals decided here travel as [`Action::Refuse`], which the gate lets
/// through in every connection state — so an unauthenticated `HELLO 99` is
/// told `NOPROTO`, as Redis tells it (6.2.24, 8.10.1), and the credential-less
/// `HELLO 2` that parsed is told [`NOAUTH_HELLO`] by the gate as before.
pub fn hello(args: &[Vec<u8>], node: &NodeInfo) -> Result<Action, String> {
    // No version at all is `HELLO` bare, which names no version to disagree
    // with and carries no options to read.
    let credentials = match args {
        [] => None,
        [version, rest @ ..] => {
            let Some(version) = parse_i64(version) else {
                return Ok(Action::Refuse(safe_error(
                    "ERR Protocol version is not an integer or out of range",
                )));
            };
            if version != 2 {
                return Ok(Action::Refuse(safe_error(NOPROTO)));
            }
            match hello_auth(rest) {
                Ok(credentials) => credentials,
                Err(text) => return Ok(Action::Refuse(safe_error(&text))),
            }
        }
    };
    let Some(Credentials { user, pass }) = credentials else {
        return Ok(Action::Hello(hello_frame(node)));
    };
    // A handshake that names a password against a node that has none is the
    // same configuration mistake as an `AUTH` against one, and is told so in
    // the same words.
    let Some(secret) = &node.password else {
        return Err(AUTH_NOT_CONFIGURED.to_owned());
    };
    let user_ok = user.eq_ignore_ascii_case(b"default");
    let pass_ok = secret.matches(pass);
    if user_ok && pass_ok {
        Ok(Action::Authenticate(Ok(hello_frame(node))))
    } else {
        Ok(Action::Authenticate(Err(Frame::Error(
            WRONGPASS.to_owned(),
        ))))
    }
}

/// The options after `HELLO`'s protocol version.
///
/// Redis's grammar is `HELLO [protover [AUTH username password] [SETNAME
/// clientname]]`. `AUTH` is answered; `SETNAME` is refused as it has been,
/// because this server has no client name to set and ignoring the option
/// would let a client believe it took effect — the same reason the refusal
/// was there before `AUTH` was accepted beside it.
pub fn hello_auth(options: &[Vec<u8>]) -> Result<Option<Credentials<'_>>, String> {
    match options {
        [] => Ok(None),
        [keyword, user, pass] if keyword.eq_ignore_ascii_case(b"auth") => {
            Ok(Some(Credentials { user, pass }))
        }
        [option, ..] => Err(format!(
            "ERR Syntax error in HELLO option '{}'",
            quote(option)
        )),
    }
}

/// The pair `AUTH` names, wherever it is spelled — as a command of its own or
/// as an option of `HELLO`.
pub struct Credentials<'a> {
    /// The username. Only `default` exists on this server, and a name that is
    /// not it is refused with the same text a wrong password is.
    user: &'a [u8],
    /// The candidate password.
    pass: &'a [u8],
}

/// The `HELLO` reply: a flat array of key-value pairs, which is how RESP2
/// carries a map.
///
/// The version and the mode are read from the node rather than written out
/// here, so that a client asking `HELLO` and a client reading `INFO` are told
/// the same two things by construction.
pub fn hello_frame(node: &NodeInfo) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(b"server".to_vec()),
        Frame::Bulk(SERVER_NAME.as_bytes().to_vec()),
        Frame::Bulk(b"version".to_vec()),
        Frame::Bulk(node.version.as_bytes().to_vec()),
        Frame::Bulk(b"proto".to_vec()),
        Frame::Integer(2),
        Frame::Bulk(b"mode".to_vec()),
        Frame::Bulk(SERVER_MODE.as_bytes().to_vec()),
        Frame::Bulk(b"role".to_vec()),
        Frame::Bulk(b"master".to_vec()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `HELLO 3` must be refused with the `NOPROTO` prefix specifically.
    ///
    /// go-redis v9 opens every connection with `HELLO 3` and downgrades to
    /// RESP2 on exactly this reply. Any other error text — including a
    /// perfectly reasonable `ERR unsupported protocol` — makes the client give
    /// up instead of falling back, so the string is a compatibility contract
    /// and not a message.
    #[test]
    fn the_noproto_text_is_byte_exact_to_redis() {
        assert_eq!(NOPROTO, "NOPROTO unsupported protocol version");
    }
}
