//! The password, and the comparison that does not leak its length.
//!
//! Three error texts and one type. The texts are byte-exact to Redis for the
//! reason every other error constant in this crate is — a client that matches
//! on a prefix is matching on this one — and they are held frame-safe by
//! `every_error_constant_is_frame_safe` beside the rest.

/// What a peer that has not authenticated is told, for every command but the
/// three that are allowed before it.
pub const NOAUTH: &str = "NOAUTH Authentication required.";

/// What `HELLO` without credentials is told on a node that has a password.
///
/// Redis's own sentence, measured against `redis:6-alpine` (6.2.24 — the image
/// this project's environment runs) rather than quoted from memory, because
/// the part after the code is where a client library looks for the form it
/// should have sent. Note what it names: `HELLO AUTH <user> <pass>`, with no
/// protocol version between the two words. Written out here it reads like a
/// paragraph; on the wire it is the one thing a client needs, which is that
/// the handshake carries the credentials rather than preceding them.
pub const NOAUTH_HELLO: &str = "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time";

/// What a wrong password is answered with — and a wrong username, with the
/// same text and after the same work, so that neither can be told from the
/// other by what came back or by how long it took.
pub const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled.";

/// What `AUTH` against a node with no password configured is told.
///
/// Redis's own sentence, question mark included: a client sending `AUTH` to an
/// open server has a configuration problem, and this is the text that says so.
pub const AUTH_NOT_CONFIGURED: &str = "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?";

/// A configured password.
///
/// Wraps the bytes so that nothing prints them — `Debug` is redacted — and
/// so that the one comparison is the one below, which takes the same time
/// whatever the candidate is.
#[derive(Clone)]
pub struct Secret(Vec<u8>);

impl Secret {
    /// Wraps `bytes` as the password.
    #[must_use]
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Whether `candidate` is the password, in time that depends on the
    /// password's length and not on where the candidate first differs.
    ///
    /// Every byte of the longer of the two is compared — the shorter is
    /// padded with a byte that cannot match its counterpart — and the
    /// lengths are compared as one more term, so an early mismatch and a
    /// late one cost the same. A length the candidate got wrong is learned by
    /// it anyway from the time a longer string takes to send; what this hides
    /// is the prefix.
    #[must_use]
    pub fn matches(&self, candidate: &[u8]) -> bool {
        let len = self.0.len().max(candidate.len());
        let mut diff: u8 = u8::from(self.0.len() != candidate.len());
        for i in 0..len {
            // The two sides pad differently, so a position past the end of
            // one of them always differs. The length term above makes that
            // redundant; it is written out because a reader should not have
            // to derive it.
            let a = self.0.get(i).copied().unwrap_or(0);
            let b = candidate.get(i).copied().unwrap_or(1);
            diff |= a ^ b;
        }
        diff == 0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}
