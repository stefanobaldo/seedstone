//! The password, and the comparison that does not leak its length.
//!
//! Three error texts and one type. The texts are byte-exact to Redis for the
//! reason every other error constant in this crate is — a client that matches
//! on a prefix is matching on this one — and they are held frame-safe by
//! `every_error_constant_is_frame_safe` beside the rest.

use std::sync::{Arc, RwLock};

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

/// The one or two passwords a node accepts.
///
/// Two, during a rotation: the operator adds the new one as a second line
/// of the password file and re-reads it, restarts the clients at their own
/// pace, then removes the old line and re-reads again. At every moment one
/// of the two lines is what every client holds, so no restart is ordered
/// against another and no client is refused for the length of somebody
/// else's boot.
///
/// `matches` costs the same whatever the count: two comparisons, always. With
/// one password the second comparison is against the same password and its
/// answer is dropped. There is no sentinel value standing in for the absent
/// slot, because any byte string — the empty one included — is a string a
/// peer can send, so there is no `Secret` that matches nothing; only a
/// comparison whose result is not used.
#[derive(Clone)]
pub struct Passwords {
    current: Secret,
    next: Option<Secret>,
}

impl Passwords {
    /// One password.
    #[must_use]
    pub const fn one(current: Secret) -> Self {
        Self {
            current,
            next: None,
        }
    }

    /// Two passwords, either of which authenticates.
    #[must_use]
    pub const fn two(current: Secret, next: Secret) -> Self {
        Self {
            current,
            next: Some(next),
        }
    }

    /// How many passwords this set holds: 1 or 2.
    #[must_use]
    pub const fn count(&self) -> usize {
        if self.next.is_some() { 2 } else { 1 }
    }

    /// Whether `candidate` is one of the passwords, in time that does not
    /// depend on which one it is or on how many there are.
    #[must_use]
    pub fn matches(&self, candidate: &[u8]) -> bool {
        let first = self.current.matches(candidate);
        let second = self.next.as_ref().map_or_else(
            || {
                // The same work as the arm above, so a timing tells nobody
                // how many lines the file has. `black_box` keeps the compiler
                // from noticing the answer is unused.
                let _ = std::hint::black_box(self.current.matches(candidate));
                false
            },
            |next| next.matches(candidate),
        );
        first || second
    }
}

impl std::fmt::Debug for Passwords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Passwords(<redacted>, count {})", self.count())
    }
}

/// The node's passwords, shared between the accept loop that may replace
/// them and every connection that reads them.
///
/// `None` is an open node. A connection reads the set once, at `AUTH` or at
/// the `HELLO` that carries one; the edge writes it once per `SIGHUP`. A
/// `RwLock` rather than an atomic pointer swap because there is no
/// contention to buy out of at that rate, and because it is in `std`.
///
/// The simulator never writes to it, so a replayed run reads the set the
/// original run read. Cloning is cheap and shares the cell: `NodeInfo` is
/// cloned per connection, and every clone must see the same passwords.
#[derive(Clone, Default)]
pub struct PasswordStore(Arc<RwLock<Option<Passwords>>>);

impl PasswordStore {
    /// A store holding `passwords`; `None` is an open node.
    #[must_use]
    pub fn new(passwords: Option<Passwords>) -> Self {
        Self(Arc::new(RwLock::new(passwords)))
    }

    /// The current set, cloned out from under the lock.
    #[must_use]
    pub fn load(&self) -> Option<Passwords> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Replaces the set. Connections already authenticated are not affected:
    /// authentication is a fact about the connection, decided when it
    /// happened, as it is in Redis.
    pub fn store(&self, passwords: Option<Passwords>) {
        *self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = passwords;
    }

    /// Whether a connection must authenticate before it can run anything.
    #[must_use]
    pub fn requires_auth(&self) -> bool {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }
}

impl std::fmt::Debug for PasswordStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PasswordStore(<redacted>)")
    }
}
