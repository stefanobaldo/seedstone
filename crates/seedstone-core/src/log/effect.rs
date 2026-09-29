//! What a record's payload says: the effect one mutation had on one key, or
//! on the whole shard.
//!
//! Effects, not commands, on purpose. A record that says `Put k v` replays to
//! the same state whatever came before it, so a shard's log can be cut to a
//! prefix and still replay exactly; a record that said `INCRBY k 5` would
//! replay to a different value on every prefix that lost an earlier
//! increment. Deadlines are absolute Unix milliseconds for the same reason:
//! `EX 30` means something only at the instant it was said.
//!
//! # Encoding
//!
//! Every integer is little-endian.
//!
//! ```text
//! tag  form
//! 1    Put:      u32 key len, key, u32 value len, value, deadline
//! 2    Del:      u32 key len, key
//! 3    Deadline: u32 key len, key, deadline
//! 4    Flush
//! 5    Rebase
//!
//! deadline: one byte, 0 for none, 1 followed by a u64 of Unix milliseconds
//! ```
//!
//! A payload with trailing bytes, an unknown tag or a length that runs past
//! its end is malformed, and recovery treats a malformed payload as the end
//! of that shard's prefix.
//!
//! A `Rebase` changes no key. It is the first record a shard writes after a
//! start whose recovery cut its log: the records an older generation left at
//! or above its sequence were not replayed, and they must not be on any later
//! start — see [`crate::log::recovery`].

use bytes::Bytes;

const TAG_PUT: u8 = 1;
const TAG_DEL: u8 = 2;
const TAG_DEADLINE: u8 = 3;
const TAG_FLUSH: u8 = 4;
const TAG_REBASE: u8 = 5;

/// The effect one mutation had, borrowed from the command that caused it.
///
/// A deadline is Unix milliseconds, absolute. `None` in a `Put` is a key
/// with no deadline; `None` in a `Deadline` is a deadline being removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect<'a> {
    /// The key now holds this value, with this deadline.
    Put {
        key: &'a [u8],
        value: &'a [u8],
        deadline: Option<u64>,
    },
    /// The key is gone — deleted, expired or evicted, which replay cannot
    /// and need not tell apart.
    Del { key: &'a [u8] },
    /// The key keeps its value and takes this deadline.
    Deadline {
        key: &'a [u8],
        deadline: Option<u64>,
    },
    /// The shard's whole keyspace is gone.
    Flush,
    /// The shard resumed here after a recovery that cut it: every record an
    /// older generation wrote at or above this one's sequence is dead.
    Rebase,
}

/// An [`Effect`] that owns its bytes: what recovery holds between reading a
/// segment and replaying it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owned {
    Put {
        key: Bytes,
        value: Bytes,
        deadline: Option<u64>,
    },
    Del {
        key: Bytes,
    },
    Deadline {
        key: Bytes,
        deadline: Option<u64>,
    },
    Flush,
    Rebase,
}

impl<'a> Effect<'a> {
    /// Appends this effect's encoding to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Put {
                key,
                value,
                deadline,
            } => {
                out.push(TAG_PUT);
                put_bytes(out, key);
                put_bytes(out, value);
                put_deadline(out, *deadline);
            }
            Self::Del { key } => {
                out.push(TAG_DEL);
                put_bytes(out, key);
            }
            Self::Deadline { key, deadline } => {
                out.push(TAG_DEADLINE);
                put_bytes(out, key);
                put_deadline(out, *deadline);
            }
            Self::Flush => out.push(TAG_FLUSH),
            Self::Rebase => out.push(TAG_REBASE),
        }
    }

    /// How many bytes [`encode`](Effect::encode) appends, so the caller can
    /// size its buffer once: every mutation pays for an encoding, and a
    /// buffer that grows while it is written pays for it more than once.
    #[must_use]
    pub const fn encoded_len(&self) -> usize {
        const LEN: usize = 4;
        const fn deadline_len(deadline: Option<u64>) -> usize {
            if deadline.is_some() { 1 + 8 } else { 1 }
        }
        1 + match self {
            Self::Put {
                key,
                value,
                deadline,
            } => LEN + key.len() + LEN + value.len() + deadline_len(*deadline),
            Self::Del { key } => LEN + key.len(),
            Self::Deadline { key, deadline } => LEN + key.len() + deadline_len(*deadline),
            Self::Flush | Self::Rebase => 0,
        }
    }

    /// Decodes a payload, or `None` if it is not a well-formed effect.
    #[must_use]
    pub fn decode(payload: &'a [u8]) -> Option<Self> {
        let (&tag, mut rest) = payload.split_first()?;
        let effect = match tag {
            TAG_PUT => {
                let key = take_bytes(&mut rest)?;
                let value = take_bytes(&mut rest)?;
                let deadline = take_deadline(&mut rest).ok()?;
                Self::Put {
                    key,
                    value,
                    deadline,
                }
            }
            TAG_DEL => Self::Del {
                key: take_bytes(&mut rest)?,
            },
            TAG_DEADLINE => {
                let key = take_bytes(&mut rest)?;
                let deadline = take_deadline(&mut rest).ok()?;
                Self::Deadline { key, deadline }
            }
            TAG_FLUSH => Self::Flush,
            TAG_REBASE => Self::Rebase,
            _ => return None,
        };
        // Trailing bytes are not a longer effect, they are a payload this
        // format did not write.
        rest.is_empty().then_some(effect)
    }

    /// This effect with its bytes copied out of the buffer they were read
    /// from.
    #[must_use]
    pub fn to_owned(&self) -> Owned {
        match *self {
            Self::Put {
                key,
                value,
                deadline,
            } => Owned::Put {
                key: Bytes::copy_from_slice(key),
                value: Bytes::copy_from_slice(value),
                deadline,
            },
            Self::Del { key } => Owned::Del {
                key: Bytes::copy_from_slice(key),
            },
            Self::Deadline { key, deadline } => Owned::Deadline {
                key: Bytes::copy_from_slice(key),
                deadline,
            },
            Self::Flush => Owned::Flush,
            Self::Rebase => Owned::Rebase,
        }
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len =
        u32::try_from(bytes.len()).expect("a key or value fits the codec's 16 MiB bulk ceiling");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
}

fn put_deadline(out: &mut Vec<u8>, deadline: Option<u64>) {
    match deadline {
        None => out.push(0),
        Some(millis) => {
            out.push(1);
            out.extend_from_slice(&millis.to_le_bytes());
        }
    }
}

fn take_bytes<'a>(cursor: &mut &'a [u8]) -> Option<&'a [u8]> {
    let (len, rest) = cursor.split_first_chunk::<4>()?;
    let len = usize::try_from(u32::from_le_bytes(*len)).ok()?;
    if rest.len() < len {
        return None;
    }
    let (bytes, rest) = rest.split_at(len);
    *cursor = rest;
    Some(bytes)
}

/// A payload this format did not write.
struct Malformed;

fn take_deadline(cursor: &mut &[u8]) -> Result<Option<u64>, Malformed> {
    let (&flag, rest) = cursor.split_first().ok_or(Malformed)?;
    match flag {
        0 => {
            *cursor = rest;
            Ok(None)
        }
        1 => {
            let (millis, rest) = rest.split_first_chunk::<8>().ok_or(Malformed)?;
            *cursor = rest;
            Ok(Some(u64::from_le_bytes(*millis)))
        }
        _ => Err(Malformed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_effect_round_trips() {
        let cases = [
            Effect::Put {
                key: b"k",
                value: b"v",
                deadline: None,
            },
            Effect::Put {
                key: b"k",
                value: b"",
                deadline: Some(1_700_000_000_123),
            },
            Effect::Del { key: b"gone" },
            Effect::Deadline {
                key: b"k",
                deadline: Some(u64::MAX),
            },
            Effect::Deadline {
                key: b"k",
                deadline: None,
            },
            Effect::Flush,
            Effect::Rebase,
        ];
        for effect in cases {
            let mut out = Vec::new();
            effect.encode(&mut out);
            assert_eq!(out.len(), effect.encoded_len(), "{effect:?}");
            assert_eq!(Effect::decode(&out), Some(effect), "{effect:?}");
        }
    }

    #[test]
    fn the_encoding_is_byte_for_byte_stable() {
        let mut out = Vec::new();
        Effect::Put {
            key: b"ab",
            value: b"c",
            deadline: Some(0x0102),
        }
        .encode(&mut out);
        assert_eq!(
            out,
            [
                1, 2, 0, 0, 0, b'a', b'b', 1, 0, 0, 0, b'c', 1, 0x02, 0x01, 0, 0, 0, 0, 0, 0
            ]
        );
    }

    #[test]
    fn a_malformed_payload_decodes_to_nothing() {
        // Unknown tag, a length past the end, a deadline flag that is
        // neither 0 nor 1, trailing bytes, and nothing at all.
        assert_eq!(Effect::decode(&[9]), None);
        assert_eq!(Effect::decode(&[2, 5, 0, 0, 0, b'a']), None);
        assert_eq!(Effect::decode(&[3, 1, 0, 0, 0, b'k', 7]), None);
        assert_eq!(Effect::decode(&[4, 0]), None);
        assert_eq!(Effect::decode(&[]), None);
    }

    #[test]
    fn to_owned_keeps_every_field() {
        let effect = Effect::Put {
            key: b"k",
            value: b"v",
            deadline: Some(5),
        };
        assert_eq!(
            effect.to_owned(),
            Owned::Put {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                deadline: Some(5),
            }
        );
        assert_eq!(Effect::Flush.to_owned(), Owned::Flush);
    }
}
