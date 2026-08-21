//! The replication log: the on-disk record format, the trait every mutating
//! command passes through, and the no-op that stands in until persistence
//! arrives.
//!
//! # Why the format tolerates holes
//!
//! The simulator tears each pending write independently, so a log recovered
//! after a crash is not a *prefix* of what was written — it is what was
//! written with arbitrary holes punched in it. A reader that stops at the
//! first bad checksum would therefore discard every intact record that
//! happened to sit after the hole, silently. Measured against that fault
//! model: 20/20 crashed logs were torn rather than truncated, and 8/20
//! salvaged nothing at all under prefix-scan recovery.
//!
//! Hence [`Decoded::Corrupt`], which reports how far to step forward rather
//! than ending the read. Damage costs the records inside the hole and nothing
//! else. The no-op log writes no bytes, but the record types are the ones a
//! real log will write: the format is right from birth.
//!
//! # Record layout
//!
//! Every integer field is little-endian.
//!
//! ```text
//! offset  width      field
//! 0       1          magic byte, 0xA5 (0x00 instead = end-of-log marker)
//! 1       4          body length in bytes, u32
//! 5       4          CRC-32/ISO-HDLC of the body, u32
//! 9       2          shard id, u16          \
//! 11      8          sequence number, u64    | the body, `body length` bytes
//! 19      len - 10   payload bytes          /
//! ```
//!
//! The checksum covers the body only, not the header: a header damaged in the
//! magic or length field is caught by those fields being implausible, and a
//! damaged checksum field can only cause a false mismatch, which is already
//! handled as damage.
//!
//! A `0x00` in the magic position is the explicit end-of-log marker. It is
//! only meaningful at a record boundary the reader arrived at by consuming
//! whole records — see [`Decoded::EndOfLog`].

/// Magic byte that opens a record.
///
/// Chosen with both nibbles set and alternating bits so that it is not a value
/// a zero-filled or all-ones region of a torn file produces by accident.
const MAGIC: u8 = 0xA5;

/// A `0x00` in the magic position: no record was ever written here.
const END_OF_LOG: u8 = 0x00;

/// Magic byte, length and checksum: the fixed header before every body.
const HEADER_LEN: usize = 1 + 4 + 4;

/// The fixed part of a body: `shard` (u16) then `seq` (u64).
///
/// A body shorter than this cannot be a record, however plausible its
/// checksum, so a length field below it is damage rather than a short read.
const BODY_FIXED_LEN: usize = 2 + 8;

/// The largest body this format will believe in.
///
/// A length field is four bytes of untrusted data: without a ceiling, one
/// flipped bit can claim a body of nearly 4 GiB and wedge a reader waiting
/// forever for bytes that will never arrive. 64 MiB is far above any command
/// a shard will ever log and far below what a corrupt field typically claims.
/// [`encode_record`] debug-asserts that it never produces a record above it.
///
/// Public so the crate that will one day build record payloads out of
/// wire-sized values can hold its arithmetic against this ceiling in a test,
/// instead of the two constants merely happening to be ordered. Today every
/// payload is empty; the day that changes, the debug-assert above becomes
/// reachable, and the ordering stops being anyone's coincidence to preserve.
pub const MAX_BODY_LEN: usize = 64 * 1024 * 1024;

/// One entry in the log: a command applied to one shard, at one position in
/// that shard's sequence.
///
/// It borrows its payload because the caller — a shard task about to apply a
/// mutating command — already holds those bytes and the log has no reason to
/// take a copy on a path that, today, writes nothing at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record<'a> {
    /// The shard this record belongs to.
    pub shard: u16,
    /// The record's position in its shard's sequence.
    pub seq: u64,
    /// The opaque bytes of the logged command.
    pub payload: &'a [u8],
}

/// What [`decode_record`] found at the start of a buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decoded<'a> {
    /// One intact record.
    Record {
        /// The shard the record belongs to.
        shard: u16,
        /// The record's position in its shard's sequence.
        seq: u64,
        /// The logged command's bytes, borrowed from the input buffer.
        payload: &'a [u8],
        /// Total bytes this record occupied, header included. Advance the
        /// cursor by exactly this to reach the next record boundary.
        consumed: usize,
    },
    /// The explicit end-of-log marker.
    ///
    /// Trustworthy only at a boundary the reader reached by consuming whole
    /// records. A reader that is resynchronising after damage is at an
    /// arbitrary offset, where a `0x00` is far more likely to be one byte of
    /// some record's `seq` field than a marker — such a reader must keep
    /// stepping, not stop.
    EndOfLog,
    /// Everything in the buffer so far is still plausible, but a record has
    /// not been completed.
    ///
    /// What that licenses depends on where the reader is, exactly as with
    /// [`EndOfLog`](Decoded::EndOfLog):
    ///
    /// - **At a boundary the reader reached by consuming whole records**, this
    ///   is an ordinary short read. Get more bytes and call again with a longer
    ///   buffer. Note that plausible is not the same as valid: a header whose
    ///   length field is plausible reports `NeedMore`, and the very same prefix
    ///   extended by its body can then report [`Corrupt`](Decoded::Corrupt) on
    ///   a checksum mismatch. A prefix is a promise about the bytes seen, not
    ///   about the bytes to come.
    /// - **While resynchronising after damage**, the reader is at an arbitrary
    ///   offset and the length field it just read was invented by the damage.
    ///   It must step one byte forward and decode again, exactly as it does for
    ///   `Corrupt`. A recovery reader that waits here instead stalls forever on
    ///   bytes that will never arrive — the wedge the rest of this format is
    ///   built to avoid.
    NeedMore,
    /// The bytes at the start of the buffer are damaged. Advance the cursor by
    /// `skip` and decode again.
    ///
    /// `skip` is one byte: resynchronisation cannot assume anything about the
    /// damaged region, and any larger stride risks stepping over the start of
    /// an intact record that survived the hole.
    ///
    /// # The reader must bound its resynchronisation
    ///
    /// Stepping a byte at a time is safe but not free, and payload bytes are
    /// opaque and network-controlled: a single value can embed any number of
    /// fake `0xA5` bytes followed by plausible length fields. After one byte of
    /// real damage, a reader that tries every offset pays a full bitwise
    /// checksum over each candidate whose claimed body happens to be present —
    /// up to 64 MiB apiece. Over an N-byte region of adversarial content that
    /// is O(N²) byte-operations, which for N in the megabytes grinds for hours
    /// rather than hanging outright, and so is far harder to diagnose.
    ///
    /// Bounding that work is the reader's job, not this function's. Either
    /// strategy suffices, and they compose:
    ///
    /// - Do not checksum a candidate whose claimed body extends past the extent
    ///   already known to be damaged — a real record cannot start inside the
    ///   hole and end beyond the region under repair.
    /// - Cap total resynchronisation effort (bytes checksummed, or candidates
    ///   tried) per hole, and give up on the region rather than the log.
    Corrupt {
        /// How far to advance before decoding again.
        skip: usize,
    },
}

/// Computes the CRC-32/ISO-HDLC ("IEEE") checksum of `data`.
///
/// Reflected polynomial `0xEDB8_8320`, initial value `0xFFFF_FFFF`, input and
/// output reflected, final XOR `0xFFFF_FFFF`. Written bitwise rather than
/// pulled in as a dependency: it runs once per logged command, and a table
/// would be a build-time input to a value the format is pinned to forever.
fn crc32_iso_hdlc(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Appends the wire encoding of `rec` to `out`.
///
/// `out` is appended to rather than replaced, so a caller can pack several
/// records into one buffer and hand the whole thing to a single write.
///
/// # Preconditions
///
/// The body — the ten fixed bytes plus the payload — must not exceed 64 MiB.
///
/// Three things discharge that obligation today, none of them here. The RESP
/// codec caps a single payload at `MAX_BULK_LEN` (in `seedstone-resp`, 16 MiB); a
/// command carries at most two payloads of that size, a key and a value, since
/// the options it may also carry are small literals and short numbers
/// (`SET k v EX 30` adds tens of bytes, not a third payload); and a record
/// describes one key, because a shard command addresses exactly one key and a
/// request naming many is fanned out into one command — and so one record —
/// per key before it reaches a shard. So the largest record any accepted
/// command can produce is about 32 MiB plus framing, comfortably under the
/// 64 MiB ceiling.
///
/// **All three are assumptions here, not enforcements.** They live in the
/// command layer, and a command carrying a third unbounded payload — or a
/// record built from a whole multi-key request — breaks the arithmetic without
/// touching this file. `MAX_BULK_LEN`'s own documentation, in `seedstone-resp`,
/// states the same from the other side; whoever adds such a command owns
/// re-deriving both.
///
/// The `debug_assert!` below is a development tripwire, not that rejection: it
/// turns a violation into a panic in debug, test and simulator builds. In
/// release the record is still written, with an exact length that
/// [`decode_record`] will always classify as damage — a silently unreadable
/// entry.
///
/// It is a `debug_assert!` rather than a `Result` so the check costs nothing on
/// the path every mutating command takes; the moment the command layer exists,
/// the ceiling belongs there.
///
/// # Panics
///
/// If the body exceeds `u32::MAX` bytes, which the length field cannot
/// describe. That is four thousand times the documented ceiling, so reaching it
/// means the precondition above was violated by a wide margin. The narrowing is
/// checked rather than truncating: a truncated length would produce a header
/// describing a different record than the bytes that follow it, and a log that
/// silently disagrees with itself is worse than one that stops.
pub fn encode_record(rec: &Record<'_>, out: &mut Vec<u8>) {
    let body_len = BODY_FIXED_LEN + rec.payload.len();
    debug_assert!(
        body_len <= MAX_BODY_LEN,
        "encode_record: body of {body_len} bytes exceeds the {MAX_BODY_LEN}-byte ceiling"
    );

    out.reserve(HEADER_LEN + body_len);
    out.push(MAGIC);
    let declared = u32::try_from(body_len)
        .expect("encode_record: a body length must fit the format's u32 field");
    out.extend_from_slice(&declared.to_le_bytes());

    // The body's fields live in three places until they are written, so
    // reserve the checksum's slot, write the body, and fold the checksum over
    // the bytes that landed. The encoder then hashes exactly the slice the
    // decoder will hash — the two cannot drift apart over what is covered.
    let crc_at = out.len();
    out.extend_from_slice(&[0u8; 4]);
    let body_at = out.len();
    out.extend_from_slice(&rec.shard.to_le_bytes());
    out.extend_from_slice(&rec.seq.to_le_bytes());
    out.extend_from_slice(rec.payload);

    let crc = crc32_iso_hdlc(&out[body_at..]);
    out[crc_at..body_at].copy_from_slice(&crc.to_le_bytes());
}

/// Decodes the record at the start of `buf`.
///
/// `buf` may hold more than one record, a trailing partial one, or damage.
/// See [`Decoded`] for how to drive this in a loop; in particular, damage
/// reports a stride rather than ending the read.
///
/// # The line between a short read and damage
///
/// [`Decoded::NeedMore`] is a claim about the bytes seen and nothing more:
/// every one of them is still plausible, so a longer buffer *may* complete
/// them. It is not a promise that one will — the same prefix, extended by a
/// body whose checksum disagrees, reports [`Decoded::Corrupt`]. It is returned
/// while the header has not fully arrived, or has and the body it plausibly
/// describes has not.
///
/// A length field that could never describe a record is not a short read at
/// all. Below the ten fixed body bytes no payload could make it valid; above
/// the 64 MiB ceiling no log this system writes could produce it. Reporting
/// `NeedMore` for either would leave a reader waiting forever on bytes that
/// can never make sense, which is exactly the wedge this format exists to
/// avoid — so both are [`Decoded::Corrupt`], and the length is validated
/// before the body's arrival is even considered.
#[must_use]
pub fn decode_record(buf: &[u8]) -> Decoded<'_> {
    match buf.first() {
        None => return Decoded::NeedMore,
        Some(&END_OF_LOG) => return Decoded::EndOfLog,
        Some(&MAGIC) => {}
        Some(_) => return Decoded::Corrupt { skip: 1 },
    }
    if buf.len() < HEADER_LEN {
        return Decoded::NeedMore;
    }

    // Plausibility before arrival: a length outside this range describes no
    // record any longer buffer could complete, so it is damage and not a
    // short read.
    let declared = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
    // On a target whose `usize` is narrower than the length field, a length
    // this platform cannot even address is damage like any other — never a
    // panic, because these four bytes are untrusted input.
    let Ok(body_len) = usize::try_from(declared) else {
        return Decoded::Corrupt { skip: 1 };
    };
    if !(BODY_FIXED_LEN..=MAX_BODY_LEN).contains(&body_len) {
        return Decoded::Corrupt { skip: 1 };
    }

    let consumed = HEADER_LEN + body_len;
    if buf.len() < consumed {
        return Decoded::NeedMore;
    }

    let body = &buf[HEADER_LEN..consumed];
    let expected = u32::from_le_bytes([buf[5], buf[6], buf[7], buf[8]]);
    if crc32_iso_hdlc(body) != expected {
        return Decoded::Corrupt { skip: 1 };
    }

    Decoded::Record {
        shard: u16::from_le_bytes([body[0], body[1]]),
        seq: u64::from_le_bytes([
            body[2], body[3], body[4], body[5], body[6], body[7], body[8], body[9],
        ]),
        payload: &body[BODY_FIXED_LEN..],
        consumed,
    }
}

/// The seam every mutating command passes through before it is applied.
///
/// The seam exists from day one even though nothing yet writes bytes, so that
/// the call sits in the flow of every mutating command from the beginning
/// rather than being threaded through later. A real log replaces the
/// implementation, not the callers: `ShardPool::spawn_with_log` takes a
/// per-shard factory, so a real log arrives as an argument at one call site.
///
/// # Where each method is called from
///
/// `append` runs inside the shard's command handler, which is a plain `fn`
/// that cannot `await` — so an implementation must keep it cheap. Buffering in
/// memory is the shape this split anticipates. `sync` is called from the shard
/// task's housekeeping tick, which is `async` and may block; that is the only
/// place in a shard where durability can be paid for.
///
/// What is *not* settled here, deliberately: whether `append` buffers or
/// writes through, the fsync cadence, and whether shards sync independently or
/// group-commit behind one shared writer. The factory admits all three — a
/// shared writer is a factory returning clones of one handle.
///
/// `Send + 'static` because a shard task owns its log and is moved onto the
/// runtime with it: the bound is what lets a shard hold a `Box<dyn
/// ReplicationLog>` without knowing which one it got.
///
/// The methods take `&mut self` because a real implementation owns a file
/// handle and a write position; only one caller may hold it, and only one
/// does — the shard task.
pub trait ReplicationLog: Send + 'static {
    /// Records `rec`, before the command it describes is applied.
    ///
    /// Returning `Ok` means the record is accepted, not that it is durable.
    /// Durability is [`sync`](ReplicationLog::sync)'s job, and the two are
    /// separate because syncing every append is what makes torn writes
    /// unobservable — precisely the fault this format is built to survive.
    ///
    /// # Errors
    ///
    /// Whatever the underlying store reports. The mutation the record
    /// describes must not proceed: a change applied without its record is the
    /// one divergence recovery cannot detect.
    fn append(&mut self, rec: Record<'_>) -> std::io::Result<()>;

    /// Makes everything appended so far durable.
    ///
    /// # Errors
    ///
    /// Whatever the underlying store reports. Nothing appended since the last
    /// successful sync may be assumed durable afterwards.
    fn sync(&mut self) -> std::io::Result<()>;
}

/// A [`ReplicationLog`] that keeps nothing.
///
/// The one implementation this server ships today. Every method succeeds without
/// doing anything: there are no bytes to write, so there is nothing for
/// [`sync`](ReplicationLog::sync) to make durable and it is trivially
/// satisfied.
#[derive(Debug)]
pub struct NoopLog;

impl ReplicationLog for NoopLog {
    fn append(&mut self, _rec: Record<'_>) -> std::io::Result<()> {
        Ok(())
    }

    fn sync(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decodes `buf` the way a recovery scan over a fully-read log does:
    /// consume whole records, step one byte over damage, and stop only at a
    /// boundary it trusts.
    fn scan(buf: &[u8]) -> Vec<(u16, u64, Vec<u8>)> {
        let mut records = Vec::new();
        let mut cursor = 0;
        let mut resynchronising = false;
        while cursor < buf.len() {
            match decode_record(&buf[cursor..]) {
                Decoded::Record {
                    shard,
                    seq,
                    payload,
                    consumed,
                } => {
                    records.push((shard, seq, payload.to_vec()));
                    cursor += consumed;
                    resynchronising = false;
                }
                Decoded::Corrupt { skip } => {
                    cursor += skip;
                    resynchronising = true;
                }
                // While resynchronising the scan sits at an arbitrary offset
                // inside damage, where neither of these means what it says: a
                // `0x00` is far more likely to be one byte of a `seq` field
                // than a marker, and a short read is a length field invented
                // by the damage. Step one byte and keep looking.
                Decoded::EndOfLog | Decoded::NeedMore if resynchronising => cursor += 1,
                // At a trusted boundary they are the two honest ways a log
                // ends: the explicit marker, or a truncation mid-record.
                Decoded::EndOfLog | Decoded::NeedMore => break,
            }
        }
        records
    }

    #[test]
    fn crc32_matches_the_iso_hdlc_check_value() {
        assert_eq!(crc32_iso_hdlc(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn records_round_trip() {
        let long_payload: Vec<u8> = (0..300u32)
            .map(|i| u8::try_from(i % 251).expect("a remainder mod 251 fits a u8"))
            .collect();
        let cases: &[Record<'_>] = &[
            Record {
                shard: 0,
                seq: 0,
                payload: b"",
            },
            Record {
                shard: 7,
                seq: 1,
                payload: b"SET k v",
            },
            Record {
                shard: u16::MAX,
                seq: u64::MAX,
                payload: b"\x00\xa5\xff\r\n",
            },
            Record {
                shard: 1023,
                seq: 42,
                payload: &long_payload,
            },
        ];
        for rec in cases {
            let mut out = Vec::new();
            encode_record(rec, &mut out);
            assert_eq!(
                decode_record(&out),
                Decoded::Record {
                    shard: rec.shard,
                    seq: rec.seq,
                    payload: rec.payload,
                    consumed: out.len(),
                },
                "{rec:?}"
            );
        }
    }

    #[test]
    fn the_encoding_is_byte_for_byte_stable() {
        // Pins field order, widths and endianness: a hex dump of this record
        // must look like this forever, on every platform.
        let mut out = Vec::new();
        encode_record(
            &Record {
                shard: 0x0102,
                seq: 0x0807_0605_0403_0201,
                payload: b"hi",
            },
            &mut out,
        );
        let body = b"\x02\x01\x01\x02\x03\x04\x05\x06\x07\x08hi";
        let mut expected = vec![0xA5, 0x0C, 0x00, 0x00, 0x00];
        // The expected checksum comes from `crc32_iso_hdlc` itself, so this
        // test says nothing about whether that function is right — it is only
        // sound because `crc32_matches_the_iso_hdlc_check_value` pins the
        // function against the published CRC-32/ISO-HDLC check value
        // separately. Do not fold the two together, and do not drop that test
        // as redundant: without it a self-consistently wrong checksum passes
        // here, at round-trip, and at every corruption test.
        expected.extend_from_slice(&crc32_iso_hdlc(body).to_le_bytes());
        expected.extend_from_slice(body);
        assert_eq!(out, expected);
    }

    #[test]
    fn several_records_decode_back_to_back() {
        let mut buf = Vec::new();
        for seq in 0..3u8 {
            encode_record(
                &Record {
                    shard: 5,
                    seq: u64::from(seq),
                    payload: &[seq; 4],
                },
                &mut buf,
            );
        }
        buf.push(END_OF_LOG);
        assert_eq!(
            scan(&buf),
            vec![(5, 0, vec![0; 4]), (5, 1, vec![1; 4]), (5, 2, vec![2; 4]),]
        );
    }

    #[test]
    fn every_proper_prefix_of_a_record_needs_more() {
        let mut buf = Vec::new();
        encode_record(
            &Record {
                shard: 3,
                seq: 9,
                payload: b"hello",
            },
            &mut buf,
        );
        for cut in 0..buf.len() {
            assert_eq!(
                decode_record(&buf[..cut]),
                Decoded::NeedMore,
                "cut at {cut} of {buf:?}"
            );
        }
    }

    #[test]
    fn an_empty_buffer_needs_more() {
        assert_eq!(decode_record(b""), Decoded::NeedMore);
    }

    #[test]
    fn a_zero_magic_byte_is_the_end_of_log() {
        assert_eq!(decode_record(&[END_OF_LOG]), Decoded::EndOfLog);
        assert_eq!(decode_record(&[END_OF_LOG, 1, 2, 3]), Decoded::EndOfLog);
    }

    #[test]
    fn a_bad_magic_byte_is_corrupt() {
        // Distinct from a bad checksum: the record never even started here.
        assert_eq!(decode_record(&[0x5A]), Decoded::Corrupt { skip: 1 });
        assert_eq!(decode_record(&[0xFF; 32]), Decoded::Corrupt { skip: 1 });
    }

    #[test]
    fn a_flipped_payload_byte_is_corrupt() {
        let mut buf = Vec::new();
        encode_record(
            &Record {
                shard: 3,
                seq: 9,
                payload: b"hello",
            },
            &mut buf,
        );
        let payload_start = HEADER_LEN + BODY_FIXED_LEN;
        buf[payload_start] ^= 0x01;
        assert_eq!(decode_record(&buf), Decoded::Corrupt { skip: 1 });
    }

    #[test]
    fn a_flipped_checksum_byte_is_corrupt() {
        let mut buf = Vec::new();
        encode_record(
            &Record {
                shard: 3,
                seq: 9,
                payload: b"hello",
            },
            &mut buf,
        );
        buf[5] ^= 0x01;
        assert_eq!(decode_record(&buf), Decoded::Corrupt { skip: 1 });
    }

    #[test]
    fn an_impossible_length_is_corrupt_rather_than_need_more() {
        // A body too short to hold shard and seq, and a body larger than the
        // format will believe in, are both damage. Reporting NeedMore would
        // wedge the reader waiting for bytes that can never make sense.
        let fixed = u32::try_from(BODY_FIXED_LEN).expect("the fixed body length is a small const");
        for len in [0u32, 1, fixed - 1] {
            let mut buf = vec![MAGIC];
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
            assert_eq!(decode_record(&buf), Decoded::Corrupt { skip: 1 }, "{len}");
        }

        let mut buf = vec![MAGIC];
        buf.extend_from_slice(&(max_body_len_u32() + 1).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(decode_record(&buf), Decoded::Corrupt { skip: 1 });
    }

    /// [`MAX_BODY_LEN`] as the width the length field actually carries.
    fn max_body_len_u32() -> u32 {
        u32::try_from(MAX_BODY_LEN).expect("the ceiling is below 4 GiB by construction")
    }

    #[test]
    fn a_body_length_at_the_ceiling_is_still_only_a_short_read() {
        // The ceiling rejects what is above it and nothing else: a header
        // claiming exactly MAX_BODY_LEN is plausible, so a buffer that has not
        // yet delivered that body is a short read.
        let mut buf = vec![MAGIC];
        buf.extend_from_slice(&max_body_len_u32().to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(decode_record(&buf), Decoded::NeedMore);
    }

    #[test]
    fn a_hole_costs_only_the_records_inside_it() {
        // The whole point of the format: a damaged record must not end the
        // read. Encode a valid record, damage it, append another valid one,
        // and the scan must still recover the later record intact. The
        // damaged record's `seq` field is seven zero bytes, so recovery only
        // works if resynchronisation steps over a `0x00` instead of reading
        // it as the end of the log.
        let mut buf = Vec::new();
        encode_record(
            &Record {
                shard: 1,
                seq: 1,
                payload: b"lost to the hole",
            },
            &mut buf,
        );
        let damaged = buf.len();
        buf[HEADER_LEN + BODY_FIXED_LEN + 2] ^= 0xFF;
        encode_record(
            &Record {
                shard: 2,
                seq: 2,
                payload: b"survived",
            },
            &mut buf,
        );
        buf.push(END_OF_LOG);

        assert_eq!(
            decode_record(&buf[..damaged]),
            Decoded::Corrupt { skip: 1 },
            "the damaged record must decode as damage on its own"
        );
        assert_eq!(scan(&buf), vec![(2, 2, b"survived".to_vec())]);
    }

    #[test]
    fn a_zero_run_at_a_trusted_boundary_ends_the_read() {
        // The limit of an explicit marker, pinned so nobody expects more of
        // it: a torn write that leaves zeroes exactly at a record boundary is
        // indistinguishable from a log that ended there, and the scan stops.
        // Records after such a hole are lost — which is why the marker is
        // only ever trusted at a boundary, never during resynchronisation.
        let mut buf = vec![0u8; 24];
        encode_record(
            &Record {
                shard: 4,
                seq: 8,
                payload: b"after the hole",
            },
            &mut buf,
        );
        buf.push(END_OF_LOG);
        assert_eq!(decode_record(&buf), Decoded::EndOfLog);
        assert!(scan(&buf).is_empty());
    }

    #[test]
    fn the_noop_log_accepts_appends_and_syncs() {
        let mut log = NoopLog;
        log.append(Record {
            shard: 0,
            seq: 0,
            payload: b"SET k v",
        })
        .expect("the no-op log never fails");
        log.sync().expect("the no-op log never fails");
    }

    #[test]
    fn the_noop_log_is_usable_through_the_trait() {
        // The shard task holds its log behind the trait, so `NoopLog` has to
        // satisfy `ReplicationLog`'s `Send + 'static` bound as an object.
        let mut log: Box<dyn ReplicationLog> = Box::new(NoopLog);
        log.append(Record {
            shard: 1,
            seq: 1,
            payload: b"",
        })
        .expect("the no-op log never fails");
        log.sync().expect("the no-op log never fails");
    }
}
