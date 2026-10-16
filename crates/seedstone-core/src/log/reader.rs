//! Reading records back: whole records consumed, damage stepped over one
//! byte at a time, and the end of a segment trusted only at a boundary
//! reached by whole records.
//!
//! Even there, an end marker is taken at its word only if nothing intact
//! follows it. The writer never writes one, so a zero where a record should
//! start is a zeroed range as often as it is the end, and the reader looks
//! past it: records after it are recovered and it is counted as a hole.
//!
//! The rule was born in `log.rs`'s tests: the simulator tears each
//! pending write independently, so a crashed segment
//! is not a prefix of what was written but what was written with holes in
//! it. A reader that stops at the first bad checksum discards every intact
//! record after the hole. This one steps over the hole and keeps going.
//!
//! Two bounds `Decoded::Corrupt` demands of any reader are met here: a
//! candidate whose declared body runs past the end of the segment is never
//! checksummed, and the bytes checksummed while resynchronising after one
//! hole are capped at [`RESYNC_CAP`] — past it the rest of the *segment*
//! is given up, and counted, rather than the process grinding for hours on
//! adversarial payload bytes. The cap ends at a candidate's boundary: the
//! one that crosses it is still checksummed whole, because it may be the
//! record the hole ends at.
//!
//! The reader reads in chunks of [`READ_CHUNK`] rather than the whole file,
//! and that is not an optimisation: the simulator corrupts reads per call,
//! so a reader that pulled a segment in one call would see its damage
//! land anywhere, while one that reads in chunks sees it confined to a
//! region — the fault a real disk delivers.
//!
//! [`ReaderMode::PrefixScan`] is the reader this module exists to replace,
//! kept so the simulator can plant it and show it caught: it treats the
//! first damage as the end of the segment, silently.

use std::io::{self, Read};

use crate::log::{BODY_FIXED_LEN, Decoded, HEADER_LEN, MAGIC, MAX_BODY_LEN, decode_record};

/// How many bytes a read pulls at a time.
pub const READ_CHUNK: usize = 64 * 1024;

/// How many bytes may be checksummed while resynchronising after one hole
/// before the rest of the segment is given up.
///
/// The cap is spent by whole candidates. One that begins with the hole at
/// or under the cap is checksummed whatever it declares — it may be the
/// intact record the hole ends at, and refusing it would give up every
/// record after it untried — and the first one after the hole has gone
/// past the cap is refused. So a hole checksums at most this plus one
/// body of [`MAX_BODY_LEN`]: seconds of work, where unbounded it is the
/// O(N²) grind `Decoded::Corrupt` warns of.
pub const RESYNC_CAP: u64 = 4 * 1024 * 1024;

/// How a reader treats damage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderMode {
    /// Step over it a byte at a time and keep reading. The reader.
    Resynchronising,
    /// Treat the first damage as the end of the segment, silently. The
    /// defect the simulator plants; never the server's own.
    PrefixScan,
}

/// What a reader saw that was not a record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Damage {
    /// Bytes stepped over while resynchronising.
    pub bytes: u64,
    /// Regions of damage entered.
    pub holes: u64,
    /// Bytes of a final record the segment ended inside of.
    pub truncated_tail: u64,
    /// Whether the rest of the segment was given up past [`RESYNC_CAP`].
    pub abandoned: bool,
}

/// One intact record, its payload copied out of the reader's buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    /// The shard the record belongs to.
    pub shard: u16,
    /// Its place in that shard's sequence.
    pub seq: u64,
    /// The encoded effect.
    pub payload: Vec<u8>,
    /// The body offset the record's magic byte sits at: bytes from the end
    /// of the segment's header.
    pub at: u64,
    /// The body offset just past its last byte. A hole the reader entered
    /// after this record began here: the boundary it was at.
    pub end: u64,
}

/// A chunked, hole-tolerant reader over one segment's records.
pub struct Reader<R: Read> {
    src: R,
    /// The bytes of records the source holds: the `len` it was given.
    len: u64,
    /// Bytes of the source not yet pulled into `buf`.
    remaining: u64,
    buf: Vec<u8>,
    /// Where in `buf` the next decode starts.
    at: usize,
    chunk: usize,
    mode: ReaderMode,
    resynchronising: bool,
    /// Set when the current hole began at a candidate the segment's end cut
    /// short: the damage count at that moment, so the hole can be
    /// reclassified as a truncated tail if nothing intact follows it.
    hole_at_tail: Option<u64>,
    /// Set when the current hole began at an end marker read at a trusted
    /// boundary: the damage as it stood. The writer never writes the
    /// marker, so it ends the segment honestly only if nothing intact
    /// follows it; then the damage is put back as it was.
    hole_at_end: Option<Damage>,
    /// Bytes this hole has checksummed; see [`RESYNC_CAP`].
    spent: u64,
    damage: Damage,
}

impl<R: Read> Reader<R> {
    /// A reader over `src`, which holds `len` bytes of records — the
    /// segment after its header.
    #[must_use]
    pub fn new(src: R, len: u64, mode: ReaderMode) -> Self {
        Self::with_chunk(src, len, mode, READ_CHUNK)
    }

    /// [`new`](Self::new) with the chunk size chosen, so a test can put a
    /// record boundary wherever it likes.
    #[must_use]
    pub fn with_chunk(src: R, len: u64, mode: ReaderMode, chunk: usize) -> Self {
        Self {
            src,
            len,
            remaining: len,
            buf: Vec::with_capacity(chunk.max(1)),
            at: 0,
            chunk: chunk.max(1),
            mode,
            resynchronising: false,
            hole_at_tail: None,
            hole_at_end: None,
            spent: 0,
            damage: Damage::default(),
        }
    }

    /// What was seen that was not a record, so far.
    #[must_use]
    pub const fn damage(&self) -> Damage {
        self.damage
    }

    /// The next intact record, or `None` at the segment's end.
    ///
    /// Named apart from `Iterator::next` because it is fallible and a
    /// read error is not the end of the segment's damage accounting.
    ///
    /// # Errors
    ///
    /// Whatever the source reports. A read error ends the segment; the
    /// caller decides what that costs.
    pub fn next_record(&mut self) -> io::Result<Option<Item>> {
        loop {
            if self.at >= self.buf.len() && self.remaining == 0 {
                self.end_of_segment();
                return Ok(None);
            }
            self.compact();
            let window = &self.buf[self.at..];
            let checksummed = checksummed_len(window);
            match decode_record(window) {
                Decoded::Record {
                    shard,
                    seq,
                    payload,
                    consumed,
                } => {
                    let at = self.offset();
                    let item = Item {
                        shard,
                        seq,
                        payload: payload.to_vec(),
                        at,
                        end: at + widen(consumed),
                    };
                    self.at += consumed;
                    self.resynchronising = false;
                    self.hole_at_tail = None;
                    self.hole_at_end = None;
                    self.spent = 0;
                    return Ok(Some(item));
                }
                Decoded::EndOfLog => {
                    if !self.resynchronising {
                        // A zeroed range reads as the marker too. Only
                        // looking tells them apart: step over it, and if
                        // nothing intact follows, it was the end.
                        if self.mode == ReaderMode::PrefixScan {
                            return Ok(None);
                        }
                        self.hole_at_end = Some(self.damage);
                        self.enter_hole();
                    }
                    self.step(1);
                }
                Decoded::NeedMore => {
                    if self.resynchronising {
                        self.resync_short_read()?;
                    } else if self.fill(self.chunk)? == 0 {
                        // The segment ends inside this candidate. Either a
                        // crash cut the last record short — the honest way a
                        // segment ends — or damage invented a length that
                        // runs past the end, with intact records behind it.
                        // Only looking tells them apart: step over it, and
                        // if nothing intact follows, the hole is a tail.
                        if self.mode == ReaderMode::PrefixScan {
                            self.damage.truncated_tail += widen(self.buf.len() - self.at);
                            return Ok(None);
                        }
                        self.enter_hole();
                        self.hole_at_tail = Some(self.damage.bytes);
                        self.step(1);
                    }
                }
                Decoded::Corrupt { skip } => {
                    if self.mode == ReaderMode::PrefixScan {
                        return Ok(None);
                    }
                    self.enter_hole();
                    if let Some(declared) = checksummed {
                        self.charge(declared);
                    }
                    self.step(skip);
                }
            }
            if self.damage.abandoned {
                return Ok(None);
            }
        }
    }

    /// The body offset the next decode starts at.
    fn offset(&self) -> u64 {
        self.len - self.remaining - widen(self.buf.len() - self.at)
    }

    /// Enters resynchronisation, counting a hole if this is a new one.
    const fn enter_hole(&mut self) {
        if !self.resynchronising {
            self.damage.holes += 1;
            self.resynchronising = true;
        }
    }

    /// Counts `declared` checksummed bytes against this hole, or gives up
    /// on the rest of the segment when the hole had already gone past
    /// [`RESYNC_CAP`] before this candidate.
    const fn charge(&mut self, declared: u64) {
        if self.overdrawn() {
            self.damage.abandoned = true;
        } else {
            self.spent = self.spent.saturating_add(declared);
        }
    }

    /// Whether this hole has checksummed past [`RESYNC_CAP`]: the next
    /// candidate is refused. A hole exactly at the cap is not.
    const fn overdrawn(&self) -> bool {
        self.spent > RESYNC_CAP
    }

    /// The segment ran out. A hole that began at an end marker, with
    /// nothing intact after it, was the end: its damage is undone. One that
    /// began at a candidate cut short by the segment's end was a truncated
    /// tail after all: its bytes move from damage to the tail.
    const fn end_of_segment(&mut self) {
        if let Some(before) = self.hole_at_end.take() {
            self.damage = before;
            self.hole_at_tail = None;
            return;
        }
        if let Some(start) = self.hole_at_tail.take() {
            let tail = self.damage.bytes - start;
            self.damage.bytes = start;
            self.damage.holes -= 1;
            self.damage.truncated_tail += tail;
        }
    }

    /// A short read while resynchronising: decide whether the candidate at
    /// the cursor can be completed within the segment and the cap, and
    /// either pull it in or step past it. The cap is charged when the
    /// candidate is checksummed, not here.
    fn resync_short_read(&mut self) -> io::Result<()> {
        let window = &self.buf[self.at..];
        if window.len() < HEADER_LEN {
            if self.remaining == 0 || self.fill(self.chunk)? == 0 {
                self.step(1);
            }
            return Ok(());
        }
        let declared = u64::from(u32::from_le_bytes([
            window[1], window[2], window[3], window[4],
        ]));
        let total = widen(HEADER_LEN) + declared;
        let available = widen(window.len()) + self.remaining;
        if total > available {
            // A real record cannot run past the end of its segment.
            self.step(1);
            return Ok(());
        }
        if self.overdrawn() {
            self.damage.abandoned = true;
            return Ok(());
        }
        let want = usize::try_from(total).map_or(usize::MAX, |total| total - window.len());
        self.fill(want)?;
        Ok(())
    }

    fn step(&mut self, n: usize) {
        self.at += n;
        self.damage.bytes += widen(n);
    }

    /// Pulls up to `want` more bytes into the buffer; returns how many came.
    fn fill(&mut self, want: usize) -> io::Result<usize> {
        let want = usize::try_from(widen(want).min(self.remaining)).unwrap_or(want);
        let start = self.buf.len();
        self.buf.resize(start + want, 0);
        let mut got = 0;
        while got < want {
            let n = self.src.read(&mut self.buf[start + got..])?;
            if n == 0 {
                break;
            }
            got += n;
        }
        self.buf.truncate(start + got);
        self.remaining -= widen(got);
        Ok(got)
    }

    /// Drops the consumed prefix once it is most of the buffer.
    fn compact(&mut self) {
        if self.at > 0 && self.at >= self.buf.len() / 2 {
            self.buf.drain(..self.at);
            self.at = 0;
        }
    }
}

/// The body length `decode_record` will checksum for the candidate at the
/// start of `window`, if it gets that far: a magic byte, a plausible length,
/// and the whole body present. What the resynchronisation cap is spent on.
fn checksummed_len(window: &[u8]) -> Option<u64> {
    if window.first() != Some(&MAGIC) || window.len() < HEADER_LEN {
        return None;
    }
    let declared = u32::from_le_bytes([window[1], window[2], window[3], window[4]]);
    let body = usize::try_from(declared).ok()?;
    ((BODY_FIXED_LEN..=MAX_BODY_LEN).contains(&body) && window.len() >= HEADER_LEN + body)
        .then(|| u64::from(declared))
}

/// A buffer length as a byte count. Never lossy on a supported target; the
/// saturation is there so untrusted input can never reach a panic.
fn widen(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{
        BODY_FIXED_LEN, Decoded, END_OF_LOG, HEADER_LEN, Record, decode_record, encode_record,
    };
    use std::io::Cursor;

    /// Every record a reader yields, plus the damage it saw, over `buf`
    /// read in chunks of `chunk`.
    fn scan(buf: &[u8], mode: ReaderMode, chunk: usize) -> (Vec<(u16, u64, Vec<u8>)>, Damage) {
        let len = u64::try_from(buf.len()).expect("a buffer length is a usize");
        let mut reader = Reader::with_chunk(Cursor::new(buf.to_vec()), len, mode, chunk);
        let mut items = Vec::new();
        while let Some(item) = reader.next_record().expect("a cursor never fails") {
            items.push((item.shard, item.seq, item.payload));
        }
        (items, reader.damage())
    }

    fn record(shard: u16, seq: u64, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode_record(
            &Record {
                shard,
                seq,
                payload,
            },
            &mut out,
        );
        out
    }

    #[test]
    fn whole_records_are_yielded_in_order_at_every_chunk_size() {
        let mut buf = Vec::new();
        for seq in 0..5u64 {
            buf.extend(record(3, seq, &[u8::try_from(seq).unwrap(); 7]));
        }
        buf.push(END_OF_LOG);
        for chunk in [1, 2, 7, 64, 4096] {
            let (items, damage) = scan(&buf, ReaderMode::Resynchronising, chunk);
            assert_eq!(items.len(), 5, "chunk {chunk}");
            assert_eq!(items[4], (3, 4, vec![4; 7]));
            assert_eq!(damage, Damage::default(), "chunk {chunk}");
        }
    }

    #[test]
    fn a_record_larger_than_a_chunk_is_read_whole() {
        let big = vec![0xAB; 10_000];
        let mut buf = record(1, 0, &big);
        buf.extend(record(1, 1, b"after"));
        buf.push(END_OF_LOG);
        let (items, damage) = scan(&buf, ReaderMode::Resynchronising, 64);
        assert_eq!(items[0].2, big);
        assert_eq!(items[1].2, b"after");
        assert_eq!(damage, Damage::default());
    }

    #[test]
    fn a_hole_costs_only_the_records_inside_it() {
        // The whole point of the format: a damaged record must not end the
        // read. The damaged record's `seq` field is seven zero bytes, so
        // recovery only works if resynchronisation steps over a `0x00`
        // instead of reading it as the end of the log.
        let mut buf = record(1, 1, b"lost to the hole");
        let damaged = buf.len();
        buf[HEADER_LEN + BODY_FIXED_LEN + 2] ^= 0xFF;
        assert_eq!(
            decode_record(&buf[..damaged]),
            Decoded::Corrupt { skip: 1 },
            "the damaged record must decode as damage on its own"
        );
        buf.extend(record(2, 2, b"survived"));
        buf.push(END_OF_LOG);
        let (items, damage) = scan(&buf, ReaderMode::Resynchronising, 8);
        assert_eq!(items, vec![(2, 2, b"survived".to_vec())]);
        assert_eq!(damage.holes, 1);
        assert_eq!(
            usize::try_from(damage.bytes).unwrap(),
            damaged,
            "the whole damaged record was stepped over"
        );
        assert!(!damage.abandoned);
    }

    #[test]
    fn an_item_carries_its_body_offsets_and_a_hole_lies_between_the_neighbours_ends() {
        let r0 = record(1, 0, b"before");
        let mut r1 = record(2, 0, b"lost to the hole");
        r1[HEADER_LEN + BODY_FIXED_LEN + 2] ^= 0xFF;
        let r2 = record(1, 1, b"after");
        let buf = [&r0[..], &r1, &r2].concat();
        let len = u64::try_from(buf.len()).unwrap();
        let (l0, l1) = (widen(r0.len()), widen(r1.len()));
        for chunk in [1, 2, 7, 64, 4096] {
            let mut reader = Reader::with_chunk(
                Cursor::new(buf.clone()),
                len,
                ReaderMode::Resynchronising,
                chunk,
            );
            let first = reader.next_record().unwrap().unwrap();
            assert_eq!((first.at, first.end), (0, l0), "chunk {chunk}");
            assert_eq!(reader.damage().holes, 0);
            let third = reader.next_record().unwrap().unwrap();
            assert_eq!((third.at, third.end), (l0 + l1, len), "chunk {chunk}");
            // The hole was entered at the boundary the first record ended
            // at, and ran to where the third begins.
            assert_eq!(reader.damage().holes, 1);
            assert_eq!(first.end + reader.damage().bytes, third.at);
            assert!(reader.next_record().unwrap().is_none());
        }
    }

    /// The writer never writes the end marker, so a zero where a record
    /// should start is damage when intact records follow it — a zeroed
    /// range, say — and the honest end of the segment only when nothing does.
    #[test]
    fn a_zeroed_record_is_a_hole_when_intact_records_follow_it() {
        let mut buf = record(1, 0, b"before");
        let zeroed = record(1, 1, b"zeroed by the disk");
        let at = buf.len();
        buf.extend(vec![0u8; zeroed.len()]);
        buf.extend(record(1, 2, b"after"));
        for chunk in [1, 8, 4096] {
            let (items, damage) = scan(&buf, ReaderMode::Resynchronising, chunk);
            assert_eq!(
                items,
                vec![(1, 0, b"before".to_vec()), (1, 2, b"after".to_vec())],
                "chunk {chunk}"
            );
            assert_eq!(damage.holes, 1, "chunk {chunk}");
            assert_eq!(
                usize::try_from(damage.bytes).unwrap(),
                buf.len() - at - record(1, 2, b"after").len()
            );
        }
        // Zeros to the end, with nothing intact after them: the end.
        let mut tail = record(1, 0, b"before");
        tail.extend([0u8; 40]);
        let (items, damage) = scan(&tail, ReaderMode::Resynchronising, 8);
        assert_eq!(items.len(), 1);
        assert_eq!(damage, Damage::default());
    }

    #[test]
    fn a_prefix_scan_stops_silently_at_the_first_damage() {
        let mut buf = record(1, 1, b"first");
        buf.extend(record(1, 2, b"damaged"));
        let damaged_at = buf.len() - 3;
        buf[damaged_at] ^= 0xFF;
        buf.extend(record(2, 0, b"never seen"));
        buf.push(END_OF_LOG);
        let (items, damage) = scan(&buf, ReaderMode::PrefixScan, 64);
        assert_eq!(items, vec![(1, 1, b"first".to_vec())]);
        assert_eq!(
            damage,
            Damage::default(),
            "the prefix scan does not know it lost anything"
        );
    }

    #[test]
    fn a_truncated_tail_is_an_honest_end_and_is_counted() {
        let mut buf = record(4, 0, b"whole");
        let partial = record(4, 1, b"cut");
        buf.extend(&partial[..partial.len() - 2]);
        let (items, damage) = scan(&buf, ReaderMode::Resynchronising, 64);
        assert_eq!(items.len(), 1);
        assert_eq!(damage.holes, 0);
        assert_eq!(
            usize::try_from(damage.truncated_tail).unwrap(),
            partial.len() - 2
        );
    }

    #[test]
    fn a_zero_run_at_a_trusted_boundary_is_stepped_over_when_records_follow() {
        // A torn write or a zeroed range that leaves zeroes exactly at a
        // record boundary reads as the end marker. The writer never writes
        // one, so the reader looks past it: records after the run are
        // recovered and the run is a hole; a run with nothing intact after
        // it is the end, and costs nothing.
        let mut buf = vec![0u8; 24];
        buf.extend(record(4, 8, b"after the hole"));
        buf.push(END_OF_LOG);
        assert_eq!(decode_record(&buf), Decoded::EndOfLog);
        let (items, damage) = scan(&buf, ReaderMode::Resynchronising, 64);
        assert_eq!(items, vec![(4, 8, b"after the hole".to_vec())]);
        assert_eq!((damage.holes, damage.bytes), (1, 24));
    }

    #[test]
    fn a_candidate_past_the_end_of_the_segment_is_not_checksummed() {
        // Damage that leaves a plausible header claiming a body longer than
        // the file: the reader must step past it rather than wait, and then
        // find the intact record that follows.
        let mut buf = vec![crate::log::MAGIC, 0xFF, 0xFF, 0x00, 0x00, 1, 2, 3, 4];
        buf.extend(record(9, 9, b"found"));
        buf.push(END_OF_LOG);
        let (items, damage) = scan(&buf, ReaderMode::Resynchronising, 4);
        assert_eq!(items, vec![(9, 9, b"found".to_vec())]);
        assert_eq!(damage.holes, 1);
    }

    /// `holes` candidates of a hole, each a plausible header whose 1 KiB
    /// body is present and fails its checksum, then a record too long for
    /// one chunk and a short one: the long one is read with a short read,
    /// which is where the budget is consulted before it is checksummed.
    fn after_a_hole(candidates: usize, chunk: usize) -> (Vec<(u16, u64, Vec<u8>)>, Damage) {
        let claimed = 1024u32;
        let mut buf = Vec::new();
        for _ in 0..candidates {
            buf.push(crate::log::MAGIC);
            buf.extend_from_slice(&claimed.to_le_bytes());
            buf.extend_from_slice(&[0u8; 4]);
        }
        buf.extend(vec![0x5A; 2048]);
        buf.extend(record(1, 0, &[0xAB; 10_000]));
        buf.extend(record(1, 1, b"after"));
        buf.push(END_OF_LOG);
        scan(&buf, ReaderMode::Resynchronising, chunk)
    }

    /// Candidates of 1 KiB that spend exactly the cap.
    fn at_the_cap() -> usize {
        usize::try_from(RESYNC_CAP / 1024).unwrap()
    }

    #[test]
    fn a_record_that_straddles_the_cap_is_checksummed() {
        // The hole leaves a kilobyte of budget and the record after it
        // declares ten: it begins inside the budget, so it is tried — it is
        // where the hole ends — and every record after it is kept.
        for chunk in [64, 4096] {
            let (items, damage) = after_a_hole(at_the_cap() - 1, chunk);
            assert_eq!(items.len(), 2, "chunk {chunk}");
            assert_eq!(items[1], (1, 1, b"after".to_vec()), "chunk {chunk}");
            assert_eq!(
                (damage.holes, damage.abandoned),
                (1, false),
                "chunk {chunk}"
            );
        }
    }

    #[test]
    fn a_hole_that_spends_exactly_the_cap_keeps_the_record_after_it() {
        for chunk in [64, 4096] {
            let (items, damage) = after_a_hole(at_the_cap(), chunk);
            assert_eq!(items.len(), 2, "chunk {chunk}");
            assert_eq!(
                (damage.holes, damage.abandoned),
                (1, false),
                "chunk {chunk}"
            );
        }
    }

    #[test]
    fn resynchronisation_gives_up_on_a_segment_past_its_budget() {
        // One candidate past the cap: it begins at the cap, so it is
        // checksummed and overdraws the hole, and the next one — the intact
        // record — is refused with the rest of the segment, counted.
        for chunk in [64, 4096] {
            let (items, damage) = after_a_hole(at_the_cap() + 1, chunk);
            assert!(items.is_empty(), "chunk {chunk}");
            assert!(damage.abandoned, "chunk {chunk}");
        }
    }
}
