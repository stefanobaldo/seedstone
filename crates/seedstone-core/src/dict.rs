//! The per-shard keyspace: a chained hash table with seeded hashing and
//! incremental rehashing.
//!
//! This is a hand-written table rather than `std::collections::HashMap`
//! because the store must be replayable: both the hash of a key and the order
//! in which the table hands its entries back have to be a pure function of the
//! seed and the sequence of operations, identical across processes, builds and
//! platforms. `HashMap` guarantees the opposite — its default hasher is seeded
//! from OS entropy and its layout is an implementation detail.
//!
//! Growing is incremental for the same reason a shard is single-threaded:
//! nothing may stall the shard task. Instead of rebuilding the whole table in
//! one pass, a growth allocates a second table and the entries migrate a
//! bucket at a time, driven by ordinary traffic and by the shard runtime
//! calling [`Dict::rehash_step`] on a timer. While that is happening the dict
//! holds two tables at once and every read has to consult both.

use std::hash::Hasher;

use crate::shard::ExpiryPolicy;
use siphasher::sip::SipHasher13;
use tokio::time::Instant;

/// The SipHash key pair that fixes a dict's hashing.
///
/// The seed is an input to the run, not a property of the machine: two nodes
/// replaying the same seed place the same key in the same bucket. It is
/// carried as a value so a simulation can hand every shard a seed derived
/// from the run's single root seed.
#[derive(Clone, Copy, Debug)]
pub struct DictSeed {
    /// First SipHash key.
    pub k0: u64,
    /// Second SipHash key.
    pub k1: u64,
}

/// What a key maps to: the bytes stored under it and, if it has one, the
/// instant it stops being visible at.
///
/// The deadline is absolute, and it is a [`tokio::time::Instant`] rather than
/// a wall-clock reading: under the simulator that clock is virtual, so a
/// deadline is a point in the *run* and a replay reaches it at the same point.
/// The relative form a client sends is resolved against `now` before it gets
/// here.
///
/// The dict compares the deadline to a clock in exactly one place —
/// [`Dict::expire_step`], which walks the table on the shard's tick looking for
/// entries nothing will ever ask for again. It acts on none of them: what that
/// walk produces is a list of keys, and removing them stays the shard's, which
/// is the only layer that can log the removal an expiry amounts to. Everywhere
/// else the dict stores this field and hands it back untouched.
///
/// # What it costs
///
/// The deadline is stored inline on every entry, whether or not that entry has
/// one, and an `Option<Instant>` is 16 bytes — `Instant` is 16 and the niche
/// absorbs the discriminant, so `None` is not cheaper. A bucket slot,
/// `(u64, Vec<u8>, Entry)` — see `Bucket` — is therefore 80 bytes against
/// the 48 a plain `(Vec<u8>, Vec<u8>)` would take: 8 for the stored hash,
/// which is a deliberate trade of space for a cheaper chain scan, 16 for this
/// field, which the great majority of keys in a real keyspace never use, and
/// 8 more for the four-byte [`touched`](Self::touched) stamp and the padding
/// that rounds it up. `the_slot_layout_is_what_entry_overhead_prices` holds
/// that figure to what the compiler actually lays out.
///
/// Stated here so nobody has to derive it from the layout. Whether it is the
/// right trade — against a side table of deadlines, or a packed representation
/// — is a design question, and this doc is not where it gets settled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The bytes stored under the key, kept verbatim.
    pub value: Vec<u8>,
    /// When the entry expires, or `None` if it never does.
    pub expires_at: Option<Instant>,
    /// When the entry was last touched, on the dict's own command counter.
    ///
    /// A counter and not a clock, so that which key is oldest is a function
    /// of the command sequence and replays identically; the resolution is one
    /// command, which is finer than a clock's. Set by [`Dict::touch`], which
    /// the read and write paths call; `0` on an entry nothing has touched yet.
    pub touched: u32,
}

/// One hash bucket: the entries whose hash selected it, in insertion order.
///
/// Collisions are resolved by chaining. The load factor is held at 1, so a
/// bucket holds one entry on average and the linear scan below is cheap.
///
/// Each element carries the key's hash beside the key and its entry. It is the
/// same word [`bucket_index`] selected the bucket with, stored rather than
/// recomputed, and it buys two things: a chain scan settles a miss on one
/// integer comparison instead of a full key comparison, and [`Dict::rehash_step`]
/// migrates an entry without a second pass through SipHash. It moves
/// nothing — the hash stored is the hash placement already used, so every key
/// lands where it always did.
type Bucket = Vec<(u64, Vec<u8>, Entry)>;

/// A table of buckets. Its length is always a power of two.
///
/// The power-of-two invariant is not just for the cheap mask in
/// [`bucket_index`]: a cursor-based scan over a table that grows underneath it
/// depends on the new table being exactly twice the old one, so that a key's
/// bucket index in the old table is the low bits of its index in the new one.
type Table = Vec<Bucket>;

/// Bucket count of a freshly seeded dict.
///
/// Small on purpose: a node holds many shards and most of them may hold
/// nothing at all, so an empty dict should cost close to nothing. The table
/// grows on demand.
const INITIAL_BUCKETS: usize = 8;

/// What one entry costs beyond its key and value bytes.
///
/// A bucket slot is `(u64, Vec<u8>, Entry)` — 8 for the stored hash, 24 for
/// the key's `Vec` header, and `Entry` at 48 (a 24-byte `Vec` header for the
/// value, a 16-byte `Option<Instant>`, and the 4-byte LRU stamp padded to 8)
/// — 80 bytes, as the [`Entry`] doc derives.
/// The two heap allocations the headers point at are counted through their
/// lengths by [`entry_bytes`]; their allocator rounding is not, and that is
/// deliberate: this is a formula the simulator can replay, not a reading.
pub const ENTRY_OVERHEAD: u64 = 80;

/// What one bucket costs when empty: its `Vec` header. A chain's slots are
/// counted per entry above.
pub const BUCKET_OVERHEAD: u64 = 24;

/// The bytes one entry is accounted at.
///
/// # Panics
///
/// If a `usize` does not fit a `u64`, which no target this builds for has.
/// The conversion is spelled fallibly rather than with a cast because the
/// coding guide admits no bare `as` between integer widths.
#[must_use]
pub fn entry_bytes(key: &[u8], value: &[u8]) -> u64 {
    ENTRY_OVERHEAD
        + u64::try_from(key.len()).expect("a key length is a usize")
        + u64::try_from(value.len()).expect("a value length is a usize")
}

/// What a table of `buckets` buckets is accounted at, empty.
fn table_bytes(buckets: usize) -> u64 {
    u64::try_from(buckets).expect("a bucket count is a usize") * BUCKET_OVERHEAD
}

/// A keyspace: byte-string keys mapped to byte-string values.
///
/// One shard task owns one `Dict` and is the only thing that touches it, so
/// there is no locking here and none is needed.
pub struct Dict {
    seed: DictSeed,
    /// The table entries are read from and, when not rehashing, written to.
    old: Table,
    /// The larger table a rehash is migrating into. `Some` exactly while a
    /// rehash is in flight.
    new: Option<Table>,
    /// Index of the next `old` bucket to migrate. Buckets below it have
    /// already been drained into `new`; meaningless when `new` is `None`.
    rehash_index: usize,
    len: usize,
    /// Whether any entry in either table may carry a deadline.
    ///
    /// Deliberately conservative: it is set by every operation that can put a
    /// deadline into the dict and cleared only where no entry can be carrying
    /// one — when the dict empties. A dict that held a deadline and then lost
    /// it keeps saying `true` until it drains, so the flag can be wrong in one
    /// direction only, the direction that costs a lookup rather than the one
    /// that misses an expiry.
    ///
    /// The point of it is [`Dict::may_hold_deadlines`]: a keyspace with no
    /// deadlines anywhere — which is nearly every keyspace — must not pay a
    /// hash per command for the expiries it does not have.
    may_hold_deadlines: bool,
    /// What this dict is accounted at, maintained by every operation that can
    /// move it rather than derived on demand.
    ///
    /// A running figure because the alternative is a walk of the keyspace per
    /// read, and the reader is the memory gauge every write consults. The
    /// tests hold it to a full recount after every mutation, which is what
    /// makes maintaining it in five places defensible.
    used_bytes: u64,
    /// How many entries currently carry a deadline.
    ///
    /// Exact, unlike [`may_hold_deadlines`](Self::may_hold_deadlines) beside
    /// it, and the two are not the same question: that flag is a fast path and
    /// is allowed to be conservative, while this is a figure `INFO`'s
    /// `# Keyspace` section prints as `expires=`, and a conservative count is
    /// a wrong one. Maintained by the four operations that can move it —
    /// insert, overwrite, `set_deadline`, remove — for the reason
    /// [`used_bytes`](Self::used_bytes) is maintained rather than derived: the
    /// alternative is a walk of the keyspace per scrape.
    with_deadline: usize,
    /// What [`Dict::touch`] stamps entries with, advanced once per touch.
    ///
    /// A command counter rather than a clock reading, for the reason
    /// [`Entry::touched`] states: the order two keys were last used in has to
    /// be a function of the command sequence, so that a replay of that
    /// sequence picks the same eviction victim.
    clock: u32,
}

impl Dict {
    /// Creates an empty dict whose hashing is fixed by `seed`.
    #[must_use]
    pub fn with_seed(seed: DictSeed) -> Self {
        Self {
            seed,
            old: empty_table(INITIAL_BUCKETS),
            new: None,
            rehash_index: 0,
            len: 0,
            may_hold_deadlines: false,
            used_bytes: table_bytes(INITIAL_BUCKETS),
            with_deadline: 0,
            clock: 0,
        }
    }

    /// Returns the entry stored under `key`, or `None` if there is none.
    ///
    /// While a rehash is in flight the key may live in either table, so both
    /// are probed. A lookup never advances the rehash: reads are on the hot
    /// path and must not pay for a migration.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        let hash = hash_key(self.seed, key);
        if let Some(entry) = find(&self.old, hash, key) {
            return Some(entry);
        }
        find(self.new.as_ref()?, hash, key)
    }

    /// Whether any entry may carry a deadline.
    ///
    /// A `false` is a guarantee — no entry in this dict has one, so nothing
    /// can be expired and a caller may skip its liveness check entirely, which
    /// is what keeps a keyspace without expiries from paying a hash per
    /// command for them. A `true` is only "maybe": see the field.
    #[must_use]
    pub const fn may_hold_deadlines(&self) -> bool {
        self.may_hold_deadlines
    }

    /// Replaces the deadline on `key`, leaving its value where it is.
    /// Returns whether the key was there.
    ///
    /// This is the *only* way to change a deadline on an entry the dict is
    /// already holding, and that is the point: handing out a `&mut Entry`
    /// instead would put [`may_hold_deadlines`](Dict::may_hold_deadlines) in
    /// the hands of every caller that ever borrows one, and a flag maintained
    /// by convention is a flag that goes wrong the first time someone forgets.
    /// With this, the two places a deadline can enter a dict are both inside
    /// this type.
    ///
    /// Like [`get`](Dict::get) it does not advance a rehash in flight: nothing
    /// was added, and the migration is paid for by the writes that grow the
    /// table.
    pub fn set_deadline(&mut self, key: &[u8], expires_at: Option<Instant>) -> bool {
        self.may_hold_deadlines |= expires_at.is_some();
        let hash = hash_key(self.seed, key);
        let entry = match find_mut(&mut self.old, hash, key) {
            Some(entry) => entry,
            None => match self.new.as_mut().and_then(|new| find_mut(new, hash, key)) {
                Some(entry) => entry,
                None => return false,
            },
        };
        // The delta before the write, since the field is about to be replaced.
        self.with_deadline = self.with_deadline + usize::from(expires_at.is_some())
            - usize::from(entry.expires_at.is_some());
        entry.expires_at = expires_at;
        true
    }

    /// Stamps `key` as touched now and advances the counter.
    ///
    /// Wrapping: after four billion touches the stamps fold over and a key
    /// touched just after the fold reads as older than one touched just
    /// before it. The sampled comparison tolerates it — a victim chosen wrong
    /// for the span of one wrap is a cache miss, not a correctness failure —
    /// and a monotone 64-bit stamp would cost every entry eight bytes to avoid
    /// a miss nobody could measure.
    ///
    /// A key that is not there is not an error: a read that missed and a
    /// write that has not landed yet both reach this, and neither has an
    /// entry to stamp.
    pub fn touch(&mut self, key: &[u8]) {
        self.clock = self.clock.wrapping_add(1);
        let stamp = self.clock;
        let hash = hash_key(self.seed, key);
        if let Some(entry) = find_mut(&mut self.old, hash, key) {
            entry.touched = stamp;
        } else if let Some(new) = self.new.as_mut()
            && let Some(entry) = find_mut(new, hash, key)
        {
            entry.touched = stamp;
        }
    }

    /// The least recently touched of up to `samples` *stored* entries met by
    /// walking from `cursor`, or `None` if the dict is empty.
    ///
    /// **Stored, not live.** This dict holds no clock and the walk is handed
    /// none, so an entry whose deadline has passed and which nothing has
    /// reclaimed yet is met, counted against `samples`, and may be returned as
    /// the victim. That is harmless where this is called from — a key that was
    /// going to be reclaimed anyway is a good one to take, and taking it frees
    /// the bytes the caller was asking for — but it means `samples` bounds the
    /// entries *examined* rather than the live candidates considered, and on a
    /// keyspace thick with expired keys the two numbers are far apart.
    ///
    /// A walk rather than a random draw: the walk order is a function of the
    /// seed and the cursor, so an eviction replays, and the cursor the caller
    /// keeps means successive samples cover the table instead of re-reading
    /// one corner of it. It takes as many steps as it needs to meet
    /// `samples` entries, bounded by one full cycle, so a sparse table does
    /// not answer from a single bucket.
    ///
    /// The comparison is a plain `<` on stamps that wrap, so the fold
    /// [`touch`](Dict::touch) documents makes one sample's worth of victims
    /// misjudged every four billion touches. A wrapping comparison would
    /// misjudge a different set — the one where the sample straddles the fold
    /// in the other direction — rather than none, and neither costs anything
    /// but a cache miss.
    ///
    /// `spared` is a key the caller will not accept as the answer. It is
    /// skipped where it is met rather than filtered out of the result, so
    /// meeting it costs the caller nothing: the walk goes on, `samples` is not
    /// charged for it, and `None` still means the dict had no other entry to
    /// offer. Filtering afterwards would answer `None` from a sample that
    /// merely happened to meet the spared key first.
    #[must_use]
    pub fn sample_oldest(
        &self,
        cursor: &mut u64,
        samples: usize,
        spared: Option<&[u8]>,
    ) -> Option<Vec<u8>> {
        if self.is_empty() {
            return None;
        }
        let mut oldest: Option<(u32, Vec<u8>)> = None;
        let mut seen = 0usize;
        let mut steps = 0usize;
        // One full cycle of the walk, in the widest table there is: the bound
        // is what keeps a sparse keyspace — many buckets, few entries — from
        // spinning here when it cannot produce `samples` at all.
        let bound = self.old.len() + self.new.as_ref().map_or(0, Vec::len);
        while seen < samples && steps < bound {
            *cursor = self.scan(*cursor, |key, entry| {
                // The spared key is excluded from candidacy rather than
                // ending the walk, so `None` keeps meaning "this shard has
                // nothing to give". It is not counted against `samples`
                // either: a sample that met only the spared key met nothing,
                // and charging it would shrink the real sample by one.
                if spared == Some(key) {
                    return;
                }
                seen += 1;
                // The key is cloned only when it becomes the candidate, not
                // once per entry met: a sample of five over a chained bucket
                // would otherwise allocate for every entry it discards.
                if oldest
                    .as_ref()
                    .is_none_or(|(stamp, _)| entry.touched < *stamp)
                {
                    oldest = Some((entry.touched, key.to_vec()));
                }
            });
            steps += 1;
        }
        oldest.map(|(_, key)| key)
    }

    /// Stores `entry` under `key`, replacing any entry already there.
    ///
    /// Overwriting an existing key leaves it where it is — including in the
    /// old table mid-rehash, from where it will migrate like any other entry.
    /// Moving it would risk leaving a stale copy behind, and the entry is
    /// reachable either way.
    pub fn insert(&mut self, key: Vec<u8>, entry: Entry) {
        // A write pays for one bucket of the migration it is competing with,
        // which is what keeps the two tables from coexisting indefinitely.
        //
        // The growth test is `>=`, and that is load-bearing — do not
        // simplify it back to `==`. The insert that starts a rehash does not
        // itself migrate a bucket, so draining an N-bucket table takes N
        // *further* writes, by which point `len` is N + 1 + N = 2N + 1 while
        // the surviving table has 2N buckets. The threshold is stepped over,
        // never landed on, and `len` only grows: with `==` the table would
        // never grow a second time and every lookup would decay into a linear
        // scan of one enormous bucket chain.
        if self.is_rehashing() {
            self.rehash_step(1);
        } else if self.len >= self.old.len() {
            self.start_rehash();
        }

        // One of the two ways a deadline enters a dict; the other is
        // [`set_deadline`](Dict::set_deadline). Both are in this file, which is
        // what makes the flag's accounting complete rather than a rule callers
        // have to follow.
        self.may_hold_deadlines |= entry.expires_at.is_some();

        let hash = hash_key(self.seed, &key);
        // The new size is computed before the move, since `entry` goes into
        // the table and the old entry's is read off what is still there.
        let after = entry_bytes(&key, &entry.value);
        let dated = usize::from(entry.expires_at.is_some());
        if let Some(existing) = find_mut(&mut self.old, hash, &key) {
            self.used_bytes = self.used_bytes - entry_bytes(&key, &existing.value) + after;
            self.with_deadline =
                self.with_deadline + dated - usize::from(existing.expires_at.is_some());
            *existing = entry;
            return;
        }
        if let Some(new) = self.new.as_mut()
            && let Some(existing) = find_mut(new, hash, &key)
        {
            self.used_bytes = self.used_bytes - entry_bytes(&key, &existing.value) + after;
            self.with_deadline =
                self.with_deadline + dated - usize::from(existing.expires_at.is_some());
            *existing = entry;
            return;
        }

        // A key that is not present yet goes straight into the table that will
        // survive the rehash, so it never has to be migrated.
        self.used_bytes += after;
        self.with_deadline += dated;
        let table = self.new.as_mut().unwrap_or(&mut self.old);
        let index = bucket_index(hash, table.len());
        table[index].push((hash, key, entry));
        self.len += 1;
    }

    /// Removes `key` and returns the entry it held, or `None` if it was absent.
    pub fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        if self.is_rehashing() {
            self.rehash_step(1);
        }

        let hash = hash_key(self.seed, key);
        let removed = remove_from(&mut self.old, hash, key).or_else(|| {
            let new = self.new.as_mut()?;
            remove_from(new, hash, key)
        });
        if let Some(removed) = &removed {
            self.used_bytes -= entry_bytes(key, &removed.value);
            self.with_deadline -= usize::from(removed.expires_at.is_some());
            self.len -= 1;
            // The one place the flag can honestly go back down: a dict holding
            // nothing holds no deadlines. Everything short of empty stays
            // "maybe", because finding out would cost a scan of the keyspace
            // to save a lookup.
            if self.len == 0 {
                self.may_hold_deadlines = false;
            }
        }
        removed
    }

    /// Drops every entry, leaving the dict as [`with_seed`](Dict::with_seed)
    /// built it.
    ///
    /// The seed survives and nothing else does: the same key placed again
    /// lands in the same bucket it always did, which is what keeps a replayed
    /// run identical across a flush. The table goes back to
    /// `INITIAL_BUCKETS` rather than keeping the capacity it had earned, and
    /// an in-flight rehash is abandoned — a dict that has just been emptied
    /// costing close to nothing is the whole point of the operation, and the
    /// growth path is there to earn the capacity back.
    ///
    /// A scan cursor held elsewhere is not invalidated by this. It is a
    /// position in a cycle, not a pointer: a walk resuming from one after a
    /// flush starts partway through a table that is empty, which
    /// [`scan`](Dict::scan) ends immediately, exactly as it does for any other
    /// emptied keyspace.
    pub fn clear(&mut self) {
        *self = Self::with_seed(self.seed);
    }

    /// Number of entries, counting both tables while a rehash is in flight.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// How many of those entries carry a deadline — `INFO`'s `expires=`.
    ///
    /// Counted, not sampled, and it counts entries whose deadline has already
    /// passed but which neither half of expiration has reached yet, for the
    /// reason [`len`](Self::len) counts them: they are still in the table, and
    /// a figure that excluded them would have to walk the keyspace to find out.
    #[must_use]
    pub const fn with_deadline(&self) -> usize {
        self.with_deadline
    }

    /// The bytes this dict is accounted at: every entry through
    /// [`entry_bytes`], plus [`BUCKET_OVERHEAD`] per bucket of every table it
    /// currently holds — two while a rehash is in flight.
    #[must_use]
    pub const fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    /// Whether the dict holds no entries.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether a rehash is in flight, i.e. the dict currently holds two tables.
    #[must_use]
    pub const fn is_rehashing(&self) -> bool {
        self.new.is_some()
    }

    /// Migrates up to `buckets` further old buckets into the new table.
    ///
    /// A no-op when no rehash is in flight, and it stops at the end of the old
    /// table rather than running past it, so a caller may pass a batch size
    /// larger than what is left. Empty buckets count against `buckets`: the
    /// point of the argument is to bound the work this call does, and skipping
    /// empties would make an unlucky call arbitrarily long.
    ///
    /// The shard runtime calls this on a timer so that a table that has stopped
    /// receiving traffic still finishes its rehash instead of holding two
    /// tables forever.
    pub fn rehash_step(&mut self, buckets: usize) {
        let Some(new) = self.new.as_mut() else {
            return;
        };

        let end = self
            .rehash_index
            .saturating_add(buckets)
            .min(self.old.len());
        for index in self.rehash_index..end {
            for slot in self.old[index].drain(..) {
                // The hash the entry was placed with, carried since that
                // placement: a migration is a re-bucketing and never a
                // rehashing, so a doubled table costs no SipHash at all.
                let target = bucket_index(slot.0, new.len());
                new[target].push(slot);
            }
        }
        self.rehash_index = end;

        // Taking `new` rather than unwrapping it is what keeps this path
        // free of a panic the borrow checker would otherwise force: the
        // table is known to be there, and `take` says so without asserting it.
        if self.rehash_index == self.old.len()
            && let Some(migrated) = self.new.take()
        {
            // The drained table is dropped here, and stops being accounted.
            self.used_bytes -= table_bytes(self.old.len());
            self.old = migrated;
            self.rehash_index = 0;
        }
    }

    /// Visits one step's worth of entries and returns the cursor for the next
    /// call.
    ///
    /// This is the only traversal this type offers, and it is deliberately not
    /// an iterator. A `SCAN` command has to hand its position back to a client
    /// between calls and resume from it later, with the dict mutating in
    /// between — a borrow-holding iterator cannot express that, and neither can
    /// anything whose order depends on more than the seed and the operation
    /// sequence.
    ///
    /// # Contract
    ///
    /// - A full cycle starts at cursor `0` and ends when a call returns `0`.
    ///   Any other return value is opaque: it must be passed back unchanged.
    /// - A key that is present for the whole cycle is visited **at least
    ///   once**.
    /// - A key may be visited **more than once**, so a caller that needs
    ///   distinct keys has to deduplicate.
    /// - A key added or removed part-way through a cycle may or may not be
    ///   visited. No guarantee either way.
    /// - A single call visits a bounded number of *buckets* — one bucket of the
    ///   smaller table, plus the two buckets of the larger one it expands into
    ///   — which is what keeps a step short enough for a shard to stay
    ///   responsive.
    /// - A single call visits an **unbounded number of entries**: a bucket is a
    ///   chain and nothing here caps its length. A caller that turns a step into
    ///   a reply has to bound what it serialises itself rather than assume a
    ///   step is small.
    /// - An empty dict ends the cycle immediately, whatever cursor it is given.
    ///
    /// # Algorithm
    ///
    /// The cursor is incremented in *reverse binary* order: the bucket bits are
    /// reversed, one is added, and the result is reversed back, so the carry
    /// propagates from the high bit of the bucket index downwards. What that
    /// buys under a table that doubles is a cycle that still *ends*. A step
    /// under a mask of `m` moves the cursor forward by exactly `1 / (m + 1)` of
    /// the keyspace, so a doubling halves the size of every later step instead
    /// of doubling the number of steps left, and a whole cycle costs no more
    /// calls than the table it ends on has buckets — however much the table
    /// grew along the way. A cursor that simply counted buckets upwards would
    /// advance one bucket per call, and a keyspace doubling faster than that
    /// would outrun it: the cursor would chase the mask and never come back to
    /// `0`. The same order is what would keep the guarantee if a table ever
    /// halved, where two buckets merge into one the cursor may already have
    /// passed; nothing shrinks a table today, and this costs nothing.
    ///
    /// The bits above the mask take part in the arithmetic — the cursor is
    /// widened to all ones outside the mask before the increment, so the carry
    /// runs out of the masked region and off the top of the reversed word. That
    /// is what makes the cycle end on exactly `0`, and it is why the cursor
    /// stays a full `u64` rather than being narrowed to the table's width.
    ///
    /// While a rehash is in flight the dict holds two tables and an entry may
    /// be in either, so a step visits the smaller table's bucket for the cursor
    /// and then every bucket of the larger table that bucket expands into,
    /// which is exactly the cursors sharing its low bits. The loop ends when
    /// the increment carries back into the bits the smaller mask covers.
    pub fn scan<F: FnMut(&[u8], &Entry)>(&self, cursor: u64, visit: F) -> u64 {
        self.scan_in_order(cursor, &ReverseBinary, visit)
    }

    /// [`scan`](Dict::scan), with the cursor's advance supplied.
    ///
    /// Every guarantee above is a property of [`ReverseBinary`], which is what
    /// [`scan`](Dict::scan) passes and the only order this crate ships. See
    /// [`WalkOrder`] for why the parameter exists at all; a caller that wants
    /// the guarantees wants [`scan`](Dict::scan).
    pub fn scan_in_order<O: WalkOrder, F: FnMut(&[u8], &Entry)>(
        &self,
        cursor: u64,
        order: &O,
        mut visit: F,
    ) -> u64 {
        // Nothing to hand back and nothing to come back for. Redis does the
        // same, and it is what lets a caller sweep an empty keyspace in one
        // call instead of one per bucket. It cannot weaken the guarantee: if
        // the dict is empty at any point of a cycle, no key was present for the
        // whole of it.
        if self.is_empty() {
            return 0;
        }

        let Some(new) = self.new.as_ref() else {
            visit_bucket(&self.old, cursor, &mut visit);
            return order.advance(cursor, mask_of(&self.old));
        };

        // `new` is allocated at exactly twice `old`, so `old` is always the
        // smaller of the two and `small ^ large` is exactly the bits the
        // doubling added. That coupling lives in `start_rehash` and is
        // invisible from here, and breaking it would not fault: with
        // `large <= small` the xor loses the discriminating bit, the loop below
        // returns after a single `new` bucket, and the scan quietly
        // under-visits for the rest of the cycle.
        debug_assert!(
            self.old.len() < new.len(),
            "scan assumes old is the smaller table"
        );
        let small = mask_of(&self.old);
        let large = mask_of(new);

        let mut v = cursor;
        visit_bucket(&self.old, v, &mut visit);
        loop {
            visit_bucket(new, v, &mut visit);
            v = order.advance(v, large);
            if v & (small ^ large) == 0 {
                return v;
            }
        }
    }

    /// Reports the keys whose deadline has passed in the next stretch of the
    /// cursor's cycle, and returns where the following call resumes.
    ///
    /// This is the active half of expiration. Lazily evicting a key when a
    /// command next touches it reclaims nothing from a key no command ever
    /// touches again, and a keyspace whose writers have moved on is exactly
    /// where the dead entries pile up. Walking the table on a timer is what
    /// bounds that, and the budget is what keeps the walk from becoming a stall.
    ///
    /// # Contract
    ///
    /// - Reporting is all it does. The entries are still there when it
    ///   returns, and removing them is the caller's — which is what lets the
    ///   shard write each removal's replication record *before* the removal,
    ///   as it does for every other mutation.
    /// - The cursor is [`scan`](Dict::scan)'s, advanced the same way and
    ///   carrying the same guarantees: a cycle starts at `0` and ends when a
    ///   call returns `0`, and a key present throughout one is reported at
    ///   least once whatever the table does underneath.
    /// - `budget_buckets` bounds the work: the call takes at most that many
    ///   cursor steps, each covering one bucket of the smaller table and,
    ///   while a rehash is in flight, the two buckets of the larger one it
    ///   expands into. It stops early when the cycle ends, never running past
    ///   `0` into a second lap.
    /// - A dict that can hold no deadline is not walked at all: the cursor
    ///   comes back unchanged and nothing is reported. See
    ///   [`may_hold_deadlines`](Dict::may_hold_deadlines) — it is what keeps
    ///   this off the tick of the shards, nearly all of them, that have no
    ///   expiries to reclaim. The shortcut is `expiry`'s to waive: a policy
    ///   answering
    ///   [`takes_undated`](crate::shard::ExpiryPolicy::takes_undated) yes is
    ///   looking for keys the flag says nothing about, so the walk happens.
    /// - **The report is a snapshot, and it stops being true the moment the
    ///   dict is written to.** Each key named was due at `now`; an insert
    ///   under that same key afterwards replaces the entry with a live one,
    ///   and removing it on the strength of this list would then destroy data
    ///   no deadline had reached. Removals do not carry that hazard — one
    ///   cannot resurrect another key — which is why the sweep may work
    ///   through the list a removal at a time. A caller that writes anything
    ///   else in between must re-check. The borrow does not enforce this: the
    ///   keys come back owned and outlive the `&self` that produced them, so
    ///   it is a contract rather than a lifetime.
    ///
    /// Which keys are due is `expiry`'s to say, and it is asked the same
    /// question the lazy path asks — under the honest
    /// [`Deadlines`](crate::shard::Deadlines) a key is expired once `now` has
    /// *reached* its deadline. The two halves consult one policy so they cannot
    /// disagree about which keys are alive.
    #[must_use]
    pub fn expire_step(
        &self,
        cursor: u64,
        budget_buckets: usize,
        now: Instant,
        expiry: &impl ExpiryPolicy,
    ) -> (u64, Vec<Vec<u8>>) {
        if !self.may_hold_deadlines && !expiry.takes_undated() {
            return (cursor, Vec::new());
        }

        let mut dead = Vec::new();
        let mut next = cursor;
        for _ in 0..budget_buckets {
            // Nothing mutates the table between these steps, so the buckets
            // they cover are disjoint and no key can be reported twice.
            next = self.scan(next, |key, entry| {
                if expiry.due_on_sweep(entry.expires_at, now) {
                    dead.push(key.to_vec());
                }
            });
            if next == 0 {
                break;
            }
        }
        (next, dead)
    }

    /// Allocates the new table and puts the dict into the rehashing state.
    ///
    /// Growth is triggered at a load factor of 1 — one entry per bucket on
    /// average. That threshold is what makes the amortization work: migrating
    /// one bucket per write completes the rehash of an N-bucket table within
    /// the N further inserts it takes to reach the next threshold.
    fn start_rehash(&mut self) {
        // Exactly twice the old size, which both keeps the power-of-two
        // invariant and is what a cursor-based scan across the two tables
        // relies on.
        self.new = Some(empty_table(self.old.len() * 2));
        // The second table's buckets are held alongside the first's until the
        // migration ends, and are accounted for the whole time they exist.
        self.used_bytes += table_bytes(self.old.len() * 2);
        self.rehash_index = 0;
    }
}

/// Builds a table of `buckets` empty buckets.
///
/// # Preconditions
///
/// `buckets` must be a power of two. Every caller derives it from
/// [`INITIAL_BUCKETS`] by doubling, so violating it is a programming error in
/// this module and is checked with a `debug_assert!`.
fn empty_table(buckets: usize) -> Table {
    debug_assert!(
        buckets.is_power_of_two(),
        "empty_table: bucket count must be a power of two"
    );
    vec![Vec::new(); buckets]
}

/// Hashes `key` under `seed`.
///
/// A free function taking the seed by value rather than a method: a hash is a
/// property of the seed and the key alone, and every caller below computes one
/// while it is about to borrow a table.
fn hash_key(seed: DictSeed, key: &[u8]) -> u64 {
    let mut hasher = SipHasher13::new_with_keys(seed.k0, seed.k1);
    hasher.write(key);
    hasher.finish()
}

/// Selects a bucket in a table of `buckets` buckets.
///
/// A mask rather than a remainder, which is what the power-of-two invariant
/// buys, and which also makes the old index the low bits of the new one when
/// the table doubles.
fn bucket_index(hash: u64, buckets: usize) -> usize {
    let mask = u64::try_from(buckets).expect("a bucket count is a usize") - 1;
    let masked = hash & mask;
    usize::try_from(masked).expect("a masked hash is below the bucket count, which is a usize")
}

/// The bucket-selecting mask of a table: its length minus one, which is a run
/// of low bits because the length is a power of two.
fn mask_of(table: &Table) -> u64 {
    debug_assert!(!table.is_empty(), "mask_of: a table is never empty");
    u64::try_from(table.len()).expect("a table length is a usize") - 1
}

/// Hands every entry of the bucket `cursor` selects in `table` to `visit`.
fn visit_bucket<F: FnMut(&[u8], &Entry)>(table: &Table, cursor: u64, visit: &mut F) {
    let index = usize::try_from(cursor & mask_of(table))
        .expect("a masked cursor is below the bucket count, which is a usize");
    for (_, key, entry) in &table[index] {
        visit(key, entry);
    }
}

/// Advances a scan cursor one step in reverse binary order under `mask`.
///
/// See [`Dict::scan`] for why the bits outside the mask are set first: the
/// increment has to carry off the top of the reversed word so that a completed
/// cycle lands back on exactly `0`.
const fn reverse_increment(cursor: u64, mask: u64) -> u64 {
    let widened = cursor | !mask;
    widened.reverse_bits().wrapping_add(1).reverse_bits()
}

/// The order a keyspace walk's cursor advances in.
///
/// Production has exactly one answer, [`ReverseBinary`], and the parameter
/// exists for the reason [`ExpiryPolicy`]'s does:
/// so the simulator can serve its own workload through a cursor that is
/// genuinely wrong, rather than through an imitation of what a wrong one would
/// look like from outside. The advance is the whole of the walk's liveness
/// argument — [`Dict::scan`] says why — so a defect planted here is the defect
/// itself.
///
/// It is a trait rather than a function pointer so the honest order
/// monomorphises and inlines back into the arithmetic it replaced, and it
/// carries `Clone + Send + 'static` because a shard executor holds one for the
/// life of the process.
pub trait WalkOrder: Clone + Send + 'static {
    /// The cursor a step starting at `cursor` hands back, under a table of
    /// `mask + 1` buckets.
    ///
    /// A cycle must start at `0` and end by returning `0`, and every bucket
    /// under `mask` must be reached before it does. Nothing checks that: it is
    /// what [`Dict::scan`]'s contract rests on, and it is what an implementor
    /// is claiming.
    ///
    /// Defaulted to the honest order, which is the opposite of what
    /// [`ExpiryPolicy`] does with its three
    /// questions — and deliberately. There, every answer is a real decision a
    /// policy has to take a position on. Here there is exactly one correct
    /// answer and a defect is the only reason to write another, so the default
    /// is what keeps a policy that has no opinion about walks from having to
    /// state one, and leaves the override readable as what it is.
    ///
    /// It calls `reverse_increment` directly rather than deferring to
    /// [`ReverseBinary`]. Routing the default through the one override that
    /// would stop it recursing makes deleting that override — the obvious
    /// tidy-up, since it is the default anyway — an unbounded recursion at
    /// runtime rather than a change with no effect.
    fn advance(&self, cursor: u64, mask: u64) -> u64 {
        reverse_increment(cursor, mask)
    }
}

/// The honest order: reverse binary, as [`Dict::scan`] describes.
///
/// A zero-sized type, so a dict walked through it costs exactly what a dict
/// walking itself did. The only implementation this crate ships, and the one
/// [`Dict::scan`] uses.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReverseBinary;

impl WalkOrder for ReverseBinary {
    fn advance(&self, cursor: u64, mask: u64) -> u64 {
        reverse_increment(cursor, mask)
    }
}

/// Whether a chain element is `key`'s, given the hash `key` was looked up with.
///
/// The stored hash is compared first, so a scan settles a miss on one word and
/// only the entry that could be a match pays for a key comparison. The key
/// comparison behind it is not redundant: it is what makes a hash collision a
/// miss rather than a wrong answer.
fn matches(slot: &(u64, Vec<u8>, Entry), hash: u64, key: &[u8]) -> bool {
    slot.0 == hash && slot.1.as_slice() == key
}

fn find<'a>(table: &'a Table, hash: u64, key: &[u8]) -> Option<&'a Entry> {
    let bucket = &table[bucket_index(hash, table.len())];
    bucket
        .iter()
        .find(|slot| matches(slot, hash, key))
        .map(|slot| &slot.2)
}

fn find_mut<'a>(table: &'a mut Table, hash: u64, key: &[u8]) -> Option<&'a mut Entry> {
    let index = bucket_index(hash, table.len());
    table[index]
        .iter_mut()
        .find(|slot| matches(slot, hash, key))
        .map(|slot| &mut slot.2)
}

fn remove_from(table: &mut Table, hash: u64, key: &[u8]) -> Option<Entry> {
    let index = bucket_index(hash, table.len());
    let bucket = &mut table[index];
    let position = bucket.iter().position(|slot| matches(slot, hash, key))?;
    // `remove` rather than `swap_remove`: it keeps the surviving entries in
    // insertion order, so a bucket's contents stay a function of the operation
    // sequence alone and iteration over it is easy to reason about.
    Some(bucket.remove(position).2)
}

#[cfg(test)]
mod tests;
