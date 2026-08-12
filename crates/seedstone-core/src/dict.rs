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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The bytes stored under the key, kept verbatim.
    pub value: Vec<u8>,
    /// When the entry expires, or `None` if it never does.
    pub expires_at: Option<Instant>,
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
        if let Some(entry) = find_mut(&mut self.old, hash, key) {
            entry.expires_at = expires_at;
            return true;
        }
        let Some(new) = self.new.as_mut() else {
            return false;
        };
        match find_mut(new, hash, key) {
            Some(entry) => {
                entry.expires_at = expires_at;
                true
            }
            None => false,
        }
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
        if let Some(existing) = find_mut(&mut self.old, hash, &key) {
            *existing = entry;
            return;
        }
        if let Some(new) = self.new.as_mut()
            && let Some(existing) = find_mut(new, hash, &key)
        {
            *existing = entry;
            return;
        }

        // A key that is not present yet goes straight into the table that will
        // survive the rehash, so it never has to be migrated.
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
        if removed.is_some() {
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

    /// Number of entries, counting both tables while a rehash is in flight.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
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
    pub fn scan<F: FnMut(&[u8], &Entry)>(&self, cursor: u64, mut visit: F) -> u64 {
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
            return reverse_increment(cursor, mask_of(&self.old));
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
            v = reverse_increment(v, large);
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
    ///   expiries to reclaim.
    ///
    /// A key is expired once `now` has *reached* its deadline, which is the
    /// same instant the lazy path uses: the two halves must not disagree about
    /// which keys are alive.
    #[must_use]
    pub fn expire_step(
        &self,
        cursor: u64,
        budget_buckets: usize,
        now: Instant,
    ) -> (u64, Vec<Vec<u8>>) {
        if !self.may_hold_deadlines {
            return (cursor, Vec::new());
        }

        let mut dead = Vec::new();
        let mut next = cursor;
        for _ in 0..budget_buckets {
            // Nothing mutates the table between these steps, so the buckets
            // they cover are disjoint and no key can be reported twice.
            next = self.scan(next, |key, entry| {
                if entry.expires_at.is_some_and(|at| at <= now) {
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
mod tests {
    use super::*;
    use std::time::Duration;

    fn seed() -> DictSeed {
        DictSeed { k0: 7, k1: 11 }
    }

    /// An entry with no deadline — what every test in this module stores. The
    /// dict never reads that field, so a deadline here would exercise nothing;
    /// what a deadline means is the shard's, and is tested there.
    fn entry(value: &[u8]) -> Entry {
        Entry {
            value: value.to_vec(),
            expires_at: None,
        }
    }

    /// The value stored under `key`, or `None` if the key is absent.
    fn value<'a>(d: &'a Dict, key: &[u8]) -> Option<&'a [u8]> {
        d.get(key).map(|entry| entry.value.as_slice())
    }

    /// Inserts ascending numeric keys until a rehash is in flight, and returns
    /// how many were inserted. Panics rather than looping forever if growth
    /// never triggers.
    fn fill_until_rehashing(d: &mut Dict) -> usize {
        let mut count = 0usize;
        while !d.is_rehashing() {
            d.insert(
                count.to_string().into_bytes(),
                entry(count.to_string().as_bytes()),
            );
            count += 1;
            assert!(count < 1_000, "the dict never started rehashing");
        }
        count
    }

    #[test]
    fn hash_key_matches_siphash_1_3() {
        // Golden vectors: these must never change. A replayed simulation and a
        // production node have to agree on where a key lands forever, so a
        // `siphasher` upgrade that altered its output — or a change to what
        // bytes get fed to the hasher — would silently invalidate every seed
        // in existence. Nothing else in this file would notice: every other
        // assertion here is "the value I put in is the value I get back",
        // which any hash function satisfies, seeded or not.
        //
        // Derived independently from a from-definition SipHash written against
        // the specification, not by printing what `hash_key` returns. That
        // reference was itself validated twice: at c=2, d=4 it reproduces the
        // published SipHash-2-4 test vectors (key 000102..0f, message
        // 00..len-1) for lengths 0 through 7, and it agrees with
        // `std::hash::SipHasher` — a third, unrelated implementation — over
        // every length from 0 to 200, which covers the multi-block path the
        // short published vectors never reach. Only the round counts differ
        // between that and SipHash-1-3.
        //
        // Do not "fix" a mismatch here by pasting in whatever `hash_key`
        // currently returns.
        let reference_key = DictSeed {
            k0: 0x0706_0504_0302_0100,
            k1: 0x0f0e_0d0c_0b0a_0908,
        };
        assert_eq!(hash_key(reference_key, b""), 0xabac_0158_050f_c4dc);
        assert_eq!(hash_key(reference_key, b"a"), 0x1c26_97ab_786a_6237);
        assert_eq!(hash_key(reference_key, b"seedstone"), 0xd6ef_d9bd_3ebb_a979);

        // And under the seed this module's tests use, including a key long
        // enough to take the multi-block path.
        let ramp: Vec<u8> = (0..100u8).collect();
        assert_eq!(hash_key(seed(), b""), 0xb8f9_c5c6_7c08_d736);
        assert_eq!(hash_key(seed(), b"key:1"), 0xa3e9_6ed8_b4a0_a1f7);
        assert_eq!(hash_key(seed(), &ramp), 0xbb82_314a_8b08_5307);
    }

    #[test]
    fn a_different_seed_hashes_and_places_keys_differently() {
        let a = seed();
        let b = DictSeed { k0: 11, k1: 7 };
        let keys: [&[u8]; 4] = [b"", b"a", b"key:1", b"seedstone"];

        // A hash that ignored its seed would make every one of these equal,
        // and the whole dict would still round-trip every key it was given.
        for key in keys {
            assert_ne!(hash_key(a, key), hash_key(b, key), "key {key:?}");
        }
        // The difference has to reach bucket placement, not just the hash
        // word: placement is what a replay actually depends on.
        assert!(
            keys.iter().any(|key| {
                bucket_index(hash_key(a, key), INITIAL_BUCKETS)
                    != bucket_index(hash_key(b, key), INITIAL_BUCKETS)
            }),
            "the two seeds put every key in the same bucket"
        );
    }

    /// The deadline flag's accounting, over every operation that can move it.
    ///
    /// A shard skips its whole liveness check when this reads `false`, so a
    /// `false` that should have been `true` is an expiry that never happens.
    /// Both ways in are covered here, and both are inside this type — a caller
    /// cannot reach an `Entry` mutably, so there is no third way to check.
    #[tokio::test(start_paused = true)]
    async fn a_dict_knows_when_no_entry_can_be_carrying_a_deadline() {
        let mut d = Dict::with_seed(seed());
        assert!(!d.may_hold_deadlines(), "a fresh dict holds nothing");

        // Entries without deadlines leave it alone: this is the state nearly
        // every keyspace stays in, and the one the fast path is for.
        d.insert(b"plain".to_vec(), entry(b"v"));
        d.insert(b"other".to_vec(), entry(b"v"));
        assert!(!d.may_hold_deadlines());

        // Way in the first: an inserted entry that carries one.
        d.insert(
            b"dated".to_vec(),
            Entry {
                value: b"v".to_vec(),
                expires_at: Some(Instant::now()),
            },
        );
        assert!(d.may_hold_deadlines());

        // Removing the dated entry does not clear it — the flag is allowed to
        // be wrong only in the direction that costs a lookup.
        assert!(d.remove(b"dated").is_some());
        assert!(d.may_hold_deadlines());

        // Emptying the dict does clear it: nothing left to carry one.
        assert!(d.remove(b"plain").is_some());
        assert!(d.remove(b"other").is_some());
        assert!(d.is_empty());
        assert!(!d.may_hold_deadlines());

        // Way in the second: a deadline put on a key already stored.
        d.insert(b"plain".to_vec(), entry(b"v"));
        assert!(!d.may_hold_deadlines());
        assert!(d.set_deadline(b"plain", Some(Instant::now())));
        assert!(d.may_hold_deadlines());
        assert!(
            d.get(b"plain").and_then(|e| e.expires_at).is_some(),
            "set_deadline did not reach the entry"
        );

        // Clearing a deadline leaves the flag up, for the same reason removing
        // the entry did.
        assert!(d.set_deadline(b"plain", None));
        assert!(d.get(b"plain").expect("still stored").expires_at.is_none());
        assert!(d.may_hold_deadlines());

        // And a deadline aimed at a key that is not there reports the miss.
        assert!(!d.set_deadline(b"absent", Some(Instant::now())));
    }

    /// `set_deadline` has to reach an entry wherever the rehash left it, the
    /// old table included — the same both-tables rule every other lookup
    /// follows. A version that only looked in the surviving table would leave
    /// half the keyspace unable to take a deadline mid-rehash.
    #[tokio::test(start_paused = true)]
    async fn set_deadline_finds_a_key_in_either_table() {
        let mut d = Dict::with_seed(seed());
        let inserted = fill_until_rehashing(&mut d);
        assert!(d.is_rehashing());

        // Key 0 predates the growth, so it can only be in the old table; the
        // last key inserted went straight into the new one.
        let newest = (inserted - 1).to_string().into_bytes();
        assert!(d.set_deadline(b"0", Some(Instant::now())));
        assert!(d.set_deadline(&newest, Some(Instant::now())));
        assert!(d.get(b"0").expect("key 0").expires_at.is_some());
        assert!(d.get(&newest).expect("newest key").expires_at.is_some());

        // And the deadlines survive the migration that follows.
        drain_rehash(&mut d);
        assert!(d.get(b"0").expect("key 0").expires_at.is_some());
        assert!(d.get(&newest).expect("newest key").expires_at.is_some());
    }

    #[test]
    fn insert_get_remove_round_trip() {
        let mut d = Dict::with_seed(seed());
        d.insert(b"a".to_vec(), entry(b"1"));
        assert_eq!(value(&d, b"a"), Some(&b"1"[..]));
        d.insert(b"a".to_vec(), entry(b"2")); // overwrite, len stays 1
        assert_eq!((d.len(), value(&d, b"a")), (1, Some(&b"2"[..])));
        assert_eq!(d.remove(b"a").map(|e| e.value), Some(b"2".to_vec()));
        assert_eq!((d.len(), value(&d, b"a")), (0, None));
    }

    #[test]
    fn survives_growth_through_many_inserts() {
        let mut d = Dict::with_seed(seed());
        for i in 0..10_000u32 {
            d.insert(i.to_string().into_bytes(), entry(i.to_string().as_bytes()));
        }
        while d.is_rehashing() {
            d.rehash_step(16);
        }
        assert_eq!(d.len(), 10_000);
        for i in (0..10_000u32).step_by(97) {
            assert_eq!(
                value(&d, i.to_string().as_bytes()),
                Some(i.to_string().as_bytes())
            );
        }
    }

    #[test]
    fn a_fresh_dict_is_empty_and_not_rehashing() {
        let d = Dict::with_seed(seed());
        assert_eq!(d.len(), 0);
        assert!(d.is_empty());
        assert!(!d.is_rehashing());
        assert_eq!(value(&d, b"absent"), None);
    }

    #[test]
    fn removing_an_absent_key_reports_it_and_leaves_len_alone() {
        let mut d = Dict::with_seed(seed());
        d.insert(b"present".to_vec(), entry(b"v"));
        assert!(d.remove(b"absent").is_none());
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn lookups_see_both_tables_while_rehashing() {
        let mut d = Dict::with_seed(seed());
        let inserted = fill_until_rehashing(&mut d);

        // Entries are spread across the two tables at this point: everything
        // written before the growth sits in the old one, the last insert in
        // the new one. Reads must not care which.
        assert!(d.is_rehashing());
        for i in 0..inserted {
            assert_eq!(
                value(&d, i.to_string().as_bytes()),
                Some(i.to_string().as_bytes()),
                "key {i} went missing mid-rehash"
            );
        }
        assert_eq!(d.len(), inserted);
        assert!(d.is_rehashing(), "a lookup must not advance the rehash");
    }

    #[test]
    fn removes_reach_into_the_old_table_while_rehashing() {
        let mut d = Dict::with_seed(seed());
        let inserted = fill_until_rehashing(&mut d);

        // Key 0 predates the growth, so it can only be in the old table.
        assert_eq!(d.remove(b"0").map(|e| e.value), Some(b"0".to_vec()));
        assert_eq!(value(&d, b"0"), None);
        assert_eq!(d.len(), inserted - 1);

        // And it stays gone once the rehash completes: the migration must not
        // resurrect it from a bucket it was never removed from.
        drain_rehash(&mut d);
        assert_eq!(value(&d, b"0"), None);
        assert_eq!(d.len(), inserted - 1);
    }

    #[test]
    fn overwriting_during_a_rehash_does_not_duplicate_the_entry() {
        let mut d = Dict::with_seed(seed());
        let inserted = fill_until_rehashing(&mut d);

        // Key 0 lives in the old table; the overwrite must update it in place
        // rather than leave a second copy in the new one.
        d.insert(b"0".to_vec(), entry(b"overwritten"));
        assert_eq!(d.len(), inserted);
        assert_eq!(value(&d, b"0"), Some(&b"overwritten"[..]));

        drain_rehash(&mut d);
        assert_eq!(d.len(), inserted);
        assert_eq!(value(&d, b"0"), Some(&b"overwritten"[..]));
        // A duplicate would survive the first removal and still answer reads.
        assert_eq!(
            d.remove(b"0").map(|e| e.value),
            Some(b"overwritten".to_vec())
        );
        assert_eq!(value(&d, b"0"), None);
    }

    #[test]
    fn rehashing_ends_when_the_old_table_drains() {
        let mut d = Dict::with_seed(seed());
        let inserted = fill_until_rehashing(&mut d);

        let steps = drain_rehash(&mut d);
        assert!(
            steps > 0,
            "the rehash was already over before it was driven"
        );
        assert!(!d.is_rehashing());
        assert_eq!(d.len(), inserted);
        for i in 0..inserted {
            assert_eq!(
                value(&d, i.to_string().as_bytes()),
                Some(i.to_string().as_bytes()),
                "key {i} was lost by the migration"
            );
        }

        // Stepping a dict that is not rehashing is a no-op, not a panic.
        d.rehash_step(64);
        assert!(!d.is_rehashing());
        assert_eq!(d.len(), inserted);
    }

    /// Drives a rehash to completion one bucket at a time, returning how many
    /// steps it took. Panics rather than looping forever if it never ends.
    fn drain_rehash(d: &mut Dict) -> usize {
        let mut steps = 0;
        while d.is_rehashing() {
            d.rehash_step(1);
            steps += 1;
            assert!(steps < 10_000, "the rehash never finished");
        }
        steps
    }

    #[test]
    fn a_rehash_step_larger_than_what_is_left_finishes_it_without_running_past() {
        let mut d = Dict::with_seed(seed());
        let inserted = fill_until_rehashing(&mut d);

        d.rehash_step(usize::MAX);
        assert!(!d.is_rehashing());
        assert_eq!(d.len(), inserted);
        for i in 0..inserted {
            assert_eq!(
                value(&d, i.to_string().as_bytes()),
                Some(i.to_string().as_bytes())
            );
        }
    }

    #[test]
    fn contents_do_not_depend_on_insertion_order() {
        // Two dicts holding the same entries, reached by different operation
        // sequences — ascending versus descending, and every key overwritten
        // once — agree on every key. This says nothing about the seed: it
        // would pass just as well with two different ones. What it pins down
        // is that an overwrite is idempotent and that the order writes arrive
        // in cannot change what the dict answers.
        let mut a = Dict::with_seed(seed());
        let mut b = Dict::with_seed(seed());
        for i in 0..200u32 {
            a.insert(i.to_string().into_bytes(), entry(i.to_string().as_bytes()));
        }
        for i in (0..200u32).rev() {
            b.insert(i.to_string().into_bytes(), entry(b"stale"));
            b.insert(i.to_string().into_bytes(), entry(i.to_string().as_bytes()));
        }
        assert_eq!(a.len(), b.len());
        for i in 0..200u32 {
            assert_eq!(
                value(&a, i.to_string().as_bytes()),
                value(&b, i.to_string().as_bytes()),
                "key {i}"
            );
        }
    }

    #[test]
    fn interleaved_inserts_and_removes_keep_len_and_contents_consistent() {
        use std::collections::BTreeSet;

        let mut d = Dict::with_seed(seed());
        let mut expected = BTreeSet::new();
        for i in 0..2_000u32 {
            let key = i.to_string().into_bytes();
            d.insert(key.clone(), entry(i.to_string().as_bytes()));
            expected.insert(key);
            // Remove an older key every third insert, so the table shrinks and
            // grows while rehashes are in flight.
            if i % 3 == 2 {
                let victim = (i / 3).to_string().into_bytes();
                assert_eq!(
                    d.remove(&victim).is_some(),
                    expected.remove(&victim),
                    "key {} disagreed on removal",
                    i / 3
                );
            }
        }
        assert_eq!(d.len(), expected.len());
        for key in &expected {
            assert_eq!(value(&d, key), Some(key.as_slice()), "key {key:?}");
        }
        for i in 0..2_000u32 {
            let key = i.to_string().into_bytes();
            if !expected.contains(&key) {
                assert_eq!(value(&d, &key), None, "key {i} should be gone");
            }
        }

        // Draining what is left by removal must bring the dict back to empty.
        // A `len` that drifted anywhere above would surface here.
        for key in &expected {
            assert!(
                d.remove(key).is_some(),
                "key {key:?} vanished before its removal"
            );
        }
        assert_eq!(d.len(), 0);
        assert!(d.is_empty());
    }

    #[test]
    fn scan_visits_every_key_when_static() {
        let mut d = Dict::with_seed(seed());
        for i in 0..500u32 {
            d.insert(i.to_string().into_bytes(), entry(b""));
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut c = 0;
        let mut steps = 0;
        loop {
            c = d.scan(c, |k, _| {
                seen.insert(k.to_vec());
            });
            if c == 0 {
                break;
            }
            // Purely a guard, not an acceptance criterion: the failure this
            // test exists to catch includes a cursor that never comes back to
            // 0, and without a bound that failure wedges the process instead of
            // reporting itself.
            steps += 1;
            assert!(steps < 10_000, "the cursor never returned to 0");
        }
        assert_eq!(seen.len(), 500);
    }

    #[test]
    fn scan_sees_every_stable_key_across_growth() {
        // Keys inserted before the scan starts and never removed must be visited
        // at least once even though the table grows (and rehashes) mid-scan.
        let mut d = Dict::with_seed(seed());
        for i in 0..64u32 {
            d.insert(format!("stable-{i}").into_bytes(), entry(b""));
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut c = 0;
        let mut extra = 0u32;
        let mut steps = 0;
        loop {
            c = d.scan(c, |k, _| {
                seen.insert(k.to_vec());
            });
            if c == 0 {
                break;
            }
            // As above, a guard rather than an assertion about the keyspace:
            // this is the test a cursor outrun by a growing table hangs in.
            steps += 1;
            assert!(steps < 10_000, "the cursor never returned to 0");

            for _ in 0..8 {
                d.insert(format!("noise-{extra}").into_bytes(), entry(b""));
                extra += 1;
            }
            d.rehash_step(1);
        }
        for i in 0..64u32 {
            assert!(
                seen.contains(format!("stable-{i}").as_bytes()),
                "lost stable-{i}"
            );
        }
    }

    #[test]
    fn scanning_an_empty_dict_ends_the_cycle_without_visiting_anything() {
        let d = Dict::with_seed(seed());
        // Not "returns 0 eventually": an empty keyspace must cost one call, not
        // one call per bucket, or a node full of empty shards would answer a
        // sweep with a long run of empty replies.
        assert_eq!(
            d.scan(0, |k, _| panic!("visited {k:?} in an empty dict")),
            0
        );
    }

    #[test]
    fn a_full_cycle_at_rest_visits_every_bucket_exactly_once() {
        use std::collections::BTreeSet;

        let mut d = Dict::with_seed(seed());
        for i in 0..100u32 {
            d.insert(i.to_string().into_bytes(), entry(b""));
        }
        drain_rehash(&mut d);

        // The keys alone cannot show this: several share a bucket and some
        // buckets are empty, so a cursor that skipped or repeated a bucket
        // could still deliver every key. Read the buckets directly.
        let buckets = d.old.len();
        let mask = u64::try_from(buckets).expect("a bucket count is a usize") - 1;
        let mut visited = Vec::new();
        let mut c = 0;
        loop {
            visited.push(usize::try_from(c & mask).expect("a masked cursor fits a bucket index"));
            c = d.scan(c, |_, _| {});
            if c == 0 {
                break;
            }
            assert!(visited.len() <= buckets, "the cursor never returned to 0");
        }

        assert_eq!(visited.len(), buckets, "wrong number of steps in a cycle");
        let distinct: BTreeSet<usize> = visited.iter().copied().collect();
        assert_eq!(distinct.len(), buckets, "a bucket repeated: {visited:?}");
    }

    #[test]
    fn a_cycle_spanning_the_end_of_a_rehash_still_sees_every_stable_key() {
        use std::collections::BTreeSet;

        let mut d = Dict::with_seed(seed());
        let inserted = fill_until_rehashing(&mut d);
        assert!(
            d.is_rehashing(),
            "the cycle must start with two tables live"
        );

        let mut seen = BTreeSet::new();
        let mut c = 0;
        let mut steps = 0;
        let mut finished_mid_cycle = false;
        loop {
            c = d.scan(c, |k, _| {
                seen.insert(k.to_vec());
            });
            if c == 0 {
                break;
            }
            steps += 1;
            // Collapse the two tables into one part-way through, so the rest of
            // the cycle runs against the larger table alone with a cursor that
            // was produced while both were live.
            if steps == 2 {
                drain_rehash(&mut d);
                finished_mid_cycle = true;
            }
            assert!(steps < 10_000, "the cursor never returned to 0");
        }

        assert!(finished_mid_cycle, "the cycle ended before the rehash did");
        assert!(!d.is_rehashing());
        for i in 0..inserted {
            assert!(seen.contains(i.to_string().as_bytes()), "lost key {i}");
        }
    }

    #[test]
    fn the_growth_guarantee_holds_wherever_in_the_cycle_the_growth_starts() {
        use std::collections::BTreeSet;

        // The tests above each exercise one interleaving of growth and cursor,
        // and a cursor that advanced in plain binary order would survive some
        // of them. Sweep every position in the cycle at which the table can
        // start doubling and demand the guarantee at each one.
        let mut d = Dict::with_seed(seed());
        for i in 0..100u32 {
            d.insert(format!("stable-{i}").into_bytes(), entry(b""));
        }
        drain_rehash(&mut d);
        let cycle = d.old.len();

        for grow_at in 0..cycle {
            let mut d = Dict::with_seed(seed());
            for i in 0..100u32 {
                d.insert(format!("stable-{i}").into_bytes(), entry(b""));
            }
            drain_rehash(&mut d);
            assert_eq!(d.old.len(), cycle);

            let mut seen = BTreeSet::new();
            let mut c = 0;
            let mut step = 0usize;
            let mut noise = 0u32;
            loop {
                if step == grow_at {
                    // Write until the table starts doubling, exactly here.
                    while !d.is_rehashing() {
                        d.insert(format!("noise-{noise}").into_bytes(), entry(b""));
                        noise += 1;
                    }
                }
                c = d.scan(c, |k, _| {
                    seen.insert(k.to_vec());
                });
                if c == 0 {
                    break;
                }
                // Let the migration run alongside the rest of the cycle, so the
                // cursor also crosses the moment the two tables collapse back
                // into one.
                d.rehash_step(3);
                step += 1;
                assert!(step < 10_000, "the cursor never returned to 0");
            }

            assert!(noise > 0, "the table never grew (grow_at {grow_at})");
            for i in 0..100u32 {
                assert!(
                    seen.contains(format!("stable-{i}").as_bytes()),
                    "lost stable-{i} when growth started at step {grow_at}"
                );
            }
        }
    }

    #[test]
    fn a_cycle_under_continuous_growth_converges_instead_of_chasing_the_table() {
        // Coverage is not the only thing the traversal order buys, and on a
        // dict that only ever grows it is not the sharpest test of it: a cursor
        // that counted its bucket bits in plain binary order would still reach
        // every bucket, because a bucket that splits when the table doubles
        // splits into one index the cursor has passed and one still ahead of
        // it. What such a cursor loses is *termination*, and with it the whole
        // point of handing the value back to a client: it advances one bucket
        // per call, so a table that doubles faster than that outruns it and the
        // cycle never ends.
        //
        // What makes the cycle finite is that a step is a fixed *fraction* of
        // the keyspace rather than a fixed number of buckets: a step under mask
        // `m` moves the cursor forward by exactly `1 / (m + 1)` of the whole,
        // and doubling the table only makes later steps smaller. Read that
        // position off the cursor — it is the cursor's bits in reverse — and
        // demand it strictly increase. A cursor that moved through the keyspace
        // in any other order fails on the second call instead of hanging.
        let mut d = Dict::with_seed(seed());
        for i in 0..64u32 {
            d.insert(format!("stable-{i}").into_bytes(), entry(b""));
        }
        let started_over = d.new.as_ref().map_or(d.old.len(), Vec::len);

        let mut c = 0;
        let mut position = 0u64;
        let mut steps = 0usize;
        let mut noise = 0u32;
        loop {
            c = d.scan(c, |_, _| {});
            if c == 0 {
                break;
            }
            let next = c.reverse_bits();
            assert!(
                next > position,
                "the cursor moved backwards through the keyspace: \
                 {position} then {next}"
            );
            position = next;

            steps += 1;
            // Monotone progress does not by itself say the cursor ever lands
            // back on 0 — a cursor whose steps shrink faster than the table
            // grows would advance forever without wrapping. Stop it here, so
            // that failure is a failure and not a hung suite. A correct cycle
            // takes a few hundred steps.
            assert!(steps < 10_000, "the cycle never came back to cursor 0");

            for _ in 0..8 {
                d.insert(format!("noise-{noise}").into_bytes(), entry(b""));
                noise += 1;
            }
            d.rehash_step(1);
        }

        // Monotone progress alone would allow a cycle that crawls. Since every
        // step is `1 / (m + 1)` of the keyspace and `m` only ever grows, a
        // whole cycle costs no more steps than the table it ends on has
        // buckets, however much the table grew along the way.
        let ended_over = d.new.as_ref().map_or(d.old.len(), Vec::len);
        assert!(ended_over > started_over, "the table never grew");
        assert!(
            steps <= ended_over,
            "a cycle over {ended_over} buckets took {steps} steps"
        );
    }

    /// An entry whose deadline is `expires_at`.
    fn dated(value: &[u8], expires_at: Instant) -> Entry {
        Entry {
            value: value.to_vec(),
            expires_at: Some(expires_at),
        }
    }

    /// Sweeps from `cursor` to the end of the cycle at `budget` buckets a call
    /// and returns every key reported dead, in the order the cursor reached
    /// them.
    fn sweep_from(d: &Dict, cursor: u64, budget: usize, now: Instant) -> Vec<Vec<u8>> {
        let mut dead = Vec::new();
        let mut cursor = cursor;
        let mut steps = 0;
        loop {
            let (next, mut batch) = d.expire_step(cursor, budget, now);
            dead.append(&mut batch);
            cursor = next;
            if cursor == 0 {
                break;
            }
            steps += 1;
            assert!(steps < 10_000, "the sweep's cursor never returned to 0");
        }
        dead
    }

    #[test]
    fn the_sweep_never_eats_the_living() {
        use std::collections::BTreeSet;

        // Built forwards from one reading rather than backwards from `now`, so
        // no arithmetic here can leave `Instant`'s range.
        let start = Instant::now();
        let (past, now, future) = (
            start,
            start + Duration::from_secs(1),
            start + Duration::from_mins(1),
        );

        // Half the keyspace past its deadline, a quarter still ahead of one,
        // a quarter carrying none at all. The two survivor kinds are different
        // failures: a sweep that ignored the deadline would take the future
        // one, and a sweep that treated "no deadline" as "expired at zero"
        // would take the other.
        let mut d = Dict::with_seed(seed());
        let mut expired = BTreeSet::new();
        let mut alive = BTreeSet::new();
        for i in 0..200u32 {
            let key = format!("k{i}").into_bytes();
            match i % 4 {
                0 | 1 => {
                    d.insert(key.clone(), dated(b"v", past));
                    expired.insert(key);
                }
                2 => {
                    d.insert(key.clone(), entry(b"v"));
                    alive.insert(key);
                }
                _ => {
                    d.insert(key.clone(), dated(b"v", future));
                    alive.insert(key);
                }
            }
        }

        let dead = sweep_from(&d, 0, 4, now);
        assert_eq!(
            dead.len(),
            expired.len(),
            "the sweep reported a key twice or missed one"
        );
        assert_eq!(
            dead.iter().cloned().collect::<BTreeSet<_>>(),
            expired,
            "the sweep did not report exactly the keys whose deadline had passed"
        );

        // And what it reported is what a caller can remove: the survivors are
        // all still there afterwards, and nothing else is.
        for key in &dead {
            assert!(
                d.remove(key).is_some(),
                "key {key:?} was not there to remove"
            );
        }
        assert_eq!(d.len(), alive.len());
        for key in &alive {
            assert!(d.get(key).is_some(), "the sweep ate {key:?}");
        }
    }

    /// The fast path the flag exists for, asserted where it is observable: a
    /// dict that can hold no deadline hands the cursor straight back, so the
    /// sweep costs a keyspace without expiries one branch per tick rather than
    /// a walk of its table.
    #[test]
    fn a_dict_that_holds_no_deadline_is_not_walked_at_all() {
        let now = Instant::now();
        let mut d = Dict::with_seed(seed());
        for i in 0..100u32 {
            d.insert(format!("k{i}").into_bytes(), entry(b"v"));
        }

        // A walked cursor cannot come back where it started: a step always
        // moves it. Two starting points, because one of them could be the
        // cursor a completed cycle happens to return.
        assert_eq!(d.expire_step(0, 4, now), (0, Vec::new()));
        assert_eq!(d.expire_step(1 << 63, 4, now), (1 << 63, Vec::new()));

        // And it is skipped work, not lost work: one dated entry and the walk
        // happens again.
        d.insert(b"dated".to_vec(), dated(b"v", now));
        assert_ne!(
            d.expire_step(0, 4, now).0,
            0,
            "a dict holding a deadline was not walked"
        );
    }

    #[test]
    fn the_sweep_respects_its_budget() {
        use std::collections::BTreeSet;

        // Fifty entries over a table that has settled at sixty-four buckets, so
        // a budget of four is a small fraction of a cycle and several of the
        // buckets it covers are empty.
        let past = Instant::now();
        let now = past + Duration::from_secs(1);
        let mut d = Dict::with_seed(seed());
        for i in 0..50u32 {
            d.insert(format!("k{i}").into_bytes(), dated(b"v", past));
        }
        drain_rehash(&mut d);
        assert_eq!(d.old.len(), 64, "the table is not the size this test wants");

        // Where four ordinary scan steps end, and what they see on the way:
        // the budget is spent in exactly that traversal and no further.
        let mut expected_cursor = 0;
        let mut expected_dead = BTreeSet::new();
        for _ in 0..4 {
            expected_cursor = d.scan(expected_cursor, |key, _| {
                expected_dead.insert(key.to_vec());
            });
        }

        let (cursor, dead) = d.expire_step(0, 4, now);
        assert_eq!(cursor, expected_cursor, "the sweep overran its budget");
        assert_eq!(dead.iter().cloned().collect::<BTreeSet<_>>(), expected_dead);
        assert!(!dead.is_empty(), "four buckets of fifty keys held none");
        assert!(
            dead.len() < 50,
            "a budgeted step reported the whole keyspace"
        );

        // The rest is left for later rather than skipped: resuming from the
        // cursor the budgeted call handed back reports every key it did not,
        // and between them they cover the keyspace exactly once.
        let mut covered: BTreeSet<Vec<u8>> = dead.iter().cloned().collect();
        for key in sweep_from(&d, cursor, 4, now) {
            assert!(
                covered.insert(key.clone()),
                "key {key:?} was reported twice in one cycle"
            );
        }
        assert_eq!(
            covered.len(),
            50,
            "the cycle did not reach the keys the budget left behind"
        );
    }

    #[test]
    fn scan_hands_over_the_value_stored_under_each_key() {
        use std::collections::BTreeMap;

        let mut d = Dict::with_seed(seed());
        for i in 0..50u32 {
            d.insert(
                i.to_string().into_bytes(),
                entry(format!("v{i}").as_bytes()),
            );
        }
        drain_rehash(&mut d);

        let mut seen = BTreeMap::new();
        let mut c = 0;
        loop {
            c = d.scan(c, |k, entry| {
                seen.insert(k.to_vec(), entry.value.clone());
            });
            if c == 0 {
                break;
            }
        }
        assert_eq!(seen.len(), 50);
        for i in 0..50u32 {
            assert_eq!(
                seen.get(i.to_string().as_bytes()).map(Vec::as_slice),
                Some(format!("v{i}").as_bytes()),
                "key {i} came back with the wrong value"
            );
        }
    }
}
