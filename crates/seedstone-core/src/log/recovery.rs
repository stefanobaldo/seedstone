//! Start-up: the newest usable image of each shard, the tail of its log
//! replayed over it, and everything nothing used removed.
//!
//! Five passes, each bounded:
//!
//! 1. **Headers.** Every `.snap`'s header — one chunk each — gives the
//!    shards it images and their bases. A header that is short, carries the
//!    wrong magic or fails its checksum is damage, charged to every shard as
//!    a bad segment header is; a version above this build's refuses the
//!    start.
//! 2. **Segments.** Every `.seg` in name order — one per rotation of each
//!    generation — every intact record bucketed by shard: the whole of what
//!    is on disk, which compaction keeps to about a snapshot's worth per
//!    executor. Not only the tail
//!    from the newest image's base: whether that image is usable is not
//!    known until this pass has found every `Rebase`, so a tail read
//!    against an image that then proves dead would be a tail with a hole
//!    where the image was. Memory: the keyspace plus the log on disk.
//! 3. **Images.** Every `.snap` newest first, its entries inserted straight
//!    into the dicts of the shards that still want one. At the file's end
//!    the footer must be there and its counts must match, and no damage may
//!    have been met; otherwise the file is refused and those shards wait
//!    for the next older file.
//! 4. **Tails.** Per shard, the gapless prefix from the chosen base — the
//!    same prefix rule as ever, starting at the base rather than at `0` —
//!    replayed over the image. One file holds every shard's records, so
//!    damage in it is charged to each shard it may have cost without a
//!    trace: one whose highest record precedes the latest hole, or that has
//!    none. A shard with a record after the hole shows what it lost there
//!    as a gap, and a gap on a disk with damage is reported as a loss.
//! 5. **Garbage.** Every `.snap` no shard used and every `.seg` no kept
//!    record came from is removed — unless damage was met reading it: a
//!    read can fail where the next succeeds, and the file may be the only
//!    copy of what it held.
//!
//! **The prefix rule.** A shard applies sequence `base, base+1, …` and
//! stops at the first number that is missing; everything of that shard
//! with a higher sequence, in any segment, is discarded and counted.
//! Replaying past a gap would produce a state the shard never held. What
//! the server does with a gap is serve the prefix and say so, per shard —
//! a cache that refused to start over damage it can absorb would be down
//! for longer than the damage costs.
//!
//! **A cut is not undone by a later start.** A shard cut at a gap resumes
//! there, and the next generation reuses the sequence numbers of the
//! records that were cut — which are still on disk. So the first record a
//! shard writes after a cut is a `Rebase` at its resume point, and a
//! record of an older generation at or above a newer generation's `Rebase`
//! is dead: never replayed, never counted as a gap. Where two generations
//! still hold the same sequence number, the newer one's record is kept.
//!
//! **A snapshot can be dead too.** Its image holds the effect of every
//! record below its base — including records a later start declared dead
//! by rebasing below that base, which can happen when that start refused
//! the snapshot (read corruption is drawn per read, and can be transient)
//! and cut the log. So a snapshot of generation `g` with base `b` is dead
//! for a shard when a `Rebase` of a generation above `g` sits below `b`.
//!
//! **A refused image is a possible loss only where the log cannot make up
//! for it.** Its shards are marked lossy if the log they fall back to stops
//! short of the image's base, and not otherwise, whatever made it refused:
//! a footer whose counts disagree, damage before its end, or no footer at
//! all. Damage alone is not evidence of a loss — a crash can persist a
//! file's unsynced writes out of order, so a snapshot interrupted mid-cycle
//! may hold a hole before where it stops while the log it would have
//! covered is still whole. Nor is a missing footer evidence of none: a
//! finished file whose footer was lost reads as one a crash interrupted,
//! and compaction may already have removed its log. A refused file whose
//! shards the log does not cover stays on disk, since the next read of it
//! may be whole.
//!
//! A segment whose header names a format version above this build's is
//! refused, and the node does not start: that is not damage, it is a
//! downgrade, and guessing at it would be worse than stopping. One whose
//! version is below this build's is refused too: the directory was written
//! by an earlier build, and reading its files as damage would charge a loss
//! to every shard instead of saying what they are. A header
//! that is short, carries the wrong magic or fails its checksum is damage:
//! the segment is abandoned and counted, and every shard is marked lossy,
//! because a loss nobody can attribute to a shard is a possible loss for
//! all of them.

use std::io;
use std::path::Path;

use bytes::Bytes;

use crate::dict::{Dict, DictSeed, Entry, shard_seed};
use crate::log::disk::Disk;
use crate::log::effect::{Effect, Owned};
use crate::log::file::{
    HeaderError, SEGMENT_HEADER_LEN, decode_segment_header, parse_name, parse_segment_name,
};
use crate::log::reader::{Item, Reader};
use crate::log::snapshot::{FOOTER_SHARD, Footer, SnapshotHeader, parse_snapshot_name};
use crate::shard::{Now, Replayed, replay_into};

/// Re-exported: every caller of [`recover`] chooses one.
pub use crate::log::reader::ReaderMode;

/// How much of a snapshot is read to get its header: the fixed part plus
/// ten bytes per shard, which for the deployed 1024 shards is under 11 KiB.
const HEADER_CHUNK: usize = 16 * 1024;

/// What [`recover`] is asked for.
pub struct RecoverSpec<'a, D: Disk> {
    pub disk: &'a D,
    /// The `wal/` directory.
    pub wal: &'a Path,
    pub shards: u16,
    pub reader: ReaderMode,
    /// The planted defect: accept a snapshot with no footer. What a
    /// recovery that trusted an unfinished image would do — it restores
    /// the keys scanned before the crash and loses the rest.
    pub trust_unfinished: bool,
    pub seed: DictSeed,
    /// The clock the image's deadlines are resolved against.
    pub now: Now,
}

// By hand: a derive would ask `D: Copy`, and the spec only borrows it.
impl<D: Disk> Clone for RecoverSpec<'_, D> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<D: Disk> Copy for RecoverSpec<'_, D> {}

/// One shard as recovery rebuilt it.
pub struct RecoveredShard {
    pub dict: Dict,
    /// The position the shard resumes at.
    pub seq: u64,
    /// Whether damage on disk could explain a loss of this shard's
    /// records: a hole in the node's log after its highest record, or with
    /// no record of it at all; a gap in its records where the log had
    /// damage; a record of it this build could not read; a segment or
    /// snapshot header nobody could read; or a refused image of it whose
    /// base the log does not reach. A gap alone does not set it — a gap in
    /// an intact log is not something a disk did.
    pub lossy: bool,
    /// Whether the shard's prefix was cut, or its loss is possible: the
    /// pool writes a `Rebase` before serving it.
    pub cut: bool,
}

impl std::fmt::Debug for RecoveredShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveredShard")
            .field("keys", &self.dict.len())
            .field("seq", &self.seq)
            .field("lossy", &self.lossy)
            .field("cut", &self.cut)
            .finish()
    }
}

/// One shard the report names as cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardTruncation {
    pub shard: u16,
    /// Records applied before the gap.
    pub applied: u64,
    /// Records discarded after it.
    pub discarded: u64,
}

/// What recovery did, for the node's log line.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Report {
    pub segments: u64,
    pub records: u64,
    pub applied: u64,
    pub discarded: u64,
    pub malformed: u64,
    pub damage_bytes: u64,
    pub holes: u64,
    pub abandoned_segments: u64,
    pub truncated: Vec<ShardTruncation>,
    /// Snapshot files at least one shard took its image from.
    pub snapshots_used: u64,
    /// Snapshot files refused: no footer, counts that did not match,
    /// damage inside, a header that failed, or shards outside the node.
    pub snapshots_refused: u64,
    /// Files removed because nothing used them.
    pub files_removed: u64,
}

/// Every shard, and the report.
#[derive(Debug)]
pub struct Recovery {
    pub shards: Vec<RecoveredShard>,
    pub report: Report,
}

/// Reads `wal` and rebuilds every shard.
///
/// # Errors
///
/// The directory cannot be listed, or a file names a format version above
/// this build's — the two cases where the node must not start. Everything
/// else is damage, counted and survived.
pub fn recover<D: Disk>(spec: RecoverSpec<'_, D>) -> io::Result<Recovery> {
    let names = spec.disk.list(spec.wal)?;
    let mut report = Report::default();
    let (mut snaps, header_damage) = read_headers(&spec, &names, &mut report)?;
    let segments = segment_files(&names);
    let mut scan = Scan::new(spec.shards, segments.len());
    scan.unattributed_loss |= header_damage;
    for (index, file) in segments.iter().enumerate() {
        scan.segment(spec.disk, spec.wal, spec.reader, file, index)?;
    }
    scan.charge_holes();
    let rebases = scan.rebases();
    let mut building = Building {
        dicts: (0..spec.shards)
            .map(|shard| Dict::with_seed(shard_seed(spec.seed, shard)))
            .collect(),
        due: vec![Vec::new(); usize::from(spec.shards)],
    };
    let chosen = read_images(&spec, &mut snaps, &rebases, &mut building, &mut report);
    let unread = std::mem::take(&mut scan.unread);
    let (shards, kept) = replay_tails(&spec, scan, &chosen, building, segments.len(), &mut report);
    for snap in &mut snaps {
        snap.damaged |= snap
            .unfinished_for
            .iter()
            .any(|(shard, base)| shards[usize::from(*shard)].seq < *base);
    }
    remove_garbage(&spec, &snaps, &segments, &kept, &unread, &mut report);
    Ok(Recovery { shards, report })
}

/// The dicts being rebuilt, and per shard the image keys whose deadline
/// had already passed — removed after the tail unless the tail moved them.
struct Building {
    dicts: Vec<Dict>,
    due: Vec<Vec<Bytes>>,
}

/// One snapshot file, as its header describes it.
struct SnapFile {
    name: String,
    /// `None` when pass 1 refused the file: nothing takes an image from it.
    header: Option<SnapshotHeader>,
    /// Set in pass 3 when at least one shard took its image from here.
    used: bool,
    /// Damage was met reading it — its header in pass 1, or its body in
    /// pass 3. Such a file is never garbage: see [`remove_garbage`].
    damaged: bool,
    /// The shards and bases pass 3 refused it for with no footer. It is
    /// kept if the log of any of them stops short of its base: nothing on
    /// disk says it was never finished, and it may be the only copy.
    unfinished_for: Vec<(u16, u64)>,
}

impl SnapFile {
    /// Newest first: generation, then cycle — from the name, which pass 1
    /// checked against the header wherever there is one.
    fn age(&self) -> std::cmp::Reverse<(u64, u32)> {
        let (generation, _, cycle) = parse_snapshot_name(&self.name).unwrap_or_default();
        std::cmp::Reverse((generation, cycle))
    }
}

/// Pass 1: every `.snap`'s header, newest first. The flag is whether any
/// header was damage — a loss no shard can be named for.
fn read_headers<D: Disk>(
    spec: &RecoverSpec<'_, D>,
    names: &[String],
    report: &mut Report,
) -> io::Result<(Vec<SnapFile>, bool)> {
    let mut snaps = Vec::new();
    let mut damage = false;
    for name in names {
        let Some(from_name) = parse_snapshot_name(name) else {
            continue;
        };
        let mut chunk = vec![0u8; HEADER_CHUNK];
        let read = spec
            .disk
            .open_read(&spec.wal.join(name))
            .map_or(0, |mut src| read_fully(&mut src, &mut chunk));
        let mut unreadable = false;
        let header = match SnapshotHeader::decode(&chunk[..read]) {
            Ok(header) => Some(header).filter(|header| {
                // Not this node's file, or damage the checksum happened to
                // pass: refused, and nothing takes an image from it.
                let mut seen = vec![false; usize::from(spec.shards)];
                (header.generation, header.executor, header.cycle) == from_name
                    && header.bases.iter().all(|(shard, _)| {
                        seen.get_mut(usize::from(*shard))
                            .is_some_and(|seen| !std::mem::replace(seen, true))
                    })
            }),
            Err(HeaderError::NewerVersion(version)) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("{name}: format version {version} is newer than this build reads"),
                ));
            }
            Err(HeaderError::OlderVersion(version)) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "{name}: format version {version} predates this build's layout; \
                         the directory was written by an earlier build and is not read"
                    ),
                ));
            }
            Err(HeaderError::Short | HeaderError::BadMagic | HeaderError::BadChecksum) => {
                // Synced before the file's name was: a header that does
                // not read is damage, not a crash.
                unreadable = true;
                None
            }
        };
        damage |= unreadable;
        if header.is_none() {
            report.snapshots_refused += 1;
        }
        snaps.push(SnapFile {
            name: name.clone(),
            damaged: unreadable,
            header,
            used: false,
            unfinished_for: Vec::new(),
        });
    }
    snaps.sort_by_key(SnapFile::age);
    Ok((snaps, damage))
}

/// One `.seg`: the generation and rotation its name carries.
struct SegmentFile {
    generation: u64,
    rotation: u32,
    name: String,
}

/// Every `.seg`, sorted by generation and rotation.
///
/// The previous layout's names — `<generation>-<executor>-<rotation>.seg` —
/// are listed too, and this is the one place that shape is still known: a
/// start must read such a file's header and refuse the directory, not pass
/// over a file it does not recognise and serve an empty node.
fn segment_files(names: &[String]) -> Vec<SegmentFile> {
    let mut files: Vec<SegmentFile> = names
        .iter()
        .filter_map(|name| {
            parse_segment_name(name)
                .or_else(|| parse_name(name, ".seg").map(|(g, _, r)| (g, r)))
                .map(|(generation, rotation)| SegmentFile {
                    generation,
                    rotation,
                    name: name.clone(),
                })
        })
        .collect();
    files.sort_by(|a, b| {
        (a.generation, a.rotation, &a.name).cmp(&(b.generation, b.rotation, &b.name))
    });
    files
}

/// What pass 2 has read: per shard, every intact record on disk, each
/// with the generation and file it came from.
struct Scan {
    report: Report,
    /// Per shard: `(seq, generation, file index, effect)`.
    buckets: Vec<Vec<(u64, u64, usize, Owned)>>,
    /// Per shard: damage may have taken records of it that nothing else on
    /// disk shows missing.
    damaged: Vec<bool>,
    /// Intact records read so far, over every segment in order: where a
    /// record or a hole sits in the node's log.
    position: u64,
    /// Per shard: the position of its record with the highest sequence.
    last: Vec<Option<(u64, u64)>>,
    /// The position of the latest damage: before the record read there,
    /// or at a segment's end.
    last_hole: Option<u64>,
    /// A loss nobody can attribute to a shard: every shard may have paid.
    unattributed_loss: bool,
    /// Per segment file: damage was met reading it. Such a file is never
    /// garbage: see [`remove_garbage`].
    unread: Vec<bool>,
}

impl Scan {
    fn new(shards: u16, files: usize) -> Self {
        Self {
            report: Report::default(),
            buckets: (0..shards).map(|_| Vec::new()).collect(),
            damaged: vec![false; usize::from(shards)],
            position: 0,
            last: vec![None; usize::from(shards)],
            last_hole: None,
            unattributed_loss: false,
            unread: vec![false; files],
        }
    }

    /// Reads one segment, the `index`th, into the buckets.
    fn segment<D: Disk>(
        &mut self,
        disk: &D,
        wal: &Path,
        mode: ReaderMode,
        file: &SegmentFile,
        index: usize,
    ) -> io::Result<()> {
        self.report.segments += 1;
        let path = wal.join(&file.name);
        let (Ok(len), Ok(mut src)) = (disk.len(&path), disk.open_read(&path)) else {
            self.abandon(index);
            return Ok(());
        };
        // Shorter than a header: a creation that failed before its header
        // was whole. Nothing is appended to a segment before its header is
        // synced, so it never held a record — not damage, and nothing lost.
        if len < SEGMENT_HEADER_LEN as u64 {
            return Ok(());
        }
        let mut header = [0u8; SEGMENT_HEADER_LEN];
        let read = read_fully(&mut src, &mut header);
        match decode_segment_header(&header[..read]) {
            Ok(_) => {}
            Err(HeaderError::NewerVersion(version)) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "{}: format version {version} is newer than this build reads",
                        file.name
                    ),
                ));
            }
            Err(HeaderError::OlderVersion(version)) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "{}: format version {version} predates this build's layout; \
                         the directory was written by an earlier build and is not read",
                        file.name
                    ),
                ));
            }
            Err(HeaderError::Short | HeaderError::BadMagic | HeaderError::BadChecksum) => {
                self.abandon(index);
                return Ok(());
            }
        }
        let body_len = len.saturating_sub(SEGMENT_HEADER_LEN as u64);
        let mut reader = Reader::new(src, body_len, mode);
        let mut holes = 0;
        loop {
            match reader.next_record() {
                Ok(Some(item)) => {
                    // A hole the reader stepped over to reach this record
                    // lies before it.
                    if reader.damage().holes > holes {
                        holes = reader.damage().holes;
                        self.last_hole = Some(self.position);
                    }
                    self.item(&item, file.generation, index);
                }
                Ok(None) => break,
                Err(_) => {
                    self.abandon(index);
                    break;
                }
            }
        }
        let damage = reader.damage();
        self.report.damage_bytes += damage.bytes + damage.truncated_tail;
        self.report.holes += damage.holes;
        if damage.abandoned {
            self.abandon(index);
        }
        if damage.bytes > 0 || damage.holes > 0 || damage.truncated_tail > 0 {
            self.unread[index] = true;
        }
        // Damage at the segment's end — a hole nothing intact followed, or
        // a cut tail, which can be a damaged length on the last record
        // rather than a crash — lies after every record read so far.
        if damage.holes > holes || damage.truncated_tail > 0 {
            self.last_hole = Some(self.position);
        }
        Ok(())
    }

    /// Charges the latest hole to every shard whose highest record precedes
    /// it, or that has none: those are the shards it may have taken records
    /// from with nothing left to show for it.
    ///
    /// One file holds every shard's records, so a hole could have held any
    /// of them. A shard with an intact record after it is not charged: its
    /// sequences are contiguous, so whatever the hole took from it is a gap
    /// below that record, which its prefix finds and [`replay_tails`]
    /// reports as a loss because the disk had damage. The highest sequence
    /// and not merely the last record read, so that an older record
    /// written again after the hole does not stand in for the ones it took.
    fn charge_holes(&mut self) {
        let Some(hole) = self.last_hole else {
            return;
        };
        for (damaged, last) in self.damaged.iter_mut().zip(&self.last) {
            if last.is_none_or(|(_, at)| at < hole) {
                *damaged = true;
            }
        }
    }

    /// One intact record, bucketed by shard.
    fn item(&mut self, item: &Item, generation: u64, file: usize) {
        self.report.records += 1;
        let at = self.position;
        self.position += 1;
        let Some(bucket) = self.buckets.get_mut(usize::from(item.shard)) else {
            self.report.malformed += 1;
            return;
        };
        let last = &mut self.last[usize::from(item.shard)];
        if last.is_none_or(|(seq, _)| item.seq >= seq) {
            *last = Some((item.seq, at));
        }
        if let Some(effect) = Effect::decode(&item.payload) {
            bucket.push((item.seq, generation, file, effect.to_owned()));
        } else {
            // A well-checksummed record this build cannot read ends the
            // shard's prefix where it sits: leaving its sequence out of the
            // bucket is exactly a gap, and one recovery knows the cause of.
            self.report.malformed += 1;
            self.damaged[usize::from(item.shard)] = true;
            self.unread[file] = true;
        }
    }

    /// A segment, or the rest of one, that could not be read.
    fn abandon(&mut self, index: usize) {
        self.report.abandoned_segments += 1;
        self.unattributed_loss = true;
        self.unread[index] = true;
    }

    /// Per shard, every `(generation, seq)` a `Rebase` was read at.
    fn rebases(&self) -> Vec<Vec<(u64, u64)>> {
        self.buckets
            .iter()
            .map(|bucket| {
                bucket
                    .iter()
                    .filter(|(_, _, _, effect)| matches!(effect, Owned::Rebase))
                    .map(|(seq, generation, _, _)| (*generation, *seq))
                    .collect()
            })
            .collect()
    }
}

/// What pass 3 chose, per shard.
struct Chosen {
    /// The base of the image the shard took, if it took one.
    images: Vec<Option<u64>>,
    /// The highest base of an image of the shard that was refused. The
    /// shard is lossy if the log it falls back to stops short of it: what
    /// the image covered may have been compacted away.
    refused: Vec<Option<u64>>,
}

/// What reading one snapshot file concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// Footer present, counts matching, no damage met: the image stands.
    Usable,
    /// No footer and nothing wrong before where the file stops: a cycle a
    /// crash interrupted, or a finished file whose footer was lost — which
    /// nothing on disk tells apart. Refused, and a loss only where the log
    /// stops short of its base, as for [`Verdict::Damaged`].
    Unfinished,
    /// Anything else: refused, and damage that may explain a loss — the
    /// shards it imaged are lossy unless the log still reaches its base.
    Damaged,
}

/// Pass 3: the newest usable image of every shard, streamed into its dict.
fn read_images<D: Disk>(
    spec: &RecoverSpec<'_, D>,
    snaps: &mut [SnapFile],
    rebases: &[Vec<(u64, u64)>],
    building: &mut Building,
    report: &mut Report,
) -> Chosen {
    let shards = building.dicts.len();
    let mut chosen = Chosen {
        images: vec![None; shards],
        refused: vec![None; shards],
    };
    for snap in snaps.iter_mut() {
        let Some(header) = &snap.header else {
            continue; // refused in pass 1
        };
        let wanted: Vec<(u16, u64)> = header
            .bases
            .iter()
            .copied()
            .filter(|(shard, base)| {
                chosen.images[usize::from(*shard)].is_none()
                    && !is_dead(header.generation, *base, &rebases[usize::from(*shard)])
            })
            .collect();
        if wanted.is_empty() {
            continue;
        }
        let verdict = read_image(spec, &snap.name, header, &wanted, building);
        if verdict == Verdict::Usable {
            for (shard, base) in &wanted {
                chosen.images[usize::from(*shard)] = Some(*base);
            }
            snap.used = true;
            report.snapshots_used += 1;
        } else {
            for (shard, base) in &wanted {
                building.dicts[usize::from(*shard)].clear();
                building.due[usize::from(*shard)].clear();
                let refused = &mut chosen.refused[usize::from(*shard)];
                *refused = (*refused).max(Some(*base));
            }
            snap.damaged |= verdict == Verdict::Damaged;
            if verdict == Verdict::Unfinished {
                snap.unfinished_for.clone_from(&wanted);
            }
            report.snapshots_refused += 1;
        }
    }
    chosen
}

/// A snapshot of `generation` with `base` is dead for a shard when a
/// newer generation rebased it below the base — see the module doc.
fn is_dead(generation: u64, base: u64, rebases: &[(u64, u64)]) -> bool {
    rebases
        .iter()
        .any(|(newer, at)| *newer > generation && *at < base)
}

/// Reads one snapshot's entries into the dicts of `wanted`, and says
/// whether the file is usable.
fn read_image<D: Disk>(
    spec: &RecoverSpec<'_, D>,
    name: &str,
    header: &SnapshotHeader,
    wanted: &[(u16, u64)],
    building: &mut Building,
) -> Verdict {
    let path = spec.wal.join(name);
    let header_len = header.encoded_len();
    let (Ok(len), Ok(mut src)) = (spec.disk.len(&path), spec.disk.open_read(&path)) else {
        return Verdict::Damaged;
    };
    let mut skip = vec![0u8; header_len];
    if read_fully(&mut src, &mut skip) != header_len {
        return Verdict::Damaged;
    }
    let mut reader = Reader::new(src, len.saturating_sub(header_len as u64), spec.reader);
    let mut counts: Vec<(u16, u64)> = header.bases.iter().map(|(shard, _)| (*shard, 0)).collect();
    let mut footer: Option<Footer> = None;
    let mut clean = true;
    while clean {
        let item = match reader.next_record() {
            Ok(Some(item)) => item,
            Ok(None) => break,
            Err(_) => {
                clean = false;
                break;
            }
        };
        clean = if footer.is_some() {
            false // nothing follows the footer
        } else if item.shard == FOOTER_SHARD {
            footer = Footer::decode(&item.payload);
            footer.is_some()
        } else {
            take_entry(spec.now, &item, wanted, &mut counts, building)
        };
    }
    let damage = reader.damage();
    // A torn tail is what a crash leaves of entries not yet synced, so it
    // only counts against a file that claims to be finished.
    let damaged_inside = damage.bytes > 0 || damage.holes > 0 || damage.abandoned;
    let torn = damage.truncated_tail > 0;
    match footer {
        // The plant takes a file with no footer at its word, damage and
        // all — what a recovery that read an image up to wherever it
        // stopped would do. A file *with* a footer is held to the honest
        // rule either way.
        None if spec.trust_unfinished => Verdict::Usable,
        None if clean && !damaged_inside => Verdict::Unfinished,
        Some(footer) if clean && !damaged_inside && !torn && footer.counts == counts => {
            Verdict::Usable
        }
        _ => Verdict::Damaged,
    }
}

/// One entry of a snapshot: counted, and inserted if its shard wants this
/// image. `false` if it is not an entry this file can hold.
fn take_entry(
    now: Now,
    item: &Item,
    wanted: &[(u16, u64)],
    counts: &mut [(u16, u64)],
    building: &mut Building,
) -> bool {
    let Some(count) = counts.iter_mut().find(|(shard, _)| *shard == item.shard) else {
        return false; // a shard the header did not list
    };
    count.1 += 1;
    let Some(Effect::Put {
        key,
        value,
        deadline,
    }) = Effect::decode(&item.payload)
    else {
        return false;
    };
    if !wanted.iter().any(|(shard, _)| *shard == item.shard) {
        return true;
    }
    let (expires_at, passed) = match deadline.map(|at| now.replay_deadline(at)) {
        None => (None, false),
        Some(Replayed::At(at)) => (at, false),
        Some(Replayed::Past) => (Some(now.instant), true),
    };
    let key = Bytes::copy_from_slice(key);
    let shard = usize::from(item.shard);
    if passed {
        building.due[shard].push(key.clone());
    }
    building.dicts[shard].insert(
        key,
        Entry {
            value: Bytes::copy_from_slice(value),
            expires_at,
            touched: 0,
        },
    );
    true
}

/// Pass 4: every shard's tail replayed over its image. Returns the shards
/// and, per segment file, whether a kept record came from it.
fn replay_tails<D: Disk>(
    spec: &RecoverSpec<'_, D>,
    scan: Scan,
    chosen: &Chosen,
    building: Building,
    files: usize,
    report: &mut Report,
) -> (Vec<RecoveredShard>, Vec<bool>) {
    let mut kept = vec![false; files];
    report.segments = scan.report.segments;
    report.records = scan.report.records;
    report.malformed = scan.report.malformed;
    report.damage_bytes = scan.report.damage_bytes;
    report.holes = scan.report.holes;
    report.abandoned_segments = scan.report.abandoned_segments;
    let Building { dicts, due } = building;
    let mut shards = Vec::with_capacity(dicts.len());
    let zipped = dicts
        .into_iter()
        .zip(due)
        .zip(scan.buckets)
        .zip(scan.damaged);
    for (shard, (((mut dict, due), bucket), damaged)) in (0u16..).zip(zipped) {
        let index = usize::from(shard);
        let base = chosen.images[index].unwrap_or(0);
        let (records, discarded) = prefix(bucket, base, &mut kept);
        let applied = records.len() as u64;
        report.applied += applied;
        report.discarded += discarded;
        let gap = discarded > 0;
        if gap {
            report.truncated.push(ShardTruncation {
                shard,
                applied,
                discarded,
            });
        }
        let mut seq = base;
        replay_into(&mut dict, &mut seq, records, spec.now, due);
        // A refused image is a loss only where the log does not reach what
        // it covered: short of its base, those records may be gone.
        let short_of_refused = chosen.refused[index].is_some_and(|refused| seq < refused);
        // A gap where the disk had damage is records the damage took.
        let lossy = damaged
            || scan.unattributed_loss
            || short_of_refused
            || (gap && scan.last_hole.is_some());
        shards.push(RecoveredShard {
            dict,
            seq,
            lossy,
            cut: gap || lossy,
        });
    }
    (shards, kept)
}

/// A shard's records from `start` upwards to the first missing sequence,
/// and how many came after it — once every record a newer generation's
/// `Rebase` made dead is gone. Marks in `kept` the files the returned
/// records came from, and the file of every `Rebase`, which must outlive
/// the records it kills.
fn prefix(
    bucket: Vec<(u64, u64, usize, Owned)>,
    start: u64,
    kept: &mut [bool],
) -> (Vec<(u64, Owned)>, u64) {
    let rebases: Vec<(u64, u64)> = bucket
        .iter()
        .filter(|(_, _, _, effect)| matches!(effect, Owned::Rebase))
        .map(|(seq, generation, file, _)| {
            kept[*file] = true;
            (*seq, *generation)
        })
        .collect();
    let mut live: Vec<(u64, u64, usize, Owned)> = bucket
        .into_iter()
        .filter(|(seq, generation, _, _)| {
            *seq >= start
                && !rebases
                    .iter()
                    .any(|(from, newer)| newer > generation && seq >= from)
        })
        .collect();
    // By sequence, the newest generation first within one: the duplicate
    // below is then the older record, and it is the one skipped.
    live.sort_by_key(|(seq, generation, _, _)| (*seq, std::cmp::Reverse(*generation)));
    let mut records = Vec::with_capacity(live.len());
    let mut expected = start;
    let mut discarded = 0u64;
    for (seq, _, file, effect) in live {
        if discarded > 0 || seq > expected {
            discarded += 1;
        } else if seq == expected {
            kept[file] = true;
            records.push((seq, effect));
            expected += 1;
        }
        // Below `expected`: the same record written twice, by a write that
        // failed part-way and was retried whole, or an older generation's
        // record under a newer one's.
    }
    (records, discarded)
}

/// Pass 5: remove every snapshot nothing used and every segment no kept
/// record came from — of the files read whole without damage. A removal
/// that fails is left for the next start.
///
/// A file damage was met in stays: a read can fail where the next one
/// succeeds, and what could not be read may be the only copy of records
/// no other file holds — a finished image whose log a compaction already
/// removed, a segment whose header failed this once. Removing it would
/// turn a loss this start reported into one the next start cannot see.
/// Once every executor of this process has a durable snapshot, the
/// writer removes every older generation's files, these among them.
fn remove_garbage<D: Disk>(
    spec: &RecoverSpec<'_, D>,
    snaps: &[SnapFile],
    segments: &[SegmentFile],
    kept: &[bool],
    unread: &[bool],
    report: &mut Report,
) {
    let garbage = snaps
        .iter()
        .filter(|snap| !snap.used && !snap.damaged)
        .map(|snap| snap.name.as_str())
        .chain(
            segments
                .iter()
                .zip(kept.iter().zip(unread))
                .filter(|(_, (kept, unread))| !**kept && !**unread)
                .map(|(file, _)| file.name.as_str()),
        );
    let mut removed = 0;
    for name in garbage {
        if spec.disk.remove_file(&spec.wal.join(name)).is_ok() {
            removed += 1;
        }
    }
    if removed > 0 {
        // Best effort: an entry the sync did not make durable is found and
        // removed again by the next start.
        let _ = spec.disk.sync_dir(spec.wal);
    }
    report.files_removed = removed;
}

fn read_fully<R: io::Read>(src: &mut R, buf: &mut [u8]) -> usize {
    let mut got = 0;
    while got < buf.len() {
        match src.read(&mut buf[got..]) {
            Ok(0) | Err(_) => break,
            Ok(n) => got += n,
        }
    }
    got
}

#[cfg(test)]
mod tests;
