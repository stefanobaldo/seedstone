//! The simulator's side of the filesystem seam.
//!
//! turmoil's filesystem is a type swap that panics outside a simulation, so
//! nothing here may be reached from production — the crate boundary is the
//! guarantee. Its `File` implements the real `std::io` traits, which is why
//! the seam is a handful of verbs and not a file abstraction.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::RngExt;
use rand::rngs::ChaCha8Rng;
use seedstone_core::log::disk::{Disk, LogFile, SyncFuture};
use turmoil::fs::shim::std::fs::{self as sim_fs, File, OpenOptions};

use crate::outcome::lock;

/// The simulated filesystem of the current host.
///
/// A deferred sync waits a latency drawn from the run's own stream before
/// turmoil syncs the file, so that a crash can land while one is in
/// flight; the inline sync and every other verb answer at once. A sync of
/// either kind may fail, at a rate drawn from the same stream.
#[derive(Debug, Clone, Default)]
pub struct SimDisk {
    /// The range a deferred sync's latency is drawn from, in milliseconds,
    /// both ends included.
    latency_ms: (u64, u64),
    /// Probability, in permille, that a sync — deferred or inline — fails
    /// with `EIO`. turmoil 0.7.2's `sync_data` checks no probability of its
    /// own (read against its `fs/shim/std/fs/mod.rs` on 2026-10-02), so
    /// the shape's `io_error_permille` is applied here, from the run's own
    /// stream, where the writer's rotation, the checkpoint's footer and the
    /// deferred sync all pass.
    sync_fault_permille: u16,
    /// The stream the latencies and the sync faults are drawn from, or
    /// `None` for a disk whose syncs take no time and never fail.
    rng: Option<Arc<Mutex<ChaCha8Rng>>>,
}

impl SimDisk {
    /// A disk whose deferred syncs take a latency in `latency_ms` and whose
    /// syncs fail at `sync_fault_permille`, both drawn from `rng`; with no
    /// stream, they take none and never fail.
    #[must_use]
    pub const fn new(
        latency_ms: (u64, u64),
        sync_fault_permille: u16,
        rng: Option<Arc<Mutex<ChaCha8Rng>>>,
    ) -> Self {
        Self {
            latency_ms,
            sync_fault_permille,
            rng,
        }
    }

    fn draw(&self) -> Duration {
        let Some(rng) = &self.rng else {
            return Duration::ZERO;
        };
        let (min, max) = self.latency_ms;
        Duration::from_millis(lock(rng).random_range(min..=max))
    }

    /// Whether this sync fails: one draw from the same stream the latency
    /// comes from, so a shape with no sync faults draws nothing extra.
    fn sync_fails(&self) -> bool {
        if self.sync_fault_permille == 0 {
            return false;
        }
        let Some(rng) = &self.rng else {
            return false;
        };
        lock(rng).random_range(0..1000u16) < self.sync_fault_permille
    }
}

/// A simulated file open for appending, where, and the disk it was opened
/// on.
pub struct SimFile {
    file: File,
    path: PathBuf,
    disk: SimDisk,
}

impl LogFile for SimFile {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        Write::write_all(&mut self.file, bytes)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        if self.disk.sync_fails() {
            return Err(io::Error::other("injected sync failure"));
        }
        self.file.sync_data()
    }

    /// turmoil's sync, after the drawn latency, on a handle of its own: the
    /// log may rotate away from this file while the sync is in flight.
    ///
    /// A file removed meanwhile is synced successfully, as `fdatasync` on
    /// the descriptor of an unlinked file is — read on Linux 6.12.76 on
    /// 2026-10-02. turmoil resolves a handle to its path and fails a sync
    /// of one that is gone, and the log removes a rotation the checkpoint
    /// covered whether or not a sync of it is still in flight.
    ///
    /// Whether it fails is drawn at the call, before the sleep, so the
    /// stream's order does not depend on when the future is polled.
    fn sync_later(&self) -> SyncFuture {
        let latency = self.disk.draw();
        let fails = self.disk.sync_fails();
        let handle = self.file.try_clone();
        let path = self.path.clone();
        Box::pin(async move {
            tokio::time::sleep(latency).await;
            if fails {
                return Err(io::Error::other("injected sync failure"));
            }
            if !sim_fs::exists(&path) {
                return Ok(());
            }
            handle?.sync_data()
        })
    }
}

impl Disk for SimDisk {
    type File = SimFile;
    type ReadFile = File;

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        sim_fs::create_dir_all(dir)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        sim_fs::sync_dir(dir)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in sim_fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        // turmoil gathers the entries into a `HashSet`, whose order is drawn
        // per process; a listing a caller walks must be a function of the
        // seed, so it is put in name order here.
        names.sort_unstable();
        Ok(names)
    }

    fn create_append(&self, path: &Path) -> io::Result<Self::File> {
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .map(|file| SimFile {
                file,
                path: path.to_path_buf(),
                disk: self.clone(),
            })
    }

    fn open_read(&self, path: &Path) -> io::Result<Self::ReadFile> {
        File::open(path)
    }

    fn len(&self, path: &Path) -> io::Result<u64> {
        Ok(sim_fs::metadata(path)?.len())
    }

    fn write_file(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut file = File::create(path)?;
        Write::write_all(&mut file, bytes)?;
        file.sync_data()
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        sim_fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        sim_fs::remove_file(path)
    }
}
