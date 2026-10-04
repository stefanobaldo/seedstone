//! The filesystem seam: the handful of operations the log needs, with a
//! `std::fs` implementation here and the simulator's elsewhere.
//!
//! A trait over *our* operations rather than over a file abstraction, on
//! purpose. The simulator's filesystem is a type swap that panics outside a
//! simulation, so production and simulation cannot share one file type; but
//! its `File` implements the real `std::io` traits, so what has to be
//! abstracted is only the nine verbs below, not a filesystem.
//!
//! A verb added here is a review question: the seam is narrow so that a
//! reader can hold in one sitting everything the log can do to a disk.

use std::future::Future;
use std::io::{self, Read, Write};
use std::path::Path;
use std::pin::Pin;

/// A sync that completes later: the executor's, issued and awaited beside
/// the batches that follow it.
pub type SyncFuture = Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'static>>;

/// An open, append-only file the log writes records into.
pub trait LogFile: Send + 'static {
    /// Appends `bytes`, all of them or an error.
    ///
    /// # Errors
    ///
    /// Whatever the store reports; the caller keeps its bytes and retries.
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// Makes every byte written so far durable, now, on this thread. The
    /// start path's and the checkpoint's: a few calls per cycle, never on
    /// the request path.
    ///
    /// # Errors
    ///
    /// Whatever the store reports; nothing written since the last
    /// successful call may be assumed durable.
    fn sync_data(&mut self) -> io::Result<()>;

    /// Makes every byte written *before this call* durable, on some other
    /// thread or at some later instant, and says when it has. Bytes written
    /// after the call may or may not be covered; the caller accounts only
    /// for what was written before it.
    ///
    /// # Errors
    ///
    /// Through the future: whatever the store reports. The future itself is
    /// infallible to create — a handle that could not be cloned reports it
    /// as the sync's failure.
    fn sync_later(&self) -> SyncFuture;
}

/// The verbs the log needs from a filesystem.
pub trait Disk {
    /// The file type appends go to.
    type File: LogFile;
    /// The file type recovery reads from.
    type ReadFile: Read;

    /// Creates `dir` and every missing parent.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    fn create_dir_all(&self, dir: &Path) -> io::Result<()>;

    /// Makes `dir`'s entries durable: a file created and synced whose
    /// directory entry was not is an orphan after a crash.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;

    /// The names of the files in `dir`, in no particular order.
    ///
    /// # Errors
    ///
    /// Whatever the store reports, including `dir` not existing.
    fn list(&self, dir: &Path) -> io::Result<Vec<String>>;

    /// Opens `path` for appending, creating it if it is not there.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    fn create_append(&self, path: &Path) -> io::Result<Self::File>;

    /// Opens `path` for reading from its start.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    fn open_read(&self, path: &Path) -> io::Result<Self::ReadFile>;

    /// How many bytes `path` holds.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    fn len(&self, path: &Path) -> io::Result<u64>;

    /// Writes `bytes` as the whole content of `path`, creating or
    /// truncating it, and syncs the file — not the directory.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    fn write_file(&self, path: &Path, bytes: &[u8]) -> io::Result<()>;

    /// Renames `from` to `to`, replacing `to` if it exists.
    ///
    /// # Errors
    ///
    /// Whatever the store reports.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Removes `path`.
    ///
    /// The one thing the log deletes, and it is a whole file it wrote
    /// itself: a segment or a snapshot a durable snapshot has made
    /// redundant. The directory is not synced here — the caller syncs once
    /// after every removal of a batch.
    ///
    /// # Errors
    ///
    /// Whatever the store reports, `NotFound` included: the caller counts
    /// what it removed, and a file that was already gone is not one of them.
    fn remove_file(&self, path: &Path) -> io::Result<()>;
}

/// The real filesystem.
#[derive(Debug, Clone, Copy, Default)]
pub struct StdDisk;

impl LogFile for std::fs::File {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        Write::write_all(self, bytes)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        Self::sync_data(self)
    }

    /// `fdatasync` on tokio's blocking pool, over a duplicate descriptor:
    /// the executor keeps appending to the page cache through its own
    /// while the kernel flushes, and a flush covers everything written
    /// before it was asked for, which is all the caller accounts for.
    fn sync_later(&self) -> SyncFuture {
        let handle = self.try_clone();
        Box::pin(async move {
            let handle = handle?;
            tokio::task::spawn_blocking(move || handle.sync_data())
                .await
                .map_err(|join| {
                    io::Error::other(format!("the sync thread did not finish: {join}"))
                })?
        })
    }
}

impl Disk for StdDisk {
    type File = std::fs::File;
    type ReadFile = std::fs::File;

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dir)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        std::fs::File::open(dir)?.sync_all()
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        Ok(names)
    }

    fn create_append(&self, path: &Path) -> io::Result<Self::File> {
        std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
    }

    fn open_read(&self, path: &Path) -> io::Result<Self::ReadFile> {
        std::fs::File::open(path)
    }

    fn len(&self, path: &Path) -> io::Result<u64> {
        Ok(std::fs::metadata(path)?.len())
    }

    fn write_file(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut file = std::fs::File::create(path)?;
        Write::write_all(&mut file, bytes)?;
        file.sync_data()
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }
}

/// A filesystem in a map, for the tests of everything above the seam.
///
/// It exists so that the log, the reader and recovery can be tested with
/// damage placed byte by byte, on a store that can be told to fail its
/// writes, without a temporary directory and without the simulator.
#[cfg(test)]
pub(crate) mod mem {
    use super::{Disk, LogFile, SyncFuture};
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::{self, Cursor};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MemFs {
        dirs: BTreeSet<PathBuf>,
        files: BTreeMap<PathBuf, Vec<u8>>,
        /// How many bytes of each file the last successful sync covered.
        synced: BTreeMap<PathBuf, usize>,
        /// How many syncs of each file succeeded.
        syncs: BTreeMap<PathBuf, u32>,
        /// How long a deferred sync takes to answer: zero unless a test
        /// holds a round in flight.
        sync_latency: std::time::Duration,
        fail_writes: bool,
        /// The kind a failed write reports: `Other` unless a test names one.
        write_kind: WriteKind,
        fail_syncs: bool,
        fail_removes: bool,
        /// Whether creating a file fails.
        fail_creates: bool,
        /// File syncs to let through before one fails, once.
        sync_fails_after: Option<u32>,
        /// Directory syncs to let through before one fails, once.
        dir_sync_fails_after: Option<u32>,
    }

    /// `io::ErrorKind` with a default, so that `MemFs` can derive its own.
    #[derive(Clone, Copy)]
    struct WriteKind(io::ErrorKind);

    impl Default for WriteKind {
        fn default() -> Self {
            Self(io::ErrorKind::Other)
        }
    }

    impl MemFs {
        fn write_failure(&self) -> io::Error {
            io::Error::new(self.write_kind.0, "injected write failure")
        }

        /// Records that `path`'s current bytes are synced.
        fn mark_synced(&mut self, path: &Path) {
            let len = self.files.get(path).map_or(0, Vec::len);
            self.synced.insert(path.to_path_buf(), len);
            *self.syncs.entry(path.to_path_buf()).or_default() += 1;
        }

        /// Whether the next file sync fails, counting the one-shot down.
        fn sync_fails(&mut self) -> bool {
            self.fail_writes || self.fail_syncs || countdown(&mut self.sync_fails_after)
        }
    }

    /// Counts a sync down: `true` when this is the one that fails.
    fn countdown(slot: &mut Option<u32>) -> bool {
        match slot {
            Some(0) => {
                *slot = None;
                true
            }
            Some(left) => {
                *left -= 1;
                false
            }
            None => false,
        }
    }

    /// The map, shared by every handle onto it.
    #[derive(Clone, Default)]
    pub struct MemDisk(Arc<Mutex<MemFs>>);

    /// An append handle: the disk and the path it appends to.
    pub struct MemFile {
        disk: MemDisk,
        path: PathBuf,
    }

    impl MemDisk {
        fn lock(&self) -> std::sync::MutexGuard<'_, MemFs> {
            self.0.lock().expect("mem disk")
        }

        /// What `path` holds right now.
        pub fn contents(&self, path: &Path) -> Vec<u8> {
            self.lock().files.get(path).cloned().unwrap_or_default()
        }

        /// How many of `path`'s bytes the last successful sync of it
        /// covered.
        pub fn synced_len(&self, path: &Path) -> usize {
            self.lock().synced.get(path).copied().unwrap_or(0)
        }

        /// How many syncs of `path` succeeded.
        pub fn sync_count(&self, path: &Path) -> u32 {
            self.lock().syncs.get(path).copied().unwrap_or(0)
        }

        /// Makes every deferred sync answer `latency` after its call, on
        /// tokio's clock: how a test holds a round in flight.
        pub fn set_sync_latency(&self, latency: std::time::Duration) {
            self.lock().sync_latency = latency;
        }

        /// Makes the next file sync fail, once.
        pub fn fail_next_sync(&self) {
            self.fail_one_sync_after(0);
        }

        /// Makes every file creation fail, or none.
        pub fn fail_creates(&self, fail: bool) {
            self.lock().fail_creates = fail;
        }

        /// Replaces what `path` holds — how a test plants damage.
        pub fn overwrite(&self, path: &Path, bytes: Vec<u8>) {
            self.lock().files.insert(path.to_path_buf(), bytes);
        }

        /// Whether every write from now on fails.
        pub fn fail_writes(&self, fail: bool) {
            let mut fs = self.lock();
            fs.fail_writes = fail;
            fs.write_kind = WriteKind::default();
        }

        /// Every write from now on fails with `kind`: how a test fills the
        /// disk (`StorageFull`) rather than breaks it.
        pub fn fail_writes_with(&self, kind: io::ErrorKind) {
            let mut fs = self.lock();
            fs.fail_writes = true;
            fs.write_kind = WriteKind(kind);
        }

        /// Whether every file sync from now on fails, writes still landing:
        /// how a test fails a sync alone.
        pub fn fail_syncs(&self, fail: bool) {
            self.lock().fail_syncs = fail;
        }

        /// Whether every removal from now on fails.
        pub fn fail_removes(&self, fail: bool) {
            self.lock().fail_removes = fail;
        }

        /// The file sync after the next `skip` fails, once: how a test fails
        /// one step of a sequence of syncs and lets the rest succeed.
        pub fn fail_one_sync_after(&self, skip: u32) {
            self.lock().sync_fails_after = Some(skip);
        }

        /// The directory sync after the next `skip` fails, once.
        pub fn fail_one_dir_sync_after(&self, skip: u32) {
            self.lock().dir_sync_fails_after = Some(skip);
        }
    }

    impl LogFile for MemFile {
        fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
            let mut fs = self.disk.lock();
            if fs.fail_writes {
                return Err(fs.write_failure());
            }
            fs.files
                .entry(self.path.clone())
                .or_default()
                .extend_from_slice(bytes);
            drop(fs);
            Ok(())
        }

        fn sync_data(&mut self) -> io::Result<()> {
            let mut fs = self.disk.lock();
            if fs.sync_fails() {
                return Err(io::Error::other("injected sync failure"));
            }
            fs.mark_synced(&self.path);
            drop(fs);
            Ok(())
        }

        /// The same decision as [`sync_data`](LogFile::sync_data), taken at
        /// the call, answered after the disk's latency — at once unless a
        /// test set one.
        fn sync_later(&self) -> SyncFuture {
            let mut fs = self.disk.lock();
            let result = if fs.sync_fails() {
                Err(io::Error::other("injected sync failure"))
            } else {
                fs.mark_synced(&self.path);
                Ok(())
            };
            let latency = fs.sync_latency;
            drop(fs);
            if latency.is_zero() {
                return Box::pin(std::future::ready(result));
            }
            Box::pin(async move {
                tokio::time::sleep(latency).await;
                result
            })
        }
    }

    impl Disk for MemDisk {
        type File = MemFile;
        type ReadFile = Cursor<Vec<u8>>;

        fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
            self.lock().dirs.insert(dir.to_path_buf());
            Ok(())
        }

        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            let mut fs = self.lock();
            if countdown(&mut fs.dir_sync_fails_after) {
                return Err(io::Error::other("injected directory sync failure"));
            }
            if fs.dirs.contains(dir) {
                Ok(())
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "no such directory"))
            }
        }

        fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
            let fs = self.lock();
            if !fs.dirs.contains(dir) {
                return Err(io::Error::new(io::ErrorKind::NotFound, "no such directory"));
            }
            Ok(fs
                .files
                .keys()
                .filter(|path| path.parent() == Some(dir))
                .filter_map(|path| path.file_name())
                .map(|name| name.to_string_lossy().into_owned())
                .collect())
        }

        fn create_append(&self, path: &Path) -> io::Result<Self::File> {
            let mut fs = self.lock();
            if fs.fail_creates {
                return Err(io::Error::other("injected create failure"));
            }
            fs.files.entry(path.to_path_buf()).or_default();
            drop(fs);
            Ok(MemFile {
                disk: self.clone(),
                path: path.to_path_buf(),
            })
        }

        fn open_read(&self, path: &Path) -> io::Result<Self::ReadFile> {
            self.lock()
                .files
                .get(path)
                .cloned()
                .map(Cursor::new)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))
        }

        fn len(&self, path: &Path) -> io::Result<u64> {
            self.lock()
                .files
                .get(path)
                .map(|bytes| bytes.len() as u64)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))
        }

        fn write_file(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
            let mut fs = self.lock();
            if fs.fail_writes {
                return Err(fs.write_failure());
            }
            fs.files.insert(path.to_path_buf(), bytes.to_vec());
            drop(fs);
            Ok(())
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            let mut fs = self.lock();
            let bytes = fs
                .files
                .remove(from)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))?;
            fs.files.insert(to.to_path_buf(), bytes);
            drop(fs);
            Ok(())
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            let mut fs = self.lock();
            if fs.fail_removes {
                return Err(io::Error::other("injected removal failure"));
            }
            fs.files
                .remove(path)
                .map(|_| ())
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::mem::MemDisk;
    use super::*;

    /// Every verb, against the real filesystem in a temporary directory.
    #[test]
    fn the_std_disk_does_what_each_verb_says() {
        let dir = std::env::temp_dir().join(format!("seedstone-disk-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let disk = StdDisk;
        let wal = dir.join("wal");
        disk.create_dir_all(&wal).unwrap();
        disk.sync_dir(&wal).unwrap();
        assert!(disk.list(&wal).unwrap().is_empty());

        let path = wal.join("a.seg");
        let mut file = disk.create_append(&path).unwrap();
        LogFile::write_all(&mut file, b"hello").unwrap();
        LogFile::sync_data(&mut file).unwrap();
        drop(file);
        let mut again = disk.create_append(&path).unwrap();
        LogFile::write_all(&mut again, b" world").unwrap();
        drop(again);
        assert_eq!(disk.len(&path).unwrap(), 11);
        let mut read = String::new();
        disk.open_read(&path)
            .unwrap()
            .read_to_string(&mut read)
            .unwrap();
        assert_eq!(read, "hello world");

        disk.write_file(&wal.join("g.tmp"), b"7").unwrap();
        disk.rename(&wal.join("g.tmp"), &wal.join("g")).unwrap();
        let mut names = disk.list(&wal).unwrap();
        names.sort();
        assert_eq!(names, ["a.seg", "g"]);

        disk.remove_file(&wal.join("g")).unwrap();
        assert_eq!(disk.list(&wal).unwrap(), ["a.seg"]);
        assert_eq!(
            disk.remove_file(&wal.join("g")).unwrap_err().kind(),
            io::ErrorKind::NotFound,
            "removing what is not there is an error, not a no-op: the caller counts"
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The in-memory disk agrees with the real one on the same script.
    #[test]
    fn the_mem_disk_does_what_each_verb_says() {
        let disk = MemDisk::default();
        let wal = Path::new("/data/wal");
        assert!(disk.sync_dir(wal).is_err(), "no directory yet");
        disk.create_dir_all(wal).unwrap();
        disk.sync_dir(wal).unwrap();

        let path = wal.join("a.seg");
        let mut file = disk.create_append(&path).unwrap();
        file.write_all(b"hello").unwrap();
        let mut again = disk.create_append(&path).unwrap();
        again.write_all(b" world").unwrap();
        assert_eq!(disk.len(&path).unwrap(), 11);
        assert_eq!(disk.contents(&path), b"hello world");

        disk.write_file(&wal.join("g.tmp"), b"7").unwrap();
        disk.rename(&wal.join("g.tmp"), &wal.join("g")).unwrap();
        let mut names = disk.list(wal).unwrap();
        names.sort();
        assert_eq!(names, ["a.seg", "g"]);

        disk.remove_file(&wal.join("g")).unwrap();
        assert_eq!(disk.list(wal).unwrap(), ["a.seg"]);
        assert_eq!(
            disk.remove_file(&wal.join("g")).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        disk.fail_removes(true);
        assert!(
            disk.remove_file(&path).is_err(),
            "an injected removal failure"
        );
        assert_eq!(disk.contents(&path), b"hello world", "and the file stays");
        disk.fail_removes(false);

        disk.fail_writes(true);
        assert!(file.write_all(b"!").is_err());
        assert_eq!(
            disk.contents(&path),
            b"hello world",
            "a failed write wrote nothing"
        );

        disk.overwrite(&path, b"hellX world".to_vec());
        assert_eq!(
            disk.contents(&path),
            b"hellX world",
            "damage is planted in place"
        );
    }

    /// The std disk's deferred sync runs off the caller's thread and
    /// resolves; the file is still appendable while it does.
    #[tokio::test]
    async fn the_std_disk_syncs_later_and_the_file_stays_writable() {
        let dir = std::env::temp_dir().join(format!("seedstone-sync-later-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut file = StdDisk.create_append(&dir.join("a.seg")).unwrap();
        LogFile::write_all(&mut file, b"one").unwrap();
        let pending = file.sync_later();
        LogFile::write_all(&mut file, b"two").unwrap();
        pending.await.unwrap();
        assert_eq!(StdDisk.len(&dir.join("a.seg")).unwrap(), 6);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The mem disk's deferred sync answers what its blocking one would,
    /// including the one-shot countdown.
    #[tokio::test]
    async fn the_mem_disk_syncs_later_with_the_same_failures() {
        let disk = MemDisk::default();
        let wal = Path::new("/data/wal");
        disk.create_dir_all(wal).unwrap();
        let file = disk.create_append(&wal.join("a.seg")).unwrap();
        assert!(file.sync_later().await.is_ok());
        disk.fail_one_sync_after(0);
        assert!(file.sync_later().await.is_err(), "the one that fails");
        assert!(file.sync_later().await.is_ok(), "and the next does not");
        disk.fail_writes_with(io::ErrorKind::StorageFull);
        let mut full = disk.create_append(&wal.join("b.seg")).unwrap();
        assert_eq!(
            full.write_all(b"x").unwrap_err().kind(),
            io::ErrorKind::StorageFull,
            "a full disk is a kind a test can name"
        );
    }
}
