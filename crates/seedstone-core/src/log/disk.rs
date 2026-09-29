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

use std::io::{self, Read, Write};
use std::path::Path;

/// An open, append-only file the log writes records into.
pub trait LogFile: Send + 'static {
    /// Appends `bytes`, all of them or an error.
    ///
    /// # Errors
    ///
    /// Whatever the store reports; the caller keeps its bytes and retries.
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// Makes every byte written so far durable.
    ///
    /// # Errors
    ///
    /// Whatever the store reports; nothing written since the last
    /// successful call may be assumed durable.
    fn sync_data(&mut self) -> io::Result<()>;
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
    use super::{Disk, LogFile};
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::{self, Cursor};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MemFs {
        dirs: BTreeSet<PathBuf>,
        files: BTreeMap<PathBuf, Vec<u8>>,
        fail_writes: bool,
        fail_syncs: bool,
        fail_removes: bool,
        /// File syncs to let through before one fails, once.
        sync_fails_after: Option<u32>,
        /// Directory syncs to let through before one fails, once.
        dir_sync_fails_after: Option<u32>,
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

        /// Replaces what `path` holds — how a test plants damage.
        pub fn overwrite(&self, path: &Path, bytes: Vec<u8>) {
            self.lock().files.insert(path.to_path_buf(), bytes);
        }

        /// Whether every write from now on fails.
        pub fn fail_writes(&self, fail: bool) {
            self.lock().fail_writes = fail;
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
                return Err(io::Error::other("injected write failure"));
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
            let failing = fs.fail_writes || fs.fail_syncs || countdown(&mut fs.sync_fails_after);
            drop(fs);
            if failing {
                return Err(io::Error::other("injected sync failure"));
            }
            Ok(())
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
            self.lock().files.entry(path.to_path_buf()).or_default();
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
                return Err(io::Error::other("injected write failure"));
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
}
