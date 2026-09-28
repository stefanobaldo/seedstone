//! The simulator's side of the filesystem seam.
//!
//! turmoil's filesystem is a type swap that panics outside a simulation, so
//! nothing here may be reached from production — the crate boundary is the
//! guarantee. Its `File` implements the real `std::io` traits, which is why
//! the seam is a handful of verbs and not a file abstraction.

use std::io::{self, Write};
use std::path::Path;

use seedstone_core::log::disk::{Disk, LogFile};
use turmoil::fs::shim::std::fs::{self as sim_fs, File, OpenOptions};

/// The simulated filesystem of the current host.
#[derive(Debug, Clone, Copy, Default)]
pub struct SimDisk;

/// A simulated file open for appending.
///
/// A newtype because the trait and the type both belong to other crates.
pub struct SimFile(File);

impl LogFile for SimFile {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        Write::write_all(&mut self.0, bytes)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.0.sync_data()
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
        Ok(names)
    }

    fn create_append(&self, path: &Path) -> io::Result<Self::File> {
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .map(SimFile)
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
}
