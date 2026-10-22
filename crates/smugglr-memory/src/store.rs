//! Where memory is kept (NFR-SMUGGLR-009): the memory store interface, and
//! its file backend in smugglr's config directory. A browser tab keeps its
//! memory in its own OPFS storage through the same interface.

use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use crate::{Error, io_at};

/// Where memory keeps its files, each named by a path of `/`-separated
/// parts.
pub trait Store: std::fmt::Debug + Send + Sync {
    /// The text of the file at `path`; `None` when there is none.
    ///
    /// # Errors
    /// When the file is there and cannot be read.
    fn read(&self, path: &str) -> Result<Option<String>, Error>;

    /// Replaces the file at `path` with `text`. Once it returns the file
    /// holds `text`, and a cut at any moment leaves the old text or the new,
    /// never part of either.
    ///
    /// # Errors
    /// When the file cannot be written; it then holds the old text.
    fn replace(&self, path: &str, text: &str) -> Result<(), Error>;

    /// Adds `text` to the end of the file at `path`, making it when there is
    /// none; kept before it returns.
    ///
    /// # Errors
    /// When the file cannot be written.
    fn append(&self, path: &str, text: &str) -> Result<(), Error>;

    /// Removes the file at `path`; nothing to do when there is none.
    ///
    /// # Errors
    /// When the file is there and cannot be removed.
    fn remove(&self, path: &str) -> Result<(), Error>;
}

/// Memory's files in a folder on this machine: smugglr's config directory.
#[derive(Debug, Clone)]
pub struct Files {
    root: PathBuf,
}

impl Files {
    /// The files under `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn at(&self, path: &str) -> PathBuf {
        path.split('/')
            .fold(self.root.clone(), |at, part| at.join(part))
    }
}

impl Store for Files {
    fn read(&self, path: &str) -> Result<Option<String>, Error> {
        let path = self.at(path);
        match fs::read_to_string(&path) {
            Ok(text) => Ok(Some(text)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(Error::Io { path, source }),
        }
    }

    fn replace(&self, path: &str, text: &str) -> Result<(), Error> {
        let path = self.at(path);
        let folder = folder_of(&path);
        fs::create_dir_all(&folder).map_err(io_at(&folder))?;
        let mut next = path.clone().into_os_string();
        next.push(".next");
        let next = PathBuf::from(next);
        // The new file's data reaches the disk before the rename, and the
        // rename before replace returns, so a power cut leaves the old file
        // or the new, never an empty one.
        let mut file = fs::File::create(&next).map_err(io_at(&next))?;
        file.write_all(text.as_bytes()).map_err(io_at(&next))?;
        file.sync_all().map_err(io_at(&next))?;
        fs::rename(&next, &path).map_err(io_at(&path))?;
        sync_folder(&folder).map_err(io_at(&folder))
    }

    fn append(&self, path: &str, text: &str) -> Result<(), Error> {
        let path = self.at(path);
        let folder = folder_of(&path);
        fs::create_dir_all(&folder).map_err(io_at(&folder))?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(io_at(&path))?;
        file.write_all(text.as_bytes()).map_err(io_at(&path))?;
        file.sync_data().map_err(io_at(&path))
    }

    fn remove(&self, path: &str) -> Result<(), Error> {
        let path = self.at(path);
        match fs::remove_file(&path) {
            Err(err) if err.kind() != io::ErrorKind::NotFound => {
                Err(Error::Io { path, source: err })
            }
            _ => Ok(()),
        }
    }
}

fn folder_of(path: &Path) -> PathBuf {
    path.parent().map_or_else(PathBuf::new, Path::to_path_buf)
}

/// Flushes a folder's entries (a rename into it) to disk. Windows cannot
/// open a folder as a file, so there the step is skipped.
#[cfg(unix)]
fn sync_folder(folder: &Path) -> io::Result<()> {
    fs::File::open(folder)?.sync_all()
}

#[cfg(not(unix))]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the same signature as the unix build's"
)]
fn sync_folder(_: &Path) -> io::Result<()> {
    Ok(())
}
