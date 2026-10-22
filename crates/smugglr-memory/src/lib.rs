//! smugglr's memory, in its own files and nothing in any place
//! (FR-SMUGGLR-072).
//!
//! For each pair of places, memory keeps each key's last hash from the last
//! done run, per table. The CLI and mesh nodes keep it in smugglr's config
//! directory, one folder per pair of places; the folder is named by a hash of
//! the two places' addresses, the same whichever is named first.
//!
//! While a run writes, the pair's folder also holds the run's journal,
//! `smugglr.sync` (FR-SMUGGLR-073): one line per row, holding its table,
//! side, key and from and to hashes, written and flushed before the row is.

mod device;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use smugglr_core::change::Op;
use smugglr_core::plan::{Hashes, Side, Write};
use smugglr_core::row_hash::row_hash;

pub use device::{Device, Row};

/// The file in a pair's folder that holds each key's last hash.
const LAST: &str = "last";

/// The journal of a run that is writing.
const JOURNAL: &str = "smugglr.sync";

/// What memory says when its files cannot be read or written.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A file or folder could not be read or written.
    #[error("{}: {source}", path.display())]
    Io {
        /// The file or folder.
        path: PathBuf,
        /// What the system said.
        #[source]
        source: io::Error,
    },
    /// A line of a memory file is not one smugglr wrote.
    #[error("{}: line {line} is not smugglr's memory", path.display())]
    Unreadable {
        /// The file.
        path: PathBuf,
        /// The line's number, from 1.
        line: usize,
    },
}

/// smugglr's config directory: `$XDG_CONFIG_HOME/smugglr`, else
/// `$HOME/.config/smugglr`; `None` when neither is set.
#[must_use]
pub fn config_dir() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))?;
    Some(base.join("smugglr"))
}

/// The memory of one pair of places.
#[derive(Debug)]
pub struct Memory {
    folder: PathBuf,
    tables: BTreeMap<String, Hashes>,
    /// A run of this pair that wrote saved memory, even of empty tables.
    synced: bool,
}

impl Memory {
    /// Opens the memory of the pair of places at addresses `a` and `b`, kept
    /// under `config`; a pair never synced has none.
    ///
    /// # Errors
    /// When the pair's memory cannot be read.
    pub fn open(config: &Path, a: &str, b: &str) -> Result<Self, Error> {
        let (first, second) = if a <= b { (a, b) } else { (b, a) };
        let folder = config.join(hex(&row_hash(&[first, second])));
        let path = folder.join(LAST);
        let (text, synced) = match fs::read_to_string(&path) {
            Ok(text) => (text, true),
            Err(err) if err.kind() == io::ErrorKind::NotFound => (String::new(), false),
            Err(source) => return Err(Error::Io { path, source }),
        };
        let mut tables: BTreeMap<String, Hashes> = BTreeMap::new();
        for (i, line) in text.lines().enumerate() {
            let unreadable = || Error::Unreadable {
                path: path.clone(),
                line: i + 1,
            };
            let mut fields = line.split(' ').map(unhex);
            let (Some(Some(table)), Some(Some(hash))) = (fields.next(), fields.next()) else {
                return Err(unreadable());
            };
            let table = String::from_utf8(table).map_err(|_| unreadable())?;
            let hash: [u8; 16] = hash.try_into().map_err(|_| unreadable())?;
            let key = fields
                .map(|field| field.and_then(|bytes| String::from_utf8(bytes).ok()))
                .collect::<Option<Vec<String>>>()
                .ok_or_else(unreadable)?;
            tables.entry(table).or_default().insert(key, hash);
        }
        Ok(Self {
            folder,
            tables,
            synced,
        })
    }

    /// Whether the pair has no memory yet: no run of it was ever saved. A
    /// dry run saves none, so the run after it is still a first sync.
    #[must_use]
    pub fn first_sync(&self) -> bool {
        !self.synced
    }

    /// Each key's last hash in `table`; empty when memory holds none.
    #[must_use]
    pub fn table(&self, table: &str) -> Hashes {
        self.tables.get(table).cloned().unwrap_or_default()
    }

    /// Sets each key's last hash in `table`.
    pub fn set_table(&mut self, table: &str, hashes: Hashes) {
        self.tables.insert(table.to_owned(), hashes);
    }

    /// Saves the memory in the pair's folder, replacing what was there.
    ///
    /// # Errors
    /// When the folder or its file cannot be written.
    pub fn save(&self) -> Result<(), Error> {
        fs::create_dir_all(&self.folder).map_err(io_at(&self.folder))?;
        let mut text = String::new();
        for (table, hashes) in &self.tables {
            for (key, hash) in hashes {
                text.push_str(&hex(table.as_bytes()));
                text.push(' ');
                text.push_str(&hex(hash));
                for column in key {
                    text.push(' ');
                    text.push_str(&hex(column.as_bytes()));
                }
                text.push('\n');
            }
        }
        let path = self.folder.join(LAST);
        let next = self.folder.join("last.next");
        // The new file's data reaches the disk before the rename, and the
        // rename before save returns, so a power cut leaves the old memory
        // or the new, never an empty file.
        let mut file = fs::File::create(&next).map_err(io_at(&next))?;
        file.write_all(text.as_bytes()).map_err(io_at(&next))?;
        file.sync_all().map_err(io_at(&next))?;
        fs::rename(&next, &path).map_err(io_at(&path))?;
        sync_folder(&self.folder).map_err(io_at(&self.folder))
    }

    /// Adds a line per write to the journal and flushes it to disk; call it
    /// before the rows are written.
    ///
    /// # Errors
    /// When the journal cannot be written.
    pub fn journal(&self, table: &str, writes: &[Write]) -> Result<(), Error> {
        if writes.is_empty() {
            return Ok(());
        }
        fs::create_dir_all(&self.folder).map_err(io_at(&self.folder))?;
        let path = self.folder.join(JOURNAL);
        let mut text = String::new();
        for write in writes {
            let hash = |hash: Option<[u8; 16]>| hash.map_or_else(|| "-".to_owned(), |h| hex(&h));
            let side = match write.side {
                Side::Source => "s",
                Side::Target => "t",
            };
            let _ = write!(
                text,
                "{} {side} {} {}",
                hex(table.as_bytes()),
                hash(write.from),
                hash(write.to)
            );
            for column in &write.key {
                text.push(' ');
                text.push_str(&hex(column.as_bytes()));
            }
            text.push('\n');
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(io_at(&path))?;
        file.write_all(text.as_bytes()).map_err(io_at(&path))?;
        file.sync_data().map_err(io_at(&path))
    }

    /// The journal a cut-off run left, by table; empty when there is none.
    ///
    /// # Errors
    /// When the journal cannot be read.
    pub fn journaled(&self) -> Result<Vec<(String, Write)>, Error> {
        let path = self.folder.join(JOURNAL);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => return Err(Error::Io { path, source }),
        };
        let mut lines = Vec::new();
        // A line cut off by a crash ends without its newline; its row was
        // never written, since a row is written only after its line is on
        // disk.
        let whole = text
            .split_inclusive('\n')
            .filter_map(|line| line.strip_suffix('\n'));
        for (i, line) in whole.enumerate() {
            let unreadable = || Error::Unreadable {
                path: path.clone(),
                line: i + 1,
            };
            let mut fields = line.split(' ');
            let mut next = || fields.next().ok_or_else(unreadable);
            let table = unhex(next()?)
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .ok_or_else(unreadable)?;
            let side = match next()? {
                "s" => Side::Source,
                "t" => Side::Target,
                _ => return Err(unreadable()),
            };
            let hash = |field: &str| -> Result<Option<[u8; 16]>, Error> {
                if field == "-" {
                    return Ok(None);
                }
                let bytes = unhex(field).ok_or_else(unreadable)?;
                Ok(Some(bytes.try_into().map_err(|_| unreadable())?))
            };
            let from = hash(next()?)?;
            let to = hash(next()?)?;
            let key = fields
                .map(|field| unhex(field).and_then(|bytes| String::from_utf8(bytes).ok()))
                .collect::<Option<Vec<String>>>()
                .ok_or_else(unreadable)?;
            let op = match (from, to) {
                (_, None) => Op::Down,
                (None, Some(_)) => Op::Up,
                (Some(_), Some(_)) => Op::Upsert,
            };
            lines.push((
                table,
                Write {
                    side,
                    op,
                    key,
                    from,
                    to,
                    overwrite: false,
                },
            ));
        }
        Ok(lines)
    }

    /// Deletes the journal; call it once the run is done.
    ///
    /// # Errors
    /// When the journal is there and cannot be deleted.
    pub fn clear_journal(&self) -> Result<(), Error> {
        let path = self.folder.join(JOURNAL);
        match fs::remove_file(&path) {
            Err(err) if err.kind() != io::ErrorKind::NotFound => {
                Err(Error::Io { path, source: err })
            }
            _ => Ok(()),
        }
    }
}

/// Makes an I/O error at `path` into memory's error.
fn io_at(path: &Path) -> impl FnOnce(io::Error) -> Error {
    let path = path.to_owned();
    move |source| Error::Io { path, source }
}

/// Flushes a folder's entries (a rename into it) to disk. Windows cannot
/// open a folder as a file, so there the step is skipped.
#[cfg(unix)]
fn sync_folder(folder: &Path) -> io::Result<()> {
    fs::File::open(folder)?.sync_all()
}

#[cfg(not(unix))]
fn sync_folder(_: &Path) -> io::Result<()> {
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.is_ascii() || !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use smugglr_core::plan::Hashes;

    use super::{Error, Memory};

    #[test]
    fn memory_comes_back_as_saved_whichever_place_is_named_first() -> Result<(), Error> {
        let config = std::env::temp_dir().join(format!("smugglr-memory-{}", std::process::id()));
        let hashes = Hashes::from([(vec!["'a b'".to_owned(), "1".to_owned()], [7; 16])]);
        let mut memory = Memory::open(&config, "./a.db", "./b.db")?;
        memory.set_table("notes", hashes.clone());
        memory.save()?;
        assert_eq!(
            Memory::open(&config, "./b.db", "./a.db")?.table("notes"),
            hashes
        );
        let _ = std::fs::remove_dir_all(&config);
        Ok(())
    }
}
