//! The places smugglr keeps in step (FR-SMUGGLR-071).
//!
//! A place is any SQLite database smugglr can reach: a local file, a service
//! that speaks HTTP-SQL, a hosted SQLite service through its adapter, a
//! browser tab. Every place speaks [`Place`], so anyone can write an adapter
//! for a service smugglr does not know.
//!
//! A place computes each row's hash where the data lives, with the engine's
//! one hash definition over the row's `quote()` text, and returns key and
//! hash; it reads row values only for the keys the engine asks for. A place
//! never builds a [`Change`] and never decides anything: the engine does.
//! Rows travel as SQLite's `quote()` text of each column, so every value,
//! integers beyond 2^53 included, is read without loss.

#[cfg(feature = "native")]
pub mod d1;
pub mod http_sql;
#[cfg(feature = "native")]
pub mod local_file;
#[cfg_attr(not(feature = "native"), allow(dead_code))]
mod queries;
mod value;

use std::path::Path;

use smugglr_core::change::Change;
use smugglr_core::large::Large;
use smugglr_core::row_hash::row_hash;

#[cfg(feature = "native")]
pub use d1::D1;
pub use http_sql::{HttpSql, Transport};
#[cfg(feature = "native")]
pub use local_file::LocalFile;

/// What a place says when it cannot do what was asked.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The address names a database that is not SQLite.
    #[error("smugglr keeps SQLite in step with SQLite.")]
    OtherEngine,
    /// The address names a kind of place smugglr cannot reach.
    #[error("smugglr cannot reach {0}:// places")]
    Unreachable(String),
    /// A hosted place needs a variable that is not set.
    #[error("{address} needs {var}, and it is not set")]
    Missing {
        /// The place's address.
        address: String,
        /// The variable.
        var: &'static str,
    },
    /// A value is not SQLite `quote()` text.
    #[error("{0:?} is not SQLite quote() text")]
    Value(String),
    /// SQLite refused, in the file at `address`.
    #[cfg(feature = "native")]
    #[error("{address}: {source}")]
    Sqlite {
        /// The place's address.
        address: String,
        /// What SQLite said.
        #[source]
        source: rusqlite::Error,
    },
    /// An adapter's own failure, in its own words.
    #[error("{0}")]
    Adapter(String),
}

/// A row's key: SQLite's `quote()` text of each primary-key column, in key
/// order.
pub type Key = Vec<String>;

/// One table of a place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    /// The table's name.
    pub name: String,
    /// Its column names, in column order.
    pub columns: Vec<String>,
    /// Each column's declared type, in column order; empty text for a
    /// column declared without one.
    pub types: Vec<String>,
    /// Its primary key's column names, in key order; empty when it declares
    /// none.
    pub key: Vec<String>,
    /// The database hands out the table's keys itself: in SQLite, its key is
    /// one column declared `INTEGER PRIMARY KEY`, the rowid.
    pub self_keyed: bool,
}

impl Table {
    /// The key of `row`, a row of this table in column order.
    #[must_use]
    pub fn key_of(&self, row: &[String]) -> Key {
        self.key
            .iter()
            .filter_map(|column| self.columns.iter().position(|c| c == column))
            .filter_map(|i| row.get(i).cloned())
            .collect()
    }
}

/// A database smugglr keeps in step.
///
/// Keys and values are SQLite's `quote()` text of each column; hashes are
/// [`row_hash`](smugglr_core::row_hash::row_hash) of the row's columns in
/// column order.
pub trait Place {
    /// The place's address, the same every run.
    fn address(&self) -> &str;

    /// The host's own tables, as names or patterns with `*` and `?`: a sync
    /// with this place ignores them on both sides, and the place never reads
    /// them (FR-SMUGGLR-066). None by default.
    fn own_tables(&self) -> &[&str] {
        &[]
    }

    /// Every table the place holds, except SQLite's own. A table of the
    /// host's own ([`Place::own_tables`]) is named and never read, so it has
    /// no columns.
    ///
    /// # Errors
    /// When the place cannot be read.
    fn tables(&mut self) -> Result<Vec<Table>, Error>;

    /// Each row's key and hash, computed where the data lives.
    ///
    /// # Errors
    /// When the place cannot be read.
    fn hashes(&mut self, table: &Table) -> Result<Vec<(Key, [u8; 16])>, Error>;

    /// The columns of each row whose key is asked for, in column order; a key
    /// with no row is left out.
    ///
    /// # Errors
    /// When the place cannot be read.
    fn rows(&mut self, table: &Table, keys: &[Key]) -> Result<Vec<Vec<String>>, Error>;

    /// Each row's key and its weight: the bytes of its columns' `quote()`
    /// text, what the row is when it travels. A first sync weighs itself with
    /// this before it moves anything (FR-SMUGGLR-065).
    ///
    /// The default reads every row through [`Place::rows`]; a place that can
    /// weigh rows where the data lives should do so instead.
    ///
    /// # Errors
    /// When the place cannot be read.
    fn weights(&mut self, table: &Table) -> Result<Vec<(Key, u64)>, Error> {
        let keys: Vec<Key> = self
            .hashes(table)?
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        Ok(self
            .rows(table, &keys)?
            .into_iter()
            .map(|row| {
                let weight = row.iter().map(|column| column.len() as u64).sum();
                (table.key_of(&row), weight)
            })
            .collect())
    }

    /// Writes the changes to the table, all in one transaction, each only
    /// where its row is still at the change's `from` (no row when `from` is
    /// `None`), and returns the keys of the rows it wrote. The engine's
    /// statement carries that condition; the place decides nothing.
    ///
    /// # Errors
    /// When the place cannot be written; nothing is then written.
    fn write(&mut self, table: &Table, changes: &[Change]) -> Result<Vec<Key>, Error>;

    /// The rows of `keys` as [`Place::rows`] reads them, and each row's
    /// hash, except that a text or blob value of more than `large` bytes is
    /// its [`Large::placeholder`], hashed where it lives and never held
    /// whole (FR-SMUGGLR-078 rev 28).
    ///
    /// The default reads every row whole through [`Place::rows`].
    ///
    /// # Errors
    /// When the place cannot be read.
    fn rows_apart(
        &mut self,
        table: &Table,
        keys: &[Key],
        large: u64,
    ) -> Result<Vec<(Vec<String>, [u8; 16])>, Error> {
        let rows = self.rows(table, keys)?;
        Ok(apart(table, rows, large))
    }

    /// Up to `len` bytes of the value in column `column` (its position) of
    /// the row `key`, from `offset`: the value's own bytes, a text's UTF-8
    /// or a blob's. Fewer at the value's end; none past it or for a row not
    /// there.
    ///
    /// The default reads the row whole through [`Place::rows`].
    ///
    /// # Errors
    /// When the place cannot be read.
    fn read_value(
        &mut self,
        table: &Table,
        key: &Key,
        column: usize,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, Error> {
        let rows = self.rows(table, std::slice::from_ref(key))?;
        let bytes = rows
            .first()
            .and_then(|row| row.get(column))
            .and_then(|quoted| Large::of(quoted))
            .map(|(_, bytes)| bytes)
            .unwrap_or_default();
        let start = usize::try_from(offset).unwrap_or(usize::MAX).min(bytes.len());
        Ok(bytes[start..bytes.len().min(start.saturating_add(len))].to_vec())
    }

    /// Writes the changes as [`Place::write`] does, where a value may be a
    /// [`Large::placeholder`] whose own bytes are in the file `file` names
    /// for it.
    ///
    /// The default reads each such value whole from its file.
    ///
    /// # Errors
    /// When a value's file cannot be read, or the place cannot be written;
    /// nothing is then written.
    fn write_apart(
        &mut self,
        table: &Table,
        changes: &[Change],
        file: &dyn Fn(&Large) -> Option<std::path::PathBuf>,
    ) -> Result<Vec<Key>, Error> {
        let mut whole = changes.to_vec();
        for change in &mut whole {
            for value in &mut change.values {
                if let Some(large) = Large::parse(value) {
                    *value = quoted_from(&large, &file(&large).unwrap_or_default())?;
                }
            }
        }
        self.write(table, &whole)
    }
}

/// `rows`, read whole, with each row's hash, and each text or blob value of
/// more than `large` bytes outside the key as its placeholder.
#[cfg_attr(not(feature = "native"), allow(dead_code))]
fn apart(table: &Table, rows: Vec<Vec<String>>, large: u64) -> Vec<(Vec<String>, [u8; 16])> {
    rows.into_iter()
        .map(|row| {
            let columns: Vec<&str> = row.iter().map(String::as_str).collect();
            let hash = row_hash(&columns);
            let row = row
                .into_iter()
                .enumerate()
                .map(|(i, quoted)| {
                    let in_key = table.columns.get(i).is_some_and(|c| table.key.contains(c));
                    match Large::of(&quoted) {
                        Some((value, _)) if !in_key && value.size > large => value.placeholder(),
                        _ => quoted,
                    }
                })
                .collect();
            (row, hash)
        })
        .collect()
}

/// The `quote()` text of the large value whose own bytes are in the file at
/// `path`, read whole.
fn quoted_from(large: &Large, path: &Path) -> Result<String, Error> {
    let bytes = std::fs::read(path)
        .map_err(|err| Error::Adapter(format!("{}: {err}", path.display())))?;
    let mut quoted = smugglr_core::large::opening(large.blob).to_vec();
    smugglr_core::large::quote(large.blob, &bytes, &mut quoted);
    quoted.push(b'\'');
    String::from_utf8(quoted)
        .map_err(|_| Error::Adapter(format!("{} is not UTF-8 text", path.display())))
}

/// Database addresses that name another engine.
#[cfg(feature = "native")]
const OTHER_ENGINES: &[&str] = &[
    "postgres",
    "postgresql",
    "mysql",
    "mariadb",
    "sqlserver",
    "mssql",
    "oracle",
    "mongodb",
    "mongodb+srv",
    "redis",
    "cockroachdb",
    "duckdb",
];

/// Opens the place an address names: a path or `file://` URL is a local
/// file, an `http://` or `https://` URL a service that speaks HTTP-SQL, and
/// `d1://<database-name>` a Cloudflare D1 database, the HTTP-SQL place
/// through D1's adapter.
///
/// # Errors
/// [`Error::OtherEngine`] for a database that is not SQLite,
/// [`Error::Unreachable`] for a kind of place smugglr cannot reach,
/// [`Error::Missing`] for a D1 database without its account id or token,
/// and the place's own error when it cannot be opened.
#[cfg(feature = "native")]
pub fn open(address: &str) -> Result<Box<dyn Place>, Error> {
    match address.split_once("://") {
        None => Ok(Box::new(LocalFile::open(address)?)),
        Some(("file", path)) => Ok(Box::new(LocalFile::open(path)?)),
        Some((scheme, name)) if scheme.eq_ignore_ascii_case("d1") => Ok(Box::new(D1::open(name)?)),
        Some((scheme, _))
            if scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https") =>
        {
            Ok(Box::new(HttpSql::open(address)))
        }
        Some((scheme, _)) if OTHER_ENGINES.contains(&scheme.to_ascii_lowercase().as_str()) => {
            Err(Error::OtherEngine)
        }
        Some((scheme, _)) => Err(Error::Unreachable(scheme.to_owned())),
    }
}

#[cfg(all(test, feature = "native"))]
mod tests {
    use super::{Error, open};

    #[test]
    fn another_engine_is_refused() {
        assert!(matches!(
            open("postgres://db.example.com/shop"),
            Err(Error::OtherEngine)
        ));
    }
}
