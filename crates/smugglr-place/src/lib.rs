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

pub mod http_sql;
pub mod local_file;
mod queries;
mod value;

use smugglr_core::change::Change;
use smugglr_core::shape::Ledger;

pub use http_sql::HttpSql;
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
    /// A value is not SQLite `quote()` text.
    #[error("{0:?} is not SQLite quote() text")]
    Value(String),
    /// SQLite refused, in the file at `address`.
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

/// A database smugglr keeps in step.
///
/// Keys and values are SQLite's `quote()` text of each column; hashes are
/// [`row_hash`](smugglr_core::row_hash::row_hash) of the row's columns in
/// column order.
pub trait Place {
    /// The place's address, the same every run.
    fn address(&self) -> &str;

    /// Every table the place holds, except SQLite's own.
    ///
    /// # Errors
    /// When the place cannot be read.
    fn tables(&mut self) -> Result<Vec<Table>, Error>;

    /// The latest migration in the place's migratr ledger, read from its
    /// `_migratr_migrations` table; `None` when the place has no ledger. A
    /// place that cannot hold one keeps this default.
    ///
    /// # Errors
    /// When the place cannot be read.
    fn ledger(&mut self) -> Result<Option<Ledger>, Error> {
        Ok(None)
    }

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

    /// Writes the changes to the table, all in one transaction, each only
    /// where its row is still at the change's `from` (no row when `from` is
    /// `None`), and returns the keys of the rows it wrote. The engine's
    /// statement carries that condition; the place decides nothing.
    ///
    /// # Errors
    /// When the place cannot be written; nothing is then written.
    fn write(&mut self, table: &Table, changes: &[Change]) -> Result<Vec<Key>, Error>;
}

/// Database addresses that name another engine.
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
/// file, and an `http://` or `https://` URL a service that speaks HTTP-SQL.
///
/// # Errors
/// [`Error::OtherEngine`] for a database that is not SQLite,
/// [`Error::Unreachable`] for a kind of place smugglr cannot reach, and the
/// place's own error when it cannot be opened.
pub fn open(address: &str) -> Result<Box<dyn Place>, Error> {
    match address.split_once("://") {
        None => Ok(Box::new(LocalFile::open(address)?)),
        Some(("file", path)) => Ok(Box::new(LocalFile::open(path)?)),
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

#[cfg(test)]
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
