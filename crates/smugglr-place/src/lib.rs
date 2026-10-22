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

use smugglr_core::change::Change;

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
