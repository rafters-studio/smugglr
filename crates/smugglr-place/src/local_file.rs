//! A local SQLite file as a place.
//!
//! The engine's row hash runs as a SQL function registered on the
//! connection, so each row is hashed where it lives. The file is opened for
//! reading and writing but never created: an empty place gets its shape from
//! the person, never from smugglr.

use std::path::Path;
use std::time::Duration;

use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, OpenFlags, params_from_iter};
use smugglr_core::change::{Change, Op};
use smugglr_core::row_hash::row_hash;

use crate::queries::{self, ROW_HASH};
use crate::value::{Value, unquote};
use crate::{Error, Key, Place, Table};

/// How long a read or write waits for another writer to let go of the
/// file's lock before the run stops. Current choice (2026-10-22): 10
/// seconds, twice the 5-second default most SQLite tools use; revisable.
const BUSY: Duration = Duration::from_secs(10);

/// A SQLite file on this machine.
#[derive(Debug)]
pub struct LocalFile {
    address: String,
    connection: Connection,
}

impl LocalFile {
    /// Opens the SQLite file at `path`, which must exist.
    ///
    /// # Errors
    /// When the file cannot be opened as SQLite.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let address = path
            .canonicalize()
            .map_or_else(|_| path.display().to_string(), |p| p.display().to_string());
        let sqlite = |source| Error::Sqlite {
            address: address.clone(),
            source,
        };
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(sqlite)?;
        connection.busy_timeout(BUSY).map_err(sqlite)?;
        connection
            .create_scalar_function(
                ROW_HASH,
                -1,
                FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                |context| {
                    let columns = (0..context.len())
                        .map(|i| context.get::<String>(i))
                        .collect::<rusqlite::Result<Vec<String>>>()?;
                    let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
                    Ok(row_hash(&columns).to_vec())
                },
            )
            .map_err(sqlite)?;
        Ok(Self {
            address,
            connection,
        })
    }

    fn sqlite(&self, source: rusqlite::Error) -> Error {
        Error::Sqlite {
            address: self.address.clone(),
            source,
        }
    }

    fn columns(&self, name: &str) -> rusqlite::Result<Table> {
        let mut statement = self.connection.prepare(queries::COLUMNS)?;
        let mut columns = Vec::new();
        let mut types = Vec::new();
        let mut key = Vec::new();
        let mut rows = statement.query([name])?;
        while let Some(row) = rows.next()? {
            let column: String = row.get(0)?;
            let position: i64 = row.get(1)?;
            let declared: String = row.get(2)?;
            if position > 0 {
                key.push((position, column.clone(), declared));
            }
            columns.push(column);
            types.push(row.get(2)?);
        }
        key.sort();
        let declared: Vec<&str> = key
            .iter()
            .map(|(_, _, declared)| declared.as_str())
            .collect();
        let self_keyed = queries::self_keyed(&declared, || {
            self.connection
                .query_row(queries::KEY_INDEXED, [name], |row| row.get::<_, bool>(0))
        })?;
        Ok(Table {
            name: name.to_owned(),
            columns,
            types,
            key: key.into_iter().map(|(_, column, _)| column).collect(),
            self_keyed,
        })
    }
}

impl Place for LocalFile {
    fn address(&self) -> &str {
        &self.address
    }

    fn tables(&mut self) -> Result<Vec<Table>, Error> {
        let names = self
            .connection
            .prepare(queries::TABLES)
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<String>>>()
            })
            .map_err(|source| self.sqlite(source))?;
        names
            .iter()
            .map(|name| self.columns(name).map_err(|source| self.sqlite(source)))
            .collect()
    }

    fn hashes(&mut self, table: &Table) -> Result<Vec<(Key, [u8; 16])>, Error> {
        let width = table.key.len();
        self.connection
            .prepare(&queries::hashes(table))
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| {
                        let key = (0..width)
                            .map(|i| row.get::<_, String>(i))
                            .collect::<rusqlite::Result<Vec<String>>>()?;
                        Ok((key, row.get::<_, [u8; 16]>(width)?))
                    })?
                    .collect()
            })
            .map_err(|source| self.sqlite(source))
    }

    fn weights(&mut self, table: &Table) -> Result<Vec<(Key, u64)>, Error> {
        let width = table.key.len();
        self.connection
            .prepare(&queries::weights(table))
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| {
                        let key = (0..width)
                            .map(|i| row.get::<_, String>(i))
                            .collect::<rusqlite::Result<Vec<String>>>()?;
                        // A length is never negative.
                        let bytes = u64::try_from(row.get::<_, i64>(width)?).unwrap_or_default();
                        Ok((key, bytes))
                    })?
                    .collect()
            })
            .map_err(|source| self.sqlite(source))
    }

    fn rows(&mut self, table: &Table, keys: &[Key]) -> Result<Vec<Vec<String>>, Error> {
        let width = table.columns.len();
        let address = self.address.clone();
        let sqlite = |source| Error::Sqlite {
            address: address.clone(),
            source,
        };
        let mut statement = self
            .connection
            .prepare(&queries::row(table))
            .map_err(sqlite)?;
        let mut found = Vec::new();
        for key in keys {
            let key = key
                .iter()
                .map(|column| unquote(column))
                .collect::<Result<Vec<_>, Error>>()?;
            let mut rows = statement.query(params_from_iter(key)).map_err(sqlite)?;
            if let Some(row) = rows.next().map_err(sqlite)? {
                let columns = (0..width)
                    .map(|i| row.get::<_, String>(i))
                    .collect::<rusqlite::Result<Vec<String>>>()
                    .map_err(sqlite)?;
                found.push(columns);
            }
        }
        Ok(found)
    }

    fn write(&mut self, table: &Table, changes: &[Change]) -> Result<Vec<Key>, Error> {
        let address = self.address.clone();
        let sqlite = |source| Error::Sqlite {
            address: address.clone(),
            source,
        };
        let unquoted = |texts: &[String]| {
            texts
                .iter()
                .map(|text| unquote(text))
                .collect::<Result<Vec<Value>, Error>>()
        };
        let transaction = self.connection.transaction().map_err(sqlite)?;
        let mut written = Vec::new();
        {
            let mut insert = transaction
                .prepare(&queries::insert(table))
                .map_err(sqlite)?;
            let mut update = transaction
                .prepare(&queries::update(table))
                .map_err(sqlite)?;
            let mut delete = transaction
                .prepare(&queries::delete(table))
                .map_err(sqlite)?;
            for change in changes {
                let from = change.from.map(|hash| Value::Blob(hash.to_vec()));
                let count = match (change.op, from) {
                    (Op::Up, _) | (Op::Upsert, None) => insert
                        .execute(params_from_iter(unquoted(&change.values)?))
                        .map_err(sqlite)?,
                    (Op::Upsert, Some(from)) => {
                        let mut params = unquoted(&change.values)?;
                        params.push(from);
                        update.execute(params_from_iter(params)).map_err(sqlite)?
                    }
                    (Op::Down, Some(from)) => {
                        let mut params = unquoted(&change.key)?;
                        params.push(from);
                        delete.execute(params_from_iter(params)).map_err(sqlite)?
                    }
                    (Op::Down, None) => 0,
                };
                if count > 0 {
                    written.push(change.key.clone());
                }
            }
        }
        transaction.commit().map_err(sqlite)?;
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use smugglr_core::change::{Change, Op};
    use smugglr_core::row_hash::row_hash;

    use super::LocalFile;
    use crate::{Error, Place};

    fn file(name: &str) -> Result<PathBuf, Error> {
        let path =
            std::env::temp_dir().join(format!("smugglr-place-{}-{name}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        rusqlite::Connection::open(&path)
            .and_then(|c| c.execute_batch("CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER)"))
            .map_err(|source| Error::Sqlite {
                address: path.display().to_string(),
                source,
            })?;
        Ok(path)
    }

    #[test]
    fn a_big_integer_round_trips_and_hashes_where_it_lives() -> Result<(), Error> {
        let path = file("round-trip")?;
        let mut place = LocalFile::open(&path)?;
        let table = place
            .tables()?
            .into_iter()
            .next()
            .ok_or(Error::Adapter("no table".to_owned()))?;
        let values = vec!["'a'".to_owned(), "9007199254740993".to_owned()];
        let hash = row_hash(&["'a'", "9007199254740993"]);
        let change = Change {
            op: Op::Up,
            table: "t".to_owned(),
            key: vec!["'a'".to_owned()],
            from: None,
            to: Some(hash),
            values: values.clone(),
        };
        place.write(&table, std::slice::from_ref(&change))?;
        assert_eq!(place.hashes(&table)?, vec![(change.key.clone(), hash)]);
        // Weighed where the row lives, as the bytes of its quote() text.
        assert_eq!(place.weights(&table)?, vec![(change.key.clone(), 3 + 16)]);
        assert_eq!(place.rows(&table, &[change.key])?, vec![values]);
        let _ = std::fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn a_write_waits_out_another_connection_holding_the_lock() -> Result<(), Error> {
        let path = file("busy")?;
        let mut place = LocalFile::open(&path)?;
        let table = place
            .tables()?
            .into_iter()
            .next()
            .ok_or(Error::Adapter("no table".to_owned()))?;
        // Another writer holds the file's lock for 7 seconds, longer than
        // rusqlite's implicit 5.
        let (locked, held) = std::sync::mpsc::channel();
        let other = path.clone();
        let holder = std::thread::spawn(move || -> rusqlite::Result<()> {
            let connection = rusqlite::Connection::open(other)?;
            connection.execute_batch("BEGIN EXCLUSIVE")?;
            let _ = locked.send(());
            std::thread::sleep(std::time::Duration::from_secs(7));
            connection.execute_batch("ROLLBACK")
        });
        let _ = held.recv();
        let row = ["'a'", "1"];
        let change = Change {
            op: Op::Up,
            table: "t".to_owned(),
            key: vec!["'a'".to_owned()],
            from: None,
            to: Some(row_hash(&row)),
            values: row.iter().map(|&c| c.to_owned()).collect(),
        };
        let wrote = place.write(&table, std::slice::from_ref(&change));
        let released = holder.join();
        let _ = std::fs::remove_file(&path);
        assert!(
            matches!(released, Ok(Ok(()))),
            "the holder failed: {released:?}"
        );
        assert_eq!(wrote?, vec![change.key]);
        Ok(())
    }

    #[test]
    fn a_new_row_breaking_another_unique_column_fails_the_write() -> Result<(), Error> {
        let path = file("unique")?;
        rusqlite::Connection::open(&path)
            .and_then(|c| {
                c.execute_batch("CREATE UNIQUE INDEX t_n ON t (n); INSERT INTO t VALUES ('a', 1)")
            })
            .map_err(|source| Error::Sqlite {
                address: path.display().to_string(),
                source,
            })?;
        let mut place = LocalFile::open(&path)?;
        let table = place
            .tables()?
            .into_iter()
            .next()
            .ok_or(Error::Adapter("no table".to_owned()))?;
        let row = ["'b'", "1"];
        let new = Change {
            op: Op::Up,
            table: "t".to_owned(),
            key: vec!["'b'".to_owned()],
            from: None,
            to: Some(row_hash(&row)),
            values: row.iter().map(|&c| c.to_owned()).collect(),
        };
        // Only a row already at the key is a change that waits; any other
        // conflict is an error, never a row silently left out.
        let failed = place.write(&table, &[new]).is_err();
        let _ = std::fs::remove_file(&path);
        assert!(failed, "the write of a row breaking t_n did not fail");
        Ok(())
    }

    #[test]
    fn a_row_no_longer_at_from_is_not_written() -> Result<(), Error> {
        let path = file("moved-on")?;
        let mut place = LocalFile::open(&path)?;
        let table = place
            .tables()?
            .into_iter()
            .next()
            .ok_or(Error::Adapter("no table".to_owned()))?;
        let row = ["'a'", "1"];
        let first = Change {
            op: Op::Up,
            table: "t".to_owned(),
            key: vec!["'a'".to_owned()],
            from: None,
            to: Some(row_hash(&row)),
            values: row.iter().map(|&c| c.to_owned()).collect(),
        };
        assert_eq!(
            place.write(&table, std::slice::from_ref(&first))?,
            vec![first.key.clone()]
        );
        // An edit whose from is not the row's hash here: the row moved on.
        let stale = Change {
            op: Op::Upsert,
            from: Some([9; 16]),
            to: Some(row_hash(&["'a'", "2"])),
            values: vec!["'a'".to_owned(), "2".to_owned()],
            ..first.clone()
        };
        assert_eq!(place.write(&table, &[stale])?, Vec::<Vec<String>>::new());
        assert_eq!(place.rows(&table, &[first.key])?, vec![first.values]);
        let _ = std::fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn only_a_rowid_key_is_handed_out_by_the_database() -> Result<(), Error> {
        let path = file("self-keyed")?;
        rusqlite::Connection::open(&path)
            .and_then(|c| {
                c.execute_batch(
                    "CREATE TABLE a (id INTEGER PRIMARY KEY, n TEXT); \
                     CREATE TABLE b (id integer primary key autoincrement, n TEXT); \
                     CREATE TABLE c (id INT PRIMARY KEY, n TEXT); \
                     CREATE TABLE d (id INTEGER PRIMARY KEY, n TEXT) WITHOUT ROWID; \
                     CREATE TABLE e (id INTEGER, n TEXT, PRIMARY KEY (id, n)); \
                     CREATE TABLE f (id INTEGER, n TEXT, PRIMARY KEY (id))",
                )
            })
            .map_err(|source| Error::Sqlite {
                address: path.display().to_string(),
                source,
            })?;
        let self_keyed: Vec<(String, bool)> = LocalFile::open(&path)?
            .tables()?
            .into_iter()
            .map(|table| (table.name, table.self_keyed))
            .collect();
        let want = [
            ("a", true),
            ("b", true),
            ("c", false),
            ("d", false),
            ("e", false),
            ("f", true),
            ("t", false),
        ]
        .map(|(name, keyed)| (name.to_owned(), keyed));
        assert_eq!(self_keyed, want);
        let _ = std::fs::remove_file(&path);
        Ok(())
    }
}
