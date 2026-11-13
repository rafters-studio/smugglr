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
use smugglr_core::row_hash::{RowHasher, row_hash};

use crate::keyless;
use crate::queries::{self, ROW_HASH};
use crate::value::{Value, unquote};
use crate::{Error, Key, LOCAL_LIMIT, Place, Table};

/// How long a read or write waits for another writer to let go of the
/// file's lock before the run stops: 10 seconds, twice the 5-second default
/// most SQLite tools use.
const BUSY: Duration = Duration::from_secs(10);

/// A value of more than this many bytes is read alone when a row is hashed,
/// so a row's hash holds no more than one large value in memory at once:
/// memory follows the largest value, not the whole row (FR-SMUGGLR-071).
const STREAMED: u64 = 1 << 20;

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
        // Only a rowid key can count itself up.
        let autoincrement = self_keyed
            && self
                .connection
                .query_row(queries::TABLE_SQL, [name], |row| {
                    row.get::<_, Option<String>>(0)
                })?
                .is_some_and(|sql| queries::autoincrement(&sql));
        Ok(Table {
            name: name.to_owned(),
            columns,
            types,
            key: key.into_iter().map(|(_, column, _)| column).collect(),
            self_keyed,
            autoincrement,
        })
    }

    /// Every row of a table without a primary key, as [`keyless::all`]
    /// reads it, each passed to `each` with its key as it is read, so no
    /// more than one row is held.
    fn keyless_each<T>(
        &self,
        table: &Table,
        mut each: impl FnMut(Key, Vec<String>) -> Option<T>,
    ) -> Result<Vec<T>, Error> {
        let width = table.columns.len();
        let mut copies = keyless::Copies::default();
        let mut found = Vec::new();
        self.connection
            .prepare(&keyless::all(table, LOCAL_LIMIT))
            .and_then(|mut statement| {
                let mut rows = statement.query([])?;
                while let Some(row) = rows.next()? {
                    let row = (0..width)
                        .map(|i| row.get::<_, String>(i))
                        .collect::<rusqlite::Result<Vec<String>>>()?;
                    found.extend(each(copies.key(&row), row));
                }
                Ok(())
            })
            .map_err(|source| self.sqlite(source))?;
        Ok(found)
    }

    /// Every row of a table without a primary key.
    fn keyless_rows(&self, table: &Table) -> Result<Vec<Vec<String>>, Error> {
        self.keyless_each(table, |_, row| Some(row))
    }

    /// Writes `changes` to a table without a primary key, each copy only
    /// where the count of its values allows, and returns the keys written.
    fn keyless_write(&mut self, table: &Table, changes: &[Change]) -> Result<Vec<Key>, Error> {
        let held = keyless::by_identity(self.keyless_rows(table)?);
        let address = self.address.clone();
        let sqlite = |source| Error::Sqlite {
            address: address.clone(),
            source,
        };
        let transaction = self.connection.transaction().map_err(sqlite)?;
        let mut written = Vec::new();
        {
            let mut insert = transaction
                .prepare(&keyless::insert(table))
                .map_err(sqlite)?;
            let mut delete = transaction
                .prepare(&keyless::delete(table))
                .map_err(sqlite)?;
            for change in keyless::ordered(changes) {
                let copy = Value::Integer(keyless::copy(&change.key));
                let quoted = |row: &[String]| row.iter().cloned().map(Value::Text).collect();
                let count = if change.op == Op::Down {
                    let Some(row) = change.key.first().and_then(|id| held.get(id)) else {
                        continue;
                    };
                    let mut params: Vec<Value> = quoted(row);
                    params.push(copy);
                    delete.execute(params_from_iter(params)).map_err(sqlite)?
                } else {
                    let mut params = change
                        .values
                        .iter()
                        .map(|text| unquote(text))
                        .collect::<Result<Vec<Value>, Error>>()?;
                    params.extend::<Vec<Value>>(quoted(&change.values));
                    params.push(copy);
                    insert.execute(params_from_iter(params)).map_err(sqlite)?
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

    fn parents(&mut self, table: &str) -> Result<Vec<String>, Error> {
        self.connection
            .prepare(queries::PARENTS)
            .and_then(|mut statement| {
                statement
                    .query_map([table], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<String>>>()
            })
            .map_err(|source| self.sqlite(source))
    }

    fn hashes(&mut self, table: &Table) -> Result<Vec<(Key, [u8; 16])>, Error> {
        if table.key.is_empty() {
            return self.keyless_each(table, |key, row| {
                let columns: Vec<&str> = row.iter().map(String::as_str).collect();
                Some((key, row_hash(&columns)))
            });
        }
        // Hashed here column by column, the same hash the SQL function
        // gives; a large value is read alone, and one too large to quote is
        // hashed as its unquotable text, so its row reads as changed and is
        // refused when it is read to move.
        let width = table.key.len();
        let address = self.address.clone();
        let sqlite = |source| Error::Sqlite {
            address: address.clone(),
            source,
        };
        let mut statement = self
            .connection
            .prepare(&queries::hashed(table, STREAMED))
            .map_err(sqlite)?;
        let mut rows = statement.query([]).map_err(sqlite)?;
        let mut found = Vec::new();
        while let Some(row) = rows.next().map_err(sqlite)? {
            let key = (0..width)
                .map(|i| row.get::<_, String>(i))
                .collect::<rusqlite::Result<Key>>()
                .map_err(sqlite)?;
            let mut hasher = RowHasher::new();
            for (i, column) in table.columns.iter().enumerate() {
                let quoted = if let Some(quoted) =
                    row.get::<_, Option<String>>(width + i).map_err(sqlite)?
                {
                    quoted
                } else {
                    let params = key
                        .iter()
                        .map(|text| unquote(text))
                        .collect::<Result<Vec<_>, Error>>()?;
                    self.connection
                        .query_row(
                            &queries::value(table, column, LOCAL_LIMIT),
                            params_from_iter(params),
                            |row| row.get::<_, String>(0),
                        )
                        .map_err(sqlite)?
                };
                hasher.length(quoted.len() as u64);
                hasher.piece(quoted.as_bytes());
            }
            found.push((key, hasher.finish()));
        }
        Ok(found)
    }

    fn weights(&mut self, table: &Table) -> Result<Vec<(Key, u64)>, Error> {
        if table.key.is_empty() {
            return self.keyless_each(table, |key, row| {
                Some((key, row.iter().map(|column| column.len() as u64).sum()))
            });
        }
        let width = table.key.len();
        self.connection
            .prepare(&queries::weights(table, LOCAL_LIMIT))
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
        if table.key.is_empty() {
            let ids = keyless::identities(keys);
            return self.keyless_each(table, |key, row| {
                key.first()
                    .is_some_and(|id| ids.contains(id))
                    .then_some(row)
            });
        }
        let width = table.columns.len();
        let address = self.address.clone();
        let sqlite = |source| Error::Sqlite {
            address: address.clone(),
            source,
        };
        // One read transaction for every key asked for, not a lock taken and
        // dropped per statement.
        let transaction = self.connection.transaction().map_err(sqlite)?;
        let mut found = Vec::new();
        {
            let mut statement = transaction
                .prepare(&queries::row_within(table, LOCAL_LIMIT))
                .map_err(sqlite)?;
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
        }
        transaction.commit().map_err(sqlite)?;
        Ok(found)
    }

    fn write(&mut self, table: &Table, changes: &[Change]) -> Result<Vec<Key>, Error> {
        if table.key.is_empty() {
            return self.keyless_write(table, changes);
        }
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
        let sqlite = |source| Error::Sqlite {
            address: path.display().to_string(),
            source,
        };
        // The connection waits 10 seconds for a held lock, as BUSY says.
        // Checked before the busy handler below replaces that wait.
        let timeout: i64 = place
            .connection
            .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .map_err(sqlite)?;
        assert_eq!(timeout, 10_000);
        // SQLite calls the busy handler when the write meets the held lock:
        // the first call tells the test, and a bound makes a write that never
        // gets the lock fail instead of hanging.
        let (meets, met) = std::sync::mpsc::channel();
        place
            .connection
            .busy_handler(Some(move |count: i32| {
                if count == 0 {
                    let _ = meets.send(());
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
                count < 10_000
            }))
            .map_err(sqlite)?;
        // Another connection holds the file's lock until the test lets go.
        let holder = rusqlite::Connection::open(&path).map_err(sqlite)?;
        holder.execute_batch("BEGIN EXCLUSIVE").map_err(sqlite)?;
        let row = ["'a'", "1"];
        let change = Change {
            op: Op::Up,
            table: "t".to_owned(),
            key: vec!["'a'".to_owned()],
            from: None,
            to: Some(row_hash(&row)),
            values: row.iter().map(|&c| c.to_owned()).collect(),
        };
        let sent = change.clone();
        let writer = std::thread::spawn(move || place.write(&table, std::slice::from_ref(&sent)));
        // The write is blocked on the lock: SQLite has called the handler.
        // If the write ends without meeting the lock, the handler is dropped
        // with the place and this returns an error instead.
        let blocked = met.recv();
        let released = holder.execute_batch("ROLLBACK").map_err(sqlite);
        let wrote = writer.join();
        drop(holder);
        let _ = std::fs::remove_file(&path);
        assert!(blocked.is_ok(), "the write never met the held lock");
        released?;
        let wrote = wrote.map_err(|_| Error::Adapter("the writer panicked".to_owned()))?;
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

    #[test]
    fn a_row_with_a_large_value_hashes_as_the_sql_function_hashes_it() -> Result<(), Error> {
        let path = file("large")?;
        let sqlite = |source| Error::Sqlite {
            address: String::new(),
            source,
        };
        let connection = rusqlite::Connection::open(&path).map_err(sqlite)?;
        connection
            .execute_batch(
                "CREATE TABLE big (id TEXT PRIMARY KEY, body TEXT, data BLOB); \
                 INSERT INTO big VALUES ('a', printf('%.2000000c', 'x') || 'it''s', \
                 randomblob(1500000)), ('b', 'small', NULL)",
            )
            .map_err(sqlite)?;
        let mut want: Vec<(Vec<String>, [u8; 16])> = Vec::new();
        for id in ["a", "b"] {
            let row: Vec<String> = connection
                .query_row(
                    "SELECT quote(id), quote(body), quote(data) FROM big WHERE id = ?1",
                    [id],
                    |row| Ok(vec![row.get(0)?, row.get(1)?, row.get(2)?]),
                )
                .map_err(sqlite)?;
            let columns: Vec<&str> = row.iter().map(String::as_str).collect();
            want.push((vec![row[0].clone()], row_hash(&columns)));
        }
        drop(connection);
        let mut place = LocalFile::open(&path)?;
        let table = place
            .tables()?
            .into_iter()
            .find(|table| table.name == "big")
            .ok_or(Error::Adapter("no table".to_owned()))?;
        let mut hashes = place.hashes(&table)?;
        hashes.sort();
        assert_eq!(hashes, want);
        let _ = std::fs::remove_file(&path);
        Ok(())
    }
}
