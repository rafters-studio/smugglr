//! A local SQLite file as a place.
//!
//! The engine's row hash runs as a SQL function registered on the
//! connection, so each row is hashed where it lives. The file is opened for
//! reading and writing but never created: an empty place gets its shape from
//! the person, never from smugglr.

use std::collections::HashSet;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::functions::FunctionFlags;
use rusqlite::types::ToSqlOutput;
use rusqlite::{Connection, MAIN_DB, OpenFlags, OptionalExtension, ToSql, params_from_iter};
use smugglr_core::change::{Change, Op};
use smugglr_core::large::{self, Large};
use smugglr_core::row_hash::{RowHasher, row_hash};

use crate::queries::{self, ROW_HASH};
use crate::value::{Value, unquote};
use crate::{Error, Key, Place, Table};

/// How long a read or write waits for another writer to let go of the
/// file's lock before the run stops. Current choice (2026-10-22): 10
/// seconds, twice the 5-second default most SQLite tools use; revisable.
const BUSY: Duration = Duration::from_secs(10);

/// A text or blob value of more than this many bytes is hashed a piece at a
/// time where it lives, never read whole (#600).
const STREAMED: u64 = 1 << 20;

/// The bytes read from or written to a large value at once.
const PIECE: usize = 1 << 20;

/// The largest value of a table with no rowid that is read: such a value is
/// read whole, in memory, once, and one larger is refused (#600). Current
/// choice 2026-10-26: 64 MiB; revisable.
const WHOLE: u64 = 64 << 20;

/// A SQLite file on this machine.
#[derive(Debug)]
pub struct LocalFile {
    address: String,
    connection: Connection,
    /// The tables with no rowid the record has named, so each is named once.
    named: HashSet<String>,
    /// The last value of a table with no rowid read whole, by table, key and
    /// column, so its pieces are cut from one read.
    whole: Option<((String, Key, usize), Vec<u8>)>,
}

/// A parameter: a value, or a blob of zeros of a size, its bytes written
/// after.
enum Param {
    Value(Value),
    Zeros(i32),
}

impl ToSql for Param {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Self::Value(value) => value.to_sql(),
            Self::Zeros(size) => Ok(ToSqlOutput::ZeroBlob(*size)),
        }
    }
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
            named: HashSet::new(),
            whole: None,
        })
    }

    /// The name `table`'s rowid answers to; `None` for a `WITHOUT ROWID`
    /// table, or one whose columns take every name of its rowid, whose
    /// large values are then read and written whole, as the record says
    /// once.
    fn rowid(&mut self, table: &Table) -> Result<Option<&'static str>, Error> {
        let without: bool = self
            .connection
            .query_row(queries::WITHOUT_ROWID, [&table.name], |row| row.get(0))
            .optional()
            .map_err(|source| self.sqlite(source))?
            .unwrap_or(false);
        let name = ["rowid", "_rowid_", "oid"]
            .into_iter()
            .find(|name| !table.columns.iter().any(|c| c.eq_ignore_ascii_case(name)));
        let name = name.filter(|_| !without);
        if name.is_none() && self.named.insert(table.name.clone()) {
            println!(
                "smugglr: {} in {} has no rowid, so a large value of it is read and \
                 written whole, in memory",
                table.name, self.address
            );
        }
        Ok(name)
    }

    /// Each row the statement `sql` (built by [`queries::apart`]) gives for
    /// `params`: its key, its columns' `quote()` text (each large value as
    /// its placeholder when `values`, else empty) and its hash, each large
    /// value read a piece at a time.
    fn read_apart(
        &self,
        table: &Table,
        sql: &str,
        params: &[Value],
        values: bool,
    ) -> rusqlite::Result<Vec<(Key, Vec<String>, [u8; 16])>> {
        let (keys, width) = (table.key.len(), table.columns.len());
        let mut statement = self.connection.prepare_cached(sql)?;
        let mut rows = statement.query(params_from_iter(params))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let key = (0..keys)
                .map(|i| row.get::<_, String>(i))
                .collect::<rusqlite::Result<Key>>()?;
            let rowid: i64 = row.get(keys)?;
            let mut hasher = RowHasher::new();
            let mut columns = Vec::with_capacity(width);
            for i in 0..width {
                let quoted: Option<String> = row.get(keys + 1 + i)?;
                let blob: Option<bool> = row.get(keys + 1 + width + i)?;
                match (quoted, blob) {
                    (None, Some(blob)) => {
                        let large = self.stream(table, i, rowid, blob, &mut hasher, values)?;
                        columns.push(if values { large.placeholder() } else { String::new() });
                    }
                    (quoted, _) => {
                        let quoted = quoted.unwrap_or_else(|| "NULL".to_owned());
                        hasher.length(quoted.len() as u64);
                        hasher.piece(quoted.as_bytes());
                        columns.push(quoted);
                    }
                }
            }
            out.push((key, columns, hasher.finish()));
        }
        Ok(out)
    }

    /// Feeds the `quote()` text of the large value in column `i` of the row
    /// `rowid` to `hasher`, a piece at a time, and gives the value's size
    /// and, when `hash`, its own hash.
    fn stream(
        &self,
        table: &Table,
        i: usize,
        rowid: i64,
        blob: bool,
        hasher: &mut RowHasher,
        hash: bool,
    ) -> rusqlite::Result<Large> {
        let column = table.columns.get(i).map_or("", String::as_str);
        let handle = self
            .connection
            .blob_open(MAIN_DB, table.name.as_str(), column, rowid, true)?;
        let size = handle.len();
        let mut piece = vec![0; PIECE.min(size.max(1))];
        // A text's quote() text is longer by each quote it holds: counted
        // first, since the row hash takes a column's length before its text.
        let mut quotes = 0_u64;
        if !blob {
            let mut at = 0;
            while at < size {
                let read = handle.read_at(&mut piece, at)?;
                if read == 0 {
                    break;
                }
                quotes += piece[..read].iter().filter(|&&b| b == b'\'').count() as u64;
                at += read;
            }
        }
        hasher.length(large::quoted_len(blob, size as u64, quotes));
        hasher.piece(large::opening(blob));
        let mut value = large::Hasher::new(blob);
        let mut quoted = Vec::new();
        let mut at = 0;
        while at < size {
            let read = handle.read_at(&mut piece, at)?;
            if read == 0 {
                break;
            }
            quoted.clear();
            large::quote(blob, &piece[..read], &mut quoted);
            hasher.piece(&quoted);
            if hash {
                value.update(&piece[..read]);
            }
            at += read;
        }
        hasher.piece(b"'");
        Ok(Large {
            blob,
            size: size as u64,
            hash: if hash { value.finish() } else { [0; 32] },
        })
    }

    /// The rowid of the row `key` (as `rowid` names it); `None` when there
    /// is no such row.
    fn rowid_of(&self, table: &Table, rowid: &str, key: &Key) -> Result<Option<i64>, Error> {
        let key = key
            .iter()
            .map(|column| unquote(column))
            .collect::<Result<Vec<_>, Error>>()?;
        self.connection
            .query_row(&queries::rowid(table, rowid), params_from_iter(key), |row| {
                row.get(0)
            })
            .optional()
            .map_err(|source| self.sqlite(source))
    }

    /// The value in column `name` of the row `key` of a table with no
    /// rowid, read whole, once; refused, and named, past [`WHOLE`].
    fn read_whole(&self, table: &Table, key: &Key, name: &str) -> Result<Vec<u8>, Error> {
        let key = key
            .iter()
            .map(|text| unquote(text))
            .collect::<Result<Vec<_>, Error>>()?;
        let size: Option<i64> = self
            .connection
            .query_row(&queries::value(table, name, false), params_from_iter(&key), |row| {
                row.get(0)
            })
            .optional()
            .map_err(|source| self.sqlite(source))?
            .flatten();
        let size = u64::try_from(size.unwrap_or(0)).unwrap_or(0);
        if size > WHOLE {
            return Err(Error::Adapter(format!(
                "a value of {} bytes in {}, which has no rowid, is larger than the {} \
                 bytes such a table's values are read up to",
                size, table.name, WHOLE
            )));
        }
        self.connection
            .query_row(&queries::value(table, name, true), params_from_iter(&key), |row| {
                row.get::<_, Option<Vec<u8>>>(0)
            })
            .optional()
            .map(|bytes| bytes.flatten().unwrap_or_default())
            .map_err(|source| self.sqlite(source))
    }

    /// Writes `changes` as [`Place::write`] says; with `file`, a value may
    /// be a large value's placeholder, whose own bytes are in the file
    /// `file` names, written a piece at a time.
    fn write_with(
        &mut self,
        table: &Table,
        changes: &[Change],
        file: Option<&dyn Fn(&Large) -> Option<PathBuf>>,
    ) -> Result<Vec<Key>, Error> {
        let has_large = file.is_some()
            && changes
                .iter()
                .any(|change| change.values.iter().any(|v| Large::parse(v).is_some()));
        let rowid = if has_large { self.rowid(table)? } else { None };
        let address = self.address.clone();
        let sqlite = |source| Error::Sqlite {
            address: address.clone(),
            source,
        };
        // Each large value in a change: its column and file, written after
        // its row, unless the table has no rowid and it is bound whole.
        let params = |change: &Change| -> Result<(Vec<Param>, Vec<(usize, Large, PathBuf)>), Error> {
            let mut params = Vec::with_capacity(change.values.len());
            let mut after = Vec::new();
            for (i, text) in change.values.iter().enumerate() {
                match (file, Large::parse(text)) {
                    (Some(file), Some(large)) => {
                        let path = file(&large).unwrap_or_default();
                        match i32::try_from(large.size) {
                            Ok(size) if large.blob && rowid.is_some() => {
                                params.push(Param::Zeros(size));
                                after.push((i, large, path));
                            }
                            _ => params.push(Param::Value(unquote(&crate::quoted_from(
                                &large, &path,
                            )?)?)),
                        }
                    }
                    _ => params.push(Param::Value(unquote(text)?)),
                }
            }
            Ok((params, after))
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
                let (count, after) = match (change.op, from) {
                    (Op::Up, _) | (Op::Upsert, None) => {
                        let (params, after) = params(change)?;
                        let count = insert.execute(params_from_iter(params)).map_err(sqlite)?;
                        (count, after)
                    }
                    (Op::Upsert, Some(from)) => {
                        let (mut params, after) = params(change)?;
                        params.push(Param::Value(from));
                        let count = update.execute(params_from_iter(params)).map_err(sqlite)?;
                        (count, after)
                    }
                    (Op::Down, Some(from)) => {
                        let mut params: Vec<Param> = change
                            .key
                            .iter()
                            .map(|text| unquote(text).map(Param::Value))
                            .collect::<Result<_, Error>>()?;
                        params.push(Param::Value(from));
                        let count = delete.execute(params_from_iter(params)).map_err(sqlite)?;
                        (count, Vec::new())
                    }
                    (Op::Down, None) => (0, Vec::new()),
                };
                if count == 0 {
                    continue;
                }
                if let (Some(rowid), false) = (rowid, after.is_empty()) {
                    let key = change
                        .key
                        .iter()
                        .map(|column| unquote(column))
                        .collect::<Result<Vec<_>, Error>>()?;
                    let at: i64 = transaction
                        .query_row(&queries::rowid(table, rowid), params_from_iter(key), |row| {
                            row.get(0)
                        })
                        .map_err(sqlite)?;
                    for (i, _, path) in &after {
                        let column = table.columns.get(*i).map_or("", String::as_str);
                        let mut handle = transaction
                            .blob_open(MAIN_DB, table.name.as_str(), column, at, false)
                            .map_err(sqlite)?;
                        let mut source = std::fs::File::open(path).map_err(|err| {
                            Error::Adapter(format!("{}: {err}", path.display()))
                        })?;
                        let mut piece = vec![0; PIECE];
                        let mut offset = 0;
                        loop {
                            let read = source.read(&mut piece).map_err(|err| {
                                Error::Adapter(format!("{}: {err}", path.display()))
                            })?;
                            if read == 0 {
                                break;
                            }
                            handle.write_at(&piece[..read], offset).map_err(sqlite)?;
                            offset += read;
                        }
                    }
                }
                written.push(change.key.clone());
            }
        }
        transaction.commit().map_err(sqlite)?;
        Ok(written)
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
        // Hashed here, where a value too large to hold is read a piece at a
        // time; the same row hash the SQL function gives.
        if let Some(rowid) = self.rowid(table)? {
            let sql = queries::apart(table, rowid, STREAMED, false);
            return self
                .read_apart(table, &sql, &[], false)
                .map(|rows| rows.into_iter().map(|(key, _, hash)| (key, hash)).collect())
                .map_err(|source| self.sqlite(source));
        }
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
        // One read transaction for every key asked for, not a lock taken and
        // dropped per statement.
        let transaction = self.connection.transaction().map_err(sqlite)?;
        let mut found = Vec::new();
        {
            let mut statement = transaction.prepare(&queries::row(table)).map_err(sqlite)?;
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
        self.write_with(table, changes, None)
    }

    fn write_apart(
        &mut self,
        table: &Table,
        changes: &[Change],
        file: &dyn Fn(&Large) -> Option<PathBuf>,
    ) -> Result<Vec<Key>, Error> {
        self.write_with(table, changes, Some(file))
    }

    fn rows_apart(
        &mut self,
        table: &Table,
        keys: &[Key],
        large: u64,
    ) -> Result<Vec<(Vec<String>, [u8; 16])>, Error> {
        let Some(rowid) = self.rowid(table)? else {
            let rows = self.rows(table, keys)?;
            return Ok(crate::apart(table, rows, large));
        };
        let sql = queries::apart(table, rowid, large, true);
        let address = self.address.clone();
        let sqlite = |source| Error::Sqlite {
            address: address.clone(),
            source,
        };
        // One read transaction for every key, as for rows.
        self.connection.execute_batch("BEGIN").map_err(sqlite)?;
        let read = (|| {
            let mut found = Vec::new();
            for key in keys {
                let key = key
                    .iter()
                    .map(|column| unquote(column))
                    .collect::<Result<Vec<_>, Error>>()?;
                let rows = self.read_apart(table, &sql, &key, true).map_err(sqlite)?;
                found.extend(rows.into_iter().map(|(_, columns, hash)| (columns, hash)));
            }
            Ok(found)
        })();
        self.connection.execute_batch("COMMIT").map_err(sqlite)?;
        read
    }

    fn read_value(
        &mut self,
        table: &Table,
        key: &Key,
        column: usize,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, Error> {
        let name = table.columns.get(column).cloned().unwrap_or_default();
        let Some(rowid) = self.rowid(table)? else {
            let at = (table.name.clone(), key.clone(), column);
            if self.whole.as_ref().is_none_or(|(read, _)| *read != at) {
                self.whole = None;
                let bytes = self.read_whole(table, key, &name)?;
                self.whole = Some((at, bytes));
            }
            let bytes = self.whole.as_ref().map_or(&[][..], |(_, bytes)| bytes);
            let start = usize::try_from(offset).unwrap_or(usize::MAX).min(bytes.len());
            return Ok(bytes[start..bytes.len().min(start.saturating_add(len))].to_vec());
        };
        let Some(at) = self.rowid_of(table, rowid, key)? else {
            return Ok(Vec::new());
        };
        let handle = self
            .connection
            .blob_open(MAIN_DB, table.name.as_str(), name.as_str(), at, true)
            .map_err(|source| self.sqlite(source))?;
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let mut bytes = vec![0; len.min(handle.len().saturating_sub(start))];
        let read = handle
            .read_at(&mut bytes, start)
            .map_err(|source| self.sqlite(source))?;
        bytes.truncate(read);
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use smugglr_core::change::{Change, Op};
    use smugglr_core::hex::hex;
    use smugglr_core::large::Large;
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

    #[test]
    fn a_large_value_is_hashed_read_and_written_a_piece_at_a_time() -> Result<(), Error> {
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
                 randomblob(1500000))",
            )
            .map_err(sqlite)?;
        let whole: Vec<String> = connection
            .query_row(
                "SELECT quote(id), quote(body), quote(data) FROM big",
                [],
                |row| Ok(vec![row.get(0)?, row.get(1)?, row.get(2)?]),
            )
            .map_err(sqlite)?;
        drop(connection);
        let columns: Vec<&str> = whole.iter().map(String::as_str).collect();
        let hash = row_hash(&columns);
        let mut place = LocalFile::open(&path)?;
        let table = place
            .tables()?
            .into_iter()
            .find(|table| table.name == "big")
            .ok_or(Error::Adapter("no table".to_owned()))?;
        let key = vec!["'a'".to_owned()];
        // The same hash the SQL function gives over the whole quote() text.
        assert_eq!(place.hashes(&table)?, vec![(key.clone(), hash)]);
        let apart = place.rows_apart(&table, std::slice::from_ref(&key), 1_000)?;
        let (body, _) = Large::of(&whole[1]).ok_or(Error::Adapter("body".to_owned()))?;
        let (data, bytes) = Large::of(&whole[2]).ok_or(Error::Adapter("data".to_owned()))?;
        let row = vec![key[0].clone(), body.placeholder(), data.placeholder()];
        assert_eq!(apart, vec![(row.clone(), hash)]);
        assert_eq!(place.read_value(&table, &key, 2, 10, 5)?, bytes[10..15].to_vec());
        // Written elsewhere from files of the values' own bytes.
        let folder = std::env::temp_dir().join(format!("smugglr-place-{}-files", std::process::id()));
        std::fs::create_dir_all(&folder).map_err(|err| Error::Adapter(err.to_string()))?;
        let mut text = Vec::new();
        let mut at = 0;
        while at < body.size {
            let piece = place.read_value(&table, &key, 1, at, 300_000)?;
            at += piece.len() as u64;
            text.extend(piece);
        }
        let files = [(body, text), (data, bytes)];
        for (large, bytes) in &files {
            std::fs::write(folder.join(hex(&large.hash)), bytes)
                .map_err(|err| Error::Adapter(err.to_string()))?;
        }
        place.write(&table, &[Change {
            op: Op::Down,
            table: "big".to_owned(),
            key: key.clone(),
            from: Some(hash),
            to: None,
            values: Vec::new(),
        }])?;
        assert_eq!(place.hashes(&table)?, Vec::new());
        let wrote = place.write_apart(
            &table,
            &[Change {
                op: Op::Up,
                table: "big".to_owned(),
                key: key.clone(),
                from: None,
                to: Some(hash),
                values: row,
            }],
            &|large| Some(folder.join(hex(&large.hash))),
        )?;
        assert_eq!(wrote, vec![key.clone()]);
        assert_eq!(place.hashes(&table)?, vec![(key, hash)]);
        let _ = std::fs::remove_dir_all(&folder);
        let _ = std::fs::remove_file(&path);
        Ok(())
    }
}
