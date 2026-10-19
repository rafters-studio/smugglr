//! One direct sync between two places: read each place's hashes, let the
//! engine plan each table, write, and keep the pair's memory.

use std::collections::BTreeMap;
use std::path::PathBuf;

use smugglr_core::change::{Change, Op};
use smugglr_core::ignore::Ignore;
use smugglr_core::plan::{Hashes, Plan, Side, SyncMode, Write, not_read_back, plan};
use smugglr_core::record::{self, Record};
use smugglr_core::row_hash::row_hash;
use smugglr_memory::Memory;
use smugglr_place::{Place, Table};

/// How a sync runs.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Where smugglr keeps its memory; `None` is smugglr's config directory.
    pub config_dir: Option<PathBuf>,
    /// What happens to a row changed in both places.
    pub sync_mode: SyncMode,
    /// Show what the sync would do, and write nothing.
    pub dryrun: bool,
    /// The tables the person lists to ignore, names or patterns with `*`
    /// and `?`, beside those smugglr always ignores.
    pub ignore: Vec<String>,
}

/// Why a sync could not run.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A place could not be reached, read or written.
    #[error(transparent)]
    Place(#[from] smugglr_place::Error),
    /// smugglr's memory could not be read or written.
    #[error(transparent)]
    Memory(#[from] smugglr_memory::Error),
    /// The two places do not hold the same tables, or a table has no
    /// primary key; the run stops before any row is read.
    #[error("{0}")]
    Shape(String),
    /// There is no config directory to keep memory in.
    #[error("smugglr has no config directory: set XDG_CONFIG_HOME or HOME")]
    NoConfigDir,
}

/// Syncs `source` and `target` once, and says what it did.
///
/// # Errors
/// When a place cannot be reached, read or written, or memory cannot be kept.
pub fn sync(
    source: &mut dyn Place,
    target: &mut dyn Place,
    options: &Options,
) -> Result<Record, Error> {
    let config = match &options.config_dir {
        Some(dir) => dir.clone(),
        None => smugglr_memory::config_dir().ok_or(Error::NoConfigDir)?,
    };
    let mut memory = Memory::open(&config, source.address(), target.address())?;
    // An ignored table is set aside before anything else, so it is never
    // compared, hashed, read or written.
    let ignore = Ignore::new(options.ignore.iter().cloned());
    let (tables, mut ignored) = ignore.split(source.tables()?, |table| &table.name);
    let (theirs, ignored_there) = ignore.split(target.tables()?, |table| &table.name);
    ignored.extend(ignored_there);
    check_shape(&tables, &theirs, source.address(), target.address())?;
    let mut done = BTreeMap::new();
    for table in tables {
        let here: Hashes = source.hashes(&table)?.into_iter().collect();
        let there: Hashes = target.hashes(&table)?.into_iter().collect();
        let last = memory.table(&table.name);
        let planned = plan(&here, &there, &last, options.sync_mode);
        done.insert(table.name.clone(), record::table(&planned, options.dryrun));
        let Plan::Move {
            writes,
            memory: mut next,
        } = planned
        else {
            continue;
        };
        if options.dryrun {
            continue;
        }
        let (to_target, to_source): (Vec<Write>, Vec<Write>) = writes
            .into_iter()
            .partition(|write| write.side == Side::Target);
        let mut mismatched = carry(
            &table,
            &mut *source,
            &mut *target,
            &to_target,
            &last,
            &mut next,
        )?;
        mismatched.extend(carry(
            &table,
            &mut *target,
            &mut *source,
            &to_source,
            &last,
            &mut next,
        )?);
        if !mismatched.is_empty() {
            // Not done: the table keeps its old memory.
            done.insert(table.name.clone(), record::not_read_back(mismatched));
            continue;
        }
        memory.set_table(&table.name, next);
    }
    if !options.dryrun {
        memory.save()?;
    }
    Ok(Record::new(done, ignored, options.dryrun))
}

/// Stops the run, before any row is read, unless both places hold the same
/// tables and each has a primary key. The full shape check and its words
/// are #506's; a table without a primary key is #507's.
fn check_shape(ours: &[Table], theirs: &[Table], source: &str, target: &str) -> Result<(), Error> {
    for (tables, others, here, there) in [
        (ours, theirs, source, target),
        (theirs, ours, target, source),
    ] {
        for table in tables {
            if !others.iter().any(|other| other.name == table.name) {
                return Err(Error::Shape(format!(
                    "the table {} is in {here} but not in {there}",
                    table.name
                )));
            }
            if table.key.is_empty() {
                return Err(Error::Shape(format!(
                    "the table {} in {here} has no primary key",
                    table.name
                )));
            }
        }
    }
    Ok(())
}

/// Writes `writes` in `to`, with `from`'s copy of each row, reads the rows
/// back and returns the keys of those that do not match what was written. A
/// row gone from `from` since its hash was read is not written, and its key
/// keeps its `last` hash in memory, so the next run finds it missing on that
/// side.
fn carry(
    table: &Table,
    from: &mut dyn Place,
    to: &mut dyn Place,
    writes: &[Write],
    last: &Hashes,
    memory: &mut Hashes,
) -> Result<Vec<Vec<String>>, Error> {
    let changes = changes(table, from, writes)?;
    for write in writes {
        if !changes.iter().any(|change| change.key == write.key) {
            match last.get(&write.key) {
                Some(&hash) => memory.insert(write.key.clone(), hash),
                None => memory.remove(&write.key),
            };
        }
    }
    to.write(table, &changes)?;
    let written: Vec<Write> = writes
        .iter()
        .filter(|write| changes.iter().any(|change| change.key == write.key))
        .cloned()
        .collect();
    let keys: Vec<Vec<String>> = written.iter().map(|write| write.key.clone()).collect();
    let read_back: Hashes = to
        .rows(table, &keys)?
        .into_iter()
        .filter_map(|row| {
            let columns: Vec<&str> = row.iter().map(String::as_str).collect();
            Some((key_of(table, &row)?, row_hash(&columns)))
        })
        .collect();
    Ok(not_read_back(&written, &read_back))
}

/// A row's key, from its columns in column order.
fn key_of(table: &Table, row: &[String]) -> Option<Vec<String>> {
    table
        .key
        .iter()
        .map(|column| {
            let i = table.columns.iter().position(|c| c == column)?;
            row.get(i).cloned()
        })
        .collect()
}

/// The Changes for `writes`, with values read from `from`, the other side;
/// a write whose row `from` no longer holds is left out.
fn changes(table: &Table, from: &mut dyn Place, writes: &[Write]) -> Result<Vec<Change>, Error> {
    let wanted: Vec<Vec<String>> = writes
        .iter()
        .filter(|write| write.op != Op::Down)
        .map(|write| write.key.clone())
        .collect();
    let mut rows: BTreeMap<Vec<String>, Vec<String>> = from
        .rows(table, &wanted)?
        .into_iter()
        .filter_map(|row| Some((key_of(table, &row)?, row)))
        .collect();
    Ok(writes
        .iter()
        .filter_map(|write| {
            let values = match write.op {
                Op::Down => Vec::new(),
                Op::Up | Op::Upsert => rows.remove(&write.key)?,
            };
            Some(Change {
                op: write.op,
                table: table.name.clone(),
                key: write.key.clone(),
                from: write.from,
                to: write.to,
                values,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use smugglr_core::change::Change;
    use smugglr_core::plan::Hashes;
    use smugglr_memory::Memory;
    use smugglr_place::{Error as PlaceError, Key, LocalFile, Place, Table};

    use super::{Error, Options, sync};

    /// A place with one row whose hash it reports but whose values are gone
    /// by the time they are read.
    #[derive(Debug)]
    struct Vanishing(&'static str, Vec<(Key, [u8; 16])>);

    impl Place for Vanishing {
        fn address(&self) -> &str {
            self.0
        }
        fn tables(&mut self) -> Result<Vec<Table>, PlaceError> {
            Ok(vec![Table {
                name: "notes".to_owned(),
                columns: vec!["id".to_owned()],
                key: vec!["id".to_owned()],
            }])
        }
        fn hashes(&mut self, _: &Table) -> Result<Vec<(Key, [u8; 16])>, PlaceError> {
            Ok(self.1.clone())
        }
        fn rows(&mut self, _: &Table, _: &[Key]) -> Result<Vec<Vec<String>>, PlaceError> {
            Ok(Vec::new())
        }
        fn write(&mut self, _: &Table, _: &[Change]) -> Result<(), PlaceError> {
            Ok(())
        }
    }

    #[test]
    fn a_row_gone_before_its_values_are_read_keeps_its_last_hash() -> Result<(), Error> {
        let config = std::env::temp_dir().join(format!("smugglr-sync-{}", std::process::id()));
        let key = vec!["'n1'".to_owned()];
        let last = Hashes::from([(key.clone(), [1; 16])]);
        let mut memory = Memory::open(&config, "a", "b")?;
        memory.set_table("notes", last.clone());
        memory.save()?;
        // Edited in the source, then gone before its values are read.
        let mut source = Vanishing("a", vec![(key.clone(), [2; 16])]);
        let mut target = Vanishing("b", vec![(key, [1; 16])]);
        let options = Options {
            config_dir: Some(config.clone()),
            ..Options::default()
        };
        sync(&mut source, &mut target, &options)?;
        assert_eq!(Memory::open(&config, "a", "b")?.table("notes"), last);
        let _ = std::fs::remove_dir_all(&config);
        Ok(())
    }

    fn sqlite(path: &std::path::Path, sql: &str) -> rusqlite::Result<rusqlite::Connection> {
        let connection = rusqlite::Connection::open(path)?;
        connection.execute_batch(sql)?;
        Ok(connection)
    }

    /// Each row of `table`, every column as debug text.
    fn dump(connection: &rusqlite::Connection, table: &str) -> rusqlite::Result<Vec<String>> {
        let mut statement = connection.prepare(&format!("SELECT * FROM {table} ORDER BY 1"))?;
        let width = statement.column_count();
        statement
            .query_map([], |row| {
                (0..width)
                    .map(|i| row.get::<_, rusqlite::types::Value>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map(|values| format!("{values:?}"))
            })?
            .collect()
    }

    /// Real files: ignored tables, one only in the source and without a
    /// primary key, neither stop the run nor change, and the record names
    /// each once.
    #[test]
    fn ignored_tables_are_left_alone_and_named_once() -> Result<(), Box<dyn std::error::Error>> {
        let dir = std::env::temp_dir().join(format!("smugglr-ignore-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        // tags is in step, but each place's sqlite_sequence counts it
        // differently; SQLite itself moves the counter of a table written.
        let shape = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT); \
                     CREATE TABLE tags (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT); \
                     CREATE TABLE _migratr_migrations (version TEXT PRIMARY KEY); \
                     CREATE TABLE sessions_tmp (id TEXT PRIMARY KEY, who TEXT);";
        let source = sqlite(
            &dir.join("source.db"),
            &format!(
                "{shape} CREATE TABLE cache_pages (url TEXT, body BLOB); \
                 INSERT INTO notes (body) VALUES ('a'), ('b'); \
                 INSERT INTO tags (name) VALUES ('t'); \
                 INSERT INTO _migratr_migrations VALUES ('0001'), ('0002'); \
                 INSERT INTO sessions_tmp VALUES ('s1', 'source'); \
                 INSERT INTO cache_pages VALUES ('/', x'00');"
            ),
        )?;
        let target = sqlite(
            &dir.join("target.db"),
            &format!(
                "{shape} INSERT INTO tags (name) VALUES ('t'), ('gone'); \
                 DELETE FROM tags WHERE name = 'gone'; \
                 INSERT INTO _migratr_migrations VALUES ('0001');"
            ),
        )?;
        let watched = ["sqlite_sequence", "_migratr_migrations", "sessions_tmp"];
        let before = watched
            .iter()
            .map(|table| dump(&target, table))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let options = Options {
            config_dir: Some(dir.join("config")),
            ignore: vec!["cache_*".to_owned(), "sessions_tmp".to_owned()],
            ..Options::default()
        };
        let record = sync(
            &mut LocalFile::open(dir.join("source.db"))?,
            &mut LocalFile::open(dir.join("target.db"))?,
            &options,
        )?;
        let after = watched
            .iter()
            .map(|table| dump(&target, table))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(before, after);
        assert_eq!(dump(&target, "notes")?.len(), 2);
        assert_eq!(
            record
                .ignored
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["_migratr_migrations", "cache_pages", "sessions_tmp"]
        );
        assert_eq!(record.tables.keys().collect::<Vec<_>>(), ["notes", "tags"]);
        drop((source, target));
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
