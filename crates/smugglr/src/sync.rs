//! One direct sync between two places: read each place's hashes, let the
//! engine plan each table, write, and keep the pair's memory.

use std::collections::BTreeMap;
use std::path::PathBuf;

use smugglr_core::change::{Change, Op};
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
    let tables = source.tables()?;
    check_shape(
        &tables,
        &target.tables()?,
        source.address(),
        target.address(),
    )?;
    let mut done = Record::default();
    for table in tables {
        let here: Hashes = source.hashes(&table)?.into_iter().collect();
        let there: Hashes = target.hashes(&table)?.into_iter().collect();
        let last = memory.table(&table.name);
        let planned = plan(&here, &there, &last, options.sync_mode);
        done.tables
            .insert(table.name.clone(), record::table(&planned));
        let Plan::Move {
            writes,
            memory: mut next,
        } = planned
        else {
            continue;
        };
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
            done.tables
                .insert(table.name.clone(), record::not_read_back(mismatched));
            continue;
        }
        memory.set_table(&table.name, next);
    }
    memory.save()?;
    Ok(done)
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
    use smugglr_place::{Error as PlaceError, Key, Place, Table};

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
}
