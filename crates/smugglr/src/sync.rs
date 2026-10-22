//! One direct sync between two places: read each place's hashes, let the
//! engine plan each table, weigh a first sync, write, and keep the pair's
//! memory.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use smugglr_core::change::{Change, Op};
use smugglr_core::ignore::Ignore;
use smugglr_core::plan::{
    Hashes, Plan, Side, Steered, SyncMode, Write, changed_in_both, not_read_back, plan_steered,
    recover,
};
use smugglr_core::record::{self, Record};
use smugglr_core::row_hash::row_hash;
use smugglr_core::steering::{Steering, needs_steering};
use smugglr_core::weight::{DEFAULT_MAX_WIRE, Weight, too_much};
use smugglr_memory::Memory;
use smugglr_place::{Place, Table};

use crate::shape;

/// How the places connect, as `--sync-type` names it.
///
/// Only direct is built; hub, multi and mesh are not yet, so they cannot be
/// named here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SyncType {
    /// The two places, source and target, synced with each other.
    #[default]
    Direct,
}

/// How a sync runs: the options the flags give, as `--dryrun`,
/// `--sync-mode`, `--sync-type` and `--watch` do, and the settings the
/// config gives.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Where smugglr keeps its memory; `None` is smugglr's config directory.
    pub config_dir: Option<PathBuf>,
    /// What happens to a row changed in both places.
    pub sync_mode: SyncMode,
    /// How the places connect.
    pub sync_type: SyncType,
    /// Show what the sync would do, and write nothing.
    pub dryrun: bool,
    /// Rerun every given interval, until stopped. One [`sync`] is one whole
    /// run; repeating belongs to the front end that reads this, as the CLI
    /// does for `--watch`.
    pub watch: Option<Duration>,
    /// The tables the run is limited to; empty is every table.
    pub tables: Vec<String>,
    /// The tables the person lists to ignore, names or patterns with `*`
    /// and `?`, beside those smugglr always ignores.
    pub ignore: Vec<String>,
    /// The most bytes a first sync moves; `None` is
    /// [`DEFAULT_MAX_WIRE`], 100 MB.
    pub max_wire: Option<u64>,
    /// The steering columns the config names; only `--sync-mode last` reads
    /// them.
    pub steering: Steering,
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
    /// A table has no primary key, or no place has a table the run is
    /// limited to; the run stops before any row is read.
    #[error("{0}")]
    Shape(String),
    /// There is no config directory to keep memory in.
    #[error("smugglr has no config directory: set XDG_CONFIG_HOME or HOME")]
    NoConfigDir,
    /// `--sync-mode last` with tables that have no steering column, each
    /// named; the run stops before any row is read or written.
    #[error("{}", needs_steering(.0))]
    NoSteering(Vec<String>),
    /// The run began writing and then could not go on: `cause` says why.
    /// `record` is the run's record: the tables it finished, and the table
    /// it was writing stopped, naming the rows it wrote. It is not done;
    /// memory and the journal are left as they are, and rows are
    /// idempotent, so the next run finishes it.
    #[error("{cause}")]
    CutOff {
        /// What the run did before it stopped.
        record: Box<Record>,
        /// Why it could not go on.
        cause: Box<Error>,
    },
}

/// Syncs `source` and `target` once, and returns the record of what it did:
/// the same record `smugglr sync` prints.
///
/// A stop is a record, never an error: a table stopped on a row changed in
/// both places, a shape mismatch, or a first sync too much for the wire each
/// comes back as `Ok`, the record naming it.
///
/// # Errors
/// When a place cannot be reached, read or written, or memory cannot be
/// kept; and before any row is read, when the run is named wrong (a table no
/// place has, a table without a primary key, or `--sync-mode last` without a
/// steering column). Once a journal line is written, a failure is
/// [`Error::CutOff`], carrying the run's record: the table being written
/// stopped, naming the rows it wrote.
pub fn sync(
    source: &mut dyn Place,
    target: &mut dyn Place,
    options: &Options,
) -> Result<Record, Error> {
    // An ignored table is set aside before anything else, so it is never
    // compared, hashed, read or written.
    let ignore = ignore(source, target, options);
    let (every, mut ignored) = ignore.split(source.tables()?, |table| &table.name);
    let (theirs_every, ignored_there) = ignore.split(target.tables()?, |table| &table.name);
    ignored.extend(ignored_there);
    let named = |table: &Table| options.tables.is_empty() || options.tables.contains(&table.name);
    let tables: Vec<Table> = every.iter().filter(|t| named(t)).cloned().collect();
    let theirs: Vec<Table> = theirs_every.iter().filter(|t| named(t)).cloned().collect();
    // Before memory is touched or a row read.
    steering_named(&tables, &theirs, options)?;
    let config = match &options.config_dir {
        Some(dir) => dir.clone(),
        None => smugglr_memory::config_dir().ok_or(Error::NoConfigDir)?,
    };
    // Memory is only read here; a shape stop says whether the pair is new.
    let mut memory = Memory::open(&config, source.address(), target.address())?;
    // The whole places' shapes, not only the tables named: any mismatch
    // stops the whole run (FR-SMUGGLR-070).
    if let Some(mismatch) = shape::check(source.address(), target.address(), &every, &theirs_every)?
    {
        return Ok(Record::shape_mismatch(
            mismatch,
            ignored,
            memory.first_sync(),
        ));
    }
    if let Some(name) = options
        .tables
        .iter()
        .find(|name| !tables.iter().any(|table| &table.name == *name))
    {
        return Err(Error::Shape(format!("neither place has a table {name}")));
    }
    // Every table both places hold, not only those named: the journal goes
    // once it is finished, so no table's lines may be left unfinished.
    let common: Vec<Table> = every
        .into_iter()
        .filter(|table| theirs_every.iter().any(|other| other.name == table.name))
        .collect();
    finish_cut_off_run(&mut memory, &common, source, target, options.dryrun)?;
    let mut done = BTreeMap::new();
    let plans = plan_tables(
        tables,
        &theirs,
        &mut *source,
        &mut *target,
        &mut memory,
        options,
        &mut done,
    )?;
    // A table memory holds nothing for is in its first sync, even when the
    // pair synced before, and cannot tell a delete (FR-SMUGGLR-081).
    let unknown = memory.first_sync() || plans.iter().any(|(_, last, _)| last.is_empty());
    let weight = if memory.first_sync() {
        Some(weigh(&plans, &mut *source, &mut *target)?)
    } else {
        None
    };
    if let Some(weight) = weight
        && !options.dryrun
        && weight.over(options.max_wire.unwrap_or(DEFAULT_MAX_WIRE))
    {
        // Nothing written, and no memory kept: the pair stays unsynced.
        let why = too_much(&weight, source.address(), target.address());
        return Ok(Record::too_much(weight, why, ignored));
    }
    let mut wrote = Wrote::default();
    let unmatched = match write_all(
        plans,
        &mut *source,
        &mut *target,
        &mut memory,
        options.dryrun,
        &mut done,
        &mut wrote,
    ) {
        Ok(unmatched) => unmatched,
        // Once a journal line is written a row may have been: the table
        // being written stops, naming the rows it wrote.
        Err(cause) if wrote.journaled => {
            if let Some(name) = wrote.writing.take() {
                done.insert(name, record::lost(wrote.keys, &cause.to_string()));
            }
            let mut record = Record::new(done, ignored, false, weight);
            record.deletes_seen = !unknown;
            return Err(cut_off(record, cause));
        }
        Err(cause) => return Err(cause),
    };
    let mut record = Record::new(done, ignored, options.dryrun, weight);
    record.deletes_seen = !unknown;
    if !options.dryrun {
        let kept = memory.save().and_then(|()| {
            // Kept after a run whose rows read back different (exit 4), so
            // the next run finishes it; any other run's memory already holds
            // what it wrote.
            if unmatched {
                Ok(())
            } else {
                memory.clear_journal()
            }
        });
        if let Err(err) = kept {
            return Err(if record.wrote() {
                cut_off(record, err.into())
            } else {
                err.into()
            });
        }
    }
    Ok(record)
}

/// The error for a run that wrote rows and then stopped on `cause`,
/// carrying its `record`.
fn cut_off(record: Record, cause: Error) -> Error {
    Error::CutOff {
        record: Box::new(record),
        cause: Box::new(cause),
    }
}

/// What a run has written so far, kept as it goes so a run that cannot go
/// on can say what it wrote.
#[derive(Debug, Default)]
struct Wrote {
    /// A journal line was written: from here on a row may have been
    /// written, even when no write has answered yet.
    journaled: bool,
    /// The table being written.
    writing: Option<String>,
    /// The keys of the rows the places answered as written in that table.
    keys: Vec<Vec<String>>,
}

/// Writes each table's planned moves, table by table, recording each in
/// `done` and noting in `wrote` what is written as it goes. Memory is set
/// for each table written, not saved: a row that read back different keeps
/// its last hash. Returns whether any table's rows read back different.
fn write_all(
    plans: Vec<(Table, Hashes, Plan)>,
    source: &mut dyn Place,
    target: &mut dyn Place,
    memory: &mut Memory,
    dryrun: bool,
    done: &mut BTreeMap<String, record::Table>,
    wrote: &mut Wrote,
) -> Result<bool, Error> {
    let mut unmatched = false;
    for (table, last, planned) in plans {
        done.insert(table.name.clone(), record::table(&planned, dryrun));
        let Plan::Move {
            writes,
            memory: mut next,
        } = planned
        else {
            continue;
        };
        if dryrun {
            continue;
        }
        wrote.writing = Some(table.name.clone());
        wrote.keys.clear();
        let (mismatched, written) = carry_both(
            &table,
            &mut *source,
            &mut *target,
            writes,
            memory,
            &last,
            &mut next,
            wrote,
        )?;
        wrote.writing = None;
        // The record counts only the rows written.
        let moved = Plan::Move {
            writes: written,
            memory: next.clone(),
        };
        done.insert(table.name.clone(), record::table(&moved, false));
        if !mismatched.is_empty() {
            // Not done: a row that read back different keeps its last hash.
            unmatched = true;
            for key in &mismatched {
                match last.get(key) {
                    Some(&hash) => next.insert(key.clone(), hash),
                    None => next.remove(key),
                };
            }
            done.insert(table.name.clone(), record::not_read_back(mismatched));
        }
        memory.set_table(&table.name, next);
    }
    Ok(unmatched)
}

/// The tables a run ignores: those smugglr always ignores, those the person
/// lists, and a host's own tables as either place declares them, on both
/// sides (FR-SMUGGLR-066).
fn ignore(source: &dyn Place, target: &dyn Place, options: &Options) -> Ignore {
    Ignore::new(
        options
            .ignore
            .iter()
            .map(String::as_str)
            .chain(source.own_tables().iter().copied())
            .chain(target.own_tables().iter().copied())
            .map(str::to_owned),
    )
}

/// Under `--sync-mode last`, fails naming every table the run would plan
/// that has no steering column.
fn steering_named(tables: &[Table], theirs: &[Table], options: &Options) -> Result<(), Error> {
    if options.sync_mode != SyncMode::Last {
        return Ok(());
    }
    // A table whose database hands out its keys stops unplanned.
    let self_keyed = |table: &Table| {
        theirs
            .iter()
            .any(|other| other.name == table.name && other.self_keyed)
    };
    let planned = tables
        .iter()
        .filter(|table| !table.self_keyed && !self_keyed(table))
        .map(|table| (table.name.as_str(), table.columns.as_slice()));
    let lacking = options.steering.lacking(planned);
    if lacking.is_empty() {
        Ok(())
    } else {
        Err(Error::NoSteering(lacking))
    }
}

/// Plans every table before any is written, so a first sync can weigh
/// itself whole. A table whose database hands out its own keys, in either
/// place, goes into `done` as stopped before any row is read; the run's other
/// tables go on. A table that stops has its alike rows set in memory, not
/// saved (FR-SMUGGLR-081).
fn plan_tables(
    tables: Vec<Table>,
    theirs: &[Table],
    source: &mut dyn Place,
    target: &mut dyn Place,
    memory: &mut Memory,
    options: &Options,
    done: &mut BTreeMap<String, record::Table>,
) -> Result<Vec<(Table, Hashes, Plan)>, Error> {
    let mut plans = Vec::new();
    for table in tables {
        if table.self_keyed
            || theirs
                .iter()
                .any(|other| other.name == table.name && other.self_keyed)
        {
            done.insert(table.name.clone(), record::self_keyed());
            continue;
        }
        let here: Hashes = source.hashes(&table)?.into_iter().collect();
        let there: Hashes = target.hashes(&table)?.into_iter().collect();
        let last = memory.table(&table.name);
        let steered = if options.sync_mode == SyncMode::Last {
            steered(
                &table,
                &options.steering,
                &here,
                &there,
                &last,
                source,
                target,
            )?
        } else {
            Steered::new()
        };
        let planned = plan_steered(
            &table.name,
            &here,
            &there,
            &last,
            options.sync_mode,
            &steered,
        );
        if let Plan::Stop { .. } = planned {
            memory.set_table(&table.name, agreed(&here, &there, last.clone()));
        }
        plans.push((table, last, planned));
    }
    Ok(plans)
}

/// `last` with each key whose rows are alike in both places, `here` and
/// `there`, at their hash, and gone when neither holds it; every other key
/// keeps its last agreed hash.
fn agreed(here: &Hashes, there: &Hashes, mut last: Hashes) -> Hashes {
    last.retain(|key, _| here.contains_key(key) || there.contains_key(key));
    for (key, hash) in here {
        if there.get(key) == Some(hash) {
            last.insert(key.clone(), *hash);
        }
    }
    last
}

/// The steering values, in each place, of the rows changed in both: read
/// only for those rows, and only under `--sync-mode last`.
fn steered(
    table: &Table,
    steering: &Steering,
    here: &Hashes,
    there: &Hashes,
    last: &Hashes,
    source: &mut dyn Place,
    target: &mut dyn Place,
) -> Result<Steered, Error> {
    let keys = changed_in_both(here, there, last);
    let column = steering
        .column(&table.name)
        .and_then(|name| table.columns.iter().position(|c| c == name));
    let (Some(column), false) = (column, keys.is_empty()) else {
        return Ok(Steered::new());
    };
    let values = |place: &mut dyn Place| -> Result<BTreeMap<Vec<String>, String>, Error> {
        Ok(place
            .rows(table, &keys)?
            .into_iter()
            .filter_map(|row| Some((key_of(table, &row)?, row.get(column)?.clone())))
            .collect())
    };
    let ours = values(source)?;
    let mut theirs = values(target)?;
    Ok(ours
        .into_iter()
        .filter_map(|(key, value)| {
            let other = theirs.remove(&key)?;
            Some((key, (value, other)))
        })
        .collect())
}

/// What the planned writes move: each row written with values, weighed in
/// the place it is copied from, and the tables they are in. A delete moves
/// no row, and a stopped table moves nothing.
fn weigh(
    plans: &[(Table, Hashes, Plan)],
    source: &mut dyn Place,
    target: &mut dyn Place,
) -> Result<Weight, Error> {
    let mut weight = Weight::default();
    for (table, _, planned) in plans {
        let Plan::Move { writes, .. } = planned else {
            continue;
        };
        let copies: Vec<&Write> = writes.iter().filter(|write| write.op != Op::Down).collect();
        if copies.is_empty() {
            continue;
        }
        let weights = |place: &mut dyn Place, side: Side| -> Result<BTreeMap<_, _>, Error> {
            if copies.iter().any(|write| write.side == side) {
                Ok(place.weights(table)?.into_iter().collect())
            } else {
                Ok(BTreeMap::new())
            }
        };
        // Rows written in the target are copied from the source.
        let from_source = weights(&mut *source, Side::Target)?;
        let from_target = weights(&mut *target, Side::Source)?;
        for write in copies {
            let from = match write.side {
                Side::Target => &from_source,
                Side::Source => &from_target,
            };
            weight.rows += 1;
            weight.bytes += from.get(&write.key).copied().unwrap_or_default();
        }
        weight.tables += 1;
    }
    Ok(weight)
}

/// Finishes a run that was cut off while writing: each journal line whose
/// row is now at its `to` counts as written, and otherwise memory keeps its
/// `from`, so this run carries the write again or stops on a row changed
/// since the cut. The memory so finished is saved before the journal goes.
fn finish_cut_off_run(
    memory: &mut Memory,
    tables: &[Table],
    source: &mut dyn Place,
    target: &mut dyn Place,
    dryrun: bool,
) -> Result<(), Error> {
    let journaled = memory.journaled()?;
    if journaled.is_empty() {
        // A journal holding only a line torn by a crash names no row
        // written; it goes, so the next lines are not appended to it.
        if !dryrun {
            memory.clear_journal()?;
        }
        return Ok(());
    }
    for table in tables {
        let lines: Vec<&Write> = journaled
            .iter()
            .filter(|(name, _)| *name == table.name)
            .map(|(_, write)| write)
            .collect();
        if lines.is_empty() {
            continue;
        }
        let mut last = memory.table(&table.name);
        recover_side(table, &lines, Side::Source, source, &mut last)?;
        recover_side(table, &lines, Side::Target, target, &mut last)?;
        memory.set_table(&table.name, last);
    }
    if !dryrun {
        memory.save()?;
        memory.clear_journal()?;
    }
    Ok(())
}

/// Takes the journal lines for `side`, whose place is `place`, into `last`.
fn recover_side(
    table: &Table,
    lines: &[&Write],
    side: Side,
    place: &mut dyn Place,
    last: &mut Hashes,
) -> Result<(), Error> {
    let lines: Vec<&Write> = lines.iter().copied().filter(|w| w.side == side).collect();
    let keys: Vec<Vec<String>> = lines.iter().map(|w| w.key.clone()).collect();
    let here = read_hashes(table, place, &keys)?;
    for line in lines {
        recover(last, line, here.get(&line.key).copied());
    }
    Ok(())
}

/// The hash of each row `keys` names, as `place` holds it now.
fn read_hashes(
    table: &Table,
    place: &mut dyn Place,
    keys: &[Vec<String>],
) -> Result<Hashes, Error> {
    Ok(place
        .rows(table, keys)?
        .into_iter()
        .filter_map(|row| {
            let columns: Vec<&str> = row.iter().map(String::as_str).collect();
            Some((key_of(table, &row)?, row_hash(&columns)))
        })
        .collect())
}

/// Carries `writes` each way: those for the target from the source, then
/// those for the source from the target. Returns the keys of rows that read
/// back different, and the writes that were made.
#[allow(clippy::too_many_arguments)]
fn carry_both(
    table: &Table,
    source: &mut dyn Place,
    target: &mut dyn Place,
    writes: Vec<Write>,
    journal: &Memory,
    last: &Hashes,
    memory: &mut Hashes,
    wrote: &mut Wrote,
) -> Result<(Vec<Vec<String>>, Vec<Write>), Error> {
    let (to_target, to_source): (Vec<Write>, Vec<Write>) = writes
        .into_iter()
        .partition(|write| write.side == Side::Target);
    let (mut mismatched, mut written) = carry(
        table, source, target, &to_target, journal, last, memory, wrote,
    )?;
    let (more, also) = carry(
        table, target, source, &to_source, journal, last, memory, wrote,
    )?;
    mismatched.extend(more);
    written.extend(also);
    Ok((mismatched, written))
}

/// Journals and writes `writes` in `to`, with `from`'s copy of each row,
/// reads back the rows written and returns the keys of those that do not
/// match what was written, and the writes that were made. Each write the
/// place answers as made is noted in `wrote` before the rows are read back.
#[allow(clippy::too_many_arguments)]
fn carry(
    table: &Table,
    from: &mut dyn Place,
    to: &mut dyn Place,
    writes: &[Write],
    journal: &Memory,
    last: &Hashes,
    memory: &mut Hashes,
    wrote: &mut Wrote,
) -> Result<(Vec<Vec<String>>, Vec<Write>), Error> {
    let changes = changes(table, from, writes)?;
    let journaled: Vec<Write> = writes
        .iter()
        .filter(|write| changes.iter().any(|change| change.key == write.key))
        .cloned()
        .collect();
    // The journal line goes to disk before the row does.
    journal.journal(&table.name, &journaled)?;
    if !journaled.is_empty() {
        wrote.journaled = true;
    }
    let made = to.write(table, &changes)?;
    // A row not written (gone from `from`, or no longer at the change's
    // `from` in `to`) keeps its last hash, so the next run decides it from
    // fresh reads.
    for write in writes {
        if !made.contains(&write.key) {
            match last.get(&write.key) {
                Some(&hash) => memory.insert(write.key.clone(), hash),
                None => memory.remove(&write.key),
            };
        }
    }
    let written: Vec<Write> = journaled
        .into_iter()
        .filter(|write| made.contains(&write.key))
        .collect();
    wrote
        .keys
        .extend(written.iter().map(|write| write.key.clone()));
    let read_back = read_hashes(table, to, &made)?;
    Ok((not_read_back(&written, &read_back), written))
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
                types: vec!["TEXT".to_owned()],
                key: vec!["id".to_owned()],
                self_keyed: false,
            }])
        }
        fn hashes(&mut self, _: &Table) -> Result<Vec<(Key, [u8; 16])>, PlaceError> {
            Ok(self.1.clone())
        }
        fn rows(&mut self, _: &Table, _: &[Key]) -> Result<Vec<Vec<String>>, PlaceError> {
            Ok(Vec::new())
        }
        fn write(&mut self, _: &Table, _: &[Change]) -> Result<Vec<Key>, PlaceError> {
            Ok(Vec::new())
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

    /// A later sync whose table stops keeps the last hash of a row deleted
    /// in one place since, so the delete is not undone once the stop is
    /// settled.
    #[test]
    fn a_stopped_table_keeps_the_last_hash_of_a_row_deleted_since()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = std::env::temp_dir().join(format!("smugglr-stop-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        let rows = "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT); \
                    INSERT INTO notes VALUES ('n1', 'a'), ('n2', 'b');";
        let source = sqlite(&dir.join("source.db"), rows)?;
        let target = sqlite(&dir.join("target.db"), rows)?;
        let options = Options {
            config_dir: Some(dir.join("config")),
            ..Options::default()
        };
        let mut here = LocalFile::open(dir.join("source.db"))?;
        let mut there = LocalFile::open(dir.join("target.db"))?;
        sync(&mut here, &mut there, &options)?;
        let addresses = (here.address().to_owned(), there.address().to_owned());
        let open = || Memory::open(&dir.join("config"), &addresses.0, &addresses.1);
        let before = open()?.table("notes");
        source.execute_batch(
            "DELETE FROM notes WHERE id = 'n1'; UPDATE notes SET body = 'x' WHERE id = 'n2';",
        )?;
        target.execute_batch("UPDATE notes SET body = 'y' WHERE id = 'n2';")?;
        sync(&mut here, &mut there, &options)?;
        assert_eq!(open()?.table("notes"), before);
        drop((source, target));
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
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
        // notes is keyed by text: a rowid key would stop it (#507).
        let shape = "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT); \
                     CREATE TABLE tags (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT); \
                     CREATE TABLE sessions_tmp (id TEXT PRIMARY KEY, who TEXT);";
        let source = sqlite(
            &dir.join("source.db"),
            &format!(
                "{shape} CREATE TABLE cache_pages (url TEXT, body BLOB); \
                 INSERT INTO notes VALUES ('n1', 'a'), ('n2', 'b'); \
                 INSERT INTO tags (name) VALUES ('t'); \
                 INSERT INTO sessions_tmp VALUES ('s1', 'source'); \
                 INSERT INTO cache_pages VALUES ('/', x'00');"
            ),
        )?;
        let target = sqlite(
            &dir.join("target.db"),
            &format!(
                "{shape} INSERT INTO tags (name) VALUES ('t'), ('gone'); \
                 DELETE FROM tags WHERE name = 'gone';"
            ),
        )?;
        let watched = ["sqlite_sequence", "sessions_tmp"];
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
            ["cache_pages", "sessions_tmp"]
        );
        assert_eq!(record.tables.keys().collect::<Vec<_>>(), ["notes", "tags"]);
        drop((source, target));
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
