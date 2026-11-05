//! `smugglr sync <local> <place> --migratr`: migrations carried from a local
//! database to a place (FR-SMUGGLR-083).
//!
//! migratr keeps the migrations, the ledger and what is pending; smugglr owns
//! the connection and gives migratr the place through its [`Executor`]. A run
//! knows where it is from the two ledgers. With migrations pending locally it
//! syncs the places, takes and verifies a snapshot, migrates the local
//! database and stops for a check. Run again, it applies to the place what the
//! place's ledger lacks and syncs rows.
//!
//! A migration that needs a rebuild never goes to a place as migratr writes
//! it: its statements use temp tables and rely on turning foreign keys off,
//! which a hosted place may refuse or ignore, so a child's rows would be lost.
//! The place's copy of each table it touches, and of each table referencing
//! one, is dropped children first and created from the local schema, and rows
//! are sent parents first. Each table's memory is cleared before it is
//! dropped: a table memory holds nothing for cannot tell a delete, so an
//! emptied table never deletes the local rows, however the run ends.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::path::{Path, PathBuf};

use migratr::{
    AtomicError, Executor, LedgerRow, Migration, Op, RusqliteExecutor, SchemaSnapshot, SqlReader,
    TableInfo, load_dir, read_ledger_through, read_schema_through, rebuild_tables, record_applied,
    up,
};
use rusqlite::{Connection, OpenFlags};
use serde_json::Value as Json;
use smugglr_core::ignore::Ignore;
use smugglr_core::plan::{Hashes, SyncMode};
use smugglr_core::record::{Record, Status, Table as TablePart};
use smugglr_memory::Memory;
use smugglr_place::{HttpSql, Key, Place, Table, file_path, ident};
use smugglr_sync::{Options, sync};

/// migratr's ledger, which no sync touches: it is written here, as part of a
/// migration.
const LEDGER: &str = smugglr_core::ignore::MIGRATR_LEDGER;

/// One `--migratr` run.
#[derive(Debug, Clone)]
pub struct Request {
    /// The local database, as the person names it.
    pub source: String,
    /// The place, as the person names it.
    pub target: String,
    /// The migrations directory.
    pub dir: PathBuf,
    /// Whether the first sync, the snapshot, its verification and the
    /// comparison before a rebuild run.
    pub safe: bool,
    /// How its syncs run.
    pub options: Options,
}

/// Where a run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finish {
    /// The final sync is in step, or a dry run showed what a run would do.
    Done,
    /// The local database was migrated, for the person to check.
    Check,
    /// The run stopped with nothing changed by its last step.
    Stopped,
    /// The place was migrated and the final sync is not in step.
    NotDone,
}

/// What a run did.
#[derive(Debug, Clone)]
pub struct Outcome {
    /// Where it ended.
    pub finish: Finish,
    /// What it has to say, in order.
    pub said: Vec<String>,
    /// The record of its last sync, when it ran one.
    pub record: Option<Record>,
}

/// Why a run stopped short of an outcome.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// It stopped before changing anything further.
    #[error("{0}")]
    Stop(String),
    /// It stopped with the place possibly changed.
    #[error("{0}")]
    NotDone(String),
}

fn stop(err: impl Display) -> Error {
    Error::Stop(err.to_string())
}

/// What takes the snapshot: a consistent copy of the local database, named
/// by the second argument, beside it.
type Snapshot = dyn Fn(&mut RusqliteExecutor, &str) -> Result<Option<PathBuf>, rusqlite::Error>;

/// Runs `request`, with `take` taking any snapshot.
///
/// # Errors
/// [`Error::Stop`] when the run refuses or cannot go on, and
/// [`Error::NotDone`] when that happens once the place may have changed.
pub fn carry(request: &Request, take: &Snapshot) -> Result<Outcome, Error> {
    let Some(path) = file_path(&request.source).map(Path::new) else {
        return Err(Error::Stop(format!(
            "--migratr carries from a local database, and {} is not one",
            request.source
        )));
    };
    let mut run = Run {
        request,
        take,
        snapshot: snapshot_path(path),
        migrations: load_dir(&request.dir).map_err(stop)?,
        local: RusqliteExecutor::new(open_file(path)?),
        source: smugglr_place::open(&request.source).map_err(stop)?,
        target: smugglr_place::open(&request.target).map_err(stop)?,
        said: Vec::new(),
        local_wins: false,
    };
    match file_path(&request.target) {
        Some(target) => run.go(&mut RusqliteExecutor::new(open_file(Path::new(target))?)),
        None => run.go(&mut Hosted(
            smugglr_place::service(&request.target).map_err(stop)?,
        )),
    }
}

/// Opens the SQLite file at `path`, never creating it, as a place does.
fn open_file(path: &Path) -> Result<Connection, Error> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
        .map_err(|err| Error::Stop(format!("{}: {err}", path.display())))
}

/// Where the verified snapshot of the database at `local` is kept, in
/// migratr's folder beside it under a name migratr never prunes. Only a
/// snapshot that verified carries this name; what it is from before, its own
/// ledger says.
fn snapshot_path(local: &Path) -> PathBuf {
    let dir = local
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let stem = local.file_stem().unwrap_or_default().to_string_lossy();
    dir.join(".migratr")
        .join("snapshots")
        .join(format!("{stem}.smugglr.db"))
}

/// The tables that differ between two databases of the same tables: each row,
/// as its table and key, that one holds and the other does not hold the same,
/// the ledger left out.
fn differing(
    a: &mut dyn Place,
    b: &mut dyn Place,
) -> Result<Vec<(String, Key)>, smugglr_place::Error> {
    let (ours, theirs) = (a.tables()?, b.tables()?);
    let mut names: BTreeSet<&String> = ours.iter().chain(&theirs).map(|t| &t.name).collect();
    names.retain(|name| name.as_str() != LEDGER);
    let hashes = |place: &mut dyn Place, tables: &[Table], name: &str| {
        Ok::<_, smugglr_place::Error>(match tables.iter().find(|t| t.name == name) {
            Some(table) if !table.columns.is_empty() => {
                place.hashes(table)?.into_iter().collect::<Hashes>()
            }
            _ => Hashes::new(),
        })
    };
    let mut differ = Vec::new();
    for name in names {
        let here = hashes(&mut *a, &ours, name)?;
        let there = hashes(&mut *b, &theirs, name)?;
        let keys: BTreeSet<&Key> = here.keys().chain(there.keys()).collect();
        differ.extend(
            keys.into_iter()
                .filter(|key| here.get(*key) != there.get(*key))
                .map(|key| (name.clone(), key.clone())),
        );
    }
    Ok(differ)
}

/// A row, for the person to read.
fn row_name((table, key): &(String, Key)) -> String {
    format!("{table} {}", key.join(", "))
}

/// Whether a sync's record says the places are in step.
fn in_step(record: &Record) -> bool {
    record.status == Status::Done && record.refused.is_empty()
}

/// Whether a dry run's record says a real run would stop.
fn would_stop(record: &Record) -> bool {
    record.shape.is_some()
        || !record.refused.is_empty()
        || record
            .tables
            .values()
            .any(|table| matches!(table, TablePart::Stopped { .. }))
}

/// One run, as it goes.
struct Run<'a> {
    request: &'a Request,
    take: &'a Snapshot,
    snapshot: PathBuf,
    migrations: Vec<Migration>,
    local: RusqliteExecutor,
    source: Box<dyn Place>,
    target: Box<dyn Place>,
    said: Vec<String>,
    /// The place was found unchanged since the snapshot, so the local
    /// database's rows win the final sync.
    local_wins: bool,
}

impl Run<'_> {
    fn outcome(&mut self, finish: Finish, record: Option<Record>) -> Outcome {
        Outcome {
            finish,
            said: std::mem::take(&mut self.said),
            record,
        }
    }

    fn go<E: Executor>(&mut self, place: &mut E) -> Result<Outcome, Error> {
        let here = self.local.read_ledger().map_err(stop)?;
        let there = place.read_ledger().map_err(stop)?;
        // Enforced, whatever the settings say.
        for row in &there {
            if !here
                .iter()
                .any(|ours| ours.version == row.version && ours.checksum == row.checksum)
            {
                return Err(Error::Stop(format!(
                    "migration {} is in the ledger of {} and has not run on the local database \
                     with the same checksum. Only one thing fixes it: migratr up on the local \
                     database, with that migration's file. Nothing changed.",
                    row.version, self.request.target
                )));
            }
        }
        let ran = self.ran(&here)?;
        let pending = self
            .migrations
            .iter()
            .any(|m| !here.iter().any(|row| row.version == m.version));
        if pending {
            return self.first_run();
        }
        let mut behind: Vec<Migration> = ran
            .iter()
            .filter(|m| !there.iter().any(|row| row.version == m.version))
            .cloned()
            .collect();
        if behind.is_empty() {
            return self.final_sync();
        }
        let tables = self.target.tables().map_err(stop)?;
        let empty = tables
            .iter()
            .all(|t| t.name == LEDGER || t.columns.is_empty());
        let mut adopted = Vec::new();
        if there.is_empty() && !empty {
            let Some(at) = self.place_at(place, &ran)? else {
                return self.other_shape();
            };
            behind = ran[at..].to_vec();
            adopted = ran[..at].to_vec();
        }
        if self.request.safe && !empty {
            self.needs_snapshot(&behind)?;
        }
        self.record(place, &adopted)?;
        if behind.is_empty() {
            return self.final_sync();
        }
        // A dry run records nothing, so its ledger still lacks what is adopted.
        let carried = if self.request.options.dryrun {
            &ran[adopted.len()..]
        } else {
            &ran[..]
        };
        self.catch_up(place, carried, &behind)
    }

    /// The migrations the local ledger holds, each from its file, checked
    /// against the ledger: what has not run with the same checksum never
    /// reaches a place.
    fn ran(&self, here: &[LedgerRow]) -> Result<Vec<Migration>, Error> {
        here.iter()
            .map(|row| {
                self.migrations
                    .iter()
                    .find(|m| m.version == row.version && m.checksum == row.checksum)
                    .cloned()
                    .ok_or_else(|| {
                        Error::Stop(format!(
                            "migration {} in the local ledger has no file with its checksum in \
                             {}. Nothing changed.",
                            row.version,
                            self.request.dir.display()
                        ))
                    })
            })
            .collect()
    }

    /// The options a sync runs with. Under safe, with the place found
    /// unchanged since the snapshot, the local database's rows are kept in the
    /// default mode: every difference came from it.
    fn options(&self) -> Options {
        let mut options = self.request.options.clone();
        if self.local_wins && options.sync_mode == SyncMode::Stop {
            options.sync_mode = SyncMode::Source;
        }
        options
    }

    /// One sync with `options`; a run cut off part way is not done.
    fn sync(&mut self, options: &Options) -> Result<Record, Error> {
        sync(self.source.as_mut(), self.target.as_mut(), options).map_err(|err| match err {
            smugglr_sync::Error::CutOff { record, cause } => {
                Error::NotDone(format!("{record}{cause}{}", self.kept()))
            }
            other => stop(other),
        })
    }

    /// Names the snapshot, when there is one.
    fn kept(&self) -> String {
        if self.snapshot.exists() {
            format!(
                "\nThe snapshot {} is the way back.",
                self.snapshot.display()
            )
        } else {
            String::new()
        }
    }

    /// Migrations are pending locally.
    fn first_run(&mut self) -> Result<Outcome, Error> {
        if self.request.options.dryrun {
            let mut dry = Dry::new(&mut self.local);
            up(&mut dry, &self.migrations, None).map_err(stop)?;
            let sent = dry.sent;
            self.said.push(format!(
                "--dryrun: the local database would run:\n{}",
                sent.join("\n")
            ));
            return Ok(self.outcome(Finish::Done, None));
        }
        if self.request.safe {
            let options = self.options();
            let mut preview = options.clone();
            preview.dryrun = true;
            // A preview first, so a run that would stop changes neither place.
            let record = self.sync(&preview)?;
            if would_stop(&record) {
                self.said.push(
                    "The places are not in step, so no migration ran and neither place changed."
                        .to_owned(),
                );
                return Ok(self.outcome(Finish::Stopped, Some(record)));
            }
            let record = self.sync(&options)?;
            if !in_step(&record) {
                self.said
                    .push("The places are not in step, so no migration ran.".to_owned());
                return Ok(self.outcome(Finish::Stopped, Some(record)));
            }
            self.take_verified()?;
            self.said
                .push(format!("snapshot: {}", self.snapshot.display()));
        }
        let report = up(&mut self.local, &self.migrations, None).map_err(stop)?;
        self.said.push(format!(
            "The local database ran {:?}. Check it, then run the command again to carry it to {}.",
            report.applied, self.request.target
        ));
        Ok(self.outcome(Finish::Check, None))
    }

    /// Takes the snapshot under a passing name, and gives it its own only
    /// when its hashes are the local database's.
    fn take_verified(&mut self) -> Result<(), Error> {
        let file_name = self
            .snapshot
            .with_extension("tmp")
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        let passing = self.snapshot.with_file_name(&file_name);
        if passing.exists() {
            std::fs::remove_file(&passing).map_err(stop)?;
        }
        let taken = (self.take)(&mut self.local, &file_name)
            .map_err(stop)?
            .ok_or_else(|| stop("the local database cannot be copied"))?;
        let mut copy = smugglr_place::open(&taken.display().to_string()).map_err(stop)?;
        let differ = differing(self.source.as_mut(), copy.as_mut()).map_err(stop)?;
        drop(copy);
        if let Some(row) = differ.first() {
            let _ = std::fs::remove_file(&taken);
            return Err(Error::Stop(format!(
                "The snapshot does not match the local database (first at {}), so no migration \
                 ran. Nothing is lost: run again.",
                row_name(row)
            )));
        }
        std::fs::rename(&taken, &self.snapshot).map_err(stop)
    }

    /// A place with tables and no ledger: how many of the migrations `ran`
    /// its shape already has, the fewest left to run bringing it to the local
    /// shape, tried on an empty copy of its schema; `None` when no number does.
    fn place_at<E: Executor>(
        &mut self,
        place: &mut E,
        ran: &[Migration],
    ) -> Result<Option<usize>, Error> {
        let schema = place.read_schema().map_err(stop)?;
        let wanted = shape_of(&self.local.read_schema().map_err(stop)?);
        for at in (0..=ran.len()).rev() {
            let mut copy = empty_copy(&schema)?;
            if up(&mut copy, &ran[at..], None).is_ok()
                && shape_of(&copy.read_schema().map_err(stop)?) == wanted
            {
                return Ok(Some(at));
            }
        }
        Ok(None)
    }

    /// A place with tables, no ledger and a shape no migration explains: the
    /// sync's own record names the difference.
    fn other_shape(&mut self) -> Result<Outcome, Error> {
        let mut preview = self.request.options.clone();
        preview.ignore.push(LEDGER.to_owned());
        preview.dryrun = true;
        let record = self.sync(&preview)?;
        self.said.push(
            "The place has tables, no ledger and another shape, so nothing changed.".to_owned(),
        );
        Ok(self.outcome(Finish::Stopped, Some(record)))
    }

    /// Records `adopted` in the place's ledger, running none of them.
    fn record<E: Executor>(&mut self, place: &mut E, adopted: &[Migration]) -> Result<(), Error> {
        if adopted.is_empty() {
            return Ok(());
        }
        if self.request.options.dryrun {
            let mut dry = Dry::new(place);
            for migration in adopted {
                record_applied(&mut dry, migration).map_err(stop)?;
            }
            self.said
                .push(format!("--dryrun: would send:\n{}", dry.sent.join("\n")));
            return Ok(());
        }
        for migration in adopted {
            record_applied(place, migration).map_err(|err| self.not_done(err))?;
        }
        self.align(place)?;
        let names: Vec<String> = adopted
            .iter()
            .map(|m| format!("{}_{}", m.version, m.name))
            .collect();
        self.said.push(format!(
            "The place already has their shape: recorded in its ledger, running nothing: {}",
            names.join(", ")
        ));
        Ok(())
    }

    /// Under safe, the migrations the place lacks need a verified snapshot
    /// from before them.
    fn needs_snapshot(&self, behind: &[Migration]) -> Result<(), Error> {
        let none = |migration: &Migration| {
            Error::Stop(format!(
                "Migration {} ran on the local database with no verified snapshot from before \
                 it, so nothing changed on the place. Either migratr down and run smugglr again, \
                 or set safe to false under [migratr].",
                migration.version
            ))
        };
        let Some(first) = behind.first() else {
            return Ok(());
        };
        if !self.snapshot.exists() {
            return Err(none(first));
        }
        let held = RusqliteExecutor::new(open_file(&self.snapshot)?)
            .read_ledger()
            .map_err(stop)?;
        match behind
            .iter()
            .find(|m| held.iter().any(|row| row.version == m.version))
        {
            Some(migration) => Err(none(migration)),
            None => Ok(()),
        }
    }

    /// Applies to the place what its ledger lacks, then syncs.
    fn catch_up<E: Executor>(
        &mut self,
        place: &mut E,
        ran: &[Migration],
        behind: &[Migration],
    ) -> Result<Outcome, Error> {
        self.place_unchanged()?;
        let schema = place.read_schema().map_err(stop)?;
        let Some(at) = first_rebuild(&schema, behind)? else {
            if self.request.options.dryrun {
                let mut dry = Dry::new(place);
                up(&mut dry, ran, None).map_err(stop)?;
                let sent = dry.sent;
                self.said
                    .push(format!("--dryrun: would send:\n{}", sent.join("\n")));
                return Ok(self.outcome(Finish::Done, None));
            }
            up(place, ran, None).map_err(|err| self.not_done(err))?;
            self.align(place)?;
            return self.final_sync();
        };
        let local = self.local.read_schema().map_err(stop)?;
        let (before, from) = behind.split_at(at);
        no_raw_sql(from)?;
        let closed = closure(&named_tables(from), &[&schema, &local]);
        let plan = Rebuild::new(&closed, &schema, &local);
        if !plan.drop.is_empty()
            && let Some(table) = in_a_cycle(&closed, &[&schema, &local])
        {
            return Err(Error::Stop(format!(
                "A rebuild would drop {table}, and its rows might not come back: it references \
                 itself, alone or through other tables, and rows are sent one table at a time \
                 with foreign keys on. Nothing was dropped."
            )));
        }
        let to = before.last().map(|m| m.version);
        if self.request.options.dryrun {
            let mut dry = Dry::new(&mut *place);
            if to.is_some() {
                up(&mut dry, ran, to).map_err(stop)?;
            }
            let mut sent = dry.sent;
            sent.extend(plan.statements(place, from)?);
            self.said
                .push(format!("--dryrun: would send:\n{}", sent.join("\n")));
            return Ok(self.outcome(Finish::Done, None));
        }
        self.said.push(format!(
            "Rebuilding {} on {}: the app must not write to it until this run is done.",
            closed.iter().cloned().collect::<Vec<_>>().join(", "),
            self.request.target
        ));
        if to.is_some() {
            up(place, ran, to).map_err(|err| self.not_done(err))?;
        }
        let mut statements = plan.statements(place, from)?;
        statements.extend(self.same_ledger()?);
        self.forget(&closed)?;
        place
            .run_atomic(&statements, false)
            .map_err(|err| self.not_done(err.source))?;
        self.send(&plan.create)?;
        self.final_sync()
    }

    /// Under safe, before any migration reaches the place: a row the place
    /// changed since the snapshot stops the run, naming it, and a place found
    /// unchanged lets the local database's rows win the final sync.
    fn place_unchanged(&mut self) -> Result<(), Error> {
        // An empty place has no snapshot to answer to.
        if !self.request.safe || !self.snapshot.exists() {
            return Ok(());
        }
        let mut held = smugglr_place::open(&self.snapshot.display().to_string()).map_err(stop)?;
        let differ = differing(held.as_mut(), self.target.as_mut()).map_err(stop)?;
        if differ.is_empty() {
            self.local_wins = true;
            return Ok(());
        }
        let rows: Vec<String> = differ.iter().map(row_name).collect();
        Err(Error::Stop(format!(
            "The place changed since the snapshot, at {}, so nothing changed on it. Bring those \
             rows to the local database, or set safe to false under [migratr].",
            rows.join("; ")
        )))
    }

    /// The statements that give the place's ledger rows the local database's
    /// applied times, so the two ledgers are the same rows.
    fn same_ledger(&self) -> Result<Vec<String>, Error> {
        let mut statement = self
            .local
            .connection()
            .prepare(&format!("SELECT version, applied_at FROM {LEDGER}"))
            .map_err(stop)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(stop)?;
        rows.map(|row| {
            let (version, at) = row.map_err(stop)?;
            Ok(format!(
                "UPDATE {LEDGER} SET applied_at = '{}' WHERE version = {version}",
                at.replace('\'', "''")
            ))
        })
        .collect()
    }

    /// Gives the place's ledger the local database's applied times.
    fn align<E: Executor>(&self, place: &mut E) -> Result<(), Error> {
        let statements = self.same_ledger()?;
        place
            .run_atomic(&statements, false)
            .map_err(|err| self.not_done(err.source))
    }

    /// Clears the pair's memory of `tables`, before they are dropped. A pair
    /// with no memory has nothing to clear, and stays a first sync.
    fn forget(&self, tables: &BTreeSet<String>) -> Result<(), Error> {
        let dir = self
            .request
            .options
            .config_dir
            .clone()
            .or_else(smugglr_memory::config_dir)
            .ok_or_else(|| stop(smugglr_sync::Error::NoConfigDir))?;
        let mut memory =
            Memory::open(&dir, self.source.address(), self.target.address()).map_err(stop)?;
        if memory.first_sync() {
            return Ok(());
        }
        for table in tables {
            memory.set_table(table, Hashes::new());
        }
        memory.save().map_err(stop)
    }

    /// Sends the rows of each rebuilt table, parents first, the local
    /// database winning.
    fn send(&mut self, tables: &[String]) -> Result<(), Error> {
        let mut options = self.options();
        options.sync_mode = SyncMode::Source;
        let ignored = Ignore::new(options.ignore.clone());
        for table in tables.iter().filter(|table| !ignored.ignores(table)) {
            options.tables = vec![table.clone()];
            let record = self.sync(&options)?;
            if !in_step(&record) {
                return Err(Error::NotDone(format!(
                    "{record}The rows of {table} did not reach the place.{}",
                    self.kept()
                )));
            }
        }
        Ok(())
    }

    fn not_done(&self, cause: impl Display) -> Error {
        Error::NotDone(format!("{cause}{}", self.kept()))
    }

    /// The sync that says whether the run is done. Only in step is done; then
    /// the snapshot has done its work.
    fn final_sync(&mut self) -> Result<Outcome, Error> {
        let options = self.options();
        let record = self.sync(&options)?;
        if self.request.options.dryrun {
            let finish = if would_stop(&record) {
                Finish::Stopped
            } else {
                Finish::Done
            };
            return Ok(self.outcome(finish, Some(record)));
        }
        if in_step(&record) {
            if self.snapshot.exists() {
                std::fs::remove_file(&self.snapshot).map_err(stop)?;
            }
            self.said.push("in step".to_owned());
            return Ok(self.outcome(Finish::Done, Some(record)));
        }
        self.said.push(format!(
            "Not done: the places are not in step.{}",
            self.kept()
        ));
        Ok(self.outcome(Finish::NotDone, Some(record)))
    }
}

/// An empty in-memory database of `schema`, where a migration can be tried.
fn empty_copy(schema: &SchemaSnapshot) -> Result<RusqliteExecutor, Error> {
    let connection = Connection::open_in_memory().map_err(stop)?;
    for object in &schema.objects {
        if object.name.starts_with("sqlite_") || object.name == LEDGER {
            continue;
        }
        if let Some(sql) = &object.sql {
            connection.execute_batch(sql).map_err(stop)?;
        }
    }
    Ok(RusqliteExecutor::new(connection))
}

/// What a table is, beside its rows: each column as declared, its foreign
/// keys, and how many indexes it has (a UNIQUE column holds one).
type TableShape = (Vec<String>, BTreeSet<String>, usize);

/// Each table's shape, without the ledger. More than a sync compares: a
/// migration can change a table and leave its columns' names and types as
/// they were, and such a place has not run it.
fn shape_of(schema: &SchemaSnapshot) -> BTreeMap<String, TableShape> {
    schema
        .tables
        .iter()
        .filter(|table| table.name != LEDGER && !table.name.starts_with("sqlite_"))
        .map(|table| {
            let columns = table.columns.iter().map(|c| format!("{c:?}")).collect();
            let keys = table
                .foreign_keys
                .iter()
                .map(|key| {
                    format!(
                        "{} {} {:?} {} {}",
                        key.table, key.from, key.to, key.on_update, key.on_delete
                    )
                })
                .collect();
            let indexes = schema
                .objects
                .iter()
                .filter(|o| o.kind == "index" && o.tbl_name == table.name)
                .count();
            (table.name.clone(), (columns, keys, indexes))
        })
        .collect()
}

/// Where in `behind` the first migration that needs a rebuild is, tried on an
/// empty copy of the place's schema: each decision needs the schema as the
/// migrations before it left it.
fn first_rebuild(schema: &SchemaSnapshot, behind: &[Migration]) -> Result<Option<usize>, Error> {
    let mut copy = empty_copy(schema)?;
    for (at, migration) in behind.iter().enumerate() {
        let before = copy.read_schema().map_err(stop)?;
        if !rebuild_tables(&before, migration).map_err(stop)?.is_empty() {
            return Ok(Some(at));
        }
        up(&mut copy, &behind[..=at], None).map_err(stop)?;
    }
    Ok(None)
}

/// From the first rebuild on, migrations are recorded and not run, and raw SQL
/// cannot be seen into, so its work would be lost behind a true-looking ledger.
fn no_raw_sql(migrations: &[Migration]) -> Result<(), Error> {
    match migrations
        .iter()
        .find(|m| m.up.iter().any(|op| matches!(op, Op::RawSql { .. })))
    {
        Some(migration) => Err(Error::Stop(format!(
            "Migration {}_{} holds raw SQL at or after a rebuild, which smugglr cannot carry, so \
             nothing changed on the place. Carry the rebuild in a run of its own, before any \
             migration with raw SQL, and keep raw SQL out of a migration that rebuilds.",
            migration.version, migration.name
        ))),
        None => Ok(()),
    }
}

/// A table of `tables` that references itself, directly or through others, in
/// any of `schemas`.
fn in_a_cycle<'a>(tables: &'a BTreeSet<String>, schemas: &[&SchemaSnapshot]) -> Option<&'a String> {
    let parents = |name: &str| -> Vec<String> {
        schemas
            .iter()
            .flat_map(|schema| &schema.tables)
            .filter(|table| table.name.eq_ignore_ascii_case(name))
            .flat_map(|table| &table.foreign_keys)
            .map(|key| key.table.to_ascii_lowercase())
            .collect()
    };
    tables.iter().find(|table| {
        let start = table.to_ascii_lowercase();
        let mut seen = BTreeSet::new();
        let mut waiting = parents(&start);
        while let Some(parent) = waiting.pop() {
            if parent == start {
                return true;
            }
            if seen.insert(parent.clone()) {
                waiting.extend(parents(&parent));
            }
        }
        false
    })
}

/// The tables `migrations` name.
fn named_tables(migrations: &[Migration]) -> BTreeSet<String> {
    let mut tables = BTreeSet::new();
    for migration in migrations {
        for op in &migration.up {
            match op {
                Op::CreateTable { table, .. }
                | Op::DropTable { table, .. }
                | Op::AddColumn { table, .. }
                | Op::DropColumn { table, .. }
                | Op::RenameColumn { table, .. }
                | Op::CreateIndex { table, .. } => {
                    tables.insert(table.clone());
                }
                Op::RenameTable { from, to } => {
                    tables.insert(from.clone());
                    tables.insert(to.clone());
                }
                Op::DropIndex { definition } => {
                    tables.insert(definition.table.clone());
                }
                Op::RawSql { .. } => {}
            }
        }
    }
    tables
}

/// `tables` and every table referencing one, in any of `schemas`.
fn closure(tables: &BTreeSet<String>, schemas: &[&SchemaSnapshot]) -> BTreeSet<String> {
    let mut closed = tables.clone();
    loop {
        let more: Vec<String> = schemas
            .iter()
            .flat_map(|schema| &schema.tables)
            .filter(|table| {
                !closed.contains(&table.name)
                    && table
                        .foreign_keys
                        .iter()
                        .any(|key| closed.iter().any(|t| t.eq_ignore_ascii_case(&key.table)))
            })
            .map(|table| table.name.clone())
            .collect();
        if more.is_empty() {
            return closed;
        }
        closed.extend(more);
    }
}

/// The tables of `schema` in `set`, each after the tables it references.
fn parents_first(schema: &SchemaSnapshot, set: &BTreeSet<String>) -> Vec<String> {
    fn visit(
        schema: &SchemaSnapshot,
        table: &TableInfo,
        order: &mut Vec<String>,
        seen: &mut BTreeSet<String>,
        set: &BTreeSet<String>,
    ) {
        if !seen.insert(table.name.clone()) {
            return;
        }
        for key in &table.foreign_keys {
            if let Some(parent) = schema
                .tables
                .iter()
                .find(|t| t.name.eq_ignore_ascii_case(&key.table) && set.contains(&t.name))
            {
                visit(schema, parent, order, seen, set);
            }
        }
        order.push(table.name.clone());
    }
    let (mut order, mut seen) = (Vec::new(), BTreeSet::new());
    for table in schema.tables.iter().filter(|t| set.contains(&t.name)) {
        visit(schema, table, &mut order, &mut seen, set);
    }
    order
}

/// A rebuild on the place.
struct Rebuild {
    /// The place's tables to drop, children first.
    drop: Vec<String>,
    /// The tables to create, parents first.
    create: Vec<String>,
    /// What creates them and their indexes and triggers.
    objects: Vec<String>,
}

impl Rebuild {
    fn new(tables: &BTreeSet<String>, there: &SchemaSnapshot, here: &SchemaSnapshot) -> Self {
        let mut drop = parents_first(there, tables);
        drop.reverse();
        let create = parents_first(here, tables);
        let mut objects: Vec<String> = Vec::new();
        for table in &create {
            objects.extend(
                here.objects
                    .iter()
                    .filter(|object| object.kind == "table" && &object.name == table)
                    .filter_map(|object| object.sql.clone()),
            );
        }
        for kind in ["index", "trigger"] {
            objects.extend(
                here.objects
                    .iter()
                    .filter(|object| object.kind == kind && create.contains(&object.tbl_name))
                    .filter_map(|object| object.sql.clone()),
            );
        }
        Self {
            drop,
            create,
            objects,
        }
    }

    /// The statements of the rebuild, with `migrations` recorded in the
    /// ledger, to run as one batch: drops, creates, ledger rows.
    fn statements<E: Executor>(
        &self,
        place: &mut E,
        migrations: &[Migration],
    ) -> Result<Vec<String>, Error> {
        let mut statements: Vec<String> = self
            .drop
            .iter()
            .map(|table| format!("DROP TABLE {}", ident(table)))
            .collect();
        statements.extend(self.objects.iter().cloned());
        let mut ledger = Dry::new(place);
        for migration in migrations {
            record_applied(&mut ledger, migration).map_err(stop)?;
        }
        statements.append(&mut ledger.sent);
        Ok(statements)
    }
}

/// An executor that answers reads from `inner` and only keeps what it would
/// have run, for a dry run and for a batch made of migratr's own statements.
#[derive(Debug)]
struct Dry<'a, E> {
    inner: &'a mut E,
    sent: Vec<String>,
}

impl<'a, E> Dry<'a, E> {
    fn new(inner: &'a mut E) -> Self {
        Self {
            inner,
            sent: Vec::new(),
        }
    }
}

impl<E: Executor> Executor for Dry<'_, E> {
    type Error = E::Error;

    fn read_schema(&mut self) -> Result<SchemaSnapshot, Self::Error> {
        self.inner.read_schema()
    }

    fn read_ledger(&mut self) -> Result<Vec<LedgerRow>, Self::Error> {
        self.inner.read_ledger()
    }

    fn run_atomic(
        &mut self,
        statements: &[String],
        _suspend_foreign_keys: bool,
    ) -> Result<(), AtomicError<Self::Error>> {
        self.sent.extend_from_slice(statements);
        Ok(())
    }

    fn snapshot(&mut self, _file_name: &str) -> Result<Option<PathBuf>, Self::Error> {
        Ok(None)
    }
}

/// A service that speaks HTTP-SQL, as migratr's [`Executor`]. Nothing here
/// knows which service it is.
#[derive(Debug)]
struct Hosted(HttpSql);

impl SqlReader for Hosted {
    type Error = smugglr_place::Error;

    fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Json>>, Self::Error> {
        self.0.query(sql, &[])
    }

    fn malformed(&self, what: &str) -> Self::Error {
        smugglr_place::Error::Adapter(format!("{}: {what}", self.0.address()))
    }
}

impl Executor for Hosted {
    type Error = smugglr_place::Error;

    fn read_schema(&mut self) -> Result<SchemaSnapshot, Self::Error> {
        let own = Ignore::new(self.0.own_tables().iter().copied());
        read_schema_through(self, |table| own.ignores(table))
    }

    fn read_ledger(&mut self) -> Result<Vec<LedgerRow>, Self::Error> {
        read_ledger_through(self)
    }

    fn run_atomic(
        &mut self,
        statements: &[String],
        suspend_foreign_keys: bool,
    ) -> Result<(), AtomicError<Self::Error>> {
        // migratr asks this of a rebuild, which never goes to a place as
        // migratr writes it: a place may not honour it, and children die.
        if suspend_foreign_keys {
            return Err(AtomicError::outside_statements(
                smugglr_place::Error::Adapter(
                    "a place is not asked to turn foreign keys off".to_owned(),
                ),
            ));
        }
        let batch: Vec<_> = statements
            .iter()
            .map(|sql| (sql.clone(), Vec::new()))
            .collect();
        self.0
            .batch(&batch, true)
            .map(drop)
            .map_err(AtomicError::outside_statements)
    }

    fn snapshot(&mut self, _file_name: &str) -> Result<Option<PathBuf>, Self::Error> {
        // The way back is the snapshot of the local database.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use migratr::{ColumnInfo, ForeignKeyInfo, SchemaObject, SchemaSnapshot, TableInfo};

    use super::{Rebuild, closure, parents_first, snapshot_path};

    fn table(name: &str, parent: Option<&str>) -> TableInfo {
        TableInfo {
            name: name.to_owned(),
            columns: Vec::<ColumnInfo>::new(),
            foreign_keys: parent
                .map(|parent| ForeignKeyInfo {
                    id: 0,
                    seq: 0,
                    table: parent.to_owned(),
                    from: "p".to_owned(),
                    to: None,
                    on_update: String::new(),
                    on_delete: String::new(),
                    match_clause: String::new(),
                })
                .into_iter()
                .collect(),
        }
    }

    fn family() -> SchemaSnapshot {
        SchemaSnapshot {
            objects: vec![SchemaObject {
                kind: "table".to_owned(),
                name: "a_child".to_owned(),
                tbl_name: "a_child".to_owned(),
                sql: Some("CREATE TABLE a_child (id TEXT)".to_owned()),
            }],
            tables: vec![
                table("a_child", Some("Parent")),
                table("grandchild", Some("a_child")),
                table("parent", None),
                table("other", None),
            ],
        }
    }

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn the_closure_takes_every_table_that_references_one() {
        assert_eq!(
            closure(&set(&["parent"]), &[&family()]),
            set(&["a_child", "grandchild", "parent"])
        );
    }

    #[test]
    fn parents_come_before_children_whatever_their_names() {
        let tables = set(&["a_child", "grandchild", "parent"]);
        assert_eq!(
            parents_first(&family(), &tables),
            vec!["parent", "a_child", "grandchild"]
        );
    }

    #[test]
    fn a_rebuild_drops_children_first_and_creates_only_what_the_local_schema_holds() {
        let tables = set(&["a_child", "grandchild", "parent"]);
        let plan = Rebuild::new(&tables, &family(), &family());
        assert_eq!(plan.drop, vec!["grandchild", "a_child", "parent"]);
        assert_eq!(plan.objects, vec!["CREATE TABLE a_child (id TEXT)"]);
    }

    #[test]
    fn the_snapshot_has_a_name_migratr_does_not_prune() {
        let path = snapshot_path(std::path::Path::new("/d/20260101000000_1_up.db"));
        let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
        assert_eq!(name.as_deref(), Some("20260101000000_1_up.smugglr.db"));
    }
}
