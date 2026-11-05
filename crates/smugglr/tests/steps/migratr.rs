//! Steps for `smugglr sync --migratr` (FR-SMUGGLR-083): a local SQLite file,
//! and a place that is the stand-in of D1's API, which enforces foreign keys
//! and refuses temp tables, or where a step says so a second local file.

use std::path::PathBuf;
use std::process::Output;

use migratr::{Executor, RusqliteExecutor, load_dir, up};
use rstest_bdd_macros::{given, then, when};
use rusqlite::Connection;
use serde_json::Value;
use smugglr::migratr::{Request, carry};
use smugglr_core::plan::SyncMode;
use smugglr_sync::Options;

use crate::d1::d1;
use crate::setup::{NOTES, THREE_NOTES, World, rows_of, sql_on};

/// Adds a column, which needs no rebuild.
const ADD_TAG: &str = r#"{"up": [{"op": "add_column", "table": "notes",
    "column": {"name": "tag", "type": "TEXT"}}]}"#;
const ADD_TAG_FILE: &str = "20260101000000_add_tag.json";
const VERSION: &str = "20260101000000";

/// A parent whose `code` is UNIQUE, so dropping it needs a rebuild.
const PARENT: &str = "CREATE TABLE parent (id TEXT PRIMARY KEY, code TEXT UNIQUE, name TEXT); \
     INSERT INTO parent VALUES ('p1', 'a', 'one'), ('p2', 'b', 'two')";
const CHILD: &str = "CREATE TABLE child (id TEXT PRIMARY KEY, \
     parent_id TEXT REFERENCES parent(id) ON DELETE CASCADE); \
     INSERT INTO child VALUES ('c1', 'p1'), ('c2', 'p1'), ('c3', 'p2')";
const SELF_KEYED_CHILD: &str = "CREATE TABLE child (id INTEGER PRIMARY KEY, \
     parent_id TEXT REFERENCES parent(id)); INSERT INTO child VALUES (1, 'p1')";
const DROP_CODE: &str = r#"{"up": [{"op": "drop_column", "table": "parent",
    "column": {"name": "code", "type": "TEXT"}}]}"#;
const DROP_FILE: &str = "20260201000000_drop_code.json";
/// A rebuild that leaves the same column names, types and order: `code`
/// goes, UNIQUE with it, and comes back.
const SAME_COLUMNS: &str = r#"{"up": [
    {"op": "drop_column", "table": "kept", "column": {"name": "code", "type": "TEXT"}},
    {"op": "add_column", "table": "kept", "column": {"name": "code", "type": "TEXT"}}]}"#;
/// A table whose last column is UNIQUE and empty, so the rebuild leaves every
/// row's hash as it was and only cleared memory keeps the local rows.
const KEPT: &str = "CREATE TABLE kept (id TEXT PRIMARY KEY, name TEXT, code TEXT UNIQUE); \
     INSERT INTO kept VALUES ('p1', 'one', NULL), ('p2', 'two', NULL)";

fn dir(world: &World) -> Result<PathBuf, String> {
    let dir = world.folder()?.join("migrations");
    std::fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
    Ok(dir)
}

fn migration(world: &World, file: &str, text: &str) -> Result<(), String> {
    std::fs::write(dir(world)?.join(file), text).map_err(|err| err.to_string())
}

fn local(world: &World) -> Result<String, String> {
    world.source.get().ok_or_else(|| "no local".to_owned())
}

/// The place's SQLite file, behind the stand-in or itself the place.
fn place_file(world: &World) -> Result<String, String> {
    world.service.get().ok_or_else(|| "no place".to_owned())
}

/// A local database running `here`, and a place running `there` behind the
/// stand-in of D1.
fn pair(world: &World, here: &str, there: &str) -> Result<(), String> {
    world.source.set(world.place("source.db", here)?);
    let (address, _) = d1(world, "shop", "place.db", there)?;
    world
        .service
        .set(world.folder()?.join("place.db").display().to_string());
    world.target.set(address);
    Ok(())
}

/// A local database running `here`, and a place that is the file running
/// `there`.
fn file_pair(world: &World, here: &str, there: &str) -> Result<(), String> {
    world.source.set(world.place("source.db", here)?);
    let path = world.place("place.db", there)?;
    world.service.set(path.clone());
    world.target.set(path);
    Ok(())
}

fn with_migratr(world: &World, more: &[&str]) -> Result<Output, String> {
    let dir = dir(world)?.display().to_string();
    let mut flags = vec!["--migratr", dir.as_str()];
    flags.extend_from_slice(more);
    world.sync(&flags)
}

/// A run stopped for its check, as a first run does; anything else is a
/// failed setup.
fn first_run(world: &World) -> Result<(), String> {
    let output = with_migratr(world, &[])?;
    let first = said(world)?;
    world.first_said.set(first.clone());
    if first.contains("run the command again") {
        Ok(())
    } else {
        Err(format!(
            "the first run did not stop for a check: {output:?}"
        ))
    }
}

/// migratr's own `up` on the local database, outside smugglr.
fn migratr_up(world: &World) -> Result<(), String> {
    let connection = Connection::open(local(world)?).map_err(|err| err.to_string())?;
    let migrations = load_dir(&dir(world)?).map_err(|err| err.to_string())?;
    up(&mut RusqliteExecutor::new(connection), &migrations, None)
        .map(drop)
        .map_err(|err| err.to_string())
}

fn ledger(path: &str) -> Result<Vec<String>, String> {
    let connection = Connection::open(path).map_err(|err| err.to_string())?;
    let rows = RusqliteExecutor::new(connection)
        .read_ledger()
        .map_err(|err| err.to_string())?;
    Ok(rows.iter().map(|row| row.version.to_string()).collect())
}

fn snapshot(world: &World) -> Result<PathBuf, String> {
    Ok(world
        .folder()?
        .join(".migratr")
        .join("snapshots")
        .join("source.smugglr.db"))
}

/// Everything a SQLite file holds.
fn state(path: &str) -> Result<String, String> {
    let mut all = vec![format!(
        "{:?}",
        rows_of(
            path,
            "SELECT type, name, sql FROM sqlite_schema ORDER BY name"
        )?
    )];
    let connection = Connection::open(path).map_err(|err| err.to_string())?;
    let names: Vec<String> = connection
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
        .and_then(|mut statement| {
            statement
                .query_map([], |row| row.get(0))
                .and_then(Iterator::collect)
        })
        .map_err(|err| err.to_string())?;
    for name in names {
        all.push(format!(
            "{name}: {:?}",
            rows_of(path, &format!("SELECT * FROM \"{name}\" ORDER BY 1"))?
        ));
    }
    Ok(all.join("\n"))
}

fn columns(path: &str, table: &str) -> Result<Vec<String>, String> {
    let connection = Connection::open(path).map_err(|err| err.to_string())?;
    connection
        .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
        .and_then(|mut statement| {
            statement
                .query_map([], |row| row.get(0))
                .and_then(Iterator::collect)
        })
        .map_err(|err| err.to_string())
}

fn count(path: &str, table: &str) -> Result<usize, String> {
    Ok(rows_of(path, &format!("SELECT * FROM {table}"))?.len())
}

/// All a run printed.
fn said(world: &World) -> Result<String, String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    Ok(format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

fn says(world: &World, wanted: &str) -> Result<(), String> {
    let text = said(world)?;
    if text.contains(wanted) {
        Ok(())
    } else {
        Err(format!("expected {wanted:?} in: {text}"))
    }
}

/// What a scenario's When saw of the places before it ran: (local, place).
fn before(world: &World) -> Result<(String, String), String> {
    world.carried.get().ok_or_else(|| "no run".to_owned())
}

fn unchanged(what: &str, was: &str, is: &str) -> Result<(), String> {
    if was == is {
        Ok(())
    } else {
        Err(format!("{what} changed:\nwas {was}\nis  {is}"))
    }
}

fn run_with(world: &World, more: &[&str]) -> Result<(), String> {
    world
        .carried
        .set((state(&local(world)?)?, state(&place_file(world)?)?));
    // A snapshot made to come out differing needs the run inside this process,
    // where what takes the snapshot can be given.
    if world.records.take().is_some() {
        return run_in_process(world);
    }
    with_migratr(world, more).map(drop)
}

fn run_in_process(world: &World) -> Result<(), String> {
    let request = Request {
        source: local(world)?,
        target: world.target.get().ok_or("no target")?,
        dir: dir(world)?,
        safe: true,
        options: Options {
            config_dir: Some(world.config()?),
            sync_mode: SyncMode::Stop,
            ..Options::default()
        },
    };
    let differing = |local: &mut RusqliteExecutor, name: &str| {
        let taken = local.snapshot(name)?;
        if let Some(path) = &taken {
            Connection::open(path)?.execute_batch("UPDATE notes SET body = 'other'")?;
        }
        Ok(taken)
    };
    let text = match carry(&request, &differing) {
        Ok(outcome) => outcome.said.join("\n"),
        Err(err) => err.to_string(),
    };
    world.output.set(Output {
        status: std::process::ExitStatus::default(),
        stdout: text.into_bytes(),
        stderr: Vec::new(),
    });
    Ok(())
}

// Givens.

fn notes() -> String {
    format!("{NOTES}; {THREE_NOTES}")
}

#[given("a local database and a place in step")]
fn in_step(world: &World) -> Result<(), String> {
    pair(world, &notes(), &notes())
}

#[given("a migrations directory holding a migration the local database has not run")]
fn pending_migration(world: &World) -> Result<(), String> {
    migration(world, ADD_TAG_FILE, ADD_TAG)
}

#[given("a local database that has run a migration through the first run, with a new row")]
fn first_run_with_a_new_row(world: &World) -> Result<(), String> {
    pair(world, &notes(), &notes())?;
    migration(world, ADD_TAG_FILE, ADD_TAG)?;
    first_run(world)?;
    sql_on(
        &local(world)?,
        "INSERT INTO notes VALUES ('n4', 'new', 4, 'tagged')",
    )
}

#[given("a place that has not run it")]
fn place_has_not_run_it(world: &World) -> Result<(), String> {
    if ledger(&place_file(world)?)?.is_empty() {
        Ok(())
    } else {
        Err("the place has run it".to_owned())
    }
}

#[given("a place that has not")]
fn place_has_not(world: &World) -> Result<(), String> {
    place_has_not_run_it(world)
}

#[given("a place whose ledger holds a migration the local ledger lacks")]
fn place_ledger_ahead(world: &World) -> Result<(), String> {
    dir(world)?;
    let ahead = format!(
        "{}; CREATE TABLE _migratr_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, \
         checksum TEXT NOT NULL, applied_at TEXT NOT NULL); \
         INSERT INTO _migratr_migrations VALUES ({VERSION}, 'ahead', 'abc', 'then')",
        notes()
    );
    file_pair(world, &notes(), &ahead)
}

/// The migration that creates what [`NOTES`] creates.
const CREATE_NOTES: &str = r#"{"up": [{"op": "create_table", "table": "notes", "columns": [
    {"name": "id", "type": "TEXT", "primary_key": 1},
    {"name": "body", "type": "TEXT"},
    {"name": "created_at", "type": "INTEGER"}]}]}"#;

#[given("a local database that has run its migrations")]
fn local_ran_its_migrations(world: &World) -> Result<(), String> {
    world.source.set(world.place("source.db", "")?);
    migration(world, "20251201000000_create_notes.json", CREATE_NOTES)?;
    migratr_up(world)?;
    sql_on(&local(world)?, THREE_NOTES)
}

#[given("a place with the same tables and rows and no ledger")]
fn place_same_no_ledger(world: &World) -> Result<(), String> {
    let path = world.place("place.db", &notes())?;
    world.service.set(path.clone());
    world.target.set(path);
    Ok(())
}

#[given("a place with no ledger and a column the local database lacks")]
fn place_other_shape(world: &World) -> Result<(), String> {
    let path = world.place(
        "place.db",
        "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT, created_at INTEGER, stray TEXT)",
    )?;
    world.service.set(path.clone());
    world.target.set(path);
    Ok(())
}

#[given("a local database that has run two migrations and holds rows")]
fn local_ran_two(world: &World) -> Result<(), String> {
    local_ran_its_migrations(world)?;
    migration(world, ADD_TAG_FILE, ADD_TAG)?;
    migratr_up(world)
}

#[given("a place with no tables")]
fn empty_place(world: &World) -> Result<(), String> {
    let (address, _) = d1(world, "shop", "place.db", "")?;
    world
        .service
        .set(world.folder()?.join("place.db").display().to_string());
    world.target.set(address);
    Ok(())
}

#[given("a local database and a place with a row changed in both")]
fn row_changed_in_both(world: &World) -> Result<(), String> {
    pair(world, &notes(), &notes())?;
    world.sync_ok()?;
    sql_on(
        &local(world)?,
        "UPDATE notes SET body = 'here' WHERE id = 'n1'",
    )?;
    sql_on(
        &place_file(world)?,
        "UPDATE notes SET body = 'there' WHERE id = 'n1'",
    )?;
    world.watched.set("n1".to_owned());
    Ok(())
}

#[given("a snapshot that comes out differing from the local database")]
fn snapshot_differs(world: &World) -> Result<(), String> {
    // In process, so the place's file itself: no variables to carry.
    world.target.set(place_file(world)?);
    migration(world, ADD_TAG_FILE, ADD_TAG)?;
    world.records.set(Vec::new());
    Ok(())
}

#[given(
    "a parent table with a child table with ON DELETE CASCADE, in step on a local database and a place"
)]
fn parent_and_child(world: &World) -> Result<(), String> {
    let both = format!("{PARENT}; {CHILD}");
    pair(world, &both, &both)?;
    world.sync_ok()
}

#[given("a child table keyed by INTEGER PRIMARY KEY that references a parent")]
fn self_keyed_child(world: &World) -> Result<(), String> {
    let both = format!("{PARENT}; {SELF_KEYED_CHILD}");
    pair(world, &both, &both)?;
    world.sync(&[]).map(drop)
}

#[given("a migration that needs a rebuild of the parent, run on the local database")]
fn rebuild_of_parent(world: &World) -> Result<(), String> {
    migration(world, DROP_FILE, DROP_CODE)?;
    first_run(world)
}

#[given("a table in step on a local database and a place")]
fn table_in_step(world: &World) -> Result<(), String> {
    pair(world, KEPT, KEPT)?;
    world.sync_ok()
}

#[given(
    "a migration that needs a rebuild and leaves the columns as they were, run on the local database"
)]
fn rebuild_same_columns(world: &World) -> Result<(), String> {
    migration(world, DROP_FILE, SAME_COLUMNS)?;
    first_run(world)?;
    let columns = columns(&local(world)?, "kept")?;
    if columns == ["id", "name", "code"] {
        Ok(())
    } else {
        Err(format!("the columns came out {columns:?}"))
    }
}

#[given("a local database that has run a rebuild through the first run")]
fn ran_a_rebuild(world: &World) -> Result<(), String> {
    table_in_step(world)?;
    migration(world, DROP_FILE, SAME_COLUMNS)?;
    first_run(world)
}

#[given("a place with a row changed since the snapshot")]
fn place_moved(world: &World) -> Result<(), String> {
    // The rebuild scenarios hold a kept table; the others hold notes.
    let place = place_file(world)?;
    if columns(&place, "kept").is_ok_and(|columns| !columns.is_empty()) {
        sql_on(&place, "UPDATE kept SET name = 'moved' WHERE id = 'p1'")?;
        world.watched.set("p1".to_owned());
    } else {
        sql_on(&place, "UPDATE notes SET body = 'moved' WHERE id = 'n2'")?;
        world.watched.set("n2".to_owned());
    }
    Ok(())
}

#[given("a new row on the local database in the table keyed by INTEGER PRIMARY KEY")]
fn new_counter(world: &World) -> Result<(), String> {
    sql_on(&local(world)?, "INSERT INTO counters VALUES (2, 7)")?;
    world.new_row.set((
        "counters".to_owned(),
        "SELECT n FROM counters WHERE id = 2".to_owned(),
    ));
    Ok(())
}

#[given("a local database and a place after a run that carried a migration")]
fn after_a_carry(world: &World) -> Result<(), String> {
    pair(world, &notes(), &notes())?;
    migration(world, ADD_TAG_FILE, ADD_TAG)?;
    first_run(world)?;
    let output = with_migratr(world, &[])?;
    if output.status.code() == Some(0) {
        Ok(())
    } else {
        Err(format!("the carry was not done: {}", said(world)?))
    }
}

#[given("a local database that has run an added column through the first run")]
fn added_column_first_run(world: &World) -> Result<(), String> {
    pair(world, &notes(), &notes())?;
    migration(world, ADD_TAG_FILE, ADD_TAG)?;
    first_run(world)
}

#[given("a row edited on the local database after that run")]
fn edited_locally(world: &World) -> Result<(), String> {
    sql_on(
        &local(world)?,
        "UPDATE notes SET body = 'edited here' WHERE id = 'n1'",
    )
}

#[given("a config with safe set to false under migratr")]
fn safe_off(world: &World) -> Result<(), String> {
    let config = world.config()?;
    std::fs::create_dir_all(&config).map_err(|err| err.to_string())?;
    let dir = dir(world)?.display().to_string().replace('\\', "\\\\");
    std::fs::write(
        config.join("config.toml"),
        format!("[migratr]\ndir = \"{dir}\"\nsafe = false\n"),
    )
    .map_err(|err| err.to_string())
}

#[given("a local database that has run a migration")]
fn local_ran_one(world: &World) -> Result<(), String> {
    pair(world, &notes(), &notes())?;
    migration(world, ADD_TAG_FILE, ADD_TAG)?;
    first_run(world)
}

#[given(
    "a local database and a place whose rows differ after the migration in a way the sync stops on"
)]
fn rows_differ_after(world: &World) -> Result<(), String> {
    pair(world, &notes(), &notes())?;
    world.sync_ok()?;
    migration(world, ADD_TAG_FILE, ADD_TAG)?;
    first_run(world)?;
    // Under safe, a row the place changed stops the run before the place is
    // migrated, so the final sync can only stop on one with safe off. The
    // snapshot from the first run stays.
    safe_off(world)?;
    sql_on(
        &local(world)?,
        "UPDATE notes SET body = 'here' WHERE id = 'n1'",
    )?;
    sql_on(
        &place_file(world)?,
        "UPDATE notes SET body = 'there' WHERE id = 'n1'",
    )
}

#[given(
    "a local database and a place with a table keyed by INTEGER PRIMARY KEY and a table keyed by text"
)]
fn self_keyed_and_text(world: &World) -> Result<(), String> {
    let both = format!(
        "{}; CREATE TABLE counters (id INTEGER PRIMARY KEY, n INTEGER); \
         INSERT INTO counters VALUES (1, 1)",
        notes()
    );
    pair(world, &both, &both)
}

#[given("a migration the local database has run and the place has not")]
fn ran_here_not_there(world: &World) -> Result<(), String> {
    migration(world, ADD_TAG_FILE, ADD_TAG)?;
    first_run(world)
}

#[given("a local database that has run a migration through migratr up")]
fn ran_through_migratr_up(world: &World) -> Result<(), String> {
    pair(world, &notes(), &notes())?;
    migration(world, ADD_TAG_FILE, ADD_TAG)?;
    migratr_up(world)
}

// Whens.

#[when("the agent runs smugglr sync on them with --migratr")]
fn run(world: &World) -> Result<(), String> {
    run_with(world, &[])
}

#[when("the agent runs smugglr sync on them with --migratr and --json")]
fn run_json(world: &World) -> Result<(), String> {
    run_with(world, &["--json"])
}

#[when("the agent runs smugglr sync on them with --migratr and --dryrun")]
fn run_dry(world: &World) -> Result<(), String> {
    run_with(world, &["--dryrun"])
}

// Thens.

#[then("the local database has run the migration")]
fn local_ran(world: &World) -> Result<(), String> {
    let ledger = ledger(&local(world)?)?;
    if ledger == [VERSION] && columns(&local(world)?, "notes")?.contains(&"tag".to_owned()) {
        Ok(())
    } else {
        Err(format!("the local ledger holds {ledger:?}"))
    }
}

#[then("a snapshot of the local database from before the migration exists")]
fn snapshot_from_before(world: &World) -> Result<(), String> {
    let path = snapshot(world)?.display().to_string();
    if !ledger(&path)?.is_empty() || columns(&path, "notes")?.contains(&"tag".to_owned()) {
        return Err("the snapshot is not from before the migration".to_owned());
    }
    Ok(())
}

#[then("the place has not changed")]
fn place_unchanged(world: &World) -> Result<(), String> {
    unchanged("the place", &before(world)?.1, &state(&place_file(world)?)?)
}

#[then("the output says to check the local database and run the command again")]
fn says_check(world: &World) -> Result<(), String> {
    says(world, "Check it")?;
    says(world, "run the command again")
}

#[then("the place has run the migration")]
fn place_ran(world: &World) -> Result<(), String> {
    if columns(&place_file(world)?, "notes")?.contains(&"tag".to_owned()) {
        Ok(())
    } else {
        Err(format!("no tag column: {}", said(world)?))
    }
}

#[then("the place's ledger holds the migration")]
fn place_ledger_holds(world: &World) -> Result<(), String> {
    let held = ledger(&place_file(world)?)?;
    if held == [VERSION] {
        Ok(())
    } else {
        Err(format!("the place's ledger holds {held:?}"))
    }
}

#[then("the place holds the new row")]
fn place_new_row(world: &World) -> Result<(), String> {
    let query = world.new_row.get().map_or_else(
        || "SELECT tag FROM notes WHERE id = 'n4'".to_owned(),
        |(_, query)| query,
    );
    let rows = rows_of(&place_file(world)?, &query)?;
    if rows.len() == 1 {
        Ok(())
    } else {
        Err(format!("no new row: {}", said(world)?))
    }
}

#[then("the run says in step")]
fn says_in_step(world: &World) -> Result<(), String> {
    // A whole line: "not in step" holds the same words. A --migratr run says
    // "in step"; a plain sync's record says it is done.
    let text = said(world)?;
    if text
        .lines()
        .any(|line| line == "in step" || line == "smugglr sync: done")
    {
        Ok(())
    } else {
        Err(format!("the run did not end in step: {text}"))
    }
}

#[then("the run stops naming the version")]
fn stops_naming_version(world: &World) -> Result<(), String> {
    says(world, VERSION)
}

#[then("the output names migratr up as the only fix")]
fn names_migratr_up(world: &World) -> Result<(), String> {
    says(world, "migratr up")?;
    says(world, "Only one thing fixes it")
}

/// Neither place changed since a `--migratr` run began; shape.rs's step of
/// this name comes here for such a run.
pub(crate) fn neither_changed(world: &World) -> Result<(), String> {
    let (here, there) = before(world)?;
    unchanged("the local database", &here, &state(&local(world)?)?)?;
    unchanged("the place", &there, &state(&place_file(world)?)?)
}

#[then("the place's ledger holds the local ledger's migrations")]
fn ledgers_alike(world: &World) -> Result<(), String> {
    let (here, there) = (ledger(&local(world)?)?, ledger(&place_file(world)?)?);
    if !here.is_empty() && here == there {
        Ok(())
    } else {
        Err(format!("local {here:?}, place {there:?}: {}", said(world)?))
    }
}

#[then("no migration ran on the place")]
fn no_migration_ran(world: &World) -> Result<(), String> {
    // A CREATE TABLE sent to the place would have failed, and would read as
    // migratr writes it.
    let sql = rows_of(
        &place_file(world)?,
        "SELECT sql FROM sqlite_schema WHERE name = 'notes'",
    )?;
    if format!("{sql:?}").contains(&NOTES.replace('"', "\\\"")) {
        Ok(())
    } else {
        Err(format!("notes is now {sql:?}"))
    }
}

#[then("the output names the rows it recorded")]
fn names_recorded(world: &World) -> Result<(), String> {
    says(world, "20251201000000_create_notes")
}

#[then("the run stops naming the column")]
fn stops_naming_column(world: &World) -> Result<(), String> {
    says(world, "stray")
}

#[then("the place has run both migrations")]
fn place_ran_both(world: &World) -> Result<(), String> {
    let held = ledger(&place_file(world)?)?;
    if held.len() == 2 && columns(&place_file(world)?, "notes")?.contains(&"tag".to_owned()) {
        Ok(())
    } else {
        Err(format!(
            "the place's ledger holds {held:?}: {}",
            said(world)?
        ))
    }
}

#[then("the place holds the rows")]
fn place_holds_rows(world: &World) -> Result<(), String> {
    let held = count(&place_file(world)?, "notes")?;
    if held == 3 {
        Ok(())
    } else {
        Err(format!("the place holds {held} rows: {}", said(world)?))
    }
}

#[then("the run stops naming the row")]
fn stops_naming_row(world: &World) -> Result<(), String> {
    let row = world.watched.get().ok_or("no row watched")?;
    says(world, &row)
}

#[then("the local database has not run the migration")]
fn local_not_run(world: &World) -> Result<(), String> {
    let held = ledger(&local(world)?)?;
    if held.is_empty() && !columns(&local(world)?, "notes")?.contains(&"tag".to_owned()) {
        Ok(())
    } else {
        Err(format!("the local ledger holds {held:?}"))
    }
}

#[then("the run stops saying the snapshot does not match")]
fn says_no_match(world: &World) -> Result<(), String> {
    says(world, "does not match")
}

#[then("the place's parent has its new shape")]
fn parent_new_shape(world: &World) -> Result<(), String> {
    let columns = columns(&place_file(world)?, "parent")?;
    if columns == ["id", "name"] {
        Ok(())
    } else {
        Err(format!("parent has {columns:?}: {}", said(world)?))
    }
}

/// Every row of `parent` and of `child` in the file at `path`.
fn family(path: &str) -> Result<[Vec<Vec<String>>; 2], String> {
    Ok([
        rows_of(path, "SELECT * FROM parent ORDER BY id")?,
        rows_of(path, "SELECT * FROM child ORDER BY id")?,
    ])
}

#[then("the place holds every parent and child row of the local database")]
fn place_holds_family(world: &World) -> Result<(), String> {
    let (here, there) = (family(&local(world)?)?, family(&place_file(world)?)?);
    if here.iter().all(|rows| !rows.is_empty()) && here == there {
        Ok(())
    } else {
        Err(format!("local {here:?}, place {there:?}: {}", said(world)?))
    }
}

#[then("the local database has lost no row")]
fn local_lost_nothing(world: &World) -> Result<(), String> {
    let path = local(world)?;
    for (table, rows) in [("parent", 2), ("child", 3), ("notes", 3)] {
        let held = rows_of(
            &path,
            &format!("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '{table}'"),
        )?;
        if !held.is_empty() && count(&path, table)? != rows {
            return Err(format!("{table} lost rows: {}", said(world)?));
        }
    }
    Ok(())
}

#[then("the local database holds every row it held")]
fn local_holds_all(world: &World) -> Result<(), String> {
    let held = count(&local(world)?, "kept")?;
    if held == 2 {
        Ok(())
    } else {
        Err(format!("the local table holds {held}: {}", said(world)?))
    }
}

#[then("the place holds every one of those rows")]
fn place_holds_all(world: &World) -> Result<(), String> {
    let (here, there) = (
        rows_of(&local(world)?, "SELECT * FROM kept ORDER BY id")?,
        rows_of(&place_file(world)?, "SELECT * FROM kept ORDER BY id")?,
    );
    if here == there && here.len() == 2 {
        Ok(())
    } else {
        Err(format!("local {here:?}, place {there:?}: {}", said(world)?))
    }
}

#[then("the place still holds that row as changed")]
fn place_kept_change(world: &World) -> Result<(), String> {
    let rows = rows_of(
        &place_file(world)?,
        "SELECT 1 FROM kept WHERE id = 'p1' AND name = 'moved'",
    )?;
    if rows.len() == 1 {
        Ok(())
    } else {
        Err(format!("the row is gone: {}", said(world)?))
    }
}

#[then("no table on the place was dropped")]
fn nothing_dropped(world: &World) -> Result<(), String> {
    place_unchanged(world)
}

#[then("no snapshot is taken")]
fn no_snapshot(world: &World) -> Result<(), String> {
    // The file alone proves nothing: a run that reaches in step deletes it.
    let first = world.first_said.get().ok_or("no first run")?;
    if first.contains("snapshot:") || snapshot(world)?.exists() {
        Err(format!("a snapshot was taken: {first}"))
    } else {
        Ok(())
    }
}

#[then("the record's run status is not done")]
fn status_not_done(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let record: Value = serde_json::from_slice(&output.stdout).map_err(|err| err.to_string())?;
    if record["status"] == "done" {
        Err("the run reported done".to_owned())
    } else {
        Ok(())
    }
}

#[then("the output names the rows that differ")]
fn names_rows_that_differ(world: &World) -> Result<(), String> {
    says(world, "n1")
}

#[then("the output names the snapshot")]
fn names_snapshot(world: &World) -> Result<(), String> {
    says(world, "source.smugglr.db")
}

#[then("the snapshot still exists")]
fn snapshot_exists(world: &World) -> Result<(), String> {
    if snapshot(world)?.exists() {
        Ok(())
    } else {
        Err("the snapshot is gone".to_owned())
    }
}

#[then("the output names the table and its column as not compliant")]
fn names_not_compliant(world: &World) -> Result<(), String> {
    says(world, "counters: the database hands out its key id")?;
    says(world, "not compliant")
}

#[then("the text-keyed table's rows are in step")]
fn text_table_in_step(world: &World) -> Result<(), String> {
    let query = "SELECT * FROM notes ORDER BY id";
    let (here, there) = (
        rows_of(&local(world)?, query)?,
        rows_of(&place_file(world)?, query)?,
    );
    if here == there {
        Ok(())
    } else {
        Err(format!("local {here:?}, place {there:?}"))
    }
}

#[then("the run says in step for the rest")]
fn in_step_for_the_rest(world: &World) -> Result<(), String> {
    says_in_step(world)
}

#[then("the run stops naming the child table")]
fn stops_naming_child(world: &World) -> Result<(), String> {
    says(world, "would drop child")
}

#[then("the place's rows are as they were")]
fn place_rows_same(world: &World) -> Result<(), String> {
    place_unchanged(world)
}

#[then("the output holds the migration's statements")]
fn holds_statements(world: &World) -> Result<(), String> {
    says(world, "ADD COLUMN")
}

#[then("the run stops saying no verified snapshot exists")]
fn says_no_snapshot(world: &World) -> Result<(), String> {
    says(world, "no verified snapshot")
}

#[then("the output names migratr down and safe")]
fn names_ways_on(world: &World) -> Result<(), String> {
    says(world, "migratr down")?;
    says(world, "safe to false")
}

#[when("the agent runs smugglr sync on them without --migratr")]
fn plain_sync(world: &World) -> Result<(), String> {
    world.sync(&[]).map(drop)
}

#[then("the output names the table as handing out its own keys")]
fn names_self_keyed(world: &World) -> Result<(), String> {
    let text = said(world)?;
    if text
        .lines()
        .any(|line| line.contains("hands out its own keys:") && line.contains("counters"))
    {
        Ok(())
    } else {
        Err(format!("counters is not named: {text}"))
    }
}

#[then("the exit status is 0")]
fn exit_zero(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    if output.status.code() == Some(0) {
        Ok(())
    } else {
        Err(format!("exit {:?}: {}", output.status.code(), said(world)?))
    }
}

#[then("the place holds the edited row")]
fn place_edited(world: &World) -> Result<(), String> {
    let rows = rows_of(
        &place_file(world)?,
        "SELECT body FROM notes WHERE id = 'n1'",
    )?;
    if rows.len() == 1 && rows[0].iter().any(|value| value.contains("edited here")) {
        Ok(())
    } else {
        Err(format!("the place holds {rows:?}: {}", said(world)?))
    }
}
