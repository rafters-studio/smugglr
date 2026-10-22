//! Steps for one sync between two places as they are: real SQLite files,
//! smugglr's memory in its config directory, an adapter written here.

use std::collections::BTreeMap;

use smugglr::{Options, sync};
use smugglr_core::change::{Change, Op};
use smugglr_core::plan::SyncMode;
use smugglr_core::record::{Record, Table as Entry};
use smugglr_core::row_hash::row_hash;
use smugglr_place::{Error, Key, LocalFile, Place, Table};

use rstest_bdd_macros::{given, then, when};

use crate::setup::{NOTES, ORDERS, THREE_NOTES, World, note, rows_of, sql_on};

/// A service smugglr does not know: tables of rows held in this program,
/// reached through an adapter written outside smugglr. It cannot run the
/// engine's hash where the rows live, so the adapter hashes their `quote()`
/// text itself.
#[derive(Debug, Default)]
struct Shelf {
    tables: BTreeMap<String, (Table, BTreeMap<Key, Vec<String>>)>,
}

impl Shelf {
    fn rows(&self, table: &str) -> Vec<Vec<String>> {
        self.tables
            .get(table)
            .map(|(_, rows)| rows.values().cloned().collect())
            .unwrap_or_default()
    }
}

impl Place for Shelf {
    fn address(&self) -> &'static str {
        "shelf://notes"
    }

    fn tables(&mut self) -> Result<Vec<Table>, Error> {
        Ok(self
            .tables
            .values()
            .map(|(table, _)| table.clone())
            .collect())
    }

    fn hashes(&mut self, table: &Table) -> Result<Vec<(Key, [u8; 16])>, Error> {
        Ok(self
            .tables
            .get(&table.name)
            .map(|(_, rows)| {
                rows.iter()
                    .map(|(key, row)| {
                        let columns: Vec<&str> = row.iter().map(String::as_str).collect();
                        (key.clone(), row_hash(&columns))
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    fn rows(&mut self, table: &Table, keys: &[Key]) -> Result<Vec<Vec<String>>, Error> {
        let Some((_, rows)) = self.tables.get(&table.name) else {
            return Ok(Vec::new());
        };
        Ok(keys
            .iter()
            .filter_map(|key| rows.get(key).cloned())
            .collect())
    }

    fn write(&mut self, table: &Table, changes: &[Change]) -> Result<Vec<Key>, Error> {
        let (_, rows) = self
            .tables
            .get_mut(&table.name)
            .ok_or_else(|| Error::Adapter(format!("no table {}", table.name)))?;
        let mut written = Vec::new();
        for change in changes {
            // Only where the row is still at the change's from.
            let here = rows.get(&change.key).map(|row| {
                let columns: Vec<&str> = row.iter().map(String::as_str).collect();
                row_hash(&columns)
            });
            if here != change.from {
                continue;
            }
            match change.op {
                Op::Down => {
                    rows.remove(&change.key);
                }
                Op::Up | Op::Upsert => {
                    rows.insert(change.key.clone(), change.values.clone());
                }
            }
            written.push(change.key.clone());
        }
        Ok(written)
    }
}

/// Each notes row of the file at `path` as its columns' `quote()` text.
fn quoted_notes(path: &str) -> Result<Vec<Vec<String>>, String> {
    let connection = rusqlite::Connection::open(path).map_err(|err| err.to_string())?;
    let mut statement = connection
        .prepare("SELECT quote(id), quote(body), quote(created_at) FROM notes ORDER BY id")
        .map_err(|err| err.to_string())?;
    statement
        .query_map([], |row| (0..3).map(|i| row.get::<_, String>(i)).collect())
        .and_then(Iterator::collect)
        .map_err(|err| err.to_string())
}

fn options(world: &World) -> Result<Options, String> {
    Ok(Options {
        config_dir: Some(world.config()?),
        ..Options::default()
    })
}

#[given(
    "an adapter written outside smugglr for a service smugglr does not know, and a local file with rows that service lacks"
)]
fn adapter_and_file(world: &World) -> Result<(), String> {
    let file = world.place("field.db", &format!("{NOTES}; {THREE_NOTES}"))?;
    world.source.set(file);
    Ok(())
}

#[when("a program syncs the file with that service through the adapter")]
fn program_syncs_adapter(world: &World) -> Result<(), String> {
    let path = world.source.get().ok_or("no file")?;
    let mut file = LocalFile::open(&path).map_err(|err| err.to_string())?;
    let notes = Table {
        name: "notes".to_owned(),
        columns: vec!["id".to_owned(), "body".to_owned(), "created_at".to_owned()],
        types: vec!["TEXT".to_owned(), "TEXT".to_owned(), "INTEGER".to_owned()],
        key: vec!["id".to_owned()],
        self_keyed: false,
    };
    let mut shelf = Shelf::default();
    shelf
        .tables
        .insert("notes".to_owned(), (notes, BTreeMap::new()));
    sync(&mut file, &mut shelf, &options(world)?).map_err(|err| err.to_string())?;
    let held = shelf.rows("notes");
    world.watched.set(format!("{held:?}"));
    Ok(())
}

#[then("the service holds the same rows as the file")]
fn service_holds_file(world: &World) -> Result<(), String> {
    let path = world.source.get().ok_or("no file")?;
    let held = match (world.watched.get(), world.service.get()) {
        (Some(held), _) => held,
        (None, Some(service)) => format!("{:?}", quoted_notes(&service)?),
        (None, None) => return Err("the service was not synced".to_owned()),
    };
    let want = format!("{:?}", quoted_notes(&path)?);
    if held == want {
        Ok(())
    } else {
        Err(format!("the service holds {held}, the file {want}"))
    }
}

/// The source file, which commits a row of its own once the run has read
/// its hashes.
#[derive(Debug)]
struct Busy {
    file: LocalFile,
    path: String,
    committed: bool,
}

impl Place for Busy {
    fn address(&self) -> &str {
        self.file.address()
    }

    fn tables(&mut self) -> Result<Vec<Table>, Error> {
        self.file.tables()
    }

    fn hashes(&mut self, table: &Table) -> Result<Vec<(Key, [u8; 16])>, Error> {
        let hashes = self.file.hashes(table)?;
        if !self.committed {
            sql_on(
                &self.path,
                "INSERT INTO notes VALUES ('mid-run', 'committed while read')",
            )
            .map_err(Error::Adapter)?;
            self.committed = true;
        }
        Ok(hashes)
    }

    fn rows(&mut self, table: &Table, keys: &[Key]) -> Result<Vec<Vec<String>>, Error> {
        self.file.rows(table, keys)
    }

    fn write(&mut self, table: &Table, changes: &[Change]) -> Result<Vec<Key>, Error> {
        self.file.write(table, changes)
    }
}

#[given(
    "two places with no steering column, and a row committed into the source while a run is reading"
)]
fn committed_mid_run(world: &World) -> Result<(), String> {
    // No column whose values move forward on every change.
    let table = "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)";
    let source = world.place(
        "source.db",
        &format!("{table}; INSERT INTO notes VALUES ('n1', 'first'), ('n2', 'second')"),
    )?;
    let target = world.place("target.db", table)?;
    world.source.set(source.clone());
    world.target.set(target.clone());
    world.watched.set("mid-run".to_owned());
    let mut busy = Busy {
        file: LocalFile::open(&source).map_err(|err| err.to_string())?,
        path: source,
        committed: false,
    };
    let mut target = LocalFile::open(&target).map_err(|err| err.to_string())?;
    sync(&mut busy, &mut target, &options(world)?).map_err(|err| err.to_string())?;
    if busy.committed {
        Ok(())
    } else {
        Err("the row was not committed during the run".to_owned())
    }
}

#[when("that run ends and the agent runs smugglr sync on them again")]
fn sync_again(world: &World) -> Result<(), String> {
    world.sync_ok()
}

#[then("the target holds that row")]
fn target_holds(world: &World) -> Result<(), String> {
    let (source, target) = places(world)?;
    let id = world.watched.get().ok_or("no row watched")?;
    match (note(&source, &id)?, note(&target, &id)?) {
        (Some(want), Some(got)) if want == got => Ok(()),
        (want, got) => Err(format!(
            "the source holds {want:?}, the target {got:?}{}",
            stderr(world)
        )),
    }
}

#[given(
    "two places synced before, the target's clock 60 seconds behind, and a row since added to the target"
)]
fn clock_behind(world: &World) -> Result<(), String> {
    let source = world.place("source.db", &format!("{NOTES}; {THREE_NOTES}"))?;
    let target = world.place("target.db", NOTES)?;
    world.source.set(source);
    world.target.set(target.clone());
    world.sync_ok()?;
    // The target stamps its rows with its own clock, 60 seconds behind the
    // source's: its new row looks older than the last sync.
    sql_on(
        &target,
        "INSERT INTO notes VALUES ('behind', 'stamped 60 s behind', \
         CAST(strftime('%s', 'now') AS INTEGER) - 60)",
    )?;
    world.watched.set("behind".to_owned());
    Ok(())
}

#[then("the source holds that row")]
fn source_holds(world: &World) -> Result<(), String> {
    let (source, target) = places(world)?;
    let id = world.watched.get().ok_or("no row watched")?;
    match (note(&source, &id)?, note(&target, &id)?) {
        (Some(got), Some(want)) if want == got => Ok(()),
        (got, want) => Err(format!(
            "the source holds {got:?}, the target {want:?}{}",
            stderr(world)
        )),
    }
}

#[given("two places with rows to move")]
fn rows_to_move(world: &World) -> Result<(), String> {
    let source = world.place(
        "source.db",
        &format!("{NOTES}; {THREE_NOTES}; PRAGMA user_version = 7; PRAGMA application_id = 42"),
    )?;
    let target = world.place(
        "target.db",
        &format!(
            "{NOTES}; INSERT INTO notes VALUES ('t1', 'only here', 4); \
             PRAGMA user_version = 3; PRAGMA application_id = 9"
        ),
    )?;
    world
        .watched
        .set(format!("{:?}", [marks(&source)?, marks(&target)?]));
    world.source.set(source);
    world.target.set(target);
    Ok(())
}

/// A place's `sqlite_master`, `user_version` and `application_id`.
fn marks(path: &str) -> Result<Vec<Vec<String>>, String> {
    let mut marks = rows_of(path, "SELECT * FROM sqlite_master ORDER BY type, name")?;
    marks.extend(rows_of(path, "PRAGMA user_version")?);
    marks.extend(rows_of(path, "PRAGMA application_id")?);
    Ok(marks)
}

#[then(
    "each place's sqlite_master, user_version and application_id are as they were before the run"
)]
fn no_marks(world: &World) -> Result<(), String> {
    let (source, target) = places(world)?;
    let output = world.output.get().ok_or("smugglr was not run")?;
    if !output.status.success() {
        return Err(format!(
            "smugglr sync exited {}{}",
            output.status,
            stderr(world)
        ));
    }
    if note(&target, "n1")?.is_none() {
        return Err("no row moved".to_owned());
    }
    let before = world.watched.get().ok_or("no marks taken")?;
    let after = format!("{:?}", [marks(&source)?, marks(&target)?]);
    if before == after {
        Ok(())
    } else {
        Err(format!("before the run {before}, after it {after}"))
    }
}

#[given("a local file synced with two different places, one sync each")]
fn synced_with_two(world: &World) -> Result<(), String> {
    let file = world.place("field.db", &format!("{NOTES}; {THREE_NOTES}"))?;
    for other in ["office.db", "laptop.db"] {
        let other = world.place(other, NOTES)?;
        world.source.set(file.clone());
        world.target.set(other);
        world.sync_ok()?;
    }
    Ok(())
}

#[when("the agent looks in smugglr's config directory")]
fn look_in_config(world: &World) -> Result<(), String> {
    let config = world.config()?;
    let folders: Vec<String> = std::fs::read_dir(&config)
        .map_err(|err| format!("{}: {err}", config.display()))?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    world.watched.set(folders.join(" "));
    Ok(())
}

#[then("it holds one folder for each of the two pairs")]
fn one_folder_per_pair(world: &World) -> Result<(), String> {
    let folders = world.watched.get().ok_or("nobody looked")?;
    match folders.split(' ').filter(|f| !f.is_empty()).count() {
        2 => Ok(()),
        n => Err(format!("the config directory holds {n} folders: {folders}")),
    }
}

fn places(world: &World) -> Result<(String, String), String> {
    Ok((
        world.source.get().ok_or("no source")?,
        world.target.get().ok_or("no target")?,
    ))
}

fn stderr(world: &World) -> String {
    world.output.get().map_or_else(String::new, |output| {
        format!(
            " (smugglr said: {})",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[given("a target that keeps a written row differently from how it was written")]
fn altering_target(world: &World) -> Result<(), String> {
    let source = world.place("source.db", &format!("{NOTES}; {THREE_NOTES}"))?;
    let target = world.place(
        "target.db",
        &format!(
            "{NOTES}; CREATE TRIGGER shout AFTER INSERT ON notes BEGIN \
             UPDATE notes SET body = upper(NEW.body) WHERE id = NEW.id; END"
        ),
    )?;
    world.source.set(source);
    world.target.set(target);
    Ok(())
}

#[when("the agent runs smugglr sync on the source and that target")]
fn sync_on_that_target(world: &World) -> Result<(), String> {
    world.sync(&[]).map(|_| ())
}

#[then("smugglr exits 4")]
fn exits_4(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    match output.status.code() {
        Some(4) => Ok(()),
        code => Err(format!("smugglr exited {code:?}{}", stderr(world))),
    }
}

#[given("two places where the source has rows the target lacks")]
fn source_has_rows(world: &World) -> Result<(), String> {
    world
        .source
        .set(world.place("source.db", &format!("{NOTES}; {THREE_NOTES}"))?);
    world.target.set(world.place("target.db", NOTES)?);
    Ok(())
}

#[when("the agent runs smugglr sync on them and the run is done")]
fn sync_done(world: &World) -> Result<(), String> {
    let output = world.sync(&["--json"])?;
    let said = String::from_utf8_lossy(&output.stdout);
    if output.status.success() && said.contains("\"done\"") {
        Ok(())
    } else {
        Err(format!("the run is not done: {said}{}", stderr(world)))
    }
}

/// Every file under `dir`, with its contents.
fn files_under(dir: &std::path::Path) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut files = Vec::new();
    let entries = std::fs::read_dir(dir).map_err(|err| format!("{}: {err}", dir.display()))?;
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            files.extend(files_under(&path)?);
        } else {
            let bytes = std::fs::read(&path).map_err(|err| format!("{}: {err}", path.display()))?;
            files.push((path.display().to_string(), bytes));
        }
    }
    Ok(files)
}

#[then("smugglr's own files hold no copy of the run's record")]
fn record_not_kept(world: &World) -> Result<(), String> {
    let files = files_under(&world.config()?)?;
    if files.is_empty() {
        return Err("smugglr kept no files at all".to_owned());
    }
    // Words only the record holds.
    let marks = ["to_target", "moved", "status", "deleted_in_target"];
    for (path, bytes) in files {
        let text = String::from_utf8_lossy(&bytes);
        if let Some(mark) = marks.iter().find(|mark| text.contains(**mark)) {
            return Err(format!("{path} holds {mark:?} from the record"));
        }
    }
    Ok(())
}

/// The target of a run cut off while writing `table`: after writing it when
/// `after`, else before.
#[derive(Debug)]
struct Cut {
    file: LocalFile,
    table: &'static str,
    after: bool,
}

impl Place for Cut {
    fn address(&self) -> &str {
        self.file.address()
    }

    fn tables(&mut self) -> Result<Vec<Table>, Error> {
        self.file.tables()
    }

    fn hashes(&mut self, table: &Table) -> Result<Vec<(Key, [u8; 16])>, Error> {
        self.file.hashes(table)
    }

    fn rows(&mut self, table: &Table, keys: &[Key]) -> Result<Vec<Vec<String>>, Error> {
        self.file.rows(table, keys)
    }

    fn write(&mut self, table: &Table, changes: &[Change]) -> Result<Vec<Key>, Error> {
        if table.name != self.table {
            return self.file.write(table, changes);
        }
        if self.after {
            self.file.write(table, changes)?;
        }
        Err(Error::Adapter(CUT_OFF.to_owned()))
    }
}

/// What a `Cut` place says when the run is cut off.
const CUT_OFF: &str = "the run was cut off";

/// Runs a sync through the Rust crates whose target is cut off while
/// writing `table`, and fails unless the run ends there.
fn cut_run(world: &World, table: &'static str, after: bool) -> Result<(), String> {
    let (source, target) = places(world)?;
    let mut source = LocalFile::open(&source).map_err(|err| err.to_string())?;
    let mut target = Cut {
        file: LocalFile::open(&target).map_err(|err| err.to_string())?,
        table,
        after,
    };
    // Cut once a journal line is written, the Cut place's error comes back
    // inside the record-carrying error.
    let cut = |err: &smugglr::Error| matches!(err, smugglr::Error::Place(Error::Adapter(message)) if message == CUT_OFF);
    match sync(&mut source, &mut target, &options(world)?) {
        Err(smugglr::Error::CutOff { cause, .. }) if cut(&cause) => Ok(()),
        Err(err) if cut(&err) => Ok(()),
        Err(err) => Err(format!("the run ended before the cut: {err}")),
        Ok(record) => Err(format!("the run was not cut off: {}", record.json())),
    }
}

#[given(
    "a run that copied a new row from the source to the target and was cut off before it ended"
)]
fn copied_then_cut(world: &World) -> Result<(), String> {
    world
        .source
        .set(world.place("source.db", &format!("{NOTES}; {THREE_NOTES}"))?);
    world.target.set(world.place("target.db", NOTES)?);
    world.sync_ok()?;
    let (source, target) = places(world)?;
    sql_on(&source, "INSERT INTO notes VALUES ('n9', 'new', 9)")?;
    cut_run(world, "notes", true)?;
    world.watched.set("n9".to_owned());
    match note(&target, "n9")? {
        Some(_) => Ok(()),
        None => Err("the cut-off run did not copy the row".to_owned()),
    }
}

#[when("the row is deleted in the source and the agent runs smugglr sync on them again")]
fn delete_and_sync(world: &World) -> Result<(), String> {
    let (source, _) = places(world)?;
    let id = world.watched.get().ok_or("no row watched")?;
    sql_on(&source, &format!("DELETE FROM notes WHERE id = '{id}'"))?;
    world.sync_ok()
}

#[then("the row is gone from both places")]
fn gone_from_both(world: &World) -> Result<(), String> {
    let (source, target) = places(world)?;
    let id = world.watched.get().ok_or("no row watched")?;
    match (note(&source, &id)?, note(&target, &id)?) {
        (None, None) => Ok(()),
        (here, there) => Err(format!("the source holds {here:?}, the target {there:?}")),
    }
}

#[given("a run between two places cut off part-way through writing")]
fn cut_part_way(world: &World) -> Result<(), String> {
    world.source.set(world.place(
        "source.db",
        &format!(
            "{NOTES}; {THREE_NOTES}; {ORDERS}; \
             INSERT INTO orders VALUES ('o1', 'tent'), ('o2', 'stove')"
        ),
    )?);
    world.target.set(world.place(
        "target.db",
        &format!("{NOTES}; {ORDERS}; INSERT INTO orders VALUES ('o3', 'lamp')"),
    )?);
    // notes is written, then the run is cut before orders.
    cut_run(world, "orders", false)?;
    let (_, target) = places(world)?;
    match rows_of(&target, "SELECT id FROM notes")?.len() {
        3 => Ok(()),
        n => Err(format!("the cut-off run wrote {n} of the 3 notes")),
    }
}

#[when("the agent runs smugglr sync on them again")]
fn sync_again_once(world: &World) -> Result<(), String> {
    world.sync(&[]).map(|_| ())
}

#[then("both places hold the same rows")]
fn same_rows(world: &World) -> Result<(), String> {
    let (source, target) = places(world)?;
    for table in ["notes", "orders"] {
        let query = format!("SELECT * FROM {table} ORDER BY id");
        let (here, there) = (rows_of(&source, &query)?, rows_of(&target, &query)?);
        if here != there || here.is_empty() {
            return Err(format!(
                "{table}: the source holds {here:?}, the target {there:?}{}",
                stderr(world)
            ));
        }
    }
    Ok(())
}

#[given(
    "a run cut off before writing a row of notes to the target, and that row since edited in the target"
)]
fn cut_before_row(world: &World) -> Result<(), String> {
    world
        .source
        .set(world.place("source.db", &format!("{NOTES}; {THREE_NOTES}"))?);
    world.target.set(world.place("target.db", NOTES)?);
    world.sync_ok()?;
    let (source, target) = places(world)?;
    sql_on(
        &source,
        "UPDATE notes SET body = 'edited in the source' WHERE id = 'n1'",
    )?;
    cut_run(world, "notes", false)?;
    sql_on(
        &target,
        "UPDATE notes SET body = 'edited after the cut' WHERE id = 'n1'",
    )?;
    world.watched.set("n1".to_owned());
    Ok(())
}

#[when("the agent runs smugglr sync on them again with --json")]
fn sync_again_json(world: &World) -> Result<(), String> {
    world.sync(&["--json"]).map(|_| ())
}

#[then("the record names that row's key as stopped")]
fn named_as_stopped(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let record: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|err| format!("not a record: {err}{}", stderr(world)))?;
    let id = world.watched.get().ok_or("no row watched")?;
    let notes = record.pointer("/tables/notes").cloned().unwrap_or_default();
    let named = notes
        .get("rows")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|rows| rows.contains(&serde_json::Value::String(id)));
    if notes.get("status").and_then(serde_json::Value::as_str) == Some("stopped") && named {
        Ok(())
    } else {
        Err(format!("the record gives notes {notes}"))
    }
}

#[then("smugglr's files hold no smugglr.sync journal")]
fn no_journal(world: &World) -> Result<(), String> {
    let files = files_under(&world.config()?)?;
    if files.is_empty() {
        return Err("smugglr kept no files at all".to_owned());
    }
    match files
        .iter()
        .find(|(path, _)| path.ends_with("smugglr.sync"))
    {
        None => Ok(()),
        Some((path, _)) => Err(format!("{path} remains")),
    }
}

/// A run limited by --table still finishes every table a cut-off run left
/// in the journal, so a later delete is not undone.
#[test]
fn a_table_limited_run_finishes_every_cut_off_table() -> Result<(), String> {
    let world = World::default();
    world.source.set(world.place(
        "source.db",
        &format!("{NOTES}; {THREE_NOTES}; {ORDERS}; INSERT INTO orders VALUES ('o1', 'tent')"),
    )?);
    world
        .target
        .set(world.place("target.db", &format!("{NOTES}; {ORDERS}"))?);
    world.sync_ok()?;
    let (source, target) = places(&world)?;
    sql_on(&source, "INSERT INTO orders VALUES ('o9', 'new')")?;
    cut_run(&world, "orders", true)?;
    let output = world.sync(&["--table", "notes"])?;
    if !output.status.success() {
        return Err(format!("the --table run exited {}", output.status));
    }
    sql_on(&source, "DELETE FROM orders WHERE id = 'o9'")?;
    world.sync_ok()?;
    let query = "SELECT * FROM orders WHERE id = 'o9'";
    match (rows_of(&source, query)?, rows_of(&target, query)?) {
        (here, there) if here.is_empty() && there.is_empty() => Ok(()),
        (here, there) => Err(format!("the source holds {here:?}, the target {there:?}")),
    }
}

/// Two real files synced once through `smugglr::sync`, then note n1 edited
/// in both and, in the source, a new order; returns the record of the next
/// run under `mode`.
fn both_sides_edit(world: &World, mode: SyncMode) -> Result<Record, String> {
    edited_in_both(world)?;
    let (source, target) = places(world)?;
    let options = Options {
        sync_mode: mode,
        ..options(world)?
    };
    sync(&mut open(&source)?, &mut open(&target)?, &options).map_err(|err| err.to_string())
}

fn open(path: &str) -> Result<LocalFile, String> {
    LocalFile::open(path).map_err(|err| err.to_string())
}

/// Two real files synced once through `smugglr::sync`, then note n1 edited
/// in both and, in the source, a new order.
fn edited_in_both(world: &World) -> Result<(), String> {
    edited_in_both_with(world, "INSERT INTO orders VALUES ('o2', 'stove')")
}

/// Two real files synced once through `smugglr::sync`, then note n1 edited
/// in both, and `source_sql` run on the source.
fn edited_in_both_with(world: &World, source_sql: &str) -> Result<(), String> {
    let source = world.place(
        "source.db",
        &format!("{NOTES}; {THREE_NOTES}; {ORDERS}; INSERT INTO orders VALUES ('o1', 'tent')"),
    )?;
    let target = world.place("target.db", &format!("{NOTES}; {ORDERS}"))?;
    world.source.set(source.clone());
    world.target.set(target.clone());
    sync(&mut open(&source)?, &mut open(&target)?, &options(world)?)
        .map_err(|err| err.to_string())?;
    sql_on(
        &source,
        &format!("UPDATE notes SET body = 'source copy' WHERE id = 'n1'; {source_sql}"),
    )?;
    sql_on(
        &target,
        "UPDATE notes SET body = 'target copy' WHERE id = 'n1'",
    )
}

/// #509 through the real sync: a stop stops only its table.
#[test]
fn sync_stops_only_the_table_with_a_both_sides_change() -> Result<(), String> {
    let world = World::default();
    let record = both_sides_edit(&world, SyncMode::Stop)?;
    let (source, target) = places(&world)?;
    let notes_stopped = matches!(record.tables.get("notes"), Some(Entry::Stopped { .. }));
    let orders_moved = matches!(record.tables.get("orders"), Some(Entry::Moved(_)));
    let o2 = rows_of(&target, "SELECT * FROM orders WHERE id = 'o2'")?;
    if notes_stopped
        && orders_moved
        && !o2.is_empty()
        && note(&source, "n1")? != note(&target, "n1")?
    {
        Ok(())
    } else {
        Err(format!("the record is {}", record.json()))
    }
}

/// #509 through the real sync: --sync-mode source writes the source's copy
/// into the target and names it.
#[test]
fn sync_mode_source_keeps_the_sources_copy() -> Result<(), String> {
    let world = World::default();
    let record = both_sides_edit(&world, SyncMode::Source)?;
    let (source, target) = places(&world)?;
    let named = matches!(record.tables.get("notes"),
        Some(Entry::Moved(moves)) if moves.overwritten_in_target.len() == 1);
    let held = note(&target, "n1")?;
    if named && held == note(&source, "n1")? && format!("{held:?}").contains("source copy") {
        Ok(())
    } else {
        Err(format!(
            "the target holds {held:?}; the record is {}",
            record.json()
        ))
    }
}

/// #509 through the real sync: --sync-mode target writes the target's copy
/// into the source and names it.
#[test]
fn sync_mode_target_keeps_the_targets_copy() -> Result<(), String> {
    let world = World::default();
    let record = both_sides_edit(&world, SyncMode::Target)?;
    let (source, target) = places(&world)?;
    let named = matches!(record.tables.get("notes"),
        Some(Entry::Moved(moves)) if moves.overwritten_in_source.len() == 1);
    let held = note(&source, "n1")?;
    if named && held == note(&target, "n1")? && format!("{held:?}").contains("target copy") {
        Ok(())
    } else {
        Err(format!(
            "the source holds {held:?}; the record is {}",
            record.json()
        ))
    }
}

/// Each place's rows and marks, to tell whether a run wrote anything.
fn snapshot(world: &World) -> Result<String, String> {
    let (source, target) = places(world)?;
    let rows = |path: &str| rows_of(path, "SELECT * FROM notes ORDER BY id");
    Ok(format!(
        "{:?}",
        [
            rows(&source)?,
            marks(&source)?,
            rows(&target)?,
            marks(&target)?
        ]
    ))
}

/// #512's dry run through the real sync: `smugglr sync --dryrun` writes
/// nothing to either place and keeps no memory.
#[test]
fn a_real_dry_run_writes_nothing_and_keeps_no_memory() -> Result<(), String> {
    let world = World::default();
    world
        .source
        .set(world.place("source.db", &format!("{NOTES}; {THREE_NOTES}"))?);
    world.target.set(world.place(
        "target.db",
        &format!("{NOTES}; INSERT INTO notes VALUES ('t1', 'only here', 4)"),
    )?);
    let before = snapshot(&world)?;
    let output = world.sync(&["--dryrun", "--json"])?;
    let said = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() || !said.contains("\"preview\"") || !said.contains("would move") {
        return Err(format!("the dry run printed {said}{}", stderr(&world)));
    }
    let after = snapshot(&world)?;
    if before != after {
        return Err(format!("a place changed: before {before}, after {after}"));
    }
    let config = world.config()?;
    let kept = if config.exists() {
        files_under(&config)?
    } else {
        Vec::new()
    };
    if kept.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "the dry run kept {:?}",
            kept.iter().map(|(p, _)| p).collect::<Vec<_>>()
        ))
    }
}

/// #507 through the real sync: in two SQLite files, a sessions table keyed
/// INTEGER PRIMARY KEY stops with an empty rows list and its why, and is not
/// written, while notes, keyed by text, moves.
#[test]
fn a_real_self_keyed_table_stops_and_the_others_move() -> Result<(), String> {
    const SESSIONS: &str = "CREATE TABLE sessions (id INTEGER PRIMARY KEY, user TEXT)";
    let world = World::default();
    world.source.set(world.place(
        "source.db",
        &format!("{NOTES}; {THREE_NOTES}; {SESSIONS}; INSERT INTO sessions VALUES (1, 'ada')"),
    )?);
    world.target.set(world.place(
        "target.db",
        &format!("{NOTES}; {SESSIONS}; INSERT INTO sessions VALUES (1, 'grace')"),
    )?);
    let output = world.sync(&["--json"])?;
    let said = String::from_utf8_lossy(&output.stdout);
    let record: serde_json::Value = serde_json::from_str(&said)
        .map_err(|err| format!("the record is not JSON ({err}): {said}{}", stderr(&world)))?;
    let sessions = &record["tables"]["sessions"];
    let why = sessions["why"].as_str().unwrap_or_default();
    if sessions["status"] != "stopped"
        || sessions["rows"] != serde_json::json!([])
        || !why.contains("two places would hand out the same key")
        || !why.contains("UUIDv7")
    {
        return Err(format!("sessions: {sessions}"));
    }
    if record["tables"]["notes"]["status"] != "moved" {
        return Err(format!("notes did not move: {record}"));
    }
    let (_, target) = places(&world)?;
    let held = rows_of(&target, "SELECT user FROM sessions")?;
    if held == vec![vec!["Text(\"grace\")".to_owned()]] {
        Ok(())
    } else {
        Err(format!("the target's sessions became {held:?}"))
    }
}

/// Two places never synced with each other: three notes only in the source,
/// one only in the target.
fn never_synced(world: &World) -> Result<(), String> {
    world
        .source
        .set(world.place("source.db", &format!("{NOTES}; {THREE_NOTES}"))?);
    world.target.set(world.place(
        "target.db",
        &format!("{NOTES}; INSERT INTO notes VALUES ('t1', 'only here', 4)"),
    )?);
    Ok(())
}

#[given("two places never synced with each other")]
fn two_never_synced(world: &World) -> Result<(), String> {
    never_synced(world)
}

#[given("two places never synced with each other, and a row only in the target")]
fn only_in_target(world: &World) -> Result<(), String> {
    never_synced(world)?;
    world.watched.set("t1".to_owned());
    Ok(())
}

#[when("the agent runs smugglr sync on them with --dryrun")]
fn sync_dryrun(world: &World) -> Result<(), String> {
    world.sync(&["--dryrun"]).map(|_| ())
}

/// The record smugglr printed with `--json`.
fn record(world: &World) -> Result<serde_json::Value, String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&said)
        .map_err(|err| format!("the record is not JSON ({err}): {said}{}", stderr(world)))
}

#[then("the record says first_sync true")]
fn says_first_sync(world: &World) -> Result<(), String> {
    let record = record(world)?;
    match record.get("first_sync") {
        Some(serde_json::Value::Bool(true)) => Ok(()),
        _ => Err(format!("the record says {record}")),
    }
}

#[then("the record says deletes_seen false")]
fn says_no_deletes_seen(world: &World) -> Result<(), String> {
    let record = record(world)?;
    match record.get("deletes_seen") {
        Some(serde_json::Value::Bool(false)) => Ok(()),
        _ => Err(format!("the record says {record}")),
    }
}

/// The `max_wire` setting of the scenarios that are too much: fewer bytes
/// than the rows of [`never_synced`] weigh.
const SMALL_MAX_WIRE: u64 = 20;

#[given("two places never synced with each other, holding more than the max_wire setting allows")]
fn over_max_wire(world: &World) -> Result<(), String> {
    never_synced(world)?;
    let config = world.config()?;
    std::fs::create_dir_all(&config).map_err(|err| format!("{}: {err}", config.display()))?;
    let file = config.join("config.toml");
    std::fs::write(&file, format!("max_wire = {SMALL_MAX_WIRE}\n"))
        .map_err(|err| format!("{}: {err}", file.display()))?;
    let (rows, bytes, _) = expected_weight(world)?;
    if bytes <= SMALL_MAX_WIRE {
        return Err(format!("the places hold only {rows} rows of {bytes} bytes"));
    }
    // The shape steps' "neither place has changed" compares this snapshot.
    world.watched.set(crate::shape::snapshot(world)?);
    Ok(())
}

#[then("the output tells the agent to move it with smugglr stash and then smugglr retrieve")]
fn says_stash_then_retrieve(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = String::from_utf8_lossy(&output.stdout);
    match (said.find("smugglr stash"), said.find("smugglr retrieve")) {
        (Some(stash), Some(retrieve)) if stash < retrieve => Ok(()),
        _ => Err(format!("smugglr printed {said:?}{}", stderr(world))),
    }
}

/// The rows, bytes and tables a first sync of [`never_synced`]'s places
/// moves, found here from the files: every notes row is on one side only,
/// so each is copied, and a row's bytes are its columns' `quote()` text.
fn expected_weight(world: &World) -> Result<(u64, u64, u64), String> {
    let (source, target) = places(world)?;
    let mut rows = 0;
    let mut bytes = 0;
    for path in [source, target] {
        for row in quoted_notes(&path)? {
            rows += 1;
            bytes += row.iter().map(|column| column.len() as u64).sum::<u64>();
        }
    }
    Ok((rows, bytes, 1))
}

#[then("the output gives the rows, bytes and tables the first sync would move")]
fn gives_weight(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = String::from_utf8_lossy(&output.stdout);
    let (rows, bytes, tables) = expected_weight(world)?;
    let want = format!("weight: {rows} rows, {bytes} B in {tables} table");
    let found = said.lines().any(|line| line.trim() == want);
    if output.status.success() && found {
        Ok(())
    } else {
        Err(format!(
            "wanted {want:?}; smugglr printed {said:?}{}",
            stderr(world)
        ))
    }
}

#[given(
    "no max_wire setting, and two places never synced with each other whose rows weigh more than 100 MB"
)]
fn over_default(world: &World) -> Result<(), String> {
    // 110 rows of one million characters of text each: 110 MB stored, and
    // more than 100 MB however a megabyte is counted.
    let source = world.place(
        "source.db",
        &format!(
            "{NOTES}; WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 110) \
             INSERT INTO notes SELECT 'big-' || i, hex(zeroblob(500000)), i FROM n"
        ),
    )?;
    let target = world.place("target.db", NOTES)?;
    let config = world.config()?.join("config.toml");
    if config.exists() {
        return Err(format!("{} sets max_wire", config.display()));
    }
    world.source.set(source);
    world.target.set(target);
    Ok(())
}

#[then("smugglr exits 5")]
fn exits_5(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    match output.status.code() {
        Some(5) => Ok(()),
        code => Err(format!("smugglr exited {code:?}{}", stderr(world))),
    }
}

/// What a `--sync-mode last` run on real files gives: its record, its exit
/// code, and the source's and the target's orders afterwards.
type LastRun = (
    serde_json::Value,
    Option<i32>,
    Vec<Vec<String>>,
    Vec<Vec<String>>,
);

/// Two real files holding one order, steered by `config`, synced once; then
/// the order edited in both, its `version` set to `in_source` and
/// `in_target`. Returns what `smugglr sync --sync-mode last --json` printed
/// and exited with, and each place's order afterwards.
fn last_on_real_files(
    world: &World,
    config: &str,
    in_source: i64,
    in_target: i64,
) -> Result<LastRun, String> {
    let orders = "CREATE TABLE orders (id TEXT PRIMARY KEY, item TEXT, version INTEGER)";
    let source = world.place(
        "source.db",
        &format!("{orders}; INSERT INTO orders VALUES ('o1', 'tent', 1)"),
    )?;
    let target = world.place("target.db", orders)?;
    world.source.set(source.clone());
    world.target.set(target.clone());
    world.sync_ok()?;
    crate::cli::config(world, config)?;
    sql_on(
        &source,
        &format!("UPDATE orders SET item = 'source copy', version = {in_source}"),
    )?;
    sql_on(
        &target,
        &format!("UPDATE orders SET item = 'target copy', version = {in_target}"),
    )?;
    let output = world.sync(&["--sync-mode", "last", "--json"])?;
    let said = String::from_utf8_lossy(&output.stdout);
    let record = serde_json::from_str(&said)
        .map_err(|err| format!("the record is not JSON ({err}): {said}{}", stderr(world)))?;
    let query = "SELECT item, version FROM orders";
    Ok((
        record,
        output.status.code(),
        rows_of(&source, query)?,
        rows_of(&target, query)?,
    ))
}

/// #510 through the real CLI: an integer steering value of 10 is further on
/// than 9, so the target's copy is kept in both places and named.
#[test]
fn real_files_keep_the_copy_further_on_by_number() -> Result<(), String> {
    let world = World::default();
    let (record, code, source, target) =
        last_on_real_files(&world, "steering = \"version\"\n", 9, 10)?;
    let orders = &record["tables"]["orders"];
    if code == Some(0)
        && orders["overwritten_in_source"] == serde_json::json!(["o1"])
        && source == target
        && format!("{source:?}").contains("target copy")
    {
        Ok(())
    } else {
        Err(format!(
            "the source holds {source:?}; the record is {record}"
        ))
    }
}

/// #510 through the real CLI: a per-table steering column is used for its
/// table, and equal values stop the table, naming the row.
#[test]
fn real_files_stop_on_equal_values_of_a_per_table_column() -> Result<(), String> {
    let world = World::default();
    let (record, _, source, target) =
        last_on_real_files(&world, "[tables.orders]\nsteering = \"version\"\n", 7, 7)?;
    let orders = &record["tables"]["orders"];
    if orders["status"] == "stopped"
        && orders["rows"] == serde_json::json!(["o1"])
        && source != target
    {
        Ok(())
    } else {
        Err(format!("the record is {record}"))
    }
}

/// #510 through the real CLI: a refusal for want of a steering column
/// writes nothing and keeps no memory.
#[test]
fn real_files_refused_for_want_of_steering_keep_nothing() -> Result<(), String> {
    let world = World::default();
    world.source.set(world.place(
        "source.db",
        &format!("{NOTES}; {THREE_NOTES}; {ORDERS}; INSERT INTO orders VALUES ('o1', 'tent')"),
    )?);
    world
        .target
        .set(world.place("target.db", &format!("{NOTES}; {ORDERS}"))?);
    crate::cli::config(&world, "[tables.notes]\nsteering = \"created_at\"\n")?;
    let before = crate::shape::snapshot(&world)?;
    let output = world.sync(&["--sync-mode", "last"])?;
    let said = String::from_utf8_lossy(&output.stderr);
    let kept: Vec<String> = files_under(&world.config()?)?
        .into_iter()
        .map(|(path, _)| path)
        .filter(|path| !path.ends_with("config.toml"))
        .collect();
    if output.status.code() == Some(2)
        && said.contains("needs a steering column for: orders.")
        && crate::shape::snapshot(&world)? == before
        && kept.is_empty()
    {
        Ok(())
    } else {
        Err(format!(
            "smugglr exited {} saying {said:?}; kept {kept:?}",
            output.status
        ))
    }
}

#[given(
    "two places with rows to move, whose orders table has no steering column named in the config"
)]
fn rows_to_move_without_steering(world: &World) -> Result<(), String> {
    world.source.set(world.place(
        "source.db",
        &format!("{NOTES}; {THREE_NOTES}; {ORDERS}; INSERT INTO orders VALUES ('o1', 'tent')"),
    )?);
    world
        .target
        .set(world.place("target.db", &format!("{NOTES}; {ORDERS}"))?);
    // notes steers; orders has none named.
    crate::cli::config(world, "[tables.notes]\nsteering = \"created_at\"\n")?;
    // The shape steps' "neither place has changed" compares this snapshot.
    world.watched.set(crate::shape::snapshot(world)?);
    Ok(())
}

#[given(
    "two places where orders has rows to move and notes has a row changed in both places since the last sync"
)]
fn orders_move_notes_stop(world: &World) -> Result<(), String> {
    edited_in_both(world)
}

#[given("two places whose only differences are rows changed in both places since the last sync")]
fn only_changed_in_both(world: &World) -> Result<(), String> {
    edited_in_both_with(world, "")
}

#[given("two places with a row changed in both since the last sync")]
fn changed_in_both_since(world: &World) -> Result<(), String> {
    edited_in_both(world)
}

#[given("a local file and an HTTP-SQL address that does not answer")]
fn file_and_silent_address(world: &World) -> Result<(), String> {
    world
        .source
        .set(world.place("field.db", &format!("{NOTES}; {THREE_NOTES}"))?);
    // A port just let go: nothing listens there, so a connection is refused
    // at once rather than left to time out.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map_err(|err| err.to_string())?
        .port();
    world.target.set(format!("http://127.0.0.1:{port}/sql"));
    Ok(())
}

/// A program's call: each place opened by its address, as the CLI opens
/// it, then `smugglr::sync` with `options`.
fn program_sync(world: &World, options: &Options) -> Result<(), String> {
    let (source, target) = places(world)?;
    let open = |address: &str| smugglr_place::open(address).map_err(|err| err.to_string());
    let (mut source, mut target) = (open(&source)?, open(&target)?);
    world
        .called
        .set(sync(source.as_mut(), target.as_mut(), options));
    Ok(())
}

#[when("a program syncs them through the Rust crates")]
fn program_syncs(world: &World) -> Result<(), String> {
    program_sync(world, &options(world)?)
}

#[when("a program syncs them through the Rust crates with dryrun set")]
fn program_syncs_dryrun(world: &World) -> Result<(), String> {
    let options = Options {
        dryrun: true,
        ..options(world)?
    };
    program_sync(world, &options)
}

#[then("the call returns a record whose table status for that row's table is stopped")]
fn call_names_stopped(world: &World) -> Result<(), String> {
    use smugglr_core::record::Table as Entry;
    match world.called.take().ok_or("no program called smugglr")? {
        Ok(record) if matches!(record.tables.get("notes"), Some(Entry::Stopped { .. })) => Ok(()),
        Ok(record) => Err(format!("the record is {}", record.json())),
        Err(err) => Err(format!("the call failed: {err}")),
    }
}

#[then("the call returns an error")]
fn call_fails(world: &World) -> Result<(), String> {
    match world.called.take().ok_or("no program called smugglr")? {
        // The place, not memory or the config directory.
        Err(smugglr::Error::Place(_)) => Ok(()),
        Err(err) => Err(format!("the call failed, but not on the place: {err}")),
        Ok(record) => Err(format!("the call returned a record: {}", record.json())),
    }
}

#[then("the call returns a record whose run status is preview")]
fn call_previews(world: &World) -> Result<(), String> {
    use smugglr_core::record::Status;
    let (_, target) = places(world)?;
    match world.called.take().ok_or("no program called smugglr")? {
        Ok(record) if record.status == Status::Preview => {
            // A preview writes nothing.
            match rows_of(&target, "SELECT * FROM notes")?.len() {
                0 => Ok(()),
                n => Err(format!("the dry run wrote {n} rows into the target")),
            }
        }
        Ok(record) => Err(format!("the record is {}", record.json())),
        Err(err) => Err(format!("the call failed: {err}")),
    }
}

/// Two places never synced, each holding the notes `notes` inserts: orders
/// o1 and o2 alike in both, o3 written differently in each, so orders stops
/// on o3.
fn orders_changed_in_both(world: &World, notes: &str) -> Result<(), String> {
    let orders = "INSERT INTO orders VALUES ('o1', 'tent'), ('o2', 'stove')";
    world.source.set(world.place(
        "source.db",
        &format!("{NOTES}; {notes}; {ORDERS}; {orders}, ('o3', 'lamp')"),
    )?);
    world.target.set(world.place(
        "target.db",
        &format!("{NOTES}; {notes}; {ORDERS}; {orders}, ('o3', 'rope')"),
    )?);
    Ok(())
}

/// Runs smugglr sync and fails unless the record stops orders.
fn sync_stopping_orders(world: &World) -> Result<(), String> {
    world.sync(&["--json"])?;
    let record = record(world)?;
    match record.pointer("/tables/orders/status") {
        Some(serde_json::Value::String(status)) if status == "stopped" => Ok(()),
        _ => Err(format!("orders did not stop: {record}{}", stderr(world))),
    }
}

#[given(
    "two places that hold orders alike except one row changed in both places, never synced before"
)]
fn orders_alike_never_synced(world: &World) -> Result<(), String> {
    orders_changed_in_both(world, "SELECT 1")?;
    world.watched.set("o1".to_owned());
    Ok(())
}

#[when(
    "the agent runs smugglr sync on them, orders stops, and another orders row that both places held alike is then deleted from the source"
)]
fn stop_then_delete_order(world: &World) -> Result<(), String> {
    sync_stopping_orders(world)?;
    let (source, _) = places(world)?;
    let id = world.watched.get().ok_or("no row watched")?;
    sql_on(&source, &format!("DELETE FROM orders WHERE id = '{id}'"))
}

#[when("the agent runs smugglr sync on them again with --sync-mode source")]
fn sync_again_source(world: &World) -> Result<(), String> {
    world.sync(&["--sync-mode", "source"]).map(|_| ())
}

#[then("that orders row is gone from the target")]
fn order_gone_from_target(world: &World) -> Result<(), String> {
    let (_, target) = places(world)?;
    let id = world.watched.get().ok_or("no row watched")?;
    match rows_of(&target, &format!("SELECT * FROM orders WHERE id = '{id}'"))?
        .into_iter()
        .next()
    {
        None => Ok(()),
        Some(row) => Err(format!("the target holds {row:?}{}", stderr(world))),
    }
}

#[given("two places that synced before, and a table memory holds nothing for")]
fn synced_with_unknown_table(world: &World) -> Result<(), String> {
    world
        .source
        .set(world.place("source.db", &format!("{NOTES}; {THREE_NOTES}"))?);
    world.target.set(world.place("target.db", NOTES)?);
    world.sync_ok()?;
    // orders comes after that sync, in both places, so the shapes match and
    // memory holds nothing for it.
    let (source, target) = places(world)?;
    sql_on(
        &source,
        &format!("{ORDERS}; INSERT INTO orders VALUES ('o1', 'tent')"),
    )?;
    sql_on(&target, ORDERS)
}

#[given("two places never synced, holding orders alike except one row changed in both places")]
fn orders_alike_but_one(world: &World) -> Result<(), String> {
    orders_changed_in_both(world, "SELECT 1")
}

#[when("the agent runs smugglr sync on them and orders stops")]
fn sync_orders_stops(world: &World) -> Result<(), String> {
    sync_stopping_orders(world)
}

/// Each orders key's hash in the place at `path`, and the place's address.
fn orders_hashes(path: &str) -> Result<(String, BTreeMap<Key, [u8; 16]>), String> {
    let mut place = LocalFile::open(path).map_err(|err| err.to_string())?;
    let table = place
        .tables()
        .map_err(|err| err.to_string())?
        .into_iter()
        .find(|table| table.name == "orders")
        .ok_or_else(|| format!("{path} has no orders"))?;
    let hashes = place
        .hashes(&table)
        .map_err(|err| err.to_string())?
        .into_iter()
        .collect();
    Ok((place.address().to_owned(), hashes))
}

#[then("memory holds the hash of every orders key whose rows were alike")]
fn memory_holds_alike_orders(world: &World) -> Result<(), String> {
    let (source, target) = places(world)?;
    let (here, ours) = orders_hashes(&source)?;
    let (there, theirs) = orders_hashes(&target)?;
    let alike: BTreeMap<Key, [u8; 16]> = ours
        .into_iter()
        .filter(|(key, hash)| theirs.get(key) == Some(hash))
        .collect();
    let kept = smugglr_memory::Memory::open(&world.config()?, &here, &there)
        .map_err(|err| err.to_string())?
        .table("orders");
    if alike.len() == 2 && kept == alike {
        Ok(())
    } else {
        Err(format!(
            "memory holds {kept:?} for orders, the alike rows hash {alike:?}"
        ))
    }
}
