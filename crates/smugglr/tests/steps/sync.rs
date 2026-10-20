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
        key: vec!["id".to_owned()],
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
    match sync(&mut source, &mut target, &options(world)?) {
        Err(smugglr::Error::Place(Error::Adapter(message))) if message == CUT_OFF => Ok(()),
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
    let source = world.place(
        "source.db",
        &format!("{NOTES}; {THREE_NOTES}; {ORDERS}; INSERT INTO orders VALUES ('o1', 'tent')"),
    )?;
    let target = world.place("target.db", &format!("{NOTES}; {ORDERS}"))?;
    world.source.set(source.clone());
    world.target.set(target.clone());
    let open = |path: &str| LocalFile::open(path).map_err(|err| err.to_string());
    let run = |mode| {
        let options = Options {
            sync_mode: mode,
            ..options(world)?
        };
        sync(&mut open(&source)?, &mut open(&target)?, &options).map_err(|err| err.to_string())
    };
    run(SyncMode::Stop)?;
    sql_on(
        &source,
        "UPDATE notes SET body = 'source copy' WHERE id = 'n1'; \
                     INSERT INTO orders VALUES ('o2', 'stove')",
    )?;
    sql_on(
        &target,
        "UPDATE notes SET body = 'target copy' WHERE id = 'n1'",
    )?;
    run(mode)
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
