//! Steps for one sync between two places as they are: real SQLite files,
//! smugglr's memory in its config directory, an adapter written here.

use std::collections::BTreeMap;

use smugglr::{Options, sync};
use smugglr_core::change::{Change, Op};
use smugglr_core::row_hash::row_hash;
use smugglr_place::{Error, Key, LocalFile, Place, Table};

use rstest_bdd_macros::{given, then, when};

use crate::setup::{NOTES, World, note, rows_of, sql_on};

const THREE_NOTES: &str = "INSERT INTO notes VALUES ('n1', 'first', 1), ('n2', 'second', 2), \
     ('n3', 'third', 3)";

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

    fn write(&mut self, table: &Table, changes: &[Change]) -> Result<(), Error> {
        let (_, rows) = self
            .tables
            .get_mut(&table.name)
            .ok_or_else(|| Error::Adapter(format!("no table {}", table.name)))?;
        for change in changes {
            match change.op {
                Op::Down => {
                    rows.remove(&change.key);
                }
                Op::Up | Op::Upsert => {
                    rows.insert(change.key.clone(), change.values.clone());
                }
            }
        }
        Ok(())
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
    let held = world.watched.get().ok_or("the service was not synced")?;
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

    fn write(&mut self, table: &Table, changes: &[Change]) -> Result<(), Error> {
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

#[when("the agent runs smugglr sync on them with --json")]
fn sync_json(world: &World) -> Result<(), String> {
    world.sync(&["--json"]).map(|_| ())
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
    world.watched.set(snapshot(world)?);
    Ok(())
}

#[then("neither place has changed")]
fn neither_changed(world: &World) -> Result<(), String> {
    let before = world.watched.get().ok_or("no snapshot taken")?;
    // The run reached the places and said what it did, so nothing changing
    // is its doing, not a failure to start.
    let output = world.output.get().ok_or("smugglr was not run")?;
    if !String::from_utf8_lossy(&output.stdout).starts_with("smugglr sync:") {
        return Err(format!("smugglr printed no record{}", stderr(world)));
    }
    let after = snapshot(world)?;
    if before == after {
        Ok(())
    } else {
        Err(format!(
            "a place changed: before {before}, after {after}{}",
            stderr(world)
        ))
    }
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
