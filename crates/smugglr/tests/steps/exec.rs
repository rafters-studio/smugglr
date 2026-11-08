//! Steps for `smugglr exec`, migratr's way to reach a place
//! (FR-SMUGGLR-084). migratr is played here: the local ledger is written
//! directly, with the up and down hashes migratr records.

use std::io::Write;
use std::process::{Command, Output, Stdio};

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, then, when};
use serde_json::Value as Json;
use sha2::{Digest, Sha256};

use crate::d1::d1;
use crate::migratr::state;
use crate::setup::{NOTES, World, rows_of, sql_on};

/// The ledger migratr keeps, with the hash columns.
const LEDGER: &str = "CREATE TABLE _migratr_migrations (version INTEGER PRIMARY KEY, \
     name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL, up_hash TEXT, \
     down_hash TEXT)";

const VERSION: &str = "20261107000001";

/// What a migration adding a tag runs, up and down.
const UP: [&str; 2] = [
    "ALTER TABLE notes ADD COLUMN tag TEXT",
    "CREATE INDEX notes_tag ON notes (tag)",
];
const DOWN: [&str; 2] = ["DROP INDEX notes_tag", "ALTER TABLE notes DROP COLUMN tag"];

/// One migration's up whose last statement fails where two notes share a
/// body.
const UNIQUE_BODY: [&str; 2] = [
    "ALTER TABLE notes ADD COLUMN tag TEXT",
    "CREATE UNIQUE INDEX notes_body ON notes (body)",
];

/// A schema holding a CHECK, a UNIQUE item, a trigger and a view.
const CHECKED: &str = "CREATE TABLE stock (id TEXT PRIMARY KEY, sku TEXT, qty INTEGER, \
     CHECK (qty >= 0), UNIQUE (sku)); \
     CREATE TABLE moves (id TEXT PRIMARY KEY, sku TEXT); \
     CREATE TRIGGER moved AFTER INSERT ON moves BEGIN \
       UPDATE stock SET qty = qty - 1 WHERE sku = NEW.sku; END; \
     CREATE VIEW low AS SELECT sku FROM stock WHERE qty < 5";

/// What a scenario's exec calls work with.
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct Exec {
    /// The local database's path.
    local: Slot<String>,
    /// The place's file.
    place: Slot<String>,
    /// The statements the local database ran.
    batch: Slot<Vec<String>>,
    /// The fingerprint migratr expects of the place.
    expect: Slot<String>,
    /// Each exec call's output.
    outputs: Slot<Vec<Output>>,
    /// The fingerprints the read form gave: the place's, then the file's.
    fingerprints: Slot<Vec<String>>,
}

#[fixture]
pub(crate) fn exec() -> Exec {
    Exec::default()
}

fn hash(batch: &[&str]) -> String {
    format!("{:x}", Sha256::digest(batch.join("\n").as_bytes()))
}

fn owned(batch: &[&str]) -> Vec<String> {
    batch.iter().map(|sql| (*sql).to_owned()).collect()
}

/// The ledger row migratr writes for the tag migration whose up is `up`.
fn ledger_row(up: &[&str]) -> String {
    format!(
        "{LEDGER}; INSERT INTO _migratr_migrations VALUES ({VERSION}, 'add_tag', 'sum', \
         '2026-11-07T00:00:00Z', '{}', '{}')",
        hash(up),
        hash(&DOWN)
    )
}

/// Runs `smugglr exec` with `args`, `batch` on stdin.
fn run(world: &World, args: &[&str], batch: &[String]) -> Result<Output, String> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_smugglr"))
        .arg("exec")
        .args(args)
        .env("XDG_CONFIG_HOME", world.xdg()?)
        .envs(world.env.get().unwrap_or_default())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("failed to start smugglr: {err}"))?;
    let input = serde_json::to_string(batch).map_err(|err| err.to_string())?;
    child
        .stdin
        .take()
        .ok_or("no stdin")?
        .write_all(input.as_bytes())
        // A call that reads no batch may have exited already.
        .or_else(|err| match err.kind() {
            std::io::ErrorKind::BrokenPipe => Ok(()),
            _ => Err(err.to_string()),
        })?;
    child.wait_with_output().map_err(|err| err.to_string())
}

fn json(output: &Output) -> Result<Json, String> {
    serde_json::from_slice(&output.stdout).map_err(|err| {
        format!(
            "not JSON ({err}): {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

/// The read form's fingerprint of `address`.
fn fingerprint(world: &World, address: &str) -> Result<String, String> {
    let output = run(world, &[address, "--read"], &[])?;
    json(&output)?["fingerprint"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("no fingerprint: {output:?}"))
}

/// The local database and the place file, the place's state and
/// fingerprint kept as before.
fn pair(world: &World, exec: &Exec, here: &str, there: &str) -> Result<(), String> {
    let n = exec.outputs.get().map_or(0, |outputs| outputs.len());
    exec.local.set(world.place(&format!("local{n}.db"), here)?);
    let place = world.place(&format!("place{n}.db"), there)?;
    exec.expect.set(fingerprint(world, &place)?);
    keep(world, exec, &place)?;
    exec.place.set(place);
    Ok(())
}

/// The place as it is, for the migratr step that says it has not changed.
fn keep(world: &World, exec: &Exec, place: &str) -> Result<(), String> {
    let local = exec.local.get().ok_or("no local")?;
    world.service.set(place.to_owned());
    world.carried.set((state(&local)?, state(place)?));
    Ok(())
}

/// The local database has run the tag migration with `up`; the place has
/// not.
fn ran_here(world: &World, exec: &Exec, up: &[&str], there: &str) -> Result<(), String> {
    let here = format!("{NOTES}; {}; {}", up.join("; "), ledger_row(up));
    pair(world, exec, &here, there)?;
    exec.batch.set(owned(up));
    Ok(())
}

fn call(world: &World, exec: &Exec, way: &str, batch: &[String]) -> Result<Output, String> {
    let (local, place) = (
        exec.local.get().ok_or("no local")?,
        exec.place.get().ok_or("no place")?,
    );
    let expect = exec.expect.get().ok_or("no fingerprint expected")?;
    let output = run(
        world,
        &[
            &local,
            &place,
            "--version",
            VERSION,
            way,
            "--expect",
            &expect,
        ],
        batch,
    )?;
    let mut outputs = exec.outputs.get().unwrap_or_default();
    outputs.push(output.clone());
    exec.outputs.set(outputs);
    Ok(output)
}

fn last(exec: &Exec) -> Result<Output, String> {
    exec.outputs
        .get()
        .and_then(|outputs| outputs.last().cloned())
        .ok_or_else(|| "exec was not run".to_owned())
}

fn refused(exec: &Exec, reason: &str) -> Result<(), String> {
    let output = last(exec)?;
    let said = json(&output)?;
    if output.status.code() == Some(3) && said["refused"] == reason {
        Ok(())
    } else {
        Err(format!(
            "expected a {reason} refusal, exit 3: {said} {output:?}"
        ))
    }
}

fn batch_of(exec: &Exec) -> Result<Vec<String>, String> {
    exec.batch.get().ok_or_else(|| "no statements".to_owned())
}

/// The batch with one byte of its last statement changed.
fn one_byte_changed(batch: &[String]) -> Vec<String> {
    let mut changed = batch.to_vec();
    if let Some(statement) = changed.last_mut() {
        *statement = statement.replacen("tag", "tah", 1);
    }
    changed
}

#[given("a local database whose ledger records a migration and the hash of its statements")]
#[given("a local database whose ledger records a migration with its up and down hashes")]
fn local_ran_it(world: &World, exec: &Exec) -> Result<(), String> {
    ran_here(world, exec, &UP, NOTES)
}

#[given("a place whose fingerprint is the one expected before that migration")]
fn place_as_expected(exec: &Exec) -> Result<(), String> {
    exec.expect
        .get()
        .map(drop)
        .ok_or_else(|| "no place".to_owned())
}

#[given("a place with a column added outside migratr")]
fn column_added(world: &World, exec: &Exec) -> Result<(), String> {
    ran_here(world, exec, &UP, NOTES)?;
    let place = exec.place.get().ok_or("no place")?;
    sql_on(&place, "ALTER TABLE notes ADD COLUMN extra TEXT")?;
    keep(world, exec, &place)
}

#[given("statements the local database ran whose last one fails on the place")]
fn last_fails_there(world: &World, exec: &Exec) -> Result<(), String> {
    let twice = format!("{NOTES}; INSERT INTO notes VALUES ('n1', 'same', 1), ('n2', 'same', 2)");
    ran_here(world, exec, &UNIQUE_BODY, &twice)
}

#[given("a place")]
fn a_place(world: &World, exec: &Exec) -> Result<(), String> {
    let place = format!(
        "{NOTES}; INSERT INTO notes VALUES ('n1', 'secret', 1); {}",
        ledger_row(&UP)
    );
    pair(world, exec, NOTES, &place)
}

#[given("a place file whose ledger holds a version with its down hash")]
fn place_holds_version(world: &World, exec: &Exec) -> Result<(), String> {
    let there = format!("{NOTES}; {}; {}", UP.join("; "), ledger_row(&UP));
    pair(world, exec, &format!("{NOTES}; {LEDGER}"), &there)?;
    exec.batch.set(owned(&DOWN));
    Ok(())
}

#[given("a local database whose ledger no longer holds that version")]
fn local_let_it_go(exec: &Exec) -> Result<(), String> {
    let local = exec.local.get().ok_or("no local")?;
    let held = rows_of(&local, "SELECT version FROM _migratr_migrations")?;
    if held.is_empty() {
        Ok(())
    } else {
        Err(format!("the local ledger holds {held:?}"))
    }
}

#[given(
    "instead a local database whose ledger still holds that version, or a batch with one byte changed"
)]
fn instead(world: &World, exec: &Exec) -> Result<(), String> {
    place_holds_version(world, exec)?;
    let local = exec.local.get().ok_or("no local")?;
    sql_on(
        &local,
        &format!(
            "INSERT INTO _migratr_migrations VALUES ({VERSION}, 'add_tag', 'sum', 'now', '{}', '{}')",
            hash(&UP),
            hash(&DOWN)
        ),
    )
}

#[when("migratr calls smugglr exec with those statements")]
#[when("migratr calls smugglr exec with statements the local database ran")]
#[when("migratr runs exec with that migration's statements on a place file")]
fn exec_up(world: &World, exec: &Exec) -> Result<(), String> {
    call(world, exec, "--up", &batch_of(exec)?).map(drop)
}

#[when("migratr calls smugglr exec with them")]
fn exec_up_failing(world: &World, exec: &Exec) -> Result<(), String> {
    let batch = batch_of(exec)?;
    let output = call(world, exec, "--up", &batch)?;
    let said = json(&output)?;
    if output.status.code() == Some(1) && said["failed"] == batch[batch.len() - 1] {
        Ok(())
    } else {
        Err(format!(
            "expected the last statement to fail, exit 1: {said}"
        ))
    }
}

#[when("migratr calls smugglr exec with one byte of those statements changed")]
fn exec_changed(world: &World, exec: &Exec) -> Result<(), String> {
    call(world, exec, "--up", &one_byte_changed(&batch_of(exec)?)).map(drop)
}

#[when("migratr runs exec with the down statements for that version")]
fn exec_down(world: &World, exec: &Exec) -> Result<(), String> {
    call(world, exec, "--down", &batch_of(exec)?).map(drop)
}

#[when("migratr runs exec with the down statements")]
fn exec_down_both(world: &World, exec: &Exec) -> Result<(), String> {
    let batch = batch_of(exec)?;
    // The local ledger still holds the version.
    call(world, exec, "--down", &batch)?;
    // It no longer does, and one byte is changed.
    let local = exec.local.get().ok_or("no local")?;
    sql_on(&local, "DELETE FROM _migratr_migrations")?;
    call(world, exec, "--down", &one_byte_changed(&batch)).map(drop)
}

#[when("migratr calls smugglr exec's read form for the schema and the ledger")]
fn read_form(world: &World, exec: &Exec) -> Result<(), String> {
    let place = exec.place.get().ok_or("no place")?;
    exec.outputs
        .set(vec![run(world, &[&place, "--read"], &[])?]);
    Ok(())
}

#[then("the place has run them")]
fn place_ran(exec: &Exec) -> Result<(), String> {
    let output = last(exec)?;
    let place = exec.place.get().ok_or("no place")?;
    let index = rows_of(
        &place,
        "SELECT name FROM sqlite_master WHERE name = 'notes_tag'",
    )?;
    if output.status.success() && !index.is_empty() {
        Ok(())
    } else {
        Err(format!("the place did not run them: {output:?}"))
    }
}

#[then("exec returns the place's new fingerprint")]
fn new_fingerprint(world: &World, exec: &Exec) -> Result<(), String> {
    let said = json(&last(exec)?)?;
    let place = exec.place.get().ok_or("no place")?;
    let local = exec.local.get().ok_or("no local")?;
    let now = fingerprint(world, &place)?;
    let given = said["fingerprint"].as_str().unwrap_or_default();
    if given == now && now == fingerprint(world, &local)? && Some(now) != exec.expect.get() {
        Ok(())
    } else {
        Err(format!(
            "exec returned {said}, not the place's new fingerprint"
        ))
    }
}

#[then("exec says the statements are not the ones the local database ran")]
fn says_not_ran(exec: &Exec) -> Result<(), String> {
    refused(exec, "hash")
}

#[then("exec says the place's shape is not the one expected")]
fn says_shape(exec: &Exec) -> Result<(), String> {
    refused(exec, "fingerprint")
}

#[then("it returns them as JSON")]
fn returns_json(exec: &Exec) -> Result<(), String> {
    let said = json(&last(exec)?)?;
    let has_notes = said["schema"]
        .as_array()
        .is_some_and(|rows| rows.iter().any(|row| row["name"] == "notes"));
    let ledger = &said["ledger"][0];
    let whole = ledger["version"] == VERSION.parse::<i64>().unwrap_or_default()
        && ledger["up_hash"] == hash(&UP)
        && ledger["applied_at"].is_string();
    let fingerprint = said["fingerprint"].as_str().unwrap_or_default();
    if has_notes
        && whole
        && fingerprint.len() == 64
        && said.as_object().map(serde_json::Map::len) == Some(3)
    {
        Ok(())
    } else {
        Err(format!(
            "not the schema, the ledger and the fingerprint: {said}"
        ))
    }
}

#[then("a read form asking for a table's rows is refused")]
fn rows_refused(world: &World, exec: &Exec) -> Result<(), String> {
    let place = exec.place.get().ok_or("no place")?;
    let output = run(world, &[&place, "--read", "SELECT * FROM notes"], &[])?;
    let printed = format!("{output:?}");
    if output.status.code() == Some(2) && !printed.contains("secret") {
        Ok(())
    } else {
        Err(format!("the query was not refused: {printed}"))
    }
}

#[then("the place's ledger holds that version with the same up and down hashes")]
fn ledger_holds(exec: &Exec) -> Result<(), String> {
    let place = exec.place.get().ok_or("no place")?;
    let held = rows_of(
        &place,
        "SELECT version, up_hash, down_hash FROM _migratr_migrations",
    )?;
    let wanted = vec![vec![
        format!("Integer({VERSION})"),
        format!("Text({:?})", hash(&UP)),
        format!("Text({:?})", hash(&DOWN)),
    ]];
    if held == wanted {
        Ok(())
    } else {
        Err(format!("the place's ledger holds {held:?}"))
    }
}

#[then("they run and the place's ledger no longer holds that version")]
fn down_ran(exec: &Exec) -> Result<(), String> {
    let output = last(exec)?;
    let place = exec.place.get().ok_or("no place")?;
    let held = rows_of(&place, "SELECT version FROM _migratr_migrations")?;
    let tag = rows_of(
        &place,
        "SELECT 1 FROM pragma_table_info('notes') WHERE name = 'tag'",
    )?;
    if output.status.success() && held.is_empty() && tag.is_empty() {
        Ok(())
    } else {
        Err(format!("the down did not run: {held:?} {output:?}"))
    }
}

#[then("exec refuses and the place has not changed")]
fn down_refused(world: &World, exec: &Exec) -> Result<(), String> {
    let outputs = exec.outputs.get().unwrap_or_default();
    let reasons: Vec<Json> = outputs
        .iter()
        .skip(1)
        .map(|output| json(output).map(|said| said["refused"].clone()))
        .collect::<Result<_, _>>()?;
    if reasons != ["version", "hash"] {
        return Err(format!(
            "expected a version and a hash refusal: {reasons:?}"
        ));
    }
    let place = exec.place.get().ok_or("no place")?;
    let was = world.carried.get().ok_or("no place kept")?.1;
    if state(&place)? == was {
        Ok(())
    } else {
        Err("the place changed".to_owned())
    }
}

#[given(
    "a d1:// place and a local file holding the same schema with a CHECK, a UNIQUE item, a trigger and a view"
)]
fn d1_and_file(world: &World, exec: &Exec) -> Result<(), String> {
    let (address, _) = d1(world, "shop", "place.db", CHECKED)?;
    exec.place.set(address);
    exec.local.set(world.place("local.db", CHECKED)?);
    Ok(())
}

#[when("migratr runs exec's read form on each")]
fn read_each(world: &World, exec: &Exec) -> Result<(), String> {
    let place = exec.place.get().ok_or("no place")?;
    let local = exec.local.get().ok_or("no local")?;
    exec.fingerprints.set(vec![
        fingerprint(world, &place)?,
        fingerprint(world, &local)?,
    ]);
    Ok(())
}

#[then("both fingerprints are equal")]
fn equal(exec: &Exec) -> Result<(), String> {
    match exec.fingerprints.get().as_deref() {
        Some([place, file]) if place == file => Ok(()),
        other => Err(format!("the fingerprints differ: {other:?}")),
    }
}

#[given("the local file's CHECK changed")]
fn check_changed(world: &World, exec: &Exec) -> Result<(), String> {
    exec.local
        .set(world.place("changed.db", &CHECKED.replace("qty >= 0", "qty >= 1"))?);
    Ok(())
}

#[when("migratr runs exec's read form on it")]
fn read_it(world: &World, exec: &Exec) -> Result<(), String> {
    let local = exec.local.get().ok_or("no local")?;
    let mut fingerprints = exec.fingerprints.get().unwrap_or_default();
    fingerprints.truncate(1);
    fingerprints.push(fingerprint(world, &local)?);
    exec.fingerprints.set(fingerprints);
    Ok(())
}

#[then("its fingerprint differs from the place's")]
fn differs(exec: &Exec) -> Result<(), String> {
    match exec.fingerprints.get().as_deref() {
        Some([place, file]) if place != file => Ok(()),
        other => Err(format!("the fingerprints are equal: {other:?}")),
    }
}

#[when("the agent runs smugglr --help")]
fn help(world: &World) -> Result<(), String> {
    world.smugglr(&["--help"]).map(drop)
}

#[then("exec is not named")]
fn exec_not_named(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let printed = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if printed.contains("exec") {
        Err(format!("--help names exec: {printed}"))
    } else {
        Ok(())
    }
}
