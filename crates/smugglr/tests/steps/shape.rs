//! Steps for a shape mismatch: real SQLite files whose shapes differ, and
//! the CLI run on them.

use rstest_bdd_macros::{given, then, when};
use serde_json::Value;

use crate::setup::{NOTES, World, rows_of};

/// The orders table both places hold, unless a scenario changes it.
const ORDERS: &str = "CREATE TABLE orders (id TEXT PRIMARY KEY, item TEXT)";

const ROWS: &str = "INSERT INTO notes VALUES ('n1', 'first', 1), ('n2', 'second', 2)";

/// The column the target's orders has and the source's lacks.
const EXTRA: &str = "discount";

fn places(world: &World, source: &str, target: &str) -> Result<(), String> {
    world.source.set(world.place("source.db", source)?);
    world.target.set(world.place("target.db", target)?);
    Ok(())
}

/// Both places hold notes and orders; the target's orders has [`EXTRA`],
/// and the source's notes holds `notes_rows`.
fn extra_column(world: &World, notes_rows: &str) -> Result<(), String> {
    places(
        world,
        &format!("{NOTES}; {notes_rows} {ORDERS};"),
        &format!("{NOTES}; CREATE TABLE orders (id TEXT PRIMARY KEY, item TEXT, {EXTRA} INTEGER)"),
    )
}

/// Every table, index and row of each place, with its `user_version` and
/// `application_id`; the rows of notes and orders where the place has them.
pub(crate) fn snapshot(world: &World) -> Result<String, String> {
    let mut all = Vec::new();
    for place in [world.source.get(), world.target.get()] {
        let path = place.ok_or("no place named")?;
        all.push(rows_of(&path, "SELECT * FROM sqlite_schema ORDER BY name")?);
        all.push(rows_of(&path, "PRAGMA user_version")?);
        all.push(rows_of(&path, "PRAGMA application_id")?);
        for table in ["notes", "orders"] {
            let held =
                format!("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '{table}'");
            if rows_of(&path, &held)?.is_empty() {
                continue;
            }
            all.push(rows_of(
                &path,
                &format!("SELECT * FROM {table} ORDER BY id"),
            )?);
        }
    }
    Ok(format!("{all:?}"))
}

#[given("two places whose orders table has a column in the target that the source lacks")]
fn target_has_extra_column(world: &World) -> Result<(), String> {
    extra_column(world, "")?;
    world.watched.set(EXTRA.to_owned());
    Ok(())
}

#[given(
    "two places whose orders table has a column in the target that the source lacks, and whose notes table has rows to move"
)]
fn extra_column_and_rows(world: &World) -> Result<(), String> {
    extra_column(world, &format!("{ROWS};"))?;
    world.watched.set(snapshot(world)?);
    Ok(())
}

#[given("two places whose orders tables have the same columns in a different order")]
fn columns_reordered(world: &World) -> Result<(), String> {
    places(
        world,
        "CREATE TABLE orders (id TEXT PRIMARY KEY, item TEXT, qty INTEGER)",
        "CREATE TABLE orders (id TEXT PRIMARY KEY, qty INTEGER, item TEXT)",
    )
}

#[given("a source with tables and a target with none")]
fn empty_target(world: &World) -> Result<(), String> {
    world
        .source
        .set(world.place("source.db", &format!("{NOTES}; {ROWS}; {ORDERS}"))?);
    // A file of no bytes is an empty SQLite database.
    let target = world.folder()?.join("target.db");
    std::fs::write(&target, b"").map_err(|err| format!("{}: {err}", target.display()))?;
    world.target.set(target.display().to_string());
    Ok(())
}

#[when("the agent runs smugglr sync on them with --json")]
fn agent_syncs_json(world: &World) -> Result<(), String> {
    world.sync(&["--json"]).map(|_| ())
}

/// The record smugglr printed as JSON.
fn record(world: &World) -> Result<Value, String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&said).map_err(|err| {
        format!(
            "smugglr did not print a record ({err}): {said:?}, saying {:?}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

/// The record's shape stop.
fn shape(world: &World) -> Result<Value, String> {
    let record = record(world)?;
    record
        .get("shape")
        .cloned()
        .ok_or_else(|| format!("the record names no shape difference: {record}"))
}

#[then("the record's run status is stopped")]
fn run_stopped(world: &World) -> Result<(), String> {
    let record = record(world)?;
    match record.get("status").and_then(Value::as_str) {
        Some("stopped") => Ok(()),
        _ => Err(format!("the record is {record}")),
    }
}

#[then("neither place has changed")]
fn nothing_changed(world: &World) -> Result<(), String> {
    world.output.get().ok_or("smugglr was not run")?;
    let before = world.watched.get().ok_or("no snapshot taken")?;
    let after = snapshot(world)?;
    if before == after {
        Ok(())
    } else {
        Err(format!("before the run {before}, after it {after}"))
    }
}

#[then("the record names that column")]
fn names_column(world: &World) -> Result<(), String> {
    let shape = shape(world)?;
    let column = world.watched.get().ok_or("no column watched")?;
    let named = shape
        .get("columns")
        .and_then(Value::as_array)
        .is_some_and(|columns| {
            columns
                .iter()
                .any(|c| c.get("name").and_then(Value::as_str) == Some(column.as_str()))
        });
    if named && shape.get("table").and_then(Value::as_str) == Some("orders") {
        Ok(())
    } else {
        Err(format!("the record's shape stop is {shape}"))
    }
}

#[then("the record says the target has no tables")]
fn says_no_tables(world: &World) -> Result<(), String> {
    let shape = shape(world)?;
    let why = shape.get("why").and_then(Value::as_str).unwrap_or_default();
    if why.to_lowercase().contains("the target has no tables") {
        Ok(())
    } else {
        Err(format!("the record's shape stop is {shape}"))
    }
}

#[then("the target still has no tables")]
fn still_no_tables(world: &World) -> Result<(), String> {
    world.output.get().ok_or("smugglr was not run")?;
    let target = world.target.get().ok_or("no target")?;
    let schema = rows_of(&target, "SELECT * FROM sqlite_schema")?;
    if schema.is_empty() {
        Ok(())
    } else {
        Err(format!("the target now holds {schema:?}"))
    }
}
