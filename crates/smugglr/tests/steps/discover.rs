//! Steps for `smugglr sync --discover`: real SQLite files, and the CLI run
//! on them.

use rstest_bdd_macros::{given, then, when};
use serde_json::Value;

use crate::setup::{NOTES, ORDERS, THREE_NOTES, World};
use crate::shape::snapshot;

/// Both places' tables: a parent and its child, notes, a trigger, a view,
/// an index, and a table the config ignores.
const HELD: &str = "CREATE TABLE org (id TEXT PRIMARY KEY, name TEXT); \
     CREATE TABLE member (id TEXT PRIMARY KEY, org TEXT REFERENCES org(id)); \
     CREATE INDEX member_org ON member(org); \
     CREATE VIEW org_names AS SELECT name FROM org; \
     CREATE TRIGGER org_named AFTER INSERT ON org BEGIN SELECT 1; END; \
     CREATE TABLE cache_pages (url TEXT PRIMARY KEY, body BLOB);";

fn both(world: &World, sql: &str) -> Result<(), String> {
    world.source.set(world.place("source.db", sql)?);
    world.target.set(world.place("target.db", sql)?);
    Ok(())
}

/// What smugglr printed to stdout.
fn stdout(world: &World) -> Result<String, String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[given("two places holding tables, a trigger, a view and an index")]
fn tables_trigger_view_index(world: &World) -> Result<(), String> {
    both(
        world,
        &format!("{NOTES}; {THREE_NOTES}; {ORDERS}; {HELD} INSERT INTO org VALUES ('o1', 'one');"),
    )?;
    let config = world.config()?;
    std::fs::create_dir_all(&config).map_err(|err| err.to_string())?;
    std::fs::write(config.join("config.toml"), "ignore = [\"cache_*\"]\n")
        .map_err(|err| err.to_string())?;
    world.watched.set(snapshot(world)?);
    Ok(())
}

#[given("a place with two tables that reference each other")]
fn two_tables_in_a_cycle(world: &World) -> Result<(), String> {
    both(
        world,
        "CREATE TABLE a (id TEXT PRIMARY KEY, b TEXT REFERENCES b(id)); \
         CREATE TABLE b (id TEXT PRIMARY KEY, a TEXT REFERENCES a(id));",
    )
}

#[given("a place with a table of many rows")]
fn many_rows(world: &World) -> Result<(), String> {
    both(
        world,
        &format!(
            "{NOTES}; WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n \
             WHERE i < 5000) INSERT INTO notes SELECT 'n' || i, 'body ' || i, i FROM n;"
        ),
    )
}

#[given("two places whose shapes differ")]
fn shapes_differ(world: &World) -> Result<(), String> {
    world.source.set(world.place("source.db", ORDERS)?);
    world.target.set(world.place(
        "target.db",
        "CREATE TABLE orders (id TEXT PRIMARY KEY, item TEXT, discount INTEGER)",
    )?);
    Ok(())
}

#[when("the agent runs smugglr sync on them with --discover")]
fn discover_on_them(world: &World) -> Result<(), String> {
    world.sync(&["--discover"]).map(|_| ())
}

#[when("the agent runs smugglr sync with --discover")]
fn discover(world: &World) -> Result<(), String> {
    world.sync(&["--discover"]).map(|_| ())
}

#[when("the agent runs smugglr sync with --discover --json")]
fn discover_json(world: &World) -> Result<(), String> {
    world.sync(&["--discover", "--json"]).map(|_| ())
}

#[then("each place's identity is printed first")]
fn identity_first(world: &World) -> Result<(), String> {
    let said = stdout(world)?;
    for (side, place) in [
        ("source", world.source.get()),
        ("target", world.target.get()),
    ] {
        let place = place.ok_or("no place named")?;
        let name = std::path::Path::new(&place)
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let section = said
            .split(&format!("{side}: "))
            .nth(1)
            .ok_or_else(|| format!("no {side} section: {said}"))?;
        let first = section.lines().next().unwrap_or_default();
        if !(first.starts_with("file ") && first.ends_with(&name)) {
            return Err(format!("the {side} section begins {first:?}: {said}"));
        }
    }
    Ok(())
}

#[then(
    "its ledger, tables with rows and estimated bytes, copy order, triggers, views, indexes and ignored tables are printed"
)]
fn everything_printed(world: &World) -> Result<(), String> {
    let said = stdout(world)?;
    for line in [
        "  ledger: none",
        "  table notes: 3 rows, about ",
        "  copy order: notes, orders, org, member",
        "  triggers: org_named on org",
        "  views: org_names",
        "  indexes: member_org on member",
        "  ignored: cache_pages",
    ] {
        if said.matches(line).count() != 2 {
            return Err(format!("not {line:?} for each place: {said}"));
        }
    }
    Ok(())
}

#[then("the cycle is listed first as blocking")]
fn cycle_first(world: &World) -> Result<(), String> {
    let said = stdout(world)?;
    let first = said.lines().next().unwrap_or_default();
    if first.starts_with("blocking: the tables a, b in ") && first.contains("reference each other")
    {
        Ok(())
    } else {
        Err(format!("the first line is {first:?}: {said}"))
    }
}

#[then("the exit code is 3")]
fn exit_code_3(world: &World) -> Result<(), String> {
    exit_3(world)
}

#[then("the run stops")]
fn run_stops(world: &World) -> Result<(), String> {
    exit_3(world)
}

fn exit_3(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    if output.status.code() == Some(3) {
        Ok(())
    } else {
        Err(format!(
            "smugglr exited {:?}, saying {:?}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

#[then("that table's bytes are printed as an estimate")]
fn bytes_estimated(world: &World) -> Result<(), String> {
    let said = stdout(world)?;
    let line = said
        .lines()
        .find(|line| line.starts_with("  table notes: 5000 rows, about "))
        .ok_or_else(|| format!("no line for notes: {said}"))?;
    if line.ends_with("bytes, estimated from a sample of 1000 rows") {
        Ok(())
    } else {
        Err(format!("notes reads {line:?}"))
    }
}

#[then("the output has identity, ledger, blocking and shapes_match keys")]
fn json_keys(world: &World) -> Result<(), String> {
    let said = stdout(world)?;
    let report: Value = serde_json::from_str(&said).map_err(|err| format!("{err}: {said}"))?;
    for side in ["source", "target"] {
        for key in ["identity", "ledger"] {
            if report[side].get(key).is_none() {
                return Err(format!("{side} has no {key}: {report}"));
            }
        }
    }
    if report["blocking"].is_array() && report["shapes_match"].is_boolean() {
        Ok(())
    } else {
        Err(format!("no blocking or shapes_match: {report}"))
    }
}

#[then("its output names --discover")]
fn names_discover(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = String::from_utf8_lossy(&output.stderr);
    let last = said.lines().last().unwrap_or_default();
    if last.contains("--discover") {
        Ok(())
    } else {
        Err(format!("stderr ends {last:?}"))
    }
}
