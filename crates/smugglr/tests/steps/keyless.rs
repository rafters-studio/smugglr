//! Steps for a table without a primary key (FR-SMUGGLR-086).

use rstest_bdd_macros::{given, then, when};
use serde_json::Value;

use crate::setup::{World, rows_of, sql_on};

/// The keyless notes table.
const KEYLESS: &str = "CREATE TABLE notes (body TEXT, n INTEGER)";

/// The full-text table docs.
const DOCS: &str = "CREATE VIRTUAL TABLE docs USING fts5(title, body)";

/// The notes rows of the file at `path`, sorted.
fn notes(path: &str) -> Result<Vec<Vec<String>>, String> {
    rows_of(path, "SELECT body, n FROM notes ORDER BY body, n")
}

fn places(world: &World) -> Result<(String, String), String> {
    Ok((
        world.source.get().ok_or("no source named")?,
        world.target.get().ok_or("no target named")?,
    ))
}

#[given("two place files each holding a table notes with no primary key, with different rows")]
fn keyless_different(world: &World) -> Result<(), String> {
    world.source.set(world.place(
        "source.db",
        &format!("{KEYLESS}; INSERT INTO notes VALUES ('a', 1), ('b', 2)"),
    )?);
    world.target.set(world.place(
        "target.db",
        &format!("{KEYLESS}; INSERT INTO notes VALUES ('c', 3)"),
    )?);
    Ok(())
}

#[then("each place holds both sides' rows")]
fn both_rows(world: &World) -> Result<(), String> {
    let (source, target) = places(world)?;
    let want = vec![
        vec!["Text(\"a\")".to_owned(), "Integer(1)".to_owned()],
        vec!["Text(\"b\")".to_owned(), "Integer(2)".to_owned()],
        vec!["Text(\"c\")".to_owned(), "Integer(3)".to_owned()],
    ];
    for path in [&source, &target] {
        let held = notes(path)?;
        if held != want {
            return Err(format!("{path} holds {held:?}"));
        }
    }
    Ok(())
}

#[then("a second sync moves nothing")]
fn second_moves_nothing(world: &World) -> Result<(), String> {
    let output = world.sync(&["--json"])?;
    let said = String::from_utf8_lossy(&output.stdout);
    let record: Value = serde_json::from_str(&said).map_err(|err| format!("{err}: {said}"))?;
    let tables = record["tables"]
        .as_object()
        .ok_or("a record without tables")?;
    if output.status.success() && tables.values().all(|table| table["status"] == "in step") {
        Ok(())
    } else {
        Err(format!("the second sync exited {}: {said}", output.status))
    }
}

#[given("two place files in step holding a keyless table notes")]
fn keyless_in_step(world: &World) -> Result<(), String> {
    let rows = format!("{KEYLESS}; INSERT INTO notes VALUES ('a', 1), ('b', 2)");
    world.source.set(world.place("source.db", &rows)?);
    world.target.set(world.place("target.db", &rows)?);
    world.sync_ok()
}

#[given("one side changes a row's values")]
fn change_a_row(world: &World) -> Result<(), String> {
    let (source, _) = places(world)?;
    sql_on(&source, "UPDATE notes SET body = 'z' WHERE body = 'a'")
}

#[then("the other side holds the changed row and not the old one")]
fn changed_row_arrived(world: &World) -> Result<(), String> {
    let (_, target) = places(world)?;
    let held = notes(&target)?;
    let want = vec![
        vec!["Text(\"b\")".to_owned(), "Integer(2)".to_owned()],
        vec!["Text(\"z\")".to_owned(), "Integer(1)".to_owned()],
    ];
    if held == want {
        Ok(())
    } else {
        Err(format!("the target holds {held:?}"))
    }
}

#[given(
    "a place file whose keyless table notes holds two identical rows, and an empty place file with the same table"
)]
fn identical_rows(world: &World) -> Result<(), String> {
    world.source.set(world.place(
        "source.db",
        &format!("{KEYLESS}; INSERT INTO notes VALUES ('a', 1), ('a', 1)"),
    )?);
    world.target.set(world.place("target.db", KEYLESS)?);
    Ok(())
}

#[then("the empty place holds two identical rows")]
fn two_identical(world: &World) -> Result<(), String> {
    let (_, target) = places(world)?;
    let held = notes(&target)?;
    let row = vec!["Text(\"a\")".to_owned(), "Integer(1)".to_owned()];
    if held == vec![row.clone(), row] {
        Ok(())
    } else {
        Err(format!("the target holds {held:?}"))
    }
}

#[given("two place files each holding an FTS5 table docs, one with two rows")]
fn fts_places(world: &World) -> Result<(), String> {
    world.source.set(world.place(
        "source.db",
        &format!(
            "{DOCS}; INSERT INTO docs VALUES ('one', 'smuggled rum'), \
             ('two', 'smuggled silk')"
        ),
    )?);
    world.target.set(world.place("target.db", DOCS)?);
    Ok(())
}

#[then("a full-text MATCH on the other place finds both rows")]
fn match_finds_both(world: &World) -> Result<(), String> {
    let (_, target) = places(world)?;
    let found = rows_of(
        &target,
        "SELECT title FROM docs WHERE docs MATCH 'smuggled' ORDER BY title",
    )?;
    let want = vec![
        vec!["Text(\"one\")".to_owned()],
        vec!["Text(\"two\")".to_owned()],
    ];
    if found == want {
        Ok(())
    } else {
        Err(format!("MATCH found {found:?}"))
    }
}

#[then("no shadow table of docs was written by the sync")]
fn no_shadow_synced(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if output.status.success() && said.contains("docs") && !said.contains("docs_") {
        Ok(())
    } else {
        Err(format!("smugglr exited {} saying {said:?}", output.status))
    }
}

#[given("a place file with a table using AUTOINCREMENT")]
fn autoincrement_place(world: &World) -> Result<(), String> {
    let shape = "CREATE TABLE tags (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT); \
                 CREATE TABLE orders (id TEXT PRIMARY KEY, item TEXT)";
    world.source.set(world.place(
        "source.db",
        &format!(
            "{shape}; INSERT INTO tags (name) VALUES ('t'); \
             INSERT INTO orders VALUES ('o1', 'tent')"
        ),
    )?);
    world.target.set(world.place("target.db", shape)?);
    Ok(())
}

#[when("the agent runs smugglr sync on it")]
fn sync_on_it(world: &World) -> Result<(), String> {
    world.sync(&[]).map(|_| ())
}

#[then("the sync stops before anything is written")]
fn stopped_unwritten(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let (_, target) = places(world)?;
    let held = rows_of(
        &target,
        "SELECT (SELECT count(*) FROM tags) + (SELECT count(*) FROM orders)",
    )?;
    if !output.status.success() && held == vec![vec!["Integer(0)".to_owned()]] {
        Ok(())
    } else {
        Err(format!(
            "smugglr exited {} and the target holds {held:?}",
            output.status
        ))
    }
}

#[then("the output names the table")]
fn names_the_table(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = String::from_utf8_lossy(&output.stderr);
    if said.contains("tags") && said.contains("AUTOINCREMENT") {
        Ok(())
    } else {
        Err(format!("smugglr said {said:?}"))
    }
}
