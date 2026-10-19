//! Steps for smugglr sync on the command line.

use rstest_bdd_macros::{given, then, when};

use crate::setup::{NOTES, World};

#[given("a local file and a postgres:// address")]
fn file_and_postgres(world: &World) -> Result<(), String> {
    world.source.set(world.place("shop.db", NOTES)?);
    world
        .target
        .set("postgres://db.example.com/shop".to_owned());
    Ok(())
}

#[when("the agent runs smugglr sync on them")]
fn agent_syncs(world: &World) -> Result<(), String> {
    world.sync(&[]).map(|_| ())
}

#[then("smugglr refuses, saying smugglr keeps SQLite in step with SQLite")]
fn refuses_other_engine(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() && said.contains("smugglr keeps SQLite in step with SQLite") {
        Ok(())
    } else {
        Err(format!("smugglr exited {} saying {said:?}", output.status))
    }
}

/// The orders table of the record-as-text scenario.
const ORDERS: &str = "CREATE TABLE orders (id TEXT PRIMARY KEY, item TEXT)";

#[given("two places where the source's orders table has rows the target lacks")]
fn source_orders_has_rows(world: &World) -> Result<(), String> {
    world.source.set(world.place(
        "source.db",
        &format!("{ORDERS}; INSERT INTO orders VALUES ('o1', 'tent'), ('o2', 'stove')"),
    )?);
    world.target.set(world.place("target.db", ORDERS)?);
    Ok(())
}

#[when("the agent runs smugglr sync on them without --json")]
fn sync_as_text(world: &World) -> Result<(), String> {
    world.sync(&[]).map(|_| ())
}

#[then("the output names orders with its status and counts in text")]
fn record_as_text(world: &World) -> Result<(), String> {
    let output = world.output.get().ok_or("smugglr was not run")?;
    let said = String::from_utf8_lossy(&output.stdout);
    let line = said
        .lines()
        .find(|line| line.trim_start().starts_with("orders"));
    match line {
        Some(line)
            if !said.trim_start().starts_with('{')
                && line.contains("moved")
                && line.contains("2 to target")
                && line.contains("0 to source") =>
        {
            Ok(())
        }
        _ => Err(format!("smugglr printed {said:?}")),
    }
}
