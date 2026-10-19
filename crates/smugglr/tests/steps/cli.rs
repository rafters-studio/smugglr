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
