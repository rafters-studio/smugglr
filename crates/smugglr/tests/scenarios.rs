//! Acceptance-scenario runner for the `smugglr` crate.
//!
//! `scenarios!` discovers every `.feature` file under `tests/` and emits one
//! test per scenario. Step definitions must compile into this binary.

#[path = "steps/cli.rs"]
mod cli;
#[path = "steps/http_sql.rs"]
mod http_sql;
#[path = "steps/world.rs"]
mod setup;
#[path = "steps/shape.rs"]
mod shape;
#[path = "steps/sync.rs"]
mod sync;
#[path = "steps/whole.rs"]
mod whole;

use std::process::{Command, Output};

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, scenarios, then, when};
use setup::{World, world};
use whole::{Soak, soak};

#[derive(Debug, Default, ScenarioState)]
struct Run {
    output: Slot<Output>,
}

#[fixture]
fn run() -> Run {
    Run::default()
}

#[when("smugglr is run with no arguments")]
fn run_with_no_arguments(run: &Run) -> Result<(), String> {
    let output = Command::new(env!("CARGO_BIN_EXE_smugglr"))
        .output()
        .map_err(|err| format!("failed to start smugglr: {err}"))?;
    run.output.set(output);
    Ok(())
}

#[then("it prints its name and version")]
fn prints_name_and_version(run: &Run) -> Result<(), String> {
    let output = run.output.take().ok_or("smugglr was not run")?;
    if !output.status.success() {
        return Err(format!("smugglr exited with {}", output.status));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let expected = format!("smugglr {}", env!("CARGO_PKG_VERSION"));
    if stdout.trim() == expected {
        Ok(())
    } else {
        Err(format!("expected {expected:?}, got {:?}", stdout.trim()))
    }
}

scenarios!("tests", fixtures = [run: Run, world: World, soak: Soak]);
