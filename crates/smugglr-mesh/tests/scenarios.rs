//! Acceptance-scenario runner for the `smugglr-mesh` crate.
//!
//! `scenarios!` discovers every `.feature` file under `tests/` and emits one
//! test per scenario. Step definitions must compile into this binary.

#[path = "steps/node.rs"]
mod node;

use node::{Mesh, mesh};
use rstest_bdd_macros::scenarios;

scenarios!("tests", fixtures = [mesh: Mesh]);
