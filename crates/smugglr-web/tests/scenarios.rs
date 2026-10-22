//! Acceptance-scenario runner for the `smugglr-web` crate.
//!
//! `scenarios!` discovers every `.feature` file under `tests/` and emits one
//! test per scenario. Step definitions must compile into this binary.

#[path = "steps/bridges.rs"]
mod bridges;
#[path = "steps/browser.rs"]
mod browser;
#[path = "steps/standin.rs"]
mod standin;
#[path = "steps/tab.rs"]
mod tab;

use rstest_bdd_macros::scenarios;
use tab::{Web, web};

scenarios!("tests", fixtures = [web: Web]);
