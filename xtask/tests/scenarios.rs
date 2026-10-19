//! Acceptance-scenario runner for the `xtask` crate.
//!
//! `scenarios!` discovers every `.feature` file under `tests/` and emits one
//! test per scenario. Step definitions must compile into this binary.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use cargo_metadata::{Metadata, MetadataCommand};
use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, scenarios, then, when};
use xtask::crate_map::{Violation, check};

#[derive(Debug, Default, ScenarioState)]
struct Check {
    violations: Slot<Vec<Violation>>,
}

#[fixture]
fn state() -> Check {
    Check::default()
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn metadata(root: &Path) -> Result<Metadata, String> {
    MetadataCommand::new()
        .manifest_path(root.join("Cargo.toml"))
        .no_deps()
        .exec()
        .map_err(|err| format!("cargo metadata failed for {}: {err}", root.display()))
}

fn write(path: &Path, text: &str) -> Result<(), String> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|err| format!("{}: {err}", dir.display()))?;
    }
    fs::write(path, text).map_err(|err| format!("{}: {err}", path.display()))
}

fn library(dir: &Path, name: &str, dependencies: &str) -> Result<(), String> {
    write(
        &dir.join("Cargo.toml"),
        &format!(
            "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[dependencies]\n{dependencies}"
        ),
    )?;
    write(&dir.join("src/lib.rs"), "")
}

/// A two-crate workspace in which smugglr-core has one dependency, either on
/// its sibling workspace crate or on an external crate outside the workspace.
fn changed_workspace(dependency: &str, external: bool) -> Result<Metadata, String> {
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("crate-map-{dependency}"));
    let workspace = root.join("workspace");
    write(
        &workspace.join("Cargo.toml"),
        "[workspace]\nresolver = \"3\"\nmembers = [\"smugglr-core\", \"smugglr-memory\"]\n",
    )?;
    let path = if external {
        library(&root.join(dependency), dependency, "")?;
        format!("../../{dependency}")
    } else {
        format!("../{dependency}")
    };
    library(
        &workspace.join("smugglr-core"),
        "smugglr-core",
        &format!("{dependency} = {{ path = \"{path}\" }}\n"),
    )?;
    library(&workspace.join("smugglr-memory"), "smugglr-memory", "")?;
    metadata(&workspace)
}

#[given("the workspace as it is")]
fn the_workspace(state: &Check) -> Result<(), String> {
    state.violations.set(check(&metadata(&workspace_root())?));
    Ok(())
}

#[then("the crate map holds")]
fn the_map_holds(state: &Check) -> Result<(), String> {
    let violations = state.violations.get().ok_or("the map was not checked")?;
    if violations.is_empty() {
        Ok(())
    } else {
        Err(lines(&violations))
    }
}

#[then("smugglr-core depends on no crate that does I/O or reads a clock")]
fn core_is_pure(state: &Check) -> Result<(), String> {
    let violations = state.violations.get().ok_or("the map was not checked")?;
    let impure: Vec<Violation> = violations
        .into_iter()
        .filter(|violation| matches!(violation, Violation::Impure { .. }))
        .collect();
    if impure.is_empty() {
        Ok(())
    } else {
        Err(lines(&impure))
    }
}

#[then("smugglr-core builds for wasm32-unknown-unknown")]
fn core_builds_for_wasm() -> Result<(), String> {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let target_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("wasm32");
    let output = Command::new(cargo)
        .current_dir(workspace_root())
        .args([
            "build",
            "-p",
            "smugglr-core",
            "--target",
            "wasm32-unknown-unknown",
        ])
        .arg("--target-dir")
        .arg(&target_dir)
        .output()
        .map_err(|err| format!("failed to start cargo: {err}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "smugglr-core did not build for wasm32-unknown-unknown:\n{}",
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

#[when("a change makes smugglr-core depend on the workspace crate {dependency}")]
fn depend_on_workspace_crate(state: &Check, dependency: String) -> Result<(), String> {
    state
        .violations
        .set(check(&changed_workspace(&dependency, false)?));
    Ok(())
}

#[when("a change makes smugglr-core depend on the external crate {dependency}")]
fn depend_on_external_crate(state: &Check, dependency: String) -> Result<(), String> {
    state
        .violations
        .set(check(&changed_workspace(&dependency, true)?));
    Ok(())
}

#[then("the crate map check fails naming {from} and {to}")]
fn fails_naming(state: &Check, from: String, to: String) -> Result<(), String> {
    let violations = state.violations.take().ok_or("the map was not checked")?;
    let named = violations.iter().any(|violation| {
        let line = violation.to_string();
        line.contains(&from) && line.contains(&to)
    });
    if named {
        Ok(())
    } else {
        Err(format!(
            "expected a violation naming {from} and {to}, got: {}",
            lines(&violations)
        ))
    }
}

fn lines(violations: &[Violation]) -> String {
    violations
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

scenarios!("tests", fixtures = [state: Check]);
