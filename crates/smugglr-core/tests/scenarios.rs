//! Acceptance-scenario runner for the `smugglr-core` crate.
//!
//! `scenarios!` discovers every `.feature` file under `tests/` and emits one
//! test per scenario. Step definitions must compile into this binary.

use std::fmt::Write as _;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, scenarios, then, when};
use smugglr_core::row_hash::row_hash;

/// Rows as SQLite's `quote()` text of each column, and the hashes taken.
#[derive(Debug, Default, ScenarioState)]
struct Rows {
    rows: Slot<Vec<Vec<String>>>,
    hashes: Slot<Vec<String>>,
}

#[fixture]
fn rows() -> Rows {
    Rows::default()
}

fn hex(hash: [u8; 16]) -> String {
    hash.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

fn native_hash(row: &[String]) -> String {
    let columns: Vec<&str> = row.iter().map(String::as_str).collect();
    hex(row_hash(&columns))
}

/// A program that prints the row hash of the row whose columns' `quote()`
/// text are its arguments, as lowercase hex.
const WASM_MAIN: &str = r#"fn main() {
    let columns: Vec<String> = std::env::args().skip(1).collect();
    let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
    for byte in smugglr_core::row_hash::row_hash(&columns) {
        print!("{byte:02x}");
    }
    println!();
}
"#;

fn write(path: &Path, text: &str) -> Result<(), String> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|err| format!("{}: {err}", dir.display()))?;
    }
    fs::write(path, text).map_err(|err| format!("{}: {err}", path.display()))
}

/// Builds smugglr-core into a small program for wasm32-wasip1 and returns the
/// module. The program is its own package, so smugglr-core is built as a
/// plain dependency, without its dev-dependencies.
fn wasm_engine() -> Result<PathBuf, String> {
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join("row-hash-wasm");
    let core = Path::new(env!("CARGO_MANIFEST_DIR"));
    write(
        &root.join("Cargo.toml"),
        &format!(
            "[package]\nname = \"row-hash-wasm\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n\
             [dependencies]\nsmugglr-core = {{ path = {:?} }}\n\n[workspace]\n",
            core.display().to_string()
        ),
    )?;
    write(&root.join("src/main.rs"), WASM_MAIN)?;
    // Pin the workspace's resolved versions.
    fs::copy(core.join("../../Cargo.lock"), root.join("Cargo.lock"))
        .map_err(|err| format!("copying Cargo.lock: {err}"))?;
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let output = Command::new(cargo)
        .current_dir(&root)
        .args(["build", "--target", "wasm32-wasip1"])
        .output()
        .map_err(|err| format!("failed to start cargo: {err}"))?;
    if !output.status.success() {
        return Err(format!(
            "smugglr-core did not build for wasm32-wasip1 (is the target installed? \
             `rustup target add wasm32-wasip1`):\n{}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(root.join("target/wasm32-wasip1/debug/row-hash-wasm.wasm"))
}

/// Runs the WASM engine under node's `node:wasi` and returns its hash.
fn wasm_hash(module: &Path, row: &[String]) -> Result<String, String> {
    const RUN: &str = "const { readFileSync } = require('node:fs');
const { WASI } = require('node:wasi');
const [module, ...columns] = process.argv.slice(1);
const wasi = new WASI({ version: 'preview1', args: ['row_hash', ...columns] });
WebAssembly.instantiate(readFileSync(module), wasi.getImportObject())
  .then(({ instance }) => { process.exitCode = wasi.start(instance); });";
    let output = Command::new("node")
        .arg("-e")
        .arg(RUN)
        .arg(module)
        .args(row)
        .output()
        .map_err(|err| {
            if err.kind() == ErrorKind::NotFound {
                "node is missing: the WASM engine runs under node's node:wasi, \
                 and no `node` was found on PATH"
                    .to_owned()
            } else {
                format!("failed to start node: {err}")
            }
        })?;
    if !output.status.success() {
        return Err(format!(
            "the WASM engine failed under node ({}):\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[given("a row holding the integer 9007199254740993")]
fn a_big_integer(rows: &Rows) {
    rows.rows.set(vec![vec!["9007199254740993".to_owned()]]);
}

#[given(
    "three rows alike except one column, holding NULL in the first, empty text in the second and an empty blob in the third"
)]
fn null_text_blob(rows: &Rows) {
    let row = |column: &str| vec!["1".to_owned(), column.to_owned()];
    rows.rows.set(vec![row("NULL"), row("''"), row("X''")]);
}

#[when("the native engine and the WASM engine each hash the row")]
fn native_and_wasm(rows: &Rows) -> Result<(), String> {
    let all = rows.rows.take().ok_or("no row was given")?;
    let row = all.first().ok_or("no row was given")?;
    let module = wasm_engine()?;
    rows.hashes
        .set(vec![native_hash(row), wasm_hash(&module, row)?]);
    Ok(())
}

#[when("the engine hashes the three rows")]
fn hash_three(rows: &Rows) -> Result<(), String> {
    let all = rows.rows.take().ok_or("no rows were given")?;
    rows.hashes
        .set(all.iter().map(|row| native_hash(row)).collect());
    Ok(())
}

#[then("the two hashes are identical")]
fn identical(rows: &Rows) -> Result<(), String> {
    let hashes = rows.hashes.take().ok_or("the row was not hashed")?;
    match hashes.as_slice() {
        [native, wasm] if native == wasm => Ok(()),
        [native, wasm] => Err(format!("native {native} differs from WASM {wasm}")),
        _ => Err(format!("expected two hashes, got {hashes:?}")),
    }
}

#[then("the three hashes all differ")]
fn all_differ(rows: &Rows) -> Result<(), String> {
    let hashes = rows.hashes.take().ok_or("the rows were not hashed")?;
    match hashes.as_slice() {
        [a, b, c] if a != b && b != c && a != c => Ok(()),
        _ => Err(format!("expected three different hashes, got {hashes:?}")),
    }
}

scenarios!("tests", fixtures = [rows: Rows]);
