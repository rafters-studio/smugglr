//! Acceptance-scenario runner for the `smugglr-core` crate.
//!
//! `scenarios!` discovers every `.feature` file under `tests/` and emits one
//! test per scenario. Step definitions must compile into this binary.

#[path = "steps/envelope.rs"]
mod envelope_steps;
#[path = "steps/ignore.rs"]
mod ignore_steps;
#[path = "steps/plan.rs"]
mod plan_steps;
#[path = "steps/shape.rs"]
mod shape_steps;
#[path = "steps/steering.rs"]
mod steering_steps;

use std::fmt::Write as _;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, scenarios, then, when};
use smugglr_core::change::{Change, Decision, Op, decide};
use smugglr_core::row_hash::row_hash;

use envelope_steps::{Mesh, mesh};
use plan_steps::{Pair, pair};
use shape_steps::{Device, device};

/// Rows as SQLite's `quote()` text of each column, and the hashes taken.
#[derive(Debug, Default, ScenarioState)]
#[expect(
    clippy::struct_field_names,
    reason = "the scenarios' steps name the given rows `rows`"
)]
struct Rows {
    rows: Slot<Vec<Vec<String>>>,
    hashes: Slot<Vec<String>>,
    /// Each row's `quote()` text and hash, as the browser's SQLite build gave them.
    browser: Slot<Vec<(String, String)>>,
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

/// Starts `program`, naming what to install when it is missing.
fn run(command: &mut Command, program: &str, install: &str) -> Result<String, String> {
    let output = command.output().map_err(|err| {
        if err.kind() == ErrorKind::NotFound {
            format!("{program} is missing: no `{program}` was found on PATH; {install}")
        } else {
            format!("failed to start {program}: {err}")
        }
    })?;
    if !output.status.success() {
        return Err(format!(
            "{program} failed ({}):\n{}{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Builds smugglr-web with wasm-pack, then hashes each one-column row in the
/// browser's SQLite build under headless Chromium, through
/// `packages/web/test/row-hash.mjs`. Returns each row's `quote()` text and
/// hash as the browser gave them.
fn browser_hashes(rows: &[Vec<String>]) -> Result<Vec<(String, String)>, String> {
    let web = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packages/web");
    let pnpm = if cfg!(windows) { "pnpm.cmd" } else { "pnpm" };
    run(
        Command::new(pnpm)
            .current_dir(&web)
            .args(["run", "build:wasm"])
            .env(
                "CARGO_TARGET_DIR",
                Path::new(env!("CARGO_TARGET_TMPDIR")).join("smugglr-web"),
            ),
        "pnpm",
        "install pnpm and wasm-pack, then run `pnpm install` at the repo root",
    )?;
    let literals = rows
        .iter()
        .map(|row| match row.as_slice() {
            [column] => Ok(column.as_str()),
            _ => Err(format!("the browser hashes one-column rows, not {row:?}")),
        })
        .collect::<Result<Vec<&str>, String>>()?;
    let stdout = run(
        Command::new("node")
            .current_dir(&web)
            .arg("test/row-hash.mjs")
            .args(literals),
        "node",
        "then run `pnpm exec playwright install chromium-headless-shell` in packages/web",
    )?;
    serde_json::from_str(stdout.trim())
        .map_err(|err| format!("the browser gave no hashes ({err}): {stdout}"))
}

/// One-column rows, as `quote()` text: an integer beyond 2^53, i64 max and
/// min, NULL, empty text and an empty blob.
#[given("a row holding 9007199254740993 and rows holding NULL, empty text and an empty blob")]
fn browser_rows(rows: &Rows) {
    rows.rows.set(
        [
            "9007199254740993",
            "9223372036854775807",
            "-9223372036854775808",
            "NULL",
            "''",
            "X''",
        ]
        .iter()
        .map(|&column| vec![column.to_owned()])
        .collect(),
    );
}

#[when("the browser's SQLite build and the native engine each hash them")]
fn browser_and_native(rows: &Rows) -> Result<(), String> {
    let all = rows.rows.get().ok_or("no rows were given")?;
    rows.browser.set(browser_hashes(&all)?);
    rows.hashes
        .set(all.iter().map(|row| native_hash(row)).collect());
    Ok(())
}

#[then("each row's two hashes are identical")]
fn each_identical(rows: &Rows) -> Result<(), String> {
    let all = rows.rows.take().ok_or("no rows were given")?;
    let native = rows
        .hashes
        .take()
        .ok_or("the native engine hashed nothing")?;
    let browser = rows.browser.take().ok_or("the browser hashed nothing")?;
    if browser.len() != all.len() {
        return Err(format!(
            "{} rows given, the browser hashed {browser:?}",
            all.len()
        ));
    }
    for ((row, native), (quoted, hash)) in all.iter().zip(&native).zip(&browser) {
        if row.as_slice() != [quoted.as_str()] || native != hash {
            return Err(format!(
                "row {row:?}: native {native}, the browser read {quoted} and hashed {hash}"
            ));
        }
    }
    Ok(())
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

#[given("the row whose columns' quote() text is 1 and 'smugglr'")]
fn the_known_row(rows: &Rows) {
    rows.rows
        .set(vec![vec!["1".to_owned(), "'smugglr'".to_owned()]]);
}

/// The row hash of the row `1, 'smugglr'`, computed once from `row_hash` and
/// written here, so a change to the hash's algorithm, its length prefix or
/// its 16-byte length fails.
const KNOWN_HASH: &str = "6198887a4dfed5172bf2a92c7661e7e4";

#[when("the engine hashes the row")]
fn hash_one(rows: &Rows) -> Result<(), String> {
    let all = rows.rows.take().ok_or("no row was given")?;
    let row = all.first().ok_or("no row was given")?;
    rows.hashes.set(vec![native_hash(row)]);
    Ok(())
}

#[then("the hash is the 16-byte value recorded for that row")]
fn known_value(rows: &Rows) -> Result<(), String> {
    let hashes = rows.hashes.take().ok_or("the row was not hashed")?;
    match hashes.as_slice() {
        [hash] if hash == KNOWN_HASH => Ok(()),
        [hash] => Err(format!("the row hashed to {hash}, not {KNOWN_HASH}")),
        _ => Err(format!("expected one hash, got {hashes:?}")),
    }
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

/// A place holding one row, and the change arriving at it.
#[derive(Debug, Default, ScenarioState)]
struct Place {
    /// The row's columns as SQLite's `quote()` text.
    row: Slot<Vec<String>>,
    /// The row as it was before the change arrived.
    before: Slot<Vec<String>>,
    change: Slot<Change>,
    /// How many times the place wrote the row.
    writes: Slot<usize>,
    /// What the place decided about the change.
    decision: Slot<Decision>,
}

#[fixture]
fn place() -> Place {
    Place::default()
}

fn quoted(columns: &[&str]) -> Vec<String> {
    columns.iter().map(|&column| column.to_owned()).collect()
}

fn hash_of(row: &[String]) -> [u8; 16] {
    let columns: Vec<&str> = row.iter().map(String::as_str).collect();
    row_hash(&columns)
}

fn arriving(from: &[String], to: Vec<String>) -> Change {
    Change {
        op: Op::Upsert,
        table: "notes".to_owned(),
        key: quoted(&["1"]),
        from: Some(hash_of(from)),
        to: Some(hash_of(&to)),
        values: to,
    }
}

fn holding(place: &Place, row: Vec<String>, change: Change) {
    place.before.set(row.clone());
    place.row.set(row);
    place.change.set(change);
    place.writes.set(0);
}

#[given("a place holding a row whose hash is the from of an arriving change")]
fn at_from(place: &Place) {
    let row = quoted(&["1", "'draft'"]);
    let change = arriving(&row, quoted(&["1", "'final'"]));
    holding(place, row, change);
}

#[given("a place holding a row whose hash is already the to of an arriving change")]
fn at_to(place: &Place) {
    let row = quoted(&["1", "'final'"]);
    let change = arriving(&quoted(&["1", "'draft'"]), row.clone());
    holding(place, row, change);
}

#[when("the place receives the change")]
fn receive(place: &Place) -> Result<(), String> {
    let change = place.change.get().ok_or("no change arrived")?;
    let row = place.row.get().ok_or("the place holds no row")?;
    let decision = decide(&change, Some(hash_of(&row)), None);
    place.decision.set(decision);
    if decision == Decision::Apply {
        place.row.set(change.values);
        place
            .writes
            .with_mut(|writes| *writes += 1)
            .ok_or("the place keeps no write count")?;
    }
    Ok(())
}

#[then("the change counts as done, not as waiting")]
fn done_not_waiting(place: &Place) -> Result<(), String> {
    match place.decision.get() {
        Some(Decision::Skip) => Ok(()),
        Some(Decision::Hold) => Err("the change was held: it waits, not done".to_owned()),
        other => Err(format!("expected the change skipped, got {other:?}")),
    }
}

#[then("the row holds the change's values")]
fn holds_values(place: &Place) -> Result<(), String> {
    let change = place.change.get().ok_or("no change arrived")?;
    let row = place.row.get().ok_or("the place holds no row")?;
    if row == change.values {
        Ok(())
    } else {
        Err(format!("the row holds {row:?}, not {:?}", change.values))
    }
}

#[then("the row is unchanged")]
fn unchanged(place: &Place) -> Result<(), String> {
    let before = place.before.get().ok_or("the place held no row")?;
    let row = place.row.get().ok_or("the place holds no row")?;
    match place.writes.get() {
        Some(0) if row == before => Ok(()),
        writes => Err(format!(
            "the row went from {before:?} to {row:?} after {writes:?} writes"
        )),
    }
}

scenarios!(
    "tests",
    fixtures = [rows: Rows, place: Place, pair: Pair, device: Device, mesh: Mesh]
);
