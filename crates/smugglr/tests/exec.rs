//! `smugglr exec`'s refusals beyond its scenarios' cases, and a place ledger
//! from before the hash columns, read from the binary.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::Connection;
use serde_json::Value as Json;
use sha2::{Digest, Sha256};

const NOTES: &str = "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)";
const UP: &str = "ALTER TABLE notes ADD COLUMN tag TEXT";

static NEXT: AtomicUsize = AtomicUsize::new(0);

fn file(name: &str, sql: &str) -> Result<String, String> {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "exec-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
    let path = dir.join(name).display().to_string();
    Connection::open(&path)
        .and_then(|connection| connection.execute_batch(sql))
        .map_err(|err| format!("{sql}: {err}"))?;
    Ok(path)
}

fn hash(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

/// `smugglr exec` with `args`, `batch` on stdin: its exit code and what it
/// printed.
fn exec(args: &[&str], batch: &[&str]) -> Result<(Option<i32>, Json), String> {
    let error = |err: std::io::Error| err.to_string();
    let mut child = Command::new(env!("CARGO_BIN_EXE_smugglr"))
        .arg("exec")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(error)?;
    let input = serde_json::to_string(batch).map_err(|err| err.to_string())?;
    child
        .stdin
        .take()
        .ok_or("no stdin")?
        .write_all(input.as_bytes())
        .map_err(error)?;
    let output = child.wait_with_output().map_err(error)?;
    let said = serde_json::from_slice(&output.stdout).unwrap_or(Json::Null);
    Ok((output.status.code(), said))
}

/// A local database that ran `UP` as version 1 with `up_hash`, and a place
/// file holding `place`; with the place's fingerprint.
fn pair(up_hash: &str, place: &str) -> Result<(String, String, String), String> {
    let local = file(
        "local.db",
        &format!(
            "{NOTES}; {UP}; CREATE TABLE _migratr_migrations (version INTEGER PRIMARY KEY, \
             name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL, \
             up_hash TEXT, down_hash TEXT); \
             INSERT INTO _migratr_migrations VALUES (1, 'tag', 'c', 'now', {up_hash}, NULL)"
        ),
    )?;
    let place = file("place.db", place)?;
    let (_, read) = exec(&[&place, "--read"], &[])?;
    let print = read["fingerprint"].as_str().ok_or("no fingerprint")?;
    Ok((local, place, print.to_owned()))
}

/// An up of `version` from the local database to the place: its exit code
/// and what was refused.
fn up(
    (local, place, print): &(String, String, String),
    version: &str,
    batch: &[&str],
) -> Result<(Option<i32>, Option<String>), String> {
    let args = [
        local,
        place,
        "--version",
        version,
        "--up",
        "--expect",
        print,
    ];
    let (code, said) = exec(&args, batch)?;
    Ok((code, said["refused"].as_str().map(str::to_owned)))
}

fn sql(path: &str, query: &str) -> Result<Vec<String>, String> {
    let connection = Connection::open(path).map_err(|err| err.to_string())?;
    let mut statement = connection.prepare(query).map_err(|err| err.to_string())?;
    statement
        .query_map([], |row| row.get(0))
        .and_then(Iterator::collect)
        .map_err(|err| err.to_string())
}

fn columns(path: &str) -> Result<Vec<String>, String> {
    sql(path, "SELECT name FROM pragma_table_info('notes')")
}

fn refusal(reason: &str) -> (Option<i32>, Option<String>) {
    (Some(3), Some(reason.to_owned()))
}

#[test]
fn a_statement_added_a_version_not_held_and_a_null_hash_are_refused() -> Result<(), String> {
    let ran = pair(&format!("'{}'", hash(UP)), NOTES)?;
    assert_eq!(up(&ran, "1", &[UP, "DROP TABLE notes"])?, refusal("hash"));
    assert_eq!(up(&ran, "2", &[UP])?, refusal("version"));
    assert_eq!(columns(&ran.1)?, ["id", "body"]);

    let unhashed = pair("NULL", NOTES)?;
    assert_eq!(up(&unhashed, "1", &[UP])?, refusal("null_hash"));
    assert_eq!(columns(&unhashed.1)?, ["id", "body"]);
    Ok(())
}

#[test]
fn a_place_ledger_from_before_the_hashes_gains_them_with_its_row() -> Result<(), String> {
    let before = format!(
        "{NOTES}; CREATE TABLE _migratr_migrations (version INTEGER PRIMARY KEY, \
         name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL)"
    );
    let ran = pair(&format!("'{}'", hash(UP)), &before)?;
    assert_eq!(up(&ran, "1", &[UP])?, (Some(0), None));
    assert_eq!(
        sql(&ran.1, "SELECT up_hash FROM _migratr_migrations")?,
        [hash(UP)]
    );
    Ok(())
}

#[test]
fn the_read_form_takes_nothing_but_read() -> Result<(), String> {
    let place = file("place.db", NOTES)?;
    for args in [
        vec![place.as_str(), "--read", "notes"],
        vec![place.as_str()],
        vec!["--read"],
    ] {
        assert_eq!(exec(&args, &[])?.0, Some(2), "{args:?}");
    }
    Ok(())
}
