//! Regression test for #191: `watch --json` must emit exactly ONE JSON record
//! on a fatal error, not two.
//!
//! Before the fix, a fatal (non-transient) error on a watch tick in JSON mode
//! printed a `WatchTickOutput` error line from the tick handler AND then
//! returned `Err`, which made `main` print a SECOND, differently-shaped
//! `ErrorOutput` line for the same failure. The fix (crates/smugglr/src/watch.rs)
//! prints the single `WatchTickOutput` error record and exits directly with the
//! error's code, so downstream JSON consumers see one record per failure.
//!
//! This test drives the real binary at a config whose local/target databases
//! are not valid SQLite files. The two `LocalDb::open` calls succeed (the files
//! exist and SQLite opens lazily), so the failure surfaces from `sync_all` on
//! the first tick -- exactly the fatal-in-tick path #191 is about, which is why
//! the emitted record is a `WatchTickOutput` (it carries a `tick` field) rather
//! than an `ErrorOutput`.

use std::fs;
use std::process::Command;

use serde_json::Value;

#[test]
fn watch_json_fatal_error_emits_single_record() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let local_db = dir.path().join("local.db");
    let target_db = dir.path().join("target.db");
    let config_path = dir.path().join("config.toml");

    // Files exist so `open_with_flags(READ_WRITE)` succeeds, but their contents
    // are not a valid SQLite database, so the first `list_tables()` inside
    // `sync_all` fails fatally on tick #1.
    fs::write(&local_db, b"not a sqlite database").expect("write local db");
    fs::write(&target_db, b"not a sqlite database").expect("write target db");

    let config = format!(
        "local_db = {local:?}\n\n[target]\ntype = \"sqlite\"\ndatabase = {target:?}\n",
        local = local_db.to_str().expect("utf8 path"),
        target = target_db.to_str().expect("utf8 path"),
    );
    fs::write(&config_path, config).expect("write config");

    let output = Command::new(env!("CARGO_BIN_EXE_smugglr"))
        .arg("--config")
        .arg(&config_path)
        .arg("--output")
        .arg("json")
        .arg("watch")
        .arg("--interval")
        .arg("1")
        .output()
        .expect("run smugglr");

    // The fatal error must terminate the daemon with a non-zero code.
    assert!(
        !output.status.success(),
        "expected non-zero exit, got {:?}",
        output.status
    );

    // stdout carries only JSON (tracing logs go to stderr). #191: there must be
    // exactly one record. Pre-fix this had two lines (WatchTickOutput +
    // ErrorOutput); post-fix it has one.
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(
        lines.len(),
        1,
        "expected exactly one JSON record on stdout, got {}:\n{stdout}",
        lines.len()
    );

    // The single record is the tick-error `WatchTickOutput`: it carries a `tick`
    // field and reports the error status. The pre-fix second line was an
    // `ErrorOutput` (no `tick` field), so requiring the sole line to be a
    // WatchTickOutput also proves we exercised the fatal-in-tick path.
    let record: Value = serde_json::from_str(lines[0]).expect("stdout line is JSON");
    assert_eq!(record["command"], "watch");
    assert_eq!(record["status"], "error");
    assert!(
        record.get("tick").and_then(Value::as_u64).is_some(),
        "single record must be a WatchTickOutput (has `tick`), got: {}",
        lines[0]
    );
    assert!(
        record.get("error").and_then(Value::as_str).is_some(),
        "record must carry an error message, got: {}",
        lines[0]
    );
}
