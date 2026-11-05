//! What `smugglr sync --migratr` exits with, read from the binary: a caller
//! reads this, and a stop must never look like success.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::Connection;

const STOPPED: i32 = 3;
const NOT_DONE: i32 = 4;
const TOO_MUCH: i32 = 5;

const ADD_TAG: &str = r#"{"up": [{"op": "add_column", "table": "notes",
    "column": {"name": "tag", "type": "TEXT"}}]}"#;

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Pair(PathBuf);

impl Pair {
    /// A local database and a place in step, with a migration not yet run.
    fn new() -> Self {
        let pair = Self::apart();
        assert_eq!(pair.run(&[]), Some(0), "the first sync");
        pair
    }

    /// The same, before any sync.
    fn apart() -> Self {
        let folder = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "migratr-exit-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&folder);
        for dir in ["migrations", "there", "config"] {
            assert!(std::fs::create_dir_all(folder.join(dir)).is_ok());
        }
        assert!(
            std::fs::write(
                folder.join("migrations/20260201000000_add_tag_to_notes.json"),
                ADD_TAG
            )
            .is_ok()
        );
        let pair = Self(folder);
        for db in ["local.db", "there/place.db"] {
            pair.sql(db, "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)");
        }
        pair.sql("local.db", "INSERT INTO notes VALUES ('n1', 'one')");
        pair
    }

    fn sql(&self, db: &str, sql: &str) {
        let connection = Connection::open(self.0.join(db));
        assert!(
            connection.is_ok_and(|c| c.execute_batch(sql).is_ok()),
            "{sql}"
        );
    }

    /// `smugglr sync local place` with `flags`; its exit code.
    fn run(&self, flags: &[&str]) -> Option<i32> {
        Command::new(env!("CARGO_BIN_EXE_smugglr"))
            .args(["sync", "local.db", "there/place.db"])
            .args(flags)
            .current_dir(&self.0)
            .env("XDG_CONFIG_HOME", self.0.join("config"))
            .output()
            .ok()?
            .status
            .code()
    }
}

const MIGRATR: &[&str] = &["--migratr", "migrations"];

#[test]
fn a_run_that_stops_for_places_not_in_step_does_not_exit_success() {
    let pair = Pair::new();
    pair.sql("local.db", "UPDATE notes SET body = 'here'");
    pair.sql("there/place.db", "UPDATE notes SET body = 'there'");
    assert_eq!(pair.run(MIGRATR), Some(STOPPED));
}

#[test]
fn a_place_with_no_ledger_and_another_shape_does_not_exit_success() {
    let pair = Pair::new();
    assert_eq!(pair.run(MIGRATR), Some(NOT_DONE), "the first run");
    pair.sql("there/place.db", "ALTER TABLE notes ADD COLUMN other TEXT");
    assert_eq!(pair.run(MIGRATR), Some(STOPPED));
}

#[test]
fn a_first_run_exits_not_done_and_a_carried_run_exits_success() {
    let pair = Pair::new();
    assert_eq!(pair.run(MIGRATR), Some(NOT_DONE), "stopped for its check");
    assert_eq!(pair.run(MIGRATR), Some(0));
}

#[test]
fn a_final_sync_not_in_step_does_not_exit_success() {
    let pair = Pair::new();
    assert_eq!(pair.run(MIGRATR), Some(NOT_DONE), "the first run");
    pair.sql("local.db", "UPDATE notes SET body = 'here'");
    pair.sql("there/place.db", "UPDATE notes SET body = 'there'");
    assert_eq!(pair.run(MIGRATR), Some(NOT_DONE));
}

#[test]
fn a_dry_run_showing_a_stop_does_not_exit_success() {
    let pair = Pair::new();
    assert_eq!(pair.run(MIGRATR), Some(NOT_DONE), "the first run");
    assert_eq!(pair.run(MIGRATR), Some(0), "carried");
    pair.sql("local.db", "UPDATE notes SET body = 'here'");
    pair.sql("there/place.db", "UPDATE notes SET body = 'there'");
    let mut flags = MIGRATR.to_vec();
    flags.push("--dryrun");
    assert_eq!(pair.run(&flags), Some(STOPPED));
}

#[test]
fn a_run_too_much_for_the_wire_exits_as_a_plain_sync_does() {
    let pair = Pair::apart();
    let settings = pair.0.join("config/smugglr");
    assert!(std::fs::create_dir_all(&settings).is_ok());
    assert!(std::fs::write(settings.join("config.toml"), "max_wire = 1\n").is_ok());
    assert_eq!(pair.run(MIGRATR), Some(TOO_MUCH));
}
