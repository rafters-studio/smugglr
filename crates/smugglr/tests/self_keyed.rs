//! A table keyed INTEGER PRIMARY KEY through the real binary on real files
//! (FR-SMUGGLR-069): the record and the exit code a caller reads.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::Connection;

static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Pair(PathBuf);

impl Pair {
    /// Two files with one `sessions` table keyed by the database, in step.
    fn new() -> Self {
        let folder = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "self-keyed-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&folder);
        assert!(std::fs::create_dir_all(folder.join("config")).is_ok());
        let pair = Self(folder);
        for db in ["a.db", "b.db"] {
            pair.sql(
                db,
                "CREATE TABLE sessions (id INTEGER PRIMARY KEY, user TEXT, updated TEXT)",
            );
        }
        pair.sql("a.db", "INSERT INTO sessions VALUES (1, 'ada', '1')");
        assert_eq!(pair.run(&[]).status.code(), Some(0), "the first sync");
        pair
    }

    fn sql(&self, db: &str, sql: &str) {
        let connection = Connection::open(self.0.join(db));
        assert!(
            connection.is_ok_and(|c| c.execute_batch(sql).is_ok()),
            "{sql}"
        );
    }

    fn rows(&self, db: &str) -> Vec<String> {
        let Ok(connection) = Connection::open(self.0.join(db)) else {
            return Vec::new();
        };
        let Ok(mut statement) =
            connection.prepare("SELECT id || ':' || user FROM sessions ORDER BY id")
        else {
            return Vec::new();
        };
        statement
            .query_map([], |row| row.get(0))
            .map(|rows| rows.filter_map(Result::ok).collect())
            .unwrap_or_default()
    }

    fn run(&self, flags: &[&str]) -> Output {
        let output = Command::new(env!("CARGO_BIN_EXE_smugglr"))
            .args(["sync", "a.db", "b.db"])
            .args(flags)
            .current_dir(&self.0)
            .env("XDG_CONFIG_HOME", self.0.join("config"))
            .output();
        assert!(output.is_ok(), "smugglr did not run");
        output.unwrap_or_else(|_| unreachable!())
    }
}

fn said(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn one_writer_moves_and_the_record_names_the_table_once() {
    let pair = Pair::new();
    pair.sql("a.db", "INSERT INTO sessions VALUES (2, 'grace', '1')");
    let output = pair.run(&[]);
    let text = said(&output);
    assert_eq!(output.status.code(), Some(0), "{text}");
    assert_eq!(pair.rows("b.db"), ["1:ada", "2:grace"]);
    assert_eq!(
        text.matches("hands out its own keys: sessions").count(),
        1,
        "{text}"
    );
    assert!(text.contains("docs/integer-keys.md"), "{text}");
}

#[test]
fn a_key_both_places_handed_out_stops_on_that_row_and_changes_nothing() {
    let pair = Pair::new();
    pair.sql("a.db", "INSERT INTO sessions VALUES (2, 'grace', '1')");
    pair.sql("b.db", "INSERT INTO sessions VALUES (2, 'linus', '1')");
    let output = pair.run(&[]);
    let text = said(&output);
    assert_eq!(output.status.code(), Some(3), "{text}");
    assert!(text.contains("Both places handed out these keys"), "{text}");
    assert_eq!(pair.rows("a.db"), ["1:ada", "2:grace"]);
    assert_eq!(pair.rows("b.db"), ["1:ada", "2:linus"]);
}

#[test]
fn under_last_the_table_stops_and_says_why() {
    let pair = Pair::new();
    let settings = pair.0.join("config/smugglr");
    assert!(std::fs::create_dir_all(&settings).is_ok());
    assert!(std::fs::write(settings.join("config.toml"), "steering = \"updated\"\n").is_ok());
    pair.sql("a.db", "INSERT INTO sessions VALUES (2, 'grace', '2')");
    let output = pair.run(&["--sync-mode", "last"]);
    let text = said(&output);
    assert!(
        text.contains("two places would hand out the same key"),
        "{text}"
    );
    assert_eq!(pair.rows("b.db"), ["1:ada"]);
}
