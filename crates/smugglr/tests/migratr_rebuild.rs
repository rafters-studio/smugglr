//! What a `--migratr` rebuild must not drop, and the order it drops in, on
//! places that are SQLite files with foreign keys on. The requirement's
//! scenarios are in `migratr.feature`; these hold cases it leaves to the
//! build.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use migratr::Executor;
use rusqlite::Connection;
use smugglr::migratr::{Error, Finish, Outcome, Request, carry};
use smugglr::{Options, sync};
use smugglr_place::LocalFile;

const VERSION: &str = "20260201000000";

/// A file a rebuild drops: its UNIQUE `code` goes.
const DROP_CODE: &str = r#"{"up": [{"op": "drop_column", "table": "parent",
    "column": {"name": "code", "type": "TEXT"}}]}"#;

const PARENT: &str = "CREATE TABLE parent (id TEXT PRIMARY KEY, code TEXT UNIQUE); \
     INSERT INTO parent VALUES ('p1', 'a'), ('p2', 'b');";

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// A local database and a place, both running `sql` and in step, with the
/// migrations `files` in a directory.
struct Pair {
    folder: PathBuf,
    /// Tables the config ignores.
    ignore: Vec<String>,
}

impl Pair {
    fn new(sql: &str, files: &[(&str, &str)]) -> Self {
        let folder = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "migratr-rebuild-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&folder);
        let pair = Self {
            folder,
            ignore: Vec::new(),
        };
        assert!(std::fs::create_dir_all(pair.folder.join("migrations")).is_ok());
        // Apart, as a database and a place are: migratr names the snapshot it
        // takes of each by the second, in a folder beside the database.
        assert!(std::fs::create_dir_all(pair.folder.join("there")).is_ok());
        for name in ["local.db", "there/place.db"] {
            let connection = Connection::open(pair.folder.join(name));
            assert!(
                connection.is_ok_and(|c| c.execute_batch(sql).is_ok()),
                "{sql}"
            );
        }
        for (file, text) in files {
            assert!(std::fs::write(pair.folder.join("migrations").join(file), text).is_ok());
        }
        let synced = LocalFile::open(pair.folder.join("local.db")).and_then(|mut local| {
            let mut place = LocalFile::open(pair.folder.join("there/place.db"))?;
            Ok(sync(&mut local, &mut place, &pair.options()).is_ok())
        });
        assert!(matches!(synced, Ok(true)));
        pair
    }

    fn options(&self) -> Options {
        Options {
            config_dir: Some(self.folder.join("config")),
            ignore: self.ignore.clone(),
            ..Options::default()
        }
    }

    /// The place gains a table only it holds, with rows, that the config
    /// ignores.
    fn ignoring(mut self, sql: &str, table: &str) -> Self {
        let connection = Connection::open(self.path("there/place.db"));
        assert!(
            connection.is_ok_and(|c| c.execute_batch(sql).is_ok()),
            "{sql}"
        );
        self.ignore.push(table.to_owned());
        self
    }

    fn path(&self, name: &str) -> String {
        self.folder.join(name).display().to_string()
    }

    fn carry(&self) -> Result<Outcome, Error> {
        let request = Request {
            source: self.path("local.db"),
            target: self.path("there/place.db"),
            dir: self.folder.join("migrations"),
            safe: true,
            options: self.options(),
        };
        carry(&request, &|local, name| local.snapshot(name))
    }

    /// The first run, which must stop for its check, and the run after it.
    fn carry_twice(&self) -> Result<Outcome, Error> {
        let first = self.carry().map(|outcome| outcome.finish);
        assert_eq!(first.ok(), Some(Finish::Check));
        self.carry()
    }

    fn count(&self, name: &str, table: &str) -> i64 {
        Connection::open(self.path(name))
            .and_then(|c| {
                c.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
            })
            .unwrap_or(-1)
    }

    /// Every statement that made the place, and whether its ledger exists.
    fn place_schema(&self) -> Vec<String> {
        let connection = Connection::open(self.path("there/place.db"));
        let Ok(connection) = connection else {
            return Vec::new();
        };
        let mut statement =
            connection.prepare("SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name");
        let Ok(statement) = statement.as_mut() else {
            return Vec::new();
        };
        statement
            .query_map([], |row| row.get(0))
            .and_then(Iterator::collect)
            .unwrap_or_default()
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.folder);
    }
}

/// `carry` stopped saying `words`, with the place as `was`.
fn stopped(pair: &Pair, result: Result<Outcome, Error>, words: &[&str], was: &[String]) {
    let text = match result {
        Err(Error::Stop(text)) => text,
        other => format!("expected a stop, got {other:?}"),
    };
    for word in words {
        assert!(text.contains(word), "{word:?} in {text}");
    }
    assert_eq!(pair.place_schema(), was);
}

#[test]
fn a_rebuild_drops_a_restrict_child_before_its_parent() {
    let sql = format!(
        "{PARENT} CREATE TABLE child (id TEXT PRIMARY KEY, \
         parent_id TEXT REFERENCES parent(id) ON DELETE RESTRICT); \
         INSERT INTO child VALUES ('c1', 'p1'), ('c2', 'p2');"
    );
    let pair = Pair::new(&sql, &[(&format!("{VERSION}_drop.json"), DROP_CODE)]);
    let finish = pair.carry_twice().map(|outcome| outcome.finish);
    assert_eq!(finish.map_err(|err| err.to_string()), Ok(Finish::Done));
    assert_eq!(pair.count("there/place.db", "parent"), 2);
    assert_eq!(pair.count("there/place.db", "child"), 2);
    assert_eq!(pair.count("local.db", "child"), 2);
}

#[test]
fn raw_sql_after_a_rebuild_stops_before_anything_is_dropped() {
    let raw = r#"{"up": [{"op": "raw_sql", "up": "CREATE TABLE made (id TEXT PRIMARY KEY)"}]}"#;
    let pair = Pair::new(
        PARENT,
        &[
            (&format!("{VERSION}_drop.json"), DROP_CODE),
            ("20260301000000_raw_later.json", raw),
        ],
    );
    let was = pair.place_schema();
    stopped(
        &pair,
        pair.carry_twice(),
        &["20260301000000_raw_later", "raw SQL"],
        &was,
    );
    assert_eq!(pair.count("there/place.db", "parent"), 2);
}

#[test]
fn raw_sql_in_the_rebuilding_migration_stops_before_anything_is_dropped() {
    let both = r#"{"up": [
        {"op": "drop_column", "table": "parent", "column": {"name": "code", "type": "TEXT"}},
        {"op": "raw_sql", "up": "CREATE TABLE made (id TEXT PRIMARY KEY)"}]}"#;
    let pair = Pair::new(PARENT, &[(&format!("{VERSION}_both.json"), both)]);
    let was = pair.place_schema();
    stopped(&pair, pair.carry_twice(), &[VERSION, "raw SQL"], &was);
}

#[test]
fn raw_sql_alone_still_runs_on_the_place() {
    let raw = r#"{"up": [{"op": "raw_sql", "up": "CREATE TABLE made (id TEXT PRIMARY KEY)"}]}"#;
    let pair = Pair::new(PARENT, &[(&format!("{VERSION}_raw.json"), raw)]);
    let finish = pair.carry_twice().map(|outcome| outcome.finish);
    assert_eq!(finish.map_err(|err| err.to_string()), Ok(Finish::Done));
    assert_eq!(pair.count("there/place.db", "made"), 0);
}

#[test]
fn a_table_that_references_itself_stops_before_anything_is_dropped() {
    let sql = "CREATE TABLE parent (id TEXT PRIMARY KEY, code TEXT UNIQUE, \
         boss TEXT REFERENCES parent(id)); \
         INSERT INTO parent VALUES ('p1', 'a', NULL), ('p2', 'b', 'p1');";
    let pair = Pair::new(sql, &[(&format!("{VERSION}_drop.json"), DROP_CODE)]);
    let was = pair.place_schema();
    stopped(&pair, pair.carry_twice(), &["would drop parent"], &was);
    assert_eq!(pair.count("there/place.db", "parent"), 2);
}

#[test]
fn a_foreign_key_cycle_stops_before_anything_is_dropped() {
    let sql = "CREATE TABLE parent (id TEXT PRIMARY KEY, code TEXT UNIQUE, \
         last TEXT REFERENCES child(id)); \
         CREATE TABLE child (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES parent(id)); \
         INSERT INTO parent VALUES ('p1', 'a', NULL); INSERT INTO child VALUES ('c1', 'p1');";
    let pair = Pair::new(sql, &[(&format!("{VERSION}_drop.json"), DROP_CODE)]);
    let was = pair.place_schema();
    stopped(&pair, pair.carry_twice(), &["would drop"], &was);
    assert_eq!(pair.count("there/place.db", "child"), 1);
}

/// A migration that adds a column, needing no rebuild.
const ADD_TAG: &str = r#"{"up": [{"op": "add_column", "table": "parent",
    "column": {"name": "tag", "type": "TEXT"}}]}"#;

const KEPT_AWAY: &str = "CREATE TABLE kept_away (id TEXT PRIMARY KEY, v TEXT); \
     INSERT INTO kept_away VALUES ('k1', 'one'), ('k2', 'two');";

#[test]
fn a_place_holding_an_ignored_table_is_matched_and_carried_without_it() {
    let pair = Pair::new(PARENT, &[(&format!("{VERSION}_add_tag.json"), ADD_TAG)])
        .ignoring(KEPT_AWAY, "kept_away");
    let finish = pair.carry_twice().map(|outcome| outcome.finish);
    assert_eq!(finish.map_err(|err| err.to_string()), Ok(Finish::Done));
    assert!(
        pair.place_schema()
            .iter()
            .any(|sql| sql.contains("\"tag\""))
    );
    assert_eq!(pair.count("there/place.db", "kept_away"), 2);
}

#[test]
fn under_safe_a_change_to_an_ignored_table_does_not_stop_a_carry() {
    let pair = Pair::new(PARENT, &[(&format!("{VERSION}_add_tag.json"), ADD_TAG)])
        .ignoring(KEPT_AWAY, "kept_away");
    assert_eq!(
        pair.carry().map(|outcome| outcome.finish).ok(),
        Some(Finish::Check)
    );
    let changed = Connection::open(pair.path("there/place.db"))
        .and_then(|c| c.execute_batch("UPDATE kept_away SET v = 'changed' WHERE id = 'k1'"));
    assert!(changed.is_ok());
    let finish = pair.carry().map(|outcome| outcome.finish);
    assert_eq!(finish.map_err(|err| err.to_string()), Ok(Finish::Done));
}

#[test]
fn a_rebuild_whose_table_an_ignored_table_references_stops_before_anything_is_dropped() {
    let pair = Pair::new(PARENT, &[(&format!("{VERSION}_drop.json"), DROP_CODE)]).ignoring(
        "CREATE TABLE kept_away (id TEXT PRIMARY KEY, p TEXT REFERENCES parent(id)); \
         INSERT INTO kept_away VALUES ('k1', 'p1');",
        "kept_away",
    );
    assert_eq!(
        pair.carry().map(|outcome| outcome.finish).ok(),
        Some(Finish::Check)
    );
    let was = pair.place_schema();
    stopped(&pair, pair.carry(), &["kept_away", "ignore"], &was);
    assert_eq!(pair.count("there/place.db", "kept_away"), 1);
}
