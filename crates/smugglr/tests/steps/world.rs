//! Places as SQLite files in a scenario's own folder, smugglr's config
//! directory beside them, and the CLI run on them.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::ScenarioState;
use rusqlite::Connection;

/// A scenario's folder, its places and what smugglr printed.
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct World {
    folder: Slot<PathBuf>,
    /// The source's address, as the agent names it.
    pub(crate) source: Slot<String>,
    /// The target's address.
    pub(crate) target: Slot<String>,
    /// What the last smugglr run printed.
    pub(crate) output: Slot<Output>,
    /// The key (`id`) of the row the scenario watches.
    pub(crate) watched: Slot<String>,
    /// Two copies of a pair of places, each as (source, target).
    pub(crate) copies: Slot<Vec<(String, String)>>,
    /// What each copy's sync printed.
    pub(crate) records: Slot<Vec<String>>,
    /// Each line a watching smugglr printed, with the seconds since it
    /// started.
    pub(crate) lines: Slot<Vec<(f64, String)>>,
}

#[fixture]
pub(crate) fn world() -> World {
    World::default()
}

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// Three notes rows.
pub(crate) const THREE_NOTES: &str = "INSERT INTO notes VALUES ('n1', 'first', 1), ('n2', 'second', 2), \
     ('n3', 'third', 3)";

/// The orders table some scenarios' places hold.
pub(crate) const ORDERS: &str = "CREATE TABLE orders (id TEXT PRIMARY KEY, item TEXT)";

/// The notes table every scenario's places hold.
pub(crate) const NOTES: &str =
    "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT, created_at INTEGER)";

impl World {
    /// The scenario's own folder, made on first use.
    pub(crate) fn folder(&self) -> Result<PathBuf, String> {
        if let Some(folder) = self.folder.get() {
            return Ok(folder);
        }
        let folder = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "scenario-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&folder);
        std::fs::create_dir_all(&folder).map_err(|err| format!("{}: {err}", folder.display()))?;
        self.folder.set(folder.clone());
        Ok(folder)
    }

    /// The `XDG_CONFIG_HOME` smugglr runs under.
    pub(crate) fn xdg(&self) -> Result<PathBuf, String> {
        Ok(self.folder()?.join("config"))
    }

    /// smugglr's config directory.
    pub(crate) fn config(&self) -> Result<PathBuf, String> {
        Ok(self.xdg()?.join("smugglr"))
    }

    /// Makes the SQLite file `name` running `sql`, and returns its path.
    pub(crate) fn place(&self, name: &str, sql: &str) -> Result<String, String> {
        let path = self.folder()?.join(name);
        sql_on(&path.display().to_string(), sql)?;
        Ok(path.display().to_string())
    }

    /// Runs smugglr with `args`.
    pub(crate) fn smugglr(&self, args: &[&str]) -> Result<Output, String> {
        let output = Command::new(env!("CARGO_BIN_EXE_smugglr"))
            .args(args)
            .env("XDG_CONFIG_HOME", self.xdg()?)
            .output()
            .map_err(|err| format!("failed to start smugglr: {err}"))?;
        self.output.set(output.clone());
        Ok(output)
    }

    /// Runs `smugglr sync <source> <target>` followed by `flags`.
    pub(crate) fn sync(&self, flags: &[&str]) -> Result<Output, String> {
        let source = self.source.get().ok_or("no source named")?;
        let target = self.target.get().ok_or("no target named")?;
        let mut args = vec!["sync", source.as_str(), target.as_str()];
        args.extend_from_slice(flags);
        self.smugglr(&args)
    }

    /// Runs the sync and fails unless smugglr exits 0.
    pub(crate) fn sync_ok(&self) -> Result<(), String> {
        let output = self.sync(&[])?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "smugglr sync exited {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            ))
        }
    }
}

fn open(path: &str) -> Result<Connection, String> {
    Connection::open(path).map_err(|err| format!("{path}: {err}"))
}

/// Runs `sql` on the SQLite file at `path`.
pub(crate) fn sql_on(path: &str, sql: &str) -> Result<(), String> {
    open(path)?
        .execute_batch(sql)
        .map_err(|err| format!("{path}: {err}"))
}

/// Each row of `query` on the file at `path`, every column as text.
pub(crate) fn rows_of(path: &str, query: &str) -> Result<Vec<Vec<String>>, String> {
    let connection = open(path)?;
    let mut statement = connection
        .prepare(query)
        .map_err(|err| format!("{path}: {err}"))?;
    let width = statement.column_count();
    statement
        .query_map([], |row| {
            (0..width)
                .map(|i| {
                    row.get::<_, rusqlite::types::Value>(i)
                        .map(|value| format!("{value:?}"))
                })
                .collect::<rusqlite::Result<Vec<String>>>()
        })
        .and_then(Iterator::collect)
        .map_err(|err| format!("{path}: {err}"))
}

/// The notes row `id` in the file at `path`, if any.
pub(crate) fn note(path: &str, id: &str) -> Result<Option<Vec<String>>, String> {
    let id = id.replace('\'', "''");
    Ok(
        rows_of(path, &format!("SELECT * FROM notes WHERE id = '{id}'"))?
            .into_iter()
            .next(),
    )
}
