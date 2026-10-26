//! A whole blob's copy of a row this device already holds, different here,
//! is decided by `--sync-mode` and named, never left silently (issue #608).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::Connection;
use smugglr_core::plan::SyncMode;
use smugglr_core::record::{self, CHANGED_HERE_AND_IN_BLOB, Key};
use smugglr_core::steering::Steering;
use smugglr_multi::{Settings, retrieve, stash};
use smugglr_place::LocalFile;

static NEXT: AtomicUsize = AtomicUsize::new(0);

fn sql(folder: &Path, device: &str, statement: &str) -> Result<(), String> {
    Connection::open(folder.join(format!("{device}.db")))
        .and_then(|c| c.execute_batch(statement))
        .map_err(|err| err.to_string())
}

fn place(folder: &Path, device: &str) -> Result<LocalFile, String> {
    LocalFile::open(folder.join(format!("{device}.db")).display().to_string())
        .map_err(|err| err.to_string())
}

fn body(folder: &Path, device: &str, id: &str) -> Result<String, String> {
    Connection::open(folder.join(format!("{device}.db")))
        .and_then(|c| c.query_row("SELECT body FROM notes WHERE id = ?1", [id], |r| r.get(0)))
        .map_err(|err| err.to_string())
}

/// `a` whole-stashes a newer `n1` and `b`, which never stashed and holds an
/// older `n1`, retrieves it under `mode`: b's `n1` after, and the record's
/// part for `notes`.
fn first_retrieve(mode: SyncMode) -> Result<(String, record::Table), String> {
    let folder = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "multi-steering-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&folder);
    std::fs::create_dir_all(&folder).map_err(|err| err.to_string())?;
    let table = "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT, updated_at INTEGER);";
    sql(
        &folder,
        "a",
        &format!("{table} INSERT INTO notes VALUES ('n1', 'newer', 2), ('n2', 'two', 1);"),
    )?;
    sql(
        &folder,
        "b",
        &format!("{table} INSERT INTO notes VALUES ('n1', 'older', 1);"),
    )?;
    let settings = Settings {
        store: std::sync::Arc::new(smugglr_memory::Files::new(folder.join("config"))),
        key: [7; 32],
        node: "a".to_owned(),
        targets: vec!["b".to_owned()],
        graveyard: 600,
        now: 2_000_000_000,
        sync_mode: mode,
        steering: Steering {
            all: Some("updated_at".to_owned()),
            ..Steering::default()
        },
        ignore: Vec::new(),
    };
    let blob =
        stash(&mut place(&folder, "a")?, &settings, true, |_| Ok(())).map_err(|e| e.to_string())?;
    let b = Settings {
        node: "b".to_owned(),
        ..settings
    };
    let mut record =
        retrieve(&mut place(&folder, "b")?, &[blob], &b, &[]).map_err(|e| e.to_string())?;
    let notes = record
        .tables
        .remove("notes")
        .ok_or("no notes in the record")?;
    Ok((body(&folder, "b", "n1")?, notes))
}

#[test]
fn last_overwrites_an_older_row_held_here_and_names_it() -> Result<(), String> {
    let (n1, notes) = first_retrieve(SyncMode::Last)?;
    assert_eq!(n1, "newer");
    let record::Table::Moved(moves) = notes else {
        return Err(format!("not moved: {notes:?}"));
    };
    assert_eq!(
        moves.overwritten_in_target,
        vec![Key(vec!["'n1'".to_owned()])]
    );
    assert_eq!(moves.to_target, 2);
    Ok(())
}

#[test]
fn stop_names_a_row_held_here_that_differs_from_the_blob() -> Result<(), String> {
    let (n1, notes) = first_retrieve(SyncMode::Stop)?;
    assert_eq!(n1, "older");
    let record::Table::Stopped { rows, why, .. } = notes else {
        return Err(format!("not stopped: {notes:?}"));
    };
    assert_eq!(rows, vec![Key(vec!["'n1'".to_owned()])]);
    assert_eq!(why, CHANGED_HERE_AND_IN_BLOB);
    Ok(())
}
