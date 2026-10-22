//! A blob's header is signed, a whole take needs every envelope it lists,
//! and a whole blob past its sunset is dead (issue #519, note 7).

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::Connection;
use smugglr_core::plan::SyncMode;
use smugglr_core::steering::Steering;
use smugglr_multi::{Blob, Settings, retrieve, stash};
use smugglr_place::LocalFile;

const NOW: u64 = 2_000_000_000;

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// A folder of its own, two device files in it, and the settings to run
/// with; `b` holds three notes, `a` the same three.
fn devices() -> Result<(PathBuf, Settings), String> {
    let folder = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "multi-header-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&folder);
    std::fs::create_dir_all(&folder).map_err(|err| err.to_string())?;
    for device in ["a", "b"] {
        sql(
            &folder,
            device,
            "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT); \
             INSERT INTO notes VALUES ('n1', 'one'), ('n2', 'two'), ('n3', 'three');",
        )?;
    }
    let settings = Settings {
        config_dir: folder.join("config"),
        key: [7; 32],
        node: "a".to_owned(),
        targets: vec!["b".to_owned()],
        graveyard: 600,
        now: NOW,
        sync_mode: SyncMode::Stop,
        steering: Steering::default(),
        ignore: Vec::new(),
    };
    // b has stashed, so a whole take would replace each row of b's.
    let b = Settings {
        node: "b".to_owned(),
        ..settings.clone()
    };
    stash(&mut place(&folder, "b")?, &b, false).map_err(|err| err.to_string())?;
    Ok((folder, settings))
}

fn sql(folder: &std::path::Path, device: &str, statement: &str) -> Result<(), String> {
    Connection::open(folder.join(format!("{device}.db")))
        .and_then(|c| c.execute_batch(statement))
        .map_err(|err| err.to_string())
}

fn place(folder: &std::path::Path, device: &str) -> Result<LocalFile, String> {
    LocalFile::open(folder.join(format!("{device}.db")).display().to_string())
        .map_err(|err| err.to_string())
}

fn notes(folder: &std::path::Path, device: &str) -> Result<Vec<(String, String)>, String> {
    let connection =
        Connection::open(folder.join(format!("{device}.db"))).map_err(|err| err.to_string())?;
    let mut statement = connection
        .prepare("SELECT id, body FROM notes ORDER BY id")
        .map_err(|err| err.to_string())?;
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .and_then(Iterator::collect)
        .map_err(|err| err.to_string())
}

/// `b` retrieves `blob` as device b.
fn retrieve_on_b(
    folder: &std::path::Path,
    settings: &Settings,
    blob: &Blob,
) -> Result<Vec<(String, String)>, String> {
    let settings = Settings {
        node: "b".to_owned(),
        ..settings.clone()
    };
    retrieve(
        &mut place(folder, "b")?,
        std::slice::from_ref(blob),
        &settings,
        &[],
    )
    .map_err(|err| err.to_string())?;
    notes(folder, "b")
}

#[test]
fn a_blob_whose_header_was_changed_is_refused_whole() -> Result<(), String> {
    let (folder, settings) = devices()?;
    stash(&mut place(&folder, "a")?, &settings, false).map_err(|err| err.to_string())?;
    sql(
        &folder,
        "a",
        "UPDATE notes SET body = 'edited' WHERE id = 'n1'",
    )?;
    let mut blob = stash(&mut place(&folder, "a")?, &settings, false).map_err(|e| e.to_string())?;
    // Marked whole after signing, the one-change blob would delete n2 and n3.
    blob.whole = true;
    let before = notes(&folder, "b")?;
    assert_eq!(retrieve_on_b(&folder, &settings, &blob)?, before);
    Ok(())
}

#[test]
fn a_whole_blob_with_an_envelope_that_fails_applies_nothing() -> Result<(), String> {
    let (folder, settings) = devices()?;
    sql(
        &folder,
        "a",
        "UPDATE notes SET body = 'edited' WHERE id = 'n1'",
    )?;
    let mut blob = stash(&mut place(&folder, "a")?, &settings, true).map_err(|e| e.to_string())?;
    // Values that no longer hash to the envelope's to; the header, which
    // lists the envelope's hash, still verifies.
    let n2 = blob
        .changes
        .iter_mut()
        .find(|envelope| envelope.change.key == ["'n2'"])
        .ok_or("no n2")?;
    n2.change.values[1] = "'forged'".to_owned();
    assert!(blob.verify(&settings.key));
    let before = notes(&folder, "b")?;
    assert_eq!(retrieve_on_b(&folder, &settings, &blob)?, before);
    Ok(())
}

#[test]
fn a_whole_blob_past_its_sunset_is_refused() -> Result<(), String> {
    let (folder, settings) = devices()?;
    let blob = stash(&mut place(&folder, "a")?, &settings, true).map_err(|e| e.to_string())?;
    // b moved on after the whole stash was made; taking it would roll b back.
    sql(
        &folder,
        "b",
        "UPDATE notes SET body = 'newer' WHERE id = 'n3'",
    )?;
    let later = Settings {
        now: NOW + 601,
        ..settings.clone()
    };
    let b = Settings {
        node: "b".to_owned(),
        ..later.clone()
    };
    stash(&mut place(&folder, "b")?, &b, false).map_err(|err| err.to_string())?;
    let before = notes(&folder, "b")?;
    assert_eq!(retrieve_on_b(&folder, &later, &blob)?, before);
    Ok(())
}

#[test]
fn an_empty_whole_blob_past_its_sunset_is_refused() -> Result<(), String> {
    let (folder, settings) = devices()?;
    // A place with no rows stashes a whole blob that holds no envelopes;
    // taken, it would delete every row of b's.
    sql(&folder, "a", "DELETE FROM notes")?;
    let blob = stash(&mut place(&folder, "a")?, &settings, true).map_err(|e| e.to_string())?;
    assert!(blob.changes.is_empty() && blob.graveyard.is_empty());
    let later = Settings {
        now: NOW + 601,
        ..settings.clone()
    };
    let before = notes(&folder, "b")?;
    assert_eq!(retrieve_on_b(&folder, &later, &blob)?, before);
    Ok(())
}

#[test]
fn a_whole_blob_whose_tombstone_died_after_the_stash_is_still_taken() -> Result<(), String> {
    let (folder, settings) = devices()?;
    stash(&mut place(&folder, "a")?, &settings, false).map_err(|err| err.to_string())?;
    // a deletes n2 at NOW: its tombstone's sunset is NOW + 600.
    sql(&folder, "a", "DELETE FROM notes WHERE id = 'n2'")?;
    stash(&mut place(&folder, "a")?, &settings, false).map_err(|err| err.to_string())?;
    sql(
        &folder,
        "a",
        "UPDATE notes SET body = 'edited' WHERE id = 'n1'",
    )?;
    // The whole stash at NOW + 550 lives until NOW + 1150 and carries the
    // tombstone, which dies at NOW + 600, before b retrieves at NOW + 700.
    let at = |now| Settings {
        now,
        ..settings.clone()
    };
    let blob =
        stash(&mut place(&folder, "a")?, &at(NOW + 550), true).map_err(|err| err.to_string())?;
    assert_eq!(blob.graveyard.len(), 1);
    assert_eq!(
        retrieve_on_b(&folder, &at(NOW + 700), &blob)?,
        notes(&folder, "a")?
    );
    Ok(())
}
