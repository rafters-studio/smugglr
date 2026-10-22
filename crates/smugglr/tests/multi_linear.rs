//! A retrieve's cost grows linearly with the rows it writes (#577): a whole
//! blob of twice the rows takes less than three times as long to retrieve
//! into an empty place.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use smugglr_core::plan::SyncMode;
use smugglr_core::record::Status;
use smugglr_core::steering::Steering;
use smugglr_multi::{Settings, retrieve, stash};
use smugglr_place::LocalFile;

/// Stashes `rows` rows whole from one device and retrieves the blob into
/// another, empty one; returns how long the retrieve took.
fn time_whole_retrieve(rows: u32) -> Result<Duration, Box<dyn std::error::Error>> {
    let folder = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("multi-linear-{rows}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&folder);
    std::fs::create_dir_all(&folder)?;
    let shape = "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT);";
    Connection::open(folder.join("a.db"))?.execute_batch(&format!(
        "{shape} WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n \
         WHERE i < {rows}) INSERT INTO notes SELECT 'n' || i, 'body ' || i FROM n;"
    ))?;
    let empty = Connection::open(folder.join("b.db"))?;
    empty.execute_batch(shape)?;
    let settings = Settings {
        store: std::sync::Arc::new(smugglr_memory::Files::new(folder.join("config"))),
        key: [7; 32],
        node: "a".to_owned(),
        targets: vec!["b".to_owned()],
        graveyard: 600,
        now: 2_000_000_000,
        sync_mode: SyncMode::Stop,
        steering: Steering::default(),
        ignore: Vec::new(),
    };
    let blob = stash(
        &mut LocalFile::open(folder.join("a.db"))?,
        &settings,
        true,
        |_| Ok(()),
    )?;
    let b = Settings {
        node: "b".to_owned(),
        ..settings
    };
    let mut place = LocalFile::open(folder.join("b.db"))?;
    let start = Instant::now();
    let record = retrieve(&mut place, &[blob], &b, &[])?;
    let took = start.elapsed();
    assert_eq!(record.status, Status::Done, "the retrieve of {rows} rows");
    let held: u32 = empty.query_row("SELECT count(*) FROM notes", [], |row| row.get(0))?;
    assert_eq!(held, rows);
    drop((place, empty));
    let _ = std::fs::remove_dir_all(&folder);
    Ok(took)
}

#[test]
fn a_whole_retrieve_twice_as_big_takes_less_than_three_times_as_long()
-> Result<(), Box<dyn std::error::Error>> {
    let smaller = time_whole_retrieve(200_000)?;
    let larger = time_whole_retrieve(400_000)?;
    assert!(
        larger < smaller * 3,
        "200,000 rows took {smaller:?}, 400,000 rows took {larger:?}"
    );
    Ok(())
}
