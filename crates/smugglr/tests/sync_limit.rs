//! A dry run names the rows a sync would refuse for a value over the value
//! limit, as the run itself does (FR-SMUGGLR-071 rev 11).

use std::path::PathBuf;

use rusqlite::Connection;
use smugglr::{Options, sync};
use smugglr_place::LocalFile;

#[test]
fn a_dry_run_names_a_value_over_the_limit() -> Result<(), Box<dyn std::error::Error>> {
    let folder = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("sync-limit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&folder);
    std::fs::create_dir_all(&folder)?;
    let shape = "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT);";
    Connection::open(folder.join("source.db"))?.execute_batch(&format!(
        "{shape} INSERT INTO notes VALUES ('big', printf('%.2000000c', 'x')), ('small', 'fits');"
    ))?;
    Connection::open(folder.join("target.db"))?.execute_batch(shape)?;
    let options = Options {
        config_dir: Some(folder.join("config")),
        dryrun: true,
        max_value: Some(1_000_000),
        ..Options::default()
    };
    let mut source = LocalFile::open(folder.join("source.db"))?;
    let mut target = LocalFile::open(folder.join("target.db"))?;
    let record = sync(&mut source, &mut target, &options)?;
    let text = record.to_string();
    assert!(
        text.contains("refused: notes big: a value of 2000000 bytes in body"),
        "{text}"
    );
    assert_eq!(record.refused.len(), 1, "{text}");
    drop((source, target));
    let _ = std::fs::remove_dir_all(&folder);
    Ok(())
}
