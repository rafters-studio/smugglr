//! Stash and retrieve refuse and name a value over the value limit, and
//! move every other row (FR-SMUGGLR-071 rev 11).

use std::path::PathBuf;

use rusqlite::Connection;
use smugglr_core::plan::SyncMode;
use smugglr_core::steering::Steering;
use smugglr_multi::{Settings, retrieve, stash};
use smugglr_place::LocalFile;

#[test]
fn stash_and_retrieve_name_a_value_over_the_limit_and_move_the_rest()
-> Result<(), Box<dyn std::error::Error>> {
    let folder = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("multi-limit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&folder);
    std::fs::create_dir_all(&folder)?;
    let shape = "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT);";
    Connection::open(folder.join("a.db"))?.execute_batch(&format!(
        "{shape} INSERT INTO notes VALUES ('big', printf('%.2000000c', 'x')), \
         ('small', 'fits');"
    ))?;
    Connection::open(folder.join("b.db"))?.execute_batch(shape)?;
    let settings = |node: &str, max_value: u64| Settings {
        store: std::sync::Arc::new(smugglr_memory::Files::new(folder.join(node))),
        key: [7; 32],
        node: node.to_owned(),
        targets: Vec::new(),
        graveyard: 600,
        now: 2_000_000_000,
        sync_mode: SyncMode::Stop,
        steering: Steering::default(),
        ignore: Vec::new(),
        max_value,
    };
    // A stash under a 1 MB limit leaves the 2 MB row out, and names it.
    let mut a = LocalFile::open(folder.join("a.db"))?;
    let low = stash(&mut a, &settings("low", 1_000_000), true, |_| Ok(()))?;
    let keys: Vec<&str> = low
        .changes
        .iter()
        .map(|envelope| envelope.change.key[0].as_str())
        .collect();
    assert_eq!(keys, ["'small'"]);
    let named: Vec<String> = low.refused.iter().map(ToString::to_string).collect();
    assert_eq!(named.len(), 1, "{named:?}");
    assert!(named[0].contains("refused: notes big: a value of 2000000 bytes in body"));
    // A blob holding it, retrieved under a 1 MB limit: named, and the rest
    // taken.
    let whole = stash(&mut a, &settings("a", 2_000_000), true, |_| Ok(()))?;
    assert_eq!(whole.changes.len(), 2);
    let mut b = LocalFile::open(folder.join("b.db"))?;
    let record = retrieve(&mut b, &[whole], &settings("b", 1_000_000), &[])?;
    assert_eq!(record.refused.len(), 1, "{record}");
    assert!(
        record.to_string().contains("refused: notes big"),
        "{record}"
    );
    let held: Vec<String> = Connection::open(folder.join("b.db"))?
        .prepare("SELECT id FROM notes")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    assert_eq!(held, ["small"]);
    drop((a, b));
    let _ = std::fs::remove_dir_all(&folder);
    Ok(())
}
