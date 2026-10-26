//! A stash of rows handed over whole (FR-SMUGGLR-062, FR-SMUGGLR-077): an
//! app's server that reaches its database asynchronously, as a Cloudflare
//! Worker reaches D1, reads the rows itself and passes them in, and
//! smugglr-multi's own stash makes the blob a tab fills itself from with
//! `retrieve()`. Nothing is kept between stashes, so each blob is whole.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use serde_json::Value as Json;
use smugglr_core::change::Change;
use smugglr_core::row_hash::row_hash;
use smugglr_memory::Store;
use smugglr_multi::{Blob, Settings};
use smugglr_place::{Error, Key, Place, Table};
use wasm_bindgen::prelude::{JsValue, wasm_bindgen};

use crate::tab::{fail, settings as multi_settings, texts};

/// Tables and their rows, as the app read them: each row its columns'
/// `quote()` text, in column order.
#[derive(Debug)]
struct Rows {
    tables: Vec<(Table, BTreeMap<Key, Vec<String>>)>,
}

impl Rows {
    /// `[{name, columns, types, key, self_keyed, rows}]`, each table as a
    /// place's `tables` answers it, with its rows.
    fn from_json(text: &str) -> Result<Self, String> {
        let json: Json = serde_json::from_str(text).map_err(|err| err.to_string())?;
        let tables = json.as_array().ok_or("the tables are not a list")?;
        let tables = tables
            .iter()
            .map(|table| {
                let name = table["name"].as_str().ok_or("a table without a name")?;
                let table_of = Table {
                    name: name.to_owned(),
                    columns: texts(&table["columns"]),
                    types: texts(&table["types"]),
                    key: texts(&table["key"]),
                    self_keyed: table["self_keyed"].as_bool().unwrap_or(false),
                };
                let rows = table["rows"]
                    .as_array()
                    .ok_or_else(|| format!("{name} has no rows list"))?
                    .iter()
                    .map(|row| {
                        let row = texts(row);
                        if row.len() == table_of.columns.len() {
                            Ok((table_of.key_of(&row), row))
                        } else {
                            Err(format!("a row of {name} does not fit its columns"))
                        }
                    })
                    .collect::<Result<_, String>>()?;
                Ok((table_of, rows))
            })
            .collect::<Result<_, String>>()?;
        Ok(Self { tables })
    }

    fn table(&self, table: &Table) -> Result<&BTreeMap<Key, Vec<String>>, Error> {
        self.tables
            .iter()
            .find(|(t, _)| t.name == table.name)
            .map(|(_, rows)| rows)
            .ok_or_else(|| Error::Adapter(format!("no table {}", table.name)))
    }
}

impl Place for Rows {
    fn address(&self) -> &'static str {
        "rows"
    }

    fn tables(&mut self) -> Result<Vec<Table>, Error> {
        Ok(self.tables.iter().map(|(table, _)| table.clone()).collect())
    }

    fn hashes(&mut self, table: &Table) -> Result<Vec<(Key, [u8; 16])>, Error> {
        Ok(self
            .table(table)?
            .iter()
            .map(|(key, row)| {
                let columns: Vec<&str> = row.iter().map(String::as_str).collect();
                (key.clone(), row_hash(&columns))
            })
            .collect())
    }

    fn rows(&mut self, table: &Table, keys: &[Key]) -> Result<Vec<Vec<String>>, Error> {
        let rows = self.table(table)?;
        Ok(keys
            .iter()
            .filter_map(|key| rows.get(key).cloned())
            .collect())
    }

    fn write(&mut self, _: &Table, _: &[Change]) -> Result<Vec<Key>, Error> {
        Err(Error::Adapter(
            "rows handed over to stash are read only".to_owned(),
        ))
    }
}

/// Memory for one stash only.
#[derive(Debug, Default)]
struct Once(Mutex<BTreeMap<String, String>>);

impl Store for Once {
    fn read(&self, path: &str) -> Result<Option<String>, smugglr_memory::Error> {
        let files = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(files.get(path).cloned())
    }

    fn replace(&self, path: &str, text: &str) -> Result<(), smugglr_memory::Error> {
        let mut files = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        files.insert(path.to_owned(), text.to_owned());
        Ok(())
    }

    fn append(&self, path: &str, text: &str) -> Result<(), smugglr_memory::Error> {
        let mut files = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        files.entry(path.to_owned()).or_default().push_str(text);
        Ok(())
    }

    fn remove(&self, path: &str) -> Result<(), smugglr_memory::Error> {
        let mut files = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        files.remove(path);
        Ok(())
    }
}

fn stash(tables: &str, settings: &Settings) -> Result<Blob, String> {
    let mut rows = Rows::from_json(tables)?;
    smugglr_multi::stash(&mut rows, settings, true, |_| Ok(())).map_err(|err| err.to_string())
}

/// Stashes the rows handed over, and returns the blob as JSON text.
/// `tables` is `[{name, columns, types, key, self_keyed, rows}]`, each row
/// its columns' `quote()` text; `settings` is `{key, node, targets,
/// graveyard, now}`, as a tab's stash takes them.
///
/// # Errors
/// When the tables or settings are not as described, or a table has no
/// primary key.
#[wasm_bindgen(js_name = stashRows)]
pub fn stash_rows(tables: &str, settings: &str) -> Result<String, JsValue> {
    let settings = multi_settings(settings, Arc::new(Once::default()), Vec::new())?;
    stash(tables, &settings)
        .map(|blob| blob.to_json())
        .map_err(fail)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rusqlite::Connection;
    use serde_json::json;
    use smugglr_core::envelope::GRAVEYARD;
    use smugglr_core::plan::SyncMode;
    use smugglr_core::record::Status;
    use smugglr_core::steering::Steering;
    use smugglr_memory::Files;
    use smugglr_multi::Settings;
    use smugglr_place::LocalFile;

    use super::{Once, stash};

    const KEY: [u8; 32] = [7; 32];
    const SCHEMA: &str = "CREATE TABLE notes (id TEXT PRIMARY KEY, owner TEXT NOT NULL, body TEXT);
         CREATE TABLE tags (owner TEXT NOT NULL, name TEXT NOT NULL, n INTEGER, PRIMARY KEY (owner, name));";

    fn settings(store: Arc<dyn smugglr_memory::Store>, node: &str) -> Settings {
        Settings {
            store,
            key: KEY,
            node: node.to_owned(),
            targets: vec!["tab".to_owned()],
            graveyard: GRAVEYARD,
            now: 1_800_000_000,
            sync_mode: SyncMode::Stop,
            steering: Steering::default(),
            ignore: Vec::new(),
            max_value: smugglr_core::weight::DEFAULT_MAX_VALUE,
        }
    }

    fn quoted(db: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
        let mut statement = db.prepare(sql).map_err(|err| err.to_string())?;
        let width = statement.column_count();
        statement
            .query_map([], |row| (0..width).map(|i| row.get(i)).collect())
            .and_then(Iterator::collect)
            .map_err(|err| err.to_string())
    }

    #[test]
    fn a_device_retrieving_a_stash_of_rows_handed_over_holds_every_row() -> Result<(), String> {
        let text = |err: &dyn std::fmt::Display| err.to_string();
        let rows = json!([
            {"name": "notes", "columns": ["id", "owner", "body"], "types": ["TEXT", "TEXT", "TEXT"],
             "key": ["id"], "self_keyed": false,
             "rows": [["'n1'", "'ada'", "'it''s ada''s'"], ["'n2'", "'ada'", "NULL"]]},
            {"name": "tags", "columns": ["owner", "name", "n"], "types": ["TEXT", "TEXT", "INTEGER"],
             "key": ["owner", "name"], "self_keyed": false,
             "rows": [["'ada'", "'home'", "9223372036854775807"], ["'ada'", "'work'", "X'00FF'"]]}
        ]);
        let blob = stash(
            &rows.to_string(),
            &settings(Arc::new(Once::default()), "server"),
        )?;

        let folder = std::env::temp_dir().join(format!("smugglr-web-rows-{}", std::process::id()));
        std::fs::create_dir_all(&folder).map_err(|err| text(&err))?;
        let path = folder.join("tab.db");
        Connection::open(&path)
            .and_then(|db| db.execute_batch(SCHEMA))
            .map_err(|err| text(&err))?;
        let mut place = LocalFile::open(&path).map_err(|err| text(&err))?;
        let record = smugglr_multi::retrieve(
            &mut place,
            &[blob],
            &settings(Arc::new(Files::new(folder.join("memory"))), "tab"),
            &[],
        )
        .map_err(|err| text(&err))?;

        let db = Connection::open(&path).map_err(|err| text(&err))?;
        let notes = quoted(
            &db,
            "SELECT quote(id), quote(owner), quote(body) FROM notes ORDER BY id",
        )?;
        let tags = quoted(
            &db,
            "SELECT quote(owner), quote(name), quote(n) FROM tags ORDER BY name",
        )?;
        // Windows removes no file a connection still holds open.
        drop(db);
        drop(place);
        std::fs::remove_dir_all(&folder).map_err(|err| text(&err))?;

        assert_eq!(record.status, Status::Done);
        assert_eq!(
            notes,
            [
                ["'n1'", "'ada'", "'it''s ada''s'"],
                ["'n2'", "'ada'", "NULL"]
            ]
        );
        assert_eq!(
            tags,
            [
                ["'ada'", "'home'", "9223372036854775807"],
                ["'ada'", "'work'", "X'00FF'"]
            ]
        );
        Ok(())
    }
}
