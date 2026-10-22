//! A tab's own database, kept in step by the engine in the tab with an
//! endpoint the app names (FR-SMUGGLR-062).
//!
//! The tab's database is a place like any other (FR-SMUGGLR-071): HTTP-SQL
//! run by the worker on the tab's SQLite connection. Each run is the
//! command line's own: [`smugglr_sync::sync`] with the fetcher, and
//! smugglr-multi's stash, retrieve and replay, with the tab's memory in its
//! own OPFS storage. Each run returns its record and the keys it wrote into
//! the tab, by table, for the worker to fire `table-changed` with.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{Value as Json, json};
use smugglr_core::change::Change;
use smugglr_core::envelope::GRAVEYARD;
use smugglr_core::hex::unhex;
use smugglr_core::plan::Hashes;
use smugglr_core::record::Record;
use smugglr_memory::{Memory, Store};
use smugglr_multi::{Blob, Settings};
use smugglr_place::{Error, HttpSql, Key, Place, Table, Transport};
use smugglr_sync::Options;
use wasm_bindgen::prelude::{JsValue, wasm_bindgen};

use crate::fetcher::Fetcher;
use crate::host;
use crate::opfs::Opfs;

/// HTTP-SQL on the tab's own connection, run by the worker.
#[derive(Debug)]
struct TabSql;

impl Transport for TabSql {
    fn send(&self, request: &Json) -> Result<(bool, Json), String> {
        let answer = host::sql(&request.to_string())?;
        let answer = serde_json::from_str(&answer).map_err(|err| err.to_string())?;
        Ok((true, answer))
    }

    fn hashes_where_rows_live(&self) -> bool {
        true
    }
}

/// The tab's database, its writes noted: the keys written, by table.
#[derive(Debug)]
struct Watched {
    place: HttpSql,
    changed: BTreeMap<String, Vec<Key>>,
}

impl Place for Watched {
    fn address(&self) -> &str {
        self.place.address()
    }

    fn tables(&mut self) -> Result<Vec<Table>, Error> {
        self.place.tables()
    }

    fn hashes(&mut self, table: &Table) -> Result<Vec<(Key, [u8; 16])>, Error> {
        self.place.hashes(table)
    }

    fn rows(&mut self, table: &Table, keys: &[Key]) -> Result<Vec<Vec<String>>, Error> {
        self.place.rows(table, keys)
    }

    fn weights(&mut self, table: &Table) -> Result<Vec<(Key, u64)>, Error> {
        self.place.weights(table)
    }

    fn write(&mut self, table: &Table, changes: &[Change]) -> Result<Vec<Key>, Error> {
        let written = self.place.write(table, changes)?;
        if !written.is_empty() {
            self.changed
                .entry(table.name.clone())
                .or_default()
                .extend(written.iter().cloned());
        }
        Ok(written)
    }
}

/// What `open()` names: the tab's database, the endpoint, and the settings
/// the app gives.
#[derive(Debug)]
struct Tab {
    db: String,
    endpoint: String,
    ignore: Vec<String>,
    max_wire: Option<u64>,
}

impl Tab {
    fn from_json(text: &str) -> Result<Self, JsValue> {
        let json: Json = serde_json::from_str(text).map_err(fail)?;
        let text = |name: &str| {
            json[name]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| JsValue::from_str(&format!("open() needs {name}")))
        };
        Ok(Self {
            db: text("db")?,
            endpoint: text("endpoint")?,
            ignore: texts(&json["ignore"]),
            max_wire: json["maxWire"].as_u64(),
        })
    }

    fn place(&self) -> Watched {
        Watched {
            place: HttpSql::through(&format!("opfs:/{}", self.db), Box::new(TabSql)),
            changed: BTreeMap::new(),
        }
    }

    fn store() -> Arc<dyn Store> {
        Arc::new(Opfs)
    }

    fn memory(&self, tab: &dyn Place) -> Result<Memory, JsValue> {
        Memory::open_in(Self::store(), tab.address(), &self.endpoint).map_err(fail)
    }

    /// One direct sync of the endpoint (the source) with the tab (the
    /// target), so a first sync too much for the wire says to fill the tab
    /// by retrieve.
    fn sync(&self) -> Result<String, JsValue> {
        let mut tab = self.place();
        let options = Options {
            memory: Some(Self::store()),
            ignore: self.ignore.clone(),
            max_wire: self.max_wire,
            ..Options::default()
        };
        let mut endpoint = Fetcher::new(&self.endpoint);
        match smugglr_sync::sync(&mut endpoint, &mut tab, &options) {
            Ok(record) => Ok(answer(&record, &tab.changed)),
            // A run cut off after writing rows into the tab still names
            // them, so table-changed goes out before the error.
            Err(err) if !tab.changed.is_empty() => {
                Ok(json!({"error": err.to_string(), "changed": tab.changed}).to_string())
            }
            Err(err) => Err(fail(err)),
        }
    }

    /// The settings of a stash, a retrieve or a replay, from the app's.
    fn settings(&self, text: &str) -> Result<Settings, JsValue> {
        let json: Json = serde_json::from_str(text).map_err(fail)?;
        let key = json["key"]
            .as_str()
            .and_then(unhex)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| JsValue::from_str("the key is 32 bytes as hex"))?;
        Ok(Settings {
            store: Self::store(),
            key,
            node: json["node"].as_str().unwrap_or_default().to_owned(),
            targets: texts(&json["targets"]),
            graveyard: json["graveyard"].as_u64().unwrap_or(GRAVEYARD),
            now: json["now"].as_u64().unwrap_or_default(),
            sync_mode: smugglr_core::plan::SyncMode::Stop,
            steering: smugglr_core::steering::Steering::default(),
            ignore: self.ignore.clone(),
        })
    }

    /// A retrieve into a tab that has no memory with its endpoint fills it
    /// from a blob made from that endpoint: each row it wrote is where the
    /// two places last agreed, so the sync that follows carries only what
    /// changed since the blob.
    fn agree(
        &self,
        tab: &mut dyn Place,
        changed: &BTreeMap<String, Vec<Key>>,
    ) -> Result<(), JsValue> {
        let mut memory = self.memory(tab)?;
        if !memory.first_sync() || changed.is_empty() {
            return Ok(());
        }
        let tables = tab.tables().map_err(fail)?;
        for table in tables {
            let Some(keys) = changed.get(&table.name) else {
                continue;
            };
            let agreed: Hashes = tab
                .hashes(&table)
                .map_err(fail)?
                .into_iter()
                .filter(|(key, _)| keys.contains(key))
                .collect();
            memory.set_table(&table.name, agreed);
        }
        memory.save().map_err(fail)
    }
}

fn texts(json: &Json) -> Vec<String> {
    json.as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn fail(err: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&err.to_string())
}

/// A run's record and the keys it wrote into the tab, by table.
fn answer(record: &Record, changed: &BTreeMap<String, Vec<Key>>) -> String {
    json!({"record": record, "changed": changed}).to_string()
}

/// Opens the tab's sync with its endpoint: when the tab has no memory for
/// the pair, it runs the first sync and returns its run; otherwise nothing.
///
/// # Errors
/// When a place cannot be reached, read or written, or memory kept.
#[wasm_bindgen(js_name = openTab)]
pub fn open_tab(tab: &str) -> Result<Option<String>, JsValue> {
    let tab = Tab::from_json(tab)?;
    if tab.memory(&tab.place())?.first_sync() {
        tab.sync().map(Some)
    } else {
        Ok(None)
    }
}

/// Syncs the tab with its endpoint once, and returns the run.
///
/// # Errors
/// When a place cannot be reached, read or written, or memory kept; a stop
/// is a record, never an error.
#[wasm_bindgen(js_name = syncTab)]
pub fn sync_tab(tab: &str) -> Result<String, JsValue> {
    Tab::from_json(tab)?.sync()
}

/// Stashes the tab, and returns the blob as JSON text.
///
/// # Errors
/// When the tab cannot be read, or its memory kept.
#[wasm_bindgen(js_name = stashTab)]
pub fn stash_tab(tab: &str, settings: &str) -> Result<String, JsValue> {
    let tab = Tab::from_json(tab)?;
    let settings = tab.settings(settings)?;
    let blob =
        smugglr_multi::stash(&mut tab.place(), &settings, false, |_| Ok(())).map_err(fail)?;
    Ok(blob.to_json())
}

/// Retrieves blobs, each as JSON text, into the tab, and returns the run.
///
/// # Errors
/// When a blob is not one stash made, the tab cannot be read or written, or
/// its memory kept.
#[wasm_bindgen(js_name = retrieveTab)]
#[expect(
    clippy::needless_pass_by_value,
    reason = "wasm-bindgen hands the blobs over from JS as an owned Vec"
)]
pub fn retrieve_tab(tab: &str, settings: &str, blobs: Vec<String>) -> Result<String, JsValue> {
    let tab = Tab::from_json(tab)?;
    let settings = tab.settings(settings)?;
    let blobs = blobs
        .iter()
        .map(|text| Blob::from_json(text).ok_or_else(|| JsValue::from_str("not a smugglr blob")))
        .collect::<Result<Vec<Blob>, JsValue>>()?;
    let mut place = tab.place();
    let record = smugglr_multi::retrieve(&mut place, &blobs, &settings, &[]).map_err(fail)?;
    let changed = std::mem::take(&mut place.changed);
    tab.agree(&mut place, &changed)?;
    Ok(answer(&record, &changed))
}

/// Replays the change a retrieve dropped, by its change hash, and returns
/// the run.
///
/// # Errors
/// When the dropped log holds no such change, the tab cannot be read or
/// written, or its memory kept.
#[wasm_bindgen(js_name = replayTab)]
pub fn replay_tab(tab: &str, settings: &str, change: &str) -> Result<String, JsValue> {
    let tab = Tab::from_json(tab)?;
    let settings = tab.settings(settings)?;
    let mut place = tab.place();
    let record = smugglr_multi::replay(&mut place, change, &settings).map_err(fail)?;
    Ok(answer(&record, &place.changed))
}
