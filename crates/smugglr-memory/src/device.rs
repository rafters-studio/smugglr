//! What multi keeps for one device between runs, in smugglr's own files
//! (FR-SMUGGLR-077): each key's hash at the last stash, each key's last
//! applied `from`, the graveyard, the delete envelopes kept until their
//! sunset, and the dropped log, each change a retrieve dropped
//! (FR-SMUGGLR-076). The device's folder is named by a hash of its place's
//! address.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde_json::{Value, json};
use smugglr_core::envelope::Envelope;
use smugglr_core::plan::Hashes;
use smugglr_core::row_hash::row_hash;

use crate::{Error, Store, hex, unhex};

/// The file in a device's folder.
const FILE: &str = "device.json";

/// A row of a table, by table name and key.
pub type Row = (String, Vec<String>);

/// What multi keeps for one device.
#[derive(Debug, Default)]
pub struct Device {
    folder: String,
    /// The device has stashed before: its next stash holds only what
    /// changed since.
    pub stashed: bool,
    /// Each key's hash at the last stash, by table.
    pub baseline: BTreeMap<String, Hashes>,
    /// Each key's last applied `from`: the `from` of the change that brought
    /// the row to its hash here.
    pub last_from: BTreeMap<Row, Option<[u8; 16]>>,
    /// The delete envelopes kept until their sunset, one per key.
    pub graveyard: BTreeMap<Row, Envelope>,
    /// The dropped log: each change dropped here, envelope and values, by
    /// which a replay re-sends it.
    pub dropped: Vec<Envelope>,
}

impl Device {
    /// Opens what multi keeps for the place at `address`, in `store`; a
    /// place that never stashed or retrieved has nothing.
    ///
    /// # Errors
    /// When the file is there and cannot be read, or is not one smugglr
    /// wrote.
    pub fn open(store: &dyn Store, address: &str) -> Result<Self, Error> {
        let folder = hex(&row_hash(&["multi", address]));
        let path = format!("{folder}/{FILE}");
        let Some(text) = store.read(&path)? else {
            return Ok(Self {
                folder,
                ..Self::default()
            });
        };
        let path = PathBuf::from(path);
        let unreadable = || Error::Unreadable {
            path: path.clone(),
            line: 1,
        };
        let json: Value = serde_json::from_str(&text).map_err(|_| unreadable())?;
        let mut device = Self {
            folder,
            stashed: json
                .get("stashed")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            ..Self::default()
        };
        // A hash as the file holds it: hex, or `null` for no row.
        let hash16 = |value: &Value| -> Option<Option<[u8; 16]>> {
            match value {
                Value::Null => Some(None),
                value => Some(Some(unhex(value.as_str()?)?.try_into().ok()?)),
            }
        };
        let rows = json.get("rows").and_then(Value::as_array);
        for row in rows.into_iter().flatten() {
            let (table, key) = row_of(row).ok_or_else(unreadable)?;
            if let Some(hash) = row.get("hash") {
                let hash = hash16(hash).flatten().ok_or_else(unreadable)?;
                device
                    .baseline
                    .entry(table.clone())
                    .or_default()
                    .insert(key.clone(), hash);
            }
            if let Some(from) = row.get("from") {
                let from = hash16(from).ok_or_else(unreadable)?;
                device.last_from.insert((table, key), from);
            }
        }
        let graveyard = json.get("graveyard").and_then(Value::as_array);
        for tombstone in graveyard.into_iter().flatten() {
            let tombstone = Envelope::from_json(tombstone).ok_or_else(unreadable)?;
            let row = (tombstone.change.table.clone(), tombstone.change.key.clone());
            device.graveyard.insert(row, tombstone);
        }
        let dropped = json.get("dropped").and_then(Value::as_array);
        for envelope in dropped.into_iter().flatten() {
            let envelope = Envelope::from_json(envelope).ok_or_else(unreadable)?;
            device.dropped.push(envelope);
        }
        Ok(device)
    }

    /// Saves what the device keeps in `store`, replacing what was there, less
    /// the tombstones whose sunset is before `now`.
    ///
    /// # Errors
    /// When the folder or its file cannot be written.
    pub fn save(&mut self, store: &dyn Store, now: u64) -> Result<(), Error> {
        self.graveyard
            .retain(|_, tombstone| now <= tombstone.sunset);
        let mut rows: BTreeMap<Row, serde_json::Map<String, Value>> = BTreeMap::new();
        for (table, hashes) in &self.baseline {
            for (key, hash) in hashes {
                rows.entry((table.clone(), key.clone()))
                    .or_default()
                    .insert("hash".to_owned(), json!(hex(hash)));
            }
        }
        for (row, from) in &self.last_from {
            rows.entry(row.clone())
                .or_default()
                .insert("from".to_owned(), json!(from.map(|from| hex(&from))));
        }
        let rows: Vec<Value> = rows
            .into_iter()
            .map(|((table, key), mut fields)| {
                fields.insert("table".to_owned(), json!(table));
                fields.insert("key".to_owned(), json!(key));
                Value::Object(fields)
            })
            .collect();
        let graveyard: Vec<Value> = self.graveyard.values().map(Envelope::to_json).collect();
        let dropped: Vec<Value> = self.dropped.iter().map(Envelope::to_json).collect();
        let text = json!({
            "stashed": self.stashed,
            "rows": rows,
            "graveyard": graveyard,
            "dropped": dropped,
        });
        // As memory's save: the old file or the new one, never part of one.
        store.replace(&format!("{}/{FILE}", self.folder), &text.to_string())
    }
}

fn row_of(row: &Value) -> Option<Row> {
    let table = row.get("table")?.as_str()?.to_owned();
    let key = row
        .get("key")?
        .as_array()?
        .iter()
        .map(|column| column.as_str().map(str::to_owned))
        .collect::<Option<Vec<String>>>()?;
    Some((table, key))
}
