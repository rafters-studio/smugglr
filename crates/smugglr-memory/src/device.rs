//! What multi keeps for one device between runs, in smugglr's own files
//! (FR-SMUGGLR-077): each key's hash at the last stash, each key's last
//! applied `from`, and the graveyard, the delete envelopes kept until their
//! sunset (FR-SMUGGLR-076). The device's folder is named by a hash of its
//! place's address.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use smugglr_core::envelope::Envelope;
use smugglr_core::plan::Hashes;
use smugglr_core::row_hash::row_hash;

use crate::{Error, hex, io_at, sync_folder, unhex};

/// The file in a device's folder.
const FILE: &str = "device.json";

/// A row of a table, by table name and key.
pub type Row = (String, Vec<String>);

/// What multi keeps for one device.
#[derive(Debug, Default)]
pub struct Device {
    folder: PathBuf,
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
}

impl Device {
    /// Opens what multi keeps for the place at `address`, under `config`; a
    /// place that never stashed or retrieved has nothing.
    ///
    /// # Errors
    /// When the file is there and cannot be read, or is not one smugglr
    /// wrote.
    pub fn open(config: &Path, address: &str) -> Result<Self, Error> {
        let folder = config.join(hex(&row_hash(&["multi", address])));
        let path = folder.join(FILE);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(Self {
                    folder,
                    ..Self::default()
                });
            }
            Err(source) => return Err(Error::Io { path, source }),
        };
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
        Ok(device)
    }

    /// Saves what the device keeps, replacing what was there, less the
    /// tombstones whose sunset is before `now`.
    ///
    /// # Errors
    /// When the folder or its file cannot be written.
    pub fn save(&mut self, now: u64) -> Result<(), Error> {
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
        let text = json!({"stashed": self.stashed, "rows": rows, "graveyard": graveyard});
        fs::create_dir_all(&self.folder).map_err(io_at(&self.folder))?;
        let path = self.folder.join(FILE);
        let next = self.folder.join("device.json.next");
        // As memory's save: the old file or the new one, never an empty one.
        let mut file = fs::File::create(&next).map_err(io_at(&next))?;
        file.write_all(text.to_string().as_bytes())
            .map_err(io_at(&next))?;
        file.sync_all().map_err(io_at(&next))?;
        fs::rename(&next, &path).map_err(io_at(&path))?;
        sync_folder(&self.folder).map_err(io_at(&self.folder))
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
