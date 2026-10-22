//! Multi: a person's own devices keep in step through stash and retrieve,
//! without the hub holding that data (FR-SMUGGLR-077).
//!
//! [`stash`] makes a [`Blob`] of one envelope per row changed since the
//! device's last stash, each table's shape stamp once in its header;
//! [`retrieve`] verifies blobs and applies them by the envelope rules
//! (FR-SMUGGLR-075, FR-SMUGGLR-076), in any order. A whole blob holds every
//! row and the graveyard: a place's first stash is whole, and a device away
//! past the sunset catches up from one. Where blobs are kept, the device
//! list, crossing off and keys are the app's. A change a retrieve drops is
//! kept in the dropped log, and [`replay`] re-sends it.

mod retrieve;
mod stash;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::{Value, json};
use smugglr_core::envelope::Envelope;
use smugglr_core::hex::{hex, unhex};
use smugglr_core::ignore::Ignore;
use smugglr_core::plan::{Hashes, SyncMode};
use smugglr_core::row_hash::row_hash;
use smugglr_core::shape::{Column, Stamp};
use smugglr_core::steering::Steering;
use smugglr_memory::Store;
use smugglr_place::{Key, Place, Table};

pub use retrieve::{replay, retrieve};
pub use stash::stash;

/// The blob format version.
const VERSION: u64 = 1;

/// Why a stash or a retrieve could not run.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A place could not be reached, read or written.
    #[error(transparent)]
    Place(#[from] smugglr_place::Error),
    /// What smugglr keeps for the device could not be read or written.
    #[error(transparent)]
    Memory(#[from] smugglr_memory::Error),
    /// A table has no primary key; nothing is read.
    #[error("{0}")]
    Shape(String),
    /// The caller could not keep the blob; the device's stash is as it was.
    #[error("{0}")]
    Keep(String),
    /// A replay named a change the dropped log does not hold.
    #[error("the dropped log holds no change {0}")]
    NotDropped(String),
}

/// How a stash or a retrieve runs.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Where smugglr keeps what each device needs: files in smugglr's config
    /// directory, or a browser tab's own storage.
    pub store: Arc<dyn Store>,
    /// The shared key that signs and verifies envelopes.
    pub key: [u8; 32],
    /// This device's name.
    pub node: String,
    /// The devices a stash is for.
    pub targets: Vec<String>,
    /// The graveyard window in seconds: each change's sunset is `now` plus
    /// this.
    pub graveyard: u64,
    /// Now, in seconds since the Unix epoch.
    pub now: u64,
    /// What happens to a row changed here and in a blob: stop or last.
    pub sync_mode: SyncMode,
    /// The steering columns `last` reads.
    pub steering: Steering,
    /// The tables to ignore, beside those smugglr always ignores.
    pub ignore: Vec<String>,
}

impl Settings {
    /// The id of the key that signs: the first 8 bytes of its hash, as hex.
    fn key_id(&self) -> String {
        hex(&row_hash(&[&hex(&self.key)])[..8])
    }
}

/// What stash makes and retrieve applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob {
    /// The device that stashed it.
    pub node: String,
    /// The devices it is for.
    pub targets: Vec<String>,
    /// It holds every row and the graveyard, not what changed since the
    /// last stash.
    pub whole: bool,
    /// Each table's shape stamp, once.
    pub stamps: BTreeMap<String, Stamp>,
    /// One envelope per row.
    pub changes: Vec<Envelope>,
    /// A whole blob's graveyard: the delete envelopes still in their sunset.
    pub graveyard: Vec<Envelope>,
    /// The header's signature: over `whole`, `node`, the blob's `sunset`,
    /// `targets`, each table's stamp, and the count and change hash of
    /// every envelope.
    pub signature: [u8; 32],
    /// When the blob stops being taken, in seconds since the Unix epoch: its
    /// stash time plus the graveyard window. A whole blob past it is dead,
    /// even one with no envelopes.
    pub sunset: u64,
}

impl Blob {
    /// Signs the header with `key`.
    pub fn sign(&mut self, key: &[u8; 32]) {
        self.signature = *self.signed(key).as_bytes();
    }

    /// Whether the header's signature matches under `key`: nothing in the
    /// header changed, and every envelope it lists is there and no other.
    #[must_use]
    pub fn verify(&self, key: &[u8; 32]) -> bool {
        // blake3::Hash compares in constant time.
        self.signed(key) == blake3::Hash::from(self.signature)
    }

    /// BLAKE3 keyed with `key` over each header field, each prefixed with
    /// its byte length, so no field can run into the next.
    fn signed(&self, key: &[u8; 32]) -> blake3::Hash {
        fn field(hasher: &mut blake3::Hasher, bytes: &[u8]) {
            hasher.update(&(bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
        }
        let mut hasher = blake3::Hasher::new_keyed(key);
        field(&mut hasher, &VERSION.to_be_bytes());
        field(&mut hasher, if self.whole { b"whole" } else { b"since" });
        field(&mut hasher, self.node.as_bytes());
        field(&mut hasher, &self.sunset.to_be_bytes());
        field(&mut hasher, &(self.targets.len() as u64).to_be_bytes());
        for target in &self.targets {
            field(&mut hasher, target.as_bytes());
        }
        field(&mut hasher, &(self.stamps.len() as u64).to_be_bytes());
        for (table, stamp) in &self.stamps {
            field(&mut hasher, table.as_bytes());
            field(&mut hasher, &stamp.0);
        }
        for envelopes in [&self.changes, &self.graveyard] {
            field(&mut hasher, &(envelopes.len() as u64).to_be_bytes());
            for envelope in envelopes {
                field(&mut hasher, &envelope.signature);
            }
        }
        hasher.finalize()
    }

    /// The blob as JSON text: its header, then each envelope without the
    /// stamp the header holds.
    #[must_use]
    pub fn to_json(&self) -> String {
        let envelopes = |envelopes: &[Envelope]| -> Vec<Value> {
            envelopes
                .iter()
                .map(|envelope| {
                    let mut json = envelope.to_json();
                    if let Some(fields) = json.as_object_mut() {
                        fields.remove("stamp");
                    }
                    json
                })
                .collect()
        };
        let stamps: BTreeMap<&String, String> = self
            .stamps
            .iter()
            .map(|(table, stamp)| (table, hex(&stamp.0)))
            .collect();
        json!({
            "v": VERSION,
            "node": self.node,
            "targets": self.targets,
            "whole": self.whole,
            "stamps": stamps,
            "changes": envelopes(&self.changes),
            "graveyard": envelopes(&self.graveyard),
            "signature": hex(&self.signature),
            "sunset": self.sunset,
        })
        .to_string()
    }

    /// The blob [`Blob::to_json`] wrote; `None` when `text` is not one. Each
    /// envelope takes its table's stamp from the header, so a header changed
    /// after stashing fails each envelope's signature.
    #[must_use]
    pub fn from_json(text: &str) -> Option<Self> {
        let json: Value = serde_json::from_str(text).ok()?;
        if json.get("v")?.as_u64()? != VERSION {
            return None;
        }
        let mut stamps = BTreeMap::new();
        for (table, stamp) in json.get("stamps")?.as_object()? {
            let bytes = unhex(stamp.as_str()?)?;
            stamps.insert(table.clone(), Stamp(bytes.try_into().ok()?));
        }
        let envelopes = |name: &str| -> Option<Vec<Envelope>> {
            json.get(name)?
                .as_array()?
                .iter()
                .map(|envelope| {
                    let mut envelope = envelope.clone();
                    let table = envelope.get("table")?.as_str()?;
                    let stamp = hex(&stamps.get(table)?.0);
                    envelope
                        .as_object_mut()?
                        .insert("stamp".to_owned(), json!(stamp));
                    Envelope::from_json(&envelope)
                })
                .collect()
        };
        Some(Self {
            node: json.get("node")?.as_str()?.to_owned(),
            targets: json
                .get("targets")?
                .as_array()?
                .iter()
                .map(|target| target.as_str().map(str::to_owned))
                .collect::<Option<_>>()?,
            whole: json.get("whole")?.as_bool()?,
            changes: envelopes("changes")?,
            graveyard: envelopes("graveyard")?,
            signature: unhex(json.get("signature")?.as_str()?)?.try_into().ok()?,
            sunset: json.get("sunset")?.as_u64()?,
            stamps,
        })
    }
}

/// The place's tables, less those ignored, and the names of the ignored.
fn tables(
    place: &mut dyn Place,
    settings: &Settings,
) -> Result<(Vec<Table>, BTreeSet<String>), Error> {
    let ignore = Ignore::new(
        settings
            .ignore
            .iter()
            .map(String::as_str)
            .chain(place.own_tables().iter().copied())
            .map(str::to_owned),
    );
    let (tables, ignored) = ignore.split(place.tables()?, |table| &table.name);
    if let Some(table) = tables.iter().find(|table| table.key.is_empty()) {
        return Err(Error::Shape(format!(
            "the table {} in {} has no primary key",
            table.name,
            place.address()
        )));
    }
    Ok((tables, ignored))
}

/// A table's shape stamp.
fn stamp(table: &Table) -> Stamp {
    Stamp::of(&columns(table))
}

fn columns(table: &Table) -> Vec<Column> {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(i, name)| Column {
            name: name.clone(),
            kind: table.types.get(i).cloned().unwrap_or_default(),
        })
        .collect()
}

/// Each row's hash in `table`, by key.
fn hashes(place: &mut dyn Place, table: &Table) -> Result<Hashes, Error> {
    Ok(place.hashes(table)?.into_iter().collect())
}

/// The columns of each row of `table` whose key is in `keys`, by key.
fn rows(
    place: &mut dyn Place,
    table: &Table,
    keys: &[Key],
) -> Result<BTreeMap<Key, Vec<String>>, Error> {
    Ok(place
        .rows(table, keys)?
        .into_iter()
        .map(|row| (table.key_of(&row), row))
        .collect())
}
