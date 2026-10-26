//! Stash: one envelope per row changed since the device's last stash.

use std::collections::BTreeMap;

use smugglr_core::change::{Change, op};
use smugglr_core::envelope::{Envelope, VERSION};
use smugglr_core::plan::Hashes;
use smugglr_core::record::Refused;
use smugglr_core::row_hash::row_hash;
use smugglr_core::weight::over_limit;
use smugglr_memory::Device;
use smugglr_place::{Key, Place, Table};

use crate::{Blob, Error, Settings, hashes, rows, stamp, tables};

/// Stashes `place`: a blob of one signed envelope per row changed since its
/// last stash, a delete for each row deleted since, for `settings.targets`.
/// With `whole`, or when the place never stashed, the blob holds every row
/// and the graveyard instead. `keep` keeps the blob (writes it where the app
/// takes it from); only once it has are the device's hashes kept as at this
/// stash, so a blob that was never kept leaves the next stash carrying every
/// change since the last blob that was.
///
/// # Errors
/// When the place cannot be read, a table has no primary key, `keep` fails
/// ([`Error::Keep`]), or what the device keeps cannot be read or written.
pub fn stash(
    place: &mut dyn Place,
    settings: &Settings,
    whole: bool,
    keep: impl FnOnce(&Blob) -> Result<(), String>,
) -> Result<Blob, Error> {
    let mut device = Device::open(&*settings.store, place.address())?;
    let whole = whole || !device.stashed;
    let sunset = settings.now.saturating_add(settings.graveyard);
    let (tables, _) = tables(place, settings)?;
    let mut blob = Blob {
        node: settings.node.clone(),
        targets: settings.targets.clone(),
        whole,
        stamps: BTreeMap::new(),
        changes: Vec::new(),
        graveyard: Vec::new(),
        signature: [0; 32],
        sunset,
        refused: Vec::new(),
    };
    let limit = settings.limit(place);
    for table in &tables {
        let stamp = stamp(table);
        blob.stamps.insert(table.name.clone(), stamp);
        let now = hashes(place, table)?;
        let before = device.baseline.remove(&table.name).unwrap_or_default();
        let mut baseline = now.clone();
        // A whole stash carries each row as if new: retrieve takes it as it is.
        let from = |key: &Key| {
            if whole {
                None
            } else {
                before.get(key).copied()
            }
        };
        let keys: Vec<Key> = now
            .iter()
            .filter(|(key, hash)| whole || before.get(*key) != Some(*hash))
            .map(|(key, _)| key.clone())
            .collect();
        let values = rows(place, table, &keys)?;
        let mut changes = Vec::new();
        for key in keys {
            match values.get(&key) {
                Some(values) if let Some(over) = over_limit(values, limit.0) => blob
                    .refused
                    .push(refused(table, key, over, &limit, (&mut baseline, &before))),
                Some(values) => {
                    let columns: Vec<&str> = values.iter().map(String::as_str).collect();
                    changes.push((key.clone(), from(&key), Some(row_hash(&columns)), values));
                }
                // Deleted while the stash read: the next stash carries it.
                None => keep_last(&mut baseline, &before, key),
            }
        }
        let empty = Vec::new();
        if !whole {
            for (key, hash) in &before {
                if !now.contains_key(key) {
                    changes.push((key.clone(), Some(*hash), None, &empty));
                }
            }
        }
        for (key, from, to, values) in changes {
            let mut envelope = Envelope {
                v: VERSION,
                node: settings.node.clone(),
                change: Change {
                    op: op(from, to),
                    table: table.name.clone(),
                    key: key.clone(),
                    from,
                    to,
                    values: values.clone(),
                },
                targets: settings.targets.clone(),
                sunset,
                txn: None,
                key_id: settings.key_id(),
                stamp,
                signature: [0; 32],
            };
            envelope.sign(&settings.key);
            let row = (table.name.clone(), key);
            if !whole {
                device.last_from.insert(row.clone(), from);
            }
            if to.is_none() {
                device.graveyard.insert(row, envelope.clone());
            }
            blob.changes.push(envelope);
        }
        device.baseline.insert(table.name.clone(), baseline);
    }
    if whole {
        blob.graveyard = device
            .graveyard
            .values()
            .filter(|tombstone| settings.now <= tombstone.sunset)
            .cloned()
            .collect();
    }
    blob.sign(&settings.key);
    keep(&blob).map_err(Error::Keep)?;
    device.stashed = true;
    device.save(&*settings.store, settings.now)?;
    Ok(blob)
}

/// Keeps `key`'s last stashed hash in `baseline`, as `before` holds it, so
/// the next stash carries its row.
fn keep_last(baseline: &mut Hashes, before: &Hashes, key: Key) {
    match before.get(&key) {
        Some(hash) => drop(baseline.insert(key, *hash)),
        None => drop(baseline.remove(&key)),
    }
}

/// The row `key` of `table`, refused for its value of `bytes` in column
/// `column`, over the limit of `place`: named, and its last hash kept in
/// `baseline` as `before` holds it, so a later stash carries it once it fits.
fn refused(
    table: &Table,
    key: Key,
    (column, bytes): (usize, u64),
    (limit, place): &(u64, String),
    (baseline, before): (&mut Hashes, &Hashes),
) -> Refused {
    let refused = Refused::new(
        &table.name,
        &key,
        table.columns.get(column).map_or("", String::as_str),
        bytes,
        (*limit, place),
    );
    keep_last(baseline, before, key);
    refused
}
