//! Stash: one envelope per row changed since the device's last stash.

use std::collections::BTreeMap;

use smugglr_core::change::{Change, op};
use smugglr_core::envelope::{Envelope, VERSION};
use smugglr_core::row_hash::row_hash;
use smugglr_memory::Device;
use smugglr_place::{Key, Place};

use crate::{Blob, Error, Settings, hashes, rows, stamp, tables};

/// Stashes `place`: a blob of one signed envelope per row changed since its
/// last stash, a delete for each row deleted since, for `settings.targets`.
/// With `whole`, or when the place never stashed, the blob holds every row
/// and the graveyard instead. The device's hashes are kept as at this stash.
///
/// # Errors
/// When the place cannot be read, a table has no primary key, or what the
/// device keeps cannot be read or written.
pub fn stash(place: &mut dyn Place, settings: &Settings, whole: bool) -> Result<Blob, Error> {
    let mut device = Device::open(&settings.config_dir, place.address())?;
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
    };
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
                Some(values) => {
                    let columns: Vec<&str> = values.iter().map(String::as_str).collect();
                    changes.push((key.clone(), from(&key), Some(row_hash(&columns)), values));
                }
                // Deleted while the stash read: the next stash carries it.
                None => match before.get(&key) {
                    Some(hash) => drop(baseline.insert(key, *hash)),
                    None => drop(baseline.remove(&key)),
                },
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
    device.stashed = true;
    device.save(settings.now)?;
    Ok(blob)
}
