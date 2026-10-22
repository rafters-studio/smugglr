//! Retrieve: verify blobs and apply them by the envelope rules, in any
//! order.

use std::collections::{BTreeMap, BTreeSet};

use smugglr_core::change::{Change, op};
use smugglr_core::envelope::{Arrival, Envelope, Here, receive};
use smugglr_core::plan::{Hashes, Side, SyncMode};
use smugglr_core::record::{
    self, CHANGED_HERE_AND_IN_BLOB, Dropped, EDIT_VERSUS_DELETE, Key as Named, Moves, PAST_SUNSET,
    REFUSED, Record, WAITING,
};
use smugglr_core::shape::Mismatch;
use smugglr_memory::{Device, Row};
use smugglr_place::{Key, Place, Table};

use crate::{Blob, Error, Settings, columns, hashes, rows, stamp, tables};

/// Retrieves `blobs` into `place`: each blob's header stamps are checked
/// against the place's tables first, and any difference stops the run before
/// a change is applied. Whole blobs are taken first: every row not changed
/// here since this device's last stash becomes the blob's, and the blob's
/// graveyard joins the device's. Every other blob's envelopes are then
/// received together, in turn until no more apply, so blobs apply in any
/// order. The record is done when every change was applied, skipped or
/// dropped at its sunset or the graveyard. With one blob, it names the
/// blob's targets that remain: its targets less `crossed_off` and this
/// device.
///
/// # Errors
/// When the place cannot be read or written, a table has no primary key,
/// or what the device keeps cannot be read or written.
pub fn retrieve(
    place: &mut dyn Place,
    blobs: &[Blob],
    settings: &Settings,
    crossed_off: &[String],
) -> Result<Record, Error> {
    let remaining = match blobs {
        [blob] => Some(
            blob.targets
                .iter()
                .filter(|target| **target != settings.node && !crossed_off.contains(target))
                .cloned()
                .collect(),
        ),
        _ => None,
    };
    let (mut accepted, mut refused) = (Vec::new(), Vec::new());
    for blob in blobs {
        match refusal(blob, settings) {
            Some(why) => refused.push((blob, why)),
            None => accepted.push(blob.clone()),
        }
    }
    let blobs = accepted.as_slice();
    let (tables, ignored) = tables(place, settings)?;
    if let Some(mismatch) = mismatch(blobs, &tables, &ignored) {
        let mut record = Record::shape_mismatch(mismatch, ignored, false);
        record.targets_remaining = remaining;
        return Ok(record);
    }
    let mut run = Run {
        device: Device::open(&settings.config_dir, place.address())?,
        tables: tables
            .into_iter()
            .map(|table| (table.name.clone(), table))
            .collect(),
        ..Run::default()
    };
    for (blob, why) in refused {
        run.refuse(blob, why);
    }
    for blob in blobs.iter().filter(|blob| blob.whole) {
        run.take_whole(place, blob, settings)?;
    }
    let envelopes = blobs
        .iter()
        .filter(|blob| !blob.whole)
        .flat_map(|blob| &blob.changes)
        .filter(|envelope| run.tables.contains_key(&envelope.change.table))
        .cloned()
        .collect();
    run.receive_all(place, envelopes, settings)?;
    let mut done = BTreeMap::new();
    let written: Vec<String> = run.expected.keys().cloned().collect();
    for name in written {
        let table = &run.tables[&name];
        let read = hashes(place, table)?;
        let unmatched: Vec<Key> = run.expected[&name]
            .iter()
            .filter(|(key, to)| read.get(*key) != to.as_ref())
            .map(|(key, _)| key.clone())
            .collect();
        if !unmatched.is_empty() {
            done.insert(name, record::not_read_back(unmatched));
        }
    }
    for (name, moves) in std::mem::take(&mut run.moves) {
        done.entry(name).or_insert(record::Table::Moved(moves));
    }
    for (name, named) in std::mem::take(&mut run.named) {
        let why = named.first().map_or(WAITING, |(_, why)| why).to_owned();
        let rows = named
            .into_iter()
            .filter(|(key, _)| !key.is_empty())
            .map(|(key, _)| Named(key))
            .collect();
        let stopped = record::Table::Stopped {
            rows,
            not_read_back: Vec::new(),
            why,
        };
        done.entry(name)
            .and_modify(|table| {
                if !matches!(table, record::Table::Stopped { .. }) {
                    *table = stopped.clone();
                }
            })
            .or_insert(stopped);
    }
    run.device.save(settings.now)?;
    let mut record = Record::new(done, ignored, false, None);
    record.dropped = run.dropped;
    record.targets_remaining = remaining;
    Ok(record)
}

/// Why `blob` is refused whole, with nothing of it applied: its header's
/// signature fails, or it is whole and one of its envelopes fails its
/// signature, or the blob or one of its envelopes is past its sunset
/// (FR-SMUGGLR-076), so an old whole blob, even an empty one, cannot roll a
/// device back. `None` when it is not refused.
fn refusal(blob: &Blob, settings: &Settings) -> Option<&'static str> {
    if !blob.verify(&settings.key) {
        return Some(REFUSED);
    }
    if !blob.whole {
        return None;
    }
    let mut envelopes = blob.changes.iter().chain(&blob.graveyard);
    if envelopes
        .clone()
        .any(|envelope| !envelope.verify(&settings.key))
    {
        Some(REFUSED)
    } else if settings.now > blob.sunset || envelopes.any(|envelope| settings.now > envelope.sunset)
    {
        Some(PAST_SUNSET)
    } else {
        None
    }
}

/// The first table whose stamp in a blob's header differs from the place's,
/// or that the place does not hold.
fn mismatch(blobs: &[Blob], tables: &[Table], ignored: &BTreeSet<String>) -> Option<Mismatch> {
    blobs
        .iter()
        .flat_map(|blob| &blob.stamps)
        .filter(|(name, _)| !ignored.contains(*name))
        .find_map(
            |(name, arrived)| match tables.iter().find(|table| &table.name == name) {
                None => Some(Mismatch::Table {
                    table: name.clone(),
                    only_in: Side::Source,
                }),
                Some(table) if stamp(table) != *arrived => Some(Mismatch::Stamps {
                    table: name.clone(),
                    columns: columns(table),
                    stamp: stamp(table),
                    arrived: *arrived,
                }),
                Some(_) => None,
            },
        )
}

/// What a retrieve has done so far.
#[derive(Debug, Default)]
struct Run {
    device: Device,
    tables: BTreeMap<String, Table>,
    /// Each table's hashes here, as read and then as written.
    here: BTreeMap<String, Hashes>,
    moves: BTreeMap<String, Moves>,
    /// Rows not done, by table, each with why.
    named: BTreeMap<String, Vec<(Key, &'static str)>>,
    /// Each row written and the hash it must read back with.
    expected: BTreeMap<String, BTreeMap<Key, Option<[u8; 16]>>>,
    dropped: Vec<Dropped>,
}

impl Run {
    /// Reads the hashes of `name` here, once.
    fn load(&mut self, place: &mut dyn Place, name: &str) -> Result<(), Error> {
        if !self.here.contains_key(name)
            && let Some(table) = self.tables.get(name)
        {
            let read = hashes(place, table)?;
            self.here.insert(name.to_owned(), read);
        }
        Ok(())
    }

    /// Names every table a refused blob holds, with each of its rows.
    fn refuse(&mut self, blob: &Blob, why: &'static str) {
        let names: Vec<String> = blob
            .stamps
            .keys()
            .filter(|name| self.tables.contains_key(*name))
            .cloned()
            .collect();
        for name in names {
            self.name(&name, &[], why);
            for envelope in blob.changes.iter().filter(|e| e.change.table == name) {
                self.name(&name, &envelope.change.key, why);
            }
        }
    }

    fn name(&mut self, table: &str, key: &[String], why: &'static str) {
        self.named
            .entry(table.to_owned())
            .or_default()
            .push((key.to_vec(), why));
    }

    /// Writes `changes` to `name`, and returns those written.
    fn write(
        &mut self,
        place: &mut dyn Place,
        name: &str,
        changes: Vec<Change>,
    ) -> Result<Vec<Change>, Error> {
        if changes.is_empty() {
            return Ok(changes);
        }
        let Some(table) = self.tables.get(name) else {
            return Ok(Vec::new());
        };
        let written = place.write(table, &changes)?;
        let mut landed = Vec::new();
        for change in changes {
            if !written.contains(&change.key) {
                self.name(name, &change.key, WAITING);
                continue;
            }
            let here = self.here.entry(name.to_owned()).or_default();
            match change.to {
                Some(to) => drop(here.insert(change.key.clone(), to)),
                None => drop(here.remove(&change.key)),
            }
            let moves = self.moves.entry(name.to_owned()).or_default();
            if change.to.is_some() {
                moves.to_target += 1;
            } else {
                moves.deleted_in_target.push(Named(change.key.clone()));
            }
            self.expected
                .entry(name.to_owned())
                .or_default()
                .insert(change.key.clone(), change.to);
            landed.push(change);
        }
        Ok(landed)
    }

    /// Takes a whole blob: every row not changed here since the last stash
    /// becomes the blob's, and its graveyard joins the device's. This
    /// device's own changes since its last stash stay, for its next stash.
    fn take_whole(
        &mut self,
        place: &mut dyn Place,
        blob: &Blob,
        settings: &Settings,
    ) -> Result<(), Error> {
        let mut rows: BTreeMap<&str, BTreeMap<&Key, &Envelope>> = BTreeMap::new();
        for envelope in &blob.changes {
            let change = &envelope.change;
            if !self.tables.contains_key(&change.table) {
                continue;
            }
            // Every envelope of a whole blob verified before it was taken.
            rows.entry(&change.table)
                .or_default()
                .insert(&change.key, envelope);
        }
        let names: Vec<String> = blob
            .stamps
            .keys()
            .filter(|name| self.tables.contains_key(*name))
            .cloned()
            .collect();
        for name in names {
            self.load(place, &name)?;
            let here = self.here.get(&name).cloned().unwrap_or_default();
            let at_stash = self.device.baseline.remove(&name).unwrap_or_default();
            let mut baseline = at_stash.clone();
            let own = |key: &Key| here.get(key) != at_stash.get(key);
            let theirs = rows.remove(name.as_str()).unwrap_or_default();
            let mut changes = Vec::new();
            for (key, envelope) in &theirs {
                let (from, to) = (here.get(*key).copied(), envelope.change.to);
                if !own(key) && from != to {
                    changes.push(Change {
                        op: op(from, to),
                        from,
                        ..envelope.change.clone()
                    });
                }
            }
            for (key, hash) in &here {
                if !own(key) && !theirs.contains_key(key) {
                    changes.push(Change {
                        op: op(Some(*hash), None),
                        table: name.clone(),
                        key: key.clone(),
                        from: Some(*hash),
                        to: None,
                        values: Vec::new(),
                    });
                }
            }
            let landed: BTreeSet<Key> = self
                .write(place, &name, changes)?
                .into_iter()
                .map(|change| change.key)
                .collect();
            for key in here.keys().chain(theirs.keys().copied()) {
                if own(key) {
                    continue;
                }
                let to = theirs.get(key).and_then(|envelope| envelope.change.to);
                let unchanged = here.get(key).copied() == to;
                if !unchanged && !landed.contains(key) {
                    continue;
                }
                self.device.last_from.remove(&(name.clone(), key.clone()));
                match to {
                    Some(to) => drop(baseline.insert(key.clone(), to)),
                    None => drop(baseline.remove(key)),
                }
            }
            self.device.baseline.insert(name, baseline);
        }
        for tombstone in &blob.graveyard {
            let change = &tombstone.change;
            if self.tables.contains_key(&change.table)
                && settings.now <= tombstone.sunset
                && tombstone.verify(&settings.key)
            {
                self.device
                    .graveyard
                    .entry((change.table.clone(), change.key.clone()))
                    .or_insert_with(|| tombstone.clone());
            }
        }
        self.device.stashed = true;
        Ok(())
    }

    /// Receives `pending` together, writes what applies, and receives what
    /// waits again until nothing more applies.
    fn receive_all(
        &mut self,
        place: &mut dyn Place,
        mut pending: Vec<Envelope>,
        settings: &Settings,
    ) -> Result<(), Error> {
        while !pending.is_empty() {
            let names: BTreeSet<String> = pending
                .iter()
                .map(|envelope| envelope.change.table.clone())
                .collect();
            for name in &names {
                self.load(place, name)?;
            }
            let steering = self.steering(place, &pending, settings)?;
            let arrivals = receive(
                &pending,
                &settings.key,
                settings.sync_mode,
                settings.now,
                |change| self.arrives_at(change, &steering),
            );
            let mut applying: BTreeMap<String, Vec<&Envelope>> = BTreeMap::new();
            let mut waiting = Vec::new();
            for (envelope, arrival) in pending.iter().zip(arrivals) {
                let change = &envelope.change;
                let row = (change.table.clone(), change.key.clone());
                match arrival {
                    Arrival::Apply => applying
                        .entry(change.table.clone())
                        .or_default()
                        .push(envelope),
                    Arrival::Skip => {
                        if change.to.is_none() {
                            self.device.graveyard.insert(row, envelope.clone());
                        }
                    }
                    Arrival::Hold => waiting.push(envelope.clone()),
                    Arrival::Stop => {
                        let gone = self
                            .here
                            .get(&row.0)
                            .is_none_or(|h| !h.contains_key(&row.1));
                        let why = if gone {
                            EDIT_VERSUS_DELETE
                        } else {
                            CHANGED_HERE_AND_IN_BLOB
                        };
                        self.name(&row.0, &row.1, why);
                    }
                    Arrival::Dropped(reason) => self.dropped.push(Dropped::new(envelope, reason)),
                    Arrival::Refused => self.name(&row.0, &row.1, REFUSED),
                }
            }
            let mut applied = false;
            for (name, envelopes) in applying {
                let before = self.here.get(&name).cloned().unwrap_or_default();
                let changes = envelopes.iter().map(|e| e.change.clone()).collect();
                let landed = self.write(place, &name, changes)?;
                for envelope in envelopes {
                    let change = &envelope.change;
                    if !landed.iter().any(|landed| landed == change) {
                        continue;
                    }
                    applied = true;
                    self.applied(&name, envelope, before.get(&change.key).copied());
                }
            }
            if !applied {
                for envelope in &waiting {
                    self.name(&envelope.change.table, &envelope.change.key, WAITING);
                }
                break;
            }
            pending = waiting;
        }
        Ok(())
    }

    /// Keeps what an applied envelope leaves: the key's last applied `from`,
    /// its tombstone, and, when the row had not changed here since the last
    /// stash, its hash at that stash, so the next stash does not carry it
    /// back.
    fn applied(&mut self, name: &str, envelope: &Envelope, before: Option<[u8; 16]>) {
        let change = &envelope.change;
        let row: Row = (name.to_owned(), change.key.clone());
        let baseline = self.device.baseline.entry(name.to_owned()).or_default();
        if baseline.get(&change.key).copied() == before {
            match change.to {
                Some(to) => drop(baseline.insert(change.key.clone(), to)),
                None => drop(baseline.remove(&change.key)),
            }
        }
        self.device.last_from.insert(row.clone(), change.from);
        if change.to.is_none() {
            self.device.graveyard.insert(row, envelope.clone());
        }
    }

    /// The row a change arrives at: its hash here; the `from` of this
    /// device's own change since its last stash, or else of the last change
    /// applied here; its steering value; and its tombstone.
    fn arrives_at(&self, change: &Change, steering: &BTreeMap<Row, (usize, String)>) -> Here {
        let row: Row = (change.table.clone(), change.key.clone());
        let hash = self
            .here
            .get(&change.table)
            .and_then(|here| here.get(&change.key))
            .copied();
        let at_stash = self
            .device
            .baseline
            .get(&change.table)
            .and_then(|baseline| baseline.get(&change.key))
            .copied();
        let last_from = if self.device.stashed && hash != at_stash {
            Some(at_stash)
        } else {
            self.device.last_from.get(&row).copied()
        };
        Here {
            hash,
            last_from,
            steering: steering.get(&row).cloned(),
            tombstone: self.device.graveyard.get(&row).cloned(),
        }
    }

    /// Under `--sync-mode last`, each pending row's steering column position
    /// and its value here.
    fn steering(
        &self,
        place: &mut dyn Place,
        pending: &[Envelope],
        settings: &Settings,
    ) -> Result<BTreeMap<Row, (usize, String)>, Error> {
        let mut steering = BTreeMap::new();
        if settings.sync_mode != SyncMode::Last {
            return Ok(steering);
        }
        let mut keys: BTreeMap<&str, Vec<Key>> = BTreeMap::new();
        for envelope in pending {
            let change = &envelope.change;
            keys.entry(&change.table)
                .or_default()
                .push(change.key.clone());
        }
        for (name, keys) in keys {
            let Some(table) = self.tables.get(name) else {
                continue;
            };
            let Some(at) = settings
                .steering
                .column(name)
                .and_then(|column| table.columns.iter().position(|c| c == column))
            else {
                continue;
            };
            for (key, row) in rows(place, table, &keys)? {
                if let Some(value) = row.get(at) {
                    steering.insert((name.to_owned(), key), (at, value.clone()));
                }
            }
        }
        Ok(steering)
    }
}
