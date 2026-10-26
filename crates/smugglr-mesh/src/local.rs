//! The node's own database: finds the rows changed here by comparing each
//! row's hash with its last hash per key, and applies the changes it
//! receives through smugglr-core's one decision path, written through the
//! place in one transaction per table per batch (FR-SMUGGLR-075).
//!
//! What it keeps per key (the last hash, the last applied `from`, and a
//! delete's tombstone) lives in memory and in a journal in smugglr's own
//! files, to which each change appends only the keys it touched. The journal
//! is rewritten once, when the node starts. The same journal is the node's
//! dropped log: every change it drops, envelope and values, kept so it can
//! be replayed (FR-SMUGGLR-076).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde_json::{Value, json};
use smugglr_core::change::{Change, op};
use smugglr_core::envelope::{self, Arrival, Envelope, Here, VERSION, receive, receive_replayed};
use smugglr_core::hex::{hex, unhex};
use smugglr_core::ignore::Ignore;
use smugglr_core::plan::Hashes;
use smugglr_core::row_hash::row_hash;
use smugglr_core::shape::{Column, Stamp};
use smugglr_memory::Store;
use smugglr_place::{Key, Place, Table};

use crate::{Error, Settings};

/// A table's row, by its table and key.
type Row = (String, Key);

/// What one batch of received changes came to, in the batch's order.
#[derive(Debug, Default)]
pub(crate) struct Applied {
    /// The changes the node now has: applied, or already here.
    pub(crate) has: Vec<[u8; 32]>,
    /// Of those, the ones this batch wrote, to echo.
    pub(crate) wrote: Vec<[u8; 32]>,
    /// The changes that wait for their `from` to appear.
    pub(crate) waiting: Vec<Envelope>,
    /// The changes it stopped or dropped: decided, so acknowledged.
    pub(crate) decided: Vec<[u8; 32]>,
}

/// The node's database and what it keeps per key.
#[derive(Debug)]
pub(crate) struct Local {
    store: std::sync::Arc<dyn Store>,
    journal: String,
    tables: BTreeMap<String, Table>,
    baseline: BTreeMap<String, Hashes>,
    last_from: HashMap<Row, Option<[u8; 16]>>,
    tombstones: HashMap<Row, Envelope>,
    /// The waiting changes the record has named, so each is named once.
    held: HashSet<[u8; 32]>,
    /// The changes stopped or dropped the record has named, likewise.
    named: HashSet<[u8; 32]>,
    /// The dropped log: every change the node dropped, until it is replayed.
    dropped: Vec<Envelope>,
}

impl Local {
    /// The node's database `place`, with what the journal in `folder` keeps.
    /// On the node's first run it notes every row's hash, and so sends none
    /// of the rows already there.
    pub(crate) fn open(
        place: &mut dyn Place,
        settings: &Settings,
        folder: &str,
        now: u64,
    ) -> Result<Self, Error> {
        let mut local = Self {
            store: settings.store.clone(),
            journal: format!("{folder}/rows.jsonl"),
            tables: BTreeMap::new(),
            baseline: BTreeMap::new(),
            last_from: HashMap::new(),
            tombstones: HashMap::new(),
            held: HashSet::new(),
            named: HashSet::new(),
            dropped: Vec::new(),
        };
        local.read_tables(place, settings)?;
        match local.store.read(&local.journal)? {
            Some(text) => {
                for line in text.lines() {
                    if let Ok(value) = serde_json::from_str::<Value>(line) {
                        local.replay(&value);
                    }
                }
                local
                    .tombstones
                    .retain(|_, tombstone| now <= tombstone.sunset);
            }
            None => {
                for table in local.tables.values() {
                    let hashes = place.hashes(table)?.into_iter().collect();
                    local.baseline.insert(table.name.clone(), hashes);
                }
            }
        }
        local.rewrite()?;
        Ok(local)
    }

    /// The place's tables, less those ignored.
    fn read_tables(&mut self, place: &mut dyn Place, settings: &Settings) -> Result<(), Error> {
        let ignore = Ignore::new(
            settings
                .ignore
                .iter()
                .map(String::as_str)
                .chain(place.own_tables().iter().copied())
                .map(str::to_owned),
        );
        let (tables, _) = ignore.split(place.tables()?, |table| &table.name);
        if let Some(table) = tables.iter().find(|table| table.key.is_empty()) {
            return Err(Error::Shape(format!(
                "the table {} in {} has no primary key",
                table.name,
                place.address()
            )));
        }
        self.tables = tables
            .into_iter()
            .map(|table| (table.name.clone(), table))
            .collect();
        Ok(())
    }

    fn replay(&mut self, line: &Value) {
        let row = |value: &Value| -> Option<Row> {
            let table = value.get(0)?.as_str()?.to_owned();
            let key = value
                .get(1)?
                .as_array()?
                .iter()
                .map(|column| column.as_str().map(str::to_owned))
                .collect::<Option<Key>>()?;
            Some((table, key))
        };
        let hash = |value: &Value| -> Option<Option<[u8; 16]>> {
            match value.get(2)? {
                Value::Null => Some(None),
                text => Some(Some(unhex(text.as_str()?)?.try_into().ok()?)),
            }
        };
        if let Some(value) = line.get("b")
            && let (Some((table, key)), Some(hash)) = (row(value), hash(value))
        {
            let hashes = self.baseline.entry(table).or_default();
            match hash {
                Some(hash) => drop(hashes.insert(key, hash)),
                None => drop(hashes.remove(&key)),
            }
        } else if let Some(value) = line.get("f")
            && let (Some(row), Some(from)) = (row(value), hash(value))
        {
            self.last_from.insert(row, from);
        } else if let Some(envelope) = line.get("t").and_then(Envelope::from_json) {
            let row = (envelope.change.table.clone(), envelope.change.key.clone());
            self.tombstones.insert(row, envelope);
        } else if let Some(envelope) = line.get("d").and_then(Envelope::from_json) {
            self.log_dropped(envelope);
        }
    }

    /// Writes the journal anew from what is kept: once, when the node starts.
    fn rewrite(&self) -> Result<(), Error> {
        let mut text = String::new();
        for (table, hashes) in &self.baseline {
            for (key, hash) in hashes {
                text.push_str(&json!({"b": [table, key, hex(hash)]}).to_string());
                text.push('\n');
            }
        }
        for ((table, key), from) in &self.last_from {
            text.push_str(&json!({"f": [table, key, from.map(|f| hex(&f))]}).to_string());
            text.push('\n');
        }
        for tombstone in self.tombstones.values() {
            text.push_str(&json!({"t": tombstone.to_json()}).to_string());
            text.push('\n');
        }
        for envelope in &self.dropped {
            text.push_str(&json!({"d": envelope.to_json()}).to_string());
            text.push('\n');
        }
        Ok(self.store.replace(&self.journal, &text)?)
    }

    /// The journal lines for a row brought from `from` to `to` (an applied
    /// change or the node's own), and its tombstone for a delete.
    fn moved_lines(envelope: &Envelope) -> String {
        let change = &envelope.change;
        let (table, key) = (&change.table, &change.key);
        let mut lines = String::new();
        lines.push_str(&json!({"b": [table, key, change.to.map(|t| hex(&t))]}).to_string());
        lines.push('\n');
        lines.push_str(&json!({"f": [table, key, change.from.map(|f| hex(&f))]}).to_string());
        lines.push('\n');
        if change.to.is_none() {
            lines.push_str(&Self::tombstone_line(envelope));
        }
        lines
    }

    fn tombstone_line(envelope: &Envelope) -> String {
        format!("{}\n", json!({"t": envelope.to_json()}))
    }

    /// Keeps in memory that the row now hashes to `to`, brought from `from`.
    fn remember_move(&mut self, envelope: &Envelope) {
        let change = &envelope.change;
        let (table, key) = (&change.table, &change.key);
        let hashes = self.baseline.entry(table.clone()).or_default();
        match change.to {
            Some(to) => drop(hashes.insert(key.clone(), to)),
            None => drop(hashes.remove(key)),
        }
        self.last_from
            .insert((table.clone(), key.clone()), change.from);
        if change.to.is_none() {
            self.remember_tombstone(envelope);
        }
    }

    fn remember_tombstone(&mut self, envelope: &Envelope) {
        let row = (envelope.change.table.clone(), envelope.change.key.clone());
        self.tombstones.insert(row, envelope.clone());
    }

    /// Keeps `found`, the node's own changes now in its outbox, as the last
    /// hash of each key: written to the journal first, and only then in
    /// memory, so a change that could not be kept is found again.
    pub(crate) fn keep(&mut self, found: &[Envelope]) -> Result<(), Error> {
        let lines: String = found.iter().map(Self::moved_lines).collect();
        self.append(&lines)?;
        for envelope in found {
            self.remember_move(envelope);
        }
        Ok(())
    }

    fn append(&self, lines: &str) -> Result<(), Error> {
        if lines.is_empty() {
            return Ok(());
        }
        Ok(self.store.append(&self.journal, lines)?)
    }

    /// One envelope per row changed here since it was last noted, signed
    /// with `sign`, as many as fit in `budget` bytes as `weigh` weighs each;
    /// the rows left over are found again on a later look. Also whether the
    /// budget left rows over. Nothing is kept until [`Local::keep`], so a
    /// look that fails part way loses no row.
    pub(crate) fn changes(
        &self,
        place: &mut dyn Place,
        settings: &Settings,
        sign: &[u8; 32],
        now: u64,
        budget: u64,
        weigh: impl Fn(&Envelope) -> u64,
    ) -> Result<(Vec<Envelope>, bool), Error> {
        let (mut found, mut spent) = (Vec::new(), 0_u64);
        let mut full = false;
        let tables: Vec<Table> = self.tables.values().cloned().collect();
        'tables: for table in &tables {
            let stamp = stamp(table);
            let now_hashes: Hashes = place.hashes(table)?.into_iter().collect();
            let empty = Hashes::new();
            let before = self.baseline.get(&table.name).unwrap_or(&empty);
            let mut moved: Vec<(Key, Option<[u8; 16]>)> = now_hashes
                .iter()
                .filter(|(key, hash)| before.get(*key) != Some(*hash))
                .map(|(key, _)| (key.clone(), before.get(key).copied()))
                .collect();
            moved.extend(
                before
                    .iter()
                    .filter(|(key, _)| !now_hashes.contains_key(*key))
                    .map(|(key, hash)| (key.clone(), Some(*hash))),
            );
            for chunk in moved.chunks(1_000) {
                let keys: Vec<Key> = chunk.iter().map(|(key, _)| key.clone()).collect();
                let rows: BTreeMap<Key, Vec<String>> = place
                    .rows(table, &keys)?
                    .into_iter()
                    .map(|row| (table.key_of(&row), row))
                    .collect();
                for (key, from) in chunk {
                    let values = rows.get(key).cloned().unwrap_or_default();
                    let to = rows.get(key).map(|values| {
                        let columns: Vec<&str> = values.iter().map(String::as_str).collect();
                        row_hash(&columns)
                    });
                    if to == *from {
                        continue;
                    }
                    let mut envelope = Envelope {
                        v: VERSION,
                        node: settings.node.clone(),
                        change: Change {
                            op: op(*from, to),
                            table: table.name.clone(),
                            key: key.clone(),
                            from: *from,
                            to,
                            values,
                        },
                        targets: Vec::new(),
                        sunset: now.saturating_add(settings.graveyard),
                        txn: None,
                        key_id: smugglr_multi::key_id(sign),
                        stamp,
                        signature: [0; 32],
                    };
                    envelope.sign(sign);
                    let size = weigh(&envelope);
                    if spent + size > budget {
                        full = true;
                        break 'tables;
                    }
                    spent += size;
                    found.push(envelope);
                }
            }
        }
        Ok((found, full))
    }

    /// The row each change's key holds here, as the decision path reads it.
    fn here(
        &self,
        place: &mut dyn Place,
        settings: &Settings,
        envelopes: &[&Envelope],
    ) -> Result<HashMap<Row, Here>, Error> {
        let mut keys: BTreeMap<&str, BTreeSet<Key>> = BTreeMap::new();
        for envelope in envelopes {
            let change = &envelope.change;
            keys.entry(&change.table)
                .or_default()
                .insert(change.key.clone());
        }
        let mut here = HashMap::new();
        for (name, keys) in keys {
            let Some(table) = self.tables.get(name) else {
                continue;
            };
            let steering = settings
                .steering
                .column(name)
                .and_then(|column| table.columns.iter().position(|c| c == column));
            let keys: Vec<Key> = keys.into_iter().collect();
            let rows: BTreeMap<Key, Vec<String>> = place
                .rows(table, &keys)?
                .into_iter()
                .map(|row| (table.key_of(&row), row))
                .collect();
            for key in keys {
                let row = rows.get(&key);
                let hash = row.map(|values| {
                    let columns: Vec<&str> = values.iter().map(String::as_str).collect();
                    row_hash(&columns)
                });
                let at = (name.to_owned(), key);
                here.insert(
                    at.clone(),
                    Here {
                        hash,
                        last_from: self.last_from.get(&at).copied(),
                        steering: steering.and_then(|i| Some((i, row?.get(i)?.clone()))),
                        tombstone: self.tombstones.get(&at).cloned(),
                    },
                );
            }
        }
        Ok(here)
    }

    /// Decides `envelopes`, all verified with `sign` and of tables whose
    /// shape matches, through the one decision path, and writes those that
    /// apply, one transaction per table.
    pub(crate) fn apply(
        &mut self,
        place: &mut dyn Place,
        settings: &Settings,
        sign: &[u8; 32],
        envelopes: &[Envelope],
        now: u64,
    ) -> Result<Applied, Error> {
        self.decide(place, settings, sign, envelopes, now, false)
    }

    /// Replays the change the dropped log holds under the change hash
    /// `hash` (FR-SMUGGLR-076): re-signed with `sign` and a fresh sunset, it
    /// leaves the log and is decided like any change, overriding its drop;
    /// dropped again, it goes back in. What it writes is not noted as the
    /// row's last hash, so the node's next run finds it as its own change
    /// and sends it to its peers; one that waits for its `from` stays in the
    /// log. `None` when the log holds no such change.
    pub(crate) fn replay_dropped(
        &mut self,
        place: &mut dyn Place,
        settings: &Settings,
        sign: &[u8; 32],
        hash: &[u8; 32],
        now: u64,
    ) -> Result<Option<Applied>, Error> {
        let sunset = now.saturating_add(settings.graveyard);
        let Some(resent) = envelope::replay(&self.dropped, hash, sunset, sign) else {
            return Ok(None);
        };
        let at = self
            .dropped
            .iter()
            .position(|logged| &logged.signature == hash);
        let logged = at.map(|at| self.dropped.remove(at));
        self.named.remove(hash);
        let applied = self.decide(place, settings, sign, &[resent], now, true)?;
        // A replay that waits has no one to send it again: it stays logged.
        if !applied.waiting.is_empty()
            && let Some(logged) = logged
        {
            self.log_dropped(logged);
        }
        self.rewrite()?;
        Ok(Some(applied))
    }

    fn decide(
        &mut self,
        place: &mut dyn Place,
        settings: &Settings,
        sign: &[u8; 32],
        envelopes: &[Envelope],
        now: u64,
        replayed: bool,
    ) -> Result<Applied, Error> {
        let refs: Vec<&Envelope> = envelopes.iter().collect();
        let here = self.here(place, settings, &refs)?;
        let here = |change: &Change| {
            here.get(&(change.table.clone(), change.key.clone()))
                .cloned()
                .unwrap_or_default()
        };
        let arrivals = if replayed {
            receive_replayed(envelopes, sign, settings.sync_mode, now, here)
        } else {
            receive(envelopes, sign, settings.sync_mode, now, here)
        };
        let mut applied = Applied::default();
        let (mut moves, mut tombstones): (Vec<Envelope>, Vec<Envelope>) = (Vec::new(), Vec::new());
        let mut dropped = Vec::new();
        let mut writing: BTreeMap<String, Vec<&Envelope>> = BTreeMap::new();
        for (envelope, arrival) in envelopes.iter().zip(arrivals) {
            let change = &envelope.change;
            match arrival {
                Arrival::Apply => writing
                    .entry(change.table.clone())
                    .or_default()
                    .push(envelope),
                Arrival::Skip => {
                    if change.to.is_none() {
                        tombstones.push(envelope.clone());
                    }
                    applied.has.push(envelope.signature);
                }
                Arrival::Hold => {
                    let here = here(change).hash;
                    self.hold(
                        envelope,
                        &format!(
                            "its from {} matches nothing here (the row here is {})",
                            short(change.from),
                            short(here)
                        ),
                    );
                    applied.waiting.push(envelope.clone());
                }
                // Decided, so acknowledged, and named once however often
                // a copy arrives (FR-SMUGGLR-078 rev 24).
                Arrival::Stop => {
                    self.name(
                        envelope,
                        &format!(
                            "stopped {} {:?}: changed here and in a change from {}",
                            change.table, change.key, envelope.node
                        ),
                    );
                    applied.decided.push(envelope.signature);
                }
                Arrival::Dropped(reason) => {
                    if self.name(
                        envelope,
                        &format!(
                            "dropped a change to {} {:?} from {}: {} (change {})",
                            change.table,
                            change.key,
                            envelope.node,
                            reason.words(),
                            hex(&envelope.signature)
                        ),
                    ) && self.log_dropped(envelope.clone())
                    {
                        dropped.push(envelope.clone());
                    }
                    applied.decided.push(envelope.signature);
                }
                Arrival::Refused => eprintln!(
                    "smugglr mesh: refused a change to {} from {}: key {} is not one this node holds",
                    change.table, envelope.node, envelope.key_id
                ),
            }
        }
        for (name, envelopes) in writing {
            self.write(place, &name, &envelopes, &mut applied, &mut moves)?;
        }
        if replayed {
            // Left for the next run to find as the node's own change.
            return Ok(applied);
        }
        // The journal first, then memory, as for the node's own changes.
        let lines: String = moves
            .iter()
            .map(Self::moved_lines)
            .chain(tombstones.iter().map(Self::tombstone_line))
            .chain(
                dropped
                    .iter()
                    .map(|envelope| format!("{}\n", json!({"d": envelope.to_json()}))),
            )
            .collect();
        self.append(&lines)?;
        for envelope in &moves {
            self.remember_move(envelope);
        }
        for envelope in &tombstones {
            self.remember_tombstone(envelope);
        }
        Ok(applied)
    }

    /// Writes `envelopes`, all to the table `name`, in one transaction, and
    /// notes in `applied` and `moves` what landed.
    fn write(
        &mut self,
        place: &mut dyn Place,
        name: &str,
        envelopes: &[&Envelope],
        applied: &mut Applied,
        moves: &mut Vec<Envelope>,
    ) -> Result<(), Error> {
        let Some(table) = self.tables.get(name).cloned() else {
            return Ok(());
        };
        let changes: Vec<Change> = envelopes.iter().map(|e| e.change.clone()).collect();
        let landed: BTreeSet<Key> = place.write(&table, &changes)?.into_iter().collect();
        let read_back = read_back(place, &table, &landed)?;
        let (mut makers, mut keys, mut count) = (BTreeSet::new(), BTreeSet::new(), 0);
        for &envelope in envelopes {
            if landed.contains(&envelope.change.key) {
                let now_here = read_back.get(&envelope.change.key).copied();
                if now_here != envelope.change.to {
                    println!(
                        "smugglr mesh: a change to {name} {:?} from {} reads back as {}, not {}",
                        envelope.change.key,
                        envelope.node,
                        short(now_here),
                        short(envelope.change.to)
                    );
                }
                self.held.remove(&envelope.signature);
                moves.push(envelope.clone());
                applied.has.push(envelope.signature);
                applied.wrote.push(envelope.signature);
                makers.insert(envelope.node.as_str());
                keys.insert(envelope.key_id.as_str());
                count += 1;
            } else {
                self.hold(
                    envelope,
                    "the row moved here between the read and the write",
                );
                applied.waiting.push(envelope.clone());
            }
        }
        if count > 0 {
            println!(
                "smugglr mesh: applied {count} {} to {name} from {}, key {}",
                if count == 1 { "change" } else { "changes" },
                makers.into_iter().collect::<Vec<_>>().join(", "),
                keys.into_iter().collect::<Vec<_>>().join(", "),
            );
        }
        Ok(())
    }

    /// Names in the record, once, a change that waits, and why.
    fn hold(&mut self, envelope: &Envelope, why: &str) {
        if self.held.insert(envelope.signature) {
            println!(
                "smugglr mesh: holding a change to {} {:?} from {}: {why}",
                envelope.change.table, envelope.change.key, envelope.node
            );
        }
    }

    /// Prints `line` for `envelope`, a change decided (stopped or dropped),
    /// in the record, once however often a copy of it arrives; whether it
    /// printed.
    fn name(&mut self, envelope: &Envelope, line: &str) -> bool {
        let first = self.named.insert(envelope.signature);
        if first {
            println!("smugglr mesh: {line}");
        }
        first
    }

    /// Keeps `envelope` in the dropped log, once; whether it was new there.
    fn log_dropped(&mut self, envelope: Envelope) -> bool {
        let new = self
            .dropped
            .iter()
            .all(|logged| logged.signature != envelope.signature);
        if new {
            self.dropped.push(envelope);
        }
        new
    }

    /// Whether `envelope`'s table is one this node keeps, with the same
    /// shape.
    pub(crate) fn same_shape(&self, envelope: &Envelope) -> bool {
        self.tables
            .get(&envelope.change.table)
            .is_some_and(|table| stamp(table) == envelope.stamp)
    }
}

/// The hash each of `keys` reads back as, after a write.
fn read_back(
    place: &mut dyn Place,
    table: &Table,
    keys: &BTreeSet<Key>,
) -> Result<HashMap<Key, [u8; 16]>, Error> {
    let keys: Vec<Key> = keys.iter().cloned().collect();
    Ok(place
        .rows(table, &keys)?
        .into_iter()
        .map(|row| {
            let columns: Vec<&str> = row.iter().map(String::as_str).collect();
            (table.key_of(&row), row_hash(&columns))
        })
        .collect())
}

/// A row hash as the record shows it: its first 8 bytes, or "absent".
fn short(hash: Option<[u8; 16]>) -> String {
    hash.map_or_else(|| "absent".to_owned(), |hash| hex(&hash[..8]))
}

/// A table's shape stamp.
fn stamp(table: &Table) -> Stamp {
    let columns: Vec<Column> = table
        .columns
        .iter()
        .enumerate()
        .map(|(i, name)| Column {
            name: name.clone(),
            kind: table.types.get(i).cloned().unwrap_or_default(),
        })
        .collect();
    Stamp::of(&columns)
}
