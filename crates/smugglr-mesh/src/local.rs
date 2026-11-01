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
use smugglr_core::record::Refused;
use smugglr_core::row_hash::row_hash;
use smugglr_core::shape::{Column, Stamp};
use smugglr_memory::Store;
use smugglr_place::{Key, Place, Table};

use crate::{Error, Settings};

/// A table's row, by its table and key.
type Row = (String, Key);

/// A change to write, and the hash the row must hold for it to land.
type Writing<'a> = (&'a Envelope, Option<[u8; 16]>);

/// What a look for the node's own changes found.
#[derive(Debug, Default)]
pub(crate) struct Changes {
    /// The changes to send, as many as fit the budget.
    pub(crate) found: Vec<Envelope>,
    /// The budget left rows over.
    pub(crate) full: bool,
}

/// The value limit of the node whose database is `place` (FR-SMUGGLR-071):
/// the place's own, lowered by the `max_value` setting, and the place's
/// address.
pub(crate) fn limit(place: &dyn Place, settings: &Settings) -> (u64, String) {
    (
        place.value_limit().min(settings.max_value),
        place.address().to_owned(),
    )
}

/// Whether `envelope`, a change to `table`, holds a value over `limit`, the
/// value limit of the place it names; the record names it when it does.
pub(crate) fn refuse(envelope: &Envelope, table: &Table, limit: (u64, &str)) -> bool {
    let Some(refused) = oversized(envelope, table, limit) else {
        return false;
    };
    println!("smugglr mesh: {refused}, from {}", envelope.node);
    true
}

/// The refusal of `envelope`, a change to `table`, when it holds a value
/// over `limit`, the value limit of the place it names.
fn oversized(envelope: &Envelope, table: &Table, (limit, place): (u64, &str)) -> Option<Refused> {
    let change = &envelope.change;
    Refused::find(
        &change.table,
        &table.columns,
        &change.key,
        &change.values,
        (limit, place),
    )
}

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
    /// The node's own rows refused for a value over the value limit, at the
    /// hash the record named each at, so each is named once.
    oversized: HashSet<(Row, Option<[u8; 16]>)>,
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
            oversized: HashSet::new(),
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
    /// A change with a value over the value limit (FR-SMUGGLR-071) is
    /// named in the record once and not sent, and its key's last hash stays
    /// as it was, so the next edit that fits starts from a hash its peers
    /// hold.
    pub(crate) fn changes(
        &mut self,
        place: &mut dyn Place,
        settings: &Settings,
        sign: &[u8; 32],
        now: u64,
        budget: u64,
        weigh: impl Fn(&Envelope) -> u64,
    ) -> Result<Changes, Error> {
        let (most, address) = limit(place, settings);
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
                    if let Some(refused) = oversized(&envelope, table, (most, &address)) {
                        let row = (table.name.clone(), key.clone());
                        if self.oversized.insert((row, to)) {
                            println!("smugglr mesh: {refused}, from {}", envelope.node);
                        }
                        continue;
                    }
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
        Ok(Changes { found, full })
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
        let mut applying = Vec::new();
        for (envelope, arrival) in envelopes.iter().zip(arrivals) {
            let change = &envelope.change;
            match arrival {
                Arrival::Apply => applying.push(envelope),
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
        for (name, envelopes) in guarded(applying, here) {
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

    /// Writes `envelopes`, all to the table `name`, in one transaction, each
    /// over the row as its guard reads, and notes in `applied` and `moves`
    /// what landed.
    fn write(
        &mut self,
        place: &mut dyn Place,
        name: &str,
        envelopes: &[Writing],
        applied: &mut Applied,
        moves: &mut Vec<Envelope>,
    ) -> Result<(), Error> {
        let Some(table) = self.tables.get(name).cloned() else {
            return Ok(());
        };
        let changes: Vec<Change> = envelopes
            .iter()
            .map(|(envelope, guard)| Change {
                op: op(*guard, envelope.change.to),
                from: *guard,
                ..envelope.change.clone()
            })
            .collect();
        let landed: BTreeSet<Key> = place.write(&table, &changes)?.into_iter().collect();
        let read_back = read_back(place, &table, &landed)?;
        for envelope in misread(envelopes, &landed, &read_back) {
            println!(
                "smugglr mesh: a change to {name} {:?} from {} reads back as {}, not {}",
                envelope.change.key,
                envelope.node,
                short(read_back.get(&envelope.change.key).copied()),
                short(envelope.change.to)
            );
        }
        let (mut makers, mut keys, mut count) = (BTreeSet::new(), BTreeSet::new(), 0);
        for &(envelope, _) in envelopes {
            if landed.contains(&envelope.change.key) {
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

    /// The table `name` as this node keeps it.
    pub(crate) fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
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

/// `applying`, in order and by table, each with the hash its row must hold
/// for it to land: the row as `here` reads it, or as the change before it
/// leaves it. A conflict `--sync-mode last` keeps is written over the row
/// here, which is not at the change's `from`; a row that moves after the read
/// still fails the write's guard.
fn guarded<'a>(
    applying: Vec<&'a Envelope>,
    here: impl Fn(&Change) -> Here,
) -> BTreeMap<String, Vec<Writing<'a>>> {
    let mut at: HashMap<Row, Option<[u8; 16]>> = HashMap::new();
    let mut writing: BTreeMap<String, Vec<Writing<'a>>> = BTreeMap::new();
    for envelope in applying {
        let change = &envelope.change;
        let row = (change.table.clone(), change.key.clone());
        let guard = at.get(&row).copied().unwrap_or_else(|| here(change).hash);
        at.insert(row, change.to);
        writing
            .entry(change.table.clone())
            .or_default()
            .push((envelope, guard));
    }
    writing
}

/// The changes among `envelopes` whose rows do not read as they should, of
/// the keys that `landed`. The read comes after the whole burst, so only a
/// key's last change should read back as its `to`.
fn misread<'a>(
    envelopes: &[Writing<'a>],
    landed: &BTreeSet<Key>,
    read_back: &HashMap<Key, [u8; 16]>,
) -> Vec<&'a Envelope> {
    let last: HashMap<&Key, &'a Envelope> = envelopes
        .iter()
        .map(|&(envelope, _)| (&envelope.change.key, envelope))
        .collect();
    last.into_iter()
        .filter(|(key, envelope)| {
            landed.contains(*key) && read_back.get(*key).copied() != envelope.change.to
        })
        .map(|(_, envelope)| envelope)
        .collect()
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rusqlite::Connection;
    use smugglr_core::change::{Change, op};
    use smugglr_core::envelope::{Envelope, VERSION};
    use smugglr_core::plan::SyncMode;
    use smugglr_core::row_hash::row_hash;
    use smugglr_core::shape::Stamp;
    use smugglr_core::steering::Steering;
    use smugglr_memory::Files;
    use smugglr_place::{LocalFile, Place};

    use super::{Applied, Local, misread, stamp};
    use crate::{BACKPRESSURE, Error, K, Settings};

    const SIGN: [u8; 32] = [7; 32];
    const NOW: u64 = 1_000;
    const ID: &str = "'1'";

    static NEXT: AtomicUsize = AtomicUsize::new(0);

    /// A row's `quote()` text: its key, its body and its steering value.
    fn row(body: &str, updated: &str) -> Vec<String> {
        vec![ID.to_owned(), format!("'{body}'"), format!("'{updated}'")]
    }

    fn hash(values: &[String]) -> [u8; 16] {
        let columns: Vec<&str> = values.iter().map(String::as_str).collect();
        row_hash(&columns)
    }

    /// A node whose database holds one row at `row("f", "1")`, and has noted
    /// it, under `--sync-mode last` steered by `updated`.
    struct Node {
        place: LocalFile,
        settings: Settings,
        local: Local,
        path: std::path::PathBuf,
    }

    impl Node {
        fn new() -> Result<Self, Box<dyn std::error::Error>> {
            let dir = std::env::temp_dir().join(format!(
                "smugglr-mesh-local-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir)?;
            let path = dir.join("node.db");
            Connection::open(&path)?.execute_batch(
                "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT, updated TEXT);
                 INSERT INTO notes VALUES ('1', 'f', '1');",
            )?;
            let settings = Settings {
                store: Arc::new(Files::new(dir.join("kept"))),
                keys: vec![[1; 32]],
                node: "here".to_owned(),
                port: 0,
                graveyard: 600,
                sync_mode: SyncMode::Last,
                steering: Steering {
                    all: Some("updated".to_owned()),
                    ..Steering::default()
                },
                ignore: Vec::new(),
                k: K,
                backpressure: BACKPRESSURE,
                peers: Vec::new(),
                max_value: u64::MAX,
            };
            let mut place = LocalFile::open(&path)?;
            let local = Local::open(&mut place, &settings, "node", NOW)?;
            Ok(Self {
                place,
                settings,
                local,
                path,
            })
        }

        /// A program on this machine edits the row, and the node finds it.
        fn edit_here(
            &mut self,
            body: &str,
            updated: &str,
        ) -> Result<(), Box<dyn std::error::Error>> {
            self.write_outside(body, updated)?;
            let found =
                self.local
                    .changes(&mut self.place, &self.settings, &SIGN, NOW, u64::MAX, |_| 0)?;
            self.local.keep(&found.found)?;
            Ok(())
        }

        fn write_outside(&self, body: &str, updated: &str) -> rusqlite::Result<usize> {
            Connection::open(&self.path)?
                .execute("UPDATE notes SET body = ?1, updated = ?2", [body, updated])
        }

        fn signed(&self, from: Option<[u8; 16]>, values: Vec<String>) -> Envelope {
            let to = Some(hash(&values));
            let mut envelope = Envelope {
                v: VERSION,
                node: "peer".to_owned(),
                change: Change {
                    op: op(from, to),
                    table: "notes".to_owned(),
                    key: vec![ID.to_owned()],
                    from,
                    to,
                    values,
                },
                targets: Vec::new(),
                sunset: NOW + 600,
                txn: None,
                key_id: smugglr_multi::key_id(&SIGN),
                stamp: self.local.table("notes").map_or(Stamp([0; 16]), stamp),
                signature: [0; 32],
            };
            envelope.sign(&SIGN);
            envelope
        }

        fn take(&mut self, envelopes: &[Envelope]) -> Result<Applied, Error> {
            self.local
                .apply(&mut self.place, &self.settings, &SIGN, envelopes, NOW)
        }

        fn body(&self) -> rusqlite::Result<String> {
            Connection::open(&self.path)?.query_row("SELECT body FROM notes", [], |row| row.get(0))
        }
    }

    type Tried = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn a_conflict_kept_under_last_lands_and_its_next_copy_is_a_repeat() -> Tried {
        let mut node = Node::new()?;
        let f = hash(&row("f", "1"));
        node.edit_here("mine", "2")?;
        let theirs = node.signed(Some(f), row("theirs", "3"));

        let applied = node.take(std::slice::from_ref(&theirs))?;
        assert_eq!(applied.wrote, [theirs.signature]);
        assert_eq!(applied.waiting.len(), 0);
        assert_eq!(node.body()?, "theirs");

        let again = node.take(std::slice::from_ref(&theirs))?;
        assert_eq!(again.has, [theirs.signature]);
        assert_eq!(again.wrote.len(), 0);
        Ok(())
    }

    #[test]
    fn a_conflict_not_further_on_leaves_the_row_here() -> Tried {
        let mut node = Node::new()?;
        let f = hash(&row("f", "1"));
        node.edit_here("mine", "3")?;
        let theirs = node.signed(Some(f), row("theirs", "2"));

        let applied = node.take(std::slice::from_ref(&theirs))?;
        assert_eq!(applied.has, [theirs.signature]);
        assert_eq!(applied.wrote.len(), 0);
        assert_eq!(applied.waiting.len(), 0);
        assert_eq!(node.body()?, "mine");
        Ok(())
    }

    #[test]
    fn a_kept_conflict_then_its_next_change_in_one_burst_both_land() -> Tried {
        let mut node = Node::new()?;
        let f = hash(&row("f", "1"));
        node.edit_here("mine", "2")?;
        let first = node.signed(Some(f), row("first", "3"));
        let second = node.signed(first.change.to, row("second", "4"));

        let applied = node.take(&[first.clone(), second.clone()])?;
        assert_eq!(applied.wrote, [first.signature, second.signature]);
        assert_eq!(applied.waiting.len(), 0);
        assert_eq!(node.body()?, "second");
        Ok(())
    }

    #[test]
    fn a_row_that_moved_since_it_was_read_is_held_not_overwritten() -> Tried {
        let mut node = Node::new()?;
        let f = hash(&row("f", "1"));
        node.edit_here("mine", "2")?;
        let theirs = node.signed(Some(f), row("theirs", "3"));
        // The guard the decision read, which a move since then no longer meets.
        let stale = Some(hash(&row("gone", "9")));
        let (mut applied, mut moves) = (Applied::default(), Vec::new());
        node.local.write(
            &mut node.place,
            "notes",
            &[(&theirs, stale)],
            &mut applied,
            &mut moves,
        )?;
        assert_eq!(applied.waiting.len(), 1);
        assert_eq!(moves.len(), 0);
        assert_eq!(node.body()?, "mine");
        Ok(())
    }

    #[test]
    fn only_a_keys_last_change_in_a_burst_should_read_back_as_its_to() -> Tried {
        let node = Node::new()?;
        let first = node.signed(None, row("first", "2"));
        let second = node.signed(first.change.to, row("second", "3"));
        let burst = [(&first, None), (&second, first.change.to)];
        let key = vec![ID.to_owned()];
        let landed = BTreeSet::from([key.clone()]);

        let settled = HashMap::from([(key.clone(), hash(&row("second", "3")))]);
        assert_eq!(misread(&burst, &landed, &settled).len(), 0);

        let other = HashMap::from([(key, hash(&row("other", "4")))]);
        let wrong: Vec<[u8; 32]> = misread(&burst, &landed, &other)
            .iter()
            .map(|envelope| envelope.signature)
            .collect();
        assert_eq!(wrong, [second.signature]);
        Ok(())
    }
}
