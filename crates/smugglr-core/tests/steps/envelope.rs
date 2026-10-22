//! Steps for the signed envelope: nodes that each hold rows of one notes
//! table (`id`, `body`, `updated_at`), make signed envelopes of their own changes,
//! and receive each other's through the engine's one decision path; each
//! keeps its graveyard and its dropped log, and reads the time from `now`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, then, when};
use smugglr_core::change::{Change, op};
use smugglr_core::envelope::{Arrival, Envelope, GRAVEYARD, Here, Txn, VERSION, receive, replay};
use smugglr_core::plan::SyncMode;
use smugglr_core::record::{self, Dropped, Reason, Record};
use smugglr_core::row_hash::row_hash;
use smugglr_core::shape::Stamp;

use crate::Place;

/// The shared key every node signs and verifies with.
const KEY: [u8; 32] = [42; 32];
/// The position of `updated_at`, the steering column, in a notes row.
const UPDATED_AT: usize = 2;
/// When the graveyard scenarios begin, in seconds since the Unix epoch.
const T0: u64 = 1_800_000_000;
/// One hour, in seconds.
const HOUR: u64 = 60 * 60;

/// A row a node holds (`values` is `None` once it is deleted), and the
/// `from` of the change that brought it here (`None` for a row that came
/// from no row).
#[derive(Debug, Clone)]
struct Row {
    values: Option<Vec<String>>,
    last_from: Option<[u8; 16]>,
}

/// One node: its rows by key, what waits, the rows it stopped, its
/// graveyard, its dropped log and what its record names as dropped.
#[derive(Debug, Clone, Default)]
pub(crate) struct Node {
    name: String,
    now: u64,
    rows: BTreeMap<Vec<String>, Row>,
    held: Vec<Envelope>,
    stopped: Vec<Vec<String>>,
    applied: usize,
    graveyard: BTreeMap<Vec<String>, Envelope>,
    log: Vec<Envelope>,
    dropped: Vec<Dropped>,
}

/// The nodes of a scenario and the envelopes in flight.
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct Mesh {
    nodes: Slot<Vec<Node>>,
    /// The envelopes each node made, by node.
    made: Slot<Vec<Vec<Envelope>>>,
    /// The envelopes a single place is about to receive.
    arriving: Slot<Vec<Envelope>>,
    /// The record of the run that ended.
    record: Slot<Record>,
}

#[fixture]
pub(crate) fn mesh() -> Mesh {
    Mesh::default()
}

fn key(id: &str) -> Vec<String> {
    vec![id.to_owned()]
}

fn note(id: &str, body: &str, updated_at: &str) -> Vec<String> {
    vec![
        id.to_owned(),
        format!("'{body}'"),
        format!("'{updated_at}'"),
    ]
}

fn hash(values: &[String]) -> [u8; 16] {
    let columns: Vec<&str> = values.iter().map(String::as_str).collect();
    row_hash(&columns)
}

/// The signed envelope of `node`'s change of row `id` from `from` to
/// `values` (`None` deletes the row), carried until `sunset`.
fn envelope(
    node: &str,
    id: &str,
    from: Option<[u8; 16]>,
    values: Option<Vec<String>>,
    sunset: u64,
) -> Envelope {
    let to = values.as_deref().map(hash);
    let mut envelope = Envelope {
        v: VERSION,
        node: node.to_owned(),
        change: Change {
            op: op(from, to),
            table: "notes".to_owned(),
            key: key(id),
            from,
            to,
            values: values.unwrap_or_default(),
        },
        targets: Vec::new(),
        sunset,
        txn: None,
        key_id: "k1".to_owned(),
        stamp: Stamp([0; 16]),
        signature: [0; 32],
    };
    envelope.sign(&KEY);
    envelope
}

impl Node {
    fn holding(name: &str, rows: &[Vec<String>]) -> Self {
        let rows = rows
            .iter()
            .map(|values| {
                let row = Row {
                    values: Some(values.clone()),
                    last_from: None,
                };
                (vec![values[0].clone()], row)
            })
            .collect();
        Self {
            name: name.to_owned(),
            rows,
            ..Self::default()
        }
    }

    fn at(mut self, now: u64) -> Self {
        self.now = now;
        self
    }

    /// Changes row `id` here to `values` (`None` deletes it), and returns
    /// the signed envelope of the change, carried for the default graveyard
    /// window.
    fn change(&mut self, id: &str, values: Option<Vec<String>>) -> Envelope {
        let from = self
            .rows
            .get(&key(id))
            .and_then(|row| row.values.as_deref().map(hash));
        let made = envelope(&self.name, id, from, values.clone(), self.now + GRAVEYARD);
        if values.is_none() {
            self.graveyard.insert(key(id), made.clone());
        }
        self.rows.insert(
            key(id),
            Row {
                values,
                last_from: from,
            },
        );
        made
    }

    fn edit(&mut self, values: Vec<String>) -> Envelope {
        let id = values[0].clone();
        self.change(&id, Some(values))
    }

    fn delete(&mut self, id: &str) -> Envelope {
        self.change(id, None)
    }

    fn here(&self, change: &Change) -> Here {
        let row = self.rows.get(&change.key);
        let values = row.and_then(|row| row.values.as_ref());
        Here {
            hash: values.map(|values| hash(values)),
            last_from: row.map(|row| row.last_from),
            steering: values.map(|values| (UPDATED_AT, values[UPDATED_AT].clone())),
            tombstone: self.graveyard.get(&change.key).cloned(),
        }
    }

    /// Receives `envelopes` with what waits here, applying what applies,
    /// until nothing more does. A delete applied or skipped is the key's
    /// tombstone; a dropped change goes to the dropped log and the record.
    fn deliver(&mut self, envelopes: &[Envelope], mode: SyncMode) {
        let mut pending: Vec<Envelope> = std::mem::take(&mut self.held);
        pending.extend_from_slice(envelopes);
        loop {
            let arrivals = receive(&pending, &KEY, mode, self.now, |change| self.here(change));
            let mut moved = false;
            for (envelope, arrival) in pending.drain(..).zip(arrivals) {
                let change = &envelope.change;
                if matches!(arrival, Arrival::Apply | Arrival::Skip) && change.to.is_none() {
                    self.graveyard.insert(change.key.clone(), envelope.clone());
                }
                match arrival {
                    Arrival::Apply => {
                        let values = change.to.map(|_| change.values.clone());
                        self.rows.insert(
                            change.key.clone(),
                            Row {
                                values,
                                last_from: change.from,
                            },
                        );
                        self.applied += 1;
                        moved = true;
                    }
                    Arrival::Hold => self.held.push(envelope),
                    Arrival::Stop => self.stopped.push(change.key.clone()),
                    Arrival::Dropped(reason) => {
                        self.dropped.push(Dropped::new(&envelope, reason));
                        self.log.push(envelope);
                    }
                    Arrival::Skip | Arrival::Refused => {}
                }
            }
            if !moved || self.held.is_empty() {
                return;
            }
            pending = std::mem::take(&mut self.held);
        }
    }

    fn row(&self, id: &str) -> Result<&[String], String> {
        self.rows
            .get(&key(id))
            .and_then(|row| row.values.as_deref())
            .ok_or_else(|| format!("node {} holds no row {id}", self.name))
    }

    fn absent(&self, id: &str) -> Result<(), String> {
        match self.row(id) {
            Ok(row) => Err(format!("node {} holds {row:?}", self.name)),
            Err(_) => Ok(()),
        }
    }

    /// The record of what this node did: the rows it stopped, edits of rows
    /// deleted here, and what it dropped.
    fn record(&self) -> Record {
        let mut tables = BTreeMap::new();
        if !self.stopped.is_empty() {
            tables.insert(
                "notes".to_owned(),
                record::edit_versus_delete(self.stopped.clone()),
            );
        }
        let mut record = Record::new(tables, BTreeSet::new(), false, None);
        record.dropped.clone_from(&self.dropped);
        record
    }
}

/// Two nodes holding the same note, then each changing it before hearing
/// the other, the second change's `updated_at` later.
fn changed_on_both(mesh: &Mesh, names: &[&str]) {
    let first = note("1", "draft", "2026-10-01T09:00:00Z");
    let mut nodes: Vec<Node> = names
        .iter()
        .map(|name| Node::holding(name, std::slice::from_ref(&first)))
        .collect();
    let a = nodes[0].edit(note("1", "from a", "2026-10-01T10:00:00Z"));
    let b = nodes[1].edit(note("1", "from b", "2026-10-01T11:00:00Z"));
    mesh.made.set(vec![vec![a], vec![b]]);
    mesh.nodes.set(nodes);
}

fn each_receives_the_other(mesh: &Mesh, mode: SyncMode) -> Result<(), String> {
    let sent = mesh.made.get().ok_or("no node changed the row")?;
    mesh.nodes
        .with_mut(|nodes| {
            nodes[0].deliver(&sent[1], mode);
            nodes[1].deliver(&sent[0], mode);
        })
        .ok_or_else(|| "there are no nodes".to_owned())
}

#[given("a place that missed a change to a row, and then receives the next change to that row")]
fn missed_one(mesh: &Mesh) {
    let mut source = Node::holding("source", &[note("1", "draft", "2026-10-01T09:00:00Z")]);
    let mut place = source.clone();
    "place".clone_into(&mut place.name);
    let missed = source.edit(note("1", "second", "2026-10-01T10:00:00Z"));
    let next = source.edit(note("1", "third", "2026-10-01T11:00:00Z"));
    place.deliver(std::slice::from_ref(&next), SyncMode::Stop);
    mesh.made.set(vec![vec![missed, next]]);
    mesh.nodes.set(vec![place]);
}

#[when("the missed change arrives")]
fn missed_arrives(mesh: &Mesh) -> Result<(), String> {
    let made = mesh.made.get().ok_or("no change was made")?;
    mesh.nodes
        .with_mut(|nodes| nodes[0].deliver(&made[0][..1], SyncMode::Stop))
        .ok_or_else(|| "there is no place".to_owned())
}

#[then("the row holds the values of the later change")]
fn holds_later(mesh: &Mesh) -> Result<(), String> {
    let made = mesh.made.get().ok_or("no change was made")?;
    let nodes = mesh.nodes.get().ok_or("there is no place")?;
    let row = nodes[0].row("1")?;
    let later = &made[0][1].change.values;
    if row == later.as_slice() {
        Ok(())
    } else {
        Err(format!("the row holds {row:?}, not {later:?}"))
    }
}

#[given("two nodes that each changed the same row before hearing the other")]
fn two_changed(mesh: &Mesh) {
    changed_on_both(mesh, &["a", "b"]);
}

#[given(
    "a config naming updated_at as the steering column, and two nodes that each changed the same row before hearing the other, one with a later updated_at"
)]
fn two_changed_steered(mesh: &Mesh) {
    changed_on_both(mesh, &["a", "b"]);
}

#[when("each node receives the other's change")]
fn each_receives(mesh: &Mesh) -> Result<(), String> {
    each_receives_the_other(mesh, SyncMode::Stop)
}

#[when("each node receives the other's change under --sync-mode last")]
fn each_receives_last(mesh: &Mesh) -> Result<(), String> {
    each_receives_the_other(mesh, SyncMode::Last)
}

#[then("each node names that row as stopped")]
fn each_stopped(mesh: &Mesh) -> Result<(), String> {
    let nodes = mesh.nodes.get().ok_or("there are no nodes")?;
    for node in &nodes {
        if node.stopped != [key("1")] {
            return Err(format!("node {} stopped {:?}", node.name, node.stopped));
        }
    }
    Ok(())
}

#[then("the row on both nodes holds the values of the change with the later updated_at")]
fn both_hold_later(mesh: &Mesh) -> Result<(), String> {
    let made = mesh.made.get().ok_or("no node changed the row")?;
    let nodes = mesh.nodes.get().ok_or("there are no nodes")?;
    let later = &made[1][0].change.values;
    for node in &nodes {
        let row = node.row("1")?;
        if row != later.as_slice() || !node.stopped.is_empty() {
            return Err(format!(
                "node {} holds {row:?} and stopped {:?}",
                node.name, node.stopped
            ));
        }
    }
    Ok(())
}

#[given("a transaction of several changes, one of which cannot be applied here")]
fn transaction(mesh: &Mesh) {
    let rows = [
        note("1", "one", "2026-10-01T09:00:00Z"),
        note("2", "two", "2026-10-01T09:00:00Z"),
    ];
    let place = Node::holding("place", &rows);
    let txn = Txn {
        id: "01a16339-0000-7000-8000-000000000001".to_owned(),
        count: 2,
    };
    let applies = envelope(
        "source",
        "1",
        Some(hash(&rows[0])),
        Some(note("1", "one, changed", "2026-10-01T10:00:00Z")),
        GRAVEYARD,
    );
    // Built on a row this place never held.
    let waits = envelope(
        "source",
        "2",
        Some(hash(&note("2", "elsewhere", "2026-10-01T09:30:00Z"))),
        Some(note("2", "two, changed", "2026-10-01T10:00:00Z")),
        GRAVEYARD,
    );
    let made = [applies, waits]
        .into_iter()
        .map(|mut envelope| {
            envelope.txn = Some(txn.clone());
            envelope.sign(&KEY);
            envelope
        })
        .collect();
    mesh.arriving.set(made);
    mesh.nodes.set(vec![place]);
}

#[when("the place receives the transaction")]
fn receives_transaction(mesh: &Mesh) -> Result<(), String> {
    let arriving = mesh.arriving.get().ok_or("no transaction arrived")?;
    mesh.nodes
        .with_mut(|nodes| nodes[0].deliver(&arriving, SyncMode::Stop))
        .ok_or_else(|| "there is no place".to_owned())
}

#[then("none of its changes is applied")]
fn none_applied(mesh: &Mesh) -> Result<(), String> {
    let nodes = mesh.nodes.get().ok_or("there is no place")?;
    let place = &nodes[0];
    let unchanged = place.row("1")? == note("1", "one", "2026-10-01T09:00:00Z").as_slice()
        && place.row("2")? == note("2", "two", "2026-10-01T09:00:00Z").as_slice();
    if place.applied == 0 && unchanged {
        Ok(())
    } else {
        Err(format!("the place applied {} changes", place.applied))
    }
}

/// A place holding one note, and a change of it signed by its maker.
fn place_and_change(mesh: &Mesh, place: &Place) -> Envelope {
    let row = note("1", "draft", "2026-10-01T09:00:00Z");
    place.before.set(row.clone());
    place.row.set(row.clone());
    place.writes.set(0);
    mesh.nodes
        .set(vec![Node::holding("place", std::slice::from_ref(&row))]);
    envelope(
        "maker",
        "1",
        Some(hash(&row)),
        Some(note("1", "final", "2026-10-01T10:00:00Z")),
        GRAVEYARD,
    )
}

#[given("a change whose envelope was altered after it was signed")]
fn tampered(mesh: &Mesh, place: &Place) {
    let mut change = place_and_change(mesh, place);
    "someone else".clone_into(&mut change.node);
    mesh.arriving.set(vec![change]);
}

#[given("a change whose values do not hash to the envelope's to")]
fn values_off(mesh: &Mesh, place: &Place) {
    let mut change = place_and_change(mesh, place);
    "'not what was signed'".clone_into(&mut change.change.values[1]);
    mesh.arriving.set(vec![change]);
}

#[when("a place receives the change")]
fn a_place_receives(mesh: &Mesh, place: &Place) -> Result<(), String> {
    let arriving = mesh.arriving.get().ok_or("no change arrived")?;
    let mut nodes = mesh.nodes.take().ok_or("there is no place")?;
    nodes[0].deliver(&arriving, SyncMode::Stop);
    place.row.set(nodes[0].row("1")?.to_vec());
    place.writes.set(nodes[0].applied);
    Ok(())
}

#[given(
    "three nodes, two of which change the same row before hearing each other, and a third that has applied the first change"
)]
fn three_nodes(mesh: &Mesh) -> Result<(), String> {
    changed_on_both(mesh, &["a", "b", "c"]);
    let made = mesh.made.get().ok_or("no node changed the row")?;
    mesh.nodes
        .with_mut(|nodes| nodes[2].deliver(&made[0], SyncMode::Stop))
        .ok_or_else(|| "there are no nodes".to_owned())
}

#[when("the second change reaches the third node under --sync-mode stop")]
fn second_reaches_third(mesh: &Mesh) -> Result<(), String> {
    let made = mesh.made.get().ok_or("no node changed the row")?;
    mesh.nodes
        .with_mut(|nodes| nodes[2].deliver(&made[1], SyncMode::Stop))
        .ok_or_else(|| "there are no nodes".to_owned())
}

#[then("the third node stops that row and names it by key")]
fn third_stops(mesh: &Mesh) -> Result<(), String> {
    let made = mesh.made.get().ok_or("no node changed the row")?;
    let nodes = mesh.nodes.get().ok_or("there are no nodes")?;
    let third = &nodes[2];
    let first = &made[0][0].change.values;
    if third.stopped == [key("1")] && third.row("1")? == first.as_slice() {
        Ok(())
    } else {
        Err(format!(
            "the third node stopped {:?} and holds {:?}",
            third.stopped,
            third.row("1")?
        ))
    }
}

// The graveyard and sunsets (FR-SMUGGLR-076). The place a scenario watches
// is the last of its nodes.

fn draft() -> Vec<String> {
    note("1", "draft", "2026-10-01T09:00:00Z")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

fn unhex(text: &str) -> Result<[u8; 32], String> {
    let mut bytes = [0; 32];
    if text.len() != 64 {
        return Err(format!("{text} is not a change hash"));
    }
    for (at, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[at * 2..at * 2 + 2], 16)
            .map_err(|err| format!("{text}: {err}"))?;
    }
    Ok(bytes)
}

fn watched(mesh: &Mesh) -> Result<Node, String> {
    mesh.nodes
        .get()
        .and_then(|nodes| nodes.last().cloned())
        .ok_or_else(|| "there is no place".to_owned())
}

/// Runs `f` on the watched place.
fn at_place(mesh: &Mesh, f: impl FnOnce(&mut Node)) -> Result<(), String> {
    mesh.nodes
        .with_mut(|nodes| nodes.last_mut().map(f))
        .flatten()
        .ok_or_else(|| "there is no place".to_owned())
}

fn made(mesh: &Mesh, node: usize) -> Result<Envelope, String> {
    mesh.made
        .get()
        .and_then(|made| made.get(node).and_then(|made| made.first().cloned()))
        .ok_or_else(|| format!("node {node} made no change"))
}

fn names_dropped(node: &Node, envelope: &Envelope, reason: Reason) -> Result<(), String> {
    let dropped = node.record().dropped;
    let named = dropped.iter().any(|dropped| {
        dropped.table == "notes"
            && dropped.key == record::Key(envelope.change.key.clone())
            && dropped.reason == reason
            && dropped.change == hex(&envelope.signature)
    });
    if named {
        Ok(())
    } else {
        Err(format!("node {}'s record names {dropped:?}", node.name))
    }
}

#[given("a row inserted on one node and deleted on another")]
fn inserted_then_deleted(mesh: &Mesh) {
    let mut a = Node::holding("a", &[]).at(T0);
    let mut b = Node::holding("b", &[]).at(T0);
    let insert = a.edit(draft());
    b.deliver(std::slice::from_ref(&insert), SyncMode::Stop);
    let delete = b.delete("1");
    mesh.made.set(vec![vec![insert], vec![delete]]);
    mesh.nodes
        .set(vec![a, b, Node::holding("place", &[]).at(T0 + HOUR)]);
}

#[when("a third place receives the delete before the insert, inside the graveyard window")]
fn delete_first(mesh: &Mesh) -> Result<(), String> {
    let (insert, delete) = (made(mesh, 0)?, made(mesh, 1)?);
    at_place(mesh, |place| {
        place.deliver(&[delete], SyncMode::Stop);
        place.deliver(&[insert], SyncMode::Stop);
    })
}

#[then("the row is absent at the third place")]
fn absent_at_third(mesh: &Mesh) -> Result<(), String> {
    watched(mesh)?.absent("1")
}

#[then("the row is absent at that place")]
fn absent_at_that_place(mesh: &Mesh) -> Result<(), String> {
    watched(mesh)?.absent("1")
}

#[then("the row is absent at the place")]
fn absent_at_place(mesh: &Mesh) -> Result<(), String> {
    watched(mesh)?.absent("1")
}

#[given("a row inserted, then edited, then deleted on other nodes")]
fn inserted_edited_deleted(mesh: &Mesh) {
    let mut a = Node::holding("a", &[]).at(T0);
    let mut b = Node::holding("b", &[]).at(T0);
    let mut c = Node::holding("c", &[]).at(T0);
    let insert = a.edit(draft());
    b.deliver(std::slice::from_ref(&insert), SyncMode::Stop);
    let edit = b.edit(note("1", "edited", "2026-10-01T10:00:00Z"));
    c.deliver(&[insert.clone(), edit.clone()], SyncMode::Stop);
    let delete = c.delete("1");
    mesh.made.set(vec![vec![insert], vec![edit], vec![delete]]);
    mesh.nodes
        .set(vec![a, b, c, Node::holding("place", &[]).at(T0 + HOUR)]);
}

#[when(
    "a place receives the delete first, then the insert, then the edit, inside the graveyard window"
)]
fn delete_insert_edit(mesh: &Mesh) -> Result<(), String> {
    let order = [made(mesh, 2)?, made(mesh, 0)?, made(mesh, 1)?];
    at_place(mesh, |place| {
        for envelope in order {
            place.deliver(&[envelope], SyncMode::Stop);
        }
    })
}

#[given(
    "a row deleted on every node, and then re-created with the same key on one node inside the graveyard window"
)]
fn recreated(mesh: &Mesh) {
    let mut nodes: Vec<Node> = ["a", "b", "c"]
        .iter()
        .map(|name| Node::holding(name, &[draft()]).at(T0))
        .collect();
    let delete = nodes[0].delete("1");
    for node in &mut nodes[1..] {
        node.deliver(std::slice::from_ref(&delete), SyncMode::Stop);
    }
    for node in &mut nodes {
        node.now = T0 + HOUR;
    }
    let insert = nodes[0].edit(note("1", "again", "2026-10-01T10:00:00Z"));
    mesh.made.set(vec![vec![insert]]);
    mesh.nodes.set(nodes);
}

#[when("the other places receive the insert")]
fn others_receive(mesh: &Mesh) -> Result<(), String> {
    let insert = made(mesh, 0)?;
    mesh.nodes
        .with_mut(|nodes| {
            for node in &mut nodes[1..] {
                node.deliver(std::slice::from_ref(&insert), SyncMode::Stop);
            }
        })
        .ok_or_else(|| "there are no nodes".to_owned())
}

#[then("the row is absent at the other places")]
fn absent_at_others(mesh: &Mesh) -> Result<(), String> {
    let nodes = mesh.nodes.get().ok_or("there are no nodes")?;
    nodes[1..].iter().try_for_each(|node| node.absent("1"))
}

#[then("it stays where it was made")]
fn stays_at_maker(mesh: &Mesh) -> Result<(), String> {
    let insert = made(mesh, 0)?;
    let nodes = mesh.nodes.get().ok_or("there are no nodes")?;
    let row = nodes[0].row("1")?;
    if row == insert.change.values.as_slice() {
        Ok(())
    } else {
        Err(format!("the maker holds {row:?}"))
    }
}

#[then("the record at the other places names it under dropped")]
fn others_name_it(mesh: &Mesh) -> Result<(), String> {
    let insert = made(mesh, 0)?;
    let nodes = mesh.nodes.get().ok_or("there are no nodes")?;
    nodes[1..]
        .iter()
        .try_for_each(|node| names_dropped(node, &insert, Reason::Graveyard))
}

/// A place that deleted its row at `T0` and whose clock reads `now`.
fn deleted_at_place(mesh: &Mesh, now: u64) {
    let mut place = Node::holding("place", &[draft()]).at(T0);
    place.delete("1");
    mesh.nodes.set(vec![place.at(now)]);
}

#[given("a row deleted at a place longer ago than the graveyard window")]
fn deleted_long_ago(mesh: &Mesh) {
    deleted_at_place(mesh, T0 + GRAVEYARD + HOUR);
}

#[given("no graveyard setting, and a row deleted at a place less than 12 hours ago")]
fn deleted_lately(mesh: &Mesh) {
    deleted_at_place(mesh, T0 + 11 * HOUR);
}

#[when("the place receives an insert for that key")]
fn insert_arrives(mesh: &Mesh) -> Result<(), String> {
    let now = watched(mesh)?.now;
    let insert = envelope(
        "maker",
        "1",
        None,
        Some(note("1", "again", "2026-10-01T10:00:00Z")),
        now + GRAVEYARD,
    );
    mesh.arriving.set(vec![insert.clone()]);
    at_place(mesh, |place| place.deliver(&[insert], SyncMode::Stop))
}

#[then("the place holds the row")]
fn holds_row(mesh: &Mesh) -> Result<(), String> {
    let insert = mesh.arriving.get().ok_or("no insert arrived")?;
    let place = watched(mesh)?;
    let row = place.row("1")?;
    if row == insert[0].change.values.as_slice() {
        Ok(())
    } else {
        Err(format!("the place holds {row:?}"))
    }
}

#[given("a change whose sunset has passed")]
fn dead_change(mesh: &Mesh) {
    let change = envelope(
        "maker",
        "1",
        Some(hash(&draft())),
        Some(note("1", "late", "2026-10-01T10:00:00Z")),
        T0,
    );
    mesh.arriving.set(vec![change]);
    mesh.nodes
        .set(vec![Node::holding("place", &[draft()]).at(T0 + HOUR)]);
}

#[when("a place receives it")]
fn receives_it(mesh: &Mesh) -> Result<(), String> {
    let arriving = mesh.arriving.get().ok_or("no change arrived")?;
    at_place(mesh, |place| place.deliver(&arriving, SyncMode::Stop))
}

#[then("the place's row is unchanged")]
fn row_unchanged(mesh: &Mesh) -> Result<(), String> {
    let place = watched(mesh)?;
    let row = place.row("1")?;
    if row == draft().as_slice() && place.applied == 0 {
        Ok(())
    } else {
        Err(format!("the place holds {row:?}"))
    }
}

#[given("a place holding a change whose from matches nothing there")]
fn holding_waiting(mesh: &Mesh) -> Result<(), String> {
    waiting(mesh)
}

#[given("a change waiting on a device for the change it built on, which never arrives")]
fn waiting(mesh: &Mesh) -> Result<(), String> {
    let waits = envelope(
        "maker",
        "1",
        Some(hash(&note("1", "elsewhere", "2026-10-01T09:30:00Z"))),
        Some(note("1", "built on it", "2026-10-01T10:00:00Z")),
        T0 + GRAVEYARD,
    );
    let mut place = Node::holding("place", &[draft()]).at(T0);
    place.deliver(std::slice::from_ref(&waits), SyncMode::Stop);
    if place.held.len() != 1 {
        return Err(format!(
            "the place holds {} waiting changes",
            place.held.len()
        ));
    }
    mesh.arriving.set(vec![waits]);
    mesh.nodes.set(vec![place]);
    Ok(())
}

#[when("the change's sunset passes")]
fn its_sunset_passes(mesh: &Mesh) -> Result<(), String> {
    sunset_passes(mesh)
}

#[when("the waiting change's sunset passes and the device next retrieves")]
fn sunset_passes(mesh: &Mesh) -> Result<(), String> {
    at_place(mesh, |place| {
        place.now = T0 + GRAVEYARD + 1;
        place.deliver(&[], SyncMode::Stop);
    })
}

#[then("the place holds no waiting change")]
fn none_waiting(mesh: &Mesh) -> Result<(), String> {
    let place = watched(mesh)?;
    if place.held.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "the place holds {} waiting changes",
            place.held.len()
        ))
    }
}

#[then(
    "the record names that change's table and key under dropped, with the reason that its sunset passed"
)]
fn names_waiting_dropped(mesh: &Mesh) -> Result<(), String> {
    let waits = mesh.arriving.get().ok_or("no change waited")?;
    names_dropped(&watched(mesh)?, &waits[0], Reason::Sunset)
}

/// Devices `a` and `b` holding the same row at `T0`; `a` deletes it and
/// `b` edits it before hearing of the delete.
fn deleted_and_edited() -> (Node, Node, Envelope, Envelope) {
    let mut a = Node::holding("a", &[draft()]).at(T0);
    let mut b = Node::holding("b", &[draft()]).at(T0);
    let delete = a.delete("1");
    let edit = b.edit(note("1", "edited", "2026-10-01T10:00:00Z"));
    (a, b, delete, edit)
}

#[given("a row deleted on one device, and an edit to it made on another device")]
fn late_edit(mesh: &Mesh) {
    let (a, b, delete, edit) = deleted_and_edited();
    let mut place = Node::holding("place", &[draft()]).at(T0);
    place.deliver(std::slice::from_ref(&delete), SyncMode::Stop);
    mesh.made.set(vec![vec![delete], vec![edit]]);
    mesh.nodes.set(vec![a, b, place]);
}

#[when("a place receives the edit past its sunset, after the row's tombstone has gone")]
fn edit_arrives_late(mesh: &Mesh) -> Result<(), String> {
    let edit = made(mesh, 1)?;
    at_place(mesh, |place| {
        place.now = T0 + GRAVEYARD + HOUR;
        place.deliver(&[edit], SyncMode::Stop);
    })
}

#[then("the place ignores the edit")]
fn ignores_edit(mesh: &Mesh) -> Result<(), String> {
    let place = watched(mesh)?;
    if place.stopped.is_empty() {
        place.absent("1")
    } else {
        Err(format!("the place stopped {:?}", place.stopped))
    }
}

#[then("its dropped log holds the edit")]
fn log_holds_edit(mesh: &Mesh) -> Result<(), String> {
    let edit = made(mesh, 1)?;
    let place = watched(mesh)?;
    if place.log == [edit] {
        Ok(())
    } else {
        Err(format!("the dropped log holds {:?}", place.log))
    }
}

#[then("the record names the edit by its change hash")]
fn names_edit(mesh: &Mesh) -> Result<(), String> {
    names_dropped(&watched(mesh)?, &made(mesh, 1)?, Reason::Sunset)
}

#[given("a place whose dropped log holds an edit to a row deleted elsewhere")]
fn logged_late_edit(mesh: &Mesh) -> Result<(), String> {
    late_edit(mesh);
    edit_arrives_late(mesh)?;
    log_holds_edit(mesh)
}

#[when("the agent replays it with --replay and its change hash, under --sync-mode stop")]
fn replays(mesh: &Mesh) -> Result<(), String> {
    let place = watched(mesh)?;
    let named = place.record().dropped;
    let hash = unhex(
        &named
            .first()
            .ok_or("the record names nothing dropped")?
            .change,
    )?;
    let replayed = replay(&place.log, &hash, place.now + GRAVEYARD, &KEY)
        .ok_or("the dropped log holds no change with that hash")?;
    at_place(mesh, |place| place.deliver(&[replayed], SyncMode::Stop))
}

#[then("it stops as edit versus delete")]
fn stops_edit_versus_delete(mesh: &Mesh) -> Result<(), String> {
    let place = watched(mesh)?;
    if place.stopped == [key("1")] {
        place.absent("1")
    } else {
        Err(format!("the place stopped {:?}", place.stopped))
    }
}

#[then("the record names the row")]
fn record_names_row(mesh: &Mesh) -> Result<(), String> {
    let record = watched(mesh)?.record();
    match record.tables.get("notes") {
        Some(record::Table::Stopped { rows, why, .. })
            if *rows == [record::Key(key("1"))] && why == record::EDIT_VERSUS_DELETE =>
        {
            Ok(())
        }
        other => Err(format!("the record gives notes {other:?}")),
    }
}

#[given(
    "a row deleted on one device, and on another device an edit made to that row before it heard of the delete"
)]
fn edit_before_hearing(mesh: &Mesh) {
    let (a, b, delete, edit) = deleted_and_edited();
    mesh.made.set(vec![vec![delete], vec![edit]]);
    // The first device is the place watched.
    mesh.nodes.set(vec![b, a.at(T0 + HOUR)]);
}

#[when("the edit's change arrives on the first device under --sync-mode stop")]
fn edit_reaches_deleter(mesh: &Mesh) -> Result<(), String> {
    let edit = made(mesh, 1)?;
    at_place(mesh, |place| place.deliver(&[edit], SyncMode::Stop))
}

#[then("the row stops and is named by key")]
fn stops_named(mesh: &Mesh) -> Result<(), String> {
    stops_edit_versus_delete(mesh)?;
    record_names_row(mesh)
}

#[given("a place that drops a change")]
fn drops_a_change(mesh: &Mesh) -> Result<(), String> {
    dead_change(mesh);
    receives_it(mesh)
}

#[when("the run ends")]
fn run_ends(mesh: &Mesh) -> Result<(), String> {
    mesh.record.set(watched(mesh)?.record());
    Ok(())
}

#[then("the dropped log holds the change's envelope and values")]
fn log_holds_change(mesh: &Mesh) -> Result<(), String> {
    let arriving = mesh.arriving.get().ok_or("no change arrived")?;
    let place = watched(mesh)?;
    let kept = place.log.first().ok_or("the dropped log is empty")?;
    if place.log.len() == 1 && *kept == arriving[0] && !kept.change.values.is_empty() {
        Ok(())
    } else {
        Err(format!("the dropped log holds {:?}", place.log))
    }
}

#[then("the record names it under dropped with its table, key, reason and change hash")]
fn record_names_change(mesh: &Mesh) -> Result<(), String> {
    let arriving = mesh.arriving.get().ok_or("no change arrived")?;
    let record = mesh.record.get().ok_or("the run has not ended")?;
    let change = &arriving[0];
    let named = record.dropped.iter().any(|dropped| {
        dropped.table == change.change.table
            && dropped.key == record::Key(change.change.key.clone())
            && dropped.reason == Reason::Sunset
            && dropped.change == hex(&change.signature)
    });
    if named {
        Ok(())
    } else {
        Err(format!("the record names {:?}", record.dropped))
    }
}
