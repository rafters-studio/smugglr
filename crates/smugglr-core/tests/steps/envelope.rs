//! Steps for the signed envelope: nodes that each hold rows of one notes
//! table (`id`, `body`, `updated_at`), make signed envelopes of their own changes,
//! and receive each other's through the engine's one decision path.

use std::collections::BTreeMap;

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, then, when};
use smugglr_core::change::{Change, op};
use smugglr_core::envelope::{Arrival, Envelope, Here, Txn, VERSION, receive};
use smugglr_core::plan::SyncMode;
use smugglr_core::row_hash::row_hash;
use smugglr_core::shape::Stamp;

use crate::Place;

/// The shared key every node signs and verifies with.
const KEY: [u8; 32] = [42; 32];
/// The position of `updated_at`, the steering column, in a notes row.
const UPDATED_AT: usize = 2;

/// A row a node holds, and the `from` of the change that brought it here
/// (`None` for a row that came from no row).
#[derive(Debug, Clone)]
struct Row {
    values: Vec<String>,
    last_from: Option<[u8; 16]>,
}

/// One node: its rows by key, what waits, and the rows it stopped.
#[derive(Debug, Clone, Default)]
pub(crate) struct Node {
    name: String,
    rows: BTreeMap<Vec<String>, Row>,
    held: Vec<Envelope>,
    stopped: Vec<Vec<String>>,
    applied: usize,
}

/// The nodes of a scenario and the envelopes in flight.
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct Mesh {
    nodes: Slot<Vec<Node>>,
    /// The envelopes each node made, by node.
    made: Slot<Vec<Vec<Envelope>>>,
    /// The envelopes a single place is about to receive.
    arriving: Slot<Vec<Envelope>>,
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

fn envelope(node: &str, from: Option<[u8; 16]>, values: Vec<String>) -> Envelope {
    let to = Some(hash(&values));
    let mut envelope = Envelope {
        v: VERSION,
        node: node.to_owned(),
        change: Change {
            op: op(from, to),
            table: "notes".to_owned(),
            key: vec![values[0].clone()],
            from,
            to,
            values,
        },
        targets: Vec::new(),
        sunset: 0,
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
                    values: values.clone(),
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

    /// Changes a row here and returns the signed envelope of the change.
    fn edit(&mut self, values: Vec<String>) -> Envelope {
        let row = self.rows.get(&vec![values[0].clone()]);
        let from = row.map(|row| hash(&row.values));
        let made = envelope(&self.name, from, values.clone());
        self.rows.insert(
            vec![values[0].clone()],
            Row {
                values,
                last_from: from,
            },
        );
        made
    }

    fn here(&self, change: &Change) -> Here {
        let row = self.rows.get(&change.key);
        Here {
            hash: row.map(|row| hash(&row.values)),
            last_from: row.map(|row| row.last_from),
            steering: row.map(|row| (UPDATED_AT, row.values[UPDATED_AT].clone())),
        }
    }

    /// Receives `envelopes` with what waits here, applying what applies,
    /// until nothing more does.
    fn deliver(&mut self, envelopes: &[Envelope], mode: SyncMode) {
        let mut pending: Vec<Envelope> = std::mem::take(&mut self.held);
        pending.extend_from_slice(envelopes);
        loop {
            let arrivals = receive(&pending, &KEY, mode, |change| self.here(change));
            let mut moved = false;
            for (envelope, arrival) in pending.drain(..).zip(arrivals) {
                let change = &envelope.change;
                match arrival {
                    Arrival::Apply => {
                        self.rows.insert(
                            change.key.clone(),
                            Row {
                                values: change.values.clone(),
                                last_from: change.from,
                            },
                        );
                        self.applied += 1;
                        moved = true;
                    }
                    Arrival::Hold => self.held.push(envelope),
                    Arrival::Stop => self.stopped.push(change.key.clone()),
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
            .map(|row| row.values.as_slice())
            .ok_or_else(|| format!("node {} holds no row {id}", self.name))
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
        Some(hash(&rows[0])),
        note("1", "one, changed", "2026-10-01T10:00:00Z"),
    );
    // Built on a row this place never held.
    let waits = envelope(
        "source",
        Some(hash(&note("2", "elsewhere", "2026-10-01T09:30:00Z"))),
        note("2", "two, changed", "2026-10-01T10:00:00Z"),
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
        Some(hash(&row)),
        note("1", "final", "2026-10-01T10:00:00Z"),
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
