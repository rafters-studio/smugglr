//! The signed envelope multi and the mesh carry each Change in
//! (FR-SMUGGLR-075).
//!
//! An [`Envelope`] is a [`Change`] plus `node`, `targets`, `sunset`, `txn`,
//! `key_id`, the table's shape stamp and a signature over all of them; the
//! row's values follow, bound by `to`, the hash they must have. On arrival
//! each change goes through the one decision path ([`decide`]): skipped when
//! its `to` is here or is the `from` of the last change applied to the row
//! (a late copy the row has moved past), applied when its `from` is here,
//! held when its `from` matches nothing else here, and a conflict when the
//! place's last change of the
//! row came from the same `from` to a different `to`, which `--sync-mode`
//! settles (stop or last). Changes that were one transaction at the source
//! are applied all or none. A change whose signature does not match its
//! envelope, or whose values do not hash to its `to`, is refused.
//!
//! The signature is BLAKE3 keyed with the shared key the caller chooses.
//!
//! Every delete leaves a tombstone, and every change carries a sunset
//! (FR-SMUGGLR-076). A change past its sunset is dead: dropped on arrival
//! and dropped if waiting. An insert or a waiting change for a key whose
//! tombstone is live is dropped; an edit built on the deleted row, whose
//! `from` is the tombstone's `from`, is an edit-versus-delete conflict. A
//! dropped change goes to the place's dropped log, and [`replay`] re-sends
//! it with a fresh sunset.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use crate::change::{Change, Decision, Op, decide};
use crate::hex::{hex, unhex};
use crate::plan::SyncMode;
use crate::record::Reason;
use crate::row_hash::row_hash;
use crate::shape::Stamp;
use crate::steering::further_on;

/// The envelope format version, `v`.
pub const VERSION: u32 = 1;

/// The graveyard setting's default, in seconds: how long a change is
/// carried, and so how long a delete's tombstone is kept, when its sender
/// names no other.
pub const GRAVEYARD: u64 = 12 * 60 * 60;

/// The transaction a change was one part of at its source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Txn {
    /// The transaction's id, the same in each of its changes.
    pub id: String,
    /// How many changes the transaction holds.
    pub count: u32,
}

/// One change as multi and the mesh carry it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    /// The format version, [`VERSION`].
    pub v: u32,
    /// The node that made the change.
    pub node: String,
    /// The change; its values travel after the signature, bound by its `to`.
    pub change: Change,
    /// Who still needs the change (multi only; the mesh tracks
    /// acknowledgements).
    pub targets: Vec<String>,
    /// When the change stops being carried, in seconds since the Unix
    /// epoch; past it the change is dead.
    pub sunset: u64,
    /// The source transaction the change was part of, if any.
    pub txn: Option<Txn>,
    /// The shared key that signed the envelope.
    pub key_id: String,
    /// The table's shape stamp.
    pub stamp: Stamp,
    /// The signature over every field above but the values. It is also the
    /// change hash the record and the dropped log name the change by.
    ///
    /// The signature is the change hash because it is already a digest of
    /// every field, the same for every copy of the change, and new once a
    /// replay gives a fresh sunset.
    pub signature: [u8; 32],
}

impl Envelope {
    /// Signs the envelope with `key`.
    pub fn sign(&mut self, key: &[u8; 32]) {
        self.signature = *self.signed(key).as_bytes();
    }

    /// Whether the signature matches the envelope under `key` and the values
    /// hash to its `to` (a change that leaves no row carries no values).
    #[must_use]
    pub fn verify(&self, key: &[u8; 32]) -> bool {
        let values: Vec<&str> = self.change.values.iter().map(String::as_str).collect();
        let values_match = match self.change.to {
            Some(to) => row_hash(&values) == to,
            None => values.is_empty(),
        };
        // blake3::Hash compares in constant time.
        values_match && self.signed(key) == blake3::Hash::from(self.signature)
    }

    /// BLAKE3 keyed with `key` over each field, each prefixed with its byte
    /// length, so no field can run into the next.
    fn signed(&self, key: &[u8; 32]) -> blake3::Hash {
        fn field(hasher: &mut blake3::Hasher, bytes: &[u8]) {
            hasher.update(&(bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
        }
        fn hash(hasher: &mut blake3::Hasher, hash: Option<[u8; 16]>) {
            field(hasher, hash.as_ref().map_or(&[][..], |hash| hash));
        }
        let change = &self.change;
        let mut hasher = blake3::Hasher::new_keyed(key);
        field(&mut hasher, &self.v.to_be_bytes());
        field(
            &mut hasher,
            match change.op {
                Op::Up => b"up",
                Op::Down => b"down",
                Op::Upsert => b"upsert",
            },
        );
        field(&mut hasher, self.node.as_bytes());
        field(&mut hasher, change.table.as_bytes());
        field(&mut hasher, &(change.key.len() as u64).to_be_bytes());
        for column in &change.key {
            field(&mut hasher, column.as_bytes());
        }
        hash(&mut hasher, change.from);
        hash(&mut hasher, change.to);
        field(&mut hasher, &(self.targets.len() as u64).to_be_bytes());
        for target in &self.targets {
            field(&mut hasher, target.as_bytes());
        }
        field(&mut hasher, &self.sunset.to_be_bytes());
        match &self.txn {
            Some(txn) => {
                field(&mut hasher, b"txn");
                field(&mut hasher, txn.id.as_bytes());
                field(&mut hasher, &txn.count.to_be_bytes());
            }
            None => field(&mut hasher, b""),
        }
        field(&mut hasher, self.key_id.as_bytes());
        field(&mut hasher, &self.stamp.0);
        hasher.finalize()
    }
}

impl Envelope {
    /// The envelope as JSON, its values included: how a blob and smugglr's
    /// own files hold it.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        let change = &self.change;
        let hash = |hash: Option<[u8; 16]>| hash.map(|hash| hex(&hash));
        let txn = self
            .txn
            .as_ref()
            .map(|txn| serde_json::json!({"id": txn.id, "count": txn.count}));
        serde_json::json!({
            "v": self.v,
            "op": match change.op {
                Op::Up => "up",
                Op::Down => "down",
                Op::Upsert => "upsert",
            },
            "node": self.node,
            "table": change.table,
            "key": change.key,
            "from": hash(change.from),
            "to": hash(change.to),
            "targets": self.targets,
            "sunset": self.sunset,
            "txn": txn,
            "key_id": self.key_id,
            "stamp": hex(&self.stamp.0),
            "signature": hex(&self.signature),
            "values": change.values,
        })
    }

    /// The envelope [`Envelope::to_json`] wrote; `None` when `json` is not
    /// one. Whether it is signed is [`Envelope::verify`]'s to say.
    #[must_use]
    pub fn from_json(json: &serde_json::Value) -> Option<Self> {
        let text = |name: &str| json.get(name)?.as_str().map(str::to_owned);
        let texts = |name: &str| -> Option<Vec<String>> {
            json.get(name)?
                .as_array()?
                .iter()
                .map(|item| item.as_str().map(str::to_owned))
                .collect()
        };
        let hash = |name: &str| -> Option<Option<[u8; 16]>> {
            match json.get(name)? {
                serde_json::Value::Null => Some(None),
                value => Some(Some(bytes(value.as_str()?)?)),
            }
        };
        let op = match json.get("op")?.as_str()? {
            "up" => Op::Up,
            "down" => Op::Down,
            "upsert" => Op::Upsert,
            _ => return None,
        };
        let txn = match json.get("txn") {
            None | Some(serde_json::Value::Null) => None,
            Some(txn) => Some(Txn {
                id: txn.get("id")?.as_str()?.to_owned(),
                count: u32::try_from(txn.get("count")?.as_u64()?).ok()?,
            }),
        };
        Some(Self {
            v: u32::try_from(json.get("v")?.as_u64()?).ok()?,
            node: text("node")?,
            change: Change {
                op,
                table: text("table")?,
                key: texts("key")?,
                from: hash("from")?,
                to: hash("to")?,
                values: texts("values")?,
            },
            targets: texts("targets")?,
            sunset: json.get("sunset")?.as_u64()?,
            txn,
            key_id: text("key_id")?,
            stamp: Stamp(bytes(&text("stamp")?)?),
            signature: bytes(&text("signature")?)?,
        })
    }
}

/// The `N` bytes the hex `text` holds; `None` when it holds anything else.
fn bytes<const N: usize>(text: &str) -> Option<[u8; N]> {
    unhex(text)?.try_into().ok()
}

/// A change's row as the place it arrives at holds it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Here {
    /// The row's hash; `None` when the place holds no row for the key.
    pub hash: Option<[u8; 16]>,
    /// The `from` of the change that brought the row to `hash` (the last
    /// change applied here, or the place's own change), when there was one.
    pub last_from: Option<Option<[u8; 16]>>,
    /// The steering column's position among the row's columns and its value
    /// here, as SQLite's `quote()` text; for [`SyncMode::Last`].
    pub steering: Option<(usize, String)>,
    /// The key's tombstone: the delete envelope the place keeps for it until
    /// the delete's sunset. A delete applied or skipped here is the key's
    /// tombstone.
    pub tombstone: Option<Envelope>,
}

/// What a place does with an envelope that arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arrival {
    /// Write the change's values; the place keeps its `from` as the key's
    /// last applied `from`.
    Apply,
    /// Leave the row: it is already at the change's `to`, the last change
    /// applied here was built on its `to`, or under `--sync-mode last` the
    /// copy here is further on.
    Skip,
    /// The change waits: its `from` matches nothing here, or another change
    /// of its transaction cannot be applied yet.
    Hold,
    /// The row changed here and in the change from the same `from`, and
    /// `--sync-mode` stops the row: name it. Against a key that holds no row
    /// here, it is an edit versus a delete.
    Stop,
    /// The change is past its sunset, or an insert or waiting change for a
    /// key in the graveyard: keep it in the dropped log and name it in the
    /// record.
    Dropped(Reason),
    /// The signature does not match the envelope under the key, or the
    /// values do not hash to the change's `to`.
    Refused,
}

/// Decides each of `envelopes` arriving together, in order, at `now`
/// (seconds since the Unix epoch), verified with `key`. The first envelope of
/// a row is decided against the row `here` gives for its change, and each
/// later one against the row as the envelopes before it leave it, so a batch
/// arrives as the same envelopes would one call at a time. What waits is
/// received again with what arrives next, so a waiting change past its sunset
/// is dropped. A conflict follows `mode`: under [`SyncMode::Last`] the copy
/// whose steering value is further on is kept, and a row whose two values are
/// equal, or that has none, stops; under every other mode the row stops. A
/// change of a transaction (its maker's `node` and the transaction's `id`) is
/// applied only when all of the transaction's distinct changes have arrived
/// unrefused and each is applied or skipped; otherwise it is held, and leaves
/// its row as it was.
pub fn receive(
    envelopes: &[Envelope],
    key: &[u8; 32],
    mode: SyncMode,
    now: u64,
    here: impl Fn(&Change) -> Here,
) -> Vec<Arrival> {
    decide_all(envelopes, key, mode, now, here, false)
}

/// Decides `envelopes` the place replays ([`replay`]) as [`receive`] does,
/// except that a replay overrides the graveyard: a live tombstone does not
/// drop them.
pub fn receive_replayed(
    envelopes: &[Envelope],
    key: &[u8; 32],
    mode: SyncMode,
    now: u64,
    here: impl Fn(&Change) -> Here,
) -> Vec<Arrival> {
    decide_all(envelopes, key, mode, now, here, true)
}

fn decide_all(
    envelopes: &[Envelope],
    key: &[u8; 32],
    mode: SyncMode,
    now: u64,
    here: impl Fn(&Change) -> Here,
    replayed: bool,
) -> Vec<Arrival> {
    // Holding a member can change what follows it, so decide again until no
    // further member is held.
    let mut held = BTreeSet::new();
    loop {
        let arrivals = in_order(envelopes, key, (mode, now, replayed), &here, &held);
        let broken = broken(envelopes, &arrivals);
        let before = held.len();
        for (index, (envelope, arrival)) in envelopes.iter().zip(&arrivals).enumerate() {
            let in_broken = envelope
                .txn
                .as_ref()
                .is_some_and(|txn| broken.contains(&(envelope.node.as_str(), txn.id.as_str())));
            if in_broken && *arrival == Arrival::Apply {
                held.insert(index);
            }
        }
        if held.len() == before {
            return arrivals;
        }
    }
}

/// Decides each envelope against its row as the envelopes before it leave
/// it; an envelope in `held` that would apply is held instead.
fn in_order(
    envelopes: &[Envelope],
    key: &[u8; 32],
    (mode, now, replayed): (SyncMode, u64, bool),
    here: impl Fn(&Change) -> Here,
    held: &BTreeSet<usize>,
) -> Vec<Arrival> {
    let mut rows: BTreeMap<(&str, &[String]), Here> = BTreeMap::new();
    let mut arrivals = Vec::with_capacity(envelopes.len());
    for (index, envelope) in envelopes.iter().enumerate() {
        let change = &envelope.change;
        let row = rows
            .entry((change.table.as_str(), change.key.as_slice()))
            .or_insert_with(|| here(change));
        let mut arrival = arrive(envelope, key, mode, now, row, replayed);
        if arrival == Arrival::Apply && held.contains(&index) {
            arrival = Arrival::Hold;
        }
        if arrival == Arrival::Apply {
            row.hash = change.to;
            row.last_from = Some(change.from);
            row.steering = row
                .steering
                .take()
                .and_then(|(at, _)| Some((at, change.values.get(at)?.clone())));
        }
        if change.to.is_none() && matches!(arrival, Arrival::Apply | Arrival::Skip) {
            row.tombstone = Some(envelope.clone());
        }
        arrivals.push(arrival);
    }
    arrivals
}

/// The transactions, by maker and id, that cannot be applied: one of their
/// distinct unrefused members is missing or neither applied nor skipped. The
/// same id from two nodes is two transactions, and a member is counted once
/// however many copies arrive.
fn broken<'a>(envelopes: &'a [Envelope], arrivals: &[Arrival]) -> BTreeSet<(&'a str, &'a str)> {
    let mut broken = BTreeSet::new();
    for (envelope, arrival) in envelopes.iter().zip(arrivals) {
        let Some(txn) = &envelope.txn else { continue };
        if *arrival == Arrival::Refused {
            continue;
        }
        let members: BTreeSet<&[u8; 32]> = envelopes
            .iter()
            .zip(arrivals)
            .filter(|(other, arrival)| {
                **arrival != Arrival::Refused
                    && other.node == envelope.node
                    && other.txn.as_ref().is_some_and(|other| other.id == txn.id)
            })
            .map(|(other, _)| &other.signature)
            .collect();
        if members.len() != txn.count as usize || !matches!(arrival, Arrival::Apply | Arrival::Skip)
        {
            broken.insert((envelope.node.as_str(), txn.id.as_str()));
        }
    }
    broken
}

fn arrive(
    envelope: &Envelope,
    key: &[u8; 32],
    mode: SyncMode,
    now: u64,
    here: &Here,
    replayed: bool,
) -> Arrival {
    if !envelope.verify(key) {
        return Arrival::Refused;
    }
    if now > envelope.sunset {
        return Arrival::Dropped(Reason::Sunset);
    }
    let change = &envelope.change;
    let tombstone = here
        .tombstone
        .as_ref()
        .filter(|tombstone| now <= tombstone.sunset);
    // The change that brought the row here, as decide() reads it: for an
    // edit built on a row in the graveyard, its delete.
    let last = match tombstone {
        Some(tombstone) if change.to.is_some() && change.from == tombstone.change.from => {
            Some(tombstone.change.clone())
        }
        _ => here.last_from.map(|from| Change {
            op: change.op,
            table: change.table.clone(),
            key: change.key.clone(),
            from,
            to: here.hash,
            values: Vec::new(),
        }),
    };
    let decision = decide(change, here.hash, last.as_ref());
    let insert = decision == Decision::Apply && change.from.is_none();
    if !replayed && tombstone.is_some() && (insert || decision == Decision::Hold) {
        return Arrival::Dropped(Reason::Graveyard);
    }
    match decision {
        Decision::Apply => Arrival::Apply,
        Decision::Skip => Arrival::Skip,
        // The last change applied here was built on this one's `to`: the
        // row has moved past it, so a late copy is a repeat, not a gap
        // (docs/mesh-protocol.md: "a peer whose last applied from equals
        // the change's to built on it").
        Decision::Hold if change.to.is_some() && here.last_from == Some(change.to) => Arrival::Skip,
        Decision::Hold => Arrival::Hold,
        Decision::Conflict => match (mode, &here.steering) {
            (SyncMode::Last, Some((at, mine))) => {
                match change
                    .values
                    .get(*at)
                    .map(|theirs| further_on(theirs, mine))
                {
                    Some(Ordering::Greater) => Arrival::Apply,
                    Some(Ordering::Less) => Arrival::Skip,
                    _ => Arrival::Stop,
                }
            }
            _ => Arrival::Stop,
        },
    }
}

/// Re-sends the change in the dropped log `log` whose change hash is
/// `hash`, with the fresh `sunset` and signed with `key`, overriding its
/// drop; [`receive_replayed`] then decides it like any change. `None` when the log
/// holds no such change.
#[must_use]
pub fn replay(log: &[Envelope], hash: &[u8; 32], sunset: u64, key: &[u8; 32]) -> Option<Envelope> {
    let mut envelope = log
        .iter()
        .find(|envelope| &envelope.signature == hash)?
        .clone();
    envelope.sunset = sunset;
    envelope.sign(key);
    Some(envelope)
}

#[cfg(test)]
mod tests {
    use super::{Arrival, Envelope, Here, Txn, VERSION, receive};
    use crate::change::{Change, Op};
    use crate::plan::SyncMode;
    use crate::record::Reason;
    use crate::row_hash::row_hash;
    use crate::shape::Stamp;

    const KEY: [u8; 32] = [7; 32];

    fn signed(from: Option<[u8; 16]>, values: &[&str]) -> Envelope {
        let mut envelope = Envelope {
            v: VERSION,
            node: "a".to_owned(),
            change: Change {
                op: Op::Upsert,
                table: "notes".to_owned(),
                key: vec!["1".to_owned()],
                from,
                to: Some(row_hash(values)),
                values: values.iter().map(|&value| value.to_owned()).collect(),
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

    #[test]
    fn a_place_whose_last_change_came_from_the_same_from_sees_the_conflict() {
        let (f, a) = (row_hash(&["1", "'f'"]), row_hash(&["1", "'a'"]));
        let here = Here {
            hash: Some(a),
            last_from: Some(Some(f)),
            steering: None,
            tombstone: None,
        };
        let b = signed(Some(f), &["1", "'b'"]);
        assert_eq!(
            receive(&[b], &KEY, SyncMode::Stop, 0, |_| here.clone()),
            [Arrival::Stop]
        );
    }

    #[test]
    fn a_change_built_on_the_last_applied_one_applies() {
        let (f, a) = (row_hash(&["1", "'f'"]), row_hash(&["1", "'a'"]));
        let here = Here {
            hash: Some(a),
            last_from: Some(Some(f)),
            steering: None,
            tombstone: None,
        };
        let c = signed(Some(a), &["1", "'c'"]);
        assert_eq!(
            receive(&[c], &KEY, SyncMode::Stop, 0, |_| here.clone()),
            [Arrival::Apply]
        );
    }

    fn in_txn(mut envelope: Envelope, count: u32) -> Envelope {
        envelope.txn = Some(Txn {
            id: "t1".to_owned(),
            count,
        });
        envelope.sign(&KEY);
        envelope
    }

    #[test]
    fn copies_of_one_member_do_not_complete_a_transaction() {
        let f = row_hash(&["1", "'f'"]);
        let here = Here {
            hash: Some(f),
            last_from: None,
            steering: None,
            tombstone: None,
        };
        let member = in_txn(signed(Some(f), &["1", "'a'"]), 2);
        assert_eq!(
            receive(&[member.clone(), member], &KEY, SyncMode::Stop, 0, |_| here
                .clone()),
            [Arrival::Hold, Arrival::Hold]
        );
    }

    #[test]
    fn the_same_txn_id_from_another_node_is_another_transaction() {
        let f = row_hash(&["1", "'f'"]);
        let here = Here {
            hash: Some(f),
            last_from: None,
            steering: None,
            tombstone: None,
        };
        let mine = in_txn(signed(Some(f), &["1", "'a'"]), 2);
        let mut theirs = signed(Some(f), &["1", "'b'"]);
        "b".clone_into(&mut theirs.node);
        let theirs = in_txn(theirs, 2);
        assert_eq!(
            receive(&[mine, theirs], &KEY, SyncMode::Stop, 0, |_| here.clone()),
            [Arrival::Hold, Arrival::Hold]
        );
    }

    #[test]
    fn a_replayed_change_already_applied_is_skipped() {
        let (f, a) = (row_hash(&["1", "'f'"]), row_hash(&["1", "'a'"]));
        let here = Here {
            hash: Some(a),
            last_from: Some(Some(f)),
            steering: None,
            tombstone: None,
        };
        let replay = signed(Some(f), &["1", "'a'"]);
        assert_eq!(
            receive(&[replay], &KEY, SyncMode::Stop, 0, |_| here.clone()),
            [Arrival::Skip]
        );
    }

    #[test]
    fn a_late_copy_of_a_change_the_row_moved_past_is_skipped() {
        let (f, a, b) = (
            row_hash(&["1", "'f'"]),
            row_hash(&["1", "'a'"]),
            row_hash(&["1", "'b'"]),
        );
        let here = Here {
            hash: Some(b),
            last_from: Some(Some(a)),
            steering: None,
            tombstone: None,
        };
        let late = signed(Some(f), &["1", "'a'"]);
        assert_eq!(
            receive(&[late], &KEY, SyncMode::Stop, 0, |_| here.clone()),
            [Arrival::Skip]
        );
    }

    #[test]
    fn a_steering_position_past_the_values_stops_without_panicking() {
        let (f, a) = (row_hash(&["1", "'f'"]), row_hash(&["1", "'a'"]));
        let here = Here {
            hash: Some(a),
            last_from: Some(Some(f)),
            steering: Some((9, "'2026'".to_owned())),
            tombstone: None,
        };
        let b = signed(Some(f), &["1", "'b'"]);
        assert_eq!(
            receive(&[b], &KEY, SyncMode::Last, 0, |_| here.clone()),
            [Arrival::Stop]
        );
    }

    fn from_node(mut envelope: Envelope, node: &str) -> Envelope {
        node.clone_into(&mut envelope.node);
        envelope.sign(&KEY);
        envelope
    }

    #[test]
    fn a_same_from_conflict_in_one_call_applies_the_first_and_stops_the_second() {
        let f = row_hash(&["1", "'f'"]);
        let here = Here {
            hash: Some(f),
            last_from: None,
            steering: None,
            tombstone: None,
        };
        let a = signed(Some(f), &["1", "'a'"]);
        let b = from_node(signed(Some(f), &["1", "'b'"]), "b");
        assert_eq!(
            receive(&[a, b], &KEY, SyncMode::Stop, 0, |_| here.clone()),
            [Arrival::Apply, Arrival::Stop]
        );
    }

    #[test]
    fn a_same_from_conflict_in_one_call_under_last_keeps_the_copy_further_on() {
        let f = row_hash(&["1", "'f'"]);
        let here = Here {
            hash: Some(f),
            last_from: None,
            steering: Some((1, "'f'".to_owned())),
            tombstone: None,
        };
        let a = signed(Some(f), &["1", "'z'"]);
        let b = from_node(signed(Some(f), &["1", "'m'"]), "b");
        let batch = receive(&[a.clone(), b.clone()], &KEY, SyncMode::Last, 0, |_| {
            here.clone()
        });
        let first = receive(std::slice::from_ref(&a), &KEY, SyncMode::Last, 0, |_| {
            here.clone()
        });
        let after = Here {
            hash: a.change.to,
            last_from: Some(a.change.from),
            steering: Some((1, "'z'".to_owned())),
            tombstone: None,
        };
        let second = receive(&[b], &KEY, SyncMode::Last, 0, |_| after.clone());
        assert_eq!(batch, [Arrival::Apply, Arrival::Skip]);
        assert_eq!(batch, [first, second].concat());
    }

    #[test]
    fn a_transaction_changing_one_row_twice_in_one_call_applies() {
        let (f, a) = (row_hash(&["1", "'f'"]), row_hash(&["1", "'a'"]));
        let here = Here {
            hash: Some(f),
            last_from: None,
            steering: None,
            tombstone: None,
        };
        let first = in_txn(signed(Some(f), &["1", "'a'"]), 2);
        let second = in_txn(signed(Some(a), &["1", "'b'"]), 2);
        assert_eq!(
            receive(&[first, second], &KEY, SyncMode::Stop, 0, |_| here.clone()),
            [Arrival::Apply, Arrival::Apply]
        );
    }

    #[test]
    fn a_refused_envelope_is_not_a_member_of_the_transaction_it_names() {
        let f = row_hash(&["1", "'f'"]);
        let here = Here {
            hash: Some(f),
            last_from: None,
            steering: None,
            tombstone: None,
        };
        let member = in_txn(signed(Some(f), &["1", "'a'"]), 1);
        let mut forged = in_txn(signed(Some(f), &["1", "'b'"]), 1);
        forged.signature = [9; 32];
        assert_eq!(
            receive(&[member, forged], &KEY, SyncMode::Stop, 0, |_| here.clone()),
            [Arrival::Apply, Arrival::Refused]
        );
    }

    #[test]
    fn a_different_key_is_refused() {
        let envelope = signed(None, &["1", "'a'"]);
        assert!(envelope.verify(&KEY));
        assert!(!envelope.verify(&[8; 32]));
    }

    #[test]
    fn an_insert_after_a_delete_in_one_call_is_dropped_for_the_graveyard() {
        let f = row_hash(&["1", "'f'"]);
        let here = Here {
            hash: Some(f),
            last_from: None,
            steering: None,
            tombstone: None,
        };
        let mut delete = signed(Some(f), &["1", "'f'"]);
        delete.change.op = Op::Down;
        delete.change.to = None;
        delete.change.values = Vec::new();
        delete.sunset = 100;
        delete.sign(&KEY);
        let mut insert = signed(None, &["1", "'again'"]);
        insert.sunset = 100;
        insert.sign(&KEY);
        assert_eq!(
            receive(&[delete, insert], &KEY, SyncMode::Stop, 0, |_| here.clone()),
            [Arrival::Apply, Arrival::Dropped(Reason::Graveyard)]
        );
    }
}
