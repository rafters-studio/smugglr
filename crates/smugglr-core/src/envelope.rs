//! The signed envelope multi and the mesh carry each Change in
//! (FR-SMUGGLR-075).
//!
//! An [`Envelope`] is a [`Change`] plus `node`, `targets`, `sunset`, `txn`,
//! `key_id`, the table's shape stamp and a signature over all of them; the
//! row's values follow, bound by `to`, the hash they must have. On arrival
//! each change goes through the one decision path ([`decide`]): skipped when
//! its `to` is here, applied when its `from` is, held when its `from`
//! matches nothing here, and a conflict when the place's last change of the
//! row came from the same `from` to a different `to`, which `--sync-mode`
//! settles (stop or last). Changes that were one transaction at the source
//! are applied all or none. A change whose signature does not match its
//! envelope, or whose values do not hash to its `to`, is refused.
//!
//! The signature is BLAKE3 keyed with the shared key the caller chooses.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use crate::change::{Change, Decision, Op, decide};
use crate::plan::SyncMode;
use crate::row_hash::row_hash;
use crate::shape::Stamp;
use crate::steering::further_on;

/// The envelope format version, `v`.
pub const VERSION: u32 = 1;

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
    /// When the change stops being carried.
    pub sunset: u64,
    /// The source transaction the change was part of, if any.
    pub txn: Option<Txn>,
    /// The shared key that signed the envelope.
    pub key_id: String,
    /// The table's shape stamp.
    pub stamp: Stamp,
    /// The signature over every field above but the values.
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
}

/// What a place does with an envelope that arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arrival {
    /// Write the change's values; the place keeps its `from` as the key's
    /// last applied `from`.
    Apply,
    /// Leave the row: it is already at the change's `to`, or under
    /// `--sync-mode last` the copy here is further on.
    Skip,
    /// The change waits: its `from` matches nothing here, or another change
    /// of its transaction cannot be applied yet.
    Hold,
    /// The row changed here and in the change from the same `from`, and
    /// `--sync-mode` stops the row: name it.
    Stop,
    /// The signature does not match the envelope under the key, or the
    /// values do not hash to the change's `to`.
    Refused,
}

/// Decides each of `envelopes` arriving together, in order, verified with
/// `key`, against the row `here` gives for its change. A conflict follows
/// `mode`: under [`SyncMode::Last`] the copy whose steering value is further
/// on is kept, and a row whose two values are equal, or that has none,
/// stops; under every other mode the row stops. A change of a transaction
/// is applied only when all of the transaction's changes have arrived and
/// each is applied or skipped; otherwise it is held.
pub fn receive(
    envelopes: &[Envelope],
    key: &[u8; 32],
    mode: SyncMode,
    here: impl Fn(&Change) -> Here,
) -> Vec<Arrival> {
    let mut arrivals: Vec<Arrival> = envelopes
        .iter()
        .map(|envelope| arrive(envelope, key, mode, &here(&envelope.change)))
        .collect();
    let mut broken = BTreeSet::new();
    for (envelope, arrival) in envelopes.iter().zip(&arrivals) {
        let Some(txn) = &envelope.txn else { continue };
        let arrived = envelopes
            .iter()
            .filter(|other| other.txn.as_ref().is_some_and(|other| other.id == txn.id))
            .count();
        if arrived != txn.count as usize || !matches!(arrival, Arrival::Apply | Arrival::Skip) {
            broken.insert(txn.id.as_str());
        }
    }
    for (envelope, arrival) in envelopes.iter().zip(&mut arrivals) {
        let in_broken = envelope
            .txn
            .as_ref()
            .is_some_and(|txn| broken.contains(txn.id.as_str()));
        if in_broken && *arrival == Arrival::Apply {
            *arrival = Arrival::Hold;
        }
    }
    arrivals
}

fn arrive(envelope: &Envelope, key: &[u8; 32], mode: SyncMode, here: &Here) -> Arrival {
    if !envelope.verify(key) {
        return Arrival::Refused;
    }
    let change = &envelope.change;
    // The change that brought the row here, as decide() reads it.
    let last = here.last_from.map(|from| Change {
        op: change.op,
        table: change.table.clone(),
        key: change.key.clone(),
        from,
        to: here.hash,
        values: Vec::new(),
    });
    match decide(change, here.hash, last.as_ref()) {
        Decision::Apply => Arrival::Apply,
        Decision::Skip => Arrival::Skip,
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

#[cfg(test)]
mod tests {
    use super::{Arrival, Envelope, Here, VERSION, receive};
    use crate::change::{Change, Op};
    use crate::plan::SyncMode;
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
        };
        let b = signed(Some(f), &["1", "'b'"]);
        assert_eq!(
            receive(&[b], &KEY, SyncMode::Stop, |_| here.clone()),
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
        };
        let c = signed(Some(a), &["1", "'c'"]);
        assert_eq!(
            receive(&[c], &KEY, SyncMode::Stop, |_| here.clone()),
            [Arrival::Apply]
        );
    }

    #[test]
    fn a_different_key_is_refused() {
        let envelope = signed(None, &["1", "'a'"]);
        assert!(envelope.verify(&KEY));
        assert!(!envelope.verify(&[8; 32]));
    }
}
