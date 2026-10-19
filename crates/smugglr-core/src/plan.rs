//! What a direct sync does to one table (FR-SMUGGLR-072).
//!
//! Each key's last hash per pair of places, from the last done run, alone
//! decides what changed: a key memory does not know is new and is copied; a
//! known key missing on one side was deleted there and the delete is carried;
//! a row whose hash matches memory did not change on that side; a row changed
//! on both sides follows `--sync-mode` (FR-SMUGGLR-067). No clock, key order
//! or steering column is read.

use std::collections::{BTreeMap, BTreeSet};

use crate::change::{Change, Decision, Op, decide, op};

/// Each row's hash by key, the key as SQLite's `quote()` text of each key
/// column.
pub type Hashes = BTreeMap<Vec<String>, [u8; 16]>;

/// What a sync does with a row changed in both places (`--sync-mode`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SyncMode {
    /// Stop the row's table and name the row.
    #[default]
    Stop,
    /// Keep the source's copy.
    Source,
    /// Keep the target's copy.
    Target,
}

/// One of the two places of a direct sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    /// The first place named.
    Source,
    /// The second place named.
    Target,
}

/// One row to write, in `side`, with the other side's copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write {
    /// The place written.
    pub side: Side,
    /// What the write does.
    pub op: Op,
    /// The row's key.
    pub key: Vec<String>,
    /// The row's hash in `side` before the write; `None` when there is none.
    pub from: Option<[u8; 16]>,
    /// The row's hash after it; `None` when the row is deleted.
    pub to: Option<[u8; 16]>,
    /// The row changed in both places and `--sync-mode` replaces `side`'s
    /// copy.
    pub overwrite: bool,
}

/// What a sync does to one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Write these rows (none when the table is in step); afterwards both
    /// places hold `memory`.
    Move {
        /// The rows to write.
        writes: Vec<Write>,
        /// Each key's hash in both places once the writes land: the next
        /// run's memory, built from the rows this run read and writes.
        memory: Hashes,
    },
    /// These rows changed in both places; nothing in the table is written.
    Stop {
        /// The rows' keys.
        rows: Vec<Vec<String>>,
    },
}

/// Plans one table from each place's hashes and memory's last hashes,
/// through the one decision path: each place's difference from memory is a
/// [`Change`] (from memory's hash to that place's), decided by [`decide`]
/// against the other place's row and the other place's own change.
#[must_use]
pub fn plan(
    table: &str,
    source: &Hashes,
    target: &Hashes,
    memory: &Hashes,
    mode: SyncMode,
) -> Plan {
    let keys: BTreeSet<&Vec<String>> = source
        .keys()
        .chain(target.keys())
        .chain(memory.keys())
        .collect();
    let mut writes = Vec::new();
    let mut next = Hashes::new();
    let mut stopped = Vec::new();
    for key in keys {
        let (s, t, m) = (
            source.get(key).copied(),
            target.get(key).copied(),
            memory.get(key).copied(),
        );
        let change = |to: Option<[u8; 16]>| {
            (to != m).then(|| Change {
                op: op(m, to),
                table: table.to_owned(),
                key: key.clone(),
                from: m,
                to,
                values: Vec::new(),
            })
        };
        let (ours, theirs) = (change(s), change(t));
        // Each place's change, decided where it would land.
        let decided = match (&ours, &theirs) {
            (None, None) => Decision::Skip,
            (Some(ours), _) => decide(ours, t, theirs.as_ref()),
            (None, Some(theirs)) => decide(theirs, s, None),
        };
        let side = if ours.is_some() {
            Side::Target
        } else {
            Side::Source
        };
        let (side, overwrite) = match (decided, mode) {
            (Decision::Skip, _) => {
                if let Some(hash) = s {
                    next.insert(key.clone(), hash);
                }
                continue;
            }
            (Decision::Apply, _) => (side, false),
            (Decision::Conflict, SyncMode::Source) => (Side::Target, true),
            (Decision::Conflict, SyncMode::Target) => (Side::Source, true),
            (Decision::Conflict | Decision::Hold, _) => {
                stopped.push(key.clone());
                continue;
            }
        };
        let (from, to) = match side {
            Side::Target => (t, s),
            Side::Source => (s, t),
        };
        if let Some(hash) = to {
            next.insert(key.clone(), hash);
        }
        writes.push(Write {
            side,
            op: op(from, to),
            key: key.clone(),
            from,
            to,
            overwrite,
        });
    }
    if stopped.is_empty() {
        Plan::Move {
            writes,
            memory: next,
        }
    } else {
        Plan::Stop { rows: stopped }
    }
}

/// Takes one journal line of a run cut off while writing (FR-SMUGGLR-073)
/// into `memory`: when the row is now at the write's `to` (`here` is its
/// hash in the place written), the write counts as done and memory takes
/// `to`; otherwise memory keeps the write's `from`, so the next plan carries
/// the write again, or stops on a row changed since the cut.
pub fn recover(memory: &mut Hashes, write: &Write, here: Option<[u8; 16]>) {
    let agreed = if here == write.to {
        write.to
    } else {
        write.from
    };
    match agreed {
        Some(hash) => memory.insert(write.key.clone(), hash),
        None => memory.remove(&write.key),
    };
}

/// The keys of `writes` whose rows, read back after writing, do not hash to
/// what was written (FR-SMUGGLR-064): `read_back` is each written row's hash
/// as its place now holds it, a deleted row absent.
#[must_use]
pub fn not_read_back(writes: &[Write], read_back: &Hashes) -> Vec<Vec<String>> {
    writes
        .iter()
        .filter(|write| read_back.get(&write.key).copied() != write.to)
        .map(|write| write.key.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{Hashes, Plan, Side, SyncMode, Write, plan, recover};
    use crate::change::Op;

    #[test]
    fn a_journaled_write_counts_only_when_its_row_is_at_to() {
        let key = vec!["1".to_owned()];
        let write = Write {
            side: Side::Target,
            op: Op::Upsert,
            key: key.clone(),
            from: Some([1; 16]),
            to: Some([2; 16]),
            overwrite: false,
        };
        let mut memory = Hashes::new();
        recover(&mut memory, &write, Some([2; 16]));
        assert_eq!(memory.get(&key), Some(&[2; 16]));
        recover(&mut memory, &write, Some([3; 16]));
        assert_eq!(memory.get(&key), Some(&[1; 16]));
    }

    #[test]
    fn a_first_sync_of_equal_rows_is_in_step() {
        let hashes = Hashes::from([(vec!["1".to_owned()], [1; 16])]);
        assert_eq!(
            plan("t", &hashes, &hashes, &Hashes::new(), SyncMode::Stop),
            Plan::Move {
                writes: Vec::new(),
                memory: hashes.clone()
            }
        );
    }
}
