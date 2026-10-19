//! What a direct sync does to one table (FR-SMUGGLR-072).
//!
//! Each key's last hash per pair of places, from the last done run, alone
//! decides what changed: a key memory does not know is new and is copied; a
//! known key missing on one side was deleted there and the delete is carried;
//! a row whose hash matches memory did not change on that side; a row changed
//! on both sides stops its table. No clock, key order or steering column is
//! read.

use std::collections::{BTreeMap, BTreeSet};

use crate::change::Op;

/// Each row's hash by key, the key as SQLite's `quote()` text of each key
/// column.
pub type Hashes = BTreeMap<Vec<String>, [u8; 16]>;

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

/// Plans one table from each place's hashes and memory's last hashes.
#[must_use]
pub fn plan(source: &Hashes, target: &Hashes, memory: &Hashes) -> Plan {
    let keys: BTreeSet<&Vec<String>> = source
        .keys()
        .chain(target.keys())
        .chain(memory.keys())
        .collect();
    let mut writes = Vec::new();
    let mut next = Hashes::new();
    let mut stopped = Vec::new();
    for key in keys {
        let (s, t, m) = (source.get(key), target.get(key), memory.get(key));
        if s == t {
            if let Some(&hash) = s {
                next.insert(key.clone(), hash);
            }
            continue;
        }
        let (side, from, to) = match (s != m, t != m) {
            (true, false) => (Side::Target, t, s),
            (false, true) => (Side::Source, s, t),
            _ => {
                stopped.push(key.clone());
                continue;
            }
        };
        let op = match (from, to) {
            (_, None) => Op::Down,
            (None, Some(_)) => Op::Up,
            (Some(_), Some(_)) => Op::Upsert,
        };
        if let Some(&hash) = to {
            next.insert(key.clone(), hash);
        }
        writes.push(Write {
            side,
            op,
            key: key.clone(),
            from: from.copied(),
            to: to.copied(),
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

#[cfg(test)]
mod tests {
    use super::{Hashes, Plan, plan};

    #[test]
    fn a_first_sync_of_equal_rows_is_in_step() {
        let hashes = Hashes::from([(vec!["1".to_owned()], [1; 16])]);
        assert_eq!(
            plan(&hashes, &hashes, &Hashes::new()),
            Plan::Move {
                writes: Vec::new(),
                memory: hashes.clone()
            }
        );
    }
}
