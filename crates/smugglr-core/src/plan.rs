//! What a direct sync does to one table (FR-SMUGGLR-072).
//!
//! Each key's last hash per pair of places, from the last done run, alone
//! decides what changed: a key memory does not know is new and is copied; a
//! known key missing on one side was deleted there and the delete is carried;
//! a row whose hash matches memory did not change on that side; a row changed
//! on both sides follows `--sync-mode` (FR-SMUGGLR-067). No clock or key
//! order is read, and a steering column only under `--sync-mode last`
//! (FR-SMUGGLR-068), and only for rows changed on both sides.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::change::{Change, Decision, Op, decide, op};
use crate::steering::further_on;

/// Each row's hash by key, the key as SQLite's `quote()` text of each key
/// column.
pub type Hashes = BTreeMap<Vec<String>, [u8; 16]>;

/// Each row's steering value in the source and in the target, by key, as
/// SQLite's `quote()` text; for `--sync-mode last`.
pub type Steered = BTreeMap<Vec<String>, (String, String)>;

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
    /// Keep the copy whose steering column is further on; stop the row's
    /// table and name the row when the two values are equal.
    Last,
}

/// One of the two places of a direct sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
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
///
/// Under [`SyncMode::Last`] no steering value is known here, so every row
/// changed in both places stops; [`plan_steered`] takes them.
#[must_use]
pub fn plan(
    table: &str,
    source: &Hashes,
    target: &Hashes,
    memory: &Hashes,
    mode: SyncMode,
) -> Plan {
    plan_steered(table, source, target, memory, mode, &Steered::new())
}

/// Each key of the table and what the one decision path makes of it, with
/// the side a write of it would land in.
fn decisions<'a>(
    table: &str,
    source: &'a Hashes,
    target: &'a Hashes,
    memory: &'a Hashes,
) -> impl Iterator<Item = (&'a Vec<String>, Decision, Side)> {
    let keys: BTreeSet<&Vec<String>> = source
        .keys()
        .chain(target.keys())
        .chain(memory.keys())
        .collect();
    keys.into_iter().map(move |key| {
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
        (key, decided, side)
    })
}

/// The keys of the rows changed in both places since memory's last hashes:
/// those whose steering values `--sync-mode last` needs.
#[must_use]
pub fn changed_in_both(source: &Hashes, target: &Hashes, memory: &Hashes) -> Vec<Vec<String>> {
    decisions("", source, target, memory)
        .filter(|(_, decided, _)| *decided == Decision::Conflict)
        .map(|(key, _, _)| key.clone())
        .collect()
}

/// [`plan`], with each row's steering values for [`SyncMode::Last`]: of a
/// row changed in both places it keeps the copy whose steering value is
/// further on, and stops on a row whose two values are equal or that has
/// none in `steered` (a row deleted on one side has none there).
#[must_use]
pub fn plan_steered(
    table: &str,
    source: &Hashes,
    target: &Hashes,
    memory: &Hashes,
    mode: SyncMode,
    steered: &Steered,
) -> Plan {
    let mut writes = Vec::new();
    let mut next = Hashes::new();
    let mut stopped = Vec::new();
    for (key, decided, side) in decisions(table, source, target, memory) {
        let (s, t) = (source.get(key).copied(), target.get(key).copied());
        let last = || {
            let (here, there) = steered.get(key)?;
            match further_on(here, there) {
                Ordering::Greater => Some(Side::Target),
                Ordering::Less => Some(Side::Source),
                Ordering::Equal => None,
            }
        };
        // Under last, the side a row changed in both places is written in.
        let kept = (decided == Decision::Conflict && mode == SyncMode::Last)
            .then(last)
            .flatten();
        let (side, overwrite) = match (decided, mode, kept) {
            (Decision::Skip, ..) => {
                if let Some(hash) = s {
                    next.insert(key.clone(), hash);
                }
                continue;
            }
            (Decision::Apply, ..) => (side, false),
            (Decision::Conflict, SyncMode::Source, _) => (Side::Target, true),
            (Decision::Conflict, SyncMode::Target, _) => (Side::Source, true),
            (Decision::Conflict, SyncMode::Last, Some(written)) => (written, true),
            (Decision::Conflict | Decision::Hold, ..) => {
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
    use std::collections::BTreeSet;

    use super::{
        Hashes, Plan, Side, Steered, SyncMode, Write, changed_in_both, plan, plan_steered, recover,
    };
    use crate::change::{Op, op};

    /// FR-SMUGGLR-072's per-key rules, as a direct sync planned before it
    /// decided through the one decision path.
    fn per_key(source: &Hashes, target: &Hashes, memory: &Hashes, mode: SyncMode) -> Plan {
        let keys: BTreeSet<&Vec<String>> = source
            .keys()
            .chain(target.keys())
            .chain(memory.keys())
            .collect();
        let (mut writes, mut next, mut stopped) = (Vec::new(), Hashes::new(), Vec::new());
        for key in keys {
            let (s, t, m) = (
                source.get(key).copied(),
                target.get(key).copied(),
                memory.get(key).copied(),
            );
            if s == t {
                if let Some(hash) = s {
                    next.insert(key.clone(), hash);
                }
                continue;
            }
            let (side, from, to, overwrite) = match (s != m, t != m, mode) {
                (true, false, _) => (Side::Target, t, s, false),
                (false, true, _) => (Side::Source, s, t, false),
                (_, _, SyncMode::Source) => (Side::Target, t, s, true),
                (_, _, SyncMode::Target) => (Side::Source, s, t, true),
                // Without steering values, last stops as stop does.
                (_, _, SyncMode::Stop | SyncMode::Last) => {
                    stopped.push(key.clone());
                    continue;
                }
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

    /// FR-SMUGGLR-075: deciding through the one decision path gives the
    /// same writes, stops and memory as the per-key rules, for every state
    /// of a two-key table (each key absent or at one of three hashes in the
    /// source, the target and memory) under every sync mode, last with no
    /// steering values.
    #[test]
    fn the_one_decision_path_plans_as_the_per_key_rules() {
        let hashes = [None, Some([1; 16]), Some([2; 16]), Some([3; 16])];
        let mut states = Vec::new();
        for s in hashes {
            for t in hashes {
                for m in hashes {
                    states.push([s, t, m]);
                }
            }
        }
        let mut compared = 0;
        for a in &states {
            for b in &states {
                let mut places = [Hashes::new(), Hashes::new(), Hashes::new()];
                for (key, state) in [("1", a), ("2", b)] {
                    for (place, hash) in places.iter_mut().zip(state) {
                        if let Some(hash) = hash {
                            place.insert(vec![key.to_owned()], *hash);
                        }
                    }
                }
                let [source, target, memory] = &places;
                for mode in [
                    SyncMode::Stop,
                    SyncMode::Source,
                    SyncMode::Target,
                    SyncMode::Last,
                ] {
                    assert_eq!(
                        plan("t", source, target, memory, mode),
                        per_key(source, target, memory, mode),
                        "{a:?} {b:?} {mode:?}"
                    );
                    compared += 1;
                }
            }
        }
        assert_eq!(compared, 64 * 64 * 4);
    }

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

    /// Memory, and a row changed differently in each place since.
    fn both_changed() -> (Hashes, Hashes, Hashes) {
        let key = vec!["'o1'".to_owned()];
        (
            Hashes::from([(key.clone(), [2; 16])]),
            Hashes::from([(key.clone(), [3; 16])]),
            Hashes::from([(key, [1; 16])]),
        )
    }

    #[test]
    fn last_keeps_the_copy_further_on() {
        let (source, target, memory) = both_changed();
        let key = vec!["'o1'".to_owned()];
        assert_eq!(
            changed_in_both(&source, &target, &memory),
            std::slice::from_ref(&key)
        );
        let steered = Steered::from([(key.clone(), ("9".to_owned(), "10".to_owned()))]);
        let planned = plan_steered("t", &source, &target, &memory, SyncMode::Last, &steered);
        let kept = match &planned {
            Plan::Move { writes, .. } => writes
                .iter()
                .map(|write| (write.side, write.overwrite))
                .collect(),
            Plan::Stop { .. } => Vec::new(),
        };
        assert_eq!(kept, [(Side::Source, true)], "{planned:?}");
    }

    #[test]
    fn last_stops_on_equal_or_missing_steering_values() {
        let (source, target, memory) = both_changed();
        let key = vec!["'o1'".to_owned()];
        let equal = Steered::from([(key.clone(), ("'a'".to_owned(), "'a'".to_owned()))]);
        for steered in [equal, Steered::new()] {
            assert_eq!(
                plan_steered("t", &source, &target, &memory, SyncMode::Last, &steered),
                Plan::Stop {
                    rows: vec![key.clone()]
                }
            );
        }
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
