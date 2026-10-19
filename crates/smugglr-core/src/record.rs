//! The record: what a sync did, table by table, in smugglr's own words
//! (FR-SMUGGLR-063, FR-SMUGGLR-067).
//!
//! Key lists name rows only where they were deleted, overwritten by
//! `--sync-mode`, or stopped; rows copied or carried over appear only in the
//! counts. No word for one copy being better than another appears in it.

use std::collections::BTreeMap;

use serde::{Serialize, Serializer};

use crate::change::Op;
use crate::plan::{Plan, Side};

/// Why a table stops when rows changed in both places.
pub const CHANGED_IN_BOTH: &str = "These rows changed in both places since the last sync. \
     smugglr stops rather than choose. Rerun with --sync-mode source or target to keep one \
     place's copy, or name a steering column and use --sync-mode last.";

/// What a sync did, by table name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Record {
    /// Each table's part, by name.
    pub tables: BTreeMap<String, Table>,
}

impl Record {
    /// The record as JSON.
    #[must_use]
    pub fn json(&self) -> String {
        // Every map key is a string and every value plain data, so this
        // cannot fail.
        serde_json::to_string_pretty(self).unwrap_or_default()
    }
}

/// What a sync did to one table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status")]
pub enum Table {
    /// Both places already held the same rows.
    #[serde(rename = "in step")]
    InStep,
    /// Rows moved.
    #[serde(rename = "moved")]
    Moved(Moves),
    /// Nothing in the table was written.
    #[serde(rename = "stopped")]
    Stopped {
        /// The rows that stopped it, by key.
        rows: Vec<Key>,
        /// Why, and how to go on.
        why: String,
    },
}

/// The rows a moving table moved.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Moves {
    /// Rows written in the target.
    pub to_target: usize,
    /// Rows written in the source.
    pub to_source: usize,
    /// Rows deleted in the target, carried from the source.
    pub deleted_in_target: Vec<Key>,
    /// Rows deleted in the source, carried from the target.
    pub deleted_in_source: Vec<Key>,
    /// Target rows replaced by `--sync-mode`.
    pub overwritten_in_target: Vec<Key>,
    /// Source rows replaced by `--sync-mode`.
    pub overwritten_in_source: Vec<Key>,
}

/// A row's key: SQLite's `quote()` text of each key column. A one-column
/// key is written as its value, text without its quotes; a longer one as a
/// list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Key(pub Vec<String>);

fn shown(column: &str) -> String {
    column
        .strip_prefix('\'')
        .and_then(|text| text.strip_suffix('\''))
        .map_or_else(|| column.to_owned(), |text| text.replace("''", "'"))
}

impl Serialize for Key {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0.as_slice() {
            [column] => serializer.serialize_str(&shown(column)),
            columns => serializer.collect_seq(columns.iter().map(|column| shown(column))),
        }
    }
}

/// The record of one table's plan, once its writes have landed.
#[must_use]
pub fn table(plan: &Plan) -> Table {
    match plan {
        Plan::Stop { rows } => Table::Stopped {
            rows: rows.iter().cloned().map(Key).collect(),
            why: CHANGED_IN_BOTH.to_owned(),
        },
        Plan::Move { writes, .. } if writes.is_empty() => Table::InStep,
        Plan::Move { writes, .. } => {
            let mut moves = Moves::default();
            for write in writes {
                let key = Key(write.key.clone());
                let (count, deleted, overwritten) = match write.side {
                    Side::Target => (
                        &mut moves.to_target,
                        &mut moves.deleted_in_target,
                        &mut moves.overwritten_in_target,
                    ),
                    Side::Source => (
                        &mut moves.to_source,
                        &mut moves.deleted_in_source,
                        &mut moves.overwritten_in_source,
                    ),
                };
                if write.overwrite {
                    overwritten.push(key);
                } else if write.op == Op::Down {
                    deleted.push(key);
                }
                if write.op != Op::Down {
                    *count += 1;
                }
            }
            Table::Moved(moves)
        }
    }
}
