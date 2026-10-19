//! The record: what a sync did, table by table, in smugglr's own words
//! (FR-SMUGGLR-063, FR-SMUGGLR-067, FR-SMUGGLR-069).
//!
//! Key lists name rows only where they were deleted, overwritten by
//! `--sync-mode`, or stopped; rows copied or carried over appear only in the
//! counts. No word for one copy being better than another appears in it.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Serialize, Serializer};

use crate::change::Op;
use crate::plan::{Plan, Side};

/// Why a table stops when rows changed in both places.
pub const CHANGED_IN_BOTH: &str = "These rows changed in both places since the last sync. \
     smugglr stops rather than choose. Rerun with --sync-mode source or target to keep one \
     place's copy, or name a steering column and use --sync-mode last.";

/// Why a table stops when rows it wrote read back different.
pub const NOT_READ_BACK: &str = "smugglr wrote these rows and read them back different from \
     what it wrote, so the run is not done. Something in the place changes rows as they are \
     written.";

/// Why a table stops when its database hands out its own keys
/// (FR-SMUGGLR-069).
pub const SELF_KEYED: &str = "This table's database hands out its own keys (INTEGER PRIMARY \
     KEY), so two places would hand out the same key to different rows and one would erase the \
     other. smugglr does no re-sequencing. Any random, unique key, such as UUIDv7, lets it sync.";

/// What a run did as a whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Status {
    /// A dry run: what a sync would do; nothing was written.
    #[serde(rename = "preview")]
    Preview,
    /// Every table that had rows to move moved.
    #[serde(rename = "done")]
    Done,
    /// Some tables moved and some stopped.
    #[serde(rename = "stopped in part")]
    StoppedInPart,
    /// Tables stopped and none moved.
    #[serde(rename = "stopped")]
    Stopped,
}

/// What a sync did: one status for the run, one entry per table by name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Record {
    /// The run's status.
    pub status: Status,
    /// Each table's part, by name.
    pub tables: BTreeMap<String, Table>,
}

impl Record {
    /// The record of a run whose tables did `tables`; a dry run's when
    /// `dryrun`.
    #[must_use]
    pub fn new(tables: BTreeMap<String, Table>, dryrun: bool) -> Self {
        let any = |f: fn(&Table) -> bool| tables.values().any(f);
        let status = if dryrun {
            Status::Preview
        } else if !any(|t| matches!(t, Table::Stopped { .. })) {
            Status::Done
        } else if any(|t| matches!(t, Table::Moved(_))) {
            Status::StoppedInPart
        } else {
            Status::Stopped
        };
        Self { status, tables }
    }

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
    /// A dry run found rows to move.
    #[serde(rename = "would move")]
    WouldMove(Moves),
    /// Rows moved.
    #[serde(rename = "moved")]
    Moved(Moves),
    /// The table did not move: nothing in it was written, or rows it wrote
    /// read back different.
    #[serde(rename = "stopped")]
    Stopped {
        /// The rows that stopped it, by key.
        rows: Vec<Key>,
        /// Rows written that read back different from what was written.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        not_read_back: Vec<Key>,
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

/// The record of one table's plan: once its writes have landed, or, in a
/// dry run, what they would be.
#[must_use]
pub fn table(plan: &Plan, dryrun: bool) -> Table {
    match plan {
        Plan::Stop { rows } => Table::Stopped {
            rows: rows.iter().cloned().map(Key).collect(),
            not_read_back: Vec::new(),
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
            if dryrun {
                Table::WouldMove(moves)
            } else {
                Table::Moved(moves)
            }
        }
    }
}

/// The record of a table whose written rows `keys` read back different.
#[must_use]
pub fn not_read_back(keys: Vec<Vec<String>>) -> Table {
    Table::Stopped {
        rows: Vec::new(),
        not_read_back: keys.into_iter().map(Key).collect(),
        why: NOT_READ_BACK.to_owned(),
    }
}

/// The record of a table whose database hands out its own keys: stopped,
/// with no rows read.
#[must_use]
pub fn self_keyed() -> Table {
    Table::Stopped {
        rows: Vec::new(),
        not_read_back: Vec::new(),
        why: SELF_KEYED.to_owned(),
    }
}

fn keys(keys: &[Key]) -> String {
    keys.iter()
        .map(|key| {
            key.0
                .iter()
                .map(|c| shown(c))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// The record as text: the run's status, then each table with its status
/// and counts, and the rows it names.
impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = match self.status {
            Status::Preview => "preview",
            Status::Done => "done",
            Status::StoppedInPart => "stopped in part",
            Status::Stopped => "stopped",
        };
        writeln!(f, "smugglr sync: {status}")?;
        for (name, table) in &self.tables {
            match table {
                Table::InStep => writeln!(f, "  {name}: in step")?,
                Table::WouldMove(moves) | Table::Moved(moves) => {
                    let status = if matches!(table, Table::Moved(_)) {
                        "moved"
                    } else {
                        "would move"
                    };
                    writeln!(
                        f,
                        "  {name}: {status}, {} to target, {} to source, {} deleted in target, \
                         {} deleted in source, {} overwritten in target, {} overwritten in source",
                        moves.to_target,
                        moves.to_source,
                        moves.deleted_in_target.len(),
                        moves.deleted_in_source.len(),
                        moves.overwritten_in_target.len(),
                        moves.overwritten_in_source.len(),
                    )?;
                    for (label, list) in [
                        ("deleted in target", &moves.deleted_in_target),
                        ("deleted in source", &moves.deleted_in_source),
                        ("overwritten in target", &moves.overwritten_in_target),
                        ("overwritten in source", &moves.overwritten_in_source),
                    ] {
                        if !list.is_empty() {
                            writeln!(f, "    {label}: {}", keys(list))?;
                        }
                    }
                }
                Table::Stopped {
                    rows,
                    not_read_back,
                    why,
                } => {
                    writeln!(f, "  {name}: stopped")?;
                    if !rows.is_empty() {
                        writeln!(f, "    rows: {}", keys(rows))?;
                    }
                    if !not_read_back.is_empty() {
                        writeln!(f, "    not read back: {}", keys(not_read_back))?;
                    }
                    writeln!(f, "    {why}")?;
                }
            }
        }
        Ok(())
    }
}
