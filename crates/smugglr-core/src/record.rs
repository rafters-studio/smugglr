//! The record: what a sync did, table by table, in smugglr's own words
//! (FR-SMUGGLR-063, FR-SMUGGLR-065, FR-SMUGGLR-067, FR-SMUGGLR-069,
//! FR-SMUGGLR-076).
//!
//! Key lists name rows only where they were deleted, overwritten by
//! `--sync-mode`, or stopped; rows copied or carried over appear only in the
//! counts. No word for one copy being better than another appears in it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Serialize, Serializer};

use crate::change::Op;
use crate::envelope::Envelope;
use crate::plan::{Plan, Side};
use crate::shape::Mismatch;
use crate::weight::Weight;

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

/// Why a row stops when an edit was made to it after it was deleted
/// elsewhere (FR-SMUGGLR-076).
pub const EDIT_VERSUS_DELETE: &str = "These rows were edited in one place after they were \
     deleted in another. smugglr stops rather than choose. Rerun with --sync-mode source or \
     target to keep one place's copy.";

/// Why a table stops when a blob changes rows that also changed here since
/// the last stash (FR-SMUGGLR-077).
pub const CHANGED_HERE_AND_IN_BLOB: &str = "These rows changed here and in the blob, each \
     from the same row. smugglr stops rather than choose. Name a steering column and \
     retrieve with --sync-mode last to keep the copy further on.";

/// Why a table is not done when a blob's changes wait for an earlier change
/// this place has not seen (FR-SMUGGLR-077).
pub const WAITING: &str = "These rows wait for an earlier change to them that this place has \
     not seen. Retrieve the blob that holds it, then this blob again.";

/// Why a table is not done when a blob's changes do not match their
/// signature under the key, or their values do not hash to their `to`.
pub const REFUSED: &str = "These changes do not match their signature under this key, or \
     their values do not match their hash. smugglr applied none of them.";

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
    /// A first sync over `max_wire`: nothing was written.
    ///
    /// Current choice (2026-10-19): its own run status, not `stopped`,
    /// because each exit code then follows from one status (3 for stopped,
    /// 5 for too much, FR-SMUGGLR-060) and "too much" is the spec's word for
    /// it; revisable.
    #[serde(rename = "too much")]
    TooMuch,
}

/// What a sync did: one status for the run, one entry per table by name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Record {
    /// The run's status.
    pub status: Status,
    /// The pair had no memory yet: a row on one side only was copied as new.
    pub first_sync: bool,
    /// Whether the run could tell a delete from a row never copied; never on
    /// a first sync.
    pub deletes_seen: bool,
    /// What a first sync moves, or would move; none on a later sync.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<Weight>,
    /// Why nothing moved, and how to go on; only when the run is too much.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
    /// Each table's part, by name.
    pub tables: BTreeMap<String, Table>,
    /// The tables the run ignored, in either place, each named once.
    pub ignored: BTreeSet<String>,
    /// The shapes of the places differ: the run stopped before any row was
    /// read (FR-SMUGGLR-070).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shape: Option<ShapeStop>,
    /// The changes the run dropped, each kept in the dropped log.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub dropped: Vec<Dropped>,
    /// A retrieve's blob's targets that still need it: its targets less
    /// those the app crossed off and this device (FR-SMUGGLR-077).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub targets_remaining: Option<Vec<String>>,
}

/// Why a change was dropped (FR-SMUGGLR-076).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// It arrived, or waited, past its sunset.
    Sunset,
    /// It is an insert or a waiting change for a key in the graveyard.
    Graveyard,
}

impl Reason {
    /// The record's words for the reason, in the text and the JSON alike.
    #[must_use]
    pub const fn words(self) -> &'static str {
        match self {
            Self::Sunset => "its sunset passed",
            Self::Graveyard => "its key is in the graveyard",
        }
    }
}

impl Serialize for Reason {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.words())
    }
}

/// A change the run dropped, named by its table, key, reason and change
/// hash, by which a replay re-sends it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Dropped {
    /// The table the change's row is in.
    pub table: String,
    /// The row's key.
    pub key: Key,
    /// Why it was dropped.
    pub reason: Reason,
    /// The change hash, as lowercase hex.
    pub change: String,
}

/// Lowercase hex digits, for the change hash.
const HEX: &[u8; 16] = b"0123456789abcdef";

impl Dropped {
    /// The record's name for `envelope`, dropped for `reason`.
    #[must_use]
    pub fn new(envelope: &Envelope, reason: Reason) -> Self {
        Self {
            table: envelope.change.table.clone(),
            key: Key(envelope.change.key.clone()),
            reason,
            change: envelope
                .signature
                .iter()
                .flat_map(|byte| [HEX[usize::from(byte >> 4)], HEX[usize::from(byte & 0xf)]])
                .map(char::from)
                .collect(),
        }
    }
}

/// A run stopped because the places' shapes differ.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShapeStop {
    /// How the shapes differ.
    #[serde(flatten)]
    pub mismatch: Mismatch,
    /// The difference in words, and how to go on.
    pub why: String,
}

impl Record {
    /// The record of a run whose tables did `tables` and which ignored the
    /// tables `ignored`; a dry run's when `dryrun`. `first_sync` is a first
    /// sync's weight, `None` when the pair synced before.
    #[must_use]
    pub fn new(
        tables: BTreeMap<String, Table>,
        ignored: BTreeSet<String>,
        dryrun: bool,
        first_sync: Option<Weight>,
    ) -> Self {
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
        Self {
            status,
            first_sync: first_sync.is_some(),
            deletes_seen: first_sync.is_none(),
            weight: first_sync,
            why: None,
            tables,
            ignored,
            shape: None,
            dropped: Vec::new(),
            targets_remaining: None,
        }
    }

    /// The record of a run stopped, before any row was read, because the
    /// places' shapes differ as `mismatch` says; it ignored the tables
    /// `ignored`. `first_sync` when the pair had no memory yet; no run that
    /// reads no row sees a delete.
    #[must_use]
    pub fn shape_mismatch(mismatch: Mismatch, ignored: BTreeSet<String>, first_sync: bool) -> Self {
        Self {
            status: Status::Stopped,
            first_sync,
            deletes_seen: false,
            weight: None,
            why: None,
            tables: BTreeMap::new(),
            ignored,
            shape: Some(ShapeStop {
                why: mismatch.why(),
                mismatch,
            }),
            dropped: Vec::new(),
            targets_remaining: None,
        }
    }

    /// The record of a first sync that weighs `weight`, over `max_wire`: it
    /// wrote nothing, and `why` says how to move the data instead. It ignored
    /// the tables `ignored`.
    #[must_use]
    pub fn too_much(weight: Weight, why: String, ignored: BTreeSet<String>) -> Self {
        Self {
            status: Status::TooMuch,
            first_sync: true,
            deletes_seen: false,
            weight: Some(weight),
            why: Some(why),
            tables: BTreeMap::new(),
            ignored,
            shape: None,
            dropped: Vec::new(),
            targets_remaining: None,
        }
    }

    /// Whether the run wrote any row: a table moved, or rows read back
    /// different.
    #[must_use]
    pub fn wrote(&self) -> bool {
        self.tables.values().any(|table| match table {
            Table::Moved(_) => true,
            Table::Stopped { not_read_back, .. } => !not_read_back.is_empty(),
            Table::InStep | Table::WouldMove(_) => false,
        })
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

/// The record of the table a run was writing when a place stopped
/// answering, on `cause`: stopped, naming the rows it wrote by key. Rows are
/// idempotent, so running again finishes it.
#[must_use]
pub fn lost(rows: Vec<Vec<String>>, cause: &str) -> Table {
    Table::Stopped {
        rows: rows.into_iter().map(Key).collect(),
        not_read_back: Vec::new(),
        why: format!(
            "smugglr wrote these rows and then stopped: {cause}. The run is not done. Rows \
             are idempotent: run smugglr sync again once both places answer, and it \
             finishes what this run began."
        ),
    }
}

/// The record of rows `keys` edited after they were deleted elsewhere:
/// stopped, naming them.
#[must_use]
pub fn edit_versus_delete(keys: Vec<Vec<String>>) -> Table {
    Table::Stopped {
        rows: keys.into_iter().map(Key).collect(),
        not_read_back: Vec::new(),
        why: EDIT_VERSUS_DELETE.to_owned(),
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

/// `text` for a terminal: each control character escaped, so a table or key
/// another node sent cannot drive the terminal the record is printed to.
fn printable(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_control() {
                c.escape_default().to_string()
            } else {
                c.to_string()
            }
        })
        .collect()
}

fn keys(keys: &[Key]) -> String {
    keys.iter()
        .map(|key| {
            key.0
                .iter()
                .map(|c| printable(&shown(c)))
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
            Status::TooMuch => "too much",
        };
        writeln!(f, "smugglr sync: {status}")?;
        if self.first_sync {
            writeln!(
                f,
                "  first sync: a row on one side only is copied as new; deletes are not seen"
            )?;
        }
        // A why already gives the weight.
        if let Some(why) = &self.why {
            for line in why.lines() {
                writeln!(f, "  {line}")?;
            }
        } else if let Some(weight) = &self.weight {
            writeln!(f, "  weight: {weight}")?;
        }
        if !self.ignored.is_empty() {
            let ignored: Vec<&str> = self.ignored.iter().map(String::as_str).collect();
            writeln!(f, "  ignored: {}", ignored.join(", "))?;
        }
        if let Some(shape) = &self.shape {
            writeln!(f, "  shape: {}", shape.why)?;
        }
        for (name, table) in &self.tables {
            let name = printable(name);
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
        for dropped in &self.dropped {
            writeln!(
                f,
                "  dropped: {} {}, {}, change {}",
                printable(&dropped.table),
                keys(std::slice::from_ref(&dropped.key)),
                dropped.reason.words(),
                dropped.change
            )?;
        }
        if let Some(remaining) = &self.targets_remaining {
            let remaining: Vec<String> = remaining.iter().map(|t| printable(t)).collect();
            writeln!(f, "  targets remaining: {}", remaining.join(", "))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Key, keys, printable};

    #[test]
    fn a_key_holding_an_escape_byte_prints_escaped() {
        let key = Key(vec!["'a\u{1b}[2Jb'".to_owned()]);
        let text = keys(&[key]);
        assert!(!text.contains('\u{1b}'), "{text:?}");
        assert_eq!(text, "a\\u{1b}[2Jb");
    }

    #[test]
    fn a_table_name_holding_a_newline_prints_on_one_line() {
        assert_eq!(printable("notes\nfake: line"), "notes\\nfake: line");
    }
}
