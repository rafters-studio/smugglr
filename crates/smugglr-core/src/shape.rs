//! Shape comparison and the shape stamp (FR-SMUGGLR-070): compare and stop
//! only. smugglr never changes a shape; that is migratr's job.
//!
//! Where both places carry migratr's ledger, the shape is the ledger's latest
//! version and checksum, and nothing behind migratr's back is policed.
//! Otherwise each table's shape is its column names, types and order. A
//! table whose shape is known only by a stamp (in a blob or an envelope) is
//! compared by stamp: the ledger's latest when this place has the ledger,
//! otherwise the 16-byte column hash.

use std::fmt::Write as _;

use serde::{Serialize, Serializer};

use crate::plan::Side;
use crate::row_hash::row_hash;

/// The name of migratr's ledger table.
pub const LEDGER: &str = "_migratr_migrations";

/// One column of a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Column {
    /// The column's name.
    pub name: String,
    /// Its declared type, as the place declares it; empty when it has none.
    #[serde(rename = "type")]
    pub kind: String,
}

/// One table's shape: its columns in column order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableShape {
    /// The table's name.
    pub name: String,
    /// Its columns, in column order.
    pub columns: Vec<Column>,
}

/// The latest migration in a place's migratr ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Ledger {
    /// The latest migration's version.
    pub version: i64,
    /// Its checksum.
    pub checksum: String,
}

/// A place's shape: its ledger's latest migration, when it carries migratr's
/// ledger, and its tables, the ledger's own left out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shape {
    /// The latest migration in the place's ledger; `None` without one.
    pub ledger: Option<Ledger>,
    /// The place's tables.
    pub tables: Vec<TableShape>,
}

/// A table's shape stamp: migratr's latest version and checksum when the
/// ledger is present, otherwise the column hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stamp {
    /// The latest migration in the place's ledger.
    Ledger(Ledger),
    /// The table's [`column_hash`].
    Columns([u8; 16]),
}

impl Serialize for Stamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Ledger(ledger) => ledger.serialize(serializer),
            Self::Columns(hash) => serializer.serialize_str(&hex(hash)),
        }
    }
}

fn hex(hash: &[u8]) -> String {
    hash.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

impl Stamp {
    /// The stamp of a table with `columns` in a place whose ledger's latest
    /// migration is `ledger`.
    #[must_use]
    pub fn of(ledger: Option<&Ledger>, columns: &[Column]) -> Self {
        ledger.map_or_else(
            || Self::Columns(column_hash(columns)),
            |ledger| Self::Ledger(ledger.clone()),
        )
    }
}

/// The 16-byte hash of a table's column names, types and order: the same
/// BLAKE3 hash as a row, over each column's name then type, in column order.
#[must_use]
pub fn column_hash(columns: &[Column]) -> [u8; 16] {
    let fields: Vec<&str> = columns
        .iter()
        .flat_map(|column| [column.name.as_str(), column.kind.as_str()])
        .collect();
    row_hash(&fields)
}

/// Where a column sits in one place's table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Placed {
    /// Its position in column order, from 0.
    pub position: usize,
    /// Its declared type.
    #[serde(rename = "type")]
    pub kind: String,
}

/// A column the two places hold differently: in one only, at another
/// position, or with another type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ColumnDifference {
    /// The column's name.
    pub name: String,
    /// The column in the source; `None` when the source lacks it.
    pub source: Option<Placed>,
    /// The column in the target; `None` when the target lacks it.
    pub target: Option<Placed>,
}

/// How two shapes differ.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "difference")]
pub enum Mismatch {
    /// Both places carry migratr's ledger, at different latest migrations.
    #[serde(rename = "migratr versions")]
    Versions {
        /// The source's latest migration.
        source: Ledger,
        /// The target's latest migration.
        target: Ledger,
    },
    /// The source has tables and the target none.
    #[serde(rename = "no tables")]
    NoTables,
    /// A table is in one place only.
    #[serde(rename = "table")]
    Table {
        /// The table's name.
        table: String,
        /// The place that holds it.
        only_in: Side,
    },
    /// A table's columns differ.
    #[serde(rename = "columns")]
    Columns {
        /// The table's name.
        table: String,
        /// The columns that differ.
        columns: Vec<ColumnDifference>,
    },
    /// A table's shape is known only by its stamp, and the stamps differ.
    #[serde(rename = "stamps")]
    Stamps {
        /// The table's name.
        table: String,
        /// This place's columns of the table.
        columns: Vec<Column>,
        /// This place's stamp.
        stamp: Stamp,
        /// The stamp it arrived with.
        arrived: Stamp,
    },
}

/// What every shape stop says about going on.
const MIGRATR: &str = "smugglr stopped before reading any row and wrote nothing. smugglr \
     never changes a shape: changing shapes is migratr's job. Give both places the same shape, \
     then run again.";

fn side(side: Side) -> &'static str {
    match side {
        Side::Source => "source",
        Side::Target => "target",
    }
}

fn other(only_in: Side) -> Side {
    match only_in {
        Side::Source => Side::Target,
        Side::Target => Side::Source,
    }
}

fn stamp_text(stamp: &Stamp) -> String {
    match stamp {
        Stamp::Ledger(ledger) => format!(
            "migratr version {} (checksum {})",
            ledger.version, ledger.checksum
        ),
        Stamp::Columns(hash) => format!("column hash {}", hex(hash)),
    }
}

impl Mismatch {
    /// The difference in words, and how to go on.
    #[must_use]
    pub fn why(&self) -> String {
        let difference = match self {
            Self::Versions { source, target } => format!(
                "The source's migratr ledger is at version {} and the target's at version {}.",
                source.version, target.version
            ),
            Self::NoTables => "The target has no tables.".to_owned(),
            Self::Table { table, only_in } => format!(
                "The table {table} is in the {} but not in the {}.",
                side(*only_in),
                side(other(*only_in))
            ),
            Self::Columns { table, columns } => {
                let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                format!(
                    "The table {table} has different columns in the two places: {}.",
                    names.join(", ")
                )
            }
            Self::Stamps {
                table,
                columns,
                stamp,
                arrived,
            } => {
                let columns: Vec<String> = columns
                    .iter()
                    .map(|c| format!("{} {}", c.name, c.kind).trim_end().to_owned())
                    .collect();
                format!(
                    "The table {table} here ({}) has the shape stamp {}; it arrived with {}.",
                    columns.join(", "),
                    stamp_text(stamp),
                    stamp_text(arrived)
                )
            }
        };
        format!("{difference} {MIGRATR}")
    }
}

/// The first difference between the shapes of `source` and `target`, or
/// `None` when they are the same.
#[must_use]
pub fn compare(source: &Shape, target: &Shape) -> Option<Mismatch> {
    let ledgers_match = match (&source.ledger, &target.ledger) {
        (Some(s), Some(t)) if s != t => {
            return Some(Mismatch::Versions {
                source: s.clone(),
                target: t.clone(),
            });
        }
        (Some(_), Some(_)) => true,
        _ => false,
    };
    if target.tables.is_empty() && !source.tables.is_empty() {
        return Some(Mismatch::NoTables);
    }
    for (tables, others, only_in) in [
        (&source.tables, &target.tables, Side::Source),
        (&target.tables, &source.tables, Side::Target),
    ] {
        if let Some(table) = tables
            .iter()
            .find(|table| !others.iter().any(|other| other.name == table.name))
        {
            return Some(Mismatch::Table {
                table: table.name.clone(),
                only_in,
            });
        }
    }
    if ledgers_match {
        // The same migration in both: smugglr does not police changes made
        // behind migratr's back.
        return None;
    }
    source.tables.iter().find_map(|ours| {
        let theirs = target.tables.iter().find(|t| t.name == ours.name)?;
        let columns = differences(&ours.columns, &theirs.columns);
        (!columns.is_empty()).then(|| Mismatch::Columns {
            table: ours.name.clone(),
            columns,
        })
    })
}

fn placed(columns: &[Column], name: &str) -> Option<Placed> {
    columns
        .iter()
        .position(|column| column.name == name)
        .map(|position| Placed {
            position,
            kind: columns[position].kind.clone(),
        })
}

/// Each column held differently, the source's first, in column order.
fn differences(source: &[Column], target: &[Column]) -> Vec<ColumnDifference> {
    let mut names: Vec<&str> = source.iter().map(|c| c.name.as_str()).collect();
    for column in target {
        if !names.contains(&column.name.as_str()) {
            names.push(&column.name);
        }
    }
    names
        .into_iter()
        .filter_map(|name| {
            let (s, t) = (placed(source, name), placed(target, name));
            (s != t).then(|| ColumnDifference {
                name: name.to_owned(),
                source: s,
                target: t,
            })
        })
        .collect()
}

/// Compares `table`, in a place whose ledger's latest migration is `ledger`,
/// with the stamp `arrived` it is known by elsewhere (a blob or an
/// envelope); `None` when they match.
#[must_use]
pub fn compare_stamp(
    table: &TableShape,
    ledger: Option<&Ledger>,
    arrived: &Stamp,
) -> Option<Mismatch> {
    let stamp = Stamp::of(ledger, &table.columns);
    (stamp != *arrived).then(|| Mismatch::Stamps {
        table: table.name.clone(),
        columns: table.columns.clone(),
        stamp,
        arrived: arrived.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::{Column, Ledger, Mismatch, Shape, TableShape, column_hash, compare};

    fn columns(names: &[&str]) -> Vec<Column> {
        names
            .iter()
            .map(|name| Column {
                name: (*name).to_owned(),
                kind: "TEXT".to_owned(),
            })
            .collect()
    }

    fn shape(ledger: Option<i64>, names: &[&str]) -> Shape {
        Shape {
            ledger: ledger.map(|version| Ledger {
                version,
                checksum: format!("c{version}"),
            }),
            tables: vec![TableShape {
                name: "orders".to_owned(),
                columns: columns(names),
            }],
        }
    }

    #[test]
    fn column_order_changes_the_column_hash() {
        assert_ne!(
            column_hash(&columns(&["id", "item"])),
            column_hash(&columns(&["item", "id"]))
        );
    }

    #[test]
    fn the_same_ledger_is_not_policed() {
        assert_eq!(
            compare(&shape(Some(3), &["id"]), &shape(Some(3), &["id", "extra"])),
            None
        );
    }

    #[test]
    fn one_ledger_falls_back_to_columns() {
        assert!(matches!(
            compare(&shape(Some(3), &["id"]), &shape(None, &["id", "extra"])),
            Some(Mismatch::Columns { .. })
        ));
    }
}
