//! Shape comparison and the shape stamp (FR-SMUGGLR-070): compare and stop
//! only. smugglr never changes a shape.
//!
//! Each table's shape is its column names, types and order. A table whose
//! shape is known only by a stamp (in a blob or an envelope) is compared by
//! stamp: the 16-byte column hash.

use crate::hex::hex;

use serde::{Serialize, Serializer};

use crate::plan::Side;
use crate::row_hash::row_hash;

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

/// A place's shape: its tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shape {
    /// The place's tables.
    pub tables: Vec<TableShape>,
}

/// A table's shape stamp: its [`column_hash`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp(pub [u8; 16]);

impl Serialize for Stamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex(&self.0))
    }
}

impl Stamp {
    /// The stamp of a table with `columns`.
    #[must_use]
    pub fn of(columns: &[Column]) -> Self {
        Self(column_hash(columns))
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
const GO_ON: &str = "smugglr stopped before reading any row and wrote nothing. smugglr \
     never changes a shape. Give both places the same shape, then run again.";

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

fn stamp_text(stamp: Stamp) -> String {
    format!("column hash {}", hex(&stamp.0))
}

impl Mismatch {
    /// The difference in words, and how to go on.
    #[must_use]
    pub fn why(&self) -> String {
        let difference = match self {
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
                    stamp_text(*stamp),
                    stamp_text(*arrived)
                )
            }
        };
        format!("{difference} {GO_ON}")
    }
}

/// The first difference between the shapes of `source` and `target`, or
/// `None` when they are the same.
#[must_use]
pub fn compare(source: &Shape, target: &Shape) -> Option<Mismatch> {
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

/// Compares `table` with the stamp `arrived` it is known by elsewhere (a
/// blob or an envelope); `None` when they match.
#[must_use]
pub fn compare_stamp(table: &TableShape, arrived: Stamp) -> Option<Mismatch> {
    let stamp = Stamp::of(&table.columns);
    (stamp != arrived).then(|| Mismatch::Stamps {
        table: table.name.clone(),
        columns: table.columns.clone(),
        stamp,
        arrived,
    })
}

#[cfg(test)]
mod tests {
    use super::{Column, Mismatch, Shape, TableShape, column_hash, compare};

    fn columns(names: &[&str]) -> Vec<Column> {
        names
            .iter()
            .map(|name| Column {
                name: (*name).to_owned(),
                kind: "TEXT".to_owned(),
            })
            .collect()
    }

    fn shape(names: &[&str]) -> Shape {
        Shape {
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
    fn a_column_in_one_place_only_is_a_mismatch() {
        assert!(matches!(
            compare(&shape(&["id"]), &shape(&["id", "extra"])),
            Some(Mismatch::Columns { .. })
        ));
    }
}
