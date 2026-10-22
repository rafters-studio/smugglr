//! Before any row is read, the shapes of the two places are compared
//! (FR-SMUGGLR-070); smugglr never changes a shape.

use smugglr_core::shape::{self, Column, Mismatch, Shape, TableShape};
use smugglr_place::Table;

use crate::Error;

/// A place's shape: its tables.
fn shape_of(tables: &[Table]) -> Shape {
    Shape {
        tables: tables
            .iter()
            .map(|table| TableShape {
                name: table.name.clone(),
                columns: table
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, name)| Column {
                        name: name.clone(),
                        kind: table.types.get(i).cloned().unwrap_or_default(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// How the shapes of the places at `source` and `target`, whose tables are
/// `ours` and `theirs`, differ; `None` when they are the same. Reads no row.
///
/// # Errors
/// When a table has no primary key.
pub(crate) fn check(
    source: &str,
    target: &str,
    ours: &[Table],
    theirs: &[Table],
) -> Result<Option<Mismatch>, Error> {
    let mismatch = shape::compare(&shape_of(ours), &shape_of(theirs));
    if mismatch.is_some() {
        return Ok(mismatch);
    }
    for (tables, here) in [(ours, source), (theirs, target)] {
        if let Some(table) = tables.iter().find(|table| table.key.is_empty()) {
            return Err(Error::Shape(format!(
                "the table {} in {here} has no primary key",
                table.name
            )));
        }
    }
    Ok(None)
}
