//! Before any row is read, the shapes of the two places are compared
//! (FR-SMUGGLR-070); smugglr never changes a shape.

use smugglr_core::shape::{self, Column, Mismatch, Shape, TableShape};
use smugglr_place::{Place, Table};

use crate::sync::Error;

/// A place's shape: its ledger's latest migration and its tables, the
/// ledger's own among them, so a ledger in one place only is a table that
/// place alone holds.
fn shape_of(place: &mut dyn Place, tables: &[Table]) -> Result<Shape, Error> {
    Ok(Shape {
        ledger: place.ledger()?,
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
    })
}

/// How the shapes of `source` and `target`, whose tables are `ours` and
/// `theirs`, differ; `None` when they are the same. Reads no row.
///
/// # Errors
/// When a place cannot be read, or a table has no primary key (#507's).
pub(crate) fn check(
    source: &mut dyn Place,
    target: &mut dyn Place,
    ours: &[Table],
    theirs: &[Table],
) -> Result<Option<Mismatch>, Error> {
    let mismatch = shape::compare(&shape_of(source, ours)?, &shape_of(target, theirs)?);
    if mismatch.is_some() {
        return Ok(mismatch);
    }
    for (tables, here) in [(ours, source.address()), (theirs, target.address())] {
        if let Some(table) = tables.iter().find(|table| table.key.is_empty()) {
            return Err(Error::Shape(format!(
                "the table {} in {here} has no primary key",
                table.name
            )));
        }
    }
    Ok(None)
}
