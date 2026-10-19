//! The SQL the local file runs. Names pass through [`ident`]; values are
//! always parameters.

use crate::Table;

/// The name of the row-hash function registered on each connection.
pub(crate) const ROW_HASH: &str = "smugglr_row_hash";

/// Every table except SQLite's own.
pub(crate) const TABLES: &str = "SELECT name FROM sqlite_schema \
     WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name";

/// A table's columns and where each sits in the primary key (0 when not).
pub(crate) const COLUMNS: &str = "SELECT name, pk FROM pragma_table_info(?1) ORDER BY cid";

/// A name as a quoted SQL identifier.
pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quoted(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| format!("quote({})", ident(column)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `k1 IS ?1 AND k2 IS ?2 ...`, numbering from `first`.
fn by_key(key: &[String], first: usize) -> String {
    key.iter()
        .enumerate()
        .map(|(i, column)| format!("{} IS ?{}", ident(column), first + i))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Each row's key columns' `quote()` text, then its row hash.
pub(crate) fn hashes(table: &Table) -> String {
    format!(
        "SELECT {}, {ROW_HASH}({}) FROM {}",
        quoted(&table.key),
        quoted(&table.columns),
        ident(&table.name)
    )
}

/// One row's columns' `quote()` text, by key.
pub(crate) fn row(table: &Table) -> String {
    format!(
        "SELECT {} FROM {} WHERE {}",
        quoted(&table.columns),
        ident(&table.name),
        by_key(&table.key, 1)
    )
}

/// Inserts one row only where its key holds none (a change whose `from` is
/// no row); every column a parameter in column order.
pub(crate) fn insert(table: &Table) -> String {
    let columns: Vec<String> = table.columns.iter().map(|c| ident(c)).collect();
    let params: Vec<String> = (1..=columns.len()).map(|i| format!("?{i}")).collect();
    format!(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING",
        ident(&table.name),
        columns.join(", "),
        params.join(", ")
    )
}

/// The row's hash is the change's `from`: parameter `?{n}`.
fn at_from(table: &Table, n: usize) -> String {
    format!("{ROW_HASH}({}) = ?{n}", quoted(&table.columns))
}

/// Updates one row only where it is still at the change's `from`: every
/// column a parameter in column order, then `from`.
pub(crate) fn update(table: &Table) -> String {
    let position =
        |column: &String| table.columns.iter().position(|c| c == column).unwrap_or(0) + 1;
    let mut set: Vec<String> = table
        .columns
        .iter()
        .filter(|column| !table.key.contains(column))
        .map(|column| format!("{} = ?{}", ident(column), position(column)))
        .collect();
    if set.is_empty() {
        // Every column is in the key; the row's values are its key.
        set = table
            .key
            .iter()
            .map(|c| format!("{0} = {0}", ident(c)))
            .collect();
    }
    let key: Vec<String> = table
        .key
        .iter()
        .map(|column| format!("{} IS ?{}", ident(column), position(column)))
        .collect();
    format!(
        "UPDATE {} SET {} WHERE {} AND {}",
        ident(&table.name),
        set.join(", "),
        key.join(" AND "),
        at_from(table, table.columns.len() + 1)
    )
}

/// Deletes one row only where it is still at the change's `from`: by key,
/// then `from`.
pub(crate) fn delete(table: &Table) -> String {
    format!(
        "DELETE FROM {} WHERE {} AND {}",
        ident(&table.name),
        by_key(&table.key, 1),
        at_from(table, table.key.len() + 1)
    )
}

#[cfg(test)]
mod tests {
    use super::{ident, update};
    use crate::Table;

    #[test]
    fn a_name_with_a_quote_stays_one_identifier() {
        assert_eq!(ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn update_sets_every_column_outside_the_key_only_at_from() {
        let table = Table {
            name: "notes".to_owned(),
            columns: vec!["id".to_owned(), "body".to_owned()],
            key: vec!["id".to_owned()],
        };
        assert_eq!(
            update(&table),
            "UPDATE \"notes\" SET \"body\" = ?2 WHERE \"id\" IS ?1 \
             AND smugglr_row_hash(quote(\"id\"), quote(\"body\")) = ?3"
        );
    }
}
