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

/// Writes one row, every column a parameter in column order.
pub(crate) fn upsert(table: &Table) -> String {
    let columns: Vec<String> = table.columns.iter().map(|c| ident(c)).collect();
    let params: Vec<String> = (1..=columns.len()).map(|i| format!("?{i}")).collect();
    let key: Vec<String> = table.key.iter().map(|c| ident(c)).collect();
    let set: Vec<String> = table
        .columns
        .iter()
        .filter(|column| !table.key.contains(column))
        .map(|column| format!("{0} = excluded.{0}", ident(column)))
        .collect();
    let action = if set.is_empty() {
        "NOTHING".to_owned()
    } else {
        format!("UPDATE SET {}", set.join(", "))
    };
    format!(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO {action}",
        ident(&table.name),
        columns.join(", "),
        params.join(", "),
        key.join(", ")
    )
}

/// Deletes one row, by key.
pub(crate) fn delete(table: &Table) -> String {
    format!(
        "DELETE FROM {} WHERE {}",
        ident(&table.name),
        by_key(&table.key, 1)
    )
}

#[cfg(test)]
mod tests {
    use super::{ident, upsert};
    use crate::Table;

    #[test]
    fn a_name_with_a_quote_stays_one_identifier() {
        assert_eq!(ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn upsert_sets_every_column_outside_the_key() {
        let table = Table {
            name: "notes".to_owned(),
            columns: vec!["id".to_owned(), "body".to_owned()],
            key: vec!["id".to_owned()],
        };
        assert_eq!(
            upsert(&table),
            "INSERT INTO \"notes\" (\"id\", \"body\") VALUES (?1, ?2) \
             ON CONFLICT (\"id\") DO UPDATE SET \"body\" = excluded.\"body\""
        );
    }
}
