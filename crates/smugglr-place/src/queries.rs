//! The SQL the places run. Names pass through [`ident`]; values are always
//! parameters.

use crate::Table;

/// The name of the row-hash function registered on each connection.
pub(crate) const ROW_HASH: &str = "smugglr_row_hash";

/// Every table except SQLite's own.
pub(crate) const TABLES: &str = "SELECT name FROM sqlite_schema \
     WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name";

/// A table's columns, where each sits in the primary key (0 when not), and
/// each one's declared type.
pub(crate) const COLUMNS: &str = "SELECT name, pk, type FROM pragma_table_info(?1) ORDER BY cid";

/// Whether a table's primary key has an index of its own: true for a
/// `WITHOUT ROWID` table and for any key that is not the rowid.
pub(crate) const KEY_INDEXED: &str =
    "SELECT EXISTS (SELECT 1 FROM pragma_index_list(?1) WHERE origin = 'pk')";

/// Whether the database hands out a table's keys, from [`COLUMNS`]' declared
/// type of each key column (in key order) and [`KEY_INDEXED`]: SQLite hands
/// out the key of a rowid table whose key is one column declared exactly
/// INTEGER, since that column is the rowid. `key_indexed` runs
/// [`KEY_INDEXED`], and only for such a key.
pub(crate) fn self_keyed<E>(
    declared: &[&str],
    key_indexed: impl FnOnce() -> Result<bool, E>,
) -> Result<bool, E> {
    match declared {
        [declared] if declared.eq_ignore_ascii_case("INTEGER") => Ok(!key_indexed()?),
        _ => Ok(false),
    }
}

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

/// Each row's key columns' `quote()` text, then every column's, for a place
/// that cannot run the row hash where the rows live.
pub(crate) fn quoted_rows(table: &Table) -> String {
    format!(
        "SELECT {}, {} FROM {}",
        quoted(&table.key),
        quoted(&table.columns),
        ident(&table.name)
    )
}

/// Each row's key columns' `quote()` text, then the bytes of all its
/// columns' `quote()` text.
pub(crate) fn weights(table: &Table) -> String {
    let bytes: Vec<String> = table
        .columns
        .iter()
        .map(|column| format!("length(CAST(quote({}) AS BLOB))", ident(column)))
        .collect();
    format!(
        "SELECT {}, {} FROM {}",
        quoted(&table.key),
        bytes.join(" + "),
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
/// no row); every column a parameter in column order. Only a conflict on
/// the key is passed over; any other constraint fails the write.
pub(crate) fn insert(table: &Table) -> String {
    let columns: Vec<String> = table.columns.iter().map(|c| ident(c)).collect();
    let params: Vec<String> = (1..=columns.len()).map(|i| format!("?{i}")).collect();
    let key: Vec<String> = table.key.iter().map(|c| ident(c)).collect();
    format!(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
        ident(&table.name),
        columns.join(", "),
        params.join(", "),
        key.join(", ")
    )
}

/// The row's hash is the change's `from`: parameter `?{n}`.
fn at_from(table: &Table, n: usize) -> String {
    format!("{ROW_HASH}({}) = ?{n}", quoted(&table.columns))
}

/// The row is still as it was read: each column's `quote()` text is
/// parameter `?{n}` onward, in column order.
fn at_quoted(table: &Table, n: usize) -> String {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(i, column)| format!("quote({}) IS ?{}", ident(column), n + i))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Updates one row only where it is still at the change's `from`: every
/// column a parameter in column order, then `from`.
pub(crate) fn update(table: &Table) -> String {
    update_where(table, at_from)
}

/// Updates one row only where it is still as it was read: every column a
/// parameter in column order, then each column's `quote()` text as read.
pub(crate) fn update_at_quoted(table: &Table) -> String {
    update_where(table, at_quoted)
}

fn update_where(table: &Table, at: fn(&Table, usize) -> String) -> String {
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
        at(table, table.columns.len() + 1)
    )
}

/// Deletes one row only where it is still at the change's `from`: by key,
/// then `from`.
pub(crate) fn delete(table: &Table) -> String {
    delete_where(table, at_from)
}

/// Deletes one row only where it is still as it was read: by key, then each
/// column's `quote()` text as read.
pub(crate) fn delete_at_quoted(table: &Table) -> String {
    delete_where(table, at_quoted)
}

fn delete_where(table: &Table, at: fn(&Table, usize) -> String) -> String {
    format!(
        "DELETE FROM {} WHERE {} AND {}",
        ident(&table.name),
        by_key(&table.key, 1),
        at(table, table.key.len() + 1)
    )
}

#[cfg(test)]
mod tests {
    use super::{delete_at_quoted, ident, quoted_rows, update, update_at_quoted};
    use crate::Table;

    fn notes() -> Table {
        Table {
            name: "notes".to_owned(),
            columns: vec!["id".to_owned(), "body".to_owned()],
            types: vec!["TEXT".to_owned(), "TEXT".to_owned()],
            key: vec!["id".to_owned()],
            self_keyed: false,
        }
    }

    #[test]
    fn http_sql_statements_read_and_write_by_quote_text() {
        let table = notes();
        assert_eq!(
            quoted_rows(&table),
            "SELECT quote(\"id\"), quote(\"id\"), quote(\"body\") FROM \"notes\""
        );
        assert_eq!(
            update_at_quoted(&table),
            "UPDATE \"notes\" SET \"body\" = ?2 WHERE \"id\" IS ?1 \
             AND quote(\"id\") IS ?3 AND quote(\"body\") IS ?4"
        );
        assert_eq!(
            delete_at_quoted(&table),
            "DELETE FROM \"notes\" WHERE \"id\" IS ?1 \
             AND quote(\"id\") IS ?2 AND quote(\"body\") IS ?3"
        );
    }

    #[test]
    fn a_name_with_a_quote_stays_one_identifier() {
        assert_eq!(ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn update_sets_every_column_outside_the_key_only_at_from() {
        assert_eq!(
            update(&notes()),
            "UPDATE \"notes\" SET \"body\" = ?2 WHERE \"id\" IS ?1 \
             AND smugglr_row_hash(quote(\"id\"), quote(\"body\")) = ?3"
        );
    }
}
