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

/// What Cloudflare D1's adapter does to an HTTP-SQL statement before D1
/// runs it, as D1's quirks need (NFR-SMUGGLR-008).
pub(crate) mod d1 {
    /// The most parameters D1 binds to one statement.
    pub(crate) const MAX_PARAMS: usize = 100;

    /// What a parameter is, for D1, which binds every parameter as text:
    /// the kind the statement turns the text back into.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Kind {
        Null,
        Integer,
        Real,
        Text,
        Blob,
    }

    /// Parameter `?n` turned back from text into its kind.
    fn typed(kind: Kind, n: &str) -> String {
        match kind {
            // Always NULL, with the parameter still in the statement.
            Kind::Null => format!("NULLIF(?{n}, ?{n})"),
            Kind::Integer => format!("CAST(?{n} AS INTEGER)"),
            Kind::Real => format!("CAST(?{n} AS REAL)"),
            Kind::Text => format!("?{n}"),
            Kind::Blob => format!("unhex(?{n})"),
        }
    }

    /// `sql` with each numbered parameter `?n` turned back into its kind,
    /// `kinds[n - 1]`. Text inside a string literal or a quoted identifier
    /// is left as it is.
    pub(crate) fn typed_params(sql: &str, kinds: &[Kind]) -> String {
        let mut out = String::with_capacity(sql.len());
        let mut chars = sql.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '\'' | '"' => {
                    // A literal or identifier runs to its closing quote; a
                    // doubled quote inside it is one quote, read in turn.
                    out.push(c);
                    for inner in chars.by_ref() {
                        out.push(inner);
                        if inner == c {
                            break;
                        }
                    }
                }
                '?' => {
                    let mut n = String::new();
                    while let Some(&digit) = chars.peek().filter(|d| d.is_ascii_digit()) {
                        n.push(digit);
                        chars.next();
                    }
                    let kind = n
                        .parse::<usize>()
                        .ok()
                        .and_then(|i| kinds.get(i.checked_sub(1)?));
                    if let Some(&kind) = kind {
                        out.push_str(&typed(kind, &n));
                    } else {
                        out.push('?');
                        out.push_str(&n);
                    }
                }
                _ => out.push(c),
            }
        }
        out
    }

    /// Whether `sql` writes: D1 counts changes by connection, not by
    /// statement, so the adapter has a write return a row for each row it
    /// writes.
    pub(crate) fn writes(sql: &str) -> bool {
        let first = sql.split_whitespace().next().unwrap_or("");
        ["INSERT", "UPDATE", "DELETE", "REPLACE"]
            .iter()
            .any(|verb| first.eq_ignore_ascii_case(verb))
    }

    /// A write that returns a row for each row it writes.
    pub(crate) fn returning(sql: &str) -> String {
        format!("{} RETURNING 1", sql.trim_end().trim_end_matches(';'))
    }
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
    fn d1_turns_each_text_parameter_back_into_its_kind() {
        use super::d1::{Kind, returning, typed_params, writes};
        let table = Table {
            name: "no?1tes".to_owned(),
            columns: vec!["id".to_owned(), "n".to_owned(), "data".to_owned()],
            types: vec![String::new(); 3],
            key: vec!["id".to_owned()],
            self_keyed: false,
        };
        let kinds = [
            Kind::Integer,
            Kind::Null,
            Kind::Blob,
            Kind::Text,
            Kind::Text,
            Kind::Text,
        ];
        let update = typed_params(&update_at_quoted(&table), &kinds);
        assert_eq!(
            update,
            "UPDATE \"no?1tes\" SET \"n\" = NULLIF(?2, ?2), \"data\" = unhex(?3) \
             WHERE \"id\" IS CAST(?1 AS INTEGER) \
             AND quote(\"id\") IS ?4 AND quote(\"n\") IS ?5 AND quote(\"data\") IS ?6"
        );
        assert!(writes(&update));
        assert_eq!(
            returning(&super::insert(&table)),
            "INSERT INTO \"no?1tes\" (\"id\", \"n\", \"data\") VALUES (?1, ?2, ?3) \
             ON CONFLICT (\"id\") DO NOTHING RETURNING 1"
        );
        assert_eq!(
            typed_params("SELECT '?1', ?1, ?12", &[Kind::Real]),
            "SELECT '?1', CAST(?1 AS REAL), ?12"
        );
        assert!(!writes(super::TABLES));
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
