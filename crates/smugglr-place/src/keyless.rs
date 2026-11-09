//! A table without a primary key (FR-SMUGGLR-086). Each row is identified by
//! the SHA-256 of all its values, so a row whose values change is a delete
//! of the old row and an insert of the new, and identical rows are counted:
//! a row's key is its identity and which copy of those values it is, from 1.
//! Such a table always has a rowid, which deletes one copy.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};
use smugglr_core::change::{Change, Op};

use crate::queries::{ident, quoted_within};
use crate::{Key, Table};

/// A row's identity: the SHA-256 of its columns' `quote()` text, each
/// prefixed with its byte length as the row hash frames them, as quoted hex
/// text.
fn identity(row: &[String]) -> String {
    let mut hasher = Sha256::new();
    for column in row {
        hasher.update((column.len() as u64).to_be_bytes());
        hasher.update(column.as_bytes());
    }
    format!("'{:x}'", hasher.finalize())
}

/// The copies of each identity counted so far, which key each next row.
#[derive(Debug, Default)]
pub(crate) struct Copies(BTreeMap<String, u64>);

impl Copies {
    /// The key of `row`: its identity and its copy among the identical rows
    /// keyed before it.
    pub(crate) fn key(&mut self, row: &[String]) -> Key {
        let id = identity(row);
        let copy = self.0.entry(id.clone()).or_default();
        *copy += 1;
        vec![id, copy.to_string()]
    }
}

/// Each row with its key, as [`Copies::key`] gives it.
pub(crate) fn keyed(rows: Vec<Vec<String>>) -> Vec<(Key, Vec<String>)> {
    let mut copies = Copies::default();
    rows.into_iter()
        .map(|row| (copies.key(&row), row))
        .collect()
}

/// The identities `keys` name.
pub(crate) fn identities(keys: &[Key]) -> BTreeSet<&String> {
    keys.iter().filter_map(|key| key.first()).collect()
}

/// Every copy, in `all`, of the values each of `keys` names.
pub(crate) fn wanted(all: Vec<Vec<String>>, keys: &[Key]) -> Vec<Vec<String>> {
    let ids = identities(keys);
    all.into_iter()
        .filter(|row| ids.contains(&identity(row)))
        .collect()
}

/// Which copy a key names; 0 for a key that is not a keyless key.
pub(crate) fn copy(key: &Key) -> i64 {
    key.get(1).and_then(|n| n.parse().ok()).unwrap_or(0)
}

/// The changes a keyless table can take, in the order they are written:
/// deletes from the last copy down, then inserts from the first copy up, so
/// each copy's condition sees the copies before it.
pub(crate) fn ordered(changes: &[Change]) -> Vec<&Change> {
    let mut downs: Vec<&Change> = changes
        .iter()
        .filter(|change| change.op == Op::Down && change.from.is_some())
        .collect();
    downs.sort_by_key(|change| std::cmp::Reverse(copy(&change.key)));
    // A key is its values, so a row at the same key never changes: only an
    // insert, never an update.
    let mut ups: Vec<&Change> = changes
        .iter()
        .filter(|change| change.op == Op::Up || (change.op == Op::Upsert && change.from.is_none()))
        .collect();
    ups.sort_by_key(|change| copy(&change.key));
    downs.extend(ups);
    downs
}

/// The values each identity in `all` holds, as `quote()` text.
pub(crate) fn by_identity(all: Vec<Vec<String>>) -> BTreeMap<String, Vec<String>> {
    all.into_iter().map(|row| (identity(&row), row)).collect()
}

/// Every row's columns' `quote()` text, as [`quoted_within`] gives it.
pub(crate) fn all(table: &Table, limit: u64) -> String {
    format!(
        "SELECT {} FROM {}",
        quoted_within(table, limit),
        ident(&table.name)
    )
}

/// Each column's `quote()` text is parameter `?{n}` onward, in column order.
fn matching(table: &Table, n: usize) -> String {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(i, column)| format!("quote({}) IS ?{}", ident(column), n + i))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// How many rows hold the values whose `quote()` text is parameter `?{n}`
/// onward.
fn count(table: &Table, n: usize) -> String {
    format!(
        "(SELECT count(*) FROM {} WHERE {})",
        ident(&table.name),
        matching(table, n)
    )
}

/// Inserts one copy only where fewer copies than it are held: every column
/// a parameter in column order, then each column's `quote()` text, then the
/// copy. A plain insert, which a full-text table takes.
pub(crate) fn insert(table: &Table) -> String {
    let width = table.columns.len();
    let columns: Vec<String> = table.columns.iter().map(|c| ident(c)).collect();
    let params: Vec<String> = (1..=width).map(|i| format!("?{i}")).collect();
    format!(
        "INSERT INTO {} ({}) SELECT {} WHERE {} < ?{}",
        ident(&table.name),
        columns.join(", "),
        params.join(", "),
        count(table, width + 1),
        2 * width + 1
    )
}

/// Deletes one copy only where at least as many copies as it are held: each
/// column's `quote()` text, then the copy.
pub(crate) fn delete(table: &Table) -> String {
    let width = table.columns.len();
    format!(
        "DELETE FROM {0} WHERE rowid = (SELECT rowid FROM {0} WHERE {1} LIMIT 1) AND {2} >= ?{3}",
        ident(&table.name),
        matching(table, 1),
        count(table, 1),
        width + 1
    )
}

#[cfg(test)]
mod tests {
    use super::{copy, delete, insert, keyed};
    use crate::Table;

    #[test]
    fn identical_rows_are_each_their_own_copy() {
        let row = vec!["'a'".to_owned(), "1".to_owned()];
        let other = vec!["'b'".to_owned(), "1".to_owned()];
        let keys: Vec<_> = keyed(vec![row.clone(), other, row])
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys[0][0], keys[2][0]);
        assert_ne!(keys[0][0], keys[1][0]);
        assert_eq!(
            keys.iter().map(copy).collect::<Vec<_>>(),
            [1, 1, 2],
            "{keys:?}"
        );
    }

    #[test]
    fn a_copy_is_written_on_its_count() {
        let table = Table {
            name: "notes".to_owned(),
            columns: vec!["a".to_owned(), "b".to_owned()],
            types: vec![String::new(); 2],
            key: Vec::new(),
            self_keyed: false,
            autoincrement: false,
        };
        assert_eq!(
            insert(&table),
            "INSERT INTO \"notes\" (\"a\", \"b\") SELECT ?1, ?2 WHERE (SELECT count(*) FROM \
             \"notes\" WHERE quote(\"a\") IS ?3 AND quote(\"b\") IS ?4) < ?5"
        );
        assert_eq!(
            delete(&table),
            "DELETE FROM \"notes\" WHERE rowid = (SELECT rowid FROM \"notes\" WHERE \
             quote(\"a\") IS ?1 AND quote(\"b\") IS ?2 LIMIT 1) AND (SELECT count(*) FROM \
             \"notes\" WHERE quote(\"a\") IS ?1 AND quote(\"b\") IS ?2) >= ?3"
        );
    }
}
