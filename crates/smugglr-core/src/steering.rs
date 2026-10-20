//! The steering column `--sync-mode last` reads (FR-SMUGGLR-068).
//!
//! A steering column is one the person already has, whose values move
//! forward on every change, named in the config once for all tables or per
//! table. smugglr never adds one or asks for one, and only `--sync-mode
//! last` reads it: of a row changed in both places it keeps the copy whose
//! steering value is further on, in SQLite's own order.

use std::cmp::Ordering;
use std::collections::BTreeMap;

/// The steering columns the config names.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Steering {
    /// The column for every table (`steering = "updated_at"`).
    pub all: Option<String>,
    /// The column for a table, by table name (`[tables.orders] steering =
    /// "modified"`); it takes the place of `all` for that table.
    pub tables: BTreeMap<String, String>,
}

impl Steering {
    /// The steering column the config names for `table`, if any.
    #[must_use]
    pub fn column(&self, table: &str) -> Option<&str> {
        self.tables
            .get(table)
            .or(self.all.as_ref())
            .map(String::as_str)
    }

    /// The names of `tables` (each a name and its columns) that have no
    /// steering column: none is named for it, or the one named is not one
    /// of its columns. In the order given.
    #[must_use]
    pub fn lacking<'a>(
        &self,
        tables: impl IntoIterator<Item = (&'a str, &'a [String])>,
    ) -> Vec<String> {
        tables
            .into_iter()
            .filter(|(name, columns)| {
                self.column(name)
                    .is_none_or(|column| !columns.iter().any(|c| c == column))
            })
            .map(|(name, _)| name.to_owned())
            .collect()
    }
}

/// The one error for `--sync-mode last` with tables that have no steering
/// column, naming every such table.
#[must_use]
pub fn needs_steering(tables: &[String]) -> String {
    format!(
        "--sync-mode last needs a steering column for: {}. Name one in config, or use \
         --sync-mode stop|source|target.",
        tables.join(", ")
    )
}

/// A value as SQLite's `quote()` text gives it, in SQLite's order of
/// storage classes: NULL, then numbers, then text, then blobs.
#[derive(Debug, PartialEq)]
enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    /// Upper-case hex of the bytes.
    Blob(String),
}

impl Value {
    fn parse(quoted: &str) -> Self {
        if quoted.eq_ignore_ascii_case("NULL") {
            return Self::Null;
        }
        if let Some(text) = quoted
            .strip_prefix('\'')
            .and_then(|text| text.strip_suffix('\''))
        {
            return Self::Text(text.replace("''", "'"));
        }
        if let Some(hex) = quoted
            .strip_prefix(['X', 'x'])
            .and_then(|hex| hex.strip_prefix('\''))
            .and_then(|hex| hex.strip_suffix('\''))
        {
            return Self::Blob(hex.to_ascii_uppercase());
        }
        if let Ok(integer) = quoted.parse::<i64>() {
            return Self::Integer(integer);
        }
        match quoted.parse::<f64>() {
            Ok(real) => Self::Real(real),
            // Not quote() text: compared as text, so it still orders.
            Err(_) => Self::Text(quoted.to_owned()),
        }
    }

    fn class(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Integer(_) | Self::Real(_) => 1,
            Self::Text(_) => 2,
            Self::Blob(_) => 3,
        }
    }
}

/// How the steering value `a` stands to `b`, both SQLite `quote()` text, in
/// SQLite's order: `Greater` when `a` is further on. Numbers compare by
/// value, text by its bytes (SQLite's BINARY collation), blobs by theirs.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn further_on(a: &str, b: &str) -> Ordering {
    let (a, b) = (Value::parse(a), Value::parse(b));
    match (&a, &b) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Integer(x), Value::Real(y)) => (*x as f64).total_cmp(y),
        (Value::Real(x), Value::Integer(y)) => x.total_cmp(&(*y as f64)),
        (Value::Real(x), Value::Real(y)) => x.total_cmp(y),
        (Value::Text(x), Value::Text(y)) | (Value::Blob(x), Value::Blob(y)) => {
            x.as_bytes().cmp(y.as_bytes())
        }
        _ => a.class().cmp(&b.class()),
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::{Steering, further_on, needs_steering};

    #[test]
    fn numbers_compare_by_value_not_by_text() {
        assert_eq!(further_on("10", "9"), Ordering::Greater);
        assert_eq!(further_on("-3", "2"), Ordering::Less);
        assert_eq!(further_on("2.5", "2"), Ordering::Greater);
        assert_eq!(further_on("7", "7.0"), Ordering::Equal);
    }

    #[test]
    fn text_compares_by_its_bytes() {
        assert_eq!(
            further_on("'2026-10-01T11:00:00Z'", "'2026-10-01T10:00:00Z'"),
            Ordering::Greater
        );
        assert_eq!(further_on("'it''s'", "'it''s'"), Ordering::Equal);
    }

    #[test]
    fn storage_classes_order_as_sqlite_orders_them() {
        assert_eq!(further_on("NULL", "0"), Ordering::Less);
        assert_eq!(further_on("99", "'1'"), Ordering::Less);
        assert_eq!(further_on("X'00'", "'z'"), Ordering::Greater);
        assert_eq!(further_on("X'0A'", "X'0a'"), Ordering::Equal);
    }

    #[test]
    fn a_table_setting_takes_the_place_of_the_one_for_all() {
        let steering = Steering {
            all: Some("updated_at".to_owned()),
            tables: [("orders".to_owned(), "modified".to_owned())].into(),
        };
        assert_eq!(steering.column("orders"), Some("modified"));
        assert_eq!(steering.column("notes"), Some("updated_at"));
        assert_eq!(Steering::default().column("notes"), None);
    }

    #[test]
    fn a_table_lacks_one_when_none_is_named_or_the_named_column_is_not_its() {
        let steering = Steering {
            all: None,
            tables: [
                ("notes".to_owned(), "updated_at".to_owned()),
                ("tags".to_owned(), "updated_at".to_owned()),
            ]
            .into(),
        };
        let with = vec!["id".to_owned(), "updated_at".to_owned()];
        let without = vec!["id".to_owned()];
        let lacking = steering.lacking([
            ("orders", without.as_slice()),
            ("notes", with.as_slice()),
            ("tags", without.as_slice()),
        ]);
        assert_eq!(lacking, ["orders", "tags"]);
        assert_eq!(
            needs_steering(&["orders".to_owned(), "line_items".to_owned()]),
            "--sync-mode last needs a steering column for: orders, line_items. Name one in \
             config, or use --sync-mode stop|source|target."
        );
    }
}
