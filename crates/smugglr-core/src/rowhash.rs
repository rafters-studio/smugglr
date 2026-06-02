//! The one canonical home for the row content-hash and the primary-key text
//! expression.
//!
//! These two pieces are a WIRE CONTRACT. Two peers that hash a row differently
//! never converge -- the row reads as `content_differs` forever -- and nothing
//! signals the mismatch. Before this module existed the hash was reimplemented
//! three times (smugglr-core `local.rs` over rusqlite rows, the http-sql plugin
//! over JSON, and the wasm adapter over JSON) and the copies had drifted on
//! float formatting, blob encoding, exact-vs-glob column exclusion, and whether
//! a custom `timestamp_column` is hashed -- every drift a silent-corruption bug.
//!
//! This module is always compiled (no `native` deps): both the plugin and the
//! wasm adapter depend on smugglr-core with `default-features = false` and call
//! straight into here. The native path (`local.rs`) converts each rusqlite row
//! into the same `serde_json::Value` map (via its existing `get_json_value`) and
//! hashes it through the identical code, so all three paths agree by
//! construction.

use std::collections::HashMap;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::column_excluded;

/// Columns never folded into the content hash: they carry change-tracking
/// timestamps, not content. A row whose only delta is its timestamp must NOT
/// read as `content_differs` -- that is the whole reason content_hash and
/// `updated_at` are separate fields.
const TIMESTAMP_COLUMNS: [&str; 2] = ["updated_at", "created_at"];

/// Hash a row's content for change detection.
///
/// The hash folds every column in `columns_in_order` EXCEPT:
/// - the built-in timestamp columns (`updated_at`, `created_at`),
/// - the configured `timestamp_column` (the conflict-resolution field), and
/// - any column matching an `exclude` glob (via [`column_excluded`], so
///   patterns like `*_embedding` are honored -- exact-string matching here
///   would diverge from transfer-time column stripping).
///
/// For each retained column the canonical byte form of its value is appended,
/// followed by a `|` separator. A NULL or absent column contributes only the
/// separator. Column order is the table's definition order, shared by every
/// call site, so the bytes are stable across sources.
pub fn content_hash(
    row: &HashMap<String, Value>,
    columns_in_order: &[String],
    exclude: &[String],
    timestamp_column: &str,
) -> String {
    let mut hasher = Sha256::new();
    for col in columns_in_order {
        if TIMESTAMP_COLUMNS.contains(&col.as_str())
            || col == timestamp_column
            || column_excluded(col, exclude)
        {
            continue;
        }
        if let Some(val) = row.get(col) {
            match val {
                Value::Null => {}
                Value::String(s) => hasher.update(s.as_bytes()),
                Value::Number(n) => hasher.update(n.to_string().as_bytes()),
                Value::Bool(b) => hasher.update(if *b { "1" } else { "0" }.as_bytes()),
                other => hasher.update(other.to_string().as_bytes()),
            }
        }
        hasher.update(b"|");
    }
    hex::encode(hasher.finalize())
}

/// Build a SQLite expression that renders the primary key as stable text.
///
/// Single-column PKs become `CAST("col" AS TEXT)`; composite PKs join each cast
/// part with `|`. The `CAST` is load-bearing on the JSON path: without it an
/// integer PK comes back as a JSON number and the `__pk` lookup (which expects a
/// string) silently yields an empty key. The native path reads the value
/// type-agnostically, so the cast is harmless there -- making this the one form
/// that is correct everywhere.
pub fn pk_text_expr(primary_key: &[String]) -> String {
    primary_key
        .iter()
        .map(|k| format!("CAST(\"{}\" AS TEXT)", k))
        .collect::<Vec<_>>()
        .join(" || '|' || ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    // Golden vectors: fixed rows -> fixed hex. These pin the wire contract. If a
    // value changes, the hash of every affected row changes and forces a full
    // re-sync -- so a deliberate change must update these on purpose, never by
    // accident.
    #[test]
    fn golden_scalar_row() {
        // Row {id:1, name:"ada", score:3} hashes the bytes "1|ada|3|".
        // sha256("1|ada|3|") is pinned here as the wire contract.
        let r = row(&[
            ("id", json!(1)),
            ("name", json!("ada")),
            ("score", json!(3)),
        ]);
        let cols = ["id".to_string(), "name".to_string(), "score".to_string()];
        assert_eq!(
            content_hash(&r, &cols, &[], "updated_at"),
            "ce5122f076c9b5fcb09b489db573729ca774237be48ee52b4f9344c469bdfe42"
        );
    }

    #[test]
    fn timestamp_and_excludes_are_skipped() {
        let cols = [
            "id".to_string(),
            "updated_at".to_string(),
            "v".to_string(),
            "v_embedding".to_string(),
        ];
        // Changing updated_at, created_at, the configured ts col, or a
        // glob-excluded col must not change the hash.
        let base = row(&[
            ("id", json!(1)),
            ("updated_at", json!(100)),
            ("v", json!("x")),
            ("v_embedding", json!("aaa")),
        ]);
        let changed = row(&[
            ("id", json!(1)),
            ("updated_at", json!(999)),
            ("v", json!("x")),
            ("v_embedding", json!("zzz")),
        ]);
        let exclude = ["*_embedding".to_string()];
        assert_eq!(
            content_hash(&base, &cols, &exclude, "updated_at"),
            content_hash(&changed, &cols, &exclude, "updated_at")
        );
        // But changing a real content column DOES change the hash.
        let real_change = row(&[
            ("id", json!(1)),
            ("updated_at", json!(100)),
            ("v", json!("y")),
            ("v_embedding", json!("aaa")),
        ]);
        assert_ne!(
            content_hash(&base, &cols, &exclude, "updated_at"),
            content_hash(&real_change, &cols, &exclude, "updated_at")
        );
    }

    #[test]
    fn custom_timestamp_column_is_skipped() {
        let cols = ["id".to_string(), "modified".to_string(), "v".to_string()];
        let a = row(&[("id", json!(1)), ("modified", json!(1)), ("v", json!("x"))]);
        let b = row(&[("id", json!(1)), ("modified", json!(2)), ("v", json!("x"))]);
        assert_eq!(
            content_hash(&a, &cols, &[], "modified"),
            content_hash(&b, &cols, &[], "modified")
        );
    }

    #[test]
    fn null_and_absent_contribute_only_separator() {
        let cols = ["id".to_string(), "a".to_string(), "b".to_string()];
        let with_null = row(&[("id", json!(1)), ("a", Value::Null), ("b", json!("x"))]);
        let absent = row(&[("id", json!(1)), ("b", json!("x"))]); // "a" missing
        assert_eq!(
            content_hash(&with_null, &cols, &[], "updated_at"),
            content_hash(&absent, &cols, &[], "updated_at")
        );
    }

    #[test]
    fn pk_text_expr_single_and_composite() {
        assert_eq!(pk_text_expr(&["id".to_string()]), "CAST(\"id\" AS TEXT)");
        assert_eq!(
            pk_text_expr(&["a".to_string(), "b".to_string()]),
            "CAST(\"a\" AS TEXT) || '|' || CAST(\"b\" AS TEXT)"
        );
    }
}
