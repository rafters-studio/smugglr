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

// Per-value type tags. They make the byte stream self-describing so that values
// which would otherwise share bytes hash distinctly -- in particular a NULL
// column versus an empty-string column (both were just `|` before, a real
// "different rows, same hash" collision). The tag is part of the WIRE CONTRACT.
const TAG_NULL: u8 = 0;
const TAG_NUMBER: u8 = 1; // also bool, normalized to "1"/"0"
const TAG_STRING: u8 = 2;
const TAG_OTHER: u8 = 3; // JSON array/object (only reachable on the JSON paths)

/// Canonical decimal form of a JSON number, identical for a value regardless of
/// whether the source serialized it as an integer or a float.
///
/// This is what makes the native path (SQLite `REAL` 1.0 -> serde `1.0`) agree
/// with a remote backend that drops the decimal (JSON `1`): a whole-valued float
/// is rendered as its integer form, so both sides hash `"1"`.
fn canonical_number(n: &serde_json::Number) -> String {
    if let Some(i) = n.as_i64() {
        return i.to_string();
    }
    if let Some(u) = n.as_u64() {
        return u.to_string();
    }
    if let Some(f) = n.as_f64() {
        // Integral floats within the exact-integer range collapse to integer
        // form so `1.0` and `1` converge; everything else uses the float form.
        if f.is_finite() && f.fract() == 0.0 && f.abs() < 9_007_199_254_740_992.0 {
            return (f as i64).to_string();
        }
        return f.to_string();
    }
    n.to_string()
}

/// Hash a row's content for change detection.
///
/// The hash folds every column in `columns_in_order` EXCEPT:
/// - the built-in timestamp columns (`updated_at`, `created_at`),
/// - the configured `timestamp_column` (the conflict-resolution field), and
/// - any column matching an `exclude` glob (via [`column_excluded`], so
///   patterns like `*_embedding` are honored -- exact-string matching here
///   would diverge from transfer-time column stripping).
///
/// Each retained column contributes a one-byte type tag, then the canonical
/// byte form of its value, then a `|` separator. Column order is the table's
/// definition order, shared by every call site, so the bytes are stable across
/// sources. An absent column is treated as NULL (a missing JSON field and an
/// explicit null are the same row).
///
/// BLOB wire contract: the native path renders blobs as lowercase hex (via
/// `local.rs::get_json_value`). For a remote (JSON) source to converge, its
/// endpoint MUST also transport blob columns as lowercase-hex strings; a backend
/// that returns base64 or a byte array will hash differently. Backends that
/// cannot meet this should `exclude` their blob columns. (Residual of #202.)
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
        match row.get(col) {
            None | Some(Value::Null) => hasher.update([TAG_NULL]),
            Some(Value::String(s)) => {
                hasher.update([TAG_STRING]);
                hasher.update(s.as_bytes());
            }
            Some(Value::Number(n)) => {
                hasher.update([TAG_NUMBER]);
                hasher.update(canonical_number(n).as_bytes());
            }
            Some(Value::Bool(b)) => {
                // Bool shares the NUMBER tag/encoding so a JSON `true` converges
                // with the integer `1` SQLite stores for a boolean column.
                hasher.update([TAG_NUMBER]);
                hasher.update(if *b { b"1" } else { b"0" });
            }
            Some(other) => {
                hasher.update([TAG_OTHER]);
                hasher.update(other.to_string().as_bytes());
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
        // Row {id:1, name:"ada", score:3} hashes the tagged byte stream
        // 0x01"1"| 0x02"ada"| 0x01"3"| -- pinned here as the wire contract.
        let r = row(&[
            ("id", json!(1)),
            ("name", json!("ada")),
            ("score", json!(3)),
        ]);
        let cols = ["id".to_string(), "name".to_string(), "score".to_string()];
        assert_eq!(
            content_hash(&r, &cols, &[], "updated_at"),
            "0ca55cfc21c3cbd74d62ae08bc414d39465cf950c4e73eca95c0f22238e98530"
        );
    }

    #[test]
    fn null_and_empty_string_hash_differently() {
        // The collision the type tags exist to kill: NULL vs "".
        let cols = ["id".to_string(), "v".to_string()];
        let null_v = row(&[("id", json!(1)), ("v", Value::Null)]);
        let empty_v = row(&[("id", json!(1)), ("v", json!(""))]);
        assert_ne!(
            content_hash(&null_v, &cols, &[], "updated_at"),
            content_hash(&empty_v, &cols, &[], "updated_at")
        );
    }

    #[test]
    fn integer_and_whole_float_converge() {
        // A SQLite REAL 1.0 (serde f64 1.0) and a backend's JSON `1` must hash
        // identically, or the row reads content_differs forever (#179/#197).
        let cols = ["id".to_string(), "v".to_string()];
        let as_int = row(&[("id", json!(1)), ("v", json!(2))]);
        let as_float = row(&[("id", json!(1)), ("v", json!(2.0))]);
        assert_eq!(
            content_hash(&as_int, &cols, &[], "updated_at"),
            content_hash(&as_float, &cols, &[], "updated_at")
        );
        // A non-integral float is unchanged and distinct.
        let frac = row(&[("id", json!(1)), ("v", json!(2.5))]);
        assert_ne!(
            content_hash(&as_int, &cols, &[], "updated_at"),
            content_hash(&frac, &cols, &[], "updated_at")
        );
    }

    #[test]
    fn bool_converges_with_integer() {
        // SQLite stores a boolean as 0/1; a JSON source may send `true`. They
        // must agree.
        let cols = ["id".to_string(), "flag".to_string()];
        let as_bool = row(&[("id", json!(1)), ("flag", json!(true))]);
        let as_int = row(&[("id", json!(1)), ("flag", json!(1))]);
        assert_eq!(
            content_hash(&as_bool, &cols, &[], "updated_at"),
            content_hash(&as_int, &cols, &[], "updated_at")
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
