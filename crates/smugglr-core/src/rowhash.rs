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

use base64::Engine as _;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::column_excluded;
use crate::error::SyncError;

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
/// BLOB wire contract: blobs fold as lowercase hex on EVERY path. The native
/// path emits hex directly (`local.rs::get_json_value`); a JSON backend that
/// renders base64 (or another encoding) MUST canonicalize its blob columns to
/// hex via [`canonicalize_blob_columns`] before calling this, so the same bytes
/// hash identically everywhere. A value that cannot be canonicalized should be
/// `exclude`d on both peers -- one-sided exclusion does not converge, since the
/// hex-rendering native side still folds it. (#292, residual of #202.)
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

/// How a backend renders a BLOB column's bytes into the JSON string that
/// reaches [`content_hash`].
///
/// The native rusqlite path renders lowercase hex (`local.rs::get_json_value`);
/// JSON SQL endpoints (Turso/rqlite/D1 and the wasm executors) commonly render
/// standard base64. Two peers that fold different renderings of the SAME bytes
/// never converge -- the row reads `content_differs` forever. The content hash
/// pins ONE canonical form (lowercase hex), so a non-hex backend must declare
/// its encoding and canonicalize before hashing. (#292, residual of #202.)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobEncoding {
    /// Lowercase hexadecimal -- the canonical form; the native path already
    /// emits this, so canonicalizing it only validates and normalizes case.
    Hex,
    /// Standard (padded) base64 -- what the JSON SQL backends emit.
    Base64,
}

/// True when a declared SQLite column type denotes a BLOB, so its rendered
/// string value must be canonicalized to hex before hashing.
///
/// Match is on the declared type containing `BLOB` (case-insensitive), the
/// affinity rule SQLite itself uses. An EMPTY declared type is deliberately NOT
/// treated as a blob: although SQLite gives it BLOB affinity, such columns hold
/// arbitrary dynamically-typed values, and base64-decoding a genuine text value
/// would corrupt it. Only explicitly-declared BLOB columns are canonicalized --
/// the unambiguous, common case that the divergence bug concerns.
pub fn is_blob_column(col_type: &str) -> bool {
    col_type.to_ascii_uppercase().contains("BLOB")
}

/// Re-encode a backend's rendered BLOB string to the canonical lowercase-hex
/// form the content hash folds.
///
/// Returns `None` when `value` does not decode under `encoding`. The caller must
/// then leave the value untouched and warn -- a value we cannot canonicalize is
/// never silently re-folded in its divergent encoding (that is the original bug)
/// and never silently corrupted by a mis-guessed decode.
pub fn canonical_blob_hex(value: &str, encoding: BlobEncoding) -> Option<String> {
    let bytes = match encoding {
        // Already canonical; decode+encode validates and lowercases.
        BlobEncoding::Hex => hex::decode(value).ok()?,
        BlobEncoding::Base64 => base64::engine::general_purpose::STANDARD
            .decode(value)
            .ok()?,
    };
    Some(hex::encode(bytes))
}

/// Rewrite every listed blob column in `row` from `encoding` to canonical
/// lowercase hex in place, so a JSON backend's [`content_hash`] converges with
/// the native (hex) reference.
///
/// Absent, NULL, or non-string values are left untouched (a NULL blob folds as
/// NULL on every path; a byte-array rendering is out of scope). Returns the
/// names of any columns whose string value could not be decoded under
/// `encoding`: the caller MUST warn on these -- their raw rendering would hash
/// divergently and the operator should `exclude` the column. (#292)
pub fn canonicalize_blob_columns(
    row: &mut HashMap<String, Value>,
    blob_columns: &[String],
    encoding: BlobEncoding,
) -> Vec<String> {
    let mut undecodable = Vec::new();
    for col in blob_columns {
        let raw = match row.get(col) {
            Some(Value::String(s)) => s.clone(),
            _ => continue,
        };
        match canonical_blob_hex(&raw, encoding) {
            Some(hexed) => {
                row.insert(col.clone(), Value::String(hexed));
            }
            None => undecodable.push(col.clone()),
        }
    }
    undecodable
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
    // Single-column PK: no delimiter, so no collision is possible -- render it
    // unchanged. This preserves the `__pk` wire contract for the overwhelmingly
    // common single-PK case; only composite-PK tables re-render (and re-sync once).
    if primary_key.len() == 1 {
        return format!("CAST(\"{}\" AS TEXT)", primary_key[0]);
    }
    // Composite PK: a bare `|` join is NOT injective. `{a:'x|', b:'y'}` and
    // `{a:'x', b:'|y'}` both render `x||y`, collapsing two distinct rows onto one
    // `__pk` -- silent row loss in the pk-keyed metadata map. Escape the escape
    // char first, then the delimiter, in each part so the join is unambiguous.
    primary_key
        .iter()
        .map(|k| {
            format!(
                "REPLACE(REPLACE(CAST(\"{}\" AS TEXT), '\\', '\\\\'), '|', '\\|')",
                k
            )
        })
        .collect::<Vec<_>>()
        .join(" || '|' || ")
}

/// Build the `SELECT * ... WHERE <pk> IN (?, ...)` query the adapters use to
/// fetch full rows by primary key, guarding the empty-primary-key case.
///
/// `pk_text_expr` returns "" for an empty primary key, which would splice into
/// `WHERE  IN (?, ?)` -- malformed SQL that fails with an opaque remote/executor
/// error. `get_row_metadata` guards this; `get_rows` did not, across all three
/// adapters (native http-sql, wasm fetch, wasm local). This is the one guarded
/// builder they now share, so the guard cannot drift or be forgotten (#198).
pub fn pk_in_query(
    table: &str,
    primary_key: &[String],
    pk_count: usize,
) -> Result<String, SyncError> {
    if primary_key.is_empty() {
        return Err(SyncError::Config(format!(
            "no primary key for table: {table}"
        )));
    }
    let placeholders = vec!["?"; pk_count].join(", ");
    Ok(format!(
        "SELECT * FROM \"{}\" WHERE {} IN ({})",
        table,
        pk_text_expr(primary_key),
        placeholders
    ))
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
    fn blob_converges_across_hex_and_base64_backends() {
        // Spike S (#292): the blob "He" is rendered as lowercase hex "4865" by the
        // native rusqlite path and as standard base64 "SGU=" by a JSON backend.
        // Folding the raw renderings diverges, so the row reads content_differs
        // forever and the two nodes NEVER converge. Canonicalizing the JSON
        // backend's blob column to hex before hashing makes them agree.
        let cols = ["id".to_string(), "data".to_string()];
        let native = row(&[("id", json!(1)), ("data", json!("4865"))]); // hex reference
        let json_raw = row(&[("id", json!(1)), ("data", json!("SGU="))]); // base64

        // Pre-fix: the raw renderings hash differently.
        assert_ne!(
            content_hash(&native, &cols, &[], "updated_at"),
            content_hash(&json_raw, &cols, &[], "updated_at"),
            "raw hex vs base64 renderings must diverge -- this is the bug"
        );

        // Fix: canonicalize the JSON backend's blob column, then hash.
        let mut json_canon = json_raw.clone();
        let undecodable =
            canonicalize_blob_columns(&mut json_canon, &["data".to_string()], BlobEncoding::Base64);
        assert!(undecodable.is_empty(), "SGU= must decode as base64");
        assert_eq!(
            json_canon.get("data"),
            Some(&json!("4865")),
            "base64 SGU= must canonicalize to hex 4865"
        );
        assert_eq!(
            content_hash(&native, &cols, &[], "updated_at"),
            content_hash(&json_canon, &cols, &[], "updated_at"),
            "after canonicalization the blob column must converge across backends"
        );
    }

    #[test]
    fn canonical_blob_hex_round_trips_and_lowercases() {
        // Base64 decodes to the same bytes hex encodes; hex input is normalized
        // to lowercase so a backend rendering uppercase hex still converges.
        assert_eq!(
            canonical_blob_hex("SGU=", BlobEncoding::Base64).as_deref(),
            Some("4865")
        );
        assert_eq!(
            canonical_blob_hex("4865", BlobEncoding::Hex).as_deref(),
            Some("4865")
        );
        // The ambiguity that forces a caller-supplied encoding: "4865" is valid
        // base64 too, decoding to DIFFERENT bytes than as hex -- so a blind decode
        // without knowing the source encoding would corrupt the value.
        assert_ne!(
            canonical_blob_hex("4865", BlobEncoding::Base64).as_deref(),
            Some("4865")
        );
        assert_eq!(
            canonical_blob_hex("DEADBEEF", BlobEncoding::Hex).as_deref(),
            Some("deadbeef")
        );
    }

    #[test]
    fn canonical_blob_hex_reports_undecodable() {
        // A value that is not valid under the declared encoding is rejected (None)
        // rather than silently folded in its divergent form.
        assert_eq!(
            canonical_blob_hex("!!!not-base64!!!", BlobEncoding::Base64),
            None
        );
        assert_eq!(canonical_blob_hex("xyz", BlobEncoding::Hex), None);
    }

    #[test]
    fn canonicalize_blob_columns_skips_null_absent_and_flags_bad() {
        // NULL / absent / non-string blob values are left untouched; an
        // undecodable string is reported for the caller to warn on.
        let mut r = row(&[
            ("id", json!(1)),
            ("good", json!("SGU=")),
            ("null_blob", Value::Null),
            ("bad", json!("%%%")),
        ]);
        let cols = [
            "good".to_string(),
            "null_blob".to_string(),
            "absent".to_string(),
            "bad".to_string(),
        ];
        let undecodable = canonicalize_blob_columns(&mut r, &cols, BlobEncoding::Base64);
        assert_eq!(r.get("good"), Some(&json!("4865")));
        assert_eq!(r.get("null_blob"), Some(&Value::Null));
        assert_eq!(undecodable, vec!["bad".to_string()]);
    }

    #[test]
    fn is_blob_column_matches_declared_blob_only() {
        assert!(is_blob_column("BLOB"));
        assert!(is_blob_column("blob"));
        assert!(is_blob_column("MEDIUMBLOB"));
        assert!(!is_blob_column("TEXT"));
        assert!(!is_blob_column("INTEGER"));
        // Empty declared type has BLOB affinity in SQLite but is deliberately NOT
        // canonicalized -- it holds dynamically-typed values a base64-decode could
        // corrupt.
        assert!(!is_blob_column(""));
    }

    #[test]
    fn pk_text_expr_single_and_composite() {
        // Single PK renders unchanged (no delimiter, no collision possible).
        assert_eq!(pk_text_expr(&["id".to_string()]), "CAST(\"id\" AS TEXT)");
        // Composite PK escapes '\' then '|' in each part before the '|' join.
        assert_eq!(
            pk_text_expr(&["a".to_string(), "b".to_string()]),
            "REPLACE(REPLACE(CAST(\"a\" AS TEXT), '\\', '\\\\'), '|', '\\|') \
             || '|' || \
             REPLACE(REPLACE(CAST(\"b\" AS TEXT), '\\', '\\\\'), '|', '\\|')"
        );
    }

    #[test]
    fn pk_text_expr_composite_is_injective() {
        // Regression for the delimiter-collision bug: `{a:'x|', b:'y'}` and
        // `{a:'x', b:'|y'}` both rendered `x||y` under a bare `|` join, collapsing
        // two rows onto one `__pk`. This helper mirrors the SQL escaping (REPLACE
        // '\' -> '\\' then '|' -> '\|', join with '|') so we can assert the two
        // distinct composite keys now render distinctly without a database.
        fn render(parts: &[&str]) -> String {
            parts
                .iter()
                .map(|p| p.replace('\\', "\\\\").replace('|', "\\|"))
                .collect::<Vec<_>>()
                .join("|")
        }
        assert_ne!(render(&["x|", "y"]), render(&["x", "|y"]));
        assert_eq!(render(&["x|", "y"]), "x\\||y");
        assert_eq!(render(&["x", "|y"]), "x|\\|y");
    }

    #[test]
    fn pk_in_query_rejects_empty_primary_key() {
        // Regression for #198: an empty primary key renders pk_text_expr to "",
        // which splices into malformed `WHERE  IN (?, ?)`. The builder must error
        // rather than emit that SQL (which get_rows previously sent verbatim).
        assert!(
            pk_in_query("items", &[], 2).is_err(),
            "empty primary key must be rejected, not spliced into malformed SQL"
        );
    }

    #[test]
    fn pk_in_query_builds_select_for_valid_pk() {
        let sql = pk_in_query("notes", &["id".to_string()], 2).unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM \"notes\" WHERE CAST(\"id\" AS TEXT) IN (?, ?)"
        );
    }
}
