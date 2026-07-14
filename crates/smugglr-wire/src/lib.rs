//! Canonical wire types for the smugglr plugin protocol.
//!
//! [`TableInfo`], [`ColumnInfo`], and [`RowMeta`] are the JSON-RPC payloads
//! exchanged between the smugglr host and adapter plugins over stdin/stdout
//! (see `smugglr-core::plugin` and `smugglr-plugin-sdk::run`). Both sides used
//! to define their own copy of these structs -- the host additionally kept a
//! third, `Wire`-prefixed copy purely to `#[derive(Deserialize)]` without
//! pulling in the SDK crate (which drags in tokio + stdin/stdout at module
//! scope, breaking the host's wasm32-compiled `smugglr-core::datasource` path).
//!
//! This crate holds one definition. It depends on nothing but `serde`, so it
//! compiles on wasm32 and can sit underneath both `smugglr-core` (host side)
//! and `smugglr-plugin-sdk` (plugin side) without creating a core <-> sdk
//! dependency cycle. See issue #228.
//!
//! The serde representation (field names, attributes, order) is the wire
//! contract -- changing it is a breaking protocol change. The snapshot tests
//! below pin the exact JSON shape.

use serde::{Deserialize, Serialize};

/// Table schema information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(default)]
    pub col_type: String,
    #[serde(default)]
    pub notnull: bool,
    #[serde(default)]
    pub pk: bool,
}

/// A row's primary key, change-detection hash, and optional timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowMeta {
    pub pk_value: String,
    pub updated_at: Option<String>,
    pub content_hash: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the exact JSON shape of `TableInfo` -- field names and order are
    /// the wire contract between host and plugin. Any accidental rename,
    /// reorder, or added/removed field must fail this test.
    #[test]
    fn table_info_snapshot() {
        let info = TableInfo {
            name: "users".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    col_type: "INTEGER".to_string(),
                    notnull: true,
                    pk: true,
                },
                ColumnInfo {
                    name: "email".to_string(),
                    col_type: "TEXT".to_string(),
                    notnull: false,
                    pk: false,
                },
            ],
            primary_key: vec!["id".to_string()],
        };
        let json = serde_json::to_string(&info).unwrap();
        assert_eq!(
            json,
            r#"{"name":"users","columns":[{"name":"id","col_type":"INTEGER","notnull":true,"pk":true},{"name":"email","col_type":"TEXT","notnull":false,"pk":false}],"primary_key":["id"]}"#
        );
    }

    /// `col_type`, `notnull`, and `pk` are `#[serde(default)]` -- a plugin may
    /// omit them and still deserialize, defaulting to `""`, `false`, `false`.
    #[test]
    fn column_info_defaults_on_missing_fields() {
        let json = r#"{"name":"email"}"#;
        let col: ColumnInfo = serde_json::from_str(json).unwrap();
        assert_eq!(col.name, "email");
        assert_eq!(col.col_type, "");
        assert!(!col.notnull);
        assert!(!col.pk);
    }

    /// `RowMeta.updated_at` has no `skip_serializing_if` -- `None` must still
    /// emit an explicit `"updated_at":null`, not omit the key.
    #[test]
    fn row_meta_snapshot_with_updated_at() {
        let meta = RowMeta {
            pk_value: "42".to_string(),
            updated_at: Some("2026-04-03T12:00:00Z".to_string()),
            content_hash: "abc123".to_string(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        assert_eq!(
            json,
            r#"{"pk_value":"42","updated_at":"2026-04-03T12:00:00Z","content_hash":"abc123"}"#
        );
    }

    #[test]
    fn row_meta_snapshot_without_updated_at() {
        let meta = RowMeta {
            pk_value: "42".to_string(),
            updated_at: None,
            content_hash: "abc123".to_string(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        assert_eq!(
            json,
            r#"{"pk_value":"42","updated_at":null,"content_hash":"abc123"}"#
        );
    }
}
