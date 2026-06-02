//! Shared pure helpers for the wasm DataSource adapters.
//!
//! Both `FetchDataSource` (HTTP SQL) and `LocalSqlDataSource` (JS executor)
//! need the same SQL-string building, row reshaping, and content hashing.
//! These functions are self-free (no adapter state) so they live here once
//! and are called from both adapters as `adapter_common::<fn>`.

use smugglr_core::datasource::{ColumnInfo, RowMeta, TableInfo};
use std::collections::HashMap;

use serde_json::Value;

// Row content hash and primary-key text expression -- the one canonical
// definition lives in smugglr-core::rowhash so the native, plugin, and wasm
// paths cannot drift. Re-exported under the names this crate already uses.
pub(crate) use smugglr_core::rowhash::content_hash;
pub(crate) use smugglr_core::rowhash::pk_text_expr as build_pk_text_expr;

/// Parse the rows of a `PRAGMA table_info(...)` result into a [`TableInfo`].
///
/// Shared by both adapters, which differ only in how they obtain the
/// `(columns, rows)` pair: `LocalSqlDataSource` via `run()`, `FetchDataSource`
/// via `execute()` + `extract_columns`/`extract_rows`.
pub(crate) fn parse_table_info(table: &str, columns: &[String], rows: &[Vec<Value>]) -> TableInfo {
    let name_idx = columns.iter().position(|c| c == "name").unwrap_or(1);
    let type_idx = columns.iter().position(|c| c == "type").unwrap_or(2);
    let notnull_idx = columns.iter().position(|c| c == "notnull").unwrap_or(3);
    let pk_idx = columns.iter().position(|c| c == "pk").unwrap_or(5);

    let mut col_infos = Vec::new();
    let mut primary_key = Vec::new();

    for row in rows {
        let name = row
            .get(name_idx)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let col_type = row
            .get(type_idx)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let notnull = row.get(notnull_idx).and_then(|v| v.as_i64()).unwrap_or(0) != 0;
        let pk = row.get(pk_idx).and_then(|v| v.as_i64()).unwrap_or(0) != 0;

        if pk {
            primary_key.push(name.clone());
        }

        col_infos.push(ColumnInfo {
            name,
            col_type,
            notnull,
            pk,
        });
    }

    TableInfo {
        name: table.to_string(),
        columns: col_infos,
        primary_key,
    }
}

/// Reshape positional result rows into per-row column->value maps.
pub(crate) fn rows_to_maps(columns: &[String], rows: &[Vec<Value>]) -> Vec<HashMap<String, Value>> {
    rows.iter()
        .map(|row| {
            columns
                .iter()
                .zip(row.iter())
                .map(|(col, val)| (col.clone(), val.clone()))
                .collect()
        })
        .collect()
}

/// Convert result rows (each with a synthetic `__pk` column) into RowMeta
/// entries keyed by primary key. Used by both full-scan and incremental
/// metadata fetches.
pub(crate) fn row_maps_to_metadata(
    maps: &[HashMap<String, Value>],
    column_order: &[String],
    timestamp_column: &str,
    exclude_columns: &[String],
) -> HashMap<String, RowMeta> {
    let mut result = HashMap::with_capacity(maps.len());
    for row in maps {
        let pk = row
            .get("__pk")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let updated_at = row
            .get(timestamp_column)
            .and_then(|v| v.as_str())
            .map(String::from);
        let content_hash = content_hash(row, column_order, exclude_columns, timestamp_column);

        result.insert(
            pk.clone(),
            RowMeta {
                pk_value: pk,
                updated_at,
                content_hash,
            },
        );
    }
    result
}

/// Build an `INSERT OR REPLACE` batch statement plus its flattened params.
pub(crate) fn generate_batch_sql(
    table: &str,
    columns: &[String],
    rows: &[HashMap<String, Value>],
) -> (String, Vec<Value>) {
    let col_list = columns
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    let row_placeholder = format!("({})", vec!["?"; columns.len()].join(", "));
    let all_placeholders = vec![row_placeholder.as_str(); rows.len()].join(", ");

    let sql = format!(
        "INSERT OR REPLACE INTO \"{}\" ({}) VALUES {}",
        table, col_list, all_placeholders
    );

    let params: Vec<Value> = rows
        .iter()
        .flat_map(|row| {
            columns
                .iter()
                .map(|c| row.get(c).cloned().unwrap_or(Value::Null))
        })
        .collect();

    (sql, params)
}
