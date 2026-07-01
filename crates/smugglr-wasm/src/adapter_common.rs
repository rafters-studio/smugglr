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

/// Build the incremental-metadata query: every row whose `timestamp_column`
/// is at or after the cursor, with a synthetic `__pk` text column.
///
/// `>=` (not `>`): a row written at exactly the cursor timestamp AFTER the
/// scan that established that cursor would never satisfy `> cursor` on a
/// later pass (same-tick / whole-second granularity), silently dropping its
/// change until clearCache(). Re-fetching the boundary tick is safe because
/// the caller merges results into the PK-keyed cache, so rows already seen at
/// the boundary are overwritten idempotently and only genuinely-new boundary
/// rows are admitted. See bug #199 -- the predicate must stay `>=`.
pub(crate) fn incremental_metadata_sql(
    table: &str,
    primary_key: &[String],
    timestamp_column: &str,
) -> String {
    format!(
        "SELECT *, {} AS __pk FROM \"{}\" WHERE \"{}\" >= ?",
        build_pk_text_expr(primary_key),
        table,
        timestamp_column
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;

    #[wasm_bindgen_test]
    fn incremental_metadata_sql_uses_inclusive_predicate() {
        // Regression for #199: the incremental cursor predicate must be `>=`,
        // not `>`, so a boundary-tick row is re-admitted. Pinning the exact
        // SQL string fails loudly if the operator ever reverts to `>`.
        let sql = incremental_metadata_sql("items", &["id".to_string()], "updated_at");
        assert_eq!(
            sql,
            "SELECT *, CAST(\"id\" AS TEXT) AS __pk FROM \"items\" WHERE \"updated_at\" >= ?"
        );
    }
}
