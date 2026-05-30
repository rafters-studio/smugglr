//! Shared pure helpers for the wasm DataSource adapters.
//!
//! Both `FetchDataSource` (HTTP SQL) and `LocalSqlDataSource` (JS executor)
//! need the same SQL-string building, row reshaping, and content hashing.
//! These functions are self-free (no adapter state) so they live here once
//! and are called from both adapters as `adapter_common::<fn>`.

use sha2::{Digest, Sha256};
use smugglr_core::config::column_excluded;
use smugglr_core::datasource::RowMeta;
use std::collections::HashMap;

use serde_json::Value;

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

/// Build a SQLite expression that casts primary key columns to TEXT.
///
/// For single-column PKs this is `CAST("col" AS TEXT)`. For composite PKs
/// the parts are joined with `|` to produce a stable string form matching
/// the rest of smugglr's primary key encoding.
pub(crate) fn build_pk_text_expr(primary_key: &[String]) -> String {
    if primary_key.len() == 1 {
        format!("CAST(\"{}\" AS TEXT)", primary_key[0])
    } else {
        primary_key
            .iter()
            .map(|k| format!("CAST(\"{}\" AS TEXT)", k))
            .collect::<Vec<_>>()
            .join(" || '|' || ")
    }
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

/// Content hash matching smugglr-core local.rs exactly, including the
/// glob-pattern column exclusion (via `column_excluded`) -- exact-string
/// matching here would diverge from transfer-time stripping and produce
/// phantom `content_differs` for glob-excluded columns.
pub(crate) fn content_hash(
    row: &HashMap<String, Value>,
    columns_in_order: &[String],
    exclude: &[String],
    timestamp_column: &str,
) -> String {
    let timestamp_columns = ["updated_at", "created_at"];
    let mut hasher = Sha256::new();
    for col in columns_in_order {
        if timestamp_columns.contains(&col.as_str())
            || column_excluded(col, exclude)
            || col == timestamp_column
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
