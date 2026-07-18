//! Shared pure helpers for the wasm DataSource adapters.
//!
//! Both `FetchDataSource` (HTTP SQL) and `LocalSqlDataSource` (JS executor)
//! need the same SQL-string building, row reshaping, and content hashing.
//! These functions are self-free (no adapter state) so they live here once
//! and are called from both adapters as `adapter_common::<fn>`.

use smugglr_core::datasource::{extract_updated_at, ColumnInfo, RowMeta, TableInfo};
use smugglr_core::error::Result;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use serde_json::Value;

// Row content hash and primary-key text expression -- the one canonical
// definition lives in smugglr-core::rowhash so the native, plugin, and wasm
// paths cannot drift. Re-exported under the names this crate already uses.
pub(crate) use smugglr_core::rowhash::content_hash;
pub(crate) use smugglr_core::rowhash::pk_text_expr as build_pk_text_expr;

// Batch-SQL generation and row reshaping -- the one canonical definition
// lives in smugglr-core::batch_sql so the http-sql plugin and wasm adapters
// cannot drift (#222). Re-exported under the names this crate already uses.
pub(crate) use smugglr_core::batch_sql::generate_batch_sql;
pub(crate) use smugglr_core::batch_sql::rows_to_maps;

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

/// Convert result rows (each with a synthetic `__pk` column) into RowMeta
/// entries keyed by primary key. Used by both full-scan and incremental
/// metadata fetches.
pub(crate) fn row_maps_to_metadata(
    maps: &[HashMap<String, Value>],
    column_order: &[String],
    timestamp_column: &str,
    exclude_columns: &[String],
    table: &str,
) -> HashMap<String, RowMeta> {
    let mut result = HashMap::with_capacity(maps.len());
    for row in maps {
        // Parity with core local.rs: a NULL rendered __pk (a NULL part in a
        // composite PK propagates through `||`) cannot key pk-based sync.
        // Coercing it to "" would collapse every such row onto one entry --
        // silently dropping rows and provoking spurious deletes. Skip and warn.
        let pk = match row.get("__pk").and_then(|v| v.as_str()) {
            Some(pk) => pk.to_string(),
            None => {
                web_sys::console::warn_1(
                    &format!("smugglr: skipping row in {} with NULL primary key", table).into(),
                );
                continue;
            }
        };
        let updated_at = extract_updated_at(row.get(timestamp_column));
        let content_hash = content_hash(row, column_order, exclude_columns, timestamp_column);

        if let Some(prev) = result.insert(
            pk.clone(),
            RowMeta {
                pk_value: pk.clone(),
                updated_at,
                content_hash,
            },
        ) {
            // Two rows rendering to the same PK text means the PK is not unique
            // as encoded -- the metadata map silently lost `prev`. Surface it.
            web_sys::console::warn_1(
                &format!(
                    "smugglr: duplicate primary key {} in {} -- a row was overwritten in change metadata (prev hash {})",
                    pk, table, prev.content_hash
                )
                .into(),
            );
        }
    }
    result
}

/// Canonicalize every declared BLOB column across `maps` from the backend's
/// base64 rendering to the lowercase hex the content hash pins, so a blob column
/// converges with the native (hex) reference instead of reading `content_differs`
/// forever (#292). BLOB columns are detected from `info` via the shared
/// `rowhash::is_blob_column`, so the wasm, plugin, and native paths cannot drift
/// on what counts as a blob. Call this on the row maps BEFORE
/// [`row_maps_to_metadata`]. A value that fails to decode is left untouched and
/// warned about -- it hashes divergently and the operator should `exclude` it.
///
/// NOTE: base64 is assumed as the wire encoding (per spike S). A backend that
/// renders blobs as hex instead would be corrupted by this decode; per-endpoint
/// encoding belongs in the profile (future work).
pub(crate) fn canonicalize_json_blobs(maps: &mut [HashMap<String, Value>], info: &TableInfo) {
    let blob_columns: Vec<String> = info
        .columns
        .iter()
        .filter(|c| smugglr_core::rowhash::is_blob_column(&c.col_type))
        .map(|c| c.name.clone())
        .collect();
    if blob_columns.is_empty() {
        return;
    }
    for row in maps.iter_mut() {
        for col in smugglr_core::rowhash::canonicalize_blob_columns(
            row,
            &blob_columns,
            smugglr_core::rowhash::BlobEncoding::Base64,
        ) {
            web_sys::console::warn_1(
                &format!(
                    "smugglr: blob column {} in {} did not decode as base64 -- it hashes divergently across backends; add it to exclude_columns",
                    col, info.name
                )
                .into(),
            );
        }
    }
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

/// Look up `table` in `cache`, computing and memoizing it via `fetch` on a
/// miss.
///
/// Shared by both adapters' `cached_table_info` inherent methods, which
/// differ only in how they obtain a fresh [`TableInfo`] on a cache miss
/// (`self.table_info(table)`, a `DataSource` trait method both implement).
/// `fetch` is passed as an already-constructed future rather than a closure:
/// futures are lazy, so `self.table_info(table)` at the call site does no
/// work until this function `.await`s it -- meaning the miss-only fetch
/// semantics are identical to the pre-extraction inline code. The
/// non-atomic check-then-insert (a benign duplicate-fetch race window) is
/// preserved exactly, as is clone-on-hit / insert-then-clone-return on miss.
pub(crate) async fn cached_table_info<F>(
    cache: &Mutex<HashMap<String, TableInfo>>,
    table: &str,
    fetch: F,
) -> Result<TableInfo>
where
    F: Future<Output = Result<TableInfo>>,
{
    if let Some(info) = cache.lock().unwrap().get(table) {
        return Ok(info.clone());
    }
    let info = fetch.await?;
    cache
        .lock()
        .unwrap()
        .insert(table.to_string(), info.clone());
    Ok(info)
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

    #[wasm_bindgen_test]
    fn row_maps_to_metadata_round_trips_integer_timestamp() {
        // Regression for #177: a remote SQL-over-HTTP endpoint returns an integer
        // Unix timestamp as a JSON number. The pre-fix as_str()-only extraction in
        // this builder dropped it to None, which forced the changed row into
        // content_differs -- skipped in both directions under newer_wins/uuid_v7_wins.
        // It must round-trip to the decimal string, matching how local.rs renders
        // the same value, so the two sides compare equal. Fails on the pre-fix code
        // (updated_at == None), passes after.
        let mut row = HashMap::new();
        row.insert("__pk".to_string(), Value::String("k1".to_string()));
        row.insert(
            "updated_at".to_string(),
            Value::Number(serde_json::Number::from(1_700_000_000_i64)),
        );
        row.insert("name".to_string(), Value::String("alice".to_string()));

        let meta = row_maps_to_metadata(
            &[row],
            &["name".to_string(), "updated_at".to_string()],
            "updated_at",
            &[],
            "items",
        );

        assert_eq!(
            meta.get("k1").expect("row keyed by __pk").updated_at,
            Some("1700000000".to_string())
        );
    }

    #[wasm_bindgen_test]
    fn row_maps_to_metadata_skips_null_primary_key() {
        // Regression for #231: a NULL rendered __pk (e.g. a NULL part of a
        // composite PK, since `||` propagates NULL) must be skipped, not coerced
        // to "". Two such rows previously collapsed onto a single "" key --
        // silently dropping one and provoking spurious deletes. Pre-fix this
        // returns 1 entry (keyed ""); after the fix it returns 0.
        let mut a = HashMap::new();
        a.insert("__pk".to_string(), Value::Null);
        a.insert("name".to_string(), Value::String("alice".to_string()));
        let mut b = HashMap::new();
        b.insert("__pk".to_string(), Value::Null);
        b.insert("name".to_string(), Value::String("bob".to_string()));

        let meta = row_maps_to_metadata(&[a, b], &["name".to_string()], "updated_at", &[], "items");

        assert!(
            meta.is_empty(),
            "NULL-__pk rows must be skipped, not collapsed onto one key; got {} entries",
            meta.len()
        );
    }
}
