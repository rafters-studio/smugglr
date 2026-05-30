//! Local SQLite adapter -- runs SQL against a JS-provided executor.
//!
//! Rust speaks SQL strings + parameter arrays; the JS side provides any
//! object satisfying `{ run(sql, params): Promise<{columns, rows}> }`.
//! First shipped executor is wa-sqlite + OPFS; the same adapter accepts
//! better-sqlite3 (Node), official sqlite-wasm, or sql.js without
//! changes here.

use smugglr_core::datasource::{DataSource, RowMeta, TableInfo};
use smugglr_core::error::{Result, SyncError};
use std::collections::HashMap;

use crate::adapter_common;

use serde_json::Value;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

pub struct LocalSqlDataSource {
    executor: JsValue,
    table_info_cache: std::sync::Mutex<HashMap<String, TableInfo>>,
}

// wasm32 is single-threaded; the !Sync of JsValue would be load-bearing on
// multi-threaded targets, but here no cross-thread sharing is possible.
// Same pattern the rest of the wasm-bindgen ecosystem uses for storing
// JsValues in long-lived structs.
unsafe impl Send for LocalSqlDataSource {}
unsafe impl Sync for LocalSqlDataSource {}

impl LocalSqlDataSource {
    pub fn new(executor: JsValue) -> Self {
        Self {
            executor,
            table_info_cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Issue `DELETE FROM "<table>"`, leaving the schema in place.
    /// Used by `Smugglr.eraseLocal()` for GDPR-style local wipes.
    pub(crate) async fn delete_all_rows(&self, table: &str) -> Result<()> {
        let sql = format!("DELETE FROM \"{}\"", table);
        self.run(&sql, &[]).await?;
        Ok(())
    }

    async fn run(&self, sql: &str, params: &[Value]) -> Result<RunResult> {
        let run_fn = js_sys::Reflect::get(&self.executor, &JsValue::from_str("run"))
            .map_err(|e| SyncError::Remote(format!("executor.run missing: {:?}", e)))?
            .dyn_into::<js_sys::Function>()
            .map_err(|_| SyncError::Remote("executor.run is not a function".into()))?;

        let params_js = serde_wasm_bindgen::to_value(params)
            .map_err(|e| SyncError::Remote(format!("failed to serialize params: {}", e)))?;

        let promise = run_fn
            .call2(&self.executor, &JsValue::from_str(sql), &params_js)
            .map_err(|e| SyncError::Remote(format!("executor.run threw: {:?}", e)))?
            .dyn_into::<js_sys::Promise>()
            .map_err(|_| SyncError::Remote("executor.run did not return a Promise".into()))?;

        let result_js = JsFuture::from(promise)
            .await
            .map_err(|e| SyncError::Remote(format!("executor.run rejected: {:?}", e)))?;

        serde_wasm_bindgen::from_value(result_js)
            .map_err(|e| SyncError::Remote(format!("invalid executor result shape: {}", e)))
    }

    pub async fn get_row_metadata_since(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
        since_timestamp: &str,
    ) -> Result<HashMap<String, RowMeta>> {
        let info = self.cached_table_info(table).await?;
        if info.primary_key.is_empty() {
            return Err(SyncError::Config(format!(
                "no primary key for table: {}",
                table
            )));
        }

        let pk_expr = adapter_common::build_pk_text_expr(&info.primary_key);
        let column_order: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();
        let sql = format!(
            "SELECT *, {} AS __pk FROM \"{}\" WHERE \"{}\" > ?",
            pk_expr, table, timestamp_column
        );
        let params = vec![Value::String(since_timestamp.to_string())];
        let result = self.run(&sql, &params).await?;
        let maps = adapter_common::rows_to_maps(&result.columns, &result.rows);

        Ok(adapter_common::row_maps_to_metadata(
            &maps,
            &column_order,
            timestamp_column,
            exclude_columns,
        ))
    }

    async fn cached_table_info(&self, table: &str) -> Result<TableInfo> {
        if let Some(info) = self.table_info_cache.lock().unwrap().get(table) {
            return Ok(info.clone());
        }
        let info = self.table_info(table).await?;
        self.table_info_cache
            .lock()
            .unwrap()
            .insert(table.to_string(), info.clone());
        Ok(info)
    }
}

#[derive(serde::Deserialize)]
struct RunResult {
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    rows: Vec<Vec<Value>>,
}

impl DataSource for LocalSqlDataSource {
    async fn list_tables(&self) -> Result<Vec<String>> {
        let result = self
            .run(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
                &[],
            )
            .await?;

        let name_idx = result.columns.iter().position(|c| c == "name").unwrap_or(0);
        Ok(result
            .rows
            .iter()
            .filter_map(|row| row.get(name_idx).and_then(|v| v.as_str()).map(String::from))
            .collect())
    }

    async fn table_info(&self, table: &str) -> Result<TableInfo> {
        let result = self
            .run(&format!("PRAGMA table_info('{}')", table), &[])
            .await?;
        Ok(adapter_common::parse_table_info(
            table,
            &result.columns,
            &result.rows,
        ))
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> Result<HashMap<String, RowMeta>> {
        let info = self.cached_table_info(table).await?;
        if info.primary_key.is_empty() {
            return Err(SyncError::Config(format!(
                "no primary key for table: {}",
                table
            )));
        }

        let pk_expr = adapter_common::build_pk_text_expr(&info.primary_key);
        let column_order: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();
        let sql = format!("SELECT *, {} AS __pk FROM \"{}\"", pk_expr, table);
        let result = self.run(&sql, &[]).await?;
        let maps = adapter_common::rows_to_maps(&result.columns, &result.rows);

        Ok(adapter_common::row_maps_to_metadata(
            &maps,
            &column_order,
            timestamp_column,
            exclude_columns,
        ))
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, Value>>> {
        if pk_values.is_empty() {
            return Ok(vec![]);
        }

        let info = self.cached_table_info(table).await?;
        let pk_expr = adapter_common::build_pk_text_expr(&info.primary_key);

        let placeholders: Vec<String> = pk_values.iter().map(|_| "?".to_string()).collect();
        let params: Vec<Value> = pk_values.iter().map(|v| Value::String(v.clone())).collect();
        let sql = format!(
            "SELECT * FROM \"{}\" WHERE {} IN ({})",
            table,
            pk_expr,
            placeholders.join(", ")
        );

        let result = self.run(&sql, &params).await?;
        Ok(adapter_common::rows_to_maps(&result.columns, &result.rows))
    }

    async fn upsert_rows(&self, table: &str, rows: &[HashMap<String, Value>]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let columns: Vec<String> = rows[0].keys().cloned().collect();
        let (sql, params) = adapter_common::generate_batch_sql(table, &columns, rows);
        self.run(&sql, &params).await.map_err(|e| {
            SyncError::Remote(format!(
                "batch upsert failed for table '{}' ({} rows): {}",
                table,
                rows.len(),
                e
            ))
        })?;
        Ok(rows.len())
    }

    async fn row_count(&self, table: &str) -> Result<usize> {
        let sql = format!("SELECT COUNT(*) AS cnt FROM \"{}\"", table);
        let result = self.run(&sql, &[]).await?;
        let count = result
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        Ok(count as usize)
    }
}
