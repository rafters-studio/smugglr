//! Local SQLite adapter -- runs SQL against a JS-provided executor.
//!
//! Rust speaks SQL strings + parameter arrays; the JS side provides any
//! object satisfying `{ run(sql, params): Promise<{columns, rows}> }`.
//! First shipped executor is wa-sqlite + OPFS; the same adapter accepts
//! better-sqlite3 (Node), official sqlite-wasm, or sql.js without
//! changes here.

use sha2::{Digest, Sha256};
use smugglr_core::datasource::{ColumnInfo, DataSource, RowMeta, TableInfo};
use smugglr_core::error::{Result, SyncError};
use std::collections::HashMap;

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

    fn rows_to_maps(&self, columns: &[String], rows: &[Vec<Value>]) -> Vec<HashMap<String, Value>> {
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

    fn build_pk_text_expr(primary_key: &[String]) -> String {
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

    fn row_maps_to_metadata(
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
            let content_hash =
                Self::content_hash(row, column_order, exclude_columns, timestamp_column);
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

    fn content_hash(
        row: &HashMap<String, Value>,
        columns_in_order: &[String],
        exclude: &[String],
        timestamp_column: &str,
    ) -> String {
        let timestamp_columns = ["updated_at", "created_at"];
        let mut hasher = Sha256::new();
        for col in columns_in_order {
            if timestamp_columns.contains(&col.as_str())
                || exclude.iter().any(|e| e == col)
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

        let pk_expr = Self::build_pk_text_expr(&info.primary_key);
        let column_order: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();
        let sql = format!(
            "SELECT *, {} AS __pk FROM \"{}\" WHERE \"{}\" > ?",
            pk_expr, table, timestamp_column
        );
        let params = vec![Value::String(since_timestamp.to_string())];
        let result = self.run(&sql, &params).await?;
        let maps = self.rows_to_maps(&result.columns, &result.rows);

        Ok(Self::row_maps_to_metadata(
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

    fn generate_batch_sql(
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

        let name_idx = result.columns.iter().position(|c| c == "name").unwrap_or(1);
        let type_idx = result.columns.iter().position(|c| c == "type").unwrap_or(2);
        let notnull_idx = result
            .columns
            .iter()
            .position(|c| c == "notnull")
            .unwrap_or(3);
        let pk_idx = result.columns.iter().position(|c| c == "pk").unwrap_or(5);

        let mut col_infos = Vec::new();
        let mut primary_key = Vec::new();

        for row in &result.rows {
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

        Ok(TableInfo {
            name: table.to_string(),
            columns: col_infos,
            primary_key,
        })
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

        let pk_expr = Self::build_pk_text_expr(&info.primary_key);
        let column_order: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();
        let sql = format!("SELECT *, {} AS __pk FROM \"{}\"", pk_expr, table);
        let result = self.run(&sql, &[]).await?;
        let maps = self.rows_to_maps(&result.columns, &result.rows);

        Ok(Self::row_maps_to_metadata(
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
        let pk_expr = Self::build_pk_text_expr(&info.primary_key);

        let placeholders: Vec<String> = pk_values.iter().map(|_| "?".to_string()).collect();
        let params: Vec<Value> = pk_values.iter().map(|v| Value::String(v.clone())).collect();
        let sql = format!(
            "SELECT * FROM \"{}\" WHERE {} IN ({})",
            table,
            pk_expr,
            placeholders.join(", ")
        );

        let result = self.run(&sql, &params).await?;
        Ok(self.rows_to_maps(&result.columns, &result.rows))
    }

    async fn upsert_rows(&self, table: &str, rows: &[HashMap<String, Value>]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let columns: Vec<String> = rows[0].keys().cloned().collect();
        let (sql, params) = Self::generate_batch_sql(table, &columns, rows);
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
