//! HTTP SQL adapter using browser fetch API.
//!
//! Implements the DataSource trait from smugglr-core using web-sys fetch
//! instead of reqwest. Shares profile definitions with the native http-sql plugin.

use sha2::{Digest, Sha256};
use smugglr_core::datasource::{ColumnInfo, DataSource, RowMeta, TableInfo};
use smugglr_core::error::{Result, SyncError};
use smugglr_core::profile::{AuthFormat, Profile};
use std::collections::HashMap;

use serde_json::Value;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Request, RequestInit, RequestMode, Response};

pub struct FetchDataSource {
    url: String,
    // Interior-mutable so `Smugglr.updateAuth(...)` can swap the token at
    // runtime without touching the table_info_cache or the source endpoint.
    auth_token: std::sync::Mutex<String>,
    profile: Profile,
    table_info_cache: std::sync::Mutex<HashMap<String, TableInfo>>,
}

impl FetchDataSource {
    pub fn new(url: String, auth_token: String, profile: Profile) -> Self {
        Self {
            url,
            auth_token: std::sync::Mutex::new(auth_token),
            profile,
            table_info_cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Replace the auth token. Subsequent requests use the new value.
    /// Safe to call mid-flight; no in-flight request is mutated.
    pub fn set_auth_token(&self, token: String) {
        *self.auth_token.lock().unwrap() = token;
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<Value> {
        let body = self.profile.build_request(sql, params);
        let body_str =
            serde_json::to_string(&body).map_err(|e| SyncError::Remote(e.to_string()))?;

        let opts = RequestInit::new();
        opts.set_method("POST");
        opts.set_mode(RequestMode::Cors);
        opts.set_body(&wasm_bindgen::JsValue::from_str(&body_str));

        let request = Request::new_with_str_and_init(&self.url, &opts)
            .map_err(|e| SyncError::Remote(format!("failed to create request: {:?}", e)))?;

        let headers = request.headers();
        headers
            .set("Content-Type", "application/json")
            .map_err(|e| SyncError::Remote(format!("failed to set header: {:?}", e)))?;

        let token = self.auth_token.lock().unwrap().clone();
        if !token.is_empty() {
            match self.profile.auth_format {
                AuthFormat::Bearer => {
                    headers
                        .set("Authorization", &format!("Bearer {}", token))
                        .map_err(|e| {
                            SyncError::Remote(format!("failed to set auth header: {:?}", e))
                        })?;
                }
                AuthFormat::Basic => {
                    headers
                        .set("Authorization", &format!("Basic {}", token))
                        .map_err(|e| {
                            SyncError::Remote(format!("failed to set auth header: {:?}", e))
                        })?;
                }
                AuthFormat::None => {}
            }
        }

        let global = js_sys::global();
        let resp_value = if let Some(window) = global.dyn_ref::<web_sys::Window>() {
            JsFuture::from(window.fetch_with_request(&request)).await
        } else {
            // WorkerGlobalScope or other environments
            let fetch_fn = js_sys::Reflect::get(&global, &"fetch".into())
                .map_err(|e| SyncError::Remote(format!("fetch not available: {:?}", e)))?;
            let fetch_fn = fetch_fn
                .dyn_into::<js_sys::Function>()
                .map_err(|_| SyncError::Remote("fetch is not a function".into()))?;
            JsFuture::from(
                fetch_fn
                    .call1(&global, &request)
                    .map_err(|e| SyncError::Remote(format!("fetch call failed: {:?}", e)))?
                    .dyn_into::<js_sys::Promise>()
                    .map_err(|_| SyncError::Remote("fetch did not return a promise".into()))?,
            )
            .await
        }
        .map_err(|e| SyncError::Remote(format!("fetch failed: {:?}", e)))?;

        let resp: Response = resp_value
            .dyn_into()
            .map_err(|_| SyncError::Remote("response is not a Response".into()))?;

        if !resp.ok() {
            let status = resp.status();
            let body_text = JsFuture::from(
                resp.text()
                    .map_err(|e| SyncError::Remote(format!("failed to read body: {:?}", e)))?,
            )
            .await
            .map_err(|e| SyncError::Remote(format!("failed to read body: {:?}", e)))?;
            let body_str = body_text.as_string().unwrap_or_default();
            return Err(SyncError::Remote(format!(
                "HTTP {} from {}: {}",
                status, self.url, body_str
            )));
        }

        let json_promise = resp
            .json()
            .map_err(|e| SyncError::Remote(format!("failed to parse JSON: {:?}", e)))?;
        let json_value = JsFuture::from(json_promise)
            .await
            .map_err(|e| SyncError::Remote(format!("failed to parse JSON: {:?}", e)))?;

        serde_wasm_bindgen::from_value(json_value)
            .map_err(|e| SyncError::Remote(format!("JSON deserialization failed: {}", e)))
    }

    fn extract_rows(&self, response: &Value, columns: &[String]) -> Result<Vec<Vec<Value>>> {
        let rows_val = Profile::extract_path(response, &self.profile.rows_path)
            .ok_or_else(|| SyncError::Remote("rows not found in response".into()))?;

        match rows_val.as_array() {
            Some(arr) => Ok(arr
                .iter()
                .map(|row| {
                    if let Some(arr) = row.as_array() {
                        arr.clone()
                    } else if let Some(obj) = row.as_object() {
                        columns
                            .iter()
                            .map(|c| obj.get(c).cloned().unwrap_or(Value::Null))
                            .collect()
                    } else {
                        vec![row.clone()]
                    }
                })
                .collect()),
            None => Ok(vec![]),
        }
    }

    fn extract_columns(&self, response: &Value) -> Result<Vec<String>> {
        if let Some(cols_val) = Profile::extract_path(response, &self.profile.columns_path) {
            if let Some(arr) = cols_val.as_array() {
                let names: Vec<String> = arr
                    .iter()
                    .filter_map(|v| {
                        if let Some(s) = v.as_str() {
                            Some(s.to_string())
                        } else if let Some(obj) = v.as_object() {
                            obj.get("name").and_then(|n| n.as_str()).map(String::from)
                        } else {
                            None
                        }
                    })
                    .collect();

                if !names.is_empty() {
                    return Ok(names);
                }

                if let Some(first) = arr.first().and_then(|v| v.as_object()) {
                    return Ok(first.keys().cloned().collect());
                }
            }
        }

        if let Some(rows_val) = Profile::extract_path(response, &self.profile.rows_path) {
            if let Some(arr) = rows_val.as_array() {
                if let Some(first) = arr.first().and_then(|v| v.as_object()) {
                    return Ok(first.keys().cloned().collect());
                }
            }
        }

        Err(SyncError::Remote("columns not found in response".into()))
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

    /// Build a SQLite expression that casts primary key columns to TEXT.
    ///
    /// For single-column PKs this is `CAST("col" AS TEXT)`. For composite PKs
    /// the parts are joined with `|` to produce a stable string form matching
    /// the rest of smugglr's primary key encoding.
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

    /// Convert result rows (each with a synthetic `__pk` column) into RowMeta
    /// entries keyed by primary key. Used by both full-scan and incremental
    /// metadata fetches.
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

    /// Content hash matching smugglr-core local.rs algorithm exactly.
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

    /// Query row metadata for rows with `timestamp_column > since_timestamp`.
    ///
    /// Used by the incremental diff path to fetch only changed rows instead of
    /// the full table.
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
        let response = self.execute(&sql, &params).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        let maps = self.rows_to_maps(&columns, &rows);

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

    fn max_rows_per_batch(num_columns: usize, max_bind_params: usize) -> Option<usize> {
        if max_bind_params > 0 && num_columns > 0 {
            Some((max_bind_params / num_columns).max(1))
        } else {
            None
        }
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

impl DataSource for FetchDataSource {
    async fn list_tables(&self) -> Result<Vec<String>> {
        let response = self
            .execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' ORDER BY name",
                &[],
            )
            .await?;

        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;

        let name_idx = columns.iter().position(|c| c == "name").unwrap_or(0);
        Ok(rows
            .iter()
            .filter_map(|row| row.get(name_idx).and_then(|v| v.as_str()).map(String::from))
            .collect())
    }

    async fn table_info(&self, table: &str) -> Result<TableInfo> {
        let response = self
            .execute(&format!("PRAGMA table_info('{}')", table), &[])
            .await?;

        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;

        let name_idx = columns.iter().position(|c| c == "name").unwrap_or(1);
        let type_idx = columns.iter().position(|c| c == "type").unwrap_or(2);
        let notnull_idx = columns.iter().position(|c| c == "notnull").unwrap_or(3);
        let pk_idx = columns.iter().position(|c| c == "pk").unwrap_or(5);

        let mut col_infos = Vec::new();
        let mut primary_key = Vec::new();

        for row in &rows {
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
        let response = self.execute(&sql, &[]).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        let maps = self.rows_to_maps(&columns, &rows);

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

        let response = self.execute(&sql, &params).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        Ok(self.rows_to_maps(&columns, &rows))
    }

    async fn upsert_rows(&self, table: &str, rows: &[HashMap<String, Value>]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let columns: Vec<String> = rows[0].keys().cloned().collect();
        let batch_size = Self::max_rows_per_batch(columns.len(), self.profile.max_bind_params)
            .unwrap_or(rows.len());

        let mut total = 0;
        for batch in rows.chunks(batch_size) {
            let (sql, params) = Self::generate_batch_sql(table, &columns, batch);

            self.execute(&sql, &params).await.map_err(|e| {
                SyncError::Remote(format!(
                    "batch upsert failed for table '{}' ({} rows in batch): {}",
                    table,
                    batch.len(),
                    e
                ))
            })?;
            total += batch.len();
        }

        Ok(total)
    }

    async fn row_count(&self, table: &str) -> Result<usize> {
        let sql = format!("SELECT COUNT(*) AS cnt FROM \"{}\"", table);
        let response = self.execute(&sql, &[]).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        let count = rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        Ok(count as usize)
    }
}
