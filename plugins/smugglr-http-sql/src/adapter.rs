//! HTTP SQL adapter implementing the PluginAdapter trait.

use crate::profile::{AuthFormat, Profile};
use reqwest::Client;
use serde_json::Value;
use sha2::{Digest, Sha256};
use smugglr_plugin_sdk::{ColumnInfo, PluginAdapter, PluginError, RowMeta, TableInfo};
use std::collections::HashMap;
use std::sync::Mutex;

pub struct HttpSqlAdapter {
    client: Option<Client>,
    url: String,
    auth_token: String,
    profile: Profile,
    table_info_cache: Mutex<HashMap<String, TableInfo>>,
}

impl HttpSqlAdapter {
    pub fn new() -> Self {
        Self {
            client: None,
            url: String::new(),
            auth_token: String::new(),
            profile: Profile::generic(),
            table_info_cache: Mutex::new(HashMap::new()),
        }
    }

    async fn cached_table_info(&self, table: &str) -> Result<TableInfo, PluginError> {
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

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<Value, PluginError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| PluginError::new("not initialized"))?;

        let body = self.profile.build_request(sql, params);
        let mut req = client.post(&self.url).json(&body);

        req = match self.profile.auth_format {
            AuthFormat::Bearer if !self.auth_token.is_empty() => req.bearer_auth(&self.auth_token),
            AuthFormat::Basic if !self.auth_token.is_empty() => {
                req.basic_auth(&self.auth_token, None::<&str>)
            }
            _ => req,
        };

        let resp = req
            .send()
            .await
            .map_err(|e| PluginError::new(format!("HTTP request failed: {}", e)))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(PluginError::new(format!(
                "HTTP {} from {}: {}",
                status, self.url, body
            )));
        }

        resp.json::<Value>()
            .await
            .map_err(|e| PluginError::new(format!("Failed to parse response JSON: {}", e)))
    }

    fn extract_rows(
        &self,
        response: &Value,
        columns: &[String],
    ) -> Result<Vec<Vec<Value>>, PluginError> {
        let rows_val = Profile::extract_path(response, &self.profile.rows_path)
            .ok_or_else(|| PluginError::new("rows not found in response"))?;

        match rows_val.as_array() {
            Some(arr) => Ok(arr
                .iter()
                .map(|row| {
                    if let Some(arr) = row.as_array() {
                        arr.clone()
                    } else if let Some(obj) = row.as_object() {
                        // Extract values in column order for consistency
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

    fn extract_columns(&self, response: &Value) -> Result<Vec<String>, PluginError> {
        // Try the configured columns_path first
        if let Some(cols_val) = Profile::extract_path(response, &self.profile.columns_path) {
            if let Some(arr) = cols_val.as_array() {
                // If array contains strings or {name: ...} objects, use them
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

                // If rows are objects, extract column names from the first row
                if let Some(first) = arr.first().and_then(|v| v.as_object()) {
                    return Ok(first.keys().cloned().collect());
                }
            }
        }

        // Fallback: extract columns from the first row object at rows_path
        if let Some(rows_val) = Profile::extract_path(response, &self.profile.rows_path) {
            if let Some(arr) = rows_val.as_array() {
                if let Some(first) = arr.first().and_then(|v| v.as_object()) {
                    return Ok(first.keys().cloned().collect());
                }
            }
        }

        Err(PluginError::new("columns not found in response"))
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

    /// Maximum rows per batch for a given column count and bind param limit.
    /// Returns `None` when there is no limit (max_bind_params == 0).
    fn max_rows_per_batch(num_columns: usize, max_bind_params: usize) -> Option<usize> {
        if max_bind_params > 0 && num_columns > 0 {
            Some((max_bind_params / num_columns).max(1))
        } else {
            None
        }
    }

    /// Generate a multi-row INSERT OR REPLACE statement with flattened params.
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

    /// Hash row content for change detection.
    ///
    /// IMPORTANT: This must match smugglr-core local.rs hashing algorithm exactly.
    /// local.rs hashes values only (no keys) in column definition order, using
    /// empty string for NULL. Any divergence breaks cross-source sync.
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
                    Value::Null => {} // empty string -- matches local.rs None behavior
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
}

impl PluginAdapter for HttpSqlAdapter {
    async fn initialize(&mut self, config: HashMap<String, String>) -> Result<(), PluginError> {
        self.url = config
            .get("url")
            .ok_or_else(|| PluginError::new("missing config: url"))?
            .clone();

        self.auth_token = config.get("auth_token").cloned().unwrap_or_default();

        let profile_name = config
            .get("profile")
            .map(String::as_str)
            .unwrap_or("generic");
        self.profile = Profile::from_name(profile_name)
            .ok_or_else(|| PluginError::new(format!("unknown profile: {}", profile_name)))?;

        self.client = Some(Client::new());

        // Test the connection with a simple query
        self.execute("SELECT 1", &[]).await?;
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<String>, PluginError> {
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

    async fn table_info(&self, table: &str) -> Result<TableInfo, PluginError> {
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
    ) -> Result<HashMap<String, RowMeta>, PluginError> {
        let info = self.cached_table_info(table).await?;
        if info.primary_key.is_empty() {
            return Err(PluginError::new(format!(
                "no primary key for table: {}",
                table
            )));
        }

        let pk_expr = if info.primary_key.len() == 1 {
            format!("CAST(\"{}\" AS TEXT)", info.primary_key[0])
        } else {
            let parts: Vec<String> = info
                .primary_key
                .iter()
                .map(|k| format!("CAST(\"{}\" AS TEXT)", k))
                .collect();
            parts.join(" || '|' || ")
        };

        // Column order from table_info -- must match local.rs hashing order
        let column_order: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();

        let sql = format!("SELECT *, {} AS __pk FROM \"{}\"", pk_expr, table);
        let response = self.execute(&sql, &[]).await?;
        let columns = self.extract_columns(&response)?;
        let rows = self.extract_rows(&response, &columns)?;
        let maps = self.rows_to_maps(&columns, &rows);

        let mut result = HashMap::new();
        for row in &maps {
            let pk = row
                .get("__pk")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let updated_at = row
                .get(timestamp_column)
                .and_then(|v| v.as_str())
                .map(String::from);
            let hash = Self::content_hash(row, &column_order, exclude_columns, timestamp_column);

            result.insert(
                pk.clone(),
                RowMeta {
                    pk_value: pk,
                    updated_at,
                    content_hash: hash,
                },
            );
        }

        Ok(result)
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, Value>>, PluginError> {
        if pk_values.is_empty() {
            return Ok(vec![]);
        }

        let info = self.cached_table_info(table).await?;
        let pk_expr = if info.primary_key.len() == 1 {
            format!("CAST(\"{}\" AS TEXT)", info.primary_key[0])
        } else {
            let parts: Vec<String> = info
                .primary_key
                .iter()
                .map(|k| format!("CAST(\"{}\" AS TEXT)", k))
                .collect();
            parts.join(" || '|' || ")
        };

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

    async fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, Value>],
    ) -> Result<usize, PluginError> {
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
                PluginError::new(format!(
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

    async fn row_count(&self, table: &str) -> Result<usize, PluginError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_hash_excludes_timestamp() {
        let cols = vec!["id".into(), "name".into(), "updated_at".into()];
        let mut row = HashMap::new();
        row.insert("id".into(), Value::from(1));
        row.insert("name".into(), Value::from("alice"));
        row.insert("updated_at".into(), Value::from("2026-01-01"));

        let hash1 = HttpSqlAdapter::content_hash(&row, &cols, &[], "updated_at");

        row.insert("updated_at".into(), Value::from("2026-12-31"));
        let hash2 = HttpSqlAdapter::content_hash(&row, &cols, &[], "updated_at");

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_content_hash_excludes_columns() {
        let cols = vec!["id".into(), "name".into(), "embedding".into()];
        let mut row = HashMap::new();
        row.insert("id".into(), Value::from(1));
        row.insert("name".into(), Value::from("alice"));
        row.insert("embedding".into(), Value::from("big blob"));

        let hash1 = HttpSqlAdapter::content_hash(&row, &cols, &["embedding".into()], "updated_at");

        row.insert("embedding".into(), Value::from("different blob"));
        let hash2 = HttpSqlAdapter::content_hash(&row, &cols, &["embedding".into()], "updated_at");

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_content_hash_changes_on_data_change() {
        let cols = vec!["id".into(), "name".into()];
        let mut row = HashMap::new();
        row.insert("id".into(), Value::from(1));
        row.insert("name".into(), Value::from("alice"));

        let hash1 = HttpSqlAdapter::content_hash(&row, &cols, &[], "updated_at");

        row.insert("name".into(), Value::from("bob"));
        let hash2 = HttpSqlAdapter::content_hash(&row, &cols, &[], "updated_at");

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_rows_to_maps() {
        let adapter = HttpSqlAdapter::new();
        let columns = vec!["id".into(), "name".into()];
        let rows = vec![
            vec![Value::from(1), Value::from("alice")],
            vec![Value::from(2), Value::from("bob")],
        ];
        let maps = adapter.rows_to_maps(&columns, &rows);
        assert_eq!(maps.len(), 2);
        assert_eq!(maps[0]["name"], "alice");
        assert_eq!(maps[1]["id"], 2);
    }

    fn make_row(id: i64, name: &str) -> HashMap<String, Value> {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::from(id));
        row.insert("name".to_string(), Value::from(name));
        row
    }

    #[test]
    fn batch_size_no_limit() {
        assert_eq!(HttpSqlAdapter::max_rows_per_batch(2, 0), None);
    }

    #[test]
    fn batch_size_d1_limit() {
        // 2 columns, 100 param limit -> 50 rows per batch
        assert_eq!(HttpSqlAdapter::max_rows_per_batch(2, 100), Some(50));
    }

    #[test]
    fn batch_size_wide_table() {
        // 50 columns, 100 param limit -> 2 rows per batch
        assert_eq!(HttpSqlAdapter::max_rows_per_batch(50, 100), Some(2));
    }

    #[test]
    fn batch_size_wider_than_limit() {
        // 150 columns, 100 param limit -> 1 row per batch (never zero)
        assert_eq!(HttpSqlAdapter::max_rows_per_batch(150, 100), Some(1));
    }

    #[test]
    fn batch_size_zero_columns() {
        assert_eq!(HttpSqlAdapter::max_rows_per_batch(0, 100), None);
    }

    #[test]
    fn generate_batch_sql_single_row() {
        let rows = vec![make_row(1, "alice")];
        let columns = vec!["id".to_string(), "name".to_string()];
        let (sql, params) = HttpSqlAdapter::generate_batch_sql("users", &columns, &rows);

        assert!(sql.starts_with("INSERT OR REPLACE INTO \"users\""));
        assert!(sql.contains("(?, ?)"));
        assert!(!sql.contains("), ("));
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn generate_batch_sql_multi_row() {
        let rows = vec![
            make_row(1, "alice"),
            make_row(2, "bob"),
            make_row(3, "charlie"),
        ];
        let columns = vec!["id".to_string(), "name".to_string()];
        let (sql, params) = HttpSqlAdapter::generate_batch_sql("users", &columns, &rows);

        assert!(sql.contains("(?, ?), (?, ?), (?, ?)"));
        assert_eq!(params.len(), 6);
    }

    #[test]
    fn generate_batch_sql_null_for_missing_column() {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::from(1));
        // "name" is missing from this row
        let columns = vec!["id".to_string(), "name".to_string()];
        let (_, params) = HttpSqlAdapter::generate_batch_sql("users", &columns, &[row]);

        assert_eq!(params[0], Value::from(1));
        assert_eq!(params[1], Value::Null);
    }

    #[test]
    fn batch_splitting_respects_param_limit() {
        // 10 columns, 100 param limit -> max 10 rows per batch
        let columns: Vec<String> = (0..10).map(|i| format!("col_{}", i)).collect();
        let rows: Vec<HashMap<String, Value>> = (0..25)
            .map(|i| {
                columns
                    .iter()
                    .map(|c| (c.clone(), Value::from(i)))
                    .collect()
            })
            .collect();

        let batch_size =
            HttpSqlAdapter::max_rows_per_batch(columns.len(), 100).unwrap_or(rows.len());
        assert_eq!(batch_size, 10);

        let batches: Vec<&[HashMap<String, Value>]> = rows.chunks(batch_size).collect();
        assert_eq!(batches.len(), 3); // 10 + 10 + 5
        assert_eq!(batches[0].len(), 10);
        assert_eq!(batches[1].len(), 10);
        assert_eq!(batches[2].len(), 5);

        // Verify no batch exceeds param limit
        for batch in &batches {
            let (_, params) = HttpSqlAdapter::generate_batch_sql("test", &columns, batch);
            assert!(params.len() <= 100);
        }
    }
}
