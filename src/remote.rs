//! Cloudflare D1 HTTP API client

use crate::error::{Result, SyncError};
use crate::local::RowMeta;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tracing::{debug, info};

/// D1 API client
pub struct D1Client {
    client: Client,
    account_id: String,
    database_id: String,
    api_token: String,
}

/// D1 query request
#[derive(Debug, Serialize)]
struct D1Request {
    sql: String,
    params: Vec<JsonValue>,
}

/// D1 API response
#[derive(Debug, Deserialize)]
struct D1Response {
    result: Option<Vec<D1Result>>,
    success: bool,
    errors: Option<Vec<D1Error>>,
}

#[derive(Debug, Deserialize)]
struct D1Result {
    results: Option<Vec<HashMap<String, JsonValue>>>,
    #[allow(dead_code)]
    success: bool,
    meta: Option<D1Meta>,
}

#[derive(Debug, Deserialize)]
struct D1Meta {
    #[allow(dead_code)]
    changed_db: Option<bool>,
    changes: Option<i64>,
    #[allow(dead_code)]
    duration: Option<f64>,
    #[allow(dead_code)]
    rows_read: Option<i64>,
    #[allow(dead_code)]
    rows_written: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct D1Error {
    code: Option<i64>,
    message: String,
}

impl D1Client {
    /// Create a new D1 client
    pub fn new(account_id: String, database_id: String, api_token: String) -> Self {
        Self {
            client: Client::new(),
            account_id,
            database_id,
            api_token,
        }
    }

    /// Get the API endpoint URL
    fn endpoint(&self) -> String {
        format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query",
            self.account_id, self.database_id
        )
    }

    /// Execute a SQL query
    async fn query(
        &self,
        sql: &str,
        params: Vec<JsonValue>,
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        let request = D1Request {
            sql: sql.to_string(),
            params,
        };

        debug!("D1 query: {}", sql);

        let response = self
            .client
            .post(self.endpoint())
            .bearer_auth(&self.api_token)
            .json(&request)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            return Err(SyncError::Remote(format!(
                "HTTP {}: {}",
                status,
                body.chars().take(500).collect::<String>()
            )));
        }

        let d1_response: D1Response = serde_json::from_str(&body)?;

        if !d1_response.success {
            if let Some(errors) = d1_response.errors {
                if let Some(err) = errors.first() {
                    return Err(SyncError::D1Api {
                        message: err.message.clone(),
                        code: err.code,
                    });
                }
            }
            return Err(SyncError::Remote("Unknown D1 error".to_string()));
        }

        let results = d1_response
            .result
            .and_then(|r| r.into_iter().next())
            .and_then(|r| r.results)
            .unwrap_or_default();

        Ok(results)
    }

    /// Execute a write query (INSERT, UPDATE, DELETE)
    async fn execute(&self, sql: &str, params: Vec<JsonValue>) -> Result<i64> {
        let request = D1Request {
            sql: sql.to_string(),
            params,
        };

        debug!("D1 execute: {}", sql);

        let response = self
            .client
            .post(self.endpoint())
            .bearer_auth(&self.api_token)
            .json(&request)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            return Err(SyncError::Remote(format!(
                "HTTP {}: {}",
                status,
                body.chars().take(500).collect::<String>()
            )));
        }

        let d1_response: D1Response = serde_json::from_str(&body)?;

        if !d1_response.success {
            if let Some(errors) = d1_response.errors {
                if let Some(err) = errors.first() {
                    return Err(SyncError::D1Api {
                        message: err.message.clone(),
                        code: err.code,
                    });
                }
            }
            return Err(SyncError::Remote("Unknown D1 error".to_string()));
        }

        let changes = d1_response
            .result
            .and_then(|r| r.into_iter().next())
            .and_then(|r| r.meta)
            .and_then(|m| m.changes)
            .unwrap_or(0);

        Ok(changes)
    }

    /// Test connection to D1
    pub async fn test_connection(&self) -> Result<()> {
        info!("Testing D1 connection...");
        let results = self.query("SELECT 1 as test", vec![]).await?;
        if results.is_empty() {
            return Err(SyncError::Remote("Empty response from D1".to_string()));
        }
        info!("D1 connection successful");
        Ok(())
    }

    /// List all tables in D1
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let results = self
            .query(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
                vec![],
            )
            .await?;

        let tables: Vec<String> = results
            .into_iter()
            .filter_map(|row| {
                row.get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        debug!("Found {} tables in D1", tables.len());
        Ok(tables)
    }

    /// Get table column info
    pub async fn table_columns(&self, table: &str) -> Result<Vec<String>> {
        let results = self
            .query(&format!("PRAGMA table_info(\"{}\")", table), vec![])
            .await?;

        let columns: Vec<String> = results
            .into_iter()
            .filter_map(|row| {
                row.get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        Ok(columns)
    }

    /// Get primary key columns for a table
    pub async fn primary_key_columns(&self, table: &str) -> Result<Vec<String>> {
        let results = self
            .query(&format!("PRAGMA table_info(\"{}\")", table), vec![])
            .await?;

        let pk_cols: Vec<String> = results
            .into_iter()
            .filter(|row| {
                row.get("pk")
                    .and_then(|v| v.as_i64())
                    .map(|n| n > 0)
                    .unwrap_or(false)
            })
            .filter_map(|row| {
                row.get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        if pk_cols.is_empty() {
            return Err(SyncError::NoPrimaryKey(table.to_string()));
        }

        Ok(pk_cols)
    }

    /// Check if table has a specific column
    #[allow(dead_code)]
    pub async fn has_column(&self, table: &str, column: &str) -> Result<bool> {
        let columns = self.table_columns(table).await?;
        Ok(columns.iter().any(|c| c == column))
    }

    /// Get row metadata for change detection
    pub async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
    ) -> Result<HashMap<String, RowMeta>> {
        let pk_cols = self.primary_key_columns(table).await?;
        let columns = self.table_columns(table).await?;
        let has_timestamp = columns.iter().any(|c| c == timestamp_column);

        // Build the SELECT query
        let pk_select = pk_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(" || '|' || ");

        let all_cols = columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "SELECT {} as __pk, {} FROM \"{}\"",
            pk_select, all_cols, table
        );

        let results = self.query(&sql, vec![]).await?;
        let mut metadata = HashMap::new();

        for row in results {
            let pk_value = row
                .get("__pk")
                .and_then(|v| match v {
                    JsonValue::String(s) => Some(s.clone()),
                    JsonValue::Number(n) => Some(n.to_string()),
                    _ => None,
                })
                .unwrap_or_default();

            // Build content hash from columns (excluding timestamp columns)
            let timestamp_columns = ["updated_at", "created_at"];
            let mut hasher = Sha256::new();
            for col in &columns {
                // Skip timestamp columns in content hash
                if timestamp_columns.contains(&col.as_str()) {
                    continue;
                }
                let val = row
                    .get(col)
                    .map(|v| match v {
                        JsonValue::Null => String::new(),
                        JsonValue::String(s) => s.clone(),
                        _ => v.to_string(),
                    })
                    .unwrap_or_default();
                hasher.update(val.as_bytes());
                hasher.update(b"|");
            }
            let content_hash = hex::encode(hasher.finalize());

            // Handle updated_at as either integer (Unix timestamp) or string
            let updated_at = if has_timestamp {
                row.get(timestamp_column).and_then(|v| {
                    // Try integer first, then string
                    if let Some(n) = v.as_i64() {
                        Some(n.to_string())
                    } else {
                        v.as_str().map(|s| s.to_string())
                    }
                })
            } else {
                None
            };

            metadata.insert(
                pk_value.clone(),
                RowMeta {
                    pk_value,
                    updated_at,
                    content_hash,
                },
            );
        }

        debug!("Got {} rows from D1 table {}", metadata.len(), table);
        Ok(metadata)
    }

    /// Get full row data for specific primary keys
    pub async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        if pk_values.is_empty() {
            return Ok(vec![]);
        }

        let pk_cols = self.primary_key_columns(table).await?;
        let columns = self.table_columns(table).await?;

        let pk_expr = pk_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(" || '|' || ");

        let cols = columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        // D1 has a limit on query size, so we batch
        let batch_size = 100;
        let mut all_results = Vec::new();

        for chunk in pk_values.chunks(batch_size) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let sql = format!(
                "SELECT {} FROM \"{}\" WHERE {} IN ({})",
                cols, table, pk_expr, placeholders
            );

            let params: Vec<JsonValue> =
                chunk.iter().map(|v| JsonValue::String(v.clone())).collect();
            let results = self.query(&sql, params).await?;
            all_results.extend(results);
        }

        Ok(all_results)
    }

    /// Insert or replace rows
    pub async fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, JsonValue>],
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let columns = self.table_columns(table).await?;
        let col_list = columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        let mut total_changes = 0;

        // D1 has limits, so we process one row at a time
        // (batch operations are limited in D1)
        for row in rows {
            let placeholders = columns.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let sql = format!(
                "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({})",
                table, col_list, placeholders
            );

            let params: Vec<JsonValue> = columns
                .iter()
                .map(|col| row.get(col).cloned().unwrap_or(JsonValue::Null))
                .collect();

            let changes = self.execute(&sql, params).await?;
            total_changes += changes as usize;
        }

        info!("Upserted {} rows into D1 table {}", total_changes, table);
        Ok(total_changes)
    }

    /// Delete rows by primary key
    /// Reserved for full sync mode
    #[allow(dead_code)]
    pub async fn delete_rows(&self, table: &str, pk_values: &[String]) -> Result<usize> {
        if pk_values.is_empty() {
            return Ok(0);
        }

        let pk_cols = self.primary_key_columns(table).await?;
        let pk_expr = pk_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(" || '|' || ");

        let mut total_changes = 0;

        // Delete in batches
        let batch_size = 100;
        for chunk in pk_values.chunks(batch_size) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
            let sql = format!(
                "DELETE FROM \"{}\" WHERE {} IN ({})",
                table, pk_expr, placeholders
            );

            let params: Vec<JsonValue> =
                chunk.iter().map(|v| JsonValue::String(v.clone())).collect();
            let changes = self.execute(&sql, params).await?;
            total_changes += changes as usize;
        }

        info!("Deleted {} rows from D1 table {}", total_changes, table);
        Ok(total_changes)
    }

    /// Get row count for a table
    pub async fn row_count(&self, table: &str) -> Result<usize> {
        let results = self
            .query(
                &format!("SELECT COUNT(*) as count FROM \"{}\"", table),
                vec![],
            )
            .await?;

        let count = results
            .first()
            .and_then(|r| r.get("count"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;

        Ok(count)
    }
}
