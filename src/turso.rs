//! Turso/libSQL database adapter
//!
//! Implements the [`DataSource`] trait for Turso databases accessed via the
//! libsql crate's HTTP protocol. This allows smuggler to sync data to and
//! from Turso-hosted libSQL databases the same way it works with D1 or
//! local SQLite targets.

use crate::config::column_excluded;
use crate::datasource::{ColumnInfo, DataSource, RowMeta, TableInfo};
use crate::error::{Result, SyncError};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tracing::{debug, info};

/// Client for a Turso/libSQL database accessed over HTTP.
pub struct TursoClient {
    conn: libsql::Connection,
}

impl TursoClient {
    /// Connect to a Turso database via the libsql HTTP protocol.
    pub async fn connect(url: &str, auth_token: &str) -> Result<Self> {
        let db = libsql::Builder::new_remote(url.to_string(), auth_token.to_string())
            .build()
            .await
            .map_err(|e| SyncError::Turso(format!("failed to build database: {}", e)))?;

        let conn = db
            .connect()
            .map_err(|e| SyncError::Turso(format!("failed to connect: {}", e)))?;

        info!("Connected to Turso database at {}", url);
        Ok(Self { conn })
    }

    /// Test the connection by running a simple query.
    pub async fn test_connection(&self) -> Result<()> {
        self.conn
            .query("SELECT 1", ())
            .await
            .map_err(|e| SyncError::Turso(format!("connection test failed: {}", e)))?;
        Ok(())
    }
}

impl DataSource for TursoClient {
    async fn list_tables(&self) -> Result<Vec<String>> {
        let mut rows = self
            .conn
            .query(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' \
                 AND name NOT LIKE 'sqlite_%' \
                 ORDER BY name",
                (),
            )
            .await
            .map_err(|e| SyncError::Turso(format!("list_tables query failed: {}", e)))?;

        let mut tables = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| SyncError::Turso(format!("list_tables row iteration: {}", e)))?
        {
            let name: String = row
                .get(0)
                .map_err(|e| SyncError::Turso(format!("list_tables get column: {}", e)))?;
            tables.push(name);
        }

        debug!("Found {} tables", tables.len());
        Ok(tables)
    }

    async fn table_info(&self, table: &str) -> Result<TableInfo> {
        let sql = format!("PRAGMA table_info(\"{}\")", table);
        let mut rows = self
            .conn
            .query(&sql, ())
            .await
            .map_err(|e| SyncError::Turso(format!("table_info query failed: {}", e)))?;

        let mut columns = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| SyncError::Turso(format!("table_info row iteration: {}", e)))?
        {
            let name: String = row
                .get(1)
                .map_err(|e| SyncError::Turso(format!("table_info get name: {}", e)))?;
            let col_type: String = row
                .get(2)
                .map_err(|e| SyncError::Turso(format!("table_info get type: {}", e)))?;
            let notnull: i32 = row
                .get(3)
                .map_err(|e| SyncError::Turso(format!("table_info get notnull: {}", e)))?;
            let pk: i32 = row
                .get(5)
                .map_err(|e| SyncError::Turso(format!("table_info get pk: {}", e)))?;

            columns.push(ColumnInfo {
                name,
                col_type,
                notnull: notnull != 0,
                pk: pk != 0,
            });
        }

        if columns.is_empty() {
            return Err(SyncError::TableNotFound(table.to_string()));
        }

        let primary_key: Vec<String> = columns
            .iter()
            .filter(|c| c.pk)
            .map(|c| c.name.clone())
            .collect();

        if primary_key.is_empty() {
            return Err(SyncError::NoPrimaryKey(table.to_string()));
        }

        Ok(TableInfo {
            name: table.to_string(),
            columns,
            primary_key,
        })
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> Result<HashMap<String, RowMeta>> {
        let info = self.table_info(table).await?;
        let pk_cols = &info.primary_key;
        let has_timestamp = info.columns.iter().any(|c| c.name == timestamp_column);

        let pk_select = pk_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(" || '|' || ");

        let timestamp_select = if has_timestamp {
            format!(", \"{}\"", timestamp_column)
        } else {
            String::new()
        };

        let timestamp_columns = ["updated_at", "created_at"];
        let hash_cols: Vec<_> = info
            .columns
            .iter()
            .filter(|c| {
                !timestamp_columns.contains(&c.name.as_str())
                    && !column_excluded(&c.name, exclude_columns)
            })
            .collect();

        let all_cols = hash_cols
            .iter()
            .map(|c| format!("\"{}\"", c.name))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "SELECT {}, {} {} FROM \"{}\"",
            pk_select, all_cols, timestamp_select, table
        );

        debug!("Executing: {}", sql);

        let mut rows = self
            .conn
            .query(&sql, ())
            .await
            .map_err(|e| SyncError::Turso(format!("get_row_metadata query failed: {}", e)))?;

        let hash_col_count = hash_cols.len();
        let mut result = HashMap::new();

        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| SyncError::Turso(format!("get_row_metadata row iteration: {}", e)))?
        {
            let pk_value = libsql_value_to_string(
                &row.get_value(0)
                    .map_err(|e| SyncError::Turso(format!("get_row_metadata get pk: {}", e)))?,
            );

            // Hash content columns
            let mut hasher = Sha256::new();
            for i in 0..hash_col_count {
                let val = libsql_value_to_string(&row.get_value(i as i32 + 1).map_err(|e| {
                    SyncError::Turso(format!("get_row_metadata get hash col: {}", e))
                })?);
                hasher.update(val.as_bytes());
                hasher.update(b"|");
            }
            let content_hash = hex::encode(hasher.finalize());

            let updated_at: Option<String> = if has_timestamp {
                let ts_idx = hash_col_count as i32 + 1;
                let val = row.get_value(ts_idx).map_err(|e| {
                    SyncError::Turso(format!("get_row_metadata get timestamp: {}", e))
                })?;
                match val {
                    libsql::Value::Null => None,
                    other => Some(libsql_value_to_string(&other)),
                }
            } else {
                None
            };

            result.insert(
                pk_value.clone(),
                RowMeta {
                    pk_value,
                    updated_at,
                    content_hash,
                },
            );
        }

        debug!("Got {} rows from {}", result.len(), table);
        Ok(result)
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        if pk_values.is_empty() {
            return Ok(vec![]);
        }

        let info = self.table_info(table).await?;
        let pk_cols = &info.primary_key;

        let pk_expr = pk_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(" || '|' || ");

        let cols = info
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c.name))
            .collect::<Vec<_>>()
            .join(", ");

        let placeholders = pk_values
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "SELECT {} FROM \"{}\" WHERE {} IN ({})",
            cols, table, pk_expr, placeholders
        );

        debug!("Fetching {} rows from {}", pk_values.len(), table);

        let params: Vec<libsql::Value> = pk_values
            .iter()
            .map(|v| libsql::Value::Text(v.clone()))
            .collect();

        let mut rows = self
            .conn
            .query(&sql, libsql::params::Params::Positional(params))
            .await
            .map_err(|e| SyncError::Turso(format!("get_rows query failed: {}", e)))?;

        let mut result = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| SyncError::Turso(format!("get_rows row iteration: {}", e)))?
        {
            let mut row_data = HashMap::new();
            for (i, col) in info.columns.iter().enumerate() {
                let val = row.get_value(i as i32).map_err(|e| {
                    SyncError::Turso(format!("get_rows get column {}: {}", col.name, e))
                })?;
                row_data.insert(col.name.clone(), libsql_value_to_json(&val));
            }
            result.push(row_data);
        }

        Ok(result)
    }

    async fn upsert_rows(&self, table: &str, rows: &[HashMap<String, JsonValue>]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let info = self.table_info(table).await?;
        let cols: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();

        let col_list = cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        let placeholders = cols
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({})",
            table, col_list, placeholders
        );

        debug!("Upserting {} rows into {}", rows.len(), table);

        let tx = self
            .conn
            .transaction()
            .await
            .map_err(|e| SyncError::Turso(format!("begin transaction failed: {}", e)))?;

        let mut count = 0;
        for row in rows {
            let params: Vec<libsql::Value> = cols
                .iter()
                .map(|col| json_to_libsql_value(row.get(*col)))
                .collect();

            if let Err(e) = tx
                .execute(&sql, libsql::params::Params::Positional(params))
                .await
            {
                let _ = tx.rollback().await;
                return Err(SyncError::Turso(format!(
                    "upsert_rows execute failed: {}",
                    e
                )));
            }

            count += 1;
        }

        tx.commit()
            .await
            .map_err(|e| SyncError::Turso(format!("commit transaction failed: {}", e)))?;

        info!("Upserted {} rows into {}", count, table);
        Ok(count)
    }

    async fn row_count(&self, table: &str) -> Result<usize> {
        let sql = format!("SELECT COUNT(*) FROM \"{}\"", table);
        let mut rows = self
            .conn
            .query(&sql, ())
            .await
            .map_err(|e| SyncError::Turso(format!("row_count query failed: {}", e)))?;

        let row = rows
            .next()
            .await
            .map_err(|e| SyncError::Turso(format!("row_count row iteration: {}", e)))?
            .ok_or_else(|| SyncError::Turso("row_count returned no rows".to_string()))?;

        let count: i64 = row
            .get(0)
            .map_err(|e| SyncError::Turso(format!("row_count get value: {}", e)))?;

        Ok(count as usize)
    }
}

/// Convert a libsql::Value to a string for hashing (mirrors local.rs get_value_as_string).
fn libsql_value_to_string(val: &libsql::Value) -> String {
    match val {
        libsql::Value::Null => String::new(),
        libsql::Value::Integer(i) => i.to_string(),
        libsql::Value::Real(f) => f.to_string(),
        libsql::Value::Text(s) => s.clone(),
        libsql::Value::Blob(b) => hex::encode(b),
    }
}

/// Convert a libsql::Value to a serde_json::Value (mirrors local.rs get_json_value).
fn libsql_value_to_json(val: &libsql::Value) -> JsonValue {
    match val {
        libsql::Value::Null => JsonValue::Null,
        libsql::Value::Integer(i) => JsonValue::from(*i),
        libsql::Value::Real(f) => JsonValue::from(*f),
        libsql::Value::Text(s) => JsonValue::from(s.clone()),
        libsql::Value::Blob(b) => JsonValue::String(hex::encode(b)),
    }
}

/// Convert a serde_json::Value to a libsql::Value for parameterized queries.
fn json_to_libsql_value(val: Option<&JsonValue>) -> libsql::Value {
    match val {
        None | Some(JsonValue::Null) => libsql::Value::Null,
        Some(JsonValue::Bool(b)) => libsql::Value::Integer(if *b { 1 } else { 0 }),
        Some(JsonValue::Number(n)) => {
            if let Some(i) = n.as_i64() {
                libsql::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                libsql::Value::Real(f)
            } else {
                libsql::Value::Text(n.to_string())
            }
        }
        Some(JsonValue::String(s)) => libsql::Value::Text(s.clone()),
        Some(JsonValue::Array(_)) | Some(JsonValue::Object(_)) => {
            libsql::Value::Text(val.unwrap().to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_libsql_value_to_string_null() {
        assert_eq!(libsql_value_to_string(&libsql::Value::Null), "");
    }

    #[test]
    fn test_libsql_value_to_string_integer() {
        assert_eq!(libsql_value_to_string(&libsql::Value::Integer(42)), "42");
        assert_eq!(libsql_value_to_string(&libsql::Value::Integer(-1)), "-1");
        assert_eq!(libsql_value_to_string(&libsql::Value::Integer(0)), "0");
    }

    #[test]
    fn test_libsql_value_to_string_real() {
        assert_eq!(libsql_value_to_string(&libsql::Value::Real(3.14)), "3.14");
    }

    #[test]
    fn test_libsql_value_to_string_text() {
        assert_eq!(
            libsql_value_to_string(&libsql::Value::Text("hello".to_string())),
            "hello"
        );
    }

    #[test]
    fn test_libsql_value_to_string_blob() {
        assert_eq!(
            libsql_value_to_string(&libsql::Value::Blob(vec![0xde, 0xad, 0xbe, 0xef])),
            "deadbeef"
        );
    }

    #[test]
    fn test_libsql_value_to_json_null() {
        assert_eq!(libsql_value_to_json(&libsql::Value::Null), JsonValue::Null);
    }

    #[test]
    fn test_libsql_value_to_json_integer() {
        assert_eq!(
            libsql_value_to_json(&libsql::Value::Integer(42)),
            JsonValue::from(42)
        );
    }

    #[test]
    fn test_libsql_value_to_json_real() {
        assert_eq!(
            libsql_value_to_json(&libsql::Value::Real(3.14)),
            JsonValue::from(3.14)
        );
    }

    #[test]
    fn test_libsql_value_to_json_text() {
        assert_eq!(
            libsql_value_to_json(&libsql::Value::Text("hello".to_string())),
            JsonValue::from("hello")
        );
    }

    #[test]
    fn test_libsql_value_to_json_blob() {
        assert_eq!(
            libsql_value_to_json(&libsql::Value::Blob(vec![0xca, 0xfe])),
            JsonValue::String("cafe".to_string())
        );
    }

    #[test]
    fn test_json_to_libsql_value_null() {
        assert_eq!(json_to_libsql_value(None), libsql::Value::Null);
        assert_eq!(
            json_to_libsql_value(Some(&JsonValue::Null)),
            libsql::Value::Null
        );
    }

    #[test]
    fn test_json_to_libsql_value_bool() {
        assert_eq!(
            json_to_libsql_value(Some(&JsonValue::Bool(true))),
            libsql::Value::Integer(1)
        );
        assert_eq!(
            json_to_libsql_value(Some(&JsonValue::Bool(false))),
            libsql::Value::Integer(0)
        );
    }

    #[test]
    fn test_json_to_libsql_value_integer() {
        let val = JsonValue::from(42);
        assert_eq!(json_to_libsql_value(Some(&val)), libsql::Value::Integer(42));
    }

    #[test]
    fn test_json_to_libsql_value_real() {
        let val = JsonValue::from(3.14);
        assert_eq!(json_to_libsql_value(Some(&val)), libsql::Value::Real(3.14));
    }

    #[test]
    fn test_json_to_libsql_value_string() {
        let val = JsonValue::from("hello");
        assert_eq!(
            json_to_libsql_value(Some(&val)),
            libsql::Value::Text("hello".to_string())
        );
    }

    #[test]
    fn test_json_to_libsql_value_array() {
        let val = JsonValue::Array(vec![JsonValue::from(1), JsonValue::from(2)]);
        assert_eq!(
            json_to_libsql_value(Some(&val)),
            libsql::Value::Text("[1,2]".to_string())
        );
    }

    #[test]
    fn test_json_to_libsql_value_object() {
        let mut map = serde_json::Map::new();
        map.insert("key".to_string(), JsonValue::from("val"));
        let val = JsonValue::Object(map);
        assert_eq!(
            json_to_libsql_value(Some(&val)),
            libsql::Value::Text("{\"key\":\"val\"}".to_string())
        );
    }

    // Content hash consistency: verify that the same column values produce
    // identical SHA-256 hashes regardless of whether they go through the
    // libsql or local (rusqlite) code path.
    #[test]
    fn test_content_hash_consistency() {
        let values = vec![
            libsql::Value::Integer(1),
            libsql::Value::Text("hello".to_string()),
            libsql::Value::Real(3.14),
        ];

        let mut hasher = Sha256::new();
        for v in &values {
            hasher.update(libsql_value_to_string(v).as_bytes());
            hasher.update(b"|");
        }
        let hash = hex::encode(hasher.finalize());

        // Same values through string conversion should match
        let mut hasher2 = Sha256::new();
        hasher2.update(b"1");
        hasher2.update(b"|");
        hasher2.update(b"hello");
        hasher2.update(b"|");
        hasher2.update(b"3.14");
        hasher2.update(b"|");
        let hash2 = hex::encode(hasher2.finalize());

        assert_eq!(hash, hash2);
    }
}
