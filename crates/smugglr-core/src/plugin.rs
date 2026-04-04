//! Runtime plugin adapter for the DataSource trait.
//!
//! Plugins are standalone binaries that implement the DataSource interface
//! via JSON-RPC over stdin/stdout. This enables adapter development in any
//! language without recompiling smuggler.
//!
//! ## Protocol
//!
//! Each request is a single JSON line on stdin:
//! ```json
//! {"jsonrpc":"2.0","method":"list_tables","params":{},"id":1}
//! ```
//!
//! Each response is a single JSON line on stdout:
//! ```json
//! {"jsonrpc":"2.0","result":["users","posts"],"id":1}
//! ```
//!
//! ## Methods
//!
//! - `initialize` - params: `{config: {key: value, ...}}`
//! - `list_tables` - params: `{}`
//! - `table_info` - params: `{table: string}`
//! - `get_row_metadata` - params: `{table, timestamp_column, exclude_columns}`
//! - `get_rows` - params: `{table, pk_values}`
//! - `upsert_rows` - params: `{table, rows}`
//! - `row_count` - params: `{table}`

use crate::datasource::{ColumnInfo, DataSource, RowMeta, TableInfo};
use crate::error::{Result, SyncError};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tracing::debug;

/// A DataSource backed by an external plugin process communicating via JSON-RPC.
pub struct PluginDataSource {
    io: Mutex<PluginIo>,
    plugin_name: String,
}

struct PluginIo {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
    read_buf: String,
}

#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    method: &'a str,
    params: JsonValue,
    id: u64,
}

#[derive(Deserialize)]
struct RpcResponse {
    #[allow(dead_code)]
    jsonrpc: String,
    result: Option<JsonValue>,
    error: Option<RpcError>,
    id: u64,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct WireTableInfo {
    name: String,
    columns: Vec<WireColumnInfo>,
    primary_key: Vec<String>,
}

#[derive(Deserialize)]
struct WireColumnInfo {
    name: String,
    #[serde(default)]
    col_type: String,
    #[serde(default)]
    notnull: bool,
    #[serde(default)]
    pk: bool,
}

#[derive(Deserialize)]
struct WireRowMeta {
    pk_value: String,
    updated_at: Option<String>,
    content_hash: String,
}

impl PluginDataSource {
    /// Spawn a plugin process and send the `initialize` handshake.
    pub async fn start(
        plugin_path: &Path,
        plugin_name: &str,
        config: &HashMap<String, String>,
    ) -> Result<Self> {
        let mut child = Command::new(plugin_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .map_err(|e| {
                SyncError::Plugin(format!(
                    "Failed to spawn plugin '{}' at {}: {}",
                    plugin_name,
                    plugin_path.display(),
                    e
                ))
            })?;

        let stdin = child.stdin.take().expect("stdin was piped");
        let stdout = child.stdout.take().expect("stdout was piped");

        let ds = Self {
            io: Mutex::new(PluginIo {
                child,
                stdin: BufWriter::new(stdin),
                stdout: BufReader::new(stdout),
                next_id: 0,
                read_buf: String::new(),
            }),
            plugin_name: plugin_name.to_string(),
        };

        let config_value = serde_json::to_value(config)
            .map_err(|e| SyncError::Plugin(format!("Failed to serialize plugin config: {}", e)))?;

        let _: JsonValue = ds
            .call("initialize", serde_json::json!({ "config": config_value }))
            .await?;

        Ok(ds)
    }

    /// Send a JSON-RPC request and read the response.
    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: JsonValue,
    ) -> Result<T> {
        let mut io = self.io.lock().await;

        let id = io.next_id;
        io.next_id += 1;

        let request = RpcRequest {
            jsonrpc: "2.0",
            method,
            params,
            id,
        };

        let mut line = serde_json::to_string(&request)
            .map_err(|e| SyncError::Plugin(format!("Failed to serialize request: {}", e)))?;
        line.push('\n');

        debug!("Plugin {} request: {} id={}", self.plugin_name, method, id);

        io.stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| SyncError::Plugin(format!("Failed to write to plugin: {}", e)))?;
        io.stdin
            .flush()
            .await
            .map_err(|e| SyncError::Plugin(format!("Failed to flush plugin stdin: {}", e)))?;

        let PluginIo {
            ref mut stdout,
            ref mut read_buf,
            ..
        } = *io;
        read_buf.clear();
        stdout
            .read_line(read_buf)
            .await
            .map_err(|e| SyncError::Plugin(format!("Failed to read from plugin: {}", e)))?;

        if read_buf.is_empty() {
            return Err(SyncError::Plugin(format!(
                "Plugin '{}' closed stdout unexpectedly",
                self.plugin_name
            )));
        }

        let response: RpcResponse = serde_json::from_str(read_buf).map_err(|e| {
            SyncError::Plugin(format!(
                "Invalid JSON-RPC response from plugin '{}': {}\nResponse: {}",
                self.plugin_name,
                e,
                read_buf.trim()
            ))
        })?;

        if response.id != id {
            return Err(SyncError::Plugin(format!(
                "Response ID mismatch from plugin '{}': expected {}, got {}",
                self.plugin_name, id, response.id
            )));
        }

        if let Some(err) = response.error {
            return Err(SyncError::Plugin(format!(
                "Plugin '{}' error (code {}): {}",
                self.plugin_name, err.code, err.message
            )));
        }

        let result = response.result.ok_or_else(|| {
            SyncError::Plugin(format!(
                "Plugin '{}' returned neither result nor error",
                self.plugin_name
            ))
        })?;

        serde_json::from_value(result)
            .map_err(|e| SyncError::Plugin(format!("Failed to deserialize plugin response: {}", e)))
    }
}

impl Drop for PluginDataSource {
    fn drop(&mut self) {
        let io = self.io.get_mut();
        let _ = io.child.start_kill();
    }
}

impl DataSource for PluginDataSource {
    async fn list_tables(&self) -> Result<Vec<String>> {
        self.call("list_tables", serde_json::json!({})).await
    }

    async fn table_info(&self, table: &str) -> Result<TableInfo> {
        let wire: WireTableInfo = self
            .call("table_info", serde_json::json!({ "table": table }))
            .await?;
        Ok(TableInfo {
            name: wire.name,
            columns: wire
                .columns
                .into_iter()
                .map(|c| ColumnInfo {
                    name: c.name,
                    col_type: c.col_type,
                    notnull: c.notnull,
                    pk: c.pk,
                })
                .collect(),
            primary_key: wire.primary_key,
        })
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> Result<HashMap<String, RowMeta>> {
        let wire: HashMap<String, WireRowMeta> = self
            .call(
                "get_row_metadata",
                serde_json::json!({
                    "table": table,
                    "timestamp_column": timestamp_column,
                    "exclude_columns": exclude_columns,
                }),
            )
            .await?;
        Ok(wire
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    RowMeta {
                        pk_value: v.pk_value,
                        updated_at: v.updated_at,
                        content_hash: v.content_hash,
                    },
                )
            })
            .collect())
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        self.call(
            "get_rows",
            serde_json::json!({
                "table": table,
                "pk_values": pk_values,
            }),
        )
        .await
    }

    async fn upsert_rows(&self, table: &str, rows: &[HashMap<String, JsonValue>]) -> Result<usize> {
        self.call(
            "upsert_rows",
            serde_json::json!({
                "table": table,
                "rows": rows,
            }),
        )
        .await
    }

    async fn row_count(&self, table: &str) -> Result<usize> {
        self.call("row_count", serde_json::json!({ "table": table }))
            .await
    }
}

/// Resolve a plugin name to its binary path.
///
/// Search order:
/// 1. `~/.smugglr/plugins/smuggler-{name}`
/// 2. `smuggler-{name}` on `$PATH`
pub fn resolve_plugin_path(name: &str) -> Result<PathBuf> {
    let binary_name = format!("smugglr-{}", name);

    // Check ~/.smugglr/plugins/
    if let Ok(home) = std::env::var("HOME") {
        let candidate = PathBuf::from(home)
            .join(".smuggler")
            .join("plugins")
            .join(&binary_name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    // Check $PATH
    if let Some(path) = find_in_path(&binary_name) {
        return Ok(path);
    }

    Err(SyncError::Plugin(format!(
        "Plugin '{}' not found. Searched: ~/.smugglr/plugins/{}, $PATH/{}",
        name, binary_name, binary_name
    )))
}

fn find_in_path(name: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths).find_map(|dir| {
            let candidate = dir.join(name);
            if candidate.is_file() {
                Some(candidate)
            } else {
                None
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_plugin_not_found() {
        let result = resolve_plugin_path("nonexistent-plugin-abc123");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SyncError::Plugin(_)));
        assert!(err.to_string().contains("nonexistent-plugin-abc123"));
    }

    #[test]
    fn test_find_in_path_nonexistent() {
        assert!(find_in_path("smugglr-totally-fake-binary").is_none());
    }

    #[test]
    fn test_rpc_request_serialization() {
        let req = RpcRequest {
            jsonrpc: "2.0",
            method: "list_tables",
            params: serde_json::json!({}),
            id: 42,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert!(json.contains("\"method\":\"list_tables\""));
        assert!(json.contains("\"id\":42"));
    }

    #[test]
    fn test_rpc_response_deserialization() {
        let json = r#"{"jsonrpc":"2.0","result":["users","posts"],"id":1}"#;
        let resp: RpcResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, 1);
        assert!(resp.error.is_none());
        let tables: Vec<String> = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert_eq!(tables, vec!["users", "posts"]);
    }

    #[test]
    fn test_rpc_error_response_deserialization() {
        let json =
            r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"table not found"},"id":2}"#;
        let resp: RpcResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, 2);
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32000);
        assert_eq!(err.message, "table not found");
    }

    #[test]
    fn test_wire_table_info_deserialization() {
        let json = r#"{
            "name": "users",
            "columns": [
                {"name": "id", "col_type": "INTEGER", "notnull": true, "pk": true},
                {"name": "email", "col_type": "TEXT"}
            ],
            "primary_key": ["id"]
        }"#;
        let info: WireTableInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.name, "users");
        assert_eq!(info.columns.len(), 2);
        assert!(info.columns[0].pk);
        assert!(!info.columns[1].pk);
        assert_eq!(info.columns[1].col_type, "TEXT");
    }

    #[test]
    fn test_wire_row_meta_deserialization() {
        let json = r#"{
            "pk_value": "42",
            "updated_at": "2026-04-03T12:00:00Z",
            "content_hash": "abc123"
        }"#;
        let meta: WireRowMeta = serde_json::from_str(json).unwrap();
        assert_eq!(meta.pk_value, "42");
        assert_eq!(meta.updated_at.unwrap(), "2026-04-03T12:00:00Z");
        assert_eq!(meta.content_hash, "abc123");
    }
}
