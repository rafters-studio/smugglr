//! SDK for building smuggler adapter plugins.
//!
//! Implement [`PluginAdapter`] and call [`run`] to get a working plugin binary
//! that communicates with smuggler via JSON-RPC over stdin/stdout.
//!
//! # Example
//!
//! ```rust,no_run
//! use smugglr_plugin_sdk::{PluginAdapter, PluginError, run};
//! use smugglr_plugin_sdk::{TableInfo, ColumnInfo, RowMeta};
//! use serde_json::Value;
//! use std::collections::HashMap;
//!
//! struct MyAdapter;
//!
//! impl PluginAdapter for MyAdapter {
//!     // implement all methods...
//! #   async fn initialize(&mut self, _config: HashMap<String, String>) -> Result<(), PluginError> { Ok(()) }
//! #   async fn list_tables(&self) -> Result<Vec<String>, PluginError> { Ok(vec![]) }
//! #   async fn table_info(&self, _table: &str) -> Result<TableInfo, PluginError> { todo!() }
//! #   async fn get_row_metadata(&self, _table: &str, _ts: &str, _exc: &[String]) -> Result<HashMap<String, RowMeta>, PluginError> { Ok(HashMap::new()) }
//! #   async fn get_rows(&self, _table: &str, _pks: &[String]) -> Result<Vec<HashMap<String, Value>>, PluginError> { Ok(vec![]) }
//! #   async fn upsert_rows(&self, _table: &str, _rows: &[HashMap<String, Value>]) -> Result<usize, PluginError> { Ok(0) }
//! #   async fn row_count(&self, _table: &str) -> Result<usize, PluginError> { Ok(0) }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     run(MyAdapter).await;
//! }
//! ```

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

// -- Public types --

/// Table schema information, row metadata, and column info.
///
/// Re-exported from `smugglr-wire`, the canonical definition shared with
/// `smugglr-core`'s host-side plugin adapter -- see #228. Plugin authors keep
/// importing these from `smugglr_plugin_sdk` as before.
pub use smugglr_wire::{ColumnInfo, RowMeta, TableInfo};

/// Well-known JSON-RPC error code that a plugin uses to signal a *transient*
/// failure (rate limit, 5xx, timeout against a remote backend) that smugglr
/// should treat as retryable, mirroring the native `is_retryable` retry path.
///
/// Codes are conveyed on the wire in the JSON-RPC `error.code` field. The host
/// honors this specific code by routing the error onto the retry/backoff path
/// instead of failing permanently. Plugin authors construct such errors with
/// [`PluginError::transient`].
///
/// This sits in the JSON-RPC "server error" reserved range (-32099..=-32000)
/// and is distinct from the default plugin error code (-32000) so the host can
/// distinguish "retry me" from "this is fatal".
pub const TRANSIENT_ERROR_CODE: i64 = -32010;

#[derive(Debug)]
pub struct PluginError {
    pub message: String,
    pub code: i64,
}

impl PluginError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: -32000,
        }
    }

    pub fn with_code(message: impl Into<String>, code: i64) -> Self {
        Self {
            message: message.into(),
            code,
        }
    }

    /// Construct an error the host should treat as transient and retry.
    ///
    /// Use this when the underlying backend reports a recoverable failure
    /// (HTTP 429/5xx, connection timeout, etc.). The error carries
    /// [`TRANSIENT_ERROR_CODE`] on the JSON-RPC wire.
    pub fn transient(message: impl Into<String>) -> Self {
        Self::with_code(message, TRANSIENT_ERROR_CODE)
    }

    /// Whether this error is tagged as transient/retryable via its code.
    pub fn is_transient(&self) -> bool {
        self.code == TRANSIENT_ERROR_CODE
    }
}

impl std::fmt::Display for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PluginError {}

impl From<String> for PluginError {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for PluginError {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

// -- Adapter trait --

/// Trait that plugin authors implement to create a smuggler adapter.
///
/// Each method corresponds to a DataSource operation. The SDK handles
/// JSON-RPC protocol details -- you just implement the database logic.
pub trait PluginAdapter: Send + Sync {
    fn initialize(
        &mut self,
        config: HashMap<String, String>,
    ) -> impl std::future::Future<Output = Result<(), PluginError>> + Send;

    fn list_tables(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<String>, PluginError>> + Send;

    fn table_info(
        &self,
        table: &str,
    ) -> impl std::future::Future<Output = Result<TableInfo, PluginError>> + Send;

    fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> impl std::future::Future<Output = Result<HashMap<String, RowMeta>, PluginError>> + Send;

    fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> impl std::future::Future<Output = Result<Vec<HashMap<String, Value>>, PluginError>> + Send;

    fn upsert_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, Value>],
    ) -> impl std::future::Future<Output = Result<usize, PluginError>> + Send;

    fn row_count(
        &self,
        table: &str,
    ) -> impl std::future::Future<Output = Result<usize, PluginError>> + Send;
}

// -- JSON-RPC protocol types --

#[derive(Deserialize)]
struct RpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    method: String,
    params: Value,
    id: u64,
}

#[derive(Serialize)]
struct RpcResponse {
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcErrorObj>,
    id: u64,
}

#[derive(Serialize)]
struct RpcErrorObj {
    code: i64,
    message: String,
}

impl RpcResponse {
    fn ok(id: u64, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            result: Some(result),
            error: None,
            id,
        }
    }

    fn err(id: u64, code: i64, message: String) -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcErrorObj { code, message }),
            id,
        }
    }
}

// -- Param extraction helpers --

fn param_str(params: &Value, key: &str) -> Result<String, PluginError> {
    params
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| PluginError::with_code(format!("missing param: {}", key), -32602))
}

fn param_str_slice(params: &Value, key: &str) -> Result<Vec<String>, PluginError> {
    let arr = params
        .get(key)
        .and_then(|v| v.as_array())
        .ok_or_else(|| PluginError::with_code(format!("missing param: {}", key), -32602))?;

    // Error loudly on non-string elements rather than silently dropping them:
    // a numeric pk_value (e.g. `[1, 2, 3]`) would otherwise shorten the list
    // and cause silent data divergence in get_rows.
    arr.iter()
        .enumerate()
        .map(|(idx, v)| {
            v.as_str().map(String::from).ok_or_else(|| {
                PluginError::with_code(
                    format!(
                        "invalid param: {}[{}]: expected string, got {}",
                        key, idx, v
                    ),
                    -32602,
                )
            })
        })
        .collect()
}

/// Take a JSON-RPC param by key and deserialize it into `T`, leaving `Value::Null`
/// behind in `params` in its place (see [`Value::take`]).
///
/// Distinguishes "key absent" (missing param) from "key present but the wrong
/// shape" (invalid param) so callers get an accurate error message instead of
/// a misleading "missing" for malformed-but-present data.
fn param_de<T: DeserializeOwned>(params: &mut Value, key: &str) -> Result<T, PluginError> {
    match params.get_mut(key).map(Value::take) {
        None => Err(PluginError::with_code(
            format!("missing param: {}", key),
            -32602,
        )),
        Some(v) => serde_json::from_value(v)
            .map_err(|e| PluginError::with_code(format!("invalid param: {}: {}", key, e), -32602)),
    }
}

// -- Dispatch --

async fn dispatch(
    adapter: &mut impl PluginAdapter,
    req: &mut RpcRequest,
) -> Result<Value, PluginError> {
    match req.method.as_str() {
        "initialize" => {
            let config: HashMap<String, String> = param_de(&mut req.params, "config")?;
            adapter.initialize(config).await?;
            Ok(Value::Bool(true))
        }
        "list_tables" => {
            let tables = adapter.list_tables().await?;
            Ok(serde_json::to_value(tables).unwrap())
        }
        "table_info" => {
            let table = param_str(&req.params, "table")?;
            let info = adapter.table_info(&table).await?;
            Ok(serde_json::to_value(info).unwrap())
        }
        "get_row_metadata" => {
            let table = param_str(&req.params, "table")?;
            let ts_col = param_str(&req.params, "timestamp_column")?;
            let exclude = param_str_slice(&req.params, "exclude_columns")?;
            let meta = adapter.get_row_metadata(&table, &ts_col, &exclude).await?;
            Ok(serde_json::to_value(meta).unwrap())
        }
        "get_rows" => {
            let table = param_str(&req.params, "table")?;
            let pk_values = param_str_slice(&req.params, "pk_values")?;
            let rows = adapter.get_rows(&table, &pk_values).await?;
            Ok(serde_json::to_value(rows).unwrap())
        }
        "upsert_rows" => {
            let table = param_str(&req.params, "table")?;
            let rows: Vec<HashMap<String, Value>> = param_de(&mut req.params, "rows")?;
            let count = adapter.upsert_rows(&table, &rows).await?;
            Ok(serde_json::to_value(count).unwrap())
        }
        "row_count" => {
            let table = param_str(&req.params, "table")?;
            let count = adapter.row_count(&table).await?;
            Ok(serde_json::to_value(count).unwrap())
        }
        _ => Err(PluginError::with_code(
            format!("unknown method: {}", req.method),
            -32601,
        )),
    }
}

// -- Entry point --

/// Run the plugin server loop. Reads JSON-RPC from stdin, dispatches
/// to the adapter, writes responses to stdout. Runs until stdin closes.
pub async fn run(mut adapter: impl PluginAdapter) {
    let stdin = BufReader::new(tokio::io::stdin());
    let mut stdout = BufWriter::new(tokio::io::stdout());
    let mut lines = stdin.lines();

    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(_) => break,
        };

        let mut req: RpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                // Recover the request id with a lenient pre-parse so the host's
                // strict `response.id == request.id` check still matches and the
                // real -32700 parse error surfaces instead of an ID mismatch.
                let id = recover_id(&line);
                let resp = RpcResponse::err(id, -32700, format!("parse error: {}", e));
                write_response(&mut stdout, &resp).await;
                continue;
            }
        };

        let id = req.id;
        let resp = match dispatch(&mut adapter, &mut req).await {
            Ok(result) => RpcResponse::ok(id, result),
            Err(e) => RpcResponse::err(id, e.code, e.message),
        };

        write_response(&mut stdout, &resp).await;
    }
}

/// Best-effort extraction of the JSON-RPC request id from a line that failed to
/// parse as a full [`RpcRequest`]. Returns the recovered id, or 0 if the id
/// cannot be recovered (the id is fundamentally unknowable when the line is not
/// valid JSON at all).
fn recover_id(line: &str) -> u64 {
    serde_json::from_str::<Value>(line)
        .ok()
        .and_then(|v| v.get("id").and_then(Value::as_u64))
        .unwrap_or(0)
}

async fn write_response(stdout: &mut BufWriter<tokio::io::Stdout>, resp: &RpcResponse) {
    // Serialization of RpcResponse cannot fail (all fields are serializable)
    let mut line = serde_json::to_string(resp).expect("RpcResponse is always serializable");
    line.push('\n');
    let _ = stdout.write_all(line.as_bytes()).await;
    let _ = stdout.flush().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_response_ok_serialization() {
        let resp = RpcResponse::ok(1, Value::Bool(true));
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"result\":true"));
        assert!(!json.contains("\"error\""));
    }

    #[test]
    fn test_rpc_response_err_serialization() {
        let resp = RpcResponse::err(2, -32000, "bad".into());
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"error\""));
        assert!(!json.contains("\"result\""));
        assert!(json.contains("-32000"));
    }

    #[test]
    fn test_param_str_extraction() {
        let params = serde_json::json!({"table": "users"});
        assert_eq!(param_str(&params, "table").unwrap(), "users");
        assert!(param_str(&params, "missing").is_err());
    }

    #[test]
    fn test_param_str_slice_extraction() {
        let params = serde_json::json!({"pk_values": ["1", "2", "3"]});
        let vals = param_str_slice(&params, "pk_values").unwrap();
        assert_eq!(vals, vec!["1", "2", "3"]);
    }

    // Regression for #204: a non-string array element (e.g. numeric pk) must
    // error loudly with -32602 rather than being silently dropped, which would
    // shorten the pk_value list and cause silent data divergence in get_rows.
    #[test]
    fn test_param_str_slice_rejects_non_string_elements() {
        let params = serde_json::json!({"pk_values": ["1", 2, "3"]});
        let err = param_str_slice(&params, "pk_values").unwrap_err();
        assert_eq!(err.code, -32602);
        assert!(err.message.contains("pk_values[1]"), "{}", err.message);
        // Pre-fix behavior would have returned ["1", "3"] silently.
        assert!(param_str_slice(&params, "pk_values").is_err());
    }

    #[test]
    fn test_param_str_slice_missing_vs_invalid() {
        // Absent key is still a "missing param" error.
        let absent = serde_json::json!({});
        let err = param_str_slice(&absent, "pk_values").unwrap_err();
        assert!(err.message.contains("missing param"), "{}", err.message);
    }

    // Regression for #205: a present-but-malformed `rows` param must report an
    // "invalid param" decode error, not the misleading "missing param: rows".
    #[test]
    fn test_param_rows_malformed_reports_invalid_not_missing() {
        // rows present but an object instead of an array.
        let mut params = serde_json::json!({"rows": {"id": 1}});
        let err = param_de::<Vec<HashMap<String, Value>>>(&mut params, "rows").unwrap_err();
        assert_eq!(err.code, -32602);
        assert!(
            err.message.contains("invalid param: rows"),
            "{}",
            err.message
        );
        assert!(!err.message.contains("missing"), "{}", err.message);
    }

    #[test]
    fn test_param_rows_missing_still_reports_missing() {
        let mut params = serde_json::json!({});
        let err = param_de::<Vec<HashMap<String, Value>>>(&mut params, "rows").unwrap_err();
        assert!(
            err.message.contains("missing param: rows"),
            "{}",
            err.message
        );
    }

    // Regression for #205: a present-but-malformed `config` param must report an
    // "invalid param" decode error, not the misleading "missing param: config".
    #[test]
    fn test_param_config_malformed_reports_invalid_not_missing() {
        // config present but a string instead of a map.
        let mut params = serde_json::json!({"config": "not-a-map"});
        let err = param_de::<HashMap<String, String>>(&mut params, "config").unwrap_err();
        assert_eq!(err.code, -32602);
        assert!(
            err.message.contains("invalid param: config"),
            "{}",
            err.message
        );
        assert!(!err.message.contains("missing"), "{}", err.message);
    }

    // Regression for #206: a parse-failed line carrying a recoverable id must
    // echo that id, not a hardcoded 0, so the host's id-match check passes and
    // the real -32700 parse error surfaces instead of an "ID mismatch".
    #[test]
    fn test_recover_id_from_unparseable_request() {
        // Valid JSON object with an id, but missing required fields (no method),
        // so it fails RpcRequest deserialization yet still yields its id.
        let line = r#"{"jsonrpc":"2.0","id":7}"#;
        assert!(serde_json::from_str::<RpcRequest>(line).is_err());
        assert_eq!(recover_id(line), 7);
    }

    #[test]
    fn test_recover_id_falls_back_to_zero_on_garbage() {
        // Not valid JSON at all -- the id is unknowable, so fall back to 0.
        assert_eq!(recover_id("this is not json"), 0);
        // Valid JSON but no id field.
        assert_eq!(recover_id(r#"{"method":"x"}"#), 0);
    }

    // Regression for #203: the SDK exposes a well-known transient error code so
    // plugins can signal retryable failures to the host. The code must serialize
    // onto the JSON-RPC wire so the host can honor it.
    #[test]
    fn test_transient_error_code() {
        let err = PluginError::transient("rate limited");
        assert_eq!(err.code, TRANSIENT_ERROR_CODE);
        assert!(err.is_transient());
        assert!(!PluginError::new("oops").is_transient());

        let resp = RpcResponse::err(1, err.code, err.message);
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(&TRANSIENT_ERROR_CODE.to_string()));
    }

    #[test]
    fn test_param_config_extraction() {
        let mut params = serde_json::json!({"config": {"url": "http://localhost", "token": "abc"}});
        let config = param_de::<HashMap<String, String>>(&mut params, "config").unwrap();
        assert_eq!(config.get("url").unwrap(), "http://localhost");
        assert_eq!(config.get("token").unwrap(), "abc");
    }

    #[test]
    fn test_param_rows_extraction() {
        let mut params = serde_json::json!({"rows": [{"id": 1, "name": "alice"}]});
        let rows = param_de::<Vec<HashMap<String, Value>>>(&mut params, "rows").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name").unwrap(), "alice");
    }

    #[test]
    fn test_plugin_error_from_str() {
        let err: PluginError = "something broke".into();
        assert_eq!(err.message, "something broke");
        assert_eq!(err.code, -32000);
    }

    #[test]
    fn test_plugin_error_with_code() {
        let err = PluginError::with_code("not found", -32601);
        assert_eq!(err.code, -32601);
    }

    #[test]
    fn test_table_info_roundtrip() {
        let info = TableInfo {
            name: "users".into(),
            columns: vec![
                ColumnInfo {
                    name: "id".into(),
                    col_type: "INTEGER".into(),
                    notnull: true,
                    pk: true,
                },
                ColumnInfo {
                    name: "email".into(),
                    col_type: "TEXT".into(),
                    notnull: false,
                    pk: false,
                },
            ],
            primary_key: vec!["id".into()],
        };
        let json = serde_json::to_string(&info).unwrap();
        let parsed: TableInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "users");
        assert_eq!(parsed.columns.len(), 2);
        assert_eq!(parsed.primary_key, vec!["id"]);
    }

    #[test]
    fn test_row_meta_roundtrip() {
        let meta = RowMeta {
            pk_value: "42".into(),
            updated_at: Some("2026-04-03T12:00:00Z".into()),
            content_hash: "abc123".into(),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let parsed: RowMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.pk_value, "42");
        assert_eq!(parsed.content_hash, "abc123");
    }

    // Integration test: dispatch routes correctly
    struct NoopAdapter;

    impl PluginAdapter for NoopAdapter {
        async fn initialize(
            &mut self,
            _config: HashMap<String, String>,
        ) -> Result<(), PluginError> {
            Ok(())
        }
        async fn list_tables(&self) -> Result<Vec<String>, PluginError> {
            Ok(vec!["test_table".into()])
        }
        async fn table_info(&self, table: &str) -> Result<TableInfo, PluginError> {
            Ok(TableInfo {
                name: table.to_string(),
                columns: vec![],
                primary_key: vec![],
            })
        }
        async fn get_row_metadata(
            &self,
            _table: &str,
            _ts: &str,
            _exc: &[String],
        ) -> Result<HashMap<String, RowMeta>, PluginError> {
            Ok(HashMap::new())
        }
        async fn get_rows(
            &self,
            _table: &str,
            _pks: &[String],
        ) -> Result<Vec<HashMap<String, Value>>, PluginError> {
            Ok(vec![])
        }
        async fn upsert_rows(
            &self,
            _table: &str,
            _rows: &[HashMap<String, Value>],
        ) -> Result<usize, PluginError> {
            Ok(0)
        }
        async fn row_count(&self, _table: &str) -> Result<usize, PluginError> {
            Ok(42)
        }
    }

    #[tokio::test]
    async fn test_dispatch_list_tables() {
        let mut adapter = NoopAdapter;
        let mut req = RpcRequest {
            jsonrpc: "2.0".into(),
            method: "list_tables".into(),
            params: serde_json::json!({}),
            id: 1,
        };
        let result = dispatch(&mut adapter, &mut req).await.unwrap();
        let tables: Vec<String> = serde_json::from_value(result).unwrap();
        assert_eq!(tables, vec!["test_table"]);
    }

    #[tokio::test]
    async fn test_dispatch_row_count() {
        let mut adapter = NoopAdapter;
        let mut req = RpcRequest {
            jsonrpc: "2.0".into(),
            method: "row_count".into(),
            params: serde_json::json!({"table": "users"}),
            id: 2,
        };
        let result = dispatch(&mut adapter, &mut req).await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_dispatch_unknown_method() {
        let mut adapter = NoopAdapter;
        let mut req = RpcRequest {
            jsonrpc: "2.0".into(),
            method: "nonexistent".into(),
            params: serde_json::json!({}),
            id: 3,
        };
        let err = dispatch(&mut adapter, &mut req).await.unwrap_err();
        assert_eq!(err.code, -32601);
        assert!(err.message.contains("nonexistent"));
    }

    #[tokio::test]
    async fn test_dispatch_missing_param() {
        let mut adapter = NoopAdapter;
        let mut req = RpcRequest {
            jsonrpc: "2.0".into(),
            method: "table_info".into(),
            params: serde_json::json!({}),
            id: 4,
        };
        let err = dispatch(&mut adapter, &mut req).await.unwrap_err();
        assert_eq!(err.code, -32602);
    }

    #[tokio::test]
    async fn test_dispatch_initialize() {
        let mut adapter = NoopAdapter;
        let mut req = RpcRequest {
            jsonrpc: "2.0".into(),
            method: "initialize".into(),
            params: serde_json::json!({"config": {"key": "value"}}),
            id: 5,
        };
        let result = dispatch(&mut adapter, &mut req).await.unwrap();
        assert_eq!(result, Value::Bool(true));
    }
}
