//! smugglr WASM package -- sync engine for browser and Node.js.
//!
//! Provides push, pull, sync, and diff operations between HTTP SQL endpoints
//! using the browser fetch API. Same diff engine as the CLI, compiled to WASM.
//!
//! The WASM adapter bypasses smugglr-core's generic sync orchestration (which
//! requires Send futures for tokio) and drives the diff engine directly against
//! FetchDataSource. The diff algorithm, conflict resolution, and batch planning
//! are all from smugglr-core.
//!
//! Compilation gate: this crate requires `target_arch = "wasm32"`. FetchDataSource
//! uses `!Send` types (JsFuture, Rc, RefCell) which are incompatible with the
//! `Send` futures that smugglr-core's DataSource trait requires on multi-threaded
//! targets. Under `wasm32-unknown-unknown` the single-threaded target relaxes
//! `Send` bounds and the crate builds cleanly. See issue #93 for details.

#![cfg(target_arch = "wasm32")]

mod fetch_adapter;

use fetch_adapter::FetchDataSource;
use smugglr_core::config::{column_excluded, ConflictResolution, SyncConfig};
use smugglr_core::datasource::DataSource;
use smugglr_core::diff::diff_table;
use smugglr_core::profile::Profile;

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

#[derive(Deserialize)]
struct JsEndpointConfig {
    url: String,
    #[serde(default, alias = "authToken")]
    auth_token: String,
    #[serde(default = "default_profile")]
    profile: String,
}

fn default_profile() -> String {
    "generic".to_string()
}

#[derive(Deserialize)]
struct JsSmugglrConfig {
    source: JsEndpointConfig,
    dest: JsEndpointConfig,
    #[serde(default)]
    sync: JsSyncConfig,
}

#[derive(Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct JsSyncConfig {
    tables: Vec<String>,
    exclude_tables: Vec<String>,
    exclude_columns: Vec<String>,
    timestamp_column: Option<String>,
    conflict_resolution: Option<String>,
    batch_size: Option<usize>,
}

#[derive(Serialize)]
struct JsSyncResult {
    command: String,
    status: String,
    tables: Vec<JsTableResult>,
}

#[derive(Serialize)]
struct JsTableResult {
    name: String,
    #[serde(skip_serializing_if = "is_zero")]
    rows_pushed: usize,
    #[serde(skip_serializing_if = "is_zero")]
    rows_pulled: usize,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

#[derive(Serialize)]
struct JsDiffResult {
    command: String,
    status: String,
    tables: Vec<JsTableDiff>,
}

#[derive(Serialize)]
struct JsTableDiff {
    name: String,
    local_only: usize,
    remote_only: usize,
    local_newer: usize,
    remote_newer: usize,
    content_differs: usize,
    identical: usize,
}

fn parse_conflict_resolution(s: Option<&str>) -> ConflictResolution {
    match s {
        Some("remote_wins") => ConflictResolution::RemoteWins,
        Some("newer_wins") => ConflictResolution::NewerWins,
        Some("uuid_v7_wins") => ConflictResolution::UuidV7Wins,
        _ => ConflictResolution::LocalWins,
    }
}

fn build_sync_config(js: &JsSyncConfig) -> SyncConfig {
    let mut sync = SyncConfig::default();
    if !js.tables.is_empty() {
        sync.tables = js.tables.clone();
    }
    if !js.exclude_tables.is_empty() {
        sync.exclude_tables = js.exclude_tables.clone();
    }
    if !js.exclude_columns.is_empty() {
        sync.exclude_columns = js.exclude_columns.clone();
    }
    if let Some(ref ts) = js.timestamp_column {
        sync.timestamp_column = ts.clone();
    }
    sync.conflict_resolution = parse_conflict_resolution(js.conflict_resolution.as_deref());
    if let Some(bs) = js.batch_size {
        sync.batch_size = bs;
    }
    sync
}

fn build_datasource(endpoint: &JsEndpointConfig) -> Result<FetchDataSource, JsValue> {
    let profile = Profile::from_name(&endpoint.profile)
        .ok_or_else(|| JsValue::from_str(&format!("unknown profile: {}", endpoint.profile)))?;
    Ok(FetchDataSource::new(
        endpoint.url.clone(),
        endpoint.auth_token.clone(),
        profile,
    ))
}

/// Get tables to sync by finding the intersection of both sides,
/// filtering by config, and requiring a primary key.
async fn get_sync_tables(
    source: &FetchDataSource,
    dest: &FetchDataSource,
    sync_config: &SyncConfig,
) -> Result<Vec<String>, JsValue> {
    let source_tables: std::collections::HashSet<String> = source
        .list_tables()
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?
        .into_iter()
        .collect();
    let dest_tables: std::collections::HashSet<String> = dest
        .list_tables()
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?
        .into_iter()
        .collect();

    let mut tables = Vec::new();
    for t in source_tables.intersection(&dest_tables) {
        if !sync_config.tables.is_empty() && !sync_config.tables.contains(t) {
            continue;
        }
        if sync_config.exclude_tables.iter().any(|ex| ex == t) {
            continue;
        }
        let info = source
            .table_info(t)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        if !info.primary_key.is_empty() {
            tables.push(t.clone());
        }
    }
    tables.sort();
    Ok(tables)
}

/// Strip excluded columns from row data before transfer.
fn strip_excluded(
    rows: Vec<std::collections::HashMap<String, serde_json::Value>>,
    exclude: &[String],
) -> Vec<std::collections::HashMap<String, serde_json::Value>> {
    if exclude.is_empty() {
        return rows;
    }
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .filter(|(col, _)| !column_excluded(col, exclude))
                .collect()
        })
        .collect()
}

/// Transfer rows from source to dest in batches.
async fn transfer_rows(
    source: &FetchDataSource,
    dest: &FetchDataSource,
    table: &str,
    pk_values: &[String],
    batch_size: usize,
    exclude_columns: &[String],
) -> Result<usize, JsValue> {
    if pk_values.is_empty() {
        return Ok(0);
    }
    let rows = source
        .get_rows(table, pk_values)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let rows = strip_excluded(rows, exclude_columns);

    let mut total = 0;
    for chunk in rows.chunks(batch_size) {
        let count = dest
            .upsert_rows(table, chunk)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        total += count;
    }
    Ok(total)
}

#[wasm_bindgen]
pub struct Smugglr {
    sync_config: SyncConfig,
    source: FetchDataSource,
    dest: FetchDataSource,
}

#[wasm_bindgen]
impl Smugglr {
    /// Initialize smugglr with source and destination endpoints.
    #[wasm_bindgen]
    pub fn init(config_js: JsValue) -> Result<Smugglr, JsValue> {
        let js_config: JsSmugglrConfig = serde_wasm_bindgen::from_value(config_js)
            .map_err(|e| JsValue::from_str(&format!("invalid config: {}", e)))?;

        let sync_config = build_sync_config(&js_config.sync);
        let source = build_datasource(&js_config.source)?;
        let dest = build_datasource(&js_config.dest)?;

        Ok(Smugglr {
            sync_config,
            source,
            dest,
        })
    }

    /// Push source rows to destination.
    #[wasm_bindgen]
    pub async fn push(&self, dry_run: Option<bool>) -> Result<JsValue, JsValue> {
        let dry_run = dry_run.unwrap_or(false);
        let tables = get_sync_tables(&self.source, &self.dest, &self.sync_config).await?;
        let conflict = self.sync_config.conflict_resolution;
        let batch_size = self.sync_config.batch_size;

        let mut results = Vec::new();
        for table in &tables {
            let diff = diff_table(
                &self.source,
                &self.dest,
                table,
                &self.sync_config.timestamp_column,
                &self.sync_config.exclude_columns,
            )
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let to_push = diff.rows_to_push(conflict);
            let pushed = if dry_run || to_push.is_empty() {
                to_push.len()
            } else {
                transfer_rows(
                    &self.source,
                    &self.dest,
                    table,
                    &to_push,
                    batch_size,
                    &self.sync_config.exclude_columns,
                )
                .await?
            };

            results.push(JsTableResult {
                name: table.clone(),
                rows_pushed: pushed,
                rows_pulled: 0,
            });
        }

        let output = JsSyncResult {
            command: "push".into(),
            status: if dry_run { "dry_run" } else { "ok" }.into(),
            tables: results,
        };
        serde_wasm_bindgen::to_value(&output)
            .map_err(|e| JsValue::from_str(&format!("serialization failed: {}", e)))
    }

    /// Pull destination rows to source.
    #[wasm_bindgen]
    pub async fn pull(&self, dry_run: Option<bool>) -> Result<JsValue, JsValue> {
        let dry_run = dry_run.unwrap_or(false);
        let tables = get_sync_tables(&self.source, &self.dest, &self.sync_config).await?;
        let conflict = self.sync_config.conflict_resolution;
        let batch_size = self.sync_config.batch_size;

        let mut results = Vec::new();
        for table in &tables {
            let diff = diff_table(
                &self.source,
                &self.dest,
                table,
                &self.sync_config.timestamp_column,
                &self.sync_config.exclude_columns,
            )
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let to_pull = diff.rows_to_pull(conflict);
            let pulled = if dry_run || to_pull.is_empty() {
                to_pull.len()
            } else {
                transfer_rows(
                    &self.dest,
                    &self.source,
                    table,
                    &to_pull,
                    batch_size,
                    &self.sync_config.exclude_columns,
                )
                .await?
            };

            results.push(JsTableResult {
                name: table.clone(),
                rows_pushed: 0,
                rows_pulled: pulled,
            });
        }

        let output = JsSyncResult {
            command: "pull".into(),
            status: if dry_run { "dry_run" } else { "ok" }.into(),
            tables: results,
        };
        serde_wasm_bindgen::to_value(&output)
            .map_err(|e| JsValue::from_str(&format!("serialization failed: {}", e)))
    }

    /// Bidirectional sync.
    #[wasm_bindgen]
    pub async fn sync(&self, dry_run: Option<bool>) -> Result<JsValue, JsValue> {
        let dry_run = dry_run.unwrap_or(false);
        let tables = get_sync_tables(&self.source, &self.dest, &self.sync_config).await?;
        let conflict = self.sync_config.conflict_resolution;
        let batch_size = self.sync_config.batch_size;

        let mut results = Vec::new();
        for table in &tables {
            let diff = diff_table(
                &self.source,
                &self.dest,
                table,
                &self.sync_config.timestamp_column,
                &self.sync_config.exclude_columns,
            )
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let to_push = diff.rows_to_push(conflict);
            let to_pull = diff.rows_to_pull(conflict);

            let pushed = if dry_run || to_push.is_empty() {
                to_push.len()
            } else {
                transfer_rows(
                    &self.source,
                    &self.dest,
                    table,
                    &to_push,
                    batch_size,
                    &self.sync_config.exclude_columns,
                )
                .await?
            };

            let pulled = if dry_run || to_pull.is_empty() {
                to_pull.len()
            } else {
                transfer_rows(
                    &self.dest,
                    &self.source,
                    table,
                    &to_pull,
                    batch_size,
                    &self.sync_config.exclude_columns,
                )
                .await?
            };

            results.push(JsTableResult {
                name: table.clone(),
                rows_pushed: pushed,
                rows_pulled: pulled,
            });
        }

        let output = JsSyncResult {
            command: "sync".into(),
            status: if dry_run { "dry_run" } else { "ok" }.into(),
            tables: results,
        };
        serde_wasm_bindgen::to_value(&output)
            .map_err(|e| JsValue::from_str(&format!("serialization failed: {}", e)))
    }

    /// Read-only diff between source and destination.
    #[wasm_bindgen]
    pub async fn diff(&self) -> Result<JsValue, JsValue> {
        let tables = get_sync_tables(&self.source, &self.dest, &self.sync_config).await?;

        let mut table_diffs = Vec::new();
        for table in &tables {
            let diff = diff_table(
                &self.source,
                &self.dest,
                table,
                &self.sync_config.timestamp_column,
                &self.sync_config.exclude_columns,
            )
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

            let stats = diff.stats();
            table_diffs.push(JsTableDiff {
                name: table.clone(),
                local_only: stats.local_only,
                remote_only: stats.remote_only,
                local_newer: stats.local_newer,
                remote_newer: stats.remote_newer,
                content_differs: stats.content_differs,
                identical: stats.identical,
            });
        }

        let output = JsDiffResult {
            command: "diff".into(),
            status: "ok".into(),
            tables: table_diffs,
        };
        serde_wasm_bindgen::to_value(&output)
            .map_err(|e| JsValue::from_str(&format!("serialization failed: {}", e)))
    }
}
