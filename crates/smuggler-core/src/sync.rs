//! Sync orchestration
//!
//! All sync functions are generic over the [`DataSource`] trait, enabling
//! any pair of data sources (local<->local, local<->D1, local<->S3, etc.)
//! to sync using the same diff-and-apply engine.

use crate::config::{column_excluded, BatchConfig, Config, ConflictResolution};
use crate::datasource::DataSource;
use crate::diff::{diff_table, DiffStats, TableDiff};
use crate::error::Result;

/// Per-table primary key lists from the diff, for verbose dry-run output.
#[derive(Debug, Clone)]
pub struct DiffDetail {
    pub local_only: Vec<String>,
    pub remote_only: Vec<String>,
    pub local_newer: Vec<String>,
    pub remote_newer: Vec<String>,
    pub content_differs: Vec<String>,
}

impl DiffDetail {
    pub fn from_diff(diff: &TableDiff) -> Self {
        Self {
            local_only: diff.local_only.clone(),
            remote_only: diff.remote_only.clone(),
            local_newer: diff.local_newer.clone(),
            remote_newer: diff.remote_newer.clone(),
            content_differs: diff.content_differs.clone(),
        }
    }
}
use std::collections::HashMap;
use tracing::{info, warn};

/// Trait for reporting sync progress to the UI layer.
///
/// The core library uses this trait instead of depending on indicatif directly,
/// allowing CLI and non-CLI consumers to provide their own progress rendering.
pub trait SyncProgress: Send + Sync {
    /// Called when a row transfer begins.
    fn on_transfer_start(&self, total_rows: usize, label: &str, table: &str);
    /// Called after each batch of rows is transferred.
    fn on_batch_complete(&self, rows_in_batch: usize);
    /// Called when a row transfer finishes.
    fn on_transfer_finish(&self, total_rows: usize, label: &str);
}

/// No-op progress reporter for headless or library usage.
pub struct NoProgress;

impl SyncProgress for NoProgress {
    fn on_transfer_start(&self, _: usize, _: &str, _: &str) {}
    fn on_batch_complete(&self, _: usize) {}
    fn on_transfer_finish(&self, _: usize, _: &str) {}
}

/// Result of a sync operation
#[derive(Debug)]
pub struct SyncResult {
    pub table: String,
    pub rows_pushed: usize,
    pub rows_pulled: usize,
    /// Per-table diff breakdown, populated when diff was computed.
    pub diff_stats: Option<DiffStats>,
    /// Per-table PK lists from diff, populated for verbose dry-run output.
    pub diff_detail: Option<DiffDetail>,
}

impl SyncResult {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            rows_pushed: 0,
            rows_pulled: 0,
            diff_stats: None,
            diff_detail: None,
        }
    }

    pub fn has_changes(&self) -> bool {
        self.rows_pushed > 0 || self.rows_pulled > 0
    }
}

/// Strip excluded columns from row data before transfer.
///
/// Returns rows unchanged if `exclude_columns` is empty (fast path).
fn strip_excluded_columns(
    rows: Vec<HashMap<String, serde_json::Value>>,
    exclude_columns: &[String],
) -> Vec<HashMap<String, serde_json::Value>> {
    if exclude_columns.is_empty() {
        return rows;
    }

    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .filter(|(col, _)| !column_excluded(col, exclude_columns))
                .collect()
        })
        .collect()
}

/// Transfer rows from one DataSource to another.
///
/// Fetches rows by primary key from `source`, then upserts into `dest`
/// in chunks (sized by `batch_config.batch_size`) with progress reporting
/// via the provided [`SyncProgress`] implementation.
/// Excluded columns are stripped before upserting.
#[allow(clippy::too_many_arguments)]
async fn transfer_rows<Src: DataSource, Dst: DataSource>(
    source: &Src,
    dest: &Dst,
    table: &str,
    pk_values: &[String],
    batch_config: &BatchConfig,
    exclude_columns: &[String],
    label: &str,
    progress: &dyn SyncProgress,
) -> Result<usize> {
    let rows = source.get_rows(table, pk_values).await?;

    if rows.is_empty() {
        warn!("No rows found in source for {}", label);
        return Ok(0);
    }

    let rows = strip_excluded_columns(rows, exclude_columns);

    progress.on_transfer_start(rows.len(), label, table);
    let mut total = 0;

    for chunk in rows.chunks(batch_config.batch_size) {
        let count = dest.upsert_rows(table, chunk).await?;
        total += count;
        progress.on_batch_complete(chunk.len());
    }

    progress.on_transfer_finish(total, label);
    Ok(total)
}

/// Push changes from source to destination for a single table.
#[allow(clippy::too_many_arguments)]
pub async fn push_table<Src: DataSource, Dst: DataSource>(
    source: &Src,
    dest: &Dst,
    table: &str,
    diff: &TableDiff,
    conflict_resolution: ConflictResolution,
    batch_config: &BatchConfig,
    exclude_columns: &[String],
    dry_run: bool,
    progress: &dyn SyncProgress,
) -> Result<SyncResult> {
    let mut result = SyncResult::new(table);
    let rows_to_push = diff.rows_to_push(conflict_resolution);

    if rows_to_push.is_empty() {
        info!("No changes to push for table: {}", table);
        return Ok(result);
    }

    info!(
        "Pushing {} rows to table: {} (dry_run={})",
        rows_to_push.len(),
        table,
        dry_run
    );

    if dry_run {
        result.rows_pushed = rows_to_push.len();
        return Ok(result);
    }

    result.rows_pushed = transfer_rows(
        source,
        dest,
        table,
        &rows_to_push,
        batch_config,
        exclude_columns,
        "Pushing",
        progress,
    )
    .await?;
    Ok(result)
}

/// Pull changes from source to destination for a single table.
#[allow(clippy::too_many_arguments)]
pub async fn pull_table<Src: DataSource, Dst: DataSource>(
    local: &Dst,
    remote: &Src,
    table: &str,
    diff: &TableDiff,
    conflict_resolution: ConflictResolution,
    batch_config: &BatchConfig,
    exclude_columns: &[String],
    dry_run: bool,
    progress: &dyn SyncProgress,
) -> Result<SyncResult> {
    let mut result = SyncResult::new(table);
    let rows_to_pull = diff.rows_to_pull(conflict_resolution);

    if rows_to_pull.is_empty() {
        info!("No changes to pull for table: {}", table);
        return Ok(result);
    }

    info!(
        "Pulling {} rows to table: {} (dry_run={})",
        rows_to_pull.len(),
        table,
        dry_run
    );

    if dry_run {
        result.rows_pulled = rows_to_pull.len();
        return Ok(result);
    }

    result.rows_pulled = transfer_rows(
        remote,
        local,
        table,
        &rows_to_pull,
        batch_config,
        exclude_columns,
        "Pulling",
        progress,
    )
    .await?;
    Ok(result)
}

/// Bidirectional sync of a single table: push source->dest and pull dest->source.
#[allow(clippy::too_many_arguments)]
pub async fn sync_table<A: DataSource, B: DataSource>(
    a: &A,
    b: &B,
    table: &str,
    timestamp_column: &str,
    conflict_resolution: ConflictResolution,
    batch_config: &BatchConfig,
    exclude_columns: &[String],
    dry_run: bool,
    progress: &dyn SyncProgress,
) -> Result<SyncResult> {
    let diff = diff_table(a, b, table, timestamp_column, exclude_columns).await?;
    let (stats, detail) = if dry_run {
        (Some(diff.stats()), Some(DiffDetail::from_diff(&diff)))
    } else {
        (None, None)
    };

    if !diff.has_changes() {
        info!("Table {} is in sync", table);
        let mut r = SyncResult::new(table);
        r.diff_stats = stats;
        r.diff_detail = detail;
        return Ok(r);
    }

    let push_result = push_table(
        a,
        b,
        table,
        &diff,
        conflict_resolution,
        batch_config,
        exclude_columns,
        dry_run,
        progress,
    )
    .await?;
    let pull_result = pull_table(
        a,
        b,
        table,
        &diff,
        conflict_resolution,
        batch_config,
        exclude_columns,
        dry_run,
        progress,
    )
    .await?;

    Ok(SyncResult {
        table: table.to_string(),
        rows_pushed: push_result.rows_pushed,
        rows_pulled: pull_result.rows_pulled,
        diff_stats: stats,
        diff_detail: detail,
    })
}

/// Get list of tables to sync based on config
pub async fn get_tables_to_sync<A: DataSource, B: DataSource>(
    local: &A,
    remote: &B,
    config: &Config,
) -> Result<Vec<String>> {
    let local_tables: std::collections::HashSet<_> =
        local.list_tables().await?.into_iter().collect();
    let remote_tables: std::collections::HashSet<_> =
        remote.list_tables().await?.into_iter().collect();

    let common: Vec<String> = local_tables
        .intersection(&remote_tables)
        .filter(|t| config.should_sync_table(t))
        .cloned()
        .collect();

    let mut syncable = Vec::new();
    for table in common {
        match local.table_info(&table).await {
            Ok(info) if !info.primary_key.is_empty() => {
                syncable.push(table);
            }
            Ok(_) => {
                warn!(
                    "Skipping table '{}': no primary key (required for change detection)",
                    table
                );
            }
            Err(e) => {
                warn!("Skipping table '{}': {}", table, e);
            }
        }
    }

    for table in local_tables.difference(&remote_tables) {
        if config.should_sync_table(table) {
            warn!("Table '{}' exists only in source", table);
        }
    }

    for table in remote_tables.difference(&local_tables) {
        if config.should_sync_table(table) {
            warn!("Table '{}' exists only in destination", table);
        }
    }

    info!("Found {} tables to sync", syncable.len());
    Ok(syncable)
}

/// Push all tables from source to destination.
pub async fn push_all<Src: DataSource, Dst: DataSource>(
    source: &Src,
    dest: &Dst,
    config: &Config,
    tables: Option<Vec<String>>,
    dry_run: bool,
    progress: &dyn SyncProgress,
) -> Result<Vec<SyncResult>> {
    let tables_to_sync = match tables {
        Some(t) => t,
        None => get_tables_to_sync(source, dest, config).await?,
    };

    let batch_config = BatchConfig::from_sync_config(&config.sync);
    let mut results = Vec::new();

    for table in &tables_to_sync {
        let diff = diff_table(
            source,
            dest,
            table,
            &config.sync.timestamp_column,
            &config.sync.exclude_columns,
        )
        .await?;

        let (stats, detail) = if dry_run {
            (Some(diff.stats()), Some(DiffDetail::from_diff(&diff)))
        } else {
            (None, None)
        };

        if !diff.has_changes() {
            info!("Table {} is in sync", table);
            let mut r = SyncResult::new(table);
            r.diff_stats = stats;
            r.diff_detail = detail;
            results.push(r);
            continue;
        }

        let mut result = push_table(
            source,
            dest,
            table,
            &diff,
            config.sync.conflict_resolution,
            &batch_config,
            &config.sync.exclude_columns,
            dry_run,
            progress,
        )
        .await?;
        result.diff_stats = stats;
        result.diff_detail = detail;
        results.push(result);
    }

    Ok(results)
}

/// Pull all tables from source to destination.
pub async fn pull_all<Src: DataSource, Dst: DataSource>(
    local: &Dst,
    remote: &Src,
    config: &Config,
    tables: Option<Vec<String>>,
    dry_run: bool,
    progress: &dyn SyncProgress,
) -> Result<Vec<SyncResult>> {
    let tables_to_sync = match tables {
        Some(t) => t,
        None => get_tables_to_sync(local, remote, config).await?,
    };

    let batch_config = BatchConfig::from_sync_config(&config.sync);
    let mut results = Vec::new();

    for table in &tables_to_sync {
        let diff = diff_table(
            local,
            remote,
            table,
            &config.sync.timestamp_column,
            &config.sync.exclude_columns,
        )
        .await?;

        let (stats, detail) = if dry_run {
            (Some(diff.stats()), Some(DiffDetail::from_diff(&diff)))
        } else {
            (None, None)
        };

        if !diff.has_changes() {
            info!("Table {} is in sync", table);
            let mut r = SyncResult::new(table);
            r.diff_stats = stats;
            r.diff_detail = detail;
            results.push(r);
            continue;
        }

        let mut result = pull_table(
            local,
            remote,
            table,
            &diff,
            config.sync.conflict_resolution,
            &batch_config,
            &config.sync.exclude_columns,
            dry_run,
            progress,
        )
        .await?;
        result.diff_stats = stats;
        result.diff_detail = detail;
        results.push(result);
    }

    Ok(results)
}

/// Bidirectional sync of all tables.
pub async fn sync_all<A: DataSource, B: DataSource>(
    a: &A,
    b: &B,
    config: &Config,
    tables: Option<Vec<String>>,
    dry_run: bool,
    progress: &dyn SyncProgress,
) -> Result<Vec<SyncResult>> {
    let tables_to_sync = match tables {
        Some(t) => t,
        None => get_tables_to_sync(a, b, config).await?,
    };

    let batch_config = BatchConfig::from_sync_config(&config.sync);
    let mut results = Vec::new();

    for table in &tables_to_sync {
        let result = sync_table(
            a,
            b,
            table,
            &config.sync.timestamp_column,
            config.sync.conflict_resolution,
            &batch_config,
            &config.sync.exclude_columns,
            dry_run,
            progress,
        )
        .await?;
        results.push(result);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_excluded_columns_stripped_from_rows() {
        let rows = vec![
            HashMap::from([
                ("id".to_string(), json!(1)),
                ("name".to_string(), json!("Alice")),
                ("title_embedding".to_string(), json!([0.1, 0.2, 0.3])),
                ("vector".to_string(), json!([1, 2, 3])),
            ]),
            HashMap::from([
                ("id".to_string(), json!(2)),
                ("name".to_string(), json!("Bob")),
                ("title_embedding".to_string(), json!([0.4, 0.5, 0.6])),
                ("vector".to_string(), json!([4, 5, 6])),
            ]),
        ];

        let exclude = vec!["*_embedding".to_string(), "vector".to_string()];
        let stripped = strip_excluded_columns(rows, &exclude);

        assert_eq!(stripped.len(), 2);
        for row in &stripped {
            assert!(row.contains_key("id"));
            assert!(row.contains_key("name"));
            assert!(!row.contains_key("title_embedding"));
            assert!(!row.contains_key("vector"));
        }
    }

    #[test]
    fn test_empty_exclusion_preserves_all_columns() {
        let rows = vec![HashMap::from([
            ("id".to_string(), json!(1)),
            ("name".to_string(), json!("Alice")),
            ("embedding".to_string(), json!([0.1, 0.2])),
        ])];

        let stripped = strip_excluded_columns(rows.clone(), &[]);
        assert_eq!(stripped, rows);
    }

    #[test]
    fn test_strip_with_prefix_pattern() {
        let rows = vec![HashMap::from([
            ("id".to_string(), json!(1)),
            ("blob_image".to_string(), json!("base64data")),
            ("blob_thumb".to_string(), json!("base64thumb")),
            ("title".to_string(), json!("Photo")),
        ])];

        let exclude = vec!["blob_*".to_string()];
        let stripped = strip_excluded_columns(rows, &exclude);

        assert_eq!(stripped[0].len(), 2);
        assert!(stripped[0].contains_key("id"));
        assert!(stripped[0].contains_key("title"));
    }
}
