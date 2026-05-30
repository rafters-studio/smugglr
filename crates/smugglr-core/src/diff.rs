//! Change detection between local and remote databases

use crate::config::ConflictResolution;
use crate::datasource::{DataSource, RowMeta};
use crate::error::Result;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

/// Per-table diff statistics (counts only, no PK values).
#[derive(Debug, Clone, Serialize)]
pub struct DiffStats {
    pub local_only: usize,
    pub remote_only: usize,
    pub local_newer: usize,
    pub remote_newer: usize,
    pub content_differs: usize,
    pub identical: usize,
}

/// Represents the differences between local and remote for a table
#[derive(Debug, Default)]
pub struct TableDiff {
    #[allow(dead_code)]
    pub table: String,
    /// Rows that exist only in local
    pub local_only: Vec<String>,
    /// Rows that exist only in remote
    pub remote_only: Vec<String>,
    /// Rows where local is newer (by timestamp)
    pub local_newer: Vec<String>,
    /// Rows where remote is newer (by timestamp)
    pub remote_newer: Vec<String>,
    /// Rows that differ but have no timestamp (use content hash)
    pub content_differs: Vec<String>,
    /// Rows that are identical
    pub identical: Vec<String>,
}

impl TableDiff {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            ..Default::default()
        }
    }

    /// Check if there are any differences
    pub fn has_changes(&self) -> bool {
        !self.local_only.is_empty()
            || !self.remote_only.is_empty()
            || !self.local_newer.is_empty()
            || !self.remote_newer.is_empty()
            || !self.content_differs.is_empty()
    }

    /// Get rows that should be pushed (local -> remote)
    pub fn rows_to_push(&self, conflict_resolution: ConflictResolution) -> Vec<String> {
        let mut rows = Vec::new();
        rows.extend(self.local_only.clone());
        rows.extend(self.local_newer.clone());

        match conflict_resolution {
            ConflictResolution::LocalWins => {
                rows.extend(self.content_differs.clone());
            }
            ConflictResolution::RemoteWins => {
                // Don't push content_differs
            }
            ConflictResolution::NewerWins => {
                if !self.content_differs.is_empty() {
                    warn!(
                        "{} row(s) in '{}' have different content but no usable timestamps -- \
                         skipped under newer_wins. Use local_wins or remote_wins to resolve.",
                        self.content_differs.len(),
                        self.table
                    );
                }
            }
            ConflictResolution::UuidV7Wins => {
                if !self.content_differs.is_empty() {
                    warn!(
                        "{} row(s) in '{}' have different content but same PK (identical \
                         UUIDv7 timestamp) -- skipped under uuid_v7_wins. \
                         Use local_wins or remote_wins to resolve.",
                        self.content_differs.len(),
                        self.table
                    );
                }
            }
        }

        rows
    }

    /// Get rows that should be pulled (remote -> local)
    pub fn rows_to_pull(&self, conflict_resolution: ConflictResolution) -> Vec<String> {
        let mut rows = Vec::new();
        rows.extend(self.remote_only.clone());
        rows.extend(self.remote_newer.clone());

        match conflict_resolution {
            ConflictResolution::LocalWins => {
                // Don't pull content_differs
            }
            ConflictResolution::RemoteWins => {
                rows.extend(self.content_differs.clone());
            }
            ConflictResolution::NewerWins => {
                // Warning already emitted in rows_to_push
            }
            ConflictResolution::UuidV7Wins => {
                // Warning already emitted in rows_to_push
            }
        }

        rows
    }

    /// Compute aggregate diff statistics (counts only).
    pub fn stats(&self) -> DiffStats {
        DiffStats {
            local_only: self.local_only.len(),
            remote_only: self.remote_only.len(),
            local_newer: self.local_newer.len(),
            remote_newer: self.remote_newer.len(),
            content_differs: self.content_differs.len(),
            identical: self.identical.len(),
        }
    }

    /// Summary string for display
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();

        if !self.local_only.is_empty() {
            parts.push(format!("+{} local only", self.local_only.len()));
        }
        if !self.remote_only.is_empty() {
            parts.push(format!("+{} remote only", self.remote_only.len()));
        }
        if !self.local_newer.is_empty() {
            parts.push(format!("{} local newer", self.local_newer.len()));
        }
        if !self.remote_newer.is_empty() {
            parts.push(format!("{} remote newer", self.remote_newer.len()));
        }
        if !self.content_differs.is_empty() {
            parts.push(format!("{} content differs", self.content_differs.len()));
        }
        if !self.identical.is_empty() {
            parts.push(format!("{} identical", self.identical.len()));
        }

        if parts.is_empty() {
            "no data".to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// Classify row-level differences between two pre-fetched metadata maps.
///
/// Pure function with no I/O: partitions primary keys into local-only,
/// remote-only, newer-on-each-side, content-differs, and identical buckets.
/// Used by [`diff_table`] after fetching metadata, and by the WASM package's
/// cached diff path which bypasses per-call full scans.
pub fn classify_diff(
    local_meta: &HashMap<String, RowMeta>,
    remote_meta: &HashMap<String, RowMeta>,
    table: &str,
) -> TableDiff {
    let local_keys: HashSet<&String> = local_meta.keys().collect();
    let remote_keys: HashSet<&String> = remote_meta.keys().collect();

    let mut diff = TableDiff::new(table);

    for pk in local_keys.difference(&remote_keys) {
        diff.local_only.push((*pk).clone());
    }

    for pk in remote_keys.difference(&local_keys) {
        diff.remote_only.push((*pk).clone());
    }

    for pk in local_keys.intersection(&remote_keys) {
        let local_row = &local_meta[*pk];
        let remote_row = &remote_meta[*pk];

        if local_row.content_hash == remote_row.content_hash {
            diff.identical.push((*pk).clone());
            continue;
        }

        match (&local_row.updated_at, &remote_row.updated_at) {
            (Some(local_ts), Some(remote_ts)) => {
                if local_ts > remote_ts {
                    diff.local_newer.push((*pk).clone());
                } else if remote_ts > local_ts {
                    diff.remote_newer.push((*pk).clone());
                } else {
                    // Same timestamp, different content: treat as conflict.
                    diff.content_differs.push((*pk).clone());
                }
            }
            _ => {
                diff.content_differs.push((*pk).clone());
            }
        }
    }

    diff
}

/// Compare two data sources for a table
pub async fn diff_table<A: DataSource, B: DataSource>(
    local: &A,
    remote: &B,
    table: &str,
    timestamp_column: &str,
    exclude_columns: &[String],
) -> Result<TableDiff> {
    info!("Computing diff for table: {}", table);

    // Get metadata from both sides (excluded columns are omitted from content hash)
    let local_meta = local
        .get_row_metadata(table, timestamp_column, exclude_columns)
        .await?;
    let remote_meta = remote
        .get_row_metadata(table, timestamp_column, exclude_columns)
        .await?;

    let diff = classify_diff(&local_meta, &remote_meta, table);

    debug!(
        "Diff for {}: local_only={}, remote_only={}, local_newer={}, remote_newer={}, content_differs={}, identical={}",
        table,
        diff.local_only.len(),
        diff.remote_only.len(),
        diff.local_newer.len(),
        diff.remote_newer.len(),
        diff.content_differs.len(),
        diff.identical.len()
    );

    Ok(diff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuidv7_wins_push_includes_local_only_and_newer() {
        let diff = TableDiff {
            table: "items".to_string(),
            local_only: vec!["018ec7e6-1a80-7000-8000-aaaaaaaaaaaa".to_string()],
            remote_only: vec!["018ec7e6-1a81-7000-8000-bbbbbbbbbbbb".to_string()],
            local_newer: vec!["pk3".to_string()],
            remote_newer: vec!["pk4".to_string()],
            content_differs: vec![],
            identical: vec![],
        };

        let push = diff.rows_to_push(ConflictResolution::UuidV7Wins);
        assert_eq!(push.len(), 2);
        assert!(push.contains(&"018ec7e6-1a80-7000-8000-aaaaaaaaaaaa".to_string()));
        assert!(push.contains(&"pk3".to_string()));

        let pull = diff.rows_to_pull(ConflictResolution::UuidV7Wins);
        assert_eq!(pull.len(), 2);
        assert!(pull.contains(&"018ec7e6-1a81-7000-8000-bbbbbbbbbbbb".to_string()));
        assert!(pull.contains(&"pk4".to_string()));
    }

    #[test]
    fn test_uuidv7_wins_content_differs_skipped() {
        let diff = TableDiff {
            table: "items".to_string(),
            local_only: vec![],
            remote_only: vec![],
            local_newer: vec![],
            remote_newer: vec![],
            content_differs: vec!["018ec7e6-1a80-7000-8000-aaaaaaaaaaaa".to_string()],
            identical: vec![],
        };

        assert!(diff.rows_to_push(ConflictResolution::UuidV7Wins).is_empty());
        assert!(diff.rows_to_pull(ConflictResolution::UuidV7Wins).is_empty());
    }

    #[test]
    fn test_non_uuidv7_falls_back_to_newer_wins_behavior() {
        let diff = TableDiff {
            table: "scores".to_string(),
            local_only: vec![],
            remote_only: vec![],
            local_newer: vec!["42".to_string()],
            remote_newer: vec![],
            content_differs: vec!["99".to_string()],
            identical: vec![],
        };

        let push = diff.rows_to_push(ConflictResolution::UuidV7Wins);
        assert_eq!(push, vec!["42".to_string()]);
        assert!(diff.rows_to_pull(ConflictResolution::UuidV7Wins).is_empty());
    }
}
