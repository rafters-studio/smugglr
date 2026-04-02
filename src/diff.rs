//! Change detection between local and remote databases

use crate::config::ConflictResolution;
use crate::datasource::DataSource;
use crate::error::Result;
use std::collections::HashSet;
use tracing::{debug, info, warn};

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
                        self.table,
                    );
                }
            }
            ConflictResolution::UuidV7Wins => {
                // UUIDv7 PKs prevent insert collisions. For same-row modifications
                // without usable timestamps, we can't determine which is newer.
                // UUIDv7 creation time is the same on both sides (same PK).
                if !self.content_differs.is_empty() {
                    warn!(
                        "{} row(s) in '{}' have different content but no usable timestamps -- \
                         skipped under uuid_v7_wins. UUIDv7 prevents insert collisions but \
                         cannot resolve same-row modification conflicts without an updated_at \
                         column. Add a timestamp column or use local_wins/remote_wins.",
                        self.content_differs.len(),
                        self.table,
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
            ConflictResolution::NewerWins | ConflictResolution::UuidV7Wins => {
                // Warning already emitted in rows_to_push
            }
        }

        rows
    }

    /// Get rows to delete from remote (for push)
    /// Reserved for full sync mode
    #[allow(dead_code)]
    pub fn rows_to_delete_remote(&self) -> Vec<String> {
        // Rows that only exist in remote but not in local
        // This is only for full sync mode, not incremental
        vec![]
    }

    /// Get rows to delete from local (for pull)
    /// Reserved for full sync mode
    #[allow(dead_code)]
    pub fn rows_to_delete_local(&self) -> Vec<String> {
        // Rows that only exist in local but not in remote
        // This is only for full sync mode, not incremental
        vec![]
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

/// Compare two data sources for a table.
///
/// When `conflict_resolution` is provided, performs additional validation
/// (e.g. checking for UUIDv7 PKs when using `UuidV7Wins`).
pub async fn diff_table<A: DataSource, B: DataSource>(
    local: &A,
    remote: &B,
    table: &str,
    timestamp_column: &str,
) -> Result<TableDiff> {
    diff_table_with_resolution(local, remote, table, timestamp_column, None).await
}

/// Compare two data sources for a table, with optional conflict resolution validation.
pub async fn diff_table_with_resolution<A: DataSource, B: DataSource>(
    local: &A,
    remote: &B,
    table: &str,
    timestamp_column: &str,
    conflict_resolution: Option<ConflictResolution>,
) -> Result<TableDiff> {
    info!("Computing diff for table: {}", table);

    // Get metadata from both sides
    let local_meta = local.get_row_metadata(table, timestamp_column).await?;
    let remote_meta = remote.get_row_metadata(table, timestamp_column).await?;

    let local_keys: HashSet<&String> = local_meta.keys().collect();
    let remote_keys: HashSet<&String> = remote_meta.keys().collect();

    let mut diff = TableDiff::new(table);

    // Find rows only in local
    for pk in local_keys.difference(&remote_keys) {
        diff.local_only.push((*pk).clone());
    }

    // Find rows only in remote
    for pk in remote_keys.difference(&local_keys) {
        diff.remote_only.push((*pk).clone());
    }

    // If using UuidV7Wins, validate that PKs are UUIDv7.
    // Master-master sync with non-UUIDv7 PKs is unsafe (insert collisions).
    if conflict_resolution == Some(ConflictResolution::UuidV7Wins) && !local_keys.is_empty() {
        let sample_pks: Vec<&String> = local_keys.iter().take(10).copied().collect();
        let non_uuid_count = sample_pks.iter().filter(|pk| !is_uuidv7(pk)).count();

        if non_uuid_count == sample_pks.len() {
            return Err(crate::error::SyncError::Config(format!(
                "Table '{}': uuid_v7_wins requires UUIDv7 primary keys, but all sampled \
                 PKs are non-UUIDv7 (e.g. '{}'). Master-master sync with integer or UUIDv4 \
                 PKs will cause insert collisions. Migrate to UUIDv7 PKs or use a different \
                 conflict_resolution strategy.",
                table,
                sample_pks.first().map(|s| s.as_str()).unwrap_or("?")
            )));
        } else if non_uuid_count > 0 {
            warn!(
                "Table '{}': uuid_v7_wins configured but {}/{} sampled PKs are not UUIDv7. \
                 Mixed PK types may cause collisions for non-UUIDv7 rows.",
                table,
                non_uuid_count,
                sample_pks.len()
            );
        }
    }

    // Compare rows that exist in both
    for pk in local_keys.intersection(&remote_keys) {
        let local_row = &local_meta[*pk];
        let remote_row = &remote_meta[*pk];

        // First check content hash - if identical, no change needed
        if local_row.content_hash == remote_row.content_hash {
            diff.identical.push((*pk).clone());
            continue;
        }

        // Content differs - check timestamps if available
        match (&local_row.updated_at, &remote_row.updated_at) {
            (Some(local_ts), Some(remote_ts)) => {
                if local_ts > remote_ts {
                    diff.local_newer.push((*pk).clone());
                } else if remote_ts > local_ts {
                    diff.remote_newer.push((*pk).clone());
                } else {
                    // Same timestamp but different content - conflict
                    diff.content_differs.push((*pk).clone());
                }
            }
            _ => {
                // No timestamps available - mark as content differs
                diff.content_differs.push((*pk).clone());
            }
        }
    }

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

/// Compare all tables
/// Reserved for batch operations
#[allow(dead_code)]
pub async fn diff_all<A: DataSource, B: DataSource>(
    local: &A,
    remote: &B,
    tables: &[String],
    timestamp_column: &str,
) -> Result<Vec<TableDiff>> {
    let mut diffs = Vec::new();

    for table in tables {
        let diff = diff_table(local, remote, table, timestamp_column).await?;
        diffs.push(diff);
    }

    Ok(diffs)
}

/// Extract the Unix millisecond timestamp from a UUIDv7 string.
///
/// UUIDv7 format: `xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx`
/// The first 48 bits (12 hex chars) encode Unix timestamp in milliseconds.
/// The version nibble (position 13) must be `7`.
///
/// Returns `None` if the string is not a valid UUIDv7.
#[allow(dead_code)] // Used by broadcast sync (#39)
pub fn extract_uuidv7_timestamp(uuid_str: &str) -> Option<u64> {
    // Strip hyphens: 32 hex chars
    let hex: String = uuid_str.chars().filter(|c| *c != '-').collect();

    if hex.len() != 32 {
        return None;
    }

    // Check version nibble (13th hex char, index 12) == 7
    if hex.as_bytes().get(12) != Some(&b'7') {
        return None;
    }

    // First 12 hex chars = 48-bit timestamp in milliseconds
    let ts_hex = &hex[..12];
    u64::from_str_radix(ts_hex, 16).ok()
}

/// Check if a string looks like a UUIDv7.
#[allow(dead_code)]
pub fn is_uuidv7(s: &str) -> bool {
    extract_uuidv7_timestamp(s).is_some()
}

/// Compare two UUIDv7 strings by their embedded timestamps.
///
/// Returns `Ordering::Greater` if `a` is newer than `b`.
/// Returns `None` if either string is not a valid UUIDv7.
#[allow(dead_code)]
pub fn compare_uuidv7(a: &str, b: &str) -> Option<std::cmp::Ordering> {
    let ts_a = extract_uuidv7_timestamp(a)?;
    let ts_b = extract_uuidv7_timestamp(b)?;
    Some(ts_a.cmp(&ts_b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_uuidv7_timestamp_valid() {
        // UUIDv7: 018cc6b0-c000-7000-8000-000000000000
        // First 12 hex chars (sans hyphens): 018cc6b0c000
        // 0x018cc6b0c000 = 1704140521472 ms
        let uuid = "018cc6b0-c000-7000-8000-000000000000";
        let ts = extract_uuidv7_timestamp(uuid);
        assert_eq!(ts, Some(1704140521472));
    }

    #[test]
    fn test_extract_uuidv7_timestamp_v4_rejected() {
        // UUIDv4 has version nibble 4, not 7
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(extract_uuidv7_timestamp(uuid), None);
    }

    #[test]
    fn test_extract_uuidv7_timestamp_invalid_string() {
        assert_eq!(extract_uuidv7_timestamp("not-a-uuid"), None);
        assert_eq!(extract_uuidv7_timestamp(""), None);
        assert_eq!(extract_uuidv7_timestamp("12345"), None);
    }

    #[test]
    fn test_is_uuidv7() {
        assert!(is_uuidv7("018cc6b0-c000-7000-8000-000000000000"));
        assert!(!is_uuidv7("550e8400-e29b-41d4-a716-446655440000")); // v4
        assert!(!is_uuidv7("12345"));
        assert!(!is_uuidv7(""));
    }

    #[test]
    fn test_compare_uuidv7_ordering() {
        // Earlier timestamp
        let earlier = "018cc6b0-c000-7000-8000-000000000000";
        // Later timestamp (1 ms later)
        let later = "018cc6b0-c001-7000-8000-000000000000";

        assert_eq!(
            compare_uuidv7(earlier, later),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            compare_uuidv7(later, earlier),
            Some(std::cmp::Ordering::Greater)
        );
        assert_eq!(
            compare_uuidv7(earlier, earlier),
            Some(std::cmp::Ordering::Equal)
        );
    }

    #[test]
    fn test_compare_uuidv7_non_uuid_returns_none() {
        let valid = "018cc6b0-c000-7000-8000-000000000000";
        assert_eq!(compare_uuidv7(valid, "not-uuid"), None);
        assert_eq!(compare_uuidv7("not-uuid", valid), None);
    }

    #[test]
    fn test_uuidv7_wins_behaves_like_newer_wins_for_content_differs() {
        let diff = TableDiff {
            table: "test".to_string(),
            local_only: vec!["a".to_string()],
            remote_only: vec!["b".to_string()],
            local_newer: vec![],
            remote_newer: vec![],
            content_differs: vec!["c".to_string()],
            identical: vec![],
        };

        // UuidV7Wins skips content_differs (like NewerWins)
        let push = diff.rows_to_push(ConflictResolution::UuidV7Wins);
        assert_eq!(push, vec!["a".to_string()]);

        let pull = diff.rows_to_pull(ConflictResolution::UuidV7Wins);
        assert_eq!(pull, vec!["b".to_string()]);
    }

    #[test]
    fn test_uuidv7_wins_content_differs_warns_about_timestamps() {
        // UuidV7Wins with content_differs rows should not include them
        // in push (same behavior as NewerWins -- can't resolve without timestamps)
        let diff = TableDiff {
            table: "test".to_string(),
            local_only: vec![],
            remote_only: vec![],
            local_newer: vec!["newer-local".to_string()],
            remote_newer: vec!["newer-remote".to_string()],
            content_differs: vec!["conflict-row".to_string()],
            identical: vec![],
        };

        let push = diff.rows_to_push(ConflictResolution::UuidV7Wins);
        assert!(push.contains(&"newer-local".to_string()));
        assert!(!push.contains(&"conflict-row".to_string()));

        let pull = diff.rows_to_pull(ConflictResolution::UuidV7Wins);
        assert!(pull.contains(&"newer-remote".to_string()));
        assert!(!pull.contains(&"conflict-row".to_string()));
    }

    #[test]
    fn test_rows_to_push_local_wins() {
        let diff = TableDiff {
            table: "test".to_string(),
            local_only: vec!["a".to_string()],
            remote_only: vec![],
            local_newer: vec!["b".to_string()],
            remote_newer: vec![],
            content_differs: vec!["c".to_string()],
            identical: vec![],
        };

        let push = diff.rows_to_push(ConflictResolution::LocalWins);
        assert!(push.contains(&"a".to_string()));
        assert!(push.contains(&"b".to_string()));
        assert!(push.contains(&"c".to_string()));
    }

    #[test]
    fn test_rows_to_push_remote_wins() {
        let diff = TableDiff {
            table: "test".to_string(),
            local_only: vec!["a".to_string()],
            remote_only: vec![],
            local_newer: vec![],
            remote_newer: vec![],
            content_differs: vec!["c".to_string()],
            identical: vec![],
        };

        let push = diff.rows_to_push(ConflictResolution::RemoteWins);
        assert!(push.contains(&"a".to_string()));
        assert!(!push.contains(&"c".to_string()));
    }
}
