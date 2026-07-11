//! Change detection between local and remote databases

use crate::config::ConflictResolution;
use crate::datasource::{DataSource, RowMeta};
use crate::error::Result;
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

/// Order two `updated_at` timestamps for conflict resolution.
///
/// Integer Unix timestamps render as decimal strings ("999", "1000"); comparing
/// those lexicographically is wrong across a digit-count boundary ("999" sorts
/// after "1000"), so parse and compare numerically when BOTH sides are integers.
/// ISO-8601 text timestamps are fixed-width, where lexical order == chronological,
/// so fall back to string comparison when NEITHER side is an integer. A mixed
/// pair (one integer, one non-integer) has no meaningful ordering -- return
/// `None` so the caller routes it to `content_differs` rather than inventing a
/// confident (and likely wrong) winner from a lexical compare of unlike
/// representations.
fn compare_ts(a: &str, b: &str) -> Option<Ordering> {
    match (a.parse::<i64>(), b.parse::<i64>()) {
        (Ok(x), Ok(y)) => Some(x.cmp(&y)),
        (Err(_), Err(_)) => Some(a.cmp(b)),
        _ => None,
    }
}

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
    /// Table this diff is for (used in conflict warnings).
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

        // local_wins pushes the local side of a content conflict. The other
        // policies pull it (remote_wins) or skip it (newer/uuid). The "skipped"
        // warning is emitted once per diff via `warn_unresolved_conflicts`, not
        // here -- this accessor stays pure (no logging side effect).
        if matches!(conflict_resolution, ConflictResolution::LocalWins) {
            rows.extend(self.content_differs.clone());
        }

        rows
    }

    /// Get rows that should be pulled (remote -> local)
    pub fn rows_to_pull(&self, conflict_resolution: ConflictResolution) -> Vec<String> {
        let mut rows = Vec::new();
        rows.extend(self.remote_only.clone());
        rows.extend(self.remote_newer.clone());

        // remote_wins pulls the remote side of a content conflict; the others
        // push it (local_wins) or skip it (newer/uuid). Pure accessor; see
        // `warn_unresolved_conflicts` for the skipped-rows warning.
        if matches!(conflict_resolution, ConflictResolution::RemoteWins) {
            rows.extend(self.content_differs.clone());
        }

        rows
    }

    /// Content-differing rows this policy leaves UNRESOLVED -- skipped in both
    /// directions because there's no usable tiebreaker. `newer_wins` /
    /// `uuid_v7_wins` skip same-timestamp/same-PK conflicts; `local_wins` /
    /// `remote_wins` always resolve them, so none are unresolved.
    pub fn unresolved_conflicts(&self, conflict_resolution: ConflictResolution) -> &[String] {
        match conflict_resolution {
            ConflictResolution::NewerWins | ConflictResolution::UuidV7Wins => &self.content_differs,
            ConflictResolution::LocalWins | ConflictResolution::RemoteWins => &[],
        }
    }

    /// Warn once about content-differing rows skipped under the conflict policy.
    ///
    /// Call this at each diff-creation site (not inside the row accessors) so the
    /// warning fires exactly once per table in every direction -- including a
    /// pull-only run, which previously got no warning at all.
    pub fn warn_unresolved_conflicts(&self, conflict_resolution: ConflictResolution) {
        let count = self.unresolved_conflicts(conflict_resolution).len();
        if count == 0 {
            return;
        }
        // count > 0 implies a skipping policy (unresolved_conflicts is empty for
        // local_wins/remote_wins); pick the reason, sharing one warning template.
        let reason = match conflict_resolution {
            ConflictResolution::NewerWins => {
                "missing or incomparable timestamps (skipped under newer_wins)"
            }
            ConflictResolution::UuidV7Wins => {
                "same PK with identical UUIDv7 timestamp (skipped under uuid_v7_wins)"
            }
            ConflictResolution::LocalWins | ConflictResolution::RemoteWins => return,
        };
        warn!(
            "{} row(s) in '{}' have different content but {}. \
             Use local_wins or remote_wins to resolve.",
            count, self.table, reason
        );
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
            (Some(local_ts), Some(remote_ts)) => match compare_ts(local_ts, remote_ts) {
                Some(Ordering::Greater) => diff.local_newer.push((*pk).clone()),
                Some(Ordering::Less) => diff.remote_newer.push((*pk).clone()),
                // Equal timestamps or unlike representations (one integer, one
                // text): no safe tiebreaker, so treat as a content conflict.
                Some(Ordering::Equal) | None => diff.content_differs.push((*pk).clone()),
            },
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
    fn unresolved_conflicts_tracks_only_newer_and_uuid() {
        let diff = TableDiff {
            table: "t".to_string(),
            local_only: vec![],
            remote_only: vec![],
            local_newer: vec![],
            remote_newer: vec![],
            content_differs: vec!["a".to_string(), "b".to_string()],
            identical: vec![],
        };
        // newer/uuid leave same-content conflicts unresolved (skipped both ways)
        assert_eq!(
            diff.unresolved_conflicts(ConflictResolution::NewerWins)
                .len(),
            2
        );
        assert_eq!(
            diff.unresolved_conflicts(ConflictResolution::UuidV7Wins)
                .len(),
            2
        );
        // local/remote always resolve them -> none unresolved
        assert!(diff
            .unresolved_conflicts(ConflictResolution::LocalWins)
            .is_empty());
        assert!(diff
            .unresolved_conflicts(ConflictResolution::RemoteWins)
            .is_empty());
        // Regression for #144: the skip is symmetric -- a pull-only run under
        // newer_wins also skips the content-differs rows (the warning is now
        // emitted at the diff-creation site, not only on the push path).
        assert!(diff.rows_to_pull(ConflictResolution::NewerWins).is_empty());
        assert!(diff.rows_to_push(ConflictResolution::NewerWins).is_empty());
        // local_wins pushes them; remote_wins pulls them.
        assert_eq!(diff.rows_to_push(ConflictResolution::LocalWins).len(), 2);
        assert_eq!(diff.rows_to_pull(ConflictResolution::RemoteWins).len(), 2);
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

    // --- Integer-timestamp conflict resolution (#176 / #177) ---

    fn one(hash: &str, ts: Option<&str>) -> HashMap<String, RowMeta> {
        let mut m = HashMap::new();
        m.insert(
            "r".to_string(),
            RowMeta {
                pk_value: "r".to_string(),
                updated_at: ts.map(String::from),
                content_hash: hash.to_string(),
            },
        );
        m
    }

    // #177: an integer-timestamp row whose content changed must be classified by
    // direction and actually sync -- NOT dropped into content_differs (which is
    // skipped in both directions under newer_wins). This is the literal symptom
    // the bug produced once both sides finally carry the integer timestamp.
    #[test]
    fn integer_timestamp_changed_row_syncs_not_skipped() {
        // remote newer (larger unix ts), different content
        let local = one("A", Some("1700000000"));
        let remote = one("B", Some("1700000100"));
        let diff = classify_diff(&local, &remote, "t");

        assert_eq!(diff.remote_newer, vec!["r".to_string()]);
        assert!(diff.content_differs.is_empty());
        // ...and it is actually pulled under newer_wins, not silently skipped.
        assert!(diff
            .rows_to_pull(ConflictResolution::NewerWins)
            .contains(&"r".to_string()));
        assert!(diff.rows_to_push(ConflictResolution::NewerWins).is_empty());
    }

    // #176: integer timestamps straddling a digit-count boundary. Lexically
    // "1000" < "999" (wrong); numerically 1000 > 999. The newer row (local) must
    // win and be pushed -- the pre-fix code picked the OLDER remote row.
    #[test]
    fn integer_timestamp_lexicographic_boundary_orders_numerically() {
        let local = one("A", Some("1000")); // newer
        let remote = one("B", Some("999")); // older
        let diff = classify_diff(&local, &remote, "t");

        assert_eq!(diff.local_newer, vec!["r".to_string()]);
        assert!(diff.remote_newer.is_empty());
        assert!(diff
            .rows_to_push(ConflictResolution::NewerWins)
            .contains(&"r".to_string()));
    }

    // ISO-8601 text timestamps are fixed-width -> lexical order is chronological.
    // Must keep working unchanged (they never enter the numeric branch).
    #[test]
    fn iso8601_timestamps_still_compare_lexically() {
        let local = one("A", Some("2023-06-01T00:00:01Z")); // newer
        let remote = one("B", Some("2023-06-01T00:00:00Z"));
        let diff = classify_diff(&local, &remote, "t");
        assert_eq!(diff.local_newer, vec!["r".to_string()]);
    }

    // Mixed representation (one integer, one ISO text for the same column --
    // reachable via schema skew between local and remote). There is no honest
    // ordering, so route to content_differs rather than invent a confident
    // lexical winner ("2023-..." vs "1700..." would always pick local).
    #[test]
    fn mixed_timestamp_representation_is_content_differs() {
        let local = one("A", Some("1700000000")); // integer
        let remote = one("B", Some("2023-06-01T00:00:00Z")); // ISO text
        let diff = classify_diff(&local, &remote, "t");
        assert_eq!(diff.content_differs, vec!["r".to_string()]);
        assert!(diff.local_newer.is_empty());
        assert!(diff.remote_newer.is_empty());
    }

    // Equal integer timestamps with differing content: no tiebreaker -> conflict.
    #[test]
    fn equal_integer_timestamps_are_content_differs() {
        let local = one("A", Some("1700000000"));
        let remote = one("B", Some("1700000000"));
        let diff = classify_diff(&local, &remote, "t");
        assert_eq!(diff.content_differs, vec!["r".to_string()]);
    }
}
