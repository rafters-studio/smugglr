//! Point-in-time snapshots for disaster recovery.
//!
//! `snapshot` creates a timestamped full copy of the local database in the relay store.
//! `restore` downloads a snapshot and replaces the local database.
//! `list_snapshots` enumerates available snapshots with metadata.

use crate::config::StashConfig;
use crate::datasource::DataSource;
use crate::error::{Result, SyncError};
use crate::local::LocalDb;
use crate::stash::build_store;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::{debug, info};

/// Metadata stored alongside each snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub timestamp: String,
    pub size_bytes: u64,
    pub tables: Vec<SnapshotTableMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotTableMeta {
    pub name: String,
    pub row_count: usize,
}

/// Result of a snapshot operation.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotResult {
    pub timestamp: String,
    pub size_bytes: u64,
    pub tables: Vec<SnapshotTableMeta>,
}

/// An entry in the snapshot list.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotEntry {
    pub timestamp: String,
    pub size_bytes: u64,
    pub tables: Vec<SnapshotTableMeta>,
}

/// Derive the snapshots prefix from the stash config URL.
///
/// If the relay path is `path/to/relay.sqlite`, snapshots go under `path/to/snapshots/`.
fn snapshots_prefix(relay_path: &ObjectPath) -> ObjectPath {
    let relay_str = relay_path.as_ref();
    let parent = match relay_str.rfind('/') {
        Some(idx) => &relay_str[..idx],
        None => "",
    };
    if parent.is_empty() {
        ObjectPath::from("snapshots")
    } else {
        ObjectPath::from(format!("{}/snapshots", parent))
    }
}

/// Create a point-in-time snapshot of the local database.
pub async fn snapshot(
    config: &StashConfig,
    local_db_path: &str,
    dry_run: bool,
) -> Result<SnapshotResult> {
    let (store, relay_path) = build_store(config)?;
    let prefix = snapshots_prefix(&relay_path);

    let local = LocalDb::open_readonly(local_db_path)?;
    let timestamp = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();

    // Gather table metadata
    let all_tables = local.list_tables().await?;
    let mut tables = Vec::new();
    for table in &all_tables {
        let count = local.row_count(table).await?;
        tables.push(SnapshotTableMeta {
            name: table.clone(),
            row_count: count,
        });
    }

    let db_bytes = std::fs::read(local_db_path)
        .map_err(|e| SyncError::Stash(format!("Failed to read local database: {}", e)))?;
    let size_bytes = db_bytes.len() as u64;

    if dry_run {
        debug!("Dry run -- skipping upload");
        return Ok(SnapshotResult {
            timestamp,
            size_bytes,
            tables,
        });
    }

    // Upload snapshot database
    let snap_path = ObjectPath::from(format!("{}/{}.sqlite", prefix, timestamp));
    info!("Uploading snapshot to {}", snap_path);
    store
        .put(&snap_path, PutPayload::from(db_bytes))
        .await
        .map_err(|e| SyncError::Stash(format!("Failed to upload snapshot: {}", e)))?;

    // Upload metadata sidecar
    let meta = SnapshotMeta {
        timestamp: timestamp.clone(),
        size_bytes,
        tables: tables.clone(),
    };
    let meta_json = serde_json::to_vec(&meta)?;
    let meta_path = ObjectPath::from(format!("{}/{}.meta.json", prefix, timestamp));
    store
        .put(&meta_path, PutPayload::from(meta_json))
        .await
        .map_err(|e| SyncError::Stash(format!("Failed to upload snapshot metadata: {}", e)))?;

    info!("Snapshot created: {} ({} bytes)", timestamp, size_bytes);

    Ok(SnapshotResult {
        timestamp,
        size_bytes,
        tables,
    })
}

/// List available snapshots from the relay store.
pub async fn list_snapshots(config: &StashConfig) -> Result<Vec<SnapshotEntry>> {
    let (store, relay_path) = build_store(config)?;
    let prefix = snapshots_prefix(&relay_path);

    let list_result = store
        .list_with_delimiter(Some(&prefix))
        .await
        .map_err(|e| SyncError::Stash(format!("Failed to list snapshots: {}", e)))?;

    let mut entries = Vec::new();

    for obj in &list_result.objects {
        let path_str = obj.location.as_ref();
        if !path_str.ends_with(".meta.json") {
            continue;
        }

        // Download and parse metadata
        match store.get(&obj.location).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(|e| {
                    SyncError::Stash(format!("Failed to read snapshot metadata: {}", e))
                })?;
                match serde_json::from_slice::<SnapshotMeta>(&bytes) {
                    Ok(meta) => {
                        entries.push(SnapshotEntry {
                            timestamp: meta.timestamp,
                            size_bytes: meta.size_bytes,
                            tables: meta.tables,
                        });
                    }
                    Err(e) => {
                        debug!("Skipping malformed metadata at {}: {}", path_str, e);
                    }
                }
            }
            Err(e) => {
                debug!("Failed to read metadata at {}: {}", path_str, e);
            }
        }
    }

    entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    Ok(entries)
}

/// Restore a snapshot to the local database path.
///
/// Finds the snapshot closest to `target_timestamp` and replaces the local database.
/// If `dry_run`, reports what would be restored without writing.
pub async fn restore(
    config: &StashConfig,
    local_db_path: &str,
    target_timestamp: &str,
    dry_run: bool,
) -> Result<SnapshotResult> {
    let (store, relay_path) = build_store(config)?;
    let prefix = snapshots_prefix(&relay_path);

    // Find the matching snapshot
    let snapshots = list_snapshots(config).await?;
    if snapshots.is_empty() {
        return Err(SyncError::Stash("No snapshots available".into()));
    }

    // Find closest match: exact match first, then closest before the target
    let entry = snapshots
        .iter()
        .find(|s| s.timestamp == target_timestamp)
        .or_else(|| {
            snapshots
                .iter()
                .filter(|s| s.timestamp.as_str() <= target_timestamp)
                .max_by_key(|s| &s.timestamp)
        })
        .ok_or_else(|| {
            SyncError::Stash(format!(
                "No snapshot found at or before {}. Earliest available: {}",
                target_timestamp,
                snapshots
                    .last()
                    .map(|s| s.timestamp.as_str())
                    .unwrap_or("none")
            ))
        })?;

    let result = SnapshotResult {
        timestamp: entry.timestamp.clone(),
        size_bytes: entry.size_bytes,
        tables: entry.tables.clone(),
    };

    if dry_run {
        info!(
            "Would restore snapshot {} ({} bytes)",
            entry.timestamp, entry.size_bytes
        );
        return Ok(result);
    }

    // Download the snapshot
    let snap_path = ObjectPath::from(format!("{}/{}.sqlite", prefix, entry.timestamp));
    info!("Downloading snapshot from {}", snap_path);

    let get_result = store.get(&snap_path).await.map_err(|e| {
        SyncError::Stash(format!(
            "Failed to download snapshot {}: {}",
            entry.timestamp, e
        ))
    })?;

    let bytes = get_result
        .bytes()
        .await
        .map_err(|e| SyncError::Stash(format!("Failed to read snapshot body: {}", e)))?;

    // Write to local database path (atomic via temp file + rename)
    let local_path = Path::new(local_db_path);
    let parent = local_path.parent().unwrap_or(Path::new("."));
    let temp_path = parent.join(format!(".smugglr-restore-{}.tmp", std::process::id()));

    std::fs::write(&temp_path, &bytes)
        .map_err(|e| SyncError::Stash(format!("Failed to write snapshot temp file: {}", e)))?;

    // Validate the downloaded file is a valid SQLite database.
    // Explicitly drop the connection before rename to release file handles (Windows).
    {
        let conn = rusqlite::Connection::open_with_flags(
            &temp_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            SyncError::Stash(format!(
                "Downloaded snapshot is not a valid SQLite database: {}",
                e
            ))
        })?;
        conn.execute_batch("SELECT 1").map_err(|e| {
            let _ = std::fs::remove_file(&temp_path);
            SyncError::Stash(format!(
                "Downloaded snapshot is not a valid SQLite database: {}",
                e
            ))
        })?;
    }

    std::fs::rename(&temp_path, local_path).map_err(|e| {
        let _ = std::fs::remove_file(&temp_path);
        SyncError::Stash(format!("Failed to replace local database: {}", e))
    })?;

    info!(
        "Restored snapshot {} ({} bytes) to {}",
        entry.timestamp, entry.size_bytes, local_db_path
    );

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    fn create_test_db(path: &Path, rows: &[(i64, &str, &str)]) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                updated_at TEXT
            )",
        )
        .unwrap();
        for (id, name, updated_at) in rows {
            conn.execute(
                "INSERT OR REPLACE INTO items (id, name, updated_at) VALUES (?1, ?2, ?3)",
                rusqlite::params![id, name, updated_at],
            )
            .unwrap();
        }
    }

    fn make_file_stash_config(dir: &Path) -> StashConfig {
        let relay_path = dir.join("relay.sqlite");
        StashConfig {
            url: format!("file://{}", relay_path.display()),
            access_key_id: None,
            secret_access_key: None,
            region: None,
            endpoint: None,
        }
    }

    #[tokio::test]
    async fn test_snapshot_creates_files() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(
            &local_path,
            &[(1, "alpha", "2024-01-01"), (2, "beta", "2024-01-02")],
        );

        let config = make_file_stash_config(&snap_dir);

        let result = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        assert!(!result.timestamp.is_empty());
        assert!(result.size_bytes > 0);
        assert_eq!(result.tables.len(), 1);
        assert_eq!(result.tables[0].name, "items");
        assert_eq!(result.tables[0].row_count, 2);

        // Verify files were created in the snapshots/ directory
        let snapshots_dir = snap_dir.join("snapshots");
        assert!(snapshots_dir.exists());
        let entries: Vec<_> = std::fs::read_dir(&snapshots_dir)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entries.len(), 2); // .sqlite + .meta.json
    }

    #[tokio::test]
    async fn test_snapshot_dry_run() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);

        let config = make_file_stash_config(&snap_dir);

        let result = snapshot(&config, local_path.to_str().unwrap(), true)
            .await
            .unwrap();

        assert!(!result.timestamp.is_empty());
        assert!(result.size_bytes > 0);

        // No files should have been created
        let snapshots_dir = snap_dir.join("snapshots");
        assert!(!snapshots_dir.exists());
    }

    #[tokio::test]
    async fn test_list_snapshots_empty() {
        let dir = TempDir::new().unwrap();
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        let config = make_file_stash_config(&snap_dir);

        let list = list_snapshots(&config).await.unwrap();
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn test_snapshot_then_list() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);

        let config = make_file_stash_config(&snap_dir);

        let snap_result = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        let list = list_snapshots(&config).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].timestamp, snap_result.timestamp);
        assert_eq!(list[0].size_bytes, snap_result.size_bytes);
    }

    #[tokio::test]
    async fn test_snapshot_then_restore() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(
            &local_path,
            &[(1, "alpha", "2024-01-01"), (2, "beta", "2024-01-02")],
        );

        let config = make_file_stash_config(&snap_dir);

        // Take snapshot
        let snap_result = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        // Modify local database (simulate bad migration)
        let conn = Connection::open(&local_path).unwrap();
        conn.execute_batch(
            "DELETE FROM items; INSERT INTO items VALUES (99, 'corrupt', '2024-06-01');",
        )
        .unwrap();
        drop(conn);

        // Restore
        let restore_result = restore(
            &config,
            local_path.to_str().unwrap(),
            &snap_result.timestamp,
            false,
        )
        .await
        .unwrap();

        assert_eq!(restore_result.timestamp, snap_result.timestamp);

        // Verify local has original data
        let conn =
            Connection::open_with_flags(&local_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);

        let name: String = conn
            .query_row("SELECT name FROM items WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(name, "alpha");
    }

    #[tokio::test]
    async fn test_restore_dry_run() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[(1, "alpha", "2024-01-01")]);

        let config = make_file_stash_config(&snap_dir);

        let snap_result = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        // Corrupt local
        let conn = Connection::open(&local_path).unwrap();
        conn.execute_batch("DELETE FROM items").unwrap();
        drop(conn);

        // Dry run restore
        let result = restore(
            &config,
            local_path.to_str().unwrap(),
            &snap_result.timestamp,
            true,
        )
        .await
        .unwrap();

        assert_eq!(result.timestamp, snap_result.timestamp);

        // Local should still be empty (dry run)
        let conn =
            Connection::open_with_flags(&local_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_restore_no_snapshots() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[]);

        let config = make_file_stash_config(&snap_dir);

        let result = restore(
            &config,
            local_path.to_str().unwrap(),
            "2024-01-01T00:00:00Z",
            false,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SyncError::Stash(_)));
    }

    #[tokio::test]
    async fn test_restore_closest_timestamp() {
        let dir = TempDir::new().unwrap();
        let local_path = dir.path().join("local.sqlite");
        let snap_dir = dir.path().join("snap_store");
        std::fs::create_dir_all(&snap_dir).unwrap();

        create_test_db(&local_path, &[(1, "v1", "2024-01-01")]);
        let config = make_file_stash_config(&snap_dir);

        // Take first snapshot
        let snap1 = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        // Modify and take second snapshot
        let conn = Connection::open(&local_path).unwrap();
        conn.execute(
            "UPDATE items SET name = 'v2', updated_at = '2024-02-01' WHERE id = 1",
            [],
        )
        .unwrap();
        drop(conn);

        let snap2 = snapshot(&config, local_path.to_str().unwrap(), false)
            .await
            .unwrap();

        // Restore to first snapshot by exact timestamp
        let result = restore(
            &config,
            local_path.to_str().unwrap(),
            &snap1.timestamp,
            false,
        )
        .await
        .unwrap();

        assert_eq!(result.timestamp, snap1.timestamp);

        {
            let conn = Connection::open_with_flags(
                &local_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .unwrap();
            let name: String = conn
                .query_row("SELECT name FROM items WHERE id = 1", [], |r| r.get(0))
                .unwrap();
            assert_eq!(name, "v1");
        }

        // Restore to second snapshot
        let result = restore(
            &config,
            local_path.to_str().unwrap(),
            &snap2.timestamp,
            false,
        )
        .await
        .unwrap();

        assert_eq!(result.timestamp, snap2.timestamp);
    }

    #[test]
    fn test_snapshots_prefix() {
        let path = ObjectPath::from("path/to/relay.sqlite");
        let prefix = snapshots_prefix(&path);
        assert_eq!(prefix.as_ref(), "path/to/snapshots");

        let path = ObjectPath::from("relay.sqlite");
        let prefix = snapshots_prefix(&path);
        assert_eq!(prefix.as_ref(), "snapshots");
    }
}
