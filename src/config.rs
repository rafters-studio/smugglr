//! Configuration loading from TOML

use crate::error::{Result, SyncError};
use serde::Deserialize;
use std::path::Path;
use tracing::{debug, info};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub cloudflare_account_id: String,
    pub cloudflare_api_token: String,
    pub database_id: String,

    /// Path to local SQLite database (optional - auto-detected from wrangler if not set)
    pub local_db: Option<String>,

    #[serde(default)]
    pub sync: SyncConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncConfig {
    /// Tables to sync (empty = all non-excluded tables)
    #[serde(default)]
    pub tables: Vec<String>,

    /// Tables to always exclude
    #[serde(default = "default_exclude_tables")]
    pub exclude_tables: Vec<String>,

    /// Column used for timestamp-based change detection
    #[serde(default = "default_timestamp_column")]
    pub timestamp_column: String,

    /// How to resolve conflicts
    #[serde(default)]
    pub conflict_resolution: ConflictResolution,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            tables: Vec::new(),
            exclude_tables: default_exclude_tables(),
            timestamp_column: default_timestamp_column(),
            conflict_resolution: ConflictResolution::default(),
        }
    }
}

fn default_exclude_tables() -> Vec<String> {
    vec![
        "sqlite_sequence".to_string(),
        "_cf_KV".to_string(),
        "__drizzle_migrations".to_string(),
    ]
}

fn default_timestamp_column() -> String {
    "updated_at".to_string()
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
pub enum ConflictResolution {
    /// Local changes always win
    #[default]
    LocalWins,
    /// Remote changes always win
    RemoteWins,
    /// Newer timestamp wins
    NewerWins,
}

impl Config {
    /// Load config from a TOML file
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(SyncError::ConfigNotFound(path.display().to_string()));
        }

        let content = std::fs::read_to_string(path)?;
        let mut config: Config =
            toml::from_str(&content).map_err(|e| SyncError::Config(e.to_string()))?;

        // Auto-detect local_db if not specified
        if config.local_db.is_none() {
            config.local_db = Some(detect_local_db()?);
        }

        Ok(config)
    }

    /// Get the local database path (guaranteed to be Some after load)
    pub fn local_db_path(&self) -> &str {
        self.local_db.as_deref().expect("local_db should be set after load")
    }

    /// Check if a table should be synced
    pub fn should_sync_table(&self, table: &str) -> bool {
        // Always exclude certain tables
        if self.sync.exclude_tables.iter().any(|t| t == table) {
            return false;
        }

        // If specific tables are configured, only sync those
        if !self.sync.tables.is_empty() {
            return self.sync.tables.iter().any(|t| t == table);
        }

        // Otherwise sync all non-excluded tables
        true
    }
}

/// Auto-detect the local D1 database from wrangler's state directory
fn detect_local_db() -> Result<String> {
    let miniflare_dir = Path::new(".wrangler/state/v3/d1/miniflare-D1DatabaseObject");

    if !miniflare_dir.exists() {
        return Err(SyncError::Config(
            "No .wrangler/state/v3/d1/miniflare-D1DatabaseObject directory found. Run 'wrangler dev' first.".to_string()
        ));
    }

    // Find the largest non-empty sqlite file (the active database)
    let mut best_file: Option<(std::path::PathBuf, u64)> = None;

    for entry in std::fs::read_dir(miniflare_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().is_some_and(|ext| ext == "sqlite") {
            let metadata = std::fs::metadata(&path)?;
            let size = metadata.len();

            // Skip empty files
            if size == 0 {
                continue;
            }

            debug!("Found sqlite: {} ({} bytes)", path.display(), size);

            // Prefer larger files (more data = more likely the active one)
            if best_file.as_ref().is_none_or(|(_, best_size)| size > *best_size) {
                best_file = Some((path, size));
            }
        }
    }

    match best_file {
        Some((path, size)) => {
            let path_str = path.display().to_string();
            info!(
                "Auto-detected local database: {} ({} bytes)",
                path_str, size
            );
            Ok(path_str)
        }
        None => Err(SyncError::Config(
            "No non-empty sqlite files found in wrangler state. Run 'wrangler dev' and make some queries first.".to_string()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_excludes() {
        let config = Config {
            cloudflare_account_id: "test".into(),
            cloudflare_api_token: "test".into(),
            database_id: "test".into(),
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
        };

        assert!(!config.should_sync_table("sqlite_sequence"));
        assert!(!config.should_sync_table("_cf_KV"));
        assert!(config.should_sync_table("abilities"));
    }

    #[test]
    fn test_specific_tables() {
        let config = Config {
            cloudflare_account_id: "test".into(),
            cloudflare_api_token: "test".into(),
            database_id: "test".into(),
            local_db: Some("test.db".into()),
            sync: SyncConfig {
                tables: vec!["abilities".into(), "talents".into()],
                ..Default::default()
            },
        };

        assert!(config.should_sync_table("abilities"));
        assert!(config.should_sync_table("talents"));
        assert!(!config.should_sync_table("disciplines"));
    }
}
