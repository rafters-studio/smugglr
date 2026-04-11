//! Configuration loading from TOML

use crate::error::{Result, SyncError};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Legacy flat D1 fields (use [target] section instead for new configs)
    pub cloudflare_account_id: Option<String>,
    pub cloudflare_api_token: Option<String>,
    pub database_id: Option<String>,

    /// Path to local SQLite database (optional - auto-detected from wrangler if not set)
    pub local_db: Option<String>,

    #[serde(default)]
    pub sync: SyncConfig,

    /// Optional stash config for S3-compatible relay sync
    pub stash: Option<StashConfig>,

    /// Target database configuration (sqlite or d1)
    pub target: Option<TargetConfig>,

    /// LAN broadcast sync configuration
    #[cfg(feature = "native")]
    pub broadcast: Option<crate::broadcast::BroadcastConfig>,
}

/// Target database configuration
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TargetConfig {
    /// Cloudflare D1 target (or any D1-compatible HTTP SQL endpoint)
    D1 {
        account_id: String,
        database_id: String,
        api_token: String,
        /// Custom endpoint URL (overrides the default Cloudflare D1 API).
        /// Use this to point at a DO bridge or other D1-compatible endpoint.
        url: Option<String>,
    },
    /// Local SQLite target
    Sqlite { database: String },
    /// External plugin adapter
    Plugin {
        /// Plugin name (resolved from ~/.smugglr/plugins/smuggler-{name} or $PATH)
        name: Option<String>,
        /// Explicit path to plugin binary
        path: Option<String>,
        /// Plugin-specific configuration passed to initialize
        #[serde(default)]
        config: HashMap<String, String>,
    },
}

/// Resolved target after merging legacy fields with [target] section
#[derive(Debug, Clone)]
pub enum ResolvedTarget {
    D1 {
        account_id: String,
        database_id: String,
        api_token: String,
        url: Option<String>,
    },
    Sqlite {
        database: String,
    },
    Plugin {
        path: PathBuf,
        name: String,
        config: HashMap<String, String>,
    },
}

/// Configuration for S3-compatible relay sync (stash/retrieve).
///
/// Supports S3, R2, GCS, Azure, and local filesystem URLs.
#[derive(Debug, Clone, Deserialize)]
pub struct StashConfig {
    /// Object store URL: s3://bucket/path/relay.sqlite, file:///local/path, etc.
    pub url: String,

    /// AWS access key ID (optional if using instance roles or env vars)
    pub access_key_id: Option<String>,

    /// AWS secret access key
    pub secret_access_key: Option<String>,

    /// AWS region (default: us-east-1)
    pub region: Option<String>,

    /// Custom endpoint for R2, MinIO, etc.
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncConfig {
    /// Tables to sync (empty = all non-excluded tables)
    #[serde(default)]
    pub tables: Vec<String>,

    /// Tables to always exclude
    #[serde(default = "default_exclude_tables")]
    pub exclude_tables: Vec<String>,

    /// Column name patterns to exclude from sync (glob-style: "*_embedding", "vector")
    #[serde(default)]
    pub exclude_columns: Vec<String>,

    /// Column used for timestamp-based change detection
    #[serde(default = "default_timestamp_column")]
    pub timestamp_column: String,

    /// How to resolve conflicts
    #[serde(default)]
    pub conflict_resolution: ConflictResolution,

    /// Maximum number of retry attempts for transient failures
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Initial delay in milliseconds before first retry (doubles each attempt)
    #[serde(default = "default_initial_retry_delay_ms")]
    pub initial_retry_delay_ms: u64,

    /// Maximum delay in milliseconds between retries (cap for exponential backoff)
    #[serde(default = "default_max_retry_delay_ms")]
    pub max_retry_delay_ms: u64,

    /// Backoff multiplier for exponential backoff (default: 2.0)
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,

    /// Maximum number of rows per batch for upsert operations
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum bytes per SQL statement (D1 has limits)
    #[serde(default = "default_max_statement_bytes")]
    pub max_statement_bytes: usize,
}

fn default_batch_size() -> usize {
    100
}

fn default_max_statement_bytes() -> usize {
    // D1 has a 100KB limit per statement, use 90KB to be safe
    90 * 1024
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            tables: Vec::new(),
            exclude_tables: default_exclude_tables(),
            exclude_columns: Vec::new(),
            timestamp_column: default_timestamp_column(),
            conflict_resolution: ConflictResolution::default(),
            max_retries: default_max_retries(),
            initial_retry_delay_ms: default_initial_retry_delay_ms(),
            max_retry_delay_ms: default_max_retry_delay_ms(),
            backoff_multiplier: default_backoff_multiplier(),
            batch_size: default_batch_size(),
            max_statement_bytes: default_max_statement_bytes(),
        }
    }
}

impl SyncConfig {
    /// Check if a column name should be excluded from sync.
    ///
    /// Supports simple glob patterns:
    /// - `*_embedding` matches columns ending with `_embedding`
    /// - `embedding_*` matches columns starting with `embedding_`
    /// - `*embed*` matches columns containing `embed`
    /// - `vector` matches the exact column name `vector`
    #[allow(dead_code)]
    pub fn should_exclude_column(&self, column: &str) -> bool {
        self.exclude_columns
            .iter()
            .any(|pattern| column_glob_match(pattern, column))
    }

    /// Filter a list of column names, removing excluded ones.
    #[allow(dead_code)]
    pub fn filter_columns<'a>(&self, columns: &[&'a str]) -> Vec<&'a str> {
        columns
            .iter()
            .filter(|c| !self.should_exclude_column(c))
            .copied()
            .collect()
    }
}

/// Check if a column name matches any exclusion pattern in the given list.
pub fn column_excluded(column: &str, patterns: &[String]) -> bool {
    patterns
        .iter()
        .any(|pattern| column_glob_match(pattern, column))
}

/// Simple glob matching for column name patterns.
///
/// Supports `*` at start, end, or both. No `?` or character classes --
/// these patterns are intentionally simple for config ergonomics.
fn column_glob_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let starts_star = pattern.starts_with('*');
    let ends_star = pattern.ends_with('*');

    match (starts_star, ends_star) {
        (false, false) => {
            // Exact match: "vector"
            pattern == value
        }
        (true, true) if pattern.len() >= 2 => {
            // Contains: "*embed*"
            let inner = &pattern[1..pattern.len() - 1];
            value.contains(inner)
        }
        (true, false) => {
            // Suffix: "*_embedding"
            value.ends_with(&pattern[1..])
        }
        (false, true) => {
            // Prefix: "embedding_*"
            value.starts_with(&pattern[..pattern.len() - 1])
        }
        _ => false,
    }
}

/// Configuration for batch operations
#[derive(Debug, Clone, Copy)]
pub struct BatchConfig {
    /// Maximum number of rows per batch
    pub batch_size: usize,
    /// Maximum bytes per SQL statement
    pub max_statement_bytes: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            max_statement_bytes: default_max_statement_bytes(),
        }
    }
}

impl BatchConfig {
    /// Create BatchConfig from SyncConfig
    #[allow(dead_code)]
    pub fn from_sync_config(sync: &SyncConfig) -> Self {
        Self {
            batch_size: sync.batch_size,
            max_statement_bytes: sync.max_statement_bytes,
        }
    }
}

fn default_max_retries() -> u32 {
    5
}

fn default_initial_retry_delay_ms() -> u64 {
    100 // per issue #3 spec
}

fn default_max_retry_delay_ms() -> u64 {
    30_000 // per issue #3 spec
}

fn default_backoff_multiplier() -> f64 {
    2.0
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
    /// UUIDv7 primary key with higher embedded timestamp wins.
    /// Falls back to NewerWins when PKs are not valid UUIDv7.
    UuidV7Wins,
}

/// Retry configuration for D1 API calls
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial delay in milliseconds before first retry
    pub initial_delay_ms: u64,
    /// Maximum delay in milliseconds (cap for exponential backoff)
    pub max_delay_ms: u64,
    /// Backoff multiplier for exponential backoff
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            initial_delay_ms: default_initial_retry_delay_ms(),
            max_delay_ms: default_max_retry_delay_ms(),
            backoff_multiplier: default_backoff_multiplier(),
        }
    }
}

#[allow(dead_code)]
impl RetryConfig {
    /// Create RetryConfig from SyncConfig settings.
    ///
    /// Validates that backoff_multiplier >= 1.0 (clamps invalid values).
    pub fn from_sync_config(sync: &SyncConfig) -> Self {
        Self {
            max_retries: sync.max_retries.min(100), // cap at reasonable max
            initial_delay_ms: sync.initial_retry_delay_ms,
            max_delay_ms: sync.max_retry_delay_ms,
            // Ensure multiplier is at least 1.0 to avoid zero/negative delays
            backoff_multiplier: sync.backoff_multiplier.max(1.0),
        }
    }

    /// Calculate delay for a given attempt (0-indexed)
    pub fn delay_for_attempt(&self, attempt: u32) -> u64 {
        let delay = self.initial_delay_ms as f64 * self.backoff_multiplier.powi(attempt as i32);
        (delay as u64).min(self.max_delay_ms)
    }
}

impl Config {
    /// Parse config from a TOML string without filesystem access.
    ///
    /// Skips local_db auto-detection and target validation.
    /// Use this for WASM or library consumers that construct config programmatically.
    pub fn from_toml_str(content: &str) -> Result<Self> {
        toml::from_str(content).map_err(|e| SyncError::Config(e.to_string()))
    }

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
            match detect_local_db() {
                Ok(path) => config.local_db = Some(path),
                Err(e) => {
                    // local_db is always required (it's the source database)
                    return Err(e);
                }
            }
        }

        // Target resolution is deferred to command dispatch: some commands
        // (broadcast, stash, retrieve, snapshot, restore) never touch the
        // target and must not fail at load time if the plugin binary is
        // missing. See crates/smugglr/src/main.rs for where target resolution
        // actually runs.
        Ok(config)
    }

    /// Resolve the target from either [target] section or legacy flat fields.
    pub fn resolve_target(&self) -> Result<ResolvedTarget> {
        if let Some(ref target) = self.target {
            return Ok(match target {
                TargetConfig::D1 {
                    account_id,
                    database_id,
                    api_token,
                    url,
                } => resolve_d1_plugin_target(account_id, database_id, api_token, url.as_deref())?,
                TargetConfig::Sqlite { database } => ResolvedTarget::Sqlite {
                    database: database.clone(),
                },
                TargetConfig::Plugin { name, path, config } => {
                    let (resolved_path, resolved_name) = match (name, path) {
                        (_, Some(p)) => {
                            let pb = PathBuf::from(p);
                            let n = pb
                                .file_name()
                                .map(|f| f.to_string_lossy().into_owned())
                                .unwrap_or_else(|| p.clone());
                            (pb, n)
                        }
                        (Some(_n), None) => {
                            #[cfg(feature = "native")]
                            {
                                let pb = crate::plugin::resolve_plugin_path(_n)?;
                                (pb, _n.clone())
                            }
                            #[cfg(not(feature = "native"))]
                            {
                                return Err(SyncError::Config(
                                    "Plugin targets require the 'native' feature".into(),
                                ));
                            }
                        }
                        (None, None) => {
                            return Err(SyncError::Config(
                                "Plugin target requires either 'name' or 'path'".into(),
                            ));
                        }
                    };
                    ResolvedTarget::Plugin {
                        path: resolved_path,
                        name: resolved_name,
                        config: config.clone(),
                    }
                }
            });
        }

        // Fall back to legacy flat fields
        match (
            &self.cloudflare_account_id,
            &self.database_id,
            &self.cloudflare_api_token,
        ) {
            (Some(account_id), Some(database_id), Some(api_token)) => {
                resolve_d1_plugin_target(account_id, database_id, api_token, None)
            }
            _ => Err(SyncError::Config(
                "No target configured. Add a [target] section or set cloudflare_account_id, database_id, and cloudflare_api_token.".to_string()
            )),
        }
    }

    /// Get the local database path (guaranteed to be Some after load)
    pub fn local_db_path(&self) -> &str {
        self.local_db
            .as_deref()
            .expect("local_db should be set after load")
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

    /// Get retry configuration from sync settings
    #[allow(dead_code)]
    pub fn retry_config(&self) -> RetryConfig {
        RetryConfig::from_sync_config(&self.sync)
    }
}

/// Build a `ResolvedTarget::Plugin` for the http-sql plugin with a d1 profile.
///
/// Both the explicit `[target] type = "d1"` branch and the legacy flat-fields branch
/// funnel through here. The `ResolvedTarget::D1` variant is preserved in the enum for
/// issue #83 which removes it entirely; at runtime, D1 config always resolves to a plugin.
fn resolve_d1_plugin_target(
    account_id: &str,
    database_id: &str,
    api_token: &str,
    url: Option<&str>,
) -> Result<ResolvedTarget> {
    let mut plugin_config = HashMap::new();
    plugin_config.insert("profile".to_string(), "d1".to_string());
    plugin_config.insert("account_id".to_string(), account_id.to_string());
    plugin_config.insert("database_id".to_string(), database_id.to_string());
    plugin_config.insert("api_token".to_string(), api_token.to_string());
    if let Some(u) = url {
        plugin_config.insert("url".to_string(), u.to_string());
    }

    let plugin_path = resolve_http_sql_plugin_path()?;

    Ok(ResolvedTarget::Plugin {
        path: plugin_path,
        name: "smugglr-http-sql".to_string(),
        config: plugin_config,
    })
}

/// Resolve the path to the smugglr-http-sql plugin binary.
///
/// Under `cfg(test)` a placeholder path is returned so unit tests can assert on
/// config synthesis without requiring the binary to be installed.
fn resolve_http_sql_plugin_path() -> Result<PathBuf> {
    #[cfg(test)]
    {
        Ok(PathBuf::from("/fake/smugglr-http-sql"))
    }
    #[cfg(all(not(test), feature = "native"))]
    {
        crate::plugin::resolve_plugin_path("http-sql")
            .map_err(|_| SyncError::Config("d1 target requires the smugglr-http-sql plugin".into()))
    }
    #[cfg(all(not(test), not(feature = "native")))]
    {
        Err(SyncError::Config(
            "d1 target requires the 'native' feature".into(),
        ))
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
            if best_file
                .as_ref()
                .map_or(true, |(_, best_size)| size > *best_size)
            {
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

    fn test_config_d1() -> Config {
        Config {
            cloudflare_account_id: Some("test_acct".into()),
            cloudflare_api_token: Some("test_token".into()),
            database_id: Some("test_db".into()),
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: None,
            broadcast: None,
        }
    }

    fn test_config_sqlite_target() -> Config {
        Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some("source.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: Some(TargetConfig::Sqlite {
                database: "backup.db".into(),
            }),
            broadcast: None,
        }
    }

    /// Assert that `target` is a `ResolvedTarget::Plugin` pointing at the
    /// http-sql plugin with a synthesized d1 profile config. Used by every
    /// test that exercises the D1-to-plugin routing in `resolve_target`.
    fn assert_d1_plugin(
        target: &ResolvedTarget,
        account_id: &str,
        database_id: &str,
        api_token: &str,
        url: Option<&str>,
    ) {
        let ResolvedTarget::Plugin { name, config, .. } = target else {
            panic!("expected plugin target, got {:?}", target);
        };
        assert_eq!(name, "smugglr-http-sql");
        assert_eq!(config.get("profile").map(String::as_str), Some("d1"));
        assert_eq!(
            config.get("account_id").map(String::as_str),
            Some(account_id)
        );
        assert_eq!(
            config.get("database_id").map(String::as_str),
            Some(database_id)
        );
        assert_eq!(config.get("api_token").map(String::as_str), Some(api_token));
        assert_eq!(config.get("url").map(String::as_str), url);
    }

    #[test]
    fn test_default_excludes() {
        let config = test_config_d1();

        assert!(!config.should_sync_table("sqlite_sequence"));
        assert!(!config.should_sync_table("_cf_KV"));
        assert!(config.should_sync_table("abilities"));
    }

    #[test]
    fn test_specific_tables() {
        let mut config = test_config_d1();
        config.sync.tables = vec!["abilities".into(), "talents".into()];

        assert!(config.should_sync_table("abilities"));
        assert!(config.should_sync_table("talents"));
        assert!(!config.should_sync_table("disciplines"));
    }

    #[test]
    fn test_resolve_target_legacy_d1() {
        let config = test_config_d1();
        let target = config.resolve_target().unwrap();
        assert_d1_plugin(&target, "test_acct", "test_db", "test_token", None);
    }

    #[test]
    fn test_resolve_target_sqlite() {
        let config = test_config_sqlite_target();
        let target = config.resolve_target().unwrap();
        match target {
            ResolvedTarget::Sqlite { database } => assert_eq!(database, "backup.db"),
            _ => panic!("expected SQLite target"),
        }
    }

    #[test]
    fn test_resolve_target_explicit_d1() {
        let config = Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: Some(TargetConfig::D1 {
                account_id: "acct".into(),
                database_id: "db".into(),
                api_token: "tok".into(),
                url: None,
            }),
            broadcast: None,
        };
        let target = config.resolve_target().unwrap();
        assert_d1_plugin(&target, "acct", "db", "tok", None);
    }

    #[test]
    fn test_resolve_target_explicit_overrides_legacy() {
        let config = Config {
            cloudflare_account_id: Some("old_acct".into()),
            cloudflare_api_token: Some("old_token".into()),
            database_id: Some("old_db".into()),
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: Some(TargetConfig::Sqlite {
                database: "backup.db".into(),
            }),
            broadcast: None,
        };
        let target = config.resolve_target().unwrap();
        assert!(matches!(target, ResolvedTarget::Sqlite { .. }));
    }

    #[test]
    fn test_resolve_target_no_config() {
        let config = Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: None,
            broadcast: None,
        };
        assert!(config.resolve_target().is_err());
    }

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_delay_ms, 100);
        assert_eq!(config.max_delay_ms, 30_000);
        assert_eq!(config.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_backoff_multiplier_clamped() {
        let sync = SyncConfig {
            backoff_multiplier: 0.5,
            ..Default::default()
        };
        let retry = RetryConfig::from_sync_config(&sync);
        assert_eq!(retry.backoff_multiplier, 1.0);
    }

    #[test]
    fn test_max_retries_capped() {
        let sync = SyncConfig {
            max_retries: 1000,
            ..Default::default()
        };
        let retry = RetryConfig::from_sync_config(&sync);
        assert_eq!(retry.max_retries, 100);
    }

    #[test]
    fn test_retry_config_from_sync_config() {
        let sync = SyncConfig {
            max_retries: 3,
            initial_retry_delay_ms: 500,
            max_retry_delay_ms: 30000,
            backoff_multiplier: 1.5,
            ..Default::default()
        };
        let retry = RetryConfig::from_sync_config(&sync);
        assert_eq!(retry.max_retries, 3);
        assert_eq!(retry.initial_delay_ms, 500);
        assert_eq!(retry.max_delay_ms, 30000);
        assert_eq!(retry.backoff_multiplier, 1.5);
    }

    #[test]
    fn test_delay_for_attempt_exponential() {
        let config = RetryConfig {
            max_retries: 5,
            initial_delay_ms: 1000,
            max_delay_ms: 60000,
            backoff_multiplier: 2.0,
        };

        assert_eq!(config.delay_for_attempt(0), 1000);
        assert_eq!(config.delay_for_attempt(1), 2000);
        assert_eq!(config.delay_for_attempt(2), 4000);
        assert_eq!(config.delay_for_attempt(3), 8000);
        assert_eq!(config.delay_for_attempt(4), 16000);
    }

    #[test]
    fn test_delay_capped_at_max() {
        let config = RetryConfig {
            max_retries: 10,
            initial_delay_ms: 1000,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        };

        assert_eq!(config.delay_for_attempt(0), 1000);
        assert_eq!(config.delay_for_attempt(1), 2000);
        assert_eq!(config.delay_for_attempt(2), 4000);
        assert_eq!(config.delay_for_attempt(3), 5000);
        assert_eq!(config.delay_for_attempt(4), 5000);
    }

    #[test]
    fn test_config_retry_config() {
        let mut config = test_config_d1();
        config.sync.max_retries = 10;
        config.sync.initial_retry_delay_ms = 500;

        let retry = config.retry_config();
        assert_eq!(retry.max_retries, 10);
        assert_eq!(retry.initial_delay_ms, 500);
    }

    #[test]
    fn test_parse_toml_sqlite_target() {
        let toml_str = r#"
local_db = "game.db"

[target]
type = "sqlite"
database = "backup.db"

[sync]
tables = ["abilities"]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(matches!(config.target, Some(TargetConfig::Sqlite { .. })));
        let target = config.resolve_target().unwrap();
        match target {
            ResolvedTarget::Sqlite { database } => assert_eq!(database, "backup.db"),
            _ => panic!("expected sqlite"),
        }
    }

    #[test]
    fn test_parse_toml_d1_target() {
        let toml_str = r#"
local_db = "game.db"

[target]
type = "d1"
account_id = "acct123"
database_id = "db456"
api_token = "tok789"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let target = config.resolve_target().unwrap();
        assert_d1_plugin(&target, "acct123", "db456", "tok789", None);
    }

    #[test]
    fn test_parse_toml_legacy_d1() {
        let toml_str = r#"
cloudflare_account_id = "acct"
cloudflare_api_token = "tok"
database_id = "db"
local_db = "game.db"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.target.is_none());
        let target = config.resolve_target().unwrap();
        assert_d1_plugin(&target, "acct", "db", "tok", None);
    }

    // -- Column exclusion tests --

    #[test]
    fn test_column_exclusion_pattern_matching() {
        // Suffix: "*_embedding" matches "title_embedding" but not "embedding_title"
        let sync = SyncConfig {
            exclude_columns: vec!["*_embedding".to_string()],
            ..Default::default()
        };
        assert!(sync.should_exclude_column("title_embedding"));
        assert!(sync.should_exclude_column("content_embedding"));
        assert!(!sync.should_exclude_column("embedding_title"));
        assert!(!sync.should_exclude_column("title"));

        // Prefix: "embedding_*" matches "embedding_title" but not "title_embedding"
        let sync = SyncConfig {
            exclude_columns: vec!["embedding_*".to_string()],
            ..Default::default()
        };
        assert!(sync.should_exclude_column("embedding_title"));
        assert!(sync.should_exclude_column("embedding_content"));
        assert!(!sync.should_exclude_column("title_embedding"));

        // Exact: "vector" matches "vector" but not "vector_data" or "my_vector"
        let sync = SyncConfig {
            exclude_columns: vec!["vector".to_string()],
            ..Default::default()
        };
        assert!(sync.should_exclude_column("vector"));
        assert!(!sync.should_exclude_column("vector_data"));
        assert!(!sync.should_exclude_column("my_vector"));
    }

    #[test]
    fn test_column_exclusion_contains_pattern() {
        let sync = SyncConfig {
            exclude_columns: vec!["*embed*".to_string()],
            ..Default::default()
        };
        assert!(sync.should_exclude_column("title_embedding"));
        assert!(sync.should_exclude_column("embedding_title"));
        assert!(sync.should_exclude_column("embed"));
        assert!(!sync.should_exclude_column("vector"));
    }

    #[test]
    fn test_column_exclusion_multiple_patterns() {
        let sync = SyncConfig {
            exclude_columns: vec![
                "*_embedding".to_string(),
                "vector".to_string(),
                "blob_*".to_string(),
            ],
            ..Default::default()
        };
        assert!(sync.should_exclude_column("title_embedding"));
        assert!(sync.should_exclude_column("vector"));
        assert!(sync.should_exclude_column("blob_data"));
        assert!(!sync.should_exclude_column("title"));
        assert!(!sync.should_exclude_column("name"));
    }

    #[test]
    fn test_empty_exclusion_syncs_all() {
        let sync = SyncConfig::default();
        assert!(!sync.should_exclude_column("anything"));
        assert!(!sync.should_exclude_column("title_embedding"));
        assert!(!sync.should_exclude_column("vector"));
    }

    #[test]
    fn test_column_exclusion_wildcard_all() {
        let sync = SyncConfig {
            exclude_columns: vec!["*".to_string()],
            ..Default::default()
        };
        assert!(sync.should_exclude_column("anything"));
    }

    #[test]
    fn test_filter_columns() {
        let sync = SyncConfig {
            exclude_columns: vec!["*_embedding".to_string(), "vector".to_string()],
            ..Default::default()
        };
        let cols = vec!["id", "name", "title_embedding", "vector", "description"];
        let filtered = sync.filter_columns(&cols);
        assert_eq!(filtered, vec!["id", "name", "description"]);
    }

    #[test]
    fn test_column_excluded_standalone() {
        let patterns = vec!["*_embedding".to_string(), "blob_*".to_string()];
        assert!(column_excluded("title_embedding", &patterns));
        assert!(column_excluded("blob_data", &patterns));
        assert!(!column_excluded("name", &patterns));
        assert!(!column_excluded("id", &patterns));
    }

    #[test]
    fn test_parse_toml_with_exclude_columns() {
        let toml_str = r#"
cloudflare_account_id = "acct"
cloudflare_api_token = "tok"
database_id = "db"
local_db = "game.db"

[sync]
exclude_columns = ["*_embedding", "vector"]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.sync.exclude_columns.len(), 2);
        assert!(config.sync.should_exclude_column("title_embedding"));
        assert!(config.sync.should_exclude_column("vector"));
        assert!(!config.sync.should_exclude_column("name"));
    }

    #[test]
    fn test_parse_toml_plugin_target_with_name() {
        let toml_str = r#"
local_db = "game.db"

[target]
type = "plugin"
name = "turso"

[target.config]
url = "libsql://my-db.turso.io"
auth_token = "tok123"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        match &config.target {
            Some(TargetConfig::Plugin { name, path, config }) => {
                assert_eq!(name.as_deref(), Some("turso"));
                assert!(path.is_none());
                assert_eq!(config.get("url").unwrap(), "libsql://my-db.turso.io");
                assert_eq!(config.get("auth_token").unwrap(), "tok123");
            }
            _ => panic!("expected plugin target"),
        }
    }

    #[test]
    fn test_parse_toml_plugin_target_with_path() {
        let toml_str = r#"
local_db = "game.db"

[target]
type = "plugin"
path = "/usr/local/bin/smugglr-turso"

[target.config]
url = "libsql://my-db.turso.io"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        match &config.target {
            Some(TargetConfig::Plugin { name, path, config }) => {
                assert!(name.is_none());
                assert_eq!(path.as_deref(), Some("/usr/local/bin/smugglr-turso"));
                assert_eq!(config.get("url").unwrap(), "libsql://my-db.turso.io");
            }
            _ => panic!("expected plugin target"),
        }
    }

    #[test]
    fn test_parse_toml_plugin_target_empty_config() {
        let toml_str = r#"
local_db = "game.db"

[target]
type = "plugin"
path = "/usr/local/bin/smuggler-custom"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        match &config.target {
            Some(TargetConfig::Plugin { config, .. }) => {
                assert!(config.is_empty());
            }
            _ => panic!("expected plugin target"),
        }
    }

    #[test]
    fn test_resolve_plugin_target_with_path() {
        let config = Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: Some(TargetConfig::Plugin {
                name: None,
                path: Some("/usr/local/bin/smugglr-turso".into()),
                config: HashMap::new(),
            }),
            broadcast: None,
        };
        let target = config.resolve_target().unwrap();
        match target {
            ResolvedTarget::Plugin { path, name, .. } => {
                assert_eq!(path, PathBuf::from("/usr/local/bin/smugglr-turso"));
                assert_eq!(name, "smugglr-turso");
            }
            _ => panic!("expected plugin target"),
        }
    }

    #[test]
    fn test_resolve_plugin_target_no_name_or_path() {
        let config = Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some("test.db".into()),
            sync: SyncConfig::default(),
            stash: None,
            target: Some(TargetConfig::Plugin {
                name: None,
                path: None,
                config: HashMap::new(),
            }),
            broadcast: None,
        };
        assert!(config.resolve_target().is_err());
    }
}
