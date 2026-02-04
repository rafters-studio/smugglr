//! Error types for d1-sync

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Local database error: {0}")]
    LocalDb(#[from] rusqlite::Error),

    #[error("Remote API error: {0}")]
    Remote(String),

    #[error("HTTP request error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("No primary key found for table: {0}")]
    NoPrimaryKey(String),

    /// Reserved for strict conflict detection mode
    #[allow(dead_code)]
    #[error("Sync conflict on table {table}, row {pk}: local={local_ts}, remote={remote_ts}")]
    Conflict {
        table: String,
        pk: String,
        local_ts: String,
        remote_ts: String,
    },

    #[error("D1 API error: {message} (code: {code:?})")]
    D1Api { message: String, code: Option<i64> },

    #[error("Config file not found: {0}")]
    ConfigNotFound(String),
}

pub type Result<T> = std::result::Result<T, SyncError>;
