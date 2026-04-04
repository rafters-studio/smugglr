//! smugglr: SQLite replication across platforms.
//!
//! Core sync engine: content-hash diffing, delta computation, bidirectional
//! replication. Use as a library from Rust or compile to WASM for browser use.
//!
//! The `native` feature (default) enables platform-specific backends:
//! LocalDb (rusqlite), D1Client, PluginDataSource, broadcast, stash, daemon.
//! Without it, only the diff/sync engine and trait definitions are available.

pub mod batch;
pub mod config;
pub mod datasource;
pub mod diff;
pub mod error;
pub mod sync;
pub mod table;

#[cfg(feature = "native")]
pub mod broadcast;
#[cfg(feature = "native")]
pub mod daemon;
#[cfg(feature = "native")]
pub mod local;
#[cfg(feature = "native")]
pub mod plugin;
#[cfg(feature = "native")]
pub mod remote;
#[cfg(feature = "native")]
pub mod snapshot;
#[cfg(feature = "native")]
pub mod stash;

pub use config::Config;
pub use datasource::DataSource;
pub use diff::{diff_table, DiffStats, TableDiff};
pub use error::{Result, SyncError};
pub use sync::{pull_all, push_all, sync_all, DiffDetail, NoProgress, SyncProgress, SyncResult};

#[cfg(feature = "native")]
pub use local::LocalDb;
#[cfg(feature = "native")]
pub use plugin::PluginDataSource;
#[cfg(feature = "native")]
pub use remote::D1Client;
