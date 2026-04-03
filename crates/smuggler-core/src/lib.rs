//! smuggler-core: sync engine for bidirectional SQLite/D1 synchronization.
//!
//! This crate contains the core sync logic without any CLI dependencies.
//! It can be used as a library by other Rust applications that need
//! SQLite <-> D1 synchronization.

pub mod batch;
pub mod broadcast;
pub mod config;
pub mod daemon;
pub mod datasource;
pub mod diff;
pub mod error;
pub mod local;
pub mod plugin;
pub mod remote;
pub mod stash;
pub mod sync;
pub mod table;

pub use config::Config;
pub use datasource::DataSource;
pub use diff::{diff_table, DiffStats, TableDiff};
pub use error::{Result, SyncError};
pub use local::LocalDb;
pub use plugin::PluginDataSource;
pub use remote::D1Client;
pub use sync::{pull_all, push_all, sync_all, DiffDetail, NoProgress, SyncProgress, SyncResult};
