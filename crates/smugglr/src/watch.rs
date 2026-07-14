//! Watch daemon for continuous sync
//!
//! Runs `sync_all` on a configurable interval with a `last_sync` cursor
//! persisted in the config file. First tick always does a full sync;
//! subsequent ticks reuse the same engine (the cursor is informational
//! for the user, not an optimization gate in v1).

use crate::output::{OutputFormat, WatchTickOutput};
use smugglr_core::config::{Config, ResolvedTarget};
use smugglr_core::daemon::{
    is_transient_error, now_iso8601, pid_lock_path, update_last_sync, PidLock,
};
use smugglr_core::error::Result;
use smugglr_core::local::LocalDb;
use smugglr_core::plugin::PluginDataSource;
use smugglr_core::sync::{sync_all, NoProgress};
use std::path::Path;
use tokio::signal;
use tokio::time::{self, Duration};
use tracing::{error, info, warn};

/// Run the watch daemon loop.
pub async fn run_watch(
    config: &Config,
    config_path: &Path,
    target: ResolvedTarget,
    interval_secs: u64,
    dry_run: bool,
    fmt: OutputFormat,
) -> Result<()> {
    let pid_path = pid_lock_path(config_path);
    let _pid_lock = PidLock::acquire(&pid_path)?;

    info!(
        "Starting watch daemon (interval: {}s, dry_run: {})",
        interval_secs, dry_run
    );

    // Start plugin once before the loop to avoid respawning every tick
    let plugin = if let ResolvedTarget::Plugin {
        ref path,
        ref name,
        config: ref plugin_config,
    } = target
    {
        Some(PluginDataSource::start(path, name, plugin_config).await?)
    } else {
        None
    };

    // `tokio::time::interval` panics on a zero period; clamp defensively so a
    // `--interval 0` (or any path that bypasses clap's range validation) cannot
    // crash the daemon.
    let interval_secs = interval_secs.max(1);

    let mut tick_count: u64 = 0;
    let mut interval = time::interval(Duration::from_secs(interval_secs));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                tick_count += 1;
                info!("Watch tick #{}", tick_count);

                let result = match &target {
                    ResolvedTarget::Sqlite { .. } => {
                        let local = open_local(config, dry_run)?;
                        // Mirrors run_sync/run_pull's dry-run-readonly convention: in
                        // dry-run nothing is written (sync_all bails before
                        // transfer_rows), so the target does not need write access.
                        let target_db = crate::TargetSource::open(&target, !dry_run).await?;
                        sync_all(&local, &target_db, config, None, dry_run, &NoProgress).await
                    }
                    ResolvedTarget::Plugin { .. } => {
                        let local = open_local(config, dry_run)?;
                        let plugin = plugin.as_ref().expect("plugin initialized before loop");
                        sync_all(&local, plugin, config, None, dry_run, &NoProgress).await
                    }
                };

                match result {
                    Ok(results) => {
                        let total_pushed: usize = results.iter().map(|r| r.rows_pushed).sum();
                        let total_pulled: usize = results.iter().map(|r| r.rows_pulled).sum();

                        if fmt == OutputFormat::Json {
                            let out = WatchTickOutput::from_results(tick_count, &results, dry_run);
                            println!("{}", serde_json::to_string(&out).expect("WatchTickOutput is always serializable"));
                        } else if total_pushed > 0 || total_pulled > 0 {
                            info!(
                                "Tick #{}: {} pushed, {} pulled across {} tables",
                                tick_count, total_pushed, total_pulled, results.len()
                            );
                        } else {
                            info!("Tick #{}: no changes", tick_count);
                        }

                        if !dry_run {
                            let ts = now_iso8601();
                            if let Err(e) = update_last_sync(config_path, &ts) {
                                warn!("Failed to update last_sync: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        if is_transient_error(&e) {
                            warn!("Transient error on tick #{}: {}. Will retry next tick.", tick_count, e);
                            if fmt == OutputFormat::Json {
                                let out = WatchTickOutput::from_error(tick_count, &e.to_string());
                                println!("{}", serde_json::to_string(&out).expect("WatchTickOutput is always serializable"));
                            }
                        } else {
                            error!("Fatal error on tick #{}: {}", tick_count, e);
                            if fmt == OutputFormat::Json {
                                // In JSON mode the WatchTickOutput error line is the single
                                // failure record for this stream. Exit directly with the
                                // SyncError's code rather than returning Err, which would make
                                // main's `exit_json_error` emit a second, differently-shaped
                                // ErrorOutput line for the same failure.
                                let out = WatchTickOutput::from_error(tick_count, &e.to_string());
                                println!("{}", serde_json::to_string(&out).expect("WatchTickOutput is always serializable"));
                                std::process::exit(e.exit_code());
                            }
                            return Err(e);
                        }
                    }
                }
            }
            _ = signal::ctrl_c() => {
                info!("Received shutdown signal. Stopping watch daemon.");
                break;
            }
        }
    }

    info!("Watch daemon stopped after {} ticks", tick_count);
    Ok(())
}

/// Open the local DB in the same dry-run-readonly mode `run_sync`/`run_pull`
/// use (main.rs): read-only when `dry_run` is true, read-write otherwise.
///
/// In dry-run, `sync_all` never reaches a writer -- `push_table`/`pull_table`
/// both return before `transfer_rows`, the sole caller of `upsert_rows` -- so
/// a read-write connection asks for write access dry-run never uses, and it
/// diverges from the dry-run-readonly convention `run_sync`/`run_pull`
/// establish (#217).
fn open_local(config: &Config, dry_run: bool) -> Result<LocalDb> {
    if dry_run {
        LocalDb::open_readonly(config.local_db_path())
    } else {
        LocalDb::open(config.local_db_path())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smugglr_core::config::SyncConfig;
    use smugglr_core::datasource::DataSource;
    use std::collections::HashMap;

    /// Create a fresh SQLite file at `path` with one table, `t (id, v)`.
    ///
    /// `LocalDb`/`TargetSource` intentionally expose no raw-SQL or
    /// table-creation API (production code never needs one), so the fixture
    /// is built with a direct `rusqlite` connection instead.
    fn make_db_with_table(path: &std::path::Path) {
        let conn = rusqlite::Connection::open(path).expect("create fixture db");
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .expect("create fixture table");
    }

    fn sample_row() -> HashMap<String, serde_json::Value> {
        let mut row = HashMap::new();
        row.insert("id".to_string(), serde_json::json!(1));
        row.insert("v".to_string(), serde_json::json!("x"));
        row
    }

    fn config_for(db_path: &std::path::Path) -> Config {
        Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some(db_path.to_string_lossy().into_owned()),
            sync: SyncConfig::default(),
            stash: None,
            target: None,
            broadcast: None,
        }
    }

    /// Regression for #217 (local DB): a dry-run watch tick's local
    /// connection must be genuinely read-only, not merely "unwritten in
    /// practice." Before the fix, `open_local`'s equivalent (the raw
    /// `LocalDb::open(...)` call in watch.rs) opened read-write
    /// unconditionally, ignoring `dry_run`. On that code, a write attempt
    /// against the dry-run connection succeeds -- the no-writes guarantee
    /// lived only in caller discipline (`sync_all` bailing before
    /// `transfer_rows`), not in the connection itself. This test fails
    /// against that code (the first assertion below) and passes once
    /// `open_local` opens read-only in dry-run, because SQLite itself then
    /// rejects the write.
    #[tokio::test]
    async fn open_local_dry_run_connection_rejects_writes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("local.db");
        make_db_with_table(&db_path);
        let config = config_for(&db_path);

        let db = open_local(&config, true).expect("dry-run open should succeed");
        let write = db
            .upsert_rows("t", std::slice::from_ref(&sample_row()))
            .await;
        assert!(
            write.is_err(),
            "dry-run's local connection must reject writes, but a write succeeded"
        );

        // Control: the same DB opened non-dry-run accepts the identical
        // write. This proves the failure above is specifically about the
        // dry-run open mode, not an unrelated problem with the fixture.
        let db_rw = open_local(&config, false).expect("non-dry-run open should succeed");
        let write_rw = db_rw
            .upsert_rows("t", std::slice::from_ref(&sample_row()))
            .await;
        assert!(
            write_rw.is_ok(),
            "non-dry-run's local connection should accept writes, got: {:?}",
            write_rw
        );
    }

    /// Regression for #217 (SQLite target): mirrors the local-DB assertion
    /// above for the SQLite target opened via `TargetSource::open` in the
    /// watch loop's `ResolvedTarget::Sqlite` arm. Before the fix this arm
    /// called `LocalDb::open(database)` unconditionally (also ignoring
    /// `dry_run`), so the same write-succeeds-when-it-shouldn't failure
    /// applies to the target connection.
    #[tokio::test]
    async fn target_source_open_dry_run_connection_rejects_writes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("target.db");
        make_db_with_table(&db_path);

        let target = ResolvedTarget::Sqlite {
            database: db_path.to_string_lossy().into_owned(),
        };
        let dry_run = true;

        let target_db = crate::TargetSource::open(&target, !dry_run)
            .await
            .expect("dry-run target open should succeed");
        let write = target_db
            .upsert_rows("t", std::slice::from_ref(&sample_row()))
            .await;
        assert!(
            write.is_err(),
            "dry-run's target connection must reject writes, but a write succeeded"
        );
    }
}
