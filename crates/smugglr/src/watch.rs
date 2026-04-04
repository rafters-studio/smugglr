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
use smugglr_core::remote::D1Client;
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

    let mut tick_count: u64 = 0;
    let mut interval = time::interval(Duration::from_secs(interval_secs));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                tick_count += 1;
                info!("Watch tick #{}", tick_count);

                let result = match &target {
                    ResolvedTarget::D1 { account_id, database_id, api_token } => {
                        let local = LocalDb::open(config.local_db_path())?;
                        let remote = D1Client::with_retry_config(
                            account_id.clone(),
                            database_id.clone(),
                            api_token.clone(),
                            config.retry_config(),
                        );
                        if let Err(e) = remote.test_connection().await {
                            warn!("Connection test failed on tick #{}: {}. Will retry next tick.", tick_count, e);
                            if fmt == OutputFormat::Json {
                                let out = WatchTickOutput::from_error(tick_count, &e.to_string());
                                println!("{}", serde_json::to_string(&out).expect("WatchTickOutput is always serializable"));
                            }
                            continue;
                        }
                        sync_all(&local, &remote, config, None, dry_run, &NoProgress).await
                    }
                    ResolvedTarget::Sqlite { database } => {
                        let local = LocalDb::open(config.local_db_path())?;
                        let target_db = LocalDb::open(database)?;
                        sync_all(&local, &target_db, config, None, dry_run, &NoProgress).await
                    }
                    ResolvedTarget::Plugin { .. } => {
                        let local = LocalDb::open(config.local_db_path())?;
                        let plugin = plugin.as_ref().expect("plugin initialized before loop");
                        sync_all(&local, plugin, config, None, dry_run, &NoProgress).await
                    }
                };

                match result {
                    Ok(results) => {
                        let total_pushed: usize = results.iter().map(|r| r.rows_pushed).sum();
                        let total_pulled: usize = results.iter().map(|r| r.rows_pulled).sum();

                        if fmt == OutputFormat::Json {
                            let out = WatchTickOutput::from_results(tick_count, &results);
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
                                let out = WatchTickOutput::from_error(tick_count, &e.to_string());
                                println!("{}", serde_json::to_string(&out).expect("WatchTickOutput is always serializable"));
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
