//! Embedded smugglr inside a tokio service.
//!
//! The service holds a local SQLite file open (`LocalDb`) and syncs it on a
//! tick against an HTTP-SQL endpoint reached through the `smugglr-http-sql`
//! plugin (`PluginDataSource`). The endpoint speaks the `generic` profile:
//! POST `{sql, params}` in, `{columns, rows}` out. Swap the profile name for
//! `turso`, `d1`, or `rqlite` to point the same code at a hosted service.

use std::collections::HashMap;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::pin::pin;
use std::time::Duration;

use anyhow::{Context, Result};
use smugglr_core::config::Config;
use smugglr_core::local::LocalDb;
use smugglr_core::plugin::{resolve_plugin_path, PluginDataSource};
use smugglr_core::sync::{sync_all, NoProgress};
use tokio::signal;
use tokio::time::{interval, MissedTickBehavior};

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .init();

    let local = LocalDb::open(env_or("LOCAL_DB", "local.db"))?;

    // The plugin binary: `SMUGGLR_HTTP_SQL_PLUGIN` if set, otherwise the same
    // search the CLI uses (~/.smugglr/plugins, then $PATH).
    let plugin_path = match std::env::var_os("SMUGGLR_HTTP_SQL_PLUGIN") {
        Some(p) => PathBuf::from(p),
        None => resolve_plugin_path("http-sql")?,
    };
    let plugin_config = HashMap::from([
        ("profile".to_string(), env_or("HTTP_SQL_PROFILE", "generic")),
        (
            "url".to_string(),
            env_or("HTTP_SQL_URL", "http://127.0.0.1:18787/sql"),
        ),
        ("auth_token".to_string(), env_or("HTTP_SQL_TOKEN", "")),
    ]);
    let remote = PluginDataSource::start(&plugin_path, "smugglr-http-sql", &plugin_config)
        .await
        .with_context(|| format!("starting {}", plugin_path.display()))?;

    let config = Config::from_toml_str("[sync]\nconflict_resolution = \"newer_wins\"\n")?;

    let tick_every = Duration::from_secs(
        env_or("SYNC_INTERVAL", "30")
            .parse()
            .context("SYNC_INTERVAL must be a whole number of seconds")?,
    );
    let mut ticker = interval(tick_every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // Pinned once so the Ctrl-C handler is installed on the first poll and
    // stays installed. A SIGINT that arrives while a sync is running is
    // delivered to the next `select!`, after that sync has returned.
    let mut shutdown = pin!(signal::ctrl_c());

    tracing::info!("syncing every {:?}", tick_every);

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                tracing::info!("shutdown signal received");
                break;
            }
            _ = ticker.tick() => {}
        }

        // The sync runs outside `select!` so shutdown cannot cancel it midway.
        match sync_all(&local, &remote, &config, None, false, &NoProgress).await {
            Ok(results) => {
                let pushed: usize = results.iter().map(|r| r.rows_pushed).sum();
                let pulled: usize = results.iter().map(|r| r.rows_pulled).sum();
                tracing::info!(
                    "sync ok: {} tables, {} rows pushed, {} rows pulled",
                    results.len(),
                    pushed,
                    pulled
                );
            }
            Err(err) => {
                tracing::warn!("sync failed: {}", err);
            }
        }
    }

    // Dropping `remote` kills the plugin process (`PluginDataSource::drop`).
    drop(remote);
    Ok(())
}
