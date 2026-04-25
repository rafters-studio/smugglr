//! Embedded smugglr inside a tokio service.
//!
//! The service holds two LocalDb instances open (`a.sqlite` and `b.sqlite`)
//! and bidirectionally syncs them on a tick. Replace either side with an
//! HTTP-SQL endpoint via the smugglr-http-sql plugin in real deployments.

use std::time::Duration;

use anyhow::Result;
use smugglr_core::config::Config;
use smugglr_core::local::LocalDb;
use smugglr_core::sync::{sync_all, NoProgress};
use tokio::signal;
use tokio::time::{interval, MissedTickBehavior};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let a = LocalDb::open("a.sqlite")?;
    let b = LocalDb::open("b.sqlite")?;
    let config = Config::from_toml_str("")?;

    let tick_every = Duration::from_secs(
        std::env::var("SYNC_INTERVAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30),
    );
    let mut ticker = interval(tick_every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    tracing::info!("syncing every {:?}", tick_every);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                match sync_all(&a, &b, &config, None, false, &NoProgress).await {
                    Ok(results) => {
                        let total: usize = results
                            .iter()
                            .map(|r| r.rows_pushed + r.rows_pulled)
                            .sum();
                        tracing::info!("sync ok: {} rows across {} tables", total, results.len());
                    }
                    Err(err) => {
                        tracing::warn!("sync failed: {}", err);
                    }
                }
            }
            _ = signal::ctrl_c() => {
                tracing::info!("shutdown signal received");
                break;
            }
        }
    }

    Ok(())
}
