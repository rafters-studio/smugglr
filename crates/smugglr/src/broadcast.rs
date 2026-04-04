//! Broadcast daemon loop (CLI layer).
//!
//! The engine logic (peer discovery, delta protocol, TCP sync) lives in
//! `smugglr_core::broadcast`. This module provides the daemon loop that
//! ties it all together with signal handling and PID locking.

use smugglr_core::broadcast::{
    broadcast_pid_lock_path, handle_sync_connection, hash_db_path, run_broadcast_once,
    BroadcastConfig, Peer, PeerDiscovery, ReplayGuard,
};
use smugglr_core::config::Config;
use smugglr_core::daemon::PidLock;
use smugglr_core::error::{Result, SyncError};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Run the broadcast daemon loop.
///
/// Starts a TCP sync server, discovers peers via UDP broadcast, and
/// exchanges deltas on each interval tick. When `once` is true, runs
/// a single cycle and exits. When `dry_run` is true, logs what would
/// happen but does not apply deltas.
pub async fn run_broadcast(
    config: &Config,
    config_path: &std::path::Path,
    broadcast_config: &BroadcastConfig,
    once: bool,
    dry_run: bool,
) -> Result<()> {
    use tokio::signal;
    use tokio::time;

    let pid_path = broadcast_pid_lock_path(config_path);
    let _pid_lock = PidLock::acquire(&pid_path)?;

    let db_path_hash = hash_db_path(config.local_db_path());
    let instance_id = broadcast_config.resolve_instance_id();
    let port = broadcast_config.port;

    info!(
        "Starting broadcast daemon (port: {}, interval: {}s, instance: {}, dry_run: {})",
        port, broadcast_config.interval_secs, instance_id, dry_run
    );

    // Shared replay guard across TCP server and sync loop
    let replay_guard = Arc::new(tokio::sync::Mutex::new(ReplayGuard::new()));

    // Start TCP sync server in background
    let tcp_config = config.clone();
    let tcp_broadcast_config = broadcast_config.clone();
    let tcp_db_path_hash = db_path_hash.clone();
    let tcp_replay_guard = replay_guard.clone();
    let tcp_addr: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port));

    let tcp_listener = tokio::net::TcpListener::bind(tcp_addr).await.map_err(|e| {
        SyncError::Broadcast(format!(
            "TCP bind {}:{}: {}",
            Ipv4Addr::UNSPECIFIED,
            port,
            e
        ))
    })?;

    info!("TCP sync server listening on port {}", port);

    let server_handle = tokio::spawn(async move {
        loop {
            match tcp_listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("Accepted TCP connection from {}", addr);
                    if let Err(e) = handle_sync_connection(
                        stream,
                        &tcp_config,
                        &tcp_broadcast_config,
                        &tcp_db_path_hash,
                        &tcp_replay_guard,
                    )
                    .await
                    {
                        warn!("Error handling sync connection from {}: {}", addr, e);
                    }
                }
                Err(e) => {
                    warn!("TCP accept error: {}", e);
                }
            }
        }
    });

    let interval_secs = broadcast_config.interval_secs;
    let mut tick_count: u64 = 0;
    let mut interval = time::interval(Duration::from_secs(interval_secs));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                tick_count += 1;
                info!("Broadcast tick #{}", tick_count);

                if dry_run {
                    // In dry_run mode, just discover peers and report
                    let discovery = PeerDiscovery::new(
                        broadcast_config.clone(),
                        db_path_hash.clone(),
                    ).await?;
                    let listen_dur = Duration::from_secs(interval_secs.min(5));
                    let peers = discovery.discover_once(listen_dur).await?;
                    let compatible: Vec<&Peer> = peers
                        .iter()
                        .filter(|p| p.db_path_hash == db_path_hash)
                        .collect();
                    info!(
                        "Tick #{} (dry run): {} peers discovered, {} compatible",
                        tick_count, peers.len(), compatible.len()
                    );
                    for peer in &compatible {
                        info!(
                            "  Would sync with: {} at {}",
                            peer.instance_id, peer.addr
                        );
                    }
                } else {
                    match run_broadcast_once(config, broadcast_config, &replay_guard).await {
                        Ok(result) => {
                            if result.rows_received > 0 || result.rows_sent > 0 {
                                info!(
                                    "Tick #{}: {} peers synced, {} rows received, {} rows sent",
                                    tick_count, result.peers_synced,
                                    result.rows_received, result.rows_sent
                                );
                            } else {
                                info!(
                                    "Tick #{}: {} peers discovered, no changes",
                                    tick_count, result.peers_discovered
                                );
                            }
                        }
                        Err(e) => {
                            warn!("Broadcast tick #{} failed: {}", tick_count, e);
                        }
                    }
                }

                if once {
                    info!("Single cycle complete, exiting");
                    break;
                }
            }
            _ = signal::ctrl_c() => {
                info!("Received shutdown signal. Stopping broadcast daemon.");
                break;
            }
        }
    }

    server_handle.abort();
    info!("Broadcast daemon stopped after {} ticks", tick_count);
    Ok(())
}
