//! Broadcast daemon loop (CLI layer) -- the LAN broadcast shape.
//!
//! `smugglr broadcast` is masterless multicast gossip: every node runs this
//! identical loop, multicasting its content-hash digest and applying rows it
//! hears, so two or two hundred nodes on a subnet converge automatically with no
//! coordinator. The engine is `smugglr_core::multicast`.
//!
//! This is deliberately NOT the TCP path. TCP (`smugglr_core::broadcast`'s
//! framed sync envelope, #90) is a different shape: cross-process and
//! cross-subnet sync, where an embedder bridges what multicast cannot reach.
//! Both shapes coexist; this daemon drives the multicast one.

use smugglr_core::broadcast::{broadcast_pid_lock_path, BroadcastConfig};
use smugglr_core::config::Config;
use smugglr_core::daemon::PidLock;
use smugglr_core::error::Result;
use smugglr_core::multicast::{Gossip, GossipEvent, DEFAULT_GROUP, RECV_BUF};
use smugglr_core::LocalDb;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Run the broadcast daemon loop.
///
/// Joins the multicast group, then runs the masterless gossip loop: a continuous
/// listener applying inbound rows and answering pulls, plus a heartbeat that
/// multicasts this node's `primary_key -> content_hash` digest each interval. A
/// peer that hears a digest covering rows it lacks pulls them; whoever holds
/// them multicasts the rows, so every listener converges from one answer.
///
/// - `once`: send a single heartbeat, converge briefly, then exit.
/// - `dry_run`: report the digest this node would advertise; send/apply nothing.
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

    let instance_id = broadcast_config.resolve_instance_id();
    // `tokio::time::interval` panics on a zero period; clamp defensively so a
    // `--interval 0` (or a config carrying interval_secs == 0) cannot crash the
    // daemon.
    let interval_secs = broadcast_config.interval_secs.max(1);

    info!(
        "Starting masterless multicast sync (group {}, port {}, interval {}s, instance {}, dry_run {})",
        DEFAULT_GROUP, broadcast_config.port, interval_secs, instance_id, dry_run
    );

    let local = Arc::new(
        LocalDb::open(config.local_db_path())?.with_duplicate_pk(config.sync.duplicate_pk),
    );

    if dry_run {
        // Advertise nothing, apply nothing -- just report what we would gossip.
        let gossip = Gossip::bind(broadcast_config, DEFAULT_GROUP).await?;
        let bodies = gossip.digest_bodies(&local, config).await?;
        info!(
            "Dry run: would multicast {} digest datagram(s); not sending, not applying",
            bodies.len()
        );
        return Ok(());
    }

    let gossip = Arc::new(Gossip::bind(broadcast_config, DEFAULT_GROUP).await?);

    // Continuous listener. A masterless node keeps gossiping through transient
    // errors, so a recv failure is logged, never fatal.
    let recv_handle = {
        let gossip = gossip.clone();
        let local = local.clone();
        let config = config.clone();
        tokio::spawn(async move {
            // Allocated once, reused across every datagram: `recv_and_handle`
            // previously allocated+zeroed a fresh RECV_BUF-sized (64 KiB) buffer
            // per call, which churned the allocator on every packet in this
            // steady-state gossip loop.
            let mut recv_buf = vec![0u8; RECV_BUF];
            loop {
                match gossip.recv_and_handle(&mut recv_buf, &local, &config).await {
                    // A rejected row is the conflict guard doing its job, not an
                    // error -- but a run where every peer row is turned away is
                    // indistinguishable from a run where none arrived unless the
                    // count is said out loud.
                    Ok(GossipEvent::Applied {
                        table,
                        rows,
                        rejected,
                    }) if rows > 0 || rejected > 0 => {
                        if rejected > 0 {
                            info!(
                                "Applied {} row(s) to '{}' ({} turned away as not newer)",
                                rows, table, rejected
                            )
                        } else {
                            info!("Applied {} row(s) to '{}'", rows, table)
                        }
                    }
                    Ok(GossipEvent::Served { table, rows }) if rows > 0 => {
                        debug!("Served {} row(s) of '{}'", rows, table)
                    }
                    Ok(_) => {}
                    Err(e) => warn!("gossip recv: {}", e),
                }
            }
        })
    };

    if once {
        gossip.broadcast_digests(&local, config).await?;
        info!("Single heartbeat sent; converging briefly...");
        time::sleep(Duration::from_secs(interval_secs.clamp(1, 5))).await;
        recv_handle.abort();
        info!("Single cycle complete, exiting");
        return Ok(());
    }

    let mut tick: u64 = 0;
    let mut interval = time::interval(Duration::from_secs(interval_secs));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                tick += 1;
                match gossip.broadcast_digests(&local, config).await {
                    Ok(n) => info!("Heartbeat #{}: multicast {} digest datagram(s)", tick, n),
                    Err(e) => warn!("Heartbeat #{} failed: {}", tick, e),
                }
            }
            _ = signal::ctrl_c() => {
                info!("Received shutdown signal. Stopping multicast sync.");
                break;
            }
        }
    }

    recv_handle.abort();
    info!("Multicast sync stopped after {} heartbeat(s)", tick);
    Ok(())
}
