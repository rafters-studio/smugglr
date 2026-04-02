//! UDP peer discovery for LAN broadcast sync.
//!
//! Smuggler instances on the same subnet discover each other via UDP broadcast
//! on a configurable port (default: 31337). Each instance periodically announces
//! its presence, and maintains a peer table with TTL-based expiry.
//!
//! This module handles discovery only -- delta sync and conflict resolution
//! are handled by the existing diff/sync engine.

use crate::error::{Result, SyncError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Protocol version for announcement packets. Bump on breaking changes.
const PROTOCOL_VERSION: u8 = 1;

/// Default UDP port for broadcast discovery.
pub const DEFAULT_PORT: u16 = 31337;

/// Default interval between broadcast announcements.
const DEFAULT_INTERVAL_SECS: u64 = 30;

/// Peers not seen within this many intervals are pruned.
const PEER_TTL_MULTIPLIER: u64 = 3;

/// Maximum size of a UDP announcement packet.
const MAX_PACKET_SIZE: usize = 1024;

/// Configuration for LAN broadcast sync.
#[derive(Debug, Clone, Deserialize)]
pub struct BroadcastConfig {
    /// UDP port for broadcast (default: 31337)
    #[serde(default = "default_port")]
    pub port: u16,

    /// Broadcast interval in seconds (default: 30)
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,

    /// Instance identity (defaults to hostname)
    pub instance_id: Option<String>,
}

fn default_port() -> u16 {
    DEFAULT_PORT
}

fn default_interval_secs() -> u64 {
    DEFAULT_INTERVAL_SECS
}

impl Default for BroadcastConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            interval_secs: DEFAULT_INTERVAL_SECS,
            instance_id: None,
        }
    }
}

impl BroadcastConfig {
    /// Resolve the instance ID, falling back to hostname.
    pub fn resolve_instance_id(&self) -> String {
        self.instance_id
            .clone()
            .unwrap_or_else(|| hostname().unwrap_or_else(|| "unknown".to_string()))
    }

    /// Peer TTL based on broadcast interval.
    pub fn peer_ttl(&self) -> Duration {
        Duration::from_secs(self.interval_secs * PEER_TTL_MULTIPLIER)
    }
}

/// A discovered peer on the LAN.
#[derive(Debug, Clone)]
pub struct Peer {
    /// Unique instance identifier
    pub instance_id: String,
    /// Network address the announcement came from
    pub addr: SocketAddr,
    /// Path hash of the database being synced (to match compatible peers)
    pub db_path_hash: String,
    /// When we last heard from this peer
    pub last_seen: Instant,
    /// Protocol version the peer is running
    pub protocol_version: u8,
}

impl Peer {
    /// Check if this peer has expired based on the given TTL.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.last_seen.elapsed() > ttl
    }
}

/// Announcement packet broadcast over UDP.
///
/// Kept small to fit in a single UDP datagram with room to spare.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Announcement {
    /// Protocol version (for forward compatibility)
    pub version: u8,
    /// Unique instance identifier
    pub instance_id: String,
    /// SHA256 hash of the database path (not the path itself, for privacy)
    pub db_path_hash: String,
    /// TCP port for sync connections (may differ from broadcast port in future)
    pub sync_port: u16,
}

impl Announcement {
    pub fn new(instance_id: String, db_path_hash: String, sync_port: u16) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            instance_id,
            db_path_hash,
            sync_port,
        }
    }

    /// Serialize to bytes for UDP transmission.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| SyncError::Broadcast(format!("serialize: {}", e)))
    }

    /// Deserialize from received UDP bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| SyncError::Broadcast(format!("deserialize: {}", e)))
    }
}

/// Manages peer discovery via UDP subnet broadcast.
pub struct PeerDiscovery {
    config: BroadcastConfig,
    instance_id: String,
    peers: Arc<RwLock<HashMap<String, Peer>>>,
    socket: UdpSocket,
    announcement: Announcement,
}

impl PeerDiscovery {
    /// Bind the UDP socket and prepare for broadcast.
    pub async fn new(config: BroadcastConfig, db_path_hash: String) -> Result<Self> {
        let instance_id = config.resolve_instance_id();
        let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, config.port);

        let socket = UdpSocket::bind(bind_addr).await.map_err(|e| {
            SyncError::Broadcast(format!(
                "bind {}:{}: {}",
                Ipv4Addr::UNSPECIFIED,
                config.port,
                e
            ))
        })?;

        socket
            .set_broadcast(true)
            .map_err(|e| SyncError::Broadcast(format!("enable SO_BROADCAST: {}", e)))?;

        let announcement = Announcement::new(instance_id.clone(), db_path_hash, config.port);

        info!(
            "Peer discovery bound to port {}, instance_id={}",
            config.port, instance_id
        );

        Ok(Self {
            config,
            instance_id,
            peers: Arc::new(RwLock::new(HashMap::new())),
            socket,
            announcement,
        })
    }

    /// Send a broadcast announcement to all peers on the subnet.
    pub async fn announce(&self) -> Result<()> {
        let data = self.announcement.to_bytes()?;
        let broadcast_addr = SocketAddrV4::new(Ipv4Addr::BROADCAST, self.config.port);

        match self.socket.send_to(&data, broadcast_addr).await {
            Ok(n) => {
                debug!("Broadcast announcement sent ({} bytes)", n);
                Ok(())
            }
            Err(e) => {
                // Non-fatal: log and continue. Peer may still discover us
                // via their own broadcast.
                warn!("Broadcast send failed: {}", e);
                Ok(())
            }
        }
    }

    /// Listen for a single announcement from the network.
    ///
    /// Returns the parsed announcement and the sender's address,
    /// or None if the packet was from ourselves or unparseable.
    pub async fn receive_one(&self) -> Result<Option<(Announcement, SocketAddr)>> {
        let mut buf = [0u8; MAX_PACKET_SIZE];

        let (n, addr) = self
            .socket
            .recv_from(&mut buf)
            .await
            .map_err(|e| SyncError::Broadcast(format!("recv: {}", e)))?;

        let announcement = match Announcement::from_bytes(&buf[..n]) {
            Ok(a) => a,
            Err(e) => {
                debug!("Ignoring malformed packet from {}: {}", addr, e);
                return Ok(None);
            }
        };

        // Ignore our own announcements
        if announcement.instance_id == self.instance_id {
            return Ok(None);
        }

        // Ignore incompatible protocol versions
        if announcement.version != PROTOCOL_VERSION {
            debug!(
                "Ignoring peer {} with protocol version {} (ours: {})",
                announcement.instance_id, announcement.version, PROTOCOL_VERSION
            );
            return Ok(None);
        }

        Ok(Some((announcement, addr)))
    }

    /// Update the peer table with a received announcement.
    pub async fn register_peer(&self, announcement: &Announcement, addr: SocketAddr) {
        let peer = Peer {
            instance_id: announcement.instance_id.clone(),
            addr,
            db_path_hash: announcement.db_path_hash.clone(),
            last_seen: Instant::now(),
            protocol_version: announcement.version,
        };

        let mut peers = self.peers.write().await;
        let is_new = !peers.contains_key(&announcement.instance_id);
        peers.insert(announcement.instance_id.clone(), peer);

        if is_new {
            info!(
                "Discovered new peer: {} at {}",
                announcement.instance_id, addr
            );
        } else {
            debug!("Updated peer: {} at {}", announcement.instance_id, addr);
        }
    }

    /// Remove peers that haven't announced within the TTL window.
    pub async fn prune_expired(&self) -> Vec<String> {
        let ttl = self.config.peer_ttl();
        let mut peers = self.peers.write().await;

        let expired: Vec<String> = peers
            .iter()
            .filter(|(_, p)| p.is_expired(ttl))
            .map(|(id, _)| id.clone())
            .collect();

        for id in &expired {
            info!("Peer expired: {}", id);
            peers.remove(id);
        }

        expired
    }

    /// Get a snapshot of all currently known peers.
    pub async fn peers(&self) -> Vec<Peer> {
        self.peers.read().await.values().cloned().collect()
    }

    /// Get peers that share the same database path hash (compatible for sync).
    pub async fn compatible_peers(&self, db_path_hash: &str) -> Vec<Peer> {
        self.peers
            .read()
            .await
            .values()
            .filter(|p| p.db_path_hash == db_path_hash && !p.is_expired(self.config.peer_ttl()))
            .cloned()
            .collect()
    }

    /// Run the announce-listen-prune loop for a single cycle.
    ///
    /// Sends an announcement, listens for responses for a short window,
    /// prunes expired peers, and returns the current peer list.
    pub async fn discover_once(&self, listen_duration: Duration) -> Result<Vec<Peer>> {
        self.announce().await?;

        let deadline = Instant::now() + listen_duration;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, self.receive_one()).await {
                Ok(Ok(Some((announcement, addr)))) => {
                    self.register_peer(&announcement, addr).await;
                }
                Ok(Ok(None)) => {
                    // Own packet or unparseable, continue listening
                }
                Ok(Err(e)) => {
                    warn!("Error receiving announcement: {}", e);
                }
                Err(_) => {
                    // Timeout reached
                    break;
                }
            }
        }

        self.prune_expired().await;
        Ok(self.peers().await)
    }

    /// Get a shared handle to the peer table for use from other tasks.
    pub fn peer_table(&self) -> Arc<RwLock<HashMap<String, Peer>>> {
        Arc::clone(&self.peers)
    }

    /// The instance ID of this discovery instance.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }
}

/// Get the system hostname, if available.
fn hostname() -> Option<String> {
    #[cfg(unix)]
    {
        use std::ffi::CStr;
        let mut buf = [0u8; 256];
        let ret = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
        if ret == 0 {
            let cstr = unsafe { CStr::from_ptr(buf.as_ptr() as *const libc::c_char) };
            cstr.to_str().ok().map(String::from)
        } else {
            None
        }
    }
    #[cfg(not(unix))]
    {
        std::env::var("COMPUTERNAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .ok()
    }
}

/// Compute a SHA256 hash of a database path for use in announcements.
///
/// We hash the path rather than sending it raw to avoid leaking filesystem
/// structure over the network.
pub fn hash_db_path(path: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(path.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_announcement_roundtrip() {
        let original =
            Announcement::new("test-machine".to_string(), "abc123hash".to_string(), 31337);

        let bytes = original.to_bytes().expect("serialize");
        let decoded = Announcement::from_bytes(&bytes).expect("deserialize");

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_announcement_fits_in_udp() {
        // Worst case: long instance ID and full hash
        let announcement = Announcement::new("a".repeat(128), "f".repeat(64), 31337);

        let bytes = announcement.to_bytes().expect("serialize");
        // UDP payload limit is 65507, but we want to stay well under for
        // fragmentation avoidance. Announcement should be tiny.
        assert!(bytes.len() < MAX_PACKET_SIZE);
    }

    #[test]
    fn test_announcement_rejects_garbage() {
        let result = Announcement::from_bytes(b"not json at all");
        assert!(result.is_err());
    }

    #[test]
    fn test_hash_db_path_deterministic() {
        let h1 = hash_db_path("/home/user/legion.db");
        let h2 = hash_db_path("/home/user/legion.db");
        assert_eq!(h1, h2);

        let h3 = hash_db_path("/home/other/legion.db");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_peer_expiry() {
        let peer = Peer {
            instance_id: "test".to_string(),
            addr: "127.0.0.1:31337".parse().unwrap(),
            db_path_hash: "abc".to_string(),
            last_seen: Instant::now() - Duration::from_secs(100),
            protocol_version: 1,
        };

        assert!(peer.is_expired(Duration::from_secs(90)));
        assert!(!peer.is_expired(Duration::from_secs(110)));
    }

    #[test]
    fn test_broadcast_config_defaults() {
        let config = BroadcastConfig::default();
        assert_eq!(config.port, 31337);
        assert_eq!(config.interval_secs, 30);
        assert!(config.instance_id.is_none());
        assert_eq!(config.peer_ttl(), Duration::from_secs(90));
    }

    #[tokio::test]
    async fn test_peer_discovery_loopback() {
        // Two discovery instances on different ports to avoid bind conflict
        let config_a = BroadcastConfig {
            port: 0, // OS-assigned port
            interval_secs: 1,
            instance_id: Some("machine-a".to_string()),
        };
        let config_b = BroadcastConfig {
            port: 0,
            interval_secs: 1,
            instance_id: Some("machine-b".to_string()),
        };

        let hash = hash_db_path("/test/legion.db");
        let discovery_a = PeerDiscovery::new(config_a, hash.clone()).await.unwrap();
        let discovery_b = PeerDiscovery::new(config_b, hash.clone()).await.unwrap();

        // Get the actual bound ports
        let _port_a = discovery_a.socket.local_addr().unwrap().port();
        let port_b = discovery_b.socket.local_addr().unwrap().port();

        // Manually send announcement from A to B's port
        let announcement_a = discovery_a.announcement.to_bytes().unwrap();
        let addr_b = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port_b);
        discovery_a
            .socket
            .send_to(&announcement_a, addr_b)
            .await
            .unwrap();

        // B should receive A's announcement
        let result = tokio::time::timeout(Duration::from_secs(2), discovery_b.receive_one())
            .await
            .expect("timeout")
            .expect("receive");

        let (announcement, _addr) = result.expect("should have received announcement");
        assert_eq!(announcement.instance_id, "machine-a");
        assert_eq!(announcement.db_path_hash, hash);
    }

    #[tokio::test]
    async fn test_register_and_prune_peers() {
        let config = BroadcastConfig {
            port: 0,
            interval_secs: 1, // 1s interval = 3s TTL
            instance_id: Some("test-host".to_string()),
        };

        let hash = hash_db_path("/test/db.sqlite");
        let discovery = PeerDiscovery::new(config, hash.clone()).await.unwrap();

        // Register a peer
        let announcement = Announcement::new("remote-peer".to_string(), hash.clone(), 31337);
        let addr: SocketAddr = "192.168.1.100:31337".parse().unwrap();
        discovery.register_peer(&announcement, addr).await;

        // Peer should be visible
        let peers = discovery.peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].instance_id, "remote-peer");

        // Compatible peers should include it
        let compatible = discovery.compatible_peers(&hash).await;
        assert_eq!(compatible.len(), 1);

        // Incompatible hash should not match
        let incompatible = discovery.compatible_peers("different-hash").await;
        assert_eq!(incompatible.len(), 0);
    }
}
