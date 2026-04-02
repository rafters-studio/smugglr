//! LAN broadcast sync: peer discovery, delta serialization, and encryption.
//!
//! Smuggler instances on the same subnet discover each other via UDP broadcast
//! on a configurable port (default: 31337). Each instance periodically announces
//! its presence, and maintains a peer table with TTL-based expiry.
//!
//! The delta protocol serializes table diffs into packets for network transport,
//! handling UDP size limits by splitting large deltas into multiple parts.
//!
//! When a pre-shared key is configured, all broadcast traffic is encrypted with
//! XChaCha20-Poly1305 AEAD. Plaintext and encrypted modes are mutually exclusive.

use crate::error::{Result, SyncError};
use chacha20poly1305::aead::{Aead, NewAead};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use rand::RngCore;
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

/// Minimum encrypted packet size: 24-byte nonce + 16-byte Poly1305 tag.
const ENCRYPTION_OVERHEAD: usize = 24 + 16;

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

    /// 256-bit pre-shared key, hex-encoded (64 hex chars).
    /// When set, all broadcast traffic is encrypted with XChaCha20-Poly1305.
    pub secret: Option<String>,
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
            secret: None,
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

    /// Parse the hex-encoded secret into a 256-bit key.
    /// Returns None if no secret is configured.
    pub fn encryption_key(&self) -> Result<Option<[u8; 32]>> {
        match &self.secret {
            None => Ok(None),
            Some(hex_key) => {
                let bytes = hex::decode(hex_key).map_err(|_| {
                    SyncError::Config(
                        "broadcast secret must be 64 hex characters (256-bit key)".to_string(),
                    )
                })?;
                let key: [u8; 32] = bytes.try_into().map_err(|_| {
                    SyncError::Config(
                        "broadcast secret must be 64 hex characters (256-bit key)".to_string(),
                    )
                })?;
                Ok(Some(key))
            }
        }
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

// ---------------------------------------------------------------------------
// XChaCha20-Poly1305 encryption
// ---------------------------------------------------------------------------

/// Encrypt a serialized packet for broadcast.
///
/// Wire format: `[24-byte nonce][ciphertext + 16-byte Poly1305 tag]`
fn encrypt_packet(plaintext: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    let cipher = XChaCha20Poly1305::new(key.into());

    let mut nonce_bytes = [0u8; 24];
    rand::rng().fill_bytes(&mut nonce_bytes);
    let nonce = XNonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| SyncError::Broadcast("encryption failed".to_string()))?;

    let mut packet = Vec::with_capacity(24 + ciphertext.len());
    packet.extend_from_slice(&nonce_bytes);
    packet.extend_from_slice(&ciphertext);
    Ok(packet)
}

/// Decrypt a received packet.
///
/// Expects wire format: `[24-byte nonce][ciphertext + 16-byte Poly1305 tag]`
fn decrypt_packet(data: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    if data.len() < ENCRYPTION_OVERHEAD {
        return Err(SyncError::Broadcast(format!(
            "packet too short ({} bytes, minimum {})",
            data.len(),
            ENCRYPTION_OVERHEAD
        )));
    }

    let (nonce_bytes, ciphertext) = data.split_at(24);
    let nonce = XNonce::from_slice(nonce_bytes);
    let cipher = XChaCha20Poly1305::new(key.into());

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| SyncError::Broadcast("authentication failed".to_string()))
}

/// Wrap plaintext in an encryption envelope if a key is provided.
pub fn maybe_encrypt(plaintext: &[u8], key: &Option<[u8; 32]>) -> Result<Vec<u8>> {
    match key {
        Some(k) => encrypt_packet(plaintext, k),
        None => Ok(plaintext.to_vec()),
    }
}

/// Unwrap a potentially encrypted packet. Returns None to signal "drop this packet".
pub fn maybe_decrypt(data: &[u8], key: &Option<[u8; 32]>) -> Result<Option<Vec<u8>>> {
    match key {
        Some(k) => {
            if data.len() < ENCRYPTION_OVERHEAD {
                warn!(
                    "Dropping packet: too short for encrypted mode ({} bytes)",
                    data.len()
                );
                return Ok(None);
            }
            Ok(Some(decrypt_packet(data, k)?))
        }
        None => {
            if data.first() != Some(&b'{') && data.first() != Some(&b'[') {
                warn!("Dropping encrypted packet: no secret configured");
                return Ok(None);
            }
            Ok(Some(data.to_vec()))
        }
    }
}

// ---------------------------------------------------------------------------
// Peer discovery
// ---------------------------------------------------------------------------

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
                warn!("Broadcast send failed: {}", e);
                Ok(())
            }
        }
    }

    /// Listen for a single announcement from the network.
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

        if announcement.instance_id == self.instance_id {
            return Ok(None);
        }

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
                Ok(Ok(None)) => {}
                Ok(Err(e)) => {
                    warn!("Error receiving announcement: {}", e);
                }
                Err(_) => {
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
pub fn hash_db_path(path: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(path.as_bytes());
    hex::encode(hasher.finalize())
}

// ---------------------------------------------------------------------------
// Delta serialization wire protocol
// ---------------------------------------------------------------------------

/// Maximum UDP payload size (65535 - 20 IP header - 8 UDP header).
const MAX_UDP_PAYLOAD: usize = 65507;

/// Conservative packet size to avoid IP fragmentation on most networks.
const SAFE_PACKET_SIZE: usize = 1400;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeltaPacket {
    pub version: u8,
    pub source_id: String,
    pub seq: u64,
    pub part: u16,
    pub total_parts: u16,
    pub table: String,
    pub upserts: Vec<HashMap<String, serde_json::Value>>,
    pub deletes: Vec<String>,
}

impl DeltaPacket {
    pub fn new(source_id: String, seq: u64, table: String) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            source_id,
            seq,
            part: 0,
            total_parts: 1,
            table,
            upserts: Vec::new(),
            deletes: Vec::new(),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|e| SyncError::Broadcast(format!("delta serialize: {}", e)))
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| SyncError::Broadcast(format!("delta deserialize: {}", e)))
    }

    pub fn is_empty(&self) -> bool {
        self.upserts.is_empty() && self.deletes.is_empty()
    }
}

pub fn split_delta(
    source_id: &str,
    seq: u64,
    table: &str,
    upserts: Vec<HashMap<String, serde_json::Value>>,
    deletes: Vec<String>,
) -> Result<Vec<DeltaPacket>> {
    let mut base = DeltaPacket {
        version: PROTOCOL_VERSION,
        source_id: source_id.to_string(),
        seq,
        part: 0,
        total_parts: 1,
        table: table.to_string(),
        upserts: Vec::new(),
        deletes,
    };

    if upserts.is_empty() {
        return Ok(vec![base]);
    }

    base.upserts = upserts.clone();
    let serialized = base.to_bytes()?;
    if serialized.len() <= SAFE_PACKET_SIZE {
        return Ok(vec![base]);
    }

    let mut packets: Vec<DeltaPacket> = Vec::new();
    let mut current = DeltaPacket {
        version: PROTOCOL_VERSION,
        source_id: source_id.to_string(),
        seq,
        part: 0,
        total_parts: 0,
        table: table.to_string(),
        upserts: Vec::new(),
        deletes: base.deletes.clone(),
    };

    for row in upserts {
        current.upserts.push(row);

        let size = current.to_bytes()?.len();
        if size > SAFE_PACKET_SIZE && current.upserts.len() > 1 {
            let overflow = current.upserts.pop().unwrap();
            packets.push(current);

            current = DeltaPacket {
                version: PROTOCOL_VERSION,
                source_id: source_id.to_string(),
                seq,
                part: packets.len() as u16,
                total_parts: 0,
                table: table.to_string(),
                upserts: vec![overflow],
                deletes: Vec::new(),
            };
        }
    }

    if !current.upserts.is_empty() || !current.deletes.is_empty() {
        current.part = packets.len() as u16;
        packets.push(current);
    }

    let total = packets.len() as u16;
    for p in &mut packets {
        p.total_parts = total;
    }

    for p in &packets {
        let size = p.to_bytes()?.len();
        if size > MAX_UDP_PAYLOAD {
            return Err(SyncError::Broadcast(format!(
                "single row exceeds UDP payload limit ({} bytes for table '{}')",
                size, table
            )));
        }
    }

    Ok(packets)
}

#[derive(Debug, Default)]
pub struct SequenceTracker {
    sequences: HashMap<String, u64>,
}

impl SequenceTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next(&mut self, table: &str) -> u64 {
        let seq = self.sequences.entry(table.to_string()).or_insert(0);
        let current = *seq;
        *seq += 1;
        current
    }

    pub fn current(&self, table: &str) -> u64 {
        self.sequences.get(table).copied().unwrap_or(0)
    }
}

pub fn reassemble_delta(parts: &[DeltaPacket]) -> Option<DeltaPacket> {
    if parts.is_empty() {
        return None;
    }

    let expected_total = parts[0].total_parts as usize;
    if parts.len() != expected_total {
        return None;
    }

    let seq = parts[0].seq;
    let table = &parts[0].table;
    if parts.iter().any(|p| p.seq != seq || p.table != *table) {
        return None;
    }

    let mut sorted: Vec<&DeltaPacket> = parts.iter().collect();
    sorted.sort_by_key(|p| p.part);

    let mut merged = DeltaPacket::new(parts[0].source_id.clone(), seq, table.clone());
    merged.total_parts = 1;

    for part in sorted {
        merged.upserts.extend(part.upserts.clone());
        merged.deletes.extend(part.deletes.clone());
    }

    Some(merged)
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
        let announcement = Announcement::new("a".repeat(128), "f".repeat(64), 31337);
        let bytes = announcement.to_bytes().expect("serialize");
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
        assert!(config.secret.is_none());
        assert_eq!(config.peer_ttl(), Duration::from_secs(90));
    }

    #[tokio::test]
    async fn test_peer_discovery_loopback() {
        let config_a = BroadcastConfig {
            port: 0,
            interval_secs: 1,
            instance_id: Some("machine-a".to_string()),
            ..Default::default()
        };
        let config_b = BroadcastConfig {
            port: 0,
            interval_secs: 1,
            instance_id: Some("machine-b".to_string()),
            ..Default::default()
        };

        let hash = hash_db_path("/test/legion.db");
        let discovery_a = PeerDiscovery::new(config_a, hash.clone()).await.unwrap();
        let discovery_b = PeerDiscovery::new(config_b, hash.clone()).await.unwrap();

        let _port_a = discovery_a.socket.local_addr().unwrap().port();
        let port_b = discovery_b.socket.local_addr().unwrap().port();

        let announcement_a = discovery_a.announcement.to_bytes().unwrap();
        let addr_b = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port_b);
        discovery_a
            .socket
            .send_to(&announcement_a, addr_b)
            .await
            .unwrap();

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
            interval_secs: 1,
            instance_id: Some("test-host".to_string()),
            ..Default::default()
        };

        let hash = hash_db_path("/test/db.sqlite");
        let discovery = PeerDiscovery::new(config, hash.clone()).await.unwrap();

        let announcement = Announcement::new("remote-peer".to_string(), hash.clone(), 31337);
        let addr: SocketAddr = "192.168.1.100:31337".parse().unwrap();
        discovery.register_peer(&announcement, addr).await;

        let peers = discovery.peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].instance_id, "remote-peer");

        let compatible = discovery.compatible_peers(&hash).await;
        assert_eq!(compatible.len(), 1);

        let incompatible = discovery.compatible_peers("different-hash").await;
        assert_eq!(incompatible.len(), 0);
    }

    // -----------------------------------------------------------------------
    // Delta protocol tests
    // -----------------------------------------------------------------------

    fn make_row(id: &str, name: &str) -> HashMap<String, serde_json::Value> {
        let mut row = HashMap::new();
        row.insert("id".to_string(), serde_json::Value::String(id.to_string()));
        row.insert(
            "name".to_string(),
            serde_json::Value::String(name.to_string()),
        );
        row
    }

    #[test]
    fn test_delta_packet_roundtrip() {
        let mut packet = DeltaPacket::new("machine-a".to_string(), 1, "users".to_string());
        packet.upserts.push(make_row("1", "Alice"));
        packet.deletes.push("2".to_string());
        let bytes = packet.to_bytes().expect("serialize");
        let decoded = DeltaPacket::from_bytes(&bytes).expect("deserialize");
        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_delta_packet_empty() {
        let packet = DeltaPacket::new("machine-a".to_string(), 0, "users".to_string());
        assert!(packet.is_empty());
        let mut non_empty = packet.clone();
        non_empty.upserts.push(make_row("1", "Bob"));
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn test_split_delta_small_fits_one_packet() {
        let upserts = vec![make_row("1", "Alice"), make_row("2", "Bob")];
        let deletes = vec!["3".to_string()];
        let packets =
            split_delta("machine-a", 1, "users", upserts.clone(), deletes.clone()).unwrap();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].upserts.len(), 2);
        assert_eq!(packets[0].deletes.len(), 1);
        assert_eq!(packets[0].part, 0);
        assert_eq!(packets[0].total_parts, 1);
    }

    #[test]
    fn test_split_delta_large_splits() {
        let big_value = "x".repeat(500);
        let upserts: Vec<HashMap<String, serde_json::Value>> = (0..20)
            .map(|i| {
                let mut row = HashMap::new();
                row.insert("id".to_string(), serde_json::Value::String(i.to_string()));
                row.insert(
                    "data".to_string(),
                    serde_json::Value::String(big_value.clone()),
                );
                row
            })
            .collect();

        let packets = split_delta("machine-a", 5, "big_table", upserts, vec![]).unwrap();
        assert!(packets.len() > 1, "should split into multiple packets");

        for p in &packets {
            assert_eq!(p.seq, 5);
            assert_eq!(p.table, "big_table");
            assert_eq!(p.total_parts, packets.len() as u16);
        }

        for (i, p) in packets.iter().enumerate() {
            assert_eq!(p.part, i as u16);
        }

        for p in &packets {
            let size = p.to_bytes().unwrap().len();
            assert!(
                size <= SAFE_PACKET_SIZE || p.upserts.len() == 1,
                "packet too large: {} bytes with {} rows",
                size,
                p.upserts.len()
            );
        }
    }

    #[test]
    fn test_split_delta_empty() {
        let packets = split_delta("machine-a", 0, "empty_table", vec![], vec![]).unwrap();
        assert_eq!(packets.len(), 1);
        assert!(packets[0].is_empty());
    }

    #[test]
    fn test_reassemble_delta_single_part() {
        let mut packet = DeltaPacket::new("machine-a".to_string(), 1, "users".to_string());
        packet.upserts.push(make_row("1", "Alice"));
        packet.total_parts = 1;
        let reassembled = reassemble_delta(&[packet.clone()]);
        assert!(reassembled.is_some());
        assert_eq!(reassembled.unwrap().upserts.len(), 1);
    }

    #[test]
    fn test_reassemble_delta_multi_part() {
        let part0 = DeltaPacket {
            version: PROTOCOL_VERSION,
            source_id: "machine-a".to_string(),
            seq: 5,
            part: 0,
            total_parts: 2,
            table: "users".to_string(),
            upserts: vec![make_row("1", "Alice")],
            deletes: vec!["99".to_string()],
        };
        let mut part1 = part0.clone();
        part1.part = 1;
        part1.upserts = vec![make_row("2", "Bob")];
        part1.deletes = vec![];

        let reassembled = reassemble_delta(&[part0, part1]);
        assert!(reassembled.is_some());
        let merged = reassembled.unwrap();
        assert_eq!(merged.upserts.len(), 2);
        assert_eq!(merged.deletes.len(), 1);
        assert_eq!(merged.seq, 5);
    }

    #[test]
    fn test_reassemble_delta_incomplete_returns_none() {
        let part0 = DeltaPacket {
            version: PROTOCOL_VERSION,
            source_id: "machine-a".to_string(),
            seq: 5,
            part: 0,
            total_parts: 3,
            table: "users".to_string(),
            upserts: vec![make_row("1", "Alice")],
            deletes: vec![],
        };
        assert!(reassemble_delta(&[part0]).is_none());
    }

    #[test]
    fn test_sequence_tracker() {
        let mut tracker = SequenceTracker::new();
        assert_eq!(tracker.current("users"), 0);
        assert_eq!(tracker.next("users"), 0);
        assert_eq!(tracker.next("users"), 1);
        assert_eq!(tracker.next("users"), 2);
        assert_eq!(tracker.current("users"), 3);
        assert_eq!(tracker.next("orders"), 0);
        assert_eq!(tracker.current("users"), 3);
    }

    #[test]
    fn test_split_and_reassemble_roundtrip() {
        let big_value = "x".repeat(500);
        let upserts: Vec<HashMap<String, serde_json::Value>> = (0..20)
            .map(|i| {
                let mut row = HashMap::new();
                row.insert("id".to_string(), serde_json::Value::String(i.to_string()));
                row.insert(
                    "data".to_string(),
                    serde_json::Value::String(big_value.clone()),
                );
                row
            })
            .collect();

        let original_count = upserts.len();
        let packets =
            split_delta("machine-a", 7, "data", upserts, vec!["del1".to_string()]).unwrap();
        assert!(packets.len() > 1);

        let reassembled = reassemble_delta(&packets).unwrap();
        assert_eq!(reassembled.upserts.len(), original_count);
        assert_eq!(reassembled.deletes, vec!["del1".to_string()]);
        assert_eq!(reassembled.seq, 7);
    }

    // -----------------------------------------------------------------------
    // Encryption tests
    // -----------------------------------------------------------------------

    fn test_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        for (i, byte) in key.iter_mut().enumerate() {
            *byte = i as u8;
        }
        key
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = test_key();
        let plaintext = b"hello smuggler broadcast";
        let encrypted = encrypt_packet(plaintext, &key).expect("encrypt");
        let decrypted = decrypt_packet(&encrypted, &key).expect("decrypt");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_wrong_key_fails_authentication() {
        let key_a = test_key();
        let mut key_b = test_key();
        key_b[0] = 0xFF;
        let encrypted = encrypt_packet(b"secret data", &key_a).expect("encrypt");
        let result = decrypt_packet(&encrypted, &key_b);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("authentication failed"), "got: {}", err);
    }

    #[test]
    fn test_tampered_ciphertext_fails() {
        let key = test_key();
        let mut encrypted = encrypt_packet(b"important data", &key).expect("encrypt");
        encrypted[30] ^= 0xFF;
        let result = decrypt_packet(&encrypted, &key);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("authentication failed"), "got: {}", err);
    }

    #[test]
    fn test_nonce_uniqueness() {
        let key = test_key();
        let plaintext = b"same plaintext twice";
        let encrypted_1 = encrypt_packet(plaintext, &key).expect("encrypt 1");
        let encrypted_2 = encrypt_packet(plaintext, &key).expect("encrypt 2");
        assert_ne!(encrypted_1, encrypted_2);
        assert_eq!(decrypt_packet(&encrypted_1, &key).unwrap(), plaintext);
        assert_eq!(decrypt_packet(&encrypted_2, &key).unwrap(), plaintext);
    }

    #[test]
    fn test_packet_too_short() {
        let key = test_key();
        let short_data = vec![0u8; 20];
        let result = decrypt_packet(&short_data, &key);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("packet too short"), "got: {}", err);
    }

    #[test]
    fn test_maybe_encrypt_decrypt_with_key() {
        let key = Some(test_key());
        let plaintext = b"broadcast payload";
        let encrypted = maybe_encrypt(plaintext, &key).expect("encrypt");
        assert_ne!(&encrypted[..], &plaintext[..]);
        let decrypted = maybe_decrypt(&encrypted, &key)
            .expect("decrypt")
            .expect("should not be dropped");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_maybe_encrypt_decrypt_without_key() {
        let plaintext = b"{\"version\":1}";
        let result = maybe_encrypt(plaintext, &None).expect("passthrough");
        assert_eq!(&result[..], &plaintext[..]);
        let decrypted = maybe_decrypt(&result, &None)
            .expect("passthrough")
            .expect("should not be dropped");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_plaintext_mode_drops_encrypted_packet() {
        let key = test_key();
        let encrypted = encrypt_packet(b"secret", &key).expect("encrypt");
        let result = maybe_decrypt(&encrypted, &None).expect("should not error");
        assert!(result.is_none());
    }

    #[test]
    fn test_encrypted_mode_drops_short_plaintext_packet() {
        let key = Some(test_key());
        let plaintext = b"{\"v\":1}";
        let result = maybe_decrypt(plaintext, &key).expect("should not error");
        assert!(result.is_none());
    }

    #[test]
    fn test_encryption_key_parsing() {
        let config = BroadcastConfig {
            secret: Some("a".repeat(64)),
            ..Default::default()
        };
        let key = config.encryption_key().unwrap();
        assert!(key.is_some());
        assert_eq!(key.unwrap(), [0xAA; 32]);

        let config = BroadcastConfig::default();
        assert!(config.encryption_key().unwrap().is_none());

        let config = BroadcastConfig {
            secret: Some("not-hex".to_string()),
            ..Default::default()
        };
        assert!(config.encryption_key().is_err());

        let config = BroadcastConfig {
            secret: Some("aabb".to_string()),
            ..Default::default()
        };
        assert!(config.encryption_key().is_err());
    }

    #[test]
    fn test_announcement_encrypt_decrypt_roundtrip() {
        let key = test_key();
        let announcement =
            Announcement::new("test-machine".to_string(), "abc123hash".to_string(), 31337);
        let plaintext = announcement.to_bytes().expect("serialize");
        let encrypted = encrypt_packet(&plaintext, &key).expect("encrypt");
        let decrypted = decrypt_packet(&encrypted, &key).expect("decrypt");
        let decoded = Announcement::from_bytes(&decrypted).expect("deserialize");
        assert_eq!(announcement, decoded);
    }

    #[test]
    fn test_delta_packet_encrypt_decrypt_roundtrip() {
        let key = test_key();
        let mut packet = DeltaPacket::new("machine-a".to_string(), 1, "users".to_string());
        packet.upserts.push(make_row("1", "Alice"));
        packet.deletes.push("2".to_string());
        let plaintext = packet.to_bytes().expect("serialize");
        let encrypted = encrypt_packet(&plaintext, &key).expect("encrypt");
        let decrypted = decrypt_packet(&encrypted, &key).expect("decrypt");
        let decoded = DeltaPacket::from_bytes(&decrypted).expect("deserialize");
        assert_eq!(packet, decoded);
    }
}
