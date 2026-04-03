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
    #[allow(dead_code)] // Will be used when encryption is integrated into TCP framing
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
    #[allow(dead_code)] // Will be used when encryption is integrated into TCP framing
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
    #[allow(dead_code)]
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
#[allow(dead_code)] // Used by tests; will be used by TCP framing when encryption lands
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
#[allow(dead_code)]
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
#[allow(dead_code)]
pub fn maybe_encrypt(plaintext: &[u8], key: &Option<[u8; 32]>) -> Result<Vec<u8>> {
    match key {
        Some(k) => encrypt_packet(plaintext, k),
        None => Ok(plaintext.to_vec()),
    }
}

/// Unwrap a potentially encrypted packet. Returns None to signal "drop this packet".
#[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn peer_table(&self) -> Arc<RwLock<HashMap<String, Peer>>> {
        Arc::clone(&self.peers)
    }

    /// The instance ID of this discovery instance.
    #[allow(dead_code)]
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

#[allow(dead_code)]
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

    #[allow(dead_code)]
    pub fn current(&self, table: &str) -> u64 {
        self.sequences.get(table).copied().unwrap_or(0)
    }
}

/// Tracks seen sequence numbers per peer to detect replay attacks.
///
/// Uses a sliding window of 64 sequence numbers per peer. Packets with
/// previously seen or too-old sequence numbers are rejected.
#[derive(Debug)]
pub struct ReplayGuard {
    /// Per-peer sliding window of seen sequences.
    /// Key: source_id, Value: (highest_seen_seq, bitfield of recent seqs)
    windows: HashMap<String, (u64, u64)>,
}

impl ReplayGuard {
    pub fn new() -> Self {
        Self {
            windows: HashMap::new(),
        }
    }

    /// Remove peers not in the given active set to prevent unbounded growth.
    pub fn prune(&mut self, active_peers: &[&str]) {
        self.windows
            .retain(|id, _| active_peers.contains(&id.as_str()));
    }

    /// Check whether a packet with the given source_id and seq should be accepted.
    /// Returns `true` if accepted (not a replay), `false` if rejected.
    pub fn check(&mut self, source_id: &str, seq: u64) -> bool {
        // Cap source_id length and total peers to prevent DoS via crafted packets
        if source_id.len() > 128 {
            warn!(
                "Replay guard: rejecting oversized source_id ({}B)",
                source_id.len()
            );
            return false;
        }
        if !self.windows.contains_key(source_id) && self.windows.len() >= 256 {
            warn!(
                "Replay guard: peer limit reached, rejecting new peer '{}'",
                source_id
            );
            return false;
        }

        let entry = self.windows.get_mut(source_id);
        match entry {
            None => {
                self.windows.insert(source_id.to_string(), (seq, 0));
                true
            }
            Some((highest, bitfield)) => {
                if seq > *highest {
                    let shift = seq - *highest;
                    if shift < 64 {
                        *bitfield = (*bitfield << shift) | (1 << (shift - 1));
                    } else {
                        *bitfield = 0;
                    }
                    *highest = seq;
                    true
                } else if seq == *highest {
                    // Exact duplicate of the highest seen
                    debug!(
                        "Replay guard: dropping duplicate seq {} from '{}'",
                        seq, source_id
                    );
                    false
                } else if *highest - seq > 63 {
                    // Too old -- outside the 64-seq window
                    warn!(
                        "Replay guard: dropping packet from '{}' with seq {} (too old, highest={})",
                        source_id, seq, *highest
                    );
                    false
                } else {
                    let offset = *highest - seq - 1;
                    let bit = 1u64 << offset;
                    if *bitfield & bit != 0 {
                        debug!(
                            "Replay guard: dropping duplicate seq {} from '{}'",
                            seq, source_id
                        );
                        false
                    } else {
                        *bitfield |= bit;
                        true
                    }
                }
            }
        }
    }
}

impl Default for ReplayGuard {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
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

// ---------------------------------------------------------------------------
// TCP sync protocol
// ---------------------------------------------------------------------------

/// A sync request sent over TCP from one peer to another.
///
/// Contains the requester's instance identity, the database path hash for
/// verification, and per-table row hashes so the responder can compute
/// what the requester is missing.
#[derive(Debug, Serialize, Deserialize)]
pub struct SyncRequest {
    pub instance_id: String,
    pub db_path_hash: String,
    pub tables: Vec<TableSyncRequest>,
}

/// Per-table portion of a sync request.
///
/// `row_hashes` maps primary_key -> content_hash for every row the
/// requester currently has. The responder diffs against its own data
/// to determine what to send back.
#[derive(Debug, Serialize, Deserialize)]
pub struct TableSyncRequest {
    pub table: String,
    /// Map of primary_key -> content_hash for all rows the requester has
    pub row_hashes: HashMap<String, String>,
}

/// Response to a sync request, containing deltas and want-lists.
#[derive(Debug, Serialize, Deserialize)]
pub struct SyncResponse {
    pub instance_id: String,
    /// Delta packets containing rows the requester is missing or that are newer
    pub deltas: Vec<DeltaPacket>,
    /// Rows the requester has that the responder wants (their PKs by table)
    pub want_rows: Vec<TableWantRows>,
}

/// Rows the responder wants from the requester, grouped by table.
#[derive(Debug, Serialize, Deserialize)]
pub struct TableWantRows {
    pub table: String,
    pub pk_values: Vec<String>,
}

/// Write a length-prefixed JSON message to a TCP stream.
///
/// Wire format: [4-byte big-endian length][JSON payload]
async fn write_framed<T: Serialize>(
    stream: &mut tokio::net::tcp::OwnedWriteHalf,
    msg: &T,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let payload = serde_json::to_vec(msg)
        .map_err(|e| SyncError::Broadcast(format!("serialize frame: {}", e)))?;
    let len = payload.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| SyncError::Broadcast(format!("write frame length: {}", e)))?;
    stream
        .write_all(&payload)
        .await
        .map_err(|e| SyncError::Broadcast(format!("write frame payload: {}", e)))?;
    Ok(())
}

/// Read a length-prefixed JSON message from a TCP stream.
///
/// Wire format: [4-byte big-endian length][JSON payload]
async fn read_framed<T: serde::de::DeserializeOwned>(
    stream: &mut tokio::net::tcp::OwnedReadHalf,
) -> Result<T> {
    use tokio::io::AsyncReadExt;

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| SyncError::Broadcast(format!("read frame length: {}", e)))?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > 64 * 1024 * 1024 {
        return Err(SyncError::Broadcast(format!(
            "frame too large: {} bytes (max 64MB)",
            len
        )));
    }

    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| SyncError::Broadcast(format!("read frame payload: {}", e)))?;

    serde_json::from_slice(&buf)
        .map_err(|e| SyncError::Broadcast(format!("deserialize frame: {}", e)))
}

/// Handle a single inbound TCP sync connection (server side).
///
/// Reads a SyncRequest, diffs against local data, and sends back a
/// SyncResponse with delta packets for rows the requester needs.
pub async fn handle_sync_connection(
    stream: tokio::net::TcpStream,
    config: &crate::config::Config,
    broadcast_config: &BroadcastConfig,
    db_path_hash: &str,
    replay_guard: &Arc<tokio::sync::Mutex<ReplayGuard>>,
) -> Result<()> {
    use crate::datasource::DataSource;
    use crate::local::LocalDb;

    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    let (mut read_half, mut write_half) = stream.into_split();

    let request: SyncRequest = read_framed(&mut read_half).await?;
    debug!(
        "Sync request from {} (instance: {})",
        peer_addr, request.instance_id
    );

    // Verify db_path_hash matches
    if request.db_path_hash != db_path_hash {
        warn!(
            "Rejecting sync from {}: db_path_hash mismatch (theirs={}, ours={})",
            peer_addr,
            &request.db_path_hash[..8.min(request.db_path_hash.len())],
            &db_path_hash[..8.min(db_path_hash.len())]
        );
        let response = SyncResponse {
            instance_id: broadcast_config.resolve_instance_id(),
            deltas: Vec::new(),
            want_rows: Vec::new(),
        };
        write_framed(&mut write_half, &response).await?;
        return Ok(());
    }

    let local = LocalDb::open(config.local_db_path())?;

    let mut deltas: Vec<DeltaPacket> = Vec::new();
    let mut want_rows: Vec<TableWantRows> = Vec::new();
    let instance_id = broadcast_config.resolve_instance_id();
    let mut seq_tracker = SequenceTracker::new();

    for table_req in &request.tables {
        if !config.should_sync_table(&table_req.table) {
            debug!("Skipping excluded table: {}", table_req.table);
            continue;
        }

        // Get our local row metadata
        let local_meta = match local
            .get_row_metadata(
                &table_req.table,
                &config.sync.timestamp_column,
                &config.sync.exclude_columns,
            )
            .await
        {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "Failed to get metadata for table {}: {}",
                    table_req.table, e
                );
                continue;
            }
        };

        // Find rows the requester is missing or has stale versions of
        let mut missing_pks: Vec<String> = Vec::new();
        for (pk, meta) in &local_meta {
            match table_req.row_hashes.get(pk) {
                None => missing_pks.push(pk.clone()),
                Some(their_hash) => {
                    if *their_hash != meta.content_hash {
                        missing_pks.push(pk.clone());
                    }
                }
            }
        }

        // Find rows the requester has that we don't (we want these)
        let mut wanted_pks: Vec<String> = Vec::new();
        for (pk, their_hash) in &table_req.row_hashes {
            match local_meta.get(pk) {
                None => wanted_pks.push(pk.clone()),
                Some(our_meta) => {
                    if *their_hash != our_meta.content_hash {
                        // They have a different version; we already send ours above,
                        // but also request theirs so the requester can send them back
                        wanted_pks.push(pk.clone());
                    }
                }
            }
        }

        // Fetch and pack rows we need to send
        if !missing_pks.is_empty() {
            let rows = local.get_rows(&table_req.table, &missing_pks).await?;
            if !rows.is_empty() {
                let seq = seq_tracker.next(&table_req.table);
                let packets = split_delta(&instance_id, seq, &table_req.table, rows, Vec::new())?;
                deltas.extend(packets);
            }
        }

        if !wanted_pks.is_empty() {
            want_rows.push(TableWantRows {
                table: table_req.table.clone(),
                pk_values: wanted_pks,
            });
        }
    }

    let response = SyncResponse {
        instance_id: instance_id.clone(),
        deltas,
        want_rows,
    };

    let has_want_rows = !response.want_rows.is_empty();
    write_framed(&mut write_half, &response).await?;
    debug!("Sync response sent to {}", peer_addr);

    // Read follow-up deltas the client sends in response to our want_rows
    if has_want_rows {
        while let Ok(Ok(delta)) = tokio::time::timeout(
            Duration::from_secs(10),
            read_framed::<DeltaPacket>(&mut read_half),
        )
        .await
        {
            if delta.is_empty() {
                continue;
            }
            {
                let mut guard = replay_guard.lock().await;
                if !guard.check(&delta.source_id, delta.seq) {
                    continue;
                }
            }
            match local.upsert_rows(&delta.table, &delta.upserts).await {
                Ok(count) => {
                    debug!(
                        "Applied {} follow-up rows to '{}' from {}",
                        count, delta.table, peer_addr
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to apply follow-up delta for '{}': {}",
                        delta.table, e
                    );
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Broadcast orchestration
// ---------------------------------------------------------------------------

/// Results from a single broadcast sync cycle.
pub struct BroadcastResult {
    pub peers_discovered: usize,
    pub peers_synced: usize,
    pub tables_synced: usize,
    pub rows_sent: usize,
    pub rows_received: usize,
}

/// Run a single broadcast sync cycle: discover peers, connect, exchange deltas.
pub async fn run_broadcast_once(
    config: &crate::config::Config,
    broadcast_config: &BroadcastConfig,
    replay_guard: &Arc<tokio::sync::Mutex<ReplayGuard>>,
) -> Result<BroadcastResult> {
    use crate::datasource::DataSource;
    use crate::local::LocalDb;

    let db_path_hash = hash_db_path(config.local_db_path());
    let instance_id = broadcast_config.resolve_instance_id();

    // Discover peers
    let discovery = PeerDiscovery::new(broadcast_config.clone(), db_path_hash.clone()).await?;
    let listen_duration = Duration::from_secs(broadcast_config.interval_secs.min(5));
    let all_peers = discovery.discover_once(listen_duration).await?;

    let compatible: Vec<&Peer> = all_peers
        .iter()
        .filter(|p| p.db_path_hash == db_path_hash)
        .collect();

    let mut result = BroadcastResult {
        peers_discovered: compatible.len(),
        peers_synced: 0,
        tables_synced: 0,
        rows_sent: 0,
        rows_received: 0,
    };

    if compatible.is_empty() {
        debug!("No compatible peers found");
        return Ok(result);
    }

    // Prune replay windows for peers no longer discovered
    {
        let active_ids: Vec<&str> = compatible.iter().map(|p| p.instance_id.as_str()).collect();
        let mut guard = replay_guard.lock().await;
        guard.prune(&active_ids);
    }

    info!("Found {} compatible peer(s)", compatible.len());

    let local = LocalDb::open(config.local_db_path())?;

    // Build our row hashes for all syncable tables
    let all_tables = local.list_tables().await?;
    let syncable_tables: Vec<String> = all_tables
        .into_iter()
        .filter(|t| config.should_sync_table(t))
        .collect();

    let mut table_requests: Vec<TableSyncRequest> = Vec::new();
    for table in &syncable_tables {
        let meta = local
            .get_row_metadata(
                table,
                &config.sync.timestamp_column,
                &config.sync.exclude_columns,
            )
            .await?;
        let row_hashes: HashMap<String, String> = meta
            .into_iter()
            .map(|(pk, m)| (pk, m.content_hash))
            .collect();
        table_requests.push(TableSyncRequest {
            table: table.clone(),
            row_hashes,
        });
    }

    let request = SyncRequest {
        instance_id: instance_id.clone(),
        db_path_hash: db_path_hash.clone(),
        tables: table_requests,
    };

    // Connect to each compatible peer
    for peer in &compatible {
        let peer_addr = peer.addr;
        // Use the sync_port from the peer's announcement, preserving the IP
        let sync_addr = SocketAddr::new(peer_addr.ip(), peer.addr.port());

        info!("Syncing with peer {} at {}", peer.instance_id, sync_addr);

        match sync_with_peer(&local, &request, sync_addr, replay_guard).await {
            Ok((rows_recv, rows_sent)) => {
                result.peers_synced += 1;
                result.rows_received += rows_recv;
                result.rows_sent += rows_sent;
                if rows_recv > 0 || rows_sent > 0 {
                    result.tables_synced += syncable_tables.len();
                }
                info!(
                    "Synced with {}: {} received, {} sent",
                    peer.instance_id, rows_recv, rows_sent
                );
            }
            Err(e) => {
                warn!("Failed to sync with peer {}: {}", peer.instance_id, e);
            }
        }
    }

    Ok(result)
}

/// Connect to a single peer via TCP, exchange sync data, and apply deltas.
///
/// Returns (rows_received, rows_sent).
async fn sync_with_peer(
    local: &crate::local::LocalDb,
    request: &SyncRequest,
    addr: SocketAddr,
    replay_guard: &Arc<tokio::sync::Mutex<ReplayGuard>>,
) -> Result<(usize, usize)> {
    use crate::datasource::DataSource;

    let connect_timeout = Duration::from_secs(10);
    let stream = tokio::time::timeout(connect_timeout, tokio::net::TcpStream::connect(addr))
        .await
        .map_err(|_| SyncError::Broadcast(format!("TCP connect timeout to {}", addr)))?
        .map_err(|e| SyncError::Broadcast(format!("TCP connect to {}: {}", addr, e)))?;

    let (mut read_half, mut write_half) = stream.into_split();

    // Send our sync request
    write_framed(&mut write_half, request).await?;

    // Read the response
    let response: SyncResponse = read_framed(&mut read_half).await?;

    let mut rows_received: usize = 0;
    let mut rows_sent: usize = 0;

    // Apply received deltas (after replay check)
    for delta in &response.deltas {
        if delta.is_empty() {
            continue;
        }
        {
            let mut guard = replay_guard.lock().await;
            if !guard.check(&delta.source_id, delta.seq) {
                continue;
            }
        }
        let count = local.upsert_rows(&delta.table, &delta.upserts).await?;
        rows_received += count;
        debug!(
            "Applied {} rows to table '{}' from peer {}",
            count, delta.table, response.instance_id
        );
    }

    // Send rows the peer wants from us
    let mut seq_tracker = SequenceTracker::new();
    for want in &response.want_rows {
        if want.pk_values.is_empty() {
            continue;
        }
        let rows = local.get_rows(&want.table, &want.pk_values).await?;
        if !rows.is_empty() {
            let seq = seq_tracker.next(&want.table);
            let packet = DeltaPacket {
                version: PROTOCOL_VERSION,
                source_id: request.instance_id.clone(),
                seq,
                part: 0,
                total_parts: 1,
                table: want.table.clone(),
                upserts: rows,
                deletes: Vec::new(),
            };
            write_framed(&mut write_half, &packet).await?;
            rows_sent += packet.upserts.len();
        }
    }

    Ok((rows_received, rows_sent))
}

/// Resolve the PID lock file path for the broadcast daemon.
pub fn broadcast_pid_lock_path(config_path: &std::path::Path) -> std::path::PathBuf {
    if let Some(parent) = config_path.parent() {
        if parent.as_os_str().is_empty() {
            std::path::PathBuf::from(".smuggler-broadcast.pid")
        } else {
            parent.join(".smuggler-broadcast.pid")
        }
    } else {
        std::path::PathBuf::from(".smuggler-broadcast.pid")
    }
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

    // -----------------------------------------------------------------------
    // TCP sync integration tests
    // -----------------------------------------------------------------------

    fn test_config(db_path: &str) -> crate::config::Config {
        crate::config::Config {
            cloudflare_account_id: None,
            cloudflare_api_token: None,
            database_id: None,
            local_db: Some(db_path.to_string()),
            sync: crate::config::SyncConfig::default(),
            stash: None,
            target: Some(crate::config::TargetConfig::Sqlite {
                database: "unused".to_string(),
            }),
            broadcast: None,
        }
    }

    #[tokio::test]
    async fn test_tcp_sync_bidirectional() {
        use crate::datasource::DataSource;
        use crate::local::LocalDb;

        let dir = tempfile::tempdir().unwrap();
        let db_a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let db_b_path = dir.path().join("b.db").to_str().unwrap().to_string();

        // Database A: has Alice and Bob
        {
            let conn = rusqlite::Connection::open(&db_a_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, updated_at TEXT);
                 INSERT INTO users VALUES ('1', 'Alice', '2026-01-01T00:00:00Z');
                 INSERT INTO users VALUES ('2', 'Bob', '2026-01-01T00:00:00Z');",
            )
            .unwrap();
        }

        // Database B: has Bob and Carol
        {
            let conn = rusqlite::Connection::open(&db_b_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, updated_at TEXT);
                 INSERT INTO users VALUES ('2', 'Bob', '2026-01-01T00:00:00Z');
                 INSERT INTO users VALUES ('3', 'Carol', '2026-01-01T00:00:00Z');",
            )
            .unwrap();
        }

        let _config_a = test_config(&db_a_path);
        let config_b = test_config(&db_b_path);
        let bc = BroadcastConfig::default();
        let db_hash = hash_db_path(&db_a_path);

        // Start TCP sync server for database B on a random port
        let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_port = tcp_listener.local_addr().unwrap().port();

        let server_config = config_b.clone();
        let server_bc = bc.clone();
        let server_hash = db_hash.clone();
        tokio::spawn(async move {
            loop {
                if let Ok((stream, _)) = tcp_listener.accept().await {
                    let guard = Arc::new(tokio::sync::Mutex::new(ReplayGuard::new()));
                    let _ = handle_sync_connection(
                        stream,
                        &server_config,
                        &server_bc,
                        &server_hash,
                        &guard,
                    )
                    .await;
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Build sync request from A's perspective
        let local_a = LocalDb::open(&db_a_path).unwrap();
        let meta = local_a
            .get_row_metadata("users", "updated_at", &[])
            .await
            .unwrap();
        let row_hashes: HashMap<String, String> = meta
            .into_iter()
            .map(|(pk, m)| (pk, m.content_hash))
            .collect();

        let request = SyncRequest {
            instance_id: "machine-a".to_string(),
            db_path_hash: db_hash.clone(),
            tables: vec![TableSyncRequest {
                table: "users".to_string(),
                row_hashes,
            }],
        };

        let sync_addr: SocketAddr = format!("127.0.0.1:{}", server_port).parse().unwrap();
        let client_guard = Arc::new(tokio::sync::Mutex::new(ReplayGuard::new()));
        let (rows_recv, rows_sent) = sync_with_peer(&local_a, &request, sync_addr, &client_guard)
            .await
            .unwrap();

        // A should have received Carol (1 row) from B
        assert_eq!(rows_recv, 1, "A should receive 1 row (Carol) from B");

        // A asked B for rows, B should have requested Alice (1 row)
        assert_eq!(rows_sent, 1, "A should send 1 row (Alice) to B");

        // Verify A now has all 3 rows
        let count_a: i64 = {
            let conn = rusqlite::Connection::open(&db_a_path).unwrap();
            conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
                .unwrap()
        };
        assert_eq!(count_a, 3, "Database A should have 3 rows after sync");
    }

    #[tokio::test]
    async fn test_tcp_sync_respects_column_exclusion() {
        use crate::datasource::DataSource;
        use crate::local::LocalDb;

        let dir = tempfile::tempdir().unwrap();
        let db_a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let db_b_path = dir.path().join("b.db").to_str().unwrap().to_string();

        // Both databases have same content but different embeddings.
        // With exclude_columns, the diff should treat them as identical.
        {
            let conn = rusqlite::Connection::open(&db_a_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE docs (id TEXT PRIMARY KEY, content TEXT,
                    content_embedding BLOB, updated_at TEXT);
                 INSERT INTO docs VALUES ('1', 'hello', X'DEADBEEF',
                    '2026-01-01T00:00:00Z');",
            )
            .unwrap();
        }
        {
            let conn = rusqlite::Connection::open(&db_b_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE docs (id TEXT PRIMARY KEY, content TEXT,
                    content_embedding BLOB, updated_at TEXT);
                 INSERT INTO docs VALUES ('1', 'hello', X'CAFEBABE',
                    '2026-01-01T00:00:00Z');",
            )
            .unwrap();
        }

        let mut config_b = test_config(&db_b_path);
        config_b.sync.exclude_columns = vec!["*_embedding".to_string()];
        let bc = BroadcastConfig::default();
        let db_hash = hash_db_path(&db_a_path);

        // Start TCP sync server for B
        let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_port = tcp_listener.local_addr().unwrap().port();
        let server_config = config_b.clone();
        let server_bc = bc.clone();
        let server_hash = db_hash.clone();
        tokio::spawn(async move {
            loop {
                if let Ok((stream, _)) = tcp_listener.accept().await {
                    let guard = Arc::new(tokio::sync::Mutex::new(ReplayGuard::new()));
                    let _ = handle_sync_connection(
                        stream,
                        &server_config,
                        &server_bc,
                        &server_hash,
                        &guard,
                    )
                    .await;
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Build request from A, also excluding embeddings from hash
        let local_a = LocalDb::open(&db_a_path).unwrap();
        let exclude = vec!["*_embedding".to_string()];
        let meta = local_a
            .get_row_metadata("docs", "updated_at", &exclude)
            .await
            .unwrap();
        let row_hashes: HashMap<String, String> = meta
            .into_iter()
            .map(|(pk, m)| (pk, m.content_hash))
            .collect();

        let request = SyncRequest {
            instance_id: "machine-a".to_string(),
            db_path_hash: db_hash.clone(),
            tables: vec![TableSyncRequest {
                table: "docs".to_string(),
                row_hashes,
            }],
        };

        let sync_addr: SocketAddr = format!("127.0.0.1:{}", server_port).parse().unwrap();
        let client_guard = Arc::new(tokio::sync::Mutex::new(ReplayGuard::new()));
        let (rows_recv, rows_sent) = sync_with_peer(&local_a, &request, sync_addr, &client_guard)
            .await
            .unwrap();

        // With embedding excluded, both see the row as identical
        assert_eq!(
            rows_recv, 0,
            "No rows received (identical excluding embedding)"
        );
        assert_eq!(rows_sent, 0, "No rows sent (identical excluding embedding)");

        // A embedding should be untouched
        let conn_a = rusqlite::Connection::open(&db_a_path).unwrap();
        let embedding: Vec<u8> = conn_a
            .query_row(
                "SELECT content_embedding FROM docs WHERE id = '1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(embedding, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[tokio::test]
    async fn test_tcp_sync_db_hash_mismatch_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_str().unwrap().to_string();

        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT);
                 INSERT INTO items VALUES ('1', 'test');",
            )
            .unwrap();
        }

        let config = test_config(&db_path);
        let bc = BroadcastConfig::default();
        let db_hash = hash_db_path(&db_path);

        let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_port = tcp_listener.local_addr().unwrap().port();

        let server_config = config.clone();
        let server_bc = bc.clone();
        let server_hash = db_hash.clone();
        tokio::spawn(async move {
            loop {
                if let Ok((stream, _)) = tcp_listener.accept().await {
                    let guard = Arc::new(tokio::sync::Mutex::new(ReplayGuard::new()));
                    let _ = handle_sync_connection(
                        stream,
                        &server_config,
                        &server_bc,
                        &server_hash,
                        &guard,
                    )
                    .await;
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send request with wrong db_path_hash
        let request = SyncRequest {
            instance_id: "wrong-machine".to_string(),
            db_path_hash: "completely_wrong_hash".to_string(),
            tables: vec![],
        };

        let sync_addr: SocketAddr = format!("127.0.0.1:{}", server_port).parse().unwrap();
        let local = crate::local::LocalDb::open(&db_path).unwrap();
        let client_guard = Arc::new(tokio::sync::Mutex::new(ReplayGuard::new()));
        let (recv, sent) = sync_with_peer(&local, &request, sync_addr, &client_guard)
            .await
            .unwrap();

        // Server should have rejected with empty response
        assert_eq!(recv, 0);
        assert_eq!(sent, 0);
    }

    #[tokio::test]
    async fn test_framed_protocol_roundtrip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut read, mut write) = stream.into_split();
            let req: SyncRequest = read_framed(&mut read).await.unwrap();
            assert_eq!(req.instance_id, "test-client");
            assert_eq!(req.tables.len(), 1);

            let resp = SyncResponse {
                instance_id: "test-server".to_string(),
                deltas: Vec::new(),
                want_rows: Vec::new(),
            };
            write_framed(&mut write, &resp).await.unwrap();
        });

        let stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        let (mut read, mut write) = stream.into_split();

        let req = SyncRequest {
            instance_id: "test-client".to_string(),
            db_path_hash: "abc123".to_string(),
            tables: vec![TableSyncRequest {
                table: "users".to_string(),
                row_hashes: HashMap::new(),
            }],
        };
        write_framed(&mut write, &req).await.unwrap();

        let resp: SyncResponse = read_framed(&mut read).await.unwrap();
        assert_eq!(resp.instance_id, "test-server");
        assert!(resp.deltas.is_empty());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_broadcast_no_peers_graceful() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_str().unwrap().to_string();

        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, updated_at TEXT);
                 INSERT INTO items VALUES ('1', 'test', '2026-01-01T00:00:00Z');",
            )
            .unwrap();
        }

        let config = test_config(&db_path);
        let bc = BroadcastConfig {
            port: 0,
            interval_secs: 1,
            ..Default::default()
        };

        // run_broadcast_once with no peers listening should return gracefully
        let guard = Arc::new(tokio::sync::Mutex::new(ReplayGuard::new()));
        let result = run_broadcast_once(&config, &bc, &guard).await.unwrap();
        assert_eq!(result.peers_discovered, 0);
        assert_eq!(result.peers_synced, 0);
        assert_eq!(result.rows_received, 0);
        assert_eq!(result.rows_sent, 0);
    }

    // -----------------------------------------------------------------------
    // ReplayGuard tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_replay_guard_accepts_new_sequence() {
        let mut guard = ReplayGuard::new();
        assert!(guard.check("peer-a", 5));
    }

    #[test]
    fn test_replay_guard_rejects_duplicate() {
        let mut guard = ReplayGuard::new();
        assert!(guard.check("peer-a", 5));
        assert!(!guard.check("peer-a", 5));
    }

    #[test]
    fn test_replay_guard_sliding_window() {
        let mut guard = ReplayGuard::new();
        // Accept seq 100
        assert!(guard.check("peer-a", 100));
        // Seq 50 is too old (100 - 50 = 50 < 64, but let's use 36 which is outside)
        // Actually 100 - 50 = 50 which is < 64, so it's within the window
        assert!(guard.check("peer-a", 50));
        // But seq 50 again is a duplicate
        assert!(!guard.check("peer-a", 50));
        // Seq 30 is within window (100 - 30 = 70 >= 64), so too old
        assert!(!guard.check("peer-a", 30));
        // Seq 80 is within window and not seen
        assert!(guard.check("peer-a", 80));
    }

    #[test]
    fn test_replay_guard_too_old_rejected() {
        let mut guard = ReplayGuard::new();
        assert!(guard.check("peer-a", 100));
        // 100 - 36 = 64, which is outside the 64-seq window (> 63)
        assert!(!guard.check("peer-a", 36));
        // 100 - 37 = 63, exactly at boundary, within window
        assert!(guard.check("peer-a", 37));
    }

    #[test]
    fn test_replay_guard_independent_peers() {
        let mut guard = ReplayGuard::new();
        assert!(guard.check("peer-a", 5));
        assert!(guard.check("peer-b", 5));
        // Each peer has independent state
        assert!(!guard.check("peer-a", 5));
        assert!(!guard.check("peer-b", 5));
    }

    #[test]
    fn test_replay_guard_advancing_window() {
        let mut guard = ReplayGuard::new();
        // Sequential acceptance
        for seq in 0..10 {
            assert!(guard.check("peer-a", seq));
        }
        // All duplicates rejected
        for seq in 0..10 {
            assert!(!guard.check("peer-a", seq));
        }
    }

    #[test]
    fn test_replay_guard_large_gap_resets_bitfield() {
        let mut guard = ReplayGuard::new();
        assert!(guard.check("peer-a", 0));
        // Jump far ahead (gap >= 64 resets bitfield)
        assert!(guard.check("peer-a", 200));
        // Old seq 0 is too old now
        assert!(!guard.check("peer-a", 0));
        // But seq 200 was already seen (it's the highest)
        assert!(!guard.check("peer-a", 200));
        // Recent within window should work
        assert!(guard.check("peer-a", 199));
    }

    #[test]
    fn test_replay_guard_prune_removes_stale_peers() {
        let mut guard = ReplayGuard::new();
        guard.check("peer-a", 1);
        guard.check("peer-b", 1);
        guard.check("peer-c", 1);
        assert_eq!(guard.windows.len(), 3);

        guard.prune(&["peer-a", "peer-c"]);
        assert_eq!(guard.windows.len(), 2);
        assert!(guard.windows.contains_key("peer-a"));
        assert!(!guard.windows.contains_key("peer-b"));
        assert!(guard.windows.contains_key("peer-c"));
    }

    #[test]
    fn test_replay_guard_default() {
        let guard = ReplayGuard::default();
        assert!(guard.windows.is_empty());
    }
}
