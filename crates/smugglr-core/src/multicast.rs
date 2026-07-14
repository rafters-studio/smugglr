//! Masterless multicast gossip sync -- the LAN broadcast shape.
//!
//! Spec (`vault/site/homepage.md` -> "LAN broadcast semantics"):
//!
//! - **Masterless / peer-symmetric.** Every node runs the identical loop; there
//!   is no coordinator and no client/server distinction.
//! - **Membership = key possession.** All datagrams are XChaCha20-Poly1305
//!   sealed with a shared key (see [`crate::broadcast::maybe_encrypt`]); holding
//!   the key is the only access control. There is no path-, filename-, or
//!   DB-identity scoping: two replicas of one logical database sync regardless of
//!   where each stores its file. Isolate clusters on one LAN with distinct keys
//!   (a foreign key's datagrams simply fail to decrypt and are dropped).
//! - **Idempotent apply / last-received-wins.** An incoming row is applied with
//!   `INSERT OR REPLACE`; applying the same row twice is a no-op, so lost
//!   datagrams are safe. On same-PK divergence the received row wins.
//! - **Heartbeat reconciliation.** Each node periodically multicasts a
//!   `primary_key -> content_hash` [`Digest`](Body::Digest) for every syncable
//!   table. A peer that hears a digest covering rows it lacks (or hashes
//!   differently) multicasts a [`Want`](Body::Want); whoever holds those rows
//!   answers with a [`Delta`](Body::Delta). Late joiners and partition rejoins
//!   converge through the same path with no special case.
//!
//! ## v0.1 boundaries (named, per the smugglr conflict doctrine -- do not hide)
//!
//! - **Concurrent same-PK divergence resolves silently** (last-received-wins, no
//!   vector clocks/CRDTs). UUIDv7 PKs make this rare; the single-writer-per-row
//!   workload makes it rarer. Visible-conflict / strict-replay is v0.2.
//! - **Deletes** ride the live [`Delta`](Body::Delta) `deletes` list only. The
//!   heartbeat advertises *presence*, so an absent PK is indistinguishable from
//!   not-yet-received -- the digest reconciles upserts/divergence, not deletions.
//!   Tombstone propagation is v0.2.
//! - **Schema is the user's.** smugglr syncs rows, never DDL; a row for a table
//!   that does not exist locally is dropped with a warning.

use crate::broadcast::{
    maybe_decrypt, maybe_encrypt, BroadcastConfig, DeltaPacket, ReplayGuard, DEFAULT_PORT,
    PROTOCOL_VERSION, SAFE_PACKET_SIZE,
};
use crate::config::Config;
use crate::datasource::DataSource;
use crate::error::{Result, SyncError};
use crate::local::LocalDb;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Default administratively-scoped IPv4 multicast group for smugglr gossip.
///
/// 239.0.0.0/8 is the IPv4 "Administratively Scoped" block (RFC 2365): routed
/// within an organization, never onto the public internet.
pub const DEFAULT_GROUP: Ipv4Addr = Ipv4Addr::new(239, 255, 43, 21);

/// Receive buffer: the largest datagram we will accept (one full UDP payload).
///
/// `pub` so callers of [`Gossip::recv_and_handle`] can size the reusable buffer
/// they hoist out of their receive loop (see that method's doc comment).
pub const RECV_BUF: usize = 65_536;

/// Wire headroom reserved when chunking deltas for multicast: the `Msg` JSON
/// wrapper (`{"version":N,"body":{"t":"Delta",...}}`, a fixed ~35 bytes) plus the
/// XChaCha20-Poly1305 nonce+tag (40 bytes). Conservative, so a sealed `Delta`
/// datagram never exceeds [`SAFE_PACKET_SIZE`].
const DELTA_WIRE_RESERVE: usize = 128;

/// Wire headroom reserved when chunking digests for multicast: the
/// XChaCha20-Poly1305 nonce+tag (24 + 16 = 40 bytes) that [`Gossip::seal`] adds
/// on top of the serialized `Msg`. `split_digest` already probes against the
/// full `Msg` envelope, so unlike [`DELTA_WIRE_RESERVE`] only the AEAD seal
/// overhead must be reserved here. Conservative, so a sealed `Digest` datagram
/// never exceeds [`SAFE_PACKET_SIZE`].
const DIGEST_WIRE_RESERVE: usize = 64;

/// A `primary_key -> content_hash` advertisement for one table (one datagram).
///
/// Large tables are chunked across several parts; each part is processed and
/// answered independently (no reassembly), so a lost part only delays the rows
/// it covered until the next heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DigestPacket {
    pub source_id: String,
    pub seq: u64,
    pub part: u16,
    pub total_parts: u16,
    pub table: String,
    /// `primary_key -> content_hash` for this node's rows in `table` (this chunk).
    pub hashes: HashMap<String, String>,
}

/// A request for specific rows the sender is missing or holds a differing hash
/// for. Answered by any node that holds them, over multicast, so every listener
/// converges from a single answer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WantPacket {
    pub source_id: String,
    pub table: String,
    /// Primary keys the sender wants pulled.
    pub pks: Vec<String>,
}

/// The gossip payload carried inside a sealed datagram.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "t")]
pub enum Body {
    Digest(DigestPacket),
    Want(WantPacket),
    Delta(DeltaPacket),
}

/// A versioned gossip datagram. Serialized to JSON, then sealed with the shared
/// key before it hits the wire. Membership is key possession: a node that can
/// decrypt the datagram is on the network. There is no path- or filename-based
/// scoping -- two replicas of one logical database sync regardless of where each
/// stores its file. Run isolated clusters on the same LAN by using different keys.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Msg {
    pub version: u8,
    pub body: Body,
}

impl Msg {
    fn new(body: Body) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            body,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| SyncError::Broadcast(format!("msg serialize: {e}")))
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| SyncError::Broadcast(format!("msg deserialize: {e}")))
    }
}

/// Why a received datagram was dropped without effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IgnoreReason {
    /// Our own multicast, looped back.
    SelfOrigin,
    /// Sequence already seen (replay guard).
    Replay,
    /// Table not in this node's sync set.
    Table,
    /// Protocol version mismatch.
    Version,
    /// Could not decrypt with our key.
    Undecryptable,
    /// Decrypted bytes did not parse as a `Msg`.
    Malformed,
}

/// Outcome of handling one received datagram. Returned for logging and tests.
#[derive(Debug, Clone, PartialEq)]
pub enum GossipEvent {
    /// A digest part was processed; `wanted` rows were requested via a `Want`.
    Digest { table: String, wanted: usize },
    /// A `Want` was answered by multicasting `rows` rows.
    Served { table: String, rows: usize },
    /// A delta was applied; `rows` rows changed locally.
    Applied { table: String, rows: usize },
    /// Dropped without effect; see [`IgnoreReason`].
    Ignored(IgnoreReason),
}

/// Split a `primary_key -> content_hash` map into datagram-sized digest parts
/// (`part`/`total_parts` set, `seq` left 0 for the caller to assign).
///
/// Mirrors [`crate::broadcast::split_delta`]'s sizing and seq convention: entries
/// accumulate until the serialized packet would exceed [`SAFE_PACKET_SIZE`], then
/// a new part starts. The caller stamps a unique seq per part so the replay guard
/// sees one monotonic sequence per source.
pub fn split_digest(
    source_id: &str,
    table: &str,
    hashes: HashMap<String, String>,
) -> Result<Vec<DigestPacket>> {
    let mk = || DigestPacket {
        source_id: source_id.to_string(),
        seq: 0,
        part: 0,
        total_parts: 1,
        table: table.to_string(),
        hashes: HashMap::new(),
    };

    if hashes.is_empty() {
        return Ok(vec![mk()]);
    }

    // Reserve AEAD seal headroom so a *sealed* part stays under the safe MTU,
    // mirroring split_delta's `reserve` discipline (the probe already includes
    // the Msg envelope, so only the 40-byte nonce+tag must be reserved here).
    let limit = SAFE_PACKET_SIZE.saturating_sub(DIGEST_WIRE_RESERVE);

    let mut parts: Vec<DigestPacket> = Vec::new();
    let mut current = mk();

    for (pk, hash) in hashes {
        current.hashes.insert(pk.clone(), hash.clone());
        // Measure with the envelope so we stay under the wire limit end-to-end.
        let probe = Msg::new(Body::Digest(current.clone())).to_bytes()?;
        if probe.len() > limit && current.hashes.len() > 1 {
            current.hashes.remove(&pk);
            parts.push(std::mem::replace(&mut current, mk()));
            current.hashes.insert(pk, hash);
        }
    }
    if !current.hashes.is_empty() {
        parts.push(current);
    }

    let total = parts.len() as u16;
    for (i, p) in parts.iter_mut().enumerate() {
        p.part = i as u16;
        p.total_parts = total;
    }
    Ok(parts)
}

/// The pks a node should pull given a peer's advertised `hashes`: those the peer
/// has that we **lack** or hold a **differing** content hash for.
pub fn want_set(
    peer_hashes: &HashMap<String, String>,
    local_hashes: &HashMap<String, String>,
) -> Vec<String> {
    peer_hashes
        .iter()
        .filter(|(pk, h)| match local_hashes.get(*pk) {
            Some(local) => local != *h, // present but differs -> pull
            None => true,               // absent locally -> pull
        })
        .map(|(pk, _)| pk.clone())
        .collect()
}

/// Bind a UDP socket joined to `group:port` and ready for masterless gossip.
///
/// `SO_REUSEADDR` + `SO_REUSEPORT` let multiple nodes share the port on one host
/// (needed for tests and co-located instances). `multicast_loop` is on so
/// same-host peers hear each other; self-origin datagrams are filtered by
/// `source_id`/`instance_id` in [`Gossip::handle`].
fn bind_multicast(port: u16, group: Ipv4Addr) -> Result<UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};

    fn io(what: &'static str) -> impl Fn(std::io::Error) -> SyncError {
        move |e| SyncError::Broadcast(format!("{what}: {e}"))
    }

    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).map_err(io("socket"))?;
    sock.set_reuse_address(true).map_err(io("SO_REUSEADDR"))?;
    #[cfg(unix)]
    sock.set_reuse_port(true).map_err(io("SO_REUSEPORT"))?;
    sock.set_nonblocking(true).map_err(io("nonblocking"))?;

    let bind = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    sock.bind(&bind.into()).map_err(io("bind"))?;
    sock.join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED)
        .map_err(io("join_multicast_v4"))?;
    sock.set_multicast_loop_v4(true).map_err(io("loop_v4"))?;

    let std_sock: std::net::UdpSocket = sock.into();
    UdpSocket::from_std(std_sock).map_err(io("from_std"))
}

/// A masterless gossip node: one multicast socket plus the identical
/// broadcast/listen loop every peer runs.
pub struct Gossip {
    socket: Arc<UdpSocket>,
    dest: SocketAddrV4,
    instance_id: String,
    key: Option<[u8; 32]>,
    seq: Arc<AtomicU64>,
    replay: Arc<Mutex<ReplayGuard>>,
}

impl Gossip {
    /// Join the multicast group and prepare to gossip. Membership is the shared
    /// key in `broadcast` -- any node that can decrypt is on the network.
    pub async fn bind(broadcast: &BroadcastConfig, group: Ipv4Addr) -> Result<Self> {
        let port = if broadcast.port == 0 {
            DEFAULT_PORT
        } else {
            broadcast.port
        };
        let socket = bind_multicast(port, group)?;
        Ok(Self {
            socket: Arc::new(socket),
            dest: SocketAddrV4::new(group, port),
            instance_id: broadcast.resolve_instance_id(),
            key: broadcast.encryption_key()?,
            seq: Arc::new(AtomicU64::new(0)),
            replay: Arc::new(Mutex::new(ReplayGuard::new())),
        })
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// This node's `primary_key -> content_hash` map for `table`.
    async fn row_hashes(
        &self,
        local: &LocalDb,
        config: &Config,
        table: &str,
    ) -> Result<HashMap<String, String>> {
        Ok(local
            .get_row_metadata(
                table,
                &config.sync.timestamp_column,
                &config.sync.exclude_columns,
            )
            .await?
            .into_iter()
            .map(|(pk, m)| (pk, m.content_hash))
            .collect())
    }

    /// Seal a message body into an on-the-wire datagram (JSON + AEAD).
    fn seal(&self, body: Body) -> Result<Vec<u8>> {
        let plain = Msg::new(body).to_bytes()?;
        maybe_encrypt(&plain, &self.key)
    }

    /// Seal and multicast a batch of bodies to the group.
    async fn emit(&self, bodies: Vec<Body>) -> Result<()> {
        for body in bodies {
            let sealed = self.seal(body)?;
            self.socket
                .send_to(&sealed, self.dest)
                .await
                .map_err(|e| SyncError::Broadcast(format!("multicast send: {e}")))?;
        }
        Ok(())
    }

    /// Build the digest datagrams for one heartbeat: a `primary_key ->
    /// content_hash` advertisement, chunked, for every syncable table.
    pub async fn digest_bodies(&self, local: &LocalDb, config: &Config) -> Result<Vec<Body>> {
        let tables: Vec<String> = local
            .list_tables()
            .await?
            .into_iter()
            .filter(|t| config.should_sync_table(t))
            .collect();

        let mut bodies = Vec::new();
        for table in &tables {
            let hashes = self.row_hashes(local, config, table).await?;
            for mut part in split_digest(&self.instance_id, table, hashes)? {
                part.seq = self.next_seq();
                bodies.push(Body::Digest(part));
            }
        }
        Ok(bodies)
    }

    /// Multicast a content-hash digest for every syncable table -- one heartbeat.
    /// Returns the number of digest datagrams sent.
    pub async fn broadcast_digests(&self, local: &LocalDb, config: &Config) -> Result<usize> {
        let bodies = self.digest_bodies(local, config).await?;
        let n = bodies.len();
        self.emit(bodies).await?;
        Ok(n)
    }

    /// Build delta datagrams for `rows` of `table` (a live write or a `Want`
    /// answer), each part given a unique seq so the replay guard dedupes.
    fn delta_bodies(
        &self,
        table: &str,
        upserts: Vec<HashMap<String, serde_json::Value>>,
        deletes: Vec<String>,
    ) -> Result<Vec<Body>> {
        let parts = crate::broadcast::split_delta(
            &self.instance_id,
            0,
            table,
            upserts,
            deletes,
            DELTA_WIRE_RESERVE,
        )?;
        Ok(parts
            .into_iter()
            .map(|mut part| {
                part.seq = self.next_seq();
                Body::Delta(part)
            })
            .collect())
    }

    /// Multicast `rows` of `table` as a delta. Returns rows sent.
    pub async fn broadcast_delta(
        &self,
        table: &str,
        upserts: Vec<HashMap<String, serde_json::Value>>,
        deletes: Vec<String>,
    ) -> Result<usize> {
        let rows = upserts.len();
        let bodies = self.delta_bodies(table, upserts, deletes)?;
        self.emit(bodies).await?;
        Ok(rows)
    }

    /// Receive one datagram, act on it, and multicast any response.
    ///
    /// This is the whole peer-symmetric loop body: a `Digest` we hear may make us
    /// send a `Want`; a `Want` we hear may make us send a `Delta`; a `Delta` we
    /// hear we apply idempotently.
    ///
    /// `buf` is a scratch receive buffer owned by the caller, at least
    /// [`RECV_BUF`] bytes -- callers running a receive loop should allocate it
    /// once outside the loop and pass the same buffer on every call, so a
    /// steady-state gossip listener does not allocate+zero 64 KiB per datagram.
    pub async fn recv_and_handle(
        &self,
        buf: &mut [u8],
        local: &LocalDb,
        config: &Config,
    ) -> Result<GossipEvent> {
        let (n, _addr) = self
            .socket
            .recv_from(buf)
            .await
            .map_err(|e| SyncError::Broadcast(format!("multicast recv: {e}")))?;
        let (event, out) = self.handle(&buf[..n], local, config).await?;
        self.emit(out).await?;
        Ok(event)
    }

    /// Decode, validate, and decide on one raw datagram. Returns the local effect
    /// plus any datagrams to multicast in response. Pure of socket I/O so the
    /// full protocol is testable deterministically (see tests).
    pub async fn handle(
        &self,
        datagram: &[u8],
        local: &LocalDb,
        config: &Config,
    ) -> Result<(GossipEvent, Vec<Body>)> {
        let none = Vec::new();
        // A datagram we cannot turn into plaintext is not ours: a foreign key
        // (AEAD auth failure), a runt packet, or noise. Drop it as a clean,
        // counted Ignored -- never an error -- so a different-keyed cluster
        // sharing the LAN is silent, not a stream of recv-error logs.
        let plain = match maybe_decrypt(datagram, &self.key) {
            Ok(Some(p)) => p,
            Ok(None) | Err(_) => {
                return Ok((GossipEvent::Ignored(IgnoreReason::Undecryptable), none))
            }
        };
        let msg = match Msg::from_bytes(&plain) {
            Ok(m) => m,
            Err(_) => return Ok((GossipEvent::Ignored(IgnoreReason::Malformed), none)),
        };
        if msg.version != PROTOCOL_VERSION {
            return Ok((GossipEvent::Ignored(IgnoreReason::Version), none));
        }

        match msg.body {
            Body::Digest(d) => self.on_digest(d, local, config).await,
            Body::Want(w) => self.on_want(w, local, config).await,
            Body::Delta(d) => self.on_delta(d, local, config).await,
        }
    }

    async fn on_digest(
        &self,
        d: DigestPacket,
        local: &LocalDb,
        config: &Config,
    ) -> Result<(GossipEvent, Vec<Body>)> {
        if d.source_id == self.instance_id {
            return Ok((GossipEvent::Ignored(IgnoreReason::SelfOrigin), Vec::new()));
        }
        if !self.replay.lock().await.check(&d.source_id, d.seq) {
            return Ok((GossipEvent::Ignored(IgnoreReason::Replay), Vec::new()));
        }
        if !config.should_sync_table(&d.table) {
            return Ok((GossipEvent::Ignored(IgnoreReason::Table), Vec::new()));
        }

        let local_hashes = match self.row_hashes(local, config, &d.table).await {
            Ok(h) => h,
            // No local view (e.g. the table is absent on a late joiner) -> treat
            // as empty so we want everything the peer advertises.
            Err(e) => {
                debug!(
                    "no local hashes for '{}' ({}); treating as empty",
                    d.table, e
                );
                HashMap::new()
            }
        };

        let want = want_set(&d.hashes, &local_hashes);
        let wanted = want.len();
        let out = if want.is_empty() {
            Vec::new()
        } else {
            vec![Body::Want(WantPacket {
                source_id: self.instance_id.clone(),
                table: d.table.clone(),
                pks: want,
            })]
        };
        Ok((
            GossipEvent::Digest {
                table: d.table,
                wanted,
            },
            out,
        ))
    }

    async fn on_want(
        &self,
        w: WantPacket,
        local: &LocalDb,
        config: &Config,
    ) -> Result<(GossipEvent, Vec<Body>)> {
        if w.source_id == self.instance_id {
            return Ok((GossipEvent::Ignored(IgnoreReason::SelfOrigin), Vec::new()));
        }
        if !config.should_sync_table(&w.table) || w.pks.is_empty() {
            return Ok((GossipEvent::Ignored(IgnoreReason::Table), Vec::new()));
        }
        let rows = match local.get_rows(&w.table, &w.pks).await {
            Ok(r) => r,
            Err(e) => {
                warn!("cannot serve '{}': {}", w.table, e);
                return Ok((
                    GossipEvent::Served {
                        table: w.table,
                        rows: 0,
                    },
                    Vec::new(),
                ));
            }
        };
        if rows.is_empty() {
            return Ok((
                GossipEvent::Served {
                    table: w.table,
                    rows: 0,
                },
                Vec::new(),
            ));
        }
        let n = rows.len();
        let out = self.delta_bodies(&w.table, rows, Vec::new())?;
        Ok((
            GossipEvent::Served {
                table: w.table,
                rows: n,
            },
            out,
        ))
    }

    async fn on_delta(
        &self,
        d: DeltaPacket,
        local: &LocalDb,
        config: &Config,
    ) -> Result<(GossipEvent, Vec<Body>)> {
        if d.source_id == self.instance_id {
            return Ok((GossipEvent::Ignored(IgnoreReason::SelfOrigin), Vec::new()));
        }
        if !self.replay.lock().await.check(&d.source_id, d.seq) {
            return Ok((GossipEvent::Ignored(IgnoreReason::Replay), Vec::new()));
        }
        if !config.should_sync_table(&d.table) {
            return Ok((GossipEvent::Ignored(IgnoreReason::Table), Vec::new()));
        }

        let mut changed = 0;
        if !d.upserts.is_empty() {
            match local.upsert_rows(&d.table, &d.upserts).await {
                Ok(n) => changed += n,
                // Schema is the user's; a row for a missing table is dropped.
                Err(e) => warn!("drop delta for table '{}': {}", d.table, e),
            }
        }
        if !d.deletes.is_empty() {
            debug!(
                "delta carries {} deletes for '{}' (live-path only in v0.1)",
                d.deletes.len(),
                d.table
            );
        }
        Ok((
            GossipEvent::Applied {
                table: d.table,
                rows: changed,
            },
            Vec::new(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(db_path: &str) -> Config {
        Config {
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

    fn seed(path: &str, rows: &[(&str, &str)]) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, updated_at TEXT);")
            .unwrap();
        for (id, name) in rows {
            conn.execute(
                "INSERT INTO users VALUES (?1, ?2, '2026-01-01T00:00:00Z')",
                [id, name],
            )
            .unwrap();
        }
    }

    fn count(path: &str) -> i64 {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
            .unwrap()
    }

    fn name_of(path: &str, id: &str) -> Option<String> {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.query_row("SELECT name FROM users WHERE id = ?1", [id], |r| r.get(0))
            .ok()
    }

    #[test]
    fn msg_tag_roundtrip() {
        for body in [
            Body::Want(WantPacket {
                source_id: "a".into(),
                table: "users".into(),
                pks: vec!["1".into()],
            }),
            Body::Digest(DigestPacket {
                source_id: "a".into(),
                seq: 7,
                part: 0,
                total_parts: 1,
                table: "users".into(),
                hashes: HashMap::from([("1".into(), "h".into())]),
            }),
        ] {
            let msg = Msg::new(body);
            let bytes = msg.to_bytes().unwrap();
            assert_eq!(Msg::from_bytes(&bytes).unwrap(), msg);
        }
    }

    #[test]
    fn want_set_lacks_and_differs() {
        let peer = HashMap::from([
            ("1".to_string(), "h1".to_string()),
            ("2".to_string(), "h2".to_string()),
            ("3".to_string(), "h3".to_string()),
        ]);
        let local = HashMap::from([
            ("1".to_string(), "h1".to_string()), // identical -> skip
            ("2".to_string(), "DIFFERENT".to_string()), // differ -> want
                                                 // 3 missing -> want
        ]);
        let mut got = want_set(&peer, &local);
        got.sort();
        assert_eq!(got, vec!["2".to_string(), "3".to_string()]);
    }

    #[test]
    fn split_digest_chunks_large_maps() {
        let big: HashMap<String, String> = (0..2000)
            .map(|i| (format!("pk-{i:08}"), format!("hash-{i:016}")))
            .collect();
        let parts = split_digest("node", "users", big.clone()).unwrap();
        assert!(parts.len() > 1, "2000 entries must span multiple datagrams");
        // Each part fits the wire, parts are numbered 0..total, union is lossless.
        let total = parts.len() as u16;
        let mut nums = std::collections::HashSet::new();
        let mut reunion = HashMap::new();
        for p in &parts {
            let sealed = Msg::new(Body::Digest(p.clone())).to_bytes().unwrap();
            assert!(sealed.len() <= SAFE_PACKET_SIZE, "part exceeds safe size");
            assert_eq!(p.total_parts, total, "total_parts set on every part");
            assert!(nums.insert(p.part), "part numbers must be unique");
            reunion.extend(p.hashes.clone());
        }
        assert_eq!(reunion, big, "chunking must be lossless");
    }

    /// Regression for #168/#173: a sealed digest part must stay under the safe
    /// MTU. Before the fix, split_digest sized parts against the bare `Msg`
    /// envelope up to SAFE_PACKET_SIZE, so adding the 40-byte AEAD seal pushed
    /// the on-the-wire datagram over the limit. Mirrors
    /// `keyed_delta_parts_stay_under_mtu_when_sealed` by sealing each part.
    #[tokio::test]
    async fn keyed_digest_parts_stay_under_mtu_when_sealed() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        let key = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        seed(&a_path, &[]);
        seed(&b_path, &[]);
        let (a, _ac, _al, _b, _bc, _bl) =
            two_nodes_keyed(&a_path, &b_path, Some(key), Some(key)).await;

        let big: HashMap<String, String> = (0..2000)
            .map(|i| (format!("pk-{i:020}"), format!("hash-{i:032}")))
            .collect();
        let parts = split_digest("node-a", "users", big).unwrap();
        assert!(parts.len() > 1, "2000 entries must span multiple datagrams");
        for part in parts {
            let sealed = a.seal(Body::Digest(part)).unwrap();
            assert!(
                sealed.len() <= SAFE_PACKET_SIZE,
                "sealed digest part is {} bytes, exceeds SAFE_PACKET_SIZE {}",
                sealed.len(),
                SAFE_PACKET_SIZE
            );
        }
    }

    /// Bind two gossip nodes with distinct instance ids on a test group. They
    /// share no path or DB identity, so convergence proves sync is scoped by
    /// group/key membership alone, never by file location.
    async fn two_nodes(
        a_path: &str,
        b_path: &str,
    ) -> (Gossip, Config, LocalDb, Gossip, Config, LocalDb) {
        two_nodes_keyed(a_path, b_path, None, None).await
    }

    /// Like `two_nodes` but with explicit per-node encryption keys (hex), so
    /// tests can exercise the sealed wire and cross-key isolation.
    async fn two_nodes_keyed(
        a_path: &str,
        b_path: &str,
        key_a: Option<&str>,
        key_b: Option<&str>,
    ) -> (Gossip, Config, LocalDb, Gossip, Config, LocalDb) {
        let a_cfg = cfg(a_path);
        let b_cfg = cfg(b_path);
        let a_local = LocalDb::open(a_path).unwrap();
        let b_local = LocalDb::open(b_path).unwrap();

        let bc_a = BroadcastConfig {
            instance_id: Some("node-a".into()),
            secret: key_a.map(String::from),
            ..Default::default()
        };
        let bc_b = BroadcastConfig {
            instance_id: Some("node-b".into()),
            secret: key_b.map(String::from),
            ..Default::default()
        };

        let group = Ipv4Addr::new(239, 255, 99, 88);
        let a = Gossip::bind(&bc_a, group).await.unwrap();
        let b = Gossip::bind(&bc_b, group).await.unwrap();
        (a, a_cfg, a_local, b, b_cfg, b_local)
    }

    /// Deliver one body emitted by `sender` into `receiver` -- faithful wire
    /// path (seal -> JSON+AEAD -> handle), no socket-delivery dependency.
    async fn route(
        sender: &Gossip,
        body: Body,
        receiver: &Gossip,
        rlocal: &LocalDb,
        rcfg: &Config,
    ) -> (GossipEvent, Vec<Body>) {
        let sealed = sender.seal(body).unwrap();
        receiver.handle(&sealed, rlocal, rcfg).await.unwrap()
    }

    fn only(mut bodies: Vec<Body>) -> Body {
        assert_eq!(bodies.len(), 1, "expected exactly one response body");
        bodies.remove(0)
    }

    // The full masterless cycle: late joiner converges, divergent edit converges
    // (received wins), and a replayed delta is dropped. Deterministic: every
    // datagram is routed by hand, so this proves the protocol independent of
    // whether the host actually delivers multicast.
    #[tokio::test]
    async fn masterless_convergence_late_joiner_and_idempotency() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();

        // A holds Alice+Bob; B is an empty late joiner with the table only.
        seed(&a_path, &[("1", "Alice"), ("2", "Bob")]);
        seed(&b_path, &[]);
        let (a, a_cfg, a_local, b, b_cfg, b_local) = two_nodes(&a_path, &b_path).await;

        // 1) A heartbeats one digest part.
        let mut digests = a.digest_bodies(&a_local, &a_cfg).await.unwrap();
        assert_eq!(digests.len(), 1, "one digest part for a small table");
        let digest = digests.remove(0);

        // 2) B hears it and wants both rows.
        let (ev, out) = route(&a, digest, &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Digest { wanted: 2, .. }),
            "got {ev:?}"
        );
        let want = only(out);

        // 3) A hears the want and serves both rows.
        let (ev, out) = route(&b, want, &a, &a_local, &a_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Served { rows: 2, .. }),
            "got {ev:?}"
        );
        let delta = only(out);

        // 4) B hears the delta and applies -> converged.
        let (ev, out) = route(&a, delta.clone(), &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Applied { rows: 2, .. }),
            "got {ev:?}"
        );
        assert!(out.is_empty());
        assert_eq!(count(&b_path), 2, "late joiner B converged to A");
        assert_eq!(name_of(&b_path, "1").as_deref(), Some("Alice"));

        // 4b) Replaying the very same delta is dropped by the replay guard.
        let (ev, _) = route(&a, delta, &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Ignored(IgnoreReason::Replay)),
            "got {ev:?}"
        );

        // 5) Divergent edit: A changes Alice, re-heartbeats; B pulls and the
        //    received row wins.
        {
            let conn = rusqlite::Connection::open(&a_path).unwrap();
            conn.execute("UPDATE users SET name = 'Alice2' WHERE id = '1'", [])
                .unwrap();
        }
        let digest = only(a.digest_bodies(&a_local, &a_cfg).await.unwrap());
        let (ev, out) = route(&a, digest, &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Digest { wanted: 1, .. }),
            "got {ev:?}"
        );
        let (_ev, out) = route(&b, only(out), &a, &a_local, &a_cfg).await;
        let (ev, _) = route(&a, only(out), &b, &b_local, &b_cfg).await;
        assert!(matches!(ev, GossipEvent::Applied { .. }), "got {ev:?}");
        assert_eq!(
            name_of(&b_path, "1").as_deref(),
            Some("Alice2"),
            "divergent row resolves last-received-wins"
        );
    }

    // Convergence over the ENCRYPTED wire: the same shared key on both nodes.
    // Proves seal-on-A / decrypt-on-B round-trips real XChaCha20-Poly1305
    // ciphertext -- the production path. The keyless convergence test above
    // exercises only the plaintext passthrough.
    #[tokio::test]
    async fn keyed_convergence() {
        let key = "ab".repeat(32); // 64 hex chars -> valid 256-bit key
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        seed(&a_path, &[("1", "Alice"), ("2", "Bob")]);
        seed(&b_path, &[]);
        let (a, a_cfg, a_local, b, b_cfg, b_local) =
            two_nodes_keyed(&a_path, &b_path, Some(&key), Some(&key)).await;

        let digest = only(a.digest_bodies(&a_local, &a_cfg).await.unwrap());
        let (ev, out) = route(&a, digest, &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Digest { wanted: 2, .. }),
            "got {ev:?}"
        );
        let (ev, out) = route(&b, only(out), &a, &a_local, &a_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Served { rows: 2, .. }),
            "got {ev:?}"
        );
        let (ev, _) = route(&a, only(out), &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Applied { rows: 2, .. }),
            "got {ev:?}"
        );
        assert_eq!(count(&b_path), 2, "encrypted convergence");
        assert_eq!(name_of(&b_path, "1").as_deref(), Some("Alice"));
    }

    // Cross-key isolation -- the only isolation mechanism after db_path_hash was
    // removed, so it is load-bearing. A datagram sealed with key K1 is dropped
    // CLEANLY (Ignored(Undecryptable), never an error) by a node holding K2, and
    // that node's data is untouched. If `handle` ever let the decrypt error
    // propagate instead, `route`'s unwrap would panic here.
    #[tokio::test]
    async fn wrong_key_datagram_is_dropped() {
        let k1 = "ab".repeat(32);
        let k2 = "cd".repeat(32);
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        seed(&a_path, &[("1", "Alice")]);
        seed(&b_path, &[]);
        let (a, a_cfg, a_local, b, b_cfg, b_local) =
            two_nodes_keyed(&a_path, &b_path, Some(&k1), Some(&k2)).await;

        let digest = only(a.digest_bodies(&a_local, &a_cfg).await.unwrap());
        let (ev, out) = route(&a, digest, &b, &b_local, &b_cfg).await;
        assert!(
            matches!(ev, GossipEvent::Ignored(IgnoreReason::Undecryptable)),
            "foreign-key datagram must drop cleanly, got {ev:?}"
        );
        assert!(out.is_empty());
        assert_eq!(count(&b_path), 0, "foreign-key node must not converge");
    }

    // Regression for the AEAD-envelope-overhead bug (#143): a delta chunked for
    // multicast, once SEALED (Msg wrapper + XChaCha20 nonce+tag), must stay
    // within SAFE_PACKET_SIZE. split_delta reserves DELTA_WIRE_RESERVE for it;
    // with a key set the seal adds the full 40-byte AEAD overhead.
    #[tokio::test]
    async fn keyed_delta_parts_stay_under_mtu_when_sealed() {
        let key = "ab".repeat(32);
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        seed(&a_path, &[]);
        seed(&b_path, &[]);
        let (a, _ac, _al, _b, _bc, _bl) =
            two_nodes_keyed(&a_path, &b_path, Some(&key), Some(&key)).await;

        // Enough rows to force multiple delta parts.
        let rows: Vec<HashMap<String, serde_json::Value>> = (0..400)
            .map(|i| {
                HashMap::from([
                    ("id".to_string(), serde_json::json!(format!("pk-{i:020}"))),
                    ("data".to_string(), serde_json::json!("x".repeat(40))),
                ])
            })
            .collect();
        let bodies = a.delta_bodies("users", rows, Vec::new()).unwrap();
        assert!(bodies.len() > 1, "rows should span multiple delta parts");
        for body in bodies {
            let sealed = a.seal(body).unwrap();
            assert!(
                sealed.len() <= SAFE_PACKET_SIZE,
                "sealed delta part is {} bytes, exceeds SAFE_PACKET_SIZE {}",
                sealed.len(),
                SAFE_PACKET_SIZE
            );
        }
    }

    // Live multicast smoke test: proves bind/join/send/recv over a real socket.
    // #[ignore] because multicast loopback delivery is environment-dependent
    // (interface selection, container netns, CI sandboxes). Run on real LAN
    // hardware: `cargo test -p smugglr-core multicast -- --ignored`.
    #[tokio::test]
    #[ignore = "requires multicast-capable loopback; run on real hardware"]
    async fn live_multicast_digest_delivers() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a.db").to_str().unwrap().to_string();
        let b_path = dir.path().join("b.db").to_str().unwrap().to_string();
        seed(&a_path, &[("1", "Alice")]);
        seed(&b_path, &[]);
        let (a, a_cfg, a_local, b, b_cfg, b_local) = two_nodes(&a_path, &b_path).await;

        a.broadcast_digests(&a_local, &a_cfg).await.unwrap();
        let mut recv_buf = vec![0u8; RECV_BUF];
        loop {
            let ev = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                b.recv_and_handle(&mut recv_buf, &b_local, &b_cfg),
            )
            .await
            .expect("multicast recv timed out")
            .expect("recv error");
            if let GossipEvent::Digest { wanted, .. } = ev {
                assert_eq!(wanted, 1, "B should want Alice over the wire");
                break;
            }
        }
    }
}
