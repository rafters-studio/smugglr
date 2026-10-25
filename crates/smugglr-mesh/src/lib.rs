//! smugglr mesh: nodes on a local network kept in step with nobody in
//! charge (FR-SMUGGLR-078).
//!
//! [`run`] runs one node until it is told to stop. It finds its own changes
//! by comparing each row's hash with its last hash per key (multi's stash),
//! and sends each change as it happens: one envelope (FR-SMUGGLR-075),
//! sealed under the shared key, broadcast over UDP. A node that applies a
//! change (multi's retrieve, by the envelope rules) acknowledges it to its
//! maker and echoes it once, 2 to 4 seconds later, unless it heard it `k`
//! more times in that wait (RFC 6206's suppression). The maker resends to
//! every peer that has not acknowledged, the interval doubling from 10 s to
//! at most 160 s, until every one has or the change's sunset passes. A
//! node's peers are the nodes it hears; it keeps them, and its own `UUIDv7`,
//! in smugglr's own files.

mod node;
mod wire;

use std::net::SocketAddr;
use std::sync::Arc;

use smugglr_core::plan::SyncMode;
use smugglr_core::steering::Steering;
use smugglr_memory::Store;

pub use node::run;

/// The UDP port a node listens on and broadcasts to unless set.
pub const PORT: u16 = 47820;

/// How many more copies of a change a node may hear in its echo wait before
/// it no longer echoes the change, unless set.
pub const K: u32 = 2;

/// Why a node could not run.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The node's place could not be reached, read or written.
    #[error(transparent)]
    Place(#[from] smugglr_place::Error),
    /// What smugglr keeps for the node could not be read or written.
    #[error(transparent)]
    Memory(#[from] smugglr_memory::Error),
    /// A stash or a retrieve could not run.
    #[error(transparent)]
    Multi(#[from] smugglr_multi::Error),
    /// The node's socket could not be opened, or a datagram not sent.
    #[error("the mesh socket: {0}")]
    Socket(#[from] std::io::Error),
    /// The node holds no key.
    #[error("the mesh needs a shared key")]
    NoKey,
}

/// How a node runs.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Where smugglr keeps what the node needs: files in smugglr's config
    /// directory.
    pub store: Arc<dyn Store>,
    /// The shared keys the node holds, newest last: it seals and signs with
    /// the newest, and accepts the one before it.
    pub keys: Vec<[u8; 32]>,
    /// The UDP port the node listens on.
    pub port: u16,
    /// Where the node sends: the broadcast address on its port unless set.
    pub send_to: SocketAddr,
    /// The graveyard window in seconds: each change's sunset is the time it
    /// is found plus this.
    pub graveyard: u64,
    /// What happens to a row changed here and in a change: stop or last.
    pub sync_mode: SyncMode,
    /// The steering columns `last` reads.
    pub steering: Steering,
    /// The tables to ignore, beside those smugglr always ignores.
    pub ignore: Vec<String>,
    /// How many more copies heard in the echo wait suppress the echo.
    pub k: u32,
}
