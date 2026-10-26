//! smugglr mesh: nodes on a local network kept in step with nobody in
//! charge (FR-SMUGGLR-078).
//!
//! [`run`] runs one node until it is told to stop. It finds its own changes
//! by comparing each row's hash with its last hash per key (multi's stash),
//! and sends each change as it happens: one envelope (FR-SMUGGLR-075),
//! signed and sealed under the shared key, broadcast over UDP on the node's
//! port. A node that applies a change (multi's retrieve, by the envelope
//! rules) acknowledges it to its maker and echoes it once, 2 to 4 seconds
//! later, unless it heard it `k` more times in that wait (RFC 6206's
//! suppression). The maker resends while any node it has heard has not
//! acknowledged, the interval doubling from 10 s to at most 160 s, until
//! every one has or the change's sunset passes. There is no member list:
//! a node's peers are the nodes it hears under the shared key.
//!
//! The keys that seal datagrams and sign envelopes are each derived from
//! the shared key, so one key never serves two jobs. No key is ever
//! written out; a key is named only by its `key_id`.

mod node;
mod wire;

use std::sync::Arc;

use smugglr_core::plan::SyncMode;
use smugglr_core::steering::Steering;
use smugglr_memory::Store;

pub use node::run;

/// The UDP port a node broadcasts and listens on when the config sets none.
pub const PORT: u16 = 31337;

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
#[derive(Clone)]
pub struct Settings {
    /// Where smugglr keeps what the node needs: files in smugglr's config
    /// directory.
    pub store: Arc<dyn Store>,
    /// The shared keys the node holds, newest last: it signs and seals with
    /// the newest, and accepts the newest and the one before it.
    pub keys: Vec<[u8; 32]>,
    /// The node's name, which every envelope it makes carries: its
    /// machine's name.
    pub node: String,
    /// The UDP port the node broadcasts and listens on.
    pub port: u16,
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

impl std::fmt::Debug for Settings {
    /// Names each key only by its `key_id`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys: Vec<String> = self.keys.iter().map(key_id).collect();
        f.debug_struct("Settings")
            .field("keys", &keys)
            .field("node", &self.node)
            .field("port", &self.port)
            .field("graveyard", &self.graveyard)
            .field("sync_mode", &self.sync_mode)
            .field("k", &self.k)
            .finish_non_exhaustive()
    }
}

/// The `key_id` the envelopes signed under the shared key `key` carry.
#[must_use]
pub fn key_id(key: &[u8; 32]) -> String {
    smugglr_multi::key_id(&signing(key))
}

/// A new shared key from the operating system's random source.
#[must_use]
pub fn new_key() -> [u8; 32] {
    let mut key = [0; 32];
    chacha20poly1305::aead::rand_core::RngCore::fill_bytes(
        &mut chacha20poly1305::aead::OsRng,
        &mut key,
    );
    key
}

/// The key that signs envelopes, derived from the shared key.
fn signing(key: &[u8; 32]) -> [u8; 32] {
    blake3::derive_key("smugglr mesh 0.6 envelope signature", key)
}

/// The key that seals datagrams, derived from the shared key.
fn sealing(key: &[u8; 32]) -> [u8; 32] {
    blake3::derive_key("smugglr mesh 0.6 datagram seal", key)
}

#[cfg(test)]
mod tests {
    use super::{key_id, sealing, signing};

    #[test]
    fn the_sealing_and_signing_keys_differ_from_each_other_and_the_shared_key() {
        let shared = [7; 32];
        assert_ne!(sealing(&shared), signing(&shared));
        assert_ne!(sealing(&shared), shared);
        assert_ne!(signing(&shared), shared);
    }

    #[test]
    fn a_key_id_is_the_same_for_the_same_key_and_differs_for_another() {
        assert_eq!(key_id(&[7; 32]), key_id(&[7; 32]));
        assert_ne!(key_id(&[7; 32]), key_id(&[5; 32]));
        assert_eq!(key_id(&[7; 32]).len(), 16);
    }
}
