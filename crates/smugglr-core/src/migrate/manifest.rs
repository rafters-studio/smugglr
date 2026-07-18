//! The migration manifest: a structured, content-hashed, optionally-encrypted
//! description of one migration.
//!
//! A migration is one self-describing object -- an ordered list of **structured
//! ops** (never raw SQL strings; the op is the contract, generation to dialect
//! SQL happens in `apply`/`generator`), a SHA-256 checksum over the canonical
//! body, and metadata (version, target schema, reversibility flags, optional
//! pre-image). See `docs/plans/migration.md` (decisions 1, 2, 4, 10).
//!
//! Two integrity layers, matching the design's "integrity always-on,
//! confidentiality opt-in" split:
//! - [`ChecksummedManifest`] -- **always available, always on**. A SHA-256 over
//!   the canonical manifest body gives a stable content identity for the ledger
//!   and skip-if-applied, and detects accidental corruption. Reuses `sha2`, the
//!   hash smugglr already depends on (decision 10; no BLAKE3).
//! - [`Envelope`] -- **opt-in, native-only for 0.5.0**. Wraps a checksummed
//!   manifest in the existing XChaCha20-Poly1305 AEAD (decision 2), reusing
//!   `broadcast`'s `maybe_encrypt`/`maybe_decrypt` and the PSK model. It buys
//!   confidentiality for at-rest / untrusted-relay carriage; it is not a new
//!   crypto subsystem and adds no dependency. The WASM/edge envelope defers with
//!   remote apply, so there is no `Cargo.toml` change and no `wasm32` gate here.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Errors from the migrate subsystem.
///
/// Bridged into [`crate::error::SyncError`] via `SyncError::Migrate` so the whole
/// migrate tree can surface through the crate's one error type. Kept always-
/// compiled (no `native` gate) so the bridge exists on every target; the
/// envelope's crypto failures fold into [`MigrateError::Envelope`] as a string
/// rather than leaking a native-only type into the enum.
#[derive(Error, Debug)]
pub enum MigrateError {
    /// The manifest's stored checksum does not match its recomputed body hash --
    /// the manifest was altered (or corrupted) after it was sealed.
    #[error("manifest checksum mismatch: expected {expected}, computed {actual}")]
    Checksum { expected: String, actual: String },

    /// Serializing or deserializing the manifest body failed.
    #[error("manifest serialization error: {0}")]
    Serialization(String),

    /// Sealing or opening the AEAD envelope failed (encryption, decryption, or a
    /// too-short/garbage ciphertext).
    #[error("migration envelope error: {0}")]
    Envelope(String),
}

/// A structured column definition, used by table-shaping ops.
///
/// The type is an opaque SQLite type token; the manifest never interprets it
/// (that is `apply`/`schema_projection`'s job). Keeping it structured -- rather
/// than a raw column-clause string -- is what lets the lint and reverse layers
/// reason about a change (settled decision: structured ops, not raw SQL).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// Declared SQLite type token, opaque to the manifest (e.g. `TEXT`,
    /// `INTEGER`, `BLOB`).
    pub column_type: String,
    /// Whether the column is `NOT NULL`.
    #[serde(default)]
    pub not_null: bool,
    /// Whether the column participates in the primary key.
    #[serde(default)]
    pub primary_key: bool,
    /// An optional SQL-side default expression, carried verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

/// A single structured migration operation.
///
/// This is deliberately an enum of shapes, **not** a raw SQL string: the ledger
/// hashes it, the lint classifies it ([`Op::class`]), and reverse inverts it. The
/// dialect SQL is generated from the op at apply time (one dialect, N apply
/// strategies -- decision 8), never stored here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// Create a table with the given columns.
    CreateTable {
        table: String,
        columns: Vec<Column>,
        #[serde(default)]
        without_rowid: bool,
    },
    /// Drop a table (destructive -- loses all rows).
    DropTable { table: String },
    /// Add a column to an existing table (additive).
    AddColumn { table: String, column: Column },
    /// Drop a column from an existing table (destructive -- loses that column).
    DropColumn { table: String, column: String },
    /// Create an index.
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        #[serde(default)]
        unique: bool,
    },
    /// Drop an index (additive to invert -- the index definition is recreatable).
    DropIndex { name: String },
    /// Rename a table.
    RenameTable { from: String, to: String },
    /// Rename a column.
    RenameColumn {
        table: String,
        from: String,
        to: String,
    },
}

/// The reversibility / safety class of an op (decisions 4 and 5).
///
/// Drives the destructive-lint and pre-image capture: additive ops reverse for
/// free; destructive ops reverse only against a captured pre-image; hash-
/// rewriting ops force the version-gate on row-sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpClass {
    /// Structurally reversible with no data carried (drop what was added).
    Additive,
    /// Loses data unless a pre-image was captured first.
    Destructive,
    /// Rewrites content hashes (forces the row-sync version gate).
    HashRewriting,
}

impl Op {
    /// Classify this op for the destructive-lint.
    ///
    /// 0.5.0 covers the structural ops; the row-transforming ops (backfills) that
    /// are `HashRewriting` arrive with the apply engine, so no op here is yet
    /// classified `HashRewriting` -- the variant exists for the lint (#275) and
    /// the version-gate (deferred) to key on.
    pub fn class(&self) -> OpClass {
        match self {
            Op::CreateTable { .. }
            | Op::AddColumn { .. }
            | Op::CreateIndex { .. }
            | Op::DropIndex { .. }
            | Op::RenameTable { .. }
            | Op::RenameColumn { .. } => OpClass::Additive,
            Op::DropTable { .. } | Op::DropColumn { .. } => OpClass::Destructive,
        }
    }
}

/// Reversibility flags for the whole manifest (mirrors the design sketch).
///
/// Derivable from the ops, but carried explicitly so a reader (and the ledger)
/// can gate on them without re-classifying every op.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Flags {
    /// Any op loses data.
    #[serde(default)]
    pub destructive: bool,
    /// Any op rewrites content hashes.
    #[serde(default)]
    pub hash_rewriting: bool,
}

/// The captured pre-image for a destructive migration (decision 4).
///
/// Present only for destructive ops; it is what makes the reverse honest. The
/// concrete capture (delta-scoped rows vs a relay ref) is filled in by reverse
/// (#274); the manifest only carries the reference shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Preimage {
    /// The pre-image is embedded inline (small captures only).
    Inline { rows: serde_json::Value },
    /// The pre-image lives in a content-addressed store, referenced by key.
    Ref { key: String },
}

/// A migration manifest: the ordered ops plus metadata.
///
/// This is the manifest *body* -- the thing the checksum is computed over. Seal
/// it into a [`ChecksummedManifest`] to get a stable content identity, and
/// optionally into an [`Envelope`] for confidentiality.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Monotonic migration version (authored order; see "Migrate x sync").
    pub version: u64,
    /// The schema this migration expects to apply against.
    ///
    /// **Opaque for 0.5.0.** It is stored and round-tripped but never compared;
    /// the semantic schema projection that turns it into a drift check arrives
    /// with reconcile (#290 / `schema_projection`). Treating it as an opaque
    /// `String` now avoids baking in the raw-`sqlite_master`-text hash the
    /// reconcile finding warns against.
    pub target_schema: String,
    /// Forward ops, in order.
    pub up: Vec<Op>,
    /// Reverse ops, in order (structural inverse for additive migrations).
    #[serde(default)]
    pub down: Vec<Op>,
    /// Pre-image for destructive ops, if captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preimage: Option<Preimage>,
    /// Reversibility flags.
    #[serde(default)]
    pub flags: Flags,
    /// Provenance author id (v0.1 informational; signatures are a later layer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
}

impl Manifest {
    /// Serialize the canonical manifest body.
    ///
    /// serde_json emits struct fields in declaration order, so the byte stream is
    /// stable for a given manifest value -- which is what the checksum needs.
    fn canonical_bytes(&self) -> Result<Vec<u8>, MigrateError> {
        serde_json::to_vec(self).map_err(|e| MigrateError::Serialization(e.to_string()))
    }

    /// The SHA-256 hex digest of the canonical body.
    pub fn checksum(&self) -> Result<String, MigrateError> {
        let bytes = self.canonical_bytes()?;
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        Ok(hex::encode(hasher.finalize()))
    }
}

/// A manifest paired with the SHA-256 checksum of its canonical body.
///
/// Integrity is always-on: sealing computes the checksum, [`Self::verify`]
/// recomputes and compares. This is the stable content identity the ledger and
/// skip-if-applied key on (decision 10).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChecksummedManifest {
    /// The manifest body.
    pub manifest: Manifest,
    /// SHA-256 hex digest of the canonical body of `manifest`.
    pub checksum: String,
}

impl ChecksummedManifest {
    /// Seal a manifest by computing its checksum.
    pub fn seal(manifest: Manifest) -> Result<Self, MigrateError> {
        let checksum = manifest.checksum()?;
        Ok(Self { manifest, checksum })
    }

    /// Verify the stored checksum against a fresh hash of the body.
    ///
    /// Returns [`MigrateError::Checksum`] on mismatch (the body was altered after
    /// sealing).
    pub fn verify(&self) -> Result<(), MigrateError> {
        let actual = self.manifest.checksum()?;
        if actual == self.checksum {
            Ok(())
        } else {
            Err(MigrateError::Checksum {
                expected: self.checksum.clone(),
                actual,
            })
        }
    }
}

/// The confidentiality envelope: a checksummed manifest sealed under the PSK.
///
/// **Native-only for 0.5.0.** It reuses `broadcast`'s XChaCha20-Poly1305 helpers
/// (which are themselves native-gated) and the existing 256-bit PSK -- no new
/// crypto, no new dependency, no `Cargo.toml` change, no `wasm32` gate. Sealing
/// is opt-in; integrity (the checksum) is always available without it.
#[cfg(feature = "native")]
pub struct Envelope;

#[cfg(feature = "native")]
impl Envelope {
    /// Seal a checksummed manifest into an encrypted byte blob under `key`.
    ///
    /// The manifest is JSON-serialized and encrypted with the existing AEAD via
    /// [`crate::broadcast::maybe_encrypt`]. The result carries integrity (AEAD
    /// tag on the wire) *and* the embedded SHA-256 checksum.
    pub fn seal(manifest: &ChecksummedManifest, key: &[u8; 32]) -> Result<Vec<u8>, MigrateError> {
        let plaintext =
            serde_json::to_vec(manifest).map_err(|e| MigrateError::Serialization(e.to_string()))?;
        crate::broadcast::maybe_encrypt(&plaintext, &Some(*key))
            .map_err(|e| MigrateError::Envelope(e.to_string()))
    }

    /// Open an encrypted envelope, decrypting under `key`, deserializing, and
    /// verifying the embedded checksum.
    ///
    /// A too-short or garbage envelope is a hard failure:
    /// [`crate::broadcast::maybe_decrypt`] returns `Ok(None)` to signal "drop
    /// this packet" on the gossip path, but a migration envelope that fails to
    /// open is an error, never a silent skip -- so `None` maps to
    /// [`MigrateError::Envelope`].
    pub fn open(data: &[u8], key: &[u8; 32]) -> Result<ChecksummedManifest, MigrateError> {
        let plaintext = crate::broadcast::maybe_decrypt(data, &Some(*key))
            .map_err(|e| MigrateError::Envelope(e.to_string()))?
            .ok_or_else(|| {
                MigrateError::Envelope(
                    "envelope could not be opened (too short or not encrypted under this key)"
                        .to_string(),
                )
            })?;
        let manifest: ChecksummedManifest = serde_json::from_slice(&plaintext)
            .map_err(|e| MigrateError::Serialization(e.to_string()))?;
        manifest.verify()?;
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> Manifest {
        Manifest {
            version: 1,
            target_schema: "opaque-schema-id".to_string(),
            up: vec![Op::AddColumn {
                table: "users".to_string(),
                column: Column {
                    name: "email".to_string(),
                    column_type: "TEXT".to_string(),
                    not_null: false,
                    primary_key: false,
                    default: None,
                },
            }],
            down: vec![Op::DropColumn {
                table: "users".to_string(),
                column: "email".to_string(),
            }],
            preimage: None,
            flags: Flags::default(),
            author: Some("agent-x".to_string()),
        }
    }

    #[test]
    fn op_class_partitions_additive_and_destructive() {
        assert_eq!(
            Op::AddColumn {
                table: "t".into(),
                column: Column {
                    name: "c".into(),
                    column_type: "TEXT".into(),
                    not_null: false,
                    primary_key: false,
                    default: None,
                },
            }
            .class(),
            OpClass::Additive
        );
        assert_eq!(
            Op::DropColumn {
                table: "t".into(),
                column: "c".into(),
            }
            .class(),
            OpClass::Destructive
        );
        assert_eq!(
            Op::DropTable { table: "t".into() }.class(),
            OpClass::Destructive
        );
        assert_eq!(
            Op::CreateIndex {
                name: "i".into(),
                table: "t".into(),
                columns: vec!["c".into()],
                unique: false,
            }
            .class(),
            OpClass::Additive
        );
    }

    #[test]
    fn checksum_is_stable() {
        let m = sample_manifest();
        assert_eq!(m.checksum().unwrap(), m.checksum().unwrap());
    }

    #[test]
    fn checksum_changes_with_body() {
        let mut a = sample_manifest();
        let c1 = a.checksum().unwrap();
        a.version = 2;
        assert_ne!(c1, a.checksum().unwrap());
    }

    #[test]
    fn seal_then_verify_round_trips() {
        let cm = ChecksummedManifest::seal(sample_manifest()).unwrap();
        assert!(cm.verify().is_ok());
    }

    #[test]
    fn migrate_error_bridges_to_conflict_exit_code() {
        // MigrateError bridges into SyncError::Migrate, which classifies as a
        // conflict (needs-human) -- exit code 4 per the sequencing doc.
        let err: crate::error::SyncError = MigrateError::Checksum {
            expected: "a".into(),
            actual: "b".into(),
        }
        .into();
        assert_eq!(err.exit_code(), 4);
    }

    #[test]
    fn tampered_body_fails_verification() {
        let mut cm = ChecksummedManifest::seal(sample_manifest()).unwrap();
        cm.manifest.version = 99;
        let err = cm.verify().unwrap_err();
        assert!(matches!(err, MigrateError::Checksum { .. }));
    }

    #[test]
    fn manifest_json_round_trips() {
        let cm = ChecksummedManifest::seal(sample_manifest()).unwrap();
        let json = serde_json::to_vec(&cm).unwrap();
        let back: ChecksummedManifest = serde_json::from_slice(&json).unwrap();
        assert_eq!(cm, back);
        assert!(back.verify().is_ok());
    }

    #[test]
    fn target_schema_is_opaque_string() {
        // 0.5.0 stores and round-trips target_schema without comparing it.
        let m = sample_manifest();
        let back: Manifest = serde_json::from_slice(&serde_json::to_vec(&m).unwrap()).unwrap();
        assert_eq!(back.target_schema, "opaque-schema-id");
    }

    #[test]
    #[cfg(feature = "native")]
    fn envelope_seal_open_round_trips() {
        let key = [7u8; 32];
        let cm = ChecksummedManifest::seal(sample_manifest()).unwrap();
        let sealed = Envelope::seal(&cm, &key).unwrap();
        let opened = Envelope::open(&sealed, &key).unwrap();
        assert_eq!(cm, opened);
    }

    #[test]
    #[cfg(feature = "native")]
    fn envelope_open_rejects_wrong_key() {
        let cm = ChecksummedManifest::seal(sample_manifest()).unwrap();
        let sealed = Envelope::seal(&cm, &[1u8; 32]).unwrap();
        let err = Envelope::open(&sealed, &[2u8; 32]).unwrap_err();
        assert!(matches!(err, MigrateError::Envelope(_)));
    }

    #[test]
    #[cfg(feature = "native")]
    fn envelope_open_rejects_too_short() {
        // A garbage/too-short blob is a hard error, never a silent skip.
        let err = Envelope::open(&[0u8; 4], &[9u8; 32]).unwrap_err();
        assert!(matches!(err, MigrateError::Envelope(_)));
    }
}
