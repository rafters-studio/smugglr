//! `smugglr migrate` -- versioned, reversible, integrity-checked schema-and-data
//! migrations for the SQLite family.
//!
//! Design: `docs/plans/migration.md`. Build sequencing:
//! `docs/plans/migrate-sequencing.md`.
//!
//! The whole module tree is **pre-declared here up front** (per the sequencing
//! doc's `lib.rs`/`mod.rs` mitigation): `manifest` carries the real 0.5.0 body
//! (the structured op enum, the checksummed manifest, and the native-only
//! envelope), and the remaining modules are empty stubs reserved for later
//! issues. Filling a body only touches that module -- never `lib.rs` or this
//! file -- so no later migrate PR collides on the module block.

pub mod manifest;

// --- Reserved stubs (bodies land in the listed issues) ---------------------
pub mod apply; // #273 forward apply engine
pub mod convert; // #280 int -> UUIDv7 conversion
pub mod generator; // #270 rails-style generator
pub mod ledger; // #272 versioned, tamper-evident ledger
pub mod lint; // #275 destructive-lint
pub mod log; // #289 surgical operation log + --paranoid
pub mod reconcile; // #290 schema-drift reconcile
pub mod reverse; // #274 reverse / rollback
pub mod schema_projection; // #290 pragma-derived semantic schema projection

/// The composing apply-driver (`smugglr migrate apply`). Native-only: it drives
/// a live connection through lint/capture/apply/ledger. Body lands in #296.
#[cfg(feature = "native")]
pub mod driver;

pub use manifest::{
    ChecksummedManifest, Column, Flags, Manifest, MigrateError, Op, OpClass, Preimage,
};
