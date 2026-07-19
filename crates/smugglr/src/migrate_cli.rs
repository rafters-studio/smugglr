//! `smugglr migrate` command surface.
//!
//! Owns the [`MigrateCommand`] enum -- the single home every migrate subcommand
//! is added to (per `docs/plans/migrate-sequencing.md`'s collision lane: route
//! ALL migrate commands here, never in `main.rs`). 0.5.0 ships the `new`
//! generator variant; later issues (#296 apply, #274 reverse, ...) add their
//! variants HERE.

use crate::output::OutputFormat;
use clap::Subcommand;
use smugglr_core::error::{self, SyncError};
use smugglr_core::migrate::{generator, ChecksummedManifest};

/// Subcommands of `smugglr migrate`.
#[derive(Subcommand)]
pub enum MigrateCommand {
    /// Scaffold a new migration manifest from a Rails-style column spec.
    ///
    /// Example:
    ///   smugglr migrate new create_contacts id:pk address_id:fk email:text:pii hours:int:range
    New {
        /// Migration name (e.g. `create_contacts`); the table is derived from it.
        name: String,

        /// Column specs: `name[:type][:modifier...]` (type defaults to text).
        #[arg(value_name = "COLUMN")]
        columns: Vec<String>,
    },
}

/// Dispatch a `migrate` subcommand.
pub fn run(command: &MigrateCommand, fmt: OutputFormat) -> error::Result<()> {
    match command {
        MigrateCommand::New { name, columns } => run_new(name, columns, fmt),
    }
}

/// `smugglr migrate new`: parse the column spec, emit the checksummed manifest.
///
/// The manifest itself is the artifact, so it is printed as pretty JSON in both
/// output modes. A grammar failure maps to a configuration-class error (exit
/// code 2 -- fix the input, do not retry).
fn run_new(name: &str, columns: &[String], _fmt: OutputFormat) -> error::Result<()> {
    let manifest =
        generator::generate(name, columns).map_err(|e| SyncError::Config(e.to_string()))?;
    let sealed = ChecksummedManifest::seal(manifest)?;
    let json = serde_json::to_string_pretty(&sealed)?;
    println!("{json}");
    Ok(())
}
