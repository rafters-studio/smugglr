//! `smugglr migrate` command surface.
//!
//! Owns the [`MigrateCommand`] enum -- the single home every migrate subcommand
//! is added to (per `docs/plans/migrate-sequencing.md`'s collision lane: route
//! ALL migrate commands here, never in `main.rs`). 0.5.0 ships the `new`
//! generator variant; later issues (#296 apply, #274 reverse, ...) add their
//! variants HERE.

use crate::output::{OutputFormat, Status};
use clap::Subcommand;
use serde::Serialize;
use smugglr_core::error::{self, SyncError};
use smugglr_core::migrate::driver::{self, ApplyOptions, ApplyOutcome};
use smugglr_core::migrate::ledger::Election;
use smugglr_core::migrate::{generator, ChecksummedManifest};
use std::path::{Path, PathBuf};

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

    /// Apply a migration manifest to a local SQLite database.
    ///
    /// The applied version is assigned by the ledger, not by the manifest: the
    /// migration lands as `current_version + 1`, claimed *before* the first op
    /// runs, and settled success/failed afterwards.
    ///
    /// Example:
    ///   smugglr migrate apply migrations/create_contacts.json --db ./app.db
    Apply {
        /// Path to the checksummed manifest JSON (as emitted by `migrate new`).
        #[arg(value_name = "MANIFEST")]
        manifest: PathBuf,

        /// The local SQLite database to apply against. Required and explicit --
        /// migrate commands deliberately run before any config load, so a
        /// migration applies where you point it, never where a stale
        /// `config.toml` happens to point.
        #[arg(long, value_name = "PATH")]
        db: PathBuf,

        /// Snapshot the database before applying (recovery parachute).
        ///
        /// Per-command, not global: the parachute is worth its cost on a
        /// destructive run and not on a routine additive one. The snapshot
        /// itself lands with recovery (#289); until then this warns rather than
        /// silently pretending a snapshot was taken.
        #[arg(long)]
        paranoid: bool,
    },
}

/// Dispatch a `migrate` subcommand.
pub fn run(command: &MigrateCommand, fmt: OutputFormat) -> error::Result<()> {
    match command {
        MigrateCommand::New { name, columns } => run_new(name, columns, fmt),
        MigrateCommand::Apply {
            manifest,
            db,
            paranoid,
        } => run_apply(manifest, db, *paranoid, fmt),
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

/// The JSON rendering of one apply.
///
/// Lives here rather than in `output.rs` on purpose: `migrate_cli.rs` is the
/// declared home for every migrate command (and its output), so the later
/// migrate commands -- reverse (#274), recover (#289), reconcile (#290) -- add
/// their shapes beside this one instead of contending for `output.rs`.
#[derive(Serialize)]
struct ApplyOutput<'a> {
    command: &'static str,
    status: Status,
    /// The version the driver assigned and claimed in the ledger.
    version: u64,
    /// The **applied manifest's** content identity. This is the checksum of the
    /// file that was just applied, not a read-back of the ledger row's stored
    /// value.
    ///
    /// Those used to diverge on a reclaimed row, because the ledger's reclaim
    /// paths left the stored checksum at the previous manifest's value. #328
    /// closed that: a reclaim now writes the reclaiming caller's checksum, so
    /// the two agree. This field is still not a read-back, and the distinction
    /// is kept rather than dropped -- it says what this run applied, which is
    /// what a caller reading its own command's output is asking.
    checksum: &'a str,
    /// `won` (this run applied), `already_applied`, or `held_by_other`.
    election: &'static str,
    /// How many forward ops ran. Zero unless the election was won.
    ops_applied: usize,
    /// Whether any applied op lost data (the lint's surfacing verdict).
    destructive: bool,
    /// Whether any applied op rewrites content hashes.
    hash_rewriting: bool,
    /// Whether a delta-scoped pre-image was captured for the destructive ops.
    preimage_captured: bool,
}

/// `smugglr migrate apply`: read a sealed manifest, drive the one sanctioned
/// forward-apply composition, report what the ledger now says.
///
/// A lost election is **not** an error: on a masterless fabric another node
/// holding the version, or the version already being applied, is a normal
/// outcome. It reports as `status: ok` with the election named, and exits 0 --
/// only a real refusal (a failed checksum, a lint refusal, a failed op) is an
/// error exit.
fn run_apply(
    manifest_path: &Path,
    db: &Path,
    paranoid: bool,
    fmt: OutputFormat,
) -> error::Result<()> {
    let raw = std::fs::read(manifest_path)?;
    let sealed: ChecksummedManifest = serde_json::from_slice(&raw)?;

    let opts = ApplyOptions {
        paranoid,
        ..ApplyOptions::default()
    };
    let outcome = driver::apply_migration_to_file(db, &sealed, &opts)?;

    match fmt {
        OutputFormat::Json => {
            let out = ApplyOutput {
                command: "migrate apply",
                status: Status::Ok,
                version: outcome.version,
                checksum: &outcome.checksum,
                election: election_str(outcome.election),
                ops_applied: outcome.classifications.len(),
                destructive: any_destructive(&outcome),
                hash_rewriting: any_hash_rewriting(&outcome),
                preimage_captured: outcome.preimage.is_some(),
            };
            println!("{}", serde_json::to_string(&out)?);
        }
        OutputFormat::Text => print_apply_text(&outcome),
    }
    Ok(())
}

/// The wire rendering of an election outcome. A closed match, so a future
/// [`Election`] variant is a compile error here rather than a silent gap.
fn election_str(election: Election) -> &'static str {
    match election {
        Election::Won => "won",
        Election::AlreadyApplied => "already_applied",
        Election::HeldByOther => "held_by_other",
    }
}

fn any_destructive(outcome: &ApplyOutcome) -> bool {
    outcome.classifications.iter().any(|c| c.destructive)
}

fn any_hash_rewriting(outcome: &ApplyOutcome) -> bool {
    outcome.classifications.iter().any(|c| c.hash_rewriting)
}

fn print_apply_text(outcome: &ApplyOutcome) {
    match outcome.election {
        Election::Won => {
            println!(
                "Applied migration v{} ({} op{}) -- checksum {}",
                outcome.version,
                outcome.classifications.len(),
                if outcome.classifications.len() == 1 {
                    ""
                } else {
                    "s"
                },
                outcome.checksum
            );
            if any_destructive(outcome) {
                println!(
                    "  destructive: pre-image {}",
                    if outcome.preimage.is_some() {
                        "captured"
                    } else {
                        "NOT captured"
                    }
                );
            }
            if any_hash_rewriting(outcome) {
                println!("  hash-rewriting: peers must reach this version before rows converge");
            }
        }
        Election::AlreadyApplied => {
            println!(
                "Migration v{} is already applied -- nothing to do",
                outcome.version
            );
        }
        Election::HeldByOther => {
            println!(
                "Migration v{} is held by another node -- back off and retry",
                outcome.version
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smugglr_core::migrate::{ClassifiedOp, Column, ColumnKind, Flags, Manifest, Op};

    /// Seal a one-op manifest and write it where `run_apply` expects to read it.
    fn write_manifest(dir: &std::path::Path) -> PathBuf {
        let manifest = Manifest {
            // The generator's hardcoded version -- the driver overrides it.
            version: 1,
            target_schema: "opaque".into(),
            up: vec![ClassifiedOp::new(Op::CreateTable {
                table: "contacts".into(),
                columns: vec![Column {
                    name: "id".into(),
                    kind: ColumnKind::Int,
                    constraints: vec![],
                    tags: vec![],
                }],
                without_rowid: false,
            })],
            down: vec![],
            preimage: None,
            flags: Flags::default(),
            author: None,
        };
        let sealed = ChecksummedManifest::seal(manifest).expect("seal");
        let path = dir.join("create_contacts.json");
        std::fs::write(&path, serde_json::to_vec(&sealed).expect("serialize")).expect("write");
        path
    }

    /// End-to-end through the CLI entry point: a real apply mutates the database
    /// AND leaves a ledger row, which is the baseline reconcile (#290) compares
    /// against. Without it, drift detection has nothing to report drift from.
    #[test]
    fn apply_mutates_the_database_and_writes_the_ledger() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("app.db");
        rusqlite::Connection::open(&db).expect("create db");
        let manifest = write_manifest(dir.path());

        run_apply(&manifest, &db, false, OutputFormat::Json).expect("apply succeeds");

        let conn = rusqlite::Connection::open(&db).expect("reopen db");
        let table: String = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'contacts'",
                [],
                |r| r.get(0),
            )
            .expect("contacts table exists");
        assert_eq!(table, "contacts");

        let (version, status): (i64, String) = conn
            .query_row("SELECT version, status FROM _smugglr_migrations", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .expect("ledger row exists");
        assert_eq!(version, 1);
        assert_eq!(status, "success");
    }

    /// Applying against a database that does not exist refuses rather than
    /// conjuring an empty one and reporting success.
    #[test]
    fn apply_refuses_a_missing_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = write_manifest(dir.path());
        let missing = dir.path().join("nope.db");

        let err = run_apply(&manifest, &missing, false, OutputFormat::Text)
            .expect_err("a missing database is an error");
        assert!(!missing.exists(), "no database was created");
        assert!(
            err.to_string().contains("Local database error"),
            "unexpected error: {err}"
        );
    }
}
