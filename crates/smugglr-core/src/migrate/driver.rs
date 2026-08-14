//! The composing apply-driver (#296): the one sanctioned forward-apply path.
//!
//! Every other migrate module is deliberately a single-purpose primitive --
//! [`ledger`](crate::migrate::ledger) records, [`apply`](crate::migrate::apply)
//! mutates, [`lint`](crate::migrate::lint) judges, [`reverse`](crate::migrate::reverse)
//! captures and restores -- and none of them composes. `apply.rs` in particular
//! is **ledger-free by invariant** (#273): it imports no ledger and resolves no
//! target. This module is where they are composed, so `apply.rs` never has to
//! learn about the ledger and the composition never has to be duplicated by a
//! CLI command or an embedder.
//!
//! # The apply lifecycle (`docs/plans/migration.md`, "Apply lifecycle")
//!
//! ```text
//! verify checksum
//!   -> ensure ledger schema
//!   -> version = current_version + 1        (the DRIVER assigns it)
//!   -> [optional reconcile preflight -- #290]
//!   -> ledger.try_elect(version, checksum)
//!   -> [only if Won]
//!        lint_manifest + enforce_preimage
//!        -> apply_ops(up, pre_op = capture_before)
//!        -> ledger.mark_success   |   ledger.mark_failed on error
//! ```
//!
//! ## The ledger write is two-phase, and election runs BEFORE apply
//!
//! There is no terminal "record the migration" step. Election claims the version
//! *before* the first byte of the database is mutated, and `mark_success` /
//! `mark_failed` only settle a row that already exists. That ordering is the
//! whole point: a ledger written *after* a successful apply would leave a crash
//! window in which the database has been mutated and the ledger is silent about
//! it -- the exact unrecoverable state recovery (#289) and reconcile (#290)
//! exist to avoid. With election first, every crash lands somewhere the ledger
//! already describes:
//!
//! | Crash point | Database | Ledger row | Next run |
//! |---|---|---|---|
//! | after `try_elect`, before op 1 | untouched | `pending`, leased | reclaimed once the lease expires, re-driven from op 1 |
//! | mid-loop | partially mutated | `pending`, leased | reclaimed, re-driven; per-op idempotency skips the applied ops |
//! | error mid-loop | partially mutated | `failed`, lease cleared | immediately reclaimable, re-driven |
//! | after the last op, before `mark_success` | fully mutated | `pending`, leased | reclaimed, re-driven as a no-op, then settles `success` |
//!
//! In every row of that table [`Ledger::current_version`] is unchanged, because
//! it keys on `status = 'success'`. A partially-applied version therefore never
//! advertises itself as applied.
//!
//! ## Local only
//!
//! 0.5.0 drives a local SQLite target and nothing else. Remote apply is not
//! runnable: #273 ships D1 / Turso / rqlite as pure statement *generators*, and
//! the host->target DDL transport is deferred to #291, which will build the
//! programmatic embedder API **on** [`apply_migration`] rather than beside it.
//! There must never be a second forward-apply loop.

#![cfg(feature = "native")]

use crate::error::Result;
use crate::migrate::apply::apply_ops;
use crate::migrate::ledger::{Election, Ledger, DEFAULT_LEASE_SECS};
use crate::migrate::lint::{self, Classification};
use crate::migrate::reverse::{PreimageCapturer, PreimagePayload};
use crate::migrate::{ChecksummedManifest, ClassifiedOp, MigrateError};
use rusqlite::{Connection, OpenFlags};
use std::path::Path;
use tracing::warn;

/// Knobs for one forward apply.
///
/// Two fields are **declared seams, not implemented behaviour** in 0.5.0. They
/// are here rather than in the later issues so the driver's success path -- a
/// serialize lane shared with #289 and #290 -- already has the shape those
/// issues fill in, instead of being reopened for a signature change. Setting
/// either one logs a warning rather than silently pretending, because a caller
/// who asked for a snapshot and did not get one is worse off than one who was
/// told no.
#[derive(Debug, Clone)]
pub struct ApplyOptions {
    /// How long the elected pending row's lease lasts, in seconds. A crash
    /// inside this window is reclaimable only after it expires.
    pub lease_secs: i64,

    /// Run the schema-drift preflight before electing.
    ///
    /// **Seam for #290.** The projection compare belongs before `try_elect`:
    /// refusing on drift must not leave a claimed pending row behind.
    pub reconcile_preflight: bool,

    /// Snapshot the database before mutating it (the coarse recovery parachute).
    ///
    /// **Seam for #289.** The `VACUUM INTO` snapshot belongs after the election
    /// is won and before the lint/apply block, so it captures exactly the state
    /// a failed apply must be restorable to.
    pub paranoid: bool,
}

impl Default for ApplyOptions {
    fn default() -> Self {
        Self {
            lease_secs: DEFAULT_LEASE_SECS,
            reconcile_preflight: false,
            paranoid: false,
        }
    }
}

/// What one [`apply_migration`] call did.
///
/// `election` is the honest outcome, not an error: losing an election is a
/// normal masterless result. When it is anything other than [`Election::Won`]
/// nothing was linted, applied, or captured, so `classifications` is empty and
/// `preimage` is `None`.
#[derive(Debug)]
pub struct ApplyOutcome {
    /// The version the driver assigned and elected.
    pub version: u64,
    /// The **manifest's** checksum, copied from `sealed.checksum`. This is not
    /// re-read from the ledger row and is not always what the row holds: on a
    /// reclaim the stored checksum is left at the previous manifest's value (see
    /// [`apply_migration`]'s "Known gap"). Read the row via [`Ledger::entry`]
    /// when you need the value the ledger actually recorded.
    pub checksum: String,
    /// The election result. Only [`Election::Won`] means this call applied ops.
    pub election: Election,
    /// The effective per-op classification of every applied `up` op, in order
    /// (the lint's *surfacing* verdict, which honours over-declaration).
    pub classifications: Vec<Classification>,
    /// The delta-scoped pre-image captured while destructive ops ran, or `None`
    /// when nothing destructive applied.
    pub preimage: Option<PreimagePayload>,
}

/// Apply a migration to the local SQLite file at `db_path`.
///
/// Opens the database read-write **without** `CREATE`, matching
/// [`LocalDb::open`](crate::local::LocalDb::open): migrating a database that
/// does not exist is a mistake, not a request to conjure an empty one. Every
/// other concern is [`apply_migration`]'s; this only owns the connection so a
/// caller without a `rusqlite` dependency (the CLI) can still drive an apply.
pub fn apply_migration_to_file(
    db_path: &Path,
    sealed: &ChecksummedManifest,
    opts: &ApplyOptions,
) -> Result<ApplyOutcome> {
    let conn = Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    apply_migration(&conn, sealed, opts)
}

/// Compose a full forward apply of `sealed` against a local connection.
///
/// The composition, and why each step sits where it does:
///
/// 1. **Verify the checksum.** [`ChecksummedManifest::verify`] establishes
///    exactly one thing: the body about to be applied matches the checksum
///    *travelling with it*, so the manifest was not altered after sealing. It
///    establishes nothing about the checksum on the ledger row, and the two can
///    legitimately diverge -- see the note below.
/// 2. **Ensure the ledger schema**, so a first-ever apply on a fresh database
///    does not fail reading a table that has never been created.
/// 3. **Assign the version.** `current_version + 1`, or 1 on an empty ledger.
///    The version is the *driver's* to assign, not the manifest's: the
///    generator (#270) hardcodes `version: 1` on every manifest it scaffolds,
///    so honouring `manifest.version` would make every migration claim v1.
/// 4. **Elect.** [`Ledger::try_elect`] is transaction-free and resolves the race
///    on `UNIQUE(version)`, so this whole path is portable to a target with no
///    interactive transactions.
/// 5. **Only if won:** lint, then apply, then settle. Everything past the
///    election funnels through one settle point, so *any* failure -- a lint
///    refusal, a failed op, or a failed `mark_success` -- settles the claimed
///    row as `failed` rather than abandoning it pending for a whole lease. Only
///    process death escapes the funnel, and the lease expiry covers that.
///
/// The lint runs once over the manifest (both of its gates are manifest-level),
/// while pre-image capture rides the per-op `pre_op` write-ahead hook
/// [`apply_ops`] exposes, firing before each op's own transaction so it snapshots
/// committed, pre-mutation state.
///
/// # Known gap: a reclaimed row keeps the previous manifest's checksum
///
/// [`Ledger::try_elect`] writes `sealed.checksum` **only** on the fresh-`INSERT`
/// path. Its two reclaim paths (`reclaim_pending` at `ledger.rs:426`,
/// `reclaim_failed` at `ledger.rs:442`) take no checksum parameter and their
/// `UPDATE`s never touch the `checksum` column. So the ordinary fix-and-retry
/// loop -- apply, fail, edit the manifest, re-apply -- reclaims row `vN` and
/// settles it `success` while the row still holds the **previous** manifest's
/// checksum. [`Ledger::verify_chain`] cannot catch it, because the chain is
/// recomputed from the *stored* checksum: the row is internally consistent and
/// merely factually wrong, so the tamper-evidence certifies the divergence
/// rather than flagging it.
///
/// This driver cannot close that from the outside -- a checksum-aware reclaim
/// is the ledger's to own (#272) -- and papering over it here (re-`UPDATE`ing
/// the checksum after winning) would rewrite a chain-hash input out of band and
/// trip the very tamper check it is trying to keep honest. Recorded rather than
/// worked around.
pub fn apply_migration(
    conn: &Connection,
    sealed: &ChecksummedManifest,
    opts: &ApplyOptions,
) -> Result<ApplyOutcome> {
    sealed.verify()?;
    let manifest = &sealed.manifest;

    Ledger::ensure_schema(conn)?;
    let version = Ledger::current_version(conn)?.map_or(1, |v| v + 1);

    if opts.reconcile_preflight {
        // Seam for #290: the schema-drift compare lands here, before the
        // election, so a refusal leaves no claimed row behind.
        warn!(
            version,
            "reconcile preflight requested but not implemented until #290; applying without a \
             drift check"
        );
    }

    let election = Ledger::try_elect(conn, version, &sealed.checksum, opts.lease_secs)?;
    if election != Election::Won {
        return Ok(ApplyOutcome {
            version,
            checksum: sealed.checksum.clone(),
            election,
            classifications: Vec::new(),
            preimage: None,
        });
    }

    if opts.paranoid {
        // Seam for #289: the `VACUUM INTO` snapshot lands here -- after the win,
        // before the first mutation.
        warn!(
            version,
            "--paranoid requested but the pre-migration snapshot is not implemented until #289; \
             applying without a parachute"
        );
    }

    let attempt = (|| -> Result<(Vec<Classification>, PreimagePayload)> {
        let classifications =
            lint::lint_manifest(manifest).map_err(|e| MigrateError::Lint(e.to_string()))?;
        lint::enforce_preimage(manifest).map_err(|e| MigrateError::Lint(e.to_string()))?;

        let mut capturer = PreimageCapturer::new();
        {
            let mut pre_op = |op: &ClassifiedOp| -> std::result::Result<(), MigrateError> {
                capturer.capture_before(conn, op)
            };
            apply_ops(conn, &manifest.up, &mut pre_op)?;
        }
        Ok((classifications, capturer.into_payload()))
    })();

    // `mark_success` is folded INTO the funnel rather than run after it: a bare
    // `?` here would leave the row `pending` with a live lease over a fully
    // mutated database -- the abandoned-pending state this funnel exists to
    // prevent, and worse than the lint-refusal case, because here the ops
    // actually ran. Settling `failed` instead is honest about *this run* not
    // completing, not a claim that nothing applied: apply is idempotent per-op,
    // so the reclaimer re-drives the ops as no-ops and settles `success`. The
    // one state that must never survive is a live lease nobody is holding.
    let settled = attempt.and_then(|applied| {
        Ledger::mark_success(conn, version)?;
        Ok(applied)
    });

    match settled {
        Ok((classifications, payload)) => Ok(ApplyOutcome {
            version,
            checksum: sealed.checksum.clone(),
            election,
            classifications,
            preimage: (!payload.is_empty()).then_some(payload),
        }),
        Err(e) => {
            // Best-effort settle: leave the row `failed` (and so immediately
            // reclaimable) rather than pending for the rest of the lease. The
            // original error is what the caller sees -- if this settle also
            // fails, the lease expiry is the backstop.
            let _ = Ledger::mark_failed(conn, version);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::ledger::MigrationStatus;
    use crate::migrate::reverse::{CapturedValue, TablePreimage};
    use crate::migrate::{Column, ColumnKind, Constraint, Flags, Manifest, Op, OpClass, Preimage};
    use rusqlite::OptionalExtension;
    use std::cell::RefCell;

    fn col(name: &str, kind: ColumnKind) -> Column {
        Column {
            name: name.to_string(),
            kind,
            constraints: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// A manifest whose `version` is deliberately the generator's hardcoded 1 --
    /// the driver is what assigns the real applied version.
    fn manifest_with(up: Vec<ClassifiedOp>, preimage: Option<Preimage>) -> ChecksummedManifest {
        ChecksummedManifest::seal(Manifest {
            version: 1,
            target_schema: "opaque".into(),
            up,
            down: Vec::new(),
            preimage,
            flags: Flags::default(),
            author: None,
        })
        .expect("seal manifest")
    }

    /// `users` carries a real primary key: the delta-scoped pre-image capture
    /// keys its surgical restore on the PK and refuses a PK-less table.
    fn create_users() -> ClassifiedOp {
        let mut id = col("id", ColumnKind::Int);
        id.constraints.push(Constraint::Pk);
        ClassifiedOp::new(Op::CreateTable {
            table: "users".into(),
            columns: vec![id, col("email", ColumnKind::Text)],
            without_rowid: false,
        })
    }

    fn conn() -> Connection {
        Connection::open_in_memory().expect("open in-memory db")
    }

    /// Assert against the live schema rather than the driver's own report, so a
    /// test cannot pass on a driver that returns the right outcome without
    /// having mutated anything.
    fn table_exists(conn: &Connection, table: &str) -> bool {
        conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |_| Ok(()),
        )
        .optional()
        .expect("query sqlite_master")
        .is_some()
    }

    fn column_exists(conn: &Connection, table: &str, column: &str) -> bool {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .expect("prepare table_info");
        let mut rows = stmt.query([]).expect("query table_info");
        while let Some(row) = rows.next().expect("read table_info row") {
            let name: String = row.get(1).expect("column name");
            if name == column {
                return true;
            }
        }
        false
    }

    #[test]
    fn apply_writes_the_ledger_on_a_real_apply() {
        // The property that makes reconcile (#290) non-hollow: after a real
        // apply the ledger has a baseline to compare against, so drift is
        // reportable instead of only "no baseline".
        let conn = conn();
        let sealed = manifest_with(vec![create_users()], None);

        let outcome = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap();

        assert_eq!(outcome.election, Election::Won);
        assert_eq!(outcome.version, 1);
        assert_eq!(outcome.checksum, sealed.checksum);
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(1));

        let entry = Ledger::entry(&conn, 1).unwrap().expect("ledger row");
        assert_eq!(entry.status, MigrationStatus::Success);
        assert_eq!(entry.checksum, sealed.checksum);
        assert_eq!(entry.lease_expires_at, None);
        // And the ops actually ran.
        assert!(table_exists(&conn, "users"));
    }

    #[test]
    fn driver_assigns_the_version_not_the_manifest() {
        // Both manifests carry the generator's hardcoded `version: 1`; the
        // second must still land on v2.
        let conn = conn();
        let first = manifest_with(vec![create_users()], None);
        let second = manifest_with(
            vec![ClassifiedOp::new(Op::AddColumn {
                table: "users".into(),
                column: col("nickname", ColumnKind::Text),
            })],
            None,
        );
        assert_eq!(first.manifest.version, 1);
        assert_eq!(second.manifest.version, 1);

        apply_migration(&conn, &first, &ApplyOptions::default()).unwrap();
        let outcome = apply_migration(&conn, &second, &ApplyOptions::default()).unwrap();

        assert_eq!(outcome.version, 2);
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(2));
    }

    #[test]
    fn a_held_election_applies_nothing() {
        // Another node holds a live lease on the version this driver would
        // claim: the driver reports the outcome and touches neither the schema
        // nor the lint.
        let conn = conn();
        Ledger::ensure_schema(&conn).unwrap();
        assert_eq!(
            Ledger::try_elect(&conn, 1, "someone-elses-checksum", 300).unwrap(),
            Election::Won
        );

        let sealed = manifest_with(vec![create_users()], None);
        let outcome = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap();

        assert_eq!(outcome.election, Election::HeldByOther);
        assert!(outcome.classifications.is_empty());
        assert!(outcome.preimage.is_none());
        assert_eq!(Ledger::current_version(&conn).unwrap(), None);
        assert!(!table_exists(&conn, "users"));
    }

    /// Pins #273's `apply_ops` hook **contract**, not this driver's use of it:
    /// it drives `apply_ops` directly with its own closure, so it would still
    /// pass if the driver's capture wiring were deleted. The driver's own
    /// interleave is covered by
    /// [`the_driver_interleaves_capture_per_op_before_each_mutates`].
    #[test]
    fn the_apply_ops_primitive_fires_the_hook_once_per_op_before_it_mutates() {
        // The hook must see every op, in order, and must observe pre-mutation
        // state. `users` is created by op 1, so a hook that fires before op 1
        // cannot see it and a hook firing before op 2 must.
        let conn = conn();
        let sealed = manifest_with(
            vec![
                create_users(),
                ClassifiedOp::new(Op::AddColumn {
                    table: "users".into(),
                    column: col("nickname", ColumnKind::Text),
                }),
            ],
            None,
        );

        let seen = RefCell::new(Vec::new());
        let mut capturer = PreimageCapturer::new();
        {
            let mut pre_op = |op: &ClassifiedOp| -> std::result::Result<(), MigrateError> {
                seen.borrow_mut().push(table_exists(&conn, "users"));
                capturer.capture_before(&conn, op)
            };
            apply_ops(&conn, &sealed.manifest.up, &mut pre_op).unwrap();
        }

        // Two ops, two firings, and the second saw the first op's committed effect.
        assert_eq!(seen.into_inner(), vec![false, true]);
    }

    #[test]
    fn the_driver_interleaves_capture_per_op_before_each_mutates() {
        // The composition's own interleave, asserted through `apply_migration`
        // rather than through `apply_ops`: delete the wiring inside the driver
        // and this fails. The captured VALUES are the proof of ordering -- a
        // hook firing after its op would find the column already gone and
        // capture nothing (or error), so recovering the pre-drop cells can only
        // happen if the hook ran first. Two destructive ops give one capture
        // each, in apply order, which is the per-op part.
        let conn = conn();
        let mut id = col("id", ColumnKind::Int);
        id.constraints.push(Constraint::Pk);
        apply_migration(
            &conn,
            &manifest_with(
                vec![ClassifiedOp::new(Op::CreateTable {
                    table: "users".into(),
                    columns: vec![
                        id,
                        col("email", ColumnKind::Text),
                        col("phone", ColumnKind::Text),
                    ],
                    without_rowid: false,
                })],
                None,
            ),
            &ApplyOptions::default(),
        )
        .unwrap();
        conn.execute_batch(
            "INSERT INTO users (id, email, phone) VALUES (1, 'a@example.com', '555-0100');",
        )
        .unwrap();

        let sealed = manifest_with(
            vec![
                ClassifiedOp::new(Op::DropColumn {
                    table: "users".into(),
                    column: "email".into(),
                }),
                ClassifiedOp::new(Op::DropColumn {
                    table: "users".into(),
                    column: "phone".into(),
                }),
            ],
            Some(Preimage::Inline {
                rows: serde_json::json!({ "tables": [] }),
            }),
        );
        let outcome = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap();

        let payload = outcome.preimage.expect("the driver captured a pre-image");
        assert_eq!(payload.tables.len(), 2, "one capture per destructive op");

        // In apply order, each holding the value its own op was about to lose.
        let dropped_values: Vec<(String, CapturedValue)> = payload
            .tables
            .iter()
            .map(|t| match t {
                TablePreimage::Column { dropped, rows, .. } => {
                    (dropped.clone(), rows[0][1].clone())
                }
                other => panic!("expected a dropped-column capture, got {other:?}"),
            })
            .collect();
        assert_eq!(
            dropped_values,
            vec![
                (
                    "email".to_string(),
                    CapturedValue::Text("a@example.com".into())
                ),
                ("phone".to_string(), CapturedValue::Text("555-0100".into())),
            ]
        );
        assert!(!column_exists(&conn, "users", "email"));
        assert!(!column_exists(&conn, "users", "phone"));
    }

    #[test]
    fn a_failed_apply_marks_the_row_failed_and_does_not_advance_the_version() {
        // Op 1 succeeds, op 2 renames a table that does not exist. The database
        // is left partially mutated -- and the ledger says so, which is exactly
        // the state ledger-after-apply would have hidden.
        let conn = conn();
        let sealed = manifest_with(
            vec![
                create_users(),
                ClassifiedOp::new(Op::AddColumn {
                    table: "ghosts".into(),
                    column: col("boo", ColumnKind::Text),
                }),
            ],
            None,
        );

        let err = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap_err();
        assert_eq!(err.exit_code(), 4);

        let entry = Ledger::entry(&conn, 1).unwrap().expect("ledger row");
        assert_eq!(entry.status, MigrationStatus::Failed);
        assert_eq!(entry.lease_expires_at, None);
        assert_eq!(Ledger::current_version(&conn).unwrap(), None);
        // Partially applied: op 1 landed, op 2 did not.
        assert!(table_exists(&conn, "users"));
    }

    #[test]
    fn a_lint_refusal_settles_the_claimed_row_rather_than_abandoning_it() {
        // A destructive op with no pre-image is refused by `enforce_preimage`
        // *after* the election is won, so the row must not be left pending.
        let conn = conn();
        apply_migration(
            &conn,
            &manifest_with(vec![create_users()], None),
            &ApplyOptions::default(),
        )
        .unwrap();

        let destructive = manifest_with(
            vec![ClassifiedOp::new(Op::DropColumn {
                table: "users".into(),
                column: "email".into(),
            })],
            None,
        );
        let err = apply_migration(&conn, &destructive, &ApplyOptions::default()).unwrap_err();
        assert!(err.to_string().contains("lint refused"));

        let entry = Ledger::entry(&conn, 2).unwrap().expect("ledger row");
        assert_eq!(entry.status, MigrationStatus::Failed);
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(1));
        // The refused op never ran.
        assert!(column_exists(&conn, "users", "email"));
    }

    #[test]
    fn an_under_declared_op_is_refused_before_anything_applies() {
        // `users` must exist first, or "nothing applied" would hold trivially --
        // a drop of an absent table leaves the schema unchanged either way, so
        // the assertion would prove nothing about the lint.
        let conn = conn();
        apply_migration(
            &conn,
            &manifest_with(vec![create_users()], None),
            &ApplyOptions::default(),
        )
        .unwrap();

        let sealed = manifest_with(
            vec![ClassifiedOp::declared(
                Op::DropTable {
                    table: "users".into(),
                },
                OpClass::Additive,
            )],
            None,
        );

        let err = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap_err();
        assert!(err.to_string().contains("under-states"));

        // Refused *before anything applied*: the table is still there. Checking
        // only `current_version` would not say that -- it stays 1 under a
        // ledger-after-apply ordering too, which is the bug this whole design
        // exists to rule out.
        assert!(table_exists(&conn, "users"));
        let entry = Ledger::entry(&conn, 2).unwrap().expect("ledger row");
        assert_eq!(entry.status, MigrationStatus::Failed);
        assert_eq!(entry.lease_expires_at, None);
        assert_eq!(Ledger::current_version(&conn).unwrap(), Some(1));
    }

    #[test]
    fn a_destructive_apply_captures_its_pre_image() {
        // The manifest must already carry a pre-image to clear `enforce_preimage`
        // (0.5.0's gate is manifest-level); the capturer is what makes the
        // reverse honest by snapshotting the rows the drop is about to lose.
        let conn = conn();
        apply_migration(
            &conn,
            &manifest_with(vec![create_users()], None),
            &ApplyOptions::default(),
        )
        .unwrap();
        conn.execute_batch("INSERT INTO users (id, email) VALUES (1, 'a@example.com');")
            .unwrap();

        let sealed = manifest_with(
            vec![ClassifiedOp::new(Op::DropColumn {
                table: "users".into(),
                column: "email".into(),
            })],
            Some(Preimage::Inline {
                rows: serde_json::json!({ "tables": [] }),
            }),
        );
        let outcome = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap();

        assert_eq!(outcome.election, Election::Won);
        assert_eq!(outcome.version, 2);
        assert!(outcome.classifications[0].destructive);
        let payload = outcome
            .preimage
            .expect("destructive apply captures a pre-image");
        assert_eq!(payload.tables.len(), 1);
        assert!(!column_exists(&conn, "users", "email"));
    }

    #[test]
    fn an_additive_apply_captures_nothing() {
        let conn = conn();
        let outcome = apply_migration(
            &conn,
            &manifest_with(vec![create_users()], None),
            &ApplyOptions::default(),
        )
        .unwrap();

        assert!(outcome.preimage.is_none());
        assert_eq!(outcome.classifications.len(), 1);
        assert!(outcome.classifications[0].is_additive());
    }

    #[test]
    fn a_tampered_manifest_never_reaches_the_ledger() {
        let conn = conn();
        let mut sealed = manifest_with(vec![create_users()], None);
        sealed.manifest.target_schema = "swapped-after-sealing".into();

        let err = apply_migration(&conn, &sealed, &ApplyOptions::default()).unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"));
        // The ledger schema exists only if `ensure_schema` ran; verification is
        // ahead of it, so nothing was created and nothing was claimed.
        assert!(Ledger::current_version(&conn).is_err());
    }
}
