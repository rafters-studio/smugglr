//! Drop one construct on purpose, and watch the oracle say which one.
//!
//! This is the harness proving itself, and it is the same discipline the probe
//! suite applies with its seventeen breaks. A differential oracle that reports
//! nothing is indistinguishable from one that cannot see, so each of the four
//! constructs the requirement names is deliberately lost by a transformation
//! here and the oracle is required to notice -- and to notice *that one*.
//!
//! # Why the transformations copy rows
//!
//! Every break below is a rebuild that carries the rows across. If it did not,
//! the transformed arm would come back empty, every probe would report nothing
//! to observe, and the oracle would flag a divergence -- for data loss, not for
//! the construct that was dropped. The test would be green and the mechanism
//! unproven. So each transformation copies, the assertions name the outcome
//! kind rather than merely asking whether *something* diverged, and every other
//! trait is required to have held in both arms.
//!
//! # Why the from-scratch arm is asserted sound
//!
//! Two arms that broke identically do not diverge. "No divergence" is therefore
//! also what a bad schema or a mislocated probe produces, so before any
//! comparison is trusted the arm nobody transformed is required to have held on
//! all eight traits. That check lives here rather than in [`Report`] -- what a
//! failure report should say is FR-FORGER-008's problem.

use rusqlite::Connection;

use smugglr_forger::error::BoxError;
use smugglr_forger::fixture::{Backing, Fixture, Route};
use smugglr_forger::oracle::{differential, Arm, Divergence, Outcome, Report};
use smugglr_forger::registry::TraitCase;
use smugglr_forger::schema::{Schema, Trait};

/// smugglr's migration ledger, which exists in the transformed arm and not in
/// the other. Written here as the literal a *caller* would pass, because that
/// is the correction the requirement carries: forger takes the exclusion set as
/// a parameter and never imports it. smugglr's own tests pass
/// `config::default_exclude_tables()`, which contains this name; forger does not
/// know the function exists.
const LEDGER: &str = "_smugglr_migrations";

/// What a migration engine writes around the work: its own bookkeeping table,
/// with a row in it. Nothing about the user schema changes.
const RECORDS_A_MIGRATION: &str = r#"
    CREATE TABLE "_smugglr_migrations" ("version" TEXT PRIMARY KEY, "applied_at" TEXT);
    INSERT INTO "_smugglr_migrations" ("version", "applied_at") VALUES ('0001', '2026-01-01');
"#;

// ---------------------------------------------------------------------------
// The four deliberate drops
// ---------------------------------------------------------------------------

/// smugglr#341's shape: the rebuild reconstructs the foreign key and leaves the
/// referential action behind, which turns a cascade into the `NO ACTION`
/// default. Nothing about the row counts changes on the day of the migration.
#[test]
fn dropping_a_referential_action_diverges() {
    let report = run(
        r#"
        CREATE TABLE "cascade_child_new" (
            "id" INTEGER PRIMARY KEY,
            "keeper_id" INTEGER,
            "label" TEXT,
            FOREIGN KEY ("keeper_id") REFERENCES "keeper" ("id")
        );
        INSERT INTO "cascade_child_new" ("id", "keeper_id", "label")
            SELECT "id", "keeper_id", "label" FROM "cascade_child";
        DROP TABLE "cascade_child";
        ALTER TABLE "cascade_child_new" RENAME TO "cascade_child";
        "#,
        &[LEDGER],
    );

    assert_baseline_is_sound(&report);
    assert_broke(&report, Trait::ForeignKeyWithAction);
    assert_only_these_diverged(&report, &[Trait::ForeignKeyWithAction]);
}

/// The rebuild enumerates the columns to copy from `PRAGMA table_info`, which
/// never returns a generated column at all -- so both of them are lost, and
/// they are lost in the two different ways this crate exists to tell apart.
///
/// The virtual column simply is not in the new table, and the probe's read of
/// it comes back as a value it cannot use. (Not, note, as "no such column":
/// SQLite falls back to reading an unresolved double-quoted identifier as a
/// string literal, so `SELECT "doubled"` yields the text `doubled` for every
/// row and it is the *type* that gives the absence away. That fallback is
/// recorded as a hazard for the registry, not worked around here.)
///
/// The stored one is worse: a rebuild that had the column name from somewhere
/// would re-create it as an ordinary column and copy the number into it, which
/// is byte-identical in a row dump and has stopped computing. Only moving the
/// base and re-reading catches that, and this asserts both shapes in one run.
#[test]
fn dropping_a_generated_column_diverges() {
    let report = run(
        r#"
        CREATE TABLE "virtual_generated_new" ("id" INTEGER PRIMARY KEY, "base" INTEGER, "label" TEXT);
        INSERT INTO "virtual_generated_new" ("id", "base", "label")
            SELECT "id", "base", "label" FROM "virtual_generated";
        DROP TABLE "virtual_generated";
        ALTER TABLE "virtual_generated_new" RENAME TO "virtual_generated";

        CREATE TABLE "stored_generated_new" ("id" INTEGER PRIMARY KEY, "base" INTEGER, "tripled" INTEGER, "label" TEXT);
        INSERT INTO "stored_generated_new" ("id", "base", "tripled", "label")
            SELECT "id", "base", "tripled", "label" FROM "stored_generated";
        DROP TABLE "stored_generated";
        ALTER TABLE "stored_generated_new" RENAME TO "stored_generated";
        "#,
        &[LEDGER],
    );

    assert_baseline_is_sound(&report);
    // Not "it diverged": which arm said what, and in which of the two ways.
    assert_erred(&report, Trait::GeneratedVirtual);
    assert_broke(&report, Trait::GeneratedStored);
    assert_only_these_diverged(&report, &[Trait::GeneratedVirtual, Trait::GeneratedStored]);
}

/// The conflict algorithm comes off the constraint and the constraint stays.
/// The table still refuses duplicates -- it just throws where it used to
/// absorb, which is a data outcome rather than a schema difference.
#[test]
fn dropping_an_on_conflict_clause_diverges() {
    let report = run(
        r#"
        CREATE TABLE "replace_absorbs_new" ("id" INTEGER PRIMARY KEY, "k" TEXT UNIQUE, "label" TEXT);
        INSERT INTO "replace_absorbs_new" ("id", "k", "label")
            SELECT "id", "k", "label" FROM "replace_absorbs";
        DROP TABLE "replace_absorbs";
        ALTER TABLE "replace_absorbs_new" RENAME TO "replace_absorbs";
        "#,
        &[LEDGER],
    );

    assert_baseline_is_sound(&report);
    assert_broke(&report, Trait::ColumnOnConflict);
    assert_only_these_diverged(&report, &[Trait::ColumnOnConflict]);
}

/// smugglr#344's shape, in its behaviourally visible half: the rebuild invents
/// `TEXT` for a column that was declared with no type at all. The copy itself
/// does the damage -- `TEXT` affinity converts the integer on the way in -- so
/// this is caught by the probe's `typeof()` assertion and does not lean on the
/// declared-type read the typeless probe also takes.
#[test]
fn resolving_a_typeless_declaration_diverges() {
    let report = run(
        r#"
        CREATE TABLE "typeless_new" ("id" INTEGER PRIMARY KEY, "v" TEXT, "label" TEXT);
        INSERT INTO "typeless_new" ("id", "v", "label")
            SELECT "id", "v", "label" FROM "typeless";
        DROP TABLE "typeless";
        ALTER TABLE "typeless_new" RENAME TO "typeless";
        "#,
        &[LEDGER],
    );

    assert_baseline_is_sound(&report);
    assert_broke(&report, Trait::TypelessColumn);
    assert_only_these_diverged(&report, &[Trait::TypelessColumn]);
}

// ---------------------------------------------------------------------------
// The quiet direction
// ---------------------------------------------------------------------------

/// A transformation that changes nothing about the user schema, over a schema
/// carrying all eight traits, is silent.
#[test]
fn a_transformation_that_preserves_everything_diverges_nowhere() {
    let report = run(RECORDS_A_MIGRATION, &[LEDGER]);

    assert_baseline_is_sound(&report);
    for outcome in &report.traits {
        assert_eq!(
            outcome.transformed,
            Outcome::Held,
            "{:?} did not hold in the transformed arm",
            outcome.kind
        );
    }
    assert_eq!(report.divergences(), Vec::new());
}

/// The reason the comparison is behavioural rather than textual, demonstrated
/// rather than asserted in a comment.
///
/// A faithful rebuild -- same columns, same blank type, rows carried across --
/// leaves a database that behaves identically and whose stored `CREATE` text
/// does not match the from-scratch arm's, because `ALTER TABLE ... RENAME TO`
/// rewrites the text it renames through. A verifier comparing
/// `sqlite_master.sql` would report drift here, on a transformation that lost
/// nothing. The oracle reports nothing.
#[test]
fn a_faithful_rebuild_is_silent_even_though_its_stored_ddl_text_differs() {
    const FAITHFUL: &str = r#"
        CREATE TABLE "typeless_new" ("id" INTEGER PRIMARY KEY, "v", "label" TEXT);
        INSERT INTO "typeless_new" ("id", "v", "label") SELECT "id", "v", "label" FROM "typeless";
        DROP TABLE "typeless";
        ALTER TABLE "typeless_new" RENAME TO "typeless";
    "#;

    assert_ne!(
        stored_ddl_after(Some(FAITHFUL), "typeless"),
        stored_ddl_after(None, "typeless"),
        "this test's premise is that the two arms' stored text differs; if it \
         stopped differing the test below has stopped saying anything"
    );

    let report = run(FAITHFUL, &[LEDGER]);
    assert_baseline_is_sound(&report);
    assert_eq!(report.divergences(), Vec::new());
}

// ---------------------------------------------------------------------------
// The exclusion set is the caller's
// ---------------------------------------------------------------------------

/// One transformation, one forger, two exclusion sets, two verdicts.
///
/// The ledger exists in the transformed arm by construction and never in the
/// other, so without the caller's set every comparison would report it -- the
/// trap the requirement names. With it, the same run is silent. Nothing in
/// forger changed between the two halves of this test, which is the property
/// being asserted: the set is a parameter, so the engine that owns the list
/// owns the behaviour.
#[test]
fn the_exclusion_set_is_a_caller_parameter_and_changing_it_changes_the_verdict() {
    let excluded_by_nobody = run(RECORDS_A_MIGRATION, &[]);
    assert_baseline_is_sound(&excluded_by_nobody);
    assert!(excluded_by_nobody.diverged());
    assert_eq!(
        excluded_by_nobody.divergences(),
        vec![Divergence::Table {
            name: LEDGER.to_string(),
            present_in: Arm::Transformed,
        }]
    );

    let excluded_by_the_caller = run(RECORDS_A_MIGRATION, &[LEDGER]);
    assert_baseline_is_sound(&excluded_by_the_caller);
    assert!(!excluded_by_the_caller.diverged());
    assert_eq!(excluded_by_the_caller.divergences(), Vec::new());
}

/// The set is not special-cased to the ledger. Any name the caller hands in is
/// left out of the comparison, which is what "one list, owned by the engine"
/// means in practice.
///
/// And it is left out of the *inventory* only. The exclusion set answers "whose
/// table is this", not "does any of this matter": a caller who hides a table
/// still hears about everything that table's absence changed, because the
/// probes are not filtered by it and never should be. Here the audit table the
/// trigger writes into is dropped, so hiding the name silences the inventory
/// and the trigger still reports.
#[test]
fn the_exclusion_set_hides_a_table_from_the_inventory_and_not_from_the_probes() {
    const DROPS_A_TABLE: &str = r#"DROP TABLE "audit";"#;

    let visible = run(DROPS_A_TABLE, &[LEDGER]);
    assert_baseline_is_sound(&visible);
    assert!(visible.divergences().contains(&Divergence::Table {
        name: "audit".to_string(),
        present_in: Arm::FromScratch,
    }));

    let hidden = run(DROPS_A_TABLE, &[LEDGER, "audit"]);
    assert_baseline_is_sound(&hidden);
    assert!(
        !hidden
            .divergences()
            .iter()
            .any(|divergence| matches!(divergence, Divergence::Table { .. })),
        "the caller named this table, so the inventory should not mention it: {:?}",
        hidden.divergences()
    );
    // The trigger's side effect had nowhere to land, and that is behaviour
    // rather than inventory, so excluding the name did not excuse it.
    assert_erred(&hidden, Trait::Trigger);
    assert_only_these_diverged(&hidden, &[Trait::Trigger]);
}

// ---------------------------------------------------------------------------
// A failed transformation is not a divergence
// ---------------------------------------------------------------------------

/// The distinction `ForgeError::Transform` exists to keep, asserted at the
/// oracle's own boundary: a transformation that fails leaves by the `Err` door
/// and produces no report at all, so no caller can read "the migration refused
/// to run" as "the migration ran and changed meaning".
#[test]
fn a_transformation_that_fails_is_reported_as_a_failure_and_not_as_a_divergence() {
    let schema = every_trait();
    let mut transform =
        |_: &mut Connection| -> Result<(), BoxError> { Err("the migration refused to run".into()) };

    let outcome = differential(Backing::Memory, &schema, &schema, &mut transform, [LEDGER]);

    match outcome {
        Err(smugglr_forger::ForgeError::Transform(error)) => {
            assert_eq!(error.to_string(), "the migration refused to run");
        }
        other => panic!(
            "a failing transformation must surface as ForgeError::Transform, not as a report; \
             got {other:?}"
        ),
    }
}

// ---------------------------------------------------------------------------
// The schema every case is run against
// ---------------------------------------------------------------------------

/// The eight case schemas, unioned into one that exercises every supported
/// trait.
///
/// The cases name their tables distinctly, so this is a concatenation rather
/// than a merge -- and each probe still finds its own construct by reading the
/// schema, which is the property that lets one schema carry all eight.
fn every_trait() -> Schema {
    let mut all = Schema::default();
    for kind in Trait::ALL {
        all.tables.extend(TraitCase::for_trait(kind).schema.tables);
    }
    all.validate()
        .expect("the union of the case schemas is one SQLite would accept");
    all
}

#[test]
fn the_union_of_every_case_schema_is_one_sqlite_accepts() {
    let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
    fixture
        .bring_to(Route::Schema(&every_trait()))
        .expect("the union renders to DDL SQLite accepts");
}

// ---------------------------------------------------------------------------
// Driving the oracle
// ---------------------------------------------------------------------------

/// Run one transformation over the every-trait schema, claiming to arrive back
/// at it.
///
/// Start and target are the same schema on purpose: these are transformations
/// that *say* they preserve everything, and the question the oracle answers is
/// whether they did.
fn run(statements: &str, ignore_tables: &[&str]) -> Report {
    let schema = every_trait();
    let mut transform = |conn: &mut Connection| -> Result<(), BoxError> {
        conn.execute_batch(statements)?;
        Ok(())
    };
    differential(
        Backing::Memory,
        &schema,
        &schema,
        &mut transform,
        ignore_tables.iter().copied(),
    )
    .expect("the transformation runs")
}

/// The `CREATE` text SQLite stored for a table, after optionally running a
/// transformation over the every-trait schema.
///
/// This is the comparison the oracle refuses to make, built here only to show
/// that refusing it was right.
fn stored_ddl_after(statements: Option<&str>, table: &str) -> String {
    let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
    fixture
        .bring_to(Route::Schema(&every_trait()))
        .expect("the schema stands up");
    if let Some(statements) = statements {
        fixture
            .conn()
            .execute_batch(statements)
            .expect("the transformation runs");
    }
    fixture
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )
        .expect("the table is there")
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

/// The arm nobody transformed held on every trait.
///
/// Without this, "no divergence" is also what two identically broken arms
/// produce, and every comparison in this file would be an agreement between two
/// wrong answers.
fn assert_baseline_is_sound(report: &Report) {
    for outcome in &report.traits {
        assert_eq!(
            outcome.from_scratch,
            Outcome::Held,
            "{:?} did not hold in the arm built from scratch, so nothing compared against it \
             means anything",
            outcome.kind
        );
    }
}

/// The transformed arm ran the probe and the probe failed -- the construct is
/// there and no longer behaves. Not "something diverged": a probe that reported
/// nothing to observe would also diverge from a held baseline, and that is what
/// a transformation losing the *rows* looks like rather than the construct.
fn assert_broke(report: &Report, kind: Trait) {
    let outcome = report.for_trait(kind);
    assert!(
        matches!(outcome.transformed, Outcome::Broke(_)),
        "{kind:?} should have failed its probe in the transformed arm; it said {:?}",
        outcome.transformed
    );
}

/// The transformed arm no longer has something the target schema promises, so
/// SQLite refused the probe's statement outright.
fn assert_erred(report: &Report, kind: Trait) {
    let outcome = report.for_trait(kind);
    assert!(
        matches!(outcome.transformed, Outcome::Erred(_)),
        "{kind:?} should have met a statement SQLite refused in the transformed arm; it said {:?}",
        outcome.transformed
    );
}

/// Exactly these traits diverged, and no table did.
///
/// The second half matters as much as the first: a rebuild that dropped a table
/// on its way past would show up in the inventory, and a test asserting only
/// "the trait diverged" would pass while the transformation had also destroyed
/// something nobody was looking at.
fn assert_only_these_diverged(report: &Report, expected: &[Trait]) {
    let mut diverged: Vec<Trait> = report
        .traits
        .iter()
        .filter(|outcome| outcome.diverged())
        .map(|outcome| outcome.kind)
        .collect();
    diverged.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    assert_eq!(diverged, expected, "traits that diverged");

    let tables: Vec<Divergence> = report
        .divergences()
        .into_iter()
        .filter(|divergence| matches!(divergence, Divergence::Table { .. }))
        .collect();
    assert!(tables.is_empty(), "tables also diverged: {tables:?}");
}
