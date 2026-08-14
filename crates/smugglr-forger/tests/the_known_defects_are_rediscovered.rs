//! The known-defect corpus, rediscovered -- and the part of it that cannot be.
//!
//! Ten defects were found by hand in smugglr's migrate spine, every one of them
//! in code with passing tests. A harness that cannot rediscover the ones it was
//! built for is not working, and this file is where that is settled. Each test
//! below is keyed to an issue number, and its transformation is written from
//! that issue's own stated mechanism rather than from a plausible-looking
//! rebuild. FR-FORGER-012.
//!
//! # How a defect is reproduced without depending on smugglr
//!
//! forger depends on no `smugglr-*` crate, so the pre-fix code cannot be
//! imported and run. What can be reproduced is the *behaviour*: each of these
//! defects is a rebuild that lost something, so each test here is a
//! transformation that rebuilds the table and drops that same thing, handed to
//! [`differential`] with a target schema that still promises it. The oracle then
//! answers the question the issue asked: does anything notice?
//!
//! # Why this file is not `the_oracle_catches_a_silent_drop.rs`
//!
//! That file proves the oracle *mechanism* -- that a differential comparison
//! reports a construct that was dropped, that a failed transformation leaves by
//! a different door, that the exclusion set is the caller's. Its four
//! transformations are illustrations chosen to exercise those properties.
//!
//! This file is the register: one test per issue, named for it, asserting the
//! outcome that issue's defect produces. The two overlap in subject and not in
//! purpose -- a change that broke the mechanism turns that file red, and a
//! change that stopped a known defect being seen turns this one red, and being
//! able to tell those apart from the failure list is the whole reason they are
//! separate.
//!
//! # What forger cannot rediscover, and why
//!
//! This list is the point of the exercise as much as the tests are, and it is
//! kept here rather than in a report nobody re-reads. FR-FORGER-012.
//!
//! Most of it is not a fact about this register but about forger's coverage
//! envelope: the adapters, the `ON UPDATE` and `RESTRICT` halves of
//! smugglr#341, `MATCH`, anything a log would have said, smugglr#340, and
//! smugglr#347's missing rebuild. Those are stated by [`Boundary`], derived
//! from the covered set and printed by every census run -- FR-FORGER-010 -- and
//! they are deliberately not restated here. Two accounts of one truth can
//! disagree, and this file would be the copy that goes stale the first time
//! somebody adds a probe. The two blind spots nothing can measure are
//! demonstrated below instead, in
//! [`smugglr_341_a_dropped_on_update_action_is_not_rediscovered`] and
//! [`smugglr_341_the_same_loss_on_a_restrict_key_alone_is_not_rediscovered`],
//! each of which asserts both that the loss goes unreported *and* that the
//! boundary still says so.
//!
//! What is left is particular to a defect rather than to what forger covers:
//!
//! * **smugglr#343's expression-`DEFAULT` defect in the shape it was found.**
//!   The reconstruction is a syntax error, so the transformation fails and
//!   [`differential`] returns `Err(ForgeError::Transform)` with no report -- no
//!   trait is named, deliberately, because "the migration refused to run" and
//!   "the migration ran and changed meaning" are different findings. Asserted at
//!   the error door in
//!   [`smugglr_343_an_expression_default_rendered_without_its_parentheses_leaves_by_the_error_door`].
//!   The trait-naming rediscovery next to it is of a sibling shape the issue
//!   does not document -- a rebuild that quotes what `table_info` handed it
//!   instead of emitting it bare -- and is labelled as such.
//!
//! * **smugglr#343's composite `PRIMARY KEY (a DESC, b)`.** The consequence is
//!   index ordering, and the `DescendingPrimaryKey` probe asserts the property
//!   that is not about ordering at all -- that the descending spelling is not a
//!   rowid alias -- which exists only for the single-column column-level form.
//!   The single-column form is rediscovered below; the composite one is not.
//!
//! * **smugglr#344's corruption, as opposed to its promotion.** The promotion to
//!   declared `BLOB` is rediscovered below. The corruption it opens the door to
//!   is base64 canonicalization in smugglr's `rowhash`, which is a decision made
//!   about a column's declared type in another crate -- there is no state of the
//!   database that differs.
//!
//! * **smugglr#340's outcome, as opposed to its input domain.** [`Boundary`]
//!   states that forger cannot render the identifier that defect needs. Even
//!   given the input, the issue records the outcome as a hard SQLite syntax
//!   error -- the error door again, naming no trait.
//!
//! * **smugglr#347's shape, beyond the missing rebuild [`Boundary`] states.**
//!   The only trigger in the registry references exactly the column the trigger
//!   probe's own `INSERT` names, so "a trigger abandoned by a dropped column"
//!   and "the probe's statement named a column that is gone" are the same
//!   observation. Separating them needs a registry case with a trigger over a
//!   column no probe writes, which is a change to `registry/cases.rs` and is not
//!   made here -- a committed corpus fixture is run against a case's schema, so
//!   editing one can stop a pinned defect reproducing.

use rusqlite::Connection;

use smugglr_forger::boundary::{Boundary, Subject};
use smugglr_forger::error::BoxError;
use smugglr_forger::fixture::{Backing, Fixture, Route};
use smugglr_forger::oracle::{differential, Divergence, Outcome, Report};
use smugglr_forger::registry::TraitCase;
use smugglr_forger::schema::{ReferentialAction, Schema, Trait};

// ---------------------------------------------------------------------------
// smugglr#341 -- the rebuild read five of the eight columns foreign_key_list
// returns, and the referential actions are in the three it skipped
// ---------------------------------------------------------------------------

/// `reconstruct_foreign_keys` reassembled every key it found as a bare
/// `FOREIGN KEY (...) REFERENCES ...`, so this drops the action on both
/// children rather than on one: the pragma columns it did not read were not
/// read for any key.
///
/// A `DELETE` that used to cascade now hard-fails, and nothing about the row
/// counts changes on the day of the migration -- the children are still there,
/// which is what a rebuild that kept them looks like.
#[test]
fn smugglr_341_a_rebuild_that_dropped_every_referential_action_is_rediscovered() {
    let report = run(r#"
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

        CREATE TABLE "restrict_child_new" (
            "id" INTEGER PRIMARY KEY,
            "keeper_id" INTEGER,
            "label" TEXT,
            FOREIGN KEY ("keeper_id") REFERENCES "keeper" ("id")
        );
        INSERT INTO "restrict_child_new" ("id", "keeper_id", "label")
            SELECT "id", "keeper_id", "label" FROM "restrict_child";
        DROP TABLE "restrict_child";
        ALTER TABLE "restrict_child_new" RENAME TO "restrict_child";
        "#);

    assert_baseline_is_sound(&report);
    assert_broke(&report, Trait::ForeignKeyWithAction);
    assert_only_these_diverged(&report, &[Trait::ForeignKeyWithAction]);
}

/// The half of smugglr#341 that is invisible here, demonstrated rather than
/// claimed.
///
/// `ON DELETE RESTRICT` and the `NO ACTION` default both refuse the delete while
/// enforcement is immediate; they part company only under `DEFERRABLE INITIALLY
/// DEFERRED`, where `RESTRICT` still fires at the statement and `NO ACTION`
/// waits for the commit. The schema model has no deferrable spelling, so on
/// every connection forger can build the two are the same behaviour -- and a
/// probe asserting an end state is right to say nothing.
///
/// So the rediscovery above rests entirely on its `CASCADE` half. The same
/// defect on a database whose only referential action is a `RESTRICT` passes.
///
/// The second half of the lock is the assertion that [`Boundary`] still says so.
/// Nothing can measure "these two are the same behaviour here" -- it is a fact
/// about a spelling the schema model does not have -- so the claim is held
/// between two assertions that fail in opposite directions: this test goes red
/// if the blind spot closes, and red again if the statement disappears without
/// it closing.
#[test]
fn smugglr_341_the_same_loss_on_a_restrict_key_alone_is_not_rediscovered() {
    assert!(
        Boundary::of_this_build()
            .statement(Subject::Restrict)
            .is_some(),
        "this test demonstrates a blind spot the boundary no longer states. Either the boundary \
         stopped deriving the line -- no case schema declares ON DELETE RESTRICT any more -- or \
         the statement was edited away while the blind spot below is still open"
    );

    // The premise, checked rather than assumed: this transformation really does
    // leave the key without its action. A test that records a blind spot has to
    // fail when the blind spot closes *and* when it has quietly stopped removing
    // anything -- and the second of those is invisible to the run below, whose
    // whole finding is silence. There is no behavioural check available here, so
    // this one reads the DDL SQLite stored, which is exactly the comparison the
    // oracle refuses to make and is sound for asking what a statement did rather
    // than whether two arms agree.
    let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
    fixture
        .bring_to(Route::Schema(&every_trait()))
        .expect("the schema stands up");
    assert!(
        stored_ddl(&fixture, "restrict_child").contains("RESTRICT"),
        "the case schema declares the action this test is about removing"
    );
    fixture
        .conn()
        .execute_batch(DROPS_THE_RESTRICT_ACTION)
        .expect("the rebuild applies");
    assert!(
        !stored_ddl(&fixture, "restrict_child").contains("RESTRICT"),
        "this test's premise is that the rebuild below leaves the key with the NO ACTION default"
    );

    let report = run(DROPS_THE_RESTRICT_ACTION);

    // Sound baseline, so the silence below is blindness rather than two arms
    // agreeing about a database that was already broken.
    assert_baseline_is_sound(&report);
    assert_eq!(
        report.divergences(),
        Vec::new(),
        "if this ever reports, the blind spot recorded in this file's docs has closed and the \
         docs are now wrong"
    );
}

/// The rebuild from the test above, used twice: once to show what it removed,
/// once to show that removing it goes unreported.
const DROPS_THE_RESTRICT_ACTION: &str = r#"
    CREATE TABLE "restrict_child_new" (
        "id" INTEGER PRIMARY KEY,
        "keeper_id" INTEGER,
        "label" TEXT,
        FOREIGN KEY ("keeper_id") REFERENCES "keeper" ("id")
    );
    INSERT INTO "restrict_child_new" ("id", "keeper_id", "label")
        SELECT "id", "keeper_id", "label" FROM "restrict_child";
    DROP TABLE "restrict_child";
    ALTER TABLE "restrict_child_new" RENAME TO "restrict_child";
"#;

/// The `CREATE` text SQLite stored for a table.
fn stored_ddl(fixture: &Fixture, table: &str) -> String {
    fixture
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )
        .expect("the table is there")
}

/// The other half that is invisible here, also demonstrated.
///
/// The registry's case declares `ON DELETE` actions only and its probe reads
/// `fk.on_delete`, so nothing in forger asks what an `UPDATE` to a parent key
/// does. The first half of this test proves the loss is real -- the same rebuild
/// against a fixture stops the update cascading -- and the second half runs the
/// oracle over a schema carrying that key and gets silence.
///
/// The flipped form of a test that used to pin this as a blind spot.
///
/// It asserted that dropping `ON UPDATE CASCADE` went unreported, because the
/// `ForeignKeyWithAction` case declared `ON DELETE` actions only and its probe
/// read `fk.on_delete`. smugglr#374 gave the case an `ON UPDATE CASCADE` key on
/// its own parent and the probe an arm that moves that key, so the same
/// transformation is now caught. The boundary assertion is kept and inverted:
/// the claim has to be gone, and gone because a probe earned it.
#[test]
fn smugglr_341_an_on_update_action_is_rediscovered() {
    assert!(
        !Boundary::of_this_build()
            .undeclared_on_update()
            .contains(&ReferentialAction::Cascade),
        "the boundary still claims ON UPDATE CASCADE goes unexercised, but the run below reports \
         the loss. The claim and the coverage have come apart -- one of them is lying"
    );

    // What the loss does, shown against a fixture rather than argued for:
    // whether the parent key could move at all, and whether the child followed.
    // Kept from the blind-spot version, because the premise it establishes is
    // what makes the oracle's report below meaningful rather than circular.
    let moving_the_parent_key = |ddl: Option<&str>| -> (bool, i64) {
        let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
        fixture
            .bring_to(Route::Schema(&every_trait()))
            .expect("the schema stands up");
        fixture
            .conn()
            .execute_batch(ON_UPDATE_SEED)
            .expect("the seed lands");
        if let Some(ddl) = ddl {
            fixture
                .conn()
                .execute_batch(ddl)
                .expect("the rebuild applies");
        }
        let moved = fixture
            .conn()
            .execute_batch(r#"UPDATE "updating_keeper" SET "id" = 2 WHERE "id" = 1"#)
            .is_ok();
        let followed = fixture
            .conn()
            .query_row(
                r#"SELECT count(*) FROM "updating_child" WHERE "keeper_id" = 2"#,
                [],
                |row| row.get(0),
            )
            .expect("the child is countable");
        (moved, followed)
    };
    assert_eq!(
        moving_the_parent_key(None),
        (true, 1),
        "ON UPDATE CASCADE lets the key move and takes the child with it"
    );
    assert_eq!(
        moving_the_parent_key(Some(DROPS_THE_ON_UPDATE_ACTION)),
        (false, 0),
        "this test's premise is that the rebuild below loses the cascade -- the UPDATE that used \
         to move the key and its children is refused outright, which is smugglr#341's inversion \
         on the ON UPDATE side"
    );

    let schema = every_trait();
    let report = differential(
        Backing::Memory,
        &schema,
        &schema,
        &mut |conn: &mut Connection| -> Result<(), BoxError> {
            conn.execute_batch(DROPS_THE_ON_UPDATE_ACTION)?;
            Ok(())
        },
        Vec::<&str>::new(),
    )
    .expect("the transformation runs");

    assert_baseline_is_sound(&report);
    assert_broke(&report, Trait::ForeignKeyWithAction);
    assert_only_these_diverged(&report, &[Trait::ForeignKeyWithAction]);
}

/// The rows the ON UPDATE demonstration above moves.
///
/// Its own seed rather than the registry's: the demonstration builds a bare
/// schema and asserts on ids 1 and 2, while the case's seed uses its own key
/// values for the same tables. Sharing one would couple a hand-written
/// assertion to a constant that exists for a different reason.
const ON_UPDATE_SEED: &str = r#"
    INSERT INTO "updating_keeper" ("id", "label") VALUES (1, 'renumbered');
    INSERT INTO "updating_child" ("id", "keeper_id", "label") VALUES (10, 1, 'follows');
"#;

/// The rebuild from the test above, kept next to the schema it is about.
const DROPS_THE_ON_UPDATE_ACTION: &str = r#"
    CREATE TABLE "updating_child_new" (
        "id" INTEGER PRIMARY KEY,
        "keeper_id" INTEGER,
        "label" TEXT,
        FOREIGN KEY ("keeper_id") REFERENCES "updating_keeper" ("id")
    );
    INSERT INTO "updating_child_new" ("id", "keeper_id", "label")
        SELECT "id", "keeper_id", "label" FROM "updating_child";
    DROP TABLE "updating_child";
    ALTER TABLE "updating_child_new" RENAME TO "updating_child";
"#;

// ---------------------------------------------------------------------------
// smugglr#342 -- PRAGMA table_info omits generated columns, so they were never
// in the set of columns to rebuild
// ---------------------------------------------------------------------------

/// The virtual column never entered `raw_table_info`, never reached the `kept`
/// set, and so is neither declared in the new table nor named in the copy. What
/// is left is a table that looks right to the pragma anyone would check it
/// with, because the column was never in that pragma to begin with.
#[test]
fn smugglr_342_a_virtual_generated_column_enumerated_out_of_existence_is_rediscovered() {
    let report = run(r#"
        CREATE TABLE "virtual_generated_new" (
            "id" INTEGER PRIMARY KEY, "base" INTEGER, "label" TEXT
        );
        INSERT INTO "virtual_generated_new" ("id", "base", "label")
            SELECT "id", "base", "label" FROM "virtual_generated";
        DROP TABLE "virtual_generated";
        ALTER TABLE "virtual_generated_new" RENAME TO "virtual_generated";
        "#);

    assert_baseline_is_sound(&report);
    assert_erred(&report, Trait::GeneratedVirtual);
    assert_only_these_diverged(&report, &[Trait::GeneratedVirtual]);
}

/// The stored column is the same loss with a worse ending. A rebuild that had
/// the column name from somewhere else -- the original DDL text, a hand-written
/// list -- re-creates it as an ordinary column and copies the number into it,
/// which is byte-identical in a row dump and has quietly stopped computing.
///
/// Separate from the virtual case rather than folded into it: they fail in two
/// different ways, and a regression that closed one eye would otherwise leave
/// one red test that still looks like the same test.
#[test]
fn smugglr_342_a_stored_generated_column_rebuilt_as_an_ordinary_one_is_rediscovered() {
    let report = run(r#"
        CREATE TABLE "stored_generated_new" (
            "id" INTEGER PRIMARY KEY, "base" INTEGER, "tripled" INTEGER, "label" TEXT
        );
        INSERT INTO "stored_generated_new" ("id", "base", "tripled", "label")
            SELECT "id", "base", "tripled", "label" FROM "stored_generated";
        DROP TABLE "stored_generated";
        ALTER TABLE "stored_generated_new" RENAME TO "stored_generated";
        "#);

    assert_baseline_is_sound(&report);
    assert_broke(&report, Trait::GeneratedStored);
    assert_only_these_diverged(&report, &[Trait::GeneratedStored]);
}

// ---------------------------------------------------------------------------
// smugglr#343 -- render_def emitted four fields, and a conflict clause was not
// one of them; and an expression default came back without its parentheses
// ---------------------------------------------------------------------------

/// The conflict algorithm comes off the constraint and the constraint stays, on
/// every column that carried one -- `render_def` had no field for it, so it had
/// no field for it anywhere. An `INSERT` the original absorbed now throws.
#[test]
fn smugglr_343_a_dropped_column_on_conflict_clause_is_rediscovered() {
    let report = run(r#"
        CREATE TABLE "replace_absorbs_new" (
            "id" INTEGER PRIMARY KEY, "k" TEXT UNIQUE, "label" TEXT
        );
        INSERT INTO "replace_absorbs_new" ("id", "k", "label")
            SELECT "id", "k", "label" FROM "replace_absorbs";
        DROP TABLE "replace_absorbs";
        ALTER TABLE "replace_absorbs_new" RENAME TO "replace_absorbs";

        CREATE TABLE "ignore_absorbs_new" (
            "id" INTEGER PRIMARY KEY, "v" TEXT NOT NULL, "label" TEXT
        );
        INSERT INTO "ignore_absorbs_new" ("id", "v", "label")
            SELECT "id", "v", "label" FROM "ignore_absorbs";
        DROP TABLE "ignore_absorbs";
        ALTER TABLE "ignore_absorbs_new" RENAME TO "ignore_absorbs";

        CREATE TABLE "rollback_throws_new" (
            "id" INTEGER PRIMARY KEY, "v" TEXT NOT NULL, "label" TEXT
        );
        INSERT INTO "rollback_throws_new" ("id", "v", "label")
            SELECT "id", "v", "label" FROM "rollback_throws";
        DROP TABLE "rollback_throws";
        ALTER TABLE "rollback_throws_new" RENAME TO "rollback_throws";
        "#);

    assert_baseline_is_sound(&report);
    assert_broke(&report, Trait::ColumnOnConflict);
    assert_only_these_diverged(&report, &[Trait::ColumnOnConflict]);
}

/// smugglr#343's second defect in the shape the issue records: `table_info`
/// returns an expression default with its parentheses stripped and the
/// reconstruction re-emits it bare, which SQLite refuses outright.
///
/// This is the fail-safe half of that issue and it is *not* a rediscovery that
/// names a trait. The transformation fails, so the oracle returns
/// `ForgeError::Transform` and produces no report at all -- which is the
/// distinction it keeps on purpose, and which means the acceptance bar's
/// "a failure naming the affected trait" is unreachable for this defect by
/// construction. Asserted here so that the claim is checked rather than
/// asserted in prose.
#[test]
fn smugglr_343_an_expression_default_rendered_without_its_parentheses_leaves_by_the_error_door() {
    let schema = every_trait();
    let outcome = differential(
        Backing::Memory,
        &schema,
        &schema,
        &mut |conn: &mut Connection| -> Result<(), BoxError> {
            conn.execute_batch(
                r#"
                CREATE TABLE "expression_default_new" (
                    "id" INTEGER PRIMARY KEY,
                    "made_at" TEXT DEFAULT datetime('now'),
                    "computed" INTEGER DEFAULT 2 + 3,
                    "label" TEXT
                );
                "#,
            )?;
            Ok(())
        },
        Vec::<&str>::new(),
    );

    match outcome {
        Err(smugglr_forger::ForgeError::Transform(error)) => {
            let message = error.to_string();
            assert!(
                message.contains("syntax error"),
                "the reconstruction is refused by SQLite's parser, and the operator sees that \
                 refusal rather than a divergence: {message}"
            );
        }
        other => panic!(
            "an unparenthesized expression default is a syntax error, so the transformation \
             fails and no report exists; got {other:?}"
        ),
    }
}

/// The sibling shape, which the oracle *can* name a trait for.
///
/// smugglr#343 records the bare re-emission above. A rebuild that instead quoted
/// what `table_info` handed it -- the obvious defensive move against that syntax
/// error, and the one that turns a loud failure into a silent one -- produces
/// valid DDL whose default is the expression's own source text. The row it
/// writes holds `datetime('now')` as a string, forever.
///
/// This shape is not in the issue. It is here because it is the same defect one
/// bug-fix away, and because the trait-naming half of the acceptance bar cannot
/// be met by the shape that was found.
#[test]
fn smugglr_343_an_expression_default_quoted_into_a_literal_is_rediscovered() {
    let report = run(r#"
        CREATE TABLE "expression_default_new" (
            "id" INTEGER PRIMARY KEY,
            "made_at" TEXT DEFAULT 'datetime(''now'')',
            "computed" INTEGER DEFAULT '2 + 3',
            "label" TEXT
        );
        INSERT INTO "expression_default_new" ("id", "made_at", "computed", "label")
            SELECT "id", "made_at", "computed", "label" FROM "expression_default";
        DROP TABLE "expression_default";
        ALTER TABLE "expression_default_new" RENAME TO "expression_default";
        "#);

    assert_baseline_is_sound(&report);
    assert_broke(&report, Trait::ExpressionDefault);
    assert_only_these_diverged(&report, &[Trait::ExpressionDefault]);
}

/// smugglr#343's third defect: `table_info`'s `pk` column gives position and
/// never direction, so `PRIMARY KEY (a DESC)` reconstructs ascending.
///
/// In the single-column column-level form that is not a change of index order,
/// it is a change of identity: `INTEGER PRIMARY KEY DESC` is an ordinary key
/// with a unique index over a table that keeps its own rowid, and the ascending
/// spelling *is* the rowid. The copy is what makes it visible -- the keys land
/// as rowids.
#[test]
fn smugglr_343_a_descending_primary_key_rebuilt_ascending_is_rediscovered() {
    let report = run(r#"
        CREATE TABLE "descending_key_new" ("id" INTEGER PRIMARY KEY, "label" TEXT);
        INSERT INTO "descending_key_new" ("id", "label")
            SELECT "id", "label" FROM "descending_key";
        DROP TABLE "descending_key";
        ALTER TABLE "descending_key_new" RENAME TO "descending_key";
        "#);

    assert_baseline_is_sound(&report);
    assert_broke(&report, Trait::DescendingPrimaryKey);
    assert_only_these_diverged(&report, &[Trait::DescendingPrimaryKey]);
}

// ---------------------------------------------------------------------------
// smugglr#344 -- a typeless column came back declared BLOB
// ---------------------------------------------------------------------------

/// The promotion is certain and silent, and every behavioural assertion in the
/// registry passes over it: `BLOB` affinity converts nothing, so the column goes
/// on storing a string as text and an integer as an integer. Only the declared
/// type gives it away, and the `TypelessColumn` probe reads one for exactly this
/// defect -- the single PRAGMA assertion in the registry, taken deliberately.
///
/// What the promotion costs is in another crate: `rowhash::is_blob_column`
/// deliberately excludes an empty declared type, because base64-decoding a
/// genuine text value would corrupt it, and this migration moves the column into
/// the class that gets decoded.
#[test]
fn smugglr_344_a_typeless_column_promoted_to_blob_is_rediscovered() {
    let report = run(r#"
        CREATE TABLE "typeless_new" ("id" INTEGER PRIMARY KEY, "v" BLOB, "label" TEXT);
        INSERT INTO "typeless_new" ("id", "v", "label")
            SELECT "id", "v", "label" FROM "typeless";
        DROP TABLE "typeless";
        ALTER TABLE "typeless_new" RENAME TO "typeless";
        "#);

    assert_baseline_is_sound(&report);
    assert_broke(&report, Trait::TypelessColumn);
    assert_only_these_diverged(&report, &[Trait::TypelessColumn]);
}

// ---------------------------------------------------------------------------
// smugglr#336 -- the rebuild dropped every trigger while its comment said it
// recreated them
// ---------------------------------------------------------------------------

/// `explicit_indexes` collected `type = 'index'` and nothing else, `DROP TABLE`
/// took the triggers with the table, and the replay loop had nothing to replay.
/// Audit rows stop being written, nothing errors, and the migration reports
/// success.
///
/// The committed corpus fixture for this issue pins the *other* order the same
/// rebuild can get wrong -- re-creating the trigger before the copy, so it fires
/// again over rows already audited. This is the loss itself.
#[test]
fn smugglr_336_a_rebuild_that_dropped_the_trigger_is_rediscovered() {
    let report = run(r#"
        CREATE TABLE "evented_new" ("id" INTEGER PRIMARY KEY, "note" TEXT);
        INSERT INTO "evented_new" ("id", "note") SELECT "id", "note" FROM "evented";
        DROP TABLE "evented";
        ALTER TABLE "evented_new" RENAME TO "evented";
        "#);

    assert_baseline_is_sound(&report);
    assert_broke(&report, Trait::Trigger);
    assert_only_these_diverged(&report, &[Trait::Trigger]);
}

// ---------------------------------------------------------------------------
// The quiet direction
// ---------------------------------------------------------------------------

/// Every table the defects above damaged, rebuilt with the construct kept, in
/// one transformation -- and the oracle says nothing about any of it.
///
/// One test rather than seven, because this is the direction where a composite
/// costs nothing: the assertion names the trait that broke and the arm it broke
/// in, so a red here localises itself without a bisect. It is the distinct-test
/// rule's other half that matters -- a partial regression in *rediscovery* must
/// not collapse into one red, and each of those is its own test above.
///
/// The trigger is re-created after the copy, which is the order smugglr#336's
/// corpus fixture exists to pin the other end of.
#[test]
fn a_faithful_rebuild_of_every_affected_table_is_silent() {
    let report = run(r#"
        CREATE TABLE "cascade_child_new" (
            "id" INTEGER PRIMARY KEY,
            "keeper_id" INTEGER,
            "label" TEXT,
            FOREIGN KEY ("keeper_id") REFERENCES "keeper" ("id") ON DELETE CASCADE
        );
        INSERT INTO "cascade_child_new" ("id", "keeper_id", "label")
            SELECT "id", "keeper_id", "label" FROM "cascade_child";
        DROP TABLE "cascade_child";
        ALTER TABLE "cascade_child_new" RENAME TO "cascade_child";

        CREATE TABLE "restrict_child_new" (
            "id" INTEGER PRIMARY KEY,
            "keeper_id" INTEGER,
            "label" TEXT,
            FOREIGN KEY ("keeper_id") REFERENCES "keeper" ("id") ON DELETE RESTRICT
        );
        INSERT INTO "restrict_child_new" ("id", "keeper_id", "label")
            SELECT "id", "keeper_id", "label" FROM "restrict_child";
        DROP TABLE "restrict_child";
        ALTER TABLE "restrict_child_new" RENAME TO "restrict_child";

        CREATE TABLE "virtual_generated_new" (
            "id" INTEGER PRIMARY KEY,
            "base" INTEGER,
            "doubled" INTEGER GENERATED ALWAYS AS ("base" * 2) VIRTUAL,
            "label" TEXT
        );
        INSERT INTO "virtual_generated_new" ("id", "base", "label")
            SELECT "id", "base", "label" FROM "virtual_generated";
        DROP TABLE "virtual_generated";
        ALTER TABLE "virtual_generated_new" RENAME TO "virtual_generated";

        CREATE TABLE "stored_generated_new" (
            "id" INTEGER PRIMARY KEY,
            "base" INTEGER,
            "tripled" INTEGER GENERATED ALWAYS AS ("base" * 3) STORED,
            "label" TEXT
        );
        INSERT INTO "stored_generated_new" ("id", "base", "label")
            SELECT "id", "base", "label" FROM "stored_generated";
        DROP TABLE "stored_generated";
        ALTER TABLE "stored_generated_new" RENAME TO "stored_generated";

        CREATE TABLE "replace_absorbs_new" (
            "id" INTEGER PRIMARY KEY,
            "k" TEXT UNIQUE ON CONFLICT REPLACE,
            "label" TEXT
        );
        INSERT INTO "replace_absorbs_new" ("id", "k", "label")
            SELECT "id", "k", "label" FROM "replace_absorbs";
        DROP TABLE "replace_absorbs";
        ALTER TABLE "replace_absorbs_new" RENAME TO "replace_absorbs";

        CREATE TABLE "expression_default_new" (
            "id" INTEGER PRIMARY KEY,
            "made_at" TEXT DEFAULT (datetime('now')),
            "computed" INTEGER DEFAULT (2 + 3),
            "label" TEXT
        );
        INSERT INTO "expression_default_new" ("id", "made_at", "computed", "label")
            SELECT "id", "made_at", "computed", "label" FROM "expression_default";
        DROP TABLE "expression_default";
        ALTER TABLE "expression_default_new" RENAME TO "expression_default";

        CREATE TABLE "typeless_new" ("id" INTEGER PRIMARY KEY, "v", "label" TEXT);
        INSERT INTO "typeless_new" ("id", "v", "label")
            SELECT "id", "v", "label" FROM "typeless";
        DROP TABLE "typeless";
        ALTER TABLE "typeless_new" RENAME TO "typeless";

        CREATE TABLE "descending_key_new" ("id" INTEGER PRIMARY KEY DESC, "label" TEXT);
        INSERT INTO "descending_key_new" ("id", "label")
            SELECT "id", "label" FROM "descending_key";
        DROP TABLE "descending_key";
        ALTER TABLE "descending_key_new" RENAME TO "descending_key";

        CREATE TABLE "evented_new" ("id" INTEGER PRIMARY KEY, "note" TEXT);
        INSERT INTO "evented_new" ("id", "note") SELECT "id", "note" FROM "evented";
        DROP TABLE "evented";
        ALTER TABLE "evented_new" RENAME TO "evented";
        CREATE TRIGGER "evented_audit" AFTER INSERT ON "evented"
        FOR EACH ROW BEGIN
          INSERT INTO "audit" ("note") VALUES (new."note");
        END;
        "#);

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

// ---------------------------------------------------------------------------
// The schemas
// ---------------------------------------------------------------------------

/// The eight case schemas, unioned. The cases name their tables distinctly, so
/// this is a concatenation, and each probe still finds its own construct by
/// reading the schema.
fn every_trait() -> Schema {
    let mut all = Schema::default();
    for kind in Trait::ALL {
        all.tables.extend(TraitCase::for_trait(kind).schema.tables);
    }
    all.validate()
        .expect("the union of the case schemas is one SQLite would accept");
    all
}

// ---------------------------------------------------------------------------
// Driving the oracle
// ---------------------------------------------------------------------------

/// Run one transformation over the every-trait schema, claiming to arrive back
/// at it.
///
/// Start and target are the same schema because every defect here is a rebuild
/// that *said* it preserved everything. No exclusion set: nothing in these
/// transformations writes bookkeeping of its own, so any table the inventory
/// reports is a table the transformation lost or invented.
fn run(statements: &str) -> Report {
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
        Vec::<&str>::new(),
    )
    .expect("the transformation runs")
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

/// The arm nobody transformed held on every trait. Without this, "no
/// divergence" is also what two identically broken arms produce -- which is
/// exactly the reading the two blind-spot tests above would otherwise admit.
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

/// The construct is still there and no longer behaves.
fn assert_broke(report: &Report, kind: Trait) {
    let outcome = report.for_trait(kind);
    assert!(
        matches!(outcome.transformed, Outcome::Broke(_)),
        "{kind:?} should have failed its probe in the transformed arm; it said {:?}",
        outcome.transformed
    );
}

/// The construct is gone, so SQLite refused the probe's statement outright.
fn assert_erred(report: &Report, kind: Trait) {
    let outcome = report.for_trait(kind);
    assert!(
        matches!(outcome.transformed, Outcome::Erred(_)),
        "{kind:?} should have met a statement SQLite refused in the transformed arm; it said {:?}",
        outcome.transformed
    );
}

/// Exactly these traits diverged, and no table did. The second half is what
/// keeps a rediscovery honest: a rebuild that also dropped a table on its way
/// past would pass a test that only asked whether the named trait reported.
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
