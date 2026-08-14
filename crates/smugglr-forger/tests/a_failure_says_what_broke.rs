//! Break something for real, and read what the report says about it.
//!
//! FR-FORGER-008's criteria are about output, and output is the one thing a
//! unit test of a renderer will happily agree with itself about. So these run
//! the actual oracle over an actual dropped construct and assert on the text a
//! reviewer would be handed: the trait is named, the before and the after are
//! there in those terms, the schema comes out as source rather than as SQL, and
//! nothing in it is a word you would have to open this crate to look up.
//!
//! # Why "does not contain" assertions carry weight here
//!
//! Two of the criteria are negative. "Not a diff of two generated blobs" and
//! "no failure output requires reading forger's source to interpret" are both
//! satisfied by absence, and absence is what drifts back in silently when
//! someone adds a debugging line. So the internal vocabulary -- variant names,
//! type names, the rendered DDL -- is asserted out of the report rather than
//! left to taste.

use rusqlite::Connection;

use smugglr_forger::census;
use smugglr_forger::error::BoxError;
use smugglr_forger::failure::{promise, render_divergence, render_report};
use smugglr_forger::fixture::Backing;
use smugglr_forger::oracle::{differential, Arm, Divergence, Outcome, Report, TraitOutcome};
use smugglr_forger::schema::Trait;

/// smugglr#341's shape: the rebuild reconstructs the foreign key and leaves the
/// referential action behind. The same transformation
/// `the_oracle_catches_a_silent_drop.rs` uses to prove the oracle sees it; here
/// it is used to see what the oracle then says.
const DROPS_THE_CASCADE: &str = r#"
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
"#;

fn report_after(statements: &str) -> Report {
    let schema = census::every_trait_schema();
    let mut transform = |conn: &mut Connection| -> Result<(), BoxError> {
        conn.execute_batch(statements)?;
        Ok(())
    };
    differential(
        Backing::Memory,
        &schema,
        &schema,
        &mut transform,
        std::iter::empty::<&str>(),
    )
    .expect("the transformation runs")
}

/// Every criterion FR-FORGER-008 states, against one real failure.
#[test]
fn a_dropped_referential_action_reports_the_trait_the_before_and_the_after() {
    let report = report_after(DROPS_THE_CASCADE);
    let rendered = render_report(&report);
    let one_line = flowed(&rendered);

    // Names the trait.
    assert!(
        one_line.contains("ForeignKeyWithAction"),
        "the report does not name the trait:\n{rendered}"
    );
    // And says what that name means, so the name is not the whole explanation.
    assert!(
        one_line.contains(&flowed(promise(Trait::ForeignKeyWithAction))),
        "the report names a trait and never says what it promises:\n{rendered}"
    );

    // Before and after, in those words.
    assert!(one_line.contains("before"), "{rendered}");
    assert!(one_line.contains("after"), "{rendered}");
    assert!(
        one_line.contains("never transformed"),
        "the report does not say which side was the unchanged one:\n{rendered}"
    );
    // The observation itself, in the probe's own words rather than a code.
    // The rebuild left the key with the NO ACTION default, so SQLite refused
    // the delete outright -- and the report says that, not "diverged".
    assert!(
        one_line.contains("a child declared ON DELETE CASCADE does not refuse it"),
        "the report does not say what the database actually did:\n{rendered}"
    );

    // The schema, as source rather than as SQL.
    assert!(
        rendered.contains("schema()")
            && rendered.contains(".on_delete(ReferentialAction::Cascade)"),
        "the report does not carry the schema as builder source:\n{rendered}"
    );

    // Not a diff of two generated blobs: no DDL anywhere in it.
    assert!(
        !rendered.contains("CREATE TABLE"),
        "the report renders DDL, which is the comparison the oracle refuses to \
         make:\n{rendered}"
    );

    // Nothing a reader would have to open forger to interpret.
    for internal in [
        "Outcome::",
        "ProbeError::",
        "Divergence::",
        "TraitOutcome",
        "NothingToObserve",
        "Erred",
        "Broke",
        "from_scratch",
    ] {
        assert!(
            !rendered.contains(internal),
            "the report leaks {internal:?}, which only forger's source explains:\n{rendered}"
        );
    }
}

/// The soundness verdict leads, and it leads on a clean run too.
///
/// A clean report over an unsound baseline is the most dangerous thing this
/// crate can emit, so it is the first line either way rather than a note under
/// the divergences.
#[test]
fn the_soundness_verdict_comes_first_on_a_clean_run_and_on_a_failing_one() {
    let clean = render_report(&report_after(r#"SELECT 1;"#));
    assert!(
        clean.starts_with("the baseline is sound"),
        "a clean report does not open with the verdict:\n{clean}"
    );
    assert!(clean.contains("no divergence"), "{clean}");

    let failing = render_report(&report_after(DROPS_THE_CASCADE));
    assert!(
        failing.starts_with("the baseline is sound"),
        "a failing report does not open with the verdict:\n{failing}"
    );
}

/// And when the baseline is not sound, the report says so before it says
/// anything a reader might act on.
#[test]
fn an_unsound_baseline_is_the_first_thing_the_report_says() {
    let unsound = Report {
        traits: vec![TraitOutcome {
            kind: Trait::GeneratedStored,
            transformed: Outcome::Broke("the transformed arm broke".into()),
            from_scratch: Outcome::Broke("so did the arm nobody transformed".into()),
        }],
        transformed_tables: Default::default(),
        from_scratch_tables: Default::default(),
    };

    // The two arms broke the same way, so by the oracle's own rule nothing
    // diverged -- which is exactly the run this warning exists for.
    assert!(!unsound.diverged());

    let rendered = render_report(&unsound);
    assert!(
        rendered.starts_with("THE BASELINE IS NOT SOUND"),
        "an unsound baseline is not the first thing said:\n{rendered}"
    );
    assert!(flowed(&rendered).contains("GeneratedStored"), "{rendered}");
    assert!(
        flowed(&rendered).contains("so did the arm nobody transformed"),
        "{rendered}"
    );
}

/// A table in one arm and not the other is not about a trait, and the report
/// says what to do about each direction rather than only which side had it.
#[test]
fn a_table_divergence_says_which_direction_it_is_and_what_to_do() {
    let left_behind = render_divergence(&Divergence::Table {
        name: "_smugglr_migrations".to_string(),
        present_in: Arm::Transformed,
    });
    assert!(
        flowed(&left_behind).contains("_smugglr_migrations"),
        "{left_behind}"
    );
    assert!(
        flowed(&left_behind).contains("ignore_tables"),
        "a table only the transformed arm has is usually the caller's bookkeeping, and the \
         report should say so:\n{left_behind}"
    );

    let lost = render_divergence(&Divergence::Table {
        name: "audit".to_string(),
        present_in: Arm::FromScratch,
    });
    assert!(
        flowed(&lost).contains("lost a table the target schema declares"),
        "{lost}"
    );
    assert!(
        !flowed(&lost).contains("ignore_tables"),
        "a table the transformation destroyed is not something to hide:\n{lost}"
    );
}

/// Prose is wrapped to a column, so it is compared with the wrapping taken back
/// out rather than line by line.
fn flowed(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}
