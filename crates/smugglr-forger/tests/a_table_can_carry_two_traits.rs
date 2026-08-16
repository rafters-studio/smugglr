//! Two constructs on one table, each asserted by its own probe.
//!
//! Every [`TraitCase`] puts its construct on its own table and the every-trait
//! schema concatenates those tables, so eight traits have always meant eight
//! single-construct tables. A defect that needs two constructs to *meet* was
//! therefore not merely untested but unreachable -- and meeting is what they do
//! in a rebuild, which emits columns, keys, generated declarations and replayed
//! triggers into one body.
//!
//! smugglr#398. What this file proves is narrow and specific: a combined table
//! can be built, each probe on it asserts only its own construct, and each
//! probe fails when *its* construct is broken rather than when the other one
//! is.
//!
//! The last part is the one worth stating. A single probe asserting both
//! constructs would pass whenever either held for the wrong reason and would
//! name the wrong one when it failed, which is why a combination carries a
//! probe per trait rather than a probe per table.

use smugglr_forger::error::ProbeError;
use smugglr_forger::fixture::{Backing, Fixture, Route};
use smugglr_forger::registry::Combination;
use smugglr_forger::schema::{Schema, TableConstraint, Trait};

/// Stand a schema up, seed it, and run one probe.
fn run(combination: &Combination, schema: &Schema, kind: Trait) -> Result<(), ProbeError> {
    let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
    fixture
        .bring_to(Route::Ddl(&schema.to_ddl()))
        .expect("the combined schema renders DDL SQLite accepts");
    combination.seed(fixture.conn()).expect("seed");
    let probe = combination
        .probes()
        .iter()
        .find(|(k, _)| *k == kind)
        .map(|(_, p)| *p)
        .unwrap_or_else(|| panic!("{kind:?} has no probe on this combination"));
    probe(&combination.schema, fixture.conn())
}

/// Every declared combination holds on its own schema.
///
/// The baseline. If a combination does not hold unbroken, nothing measured
/// against it afterwards means anything.
#[test]
fn every_combination_holds_on_the_table_it_declares() {
    let combinations = Combination::all();
    assert!(
        !combinations.is_empty(),
        "no combination is declared, so this file asserts nothing"
    );

    for combination in &combinations {
        assert!(
            combination.kinds.len() >= 2,
            "{}: a combination carries at least two traits, or it is a case",
            combination.name
        );
        assert_eq!(
            combination.probes().len(),
            combination.kinds.len(),
            "{}: one probe per trait, so a failure names the construct it is about",
            combination.name
        );

        for kind in &combination.kinds {
            run(combination, &combination.schema, *kind).unwrap_or_else(|error| {
                panic!(
                    "{}: {kind:?} did not hold unbroken: {error:?}",
                    combination.name
                )
            });
        }
    }
}

/// Each probe on a combined table fails for its OWN construct and holds when
/// the other one is broken.
///
/// This is the property that makes a combination worth more than two cases. A
/// probe that noticed the *other* construct's breakage would report the wrong
/// thing and would let its own regress unseen -- so both directions are checked
/// rather than just the obvious one.
#[test]
fn a_probe_on_a_combined_table_asserts_only_its_own_construct() {
    for combination in Combination::all() {
        // Break the generated column: strip the generation, leaving an
        // ordinary column that stores nothing and reads NULL.
        let mut generated_broken = combination.schema.clone();
        strip_generation(&mut generated_broken, "combined_child", "doubled");

        // Break the referential action: leave the key, take the action.
        let mut action_broken = combination.schema.clone();
        strip_on_delete(&mut action_broken, "combined_child");

        let generated_reports = run(&combination, &generated_broken, Trait::GeneratedVirtual);
        assert!(
            matches!(generated_reports, Err(ProbeError::Failed(_))),
            "{}: the generated probe has to notice its own construct broken; it said \
             {generated_reports:?}",
            combination.name
        );
        let action_survives_generated_break =
            run(&combination, &generated_broken, Trait::ForeignKeyWithAction);
        assert!(
            action_survives_generated_break.is_ok(),
            "{}: the referential probe reported on a break that is not its construct: {:?}",
            combination.name,
            action_survives_generated_break
        );

        let action_reports = run(&combination, &action_broken, Trait::ForeignKeyWithAction);
        assert!(
            matches!(action_reports, Err(ProbeError::Failed(_))),
            "{}: the referential probe has to notice its own construct broken; it said \
             {action_reports:?}",
            combination.name
        );
        let generated_survives_action_break =
            run(&combination, &action_broken, Trait::GeneratedVirtual);
        assert!(
            generated_survives_action_break.is_ok(),
            "{}: the generated probe reported on a break that is not its construct: {:?}",
            combination.name,
            generated_survives_action_break
        );
    }
}

/// Take the generation off a column, leaving an ordinary one.
fn strip_generation(schema: &mut Schema, table: &str, column: &str) {
    let col = schema
        .tables
        .iter_mut()
        .find(|t| t.name == table)
        .unwrap_or_else(|| panic!("no table {table}"))
        .columns
        .iter_mut()
        .find(|c| c.name == column)
        .unwrap_or_else(|| panic!("no column {column}"));
    col.constraints.retain(|c| {
        !matches!(
            c,
            smugglr_forger::schema::ColumnConstraint::Generated { .. }
        )
    });
}

/// Take the `ON DELETE` action off a table's foreign key, leaving the key.
fn strip_on_delete(schema: &mut Schema, table: &str) {
    let fk = schema
        .tables
        .iter_mut()
        .find(|t| t.name == table)
        .unwrap_or_else(|| panic!("no table {table}"))
        .constraints
        .iter_mut()
        .find_map(|c| match c {
            TableConstraint::ForeignKey(fk) => Some(fk),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{table} declares no foreign key"));
    fk.on_delete = None;
}
