//! Break a schema, watch a probe fail, send it through JSON, and require the
//! same failure out the other side.
//!
//! # Why equality is not the test
//!
//! A schema that serializes and deserializes to something equal is necessary
//! and nowhere near sufficient. What a regression fixture promises is that the
//! *failure* survives -- and a schema can compare equal while the thing that
//! reproduces the defect lives somewhere the fixture never carried: in the
//! statements a rebuild ran, in the trait whose probe is supposed to look at
//! it, in the message that says which of a probe's several assertions fired.
//! So the round trip here is measured at the far end, by running the
//! deserialized fixture and comparing what its probe said to what the live
//! one said. The equality assertion is kept as well, one test down, because
//! when both fail together the narrower one says where.
//!
//! # The failure being reproduced
//!
//! smugglr#341's shape: a rebuild reads five of the eight columns
//! `pragma foreign_key_list` hands it, and the referential action is in one of
//! the three it does not, so `ON DELETE CASCADE` comes back as the `NO ACTION`
//! default. Nothing about it is visible at the row counts on the day of the
//! migration -- the children are still there, which is exactly what a rebuild
//! that kept them would look like.

use smugglr_forger::corpus::{CorpusError, Regression};
use smugglr_forger::error::ProbeError;
use smugglr_forger::fixture::{Backing, Fixture, Route};
use smugglr_forger::registry::TraitCase;
use smugglr_forger::schema::{Schema, TableConstraint, Trait};

/// The registry case's schema with `ON DELETE CASCADE` taken off the cascading
/// child -- what the rebuild in smugglr#341 produced.
fn cascade_lost() -> Schema {
    let mut schema = TraitCase::for_trait(Trait::ForeignKeyWithAction).schema;
    let foreign_key = schema
        .tables
        .iter_mut()
        .find(|table| table.name == "cascade_child")
        .expect("the case schema has a cascade_child table")
        .constraints
        .iter_mut()
        .find_map(|constraint| match constraint {
            TableConstraint::ForeignKey(fk) => Some(fk),
            _ => None,
        })
        .expect("cascade_child declares a foreign key");
    foreign_key.on_delete = None;
    schema
}

/// Run the broken schema the way `tests/probes_are_non_vacuous.rs` does, with
/// nothing serialized anywhere, and return what the probe said.
fn live_failure(schema: &Schema, kind: Trait) -> String {
    let case = TraitCase::for_trait(kind);
    let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
    fixture
        .bring_to(Route::Ddl(&schema.to_ddl()))
        .expect("the broken schema still renders DDL SQLite accepts");
    case.seed(fixture.conn()).expect("seed");
    match case.probe(fixture.conn()) {
        Err(ProbeError::Failed(message)) => message,
        other => panic!("the break did not make the probe fail; it said {other:?}"),
    }
}

fn recorded(schema: Schema, kind: Trait, failure: String) -> Regression {
    Regression {
        provenance: "the round-trip test's own fixture, built in memory".into(),
        kind,
        schema,
        after_seed: Vec::new(),
        expected_failure: failure,
    }
}

#[test]
fn a_failure_recorded_as_json_reproduces_itself_when_read_back() {
    let schema = cascade_lost();
    let failure = live_failure(&schema, Trait::ForeignKeyWithAction);
    assert!(
        !failure.is_empty(),
        "a probe that fails with an empty message records nothing"
    );

    let json = recorded(schema, Trait::ForeignKeyWithAction, failure.clone()).to_json();
    let read_back = Regression::from_json(&json).expect("the fixture parses");

    // The whole requirement: not that the value survived, but that running
    // what came back out reproduces the failure that went in. `run` accepts
    // only a `ProbeError::Failed` whose message matches, so this passing is
    // the message having survived byte for byte.
    read_back
        .run()
        .unwrap_or_else(|error| panic!("the recorded failure did not reproduce: {error}"));
    assert_eq!(read_back.expected_failure, failure);
}

#[test]
fn the_schema_itself_survives_the_round_trip() {
    let schema = cascade_lost();
    let fixture = recorded(schema.clone(), Trait::ForeignKeyWithAction, "any".into());
    let read_back = Regression::from_json(&fixture.to_json()).expect("the fixture parses");

    assert_eq!(read_back.schema, schema);
    assert_eq!(read_back.schema.to_ddl(), schema.to_ddl());
    assert_eq!(read_back, fixture);
}

#[test]
fn a_fixture_whose_defect_stopped_reproducing_is_a_failure() {
    // The unbroken case schema: the cascade is intact, so the probe passes and
    // the fixture is no longer about anything. A corpus that reported this as
    // green would keep a fixture that has quietly stopped asserting.
    let intact = TraitCase::for_trait(Trait::ForeignKeyWithAction).schema;
    let fixture = recorded(
        intact,
        Trait::ForeignKeyWithAction,
        live_failure(&cascade_lost(), Trait::ForeignKeyWithAction),
    );

    assert!(matches!(fixture.run(), Err(CorpusError::NoFailure)));
}

#[test]
fn a_fixture_reproducing_some_other_failure_is_a_failure() {
    // Same break, same probe, a message from a different assertion. Running it
    // has to notice, or the recorded message is decoration and every fixture
    // in the corpus asserts only that something somewhere went wrong.
    let fixture = recorded(
        cascade_lost(),
        Trait::ForeignKeyWithAction,
        "some other thing the probe never said".into(),
    );

    match fixture.run() {
        Err(CorpusError::DifferentFailure { reported, .. }) => {
            assert_eq!(
                reported,
                live_failure(&cascade_lost(), Trait::ForeignKeyWithAction)
            );
        }
        other => panic!("a mismatched message was not reported as one: {other:?}"),
    }
}

#[test]
fn a_probe_that_could_not_observe_anything_is_not_the_failure() {
    // The trigger case, seeded and then emptied. The probe reports `Unseeded`
    // rather than `Failed` -- and a fixture that accepted it would be a
    // committed guard reproducing nothing at all, which is the one outcome
    // that must never read as a finding.
    let case = TraitCase::for_trait(Trait::Trigger);
    let fixture = Regression {
        provenance: "a trigger fixture whose rows never arrive".into(),
        kind: Trait::Trigger,
        schema: case.schema,
        after_seed: vec!["DELETE FROM \"audit\"; DELETE FROM \"evented\";".into()],
        expected_failure: "nothing to observe: the audit table is empty".into(),
    };

    assert!(matches!(fixture.run(), Err(CorpusError::NotAFinding(_))));
}

#[test]
fn a_fixture_that_asserts_nothing_is_refused_at_the_door() {
    let empty_message = recorded(cascade_lost(), Trait::ForeignKeyWithAction, String::new());
    assert!(matches!(
        Regression::from_json(&empty_message.to_json()),
        Err(CorpusError::NoExpectedFailure)
    ));

    let mut anonymous = recorded(cascade_lost(), Trait::ForeignKeyWithAction, "said".into());
    anonymous.provenance = "   ".into();
    assert!(matches!(
        Regression::from_json(&anonymous.to_json()),
        Err(CorpusError::NoProvenance)
    ));
}

#[test]
fn a_misspelled_key_is_refused_rather_than_ignored() {
    // A fixture is hand-editable. `after_seeds` silently ignored would leave a
    // file that looks like it runs statements after the seed and does not, and
    // the fixture would be testing something other than what it says.
    //
    // #372 asked whether this test was vacuous. Measured: removing
    // `deny_unknown_fields` from `Regression` fails it, so the guard is real.
    //
    // But NON-VACUOUS AND SPECIFIC ARE DIFFERENT PROPERTIES, and this asserted
    // only `Err(Parse(_))` -- which any parse failure satisfies, including one
    // that has nothing to do with the key. It read as though it had proved the
    // field was guarded while proving something weaker.
    //
    // The form that does prove it was already in this file, one function below,
    // written for the nested types in #368. The envelope now uses it too rather
    // than keeping a second, laxer standard for the outer type.
    refuses_the_misspelling(
        cascade_lost(),
        Trait::ForeignKeyWithAction,
        "after_seed",
        "after_seeds",
    );
}

/// Misspell one key in a fixture's JSON and hand the result back to the parser,
/// requiring that what comes back is a refusal naming that key.
///
/// Two things are checked that a bare `Err(Parse(_))` would not. The
/// misspelling has to have landed -- a `.replace` matching nothing leaves valid
/// JSON, and the assertion would then be about a file nobody edited. And the
/// refusal has to be *about* the key, because a parse error from some unrelated
/// cause would satisfy `Err(Parse(_))` while the test read as though it had
/// proved the field was guarded. That is the same defect shape this whole
/// mechanism exists to refuse, one level up.
fn refuses_the_misspelling(schema: Schema, kind: Trait, correct: &str, misspelled: &str) {
    let json = recorded(schema, kind, "said".into()).to_json();
    assert!(
        json.contains(&format!("\"{correct}\"")),
        "the serialized fixture has no {correct} key to misspell, so this test edits nothing"
    );
    let edited = json.replace(&format!("\"{correct}\""), &format!("\"{misspelled}\""));

    match Regression::from_json(&edited) {
        Err(CorpusError::Parse(source)) => {
            let said = source.to_string();
            assert!(
                said.contains(&format!("unknown field `{misspelled}`")),
                "the fixture was refused, but not for the misspelled key -- serde said {said:?}"
            );
        }
        Ok(_) => panic!(
            "{misspelled} parsed as though it were {correct}; the field was dropped and the \
             fixture now tests something other than what it reads as"
        ),
        Err(other) => panic!("{misspelled} was refused for the wrong reason: {other}"),
    }
}

#[test]
fn a_misspelled_key_on_a_nested_schema_type_is_refused() {
    // `decl_types` for `decl_type`. Dropped silently, the column becomes
    // typeless -- which is a trait of its own with its own probe, so the
    // fixture would still stand up, still run, and be about a different
    // defect than the one its JSON reads as declaring.
    refuses_the_misspelling(
        cascade_lost(),
        Trait::ForeignKeyWithAction,
        "decl_type",
        "decl_types",
    );

    // `trigger` for `triggers`, the other misspelling that is one character
    // from the real key. Dropped, the table renders without its trigger and
    // the Trigger case's probe reports a defect nobody introduced.
    refuses_the_misspelling(
        TraitCase::for_trait(Trait::Trigger).schema,
        Trait::Trigger,
        "triggers",
        "trigger",
    );
}

#[test]
fn a_misspelled_key_inside_an_enum_variant_is_refused() {
    // The nesting that container-level `deny_unknown_fields` has to reach
    // *through* a variant to guard: `on_conflict` is a field of
    // `ColumnConstraint::PrimaryKey`, not of any struct. If serde's attribute
    // stopped at the enum, every struct variant in the model would still be an
    // open door, and this is the only test that would notice.
    //
    // `on_conflict` rather than its siblings, and this is the part worth
    // reading twice. Serde supplies `None` for a missing `Option` field
    // whether or not anyone wrote `#[serde(default)]`, so this is the one
    // field of the variant a misspelling drops in silence -- `autoincrement`
    // is a `bool` and comes back as "missing field" on its own. Which is to
    // say the attribute is load-bearing exactly where the model is optional,
    // and `on_conflict` going missing is not a small loss: it is the whole of
    // `Trait::ColumnOnConflict`, and it changes what an INSERT does to rows
    // that are already there.
    refuses_the_misspelling(
        cascade_lost(),
        Trait::ForeignKeyWithAction,
        "on_conflict",
        "on_conflcit",
    );
}
