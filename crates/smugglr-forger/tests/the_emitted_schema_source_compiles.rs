//! The emitted schema source, pasted into a test file -- which is this one.
//!
//! FR-FORGER-008 requires a failure to print the minimal reproducing schema in
//! the builder's readable form, and requires that output to be pasteable into a
//! test file that compiles. Eyeballing it is not evidence, so this target is the
//! evidence: every block below is
//! [`failure::builder_source`](smugglr_forger::failure::builder_source) output,
//! copied in unaltered, and cargo compiles this file like any other test target.
//! If the emitter ever produced something that does not compile, this crate
//! stops building.
//!
//! Compiling is only half of it -- a block that compiles and describes some
//! other schema is worse than one that does not compile at all -- so two tests
//! close the loop:
//!
//! * the pasted source builds a schema equal to the registry case it came from,
//!   which is what makes it a *reproduction*; and
//! * the pasted text still matches what the emitter emits today, read back out
//!   of this very file, which is what stops it from quietly becoming a
//!   hand-maintained copy of an emitter that has since changed.
//!
//! Each function carries `#[rustfmt::skip]`. The text is machine-written and is
//! compared against a machine, so rustfmt must not be a third author of it.
//! When the emitter changes, the comparison test fails naming the trait whose
//! block drifted and printing the replacement text -- paste that between the
//! markers and the file is current again.

use smugglr_forger::failure::builder_source;
use smugglr_forger::registry::TraitCase;
use smugglr_forger::schema::{Schema, Trait};

/// This file, read back at compile time, so the test can compare the text that
/// was compiled against the text the emitter produces now. A literal filename
/// rather than `file!()`, which resolves against a different root.
const THIS_FILE: &str = include_str!("the_emitted_schema_source_compiles.rs");

#[rustfmt::skip]
fn foreign_key_with_action() -> Schema {
    // >>> ForeignKeyWithAction
    use smugglr_forger::schema::builder::{schema, table};
    use smugglr_forger::schema::{ColumnType::*, ReferentialAction};

    schema()
        .table(
            table("keeper")
                .pk_int("id")
                .col("label", Text, []),
        )
        .table(
            table("cascade_child")
                .pk_int("id")
                .col("keeper_id", Integer, [])
                .col("label", Text, [])
                .fk(["keeper_id"], "keeper", ["id"])
                .on_delete(ReferentialAction::Cascade),
        )
        .table(
            table("restrict_child")
                .pk_int("id")
                .col("keeper_id", Integer, [])
                .col("label", Text, [])
                .fk(["keeper_id"], "keeper", ["id"])
                .on_delete(ReferentialAction::Restrict),
        )
        .build()
        .expect("a valid schema")
    // <<< ForeignKeyWithAction
}

#[rustfmt::skip]
fn generated_virtual() -> Schema {
    // >>> GeneratedVirtual
    use smugglr_forger::schema::builder::{schema, table, Attr};
    use smugglr_forger::schema::ColumnType::*;

    schema()
        .table(
            table("virtual_generated")
                .pk_int("id")
                .col("base", Integer, [])
                .col("doubled", Integer, [Attr::Virtual("\"base\" * 2".into())])
                .col("label", Text, []),
        )
        .build()
        .expect("a valid schema")
    // <<< GeneratedVirtual
}

#[rustfmt::skip]
fn generated_stored() -> Schema {
    // >>> GeneratedStored
    use smugglr_forger::schema::builder::{schema, table, Attr};
    use smugglr_forger::schema::ColumnType::*;

    schema()
        .table(
            table("stored_generated")
                .pk_int("id")
                .col("base", Integer, [])
                .col("tripled", Integer, [Attr::Stored("\"base\" * 3".into())])
                .col("label", Text, []),
        )
        .build()
        .expect("a valid schema")
    // <<< GeneratedStored
}

#[rustfmt::skip]
fn column_on_conflict() -> Schema {
    // >>> ColumnOnConflict
    use smugglr_forger::schema::builder::{schema, table, Attr};
    use smugglr_forger::schema::ColumnType::*;

    schema()
        .table(
            table("replace_absorbs")
                .pk_int("id")
                .col("k", Text, [Attr::Unique, Attr::OnConflictReplace])
                .col("label", Text, []),
        )
        .table(
            table("ignore_absorbs")
                .pk_int("id")
                .col("v", Text, [Attr::NotNull, Attr::OnConflictIgnore])
                .col("label", Text, []),
        )
        .table(
            table("abort_throws")
                .pk_int("id")
                .col("v", Text, [Attr::NotNull, Attr::OnConflictAbort])
                .col("label", Text, []),
        )
        .table(
            table("rollback_throws")
                .pk_int("id")
                .col("v", Text, [Attr::NotNull, Attr::OnConflictRollback])
                .col("label", Text, []),
        )
        .build()
        .expect("a valid schema")
    // <<< ColumnOnConflict
}

#[rustfmt::skip]
fn expression_default() -> Schema {
    // >>> ExpressionDefault
    use smugglr_forger::schema::builder::{schema, table, Attr};
    use smugglr_forger::schema::{ColumnType::*, DefaultValue};

    schema()
        .table(
            table("expression_default")
                .pk_int("id")
                .col("made_at", Text, [Attr::Default(DefaultValue::expr("datetime('now')"))])
                .col("computed", Integer, [Attr::Default(DefaultValue::expr("2 + 3"))])
                .col("label", Text, []),
        )
        .build()
        .expect("a valid schema")
    // <<< ExpressionDefault
}

#[rustfmt::skip]
fn typeless_column() -> Schema {
    // >>> TypelessColumn
    use smugglr_forger::schema::builder::{schema, table};
    use smugglr_forger::schema::ColumnType::*;

    schema()
        .table(
            table("typeless")
                .pk_int("id")
                .typeless("v", [])
                .col("label", Text, []),
        )
        .build()
        .expect("a valid schema")
    // <<< TypelessColumn
}

#[rustfmt::skip]
fn trigger() -> Schema {
    // >>> Trigger
    use smugglr_forger::schema::builder::{schema, table};
    use smugglr_forger::schema::{ColumnType::*, Trigger, TriggerEvent, TriggerTiming};

    schema()
        .table(
            table("evented")
                .pk_int("id")
                .col("note", Text, [])
                .trigger(Trigger {
                    name: "evented_audit".into(),
                    timing: TriggerTiming::After,
                    event: TriggerEvent::Insert,
                    when: None,
                    body: vec!["INSERT INTO \"audit\" (\"note\") VALUES (new.\"note\")".into()],
                }),
        )
        .table(
            table("audit")
                .pk_int("id")
                .col("note", Text, []),
        )
        .build()
        .expect("a valid schema")
    // <<< Trigger
}

#[rustfmt::skip]
fn descending_primary_key() -> Schema {
    // >>> DescendingPrimaryKey
    use smugglr_forger::schema::builder::{schema, table};
    use smugglr_forger::schema::{ColumnType::*, SortOrder};

    schema()
        .table(
            table("descending_key")
                .pk_col("id", Integer, SortOrder::Desc)
                .col("label", Text, []),
        )
        .build()
        .expect("a valid schema")
    // <<< DescendingPrimaryKey
}

/// The block for a trait, exhaustively and with no catch-all arm -- the same
/// mechanism the registry uses, and for the same reason: a new [`Trait`] must
/// not be able to arrive without someone pasting its emitted source in here and
/// watching it compile.
fn pasted(kind: Trait) -> Schema {
    match kind {
        Trait::ForeignKeyWithAction => foreign_key_with_action(),
        Trait::GeneratedVirtual => generated_virtual(),
        Trait::GeneratedStored => generated_stored(),
        Trait::ColumnOnConflict => column_on_conflict(),
        Trait::ExpressionDefault => expression_default(),
        Trait::TypelessColumn => typeless_column(),
        Trait::Trigger => trigger(),
        Trait::DescendingPrimaryKey => descending_primary_key(),
    }
}

/// The pasted source describes the schema it was emitted from.
///
/// Without this the file would prove only that the emitter produces *something*
/// that compiles, and something that compiles and builds a different schema is
/// a reproduction of a different bug.
#[test]
fn the_pasted_source_builds_the_schema_it_came_from() {
    for kind in Trait::ALL {
        assert_eq!(
            pasted(kind),
            TraitCase::for_trait(kind).schema,
            "the source pasted in for {kind:?} builds a different schema than the case it \
             came from"
        );
    }
}

/// The pasted source is still what the emitter emits.
///
/// Compared with whitespace collapsed, because the paste is indented to sit in
/// a function body and the emitter writes at column zero. Everything that is
/// not whitespace has to match: a changed identifier, a dropped attribute or a
/// moved comma all read as drift.
#[test]
fn the_pasted_source_is_what_the_emitter_emits_today() {
    for kind in Trait::ALL {
        let emitted = builder_source(&TraitCase::for_trait(kind).schema);
        assert!(
            emitted.is_builder(),
            "{kind:?} no longer emits builder source at all: {emitted}"
        );
        // The failure carries the replacement, so re-emitting a drifted block
        // is copying it out of this message rather than finding a tool.
        assert_eq!(
            collapsed(&block(kind)),
            collapsed(emitted.text()),
            "the block pasted in for {kind:?} is not what the emitter emits now. Replace the \
             text between this file's markers for {kind:?} with:\n\n{}\n",
            emitted.text()
        );
    }
}

/// The text between this trait's markers in this file.
fn block(kind: Trait) -> String {
    let open = format!("// >>> {kind:?}\n");
    let close = format!("// <<< {kind:?}");
    let start = THIS_FILE
        .find(&open)
        .unwrap_or_else(|| panic!("{kind:?} has no pasted block in this file"))
        + open.len();
    let end = THIS_FILE[start..]
        .find(&close)
        .unwrap_or_else(|| panic!("{kind:?}'s pasted block is never closed"));
    THIS_FILE[start..start + end].to_string()
}

/// Every run of whitespace as one space, so indentation is not a difference.
fn collapsed(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}
