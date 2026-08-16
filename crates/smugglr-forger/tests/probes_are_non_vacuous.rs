//! Break the mechanism each probe guards, and watch the probe fail.
//!
//! A probe that passes against a deliberately broken schema is worse than no
//! probe: it is a green check that is a statement about nothing, and it will go
//! on being green for as long as the defect it was written for keeps arriving.
//! So every probe in the registry is run here against a database built from a
//! schema with its own trait taken away, and is required to fail.
//!
//! # How the break is expressed
//!
//! By rendering DDL, not by patching a database. Each break takes the case's
//! own schema, removes or swaps exactly the one thing the trait is named for,
//! and stands a fixture up from `Route::Ddl` -- which is what a transformation
//! that lost the construct would have produced. The probe is then handed the
//! schema that was *promised*, unbroken, and asked whether the database in
//! front of it behaves that way. That is the differential shape in miniature,
//! and forger can build it without knowing what a transformation is.
//!
//! A few breaks are not schema edits at all. A rebuild that re-creates a
//! trigger before copying rows into the new table produces a schema that is
//! correct in every particular and a database that has audited the same row
//! twice, so that break is expressed as the statements the rebuild would have
//! run. `after_seed` is for those.
//!
//! # Why some breaks are swaps rather than removals
//!
//! `ON CONFLICT ABORT` is SQLite's default. Dropping the clause changes the
//! DDL and changes nothing else, so the ABORT half is broken by swapping the
//! algorithm instead -- the defect it can actually suffer is being reconstructed
//! as a different algorithm, not being reconstructed as nothing.

use smugglr_forger::error::ProbeError;
use smugglr_forger::fixture::{Backing, Fixture, Route};
use smugglr_forger::registry::TraitCase;
use smugglr_forger::schema::{
    Column, ColumnConstraint, ColumnType, DefaultValue, OnConflict, Schema, SortOrder,
    TableConstraint, Trait,
};

/// One deliberately damaged world, and what was damaged about it.
struct Break {
    /// What was taken away, in the words the report will use.
    what: &'static str,
    /// The schema as a transformation that lost it would have left it.
    schema: Schema,
    /// Statements run after the seed, for breaks that live in what a rebuild
    /// did rather than in what it declared.
    after_seed: Vec<String>,
}

impl Break {
    fn schema(what: &'static str, schema: Schema) -> Self {
        Break {
            what,
            schema,
            after_seed: Vec::new(),
        }
    }

    fn then(mut self, statements: &[&str]) -> Self {
        self.after_seed = statements.iter().map(|s| s.to_string()).collect();
        self
    }
}

#[test]
fn every_probe_fails_when_the_mechanism_it_guards_is_broken() {
    for kind in Trait::ALL {
        let case = TraitCase::for_trait(kind);
        let breaks = breaks(kind);
        assert!(
            !breaks.is_empty(),
            "{kind:?} has no break, so its probe has never been shown to fail"
        );

        for broken in breaks {
            let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
            fixture
                .bring_to(Route::Ddl(&broken.schema.to_ddl()))
                .unwrap_or_else(|error| panic!("{kind:?} / {}: {error}", broken.what));
            case.seed(fixture.conn())
                .unwrap_or_else(|error| panic!("{kind:?} / {}: seed: {error}", broken.what));
            for statement in &broken.after_seed {
                fixture
                    .conn()
                    .execute_batch(statement)
                    .unwrap_or_else(|error| panic!("{kind:?} / {}: {error}", broken.what));
            }

            // The promise is the case's own schema; the database is the broken
            // one. Nothing else about the probe changes.
            let outcome = case.probe(fixture.conn());
            match outcome {
                Err(ProbeError::Failed(message)) => {
                    println!("{kind:?} -- broke: {}\n  said: {message}\n", broken.what);
                }
                other => panic!(
                    "{kind:?} probe did not report a failure with {} broken; it said {other:?}",
                    broken.what
                ),
            }
        }
    }
}

/// The break table.
///
/// Exhaustive over [`Trait`] and free of a catch-all arm, exactly like the
/// registry it tests: a new trait cannot be added without someone writing down
/// how to break it and watching the probe notice.
fn breaks(kind: Trait) -> Vec<Break> {
    let schema = || TraitCase::for_trait(kind).schema;
    match kind {
        Trait::ForeignKeyWithAction => {
            let mut dropped_action = schema();
            foreign_key_mut(&mut dropped_action, "cascade_child").on_delete = None;

            // The update side gets its own break rather than riding on the one
            // above. Both actions come off the same pragma row, so it is easy to
            // assume one break covers the clause -- but the probe reads them in
            // separate arms against separate tables, and only a break that
            // touches `on_update` can show the update arm asserting anything
            // (#374).
            let mut dropped_update_action = schema();
            foreign_key_mut(&mut dropped_update_action, "updating_child").on_update = None;

            // One break per action, not one for the clause. All four come off
            // the same pragma column and are lost by the same mechanism, which
            // is exactly why a single break feels sufficient and is not: each
            // arm asserts a different landing, and only a break touching that
            // arm's action can show it asserting anything (#384).
            let mut dropped_set_null = schema();
            foreign_key_mut(&mut dropped_set_null, "nulling_child").on_update = None;

            let mut dropped_set_default = schema();
            foreign_key_mut(&mut dropped_set_default, "defaulting_child").on_update = None;

            let mut dropped_key = schema();
            table_mut(&mut dropped_key, "restrict_child")
                .constraints
                .retain(|c| !matches!(c, TableConstraint::ForeignKey(_)));

            vec![
                // smugglr#341's shape: the rebuild reads five of the eight
                // columns pragma foreign_key_list hands it, and the referential
                // action is in one of the three it does not.
                Break::schema(
                    "ON DELETE CASCADE, leaving the key with the NO ACTION default",
                    dropped_action,
                ),
                Break::schema(
                    "ON UPDATE CASCADE, leaving the key with the NO ACTION default -- the same \
                     loss as above, on the half of the clause that had no probe before #374",
                    dropped_update_action,
                ),
                Break::schema(
                    "ON UPDATE SET NULL, so the parent key cannot move and the child is never \
                     cut loose",
                    dropped_set_null,
                ),
                Break::schema(
                    "ON UPDATE SET DEFAULT, so the parent key cannot move and the child never \
                     falls back to its declared default",
                    dropped_set_default,
                ),
                Break::schema(
                    "the RESTRICT child's foreign key, dropped whole",
                    dropped_key,
                ),
            ]
        }

        Trait::GeneratedVirtual => {
            let mut ordinary = schema();
            drop_generated(&mut ordinary, "virtual_generated", "doubled");
            vec![Break::schema(
                "GENERATED ALWAYS AS (...) VIRTUAL, re-created as an ordinary column",
                ordinary,
            )]
        }

        Trait::GeneratedStored => {
            let mut ordinary = schema();
            drop_generated(&mut ordinary, "stored_generated", "tripled");
            vec![Break::schema(
                "GENERATED ALWAYS AS (...) STORED, re-created as an ordinary column holding the \
                 value a rebuild would have copied into it",
                ordinary,
            )
            // Without this the break is caught by the first read and the
            // interesting half is never reached. With it, the column holds
            // exactly what a row dump would show and differs only in that it
            // has stopped computing -- which is the whole defect.
            .then(&["UPDATE \"stored_generated\" SET \"tripled\" = \"base\" * 3"])]
        }

        Trait::ColumnOnConflict => {
            let mut no_replace = schema();
            drop_conflict(&mut no_replace, "replace_absorbs", "k");
            let mut no_ignore = schema();
            drop_conflict(&mut no_ignore, "ignore_absorbs", "v");
            let mut no_rollback = schema();
            drop_conflict(&mut no_rollback, "rollback_throws", "v");
            let mut abort_swapped = schema();
            set_conflict(&mut abort_swapped, "abort_throws", "v", OnConflict::Ignore);

            vec![
                Break::schema("ON CONFLICT REPLACE, leaving a plain UNIQUE", no_replace),
                Break::schema("ON CONFLICT IGNORE, leaving a plain NOT NULL", no_ignore),
                Break::schema(
                    "ON CONFLICT ROLLBACK, leaving a plain NOT NULL -- which throws exactly like \
                     ROLLBACK and differs only in what happens to the transaction",
                    no_rollback,
                ),
                // Not a removal: ABORT is the default, so a dropped clause is
                // behaviourally the same table. What ABORT can lose is its
                // identity among the other four.
                Break::schema("ON CONFLICT ABORT, swapped for IGNORE", abort_swapped),
            ]
        }

        Trait::ExpressionDefault => {
            let mut literal_timestamp = schema();
            set_default(
                &mut literal_timestamp,
                "expression_default",
                "made_at",
                DefaultValue::text("datetime('now')"),
            );
            let mut literal_arithmetic = schema();
            set_default(
                &mut literal_arithmetic,
                "expression_default",
                "computed",
                DefaultValue::text("2 + 3"),
            );
            // The two above are caught by the probe's first assertion, which
            // compares the stored value to the source of its own default. That
            // leaves the two assertions behind it -- the timestamp's shape and
            // the arithmetic's type -- never run in the failing direction, and
            // an assertion never seen to fail is an assertion nobody has
            // checked. These two break the expression into a *different*
            // expression: it still evaluates, so the first assertion passes and
            // each of the other two is reached alone.
            let mut date_only = schema();
            set_default(
                &mut date_only,
                "expression_default",
                "made_at",
                DefaultValue::expr("date('now')"),
            );
            let mut concatenated = schema();
            set_default(
                &mut concatenated,
                "expression_default",
                "computed",
                DefaultValue::expr("'2' || '+' || '3'"),
            );

            vec![
                Break::schema(
                    "the parentheses around DEFAULT (datetime('now')), making it the literal text \
                     of itself",
                    literal_timestamp,
                ),
                Break::schema(
                    "the parentheses around DEFAULT (2 + 3), making it the literal text of itself",
                    literal_arithmetic,
                ),
                Break::schema(
                    "the time out of DEFAULT (datetime('now')), leaving an expression that still \
                     evaluates and no longer yields a timestamp",
                    date_only,
                ),
                Break::schema(
                    "the arithmetic in DEFAULT (2 + 3), leaving string concatenation that still \
                     evaluates and yields text that merely looks like a number",
                    concatenated,
                ),
            ]
        }

        Trait::TypelessColumn => {
            let mut resolved = schema();
            column_mut(&mut resolved, "typeless", "v").decl_type = Some(ColumnType::Text);
            let mut promoted = schema();
            column_mut(&mut promoted, "typeless", "v").decl_type = Some(ColumnType::Blob);
            vec![
                Break::schema(
                    "the blank type, resolved to TEXT -- which converts both values and destroys \
                     the dynamic typing",
                    resolved,
                ),
                // smugglr#344's promotion. Everything behavioural about it is
                // identical on this path: BLOB affinity converts nothing, so
                // typeof() still reads text and integer. Only the declared type
                // gives it away, which is why this probe reads one.
                Break::schema(
                    "the blank type, promoted to BLOB -- invisible to every behavioural assertion \
                     here",
                    promoted,
                ),
            ]
        }

        Trait::Trigger => {
            let mut no_trigger = schema();
            table_mut(&mut no_trigger, "evented").triggers.clear();
            vec![
                Break::schema("the trigger, dropped with the table it hung off", no_trigger),
                // The schema is untouched and correct. What is broken is the
                // order a rebuild did things in: re-creating the trigger before
                // copying the rows in makes the copy re-fire it over rows that
                // had already been audited. smugglr#336's ["before", "after"].
                Break::schema("nothing in the schema -- a rebuild re-fired the trigger over the rows it copied", schema())
                    .then(&[
                        "DROP TRIGGER \"evented_audit\";
                         CREATE TABLE \"evented_new\" (\"id\" INTEGER PRIMARY KEY, \"note\" TEXT);
                         CREATE TRIGGER \"evented_audit\" AFTER INSERT ON \"evented_new\"
                         FOR EACH ROW BEGIN
                           INSERT INTO \"audit\" (\"note\") VALUES (new.\"note\");
                         END;
                         INSERT INTO \"evented_new\" SELECT * FROM \"evented\";
                         DROP TABLE \"evented\";
                         ALTER TABLE \"evented_new\" RENAME TO \"evented\";",
                    ]),
            ]
        }

        Trait::DescendingPrimaryKey => {
            let mut ascending = schema();
            for constraint in &mut column_mut(&mut ascending, "descending_key", "id").constraints {
                if let ColumnConstraint::PrimaryKey { order, .. } = constraint {
                    *order = SortOrder::Asc;
                }
            }
            vec![Break::schema(
                "DESC on the key, making it the ascending spelling -- which is the rowid alias",
                ascending,
            )]
        }
    }
}

// --- model surgery, so the breaks read as the edits they are ----------------

fn table_mut<'a>(schema: &'a mut Schema, name: &str) -> &'a mut smugglr_forger::schema::Table {
    schema
        .tables
        .iter_mut()
        .find(|table| table.name == name)
        .unwrap_or_else(|| panic!("the case schema has a table called {name}"))
}

fn column_mut<'a>(schema: &'a mut Schema, table: &str, column: &str) -> &'a mut Column {
    table_mut(schema, table)
        .columns
        .iter_mut()
        .find(|c| c.name == column)
        .unwrap_or_else(|| panic!("the case schema has a column called {table}.{column}"))
}

fn foreign_key_mut<'a>(
    schema: &'a mut Schema,
    table: &str,
) -> &'a mut smugglr_forger::schema::ForeignKey {
    table_mut(schema, table)
        .constraints
        .iter_mut()
        .find_map(|constraint| match constraint {
            TableConstraint::ForeignKey(fk) => Some(fk),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{table} declares a foreign key"))
}

fn drop_generated(schema: &mut Schema, table: &str, column: &str) {
    column_mut(schema, table, column)
        .constraints
        .retain(|c| !matches!(c, ColumnConstraint::Generated { .. }));
}

fn drop_conflict(schema: &mut Schema, table: &str, column: &str) {
    set_conflict_slot(schema, table, column, None);
}

fn set_conflict(schema: &mut Schema, table: &str, column: &str, algorithm: OnConflict) {
    set_conflict_slot(schema, table, column, Some(algorithm));
}

fn set_conflict_slot(
    schema: &mut Schema,
    table: &str,
    column: &str,
    algorithm: Option<OnConflict>,
) {
    for constraint in &mut column_mut(schema, table, column).constraints {
        if let ColumnConstraint::NotNull(slot) | ColumnConstraint::Unique(slot) = constraint {
            *slot = algorithm;
        }
    }
}

fn set_default(schema: &mut Schema, table: &str, column: &str, value: DefaultValue) {
    for constraint in &mut column_mut(schema, table, column).constraints {
        if let ColumnConstraint::Default(slot) = constraint {
            *slot = value.clone();
        }
    }
}
