//! The validity grammar: every rule the model can violate and SQLite cannot.
//!
//! A schema is legal or illegal by cross-field invariants, not by field types.
//! `AUTOINCREMENT` is fine, `WITHOUT ROWID` is fine, and the two together are
//! not. A generated column is fine and a primary key is fine, and a generated
//! primary key is not. No derive expresses that, which is why this is a rule
//! engine rather than a macro, and why it is the bulk of the work in the model.
//!
//! # Where each invariant is enforced
//!
//! Three invariants are unrepresentable through [`super::builder`], which is
//! to say they are compile errors rather than runtime ones: `AUTOINCREMENT` on
//! a non-integer key, `AUTOINCREMENT` together with `WITHOUT ROWID`, and
//! `WITHOUT ROWID` with no primary key. Each of those is a property of the
//! *builder state*, which the type system can carry.
//!
//! Everything else needs values the type system does not have -- what the
//! other tables are, what the other columns are called, how many of them
//! there are -- and lands here, at construction. `Schema::validate` is run by
//! `SchemaBuilder::build`, so an authored schema is checked without the author
//! asking, and a literally-constructed schema can be checked on demand.
//!
//! The check stops at the first violation. Authoring is iterative and the
//! first error is the one being fixed.

use std::collections::HashSet;

use crate::error::ValidationError;

use super::{
    Column, ColumnConstraint, ColumnType, ForeignKey, Schema, SortOrder, Table, TableConstraint,
};

/// Storage classes a STRICT table accepts, upper-cased for comparison.
const STRICT_TYPES: [&str; 6] = ["INT", "INTEGER", "REAL", "TEXT", "BLOB", "ANY"];

pub fn validate(schema: &Schema) -> Result<(), ValidationError> {
    // Tables and triggers share one namespace in SQLite, so uniqueness is a
    // schema-wide question rather than a per-table one.
    let mut names: HashSet<&str> = HashSet::new();
    for table in &schema.tables {
        check_identifier(&table.name)?;
        if !names.insert(&table.name) {
            return Err(ValidationError::DuplicateTable {
                table: table.name.clone(),
            });
        }
    }
    for table in &schema.tables {
        for trigger in &table.triggers {
            check_identifier(&trigger.name)?;
            if !names.insert(&trigger.name) {
                return Err(ValidationError::DuplicateSchemaObject {
                    name: trigger.name.clone(),
                });
            }
            if trigger.body.is_empty() {
                return Err(ValidationError::EmptyTriggerBody {
                    table: table.name.clone(),
                    trigger: trigger.name.clone(),
                });
            }
        }
    }

    for table in &schema.tables {
        validate_table(table)?;
        for constraint in &table.constraints {
            if let TableConstraint::ForeignKey(fk) = constraint {
                validate_foreign_key(schema, table, fk)?;
            }
        }
    }

    Ok(())
}

fn validate_table(table: &Table) -> Result<(), ValidationError> {
    if table.columns.is_empty() {
        return Err(ValidationError::EmptyTable {
            table: table.name.clone(),
        });
    }

    let mut seen: HashSet<&str> = HashSet::new();
    for column in &table.columns {
        check_identifier(&column.name)?;
        if !seen.insert(&column.name) {
            return Err(ValidationError::DuplicateColumn {
                table: table.name.clone(),
                column: column.name.clone(),
            });
        }
    }

    // Every column a constraint names has to exist. A foreign key's parent
    // side is checked against the schema, not here.
    for constraint in &table.constraints {
        let named: Vec<&str> = match constraint {
            TableConstraint::PrimaryKey { columns, .. }
            | TableConstraint::Unique { columns, .. } => {
                columns.iter().map(|c| c.name.as_str()).collect()
            }
            TableConstraint::ForeignKey(fk) => fk.columns.iter().map(String::as_str).collect(),
            TableConstraint::Check(_) => Vec::new(),
        };
        for name in named {
            if !seen.contains(name) {
                return Err(ValidationError::UnknownColumn {
                    table: table.name.clone(),
                    column: name.to_string(),
                });
            }
        }
    }

    validate_primary_key(table)?;

    for column in &table.columns {
        validate_column(table, column)?;
    }

    Ok(())
}

/// Exactly the rules that depend on how the key was declared.
fn validate_primary_key(table: &Table) -> Result<(), ValidationError> {
    let column_level = table
        .columns
        .iter()
        .filter(|c| c.constraints.iter().any(is_primary_key))
        .count();
    let table_level = table
        .constraints
        .iter()
        .filter(|c| matches!(c, TableConstraint::PrimaryKey { .. }))
        .count();

    // SQLite rejects a table with two PRIMARY KEY declarations regardless of
    // whether they name the same columns.
    if column_level + table_level > 1 {
        return Err(ValidationError::MultiplePrimaryKeys {
            table: table.name.clone(),
        });
    }
    if table.without_rowid && column_level + table_level == 0 {
        return Err(ValidationError::WithoutRowidNeedsPrimaryKey {
            table: table.name.clone(),
        });
    }

    // A generated column has no stored value of its own to key on, whichever
    // way the key was spelled.
    for column in &table.columns {
        if column.generated().is_none() {
            continue;
        }
        let in_key = column.constraints.iter().any(is_primary_key)
            || table.constraints.iter().any(|c| match c {
                TableConstraint::PrimaryKey { columns, .. } => {
                    columns.iter().any(|ic| ic.name == column.name)
                }
                _ => false,
            });
        if in_key {
            return Err(ValidationError::GeneratedPrimaryKey {
                table: table.name.clone(),
                column: column.name.clone(),
            });
        }
    }

    Ok(())
}

fn validate_column(table: &Table, column: &Column) -> Result<(), ValidationError> {
    for constraint in &column.constraints {
        match constraint {
            ColumnConstraint::PrimaryKey {
                order,
                autoincrement: true,
                ..
            } => {
                // AUTOINCREMENT hands out keys from the rowid sequence, so it
                // needs a rowid to hand out from and a column that aliases it.
                // "INTEGER PRIMARY KEY DESC" is not that alias.
                if table.without_rowid {
                    return Err(ValidationError::AutoincrementWithoutRowid {
                        table: table.name.clone(),
                        column: column.name.clone(),
                    });
                }
                if column.decl_type != Some(ColumnType::Integer) || *order != SortOrder::Asc {
                    return Err(ValidationError::AutoincrementNeedsIntegerPrimaryKey {
                        table: table.name.clone(),
                        column: column.name.clone(),
                    });
                }
            }
            ColumnConstraint::Default(_) if column.generated().is_some() => {
                return Err(ValidationError::GeneratedWithDefault {
                    table: table.name.clone(),
                    column: column.name.clone(),
                });
            }
            _ => {}
        }
    }

    // A STRICT table admits six type names and nothing else, which makes the
    // typeless column and the invented type name -- both legal elsewhere --
    // illegal here.
    if table.strict {
        let ok = column
            .decl_type
            .as_ref()
            .is_some_and(|t| STRICT_TYPES.contains(&t.as_sql().to_ascii_uppercase().as_str()));
        if !ok {
            return Err(ValidationError::StrictNeedsStorageClass {
                table: table.name.clone(),
                column: column.name.clone(),
            });
        }
    }

    Ok(())
}

fn validate_foreign_key(
    schema: &Schema,
    table: &Table,
    fk: &ForeignKey,
) -> Result<(), ValidationError> {
    if fk.columns.is_empty() || fk.columns.len() != fk.parent_columns.len() {
        return Err(ValidationError::ForeignKeyArity {
            table: table.name.clone(),
            child: fk.columns.len(),
            parent: fk.parent_columns.len(),
            parent_table: fk.parent_table.clone(),
        });
    }

    // A self-reference is legal, so resolve against the whole schema rather
    // than excluding the declaring table.
    let parent =
        schema
            .table(&fk.parent_table)
            .ok_or_else(|| ValidationError::UnknownForeignKeyTable {
                table: table.name.clone(),
                parent_table: fk.parent_table.clone(),
            })?;

    for column in &fk.parent_columns {
        if parent.column(column).is_none() {
            return Err(ValidationError::UnknownForeignKeyColumn {
                table: table.name.clone(),
                parent_table: fk.parent_table.clone(),
                column: column.clone(),
            });
        }
    }

    Ok(())
}

fn is_primary_key(constraint: &ColumnConstraint) -> bool {
    matches!(constraint, ColumnConstraint::PrimaryKey { .. })
}

/// Identifiers are rendered inside double quotes with internal quotes doubled,
/// so almost anything survives. Two things do not: an empty name, which SQLite
/// rejects, and an embedded NUL, which truncates the statement.
fn check_identifier(name: &str) -> Result<(), ValidationError> {
    if name.is_empty() {
        return Err(ValidationError::EmptyIdentifier);
    }
    if name.contains('\0') {
        return Err(ValidationError::NulInIdentifier {
            name: name.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        Column, DefaultValue, Generated, IndexedColumn, ReferentialAction, Trigger, TriggerEvent,
        TriggerTiming,
    };

    /// Literal construction, because every rule here exists for a schema the
    /// builder refuses to express -- writing these through the builder would
    /// test nothing.
    fn column(
        name: &str,
        decl_type: Option<ColumnType>,
        constraints: Vec<ColumnConstraint>,
    ) -> Column {
        Column {
            name: name.to_string(),
            decl_type,
            constraints,
        }
    }

    fn pk(order: SortOrder, autoincrement: bool) -> ColumnConstraint {
        ColumnConstraint::PrimaryKey {
            order,
            autoincrement,
            on_conflict: None,
        }
    }

    fn table_of(name: &str, columns: Vec<Column>) -> Table {
        Table {
            name: name.to_string(),
            columns,
            constraints: Vec::new(),
            without_rowid: false,
            strict: false,
            triggers: Vec::new(),
        }
    }

    fn schema_of(tables: Vec<Table>) -> Schema {
        Schema { tables }
    }

    fn int_pk_table(name: &str) -> Table {
        table_of(
            name,
            vec![column(
                "id",
                Some(ColumnType::Integer),
                vec![pk(SortOrder::Asc, false)],
            )],
        )
    }

    #[test]
    fn a_plain_table_is_valid() {
        assert_eq!(validate(&schema_of(vec![int_pk_table("t")])), Ok(()));
    }

    #[test]
    fn autoincrement_needs_a_rowid_to_draw_from() {
        let mut table = int_pk_table("t");
        table.columns[0].constraints = vec![pk(SortOrder::Asc, true)];
        table.without_rowid = true;
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::AutoincrementWithoutRowid {
                table: "t".into(),
                column: "id".into(),
            })
        );
    }

    #[test]
    fn autoincrement_needs_an_integer_key() {
        let table = table_of(
            "t",
            vec![column(
                "id",
                Some(ColumnType::Text),
                vec![pk(SortOrder::Asc, true)],
            )],
        );
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::AutoincrementNeedsIntegerPrimaryKey {
                table: "t".into(),
                column: "id".into(),
            })
        );
    }

    #[test]
    fn a_descending_integer_key_is_not_the_rowid_alias() {
        let table = table_of(
            "t",
            vec![column(
                "id",
                Some(ColumnType::Integer),
                vec![pk(SortOrder::Desc, true)],
            )],
        );
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::AutoincrementNeedsIntegerPrimaryKey {
                table: "t".into(),
                column: "id".into(),
            })
        );
    }

    #[test]
    fn without_rowid_needs_a_key() {
        let mut table = table_of("t", vec![column("v", Some(ColumnType::Text), Vec::new())]);
        table.without_rowid = true;
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::WithoutRowidNeedsPrimaryKey { table: "t".into() })
        );
    }

    #[test]
    fn a_generated_column_cannot_be_the_key() {
        let table = table_of(
            "t",
            vec![column(
                "g",
                Some(ColumnType::Text),
                vec![
                    pk(SortOrder::Asc, false),
                    ColumnConstraint::Generated {
                        expr: "1".into(),
                        storage: Generated::Virtual,
                    },
                ],
            )],
        );
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::GeneratedPrimaryKey {
                table: "t".into(),
                column: "g".into(),
            })
        );
    }

    #[test]
    fn a_generated_column_cannot_hide_inside_a_composite_key() {
        let mut table = table_of(
            "t",
            vec![
                column("a", Some(ColumnType::Integer), Vec::new()),
                column(
                    "g",
                    Some(ColumnType::Text),
                    vec![ColumnConstraint::Generated {
                        expr: "'x'".into(),
                        storage: Generated::Stored,
                    }],
                ),
            ],
        );
        table.constraints = vec![TableConstraint::PrimaryKey {
            columns: vec![
                IndexedColumn::new("a", SortOrder::Asc),
                IndexedColumn::new("g", SortOrder::Asc),
            ],
            on_conflict: None,
        }];
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::GeneratedPrimaryKey {
                table: "t".into(),
                column: "g".into(),
            })
        );
    }

    #[test]
    fn a_generated_column_cannot_carry_a_default() {
        let table = table_of(
            "t",
            vec![column(
                "g",
                Some(ColumnType::Text),
                vec![
                    ColumnConstraint::Generated {
                        expr: "'x'".into(),
                        storage: Generated::Virtual,
                    },
                    ColumnConstraint::Default(DefaultValue::text("y")),
                ],
            )],
        );
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::GeneratedWithDefault {
                table: "t".into(),
                column: "g".into(),
            })
        );
    }

    #[test]
    fn two_keys_are_one_too_many() {
        let mut table = int_pk_table("t");
        table.columns.push(column(
            "other",
            Some(ColumnType::Text),
            vec![pk(SortOrder::Asc, false)],
        ));
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::MultiplePrimaryKeys { table: "t".into() })
        );
    }

    #[test]
    fn a_strict_table_refuses_a_typeless_column() {
        let mut table = int_pk_table("t");
        table.strict = true;
        table.columns.push(column("v", None, Vec::new()));
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::StrictNeedsStorageClass {
                table: "t".into(),
                column: "v".into(),
            })
        );
    }

    #[test]
    fn a_strict_table_refuses_an_invented_type_name() {
        let mut table = int_pk_table("t");
        table.strict = true;
        table.columns.push(column(
            "v",
            Some(ColumnType::Other("VARCHAR(8)".into())),
            Vec::new(),
        ));
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::StrictNeedsStorageClass {
                table: "t".into(),
                column: "v".into(),
            })
        );
    }

    #[test]
    fn a_strict_table_accepts_any() {
        let mut table = int_pk_table("t");
        table.strict = true;
        table.columns.push(column(
            "v",
            Some(ColumnType::Other("ANY".into())),
            Vec::new(),
        ));
        assert_eq!(validate(&schema_of(vec![table])), Ok(()));
    }

    #[test]
    fn tables_are_named_once() {
        assert_eq!(
            validate(&schema_of(vec![int_pk_table("t"), int_pk_table("t")])),
            Err(ValidationError::DuplicateTable { table: "t".into() })
        );
    }

    #[test]
    fn a_trigger_shares_the_table_namespace() {
        let mut table = int_pk_table("t");
        table.triggers = vec![Trigger {
            name: "t".into(),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            when: None,
            body: vec!["SELECT 1".into()],
        }];
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::DuplicateSchemaObject { name: "t".into() })
        );
    }

    #[test]
    fn a_trigger_needs_a_body() {
        let mut table = int_pk_table("t");
        table.triggers = vec![Trigger {
            name: "trg".into(),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            when: None,
            body: Vec::new(),
        }];
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::EmptyTriggerBody {
                table: "t".into(),
                trigger: "trg".into(),
            })
        );
    }

    #[test]
    fn columns_are_named_once() {
        let mut table = int_pk_table("t");
        table
            .columns
            .push(column("id", Some(ColumnType::Text), Vec::new()));
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::DuplicateColumn {
                table: "t".into(),
                column: "id".into(),
            })
        );
    }

    #[test]
    fn a_table_needs_a_column() {
        assert_eq!(
            validate(&schema_of(vec![table_of("t", Vec::new())])),
            Err(ValidationError::EmptyTable { table: "t".into() })
        );
    }

    #[test]
    fn a_constraint_cannot_name_a_column_that_is_not_there() {
        let mut table = int_pk_table("t");
        table.constraints = vec![TableConstraint::Unique {
            columns: vec![IndexedColumn::new("nope", SortOrder::Asc)],
            on_conflict: None,
        }];
        assert_eq!(
            validate(&schema_of(vec![table])),
            Err(ValidationError::UnknownColumn {
                table: "t".into(),
                column: "nope".into(),
            })
        );
    }

    fn child_with_fk(fk: ForeignKey) -> Table {
        let mut table = table_of(
            "child",
            vec![column("parent_id", Some(ColumnType::Integer), Vec::new())],
        );
        table.constraints = vec![TableConstraint::ForeignKey(fk)];
        table
    }

    #[test]
    fn a_foreign_key_resolves_against_the_schema() {
        let child = child_with_fk(ForeignKey {
            columns: vec!["parent_id".into()],
            parent_table: "ghost".into(),
            parent_columns: vec!["id".into()],
            on_delete: Some(ReferentialAction::Cascade),
            on_update: None,
        });
        assert_eq!(
            validate(&schema_of(vec![int_pk_table("parent"), child])),
            Err(ValidationError::UnknownForeignKeyTable {
                table: "child".into(),
                parent_table: "ghost".into(),
            })
        );
    }

    #[test]
    fn a_foreign_key_resolves_its_parent_columns() {
        let child = child_with_fk(ForeignKey {
            columns: vec!["parent_id".into()],
            parent_table: "parent".into(),
            parent_columns: vec!["ghost".into()],
            on_delete: None,
            on_update: None,
        });
        assert_eq!(
            validate(&schema_of(vec![int_pk_table("parent"), child])),
            Err(ValidationError::UnknownForeignKeyColumn {
                table: "child".into(),
                parent_table: "parent".into(),
                column: "ghost".into(),
            })
        );
    }

    #[test]
    fn a_foreign_key_maps_one_column_onto_one_column() {
        let child = child_with_fk(ForeignKey {
            columns: vec!["parent_id".into()],
            parent_table: "parent".into(),
            parent_columns: vec!["id".into(), "id".into()],
            on_delete: None,
            on_update: None,
        });
        assert_eq!(
            validate(&schema_of(vec![int_pk_table("parent"), child])),
            Err(ValidationError::ForeignKeyArity {
                table: "child".into(),
                child: 1,
                parent: 2,
                parent_table: "parent".into(),
            })
        );
    }

    #[test]
    fn a_self_reference_resolves() {
        let mut table = int_pk_table("node");
        table
            .columns
            .push(column("parent", Some(ColumnType::Integer), Vec::new()));
        table.constraints = vec![TableConstraint::ForeignKey(ForeignKey {
            columns: vec!["parent".into()],
            parent_table: "node".into(),
            parent_columns: vec!["id".into()],
            on_delete: Some(ReferentialAction::SetNull),
            on_update: None,
        })];
        assert_eq!(validate(&schema_of(vec![table])), Ok(()));
    }

    #[test]
    fn an_empty_name_is_not_an_identifier() {
        assert_eq!(
            validate(&schema_of(vec![int_pk_table("")])),
            Err(ValidationError::EmptyIdentifier)
        );
    }

    #[test]
    fn a_nul_truncates_a_statement_so_it_is_refused() {
        assert_eq!(
            validate(&schema_of(vec![int_pk_table("a\0b")])),
            Err(ValidationError::NulInIdentifier {
                name: "a\0b".into()
            })
        );
    }
}
