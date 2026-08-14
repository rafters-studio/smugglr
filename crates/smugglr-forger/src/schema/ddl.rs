//! Rendering a [`Schema`] to DDL SQLite accepts.
//!
//! The renderer is deliberately literal: it writes what the model says, in the
//! order the model says it, and never normalizes. A renderer that tidied its
//! input would hide exactly the differences forger exists to catch -- if
//! rendering "fixed" a missing `ON CONFLICT`, a transformation that dropped
//! one would round-trip clean.
//!
//! Every table is emitted before every trigger, because a trigger body may
//! reach into a table declared after the one it fires on.

use super::{
    Column, ColumnConstraint, DefaultValue, ForeignKey, IndexedColumn, Schema, SortOrder, Table,
    TableConstraint, Trigger, TriggerEvent, TriggerTiming,
};

pub fn render(schema: &Schema) -> String {
    let mut out = String::new();
    for table in &schema.tables {
        out.push_str(&render_table(table));
        out.push_str(";\n\n");
    }
    for table in &schema.tables {
        for trigger in &table.triggers {
            out.push_str(&render_trigger(&table.name, trigger));
            out.push_str(";\n\n");
        }
    }
    out
}

pub fn render_table(table: &Table) -> String {
    let mut parts: Vec<String> = table.columns.iter().map(render_column).collect();
    parts.extend(table.constraints.iter().map(render_table_constraint));

    let mut sql = format!(
        "CREATE TABLE {} (\n  {}\n)",
        quote(&table.name),
        parts.join(",\n  ")
    );

    // Table options are a comma-separated suffix outside the parentheses.
    let mut options: Vec<&str> = Vec::new();
    if table.strict {
        options.push("STRICT");
    }
    if table.without_rowid {
        options.push("WITHOUT ROWID");
    }
    if !options.is_empty() {
        sql.push(' ');
        sql.push_str(&options.join(", "));
    }
    sql
}

fn render_column(column: &Column) -> String {
    let mut sql = quote(&column.name);
    // A typeless column is the absence of a type name, not an empty one.
    if let Some(decl) = &column.decl_type {
        sql.push(' ');
        sql.push_str(decl.as_sql());
    }
    for constraint in &column.constraints {
        sql.push(' ');
        sql.push_str(&render_column_constraint(constraint));
    }
    sql
}

fn render_column_constraint(constraint: &ColumnConstraint) -> String {
    match constraint {
        ColumnConstraint::PrimaryKey {
            order,
            autoincrement,
            on_conflict,
        } => {
            let mut sql = String::from("PRIMARY KEY");
            if *order == SortOrder::Desc {
                sql.push_str(" DESC");
            }
            sql.push_str(&conflict_clause(on_conflict));
            // AUTOINCREMENT trails the conflict clause; it is part of the key
            // declaration rather than a constraint of its own.
            if *autoincrement {
                sql.push_str(" AUTOINCREMENT");
            }
            sql
        }
        ColumnConstraint::NotNull(on_conflict) => {
            format!("NOT NULL{}", conflict_clause(on_conflict))
        }
        ColumnConstraint::Unique(on_conflict) => {
            format!("UNIQUE{}", conflict_clause(on_conflict))
        }
        ColumnConstraint::Check(expr) => format!("CHECK ({expr})"),
        ColumnConstraint::Default(value) => format!("DEFAULT {}", render_default(value)),
        ColumnConstraint::Collate(name) => format!("COLLATE {name}"),
        ColumnConstraint::Generated { expr, storage } => format!(
            "GENERATED ALWAYS AS ({expr}) {}",
            match storage {
                super::Generated::Virtual => "VIRTUAL",
                super::Generated::Stored => "STORED",
            }
        ),
    }
}

fn render_default(value: &DefaultValue) -> String {
    match value {
        DefaultValue::Null => "NULL".to_string(),
        DefaultValue::Integer(n) => n.to_string(),
        DefaultValue::Real(n) => {
            // Debug formatting keeps the decimal point on whole floats, so
            // 1.0 does not silently become the integer literal 1.
            format!("{n:?}")
        }
        DefaultValue::Text(s) => quote_literal(s),
        DefaultValue::Blob(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{b:02X}")).collect();
            format!("X'{hex}'")
        }
        // The parentheses are what make this an expression rather than a
        // literal. Rendering them is not cosmetic.
        DefaultValue::Expr(expr) => format!("({expr})"),
    }
}

fn render_table_constraint(constraint: &TableConstraint) -> String {
    match constraint {
        TableConstraint::PrimaryKey {
            columns,
            on_conflict,
        } => format!(
            "PRIMARY KEY ({}){}",
            indexed_columns(columns),
            conflict_clause(on_conflict)
        ),
        TableConstraint::Unique {
            columns,
            on_conflict,
        } => format!(
            "UNIQUE ({}){}",
            indexed_columns(columns),
            conflict_clause(on_conflict)
        ),
        TableConstraint::Check(expr) => format!("CHECK ({expr})"),
        TableConstraint::ForeignKey(fk) => render_foreign_key(fk),
    }
}

fn render_foreign_key(fk: &ForeignKey) -> String {
    let mut sql = format!(
        "FOREIGN KEY ({}) REFERENCES {} ({})",
        name_list(&fk.columns),
        quote(&fk.parent_table),
        name_list(&fk.parent_columns),
    );
    if let Some(action) = &fk.on_delete {
        sql.push_str(&format!(" ON DELETE {}", action.as_sql()));
    }
    if let Some(action) = &fk.on_update {
        sql.push_str(&format!(" ON UPDATE {}", action.as_sql()));
    }
    sql
}

pub fn render_trigger(table: &str, trigger: &Trigger) -> String {
    let timing = match trigger.timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
    };
    let event = match &trigger.event {
        TriggerEvent::Insert => "INSERT".to_string(),
        TriggerEvent::Delete => "DELETE".to_string(),
        TriggerEvent::Update => "UPDATE".to_string(),
        TriggerEvent::UpdateOf(columns) => format!("UPDATE OF {}", name_list(columns)),
    };

    let mut sql = format!(
        "CREATE TRIGGER {} {timing} {event} ON {}\nFOR EACH ROW",
        quote(&trigger.name),
        quote(table),
    );
    if let Some(when) = &trigger.when {
        sql.push_str(&format!("\nWHEN {when}"));
    }
    sql.push_str("\nBEGIN\n");
    for statement in &trigger.body {
        sql.push_str(&format!("  {statement};\n"));
    }
    sql.push_str("END");
    sql
}

/// A comma-separated list of quoted identifiers.
fn name_list(names: &[String]) -> String {
    names
        .iter()
        .map(|name| quote(name))
        .collect::<Vec<_>>()
        .join(", ")
}

fn indexed_columns(columns: &[IndexedColumn]) -> String {
    columns
        .iter()
        .map(|c| match c.order {
            SortOrder::Asc => quote(&c.name),
            SortOrder::Desc => format!("{} DESC", quote(&c.name)),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn conflict_clause(on_conflict: &Option<super::OnConflict>) -> String {
    match on_conflict {
        Some(algorithm) => format!(" ON CONFLICT {}", algorithm.as_sql()),
        None => String::new(),
    }
}

/// Quote an identifier. Doubling an embedded quote is what keeps a column
/// named `a"b` from ending the identifier early.
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a string literal. Single quotes double, same rule, different delimiter.
fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::builder::{schema, table, Attr};
    use crate::schema::{ColumnType, Generated, OnConflict, ReferentialAction, TriggerEvent};

    fn one_table(builder: crate::schema::builder::SchemaBuilder) -> String {
        let schema = builder.build().expect("valid");
        render_table(&schema.tables[0])
    }

    #[test]
    fn an_autoincrement_key_renders_as_the_rowid_alias() {
        let sql = one_table(schema().table(table("t").pk_int("id").autoincrement()));
        assert_eq!(
            sql,
            "CREATE TABLE \"t\" (\n  \"id\" INTEGER PRIMARY KEY AUTOINCREMENT\n)"
        );
    }

    #[test]
    fn a_conflict_algorithm_renders_against_its_own_constraint() {
        let sql = one_table(schema().table(table("t").col(
            "v",
            ColumnType::Text,
            [
                Attr::NotNull,
                Attr::OnConflictReplace,
                Attr::Unique,
                Attr::OnConflictIgnore,
            ],
        )));
        assert!(
            sql.contains("\"v\" TEXT NOT NULL ON CONFLICT REPLACE UNIQUE ON CONFLICT IGNORE"),
            "{sql}"
        );
    }

    #[test]
    fn an_expression_default_keeps_its_parentheses() {
        let sql = one_table(schema().table(table("t").col(
            "v",
            ColumnType::Text,
            [Attr::Default(DefaultValue::expr("CURRENT_TIMESTAMP"))],
        )));
        assert!(sql.contains("DEFAULT (CURRENT_TIMESTAMP)"), "{sql}");
    }

    #[test]
    fn a_text_default_is_quoted_and_a_quote_inside_it_is_doubled() {
        let sql = one_table(schema().table(table("t").col(
            "v",
            ColumnType::Text,
            [Attr::Default(DefaultValue::text("it's"))],
        )));
        assert!(sql.contains("DEFAULT 'it''s'"), "{sql}");
    }

    #[test]
    fn a_whole_real_default_keeps_its_decimal_point() {
        let sql = one_table(schema().table(table("t").col(
            "v",
            ColumnType::Real,
            [Attr::Default(DefaultValue::Real(1.0))],
        )));
        assert!(sql.contains("DEFAULT 1.0"), "{sql}");
    }

    #[test]
    fn a_blob_default_renders_as_a_hex_literal() {
        let sql = one_table(schema().table(table("t").col(
            "v",
            ColumnType::Blob,
            [Attr::Default(DefaultValue::Blob(vec![0x00, 0xde, 0xad]))],
        )));
        assert!(sql.contains("DEFAULT X'00DEAD'"), "{sql}");
    }

    #[test]
    fn a_generated_column_names_its_storage() {
        let sql = one_table(
            schema().table(
                table("t")
                    .col("a", ColumnType::Integer, [])
                    .col("v", ColumnType::Text, [Attr::Virtual("a + 1".into())])
                    .col("s", ColumnType::Text, [Attr::Stored("a + 2".into())]),
            ),
        );
        assert!(
            sql.contains("\"v\" TEXT GENERATED ALWAYS AS (a + 1) VIRTUAL"),
            "{sql}"
        );
        assert!(
            sql.contains("\"s\" TEXT GENERATED ALWAYS AS (a + 2) STORED"),
            "{sql}"
        );
    }

    #[test]
    fn a_typeless_column_gets_no_type_name() {
        let sql = one_table(schema().table(table("t").typeless("v", [Attr::NotNull])));
        assert!(sql.contains("\"v\" NOT NULL"), "{sql}");
    }

    #[test]
    fn a_descending_key_says_so() {
        let sql = one_table(schema().table(table("t").pk_col(
            "id",
            ColumnType::Integer,
            SortOrder::Desc,
        )));
        assert!(sql.contains("\"id\" INTEGER PRIMARY KEY DESC"), "{sql}");
    }

    #[test]
    fn table_options_follow_the_closing_parenthesis() {
        let sql = one_table(schema().table(table("t").pk_text("id").strict().without_rowid()));
        assert!(sql.ends_with(") STRICT, WITHOUT ROWID"), "{sql}");
    }

    #[test]
    fn a_referential_action_renders_after_the_reference() {
        let schema = schema()
            .table(table("parent").pk_int("id"))
            .table(
                table("child")
                    .pk_int("id")
                    .col("a", ColumnType::Integer, [])
                    .fk(["a"], "parent", ["id"])
                    .on_delete(ReferentialAction::Cascade)
                    .on_update(ReferentialAction::SetNull),
            )
            .build()
            .expect("valid");
        let sql = render_table(&schema.tables[1]);
        assert!(
            sql.contains(
                "FOREIGN KEY (\"a\") REFERENCES \"parent\" (\"id\") \
                 ON DELETE CASCADE ON UPDATE SET NULL"
            ),
            "{sql}"
        );
    }

    #[test]
    fn an_embedded_quote_does_not_end_the_identifier_early() {
        let schema = Schema {
            tables: vec![Table {
                name: "we\"ird".into(),
                columns: vec![Column {
                    name: "a\"b".into(),
                    decl_type: Some(ColumnType::Text),
                    constraints: Vec::new(),
                }],
                constraints: Vec::new(),
                without_rowid: false,
                strict: false,
                triggers: Vec::new(),
            }],
        };
        let sql = render_table(&schema.tables[0]);
        assert!(sql.starts_with("CREATE TABLE \"we\"\"ird\""), "{sql}");
        assert!(sql.contains("\"a\"\"b\" TEXT"), "{sql}");
    }

    #[test]
    fn a_trigger_renders_with_its_guard_and_body() {
        let trigger = Trigger {
            name: "bump".into(),
            timing: TriggerTiming::After,
            event: TriggerEvent::UpdateOf(vec!["v".into()]),
            when: Some("new.v <> old.v".into()),
            body: vec!["UPDATE \"t\" SET \"n\" = \"n\" + 1".into()],
        };
        let sql = render_trigger("t", &trigger);
        assert_eq!(
            sql,
            "CREATE TRIGGER \"bump\" AFTER UPDATE OF \"v\" ON \"t\"\n\
             FOR EACH ROW\n\
             WHEN new.v <> old.v\n\
             BEGIN\n  \
             UPDATE \"t\" SET \"n\" = \"n\" + 1;\n\
             END"
        );
    }

    #[test]
    fn triggers_render_after_every_table() {
        let schema = schema()
            .table(table("a").pk_int("id").trigger(Trigger {
                name: "trg".into(),
                timing: TriggerTiming::After,
                event: TriggerEvent::Insert,
                when: None,
                body: vec!["INSERT INTO \"b\" DEFAULT VALUES".into()],
            }))
            .table(table("b").pk_int("id"))
            .build()
            .expect("valid");
        let sql = render(&schema);
        let trigger_at = sql.find("CREATE TRIGGER").expect("trigger rendered");
        let last_table_at = sql.find("CREATE TABLE \"b\"").expect("table rendered");
        assert!(
            trigger_at > last_table_at,
            "a trigger body may reach into a table declared after its own: {sql}"
        );
    }

    #[test]
    fn generated_storage_renders_both_ways() {
        // Guards the match in render_column_constraint against a silent swap.
        assert_eq!(
            render_column_constraint(&ColumnConstraint::Generated {
                expr: "1".into(),
                storage: Generated::Stored,
            }),
            "GENERATED ALWAYS AS (1) STORED"
        );
    }

    #[test]
    fn every_conflict_algorithm_has_a_spelling() {
        for (algorithm, expected) in [
            (OnConflict::Rollback, "ROLLBACK"),
            (OnConflict::Abort, "ABORT"),
            (OnConflict::Fail, "FAIL"),
            (OnConflict::Ignore, "IGNORE"),
            (OnConflict::Replace, "REPLACE"),
        ] {
            assert_eq!(algorithm.as_sql(), expected);
        }
    }
}
