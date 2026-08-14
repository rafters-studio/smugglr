//! The claim this crate stands or falls on: what the builder authors, SQLite
//! accepts.
//!
//! A rendering test that only compares strings proves the renderer agrees with
//! itself. These go through a real database. The first schema carries one
//! instance of every feature the `Trait` enum declares, because those are the
//! shapes a transformation has been observed to lose; the second exists to
//! reach the renderer arms the first does not, since an arm SQLite has never
//! parsed is a rendering nobody has actually checked.

use rusqlite::params;
use smugglr_forger::fixture::{Backing, Fixture, Route};
use smugglr_forger::schema::builder::{schema, table, Attr::*};
use smugglr_forger::schema::{
    ColumnConstraint, ColumnType::*, DefaultValue, IndexedColumn, OnConflict, ReferentialAction,
    Schema, SortOrder, TableConstraint, Trigger, TriggerEvent, TriggerTiming,
};

/// One instance of every declared trait, in one schema.
fn awkward_schema() -> Schema {
    schema()
        .table(
            table("author")
                .pk_int("id")
                .autoincrement()
                .col("email", Text, [NotNull, Unique, OnConflictReplace])
                .col(
                    "created",
                    Text,
                    [Default(DefaultValue::expr("CURRENT_TIMESTAMP"))],
                )
                .typeless("scratch", []),
        )
        .table(
            table("post")
                .pk_int("id")
                .col("author_id", Integer, [NotNull])
                .col("title", Text, [NotNull])
                .col("slug", Text, [Stored("lower(title)".into())])
                .col("shout", Text, [Virtual("upper(title)".into())])
                .col("revision", Integer, [Default(DefaultValue::Integer(0))])
                .fk(["author_id"], "author", ["id"])
                .on_delete(ReferentialAction::Cascade)
                .on_update(ReferentialAction::Restrict)
                .trigger(Trigger {
                    name: "post_revision".into(),
                    timing: TriggerTiming::After,
                    event: TriggerEvent::UpdateOf(vec!["title".into()]),
                    when: Some("new.title <> old.title".into()),
                    body: vec!["UPDATE \"post\" SET \"revision\" = \"revision\" + 1 \
                         WHERE \"id\" = new.\"id\""
                        .into()],
                }),
        )
        .table(
            // A descending key is not the rowid alias, whatever its type says.
            table("event")
                .pk_col("at", Integer, SortOrder::Desc)
                .col("kind", Text, [NotNull]),
        )
        .table(
            // WITHOUT ROWID over a composite key, and STRICT, which forbids
            // both the typeless column and the invented type name above.
            table("membership")
                .col("team", Text, [NotNull])
                .col("person", Text, [NotNull])
                .col("role", Text, [Default(DefaultValue::text("member"))])
                .pk_composite([
                    IndexedColumn::new("team", SortOrder::Asc),
                    IndexedColumn::new("person", SortOrder::Asc),
                ])
                .strict()
                .without_rowid(),
        )
        .build()
        .expect("the awkward schema is a legal one")
}

#[test]
fn sqlite_accepts_a_schema_carrying_every_declared_trait() {
    let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
    fixture
        .bring_to(Route::Schema(&awkward_schema()))
        .expect("SQLite accepts the rendered DDL");

    let objects: Vec<String> = fixture
        .conn()
        .prepare("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name")
        .expect("prepare")
        .query_map([], |row| row.get(0))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows");

    assert_eq!(
        objects,
        vec![
            "author".to_string(),
            "event".into(),
            "membership".into(),
            "post".into(),
            "post_revision".into(),
        ],
        "every table and the trigger reached the database"
    );
}

#[test]
fn the_rendered_columns_are_the_authored_columns() {
    let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
    let target = awkward_schema();
    fixture.bring_to(Route::Schema(&target)).expect("apply");

    for table in &target.tables {
        let found: Vec<String> = fixture
            .conn()
            .prepare("SELECT name FROM pragma_table_xinfo(?1) ORDER BY cid")
            .expect("prepare")
            .query_map(params![table.name], |row| row.get(0))
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows");
        let authored: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(found, authored, "columns of {}", table.name);
    }
}

/// The generated columns are the ones most likely to render as something
/// SQLite tolerates but does not mean, so check they compute.
#[test]
fn a_generated_column_computes_rather_than_stores_what_it_was_given() {
    let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
    fixture
        .bring_to(Route::Schema(&awkward_schema()))
        .expect("apply");
    // The bundled SQLite enforces foreign keys by default, so the parent row
    // is not optional here.
    fixture
        .conn()
        .execute(
            "INSERT INTO \"author\" (\"email\") VALUES ('a@example.com')",
            [],
        )
        .expect("insert author");
    fixture
        .conn()
        .execute(
            "INSERT INTO \"post\" (\"author_id\", \"title\") VALUES (1, 'Hello')",
            [],
        )
        .expect("insert post");

    let (slug, shout): (String, String) = fixture
        .conn()
        .query_row("SELECT \"slug\", \"shout\" FROM \"post\"", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .expect("read back");
    assert_eq!(slug, "hello");
    assert_eq!(shout, "HELLO");
}

/// Everything the trait corpus does not happen to use: conflict algorithms on
/// keys, table-level constraints, the remaining literal defaults, the
/// remaining trigger timings and events, and the type names that are not
/// storage classes.
fn remaining_grammar() -> Schema {
    let mut target = schema()
        .table(
            table("ledger")
                .pk_int("id")
                .autoincrement()
                .col(
                    "note",
                    Text,
                    [Collate("NOCASE".into()), Default(DefaultValue::Null)],
                )
                .col("amount", Numeric, [Default(DefaultValue::Real(1.5))])
                .col("sig", Blob, [Default(DefaultValue::Blob(vec![0x00, 0xff]))])
                .col("qty", Integer, [Default(DefaultValue::Integer(0))])
                .col(
                    "tag",
                    Other("VARCHAR(32)".into()),
                    [Check("length(\"tag\") < 32".into())],
                )
                .trigger(Trigger {
                    name: "ledger_before_delete".into(),
                    timing: TriggerTiming::Before,
                    event: TriggerEvent::Delete,
                    when: None,
                    body: vec!["UPDATE \"counter\" SET \"n\" = \"n\" + 1".into()],
                }),
        )
        .table(
            table("pair")
                .col("a", Text, [NotNull])
                .col("b", Text, [NotNull])
                .col("note", Text, [])
                .pk_composite([
                    IndexedColumn::new("a", SortOrder::Asc),
                    IndexedColumn::new("b", SortOrder::Desc),
                ])
                .constraint(TableConstraint::Unique {
                    columns: vec![IndexedColumn::new("note", SortOrder::Asc)],
                    on_conflict: Some(OnConflict::Replace),
                })
                .constraint(TableConstraint::Check("\"a\" <> \"b\"".into()))
                .trigger(Trigger {
                    name: "pair_after_update".into(),
                    timing: TriggerTiming::After,
                    event: TriggerEvent::Update,
                    when: None,
                    body: vec!["UPDATE \"counter\" SET \"n\" = \"n\" + 1".into()],
                }),
        )
        .table(table("counter").pk_int("id").col("n", Integer, []))
        .build()
        .expect("valid");

    // A conflict algorithm on a key is not reachable through the builder --
    // the pk_* constructors take no attributes, which is what keeps a
    // generated column from becoming one. Reach into the model for it, since
    // the rendering still has to be right.
    target.tables[0].columns[0].constraints = vec![ColumnConstraint::PrimaryKey {
        order: SortOrder::Asc,
        autoincrement: true,
        on_conflict: Some(OnConflict::Rollback),
    }];
    target.tables[1].constraints[0] = TableConstraint::PrimaryKey {
        columns: vec![
            IndexedColumn::new("a", SortOrder::Asc),
            IndexedColumn::new("b", SortOrder::Desc),
        ],
        on_conflict: Some(OnConflict::Ignore),
    };
    target.validate().expect("still a legal schema");
    target
}

#[test]
fn sqlite_accepts_every_arm_the_renderer_can_take() {
    let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
    fixture
        .bring_to(Route::Schema(&remaining_grammar()))
        .expect("SQLite accepts the rendered DDL");

    // The defaults are the arms most likely to render as something that parses
    // and means the wrong thing, so read them back rather than trusting the
    // statement's acceptance.
    fixture
        .conn()
        .execute("INSERT INTO \"ledger\" (\"tag\") VALUES ('x')", [])
        .expect("insert");
    let (note, amount, sig, qty): (Option<String>, f64, Vec<u8>, i64) = fixture
        .conn()
        .query_row(
            "SELECT \"note\", \"amount\", \"sig\", \"qty\" FROM \"ledger\"",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read back");
    assert_eq!(note, None);
    assert_eq!(amount, 1.5);
    assert_eq!(sig, vec![0x00, 0xff]);
    assert_eq!(qty, 0);
}

#[test]
fn a_schema_survives_a_round_trip_through_serde() {
    let target = awkward_schema();
    let json = serde_json::to_string(&target).expect("serialize");
    let back: Schema = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, target);
}
