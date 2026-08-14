//! Standing a fixture up, driving it, and getting rid of it.
//!
//! The teardown tests assert on the filesystem rather than on the absence of a
//! panic. "Nothing blew up" is satisfied by a leaked connection; "the path is
//! gone" is not, and on Windows a leaked connection is exactly what stops the
//! path from going away.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;

use rusqlite::Connection;
use smugglr_forger::error::{BoxError, ForgeError};
use smugglr_forger::fixture::{Backing, Fixture, Route};
use smugglr_forger::schema::builder::{schema, table, Attr::*};
use smugglr_forger::schema::{ColumnType::*, Schema};

fn target() -> Schema {
    schema()
        .table(
            table("account")
                .pk_int("id")
                .col("email", Text, [NotNull, Unique, OnConflictAbort])
                .col("nickname", Text, []),
        )
        .build()
        .expect("valid")
}

fn columns_of(fixture: &Fixture, table: &str) -> Vec<String> {
    fixture
        .conn()
        .prepare("SELECT name FROM pragma_table_xinfo(?1) ORDER BY cid")
        .expect("prepare")
        .query_map([table], |row| row.get(0))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows")
}

#[test]
fn both_backings_hand_out_a_usable_connection() {
    for backing in [Backing::Memory, Backing::File] {
        let mut fixture = Fixture::new(backing).expect("fixture");
        fixture.bring_to(Route::Schema(&target())).expect("apply");
        fixture
            .conn()
            .execute("INSERT INTO \"account\" (\"email\") VALUES ('a@b.c')", [])
            .expect("insert");
        let count: i64 = fixture
            .conn()
            .query_row("SELECT count(*) FROM \"account\"", [], |row| row.get(0))
            .expect("count");
        assert_eq!(count, 1, "{backing:?}");
    }
}

#[test]
fn an_in_memory_fixture_has_no_path_to_leave_behind() {
    let fixture = Fixture::new(Backing::Memory).expect("fixture");
    assert!(fixture.path().is_none());
}

#[test]
fn a_file_fixture_exists_while_it_is_alive_and_not_after() {
    let fixture = Fixture::new(Backing::File).expect("fixture");
    let path = fixture
        .path()
        .expect("file backing has a path")
        .to_path_buf();
    assert!(path.exists(), "the database file is there to be inspected");

    drop(fixture);
    assert!(!path.exists(), "the file went with the fixture");
    assert!(
        !path.parent().expect("temp dir").exists(),
        "and so did the directory holding it"
    );
}

#[test]
fn a_panic_mid_fixture_leaves_nothing_behind() {
    let mut captured = PathBuf::new();

    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut fixture = Fixture::new(Backing::File).expect("fixture");
        captured = fixture.path().expect("file backing").to_path_buf();
        fixture.bring_to(Route::Schema(&target())).expect("apply");
        // The fixture is live and holding an open connection right here.
        panic!("a test failed the way tests fail");
    }));

    assert!(result.is_err(), "the panic was not swallowed");
    assert!(
        !captured.exists(),
        "{} survived the panic",
        captured.display()
    );
    assert!(!captured.parent().expect("temp dir").exists());
}

#[test]
fn close_reports_what_drop_can_only_print() {
    let mut fixture = Fixture::new(Backing::File).expect("fixture");
    let path = fixture.path().expect("file backing").to_path_buf();
    fixture.bring_to(Route::Schema(&target())).expect("apply");
    fixture.close().expect("clean teardown");
    assert!(!path.exists());
}

/// The symmetry #355 is built on: a fixture reaches a state by DDL or by a
/// caller's transformation, through one method, and forger does not know which
/// of the two it just ran.
#[test]
fn every_route_arrives_at_the_same_place() {
    let target = target();

    let mut by_schema = Fixture::new(Backing::Memory).expect("fixture");
    by_schema.bring_to(Route::Schema(&target)).expect("apply");

    let mut by_ddl = Fixture::new(Backing::Memory).expect("fixture");
    by_ddl
        .bring_to(Route::Ddl(&target.to_ddl()))
        .expect("apply");

    // The transformation is opaque to forger: a closure that takes a
    // connection and reports its own error type. Nothing about it is named in
    // the fixture API.
    let start = schema()
        .table(
            table("account")
                .pk_int("id")
                .col("email", Text, [NotNull, Unique, OnConflictAbort]),
        )
        .build()
        .expect("valid");
    let mut migrate = |conn: &mut Connection| -> Result<(), BoxError> {
        let tx = conn.transaction()?;
        tx.execute("ALTER TABLE \"account\" ADD COLUMN \"nickname\" TEXT", [])?;
        tx.commit()?;
        Ok(())
    };

    let mut by_transform = Fixture::new(Backing::Memory).expect("fixture");
    by_transform.bring_to(Route::Schema(&start)).expect("apply");
    by_transform
        .bring_to(Route::Transform(&mut migrate))
        .expect("the transformation ran");

    let expected = vec!["id".to_string(), "email".into(), "nickname".into()];
    assert_eq!(columns_of(&by_schema, "account"), expected);
    assert_eq!(columns_of(&by_ddl, "account"), expected);
    assert_eq!(columns_of(&by_transform, "account"), expected);
}

#[test]
fn a_failing_transformation_is_its_own_signal() {
    let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
    fixture.bring_to(Route::Schema(&target())).expect("apply");

    let mut fails =
        |_: &mut Connection| -> Result<(), BoxError> { Err("the caller could not do it".into()) };
    let error = fixture
        .bring_to(Route::Transform(&mut fails))
        .expect_err("the transformation failed");

    // Not a Sqlite error and not an Invalid one: a differential oracle has to
    // tell "the transformation failed" apart from "the schemas diverged".
    assert!(
        matches!(error, ForgeError::Transform(_)),
        "expected a Transform error, got {error:?}"
    );
    assert!(error.to_string().contains("the caller could not do it"));
}

#[test]
fn a_schema_route_reports_the_broken_rule_rather_than_a_parse_error() {
    // Literal construction, because the builder cannot express this one.
    let mut broken = target();
    broken.tables[0].without_rowid = true;
    broken.tables[0].columns[0].constraints = Vec::new();

    let mut fixture = Fixture::new(Backing::Memory).expect("fixture");
    let error = fixture
        .bring_to(Route::Schema(&broken))
        .expect_err("WITHOUT ROWID with no key");
    assert!(
        matches!(error, ForgeError::Invalid(_)),
        "expected a validation error, got {error:?}"
    );
}
