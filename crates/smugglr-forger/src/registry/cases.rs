//! The eight cases: one schema, one seed and one probe per [`Trait`].
//!
//! Each case is a single function, and its three parts sit next to each other
//! on purpose -- the seed's values are the probe's expectations, and splitting
//! them across files is how the two drift apart. Where a probe expects a
//! specific number, that number and the expression or seed value it comes from
//! are declared together as constants, because a probe that recomputed its own
//! expectation from the schema would be a second, worse implementation of
//! SQLite.
//!
//! A probe finds the construct it exists for by reading the schema it is handed
//! -- the generated column, the key declared `DESC`, the child table whose
//! foreign key says `CASCADE`. The scaffolding around it (`id`, `label`, the
//! audit table) is this module's own and is named directly. The line between
//! the two is: whatever the trait is *about* is resolved, whatever is there to
//! hold the trait up is not.

use rusqlite::Connection;

use crate::error::ProbeError;
use crate::schema::builder::{schema, table, Attr};
use crate::schema::ddl::quote;
use crate::schema::{
    Column, ColumnConstraint, ColumnType::Integer, ColumnType::Text, DefaultValue, ForeignKey,
    Generated, OnConflict, ReferentialAction, Schema, SortOrder, Table, TableConstraint, Trait,
    Trigger, TriggerEvent, TriggerTiming,
};

use super::{count, one_column, text, TraitCase};

/// Every case seeds through this column so a probe can say which row it means
/// without depending on a rowid.
const KEY: &str = "id";
/// A plain column that carries no trait at all. It is here so that every case
/// leaves at least one ordinary value on the far side of a transformation --
/// FR-FORGER-004's surviving-column rule -- and so a rebuild that drops a
/// column has something to drop that is not the construct under test.
const LABEL: &str = "label";

// ---------------------------------------------------------------------------
// ForeignKeyWithAction
// ---------------------------------------------------------------------------

/// The parent row `ON DELETE CASCADE` is expected to take its children with.
const DOOMED: i64 = 1;
/// The parent row an `ON DELETE RESTRICT` child is expected to pin in place.
const PROTECTED: i64 = 2;
/// The parent key `ON UPDATE CASCADE` is expected to let move.
const RENUMBERED: i64 = 30;
/// Where that key is expected to move to, and where its child should follow.
const RENUMBERED_TO: i64 = 31;
/// The parent key `ON UPDATE SET NULL` is expected to let move, cutting its
/// child loose rather than carrying it.
const NULLED: i64 = 40;
/// Where that key moves to. Nothing should follow it there.
const NULLED_TO: i64 = 41;
/// The parent key `ON UPDATE SET DEFAULT` is expected to let move.
const DEFAULTED: i64 = 50;
/// Where that key moves to. Its child should not follow it there either.
const DEFAULTED_TO: i64 = 51;
/// The row an `ON UPDATE SET DEFAULT` child falls back to.
///
/// It has to be a parent row that *exists*: the action writes the column's
/// declared default into the child, and a default naming a row that is not
/// there turns the update into a foreign-key violation rather than a fallback.
/// So this is both the column's `DEFAULT` and a seeded key, and the two cannot
/// drift apart because the schema and the seed read the same constant.
const FALLBACK: i64 = 59;
/// The parent row `ON DELETE SET NULL` is expected to let go, cutting its child
/// loose rather than taking it along.
const DELETED_NULLING: i64 = 60;
/// The parent row `ON DELETE SET DEFAULT` is expected to let go.
const DELETED_DEFAULTING: i64 = 70;
/// The row an `ON DELETE SET DEFAULT` child falls back to, on the same terms as
/// [`FALLBACK`]: it has to exist, or the delete becomes a violation rather than
/// a fallback.
const DELETE_FALLBACK: i64 = 79;

pub(super) fn foreign_key_with_action() -> TraitCase {
    let schema = schema()
        .table(table("keeper").pk_int(KEY).col(LABEL, Text, []))
        .table(
            table("cascade_child")
                .pk_int(KEY)
                .col("keeper_id", Integer, [])
                .col(LABEL, Text, [])
                .fk(["keeper_id"], "keeper", [KEY])
                .on_delete(ReferentialAction::Cascade),
        )
        .table(
            table("restrict_child")
                .pk_int(KEY)
                .col("keeper_id", Integer, [])
                .col(LABEL, Text, [])
                .fk(["keeper_id"], "keeper", [KEY])
                .on_delete(ReferentialAction::Restrict),
        )
        // The ON UPDATE key gets its own parent rather than sharing `keeper`.
        // The ON DELETE arm below removes `keeper.id = DOOMED` and tries to
        // remove `keeper.id = PROTECTED`, and a third child hanging off either
        // row would make the update arm depend on which arm ran first -- or pin
        // a row the RESTRICT assertion expects to be pinned by something else.
        // Separate parents make the two arms independent by construction rather
        // than by ordering (#374).
        .table(table("updating_keeper").pk_int(KEY).col(LABEL, Text, []))
        .table(
            table("updating_child")
                .pk_int(KEY)
                .col("keeper_id", Integer, [])
                .col(LABEL, Text, [])
                .fk(["keeper_id"], "updating_keeper", [KEY])
                .on_update(ReferentialAction::Cascade),
        )
        // SET NULL and SET DEFAULT get their own parents for the same reason
        // CASCADE did: every arm of the probe moves or deletes a key, and two
        // arms sharing a parent would depend on which ran first (#384).
        .table(table("nulling_keeper").pk_int(KEY).col(LABEL, Text, []))
        .table(
            table("nulling_child")
                .pk_int(KEY)
                .col("keeper_id", Integer, [])
                .col(LABEL, Text, [])
                .fk(["keeper_id"], "nulling_keeper", [KEY])
                .on_update(ReferentialAction::SetNull),
        )
        // The delete side of the same two actions (#392). Own parents again,
        // for the third and fourth time and the same reason: every arm of this
        // probe removes or moves a key.
        .table(
            table("delete_nulling_keeper")
                .pk_int(KEY)
                .col(LABEL, Text, []),
        )
        .table(
            table("delete_nulling_child")
                .pk_int(KEY)
                .col("keeper_id", Integer, [])
                .col(LABEL, Text, [])
                .fk(["keeper_id"], "delete_nulling_keeper", [KEY])
                .on_delete(ReferentialAction::SetNull),
        )
        .table(
            table("delete_defaulting_keeper")
                .pk_int(KEY)
                .col(LABEL, Text, []),
        )
        .table(
            table("delete_defaulting_child")
                .pk_int(KEY)
                .col(
                    "keeper_id",
                    Integer,
                    [Attr::Default(DefaultValue::Integer(DELETE_FALLBACK))],
                )
                .col(LABEL, Text, [])
                .fk(["keeper_id"], "delete_defaulting_keeper", [KEY])
                .on_delete(ReferentialAction::SetDefault),
        )
        .table(table("defaulting_keeper").pk_int(KEY).col(LABEL, Text, []))
        .table(
            table("defaulting_child")
                .pk_int(KEY)
                .col(
                    "keeper_id",
                    Integer,
                    [Attr::Default(DefaultValue::Integer(FALLBACK))],
                )
                .col(LABEL, Text, [])
                .fk(["keeper_id"], "defaulting_keeper", [KEY])
                .on_update(ReferentialAction::SetDefault),
        )
        .build()
        .expect("the ForeignKeyWithAction case schema is valid");

    TraitCase {
        kind: Trait::ForeignKeyWithAction,
        schema,
        seed: |conn| {
            conn.execute_batch(&format!(
                "INSERT INTO \"keeper\" (\"{KEY}\", \"{LABEL}\") \
                   VALUES ({DOOMED}, 'its children go with it'), \
                          ({PROTECTED}, 'its child pins it in place');
                 INSERT INTO \"cascade_child\" (\"{KEY}\", \"keeper_id\", \"{LABEL}\") \
                   VALUES (10, {DOOMED}, 'first'), (11, {DOOMED}, 'second');
                 INSERT INTO \"restrict_child\" (\"{KEY}\", \"keeper_id\", \"{LABEL}\") \
                   VALUES (20, {PROTECTED}, 'only');
                 INSERT INTO \"updating_keeper\" (\"{KEY}\", \"{LABEL}\") \
                   VALUES ({RENUMBERED}, 'its key moves and its child follows');
                 INSERT INTO \"updating_child\" (\"{KEY}\", \"keeper_id\", \"{LABEL}\") \
                   VALUES (400, {RENUMBERED}, 'follows');
                 INSERT INTO \"nulling_keeper\" (\"{KEY}\", \"{LABEL}\") \
                   VALUES ({NULLED}, 'its key moves and its child is cut loose');
                 INSERT INTO \"nulling_child\" (\"{KEY}\", \"keeper_id\", \"{LABEL}\") \
                   VALUES (410, {NULLED}, 'nulled');
                 INSERT INTO \"defaulting_keeper\" (\"{KEY}\", \"{LABEL}\") \
                   VALUES ({DEFAULTED}, 'its key moves and its child falls back'), \
                          ({FALLBACK}, 'the row the child falls back to');
                 INSERT INTO \"defaulting_child\" (\"{KEY}\", \"keeper_id\", \"{LABEL}\") \
                   VALUES (420, {DEFAULTED}, 'defaulted');
                 INSERT INTO \"delete_nulling_keeper\" (\"{KEY}\", \"{LABEL}\") \
                   VALUES ({DELETED_NULLING}, 'it goes and its child is cut loose');
                 INSERT INTO \"delete_nulling_child\" (\"{KEY}\", \"keeper_id\", \"{LABEL}\") \
                   VALUES (430, {DELETED_NULLING}, 'nulled by delete');
                 INSERT INTO \"delete_defaulting_keeper\" (\"{KEY}\", \"{LABEL}\") \
                   VALUES ({DELETED_DEFAULTING}, 'it goes and its child falls back'), \
                          ({DELETE_FALLBACK}, 'the row the child falls back to');
                 INSERT INTO \"delete_defaulting_child\" (\"{KEY}\", \"keeper_id\", \"{LABEL}\") \
                   VALUES (440, {DELETED_DEFAULTING}, 'defaulted by delete');"
            ))?;
            Ok(())
        },
        probe: probe_foreign_key_with_action,
    }
}

fn probe_foreign_key_with_action(schema: &Schema, conn: &Connection) -> Result<(), ProbeError> {
    require_foreign_keys_enforced(conn)?;

    let cascading = children_with(schema, ReferentialAction::Cascade);
    let restricting = children_with(schema, ReferentialAction::Restrict);
    let updating = children_updating_with(schema, ReferentialAction::Cascade);
    if cascading.is_empty() || restricting.is_empty() || updating.is_empty() {
        return Err(ProbeError::Failed(format!(
            "the schema handed to this probe declares {} ON DELETE CASCADE, {} ON DELETE RESTRICT \
             and {} ON UPDATE CASCADE foreign keys, and the probe needs one of each to have \
             anything to assert",
            cascading.len(),
            restricting.len(),
            updating.len()
        )));
    }

    // The parent is whatever the cascading key points at, so the probe deletes
    // the row the schema says the cascade hangs off.
    let (parent, parent_key) = parent_of(cascading[0].1)?;

    // A cascade with nothing to cascade over, or a restriction with nothing
    // pinning it, is green on an empty table.
    for (children, referenced) in [(&cascading, DOOMED), (&restricting, PROTECTED)] {
        for (child, fk) in children.iter() {
            let child_column = child_column_of(fk)?;
            let rows = count(
                conn,
                &format!(
                    "SELECT count(*) FROM {} WHERE {} = {referenced}",
                    quote(&child.name),
                    quote(child_column)
                ),
            )?;
            if rows == 0 {
                return Err(ProbeError::Unseeded(format!(
                    "{} has no row referencing {parent}.{parent_key} = {referenced}, so deleting \
                     that parent would prove nothing either way",
                    child.name
                )));
            }
        }
    }

    // CASCADE: the delete goes through and the children go with it.
    let doomed_gone = conn.execute(
        &format!(
            "DELETE FROM {} WHERE {} = {DOOMED}",
            quote(parent),
            quote(parent_key)
        ),
        [],
    );
    if let Err(error) = doomed_gone {
        return Err(ProbeError::Failed(format!(
            "deleting {parent}.{parent_key} = {DOOMED} was refused ({error}), and a child \
             declared ON DELETE CASCADE does not refuse it -- the action was reconstructed as \
             something else, or dropped, which leaves NO ACTION"
        )));
    }
    for (child, fk) in &cascading {
        let child_column = child_column_of(fk)?;
        let survivors = count(
            conn,
            &format!(
                "SELECT count(*) FROM {} WHERE {} = {DOOMED}",
                quote(&child.name),
                quote(child_column)
            ),
        )?;
        if survivors != 0 {
            return Err(ProbeError::Failed(format!(
                "{survivors} row(s) of {} still reference {parent}.{parent_key} = {DOOMED} after \
                 that parent was deleted; ON DELETE CASCADE did not cascade",
                child.name
            )));
        }
    }

    // RESTRICT: the end state is the assertion, not which statement raised.
    // Under deferred enforcement the refusal arrives at COMMIT rather than at
    // the DELETE, and the probe should be reporting on the referential action
    // rather than on the transaction shape it happens to be running in.
    let attempt = conn.execute(
        &format!(
            "DELETE FROM {} WHERE {} = {PROTECTED}",
            quote(parent),
            quote(parent_key)
        ),
        [],
    );
    let parent_left = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE {} = {PROTECTED}",
            quote(parent),
            quote(parent_key)
        ),
    )?;
    if parent_left != 1 {
        return Err(ProbeError::Failed(format!(
            "{parent}.{parent_key} = {PROTECTED} was deleted even though a child declared ON \
             DELETE RESTRICT references it (the DELETE {}); the restriction did not restrict",
            match &attempt {
                Ok(rows) => format!("reported {rows} row(s) changed"),
                Err(error) => format!("reported {error}, and the row went anyway"),
            }
        )));
    }
    for (child, fk) in &restricting {
        let child_column = child_column_of(fk)?;
        let left = count(
            conn,
            &format!(
                "SELECT count(*) FROM {} WHERE {} = {PROTECTED}",
                quote(&child.name),
                quote(child_column)
            ),
        )?;
        if left != 1 {
            return Err(ProbeError::Failed(format!(
                "{} lost the row referencing {parent}.{parent_key} = {PROTECTED}; a RESTRICT \
                 child is neither cascaded nor nulled, it refuses",
                child.name
            )));
        }
    }

    // ON UPDATE CASCADE, on its own parent, so nothing above has touched the
    // rows this arm reads. The delete arm resolves its parent from the ON
    // DELETE key and this one from the ON UPDATE key, and the case declares
    // them on different tables (#374).
    let (moving, moving_key) = parent_of(updating[0].1)?;
    for (child, fk) in &updating {
        let child_column = child_column_of(fk)?;
        let rows = count(
            conn,
            &format!(
                "SELECT count(*) FROM {} WHERE {} = {RENUMBERED}",
                quote(&child.name),
                quote(child_column)
            ),
        )?;
        if rows == 0 {
            return Err(ProbeError::Unseeded(format!(
                "{} has no row referencing {moving}.{moving_key} = {RENUMBERED}, so moving that \
                 key would prove nothing either way",
                child.name
            )));
        }
    }

    // Both halves are load-bearing and they fail differently. With the action
    // dropped the parent key cannot move at all -- the UPDATE is refused by the
    // key it still has -- so asserting only that the child followed would report
    // a cascade failure for an update that never happened.
    let moved = conn.execute(
        &format!(
            "UPDATE {} SET {} = {RENUMBERED_TO} WHERE {} = {RENUMBERED}",
            quote(moving),
            quote(moving_key),
            quote(moving_key)
        ),
        [],
    );
    if let Err(error) = moved {
        return Err(ProbeError::Failed(format!(
            "moving {moving}.{moving_key} from {RENUMBERED} to {RENUMBERED_TO} was refused \
             ({error}), and a child declared ON UPDATE CASCADE does not refuse it -- the action \
             was reconstructed as something else, or dropped, which leaves NO ACTION"
        )));
    }
    for (child, fk) in &updating {
        let child_column = child_column_of(fk)?;
        let followed = count(
            conn,
            &format!(
                "SELECT count(*) FROM {} WHERE {} = {RENUMBERED_TO}",
                quote(&child.name),
                quote(child_column)
            ),
        )?;
        if followed != 1 {
            return Err(ProbeError::Failed(format!(
                "{followed} row(s) of {} reference {moving}.{moving_key} = {RENUMBERED_TO} after \
                 that key moved from {RENUMBERED}; ON UPDATE CASCADE did not carry the child over",
                child.name
            )));
        }
    }

    // ON UPDATE SET NULL and SET DEFAULT (#384). Each on its own parent again,
    // so the three update arms are independent of the order they run in.
    //
    // These two are what makes the difference between covering the CASCADE
    // action and covering the clause: they come off the same pragma column as
    // CASCADE and are lost by the same mechanism, so a rebuild that dropped
    // them was silent until there was something reading them.
    moved_key_leaves(
        conn,
        schema,
        ReferentialAction::SetNull,
        NULLED,
        NULLED_TO,
        Landing::Null,
    )?;
    moved_key_leaves(
        conn,
        schema,
        ReferentialAction::SetDefault,
        DEFAULTED,
        DEFAULTED_TO,
        Landing::Value(FALLBACK),
    )?;

    // The same two actions on the delete side (#392). Separate from the update
    // arms rather than folded in with a flag: one removes a parent and the
    // other moves a key, and the shared part is only "assert where the child
    // landed", which `Landing` already carries.
    deleted_parent_leaves(
        conn,
        schema,
        ReferentialAction::SetNull,
        DELETED_NULLING,
        Landing::Null,
    )?;
    deleted_parent_leaves(
        conn,
        schema,
        ReferentialAction::SetDefault,
        DELETED_DEFAULTING,
        Landing::Value(DELETE_FALLBACK),
    )?;

    Ok(())
}

/// Delete a parent row declared with `action`, and assert where its children
/// landed.
///
/// The delete-side twin of [`moved_key_leaves`]. They are two functions rather
/// than one with a flag because the statement differs and so does what "the
/// parent is gone" means -- one asserts the row was removed, the other that the
/// key moved. What they genuinely share is [`Landing`], and that is shared.
fn deleted_parent_leaves(
    conn: &Connection,
    schema: &Schema,
    action: ReferentialAction,
    parent_row: i64,
    landing: Landing,
) -> Result<(), ProbeError> {
    let children = children_with(schema, action);
    if children.is_empty() {
        return Err(ProbeError::Failed(format!(
            "the schema handed to this probe declares no ON DELETE {} foreign key, and the probe \
             needs one to have anything to assert",
            action.as_sql()
        )));
    }
    let (parent, parent_key) = parent_of(children[0].1)?;

    for (child, fk) in &children {
        let child_column = child_column_of(fk)?;
        let rows = count(
            conn,
            &format!(
                "SELECT count(*) FROM {} WHERE {} = {parent_row}",
                quote(&child.name),
                quote(child_column)
            ),
        )?;
        if rows == 0 {
            return Err(ProbeError::Unseeded(format!(
                "{} has no row referencing {parent}.{parent_key} = {parent_row}, so deleting that \
                 parent would prove nothing either way",
                child.name
            )));
        }
    }

    // The delete has to be permitted before the landing means anything: with
    // the action dropped the parent cannot go at all, and asserting only where
    // the child ended up would report the wrong cause.
    let gone = conn.execute(
        &format!(
            "DELETE FROM {} WHERE {} = {parent_row}",
            quote(parent),
            quote(parent_key)
        ),
        [],
    );
    if let Err(error) = gone {
        return Err(ProbeError::Failed(format!(
            "deleting {parent}.{parent_key} = {parent_row} was refused ({error}), and a child \
             declared ON DELETE {} does not refuse it -- the action was reconstructed as something \
             else, or dropped, which leaves NO ACTION",
            action.as_sql()
        )));
    }

    for (child, fk) in &children {
        let child_column = child_column_of(fk)?;
        let landed = count(
            conn,
            &format!(
                "SELECT count(*) FROM {} WHERE {}",
                quote(&child.name),
                landing.predicate(&quote(child_column))
            ),
        )?;
        if landed != 1 {
            return Err(ProbeError::Failed(format!(
                "{landed} row(s) of {} hold {} after {parent}.{parent_key} = {parent_row} was \
                 deleted; ON DELETE {} puts the child there and this one is somewhere else",
                child.name,
                landing.describe(),
                action.as_sql()
            )));
        }
    }

    Ok(())
}

/// Where a child is expected to end up when its parent's key moves out from
/// under it.
#[derive(Debug, Clone, Copy)]
enum Landing {
    /// `SET NULL`: the reference is cut rather than redirected.
    Null,
    /// `SET DEFAULT`: the reference falls back to the column's declared value.
    Value(i64),
}

impl Landing {
    /// The `WHERE` predicate that finds a child which landed correctly.
    ///
    /// Spelled as SQL rather than compared in Rust because `NULL` is not a
    /// value that `=` matches -- reading the column back and comparing it to
    /// `Some(x)` would make the `SET NULL` arm pass on a column that had been
    /// left alone if the seed had happened to be NULL.
    fn predicate(self, column: &str) -> String {
        match self {
            Landing::Null => format!("{column} IS NULL"),
            Landing::Value(value) => format!("{column} = {value}"),
        }
    }

    fn describe(self) -> String {
        match self {
            Landing::Null => "NULL".to_string(),
            Landing::Value(value) => value.to_string(),
        }
    }
}

/// Move a parent key declared with `action`, and assert where its children
/// landed.
///
/// Shared by the `SET NULL` and `SET DEFAULT` arms because they differ only in
/// where the child is expected to end up. `CASCADE` is deliberately not routed
/// through here: it asserts the child *followed the key*, which is a different
/// shape from landing on a fixed value, and folding all three together would
/// need a parameter that names the new key -- a helper longer than the two
/// arms it replaced.
fn moved_key_leaves(
    conn: &Connection,
    schema: &Schema,
    action: ReferentialAction,
    from: i64,
    to: i64,
    landing: Landing,
) -> Result<(), ProbeError> {
    let children = children_updating_with(schema, action);
    if children.is_empty() {
        return Err(ProbeError::Failed(format!(
            "the schema handed to this probe declares no ON UPDATE {} foreign key, and the probe \
             needs one to have anything to assert",
            action.as_sql()
        )));
    }
    let (parent, parent_key) = parent_of(children[0].1)?;

    for (child, fk) in &children {
        let child_column = child_column_of(fk)?;
        let rows = count(
            conn,
            &format!(
                "SELECT count(*) FROM {} WHERE {} = {from}",
                quote(&child.name),
                quote(child_column)
            ),
        )?;
        if rows == 0 {
            return Err(ProbeError::Unseeded(format!(
                "{} has no row referencing {parent}.{parent_key} = {from}, so moving that key \
                 would prove nothing either way",
                child.name
            )));
        }
    }

    // The move has to be permitted before where the child landed means
    // anything: with the action dropped the key cannot move at all, and
    // asserting only the landing would report the wrong cause.
    let moved = conn.execute(
        &format!(
            "UPDATE {} SET {} = {to} WHERE {} = {from}",
            quote(parent),
            quote(parent_key),
            quote(parent_key)
        ),
        [],
    );
    if let Err(error) = moved {
        return Err(ProbeError::Failed(format!(
            "moving {parent}.{parent_key} from {from} to {to} was refused ({error}), and a child \
             declared ON UPDATE {} does not refuse it -- the action was reconstructed as \
             something else, or dropped, which leaves NO ACTION",
            action.as_sql()
        )));
    }

    for (child, fk) in &children {
        let child_column = child_column_of(fk)?;
        let landed = count(
            conn,
            &format!(
                "SELECT count(*) FROM {} WHERE {}",
                quote(&child.name),
                landing.predicate(&quote(child_column))
            ),
        )?;
        if landed != 1 {
            return Err(ProbeError::Failed(format!(
                "{landed} row(s) of {} hold {} after {parent}.{parent_key} moved from {from} to \
                 {to}; ON UPDATE {} puts the child there and this one is somewhere else",
                child.name,
                landing.describe(),
                action.as_sql()
            )));
        }
    }

    Ok(())
}

/// Foreign keys in the schema whose `ON DELETE` is this action, with the table
/// that declares them.
fn children_with(schema: &Schema, action: ReferentialAction) -> Vec<(&Table, &ForeignKey)> {
    children_where(schema, move |fk| fk.on_delete == Some(action))
}

/// Foreign keys in the schema whose `ON UPDATE` is this action, with the table
/// that declares them.
fn children_updating_with(
    schema: &Schema,
    action: ReferentialAction,
) -> Vec<(&Table, &ForeignKey)> {
    children_where(schema, move |fk| fk.on_update == Some(action))
}

/// The shared walk behind [`children_with`] and [`children_updating_with`].
///
/// Both arms resolve their targets by reading the schema rather than naming
/// tables, so a case that gains or renames a key is picked up without editing
/// the probe -- and so a schema handed in with the action missing yields an
/// empty set the probe can refuse on, instead of silently probing nothing.
fn children_where<'s>(
    schema: &'s Schema,
    matches: impl Fn(&ForeignKey) -> bool + Copy + 's,
) -> Vec<(&'s Table, &'s ForeignKey)> {
    schema
        .tables
        .iter()
        .flat_map(|table| {
            table
                .constraints
                .iter()
                .filter_map(move |constraint| match constraint {
                    TableConstraint::ForeignKey(fk) if matches(fk) => Some((table, fk)),
                    _ => None,
                })
        })
        .collect()
}

/// The parent table and column a single-column foreign key points at.
fn parent_of(fk: &ForeignKey) -> Result<(&str, &str), ProbeError> {
    let column = fk.parent_columns.first().ok_or_else(|| {
        ProbeError::Failed(format!(
            "the foreign key on {} names no parent column",
            fk.parent_table
        ))
    })?;
    Ok((fk.parent_table.as_str(), column.as_str()))
}

/// The child column a single-column foreign key is declared on.
fn child_column_of(fk: &ForeignKey) -> Result<&str, ProbeError> {
    fk.columns.first().map(String::as_str).ok_or_else(|| {
        ProbeError::Failed(format!(
            "the foreign key on {} names no child column",
            fk.parent_table
        ))
    })
}

/// Refuse to draw a conclusion from a `DELETE` nobody was enforcing keys for.
///
/// Children surviving a deleted parent because enforcement is off looks
/// identical, at the row counts, to a `CASCADE` that came back without its
/// action. Reading the pragma is a precondition on the connection rather than
/// an assertion about the schema -- the assertions below it are all behavioural
/// -- and it is read in autocommit because `PRAGMA foreign_keys` is a silent
/// no-op inside an open transaction.
fn require_foreign_keys_enforced(conn: &Connection) -> Result<(), ProbeError> {
    if !conn.is_autocommit() {
        return Err(ProbeError::Unseeded(
            "this probe reads PRAGMA foreign_keys, which does not reflect what will apply while a \
             transaction is open"
                .to_string(),
        ));
    }
    let enforced: i64 = conn.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
    if enforced == 0 {
        return Err(ProbeError::Unseeded(
            "foreign key enforcement is off on this connection, so a deleted parent leaves its \
             children behind whatever the referential action says"
                .to_string(),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// GeneratedVirtual
// ---------------------------------------------------------------------------

/// The seeded input and the value the case's expression makes of it. The two
/// constants and the expression in the schema below are one unit.
const VIRTUAL_BASE: i64 = 21;
const VIRTUAL_DOUBLED: i64 = 42;
/// Where the probe moves the base to, and what the expression must then make
/// of it. Same unit as the pair above, and needed for the same reason the
/// stored twin needs its own: reading the column once cannot tell a generated
/// column from an ordinary one holding the number a rebuild copied into it.
const VIRTUAL_MOVED_BASE: i64 = 9;
const VIRTUAL_MOVED_DOUBLED: i64 = 18;

pub(super) fn generated_virtual() -> TraitCase {
    let schema = schema()
        .table(
            table("virtual_generated")
                .pk_int(KEY)
                .col("base", Integer, [])
                .col("doubled", Integer, [Attr::Virtual("\"base\" * 2".into())])
                .col(LABEL, Text, []),
        )
        .build()
        .expect("the GeneratedVirtual case schema is valid");

    TraitCase {
        kind: Trait::GeneratedVirtual,
        schema,
        seed: |conn| {
            conn.execute_batch(&format!(
                "INSERT INTO \"virtual_generated\" (\"{KEY}\", \"base\", \"{LABEL}\") \
                 VALUES (1, {VIRTUAL_BASE}, 'seeded')"
            ))?;
            Ok(())
        },
        probe: |schema, conn| {
            let (table, column) =
                one_column(schema, "GENERATED ALWAYS AS ... VIRTUAL", |column| {
                    matches!(column.generated(), Some((_, Generated::Virtual)))
                })?;
            require_base_row(conn, &table.name, VIRTUAL_BASE)?;

            let computed: Option<i64> = conn.query_row(
                &format!(
                    "SELECT {} FROM {} WHERE \"{KEY}\" = 1",
                    quote(&column.name),
                    quote(&table.name)
                ),
                [],
                |row| row.get(0),
            )?;
            if computed != Some(VIRTUAL_DOUBLED) {
                return Err(ProbeError::Failed(format!(
                    "{}.{} reads {computed:?} for a base of {VIRTUAL_BASE}, not {VIRTUAL_DOUBLED}; \
                     a virtual generated column that came back as an ordinary one and was never \
                     written to stores nothing and reads NULL",
                    table.name, column.name
                )));
            }

            // The assertion the stored twin has always had and this one lacked,
            // and the gap was not cosmetic: an audit drove a rebuild that
            // copies the column by name -- `INSERT INTO new SELECT "doubled"
            // ... FROM old` -- through the real oracle and every trait held.
            // Selecting a VIRTUAL column COMPUTES it, so the copy materialises
            // 42 into an ordinary column, and a probe that reads the value once
            // sees exactly what it expects while the generation is gone.
            //
            // Reading once cannot separate a generated column from an ordinary
            // one holding the number a rebuild put there. Moving the input and
            // asking whether the column followed is the only thing that can.
            conn.execute(
                &format!(
                    "UPDATE {} SET \"base\" = {VIRTUAL_MOVED_BASE} WHERE \"{KEY}\" = 1",
                    quote(&table.name)
                ),
                [],
            )?;
            let moved: Option<i64> = conn.query_row(
                &format!(
                    "SELECT {} FROM {} WHERE \"{KEY}\" = 1",
                    quote(&column.name),
                    quote(&table.name)
                ),
                [],
                |row| row.get(0),
            )?;
            if moved != Some(VIRTUAL_MOVED_DOUBLED) {
                return Err(ProbeError::Failed(format!(
                    "{}.{} still reads {moved:?} after its base moved to {VIRTUAL_MOVED_BASE}, not \
                     {VIRTUAL_MOVED_DOUBLED}; the value was copied but the computation was not, \
                     which is what a rebuild that selected the column by name leaves behind",
                    table.name, column.name
                )));
            }
            Ok(())
        },
    }
}

// ---------------------------------------------------------------------------
// GeneratedStored
// ---------------------------------------------------------------------------

/// Seeded input, its computed value, and the same pair after the probe moves
/// the base. One unit with the expression in the schema below.
const STORED_BASE: i64 = 5;
const STORED_TRIPLED: i64 = 15;
const STORED_MOVED_BASE: i64 = 7;
const STORED_MOVED_TRIPLED: i64 = 21;

pub(super) fn generated_stored() -> TraitCase {
    let schema = schema()
        .table(
            table("stored_generated")
                .pk_int(KEY)
                .col("base", Integer, [])
                .col("tripled", Integer, [Attr::Stored("\"base\" * 3".into())])
                .col(LABEL, Text, []),
        )
        .build()
        .expect("the GeneratedStored case schema is valid");

    TraitCase {
        kind: Trait::GeneratedStored,
        schema,
        seed: |conn| {
            conn.execute_batch(&format!(
                "INSERT INTO \"stored_generated\" (\"{KEY}\", \"base\", \"{LABEL}\") \
                 VALUES (1, {STORED_BASE}, 'seeded')"
            ))?;
            Ok(())
        },
        probe: |schema, conn| {
            let (table, column) = one_column(schema, "GENERATED ALWAYS AS ... STORED", |column| {
                matches!(column.generated(), Some((_, Generated::Stored)))
            })?;
            require_base_row(conn, &table.name, STORED_BASE)?;

            let read = |conn: &Connection| -> Result<Option<i64>, ProbeError> {
                Ok(conn.query_row(
                    &format!(
                        "SELECT {} FROM {} WHERE \"{KEY}\" = 1",
                        quote(&column.name),
                        quote(&table.name)
                    ),
                    [],
                    |row| row.get(0),
                )?)
            };

            let computed = read(conn)?;
            if computed != Some(STORED_TRIPLED) {
                return Err(ProbeError::Failed(format!(
                    "{}.{} reads {computed:?} for a base of {STORED_BASE}, not {STORED_TRIPLED}",
                    table.name, column.name
                )));
            }

            // The assertion that separates a stored generated column from an
            // ordinary one holding the same number. A row dump cannot tell them
            // apart, and neither can any comparison of the stored values -- what
            // is lost when the column comes back ordinary is the computation, so
            // the probe moves the input and asks whether the column followed.
            conn.execute(
                &format!(
                    "UPDATE {} SET \"base\" = {STORED_MOVED_BASE} WHERE \"{KEY}\" = 1",
                    quote(&table.name)
                ),
                [],
            )?;
            let moved = read(conn)?;
            if moved != Some(STORED_MOVED_TRIPLED) {
                return Err(ProbeError::Failed(format!(
                    "{}.{} still reads {moved:?} after its base moved to {STORED_MOVED_BASE}, not \
                     {STORED_MOVED_TRIPLED}; the value was copied but the computation was not, \
                     which is what an ordinary column holding the old result looks like",
                    table.name, column.name
                )));
            }
            Ok(())
        },
    }
}

/// Both generated cases seed one row and key their expectations to its base.
fn require_base_row(conn: &Connection, table: &str, base: i64) -> Result<(), ProbeError> {
    let seeded = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE \"{KEY}\" = 1 AND \"base\" = {base}",
            quote(table)
        ),
    )?;
    if seeded != 1 {
        return Err(ProbeError::Unseeded(format!(
            "{table} has no row 1 with a base of {base}, so the generated column has no input to \
             have computed anything from"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ColumnOnConflict
// ---------------------------------------------------------------------------

/// The key the REPLACE case's seeded row already occupies.
const OCCUPIED_KEY: &str = "occupied";

pub(super) fn column_on_conflict() -> TraitCase {
    let schema = schema()
        .table(
            table("replace_absorbs")
                .pk_int(KEY)
                .col("k", Text, [Attr::Unique, Attr::OnConflictReplace])
                .col(LABEL, Text, []),
        )
        .table(
            table("ignore_absorbs")
                .pk_int(KEY)
                .col("v", Text, [Attr::NotNull, Attr::OnConflictIgnore])
                .col(LABEL, Text, []),
        )
        .table(
            table("abort_throws")
                .pk_int(KEY)
                .col("v", Text, [Attr::NotNull, Attr::OnConflictAbort])
                .col(LABEL, Text, []),
        )
        .table(
            table("rollback_throws")
                .pk_int(KEY)
                .col("v", Text, [Attr::NotNull, Attr::OnConflictRollback])
                .col(LABEL, Text, []),
        )
        .build()
        .expect("the ColumnOnConflict case schema is valid");

    TraitCase {
        kind: Trait::ColumnOnConflict,
        schema,
        seed: |conn| {
            conn.execute_batch(&format!(
                "INSERT INTO \"replace_absorbs\" (\"{KEY}\", \"k\", \"{LABEL}\") \
                   VALUES (1, '{OCCUPIED_KEY}', 'seeded');
                 INSERT INTO \"ignore_absorbs\" (\"{KEY}\", \"v\", \"{LABEL}\") \
                   VALUES (1, 'seeded value', 'seeded');
                 INSERT INTO \"abort_throws\" (\"{KEY}\", \"v\", \"{LABEL}\") \
                   VALUES (1, 'seeded value', 'seeded');
                 INSERT INTO \"rollback_throws\" (\"{KEY}\", \"v\", \"{LABEL}\") \
                   VALUES (1, 'seeded value', 'seeded');"
            ))?;
            Ok(())
        },
        probe: probe_column_on_conflict,
    }
}

fn probe_column_on_conflict(schema: &Schema, conn: &Connection) -> Result<(), ProbeError> {
    let replace = column_with_conflict(schema, OnConflict::Replace)?;
    let ignore = column_with_conflict(schema, OnConflict::Ignore)?;
    let abort = column_with_conflict(schema, OnConflict::Abort)?;
    let rollback = column_with_conflict(schema, OnConflict::Rollback)?;

    // Per table, not once for the case. Two of these four halves are green on
    // an empty database if nobody checks: a NOT NULL violation throws with no
    // prior row to conflict with, and an insert into an empty table leaves
    // exactly the one row REPLACE promises.
    for (table, _) in [replace, ignore, abort, rollback] {
        let rows = count(
            conn,
            &format!("SELECT count(*) FROM {}", quote(&table.name)),
        )?;
        if rows != 1 {
            return Err(ProbeError::Unseeded(format!(
                "{} holds {rows} rows, and the conflict this probe provokes needs exactly the one \
                 seeded row already occupying the key",
                table.name
            )));
        }
    }

    // REPLACE absorbs: the conflicting insert lands and the row it conflicted
    // with is gone.
    let (table, column) = replace;
    let inserted = conn.execute(
        &format!(
            "INSERT INTO {} (\"{KEY}\", {}, \"{LABEL}\") VALUES (2, '{OCCUPIED_KEY}', 'probed')",
            quote(&table.name),
            quote(&column.name)
        ),
        [],
    );
    if let Err(error) = inserted {
        return Err(ProbeError::Failed(format!(
            "the insert conflicting on {}.{} was thrown ({error}); ON CONFLICT REPLACE promises \
             the row already occupying the key is replaced by it",
            table.name, column.name
        )));
    }
    let survivors = count(
        conn,
        &format!("SELECT count(*) FROM {}", quote(&table.name)),
    )?;
    let survivor_label = text(
        conn,
        &format!(
            "SELECT \"{LABEL}\" FROM {} ORDER BY \"{KEY}\"",
            quote(&table.name)
        ),
    )?;
    if survivors != 1 || survivor_label.as_deref() != Some("probed") {
        return Err(ProbeError::Failed(format!(
            "{} holds {survivors} row(s) and the first is labelled {survivor_label:?} after the \
             conflicting insert; ON CONFLICT REPLACE promises one row, the probed one",
            table.name
        )));
    }

    // IGNORE absorbs: the offending row is skipped and nothing is thrown.
    let (table, column) = ignore;
    let null_insert = conn.execute(
        &format!(
            "INSERT INTO {} (\"{KEY}\", {}, \"{LABEL}\") VALUES (2, NULL, 'probed')",
            quote(&table.name),
            quote(&column.name)
        ),
        [],
    );
    match null_insert {
        Err(error) => {
            return Err(ProbeError::Failed(format!(
                "inserting NULL into {}.{} was thrown ({error}); ON CONFLICT IGNORE promises the \
                 row is skipped and the statement succeeds",
                table.name, column.name
            )))
        }
        Ok(_) => {
            let rows = count(
                conn,
                &format!("SELECT count(*) FROM {}", quote(&table.name)),
            )?;
            if rows != 1 {
                return Err(ProbeError::Failed(format!(
                    "{} holds {rows} rows after a NULL was inserted into {}; ON CONFLICT IGNORE \
                     promises the row is skipped, and a table that grew was never enforcing NOT \
                     NULL at all",
                    table.name, column.name
                )));
            }
        }
    }

    // ABORT throws.
    let (table, column) = abort;
    let null_insert = conn.execute(
        &format!(
            "INSERT INTO {} (\"{KEY}\", {}, \"{LABEL}\") VALUES (2, NULL, 'probed')",
            quote(&table.name),
            quote(&column.name)
        ),
        [],
    );
    if null_insert.is_ok() {
        return Err(ProbeError::Failed(format!(
            "inserting NULL into {}.{} was absorbed; ON CONFLICT ABORT promises the statement is \
             thrown",
            table.name, column.name
        )));
    }
    let rows = count(
        conn,
        &format!("SELECT count(*) FROM {}", quote(&table.name)),
    )?;
    if rows != 1 {
        return Err(ProbeError::Failed(format!(
            "{} holds {rows} rows after a thrown insert; ABORT undoes the statement it threw",
            table.name
        )));
    }

    probe_rollback_ends_the_transaction(conn, rollback)
}

/// ROLLBACK is the one algorithm whose promise is not about the statement.
///
/// ABORT and ROLLBACK both throw, and a probe that asserted only "it threw"
/// would pass on a database where ROLLBACK had been reconstructed as ABORT.
/// What differs is reach: ABORT undoes its own statement and leaves the
/// enclosing transaction open, ROLLBACK takes the transaction with it. So the
/// probe opens one, does work in it, and asks whether that work survived.
fn probe_rollback_ends_the_transaction(
    conn: &Connection,
    (table, column): (&Table, &Column),
) -> Result<(), ProbeError> {
    // Raw BEGIN rather than rusqlite's transaction guard: SQLite ends this
    // transaction itself when ROLLBACK fires, and a guard's Drop would then try
    // to roll back a transaction that is no longer there.
    conn.execute_batch("BEGIN")?;
    let observed = observe_conflict_in_transaction(conn, table, column);
    // If SQLite left the transaction open -- which is itself the finding -- the
    // probe must not hand the connection back inside one.
    if !conn.is_autocommit() {
        conn.execute_batch("ROLLBACK")?;
    }
    let (thrown, transaction_ended, rows) = observed?;

    if !thrown {
        return Err(ProbeError::Failed(format!(
            "inserting NULL into {}.{} was absorbed; ON CONFLICT ROLLBACK promises the statement \
             is thrown",
            table.name, column.name
        )));
    }
    if !transaction_ended {
        return Err(ProbeError::Failed(format!(
            "the transaction around the thrown insert into {}.{} was still open afterwards; that \
             is ABORT's behaviour, and ON CONFLICT ROLLBACK promises the transaction goes too",
            table.name, column.name
        )));
    }
    if rows != 1 {
        return Err(ProbeError::Failed(format!(
            "{} holds {rows} rows; the row inserted before the failure should have gone with the \
             rolled-back transaction",
            table.name
        )));
    }
    Ok(())
}

/// Inside the transaction: put a good row in, provoke the conflict, and note
/// whether it was thrown, whether the transaction outlived it, and what is left.
///
/// Separate from its caller so that the caller can settle the transaction on
/// every path, including the one where this returns early.
fn observe_conflict_in_transaction(
    conn: &Connection,
    table: &Table,
    column: &Column,
) -> Result<(bool, bool, i64), ProbeError> {
    conn.execute(
        &format!(
            "INSERT INTO {} (\"{KEY}\", {}, \"{LABEL}\") VALUES (2, 'in the transaction', 'probed')",
            quote(&table.name),
            quote(&column.name)
        ),
        [],
    )?;
    let thrown = conn
        .execute(
            &format!(
                "INSERT INTO {} (\"{KEY}\", {}, \"{LABEL}\") VALUES (3, NULL, 'probed')",
                quote(&table.name),
                quote(&column.name)
            ),
            [],
        )
        .is_err();
    let transaction_ended = conn.is_autocommit();
    let rows = count(
        conn,
        &format!("SELECT count(*) FROM {}", quote(&table.name)),
    )?;
    Ok((thrown, transaction_ended, rows))
}

/// The column carrying a given conflict algorithm, whichever constraint it is
/// attached to.
fn column_with_conflict(
    schema: &Schema,
    algorithm: OnConflict,
) -> Result<(&Table, &Column), ProbeError> {
    one_column(
        schema,
        &format!("declared ON CONFLICT {}", algorithm.as_sql()),
        |column| {
            column
                .constraints
                .iter()
                .any(|constraint| conflict_of(constraint) == Some(algorithm))
        },
    )
}

fn conflict_of(constraint: &ColumnConstraint) -> Option<OnConflict> {
    match constraint {
        ColumnConstraint::NotNull(algorithm) | ColumnConstraint::Unique(algorithm) => *algorithm,
        ColumnConstraint::PrimaryKey { on_conflict, .. } => *on_conflict,
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// ExpressionDefault
// ---------------------------------------------------------------------------

/// `datetime('now')` renders `YYYY-MM-DD HH:MM:SS`, and this is that shape as a
/// GLOB. Matching the shape rather than a value is the point: the probe cannot
/// know the second it ran in, and does not need to.
const TIMESTAMP_SHAPE: &str =
    "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]";
/// The arithmetic default's value, evaluated. One unit with the expression.
const ARITHMETIC_RESULT: i64 = 5;

pub(super) fn expression_default() -> TraitCase {
    let schema = schema()
        .table(
            table("expression_default")
                .pk_int(KEY)
                .col(
                    "made_at",
                    Text,
                    [Attr::Default(DefaultValue::expr("datetime('now')"))],
                )
                .col(
                    "computed",
                    Integer,
                    [Attr::Default(DefaultValue::expr("2 + 3"))],
                )
                .col(LABEL, Text, []),
        )
        .build()
        .expect("the ExpressionDefault case schema is valid");

    TraitCase {
        kind: Trait::ExpressionDefault,
        schema,
        seed: |conn| {
            // Omitting both defaulted columns is the whole seed: the defaults
            // fire, and the row is the one that has to survive a transformation
            // with the value the expression gave it.
            conn.execute_batch(&format!(
                "INSERT INTO \"expression_default\" (\"{KEY}\", \"{LABEL}\") VALUES (1, 'seeded')"
            ))?;
            Ok(())
        },
        probe: probe_expression_default,
    }
}

fn probe_expression_default(schema: &Schema, conn: &Connection) -> Result<(), ProbeError> {
    let defaulted: Vec<(&Table, &Column, &str)> = schema
        .tables
        .iter()
        .flat_map(|table| {
            table.columns.iter().filter_map(move |column| {
                column
                    .constraints
                    .iter()
                    .find_map(|constraint| match constraint {
                        ColumnConstraint::Default(DefaultValue::Expr(source)) => {
                            Some((table, column, source.as_str()))
                        }
                        _ => None,
                    })
            })
        })
        .collect();
    if defaulted.is_empty() {
        return Err(ProbeError::Failed(
            "the schema handed to this probe declares no DEFAULT (expression), so there is \
             nothing to have been evaluated"
                .to_string(),
        ));
    }

    let table = defaulted[0].0;
    let seeded = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE \"{KEY}\" = 1",
            quote(&table.name)
        ),
    )?;
    if seeded != 1 {
        return Err(ProbeError::Unseeded(format!(
            "{} has no row 1, so no default was ever evaluated to look at",
            table.name
        )));
    }

    conn.execute(
        &format!(
            "INSERT INTO {} (\"{KEY}\", \"{LABEL}\") VALUES (2, 'probed')",
            quote(&table.name)
        ),
        [],
    )?;

    // The general assertion, and the one the trait is named for: a default that
    // was re-rendered without its parentheses stops being an expression and
    // becomes the literal text of itself. Comparing the stored value to the
    // expression's own source is schema-driven and needs no knowledge of what
    // the expression means.
    for (table, column, source) in &defaulted {
        let literal: i64 = conn.query_row(
            &format!(
                "SELECT count(*) FROM {} WHERE CAST({} AS TEXT) = ?1",
                quote(&table.name),
                quote(&column.name)
            ),
            [source],
            |row| row.get(0),
        )?;
        if literal != 0 {
            return Err(ProbeError::Failed(format!(
                "{literal} row(s) of {}.{} hold the text {source:?}, which is the source of their \
                 own DEFAULT; the parentheses were dropped and the expression was stored as a \
                 literal instead of evaluated",
                table.name, column.name
            )));
        }
    }

    // And the specific ones, about this case's own two columns: a timestamp is
    // shaped like a timestamp, and arithmetic comes back as a number rather
    // than as text that merely looks like one.
    let shaped = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE \"made_at\" GLOB '{TIMESTAMP_SHAPE}'",
            quote(&table.name)
        ),
    )?;
    if shaped != 2 {
        let seen = text(
            conn,
            &format!(
                "SELECT \"made_at\" FROM {} ORDER BY \"{KEY}\" DESC",
                quote(&table.name)
            ),
        )?;
        return Err(ProbeError::Failed(format!(
            "{shaped} of 2 rows hold a timestamp in made_at (most recent: {seen:?}); \
             DEFAULT (datetime('now')) yields the time it was evaluated"
        )));
    }
    let arithmetic = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE typeof(\"computed\") = 'integer' AND \"computed\" = \
             {ARITHMETIC_RESULT}",
            quote(&table.name)
        ),
    )?;
    if arithmetic != 2 {
        return Err(ProbeError::Failed(format!(
            "{arithmetic} of 2 rows hold the integer {ARITHMETIC_RESULT} in computed; an \
             evaluated arithmetic default is a number, and a stored literal is text that is not"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// TypelessColumn
// ---------------------------------------------------------------------------

pub(super) fn typeless_column() -> TraitCase {
    let schema = schema()
        .table(
            table("typeless")
                .pk_int(KEY)
                .typeless("v", [])
                .col(LABEL, Text, []),
        )
        .build()
        .expect("the TypelessColumn case schema is valid");

    TraitCase {
        kind: Trait::TypelessColumn,
        schema,
        seed: |conn| {
            // One text value and one integer value in the same column. A column
            // resolved to TEXT stores both as text, and the difference below is
            // gone.
            conn.execute_batch(&format!(
                "INSERT INTO \"typeless\" (\"{KEY}\", \"v\", \"{LABEL}\") \
                   VALUES (1, 'a string', 'seeded'), (2, 42, 'seeded')"
            ))?;
            Ok(())
        },
        probe: probe_typeless_column,
    }
}

fn probe_typeless_column(schema: &Schema, conn: &Connection) -> Result<(), ProbeError> {
    let (table, column) = one_column(schema, "declared with no type at all", |column| {
        column.decl_type.is_none()
    })?;

    let seeded = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE \"{KEY}\" IN (1, 2)",
            quote(&table.name)
        ),
    )?;
    if seeded != 2 {
        return Err(ProbeError::Unseeded(format!(
            "{} does not hold both seeded rows, and one value cannot have two types",
            table.name
        )));
    }

    let type_of = |key: i64| -> Result<Option<String>, ProbeError> {
        text(
            conn,
            &format!(
                "SELECT typeof({}) FROM {} WHERE \"{KEY}\" = {key}",
                quote(&column.name),
                quote(&table.name)
            ),
        )
    };
    let (string_row, integer_row) = (type_of(1)?, type_of(2)?);
    if string_row.as_deref() != Some("text") || integer_row.as_deref() != Some("integer") {
        return Err(ProbeError::Failed(format!(
            "{}.{} reads typeof() {string_row:?} for a string and {integer_row:?} for an integer; \
             a blank-affinity column stores each value as what it is, and one resolved to TEXT \
             converts both",
            table.name, column.name
        )));
    }

    // The one PRAGMA assertion in the registry, taken deliberately.
    //
    // Every other probe here asserts behaviour because presence is not
    // behaviour. This column is the case where the two come apart in the other
    // direction: a typeless column promoted to BLOB keeps blank affinity's
    // dynamic typing exactly, so the assertion above passes on a database where
    // the type was invented. The promotion is invisible behaviourally on the
    // native path and shows up only in the declared type -- so reading it is
    // the only way to catch it, and refusing on principle would mean shipping a
    // probe that cannot see the defect it is named for. It is an addition to
    // the behavioural assertion above, never a replacement for it.
    let declared = text(
        conn,
        &format!(
            "SELECT type FROM pragma_table_info('{}') WHERE name = '{}'",
            table.name.replace('\'', "''"),
            column.name.replace('\'', "''")
        ),
    )?;
    if declared.as_deref() != Some("") {
        return Err(ProbeError::Failed(format!(
            "{}.{} declares the type {declared:?}, and it was declared with none; a type invented \
             for a typeless column survives every behavioural check on this path",
            table.name, column.name
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Trigger
// ---------------------------------------------------------------------------

/// The table the trigger writes into. Named here rather than resolved: the
/// trigger body is SQL, and forger does not parse SQL to find out what its own
/// case wrote.
const AUDIT: &str = "audit";
/// Seeded before the transformation, inserted after it. The pair is what makes
/// "fired once" separable from "fired at all" -- smugglr#336's shape.
const BEFORE: &str = "before";
const AFTER: &str = "after";

pub(super) fn trigger() -> TraitCase {
    let schema = schema()
        .table(
            table("evented")
                .pk_int(KEY)
                .col("note", Text, [])
                .trigger(Trigger {
                    name: "evented_audit".into(),
                    timing: TriggerTiming::After,
                    event: TriggerEvent::Insert,
                    when: None,
                    body: vec![format!(
                        "INSERT INTO {} (\"note\") VALUES (new.\"note\")",
                        quote(AUDIT)
                    )],
                }),
        )
        .table(table(AUDIT).pk_int(KEY).col("note", Text, []))
        .build()
        .expect("the Trigger case schema is valid");

    TraitCase {
        kind: Trait::Trigger,
        schema,
        seed: |conn| {
            conn.execute_batch(&format!(
                "INSERT INTO \"evented\" (\"{KEY}\", \"note\") VALUES (1, '{BEFORE}')"
            ))?;
            Ok(())
        },
        probe: probe_trigger,
    }
}

fn probe_trigger(schema: &Schema, conn: &Connection) -> Result<(), ProbeError> {
    let triggered: Vec<&Table> = schema
        .tables
        .iter()
        .filter(|table| !table.triggers.is_empty())
        .collect();
    let [evented] = triggered[..] else {
        return Err(ProbeError::Failed(format!(
            "the schema handed to this probe hangs triggers off {} tables, and the probe fires \
             one",
            triggered.len()
        )));
    };

    // The precondition is the seed's own row, not the trigger's side effect:
    // the side effect is what is being asserted, and a probe that required it
    // up front would refuse where it should fail.
    let seeded = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE \"{KEY}\" = 1",
            quote(&evented.name)
        ),
    )?;
    if seeded != 1 {
        return Err(ProbeError::Unseeded(format!(
            "{} holds no row seeded before the transformation, so a trigger firing now could not \
             be told apart from one that fired twice",
            evented.name
        )));
    }

    conn.execute(
        &format!(
            "INSERT INTO {} (\"{KEY}\", \"note\") VALUES (2, '{AFTER}')",
            quote(&evented.name)
        ),
        [],
    )?;

    let audited: Vec<String> = conn
        .prepare(&format!(
            "SELECT \"note\" FROM {} ORDER BY \"{KEY}\"",
            quote(AUDIT)
        ))?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    if audited != [BEFORE, AFTER] {
        return Err(ProbeError::Failed(format!(
            "{AUDIT} holds {audited:?} after one row was seeded and one inserted; the trigger's \
             side effect is [{BEFORE:?}, {AFTER:?}]. Fewer means it did not fire -- a trigger can \
             sit in sqlite_master and still never run. More means it fired over rows that had \
             already been audited, which is what a rebuild's INSERT ... SELECT does when the \
             trigger is re-created before the copy rather than after it"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// DescendingPrimaryKey
// ---------------------------------------------------------------------------

/// Seeded keys, chosen far from the rowids SQLite will hand out (1 and 2) so
/// that "the key is the rowid" cannot be true by coincidence.
const DESCENDING_KEYS: [i64; 2] = [100, 200];
/// The row the probe inserts without naming a key at all.
const NO_KEY_GIVEN: &str = "no key given";

pub(super) fn descending_primary_key() -> TraitCase {
    let schema = schema()
        .table(
            table("descending_key")
                .pk_col(KEY, Integer, SortOrder::Desc)
                .col(LABEL, Text, []),
        )
        .build()
        .expect("the DescendingPrimaryKey case schema is valid");

    TraitCase {
        kind: Trait::DescendingPrimaryKey,
        schema,
        seed: |conn| {
            let [first, second] = DESCENDING_KEYS;
            conn.execute_batch(&format!(
                "INSERT INTO \"descending_key\" (\"{KEY}\", \"{LABEL}\") \
                   VALUES ({first}, 'seeded'), ({second}, 'seeded')"
            ))?;
            Ok(())
        },
        probe: probe_descending_primary_key,
    }
}

fn probe_descending_primary_key(schema: &Schema, conn: &Connection) -> Result<(), ProbeError> {
    let (table, column) = one_column(schema, "a PRIMARY KEY declared DESC", |column| {
        column.constraints.iter().any(|constraint| {
            matches!(
                constraint,
                ColumnConstraint::PrimaryKey {
                    order: SortOrder::Desc,
                    ..
                }
            )
        })
    })?;

    let [first, second] = DESCENDING_KEYS;
    let seeded = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE {} IN ({first}, {second})",
            quote(&table.name),
            quote(&column.name)
        ),
    )?;
    if seeded != 2 {
        return Err(ProbeError::Unseeded(format!(
            "{} does not hold both seeded keys, and a key that is not there cannot be compared to \
             a rowid",
            table.name
        )));
    }

    // The property that actually differs. INTEGER PRIMARY KEY is the rowid
    // under another name; INTEGER PRIMARY KEY DESC is an ordinary key with a
    // unique index, and the table keeps a rowid of its own underneath.
    let aliased = count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE rowid = {}",
            quote(&table.name),
            quote(&column.name)
        ),
    )?;
    if aliased != 0 {
        return Err(ProbeError::Failed(format!(
            "{} row(s) of {}.{} equal their own rowid; that is the ascending spelling, which is \
             the rowid alias -- INTEGER PRIMARY KEY DESC is not one and was seeded with keys no \
             rowid sequence would produce",
            aliased, table.name, column.name
        )));
    }

    // And the consequence of not being an alias: nothing hands out a key.
    conn.execute(
        &format!(
            "INSERT INTO {} (\"{LABEL}\") VALUES ('{NO_KEY_GIVEN}')",
            quote(&table.name)
        ),
        [],
    )?;
    let assigned = text(
        conn,
        &format!(
            "SELECT typeof({}) FROM {} WHERE \"{LABEL}\" = '{NO_KEY_GIVEN}'",
            quote(&column.name),
            quote(&table.name)
        ),
    )?;
    if assigned.as_deref() != Some("null") {
        return Err(ProbeError::Failed(format!(
            "inserting into {} without naming {} left a {assigned:?} there; only a rowid alias \
             allocates a key, and a key that allocates is an ascending INTEGER PRIMARY KEY. \
             (SQLite also leaves a non-alias key nullable, which is why this reads as NULL rather \
             than being refused.)",
            table.name, column.name
        )));
    }
    Ok(())
}
