//! Local SQLite database operations

use crate::datasource::{ColumnInfo, DataSource, RowMeta, TableInfo};
use crate::error::{Result, SyncError};
use crate::table::TableSchema;
use rusqlite::{Connection, OpenFlags, Row};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use tracing::{debug, info, warn};

/// Wrapper for local SQLite database
pub struct LocalDb {
    conn: Mutex<Connection>,
}

impl LocalDb {
    pub(crate) fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().expect("mutex poisoned")
    }

    /// Open a local SQLite database
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        info!("Opening local database: {}", path.display());

        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Open read-only for diff operations
    pub fn open_readonly(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        info!("Opening local database (read-only): {}", path.display());

        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Get the database schema for table name validation.
    ///
    /// Queries sqlite_master to get all user tables.
    pub fn get_schema(&self) -> Result<TableSchema> {
        let conn = self.conn();
        let tables = list_tables_inner(&conn)?;
        Ok(TableSchema::new(tables))
    }

    /// The SQLite storage class actually stored in `table`.`column` -- one of
    /// `integer`, `real`, `text`, `blob` -- or `None` when the column does not
    /// exist or holds only NULLs.
    ///
    /// Bounded: the first non-NULL value decides. Used to detect two peers
    /// disagreeing about how a timestamp is represented, which SQLite will order
    /// confidently (its cross-class ordering is total) but not chronologically.
    pub fn stored_storage_class(&self, table: &str, column: &str) -> Result<Option<String>> {
        let conn = self.conn();
        let sql = format!(
            "SELECT typeof(\"{}\") FROM \"{}\" WHERE \"{}\" IS NOT NULL LIMIT 1",
            column, table, column
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        match rows.next()? {
            Some(row) => Ok(Some(row.get::<_, String>(0)?)),
            None => Ok(None),
        }
    }

    /// Upsert rows under an explicit same-PK conflict [`UpsertGuard`].
    ///
    /// The unguarded [`DataSource::upsert_rows`] is `INSERT OR REPLACE` -- the
    /// received row always wins. This is the same batch write with the
    /// resolution policy compiled into the statement, so a peer's stale row can
    /// be turned away without a read-modify-write.
    ///
    /// Inherent to `LocalDb` rather than added to [`DataSource`]: a guarded
    /// write needs SQLite's `ON CONFLICT` and has no general remote analogue, so
    /// plugin-SDK implementors are unaffected.
    pub fn upsert_rows_guarded(
        &self,
        table: &str,
        rows: &[HashMap<String, JsonValue>],
        guard: UpsertGuard<'_>,
    ) -> Result<UpsertOutcome> {
        let conn = self.conn();
        upsert_rows_guarded_inner(&conn, table, rows, guard)
    }
}

impl DataSource for LocalDb {
    async fn list_tables(&self) -> Result<Vec<String>> {
        let conn = self.conn();
        list_tables_inner(&conn)
    }

    async fn table_info(&self, table: &str) -> Result<TableInfo> {
        let conn = self.conn();
        table_info_inner(&conn, table)
    }

    async fn get_row_metadata(
        &self,
        table: &str,
        timestamp_column: &str,
        exclude_columns: &[String],
    ) -> Result<HashMap<String, RowMeta>> {
        let conn = self.conn();
        get_row_metadata_inner(&conn, table, timestamp_column, exclude_columns)
    }

    async fn get_rows(
        &self,
        table: &str,
        pk_values: &[String],
    ) -> Result<Vec<HashMap<String, JsonValue>>> {
        let conn = self.conn();
        get_rows_inner(&conn, table, pk_values)
    }

    async fn upsert_rows(&self, table: &str, rows: &[HashMap<String, JsonValue>]) -> Result<usize> {
        let conn = self.conn();
        upsert_rows_inner(&conn, table, rows)
    }

    async fn row_count(&self, table: &str) -> Result<usize> {
        let conn = self.conn();
        let sql = format!("SELECT COUNT(*) FROM \"{}\"", table);
        let count: usize = conn.query_row(&sql, [], |row| row.get(0))?;
        Ok(count)
    }
}

// -- Internal functions that operate on a borrowed Connection --

fn list_tables_inner(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master
         WHERE type = 'table'
         AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )?;

    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    debug!("Found {} tables", tables.len());
    Ok(tables)
}

fn table_info_inner(conn: &Connection, table: &str) -> Result<TableInfo> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{}\")", table))?;

    let columns: Vec<ColumnInfo> = stmt
        .query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get(1)?,
                col_type: row.get(2)?,
                notnull: row.get::<_, i32>(3)? != 0,
                pk: row.get::<_, i32>(5)? != 0,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    if columns.is_empty() {
        return Err(SyncError::TableNotFound(table.to_string()));
    }

    let primary_key: Vec<String> = columns
        .iter()
        .filter(|c| c.pk)
        .map(|c| c.name.clone())
        .collect();

    if primary_key.is_empty() {
        return Err(SyncError::NoPrimaryKey(table.to_string()));
    }

    Ok(TableInfo {
        name: table.to_string(),
        columns,
        primary_key,
    })
}

fn get_row_metadata_inner(
    conn: &Connection,
    table: &str,
    timestamp_column: &str,
    exclude_columns: &[String],
) -> Result<HashMap<String, RowMeta>> {
    let info = table_info_inner(conn, table)?;
    let pk_cols = &info.primary_key;
    let has_timestamp = info.columns.iter().any(|c| c.name == timestamp_column);

    // Column definition order -- the hash folds these (minus timestamp/excluded
    // columns) in exactly this order, shared with the plugin and wasm paths.
    let column_order: Vec<String> = info.columns.iter().map(|c| c.name.clone()).collect();

    // Select the PK rendered as TEXT (`__pk`) plus every column. Hashing each
    // column's JSON value through `crate::rowhash` is what keeps the native hash
    // byte-identical to the JSON-based plugin/wasm hashes.
    let pk_expr = crate::rowhash::pk_text_expr(pk_cols);
    let col_list = column_order
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {} AS __pk, {} FROM \"{}\"",
        pk_expr, col_list, table
    );

    debug!("Executing: {}", sql);

    let mut stmt = conn.prepare(&sql)?;
    let mut result = HashMap::new();

    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        // A NULL `__pk` (a NULL part in a composite PK, since `||` propagates
        // NULL) cannot key pk-based sync. Coercing it to "" would collapse every
        // such row onto one map entry, silently dropping rows and provoking
        // spurious deletes. Skip and warn instead.
        let pk_value = match row.get::<_, Option<String>>(0)? {
            Some(pk) => pk,
            None => {
                warn!("skipping row in {} with NULL primary key", table);
                continue;
            }
        };

        // Build the column->JSON map (cols start at index 1, after __pk).
        let mut row_map: HashMap<String, JsonValue> = HashMap::with_capacity(column_order.len());
        for (i, name) in column_order.iter().enumerate() {
            row_map.insert(name.clone(), get_json_value(row, i + 1)?);
        }

        let content_hash = crate::rowhash::content_hash(
            &row_map,
            &column_order,
            exclude_columns,
            timestamp_column,
        );

        // updated_at carries either an integer Unix timestamp or a string.
        // Shared with the plugin and wasm metadata builders via the one
        // canonical extractor so the three renderings cannot drift.
        let updated_at: Option<String> = if has_timestamp {
            crate::datasource::extract_updated_at(row_map.get(timestamp_column))
        } else {
            None
        };

        if let Some(prev) = result.insert(
            pk_value.clone(),
            RowMeta {
                pk_value: pk_value.clone(),
                updated_at,
                content_hash,
            },
        ) {
            // Two rows rendering to the same PK text means the PK is not unique
            // as encoded -- the metadata map silently lost `prev`. Surface it.
            warn!(
                "duplicate primary key {} in {} -- a row was overwritten in change metadata (prev hash {})",
                pk_value, table, prev.content_hash
            );
        }
    }

    debug!("Got {} rows from {}", result.len(), table);
    Ok(result)
}

fn get_rows_inner(
    conn: &Connection,
    table: &str,
    pk_values: &[String],
) -> Result<Vec<HashMap<String, JsonValue>>> {
    if pk_values.is_empty() {
        return Ok(vec![]);
    }

    let info = table_info_inner(conn, table)?;
    let pk_cols = &info.primary_key;

    // Build primary key expression
    let pk_expr = pk_cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(" || '|' || ");

    // Build column list
    let cols = info
        .columns
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");

    // Build IN clause
    let placeholders = pk_values.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

    let sql = format!(
        "SELECT {} FROM \"{}\" WHERE {} IN ({})",
        cols, table, pk_expr, placeholders
    );

    debug!("Fetching {} rows from {}", pk_values.len(), table);

    let mut stmt = conn.prepare(&sql)?;

    let params: Vec<&dyn rusqlite::ToSql> = pk_values
        .iter()
        .map(|v| v as &dyn rusqlite::ToSql)
        .collect();

    let mut rows = stmt.query(params.as_slice())?;
    let mut result = Vec::new();

    while let Some(row) = rows.next()? {
        let mut row_data = HashMap::new();
        for (i, col) in info.columns.iter().enumerate() {
            let value = get_json_value(row, i)?;
            row_data.insert(col.name.clone(), value);
        }
        result.push(row_data);
    }

    Ok(result)
}

fn upsert_rows_inner(
    conn: &Connection,
    table: &str,
    rows: &[HashMap<String, JsonValue>],
) -> Result<usize> {
    Ok(upsert_rows_guarded_inner(conn, table, rows, UpsertGuard::Replace)?.applied)
}

/// How a received row resolves against an existing local row with the same
/// primary key.
///
/// The variant selects the SQL shape; every shape is a single statement per row
/// so the resolution rides *inside* the write and is atomic against a concurrent
/// local writer. A read-then-write predicate would be racy and would cost a
/// round trip per row on the hot apply path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertGuard<'a> {
    /// Overwrite unconditionally -- `INSERT OR REPLACE`. The historical
    /// multicast behavior, and what `remote_wins` means on the apply side.
    Replace,
    /// Keep the local row -- `ON CONFLICT DO NOTHING`. Rows absent locally are
    /// still inserted; an existing row is never touched. `local_wins`.
    KeepLocal,
    /// Accept only when the incoming row's ordering value is strictly greater
    /// than the stored row's -- `ON CONFLICT DO UPDATE ... WHERE`. `newer_wins`.
    ///
    /// The ordering value is `max` across the listed columns that actually exist
    /// on the table, computed identically for both sides of the comparison.
    NewerBy(&'a [String]),
}

/// Result of a guarded upsert batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UpsertOutcome {
    /// Rows the guard admitted (inserted or updated).
    pub applied: usize,
    /// Rows the guard turned away -- an older or equally-ordered incoming row
    /// under [`UpsertGuard::NewerBy`], or any colliding row under
    /// [`UpsertGuard::KeepLocal`]. Not an error: a rejection is the guard doing
    /// its job. Surfaced so an embedder can see that peer rows are losing.
    pub rejected: usize,
    /// The ordering columns requested were all absent from the table, so the
    /// batch fell back to [`UpsertGuard::Replace`]. The caller should say so
    /// once per table: the configuration claims an ordering that the schema
    /// cannot supply, and silently applying blind is how "I believe I am
    /// ordered and I am not" happens.
    pub ordering_unavailable: bool,
}

/// A null-tolerant `max` over `cols`, each qualified by `qualifier`.
///
/// SQLite's scalar `max(a, b, c)` returns NULL if *any* argument is NULL, which
/// would be fatal here: a live row with `deleted_at IS NULL` would produce a
/// NULL ordering value and lose every comparison. Rotating the column list
/// through `coalesce` gives each position a non-NULL fallback, so the `max` sees
/// NULLs only when *every* column is NULL -- which is exactly when the row has
/// no ordering signal at all.
///
/// One column degenerates to a bare column reference, which is the
/// single-`timestamp_column` behavior.
fn ordering_max_expr(qualifier: &str, cols: &[String]) -> String {
    let qualified: Vec<String> = cols
        .iter()
        .map(|c| format!("{}.\"{}\"", qualifier, c))
        .collect();

    if qualified.len() == 1 {
        return qualified[0].clone();
    }

    let args: Vec<String> = (0..qualified.len())
        .map(|i| {
            let rotated: Vec<&str> = (0..qualified.len())
                .map(|j| qualified[(i + j) % qualified.len()].as_str())
                .collect();
            format!("coalesce({})", rotated.join(", "))
        })
        .collect();

    format!("max({})", args.join(", "))
}

fn upsert_rows_guarded_inner(
    conn: &Connection,
    table: &str,
    rows: &[HashMap<String, JsonValue>],
    guard: UpsertGuard<'_>,
) -> Result<UpsertOutcome> {
    if rows.is_empty() {
        return Ok(UpsertOutcome::default());
    }

    let info = table_info_inner(conn, table)?;
    let cols: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();

    let col_list = cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = cols.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

    // The conflict target must be the table's declared PRIMARY KEY columns.
    // `table_info_inner` already rejects a table without one (`NoPrimaryKey`),
    // so every table that reaches here has a target `ON CONFLICT` can bind to.
    let pk_target = info
        .primary_key
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    // Ordering columns the table does not have cannot participate. If none of
    // them exist, there is no ordering signal and the guard degrades to
    // Replace -- reported, never silent.
    let mut ordering_unavailable = false;
    let present: Vec<String> = match guard {
        UpsertGuard::NewerBy(want) => {
            let present: Vec<String> = want
                .iter()
                .filter(|c| info.columns.iter().any(|ci| &&ci.name == c))
                .cloned()
                .collect();
            if present.is_empty() {
                ordering_unavailable = true;
            }
            present
        }
        _ => Vec::new(),
    };

    let sql = match guard {
        UpsertGuard::Replace => format!(
            "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({})",
            table, col_list, placeholders
        ),
        UpsertGuard::KeepLocal => format!(
            "INSERT INTO \"{}\" ({}) VALUES ({}) ON CONFLICT({}) DO NOTHING",
            table, col_list, placeholders, pk_target
        ),
        UpsertGuard::NewerBy(_) if ordering_unavailable => format!(
            "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({})",
            table, col_list, placeholders
        ),
        UpsertGuard::NewerBy(_) => {
            // Only non-PK columns are assigned: the PK columns are equal by
            // definition of the conflict.
            let assignments = cols
                .iter()
                .filter(|c| !info.primary_key.iter().any(|pk| pk == *c))
                .map(|c| format!("\"{0}\" = excluded.\"{0}\"", c))
                .collect::<Vec<_>>()
                .join(", ");
            let incoming = ordering_max_expr("excluded", &present);
            let stored = ordering_max_expr(&format!("\"{}\"", table), &present);
            // An incoming row with no ordering value never displaces a stored
            // row; a stored row with no ordering value never blocks one. Both
            // arms are needed and both are symmetric across peers, so exactly
            // one side of any pair accepts and the mesh quiesces.
            format!(
                "INSERT INTO \"{table}\" ({col_list}) VALUES ({placeholders}) \
                 ON CONFLICT({pk_target}) DO UPDATE SET {assignments} \
                 WHERE {incoming} IS NOT NULL \
                 AND ({stored} IS NULL OR {incoming} > {stored})"
            )
        }
    };

    debug!("Upserting {} rows into {}: {}", rows.len(), table, sql);

    let tx = conn.unchecked_transaction()?;
    let mut applied = 0;
    let mut rejected = 0;

    {
        let mut stmt = tx.prepare(&sql)?;
        for row in rows {
            let params: Vec<JsonToSql> = cols
                .iter()
                .map(|col| JsonToSql(row.get(*col).cloned().unwrap_or(JsonValue::Null)))
                .collect();

            let param_refs: Vec<&dyn rusqlite::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

            // A guard that turns a row away reports 0 changed rows, so the
            // statement's own count -- not the batch length -- is the truth
            // about what landed.
            if stmt.execute(param_refs.as_slice())? > 0 {
                applied += 1;
            } else {
                rejected += 1;
            }
        }
    }

    tx.commit()?;
    if rejected > 0 {
        info!(
            "Upserted {} rows into {} ({} turned away by the conflict guard)",
            applied, table, rejected
        );
    } else {
        info!("Upserted {} rows into {}", applied, table);
    }
    Ok(UpsertOutcome {
        applied,
        rejected,
        ordering_unavailable,
    })
}

/// Convert a row value to JSON by its SQLite storage class.
///
/// Inspecting the `ValueRef` once distinguishes a genuine SQL NULL (-> JSON
/// null) from a value that fails to decode. Previously every branch was a
/// `get::<T>` whose `Err` was discarded, so a TEXT column holding non-UTF-8
/// bytes fell through all four and was returned as NULL -- folding into the
/// content hash indistinguishably from a real NULL (a stable-but-wrong hash,
/// silent divergence). Such a value now surfaces a conversion error instead of
/// being silently swallowed. (#180)
///
/// Behavior is otherwise unchanged: Integer/Real become JSON numbers, valid
/// UTF-8 Text becomes a JSON string, Blob becomes lowercase hex (the rowhash
/// wire contract) -- identical to the previous typed reads.
fn get_json_value(row: &Row, idx: usize) -> Result<JsonValue> {
    use rusqlite::types::{Type, ValueRef};
    match row.get_ref(idx)? {
        ValueRef::Null => Ok(JsonValue::Null),
        ValueRef::Integer(i) => Ok(JsonValue::from(i)),
        ValueRef::Real(f) => Ok(JsonValue::from(f)),
        ValueRef::Text(bytes) => match std::str::from_utf8(bytes) {
            Ok(s) => Ok(JsonValue::from(s.to_string())),
            Err(e) => {
                Err(rusqlite::Error::FromSqlConversionFailure(idx, Type::Text, Box::new(e)).into())
            }
        },
        ValueRef::Blob(b) => Ok(JsonValue::String(hex::encode(b))),
    }
}

/// Wrapper to allow JSON values as SQL parameters
struct JsonToSql(JsonValue);

impl rusqlite::ToSql for JsonToSql {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, Value};
        match &self.0 {
            JsonValue::Null => Ok(ToSqlOutput::Owned(Value::Null)),
            JsonValue::Bool(b) => Ok(ToSqlOutput::Owned(Value::Integer(if *b { 1 } else { 0 }))),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(ToSqlOutput::Owned(Value::Integer(i)))
                } else if let Some(f) = n.as_f64() {
                    Ok(ToSqlOutput::Owned(Value::Real(f)))
                } else {
                    Ok(ToSqlOutput::Owned(Value::Text(n.to_string())))
                }
            }
            JsonValue::String(s) => Ok(ToSqlOutput::Owned(Value::Text(s.clone()))),
            JsonValue::Array(_) | JsonValue::Object(_) => {
                Ok(ToSqlOutput::Owned(Value::Text(self.0.to_string())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn one_row_conn(value_sql: &str) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "CREATE TABLE t (v); INSERT INTO t VALUES ({});",
            value_sql
        ))
        .unwrap();
        conn
    }

    fn get_col0(conn: &Connection) -> Result<JsonValue> {
        let mut stmt = conn.prepare("SELECT v FROM t").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        get_json_value(row, 0)
    }

    #[test]
    fn get_json_value_errors_on_non_utf8_text() {
        // Regression for #180: a TEXT column holding non-UTF-8 bytes (a lone 0xFF
        // cast to TEXT) previously fell through every typed read and was returned
        // as NULL -- hashing identically to a real NULL. It must now surface an
        // error instead of being silently swallowed.
        let conn = one_row_conn("CAST(x'ff' AS TEXT)");
        assert!(
            get_col0(&conn).is_err(),
            "non-UTF-8 text must error, not fold to NULL"
        );
    }

    #[test]
    fn get_json_value_null_stays_null() {
        // The other side of the distinction: a genuine SQL NULL is still Ok(Null),
        // not an error -- the fix does not conflate empty with unreadable.
        let conn = one_row_conn("NULL");
        assert_eq!(get_col0(&conn).unwrap(), JsonValue::Null);
    }

    // -- Guarded upsert (#310) --

    /// A table shaped like legion's: an ordering key that is the max over three
    /// columns, one of which (`deleted_at`) is NULL on every live row.
    fn ordered_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE rows_t (
                 id TEXT PRIMARY KEY,
                 v TEXT,
                 created_at TEXT,
                 updated_at TEXT,
                 deleted_at TEXT
             );",
        )
        .unwrap();
        conn
    }

    fn row(
        id: &str,
        v: &str,
        created: Option<&str>,
        updated: Option<&str>,
        deleted: Option<&str>,
    ) -> HashMap<String, JsonValue> {
        let j = |o: Option<&str>| o.map(JsonValue::from).unwrap_or(JsonValue::Null);
        HashMap::from([
            ("id".to_string(), JsonValue::from(id)),
            ("v".to_string(), JsonValue::from(v)),
            ("created_at".to_string(), j(created)),
            ("updated_at".to_string(), j(updated)),
            ("deleted_at".to_string(), j(deleted)),
        ])
    }

    fn read_v(conn: &Connection, id: &str) -> String {
        conn.query_row("SELECT v FROM rows_t WHERE id = ?1", [id], |r| r.get(0))
            .unwrap()
    }

    fn legion_ordering() -> Vec<String> {
        vec![
            "created_at".to_string(),
            "updated_at".to_string(),
            "deleted_at".to_string(),
        ]
    }

    #[test]
    fn ordering_max_expr_single_column_is_a_bare_reference() {
        // One entry must degenerate to exactly the single-timestamp_column
        // behavior -- no coalesce/max wrapper to change its meaning.
        assert_eq!(
            ordering_max_expr("excluded", &["updated_at".to_string()]),
            "excluded.\"updated_at\""
        );
    }

    #[test]
    fn ordering_max_expr_tolerates_nulls_via_rotation() {
        // The rotation exists because SQLite's scalar max() returns NULL if ANY
        // argument is NULL. Every position must appear first in some coalesce,
        // or a NULL in that position would poison the result.
        let e = ordering_max_expr("excluded", &legion_ordering());
        for c in ["created_at", "updated_at", "deleted_at"] {
            assert!(
                e.contains(&format!("coalesce(excluded.\"{}\"", c)),
                "{} never leads a coalesce in {}",
                c,
                e
            );
        }
    }

    #[test]
    fn newer_by_rejects_an_older_row_and_accepts_a_newer_one() {
        let conn = ordered_conn();
        let ordering = legion_ordering();
        upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "local",
                Some("2026-01-01"),
                Some("2026-05-01"),
                None,
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();

        // Older peer row: turned away, counted, local content intact.
        let older = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "stale",
                Some("2026-01-01"),
                Some("2026-04-01"),
                None,
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((older.applied, older.rejected), (0, 1));
        assert_eq!(read_v(&conn, "a"), "local");

        // Newer peer row: accepted.
        let newer = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "fresh",
                Some("2026-01-01"),
                Some("2026-06-01"),
                None,
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((newer.applied, newer.rejected), (1, 0));
        assert_eq!(read_v(&conn, "a"), "fresh");
    }

    #[test]
    fn newer_by_keeps_the_local_row_on_an_exact_tie() {
        // Strict greater-than: two writes at the identical instant stay
        // divergent rather than flapping. Matches the remote path's tie rule.
        let conn = ordered_conn();
        let ordering = legion_ordering();
        let base = row("a", "local", Some("2026-01-01"), Some("2026-05-01"), None);
        upsert_rows_guarded_inner(&conn, "rows_t", &[base], UpsertGuard::NewerBy(&ordering))
            .unwrap();

        let tie = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "peer",
                Some("2026-01-01"),
                Some("2026-05-01"),
                None,
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((tie.applied, tie.rejected), (0, 1));
        assert_eq!(read_v(&conn, "a"), "local");
    }

    #[test]
    fn tombstone_setting_only_deleted_at_wins() {
        // legion's exact shape, and the reason the ordering signal is a LIST:
        // the tombstone stamps deleted_at and leaves updated_at alone. Under a
        // single-column compare on updated_at this is a tie and the delete is
        // rejected -- the row resurrects.
        let conn = ordered_conn();
        let ordering = legion_ordering();
        upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "live",
                Some("2026-01-01"),
                Some("2026-05-01"),
                None,
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();

        let tombstone = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "gone",
                Some("2026-01-01"),
                Some("2026-05-01"),
                Some("2026-06-01"),
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((tombstone.applied, tombstone.rejected), (1, 0));
        assert_eq!(read_v(&conn, "a"), "gone");

        // And the same tombstone must not be undone by a later live row whose
        // max is lower.
        let resurrect = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row(
                "a",
                "back",
                Some("2026-01-01"),
                Some("2026-05-02"),
                None,
            )],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((resurrect.applied, resurrect.rejected), (0, 1));
        assert_eq!(read_v(&conn, "a"), "gone");
    }

    #[test]
    fn newer_by_handles_a_missing_ordering_value_on_either_side() {
        let conn = ordered_conn();
        let ordering = legion_ordering();

        // Stored row has no ordering signal at all -> it cannot block anything.
        upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row("a", "unstamped", None, None, None)],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        let over = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row("a", "stamped", None, Some("2026-01-01"), None)],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((over.applied, over.rejected), (1, 0));
        assert_eq!(read_v(&conn, "a"), "stamped");

        // Incoming row has no ordering signal -> it never displaces one that
        // does. The two arms are symmetric, so across a pair of peers exactly
        // one side accepts and the exchange terminates.
        let blind = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row("a", "unstamped-peer", None, None, None)],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((blind.applied, blind.rejected), (0, 1));
        assert_eq!(read_v(&conn, "a"), "stamped");
    }

    #[test]
    fn newer_by_falls_back_to_replace_when_the_table_has_no_ordering_column() {
        // Configuration claims an ordering the schema cannot supply. Applying
        // blind is the terminal choice, but it must be REPORTED -- a user who
        // believes they are ordered and is not is the failure this guard exists
        // to remove.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE plain (id TEXT PRIMARY KEY, v TEXT);")
            .unwrap();
        let mk = |v: &str| {
            HashMap::from([
                ("id".to_string(), JsonValue::from("a")),
                ("v".to_string(), JsonValue::from(v)),
            ])
        };
        upsert_rows_guarded_inner(&conn, "plain", &[mk("first")], UpsertGuard::Replace).unwrap();

        let ordering = legion_ordering();
        let out = upsert_rows_guarded_inner(
            &conn,
            "plain",
            &[mk("second")],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert!(out.ordering_unavailable, "the caller must be told");
        assert_eq!(out.applied, 1);
        let v: String = conn
            .query_row("SELECT v FROM plain WHERE id = 'a'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, "second", "fallback is blind overwrite, as before");
    }

    #[test]
    fn newer_by_uses_only_the_ordering_columns_the_table_actually_has() {
        // A table with updated_at but no created_at/deleted_at still orders --
        // the absent columns are dropped from the comparison rather than
        // disabling it.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE partial (id TEXT PRIMARY KEY, v TEXT, updated_at TEXT);")
            .unwrap();
        let mk = |v: &str, ts: &str| {
            HashMap::from([
                ("id".to_string(), JsonValue::from("a")),
                ("v".to_string(), JsonValue::from(v)),
                ("updated_at".to_string(), JsonValue::from(ts)),
            ])
        };
        let ordering = legion_ordering();
        upsert_rows_guarded_inner(
            &conn,
            "partial",
            &[mk("local", "2026-05-01")],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        let out = upsert_rows_guarded_inner(
            &conn,
            "partial",
            &[mk("stale", "2026-04-01")],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert!(!out.ordering_unavailable);
        assert_eq!((out.applied, out.rejected), (0, 1));
    }

    #[test]
    fn newer_by_guards_a_composite_primary_key() {
        // The conflict target is the table's declared PK columns, not the
        // rendered `__pk` text, which is a lookup key and not a constraint.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE pair (p TEXT, q TEXT, v TEXT, updated_at TEXT, PRIMARY KEY (p, q));",
        )
        .unwrap();
        let mk = |v: &str, ts: &str| {
            HashMap::from([
                ("p".to_string(), JsonValue::from("x")),
                ("q".to_string(), JsonValue::from("y")),
                ("v".to_string(), JsonValue::from(v)),
                ("updated_at".to_string(), JsonValue::from(ts)),
            ])
        };
        let ordering = vec!["updated_at".to_string()];
        upsert_rows_guarded_inner(
            &conn,
            "pair",
            &[mk("local", "2026-05-01")],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        let out = upsert_rows_guarded_inner(
            &conn,
            "pair",
            &[mk("stale", "2026-01-01")],
            UpsertGuard::NewerBy(&ordering),
        )
        .unwrap();
        assert_eq!((out.applied, out.rejected), (0, 1));
        let v: String = conn
            .query_row("SELECT v FROM pair WHERE p='x' AND q='y'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, "local");
    }

    #[test]
    fn keep_local_inserts_new_rows_but_never_overwrites() {
        let conn = ordered_conn();
        upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row("a", "local", None, Some("2026-01-01"), None)],
            UpsertGuard::Replace,
        )
        .unwrap();

        let out = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[
                row("a", "peer", None, Some("2027-01-01"), None),
                row("b", "peer", None, Some("2027-01-01"), None),
            ],
            UpsertGuard::KeepLocal,
        )
        .unwrap();
        assert_eq!((out.applied, out.rejected), (1, 1));
        assert_eq!(read_v(&conn, "a"), "local", "even a newer peer row loses");
        assert_eq!(read_v(&conn, "b"), "peer", "absent rows still arrive");
    }

    #[test]
    fn replace_still_overwrites_blindly() {
        // The default multicast policy is remote_wins, and it must behave
        // exactly as INSERT OR REPLACE always did -- including accepting a row
        // that is older by every ordering column.
        let conn = ordered_conn();
        upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row("a", "local", None, Some("2026-05-01"), None)],
            UpsertGuard::Replace,
        )
        .unwrap();
        let out = upsert_rows_guarded_inner(
            &conn,
            "rows_t",
            &[row("a", "older-peer", None, Some("2020-01-01"), None)],
            UpsertGuard::Replace,
        )
        .unwrap();
        assert_eq!((out.applied, out.rejected), (1, 0));
        assert_eq!(read_v(&conn, "a"), "older-peer");
    }

    #[test]
    fn deleted_at_only_write_changes_the_content_hash() {
        // The seam that would break legion silently: `deleted_at` must stay
        // VISIBLE to the content hash. `rowhash` excludes updated_at/created_at
        // and the configured timestamp_column; if the ordering-column list were
        // ever folded into that exclusion set, a tombstone that only stamps
        // deleted_at would hash identically to the live row, produce no digest
        // mismatch, and never be gossiped at all -- while the config says
        // newer_wins.
        let conn = ordered_conn();
        conn.execute(
            "INSERT INTO rows_t VALUES ('a','v','2026-01-01','2026-05-01',NULL)",
            [],
        )
        .unwrap();
        let before = get_row_metadata_inner(&conn, "rows_t", "updated_at", &[]).unwrap();

        conn.execute(
            "UPDATE rows_t SET deleted_at = '2026-06-01' WHERE id = 'a'",
            [],
        )
        .unwrap();
        let after = get_row_metadata_inner(&conn, "rows_t", "updated_at", &[]).unwrap();

        assert_ne!(
            before["a"].content_hash, after["a"].content_hash,
            "a deleted_at-only write must change the digest, or tombstones never propagate"
        );
    }

    #[test]
    fn stored_storage_class_reports_the_first_non_null_class() {
        let conn = ordered_conn();
        conn.execute(
            "INSERT INTO rows_t VALUES ('a','v',NULL,'2026-05-01',NULL)",
            [],
        )
        .unwrap();
        // Bypass LocalDb::open (which needs a file) by exercising the same SQL.
        let class: Option<String> = conn
            .query_row(
                "SELECT typeof(\"updated_at\") FROM \"rows_t\" WHERE \"updated_at\" IS NOT NULL LIMIT 1",
                [],
                |r| r.get(0),
            )
            .ok();
        assert_eq!(class.as_deref(), Some("text"));
        let all_null: Option<String> = conn
            .query_row(
                "SELECT typeof(\"deleted_at\") FROM \"rows_t\" WHERE \"deleted_at\" IS NOT NULL LIMIT 1",
                [],
                |r| r.get(0),
            )
            .ok();
        assert_eq!(all_null, None, "an all-NULL column has no class to compare");
    }

    #[test]
    fn get_json_value_preserves_normal_types() {
        // Behavior preservation: int/real/text/blob render exactly as the prior
        // typed reads did, so content hashes are unchanged.
        assert_eq!(
            get_col0(&one_row_conn("42")).unwrap(),
            JsonValue::from(42i64)
        );
        assert_eq!(
            get_col0(&one_row_conn("2.5")).unwrap(),
            JsonValue::from(2.5f64)
        );
        assert_eq!(
            get_col0(&one_row_conn("'hello'")).unwrap(),
            JsonValue::from("hello")
        );
        // Blob x'01ff' hashes as lowercase hex "01ff".
        assert_eq!(
            get_col0(&one_row_conn("x'01ff'")).unwrap(),
            JsonValue::from("01ff")
        );
    }
}
