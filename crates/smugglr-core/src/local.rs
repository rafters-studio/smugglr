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
    if rows.is_empty() {
        return Ok(0);
    }

    let info = table_info_inner(conn, table)?;
    let cols: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();

    // Build INSERT OR REPLACE statement
    let col_list = cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = cols.iter().map(|_| "?").collect::<Vec<_>>().join(", ");

    let sql = format!(
        "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({})",
        table, col_list, placeholders
    );

    debug!("Upserting {} rows into {}", rows.len(), table);

    let tx = conn.unchecked_transaction()?;
    let mut count = 0;

    {
        let mut stmt = tx.prepare(&sql)?;
        for row in rows {
            let params: Vec<JsonToSql> = cols
                .iter()
                .map(|col| JsonToSql(row.get(*col).cloned().unwrap_or(JsonValue::Null)))
                .collect();

            let param_refs: Vec<&dyn rusqlite::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

            stmt.execute(param_refs.as_slice())?;
            count += 1;
        }
    }

    tx.commit()?;
    info!("Upserted {} rows into {}", count, table);
    Ok(count)
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
