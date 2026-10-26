//! A service that speaks HTTP-SQL and cannot build a large value's
//! `quote()` text, as D1 cannot past its 2,000,000-byte strings: a sync
//! refuses and names the value over the service's value limit, and moves
//! every other row, instead of the service failing the whole read
//! (FR-SMUGGLR-071 rev 11).

use std::path::PathBuf;

use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};
use serde_json::{Value as Json, json};
use smugglr::{Options, sync};
use smugglr_place::{HttpSql, LocalFile, Transport};

/// The longest string the service builds, as D1's limit.
const STRINGS: usize = 1_000_000;

/// The service's value limit: half its strings, for a blob's hex, less 5%.
const LIMIT: u64 = 475_000;

/// An HTTP-SQL service over a SQLite file that fails any statement that
/// would give a string longer than [`STRINGS`].
#[derive(Debug)]
struct Small(Connection);

impl Small {
    fn statement(&self, sql: &str, params: &Json) -> Result<Json, String> {
        let error = |err: rusqlite::Error| err.to_string();
        let params: Vec<Value> = params
            .as_array()
            .into_iter()
            .flatten()
            .map(|param| match param {
                Json::Null => Value::Null,
                Json::Number(n) => n.as_i64().map_or(Value::Null, Value::Integer),
                other => Value::Text(other.as_str().unwrap_or_default().to_owned()),
            })
            .collect();
        let mut statement = self.0.prepare(sql).map_err(error)?;
        let width = statement.column_count();
        if width == 0 {
            let affected = statement.execute(params_from_iter(params)).map_err(error)?;
            return Ok(json!({"columns": [], "rows": [], "rowsAffected": affected}));
        }
        let mut rows = Vec::new();
        let mut found = statement.query(params_from_iter(params)).map_err(error)?;
        while let Some(row) = found.next().map_err(error)? {
            let mut out = Vec::with_capacity(width);
            for i in 0..width {
                out.push(match row.get::<_, Value>(i).map_err(error)? {
                    Value::Text(text) if text.len() > STRINGS => {
                        return Err("string or blob too big".to_owned());
                    }
                    Value::Text(text) => Json::String(text),
                    Value::Integer(n) => json!(n),
                    Value::Null => Json::Null,
                    _ => return Err("this service returns text".to_owned()),
                });
            }
            rows.push(out);
        }
        Ok(json!({"columns": [], "rows": rows, "rowsAffected": 0}))
    }
}

impl Transport for Small {
    fn send(&self, request: &Json) -> Result<(bool, Json), String> {
        let failed = |message: String| {
            (
                false,
                json!({"error": {"code": "sql_error", "message": message}}),
            )
        };
        if let Some(sql) = request["sql"].as_str() {
            return Ok(match self.statement(sql, &request["params"]) {
                Ok(result) => (true, result),
                Err(message) => failed(message),
            });
        }
        let results: Result<Vec<Json>, String> = request["batch"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|each| self.statement(each["sql"].as_str().unwrap_or(""), &each["params"]))
            .collect();
        Ok(match results {
            Ok(results) => (true, json!({"results": results})),
            Err(message) => failed(message),
        })
    }

    fn value_limit(&self) -> u64 {
        LIMIT
    }
}

#[test]
fn a_value_a_service_cannot_quote_is_refused_and_named_and_the_rest_moves()
-> Result<(), Box<dyn std::error::Error>> {
    let folder = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("http-sql-limit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&folder);
    std::fs::create_dir_all(&folder)?;
    let shape = "CREATE TABLE files (id TEXT PRIMARY KEY, data BLOB);";
    let service = Connection::open(folder.join("service.db"))?;
    // A 700 kB blob, whose quote() text is 1.4 MB: more than the service's
    // strings.
    service.execute_batch(&format!(
        "{shape} INSERT INTO files VALUES ('big', randomblob(700000)), ('small', X'01');"
    ))?;
    Connection::open(folder.join("file.db"))?.execute_batch(shape)?;
    let mut remote = HttpSql::through("http://small.example/", Box::new(Small(service)));
    let mut file = LocalFile::open(folder.join("file.db"))?;
    let options = Options {
        config_dir: Some(folder.join("config")),
        ..Options::default()
    };
    let record = sync(&mut remote, &mut file, &options)?;
    let named: Vec<String> = record.refused.iter().map(ToString::to_string).collect();
    assert_eq!(named.len(), 1, "{record}");
    assert!(
        named[0].contains("refused: files big: a value of 700000 bytes in data")
            && named[0].contains("value limit of 475000 bytes for http://small.example/"),
        "{}",
        named[0]
    );
    let held: Vec<String> = Connection::open(folder.join("file.db"))?
        .prepare("SELECT id FROM files")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    assert_eq!(held, ["small"]);
    drop(file);
    let _ = std::fs::remove_dir_all(&folder);
    Ok(())
}
