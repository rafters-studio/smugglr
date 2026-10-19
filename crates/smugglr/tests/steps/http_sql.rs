//! Steps for places that speak HTTP-SQL, and the local HTTP-SQL service they
//! run against: a SQLite file behind a small server in this test process.
//!
//! The server speaks the parts of HTTP-SQL 0.0.1 smugglr uses: one statement
//! or a batch, `atomic`, tagged `bigint` parameters, and the success and
//! error envelopes, emitting integers beyond 2^53 as tagged `bigint`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};

use rstest_bdd_macros::{given, then, when};
use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};
use serde_json::{Value as Json, json};

use crate::setup::{NOTES, THREE_NOTES, World, rows_of};

/// The largest integer a JSON number carries exactly.
const JSON_SAFE: i64 = (1 << 53) - 1;

/// Serves the SQLite file at `path` over HTTP-SQL on this machine until the
/// test process ends, and returns its address.
fn serve(path: &str) -> Result<String, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
    let address = listener.local_addr().map_err(|err| err.to_string())?;
    let path = path.to_owned();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let _ = answer(stream, &path);
        }
    });
    Ok(format!("http://{address}/"))
}

fn answer(mut stream: TcpStream, path: &str) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut length = 0;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 || line.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    let (status, envelope) = match serde_json::from_slice(&body)
        .map_err(|err| err.to_string())
        .and_then(|request| execute(path, &request))
    {
        Ok(envelope) => ("200 OK", envelope),
        Err(message) => (
            "400 Bad Request",
            json!({"error": {"code": "sql_error", "message": message}}),
        ),
    };
    let envelope = envelope.to_string();
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: application/http-sql+json\r\n\
         Http-Sql-Version: 0.0.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{envelope}",
        envelope.len()
    )
}

fn execute(path: &str, request: &Json) -> Result<Json, String> {
    let connection = Connection::open(path).map_err(|err| err.to_string())?;
    if let Some(sql) = request["sql"].as_str() {
        return statement(&connection, sql, &request["params"]);
    }
    let batch = request["batch"].as_array().ok_or("neither sql nor batch")?;
    let atomic = request["atomic"].as_bool().unwrap_or(false);
    let sql = |text: &str| {
        connection
            .execute_batch(text)
            .map_err(|err| err.to_string())
    };
    if atomic {
        sql("BEGIN")?;
    }
    let results = batch
        .iter()
        .map(|each| {
            statement(
                &connection,
                each["sql"].as_str().unwrap_or(""),
                &each["params"],
            )
        })
        .collect::<Result<Vec<Json>, String>>();
    match results {
        Ok(results) => {
            if atomic {
                sql("COMMIT")?;
            }
            Ok(json!({"results": results}))
        }
        Err(message) => {
            if atomic {
                sql("ROLLBACK")?;
            }
            Err(message)
        }
    }
}

fn param(value: &Json) -> Result<Value, String> {
    Ok(match value {
        Json::Null => Value::Null,
        Json::String(text) => Value::Text(text.clone()),
        Json::Number(n) => n
            .as_i64()
            .map(Value::Integer)
            .or_else(|| n.as_f64().map(Value::Real))
            .ok_or("an unreadable number")?,
        Json::Object(tagged) if tagged.get("$type") == Some(&json!("bigint")) => tagged["$value"]
            .as_str()
            .and_then(|digits| digits.parse().ok())
            .map(Value::Integer)
            .ok_or("an unreadable bigint")?,
        other => return Err(format!("this service does not take {other}")),
    })
}

fn emit(value: Value) -> Result<Json, String> {
    Ok(match value {
        Value::Null => Json::Null,
        Value::Integer(integer) if integer.abs() <= JSON_SAFE => json!(integer),
        Value::Integer(integer) => json!({"$type": "bigint", "$value": integer.to_string()}),
        Value::Real(real) => json!(real),
        Value::Text(text) => Json::String(text),
        Value::Blob(_) => return Err("this service does not return blobs".to_owned()),
    })
}

fn statement(connection: &Connection, sql: &str, params: &Json) -> Result<Json, String> {
    let error = |err: rusqlite::Error| err.to_string();
    let params = params
        .as_array()
        .map_or(Ok(Vec::new()), |params| params.iter().map(param).collect())?;
    let mut statement = connection.prepare(sql).map_err(error)?;
    let columns: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect();
    if columns.is_empty() {
        let affected = statement.execute(params_from_iter(params)).map_err(error)?;
        return Ok(
            json!({"columns": [], "rows": [], "rowsAffected": affected, "lastInsertId": null}),
        );
    }
    let mut rows = Vec::new();
    let mut found = statement.query(params_from_iter(params)).map_err(error)?;
    while let Some(row) = found.next().map_err(error)? {
        rows.push(
            (0..columns.len())
                .map(|i| row.get::<_, Value>(i).map_err(error).and_then(emit))
                .collect::<Result<Vec<Json>, String>>()?,
        );
    }
    Ok(json!({"columns": columns, "rows": rows, "rowsAffected": 0, "lastInsertId": null}))
}

/// Makes the SQLite file `name` running `sql`, serves it over HTTP-SQL and
/// returns its address; the file's path is kept as the world's service.
fn service(world: &World, name: &str, sql: &str) -> Result<String, String> {
    let path = world.place(name, sql)?;
    world.service.set(path.clone());
    serve(&path)
}

#[given(
    "a local file and a service that speaks HTTP-SQL, where the file has rows the service lacks"
)]
fn file_and_service(world: &World) -> Result<(), String> {
    world
        .source
        .set(world.place("field.db", &format!("{NOTES}; {THREE_NOTES}"))?);
    world.target.set(service(world, "service.db", NOTES)?);
    Ok(())
}

#[given(
    "a service that speaks HTTP-SQL holding a row with the integer {integer}, and a local file without it"
)]
fn service_with_integer(world: &World, integer: i64) -> Result<(), String> {
    let rows = format!("{NOTES}; INSERT INTO notes VALUES ('big', 'whole', {integer})");
    world.source.set(service(world, "service.db", &rows)?);
    world.target.set(world.place("field.db", NOTES)?);
    Ok(())
}

#[when("the agent runs smugglr sync on the service and the file")]
fn sync_service_and_file(world: &World) -> Result<(), String> {
    world.sync_ok()
}

#[then("the file's row holds {integer}")]
fn file_row_holds(world: &World, integer: i64) -> Result<(), String> {
    let path = world.target.get().ok_or("no file")?;
    let held = rows_of(&path, "SELECT created_at FROM notes WHERE id = 'big'")?;
    let want = vec![vec![format!("{:?}", Value::Integer(integer))]];
    if held == want {
        Ok(())
    } else {
        Err(format!("the file holds {held:?}, not {integer}"))
    }
}
