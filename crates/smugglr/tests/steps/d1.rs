//! Steps for Cloudflare D1 as a place, and the local stand-in of D1's REST
//! API they run against in CI: a SQLite file behind a small server in this
//! test process.
//!
//! The stand-in keeps D1's quirks that smugglr must meet: a bearer token;
//! databases found by a name filter that also matches other names; every
//! parameter text only; integers back as plain JSON numbers; its own
//! `_cf_*` tables not to be read. It runs a batch as one transaction, which
//! is the stand-in's own assumption, not D1's documented behaviour (d1.rs).
//! Before a release the @hosted scenario runs against the real service with
//! credentials.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};

use rstest_bdd_macros::{given, then, when};
use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};
use serde_json::{Value as Json, json};
use smugglr_place::{D1, HttpSql, Key, Place};

use crate::http_sql::service;
use crate::setup::{NOTES, THREE_NOTES, World, rows_of};
use crate::shape::record;

const ACCOUNT: &str = "0123456789abcdef0123456789abcdef";
const TOKEN: &str = "stand-in-token";
const UUID: &str = "01a15879-0000-7000-8000-00000000d1d1";

/// One request to the stand-in.
#[derive(Debug)]
struct Request {
    /// Its method and target, as `GET /path?query HTTP/1.1`.
    line: String,
    /// Its `Authorization` header, if any.
    authorization: Option<String>,
    body: Vec<u8>,
}

/// Serves a stand-in of D1's API whose one account holds the database
/// `name`, kept in the SQLite file at `path`, until the test process ends;
/// returns the API's address.
fn serve_d1(name: &str, path: &str) -> Result<String, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
    let address = listener.local_addr().map_err(|err| err.to_string())?;
    let (name, path) = (name.to_owned(), path.to_owned());
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let _ = answer(stream, &name, &path);
        }
    });
    Ok(format!("http://{address}/client/v4"))
}

fn answer(mut stream: TcpStream, name: &str, path: &str) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let mut length = 0;
    let mut authorization = None;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 || header.trim().is_empty() {
            break;
        }
        if let Some((field, value)) = header.split_once(':') {
            if field.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().unwrap_or(0);
            } else if field.eq_ignore_ascii_case("authorization") {
                authorization = Some(value.trim().to_owned());
            }
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    let request = Request {
        line: line.trim().to_owned(),
        authorization,
        body,
    };
    let (status, envelope) = match handle(&request, name, path) {
        Ok(result) => (
            "200 OK",
            json!({"success": true, "errors": [], "messages": [], "result": result}),
        ),
        Err((status, code, message)) => (
            status,
            json!({
                "success": false,
                "errors": [{"code": code, "message": message}],
                "messages": [],
                "result": null,
            }),
        ),
    };
    let envelope = envelope.to_string();
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{envelope}",
        envelope.len()
    )
}

type Refusal = (&'static str, u32, String);

fn handle(request: &Request, name: &str, path: &str) -> Result<Json, Refusal> {
    if request.authorization.as_deref() != Some(&format!("Bearer {TOKEN}")) {
        return Err(("401 Unauthorized", 10000, "Authentication error".to_owned()));
    }
    let databases = format!("/client/v4/accounts/{ACCOUNT}/d1/database");
    let (method, target) = request
        .line
        .split_once(' ')
        .map(|(method, rest)| (method, rest.split(' ').next().unwrap_or("")))
        .unwrap_or_default();
    if method == "GET"
        && let Some(query) = target.strip_prefix(&format!("{databases}?"))
    {
        // D1's name filter is a search: it finds other names too.
        let asked = query.strip_prefix("name=").unwrap_or("");
        return Ok(json!([
            {"uuid": "01a15879-0000-7000-8000-0000000000aa", "name": format!("{asked}-staging")},
            {"uuid": UUID, "name": name},
        ]));
    }
    if method == "POST" && target == format!("{databases}/{UUID}/raw") {
        let body: Json = serde_json::from_slice(&request.body)
            .map_err(|err| ("400 Bad Request", 7400, err.to_string()))?;
        return raw(path, &body).map_err(|message| ("400 Bad Request", 7500, message));
    }
    Err((
        "404 Not Found",
        7404,
        format!("no route for {method} {target}"),
    ))
}

/// D1's `/raw`: a batch, as one transaction.
fn raw(path: &str, body: &Json) -> Result<Json, String> {
    let batch = body["batch"]
        .as_array()
        .ok_or("this stand-in takes a batch")?;
    for statement in batch {
        let sql = statement["sql"].as_str().unwrap_or("");
        let params = statement["params"].as_array().cloned().unwrap_or_default();
        let named = params
            .iter()
            .any(|param| param.as_str().is_some_and(|text| text.starts_with("_cf_")));
        if sql.contains("\"_cf_") || named {
            return Err(format!("not authorized: SQLITE_AUTH: {sql}"));
        }
    }
    let connection = Connection::open(path).map_err(|err| err.to_string())?;
    let sql = |text: &str| {
        connection
            .execute_batch(text)
            .map_err(|err| err.to_string())
    };
    sql("BEGIN")?;
    let results = batch
        .iter()
        .map(|each| {
            rows(
                &connection,
                each["sql"].as_str().unwrap_or(""),
                &each["params"],
            )
        })
        .collect::<Result<Vec<Json>, String>>();
    sql(if results.is_ok() {
        "COMMIT"
    } else {
        "ROLLBACK"
    })?;
    Ok(Json::Array(
        results?
            .into_iter()
            .map(|rows| json!({"success": true, "results": {"rows": rows}, "meta": {}}))
            .collect(),
    ))
}

/// Runs one statement with D1's text-only parameters, and returns the rows
/// it selected or returned.
fn rows(connection: &Connection, sql: &str, params: &Json) -> Result<Json, String> {
    let error = |err: rusqlite::Error| err.to_string();
    let params = params
        .as_array()
        .map_or(Ok(Vec::new()), |params| params.iter().map(param).collect())?;
    let mut statement = connection.prepare(sql).map_err(error)?;
    let width = statement.column_count();
    let mut rows = Vec::new();
    let mut found = statement.query(params_from_iter(params)).map_err(error)?;
    while let Some(row) = found.next().map_err(error)? {
        rows.push(Json::Array(
            (0..width)
                .map(|i| row.get::<_, Value>(i).map(emit).map_err(error))
                .collect::<Result<Vec<Json>, String>>()?,
        ));
    }
    Ok(Json::Array(rows))
}

/// D1 binds every parameter as text.
fn param(value: &Json) -> Result<Value, String> {
    value
        .as_str()
        .map(|text| Value::Text(text.to_owned()))
        .ok_or_else(|| format!("D1 takes text parameters only, not {value}"))
}

/// The largest integer a JSON number carries exactly.
const JSON_SAFE: i64 = (1 << 53) - 1;

/// D1 answers with plain JSON numbers, integers beyond 2^53 rounded.
#[allow(clippy::cast_precision_loss)]
fn emit(value: Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Integer(integer) if (-JSON_SAFE..=JSON_SAFE).contains(&integer) => json!(integer),
        Value::Integer(integer) => json!(integer as f64),
        Value::Real(real) => json!(real),
        Value::Text(text) => Json::String(text),
        Value::Blob(bytes) => json!(bytes),
    }
}

/// Makes the SQLite file `file` running `sql`, serves it as the D1 database
/// `name`, and has smugglr reach it; returns the place's address.
fn d1(world: &World, name: &str, file: &str, sql: &str) -> Result<(String, String), String> {
    let path = world.place(file, sql)?;
    let api = serve_d1(name, &path)?;
    world.env.set(vec![
        ("CLOUDFLARE_ACCOUNT_ID".to_owned(), ACCOUNT.to_owned()),
        ("CLOUDFLARE_API_TOKEN".to_owned(), TOKEN.to_owned()),
        ("CLOUDFLARE_API_BASE_URL".to_owned(), api.clone()),
    ]);
    Ok((format!("d1://{name}"), api))
}

/// The table both services hold, with values that only travel whole when
/// each keeps its kind: integers at and beyond 2^53, a real, NULL, text
/// holding a quote and a comma, and an untyped column holding an integer.
const THINGS: &str = "CREATE TABLE things (id TEXT PRIMARY KEY, n INTEGER, r REAL, \
     body TEXT, extra)";

/// A second table, whose one row is changed differently in each place.
const ORDERS: &str = "CREATE TABLE orders (id TEXT PRIMARY KEY, item TEXT)";

/// What each service holds, before the sync.
const SERVICE_ROWS: &str = "INSERT INTO things VALUES \
     ('a', 9223372036854775807, 1.5, 'it''s, here', NULL), \
     ('b', 9007199254740993, 0.1, 'both', 'same'); \
     INSERT INTO orders VALUES ('o1', 'from the service')";

/// What each local file holds, before the sync.
const FILE_ROWS: &str = "INSERT INTO things VALUES \
     ('b', 9007199254740993, 0.1, 'both', 'same'), \
     ('d', -9223372036854775808, 1e300, '', 42); \
     INSERT INTO orders VALUES ('o1', 'from the file')";

#[given(
    "the same table held in a hosted SQLite service reached through its adapter and in an HTTP-SQL service, each beside an identical local file"
)]
fn hosted_and_http_sql(world: &World) -> Result<(), String> {
    let shape = format!("{THINGS}; {ORDERS}");
    let (hosted, api) = d1(
        world,
        "shop",
        "hosted.db",
        &format!("{shape}; {SERVICE_ROWS}"),
    )?;
    let http_sql = service(world, "service.db", &format!("{shape}; {SERVICE_ROWS}"))?;
    let rows = format!("{shape}; {FILE_ROWS}");
    let beside_hosted = world.place("beside-hosted.db", &rows)?;
    let beside_http_sql = world.place("beside-http-sql.db", &rows)?;
    world
        .copies
        .set(vec![(beside_hosted, hosted), (beside_http_sql, http_sql)]);
    world.watched.set(api);
    Ok(())
}

#[when("the agent syncs each pair with smugglr sync --json")]
fn sync_each_pair(world: &World) -> Result<(), String> {
    let copies = world.copies.get().ok_or("no pairs")?;
    let records = copies
        .iter()
        .map(|(file, service)| {
            let output = world.smugglr(&["sync", file, service, "--json"])?;
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        })
        .collect::<Result<Vec<String>, String>>()?;
    world.records.set(records);
    Ok(())
}

#[then(
    "the two records hold the same statuses, counts and keys, and the two services' rows hash the same"
)]
fn records_and_hashes_match(world: &World) -> Result<(), String> {
    let records = world.records.get().ok_or("no records")?;
    let [hosted, http_sql] = records
        .iter()
        .map(|said| {
            serde_json::from_str::<Json>(said)
                .map_err(|err| format!("not a record ({err}): {said}"))
        })
        .collect::<Result<Vec<Json>, String>>()?
        .try_into()
        .map_err(|_| "not two records")?;
    // A run that moved rows and stopped on one, so the records have
    // statuses, counts and keys to compare.
    if http_sql["status"] != json!("stopped in part") {
        return Err(format!(
            "the HTTP-SQL run did not move and stop: {http_sql}"
        ));
    }
    for part in ["status", "tables"] {
        if hosted[part] != http_sql[part] {
            return Err(format!(
                "the records' {part} differ: {} and {}",
                hosted[part], http_sql[part]
            ));
        }
    }
    let copies = world.copies.get().ok_or("no pairs")?;
    let api = world.watched.get().ok_or("no D1 API")?;
    let mut hosted = D1::new("shop", ACCOUNT, TOKEN, &api).place();
    let mut http_sql = HttpSql::open(&copies[1].1);
    for name in ["things", "orders"] {
        let here = hashes(&mut hosted, name)?;
        let there = hashes(&mut http_sql, name)?;
        if here != there {
            return Err(format!("{name} hashes differ: {here:?} and {there:?}"));
        }
    }
    Ok(())
}

/// Each row's key and hash in the table `name` of `place`, by key.
fn hashes(place: &mut dyn Place, name: &str) -> Result<Vec<(Key, [u8; 16])>, String> {
    let tables = place.tables().map_err(|err| err.to_string())?;
    let table = tables
        .iter()
        .find(|table| table.name == name)
        .ok_or(format!("{} has no {name}", place.address()))?;
    let mut hashes = place.hashes(table).map_err(|err| err.to_string())?;
    hashes.sort();
    Ok(hashes)
}

#[given(
    "a local file and a D1 database that holds a _cf_KV table, with no ignore list in the config"
)]
fn file_and_d1_with_cf_kv(world: &World) -> Result<(), String> {
    world
        .source
        .set(world.place("field.db", &format!("{NOTES}; {THREE_NOTES}"))?);
    let (address, _) = d1(
        world,
        "shop",
        "d1.db",
        &format!(
            "{NOTES}; CREATE TABLE _cf_KV (key TEXT PRIMARY KEY, value BLOB) WITHOUT ROWID; \
             INSERT INTO _cf_KV VALUES ('__wrangler', x'01')"
        ),
    )?;
    world.target.set(address);
    Ok(())
}

#[then("the record lists _cf_KV once among the ignored tables and the run does not stop on it")]
fn cf_kv_ignored_once(world: &World) -> Result<(), String> {
    let record = record(world)?;
    if record["ignored"] != json!(["_cf_KV"]) {
        return Err(format!("the record ignores {}", record["ignored"]));
    }
    let output = world.output.get().ok_or("smugglr was not run")?;
    if !output.status.success() || record["status"] != json!("done") {
        return Err(format!(
            "the run stopped ({}): {record}, saying {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    if record["tables"]["notes"].is_null() {
        return Err(format!("the run did not sync notes: {record}"));
    }
    Ok(())
}

/// Real files and the stand-in: every kind of value, a blob and an empty
/// blob among them, goes to D1 and is edited and deleted there by later
/// syncs, and D1's rows hash as the file's.
#[test]
fn every_kind_of_value_goes_to_d1_and_is_edited_and_deleted_there() -> Result<(), String> {
    let world = World::default();
    let shape = "CREATE TABLE kinds (id TEXT PRIMARY KEY, n INTEGER, r REAL, t TEXT, b BLOB, x)";
    let file = world.place(
        "field.db",
        &format!(
            "{shape}; INSERT INTO kinds VALUES \
             ('k1', 9223372036854775807, 1.5, 'it''s', x'00ff', 42), \
             ('k2', -9223372036854775808, -0.0, '', x'', NULL), \
             ('k3', NULL, 1e300, 'a,b', NULL, '42'), \
             ('k4', 9007199254740993, NULL, NULL, x'0a', 1.25)"
        ),
    )?;
    let (address, api) = d1(&world, "shop", "d1.db", shape)?;
    world.source.set(file.clone());
    world.target.set(address);
    world.sync_ok()?;
    let same = |what: &str| -> Result<(), String> {
        let mut d1 = D1::new("shop", ACCOUNT, TOKEN, &api).place();
        let mut local = smugglr_place::LocalFile::open(&file).map_err(|err| err.to_string())?;
        let (there, here) = (hashes(&mut d1, "kinds")?, hashes(&mut local, "kinds")?);
        if there == here {
            Ok(())
        } else {
            Err(format!(
                "after {what}, D1 holds {there:?}, the file {here:?}"
            ))
        }
    };
    same("the first sync")?;
    crate::setup::sql_on(
        &file,
        "UPDATE kinds SET b = x'beef', x = NULL WHERE id = 'k1'; DELETE FROM kinds WHERE id = 'k2'",
    )?;
    world.sync_ok()?;
    same("an edit and a delete")?;
    if rows_of(&file, "SELECT count(*) FROM kinds")? != vec![vec!["Integer(3)".to_owned()]] {
        return Err("the file lost rows".to_owned());
    }
    Ok(())
}

/// A D1 place without its account id or token is refused, naming the one
/// missing.
#[test]
fn a_missing_credential_is_refused_by_name() -> Result<(), String> {
    let world = World::default();
    let file = world.place("field.db", NOTES)?;
    for (set, missing) in [
        (None, "CLOUDFLARE_ACCOUNT_ID"),
        (
            Some(("CLOUDFLARE_ACCOUNT_ID", ACCOUNT)),
            "CLOUDFLARE_API_TOKEN",
        ),
    ] {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_smugglr"));
        command
            .args(["sync", &file, "d1://shop"])
            .env("XDG_CONFIG_HOME", world.xdg()?)
            .env_remove("CLOUDFLARE_ACCOUNT_ID")
            .env_remove("CLOUDFLARE_API_TOKEN");
        if let Some((var, value)) = set {
            command.env(var, value);
        }
        let output = command.output().map_err(|err| err.to_string())?;
        let said = String::from_utf8_lossy(&output.stderr);
        if output.status.code() != Some(2) || !said.contains(missing) {
            return Err(format!(
                "smugglr exited {} saying {said:?}, not naming {missing}",
                output.status
            ));
        }
    }
    Ok(())
}
