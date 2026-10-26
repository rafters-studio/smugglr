//! Cloudflare D1's adapter: a [`Transport`] for the HTTP-SQL place, so
//! `d1://<database-name>` is a place that speaks HTTP-SQL.
//!
//! The adapter takes each HTTP-SQL request (`{sql, params}`, or `{batch,
//! atomic}`) and answers with an HTTP-SQL response (`columns`, `rows`,
//! `rowsAffected`, or the error envelope), speaking D1's REST API
//! underneath. The place's reads, hashes and conditional writes are the
//! HTTP-SQL place's own; D1's quirks are the adapter's
//! ([`queries::d1`](crate::queries)):
//!
//! - D1 binds every parameter as text, so each parameter goes as text and
//!   the statement turns it back into its kind.
//! - D1 counts changes by connection, not by statement, so a write returns
//!   a row for each row it writes, and `rowsAffected` counts them.
//! - D1 binds at most 100 parameters to a statement; one with more is
//!   refused before it is sent.
//! - D1's own `_cf_*` tables are the host's own: never read, always
//!   ignored (FR-SMUGGLR-066).
//! - A batch goes to D1 as one `/raw` request. HTTP-SQL's `atomic: true`
//!   is not proven there: D1 documents its Workers `batch()` as one
//!   transaction, but its REST docs do not say a `/raw` batch is one. The
//!   adapter does not claim it. A write batch that fails part-way may leave
//!   its earlier rows written, which is the host's shortfall, and smugglr's
//!   journal and read-back are what cover it: the run stops not done, and
//!   the next run finishes it (FR-SMUGGLR-073).
//!
//! Each HTTP-SQL request is one D1 request, so a run makes the round trips
//! a native HTTP-SQL service would. The one request more is looking up the
//! database's id by name: once per process, the first time a D1 place of
//! that name is used, one GET to the API. Requests and answers cross the
//! [`Transport`] seam as parsed JSON, so the adapter encodes and parses
//! each D1 body once, at the wire.
//!
//! D1 answers integers as JSON numbers, so one beyond 2^53 a statement
//! selects as itself arrives rounded; the HTTP-SQL place selects every value
//! as its `quote()` text, so none of its reads meet this.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use serde_json::{Value as Json, json};

use crate::http_sql::Transport;
use crate::queries::d1::{Kind, MAX_PARAMS, returning, typed_params, writes};
use crate::{Error, HttpSql};
use smugglr_core::hex::{base64, unbase64};

/// The variable holding the Cloudflare account id.
pub const ACCOUNT_ID: &str = "CLOUDFLARE_ACCOUNT_ID";

/// The variable holding the Cloudflare API token.
pub const API_TOKEN: &str = "CLOUDFLARE_API_TOKEN";

/// The variable naming where Cloudflare's API is, when not at
/// [`DEFAULT_API`]: a stand-in of the API, as the tests run.
///
/// Current choice (2026-10-20, #555): the variable wrangler reads for the
/// same, because CI runs the D1 scenarios against a local stand-in of the
/// API and the CLI must reach it; revisable.
pub const API_BASE_URL: &str = "CLOUDFLARE_API_BASE_URL";

/// Cloudflare's API.
pub const DEFAULT_API: &str = "https://api.cloudflare.com/client/v4";

/// D1's own tables, which smugglr never reads and always ignores
/// (FR-SMUGGLR-066).
pub const OWN_TABLES: &[&str] = &["_cf_*"];

/// A D1 database, by name, in a Cloudflare account.
#[derive(Debug)]
pub struct D1 {
    name: String,
    account: String,
    token: String,
    api: String,
    agent: ureq::Agent,
}

/// D1 databases' ids, by API, account and name.
type Ids = BTreeMap<(String, String, String), String>;

/// Each D1 database's id, by API, account and name, looked up once per
/// process: the one request the adapter makes that HTTP-SQL itself would
/// not, so a `--watch` or a second place of the same database makes it no
/// more.
fn ids() -> &'static Mutex<Ids> {
    static IDS: OnceLock<Mutex<Ids>> = OnceLock::new();
    IDS.get_or_init(Mutex::default)
}

/// What D1 refused, as an HTTP-SQL error's code and message.
type Refusal = (String, String);

impl D1 {
    /// The HTTP-SQL place for the database `name`: its account id and token
    /// from [`ACCOUNT_ID`] and [`API_TOKEN`], and the API from
    /// [`API_BASE_URL`] or else [`DEFAULT_API`]. Nothing is sent until the
    /// place is read.
    ///
    /// # Errors
    /// [`Error::Missing`], naming the variable, when the account id or the
    /// token is not set.
    pub fn open(name: &str) -> Result<HttpSql, Error> {
        let var = |var: &'static str| match std::env::var(var) {
            Ok(value) if !value.is_empty() => Ok(value),
            _ => Err(Error::Missing {
                address: format!("d1://{name}"),
                var,
            }),
        };
        let account = var(ACCOUNT_ID)?;
        let token = var(API_TOKEN)?;
        let api = std::env::var(API_BASE_URL)
            .ok()
            .filter(|api| !api.is_empty())
            .unwrap_or_else(|| DEFAULT_API.to_owned());
        Ok(Self::new(name, &account, &token, &api).place())
    }

    /// The adapter for the database `name` in `account`, reached with
    /// `token` through the API at `api`.
    #[must_use]
    pub fn new(name: &str, account: &str, token: &str, api: &str) -> Self {
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        Self {
            name: name.to_owned(),
            account: account.to_owned(),
            token: token.to_owned(),
            api: api.trim_end_matches('/').to_owned(),
            agent,
        }
    }

    /// The HTTP-SQL place at `d1://<name>`, reached through this adapter.
    #[must_use]
    pub fn place(self) -> HttpSql {
        HttpSql::through(&format!("d1://{}", self.name), Box::new(self))
    }

    /// The account's D1 databases.
    fn databases(&self) -> String {
        format!("{}/accounts/{}/d1/database", self.api, self.account)
    }

    /// A D1 response's `result`, or what D1 refused.
    fn result(
        response: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    ) -> Result<Result<Json, Refusal>, String> {
        let mut response = response.map_err(|err| err.to_string())?;
        let status = response.status();
        let text = response
            .body_mut()
            .with_config()
            .limit(u64::MAX)
            .read_to_string()
            .map_err(|err| err.to_string())?;
        let mut envelope: Json = serde_json::from_str(&text)
            .map_err(|err| format!("D1 answered {status} with no JSON ({err}): {text}"))?;
        if status.is_success() && envelope["success"] == json!(true) {
            return match envelope.get_mut("result").map(Json::take) {
                Some(result) => Ok(Ok(result)),
                None => Ok(Err((
                    "server_error".to_owned(),
                    format!("D1 answered {status} with no result: {text}"),
                ))),
            };
        }
        let error = envelope["errors"]
            .as_array()
            .and_then(|errors| errors.first())
            .cloned()
            .unwrap_or_default();
        Ok(Err((
            format!("d1_{}", error["code"]),
            error["message"]
                .as_str()
                .map_or_else(|| format!("{status}: {text}"), str::to_owned),
        )))
    }

    /// The database's id: the one database in the account with its name,
    /// looked up once per process.
    fn id(&self) -> Result<Result<String, Refusal>, String> {
        let at = (self.api.clone(), self.account.clone(), self.name.clone());
        if let Some(id) = ids().lock().map_err(|err| err.to_string())?.get(&at) {
            return Ok(Ok(id.clone()));
        }
        let response = self
            .agent
            .get(self.databases())
            .query("name", &self.name)
            .header("Authorization", format!("Bearer {}", self.token))
            .call();
        let result = match Self::result(response)? {
            Ok(result) => result,
            Err(refusal) => return Ok(Err(refusal)),
        };
        // D1's name filter is a search, so the name must match whole.
        let found = result
            .as_array()
            .into_iter()
            .flatten()
            .find(|database| database["name"].as_str() == Some(self.name.as_str()))
            .and_then(|database| database["uuid"].as_str());
        Ok(match found {
            Some(id) => {
                ids()
                    .lock()
                    .map_err(|err| err.to_string())?
                    .insert(at, id.to_owned());
                Ok(id.to_owned())
            }
            None => Err((
                "not_found".to_owned(),
                format!("no D1 database named {} in the account", self.name),
            )),
        })
    }

    /// Runs `statements` (each HTTP-SQL's `{sql, params}`) as one D1 batch,
    /// and returns each one's HTTP-SQL result, or what D1 refused.
    ///
    /// `atomic` is not honored as a promise: the batch goes as one `/raw`
    /// request whatever it says, and D1's REST API offers no `BEGIN` to make
    /// it one transaction. Whether `/raw` is one is D1's to say and its REST
    /// docs do not; the release run against the real service is where it
    /// can be seen.
    fn run(&self, statements: &[Json]) -> Result<Result<Vec<Json>, Refusal>, String> {
        let mut batch = Vec::with_capacity(statements.len());
        let mut written = Vec::with_capacity(statements.len());
        for statement in statements {
            let (sql, params) = match translate(statement) {
                Ok(translated) => translated,
                Err(message) => return Ok(Err(("bad_request".to_owned(), message))),
            };
            let is_write = writes(&sql);
            batch.push(json!({
                "sql": if is_write { returning(&sql) } else { sql },
                "params": params,
            }));
            written.push(is_write);
        }
        let id = match self.id()? {
            Ok(id) => id,
            Err(refusal) => return Ok(Err(refusal)),
        };
        let response = self
            .agent
            .post(format!("{}/{id}/raw", self.databases()))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/json")
            .send(json!({ "batch": batch }).to_string());
        let results = match Self::result(response)? {
            Ok(Json::Array(results)) if results.len() == statements.len() => results,
            Ok(other) => return Err(format!("D1 answered a batch with {other}")),
            Err(refusal) => return Ok(Err(refusal)),
        };
        // An answer of the wrong shape is HTTP-SQL's server error, never an
        // empty result.
        Ok(results
            .into_iter()
            .zip(written)
            .map(|(result, is_write)| answer(result, is_write))
            .collect::<Result<Vec<Json>, String>>()
            .map_err(|message| ("server_error".to_owned(), message)))
    }
}

/// An HTTP-SQL statement as D1 takes it: every parameter as text, and the
/// statement turning each back into its kind.
fn translate(statement: &Json) -> Result<(String, Vec<Json>), String> {
    let sql = statement["sql"].as_str().ok_or("a statement without sql")?;
    let params = match statement.get("params") {
        None | Some(Json::Null) => &[][..],
        Some(Json::Array(params)) => params.as_slice(),
        Some(other) => return Err(format!("params {other} is not a list")),
    };
    if params.len() > MAX_PARAMS {
        return Err(format!(
            "D1 binds at most {MAX_PARAMS} parameters to a statement, and this one has {}",
            params.len()
        ));
    }
    let (kinds, texts): (Vec<Kind>, Vec<Json>) = params
        .iter()
        .map(|param| text(param).map(|(kind, text)| (kind, Json::String(text))))
        .collect::<Result<Vec<_>, String>>()?
        .into_iter()
        .unzip();
    Ok((typed_params(sql, &kinds), texts))
}

/// An HTTP-SQL parameter as text, and its kind.
fn text(param: &Json) -> Result<(Kind, String), String> {
    Ok(match param {
        Json::Null => (Kind::Null, String::new()),
        Json::Bool(true) => (Kind::Integer, "1".to_owned()),
        Json::Bool(false) => (Kind::Integer, "0".to_owned()),
        Json::Number(n) if n.is_i64() || n.is_u64() => (Kind::Integer, n.to_string()),
        Json::Number(n) => (Kind::Real, n.to_string()),
        Json::String(text) => (Kind::Text, text.clone()),
        Json::Object(tagged) => match (tagged.get("$type"), tagged["$value"].as_str()) {
            (Some(Json::String(kind)), Some(value)) if kind == "bigint" => {
                (Kind::Integer, value.to_owned())
            }
            (Some(Json::String(kind)), Some(value)) if kind == "blob" => {
                let bytes = unbase64(value).ok_or_else(|| format!("{value:?} is not base64"))?;
                (Kind::Blob, hex(&bytes))
            }
            _ => return Err(format!("D1 cannot take {param}")),
        },
        Json::Array(_) => return Err(format!("D1 cannot take {param}")),
    })
}

/// `bytes` as upper-case hex, as `unhex()` reads it.
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789ABCDEF";
    bytes
        .iter()
        .flat_map(|byte| {
            [
                DIGITS[usize::from(byte >> 4)],
                DIGITS[usize::from(byte & 15)],
            ]
        })
        .map(char::from)
        .collect()
}

/// One D1 result as an HTTP-SQL result. A write returned a row for each row
/// it wrote; it answers with no rows, and counts them.
///
/// An answer of any other shape is an error, never an empty result: rows
/// read as missing would look deleted on this side, and the plan would
/// carry those deletes to the other place.
fn answer(mut result: Json, wrote: bool) -> Result<Json, String> {
    let wrong = |what: &str| format!("D1 answered a statement {what}");
    if result.get("success").and_then(Json::as_bool) == Some(false) {
        return Err(wrong(&format!("as failed: {result}")));
    }
    let mut take = |field: &str| match result
        .get_mut("results")
        .and_then(|results| results.get_mut(field))
        .map(Json::take)
    {
        Some(Json::Array(values)) => Ok(values),
        Some(other) => Err(wrong(&format!("with {field} {other}"))),
        None => Err(wrong(&format!("without {field}"))),
    };
    let rows = take("rows")?;
    if wrote {
        return Ok(
            json!({"columns": [], "rows": [], "rowsAffected": rows.len(), "lastInsertId": null}),
        );
    }
    // D1 answers a blob as a list of its bytes; HTTP-SQL tags it.
    let rows = rows
        .into_iter()
        .map(|row| match row {
            Json::Array(values) => values
                .into_iter()
                .map(blob)
                .collect::<Result<Vec<Json>, String>>()
                .map(Json::Array),
            other => Err(wrong(&format!("with the row {other}"))),
        })
        .collect::<Result<Vec<Json>, String>>()?;
    // Columns are D1's to give, and HTTP-SQL's place does not read them.
    let columns = take("columns").unwrap_or_default();
    Ok(json!({
        "columns": columns,
        "rows": rows,
        "rowsAffected": 0,
        "lastInsertId": null,
    }))
}

/// A D1 value as HTTP-SQL's: a blob, which D1 answers as a list of bytes, as
/// a tagged value. A list that is not bytes is an error.
fn blob(value: Json) -> Result<Json, String> {
    let Json::Array(bytes) = &value else {
        return Ok(value);
    };
    let bytes = bytes
        .iter()
        .map(|byte| byte.as_u64().and_then(|byte| u8::try_from(byte).ok()))
        .collect::<Option<Vec<u8>>>()
        .ok_or_else(|| format!("D1 answered {value} where a value was due"))?;
    Ok(json!({"$type": "blob", "$value": base64(&bytes)}))
}

/// D1's value limit: `quote()` runs on D1, whose strings are at most
/// 2,000,000 bytes, halved for a blob's hex, less 5% headroom, as for a
/// local file (FR-SMUGGLR-071 rev 11).
const VALUE_LIMIT: u64 = 950_000;

impl Transport for D1 {
    fn send(&self, request: &Json) -> Result<(bool, Json), String> {
        let (statements, single) = match request["batch"].as_array() {
            Some(batch) => (batch.as_slice(), false),
            None => (std::slice::from_ref(request), true),
        };
        Ok(match self.run(statements)? {
            Ok(mut results) if single => match results.pop() {
                Some(result) => (true, result),
                None => (
                    false,
                    json!({"error": {"code": "server_error", "message": "D1 answered no result"}}),
                ),
            },
            Ok(results) => (true, json!({ "results": results })),
            Err((code, message)) => (false, json!({"error": {"code": code, "message": message}})),
        })
    }

    fn own_tables(&self) -> &[&str] {
        OWN_TABLES
    }

    fn value_limit(&self) -> u64 {
        VALUE_LIMIT
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{answer, text, translate};
    use crate::queries::d1::Kind;

    #[test]
    fn every_parameter_goes_as_text_with_its_kind() -> Result<(), String> {
        let (sql, params) = translate(&json!({
            "sql": "UPDATE \"t\" SET \"n\" = ?2, \"b\" = ?3, \"r\" = ?4, \"x\" = ?5 \
                    WHERE \"id\" IS ?1",
            "params": [
                "a,b",
                {"$type": "bigint", "$value": "9223372036854775807"},
                {"$type": "blob", "$value": "AP8="},
                1.5,
                null,
            ],
        }))?;
        assert_eq!(
            sql,
            "UPDATE \"t\" SET \"n\" = CAST(?2 AS INTEGER), \"b\" = unhex(?3), \
             \"r\" = CAST(?4 AS REAL), \"x\" = NULLIF(?5, ?5) WHERE \"id\" IS ?1"
        );
        assert_eq!(
            params,
            vec![
                json!("a,b"),
                json!("9223372036854775807"),
                json!("00FF"),
                json!("1.5"),
                json!("")
            ]
        );
        assert_eq!(text(&json!(42))?, (Kind::Integer, "42".to_owned()));
        Ok(())
    }

    /// The adapter's own work for one batch of 1,000 writes, as the
    /// HTTP-SQL place sends them: each statement translated for D1 and the
    /// D1 body encoded (out), and D1's answer parsed and each result turned
    /// into HTTP-SQL's (back). Network time is measured beside it, against
    /// the stand-in, in the smugglr crate's `d1_batch_send_time`.
    #[test]
    #[ignore = "a measurement: run with --ignored --nocapture"]
    fn translation_time_per_statement() -> Result<(), String> {
        use std::time::Instant;

        use super::{returning, writes};
        use crate::Table;
        use crate::value::param;

        const ROWS: u32 = 1000;
        const ROUNDS: u32 = 50;
        let table = Table {
            name: "things".to_owned(),
            columns: vec![
                "id".to_owned(),
                "n".to_owned(),
                "t".to_owned(),
                "b".to_owned(),
            ],
            types: vec![String::new(); 4],
            key: vec!["id".to_owned()],
            self_keyed: false,
        };
        let sql = crate::queries::insert(&table);
        let batch: Vec<serde_json::Value> = (0..ROWS)
            .map(|i| {
                let values = [
                    format!("'k{i}'"),
                    format!("{}", 9_007_199_254_740_993_i64 + i64::from(i)),
                    format!("'row {i}, it''s'"),
                    "X'00FF'".to_owned(),
                ];
                let params = values
                    .iter()
                    .map(|value| param(value).map_err(|err| err.to_string()))
                    .collect::<Result<Vec<_>, String>>()?;
                Ok(json!({"sql": sql, "params": params}))
            })
            .collect::<Result<_, String>>()?;
        // D1's answer to the batch: each insert returned its one row.
        let answer_text = serde_json::to_string(
            &(0..ROWS)
                .map(|_| json!({"results": {"columns": ["1"], "rows": [[1]]}, "success": true}))
                .collect::<Vec<_>>(),
        )
        .map_err(|err| err.to_string())?;

        let (mut out, mut back) = (std::time::Duration::ZERO, std::time::Duration::ZERO);
        for _ in 0..ROUNDS {
            let started = Instant::now();
            let mut d1_batch = Vec::with_capacity(batch.len());
            for statement in &batch {
                let (sql, params) = translate(statement)?;
                let sql = if writes(&sql) { returning(&sql) } else { sql };
                d1_batch.push(json!({"sql": sql, "params": params}));
            }
            let body = json!({ "batch": d1_batch }).to_string();
            out += started.elapsed();
            assert_ne!(body, "");

            let started = Instant::now();
            let results: serde_json::Value =
                serde_json::from_str(&answer_text).map_err(|err| err.to_string())?;
            let answered: Vec<serde_json::Value> = match results {
                serde_json::Value::Array(results) => results
                    .into_iter()
                    .map(|result| answer(result, true))
                    .collect::<Result<_, String>>()?,
                _ => return Err("not a list".to_owned()),
            };
            back += started.elapsed();
            assert_eq!(answered.len(), batch.len());
        }
        // What the string seam cost on top: the HTTP-SQL request encoded and
        // parsed again, and the HTTP-SQL answer encoded and parsed again.
        let request = json!({"batch": batch, "atomic": true});
        let reply = json!({"results": (0..ROWS)
            .map(|_| json!({"columns": [], "rows": [], "rowsAffected": 1, "lastInsertId": null}))
            .collect::<Vec<_>>()});
        let started = Instant::now();
        for _ in 0..ROUNDS {
            let again: serde_json::Value =
                serde_json::from_str(&request.to_string()).map_err(|err| err.to_string())?;
            let answered: serde_json::Value =
                serde_json::from_str(&reply.to_string()).map_err(|err| err.to_string())?;
            assert_eq!(
                again["batch"].as_array().map(Vec::len),
                answered["results"].as_array().map(Vec::len)
            );
        }
        let seam = started.elapsed();
        let per = |total: std::time::Duration| {
            total.as_secs_f64() * 1e6 / f64::from(ROUNDS) / f64::from(ROWS)
        };
        println!(
            "D1 adapter, {ROWS}-statement batch: out {:.2} us/statement, back {:.2} us/statement, \
             total {:.2} us/statement ({:.2} ms per batch)",
            per(out),
            per(back),
            per(out + back),
            (out + back).as_secs_f64() * 1e3 / f64::from(ROUNDS)
        );
        println!(
            "a string seam would add {:.2} us/statement more (encode and parse each way again)",
            per(seam)
        );
        Ok(())
    }

    /// Rows read as missing would look deleted on the D1 side, so an answer
    /// of the wrong shape is an error, never an empty result.
    #[test]
    fn an_answer_of_the_wrong_shape_is_an_error() {
        for (result, wrote) in [
            (json!("not an object"), false),
            (json!([1]), false),
            (json!({"results": 3}), false),
            (json!({"results": {"columns": ["a"]}}), false),
            (json!({"results": {"columns": []}}), true),
            (json!({"results": {"rows": [7]}}), false),
            (json!({"results": {"rows": [[[1, "x"]]]}}), false),
            (json!({"results": {"rows": "none"}}), true),
            (json!({"success": false, "results": {"rows": []}}), false),
        ] {
            let answered = answer(result.clone(), wrote);
            assert!(answered.is_err(), "{result} read as {answered:?}");
        }
    }

    #[test]
    fn more_than_a_hundred_parameters_are_refused() {
        let params = vec![json!(1); 101];
        assert!(translate(&json!({"sql": "SELECT 1", "params": params})).is_err());
    }

    #[test]
    fn a_write_counts_the_rows_it_returned() -> Result<(), String> {
        let result = json!({"results": {"columns": ["1"], "rows": [[1], [1]]}, "success": true});
        assert_eq!(answer(result, true)?["rowsAffected"], json!(2));
        let read = json!({"results": {"columns": ["b"], "rows": [[[0, 255]]]}});
        assert_eq!(
            answer(read, false)?["rows"],
            json!([[{"$type": "blob", "$value": "AP8="}]])
        );
        Ok(())
    }
}
