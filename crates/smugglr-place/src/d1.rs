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
//! D1 answers integers as JSON numbers, so one beyond 2^53 a statement
//! selects as itself arrives rounded; the HTTP-SQL place selects every value
//! as its `quote()` text, so none of its reads meet this.

use std::cell::OnceCell;

use serde_json::{Value as Json, json};

use crate::http_sql::Transport;
use crate::queries::d1::{Kind, MAX_PARAMS, returning, typed_params, writes};
use crate::value::{base64, unbase64};
use crate::{Error, HttpSql};

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
    /// The database's id, looked up by name on first use.
    id: OnceCell<String>,
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
            id: OnceCell::new(),
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
        let envelope: Json = serde_json::from_str(&text)
            .map_err(|err| format!("D1 answered {status} with no JSON ({err}): {text}"))?;
        if status.is_success() && envelope["success"] == json!(true) {
            return Ok(Ok(envelope["result"].clone()));
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

    /// The database's id: the one database in the account with its name.
    fn id(&self) -> Result<Result<&str, Refusal>, String> {
        if let Some(id) = self.id.get() {
            return Ok(Ok(id));
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
            Some(id) => Ok(self.id.get_or_init(|| id.to_owned())),
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
        Ok(Ok(results
            .iter()
            .zip(written)
            .map(|(result, is_write)| answer(result, is_write))
            .collect()))
    }
}

/// An HTTP-SQL statement as D1 takes it: every parameter as text, and the
/// statement turning each back into its kind.
fn translate(statement: &Json) -> Result<(String, Vec<Json>), String> {
    let sql = statement["sql"].as_str().ok_or("a statement without sql")?;
    let params = statement["params"].as_array().cloned().unwrap_or_default();
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
fn answer(result: &Json, wrote: bool) -> Json {
    let rows: Vec<Json> = result["results"]["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if wrote {
        return json!({"columns": [], "rows": [], "rowsAffected": rows.len(), "lastInsertId": null});
    }
    // D1 answers a blob as a list of its bytes; HTTP-SQL tags it.
    let rows: Vec<Json> = rows
        .into_iter()
        .map(|row| match row {
            Json::Array(values) => Json::Array(values.into_iter().map(blob).collect()),
            other => other,
        })
        .collect();
    json!({
        "columns": result["results"]["columns"].as_array().cloned().unwrap_or_default(),
        "rows": rows,
        "rowsAffected": 0,
        "lastInsertId": null,
    })
}

/// A D1 value as HTTP-SQL's: a blob, which D1 answers as a list of bytes, as
/// a tagged value.
fn blob(value: Json) -> Json {
    let bytes = value.as_array().and_then(|bytes| {
        bytes
            .iter()
            .map(|byte| byte.as_u64().and_then(|byte| u8::try_from(byte).ok()))
            .collect::<Option<Vec<u8>>>()
    });
    match bytes {
        Some(bytes) => json!({"$type": "blob", "$value": base64(&bytes)}),
        None => value,
    }
}

impl Transport for D1 {
    fn send(&self, request: &str) -> Result<(bool, String), String> {
        let request: Json =
            serde_json::from_str(request).map_err(|err| format!("not HTTP-SQL: {err}"))?;
        let (statements, single) = match request["batch"].as_array() {
            Some(batch) => (batch.clone(), false),
            None => (vec![request], true),
        };
        let response = match self.run(&statements)? {
            Ok(mut results) if single => results.pop().unwrap_or_default(),
            Ok(results) => json!({ "results": results }),
            Err((code, message)) => {
                return Ok((
                    false,
                    json!({"error": {"code": code, "message": message}}).to_string(),
                ));
            }
        };
        Ok((true, response.to_string()))
    }

    fn own_tables(&self) -> &[&str] {
        OWN_TABLES
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

    #[test]
    fn more_than_a_hundred_parameters_are_refused() {
        let params = vec![json!(1); 101];
        assert!(translate(&json!({"sql": "SELECT 1", "params": params})).is_err());
    }

    #[test]
    fn a_write_counts_the_rows_it_returned() {
        let result = json!({"results": {"columns": ["1"], "rows": [[1], [1]]}, "success": true});
        assert_eq!(answer(&result, true)["rowsAffected"], json!(2));
        let read = json!({"results": {"columns": ["b"], "rows": [[[0, 255]]]}});
        assert_eq!(
            answer(&read, false)["rows"],
            json!([[{"$type": "blob", "$value": "AP8="}]])
        );
    }
}
